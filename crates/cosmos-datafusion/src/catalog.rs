//! A lazy DataFusion `SchemaProvider` that resolves each referenced container to a
//! `CosmosTableProvider`, inferring its schema by sampling documents on first use.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use cosmos_client::CosmosClientHandle;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::error::{DataFusionError, Result};

use crate::convert;
use crate::provider::CosmosTableProvider;
use crate::SchemaCache;

pub struct CosmosSchemaProvider {
    client: Arc<CosmosClientHandle>,
    database: String,
    sample_size: usize,
    cache: Arc<SchemaCache>,
}

impl CosmosSchemaProvider {
    pub fn new(
        client: Arc<CosmosClientHandle>,
        database: String,
        sample_size: usize,
        cache: Arc<SchemaCache>,
    ) -> Self {
        Self {
            client,
            database,
            sample_size,
            cache,
        }
    }

    fn provider_for(&self, name: &str, schema: SchemaRef) -> CosmosTableProvider {
        CosmosTableProvider::new(
            self.client.clone(),
            self.database.clone(),
            name.to_string(),
            schema,
        )
    }
}

impl fmt::Debug for CosmosSchemaProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CosmosSchemaProvider")
            .field("database", &self.database)
            .finish()
    }
}

fn external(message: String) -> DataFusionError {
    DataFusionError::External(Box::new(std::io::Error::other(message)))
}

#[async_trait]
impl SchemaProvider for CosmosSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        // Resolved lazily by name; we don't enumerate containers up front.
        Vec::new()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let key = (self.database.clone(), name.to_string());

        // Cache hit: build the provider from the memoized schema, skip sampling entirely.
        let cached = self.cache.lock().expect("schema cache poisoned").get(&key).cloned();
        if let Some(schema) = cached {
            return Ok(Some(Arc::new(self.provider_for(name, schema))));
        }

        let sql = format!("SELECT * FROM c OFFSET 0 LIMIT {}", self.sample_size);
        let docs = self
            .client
            .query_documents(&self.database, name, &sql)
            .await
            .map_err(|e| external(format!("sampling container '{name}' failed: {e}")))?;

        if docs.is_empty() {
            return Ok(None);
        }

        let schema = convert::infer_schema(&docs)
            .map_err(|e| external(format!("schema inference for '{name}' failed: {e}")))?;
        self.cache
            .lock()
            .expect("schema cache poisoned")
            .insert(key, schema.clone());
        Ok(Some(Arc::new(self.provider_for(name, schema))))
    }

    fn table_exist(&self, _name: &str) -> bool {
        // Optimistic: existence is confirmed when `table()` samples the container.
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use cosmos_client::Credential;

    /// A cache hit must return the memoized schema without sampling. The client points at an
    /// unreachable endpoint, so any attempt to sample would error — proving the fast path.
    #[tokio::test]
    async fn cached_schema_skips_sampling() {
        let client = Arc::new(
            CosmosClientHandle::connect(
                "https://127.0.0.1:1/",
                // Well-known emulator key (valid base64); never actually used here.
                Credential::Key(
                    "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
                        .into(),
                ),
            )
            .expect("build client"),
        );

        let cache: Arc<SchemaCache> = Arc::new(SchemaCache::default());
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        cache
            .lock()
            .unwrap()
            .insert(("db".to_string(), "items".to_string()), schema.clone());

        let provider = CosmosSchemaProvider::new(client, "db".to_string(), 10, cache);
        let table = provider
            .table("items")
            .await
            .expect("table() should not error on a cache hit")
            .expect("cache hit yields a provider");
        assert_eq!(table.schema().fields().len(), 1);
        assert_eq!(table.schema().field(0).name(), "id");
    }
}
