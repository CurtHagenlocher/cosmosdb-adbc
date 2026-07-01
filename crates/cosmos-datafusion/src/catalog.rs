//! A lazy DataFusion `SchemaProvider` that resolves each referenced container to a
//! `CosmosTableProvider`, inferring its schema by sampling documents on first use.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use cosmos_client::CosmosClientHandle;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::error::{DataFusionError, Result};

use crate::convert;
use crate::provider::CosmosTableProvider;

pub struct CosmosSchemaProvider {
    client: Arc<CosmosClientHandle>,
    database: String,
    sample_size: usize,
}

impl CosmosSchemaProvider {
    pub fn new(client: Arc<CosmosClientHandle>, database: String, sample_size: usize) -> Self {
        Self {
            client,
            database,
            sample_size,
        }
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
        Ok(Some(Arc::new(CosmosTableProvider::new(
            self.client.clone(),
            self.database.clone(),
            name.to_string(),
            schema,
        ))))
    }

    fn table_exist(&self, _name: &str) -> bool {
        // Optimistic: existence is confirmed when `table()` samples the container.
        true
    }
}
