//! Connection metadata: `get_objects`, `get_table_schema`, `get_table_types`.
//!
//! Cosmos is a two-level namespace (account → database → container), which maps onto ADBC
//! as **catalog = database**, a single empty **schema** (Cosmos has no schema layer), and
//! **table = container**. Columns are inferred by sampling documents (§3.5); like MySQL and
//! other schemaless/2-level sources, the schema level collapses to one empty entry.

use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray};
use arrow_schema::{DataType, Field, Schema};
use cosmos_client::CosmosClientHandle;
use cosmos_datafusion::SchemaCache;
use driverbase::error::ErrorHelper as _;
use driverbase::get_objects::{ColumnInfo, GetObjectsImpl, TableAndColumnInfo, TableInfo};
use regex::Regex;

use crate::error::ErrorHelper;
use crate::runtime::Runtime;

/// The driverbase error, specialized to this driver's [`ErrorHelper`].
type DriverError = driverbase::error::Error<ErrorHelper>;

/// Documents sampled per container to infer its columns for metadata (kept small so
/// `get_objects` at column depth — which samples every container — stays cheap).
pub(crate) const METADATA_SAMPLE_SIZE: usize = 100;
/// Cosmos has no schema layer; every database exposes a single empty schema.
const DEFAULT_SCHEMA: &str = "";
/// The only ADBC table type Cosmos exposes.
const TABLE_TYPE: &str = "table";

/// Sample a container and infer its Arrow schema (shared by `get_objects` columns and
/// `get_table_schema`). Reuses the connection's [`SchemaCache`] — shared with the `datafusion`
/// dialect — so repeated metadata calls (and queries) don't re-sample the same container. The
/// cached schema is a best-effort snapshot; whichever path samples first wins. Returns a
/// `driverbase` error; the ADBC boundary maps it with `.to_adbc()`.
pub(crate) fn sample_schema(
    client: &CosmosClientHandle,
    runtime: &Runtime,
    cache: &SchemaCache,
    database: &str,
    container: &str,
) -> Result<Arc<Schema>, DriverError> {
    let key = (database.to_string(), container.to_string());
    if let Some(schema) = cache.lock().expect("schema cache poisoned").get(&key).cloned() {
        return Ok(schema);
    }
    let sql = format!("SELECT * FROM c OFFSET 0 LIMIT {METADATA_SAMPLE_SIZE}");
    let docs = runtime
        .block_on(async { client.query_documents(database, container, &sql).await })
        .map_err(|e| {
            ErrorHelper::internal(driverbase::location!())
                .message(format!("sampling '{database}/{container}': {e}"))
        })?;
    // ArrowError → DriverError via `From`. Tolerant of heterogeneous fields (→ Utf8).
    let schema = crate::inference::infer_schema(&docs, METADATA_SAMPLE_SIZE)?;
    // Don't cache an empty schema from an empty container — it may gain documents later.
    if !docs.is_empty() {
        cache.lock().expect("schema cache poisoned").insert(key, schema.clone());
    }
    Ok(schema)
}

/// A one-row reader listing the single Cosmos table type, for `Connection::get_table_types`.
pub(crate) fn table_types_reader() -> Box<dyn RecordBatchReader + Send> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "table_type",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![TABLE_TYPE]))],
    )
    .expect("static table_types batch is well-formed");
    Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema))
}

/// `GetObjectsImpl` backing `Connection::get_objects`; driverbase assembles the nested Arrow
/// result and applies depth filtering, calling these methods as needed.
pub(crate) struct CosmosGetObjects {
    client: Arc<CosmosClientHandle>,
    runtime: Arc<Runtime>,
    cache: Arc<SchemaCache>,
}

impl CosmosGetObjects {
    pub(crate) fn new(
        client: Arc<CosmosClientHandle>,
        runtime: Arc<Runtime>,
        cache: Arc<SchemaCache>,
    ) -> Self {
        Self {
            client,
            runtime,
            cache,
        }
    }
}

impl GetObjectsImpl<ErrorHelper> for CosmosGetObjects {
    fn get_catalogs(&self, filter: Option<&str>) -> Result<Vec<String>, DriverError> {
        let mut dbs = self
            .runtime
            .block_on(async { self.client.list_databases().await })
            .map_err(|e| {
                ErrorHelper::internal(driverbase::location!())
                    .message(format!("listing databases: {e}"))
            })?;
        dbs.sort();
        retain_matching(&mut dbs, filter)?;
        Ok(dbs)
    }

    fn get_db_schemas(
        &self,
        _catalog: &str,
        filter: Option<&str>,
    ) -> Result<Vec<String>, DriverError> {
        let mut schemas = vec![DEFAULT_SCHEMA.to_string()];
        retain_matching(&mut schemas, filter)?;
        Ok(schemas)
    }

    fn get_tables(
        &self,
        catalog: &str,
        _db_schema: &str,
        table_filter: Option<&str>,
        table_type_filter: Option<&[String]>,
    ) -> Result<Vec<TableInfo>, DriverError> {
        if let Some(types) = table_type_filter {
            if !types.iter().any(|t| t == TABLE_TYPE) {
                return Ok(Vec::new());
            }
        }
        let mut names = self
            .runtime
            .block_on(async { self.client.list_containers(catalog).await })
            .map_err(|e| {
                ErrorHelper::internal(driverbase::location!())
                    .message(format!("listing containers in '{catalog}': {e}"))
            })?;
        names.sort();
        retain_matching(&mut names, table_filter)?;
        Ok(names
            .into_iter()
            .map(|table_name| TableInfo {
                table_name,
                table_type: TABLE_TYPE.to_string(),
            })
            .collect())
    }

    fn get_columns(
        &self,
        catalog: &str,
        db_schema: &str,
        table_filter: Option<&str>,
        table_type_filter: Option<&[String]>,
        column_filter: Option<&str>,
    ) -> Result<Vec<TableAndColumnInfo>, DriverError> {
        let tables = self.get_tables(catalog, db_schema, table_filter, table_type_filter)?;
        let column_re = column_filter.map(like_to_regex).transpose()?;
        let mut out = Vec::with_capacity(tables.len());
        for table in tables {
            let schema =
                sample_schema(&self.client, &self.runtime, &self.cache, catalog, &table.table_name)?;
            let columns = schema
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .filter(|name| column_re.as_ref().is_none_or(|re| re.is_match(name)))
                .map(|column_name| ColumnInfo { column_name })
                .collect();
            out.push(TableAndColumnInfo { table, columns });
        }
        Ok(out)
    }
}

/// Retain only the items matching an ADBC LIKE search pattern (no filter → keep all).
fn retain_matching(items: &mut Vec<String>, filter: Option<&str>) -> Result<(), DriverError> {
    if let Some(pattern) = filter {
        let re = like_to_regex(pattern)?;
        items.retain(|i| re.is_match(i));
    }
    Ok(())
}

/// Translate an ADBC LIKE pattern (`%` = any run, `_` = any char, `\` escapes) to a regex.
fn like_to_regex(pattern: &str) -> Result<Regex, DriverError> {
    let mut re = String::with_capacity(pattern.len() + 2);
    re.push('^');
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => {
                if let Some(escaped) = chars.next() {
                    re.push_str(&regex::escape(&escaped.to_string()));
                }
            }
            '%' => re.push_str(".*"),
            '_' => re.push('.'),
            other => re.push_str(&regex::escape(&other.to_string())),
        }
    }
    re.push('$');
    Regex::new(&re).map_err(|e| {
        ErrorHelper::invalid_argument().message(format!("invalid search pattern '{pattern}': {e}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field};
    use cosmos_client::Credential;

    /// A cache hit in `sample_schema` returns the memoized schema without sampling — the
    /// client points at an unreachable endpoint, so a miss would network and error.
    #[test]
    fn sample_schema_uses_cache() {
        let client = CosmosClientHandle::connect(
            "https://127.0.0.1:1/",
            Credential::Key(
                "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
                    .into(),
            ),
        )
        .expect("build client");
        let runtime = crate::runtime::Runtime::new_multi_thread().expect("runtime");
        let cache = SchemaCache::default();
        let schema: Arc<Schema> =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        cache
            .lock()
            .unwrap()
            .insert(("db".to_string(), "items".to_string()), schema.clone());

        let got = sample_schema(&client, &runtime, &cache, "db", "items")
            .expect("cache hit should not sample");
        assert_eq!(got.fields().len(), 1);
        assert_eq!(got.field(0).name(), "id");
    }
}
