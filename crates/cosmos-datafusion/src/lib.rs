//! DataFusion federation for Cosmos DB.
//!
//! Registers Cosmos containers as DataFusion tables so ANSI SQL can be run over them:
//! projection and the filters Cosmos can evaluate are pushed into engine-executed Cosmos
//! SQL, and DataFusion performs joins / cross-container aggregates / anything unpushable
//! locally.
//!
//! Usage: build a `SessionContext`, call [`register_cosmos_schema`], then `ctx.sql(...)`.

mod catalog;
mod convert;
pub mod normalize;
mod predicate;
mod provider;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow_schema::SchemaRef;
use cosmos_client::CosmosClientHandle;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;

pub use catalog::CosmosSchemaProvider;
pub use provider::CosmosTableProvider;

/// A container's inferred Arrow schema, cached by `(database, container)`. Inference samples
/// documents, so this avoids re-sampling the same container across queries on a connection.
/// Schemas are a sample-time snapshot; the cache lives for the owner's lifetime (a connection).
pub type SchemaCache = Mutex<HashMap<(String, String), SchemaRef>>;

/// Register a Cosmos database as the default schema (`datafusion.public`) of `ctx`, so
/// unqualified table names in SQL resolve to containers in that database. Inferred container
/// schemas are memoized in `cache` (share one across queries to skip re-sampling).
pub fn register_cosmos_schema(
    ctx: &SessionContext,
    client: Arc<CosmosClientHandle>,
    database: String,
    sample_size: usize,
    cache: Arc<SchemaCache>,
) -> Result<()> {
    let provider = Arc::new(CosmosSchemaProvider::new(client, database, sample_size, cache));
    let catalog = ctx
        .catalog("datafusion")
        .ok_or_else(|| DataFusionError::Plan("default catalog 'datafusion' not found".into()))?;
    catalog.register_schema("public", provider)?;
    Ok(())
}
