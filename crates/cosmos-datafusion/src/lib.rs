//! DataFusion federation for Cosmos DB.
//!
//! Registers Cosmos containers as DataFusion tables so ANSI SQL can be run over them:
//! projection (and, later, filters) that Cosmos can do are pushed into engine-executed
//! Cosmos SQL, and DataFusion performs joins / cross-container aggregates / anything
//! unpushable locally.
//!
//! Usage: build a `SessionContext`, call [`register_cosmos_schema`], then `ctx.sql(...)`.

mod catalog;
mod convert;
mod provider;

use std::sync::Arc;

use cosmos_client::CosmosClientHandle;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;

pub use catalog::CosmosSchemaProvider;
pub use provider::CosmosTableProvider;

/// Register a Cosmos database as the default schema (`datafusion.public`) of `ctx`, so
/// unqualified table names in SQL resolve to containers in that database.
pub fn register_cosmos_schema(
    ctx: &SessionContext,
    client: Arc<CosmosClientHandle>,
    database: String,
    sample_size: usize,
) -> Result<()> {
    let provider = Arc::new(CosmosSchemaProvider::new(client, database, sample_size));
    let catalog = ctx
        .catalog("datafusion")
        .ok_or_else(|| DataFusionError::Plan("default catalog 'datafusion' not found".into()))?;
    catalog.register_schema("public", provider)?;
    Ok(())
}
