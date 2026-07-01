//! Neutral Cosmos DB transport for the ADBC driver.
//!
//! This crate is the only place that talks to Azure. It exposes async functions over a
//! small, neutral surface (endpoint + credential in, `serde_json::Value` documents out)
//! and deliberately leaks **no** Arrow / DataFusion / ADBC types — matching the layering
//! in `DESIGN.md` (mashup-rs §10).
//!
//! Cross-partition queries are executed through the experimental
//! `azure_data_cosmos_engine`, plugged into the SDK's `preview_query_engine` seam: the
//! SDK fetches the query plan + partition-key ranges and drives the pull-based pipeline
//! with its own HTTP transport, so the full cross-partition surface (ORDER BY, aggregates,
//! GROUP BY, DISTINCT, TOP, OFFSET/LIMIT, hybrid) runs server-side and merges client-side.

use std::sync::Arc;

use azure_core::credentials::Secret;
use azure_data_cosmos::{CosmosClient, QueryOptions};
use futures::TryStreamExt;
use serde_json::Value;

/// How to authenticate to the Cosmos account.
#[derive(Debug, Clone)]
pub enum Credential {
    /// Entra ID via `DefaultAzureCredential` (managed identity, service principal,
    /// developer sign-in). The modern-auth path the ODBC driver lacks.
    Entra,
    /// Account key (primary/secondary, read-write or read-only).
    Key(String),
    /// Full connection string (`AccountEndpoint=...;AccountKey=...;`).
    ConnectionString(String),
}

/// A connected Cosmos account client.
pub struct CosmosClientHandle {
    client: CosmosClient,
}

impl CosmosClientHandle {
    /// Connect to a Cosmos account. For [`Credential::ConnectionString`], `endpoint` is
    /// ignored (the connection string carries it).
    pub fn connect(endpoint: &str, credential: Credential) -> azure_core::Result<Self> {
        let client = match credential {
            Credential::Entra => {
                // azure_identity 0.30 has no all-in-one DefaultAzureCredential; DeveloperToolsCredential
                // chains the local developer sign-ins (az / azd CLI). Phase 1 will select explicitly
                // among ManagedIdentityCredential / ClientSecretCredential / WorkloadIdentityCredential
                // for production managed-identity and service-principal auth.
                let cred = azure_identity::DeveloperToolsCredential::new(None)?;
                CosmosClient::new(endpoint, cred, None)?
            }
            Credential::Key(key) => CosmosClient::with_key(endpoint, Secret::from(key), None)?,
            Credential::ConnectionString(cs) => {
                CosmosClient::with_connection_string(cs.into(), None)?
            }
        };
        Ok(Self { client })
    }

    /// Run a Cosmos SQL query across all partitions via the experimental engine, returning
    /// every matching document as a raw JSON value.
    ///
    /// Passing an empty partition key (`()`) together with a query engine is what routes
    /// execution through the engine-backed cross-partition executor in the SDK.
    pub async fn query_documents(
        &self,
        database: &str,
        container: &str,
        query: &str,
    ) -> azure_core::Result<Vec<Value>> {
        let container_client = self
            .client
            .database_client(database)
            .container_client(container);

        let engine = Arc::new(azure_data_cosmos_engine::query::QueryEngine);
        let options = QueryOptions {
            query_engine: Some(engine),
            ..Default::default()
        };

        let items: Vec<Value> = container_client
            .query_items::<Value>(query, (), Some(options))?
            .try_collect()
            .await?;
        Ok(items)
    }
}
