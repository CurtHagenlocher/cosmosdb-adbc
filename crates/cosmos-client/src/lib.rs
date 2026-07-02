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

/// How to authenticate to the Cosmos account. The Entra variants are the modern-auth paths the
/// ODBC driver lacks; account key / connection string match the ODBC driver's capability.
#[derive(Debug, Clone)]
pub enum Credential {
    /// Developer sign-in (`az` / `azd` CLI) via `DeveloperToolsCredential` — for local dev.
    Entra,
    /// Managed identity (Azure VM / App Service / AKS). `client_id` selects a specific
    /// user-assigned identity; `None` uses the system-assigned identity.
    ManagedIdentity { client_id: Option<String> },
    /// Service principal authenticating with a client secret.
    ServicePrincipal {
        tenant_id: String,
        client_id: String,
        client_secret: String,
    },
    /// Workload identity federation (e.g. AKS). IDs and the token file default to the standard
    /// `AZURE_*` environment variables when not given.
    WorkloadIdentity {
        tenant_id: Option<String>,
        client_id: Option<String>,
    },
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
        use azure_identity::{
            ClientSecretCredential, DeveloperToolsCredential, ManagedIdentityCredential,
            ManagedIdentityCredentialOptions, UserAssignedId, WorkloadIdentityCredential,
            WorkloadIdentityCredentialOptions,
        };

        // azure_identity 0.30 has no all-in-one `DefaultAzureCredential`, so each mode selects an
        // explicit credential. Construction is offline; token acquisition is deferred to first use.
        let client = match credential {
            Credential::Entra => {
                let cred = DeveloperToolsCredential::new(None)?;
                CosmosClient::new(endpoint, cred, None)?
            }
            Credential::ManagedIdentity { client_id } => {
                let options = client_id.map(|id| ManagedIdentityCredentialOptions {
                    user_assigned_id: Some(UserAssignedId::ClientId(id)),
                    ..Default::default()
                });
                let cred = ManagedIdentityCredential::new(options)?;
                CosmosClient::new(endpoint, cred, None)?
            }
            Credential::ServicePrincipal {
                tenant_id,
                client_id,
                client_secret,
            } => {
                let cred = ClientSecretCredential::new(
                    &tenant_id,
                    client_id,
                    Secret::from(client_secret),
                    None,
                )?;
                CosmosClient::new(endpoint, cred, None)?
            }
            Credential::WorkloadIdentity {
                tenant_id,
                client_id,
            } => {
                let options = WorkloadIdentityCredentialOptions {
                    tenant_id,
                    client_id,
                    ..Default::default()
                };
                let cred = WorkloadIdentityCredential::new(Some(options))?;
                CosmosClient::new(endpoint, cred, None)?
            }
            Credential::Key(key) => CosmosClient::with_key(endpoint, Secret::from(key), None)?,
            Credential::ConnectionString(cs) => {
                CosmosClient::with_connection_string(cs.into(), None)?
            }
        };
        Ok(Self { client })
    }

    /// List the databases in the account (ADBC catalogs).
    pub async fn list_databases(&self) -> azure_core::Result<Vec<String>> {
        let dbs: Vec<azure_data_cosmos::models::DatabaseProperties> = self
            .client
            .query_databases("SELECT * FROM root", None)?
            .try_collect()
            .await?;
        Ok(dbs.into_iter().map(|d| d.id).collect())
    }

    /// List the containers in a database (ADBC tables).
    pub async fn list_containers(&self, database: &str) -> azure_core::Result<Vec<String>> {
        let colls: Vec<azure_data_cosmos::models::ContainerProperties> = self
            .client
            .database_client(database)
            .query_containers("SELECT * FROM root", None)?
            .try_collect()
            .await?;
        Ok(colls.into_iter().map(|c| c.id.into_owned()).collect())
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
