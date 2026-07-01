//! Spike A: end-to-end proof of transport + auth + engine-driven cross-partition query.
//!
//! Reads configuration from environment variables and prints the first few documents
//! returned by a Cosmos SQL query executed through the experimental engine.
//!
//! Run (PowerShell), against a real account with Entra ID:
//!   $env:COSMOS_ENDPOINT="https://<acct>.documents.azure.com:443/"
//!   $env:COSMOS_AUTH="entra"; $env:COSMOS_DATABASE="db"; $env:COSMOS_CONTAINER="c"
//!   $env:COSMOS_QUERY="SELECT * FROM c ORDER BY c.someField"
//!   cargo run -p cosmos-client --example spike_a
//!
//! Or against the local Cosmos emulator with the well-known key:
//!   $env:COSMOS_AUTH="key"; $env:COSMOS_KEY="<emulator key>"
//!   (endpoint https://localhost:8081/ ...)

use std::error::Error;

use cosmos_client::{CosmosClientHandle, Credential};

/// Public, well-known key for the local Cosmos DB emulator (not a secret).
const EMULATOR_KEY: &str =
    "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Defaults target the local emulator + the data written by `seed`, so this runs with
    // zero configuration. Override any of these via env for a real account.
    let endpoint =
        std::env::var("COSMOS_ENDPOINT").unwrap_or_else(|_| "https://localhost:8081/".to_string());
    let auth = std::env::var("COSMOS_AUTH").unwrap_or_else(|_| "key".to_string());
    let database = std::env::var("COSMOS_DATABASE").unwrap_or_else(|_| "spikedb".to_string());
    let container = std::env::var("COSMOS_CONTAINER").unwrap_or_else(|_| "items".to_string());
    let query = std::env::var("COSMOS_QUERY")
        .unwrap_or_else(|_| "SELECT * FROM c ORDER BY c.mergeOrder".to_string());

    let credential = match auth.as_str() {
        "key" => Credential::Key(
            std::env::var("COSMOS_KEY").unwrap_or_else(|_| EMULATOR_KEY.to_string()),
        ),
        "connection_string" => {
            Credential::ConnectionString(std::env::var("COSMOS_CONNECTION_STRING")?)
        }
        _ => Credential::Entra,
    };

    println!("connecting to {endpoint} (auth={auth})…");
    let client = CosmosClientHandle::connect(&endpoint, credential)?;

    println!("querying [{database}/{container}]: {query}");
    let docs = client.query_documents(&database, &container, &query).await?;

    println!("returned {} document(s):", docs.len());
    for (i, doc) in docs.iter().take(5).enumerate() {
        println!("  [{i}] {}", serde_json::to_string(doc)?);
    }
    Ok(())
}
