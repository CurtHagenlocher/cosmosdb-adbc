//! Seeds a Cosmos database/container with sample documents so Spike A has cross-partition
//! data to query. Idempotent: re-running upserts the same documents.
//!
//! Defaults target the local Cosmos DB emulator (well-known key). Run:
//!   cargo run -p cosmos-client --example seed
//! then:
//!   cargo run -p cosmos-client --example spike_a
//!
//! Override via env: COSMOS_ENDPOINT, COSMOS_KEY, COSMOS_DATABASE, COSMOS_CONTAINER,
//! COSMOS_SEED_COUNT.

use std::error::Error;

use azure_core::credentials::Secret;
use azure_data_cosmos::CosmosClient;
use azure_data_cosmos::models::ContainerProperties;
use serde_json::json;

/// Public, well-known key for the local Cosmos DB emulator (not a secret).
const EMULATOR_KEY: &str =
    "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let endpoint =
        std::env::var("COSMOS_ENDPOINT").unwrap_or_else(|_| "https://localhost:8081/".to_string());
    let key = std::env::var("COSMOS_KEY").unwrap_or_else(|_| EMULATOR_KEY.to_string());
    let database = std::env::var("COSMOS_DATABASE").unwrap_or_else(|_| "spikedb".to_string());
    let container = std::env::var("COSMOS_CONTAINER").unwrap_or_else(|_| "items".to_string());
    let count: usize = std::env::var("COSMOS_SEED_COUNT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);

    let client = CosmosClient::with_key(&endpoint, Secret::from(key), None)?;

    match client.create_database(&database, None).await {
        Ok(_) => println!("created database '{database}'"),
        Err(e) => println!("create_database: {e} (continuing — it may already exist)"),
    }
    let db = client.database_client(&database);

    let properties = ContainerProperties {
        id: container.clone().into(),
        partition_key: "/pk".into(),
        ..Default::default()
    };
    match db.create_container(properties, None).await {
        Ok(_) => println!("created container '{container}' (partition key /pk)"),
        Err(e) => println!("create_container: {e} (continuing — it may already exist)"),
    }
    let container_client = db.container_client(&container);

    for i in 0..count {
        // 5 distinct partition-key values spread documents across physical partitions.
        let pk = format!("pk-{}", i % 5);
        let doc = json!({
            "id": format!("doc-{i}"),
            "pk": pk,
            "mergeOrder": count - i,        // reverse of insertion → ORDER BY is observable
            "name": format!("item {i}"),
            "value": (i as f64) * 1.5,
            "tags": ["a", "b"],
            "nested": { "k": i }
        });
        container_client.upsert_item(pk.clone(), &doc, None).await?;
    }

    println!("seeded {count} documents into '{database}/{container}'");

    // A second container to demonstrate cross-container joins in the datafusion dialect:
    // one category row per partition-key value, joinable to `items` on `pk`.
    let categories = "categories";
    let cat_props = ContainerProperties {
        id: categories.into(),
        partition_key: "/pk".into(),
        ..Default::default()
    };
    match db.create_container(cat_props, None).await {
        Ok(_) => println!("created container '{categories}' (partition key /pk)"),
        Err(e) => println!("create_container: {e} (continuing — it may already exist)"),
    }
    let cat_client = db.container_client(categories);
    for p in 0..5 {
        let pk = format!("pk-{p}");
        let doc = json!({ "id": pk, "pk": pk, "label": format!("Category {p}") });
        cat_client.upsert_item(pk.clone(), &doc, None).await?;
    }
    println!("seeded 5 documents into '{database}/{categories}'");

    // A third container with a deliberately type-conflicting field `val` (number / string /
    // object across documents) to exercise struct-mode heterogeneous handling
    // (adbc.cosmos.heterogeneous = string | variant).
    let mixed = "mixed";
    let mixed_props = ContainerProperties {
        id: mixed.into(),
        partition_key: "/pk".into(),
        ..Default::default()
    };
    match db.create_container(mixed_props, None).await {
        Ok(_) => println!("created container '{mixed}' (partition key /pk)"),
        Err(e) => println!("create_container: {e} (continuing — it may already exist)"),
    }
    let mixed_client = db.container_client(mixed);
    let mixed_docs = [
        json!({ "id": "m0", "pk": "p", "val": 42 }),
        json!({ "id": "m1", "pk": "p", "val": "hello" }),
        json!({ "id": "m2", "pk": "p", "val": { "nested": true } }),
        json!({ "id": "m3", "pk": "p", "val": null }),
    ];
    for doc in &mixed_docs {
        mixed_client.upsert_item("p", doc, None).await?;
    }
    println!("seeded {} documents into '{database}/{mixed}'", mixed_docs.len());

    println!("next: cargo run -p cosmos-client --example spike_a");
    Ok(())
}
