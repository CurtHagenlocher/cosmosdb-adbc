//! Live integration test against the local Cosmos DB emulator.
//!
//! Ignored by default (needs a running emulator + seeded data). To run:
//!   cargo run  -p cosmos-client --example seed
//!   cargo test -p cosmos-client --test live_emulator -- --ignored
//!
//! Assumes the emulator is at https://localhost:8081/ with the `spikedb/items` data
//! written by the `seed` example.

use std::collections::HashSet;

use cosmos_client::{CosmosClientHandle, Credential};

/// Public, well-known key for the local Cosmos DB emulator (not a secret).
const EMULATOR_KEY: &str =
    "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

#[tokio::test]
#[ignore = "requires the local Cosmos emulator (run the `seed` example first)"]
async fn cross_partition_order_by_merges_globally() {
    let client = CosmosClientHandle::connect(
        "https://localhost:8081/",
        Credential::Key(EMULATOR_KEY.to_string()),
    )
    .expect("connect to emulator");

    let docs = client
        .query_documents("spikedb", "items", "SELECT * FROM c ORDER BY c.mergeOrder")
        .await
        .expect("cross-partition query");

    assert_eq!(docs.len(), 50, "expected all seeded documents");

    // Global ordering across partitions is the proof the engine's client-side merge ran:
    // consecutive mergeOrder values were deliberately seeded into different partition keys,
    // so a per-partition concatenation could not produce a globally sorted result.
    let orders: Vec<i64> = docs
        .iter()
        .map(|d| d["mergeOrder"].as_i64().expect("mergeOrder is an integer"))
        .collect();
    let mut expected = orders.clone();
    expected.sort_unstable();
    assert_eq!(orders, expected, "results must be globally ordered by mergeOrder");

    // Sanity: the data really does span multiple partitions.
    let distinct_pks: HashSet<&str> = docs
        .iter()
        .map(|d| d["pk"].as_str().expect("pk is a string"))
        .collect();
    assert!(distinct_pks.len() > 1, "data should span multiple partitions");
}
