//! Live end-to-end test: drive the ADBC driver against the local Cosmos emulator and
//! confirm a native-dialect query returns an `arrow.json` result.
//!
//! Ignored by default. To run (seed via the cosmos-client example first):
//!   cargo run  -p cosmos-client --example seed
//!   cargo test -p adbc-cosmos  --test live_driver -- --ignored

use adbc_core::options::{OptionDatabase, OptionStatement, OptionValue};
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use adbc_cosmos::CosmosDriver;

/// Public, well-known key for the local Cosmos DB emulator (not a secret).
const EMULATOR_KEY: &str =
    "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

fn other(key: &str, value: &str) -> (OptionDatabase, OptionValue) {
    (OptionDatabase::Other(key.into()), OptionValue::String(value.into()))
}

#[test]
#[ignore = "requires the local Cosmos emulator (run cosmos-client's seed example first)"]
fn native_json_query_returns_arrow_json() {
    let mut driver = CosmosDriver::default();
    let db = driver
        .new_database_with_opts([
            (
                OptionDatabase::Uri,
                OptionValue::String("https://localhost:8081/".into()),
            ),
            other("adbc.cosmos.auth", "key"),
            other("adbc.cosmos.account_key", EMULATOR_KEY),
            other("adbc.cosmos.database", "spikedb"),
        ])
        .expect("new_database");

    let mut conn = db.new_connection().expect("new_connection");
    let mut stmt = conn.new_statement().expect("new_statement");

    // dialect defaults to native, output defaults to json.
    stmt.set_option(
        OptionStatement::Other("adbc.cosmos.container".into()),
        OptionValue::String("items".into()),
    )
    .expect("set container");
    stmt.set_sql_query("SELECT * FROM c ORDER BY c.mergeOrder")
        .expect("set query");

    let reader = stmt.execute().expect("execute");

    // The single column is Utf8 annotated as the arrow.json canonical extension type.
    let schema = reader.schema();
    assert_eq!(schema.fields().len(), 1);
    let field = schema.field(0);
    assert_eq!(field.name(), "document");
    assert_eq!(
        field.metadata().get("ARROW:extension:name").map(String::as_str),
        Some("arrow.json"),
    );

    // All 50 seeded documents come back.
    let batches: Vec<_> = reader.map(|b| b.expect("batch")).collect();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 50, "expected all seeded documents");
}

#[test]
#[ignore = "requires the local Cosmos emulator (run cosmos-client's seed example first)"]
fn native_struct_query_infers_schema() {
    let mut driver = CosmosDriver::default();
    let db = driver
        .new_database_with_opts([
            (
                OptionDatabase::Uri,
                OptionValue::String("https://localhost:8081/".into()),
            ),
            other("adbc.cosmos.auth", "key"),
            other("adbc.cosmos.account_key", EMULATOR_KEY),
            other("adbc.cosmos.database", "spikedb"),
        ])
        .expect("new_database");
    let mut conn = db.new_connection().expect("new_connection");
    let mut stmt = conn.new_statement().expect("new_statement");

    stmt.set_option(
        OptionStatement::Other("adbc.cosmos.container".into()),
        OptionValue::String("items".into()),
    )
    .expect("set container");
    stmt.set_option(
        OptionStatement::Other("adbc.cosmos.output".into()),
        OptionValue::String("struct".into()),
    )
    .expect("set output=struct");
    stmt.set_sql_query("SELECT * FROM c ORDER BY c.mergeOrder")
        .expect("set query");

    let reader = stmt.execute().expect("execute");

    // Inference produced real, named columns (not a single JSON blob).
    let schema = reader.schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    for expected in ["id", "pk", "mergeOrder", "name", "nested", "tags"] {
        assert!(names.contains(&expected), "inferred schema missing '{expected}' (got {names:?})");
    }

    let rows: usize = reader.map(|b| b.expect("batch").num_rows()).sum();
    assert_eq!(rows, 50);
}

#[cfg(feature = "variant")]
#[test]
#[ignore = "requires the local Cosmos emulator + --features variant (run seed first)"]
fn native_variant_query_returns_variant() {
    let mut driver = CosmosDriver::default();
    let db = driver
        .new_database_with_opts([
            (
                OptionDatabase::Uri,
                OptionValue::String("https://localhost:8081/".into()),
            ),
            other("adbc.cosmos.auth", "key"),
            other("adbc.cosmos.account_key", EMULATOR_KEY),
            other("adbc.cosmos.database", "spikedb"),
        ])
        .expect("new_database");
    let mut conn = db.new_connection().expect("new_connection");
    let mut stmt = conn.new_statement().expect("new_statement");

    stmt.set_option(
        OptionStatement::Other("adbc.cosmos.container".into()),
        OptionValue::String("items".into()),
    )
    .expect("set container");
    stmt.set_option(
        OptionStatement::Other("adbc.cosmos.output".into()),
        OptionValue::String("variant".into()),
    )
    .expect("set output=variant");
    stmt.set_sql_query("SELECT * FROM c").expect("set query");

    let reader = stmt.execute().expect("execute");
    let schema = reader.schema();
    assert_eq!(schema.fields().len(), 1);
    let field = schema.field(0);
    assert_eq!(field.name(), "document");
    assert_eq!(
        field.metadata().get("ARROW:extension:name").map(String::as_str),
        Some("arrow.parquet.variant"),
    );

    let rows: usize = reader.map(|b| b.expect("batch").num_rows()).sum();
    assert_eq!(rows, 50);
}
