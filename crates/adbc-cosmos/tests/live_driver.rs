//! Live end-to-end test: drive the ADBC driver against the local Cosmos emulator and
//! confirm a native-dialect query returns an `arrow.json` result.
//!
//! Ignored by default. To run (seed via the cosmos-client example first):
//!   cargo run  -p cosmos-client --example seed
//!   cargo test -p adbc-cosmos  --test live_driver -- --ignored

use adbc_core::options::{ObjectDepth, OptionDatabase, OptionStatement, OptionValue};
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use adbc_cosmos::CosmosDriver;
use arrow_array::Array;

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

#[test]
#[ignore = "requires the local Cosmos emulator (run cosmos-client's seed example first)"]
fn datafusion_dialect_joins_across_containers() {
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
        OptionStatement::Other("adbc.cosmos.dialect".into()),
        OptionValue::String("datafusion".into()),
    )
    .expect("set dialect=datafusion");

    // A cross-container JOIN — Cosmos cannot do this natively; DataFusion joins the two
    // container scans locally. Each of the 50 items matches exactly one category on pk.
    stmt.set_sql_query(
        "SELECT i.name AS item_name, c.label AS category \
         FROM items i JOIN categories c ON i.pk = c.pk",
    )
    .expect("set query");

    let reader = stmt.execute().expect("execute");
    let schema = reader.schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(names.contains(&"item_name"), "missing item_name (got {names:?})");
    assert!(names.contains(&"category"), "missing category (got {names:?})");

    let rows: usize = reader.map(|b| b.expect("batch").num_rows()).sum();
    assert_eq!(rows, 50, "each item should join exactly one category");
}

#[test]
#[ignore = "requires the local Cosmos emulator (run cosmos-client's seed example first)"]
fn datafusion_dialect_pushes_filter_into_cosmos() {
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
        OptionStatement::Other("adbc.cosmos.dialect".into()),
        OptionValue::String("datafusion".into()),
    )
    .expect("set dialect=datafusion");

    // `mergeOrder = 50 - i` for i in 0..50, so mergeOrder ranges 1..=50. The predicate
    // `mergeOrder > 25` selects mergeOrder 26..=50 — exactly 25 rows. This WHERE is pushed
    // into the generated Cosmos SQL (supports_filters_pushdown → Exact); DataFusion keeps
    // no residual filter.
    // `mergeOrder` is camelCase, so it must be double-quoted or DataFusion lowercases it.
    stmt.set_sql_query(r#"SELECT id, "mergeOrder" FROM items WHERE "mergeOrder" > 25"#)
        .expect("set query");

    let reader = stmt.execute().expect("execute");
    let schema = reader.schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(names.contains(&"mergeOrder"), "missing mergeOrder (got {names:?})");

    let batches: Vec<_> = reader.map(|b| b.expect("batch")).collect();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 25, "expected the mergeOrder > 25 subset");

    // Every returned row must actually satisfy the predicate (guards against a dropped
    // filter silently returning the whole container).
    for batch in &batches {
        let idx = batch.schema().index_of("mergeOrder").expect("mergeOrder column");
        let col = batch
            .column(idx)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .expect("mergeOrder is Int64");
        for r in 0..col.len() {
            assert!(col.value(r) > 25, "row leaked past the pushed filter: {}", col.value(r));
        }
    }
}

#[test]
#[ignore = "requires the local Cosmos emulator (run cosmos-client's seed example first)"]
fn datafusion_dialect_folds_count_and_avg() {
    use arrow_array::{Float64Array, Int64Array};

    // `mergeOrder` is 1..=50 across the 50 seeded items, so COUNT(*) = 50, COUNT(*) with the
    // `> 25` filter = 25, and AVG(mergeOrder) = (1+…+50)/50 = 25.5. Each folds to a single
    // `SELECT VALUE …` round-trip (see cosmos-datafusion::pushdown).
    fn scalar_i64<C: Connection>(conn: &mut C, sql: &str) -> i64 {
        let mut stmt = conn.new_statement().expect("new_statement");
        stmt.set_option(
            OptionStatement::Other("adbc.cosmos.dialect".into()),
            OptionValue::String("datafusion".into()),
        )
        .expect("set dialect");
        stmt.set_sql_query(sql).expect("set query");
        let batches: Vec<_> = stmt.execute().expect("execute").map(|b| b.expect("batch")).collect();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1, "a bare aggregate returns exactly one row");
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 count")
            .value(0)
    }

    let mut conn = open_connection();
    assert_eq!(scalar_i64(&mut conn, "SELECT COUNT(*) FROM items"), 50);
    assert_eq!(
        scalar_i64(&mut conn, r#"SELECT COUNT(*) FROM items WHERE "mergeOrder" > 25"#),
        25,
    );

    // AVG is opt-in: enable the toggle, then confirm the folded average.
    let mut stmt = conn.new_statement().expect("new_statement");
    stmt.set_option(
        OptionStatement::Other("adbc.cosmos.dialect".into()),
        OptionValue::String("datafusion".into()),
    )
    .expect("set dialect");
    stmt.set_option(
        OptionStatement::Other("adbc.cosmos.pushdown.avg".into()),
        OptionValue::String("on".into()),
    )
    .expect("set pushdown.avg=on");
    stmt.set_sql_query(r#"SELECT AVG("mergeOrder") FROM items"#).expect("set query");
    let batches: Vec<_> = stmt.execute().expect("execute").map(|b| b.expect("batch")).collect();
    let avg = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64 avg")
        .value(0);
    assert!((avg - 25.5).abs() < 1e-9, "AVG(mergeOrder) should be 25.5, got {avg}");
}

/// Open a connection to the seeded emulator database.
fn open_connection() -> impl Connection {
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
    db.new_connection().expect("new_connection")
}

#[test]
#[ignore = "requires the local Cosmos emulator (run cosmos-client's seed example first)"]
fn datafusion_dialect_reuses_cached_schema() {
    // Two datafusion-dialect queries on the SAME connection: the first populates the
    // per-connection schema cache, the second should reuse it. Both must return correctly.
    fn run_df<C: Connection>(conn: &mut C, sql: &str) -> usize {
        let mut stmt = conn.new_statement().expect("new_statement");
        stmt.set_option(
            OptionStatement::Other("adbc.cosmos.dialect".into()),
            OptionValue::String("datafusion".into()),
        )
        .expect("set dialect");
        stmt.set_sql_query(sql).expect("set query");
        let reader = stmt.execute().expect("execute");
        reader.map(|b| b.expect("batch").num_rows()).sum()
    }

    let mut conn = open_connection();
    let sql = r#"SELECT id, "mergeOrder" FROM items WHERE "mergeOrder" > 40"#;
    let first = run_df(&mut conn, sql);
    let second = run_df(&mut conn, sql);
    assert_eq!(first, 10, "mergeOrder > 40 selects 10 rows");
    assert_eq!(second, first, "cached-schema run must match the first");
}

#[test]
#[ignore = "requires the local Cosmos emulator (run cosmos-client's seed example first)"]
fn struct_inference_knobs_decimal_and_epoch() {
    use arrow_schema::{DataType, TimeUnit};

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

    for (k, v) in [
        ("adbc.cosmos.container", "items"),
        ("adbc.cosmos.output", "struct"),
        ("adbc.cosmos.number_inference", "decimal"),
        ("adbc.cosmos.decimal", "20,4"),
        ("adbc.cosmos.epoch_fields", "_ts:s"),
    ] {
        stmt.set_option(OptionStatement::Other(k.into()), OptionValue::String(v.into()))
            .expect("set option");
    }
    stmt.set_sql_query("SELECT * FROM c").expect("set query");

    let reader = stmt.execute().expect("execute");
    let schema = reader.schema();
    // `value` is a JSON float → Decimal128(20,4); `mergeOrder` is integral → stays Int64;
    // `_ts` (Cosmos epoch seconds) → Timestamp(Second).
    assert_eq!(
        schema.field_with_name("value").unwrap().data_type(),
        &DataType::Decimal128(20, 4)
    );
    assert_eq!(
        schema.field_with_name("mergeOrder").unwrap().data_type(),
        &DataType::Int64
    );
    assert_eq!(
        schema.field_with_name("_ts").unwrap().data_type(),
        &DataType::Timestamp(TimeUnit::Second, None)
    );
    let rows: usize = reader.map(|b| b.expect("batch").num_rows()).sum();
    assert_eq!(rows, 50);
}

#[test]
#[ignore = "requires the local Cosmos emulator (run cosmos-client's seed example first)"]
fn datafusion_dialect_handles_heterogeneous_container() {
    // The datafusion provider infers/decodes `mixed` whose `val` field conflicts (number /
    // string / object) — it must not crash (val is carried as Utf8).
    fn run_df<C: Connection>(conn: &mut C, sql: &str) -> usize {
        let mut stmt = conn.new_statement().expect("new_statement");
        stmt.set_option(
            OptionStatement::Other("adbc.cosmos.dialect".into()),
            OptionValue::String("datafusion".into()),
        )
        .expect("set dialect");
        stmt.set_sql_query(sql).expect("set query");
        stmt.execute()
            .expect("execute")
            .map(|b| b.expect("batch").num_rows())
            .sum()
    }
    let mut conn = open_connection();
    assert_eq!(run_df(&mut conn, "SELECT id, val FROM mixed"), 4);
}

#[test]
#[ignore = "requires the local Cosmos emulator (run cosmos-client's seed example first)"]
fn struct_heterogeneous_field_as_string() {
    use arrow_schema::DataType;

    let mut conn = open_connection();
    let mut stmt = conn.new_statement().expect("new_statement");
    for (k, v) in [
        ("adbc.cosmos.container", "mixed"),
        ("adbc.cosmos.output", "struct"),
        ("adbc.cosmos.heterogeneous", "string"),
    ] {
        stmt.set_option(OptionStatement::Other(k.into()), OptionValue::String(v.into()))
            .expect("set option");
    }
    stmt.set_sql_query("SELECT * FROM c").expect("set query");

    let reader = stmt.execute().expect("execute");
    let schema = reader.schema();
    // The top-level conflicting `val` field widens to Utf8 (a naive decode would crash here).
    assert_eq!(schema.field_with_name("val").unwrap().data_type(), &DataType::Utf8);
    // The *nested* conflict `meta.v` is normalized to Utf8 inside the struct.
    let DataType::Struct(meta) = schema.field_with_name("meta").unwrap().data_type() else {
        panic!("meta should be a Struct");
    };
    let v = meta.iter().find(|f| f.name() == "v").expect("meta.v");
    assert_eq!(v.data_type(), &DataType::Utf8);
    let rows: usize = reader.map(|b| b.expect("batch").num_rows()).sum();
    assert_eq!(rows, 4);
}

#[cfg(feature = "variant")]
#[test]
#[ignore = "requires the local Cosmos emulator + --features variant (run seed first)"]
fn struct_heterogeneous_field_as_variant() {
    let mut conn = open_connection();
    let mut stmt = conn.new_statement().expect("new_statement");
    for (k, v) in [
        ("adbc.cosmos.container", "mixed"),
        ("adbc.cosmos.output", "struct"),
        ("adbc.cosmos.heterogeneous", "variant"),
    ] {
        stmt.set_option(OptionStatement::Other(k.into()), OptionValue::String(v.into()))
            .expect("set option");
    }
    stmt.set_sql_query("SELECT * FROM c").expect("set query");

    let reader = stmt.execute().expect("execute");
    let schema = reader.schema();
    // The conflicting `val` field is carried as a self-describing Variant column.
    let val = schema.field_with_name("val").unwrap();
    assert_eq!(
        val.metadata().get("ARROW:extension:name").map(String::as_str),
        Some("arrow.parquet.variant"),
    );
    let rows: usize = reader.map(|b| b.expect("batch").num_rows()).sum();
    assert_eq!(rows, 4);
}

#[test]
#[ignore = "requires the local Cosmos emulator (run cosmos-client's seed example first)"]
fn struct_inference_knobs_recurse_into_nested() {
    use arrow_schema::DataType;

    let mut conn = open_connection();
    let mut stmt = conn.new_statement().expect("new_statement");
    for (k, v) in [
        ("adbc.cosmos.container", "items"),
        ("adbc.cosmos.output", "struct"),
        ("adbc.cosmos.number_inference", "decimal"),
        ("adbc.cosmos.decimal", "20,4"),
        ("adbc.cosmos.infer_temporal", "on"),
    ] {
        stmt.set_option(OptionStatement::Other(k.into()), OptionValue::String(v.into()))
            .expect("set option");
    }
    stmt.set_sql_query("SELECT * FROM c").expect("set query");

    let reader = stmt.execute().expect("execute");
    let schema = reader.schema();
    // `nested` is a struct: `ratio` (nested float) → Decimal128, `day` (nested ISO date) → Date32,
    // `k` (nested int) stays Int64.
    let DataType::Struct(nested) = schema.field_with_name("nested").unwrap().data_type() else {
        panic!("nested should be a Struct");
    };
    let field = |name: &str| nested.iter().find(|f| f.name() == name).unwrap().data_type().clone();
    assert_eq!(field("ratio"), DataType::Decimal128(20, 4));
    assert_eq!(field("day"), DataType::Date32);
    assert_eq!(field("k"), DataType::Int64);
    let rows: usize = reader.map(|b| b.expect("batch").num_rows()).sum();
    assert_eq!(rows, 50);
}

#[test]
#[ignore = "requires the local Cosmos emulator (run cosmos-client's seed example first)"]
fn metadata_get_table_types_lists_table() {
    let conn = open_connection();
    let reader = conn.get_table_types().expect("get_table_types");
    let types: Vec<String> = reader
        .flat_map(|b| {
            let b = b.expect("batch");
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .expect("Utf8 table_type")
                .clone();
            (0..col.len()).map(move |i| col.value(i).to_string())
        })
        .collect();
    assert_eq!(types, vec!["table"]);
}

#[test]
#[ignore = "requires the local Cosmos emulator (run cosmos-client's seed example first)"]
fn metadata_get_table_schema_infers_container() {
    let conn = open_connection();
    // catalog defaults to the connection's current database (spikedb).
    let schema = conn
        .get_table_schema(None, None, "items")
        .expect("get_table_schema");
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    for expected in ["id", "pk", "mergeOrder", "name"] {
        assert!(names.contains(&expected), "schema missing '{expected}' (got {names:?})");
    }
}

#[test]
#[ignore = "requires the local Cosmos emulator (run cosmos-client's seed example first)"]
fn metadata_get_objects_lists_catalog_and_containers() {
    let conn = open_connection();
    let reader = conn
        .get_objects(ObjectDepth::All, None, None, None, None, None)
        .expect("get_objects");
    // Flatten the whole nested result to a debug string and assert the key names appear —
    // a light check that catalog (database), tables (containers), and columns all populate.
    // (The Python validation harness navigates the structure precisely.)
    let mut found_catalog = false;
    for batch in reader {
        let batch = batch.expect("batch");
        let catalogs = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .expect("catalog_name Utf8");
        for i in 0..catalogs.len() {
            if !catalogs.is_null(i) && catalogs.value(i) == "spikedb" {
                found_catalog = true;
            }
        }
    }
    assert!(found_catalog, "get_objects did not list the 'spikedb' catalog");
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
