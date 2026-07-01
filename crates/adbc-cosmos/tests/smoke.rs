//! Phase 0 smoke test: exercises the full ADBC object graph in-process (no FFI, no
//! network) to prove the traits, option parsing, and `get_info` are wired correctly.

use adbc_core::options::{
    InfoCode, OptionDatabase, OptionStatement, OptionValue,
};
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use adbc_cosmos::CosmosDriver;

#[test]
fn driver_object_graph_and_options() {
    let mut driver = CosmosDriver::default();

    // Database options: canonical Uri + custom auth key.
    let db = driver
        .new_database_with_opts([
            (OptionDatabase::Uri, OptionValue::String("https://acct.documents.azure.com:443/".into())),
            (OptionDatabase::Other("adbc.cosmos.auth".into()), OptionValue::String("entra".into())),
            (OptionDatabase::Other("adbc.cosmos.database".into()), OptionValue::String("mydb".into())),
        ])
        .expect("new_database_with_opts");

    // Endpoint reads back; the account key never would (secret).
    assert_eq!(
        db.get_option_string(OptionDatabase::Uri).unwrap(),
        "https://acct.documents.azure.com:443/"
    );

    let mut conn = db.new_connection().expect("new_connection");

    // get_info returns a non-empty Arrow stream.
    let reader = conn
        .get_info(Some([InfoCode::DriverName].into_iter().collect()))
        .expect("get_info");
    let rows: usize = reader
        .map(|b| b.expect("batch").num_rows())
        .sum();
    assert!(rows >= 1, "get_info should yield at least one row");

    let mut stmt = conn.new_statement().expect("new_statement");

    // Statement dialect/output round-trip through the option surface.
    stmt.set_option(
        OptionStatement::Other("adbc.cosmos.dialect".into()),
        OptionValue::String("datafusion".into()),
    )
    .unwrap();
    stmt.set_option(
        OptionStatement::Other("adbc.cosmos.output".into()),
        OptionValue::String("struct".into()),
    )
    .unwrap();
    assert_eq!(
        stmt.get_option_string(OptionStatement::Other("adbc.cosmos.dialect".into())).unwrap(),
        "datafusion"
    );
    assert_eq!(
        stmt.get_option_string(OptionStatement::Other("adbc.cosmos.output".into())).unwrap(),
        "struct"
    );

    // A bad enum value is rejected.
    assert!(stmt
        .set_option(
            OptionStatement::Other("adbc.cosmos.output".into()),
            OptionValue::String("yaml".into()),
        )
        .is_err());

    // Query text is accepted; execution is not implemented yet (Phase 1).
    stmt.set_sql_query("SELECT * FROM c").unwrap();
    assert!(stmt.execute().is_err(), "execute is a Phase 1 feature");
}

#[test]
fn unknown_option_is_rejected() {
    let mut driver = CosmosDriver::default();
    let result = driver.new_database_with_opts([(
        OptionDatabase::Other("adbc.cosmos.nonsense".into()),
        OptionValue::String("x".into()),
    )]);
    assert!(result.is_err(), "unknown option keys must error");
}
