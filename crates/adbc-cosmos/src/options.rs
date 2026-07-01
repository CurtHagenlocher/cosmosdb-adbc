//! ADBC option keys recognized by the Cosmos driver, plus small value extractors.
//!
//! Custom keys arrive as `Option*::Other("adbc.cosmos.*")`; canonical ADBC keys
//! (e.g. `OptionDatabase::Uri`) are handled separately by each `Optionable` impl.

use adbc_core::error::Result;
use adbc_core::options::OptionValue;
use driverbase::error::ErrorHelper as _;

use crate::error::ErrorHelper;

// --- Database-level keys ---
/// Cosmos account endpoint URI (also accepted via the canonical `OptionDatabase::Uri`).
pub const ENDPOINT: &str = "adbc.cosmos.endpoint";
/// Authentication mode: `entra` | `key` | `connection_string`.
pub const AUTH: &str = "adbc.cosmos.auth";
/// Account key (also accepted via the canonical `OptionDatabase::Password`).
pub const ACCOUNT_KEY: &str = "adbc.cosmos.account_key";
/// Full Cosmos connection string (`AccountEndpoint=...;AccountKey=...;`).
pub const CONNECTION_STRING: &str = "adbc.cosmos.connection_string";
/// Default Cosmos database name (maps to the ADBC current catalog).
pub const DATABASE: &str = "adbc.cosmos.database";

// --- Statement-level keys ---
/// Query dialect: `native` (Cosmos SQL passthrough) | `datafusion` (ANSI SQL federation).
pub const DIALECT: &str = "adbc.cosmos.dialect";
/// Target container for the `native` dialect.
pub const CONTAINER: &str = "adbc.cosmos.container";
/// Result representation: `json` | `variant` | `struct`.
pub const OUTPUT: &str = "adbc.cosmos.output";
/// Number of documents to sample for `struct`-mode schema inference.
pub const SAMPLE_SIZE: &str = "adbc.cosmos.sample_size";

/// Extract a string option value, or error if the caller passed a non-string.
pub(crate) fn require_string(key: &str, value: OptionValue) -> Result<String> {
    match value {
        OptionValue::String(s) => Ok(s),
        _ => Err(ErrorHelper::internal("set_option")
            .format(format_args!("option '{key}' requires a string value"))
            .to_adbc()),
    }
}

/// Extract an integer option value, or error if the caller passed a non-integer.
pub(crate) fn require_int(key: &str, value: OptionValue) -> Result<i64> {
    match value {
        OptionValue::Int(i) => Ok(i),
        _ => Err(ErrorHelper::internal("set_option")
            .format(format_args!("option '{key}' requires an integer value"))
            .to_adbc()),
    }
}
