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
/// Entra tenant (directory) ID — for `service_principal` / `workload_identity`.
pub const TENANT_ID: &str = "adbc.cosmos.tenant_id";
/// Entra client (application) ID — for `service_principal` and user-assigned
/// `managed_identity` / `workload_identity`.
pub const CLIENT_ID: &str = "adbc.cosmos.client_id";
/// Service-principal client secret (secret; never returned via `get_option`).
pub const CLIENT_SECRET: &str = "adbc.cosmos.client_secret";
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
/// Numeric fidelity for `struct` inference: `float64` (default) | `decimal` (§3.5).
pub const NUMBER_INFERENCE: &str = "adbc.cosmos.number_inference";
/// `precision,scale` (e.g. `38,9`) used when `number_inference=decimal`.
pub const DECIMAL: &str = "adbc.cosmos.decimal";
/// Fallback for type-conflicting `struct` fields: `string` (currently the only mode).
pub const HETEROGENEOUS: &str = "adbc.cosmos.heterogeneous";
/// Infer `Date`/`Timestamp` from ISO-8601 strings in `struct` mode: `off` (default) | `on`.
pub const INFER_TEMPORAL: &str = "adbc.cosmos.infer_temporal";
/// Comma list of fields to read as epoch timestamps, each `name:s` or `name:ms`.
pub const EPOCH_FIELDS: &str = "adbc.cosmos.epoch_fields";
/// Fold `COUNT(*)` to `SELECT VALUE COUNT(1)` in the `datafusion` dialect: `on` (default) | `off`.
pub const PUSHDOWN_COUNT: &str = "adbc.cosmos.pushdown.count";
/// Fold `AVG(col)` to `SELECT VALUE AVG(col)` in the `datafusion` dialect: `off` (default) | `on`.
/// Off by default — Cosmos aggregate null semantics may differ from ANSI SQL (see design §3.2).
pub const PUSHDOWN_AVG: &str = "adbc.cosmos.pushdown.avg";
/// Push a single-column numeric `ORDER BY` into the engine (`datafusion` dialect): `on` (default)
/// | `off`. Only nulls-smallest, numeric keys are pushed; anything else sorts locally (§3.2).
pub const PUSHDOWN_SORT: &str = "adbc.cosmos.pushdown.sort";
/// Also push a **multi**-column `ORDER BY` (`datafusion` dialect): `off` (default) | `on`. Needs a
/// Cosmos composite index in production; the analog of ODBC `EnableSortPassdownForMultipleColumns`.
pub const PUSHDOWN_MULTI_SORT: &str = "adbc.cosmos.pushdown.multi_sort";

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
