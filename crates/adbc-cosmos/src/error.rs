//! Error helper wiring for driverbase.
//!
//! [`ErrorHelper`] carries the driver name into every error surfaced across the ADBC
//! boundary; all fallible code builds errors via `driverbase::error::ErrorHelper`
//! constructors (`not_implemented()`, `not_found()`, `internal(..)`, …) and finishes
//! with `.to_adbc()`.

#[derive(Clone, Copy, Debug)]
pub struct ErrorHelper {}

impl driverbase::error::ErrorHelper for ErrorHelper {
    const NAME: &'static str = "cosmos";
}
