//! ADBC driver for Azure Cosmos DB (NoSQL / Core SQL API).
//!
//! This crate implements the four `adbc_core` objects — [`CosmosDriver`],
//! [`database::CosmosDatabase`], [`connection::CosmosConnection`], and
//! [`statement::CosmosStatement`] — and (behind the `ffi` feature) exports the C ABI
//! entry point so the driver can be loaded by any ADBC driver manager.
//!
//! Phase 0 status: the object graph, option parsing, and `get_info` are wired up.
//! Query execution (`Statement::execute`) is not yet implemented — that arrives with
//! the `cosmos-client` transport crate in Phase 1.

mod batch_reader;
mod client;
mod connection;
mod database;
mod driver;
mod error;
mod inference;
mod metadata;
mod options;
mod output;
mod runtime;
mod statement;

pub use driver::CosmosDriver;

// Exports `AdbcDriverCosmosDbInit` (and the fallback `AdbcDriverInit`) as the C ABI
// entry points a driver manager loads by symbol.
#[cfg(feature = "ffi")]
adbc_ffi::export_driver!(AdbcDriverCosmosDbInit, driver::CosmosDriver);
