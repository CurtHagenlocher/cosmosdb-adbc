//! The top-level ADBC [`Driver`] object.

use std::sync::Arc;

use adbc_core::Driver;
use adbc_core::Optionable;
use adbc_core::error::Result;
use adbc_core::options::{OptionDatabase, OptionValue};
use driverbase::error::ErrorHelper as _;

use crate::database::CosmosDatabase;
use crate::error::ErrorHelper;
use crate::runtime::Runtime;

/// Entry point of the driver. Constructs [`CosmosDatabase`] instances.
///
/// Must implement `Default` for the `export_driver!` FFI macro.
#[derive(Default)]
pub struct CosmosDriver {}

impl Driver for CosmosDriver {
    type DatabaseType = CosmosDatabase;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        self.new_database_with_opts(vec![])
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        let runtime = Arc::new(Runtime::new_multi_thread().map_err(|e| {
            ErrorHelper::internal("runtime")
                .message(e.to_string())
                .to_adbc()
        })?);
        let mut database = CosmosDatabase::new(runtime);
        for (key, value) in opts {
            database.set_option(key, value)?;
        }
        Ok(database)
    }
}
