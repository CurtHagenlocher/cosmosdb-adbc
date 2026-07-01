//! The ADBC [`Connection`] object.
//!
//! Phase 0: `get_info` is implemented via driverbase; the remaining metadata and
//! transaction methods return "not implemented" until the transport lands in Phase 1.

use std::collections::HashSet;
use std::sync::Arc;

use adbc_core::error::Result;
use adbc_core::options::{InfoCode, ObjectDepth, OptionConnection, OptionValue};
use adbc_core::{Connection, Optionable};
use arrow_array::RecordBatchReader;
use arrow_schema::Schema;
use driverbase::error::ErrorHelper as _;

use crate::database::DatabaseConfig;
use crate::error::ErrorHelper;
use crate::options;
use crate::statement::CosmosStatement;

pub struct CosmosConnection {
    config: Arc<DatabaseConfig>,
    /// The database (ADBC catalog) this connection is scoped to.
    current_database: Option<String>,
}

impl CosmosConnection {
    pub(crate) fn new(config: Arc<DatabaseConfig>) -> Self {
        let current_database = config.database.clone();
        Self {
            config,
            current_database,
        }
    }
}

impl Connection for CosmosConnection {
    type StatementType = CosmosStatement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        Ok(CosmosStatement::new(
            self.config.clone(),
            self.current_database.clone(),
        ))
    }

    fn cancel(&mut self) -> Result<()> {
        Err(ErrorHelper::not_implemented().message("cancel").to_adbc())
    }

    fn get_info(&self, codes: Option<HashSet<InfoCode>>) -> Result<Box<dyn RecordBatchReader + Send>> {
        let mut registry = driverbase::InfoRegistry::new();
        registry.add_string(InfoCode::DriverName, "ADBC Cosmos DB Driver");
        registry.add_string(InfoCode::DriverVersion, concat!("v", env!("CARGO_PKG_VERSION")));
        registry.add_string(InfoCode::DriverArrowVersion, "v58");
        registry.add_string(InfoCode::VendorName, "Azure Cosmos DB");
        Ok(Box::new(registry.get_info(codes).build()))
    }

    fn get_objects(
        &self,
        _depth: ObjectDepth,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _table_type: Option<Vec<&str>>,
        _column_name: Option<&str>,
    ) -> Result<Box<dyn RecordBatchReader + Send>> {
        Err(ErrorHelper::not_implemented()
            .message("get_objects")
            .to_adbc())
    }

    fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: &str,
    ) -> Result<Schema> {
        Err(ErrorHelper::not_implemented()
            .message("get_table_schema")
            .to_adbc())
    }

    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send>> {
        Err(ErrorHelper::not_implemented()
            .message("get_table_types")
            .to_adbc())
    }

    fn get_statistic_names(&self) -> Result<Box<dyn RecordBatchReader + Send>> {
        Err(ErrorHelper::not_implemented()
            .message("get_statistic_names")
            .to_adbc())
    }

    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> Result<Box<dyn RecordBatchReader + Send>> {
        Err(ErrorHelper::not_implemented()
            .message("get_statistics")
            .to_adbc())
    }

    fn commit(&mut self) -> Result<()> {
        Err(ErrorHelper::not_implemented().message("commit").to_adbc())
    }

    fn rollback(&mut self) -> Result<()> {
        Err(ErrorHelper::not_implemented()
            .message("rollback")
            .to_adbc())
    }

    fn read_partition(
        &self,
        _partition: impl AsRef<[u8]>,
    ) -> Result<Box<dyn RecordBatchReader + Send>> {
        Err(ErrorHelper::not_implemented()
            .message("read_partition")
            .to_adbc())
    }
}

impl Optionable for CosmosConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match &key {
            OptionConnection::CurrentCatalog => {
                self.current_database =
                    Some(options::require_string(options::DATABASE, value)?);
            }
            OptionConnection::Other(k) if k.as_str() == options::DATABASE => {
                self.current_database =
                    Some(options::require_string(options::DATABASE, value)?);
            }
            _ => return Err(ErrorHelper::set_unknown_option(&key).to_adbc()),
        }
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match &key {
            OptionConnection::CurrentCatalog => self
                .current_database
                .clone()
                .ok_or_else(|| ErrorHelper::get_unknown_option(&key).to_adbc()),
            _ => Err(ErrorHelper::get_unknown_option(&key).to_adbc()),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        Err(ErrorHelper::get_unknown_option(&key).to_adbc())
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        Err(ErrorHelper::get_unknown_option(&key).to_adbc())
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        Err(ErrorHelper::get_unknown_option(&key).to_adbc())
    }
}
