//! The ADBC [`Database`] object: parses connection configuration from options.

use std::sync::Arc;

use adbc_core::error::Result;
use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
use adbc_core::{Database, Optionable};
use driverbase::error::ErrorHelper as _;

use crate::client::build_client;
use crate::connection::CosmosConnection;
use crate::error::ErrorHelper;
use crate::options;
use crate::runtime::Runtime;

/// Connection configuration parsed from database-level options, shared (read-only)
/// with every connection and statement created beneath this database.
#[derive(Debug, Default, Clone)]
pub struct DatabaseConfig {
    /// Cosmos account endpoint URI.
    pub endpoint: Option<String>,
    /// Authentication mode: `entra` | `managed_identity` | `service_principal` |
    /// `workload_identity` | `key` | `connection_string`.
    pub auth: Option<String>,
    /// Account key (secret; never returned via `get_option`).
    pub account_key: Option<String>,
    /// Full connection string (secret; never returned via `get_option`).
    pub connection_string: Option<String>,
    /// Entra tenant (directory) ID (service principal / workload identity).
    pub tenant_id: Option<String>,
    /// Entra client (application) ID (service principal, user-assigned MI / workload identity).
    pub client_id: Option<String>,
    /// Service-principal client secret (secret; never returned via `get_option`).
    pub client_secret: Option<String>,
    /// Default database name.
    pub database: Option<String>,
}

pub struct CosmosDatabase {
    config: DatabaseConfig,
    runtime: Arc<Runtime>,
}

impl CosmosDatabase {
    pub(crate) fn new(runtime: Arc<Runtime>) -> Self {
        Self {
            config: DatabaseConfig::default(),
            runtime,
        }
    }
}

impl Database for CosmosDatabase {
    type ConnectionType = CosmosConnection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        self.new_connection_with_opts(vec![])
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        // Constructing the client is cheap and offline; the first network call happens on
        // query execution.
        let client = Arc::new(build_client(&self.config)?);
        let mut connection = CosmosConnection::new(
            Arc::new(self.config.clone()),
            self.runtime.clone(),
            client,
        );
        for (key, value) in opts {
            connection.set_option(key, value)?;
        }
        Ok(connection)
    }
}

impl Optionable for CosmosDatabase {
    type Option = OptionDatabase;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match &key {
            // Canonical ADBC keys map onto Cosmos concepts.
            OptionDatabase::Uri => {
                self.config.endpoint = Some(options::require_string(options::ENDPOINT, value)?);
            }
            OptionDatabase::Password => {
                self.config.account_key = Some(options::require_string(options::ACCOUNT_KEY, value)?);
            }
            OptionDatabase::Other(k) => match k.as_str() {
                options::ENDPOINT => {
                    self.config.endpoint = Some(options::require_string(options::ENDPOINT, value)?);
                }
                options::AUTH => {
                    self.config.auth = Some(options::require_string(options::AUTH, value)?);
                }
                options::ACCOUNT_KEY => {
                    self.config.account_key =
                        Some(options::require_string(options::ACCOUNT_KEY, value)?);
                }
                options::CONNECTION_STRING => {
                    self.config.connection_string =
                        Some(options::require_string(options::CONNECTION_STRING, value)?);
                }
                options::TENANT_ID => {
                    self.config.tenant_id = Some(options::require_string(options::TENANT_ID, value)?);
                }
                options::CLIENT_ID => {
                    self.config.client_id = Some(options::require_string(options::CLIENT_ID, value)?);
                }
                options::CLIENT_SECRET => {
                    self.config.client_secret =
                        Some(options::require_string(options::CLIENT_SECRET, value)?);
                }
                options::DATABASE => {
                    self.config.database = Some(options::require_string(options::DATABASE, value)?);
                }
                _ => return Err(ErrorHelper::set_unknown_option(&key).to_adbc()),
            },
            _ => return Err(ErrorHelper::set_unknown_option(&key).to_adbc()),
        }
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        // Secrets (account key, connection string) are intentionally not readable back.
        let value = match &key {
            OptionDatabase::Uri => self.config.endpoint.clone(),
            OptionDatabase::Other(k) => match k.as_str() {
                options::ENDPOINT => self.config.endpoint.clone(),
                options::AUTH => self.config.auth.clone(),
                options::TENANT_ID => self.config.tenant_id.clone(),
                options::CLIENT_ID => self.config.client_id.clone(),
                options::DATABASE => self.config.database.clone(),
                _ => None,
            },
            _ => None,
        };
        value.ok_or_else(|| ErrorHelper::get_unknown_option(&key).to_adbc())
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
