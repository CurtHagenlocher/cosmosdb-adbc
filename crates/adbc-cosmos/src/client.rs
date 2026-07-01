//! Builds a `cosmos-client` handle from parsed database configuration.

use adbc_core::error::Result;
use cosmos_client::{CosmosClientHandle, Credential};
use driverbase::error::ErrorHelper as _;

use crate::database::DatabaseConfig;
use crate::error::ErrorHelper;

/// Construct a connected Cosmos client from database options. Credential selection:
/// connection string > account key (auth=key or a key is present) > Entra (default).
pub(crate) fn build_client(config: &DatabaseConfig) -> Result<CosmosClientHandle> {
    let credential = if let Some(connection_string) = &config.connection_string {
        Credential::ConnectionString(connection_string.clone())
    } else if config.auth.as_deref() == Some("key") || config.account_key.is_some() {
        let key = config.account_key.clone().ok_or_else(|| {
            ErrorHelper::internal("connect")
                .message("key auth selected but no account key was provided")
                .to_adbc()
        })?;
        Credential::Key(key)
    } else {
        Credential::Entra
    };

    let endpoint = config.endpoint.clone().unwrap_or_default();
    if endpoint.is_empty() && config.connection_string.is_none() {
        return Err(ErrorHelper::internal("connect")
            .message("endpoint (adbc.cosmos.endpoint) is required")
            .to_adbc());
    }

    CosmosClientHandle::connect(&endpoint, credential).map_err(|e| {
        ErrorHelper::internal("connect")
            .message(e.to_string())
            .to_adbc()
    })
}
