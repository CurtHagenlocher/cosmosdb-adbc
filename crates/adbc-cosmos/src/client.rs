//! Builds a `cosmos-client` handle from parsed database configuration.

use adbc_core::error::Result;
use cosmos_client::{CosmosClientHandle, Credential};
use driverbase::error::ErrorHelper as _;

use crate::database::DatabaseConfig;
use crate::error::ErrorHelper;

fn missing(field: &str, auth: &str) -> adbc_core::error::Error {
    ErrorHelper::invalid_argument()
        .message(format!("auth='{auth}' requires {field}"))
        .to_adbc()
}

/// Map database options to a [`Credential`].
///
/// A connection string, if present, always wins. Otherwise `adbc.cosmos.auth` selects the mode
/// (`key` / `entra` / `managed_identity` / `service_principal` / `workload_identity` /
/// `connection_string`); with no `auth` set, an account key (if provided) is used, else Entra
/// developer sign-in.
fn credential_from(config: &DatabaseConfig) -> Result<Credential> {
    if let Some(cs) = &config.connection_string {
        return Ok(Credential::ConnectionString(cs.clone()));
    }

    let require = |opt: &Option<String>, field: &str, auth: &str| {
        opt.clone().ok_or_else(|| missing(field, auth))
    };

    match config.auth.as_deref() {
        Some("key") => Ok(Credential::Key(require(
            &config.account_key,
            "adbc.cosmos.account_key",
            "key",
        )?)),
        Some("connection_string") => {
            Err(missing("adbc.cosmos.connection_string", "connection_string"))
        }
        Some("managed_identity") => Ok(Credential::ManagedIdentity {
            client_id: config.client_id.clone(),
        }),
        Some("service_principal") => Ok(Credential::ServicePrincipal {
            tenant_id: require(&config.tenant_id, "adbc.cosmos.tenant_id", "service_principal")?,
            client_id: require(&config.client_id, "adbc.cosmos.client_id", "service_principal")?,
            client_secret: require(
                &config.client_secret,
                "adbc.cosmos.client_secret",
                "service_principal",
            )?,
        }),
        Some("workload_identity") => Ok(Credential::WorkloadIdentity {
            tenant_id: config.tenant_id.clone(),
            client_id: config.client_id.clone(),
        }),
        Some("entra") => Ok(Credential::Entra),
        Some(other) => Err(ErrorHelper::invalid_argument()
            .message(format!(
                "unknown auth '{other}' (expected key | entra | managed_identity | \
                 service_principal | workload_identity | connection_string)"
            ))
            .to_adbc()),
        // No explicit auth: use an account key if one was supplied, else Entra developer sign-in.
        None => match &config.account_key {
            Some(key) => Ok(Credential::Key(key.clone())),
            None => Ok(Credential::Entra),
        },
    }
}

/// Construct a connected Cosmos client from database options.
pub(crate) fn build_client(config: &DatabaseConfig) -> Result<CosmosClientHandle> {
    let credential = credential_from(config)?;

    let endpoint = config.endpoint.clone().unwrap_or_default();
    if endpoint.is_empty() && config.connection_string.is_none() {
        return Err(ErrorHelper::internal("connect")
            .message("endpoint (adbc.cosmos.endpoint) is required")
            .to_adbc());
    }

    CosmosClientHandle::connect(&endpoint, credential).map_err(|e| {
        ErrorHelper::internal("connect").message(e.to_string()).to_adbc()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(auth: Option<&str>) -> DatabaseConfig {
        DatabaseConfig {
            endpoint: Some("https://acct.documents.azure.com/".into()),
            auth: auth.map(str::to_string),
            ..Default::default()
        }
    }

    #[test]
    fn connection_string_wins() {
        let mut c = config(Some("entra"));
        c.connection_string = Some("AccountEndpoint=x;AccountKey=y;".into());
        assert!(matches!(credential_from(&c).unwrap(), Credential::ConnectionString(_)));
    }

    #[test]
    fn default_prefers_key_then_entra() {
        // no auth, no key → Entra developer sign-in
        assert!(matches!(credential_from(&config(None)).unwrap(), Credential::Entra));
        // no auth, key present → Key
        let mut c = config(None);
        c.account_key = Some("k".into());
        assert!(matches!(credential_from(&c).unwrap(), Credential::Key(_)));
    }

    #[test]
    fn managed_identity_optional_client_id() {
        assert!(matches!(
            credential_from(&config(Some("managed_identity"))).unwrap(),
            Credential::ManagedIdentity { client_id: None }
        ));
        let mut c = config(Some("managed_identity"));
        c.client_id = Some("mi-client".into());
        assert!(matches!(
            credential_from(&c).unwrap(),
            Credential::ManagedIdentity { client_id: Some(id) } if id == "mi-client"
        ));
    }

    #[test]
    fn service_principal_requires_all_three() {
        assert!(credential_from(&config(Some("service_principal"))).is_err());
        let mut c = config(Some("service_principal"));
        c.tenant_id = Some("tenant".into());
        c.client_id = Some("client".into());
        assert!(credential_from(&c).is_err(), "still missing client_secret");
        c.client_secret = Some("secret".into());
        match credential_from(&c).unwrap() {
            Credential::ServicePrincipal { tenant_id, client_id, client_secret } => {
                assert_eq!(tenant_id, "tenant");
                assert_eq!(client_id, "client");
                assert_eq!(client_secret, "secret");
            }
            other => panic!("expected ServicePrincipal, got {other:?}"),
        }
    }

    #[test]
    fn workload_identity_ids_optional() {
        assert!(matches!(
            credential_from(&config(Some("workload_identity"))).unwrap(),
            Credential::WorkloadIdentity { tenant_id: None, client_id: None }
        ));
    }

    #[test]
    fn key_auth_requires_account_key() {
        assert!(credential_from(&config(Some("key"))).is_err());
    }

    #[test]
    fn unknown_auth_is_rejected() {
        assert!(credential_from(&config(Some("bogus"))).is_err());
    }
}
