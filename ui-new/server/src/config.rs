/// Runtime configuration, read entirely from the environment so the same build
/// can be pointed at any Unity Catalog server without a rebuild.
#[derive(Clone, Debug)]
pub struct Config {
    /// TCP port the bridge listens on.
    pub port: u16,
    /// Base URL of the Unity Catalog Java server the bridge proxies to.
    pub uc_server: String,
    /// Whether auth is enabled. Surfaced to the SPA at GET /config so it knows
    /// whether to gate behind login. When false the SPA renders directly.
    pub auth_enabled: bool,
    /// Google Identity Services client id; empty disables the Google button.
    pub google_client_id: String,
    pub okta_enabled: bool,
    pub keycloak_enabled: bool,
    /// Extra CORS origins to allow. Empty means same-origin proxying only.
    pub allowed_origins: Vec<String>,
}

/// Reads `key`, treating an unset variable as `None`.
fn env_value(key: &str) -> Result<Option<String>, String> {
    match std::env::var(key) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(format!("{key} is not valid Unicode: {error}")),
    }
}

fn env_bool(key: &str, default: bool) -> Result<bool, String> {
    env_value(key)?.map_or(Ok(default), |value| parse_bool(key, &value))
}

fn env_port(key: &str, default: u16) -> Result<u16, String> {
    env_value(key)?.map_or(Ok(default), |value| parse_port(key, &value))
}

fn parse_port(key: &str, value: &str) -> Result<u16, String> {
    match value.trim().parse::<u16>() {
        Ok(port) if port > 0 => Ok(port),
        _ => Err(format!("{key} must be a port number from 1 to 65535")),
    }
}

fn parse_bool(key: &str, value: &str) -> Result<bool, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(format!(
            "{key} must be one of true/false, 1/0, yes/no, or on/off"
        )),
    }
}

fn env_string(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_string())
}

impl Config {
    /// Load configuration from the process environment, applying defaults.
    pub fn from_env() -> Result<Self, String> {
        let allowed_origins = std::env::var("ALLOWED_ORIGINS")
            .ok()
            .map(|v| {
                v.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default();
        Ok(Config {
            port: env_port("PORT", 8081)?,
            uc_server: env_string("UC_SERVER", "http://localhost:8080"),
            auth_enabled: env_bool("UI_AUTH_ENABLED", false)?,
            google_client_id: env_string("GOOGLE_CLIENT_ID", ""),
            okta_enabled: env_bool("OKTA_AUTH_ENABLED", false)?,
            keycloak_enabled: env_bool("KEYCLOAK_AUTH_ENABLED", false)?,
            allowed_origins,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_bool, parse_port};

    #[test]
    fn parses_explicit_boolean_values() {
        for value in ["true", "1", "yes", "on", " TRUE "] {
            assert_eq!(parse_bool("FLAG", value), Ok(true));
        }
        for value in ["false", "0", "no", "off", " FALSE "] {
            assert_eq!(parse_bool("FLAG", value), Ok(false));
        }
    }

    #[test]
    fn rejects_unknown_boolean_values() {
        assert_eq!(
            parse_bool("FLAG", "treu"),
            Err("FLAG must be one of true/false, 1/0, yes/no, or on/off".to_string())
        );
    }

    #[test]
    fn parses_ports_from_1_to_65535() {
        for (value, port) in [("1", 1), (" 8081 ", 8081), ("65535", 65535)] {
            assert_eq!(parse_port("PORT", value), Ok(port));
        }
        for value in ["", "0", "65536", "-1", "80a"] {
            assert_eq!(
                parse_port("PORT", value),
                Err("PORT must be a port number from 1 to 65535".to_string()),
                "{value:?}"
            );
        }
    }
}
