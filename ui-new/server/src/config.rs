use std::env::VarError;

/// Runtime configuration, read entirely from the environment so the same build
/// can be pointed at any Unity Catalog server without a rebuild.
#[derive(Clone, Debug, PartialEq)]
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

/// Reads one variable the way `std::env::var` does, so tests can supply their own.
type Lookup<'a> = &'a dyn Fn(&str) -> Result<String, VarError>;

/// Reads `key`. An unset or exactly empty variable is `None`, since deployment
/// templates often render an unset value as an empty string. A whitespace-only
/// value is kept: `PORT` and the booleans reject it, and the string variables
/// use it as given.
fn env_value(env: Lookup<'_>, key: &str) -> Result<Option<String>, String> {
    match env(key) {
        Ok(value) if value.is_empty() => Ok(None),
        Ok(value) => Ok(Some(value)),
        Err(VarError::NotPresent) => Ok(None),
        Err(VarError::NotUnicode(_)) => Err(format!("{key} is not valid Unicode")),
    }
}

fn env_bool(env: Lookup<'_>, key: &str, default: bool) -> Result<bool, String> {
    env_value(env, key)?.map_or(Ok(default), |value| parse_bool(key, &value))
}

fn env_port(env: Lookup<'_>, key: &str, default: u16) -> Result<u16, String> {
    env_value(env, key)?.map_or(Ok(default), |value| parse_port(key, &value))
}

fn env_string(env: Lookup<'_>, key: &str, default: &str) -> Result<String, String> {
    Ok(env_value(env, key)?.unwrap_or_else(|| default.to_string()))
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

impl Config {
    /// Load configuration from the process environment. An unset or empty
    /// variable takes its default. An invalid `PORT` or boolean, or a value
    /// that is not valid Unicode, is an `Err` naming the variable.
    pub fn from_env() -> Result<Self, String> {
        Self::from_lookup(&|key: &str| std::env::var(key))
    }

    fn from_lookup(env: Lookup<'_>) -> Result<Self, String> {
        let allowed_origins = env_value(env, "ALLOWED_ORIGINS")?
            .map(|v| {
                v.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default();
        Ok(Config {
            port: env_port(env, "PORT", 8081)?,
            uc_server: env_string(env, "UC_SERVER", "http://localhost:8080")?,
            auth_enabled: env_bool(env, "UI_AUTH_ENABLED", false)?,
            google_client_id: env_string(env, "GOOGLE_CLIENT_ID", "")?,
            okta_enabled: env_bool(env, "OKTA_AUTH_ENABLED", false)?,
            keycloak_enabled: env_bool(env, "KEYCLOAK_AUTH_ENABLED", false)?,
            allowed_origins,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::env::VarError;
    use std::ffi::OsString;

    use super::{parse_bool, parse_port, Config};

    const KEYS: [&str; 7] = [
        "PORT",
        "UC_SERVER",
        "UI_AUTH_ENABLED",
        "GOOGLE_CLIENT_ID",
        "OKTA_AUTH_ENABLED",
        "KEYCLOAK_AUTH_ENABLED",
        "ALLOWED_ORIGINS",
    ];

    /// Builds a config from `vars`; every other variable is unset.
    fn config(vars: &[(&str, &str)]) -> Result<Config, String> {
        Config::from_lookup(&|key: &str| {
            vars.iter()
                .find(|(name, _)| *name == key)
                .map(|(_, value)| value.to_string())
                .ok_or(VarError::NotPresent)
        })
    }

    fn defaults() -> Config {
        Config {
            port: 8081,
            uc_server: "http://localhost:8080".to_string(),
            auth_enabled: false,
            google_client_id: String::new(),
            okta_enabled: false,
            keycloak_enabled: false,
            allowed_origins: Vec::new(),
        }
    }

    #[test]
    fn unset_and_empty_variables_take_their_defaults() {
        assert_eq!(config(&[]), Ok(defaults()));
        let empty: Vec<_> = KEYS.iter().map(|key| (*key, "")).collect();
        assert_eq!(config(&empty), Ok(defaults()));
    }

    #[test]
    fn reads_every_variable() {
        let config = config(&[
            ("PORT", "9000"),
            ("UC_SERVER", "https://uc.example.com"),
            ("UI_AUTH_ENABLED", "true"),
            ("GOOGLE_CLIENT_ID", "client-id"),
            ("OKTA_AUTH_ENABLED", "1"),
            ("KEYCLOAK_AUTH_ENABLED", "on"),
            ("ALLOWED_ORIGINS", "https://a.example, ,https://b.example"),
        ]);
        assert_eq!(
            config,
            Ok(Config {
                port: 9000,
                uc_server: "https://uc.example.com".to_string(),
                auth_enabled: true,
                google_client_id: "client-id".to_string(),
                okta_enabled: true,
                keycloak_enabled: true,
                allowed_origins: vec![
                    "https://a.example".to_string(),
                    "https://b.example".to_string()
                ],
            })
        );
    }

    #[test]
    fn rejects_invalid_values() {
        let not_a_port = "PORT must be a port number from 1 to 65535";
        let not_a_bool =
            |key: &str| format!("{key} must be one of true/false, 1/0, yes/no, or on/off");
        // (variable, value, error)
        let cases = [
            ("PORT", "0", not_a_port.to_string()),
            ("PORT", " ", not_a_port.to_string()),
            ("UI_AUTH_ENABLED", "treu", not_a_bool("UI_AUTH_ENABLED")),
            ("OKTA_AUTH_ENABLED", " ", not_a_bool("OKTA_AUTH_ENABLED")),
            (
                "KEYCLOAK_AUTH_ENABLED",
                "2",
                not_a_bool("KEYCLOAK_AUTH_ENABLED"),
            ),
        ];
        for (key, value, error) in cases {
            assert_eq!(config(&[(key, value)]), Err(error), "{key}={value:?}");
        }
    }

    #[test]
    fn rejects_values_that_are_not_unicode() {
        for key in KEYS {
            let config = Config::from_lookup(&|name: &str| {
                Err(if name == key {
                    VarError::NotUnicode(OsString::from("value"))
                } else {
                    VarError::NotPresent
                })
            });
            assert_eq!(config, Err(format!("{key} is not valid Unicode")), "{key}");
        }
    }

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
        for value in ["treu", " "] {
            assert_eq!(
                parse_bool("FLAG", value),
                Err("FLAG must be one of true/false, 1/0, yes/no, or on/off".to_string()),
                "{value:?}"
            );
        }
    }

    #[test]
    fn parses_ports_from_1_to_65535() {
        for (value, port) in [("1", 1), (" 8081 ", 8081), ("65535", 65535)] {
            assert_eq!(parse_port("PORT", value), Ok(port));
        }
        for value in [" ", "0", "65536", "-1", "80a"] {
            assert_eq!(
                parse_port("PORT", value),
                Err("PORT must be a port number from 1 to 65535".to_string()),
                "{value:?}"
            );
        }
    }
}
