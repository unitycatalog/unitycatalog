use std::path::PathBuf;

/// Runtime configuration, read entirely from the environment so the same image
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
    /// Extra CORS origins to allow. Empty means same-origin only (the prod
    /// deployment, where the bridge also serves the SPA).
    pub allowed_origins: Vec<String>,
    /// Directory of the built SPA to serve (prod). Missing in dev, where Vite
    /// serves the SPA and proxies RPCs here.
    pub web_dist: PathBuf,
}

fn env_bool(key: &str, default: bool) -> bool {
    match std::env::var(key) {
        Ok(v) => matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => default,
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
    pub fn from_env() -> Self {
        let port = std::env::var("PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8081);
        let allowed_origins = std::env::var("ALLOWED_ORIGINS")
            .ok()
            .map(|v| {
                v.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default();
        Config {
            port,
            uc_server: env_string("UC_SERVER", "http://localhost:8080"),
            auth_enabled: env_bool("UI_AUTH_ENABLED", false),
            google_client_id: env_string("GOOGLE_CLIENT_ID", ""),
            okta_enabled: env_bool("OKTA_AUTH_ENABLED", false),
            keycloak_enabled: env_bool("KEYCLOAK_AUTH_ENABLED", false),
            allowed_origins,
            web_dist: PathBuf::from(env_string("WEB_DIST", "../web/dist")),
        }
    }
}
