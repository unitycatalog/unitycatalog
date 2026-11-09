use std::sync::Arc;

use axum::http::{header, HeaderName, HeaderValue, Method};
use axum::routing::{get, post};
use axum::{Json, Router};
use tower_http::cors::CorsLayer;
use tower_http::services::{ServeDir, ServeFile};
use tower_http::trace::TraceLayer;

pub mod config;
pub mod proxy;

pub use config::Config;

/// Shared handler state: a reused HTTP client and the loaded config.
#[derive(Clone)]
pub struct AppState {
    pub http: reqwest::Client,
    pub config: Arc<Config>,
}

/// Runtime config the SPA reads at GET /config. The runtime equivalent of the
/// current ui's build-time REACT_APP_*_AUTH_ENABLED flags.
async fn config_handler(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "authEnabled": state.config.auth_enabled,
        "googleClientId": state.config.google_client_id,
        "oktaEnabled": state.config.okta_enabled,
        "keycloakEnabled": state.config.keycloak_enabled,
    }))
}

async fn healthz() -> &'static str {
    "ok"
}

/// Builds the application router. The SPA (prod) is served from `web_dist` with
/// an index.html fallback so client-side routes deep-link correctly; in dev this
/// directory is absent and Vite serves the SPA, proxying RPCs + /config here.
pub fn app(state: AppState) -> Router {
    let index = state.config.web_dist.join("index.html");
    let spa = ServeDir::new(&state.config.web_dist).fallback(ServeFile::new(index));

    let mut router = Router::new()
        .route("/uc.v1.UnityProxyService/Call", post(proxy::call_handler))
        .route("/config", get(config_handler))
        .route("/healthz", get(healthz))
        .fallback_service(spa);

    if let Some(cors) = build_cors(&state.config.allowed_origins) {
        router = router.layer(cors);
    }

    router.layer(TraceLayer::new_for_http()).with_state(state)
}

/// CORS is only needed when the SPA is served from a different origin than the
/// bridge (uncommon). Same-origin (prod) and the Vite dev proxy need none, so an
/// empty allowlist yields no CORS layer. Cookie auth requires explicit origins
/// with credentials (never a wildcard).
fn build_cors(origins: &[String]) -> Option<CorsLayer> {
    if origins.is_empty() {
        return None;
    }
    let parsed: Vec<HeaderValue> = origins
        .iter()
        .filter_map(|o| o.parse::<HeaderValue>().ok())
        .collect();
    Some(
        CorsLayer::new()
            .allow_origin(parsed)
            .allow_methods([Method::GET, Method::POST])
            .allow_headers([
                header::CONTENT_TYPE,
                header::AUTHORIZATION,
                HeaderName::from_static("connect-protocol-version"),
            ])
            .allow_credentials(true),
    )
}
