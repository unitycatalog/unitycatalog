use std::net::SocketAddr;
use std::sync::Arc;

use tracing_subscriber::EnvFilter;
use uc_ui_bridge::{app, AppState, Config, UPSTREAM_TIMEOUT};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = Config::from_env().unwrap_or_else(|error| {
        tracing::error!("invalid server configuration: {error}");
        std::process::exit(1);
    });
    let addr: SocketAddr = ([0, 0, 0, 0], config.port).into();

    tracing::info!(
        uc_server = %config.uc_server,
        auth_enabled = config.auth_enabled,
        "Unity Catalog UI bridge listening on {addr}"
    );

    let state = AppState {
        http: reqwest::Client::builder()
            .timeout(UPSTREAM_TIMEOUT)
            .build()
            .expect("failed to build HTTP client"),
        config: Arc::new(config),
        upstream_timeout: UPSTREAM_TIMEOUT,
    };

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("failed to bind");
    axum::serve(listener, app(state))
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server error");
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutting down");
}
