//! Embed LiveEngine's hub router in an existing Axum service.
//!
//! Run with:
//!   LIVEENGINE_TOKEN="$(openssl rand -hex 32)" cargo run --example axum-service

use axum::{routing::get, Json, Router};
use liveengine::hub::{HubConfig, HubRegistration, HubService};
use serde_json::{json, Value};
use std::net::SocketAddr;

async fn app_health() -> Json<Value> {
    Json(json!({ "app": "ok" }))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let hubs = HubService::new(HubConfig::from_env()?)?;

    let registration: HubRegistration = serde_json::from_value(json!({
        "id": "ranked",
        "mode": "internal",
        "scopes": { "game": "chess", "region": "au" },
        "scopeSchema": {
            "type": "object",
            "required": ["region"],
            "properties": { "region": { "type": "string" } }
        }
    }))?;
    hubs.register_internal(registration, liveengine::Engine::new(0))
        .await?;

    // Mount the LiveEngine routes alongside your own application routes.
    let app = Router::new()
        .route("/api/health", get(app_health))
        .merge(hubs.router());

    let bind = std::env::var("LIVEENGINE_BIND").unwrap_or_else(|_| "127.0.0.1:8787".into());
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    eprintln!("Axum service listening on {bind}");
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}
