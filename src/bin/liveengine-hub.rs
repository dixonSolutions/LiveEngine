use std::{net::SocketAddr, process::ExitCode};

use liveengine::hub::{HubConfig, HubService};

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("liveengine-hub: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), String> {
    let config = HubConfig::from_env()?;
    let bind: SocketAddr = std::env::var("LIVEENGINE_BIND")
        .unwrap_or_else(|_| "127.0.0.1:8787".into())
        .parse()
        .map_err(|e| format!("invalid LIVEENGINE_BIND: {e}"))?;
    let service = HubService::new(config)?;
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|e| format!("could not bind {bind}: {e}"))?;
    eprintln!("liveengine-hub listening on {bind}");
    axum::serve(
        listener,
        service
            .router()
            .into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .map_err(|e| format!("HTTP server failed: {e}"))
}
