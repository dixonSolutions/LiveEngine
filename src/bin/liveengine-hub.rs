use std::{net::SocketAddr, process::ExitCode, time::Duration};

use liveengine::hub::{ping_hub, HubConfig, HubService};

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("ping") => run_ping(&args[1..]).await,
        Some("serve") | None => run_serve().await,
        Some("help") | Some("--help") | Some("-h") => {
            print_usage();
            Ok(())
        }
        Some("--version") | Some("-v") => {
            println!("liveengine-hub {}", liveengine::VERSION);
            Ok(())
        }
        Some(other) => Err(format!(
            "unknown command '{other}'; run 'liveengine-hub --help'"
        )),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("liveengine-hub: {error}");
            ExitCode::FAILURE
        }
    }
}

fn print_usage() {
    println!(
        "Usage:\n  liveengine-hub [serve]\n  liveengine-hub ping <url> [--samples N] [--timeout-ms N] [--json]"
    );
}

async fn run_serve() -> Result<(), String> {
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

async fn run_ping(args: &[String]) -> Result<(), String> {
    let mut address = None;
    let mut samples = 5usize;
    let mut timeout_ms = 3000u64;
    let mut json = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--samples" => {
                samples = args
                    .get(index + 1)
                    .ok_or("--samples requires a value")?
                    .parse()
                    .map_err(|_| "--samples must be an integer")?;
                index += 2;
            }
            "--timeout-ms" => {
                timeout_ms = args
                    .get(index + 1)
                    .ok_or("--timeout-ms requires a value")?
                    .parse()
                    .map_err(|_| "--timeout-ms must be an integer")?;
                index += 2;
            }
            "--json" => {
                json = true;
                index += 1;
            }
            value if value.starts_with('-') => {
                return Err(format!("unknown ping option '{value}'"));
            }
            value => {
                if address.is_some() {
                    return Err("ping accepts one hub base URL".into());
                }
                address = Some(value.to_string());
                index += 1;
            }
        }
    }
    let address = address.ok_or("ping requires a hub base URL")?;
    let stats = ping_hub(&address, samples, Duration::from_millis(timeout_ms)).await?;
    if json {
        println!(
            "{}",
            serde_json::to_string(&stats).map_err(|e| format!("could not encode result: {e}"))?
        );
    } else {
        println!(
            "hub {address}: min {:.3} ms, avg {:.3} ms, max {:.3} ms, jitter {:.3} ms ({} samples)",
            stats.min_ms,
            stats.avg_ms,
            stats.max_ms,
            stats.jitter_ms,
            stats.samples.len()
        );
    }
    Ok(())
}
