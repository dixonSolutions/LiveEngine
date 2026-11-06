use std::{net::SocketAddr, process::ExitCode, time::Duration};

use liveengine::hub::{ping_hub, HubClient, HubConfig, HubService};

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("ping") => run_ping(&args[1..]).await,
        Some("request") => run_request(&args[1..]).await,
        Some("ticket") => run_ticket(&args[1..]).await,
        Some("watch-ticket") => run_watch_ticket(&args[1..]).await,
        Some("watch") => run_watch(&args[1..]).await,
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
        "Usage:\n  liveengine-hub [serve]\n  liveengine-hub ping <url> [--samples N] [--timeout-ms N] [--json]\n  liveengine-hub request <url> <method> <route> [json-body]\n  liveengine-hub ticket <url> <hub-id> <user-id> <room> [--path a/b] [--role peer|service] [--credential VALUE]\n  liveengine-hub watch-ticket <url> [--path a/b] [--depth N]\n  liveengine-hub watch <url> [--path a/b] [--depth N] [--count N]\n\nticket, watch-ticket and watch read the administrator token from LIVEENGINE_TOKEN.\nticket reads the credential for a hub that sets auth from LIVEENGINE_CREDENTIAL when --credential is not given.\nwatch prints one JSON message per line, the snapshot first, and stops after --count messages."
    );
}

async fn run_serve() -> Result<(), String> {
    let config = HubConfig::from_env()?;
    let bind: SocketAddr = std::env::var("LIVEENGINE_BIND")
        .unwrap_or_else(|_| "127.0.0.1:8787".into())
        .parse()
        .map_err(|e| format!("invalid LIVEENGINE_BIND: {e}"))?;
    let service = HubService::new(config)?;
    service.initialize_state().await?;
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

async fn run_request(args: &[String]) -> Result<(), String> {
    if !(3..=4).contains(&args.len()) {
        return Err("request requires URL, method, route and optional JSON body".into());
    }
    let token = std::env::var("LIVEENGINE_TOKEN").map_err(|_| "LIVEENGINE_TOKEN must be set")?;
    let client = HubClient::new(&args[0], token)?;
    let method = args[1].parse().map_err(|_| "invalid HTTP method")?;
    let body = args
        .get(3)
        .map(|s| serde_json::from_str(s).map_err(|_| "invalid JSON body"))
        .transpose()?;
    let result = client.request(method, &args[2], body).await?;
    println!(
        "{}",
        serde_json::to_string(&result).map_err(|e| e.to_string())?
    );
    Ok(())
}

/// Options shared by `ticket`, `watch-ticket` and `watch`.
struct LiveArgs {
    positional: Vec<String>,
    path: Vec<String>,
    depth: usize,
    role: String,
    credential: Option<String>,
    count: Option<usize>,
}

fn parse_live_args(args: &[String]) -> Result<LiveArgs, String> {
    let mut parsed = LiveArgs {
        positional: Vec::new(),
        path: Vec::new(),
        depth: 1,
        role: "peer".into(),
        credential: None,
        count: None,
    };
    let mut index = 0;
    while index < args.len() {
        let name = args[index].as_str();
        if !name.starts_with("--") {
            parsed.positional.push(args[index].clone());
            index += 1;
            continue;
        }
        let value = args
            .get(index + 1)
            .ok_or_else(|| format!("{name} requires a value"))?;
        match name {
            "--path" => {
                parsed.path = value
                    .split('/')
                    .filter(|segment| !segment.is_empty())
                    .map(str::to_owned)
                    .collect()
            }
            "--depth" => {
                parsed.depth = value
                    .parse()
                    .map_err(|_| "--depth must be an integer".to_string())?
            }
            "--role" => parsed.role = value.clone(),
            "--credential" => parsed.credential = Some(value.clone()),
            "--count" => {
                parsed.count = Some(
                    value
                        .parse()
                        .map_err(|_| "--count must be an integer".to_string())?,
                )
            }
            other => return Err(format!("unknown option '{other}'")),
        }
        index += 2;
    }
    Ok(parsed)
}

fn admin_client(address: &str) -> Result<HubClient, String> {
    let token = std::env::var("LIVEENGINE_TOKEN").map_err(|_| "LIVEENGINE_TOKEN must be set")?;
    HubClient::new(address, token)
}

async fn run_ticket(args: &[String]) -> Result<(), String> {
    let parsed = parse_live_args(args)?;
    let [address, hub, user, room] = parsed.positional.as_slice() else {
        return Err("ticket requires URL, hub id, user id and room".into());
    };
    if !matches!(parsed.role.as_str(), "peer" | "service") {
        return Err("--role must be peer or service".into());
    }
    let credential = parsed
        .credential
        .or_else(|| std::env::var("LIVEENGINE_CREDENTIAL").ok());
    let ticket = admin_client(address)?
        .ticket(
            &parsed.path,
            hub,
            user,
            room,
            &parsed.role,
            credential.as_deref(),
        )
        .await?;
    println!(
        "{}",
        serde_json::to_string(&ticket).map_err(|e| e.to_string())?
    );
    Ok(())
}

async fn run_watch_ticket(args: &[String]) -> Result<(), String> {
    let parsed = parse_live_args(args)?;
    let [address] = parsed.positional.as_slice() else {
        return Err("watch-ticket requires a hub base URL".into());
    };
    let ticket = admin_client(address)?
        .watch_ticket(&parsed.path, parsed.depth)
        .await?;
    println!(
        "{}",
        serde_json::to_string(&ticket).map_err(|e| e.to_string())?
    );
    Ok(())
}

async fn run_watch(args: &[String]) -> Result<(), String> {
    let parsed = parse_live_args(args)?;
    let [address] = parsed.positional.as_slice() else {
        return Err("watch requires a hub base URL".into());
    };
    let mut seen = 0usize;
    let limit = parsed.count;
    admin_client(address)?
        .watch(&parsed.path, parsed.depth, |message| {
            println!("{message}");
            seen += 1;
            limit.is_none_or(|limit| seen < limit)
        })
        .await
}
