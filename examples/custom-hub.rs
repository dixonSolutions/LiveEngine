//! A custom in-process hub with its own methods and DTOs, also hostable as a container.
use liveengine::hub::{HubConfig, HubHandler, HubRegistration, HubService};
use serde_json::{json, Value};
use std::net::SocketAddr;

#[derive(Default)]
struct CounterHub {
    count: u64,
}

impl HubHandler for CounterHub {
    fn call(&mut self, method: &str, args: Vec<Value>) -> Result<Value, String> {
        match method {
            "increment" => {
                let amount = args.first().and_then(Value::as_u64).unwrap_or(1);
                self.count = self.count.checked_add(amount).ok_or("counter overflow")?;
                Ok(json!({ "count": self.count }))
            }
            "read" => Ok(json!({ "count": self.count })),
            _ => Err("unknown counter method".into()),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let service = HubService::new(HubConfig::from_env()?)?;
    let registration: HubRegistration = serde_json::from_value(json!({
        "id": "counter",
        "mode": "internal",
        "scopes": { "kind": "custom-counter", "region": "au" }
    }))?;
    service
        .register_internal(registration, CounterHub::default())
        .await?;
    let bind = std::env::var("LIVEENGINE_BIND").unwrap_or_else(|_| "127.0.0.1:8787".into());
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    eprintln!("custom hub listening on {bind}");
    axum::serve(
        listener,
        service
            .router()
            .into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}
