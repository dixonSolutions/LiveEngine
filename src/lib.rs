//! Live pairing and hub hosting. Embed `hub::HubService::router()` in an Axum API,
//! or run the `liveengine-hub` binary with the same contract.
pub mod engine;
#[cfg(feature = "host")]
pub mod hub;
#[cfg(feature = "node")]
mod node;
pub mod rules;

pub use engine::Engine;
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
