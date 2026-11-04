use crate::Engine;
use napi_derive::napi;
use serde_json::Value;

#[napi(js_name = "NativeEngineCore")]
pub struct NativeEngineCore {
    engine: Engine,
}

#[napi]
impl NativeEngineCore {
    #[napi(constructor)]
    pub fn new(seed: Option<f64>) -> Self {
        Self {
            engine: Engine::new(seed.unwrap_or(0.0) as u64),
        }
    }

    #[napi]
    pub fn dispatch(&mut self, method: String, args: Value) -> napi::Result<Value> {
        let args = args
            .as_array()
            .cloned()
            .ok_or_else(|| napi::Error::from_reason("args must be an array"))?;
        self.engine
            .dispatch(&method, args)
            .map_err(napi::Error::from_reason)
    }
}
