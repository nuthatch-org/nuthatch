//! [`Transport`] over the host's `fetch`, for a tab's worker and for Node (RFC-0075 §5).
//!
//! Every JS value is held in a `SendWrapper`: `Source` is `Send`, and the wrapper is sound only
//! because a tab's nest runs on one thread (§4). Touched from another thread it panics.

use crate::transport::{Reply, Transport, TransportError};
use send_wrapper::SendWrapper;
use serde_json::Value;
use std::time::Duration;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;

pub struct Fetch {
    timeout_ms: u32,
}

pub struct FetchReply {
    response: SendWrapper<web_sys::Response>,
    drained: bool,
}

fn global_fn(name: &str) -> Result<(JsValue, js_sys::Function), JsValue> {
    let global: JsValue = js_sys::global().into();
    let f = js_sys::Reflect::get(&global, &JsValue::from_str(name))?;
    Ok((global, f.dyn_into()?))
}

fn failed(e: JsValue) -> TransportError {
    let name = js_sys::Reflect::get(&e, &JsValue::from_str("name"))
        .ok()
        .and_then(|n| n.as_string());
    let message = js_sys::Reflect::get(&e, &JsValue::from_str("message"))
        .ok()
        .and_then(|m| m.as_string())
        .or_else(|| e.as_string())
        .unwrap_or_else(|| format!("{e:?}"));
    TransportError {
        // `AbortSignal.timeout` rejects with a `TimeoutError` DOMException, on the request or the body.
        timed_out: name.as_deref() == Some("TimeoutError"),
        error: anyhow::anyhow!("{message}"),
    }
}

impl Fetch {
    async fn post_js(&self, url: &str, body: &Value) -> Result<web_sys::Response, JsValue> {
        let init = web_sys::RequestInit::new();
        init.set_method("POST");
        init.set_body(&JsValue::from_str(&body.to_string()));
        let headers = js_sys::Object::new();
        js_sys::Reflect::set(
            &headers,
            &JsValue::from_str("content-type"),
            &JsValue::from_str("application/json"),
        )?;
        init.set_headers(&headers);
        let abort: JsValue =
            js_sys::Reflect::get(&js_sys::global(), &JsValue::from_str("AbortSignal"))?;
        let timeout_fn: js_sys::Function =
            js_sys::Reflect::get(&abort, &JsValue::from_str("timeout"))?.dyn_into()?;
        let signal: web_sys::AbortSignal = timeout_fn
            .call1(&abort, &JsValue::from(self.timeout_ms))?
            .dyn_into()?;
        init.set_signal(Some(&signal));
        let (global, fetch) = global_fn("fetch")?;
        let promise: js_sys::Promise = fetch
            .call2(&global, &JsValue::from_str(url), &init)?
            .dyn_into()?;
        JsFuture::from(promise).await?.dyn_into()
    }

    async fn bytes_js(response: &web_sys::Response) -> Result<Vec<u8>, JsValue> {
        let buf = JsFuture::from(response.array_buffer()?).await?;
        Ok(js_sys::Uint8Array::new(&buf).to_vec())
    }
}

#[async_trait::async_trait]
impl Transport for Fetch {
    type Reply = FetchReply;

    fn build(timeout: Duration) -> anyhow::Result<Self> {
        Ok(Self {
            timeout_ms: timeout.as_millis().min(u32::MAX as u128) as u32,
        })
    }

    async fn post(&self, url: &str, body: &Value) -> Result<FetchReply, TransportError> {
        let response =
            SendWrapper::new(async { self.post_js(url, body).await.map_err(failed) }).await?;
        Ok(FetchReply {
            response: SendWrapper::new(response),
            drained: false,
        })
    }
}

#[async_trait::async_trait]
impl Reply for FetchReply {
    fn status(&self) -> u16 {
        self.response.status()
    }

    fn status_line(&self) -> String {
        format!("{} {}", self.response.status(), self.response.status_text())
    }

    fn retry_after(&self) -> Option<String> {
        self.response.headers().get("retry-after").ok().flatten()
    }

    fn content_length(&self) -> Option<u64> {
        self.response
            .headers()
            .get("content-length")
            .ok()
            .flatten()?
            .parse()
            .ok()
    }

    async fn text(self) -> Result<String, TransportError> {
        let bytes = self.read_all().await?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }

    async fn json(self) -> Result<Value, TransportError> {
        let bytes = self.read_all().await?;
        serde_json::from_slice(&bytes).map_err(|e| TransportError {
            timed_out: false,
            error: anyhow::anyhow!("error decoding response body: {e}"),
        })
    }

    async fn chunk(&mut self) -> Result<Option<bytes::Bytes>, TransportError> {
        if self.drained {
            return Ok(None);
        }
        self.drained = true;
        Ok(Some(self.read_body().await?.into()))
    }
}

impl FetchReply {
    async fn read_all(self) -> Result<Vec<u8>, TransportError> {
        self.read_body().await
    }

    async fn read_body(&self) -> Result<Vec<u8>, TransportError> {
        let response = &self.response;
        SendWrapper::new(async move { Fetch::bytes_js(response).await.map_err(failed) }).await
    }
}
