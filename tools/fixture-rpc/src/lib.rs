//! Deterministic JSON-RPC chains, served over real HTTP so the real binary can be pointed at them.
//!
//! No network, no secrets, identical on a fork. Three chains, one per job they back:
//!
//! - [`scripted::Scripted`]: one Transfer a block over blocks 1..=8, switchable live to the same
//!   headers with zero logs. The cross-nest adoption run, the eval board and the crash-point tests.
//! - [`footprint::Footprint`]: one contract, four Transfers a block, tip 20,000. The single-nest RAM
//!   budget, backfill throughput, and with `moving` the tip-lag measurement.
//! - [`multinest::Multinest`]: many contracts emitting every event of a real ABI, with a tip that
//!   moves once the backfill drains. The dense per-cursor RAM budget and point-read latency.
//!
//! None of them is a general chain simulator. Each answers the methods its job calls and refuses
//! the rest loudly, because an unimplemented method that answered `null` would look like an empty
//! chain, and an empty chain is how a gate passes having measured nothing.

pub mod footprint;
pub mod multinest;
pub mod scripted;

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread::JoinHandle;

use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::{json, Value};
use tokio::net::TcpListener;

/// Why a call produced no result.
pub enum Fault {
    /// A JSON-RPC error object, inside a 200.
    Rpc(i64, String),
    /// A request the chain cannot read at all: the whole HTTP request fails with a 500.
    Malformed,
}

pub trait Fixture: Send + Sync + 'static {
    /// One JSON-RPC request object, whole, so each chain reads `method` and `params` its own way.
    fn call(&self, req: &Value) -> Result<Value, Fault>;

    /// Anything that is not JSON-RPC, answered before the body is parsed as a call.
    fn control(&self, _method: &Method, _path: &str, _body: &[u8]) -> Option<(StatusCode, Value)> {
        None
    }
}

/// Serves `fixture` on `listener` until the future is dropped.
pub async fn serve<F: Fixture>(listener: TcpListener, fixture: Arc<F>) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let fixture = fixture.clone();
        tokio::spawn(async move {
            let svc = service_fn(move |req| handle(fixture.clone(), req));
            let _ = http1::Builder::new()
                .serve_connection(TokioIo::new(stream), svc)
                .await;
        });
    }
}

/// A fixture served on its own thread, stopped when this is dropped.
pub struct Running {
    addr: SocketAddr,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl Running {
    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    pub fn url(&self) -> String {
        format!("http://{}/", self.addr)
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Binds `bind` (port 0 picks a free one) and serves `fixture` in this process. The socket is
/// listening when this returns, so there is nothing to poll for.
pub fn spawn<F: Fixture>(fixture: F, bind: &str) -> std::io::Result<Running> {
    let listener = std::net::TcpListener::bind(bind)?;
    listener.set_nonblocking(true)?;
    let addr = listener.local_addr()?;
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let thread = std::thread::spawn(move || {
        runtime.block_on(async move {
            let listener = TcpListener::from_std(listener).expect("adopt the bound socket");
            let server = tokio::spawn(serve(listener, Arc::new(fixture)));
            let _ = stopped.await;
            server.abort();
        });
    });
    Ok(Running {
        addr,
        stop: Some(stop),
        thread: Some(thread),
    })
}

async fn handle<F: Fixture>(
    fixture: Arc<F>,
    req: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let method = req.method().clone();
    let path = req
        .uri()
        .path_and_query()
        .map_or_else(|| "/".to_string(), |p| p.as_str().to_string());
    let body = match req.into_body().collect().await {
        Ok(b) => b.to_bytes(),
        Err(_) => return Ok(empty(StatusCode::BAD_REQUEST)),
    };
    if let Some((status, v)) = fixture.control(&method, &path, &body) {
        return Ok(json_response(status, &v));
    }
    if method != Method::POST {
        return Ok(empty(StatusCode::NOT_IMPLEMENTED));
    }
    let Ok(parsed) = serde_json::from_slice::<Value>(&body) else {
        return Ok(empty(StatusCode::BAD_REQUEST));
    };
    let reply = match parsed {
        Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for it in &items {
                match answer(&*fixture, it) {
                    Some(v) => out.push(v),
                    None => return Ok(empty(StatusCode::INTERNAL_SERVER_ERROR)),
                }
            }
            Value::Array(out)
        }
        one => match answer(&*fixture, &one) {
            Some(v) => v,
            None => return Ok(empty(StatusCode::INTERNAL_SERVER_ERROR)),
        },
    };
    Ok(json_response(StatusCode::OK, &reply))
}

fn answer<F: Fixture>(fixture: &F, req: &Value) -> Option<Value> {
    let id = req.as_object()?.get("id").cloned().unwrap_or(Value::Null);
    match fixture.call(req) {
        Ok(result) => Some(json!({"jsonrpc": "2.0", "id": id, "result": result})),
        Err(Fault::Rpc(code, message)) => Some(json!({
            "jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message},
        })),
        Err(Fault::Malformed) => None,
    }
}

fn json_response(status: StatusCode, v: &Value) -> Response<Full<Bytes>> {
    let mut r = Response::new(Full::new(Bytes::from(v.to_string())));
    *r.status_mut() = status;
    r.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        hyper::header::HeaderValue::from_static("application/json"),
    );
    r
}

fn empty(status: StatusCode) -> Response<Full<Bytes>> {
    let mut r = Response::new(Full::new(Bytes::new()));
    *r.status_mut() = status;
    r
}

/// A hex quantity, with or without its `0x`.
pub(crate) fn hex_u64(v: &Value) -> Option<u64> {
    let s = v.as_str()?.trim();
    let digits = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    u64::from_str_radix(digits, 16).ok()
}

/// A count given as a JSON number or a decimal string.
pub(crate) fn int(v: &Value) -> Option<u64> {
    match v {
        Value::Number(n) => n.as_u64().or_else(|| n.as_f64().map(|f| f as u64)),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

pub(crate) fn hex(n: u64) -> String {
    format!("{n:#x}")
}

/// The name of a method nobody implemented, as the error message quotes it.
pub(crate) fn quoted(method: Option<&Value>) -> String {
    match method {
        None | Some(Value::Null) => "None".into(),
        Some(Value::String(s)) => format!("'{s}'"),
        Some(other) => other.to_string(),
    }
}

/// Whether a field counts as given: null, false, zero and empty do not.
pub(crate) fn truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64() != Some(0.0),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}
