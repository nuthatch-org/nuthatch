//! RFC-0077 S0: RFC-0075 S3's live cursor as a plain core module. It performs no I/O: `nh_step`
//! runs the follow until it needs the host, then returns the RPC requests to make or the time to
//! wait, and `nh_resume` hands back the answers. Its only imports are `nuthatch.now_ms` (through
//! the vendored web-time), `nuthatch.random_fill` and `nuthatch.log`.
//!
//! Every value crosses as JSON at a pointer and a length (§3.2). An export returns its output's
//! length, negative for an error message, and `nh_output` copies that output out.
#![cfg(target_arch = "wasm32")]

use nuthatch_core::backend::Bytes;
use nuthatch_core::rpc::RpcClient;
use nuthatch_core::transport::{Reply, Transport, TransportError};
use nuthatch_embed::cellar::Cellar;
use nuthatch_embed::derived::NestFiles;
use nuthatch_embed::tab::TabNest;
use nuthatch_embed::young;
use serde_json::{json, Value};
use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

#[link(wasm_import_module = "nuthatch")]
extern "C" {
    fn random_fill(ptr: *mut u8, len: usize);
    fn log(level: u32, ptr: *const u8, len: usize);
}

/// getrandom 0.3's and 0.4's custom backend, which `build.sh` selects. Both declare this symbol with
/// their own `Error`, a `NonZeroU32` newtype either way, so `Ok(())` is the same word to both.
#[no_mangle]
unsafe extern "Rust" fn __getrandom_v03_custom(
    dest: *mut u8,
    len: usize,
) -> Result<(), getrandom03::Error> {
    random_fill(dest, len);
    Ok(())
}

/// getrandom 0.2's, whose `js` feature the vendored copy turns into `custom` (`vendor.sh`).
#[no_mangle]
unsafe extern "Rust" fn __getrandom_custom(dest: *mut u8, len: usize) -> u32 {
    random_fill(dest, len);
    0
}

fn log_line(level: u32, s: &str) {
    unsafe { log(level, s.as_ptr(), s.len()) }
}

type Follow = Pin<Box<dyn Future<Output = anyhow::Result<Option<u64>>>>>;

struct Engine {
    nest: Arc<TabNest>,
    cellar: Arc<Cellar>,
    rpc: Arc<RpcClient<HostTransport>>,
    files: NestFiles,
    nid: String,
    follow: Option<Follow>,
    rolled: Option<u64>,
}

/// Requests the guest has made and the host has not answered, and answers not yet taken.
#[derive(Default)]
struct Exchange {
    next_id: u64,
    outbox: Vec<Value>,
    in_flight: VecDeque<u64>,
    answers: BTreeMap<u64, Value>,
    // A request's future can sit inside a `FuturesUnordered`, which polls only what was woken.
    wakers: BTreeMap<u64, Waker>,
}

/// The top-level waker: whether anything below asked to be polled again.
#[derive(Default)]
struct Woken(AtomicBool);

impl Wake for Woken {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Relaxed);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Relaxed);
    }
}

thread_local! {
    static ENGINE: RefCell<Option<Engine>> = const { RefCell::new(None) };
    static EXCHANGE: RefCell<Exchange> = RefCell::new(Exchange::default());
    static OUTPUT: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// [`Transport`] whose every POST is a request handed to the host.
struct HostTransport {
    deadline_ms: u64,
}

struct HostReply {
    status: u16,
    status_text: String,
    retry_after: Option<String>,
    body: Vec<u8>,
    drained: bool,
}

fn failed(msg: impl std::fmt::Display, timed_out: bool) -> TransportError {
    TransportError {
        timed_out,
        error: anyhow::anyhow!("{msg}"),
    }
}

#[async_trait::async_trait]
impl Transport for HostTransport {
    type Reply = HostReply;

    fn build(timeout: Duration) -> anyhow::Result<Self> {
        Ok(Self {
            deadline_ms: timeout.as_millis() as u64,
        })
    }

    async fn post(&self, url: &str, body: &Value) -> Result<HostReply, TransportError> {
        let id = EXCHANGE.with(|x| {
            let mut x = x.borrow_mut();
            let id = x.next_id;
            x.next_id += 1;
            x.outbox.push(json!({
                "id": id,
                "kind": "rpc",
                "endpoint": url,
                "body": body.to_string(),
                "deadline_ms": self.deadline_ms,
            }));
            id
        });
        let answer = std::future::poll_fn(|cx| {
            EXCHANGE.with(|x| {
                let mut x = x.borrow_mut();
                match x.answers.remove(&id) {
                    Some(a) => Poll::Ready(a),
                    None => {
                        x.wakers.insert(id, cx.waker().clone());
                        Poll::Pending
                    }
                }
            })
        })
        .await;
        if let Some(e) = answer.get("error") {
            let timed_out = answer["timed_out"].as_bool().unwrap_or(false);
            return Err(failed(e.as_str().unwrap_or("transport failed"), timed_out));
        }
        let status = answer["status"].as_u64().unwrap_or(0) as u16;
        Ok(HostReply {
            status,
            status_text: answer["status_text"].as_str().unwrap_or("").to_string(),
            retry_after: answer["retry_after"].as_str().map(str::to_string),
            body: answer["body"].as_str().unwrap_or("").as_bytes().to_vec(),
            drained: false,
        })
    }
}

#[async_trait::async_trait]
impl Reply for HostReply {
    fn status(&self) -> u16 {
        self.status
    }

    fn status_line(&self) -> String {
        format!("{} {}", self.status, self.status_text)
    }

    fn retry_after(&self) -> Option<String> {
        self.retry_after.clone()
    }

    fn content_length(&self) -> Option<u64> {
        Some(self.body.len() as u64)
    }

    async fn text(self) -> Result<String, TransportError> {
        Ok(String::from_utf8_lossy(&self.body).into_owned())
    }

    async fn json(self) -> Result<Value, TransportError> {
        serde_json::from_slice(&self.body)
            .map_err(|e| failed(format!("error decoding response body: {e}"), false))
    }

    async fn chunk(&mut self) -> Result<Option<bytes::Bytes>, TransportError> {
        if self.drained {
            return Ok(None);
        }
        self.drained = true;
        Ok(Some(std::mem::take(&mut self.body).into()))
    }
}

fn input(ptr: *mut u8, len: usize) -> Vec<u8> {
    if len == 0 {
        return Vec::new();
    }
    unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, len)) }.into_vec()
}

fn output(r: Result<Vec<u8>, String>) -> i32 {
    let (bytes, sign) = match r {
        Ok(b) => (b, 1),
        Err(e) => (e.into_bytes(), -1),
    };
    let n = bytes.len() as i32;
    OUTPUT.with(|o| *o.borrow_mut() = bytes);
    sign * n
}

fn json_out(r: Result<Value, String>) -> i32 {
    output(r.map(|v| v.to_string().into_bytes()))
}

#[no_mangle]
pub extern "C" fn nh_alloc(len: usize) -> *mut u8 {
    Box::into_raw(vec![0u8; len].into_boxed_slice()) as *mut u8
}

#[no_mangle]
pub extern "C" fn nh_free(ptr: *mut u8, len: usize) {
    drop(input(ptr, len));
}

/// Copies the last export's output to `ptr`.
///
/// # Safety
/// `ptr` must hold as many bytes as that export returned, as `nh_alloc` gives them.
#[no_mangle]
pub unsafe extern "C" fn nh_output(ptr: *mut u8) {
    OUTPUT.with(|o| {
        let o = o.borrow();
        unsafe { std::ptr::copy_nonoverlapping(o.as_ptr(), ptr, o.len()) }
    });
}

#[no_mangle]
pub extern "C" fn nh_abi() -> i32 {
    json_out(Ok(json!({"abi": 1, "capabilities": ["events"]})))
}

/// `{"nid", "contract", "start", "rpc": [endpoint names], "memory_mib"?}`. S0 has the byte-buffer
/// store only.
#[no_mangle]
pub extern "C" fn nh_open(ptr: *mut u8, len: usize) -> i32 {
    std::panic::set_hook(Box::new(|info| log_line(0, &format!("panic: {info}"))));
    json_out(open(&input(ptr, len)).map_err(|e| format!("{e:#}")))
}

fn open(config: &[u8]) -> anyhow::Result<Value> {
    let c: Value = serde_json::from_slice(config)?;
    let contract = c["contract"].as_str().unwrap_or_default().to_string();
    let start = c["start"].as_u64().unwrap_or(0);
    let rpc: Vec<String> = serde_json::from_value(c["rpc"].clone())?;
    let db = redb::Builder::new().create_with_backend(Bytes::default())?;
    let (mut nest, cellar) = young::open(db, &contract, start)?;
    if let Some(mib) = c["memory_mib"].as_u64() {
        nest.budget = nuthatch_embed::budget::TabBudget::mib(mib as u32);
        nest.budget.enforce();
    }
    let engine = Engine {
        nest: Arc::new(nest),
        cellar,
        rpc: Arc::new(RpcClient::new(rpc)?),
        files: NestFiles::new(),
        nid: c["nid"].as_str().unwrap_or("embedded").to_string(),
        follow: None,
        rolled: None,
    };
    ENGINE.with(|e| *e.borrow_mut() = Some(engine));
    Ok(json!({"storage": "bytes"}))
}

/// Advance until blocked: `{"idle": true}` once the cursor is at the tip, `{"requests": [...]}`
/// for the host to perform, or `{"wait_ms": n}`.
#[no_mangle]
pub extern "C" fn nh_step() -> i32 {
    json_out(ENGINE.with(|e| match e.borrow_mut().as_mut() {
        Some(engine) => step(engine),
        None => Err("nh_open first".into()),
    }))
}

fn step(engine: &mut Engine) -> Result<Value, String> {
    let follow = engine.follow.get_or_insert_with(|| {
        let (nest, rpc) = (engine.nest.clone(), engine.rpc.clone());
        Box::pin(async move { nest.follow(&*rpc).await })
    });
    let woken = Arc::new(Woken::default());
    let waker = Waker::from(woken.clone());
    let mut cx = Context::from_waker(&waker);
    loop {
        nuthatch_core::time::wake_sleepers();
        woken.0.store(false, Relaxed);
        match follow.as_mut().poll(&mut cx) {
            Poll::Ready(r) => {
                engine.follow = None;
                engine.rolled = r.map_err(|e| format!("{e:#}"))?;
                return Ok(json!({"idle": true}));
            }
            Poll::Pending => {}
        }
        let out = EXCHANGE.with(|x| {
            let mut x = x.borrow_mut();
            let out = std::mem::take(&mut x.outbox);
            for r in &out {
                let id = r["id"].as_u64().unwrap_or_default();
                x.in_flight.push_back(id);
            }
            out
        });
        if !out.is_empty() {
            return Ok(json!({"requests": out}));
        }
        if EXCHANGE.with(|x| !x.borrow().in_flight.is_empty()) {
            return Err("requests are unanswered: nh_resume first".into());
        }
        // A yield, or a sleep already due, is polled again at once.
        if woken.0.load(Relaxed) {
            continue;
        }
        match nuthatch_core::time::next_wake() {
            Some(until) => {
                let left = until.saturating_duration_since(nuthatch_core::time::deadline_now());
                if !left.is_zero() {
                    return Ok(json!({"wait_ms": left.as_millis() as u64}));
                }
            }
            None => return Err("the cursor is pending on nothing a host can answer".into()),
        }
    }
}

/// Answers to the last `requests`, one per request, in order: `{"status", "body", "status_text"?,
/// "retry_after"?}`, or `{"error", "timed_out"?}` where the host got no reply.
#[no_mangle]
pub extern "C" fn nh_resume(ptr: *mut u8, len: usize) -> i32 {
    json_out(resume(&input(ptr, len)))
}

fn resume(answers: &[u8]) -> Result<Value, String> {
    let answers: Vec<Value> = serde_json::from_slice(answers).map_err(|e| e.to_string())?;
    let wakers = EXCHANGE.with(|x| {
        let mut x = x.borrow_mut();
        if answers.len() != x.in_flight.len() {
            return Err(format!(
                "{} answers for {} requests",
                answers.len(),
                x.in_flight.len()
            ));
        }
        let mut wakers = Vec::new();
        for a in answers {
            let id = x.in_flight.pop_front().unwrap_or_default();
            x.answers.insert(id, a);
            wakers.extend(x.wakers.remove(&id));
        }
        Ok(wakers)
    })?;
    wakers.into_iter().for_each(Waker::wake);
    Ok(json!({}))
}

/// One request as the node's router answers it: `{"status", "content_type", "body"}`. It never
/// yields to the host, because everything a query reads is in storage.
#[no_mangle]
pub extern "C" fn nh_request(
    method: *mut u8,
    method_len: usize,
    target: *mut u8,
    target_len: usize,
    body: *mut u8,
    body_len: usize,
) -> i32 {
    let method = String::from_utf8_lossy(&input(method, method_len)).into_owned();
    let target = String::from_utf8_lossy(&input(target, target_len)).into_owned();
    let body = input(body, body_len);
    json_out(ENGINE.with(|e| {
        let e = e.borrow();
        let engine = e.as_ref().ok_or("nh_open first")?;
        let nest = nuthatch_embed::http::Nest {
            nest: &engine.nest,
            cellar: &engine.cellar,
            files: &engine.files,
            nid: Some(&engine.nid),
            version: young::node_version(),
            poll_interval_secs: None,
            log_window: young::MAINNET_LOG_WINDOW,
        };
        let mut answer =
            std::pin::pin!(nuthatch_embed::http::answer(&nest, &method, &target, &body));
        let mut cx = Context::from_waker(Waker::noop());
        for _ in 0..1_000_000 {
            if let Poll::Ready(a) = answer.as_mut().poll(&mut cx) {
                return Ok(json!({
                    "status": a.status,
                    "content_type": nuthatch_embed::http::content_type(&a),
                    "body": String::from_utf8_lossy(&a.body),
                }));
            }
        }
        Err("a query yielded a million times".to_string())
    }))
}

/// S0's probe, not part of the interface: what RFC-0075 S3's gate compares, as `LiveNest::follow`
/// reports it.
#[no_mangle]
pub extern "C" fn nh_report() -> i32 {
    json_out(ENGINE.with(|e| {
        let e = e.borrow();
        let engine = e.as_ref().ok_or("nh_open first")?;
        young::report(
            &engine.nest,
            &engine.cellar,
            &engine.rpc,
            engine.rolled,
            "bytes",
        )
        .map_err(|e| format!("{e:#}"))
    }))
}

/// The module's linear memory in bytes; it never shrinks, so it is the high-water mark.
#[no_mangle]
pub extern "C" fn nh_memory_bytes() -> f64 {
    nuthatch_embed::budget::linear_memory() as f64
}
