//! RFC-0077: the event engine as a plain core module, `abi: 1`. `embed/README.md` is the interface;
//! this is its implementation. The module performs no I/O: `nh_step` runs until it needs the host,
//! then returns the requests to perform or the time to wait, and `nh_resume` hands back the answers.
#![cfg(target_arch = "wasm32")]

mod host_file;

use host_file::HostFile;
use nuthatch_core::backend::Bytes;
use nuthatch_core::redb_store::Store;
use nuthatch_core::rpc::RpcClient;
use nuthatch_core::transport::{Reply, Transport, TransportError};
use nuthatch_embed::budget::TabBudget;
use nuthatch_embed::cellar::Cellar;
use nuthatch_embed::derived::NestFiles;
use nuthatch_embed::mirror::{self, Hydrated, MirrorConfig, MirrorHost};
use nuthatch_embed::narrow::{self, FilterSpec};
use nuthatch_embed::nest::{NestSpec, CONFIG_FILE};
use nuthatch_embed::tab::{TabNest, Verified};
use nuthatch_embed::verified::{VerifiedHead, Verifier};
use nuthatch_embed::young;
use serde::Deserialize;
use serde_json::{json, Value};
use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

/// The interface version `nh_abi` reports. Any change to an export's or import's shape bumps it;
/// `embed/tests/wasmtime-host` holds each version's shapes and fails a module that differs.
const ABI: u32 = 1;

/// The store's file under the nest's storage, as `data/<nid>/` holds it on a node.
const STORE_FILE: &str = "nuthatch.redb";
/// Where a mirror's segments are appended (RFC-0077 S4).
const PACK_FILE: &str = "mirror.pack";

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

/// getrandom 0.2's, whose `js` feature our fork turns into `custom`.
#[no_mangle]
unsafe extern "Rust" fn __getrandom_custom(dest: *mut u8, len: usize) -> u32 {
    random_fill(dest, len);
    0
}

fn log_line(level: u32, s: &str) {
    unsafe { log(level, s.as_ptr(), s.len()) }
}

/// Every export's error: `{"fault": name, "message": text}` at a negative length.
#[derive(Debug)]
struct Fault {
    name: &'static str,
    message: String,
}

impl std::fmt::Display for Fault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.name, self.message)
    }
}

impl std::error::Error for Fault {}

fn fault(name: &'static str, message: impl std::fmt::Display) -> Fault {
    Fault {
        name,
        message: message.to_string(),
    }
}

/// `e` as the fault it carries, or as `name` when it carries none.
fn fault_of(name: &'static str, e: anyhow::Error) -> Fault {
    match e.downcast::<Fault>() {
        Ok(f) => f,
        Err(e) => fault(name, format!("{e:#}")),
    }
}

fn not_open() -> Fault {
    fault(
        "not_open",
        "nh_open first, and nh_step until the store is open",
    )
}

#[derive(Deserialize, Clone, Copy, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
enum Storage {
    #[default]
    Bytes,
    Host,
}

impl Storage {
    fn name(self) -> &'static str {
        match self {
            Storage::Bytes => "bytes",
            Storage::Host => "host",
        }
    }
}

/// `nh_open`'s argument. Unknown keys are refused, so a host never believes a setting took.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    #[serde(default = "default_nid")]
    nid: String,
    /// RFC-0075's young contract, when `files` holds no `nuthatch.toml`.
    contract: Option<String>,
    start: Option<u64>,
    rpc: Vec<String>,
    /// RFC-0077 S4: hydrate the nest's sealed history from a published mirror first.
    mirror: Option<MirrorConfig>,
    /// Whether `nh_step` follows the chain past what the store holds; true unless set.
    follow: Option<bool>,
    /// RFC-0077 S5: seal only under a finalized block the host verified, asked for by request.
    #[serde(default)]
    verified: bool,
    #[serde(default)]
    storage: Storage,
    #[serde(default)]
    fresh: bool,
    memory_mib: Option<u32>,
    poll_interval_secs: Option<u64>,
    #[serde(default)]
    files: NestFiles,
    /// RFC-0077 S7: fetch only the logs these name, one `eth_getLogs` per value.
    filters: Option<Vec<FilterSpec>>,
    /// Blocks per `eth_getLogs`, in place of the chain's registered window.
    window: Option<u64>,
    /// RFC-0077 S7: index no further than this block, so the nest answers as of it.
    until: Option<u64>,
}

fn default_nid() -> String {
    "embedded".into()
}

type Follow = Pin<Box<dyn Future<Output = anyhow::Result<Option<u64>>>>>;
type Opening = Pin<Box<dyn Future<Output = anyhow::Result<Engine>>>>;

struct Engine {
    nest: Arc<TabNest>,
    verifier: Option<Arc<Verifier>>,
    follows: bool,
    hydrated: Option<Hydrated>,
    cellar: Arc<Cellar>,
    rpc: Arc<RpcClient<HostTransport>>,
    files: NestFiles,
    nid: String,
    poll_interval_secs: Option<u64>,
    storage: Storage,
    bytes: Option<Bytes>,
    follow: Option<Follow>,
    rolled: Option<u64>,
}

enum State {
    /// Storage mode `host`: the store opens through requests the host performs.
    Opening(Opening),
    Open(Box<Engine>),
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
    static STATE: RefCell<Option<State>> = const { RefCell::new(None) };
    static EXCHANGE: RefCell<Exchange> = RefCell::new(Exchange::default());
    static OUTPUT: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// Hand `request` to the host at the next `nh_step` and wait for its answer.
async fn ask(mut request: Value) -> Value {
    let id = EXCHANGE.with(|x| {
        let mut x = x.borrow_mut();
        let id = x.next_id;
        x.next_id += 1;
        request["id"] = id.into();
        x.outbox.push(request);
        id
    });
    std::future::poll_fn(|cx| {
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
    .await
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
        let answer = ask(json!({
            "kind": "rpc",
            "endpoint": url,
            "body": body.to_string(),
            "deadline_ms": self.deadline_ms,
        }))
        .await;
        if let Some(e) = answer.get("error") {
            let timed_out = answer["timed_out"].as_bool().unwrap_or(false);
            return Err(failed(e.as_str().unwrap_or("transport failed"), timed_out));
        }
        Ok(HostReply {
            status: answer["status"].as_u64().unwrap_or(0) as u16,
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

fn output(r: Result<Vec<u8>, Fault>) -> i32 {
    let (bytes, sign) = match r {
        Ok(b) => (b, 1),
        Err(f) => (
            json!({"fault": f.name, "message": f.message})
                .to_string()
                .into_bytes(),
            -1,
        ),
    };
    let n = bytes.len() as i32;
    OUTPUT.with(|o| *o.borrow_mut() = bytes);
    sign * n
}

fn json_out(r: Result<Value, Fault>) -> i32 {
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
    json_out(Ok(json!({
        "abi": ABI,
        "capabilities": ["events"],
        "storage": ["bytes", "host"],
        "engine": young::node_version(),
    })))
}

#[no_mangle]
pub extern "C" fn nh_open(ptr: *mut u8, len: usize) -> i32 {
    std::panic::set_hook(Box::new(|info| log_line(0, &format!("panic: {info}"))));
    json_out(open(&input(ptr, len)))
}

fn open(raw: &[u8]) -> Result<Value, Fault> {
    if STATE.with(|s| s.borrow().is_some()) {
        return Err(fault(
            "bad_call",
            "this instance has a nest already; one instance runs one nest",
        ));
    }
    let c: Config = serde_json::from_slice(raw).map_err(|e| fault("bad_config", e))?;
    let spec = spec_of(&c)?;
    let budget = c.memory_mib.map(TabBudget::mib).unwrap_or_default();
    budget.enforce();
    if c.mirror.is_some() {
        mirror::admit(&c.files, budget).map_err(|e| fault_of("open_failed", e))?;
    }
    let storage = c.storage;
    let state = match storage {
        Storage::Bytes => {
            if c.fresh {
                return Err(fault(
                    "bad_config",
                    "`fresh` is for storage `host`; a byte buffer starts empty",
                ));
            }
            let bytes = Bytes::default();
            let db = redb::Builder::new()
                .create_with_backend(bytes.clone())
                .map_err(|e| fault("open_failed", e))?;
            let store = Store::from_db(db, None).map_err(|e| fault("open_failed", e))?;
            let cellar = Cellar::over(store.clone()).map_err(|e| fault_of("open_failed", e))?;
            let engine = build(c, spec, store, Arc::new(cellar), Some(bytes), None)
                .map_err(|e| fault_of("open_failed", e))?;
            State::Open(Box::new(engine))
        }
        Storage::Host => State::Opening(Box::pin(open_host(c, spec))),
    };
    STATE.with(|s| *s.borrow_mut() = Some(state));
    Ok(json!({"storage": storage.name()}))
}

/// The nest `c` opens: its `nuthatch.toml`, or RFC-0075's young contract. Refused here, before the
/// host is asked for anything, when the two are mixed or a mode cannot do what is asked.
fn spec_of(c: &Config) -> Result<NestSpec, Fault> {
    let bad = |m: &str| Err(fault("bad_config", m));
    let mut spec = match (c.files.contains_key(CONFIG_FILE), &c.contract, c.start) {
        (true, None, None) => {
            NestSpec::from_files(&c.files).map_err(|e| fault_of("bad_config", e))?
        }
        (false, Some(contract), Some(start)) => {
            NestSpec::erc20(contract, start).map_err(|e| fault_of("bad_config", e))?
        }
        (true, ..) => return bad("`contract` and `start` are for a nest with no nuthatch.toml"),
        (false, ..) => {
            return bad("a nest needs `files` with a nuthatch.toml, or `contract` and `start`")
        }
    };
    if let Some(m) = &c.mirror {
        if c.storage != Storage::Host {
            return bad(
                "a mirror's history goes to the host's storage: `mirror` needs storage `host`",
            );
        }
        if c.contract.is_some() {
            return bad("`mirror` hydrates a nest from its nuthatch.toml, and this one has none");
        }
        if m.postcard.is_some() && c.follow == Some(true) {
            return bad(
                "a postcard pins what the publisher had sealed, so a pinned nest does not follow",
            );
        }
    }
    if c.mirror.is_none() {
        spec.start_block().map_err(|e| fault_of("bad_config", e))?;
    }
    if let Some(filters) = &c.filters {
        if c.mirror.is_some() {
            return bad("a mirror holds a nest's whole history, and `filters` select part of it");
        }
        if c.contract.is_some() {
            return bad("`filters` name contracts by their alias in a nuthatch.toml");
        }
        spec.narrowed = Some(narrow::build(&spec, filters).map_err(|e| fault_of("bad_config", e))?);
    }
    spec.until = c.until;
    match c.window {
        Some(0) => return bad("`window` is blocks per getLogs, at least one"),
        Some(w) => spec.chain.log_window = w,
        None => {}
    }
    Ok(spec)
}

/// A file opened through the host, as a handle.
async fn open_file(name: &str) -> anyhow::Result<i32> {
    let opened = ask(json!({"kind": "open", "name": name})).await;
    if let Some(e) = opened.get("error") {
        let fault_name = if opened["locked"] == true {
            "locked"
        } else {
            "storage"
        };
        return Err(fault(fault_name, format!("{name}: {e}")).into());
    }
    Ok(opened["handle"]
        .as_i64()
        .and_then(|h| i32::try_from(h).ok())
        .ok_or_else(|| fault("storage", format!("open answered {opened}, not a handle")))?)
}

/// A mirror's GETs, performed by the host: text answered whole, segments appended to the pack.
struct HostMirror {
    pack: i32,
}

fn got(answer: &Value, url: &str) -> anyhow::Result<Option<()>> {
    if let Some(e) = answer.get("error") {
        anyhow::bail!("GET {url}: {}", e.as_str().unwrap_or("failed"));
    }
    match answer["status"].as_u64() {
        Some(200) => Ok(Some(())),
        Some(404) => Ok(None),
        status => anyhow::bail!("GET {url}: status {}", status.unwrap_or(0)),
    }
}

#[async_trait::async_trait(?Send)]
impl MirrorHost for HostMirror {
    async fn text(&self, url: &str, max_bytes: u64) -> anyhow::Result<Option<String>> {
        let a = ask(json!({"kind": "get", "url": url, "max_bytes": max_bytes})).await;
        Ok(got(&a, url)?.map(|()| a["body"].as_str().unwrap_or("").to_string()))
    }

    async fn append(&self, url: &str, max_bytes: u64) -> anyhow::Result<Option<(u64, u64)>> {
        let a =
            ask(json!({"kind": "get", "url": url, "max_bytes": max_bytes, "append": self.pack}))
                .await;
        if got(&a, url)?.is_none() {
            return Ok(None);
        }
        match (a["offset"].as_u64(), a["length"].as_u64()) {
            (Some(at), Some(len)) => Ok(Some((at, len))),
            _ => anyhow::bail!("GET {url} into the pack answered {a}, not an offset and a length"),
        }
    }

    async fn rest(&self, ms: u64) {
        nuthatch_core::time::sleep(Duration::from_millis(ms)).await;
    }
}

/// The host's verified finalized block, asked for once a follow (RFC-0077 S5).
struct HostHead;

#[async_trait::async_trait(?Send)]
impl VerifiedHead for HostHead {
    async fn finalized(&self) -> anyhow::Result<(u64, String)> {
        let a = ask(json!({"kind": "verified_finalized"})).await;
        if let Some(e) = a.get("error") {
            anyhow::bail!("{}", e.as_str().unwrap_or("failed"));
        }
        match (a["number"].as_u64(), a["hash"].as_str()) {
            (Some(n), Some(h)) => Ok((n, h.to_string())),
            _ => anyhow::bail!("verified_finalized answered {a}, not a number and a hash"),
        }
    }
}

/// Storage mode `host`: clear the nest's files if asked, then open the store, each through the host.
async fn open_host(c: Config, spec: NestSpec) -> anyhow::Result<Engine> {
    if c.fresh {
        let listed = ask(json!({"kind": "list", "prefix": ""})).await;
        let names: Vec<String> = match listed.get("error") {
            Some(e) => return Err(fault("storage", format!("list: {e}")).into()),
            None => serde_json::from_value(listed["names"].clone())
                .map_err(|e| fault("storage", format!("list's names: {e}")))?,
        };
        for name in names {
            let removed = ask(json!({"kind": "remove", "name": name})).await;
            if let Some(e) = removed.get("error") {
                return Err(fault("storage", format!("remove {name}: {e}")).into());
            }
        }
    }
    let handle = open_file(STORE_FILE).await?;
    let db = redb::Builder::new().create_with_backend(HostFile::new(handle, STORE_FILE))?;
    let store = Store::from_db(db, None)?;
    let Some(m) = c.mirror.clone() else {
        let cellar = Arc::new(Cellar::over(store.clone())?);
        return build(c, spec, store, cellar, None, None);
    };
    let pack = open_file(PACK_FILE).await?;
    let cellar = Arc::new(Cellar::over_with_pack(
        store.clone(),
        Some(Arc::new(HostFile::new(pack, PACK_FILE))),
    )?);
    let hydrated = if mirror::indexed(&store)? {
        None
    } else {
        let h = mirror::hydrate(&HostMirror { pack }, &store, &cellar, &spec, &c.files, &m).await?;
        log_line(2, &format!("hydrated from the mirror: {}", h.to_json()));
        Some(h)
    };
    build(c, spec, store, cellar, None, hydrated)
}

fn build(
    c: Config,
    spec: NestSpec,
    store: Store,
    cellar: Arc<Cellar>,
    bytes: Option<Bytes>,
    hydrated: Option<Hydrated>,
) -> anyhow::Result<Engine> {
    let start = spec.start.unwrap_or_default();
    let budget = c.memory_mib.map(TabBudget::mib).unwrap_or_default();
    let mut nest = young::open_nest(store, cellar.clone(), spec, start, budget)?;
    // Before `derive`, which admits the entities against it (RFC-0075 §17).
    nest.budget = budget;
    if !c.files.is_empty() {
        nest.derive(&c.files)?;
    }
    let verifier = c.verified.then(|| Arc::new(Verifier::new()));
    if let Some(v) = &verifier {
        nest.verify_with(Verified {
            verifier: v.clone(),
            head: Arc::new(HostHead),
        });
    }
    Ok(Engine {
        nest: Arc::new(nest),
        verifier,
        follows: c
            .follow
            .unwrap_or(c.mirror.as_ref().is_none_or(|m| m.postcard.is_none())),
        hydrated,
        cellar,
        rpc: Arc::new(RpcClient::new(c.rpc)?),
        files: c.files,
        nid: c.nid,
        poll_interval_secs: c.poll_interval_secs,
        storage: c.storage,
        bytes,
        follow: None,
        rolled: None,
    })
}

enum Driven<T> {
    Done(T),
    Blocked(Value),
}

/// Poll `fut` until it finishes or needs the host: `{"requests": [...]}` or `{"wait_ms": n}`.
fn drive<F: Future + ?Sized>(mut fut: Pin<&mut F>) -> Result<Driven<F::Output>, Fault> {
    let woken = Arc::new(Woken::default());
    let waker = Waker::from(woken.clone());
    let mut cx = Context::from_waker(&waker);
    loop {
        nuthatch_core::time::wake_sleepers();
        woken.0.store(false, Relaxed);
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return Ok(Driven::Done(v));
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
            return Ok(Driven::Blocked(json!({"requests": out})));
        }
        if EXCHANGE.with(|x| !x.borrow().in_flight.is_empty()) {
            return Err(fault(
                "unanswered",
                "requests are unanswered: nh_resume first",
            ));
        }
        // A yield, or a sleep already due, is polled again at once.
        if woken.0.load(Relaxed) {
            continue;
        }
        match nuthatch_core::time::next_wake() {
            Some(until) => {
                let left = until.saturating_duration_since(nuthatch_core::time::deadline_now());
                if !left.is_zero() {
                    return Ok(Driven::Blocked(json!({"wait_ms": left.as_millis() as u64})));
                }
            }
            None => {
                return Err(fault(
                    "stalled",
                    "the engine is pending on nothing a host can answer",
                ))
            }
        }
    }
}

/// Advance until blocked: `{"idle": true}` once the cursor is at the tip, `{"requests": [...]}` for
/// the host to perform, or `{"wait_ms": n}`.
#[no_mangle]
pub extern "C" fn nh_step() -> i32 {
    json_out(STATE.with(|s| {
        let mut s = s.borrow_mut();
        if let Some(State::Opening(fut)) = s.as_mut() {
            match drive(fut.as_mut())? {
                Driven::Blocked(v) => return Ok(v),
                Driven::Done(Ok(engine)) => *s = Some(State::Open(Box::new(engine))),
                Driven::Done(Err(e)) => {
                    // A failed open leaves no nest, so the host may open again.
                    *s = None;
                    return Err(fault_of("open_failed", e));
                }
            }
        }
        match s.as_mut() {
            Some(State::Open(engine)) => step(engine),
            _ => Err(not_open()),
        }
    }))
}

fn step(engine: &mut Engine) -> Result<Value, Fault> {
    if !engine.follows {
        return Ok(json!({"idle": true}));
    }
    let follow = engine.follow.get_or_insert_with(|| {
        let (nest, rpc) = (engine.nest.clone(), engine.rpc.clone());
        Box::pin(async move { nest.follow(&*rpc).await })
    });
    match drive(follow.as_mut())? {
        Driven::Blocked(v) => Ok(v),
        Driven::Done(r) => {
            engine.follow = None;
            engine.rolled = r.map_err(|e| fault_of("follow_failed", e))?;
            match engine.verifier.as_ref().and_then(|v| v.unverified()) {
                Some(why) => Ok(json!({"idle": true, "unverified": why})),
                None => Ok(json!({"idle": true})),
            }
        }
    }
}

/// Answers to the last `requests`, one per request, in order.
#[no_mangle]
pub extern "C" fn nh_resume(ptr: *mut u8, len: usize) -> i32 {
    json_out(resume(&input(ptr, len)))
}

fn resume(answers: &[u8]) -> Result<Value, Fault> {
    let answers: Vec<Value> = serde_json::from_slice(answers).map_err(|e| fault("bad_call", e))?;
    let wakers = EXCHANGE.with(|x| {
        let mut x = x.borrow_mut();
        if answers.len() != x.in_flight.len() {
            return Err(fault(
                "bad_call",
                format!(
                    "{} answers for {} requests",
                    answers.len(),
                    x.in_flight.len()
                ),
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

fn with_engine<T>(f: impl FnOnce(&Engine) -> Result<T, Fault>) -> Result<T, Fault> {
    STATE.with(|s| match s.borrow().as_ref() {
        Some(State::Open(engine)) => f(engine),
        _ => Err(not_open()),
    })
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
    json_out(with_engine(|engine| {
        let nest = nuthatch_embed::http::Nest {
            nest: &engine.nest,
            cellar: &engine.cellar,
            files: &engine.files,
            nid: Some(&engine.nid),
            version: young::node_version(),
            poll_interval_secs: engine.poll_interval_secs,
            log_window: engine.nest.log_window(),
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
        Err(fault("stalled", "a query yielded a million times"))
    }))
}

/// Storage mode `bytes`: the whole store, raw, consistent between write transactions.
#[no_mangle]
pub extern "C" fn nh_export() -> i32 {
    output(with_engine(|engine| match &engine.bytes {
        Some(b) => Ok(b.snapshot()),
        None => Err(fault(
            "unsupported",
            "storage `host`: the store is the host's file already",
        )),
    }))
}

/// A probe for the gates, outside the interface's promise: what RFC-0075 S3's gate compares, as
/// `LiveNest::follow` reports it.
#[no_mangle]
pub extern "C" fn nh_report() -> i32 {
    json_out(with_engine(|engine| {
        young::report(
            &engine.nest,
            &engine.cellar,
            &engine.rpc,
            engine.rolled,
            engine.storage.name(),
        )
        .map(|mut r| {
            r["mirror"] = engine.hydrated.as_ref().map(Hydrated::to_json).into();
            r["verified"] = engine.verifier.as_ref().map(|v| v.report()).into();
            r
        })
        .map_err(|e| fault("report_failed", format!("{e:#}")))
    }))
}

/// A probe: the module's linear memory in bytes; it never shrinks, so it is the high-water mark.
#[no_mangle]
pub extern "C" fn nh_memory_bytes() -> f64 {
    nuthatch_embed::budget::linear_memory() as f64
}
