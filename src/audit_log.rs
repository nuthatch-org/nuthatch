//! The call audit log (#3095): an optional, append-only, hash-chained record of who ran what against
//! `/sql`, `/q/{name}`, the admin API and MCP. **Off unless `--audit-log <DIR>` is given**: without
//! it nothing here is installed, no layer wraps the listener and no file is opened.
//!
//! One JSON line per call, written by one thread that owns the files. Each line carries `seq` and
//! `prev`, the SHA-256 of the line before it, so `nuthatch audit verify` finds a record that was
//! deleted, edited or moved. Nothing leaves the host.
//!
//! What a record holds is the call's shape, never what the caller authenticated with: no header but
//! the configured identity header, no query string, no admin body. A statement is recorded as its
//! hash unless the operator asks for its text with `--audit-log-statements text`.
//!
//! **Identity.** nuthatch authenticates nobody here; it records the string it is given. Either a
//! header the operator's gateway sets (`--audit-log-identity-header`), which the gateway must also
//! strip from what clients send; a call carrying that header twice is refused `400`. [`caller`] is the hook a token
//! check will hand its verified subject to (#3094); nothing calls it yet. The audit layer sits outside
//! every route and every layer a router adds, so a check that refuses a call still leaves a record
//! of the refusal; [`refused`] names why.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context as TaskContext, Poll};
use std::time::Instant;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// How a statement is written into its record.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum Statements {
    /// Its SHA-256 only: says which statement ran to anyone who has it, and nothing to anyone else
    /// who cannot guess it.
    #[default]
    Hash,
    /// Its text and its hash. MCP tool arguments are written too.
    Text,
}

/// When a call's response waits for its record.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum Fsync {
    /// The writer fsyncs after every batch it drains; a response does not wait. A crash loses at
    /// most the records still queued or written since the last fsync.
    #[default]
    Batch,
    /// A response is sent only once its record is fsynced (group commit). A call that cannot get a
    /// slot to be recorded is refused `503 audit_unwritable` before it runs. A read whose record
    /// then fails to fsync is answered 503 too; a mutating admin call that already ran keeps its
    /// own response (with `nuthatch-audit: unwritable`), since a retry could repeat it.
    PerRecord,
}

/// The operator's settings, from the `--audit-log*` flags.
#[derive(Clone, Debug)]
pub struct Settings {
    pub dir: PathBuf,
    pub identity_header: Option<axum::http::HeaderName>,
    pub statements: Statements,
    pub fsync: Fsync,
    pub rotate_bytes: u64,
    /// Files kept after a rotation; `0` keeps every one.
    pub keep_files: usize,
}

/// Headers that carry a credential rather than a name. Recording one would put a secret in the log.
const CREDENTIAL_HEADERS: [&str; 17] = [
    "authorization",
    "proxy-authorization",
    "x-forwarded-authorization",
    "cookie",
    "set-cookie",
    "nuthatch-admin-token",
    "x-api-key",
    "api-key",
    "apikey",
    "x-apikey",
    "x-api-token",
    "x-auth-token",
    "x-access-token",
    "x-token",
    "x-csrf-token",
    "x-goog-api-key",
    "x-amz-security-token",
];

/// The longest identity kept; a longer one is cut there.
const IDENTITY_MAX: usize = 512;
/// Records queued for the writer. Each audited call reserves a slot before its handler runs, so a
/// full queue makes a call wait to start rather than lose its record, an abandoned one included.
const QUEUE: usize = 1024;
const GENESIS: &str = "0000000000000000000000000000000000000000000000000000000000000000";

impl Settings {
    /// From the CLI. `None` when `--audit-log` is absent, which is the default.
    pub fn from_args(a: &crate::cli::AuditLogArgs) -> Result<Option<Settings>> {
        let Some(dir) = &a.audit_log else {
            return Ok(None);
        };
        let identity_header = match &a.audit_identity_header {
            None => None,
            Some(h) => {
                let name = axum::http::HeaderName::try_from(h.as_str()).with_context(|| {
                    format!("--audit-log-identity-header {h} is not a header name")
                })?;
                if CREDENTIAL_HEADERS.contains(&name.as_str()) {
                    bail!(
                        "--audit-log-identity-header {h} carries a credential, and a record must never \
                         hold one: name the header your gateway sets to the caller's identity"
                    );
                }
                Some(name)
            }
        };
        if a.audit_rotate_mb == 0 {
            bail!("--audit-log-rotate-mb must be at least 1");
        }
        Ok(Some(Settings {
            dir: dir.clone(),
            identity_header,
            statements: a.audit_statements,
            fsync: a.audit_fsync,
            rotate_bytes: a.audit_rotate_mb.saturating_mul(1 << 20),
            keep_files: a.audit_keep,
        }))
    }
}

static INSTALLED: OnceLock<Arc<AuditLog>> = OnceLock::new();

/// Open the log `args` name, if they name one, for every listener this process binds.
pub fn install(args: &crate::cli::AuditLogArgs) -> Result<()> {
    let Some(settings) = Settings::from_args(args)? else {
        return Ok(());
    };
    let log = AuditLog::open(settings)?;
    tracing::info!(
        "audit log: recording /sql, /q, admin and MCP calls in {}",
        log.settings.dir.display()
    );
    INSTALLED
        .set(log)
        .map_err(|_| anyhow::anyhow!("the audit log is already open"))
}

/// The log [`install`] opened, if any.
pub fn installed() -> Option<Arc<AuditLog>> {
    INSTALLED.get().cloned()
}

/// Wait until every record queued so far is written and fsynced. Called once a listener has shut down.
pub async fn flush() {
    if let Some(log) = installed() {
        log.flush().await;
    }
}

// ---- what a handler tells the record -------------------------------------------------------------

#[derive(Default)]
struct Note {
    verified: Option<String>,
    refusal: Option<String>,
    nest: Option<String>,
    statement: Option<String>,
    tool: Option<String>,
    arguments: Option<String>,
    tool_error: bool,
}

tokio::task_local! {
    // Shared with the call's [`Pending`], which reads it even when the call is dropped mid-flight.
    static NOTE: Arc<Mutex<Note>>;
}

fn note(f: impl FnOnce(&mut Note)) {
    let _ = NOTE.try_with(|n| f(&mut n.lock().unwrap_or_else(|e| e.into_inner())));
}

/// The identity a token check in this process verified for the current call, such as a JWT `sub`.
/// Recorded as given, and preferred to the identity header. A no-op with the audit log off.
pub fn caller(subject: &str) {
    note(|n| n.verified = Some(clip(subject)));
}

/// Why the current call was refused, in a word or two (`token_expired`, `scope_missing`). Never a
/// token or anything a caller sent. A no-op with the audit log off.
pub fn refused(reason: &str) {
    note(|n| n.refusal = Some(clip(reason)));
}

/// The nest the current call was routed to.
pub(crate) fn nest(label: &str) {
    note(|n| {
        n.nest.get_or_insert_with(|| label.to_string());
    });
}

/// The SQL the current call runs.
pub(crate) fn statement(sql: &str) {
    note(|n| n.statement = Some(sql.to_string()));
}

/// The MCP method the current call makes, and for `tools/call` its tool and arguments.
pub(crate) fn mcp(method: &str, tool: Option<&str>, arguments: Option<&serde_json::Value>) {
    note(|n| {
        n.tool = Some(match tool {
            Some(t) => format!("{method}:{t}"),
            None => method.to_string(),
        });
        n.arguments = arguments.map(|a| a.to_string());
    });
}

/// The current MCP tool call answered with an error.
pub(crate) fn tool_error() {
    note(|n| n.tool_error = true);
}

fn clip(s: &str) -> String {
    let mut end = s.len().min(IDENTITY_MAX);
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].to_string()
}

// ---- the layer ---------------------------------------------------------------------------------

/// Which audited surface a path is, by its last segments, so a runtime's `/<nest>/sql` is `/sql`.
fn surface_of(path: &str) -> Option<&'static str> {
    let segs: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    match segs.as_slice() {
        // First: admin routes take `{*name}` wildcards, and a nest may be named `sql` or `mcp`
        // or sit under a tenant `q`, so `/_admin/nests/sql` must not read as a query surface.
        s if s.contains(&"_admin") => Some("admin"),
        [.., "q", _] => Some("query"),
        [.., "sql"] => Some("sql"),
        // `/explain` plans caller SQL over hot and sealed data; `/diff` reads every table.
        [.., "explain"] => Some("explain"),
        [.., "diff"] => Some("diff"),
        [.., "mcp"] => Some("mcp"),
        _ => None,
    }
}

/// `app` with every audited call recorded in `log`: the outermost layer of a listener.
pub fn wrap(app: axum::Router, log: Arc<AuditLog>) -> axum::Router {
    app.layer(axum::middleware::from_fn(move |req, next| {
        record(log.clone(), req, next)
    }))
}

struct Entry {
    ts_ms: u64,
    identity: Option<(String, &'static str)>,
    surface: &'static str,
    method: String,
    path: String,
    /// `None` when the call was dropped before it answered: a client that hung up mid-call.
    status: Option<u16>,
    outcome: &'static str,
    us: u64,
    arguments_sha256: Option<String>,
    note: Note,
    rows: Option<u64>,
    cost: crate::cost::Figures,
}

/// A body hashed as the route reads it, so this layer neither buffers nor caps it.
struct Hashed {
    inner: axum::body::BodyDataStream,
    state: Arc<Mutex<Digesting>>,
}

#[derive(Default)]
struct Digesting {
    h: Sha256,
    bytes: u64,
    done: bool,
}

impl futures::Stream for Hashed {
    type Item = std::result::Result<axum::body::Bytes, axum::Error>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let next = Pin::new(&mut self.inner).poll_next(cx);
        let mut d = self.state.lock().unwrap_or_else(|e| e.into_inner());
        match &next {
            Poll::Ready(Some(Ok(b))) => {
                d.h.update(b);
                d.bytes += b.len() as u64;
            }
            Poll::Ready(None) => d.done = true,
            _ => {}
        }
        next
    }
}

/// One call's record until it is queued. Dropped first - the client hung up and the server dropped
/// the call's future - it still queues what it knows, as `abandoned`: a call that ran must not go
/// unrecorded because nobody waited for its answer.
struct Pending {
    entry: Option<Entry>,
    note: Arc<Mutex<Note>>,
    meter: crate::cost::Meter,
    body: Option<Arc<Mutex<Digesting>>>,
    started: Instant,
    /// A queue slot reserved before the handler ran, so the record can always be sent: by
    /// `finish`, or by `drop` for an abandoned call. `None` only when the writer is gone.
    slot: Option<tokio::sync::mpsc::OwnedPermit<Msg>>,
    /// A non-read call to `/_admin`: its effect cannot be taken back by withholding the answer.
    mutating: bool,
}

impl Pending {
    fn complete(&mut self, status: Option<u16>) -> Option<Entry> {
        let mut e = self.entry.take()?;
        e.note = std::mem::take(&mut *self.note.lock().unwrap_or_else(|e| e.into_inner()));
        e.status = status;
        e.outcome = match status {
            None => "abandoned",
            Some(_) if e.note.tool_error => "tool_error",
            Some(0..=399) => "ok",
            Some(400..=499) => "refused",
            Some(_) => "failed",
        };
        if let Some(v) = &e.note.verified {
            e.identity = Some((v.clone(), "token"));
        }
        if let Some(body) = &self.body {
            let mut d = body.lock().unwrap_or_else(|e| e.into_inner());
            // `/q` hashes its query string even when its body is empty or unread; an admin call
            // hashes a body it read whole, and has nothing to hash otherwise.
            let whole = d.done || d.bytes == 0;
            let hash = match e.surface {
                "query" => whole,
                _ => d.done && d.bytes > 0,
            };
            if hash {
                e.arguments_sha256 = Some(hex::encode(std::mem::take(&mut d.h).finalize()));
            }
        }
        e.us = self.started.elapsed().as_micros() as u64;
        e.rows = self.meter.result_rows();
        e.cost = self.meter.figures();
        Some(e)
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        if let Some(e) = self.complete(None) {
            match self.slot.take() {
                // The slot was reserved before the call ran, so this never blocks or fails.
                Some(slot) => {
                    slot.send(Msg::Entry(Box::new(e), None));
                }
                None => {
                    tracing::error!("audit log: an abandoned call went unrecorded (writer gone)")
                }
            }
        }
    }
}

async fn record(
    log: Arc<AuditLog>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let Some(surface) = surface_of(req.uri().path()) else {
        return next.run(req).await;
    };
    if req.method() == axum::http::Method::OPTIONS {
        return next.run(req).await;
    }
    // A slot before the handler runs. A call that cannot be recorded is refused here, before it has
    // done anything, so a client that retries on 503 never repeats an action.
    let slot = match log.tx.clone().reserve_owned().await {
        Ok(slot) => Some(slot),
        Err(_) if log.settings.fsync == Fsync::PerRecord => {
            tracing::error!("audit log writer is gone: a call is refused");
            return unwritable(
                "this call could not be recorded in the audit log, so it was not run",
            );
        }
        Err(_) => {
            tracing::error!("audit log writer is gone: a call went unrecorded");
            None
        }
    };
    let mutating = surface == "admin"
        && !matches!(
            *req.method(),
            axum::http::Method::GET | axum::http::Method::HEAD
        );
    let started = Instant::now();
    let ts_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64);
    // A duplicated identity header is refused below: `get` would pick the first value, and the
    // first is the one a caller can put there.
    let (header, duplicated) = {
        let mut values = log
            .settings
            .identity_header
            .as_ref()
            .into_iter()
            .flat_map(|h| req.headers().get_all(h).iter());
        let first = values
            .next()
            .map(|v| clip(&String::from_utf8_lossy(v.as_bytes())));
        (first, values.next().is_some())
    };
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    // `/sql`, `/explain` and `/mcp` say what they run through [`statement`] and [`mcp`]; the others are hashed
    // here: `/q`'s query string and body, an admin call's body only (its query string may carry the
    // admin token, and its body is hashed because a mount's RPC URL may carry a key).
    let (req, body) = match surface {
        "query" | "admin" => {
            let (parts, body) = req.into_parts();
            let mut d = Digesting::default();
            if surface == "query" {
                let query = parts.uri.query().unwrap_or("");
                let kept: Vec<&str> = query
                    .split('&')
                    .filter(|kv| !kv.is_empty() && !kv.starts_with("token="))
                    .collect();
                d.h.update(kept.join("&").as_bytes());
                d.h.update(b"\n");
            }
            let state = Arc::new(Mutex::new(d));
            let hashed = Hashed {
                inner: body.into_data_stream(),
                state: state.clone(),
            };
            (
                axum::extract::Request::from_parts(parts, axum::body::Body::from_stream(hashed)),
                Some(state),
            )
        }
        _ => (req, None),
    };
    let note = Arc::new(Mutex::new(Note::default()));
    let meter = crate::cost::Meter::default();
    let pending = Pending {
        entry: Some(Entry {
            ts_ms,
            identity: header.filter(|_| !duplicated).map(|h| (h, "header")),
            surface,
            method,
            path,
            status: None,
            outcome: "abandoned",
            us: 0,
            arguments_sha256: None,
            note: Note::default(),
            rows: None,
            cost: Default::default(),
        }),
        note: note.clone(),
        meter: meter.clone(),
        body,
        started,
        slot,
        mutating,
    };
    let resp = if duplicated {
        NOTE.scope(note, async {
            refused("identity_header_duplicated");
            let mut r = axum::response::Response::new(axum::body::Body::from(
                r#"{"error":"identity_header_duplicated"}"#,
            ));
            *r.status_mut() = axum::http::StatusCode::BAD_REQUEST;
            r.headers_mut().insert(
                axum::http::header::CONTENT_TYPE,
                axum::http::HeaderValue::from_static("application/json"),
            );
            r
        })
        .await
    } else {
        NOTE.scope(note, crate::cost::within(meter, next.run(req)))
            .await
    };
    log.finish(pending, resp).await
}

// ---- the writer ---------------------------------------------------------------------------------

enum Msg {
    Entry(Box<Entry>, Option<tokio::sync::oneshot::Sender<bool>>),
    Flush(tokio::sync::oneshot::Sender<bool>),
}

/// An open audit log: the queue to its writer thread.
pub struct AuditLog {
    settings: Settings,
    tx: tokio::sync::mpsc::Sender<Msg>,
}

impl AuditLog {
    /// Lock `settings.dir`, take up the chain where the last record left it, and start the writer.
    pub fn open(settings: Settings) -> Result<Arc<AuditLog>> {
        let chain = Chain::open(&settings)?;
        let (tx, rx) = tokio::sync::mpsc::channel(QUEUE);
        std::thread::Builder::new()
            .name("audit-log".into())
            .spawn(move || chain.run(rx))
            .context("starting the audit log writer")?;
        Ok(Arc::new(AuditLog { settings, tx }))
    }

    /// Wait until every record queued so far is written and fsynced.
    pub async fn flush(&self) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self.tx.send(Msg::Flush(tx)).await.is_ok() {
            let _ = rx.await;
        }
    }

    async fn finish(
        &self,
        mut pending: Pending,
        resp: axum::response::Response,
    ) -> axum::response::Response {
        let per_record = self.settings.fsync == Fsync::PerRecord;
        let Some(slot) = pending.slot.take() else {
            pending.entry = None;
            return resp;
        };
        let Some(entry) = pending.complete(Some(resp.status().as_u16())) else {
            return resp;
        };
        if !per_record {
            slot.send(Msg::Entry(Box::new(entry), None));
            return resp;
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        slot.send(Msg::Entry(Box::new(entry), Some(tx)));
        if rx.await.unwrap_or(false) {
            return resp;
        }
        if pending.mutating {
            // The action has run. Answering 503 would invite a retry that repeats it.
            tracing::error!("audit log: a mutating admin call ran but its record was not written");
            let mut resp = resp;
            resp.headers_mut().insert(
                "nuthatch-audit",
                axum::http::HeaderValue::from_static("unwritable"),
            );
            return resp;
        }
        unwritable("this call could not be recorded in the audit log, so its answer is withheld")
    }
}

fn unwritable(why: &str) -> axum::response::Response {
    use axum::response::IntoResponse;
    (
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        axum::Json(serde_json::json!({ "error": "audit_unwritable", "why": why })),
    )
        .into_response()
}

#[derive(Serialize)]
struct Record<'a> {
    v: u8,
    seq: u64,
    prev: &'a str,
    ts_ms: u64,
    event: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    dropped_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    through_seq: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    identity: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    identity_from: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    surface: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    method: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    nest: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    outcome: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    refusal: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    us: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    statement_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    statement: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    arguments_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    arguments: Option<&'a serde_json::value::RawValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tables: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rows: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cost: Option<&'a crate::cost::Figures>,
}

impl<'a> Record<'a> {
    fn bare(seq: u64, prev: &'a str, event: &'a str) -> Record<'a> {
        Record {
            v: 1,
            seq,
            prev,
            ts_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis() as u64),
            event,
            dropped_bytes: None,
            through_seq: None,
            identity: None,
            identity_from: None,
            surface: None,
            method: None,
            path: None,
            nest: None,
            status: None,
            outcome: None,
            refusal: None,
            us: None,
            tool: None,
            statement_sha256: None,
            statement: None,
            arguments_sha256: None,
            arguments: None,
            tables: None,
            rows: None,
            cost: None,
        }
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn file_name(first_seq: u64) -> String {
    format!("audit-{first_seq:020}.jsonl")
}

/// The log's files, oldest first, with the first `seq` each one's name says it holds.
fn files(dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
        let path = e?.path();
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if let Some(seq) = name
            .strip_prefix("audit-")
            .and_then(|n| n.strip_suffix(".jsonl"))
            .and_then(|n| n.parse::<u64>().ok())
        {
            out.push((seq, path));
        }
    }
    out.sort();
    Ok(out)
}

/// Where the file's complete lines end, and the last of them. A file that does not end in a newline
/// has a torn record after that point.
fn last_line(f: &mut File) -> std::io::Result<(u64, Option<Vec<u8>>)> {
    let len = f.metadata()?.len();
    let (mut start, mut buf) = (len, Vec::new());
    loop {
        if let Some(nl) = buf.iter().rposition(|&b| b == b'\n') {
            let end = start + nl as u64 + 1;
            if let Some(p) = buf[..nl].iter().rposition(|&b| b == b'\n') {
                return Ok((end, Some(buf[p + 1..nl].to_vec())));
            }
            if start == 0 {
                return Ok((end, Some(buf[..nl].to_vec())));
            }
        } else if start == 0 {
            return Ok((0, None));
        }
        let step = (64 << 10).min(start);
        start -= step;
        let mut chunk = vec![0; step as usize];
        f.seek(SeekFrom::Start(start))?;
        f.read_exact(&mut chunk)?;
        chunk.extend_from_slice(&buf);
        buf = chunk;
    }
}

#[derive(Deserialize)]
struct Link {
    seq: u64,
    prev: String,
    #[serde(default)]
    event: Option<String>,
    #[serde(default)]
    through_seq: Option<u64>,
}

/// The writer's state: the chain's head and the file it appends to.
struct Chain {
    dir: PathBuf,
    statements: Statements,
    rotate_bytes: u64,
    keep_files: usize,
    _lock: File,
    seq: u64,
    head: String,
    file: Option<(File, u64)>,
    broken: bool,
}

impl Chain {
    fn open(s: &Settings) -> Result<Chain> {
        std::fs::create_dir_all(&s.dir)
            .with_context(|| format!("creating the audit log directory {}", s.dir.display()))?;
        let lock = File::create(s.dir.join("audit.lock"))?;
        if lock.try_lock().is_err() {
            bail!(
                "{} is another process's audit log: two writers would fork its chain",
                s.dir.display()
            );
        }
        let mut chain = Chain {
            dir: s.dir.clone(),
            statements: s.statements,
            rotate_bytes: s.rotate_bytes,
            keep_files: s.keep_files,
            _lock: lock,
            seq: 0,
            head: GENESIS.to_string(),
            file: None,
            broken: false,
        };
        let mut dropped = 0;
        let mut found = files(&s.dir)?;
        while let Some((_, path)) = found.pop() {
            let mut f = OpenOptions::new().read(true).write(true).open(&path)?;
            let len = f.metadata()?.len();
            let (end, line) = last_line(&mut f)?;
            if end < len {
                // A record torn by a crash mid-write was never acknowledged: cut it, and say so in
                // the next record so the cut is itself on the chain.
                f.set_len(end)?;
                f.sync_all()?;
                dropped += len - end;
            }
            let Some(line) = line else {
                // Nothing complete in it. Its name still claims `first`, so it goes.
                drop(f);
                std::fs::remove_file(&path)?;
                continue;
            };
            let link: Link = serde_json::from_slice(&line).with_context(|| {
                format!(
                    "the last record in {} does not parse; run `nuthatch audit verify {}`",
                    path.display(),
                    s.dir.display()
                )
            })?;
            chain.seq = link.seq;
            chain.head = sha256_hex(&line);
            f.seek(SeekFrom::End(0))?;
            chain.file = Some((f, end));
            break;
        }
        if dropped > 0 {
            tracing::warn!("audit log: cut a torn final record ({dropped} bytes) left by a crash");
            let seq = chain.seq + 1;
            let head = chain.head.clone();
            let mut r = Record::bare(seq, &head, "recovered");
            r.dropped_bytes = Some(dropped);
            let line = serde_json::to_vec(&r)?;
            chain.append(&line)?;
            chain.sync()?;
        }
        Ok(chain)
    }

    fn sync(&mut self) -> std::io::Result<()> {
        match &self.file {
            Some((f, _)) => f.sync_data(),
            None => Ok(()),
        }
    }

    /// Append one serialized record and advance the head. Nothing is left behind on a failure: a
    /// partial write is cut back, and a cut that fails stops the writer.
    fn append(&mut self, line: &[u8]) -> Result<()> {
        if self.broken {
            bail!("the audit log stopped after a write it could not undo");
        }
        let seq = self.seq + 1;
        let full = self
            .file
            .as_ref()
            .is_some_and(|(_, len)| *len > 0 && *len + line.len() as u64 + 1 > self.rotate_bytes);
        let rotated = full || self.file.is_none();
        if rotated {
            if let Some((f, _)) = self.file.take() {
                f.sync_all()?;
            }
            let path = self.dir.join(file_name(seq));
            let f = OpenOptions::new().create(true).append(true).open(&path)?;
            // The new file's entry must be durable before its records are.
            #[cfg(unix)]
            File::open(&self.dir)?.sync_all()?;
            let len = f.metadata()?.len();
            self.file = Some((f, len));
        }
        self.write_line(line, seq)?;
        if rotated {
            self.retain();
        }
        Ok(())
    }

    fn write_line(&mut self, line: &[u8], seq: u64) -> Result<()> {
        let Some((f, len)) = self.file.as_mut() else {
            unreachable!("opened by the caller");
        };
        let mut bytes = Vec::with_capacity(line.len() + 1);
        bytes.extend_from_slice(line);
        bytes.push(b'\n');
        if let Err(e) = f.write_all(&bytes) {
            if f.set_len(*len).is_err() {
                self.broken = true;
            }
            return Err(e.into());
        }
        *len += bytes.len() as u64;
        self.seq = seq;
        self.head = sha256_hex(line);
        Ok(())
    }

    /// Delete the oldest files past `keep_files`. The deletion is put on the chain first, as a
    /// `pruned` record in the newest file, and the newest file is made durable before anything is
    /// removed: a crash can leave too many files, never a log that has lost its past without saying
    /// so, and never one a restart would take for empty.
    fn retain(&mut self) {
        if self.keep_files == 0 {
            return;
        }
        if let Err(e) = self.prune() {
            tracing::warn!("audit log: retention skipped: {e:#}");
        }
    }

    fn prune(&mut self) -> Result<()> {
        let all = files(&self.dir)?;
        let drop = all.len().saturating_sub(self.keep_files);
        if drop == 0 {
            return Ok(());
        }
        let through = all[drop].0 - 1;
        let seq = self.seq + 1;
        let head = self.head.clone();
        let mut r = Record::bare(seq, &head, "pruned");
        r.through_seq = Some(through);
        let line = serde_json::to_vec(&r)?;
        self.write_line(&line, seq)?;
        if let Some((f, _)) = &self.file {
            f.sync_all()?;
        }
        for (_, path) in &all[..drop] {
            std::fs::remove_file(path)?;
        }
        Ok(())
    }

    fn serialize(&self, e: &Entry) -> serde_json::Result<Vec<u8>> {
        let seq = self.seq + 1;
        let n = &e.note;
        let mut r = Record::bare(seq, &self.head, "call");
        r.ts_ms = e.ts_ms;
        if let Some((id, from)) = &e.identity {
            r.identity = Some(id);
            r.identity_from = Some(from);
        }
        r.surface = Some(e.surface);
        r.method = Some(&e.method);
        r.path = Some(&e.path);
        r.nest = n.nest.as_deref();
        r.status = e.status;
        r.outcome = Some(e.outcome);
        r.refusal = n.refusal.as_deref();
        r.us = Some(e.us);
        r.tool = n.tool.as_deref();
        let text = self.statements == Statements::Text;
        if let Some(sql) = &n.statement {
            r.statement_sha256 = Some(sha256_hex(sql.as_bytes()));
            r.statement = text.then_some(sql.as_str());
            let mut tables: Vec<String> = crate::serve::relations_read(sql).into_iter().collect();
            tables.sort();
            r.tables = Some(tables);
        }
        let raw;
        if let Some(args) = &n.arguments {
            r.arguments_sha256 = Some(sha256_hex(args.as_bytes()));
            if text {
                raw = serde_json::value::RawValue::from_string(args.clone()).ok();
                r.arguments = raw.as_deref();
            }
        } else {
            r.arguments_sha256 = e.arguments_sha256.clone();
        }
        if e.surface == "query" {
            r.tables = e.path.rsplit('/').next().map(|q| vec![q.to_string()]);
        }
        r.rows = e.rows;
        r.cost = Some(&e.cost);
        serde_json::to_vec(&r)
    }

    fn run(mut self, mut rx: tokio::sync::mpsc::Receiver<Msg>) {
        while let Some(first) = rx.blocking_recv() {
            let mut batch = vec![first];
            while batch.len() < QUEUE {
                match rx.try_recv() {
                    Ok(m) => batch.push(m),
                    Err(_) => break,
                }
            }
            let mut acks = Vec::with_capacity(batch.len());
            for m in batch {
                match m {
                    Msg::Entry(e, ack) => {
                        let written = self
                            .serialize(&e)
                            .map_err(anyhow::Error::from)
                            .and_then(|line| self.append(&line));
                        if let Err(err) = &written {
                            tracing::error!(
                                "audit log: a {} call went unrecorded: {err:#}",
                                e.surface
                            );
                        }
                        acks.push((ack, written.is_ok()));
                    }
                    Msg::Flush(ack) => acks.push((Some(ack), true)),
                }
            }
            let synced = self.sync();
            if let Err(err) = &synced {
                tracing::error!("audit log: fsync failed: {err:#}");
            }
            for (ack, ok) in acks {
                if let Some(ack) = ack {
                    let _ = ack.send(ok && synced.is_ok());
                }
            }
        }
    }
}

// ---- `nuthatch audit verify` ----------------------------------------------------------------------

/// What [`verify`] found in an intact log.
#[derive(Debug, Serialize, PartialEq)]
pub struct Verified {
    pub files: usize,
    pub records: u64,
    pub first_seq: u64,
    pub last_seq: u64,
    /// The last record's hash. Kept somewhere the log's host cannot write, it lets a later
    /// `verify --head` see a cut or edited tail, which no chain can show by itself.
    pub head: String,
}

/// Walk the chain in `path`, a log directory or one of its files. An error names the first record
/// that breaks it.
pub fn verify(path: &Path, expect_head: Option<&str>) -> Result<Verified> {
    let as_dir = path.is_dir();
    let list = if as_dir {
        files(path)?
    } else {
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        let seq = name
            .strip_prefix("audit-")
            .and_then(|n| n.strip_suffix(".jsonl"))
            .and_then(|n| n.parse().ok())
            .with_context(|| format!("{} is not an audit-<seq>.jsonl file", path.display()))?;
        vec![(seq, path.to_path_buf())]
    };
    if list.is_empty() {
        bail!("no audit-<seq>.jsonl files in {}", path.display());
    }
    let mut head: Option<String> = None;
    let mut pruned_through = 0;
    let (mut first_seq, mut last_seq, mut records) = (0, 0, 0u64);
    for (named, file) in &list {
        let reader = std::io::BufReader::new(
            File::open(file).with_context(|| format!("opening {}", file.display()))?,
        );
        let mut first_in_file = true;
        for (i, line) in reader.split(b'\n').enumerate() {
            let line = line?;
            let at = format!("{} line {}", file.display(), i + 1);
            let link: Link = serde_json::from_slice(&line)
                .with_context(|| format!("{at}: not a record (edited or torn)"))?;
            if first_in_file && link.seq != *named {
                bail!("{at}: file is named for seq {named} but starts at {}: records were removed or the file renamed", link.seq);
            }
            match &head {
                None => {
                    if link.seq == 1 && link.prev != GENESIS {
                        bail!("{at}: record 1 does not start the chain");
                    }
                    first_seq = link.seq;
                }
                Some(h) => {
                    if link.seq != last_seq + 1 {
                        bail!(
                            "{at}: record {} follows record {last_seq}: a record was deleted or moved",
                            link.seq
                        );
                    }
                    if &link.prev != h {
                        bail!(
                            "{at}: record {} does not chain to record {last_seq}: one of them was edited, \
                             or a record was replaced",
                            link.seq
                        );
                    }
                }
            }
            if link.event.as_deref() == Some("pruned") {
                pruned_through = pruned_through.max(link.through_seq.unwrap_or(0));
            }
            first_in_file = false;
            last_seq = link.seq;
            head = Some(sha256_hex(&line));
            records += 1;
        }
    }
    let head = head.context("the audit log holds no records")?;
    // Only the writer's own retention may remove the oldest records, and it records that on the
    // chain. A start past seq 1 that no `pruned` record covers is a deleted prefix. The marker
    // may cover more than the visible prefix: prune writes through_seq for every file it
    // intends to drop, then deletes them one by one, so a crash partway through leaves a valid
    // chain whose first seq is still below that marker.
    // A single shipped file is allowed to start past seq 1: rotation closes it and operators
    // copy it off the host. The missing-prefix rule is for a directory that has lost its start.
    if as_dir && first_seq != 1 && pruned_through + 1 < first_seq {
        bail!(
            "the log starts at seq {first_seq} but nothing on the chain accounts for records 1..={}: \
             its oldest records were deleted",
            first_seq - 1
        );
    }
    if let Some(want) = expect_head {
        let (seq, hash) = want.split_once(':').unwrap_or(("", want));
        if hash != head || (!seq.is_empty() && seq.parse::<u64>().ok() != Some(last_seq)) {
            bail!(
                "the chain is intact but ends at {last_seq}:{head}, not {want}: its tail was cut or replaced"
            );
        }
    }
    Ok(Verified {
        files: list.len(),
        records,
        first_seq,
        last_seq,
        head,
    })
}

/// `nuthatch audit verify <path>`.
pub fn verify_cli(a: &crate::cli::AuditVerifyArgs) -> Result<()> {
    let v = verify(&a.path, a.head.as_deref())?;
    if a.json {
        println!("{}", serde_json::to_string_pretty(&v)?);
    } else {
        println!(
            "ok: {} records (seq {}..={}) in {} file(s); head {}:{}",
            v.records, v.first_seq, v.last_seq, v.files, v.last_seq, v.head
        );
        if v.first_seq != 1 {
            println!(
                "note: the log starts at seq {}: earlier files were removed by --audit-log-keep",
                v.first_seq
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::{get, post};
    use tower::ServiceExt;

    fn settings(dir: &Path) -> Settings {
        Settings {
            dir: dir.to_path_buf(),
            identity_header: Some(axum::http::HeaderName::from_static("x-forwarded-user")),
            statements: Statements::Hash,
            fsync: Fsync::PerRecord,
            rotate_bytes: 64 << 20,
            keep_files: 0,
        }
    }

    /// Routes standing in for nuthatch's: they tell the record what the real handlers tell it.
    fn app(log: Arc<AuditLog>) -> axum::Router {
        let routes = axum::Router::new()
            .route(
                "/sql",
                get(|q: axum::extract::Query<std::collections::HashMap<String, String>>| async move {
                    statement(&q["q"]);
                    crate::cost::result_rows(3);
                    "rows"
                }),
            )
            .route(
                "/_admin/mounts",
                post(|_body: axum::body::Bytes| async {
                    // What a token check (#3094) does when it refuses a call.
                    caller("bob@example");
                    refused("scope_missing");
                    axum::http::StatusCode::FORBIDDEN
                }),
            )
            .route(
                "/mcp",
                post(|body: axum::body::Bytes| async move {
                    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
                    mcp("tools/call", v["params"]["name"].as_str(), v["params"].get("arguments"));
                    "{}"
                }),
            )
            .route("/entity/{id}", get(|| async { "not audited" }))
            .route("/_admin/stuck", get(std::future::pending::<&str>));
        wrap(routes, log)
    }

    async fn send(app: &axum::Router, req: axum::http::Request<axum::body::Body>) -> u16 {
        app.clone().oneshot(req).await.unwrap().status().as_u16()
    }

    fn records(dir: &Path) -> Vec<serde_json::Value> {
        files(dir)
            .unwrap()
            .iter()
            .flat_map(|(_, p)| {
                std::fs::read_to_string(p)
                    .unwrap()
                    .lines()
                    .map(String::from)
                    .collect::<Vec<_>>()
            })
            .map(|l| serde_json::from_str(&l).unwrap())
            .collect()
    }

    async fn three_calls(app: &axum::Router) {
        let sql = axum::http::Request::get("/sql?q=SELECT%20n%20FROM%20holders&token=s3cret-tok")
            .header("x-forwarded-user", "alice")
            .header("authorization", "Bearer s3cret-bearer")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(send(app, sql).await, 200);
        let admin = axum::http::Request::post("/_admin/mounts?token=s3cret-tok")
            .header("x-forwarded-user", "mallory")
            .body(axum::body::Body::from(
                r#"{"rpc":"https://x.example/v2/s3cret-key"}"#,
            ))
            .unwrap();
        assert_eq!(send(app, admin).await, 403);
        let mcp = axum::http::Request::post("/mcp")
            .body(axum::body::Body::from(
                r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"sql","arguments":{"query":"SELECT 1"}}}"#,
            ))
            .unwrap();
        assert_eq!(send(app, mcp).await, 200);
        let other = axum::http::Request::get("/entity/0xabc")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(send(app, other).await, 200);
    }

    /// #3116: auditing a call must not make the engine measure it. Only `Nuthatch-Cost: 1` does, so a
    /// log-on `/sql` costs what a log-off one does (no QueryMetrics, no segment footer pass).
    #[tokio::test]
    async fn the_audit_meter_does_not_ask_for_engine_metrics() {
        let tmp = tempfile::tempdir().unwrap();
        let log = AuditLog::open(settings(tmp.path())).unwrap();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let s = seen.clone();
        let routes = axum::Router::new()
            .route(
                "/sql",
                get(move || {
                    let s = s.clone();
                    async move {
                        let on_blocking = crate::cost::spawn_blocking(crate::cost::asked)
                            .await
                            .unwrap();
                        s.lock().unwrap().push((crate::cost::asked(), on_blocking));
                        crate::cost::result_rows(1);
                        "rows"
                    }
                }),
            )
            .route_layer(axum::middleware::from_fn(crate::cost::layer));
        let app = wrap(routes, log.clone());
        let plain = axum::http::Request::get("/sql")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(send(&app, plain).await, 200);
        let asked = axum::http::Request::get("/sql")
            .header(crate::cost::HEADER, "1")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(send(&app, asked).await, 200);
        log.flush().await;
        assert_eq!(*seen.lock().unwrap(), vec![(false, false), (true, true)]);
        // Both calls are still audited, with their row counts.
        let r = records(tmp.path());
        assert_eq!(r.len(), 2, "{r:?}");
        assert!(r.iter().all(|x| x["rows"] == 1), "{r:?}");
    }

    /// #3134: `get` returns the first of a repeated header, which a caller can choose. Refused, and
    /// recorded as refused with no identity.
    #[tokio::test]
    async fn a_duplicated_identity_header_is_refused_and_not_recorded_as_an_identity() {
        let tmp = tempfile::tempdir().unwrap();
        let log = AuditLog::open(settings(tmp.path())).unwrap();
        let app = app(log.clone());
        let req = axum::http::Request::get("/sql?q=SELECT%201")
            .header("x-forwarded-user", "victim")
            .header("x-forwarded-user", "gateway-set")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(send(&app, req).await, 400);
        log.flush().await;
        let r = records(tmp.path());
        assert_eq!(r.len(), 1, "{r:?}");
        assert!(r[0].get("identity").is_none(), "{r:?}");
        assert_eq!(r[0]["status"], 400);
        assert!(r[0].to_string().contains("identity_header_duplicated"));
    }

    #[tokio::test]
    async fn one_record_per_audited_call_with_identity_and_no_secret() {
        let tmp = tempfile::tempdir().unwrap();
        let log = AuditLog::open(settings(tmp.path())).unwrap();
        let app = app(log.clone());
        three_calls(&app).await;
        log.flush().await;
        let r = records(tmp.path());
        assert_eq!(r.len(), 3, "{r:?}");

        assert_eq!(r[0]["seq"], 1);
        assert_eq!(r[0]["prev"], GENESIS);
        assert_eq!(r[0]["surface"], "sql");
        assert_eq!(r[0]["identity"], "alice");
        assert_eq!(r[0]["identity_from"], "header");
        assert_eq!(r[0]["outcome"], "ok");
        assert_eq!(r[0]["rows"], 3);
        assert_eq!(r[0]["tables"], serde_json::json!(["holders"]));
        assert_eq!(
            r[0]["statement_sha256"],
            sha256_hex(b"SELECT n FROM holders")
        );
        assert!(r[0].get("statement").is_none(), "text is off by default");
        assert_eq!(r[0]["path"], "/sql", "no query string");

        // A refusal is recorded, and a verified subject wins over the header.
        assert_eq!(r[1]["surface"], "admin");
        assert_eq!(r[1]["status"], 403);
        assert_eq!(r[1]["outcome"], "refused");
        assert_eq!(r[1]["refusal"], "scope_missing");
        assert_eq!(r[1]["identity"], "bob@example");
        assert_eq!(r[1]["identity_from"], "token");
        assert_eq!(
            r[1]["arguments_sha256"],
            sha256_hex(br#"{"rpc":"https://x.example/v2/s3cret-key"}"#)
        );

        assert_eq!(r[2]["surface"], "mcp");
        assert_eq!(r[2]["tool"], "tools/call:sql");
        assert!(r[2].get("identity").is_none());
        assert!(r[2].get("arguments").is_none());

        let raw: String = files(tmp.path())
            .unwrap()
            .iter()
            .map(|(_, p)| std::fs::read_to_string(p).unwrap())
            .collect();
        for secret in ["s3cret", "Bearer", "SELECT"] {
            assert!(!raw.contains(secret), "{secret} reached the log: {raw}");
        }
        let v = verify(tmp.path(), None).unwrap();
        assert_eq!((v.records, v.first_seq, v.last_seq), (3, 1, 3));
    }

    /// A call whose future is dropped before it answers - its client hung up - is still recorded.
    #[tokio::test]
    async fn an_abandoned_call_is_recorded() {
        let tmp = tempfile::tempdir().unwrap();
        let log = AuditLog::open(settings(tmp.path())).unwrap();
        let req = axum::http::Request::get("/_admin/stuck")
            .header("x-forwarded-user", "carol")
            .body(axum::body::Body::empty())
            .unwrap();
        let call = app(log.clone()).oneshot(req);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), call)
                .await
                .is_err()
        );
        log.flush().await;
        let r = records(tmp.path());
        assert_eq!(r.len(), 1, "{r:?}");
        assert_eq!(r[0]["outcome"], "abandoned");
        assert_eq!(r[0]["identity"], "carol");
        assert!(r[0].get("status").is_none());
    }

    /// An `AuditLog` over a hand-made channel, with no real writer behind it.
    fn log_over(dir: &Path, cap: usize) -> (Arc<AuditLog>, tokio::sync::mpsc::Receiver<Msg>) {
        let (tx, rx) = tokio::sync::mpsc::channel(cap);
        let log = Arc::new(AuditLog {
            settings: settings(dir),
            tx,
        });
        (log, rx)
    }

    /// Routes that say whether their handler ran.
    fn effect_app(log: Arc<AuditLog>, ran: Arc<std::sync::atomic::AtomicBool>) -> axum::Router {
        let r = ran.clone();
        wrap(
            axum::Router::new()
                .route(
                    "/_admin/mounts",
                    post(move || {
                        let r = r.clone();
                        async move {
                            r.store(true, std::sync::atomic::Ordering::SeqCst);
                            "mounted"
                        }
                    }),
                )
                .route("/sql", get(|| async { "rows" }))
                .route("/_admin/stuck", get(std::future::pending::<&str>)),
            log,
        )
    }

    fn post_mount() -> axum::http::Request<axum::body::Body> {
        axum::http::Request::post("/_admin/mounts")
            .body(axum::body::Body::from("{}"))
            .unwrap()
    }

    /// With no way to take a record, a mutating call is refused before its handler runs.
    #[tokio::test]
    async fn a_call_the_log_cannot_take_is_refused_before_it_runs() {
        let tmp = tempfile::tempdir().unwrap();
        let (log, rx) = log_over(tmp.path(), 4);
        drop(rx);
        let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let app = effect_app(log, ran.clone());
        assert_eq!(send(&app, post_mount()).await, 503);
        assert!(
            !ran.load(std::sync::atomic::Ordering::SeqCst),
            "the action ran"
        );
    }

    /// A record that fails to fsync after a mutating call ran must not turn its answer into 503;
    /// a read in the same state is still withheld.
    #[tokio::test]
    async fn a_mutating_call_is_not_answered_503_once_it_has_taken_effect() {
        let tmp = tempfile::tempdir().unwrap();
        let (log, mut rx) = log_over(tmp.path(), 4);
        // A writer whose every fsync fails.
        tokio::spawn(async move {
            while let Some(m) = rx.recv().await {
                match m {
                    Msg::Entry(_, Some(ack)) | Msg::Flush(ack) => {
                        let _ = ack.send(false);
                    }
                    Msg::Entry(_, None) => {}
                }
            }
        });
        let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let app = effect_app(log, ran.clone());
        let resp = app.clone().oneshot(post_mount()).await.unwrap();
        assert!(ran.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(resp.status().as_u16(), 200);
        assert_eq!(resp.headers()["nuthatch-audit"], "unwritable");
        let read = axum::http::Request::get("/sql")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(send(&app, read).await, 503);
    }

    /// A call dropped while the queue is otherwise full still gets its record, on its own slot.
    #[tokio::test]
    async fn an_abandoned_call_is_queued_even_when_the_queue_is_full() {
        let tmp = tempfile::tempdir().unwrap();
        let (log, mut rx) = log_over(tmp.path(), 1);
        let req = axum::http::Request::get("/_admin/stuck")
            .body(axum::body::Body::empty())
            .unwrap();
        let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let call = effect_app(log, ran).oneshot(req);
        // The only slot is held by this call; it is dropped mid-flight.
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), call)
                .await
                .is_err()
        );
        match rx.try_recv() {
            Ok(Msg::Entry(e, _)) => {
                assert_eq!(e.outcome, "abandoned");
                assert!(e.status.is_none());
            }
            _ => panic!("the abandoned record was dropped"),
        }
    }

    #[tokio::test]
    async fn text_mode_records_the_statement_and_arguments_but_still_no_credential() {
        let tmp = tempfile::tempdir().unwrap();
        let log = AuditLog::open(Settings {
            statements: Statements::Text,
            ..settings(tmp.path())
        })
        .unwrap();
        three_calls(&app(log.clone())).await;
        log.flush().await;
        let r = records(tmp.path());
        assert_eq!(r[0]["statement"], "SELECT n FROM holders");
        assert_eq!(r[2]["arguments"], serde_json::json!({"query": "SELECT 1"}));
        let raw = std::fs::read_to_string(&files(tmp.path()).unwrap()[0].1).unwrap();
        assert!(!raw.contains("s3cret"), "{raw}");
    }

    #[test]
    fn a_credential_header_is_refused_as_the_identity_header() {
        for h in [
            "Authorization",
            "cookie",
            "Proxy-Authorization",
            "Set-Cookie",
            "Nuthatch-Admin-Token",
            "X-Api-Key",
            "Api-Key",
            "X-Auth-Token",
            "X-Access-Token",
            "X-Api-Token",
            "X-Token",
            "X-Goog-Api-Key",
        ] {
            let args = crate::cli::AuditLogArgs {
                audit_log: Some("/tmp/x".into()),
                audit_identity_header: Some(h.into()),
                audit_rotate_mb: 64,
                ..Default::default()
            };
            assert!(Settings::from_args(&args).is_err(), "{h}");
        }
        assert!(Settings::from_args(&crate::cli::AuditLogArgs::default())
            .unwrap()
            .is_none());
    }

    /// Writes `n` records straight through the chain, as the writer does.
    fn write(dir: &Path, n: u64, rotate_bytes: u64, keep_files: usize) {
        let mut chain = Chain::open(&Settings {
            rotate_bytes,
            keep_files,
            ..settings(dir)
        })
        .unwrap();
        for _ in 0..n {
            let seq = chain.seq + 1;
            let head = chain.head.clone();
            let line = serde_json::to_vec(&Record::bare(seq, &head, "call")).unwrap();
            chain.append(&line).unwrap();
        }
        chain.sync().unwrap();
    }

    fn lines(path: &Path) -> Vec<String> {
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(String::from)
            .collect()
    }

    fn rewrite(path: &Path, lines: &[String]) {
        std::fs::write(
            path,
            lines.iter().map(|l| format!("{l}\n")).collect::<String>(),
        )
        .unwrap();
    }

    #[test]
    fn verify_finds_a_deleted_edited_or_reordered_record() {
        let tmp = tempfile::tempdir().unwrap();
        write(tmp.path(), 5, 64 << 20, 0);
        let file = files(tmp.path()).unwrap()[0].1.clone();
        let good = lines(&file);
        let v = verify(tmp.path(), None).unwrap();
        assert_eq!(v.records, 5);
        verify(tmp.path(), Some(&format!("5:{}", v.head))).unwrap();

        let mut deleted = good.clone();
        deleted.remove(2);
        rewrite(&file, &deleted);
        let e = verify(tmp.path(), None).unwrap_err().to_string();
        assert!(e.contains("deleted or moved"), "{e}");

        let mut edited = good.clone();
        edited[1] = edited[1].replace("\"call\"", "\"cell\"");
        rewrite(&file, &edited);
        let e = verify(tmp.path(), None).unwrap_err().to_string();
        assert!(e.contains("does not chain"), "{e}");

        let mut reordered = good.clone();
        reordered.swap(1, 3);
        rewrite(&file, &reordered);
        assert!(verify(tmp.path(), None).is_err());

        // A cut tail chains perfectly; only a head kept elsewhere sees it.
        rewrite(&file, &good[..4]);
        verify(tmp.path(), None).unwrap();
        let e = verify(tmp.path(), Some(&format!("5:{}", v.head)))
            .unwrap_err()
            .to_string();
        assert!(e.contains("tail was cut"), "{e}");
    }

    #[test]
    fn the_chain_runs_across_rotated_files_and_through_a_restart() {
        let tmp = tempfile::tempdir().unwrap();
        write(tmp.path(), 10, 400, 0);
        write(tmp.path(), 10, 400, 0);
        let all = files(tmp.path()).unwrap();
        assert!(all.len() > 2, "{all:?}");
        let v = verify(tmp.path(), None).unwrap();
        assert_eq!((v.records, v.last_seq, v.files), (20, 20, all.len()));
        // A whole middle file gone is a gap.
        std::fs::remove_file(&all[1].1).unwrap();
        assert!(verify(tmp.path(), None).is_err());
    }

    #[test]
    fn retention_keeps_the_newest_files_and_what_is_left_still_verifies() {
        let tmp = tempfile::tempdir().unwrap();
        write(tmp.path(), 30, 400, 2);
        assert_eq!(files(tmp.path()).unwrap().len(), 2);
        let v = verify(tmp.path(), None).unwrap();
        assert!(v.first_seq > 1);
        // Each prune takes a seq of its own.
        assert!(v.last_seq > 30, "{v:?}");
    }

    #[test]
    fn a_deleted_prefix_fails_verify_even_with_the_head_and_as_json() {
        let tmp = tempfile::tempdir().unwrap();
        write(tmp.path(), 20, 400, 0);
        let v = verify(tmp.path(), None).unwrap();
        let head = format!("{}:{}", v.last_seq, v.head);
        let all = files(tmp.path()).unwrap();
        assert!(all.len() > 2);
        std::fs::remove_file(&all[0].1).unwrap();
        for h in [None, Some(head.as_str())] {
            let e = verify(tmp.path(), h).unwrap_err().to_string();
            assert!(e.contains("oldest records were deleted"), "{e}");
        }
        let args = crate::cli::AuditVerifyArgs {
            path: tmp.path().to_path_buf(),
            head: Some(head),
            json: true,
        };
        assert!(verify_cli(&args).is_err());
    }

    #[test]
    fn retention_is_recorded_and_a_further_deletion_still_fails() {
        let tmp = tempfile::tempdir().unwrap();
        write(tmp.path(), 30, 400, 2);
        verify(tmp.path(), None).unwrap();
        let all = files(tmp.path()).unwrap();
        std::fs::remove_file(&all[0].1).unwrap();
        assert!(verify(tmp.path(), None).is_err());
    }

    #[test]
    fn a_single_rotated_file_still_verifies_on_its_own() {
        let tmp = tempfile::tempdir().unwrap();
        write(tmp.path(), 30, 400, 0);
        let all = files(tmp.path()).unwrap();
        assert!(all.len() >= 2, "need a rotated file");
        // The file itself is an intact chain, even though it starts past seq 1 and has no
        // pruned record (keep is off). A directory missing that earlier file is the deleted prefix.
        verify(&all[1].1, None).unwrap();
        std::fs::remove_file(&all[0].1).unwrap();
        assert!(verify(tmp.path(), None).is_err());
        verify(&all[1].1, None).unwrap();
    }

    #[test]
    fn a_crash_partway_through_retention_still_verifies() {
        let tmp = tempfile::tempdir().unwrap();
        write(tmp.path(), 30, 400, 0);
        let all = files(tmp.path()).unwrap();
        assert!(
            all.len() >= 3,
            "need several files so a mid-loop crash is visible"
        );
        let through = all[all.len() - 1].0 - 1;
        let mut chain = Chain::open(&Settings {
            rotate_bytes: 64 << 20,
            keep_files: 0,
            ..settings(tmp.path())
        })
        .unwrap();
        let seq = chain.seq + 1;
        let head = chain.head.clone();
        let mut r = Record::bare(seq, &head, "pruned");
        r.through_seq = Some(through);
        let line = serde_json::to_vec(&r).unwrap();
        chain.append(&line).unwrap();
        chain.sync().unwrap();
        // prune would now delete every file but the last. A crash after the first removal
        // leaves a start below the marker, which must still verify.
        std::fs::remove_file(&all[0].1).unwrap();
        verify(tmp.path(), None).unwrap();
        std::fs::remove_file(&all[1].1).unwrap();
        verify(tmp.path(), None).unwrap();
    }

    #[test]
    fn a_crash_after_a_keep_one_rotation_cannot_restart_the_chain_at_seq_1() {
        let tmp = tempfile::tempdir().unwrap();
        write(tmp.path(), 30, 400, 1);
        let before = verify(tmp.path(), None).unwrap();
        assert!(before.first_seq > 1);
        // The crash: a rotation had created the next file, and nothing in it survived.
        let next = tmp.path().join(file_name(before.last_seq + 1));
        File::create(&next).unwrap();
        write(tmp.path(), 1, 400, 1);
        let after = verify(tmp.path(), None).unwrap();
        assert!(after.first_seq > 1, "{after:?}");
        assert!(after.last_seq > before.last_seq, "{after:?}");
    }

    #[test]
    fn the_old_file_outlives_a_rotation_until_the_new_one_is_durable() {
        let tmp = tempfile::tempdir().unwrap();
        write(tmp.path(), 30, 400, 1);
        let mut chain = Chain::open(&Settings {
            rotate_bytes: 400,
            keep_files: 1,
            ..settings(tmp.path())
        })
        .unwrap();
        // Every rotation point leaves a log that verifies and carries on from the same seq.
        for _ in 0..40 {
            let seq = chain.seq + 1;
            let head = chain.head.clone();
            let line = serde_json::to_vec(&Record::bare(seq, &head, "call")).unwrap();
            chain.append(&line).unwrap();
            let v = verify(tmp.path(), None).unwrap();
            assert_eq!(v.last_seq, chain.seq);
            assert!(v.first_seq > 1);
        }
    }

    #[test]
    fn a_torn_final_record_is_cut_and_the_cut_is_recorded() {
        let tmp = tempfile::tempdir().unwrap();
        write(tmp.path(), 3, 64 << 20, 0);
        let file = files(tmp.path()).unwrap()[0].1.clone();
        let mut f = OpenOptions::new().append(true).open(&file).unwrap();
        f.write_all(br#"{"v":1,"seq":4,"pr"#).unwrap();
        drop(f);
        assert!(verify(tmp.path(), None).is_err());
        write(tmp.path(), 1, 64 << 20, 0);
        let r = lines(&file);
        assert_eq!(r.len(), 5);
        let rec: serde_json::Value = serde_json::from_str(&r[3]).unwrap();
        assert_eq!(rec["event"], "recovered");
        assert_eq!(rec["dropped_bytes"], 18);
        assert_eq!(verify(tmp.path(), None).unwrap().last_seq, 5);
    }

    #[test]
    fn a_second_writer_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let _first = Chain::open(&settings(tmp.path())).unwrap();
        let e = Chain::open(&settings(tmp.path()))
            .err()
            .unwrap()
            .to_string();
        assert!(e.contains("another process"), "{e}");
    }

    #[test]
    fn surfaces_are_named_by_their_last_segments() {
        assert_eq!(surface_of("/sql"), Some("sql"));
        assert_eq!(surface_of("/lodestar/sql"), Some("sql"));
        assert_eq!(surface_of("/lodestar/q/sql"), Some("query"));
        assert_eq!(surface_of("/mcp"), Some("mcp"));
        assert_eq!(surface_of("/explain"), Some("explain"));
        assert_eq!(surface_of("/lodestar/diff"), Some("diff"));
        assert_eq!(surface_of("/_admin/nests/diff"), Some("admin"));
        assert_eq!(surface_of("/_admin/mounts/a"), Some("admin"));
        assert_eq!(surface_of("/a/_admin/requests"), Some("admin"));
        assert_eq!(surface_of("/table/sql_x"), None);
        assert_eq!(surface_of("/entity/1"), None);
        assert_eq!(surface_of("/"), None);
    }

    #[test]
    fn admin_wildcards_named_like_query_surfaces_stay_admin() {
        for p in [
            "/_admin/nests/sql",
            "/_admin/nests/mcp",
            "/_admin/nests/q/something",
            "/_admin/mounts/q/x",
            "/_admin/suspend/t/mcp",
            "/_admin/resume/t/sql",
            "/_admin/move/q/x",
        ] {
            assert_eq!(surface_of(p), Some("admin"), "{p}");
        }
    }
}
