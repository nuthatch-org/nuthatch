//! RFC-0080 S0: value-term shapes filtered on a follower, and a live stream of each shape's changes.
//!
//! A spike. Nothing here runs unless `NUTHATCH_SHAPES_S0` names a shapes file on a `serve --follow`
//! process; without it the follower is the follower of today. S1 replaces the variable with
//! `[mounts.sync]`. Retractions go out as the stored row, not by key, and a client keeps its copy as
//! a count per row, so a delta's removals and puts apply in either order.

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

/// One table and a conjunction of value terms: each column's value must be one of its set.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Shape {
    pub table: String,
    #[serde(rename = "where")]
    pub terms: BTreeMap<String, Vec<String>>,
}

/// The fields of a stored row a shape could name, borrowed from its JSON. Every other field is
/// skipped as it is scanned, so a row is read once and nothing of it is copied.
struct Fields<'a>(Vec<(Cow<'a, str>, &'a RawValue)>);

impl<'a> Fields<'a> {
    /// `None` for a row that is not JSON, or whose table `table` refuses, which ends the read there.
    fn parse(
        row: &'a str,
        table: &dyn Fn(&str) -> bool,
        wanted: &dyn Fn(&str) -> bool,
    ) -> Option<Fields<'a>> {
        use serde::de::DeserializeSeed;
        // A row opening with an unescaped table name is refused without starting a parser.
        if let Some(name) = row
            .strip_prefix("{\"table\":\"")
            .and_then(|r| r.split_once('"'))
            .map(|(name, _)| name)
            .filter(|name| !name.contains('\\'))
        {
            if !table(name) {
                return None;
            }
        }
        let mut de = serde_json::Deserializer::from_str(row);
        Seed { table, wanted }.deserialize(&mut de).ok()
    }

    fn get(&self, name: &str) -> Option<&'a RawValue> {
        self.0.iter().find(|(k, _)| k == name).map(|(_, v)| *v)
    }
}

struct Seed<'w> {
    table: &'w dyn Fn(&str) -> bool,
    wanted: &'w dyn Fn(&str) -> bool,
}

impl<'de> serde::de::DeserializeSeed<'de> for Seed<'_> {
    type Value = Fields<'de>;
    fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<Fields<'de>, D::Error> {
        d.deserialize_map(self)
    }
}

impl<'de> serde::de::Visitor<'de> for Seed<'_> {
    type Value = Fields<'de>;
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("a stored row")
    }
    fn visit_map<A: serde::de::MapAccess<'de>>(self, mut m: A) -> Result<Fields<'de>, A::Error> {
        let mut out = Vec::new();
        while let Some(Key(k)) = m.next_key::<Key<'de>>()? {
            if k == "table" {
                let v = m.next_value::<&'de RawValue>()?;
                if !(self.table)(&text(v)) {
                    return Err(serde::de::Error::custom("not a shaped table"));
                }
                out.push((k, v));
            } else if (self.wanted)(&k) {
                out.push((k, m.next_value::<&'de RawValue>()?));
            } else {
                m.next_value::<serde::de::IgnoredAny>()?;
            }
        }
        Ok(Fields(out))
    }
}

/// A key borrowed from the row unless it had to be unescaped.
struct Key<'a>(Cow<'a, str>);

impl<'de> Deserialize<'de> for Key<'de> {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = Key<'de>;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a field name")
            }
            fn visit_borrowed_str<E>(self, s: &'de str) -> Result<Key<'de>, E> {
                Ok(Key(Cow::Borrowed(s)))
            }
            fn visit_str<E>(self, s: &str) -> Result<Key<'de>, E> {
                Ok(Key(Cow::Owned(s.to_string())))
            }
        }
        d.deserialize_str(V)
    }
}

/// A field as the text a term names: a string's contents, anything else its JSON.
fn text(raw: &RawValue) -> Cow<'_, str> {
    let s = raw.get();
    if s.starts_with('"') {
        match serde_json::from_str::<Cow<str>>(s) {
            Ok(v) => v,
            Err(_) => Cow::Borrowed(s),
        }
    } else {
        Cow::Borrowed(s)
    }
}

impl Shape {
    fn holds(&self, fields: &Fields, skip: Option<&str>) -> bool {
        self.terms.iter().all(|(col, values)| {
            Some(col.as_str()) == skip
                || fields
                    .get(col)
                    .is_some_and(|v| values.iter().any(|want| *want == text(v)))
        })
    }

    /// Whether the stored row `row` belongs to this shape, read without the index.
    pub fn matches(&self, row: &str) -> bool {
        let wanted = |k: &str| k == "table" || self.terms.contains_key(k);
        let table = |t: &str| t == self.table;
        match Fields::parse(row, &table, &wanted) {
            Some(f) => {
                f.get("table").map(text).as_deref() == Some(self.table.as_str())
                    && self.holds(&f, None)
            }
            None => false,
        }
    }
}

/// Each shape indexed under the values of its first term: table, column, value, shapes.
pub struct Index {
    shapes: Vec<Shape>,
    probe: HashMap<String, HashMap<String, HashMap<String, Vec<u32>>>>,
    /// Every column any shape names: the only fields of a row that are kept as it is read.
    columns: std::collections::HashSet<String>,
}

impl Index {
    pub fn new(shapes: Vec<Shape>) -> Result<Index> {
        let mut probe: HashMap<String, HashMap<String, HashMap<String, Vec<u32>>>> = HashMap::new();
        for (id, shape) in shapes.iter().enumerate() {
            let Some((col, values)) = shape.terms.iter().next() else {
                bail!("shape {id} has no term: S0 takes value terms only");
            };
            let by_value = probe
                .entry(shape.table.clone())
                .or_default()
                .entry(col.clone())
                .or_default();
            for v in values {
                let ids = by_value.entry(v.clone()).or_default();
                if ids.last() != Some(&(id as u32)) {
                    ids.push(id as u32);
                }
            }
        }
        let mut columns: std::collections::HashSet<String> = shapes
            .iter()
            .flat_map(|s| s.terms.keys().cloned())
            .collect();
        columns.insert("table".to_string());
        Ok(Index {
            shapes,
            probe,
            columns,
        })
    }

    pub fn shapes(&self) -> &[Shape] {
        &self.shapes
    }

    /// Every shape the stored row `row` belongs to, into `out`; the row is parsed once.
    pub fn matching(&self, row: &str, out: &mut Vec<u32>) {
        out.clear();
        let wanted = |k: &str| self.columns.contains(k);
        let table = |t: &str| self.probe.contains_key(t);
        let Some(fields) = Fields::parse(row, &table, &wanted) else {
            return;
        };
        let Some(table) = fields.get("table").map(text) else {
            return;
        };
        let Some(columns) = self.probe.get(table.as_ref()) else {
            return;
        };
        for (col, by_value) in columns {
            let Some(v) = fields.get(col) else {
                continue;
            };
            let Some(ids) = by_value.get(text(v).as_ref()) else {
                continue;
            };
            for &id in ids {
                if self.shapes[id as usize].holds(&fields, Some(col)) {
                    out.push(id);
                }
            }
        }
    }
}

/// Frames a reading client may fall behind by before it is closed and must refetch.
pub const ATTACHED_QUEUE: usize = 4096;
/// Frames kept for a shape nobody is reading, oldest dropped first, so the bench charges every
/// shape's queue whether or not a socket drains it.
pub const DETACHED_QUEUE: usize = 64;

#[derive(Default)]
struct Queue {
    frames: VecDeque<Bytes>,
    /// Which attachment reads it, 0 for none.
    reader: u64,
    /// The reader fell behind; it is sent a refetch and closed.
    closed: bool,
    /// Its reader's own wake-up, so a delta wakes only the streams it touched.
    notify: Arc<tokio::sync::Notify>,
}

/// What one filtered delta cost.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct DeltaCost {
    pub generation: u64,
    pub rows: u32,
    pub frames: u32,
    pub filter_ns: u64,
    pub store_apply_ns: u64,
    /// The follower's whole apply of the delta, as `feed_metrics` times it, filter excluded.
    pub apply_ns: u64,
    /// Waiting for the hub while a client attached, excluded from both.
    pub lock_wait_ns: u64,
}

/// One dataset's shapes on one follower: their queues, and the copy they are answered from.
pub struct Hub {
    index: Arc<Index>,
    queues: Vec<Queue>,
    generation: (u64, u64),
    store: Option<Arc<crate::feed::MemStore>>,
    attachments: u64,
    pub costs: Vec<DeltaCost>,
    scratch: Vec<u32>,
}

/// The first line of a stream: the shape's rows at `g`, and where the copy's hot window began.
#[derive(Serialize, Deserialize)]
pub struct Initial {
    pub g: u64,
    pub sealed_through: Option<u64>,
    pub rows: Vec<String>,
}

impl Hub {
    pub fn new(index: Arc<Index>) -> Hub {
        let n = index.shapes.len();
        Hub {
            index,
            queues: (0..n).map(|_| Queue::default()).collect(),
            generation: (0, 0),
            store: None,
            attachments: 0,
            costs: Vec::new(),
            scratch: Vec::new(),
        }
    }

    /// A new copy: every reader must refetch, and what is queued is from a copy no longer held.
    pub fn reset(&mut self, store: Arc<crate::feed::MemStore>) {
        self.generation = store.applied();
        self.store = Some(store);
        for q in &mut self.queues {
            q.frames.clear();
            if q.reader != 0 {
                q.closed = true;
                q.notify.notify_one();
            }
        }
    }

    /// Route one applied delta's changes to every shape they touch; the frames appended.
    pub fn route(&mut self, generation: (u64, u64), changes: &crate::feed::Changes) -> u32 {
        self.generation = generation;
        let mut frames = 0u32;
        let mut ids = std::mem::take(&mut self.scratch);
        for (put, rows) in [(false, &changes.removed), (true, &changes.added)] {
            for row in rows {
                self.index.matching(row, &mut ids);
                if ids.is_empty() {
                    continue;
                }
                let frame = frame(generation.1, put, row);
                for &id in &ids {
                    let q = &mut self.queues[id as usize];
                    if q.closed {
                        continue;
                    }
                    if q.reader == 0 {
                        if q.frames.len() == DETACHED_QUEUE {
                            q.frames.pop_front();
                        }
                    } else if q.frames.len() == ATTACHED_QUEUE {
                        q.closed = true;
                        q.frames.clear();
                        continue;
                    }
                    q.frames.push_back(frame.clone());
                    if q.reader != 0 {
                        q.notify.notify_one();
                    }
                    frames += 1;
                }
            }
        }
        self.scratch = ids;
        frames
    }

    /// Start reading shape `id`: its rows now, at the generation every later frame follows.
    pub fn attach(&mut self, id: usize) -> Result<(u64, Initial, Arc<tokio::sync::Notify>)> {
        let shape = self.index.shapes.get(id).context("no such shape")?.clone();
        let store = self.store.clone().context("no copy yet")?;
        let tables = store.tables();
        let sealed_through: Option<u64> = tables
            .meta
            .get("sealed_through")
            .and_then(|v| v.parse().ok());
        // `/sql` withholds a hot row at or under the watermark once its table has a segment, since
        // the segment holds that range; a hot copy can still carry one.
        let sealed = store
            .copy()
            .upgrade()
            .and_then(|c| c.catalogue())
            .and_then(|raw| serde_json::from_str::<crate::seal::Manifest>(&raw).ok())
            .is_some_and(|m| m.tables.get(&shape.table).is_some_and(|s| !s.is_empty()));
        let floor = sealed_through.filter(|_| sealed).unwrap_or(0);
        let rows = tables
            .entities
            .iter()
            .filter(|(key, _)| {
                !sealed || key.get(..12).and_then(|b| b.parse::<u64>().ok()) > Some(floor)
            })
            .map(|(_, r)| r)
            .filter(|r| shape.matches(r))
            .cloned()
            .collect();
        self.attachments += 1;
        let q = &mut self.queues[id];
        q.frames.clear();
        q.closed = false;
        q.reader = self.attachments;
        Ok((
            q.reader,
            Initial {
                g: self.generation.1,
                sealed_through,
                rows,
            },
            q.notify.clone(),
        ))
    }

    /// What reader `reader` of shape `id` has waiting, or `None` once it must refetch.
    pub fn take(&mut self, id: usize, reader: u64) -> Option<Vec<Bytes>> {
        let q = &mut self.queues[id];
        if q.reader != reader || q.closed {
            return None;
        }
        Some(q.frames.drain(..).collect())
    }

    pub fn detach(&mut self, id: usize, reader: u64) {
        let q = &mut self.queues[id];
        if q.reader == reader {
            *q = Queue::default();
        }
    }

    pub fn generation(&self) -> (u64, u64) {
        self.generation
    }
}

fn frame(generation: u64, put: bool, row: &str) -> Bytes {
    let row = serde_json::to_string(row).expect("a string serialises");
    let op = if put { "put" } else { "del" };
    Bytes::from(format!("{{\"g\":{generation},\"{op}\":{row}}}\n"))
}

/// The shapes a follower was started with, and a hub per dataset it holds.
pub struct Registry {
    index: Arc<Index>,
    hubs: Mutex<HashMap<String, Arc<Mutex<Hub>>>>,
}

static REGISTRY: OnceLock<Registry> = OnceLock::new();

/// The registry, if this process was started with `NUTHATCH_SHAPES_S0`.
pub fn registry() -> Option<&'static Registry> {
    REGISTRY.get()
}

/// `dataset`'s hub, when shapes are on, under its last path segment (its NID), which a URL can carry.
pub fn hub(dataset: &str) -> Option<Arc<Mutex<Hub>>> {
    let r = registry()?;
    let dataset = dataset.rsplit('/').next().unwrap_or(dataset);
    let mut hubs = r.hubs.lock().unwrap_or_else(|e| e.into_inner());
    Some(
        hubs.entry(dataset.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(Hub::new(r.index.clone()))))
            .clone(),
    )
}

/// Read `NUTHATCH_SHAPES_S0` and, when it is set, load its shapes and serve their streams on
/// `NUTHATCH_SHAPES_S0_LISTEN` (default `127.0.0.1:8290`).
pub async fn start_from_env() -> Result<()> {
    let Some(path) = std::env::var_os("NUTHATCH_SHAPES_S0") else {
        return Ok(());
    };
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("reading the S0 shapes file {}", path.to_string_lossy()))?;
    let shapes: Vec<Shape> = serde_json::from_str(&raw).context("parsing the S0 shapes file")?;
    let n = shapes.len();
    let index = Arc::new(Index::new(shapes)?);
    if REGISTRY
        .set(Registry {
            index,
            hubs: Mutex::default(),
        })
        .is_err()
    {
        bail!("S0 shapes were already loaded");
    }
    let listen =
        std::env::var("NUTHATCH_SHAPES_S0_LISTEN").unwrap_or_else(|_| "127.0.0.1:8290".to_string());
    let listener = tokio::net::TcpListener::bind(&listen)
        .await
        .with_context(|| format!("binding the S0 shape stream on {listen}"))?;
    tracing::warn!(shapes = n, %listen, "RFC-0080 S0 spike: filtering deltas for shapes");
    tokio::spawn(async move {
        let _ = axum::serve(listener, router()).await;
    });
    Ok(())
}

fn router() -> axum::Router {
    use axum::routing::get;
    axum::Router::new()
        .route("/datasets", get(datasets))
        .route("/stats/{dataset}", get(stats))
        .route("/shape/{dataset}/{id}", get(stream))
}

async fn datasets() -> axum::Json<serde_json::Value> {
    let Some(r) = registry() else {
        return axum::Json(serde_json::json!([]));
    };
    let hubs = r.hubs.lock().unwrap_or_else(|e| e.into_inner()).clone();
    let out: Vec<_> = hubs
        .iter()
        .map(|(d, h)| {
            let h = h.lock().unwrap_or_else(|e| e.into_inner());
            serde_json::json!({"dataset": d, "generation": h.generation.1,
                "shapes": h.queues.len(), "ready": h.store.is_some()})
        })
        .collect();
    axum::Json(serde_json::json!(out))
}

async fn stats(
    axum::extract::Path(dataset): axum::extract::Path<String>,
) -> axum::Json<Vec<DeltaCost>> {
    let costs = hub(&dataset)
        .map(|h| h.lock().unwrap_or_else(|e| e.into_inner()).costs.clone())
        .unwrap_or_default();
    axum::Json(costs)
}

async fn stream(
    axum::extract::Path((dataset, id)): axum::extract::Path<(String, usize)>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let Some(hub) = hub(&dataset) else {
        return (axum::http::StatusCode::NOT_FOUND, "shapes are off").into_response();
    };
    let (reader, initial, notify) = {
        let mut h = hub.lock().unwrap_or_else(|e| e.into_inner());
        match h.attach(id) {
            Ok(attached) => attached,
            Err(e) => return (axum::http::StatusCode::CONFLICT, e.to_string()).into_response(),
        }
    };
    let mut first = serde_json::to_vec(&initial).expect("serialises");
    first.push(b'\n');
    let guard = Detach {
        hub: hub.clone(),
        id,
        reader,
    };
    let body = futures::stream::unfold(
        (Some(Bytes::from(first)), guard, notify, false),
        move |(first, guard, notify, done)| async move {
            if done {
                return None;
            }
            if let Some(first) = first {
                return Some((Ok::<_, std::io::Error>(first), (None, guard, notify, false)));
            }
            loop {
                let woken = notify.clone();
                let waiting = woken.notified();
                let got = guard
                    .hub
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take(guard.id, guard.reader);
                match got {
                    None => {
                        let line = Bytes::from_static(b"{\"refetch\":true}\n");
                        return Some((Ok(line), (None, guard, notify, true)));
                    }
                    Some(frames) if !frames.is_empty() => {
                        let line = Bytes::from(frames.concat());
                        return Some((Ok(line), (None, guard, notify, false)));
                    }
                    Some(_) => {
                        let _ = tokio::time::timeout(Duration::from_secs(1), waiting).await;
                    }
                }
            }
        },
    );
    axum::response::Response::builder()
        .header("content-type", "application/x-ndjson")
        .body(axum::body::Body::from_stream(body))
        .expect("a valid response")
}

struct Detach {
    hub: Arc<Mutex<Hub>>,
    id: usize,
    reader: u64,
}

impl Drop for Detach {
    fn drop(&mut self) {
        self.hub
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .detach(self.id, self.reader);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feed::MemStore;
    use crate::store::Store;
    use proptest::prelude::*;
    use std::collections::BTreeSet;

    const TABLE: &str = "swap";

    fn row(block: u64, log: u64, sender: u64, fee: u64, table: &str) -> (String, String) {
        (
            Store::entity_key(block, log),
            serde_json::json!({"table": table, "block_number": block, "log_index": log,
                "sender": format!("0x{sender:040x}"), "fee": fee, "amount0": "-12"})
            .to_string(),
        )
    }

    fn shape(senders: &[u64], fees: Option<&[u64]>) -> Shape {
        let mut terms = BTreeMap::new();
        terms.insert(
            "sender".to_string(),
            senders.iter().map(|s| format!("0x{s:040x}")).collect(),
        );
        if let Some(f) = fees {
            terms.insert("fee".to_string(), f.iter().map(u64::to_string).collect());
        }
        Shape {
            table: TABLE.to_string(),
            terms,
        }
    }

    /// A client's copy: a count per stored row, as the Node client keeps it.
    #[derive(Default)]
    struct Client {
        reader: u64,
        rows: BTreeMap<String, i64>,
        g: u64,
        refetched: u32,
    }

    impl Client {
        fn attach(hub: &mut Hub, id: usize) -> Client {
            let (reader, initial, _) = hub.attach(id).unwrap();
            Client {
                reader,
                rows: initial.rows.into_iter().map(|r| (r, 1)).collect(),
                g: initial.g,
                refetched: 0,
            }
        }

        fn read(&mut self, hub: &mut Hub, id: usize) {
            let Some(frames) = hub.take(id, self.reader) else {
                let again = Client::attach(hub, id);
                *self = Client {
                    refetched: self.refetched + 1,
                    ..again
                };
                return;
            };
            for f in frames {
                let v: serde_json::Value = serde_json::from_slice(&f).unwrap();
                let g = v["g"].as_u64().unwrap();
                assert!(g >= self.g, "frames arrive in generation order");
                self.g = g;
                let (row, by) = match (v.get("put"), v.get("del")) {
                    (Some(r), None) => (r.as_str().unwrap().to_string(), 1),
                    (None, Some(r)) => (r.as_str().unwrap().to_string(), -1),
                    _ => panic!("a frame is one put or one del: {v}"),
                };
                let n = self.rows.entry(row.clone()).or_default();
                *n += by;
                if *n == 0 {
                    self.rows.remove(&row);
                }
            }
        }
    }

    #[derive(Clone, Debug)]
    enum Step {
        /// Blocks past the tip, each with this many rows.
        Window(Vec<Vec<(u64, u64, bool)>>),
        /// Roll back this many blocks, at most to the sealed watermark.
        Reorg(u64),
        /// Seal and prune this many blocks above the watermark.
        Seal(u64),
        /// A client of this shape drops and attaches afresh.
        Reattach(usize),
    }

    fn step(shapes: usize) -> impl Strategy<Value = Step> {
        let r = (0u64..12, 0u64..4, any::<bool>());
        prop_oneof![
            4 => prop::collection::vec(prop::collection::vec(r, 0..6), 1..4).prop_map(Step::Window),
            2 => (1u64..6).prop_map(Step::Reorg),
            1 => (1u64..4).prop_map(Step::Seal),
            1 => (0..shapes).prop_map(Step::Reattach),
        ]
    }

    fn shapes() -> Vec<Shape> {
        vec![
            shape(&[1], None),
            shape(&[2, 3], None),
            shape(&[1, 4, 7], None),
            shape(&[5], Some(&[0, 2])),
            shape(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], Some(&[3])),
            shape(&[11], None),
            // Never matches: a sender outside the range rows are drawn from.
            shape(&[99], None),
        ]
    }

    /// Every stored row of `TABLE` the shape holds, hot and sealed, from the writer.
    fn canonical(store: &Store, sealed: &BTreeSet<String>, s: &Shape) -> BTreeSet<String> {
        let sub = crate::store::feed_subscribe(store).unwrap();
        sub.snapshot
            .entities
            .map
            .values()
            .chain(sealed.iter())
            .filter(|r| s.matches(r))
            .cloned()
            .collect()
    }

    fn run(steps: Vec<Step>) -> std::result::Result<(), TestCaseError> {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(&tmp.path().join("nuthatch.redb")).unwrap();
        let mut sub = crate::store::feed_subscribe(&store).unwrap();
        let copy = Arc::new(MemStore::from_snapshot(tmp.path(), sub.snapshot.clone()));
        let index = Arc::new(Index::new(shapes()).unwrap());
        let n = index.shapes().len();
        let mut hub = Hub::new(index.clone());
        hub.reset(copy.clone());
        let mut clients: Vec<Client> = (0..n).map(|i| Client::attach(&mut hub, i)).collect();
        // Rows a seal moved out of hot: still the shape's, and never retracted.
        let mut sealed_rows: BTreeSet<String> = BTreeSet::new();
        let (mut tip, mut sealed) = (0u64, 0u64);
        for step in steps {
            match step {
                Step::Window(blocks) => {
                    let mut rows = Vec::new();
                    for b in blocks {
                        tip += 1;
                        for (log, (sender, fee, other)) in b.into_iter().enumerate() {
                            let table = if other { "other" } else { TABLE };
                            rows.push(row(tip, log as u64, sender, fee, table));
                        }
                    }
                    store.commit_window(&rows, None, tip).unwrap();
                }
                Step::Reorg(depth) => {
                    let to = tip.saturating_sub(depth).max(sealed);
                    store.rollback_to(to).unwrap();
                    tip = to;
                }
                Step::Seal(k) => {
                    let to = (sealed + k).min(tip);
                    if to > sealed {
                        let hot = crate::store::feed_subscribe(&store).unwrap().snapshot;
                        for (key, r) in &hot.entities.map {
                            let b: u64 = key[..12].parse().unwrap();
                            if b > sealed && b <= to {
                                sealed_rows.insert(r.clone());
                            }
                        }
                        store
                            .prune_and_set_meta(sealed + 1, to, "sealed_through", &to.to_string())
                            .unwrap();
                        sealed = to;
                    }
                }
                Step::Reattach(i) => {
                    hub.detach(i, clients[i].reader);
                    let fresh = Client::attach(&mut hub, i);
                    // A fresh client starts from hot; what it had sealed it takes from the sealed
                    // answer, which is S2's. Here it keeps the sealed rows it already held.
                    let mut rows = fresh.rows;
                    for r in clients[i].rows.keys() {
                        if sealed_rows.contains(r) {
                            rows.insert(r.clone(), 1);
                        }
                    }
                    clients[i] = Client { rows, ..fresh };
                }
            }
            while let Ok(delta) = sub.deltas.try_recv() {
                let changes = copy.apply(&delta).unwrap();
                hub.route(copy.applied(), &changes);
            }
            for (i, c) in clients.iter_mut().enumerate() {
                c.read(&mut hub, i);
                prop_assert_eq!(c.refetched, 0, "no client fell behind in this suite");
                let want = canonical(&store, &sealed_rows, &index.shapes()[i]);
                let held: BTreeSet<String> = c.rows.keys().cloned().collect();
                prop_assert!(
                    c.rows.values().all(|n| *n == 1),
                    "shape {}: a row held other than once",
                    i
                );
                prop_assert_eq!(held, want, "shape {} after a step", i);
            }
        }
        Ok(())
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

        /// RFC-0080 S0's gate: after any sequence of windows, reorgs of random depth and seals, every
        /// client's copy is the shape's canonical rows, hot and sealed, with each row held once.
        #[test]
        fn every_client_copy_is_its_canonical_rows_through_random_reorgs(
            steps in prop::collection::vec(step(7), 1..40)
        ) {
            run(steps)?;
        }
    }

    #[test]
    fn a_row_is_found_under_its_first_term_and_checked_against_the_rest() {
        let index = Index::new(shapes()).unwrap();
        let (_, r) = row(1, 0, 5, 2, TABLE);
        let mut out = Vec::new();
        index.matching(&r, &mut out);
        assert_eq!(out, vec![3]);
        let (_, r) = row(1, 0, 5, 3, TABLE);
        index.matching(&r, &mut out);
        out.sort();
        assert_eq!(out, vec![4]);
        let (_, r) = row(1, 0, 1, 0, "other");
        index.matching(&r, &mut out);
        assert!(out.is_empty(), "another table's row");
    }

    #[test]
    fn a_reader_that_falls_behind_is_closed_and_refetches() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(&tmp.path().join("nuthatch.redb")).unwrap();
        let mut sub = crate::store::feed_subscribe(&store).unwrap();
        let copy = Arc::new(MemStore::from_snapshot(tmp.path(), sub.snapshot.clone()));
        let mut hub = Hub::new(Arc::new(Index::new(vec![shape(&[1], None)]).unwrap()));
        hub.reset(copy.clone());
        let (reader, _, _) = hub.attach(0).unwrap();
        for b in 1..=(ATTACHED_QUEUE as u64 + 1) {
            store
                .commit_window(&[row(b, 0, 1, 0, TABLE)], None, b)
                .unwrap();
            let changes = copy.apply(&sub.deltas.try_recv().unwrap()).unwrap();
            hub.route(copy.applied(), &changes);
        }
        assert!(
            hub.take(0, reader).is_none(),
            "closed, not blocking the follower"
        );
    }

    /// RFC-0080 S0, a measurement: `route` alone over the fixture's deltas, every shape attached and
    /// no task waiting, so what it times is the parse, the probe, the frames and the queue pushes and
    /// not the wake-ups. `S0_SHAPES` is a shapes file and `S0_ROWS` stored rows, one a line; each
    /// delta is 20 swap rows and 180 of other tables, as the fixture commits a block.
    #[test]
    #[ignore]
    fn s0_route_cost() {
        let shapes: Vec<Shape> = serde_json::from_str(
            &std::fs::read_to_string(std::env::var("S0_SHAPES").unwrap()).unwrap(),
        )
        .unwrap();
        let rows = std::fs::read_to_string(std::env::var("S0_ROWS").unwrap()).unwrap();
        let (swaps, others): (Vec<&str>, Vec<&str>) = rows
            .lines()
            .partition(|r| r.starts_with("{\"table\":\"pool_manager__swap\""));
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(&tmp.path().join("nuthatch.redb")).unwrap();
        let sub = crate::store::feed_subscribe(&store).unwrap();
        let copy = Arc::new(MemStore::from_snapshot(tmp.path(), sub.snapshot));
        let n = shapes.len();
        let mut hub = Hub::new(Arc::new(Index::new(shapes).unwrap()));
        hub.reset(copy);
        let readers: Vec<u64> = (0..n).map(|i| hub.attach(i).unwrap().0).collect();
        let mut times = Vec::new();
        let mut frames = 0u64;
        for d in 0..300usize {
            let mut added: Vec<String> = Vec::with_capacity(200);
            for i in 0..20 {
                added.push(swaps[(d * 20 + i) % swaps.len()].to_string());
            }
            for i in 0..180 {
                added.push(others[(d * 180 + i) % others.len()].to_string());
            }
            let changes = crate::feed::Changes {
                added,
                ..Default::default()
            };
            let t = std::time::Instant::now();
            frames += u64::from(hub.route((0, d as u64 + 1), &changes));
            times.push(t.elapsed().as_nanos() as u64);
            for (i, r) in readers.iter().enumerate() {
                hub.take(i, *r);
            }
        }
        times.sort_unstable();
        println!(
            "route over {} deltas: p50 {} us, p99 {} us, {} frames a delta",
            times.len(),
            times[times.len() / 2] / 1000,
            times[times.len() * 99 / 100] / 1000,
            frames / times.len() as u64
        );
    }
}
