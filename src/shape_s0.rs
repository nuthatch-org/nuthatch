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
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

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

/// A field as the text a term names: a string's contents, anything else its JSON.
fn text(raw: &RawValue) -> Cow<'_, str> {
    text_of(raw.get())
}

/// [`text`] over a field's JSON as a column carries it.
fn text_of(s: &str) -> Cow<'_, str> {
    if s.starts_with('"') {
        match serde_json::from_str::<Cow<str>>(s) {
            Ok(v) => v,
            Err(_) => Cow::Borrowed(s),
        }
    } else {
        Cow::Borrowed(s)
    }
}

/// The JSON texts a column can carry for a term's value `v`: a string of it, or `v` itself.
fn raw_forms(v: &str) -> [String; 2] {
    [
        serde_json::Value::String(v.to_string()).to_string(),
        v.to_string(),
    ]
}

/// A term checked against a column's JSON without parsing it, unless it is escaped.
struct RawTerm {
    col: String,
    raw: HashSet<String>,
    text: HashSet<String>,
}

impl RawTerm {
    fn admits(&self, raw: &str) -> bool {
        if raw.contains('\\') {
            self.text.contains(text_of(raw).as_ref())
        } else {
            self.raw.contains(raw)
        }
    }
}

type ByValue = HashMap<String, Vec<u32>>;

/// Each shape indexed under the values of its first term: table, column, value, shapes.
pub struct Index {
    shapes: Vec<Shape>,
    probe: HashMap<String, HashMap<String, ByValue>>,
    /// Every column any shape names: the only fields of a row that are kept as it is read.
    columns: HashSet<String>,
    /// `probe` keyed as a delta's columns carry it: the table's JSON, then each value's JSON forms.
    by_raw: HashMap<String, Vec<(String, ByValue)>>,
    /// Every shape's terms past its first, for checking a column row the probe found.
    rest: Vec<Vec<RawTerm>>,
}

impl Index {
    pub fn new(shapes: Vec<Shape>) -> Result<Index> {
        let mut probe: HashMap<String, HashMap<String, ByValue>> = HashMap::new();
        let mut rest = Vec::with_capacity(shapes.len());
        for (id, shape) in shapes.iter().enumerate() {
            let mut terms = shape.terms.iter();
            let Some((col, values)) = terms.next() else {
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
            rest.push(
                terms
                    .map(|(col, values)| RawTerm {
                        col: col.clone(),
                        raw: values.iter().flat_map(|v| raw_forms(v)).collect(),
                        text: values.iter().cloned().collect(),
                    })
                    .collect(),
            );
        }
        let mut by_raw: HashMap<String, Vec<(String, ByValue)>> = HashMap::new();
        for (table, cols) in &probe {
            let raw_table = serde_json::Value::String(table.clone()).to_string();
            let entry = by_raw.entry(raw_table).or_default();
            for (col, by_value) in cols {
                let mut raw: ByValue = HashMap::new();
                for (v, ids) in by_value {
                    for form in raw_forms(v) {
                        let at = raw.entry(form).or_default();
                        for id in ids {
                            if !at.contains(id) {
                                at.push(*id);
                            }
                        }
                    }
                }
                entry.push((col.clone(), raw));
            }
        }
        let mut columns: HashSet<String> = shapes
            .iter()
            .flat_map(|s| s.terms.keys().cloned())
            .collect();
        columns.insert("table".to_string());
        Ok(Index {
            shapes,
            probe,
            columns,
            by_raw,
            rest,
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

    /// Every `(put, shape)` a decoded delta's puts match, probing its Arrow columns: a table no
    /// shape names is passed over a batch at a time, and a shaped one costs a hash probe per row
    /// per probed column. Rows a batch sent whole, or batches sent as rows, are read as JSON.
    pub fn matching_columns(
        &self,
        cols: &crate::feed_wire::PutColumns,
        added: &[String],
        out: &mut Vec<(u32, u32)>,
    ) {
        use arrow::array::{Array, StringArray};
        let mut ids = Vec::new();
        let mut by_json = |ord: u32, out: &mut Vec<(u32, u32)>| {
            self.matching(&added[ord as usize], &mut ids);
            out.extend(ids.iter().map(|&id| (ord, id)));
        };
        for (batch, ords) in cols.batches.iter().zip(&cols.rows) {
            let Some(batch) = batch else {
                for &ord in ords {
                    by_json(ord, out);
                }
                continue;
            };
            let utf8 = |name: &str| {
                batch
                    .column_by_name(name)
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            };
            let Some(whole) = batch.column(1).as_any().downcast_ref::<StringArray>() else {
                continue;
            };
            // The encoder groups a batch by its rows' `table` text, so one row names them all.
            let table = utf8("table").and_then(|t| {
                (0..t.len())
                    .find(|&r| !whole.is_valid(r) && t.is_valid(r))
                    .map(|r| t.value(r))
            });
            let Some(table) = table else {
                for &ord in ords {
                    by_json(ord, out);
                }
                continue;
            };
            let Some(probes) = self.by_raw.get(table) else {
                continue;
            };
            if whole.null_count() < whole.len() {
                for (r, &ord) in ords.iter().enumerate() {
                    if whole.is_valid(r) {
                        by_json(ord, out);
                    }
                }
            }
            let named: Vec<(&str, &StringArray)> = self
                .columns
                .iter()
                .filter_map(|c| utf8(c).map(|a| (c.as_str(), a)))
                .collect();
            let get = |name: &str| named.iter().find(|(c, _)| *c == name).map(|(_, a)| *a);
            for (col, by_value) in probes {
                let Some(values) = get(col) else {
                    continue;
                };
                let text_ids = self
                    .probe
                    .get(text_of(table).as_ref())
                    .and_then(|c| c.get(col));
                for (r, &ord) in ords.iter().enumerate() {
                    if whole.is_valid(r) || !values.is_valid(r) {
                        continue;
                    }
                    let v = values.value(r);
                    let found = if v.contains('\\') {
                        text_ids.and_then(|t| t.get(text_of(v).as_ref()))
                    } else {
                        by_value.get(v)
                    };
                    for &id in found.into_iter().flatten() {
                        let holds = self.rest[id as usize].iter().all(|t| {
                            get(&t.col).is_some_and(|a| a.is_valid(r) && t.admits(a.value(r)))
                        });
                        if holds {
                            out.push((ord, id));
                        }
                    }
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

/// How a delta's rows are read for the filter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Filter {
    /// The first S0's: every changed row's JSON.
    Json,
    /// The second S0's: the delta's Arrow columns, JSON only for what was not sent as columns.
    Columns,
}

#[derive(Default)]
struct Queue {
    frames: VecDeque<Bytes>,
    /// Which attachment reads it, 0 for none.
    reader: u64,
    /// The reader fell behind; it is sent a refetch and closed.
    closed: bool,
}

/// One shape's queue, its reader's wake-up, and whether a wake-up is already owed to it.
#[derive(Default)]
pub struct Slot {
    q: Mutex<Queue>,
    notify: tokio::sync::Notify,
    owed: AtomicBool,
}

impl Slot {
    fn lock(&self) -> std::sync::MutexGuard<'_, Queue> {
        self.q.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// What one round of delivery cost: the streams woken, the time to wake them, and the time from
/// the first hand-over it served to the last wake-up.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct DeliveryCost {
    pub streams: u32,
    pub wake_ns: u64,
    pub lag_ns: u64,
}

/// Wakes the streams a delta touched, off the apply path: the apply hands their ids over and
/// signals once, and never waits on this or on any reader.
pub struct Delivery {
    slots: Arc<[Slot]>,
    owed: Mutex<(Vec<u32>, Option<Instant>)>,
    wake: tokio::sync::Notify,
    pub costs: Mutex<Vec<DeliveryCost>>,
}

impl Delivery {
    fn new(slots: Arc<[Slot]>) -> Delivery {
        Delivery {
            slots,
            owed: Mutex::default(),
            wake: tokio::sync::Notify::new(),
            costs: Mutex::default(),
        }
    }

    fn hand(&self, ids: &mut Vec<u32>) {
        let mut owed = self.owed.lock().unwrap_or_else(|e| e.into_inner());
        if owed.0.is_empty() {
            owed.1 = Some(Instant::now());
        }
        owed.0.append(ids);
        drop(owed);
        self.wake.notify_one();
    }

    /// Wake every stream handed over since the last round; how many.
    pub fn deliver(&self) -> usize {
        let (ids, since) = {
            let mut owed = self.owed.lock().unwrap_or_else(|e| e.into_inner());
            (std::mem::take(&mut owed.0), owed.1.take())
        };
        if ids.is_empty() {
            return 0;
        }
        let t = Instant::now();
        for &id in &ids {
            let slot = &self.slots[id as usize];
            slot.owed.store(false, Ordering::SeqCst);
            slot.notify.notify_one();
        }
        let cost = DeliveryCost {
            streams: ids.len() as u32,
            wake_ns: t.elapsed().as_nanos() as u64,
            lag_ns: since.map_or(0, |s| s.elapsed().as_nanos() as u64),
        };
        self.costs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(cost);
        ids.len()
    }

    async fn run(self: Arc<Self>) {
        loop {
            self.wake.notified().await;
            self.deliver();
        }
    }
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
    slots: Arc<[Slot]>,
    filter: Filter,
    /// `None` wakes each stream inline as its frame is pushed, as the first S0 did.
    delivery: Option<Arc<Delivery>>,
    generation: (u64, u64),
    store: Option<Arc<crate::feed::MemStore>>,
    attachments: u64,
    /// Readers closed for falling behind.
    pub closed: u64,
    pub costs: Vec<DeltaCost>,
    scratch: Vec<u32>,
    matched: Vec<(u32, u32)>,
    touched: Vec<u32>,
}

/// The first line of a stream: the shape's rows at `g`, and where the copy's hot window began.
#[derive(Serialize, Deserialize)]
pub struct Initial {
    pub g: u64,
    pub sealed_through: Option<u64>,
    pub rows: Vec<String>,
}

impl Hub {
    /// The first S0's hub: JSON rows, readers woken inline.
    pub fn new(index: Arc<Index>) -> Hub {
        Hub::with(index, Filter::Json, false)
    }

    /// With `delivery`, readers are woken by [`Delivery`], which the caller runs.
    pub fn with(index: Arc<Index>, filter: Filter, delivery: bool) -> Hub {
        let n = index.shapes.len();
        let slots: Arc<[Slot]> = (0..n).map(|_| Slot::default()).collect();
        Hub {
            index,
            delivery: delivery.then(|| Arc::new(Delivery::new(slots.clone()))),
            slots,
            filter,
            generation: (0, 0),
            store: None,
            attachments: 0,
            closed: 0,
            costs: Vec::new(),
            scratch: Vec::new(),
            matched: Vec::new(),
            touched: Vec::new(),
        }
    }

    pub fn delivery(&self) -> Option<&Arc<Delivery>> {
        self.delivery.as_ref()
    }

    /// A new copy: every reader must refetch, and what is queued is from a copy no longer held.
    pub fn reset(&mut self, store: Arc<crate::feed::MemStore>) {
        self.generation = store.applied();
        self.store = Some(store);
        for slot in self.slots.iter() {
            let mut q = slot.lock();
            q.frames.clear();
            if q.reader != 0 {
                q.closed = true;
                slot.notify.notify_one();
            }
        }
    }

    /// Route one applied delta's changes to every shape they touch, reading its puts from
    /// `delta`'s columns when the filter is [`Filter::Columns`] and they line up; the frames
    /// appended.
    pub fn route_delta(
        &mut self,
        generation: (u64, u64),
        delta: &crate::feed::Delta,
        changes: &crate::feed::Changes,
    ) -> u32 {
        let columns = delta
            .columns
            .as_deref()
            .filter(|_| self.filter == Filter::Columns)
            // Every put changed a row, so `added` is the puts in order; otherwise read the JSON.
            .filter(|c| c.puts as usize == changes.added.len());
        let Some(columns) = columns else {
            return self.route(generation, changes);
        };
        self.generation = generation;
        let mut matched = std::mem::take(&mut self.matched);
        matched.clear();
        let mut frames = self.route_rows(generation.1, false, &changes.removed);
        self.index
            .matching_columns(columns, &changes.added, &mut matched);
        matched.sort_unstable();
        let mut at = 0;
        while at < matched.len() {
            let ord = matched[at].0;
            let frame = frame(generation.1, true, &changes.added[ord as usize]);
            while at < matched.len() && matched[at].0 == ord {
                frames += self.push(matched[at].1, &frame);
                at += 1;
            }
        }
        self.matched = matched;
        self.handed();
        frames
    }

    /// Route one applied delta's changes, every row read as JSON; the frames appended.
    pub fn route(&mut self, generation: (u64, u64), changes: &crate::feed::Changes) -> u32 {
        self.generation = generation;
        let frames = self.route_rows(generation.1, false, &changes.removed)
            + self.route_rows(generation.1, true, &changes.added);
        self.handed();
        frames
    }

    fn route_rows(&mut self, g: u64, put: bool, rows: &[String]) -> u32 {
        let mut frames = 0;
        let mut ids = std::mem::take(&mut self.scratch);
        for row in rows {
            self.index.matching(row, &mut ids);
            if ids.is_empty() {
                continue;
            }
            let frame = frame(g, put, row);
            for &id in &ids {
                frames += self.push(id, &frame);
            }
        }
        self.scratch = ids;
        frames
    }

    /// Queue `frame` for shape `id`, and wake its reader or owe it a wake-up; 1 if it was queued.
    fn push(&mut self, id: u32, frame: &Bytes) -> u32 {
        let slot = &self.slots[id as usize];
        let mut q = slot.lock();
        if q.closed {
            return 0;
        }
        if q.reader == 0 {
            if q.frames.len() == DETACHED_QUEUE {
                q.frames.pop_front();
            }
        } else if q.frames.len() == ATTACHED_QUEUE {
            q.closed = true;
            q.frames.clear();
            self.closed += 1;
            drop(q);
            self.owe(id);
            return 0;
        }
        q.frames.push_back(frame.clone());
        let reading = q.reader != 0;
        drop(q);
        if reading {
            self.owe(id);
        }
        1
    }

    fn owe(&mut self, id: u32) {
        let slot = &self.slots[id as usize];
        match &self.delivery {
            None => slot.notify.notify_one(),
            Some(_) => {
                if !slot.owed.swap(true, Ordering::SeqCst) {
                    self.touched.push(id);
                }
            }
        }
    }

    /// Hand what this delta touched to delivery, which wakes it off the apply path.
    fn handed(&mut self) {
        if let Some(d) = &self.delivery {
            if !self.touched.is_empty() {
                d.hand(&mut self.touched);
            }
        }
    }

    /// Start reading shape `id`: its rows now, at the generation every later frame follows.
    pub fn attach(&mut self, id: usize) -> Result<(u64, Initial, Arc<[Slot]>)> {
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
        let mut q = self.slots[id].lock();
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
            self.slots.clone(),
        ))
    }

    /// What reader `reader` of shape `id` has waiting, or `None` once it must refetch.
    pub fn take(&self, id: usize, reader: u64) -> Option<Vec<Bytes>> {
        take(&self.slots, id, reader)
    }

    pub fn detach(&mut self, id: usize, reader: u64) {
        detach(&self.slots, id, reader);
    }

    pub fn generation(&self) -> (u64, u64) {
        self.generation
    }
}

fn take(slots: &[Slot], id: usize, reader: u64) -> Option<Vec<Bytes>> {
    let mut q = slots[id].lock();
    if q.reader != reader || q.closed {
        return None;
    }
    Some(q.frames.drain(..).collect())
}

fn detach(slots: &[Slot], id: usize, reader: u64) {
    let mut q = slots[id].lock();
    if q.reader == reader {
        *q = Queue::default();
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
    filter: Filter,
    delivery: bool,
    hubs: Mutex<HashMap<String, Arc<Mutex<Hub>>>>,
}

static REGISTRY: OnceLock<Registry> = OnceLock::new();

/// The registry, if this process was started with `NUTHATCH_SHAPES_S0`.
pub fn registry() -> Option<&'static Registry> {
    REGISTRY.get()
}

/// Whether a decoded delta keeps its Arrow columns for the filter.
pub fn keeps_columns() -> bool {
    registry().is_some_and(|r| r.filter == Filter::Columns)
}

/// `dataset`'s hub, when shapes are on, under its last path segment (its NID), which a URL can carry.
pub fn hub(dataset: &str) -> Option<Arc<Mutex<Hub>>> {
    let r = registry()?;
    let dataset = dataset.rsplit('/').next().unwrap_or(dataset);
    let mut hubs = r.hubs.lock().unwrap_or_else(|e| e.into_inner());
    Some(
        hubs.entry(dataset.to_string())
            .or_insert_with(|| {
                let hub = Hub::with(r.index.clone(), r.filter, r.delivery);
                if let (Some(d), Ok(rt)) = (hub.delivery(), tokio::runtime::Handle::try_current()) {
                    rt.spawn(d.clone().run());
                }
                Arc::new(Mutex::new(hub))
            })
            .clone(),
    )
}

/// Read `NUTHATCH_SHAPES_S0` and, when it is set, load its shapes and serve their streams on
/// `NUTHATCH_SHAPES_S0_LISTEN` (default `127.0.0.1:8290`). `NUTHATCH_SHAPES_S0_FILTER` is `json`
/// or `columns` (the default), and `NUTHATCH_SHAPES_S0_DELIVERY` is `inline` or `task` (the default).
pub async fn start_from_env() -> Result<()> {
    let Some(path) = std::env::var_os("NUTHATCH_SHAPES_S0") else {
        return Ok(());
    };
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("reading the S0 shapes file {}", path.to_string_lossy()))?;
    let shapes: Vec<Shape> = serde_json::from_str(&raw).context("parsing the S0 shapes file")?;
    let n = shapes.len();
    let index = Arc::new(Index::new(shapes)?);
    let filter = match std::env::var("NUTHATCH_SHAPES_S0_FILTER").as_deref() {
        Ok("json") => Filter::Json,
        Ok("columns") | Err(_) => Filter::Columns,
        Ok(other) => bail!("NUTHATCH_SHAPES_S0_FILTER is json or columns, not {other}"),
    };
    let delivery = match std::env::var("NUTHATCH_SHAPES_S0_DELIVERY").as_deref() {
        Ok("inline") => false,
        Ok("task") | Err(_) => true,
        Ok(other) => bail!("NUTHATCH_SHAPES_S0_DELIVERY is inline or task, not {other}"),
    };
    if REGISTRY
        .set(Registry {
            index,
            filter,
            delivery,
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
    tracing::warn!(shapes = n, %listen, ?filter, delivery, "RFC-0080 S0 spike: filtering deltas for shapes");
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
        .route("/delivery/{dataset}", get(delivery_stats))
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
                "shapes": h.slots.len(), "ready": h.store.is_some(), "closed": h.closed})
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

async fn delivery_stats(
    axum::extract::Path(dataset): axum::extract::Path<String>,
) -> axum::Json<Vec<DeliveryCost>> {
    let d = hub(&dataset).and_then(|h| {
        h.lock()
            .unwrap_or_else(|e| e.into_inner())
            .delivery()
            .cloned()
    });
    axum::Json(d.map_or_else(Vec::new, |d| {
        d.costs.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }))
}

async fn stream(
    axum::extract::Path((dataset, id)): axum::extract::Path<(String, usize)>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let Some(hub) = hub(&dataset) else {
        return (axum::http::StatusCode::NOT_FOUND, "shapes are off").into_response();
    };
    let (reader, initial, slots) = {
        let mut h = hub.lock().unwrap_or_else(|e| e.into_inner());
        match h.attach(id) {
            Ok(attached) => attached,
            Err(e) => return (axum::http::StatusCode::CONFLICT, e.to_string()).into_response(),
        }
    };
    let mut first = serde_json::to_vec(&initial).expect("serialises");
    first.push(b'\n');
    let guard = Detach { slots, id, reader };
    let body = futures::stream::unfold(
        (Some(Bytes::from(first)), guard, false),
        move |(first, guard, done)| async move {
            if done {
                return None;
            }
            if let Some(first) = first {
                return Some((Ok::<_, std::io::Error>(first), (None, guard, false)));
            }
            loop {
                let slots = guard.slots.clone();
                let waiting = slots[guard.id].notify.notified();
                match take(&guard.slots, guard.id, guard.reader) {
                    None => {
                        let line = Bytes::from_static(b"{\"refetch\":true}\n");
                        return Some((Ok(line), (None, guard, true)));
                    }
                    Some(frames) if !frames.is_empty() => {
                        let line = Bytes::from(frames.concat());
                        return Some((Ok(line), (None, guard, false)));
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
    slots: Arc<[Slot]>,
    id: usize,
    reader: u64,
}

impl Drop for Detach {
    fn drop(&mut self) {
        detach(&self.slots, self.id, self.reader);
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

    fn step(shapes: usize, rows: usize) -> impl Strategy<Value = Step> {
        let r = (0u64..12, 0u64..4, any::<bool>());
        prop_oneof![
            4 => prop::collection::vec(prop::collection::vec(r, 0..rows), 1..4).prop_map(Step::Window),
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

    fn run(
        steps: Vec<Step>,
        filter: Filter,
        delivery: bool,
    ) -> std::result::Result<(), TestCaseError> {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(&tmp.path().join("nuthatch.redb")).unwrap();
        let mut sub = crate::store::feed_subscribe(&store).unwrap();
        let copy = Arc::new(MemStore::from_snapshot(tmp.path(), sub.snapshot.clone()));
        let index = Arc::new(Index::new(shapes()).unwrap());
        let n = index.shapes().len();
        let mut hub = Hub::with(index.clone(), filter, delivery);
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
                // Through the wire, as a follower receives it, so the columns are the decoded ones.
                let body = crate::feed_wire::encode_delta(&delta).unwrap();
                let delta = crate::feed_wire::decode_delta_keeping(&body, None, true).unwrap();
                let changes = copy.apply(&delta).unwrap();
                hub.route_delta(copy.applied(), &delta, &changes);
            }
            if let Some(d) = hub.delivery() {
                d.deliver();
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
            steps in prop::collection::vec(step(7, 6), 1..40)
        ) {
            run(steps, Filter::Json, false)?;
        }

        /// The second S0: the same, with puts probed in the delta's columns and readers woken by
        /// delivery. Blocks carry up to 13 rows, so a table often reaches a batch of columns.
        #[test]
        fn every_client_copy_is_its_canonical_rows_through_random_reorgs_by_columns(
            steps in prop::collection::vec(step(7, 14), 1..40)
        ) {
            run(steps, Filter::Columns, true)?;
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

    fn put_delta(rows: &[String]) -> crate::feed::Delta {
        crate::feed::Delta {
            generation: 1,
            ops: rows
                .iter()
                .enumerate()
                .map(|(i, row)| crate::feed::Op::Put {
                    key: Store::entity_key(1, i as u64),
                    row: row.clone(),
                })
                .collect(),
            ..Default::default()
        }
    }

    fn puts_of(delta: &crate::feed::Delta) -> Vec<String> {
        delta
            .ops
            .iter()
            .filter_map(|op| match op {
                crate::feed::Op::Put { row, .. } => Some(row.clone()),
                _ => None,
            })
            .collect()
    }

    /// Probing a delta's columns finds exactly the `(put, shape)` pairs reading each row's JSON
    /// does, escaped values, rows sent whole and batches sent as rows included.
    #[test]
    fn the_columns_match_what_the_json_matches() {
        let mut shapes = shapes();
        shapes.push(Shape {
            table: TABLE.to_string(),
            terms: BTreeMap::from([("sender".to_string(), vec!["a\"b".to_string()])]),
        });
        shapes.push(Shape {
            table: TABLE.to_string(),
            terms: BTreeMap::from([
                ("fee".to_string(), vec!["3".to_string()]),
                (
                    "sender".to_string(),
                    vec!["a\"b".to_string(), "\u{e9}".to_string()],
                ),
            ]),
        });
        let index = Index::new(shapes).unwrap();
        let n = index.shapes().len() as u32;
        let mut rows: Vec<String> = (0..60u64)
            .map(|i| {
                row(
                    1,
                    i,
                    i % 13,
                    i % 4,
                    if i % 3 == 0 { "other" } else { TABLE },
                )
                .1
            })
            .collect();
        rows.push(r#"{"table":"swap","sender":"a\"b","fee":3}"#.to_string());
        rows.push(r#"{"table":"swap","sender":"é","fee":3}"#.to_string());
        rows.push(r#"{"table":"swap","sender":"é","fee":3}"#.to_string());
        // An escaped key is not split into columns, so its row travels whole.
        rows.push(
            r#"{"table":"swap","sender":"0x0000000000000000000000000000000000000001"}"#.to_string(),
        );
        // Fewer rows than make a batch of columns.
        for i in 0..3u64 {
            rows.push(row(1, 100 + i, i, 0, "tiny").1);
        }
        let body = crate::feed_wire::encode_delta(&put_delta(&rows)).unwrap();
        let delta = crate::feed_wire::decode_delta_keeping(&body, None, true).unwrap();
        let cols = delta.columns.as_deref().unwrap();
        assert!(
            cols.batches.iter().any(Option::is_some),
            "a batch of columns"
        );
        assert!(cols.batches.iter().any(Option::is_none), "a batch of rows");
        let added = puts_of(&delta);
        let mut by_columns = Vec::new();
        index.matching_columns(cols, &added, &mut by_columns);
        by_columns.sort_unstable();
        let mut by_json = Vec::new();
        let mut ids = Vec::new();
        for (ord, r) in added.iter().enumerate() {
            index.matching(r, &mut ids);
            by_json.extend(ids.iter().map(|&id| (ord as u32, id)));
        }
        by_json.sort_unstable();
        assert!(by_json.len() > 20, "the fixture matches: {}", by_json.len());
        let at = |r: &str| added.iter().position(|a| a == r).unwrap() as u32;
        assert!(by_json.contains(&(at(&rows[60]), n - 2)));
        assert!(by_json.contains(&(at(&rows[60]), n - 1)));
        assert!(by_json.contains(&(at(&rows[61]), n - 1)));
        assert!(by_json.contains(&(at(&rows[62]), n - 1)));
        assert!(by_json.iter().any(|(o, _)| *o == at(&rows[63])));
        assert_eq!(by_columns, by_json);
    }

    /// A stalled reader costs the apply nothing: its queue fills, it is closed, and routing never
    /// waits on it or on delivery, which nothing runs here.
    #[test]
    fn a_stalled_reader_never_delays_routing() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(&tmp.path().join("nuthatch.redb")).unwrap();
        let sub = crate::store::feed_subscribe(&store).unwrap();
        let copy = Arc::new(MemStore::from_snapshot(tmp.path(), sub.snapshot));
        let all = Shape {
            table: TABLE.to_string(),
            terms: BTreeMap::from([("table".to_string(), vec![TABLE.to_string()])]),
        };
        let mut hub = Hub::with(
            Arc::new(Index::new(vec![all]).unwrap()),
            Filter::Columns,
            true,
        );
        hub.reset(copy);
        let (reader, _, _) = hub.attach(0).unwrap();
        let rows: Vec<String> = (0..64u64).map(|i| row(1, i, 1, 0, TABLE).1).collect();
        let body = crate::feed_wire::encode_delta(&put_delta(&rows)).unwrap();
        let delta = crate::feed_wire::decode_delta_keeping(&body, None, true).unwrap();
        let changes = crate::feed::Changes {
            added: puts_of(&delta),
            ..Default::default()
        };
        let mut queued = 0;
        for g in 1..=(ATTACHED_QUEUE as u64 / 64 + 2) {
            queued += hub.route_delta((0, g), &delta, &changes);
        }
        assert_eq!(
            queued as usize, ATTACHED_QUEUE,
            "queued to the bound, then closed"
        );
        assert_eq!(hub.closed, 1);
        assert!(hub.take(0, reader).is_none(), "closed, must refetch");
        assert_eq!(
            hub.delivery().unwrap().deliver(),
            1,
            "one wake-up owed, however many frames"
        );
    }

    /// RFC-0080 S0, a measurement: `route` alone over the fixture's deltas, every shape attached and
    /// no task waiting, so what it times is the read, the probe, the frames and the queue pushes and
    /// not the wake-ups. The first S0's JSON filter and the second's columns run interleaved, delta
    /// by delta, on the same decoded deltas. `S0_SHAPES` is a shapes file and `S0_ROWS` stored
    /// rows, one a line; each delta is 20 swap rows and 180 of other tables, as the fixture commits
    /// a block.
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
        let index = Arc::new(Index::new(shapes).unwrap());
        let mut hubs = [
            Hub::with(index.clone(), Filter::Json, false),
            Hub::with(index, Filter::Columns, true),
        ];
        let readers: Vec<Vec<u64>> = hubs
            .iter_mut()
            .map(|hub| {
                hub.reset(copy.clone());
                (0..n).map(|i| hub.attach(i).unwrap().0).collect()
            })
            .collect();
        let mut times = [Vec::new(), Vec::new()];
        let mut split = [Vec::new(), Vec::new()];
        let mut frames = [0u64; 2];
        for d in 0..300usize {
            let mut added: Vec<String> = Vec::with_capacity(200);
            for i in 0..200 {
                added.push(if i % 10 == 8 {
                    swaps[(d * 20 + i / 10) % swaps.len()].to_string()
                } else {
                    others[(d * 180 + i - i / 10) % others.len()].to_string()
                });
            }
            let body = crate::feed_wire::encode_delta(&put_delta(&added)).unwrap();
            let delta = crate::feed_wire::decode_delta_keeping(&body, None, true).unwrap();
            let changes = crate::feed::Changes {
                added: puts_of(&delta),
                ..Default::default()
            };
            // The columns route split in two: the probe alone, and encoding its matched rows.
            let t = std::time::Instant::now();
            let mut matched = Vec::new();
            hubs[1].index.matching_columns(
                delta.columns.as_deref().unwrap(),
                &changes.added,
                &mut matched,
            );
            split[0].push(t.elapsed().as_nanos() as u64);
            let mut rows: Vec<u32> = matched.iter().map(|(o, _)| *o).collect();
            rows.sort_unstable();
            rows.dedup();
            let t = std::time::Instant::now();
            for o in &rows {
                std::hint::black_box(frame(1, true, &changes.added[*o as usize]));
            }
            split[1].push(t.elapsed().as_nanos() as u64);
            for (k, hub) in hubs.iter_mut().enumerate() {
                let t = std::time::Instant::now();
                frames[k] += u64::from(hub.route_delta((0, d as u64 + 1), &delta, &changes));
                times[k].push(t.elapsed().as_nanos() as u64);
                if let Some(dl) = hub.delivery() {
                    dl.deliver();
                }
                for (i, r) in readers[k].iter().enumerate() {
                    hub.take(i, *r);
                }
            }
        }
        for (k, name) in ["json", "columns"].iter().enumerate() {
            let t = &mut times[k];
            t.sort_unstable();
            println!(
                "{name}: route over {} deltas: p50 {} us, p99 {} us, {} frames a delta",
                t.len(),
                t[t.len() / 2] / 1000,
                t[t.len() * 99 / 100] / 1000,
                frames[k] / t.len() as u64
            );
        }
        for (k, name) in ["columns probe alone", "encoding its rows' frames"]
            .iter()
            .enumerate()
        {
            let t = &mut split[k];
            t.sort_unstable();
            println!("{name}: p50 {} us", t[t.len() / 2] / 1000);
        }
        assert_eq!(frames[0], frames[1], "both filters route the same frames");
    }
}
