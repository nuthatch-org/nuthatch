//! What the watcher knows: the last body each route answered, folded in one read at a time, so a
//! live poll and a replayed recording reach the same state by the same path.

use std::collections::{BTreeMap, VecDeque};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::prom::Metrics;
use super::series;

pub const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
pub const CURSOR_BUDGET_BYTES: f64 = 2.0 * GIB;

/// Marks kept per strip: wider than any terminal shows.
const STRIP: usize = 160;

/// One read of one route: what a live poll produced, or one line of a recording.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Read {
    pub t_ms: u64,
    pub route: String,
    /// 0 when nothing answered.
    pub status: u16,
    pub body: String,
    /// The line mode's pass that made this read, so a recording can be checked against its lines.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cycle: Option<u64>,
}

#[derive(Debug, Default, Clone)]
pub struct RouteState {
    pub body: Option<(u64, Value)>,
    pub last_status: Option<(u64, u16)>,
}

impl RouteState {
    pub fn value(&self) -> Option<&Value> {
        self.body.as_ref().map(|(_, b)| b)
    }

    pub fn u64(&self, key: &str) -> Option<u64> {
        self.value()?.get(key)?.as_u64()
    }

    pub fn pointer_u64(&self, pointer: &str) -> Option<u64> {
        self.value()?.pointer(pointer)?.as_u64()
    }

    pub fn bool(&self, key: &str) -> bool {
        self.value()
            .and_then(|b| b.get(key))
            .and_then(Value::as_bool)
            .unwrap_or(false)
    }

    pub fn str(&self, key: &str) -> Option<String> {
        self.value()?.get(key)?.as_str().map(str::to_string)
    }

    pub fn read_at(&self) -> Option<u64> {
        self.body.as_ref().map(|(t, _)| *t)
    }

    fn take(&mut self, t: u64, status: u16, body: &str, keep_error_body: bool) {
        self.last_status = Some((t, status));
        if status == 200 || (keep_error_body && status != 0) {
            if let Ok(v) = serde_json::from_str::<Value>(body) {
                self.body = Some((t, v));
            }
        }
    }
}

/// The marks a nest's strips draw: one per poll that was seen to happen, one per advance of the
/// seal. Each is a delta between two reads, so nothing moves unless the nest did.
#[derive(Debug, Default, Clone)]
pub struct Track {
    /// Blocks each newly seen poll brought.
    pub arrivals: VecDeque<u64>,
    last_poll: Option<u64>,
    last_block: Option<u64>,
    /// Blocks each advance of `sealed_through` covered.
    pub stamps: VecDeque<u64>,
    last_sealed: Option<u64>,
}

impl Track {
    fn observe(&mut self, body: &Value) {
        let poll = body.get("last_poll_unixtime").and_then(Value::as_u64);
        let block = body.get("last_block").and_then(Value::as_u64);
        if let (Some(poll), Some(block)) = (poll, block) {
            if let (Some(prev_poll), Some(prev_block)) = (self.last_poll, self.last_block) {
                if poll > prev_poll {
                    push_capped(&mut self.arrivals, block.saturating_sub(prev_block));
                }
            }
            self.last_poll = Some(poll);
            self.last_block = Some(block);
        }
        if let Some(sealed) = body.get("sealed_through").and_then(Value::as_u64) {
            if let Some(prev) = self.last_sealed {
                if sealed > prev {
                    push_capped(&mut self.stamps, sealed - prev);
                }
            }
            self.last_sealed = Some(sealed);
        }
    }
}

fn push_capped(strip: &mut VecDeque<u64>, v: u64) {
    if strip.len() == STRIP {
        strip.pop_front();
    }
    strip.push_back(v);
}

/// One mount of a runtime, as `/nests` lists it.
#[derive(Debug, Clone, PartialEq)]
pub struct Mount {
    pub name: String,
    pub chain: Option<String>,
}

/// One request a nest served, as `/_admin/requests` lists it.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct Served {
    pub seq: u64,
    pub unix_ms: u64,
    pub route: String,
    #[serde(default)]
    pub name: Option<String>,
    pub status: u16,
    pub ms: f64,
}

/// What `/_admin/requests` has said: its last status, and the requests seen so far.
#[derive(Debug, Default, Clone)]
pub struct Dispatch {
    pub status: Option<u16>,
    pub served: VecDeque<Served>,
    pub last_seq: u64,
}

impl Dispatch {
    fn take(&mut self, status: u16, body: &str) {
        self.status = Some(status);
        if status != 200 {
            return;
        }
        let Ok(v) = serde_json::from_str::<Value>(body) else {
            return;
        };
        let last = v.get("last_seq").and_then(Value::as_u64).unwrap_or(0);
        // A nest that restarted counts from 1 again.
        if last < self.last_seq {
            self.served.clear();
            self.last_seq = 0;
        }
        let fresh: Vec<Served> = v
            .get("requests")
            .and_then(|r| serde_json::from_value(r.clone()).ok())
            .unwrap_or_default();
        for s in fresh.into_iter().filter(|s| s.seq > self.last_seq) {
            push_capped_served(&mut self.served, s);
        }
        self.last_seq = self.last_seq.max(last);
    }
}

fn push_capped_served(ring: &mut VecDeque<Served>, s: Served) {
    if ring.len() == STRIP {
        ring.pop_front();
    }
    ring.push_back(s);
}

/// The last interval in which an endpoint was asked anything: its mean latency and when.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Latency {
    pub mean_ms: f64,
    pub at_ms: u64,
}

#[derive(Default)]
pub struct App {
    pub ready: RouteState,
    pub root: RouteState,
    pub nests: RouteState,
    /// `/<name>/ready` for each of a runtime's mounts.
    pub nest_ready: BTreeMap<String, RouteState>,
    pub metrics: Option<(u64, Metrics)>,
    pub prev_metrics: Option<(u64, Metrics)>,
    pub metrics_status: Option<u16>,
    /// Keyed by mount name; a solo nest's is under "".
    pub tracks: BTreeMap<String, Track>,
    pub latency: BTreeMap<String, Latency>,
    /// Which of a runtime's mounts the screen shows.
    pub selected: usize,
    /// `/_admin/requests`, keyed as `tracks` is.
    pub dispatch: BTreeMap<String, Dispatch>,
}

/// The nest one engine room is drawn for: a solo nest, or one mount of a runtime.
pub struct NestView<'a> {
    /// The `nest` label its series carry, which is also its name.
    pub label: Option<String>,
    pub chain: Option<String>,
    pub ready: &'a RouteState,
    pub track: Option<&'a Track>,
    pub dispatch: Option<&'a Dispatch>,
}

impl App {
    pub fn apply(&mut self, read: &Read) {
        let t = read.t_ms;
        match read.route.as_str() {
            // A 503 still carries the reasons; only a non-answer is nothing.
            "/ready" => {
                self.ready.take(t, read.status, &read.body, true);
                if let Some(body) = self.ready.value() {
                    self.tracks.entry(String::new()).or_default().observe(body);
                }
            }
            "/" => self.root.take(t, read.status, &read.body, false),
            "/nests" => {
                self.nests.take(t, read.status, &read.body, false);
                if read.status == 404 {
                    self.nests.body = None;
                }
            }
            "/metrics" => {
                self.metrics_status = Some(read.status);
                if read.status == 200 {
                    let parsed = Metrics::parse(&read.body);
                    self.prev_metrics = self.metrics.replace((t, parsed));
                    self.remember_latency(t);
                }
            }
            "/_admin/requests" => self
                .dispatch
                .entry(String::new())
                .or_default()
                .take(read.status, &read.body),
            route => {
                if let Some(name) = route
                    .strip_prefix('/')
                    .and_then(|r| r.strip_suffix("/_admin/requests"))
                    .filter(|n| !n.is_empty() && !n.contains('/'))
                {
                    self.dispatch
                        .entry(name.to_string())
                        .or_default()
                        .take(read.status, &read.body);
                    return;
                }
                if let Some(name) = route
                    .strip_prefix('/')
                    .and_then(|r| r.strip_suffix("/ready"))
                    .filter(|n| !n.is_empty() && !n.contains('/'))
                {
                    let state = self.nest_ready.entry(name.to_string()).or_default();
                    state.take(t, read.status, &read.body, true);
                    if let Some(body) = state.value() {
                        self.tracks
                            .entry(name.to_string())
                            .or_default()
                            .observe(body);
                    }
                }
            }
        }
    }

    fn remember_latency(&mut self, t: u64) {
        for e in self.rpc_endpoints() {
            let l = [("endpoint", e.as_str())];
            let sum = self.delta(series::RPC_DURATION_SUM, &l).map(|d| d.0);
            let count = self.delta(series::RPC_DURATION_COUNT, &l).map(|d| d.0);
            if let (Some(sum), Some(count)) = (sum, count) {
                if count > 0.0 {
                    self.latency.insert(
                        e,
                        Latency {
                            mean_ms: 1000.0 * sum / count,
                            at_ms: t,
                        },
                    );
                }
            }
        }
    }

    /// The runtime's mounts, or `None` for a solo nest (whose `/nests` answers 404) and before
    /// `/nests` has answered at all.
    pub fn mounts(&self) -> Option<Vec<Mount>> {
        let body = self.nests.value()?;
        let list = body
            .get("nests")
            .and_then(Value::as_array)
            .or_else(|| body.as_array())?;
        Some(
            list.iter()
                .filter_map(|n| {
                    Some(Mount {
                        name: n.get("name")?.as_str()?.to_string(),
                        chain: n.get("chain").and_then(Value::as_str).map(str::to_string),
                    })
                })
                .collect(),
        )
    }

    /// Whether `/nests` has said which kind of process this is: a 404 is a solo nest.
    pub fn shape_known(&self) -> bool {
        matches!(self.nests.last_status, Some((_, 200 | 404)))
    }

    /// Cursors in the process: one per distinct chain in a runtime, one for a solo nest.
    pub fn cursors(&self) -> Option<usize> {
        if !self.shape_known() {
            return None;
        }
        let Some(mounts) = self.mounts() else {
            return Some(1);
        };
        let mut chains: Vec<String> = mounts.into_iter().filter_map(|m| m.chain).collect();
        chains.sort_unstable();
        chains.dedup();
        Some(chains.len())
    }

    pub fn budget_bytes(&self) -> Option<f64> {
        self.cursors().map(|c| CURSOR_BUDGET_BYTES * c as f64)
    }

    pub fn metric(&self, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
        self.metrics.as_ref()?.1.get(name, labels)
    }

    /// A solo nest's name, from the `nest` label its per-nest series carry: `/` calls every nest
    /// `nuthatch`.
    pub fn solo_name(&self) -> Option<String> {
        let (_, m) = self.metrics.as_ref()?;
        m.label_values(series::NEST_LAST_BLOCK, "nest")
            .into_iter()
            .next()
    }

    /// The nest the screen draws: the solo nest, or the selected mount.
    pub fn view(&self) -> NestView<'_> {
        match self.mounts().filter(|m| !m.is_empty()) {
            Some(mounts) => {
                let m = &mounts[self.selected.min(mounts.len() - 1)];
                NestView {
                    label: Some(m.name.clone()),
                    chain: m.chain.clone(),
                    ready: self.nest_ready.get(&m.name).unwrap_or(&EMPTY_ROUTE),
                    track: self.tracks.get(&m.name),
                    dispatch: self.dispatch.get(&m.name),
                }
            }
            None => NestView {
                label: self.solo_name(),
                chain: self.root.str("chain"),
                ready: &self.ready,
                track: self.tracks.get(""),
                dispatch: self.dispatch.get(""),
            },
        }
    }

    /// A per-nest series for `view`'s nest.
    pub fn nest_metric(&self, view: &NestView, name: &str) -> Option<f64> {
        self.metric(name, &[("nest", view.label.as_deref()?)])
    }

    /// The change in a series between the last two `/metrics` reads, and the seconds between them.
    pub fn delta(&self, name: &str, labels: &[(&str, &str)]) -> Option<(f64, f64)> {
        let (t1, now) = self.metrics.as_ref()?;
        let (t0, prev) = self.prev_metrics.as_ref()?;
        let secs = t1.saturating_sub(*t0) as f64 / 1000.0;
        Some((now.get(name, labels)? - prev.get(name, labels)?, secs))
    }

    pub fn rpc_endpoints(&self) -> Vec<String> {
        self.metrics
            .as_ref()
            .map(|(_, m)| m.label_values(series::RPC_REQUESTS, "endpoint"))
            .unwrap_or_default()
    }

    /// Endpoints whose failure count did not move between the last two reads, of all listed.
    pub fn rpc_up(&self) -> Option<(usize, usize)> {
        let endpoints = self.rpc_endpoints();
        if endpoints.is_empty() {
            return None;
        }
        let mut failing = 0;
        for e in &endpoints {
            let (d, _) = self.delta(series::RPC_FAILURES, &[("endpoint", e)])?;
            if d > 0.0 {
                failing += 1;
            }
        }
        Some((endpoints.len() - failing, endpoints.len()))
    }

    /// HTTP requests a second the process served between the last two reads, this watcher's
    /// included.
    pub fn http_rps(&self) -> Option<f64> {
        let (d, secs) = self.delta(series::HTTP_REQUESTS, &[])?;
        (secs > 0.0).then(|| d / secs)
    }

    /// The share of one core the process used between the last two reads.
    pub fn cpu_percent(&self) -> Option<f64> {
        let (d, secs) = self.delta(series::CPU, &[])?;
        (secs > 0.0).then(|| 100.0 * d / secs)
    }

    /// The nest's own poll interval; in a runtime, the shortest of its mounts'.
    pub fn poll_interval_secs(&self) -> Option<u64> {
        let of = |r: &RouteState| r.pointer_u64("/freshness/poll_interval_secs");
        match self.mounts() {
            Some(_) => self.nest_ready.values().filter_map(of).min(),
            None => of(&self.ready),
        }
    }

    /// Step to the next (or previous) mount of a runtime.
    pub fn step(&mut self, forward: bool) {
        let n = self.mounts().map_or(0, |m| m.len());
        if n == 0 {
            return;
        }
        self.selected = if forward {
            (self.selected + 1) % n
        } else {
            (self.selected + n - 1) % n
        };
    }
}

static EMPTY_ROUTE: RouteState = RouteState {
    body: None,
    last_status: None,
};
