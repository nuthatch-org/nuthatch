//! What the watcher knows: the last body each route answered, folded in one read at a time, so a
//! live poll and a replayed recording reach the same state by the same path.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::prom::Metrics;

pub const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
pub const CURSOR_BUDGET_BYTES: f64 = 2.0 * GIB;

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

    pub fn bool(&self, key: &str) -> bool {
        self.value()
            .and_then(|b| b.get(key))
            .and_then(Value::as_bool)
            .unwrap_or(false)
    }

    pub fn str(&self, key: &str) -> Option<String> {
        self.value()?.get(key)?.as_str().map(str::to_string)
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

/// One mount of a runtime, as `/nests` lists it.
#[derive(Debug, Clone, PartialEq)]
pub struct Mount {
    pub name: String,
    pub chain: Option<String>,
}

#[derive(Default, Clone)]
pub struct App {
    pub ready: RouteState,
    pub root: RouteState,
    pub nests: RouteState,
    /// `/<name>/ready` for each of a runtime's mounts.
    pub nest_ready: BTreeMap<String, RouteState>,
    pub metrics: Option<(u64, Metrics)>,
    pub prev_metrics: Option<(u64, Metrics)>,
    pub metrics_status: Option<u16>,
}

impl App {
    pub fn apply(&mut self, read: &Read) {
        let t = read.t_ms;
        match read.route.as_str() {
            // A 503 still carries the reasons; only a non-answer is nothing.
            "/ready" => self.ready.take(t, read.status, &read.body, true),
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
                }
            }
            route => {
                if let Some(name) = route
                    .strip_prefix('/')
                    .and_then(|r| r.strip_suffix("/ready"))
                    .filter(|n| !n.is_empty() && !n.contains('/'))
                {
                    self.nest_ready.entry(name.to_string()).or_default().take(
                        t,
                        read.status,
                        &read.body,
                        true,
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

    /// This state with every route whose last read served nothing emptied, for a reader that has no
    /// STALE to show: the screen keeps the old figures and marks them, the line cannot.
    pub fn answered_only(&self) -> App {
        let mut app = self.clone();
        let fresh = |r: &mut RouteState| {
            if r.body.as_ref().map(|(t, _)| *t) != r.last_status.map(|(t, _)| t) {
                r.body = None;
            }
        };
        fresh(&mut app.ready);
        fresh(&mut app.root);
        fresh(&mut app.nests);
        app.nest_ready.values_mut().for_each(fresh);
        if app.metrics_status != Some(200) {
            app.metrics = None;
        }
        app
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
        m.label_values("nuthatch_nest_last_block", "nest")
            .into_iter()
            .next()
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
            .map(|(_, m)| m.label_values("nuthatch_rpc_endpoint_requests_total", "endpoint"))
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
            let (d, _) = self.delta("nuthatch_rpc_endpoint_failures_total", &[("endpoint", e)])?;
            if d > 0.0 {
                failing += 1;
            }
        }
        Some((endpoints.len() - failing, endpoints.len()))
    }

    /// HTTP requests a second the process served between the last two reads, this watcher's
    /// included.
    pub fn http_rps(&self) -> Option<f64> {
        let (d, secs) = self.delta("nuthatch_http_requests_total", &[])?;
        (secs > 0.0).then(|| d / secs)
    }

    /// The nest's own poll interval; in a runtime, the shortest of its mounts'.
    pub fn poll_interval_secs(&self) -> Option<u64> {
        let of = |r: &RouteState| {
            r.value()?
                .pointer("/freshness/poll_interval_secs")?
                .as_u64()
        };
        match self.mounts() {
            Some(_) => self.nest_ready.values().filter_map(of).min(),
            None => of(&self.ready),
        }
    }
}
