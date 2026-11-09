//! Live mounts health - which nests and cursors are indexing, and which are quarantined (RFC-0026 §5).
//!
//! The `/nests` roster used to be a `serde_json::Value` built once at startup and cloned per request,
//! so it could not express "partly working" at all. This is the live handle that replaces it: the
//! ingestion loops write state changes here, the API reads them per request.
//!
//! The rule this exists to serve is narrow but absolute: **a quarantined unit must never look healthy.**
//! Its data is still served - a frozen view of the chain up to its last committed block is correct, just
//! not fresh - but every surface that describes it says so. `CLAUDE.md` forbids serving stale data *as
//! if healthy*; it does not forbid serving it.

use crate::metrics::now_unix;
use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

/// Why a unit is out of service, and whether it is coming back on its own.
#[derive(Clone, Debug)]
pub struct QuarantineInfo {
    /// `"nest"` - this nest alone failed. `"cursor"` - the whole chain cursor died, taking every nest
    /// mounted on it out of service with it.
    pub kind: &'static str,
    /// `"retryable"` - backing off, will be re-admitted. `"terminal"` - needs an operator; no retry.
    pub class: &'static str,
    /// The underlying error chain, which is what an operator actually needs in order to act.
    pub reason: String,
    pub since_unixtime: u64,
    /// Consecutive quarantines, driving the backoff curve.
    pub attempts: u32,
    /// When re-admission is due; `None` for a terminal fault.
    pub next_retry_unixtime: Option<u64>,
}

impl QuarantineInfo {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "kind": self.kind,
            "class": self.class,
            "reason": self.reason,
            "since_unixtime": self.since_unixtime,
            "attempts": self.attempts,
            "next_retry_unixtime": self.next_retry_unixtime,
        })
    }
}

/// The mounts's live health. Cheap to clone behind an `Arc`; every mutation is a short write lock.
#[derive(Default)]
pub struct RuntimeHealth {
    /// Nest name → its own quarantine. Absent means indexing.
    nests: RwLock<HashMap<String, QuarantineInfo>>,
    /// Chain → the cursor's quarantine. A dead cursor takes all its nests out of service, so this is
    /// consulted for any nest whose own entry is clear.
    cursors: RwLock<HashMap<String, QuarantineInfo>>,
    /// Nest name → the chain whose cursor drives it.
    chain_of: RwLock<HashMap<String, String>>,
    /// Nests that were registered with no cursor behind them. Their chain must not appear as
    /// `nuthatch_cursor_live` (#2755).
    uncursored: RwLock<HashSet<String>>,
    /// Nest name → how many times it has ever been quarantined. A monotonic counter, so a nest that
    /// flaps in and out is visible on a graph even though its *current* state keeps reading healthy.
    quarantine_count: RwLock<HashMap<String, u64>>,
    /// Alias → the mount whose dataset it shares (RFC-0032 §4). Two aliases over one nest identity
    /// are **one dataset indexed once**, so only the canonical mount is ever quarantined; the others
    /// must inherit its state or they would cheerfully report "indexing" while nothing is.
    shares: RwLock<HashMap<String, String>>,
}

impl RuntimeHealth {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that `nest` is driven by `chain`'s cursor, so a cursor fault can be attributed to it.
    pub fn register(&self, nest: &str, chain: &str) {
        self.chain_of
            .write()
            .unwrap()
            .insert(nest.to_string(), chain.to_string());
        self.uncursored.write().unwrap().remove(nest);
    }

    /// Record `nest` for quarantine without standing up a cursor for `chain` (#2755). An empty
    /// `chain` means the mount named none, and the scrape must not invent one.
    pub fn register_without_cursor(&self, nest: &str, chain: &str) {
        self.register(nest, chain);
        self.uncursored.write().unwrap().insert(nest.to_string());
    }

    /// Record that `alias` serves the same dataset as `canonical` (RFC-0032 §4), so it reports that
    /// dataset's health rather than a private, permanently-cheerful one of its own.
    pub fn register_alias(&self, alias: &str, canonical: &str, chain: &str) {
        let follows = self.uncursored.read().unwrap().contains(canonical);
        self.register(alias, chain);
        if follows {
            self.uncursored.write().unwrap().insert(alias.to_string());
        }
        self.shares
            .write()
            .unwrap()
            .insert(alias.to_string(), canonical.to_string());
    }

    /// Forget a mount that is gone, so `/metrics` stops describing it (#1648).
    pub fn unregister(&self, nest: &str) {
        self.chain_of.write().unwrap().remove(nest);
        self.uncursored.write().unwrap().remove(nest);
        self.shares.write().unwrap().remove(nest);
    }

    /// Make a former alias the mount its shared dataset is known by (#1549): a move took the old
    /// canonical name off the dataset, and this mount indexes it now.
    pub fn make_canonical(&self, nest: &str, chain: &str) {
        self.shares.write().unwrap().remove(nest);
        self.register(nest, chain);
    }

    /// Quarantine one nest. `next_retry_secs` is `None` for a terminal fault (§3).
    pub fn quarantine_nest(
        &self,
        nest: &str,
        reason: String,
        attempts: u32,
        next_retry_secs: Option<u64>,
    ) {
        let now = now_unix();
        *self
            .quarantine_count
            .write()
            .unwrap()
            .entry(nest.to_string())
            .or_insert(0) += 1;
        self.nests.write().unwrap().insert(
            nest.to_string(),
            QuarantineInfo {
                kind: "nest",
                class: if next_retry_secs.is_some() {
                    "retryable"
                } else {
                    "terminal"
                },
                reason,
                since_unixtime: now,
                attempts,
                next_retry_unixtime: next_retry_secs.map(|s| now.saturating_add(s)),
            },
        );
    }

    /// Clear a nest's quarantine - it is indexing again.
    pub fn mark_indexing(&self, nest: &str) {
        self.nests.write().unwrap().remove(nest);
    }

    /// Record a nest as **retired by the operator** (RFC-0027 §6) rather than quarantined by a fault.
    ///
    /// Reported with `class: "retired"` so `/nests` can say *why* a nest stopped indexing, and
    /// deliberately excluded from [`RuntimeHealth::unhealthy`] - readiness answers "is anything broken?",
    /// and an intended removal is not. Paging someone because an operator did what they meant to do is
    /// how alerting gets muted.
    /// Record a nest as suspended by the operator (#1548): paused on purpose, so not unhealthy.
    pub fn suspend_nest(&self, nest: &str) {
        self.nests.write().unwrap().insert(
            nest.to_string(),
            QuarantineInfo {
                kind: "nest",
                class: "suspended",
                reason: "suspended by the operator".to_string(),
                since_unixtime: now_unix(),
                attempts: 0,
                next_retry_unixtime: None,
            },
        );
    }

    pub fn retire_nest(&self, nest: &str) {
        self.nests.write().unwrap().insert(
            nest.to_string(),
            QuarantineInfo {
                kind: "nest",
                class: "retired",
                reason: "unmounted by the operator".to_string(),
                since_unixtime: now_unix(),
                attempts: 0,
                next_retry_unixtime: None,
            },
        );
    }

    /// Quarantine a whole chain cursor: every nest mounted on it is out of service until restart.
    pub fn quarantine_cursor(&self, chain: &str, reason: String) {
        self.cursors.write().unwrap().insert(
            chain.to_string(),
            QuarantineInfo {
                kind: "cursor",
                class: "terminal",
                reason,
                since_unixtime: now_unix(),
                attempts: 0,
                next_retry_unixtime: None,
            },
        );
    }

    /// Whether `chain`'s cursor has died. Its quarantine holds until restart, so a mount onto that
    /// chain must not quietly start a second cursor under it.
    pub fn cursor_quarantined(&self, chain: &str) -> bool {
        self.cursors.read().unwrap().contains_key(chain)
    }

    /// This nest's effective quarantine: its own if it has one, otherwise its cursor's. A nest that is
    /// itself fine but whose cursor died is still not indexing, and must not report that it is.
    pub fn status(&self, nest: &str) -> Option<QuarantineInfo> {
        // An alias resolves to the mount that actually indexes the shared dataset (RFC-0032 §4).
        // Only that one is ever quarantined, so asking about the alias directly would always say
        // "indexing" - the exact false-healthy report `/ready` exists to prevent.
        let canonical = self
            .shares
            .read()
            .unwrap()
            .get(nest)
            .cloned()
            .unwrap_or_else(|| nest.to_string());
        if let Some(q) = self.nests.read().unwrap().get(&canonical) {
            return Some(q.clone());
        }
        let chain = self.chain_of.read().unwrap().get(&canonical).cloned()?;
        self.cursors.read().unwrap().get(&chain).cloned()
    }

    /// `(health, quarantine)` for one nest, ready to merge into a roster entry.
    pub fn json_for(&self, nest: &str) -> (&'static str, Option<serde_json::Value>) {
        match self.status(nest) {
            Some(q) if q.class == "suspended" => ("suspended", Some(q.to_json())),
            Some(q) => ("quarantined", Some(q.to_json())),
            None => ("indexing", None),
        }
    }

    /// Every quarantined nest, as `(name, reason)` - the body of a 503 from `/ready`.
    pub fn unhealthy(&self) -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> = self
            .chain_of
            .read()
            .unwrap()
            .keys()
            // A retired nest is not unhealthy - it is absent on purpose (RFC-0027 §6). Including it
            // would hold `/ready` at 503 forever after an intended unmount.
            .filter_map(|n| {
                self.status(n)
                    .filter(|q| q.class != "retired" && q.class != "suspended")
                    .map(|q| (n.clone(), q.reason))
            })
            .collect();
        // Deterministic order so the readiness body is stable between polls.
        out.sort();
        out
    }

    /// Whether every registered nest is indexing. This is what mounts-root `/ready` answers.
    pub fn all_indexing(&self) -> bool {
        self.unhealthy().is_empty()
    }

    /// Prometheus lines for the runtime's health (RFC-0026 §5) - enough to alert on "anything
    /// quarantined" and to graph a nest that flaps.
    pub fn render_metrics(&self) -> String {
        let chain_of = self.chain_of.read().unwrap();
        if chain_of.is_empty() {
            return String::new();
        }
        let mut nests: Vec<(&String, &String)> = chain_of.iter().collect();
        nests.sort();
        let counts = self.quarantine_count.read().unwrap();

        let mut s = String::new();
        // This series always carries a chain label, so a mount that named no chain is counted
        // below and left off it (#2755).
        let health_lines: Vec<String> = nests
            .iter()
            .filter(|(_, chain)| !chain.is_empty())
            .map(|(nest, chain)| {
                let up = u8::from(self.status(nest).is_none());
                format!("nuthatch_nest_health{{nest=\"{nest}\",chain=\"{chain}\"}} {up}\n")
            })
            .collect();
        if !health_lines.is_empty() {
            s.push_str(
                "# HELP nuthatch_nest_health 1 when the nest is indexing, 0 when quarantined.\n\
                 # TYPE nuthatch_nest_health gauge\n",
            );
            for line in health_lines {
                s.push_str(&line);
            }
        }
        s.push_str(
            "# HELP nuthatch_nest_quarantine_total Times this nest has been quarantined since start.\n\
             # TYPE nuthatch_nest_quarantine_total counter\n",
        );
        for (nest, _) in &nests {
            let n = counts.get(*nest).copied().unwrap_or(0);
            s.push_str(&format!(
                "nuthatch_nest_quarantine_total{{nest=\"{nest}\"}} {n}\n"
            ));
        }

        let cursors = self.cursors.read().unwrap();
        let uncursored = self.uncursored.read().unwrap();
        // A chain with no cursor is left out of this gauge (#2755).
        let mut chains: Vec<&String> = nests
            .iter()
            .filter(|(nest, chain)| !chain.is_empty() && !uncursored.contains(*nest))
            .map(|(_, chain)| *chain)
            .collect();
        for chain in cursors.keys() {
            chains.push(chain);
        }
        drop(uncursored);
        chains.sort();
        chains.dedup();
        if chains.is_empty() {
            return s;
        }
        s.push_str(
            "# HELP nuthatch_cursor_live 1 while the chain's cursor is running, 0 once it has died.\n\
             # TYPE nuthatch_cursor_live gauge\n",
        );
        for chain in chains {
            let live = u8::from(!cursors.contains_key(chain));
            s.push_str(&format!(
                "nuthatch_cursor_live{{chain=\"{chain}\"}} {live}\n"
            ));
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_healthy_nest_reports_indexing_and_a_quarantined_one_never_does() {
        let h = RuntimeHealth::new();
        h.register("alpha", "base");
        h.register("beta", "base");
        assert!(h.all_indexing());
        assert_eq!(h.json_for("alpha").0, "indexing");

        h.quarantine_nest("alpha", "store write failed".into(), 2, Some(20));
        let (health, q) = h.json_for("alpha");
        assert_eq!(health, "quarantined");
        let q = q.unwrap();
        assert_eq!(q["class"], "retryable");
        assert_eq!(q["kind"], "nest");
        assert_eq!(q["attempts"], 2);
        assert!(
            q["next_retry_unixtime"].as_u64().unwrap() >= q["since_unixtime"].as_u64().unwrap()
        );

        // Its co-tenant is untouched - the whole point of per-nest quarantine.
        assert_eq!(h.json_for("beta").0, "indexing");
        // …but the runtime as a whole is not ready while anything is out.
        assert!(!h.all_indexing());
        assert_eq!(h.unhealthy().len(), 1);

        h.mark_indexing("alpha");
        assert!(h.all_indexing());
    }

    #[test]
    fn a_terminal_fault_has_no_retry_deadline() {
        let h = RuntimeHealth::new();
        h.register("alpha", "base");
        h.quarantine_nest("alpha", "finality violation".into(), 0, None);
        let q = h.status("alpha").unwrap();
        assert_eq!(q.class, "terminal");
        assert!(q.next_retry_unixtime.is_none());
    }

    /// A dead cursor takes every nest mounted on it out of service - including nests that never
    /// individually failed. Reporting those as "indexing" would be exactly the lie RFC-0026 forbids.
    #[test]
    fn a_dead_cursor_marks_all_its_nests_and_spares_the_other_chains() {
        let h = RuntimeHealth::new();
        h.register("alpha", "base");
        h.register("beta", "base");
        h.register("gamma", "arbitrum-one");

        h.quarantine_cursor(
            "base",
            "every nest on this cursor is terminally quarantined".into(),
        );

        for n in ["alpha", "beta"] {
            let (health, q) = h.json_for(n);
            assert_eq!(health, "quarantined", "{n} rides the dead cursor");
            assert_eq!(q.unwrap()["kind"], "cursor");
        }
        // The other chain's cursor is untouched - the per-cursor blast-radius rule, visible on the API.
        assert_eq!(h.json_for("gamma").0, "indexing");
        assert_eq!(
            h.unhealthy()
                .iter()
                .map(|(n, _)| n.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha", "beta"]
        );
    }

    /// The health surface must be scrapeable: alert on "anything quarantined", graph a flapping nest.
    #[test]
    fn metrics_expose_nest_health_cursor_liveness_and_a_flap_counter() {
        let h = RuntimeHealth::new();
        h.register("alpha", "base");
        h.register("gamma", "arbitrum-one");

        let m = h.render_metrics();
        assert!(
            m.contains("nuthatch_nest_health{nest=\"alpha\",chain=\"base\"} 1"),
            "{m}"
        );
        assert!(m.contains("nuthatch_cursor_live{chain=\"base\"} 1"), "{m}");
        assert!(
            m.contains("nuthatch_nest_quarantine_total{nest=\"alpha\"} 0"),
            "{m}"
        );

        // Flap alpha twice: current health returns to 1, but the counter remembers - which is the
        // whole point, since a nest that recovers between scrapes would otherwise look untroubled.
        for _ in 0..2 {
            h.quarantine_nest("alpha", "transient".into(), 0, Some(5));
            h.mark_indexing("alpha");
        }
        let m = h.render_metrics();
        assert!(
            m.contains("nuthatch_nest_health{nest=\"alpha\",chain=\"base\"} 1"),
            "{m}"
        );
        assert!(
            m.contains("nuthatch_nest_quarantine_total{nest=\"alpha\"} 2"),
            "{m}"
        );

        // A dead cursor shows as down, and drags its nest's health with it - the other chain is fine.
        h.quarantine_cursor("base", "cursor gone".into());
        let m = h.render_metrics();
        assert!(m.contains("nuthatch_cursor_live{chain=\"base\"} 0"), "{m}");
        assert!(
            m.contains("nuthatch_nest_health{nest=\"alpha\",chain=\"base\"} 0"),
            "{m}"
        );
        assert!(
            m.contains("nuthatch_cursor_live{chain=\"arbitrum-one\"} 1"),
            "{m}"
        );
        assert!(
            m.contains("nuthatch_nest_health{nest=\"gamma\",chain=\"arbitrum-one\"} 1"),
            "{m}"
        );
    }

    /// A mount that never got a cursor must not show one as live, including under a made-up chain
    /// name (#2755).
    #[test]
    fn a_nest_with_no_cursor_does_not_invent_a_live_cursor() {
        let h = RuntimeHealth::new();
        h.register("alpha", "base");
        h.register_without_cursor("gone", "base");
        h.quarantine_nest("gone", "did not load at boot".into(), 0, None);
        let m = h.render_metrics();
        assert!(m.contains("nuthatch_cursor_live{chain=\"base\"} 1"), "{m}");
        assert!(
            m.contains("nuthatch_nest_health{nest=\"gone\",chain=\"base\"} 0"),
            "{m}"
        );
        assert!(!m.contains("unknown"), "{m}");

        let only = RuntimeHealth::new();
        only.register_without_cursor("gone", "base");
        only.quarantine_nest("gone", "did not load at boot".into(), 0, None);
        let m = only.render_metrics();
        assert!(!m.contains("nuthatch_cursor_live"), "{m}");
        assert!(
            m.contains("nuthatch_nest_health{nest=\"gone\",chain=\"base\"} 0"),
            "{m}"
        );
        assert!(
            m.contains("nuthatch_nest_quarantine_total{nest=\"gone\"} 1"),
            "{m}"
        );

        let blank = RuntimeHealth::new();
        blank.register_without_cursor("gone", "");
        blank.quarantine_nest("gone", "did not load at boot".into(), 0, None);
        blank.register_alias("gone-alias", "gone", "");
        let m = blank.render_metrics();
        assert!(!m.contains("nuthatch_cursor_live"), "{m}");
        assert!(!m.contains("nuthatch_nest_health"), "{m}");
        assert!(
            m.contains("nuthatch_nest_quarantine_total{nest=\"gone\"} 1"),
            "{m}"
        );
        assert_eq!(blank.json_for("gone-alias").0, "quarantined");

        // A cursor that did start, and then died, still reports down.
        only.quarantine_cursor("base", "cursor gone".into());
        let m = only.render_metrics();
        assert!(m.contains("nuthatch_cursor_live{chain=\"base\"} 0"), "{m}");
    }

    /// A nest's own fault outranks its cursor's for reporting: it is the more specific cause, and the
    /// one the operator must act on.
    #[test]
    fn a_nests_own_fault_outranks_its_cursors() {
        let h = RuntimeHealth::new();
        h.register("alpha", "base");
        h.quarantine_nest(
            "alpha",
            "the balance IVM circuit thread has died".into(),
            0,
            None,
        );
        h.quarantine_cursor("base", "cursor gone".into());
        let q = h.status("alpha").unwrap();
        assert_eq!(q.kind, "nest");
        assert!(q.reason.contains("balance IVM"));
    }
}
