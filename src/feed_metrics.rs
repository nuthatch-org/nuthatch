//! What the hot-state feed costs and how far behind its followers see (RFC-0076 FR17): visibility
//! lag measured on the writer's clock, delta bytes and encode time on the writer, apply time and
//! mirror fetches on a follower, and each connection's queue.
//!
//! **Visibility never compares two hosts' clocks.** The writer notes when each commit landed. A
//! follower acknowledges each generation it applies within 10 ms, in a numbered report, and a later
//! report carries the round trip that one took; the writer then records arrival minus commit minus
//! half the round trip of the report that carried the acknowledgement, which is when the follower
//! could first answer from it.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

const SECONDS: &[f64] = &[
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
    60.0,
];
const BYTES: &[f64] = &[
    256.0, 1024.0, 4096.0, 16384.0, 65536.0, 262144.0, 1048576.0, 4194304.0, 16777216.0,
];

struct Hist {
    bounds: &'static [f64],
    /// Per bound, not cumulative, then `+Inf`; the sum; the count.
    counts: Vec<u64>,
    sum: f64,
    n: u64,
}

impl Hist {
    fn new(bounds: &'static [f64]) -> Hist {
        Hist {
            bounds,
            counts: vec![0; bounds.len() + 1],
            sum: 0.0,
            n: 0,
        }
    }

    fn observe(&mut self, v: f64) {
        let at = self
            .bounds
            .iter()
            .position(|b| v <= *b)
            .unwrap_or(self.bounds.len());
        self.counts[at] += 1;
        self.sum += v;
        self.n += 1;
    }

    fn render(&self, s: &mut String, name: &str, labels: &str) {
        let sep = if labels.is_empty() { "" } else { "," };
        let mut cum = 0;
        for (i, b) in self.bounds.iter().enumerate() {
            cum += self.counts[i];
            s.push_str(&format!("{name}_bucket{{{labels}{sep}le=\"{b}\"}} {cum}\n"));
        }
        cum += self.counts[self.bounds.len()];
        s.push_str(&format!(
            "{name}_bucket{{{labels}{sep}le=\"+Inf\"}} {cum}\n"
        ));
        let braces = |l: &str| {
            if l.is_empty() {
                String::new()
            } else {
                format!("{{{l}}}")
            }
        };
        s.push_str(&format!("{name}_sum{} {}\n", braces(labels), self.sum));
        s.push_str(&format!("{name}_count{} {}\n", braces(labels), self.n));
    }
}

/// A family of histograms keyed by their label text.
struct Family {
    bounds: &'static [f64],
    series: Mutex<BTreeMap<String, Hist>>,
}

impl Family {
    const fn new(bounds: &'static [f64]) -> Family {
        Family {
            bounds,
            series: Mutex::new(BTreeMap::new()),
        }
    }

    fn observe(&self, labels: String, v: f64) {
        let mut series = self.series.lock().unwrap_or_else(|e| e.into_inner());
        series
            .entry(labels)
            .or_insert_with(|| Hist::new(self.bounds))
            .observe(v);
    }

    fn render(&self, s: &mut String, name: &str, help: &str) {
        let series = self.series.lock().unwrap_or_else(|e| e.into_inner());
        if series.is_empty() {
            return;
        }
        s.push_str(&format!("# HELP {name} {help}\n# TYPE {name} histogram\n"));
        for (labels, h) in series.iter() {
            h.render(s, name, labels);
        }
    }

    #[cfg(test)]
    fn count(&self, labels: &str) -> u64 {
        let series = self.series.lock().unwrap();
        series.get(labels).map_or(0, |h| h.n)
    }
}

fn label(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// Which kind of delta a visibility observation is: seals wait on the mirror and are reported apart.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Tip,
    Seal,
}

impl Kind {
    fn label(self) -> &'static str {
        match self {
            Kind::Tip => "tip",
            Kind::Seal => "seal",
        }
    }
}

struct Commit {
    epoch: u64,
    generation: u64,
    at: Instant,
    kind: Kind,
}

/// The most commits per dataset remembered for an acknowledgement to be matched against.
const REMEMBERED: usize = 4096;

/// The most acknowledgements per connection waiting for their report's round trip.
const WAITING: usize = 4096;
/// The most round trips one report is read for.
pub const TRIPS_CARRIED: usize = 64;

/// A generation acknowledged in report `seq`, waiting for that report's round trip.
struct Waiting {
    seq: u64,
    dataset: String,
    kind: Kind,
    since_commit: Duration,
}

#[derive(Default)]
struct Visibility {
    commits: HashMap<String, VecDeque<Commit>>,
    /// Per connection and dataset, the last `(epoch, generation)` acknowledged.
    acked: HashMap<(u64, String), (u64, u64)>,
    waiting: HashMap<u64, VecDeque<Waiting>>,
}

/// What an acknowledgement is corrected by for the report's own trip to the writer.
#[derive(Clone, Copy, Debug)]
pub enum Trip {
    /// A follower of 5.14: the round trip its previous report took.
    Previous(Option<Duration>),
    /// Report `seq`, whose own round trip a later report carries.
    Own(u64),
}

/// `rtts` as a report carries it, `<seq>:<µs>` comma-separated; anything else is skipped.
pub fn parse_trips(rtts: &str) -> Vec<(u64, Duration)> {
    rtts.split(',')
        .filter_map(|t| {
            let (seq, us) = t.split_once(':')?;
            Some((seq.parse().ok()?, Duration::from_micros(us.parse().ok()?)))
        })
        .take(TRIPS_CARRIED)
        .collect()
}

/// Every feed figure this process keeps.
pub struct FeedMetrics {
    visibility_seconds: Family,
    delta_bytes: Family,
    encode_seconds: Family,
    apply_seconds: Family,
    seal_wait_seconds: Family,
    visibility: Mutex<Visibility>,
    conns: Mutex<BTreeMap<u64, Weak<AtomicU64>>>,
    mirror_files: AtomicU64,
    mirror_bytes: AtomicU64,
    mirror_refused: AtomicU64,
    mirror_missing: AtomicU64,
}

pub fn metrics() -> &'static FeedMetrics {
    static M: OnceLock<FeedMetrics> = OnceLock::new();
    M.get_or_init(|| FeedMetrics {
        visibility_seconds: Family::new(SECONDS),
        delta_bytes: Family::new(BYTES),
        encode_seconds: Family::new(SECONDS),
        apply_seconds: Family::new(SECONDS),
        seal_wait_seconds: Family::new(SECONDS),
        visibility: Mutex::default(),
        conns: Mutex::default(),
        mirror_files: AtomicU64::new(0),
        mirror_bytes: AtomicU64::new(0),
        mirror_refused: AtomicU64::new(0),
        mirror_missing: AtomicU64::new(0),
    })
}

impl FeedMetrics {
    fn vis(&self) -> std::sync::MutexGuard<'_, Visibility> {
        self.visibility.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// A delta was sent for `dataset`: remember when it was committed, once however many followers
    /// it goes to.
    pub fn committed(&self, dataset: &str, epoch: u64, generation: u64, at: Instant, kind: Kind) {
        let mut v = self.vis();
        let ring = v.commits.entry(dataset.to_string()).or_default();
        if ring
            .back()
            .is_some_and(|c| c.epoch == epoch && c.generation >= generation)
        {
            return;
        }
        if ring.back().is_some_and(|c| c.epoch != epoch) {
            ring.clear();
        }
        ring.push_back(Commit {
            epoch,
            generation,
            at,
            kind,
        });
        while ring.len() > REMEMBERED {
            ring.pop_front();
        }
    }

    /// A follower's report on connection `conn`, received `now`: what it has applied, the number
    /// `seq` a later follower gives it, and the round trips of its earlier reports (`rtts`), or a
    /// 5.14 follower's previous round trip (`rtt_us`).
    pub fn reported(
        &self,
        conn: u64,
        positions: &BTreeMap<String, (u64, u64)>,
        seq: Option<u64>,
        rtt_us: Option<u64>,
        rtts: Option<&str>,
        now: Instant,
    ) {
        for (seq, rtt) in parse_trips(rtts.unwrap_or_default()) {
            self.round_trip(conn, seq, rtt);
        }
        let trip = match seq {
            Some(seq) => Trip::Own(seq),
            None => Trip::Previous(rtt_us.map(Duration::from_micros)),
        };
        for (dataset, at) in positions {
            self.acked(conn, dataset, *at, trip, now);
        }
    }

    /// Connection `conn` acknowledged `dataset` at `(epoch, generation)`, received `now`. Every
    /// generation newly acknowledged is observed once, now or when `trip`'s round trip arrives; the
    /// first acknowledgement on a connection only sets where it stands, since what it covers arrived
    /// in a snapshot. Returns how many generations were newly acknowledged.
    pub fn acked(
        &self,
        conn: u64,
        dataset: &str,
        at: (u64, u64),
        trip: Trip,
        now: Instant,
    ) -> usize {
        let mut v = self.vis();
        // Only datasets this writer has sent deltas of: a report names whatever it likes.
        if !v.commits.contains_key(dataset) {
            return 0;
        }
        let key = (conn, dataset.to_string());
        // Reports travel concurrently, so one may arrive behind a later one. Epochs are not ordered:
        // a change of epoch is a step forward only onto the one this writer commits now.
        let current = v
            .commits
            .get(dataset)
            .and_then(|r| r.back())
            .map(|c| c.epoch);
        if v.acked.get(&key).is_some_and(|p| {
            if p.0 == at.0 {
                p.1 >= at.1
            } else {
                current != Some(at.0)
            }
        }) {
            return 0;
        }
        let Some(prev) = v.acked.insert(key, at) else {
            return 0;
        };
        if matches!(trip, Trip::Previous(None)) {
            return 0;
        }
        let Some(ring) = v.commits.get(dataset) else {
            return 0;
        };
        let mut seen = Vec::new();
        for c in ring {
            let fresh = c.epoch == at.0
                && c.generation <= at.1
                && (prev.0 != at.0 || c.generation > prev.1);
            if fresh {
                seen.push((c.kind, now.saturating_duration_since(c.at)));
            }
        }
        let n = seen.len();
        match trip {
            Trip::Own(seq) => {
                let waiting = v.waiting.entry(conn).or_default();
                for (kind, since_commit) in seen {
                    waiting.push_back(Waiting {
                        seq,
                        dataset: dataset.to_string(),
                        kind,
                        since_commit,
                    });
                }
                while waiting.len() > WAITING {
                    waiting.pop_front();
                }
            }
            Trip::Previous(rtt) => {
                drop(v);
                let rtt = rtt.unwrap_or_default();
                for (kind, since_commit) in seen {
                    self.observe(dataset, kind, since_commit, rtt);
                }
            }
        }
        n
    }

    /// Report `seq` on connection `conn` took `rtt` there and back: observe what it acknowledged.
    pub fn round_trip(&self, conn: u64, seq: u64, rtt: Duration) -> usize {
        let mut v = self.vis();
        let Some(waiting) = v.waiting.get_mut(&conn) else {
            return 0;
        };
        let mut done = Vec::new();
        waiting.retain(|w| {
            if w.seq == seq {
                done.push((w.dataset.clone(), w.kind, w.since_commit));
            }
            w.seq != seq
        });
        drop(v);
        for (dataset, kind, since_commit) in &done {
            self.observe(dataset, *kind, *since_commit, rtt);
        }
        done.len()
    }

    // `since_commit` is on the writer's clock and `rtt` is a duration the follower timed, so no two
    // clocks meet; halving the carrying report's own trip assumes only that its two legs match.
    fn observe(&self, dataset: &str, kind: Kind, since_commit: Duration, rtt: Duration) {
        self.visibility_seconds.observe(
            format!("dataset=\"{}\",kind=\"{}\"", label(dataset), kind.label()),
            since_commit.saturating_sub(rtt / 2).as_secs_f64(),
        );
    }

    /// Connection `conn` ended.
    pub fn gone(&self, conn: u64) {
        let mut v = self.vis();
        v.acked.retain(|(c, _), _| *c != conn);
        v.waiting.remove(&conn);
        drop(v);
        self.conns
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&conn);
    }

    pub fn delta_sent(&self, encoding: &str, bytes: usize) {
        self.delta_bytes
            .observe(format!("encoding=\"{encoding}\""), bytes as f64);
    }

    pub fn encoded(&self, encoding: &str, took: Duration) {
        self.encode_seconds
            .observe(format!("encoding=\"{encoding}\""), took.as_secs_f64());
    }

    pub fn applied(&self, took: Duration) {
        self.apply_seconds
            .observe(String::new(), took.as_secs_f64());
    }

    /// How long a sealing delta waited for the mirror to serve every file its catalogue names.
    pub fn seal_waited(&self, took: Duration) {
        self.seal_wait_seconds
            .observe(String::new(), took.as_secs_f64());
    }

    /// Connection `conn`'s queued bytes, read for as long as the connection lives.
    pub fn connection(&self, conn: u64, queued: &Arc<AtomicU64>) {
        self.conns
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(conn, Arc::downgrade(queued));
    }

    pub fn mirror_fetched(&self, bytes: u64) {
        self.mirror_files.fetch_add(1, Relaxed);
        self.mirror_bytes.fetch_add(bytes, Relaxed);
    }

    pub fn mirror_refused(&self) {
        self.mirror_refused.fetch_add(1, Relaxed);
    }

    pub fn mirror_missing(&self) {
        self.mirror_missing.fetch_add(1, Relaxed);
    }

    #[cfg(test)]
    pub fn visibility_count(&self, dataset: &str, kind: Kind) -> u64 {
        self.visibility_seconds.count(&format!(
            "dataset=\"{}\",kind=\"{}\"",
            label(dataset),
            kind.label()
        ))
    }

    pub fn render(&self, s: &mut String) {
        self.visibility_seconds.render(
            s,
            "nuthatch_feed_visibility_seconds",
            "From a writer's commit to a follower being able to answer from it, on the writer's clock (RFC-0076 FR17).",
        );
        self.delta_bytes.render(
            s,
            "nuthatch_feed_delta_bytes",
            "Bytes of one delta as sent on the feed, by encoding.",
        );
        self.encode_seconds.render(
            s,
            "nuthatch_feed_encode_seconds",
            "Time to encode one delta for the feed, by encoding; v2 is encoded once per delta.",
        );
        self.apply_seconds.render(
            s,
            "nuthatch_follower_apply_seconds",
            "Time a follower took to apply one delta, its derived state included.",
        );
        self.seal_wait_seconds.render(
            s,
            "nuthatch_follower_seal_wait_seconds",
            "Time a follower's sealing delta waited for the mirror to serve every file it names.",
        );
        let conns = self.conns.lock().unwrap_or_else(|e| e.into_inner());
        let live: Vec<(u64, u64)> = conns
            .iter()
            .filter_map(|(c, q)| Some((*c, q.upgrade()?.load(Relaxed))))
            .collect();
        if !live.is_empty() {
            s.push_str(
                "# HELP nuthatch_feed_connection_queue_bytes Encoded feed bytes queued for one follower connection.\n\
                 # TYPE nuthatch_feed_connection_queue_bytes gauge\n",
            );
            for (c, q) in live {
                s.push_str(&format!(
                    "nuthatch_feed_connection_queue_bytes{{conn=\"{c}\"}} {q}\n"
                ));
            }
        }
        drop(conns);
        let files = self.mirror_files.load(Relaxed);
        let (refused, missing) = (
            self.mirror_refused.load(Relaxed),
            self.mirror_missing.load(Relaxed),
        );
        if files + refused + missing > 0 {
            for (name, help, v) in [
                (
                    "nuthatch_follower_mirror_files_total",
                    "Files a follower fetched from the mirror and verified.",
                    files,
                ),
                (
                    "nuthatch_follower_mirror_bytes_total",
                    "Bytes a follower fetched from the mirror and verified.",
                    self.mirror_bytes.load(Relaxed),
                ),
                (
                    "nuthatch_follower_mirror_refused_total",
                    "Files a follower fetched whose bytes did not hash to what the catalogue names.",
                    refused,
                ),
                (
                    "nuthatch_follower_mirror_missing_total",
                    "Fetches of a file the mirror did not have yet.",
                    missing,
                ),
            ] {
                s.push_str(&format!(
                    "# HELP {name} {help}\n# TYPE {name} counter\n{name} {v}\n"
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn visibility_is_commit_to_ack_less_half_the_round_trip_and_counted_once() {
        let m = metrics();
        let t0 = Instant::now();
        let d = "data/vis-test";
        m.committed(d, 7, 1, t0, Kind::Tip);
        m.committed(d, 7, 2, t0, Kind::Tip);
        m.committed(d, 7, 2, t0, Kind::Tip);
        m.committed(d, 7, 3, t0, Kind::Seal);
        // A follower of 5.14 sends the round trip its previous report took.
        let rtt = Trip::Previous(Some(Duration::from_millis(20)));
        // The first acknowledgement only says where the connection stands.
        assert_eq!(m.acked(99, d, (7, 1), rtt, t0), 0);
        let later = t0 + Duration::from_millis(50);
        assert_eq!(m.acked(99, d, (7, 3), rtt, later), 2);
        assert_eq!(m.acked(99, d, (7, 3), rtt, later), 0, "never twice");
        assert_eq!(
            m.acked(99, d, (7, 2), rtt, later),
            0,
            "a late report is not a step back"
        );
        assert_eq!(m.acked(99, d, (7, 3), rtt, later), 0, "nor seen twice");
        assert_eq!(m.visibility_count(d, Kind::Tip), 1);
        assert_eq!(m.visibility_count(d, Kind::Seal), 1);
        let mut s = String::new();
        m.render(&mut s);
        let sum = s
            .lines()
            .find(|l| {
                l.starts_with(
                    "nuthatch_feed_visibility_seconds_sum{dataset=\"data/vis-test\",kind=\"tip\"}",
                )
            })
            .unwrap();
        let v: f64 = sum.rsplit(' ').next().unwrap().parse().unwrap();
        assert!((v - 0.040).abs() < 1e-9, "50 ms less half of 20: {v}");

        // The dataset's store reopened as epoch 8; a report still on its way from epoch 7 lands late.
        m.committed(d, 8, 1, t0, Kind::Tip);
        assert_eq!(m.acked(99, d, (8, 1), rtt, later), 1);
        assert_eq!(
            m.acked(99, d, (7, 3), rtt, later),
            0,
            "an old epoch is a step back"
        );
        assert_eq!(m.acked(99, d, (8, 1), rtt, later), 0, "nor seen twice");
        m.gone(99);
    }

    fn tip_sum(d: &str) -> f64 {
        let mut s = String::new();
        metrics().render(&mut s);
        let head = format!("nuthatch_feed_visibility_seconds_sum{{dataset=\"{d}\",kind=\"tip\"}} ");
        s.lines()
            .find_map(|l| l.strip_prefix(&head))
            .map_or(0.0, |v| v.parse().unwrap())
    }

    /// #2526: an acknowledgement is corrected by half the round trip of the report that carried it,
    /// not by whatever the report before it took.
    #[test]
    fn visibility_takes_half_the_carrying_reports_own_round_trip() {
        let m = metrics();
        let t0 = Instant::now();
        let d = "data/vis-own-trip";
        m.committed(d, 3, 1, t0, Kind::Tip);
        m.committed(d, 3, 2, t0, Kind::Tip);
        let at = |g| BTreeMap::from([(d.to_string(), (3, g))]);
        m.reported(98, &at(1), Some(1), None, None, t0);
        // Report 2 acknowledges generation 2, 50 ms after its commit. The report before it took
        // 90 ms, which once pinned this observation at 5 ms; its own trip took 20.
        let ms = Duration::from_millis;
        m.reported(
            98,
            &at(2),
            Some(2),
            Some(90_000),
            Some("1:90000"),
            t0 + ms(50),
        );
        assert_eq!(
            m.visibility_count(d, Kind::Tip),
            0,
            "waits for its own trip"
        );
        m.reported(
            98,
            &at(2),
            Some(3),
            Some(20_000),
            Some("2:20000,x,4:"),
            t0 + ms(60),
        );
        assert_eq!(m.visibility_count(d, Kind::Tip), 1);
        let v = tip_sum(d);
        assert!((v - 0.040).abs() < 1e-9, "50 ms less half of 20: {v}");
        assert_eq!(m.round_trip(98, 2, ms(20)), 0, "never twice");
        m.gone(98);
    }
}
