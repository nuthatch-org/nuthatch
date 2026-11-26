//! The `Nuthatch-Cost` response header (RFC-0073 FR4): what one request read and held, reported to
//! the operator's gateway. It is a report, not a price; nuthatch meters and bills nothing. A caller
//! asks for it per request with `Nuthatch-Cost: 1`; without that nothing below runs.
//!
//! The figures come from where the work happens: Burrmill's count of the sealed bytes a plan selects
//! and reads, the hot rows copied for the statement, the time spent waiting for a permit, and the
//! serialized body. A request is metered through a task-local, carried onto the blocking thread that
//! runs its statement by [`spawn_blocking`].

use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::Duration;

/// The header's name, asked for and answered under the same name. Lower case, as `http` stores it.
pub const HEADER: &str = "nuthatch-cost";

/// Routes whose responses carry the header, by the pattern each is registered under.
const METERED: &[&str] = &[
    "/sql",
    "/q/{name}",
    "/graphql",
    "/subgraphs/id/{id}",
    "/subgraphs/name/{*name}",
    "/table/{name}",
    "/entities",
    "/entity/{id}",
    "/derived",
    "/derived/{entity}",
    "/derived/{entity}/{key}",
    "/balances",
    "/balance/{address}",
    "/exposure/{address}",
    "/flags",
];

#[derive(Default)]
struct Tally {
    sealed_bytes: AtomicU64,
    sealed_bytes_read: AtomicU64,
    scan_cpu_nanos: AtomicU64,
    hot_rows: AtomicU64,
    engine_nanos: AtomicU64,
    peak_memory: AtomicU64,
    queue_nanos: AtomicU64,
    /// Rows the request's answers carried, for the audit log (#3095); not part of the header.
    result_rows: AtomicU64,
    answered: std::sync::atomic::AtomicBool,
    /// The caller sent `Nuthatch-Cost: 1`, so the engine must measure (QueryMetrics, a footer pass
    /// over the segments). A meter the audit log installs (#3095) only counts and leaves this unset,
    /// so turning the log on does not change what a statement costs (#3116).
    asked: std::sync::atomic::AtomicBool,
}

/// One request's tally, shared by every task and thread that works on it.
#[derive(Clone, Default)]
pub struct Meter(Arc<Tally>);

tokio::task_local! {
    static METER: Meter;
}

thread_local! {
    static ON_THREAD: RefCell<Option<Meter>> = const { RefCell::new(None) };
}

fn current() -> Option<Meter> {
    METER
        .try_with(Clone::clone)
        .ok()
        .or_else(|| ON_THREAD.with(|m| m.borrow().clone()))
}

fn with(f: impl FnOnce(&Tally)) {
    if let Some(m) = current() {
        f(&m.0);
    }
}

/// `tokio::task::spawn_blocking`, with the request's meter, and the copy it is served from, on the thread.
pub fn spawn_blocking<R: Send + 'static>(
    f: impl FnOnce() -> R + Send + 'static,
) -> tokio::task::JoinHandle<R> {
    // Restored on drop, so a panicking statement does not leave its meter on a pooled thread.
    struct Restore(Option<Meter>);
    impl Drop for Restore {
        fn drop(&mut self) {
            ON_THREAD.with(|m| *m.borrow_mut() = self.0.take());
        }
    }
    let meter = current();
    crate::seal::spawn_blocking(move || {
        let _restore = Restore(ON_THREAD.with(|m| m.replace(meter)));
        f()
    })
}

/// Run `f` metered by `meter`: what the audit log (#3095) records of a call it audits. A fresh
/// `Meter::default()` is counting-only until the `Nuthatch-Cost` layer asks.
pub async fn within<F: std::future::Future>(meter: Meter, f: F) -> F::Output {
    METER.scope(meter, f).await
}

/// Rows one answer carried.
pub fn result_rows(n: u64) {
    with(|t| {
        t.result_rows.fetch_add(n, Relaxed);
        t.answered.store(true, Relaxed);
    });
}

/// Whether the work on this task or thread is for a request that asked for its cost.
pub fn asked() -> bool {
    current().is_some_and(|m| m.0.asked.load(Relaxed))
}

/// What one Burrmill statement did.
pub fn engine(m: &burrmill::QueryMetrics) {
    with(|t| {
        t.sealed_bytes.fetch_add(m.sealed_bytes_scanned, Relaxed);
        t.sealed_bytes_read.fetch_add(m.sealed_bytes_read, Relaxed);
        t.scan_cpu_nanos.fetch_add(m.scan_cpu_nanos, Relaxed);
        t.engine_nanos.fetch_add(m.exec_nanos, Relaxed);
        t.peak_memory.fetch_max(m.peak_memory_bytes, Relaxed);
    });
}

/// Rows read from the hot tip for the request.
pub fn hot_rows(n: u64) {
    with(|t| {
        t.hot_rows.fetch_add(n, Relaxed);
    });
}

/// Time the request waited for an admission permit.
pub fn queued(d: Duration) {
    with(|t| {
        t.queue_nanos.fetch_add(d.as_nanos() as u64, Relaxed);
    });
}

/// A request's figures, as the audit log records them: the header's, by the header's names.
#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct Figures {
    pub sealed_bytes: u64,
    pub sealed_bytes_read: u64,
    pub hot_rows: u64,
    pub engine_us: u64,
    pub peak_mem_bytes: u64,
    pub queue_us: u64,
    pub scan_cpu_us: u64,
}

impl Meter {
    /// A request that asked for its cost: engine metrics are collected for it.
    fn ask(&self) {
        self.0.asked.store(true, Relaxed);
    }

    /// What the request did so far.
    pub fn figures(&self) -> Figures {
        let t = &self.0;
        Figures {
            sealed_bytes: t.sealed_bytes.load(Relaxed),
            sealed_bytes_read: t.sealed_bytes_read.load(Relaxed),
            hot_rows: t.hot_rows.load(Relaxed),
            engine_us: t.engine_nanos.load(Relaxed) / 1_000,
            peak_mem_bytes: t.peak_memory.load(Relaxed),
            queue_us: t.queue_nanos.load(Relaxed) / 1_000,
            scan_cpu_us: t.scan_cpu_nanos.load(Relaxed) / 1_000,
        }
    }

    /// Rows its answers carried; `None` when no answer counted any.
    pub fn result_rows(&self) -> Option<u64> {
        self.0
            .answered
            .load(Relaxed)
            .then(|| self.0.result_rows.load(Relaxed))
    }

    /// The header value: `key=value` pairs in a fixed order, every value an integer. A field is only
    /// ever added at the end.
    pub fn header(&self, result_bytes: u64) -> String {
        let t = &self.0;
        format!(
            "sealed_bytes={}; hot_rows={}; engine_us={}; peak_mem_bytes={}; queue_us={}; result_bytes={}; sealed_bytes_read={}; scan_cpu_us={}",
            t.sealed_bytes.load(Relaxed),
            t.hot_rows.load(Relaxed),
            t.engine_nanos.load(Relaxed) / 1_000,
            t.peak_memory.load(Relaxed),
            t.queue_nanos.load(Relaxed) / 1_000,
            result_bytes,
            t.sealed_bytes_read.load(Relaxed),
            t.scan_cpu_nanos.load(Relaxed) / 1_000,
        )
    }
}

/// Meter a metered route's request that asked for it, and stamp its response. A route layer, so the
/// matched pattern is known; anything else passes through untouched.
pub async fn layer(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let asked = req
        .headers()
        .get(HEADER)
        .is_some_and(|v| v.as_bytes() == b"1");
    let metered = asked
        && req
            .extensions()
            .get::<axum::extract::MatchedPath>()
            .is_some_and(|p| {
                let p = p.as_str();
                METERED.iter().any(|m| p == *m || p.ends_with(m))
            });
    if !metered {
        return next.run(req).await;
    }
    // Under the audit log's meter already (#3095): report into that one, so both see every figure.
    let (meter, mut resp) = match METER.try_with(Clone::clone) {
        Ok(outer) => {
            outer.ask();
            (outer, next.run(req).await)
        }
        Err(_) => {
            let meter = Meter::default();
            meter.ask();
            (meter.clone(), METER.scope(meter, next.run(req)).await)
        }
    };
    use axum::body::HttpBody;
    let result_bytes = resp.body().size_hint().exact().or_else(|| {
        resp.headers()
            .get(axum::http::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok()?.parse().ok())
    });
    if let Ok(v) = axum::http::HeaderValue::from_str(&meter.header(result_bytes.unwrap_or(0))) {
        resp.headers_mut().insert(HEADER, v);
    }
    resp
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_header_names_every_figure_in_order() {
        let m = Meter::default();
        METER.sync_scope(m.clone(), || {
            engine(&burrmill::QueryMetrics {
                sealed_bytes_scanned: 4096,
                sealed_bytes_read: 3000,
                output_rows: 3,
                exec_nanos: 2_500_000,
                peak_memory_bytes: 1 << 20,
                scan_cpu_nanos: 1_500_000,
            });
            engine(&burrmill::QueryMetrics {
                sealed_bytes_scanned: 1,
                peak_memory_bytes: 1,
                ..Default::default()
            });
            hot_rows(7);
            queued(Duration::from_micros(250));
        });
        assert_eq!(
            m.header(12),
            "sealed_bytes=4097; hot_rows=7; engine_us=2500; peak_mem_bytes=1048576; queue_us=250; result_bytes=12; sealed_bytes_read=3000; scan_cpu_us=1500"
        );
    }

    /// #3116: the audit log's meter counts but does not make the engine collect QueryMetrics.
    #[tokio::test]
    async fn an_audit_meter_does_not_ask_for_engine_metrics() {
        assert!(!asked());
        let m = Meter::default();
        within(m.clone(), async {
            assert!(!asked());
            assert!(!spawn_blocking(asked).await.unwrap());
            hot_rows(2);
            m.ask();
            assert!(asked());
            assert!(spawn_blocking(asked).await.unwrap());
        })
        .await;
        assert_eq!(m.figures().hot_rows, 2);
    }

    #[tokio::test]
    async fn a_blocking_task_records_into_its_request() {
        let m = Meter::default();
        METER
            .scope(m.clone(), async {
                spawn_blocking(|| hot_rows(5)).await.unwrap();
            })
            .await;
        hot_rows(100);
        assert_eq!(m.0.hot_rows.load(Relaxed), 5);
    }
}
