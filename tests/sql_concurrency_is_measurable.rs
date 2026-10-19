//! #1006 - the permit count is a memory bound nobody chose as one, and it could not be measured.
//!
//! `SQL_MAX_CONCURRENCY` is a `const usize = 2`. Measuring the throughput/RSS curve at 1/2/4/8/16
//! permits - which #1006 asks for, on the box that enforces the per-cursor RAM budget - therefore
//! needed **five separate builds**. That is how a ceiling ends up being set from whichever box was
//! convenient, which this project has done before.
//!
//! `NUTHATCH_SQL_MAX_CONCURRENCY` makes it one binary and five settings. It is deliberately capped:
//! concurrent queries do **not** serialise (RFC-0042 §14 corrected that), but each one that misses
//! the connection cache opens its own DuckDB, and unbounded at 32 clients reached **1,313 MB - 64%
//! of one cursor's entire 2 GB**, shared across every nest on that cursor.

use nuthatch::serve::{sql_max_concurrency, SQL_MAX_CONCURRENCY, SQL_MAX_CONCURRENCY_CEILING};

/// **One lock for every read or write of the variable** (#1035).
///
/// `NUTHATCH_SQL_MAX_CONCURRENCY` is process-global. This file used to guard it with *two* unrelated
/// locks - a `std::sync::Mutex` for the parser tests and a separate `tokio::sync::Mutex` for the
/// async wiring tests - which exclude each other not at all. Cargo runs both concurrently, so a
/// parser test could replace the value while `permits_with` was awaiting `spawn_nest`, and the
/// wiring test would then build its gate from someone else's number.
///
/// That is worse than an ordinary flake: the wiring test is the **only** one that catches the
/// per-nest-gate defect #1024 fixed, so a race here quietly removes the coverage rather than
/// obviously breaking it.
///
/// A `tokio::sync::Mutex` because async callers must hold it across `.await`; the synchronous tests
/// take it with `blocking_lock`, which is correct precisely because they are `#[test]` and not
/// `#[tokio::test]`, so there is no runtime on their thread to block.
fn env_lock() -> &'static tokio::sync::Mutex<()> {
    static L: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    L.get_or_init(|| tokio::sync::Mutex::new(()))
}

const KEY: &str = "NUTHATCH_SQL_MAX_CONCURRENCY";
const MEM_KEY: &str = "NUTHATCH_ANALYTICS_MEMORY_LIMIT";

/// Install a value and restore the previous one **on every path, including a panic**.
///
/// Restoring rather than removing matters: an unconditional `remove_var` on the way out destroys an
/// operator-set value in a developer's shell run, and makes the next test's "unset" premise
/// accidental rather than arranged.
///
/// **`Drop`, not a trailing statement** (review of #1035). The first version restored after `f()`
/// returned, which is not "every path": a panicking test - and a failing assertion *is* a panic -
/// left the override installed for whoever ran next in the same process. The PR text claimed every
/// path while the code claimed only the happy one, which is the exact class this file exists to
/// stop. `Drop` runs during unwinding, so the guarantee is now structural.
struct EnvRestore(Option<String>);

impl Drop for EnvRestore {
    fn drop(&mut self) {
        set_env(self.0.as_deref());
    }
}

/// Take the captured value out of the way before installing the new one.
#[must_use = "the restore happens when this guard drops; dropping it immediately restores at once"]
fn install_env(value: Option<&str>) -> EnvRestore {
    let prev = std::env::var(KEY).ok();
    set_env(value);
    EnvRestore(prev)
}

fn with_env<T>(value: Option<&str>, f: impl FnOnce() -> T) -> T {
    // Declaration order matters: `_restore` drops *before* `_g`, so the value is put back while the
    // lock is still held and no other test can observe the gap.
    let _g = env_lock().blocking_lock();
    let _restore = install_env(value);
    f()
}

fn set_env(value: Option<&str>) {
    match value {
        Some(v) => std::env::set_var(KEY, v),
        None => std::env::remove_var(KEY),
    }
}

struct MemRestore(Option<String>);

impl Drop for MemRestore {
    fn drop(&mut self) {
        match self.0.as_deref() {
            Some(v) => std::env::set_var(MEM_KEY, v),
            None => std::env::remove_var(MEM_KEY),
        }
    }
}

/// A memory_limit that keeps `(permits × limit) + derived reservation` on the 2 GiB ceiling, so a
/// concurrency measurement can still spawn. RFC-0047 C4 refuses the default 512 MB past two permits.
#[must_use]
fn install_fitting_memory(permits: usize) -> MemRestore {
    let prev = std::env::var(MEM_KEY).ok();
    let admitted = permits.clamp(1, SQL_MAX_CONCURRENCY_CEILING);
    let mb = 1024 / admitted;
    std::env::set_var(MEM_KEY, mb.to_string());
    MemRestore(prev)
}

#[test]
fn unset_keeps_the_shipped_default() {
    assert_eq!(
        with_env(None, sql_max_concurrency),
        SQL_MAX_CONCURRENCY,
        "an operator who sets nothing must get exactly the shipped value - this knob exists to make \
         the default measurable, not to change it"
    );
    assert_eq!(
        SQL_MAX_CONCURRENCY, 2,
        "the shipped default is 2 until a measured curve says otherwise (#1006)"
    );
}

#[test]
fn a_requested_value_is_honoured_across_the_measured_range() {
    for n in [1usize, 2, 4, 8, 16] {
        assert_eq!(
            with_env(Some(&n.to_string()), sql_max_concurrency),
            n,
            "the curve #1006 asks for is 1/2/4/8/16 permits; if any of those is not honoured the \
             measurement silently flattens and reports a knee that is an artefact of the harness"
        );
    }
}

#[test]
fn above_the_ceiling_is_clamped_not_obeyed() {
    // The RAM argument, enforced rather than documented. 64 permits could open 64 DuckDBs.
    for n in [17usize, 64, 10_000] {
        assert_eq!(
            with_env(Some(&n.to_string()), sql_max_concurrency),
            SQL_MAX_CONCURRENCY_CEILING,
            "a request above the ceiling must clamp. Each concurrent query can open its own DuckDB \
             and the budget is 2 GB per cursor, shared across every nest on it - an unbounded knob \
             would let an operator spend a budget they cannot see"
        );
    }
}

#[test]
fn nonsense_falls_back_rather_than_disabling_the_gate() {
    // `0` is the dangerous one: a zero-permit semaphore refuses every query forever, and an
    // operator typing 0 meaning "unlimited" would take the SQL surface down rather than open it.
    for bad in ["0", "", "  ", "-1", "eight", "4.5"] {
        let got = with_env(Some(bad), sql_max_concurrency);
        assert_eq!(
            got, SQL_MAX_CONCURRENCY,
            "`{bad}` must fall back to the default. 0 in particular must never reach the semaphore: \
             zero permits refuses every query forever, so `=0` meaning 'unlimited' would silently \
             close the analytical surface"
        );
        assert!(got > 0, "the permit count must never be zero");
    }
}

// ---------------------------------------------------------------------------------------------
// The wiring, not the function.
//
// Everything above passes with `build_nest` reading the bare `const` again - I checked, by making
// that exact edit. A test of `sql_max_concurrency()` proves the parser, and the defect #1006 is
// about lives one line further on: whether the semaphore the handlers actually acquire was built
// from it. Same shape as the mounts tests that proved a handler and never the wiring.
// ---------------------------------------------------------------------------------------------

mod common;

use std::sync::Arc;

use common::tape::*;
use nuthatch::indexer;

/// Build a nest under a given override and report the permits its gate actually got.
///
/// The caller already holds [`env_lock`]; this must not take it again or it would deadlock.
async fn permits_with(value: Option<&str>) -> usize {
    let _restore = install_env(value);
    let _mem = match value {
        Some(v) => {
            let n = v.parse::<usize>().unwrap_or(SQL_MAX_CONCURRENCY);
            Some(install_fitting_memory(n))
        }
        None => None,
    };
    let dir = tempfile::tempdir().unwrap();
    let tape = Arc::new(TapeSource::new());
    tape.insert_block(1, empty_block(1, 0, 1_700_000_000));
    tape.advance_tip_to(1);
    let cfg = scaffold_nest(dir.path(), "gate", USDC);
    let rt = indexer::spawn_nest(
        tape,
        dir.path().to_path_buf(),
        cfg,
        None,
        false,
        1,
        Some(2),
        false,
        None,
    )
    .await
    .expect("spawn_nest");
    let permits = rt.state.sql_gate.available_permits();
    let ingest = rt.ingest;
    ingest.abort();
    let _ = ingest.await;
    permits
}

#[tokio::test(flavor = "multi_thread")]
async fn the_gate_the_handlers_acquire_is_built_from_the_live_value() {
    let _guard = env_lock().lock().await;
    // Serialised deliberately: the env var is process-global.
    assert_eq!(
        permits_with(None).await,
        SQL_MAX_CONCURRENCY,
        "an unset override must produce the shipped gate"
    );
    assert_eq!(
        permits_with(Some("8")).await,
        8,
        "the semaphore `/sql` acquires must be built from the live value, not from the `const`. \
         With the const wired in, every arm of #1006's curve would run at 2 permits and report a \
         flat line that looks like a finding"
    );
    assert_eq!(
        permits_with(Some("64")).await,
        SQL_MAX_CONCURRENCY_CEILING,
        "the ceiling must hold at the gate, not only in the parser"
    );
}

/// #1024 - the gate must be owned by the **cursor**, shared by every nest on it.
///
/// Caught in review, and the same shape as everything else this sprint found: the doc comment on
/// `SQL_MAX_CONCURRENCY` has always claimed to bound "the whole analytical surface", while
/// `build_nest` gave every nest its own semaphore. At a hardcoded 2 that was survivable. Made
/// settable, `NUTHATCH_SQL_MAX_CONCURRENCY=16` across six nests would admit **96** concurrent
/// queries, each able to open its own DuckDB - the exact per-cursor overrun the override's warning
/// claims to prevent.
///
/// **The cursor, not the process.** A first attempt made the gate process-global and was too blunt:
/// unrelated cursors in a multichain runtime would contend for a budget they do not share, and
/// parallel integration tests started returning `503 server busy`. `group_by_chain` gives one
/// `spawn_runtime` per chain, so the gate is built there and handed to each nest on that cursor.
///
/// **This drives `spawn_runtime`, not `Arc::clone`.** The first version of this test asserted that
/// two clones of one `Arc` were the same allocation, which is true of `Arc` and says nothing about
/// the wiring - it passed with `spawn_runtime` mutated back to a gate per nest. Third time in this
/// sprint that a test proved a mechanism instead of a seam, so the real call is exercised here.
#[tokio::test(flavor = "multi_thread")]
async fn nests_on_one_cursor_share_one_gate() {
    let _guard = env_lock().lock().await;
    let _restore = install_env(Some("4"));
    let _mem = install_fitting_memory(4);

    let health = Arc::new(nuthatch::health::RuntimeHealth::default());
    let tape = Arc::new(TapeSource::new());
    tape.insert_block(1, empty_block(1, 0, 1_700_000_000));
    tape.advance_tip_to(1);

    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut nests = Vec::new();
    for (i, d) in dirs.iter().enumerate() {
        let name = format!("cursor-share-{i}");
        let cfg = scaffold_nest(d.path(), &name, USDC);
        health.register(&name, &cfg.nest.chain);
        nests.push((name, d.path().to_path_buf(), cfg));
    }

    let cursor = indexer::spawn_runtime(
        tape,
        nests,
        None,
        false,
        1,
        Some(2),
        false,
        None,
        health,
        false,
    )
    .await
    .expect("spawn_runtime");

    assert_eq!(cursor.states.len(), 3, "three nests on one cursor");
    let gates: Vec<_> = cursor
        .states
        .iter()
        .map(|(_, st)| st.sql_gate.clone())
        .collect();

    for (i, g) in gates.iter().enumerate().skip(1) {
        assert!(
            Arc::ptr_eq(&gates[0], g),
            "nest {i} on this cursor has its own semaphore. Three nests would then admit 3 x 4 = 12 \
             concurrent analytical queries against a budget that is per cursor and shared across \
             the nests on it (#1024)"
        );
    }

    // The consequence, which pointer equality alone does not demonstrate: a permit taken through one
    // nest must be *missing* from its co-tenants.
    //
    // **Not "acquire all 4 and expect starvation".** That was the first version and it failed on CI
    // with `4 permits: NoPermits` while passing locally: these nests are spawned and running, so the
    // runtime legitimately holds permits of its own, and a test that assumes an idle gate is timing
    // the machine rather than the property. Taking *one* and watching the count fall on every other
    // handle is the same assertion without the race.
    let before: Vec<usize> = gates.iter().map(|g| g.available_permits()).collect();
    assert!(
        before.iter().all(|&n| n == before[0]),
        "handles on one cursor disagree about the permit count before anything is taken: {before:?}"
    );
    let held = gates[0].clone().try_acquire_owned().unwrap_or_else(|_| {
        panic!(
            "no permit was free on a 4-permit cursor ({before:?}), so the observable below cannot be \
             read. This run proves nothing rather than passing."
        )
    });
    for (i, g) in gates.iter().enumerate() {
        assert_eq!(
            g.available_permits(),
            before[i] - 1,
            "a permit taken through nest 0 did not reduce what nest {i} can admit. Its gate is its \
             own, so three nests would admit 3 x 4 = 12 concurrent analytical queries against a \
             budget that is per cursor and shared across the nests on it (#1024)"
        );
    }
    drop(held);
    assert_eq!(
        gates[1].available_permits(),
        before[1],
        "releasing through one handle must return the permit to the shared gate"
    );

    cursor.ingest.abort();
    let _ = cursor.ingest.await;
    for (_, w) in cursor.alert_workers {
        w.abort();
        let _ = w.await;
    }
}

/// The guarantee the PR text claims, asserted rather than described (review of #1035).
///
/// A failing assertion *is* a panic, so "restores on every path" and "restores when a test fails"
/// are the same sentence. Before this, restoration happened after `f()` returned, so a failing test
/// handed its override to whoever ran next in the same process.
///
/// **Holds the lock for the whole check, and does not call `with_env`.** The first version of this
/// test read the environment after releasing the lock and *survived the mutation* - other tests
/// moved the value under the assertion, so it was reading the scheduler rather than the guard. It
/// cannot call `with_env` either: that would take the same non-reentrant mutex and deadlock.
#[test]
fn the_previous_value_survives_a_panic_inside_the_guard() {
    let _g = env_lock().blocking_lock();
    let prev = std::env::var(KEY).ok();

    set_env(Some("13"));
    assert_eq!(
        std::env::var(KEY).ok().as_deref(),
        Some("13"),
        "premise: the outer value is installed"
    );

    let caught = std::panic::catch_unwind(|| {
        let _inner = install_env(Some("999"));
        panic!("a failing assertion, which is what this actually guards against");
    })
    .is_err();
    assert!(caught, "premise: the closure panicked");

    assert_eq!(
        std::env::var(KEY).ok().as_deref(),
        Some("13"),
        "the override installed inside the panicking scope outlived it. The next test in this \
         process would then build its gate from 999 and either fail for the wrong reason or pass \
         vacuously (#1035)"
    );

    set_env(prev.as_deref());
}

/// #2143: the admin cockpit reports the permits the gate was built with, not the shipped default.
#[tokio::test(flavor = "multi_thread")]
async fn the_admin_config_reports_the_live_value() {
    use tower::ServiceExt;
    let _guard = env_lock().lock().await;
    let _restore = install_env(Some("8"));
    let _mem = install_fitting_memory(8);
    let dir = tempfile::tempdir().unwrap();
    let tape = Arc::new(TapeSource::new());
    tape.insert_block(1, empty_block(1, 0, 1_700_000_000));
    tape.advance_tip_to(1);
    let cfg = scaffold_nest(dir.path(), "gate", USDC);
    let rt = indexer::spawn_nest(
        tape,
        dir.path().to_path_buf(),
        cfg,
        None,
        false,
        1,
        Some(2),
        true,
        None,
    )
    .await
    .expect("spawn_nest");
    let router = nuthatch::serve::router(nuthatch::serve::SharedNest::new(rt.state.clone()));
    let resp = router
        .oneshot(
            axum::http::Request::get("/_admin/config")
                .header("host", "127.0.0.1")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    rt.ingest.abort();
    assert_eq!(v["running"]["sql"]["max_concurrency"], 8, "{v}");
}
