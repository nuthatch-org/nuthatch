//! RFC-0022 §3: the control plane holds **intent**, and the worker registry holds **liveness**.
//!
//! Runs against a real Postgres for the same reason `pg_parity` does - the interesting behaviour is
//! in the SQL (upsert semantics, the TTL comparison happening on the database's clock), and a mock
//! would be asserting my beliefs about Postgres rather than Postgres.
//!
//! ```sh
//! NUTHATCH_TEST_PG=postgres://nuthatch:nuthatch@127.0.0.1:5433/nuthatch \
//!   cargo test --features postgres-store --test control_plane
//! ```
//!
//! Skips without the variable; CI sets `NUTHATCH_REQUIRE_PG=1` so it can never skip there.

#![cfg(feature = "postgres-store")]

use nuthatch::controlplane::ControlPlane;
use nuthatch::scheduler::{plan, DesiredNest, Worker};

fn cp(test: &str) -> Option<ControlPlane> {
    let url = match std::env::var("NUTHATCH_TEST_PG") {
        Ok(u) => u,
        Err(_) if std::env::var("NUTHATCH_REQUIRE_PG").is_ok() => panic!(
            "{test}: NUTHATCH_REQUIRE_PG is set but NUTHATCH_TEST_PG is not - this suite would have \
             silently skipped"
        ),
        Err(_) => {
            eprintln!("SKIPPED {test}: set NUTHATCH_TEST_PG to run the control-plane suite");
            return None;
        }
    };
    let cp = ControlPlane::connect(&url).expect("connect");
    // One control plane per fleet means one schema, so tests share it - clear what this test owns
    // rather than assuming an empty table.
    for n in cp.desired().expect("desired") {
        cp.undeclare_nest(&n.name).expect("clear");
    }
    for w in cp.live_workers(86_400).expect("workers") {
        cp.deregister(&w.id).expect("clear");
    }
    Some(cp)
}

fn nest(name: &str, chain: &str, mb: u64) -> DesiredNest {
    DesiredNest {
        name: name.into(),
        chain: chain.into(),
        estimated_rss_mb: mb,
    }
}

#[tokio::test]
async fn desired_state_round_trips_and_is_stable() {
    let Some(cp) = cp("desired") else { return };

    cp.declare_nest(&nest("weth", "mainnet", 90)).unwrap();
    cp.declare_nest(&nest("usdc", "mainnet", 120)).unwrap();
    cp.declare_nest(&nest("arb", "arbitrum-one", 90)).unwrap();

    let got = cp.desired().unwrap();
    assert_eq!(got.len(), 3);
    assert_eq!(
        got.iter().map(|n| n.name.as_str()).collect::<Vec<_>>(),
        vec!["arb", "usdc", "weth"],
        "name-ordered, so two schedulers reading it plan identically"
    );
    assert_eq!(got[1].estimated_rss_mb, 120);
}

/// Re-declaring corrects a mistake in place. A delete-then-add would briefly empty the desired set,
/// and a scheduler reconciling in that window would drain a perfectly healthy cursor.
#[tokio::test]
async fn declaring_the_same_nest_twice_updates_rather_than_failing() {
    let Some(cp) = cp("redeclare") else { return };

    cp.declare_nest(&nest("usdc", "mainnet", 90)).unwrap();
    cp.declare_nest(&nest("usdc", "arbitrum-one", 250))
        .expect("re-declaring must be an update, not a conflict");

    let got = cp.desired().unwrap();
    assert_eq!(got.len(), 1, "still one nest, not two");
    assert_eq!(got[0].chain, "arbitrum-one", "the correction took effect");
    assert_eq!(got[0].estimated_rss_mb, 250);
}

/// Removing reports whether it did anything - an API returns 404 rather than 200 for a no-op, and a
/// scheduler logs a real removal differently from a repeated one.
#[tokio::test]
async fn undeclaring_distinguishes_a_removal_from_a_no_op() {
    let Some(cp) = cp("undeclare") else { return };
    cp.declare_nest(&nest("usdc", "mainnet", 90)).unwrap();

    assert!(cp.undeclare_nest("usdc").unwrap(), "it was there");
    assert!(!cp.undeclare_nest("usdc").unwrap(), "and now it is not");
    assert!(cp.desired().unwrap().is_empty());
}

#[tokio::test]
async fn a_nest_needs_a_name_and_a_chain() {
    let Some(cp) = cp("validation") else { return };
    assert!(cp.declare_nest(&nest("", "mainnet", 90)).is_err());
    assert!(cp.declare_nest(&nest("usdc", "", 90)).is_err());
    // #2140: a name becomes a directory on every worker.
    for name in ["../../srv/x", "/srv/x", ".."] {
        assert!(
            cp.declare_nest(&nest(name, "mainnet", 90)).is_err(),
            "{name:?} was declared"
        );
    }
    assert!(cp.heartbeat("", 2048).is_err());
}

/// Liveness is a TTL on the **database's** clock. A worker seen now is live; one seen long ago is
/// not, and the comparison never involves the caller's clock.
#[tokio::test]
async fn liveness_is_a_ttl_measured_by_the_database() {
    let Some(cp) = cp("liveness") else { return };

    cp.heartbeat("w1", 2048).unwrap();
    cp.heartbeat("w2", 4096).unwrap();

    let live = cp.live_workers(60).unwrap();
    assert_eq!(live.len(), 2, "both just heartbeated");
    assert_eq!(live[0].id, "w1");
    assert_eq!(live[1].budget_mb, 4096, "the budget round-trips");

    // A zero-second TTL means "seen strictly in the future", which nothing has been.
    assert!(
        cp.live_workers(0).unwrap().is_empty(),
        "an expired TTL must reap everyone, or a dead fleet looks alive"
    );
}

/// Heartbeat doubles as registration, so a worker that was reaped rejoins by doing exactly what it
/// does every second anyway - no separate "notice you were reaped" path to get wrong.
#[tokio::test]
async fn heartbeating_re_registers_and_updates_the_budget() {
    let Some(cp) = cp("rejoin") else { return };

    cp.heartbeat("w1", 2048).unwrap();
    cp.heartbeat("w1", 8192).unwrap();

    let live = cp.live_workers(60).unwrap();
    assert_eq!(live.len(), 1, "one worker, not two");
    assert_eq!(live[0].budget_mb, 8192, "the new budget took effect");
}

/// Graceful shutdown: a worker leaving on purpose should not have to be waited out.
#[tokio::test]
async fn deregistering_removes_a_worker_immediately() {
    let Some(cp) = cp("deregister") else { return };
    cp.heartbeat("w1", 2048).unwrap();
    assert!(cp.deregister("w1").unwrap());
    assert!(!cp.deregister("w1").unwrap(), "and it reports the no-op");
    assert!(cp.live_workers(60).unwrap().is_empty());
}

/// The whole point: what the control plane stores feeds the scheduler directly, with no translation
/// layer in between to disagree with.
#[tokio::test]
async fn desired_state_and_the_registry_drive_a_plan() {
    let Some(cp) = cp("plan") else { return };

    cp.declare_nest(&nest("usdc", "mainnet", 90)).unwrap();
    cp.declare_nest(&nest("weth", "mainnet", 90)).unwrap();
    cp.declare_nest(&nest("arb", "arbitrum-one", 90)).unwrap();
    cp.heartbeat("w1", 2048).unwrap();
    cp.heartbeat("w2", 2048).unwrap();

    let workers: Vec<Worker> = cp.live_workers(60).unwrap();
    let desired = cp.desired().unwrap();
    let p = plan(&workers, &desired, &[]);

    assert_eq!(p.assign.len(), 2, "two chains, so two cursors");
    assert!(p.unplaceable.is_empty());
    // Both mainnet nests share one cursor, so mainnet appears exactly once.
    assert_eq!(
        p.assign.iter().filter(|a| a.chain == "mainnet").count(),
        1,
        "nests sharing a chain must not be placed separately"
    );
}
