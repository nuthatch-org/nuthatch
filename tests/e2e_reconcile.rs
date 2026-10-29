//! RFC-0022 §Testing, **placement/rebalance**: "adding a worker rebalances cursors; a cursor is
//! *never* owned by two workers concurrently (single-owner invariant, asserted)."
//!
//! This is the first suite that exercises the whole chain (control plane → scheduler → lease → fence)
//! rather than any one part of it. Two workers here are two `ControlPlane` handles and two `PgStore`
//! handles against one database, which is what two processes are from the data's point of view.
//!
//! ```sh
//! NUTHATCH_TEST_PG=postgres://nuthatch:nuthatch@127.0.0.1:5433/nuthatch \
//!   cargo test --features postgres-store --test e2e_reconcile
//! ```

#![cfg(feature = "postgres-store")]

use std::sync::Arc;

use nuthatch::controlplane::ControlPlane;
use nuthatch::pgstore::PgStore;
use nuthatch::reconcile::{tick, CursorHosts};
use nuthatch::scheduler::DesiredNest;
use nuthatch::store::{CoreHotStore as _, HotStore};

const TTL: u64 = 60;

/// A worker's hosts: the chains it can run, each with its own hot store.
struct Hosts(Vec<(String, Arc<dyn HotStore>)>);

impl CursorHosts for Hosts {
    fn stores(&self) -> Vec<(String, Arc<dyn HotStore>)> {
        self.0.clone()
    }
}

fn url() -> Option<String> {
    match std::env::var("NUTHATCH_TEST_PG") {
        Ok(u) => Some(u),
        Err(_) if std::env::var("NUTHATCH_REQUIRE_PG").is_ok() => {
            panic!("NUTHATCH_REQUIRE_PG is set but NUTHATCH_TEST_PG is not")
        }
        Err(_) => {
            eprintln!("SKIPPED: set NUTHATCH_TEST_PG to run the reconcile suite");
            None
        }
    }
}

/// A fresh control plane and a store-factory scoped to this test, so tests can share one database.
fn fixture(test: &str) -> Option<(ControlPlane, String)> {
    let url = url()?;
    let cp = ControlPlane::connect(&url).expect("connect");
    for n in cp.desired().unwrap() {
        cp.undeclare_nest(&n.name).unwrap();
    }
    for w in cp.live_workers(86_400).unwrap() {
        cp.deregister(&w.id).unwrap();
    }
    Some((cp, format!("{url}|{test}")))
}

/// Distinct hot stores per (test, chain), so a rerun never inherits a lease.
fn store(scoped: &str, chain: &str) -> Arc<dyn HotStore> {
    let (url, test) = scoped.split_once('|').unwrap();
    let nest = format!("{test}_{}_{}", chain.replace('-', "_"), std::process::id());
    Arc::new(PgStore::connect(url, &nest).expect("store"))
}

fn nest(name: &str, chain: &str, mb: u64) -> DesiredNest {
    DesiredNest {
        name: name.into(),
        chain: chain.into(),
        estimated_rss_mb: mb,
    }
}

/// One worker, one cursor: reconciliation acquires the lease, and a second tick is idempotent rather
/// than re-acquiring (which would bump the fence and invalidate its own in-flight writes).
#[tokio::test]
async fn a_tick_acquires_and_then_holds() {
    let Some((cp, scoped)) = fixture("hold") else {
        return;
    };
    cp.declare_nest(&nest("usdc", "mainnet", 90)).unwrap();
    let s = store(&scoped, "mainnet");
    let hosts = Hosts(vec![("mainnet".into(), s.clone())]);

    let first = tick(&cp, &hosts, "w1", 2048, TTL).unwrap();
    assert_eq!(first.acquired, vec!["mainnet"]);
    let fence_after_first = s.current_fence().unwrap();

    let second = tick(&cp, &hosts, "w1", 2048, TTL).unwrap();
    assert!(
        second.acquired.is_empty() && second.released.is_empty(),
        "holding is not re-acquiring: {second:?}"
    );
    assert_eq!(
        s.current_fence().unwrap(),
        fence_after_first,
        "a renewal must not bump the fence, or the worker invalidates its own writes each tick"
    );
}

/// **The headline invariant.** Two workers reconcile against one cursor; exactly one ends up owning
/// it, and the loser records contention rather than failing.
#[tokio::test]
async fn two_workers_racing_for_one_cursor_produce_exactly_one_owner() {
    let Some((cp, scoped)) = fixture("race") else {
        return;
    };
    cp.declare_nest(&nest("usdc", "mainnet", 90)).unwrap();

    // Both workers can host mainnet, and both see the same store - two processes, one database.
    let s = store(&scoped, "mainnet");
    let a = Hosts(vec![("mainnet".into(), s.clone())]);
    let b = Hosts(vec![("mainnet".into(), s.clone())]);

    let ra = tick(&cp, &a, "w1", 2048, TTL).unwrap();
    let rb = tick(&cp, &b, "w2", 2048, TTL).unwrap();

    let owners = ra.acquired.len() + rb.acquired.len();
    assert_eq!(
        owners, 1,
        "exactly one worker may take the cursor - w1={ra:?} w2={rb:?}"
    );
    // w2 does not even contend: it reads the live lease, the planner keeps the cursor where it is
    // (placement is stable), so w2 never tries. Contention is for the genuine race below, where the
    // control plane and the lease disagree about who is alive.
    assert!(
        rb.acquired.is_empty() && rb.contended.is_empty(),
        "the second worker should quietly leave a healthy cursor alone: {rb:?}"
    );

    let lease = s.current_lease().unwrap().expect("someone holds it");
    assert_eq!(lease.owner, "w1");
    assert!(lease.expires_in_secs > 0);
}

/// **The case the lease exists to arbitrate**: a worker whose heartbeat has lapsed but whose lease is
/// still live.
///
/// The control plane and the lease are deliberately independent - a control-plane outage must stop
/// *rescheduling*, not *ingestion* - so they can legitimately disagree. Here the planner concludes the
/// cursor needs a new home, and the lease refuses. The refusal is the correct outcome: the old owner
/// may still be happily writing, and a plan is not permission.
#[tokio::test]
async fn a_live_lease_beats_a_plan_that_says_otherwise() {
    let Some((cp, scoped)) = fixture("contend") else {
        return;
    };
    cp.declare_nest(&nest("usdc", "mainnet", 90)).unwrap();
    let s = store(&scoped, "mainnet");

    // A worker that took the lease and has since stopped heartbeating - it is not in `live_workers`,
    // but its lease has not expired.
    s.acquire_lease("ghost", 600).unwrap();
    let ghost_fence = s.current_fence().unwrap();

    let hosts = Hosts(vec![("mainnet".into(), s.clone())]);
    let r = tick(&cp, &hosts, "w1", 2048, TTL).unwrap();

    assert_eq!(
        r.contended,
        vec!["mainnet"],
        "the plan wanted it here, the lease said no, and that is recorded rather than fatal: {r:?}"
    );
    assert!(r.acquired.is_empty());
    assert_eq!(
        s.current_lease().unwrap().unwrap().owner,
        "ghost",
        "the live holder keeps the cursor - a plan is not permission"
    );
    assert_eq!(
        s.current_fence().unwrap(),
        ghost_fence,
        "and its fence is untouched, so its in-flight writes remain valid"
    );
}

/// Adding a worker spreads cursors. The RFC's rebalance clause, reduced to what is actually testable:
/// a second worker picks up a cursor the first could not place.
#[tokio::test]
async fn a_second_worker_picks_up_what_the_first_could_not_hold() {
    let Some((cp, scoped)) = fixture("spread") else {
        return;
    };
    // Two chains, each too big for one worker to hold both.
    cp.declare_nest(&nest("a", "mainnet", 900)).unwrap();
    cp.declare_nest(&nest("b", "arbitrum-one", 900)).unwrap();

    let (sm, sa) = (store(&scoped, "mainnet"), store(&scoped, "arbitrum-one"));
    let w1 = Hosts(vec![
        ("mainnet".into(), sm.clone()),
        ("arbitrum-one".into(), sa.clone()),
    ]);

    // A single 1100MB worker can hold exactly one of the two cursors.
    let solo = tick(&cp, &w1, "w1", 1100, TTL).unwrap();
    assert_eq!(solo.acquired.len(), 1, "only one fits: {solo:?}");
    assert_eq!(
        solo.unplaceable.len(),
        1,
        "and the other is reported unplaceable rather than forgotten"
    );

    // A second worker joins and takes the other.
    let w2 = Hosts(vec![
        ("mainnet".into(), sm.clone()),
        ("arbitrum-one".into(), sa.clone()),
    ]);
    let joined = tick(&cp, &w2, "w2", 1100, TTL).unwrap();
    assert_eq!(joined.acquired.len(), 1, "the newcomer takes the rest");

    let owners: Vec<String> = [&sm, &sa]
        .iter()
        .map(|s| s.current_lease().unwrap().unwrap().owner)
        .collect();
    assert!(
        owners.contains(&"w1".to_string()) && owners.contains(&"w2".to_string()),
        "the two cursors ended up on different workers: {owners:?}"
    );
}

/// Removing a nest drains its cursor. No restart, no coordination - the next tick simply stops
/// wanting it.
#[tokio::test]
async fn undeclaring_a_nest_releases_its_cursor() {
    let Some((cp, scoped)) = fixture("drain") else {
        return;
    };
    cp.declare_nest(&nest("usdc", "mainnet", 90)).unwrap();
    let s = store(&scoped, "mainnet");
    let hosts = Hosts(vec![("mainnet".into(), s.clone())]);

    assert_eq!(
        tick(&cp, &hosts, "w1", 2048, TTL).unwrap().acquired.len(),
        1
    );
    cp.undeclare_nest("usdc").unwrap();

    let after = tick(&cp, &hosts, "w1", 2048, TTL).unwrap();
    assert_eq!(after.released, vec!["mainnet"], "the cursor is drained");
    let lease = s.current_lease().unwrap().expect("history is kept");
    assert!(
        lease.expires_in_secs <= 0,
        "released means expired, so another worker may take it"
    );
}

/// One nest going away must not disturb another's cursor - per-nest blast radius, under distribution.
#[tokio::test]
async fn draining_one_cursor_leaves_the_others_alone() {
    let Some((cp, scoped)) = fixture("isolation") else {
        return;
    };
    cp.declare_nest(&nest("a", "mainnet", 90)).unwrap();
    cp.declare_nest(&nest("b", "arbitrum-one", 90)).unwrap();

    let (sm, sa) = (store(&scoped, "mainnet"), store(&scoped, "arbitrum-one"));
    let hosts = Hosts(vec![
        ("mainnet".into(), sm.clone()),
        ("arbitrum-one".into(), sa.clone()),
    ]);

    assert_eq!(
        tick(&cp, &hosts, "w1", 4096, TTL).unwrap().acquired.len(),
        2
    );
    let arb_fence = sa.current_fence().unwrap();

    cp.undeclare_nest("a").unwrap();
    let after = tick(&cp, &hosts, "w1", 4096, TTL).unwrap();

    assert_eq!(after.released, vec!["mainnet"]);
    assert_eq!(
        sa.current_fence().unwrap(),
        arb_fence,
        "the surviving cursor's ownership is untouched - not even re-fenced"
    );
    assert!(sa.current_lease().unwrap().unwrap().expires_in_secs > 0);
}

/// #1934: a cursor claimed from under its holder - a deliberate handover, or the same worker id
/// restarting - is learned on the next tick rather than failing it. The chain leaves `held`, the
/// refusal is recorded, and the other cursor is renewed exactly as before.
#[tokio::test]
async fn a_cursor_claimed_by_another_holder_leaves_held_on_the_next_tick() {
    let Some((cp, scoped)) = fixture("lost") else {
        return;
    };
    cp.declare_nest(&nest("a", "mainnet", 90)).unwrap();
    cp.declare_nest(&nest("b", "arbitrum-one", 90)).unwrap();

    let (sm, sa) = (store(&scoped, "mainnet"), store(&scoped, "arbitrum-one"));
    let hosts = Hosts(vec![
        ("mainnet".into(), sm.clone()),
        ("arbitrum-one".into(), sa.clone()),
    ]);
    let first = tick(&cp, &hosts, "w1", 4096, TTL).unwrap();
    assert_eq!(first.held, vec!["arbitrum-one", "mainnet"]);
    let arb_fence = sa.current_fence().unwrap();

    // A second handle on the same cursor takes it, which is what a handover does.
    let rival = store(&scoped, "mainnet");
    rival.claim("w2").unwrap();

    let after = tick(&cp, &hosts, "w1", 4096, TTL)
        .expect("losing one cursor is not a tick failure: the others still need reconciling");
    assert_eq!(after.lost, vec!["mainnet"]);
    assert_eq!(after.held, vec!["arbitrum-one"]);
    assert!(
        after.acquired.is_empty() && after.released.is_empty(),
        "{after:?}"
    );
    assert_eq!(
        sa.current_fence().unwrap(),
        arb_fence,
        "the surviving cursor was renewed, not re-fenced"
    );
    assert!(sa.current_lease().unwrap().unwrap().expires_in_secs > 0);
    assert_eq!(
        sm.current_fence().unwrap(),
        rival.current_fence().unwrap(),
        "the loser must not take the cursor back"
    );
}

/// The other way to lose a cursor: the lease expires and another worker acquires it. Nothing is
/// refused on this path - the lease record simply names someone else - so `held` must follow the
/// store rather than the worker's memory of what it once acquired.
#[tokio::test]
async fn a_cursor_re_leased_elsewhere_after_expiry_is_no_longer_held() {
    let Some((cp, scoped)) = fixture("expired") else {
        return;
    };
    cp.declare_nest(&nest("a", "mainnet", 90)).unwrap();
    cp.declare_nest(&nest("b", "arbitrum-one", 90)).unwrap();

    let (sm, sa) = (store(&scoped, "mainnet"), store(&scoped, "arbitrum-one"));
    let hosts = Hosts(vec![
        ("mainnet".into(), sm.clone()),
        ("arbitrum-one".into(), sa.clone()),
    ]);
    // A zero TTL expires the leases on the spot, as a stalled worker's would have by now.
    let first = tick(&cp, &hosts, "w1", 4096, 0).unwrap();
    assert_eq!(first.held, vec!["arbitrum-one", "mainnet"]);

    let rival = store(&scoped, "mainnet");
    rival
        .acquire_lease("w2", TTL)
        .expect("an expired lease is free to take");

    let after = tick(&cp, &hosts, "w1", 4096, TTL).unwrap();
    assert_eq!(after.held, vec!["arbitrum-one"], "{after:?}");
    assert_eq!(
        sm.current_lease().unwrap().unwrap().owner,
        "w2",
        "the previous holder must not take a live lease back"
    );
}

// ---- the worker role, wired (RFC-0022 §2) -----------------------------------------------------

/// **The claim the docs make and the code did not, until now.** `--scale writer=2` is safe because
/// ownership is a lease: two workers offering the same chain result in exactly one owner, and the
/// other simply does not run it.
///
/// `reconcile::tick` had six tests and no caller before `worker::run` existed, so this asserts the
/// property through the same `Hosts` type the binary uses rather than through a bespoke test double.
#[tokio::test]
async fn two_workers_offering_one_chain_yield_one_owner() {
    let Some((cp, scoped)) = fixture("workers") else {
        return;
    };
    let (url, _) = scoped.split_once('|').unwrap();
    // A chain name unique to this run. `Hosts::from_chains` namespaces the hot store by chain -
    // correct in production, where one fleet has one cursor per chain - which means a fixed name
    // would inherit the previous run's lease and this test would assert nothing.
    let chain = format!("mainnet-{}", std::process::id());
    cp.declare_nest(&nest("usdc", &chain, 90)).unwrap();

    // Two workers, each built the way the binary builds them: `Hosts::from_chains` against the shared
    // hot store. Same chain, so the same cursor.
    let a = nuthatch::worker::Hosts::from_chains(url, std::slice::from_ref(&chain)).unwrap();
    let b = nuthatch::worker::Hosts::from_chains(url, std::slice::from_ref(&chain)).unwrap();

    let ra = tick(&cp, &a, "writer-1", 2048, 60).unwrap();
    let rb = tick(&cp, &b, "writer-2", 2048, 60).unwrap();

    assert_eq!(
        ra.acquired.len() + rb.acquired.len(),
        1,
        "exactly one worker may own the cursor - w1={ra:?} w2={rb:?}"
    );
    assert!(
        rb.acquired.is_empty(),
        "the second worker leaves a healthy cursor alone rather than fighting for it: {rb:?}"
    );
}

/// A worker only offers chains it was configured for, so the scheduler cannot hand it a cursor it
/// cannot host. Placement is a suggestion; capability is a fact.
#[tokio::test]
async fn a_worker_ignores_chains_it_does_not_host() {
    let Some((cp, scoped)) = fixture("capability") else {
        return;
    };
    let (url, _) = scoped.split_once('|').unwrap();
    let hosted = format!("hosted-{}", std::process::id());
    let other = format!("other-{}", std::process::id());
    cp.declare_nest(&nest("usdc", &hosted, 90)).unwrap();
    cp.declare_nest(&nest("arb", &other, 90)).unwrap();

    // This worker hosts only one of the two chains.
    let hosts = nuthatch::worker::Hosts::from_chains(url, std::slice::from_ref(&hosted)).unwrap();
    let r = tick(&cp, &hosts, "writer-1", 4096, 60).unwrap();

    assert_eq!(
        r.acquired,
        vec![hosted],
        "it takes what it can host and nothing else: {r:?}"
    );
}
