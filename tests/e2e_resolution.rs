//! RFC-0022 §Testing, **resolution**: "any FE node resolves + serves any nest; a breaking-version
//! endpoint and its compatible-latest sibling both resolve correctly across nodes (RFC-0020 parity,
//! distributed)."
//!
//! ## The bug this closes, which no single-box test can see
//!
//! RFC-0019 gives every nest a movable `latest` pointer, and RFC-0020 hot-swaps a compatible update
//! behind the same endpoint. On one box that is coherent: one process, one view of `latest`.
//!
//! Across a fleet it is not. If each FE node reads `latest` for itself, then during any upgrade node
//! A serves v2 while node B still serves v1 — **the same endpoint answering with two schemas
//! depending on which node the request lands on**. Nothing errors. The only symptom is a consumer
//! seeing a column appear and disappear as a load balancer moves it around.
//!
//! So the fleet resolves from the *control plane*, where the version is pinned by a deliberate write,
//! and `latest` stays what it always was: a convenience for humans and for `init`.

#![cfg(feature = "postgres-store")]

use std::sync::Arc;

use nuthatch::controlplane::ControlPlane;
use nuthatch::scheduler::DesiredNest;

fn fixture(test: &str) -> Option<Arc<ControlPlane>> {
    let url = match std::env::var("NUTHATCH_TEST_PG") {
        Ok(u) => u,
        Err(_) if std::env::var("NUTHATCH_REQUIRE_PG").is_ok() => {
            panic!("{test}: NUTHATCH_REQUIRE_PG is set but NUTHATCH_TEST_PG is not")
        }
        Err(_) => {
            eprintln!("SKIPPED {test}: set NUTHATCH_TEST_PG to run the resolution suite");
            return None;
        }
    };
    let cp = Arc::new(ControlPlane::connect(&url).expect("connect"));
    for n in cp.desired().unwrap() {
        cp.undeclare_nest(&n.name).unwrap();
    }
    Some(cp)
}

fn hash(c: char) -> String {
    c.to_string().repeat(64)
}

fn declare(cp: &ControlPlane, name: &str, chain: &str) {
    cp.declare_nest(&DesiredNest {
        name: name.into(),
        chain: chain.into(),
        estimated_rss_mb: 90,
    })
    .unwrap();
}

/// A declared-but-unpinned endpoint is **not servable**. Serving it would mean each node picking a
/// version for itself, which is the inconsistency pinning exists to prevent.
#[tokio::test]
async fn an_unpinned_endpoint_is_not_servable() {
    let Some(cp) = fixture("unpinned") else {
        return;
    };
    declare(&cp, "usdc", "mainnet");

    let r = cp.resolve("usdc").unwrap().expect("declared");
    assert_eq!(r.endpoint, "usdc");
    assert_eq!(r.version, None);
    assert!(
        !r.is_servable(),
        "declared is not the same as ready - an FE must refuse rather than guess"
    );

    assert!(
        cp.resolve("never-declared").unwrap().is_none(),
        "and an unknown endpoint is absent, not merely unservable"
    );
}

/// **The headline test.** Every FE node resolves an endpoint identically, because they all read one
/// pinned answer rather than each consulting a movable pointer.
#[tokio::test]
async fn every_node_resolves_an_endpoint_to_the_same_version() {
    let Some(cp) = fixture("agreement") else {
        return;
    };
    declare(&cp, "usdc", "mainnet");
    assert!(cp.pin_version("usdc", "1.2.0", &hash('a')).unwrap());

    // Four independent FE nodes: four separate connections to the control plane, as four processes
    // would be.
    let url = std::env::var("NUTHATCH_TEST_PG").unwrap();
    let nodes: Vec<ControlPlane> = (0..4)
        .map(|_| ControlPlane::connect(&url).expect("fe node"))
        .collect();

    let answers: Vec<_> = nodes
        .iter()
        .map(|n| n.resolve("usdc").unwrap().expect("resolves"))
        .collect();
    for a in &answers[1..] {
        assert_eq!(
            a, &answers[0],
            "FE nodes disagreeing about a version is the same endpoint serving two schemas"
        );
    }
    assert_eq!(answers[0].version.as_deref(), Some("1.2.0"));
    assert!(answers[0].is_servable());
}

/// A compatible update hot-swaps **behind the same endpoint** (RFC-0020 slice 2b), and the swap is
/// one control-plane write - so it is atomic from every node's point of view rather than racing
/// across them.
#[tokio::test]
async fn a_compatible_update_moves_the_endpoint_for_the_whole_fleet_at_once() {
    let Some(cp) = fixture("compatible") else {
        return;
    };
    declare(&cp, "usdc", "mainnet");
    cp.pin_version("usdc", "1.2.0", &hash('a')).unwrap();

    let url = std::env::var("NUTHATCH_TEST_PG").unwrap();
    let node_b = ControlPlane::connect(&url).unwrap();
    assert_eq!(
        node_b.resolve("usdc").unwrap().unwrap().version.as_deref(),
        Some("1.2.0")
    );

    // The operator advances the endpoint.
    cp.pin_version("usdc", "1.3.0", &hash('b')).unwrap();

    // A *different* node sees the new version without being told, because there is one answer.
    let after = node_b.resolve("usdc").unwrap().unwrap();
    assert_eq!(after.version.as_deref(), Some("1.3.0"));
    assert_eq!(after.bundle_hash.as_deref(), Some(hash('b').as_str()));
    assert_eq!(
        cp.desired().unwrap().len(),
        1,
        "a compatible update does not add an endpoint - that is what makes it compatible"
    );
}

/// A **breaking** update is a second endpoint served alongside the first (RFC-0020 slice 3), and both
/// resolve correctly and independently from any node.
#[tokio::test]
async fn a_breaking_version_is_a_sibling_endpoint_and_both_resolve() {
    let Some(cp) = fixture("breaking") else {
        return;
    };
    declare(&cp, "usdc", "mainnet");
    cp.pin_version("usdc", "1.3.0", &hash('b')).unwrap();

    // The breaking version arrives as its own endpoint rather than displacing the old one - existing
    // consumers keep working, which is the entire point of the breaking path.
    declare(&cp, "usdc-v2", "mainnet");
    cp.pin_version("usdc-v2", "2.0.0", &hash('c')).unwrap();

    let url = std::env::var("NUTHATCH_TEST_PG").unwrap();
    let node = ControlPlane::connect(&url).unwrap();

    let old = node.resolve("usdc").unwrap().unwrap();
    let new = node.resolve("usdc-v2").unwrap().unwrap();
    assert_eq!(
        old.version.as_deref(),
        Some("1.3.0"),
        "the old endpoint is untouched"
    );
    assert_eq!(new.version.as_deref(), Some("2.0.0"));
    assert!(old.is_servable() && new.is_servable());
    assert_ne!(old.bundle_hash, new.bundle_hash);

    // Both are real nests the scheduler must place - a breaking sibling is not a routing alias.
    assert_eq!(cp.desired().unwrap().len(), 2);
}

/// Pinning something that was never declared is a no-op that says so, rather than conjuring an
/// endpoint out of a typo.
#[tokio::test]
async fn pinning_an_undeclared_endpoint_reports_the_miss() {
    let Some(cp) = fixture("pin-miss") else {
        return;
    };
    assert!(!cp.pin_version("ghost", "1.0.0", &hash('d')).unwrap());
    assert!(cp.resolve("ghost").unwrap().is_none());
}

#[tokio::test]
async fn a_pin_needs_both_a_version_and_a_hash() {
    let Some(cp) = fixture("pin-validation") else {
        return;
    };
    declare(&cp, "usdc", "mainnet");
    assert!(cp.pin_version("usdc", "", &hash('a')).is_err());
    assert!(cp.pin_version("usdc", "1.0.0", "").is_err());
    // #2140: a pinned hash becomes a directory on every worker.
    for h in ["0xaaaa", "../../srv/x", "/srv/x"] {
        assert!(
            cp.pin_version("usdc", "1.0.0", h).is_err(),
            "{h:?} was pinned"
        );
    }
    assert!(
        !cp.resolve("usdc").unwrap().unwrap().is_servable(),
        "a rejected pin must leave the endpoint unpinned, not half-pinned"
    );
}
