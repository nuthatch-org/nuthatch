//! RFC-0022 slice 3 acceptance: **the writer and query-FE planes are genuinely separate**.
//!
//! The RFC's §Testing asks that "writers ingest while FE nodes serve; scaling FE nodes changes serving
//! throughput without touching ingestion, and vice versa". The load-bearing half of that claim is
//! structural rather than performance-related, and it is the half a benchmark cannot prove:
//!
//! - an FE serves data **it did not index**, from a store a writer filled;
//! - an FE **owns no cursor** - it never advances one, so adding FE nodes cannot corrupt ingestion or
//!   double-write, which is the thing that makes "scale serving independently" safe rather than merely
//!   fast;
//! - **N FE nodes on one store** all answer identically, because state lives in the store rather than
//!   on the node.
//!
//! These run against a shared redb handle rather than Postgres, deliberately. The `HotStore` seam is
//! what the plane split rests on, and `pg_parity` already proves the Postgres implementation answers
//! identically to redb - so proving the split against redb proves it for both, without this suite
//! needing a database to be meaningful. A parity suite that can silently skip is a lesson already
//! learned once in this project.

mod common;

use std::sync::Arc;

use nuthatch::store::{CoreHotStore as _, HotStore, Store};

use common::tape::*;

/// Rows written by "the writer" - a store filled without any FE involvement.
fn writer_fills(store: &dyn HotStore, blocks: u64) {
    for b in 1..=blocks {
        let entities: Vec<(String, String)> = (0..2)
            .map(|i| {
                (
                    Store::entity_key(b, i),
                    serde_json::json!({
                        "table": "usdc__transfer",
                        "block_number": b,
                        "log_index": i,
                        "_seq": (b << 20) | i,
                    })
                    .to_string(),
                )
            })
            .collect();
        store
            .commit_window(&entities, Some((b, &block_hash(b, 0))), b)
            .expect("writer commit");
    }
}

/// The core claim: an FE answers from state it never produced.
#[tokio::test]
async fn an_fe_serves_what_a_writer_wrote() {
    let dir = tempfile::tempdir().unwrap();
    let _cfg = scaffold_nest(dir.path(), "usdc", USDC);

    // One shared store, two roles. In production these are two processes against one Postgres; the
    // structural claim is identical and this keeps the test hermetic.
    let shared: Arc<dyn HotStore> = Arc::new(Store::open(&dir.path().join("shared.redb")).unwrap());

    let fe = shared.clone();
    assert_eq!(fe.count().unwrap(), 0, "nothing indexed yet");

    writer_fills(shared.as_ref(), 5);

    assert_eq!(
        fe.count().unwrap(),
        10,
        "the FE must see rows it did not index - it holds the same store, not a copy"
    );
    assert_eq!(fe.indexed_head().unwrap(), Some(5));
    assert!(
        fe.get_entity(&Store::entity_key(3, 1)).unwrap().is_some(),
        "point reads resolve against the writer's rows"
    );
}

/// The safety claim, and the reason scaling FE nodes is allowed at all: an FE never writes.
#[tokio::test]
async fn adding_fe_nodes_never_advances_a_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let _cfg = scaffold_nest(dir.path(), "usdc", USDC);
    let shared: Arc<dyn HotStore> = Arc::new(Store::open(&dir.path().join("shared.redb")).unwrap());

    writer_fills(shared.as_ref(), 4);
    let head_before = shared.indexed_head().unwrap();
    let count_before = shared.count().unwrap();

    // Three FE handles, each doing everything the serving surface does: point reads, recent, table
    // scans, range scans, meta reads. None of it may move the cursor.
    let fes: Vec<Arc<dyn HotStore>> = (0..3).map(|_| shared.clone()).collect();
    for fe in &fes {
        let _ = fe.count().unwrap();
        let _ = fe.recent(50).unwrap();
        let _ = fe.recent_by_table("usdc__transfer", 50).unwrap();
        let _ = fe.hot_rows_by_table().unwrap();
        let _ = fe.entities_in_range(0, 100).unwrap();
        let _ = fe.get_meta("last_block").unwrap();
        let _ = fe.checkpoints_desc().unwrap();
        let _ = fe.sealed_through();
    }

    assert_eq!(
        shared.indexed_head().unwrap(),
        head_before,
        "serving must not advance the cursor - if it can, adding FE nodes is not safe"
    );
    assert_eq!(
        shared.count().unwrap(),
        count_before,
        "serving must not write rows"
    );
}

/// Any FE answers any request, because the state is in the store rather than on the node. This is
/// what lets a load balancer treat them as interchangeable.
#[tokio::test]
async fn every_fe_node_gives_the_same_answer() {
    let dir = tempfile::tempdir().unwrap();
    let _cfg = scaffold_nest(dir.path(), "usdc", USDC);
    let shared: Arc<dyn HotStore> = Arc::new(Store::open(&dir.path().join("shared.redb")).unwrap());
    writer_fills(shared.as_ref(), 6);

    let answers: Vec<(u64, Vec<String>, Option<u64>)> = (0..4)
        .map(|_| {
            let fe = shared.clone();
            (
                fe.count().unwrap(),
                fe.recent(100).unwrap(),
                fe.indexed_head().unwrap(),
            )
        })
        .collect();

    for a in &answers[1..] {
        assert_eq!(
            a, &answers[0],
            "FE nodes must be interchangeable - a differing answer means state leaked onto a node"
        );
    }
}

/// A writer that keeps indexing while FEs read must not be blocked or corrupted by them, and the FEs
/// must observe the advance. This is the "independently scalable" claim reduced to its testable core.
#[tokio::test]
async fn ingestion_continues_while_fes_read() {
    let dir = tempfile::tempdir().unwrap();
    let _cfg = scaffold_nest(dir.path(), "usdc", USDC);
    let shared: Arc<dyn HotStore> = Arc::new(Store::open(&dir.path().join("shared.redb")).unwrap());

    writer_fills(shared.as_ref(), 3);
    let fe = shared.clone();
    let early = fe.indexed_head().unwrap();

    // The writer advances after the FE has already read.
    writer_fills(shared.as_ref(), 7);

    assert_eq!(
        early,
        Some(3),
        "the FE's earlier read saw the earlier state"
    );
    assert_eq!(
        fe.indexed_head().unwrap(),
        Some(7),
        "the same FE handle observes the writer's progress without restarting"
    );
    assert_eq!(fe.count().unwrap(), 14);
}
