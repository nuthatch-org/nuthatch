//! RFC-0022 slice 2 acceptance: **a backend swap must be invisible**.
//!
//! The RFC's §Testing states it as "served results under Postgres match the embedded redb path for
//! the same nest + range". This drives both backends through the identical sequence of operations and
//! asserts they agree at every observable point - not just at the end, because a divergence that
//! self-corrects by the final assertion is still a divergence someone's query can see.
//!
//! ## Why this is not a unit test of `PgStore`
//!
//! A unit test asserts Postgres does what I *think* redb does. This asserts it does what redb
//! *actually* does, which is a different and stronger claim - my mental model of redb's ordering and
//! edge cases is exactly the thing most likely to be wrong. Every assertion here compares two live
//! stores rather than a store against a literal.
//!
//! ## Running it
//!
//! Needs a Postgres. `docker compose -f docker-compose.scaled.yml up -d postgres`, then:
//!
//! ```sh
//! NUTHATCH_TEST_PG=postgres://nuthatch:nuthatch@127.0.0.1:5433/nuthatch \
//!   cargo test --features postgres-store --test pg_parity
//! ```
//!
//! Without the variable the suite skips, which on a laptop is a convenience and in CI would be a lie.
//! A parity suite that silently no-ops is worse than none, because the green tick means nothing and
//! everyone stops looking - so CI sets `NUTHATCH_REQUIRE_PG=1`, and a missing URL becomes a hard
//! failure rather than a quiet pass.

#![cfg(feature = "postgres-store")]

use std::sync::Arc;

use nuthatch::pgstore::PgStore;
use nuthatch::store::{HotStore, Store};

/// A unique schema per test run, so a re-run never inherits the last one's rows and two tests can
/// share a database. Derived from the test name plus the process id rather than a random value,
/// which keeps a failed run's data inspectable.
fn nest_name(test: &str) -> String {
    format!("{test}_{}", std::process::id())
}

/// The two backends under test, plus the tempdir that must outlive the redb one.
type Pair = (Arc<dyn HotStore>, Arc<dyn HotStore>, tempfile::TempDir);

/// Both backends, or `None` when no Postgres is configured.
fn pair(test: &str) -> Option<Pair> {
    let url = match std::env::var("NUTHATCH_TEST_PG") {
        Ok(u) => u,
        // A skipped test still prints `ok`, and cargo swallows stdout unless `--nocapture` - so the
        // "skips loudly" claim this file used to make was simply false, and a green tick meant
        // nothing. CI sets `NUTHATCH_REQUIRE_PG=1`, which turns a missing URL into a failure: the
        // suite can be skipped on a laptop, never in the pipeline.
        Err(_) if std::env::var("NUTHATCH_REQUIRE_PG").is_ok() => panic!(
            "{test}: NUTHATCH_REQUIRE_PG is set but NUTHATCH_TEST_PG is not - the RFC-0022 parity \
             suite would have silently skipped, which is the one thing it must never do"
        ),
        Err(_) => {
            eprintln!(
                "SKIPPED {test}: set NUTHATCH_TEST_PG to a Postgres URL to run the parity suite"
            );
            return None;
        }
    };
    let dir = tempfile::tempdir().unwrap();
    let redb: Arc<dyn HotStore> = Arc::new(Store::open(&dir.path().join("t.redb")).unwrap());
    let pg: Arc<dyn HotStore> = Arc::new(PgStore::connect(&url, &nest_name(test)).unwrap());
    Some((redb, pg, dir))
}

/// Assert both stores agree on everything an observer can read. Called after *every* mutation, so a
/// failure names the operation that caused it rather than the end of the test.
fn assert_agree(a: &dyn HotStore, b: &dyn HotStore, after: &str) {
    assert_eq!(
        a.count().unwrap(),
        b.count().unwrap(),
        "count after {after}"
    );
    assert_eq!(
        a.recent(1000).unwrap(),
        b.recent(1000).unwrap(),
        "recent after {after} - ordering divergence, check the key collation"
    );
    assert_eq!(
        a.indexed_head().unwrap(),
        b.indexed_head().unwrap(),
        "indexed_head after {after}"
    );
    assert_eq!(
        a.sealed_through(),
        b.sealed_through(),
        "sealed_through after {after}"
    );
    assert_eq!(
        a.checkpoints_desc().unwrap(),
        b.checkpoints_desc().unwrap(),
        "checkpoints_desc after {after}"
    );
    assert_eq!(
        a.entities_in_range(0, u64::MAX / 2).unwrap(),
        b.entities_in_range(0, u64::MAX / 2).unwrap(),
        "entities_in_range after {after}"
    );
    assert_eq!(a.outbox_len(), b.outbox_len(), "outbox_len after {after}");
    assert_eq!(
        a.outbox_pending(1000).unwrap(),
        b.outbox_pending(1000).unwrap(),
        "outbox_pending after {after} - enqueue order must survive the backend"
    );

    let mut a_tables: Vec<String> = a.hot_rows_by_table().unwrap().keys().cloned().collect();
    let mut b_tables: Vec<String> = b.hot_rows_by_table().unwrap().keys().cloned().collect();
    a_tables.sort();
    b_tables.sort();
    assert_eq!(a_tables, b_tables, "hot table set after {after}");
    for t in &a_tables {
        assert_eq!(
            a.hot_rows_by_table().unwrap().get(t),
            b.hot_rows_by_table().unwrap().get(t),
            "rows of {t} after {after}"
        );
    }
}

fn row(table: &str, block: u64, log_index: u64) -> (String, String) {
    (
        Store::entity_key(block, log_index),
        serde_json::json!({
            "table": table,
            "block_number": block,
            "log_index": log_index,
            "_seq": (block << 20) | log_index,
            "value": format!("v-{block}-{log_index}"),
        })
        .to_string(),
    )
}

/// #2467: a pinned handle reads the store as it stood when it was pinned. The window committed after
/// it, rows and `last_block` together, reaches every read on the latest handle and none on the pinned
/// one, on both backends.
#[tokio::test]
async fn a_pinned_handle_reads_the_window_it_was_taken_at_on_both_backends() {
    let Some((redb, pg, _dir)) = pair("pinned_window") else {
        return;
    };
    let tip_rows = |s: &dyn HotStore| {
        s.hot_rows_charged(1_000, u64::MAX, &mut |_| Ok(()))
            .unwrap()
            .rows
            .values()
            .map(Vec::len)
            .sum::<usize>()
    };
    let scanned = |s: &dyn HotStore| {
        let mut n = 0;
        s.scan_entities_in_range(0, 100, &mut |_, _| {
            n += 1;
            true
        })
        .unwrap();
        n
    };
    for (name, store) in [("redb", &redb), ("postgres", &pg)] {
        store
            .commit_window(&[row("t", 1, 0), row("t", 1, 1)], None, 1)
            .unwrap();
        let pinned = store.pinned().unwrap().expect("both backends pin");
        store
            .commit_window(&[row("t", 2, 0), row("t", 3, 0)], None, 3)
            .unwrap();
        for (handle, s, rows, last) in [("pinned", &pinned, 2, "1"), ("latest", store, 4, "3")] {
            let at = format!("{name}, {handle}");
            assert_eq!(s.count().unwrap(), rows as u64, "count, {at}");
            assert_eq!(s.recent(10).unwrap().len(), rows, "recent, {at}");
            assert_eq!(
                s.entities_in_range(0, 100).unwrap().len(),
                rows,
                "entities_in_range, {at}"
            );
            assert_eq!(scanned(s.as_ref()), rows, "scan_entities_in_range, {at}");
            assert_eq!(tip_rows(s.as_ref()), rows, "hot_rows_charged, {at}");
            assert_eq!(
                s.get_meta("last_block").unwrap().as_deref(),
                Some(last),
                "last_block, {at}"
            );
        }
        // Released, its holder is reused for the next pin, which must not see the old window.
        drop(pinned);
        store.commit_window(&[row("t", 4, 0)], None, 4).unwrap();
        let again = store.pinned().unwrap().expect("both backends pin");
        assert_eq!(again.count().unwrap(), 5, "a later pin, {name}");
        assert_eq!(
            tip_rows(again.as_ref()),
            5,
            "a later pin's hot rows, {name}"
        );
    }
    assert_agree(
        redb.as_ref(),
        pg.as_ref(),
        "a window committed past a pinned handle",
    );
}

/// The main event: a realistic sequence of everything the indexer does to a hot store.
#[tokio::test]
async fn the_two_backends_agree_through_an_indexing_lifecycle() {
    let Some((redb, pg, _dir)) = pair("lifecycle") else {
        return;
    };
    let both: [&dyn HotStore; 2] = [redb.as_ref(), pg.as_ref()];

    assert_agree(redb.as_ref(), pg.as_ref(), "open");

    // Windows of rows across several blocks, with checkpoints - the ordinary tip-following path.
    for block in 1..=8u64 {
        let entities: Vec<(String, String)> = (0..3)
            .map(|i| row(if block % 2 == 0 { "even" } else { "odd" }, block, i))
            .collect();
        let hash = format!("0x{block:064x}");
        for s in both {
            s.commit_window(&entities, Some((block, hash.as_str())), block)
                .unwrap();
        }
        assert_agree(
            redb.as_ref(),
            pg.as_ref(),
            &format!("commit_window {block}"),
        );
    }

    // A single out-of-band put, which is how seal-direct and the bench path write.
    let (k, v) = row("odd", 9, 0);
    for s in both {
        s.put_entity(&k, &v).unwrap();
    }
    assert_agree(redb.as_ref(), pg.as_ref(), "put_entity");

    // Meta round-trip, including the watermark the whole restart path reads.
    for s in both {
        s.set_meta("sealed_through", "4").unwrap();
        s.set_meta("arbitrary", "value").unwrap();
    }
    assert_agree(redb.as_ref(), pg.as_ref(), "set_meta");
    assert_eq!(
        redb.get_meta("arbitrary").unwrap(),
        pg.get_meta("arbitrary").unwrap()
    );
    assert_eq!(
        redb.get_meta("absent").unwrap(),
        pg.get_meta("absent").unwrap(),
        "a missing key must be None on both, not empty-string on one"
    );

    // A prefix read, in key order. `_` in the prefix is a LIKE wildcard, so a near-miss key must not match.
    for s in both {
        s.set_meta("ipfs_gave_up:000000000010:1", "b").unwrap();
        s.set_meta("ipfs_gave_up:000000000004:0", "a").unwrap();
        s.set_meta("ipfsXgave_up:000000000001:0", "near miss")
            .unwrap();
    }
    assert_eq!(
        redb.meta_with_prefix("ipfs_gave_up:", 10).unwrap(),
        pg.meta_with_prefix("ipfs_gave_up:", 10).unwrap()
    );
    assert_eq!(
        pg.meta_with_prefix("ipfs_gave_up:", 1).unwrap(),
        [("ipfs_gave_up:000000000004:0".to_string(), "a".to_string())]
    );
    assert_eq!(redb.count_meta_with_prefix("ipfs_gave_up:").unwrap(), 2);
    assert_eq!(pg.count_meta_with_prefix("ipfs_gave_up:").unwrap(), 2);

    // A reorg. The single most important thing to get identical: it deletes data.
    let (ra, rb) = (redb.rollback_to(5).unwrap(), pg.rollback_to(5).unwrap());
    assert_eq!(ra, rb, "rollback must remove the same number of entities");
    assert_agree(redb.as_ref(), pg.as_ref(), "rollback_to(5)");

    // Pruning past finality, which is how sealed ranges leave the hot store.
    let (pa, pb) = (
        redb.prune_and_set_meta(1, 2, "sealed_through", "2")
            .unwrap(),
        pg.prune_and_set_meta(1, 2, "sealed_through", "2").unwrap(),
    );
    assert_eq!(pa, pb, "prune must remove the same number of entities");
    assert_agree(redb.as_ref(), pg.as_ref(), "prune_and_set_meta(1,2)");
}

/// The outbox is at-least-once delivery; a sequence that diverges silently drops or double-sends.
#[tokio::test]
async fn the_outbox_sequence_is_identical_across_backends() {
    let Some((redb, pg, _dir)) = pair("outbox") else {
        return;
    };
    let both: [&dyn HotStore; 2] = [redb.as_ref(), pg.as_ref()];

    for i in 0..12u64 {
        let payload = format!(r#"{{"n":{i}}}"#);
        let (sa, sb) = (
            redb.outbox_push(&payload).unwrap(),
            pg.outbox_push(&payload).unwrap(),
        );
        assert_eq!(sa, sb, "seq {i} must match - it is the delivery identity");
        assert_eq!(sa, i, "the sequence starts at 0 and is dense");
    }
    assert_agree(redb.as_ref(), pg.as_ref(), "12 pushes");

    // Remove out of order, as a concurrent delivery worker does.
    for seq in [3u64, 0, 7] {
        for s in both {
            s.outbox_remove(seq).unwrap();
        }
    }
    assert_agree(redb.as_ref(), pg.as_ref(), "out-of-order removes");

    // Trim to a bound: the oldest go first, on both.
    let (ta, tb) = (redb.outbox_trim(4).unwrap(), pg.outbox_trim(4).unwrap());
    assert_eq!(ta, tb, "trim must drop the same count");
    assert_agree(redb.as_ref(), pg.as_ref(), "outbox_trim(4)");

    // A push *after* a trim must not reuse a sequence - the counter is monotonic, not a row count.
    let (sa, sb) = (
        redb.outbox_push("{\"after\":true}").unwrap(),
        pg.outbox_push("{\"after\":true}").unwrap(),
    );
    assert_eq!(sa, sb);
    assert_eq!(
        sa, 12,
        "the sequence must not rewind when entries are removed"
    );
}

/// Key ordering is the failure mode a locale-collated Postgres would produce, and it would look like
/// working software right up until someone read a range.
#[tokio::test]
async fn key_ordering_survives_the_backend() {
    let Some((redb, pg, _dir)) = pair("ordering") else {
        return;
    };
    let both: [&dyn HotStore; 2] = [redb.as_ref(), pg.as_ref()];

    // Deliberately inserted out of order, spanning a digit-width boundary (9→10, 99→100) where a
    // non-padded or locale-collated ordering diverges from numeric order.
    let blocks = [100u64, 9, 1, 10, 99, 2, 1000];
    for b in blocks {
        let (k, v) = row("t", b, 0);
        for s in both {
            s.put_entity(&k, &v).unwrap();
        }
    }
    assert_agree(redb.as_ref(), pg.as_ref(), "unordered inserts");

    let keys = redb.sample_entity_keys(100).unwrap();
    assert_eq!(keys, pg.sample_entity_keys(100).unwrap());
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted, "both backends must return keys in byte order");

    // A bounded range must not include the block above it.
    assert_eq!(
        redb.entities_in_range(9, 99).unwrap(),
        pg.entities_in_range(9, 99).unwrap()
    );
    assert_eq!(
        redb.entities_in_range(9, 99).unwrap().len(),
        3,
        "blocks 9, 10 and 99 - not 100, and not 1000"
    );
}

/// The seal cut streams the hot store rather than loading a range (RFC-0028 §4 amendment). Postgres
/// pages that stream a thousand rows at a time, so the fixture crosses a page boundary more than once,
/// and an early stop past the first page must stop there on both backends.
#[tokio::test]
async fn the_range_scan_pages_and_stops_on_both_backends() {
    let Some((redb, pg, _dir)) = pair("range_scan") else {
        return;
    };
    // 2,500 rows over 500 blocks, five a block, so pages end mid-block as well as between blocks.
    let rows: Vec<(String, String)> = (0..500u64)
        .flat_map(|b| (0..5u64).map(move |i| row("t", b * 7 + 3, i)))
        .collect();
    for s in [redb.as_ref(), pg.as_ref()] {
        s.commit_window(&rows, None, 499 * 7 + 3).unwrap();
    }

    let scan = |s: &dyn HotStore, from: u64, to: u64, stop_after: usize| {
        let mut seen: Vec<(u64, String)> = Vec::new();
        s.scan_entities_in_range(from, to, &mut |block, json| {
            seen.push((block, json.to_string()));
            seen.len() < stop_after
        })
        .unwrap();
        seen
    };

    for s in [redb.as_ref(), pg.as_ref()] {
        let all = scan(s, 0, u64::MAX, usize::MAX);
        assert_eq!(
            all.iter().map(|(_, j)| j.clone()).collect::<Vec<_>>(),
            s.entities_in_range(0, u64::MAX).unwrap(),
            "a full scan must visit exactly what entities_in_range returns, in the same order"
        );
        for (block, json) in &all {
            let stated = serde_json::from_str::<serde_json::Value>(json).unwrap()["block_number"]
                .as_u64()
                .unwrap();
            assert_eq!(
                *block, stated,
                "the visitor's block must be the row's block"
            );
        }
    }
    assert_eq!(
        scan(redb.as_ref(), 0, u64::MAX, usize::MAX),
        scan(pg.as_ref(), 0, u64::MAX, usize::MAX)
    );

    let stopped_redb = scan(redb.as_ref(), 0, u64::MAX, 1_500);
    let stopped_pg = scan(pg.as_ref(), 0, u64::MAX, 1_500);
    assert_eq!(
        stopped_pg.len(),
        1_500,
        "Postgres must stop past its first page, not at it"
    );
    assert_eq!(stopped_redb, stopped_pg);

    let bounded = scan(pg.as_ref(), 10, 3_000, usize::MAX);
    assert_eq!(bounded, scan(redb.as_ref(), 10, 3_000, usize::MAX));
    assert!(
        bounded.iter().all(|(b, _)| (10..=3_000).contains(b)),
        "a bounded scan must not include rows outside the range"
    );
}

/// Megabyte rows page by bytes on Postgres, a handful at a time rather than a thousand, and still
/// visit exactly what redb does in the same order, including across an early stop (#1391 review).
#[tokio::test]
async fn a_range_scan_of_megabyte_rows_agrees_on_both_backends() {
    let Some((redb, pg, _dir)) = pair("range_scan_large") else {
        return;
    };
    let pad = "x".repeat(3 * 1024 * 1024);
    let rows: Vec<(String, String)> = (0..24u64)
        .map(|b| {
            (
                Store::entity_key(b, 0),
                serde_json::json!({
                    "table": "doc",
                    "block_number": b,
                    "log_index": 0,
                    "_seq": b << 20,
                    "value": pad,
                })
                .to_string(),
            )
        })
        .collect();
    for s in [redb.as_ref(), pg.as_ref()] {
        s.commit_window(&rows, None, 23).unwrap();
    }
    let blocks = |s: &dyn HotStore, stop_after: usize| {
        let mut seen = Vec::new();
        s.scan_entities_in_range(0, u64::MAX, &mut |block, json| {
            assert!(json.len() > 3 * 1024 * 1024, "a row must arrive whole");
            seen.push(block);
            seen.len() < stop_after
        })
        .unwrap();
        seen
    };
    assert_eq!(blocks(pg.as_ref(), usize::MAX), (0..24).collect::<Vec<_>>());
    assert_eq!(
        blocks(redb.as_ref(), usize::MAX),
        blocks(pg.as_ref(), usize::MAX)
    );
    assert_eq!(blocks(pg.as_ref(), 7), blocks(redb.as_ref(), 7));
}

/// Postgres fetches a page whole before visiting any of it, so a page a thousand megabyte rows long was
/// gigabytes (#1391 review). Rows removed while the scan is under way show what had already been
/// fetched: a byte-bounded page holds only the first few, so the scan ends where the survivors end.
#[tokio::test]
async fn a_postgres_scan_fetches_a_byte_budget_of_megabyte_rows_at_a_time() {
    let Ok(url) = std::env::var("NUTHATCH_TEST_PG") else {
        assert!(
            std::env::var("NUTHATCH_REQUIRE_PG").is_err(),
            "page_bytes: NUTHATCH_REQUIRE_PG is set but NUTHATCH_TEST_PG is not"
        );
        eprintln!("SKIPPED page_bytes: set NUTHATCH_TEST_PG to a Postgres URL");
        return;
    };
    let name = nest_name("page_bytes");
    let pg = PgStore::connect(&url, &name).unwrap();
    let other = PgStore::connect(&url, &name).unwrap();
    let pad = "x".repeat(3 * 1024 * 1024);
    let rows: Vec<(String, String)> = (0..24u64)
        .map(|b| {
            (
                Store::entity_key(b, 0),
                serde_json::json!({
                    "table": "doc",
                    "block_number": b,
                    "log_index": 0,
                    "_seq": b << 20,
                    "value": pad,
                })
                .to_string(),
            )
        })
        .collect();
    pg.commit_window(&rows, None, 23).unwrap();

    let mut seen = Vec::new();
    pg.scan_entities_in_range(0, u64::MAX, &mut |block, _| {
        if seen.is_empty() {
            other.rollback_to(9).unwrap();
        }
        seen.push(block);
        true
    })
    .unwrap();
    assert_eq!(
        seen,
        (0..10).collect::<Vec<_>>(),
        "a scan that had fetched past its byte budget would still visit rows removed after it began"
    );
}

/// The `/sql` RAM guard must refuse on both, or a scaled deployment loses a protection the embedded
/// one has. Postgres would happily stream the whole tip, which makes this *more* important there.
#[tokio::test]
async fn the_hot_scan_guard_refuses_on_both_backends() {
    let Some((redb, pg, _dir)) = pair("guard") else {
        return;
    };
    let both: [&dyn HotStore; 2] = [redb.as_ref(), pg.as_ref()];
    for b in 1..=5u64 {
        let (k, v) = row("t", b, 0);
        for s in both {
            s.put_entity(&k, &v).unwrap();
        }
    }
    assert!(
        redb.hot_rows_by_table_bounded(2).is_err() && pg.hot_rows_by_table_bounded(2).is_err(),
        "5 rows against a cap of 2 must be refused by both"
    );
    assert!(
        redb.hot_rows_by_table_bounded(500).is_ok() && pg.hot_rows_by_table_bounded(500).is_ok(),
        "under the cap, both must answer"
    );
}

/// RFC-0073 FR5: both backends charge a hot copy before they copy it, the same bytes, and a refused
/// charge returns no rows.
#[tokio::test]
async fn the_hot_charge_comes_before_the_copy_on_both_backends() {
    let Some((redb, pg, _dir)) = pair("hot_charge") else {
        return;
    };
    let both: [&dyn HotStore; 2] = [redb.as_ref(), pg.as_ref()];
    for b in 1..=5u64 {
        let (k, v) = row("t", b, 0);
        for s in both {
            s.put_entity(&k, &v).unwrap();
        }
    }
    let mut charged = Vec::new();
    for s in both {
        let mut seen = None;
        let snapshot = s
            .hot_rows_charged(10, u64::MAX, &mut |b| {
                seen = Some(b);
                Ok(())
            })
            .unwrap();
        assert_eq!(seen, Some(snapshot.source_bytes));
        assert_eq!(snapshot.rows["t"].len(), 5);
        charged.push(snapshot.source_bytes);

        let refused = s.hot_rows_charged(10, u64::MAX, &mut |_| anyhow::bail!("no room"));
        assert_eq!(refused.unwrap_err().to_string(), "no room");
        let mut called = false;
        let over = s.hot_rows_charged(2, u64::MAX, &mut |_| {
            called = true;
            Ok(())
        });
        assert!(
            over.is_err() && !called,
            "a copy over the row cap is never charged"
        );
    }
    assert_eq!(charged[0], charged[1], "the same rows cost the same bytes");
    // The connection still serves after a charge was refused mid-transaction.
    assert!(pg.hot_rows_by_table_bounded(500).is_ok());
}

/// RFC-0022 slice 4: the two backends must fence **identically**. A backend that enforces ownership
/// differently is a backend on which the single-owner guarantee means something different, which is
/// the same as it not being a guarantee.
#[tokio::test]
async fn ownership_fencing_behaves_identically_on_both_backends() {
    let Some((redb, pg, _dir)) = pair("fencing") else {
        return;
    };
    let both: [&dyn HotStore; 2] = [redb.as_ref(), pg.as_ref()];

    // Unclaimed: no enforcement anywhere.
    for s in both {
        assert_eq!(s.held_fence(), 0);
        assert_eq!(s.current_fence().unwrap(), 0);
        s.put_entity(&Store::entity_key(1, 0), "{\"table\":\"t\"}")
            .expect("an unclaimed store never fences");
    }
    assert_agree(redb.as_ref(), pg.as_ref(), "unclaimed writes");

    // Claim: the fence advances the same way on both.
    let fences: Vec<u64> = both.iter().map(|s| s.claim("worker-a").unwrap()).collect();
    assert_eq!(fences[0], fences[1], "claim must yield the same fence");
    assert_eq!(fences[0], 1);

    for s in both {
        s.commit_window(&[row("t", 2, 0)], Some((2, "0xaa")), 2)
            .expect("the owner writes normally");
        // Seed the outbox while still the owner, so `outbox_trim` below has something to trim. It
        // early-returns `Ok(0)` on an empty outbox - correctly, a no-op needs no fence - and testing
        // it in that state would assert nothing.
        for i in 0..3 {
            s.outbox_push(&format!(r#"{{"n":{i}}}"#)).unwrap();
        }
    }
    assert_agree(redb.as_ref(), pg.as_ref(), "owner write");

    // A rival claims, exactly as another worker would.
    let held = fences[0];
    for s in both {
        s.set_meta(nuthatch::store::OWNER_FENCE, &(held + 1).to_string())
            .expect("the current owner may still write");
    }

    // Both must now refuse the stale holder, with the same typed error and the same numbers.
    for (name, s) in [("redb", redb.as_ref()), ("postgres", pg.as_ref())] {
        let err = s
            .commit_window(&[row("t", 3, 0)], None, 3)
            .expect_err("{name}: a fenced-out holder must not write");
        let lost = err
            .downcast_ref::<nuthatch::store::LostOwnership>()
            .unwrap_or_else(|| panic!("{name} refused for the wrong reason: {err}"));
        assert_eq!((lost.held, lost.current), (held, held + 1), "{name}");
    }

    // Every mutating path, on both, so neither backend has a hole the other lacks.
    for (name, s) in [("redb", redb.as_ref()), ("postgres", pg.as_ref())] {
        for (what, r) in [
            ("put_entity", s.put_entity("k", "{}").map(|_| ())),
            ("set_meta", s.set_meta("x", "y").map(|_| ())),
            ("set_block_hash", s.set_block_hash(1, "0xbb").map(|_| ())),
            (
                "set_block_timestamp",
                s.set_block_timestamp(1, 1_700_000_000).map(|_| ()),
            ),
            ("rollback_to", s.rollback_to(0).map(|_| ())),
            ("prune_range", s.prune_range(0, 10).map(|_| ())),
            ("outbox_push", s.outbox_push("{}").map(|_| ())),
            ("outbox_remove", s.outbox_remove(0).map(|_| ())),
            ("outbox_trim", s.outbox_trim(0).map(|_| ())),
        ] {
            let err = match r {
                Err(e) => e,
                Ok(()) => panic!("{name}/{what} accepted a write from a fenced-out holder"),
            };
            assert!(
                err.downcast_ref::<nuthatch::store::LostOwnership>()
                    .is_some(),
                "{name}/{what} failed for the wrong reason: {err}"
            );
        }
    }

    // Reads survive on both: a fenced-out node may still be serving.
    for s in both {
        assert!(s.count().is_ok());
        assert!(s.recent(10).is_ok());
    }
    assert_agree(redb.as_ref(), pg.as_ref(), "fenced-out state");
}

/// RFC-0022 slice 4b: leases must behave identically on both backends. The Postgres one measures
/// expiry on the **database's** clock and redb on the process clock - different mechanisms that must
/// produce the same answers, which is exactly the kind of thing that quietly diverges.
#[tokio::test]
async fn leases_behave_identically_on_both_backends() {
    let Some((redb, pg, _dir)) = pair("lease") else {
        return;
    };
    let both: [&dyn HotStore; 2] = [redb.as_ref(), pg.as_ref()];

    for s in both {
        assert!(s.current_lease().unwrap().is_none(), "nothing leased yet");
    }

    // Acquire: same owner, same fence, on both.
    let leases: Vec<_> = both
        .iter()
        .map(|s| s.acquire_lease("worker-a", 60).unwrap())
        .collect();
    assert_eq!(leases[0].owner, leases[1].owner);
    assert_eq!(leases[0].fence, leases[1].fence);
    assert_eq!(leases[0].fence, 1);

    // A rival is refused on both, with the same typed error.
    for (name, s) in [("redb", redb.as_ref()), ("postgres", pg.as_ref())] {
        let err = match s.acquire_lease("worker-b", 60) {
            Err(e) => e,
            Ok(_) => panic!("{name}: a live lease must not be acquirable by another worker"),
        };
        let held = err
            .downcast_ref::<nuthatch::store::LeaseHeld>()
            .unwrap_or_else(|| panic!("{name} refused for the wrong reason: {err}"));
        assert_eq!(held.by, "worker-a", "{name}");
        assert!(
            held.expires_in_secs > 0,
            "{name}: must say how long to wait"
        );
    }

    // Renewal extends without re-fencing, on both.
    for (name, s) in [("redb", redb.as_ref()), ("postgres", pg.as_ref())] {
        let r = s.renew_lease(120).unwrap();
        assert_eq!(r.fence, 1, "{name}: renewal must not re-fence");
        assert_eq!(r.owner, "worker-a", "{name}");
        assert!(
            s.current_lease().unwrap().unwrap().expires_in_secs > 60,
            "{name}: the extension took effect"
        );
    }

    // Release frees it without rewinding the fence, on both.
    for (name, s) in [("redb", redb.as_ref()), ("postgres", pg.as_ref())] {
        s.release_lease().unwrap();
        let next = s
            .acquire_lease("worker-b", 60)
            .unwrap_or_else(|e| panic!("{name}: a released lease must be takeable - {e}"));
        assert!(
            next.fence > 1,
            "{name}: the fence stays monotonic across a release"
        );
    }

    assert_agree(redb.as_ref(), pg.as_ref(), "lease lifecycle");
}
