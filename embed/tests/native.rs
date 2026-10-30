//! The native half of the S0 gate: the same circuits, the same inputs, on aarch64. `run-node.mjs`
//! prints the wasm32 figures; the digests must match.

use std::time::Instant;

#[test]
fn steps() {
    let (steps, parity) = nuthatch_embed::run_steps().unwrap();
    assert_eq!(steps[0], vec![(2, 1), (4, 1), (6, 1)]);
    assert_eq!(steps[1], vec![(4, -1), (8, 1)]);
    assert_eq!(parity, vec![((0, 18), 1), ((1, 16), 1)], "{parity:?}");
}

#[test]
fn views_agree_across_drivers() {
    use nuthatch_core::drive::Driver;
    let want = r#"[("0xa", 70), ("0xb", 30), ("0xmint", -100)]"#;
    assert_eq!(nuthatch_embed::run_views(Driver::Threaded).unwrap(), want);
    assert_eq!(nuthatch_embed::run_views(Driver::Inline).unwrap(), want);
}

/// `run-node.mjs` holds the wasm32 run to this same string.
const WINDOW: &str = concat!(
    "committed=2 rows_sha256=8b6b04dba6c9e66507c3a23852d42c97a102850215b6e22b2b3c36e19eb3f491 ",
    r#"balances=[("0x1111111111111111111111111111111111111111", 60), "#,
    r#"("0x2222222222222222222222222222222222222222", 40), "#,
    r#"("0x0000000000000000000000000000000000000000", -100)] "#,
    r#"last=Some("12") sealed_through=12 hot=0 segments=["#,
    r#""tok__transfer 10..=10 rows=1 1418460664977e9b6456532bbbb32543ece957f189c8a1d45b65317ba1705ca1", "#,
    r#""tok__transfer 12..=12 rows=1 170d4dc640f41dddf3718712c011bbdff3cd2286eed32f44df4180a4856e4346"]"#,
);

#[test]
fn a_window_through_core() {
    let got = nuthatch_embed::window::run_window().unwrap();
    eprintln!("native window: {got}");
    assert_eq!(got, WINDOW);
}

#[test]
fn stress() {
    let (groups, total) = nuthatch_embed::run_stress(2_000, 100).unwrap();
    eprintln!("native stress: groups={groups} total={total}");
    assert_eq!(groups, 1024);
}

#[test]
fn entity() {
    let t = Instant::now();
    let r = nuthatch_embed::entity::run_entity(1_000, 100, 250, |_| {}).unwrap();
    let ms = t.elapsed().as_millis();
    eprintln!(
        "native entity: windows={} left +{} -{} right +{} -{} groups={} digest={:016x} state_bytes={:?} in {ms} ms",
        r.windows,
        r.left_inserted,
        r.left_retracted,
        r.right_inserted,
        r.right_retracted,
        r.groups,
        r.digest,
        r.state_bytes
    );
    for l in &r.first_rows {
        eprintln!("  {l}");
    }
    assert!(r.oracle_agrees);
}

/// Finds the first window at which the circuit and the oracle part, for debugging the harness.
#[test]
#[ignore]
fn entity_every_window() {
    let r = nuthatch_embed::entity::run_entity(300, 100, 1, |_| {});
    eprintln!("{r:?}");
    assert!(r.is_ok());
}

/// RFC-0075 S2: `run-node.mjs` holds the wasm32 transcript to this digest.
const REORG_CASES: usize = 12;
const REORG_TRANSCRIPT_SHA256: &str =
    "4f3b8bec690c201973ce15e64987659f67dd912547767241f31121111e96592a";

#[test]
fn the_reorg_property_converges_in_every_band() {
    let t0 = Instant::now();
    let transcript = nuthatch_embed::reorg::run_property(REORG_CASES).unwrap();
    eprintln!("{transcript}\nin {:?}", t0.elapsed());
    assert_eq!(
        nuthatch_seal::content_hash(transcript.as_bytes()),
        REORG_TRANSCRIPT_SHA256
    );
}

/// The tab's `/sql` stops at the node's byte cap, not only its row cap: the node's own wide-cell case,
/// 100 rows of ~1 MiB, far under 50,000 rows and past 64 MiB.
#[test]
fn sql_caps_result_bytes_not_just_rows() {
    use nuthatch_embed::{budget::TabBudget, cellar::Cellar, derived::NestFiles, query::TabQuery};
    let db = redb::Database::builder()
        .create_with_backend(nuthatch_core::backend::Bytes::default())
        .unwrap();
    let store = nuthatch_core::redb_store::Store::from_db(db, None).unwrap();
    let cellar = Cellar::default();
    let q = futures::executor::block_on(TabQuery::bind(
        &store,
        &cellar,
        &[],
        &[],
        &NestFiles::new(),
        TabBudget::default(),
    ))
    .unwrap();
    let (rows, _, truncated) = futures::executor::block_on(
        q.collect("SELECT repeat('A', 1000000) AS x FROM range(100)", 50_000),
    )
    .unwrap();
    assert!(truncated, "the byte cap did not fire");
    assert!(rows.len() < 100, "held {} wide rows", rows.len());
}

/// Dates and timestamps as the node writes them, on both read paths; a text column is data.
#[test]
fn sql_writes_dates_as_the_node_does() {
    use nuthatch_embed::{budget::TabBudget, cellar::Cellar, derived::NestFiles, query::TabQuery};
    let db = redb::Database::builder()
        .create_with_backend(nuthatch_core::backend::Bytes::default())
        .unwrap();
    let store = nuthatch_core::redb_store::Store::from_db(db, None).unwrap();
    let cellar = Cellar::default();
    let q = futures::executor::block_on(TabQuery::bind(
        &store,
        &cellar,
        &[],
        &[],
        &NestFiles::new(),
        TabBudget::default(),
    ))
    .unwrap();
    let sql = "SELECT CAST('2026-10-05' AS DATE) AS d, \
               CAST('2026-10-05 12:34:56.25' AS TIMESTAMP) AS ts, 'Date32(1)' AS text";
    let want = serde_json::json!({
        "d": "2026-10-05",
        "ts": "2026-10-05T12:34:56.25Z",
        "text": "Date32(1)",
    });
    let (rows, _, _) = futures::executor::block_on(q.collect(sql, 10)).unwrap();
    assert_eq!(rows, vec![want.clone()]);
    assert_eq!(
        futures::executor::block_on(q.rows(sql)).unwrap(),
        vec![want]
    );
}

/// Burrmill refuses one value past 16 MiB, so the risk is a slice of near-limit rows: ten of 15 MB
/// are 150 MB in one slice. The cap is met from Arrow lengths a row at a time, so no row that would
/// cross it is encoded and what is held stays under it.
#[test]
fn sql_does_not_encode_the_row_that_would_cross_the_cap() {
    use nuthatch_embed::{budget::TabBudget, cellar::Cellar, derived::NestFiles, query::TabQuery};
    let db = redb::Database::builder()
        .create_with_backend(nuthatch_core::backend::Bytes::default())
        .unwrap();
    let store = nuthatch_core::redb_store::Store::from_db(db, None).unwrap();
    let cellar = Cellar::default();
    let q = futures::executor::block_on(TabQuery::bind(
        &store,
        &cellar,
        &[],
        &[],
        &NestFiles::new(),
        TabBudget::default(),
    ))
    .unwrap();
    let (rows, _, truncated) = futures::executor::block_on(
        q.collect("SELECT repeat('A', 15000000) AS x FROM range(10)", 50_000),
    )
    .unwrap();
    assert!(truncated, "the byte cap did not fire");
    let held: usize = rows.iter().map(nuthatch_core::answer::value_bytes).sum();
    assert!(
        held < nuthatch_core::answer::SQL_MAX_RESULT_BYTES,
        "{} rows, {held} bytes held",
        rows.len()
    );
}

/// Nested values are sized before encoding too: ten rows of a 900,000-element list are ~7 MB each as
/// `value_bytes` counts them, past the cap together, and what is held stays under it.
#[test]
fn sql_sizes_nested_values_before_encoding_them() {
    use nuthatch_embed::{budget::TabBudget, cellar::Cellar, derived::NestFiles, query::TabQuery};
    let db = redb::Database::builder()
        .create_with_backend(nuthatch_core::backend::Bytes::default())
        .unwrap();
    let store = nuthatch_core::redb_store::Store::from_db(db, None).unwrap();
    let cellar = Cellar::default();
    let q = futures::executor::block_on(TabQuery::bind(
        &store,
        &cellar,
        &[],
        &[],
        &NestFiles::new(),
        TabBudget::default(),
    ))
    .unwrap();
    let (rows, _, truncated) = futures::executor::block_on(
        q.collect("SELECT array_repeat(1, 900000) AS x FROM range(10)", 50_000),
    )
    .unwrap();
    assert!(truncated, "the byte cap did not fire");
    let held: usize = rows.iter().map(nuthatch_core::answer::value_bytes).sum();
    assert!(
        held < nuthatch_core::answer::SQL_MAX_RESULT_BYTES,
        "{} rows, {held} bytes held",
        rows.len()
    );
}

/// The admission charge is the node's: its constants as `src/runtime.rs` states them.
#[test]
fn entity_charge_is_the_nodes() {
    use nuthatch_embed::budget::{ENTITY_CIRCUIT_RSS_MB, ENTITY_RSS_BYTES_PER_ROW};
    let runtime = include_str!("../../src/runtime.rs");
    let stated = |name: &str| -> u64 {
        let line = runtime
            .lines()
            .find(|l| l.contains(&format!("const {name}: u64 = ")))
            .unwrap_or_else(|| panic!("src/runtime.rs no longer states {name}"));
        let v = line.split(" = ").nth(1).unwrap().trim_end_matches(';');
        v.replace('_', "").parse().unwrap()
    };
    assert_eq!(stated("ENTITY_RSS_BYTES_PER_ROW"), ENTITY_RSS_BYTES_PER_ROW);
    assert_eq!(stated("ENTITY_CIRCUIT_RSS_MB"), ENTITY_CIRCUIT_RSS_MB);
}

/// 150,000 declared rows (~466 MiB) fit a desktop's cursor share and not a phone's.
#[test]
fn entities_are_admitted_against_the_cursor_share() {
    use nuthatch_embed::budget::{OverBudget, TabBudget, PHONE_CEILING_MIB};
    let phone = TabBudget::mib(PHONE_CEILING_MIB);
    assert_eq!(phone.query_bytes() + phone.cursor_bytes(), phone.ceiling);
    assert!(TabBudget::default().admit_entities([150_000]).is_ok());
    assert!(phone.admit_entities([10_000, 10_000]).is_ok());
    let err = phone.admit_entities([150_000]).unwrap_err();
    assert!(matches!(err, OverBudget::Entities { .. }), "{err:?}");
    assert!(
        err.to_string().starts_with("over the tab's memory budget"),
        "{err}"
    );
}

#[test]
fn a_window_is_refused_at_the_wall_below_the_ceiling() {
    use nuthatch_embed::budget::TabBudget;
    let b = TabBudget::mib(600);
    assert_eq!(b.wall(), 600 * 1024 * 1024 - 600 * 1024 * 1024 / 16);
    assert!(b.check_memory(b.wall() - 65_536).is_ok());
    assert!(b.check_memory(b.wall()).is_err());
}

/// A statement whose working memory passes a phone's share is refused by name, as the node's is.
#[test]
fn a_statement_over_the_phone_share_is_refused() {
    use nuthatch_embed::budget::{TabBudget, PHONE_CEILING_MIB};
    use nuthatch_embed::{cellar::Cellar, derived::NestFiles, query::TabQuery};
    let db = redb::Database::builder()
        .create_with_backend(nuthatch_core::backend::Bytes::default())
        .unwrap();
    let store = nuthatch_core::redb_store::Store::from_db(db, None).unwrap();
    let cellar = Cellar::default();
    let phone = TabBudget::mib(PHONE_CEILING_MIB);
    let q = futures::executor::block_on(TabQuery::bind(
        &store,
        &cellar,
        &[],
        &[],
        &NestFiles::new(),
        phone,
    ))
    .unwrap();
    let err = futures::executor::block_on(q.collect(LODESTAR_CLASS, 10)).unwrap_err();
    assert!(err.to_string().contains("Out of Memory Error"), "{err}");
}

/// 4 million distinct 100-byte group keys, a hash table past a phone's 150 MiB share as
/// `lodestar_indexer_ledger`'s 963 MiB is.
const LODESTAR_CLASS: &str = "SELECT k, count(*) AS n FROM (SELECT repeat('k', 100) || \
                              CAST(range AS VARCHAR) AS k FROM range(4000000)) GROUP BY k";
