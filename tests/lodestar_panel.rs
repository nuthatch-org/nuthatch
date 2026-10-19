//! **#822 criteria 1 and 4**, against a copy of the real Lodestar nest.
//!
//! > 1. The selected Lodestar entity matches its old `views/*.sql` result exactly over the same real
//! >    dataset.
//! > 4. Record p50/p99 latency, rows and bytes scanned for the real Lodestar panel before and after.
//!
//! The route is `indexer_rewards` from `views/40-indexers.sql`, picked by scan cost as #822 asks -
//! 733 sealed segments - and because it is one of the few panel views expressible as an entity: the
//! daily rollups need `date_trunc`/`to_timestamp`, which §3.3's admitted subset does not carry.
//!
//! ```text
//! NUTHATCH_LODESTAR_NEST=~/corpus/horizon-nest \
//!   cargo test --release --test lodestar_panel -- --nocapture --ignored
//! ```
//!
//! Read-only against the nest directory; it declares nothing on disk and starts no indexer.
use std::time::Instant;

/// The authored view, verbatim from the nest, and the entity meant to replace it.
const VIEW: &str = "indexer_rewards";
const ENTITY_SQL: &str =
    "SELECT indexer, SUM(tokensRewards) FROM service__indexing_rewards_collected GROUP BY indexer";
/// The panel shape a dashboard actually asks for, applied to whichever relation answers.
const PANEL: &str = "SELECT * FROM {} ORDER BY 2 DESC LIMIT 20";

fn percentile(sorted: &[u128], p: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let i = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[i]
}

fn timed(f: impl Fn() -> usize, runs: usize, label: &str) -> (u128, u128, usize) {
    let mut each = Vec::with_capacity(runs);
    let mut rows = 0;
    for _ in 0..runs {
        let t = Instant::now();
        rows = f();
        each.push(t.elapsed().as_micros());
    }
    each.sort_unstable();
    let (p50, p99) = (percentile(&each, 0.50), percentile(&each, 0.99));
    println!("  {label:<28} p50 {p50:>8} µs   p99 {p99:>8} µs   {rows} rows");
    (p50, p99, rows)
}

#[test]
#[ignore = "a measurement against a corpus outside the repo, run by hand"]
fn the_lodestar_panel_before_and_after() {
    let Ok(dir) = std::env::var("NUTHATCH_LODESTAR_NEST") else {
        eprintln!("set NUTHATCH_LODESTAR_NEST to a copy of the nest");
        return;
    };
    let dir = std::path::PathBuf::from(match dir.strip_prefix("~/") {
        Some(rest) => format!("{}/{rest}", std::env::var("HOME").unwrap_or_default()),
        None => dir,
    });

    let config = nuthatch::config::Config::load(&dir).unwrap();
    let registry = nuthatch::registry::from_nest(&dir, &config).unwrap();
    let schema = registry.schema();
    let guard = nuthatch::analytics::QueryGuard {
        timeout: std::time::Duration::from_secs(120),
        max_rows: 1_000_000,
    };
    let empty = nuthatch::analytics::HotRows::new();

    // --- The entity, seeded from the same sealed history the view reads. ---
    let (plan, columns) = nuthatch::entity_lower::lower_with_columns(ENTITY_SQL).unwrap();
    let mut view = nuthatch::entity_view::EntityView::start(
        "indexer_rewards_live",
        &plan,
        &columns,
        &registry,
        1_000_000,
        true,
    )
    .unwrap();
    let t = Instant::now();
    let mut source_rows = 0usize;
    let mut segments = 0usize;
    view.seed_begin();
    for table in view.chain_tables() {
        let ts = schema.iter().find(|t| t.table == table).unwrap();
        nuthatch::seal::read_table_rows_by_segment(&dir, ts, u64::MAX, &mut |chunk| {
            source_rows += chunk.len();
            segments += 1;
            view.seed_chunk(&chunk, u64::MAX)
        })
        .unwrap();
    }
    view.seed_end().unwrap();
    let seed_ms = t.elapsed().as_millis();
    assert!(view.fault().is_none(), "faulted: {:?}", view.fault());

    let mut maintained = nuthatch::analytics::HotRows::new();
    maintained.insert("indexer_rewards_live".to_string(), view.rows_as_json());

    println!("\n=== #822 criteria 1 and 4, against {}", dir.display());
    println!(
        "source : {source_rows} rows across {segments} sealed segments of \
         service__indexing_rewards_collected"
    );
    println!("seed   : {} groups in {seed_ms} ms", view.relation().len());

    // --- Criterion 1: the same answer, exactly. ---
    let authored = nuthatch::analytics::query_hot_cold(
        &dir,
        &format!("SELECT \"indexer\", rewards FROM {VIEW} ORDER BY \"indexer\""),
        guard,
        &empty,
        u64::MAX,
        &schema,
    )
    .expect("the authored view must run");
    let mut from_view: Vec<(String, String)> = authored
        .rows
        .iter()
        .map(|r| {
            (
                r["indexer"].as_str().unwrap().to_string(),
                r["rewards"].to_string(),
            )
        })
        .collect();
    from_view.sort();

    let mut from_entity: Vec<(String, String)> = view
        .rows_as_json()
        .iter()
        .map(|r| {
            (
                r["indexer"].as_str().unwrap().to_string(),
                format!("\"{}\"", r["sum_tokensRewards"].as_str().unwrap()),
            )
        })
        .collect();
    from_entity.sort();

    println!(
        "parity : view {} rows, entity {} rows",
        from_view.len(),
        from_entity.len()
    );
    assert_eq!(
        from_entity, from_view,
        "criterion 1: the entity must match the authored view exactly over the same dataset"
    );

    // --- Criterion 4: the panel, before and after. ---
    println!("panel  : {}", PANEL.replace("{}", "<relation>"));
    let before_sql = PANEL.replace("{}", VIEW);
    let after_sql = PANEL.replace("{}", "indexer_rewards_live");
    let runs = 25;
    timed(
        || {
            nuthatch::analytics::query_hot_cold(&dir, &before_sql, guard, &empty, u64::MAX, &schema)
                .expect("before")
                .rows
                .len()
        },
        runs,
        "before (authored view)",
    );
    timed(
        || {
            nuthatch::analytics::query_hot_cold(
                &dir,
                &after_sql,
                guard,
                &maintained,
                u64::MAX,
                &schema,
            )
            .expect("after")
            .rows
            .len()
        },
        runs,
        "after (maintained relation)",
    );
}
