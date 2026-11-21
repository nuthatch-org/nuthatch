//! The sampled audit of sealed history against a second endpoint (RFC-0049 §10 item 5, #1786).
//!
//! Nothing on the ingest path can see an `eth_getLogs` answer with a hole in the middle (#1670). This
//! asks a different endpoint for a sample of sealed ranges, decodes its answer with the nest's own
//! registry, and compares the rows with the sealed segments' rows for the same blocks. A difference
//! is reported, never repaired: sealed segments are immutable, and which endpoint was wrong is for a
//! person to decide.

use crate::registry::{DecodeRegistry, DecodedRow, TableKind, TableSchema};
use crate::source::{LogFilter, Source};
use anyhow::{bail, Context, Result};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Blocks in one sampled range.
pub const DEFAULT_SPAN: u64 = 1_000;
/// Ranges the background audit samples a day.
pub const DEFAULT_PER_DAY: u64 = 24;
/// Rows held on each side for one sample. A denser range is audited up to where it ran out.
pub const SAMPLE_LOG_BUDGET: usize = 20_000;
/// Rows of each kind a mismatch report names; the counts are always complete.
const NAMED_ROWS: usize = 10;

/// `(block_number, log_index)`: one log, whichever table it decoded into.
type RowKey = (u64, u64);

/// What the audit compares: the nest's contract event tables, decoded by the registry the indexer
/// uses. Factory children are left out, since which children exist at a block depends on all the
/// history before it rather than on the sampled range.
pub struct Auditor {
    registry: DecodeRegistry,
    schema: Vec<TableSchema>,
    filter: LogFilter,
    start_block: Option<u64>,
}

impl Auditor {
    pub fn new(dir: &Path, config: &crate::config::Config) -> Result<Self> {
        let registry = crate::registry::from_nest(dir, config)?;
        let aliases: BTreeSet<&str> = config.contracts.iter().map(|c| c.alias.as_str()).collect();
        let schema: Vec<TableSchema> = registry
            .schema()
            .into_iter()
            .filter(|t| t.kind == TableKind::Event && aliases.contains(t.alias.as_str()))
            .collect();
        let addresses: Vec<String> = registry
            .addresses()
            .iter()
            .map(|a| format!("0x{}", hex::encode(a)))
            .collect();
        let topic0s: Vec<String> = schema
            .iter()
            .map(|t| t.topic0.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let filter = LogFilter::new(&addresses, &topic0s)
            .context("this nest declares no contract events, so there is nothing to audit")?;
        Ok(Self {
            registry,
            schema,
            filter,
            start_block: config.contracts.iter().filter_map(|c| c.start_block).min(),
        })
    }
}

/// One audited range and how the two sides differ.
#[derive(Debug)]
pub struct RangeReport {
    pub from: u64,
    /// The last block compared. Below `requested_to` when either side held more than
    /// [`SAMPLE_LOG_BUDGET`] rows.
    pub to: u64,
    pub requested_to: u64,
    pub endpoint_rows: usize,
    pub sealed_rows: usize,
    /// Rows the endpoint served that no sealed segment holds: what an omitting first endpoint leaves.
    pub endpoint_only: Vec<String>,
    /// Sealed rows the endpoint did not serve.
    pub sealed_only: Vec<String>,
    /// `(sealed, endpoint)` for one log that decoded differently.
    pub differing: Vec<(String, String)>,
}

impl RangeReport {
    pub fn mismatches(&self) -> usize {
        self.endpoint_only.len() + self.sealed_only.len() + self.differing.len()
    }

    /// The lines an operator reads: one summary, then up to [`NAMED_ROWS`] rows of each kind.
    pub fn describe(&self, endpoint: &str) -> Vec<String> {
        let truncated = if self.to < self.requested_to {
            let whole = if self.sealed_rows.max(self.endpoint_rows) > SAMPLE_LOG_BUDGET {
                format!(
                    "; block {} alone holds more and was compared whole",
                    self.to
                )
            } else {
                String::new()
            };
            format!(
                " (stopped at {} of {}: the range held more than {SAMPLE_LOG_BUDGET} rows on one \
                 side{whole})",
                self.to, self.requested_to
            )
        } else {
            String::new()
        };
        if self.mismatches() == 0 {
            return vec![format!(
                "sealed audit clean: blocks {}..={} against {endpoint}, {} row(s) on both sides{truncated}",
                self.from, self.to, self.sealed_rows
            )];
        }
        let mut lines = vec![format!(
            "sealed audit MISMATCH: blocks {}..={} against {endpoint}: {} row(s) the endpoint served \
             that the sealed segments lack, {} sealed row(s) it did not serve, {} decoded \
             differently ({} sealed, {} served){truncated}. Sealed data is unchanged; ask a third \
             endpoint before deciding which side is wrong",
            self.from,
            self.to,
            self.endpoint_only.len(),
            self.sealed_only.len(),
            self.differing.len(),
            self.sealed_rows,
            self.endpoint_rows,
        )];
        for row in self.endpoint_only.iter().take(NAMED_ROWS) {
            lines.push(format!("  endpoint only: {row}"));
        }
        for row in self.sealed_only.iter().take(NAMED_ROWS) {
            lines.push(format!("  sealed only:   {row}"));
        }
        for (sealed, served) in self.differing.iter().take(NAMED_ROWS) {
            lines.push(format!("  sealed:        {sealed}"));
            lines.push(format!("  served:        {served}"));
        }
        lines
    }
}

/// A row as both sides compare it. `block_timestamp` comes from a header rather than the log, and
/// the audit fetches no headers, so it is zeroed on both sides.
fn canonical(row: &DecodedRow) -> String {
    let mut row = row.clone();
    row.block_timestamp = 0;
    row.to_json().to_string()
}

fn digest(row: &str) -> [u8; 32] {
    Sha256::digest(row.as_bytes()).into()
}

/// Re-fetch `from..=to` from `source` and compare it with the sealed rows for those blocks.
pub async fn audit_range(
    dir: &Path,
    auditor: &Auditor,
    source: &dyn Source,
    from: u64,
    to: u64,
) -> Result<RangeReport> {
    let (logs, _, covered) = crate::indexer::fetch_logs_splitting_tracked(
        source,
        &auditor.filter,
        from,
        to,
        SAMPLE_LOG_BUDGET,
    )
    .await?;
    let requested_to = to;
    let to = covered.min(to);

    let mut served: BTreeMap<RowKey, String> = BTreeMap::new();
    for log in &logs {
        // `decode` matches contract decoders only, never a factory child's template.
        if let Ok(Some(row)) = auditor.registry.decode(log) {
            served.insert((row.block_number, row.log_index), canonical(&row));
        }
    }
    drop(logs);

    let read = {
        let dir = dir.to_path_buf();
        let schema = auditor.schema.clone();
        // A sweep in another process may fold a provisional segment away between reading the
        // catalogue and opening the file; the second read sees the catalogue that replaced it.
        move || {
            sealed_rows(&dir, &schema, from, to, SAMPLE_LOG_BUDGET)
                .or_else(|_| sealed_rows(&dir, &schema, from, to, SAMPLE_LOG_BUDGET))
        }
    };
    let (sealed, duplicates, to) = tokio::task::spawn_blocking(read)
        .await
        .context("the sealed read panicked")??;
    // A sealed side cut short is compared only as far as it reached.
    served.split_off(&(to + 1, 0));

    let mut report = RangeReport {
        from,
        to,
        requested_to,
        endpoint_rows: served.len(),
        sealed_rows: sealed.len() + duplicates.len(),
        endpoint_only: Vec::new(),
        sealed_only: duplicates,
        differing: Vec::new(),
    };
    for (key, row) in &sealed {
        match served.get(key) {
            None => report.sealed_only.push(row.clone()),
            Some(other) if digest(other) != digest(row) => {
                report.differing.push((row.clone(), other.clone()))
            }
            Some(_) => {}
        }
    }
    for (key, row) in served {
        if !sealed.contains_key(&key) {
            report.endpoint_only.push(row);
        }
    }
    Ok(report)
}

/// The audited tables' sealed rows in `from..=to`, and the last block they cover. Held to `budget`
/// rows in whole blocks the way the endpoint's answer is: past it, the range ends at the last
/// block that fits, or at the first block when that block alone is over, which is kept whole. A
/// second sealed row for one log is returned apart, since no endpoint can serve it twice.
fn sealed_rows(
    dir: &Path,
    schema: &[TableSchema],
    from: u64,
    to: u64,
    budget: usize,
) -> Result<(BTreeMap<RowKey, String>, Vec<String>, u64)> {
    let _lease = crate::seal::read_lease(dir);
    let manifest = crate::seal::load_manifest(dir)?;
    let mut rows: BTreeMap<RowKey, String> = BTreeMap::new();
    let mut duplicates: Vec<(u64, String)> = Vec::new();
    let end = std::cell::Cell::new(to);
    let want = |block: u64| block >= from && block <= end.get();
    for table in schema {
        let Some(segments) = manifest.tables.get(&table.table) else {
            continue;
        };
        for segment in segments
            .iter()
            .filter(|s| s.from_block <= to && s.to_block >= from)
        {
            crate::seal::visit_segment_rows(dir, segment, table, Some(&want), &mut |row| {
                let key = (row.block_number, row.log_index);
                if let Some(earlier) = rows.insert(key, canonical(&row)) {
                    duplicates.push((key.0, earlier));
                }
                let first = rows.keys().next().map_or(key.0, |k| k.0);
                let newest = rows.keys().next_back().map_or(key.0, |k| k.0);
                // One block on its own is never cut, so there is nothing to look for until a second.
                if rows.len() + duplicates.len() > budget && first < newest {
                    let over = rows.keys().nth(budget).map_or(key.0, |k| k.0);
                    let last = if first < over { over - 1 } else { over };
                    if last < end.get() {
                        rows.split_off(&(last + 1, 0));
                        duplicates.retain(|(block, _)| *block <= last);
                        end.set(last);
                    }
                }
                Ok(())
            })?;
        }
    }
    let duplicates = duplicates.into_iter().map(|(_, row)| row).collect();
    Ok((rows, duplicates, end.get()))
}

/// The sealed block range, and the audited tables' segments with their row counts.
struct Catalogue {
    lo: u64,
    hi: u64,
    weighted: Vec<(u64, u64, u64)>,
    rows: u64,
}

impl Catalogue {
    /// From the earliest block an audited table covers to the nest's `sealed_through`: the indexer
    /// cuts every table at that one boundary, so a stretch past the last event segment was sealed
    /// empty, which is where an omitted log hides. Without the stored watermark, the furthest
    /// segment of any table the indexer cuts stands in for it, which can only fall short of it.
    fn load(dir: &Path, auditor: &Auditor, watermark: Option<u64>) -> Result<Option<Self>> {
        let manifest = crate::seal::load_manifest(dir)?;
        let mut weighted: Vec<(u64, u64, u64)> = auditor
            .schema
            .iter()
            .filter_map(|t| manifest.tables.get(&t.table))
            .flatten()
            .map(|s| (s.from_block, s.to_block, s.rows as u64))
            .collect();
        weighted.sort_unstable();
        let cut = manifest
            .tables
            .iter()
            .filter(|(table, _)| !OWN_BOUNDARY.contains(&table.as_str()))
            .flat_map(|(_, segments)| segments);
        let lo = weighted
            .iter()
            .map(|w| w.0)
            .min()
            .or(auditor.start_block)
            .or_else(|| cut.clone().map(|s| s.from_block).min());
        let hi = watermark
            .filter(|&w| w > 0)
            .or_else(|| cut.map(|s| s.to_block).max());
        let (Some(lo), Some(hi)) = (lo, hi) else {
            return Ok(None);
        };
        if hi < lo {
            return Ok(None);
        }
        let rows = weighted.iter().map(|w| w.2).sum();
        Ok(Some(Self {
            lo,
            hi,
            weighted,
            rows,
        }))
    }

    /// Sample `index` under `seed`. Even samples land in a segment chosen in proportion to its rows,
    /// where an omission has something to omit; odd samples are uniform over the sealed blocks, so a
    /// range the first endpoint answered as empty is still asked about.
    fn pick(&self, seed: u64, index: u64, span: u64) -> (u64, u64) {
        let mut state = seed.wrapping_add(index.wrapping_mul(GOLDEN));
        let (a, b) = (splitmix64(&mut state), splitmix64(&mut state));
        let centre = if index.is_multiple_of(2) && self.rows > 0 {
            let mut at = a % self.rows;
            let &(from, to, _) = self
                .weighted
                .iter()
                .find(|w| {
                    if at < w.2 {
                        true
                    } else {
                        at -= w.2;
                        false
                    }
                })
                .expect("at is below the sum of the weights");
            from + b % (to - from + 1)
        } else {
            self.lo + a % (self.hi - self.lo + 1)
        };
        let span = span.clamp(1, self.hi - self.lo + 1);
        let from = centre
            .saturating_sub(span / 2)
            .clamp(self.lo, self.hi + 1 - span);
        (from, from + span - 1)
    }
}

/// Tables sealed outside the indexer's cut: the removed screening command sealed hits over a named range.
const OWN_BOUNDARY: &[&str] = &["sanction_hit"];

const GOLDEN: u64 = 0x9E37_79B9_7F4A_7C15;

/// SplitMix64, written out so a seed picks the same ranges on every build.
fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(GOLDEN);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// The ranges samples `indices` audit under `seed`, or `None` when nothing has sealed yet.
/// `watermark` is the nest's `sealed_through` when it can be read; see [`Catalogue::load`].
pub fn sample_ranges(
    dir: &Path,
    auditor: &Auditor,
    watermark: Option<u64>,
    seed: u64,
    indices: std::ops::Range<u64>,
    span: u64,
) -> Result<Option<Vec<(u64, u64)>>> {
    Ok(Catalogue::load(dir, auditor, watermark)?
        .map(|c| indices.map(|i| c.pick(seed, i, span)).collect()))
}

/// Refuse an audit endpoint the nest indexes from: an omission it made once it would make again.
pub fn refuse_indexing_endpoint(audit: &str, indexing: &[String]) -> Result<()> {
    let norm = |u: &str| u.trim().trim_end_matches('/').to_string();
    if indexing.iter().any(|u| norm(u) == norm(audit)) {
        bail!(
            "the audit endpoint {} is one this nest indexes from; an audit against it can only agree \
             with itself. Point it at a different provider",
            crate::rpc::redact_url(audit)
        );
    }
    let host = crate::rpc::redact_url(audit);
    if indexing.iter().any(|u| crate::rpc::redact_url(u) == host) {
        tracing::warn!(
            "the audit endpoint shares a host with an indexing endpoint ({host}); one provider may \
             serve both from the same backend, and an omission it makes twice is invisible"
        );
    }
    Ok(())
}

fn record(report: &RangeReport) {
    crate::metrics::METRICS.inc_audit_ranges();
    crate::metrics::METRICS.add_audit_mismatches(report.mismatches() as u64);
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// `nuthatch audit sealed`.
pub async fn run_cli(args: crate::cli::AuditSealedArgs) -> Result<()> {
    let dir = PathBuf::from(&args.dir);
    let config = crate::config::Config::load(&dir)?;
    refuse_indexing_endpoint(&args.rpc, &config.nest.rpc_urls)?;
    let auditor = Auditor::new(&dir, &config)?;
    let rpc = crate::rpc::RpcClient::new(vec![args.rpc.clone()])?;
    rpc.verify_chain_ids(config.nest.chain_id).await?;
    let endpoint = crate::rpc::redact_url(&args.rpc);
    // A running nest holds the store's lock; the catalogue then stands in for its watermark.
    let watermark = match crate::store::Store::open_existing(&dir.join(crate::config::DB_FILE)) {
        Ok(store) => Some(store.sealed_through()),
        Err(_) => {
            println!(
                "the hot store is in use or absent; sampling up to the furthest sealed segment"
            );
            None
        }
    };

    let ranges = match (args.from, args.to) {
        (Some(from), Some(to)) => {
            let Some(c) = Catalogue::load(&dir, &auditor, watermark)? else {
                bail!("nothing in {} has sealed yet", dir.display());
            };
            if from > to || from < c.lo || to > c.hi {
                bail!(
                    "blocks {from}..={to} are not inside the sealed range {}..={}",
                    c.lo,
                    c.hi
                );
            }
            vec![(from, to)]
        }
        _ => {
            let seed = args.seed.unwrap_or_else(unix_now);
            println!("seed {seed}");
            sample_ranges(&dir, &auditor, watermark, seed, 0..args.samples, args.span)?
                .with_context(|| format!("nothing in {} has sealed yet", dir.display()))?
        }
    };

    let mut mismatches = 0;
    for (from, to) in ranges {
        let report = audit_range(&dir, &auditor, &rpc, from, to).await?;
        record(&report);
        for line in report.describe(&endpoint) {
            println!("{line}");
        }
        mismatches += report.mismatches();
    }
    println!("{} JSON-RPC request(s) to {endpoint}", rpc.request_count());
    if mismatches > 0 {
        bail!("{mismatches} row(s) differ between the sealed segments and {endpoint}");
    }
    Ok(())
}

/// The background audit's settings, from `dev`'s `--audit-*` flags.
pub struct Settings {
    pub rpc: String,
    pub per_day: u64,
    pub span: u64,
    pub seed: Option<u64>,
}

/// Start the background audit: one sampled range every `86400 / per_day` seconds, the first one
/// interval after start. It never stops the nest; a failed sample is a warning and a counter.
pub fn spawn(
    dir: PathBuf,
    config: &crate::config::Config,
    indexing: &[String],
    settings: Settings,
) -> Result<tokio::task::JoinHandle<()>> {
    refuse_indexing_endpoint(&settings.rpc, indexing)?;
    let auditor = Auditor::new(&dir, config)?;
    let rpc = crate::rpc::RpcClient::new(vec![settings.rpc.clone()])?;
    let endpoint = crate::rpc::redact_url(&settings.rpc);
    let chain_id = config.nest.chain_id;
    let interval = Duration::from_secs((86_400 / settings.per_day.max(1)).max(1));
    let seed = settings.seed.unwrap_or_else(unix_now);
    let span = settings.span;
    crate::metrics::METRICS.set_audit_on();
    tracing::info!(
        "sealed audit on: {} range(s) of {span} blocks a day against {endpoint}, seed {seed}",
        settings.per_day
    );
    Ok(tokio::spawn(async move {
        let mut chain_verified = false;
        for index in 0u64.. {
            tokio::time::sleep(interval).await;
            let sample = async {
                if !chain_verified {
                    rpc.verify_chain_ids(chain_id).await?;
                    chain_verified = true;
                }
                // The seal path's copy of the store's watermark, seeded from it at start (#918).
                let watermark = Some(crate::metrics::METRICS.sealed_through_val());
                let Some(ranges) =
                    sample_ranges(&dir, &auditor, watermark, seed, index..index + 1, span)?
                else {
                    return Ok(None);
                };
                let (from, to) = ranges[0];
                audit_range(&dir, &auditor, &rpc, from, to).await.map(Some)
            };
            match sample.await {
                Ok(Some(report)) => {
                    record(&report);
                    let lines = report.describe(&endpoint);
                    if report.mismatches() == 0 {
                        tracing::info!("{} (sample {index}, seed {seed})", lines[0]);
                    } else {
                        tracing::warn!("{} (sample {index}, seed {seed})", lines[0]);
                        for line in &lines[1..] {
                            tracing::warn!("{line}");
                        }
                    }
                }
                Ok(None) => tracing::info!("sealed audit: nothing has sealed yet"),
                Err(e) => {
                    crate::metrics::METRICS.inc_audit_errors();
                    tracing::warn!(
                        "sealed audit sample {index} (seed {seed}) against {endpoint} failed: {e:#}"
                    );
                }
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalogue() -> Catalogue {
        Catalogue {
            lo: 100,
            hi: 10_099,
            weighted: vec![(100, 199, 5), (5_000, 5_999, 95)],
            rows: 100,
        }
    }

    #[test]
    fn a_seed_picks_the_same_ranges_and_another_seed_does_not() {
        let c = catalogue();
        let picks = |seed| (0..8).map(|i| c.pick(seed, i, 50)).collect::<Vec<_>>();
        assert_eq!(picks(7), picks(7));
        assert_ne!(picks(7), picks(8));
    }

    #[test]
    fn every_pick_is_span_blocks_inside_the_sealed_range() {
        let c = catalogue();
        for i in 0..2_000 {
            let (from, to) = c.pick(42, i, 50);
            assert_eq!(to - from + 1, 50, "sample {i}");
            assert!(from >= c.lo && to <= c.hi, "sample {i}: {from}..={to}");
        }
        assert_eq!(c.pick(1, 0, 1_000_000), (c.lo, c.hi));
    }

    #[test]
    fn even_samples_follow_the_rows_and_odd_samples_do_not() {
        let c = catalogue();
        let dense = |(from, to): (u64, u64)| to >= 5_000 && from <= 5_999;
        let even = (0..400)
            .step_by(2)
            .filter(|&i| dense(c.pick(3, i, 10)))
            .count();
        let odd = (1..400)
            .step_by(2)
            .filter(|&i| dense(c.pick(3, i, 10)))
            .count();
        // 95% of the rows sit in a tenth of the blocks.
        assert!(even > 170, "{even} of 200 row-weighted samples were dense");
        assert!(odd < 40, "{odd} of 200 uniform samples were dense");
    }

    #[test]
    fn the_indexing_endpoint_is_refused_and_another_is_not() {
        let pool = vec!["https://eth.example/v2/KEY/".to_string()];
        let err = refuse_indexing_endpoint("https://eth.example/v2/KEY", &pool).unwrap_err();
        assert!(!format!("{err:#}").contains("KEY"), "{err:#}");
        refuse_indexing_endpoint("https://other.example/v2/KEY", &pool).unwrap();
    }

    struct Serves(Vec<crate::rpc::Log>);

    #[async_trait::async_trait]
    impl Source for Serves {
        async fn tip(&self) -> Result<u64> {
            Ok(u64::MAX)
        }
        async fn block_hash(&self, _: u64) -> Result<Option<String>> {
            Ok(None)
        }
        async fn logs(&self, _: &LogFilter, _: u64, _: u64) -> Result<Vec<crate::rpc::Log>> {
            Ok(self.0.clone())
        }
    }

    fn nest(d: &Path, extra: &str) -> Auditor {
        std::fs::create_dir_all(d.join("abis")).unwrap();
        std::fs::write(
            d.join("abis/erc20.json"),
            include_str!("../tests/fixtures/erc20.json"),
        )
        .unwrap();
        std::fs::write(
            d.join(crate::config::CONFIG_FILE),
            format!(
                r#"
[nest]
name = "usdc"
chain = "mainnet"
chain_id = 1
rpc_urls = []

[[contracts]]
alias = "usdc"
address = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
abi = "abis/erc20.json"
events = ["Transfer"]
{extra}"#
            ),
        )
        .unwrap();
        let config = crate::config::Config::load(d).unwrap();
        Auditor::new(d, &config).unwrap()
    }

    #[test]
    fn a_blocks_table_is_neither_compared_nor_asked_for() {
        let dir = tempfile::tempdir().unwrap();
        let auditor = nest(dir.path(), "\n[extract]\nblocks = true\n");
        assert!(auditor.registry.schema().len() > 1);
        let tables: Vec<&str> = auditor.schema.iter().map(|t| t.table.as_str()).collect();
        assert_eq!(tables, ["usdc__transfer"]);
        assert_eq!(auditor.filter.topic0s(), [auditor.schema[0].topic0.clone()]);
    }

    fn transfer_at(auditor: &Auditor, block: u64) -> crate::rpc::Log {
        let word = |n: u64| format!("0x{n:064x}");
        crate::rpc::Log {
            address: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48".into(),
            topics: vec![auditor.schema[0].topic0.clone(), word(1), word(2)],
            data: word(5),
            block_number: block,
            block_hash: word(block),
            tx_hash: word(block << 20),
            log_index: 0,
        }
    }

    /// Transfers sealed with rows only at block 10, in the cut 1..=20; the cut 21..=1000 held only a
    /// blocks row, so the event table sealed it empty; a screening run sealed a hit over 21..=5000.
    fn empty_tail(d: &Path) -> Auditor {
        let auditor = nest(d, "\n[extract]\nblocks = true\n");
        let event = auditor.registry.decode(&transfer_at(&auditor, 10)).unwrap();
        crate::seal::test_set_table_floor(d, 0);
        crate::seal::seal_range(d, &[event.unwrap().to_json().to_string()], 1, 20).unwrap();
        let row = |table: &str, block: u64| {
            serde_json::json!({"table": table, "block_number": block, "log_index": 0}).to_string()
        };
        crate::seal::seal_range(d, &[row(crate::registry::BLOCKS_TABLE, 900)], 21, 1_000).unwrap();
        crate::seal::seal_range(d, &[row("sanction_hit", 3_000)], 21, 5_000).unwrap();
        auditor
    }

    /// One segment holding `per_block[i]` transfers in block `1 + i`.
    fn seal_dense(d: &Path, auditor: &Auditor, per_block: &[u64]) {
        let mut rows = Vec::new();
        for (i, &n) in per_block.iter().enumerate() {
            for log_index in 0..n {
                let log = crate::rpc::Log {
                    log_index,
                    ..transfer_at(auditor, 1 + i as u64)
                };
                rows.push(
                    auditor
                        .registry
                        .decode(&log)
                        .unwrap()
                        .unwrap()
                        .to_json()
                        .to_string(),
                );
            }
        }
        crate::seal::seal_range(d, &rows, 1, per_block.len() as u64).unwrap();
        let segments = crate::seal::load_manifest(d).unwrap().tables;
        assert_eq!(segments.values().flatten().count(), 1, "one large segment");
    }

    #[tokio::test]
    async fn a_small_sample_of_a_large_segment_decodes_only_its_own_rows() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        let auditor = nest(d, "");
        seal_dense(d, &auditor, &[10; 10_000]);
        let r = audit_range(d, &auditor, &Serves(vec![]), 5_000, 5_009)
            .await
            .unwrap();
        assert_eq!(r.sealed_rows, 100);
        assert_eq!(crate::seal::test_rows_materialised(d), 100);
    }

    #[tokio::test]
    async fn a_silent_endpoint_over_a_dense_range_holds_the_budget_and_says_it_was_cut() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        let auditor = nest(d, "");
        seal_dense(d, &auditor, &[100; 1_000]);
        // One sealed log at block 500, past where the sealed side will be cut: not a mismatch.
        let served = Serves(vec![transfer_at(&auditor, 500)]);
        let r = audit_range(d, &auditor, &served, 1, 1_000).await.unwrap();
        let held = SAMPLE_LOG_BUDGET as u64 / 100;
        assert_eq!(
            (r.to, r.requested_to),
            (held, 1_000),
            "{}",
            r.describe("x")[0]
        );
        assert_eq!(r.sealed_rows, SAMPLE_LOG_BUDGET);
        assert_eq!(r.sealed_only.len(), SAMPLE_LOG_BUDGET);
        assert!(r.endpoint_only.is_empty() && r.differing.is_empty());
        assert!(crate::seal::test_rows_materialised(d) <= SAMPLE_LOG_BUDGET + 100);
        let line = &r.describe("x")[0];
        assert!(
            line.contains(&format!("stopped at {held} of 1000")),
            "{line}"
        );
        assert!(!line.contains("alone"), "{line}");
    }

    #[tokio::test]
    async fn a_block_over_the_budget_on_its_own_is_compared_whole() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        let auditor = nest(d, "");
        let big = SAMPLE_LOG_BUDGET as u64 + 5_000;
        seal_dense(d, &auditor, &[big, 10, 10]);
        let r = audit_range(d, &auditor, &Serves(vec![]), 1, 3)
            .await
            .unwrap();
        assert_eq!(
            (r.to, r.sealed_rows),
            (1, big as usize),
            "{}",
            r.describe("x")[0]
        );
        let line = &r.describe("x")[0];
        assert!(line.contains("stopped at 1 of 3"), "{line}");
        assert!(line.contains("block 1 alone"), "{line}");
    }

    #[test]
    fn samples_reach_the_common_watermark_and_not_a_screen_seal() {
        let dir = tempfile::tempdir().unwrap();
        let auditor = empty_tail(dir.path());
        let picks = |watermark| {
            sample_ranges(dir.path(), &auditor, watermark, 5, 0..200, 5)
                .unwrap()
                .unwrap()
        };
        let inferred = picks(None);
        assert!(
            inferred.iter().all(|&(from, to)| from >= 1 && to <= 1_000),
            "{inferred:?}"
        );
        assert!(
            inferred.iter().any(|&(from, _)| from > 20),
            "the empty stretch went unsampled"
        );
        let stored = picks(Some(800));
        assert!(stored.iter().all(|&(_, to)| to <= 800), "{stored:?}");
        assert!(stored.iter().any(|&(from, _)| from > 20));
        // Zero is a gauge not yet seeded, not a watermark.
        assert_eq!(picks(Some(0)), inferred);
    }

    #[test]
    fn an_event_table_that_never_sealed_a_row_is_still_sampled() {
        // `extra` lands inside the one [[contracts]] table.
        let sealed_blocks_only = |extra: &str| {
            let dir = tempfile::tempdir().unwrap();
            let auditor = nest(dir.path(), extra);
            crate::seal::test_set_table_floor(dir.path(), 0);
            let row = serde_json::json!({
                "table": crate::registry::BLOCKS_TABLE, "block_number": 900, "log_index": 0,
            });
            crate::seal::seal_range(dir.path(), &[row.to_string()], 1, 1_000).unwrap();
            sample_ranges(dir.path(), &auditor, None, 3, 0..50, 5)
                .unwrap()
                .expect("blocks sealed, so the event table sealed empty")
        };
        let from_start = sealed_blocks_only("start_block = 300\n");
        assert!(
            from_start
                .iter()
                .all(|&(from, to)| from >= 300 && to <= 1_000),
            "{from_start:?}"
        );
        let from_cut = sealed_blocks_only("");
        assert!(from_cut.iter().any(|&(from, _)| from < 300), "{from_cut:?}");

        // Before the first seal the watermark sits at start - 1.
        let dir = tempfile::tempdir().unwrap();
        let auditor = nest(dir.path(), "start_block = 300\n");
        let none = sample_ranges(dir.path(), &auditor, Some(299), 3, 0..4, 5).unwrap();
        assert_eq!(none, None);
    }

    #[tokio::test]
    async fn a_log_omitted_from_an_empty_sealed_stretch_is_reported() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        let auditor = empty_tail(d);
        let ranges = sample_ranges(d, &auditor, None, 1, 0..1, 5_000)
            .unwrap()
            .unwrap();
        assert_eq!(ranges, [(1, 1_000)]);
        let served = Serves(vec![transfer_at(&auditor, 10), transfer_at(&auditor, 500)]);
        let r = audit_range(d, &auditor, &served, 1, 1_000).await.unwrap();
        assert_eq!(r.endpoint_only.len(), 1, "{r:?}");
        assert!(r.endpoint_only[0].contains("\"block_number\":500"), "{r:?}");
        assert!(r.sealed_only.is_empty() && r.differing.is_empty(), "{r:?}");
    }

    #[tokio::test]
    async fn a_row_sealed_twice_is_reported_as_sealed_only() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        let auditor = nest(d, "");
        let word = |n: u64| format!("0x{n:064x}");
        let log = crate::rpc::Log {
            address: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48".into(),
            topics: vec![auditor.schema[0].topic0.clone(), word(1), word(2)],
            data: word(5),
            block_number: 10,
            block_hash: word(10),
            tx_hash: word(99),
            log_index: 0,
        };
        let row = auditor.registry.decode(&log).unwrap().unwrap();
        let next_log = crate::rpc::Log {
            block_number: 11,
            block_hash: word(11),
            ..log.clone()
        };
        let next = auditor.registry.decode(&next_log).unwrap().unwrap();
        let json = |r: &DecodedRow| r.to_json().to_string();
        crate::seal::test_set_table_floor(d, 0);
        crate::seal::seal_range(d, &[json(&row)], 10, 10).unwrap();
        crate::seal::seal_range(d, &[json(&row), json(&next)], 10, 11).unwrap();

        let (rows, duplicates, _) =
            sealed_rows(d, &auditor.schema, 0, 20, SAMPLE_LOG_BUDGET).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(duplicates, vec![canonical(&row)]);

        let r = audit_range(d, &auditor, &Serves(vec![log, next_log]), 10, 11)
            .await
            .unwrap();
        assert_eq!((r.sealed_rows, r.endpoint_rows), (3, 2));
        assert_eq!(r.sealed_only, vec![canonical(&row)]);
        assert_eq!(r.mismatches(), 1);
    }
}
