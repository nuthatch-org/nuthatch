//! `nuthatch bench backfill` - honest, reproducible backfill throughput measurement (RFC-0004).
//!
//! Runs the real fetch → decode → store path over a *pinned* block range and reports sustained
//! events/sec, wall-clock, peak RSS, and RPC requests, emitting a `bench-report.json`. Measure
//! first, optimise second: nothing here optimises anything - it establishes the baseline that the
//! seal-direct / adaptive-chunker / pipeline work (later slices) must each beat on a before/after.
//!
//! The house rule (RFC-0004): every published number traces to a report artifact produced here, with
//! date, provider, hardware, and commit. No hand-typed numbers on the website.

use anyhow::{bail, Context, Result};
use serde::Serialize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::chains;
use crate::cli::BackfillBenchArgs;
use crate::config::Config;
use crate::factory::{ChildRegistry, FactorySet};
use crate::registry::DecodeRegistry;
use crate::rpc::RpcClient;
use crate::store::Store;

/// Which backfill path a run takes. Extracted from `one_run`'s `if` chain so the choice is testable
/// without a network: the factory-nest bug this exists to prevent was a *dispatch* bug, not a decode
/// or fetch bug, and it was invisible because the wrong path still returned a plausible number.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum BackfillPath {
    /// Sequential two-pass factory backfill - the only path that discovers children (RFC-0009 §3).
    Factory,
    /// Concurrent seal-direct.
    Pipelined,
    /// Sequential seal-direct.
    Direct,
    /// Decode → redb hot store.
    HotStore,
}

/// A factory nest takes the factory path **whatever else was asked for**: `--concurrency` cannot
/// override it, because the pipelined path has no child registry and would silently measure the
/// factory's own events alone.
fn choose_path(is_factory: bool, seal_direct: bool, concurrency: usize) -> BackfillPath {
    if is_factory {
        BackfillPath::Factory
    } else if seal_direct && concurrency > 1 {
        BackfillPath::Pipelined
    } else if seal_direct {
        BackfillPath::Direct
    } else {
        BackfillPath::HotStore
    }
}

/// What the run actually did with the fetch window, independent of what `--window-adaptive` asked
/// for (#744). `Pipelined` and `Factory` adapt unconditionally - the same as `nuthatch dev` - so the
/// flag has no effect on them; only `Direct` and `HotStore` (the concurrency-1, non-factory paths)
/// take it. `BenchReport.window_adaptive` is built from this, not from the flag directly, because
/// echoing the flag is the exact mistake this issue replaces (`window_adaptive: args.seal_direct`
/// echoed the storage flag rather than reporting what the run did).
/// Should the operator be told that `--window-adaptive` changed nothing? (#847)
///
/// The condition here used to be `requested && !effective_window_adaptive(path, requested)` - "was
/// adaptivity asked for and refused". [`effective_window_adaptive`] never does that: the only two
/// paths that ignore the flag ignore it *upward*, so the guard was false in all eight rows of its
/// truth table and the clause it gated had never printed once.
///
/// The situation worth naming is the opposite one, and it is real: the flag was passed, and the path
/// would have adapted anyway. Saying so is the difference between a reproducible command line and a
/// number nobody can account for - which is the whole reason #744 exists.
fn flag_had_no_effect(path: BackfillPath, requested: bool) -> bool {
    requested && matches!(path, BackfillPath::Factory | BackfillPath::Pipelined)
}

fn effective_window_adaptive(path: BackfillPath, requested: bool) -> bool {
    match path {
        BackfillPath::Factory | BackfillPath::Pipelined => true,
        BackfillPath::Direct | BackfillPath::HotStore => requested,
    }
}

/// One run's raw measurements.
struct Run {
    events: u64,
    wall_clock_s: f64,
    events_per_sec: f64,
    peak_rss_mb: u64,
    rpc_requests: u64,
    /// Children discovered during the run - 0 for a nest with no `[[factories]]`. A factory nest that
    /// reports 0 here has measured nothing but its own factory events, which is the failure this
    /// field exists to make visible.
    children: u64,
    /// Call rows sealed across the run's declared `[[calls]]` - 0 for a nest with none. See
    /// [`BenchReport::calls_resolved`].
    calls_resolved: u64,
    /// sha256 of the tape this run recorded or replayed (RFC-0039), so a published number names the
    /// exact bytes it came from. `None` on a live run.
    fixture_content_address: Option<String>,
    /// Bytes the mirror had uploaded when ingestion finished; `None` without `--publish-target`.
    published_bytes: Option<u64>,
}

/// The published artifact: medians across runs plus the pinned inputs and provenance.
#[derive(Debug, Serialize)]
pub struct BenchReport {
    pub bench: &'static str,
    pub label: Option<String>,
    pub nest: String,
    pub chain: String,
    pub from_block: u64,
    pub to_block: u64,
    pub blocks: u64,
    /// The window the backfill *started* from - the chain default (20 on mainnet, deliberately small
    /// for a dense L1 tip). On the seal-direct paths it is only a starting point: the controller sizes
    /// every subsequent window from what came back (RFC-0029 §6f), so this number does not describe
    /// what the run actually did.
    ///
    /// It is named for what it is because the old name (`window`) invited exactly the wrong reading.
    /// This run started at 20 and reached the 100,000 ceiling; a report claiming `"window": 20` reads
    /// as though 22.2M blocks were fetched 20 at a time, which would be 1,110,000 requests rather than
    /// the 454 it took. `rpc_requests` is the honest measure of range control, and the reason there is
    /// no `--window` override here is that there is no longer anything useful to override.
    pub initial_window: u64,
    /// Whether the fetch window actually adapted during this run. Set from
    /// [`effective_window_adaptive`], not echoed from `--seal-direct` - the coupling this replaces
    /// (`window_adaptive: args.seal_direct`) is what made every "vs hot store" ratio this project
    /// published a comparison across two variables at once, with `--window-adaptive` (#744) the
    /// independent flag that separates them. Always `true` on the pipelined/factory paths, which
    /// adapt unconditionally like `nuthatch dev` does.
    pub window_adaptive: bool,
    pub seal_direct: bool,
    pub concurrency: usize,
    pub runs: usize,
    /// Medians across runs.
    pub events: u64,
    pub wall_clock_s: f64,
    pub events_per_sec: f64,
    pub peak_rss_mb: u64,
    pub rpc_requests: u64,
    /// Whether the nest declares `[[factories]]`, and so took the sequential two-pass factory
    /// backfill rather than the static-address path. Recorded because it changes what `concurrency`
    /// means (it does not apply) and because a factory nest measured without it is measuring the
    /// wrong workload.
    pub factory: bool,
    /// Children discovered during the run. For a factory workload this is the number that says the
    /// harness actually followed the templates: `"factory": true` with `"children": 0` is a broken
    /// measurement, not a fast one.
    pub children: u64,
    /// How many `[[calls]]` the nest declares. Recorded so `calls_resolved: 0` is visibly a control
    /// (nothing declared) rather than indistinguishable from a run that resolved every one and just
    /// happened to declare none (#725).
    pub calls_declared: usize,
    /// Call rows actually sealed/stored across the run's declared `[[calls]]` - the number #725
    /// exists because nothing recorded before it. A nest with `calls_declared > 0` and
    /// `calls_resolved: 0` measured a strawman, the same failure #224 found in this file for the
    /// hot-store path three weeks earlier.
    pub calls_resolved: u64,
    /// Event names the nest declared (`[[contracts]].events` / `[[templates]].events`). Empty
    /// means every event in the ABI - the default, and the 12,933 nest. A `Transfer`-only nest
    /// is `["Transfer"]`, the 11,758 nest. Always present so those two cannot produce identical
    /// artefacts (#751).
    #[serde(default)]
    pub events_declared: Vec<String>,
    pub commit: Option<String>,
    /// The RPC endpoint's host, never the full URL - an API key in a committed benchmark report is a
    /// credential leak, and these get pasted into issues.
    ///
    /// RFC-0004's house rule is that every published number traces to provider, hardware, date and
    /// commit. Three of those were already recorded and this one was a documentation promise. A number
    /// whose provider is unknown is not comparable to any other number, because provider caps and
    /// latency dominate backfill throughput far more than anything in this codebase does.
    pub provider: Option<String>,
    /// Enough hardware to make two reports comparable: cores and total RAM.
    pub hardware: Option<String>,
    /// Where the throwaway redb/Parquet landed (#781). `"tmpfs"` means the storage comparison was
    /// measured on RAM; a number that does not say this is not comparable to one that sat on disk.
    pub work_backing: String,
    /// sha256 of the RFC-0039 tape this run recorded or replayed. `None` on a live run.
    ///
    /// This is what lets a published figure name the exact bytes it came from. `289 events/sec`
    /// outlived the harness that produced it by five weeks because nothing tied the number to a run;
    /// provider, hardware, date and commit describe the *conditions*, and a content address describes
    /// the *input*.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fixture_content_address: Option<String>,
    /// True when the run answered from a tape rather than a network.
    ///
    /// Stated once here rather than left for a reader to infer from a suspiciously low
    /// `rpc_requests`: on a replayed run that field counts **tape reads served**, not HTTP requests
    /// sent. It keeps its meaning - *how many times did the code under test ask the source for
    /// something* - and loses only "how many bytes went over a wire".
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub replayed: bool,
    /// Where each run mirrored its seals (RFC-0052 S2), and the median bytes uploaded by the time
    /// ingestion finished. A run that published nothing measured nothing about publishing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publish_target: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published_bytes: Option<u64>,
}

/// The endpoint's host, with any credentials stripped.
///
/// Providers put API keys in the path (`/v2/<key>`) as often as in a header, so the path goes too -
/// host and scheme identify the provider, which is all a report needs.
/// Declared event names across contracts and templates, sorted and uniqued. Empty is "every
/// event in the ABI", not "none were recorded" - a missing field on an old artefact is that
/// second meaning, and `#[serde(default)]` is how those still load.
fn events_declared_of(config: &crate::config::Config) -> Vec<String> {
    let mut v: Vec<String> = config
        .contracts
        .iter()
        .flat_map(|c| c.events.iter().cloned())
        .chain(
            config
                .templates
                .iter()
                .flat_map(|t| t.events.iter().cloned()),
        )
        .collect();
    v.sort();
    v.dedup();
    v
}

fn provider_of(url: &str) -> Option<String> {
    crate::rpc::url_host(&reqwest::Url::parse(url).ok()?)
}

/// A replayed report names the host the tape was recorded against, not the nest's configured
/// pool. The nest still has `rpc_urls` on disk; `--replay` never dials them.
fn report_provider(tape: &TapeMode<'_>, rpc_urls: &[String]) -> Option<String> {
    match tape {
        TapeMode::Replay(path) => crate::tape::ReplaySource::open(path)
            .ok()
            .and_then(|s| s.provider())
            .or_else(|| rpc_urls.first().and_then(|u| provider_of(u))),
        _ => rpc_urls.first().and_then(|u| provider_of(u)),
    }
}

/// The `[[calls]]` archive endpoint mention, routed through `provider_of` for the same reason as
/// every other endpoint mention (see `provider_of` above) - `--state-rpc` is exactly the kind of
/// URL that carries an API key in the path.
fn calls_summary_line(calls_len: usize, state_rpc_urls: &[String]) -> String {
    format!(
        "declared [[calls]]: {} - resolved against {}",
        calls_len,
        state_rpc_urls
            .first()
            .and_then(|u| provider_of(u))
            .unwrap_or_else(|| "?".to_string())
    )
}

/// Cores and total RAM, best-effort. `None` rather than a guess when it cannot be read - an invented
/// hardware string is worse than an absent one, because it looks authoritative.
fn hardware_summary() -> Option<String> {
    let cores = std::thread::available_parallelism().ok()?.get();
    let ram_gb = total_ram_gb();
    Some(match ram_gb {
        Some(gb) => format!("{cores} cores, {gb} GB RAM"),
        None => format!("{cores} cores"),
    })
}

fn total_ram_gb() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
        let kb: u64 = meminfo
            .lines()
            .find(|l| l.starts_with("MemTotal:"))?
            .split_whitespace()
            .nth(1)?
            .parse()
            .ok()?;
        return Some((kb + 512 * 1024) / (1024 * 1024));
    }
    #[cfg(target_os = "macos")]
    {
        let out = std::process::Command::new("sysctl")
            .args(["-n", "hw.memsize"])
            .output()
            .ok()?;
        let bytes: u64 = String::from_utf8_lossy(&out.stdout).trim().parse().ok()?;
        return Some(bytes / (1024 * 1024 * 1024));
    }
    #[allow(unreachable_code)]
    None
}

/// Resolve `--keep`, refusing the one path a caller is most likely to type by mistake.
///
/// Every run clears this directory before starting, so pointing it at the nest itself would delete
/// `nuthatch.toml` and the indexed data it was aimed at - the worst outcome available from this
/// flag. The check runs before the first run rather than on the way out, because the wipe is the
/// first thing `one_run` does with the path.
///
/// Split out of `backfill` so it is *reachable*: inline, the only way to reach the guard was a full
/// backfill against a live RPC, which is why it shipped on inspection alone. A guard nobody
/// exercises is the argument this whole change is built on, so it does not get an exemption.
fn resolve_keep(keep: &Option<String>) -> Result<Option<PathBuf>> {
    let Some(p) = keep.as_ref().map(PathBuf::from) else {
        return Ok(None);
    };
    if p.join(crate::config::CONFIG_FILE).exists() {
        bail!(
            "--keep {} is a nest directory (it holds {}), and every bench run clears its work dir \
             before starting. Point --keep at a new or dedicated path, then query it with \
             `nuthatch sql --dir <that path>`.",
            p.display(),
            crate::config::CONFIG_FILE
        );
    }
    Ok(Some(p))
}

pub async fn backfill(args: BackfillBenchArgs) -> Result<()> {
    if args.to < args.from {
        bail!("--to ({}) is before --from ({})", args.to, args.from);
    }
    let dir = PathBuf::from(&args.dir);
    let mut config = Config::load(&dir)?;
    // RFC-0023 tier 3's archive endpoint, same flag and the same reason `dev` carries it off
    // `nuthatch.toml`: an archive endpoint usually carries an API key.
    config.state_rpc_urls = args.state_rpc.clone();
    let registry = Arc::new(crate::registry::from_nest(&dir, &config)?);

    // Refuse rather than silently bench a declared-but-unresolved `[[calls]]` nest (#725): all three
    // seal-direct call sites below used to pass an empty calls slice regardless of what the nest
    // declared, so a run against a calls nest with no `--state-rpc` looked identical to one that had
    // resolved every call. Same guard and message shape as the production refusal at nest build time.
    //
    // The guard sits above path selection and therefore covers the hot-store arm too, which was only
    // correct from #743 onwards - before it, that arm could not resolve a call whatever endpoint you
    // supplied, so the refusal demanded a flag the path it selected had no way to use.
    let state_rpc = if config.state_rpc_urls.is_empty() {
        if !config.calls.is_empty() {
            bail!(
                "this nest declares {} `[[calls]]` entr{}, which need historical `eth_call` and \
                 therefore an archive endpoint.\n\n\
                 Pass `--state-rpc <url>` to `nuthatch bench backfill`, the same flag `nuthatch dev` \
                 takes. Benching without it would resolve zero of them and report a number \
                 indistinguishable from a run that resolved every one.",
                config.calls.len(),
                if config.calls.len() == 1 { "y" } else { "ies" },
            );
        }
        None
    } else {
        Some(Arc::new(crate::rpc::RpcClient::new(
            config.state_rpc_urls.clone(),
        )?))
    };

    // **A factory nest must be measured through the factory path** (RFC-0009 §3). The static
    // `addresses` filter below is fixed at start-up, so a factory nest measured through the plain
    // paths sees only its own creation events and never the children they announce - it reports a
    // fast, small, entirely wrong number, and reports success. That is the "harness measuring a
    // strawman" failure of RFC-0029 §4b in a second place, found running OBIB case 6 (which is
    // *entirely* child activity: 232 factory events against 35,039 expected records).
    let factory = {
        let fs = FactorySet::resolved(&config, &registry)?;
        if fs.is_empty() {
            None
        } else {
            Some(fs)
        }
    };

    let rpc_urls = match &args.rpc {
        Some(u) => vec![u.clone()],
        None => config.nest.rpc_urls.clone(),
    };
    let window = chains::lookup(&config.nest.chain)
        .map(|c| c.log_window)
        .unwrap_or(20);
    let addresses: Vec<String> = registry
        .addresses()
        .iter()
        .map(|a| format!("0x{}", hex::encode(a)))
        .collect();
    let topic0s: Vec<String> = registry
        .topic0s()
        .iter()
        .map(|t| format!("0x{}", hex::encode(t)))
        .collect();

    println!(
        "bench backfill: nest '{}' on {}, blocks {}..={} ({} blocks), window {}, {} run(s)",
        config.nest.name,
        config.nest.chain,
        args.from,
        args.to,
        args.to - args.from + 1,
        window,
        args.runs,
    );

    println!(
        "storage path: {}{}",
        if args.seal_direct {
            "seal-direct (decode → Parquet, no hot store)"
        } else {
            "hot store (decode → redb)"
        },
        if args.seal_direct && args.concurrency > 1 && factory.is_none() {
            format!(", {}-way concurrent fetch", args.concurrency)
        } else {
            String::new()
        }
    );
    // #744: computed independently of the storage-path println above, since the two used to be the
    // same flag in disguise (`window_adaptive: args.seal_direct`).
    let path = choose_path(factory.is_some(), args.seal_direct, args.concurrency);
    let window_adaptive = effective_window_adaptive(path, args.window_adaptive);
    println!(
        "fetch window: {}{}",
        if window_adaptive {
            "adaptive (RFC-0004 §2 controller)"
        } else {
            "fixed"
        },
        if flag_had_no_effect(path, args.window_adaptive) {
            " (--window-adaptive has no effect here: the pipelined/factory path always adapts, the \
             same as `nuthatch dev`)"
        } else {
            ""
        }
    );
    if let Some(fs) = &factory {
        // Say it out loud rather than silently ignoring the flag: the factory backfill is the
        // sequential two-pass path (pass 2 needs pass 1's discoveries), so `--concurrency` does not
        // apply. A number that quietly ignored the flag would be unreproducible by anyone reading the
        // command line that produced it.
        println!(
            "factory nest: {} template(s) - sequential two-pass factory backfill{}",
            fs.templates().len(),
            if args.concurrency > 1 {
                ", --concurrency does not apply"
            } else {
                ""
            }
        );
    }
    if !config.calls.is_empty() {
        // Reaching here means `state_rpc` is `Some` - the guard above refused otherwise.
        println!(
            "{}",
            calls_summary_line(config.calls.len(), &config.state_rpc_urls)
        );
    }

    let keep = resolve_keep(&args.keep)?;
    if let Some(p) = keep.as_deref() {
        println!(
            "  --keep: writing run data to {} (last run survives)",
            p.display()
        );
    }

    // RFC-0039. `--replay` touches no network by construction; `--record` wraps the live client and
    // flushes a tape once the runs are done.
    let tape_mode = match (
        args.record.as_deref().map(std::path::Path::new),
        args.replay.as_deref().map(std::path::Path::new),
    ) {
        (Some(_), Some(_)) => {
            anyhow::bail!("--record and --replay are mutually exclusive: one writes a tape, the other reads one")
        }
        (Some(p), None) => TapeMode::Record(p),
        (None, Some(p)) => TapeMode::Replay(p),
        (None, None) => TapeMode::Live,
    };

    let mut runs = Vec::with_capacity(args.runs);
    for run in 1..=args.runs {
        let r = one_run(
            &rpc_urls,
            &registry,
            factory.as_ref(),
            &addresses,
            &topic0s,
            &config.calls,
            state_rpc.as_deref(),
            config.nest.chain_id,
            args.from,
            args.to,
            window,
            args.seal_direct,
            args.window_adaptive,
            args.concurrency,
            run,
            keep.as_deref(),
            &tape_mode,
            args.publish_target.as_deref().map(|t| (dir.as_path(), t)),
        )
        .await?;
        println!(
            "  run {run}/{}: {} events in {:.1}s = {:.0} ev/s, peak {} MB, {} rpc req{}{}{}",
            args.runs,
            r.events,
            r.wall_clock_s,
            r.events_per_sec,
            r.peak_rss_mb,
            r.rpc_requests,
            if factory.is_some() {
                format!(", {} children", r.children)
            } else {
                String::new()
            },
            if config.calls.is_empty() {
                String::new()
            } else {
                format!(", {} call row(s) resolved", r.calls_resolved)
            },
            match r.published_bytes {
                Some(b) => format!(", {b} bytes published"),
                None => String::new(),
            }
        );
        runs.push(r);
    }

    let report = BenchReport {
        bench: "backfill",
        label: args.label.clone(),
        nest: config.nest.name.clone(),
        chain: config.nest.chain.clone(),
        from_block: args.from,
        to_block: args.to,
        blocks: args.to - args.from + 1,
        initial_window: window,
        // What the run actually did, not the flag echoed back - see `effective_window_adaptive`.
        window_adaptive,
        seal_direct: args.seal_direct,
        // The factory path is sequential, so reporting the flag's value would describe a run that did
        // not happen.
        concurrency: if args.seal_direct && factory.is_none() {
            args.concurrency
        } else {
            1
        },
        runs: args.runs,
        events: median_u64(runs.iter().map(|r| r.events)),
        wall_clock_s: round2(median_f64(runs.iter().map(|r| r.wall_clock_s))),
        events_per_sec: round2(median_f64(runs.iter().map(|r| r.events_per_sec))),
        peak_rss_mb: median_u64(runs.iter().map(|r| r.peak_rss_mb)),
        rpc_requests: median_u64(runs.iter().map(|r| r.rpc_requests)),
        factory: factory.is_some(),
        children: median_u64(runs.iter().map(|r| r.children)),
        calls_declared: config.calls.len(),
        calls_resolved: median_u64(runs.iter().map(|r| r.calls_resolved)),
        events_declared: events_declared_of(&config),
        commit: git_commit(),
        provider: report_provider(&tape_mode, &rpc_urls),
        fixture_content_address: runs.first().and_then(|r| r.fixture_content_address.clone()),
        replayed: matches!(tape_mode, TapeMode::Replay(_)),
        publish_target: args.publish_target.clone(),
        published_bytes: runs
            .iter()
            .map(|r| r.published_bytes)
            .collect::<Option<Vec<_>>>()
            .map(|b| median_u64(b.into_iter())),
        hardware: hardware_summary(),
        work_backing: work_backing_of(keep.as_deref().unwrap_or(&default_bench_work(0))),
    };

    let json = serde_json::to_string_pretty(&report)?;
    println!("\n{json}");
    if let Some(out) = &args.out {
        write_report(out, &json)?;
    }
    Ok(())
}

/// Write a report where a `git diff` can read it: pretty JSON **with a trailing newline**.
///
/// `serde_json::to_string_pretty` ends at the closing brace, and these reports get committed under
/// `docs/bench/` as the reference numbers - `docs/bench/point-read.json` shipped without one until
/// #385. A committed file with no final newline is a file whose last line every diff, `cat` and
/// review tool renders wrongly, and the only reason it stayed that way is that nobody wanted to
/// hand-edit an artifact and thereby stop it being an artifact. Emitting the newline here is what
/// lets the committed baseline be **byte-identical** to the one CI uploads, which is the property
/// #385 is actually about: the file in the repo is the file the enforcing machine produced.
fn write_report(out: &str, json: &str) -> Result<()> {
    std::fs::write(out, format!("{json}\n")).with_context(|| format!("failed to write {out}"))?;
    println!("\nwrote {out}");
    Ok(())
}

/// Read-path bench report (`nuthatch bench query`): entity point-read latency and the `/sql` hot∪cold
/// scan cost + peak RSS. The regression guard the perf refactors (bound the hot-scan, persistent
/// session, compact row format) must each beat on a before/after - none of these were measurable
/// before, so they could regress silently.
#[derive(Debug, Serialize)]
pub struct QueryBenchReport {
    pub bench: &'static str,
    pub label: Option<String>,
    pub nest: String,
    pub chain: String,
    /// Rows in the unsealed hot tip a `/sql` query materialises into temp tables - the scan-cost driver
    /// and the #1 RAM risk on deep-finality L2s.
    pub hot_rows: u64,
    pub sealed_through: u64,
    pub reads: usize,
    /// Whether the timed point-reads ran against an already-warmed cache. Always true, and recorded
    /// rather than assumed: a point-read latency means nothing without it, and a reader comparing two
    /// releases has no other way to know these are not cold-start numbers.
    pub warm_cache: bool,
    pub point_read_p50_us: f64,
    pub point_read_p99_us: f64,
    pub point_read_p999_us: f64,
    pub sql: String,
    pub sql_iters: usize,
    pub sql_p50_ms: f64,
    pub sql_p99_ms: f64,
    pub sql_peak_rss_mb: u64,
    pub commit: Option<String>,
    /// Cores and total RAM, as [`BenchReport`] records. A point-read latency is *entirely* a property
    /// of the machine's storage and cache, so two of these reports are not comparable at all without
    /// it - more so than for backfill, where the provider dominates. It was already on the write-path
    /// report and missing here.
    pub hardware: Option<String>,
}

/// Enforce the `--min-reads` floor and the `--max-point-read-*` ceilings, listing **every** breach
/// rather than the first.
///
/// This is what makes the bench a gate rather than a print statement: CLAUDE.md requires benchmark
/// regressions to fail the build, and until now nothing compared a point-read number to anything.
/// The comparison lives here, in Rust, rather than in the CI shell script (as `footprint.sh` does its
/// own) precisely so `cargo test --lib` covers it - an untested gate is how a gate silently stops
/// gating.
///
/// **`--min-reads` is the important one, not the ceilings.** A nest with no sampled keys reports
/// `p50 = 0.0`, and zero is under every ceiling anyone would ever set - so the gate would pass
/// loudest exactly when it had measured nothing at all. That is the "a criterion phrased as the
/// absence of an effect passes trivially when the mechanism is missing" failure this project has hit
/// five times, and `footprint.sh` guards its own version of it by asserting the row count before
/// comparing the peak. Requiring the reads is the same guard.
///
/// A limit that is not passed is neither a pass nor a failure: it is simply not asked for, which is
/// how an operator running the bench by hand should experience it.
///
/// **The failure message names where to re-baseline, because the obvious place used to be the wrong
/// one.** It once said "re-baselining against a committed report", and the only committed report was
/// `docs/bench/point-read.json` - a 32-core dev-box number (p50 1.24µs) that `docs/benchmarks.md`
/// explicitly told the reader was *not* what the ceiling was set against (the runner's baseline is
/// 0.59-0.82µs). Following the instruction re-derived a runner ceiling from dev-box hardware, and
/// this gate is the one that already proved baseline and regression do not scale together across
/// machines: the scan mutation ran *faster* on the runner (18.15µs) than on the dev box (24.17µs).
/// So the message pointed at the enforcing machine and explicitly away from the file.
///
/// #385 fixed the file rather than the sentence: `docs/bench/point-read.json` is now an artifact
/// lifted from a green `main` run of the `point-read latency` job, so it records the runner's own
/// numbers and the message can name it again. The dev-box record moved to
/// `docs/bench/point-read-devbox.json` and is named here as the one *not* to re-derive from, because
/// it is still in the tree and is still the artifact a reader might reach for. The instruction that
/// carries the actual weight is neither filename: it is **check the `hardware` field matches the
/// machine you are measuring on**, which is the only thing that made the old file wrong.
fn check_gate(
    report: &QueryBenchReport,
    max_p50_us: Option<f64>,
    max_p99_us: Option<f64>,
    min_reads: Option<usize>,
) -> Result<()> {
    let mut breaches: Vec<String> = Vec::new();
    if let Some(min) = min_reads {
        if report.reads < min {
            breaches.push(format!(
                "timed {} point-read(s), below the --min-reads floor of {min} - the nest is empty or \
                 barely indexed, so its latencies are not a measurement and passing a ceiling with \
                 them would be a false pass",
                report.reads
            ));
        }
    }
    for (name, measured, max) in [
        ("point-read p50", report.point_read_p50_us, max_p50_us),
        ("point-read p99", report.point_read_p99_us, max_p99_us),
    ] {
        if let Some(max) = max {
            if measured > max {
                breaches.push(format!(
                    "{name} {measured:.1}µs exceeds the {max:.1}µs ceiling"
                ));
            }
        }
    }
    if breaches.is_empty() {
        return Ok(());
    }
    bail!(
        "point-read gate failed - {}.\nThis is a gate, not a warning: either the change made \
         point-reads slower, or the ceiling needs re-baselining - measured on the machine that \
         enforces it, and with a stated reason.\nThe reference is docs/bench/point-read.json, \
         which is a CI-runner artifact: check its `hardware` field matches the machine you are \
         measuring on before you derive anything from it. Not from \
         docs/bench/point-read-devbox.json: that is the 32-core dev-box record, and this ceiling \
         was set on the CI runner, whose baseline and whose regression are both different \
         numbers. See \"Where the ceilings come from\" in docs/benchmarks.md.",
        breaches.join("; ")
    )
}

/// `nuthatch bench query` - measure the read path against an already-indexed nest (run offline: it
/// opens the store directly). Establishes the point-read and `/sql` scan baselines the #40 perf
/// refactors are gated on.
pub fn query(args: crate::cli::QueryBenchArgs) -> Result<()> {
    let dir = PathBuf::from(&args.dir);
    let config = Config::load(&dir)?;
    // Non-creating (#413, #469): this reads an *already-indexed* nest, so a directory with no store
    // has no baseline to measure. `Store::open` would create one, `sample_entity_keys` would find
    // nothing in it, and the bench would report a baseline for the empty file it had just made -
    // and leave that file behind for a later `holds_data` to misread.
    let store = Store::open_existing(&dir.join(crate::config::DB_FILE)).with_context(|| {
        format!(
            "no indexed store at {} - either there is no nest here (check --dir), or `nuthatch \
             dev` is running against it and needs to be stopped first",
            dir.display()
        )
    })?;

    let keys = store.sample_entity_keys(args.reads)?;
    let hot = store.hot_rows_by_table()?;
    let hot_rows: u64 = hot.values().map(|v| v.len() as u64).sum();
    let sealed_through = store.sealed_through();

    // --- Point-read latency: time get_entity over the sampled keys. ---
    let (p50_us, p99_us, p999_us, n_reads) = if keys.is_empty() {
        (0.0, 0.0, 0.0, 0)
    } else {
        // **Warm the cache first, and discard it.** The `/sql` half below has always done this and the
        // point-read half never did, so a timed read could be paying for a redb page fault - which
        // makes it a measurement of the page cache's state rather than of the read path, and a gate
        // needs the same workload every run to mean anything.
        //
        // Warm is therefore what the gate measures, and `warm_cache` records that so nobody reads
        // these as cold-start latencies. Cold-start is a real and different question - it is not the
        // one a regression gate can ask, and it is not asked here.
        //
        // Measured warm on a 32-core Linux box, 256 hot rows, 15 runs at one commit: p50 0.66-1.59µs,
        // p99 0.77-1.88µs. The unwarmed spread was *not* measured, so this is a correctness argument
        // about what is being timed, not a claim of a stability improvement.
        //
        // **Every read is required to hit, and that is a gate condition rather than a sanity check.**
        // The keys come from `sample_entity_keys` over this same store, so a miss is impossible unless
        // the read path is broken - and a broken read path is *faster* than a working one. Timing a
        // `get_entity` that returns `None` for everything would sail under any ceiling, so a gate that
        // discards the result passes most loudly exactly when the thing it guards has been removed.
        // Counting hits is what stops the measurement from being vacuous.
        let mut hits = 0usize;
        for k in &keys {
            hits += usize::from(store.get_entity(k)?.is_some());
        }
        let mut us: Vec<f64> = Vec::with_capacity(keys.len());
        for k in &keys {
            let t = Instant::now();
            let found = store.get_entity(k)?.is_some();
            // `elapsed` is read before the counter moves, so the bookkeeping is outside the window.
            us.push(t.elapsed().as_nanos() as f64 / 1000.0);
            hits += usize::from(found);
        }
        let expected = keys.len() * 2;
        if hits != expected {
            bail!(
                "point-read gate failed - {} of {} reads returned no entity. The keys were sampled \
                 from this store, so every read must hit: these numbers time a read path that is \
                 not returning data, and a broken read is faster than a working one.",
                expected - hits,
                expected
            );
        }
        us.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        (
            percentile(&us, 50.0),
            percentile(&us, 99.0),
            percentile(&us, 99.9),
            us.len(),
        )
    };

    // --- /sql hot∪cold scan cost: run the query `iters` times, time each, sample peak RSS. ---
    let sql = match &args.sql {
        Some(s) => s.clone(),
        None => {
            // Default: count over the largest hot table (forces the full-tip materialisation); a fully
            // sealed nest (no hot rows) falls back to the first registry table.
            let table = match hot
                .iter()
                .max_by_key(|(_, v)| v.len())
                .map(|(t, _)| t.clone())
            {
                Some(t) => t,
                None => crate::registry::from_nest(&dir, &config)?
                    .tables()
                    .first()
                    .map(|d| d.table.clone())
                    .context("nest has no tables to scan - pass --sql")?,
            };
            format!("SELECT count(*) FROM {table}")
        }
    };
    let guard = || crate::analytics::QueryGuard {
        timeout: Duration::from_secs(60),
        max_rows: 5_000_000,
    };
    // Warm-up (attach segments + build temp tables) kept out of the percentiles.
    crate::analytics::query_hot_cold(&dir, &sql, guard(), &hot, sealed_through, &[])
        .with_context(|| format!("running bench query: {sql}"))?;
    let rss = RssSampler::start();
    let mut ms: Vec<f64> = Vec::with_capacity(args.iters.max(1));
    for _ in 0..args.iters.max(1) {
        let t = Instant::now();
        let _ = crate::analytics::query_hot_cold(&dir, &sql, guard(), &hot, sealed_through, &[])?;
        ms.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    let sql_peak_rss_mb = rss.stop();
    ms.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    println!(
        "bench query: nest '{}' on {} - {} hot rows, sealed_through {}, {} point-read(s), {} sql iter(s)",
        config.nest.name, config.nest.chain, hot_rows, sealed_through, n_reads, args.iters,
    );
    println!("  point-read: p50 {p50_us:.1}µs  p99 {p99_us:.1}µs  p99.9 {p999_us:.1}µs");
    println!(
        "  /sql `{}`: p50 {:.1}ms  p99 {:.1}ms  peak {} MB",
        sql,
        percentile(&ms, 50.0),
        percentile(&ms, 99.0),
        sql_peak_rss_mb
    );

    let report = QueryBenchReport {
        bench: "query",
        label: args.label.clone(),
        nest: config.nest.name.clone(),
        chain: config.nest.chain.clone(),
        hot_rows,
        sealed_through,
        reads: n_reads,
        // False on the no-keys branch, where no warm pass runs. `--min-reads` refuses that run
        // anyway, but the field exists to be trusted rather than to be usually right.
        warm_cache: n_reads > 0,
        point_read_p50_us: round2(p50_us),
        point_read_p99_us: round2(p99_us),
        point_read_p999_us: round2(p999_us),
        sql,
        sql_iters: args.iters,
        sql_p50_ms: round2(percentile(&ms, 50.0)),
        sql_p99_ms: round2(percentile(&ms, 99.0)),
        sql_peak_rss_mb,
        commit: git_commit(),
        hardware: hardware_summary(),
    };
    let json = serde_json::to_string_pretty(&report)?;
    println!("\n{json}");
    if let Some(out) = &args.out {
        write_report(out, &json)?;
    }

    // The gate last, and **after** the artifact is written: a run that fails the gate is the run whose
    // report you most want to read, so failing before writing it would throw away the evidence.
    check_gate(
        &report,
        args.max_point_read_p50_us,
        args.max_point_read_p99_us,
        args.min_reads,
    )
}

#[allow(clippy::too_many_arguments)]
/// Which source a bench run drives (RFC-0039). `Replay` holds no `RpcClient`, so a replayed run
/// cannot reach a network even by mistake.
pub enum TapeMode<'a> {
    Live,
    Record(&'a std::path::Path),
    Replay(&'a std::path::Path),
}

/// Owns the constructed source and answers "how many times did the code under test ask for
/// something", which is what `BenchReport.rpc_requests` has always meant.
enum BenchSource {
    Live(RpcClient),
    Recording(
        Option<crate::tape::RecordingSource<RpcClient>>,
        std::path::PathBuf,
    ),
    Replay(crate::tape::ReplaySource),
}

impl BenchSource {
    fn as_source(&self) -> &dyn crate::source::Source {
        match self {
            BenchSource::Live(c) => c,
            BenchSource::Recording(r, _) => r.as_ref().expect("recording source taken early"),
            BenchSource::Replay(r) => r,
        }
    }

    /// On a live or recording arm this is HTTP requests sent, including failover retries. On replay
    /// it is tape reads served - the same question ("how many times did the code ask?"), minus the
    /// wire. `BenchReport.replayed` says which, so nobody has to infer it from a low number.
    fn requests(&self) -> u64 {
        match self {
            BenchSource::Live(c) => c.request_count(),
            BenchSource::Recording(r, _) => r
                .as_ref()
                .map(|x| x.inner().request_count())
                .unwrap_or_default(),
            BenchSource::Replay(r) => r.reads(),
        }
    }

    fn content_address(&self) -> Option<String> {
        match self {
            BenchSource::Replay(r) => r.content_address(),
            _ => None,
        }
    }
}

// Matching the rest of this file and `indexer.rs`: this is a harness entrypoint threading one run's
// full configuration, and bundling it into a struct would move the argument list rather than shorten
// it (#657 made the same call for `resolve_calls_for_window`).
#[allow(clippy::too_many_arguments)]
async fn one_run(
    rpc_urls: &[String],
    registry: &DecodeRegistry,
    factory: Option<&FactorySet>,
    addresses: &[String],
    topic0s: &[String],
    calls: &[crate::calls::CallDecl],
    state_rpc: Option<&RpcClient>,
    chain_id: u64,
    from: u64,
    to: u64,
    window: u64,
    seal_direct: bool,
    window_adaptive: bool,
    concurrency: usize,
    run: usize,
    keep: Option<&std::path::Path>,
    tape: &TapeMode<'_>,
    publish: Option<(&std::path::Path, &str)>,
) -> Result<Run> {
    let bench_source = match tape {
        TapeMode::Live => BenchSource::Live(RpcClient::new(rpc_urls.to_vec())?),
        TapeMode::Record(path) => BenchSource::Recording(
            Some(crate::tape::RecordingSource::new(RpcClient::new(
                rpc_urls.to_vec(),
            )?)),
            path.to_path_buf(),
        ),
        TapeMode::Replay(path) => BenchSource::Replay(crate::tape::ReplaySource::open(path)?),
    };
    let source = bench_source.as_source();
    // **Seal on the rule production seals on** (#1199). A harness that cuts segments differently
    // from `dev` measures a cadence nobody runs, and this one is a CI gate. Looked up by id because
    // that is what the harness threads; an unregistered chain gets the same default `prepare` uses.
    // On a dense bench contract the row threshold is reached long before the span, so `seal_cut`
    // takes the row cut and this changes nothing - which is the point of checking rather than
    // assuming.
    let seal_span = crate::chains::lookup_by_id(chain_id)
        .map(|c| c.seal_span)
        .unwrap_or(crate::chains::DEFAULT_SEAL_SPAN);
    // Normally a throwaway work dir per run (redb and/or Parquet segments) - never the nest's own
    // database. `--keep` points it somewhere durable instead, because a case whose criterion is a
    // ROW COUNT cannot be answered by a harness that deletes its rows: OBIB case 3 asks for 100,001
    // block records, and until this existed the only way to produce them threw them away
    // (RFC-0036 §5.1). Every run clears and rewrites the directory, so what survives is the last
    // run's data - stated because a median over 3 runs and a row count from 1 are different things.
    let work = match keep {
        Some(p) => p.to_path_buf(),
        None => default_bench_work(run),
    };
    let _ = std::fs::remove_dir_all(&work);
    std::fs::create_dir_all(&work)?;
    // The mirror names its prefix from the nest's inputs, so they are copied beside the data. Each
    // run publishes under its own prefix, or run 2 would find run 1's objects and upload nothing.
    let publisher = match publish {
        Some((nest, target)) => {
            for src in crate::blob::collect_files(nest, None)? {
                let dst = work.join(src.strip_prefix(nest)?);
                if let Some(parent) = dst.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::copy(&src, &dst)?;
            }
            let name = format!("bench-publish-{}-{run}", std::process::id());
            let settings = crate::publish::Settings {
                target: format!("{}/{name}", target.trim_end_matches('/')),
                interval: std::time::Duration::from_secs(60),
                parallelism: crate::publish::DEFAULT_PARALLELISM,
            };
            Some((
                crate::publish::spawn(work.clone(), name.clone(), settings)?,
                name,
            ))
        }
        None => None,
    };

    let rss = RssSampler::start();
    let start = Instant::now();
    // A fresh child registry per run: discovery is part of what the run measures, so carrying one
    // across runs would make run 2 cheaper than run 1 and the median a blend of two workloads.
    let mut children = ChildRegistry::new();
    // The hot arm writes call rows to redb rather than to a Parquet manifest, so it has to hand its
    // resolved count back directly - see `calls_resolved` below.
    let mut hot_calls_resolved: Option<u64> = None;
    let events = match choose_path(factory.is_some(), seal_direct, concurrency) {
        // Sequential two-pass factory backfill - the same path `dev` takes (RFC-0009 §3). Pass 2
        // fetches the children pass 1 discovered, so it cannot be pipelined against itself.
        BackfillPath::Factory => {
            let fs = factory.expect("choose_path returned Factory without a FactorySet");
            crate::indexer::backfill_direct_factory(
                source,
                registry,
                fs,
                &mut children,
                &work,
                topic0s,
                calls,
                state_rpc,
                chain_id,
                from,
                to,
                window,
                seal_span,
                fs.force_topic0(),
                |_| Ok(()),
                |_, _, _| {},
            )
            .await?
        }
        // Pipelined seal-direct: concurrent fetch, in-order deterministic sealing. **This is the
        // production seal-direct path** - `nuthatch dev` on a static nest reaches
        // `backfill_direct_pipelined` regardless of concurrency; the branch above it selects on
        // `factory.is_some()`. Benching `Direct` instead measures a path only the bench takes.
        BackfillPath::Pipelined => {
            crate::indexer::backfill_direct_pipelined(
                source,
                registry,
                &work,
                addresses,
                topic0s,
                calls,
                state_rpc,
                chain_id,
                from,
                to,
                window,
                seal_span,
                concurrency,
                |_| Ok(()),   // bench doesn't persist a resume watermark
                |_, _, _| {}, // bench doesn't render progress
            )
            .await?
        }
        // Sequential seal-direct: decode → Parquet, bypassing the hot store. Reachable only from the
        // bench (`backfill_direct` has no other non-test caller) - it is the sequential control for
        // `Pipelined`, not what production runs. `window_adaptive` reaches it here, decoupled from
        // `seal_direct`, which is what #744 asked for: this is the one place in the whole binary
        // where a caller can ask for the seal-direct storage path with a *fixed* fetch window.
        BackfillPath::Direct => {
            crate::indexer::backfill_direct(
                source,
                registry,
                &work,
                addresses,
                topic0s,
                calls,
                state_rpc,
                chain_id,
                from,
                to,
                window,
                seal_span,
                window_adaptive,
            )
            .await?
        }
        BackfillPath::HotStore => {
            let (events, resolved) = hot_store_backfill(
                source,
                registry,
                &work,
                addresses,
                topic0s,
                calls,
                state_rpc,
                chain_id,
                from,
                to,
                window,
                window_adaptive,
            )
            .await?;
            hot_calls_resolved = Some(resolved);
            events
        }
    };
    let wall_clock_s = start.elapsed().as_secs_f64();
    let peak_rss_mb = rss.stop();
    let published_bytes = publisher.map(|(publisher, name)| {
        let bytes = crate::metrics::METRICS.nest(&name).publish_bytes();
        drop(publisher);
        bytes
    });
    // Read before the directory is torn down below: `calls_resolved` is the count of rows sealed
    // under each declared call's own table (#725) - the seal-direct paths write those to Parquet the
    // same as event rows, so the manifest already has the number, no new bookkeeping needed. A nest
    // with no `[[calls]]` reads 0 here because `load_manifest` on a directory with no manifest yet
    // returns an empty catalogue. The hot arm resolves the same calls (#743) but commits them to
    // redb, where there is no manifest to count, so it returns its own count instead - the number
    // means the same thing on both arms, which is the point of #743.
    let calls_resolved: u64 = if calls.is_empty() {
        0
    } else if let Some(n) = hot_calls_resolved {
        n
    } else {
        let manifest = crate::seal::load_manifest(&work)?;
        calls
            .iter()
            .filter_map(|c| manifest.tables.get(&c.name))
            .flat_map(|segs| segs.iter())
            .map(|s| s.rows as u64)
            .sum()
    };
    // Keep the data when asked. Without this the flag would create the directory, fill it, and then
    // delete it — which is the whole defect it exists to fix, just relocated.
    if keep.is_none() {
        let _ = std::fs::remove_dir_all(&work);
    }

    // Count before flushing: writing the tape consumes the recorder.
    let rpc_requests = bench_source.requests();
    let fixture_content_address = match bench_source {
        BenchSource::Recording(mut r, path) => {
            let mut tape = r.take().expect("recording source flushed twice").into_tape(
                crate::tape::Manifest {
                    chain: Some(chain_id.to_string()),
                    provider: rpc_urls.first().and_then(|u| provider_of(u)),
                    from_block: Some(from),
                    to_block: Some(to),
                    recorded_at: Some({
                        // Same shape `lists.rs` uses for its content-addressed snapshots: no date
                        // crate, and a value that cannot drift with a locale.
                        use std::time::{SystemTime, UNIX_EPOCH};
                        let secs = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map(|d| d.as_secs())
                            .unwrap_or(0);
                        format!("unix:{secs}")
                    }),
                    commit: option_env!("NUTHATCH_COMMIT").map(str::to_string),
                    content_address: None,
                },
            );
            tape.write_dir(&path)
                .with_context(|| format!("writing tape to {}", path.display()))?;
            tape.manifest.content_address.clone()
        }
        other => other.content_address(),
    };

    Ok(Run {
        events,
        wall_clock_s,
        events_per_sec: if wall_clock_s > 0.0 {
            events as f64 / wall_clock_s
        } else {
            0.0
        },
        peak_rss_mb,
        rpc_requests,
        fixture_content_address,
        children: children.len() as u64,
        calls_resolved,
        published_bytes,
    })
}

/// The baseline path: decode → redb hot store, with the same batched `block_timestamp` fetch the
/// live `dev` loop does - and, since #743, the same tier-3 `[[calls]]` resolution - so the only
/// thing that differs from seal-direct is the storage write.
///
/// That sentence was false for one release. #725 taught the three seal-direct arms to resolve
/// declared `[[calls]]`; this arm, the one `bench backfill` takes with no path flag, took no
/// `calls` parameter at all, so for a nest declaring calls it measured a workload `nuthatch dev`
/// does not run - and it is the *denominator* of both published multipliers, so the ratio would
/// have flattered seal-direct by exactly the work this arm skipped (#743).
///
/// `adaptive` is the "hot-adaptive" control arm added for #744: `nuthatch dev`'s hot-store tip loop
/// never adapts (it runs one fixed-width fetch per finalized window, not a from-history backfill),
/// so this exists only to isolate the fetch-window controller's own contribution from the
/// storage-path delta - never to change what production's hot path does. When `false` (the
/// existing, and still the default, behaviour) `window` is held fixed for the whole run.
///
/// Returns `(rows committed, call rows among them)`. The call count is returned rather than read
/// back, because redb has no manifest for `one_run` to count the way it counts sealed segments.
#[allow(clippy::too_many_arguments)]
async fn hot_store_backfill(
    source: &dyn crate::source::Source,
    registry: &DecodeRegistry,
    dir: &std::path::Path,
    addresses: &[String],
    topic0s: &[String],
    calls: &[crate::calls::CallDecl],
    state_rpc: Option<&RpcClient>,
    chain_id: u64,
    from: u64,
    to: u64,
    window: u64,
    adaptive: bool,
) -> Result<(u64, u64)> {
    // `DB_FILE`, not a bench-specific name: with `--keep` this directory is meant to be opened by
    // `nuthatch sql --dir <path>`, which looks for the standard store. A private filename would
    // persist the data and still leave it unqueryable, which is the failure this flag exists to fix.
    let store = Store::open(&dir.join(crate::config::DB_FILE))?;
    let mut events = 0u64;
    let mut call_rows_total = 0u64;
    let mut next = from;
    // `None` in fixed mode: the width then never moves off `window`. Same controller and same
    // per-workload choice `backfill_direct` makes (a blocks nest pays per block, not per log).
    let mut chunker = adaptive.then(|| {
        if registry.blocks() {
            crate::chunker::AdaptiveWindow::for_window_with_headers(window)
        } else {
            crate::chunker::AdaptiveWindow::for_window(window)
        }
    });
    while next <= to {
        let chunk_to = (next
            + chunker
                .as_ref()
                .map_or(window, crate::chunker::AdaptiveWindow::window)
            - 1)
        .min(to);
        // Same rule as `indexer.rs`: an empty address AND topic filter means *every log on the
        // chain*, not none. A blocks-only nest has neither, and no log could decode without them.
        // The rule is now carried by `LogFilter` rather than restated here (#432).
        let logs = match crate::source::LogFilter::new(addresses, topic0s) {
            None => Vec::new(),
            Some(filter) => match source.logs(&filter, next, chunk_to).await {
                Ok(logs) => {
                    if let Some(c) = &mut chunker {
                        c.observed(logs.len() as u64);
                    }
                    logs
                }
                // Only reachable in adaptive mode: fixed mode has no chunker to shrink, so a cap
                // falls straight through to the final arm and the run fails loudly - the same
                // trade `backfill_direct` makes for its own fixed mode.
                Err(e) if chunker.is_some() && crate::chunker::is_result_too_large(&e) => {
                    if next >= chunk_to {
                        bail!(
                            "block {next} alone exceeds the provider's getLogs result cap - use a \
                             provider with a higher/no cap"
                        );
                    }
                    chunker
                        .as_mut()
                        .expect("checked chunker.is_some() above")
                        .too_large();
                    continue;
                }
                Err(e) => return Err(e).with_context(|| format!("getLogs {next}..={chunk_to}")),
            },
        };
        let mut rows: Vec<_> = logs
            .iter()
            .filter_map(|log| registry.decode(log).ok().flatten())
            .collect();
        // RFC-0036 §4.2, mirroring `indexer.rs`: a `blocks` nest must cover blocks that emitted
        // nothing, so the rows are enumerated from the WINDOW, not from `logs`. Deriving them from
        // logs is what produced "zero rows and a green run" - the exact failure the indexer's own
        // comment warns about, which this harness reimplemented rather than shared.
        if registry.blocks() {
            let want: Vec<u64> = (next..=chunk_to).collect();
            let headers = source.block_headers(&want).await?;
            let mut block_rows: Vec<_> = want
                .iter()
                .filter_map(|b| {
                    headers
                        .get(b)
                        .and_then(|h| crate::registry::block_row(*b, h, registry.timestamps()))
                })
                .collect();
            block_rows.sort_by_key(|r| r.block_number);
            rows.append(&mut block_rows);
        }
        let mut blocks: Vec<u64> = rows.iter().map(|r| r.block_number).collect();
        // Include the blocks a sampled `[[calls]]` declaration fires at, exactly as the seal-direct
        // paths do: a sampled call can land on a block that emitted no matching log, and a call row
        // with `block_timestamp: 0` is not the row `dev` would have stored.
        if state_rpc.is_some() {
            for d in calls {
                blocks.extend(d.blocks_in(next, chunk_to));
            }
        }
        blocks.sort_unstable();
        blocks.dedup();
        // **Propagate, never `unwrap_or_default()`** (#784). That call collapsed a provider error into
        // an empty map, every row took `block_timestamp = 0`, and the run reported success: on a
        // 120-block USDC range against the bundled public endpoints, five of six timestamp batches
        // 429'd and **9,115 of 11,758 rows (77.5%) were written dated 1970**. `count(block_timestamp)`
        // counts a zero, so the damage read as healthy.
        //
        // This does what the production paths already do rather than inventing a third behaviour:
        // `backfill_direct*` propagates, and `process_window` (`indexer.rs`) declines to advance the
        // cursor and re-fetches the window - it will not seal a fabricated timestamp (finding H4).
        // Third time this harness has been caught measuring something other than what it claims,
        // after #224's strawman and #725's empty `[[calls]]`, and `--keep` means its rows are a
        // surface someone reads.
        //
        // An `Ok` empty map stays legitimate: `block_timestamps` is documented best-effort, and a
        // nest that does not index timestamps never reaches here. Only a genuine failure stops the run.
        let ts = source
            .block_timestamps(&blocks)
            .await
            .with_context(|| format!("block timestamps for {next}..={chunk_to}"))?;
        for r in &mut rows {
            r.block_timestamp = ts.get(&r.block_number).copied().unwrap_or(0);
        }
        // RFC-0023 tier 3, shared with the seal-direct arms rather than reimplemented (#743): the
        // resolution loop lives in `indexer.rs` and both arms call it, so this harness cannot drift
        // from the path it is meant to be measuring for a third time.
        let mut call_rows = Vec::new();
        if let Some(rpc) = state_rpc {
            call_rows = crate::indexer::resolve_calls_for_window(
                source,
                calls,
                rpc,
                chain_id,
                &rows,
                next,
                chunk_to,
                &ts,
                registry.timestamps(),
                None,
            )
            .await?;
        }
        // **One commit per window, as the real path does** (RFC-0029 §4b).
        //
        // This used to call `put_entity` per row, which is one redb write transaction - and one fsync -
        // per row. `store.rs` PERF-2 records that exact shape as the thing that capped tip-follow
        // throughput far below the decode rate, and why `commit_window` exists. So the benchmark was
        // measuring a path the indexer deliberately does not use, and every events/sec figure it has
        // ever produced was a floor set by fsync rather than by nuthatch.
        //
        // **Any improvement from this change is a harness correction, never a product gain.** Recording
        // it as a speedup would be claiming credit for fixing our own measurement.
        //
        // Call rows go into the *same* commit as the event rows, which is what `process_window`
        // does: two commits per window would measure a write pattern production does not have.
        let batch: Vec<(String, String)> = rows
            .iter()
            .chain(call_rows.iter())
            .map(|r| {
                (
                    Store::entity_key(r.block_number, r.log_index),
                    r.to_json().to_string(),
                )
            })
            .collect();
        events += batch.len() as u64;
        call_rows_total += call_rows.len() as u64;
        store.commit_window(&batch, None, chunk_to)?;
        next = chunk_to + 1;
    }
    Ok((events, call_rows_total))
}

/// Samples this process's resident set size on a background thread, tracking the peak.
struct RssSampler {
    stop: Arc<AtomicBool>,
    peak_kb: Arc<AtomicU64>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl RssSampler {
    fn start() -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let peak_kb = Arc::new(AtomicU64::new(0));
        let (s, p) = (stop.clone(), peak_kb.clone());
        let handle = std::thread::spawn(move || {
            while !s.load(Ordering::Relaxed) {
                if let Some(kb) = current_rss_kb() {
                    p.fetch_max(kb, Ordering::Relaxed);
                }
                std::thread::sleep(Duration::from_millis(120));
            }
        });
        Self {
            stop,
            peak_kb,
            handle: Some(handle),
        }
    }

    /// Stop sampling and return the peak RSS in MB.
    fn stop(mut self) -> u64 {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
        self.peak_kb.load(Ordering::Relaxed) / 1024
    }
}

/// This process's RSS in KB. Linux via `/proc/self/status`; else `ps` (macOS/BSD). None if neither.
fn current_rss_kb() -> Option<u64> {
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                return rest.trim().trim_end_matches("kB").trim().parse().ok();
            }
        }
    }
    let pid = std::process::id().to_string();
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid])
        .output()
        .ok()?;
    String::from_utf8_lossy(&out.stdout).trim().parse().ok()
}

fn median_f64(vals: impl Iterator<Item = f64>) -> f64 {
    let mut v: Vec<f64> = vals.collect();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    median_of_sorted(&v).unwrap_or(0.0)
}

fn median_u64(vals: impl Iterator<Item = u64>) -> u64 {
    let mut v: Vec<f64> = vals.map(|x| x as f64).collect();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    median_of_sorted(&v).unwrap_or(0.0).round() as u64
}

fn median_of_sorted(v: &[f64]) -> Option<f64> {
    if v.is_empty() {
        return None;
    }
    let mid = v.len() / 2;
    Some(if v.len().is_multiple_of(2) {
        (v[mid - 1] + v[mid]) / 2.0
    } else {
        v[mid]
    })
}

fn round2(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}

/// Nearest-rank percentile of a pre-sorted slice. `p` in `[0, 100]`. Empty slice → 0.
fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = (p / 100.0 * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

/// The short commit the bench ran at (provenance for the report), or None outside a git checkout.
/// Throwaway bench store on real disk (#781). `$XDG_CACHE_HOME` or `$HOME/.cache`, never `temp_dir()`
/// - on the box that publishes numbers `/tmp` is tmpfs and the storage comparison was RAM vs RAM.
fn default_bench_work(run: usize) -> std::path::PathBuf {
    cache_root().join(format!("nuthatch-bench-{}-{run}", std::process::id()))
}

fn cache_root() -> std::path::PathBuf {
    if let Some(xdg) = std::env::var_os("XDG_CACHE_HOME") {
        return std::path::PathBuf::from(xdg);
    }
    if let Some(home) = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE")) {
        return std::path::PathBuf::from(home).join(".cache");
    }
    std::path::PathBuf::from(".cache")
}

fn work_backing_of(path: &std::path::Path) -> String {
    if is_tmpfs(path) {
        "tmpfs".into()
    } else {
        "disk".into()
    }
}

fn is_tmpfs(path: &std::path::Path) -> bool {
    #[cfg(target_os = "linux")]
    {
        if let Ok(mounts) = std::fs::read_to_string("/proc/mounts") {
            let abs = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
            let abs_s = abs.to_string_lossy();
            let mut best: Option<(&str, usize)> = None;
            for line in mounts.lines() {
                let mut bits = line.split_whitespace();
                let _dev = bits.next();
                let Some(mnt) = bits.next() else { continue };
                let Some(fstype) = bits.next() else { continue };
                let covered = if mnt == "/" {
                    abs_s.starts_with('/')
                } else {
                    abs_s == mnt || abs_s.starts_with(&format!("{mnt}/"))
                };
                if covered && best.is_none_or(|(_, n)| mnt.len() >= n) {
                    best = Some((fstype, mnt.len()));
                }
            }
            if best.is_some_and(|(t, _)| t == "tmpfs" || t == "ramfs") {
                return true;
            }
        }
    }
    path.starts_with(std::env::temp_dir())
}

fn git_commit() -> Option<String> {
    let out = std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (!s.is_empty()).then_some(s)
}

#[cfg(test)]
mod tests {

    /// A benchmark report gets pasted into issues and PRs. An API key in one is a credential leak, and
    /// providers put keys in the path as often as in a header.
    #[test]
    fn the_provider_field_carries_a_host_and_never_a_key() {
        assert_eq!(
            provider_of("https://arb-mainnet.g.alchemy.com/v2/SECRETKEY123"),
            Some("arb-mainnet.g.alchemy.com".into())
        );
        assert_eq!(
            provider_of("https://arb1.arbitrum.io/rpc"),
            Some("arb1.arbitrum.io".into())
        );
        // Whatever it returns, it must never contain the key.
        assert!(!provider_of("https://x.example/v2/SECRETKEY123")
            .unwrap()
            .contains("SECRETKEY123"));
        assert_eq!(provider_of("not-a-url"), None);
        // Basic-auth userinfo and a path-less query key are credentials too (#2206).
        assert_eq!(
            provider_of("https://user:pass@nd-1.p2pify.com/"),
            Some("nd-1.p2pify.com".into())
        );
        assert_eq!(
            provider_of("https://host.example?apikey=K"),
            Some("host.example".into())
        );
    }

    #[test]
    fn a_transfer_only_nest_and_an_all_events_nest_are_not_the_same_artefact() {
        let dir = tempfile::tempdir().unwrap();
        nest_config(dir.path());
        let all = crate::config::Config::load(dir.path()).unwrap();
        let mut transfer_only = crate::config::Config::load(dir.path()).unwrap();
        transfer_only.contracts[0].events = vec!["Transfer".into()];
        let a = events_declared_of(&all);
        let b = events_declared_of(&transfer_only);
        assert!(
            a.is_empty(),
            "an undeclared nest is the all-events nest, got {a:?}"
        );
        assert_eq!(b, vec!["Transfer".to_string()]);
        let sa = serde_json::to_string(&a).unwrap();
        let sb = serde_json::to_string(&b).unwrap();
        assert_ne!(
            sa, sb,
            "the two event sets must not serialize identically - that is the 11,758 vs 12,933 day"
        );
    }

    #[test]
    fn a_replayed_report_names_the_tape_host_not_the_nest_pool() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("entries.jsonl"),
            "{\"key\":\"tip\",\"outcomes\":[{\"outcome\":\"ok\",\"value\":1}]}\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("manifest.json"),
            r#"{"provider":"recorded.example"}"#,
        )
        .unwrap();
        let got = report_provider(
            &TapeMode::Replay(dir.path()),
            &["https://nest.example/v2/SECRET".to_string()],
        );
        assert_eq!(got.as_deref(), Some("recorded.example"));
    }

    /// #748: the `[[calls]]` summary line is the only place in this file an endpoint URL reached
    /// stdout without going through `provider_of` first. Assert on the rendered line, not the
    /// flag - a test that only checks the host is present would pass with the leak still in.
    #[test]
    fn calls_summary_line_carries_a_host_and_never_a_key() {
        let line = calls_summary_line(3, &["https://example.invalid/v2/deadbeefkey".to_string()]);
        assert!(line.contains("example.invalid"), "got {line}");
        assert!(!line.contains("deadbeefkey"), "got {line}");
    }

    /// An invented hardware string is worse than an absent one: it looks authoritative and is not.
    #[test]
    fn hardware_is_reported_or_omitted_never_guessed() {
        let h = hardware_summary().expect("this platform reports core count");
        assert!(h.contains("cores"), "got {h}");
    }

    /// #781: the default work dir must not be `temp_dir()`. On the publishing box that is tmpfs.
    #[test]
    fn default_bench_work_is_not_under_temp_dir() {
        let p = default_bench_work(0);
        let tmp = std::env::temp_dir();
        assert!(
            !p.starts_with(&tmp),
            "default work dir {p:?} is still under temp_dir {tmp:?}"
        );
        assert_eq!(work_backing_of(&tmp), "tmpfs");
    }
    use super::*;

    #[test]
    fn medians() {
        assert_eq!(median_u64([10u64, 30, 20].into_iter()), 20);
        assert_eq!(median_u64([10u64, 20, 30, 40].into_iter()), 25); // even → mean of middle two
        assert_eq!(median_u64(std::iter::empty()), 0);
        assert!((median_f64([1.0, 3.0, 2.0].into_iter()) - 2.0).abs() < 1e-9);
    }

    #[test]
    fn round_two_dp() {
        assert_eq!(round2(1234.5678), 1234.57);
    }

    #[test]
    fn percentiles_nearest_rank() {
        let v = [10.0, 20.0, 30.0, 40.0, 50.0]; // sorted, 5 elements
        assert_eq!(percentile(&v, 0.0), 10.0); // rank round(0)   = 0 → v[0]
        assert_eq!(percentile(&v, 50.0), 30.0); // rank round(2.0) = 2 → v[2]
        assert_eq!(percentile(&v, 99.0), 50.0); // rank round(3.96)= 4 → v[4]
        assert_eq!(percentile(&v, 100.0), 50.0); // rank round(4)  = 4 → v[4]
        assert_eq!(percentile(&[], 99.0), 0.0); // empty guard
        assert_eq!(percentile(&[42.0], 99.0), 42.0); // single element
    }

    /// **The regression this file exists to prevent.** A factory nest measured through any other path
    /// fetches a fixed address list that never grows, so it counts the factory's own creation events
    /// and none of the child activity the workload is *made of* - and it reports success. Found on
    /// OBIB case 6, where the plain path returned 232 events in 2.6 s against an expected 35,039.
    ///
    /// `--concurrency` must not be able to steer a factory nest off the factory path: the pipelined
    /// backfill takes no `ChildRegistry` and cannot discover anything.
    #[test]
    fn a_factory_nest_always_takes_the_factory_path() {
        for seal_direct in [true, false] {
            for concurrency in [1, 8, 16] {
                assert_eq!(
                    choose_path(true, seal_direct, concurrency),
                    BackfillPath::Factory,
                    "factory nest with seal_direct={seal_direct} concurrency={concurrency}"
                );
            }
        }
    }

    /// A report with the two numbers the gate reads and nothing else that matters.
    fn report(reads: usize, p50: f64, p99: f64) -> QueryBenchReport {
        QueryBenchReport {
            bench: "query",
            label: None,
            nest: "t".into(),
            chain: "mainnet".into(),
            hot_rows: 0,
            sealed_through: 0,
            reads,
            warm_cache: true,
            point_read_p50_us: p50,
            point_read_p99_us: p99,
            point_read_p999_us: p99,
            sql: "SELECT 1".into(),
            sql_iters: 1,
            sql_p50_ms: 0.0,
            sql_p99_ms: 0.0,
            sql_peak_rss_mb: 0,
            commit: None,
            hardware: None,
        }
    }

    /// The gate has to bite on each percentile independently, and only when asked. A ceiling nobody
    /// passed is not a ceiling of zero.
    #[test]
    fn a_point_read_ceiling_fails_the_run_and_only_when_asked() {
        let r = report(1000, 12.0, 90.0);
        // Nothing asked for → nothing enforced, however slow the run was.
        assert!(check_gate(&r, None, None, None).is_ok());
        // Under, and exactly on, the ceiling both pass; over fails.
        assert!(check_gate(&r, Some(12.0), Some(90.0), None).is_ok());
        assert!(check_gate(&r, Some(20.0), Some(100.0), None).is_ok());
        assert!(check_gate(&r, Some(11.9), None, None).is_err());
        assert!(check_gate(&r, None, Some(89.9), None).is_err());

        // Both breached → both named, so one CI run tells you everything rather than the first thing.
        let err = check_gate(&r, Some(1.0), Some(2.0), None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("p50"), "{err}");
        assert!(err.contains("p99"), "{err}");
    }

    /// **The false pass this gate would otherwise have.** An unindexed nest samples no keys, so
    /// `query` reports p50 = p99 = 0µs, and zero sits under every ceiling anyone would ever write.
    /// Without `--min-reads` the gate is green precisely when it measured nothing, which is how the
    /// footprint check used to false-pass before it asserted its row count.
    #[test]
    fn a_nest_that_measured_nothing_cannot_pass_the_gate() {
        let measured_nothing = report(0, 0.0, 0.0);
        // The trap: ceilings alone are satisfied by an empty run.
        assert!(check_gate(&measured_nothing, Some(50.0), Some(500.0), None).is_ok());
        // The floor is what refuses it.
        let err = check_gate(&measured_nothing, Some(50.0), Some(500.0), Some(1000))
            .unwrap_err()
            .to_string();
        assert!(err.contains("timed 0 point-read"), "{err}");
        // A partially-indexed nest is refused too - 12 reads is not a p99.
        assert!(check_gate(&report(12, 1.0, 1.0), None, None, Some(1000)).is_err());
        // And a full run clears it.
        assert!(check_gate(&report(1000, 1.0, 1.0), None, None, Some(1000)).is_ok());
    }

    /// An indexed nest on disk: config, ABI, and `rows` entities in the hot store.
    /// A nest directory with a config but no store - the state `bench query` must refuse rather
    /// than invent (#469).
    fn nest_config(dir: &std::path::Path) {
        std::fs::write(
            dir.join(crate::config::CONFIG_FILE),
            r#"[nest]
name = "t"
chain = "mainnet"
chain_id = 1
rpc_urls = ["https://x"]
schema_version = 1

[[contracts]]
alias = "c"
address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
abi = "abis/c.json"
"#,
        )
        .unwrap();
        std::fs::create_dir_all(dir.join("abis")).unwrap();
        std::fs::write(
            dir.join("abis/c.json"),
            r#"[{"type":"event","name":"Transfer","anonymous":false,"inputs":[{"name":"from","type":"address","indexed":true},{"name":"to","type":"address","indexed":true},{"name":"value","type":"uint256","indexed":false}]}]"#,
        )
        .unwrap();
    }

    fn indexed_nest(dir: &std::path::Path, rows: u64) {
        nest_config(dir);
        let store = Store::open(&dir.join(crate::config::DB_FILE)).unwrap();
        let batch: Vec<(String, String)> = (0..rows)
            .map(|i| {
                (
                    Store::entity_key(i + 1, 0),
                    format!(
                        r#"{{"table":"c__transfer","block_number":{},"log_index":0,"value":"1"}}"#,
                        i + 1
                    ),
                )
            })
            .collect();
        store.commit_window(&batch, None, rows).unwrap();
    }

    fn gated_args(
        dir: &std::path::Path,
        max_p50: Option<f64>,
        min_reads: Option<usize>,
    ) -> crate::cli::QueryBenchArgs {
        crate::cli::QueryBenchArgs {
            dir: dir.to_string_lossy().into_owned(),
            sql: None,
            reads: 1000,
            iters: 1,
            out: None,
            label: None,
            max_point_read_p50_us: max_p50,
            max_point_read_p99_us: None,
            min_reads,
        }
    }

    /// #413/#469: `Store::open` creates the file it is asked whether exists, so a probe against it
    /// answers its own question. Against a directory with no store, `bench query` must refuse rather
    /// than measure the empty store it just made - and must not leave that store behind either,
    /// which is the half a message-only assertion cannot see.
    #[test]
    fn bench_query_refuses_a_directory_with_no_store() {
        let dir = tempfile::tempdir().unwrap();
        nest_config(dir.path());
        let db = dir.path().join(crate::config::DB_FILE);
        assert!(!db.exists(), "fixture must start with no store");

        let err = query(gated_args(dir.path(), None, None))
            .expect_err("a directory with no store must not produce a baseline")
            .to_string();
        assert!(
            err.contains(&dir.path().display().to_string()),
            "the error must name the directory that has no store: {err}"
        );
        assert!(
            !db.exists(),
            "a refused probe must not leave a nuthatch.redb behind - that is the litter #413 was \
             about"
        );
    }

    /// **The wiring, not the mechanism.** `check_gate` can be correct and complete while `query`
    /// never calls it, which is the exact shape the board caught in PR #369: a parser with tests and
    /// a call site with none, deletable at 488 green. So this drives the real `bench query` against a
    /// real indexed nest on disk and asserts the process-level outcome - a breached ceiling must come
    /// back as `Err`, because `Ok` is what CI reads as a pass.
    #[test]
    fn bench_query_fails_when_a_ceiling_is_breached() {
        let dir = tempfile::tempdir().unwrap();
        indexed_nest(dir.path(), 64);

        // No ceiling: the same run must succeed, so the failure below is the gate and not the fixture.
        query(gated_args(dir.path(), None, None)).unwrap();

        // A ceiling no real point-read can meet. redb serves these from page cache in single-digit
        // microseconds; 0µs is unreachable by construction, so this asserts the gate, not the machine.
        let err = query(gated_args(dir.path(), Some(0.0), None))
            .expect_err("a breached ceiling must fail the run, not just print")
            .to_string();
        assert!(err.contains("point-read p50"), "{err}");

        // The re-baseline instruction has to name the machine that enforces the gate. The original
        // wording said "against a committed report", and the only committed report was a 32-core
        // dev-box one that docs/benchmarks.md says the ceiling was *not* set against - so whoever
        // followed the message re-derived a runner ceiling from the wrong hardware. Pinned here
        // because a FAIL message nobody reads until CI is red is exactly the text that rots.
        assert!(
            err.contains("machine that enforces it"),
            "the failure must say where to re-baseline, not just that one is needed: {err}"
        );
        // ...and it has to name the files, not just the principle. The assertion above stays green on
        // a message that says "measure it yourself" and nothing more, which leaves the sentences that
        // actually prevent the mistake deletable without this test noticing. Both halves are pinned
        // because #385 swapped which file is which: the runner artifact is now the reference, the
        // dev-box record is the one still sitting in the tree for a reader to reach for by mistake.
        assert!(
            err.contains("docs/bench/point-read.json"),
            "the failure must name the committed reference, since a reader who is told to \
             re-baseline will otherwise reach for whichever report they find first: {err}"
        );
        assert!(
            err.contains("Not from docs/bench/point-read-devbox.json"),
            "the failure must name the report that is *not* the baseline, since that is the one a \
             reader reaches for otherwise: {err}"
        );
        // The instruction that survives a future file move is the hardware check, not either
        // filename - the dev-box artifact was wrong for exactly one reason and it is this one.
        assert!(
            err.contains("`hardware` field"),
            "the failure must tell the reader to check the machine the reference was measured on, \
             which is the only thing that made the old reference wrong: {err}"
        );

        // The p99 flag still reaches the gate. CI stopped passing --max-point-read-p99-us when the
        // 150µs default was dropped, so nothing else drives this argument end to end any more, and a
        // deliberately-retained escape hatch with no coverage is the shape that rots unnoticed.
        // `check_gate`'s own p99 branch is unit-tested above; what this pins is the CLI wiring into
        // it, which is the half that went quiet.
        let mut p99_args = gated_args(dir.path(), None, None);
        p99_args.max_point_read_p99_us = Some(0.0);
        let err = query(p99_args)
            .expect_err("--max-point-read-p99-us must still gate when an operator asks for it")
            .to_string();
        assert!(err.contains("point-read p99"), "{err}");

        // And the floor is wired too: 64 rows cannot satisfy --min-reads 1000.
        let err = query(gated_args(dir.path(), None, Some(1000)))
            .expect_err("--min-reads must fail a nest that measured too little")
            .to_string();
        assert!(err.contains("timed 64 point-read"), "{err}");
    }

    /// The report is the artifact, so it carries its own provenance. `hardware` was on the write-path
    /// report and missing here, and a point-read latency without the machine it was measured on is
    /// not comparable to the next release's number - which is the whole point of #283.
    #[test]
    fn the_query_report_carries_its_provenance() {
        let dir = tempfile::tempdir().unwrap();
        indexed_nest(dir.path(), 8);
        let out = dir.path().join("report.json");
        let mut args = gated_args(dir.path(), None, None);
        args.out = Some(out.to_string_lossy().into_owned());
        query(args).unwrap();

        let json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&out).unwrap()).unwrap();
        assert!(
            json["hardware"]
                .as_str()
                .is_some_and(|h| h.contains("cores")),
            "report must name the hardware: {json}"
        );
        assert_eq!(json["reads"], 8);
        assert!(json["point_read_p50_us"].as_f64().unwrap() > 0.0);

        // **The committed baseline is this file, byte for byte** (#385). `docs/bench/point-read.json`
        // is lifted from a green `main` run of the `point-read latency` job rather than hand-written,
        // so anything the writer does that a committed file cannot have - here, ending at `}` with no
        // final newline - forces whoever commits it to edit the artifact, at which point it stops
        // being one. Asserted on the bytes rather than the parse, because every JSON reader in the
        // world is indifferent to the thing this pins.
        let raw = std::fs::read_to_string(&out).unwrap();
        assert!(
            raw.ends_with("}\n"),
            "a committed artifact needs a trailing newline: {:?}",
            &raw[raw.len().saturating_sub(8)..]
        );
    }

    /// **`--keep` wipes what it is given, so the guard is the only thing between a typo and a
    /// deleted nest.** The refusal has to fire on the *presence of `nuthatch.toml`*, not on
    /// anything about the run, because by the time a run has started the directory is already
    /// cleared. Absent this test the guard was reachable only through a live-RPC backfill, so
    /// nothing would have noticed it being weakened or dropped.
    #[test]
    fn keep_refuses_a_nest_directory_and_accepts_anything_else() {
        let dir = tempfile::tempdir().unwrap();

        // Not asked for: no path, and no error.
        assert!(resolve_keep(&None).unwrap().is_none());

        // A bare directory is fine - this is the intended use, a dedicated scratch path.
        let scratch = dir.path().join("scratch");
        std::fs::create_dir_all(&scratch).unwrap();
        assert_eq!(
            resolve_keep(&Some(scratch.to_string_lossy().into_owned())).unwrap(),
            Some(scratch.clone())
        );

        // So is a path that does not exist yet: the run creates it.
        let fresh = dir.path().join("not-yet");
        assert_eq!(
            resolve_keep(&Some(fresh.to_string_lossy().into_owned())).unwrap(),
            Some(fresh)
        );

        // A directory holding a nuthatch.toml is refused, and the message says where to point
        // instead - the one thing someone who has just hit this needs to know.
        std::fs::write(scratch.join(crate::config::CONFIG_FILE), "[nest]\n").unwrap();
        let err = resolve_keep(&Some(scratch.to_string_lossy().into_owned()))
            .expect_err("--keep at a nest directory must be refused, not cleared")
            .to_string();
        assert!(err.contains("is a nest directory"), "{err}");
        assert!(err.contains(crate::config::CONFIG_FILE), "{err}");
    }

    #[test]
    fn a_non_factory_nest_dispatches_on_seal_direct_and_concurrency() {
        assert_eq!(choose_path(false, true, 8), BackfillPath::Pipelined);
        assert_eq!(choose_path(false, true, 1), BackfillPath::Direct);
        assert_eq!(choose_path(false, false, 8), BackfillPath::HotStore);
        assert_eq!(choose_path(false, false, 1), BackfillPath::HotStore);
    }

    /// #744: `BenchReport.window_adaptive` must report what the run actually did, not the flag it
    /// was asked for - the exact bug it replaces (`window_adaptive: args.seal_direct`) echoed a
    /// *different* flag regardless of what happened. `Direct` and `HotStore` take the request
    /// as-is, all four combinations reachable; `Pipelined` and `Factory` always adapt, the same as
    /// `nuthatch dev`, so a `--window-adaptive` absent there must still report `true`.
    /// #847 - the notice must be able to print. The condition it replaces was false in all eight
    /// rows, so this asserts both halves: it fires where the flag is inert, and stays quiet where
    /// the flag did something.
    #[test]
    fn the_no_effect_notice_fires_exactly_where_the_flag_is_inert() {
        // Passed, and the path would have adapted regardless - the operator should be told.
        assert!(flag_had_no_effect(BackfillPath::Factory, true));
        assert!(flag_had_no_effect(BackfillPath::Pipelined, true));
        // Passed, and it changed the run - nothing to say.
        assert!(!flag_had_no_effect(BackfillPath::Direct, true));
        assert!(!flag_had_no_effect(BackfillPath::HotStore, true));
        // Not passed - nothing to say on any path, including the ones that adapt anyway.
        for path in [
            BackfillPath::Factory,
            BackfillPath::Pipelined,
            BackfillPath::Direct,
            BackfillPath::HotStore,
        ] {
            assert!(
                !flag_had_no_effect(path, false),
                "{path:?} with the flag unset"
            );
        }
    }
    #[test]
    fn window_adaptive_reports_what_the_run_did_not_what_was_asked() {
        for requested in [false, true] {
            assert_eq!(
                effective_window_adaptive(BackfillPath::HotStore, requested),
                requested
            );
            assert_eq!(
                effective_window_adaptive(BackfillPath::Direct, requested),
                requested
            );
            assert!(effective_window_adaptive(
                BackfillPath::Pipelined,
                requested
            ));
            assert!(effective_window_adaptive(BackfillPath::Factory, requested));
        }
    }

    /// A stub that answers every `eth_getLogs` (batched or not) with an empty result - the shape
    /// that makes `AdaptiveWindow::observed(0)` grow the window 4× per step, so an adaptive and a
    /// fixed run diverge sharply in request count over the same dense range.
    async fn stub_empty_logs() -> (String, tokio::task::JoinHandle<()>) {
        use axum::{routing::post, Router};
        async fn handler(body: String) -> axum::Json<serde_json::Value> {
            let req: serde_json::Value =
                serde_json::from_str(&body).unwrap_or(serde_json::json!([]));
            let one = |r: &serde_json::Value| -> serde_json::Value {
                let id = r.get("id").cloned().unwrap_or(serde_json::json!(1));
                serde_json::json!({"jsonrpc": "2.0", "id": id, "result": serde_json::json!([])})
            };
            axum::Json(match req.as_array() {
                Some(rs) => serde_json::Value::Array(rs.iter().map(one).collect()),
                None => one(&req),
            })
        }
        let app = Router::new().route("/", post(handler));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{addr}/"), handle)
    }

    /// **#784: the harness must not fabricate a stored column.** A provider error on the timestamp
    /// batch used to become `unwrap_or_default()` - an empty map - so every row took
    /// `block_timestamp = 0` and the run reported success. On a real 120-block USDC range that was
    /// 9,115 of 11,758 rows dated 1970, and `count(block_timestamp)` counts a zero, so it read healthy.
    ///
    /// Driven through `one_run`'s real hot-store arm against a tape whose timestamp calls fail, so it
    /// asserts the disposition of the *bench path* rather than of a helper.
    #[tokio::test]
    async fn a_failed_timestamp_batch_stops_the_run_rather_than_storing_zeros() {
        let dir = tempfile::tempdir().unwrap();
        nest_config(dir.path());
        let config = Config::load(dir.path()).unwrap();
        let registry = crate::registry::from_nest(dir.path(), &config).unwrap();
        let addresses: Vec<String> = registry
            .addresses()
            .iter()
            .map(|a| format!("0x{}", hex::encode(a)))
            .collect();
        let topic0s: Vec<String> = registry
            .topic0s()
            .iter()
            .map(|t| format!("0x{}", hex::encode(t)))
            .collect();

        let (url, handle) = stub_empty_logs().await;
        let tape_dir = tempfile::tempdir().unwrap();
        let tape_path = tape_dir.path().join("tape");
        // `one_run`'s default work dir is keyed by pid and run index, so every test in this binary
        // sharing that index races for one redb (cargo runs the tests as threads of one process).
        // Passing `keep` is what makes this independent of the rest of the module.
        let record_dir = tempfile::tempdir().unwrap();
        one_run(
            &[url],
            &registry,
            None,
            &addresses,
            &topic0s,
            &[],
            None,
            1,
            1,
            200,
            20,
            false,
            false,
            1,
            1,
            Some(record_dir.path()),
            &TapeMode::Record(&tape_path),
            None,
        )
        .await
        .unwrap();
        handle.abort();

        // Rewrite every recorded timestamp outcome as the provider error we actually saw live.
        let entries = tape_path.join("entries.jsonl");
        let mut out = String::new();
        let mut rewrote = 0;
        for line in std::fs::read_to_string(&entries).unwrap().lines() {
            if line.contains("\"key\":\"block_timestamps:") {
                let key = line.split('"').nth(3).unwrap().to_string();
                out.push_str(&format!(
                    "{{\"key\":\"{key}\",\"outcomes\":[{{\"outcome\":\"err\",\"message\":\"HTTP 429 Too Many Requests\"}}]}}\n"
                ));
                rewrote += 1;
            } else {
                out.push_str(line);
                out.push('\n');
            }
        }
        assert!(
            rewrote > 0,
            "the recording must contain timestamp calls to rewrite"
        );
        std::fs::write(&entries, out).unwrap();
        // Drop the manifest: its address no longer matches, and that refusal is covered by its own
        // test. This one is about the timestamp disposition.
        std::fs::remove_file(tape_path.join("manifest.json")).unwrap();

        let kept = tempfile::tempdir().unwrap();
        let result = one_run(
            &["http://127.0.0.1:1/".to_string()],
            &registry,
            None,
            &addresses,
            &topic0s,
            &[],
            None,
            1,
            1,
            200,
            20,
            false,
            false,
            1,
            2,
            Some(kept.path()),
            &TapeMode::Replay(&tape_path),
            None,
        )
        .await;

        let err = match result {
            Ok(r) => panic!(
                "a run whose timestamps all failed must not report success - got {} events, which \
                 would be stored with block_timestamp = 0",
                r.events
            ),
            Err(e) => format!("{e:#}"),
        };
        assert!(
            err.contains("block timestamps"),
            "the failure must name what could not be fetched: {err}"
        );
    }

    /// **RFC-0039 end to end, through the real bench path.** Record a live run, then replay it with
    /// the server torn down - so a replayed run that produced the same events can only have got them
    /// from the tape.
    ///
    /// The teardown is the assertion. A replay that still worked against a live stub would prove
    /// nothing about whether the tape was ever read.
    #[tokio::test]
    async fn a_recorded_run_replays_with_the_server_gone() {
        let dir = tempfile::tempdir().unwrap();
        nest_config(dir.path());
        let config = Config::load(dir.path()).unwrap();
        let registry = crate::registry::from_nest(dir.path(), &config).unwrap();
        let addresses: Vec<String> = registry
            .addresses()
            .iter()
            .map(|a| format!("0x{}", hex::encode(a)))
            .collect();
        let topic0s: Vec<String> = registry
            .topic0s()
            .iter()
            .map(|t| format!("0x{}", hex::encode(t)))
            .collect();

        let tape_dir = tempfile::tempdir().unwrap();
        let tape_path = tape_dir.path().join("tape");

        let (url, handle) = stub_empty_logs().await;
        // Same isolation as the timestamp-failure test: the default work dir is pid+run, shared
        // across every parallel test using that index.
        let recorded_dir = tempfile::tempdir().unwrap();
        let recorded = one_run(
            &[url],
            &registry,
            None,
            &addresses,
            &topic0s,
            &[],
            None,
            1,
            1,
            200,
            20,
            false,
            false,
            1,
            1,
            Some(recorded_dir.path()),
            &TapeMode::Record(&tape_path),
            None,
        )
        .await
        .unwrap();

        // The recording must have produced a content address, or nothing was written.
        let addr = recorded
            .fixture_content_address
            .clone()
            .expect("a recorded run must name the tape it wrote");
        assert_eq!(addr.len(), 64, "sha256 hex: {addr}");
        assert!(tape_path.join("entries.jsonl").is_file());
        assert!(tape_path.join("manifest.json").is_file());

        // Tear the network down. Anything the replay answers now came off the disk.
        handle.abort();

        let replayed_dir = tempfile::tempdir().unwrap();
        let replayed = one_run(
            &["http://127.0.0.1:1/".to_string()],
            &registry,
            None,
            &addresses,
            &topic0s,
            &[],
            None,
            1,
            1,
            200,
            20,
            false,
            false,
            1,
            2,
            Some(replayed_dir.path()),
            &TapeMode::Replay(&tape_path),
            None,
        )
        .await
        .unwrap();

        assert_eq!(
            replayed.events, recorded.events,
            "a replayed run must reproduce the recorded workload exactly"
        );
        assert_eq!(
            replayed.fixture_content_address.as_deref(),
            Some(addr.as_str()),
            "a replayed run names the bytes it came from"
        );

        // `rpc_requests` deliberately does NOT match across the two arms, and asserting that it did
        // was this test's first mistake. On the recording arm it counts HTTP requests `RpcClient`
        // sent; on replay it counts `Source` calls served. `RpcClient` batches several source calls
        // into one request, so the replay figure is legitimately the larger of the two (here 20
        // against 10). That is the semantic split `BenchReport.replayed` exists to declare - the
        // field keeps meaning "how many times did the code ask the source for something" and loses
        // only "how many bytes went over a wire".
        assert!(
            replayed.rpc_requests > 0,
            "a replayed run still counts what the code asked for"
        );

        // What *must* be identical is one replay against another: that is the entire deliverable.
        let again_dir = tempfile::tempdir().unwrap();
        let again = one_run(
            &["http://127.0.0.1:1/".to_string()],
            &registry,
            None,
            &addresses,
            &topic0s,
            &[],
            None,
            1,
            1,
            200,
            20,
            false,
            false,
            1,
            3,
            Some(again_dir.path()),
            &TapeMode::Replay(&tape_path),
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            again.events, replayed.events,
            "two replays must agree exactly"
        );
        assert_eq!(
            again.rpc_requests, replayed.rpc_requests,
            "two replays must ask the source for exactly the same things"
        );
    }

    /// **#744, the behaviour, not just the label.** `--window-adaptive` has to change what actually
    /// gets fetched, on both the hot-store and the seal-direct (`Direct`) path independently of one
    /// another - a flag that only flipped `BenchReport.window_adaptive` while fetching identically
    /// would be decorative. Over a 4,000-block entirely-empty range starting from a 20-block window,
    /// a fixed run issues one `getLogs` per 20 blocks (200 requests) while an adaptive run quadruples
    /// the window on every empty response and finishes in a handful. Asserted on `rpc_requests`,
    /// never on wall-clock, per this sprint's own rule for a heavily shared box.
    #[tokio::test]
    async fn window_adaptive_changes_request_count_independently_of_seal_direct() {
        let dir = tempfile::tempdir().unwrap();
        nest_config(dir.path());
        let config = Config::load(dir.path()).unwrap();
        let registry = crate::registry::from_nest(dir.path(), &config).unwrap();
        let addresses: Vec<String> = registry
            .addresses()
            .iter()
            .map(|a| format!("0x{}", hex::encode(a)))
            .collect();
        let topic0s: Vec<String> = registry
            .topic0s()
            .iter()
            .map(|t| format!("0x{}", hex::encode(t)))
            .collect();

        for seal_direct in [false, true] {
            let (url_fixed, h_fixed) = stub_empty_logs().await;
            let fixed_dir = tempfile::tempdir().unwrap();
            let fixed = one_run(
                &[url_fixed],
                &registry,
                None,
                &addresses,
                &topic0s,
                &[],
                None,
                1,
                1,
                4_000,
                20,
                seal_direct,
                false, // window_adaptive
                1,
                1,
                Some(fixed_dir.path()),
                &TapeMode::Live,
                None,
            )
            .await
            .unwrap();
            h_fixed.abort();

            let (url_adaptive, h_adaptive) = stub_empty_logs().await;
            let adaptive_dir = tempfile::tempdir().unwrap();
            let adaptive = one_run(
                &[url_adaptive],
                &registry,
                None,
                &addresses,
                &topic0s,
                &[],
                None,
                1,
                1,
                4_000,
                20,
                seal_direct,
                true, // window_adaptive
                1,
                1,
                Some(adaptive_dir.path()),
                &TapeMode::Live,
                None,
            )
            .await
            .unwrap();
            h_adaptive.abort();

            assert!(
                adaptive.rpc_requests < fixed.rpc_requests / 10,
                "seal_direct={seal_direct}: adaptive ({}) must issue far fewer getLogs requests \
                 than fixed ({}) over the same dense empty range - if these are close, \
                 --window-adaptive did not reach the {} path",
                adaptive.rpc_requests,
                fixed.rpc_requests,
                if seal_direct { "Direct" } else { "HotStore" },
            );
        }
    }

    /// A nest declaring one sampled `[[calls]]` entry (`every = 1`), mirroring `indexer.rs`'s own
    /// `write_calls_nest` fixture - built through `nuthatch.toml`, not hand-assembled structs, so
    /// the test exercises the real `Config::load` + `from_nest` path `backfill()` itself takes.
    fn write_calls_nest(dir: &std::path::Path) {
        std::fs::create_dir_all(dir.join("abis")).unwrap();
        std::fs::write(
            dir.join(crate::config::CONFIG_FILE),
            "[nest]\nname = \"c\"\nchain = \"arbitrum-one\"\nchain_id = 42161\nrpc_urls = []\n\n\
             [[contracts]]\nalias = \"tok\"\naddress = \"0x1111111111111111111111111111111111111111\"\n\
             abi = \"abis/tok.json\"\n\n\
             [[calls]]\nname = \"oracle_answer\"\n\
             contract = \"0x2222222222222222222222222222222222222222\"\n\
             calldata = \"0x18160ddd\"\nevery = 1\n",
        )
        .unwrap();
        std::fs::write(
            dir.join("abis/tok.json"),
            r#"[{"type":"event","name":"Ping","inputs":[],"anonymous":false}]"#,
        )
        .unwrap();
    }

    /// A JSON-RPC stub answering every method `one_run`'s backfill paths need: empty `eth_getLogs`
    /// (the fixture has no events), a fixed `eth_call` result (RFC-0023 tier 3 reads), and a
    /// `eth_getBlockByNumber` header (timestamps, and the per-block hash fetch `resolve_calls_for_
    /// window` still makes pre-#720). One server serves both the ingestion and archive endpoints -
    /// nothing here cares which role asked.
    /// Two calls-bench tests stand up stub HTTP servers and a work dir. In a feature build they
    /// share a process with a larger sibling set, and a race on the stub becoming reachable showed
    /// up as `calls_resolved = 0` on one leg only (#789). They take this lock so they cannot share a
    /// server, a port, or a work dir with each other.
    static CALLS_BENCH_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    async fn stub_full_rpc() -> (String, tokio::task::JoinHandle<()>) {
        use axum::{routing::post, Router};
        async fn handler(body: String) -> axum::Json<serde_json::Value> {
            let req: serde_json::Value =
                serde_json::from_str(&body).unwrap_or(serde_json::json!([]));
            let one = |r: &serde_json::Value| -> serde_json::Value {
                let method = r.get("method").and_then(|m| m.as_str()).unwrap_or("");
                let id = r.get("id").cloned().unwrap_or(serde_json::json!(1));
                let result = match method {
                    "eth_getLogs" => serde_json::json!([]),
                    "eth_call" => serde_json::json!(format!("0x{:064x}", 42)),
                    "eth_getBlockByNumber" => {
                        let n = r
                            .get("params")
                            .and_then(|p| p.get(0))
                            .and_then(|b| b.as_str())
                            .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
                            .unwrap_or(0);
                        serde_json::json!({
                            "hash": format!("0x{n:064x}"),
                            "number": format!("0x{n:x}"),
                            "timestamp": format!("0x{:x}", 1_700_000_000 + n),
                            "transactions": [],
                        })
                    }
                    _ => serde_json::Value::Null,
                };
                serde_json::json!({"jsonrpc": "2.0", "id": id, "result": result})
            };
            axum::Json(match req.as_array() {
                Some(rs) => serde_json::Value::Array(rs.iter().map(one).collect()),
                None => one(&req),
            })
        }
        let app = Router::new()
            .route("/", post(handler))
            .route("/{*rest}", post(handler));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        // Bound is not the same as accepting. Under load the first POST can lose the race with
        // `serve` starting, and `calls_resolved = 0` looks like the harness measured a strawman.
        for _ in 0..50 {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        (format!("http://{addr}/"), handle)
    }

    /// #725: all three seal-direct bench call sites hardcoded an empty `[[calls]]` slice regardless
    /// of what the nest declared, so `nuthatch bench backfill --seal-direct` against a calls nest
    /// measured a nest with none - the same "harness measures a strawman" failure #224 found in
    /// this file three weeks earlier, for a different reason. Built the fixture through the real
    /// constructor (`Config::load` + `from_nest`), not hand-assembled structs, so the defect and
    /// the fix are both proven against what `backfill()` itself runs, not a stand-in for it.
    #[tokio::test]
    async fn seal_direct_bench_resolves_declared_calls() {
        let _serial = CALLS_BENCH_LOCK.lock().await;
        let dir = tempfile::tempdir().unwrap();
        write_calls_nest(dir.path());
        let config = Config::load(dir.path()).unwrap();
        let registry = crate::registry::from_nest(dir.path(), &config).unwrap();
        assert!(
            !config.calls.is_empty(),
            "control: the fixture must actually declare a call, or this test proves nothing"
        );
        let addresses: Vec<String> = registry
            .addresses()
            .iter()
            .map(|a| format!("0x{}", hex::encode(a)))
            .collect();
        let topic0s: Vec<String> = registry
            .topic0s()
            .iter()
            .map(|t| format!("0x{}", hex::encode(t)))
            .collect();

        let (main_url, main_handle) = stub_full_rpc().await;
        let (state_url, state_handle) = stub_full_rpc().await;
        let state_rpc = RpcClient::new(vec![state_url]).unwrap();

        // `every = 1` over blocks 1..=3 samples all three - a per-block hash fetch would be three
        // `block_hash` round trips (pre-#720), a batched one a single `block_headers` call. Either
        // way this test only cares whether the call rows made it to the manifest.
        let work = tempfile::tempdir().unwrap();
        let r = one_run(
            &[main_url],
            &registry,
            None,
            &addresses,
            &topic0s,
            &config.calls,
            Some(&state_rpc),
            config.nest.chain_id,
            1,
            3,
            10,
            true,  // seal_direct: the Direct path (concurrency 1)
            false, // window_adaptive: irrelevant to this test, fixed is the cheaper default
            1,
            1,
            Some(work.path()),
            &TapeMode::Live,
            None,
        )
        .await
        .unwrap();

        main_handle.abort();
        state_handle.abort();

        assert!(
            r.calls_resolved > 0,
            "seal-direct bench must resolve the nest's declared [[calls]] and record it on the \
             run, got calls_resolved = {} - #725 is that every seal-direct call site hardcoded an \
             empty calls slice regardless of what the nest declared",
            r.calls_resolved
        );
    }

    /// #743: #725 fixed the three seal-direct arms and left the fourth - `BackfillPath::HotStore`,
    /// the arm `bench backfill` takes with no path flag, and the *denominator* of both published
    /// multipliers. `hot_store_backfill` took no `calls` parameter at all, so against a calls nest
    /// the numerator paid tier-3 cost and the denominator did not.
    ///
    /// Asserted as an equality between the two arms rather than `hot > 0` alone: the defect is not
    /// that the hot number was zero, it is that the two arms measured different workloads, and only
    /// a comparison between them can see that. Both arms run against the same fixture and the same
    /// stub, so anything other than equality is one arm doing work the other skipped.
    #[tokio::test]
    async fn hot_store_bench_resolves_the_same_calls_as_seal_direct() {
        let _serial = CALLS_BENCH_LOCK.lock().await;
        let dir = tempfile::tempdir().unwrap();
        write_calls_nest(dir.path());
        let config = Config::load(dir.path()).unwrap();
        let registry = crate::registry::from_nest(dir.path(), &config).unwrap();
        assert!(
            !config.calls.is_empty(),
            "control: the fixture must actually declare a call, or this test proves nothing"
        );
        let addresses: Vec<String> = registry
            .addresses()
            .iter()
            .map(|a| format!("0x{}", hex::encode(a)))
            .collect();
        let topic0s: Vec<String> = registry
            .topic0s()
            .iter()
            .map(|t| format!("0x{}", hex::encode(t)))
            .collect();

        let (main_url, main_handle) = stub_full_rpc().await;
        let (state_url, state_handle) = stub_full_rpc().await;
        let state_rpc = RpcClient::new(vec![state_url]).unwrap();

        // An explicit work directory per arm, rather than `one_run`'s default. That default is
        // keyed by run *index*, so it is shared by every test in this binary using that index - two
        // in flight at once (cargo runs the tests as threads of one process) clear each other's work
        // dir mid-run. Passing `keep` is what makes these two arms independent of the rest of the
        // module.
        let hot_dir = tempfile::tempdir().unwrap();
        let sealed_dir = tempfile::tempdir().unwrap();
        let run_arm = |seal_direct: bool, work: &std::path::Path| {
            let (main_url, addresses, topic0s, registry, state_rpc) = (
                main_url.clone(),
                addresses.clone(),
                topic0s.clone(),
                &registry,
                &state_rpc,
            );
            let calls = config.calls.clone();
            let chain_id = config.nest.chain_id;
            let work = work.to_path_buf();
            async move {
                one_run(
                    &[main_url],
                    registry,
                    None,
                    &addresses,
                    &topic0s,
                    &calls,
                    Some(state_rpc),
                    chain_id,
                    1,
                    3,
                    10,
                    seal_direct,
                    false, // window_adaptive: irrelevant to this test, fixed is the cheaper default
                    1,
                    1,
                    Some(&work),
                    &TapeMode::Live,
                    None,
                )
                .await
                .unwrap()
            }
        };

        let hot = run_arm(false, hot_dir.path()).await;
        let sealed = run_arm(true, sealed_dir.path()).await;

        main_handle.abort();
        state_handle.abort();

        assert!(
            sealed.calls_resolved > 0,
            "control: the seal-direct arm must resolve calls (#725), or the equality below is two \
             zeroes agreeing"
        );
        assert_eq!(
            hot.calls_resolved, sealed.calls_resolved,
            "the hot-store arm must resolve the same declared [[calls]] as the seal-direct arm - \
             it is the denominator of every published multiplier, so an arm that skips tier 3 \
             flatters seal-direct by exactly the work it skipped (#743)"
        );
        assert!(
            hot.events >= sealed.calls_resolved,
            "call rows are committed alongside event rows, so the hot arm's row count must \
             include them: events = {}, calls = {}",
            hot.events,
            hot.calls_resolved
        );
    }
}
