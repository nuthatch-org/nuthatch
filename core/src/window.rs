//! A window's reads and decode: what the cursor asks the source for, how it decodes and stamps what
//! comes back, and where finality puts the seal ceiling.

use crate::factory::{ChildRegistry, FactorySet};
use crate::metrics::CursorMetrics;
use crate::source::{Log, LogFilter, Source};
use anyhow::{Context, Result};
use nuthatch_decode::registry::{DecodeRegistry, DecodedRow};
use std::collections::{BTreeSet, HashMap, HashSet};

/// A warning that can repeat every poll fires on the first and then every this many (#1666, #1667).
pub const REPEAT_WARN_EVERY: u64 = 100;

/// How a chain decides a block is final enough to seal to the immutable cold layer. The sealing
/// invariant is unchanged either way: the columnar layer never sees a reorg, so this only sets *how
/// far behind the tip* we wait before a block is beyond reorg risk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Finality {
    /// Seal blocks at least `n` behind the tip. A conservative proxy for finality (Ethereum L1).
    Depth(u64),
    /// Prefer the node's L1-aware `finalized` block tag (correct by construction on an L2 like
    /// Arbitrum); fall back to `Depth(fallback_depth)` when the endpoint doesn't serve the tag.
    FinalizedTag { fallback_depth: u64 },
}

/// The chain moved under a window while it was being read: the tip loop reads the window again
/// rather than ending the nest (#2154).
#[derive(Debug)]
pub struct WindowForked(pub String);

impl std::fmt::Display for WindowForked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for WindowForked {}

/// Block timestamps for `blocks` - or an empty map, without touching the network, when the nest
/// declared it doesn't index them (RFC-0029 §6b).
///
/// **This is where the win actually is.** Timestamps arrive one `eth_getBlockByNumber` per block over
/// a round trip `eth_getLogs` does not carry, measured at ~85% of backfill wall clock (RFC-0029 §4).
/// Every path that stamps rows goes through here so there is exactly one place the decision is made,
/// and no path can quietly keep paying for a column its nest doesn't have.
///
/// Returning an empty map is safe rather than lossy: `DecodedRow::block_timestamp` stays 0 and
/// `to_json` omits the key entirely, so the zero never reaches the store or a segment.
pub async fn fetch_timestamps(
    source: &dyn Source,
    registry: &DecodeRegistry,
    blocks: &[u64],
) -> Result<HashMap<u64, u64>> {
    if !registry.timestamps() {
        return Ok(HashMap::new());
    }
    source.block_timestamps(blocks).await
}

/// A block the source answered without a timestamp, or with 0, refuses the window: the stamp
/// below would store 0, and once sealed it is permanent (#1911). Genesis is the one block at 0.
pub fn refuse_unstamped(
    registry: &DecodeRegistry,
    blocks: &[u64],
    timestamps: &HashMap<u64, u64>,
) -> Result<()> {
    if !registry.timestamps() {
        return Ok(());
    }
    match blocks
        .iter()
        .find(|&&b| b != 0 && timestamps.get(&b).is_none_or(|&t| t == 0))
    {
        Some(b) => anyhow::bail!(
            "the source gave block {b} no timestamp (missing or 0); refusing the window rather \
             than storing block_timestamp 0"
        ),
        None => Ok(()),
    }
}

pub struct WindowBlockData {
    pub timestamps: HashMap<u64, u64>,
    pub headers: Option<HashMap<u64, serde_json::Value>>,
    /// The same headers, for an `[extract] l1_blocks` nest's rows (#1839); empty otherwise.
    pub l1_headers: HashMap<u64, serde_json::Value>,
}

/// Canonical reads need the same header as the event timestamps. Keep it only
/// for this window; a retry or another window must fetch its own fork evidence.
pub async fn fetch_window_block_data(
    source: &dyn Source,
    registry: &DecodeRegistry,
    blocks: &[u64],
    canonical_calls: bool,
) -> Result<WindowBlockData> {
    if !canonical_calls && !registry.l1_blocks() {
        let timestamps = fetch_timestamps(source, registry, blocks).await?;
        refuse_unstamped(registry, blocks, &timestamps)?;
        return Ok(WindowBlockData {
            timestamps,
            headers: None,
            l1_headers: HashMap::new(),
        });
    }
    let headers = source.block_headers(blocks).await?;
    let mut timestamps = HashMap::new();
    for block in blocks {
        let header = headers
            .get(block)
            .with_context(|| format!("missing window header at {block}"))?;
        if registry.timestamps() {
            let timestamp = header
                .get("timestamp")
                .and_then(|v| {
                    v.as_u64().or_else(|| {
                        v.as_str()?
                            .strip_prefix("0x")
                            .and_then(|s| u64::from_str_radix(s, 16).ok())
                    })
                })
                .with_context(|| format!("missing window timestamp at {block}"))?;
            timestamps.insert(*block, timestamp);
        }
    }
    refuse_unstamped(registry, blocks, &timestamps)?;
    let (headers, l1_headers) = match (canonical_calls, registry.l1_blocks()) {
        (true, true) => (Some(headers.clone()), headers),
        (true, false) => (Some(headers), HashMap::new()),
        (false, _) => (None, headers),
    };
    Ok(WindowBlockData {
        timestamps,
        headers,
        l1_headers,
    })
}

/// One `l1_blocks` row per block that carries a row in `rows` (#1839). A header without
/// `l1BlockNumber` refuses the window, as a missing timestamp does: a 0 would be sealed for good.
pub fn l1_block_rows(
    registry: &DecodeRegistry,
    rows: &[DecodedRow],
    data: &WindowBlockData,
) -> Result<Vec<DecodedRow>> {
    if !registry.l1_blocks() {
        return Ok(Vec::new());
    }
    let mut blocks: Vec<u64> = rows.iter().map(|r| r.block_number).collect();
    blocks.sort_unstable();
    blocks.dedup();
    blocks
        .into_iter()
        .map(|b| {
            data.l1_headers
                .get(&b)
                .and_then(|h| nuthatch_decode::registry::l1_block_row(b, h, registry.timestamps()))
                .with_context(|| {
                    format!(
                        "block {b}'s header carries no l1BlockNumber; [extract] l1_blocks needs an \
                         Arbitrum-family chain and an endpoint that returns it"
                    )
                })
        })
        .collect()
}

/// Blocks that still need a header after local filtering (#765).
///
/// A topic0-only fetch (factory flip, RFC-0009 §4) returns every log on the chain with that
/// topic0, including other protocols that share the event shape. Stamping timestamps from the
/// raw log list paid `eth_getBlockByNumber` for blocks whose rows we then discarded. The kept
/// rows plus any `[[calls]]` sample blocks are the only headers worth buying.
pub fn blocks_needing_timestamps(
    rows: &[DecodedRow],
    extra: impl IntoIterator<Item = u64>,
) -> Vec<u64> {
    let mut blocks: Vec<u64> = rows.iter().map(|r| r.block_number).collect();
    blocks.extend(extra);
    blocks.sort_unstable();
    blocks.dedup();
    blocks
}

pub fn apply_row_timestamps(rows: &mut [DecodedRow], ts: &HashMap<u64, u64>) {
    for r in rows {
        r.block_timestamp = ts.get(&r.block_number).copied().unwrap_or(0);
    }
}

/// The highest block safe to seal under `finality`: the `finalized` tag when the chain uses it and
/// the node serves it, else a fixed depth below the tip. Pure, so the policy is unit-testable.
pub fn seal_ceiling(finality: Finality, tip: u64, finalized_tag: Option<u64>) -> u64 {
    match finality {
        Finality::Depth(d) => tip.saturating_sub(d),
        Finality::FinalizedTag { fallback_depth } => match finalized_tag {
            Some(n) if n < tip => n,
            Some(n) => {
                warn_implausible_finalized(n, tip, fallback_depth);
                tip.saturating_sub(fallback_depth)
            }
            None => tip.saturating_sub(fallback_depth),
        },
    }
}

/// A `finalized` tag at or past the tip is what an endpoint aliasing it to `latest` answers (#1667).
/// It can also be a tip read a block or two before the tag, which costs one poll's seal advance.
fn warn_implausible_finalized(finalized: u64, tip: u64, fallback_depth: u64) {
    static SEEN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = SEEN.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
    if n == 1 || n.is_multiple_of(REPEAT_WARN_EVERY) {
        tracing::warn!(
            "the endpoint's finalized block {finalized} is not below the tip {tip}, so it is not \
             trusted: sealing falls back to {fallback_depth} blocks below the tip ({n} time(s) so far)"
        );
    }
}

/// A log whose topic0 a declared decoder claims and whose body did not decode (RFC-0049 §2).
#[derive(Debug)]
pub struct Undecoded {
    pub block: u64,
    pub contract: String,
    pub topic0: String,
    pub error: String,
}

impl Undecoded {
    pub fn new(log: &Log, contract: Option<&str>, e: &anyhow::Error) -> Self {
        Undecoded {
            block: log.block_number,
            contract: contract.unwrap_or("other").to_string(),
            topic0: log
                .topics
                .first()
                .map_or_else(String::new, |t| t.to_ascii_lowercase()),
            error: format!("{e:#}"),
        }
    }
}

/// Counts a window's undecoded logs into `metrics` and logs them as one line, not one per log. Only
/// blocks `from..=to` count: below `from` is a refetched tail the previous window already counted.
pub fn note_undecoded(
    metrics: Option<&dyn CursorMetrics>,
    undecoded: &[Undecoded],
    from: u64,
    to: u64,
) {
    let counted: Vec<&Undecoded> = undecoded
        .iter()
        .filter(|u| (from..=to).contains(&u.block))
        .collect();
    let Some(first) = counted.first() else {
        return;
    };
    if let Some(m) = metrics {
        for u in &counted {
            m.add_undecoded(&u.contract, &u.topic0, 1);
        }
    }
    tracing::warn!(
        "{} log(s) in blocks {from}..={to} matched a declared event and did not decode; the first, \
         {} {}: {}",
        counted.len(),
        first.contract,
        first.topic0,
        first.error
    );
}

/// Decode a window's logs in chain order (block, log_index), routing each to a contract decoder or -
/// for a factory nest - a discovered child's template decoder. Timestamps may be empty here: #765
/// fetches headers only after local filtering, then `apply_row_timestamps` /
/// `ChildRegistry::apply_timestamps` fill `block_timestamp` and `discovered_timestamp`.
///
/// A child's logs are decoded from its announcing block on, including those earlier in that block
/// (its constructor's), and never from an earlier block, so what is kept does not depend on window
/// size or fetch path (#2150, RFC-0009 §3). `children` is only read: the children this window
/// announces are returned for the caller to register once the window commits (#2153).
pub fn decode_window(
    registry: &DecodeRegistry,
    factory: Option<&FactorySet>,
    children: &ChildRegistry,
    logs: &[Log],
    timestamps: &HashMap<u64, u64>,
) -> (Vec<DecodedRow>, ChildRegistry, Vec<Undecoded>) {
    let mut ordered: Vec<&Log> = logs.iter().collect();
    ordered.sort_by(|a, b| {
        a.block_number
            .cmp(&b.block_number)
            .then_with(|| a.log_index.cmp(&b.log_index))
    });

    let mut found = ChildRegistry::new();
    let mut rows = Vec::new();
    let mut undecoded = Vec::new();
    for block in ordered.chunk_by(|a, b| a.block_number == b.block_number) {
        // Children announced in this block route its logs; a pass that announces a child whose log
        // it already passed over is run again, and only the last pass's failures are kept.
        let mut in_block = ChildRegistry::new();
        let (block_rows, block_undecoded) = loop {
            let mut missed = std::collections::HashSet::new();
            let mut grew = false;
            let mut block_rows = Vec::new();
            let mut block_undecoded = Vec::new();
            for &log in block {
                let decoded = match registry.decode(log) {
                    Ok(Some(r)) => Some(r),
                    Ok(None) => factory.and_then(|_| {
                        let addr = log.address.to_ascii_lowercase();
                        let template = children
                            .get(&addr)
                            .filter(|c| c.discovered_block <= log.block_number)
                            .or_else(|| found.get(&addr))
                            .or_else(|| in_block.get(&addr))
                            .map(|c| c.template.clone());
                        if template.is_none() {
                            missed.insert(addr);
                        }
                        let t = template?;
                        match registry.decode_child(log, &t) {
                            Ok(r) => r,
                            Err(e) => {
                                block_undecoded.push(Undecoded::new(log, Some(&t), &e));
                                None
                            }
                        }
                    }),
                    Err(e) => {
                        block_undecoded.push(Undecoded::new(log, registry.alias_of(log), &e));
                        None
                    }
                };
                if let Some(mut r) = decoded {
                    r.block_timestamp = timestamps.get(&r.block_number).copied().unwrap_or(0);
                    if let Some(fs) = factory {
                        // Every child this event announces, not just the first (issue #241).
                        for child in fs.discover(&r) {
                            let known =
                                children.contains(&child.address) || found.contains(&child.address);
                            grew |= !known && missed.contains(&child.address);
                            if !known {
                                in_block.insert(child);
                            }
                        }
                    }
                    block_rows.push(r);
                }
            }
            if !grew {
                break (block_rows, block_undecoded);
            }
        };
        undecoded.extend(block_undecoded);
        // Registered in the order the stored rows fold, as `rebuild_children` does after a restart.
        if let Some(fs) = factory {
            for child in block_rows.iter().flat_map(|r| fs.discover(r)) {
                if !children.contains(&child.address) && found.insert(child.clone()) {
                    tracing::info!(
                        "factory discovered {} child {}… at block {}",
                        child.template,
                        &child.address[..12.min(child.address.len())],
                        child.discovered_block
                    );
                }
            }
        }
        rows.extend(block_rows);
    }
    (rows, found, undecoded)
}

/// Above this many discovered children, the factory backfill flips from an address-list filter to a
/// topic0-only fetch with local registry-lookup filtering (RFC-0009 §4) - providers cap address-list
/// size, and a huge list is slower than fetching by topic0 and discarding non-children locally.
pub const FACTORY_FLIP_THRESHOLD: usize = 500;

/// The templates a flipped factory nest fetches by topic0 alone, and those topic0s.
pub struct WideTemplates {
    pub names: BTreeSet<String>,
    pub topic0s: Vec<String>,
}

impl WideTemplates {
    pub fn covers(&self, template: Option<&str>) -> bool {
        template.is_some_and(|t| self.names.contains(t))
    }
}

/// Past the flip, only a template with more than [`FACTORY_FLIP_THRESHOLD`] children of its own is
/// fetched without an address. Static contracts never are: one declaring a generic event (ENS's
/// BaseRegistrar and its ERC-721 `Transfer`) would otherwise ask for every such log on the chain.
/// Small templates that together cross the threshold go wide, but only those with a child: a
/// template with none has nothing to cover, and its events may be as generic as ERC-1155 transfers.
pub fn wide_templates(
    registry: &DecodeRegistry,
    factory: &FactorySet,
    children: &ChildRegistry,
    base: usize,
    force: bool,
) -> Option<WideTemplates> {
    let mut names: BTreeSet<String> = factory
        .templates()
        .iter()
        .filter(|t| force || children.addresses_for_template(t).len() > FACTORY_FLIP_THRESHOLD)
        .cloned()
        .collect();
    if names.is_empty() && base + children.len() > FACTORY_FLIP_THRESHOLD {
        names = factory
            .templates()
            .iter()
            .filter(|t| !children.addresses_for_template(t).is_empty())
            .cloned()
            .collect();
    }
    let mut topic0s: Vec<String> = names
        .iter()
        .flat_map(|t| registry.template_topic0s(t))
        .map(|t| format!("0x{}", hex::encode(t)))
        .collect();
    topic0s.sort();
    topic0s.dedup();
    (!topic0s.is_empty()).then_some(WideTemplates { names, topic0s })
}

/// One copy of each log, in chain order. A wide fetch and an address fetch can both return a log.
pub fn dedup_logs(logs: &mut Vec<Log>) {
    logs.sort_by_key(|l| (l.block_number, l.log_index));
    logs.dedup_by_key(|l| (l.block_number, l.log_index));
}

/// `from..=to` address-filtered, then refetched for the children each round discovers until a round
/// finds none: the backfill's pass 1 and pass 2 fixpoint, for the tip loops.
///
/// `discover` decodes everything fetched so far and returns every child it knows of. A child
/// created in the range announces itself from its factory's address, which round 1 fetched, so its
/// own logs in the same range arrive in round 2.
pub async fn fetch_discovering(
    source: &dyn Source,
    addresses: &[String],
    topics: &[String],
    from: u64,
    to: u64,
    mut discover: impl FnMut(&[Log]) -> Vec<String>,
) -> Result<Vec<Log>> {
    let mut fetched: HashSet<String> = addresses.iter().map(|a| a.to_ascii_lowercase()).collect();
    let Some(filter) = LogFilter::new(addresses, topics) else {
        return Ok(Vec::new());
    };
    let mut all = source.logs(&filter, from, to).await?;
    loop {
        let new: Vec<String> = discover(&all)
            .into_iter()
            .filter(|c| fetched.insert(c.to_ascii_lowercase()))
            .collect();
        // An empty `new` would make the filter below address-less, so it returns first.
        if new.is_empty() {
            return Ok(all);
        }
        let Some(child_filter) = LogFilter::new(&new, topics) else {
            return Ok(all);
        };
        all.extend(source.logs(&child_filter, from, to).await?);
    }
}

/// What a nest's window is asked for by.
pub struct Wanted<'a> {
    pub registry: &'a DecodeRegistry,
    pub factory: Option<&'a FactorySet>,
    pub children: &'a ChildRegistry,
    pub addresses: &'a [String],
    pub topic0s: &'a [String],
    /// Set once the source refuses this factory nest's topic0-only getLogs (#1941): it is asked by
    /// address from then on, past the flip threshold or not.
    pub topic0_refused: &'a mut bool,
}

/// This window's logs. A factory nest is asked by `base ∪ children` until it has more than
/// [`FACTORY_FLIP_THRESHOLD`] children, the rule the seal-direct factory backfill follows (#1941).
pub async fn fetch_window(
    w: Wanted<'_>,
    source: &dyn Source,
    from: u64,
    to: u64,
) -> Result<Vec<Log>> {
    let Wanted {
        registry,
        factory,
        children,
        addresses,
        topic0s,
        topic0_refused,
    } = w;
    let Some(fs) = factory else {
        return match LogFilter::new(addresses, topic0s) {
            Some(f) => source.logs(&f, from, to).await,
            None => Ok(Vec::new()),
        };
    };
    let known: Vec<String> = registry
        .addresses()
        .iter()
        .map(|a| format!("0x{}", hex::encode(a)))
        .chain(children.addresses().into_iter().map(str::to_string))
        .collect();
    // Per template, as the backfill does: see `wide_templates`.
    let mut wide = if *topic0_refused {
        None
    } else {
        wide_templates(
            registry,
            fs,
            children,
            registry.addresses().len(),
            fs.force_topic0(),
        )
    };
    let mut wide_logs = Vec::new();
    if let Some(w) = &wide {
        let f =
            LogFilter::new(&[], &w.topic0s).expect("a wide template decodes at least one event");
        match source.logs(&f, from, to).await {
            Err(e) if source.refuses_topic0_only(&e) => {
                tracing::warn!(
                    "the endpoint refuses getLogs without an address list ({e:#}); asking by \
                     factory and child address from now on"
                );
                *topic0_refused = true;
                wide = None;
            }
            r => wide_logs = r?,
        }
    }
    let covered =
        |c: &str, reg: &ChildRegistry| wide.as_ref().is_some_and(|w| w.covers(reg.template_of(c)));
    let narrow: Vec<String> = known
        .into_iter()
        .filter(|c| !covered(c, children))
        .collect();
    let mut logs = fetch_discovering(source, &narrow, topic0s, from, to, |logs| {
        let mut seen = wide_logs.clone();
        seen.extend_from_slice(logs);
        let (_, found, _) = decode_window(registry, Some(fs), children, &seen, &HashMap::new());
        children
            .addresses()
            .into_iter()
            .chain(found.addresses())
            .filter(|c| !covered(c, children) && !covered(c, &found))
            .map(str::to_string)
            .collect()
    })
    .await?;
    if !wide_logs.is_empty() {
        logs.extend(wide_logs);
        dedup_logs(&mut logs);
    }
    Ok(logs)
}
