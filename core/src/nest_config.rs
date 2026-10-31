//! The nest's part of `nuthatch.toml`: `[nest]`, its contracts, templates, factories, flags and
//! `[extract]`, and the decode registry they build (RFC-0077 S4). The binary's `Config` holds these
//! beside what only a node runs (webhooks, calls, IPFS, address history); an embedding reads them
//! through [`NestConfig`].

use anyhow::{anyhow, bail, Result};
use serde::{Deserialize, Serialize};

/// The nest-config schema this build writes and every build reads. A nest declaring a version above
/// the binary's `READABLE_SCHEMA_VERSION` is rejected on load - the guard that makes `init --from` safe.
pub const CURRENT_SCHEMA_VERSION: u32 = 2;

/// v3: canonical calls and `on_any` triggers (RFC-0060). Only a `graph` build reads it.
pub const GRAPH_SCHEMA_VERSION: u32 = 3;

/// An absent `schema_version` means **1**, not "current".
///
/// Every nest written before the field existed is a v1 nest, and treating it as current would mean a
/// future v3 build silently accepted a file it had no reason to believe it understood. The default is
/// a statement about the file, not about this binary.
fn default_schema_version() -> u32 {
    1
}

/// The lowest config-schema version for a nest's timestamp policy, before call requirements.
///
/// **v2 exists for exactly one reason:** `block_timestamps = false` (RFC-0029 §6b). A pre-0.9 binary
/// does not know the field, so it would parse such a nest, ignore the declaration, and index
/// timestamps into a store built without them - the two-schemas-in-one-nest failure the runtime guard
/// catches locally but an older binary cannot. Stamping v2 makes that binary refuse the nest instead.
///
/// A nest that indexes timestamps and has no canonical calls is stamped **v1**, because it is
/// genuinely a v1 file: nothing in it needs a v2 reader, and gratuitously raising the floor would stop
/// 0.8.x opening nests it can serve perfectly well. The version records what a *reader* must
/// understand, not which binary happened to write it. Canonical calls additionally require v3,
/// enforced by `check_schema_version`, so older readers cannot silently use block-number reads.
pub fn required_schema_version(block_timestamps: bool) -> u32 {
    if block_timestamps {
        1
    } else {
        2
    }
}

/// A child-contract template (RFC-0009): a name + a vendored ABI, applied to every contract a
/// factory discovers. All children of one template share tables (`{template}__{event}`),
/// distinguished by the implicit `address` column.
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct Template {
    pub name: String,
    /// ABI path relative to the nest dir, e.g. "abis/uniswap_v3_pool.json".
    pub abi: String,
    /// Backfill filter strategy override (RFC-0009 §4): `"topic0"` forces the topic0-only fetch (with
    /// local registry-lookup filtering) instead of the address list - useful when a template is known
    /// to have many children. Omit for the automatic address-list → topic0 flip above ~500 children.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
    /// Event names to decode for every child of this template, matching `[[contracts]].events`.
    /// Empty (the default) decodes every event the template ABI defines.
    ///
    /// Distinct from `filter`, which chooses *how the range is fetched*; this chooses *what is
    /// decoded from it*. Without it the vendored ABI is the only filter, so a full `UniswapV2Pair`
    /// ABI decodes Swap, Sync, Mint, Burn, Transfer and Approval when the nest wanted Swap - not
    /// wrong, but a different workload, and nothing said so. A name here the ABI does not define is
    /// a config error, caught at registry build.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<String>,
}

/// A factory rule (RFC-0009): when `watch`'s `event` fires, the child address in `child_param` is
/// indexed under `template`. `watch` is a `[[contracts]]` alias or another template (nested).
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct Factory {
    /// The alias of the watched contract (or a template, for nested factories).
    pub watch: String,
    /// The announcing event name, e.g. "PoolCreated", or its full signature when the ABI overloads it.
    pub event: String,
    /// The event parameter holding the child contract address, e.g. "pool".
    pub child_param: String,
    /// Which [`Template`] to apply to the discovered child.
    pub template: String,
    /// Optional: only honour discoveries at or after this block.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start: Option<u64>,
}

/// Threshold & velocity flag configuration (RFC-0008 C3). Amounts are token **base units** as decimal
/// strings (i128 - no currency conversion in-core, per the RFC). Both flavours are opt-in.
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct Flags {
    /// Flag any single transfer whose value ≥ this many base units (travel-rule style).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub threshold: Option<String>,
    /// Flag an address whose outbound volume within a block-window reaches this many base units.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub velocity_amount: Option<String>,
    /// The velocity block-window size. Blocks, not wall-clock: an honest approximation of "~24h"
    /// (≈ 7200 blocks on 12s-block mainnet). Defaults to [`DEFAULT_VELOCITY_WINDOW`] when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub velocity_window: Option<u64>,
}

/// Firehose-class extraction config (RFC-0014). Both surfaces are **opt-in per nest and default
/// off**, because they are the only rows in nuthatch whose volume is unbounded by the nest's own
/// configuration: events are bounded by "how often does this contract emit", whereas `state = true`
/// on a busy contract yields a row per `SSTORE` and `traces = true` a row per internal call.
///
/// That is why [`Extract::scope_check`] refuses an unscoped nest instead of warning about it. The
/// house rule is RFC-0012's: a budget stops being a budget the moment something may quietly exceed
/// it. `unbounded = true` is the deliberate opt-out, and has to be typed by a human.
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct Extract {
    /// Emit a row per **block**: header fields, one row per block, queryable (RFC-0036 §4.2).
    ///
    /// Unlike `traces`/`state` this is **sourceable from ordinary RPC** - `eth_getBlockByNumber` is
    /// already called for every window when `[nest] block_timestamps = true`, and this keeps the rest
    /// of the header instead of reading `timestamp` and discarding it. So it deliberately does **not**
    /// count towards [`Extract::enabled`], which is the node-gated pair and the startup refusal.
    ///
    /// Bounded by construction: exactly one row per block, so no volume guard applies.
    #[serde(default)]
    pub blocks: bool,
    /// Emit an `l1_blocks` row - `l1_block_number` - for each block that carries this nest's logs, on
    /// an Arbitrum-family chain (#1839). Read from the headers those blocks' timestamps come
    /// from when the logs do not carry one, so it costs at most one header per log-bearing block
    /// rather than one per block like `blocks`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub l1_blocks: bool,
    /// Emit a row per **call** (top-level and internal), calldata decoded by 4-byte selector.
    ///
    /// Node-gated: the *internal* call tree only exists as an execution artifact, so it needs
    /// `debug_*` or a colocated node. See [`Extract::top_level_calls`] for the half that does not.
    #[serde(default)]
    pub traces: bool,
    /// Emit a row per **top-level call** to this nest's contracts - a transaction sent directly to
    /// one of them, calldata decoded by 4-byte selector. This is what a subgraph's `callHandlers`
    /// fire on (RFC-0038 §5).
    ///
    /// Unlike `traces` this is **sourceable from ordinary RPC**: a top-level call is a transaction,
    /// and `eth_getBlockByNumber(b, true)` returns them. Filing it with `traces` behind the node gate
    /// was the same bundling-by-shape error RFC-0036 corrected for blocks and transactions - grouping
    /// by "non-event data" when the thing that matters is the *source*. So it deliberately does not
    /// count towards [`Extract::enabled`], which is the node-gated set and the startup refusal.
    ///
    /// Bounded by the nest's own contracts: a transaction to an address this nest does not index is
    /// not decoded and not stored.
    #[serde(default)]
    pub top_level_calls: bool,
    /// Emit a row per **storage write**: `(address, slot, prev, new)`, raw slots, no ABI needed.
    #[serde(default)]
    pub state: bool,
    /// Restrict extraction to these contract aliases. Empty means *every address on the chain*,
    /// which is the unbounded case - not merely "every contract in this nest".
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub contracts: Vec<String>,
    /// Restrict trace extraction to these 4-byte selectors (`0x` + 8 hex). Empty means every
    /// function. Ignored when `traces = false`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub selectors: Vec<String>,
    /// Accept an unscoped extraction nest anyway. Requires a human to have typed it, and is
    /// reported in the startup log so it is never a silent state.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub unbounded: bool,
}

impl Extract {
    pub fn is_empty(&self) -> bool {
        !self.blocks
            && !self.l1_blocks
            && !self.traces
            && !self.state
            && self.contracts.is_empty()
            && self.selectors.is_empty()
            && !self.unbounded
    }

    /// Is any **node-gated** extraction surface switched on? This is the startup-refusal predicate,
    /// so `blocks` is deliberately absent: it comes from ordinary RPC (RFC-0036 §1), and refusing a
    /// nest for asking for something we can actually serve would be the wrong half of RFC-0014's
    /// bundling mistake.
    pub fn enabled(&self) -> bool {
        self.traces || self.state
    }

    /// Is any surface at all switched on, node-gated or not? For "does this nest want extraction work
    /// scheduled", as distinct from "must this nest be refused".
    pub fn any(&self) -> bool {
        self.blocks || self.traces || self.state || self.top_level_calls
    }

    /// Does this nest decode calldata at all - internal calls, top-level calls, or both?
    ///
    /// The gate for building the binary's `CallRegistry` and advertising its tables. Distinct
    /// from [`Self::enabled`], which is the *node-gated* set: a top-level-calls nest needs the call
    /// decode surface and needs no node.
    pub fn decodes_calls(&self) -> bool {
        self.traces || self.top_level_calls
    }

    /// Scoped means "bounded by something the operator named". A selector allowlist bounds traces
    /// but says nothing about state diffs, so it only counts when `state` is off - otherwise a nest
    /// could look scoped while its storage half was still chain-wide.
    pub fn is_scoped(&self) -> bool {
        if !self.contracts.is_empty() {
            return true;
        }
        !self.state && self.traces && !self.selectors.is_empty()
    }

    /// `l1_blocks` is refused beside `blocks`, whose rows already carry the column for every block and
    /// share its row key, and on a registered chain that reports no L1 block.
    pub fn l1_blocks_check(&self, chain: &str, chain_id: u64) -> Result<()> {
        if !self.l1_blocks {
            return Ok(());
        }
        if self.blocks {
            bail!(
                "[extract] l1_blocks = true beside blocks = true: the `blocks` table already holds \
                 `l1_block_number` for every block. Drop `l1_blocks` and read `blocks` instead."
            );
        }
        let known = crate::chains::lookup(chain).or_else(|| crate::chains::lookup_by_id(chain_id));
        if let Some(c) = known.filter(|c| !crate::chains::reports_l1_block_number(c.chain_id)) {
            bail!(
                "[extract] l1_blocks = true on {}, whose block headers carry no `l1BlockNumber`. It \
                 exists on Arbitrum-family chains (arbitrum-one, robinhood) only.",
                c.name
            );
        }
        Ok(())
    }

    /// The volume guard (RFC-0014 §3). `Ok(())` to proceed, `Err` with a message that says what to
    /// do about it. Called before any extraction work is scheduled.
    pub fn scope_check(&self) -> Result<()> {
        if !self.enabled() || self.is_scoped() || self.unbounded {
            return Ok(());
        }
        let which = match (self.traces, self.state) {
            (true, true) => "traces and state",
            (true, false) => "traces",
            _ => "state",
        };
        bail!(
            "[extract] {which} = true with no `contracts` scope is unbounded by construction: it \
             extracts every address on the chain, not just this nest's, so its row count is a \
             property of chain traffic rather than of your config. Add `contracts = [\"alias\", …]` \
             to scope it{}, or set `unbounded = true` to accept a nest whose footprint nobody can \
             predict.",
            if self.traces && !self.state {
                " (or `selectors = [\"0x…\"]` for traces alone)"
            } else {
                ""
            }
        )
    }

    /// Normalised selector allowlist as raw 4-byte keys, rejecting anything malformed rather than
    /// silently ignoring it - a typo'd selector that filtered nothing would look like it worked.
    pub fn selector_keys(&self) -> Result<Vec<[u8; 4]>> {
        self.selectors
            .iter()
            .map(|s| {
                let hex_part = s.strip_prefix("0x").unwrap_or(s);
                let raw = hex::decode(hex_part)
                    .map_err(|e| anyhow!("[extract] selector `{s}` is not hex: {e}"))?;
                let four: [u8; 4] = raw.as_slice().try_into().map_err(|_| {
                    anyhow!(
                        "[extract] selector `{s}` is {} bytes; a function selector is exactly 4",
                        raw.len()
                    )
                })?;
                Ok(four)
            })
            .collect()
    }
}

/// Default velocity window (~24h of 12s mainnet blocks). Documented as a block-count approximation.
pub const DEFAULT_VELOCITY_WINDOW: u64 = 7_200;

impl Flags {
    pub fn is_empty(&self) -> bool {
        self.threshold.is_none() && self.velocity_amount.is_none() && self.velocity_window.is_none()
    }

    /// The single-transfer threshold in base units, if configured and parseable.
    pub fn threshold_amount(&self) -> Option<i128> {
        self.threshold.as_deref().and_then(|s| s.parse().ok())
    }

    /// The velocity `(amount, window)` in `(base units, blocks)`, if an amount is configured.
    pub fn velocity(&self) -> Option<(i128, u64)> {
        let amount = self
            .velocity_amount
            .as_deref()
            .and_then(|s| s.parse::<i128>().ok())?;
        let window = self
            .velocity_window
            .unwrap_or(DEFAULT_VELOCITY_WINDOW)
            .max(1);
        Some((amount, window))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Nest {
    pub name: String,
    pub chain: String,
    pub chain_id: u64,
    /// Absent → none here; the run supplies them (`--rpc`, or `NUTHATCH_RPC_<chain id>` on a directory of chains).
    #[serde(default)]
    pub rpc_urls: Vec<String>,
    /// Config schema version (see `CURRENT_SCHEMA_VERSION`). Absent in older nests → treated as 1.
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    /// Whether every row carries the implicit `block_timestamp` column (RFC-0029 §6b).
    ///
    /// **This is an `init`-time declaration, not a tuning flag**, and the default is `true` because
    /// that is what every nest before it produced. Turning it off removes a column from *every* table,
    /// which RFC-0020's classifier calls `ColumnRemoved` - breaking - and it changes the bytes of every
    /// sealed segment, so a switched nest cannot reuse its own content-addressed segments. Changing it
    /// on a nest that has already indexed is refused at startup (see `TIMESTAMPS_KEY` in `indexer.rs`)
    /// and must go through the ordinary breaking-update path: a new nest, served alongside the old one
    /// for its existing consumers.
    ///
    /// What it buys is real - timestamp acquisition measured at ~85% of backfill wall clock - but it is
    /// a *new-nest* win, which is a narrower claim than "backfills get faster".
    #[serde(default = "default_block_timestamps")]
    pub block_timestamps: bool,
}

fn default_block_timestamps() -> bool {
    true
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Contract {
    pub alias: String,
    pub address: String,
    /// Deployment block (auto-detected at init); None → backfill from a tip offset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_block: Option<u64>,
    /// ABI path relative to the nest dir, e.g. "abis/usdc.json".
    pub abi: String,
    /// Optional per-contract event allowlist (RFC-0011): only these events (by ABI name) are decoded
    /// and stored for this contract. Empty (the default) indexes every event the ABI defines. This is
    /// how a nest indexing e.g. GraphToken keeps only `Transfer` instead of millions of irrelevant
    /// rows. A name here that the ABI doesn't define is a config error, caught at registry build.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<String>,
}

/// SEC-10: anything that becomes a filesystem path segment *and* a route segment gets this charset.
///
/// Tenants and aliases are both. A tenant being **opaque** to nuthatch (RFC-0032 §6) means we never
/// interpret it, not that it may contain `..` or a separator - opacity is about meaning, not about
/// what a path resolver will do with it.
pub fn safe_segment(value: &str, what: &str) -> Result<()> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        bail!("{what} '{value}' is invalid (allowed: letters, digits, '_', '-')");
    }
    Ok(())
}

/// The schema-version gates on `[nest]` alone, for a reader that loads up to `readable`.
pub fn check_schema_version(nest: &Nest, readable: u32) -> Result<()> {
    // Too new for us: the nest was authored by a later nuthatch and may mean things by fields we
    // do not know. This is the guard that makes `init --from` safe.
    if nest.schema_version > readable {
        if nest.schema_version == GRAPH_SCHEMA_VERSION {
            bail!(
                "this nest uses config schema v{GRAPH_SCHEMA_VERSION} (canonical calls or \
                 `on_any` triggers), which only a nuthatch built with `--features graph` reads"
            );
        }
        bail!(
            "this nest needs config schema v{} but this nuthatch supports up to v{} - upgrade nuthatch",
            nest.schema_version,
            readable
        );
    }
    // Too *old* for what it declares - the mirror case, and the dangerous one. A file claiming v1
    // while declaring a v2-only feature is exactly the file an older binary would accept and get
    // wrong. We cannot fix that binary; we can refuse to be the source of such a file, and anyone
    // hand-editing `block_timestamps` finds out here rather than from a nest that quietly
    // disagrees with itself on someone else's machine.
    let need = required_schema_version(nest.block_timestamps);
    if nest.schema_version < need {
        bail!(
            "this nest declares `block_timestamps = {}`, which needs `schema_version = {}` \
             (it says {}). Older nuthatch builds don't know the field and would index timestamps \
             into a nest built without them; the version is what makes them refuse instead.",
            nest.block_timestamps,
            need,
            nest.schema_version
        );
    }
    Ok(())
}

pub fn parse_address(s: &str) -> Result<alloy_primitives::Address> {
    let bytes = hex::decode(s.trim_start_matches("0x"))
        .map_err(|e| anyhow!(e).context("bad address hex"))?;
    if bytes.len() != 20 {
        return Err(anyhow!("address is not 20 bytes"));
    }
    Ok(alloy_primitives::Address::from_slice(&bytes))
}

/// Where a nest's ABIs come from: `read(path, what)` gives the name an error shows and the text, for
/// `what` "ABI" or "template ABI". A node reads its directory; an embedding, the files it was given.
pub type ReadAbi<'a> = dyn FnMut(&str, &str) -> Result<(String, String)> + 'a;

/// The decode registry `[nest]`, the contracts, the templates and `[extract]` describe.
pub fn build_registry(
    nest: &Nest,
    contracts: &[Contract],
    templates: &[Template],
    extract: &Extract,
    read: &mut ReadAbi<'_>,
) -> Result<nuthatch_decode::registry::DecodeRegistry> {
    use anyhow::Context;
    use nuthatch_decode::registry::{ContractSpec, DecodeRegistry, TemplateSpec};
    let mut specs = Vec::with_capacity(contracts.len());
    for c in contracts {
        let (shown, raw) = read(&c.abi, "ABI")?;
        let abi = nuthatch_decode::abi::parse_str(&raw)
            .with_context(|| format!("parsing ABI {shown}"))?;
        let address = parse_address(&c.address)?;
        // A parse that always returns zero used to hang the mutants job: indexer tests wait for a
        // head that never moves (#1281).
        if address.is_zero() {
            return Err(anyhow!(
                "contract `{}` is the zero address, which is not a contract nuthatch will index",
                c.alias
            ));
        }
        specs.push(ContractSpec {
            alias: c.alias.clone(),
            address,
            abi,
            events: c.events.clone(),
        });
    }
    let mut template_specs = Vec::with_capacity(templates.len());
    for t in templates {
        let (shown, raw) = read(&t.abi, "template ABI")?;
        let abi = nuthatch_decode::abi::parse_str(&raw)
            .with_context(|| format!("parsing template ABI {shown}"))?;
        template_specs.push(TemplateSpec {
            name: t.name.clone(),
            abi,
            events: t.events.clone(),
        });
    }
    Ok(DecodeRegistry::build_with_templates(specs, template_specs)?
        .with_timestamps(nest.block_timestamps)
        .with_blocks(extract.blocks)
        .with_l1_blocks(extract.l1_blocks))
}

/// The factory rules `contracts`, `templates` and `factories` declare.
pub fn declared(
    contracts: &[Contract],
    templates: &[Template],
    factories: &[Factory],
) -> crate::factory::Declared {
    use crate::factory::{Declared, FactoryDecl, TemplateDecl};
    Declared {
        contracts: contracts.iter().map(|c| c.alias.clone()).collect(),
        templates: templates
            .iter()
            .map(|t| TemplateDecl {
                name: t.name.clone(),
                topic0_only: t.filter.as_deref() == Some("topic0"),
            })
            .collect(),
        factories: factories
            .iter()
            .map(|f| FactoryDecl {
                watch: f.watch.clone(),
                event: f.event.clone(),
                child_param: f.child_param.clone(),
                template: f.template.clone(),
                start: f.start,
            })
            .collect(),
    }
}

/// Top-level tables of `nuthatch.toml` that only a node runs: delivery, `eth_call` reads, IPFS and
/// address history.
pub const NODE_ONLY_TABLES: &[&str] = &["webhooks", "alerts", "calls", "ipfs", "address_history"];

/// The nest's part of a `nuthatch.toml`, as an embedding reads it.
#[derive(Debug, Deserialize)]
pub struct NestConfig {
    pub nest: Nest,
    #[serde(default)]
    pub contracts: Vec<Contract>,
    #[serde(default)]
    pub flags: Flags,
    #[serde(default)]
    pub templates: Vec<Template>,
    #[serde(default)]
    pub factories: Vec<Factory>,
    #[serde(default)]
    pub extract: Extract,
}

impl NestConfig {
    /// `raw` read as a node reads its nest's part, with the node's gates: the schema version, `[extract]`'s
    /// `l1_blocks`, and names safe as path segments. A table only a node runs, a key nothing reads, and
    /// extraction an embedding cannot source are refused by name rather than ignored.
    pub fn parse(raw: &str) -> Result<NestConfig> {
        let table: toml::Table = toml::from_str(raw).map_err(|e| anyhow!("nuthatch.toml: {e}"))?;
        if let Some(key) = table
            .keys()
            .find(|k| NODE_ONLY_TABLES.contains(&k.as_str()))
        {
            bail!(
                "nuthatch.toml declares [{key}], which only the nuthatch binary runs: an embedded \
                 nest cannot honour it, and is not opened without it"
            );
        }
        let mut unknown = Vec::new();
        let cfg: NestConfig = serde_ignored::deserialize(toml::Deserializer::new(raw), |path| {
            unknown.push(path.to_string())
        })
        .map_err(|e| anyhow!("nuthatch.toml: {e}"))?;
        if !unknown.is_empty() {
            bail!(
                "nuthatch.toml has key(s) nuthatch does not read: {}",
                unknown.join(", ")
            );
        }
        check_schema_version(&cfg.nest, CURRENT_SCHEMA_VERSION)?;
        cfg.extract
            .l1_blocks_check(&cfg.nest.chain, cfg.nest.chain_id)?;
        if cfg.extract.any() {
            bail!(
                "nuthatch.toml's [extract] asks for blocks, calls, traces or state, which an \
                 embedded nest does not extract"
            );
        }
        for c in &cfg.contracts {
            safe_segment(&c.alias, "contract alias")?;
        }
        for t in &cfg.templates {
            safe_segment(&t.name, "template name")?;
        }
        Ok(cfg)
    }

    /// The decode registry, its ABIs read from `files` by path.
    pub fn registry(
        &self,
        files: &std::collections::BTreeMap<String, String>,
    ) -> Result<nuthatch_decode::registry::DecodeRegistry> {
        build_registry(
            &self.nest,
            &self.contracts,
            &self.templates,
            &self.extract,
            &mut |path, what| {
                let text = files
                    .get(path)
                    .ok_or_else(|| anyhow!("reading {what} {path}: not among the nest's files"))?;
                Ok((path.to_string(), text.clone()))
            },
        )
    }

    pub fn declared(&self) -> crate::factory::Declared {
        declared(&self.contracts, &self.templates, &self.factories)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NEST: &str = r#"
[nest]
name = "n"
chain = "arbitrum-one"
chain_id = 42161
rpc_urls = []

[[contracts]]
alias = "token"
address = "0x9623063377ad1b27544c965ccd7342f7ea7e88c7"
start_block = 1
abi = "abis/token.json"
"#;

    fn refused(raw: &str) -> String {
        format!("{:#}", NestConfig::parse(raw).expect_err("refused"))
    }

    #[test]
    fn the_nests_part_parses_and_builds_its_registry() {
        let cfg = NestConfig::parse(NEST).unwrap();
        let mut files = std::collections::BTreeMap::new();
        files.insert(
            "abis/token.json".to_string(),
            r#"[{"type":"event","name":"Transfer","anonymous":false,"inputs":[
                {"name":"from","type":"address","indexed":true},
                {"name":"to","type":"address","indexed":true},
                {"name":"value","type":"uint256","indexed":false}]}]"#
                .to_string(),
        );
        let registry = cfg.registry(&files).unwrap();
        assert_eq!(registry.tables()[0].table, "token__transfer");
        assert!(format!(
            "{:#}",
            cfg.registry(&Default::default()).err().expect("no ABI")
        )
        .contains("abis/token.json"));
    }

    #[test]
    fn what_only_a_node_runs_is_refused_by_name() {
        let webhook =
            format!("{NEST}\n[[webhooks]]\nname = \"w\"\ntable = \"t\"\nurl = \"http://x\"\n");
        assert!(refused(&webhook).contains("[webhooks]"));
        let calls = format!("{NEST}\n[[calls]]\nname = \"c\"\n");
        assert!(refused(&calls).contains("[calls]"));
        let typo = NEST.replace("start_block", "start_blok");
        assert!(refused(&typo).contains("contracts.0.start_blok"));
        let blocks = format!("{NEST}\n[extract]\nblocks = true\n");
        assert!(refused(&blocks).contains("[extract]"));
        let alias = NEST.replace("alias = \"token\"", "alias = \"../x\"");
        assert!(refused(&alias).contains("contract alias"));
        let graph = NEST.replace("rpc_urls = []", "rpc_urls = []\nschema_version = 3");
        assert!(refused(&graph).contains("--features graph"));
    }

    #[test]
    fn a_nest_may_leave_its_rpc_urls_to_the_run() {
        let cfg = NestConfig::parse(&NEST.replace("rpc_urls = []\n", "")).unwrap();
        assert!(cfg.nest.rpc_urls.is_empty());
    }
}
