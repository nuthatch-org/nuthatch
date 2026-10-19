//! The one config file a nest has: `nuthatch.toml`.
//!
//! v2 (RFC-0001) is a `[nest]` header plus a `[[contracts]]` array - many contracts per nest. A
//! v1 file (single top-level `address`) is migrated transparently on load, so existing projects
//! keep working.

use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

pub const CONFIG_FILE: &str = "nuthatch.toml";
pub const DB_FILE: &str = "nuthatch.redb";
/// v1 default ABI filename, retained for migration of old single-contract projects.
pub const ABI_FILE: &str = "abi.json";

/// The nest-config schema this build writes and every build reads. A nest declaring a version above
/// [`READABLE_SCHEMA_VERSION`] is rejected on load - the guard that makes `init --from` safe.
pub const CURRENT_SCHEMA_VERSION: u32 = 2;

/// v3: canonical calls and `on_any` triggers (RFC-0060). Only a `graph` build reads it.
pub const GRAPH_SCHEMA_VERSION: u32 = 3;

/// The newest nest-config schema this build can load.
pub const READABLE_SCHEMA_VERSION: u32 = if cfg!(feature = "graph") {
    GRAPH_SCHEMA_VERSION
} else {
    CURRENT_SCHEMA_VERSION
};

/// RFC-0060: the Graph history read policy needs a `graph` build. A default build would otherwise
/// serve latest-only answers without the freshness limit the policy promises.
///
/// RFC-0059: `folds/` is refused rather than skipped, because a view reading a fold would otherwise be
/// served as if the fold had no state.
fn refuse_feature_only_files(dir: &Path) -> Result<()> {
    if !cfg!(feature = "graph") && dir.join("graph/history.toml").exists() {
        bail!(
            "this nest declares graph/history.toml, the Graph history read policy, which only a \
             nuthatch built with `--features graph` honours"
        );
    }
    if !cfg!(feature = "folds") && dir.join("folds").exists() {
        bail!(
            "this nest ships folds/ (checkpointed folds, RFC-0059), which only a nuthatch built \
             with `--features folds` or `--features graph` loads"
        );
    }
    Ok(())
}

/// Ignoring the table would leave a nest that was screening every transfer quietly screening none.
fn refuse_removed_screening(raw: &str) -> Result<()> {
    let declared = raw
        .parse::<toml::Table>()
        .is_ok_and(|t| t.contains_key("screening"));
    if declared {
        bail!(
            "this nest declares [screening], and live sanctions screening was removed from nuthatch \
             on 2026-10-08 along with the WASM transform layer. Delete the [screening] table to load \
             it; sealed `sanction_hit` rows already indexed stay queryable from /sql"
        );
    }
    Ok(())
}

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

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub nest: Nest,
    /// RFC-0023 tier 3: archive endpoints for resolving declared `[[calls]]`, supplied by the
    /// operator at run time (`--state-rpc`), **never** written to `nuthatch.toml`.
    ///
    /// `#[serde(skip)]` is load-bearing, not tidiness. An archive endpoint almost always carries an
    /// API key, and `nuthatch.toml` is pinned into the nest blob and the content address - so a
    /// serialisable field here would bake a credential into the nest's identity and ship it to
    /// anyone the nest is shared with. Skipping it makes that impossible rather than discouraged.
    ///
    /// It rides on `Config` rather than a `build_nest` parameter because every layer already takes
    /// `&Config`; threading a parameter would have touched 37 call sites to carry one optional
    /// string.
    #[serde(skip)]
    pub state_rpc_urls: Vec<String>,
    /// The `serve` role: reads a store and sealed segments something else wrote, samples no calls and
    /// seals nothing, so it has no use for an archive endpoint and must not be refused for lacking
    /// one (#1167: `serve` over a `[[calls]]` nest exited telling the operator to pass a flag it does
    /// not have). Set by `serve_role` before `build_nest`; carried here for the same reason as
    /// `state_rpc_urls`, and `#[serde(skip)]` because a role is not data identity.
    #[serde(skip)]
    pub read_only_role: bool,
    /// How often the cursor asks for the tip, and whether it stops at finality (RFC-0040, #1173).
    /// Set from `--poll-interval` / `--finality-only`; `#[serde(skip)]` because how often an operator
    /// asks is not what the nest is: two nests differing only in cadence hold identical rows and must
    /// share one content address. Rides on `Config` for the same reason `state_rpc_urls` does.
    #[serde(skip)]
    pub freshness: crate::freshness::Freshness,
    /// How long a `--seal-direct` window waits for one `[[ipfs]]` document, zero for no limit (#1399).
    /// Set from `--ipfs-window-deadline`; `#[serde(skip)]` for the same reason as `freshness`.
    #[serde(skip, default = "crate::ipfs_resolve::window_deadline")]
    pub ipfs_window_deadline: std::time::Duration,
    /// The route a runtime serves this nest under (#1415). A runtime keys the nest's cursor, health and
    /// metrics by it, since two tenants' nests may share a name. `None` outside a runtime, where the
    /// nest's name is used. `#[serde(skip)]`: where a nest is mounted is not what it is.
    #[serde(skip)]
    pub route: Option<String>,
    /// RFC-0037: IPFS gateways (or a local node) for resolving declared `[[ipfs]]` documents,
    /// supplied at run time (`--ipfs`), **never** written to `nuthatch.toml`.
    ///
    /// `#[serde(skip)]` for a different reason than `state_rpc_urls`, and the reason matters: a
    /// gateway is an *access path*, not data identity. Two operators resolving the same CID through
    /// different gateways must get the same bytes - that is what content addressing means - so baking
    /// the gateway into the nest's content address would make the same data two different nests.
    ///
    /// Empty falls back to the public defaults. An operator who wants no third party points this at
    /// their own node, which is what non-negotiable 3 requires of every optional integration.
    #[serde(skip)]
    pub ipfs_gateways: Vec<String>,
    /// RFC-0037: declared IPFS resolutions. Absent → this nest never touches a gateway.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ipfs: Vec<crate::ipfs::IpfsDecl>,
    #[serde(default)]
    pub contracts: Vec<Contract>,
    /// Optional threshold & velocity flags (RFC-0008 C3). Absent → no flags, zero cost.
    #[serde(default, skip_serializing_if = "Flags::is_empty")]
    pub flags: Flags,
    /// Optional alert webhook sinks (RFC-0008 C5). Each routes annotations of the named kinds to a
    /// URL. Absent → no alerts. Delivery is at-least-once via a durable outbox; a stalled sink never
    /// blocks indexing.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub alerts: Vec<Alert>,
    /// Optional child-contract templates (RFC-0009). A template is an ABI applied to contracts
    /// discovered at runtime by a [`Factory`], rather than a fixed address. Absent → no factories.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub templates: Vec<Template>,
    /// Optional factory rules (RFC-0009): a watched contract's event announces a child contract to
    /// index with a template. Absent → static nest (no dynamic discovery).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub factories: Vec<Factory>,
    /// Optional user webhooks (RFC-0010 Part B): POST rows of an event table matching a predicate to
    /// a URL as they seal. Feeds the same host-side delivery engine as the compliance alerts.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub webhooks: Vec<Webhook>,
    /// Optional firehose-class extraction (RFC-0014): call traces and storage diffs beside event
    /// decode. Absent → events only, which is every nest today. **Declaring this does not make it
    /// run**: the only extraction source for traces/state is a colocated node (RFC-0003), so a nest
    /// that asks for it is refused at startup rather than served silently-empty tables. The config,
    /// decode and schema exist ahead of the source deliberately - see [`Extract`].
    #[serde(default, skip_serializing_if = "Extract::is_empty")]
    pub extract: Extract,
    /// Optional irreducible `eth_call` reads (RFC-0023 tier 3). Each is resolved at **pinned
    /// historical blocks** and content-addressed by `(chain, block, contract, calldata)`.
    ///
    /// Reach for this **last**. Tier 1's recipes derive most contract state from events already
    /// indexed - no RPC, no archive node, deterministic and free - and tier 2 caches immutable
    /// metadata. A `[[calls]]` entry is for what genuinely cannot be derived: an oracle read, an
    /// ungoverned parameter, a view on a contract whose events the nest does not fully cover.
    ///
    /// Declaring any of these means the nest **needs an archive RPC** for the range it backfills.
    /// That is the one external dependency tier 3 introduces, and it is why the derive-first tiers
    /// exist: they keep the zero-dependency default intact for everyone who does not need this.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub calls: Vec<crate::calls::CallDecl>,
    /// RFC-0063: watched accounts whose history the nest serves Etherscan-shaped at `/api`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address_history: Option<crate::address_history::AddressHistoryConfig>,
}

/// A user webhook subscription (RFC-0010 Part B): rows of `table` matching `where` are POSTed to `url`.
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct Webhook {
    pub name: String,
    /// The event table to watch, e.g. `staking__stake_delegated`.
    pub table: String,
    /// Optional SQL predicate over the table's columns (operator-authored, trusted like nest views).
    #[serde(default, rename = "where", skip_serializing_if = "Option::is_none")]
    pub where_clause: Option<String>,
    pub url: String,
    /// Max rows per delivery POST (default [`crate::webhooks::DEFAULT_BATCH_MAX`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_max: Option<usize>,
    /// `"sealed"` (default - never lies, finality-gated) or `"tip"` (fast, may send retractions).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finality: Option<String>,
    /// Where the cursor starts on first registration: `"registration"` (default - only rows sealed
    /// after the webhook is added, so a `--seal-direct` backfill doesn't fire history), `"genesis"`,
    /// or a block number.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<String>,
    /// Optional HMAC-SHA256 secret. When set, each delivery carries an `X-Nuthatch-Signature:
    /// sha256=<hex>` header over the POST body, so the receiver can verify it came from this nest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret: Option<String>,
}

/// One alert webhook sink: annotations whose kind is in `kinds` are POSTed to `url` (RFC-0008 C5).
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct Alert {
    /// Annotation kinds to deliver, e.g. `["threshold_flag", "entity_fault"]`.
    pub kinds: Vec<String>,
    /// The webhook endpoint. The operator configures it - it is the delivery allowlist (a sink only
    /// ever POSTs to the URLs a nest declares here).
    pub url: String,
    /// How the body is shaped for this sink.
    #[serde(default)]
    pub format: AlertFormat,
}

/// The body a sink expects.
///
/// **Discord will not take our payload.** Posting the outbox's own JSON to a Discord webhook is
/// answered `400 {"message": "Cannot send an empty message", "code": 50006}` - measured against a
/// real webhook, not inferred - while `{"content": "…"}` is answered `204`. Since the delivery
/// worker retries a non-success forever, wiring Discord up without this would give an outbox that
/// never drains and alerts that never arrive: an alerting system that fails silently, which is worse
/// than none.
///
/// An unrecognised value is a **load error**, not a fallback to `Raw`. Falling back would put the
/// nest straight into that silent-failure state for a typo.
#[derive(Debug, Serialize, Deserialize, Default, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AlertFormat {
    /// The outbox payload verbatim: `{event, kind, url, annotation}`. What every existing sink gets,
    /// and what an HMAC signature is computed over.
    #[default]
    Raw,
    /// `{"content": "…"}` - Discord, and anything else that speaks the same shape.
    Discord,
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
    /// The gate for building a [`crate::calldata::CallRegistry`] and advertising its tables. Distinct
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
    fn is_empty(&self) -> bool {
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

impl Config {
    pub fn load(dir: &Path) -> Result<Config> {
        // RFC-0018 §2's Starlark front-end was retired on 2026-07-21 (one graph nest, plain TOML) and
        // the loader is removed in 2.0. A directory still carrying `nest.star` is told so rather than
        // having it silently ignored - the file used to *take precedence*, so quietly skipping it
        // would change what a nest indexes without saying anything.
        if dir.join("nest.star").exists() {
            bail!(
                "{} has a nest.star, and the Starlark front-end was removed in 2.0 (RFC-0018 §2, \
                 retired 2026-07-21). Port it to nuthatch.toml - `nuthatch init` emits that shape, \
                 and it is what every other nest already uses.",
                dir.display()
            );
        }
        let path = dir.join(CONFIG_FILE);
        let raw = std::fs::read_to_string(&path).with_context(|| {
            format!(
                "no {CONFIG_FILE} in {} - run `nuthatch init` first",
                dir.display()
            )
        })?;
        // v2 first; fall back to migrating a v1 file.
        let cfg = match toml::from_str::<Config>(&raw) {
            Ok(cfg) => {
                for key in Self::unknown_keys(&raw) {
                    let hint = Self::suggest_key(&raw, &key)
                        .map(|k| format!(" (did you mean `{k}`?)"))
                        .unwrap_or_default();
                    tracing::warn!(
                        "{CONFIG_FILE}: `{key}` is not a key nuthatch reads, so it is ignored and \
                         its default applies{hint} - `nuthatch check` fails on it"
                    );
                }
                cfg
            }
            Err(v2_err) => Self::from_v1(&raw).map_err(|v1_err| {
                anyhow!("nuthatch.toml is neither v2 ({v2_err}) nor v1 ({v1_err})")
            })?,
        };
        cfg.check_schema_version()?;
        cfg.extract
            .l1_blocks_check(&cfg.nest.chain, cfg.nest.chain_id)?;
        // Declarations are validated at load, not at the first round trip: a typo'd address would
        // otherwise surface thousands of blocks into a backfill as a wall of identical failures.
        for c in &cfg.calls {
            c.validate()?;
        }
        for i in &cfg.ipfs {
            i.validate()?;
        }
        if let Some(h) = &cfg.address_history {
            h.validate()?;
        }
        let mut ipfs_tables = std::collections::HashSet::new();
        for t in cfg
            .ipfs
            .iter()
            .flat_map(|i| std::iter::once(&i.name).chain(i.rows.as_ref().map(|r| &r.table)))
        {
            if !ipfs_tables.insert(t) {
                bail!("[[ipfs]] table `{t}` is declared twice - each document and typed table needs its own name");
            }
        }
        cfg.refuse_tip_finality_webhooks()?;
        refuse_feature_only_files(dir)?;
        refuse_removed_screening(&raw)?;
        Ok(cfg)
    }

    /// Keys in a v2 `nuthatch.toml` that no field reads, as dotted paths (`contracts.0.start_blok`).
    /// Serde drops them, so a misspelling silently takes the default (#1582): `start_blok` backfilled
    /// from recent history instead of the declared block. Empty when the file does not parse as v2.
    pub fn unknown_keys(raw: &str) -> Vec<String> {
        unknown_paths::<Config>(raw)
    }

    /// The key nuthatch reads that `path` (an entry of [`Config::unknown_keys`]) most likely meant, if
    /// one spelling is one edit away (#1584). The parser is the oracle: each candidate is put in the
    /// typo's place and kept only if the file then reads it, so there is no list of keys to go stale.
    pub fn suggest_key(raw: &str, path: &str) -> Option<String> {
        suggest_path::<Config>(raw, path)
    }

    /// Parse and validate a nest **without** the serving-path policy refusals.
    ///
    /// `load` ends with `refuse_tip_finality_webhooks` (#577). That is right for
    /// anything that is about to *serve*: a declaration that parses and then
    /// silently does nothing is the failure this project can least afford.
    /// A declared `[[calls]]` block used to be refused the same way, until
    /// tier 3 shipped in v2.6.0 and `load` started accepting it.
    ///
    /// They are wrong for a diagnostic. `nuthatch doctor` is what an operator
    /// reaches for when a nest misbehaves, and it refused to run on any nest
    /// carrying a forward-looking declaration — reporting, on a nest that was
    /// present and parsed cleanly, that there was no nest there (#582). The
    /// tool that tells you what is wrong should be the last thing to refuse.
    ///
    /// Everything that makes the file *readable* still applies: the nest.star
    /// removal, the v1/v2 fallback, the schema version, and per-call validation.
    /// Only the "we will not run this" policy is skipped.
    pub fn load_for_diagnostics(dir: &Path) -> Result<Config> {
        let cfg = Self::load(dir);
        match cfg {
            Ok(c) => Ok(c),
            Err(e) => {
                // A policy refusal is not a reason a diagnostic cannot run, so
                // re-read past it. Anything else is a real load failure and is
                // returned unchanged.
                let reparsed = Self::parse_only(dir);
                match reparsed {
                    Ok(c) => Ok(c),
                    Err(_) => Err(e),
                }
            }
        }
    }

    /// The readable-file half of `load`: everything except the policy refusals.
    fn parse_only(dir: &Path) -> Result<Config> {
        if dir.join("nest.star").exists() {
            bail!(
                "{} has a nest.star, and the Starlark front-end was removed in 2.0 (RFC-0018 §2, \
                 retired 2026-07-21). Port it to nuthatch.toml - `nuthatch init` emits that shape, \
                 and it is what every other nest already uses.",
                dir.display()
            );
        }
        let path = dir.join(CONFIG_FILE);
        let raw = std::fs::read_to_string(&path).with_context(|| {
            format!(
                "no {CONFIG_FILE} in {} - run `nuthatch init` first",
                dir.display()
            )
        })?;
        let cfg = match toml::from_str::<Config>(&raw) {
            Ok(cfg) => cfg,
            Err(v2_err) => Self::from_v1(&raw).map_err(|v1_err| {
                anyhow!("nuthatch.toml is neither v2 ({v2_err}) nor v1 ({v1_err})")
            })?,
        };
        cfg.check_schema_version()?;
        for c in &cfg.calls {
            c.validate()?;
        }
        Ok(cfg)
    }

    /// Refuse a `[[webhooks]]` entry declaring `finality = "tip"` while `deliver_sealed` is the only
    /// delivery path that exists (issue #577).
    ///
    /// `finality` parses fine either way and `Webhook::finality` is a bare `Option<String>`, so a
    /// typo'd or aspirational `"tip"` would otherwise be accepted, start clean, and never deliver a
    /// single row - the exact "accepted, validated, and then ignored forever" shape that a declared
    /// `[[calls]]` block used to be until tier 3 shipped (#262). A load error costs an operator one
    /// clear message; silent non-delivery costs them finding out from a consumer.
    fn refuse_tip_finality_webhooks(&self) -> Result<()> {
        let tip_webhooks: Vec<&str> = self
            .webhooks
            .iter()
            .filter(|w| w.finality.as_deref() == Some("tip"))
            .map(|w| w.name.as_str())
            .collect();
        if tip_webhooks.is_empty() {
            return Ok(());
        }
        bail!(
            "webhook(s) {} declare `finality = \"tip\"`, and nuthatch cannot deliver on it yet - \
             see issue #577. Only the sealed path (`finality = \"sealed\"`, the default) delivers \
             today; a tip webhook would load cleanly and then never fire, silently, forever. Set \
             `finality = \"sealed\"` (or omit it) until tip delivery ships.",
            tip_webhooks.join(", "),
        )
    }

    /// Reader and feature gates shared by normal and diagnostic loading.
    fn check_schema_version(&self) -> Result<()> {
        // Too new for us: the nest was authored by a later nuthatch and may mean things by fields we
        // do not know. This is the guard that makes `init --from` safe.
        if self.nest.schema_version > READABLE_SCHEMA_VERSION {
            if self.nest.schema_version == GRAPH_SCHEMA_VERSION {
                bail!(
                    "this nest uses config schema v{GRAPH_SCHEMA_VERSION} (canonical calls or \
                     `on_any` triggers), which only a nuthatch built with `--features graph` reads"
                );
            }
            bail!(
                "this nest needs config schema v{} but this nuthatch supports up to v{} - upgrade nuthatch",
                self.nest.schema_version,
                READABLE_SCHEMA_VERSION
            );
        }
        // Too *old* for what it declares - the mirror case, and the dangerous one. A file claiming v1
        // while declaring a v2-only feature is exactly the file an older binary would accept and get
        // wrong. We cannot fix that binary; we can refuse to be the source of such a file, and anyone
        // hand-editing `block_timestamps` finds out here rather than from a nest that quietly
        // disagrees with itself on someone else's machine.
        let need = required_schema_version(self.nest.block_timestamps);
        if self.nest.schema_version < need {
            bail!(
                "this nest declares `block_timestamps = {}`, which needs `schema_version = {}` \
                 (it says {}). Older nuthatch builds don't know the field and would index timestamps \
                 into a nest built without them; the version is what makes them refuse instead.",
                self.nest.block_timestamps,
                need,
                self.nest.schema_version
            );
        }
        if self.calls.iter().any(|call| call.canonical) && self.nest.schema_version < 3 {
            bail!(
                "canonical calls need `schema_version = 3` (it says {}). Older nuthatch builds \
                 ignore `canonical` and would read state by block number instead of block hash; \
                 the version is what makes them refuse instead.",
                self.nest.schema_version
            );
        }
        if self.calls.iter().any(|call| !call.on_any.is_empty()) && self.nest.schema_version < 3 {
            bail!(
                "`on_any` triggers need `schema_version = 3` (it says {}). Older nuthatch builds \
                 ignore `on_any`, so those calls would never fire and store nothing; the version \
                 is what makes them refuse instead.",
                self.nest.schema_version
            );
        }
        Ok(())
    }

    fn from_v1(raw: &str) -> Result<Config> {
        #[derive(Deserialize)]
        struct V1 {
            chain: String,
            chain_id: u64,
            address: String,
            rpc_urls: Vec<String>,
        }
        let v1: V1 = toml::from_str(raw)?;
        Ok(Config {
            state_rpc_urls: Vec::new(),
            read_only_role: false,
            freshness: Default::default(),
            ipfs_window_deadline: crate::ipfs_resolve::WINDOW_DEADLINE,
            route: None,
            ipfs_gateways: Vec::new(),
            ipfs: Vec::new(),
            nest: Nest {
                name: "nest".to_string(),
                chain: v1.chain,
                chain_id: v1.chain_id,
                rpc_urls: v1.rpc_urls,
                schema_version: CURRENT_SCHEMA_VERSION,
                block_timestamps: true,
            },
            contracts: vec![Contract {
                alias: "c0".to_string(),
                address: v1.address,
                start_block: None,
                abi: ABI_FILE.to_string(),
                events: Vec::new(),
            }],
            flags: Flags::default(),
            alerts: Vec::new(),
            templates: Vec::new(),
            factories: Vec::new(),
            webhooks: Vec::new(),
            extract: Extract::default(),
            calls: Vec::new(),
            address_history: None,
        })
    }

    pub fn save(&self, dir: &Path) -> Result<()> {
        let path = dir.join(CONFIG_FILE);
        let raw = toml::to_string_pretty(self).context("failed to serialise config")?;
        std::fs::write(&path, raw)
            .with_context(|| format!("failed to write {}", path.display()))?;
        Ok(())
    }

    /// The first contract - the indexer's single-contract path uses this until step 3 generalises
    /// decode + storage to every contract in the nest.
    pub fn primary(&self) -> Result<&Contract> {
        self.contracts
            .first()
            .ok_or_else(|| anyhow!("nest has no contracts"))
    }
}

/// Keys in `raw` that no field of `T` reads, as dotted paths. Empty when `raw` does not parse:
/// a file that fails to parse has a louder error than a key it would have ignored (#1582).
pub fn unknown_paths<T>(raw: &str) -> Vec<String>
where
    T: for<'de> Deserialize<'de>,
{
    let mut unknown = Vec::new();
    let parsed: Result<T, _> = serde_ignored::deserialize(toml::Deserializer::new(raw), |path| {
        unknown.push(path.to_string());
    });
    if parsed.is_err() {
        return Vec::new();
    }
    unknown
}

/// The field `path` most likely meant, if one spelling is one edit away (#1584). The parser is the
/// oracle: the candidate is kept only when `T` then reads it.
pub fn suggest_path<T>(raw: &str, path: &str) -> Option<String>
where
    T: for<'de> Deserialize<'de>,
{
    let doc: toml::Value = toml::from_str(raw).ok()?;
    let (parent, typo) = path.rsplit_once('.').unwrap_or(("", path));
    let mut candidates: Vec<String> = one_edit_away(typo).into_iter().collect();
    candidates.sort();
    candidates.into_iter().find(|candidate| {
        let mut doc = doc.clone();
        let Some(table) = table_at(&mut doc, parent) else {
            return false;
        };
        if table.contains_key(candidate.as_str()) {
            return false;
        }
        let Some(value) = table.remove(typo) else {
            return false;
        };
        table.insert(candidate.clone(), value);
        let Ok(text) = toml::to_string(&doc) else {
            return false;
        };
        let at = if parent.is_empty() {
            candidate.clone()
        } else {
            format!("{parent}.{candidate}")
        };
        toml::from_str::<T>(&text).is_ok() && !unknown_paths::<T>(&text).contains(&at)
    })
}

/// A running process says that a key was dropped. `check` is what fails on it (#1582, #1656).
pub fn warn_unknown<T>(file: &str, raw: &str)
where
    T: for<'de> Deserialize<'de>,
{
    for key in unknown_paths::<T>(raw) {
        let hint = suggest_path::<T>(raw, &key)
            .map(|k| format!(" (did you mean `{k}`?)"))
            .unwrap_or_default();
        tracing::warn!(
            "{file}: `{key}` is not a key nuthatch reads, so it is ignored and its default \
             applies{hint} - `nuthatch check` fails on it"
        );
    }
}

/// `check`'s failure for the same keys. Absent file is fine: not every directory has every file.
pub fn refuse_unknown_file<T>(dir: &Path, file: &str) -> Result<()>
where
    T: for<'de> Deserialize<'de>,
{
    let Ok(raw) = std::fs::read_to_string(dir.join(file)) else {
        return Ok(());
    };
    let unknown = unknown_paths::<T>(&raw);
    if unknown.is_empty() {
        return Ok(());
    }
    let named: Vec<String> = unknown
        .iter()
        .map(|k| match suggest_path::<T>(&raw, k) {
            Some(s) => format!("{k} (did you mean `{s}`?)"),
            None => k.clone(),
        })
        .collect();
    bail!(
        "{file} has key(s) nuthatch does not read, so their defaults apply instead: {}. Check \
         the spelling against the config reference.",
        named.join(", ")
    )
}

/// The table at a dotted path such as `contracts.0`, where a numeric segment indexes an array.
fn table_at<'a>(doc: &'a mut toml::Value, path: &str) -> Option<&'a mut toml::value::Table> {
    let mut at = doc;
    for seg in path.split('.').filter(|s| !s.is_empty()) {
        at = match at {
            toml::Value::Table(t) => t.get_mut(seg)?,
            toml::Value::Array(a) => a.get_mut(seg.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    at.as_table_mut()
}

/// Every spelling one edit from `word`: a deletion, a swap of neighbours, a replacement or an insertion,
/// over the characters a TOML key here is written in.
fn one_edit_away(word: &str) -> std::collections::BTreeSet<String> {
    const ALPHABET: &str = "abcdefghijklmnopqrstuvwxyz0123456789_";
    let w: Vec<char> = word.chars().collect();
    let mut out = std::collections::BTreeSet::new();
    for i in 0..w.len() {
        out.insert(w[..i].iter().chain(&w[i + 1..]).collect());
        if i + 1 < w.len() {
            let mut s = w.clone();
            s.swap(i, i + 1);
            out.insert(s.into_iter().collect());
        }
    }
    for i in 0..=w.len() {
        for c in ALPHABET.chars() {
            if i < w.len() {
                let mut s = w.clone();
                s[i] = c;
                out.insert(s.into_iter().collect());
            }
            let mut s = w.clone();
            s.insert(i, c);
            out.insert(s.into_iter().collect());
        }
    }
    out.remove(word);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #1582: a misspelt key is reported, and nothing a real config says, or a parsed one writes
    /// back, is. A false positive would make every nest warn and `check` fail.
    #[test]
    fn only_keys_nothing_reads_are_unknown() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let mut seen = 0;
        for rel in ["docs/bench/nests/usdc-120", "obib-case2", "obib-case3"] {
            let raw = std::fs::read_to_string(root.join(rel).join(CONFIG_FILE)).unwrap();
            assert_eq!(Config::unknown_keys(&raw), Vec::<String>::new(), "{rel}");
            let parsed: Config = toml::from_str(&raw).unwrap();
            let written = toml::to_string(&parsed).unwrap();
            assert_eq!(
                Config::unknown_keys(&written),
                Vec::<String>::new(),
                "{rel} written back"
            );
            seen += 1;
            if rel == "docs/bench/nests/usdc-120" {
                let typo = raw.replacen("start_block", "start_blok", 1);
                assert_ne!(typo, raw, "premise: the fixture declares start_block");
                let unknown = Config::unknown_keys(&typo);
                assert_eq!(unknown.len(), 1, "{unknown:?}");
                assert!(unknown[0].ends_with("start_blok"), "{unknown:?}");
                // #1584: the parser, asked, names the key the typo meant.
                assert_eq!(
                    Config::suggest_key(&typo, &unknown[0]).as_deref(),
                    Some("start_block")
                );
                let swapped = raw.replacen("block_timestamps", "block_timstamps", 1);
                assert_ne!(
                    swapped, raw,
                    "premise: the fixture declares block_timestamps"
                );
                let unknown = Config::unknown_keys(&swapped);
                assert!(
                    unknown.iter().any(|k| k.ends_with("block_timstamps")),
                    "{unknown:?}"
                );
                let k = unknown
                    .iter()
                    .find(|k| k.ends_with("block_timstamps"))
                    .unwrap();
                assert_eq!(
                    Config::suggest_key(&swapped, k).as_deref(),
                    Some("block_timestamps")
                );
                let nonsense = raw.replacen("start_block", "zzzzzz", 1);
                let unknown = Config::unknown_keys(&nonsense);
                assert_eq!(Config::suggest_key(&nonsense, &unknown[0]), None);
            }
        }
        assert_eq!(seen, 3);
    }

    #[test]
    fn migrates_a_v1_file() {
        let v1 = r#"
            chain = "mainnet"
            chain_id = 1
            address = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
            rpc_urls = ["https://rpc.example"]
            event = "Transfer"
        "#;
        let cfg = Config::from_v1(v1).unwrap();
        assert_eq!(cfg.nest.chain, "mainnet");
        assert_eq!(cfg.nest.chain_id, 1);
        assert_eq!(cfg.contracts.len(), 1);
        assert_eq!(cfg.contracts[0].alias, "c0");
        assert_eq!(cfg.contracts[0].abi, ABI_FILE);
        assert!(cfg.contracts[0].start_block.is_none());
    }

    #[test]
    fn roundtrips_a_v2_file() {
        let cfg = Config {
            state_rpc_urls: Vec::new(),
            read_only_role: false,
            freshness: Default::default(),
            ipfs_window_deadline: crate::ipfs_resolve::WINDOW_DEADLINE,
            route: None,
            ipfs_gateways: Vec::new(),
            ipfs: Vec::new(),
            nest: Nest {
                name: "my-nest".into(),
                chain: "mainnet".into(),
                chain_id: 1,
                rpc_urls: vec!["https://rpc.example".into()],
                schema_version: CURRENT_SCHEMA_VERSION,
                block_timestamps: true,
            },
            contracts: vec![
                Contract {
                    alias: "usdc".into(),
                    address: "0xaaaa".into(),
                    start_block: Some(6_082_465),
                    abi: "abis/usdc.json".into(),
                    events: Vec::new(),
                },
                Contract {
                    alias: "weth".into(),
                    address: "0xbbbb".into(),
                    start_block: None,
                    abi: "abis/weth.json".into(),
                    events: vec!["Transfer".into()],
                },
            ],
            flags: Flags::default(),
            alerts: Vec::new(),
            templates: Vec::new(),
            factories: Vec::new(),
            webhooks: Vec::new(),
            extract: Extract::default(),
            calls: Vec::new(),
            address_history: None,
        };
        let raw = toml::to_string_pretty(&cfg).unwrap();
        let back: Config = toml::from_str(&raw).unwrap();
        assert_eq!(back.contracts.len(), 2);
        assert_eq!(back.contracts[0].start_block, Some(6_082_465));
        assert_eq!(back.contracts[1].start_block, None);
        // The per-contract event allowlist (RFC-0011) round-trips; an empty one stays empty.
        assert!(back.contracts[0].events.is_empty());
        assert_eq!(back.contracts[1].events, vec!["Transfer".to_string()]);
        assert_eq!(back.primary().unwrap().alias, "usdc");
    }

    // ---- RFC-0014 `[extract]` and the volume guard ----------------------------------------------

    fn extract_of(toml_src: &str) -> Extract {
        let cfg: Config = toml::from_str(toml_src).expect("parses");
        cfg.extract
    }

    const BASE: &str = r#"
[nest]
name = "n"
chain = "mainnet"
chain_id = 1
rpc_urls = ["https://rpc.example"]
"#;

    #[test]
    fn a_nest_without_extract_has_none_and_serialises_none() {
        let cfg: Config = toml::from_str(BASE).unwrap();
        assert!(cfg.extract.is_empty());
        assert!(!cfg.extract.enabled());
        let raw = toml::to_string_pretty(&cfg).unwrap();
        assert!(
            !raw.contains("[extract]"),
            "an unused section must not appear in a nest file: {raw}"
        );
    }

    /// The guard's whole point. An unscoped extraction nest is unbounded by *chain traffic*, not by
    /// anything the operator wrote, so it is refused rather than warned about.
    #[test]
    fn unscoped_extraction_is_refused() {
        for src in [
            "[extract]\ntraces = true\n",
            "[extract]\nstate = true\n",
            "[extract]\ntraces = true\nstate = true\n",
        ] {
            let e = extract_of(&format!("{BASE}{src}"));
            let err = match e.scope_check() {
                Err(err) => err,
                Ok(()) => panic!("{src} must not be accepted unscoped"),
            };
            assert!(
                err.to_string().contains("unbounded by construction"),
                "the refusal must say why: {err}"
            );
        }
    }

    #[test]
    fn a_contract_scope_satisfies_the_guard() {
        let e = extract_of(&format!(
            "{BASE}[extract]\ntraces = true\nstate = true\ncontracts = [\"usdc\"]\n"
        ));
        assert!(e.is_scoped());
        assert!(e.scope_check().is_ok());
    }

    /// A selector allowlist bounds *traces*. It says nothing about storage writes, so it must not be
    /// accepted as scoping for a nest whose state half is still chain-wide.
    #[test]
    fn a_selector_allowlist_scopes_traces_but_not_state() {
        let traces_only = extract_of(&format!(
            "{BASE}[extract]\ntraces = true\nselectors = [\"0xa9059cbb\"]\n"
        ));
        assert!(traces_only.scope_check().is_ok());

        let with_state = extract_of(&format!(
            "{BASE}[extract]\ntraces = true\nstate = true\nselectors = [\"0xa9059cbb\"]\n"
        ));
        assert!(
            with_state.scope_check().is_err(),
            "selectors cannot bound storage writes, so they must not appear to"
        );
    }

    #[test]
    fn unbounded_is_the_deliberate_escape_hatch() {
        let e = extract_of(&format!(
            "{BASE}[extract]\ntraces = true\nunbounded = true\n"
        ));
        assert!(e.scope_check().is_ok());
    }

    /// A guard that only fires when extraction is on: an `[extract]` section with both surfaces off
    /// is inert config, not a refusal.
    #[test]
    fn the_guard_is_silent_when_extraction_is_off() {
        let e = extract_of(&format!("{BASE}[extract]\ncontracts = [\"usdc\"]\n"));
        assert!(!e.enabled());
        assert!(e.scope_check().is_ok());
    }

    #[test]
    fn selectors_are_validated_rather_than_silently_ignored() {
        let good = extract_of(&format!(
            "{BASE}[extract]\ntraces = true\nselectors = [\"0xa9059cbb\", \"095ea7b3\"]\n"
        ));
        assert_eq!(
            good.selector_keys().unwrap(),
            vec![[0xa9, 0x05, 0x9c, 0xbb], [0x09, 0x5e, 0xa7, 0xb3]],
            "the 0x prefix is optional, as it is everywhere else in the config"
        );

        // A typo'd selector that filtered nothing would look like it worked - the worst kind of bug.
        let short = extract_of(&format!("{BASE}[extract]\nselectors = [\"0xa905\"]\n"));
        assert!(short.selector_keys().unwrap_err().to_string().contains("4"));
        let nonhex = extract_of(&format!("{BASE}[extract]\nselectors = [\"0xzzzzzzzz\"]\n"));
        assert!(nonhex
            .selector_keys()
            .unwrap_err()
            .to_string()
            .contains("not hex"));
    }

    #[test]
    fn extract_survives_a_round_trip() {
        let src = format!(
            "{BASE}[extract]\ntraces = true\nstate = true\ncontracts = [\"usdc\"]\nselectors = [\"0xa9059cbb\"]\n"
        );
        let cfg: Config = toml::from_str(&src).unwrap();
        let back: Config = toml::from_str(&toml::to_string_pretty(&cfg).unwrap()).unwrap();
        assert!(back.extract.traces && back.extract.state);
        assert_eq!(back.extract.contracts, vec!["usdc".to_string()]);
        assert_eq!(back.extract.selectors, vec!["0xa9059cbb".to_string()]);
    }

    /// A timestamped nest is stamped **v1**, not "whatever version this build is".
    ///
    /// Getting this wrong is invisible until someone tries to open a freshly-scaffolded nest with the
    /// previous release and is told to upgrade for no reason. The version describes the file's
    /// requirements; only `block_timestamps = false` actually needs a v2 reader.
    #[test]
    fn only_a_timestamp_free_nest_raises_the_schema_version() {
        assert_eq!(required_schema_version(true), 1);
        assert_eq!(required_schema_version(false), 2);
        assert!(
            required_schema_version(false) <= CURRENT_SCHEMA_VERSION,
            "this build must be able to read what it writes"
        );
    }

    #[test]
    fn canonical_calls_require_a_reader_that_understands_hash_pinning() {
        let raw = include_str!("../tests/fixtures/network-nest/nuthatch.toml");
        let dir = tempfile::tempdir().unwrap();
        for version in [1, 2, 3] {
            let mut cfg: Config = toml::from_str(raw).unwrap();
            cfg.nest.schema_version = version;
            std::fs::write(dir.path().join(CONFIG_FILE), toml::to_string(&cfg).unwrap()).unwrap();
            for result in [
                Config::load(dir.path()),
                Config::load_for_diagnostics(dir.path()),
            ] {
                if version < 3 {
                    let error = result.unwrap_err().to_string();
                    assert!(error.contains("canonical"), "{error}");
                    assert!(error.contains("schema_version = 3"), "{error}");
                } else if cfg!(feature = "graph") {
                    assert!(result.unwrap().calls.iter().all(|call| call.canonical));
                } else {
                    // A default build refuses the nest and names what would read it.
                    let error = result.unwrap_err().to_string();
                    assert!(error.contains("--features graph"), "{error}");
                }
            }
        }
        let mut cfg: Config = toml::from_str(raw).unwrap();
        cfg.nest.schema_version = 1;
        for call in &mut cfg.calls {
            call.canonical = false;
            if let Some(first) = call.on_any.first().cloned() {
                call.on = Some(first);
                call.on_any.clear();
            }
        }
        cfg.check_schema_version().unwrap();
    }

    /// An older reader ignores `on_any`, so its calls would never fire; v3 is what makes it refuse.
    #[test]
    fn on_any_triggers_need_schema_v3() {
        let raw = include_str!("../tests/fixtures/network-nest/nuthatch.toml");
        let mut cfg: Config = toml::from_str(raw).unwrap();
        assert!(cfg.calls.iter().any(|call| !call.on_any.is_empty()));
        for call in &mut cfg.calls {
            call.canonical = false;
        }
        cfg.nest.schema_version = 2;
        let error = cfg.check_schema_version().unwrap_err().to_string();
        assert!(error.contains("`on_any`"), "{error}");
        cfg.nest.schema_version = 3;
        if cfg!(feature = "graph") {
            cfg.check_schema_version().unwrap();
        }
    }

    /// #687: `load` used to call `refuse_unwired_calls`. Claiming that it still does, after the
    /// function was deleted, is the same class as an RFC header that still says tier 3 is pending.
    #[test]
    fn load_comments_do_not_claim_to_call_refuse_unwired_calls() {
        let src = include_str!("config.rs");
        let prod = src.split("#[cfg(test)]").next().unwrap();
        assert!(
            !prod.contains("load` ends with `refuse_unwired_calls"),
            "`load` docs still name a function it does not call"
        );
    }

    /// A webhook declaring `finality = "tip"` is refused rather than accepted and silently never
    /// delivered (issue #577 - the same shape as #262's `[[calls]]` refusal above).
    #[test]
    fn a_tip_finality_webhook_is_refused_rather_than_silently_ignored() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            r#"
[nest]
name = "n"
chain = "mainnet"
chain_id = 1
rpc_urls = ["https://rpc.example"]
schema_version = 1

[[contracts]]
alias = "c"
address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
abi = "abis/c.json"

[[webhooks]]
name = "fast"
table = "c__transfer"
url = "http://127.0.0.1:8099/"
finality = "tip"
"#,
        )
        .unwrap();
        let err = format!("{:#}", Config::load(dir.path()).unwrap_err());
        assert!(err.contains("#577"), "point at the issue: {err}");
        assert!(err.contains("fast"), "name the offending webhook: {err}");
        assert!(
            err.contains("sealed"),
            "a refusal should name the working alternative: {err}"
        );
    }

    #[test]
    fn a_screening_table_is_refused_by_name() {
        let dir = tempfile::tempdir().unwrap();
        let base = r#"
[nest]
name = "t"
chain = "mainnet"
chain_id = 1
rpc_urls = ["https://rpc.example"]
"#;
        let path = dir.path().join(CONFIG_FILE);
        std::fs::write(&path, base).unwrap();
        Config::load(dir.path()).expect("the same nest without [screening] loads");

        std::fs::write(&path, format!("{base}\n[screening]\nlists = [\"ab\"]\n")).unwrap();
        let err = format!("{:#}", Config::load(dir.path()).unwrap_err());
        assert!(err.contains("[screening]"), "name the table: {err}");
        assert!(err.contains("removed"), "say why: {err}");
    }

    /// The default (`finality` omitted) and the explicit `"sealed"` both load clean - only `"tip"`
    /// is refused.
    #[test]
    fn sealed_finality_webhooks_load_clean() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            r#"
[nest]
name = "n"
chain = "mainnet"
chain_id = 1
rpc_urls = ["https://rpc.example"]
schema_version = 1

[[contracts]]
alias = "c"
address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
abi = "abis/c.json"

[[webhooks]]
name = "default"
table = "c__transfer"
url = "http://127.0.0.1:8099/"

[[webhooks]]
name = "explicit"
table = "c__transfer"
url = "http://127.0.0.1:8099/"
finality = "sealed"
"#,
        )
        .unwrap();
        Config::load(dir.path()).expect("sealed (default or explicit) must load");
    }

    #[test]
    fn l1_blocks_is_refused_off_arbitrum_and_beside_blocks() {
        let load = |chain: &str, chain_id: u64, extract: &str| {
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(
                dir.path().join(CONFIG_FILE),
                format!(
                    "[nest]\nname = \"n\"\nchain = \"{chain}\"\nchain_id = {chain_id}\n\
                     rpc_urls = [\"https://rpc.example\"]\n\n[extract]\n{extract}\n"
                ),
            )
            .unwrap();
            Config::load(dir.path()).map_err(|e| e.to_string())
        };
        assert!(load("arbitrum-one", 42161, "l1_blocks = true").is_ok());
        assert!(load("robinhood", 4663, "l1_blocks = true").is_ok());
        assert!(load("some-orbit-chain", 999_999_001, "l1_blocks = true").is_ok());
        let err = load("mainnet", 1, "l1_blocks = true").unwrap_err();
        assert!(err.contains("carry no `l1BlockNumber`"), "{err}");
        let err = load("arbitrum-one", 42161, "l1_blocks = true\nblocks = true").unwrap_err();
        assert!(err.contains("already holds"), "{err}");
    }

    #[test]
    fn l1_blocks_off_is_not_written_back() {
        let out = toml::to_string(&Extract {
            blocks: true,
            ..Default::default()
        })
        .unwrap();
        assert!(!out.contains("l1_blocks"), "{out}");
    }

    #[test]
    fn a_v1_file_cannot_declare_block_timestamps_false() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            r#"
[nest]
name = "n"
chain = "mainnet"
chain_id = 1
rpc_urls = ["https://rpc.example"]
schema_version = 1
block_timestamps = false
"#,
        )
        .unwrap();
        let err = Config::load(dir.path()).unwrap_err().to_string();
        assert!(
            err.contains("schema_version = 2"),
            "must say which version it needs: {err}"
        );

        // The same file at v2 loads.
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            r#"
[nest]
name = "n"
chain = "mainnet"
chain_id = 1
rpc_urls = ["https://rpc.example"]
schema_version = 2
block_timestamps = false
"#,
        )
        .unwrap();
        let cfg = Config::load(dir.path()).unwrap();
        assert!(!cfg.nest.block_timestamps);
    }

    /// RFC-0063: a rotki-mode nest watches accounts and declares no contract, and must still load.
    #[test]
    fn an_address_history_nest_with_no_contracts_loads() {
        let dir = tempfile::tempdir().unwrap();
        let nest = r#"
[nest]
name = "n"
chain = "mainnet"
chain_id = 1
rpc_urls = ["https://rpc.example"]
schema_version = 2
"#;
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            format!(
                "{nest}\n[address_history]\naddresses = [\"0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045\"]\n"
            ),
        )
        .unwrap();
        let cfg = Config::load(dir.path()).unwrap();
        assert!(cfg.contracts.is_empty());
        let h = cfg.address_history.expect("[address_history] read");
        assert_eq!(
            h.poll_interval().unwrap(),
            crate::address_history::DEFAULT_POLL_INTERVAL
        );
        assert!(Config::unknown_keys(
            &std::fs::read_to_string(dir.path().join(CONFIG_FILE)).unwrap()
        )
        .is_empty());

        std::fs::write(
            dir.path().join(CONFIG_FILE),
            format!("{nest}\n[address_history]\naddresses = [\"0xD8dA6BF26964aF9D7eEd9e03E53415D37aA96045\"]\n"),
        )
        .unwrap();
        let err = format!("{:#}", Config::load(dir.path()).unwrap_err());
        assert!(err.contains("checksum"), "{err}");
    }

    /// A pre-slice-4 nest - no `schema_version`, no `block_timestamps` - still loads as v1 with
    /// timestamps on. This is every nest in existence today, so it had better keep working.
    #[test]
    fn a_nest_written_before_any_of_this_still_loads_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            r#"
[nest]
name = "n"
chain = "mainnet"
chain_id = 1
rpc_urls = ["https://rpc.example"]
"#,
        )
        .unwrap();
        let cfg = Config::load(dir.path()).unwrap();
        assert_eq!(cfg.nest.schema_version, 1);
        assert!(cfg.nest.block_timestamps);
    }

    /// RFC-0035 §2: the Starlark front-end is removed in 2.0, and a `nest.star` is **refused** rather
    /// than ignored.
    ///
    /// Silently skipping it would be the dangerous option: `nest.star` used to take *precedence* over
    /// `nuthatch.toml`, so a nest carrying both would quietly start indexing something different from
    /// what it indexed yesterday, with no error to explain why.
    #[test]
    fn a_nest_star_is_refused_rather_than_ignored() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("nest.star"), "# computed config\n").unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            "[nest]\nname = \"t\"\nchain = \"mainnet\"\nchain_id = 1\nrpc_urls = []\n",
        )
        .unwrap();

        let err = Config::load(dir.path()).unwrap_err().to_string();
        assert!(err.contains("removed in 2.0"), "{err}");
        assert!(
            err.contains("nuthatch.toml"),
            "the refusal must name the way forward: {err}"
        );

        // Without the `.star`, the same directory loads normally.
        std::fs::remove_file(dir.path().join("nest.star")).unwrap();
        assert_eq!(Config::load(dir.path()).unwrap().nest.name, "t");
    }

    /// RFC-0060 §5.6: a default build refuses the Graph history policy by name rather than serving
    /// latest-only answers without the freshness limit the policy promises.
    #[test]
    fn a_graph_history_policy_needs_a_graph_build() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            "[nest]\nname = \"t\"\nchain = \"mainnet\"\nchain_id = 1\nrpc_urls = []\n",
        )
        .unwrap();
        std::fs::create_dir_all(dir.path().join("graph")).unwrap();
        std::fs::write(dir.path().join("graph/history.toml"), "version = 1\n").unwrap();

        let result = Config::load(dir.path());
        if cfg!(feature = "graph") {
            assert_eq!(result.unwrap().nest.name, "t");
        } else {
            let err = result.unwrap_err().to_string();
            assert!(err.contains("graph/history.toml"), "{err}");
            assert!(err.contains("--features graph"), "{err}");
        }
    }
}
