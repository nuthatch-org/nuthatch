//! **The runtime**: one process hosting 1..N nests, with tenancy handled here rather than by a
//! container above it (RFC-0032).
//!
//! ## The roost is gone, and this is not a rename of it
//!
//! A *roost* was a **container** - a directory of nest directories that an operator chose between
//! (`dev` or `roost dev`) before they knew which they wanted. It was retired in 2.0, not renamed.
//! What is left is the runtime: the process itself, which hosts one nest or many with no ceremony
//! difference, and which owns multi-tenancy directly.
//!
//! So the types here are named for what they *are*, not for what they replaced:
//!
//! - [`MountTable`] is the file (`mounts.toml`), i.e. **the mount records plus the runtime settings**.
//!   It is not "the runtime" - the runtime is the process reading it.
//! - [`RuntimeMeta`] is the `[runtime]` section: name, chain(s), RPC endpoints, the RSS ceiling, the
//!   default tenant.
//! - [`ChainCursor`] is deliberately per-**chain**: the single-cursor law holds per chain, and a
//!   cursor is never per-runtime. A second chain is a second cursor (RFC-0021).
//!
//! ## What the runtime owns
//!
//! - **Mounts, keyed by `(tenant, NID)`.** A tenant is an opaque string with a real default, so
//!   single-tenant is `N=1` rather than a special case and nobody who does not want tenancy types the
//!   word. Two tenants mounting the same nest share **one dataset**; it is never indexed twice.
//! - **One isolated cursor per distinct chain** (`group_by_chain` → a `spawn_runtime` each), each
//!   with its own tip, finality view and reorg boundary, held to a per-cursor RSS budget.
//! - **Blast radius.** One nest's fault quarantines that nest; one cursor's death does not take its
//!   siblings (RFC-0026). Stores are always per-dataset and never shared across chains.
//!
//! Historically this began as RFC-0012's multi-nest runtime: a shared cursor fanning one `getLogs`
//! per window out to the owning nests, so N nests cost one nest's worth of RPC chatter, with per-nest
//! tables byte-identical to running each nest solo. That mechanism is unchanged; only the container
//! and the key around it went.

use crate::config::Config;
use crate::indexer;
use crate::rpc::{self, RpcClient};
use crate::source::Source;
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The **mount table**, at the runtime directory root (RFC-0032 §4).
///
/// MountTable state that the runtime owns, not authored config an operator maintains. It records which
/// alias, for which tenant, serves which nest identity - and nothing else. `mounts.toml` was its name
/// until 2.0, when the runtime was retired as a concept; a directory still carrying the old file is
/// pointed at `nuthatch migrate` rather than silently ignored.
pub const MOUNTS_FILE: &str = "mounts.toml";

/// The pre-2.0 name for [`MOUNTS_FILE`]. Retained **only** so a directory that has not been migrated
/// can be recognised and named in an error, and so `migrate` can read it.
pub const LEGACY_ROOST_FILE: &str = "roost.toml";

/// Where mounted nests live under the runtime dir: `nests/<name>/` is a nest directory, exactly as a
/// standalone nest is today.
///
/// **Pre-2.0 layout.** RFC-0032 replaces it with [`DATA_DIR`] keyed by nest identity; this constant
/// stays for un-migrated roosts and for `migrate`'s source side.
pub const NESTS_DIR: &str = "nests";

/// Where a nest's inputs and data live once addressed by identity: `data/<nid>/` (RFC-0032 §4).
///
/// The directory does not know what it is called or who mounted it - [`Mount`] records hold all of
/// that. That separation is the whole point: it is what lets two mounts name one dataset.
pub const DATA_DIR: &str = "data";

/// A nest identity, validated: 64 hex characters, exactly what [`crate::blob::Manifest::nid`]
/// produces and what a `mounts.toml` record has always been required to hold (the SEC-10 rule
/// `validate_mounts` applies below).
///
/// The only constructor is [`Nid::parse`], so a `Nid` in hand has already passed the same check a
/// path segment gets. That matters at exactly one boundary: [`RuntimeHandles::mount`] takes an
/// `Option<Nid>`, not `Option<&str>`, closing the admin surface's `POST /_admin/nests`. The
/// caller's `nid` used to reach [`MountTable::data_dir`] with nothing in between, and `Path::join`
/// treats an absolute string as a full replacement, not a suffix. A raw string cannot reach that
/// function without going through `parse` first; a guard at the handler alone would only have
/// closed the one call site this fix was written against.
///
/// Internal `nid: String` fields (`Mount`, `PreparedDataset`, `migrate`'s records) stay untyped:
/// they are populated either from `blob::nest_nid`'s own hash output, which is valid by
/// construction, or from a `mounts.toml` that `validate_mounts` already gated at load. Retyping all
/// of those is real follow-up work, not required to close the reachable path an admin caller has.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Nid(String);

impl Nid {
    pub fn parse(nid: &str) -> Result<Self> {
        if nid.len() != 64 || !nid.bytes().all(|b| b.is_ascii_hexdigit()) {
            bail!("nid '{nid}' is invalid (expected 64 hex characters)");
        }
        // Lower case is what `nest_nid` prints; one dataset must not get a second directory (#2137).
        Ok(Self(nid.to_ascii_lowercase()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl std::fmt::Display for Nid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// One mount record: an alias, and the identity of the nest whose dataset it serves (RFC-0032 §4).
///
/// **Slice 1 carries `alias` and `nid` only.** The `tenant` column and refcount semantics land on
/// this struct in slices 2-3 rather than beside it, so the growth is additive.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Mount {
    /// Who mounted it (RFC-0032 §6). An **opaque string** - nuthatch refcounts it and knows nothing
    /// else about it. No authz, no quotas, no metering; identity stays the gateway's job.
    ///
    /// Always a real value, never `Option<String>` and never null. Single-tenant is `N=1` with
    /// [`DEFAULT_TENANT`], not a special case, so there is one code path rather than two - and the
    /// one almost every user is on is the one that would otherwise rot.
    /// [`UNSAID_TENANT`] only inside deserialisation; [`MountTable`]'s `Deserialize` fills in the
    /// configured default, which a serde default on this field cannot see. A written `tenant = ""`
    /// is kept, so validation still refuses it.
    #[serde(default = "unsaid_tenant")]
    pub tenant: String,
    /// The name this mount is served under. Free-form, unique **within a tenant**, and *not* part of
    /// the nest's identity.
    pub alias: String,
    /// The nest identity ([`crate::blob::Manifest::nid`]) - the dataset key.
    pub nid: String,
    /// How much SQL surface this mount exposes (RFC-0034 §2). **Mount config, not manifest**, so
    /// changing it leaves the NID untouched, re-indexes nothing, and lets two tenants share one
    /// dataset while exposing different surfaces.
    #[serde(default)]
    pub sql: crate::allowlist::SqlAccess,
    /// The queries this mount answers by name, when `sql = "allowlist"`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub queries: Vec<crate::allowlist::NamedQuery>,
    /// Mirror this mount's dataset to a prefix as it seals (RFC-0052 S2). Mount config, so setting it
    /// moves neither the NID nor the data.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publish: Option<MountPublish>,
    /// Optional x402 price at this operator-owned mount. It exists only in a binary built with the
    /// off-by-default `counter` feature; an ordinary self-hosting build remains unpriced (#1217).
    #[cfg(feature = "counter")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub counter: Option<crate::counter::Config>,
}

impl Mount {
    /// This mount's SQL surface.
    pub fn surface(&self) -> crate::allowlist::Surface {
        crate::allowlist::Surface {
            access: self.sql,
            queries: self.queries.clone(),
        }
    }
}

/// `[mounts.publish]`: where and how often a mount's dataset is mirrored (RFC-0052 S2).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MountPublish {
    /// A directory, or `s3://bucket/prefix` with the usual `AWS_*` env.
    pub target: String,
    #[serde(default = "default_publish_interval")]
    pub interval: String,
    #[serde(default = "default_publish_parallelism")]
    pub parallelism: usize,
}

fn default_publish_interval() -> String {
    "60s".to_string()
}

fn default_publish_parallelism() -> usize {
    crate::publish::DEFAULT_PARALLELISM
}

impl MountPublish {
    pub fn settings(&self) -> Result<crate::publish::Settings> {
        if self.target.trim().is_empty() {
            bail!("publish target is empty");
        }
        let interval = crate::freshness::parse_span(&self.interval)
            .map_err(|e| anyhow::anyhow!("publish interval {:?}: {e}", self.interval))?;
        if interval.is_zero() {
            bail!("the publish interval must be at least one second");
        }
        if !(1..=16).contains(&self.parallelism) {
            bail!(
                "publish parallelism must be between 1 and 16, not {}",
                self.parallelism
            );
        }
        Ok(crate::publish::Settings {
            target: self.target.clone(),
            interval,
            parallelism: self.parallelism,
        })
    }
}

// A `[[mounts]]` record that names no tenant belongs to the configured default, the same as a legacy
// `nests` entry, so the two forms cannot disagree (RFC-0032 §6).
impl<'de> Deserialize<'de> for MountTable {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let mut table = MountTable::deserialize(d)?;
        let tenant = table.tenant_default();
        for m in &mut table.mounts {
            if m.tenant == UNSAID_TENANT {
                m.tenant = tenant.clone();
            }
        }
        Ok(table)
    }
}

impl Serialize for MountTable {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        MountTable::serialize(self, s)
    }
}

/// The tenant a mount belongs to when nobody said otherwise. Operator-configurable per mounts via
/// `[mounts] default_tenant`.
pub const DEFAULT_TENANT: &str = "default";

// A NUL cannot come from a TOML author by accident and is refused as a path segment anyway.
const UNSAID_TENANT: &str = "\u{0}";

fn unsaid_tenant() -> String {
    UNSAID_TENANT.to_string()
}

fn default_tenant() -> String {
    DEFAULT_TENANT.to_string()
}

/// One mount's coordinates: who mounted it, and what they call it (RFC-0032 §4, §7).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountRef {
    pub tenant: String,
    pub alias: String,
}

impl std::fmt::Display for MountRef {
    /// Always tenant-qualified, unlike [`MountRef::route_key`]. Logs and errors must stay
    /// unambiguous even in a single-tenant runtime whose routes omit the tenant.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.tenant, self.alias)
    }
}

impl MountRef {
    /// The path this mount is served under (RFC-0032 §7), and the key its health and footprint are
    /// recorded against.
    ///
    /// The default tenant's mounts are served by alias and every other tenant's as `tenant/alias`,
    /// each decided by the mount alone. It used to depend on how many tenants the table held, so a
    /// restart after a second tenant arrived moved routes nobody had touched (#1565).
    pub fn route_key(&self, default_tenant: &str) -> String {
        if self.tenant == default_tenant {
            self.alias.clone()
        } else {
            format!("{}/{}", self.tenant, self.alias)
        }
    }
}

/// A mounts manifest: the mounted nests plus the chain(s) they follow. A mounts may host nests across
/// **one or more chains** (RFC-0021) - one isolated cursor per distinct chain. The single-chain form
/// keeps the top-level `chain`/`chain_id`/`rpc_urls`; a multichain runtime lists its chains under
/// `[[chains]]` and lets each nest declare its own chain. The single-cursor law holds **per chain**:
/// never multiplex two chains behind one cursor.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(remote = "Self")]
pub struct MountTable {
    /// Serialised as `[runtime]`. Was `[roost]` until 2.0, when the roost was retired as a concept
    /// (RFC-0032 slice 5); `migrate` rewrites the section along with everything else.
    #[serde(rename = "runtime", alias = "roost")]
    pub runtime: RuntimeMeta,
    /// Multichain: each chain the runtime serves, with its own RPC endpoints (RFC-0021). Mutually
    /// exclusive with the top-level `chain`/`chain_id`. Empty → the single-chain top-level form.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub chains: Vec<ChainEndpoint>,
    /// Mount records (RFC-0032 §4): which alias serves which nest identity. Empty means this directory
    /// has not been migrated and still resolves nests by name under [`NESTS_DIR`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mounts: Vec<Mount>,
}

/// One chain the runtime follows, plus how to reach it - a cursor's substrate (RFC-0021).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChainEndpoint {
    pub chain: String,
    pub chain_id: u64,
    #[serde(default)]
    pub rpc_urls: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeMeta {
    /// Human name for the runtime (logging/roster only).
    pub name: String,
    /// Single-chain form: the one chain the cursor follows. Omit (with `chain_id`) for a multichain
    /// mounts that declares its chains under `[[chains]]` instead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain: Option<String>,
    /// Single-chain form: the one chain id. Omit for a multichain runtime.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain_id: Option<u64>,
    /// Single-chain form: RPC endpoints for the one chain. Overridable at runtime with `--rpc`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rpc_urls: Vec<String>,
    /// The mounted nests, by directory name under `nests/`. Superseded by `[[mounts]]` once the
    /// mount table is migrated (RFC-0032 §4) - see [`MountTable::mount_refs`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nests: Vec<String>,
    /// The tenant a mount belongs to when it does not say (RFC-0032 §6). Operator-configurable so a
    /// single-tenant deployment can call its tenant whatever it likes; absent → [`DEFAULT_TENANT`].
    /// Never `None` in effect - [`MountTable::tenant_default`] always yields a real string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_tenant: Option<String>,
    /// Resident-set ceiling **per active-chain cursor**, in MB (RFC-0021 - the footprint budget is
    /// per-cursor; a runtime's total is Σ cursors). A cursor whose *projected* RSS exceeds this is refused
    /// before it starts. Absent → the CLAUDE.md 2 GB budget ([`DEFAULT_MAX_RSS_MB`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_rss_mb: Option<u64>,
    /// Route keys of mounts suspended by the operator (#1548). Their records and data are kept; they
    /// are not indexed or served until resumed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub suspended: Vec<String>,
}

/// SEC-10: anything that becomes a filesystem path segment *and* a route segment gets this charset.
///
/// Tenants and aliases are both. A tenant being **opaque** to nuthatch (RFC-0032 §6) means we never
/// interpret it, not that it may contain `..` or a separator - opacity is about meaning, not about
/// what a path resolver will do with it.
fn safe_segment(value: &str, what: &str) -> Result<()> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        bail!("{what} '{value}' is invalid (allowed: letters, digits, '_', '-')");
    }
    Ok(())
}

/// What a move appends to a name while the new nest catches up. Valid under [`safe_segment`], so a
/// restart mid-move boots, and reserved: an operator's own names may not end in it.
pub const STAGING_SUFFIX: &str = "__moving";

/// A mount name as the admin API accepts it: `alias` or `tenant/alias`, each part as boot accepts it
/// and at most 64 characters. Anything boot would refuse is refused here, since a name the API
/// accepted is persisted, and the next start would fail on it.
pub fn check_mount_name(name: &str, default_tenant: &str) -> Result<()> {
    let refuse = |why: String| Err(MountRefusal::InvalidName(why).into());
    let (tenant, alias) = match name.split('/').collect::<Vec<_>>().as_slice() {
        [alias] => (None, *alias),
        [tenant, alias] => (Some(*tenant), *alias),
        _ => return refuse(format!("mount name '{name}' has more than one '/'")),
    };
    for (what, part) in tenant
        .map(|t| ("tenant", t))
        .into_iter()
        .chain([("alias", alias)])
    {
        if part.len() > 64 {
            return refuse(format!(
                "{what} '{}…' is longer than 64 characters",
                part.chars().take(16).collect::<String>()
            ));
        }
        if let Err(e) = safe_segment(part, what) {
            return refuse(format!("{e:#}"));
        }
    }
    if alias.ends_with(STAGING_SUFFIX) {
        return refuse(format!(
            "alias '{alias}' ends in '{STAGING_SUFFIX}', which a move reserves"
        ));
    }
    if tenant == Some(default_tenant) {
        return refuse(format!(
            "'{name}' names the default tenant; mount it as '{alias}'"
        ));
    }
    // Boot refuses these. Accepting one here persists it, and the next start stays down (#1640).
    if let Err(e) = refuse_reserved_mount(tenant, alias) {
        return refuse(format!("{e:#}"));
    }
    Ok(())
}

/// Whether a first path segment is the runtime's own: a root route, which a nested mount would
/// overlap and panic `Router::nest` (#2130), or `_admin`, which shadows the nest's routes (#1673).
fn reserved_route(segment: &str) -> bool {
    segment == "_admin" || crate::serve::RUNTIME_ROOT_ROUTES.contains(&segment)
}

fn refuse_reserved_mount(tenant: Option<&str>, alias: &str) -> Result<()> {
    if reserved_route(alias) {
        bail!("nest name '{alias}' is reserved (collides with a runtime route)");
    }
    if let Some(tenant) = tenant.filter(|part| reserved_route(part)) {
        bail!("tenant '{tenant}' is reserved (collides with a runtime route)");
    }
    Ok(())
}

/// The routes one runtime serves, checked as a set, so what `Router::nest` would panic on is refused
/// instead. A tenant is the first segment of its mounts' routes as an alias is of its own (#2130).
fn refuse_route_collisions<'a>(routes: impl IntoIterator<Item = &'a str>) -> Result<()> {
    let routes: std::collections::BTreeSet<&str> = routes.into_iter().collect();
    for route in &routes {
        let (tenant, alias) = split_route_key(route);
        refuse_reserved_mount(tenant, alias)?;
        if let Some(tenant) = tenant.filter(|t| routes.contains(t)) {
            bail!(
                "tenant '{tenant}' is also the name of a mount, so '{route}' would collide with that \
                 mount's routes"
            );
        }
    }
    Ok(())
}

/// The default per-cursor RSS ceiling: the CLAUDE.md ≤2 GB budget (RFC-0021 - now per active-chain
/// cursor, not per whole runtime).
pub const DEFAULT_MAX_RSS_MB: u64 = 2048;

// A deliberately rough, *honest* per-runtime footprint model (RFC-0012 §3). These are order-of-
// magnitude estimates for the pre-mount projection, not measurements - the roster reports the real
// `rss_bytes()` alongside so an operator can calibrate. The shared serving/runtime cost is paid once;
// each nest adds its hot-store working set + decode registry, plus a chunk per active IVM view.
//
// NEST_BASE_RSS_MB is fitted against the 20-nest Uniswap V4 PoolManager scenario
// (docs/bench/multinest-footprint.json): peak_rss=139 MB on a 120 MB base → ~1 MB/nest observed.
// 5 MB/nest gives a 5x safety margin above that observation; the IVM view terms are untested in
// that scenario and remain conservative. The known limitation: the dominant cost at high event
// rates is the near-tip window fan-out (a cursor-level term), not per-nest fixed cost, so a
// workload with a single very-high-rate nest will underrun the projection and one with many
// very-high-rate nests may overrun it. The per-cursor RAM budget job (CI) measures actual RSS
// against a ceiling and is the primary guard; this projection is the pre-mount admission check.
pub const RUNTIME_BASE_RSS_MB: u64 = 120; // serving + async runtime + on-demand Burrmill, paid once
const NEST_BASE_RSS_MB: u64 = 5; // fitted: ~1 MB/nest observed, 5x margin (see comment above)
const NEST_VIEW_RSS_MB: u64 = 40; // each extra load: exposure view, velocity view, or child registry

/// Bytes per admitted input row for an authored incremental entity (RFC-0041 §7, criterion 9).
///
/// **Measured, 2026-08-26**, with #837's corrected instrument against the captured Horizon corpus on
/// a 32-core Debian box: 889 input rows, empty-circuit RSS 77,452 KB, circuit peak 80,164 KB, so
/// `(80,164 - 77,452) x 1024 / 889 = 3,123` bytes/row. Rounded up, because an admission estimate that
/// is too low admits a mount which then breaches the budget at runtime, and too high merely refuses
/// one that would have fitted.
///
/// **Not the 183,885 the slice-zero document published.** That was `peak - fixed` across a window
/// spanning the DuckDB normalise scan, with the baseline sampled after the scan released - so it was
/// mostly the scan's transient divided by the entity's row count. The same run now reports that scan
/// separately at 247,556 KB, **3.1x the circuit's own peak**, which is what was being charged to the
/// entity. The difference is not academic: at 183,885 bytes/row the 2 GB cursor budget holds ~11,700
/// entity rows, and at 3,123 it holds ~688,000.
///
/// Still an estimate rather than a promise, and **conservative by construction**: at 889 rows the
/// fixed allocations - arena blocks, batch buffers, the trace's own bookkeeping - are a large share
/// of the 2,712 KB delta, so dividing them by 889 charges each row for a cost that does not repeat.
/// A larger corpus would amortise them and report *fewer* bytes per row. For an admission bound that
/// is the safe direction: it refuses a mount that would have fitted rather than admitting one that
/// then breaches the budget.
///
/// It is data-size driven over a fixed corpus rather than core-count driven, which is the property
/// that made the multi-nest regression figure agree across machines - but criterion 12's artifact is
/// a *cursor* measurement, not this one.
///
/// **Corroborated 2026-08-27 at real scale** (`tests/seed_scale.rs` at `ede03ca7`, against a Horizon nest on the
/// ThinkPad), which is what the 889-row caveat above was asking for. Two shapes, same 346,288
/// sealed source rows:
///
/// | relation | maintained rows | bytes/row |
/// |---|---|---|
/// | one key column, one aggregate | 309,549 | **940** |
/// | two key columns, three aggregates | 322,357 | **1,482** |
///
/// So the amortised figure is 940-1,482 rather than 3,123, and the constant is between 2.2x and 3.4x
/// it. Shape matters, as the per-row charge cannot see: a wider relation costs more per row, and
/// 1,482 is the widest measured rather than the widest possible.
///
/// **Deliberately not lowered.** The margin costs an operator headroom - at 3,200 a cursor's 2 GB
/// admits ~688,000 rows where the widest measured shape would fit ~1.48M - and that trade is worth
/// making explicitly rather than as a side effect of one afternoon's measurement on one machine.
/// Note also that half the old figure was two copies of the relation, one in the circuit and one in
/// `Published`; #897 removed the second, so these numbers are not comparable to any taken before it.
pub(crate) const ENTITY_RSS_BYTES_PER_ROW: u64 = 3_200;

/// What one authored entity's circuit and thread cost, before any of its declared rows.
///
/// **Measured 2026-08-26** on a 32-core Debian box, the same dense scenario the per-cursor budget is
/// stated in terms of: 20 nests on one cursor peaked at **138 MB with no entities and 209 MB with
/// one per nest** - 71 MB for twenty circuits, **3.5 MB each**.
///
/// Charged at 8, not 3.5. The margin is deliberate and it is doing real work: this is one run on one
/// machine, and an admission figure set too low admits a mount that then breaches the budget at
/// runtime, which is the only direction that actually hurts.
///
/// **It was `NEST_VIEW_RSS_MB` (40) until that measurement.** Reusing the built-in views' allowance
/// looked reasonable - an entity is a DBSP circuit on a thread exactly as they are - and it was 11x
/// the real cost. At 40 MB, twenty entities eat 43% of a cursor's 2 GB before a single row and
/// thirty-two eat 70%, so a realistic multi-nest runtime met refusals the hardware would have
/// handled comfortably. The first entity footprint run was refused for precisely that reason.
const ENTITY_CIRCUIT_RSS_MB: u64 = 8;

/// The most authored entities one cursor will admit, across every nest on that chain.
///
/// A count ceiling on top of the MB budget, because the two bound different things. An entity costs
/// a DBSP circuit **and an OS thread**, and the MB projection prices the state rather than the
/// thread - so a hundred entities each declaring a hundred rows projects as almost nothing and is a
/// hundred threads on one cursor. §7 asks for an explicit ceiling; this is it, and it is deliberately
/// generous rather than fitted, since the MB budget is what should bind first on any realistic nest.
const MAX_ENTITIES_PER_CURSOR: usize = 32;

/// Rough projected RSS (MB) for one nest: base + a chunk per active IVM view / factory child registry.
/// `has_labels` gates the exposure view (only spun up when the nest has labeled addresses).
/// RFC-0041 §7 criterion 11: the explicit per-cursor ceiling on authored entities.
///
/// Its own function so the arithmetic in the message is testable. A refusal whose numbers are wrong
/// is worse than no message: it sends an operator to change the thing it names.
fn refuse_over_entity_ceiling(
    on_cursor: usize,
    nests: usize,
    mounts: &str,
    chain: &str,
) -> Result<()> {
    if on_cursor <= MAX_ENTITIES_PER_CURSOR {
        return Ok(());
    }
    Err(MountRefusal::EntityCeiling {
        mounts: mounts.to_string(),
        chain: chain.to_string(),
        on_cursor,
        nests,
    }
    .into())
}

/// What a nest's declared entities cost the admission calculation: how many there are, and the sum
/// of their declared `max_rows`.
///
/// A nest that declares none, or whose manifest will not parse, contributes nothing here. The
/// manifest is *validated* at startup by `entities::load`, which refuses a malformed one loudly;
/// admission is not the place to raise that a second time, and treating a typo as "no entities" for
/// budgeting only would let a nest be admitted on a projection it does not match.
pub fn declared_entities(dir: &Path) -> (usize, u64) {
    let declared = crate::entities::load(dir).unwrap_or_default();
    (
        declared.len(),
        declared
            .iter()
            .map(|e| {
                // An entity reading an offchain table may also hold `max_rows` of offchain input
                // (#1437), which `EntityView::admit_offchain` enforces at runtime.
                let reads_offchain = e
                    .read_sql(dir)
                    .and_then(|sql| crate::entity_lower::lower_with_columns(&sql))
                    .is_ok_and(|(plan, _)| crate::entity_offchain::reads_offchain(&plan));
                e.max_rows as u64 * if reads_offchain { 2 } else { 1 }
            })
            .sum(),
    )
}

/// `entity_rows` is the sum of every authored entity's declared `max_rows` for this nest, and
/// `entity_count` how many there are (RFC-0041 §7). Passed in rather than read from disk here so
/// this stays a pure function of what the mount declares - the same reason `has_labels` is a bool.
pub fn estimate_nest_rss_mb(
    config: &Config,
    has_labels: bool,
    entity_count: usize,
    entity_rows: u64,
) -> u64 {
    let mut mb = NEST_BASE_RSS_MB;
    // Each entity is a DBSP circuit on its own thread, plus its declared state - the term the
    // built-in views do not let an author choose, and therefore the one that can surprise a budget.
    mb += ENTITY_CIRCUIT_RSS_MB * entity_count as u64;
    mb += entity_rows.saturating_mul(ENTITY_RSS_BYTES_PER_ROW) / (1024 * 1024);
    if has_labels {
        mb += NEST_VIEW_RSS_MB; // exposure view (RFC-0008 C1)
    }
    if config.flags.velocity().is_some() {
        mb += NEST_VIEW_RSS_MB; // velocity view (RFC-0008 C3)
    }
    if !config.factories.is_empty() {
        mb += NEST_VIEW_RSS_MB; // discovered-child registry (RFC-0009)
    }
    mb
}

impl MountTable {
    /// Load and validate `mounts.toml` from a runtime directory.
    pub fn load(dir: &Path) -> Result<MountTable> {
        Self::load_inner(dir, false)
    }

    /// Load for **migration**, accepting the pre-2.0 file.
    ///
    /// [`MountTable::load`] refuses a directory still holding `mounts.toml` and points at `nuthatch
    /// migrate`. `migrate` therefore cannot use it: the one command that fixes the problem would
    /// refuse to run *because* the problem exists. This is that door, and it is the only caller.
    pub fn load_for_migration(dir: &Path) -> Result<MountTable> {
        Self::load_inner(dir, true)
    }

    fn load_inner(dir: &Path, accept_legacy: bool) -> Result<MountTable> {
        let legacy = dir.join(LEGACY_ROOST_FILE);
        let path = if accept_legacy && !dir.join(MOUNTS_FILE).exists() && legacy.exists() {
            legacy
        } else {
            dir.join(MOUNTS_FILE)
        };
        // A directory still carrying the pre-2.0 file is a *migration that has not been run*, not a
        // missing file. Saying so beats "no mounts.toml", which sends an operator looking for a file
        // they never had.
        if !accept_legacy && !path.exists() && dir.join(LEGACY_ROOST_FILE).exists() {
            bail!(
                "{} holds a pre-2.0 {LEGACY_ROOST_FILE}. The roost was retired in 2.0: run \
                 `nuthatch migrate --dir {}` to move its data to identity-keyed datasets and write a \
                 {MOUNTS_FILE}. It moves data and never re-indexes; `--dry-run` prints the plan first.",
                dir.display(),
                dir.display()
            );
        }
        let raw = std::fs::read_to_string(&path)
            .with_context(|| format!("no {MOUNTS_FILE} in {}", dir.display()))?;
        let mounts: MountTable =
            toml::from_str(&raw).with_context(|| format!("parsing {}", path.display()))?;
        // `sq1 = "deny"` would otherwise leave /sql open with no record that the key was dropped.
        crate::config::warn_unknown::<MountTable>(
            path.file_name()
                .and_then(|s| s.to_str())
                .unwrap_or(MOUNTS_FILE),
            &raw,
        );
        mounts.validate_mounts()?;
        // Every mount, whichever form declared it, must be a safe path segment and must not collide
        // with a reserved top-level route - the roster and the per-nest prefixes share one namespace.
        let mut seen = std::collections::HashSet::new();
        for m in mounts.mount_refs() {
            safe_segment(&m.alias, "nest name")?;
            safe_segment(&m.tenant, "tenant")?;
            // In a multi-tenant runtime the tenant is the first path segment, so it collides too.
            refuse_reserved_mount(Some(&m.tenant), &m.alias)?;
            if !seen.insert((m.tenant.clone(), m.alias.clone())) {
                bail!("tenant '{}' mounts '{}' more than once", m.tenant, m.alias);
            }
        }
        let default_tenant = mounts.tenant_default();
        let routes: Vec<String> = mounts
            .mount_refs()
            .iter()
            .map(|m| m.route_key(&default_tenant))
            .collect();
        refuse_route_collisions(routes.iter().map(String::as_str))?;
        Ok(mounts)
    }

    /// Validate the mount records (RFC-0032 §4).
    ///
    /// A NID is a **filesystem path segment**, exactly as a nest name is, so it gets the same SEC-10
    /// treatment: hex only, fixed length, no `..`, no separators. The records are written by
    /// `migrate` rather than by hand, but a runtime dir is an operator-editable file and a bundle
    /// roster is untrusted input, so the check is on load, not on write.
    fn validate_mounts(&self) -> Result<()> {
        // The primary key is `(tenant, alias)`: two tenants may each call their mount "usdc" and
        // both must work, so uniqueness is *within* a tenant and never global (RFC-0032 §4.1).
        let mut seen = std::collections::HashSet::new();
        for m in &self.mounts {
            let nid = Nid::parse(&m.nid)
                .with_context(|| format!("mount '{}' has an invalid nest identity", m.alias))?;
            // Refused rather than lowered: the record names a directory, which on Linux is that spelling.
            if nid.as_str() != m.nid {
                bail!(
                    "mount '{}' names nid {} in upper case; nids are lower case. Write it as {nid} \
                     (and rename data/{} to match, if that directory exists)",
                    m.alias,
                    m.nid,
                    m.nid
                );
            }
            // SEC-10, again: a tenant is a path segment now, so it gets the same charset as an alias.
            // It is opaque to nuthatch, which is not the same as being allowed to contain `../`.
            safe_segment(&m.tenant, "tenant")?;
            safe_segment(&m.alias, "nest name")?;
            m.surface().validate(&m.alias)?;
            #[cfg(feature = "counter")]
            if let Some(counter) = &m.counter {
                counter.seller().with_context(|| {
                    format!("mount '{}': invalid counter configuration", m.alias)
                })?;
                if m.sql != crate::allowlist::SqlAccess::Allowlist {
                    bail!(
                        "mount '{}' configures a counter but is not `sql = \"allowlist\"`; \
                         only declared queries are priceable",
                        m.alias
                    );
                }
            }
            if let Some(publish) = &m.publish {
                publish
                    .settings()
                    .with_context(|| format!("mount '{}': invalid [mounts.publish]", m.alias))?;
            }
            if !seen.insert((&m.tenant, &m.alias)) {
                bail!("tenant '{}' mounts '{}' more than once", m.tenant, m.alias);
            }
        }
        Ok(())
    }

    /// Every mount this runtime serves, in serving order (RFC-0032 §4).
    ///
    /// **`[[mounts]]` first, then any `nests` entry no record covers.** The records are the only
    /// form that can express `(acme, usdc)` and `(globex, usdc)` as two distinct mounts, which a
    /// flat list of names cannot - but the list is not simply ignored, because a **half-migrated**
    /// mounts is a supported state and `migrate` produces one whenever it refuses a nest. Dropping
    /// the uncovered names would silently unmount exactly the nests that failed to migrate.
    pub fn mount_refs(&self) -> Vec<MountRef> {
        let mut out: Vec<MountRef> = self
            .mounts
            .iter()
            .map(|m| MountRef {
                tenant: m.tenant.clone(),
                alias: m.alias.clone(),
            })
            .collect();
        let tenant = self.tenant_default();
        for alias in &self.runtime.nests {
            if !self.mounts.iter().any(|m| &m.alias == alias) {
                out.push(MountRef {
                    tenant: tenant.clone(),
                    alias: alias.clone(),
                });
            }
        }
        out
    }

    /// The tenant a mount with no explicit one belongs to. Operator-configurable, defaulted, never
    /// absent.
    pub fn tenant_default(&self) -> String {
        self.runtime
            .default_tenant
            .clone()
            .unwrap_or_else(default_tenant)
    }

    /// Whether more than one tenant is present, which is what decides if routes carry a tenant
    /// segment (RFC-0032 §7). With one tenant the segment is ceremony, so today's URLs do not move.
    pub fn is_multi_tenant(&self) -> bool {
        let mut tenants = self.mount_refs().into_iter().map(|m| m.tenant);
        let Some(first) = tenants.next() else {
            return false;
        };
        tenants.any(|t| t != first)
    }

    /// The on-disk directory of a mounted nest, in the pre-2.0 name-keyed layout.
    ///
    /// Prefer [`MountTable::dir_for`], which consults the mount records first. This stays for un-migrated
    /// roosts and as `migrate`'s source side.
    pub fn nest_dir(dir: &Path, name: &str) -> PathBuf {
        dir.join(NESTS_DIR).join(name)
    }

    /// A dataset's directory, keyed by nest identity (RFC-0032 §4).
    pub fn data_dir(dir: &Path, nid: &str) -> PathBuf {
        dir.join(DATA_DIR).join(nid)
    }

    /// Is `dir` an identity-keyed dataset - `<root>/data/<nid>` - rather than an authored nest?
    ///
    /// The path *is* the identity under RFC-0032 §4, which is why this can be answered from the shape
    /// alone; `migrate`'s `adoptable` already enumerates datasets by the same rule (a 64-character
    /// directory name under [`DATA_DIR`]).
    ///
    /// The distinction matters because a dataset's bytes are hashed into the NID its mount record
    /// claims. Anything that rewrites a file in here - even a derived one - silently invalidates that
    /// claim, so the layer that regenerates artifacts for *authored* nests must not run here.
    pub fn is_identity_keyed(dir: &Path) -> bool {
        let looks_like_nid = |n: &std::ffi::OsStr| {
            let n = n.to_string_lossy();
            n.len() == 64 && n.chars().all(|c| c.is_ascii_hexdigit())
        };
        dir.file_name().is_some_and(looks_like_nid)
            && dir
                .parent()
                .and_then(|p| p.file_name())
                .is_some_and(|p| p == DATA_DIR)
    }

    /// Datasets whose inputs no longer hash to the identity their mount record claims (RFC-0032 §3).
    ///
    /// Content addressing is only worth anything if somebody checks it. Before this, nothing did: the
    /// runtime resolved `data/<nid>` straight out of the mount record and never recomputed it, so
    /// editing a file under a mounted dataset left the same directory, the same data, no re-index -
    /// and a record whose `nid` was simply false. Every mechanism keyed on that value (sharing,
    /// refcounting, `prune`, adoption) was then reasoning from a stale identity.
    ///
    /// `migrate`'s `nid_of` has always refused a nest whose inputs no longer reproduce its packed
    /// manifest. This is the same check on the serving path, where it was missing.
    ///
    /// **Reported per dataset, not per mount.** Two tenants sharing one drifted dataset is one fault,
    /// not two - and the canonical mount is the one that indexes it.
    ///
    /// Un-migrated nests (`nests/<alias>`, no `nid` in the record) are skipped: they claim no
    /// identity, so there is nothing to contradict.
    pub fn identity_drift(&self, dir: &Path) -> Vec<IdentityDrift> {
        let mut out = Vec::new();
        for ds in self.datasets(dir) {
            let Some(claimed) = ds.nid.clone() else {
                continue;
            };
            match crate::blob::nest_nid(&ds.dir) {
                Ok(actual) if actual == claimed => {}
                Ok(actual) => out.push(IdentityDrift {
                    alias: ds.canonical().to_string(),
                    dir: ds.dir.clone(),
                    claimed,
                    actual: Some(actual),
                }),
                // Unreadable inputs are a different fault and `Config::load` reports it with a better
                // message a moment later. Recording it here as "cannot be verified" keeps this
                // function total, and a dataset we cannot hash is not one we should claim is intact.
                Err(_) => out.push(IdentityDrift {
                    alias: ds.canonical().to_string(),
                    dir: ds.dir.clone(),
                    claimed,
                    actual: None,
                }),
            }
        }
        out
    }

    /// The distinct **datasets** this runtime mounts, each with every alias serving it (RFC-0032 §4).
    ///
    /// This is the function that makes sharing real. Two aliases naming one nest identity are one
    /// dataset: one store, one place in the cursor, **one backfill**. Iterating `nests` directly
    /// instead - the pre-2.0 shape - would open the same store twice and index the same chain data
    /// twice, which is the entire cost this design exists to remove.
    ///
    /// Order is deterministic: datasets in first-alias order, aliases within a dataset in `nests`
    /// order. `aliases[0]` is the canonical mount - the one that indexes.
    pub fn datasets(&self, dir: &Path) -> Vec<Dataset> {
        let mut out: Vec<Dataset> = Vec::new();
        for m in self.mount_refs() {
            let nid = self
                .mounts
                .iter()
                .find(|r| r.tenant == m.tenant && r.alias == m.alias)
                .map(|r| r.nid.clone());
            let path = match &nid {
                Some(nid) => Self::data_dir(dir, nid),
                None => Self::nest_dir(dir, &m.alias),
            };
            match out.iter_mut().find(|d| d.dir == path) {
                Some(d) => d.mounts.push(m),
                None => out.push(Dataset {
                    dir: path,
                    mounts: vec![m],
                    nid,
                }),
            }
        }
        out
    }

    /// Where `alias` is served from: `data/<nid>` when a mount record exists, else the pre-2.0
    /// `nests/<alias>`.
    ///
    /// Both layouts resolve through this one function so a half-migrated mounts - some nests adopted,
    /// a new one dropped into `nests/` - is a supported state rather than an accident.
    ///
    /// Alias-only, so it resolves the **first** tenant mounting that alias. That is unambiguous in a
    /// single-tenant runtime and correct in a multi-tenant one *because the dataset is shared*: two
    /// tenants with the same alias and the same identity resolve to the same directory anyway. Where
    /// the tenant matters - routing, health, footprint - use [`MountTable::datasets`], which carries it.
    pub fn dir_for(&self, dir: &Path, alias: &str) -> PathBuf {
        match self.mounts.iter().find(|m| m.alias == alias) {
            Some(m) => Self::data_dir(dir, &m.nid),
            None => Self::nest_dir(dir, alias),
        }
    }

    /// The chains this runtime serves, each with its RPC endpoints (RFC-0021). A single-chain runtime
    /// synthesizes one entry from the top-level `chain`/`chain_id`/`rpc_urls`; a multichain runtime
    /// returns its `[[chains]]`. Errors if both forms are present (ambiguous) or neither (no chain).
    pub fn chain_endpoints(&self) -> Result<Vec<ChainEndpoint>> {
        // **`[[chains]]` is the only form in 2.0** (RFC-0035 §2). The top-level
        // `chain`/`chain_id`/`rpc_urls` shorthand said the same thing a second way, and one way to say
        // a thing is the point of a config cleanup. The fields survive on [`RuntimeMeta`] purely so
        // `migrate` can *read* a pre-2.0 file and translate it - see [`Self::chains_from_legacy`].
        if !self.chains.is_empty() {
            return Ok(self.chains.clone());
        }
        if self.runtime.chain.is_some() || self.runtime.chain_id.is_some() {
            bail!(
                "runtime '{}' uses the pre-2.0 top-level chain form. 2.0 declares chains under \
                 [[chains]] - one form, one meaning. `nuthatch migrate` rewrites it for you.",
                self.runtime.name
            );
        }
        bail!(
            "runtime '{}' declares no chain - add a [[chains]] entry with chain, chain_id and rpc_urls",
            self.runtime.name
        )
    }

    /// Translate a pre-2.0 top-level chain declaration into a `[[chains]]` entry.
    ///
    /// `migrate`'s job, and the reason [`RuntimeMeta`]'s single-chain fields still exist: a legacy
    /// file *only* has the top-level form, so migration has to carry it across or it writes a
    /// `mounts.toml` that will not start. That would turn a migration into an outage.
    pub fn chains_from_legacy(&self) -> Option<ChainEndpoint> {
        match (self.runtime.chain.clone(), self.runtime.chain_id) {
            (Some(chain), Some(chain_id)) if self.chains.is_empty() => Some(ChainEndpoint {
                chain,
                chain_id,
                rpc_urls: self.runtime.rpc_urls.clone(),
            }),
            _ => None,
        }
    }
}

/// One dataset that started with no data of its own and took an existing one's (RFC-0033 §5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Adoption {
    /// The identity that adopted - the one the mount record claims.
    pub nid: String,
    /// The identity it adopted from.
    pub from_nid: String,
}

impl std::fmt::Display for Adoption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} adopted the dataset of {} - same data identity, so no re-index",
            &self.nid[..12.min(self.nid.len())],
            &self.from_nid[..12.min(self.from_nid.len())],
        )
    }
}

/// An existing dataset whose inputs imply **byte-identical data** to `want`, and which actually holds
/// some (RFC-0033 §5, early cutoff).
///
/// Shared by the two paths that can face a new identity over old data: `migrate`, which meets it when
/// a staged nest is edited between runs, and the mount path, which meets it when an edited nest is
/// installed under the identity it now hashes to. One definition, so the two cannot drift into
/// answering the question differently.
///
/// Two independent conditions, both required. The data identity is the general check; `registry_hash`
/// is a second, narrower one that pins the decode. Requiring both means a bug in the exclusion list
/// cannot on its own cause an adoption - and the failure of either costs only a re-index.
///
/// **A candidate with no derived state is not a candidate.** Adopting an empty dataset copies nothing
/// and still consumes the one chance to adopt, leaving a re-index that looks like a cutoff.
///
/// Skips `want_nid` itself: a dataset that already exists is not an adoption.
pub fn adoptable(
    root: &Path,
    want: &crate::blob::Manifest,
    want_nid: &str,
) -> Option<(String, PathBuf)> {
    let entries = std::fs::read_dir(root.join(DATA_DIR)).ok()?;
    let mut candidates: Vec<(String, PathBuf)> = entries
        .flatten()
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .map(|e| (e.file_name().to_string_lossy().to_string(), e.path()))
        .filter(|(nid, _)| nid != want_nid && nid.len() == 64)
        .collect();
    // Deterministic: two equally-adoptable datasets must not depend on directory order.
    candidates.sort();

    candidates.into_iter().find(|(_, dir)| {
        if !holds_data(dir, Adopting::From) {
            return false;
        }
        let Ok(m) = crate::blob::build_manifest(dir, None) else {
            return false;
        };
        if m.registry_hash != want.registry_hash || m.data_identity() != want.data_identity() {
            return false;
        }
        // And what the candidate **recorded when it was indexed**, not only what its files recompute
        // to now (#1369). The two differ exactly when the running binary changed how something
        // decodes and moved the registry hash with it: the old dataset then recomputes to the new
        // identity and passes every check above, while the hash inside its store still names the old
        // one. `guard_registry_identity` refuses that store at start, so nothing mixed is served -
        // but the adoption has been spent, and the mount is left holding a store it can never start
        // from and never adopts again. A recomputed identity is the running binary's opinion; the
        // recorded one is what actually wrote the rows.
        //
        // Read the same way `guard_registry_identity` reads it, and treated the same way
        // `Adopting::From` treats an unreadable store: absent or unreadable disqualifies the
        // candidate, because the cost of being wrong here is a mount an operator must clear by hand
        // and the cost of being cautious is a re-index.
        // A store stamped under the full decode identity records that identity, not the event
        // registry's hash the manifest carries, so it is compared with what the guard would write.
        let db = dir.join(crate::config::DB_FILE);
        match crate::store::recorded_registry_hash(&db) {
            Ok(Some(recorded)) => match crate::store::recorded_identity_formula(&db) {
                Ok(None) => recorded == want.registry_hash,
                Ok(Some(_)) => decode_identity_of(dir).is_some_and(|id| recorded == id),
                Err(_) => false,
            },
            Ok(None) => {
                tracing::debug!(
                    "{} recomputes to the wanted identity but records no registry hash; not \
                     adopting from it",
                    dir.display()
                );
                false
            }
            Err(err) => {
                tracing::warn!(
                    "cannot read the recorded registry hash at {}, so not adopting from it: {err:#}",
                    dir.display()
                );
                false
            }
        }
    })
}

/// The full decode identity the running binary computes for a dataset's inputs, as
/// `guard_registry_identity` records it. `None` when the inputs cannot be read, which disqualifies.
fn decode_identity_of(dir: &Path) -> Option<String> {
    let config = Config::load(dir).ok()?;
    let registry = crate::registry::from_nest(dir, &config).ok()?;
    crate::project::decode_identity(dir, &config, &registry)
        .ok()
        .map(hex::encode)
}

/// Which side of an adoption is asking [`holds_data`].
///
/// An unreadable store is the one input the two sides must answer **differently**, and the side is a
/// parameter rather than a default folded into the function so that a third caller cannot inherit
/// whichever answer happened to be hard-wired. It was hard-wired to the destination's answer, and
/// `adoptable` - the one caller that wanted the other one - silently got it (issue #415).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Adopting {
    /// The dataset being written **into**. An unreadable store counts as data: being wrong here
    /// overwrites history, and costing a re-index is the cheaper way to be wrong.
    Into,
    /// The dataset being copied **from**. An unreadable store disqualifies the candidate outright:
    /// copying one in spends the destination's single adoption, and every later start then finds a
    /// store that exists and errors - so the destination holds data, can never adopt again, and must
    /// be emptied by hand. That is issue #408's permanently-stuck shape reached through the error
    /// path, and unlike the destination's mistake it is not repaired by a re-index.
    ///
    /// Corruption is not the only way to reach it, and the other way is why this side matters more
    /// once the cutoff runs on the mount path (#414): redb takes an **exclusive file lock**, so a
    /// candidate held open by a live cursor answers `Err` too. A runtime that mounts a nest while
    /// its sibling is indexing would otherwise nominate the one dataset that must never be copied,
    /// and copying a redb mid-write does not yield a slightly-stale store, it yields a torn file
    /// that is not a store at all.
    From,
}

/// Does this dataset hold indexed state of its own - a hot store with rows in it, or sealed segments?
///
/// The question adoption turns on in both directions: a destination that holds data must never be
/// written over, and a source that holds none is not worth adopting.
///
/// **It asks about data, not about files** (issue #408). This was a presence check over
/// [`crate::blob::DERIVED_STATE`], and both of those paths appear before anything has been indexed:
/// `Store::open` creates and commits an empty `nuthatch.redb` as soon as a cursor starts, and
/// `segments/` is created before the first segment lands in it. So a runtime that was started and
/// stopped before its first block left a dataset that read as holding data, which cost both sides of
/// the decision: an empty candidate was adopted and consumed the one chance to adopt the sibling that
/// held the real history, and a destination that had once been started could never adopt again.
///
/// **An unreadable store is answered per [`Adopting`]**, and it is the only input that is. Note the
/// order that follows from it: the store is consulted before `segments/`, because a candidate whose
/// store cannot be read is not rescued by holding sealed segments beside it - copying the pair in
/// lands the unreadable store on the destination just the same.
pub fn holds_data(dir: &Path, side: Adopting) -> bool {
    let db = dir.join(crate::config::DB_FILE);
    if db.exists() {
        match crate::store::store_holds_rows(&db) {
            Ok(true) => return true,
            Ok(false) => {}
            Err(err) => {
                tracing::warn!(
                    "cannot read the store at {}, treating this dataset as {} for adoption: {err:#}",
                    db.display(),
                    match side {
                        Adopting::Into => "holding data (it will not be written over)",
                        Adopting::From => "unadoptable (it will not be copied from)",
                    }
                );
                return matches!(side, Adopting::Into);
            }
        }
    }
    // Sealed segments are data and need no lock to see. An *empty* `segments/` is not data.
    std::fs::read_dir(dir.join(crate::seal::SEGMENTS_DIR))
        .map(|mut e| e.next().is_some())
        .unwrap_or(false)
}

/// A dataset that genuinely holds indexed history: a real redb carrying `last_block = head`.
///
/// Shared by `runtime`'s and `migrate`'s tests, because the fixture it replaces was duplicated
/// across both and both were wrong the same way. That was `fs::write(DB_FILE, "indexed history")` -
/// a text file wearing a store's name. redb cannot open it, so it counted as history through
/// [`crate::store::store_holds_rows`]' **error** path rather than because it held rows, and the
/// suite therefore proved that adoption copies a store nothing can read (issue #415).
///
/// Scoped so the handle is dropped before it returns: redb holds a file lock for as long as it
/// lives, and the caller is about to copy the file.
#[cfg(test)]
pub fn seed_history(dir: &Path, head: u64) {
    // The hash a real dataset would have recorded: at indexing time `guard_registry_identity` writes
    // whatever the binary computed from these same inputs. Writing only `last_block` left a fixture
    // that held rows and claimed no identity, which is a dataset that cannot exist in production -
    // and it is the reason #1369 could sit in `adoptable` unnoticed.
    let want = crate::blob::build_manifest(dir, None)
        .expect("a history fixture must sit on a readable dataset")
        .registry_hash;
    seed_history_recording(dir, head, &want);
}

/// [`seed_history`], but recording a registry hash of the caller's choosing.
///
/// The divergence #1369 is about: a dataset whose files recompute to one identity under the running
/// binary while its store records another, which is what a decode change does to history indexed
/// before it.
#[cfg(test)]
pub fn seed_history_recording(dir: &Path, head: u64, registry_hash: &str) {
    let s = crate::store::Store::open(&dir.join(crate::config::DB_FILE))
        .expect("a store fixture must be creatable");
    s.set_meta("last_block", &head.to_string()).unwrap();
    s.set_meta(crate::store::REGISTRY_KEY, registry_hash)
        .unwrap();
}

/// What [`seed_history`] wrote, read back through a real open - `None` if there is no store, or
/// nothing can be read out of it. Every assertion that used to compare the fixture's bytes reads
/// this instead, so a fixture that stopped holding rows fails rather than passing by another route.
///
/// The existence check is load-bearing, not defensive: `Store::open` is `Database::create`, so
/// asking an absent dataset this question would answer it by creating an empty store, and an
/// assertion that adoption copied nothing would then be reading a store the assertion itself put
/// there. Same shape as issue #413.
#[cfg(test)]
pub fn history_head(dir: &Path) -> Option<u64> {
    let db = dir.join(crate::config::DB_FILE);
    if !db.exists() {
        return None;
    }
    crate::store::Store::open(&db).ok()?.indexed_head().ok()?
}

/// Early cutoff on the **mount path** (RFC-0033 §5, issue #364).
///
/// A cosmetic edit moves the NID and leaves the data identity alone, so `data/<new-nid>` is a
/// directory holding the edited inputs and nothing else. The runtime used to mount it exactly like
/// that and re-fetch the whole chain, because nothing outside `migrate` ever consulted
/// `data_identity()`. The cost RFC-0033 §5 exists to remove was being paid by every path except the
/// one an operator actually uses.
///
/// Three properties this holds deliberately:
///
/// - **Never over data.** A dataset holding a store or segments is left alone, whatever it hashes to.
///   That makes this a no-op on every ordinary restart, and it means a partial index is resumed rather
///   than silently replaced.
/// - **Copies, never moves.** The source may still be mounted - by another tenant, or by the
///   pre-edit record somebody has not removed yet. Early cutoff must not take data away from a mount
///   that is using it (`migrate.rs` holds the same line).
/// - **Cannot change what it adopts into.** It copies exactly [`crate::blob::DERIVED_STATE`], which is
///   a subset of the set the NID excludes, so the destination's identity is the same after adoption as
///   before. An adoption that moved the identity would be a drift report a moment later.
///
/// - **All of it, or none of it.** [`crate::blob::DERIVED_STATE`] is copied into a staging sibling
///   first and committed by rename, so a fault part-way through leaves the destination exactly as it
///   was: inputs only, `holds_data` false, adoptable again on the next start. Copying straight in
///   would be worse than not adopting at all - a complete store carrying `sealed_through = N` beside
///   a partial `segments/` is a permanent hole in `/sql` that nothing ever repairs, because the
///   destination now holds data and can neither adopt nor backfill from the start block.
///
/// Returns what was adopted, for logging. A failed copy is an error, and the re-index it replaces is
/// genuinely still available once the fault is fixed.
/// A dataset directory that has been past the early cutoff, and the proof it has.
///
/// The cutoff used to be *a call you had to remember*. `load_mounted` remembered it; `mount`, added
/// later for RFC-0027, did not - so an operator who edited a nest and mounted it into a running
/// runtime re-indexed the whole chain, while stopping and starting the same runtime adopted the
/// predecessor's data and re-indexed nothing (#414). That is the second time this sprint a guard
/// applied per call site was missed by the next call site added.
///
/// So the guard stops being a call and becomes a type. [`indexer::build_and_prepare_nest`] takes one
/// of these instead of a `PathBuf`, and the only way to make one is [`prepare_dataset`] (or the
/// explicitly-named [`PreparedDataset::without_nid`], for a dataset that has no content address to
/// adopt against). A future mount path cannot skip the cutoff by forgetting it; it would have to
/// reach for a constructor whose name says what it is doing.
#[derive(Debug, Clone)]
pub struct PreparedDataset {
    dir: PathBuf,
    adoption: Option<Adoption>,
}

impl PreparedDataset {
    /// A pre-2.0 `nests/<alias>` directory, which has no NID and therefore no sibling to adopt from.
    /// Named rather than derived so the one legitimate bypass reads as a decision.
    pub fn without_nid(dir: PathBuf) -> Self {
        Self {
            dir,
            adoption: None,
        }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn into_dir(self) -> PathBuf {
        self.dir
    }

    /// What the cutoff adopted, if anything. `None` covers every ordinary case: no NID, no adoptable
    /// sibling, or a destination that already holds its own data.
    pub fn adoption(&self) -> Option<&Adoption> {
        self.adoption.as_ref()
    }
}

/// Run the early cutoff over `dir` and hand back the token that lets it be indexed.
///
/// **Infallible by design.** A dataset that could not adopt still indexes - slowly, and correctly,
/// because [`adopt_dataset`] is all-or-nothing and leaves the destination inputs-only. Refusing to
/// bring a nest up over a failed *optimisation* would turn a disk-full into an outage. That policy
/// used to live inside `load_mounted`, which meant the mount path would have had to re-derive it;
/// it lives here now so both paths get it without deciding again.
pub fn prepare_dataset(
    root: &Path,
    dir: PathBuf,
    nid: Option<&str>,
    label: &str,
) -> PreparedDataset {
    let Some(nid) = nid else {
        return PreparedDataset::without_nid(dir);
    };
    let adoption = match adopt_dataset(root, &dir, nid) {
        Ok(Some(a)) => {
            tracing::info!("early cutoff: {a}");
            Some(a)
        }
        Ok(None) => None,
        Err(e) => {
            tracing::warn!(
                "{label} could not adopt an existing dataset ({e:#}) - it will index from its start block"
            );
            None
        }
    };
    PreparedDataset { dir, adoption }
}

pub fn adopt_dataset(root: &Path, dataset: &Path, nid: &str) -> Result<Option<Adoption>> {
    // A staging directory left behind means a previous adoption died mid-copy. It is scratch that
    // nothing outside this function reads, and clearing it before the `holds_data` check is what
    // makes a killed adoption recoverable rather than merely a failed one.
    let staging = adopt_staging(dataset);
    if staging.exists() {
        std::fs::remove_dir_all(&staging)
            .with_context(|| format!("clearing stale adoption staging {}", staging.display()))?;
    }
    if !dataset.is_dir() || holds_data(dataset, Adopting::Into) {
        return Ok(None);
    }
    let Ok(want) = crate::blob::build_manifest(dataset, None) else {
        // Unhashable inputs are a different fault, and `Config::load` reports it with a better
        // message a moment later. Not adopting is the safe answer to a question we cannot ask.
        return Ok(None);
    };
    let Some((from_nid, from)) = adoptable(root, &want, nid) else {
        return Ok(None);
    };
    let outcome = stage_derived_state(&from, &staging)
        .and_then(|staged| commit_derived_state(&staging, dataset, &staged));
    // Scratch in every outcome. If this removal fails, the next start clears it before deciding
    // anything, so a leftover staging directory can never read as an adoption that happened.
    let _ = std::fs::remove_dir_all(&staging);
    outcome.with_context(|| format!("adopting {} into {}", from.display(), dataset.display()))?;
    Ok(Some(Adoption {
        nid: nid.to_string(),
        from_nid,
    }))
}

/// Where an adoption assembles its copy before committing it: a sibling of the destination, so the
/// commit is a same-filesystem rename rather than a second copy.
///
/// The suffix keeps it out of [`adoptable`]'s scan and of `prune::collectable`, both of which take
/// 64-hex names only - a staging directory must never be a candidate for anything. `prune` removes it
/// alongside the dataset it belongs to, so a copy interrupted by a kill cannot outlive its owner.
pub(crate) fn adopt_staging(dataset: &Path) -> PathBuf {
    let mut name = dataset.file_name().unwrap_or_default().to_os_string();
    name.push(".adopting");
    dataset.with_file_name(name)
}

/// Copy the source's derived state into `staging`, returning the names that actually landed there.
///
/// Nothing here touches `dataset`. That is the whole point: every way this can fail - disk full,
/// permissions, an unreadable segment - leaves the destination untouched.
fn stage_derived_state(from: &Path, staging: &Path) -> Result<Vec<&'static str>> {
    std::fs::create_dir_all(staging)
        .with_context(|| format!("cannot create {}", staging.display()))?;
    let mut staged = Vec::new();
    for name in crate::blob::DERIVED_STATE {
        let src = from.join(name);
        if !src.exists() {
            continue;
        }
        let dst = staging.join(name);
        if src.is_dir() {
            crate::project::copy_dir(&src, &dst)
        } else {
            std::fs::copy(&src, &dst).map(|_| ()).map_err(Into::into)
        }
        .with_context(|| format!("staging {} from {}", name, src.display()))?;
        staged.push(*name);
    }
    Ok(staged)
}

/// Move the staged set into the destination, unwinding what already moved if one of them fails.
///
/// These are metadata-only renames within one directory tree, so this is the narrow end of the
/// funnel by design - the copy, which is the part that runs out of disk, is already done.
fn commit_derived_state(staging: &Path, dataset: &Path, staged: &[&'static str]) -> Result<()> {
    let mut moved: Vec<&'static str> = Vec::new();
    for name in staged {
        let Err(e) = std::fs::rename(staging.join(name), dataset.join(name)) else {
            moved.push(name);
            continue;
        };
        let mut err = anyhow::Error::new(e).context(format!("committing {name}"));
        for done in moved.iter().rev() {
            let landed = dataset.join(done);
            let undo = if landed.is_dir() {
                std::fs::remove_dir_all(&landed)
            } else {
                std::fs::remove_file(&landed)
            };
            if let Err(u) = undo {
                err = err.context(format!(
                    "and {done} could not be unwound ({u}) - {} may now hold a partial adoption and \
                     must be emptied by hand before it will index correctly",
                    dataset.display()
                ));
            }
        }
        return Err(err);
    }
    Ok(())
}

/// Resolve every mounted dataset to `(route key, directory, config)` - the list the cursors are built
/// from (RFC-0032 §4).
///
/// This is the mount path. `dev` gets its nests from here and cannot get them anywhere else, which is
/// what makes the early-cutoff step below reachable in production rather than in `migrate` only.
///
/// Adoption runs **before** the config load, because a dataset that adopts is one whose store the
/// loader is about to open.
pub fn load_mounted(
    dir: &Path,
    datasets: &[Dataset],
    default_tenant: &str,
) -> Result<Vec<(String, PathBuf, Config)>> {
    let mut out = Vec::with_capacity(datasets.len());
    for ds in datasets {
        // The cutoff, and the warn-and-carry-on policy behind it, now live in `prepare_dataset` so
        // the mount path gets the same treatment without re-deriving it (#414).
        let _prepared = prepare_dataset(
            dir,
            ds.dir.clone(),
            ds.nid.as_deref(),
            &ds.canonical().to_string(),
        );
        let config = Config::load(&ds.dir).with_context(|| {
            format!(
                "loading mounted nest '{}' from {}",
                ds.canonical(),
                ds.dir.display()
            )
        })?;
        out.push((
            ds.canonical().route_key(default_tenant),
            ds.dir.clone(),
            config,
        ));
    }
    Ok(out)
}

/// A publisher for every mount that declares `[mounts.publish]` (RFC-0052 S2), keyed by route. One
/// per record: two mounts of one dataset naming one target contend on S1's conditional catalogue put.
pub fn spawn_publishers(
    dir: &Path,
    mounts: &[Mount],
    default_tenant: &str,
) -> Result<Vec<(String, crate::publish::Publisher)>> {
    let mut out = Vec::new();
    for m in mounts {
        if let Some(publish) = &m.publish {
            // The first mount of a dataset is the one its cursor indexes under, as `Dataset::canonical`.
            let canonical = mounts.iter().find(|first| first.nid == m.nid).unwrap_or(m);
            let cursor_key = mount_route(canonical, default_tenant);
            out.push(spawn_publisher(
                dir,
                m,
                publish,
                default_tenant,
                &cursor_key,
            )?);
        }
    }
    Ok(out)
}

fn mount_route(m: &Mount, default_tenant: &str) -> String {
    MountRef {
        tenant: m.tenant.clone(),
        alias: m.alias.clone(),
    }
    .route_key(default_tenant)
}

/// Reports under `cursor_key`, where the dataset's cursor records `sealed_through`, so publish lag compares
/// the two.
fn spawn_publisher(
    dir: &Path,
    m: &Mount,
    publish: &MountPublish,
    default_tenant: &str,
    cursor_key: &str,
) -> Result<(String, crate::publish::Publisher)> {
    let key = mount_route(m, default_tenant);
    let dataset = MountTable::data_dir(dir, &m.nid);
    let publisher = crate::publish::spawn(dataset, cursor_key.to_string(), publish.settings()?)
        .with_context(|| format!("publishing mount '{key}'"))?;
    Ok((key, publisher))
}

/// The identity of the dataset serving `route_key`, for the provenance stamp (RFC-0035 §3).
fn ds_nid_for(datasets: &[Dataset], route_key: &str, default_tenant: &str) -> Option<Arc<str>> {
    datasets
        .iter()
        .find(|d| {
            d.mounts
                .iter()
                .any(|m| m.route_key(default_tenant) == route_key)
        })
        .and_then(|d| d.nid.as_deref().map(Arc::from))
}

/// Overlay each booted mount's record (RFC-0034 §2). Applied **per mount, not per dataset**: that is
/// the whole reason the allowlist is mount config rather than manifest. Two tenants sharing one
/// dataset can expose different surfaces over it, and neither the NID nor the data is affected.
fn overlay_boot_records(
    states: &mut [(String, crate::serve::AppState)],
    records: &[Mount],
    datasets: &[Dataset],
    default_tenant: &str,
) -> Result<()> {
    for (key, state) in states.iter_mut() {
        let record = records
            .iter()
            .find(|m| mount_route(m, default_tenant) == *key);
        let surface = served_surface(record, &state.dir, key)?;
        if surface.access != crate::allowlist::SqlAccess::Open {
            tracing::info!(
                "mount {key}: sql = {:?}, {} declared quer{}",
                surface.access,
                surface.queries.len(),
                if surface.queries.len() == 1 {
                    "y"
                } else {
                    "ies"
                }
            );
        }
        let nid = match record {
            Some(_) => ds_nid_for(datasets, key, default_tenant),
            None => state.nid.clone(),
        };
        overlay_mount_record(state, nid.as_deref(), record, surface);
    }
    Ok(())
}

/// Give every extra alias of a shared dataset its own route onto the *same* state (RFC-0032 §7).
///
/// One dataset was indexed, under its canonical alias. Each further alias gets a **clone** of that
/// `AppState` - the same `Arc` store, the same directory, the same views. Two doors, one room: no
/// second store, no second backfill, and nothing to keep in sync afterwards because there is only
/// ever one of everything.
///
/// A dataset whose canonical mount is absent (its cursor was refused) is skipped rather than
/// aliased onto nothing.
pub fn fan_out_aliases(
    datasets: &[Dataset],
    mut states: Vec<(String, crate::serve::AppState)>,
    health: &crate::health::RuntimeHealth,
    estimates: &mut std::collections::HashMap<String, u64>,
    default_tenant: &str,
) -> Vec<(String, crate::serve::AppState)> {
    let mut extra = Vec::new();
    for ds in datasets {
        let canonical = ds.canonical().route_key(default_tenant);
        let Some((_, state)) = states.iter().find(|(n, _)| n == &canonical) else {
            continue;
        };
        for m in &ds.mounts[1..] {
            let key = m.route_key(default_tenant);
            health.register_alias(&key, &canonical, &state.chain);
            // The footprint was charged once, to the dataset. Charging it again per mount would make
            // the per-cursor budget refuse a mount that costs nothing - sharing must not be taxed.
            estimates.insert(key.clone(), 0);
            extra.push((key, state.clone()));
        }
    }
    states.extend(extra);
    states
}

/// One dataset and every alias serving it (RFC-0032 §4-§5) - "two doors, one room".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dataset {
    /// Where the data lives: `data/<nid>`, or `nests/<alias>` for a nest not yet migrated.
    pub dir: PathBuf,
    /// Every mount serving this dataset, in serving order. Never empty.
    pub mounts: Vec<MountRef>,
    /// The nest identity, when this dataset is identity-keyed. `None` for the pre-2.0 layout.
    pub nid: Option<String>,
}

impl Dataset {
    /// The mount that indexes. The others are additional doors onto the same room.
    pub fn canonical(&self) -> &MountRef {
        &self.mounts[0]
    }

    /// How many mounts want this dataset (RFC-0032 §5).
    ///
    /// **Derived, never stored.** A count over the mount table cannot drift out of sync with the
    /// table, which is the failure mode every hand-maintained refcount eventually reaches.
    pub fn refcount(&self) -> usize {
        self.mounts.len()
    }
}

/// Group a runtime's *live* nests into datasets (RFC-0032 §4), for [`RuntimeHandles::mount`] /
/// [`RuntimeHandles::unmount`] to feed [`build_roster_entries`].
///
/// Deliberately grouped from `states` - the nests actually being served right now - rather than
/// [`MountTable::datasets`]'s source, the declared mount records. Those two agree at startup (every
/// declared mount is live), but not after a live mount or unmount: an unmounted co-tenant of a shared
/// dataset must drop out of `shared_with` immediately, and a nest mounted live from a bare directory
/// with no mount record yet must still appear, grouped by directory like any other.
fn live_datasets(
    dir: &Path,
    states: &[(String, crate::serve::AppState)],
    mounts: &[Mount],
    default_tenant: &str,
) -> Vec<Dataset> {
    let mut out: Vec<Dataset> = Vec::new();
    for (name, _) in states {
        let (tenant_seg, alias) = match name.split_once('/') {
            Some((t, a)) => (Some(t), a),
            None => (None, name.as_str()),
        };
        let record = mounts
            .iter()
            .find(|m| mount_route(m, default_tenant) == *name);
        let tenant = record
            .map(|m| m.tenant.clone())
            .unwrap_or_else(|| tenant_seg.unwrap_or(default_tenant).to_string());
        let nid = record.map(|m| m.nid.clone());
        let path = match &nid {
            Some(nid) => MountTable::data_dir(dir, nid),
            None => MountTable::nest_dir(dir, alias),
        };
        let mount_ref = MountRef {
            tenant,
            alias: alias.to_string(),
        };
        match out.iter_mut().find(|d| d.dir == path) {
            Some(d) => d.mounts.push(mount_ref),
            None => out.push(Dataset {
                dir: path,
                mounts: vec![mount_ref],
                nid,
            }),
        }
    }
    out
}

/// Build the `GET /nests` roster entries for a nest set (RFC-0026 §5, RFC-0032 §4).
///
/// The one place this shape is assembled, called both at startup and by [`RuntimeHandles::mount`] /
/// [`RuntimeHandles::unmount`]. Before #554 the live handlers rebuilt `self.states` but never touched
/// `self.roster`, so a nest mounted into a running runtime served real traffic while `GET /nests` kept
/// reporting the set as of boot - the roster is the one surface an operator reads to confirm a mount,
/// and it lied by omission. Recomputing from the current state on every change, rather than patching the
/// roster incrementally, is what makes that drift structurally impossible.
fn build_roster_entries(
    states: &[(String, crate::serve::AppState)],
    datasets: &[Dataset],
    default_tenant: &str,
    estimates: &std::collections::HashMap<String, u64>,
) -> Vec<serde_json::Value> {
    states
        .iter()
        .map(|(name, state)| {
            // Which dataset backs this mount, and who else is on it (RFC-0032 §4). Without this an
            // operator seeing two entries has no way to tell one shared dataset from two backfills.
            // `None` when this mount has no dataset record - a nest mounted live from a bare directory
            // with nothing yet in `mounts.toml` - and the fields below degrade gracefully to that.
            let ds = datasets.iter().find(|d| {
                d.mounts
                    .iter()
                    .any(|m| &m.route_key(default_tenant) == name)
            });
            let this = ds.and_then(|d| {
                d.mounts
                    .iter()
                    .find(|m| &m.route_key(default_tenant) == name)
            });
            let tenant = this.map(|m| m.tenant.clone());
            let shared_with: Vec<String> = ds
                .map(|d| {
                    d.mounts
                        .iter()
                        .map(|m| m.route_key(default_tenant))
                        .filter(|k| k != name)
                        .collect()
                })
                .unwrap_or_default();
            serde_json::json!({
                "name": name,
                "chain": state.chain,
                "tenant": tenant,
                "nid": ds.and_then(|d| d.nid.clone()),
                "shared_with": shared_with,
                "registry_hash": state.nest_info.get("registry_hash").cloned().unwrap_or_default(),
                "table_count": state.tables.len(),
                "base_path": format!("/{name}"),
                "estimated_rss_mb": estimates.get(name).copied().unwrap_or(0),
            })
        })
        .collect()
}

/// A mounted dataset whose inputs no longer hash to the identity its mount record claims.
///
/// See [`MountTable::identity_drift`]. `actual` is `None` when the inputs could not be hashed at all,
/// which is a different fault with the same consequence: the claimed identity is unverified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityDrift {
    /// The canonical mount serving this dataset - the one that indexes it.
    pub alias: String,
    /// The dataset directory, which is `data/<claimed>` by construction.
    pub dir: PathBuf,
    /// What the mount record says this dataset is.
    pub claimed: String,
    /// What its inputs actually hash to, or `None` if they could not be hashed.
    pub actual: Option<String>,
}

impl std::fmt::Display for IdentityDrift {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.actual {
            Some(actual) => write!(
                f,
                "'{}' claims identity {} but its inputs hash to {} - the dataset at {} has been \
                 edited in place, so its mount record is describing a nest that no longer exists",
                self.alias,
                &self.claimed[..12.min(self.claimed.len())],
                &actual[..12.min(actual.len())],
                self.dir.display()
            ),
            None => write!(
                f,
                "'{}' claims identity {} and its inputs at {} could not be hashed, so that claim is \
                 unverified",
                self.alias,
                &self.claimed[..12.min(self.claimed.len())],
                self.dir.display()
            ),
        }
    }
}

/// A chain's cursor unit (RFC-0021): the endpoint (RPC) plus the mounted nests that follow that chain.
/// Each becomes one isolated cursor - the single-cursor law, held per chain.
#[derive(Debug)]
pub struct ChainGroup {
    pub endpoint: ChainEndpoint,
    pub nests: Vec<(String, PathBuf, Config)>,
}

/// Group loaded nests by their declared chain, matching each to a runtime chain endpoint (RFC-0021).
/// A nest whose chain the runtime doesn't declare is a hard error; declared-but-unused chains are left
/// out, and [`dev`] keeps them dormant until a mount needs one (#1545). Deterministic order (endpoints as declared).
pub fn group_by_chain(
    endpoints: &[ChainEndpoint],
    mounted: Vec<(String, PathBuf, Config)>,
) -> Result<Vec<ChainGroup>> {
    let mut groups: Vec<ChainGroup> = endpoints
        .iter()
        .map(|e| ChainGroup {
            endpoint: e.clone(),
            nests: Vec::new(),
        })
        .collect();
    for (name, path, config) in mounted {
        let idx = groups.iter().position(|g| {
            g.endpoint.chain == config.nest.chain && g.endpoint.chain_id == config.nest.chain_id
        });
        match idx {
            Some(i) => groups[i].nests.push((name, path, config)),
            None => bail!(
                "nest '{name}' is on {} (chain_id {}), which this runtime doesn't declare - add it under \
                 [[chains]] (or [mounts] chain/chain_id)",
                config.nest.chain,
                config.nest.chain_id
            ),
        }
    }
    groups.retain(|g| !g.nests.is_empty());
    Ok(groups)
}

/// `nuthatch dev --dir <dir>` where `<dir>` holds a mount table: bring up every mounted nest and
/// serve them behind one listener. Reached from the same `dev` a single nest uses (RFC-0032).
///
/// One shared source drives all nests through a single `indexer::spawn_runtime` task per chain (the
/// shared cursor - one `getLogs` per window fanned out to the owning nests). Before starting it
/// projects the runtime's RSS and refuses a mount that would exceed `max_rss` (§3). A cursor that dies
/// is **quarantined, not fatal** (RFC-0026): its siblings keep indexing and serving, and the runtime
/// exits only when every cursor is gone - the per-cursor blast-radius rule, actually held.
#[allow(clippy::too_many_arguments)]
pub async fn dev(
    dir: PathBuf,
    listen: String,
    rpc_override: Vec<String>,
    rpc_fallback: Vec<String>,
    backfill: Option<u64>,
    seal_direct: bool,
    concurrency: usize,
    ipfs_window_deadline: std::time::Duration,
    window_override: Option<u64>,
    no_admin: bool,
    fail_fast: bool,
    freshness: crate::freshness::Freshness,
    cors: Vec<String>,
    registry: Option<String>,
) -> Result<()> {
    // Before the mount table is even read: a malformed origin should fail the command, not the
    // fifteenth minute of a backfill.
    let cors = crate::serve::cors_layer(&cors)?;
    let all_mounts = MountTable::load(&dir)?;
    let (mounts, suspended) = split_suspended(&all_mounts);
    let admin_enabled = indexer::admin_enabled(no_admin, &listen);
    // Empty is a valid start (#1545): nests arrive over the admin API. Without that API nothing
    // ever could.
    if mounts.mount_refs().is_empty() && !admin_enabled {
        anyhow::bail!(
            "runtime '{}' mounts nothing, and without the admin API nothing can be mounted later \
             (add a [[mounts]] record, or drop --no-admin)",
            mounts.runtime.name
        );
    }
    let meta = &mounts.runtime;
    let endpoints = mounts.chain_endpoints()?;

    // Load every mounted **dataset** - not every alias (RFC-0032 §4). Aliases sharing one nest
    // identity share one store and one place in the cursor; only the canonical one indexes, and the
    // rest become extra routes onto it further down.
    let datasets = mounts.datasets(&dir);

    // RFC-0032 §3: does each dataset still hash to the identity its mount record claims? Nothing
    // checked this before, so a dataset edited in place kept its data, kept its directory, and left
    // the record describing a nest that no longer existed - while sharing, refcounting and `prune`
    // all went on keying off that value.
    //
    // **A warning, not a refusal, and deliberately so.** There is currently no supported flow for
    // editing a nest inside a runtime, which makes editing in place the only flow an operator has.
    // Refusing to start would take away the only route they have and would break a runtime that was
    // serving correctly a moment earlier. Once an edit-and-adopt path exists this should become a
    // refusal on the mount path; until then, the operator needs to *know*, not to be locked out.
    for d in mounts.identity_drift(&dir) {
        tracing::warn!("identity drift: {d}");
    }

    // The whole table decides the tenancy shape, suspended records included, or a route key would
    // change with a suspend.
    let default_tenant_owned = all_mounts.tenant_default();
    let default_tenant = default_tenant_owned.as_str();
    let mut mounted = load_mounted(&dir, &datasets, default_tenant)?;
    // The dial is the operator's, not the nest's (RFC-0040), so it is stamped onto every mounted
    // config here rather than read from any of them - `Config::freshness` is `#[serde(skip)]`.
    for (_, _, config) in &mut mounted {
        stamp_operator_settings(config, freshness, ipfs_window_deadline);
    }
    let groups = group_by_chain(&endpoints, mounted)?;

    // A mount may narrow its author's ceiling, never widen it (RFC-0034 §3). Checked before any
    // store is opened, so a nest exposing more than its author sanctioned fails to start rather than
    // serving the excess until somebody notices.
    for ds in &datasets {
        for m in &ds.mounts {
            let rec = mounts
                .mounts
                .iter()
                .find(|r| r.tenant == m.tenant && r.alias == m.alias);
            served_surface(rec, &ds.dir, &m.route_key(default_tenant))?;
        }
    }

    for ds in &datasets {
        if ds.refcount() > 1 {
            tracing::info!(
                "dataset {} is shared by {} mounts ({}) - indexed once",
                ds.nid.as_deref().unwrap_or("<un-migrated>"),
                ds.refcount(),
                ds.mounts
                    .iter()
                    .map(|m| m.to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }
    }
    if all_mounts
        .mount_refs()
        .iter()
        .any(|m| m.tenant != default_tenant)
    {
        tracing::info!(
            "multi-tenant: routes are /<tenant>/<nest>/… ({} tenants)",
            mounts
                .mount_refs()
                .iter()
                .map(|m| m.tenant.clone())
                .collect::<std::collections::BTreeSet<_>>()
                .len()
        );
    }

    // `--rpc` is ambiguous once a runtime spans chains (which chain would it override?). Allow it only for
    // a single-chain runtime; a multichain runtime sets rpc_urls per chain under [[chains]].
    if !(rpc_override.is_empty() && rpc_fallback.is_empty()) && groups.len() > 1 {
        bail!(
            "--rpc and --rpc-fallback are ambiguous for a multichain runtime ({} chains) - set rpc_urls per chain under [[chains]]",
            groups.len()
        );
    }
    tracing::info!(
        "runtime '{}': mounting {} nest(s) across {} chain(s) - one isolated cursor per chain",
        meta.name,
        mounts.mount_refs().len(), // NOT `meta.nests`, which is empty once the mount table is authoritative
        groups.len(),
    );

    let admin_token = indexer::admin_required_token(admin_enabled);
    // The RSS budget is now **per active-chain cursor** (RFC-0021), not per whole runtime.
    let max_rss = meta.max_rss_mb.unwrap_or(DEFAULT_MAX_RSS_MB);

    // Bring up one cursor per chain group: its own source + `spawn_runtime`, isolated tip/finality/reorg,
    // and held to the per-cursor RSS budget. A cursor's failure quarantines that cursor alone (RFC-0026).
    let mut all_states: Vec<(String, crate::serve::AppState)> = Vec::new();
    let mut ingests: Vec<(String, tokio::task::JoinHandle<Result<()>>)> = Vec::new();
    let mut alert_workers: Vec<(String, tokio::task::JoinHandle<()>)> = Vec::new();
    // Chain -> that cursor's command channel, so an unmount reaches the cursor hosting the nest.
    let mut lifecycle: std::collections::HashMap<
        String,
        tokio::sync::mpsc::UnboundedSender<indexer::CursorCommand>,
    > = std::collections::HashMap::new();
    let mut estimates: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    let mut sources: std::collections::HashMap<String, Arc<dyn Source>> =
        std::collections::HashMap::new();
    let mut endpoint_counts: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    let mut chain_freshness: std::collections::HashMap<String, crate::freshness::Freshness> =
        std::collections::HashMap::new();
    let mut runtime_total_mb = RUNTIME_BASE_RSS_MB;
    // The live health surface (RFC-0026 §5): the cursors write quarantine state here, the API reads it
    // per request. Replaces the roster snapshot that was built once at startup and could not express
    // "partly working".
    let health = Arc::new(crate::health::RuntimeHealth::new());

    for mut group in groups {
        let rpc_urls = rpc::select_rpcs(&rpc_override, group.endpoint.rpc_urls.clone());
        if rpc_urls.is_empty() {
            bail!(
                "mounts '{}' chain {} has no rpc_urls (set them under [[chains]], or pass --rpc for a \
                 single-chain runtime)",
                meta.name,
                group.endpoint.chain
            );
        }
        let timestamps = group.nests.iter().any(|(_, _, c)| c.nest.block_timestamps);
        let pool: Vec<String> = rpc_urls
            .iter()
            .chain(rpc_fallback.iter())
            .cloned()
            .collect();
        if let Some(note) = crate::chains::keyless_caveat(&group.endpoint.chain, &pool, timestamps)
        {
            tracing::warn!("{note}");
        }
        let names: Vec<&str> = group
            .nests
            .iter()
            .map(|(name, _, _)| name.as_str())
            .collect();
        let concurrency =
            indexer::backfill_concurrency_for(rpc_urls.len(), concurrency, seal_direct, &names);
        endpoint_counts.insert(group.endpoint.chain.clone(), rpc_urls.len());

        // Per-cursor footprint budget (RFC-0021): this chain's nests must fit ≤ max_rss.
        let mut cursor_mb = 0u64;
        // RFC-0041 §7, criterion 11: nests on the same chain are accounted **together**, so the
        // ceiling is the cursor's rather than each nest's.
        //
        // Summed rather than accumulated in the loop below. A `+=` written as `=` counts the last
        // nest instead of the cursor, which is the whole of what this criterion is about, and it
        // survives every unit test because the loop lives inside an async function no unit test
        // reaches. There is no assignment here to get wrong.
        let entities_on_cursor: usize = group
            .nests
            .iter()
            .map(|(_, path, _)| declared_entities(path).0)
            .sum();
        for (name, path, config) in &group.nests {
            let has_labels = !crate::labels::load(path).is_empty();
            let (entity_count, entity_rows) = declared_entities(path);
            let mb = estimate_nest_rss_mb(config, has_labels, entity_count, entity_rows);
            estimates.insert(name.clone(), mb);
            cursor_mb += mb;
        }
        tracing::info!(
            "mounts cursor on {} (chain_id {}): {} nest(s), ~{cursor_mb} MB projected; budget {max_rss} MB/cursor",
            group.endpoint.chain,
            group.endpoint.chain_id,
            group.nests.len(),
        );
        refuse_over_entity_ceiling(
            entities_on_cursor,
            group.nests.len(),
            &meta.name,
            &group.endpoint.chain,
        )?;
        if cursor_mb > max_rss {
            bail!(
                "mounts '{}' cursor on {} projects ~{cursor_mb} MB but max_rss is {max_rss} MB/cursor - \
                 raise max_rss, drop a nest, or move it to another mounts",
                meta.name,
                group.endpoint.chain
            );
        }
        runtime_total_mb += cursor_mb;

        // Attribute each nest to this chain's cursor, so a cursor fault marks all of them (§5).
        for (name, _, _) in &group.nests {
            health.register(name, &group.endpoint.chain);
        }

        // One source + one shared cursor per chain - per-nest tables stay byte-identical to solo `dev`.
        let endpoint = ChainEndpoint {
            rpc_urls,
            ..group.endpoint.clone()
        };
        let (source, dial) =
            open_chain(&endpoint, rpc_fallback.clone(), &meta.name, freshness).await?;
        for (_, _, config) in &mut group.nests {
            config.freshness = dial;
        }
        chain_freshness.insert(group.endpoint.chain.clone(), dial);
        // Retained so a mount can build a nest against the same source its co-tenants use - a nest
        // mounted at runtime must be indistinguishable from one mounted at boot.
        sources.insert(group.endpoint.chain.clone(), source.clone());
        let cursor = indexer::spawn_runtime(
            source,
            group.nests,
            backfill,
            seal_direct,
            concurrency,
            window_override,
            admin_enabled,
            admin_token.clone(),
            health.clone(),
            fail_fast,
        )
        .await
        .with_context(|| {
            format!(
                "bringing up mounts '{}' cursor on {}",
                meta.name, group.endpoint.chain
            )
        })?;
        // Retain the per-nest handles: the driver needs them to re-compose the router (and abort the
        // right alert worker) when a nest is unmounted (RFC-0027 §6).
        lifecycle.insert(group.endpoint.chain.clone(), cursor.lifecycle);
        all_states.extend(cursor.states);
        ingests.push((group.endpoint.chain.clone(), cursor.ingest));
        alert_workers.extend(cursor.alert_workers);
    }

    // `--rpc` and `--rpc-fallback` only ever mean something for a single-chain runtime.
    let single_chain = endpoints.len() == 1;
    let dormant: std::collections::HashMap<String, DormantChain> = endpoints
        .iter()
        .filter(|e| !lifecycle.contains_key(&e.chain))
        .map(|e| {
            let rpc_urls = if single_chain {
                rpc::select_rpcs(&rpc_override, e.rpc_urls.clone())
            } else {
                e.rpc_urls.clone()
            };
            let chain = DormantChain {
                endpoint: ChainEndpoint {
                    rpc_urls,
                    ..e.clone()
                },
                rpc_fallback: if single_chain {
                    rpc_fallback.clone()
                } else {
                    Vec::new()
                },
            };
            (e.chain.clone(), chain)
        })
        .collect();
    if !dormant.is_empty() {
        tracing::info!(
            "chain(s) with nothing mounted yet: {} - a cursor starts on the first mount onto each",
            dormant.keys().cloned().collect::<Vec<_>>().join(", ")
        );
    }
    let (cursor_feed, mut cursor_intake) = tokio::sync::mpsc::unbounded_channel();

    tracing::info!(
        "mounts footprint: ~{runtime_total_mb} MB projected across {} cursor(s)",
        ingests.len()
    );

    let mut all_states = fan_out_aliases(
        &datasets,
        all_states,
        &health,
        &mut estimates,
        default_tenant,
    );

    overlay_boot_records(&mut all_states, &mounts.mounts, &datasets, default_tenant)?;
    let all_states = all_states;
    let publishers = spawn_publishers(&dir, &mounts.mounts, default_tenant)?;

    // Roster (`GET /nests`) across every cursor's nests, with per-nest footprint attribution and the
    // mounts's real resident set alongside the projection so operators can calibrate.
    let roster_entries = build_roster_entries(&all_states, &datasets, default_tenant, &estimates);
    let roster = serde_json::json!({
        // The runtime's own name. Called `roost` pre-2.0; the blanket rename briefly made this
        // `mounts`, which read as "the mount list" while holding a single name string.
        "runtime": meta.name,
        "chains": endpoints.iter().map(|e| e.chain.clone()).collect::<Vec<_>>(),
        "projected_rss_mb": runtime_total_mb,
        "max_rss_mb_per_cursor": max_rss,
        "rss_bytes": crate::metrics::rss_bytes(),
        "nests": roster_entries,
    });

    // The live handles: what makes the nest set changeable at runtime instead of frozen at boot
    // (RFC-0027). Everything the driver needs to re-compose the router lives here rather than being
    // moved into it and forgotten.
    let live = crate::serve::LiveRuntime::new(crate::serve::compose_runtime(
        roster.clone(),
        all_states.clone(),
        health.clone(),
    ));
    let handles = Arc::new(tokio::sync::Mutex::new(RuntimeHandles {
        live,
        states: all_states,
        alert_workers: std::mem::take(&mut alert_workers),
        publishers,
        lifecycle,
        health: health.clone(),
        roster,
        estimates: estimates.clone(),
        default_tenant: default_tenant_owned.clone(),
        suspended,
        joining: Default::default(),
        mount_ctx: MountContext {
            dir: dir.clone(),
            mounts: all_mounts.mounts.clone(),
            sources,
            endpoint_counts,
            backfill,
            seal_direct,
            concurrency,
            ipfs_window_deadline,
            window_override,
            admin_enabled,
            admin_token: admin_token.clone(),
            max_rss_mb: max_rss,
            freshness,
            chain_freshness,
            dormant,
            fail_fast,
            cursors: admin_enabled.then_some(cursor_feed),
            registry,
        },
    }));

    // The server and the cursor supervisor race; whichever ends first decides the exit (RFC-0026 §6).
    // A *single* cursor's death no longer ends anything - that is the supervisor's job to absorb.
    {
        let h = handles.lock().await;
        for name in h.suspended.keys() {
            h.health.suspend_nest(name);
        }
        if !h.suspended.is_empty() {
            h.live.swap(h.compose()?);
        }
    }
    let jobs = start_mount_jobs(&dir, &handles, admin_enabled).await?;
    let service = handles.lock().await.live.service().merge(lifecycle_routes(
        handles.clone(),
        jobs.clone(),
        admin_enabled,
        admin_token.clone(),
    ));
    let result = tokio::select! {
        r = crate::serve::bind_and_serve(&listen, service, cors) => r,
        r = supervise_cursors(&mut ingests, &mut cursor_intake, &health, fail_fast) => r,
    };
    for (_, h) in &ingests {
        h.abort();
    }
    for (_, w) in &handles.lock().await.alert_workers {
        w.abort();
    }
    result
}

/// Watch every chain cursor, quarantining the ones that die instead of taking the runtime down with them
/// (RFC-0026 §6, issue #147).
///
/// The old behaviour was `select_all` over the cursors: the **first** to finish - success or failure -
/// aborted every sibling and exited the process. So a reorg below finality on one chain tore down a
/// perfectly healthy cursor on another, which is precisely what `CLAUDE.md`'s per-cursor blast-radius
/// rule forbids. Now a dead cursor is retired from the set and logged; its nests keep serving the data
/// they had (frozen but correct - slice 3 marks them unhealthy so nobody mistakes it for fresh).
///
/// This returns - ending the runtime - only when **every** cursor is gone, because at that point nothing
/// will ever advance again and a restart is the only thing that can help. Exiting non-zero under a
/// supervisor beats staying up serving permanently-frozen data.
///
/// `pub` for one reason: this is the *only* point at which a dead cursor's fate is decided, and
/// [`dev`] reaches it through a real RPC endpoint set, so no integration test can drive the property
/// through `dev` with a fixture chain. `tests/e2e_cursor_death_isolation.rs` hands it the genuine
/// [`crate::indexer::ChainCursor::ingest`] handles of two live cursors instead (issue #387). Keeping it
/// private would only have moved that test onto handles it fabricated itself, which is the coverage
/// that already existed and the coverage the issue was filed about.
///
/// `intake` carries the cursors a mount spawns after boot (#1545). While it is open, an empty set is
/// a runtime waiting for its first mount, not a finished one.
pub async fn supervise_cursors(
    ingests: &mut Vec<(String, tokio::task::JoinHandle<Result<()>>)>,
    intake: &mut tokio::sync::mpsc::UnboundedReceiver<(
        String,
        tokio::task::JoinHandle<Result<()>>,
    )>,
    health: &crate::health::RuntimeHealth,
    fail_fast: bool,
) -> Result<()> {
    enum Next {
        Spawned(Option<(String, tokio::task::JoinHandle<Result<()>>)>),
        Ended(Result<Result<()>, tokio::task::JoinError>, usize),
    }
    let mut total = ingests.len();
    let mut failures: Vec<String> = Vec::new();
    let mut intake_open = true;
    loop {
        if ingests.is_empty() && (!failures.is_empty() || !intake_open) {
            break;
        }
        let next = if ingests.is_empty() {
            Next::Spawned(intake.recv().await)
        } else {
            tokio::select! {
                c = intake.recv(), if intake_open => Next::Spawned(c),
                (joined, idx, _) = futures::future::select_all(ingests.iter_mut().map(|(_, h)| h)) => {
                    Next::Ended(joined, idx)
                }
            }
        };
        let (joined, idx) = match next {
            Next::Spawned(Some(cursor)) => {
                total += 1;
                ingests.push(cursor);
                continue;
            }
            Next::Spawned(None) => {
                intake_open = false;
                continue;
            }
            Next::Ended(joined, idx) => (joined, idx),
        };
        let (chain, _) = ingests.remove(idx);
        let outcome = match joined {
            Ok(inner) => inner,
            Err(e) if e.is_panic() => Err(anyhow::anyhow!("the ingestion loop panicked")),
            Err(e) => Err(anyhow::anyhow!("the ingestion loop task failed: {e}")),
        };
        match outcome {
            Ok(()) => tracing::info!(
                "mounts cursor on {chain} finished cleanly; {} cursor(s) still indexing",
                ingests.len()
            ),
            Err(e) => {
                if fail_fast {
                    bail!("--fail-fast: mounts cursor on {chain} died: {e:#}");
                }
                tracing::error!(
                    "mounts cursor on {chain} QUARANTINED: {e:#} - its nests keep serving their last \
                     indexed state; {} sibling cursor(s) continue unaffected",
                    ingests.len()
                );
                // Every nest on this cursor is now out of service, however healthy it was itself.
                health.quarantine_cursor(&chain, format!("{e:#}"));
                failures.push(format!("{chain}: {e:#}"));
            }
        }
    }
    if failures.is_empty() {
        tracing::warn!("every mounts cursor ({total}) finished cleanly - nothing left to index");
        return Ok(());
    }
    bail!(
        "every mounts cursor is dead, so nothing will advance again - {}",
        failures.join("; ")
    )
}

/// The lifecycle control surface (RFC-0027 §5): mount and unmount a nest on a running mounts.
///
/// Mounted on the **outer** router rather than the composed one, which is what avoids a cycle - the
/// inner composition is swapped underneath on every change, so routes living there would be replaced
/// by the very operation that invoked them.
///
/// Gated by the same credential as the admin UI via [`crate::serve::token_ok`], deliberately: who may
/// mount is the operator's gateway's decision, and a second auth concept here would be one more thing
/// to get subtly wrong. `--no-admin` removes these routes entirely, for operators who front their own
/// control plane and want the runtime to have no lifecycle surface at all.
pub fn lifecycle_routes(
    handles: Arc<tokio::sync::Mutex<RuntimeHandles>>,
    jobs: Arc<crate::mount_jobs::MountJobs>,
    admin_enabled: bool,
    admin_token: Option<String>,
) -> axum::Router {
    use crate::mount_jobs::{ClaimError, MountJob, MountPhase};
    use axum::extract::{Path as AxPath, Query, State};
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::{delete, get, post};
    use axum::Json;

    #[derive(serde::Deserialize)]
    struct TokenQuery {
        token: Option<String>,
    }
    #[derive(serde::Deserialize)]
    struct MountQuery {
        token: Option<String>,
        /// Answer once the mount has finished, as every mount did before #1544.
        #[serde(default)]
        wait: bool,
        /// Report what the mount would cost and whether it would be refused, and mount nothing (#1550).
        #[serde(default)]
        dry_run: bool,
    }
    #[derive(serde::Deserialize)]
    struct MountBody {
        name: String,
        /// The dataset identity to mount (RFC-0032 §4): `data/<nid>/`. Optional because a nest already
        /// on record from a prior mount (or the pre-2.0 `nests/<name>` layout) still resolves without
        /// it - see [`RuntimeHandles::mount`]. Required to mount a nest **not already recorded**, i.e.
        /// most 2.0 mounts: a fresh `mounts.toml` runtime has no record to fall back on, so omitting
        /// `nid` there reproduces #517 (silently trying the pre-2.0 `nests/<name>` path instead).
        ///
        /// Untrusted request-body input, so it stays a raw `String` here rather than `Nid` - Serde
        /// has no way to turn a bad value into a clean 400, only a 422 the caller did not ask for.
        /// `mount_nest` runs it through [`Nid::parse`] itself, before the credential-gated work
        /// below even starts (NIG-185).
        nid: Option<String>,
    }

    type Shared = (
        Arc<tokio::sync::Mutex<RuntimeHandles>>,
        Arc<crate::mount_jobs::MountJobs>,
        Option<String>,
    );

    fn unauthorized() -> (StatusCode, Json<serde_json::Value>) {
        (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "admin token required"})),
        )
    }

    /// A body that would not parse, answered after the token check so a caller without one learns
    /// nothing about the request shape.
    fn bad_body(
        e: axum::extract::rejection::JsonRejection,
    ) -> (StatusCode, Json<serde_json::Value>) {
        (
            e.status(),
            Json(serde_json::json!({"error": e.body_text()})),
        )
    }

    /// The `415` a body-less POST answers without a JSON content type, as the bodied ones already do:
    /// a cross-site form can POST without a preflight, but cannot send `application/json` (#1642).
    fn not_json(headers: &HeaderMap) -> Option<(StatusCode, Json<serde_json::Value>)> {
        let json = headers
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.split(';').next())
            .map(|m| m.trim().to_ascii_lowercase())
            .is_some_and(|m| {
                m == "application/json" || (m.starts_with("application/") && m.ends_with("+json"))
            });
        (!json).then(|| {
            (
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                Json(serde_json::json!({
                    "error": "Expected request with `Content-Type: application/json`"
                })),
            )
        })
    }

    if !admin_enabled {
        return axum::Router::new();
    }

    /// Map a refusal to its status code (RFC-0027 §3). Typed rather than string-matched, so the
    /// mapping cannot drift from the reasons.
    fn status_for(err: &anyhow::Error) -> StatusCode {
        err.downcast_ref::<MountRefusal>()
            .and_then(|r| StatusCode::from_u16(r.status()).ok())
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
    }

    fn write_failed(err: &anyhow::Error) -> (StatusCode, Json<serde_json::Value>) {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": format!("{err:#}")})),
        )
    }

    /// Whether a mount of `name` on `nid` is already live, which makes it idempotent (#1646).
    fn repeats_live(h: &RuntimeHandles, name: &str, nid: Option<&str>) -> bool {
        h.states
            .iter()
            .any(|(n, s)| n == name && (nid.is_none() || s.nid.as_deref() == nid))
    }

    async fn mount_nest(
        State((handles, jobs, required)): State<Shared>,
        Query(q): Query<MountQuery>,
        headers: HeaderMap,
        body: Result<Json<MountBody>, axum::extract::rejection::JsonRejection>,
    ) -> (StatusCode, Json<serde_json::Value>) {
        if !crate::serve::token_ok(required.as_deref(), q.token.as_deref(), &headers) {
            return unauthorized();
        }
        let body = match body {
            Ok(Json(body)) => body,
            Err(e) => return bad_body(e),
        };
        if let Err(e) = check_mount_name(&body.name, jobs.default_tenant()) {
            return (
                status_for(&e),
                Json(serde_json::json!({"error": format!("{e:#}")})),
            );
        }
        // Validate before the caller's `nid` touches anything, not after: RuntimeHandles::mount
        // requires an `Nid` and cannot be called without one, but a bad value should read as a
        // caller error (400) rather than the 500 an unwrapped `Result` would produce here.
        let nid = match body.nid.as_deref().map(Nid::parse).transpose() {
            Ok(nid) => nid,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": format!("{e:#}")})),
                )
            }
        };
        let nid_str = nid.as_ref().map(|n| n.as_str().to_string());

        if q.dry_run {
            let plan = {
                let h = handles.lock().await;
                if repeats_live(&h, &body.name, nid_str.as_deref()) {
                    return (
                        StatusCode::OK,
                        Json(serde_json::json!({
                            "name": body.name,
                            "nid": nid_str,
                            "mounted": true,
                            "refusal": null,
                            "refusal_status": null,
                        })),
                    );
                }
                h.plan_mount(&body.name, nid.as_ref())
            };
            let plan = match plan {
                Ok(plan) => plan,
                Err(e) => {
                    return (
                        StatusCode::OK,
                        Json(serde_json::json!({
                            "name": body.name,
                            "refusal": format!("{e:#}"),
                            "refusal_status": e.downcast_ref::<MountRefusal>().map(MountRefusal::status),
                        })),
                    )
                }
            };
            let fetched = match (&plan.fetch_from, &plan.nid) {
                (Some(registry), Some(n)) => {
                    if let Err(e) = fetch_nid(registry, n, &plan.dir, &body.name).await {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(serde_json::json!({"error": format!("{e:#}")})),
                        );
                    }
                    true
                }
                _ => false,
            };
            let h = handles.lock().await;
            return match h.dry_run(&body.name, &plan, fetched).await {
                Ok(report) => (StatusCode::OK, Json(serde_json::json!(report))),
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": format!("{e:#}")})),
                ),
            };
        }

        if q.wait {
            let h = handles.lock().await;
            if repeats_live(&h, &body.name, nid_str.as_deref()) {
                return (
                    StatusCode::OK,
                    Json(serde_json::json!({"mounted": body.name})),
                );
            }
            return match mount_releasing(&handles, h, &body.name, nid, &|| true).await {
                Ok(()) => {
                    if let Err(e) = jobs.put(MountJob::new(
                        &body.name,
                        nid_str.as_deref(),
                        MountPhase::Live,
                    )) {
                        return write_failed(&e);
                    }
                    (
                        StatusCode::OK,
                        Json(serde_json::json!({"mounted": body.name})),
                    )
                }
                Err(e) => (
                    status_for(&e),
                    Json(serde_json::json!({"error": format!("{e:#}")})),
                ),
            };
        }

        // Decided from the jobs alone, never the runtime's lock, which a mount in progress holds.
        if let Some(job) = jobs.get(&body.name) {
            let same = job.nid == nid_str || nid_str.is_none();
            match (job.phase, same) {
                (MountPhase::Live, true) => return (StatusCode::OK, Json(serde_json::json!(job))),
                // A suspended name mounts again on its own NID as its resume.
                (MountPhase::Failed, _) | (MountPhase::Suspended, true) => {}
                (_, true) => return (StatusCode::ACCEPTED, Json(serde_json::json!(job))),
                (_, false) => {
                    return (
                        StatusCode::CONFLICT,
                        Json(serde_json::json!({
                            "error": format!(
                                "'{}' is already {} as nid {}; changing a mount's nest is `POST /_admin/move/{}` with the new nid",
                                body.name,
                                match job.phase {
                                    MountPhase::Live => "mounted",
                                    MountPhase::Suspended => "suspended",
                                    _ => "being mounted",
                                },
                                job.nid.as_deref().unwrap_or("(unrecorded)"),
                                body.name
                            )
                        })),
                    )
                }
            }
        }
        let job = match jobs.claim(MountJob::new(
            &body.name,
            nid_str.as_deref(),
            MountPhase::Accepted,
        )) {
            Ok(job) => job,
            Err(ClaimError::Persist(e)) => return write_failed(&e),
            Err(ClaimError::Running(running)) if running.nid == nid_str => {
                return (StatusCode::ACCEPTED, Json(serde_json::json!(running)))
            }
            Err(ClaimError::Running(running)) => {
                return (StatusCode::CONFLICT, Json(serde_json::json!(running)))
            }
        };
        spawn_mount_job(handles, jobs, body.name, nid, job.generation);
        (StatusCode::ACCEPTED, Json(serde_json::json!(job)))
    }

    #[derive(serde::Deserialize)]
    struct UnmountQuery {
        token: Option<String>,
        /// Also remove the dataset once no mount references it (#1547).
        #[serde(default)]
        reclaim: bool,
    }

    async fn unmount_nest(
        State((handles, jobs, required)): State<Shared>,
        AxPath(name): AxPath<String>,
        Query(q): Query<UnmountQuery>,
        headers: HeaderMap,
    ) -> (StatusCode, Json<serde_json::Value>) {
        if !crate::serve::token_ok(required.as_deref(), q.token.as_deref(), &headers) {
            return unauthorized();
        }
        // A name no mount could have is a caller's mistake. An absent valid name stays a 200 so a
        // retried DELETE is not a failure, but says so, so a typo does not read as an unmount.
        if let Err(e) = check_mount_name(&name, jobs.default_tenant()) {
            return (
                status_for(&e),
                Json(serde_json::json!({"error": format!("{e:#}")})),
            );
        }
        let mut h = handles.lock().await;
        let was_mounted =
            h.states.iter().any(|(n, _)| *n == name) || h.suspended.contains_key(&name);
        let nid = h
            .states
            .iter()
            .find(|(n, _)| *n == name)
            .and_then(|(_, s)| s.nid.as_deref().map(Nid::parse))
            .or_else(|| h.suspended.get(&name).map(|n| Nid::parse(n)))
            .transpose();
        // Refused before the unmount: once the record is gone the caller cannot learn the NID.
        let nid = match nid {
            Ok(Some(nid)) => Some(nid),
            Ok(None) if !q.reclaim || !was_mounted => None,
            Ok(None) => {
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({"error": format!(
                        "'{name}' has no recorded dataset identity to reclaim; it is on the \
                         pre-2.0 layout (`nuthatch migrate`). Nothing was unmounted"
                    )})),
                )
            }
            Err(e) => {
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({"error": format!(
                        "'{name}' records an unreadable nid ({e:#}). Nothing was unmounted"
                    )})),
                )
            }
        };
        // The record is gone from the API by now, so the NID is the caller's only way back to the
        // dataset.
        let failed = |e: String| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "unmounted": name,
                    "nid": nid.as_ref().map(Nid::as_str),
                    "error": e,
                })),
            )
        };
        if let Err(e) = h.unmount(&name).await {
            return failed(format!("{e:#}"));
        }
        if let Err(e) = jobs.forget(&name) {
            return failed(format!("{e:#}"));
        }
        let Some(nid) = nid.as_ref().filter(|_| q.reclaim) else {
            return (
                StatusCode::OK,
                Json(serde_json::json!({"unmounted": name, "was_mounted": was_mounted})),
            );
        };
        match h.reclaim(nid, &jobs) {
            Ok(r) => (
                StatusCode::OK,
                Json(serde_json::json!({"unmounted": name, "reclaim": r})),
            ),
            Err(e) => failed(format!("{e:#}")),
        }
    }

    /// Reclaim a dataset unmounted earlier without `?reclaim=true` (#1547).
    async fn reclaim_dataset(
        State((handles, jobs, required)): State<Shared>,
        AxPath(nid): AxPath<String>,
        Query(q): Query<TokenQuery>,
        headers: HeaderMap,
    ) -> (StatusCode, Json<serde_json::Value>) {
        if !crate::serve::token_ok(required.as_deref(), q.token.as_deref(), &headers) {
            return unauthorized();
        }
        let nid = match Nid::parse(&nid) {
            Ok(nid) => nid,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": format!("{e:#}")})),
                )
            }
        };
        let h = handles.lock().await;
        match h.reclaim(&nid, &jobs) {
            Ok(r) => {
                let status = match r {
                    crate::prune::Reclaim::Reclaimed { .. } => StatusCode::OK,
                    crate::prune::Reclaim::Kept { .. } => StatusCode::CONFLICT,
                    crate::prune::Reclaim::Absent { .. } => StatusCode::NOT_FOUND,
                };
                (status, Json(serde_json::json!(r)))
            }
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": format!("{e:#}")})),
            ),
        }
    }

    async fn list_mounts(
        State((_, jobs, required)): State<Shared>,
        Query(q): Query<TokenQuery>,
        headers: HeaderMap,
    ) -> (StatusCode, Json<serde_json::Value>) {
        if !crate::serve::token_ok(required.as_deref(), q.token.as_deref(), &headers) {
            return unauthorized();
        }
        (
            StatusCode::OK,
            Json(serde_json::json!({"mounts": jobs.list()})),
        )
    }

    async fn get_mount(
        State((_, jobs, required)): State<Shared>,
        AxPath(name): AxPath<String>,
        Query(q): Query<TokenQuery>,
        headers: HeaderMap,
    ) -> (StatusCode, Json<serde_json::Value>) {
        if !crate::serve::token_ok(required.as_deref(), q.token.as_deref(), &headers) {
            return unauthorized();
        }
        match jobs.get(&name) {
            Some(job) => (StatusCode::OK, Json(serde_json::json!(job))),
            None => (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": format!("no mount named '{name}'")})),
            ),
        }
    }

    async fn suspend_nest(
        State((handles, jobs, required)): State<Shared>,
        AxPath(name): AxPath<String>,
        Query(q): Query<TokenQuery>,
        headers: HeaderMap,
    ) -> (StatusCode, Json<serde_json::Value>) {
        if !crate::serve::token_ok(required.as_deref(), q.token.as_deref(), &headers) {
            return unauthorized();
        }
        if let Some(refused) = not_json(&headers) {
            return refused;
        }
        let mut h = handles.lock().await;
        if !h.states.iter().any(|(n, _)| *n == name) {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": format!("'{name}' is not mounted")})),
            );
        }
        match h.suspend(&name).await {
            Ok(()) => {
                let nid = h.suspended.get(&name).map(String::as_str);
                if let Err(e) = jobs.put(MountJob::new(&name, nid, MountPhase::Suspended)) {
                    return write_failed(&e);
                }
                (StatusCode::OK, Json(serde_json::json!({"suspended": name})))
            }
            Err(e) => (
                StatusCode::CONFLICT,
                Json(serde_json::json!({"error": format!("{e:#}")})),
            ),
        }
    }

    #[derive(serde::Deserialize)]
    struct MoveBody {
        nid: String,
    }

    async fn move_nest(
        State((handles, jobs, required)): State<Shared>,
        AxPath(name): AxPath<String>,
        Query(q): Query<MountQuery>,
        headers: HeaderMap,
        body: Result<Json<MoveBody>, axum::extract::rejection::JsonRejection>,
    ) -> (StatusCode, Json<serde_json::Value>) {
        if !crate::serve::token_ok(required.as_deref(), q.token.as_deref(), &headers) {
            return unauthorized();
        }
        let body = match body {
            Ok(Json(body)) => body,
            Err(e) => return bad_body(e),
        };
        if let Err(e) = check_mount_name(&name, jobs.default_tenant()) {
            return (
                status_for(&e),
                Json(serde_json::json!({"error": format!("{e:#}")})),
            );
        }
        let nid = match Nid::parse(&body.nid) {
            Ok(nid) => nid,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": format!("{e:#}")})),
                )
            }
        };
        if q.wait {
            let h = handles.lock().await;
            return match move_releasing(&handles, h, &name, nid.clone(), &|| true).await {
                Ok(()) => {
                    if let Err(e) =
                        jobs.put(MountJob::new(&name, Some(nid.as_str()), MountPhase::Live))
                    {
                        return write_failed(&e);
                    }
                    (
                        StatusCode::OK,
                        Json(serde_json::json!({"moved": name, "nid": nid.as_str()})),
                    )
                }
                Err(e) => (
                    status_for(&e),
                    Json(serde_json::json!({"error": format!("{e:#}")})),
                ),
            };
        }
        if let Some(job) = jobs.get(&name).filter(|j| !j.phase.finished()) {
            return (StatusCode::CONFLICT, Json(serde_json::json!(job)));
        }
        let mut job = MountJob::new(&name, Some(nid.as_str()), MountPhase::Accepted);
        job.is_move = true;
        let job = match jobs.claim(job) {
            Ok(job) => job,
            Err(ClaimError::Persist(e)) => return write_failed(&e),
            Err(ClaimError::Running(running)) => {
                return (StatusCode::CONFLICT, Json(serde_json::json!(running)))
            }
        };
        spawn_move_job(handles, jobs, name, nid, job.generation);
        (StatusCode::ACCEPTED, Json(serde_json::json!(job)))
    }

    async fn resume_nest(
        State((handles, jobs, required)): State<Shared>,
        AxPath(name): AxPath<String>,
        Query(q): Query<MountQuery>,
        headers: HeaderMap,
    ) -> (StatusCode, Json<serde_json::Value>) {
        if !crate::serve::token_ok(required.as_deref(), q.token.as_deref(), &headers) {
            return unauthorized();
        }
        if let Some(refused) = not_json(&headers) {
            return refused;
        }
        let h = handles.lock().await;
        let Some(nid) = h.suspended.get(&name).and_then(|n| Nid::parse(n).ok()) else {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": format!("'{name}' is not suspended")})),
            );
        };
        if q.wait {
            return match mount_releasing(&handles, h, &name, Some(nid.clone()), &|| true).await {
                Ok(()) => {
                    if let Err(e) =
                        jobs.put(MountJob::new(&name, Some(nid.as_str()), MountPhase::Live))
                    {
                        return write_failed(&e);
                    }
                    (StatusCode::OK, Json(serde_json::json!({"resumed": name})))
                }
                Err(e) => (
                    status_for(&e),
                    Json(serde_json::json!({"error": format!("{e:#}")})),
                ),
            };
        }
        drop(h);
        if let Some(job) = jobs.get(&name).filter(|j| !j.phase.finished()) {
            return (StatusCode::ACCEPTED, Json(serde_json::json!(job)));
        }
        let job = match jobs.claim(MountJob::new(
            &name,
            Some(nid.as_str()),
            MountPhase::Accepted,
        )) {
            Ok(job) => job,
            Err(ClaimError::Persist(e)) => return write_failed(&e),
            Err(ClaimError::Running(running)) => {
                return (StatusCode::ACCEPTED, Json(serde_json::json!(running)))
            }
        };
        spawn_mount_job(handles, jobs, name, Some(nid), job.generation);
        (StatusCode::ACCEPTED, Json(serde_json::json!(job)))
    }

    axum::Router::new()
        .route("/_admin/nests", post(mount_nest))
        .route("/_admin/nests/{*name}", delete(unmount_nest))
        .route("/_admin/mounts", get(list_mounts))
        .route("/_admin/mounts/{*name}", get(get_mount))
        .route("/_admin/suspend/{*name}", post(suspend_nest))
        .route("/_admin/resume/{*name}", post(resume_nest))
        .route("/_admin/move/{*name}", post(move_nest))
        .route("/_admin/datasets/{nid}", delete(reclaim_dataset))
        .with_state((handles, jobs, admin_token))
}

/// Persist the mounted-nest list to `mounts.toml` (RFC-0027 §5).
///
/// This is the embedded stand-in for RFC-0022's control-plane DB: desired state lives in the *same*
/// file the static path reads, so a restart converges on whatever the operator last asked for. Without
/// it, a mount would silently vanish on the next restart - the worst kind of bug, because it looks
/// like it worked.
///
/// Written temp-then-rename so a crash mid-write cannot leave a runtime with a truncated manifest and no
/// nests at all.
///
/// The conflict this creates is named rather than left to be discovered: **at runtime nuthatch owns
/// this list.** An operator who manages `mounts.toml` with configuration management should run
/// `--no-admin` and restart to change the set, because fighting a config-management tool over a file
/// is a losing game.
fn persist_mounted_nests(
    dir: &Path,
    nests: &[String],
    known: &[Mount],
    suspended: &[String],
) -> Result<()> {
    let path = dir.join(MOUNTS_FILE);
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("reading {} to persist the nest list", path.display()))?;
    let mut mounts: MountTable = toml::from_str(&raw)
        .with_context(|| format!("parsing {} before rewriting it", path.display()))?;
    // `nests` here are **route keys** - tenant-qualified when the runtime is multi-tenant - because
    // that is what the serving layer and the admin API name a mount by. Matching them against
    // `alias` alone would drop every mount in a multi-tenant mounts on the first unmount.
    let default_tenant_owned = mounts.tenant_default();
    let default_tenant = default_tenant_owned.as_str();
    let key_of = |m: &Mount| {
        MountRef {
            tenant: m.tenant.clone(),
            alias: m.alias.clone(),
        }
        .route_key(default_tenant)
    };
    // A runtime that started empty has neither list nor records, and its first mount must be
    // recorded by nid rather than by name (#1545). Only a file already on the pre-2.0 list, or with no
    // nid to record, stays on the list.
    if mounts.mounts.is_empty() && (!mounts.runtime.nests.is_empty() || known.is_empty()) {
        mounts.runtime.nests = nests.to_vec();
    } else {
        // Drop the records for mounts that are gone, or the next `load` refuses the file it just
        // wrote. **The dataset under `data/<nid>` is deliberately left on disk** - RFC-0032 §5 makes
        // collection explicit, because re-backfilling is precisely the cost this design exists to
        // avoid and an accidental unmount must not trigger one.
        mounts.mounts.retain(|m| nests.contains(&key_of(m)));
        // A move changes a live name's nid (#1549); the record on disk follows the one in memory.
        for m in mounts.mounts.iter_mut() {
            if let Some(k) = known.iter().find(|k| key_of(k) == key_of(m)) {
                m.nid = k.nid.clone();
            }
        }
        // Add records for live mounts with no entry on disk yet - a nest mounted live via `POST
        // /_admin/nests` rather than declared at boot (#517). Without this the mount works until the
        // next restart, then silently vanishes: the exact "looks like it worked" failure this file
        // exists to prevent (see the doc comment above).
        // A suspended mount keeps its record: it is only paused (#1548).
        for key in nests.iter().chain(suspended) {
            if mounts.mounts.iter().any(|m| &key_of(m) == key) {
                continue;
            }
            // A live mount that introduces a second tenant is stored under `tenant/alias`, while
            // this file is still single-tenant and `key_of` returns the alias alone (#1534).
            if let Some(m) = known
                .iter()
                .find(|m| &key_of(m) == key || format!("{}/{}", m.tenant, m.alias) == *key)
            {
                mounts.mounts.push(m.clone());
            }
        }
        mounts.runtime.nests.clear(); // `[[mounts]]` is authoritative; a stale list beside it lies
    }
    mounts.runtime.suspended = suspended.to_vec();
    let out = toml::to_string_pretty(&mounts).context("serialising mounts.toml")?;
    #[cfg(test)]
    persisted_tables()
        .lock()
        .unwrap()
        .push((dir.to_path_buf(), out.clone()));
    let tmp = path.with_extension("toml.tmp");
    std::fs::write(&tmp, out).with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, &path).with_context(|| format!("replacing {}", path.display()))?;
    Ok(())
}

/// Every `mounts.toml` a test build writes, by runtime directory, so a test can see each one (#1647).
#[cfg(test)]
fn persisted_tables() -> &'static std::sync::Mutex<Vec<(PathBuf, String)>> {
    static WRITTEN: std::sync::OnceLock<std::sync::Mutex<Vec<(PathBuf, String)>>> =
        std::sync::OnceLock::new();
    WRITTEN.get_or_init(Default::default)
}

/// The handles a runtime driver keeps so it can change its nest set while running (RFC-0027 §6).
///
/// Before this, `runtime::dev` moved every `AppState` into the composed router and kept nothing, so the
/// only way to change the mounted set was to restart the process - which stops every co-tenant nest
/// too. Retaining them is what makes an unmount possible at all.
pub struct RuntimeHandles {
    /// The swappable composition being served (RFC-0027 slice 1).
    pub live: crate::serve::LiveRuntime,
    /// Per-nest serving state, in roster order.
    pub states: Vec<(String, crate::serve::AppState)>,
    /// Alert delivery workers keyed by nest - each holds that nest's `Store` clone.
    pub alert_workers: Vec<(String, tokio::task::JoinHandle<()>)>,
    /// Mirrors keyed by mount route (RFC-0052 S2). Dropping one stops it.
    pub publishers: Vec<(String, crate::publish::Publisher)>,
    /// Chain -> that cursor's command channel.
    pub lifecycle: std::collections::HashMap<
        String,
        tokio::sync::mpsc::UnboundedSender<indexer::CursorCommand>,
    >,
    pub health: Arc<crate::health::RuntimeHealth>,
    /// The static half of the roster, re-merged with live health per request. [`RuntimeHandles::mount`]
    /// and [`RuntimeHandles::unmount`] rebuild `roster["nests"]` from `states` on every change (#554) -
    /// this is not just the startup snapshot.
    pub roster: serde_json::Value,
    /// Per-nest projected RSS, so a mount can price the cursor it is joining without re-reading
    /// every co-tenant's config.
    pub estimates: std::collections::HashMap<String, u64>,
    /// Whether the runtime serves more than one tenant, for [`build_roster_entries`]'s route keys.
    /// Frozen at startup like `mount_ctx.mounts` - tenancy shape is not something a mount changes.
    pub default_tenant: String,
    /// Suspended mounts (#1548), route key to NID: kept on disk and in `mounts.toml`, answering 503.
    pub suspended: std::collections::BTreeMap<String, String>,
    /// Mounts admitted onto a running cursor and still catching up beside it, with no lock held.
    pub joining: JoiningMounts,
    /// What a mount needs that an unmount does not: where nests live, how to reach each chain, and the
    /// settings a new nest must be built with so it behaves identically to one mounted at boot.
    pub mount_ctx: MountContext,
}

/// The operator's run-time settings, stamped onto every mounted config at boot and at a hot mount alike.
/// Neither is identity: each is `#[serde(skip)]` on `Config`.
pub fn stamp_operator_settings(
    config: &mut Config,
    freshness: crate::freshness::Freshness,
    ipfs_window_deadline: std::time::Duration,
) {
    config.freshness = freshness;
    config.ipfs_window_deadline = ipfs_window_deadline;
}

/// The context a running mounts needs in order to build and admit a nest (RFC-0027 §3).
///
/// Deliberately captured at startup rather than re-derived per mount: a nest mounted at 3am must be
/// built with the same backfill mode, concurrency, window and admin posture as its co-tenants, or two
/// nests in one mounts would behave differently for no reason an operator could see.
#[derive(Clone)]
pub struct MountContext {
    /// The runtime directory; a nest lives at `data/<nid>/`, or `nests/<name>/` if un-migrated.
    pub dir: PathBuf,
    /// The mount records as of startup (RFC-0032 §4), so a live mount resolves its dataset the same
    /// way the static path does. A runtime mount of a nest with no record still resolves through
    /// the pre-2.0 layout - a half-migrated mounts is a supported state, not an accident.
    pub mounts: Vec<Mount>,
    /// Chain -> the source driving that chain's cursor. A nest whose chain is absent cannot be mounted.
    pub sources: std::collections::HashMap<String, Arc<dyn Source>>,
    /// Chain -> the endpoints that source spreads over. A mounted nest fetches through it, so this is
    /// what `safe_backfill_concurrency` guards, not the nest's own `rpc_urls`.
    pub endpoint_counts: std::collections::HashMap<String, usize>,
    pub backfill: Option<u64>,
    pub seal_direct: bool,
    pub concurrency: usize,
    /// `--ipfs-window-deadline`, stamped onto a hot-mounted nest's config like `freshness` (#1399).
    pub ipfs_window_deadline: std::time::Duration,
    pub window_override: Option<u64>,
    pub admin_enabled: bool,
    pub admin_token: Option<String>,
    /// The per-cursor RSS ceiling a mount must not breach (`CLAUDE.md`; RFC-0021 §0).
    pub max_rss_mb: u64,
    /// The runtime's freshness dial (RFC-0040), stamped onto a hot-mounted nest's config so it runs
    /// at the cadence its co-tenants do.
    pub freshness: crate::freshness::Freshness,
    /// Chain -> the dial its cursor runs at, once `freshness` has been settled on its block time.
    pub chain_freshness: std::collections::HashMap<String, crate::freshness::Freshness>,
    /// Declared chains nothing mounted onto at boot (#1545). Their RPC is not dialled until the first
    /// mount onto one, so an unused chain, or one whose endpoint is down, cannot stop the runtime.
    pub dormant: std::collections::HashMap<String, DormantChain>,
    pub fail_fast: bool,
    /// Where a cursor that a mount spawns goes to be supervised. `None` without the admin surface,
    /// which is the only thing that mounts.
    pub cursors: Option<CursorFeed>,
    /// `--registry`: where a mount fetches a NID this runtime does not hold (#1543).
    pub registry: Option<String>,
}

/// A cursor's ingest task, handed to [`supervise_cursors`] after boot.
pub type CursorFeed =
    tokio::sync::mpsc::UnboundedSender<(String, tokio::task::JoinHandle<Result<()>>)>;

/// A declared chain with no cursor yet: its endpoint, with `--rpc` already applied, and its fallbacks.
#[derive(Clone, Debug)]
pub struct DormantChain {
    pub endpoint: ChainEndpoint,
    pub rpc_fallback: Vec<String>,
}

/// Dial a chain's RPC pool, check it is the chain declared, and settle the freshness dial on it.
async fn open_chain(
    endpoint: &ChainEndpoint,
    rpc_fallback: Vec<String>,
    runtime: &str,
    freshness: crate::freshness::Freshness,
) -> Result<(Arc<dyn Source>, crate::freshness::Freshness)> {
    if endpoint.rpc_urls.is_empty() {
        bail!(
            "mounts '{runtime}' chain {} has no rpc_urls (set them under [[chains]], or pass --rpc for a \
             single-chain runtime)",
            endpoint.chain
        );
    }
    // Verify the whole pool is on this chain (issue #150): pasting one chain's endpoint under
    // another's `[[chains]]` entry is an easy slip, and failover would mask it indefinitely.
    let rpc = RpcClient::with_fallbacks(endpoint.rpc_urls.clone(), rpc_fallback)?;
    rpc.verify_chain_ids(endpoint.chain_id)
        .await
        .with_context(|| {
            format!(
                "verifying rpc_urls for mounts '{runtime}' cursor on {}",
                endpoint.chain
            )
        })?;
    let dial = freshness.for_chain(&endpoint.chain, &rpc).await;
    Ok((Arc::new(rpc), dial))
}

/// Why a mount was refused (RFC-0027 §3). Typed so the control surface can map each to its status
/// code without parsing strings.
#[derive(Debug)]
pub enum MountRefusal {
    /// Mounting over a live name is an *upgrade*, and that is RFC-0020's job.
    AlreadyMounted(String),
    /// The mounts declares no cursor for this nest's chain. Adding a chain at runtime is a non-goal.
    UndeclaredChain { nest: String, chain: String },
    /// The cursor's projected footprint would exceed its ceiling. A refusal, not a warning - the
    /// budget stops being a budget the moment it becomes advisory.
    OverBudget {
        nest: String,
        chain: String,
        projected_mb: u64,
        ceiling_mb: u64,
    },
    /// The runtime does not hold this NID and has no registry to fetch it from (#1543).
    NotHeld { nid: String },
    /// The chain's cursor died; its quarantine holds until restart (#1545).
    CursorStopped { nest: String, chain: String },
    /// A name boot would refuse, so it may never be persisted.
    InvalidName(String),
    /// The name is suspended on another NID. Resuming it is a mount of that NID (#1645).
    Suspended { name: String, nid: String },
    /// The cursor would carry more authored entities than it admits (RFC-0041 §7).
    EntityCeiling {
        mounts: String,
        chain: String,
        on_cursor: usize,
        nests: usize,
    },
    /// The runtime seals direct, which would serve the nest's authored entities empty (#1537).
    SealDirectEntities { nest: String },
    /// Another mount of this name or dataset is still catching up (RFC-0027 §4).
    Joining { nest: String, by: String },
}

impl std::fmt::Display for MountRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MountRefusal::AlreadyMounted(n) => write!(
                f,
                "nest '{n}' is already mounted - changing a mounted nest is `POST /_admin/move/{n}`, not a mount"
            ),
            MountRefusal::UndeclaredChain { nest, chain } => write!(
                f,
                "nest '{nest}' is on {chain}, which this runtime declares no cursor for - add it under \
                 [[chains]] and restart"
            ),
            MountRefusal::OverBudget {
                nest,
                chain,
                projected_mb,
                ceiling_mb,
            } => write!(
                f,
                "mounting '{nest}' would put the {chain} cursor at ~{projected_mb} MB against a \
                 {ceiling_mb} MB ceiling - raise max_rss_mb, unmount something, or use another mounts"
            ),
            MountRefusal::InvalidName(why) => write!(f, "{why}"),
            MountRefusal::Joining { nest, by } => write!(
                f,
                "'{nest}' cannot mount while '{by}' is still catching up on that name or dataset; \
                 retry once it is live or has failed"
            ),
            MountRefusal::Suspended { name, nid } => write!(
                f,
                "'{name}' is suspended as nid {nid}; resume it, or unmount it before mounting \
                 another nid under its name"
            ),
            MountRefusal::CursorStopped { nest, chain } => write!(
                f,
                "the cursor on {chain} has stopped; restart the runtime to mount '{nest}' onto it"
            ),
            MountRefusal::NotHeld { nid } => write!(
                f,
                "this runtime does not hold nid {nid} and has no registry to fetch it from - start it \
                 with --registry"
            ),
            MountRefusal::EntityCeiling {
                mounts,
                chain,
                on_cursor,
                nests,
            } => write!(
                f,
                "mounts '{mounts}' cursor on {chain} declares {on_cursor} authored entities across {nests} \
                 nest(s), over the ceiling of {MAX_ENTITIES_PER_CURSOR} per cursor. Each entity is a circuit \
                 on its own thread; nests sharing a chain share the cursor and are counted together \
                 (RFC-0041 §7). Move some to another chain's runtime, or drop {} of them.",
                on_cursor - MAX_ENTITIES_PER_CURSOR,
            ),
            MountRefusal::SealDirectEntities { nest } => write!(
                f,
                "nest '{nest}' declares authored incremental entities, which this runtime's \
                 --seal-direct would serve empty; restart it without --seal-direct to mount this nest"
            ),
        }
    }
}

impl std::error::Error for MountRefusal {}

impl MountRefusal {
    /// The HTTP status a refusal is answered with, typed so the mapping cannot drift from the reasons.
    pub fn status(&self) -> u16 {
        match self {
            MountRefusal::AlreadyMounted(_)
            | MountRefusal::Suspended { .. }
            | MountRefusal::Joining { .. }
            | MountRefusal::UndeclaredChain { .. }
            | MountRefusal::CursorStopped { .. }
            | MountRefusal::SealDirectEntities { .. } => 409,
            MountRefusal::OverBudget { .. } | MountRefusal::EntityCeiling { .. } => 507,
            MountRefusal::NotHeld { .. } => 404,
            MountRefusal::InvalidName(_) => 400,
        }
    }
}

/// How long to wait for a cursor to acknowledge that it has released a nest.
///
/// Generous, because the cursor applies lifecycle commands at a **window boundary** - it may be
/// mid-window against a slow provider when the command arrives. Timing out is not a failure of the
/// unmount so much as a refusal to guess: we would rather report that the cursor has not let go than
/// tear the routes down while it is still writing.
const UNMOUNT_ACK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

type CursorTx = tokio::sync::mpsc::UnboundedSender<indexer::CursorCommand>;

/// Rename a nest on a cursor, failing if the cursor did not apply it.
async fn rename_on(tx: &CursorTx, chain: &str, from: &str, to: &str) -> Result<()> {
    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
    tx.send(indexer::CursorCommand::Rename {
        from: from.to_string(),
        to: to.to_string(),
        ack: Some(ack_tx),
    })
    .map_err(|_| anyhow::anyhow!("the {chain} cursor is gone"))?;
    match tokio::time::timeout(UNMOUNT_ACK_TIMEOUT, ack_rx).await {
        Ok(Ok(true)) => Ok(()),
        Ok(Ok(false)) => bail!("the {chain} cursor could not rename '{from}' to '{to}'"),
        _ => bail!("the {chain} cursor did not acknowledge renaming '{from}' to '{to}'"),
    }
}

/// A move's cursor half: `name`'s key passes to `holder` (a mount left on the shared dataset), then to
/// the `incoming` staged nest. If the second rename fails the first is undone, so a failed move leaves
/// the cursor as the runtime's own records still describe it.
async fn swap_cursor_keys(
    tx: &CursorTx,
    chain: &str,
    name: &str,
    incoming: Option<&str>,
    holder: Option<&str>,
) -> Result<()> {
    if let Some(holder) = holder {
        rename_on(tx, chain, name, holder).await?;
    }
    let Some(incoming) = incoming else {
        return Ok(());
    };
    let Err(e) = rename_on(tx, chain, incoming, name).await else {
        return Ok(());
    };
    if let Some(holder) = holder {
        if let Err(back) = rename_on(tx, chain, holder, name).await {
            tracing::error!("the {chain} cursor could not give '{name}' back its key: {back:#}");
        }
    }
    Err(e)
}

/// `name` is a route key: `<alias>` single-tenant, `<tenant>/<alias>` multi-tenant. Split so a record
/// lookup works either way, rather than missing every multi-tenant mount and falling through to the
/// pre-2.0 directory - which would come up empty and re-backfill.
fn split_route_key(name: &str) -> (Option<&str>, &str) {
    match name.split_once('/') {
        Some((t, a)) => (Some(t), a),
        None => (None, name),
    }
}

/// What a mount would cost this runtime, and whether it would be refused (#1550).
#[derive(Debug, serde::Serialize)]
pub struct DryRun {
    pub name: String,
    pub nid: Option<String>,
    /// Fetched from the registry by this dry run. The verified nest stays installed; nothing else
    /// is written.
    pub fetched: bool,
    /// The mount would serve a dataset this mount already indexes, at no further cost.
    pub shares: Option<String>,
    pub chain: Option<String>,
    pub start_block: Option<u64>,
    /// Known only for a chain whose cursor is running; a dry run dials nothing.
    pub tip: Option<u64>,
    pub blocks_to_backfill: Option<u64>,
    /// A dataset is already on disk, so `blocks_to_backfill` is an upper bound.
    pub has_data: bool,
    /// Extraction that costs RPC calls per block, beyond the shared `getLogs`.
    pub per_block_rpc: Vec<&'static str>,
    pub incoming_mb: Option<u64>,
    pub projected_mb: Option<u64>,
    pub ceiling_mb: u64,
    /// What a real mount would answer, if it would be refused.
    pub refusal: Option<String>,
    pub refusal_status: Option<u16>,
}

/// What [`RuntimeHandles::admit`] found: the chain, how to reach its cursor, and what it would cost.
struct Admission {
    chain: String,
    dormant: Option<DormantChain>,
    lifecycle: Option<tokio::sync::mpsc::UnboundedSender<indexer::CursorCommand>>,
    incoming_mb: u64,
    projected_mb: u64,
}

/// Where a mount's dataset lives, and the registry to fetch it from when it is not there yet.
#[derive(Debug)]
pub struct MountPlan {
    pub nid: Option<String>,
    pub dir: PathBuf,
    pub fetch_from: Option<String>,
}

/// A mount admitted onto a running cursor and still catching up beside it (RFC-0027 §4).
#[derive(Clone, Debug)]
pub struct Joining {
    pub name: String,
    pub chain: String,
    pub dir: PathBuf,
    pub incoming_mb: u64,
}

/// Every mount between admission and its join. Their catch-ups run with no lock held, so this is
/// how admission still prices them and a second mount of the name or dataset is refused.
#[derive(Clone, Default)]
pub struct JoiningMounts(Arc<std::sync::Mutex<Vec<Joining>>>);

impl JoiningMounts {
    fn reserve(&self, joining: Joining) -> Reservation {
        let name = joining.name.clone();
        self.held().push(joining);
        Reservation {
            mounts: self.clone(),
            name,
        }
    }

    pub fn list(&self) -> Vec<Joining> {
        self.held().clone()
    }

    fn held(&self) -> std::sync::MutexGuard<'_, Vec<Joining>> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// A mount's place in [`JoiningMounts`], given back when dropped, so a mount abandoned at any await
/// cannot keep it.
struct Reservation {
    mounts: JoiningMounts,
    name: String,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.mounts.held().retain(|j| j.name != self.name);
    }
}

/// How far [`RuntimeHandles::begin_mount`] got.
enum Begun {
    /// Mounted and routed: a shared dataset, or the nest that started its chain's cursor.
    Mounted,
    /// Admitted and reserved; it catches up, then joins.
    Join(Box<PendingJoin>),
}

/// What a join needs once its nest has caught up.
struct JoinMeta {
    name: String,
    tenant: Option<String>,
    alias: String,
    nid: Option<String>,
    chain: String,
    lifecycle: tokio::sync::mpsc::UnboundedSender<indexer::CursorCommand>,
    incoming: u64,
    record: Option<Mount>,
    surface: crate::allowlist::Surface,
    _reservation: Reservation,
}

type Built = (
    indexer::NestIngest,
    crate::serve::AppState,
    Option<tokio::task::JoinHandle<()>>,
    u64,
);

/// A mount admitted onto a running cursor, not yet built.
struct PendingJoin {
    meta: JoinMeta,
    source: Arc<dyn Source>,
    dataset: PreparedDataset,
    config: Config,
    backfill: Option<u64>,
    seal_direct: bool,
    concurrency: usize,
    window_override: Option<u64>,
    admin_enabled: bool,
    admin_token: Option<String>,
    sql_gate: Arc<tokio::sync::Semaphore>,
    /// The names the cursor knows its other nests by, to read how far it has got.
    co_tenants: Vec<String>,
}

impl PendingJoin {
    /// Phase 1 (RFC-0027 §4): build the nest and catch it up to its cursor, needing no lock.
    async fn catch_up(
        self,
        wanted: &(dyn Fn() -> bool + Send + Sync),
    ) -> (JoinMeta, Result<Built>) {
        let co_tenants = self.co_tenants;
        let cursor_at = move || {
            co_tenants
                .iter()
                .filter_map(|k| crate::metrics::METRICS.nest_if_known(k))
                .map(|m| m.last_block())
                .max()
        };
        let name = self.meta.name.clone();
        let built = indexer::build_and_prepare_nest(
            &self.source,
            self.dataset,
            &self.config,
            self.backfill,
            self.seal_direct,
            self.concurrency,
            self.window_override,
            self.admin_enabled,
            self.admin_token,
            None,
            self.sql_gate,
            indexer::CatchUp {
                cursor_at: &cursor_at,
                wanted,
            },
        )
        .await
        .with_context(|| format!("preparing nest '{name}' for mount"));
        (self.meta, built)
    }
}

/// Drop a join that is no longer wanted, releasing its store before the caller can retry.
async fn abandon_join(meta: JoinMeta, built: Result<Built>, why: anyhow::Error) -> anyhow::Error {
    drop(meta);
    match built {
        Ok((nest, state, worker, _)) => {
            drop(nest);
            indexer::release_refused(why, state, worker).await
        }
        Err(_) => why,
    }
}

/// [`RuntimeHandles::mount`] for a caller holding the runtime's lock as `h`: it is let go while the
/// nest is fetched, built and caught up, and taken again to admit it and to join it (#2134).
async fn mount_releasing(
    handles: &Arc<tokio::sync::Mutex<RuntimeHandles>>,
    mut h: tokio::sync::MutexGuard<'_, RuntimeHandles>,
    name: &str,
    nid: Option<Nid>,
    wanted: &(dyn Fn() -> bool + Send + Sync),
) -> Result<()> {
    let plan = h.plan_mount(name, nid.as_ref())?;
    let fetched = match (&plan.fetch_from, &plan.nid) {
        (Some(registry), Some(n)) => {
            drop(h);
            fetch_nid(registry, n, &plan.dir, name).await?;
            h = handles.lock().await;
            true
        }
        _ => false,
    };
    let mounted = match h.begin_mount(name, nid).await {
        Ok(Begun::Mounted) => Ok(()),
        Ok(Begun::Join(pending)) => {
            drop(h);
            let (meta, built) = pending.catch_up(wanted).await;
            h = handles.lock().await;
            if wanted() {
                h.finish_join(meta, built).await
            } else {
                let why = anyhow::anyhow!("mount of '{name}' was unmounted before it joined");
                Err(abandon_join(meta, built, why).await)
            }
        }
        Err(e) => Err(e),
    };
    // Only a fetch no live mount has since taken up is ours to remove.
    if mounted.is_err() && fetched && !h.states.iter().any(|(_, s)| s.dir == plan.dir) {
        let _ = std::fs::remove_dir_all(&plan.dir);
    }
    mounted
}

/// [`RuntimeHandles::move_name`] as [`mount_releasing`] is a mount: the staged nest catches up with no
/// lock held, so the name's routes switch only once it has.
async fn move_releasing(
    handles: &Arc<tokio::sync::Mutex<RuntimeHandles>>,
    h: tokio::sync::MutexGuard<'_, RuntimeHandles>,
    name: &str,
    nid: Nid,
    wanted: &(dyn Fn() -> bool + Send + Sync),
) -> Result<()> {
    let Some(staging) = h.move_staging(name, &nid)? else {
        return Ok(());
    };
    mount_releasing(handles, h, &staging, Some(nid.clone()), wanted).await?;
    let mut h = handles.lock().await;
    if !wanted() {
        let cause = anyhow::anyhow!("move of '{name}' was unmounted before it moved");
        return Err(h.abandon_move(&staging, cause).await);
    }
    h.complete_move(name, &staging, nid).await
}

async fn fetch_nid(registry: &str, nid: &str, dir: &Path, name: &str) -> Result<()> {
    crate::distribution::install_by_nid(registry, nid, dir)
        .await
        .with_context(|| format!("fetching nid {nid} from the registry"))?;
    tracing::info!("fetched nid {nid} from the registry for '{name}'");
    Ok(())
}

/// The mount jobs a runtime starts with (#1544): every mounted nest as live, and every job a restart
/// interrupted running again. Nothing resumes without the admin API, which is what mounts.
pub async fn start_mount_jobs(
    dir: &Path,
    handles: &Arc<tokio::sync::Mutex<RuntimeHandles>>,
    admin_enabled: bool,
) -> Result<Arc<crate::mount_jobs::MountJobs>> {
    use crate::mount_jobs::{MountJob, MountJobs, MountPhase};
    // Before any job runs, so a fetch a killed process left staged cannot be mistaken for one running.
    crate::mount_jobs::clear_stale_fetches(dir);
    let tenant = handles.lock().await.default_tenant.clone();
    let jobs = Arc::new(MountJobs::load(dir).with_default_tenant(&tenant));
    {
        let h = handles.lock().await;
        for (name, state) in &h.states {
            // A move interrupted by the restart is live on its old nest and not done.
            if jobs
                .get(name)
                .is_some_and(|j| j.is_move && !j.phase.finished())
            {
                continue;
            }
            jobs.put(MountJob::new(name, state.nid.as_deref(), MountPhase::Live))?;
        }
        for (name, nid) in &h.suspended {
            jobs.put(MountJob::new(name, Some(nid), MountPhase::Suspended))?;
        }
    }
    if admin_enabled {
        for job in jobs.unfinished() {
            tracing::info!("resuming the mount of '{}' a restart interrupted", job.name);
            let nid = job.nid.as_deref().and_then(|n| Nid::parse(n).ok());
            match (job.is_move, nid) {
                (true, Some(nid)) => {
                    spawn_move_job(handles.clone(), jobs.clone(), job.name, nid, job.generation);
                }
                (_, nid) => {
                    let generation = job.generation;
                    spawn_mount_job(handles.clone(), jobs.clone(), job.name, nid, generation);
                }
            }
        }
    }
    Ok(jobs)
}

#[cfg(test)]
type MountJoinFn = std::sync::Arc<dyn Fn() + Send + Sync>;
#[cfg(test)]
type MountJoinHook =
    std::sync::Mutex<std::collections::HashMap<(std::path::PathBuf, String), MountJoinFn>>;

/// The gap between a job's fetch and its join, by runtime dir and job name, so another test's job of
/// the same name never runs it. A test forgets the job here (#1638).
#[cfg(test)]
fn before_mount_join() -> &'static MountJoinHook {
    static HOOK: std::sync::OnceLock<MountJoinHook> = std::sync::OnceLock::new();
    HOOK.get_or_init(Default::default)
}

/// Calls to `RuntimeHandles::mount`, by runtime dir, so another test's mount cannot move this one's
/// count (#1820).
#[cfg(test)]
fn mount_entries() -> &'static std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, usize>>
{
    static ENTRIES: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, usize>>,
    > = std::sync::OnceLock::new();
    ENTRIES.get_or_init(Default::default)
}

#[cfg(test)]
fn mount_entries_in(dir: &Path) -> usize {
    mount_entries()
        .lock()
        .unwrap()
        .get(dir)
        .copied()
        .unwrap_or(0)
}

/// Run a mount as a job (#1544). The fetch and the catch-up happen with no lock held, so neither holds
/// up another admin call; only admission and the join take the runtime's lock (#2134).
pub fn spawn_mount_job(
    handles: Arc<tokio::sync::Mutex<RuntimeHandles>>,
    jobs: Arc<crate::mount_jobs::MountJobs>,
    name: String,
    nid: Option<Nid>,
    generation: u64,
) -> tokio::task::JoinHandle<()> {
    use crate::mount_jobs::MountPhase;
    let wanted = nid.as_ref().map(|n| n.as_str().to_string());
    tokio::spawn(async move {
        let outcome = async {
            let plan = handles.lock().await.plan_mount(&name, nid.as_ref())?;
            let fetched = match (&plan.fetch_from, &plan.nid) {
                (Some(registry), Some(n)) => {
                    jobs.advance_if_owned(&name, generation, MountPhase::Fetching, None)?;
                    fetch_nid(registry, n, &plan.dir, &name).await?;
                    true
                }
                _ => false,
            };
            jobs.advance_if_owned(&name, generation, MountPhase::Joining, None)?;
            #[cfg(test)]
            {
                let runtime_dir = handles.lock().await.mount_ctx.dir.clone();
                let hook = before_mount_join()
                    .lock()
                    .unwrap()
                    .get(&(runtime_dir, name.clone()))
                    .cloned();
                if let Some(hook) = hook {
                    hook();
                }
            }
            // The unmount holds this lock across forget. A check before it still joins (#1638).
            let h = handles.lock().await;
            if !jobs.owns(&name, generation) {
                if fetched && !h.states.iter().any(|(_, s)| s.dir == plan.dir) {
                    let _ = std::fs::remove_dir_all(&plan.dir);
                }
                bail!("mount of '{name}' was unmounted before it joined");
            }
            let owned = || jobs.owns(&name, generation);
            let mounted = mount_releasing(&handles, h, &name, nid, &owned).await;
            let h = handles.lock().await;
            // Only a fetch no live mount has since taken up is ours to remove.
            if mounted.is_err() && fetched && !h.states.iter().any(|(_, s)| s.dir == plan.dir) {
                let _ = std::fs::remove_dir_all(&plan.dir);
            }
            mounted
        }
        .await;
        // Already mounted on the very NID asked for is this job's goal reached, not a failure.
        let reached = |h: &RuntimeHandles| {
            h.states.iter().any(|(n, s)| {
                *n == name && (wanted.is_none() || s.nid.as_deref() == wanted.as_deref())
            })
        };
        match outcome {
            Ok(()) => {
                if let Err(e) = jobs.advance_if_owned(&name, generation, MountPhase::Live, None) {
                    tracing::warn!("'{name}' is mounted but the job file was not updated: {e:#}");
                }
            }
            Err(e)
                if matches!(
                    e.downcast_ref::<MountRefusal>(),
                    Some(MountRefusal::AlreadyMounted(_))
                ) && reached(&*handles.lock().await) =>
            {
                if let Err(e) = jobs.advance_if_owned(&name, generation, MountPhase::Live, None) {
                    tracing::warn!("'{name}' is mounted but the job file was not updated: {e:#}");
                }
            }
            Err(e) if !jobs.owns(&name, generation) => {
                tracing::info!("mount of '{name}' stopped before it joined: {e:#}");
            }
            Err(e) => {
                tracing::warn!("mounting '{name}' failed: {e:#}");
                if let Err(write) = jobs.advance_if_owned(
                    &name,
                    generation,
                    MountPhase::Failed,
                    Some(format!("{e:#}")),
                ) {
                    tracing::warn!(
                        "mount of '{name}' failed and the job file was not updated: {write:#}"
                    );
                }
            }
        }
    })
}

/// The mount table without its suspended records, and those records by route key to NID (#1548).
/// A suspended mount is neither loaded nor indexed at boot, but its record and data stay.
pub fn split_suspended(
    table: &MountTable,
) -> (MountTable, std::collections::BTreeMap<String, String>) {
    let default_tenant_owned = table.tenant_default();
    let default_tenant = default_tenant_owned.as_str();
    let key_of = |m: &Mount| {
        MountRef {
            tenant: m.tenant.clone(),
            alias: m.alias.clone(),
        }
        .route_key(default_tenant)
    };
    let mut active = table.clone();
    let mut suspended = std::collections::BTreeMap::new();
    active.mounts.retain(|m| {
        let key = key_of(m);
        if table.runtime.suspended.contains(&key) {
            suspended.insert(key, m.nid.clone());
            false
        } else {
            true
        }
    });
    (active, suspended)
}

/// What a suspended mount serves: a 503 that says so, rather than the 404 of a mount that is gone.
fn suspended_router(name: &str) -> axum::Router {
    let body = serde_json::json!({
        "error": format!("'{name}' is suspended; POST /_admin/resume/{name} to resume it"),
        "suspended": true,
    });
    axum::Router::new().fallback(move || {
        let body = body.clone();
        async move {
            (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                axum::Json(body),
            )
        }
    })
}

/// Run a move as a job (#1549): fetch and catch up the new NID with no lock held, then move under it.
pub fn spawn_move_job(
    handles: Arc<tokio::sync::Mutex<RuntimeHandles>>,
    jobs: Arc<crate::mount_jobs::MountJobs>,
    name: String,
    nid: Nid,
    generation: u64,
) -> tokio::task::JoinHandle<()> {
    use crate::mount_jobs::MountPhase;
    tokio::spawn(async move {
        let outcome = async {
            let staging = RuntimeHandles::staging_name(&name);
            let plan = handles.lock().await.plan_mount(&staging, Some(&nid))?;
            let fetched = match (&plan.fetch_from, &plan.nid) {
                (Some(registry), Some(n)) => {
                    jobs.advance_if_owned(&name, generation, MountPhase::Fetching, None)?;
                    fetch_nid(registry, n, &plan.dir, &name).await?;
                    true
                }
                _ => false,
            };
            jobs.advance_if_owned(&name, generation, MountPhase::Joining, None)?;
            crate::crash::point("move:before-join");
            let h = handles.lock().await;
            // As for a mount: a claim that was forgotten, or replaced, must not move the name (#1638).
            if !jobs.owns(&name, generation) {
                if fetched && !h.states.iter().any(|(_, s)| s.dir == plan.dir) {
                    let _ = std::fs::remove_dir_all(&plan.dir);
                }
                bail!("move of '{name}' was unmounted before it moved");
            }
            let owned = || jobs.owns(&name, generation);
            let moved = move_releasing(&handles, h, &name, nid, &owned).await;
            let h = handles.lock().await;
            // As for a mount: a refused move removes only a fetch no live mount has since taken up.
            if moved.is_err() && fetched && !h.states.iter().any(|(_, s)| s.dir == plan.dir) {
                let _ = std::fs::remove_dir_all(&plan.dir);
            }
            moved
        }
        .await;
        match outcome {
            Ok(()) => {
                if let Err(e) = jobs.advance_if_owned(&name, generation, MountPhase::Live, None) {
                    tracing::warn!("'{name}' is moved but the job file was not updated: {e:#}");
                }
            }
            Err(e) if !jobs.owns(&name, generation) => {
                tracing::info!("move of '{name}' stopped before it moved: {e:#}");
            }
            Err(e) => {
                tracing::warn!("moving '{name}' failed: {e:#}");
                if let Err(write) = jobs.advance_if_owned(
                    &name,
                    generation,
                    MountPhase::Failed,
                    Some(format!("{e:#}")),
                ) {
                    tracing::warn!(
                        "move of '{name}' failed and the job file was not updated: {write:#}"
                    );
                }
            }
        }
    })
}

/// The SQL surface a mount serves over `dataset`: its record's, or open with none, bounded by the
/// author's ceiling (RFC-0034 §3). Refused when the record widens the ceiling.
fn served_surface(
    record: Option<&Mount>,
    dataset: &Path,
    route: &str,
) -> Result<crate::allowlist::Surface> {
    let surface = record.map(Mount::surface).unwrap_or_default();
    match crate::allowlist::Ceiling::load(dataset)? {
        Some(ceiling) => surface.within(&ceiling, route),
        None => Ok(surface),
    }
}

/// Stamp a mount's dataset identity, record and [`served_surface`] onto the state served under its
/// name. Every path that installs a state under a name goes through here: boot, mount, resume and
/// move (#1536, #2129).
fn overlay_mount_record(
    state: &mut crate::serve::AppState,
    nid: Option<&str>,
    record: Option<&Mount>,
    surface: crate::allowlist::Surface,
) {
    state.nid = nid.map(Arc::from);
    state.surface = Arc::new(surface);
    #[cfg(feature = "counter")]
    {
        state.counter = record.and_then(|r| r.counter.clone()).map(Arc::new);
    }
    #[cfg(not(feature = "counter"))]
    let _ = record;
}

impl RuntimeHandles {
    /// Mount a nest into the running mounts (RFC-0027 §3-§4).
    ///
    /// Admission first, work second: every refusal is decided before a store is opened or a block is
    /// fetched, so a rejected mount costs nothing and leaves nothing behind.
    ///
    /// Then phase 1 - build and `prepare` the nest **outside** the cursor, so it catches up on its own
    /// before joining. Phase 2 hands it over at a window boundary. Doing it the other way round would
    /// drag every co-tenant back to the new nest's start block, because the cursor advances from the
    /// min of its live nests.
    ///
    /// Routes appear only after the cursor has acknowledged, so a nest is never reachable before it is
    /// actually indexing.
    ///
    /// `nid` is the dataset identity to mount (RFC-0032 §4). It is optional because a nest already on
    /// record - from a prior mount this session, or a pre-2.0 `mounts.toml` with no `[[mounts]]` at
    /// all - still resolves without it. It is **required** to mount a nest with no existing record,
    /// which in a migrated (2.0) runtime is the common case: there is no `nests/<name>` to fall back
    /// on, so omitting it there used to resolve `data/<nid>/`'s nest against the pre-2.0 layout and
    /// fail (#517).
    ///
    /// Takes `Option<Nid>`, not `Option<&str>` - an admin caller's `nid` used to reach
    /// [`MountTable::data_dir`] unvalidated, and `Path::join` discards the mount root entirely when
    /// given an absolute string. [`Nid::parse`] is the only way to produce one, so nothing can call
    /// this function with a raw string.
    pub async fn mount(&mut self, name: &str, nid: Option<Nid>) -> Result<()> {
        let plan = self.plan_mount(name, nid.as_ref())?;
        // A NID this runtime does not hold is fetched first (#1543). Whatever refuses the mount after
        // that removes the fetch again, so a refusal still leaves nothing behind.
        let fetched = match (&plan.fetch_from, &plan.nid) {
            (Some(registry), Some(nid)) => {
                fetch_nid(registry, nid, &plan.dir, name).await?;
                true
            }
            _ => false,
        };
        let mounted = match self.begin_mount(name, nid).await {
            Ok(Begun::Mounted) => Ok(()),
            Ok(Begun::Join(pending)) => {
                let (meta, built) = pending.catch_up(&|| true).await;
                self.finish_join(meta, built).await
            }
            Err(e) => Err(e),
        };
        if mounted.is_err() && fetched {
            let _ = std::fs::remove_dir_all(&plan.dir);
        }
        mounted
    }

    /// The part of a mount that needs the runtime's lock before its catch-up, once its dataset is on
    /// disk: admission, and for a nest joining a running cursor, its reservation.
    async fn begin_mount(&mut self, name: &str, nid: Option<Nid>) -> Result<Begun> {
        #[cfg(test)]
        {
            *mount_entries()
                .lock()
                .unwrap()
                .entry(self.mount_ctx.dir.clone())
                .or_insert(0) += 1;
        }
        let plan = self.plan_mount(name, nid.as_ref())?;
        let (tenant, alias) = split_route_key(name);
        self.begin_dataset(name, tenant, alias, plan.nid, plan.dir)
            .await
    }

    /// Every check that decides whether a nest may join its chain's cursor (RFC-0027 §3). A mount and
    /// its dry run both call this, so they cannot disagree about a refusal (#1550).
    fn admit(&self, name: &str, config: &Config, dir: &Path) -> Result<Admission> {
        let chain = config.nest.chain.clone();
        let dormant = self
            .mount_ctx
            .dormant
            .get(&chain)
            .filter(|d| d.endpoint.chain_id == config.nest.chain_id)
            .cloned();
        if !self.mount_ctx.sources.contains_key(&chain) && dormant.is_none() {
            return Err(MountRefusal::UndeclaredChain {
                nest: name.to_string(),
                chain,
            }
            .into());
        }
        // A chain with no running cursor gets one (#1545), unless it had one that died: that
        // quarantine holds until restart, and nests may still be serving from it.
        let lifecycle = self
            .lifecycle
            .get(&chain)
            .filter(|tx| !tx.is_closed())
            .cloned();
        if lifecycle.is_none()
            && (self.health.cursor_quarantined(&chain)
                || self.states.iter().any(|(_, s)| s.chain == chain))
        {
            return Err(MountRefusal::CursorStopped {
                nest: name.to_string(),
                chain,
            }
            .into());
        }
        // The budget check is the reason this is a refusal rather than a warning: `CLAUDE.md`'s
        // per-cursor ceiling stops being a budget the moment a mount may quietly exceed it. Projected
        // against *this cursor's* current membership, not the whole mounts - the ceiling is per cursor.
        let has_labels = !crate::labels::load(dir).is_empty();
        let (entity_count, entity_rows) = declared_entities(dir);
        if self.mount_ctx.seal_direct && entity_count > 0 {
            return Err(MountRefusal::SealDirectEntities {
                nest: name.to_string(),
            }
            .into());
        }
        let incoming_mb = estimate_nest_rss_mb(config, has_labels, entity_count, entity_rows);
        let joining: Vec<Joining> = self
            .joining
            .list()
            .into_iter()
            .filter(|j| j.chain == chain)
            .collect();
        let existing: u64 = self
            .states
            .iter()
            .filter(|(_, s)| s.chain == chain)
            .map(|(n, _)| self.estimates.get(n).copied().unwrap_or(NEST_BASE_RSS_MB))
            .chain(joining.iter().map(|j| j.incoming_mb))
            .sum();
        let projected_mb = RUNTIME_BASE_RSS_MB + existing + incoming_mb;
        // An alias clones the dataset's state, so counting every state would tax a shared nest
        // twice. Boot groups by dataset; this does the same by directory.
        let runtime = self.roster["runtime"].as_str().unwrap_or("runtime");
        let mut seen_dirs = std::collections::HashSet::new();
        let mut already = 0usize;
        let mut nests_here = 0usize;
        for (_, state) in self.states.iter().filter(|(_, s)| s.chain == chain) {
            if !seen_dirs.insert(state.dir.clone()) {
                continue;
            }
            already += declared_entities(&state.dir).0;
            nests_here += 1;
        }
        for j in &joining {
            if seen_dirs.insert(j.dir.clone()) {
                already += declared_entities(&j.dir).0;
                nests_here += 1;
            }
        }
        // A second name for a directory already on the cursor shares that dataset.
        let (on_cursor, nests) = if seen_dirs.contains(dir) {
            (already, nests_here)
        } else {
            (already + entity_count, nests_here + 1)
        };
        refuse_over_entity_ceiling(on_cursor, nests, runtime, &chain)?;
        if projected_mb > self.mount_ctx.max_rss_mb {
            return Err(MountRefusal::OverBudget {
                nest: name.to_string(),
                chain,
                projected_mb,
                ceiling_mb: self.mount_ctx.max_rss_mb,
            }
            .into());
        }
        Ok(Admission {
            chain,
            dormant,
            lifecycle,
            incoming_mb,
            projected_mb,
        })
    }

    /// A mount's dry run (#1550), once [`Self::plan_mount`] has placed its dataset on disk. Runs the
    /// same admission a mount does and changes nothing.
    pub async fn dry_run(&self, name: &str, plan: &MountPlan, fetched: bool) -> Result<DryRun> {
        let mut report = DryRun {
            name: name.to_string(),
            nid: plan.nid.clone(),
            fetched,
            shares: None,
            chain: None,
            start_block: None,
            tip: None,
            blocks_to_backfill: None,
            has_data: plan.dir.join(crate::config::DB_FILE).exists(),
            per_block_rpc: Vec::new(),
            incoming_mb: None,
            projected_mb: None,
            ceiling_mb: self.mount_ctx.max_rss_mb,
            refusal: None,
            refusal_status: None,
        };
        if let Some((holder, _)) = self.states.iter().find(|(_, s)| s.dir == plan.dir) {
            report.shares = Some(holder.clone());
            return Ok(report);
        }
        let config = Config::load(&plan.dir)
            .with_context(|| format!("loading nest '{name}' from {}", plan.dir.display()))?;
        report.chain = Some(config.nest.chain.clone());
        report.start_block = self
            .mount_ctx
            .backfill
            .or_else(|| config.contracts.iter().filter_map(|c| c.start_block).min());
        let x = &config.extract;
        for (on, what) in [
            (x.blocks, "[extract] blocks"),
            (x.traces, "[extract] traces"),
            (x.top_level_calls, "[extract] top_level_calls"),
            (x.state, "[extract] state"),
            (!config.calls.is_empty(), "[[calls]]"),
        ] {
            if on {
                report.per_block_rpc.push(what);
            }
        }
        match self.admit(name, &config, &plan.dir) {
            Ok(adm) => {
                report.incoming_mb = Some(adm.incoming_mb);
                report.projected_mb = Some(adm.projected_mb);
            }
            Err(e) => {
                report.refusal_status = e.downcast_ref::<MountRefusal>().map(MountRefusal::status);
                report.refusal = Some(format!("{e:#}"));
            }
        }
        // What the running cursor last saw: this holds the runtime's lock, so it dials nothing.
        report.tip = self
            .states
            .iter()
            .filter(|(_, s)| s.chain == config.nest.chain)
            .filter_map(|(n, s)| {
                let key = s.runtime_health.as_ref().map_or(n, |(k, _)| k);
                crate::metrics::METRICS.nest_if_known(key).map(|m| m.tip())
            })
            .filter(|&tip| tip > 0)
            .max();
        report.blocks_to_backfill = report
            .tip
            .zip(report.start_block)
            .map(|(tip, start)| tip.saturating_sub(start));
        Ok(report)
    }

    /// Resolve a mount's dataset and whether it must be fetched first, refusing what can be refused
    /// without doing any work.
    pub fn plan_mount(&self, name: &str, nid: Option<&Nid>) -> Result<MountPlan> {
        if self.states.iter().any(|(n, _)| n == name) {
            return Err(MountRefusal::AlreadyMounted(name.to_string()).into());
        }
        if self.joining.list().iter().any(|j| j.name == name) {
            return Err(MountRefusal::Joining {
                nest: name.to_string(),
                by: name.to_string(),
            }
            .into());
        }
        let (tenant, alias) = split_route_key(name);
        for part in tenant.into_iter().chain([alias]) {
            if let Err(e) = safe_segment(part, "mount name part") {
                return Err(MountRefusal::InvalidName(format!("{e:#}")).into());
            }
        }
        // Keys are per mount (#1565): a name spelling out the default tenant would be served under
        // it now and by alias alone after a restart.
        if tenant == Some(self.default_tenant.as_str()) {
            return Err(MountRefusal::InvalidName(format!(
                "'{name}' names the default tenant; mount it as '{alias}'"
            ))
            .into());
        }
        if let Err(e) = refuse_reserved_mount(tenant, alias) {
            return Err(MountRefusal::InvalidName(format!("{e:#}")).into());
        }
        if let Err(e) = refuse_route_collisions(self.routes().chain([name])) {
            return Err(MountRefusal::InvalidName(format!("{e:#}")).into());
        }
        if let (Some(held), Some(asked)) = (self.suspended.get(name), nid) {
            if held != asked.as_str() {
                return Err(MountRefusal::Suspended {
                    name: name.to_string(),
                    nid: held.clone(),
                }
                .into());
            }
        }
        // The caller's `nid` wins over any existing record - it names the dataset to mount, not a
        // request to overwrite one. It falls back to a record from a prior mount/load only when the
        // caller does not know it, e.g. remounting a nest this runtime has already seen.
        let nid = nid.map(|n| n.as_str().to_string()).or_else(|| {
            self.mount_ctx
                .mounts
                .iter()
                .find(|m| mount_route(m, &self.default_tenant) == name)
                .map(|m| m.nid.clone())
        });
        let dir = match &nid {
            Some(nid) => MountTable::data_dir(&self.mount_ctx.dir, nid),
            None => MountTable::nest_dir(&self.mount_ctx.dir, alias),
        };
        let fetch_from = match &nid {
            Some(nid) if !dir.exists() => match &self.mount_ctx.registry {
                Some(registry) => Some(registry.clone()),
                None => return Err(MountRefusal::NotHeld { nid: nid.clone() }.into()),
            },
            _ => None,
        };
        Ok(MountPlan {
            nid,
            dir,
            fetch_from,
        })
    }

    /// Everything of a mount up to its catch-up: admission, and the whole mount when it shares a
    /// dataset or starts its chain's cursor. A nest joining a running cursor comes back reserved.
    async fn begin_dataset(
        &mut self,
        name: &str,
        tenant: Option<&str>,
        alias: &str,
        nid: Option<String>,
        dir: PathBuf,
    ) -> Result<Begun> {
        if let Some(j) = self.joining.list().into_iter().find(|j| j.dir == dir) {
            return Err(MountRefusal::Joining {
                nest: name.to_string(),
                by: j.name,
            }
            .into());
        }
        // A dataset another live mount already holds is served through a clone of that mount's state,
        // as `fan_out_aliases` does at boot. Opening its store again is refused by redb (#1475).
        let shared = self
            .states
            .iter()
            .find(|(_, s)| s.dir == dir)
            .map(|(n, s)| (n.clone(), s.clone()));
        let (state, worker, incoming, cursor_key) = match shared {
            Some((holder, mut state)) => {
                let cursor_key = state
                    .runtime_health
                    .as_ref()
                    .map_or(holder, |(n, _)| n.clone());
                self.health.register_alias(name, &cursor_key, &state.chain);
                tracing::info!(
                    "nest '{name}' mounted onto the dataset '{cursor_key}' already indexes"
                );
                // The surface is the mount's own, never its co-tenant's (RFC-0034 §2).
                let record = self.surface_record(name);
                let surface = served_surface(record.as_ref(), &dir, name)?;
                let held = state.nid.clone();
                overlay_mount_record(&mut state, held.as_deref(), record.as_ref(), surface);
                (state, None, 0, cursor_key)
            }
            None => {
                let mut config = Config::load(&dir)
                    .with_context(|| format!("loading nest '{name}' from {}", dir.display()))?;
                let dial = self
                    .mount_ctx
                    .chain_freshness
                    .get(&config.nest.chain)
                    .copied()
                    .unwrap_or(self.mount_ctx.freshness);
                stamp_operator_settings(&mut config, dial, self.mount_ctx.ipfs_window_deadline);
                let Admission {
                    chain,
                    dormant,
                    lifecycle,
                    incoming_mb: incoming,
                    ..
                } = self.admit(name, &config, &dir)?;
                let record = self.surface_record(name);
                let surface = served_surface(record.as_ref(), &dir, name)?;

                // After the budget, so a refused mount never dials anything.
                if let Some(d) = dormant {
                    let runtime = self.roster["runtime"]
                        .as_str()
                        .unwrap_or("runtime")
                        .to_string();
                    let (source, dial) = open_chain(
                        &d.endpoint,
                        d.rpc_fallback,
                        &runtime,
                        self.mount_ctx.freshness,
                    )
                    .await?;
                    self.mount_ctx.sources.insert(chain.clone(), source);
                    self.mount_ctx
                        .endpoint_counts
                        .insert(chain.clone(), d.endpoint.rpc_urls.len());
                    self.mount_ctx.chain_freshness.insert(chain.clone(), dial);
                    self.mount_ctx.dormant.remove(&chain);
                    stamp_operator_settings(&mut config, dial, self.mount_ctx.ipfs_window_deadline);
                }
                let Some(source) = self.mount_ctx.sources.get(&chain).cloned() else {
                    return Err(MountRefusal::UndeclaredChain {
                        nest: name.to_string(),
                        chain,
                    }
                    .into());
                };

                // The early cutoff (RFC-0033 §5), which this path did not apply until #414: an operator who
                // edits a nest cosmetically and mounts it into a running runtime re-indexed from the start
                // block, where a restart would have adopted the predecessor's dataset and re-indexed
                // nothing. The mount API exists precisely so operators need not restart, so the cheaper path
                // was the one that paid.
                //
                // After the budget refusal deliberately: an over-budget mount is turned away without first
                // copying a dataset for a nest that is not going to run.
                let dataset_dir = dir.clone();
                let prepared = prepare_dataset(&self.mount_ctx.dir, dir, nid.as_deref(), name);

                // Guarded by the chain's endpoints, as at boot. A chain missing from the count is capped to one.
                let concurrency = indexer::backfill_concurrency_for(
                    self.mount_ctx
                        .endpoint_counts
                        .get(&chain)
                        .copied()
                        .unwrap_or(1),
                    self.mount_ctx.concurrency,
                    self.mount_ctx.seal_direct,
                    &[name],
                );
                config.route = Some(name.to_string());

                let (state, worker) = match lifecycle {
                    None => {
                        // The first nest on this chain (#1545). It starts the chain's cursor exactly as boot
                        // would, so it backfills inside that cursor and serves while it does, as at boot.
                        self.health.register(name, &chain);
                        // An earlier unmount of this name left it recorded as retired.
                        self.health.mark_indexing(name);
                        let mut cursor = indexer::spawn_runtime(
                            source,
                            vec![(name.to_string(), prepared.into_dir(), config)],
                            self.mount_ctx.backfill,
                            self.mount_ctx.seal_direct,
                            concurrency,
                            self.mount_ctx.window_override,
                            self.mount_ctx.admin_enabled,
                            self.mount_ctx.admin_token.clone(),
                            self.health.clone(),
                            self.mount_ctx.fail_fast,
                        )
                        .await
                        .with_context(|| format!("starting the {chain} cursor for '{name}'"))?;
                        let (_, mut state) =
                            cursor.states.pop().expect("one nest in, one state out");
                        let worker = cursor.alert_workers.pop().map(|(_, w)| w);
                        overlay_mount_record(&mut state, nid.as_deref(), record.as_ref(), surface);
                        self.lifecycle.insert(chain.clone(), cursor.lifecycle);
                        match &self.mount_ctx.cursors {
                            Some(feed) => {
                                if feed.send((chain.clone(), cursor.ingest)).is_err() {
                                    tracing::warn!(
                                        "the {chain} cursor started while the runtime was stopping"
                                    );
                                }
                            }
                            None => drop(cursor.ingest),
                        }
                        tracing::info!("nest '{name}' started the {chain} cursor");
                        (state, worker)
                    }
                    Some(lifecycle) => {
                        // Phase 1 runs with no lock held, so the nest's place on the cursor is
                        // reserved here, priced as admission priced it.
                        let sql_gate = self
                            .states
                            .iter()
                            .find(|(_, s)| s.chain == chain)
                            .map(|(_, s)| Arc::clone(&s.sql_gate))
                            .unwrap_or_else(crate::serve::new_sql_gate);
                        let co_tenants = self
                            .states
                            .iter()
                            .filter(|(_, s)| s.chain == chain)
                            .map(|(n, s)| {
                                s.runtime_health
                                    .as_ref()
                                    .map_or_else(|| n.clone(), |(k, _)| k.clone())
                            })
                            .collect();
                        let reservation = self.joining.reserve(Joining {
                            name: name.to_string(),
                            chain: chain.clone(),
                            dir: dataset_dir,
                            incoming_mb: incoming,
                        });
                        return Ok(Begun::Join(Box::new(PendingJoin {
                            meta: JoinMeta {
                                name: name.to_string(),
                                tenant: tenant.map(str::to_string),
                                alias: alias.to_string(),
                                nid,
                                chain,
                                lifecycle,
                                incoming,
                                record,
                                surface,
                                _reservation: reservation,
                            },
                            source,
                            dataset: prepared,
                            config,
                            backfill: self.mount_ctx.backfill,
                            seal_direct: self.mount_ctx.seal_direct,
                            concurrency,
                            window_override: self.mount_ctx.window_override,
                            admin_enabled: self.mount_ctx.admin_enabled,
                            admin_token: self.mount_ctx.admin_token.clone(),
                            sql_gate,
                            co_tenants,
                        })));
                    }
                };
                (state, worker, incoming, name.to_string())
            }
        };
        self.publish_mount(
            name,
            tenant,
            alias,
            nid,
            state,
            worker,
            incoming,
            &cursor_key,
        )?;
        Ok(Begun::Mounted)
    }

    /// [`Self::mount`] once the dataset directory is resolved and on disk.
    #[cfg(test)]
    async fn mount_dataset(
        &mut self,
        name: &str,
        tenant: Option<&str>,
        alias: &str,
        nid: Option<String>,
        dir: PathBuf,
    ) -> Result<()> {
        match self.begin_dataset(name, tenant, alias, nid, dir).await? {
            Begun::Mounted => Ok(()),
            Begun::Join(pending) => {
                let (meta, built) = pending.catch_up(&|| true).await;
                self.finish_join(meta, built).await
            }
        }
    }

    /// Phase 2 (RFC-0027 §4): hand a caught-up nest to its cursor at a window boundary, wait for it
    /// to be in the set, and only then route it.
    async fn finish_join(&mut self, meta: JoinMeta, built: Result<Built>) -> Result<()> {
        let JoinMeta {
            name,
            tenant,
            alias,
            nid,
            chain,
            lifecycle,
            incoming,
            record,
            surface,
            _reservation,
        } = meta;
        let name = name.as_str();
        let (nest, mut state, worker, next) = built?;
        state.runtime_health = Some((name.to_string(), self.health.clone()));
        self.health.register(name, &chain);
        // `/sql` provenance names the dataset that answered (RFC-0035 §3, src/serve.rs:1161-1172), and
        // this is the only place that knows it at mount time: `nid` above is already the resolved
        // identity (the caller's, or the record's for a remount), the same one `dir` was derived from
        // and the same one persisted into the mount record below. `dev()`'s startup path stamps this
        // from the dataset scan (`ds_nid_for`); a live mount has no such scan to run, so it must stamp
        // it here or serve `nid: null` until the next restart (#557).
        overlay_mount_record(&mut state, nid.as_deref(), record.as_ref(), surface);

        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        let refused = if lifecycle
            .send(indexer::CursorCommand::Mount {
                nest: Box::new(nest),
                next,
                ack: Some(ack_tx),
            })
            .is_err()
        {
            Some(anyhow::anyhow!(
                "cursor on {chain} is gone; cannot mount '{name}'"
            ))
        } else {
            match tokio::time::timeout(UNMOUNT_ACK_TIMEOUT, ack_rx).await {
                Ok(Ok(())) => None,
                Ok(Err(_)) => Some(anyhow::anyhow!(
                    "cursor on {chain} stopped while mounting '{name}'"
                )),
                Err(_) => Some(anyhow::anyhow!(
                    "cursor on {chain} did not acknowledge mounting '{name}' within {}s",
                    UNMOUNT_ACK_TIMEOUT.as_secs()
                )),
            }
        };
        if let Some(e) = refused {
            // Neither the worker's `JoinHandle` nor its abort stops a delivery already on the
            // blocking pool, and a retry cannot open the store until that lets go (#1535, #1768).
            return Err(indexer::release_refused(e, state, worker).await);
        }
        tracing::info!("nest '{name}' mounted onto the {chain} cursor at block {next}");
        self.publish_mount(
            name,
            tenant.as_deref(),
            &alias,
            nid,
            state,
            worker,
            incoming,
            name,
        )
    }

    /// Route a mount the cursor already holds, record it and persist.
    #[allow(clippy::too_many_arguments)]
    fn publish_mount(
        &mut self,
        name: &str,
        tenant: Option<&str>,
        alias: &str,
        nid: Option<String>,
        state: crate::serve::AppState,
        worker: Option<tokio::task::JoinHandle<()>>,
        incoming: u64,
        cursor_key: &str,
    ) -> Result<()> {
        // Only now do the routes appear.
        if let Some(worker) = worker {
            self.alert_workers.push((name.to_string(), worker));
        }
        // The cursor already holds the nest, so a publisher that will not start is reported rather
        // than unwinding a mount that is otherwise complete.
        if let Some(record) = self
            .mount_ctx
            .mounts
            .iter()
            .find(|m| mount_route(m, &self.default_tenant) == name)
        {
            if let (Some(publish), Some(_)) = (&record.publish, &nid) {
                match spawn_publisher(
                    &self.mount_ctx.dir,
                    record,
                    publish,
                    &self.default_tenant,
                    cursor_key,
                ) {
                    Ok((_, publisher)) => self.publishers.push((name.to_string(), publisher)),
                    Err(e) => {
                        tracing::error!("mounted '{name}' but its mirror did not start: {e:#}")
                    }
                }
            }
        }
        self.estimates.insert(name.to_string(), incoming);
        self.states.push((name.to_string(), state));
        // A 2.0 mount gets (or refreshes) its record here, so the persist just below can write it into
        // `mounts.toml` even though it had none on disk - the durable half of the fix, matching what
        // unmount already does for removal. A pre-2.0 mount (no `nid`) has no record to keep; `persist`
        // already falls back to the flat `nests` list for that layout.
        if let Some(nid) = &nid {
            let record_tenant = tenant.map_or_else(|| self.default_tenant.clone(), str::to_string);
            match self
                .mount_ctx
                .mounts
                .iter_mut()
                .find(|m| m.alias == alias && m.tenant == record_tenant)
            {
                Some(m) => m.nid = nid.clone(),
                None => self.mount_ctx.mounts.push(Mount {
                    tenant: record_tenant,
                    alias: alias.to_string(),
                    nid: nid.clone(),
                    sql: Default::default(),
                    queries: Vec::new(),
                    publish: None,
                    #[cfg(feature = "counter")]
                    counter: None,
                }),
            }
        }
        // The roster (`GET /nests`) is rebuilt from the live `states`, not patched - #554 was exactly
        // this step missing, which left the roster reporting its startup snapshot while the mount
        // otherwise worked in full: dataset resolved, cursor caught up, routes serving.
        let datasets = live_datasets(
            &self.mount_ctx.dir,
            &self.states,
            &self.mount_ctx.mounts,
            &self.default_tenant,
        );
        self.roster["nests"] = serde_json::json!(build_roster_entries(
            &self.states,
            &datasets,
            &self.default_tenant,
            &self.estimates,
        ));
        // A mount of a suspended name is its resume; it stays suspended until the join has landed.
        self.suspended.remove(name);
        self.live.swap(self.compose()?);
        self.persist()?;
        Ok(())
    }

    /// The routes to serve: every live mount, and a 503 for each suspended one not live again yet.
    pub fn compose(&self) -> Result<axum::Router> {
        // `plan_mount` refuses a colliding name before it joins; this keeps a miss from panicking.
        refuse_route_collisions(self.routes())?;
        let mut router = crate::serve::compose_runtime(
            self.roster.clone(),
            self.states.clone(),
            self.health.clone(),
        );
        for name in self.suspended.keys() {
            if !self.states.iter().any(|(n, _)| n == name) {
                router = router.nest(&format!("/{name}"), suspended_router(name));
            }
        }
        Ok(router)
    }

    /// Every name [`Self::compose`] serves: the live mounts and the suspended ones.
    fn routes(&self) -> impl Iterator<Item = &str> {
        self.states
            .iter()
            .map(|(n, _)| n.as_str())
            .chain(self.suspended.keys().map(String::as_str))
    }

    /// Suspend a mount (#1548): drain it off its cursor as an unmount does, but keep its record and
    /// its data, answer 503 in its place, and keep it suspended across a restart.
    pub async fn suspend(&mut self, name: &str) -> Result<()> {
        let Some((_, state)) = self.states.iter().find(|(n, _)| n == name) else {
            bail!("'{name}' is not mounted");
        };
        let Some(nid) = state.nid.as_deref().map(str::to_string) else {
            bail!("'{name}' has no recorded nid to resume from; `nuthatch migrate` records one");
        };
        // Suspended before the unmount writes the table, so its one write keeps the record (#1647).
        self.suspended.insert(name.to_string(), nid);
        let unmounted = self.unmount(name).await;
        if self.states.iter().any(|(n, _)| n == name) {
            self.suspended.remove(name);
            return unmounted;
        }
        self.health.suspend_nest(name);
        unmounted?;
        tracing::info!("nest '{name}' suspended");
        Ok(())
    }

    /// Write the current mounted set to `mounts.toml`.
    ///
    /// The live change has already happened and is not undone if the rewrite fails. The error
    /// is returned so the route does not report success for a change a restart will drop.
    fn persist(&self) -> Result<()> {
        // A move's staging mount is never recorded: a restart mid-move keeps the old nest, and the
        // move job, resumed, stages the new one again.
        let names: Vec<String> = self
            .states
            .iter()
            .map(|(n, _)| n.clone())
            .filter(|n| !n.ends_with(STAGING_SUFFIX))
            .collect();
        let suspended: Vec<String> = self.suspended.keys().cloned().collect();
        persist_mounted_nests(
            &self.mount_ctx.dir,
            &names,
            &self.mount_ctx.mounts,
            &suspended,
        )
        .with_context(|| {
            format!(
                "the runtime's nest set changed but {MOUNTS_FILE} could not be updated - the \
                     change is live now but will not survive a restart"
            )
        })
    }

    /// Reclaim an unmounted dataset's disk (#1547). A mount record naming it keeps it, and a store
    /// this process still holds is refused.
    ///
    /// So does an unfinished mount job for it, which may have fetched it and not joined yet (#1674).
    /// A job claimed after this check plans its mount under the lock held here, so after the removal.
    pub fn reclaim(
        &self,
        nid: &Nid,
        jobs: &crate::mount_jobs::MountJobs,
    ) -> Result<crate::prune::Reclaim> {
        let mounting: Vec<String> = jobs
            .list()
            .into_iter()
            .filter(|j| !j.phase.finished())
            .filter(|j| {
                let recorded = || {
                    self.mount_ctx
                        .mounts
                        .iter()
                        .find(|m| mount_route(m, &self.default_tenant) == j.name)
                        .map(|m| m.nid.as_str())
                };
                j.nid.as_deref().or_else(recorded) == Some(nid.as_str())
            })
            .map(|j| {
                let (tenant, alias) = split_route_key(&j.name);
                format!("{}/{alias}", tenant.unwrap_or(&self.default_tenant))
            })
            .collect();
        // A join whose job was forgotten still has the store open until its catch-up notices.
        let data = MountTable::data_dir(&self.mount_ctx.dir, nid.as_str());
        let mounting: Vec<String> = mounting
            .into_iter()
            .chain(
                self.joining
                    .list()
                    .into_iter()
                    .filter(|j| j.dir == data)
                    .map(|j| {
                        let (tenant, alias) = split_route_key(&j.name);
                        format!("{}/{alias}", tenant.unwrap_or(&self.default_tenant))
                    }),
            )
            .collect();
        if !mounting.is_empty() {
            return Ok(crate::prune::Reclaim::Kept {
                nid: nid.as_str().to_string(),
                mounted_by: mounting,
            });
        }
        crate::prune::reclaim(&self.mount_ctx.dir, nid.as_str())
    }

    /// Unmount a nest: drain its cursor, release every handle to its store, then remove its routes.
    ///
    /// The ordering is the contract (RFC-0027 §6). The cursor is asked first and acknowledged before
    /// anything is torn down, because a route removed while the cursor is still committing a window
    /// would leave the nest writing data nobody can read - and, worse, would make "the store is
    /// closed" a race rather than a fact.
    ///
    /// Three holders of the nest's `Store` must drop before redb releases the file: the cursor's (via
    /// the ack), the alert delivery worker's (aborted here), and the serving state's (dropped when the
    /// router is re-composed without it). Miss any one and the file stays locked - which is exactly
    /// what the acceptance test checks, by reopening it.
    ///
    /// Idempotent: unmounting a nest that is not mounted is a no-op, not an error.
    pub async fn unmount(&mut self, name: &str) -> Result<()> {
        let Some(idx) = self.states.iter().position(|(n, _)| n == name) else {
            // A suspended mount is already off its cursor; unmounting it drops the record.
            if self.suspended.remove(name).is_some() {
                self.health.retire_nest(name);
                self.live.swap(self.compose()?);
                return self.persist();
            }
            tracing::debug!("nest '{name}' is not mounted; nothing to unmount");
            return Ok(());
        };
        let chain = self.states[idx].1.chain.clone();
        // The route key is `tenant/alias` once more than one tenant is mounted, and the record's
        // `alias` is only the second segment. The serving state already knows its dataset (#1531).
        let dataset_dir = self.states[idx].1.dir.clone();
        // The cursor and alert worker know a shared dataset by the mount that first indexed it, which
        // may be this one or another. While any other mount still holds the store, both stay running
        // (RFC-0032 §5): only this mount's routes go.
        let cursor_key = self.states[idx]
            .1
            .runtime_health
            .as_ref()
            .map_or_else(|| name.to_string(), |(n, _)| n.clone());
        let still_held = self
            .states
            .iter()
            .enumerate()
            .any(|(i, (_, s))| i != idx && Arc::ptr_eq(&s.store, &self.states[idx].1.store));
        if still_held {
            // The dataset is known on its cursor by this name. One of the mounts left on it takes the
            // key, as in a move, so its health and attribution follow a mount that exists (#1648).
            if name == cursor_key {
                let store = self.states[idx].1.store.clone();
                let sharers = self.dataset_sharers(name, &store);
                if let Some(holder) = sharers.first().cloned() {
                    let tx = self
                        .lifecycle
                        .get(&chain)
                        .cloned()
                        .ok_or_else(|| anyhow::anyhow!("the {chain} cursor is gone"))?;
                    swap_cursor_keys(&tx, &chain, name, None, Some(&holder)).await?;
                    self.finish_rehome(&chain, name, &holder, &sharers);
                    crate::metrics::METRICS.remove_nest(name);
                }
            }
            self.health.unregister(name);
            self.publishers.retain(|(n, _)| n != name);
            self.states.remove(idx);
            return self.recompose_after_unmount(name);
        }

        let released = self.states[idx].1.store.released();
        self.drain_cursor_nest(&chain, &cursor_key, name).await?;
        self.health.unregister(name);
        self.health.unregister(&cursor_key);
        self.publishers.retain(|(n, _)| n != name);

        // 3. Drop the serving state - the third - and re-compose without it. Requests already in
        //    flight finish against the old composition; new ones 404.
        self.states.remove(idx);
        crate::analytics::invalidate_session_cache(&dataset_dir);
        crate::metrics::METRICS.remove_nest(&cursor_key);
        crate::metrics::METRICS.remove_nest(name);
        self.recompose_after_unmount(name)?;
        // An in-flight request, or a delivery the aborted alert worker had handed to the blocking
        // pool, still holds the store; a remount of this dataset before it lets go fails (#1764).
        indexer::await_store_release(released, &dataset_dir).await
    }

    /// The mounts other than `name` on the dataset behind `store`, the first of which inherits its key.
    fn dataset_sharers(&self, name: &str, store: &Arc<dyn crate::store::HotStore>) -> Vec<String> {
        self.states
            .iter()
            .filter(|(n, s)| n != name && Arc::ptr_eq(&s.store, store))
            .map(|(n, _)| n.clone())
            .collect()
    }

    /// Record that a shared dataset's cursor key has passed from `name`, moving off it, to `holder`.
    fn finish_rehome(&mut self, chain: &str, name: &str, holder: &str, sharers: &[String]) {
        let holder = holder.to_string();
        let health = self.health.clone();
        for (n, s) in self.states.iter_mut().filter(|(n, _)| sharers.contains(n)) {
            s.runtime_health = Some((holder.clone(), health.clone()));
            if *n != holder {
                health.register_alias(n, &holder, chain);
            }
        }
        health.make_canonical(&holder, chain);
        if let Some(w) = self.alert_workers.iter_mut().find(|(n, _)| n == name) {
            w.0 = holder.clone();
        }
        tracing::info!("the dataset '{name}' moves off is now known as '{holder}'");
    }

    /// The name a move stages its incoming nest under while it catches up.
    pub fn staging_name(name: &str) -> String {
        format!("{name}{STAGING_SUFFIX}")
    }

    /// The record whose surface `name` serves. A move's staging name serves the moving name's, so
    /// the incoming nest is never routed more open than the name it will take (#2129).
    fn surface_record(&self, name: &str) -> Option<Mount> {
        let owner = name.strip_suffix(STAGING_SUFFIX).unwrap_or(name);
        self.mount_ctx
            .mounts
            .iter()
            .find(|m| mount_route(m, &self.default_tenant) == owner)
            .cloned()
    }

    /// Undo a move's staging mount and return `cause`, the reason the move stopped. An unmount keeps a
    /// name's record for a remount and nothing remounts a staging name, so the record goes too, but
    /// only once the unmount has landed: one whose drain failed leaves the nest mounted.
    async fn abandon_move(&mut self, staging: &str, cause: anyhow::Error) -> anyhow::Error {
        match self.unmount(staging).await {
            Ok(()) => {
                self.mount_ctx
                    .mounts
                    .retain(|m| mount_route(m, &self.default_tenant) != staging);
                cause
            }
            Err(e) => anyhow::anyhow!(
                "{cause:#}; backing out '{staging}' failed too, so it is still mounted: {e:#}"
            ),
        }
    }

    /// Move a live name to another NID without a gap (#1549).
    ///
    /// The new NID is mounted under [`Self::staging_name`], which catches it up before it joins its
    /// cursor. The old nest is then drained off the cursor, the new one takes its name there, and the
    /// name's routes switch in one swap: a reader sees the old nest until then, and the new after.
    pub async fn move_name(&mut self, name: &str, nid: Nid) -> Result<()> {
        let Some(staging) = self.move_staging(name, &nid)? else {
            return Ok(());
        };
        self.mount(&staging, Some(nid.clone())).await?;
        self.complete_move(name, &staging, nid).await
    }

    /// The name a move of `name` to `nid` stages its new nest under, or `None` when it is already there.
    fn move_staging(&self, name: &str, nid: &Nid) -> Result<Option<String>> {
        let Some(old) = self.states.iter().find(|(n, _)| n == name).map(|(_, s)| s) else {
            bail!("'{name}' is not mounted");
        };
        if old.nid.as_deref() == Some(nid.as_str()) {
            return Ok(None);
        }
        Ok(Some(Self::staging_name(name)))
    }

    /// The rest of a move once its staged nest has joined the cursor.
    async fn complete_move(&mut self, name: &str, staging: &str, nid: Nid) -> Result<()> {
        let staging = staging.to_string();
        let Some(old) = self.states.iter().find(|(n, _)| n == name).map(|(_, s)| s) else {
            let cause = anyhow::anyhow!("'{name}' was unmounted while its move caught up");
            return Err(self.abandon_move(&staging, cause).await);
        };
        let chain = old.chain.clone();
        let old_key = old
            .runtime_health
            .as_ref()
            .map_or_else(|| name.to_string(), |(n, _)| n.clone());
        let old_store = old.store.clone();
        let Some(new_idx) = self.states.iter().position(|(n, _)| *n == staging) else {
            bail!("the staged nest of '{name}' was unmounted before it took the name");
        };
        if self.states[new_idx].1.chain != chain {
            let cause = anyhow::anyhow!("a move keeps its chain: '{name}' is on {chain}");
            return Err(self.abandon_move(&staging, cause).await);
        }
        let record = self.surface_record(name);
        let surface = match served_surface(record.as_ref(), &self.states[new_idx].1.dir, name) {
            Ok(surface) => surface,
            Err(e) => return Err(self.abandon_move(&staging, e).await),
        };
        let new_key = self.states[new_idx]
            .1
            .runtime_health
            .as_ref()
            .map_or_else(|| staging.clone(), |(n, _)| n.clone());

        // Readers keep the old nest's routes while it comes off the cursor.
        let old_shared = self
            .states
            .iter()
            .filter(|(_, s)| Arc::ptr_eq(&s.store, &old_store))
            .count()
            > 1;
        if !old_shared {
            // The old nest keeps its name and routes, so the staged one must not stay beside it.
            if let Err(e) = self.drain_cursor_nest(&chain, &old_key, name).await {
                return Err(self.abandon_move(&staging, e).await);
            }
            crate::metrics::METRICS.remove_nest(&old_key);
        }
        // A shared dataset known on the cursor by the name moving off it keeps indexing for the
        // mounts left on it: one of them takes the key, or the incoming nest cannot.
        let sharers = if old_shared && old_key == name {
            self.dataset_sharers(name, &old_store)
        } else {
            Vec::new()
        };
        let incoming = (new_key == staging).then_some(staging.as_str());
        if !sharers.is_empty() || incoming.is_some() {
            let swapped = match self.lifecycle.get(&chain).cloned() {
                Some(tx) => {
                    swap_cursor_keys(
                        &tx,
                        &chain,
                        name,
                        incoming,
                        sharers.first().map(String::as_str),
                    )
                    .await
                }
                None => Err(anyhow::anyhow!("the {chain} cursor is gone")),
            };
            if let Err(e) = swapped {
                return Err(self.abandon_move(&staging, e).await);
            }
        }
        if let Some(holder) = sharers.first() {
            self.finish_rehome(&chain, name, holder, &sharers);
        }
        let new_idx = self
            .states
            .iter()
            .position(|(n, _)| *n == staging)
            .expect("still staged");
        let (_, mut new_state) = self.states.remove(new_idx);
        if new_key == staging {
            new_state.runtime_health = Some((name.to_string(), self.health.clone()));
            self.health.register(name, &chain);
            self.health.mark_indexing(name);
            if let Some(w) = self.alert_workers.iter_mut().find(|(n, _)| *n == staging) {
                w.0 = name.to_string();
            }
        } else {
            self.health.register_alias(name, &new_key, &chain);
        }
        overlay_mount_record(&mut new_state, Some(nid.as_str()), record.as_ref(), surface);
        let old_idx = self
            .states
            .iter()
            .position(|(n, _)| n == name)
            .expect("still mounted");
        let (_, old_state) =
            std::mem::replace(&mut self.states[old_idx], (name.to_string(), new_state));
        if let Some(mb) = self.estimates.remove(&staging) {
            self.estimates.insert(name.to_string(), mb);
        }
        self.publishers.retain(|(n, _)| n != name);
        for p in self.publishers.iter_mut().filter(|(n, _)| *n == staging) {
            p.0 = name.to_string();
        }
        self.mount_ctx
            .mounts
            .retain(|m| mount_route(m, &self.default_tenant) != staging);
        for m in self
            .mount_ctx
            .mounts
            .iter_mut()
            .filter(|m| mount_route(m, &self.default_tenant) == name)
        {
            m.nid = nid.as_str().to_string();
        }
        let released = (!old_shared).then(|| {
            crate::analytics::invalidate_session_cache(&old_state.dir);
            (old_state.store.released(), old_state.dir.clone())
        });
        drop((old_state, old_store));
        self.recompose()?;
        tracing::info!("'{name}' moved to nid {nid}");
        match released {
            // A move back to the old NID reopens this store (#1764).
            Some((released, dir)) => indexer::await_store_release(released, &dir).await,
            None => Ok(()),
        }
    }

    /// Take a nest off its cursor and stop its alert worker, waiting for both to let go of its store
    /// (RFC-0027 §6). `cursor_key` is the name the cursor knows it by; `name` is for the errors.
    async fn drain_cursor_nest(&mut self, chain: &str, cursor_key: &str, name: &str) -> Result<()> {
        // 1. Drain the cursor and wait for it to let go.
        //
        // No channel for this nest's chain means we cannot ask the cursor to stop, and tearing the
        // routes down regardless would leave it writing to a store nobody can read - the exact failure
        // §6 orders this sequence to prevent. So this is an error, not a skip. (An early draft skipped
        // silently; the acceptance test then failed on a *held* store, which is how the gap surfaced.)
        {
            let tx = self.lifecycle.get(chain).ok_or_else(|| {
                anyhow::anyhow!(
                    "no cursor channel for chain '{chain}' hosting '{name}' - refusing to unmount \
                     without draining it first"
                )
            })?;
            let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
            if tx
                .send(indexer::CursorCommand::Unmount {
                    name: cursor_key.to_string(),
                    ack: Some(ack_tx),
                })
                .is_ok()
            {
                match tokio::time::timeout(UNMOUNT_ACK_TIMEOUT, ack_rx).await {
                    Ok(Ok(())) => {}
                    // A closed channel means the cursor is already gone, which is as released as it
                    // gets. A timeout is not: report it rather than tearing down regardless.
                    Ok(Err(_)) => tracing::debug!("cursor on {chain} already stopped"),
                    Err(_) => bail!(
                        "cursor on {chain} did not acknowledge unmounting '{name}' within {}s - \
                         refusing to remove its routes while it may still be writing",
                        UNMOUNT_ACK_TIMEOUT.as_secs()
                    ),
                }
            }
        }

        // 2. Stop and drop the nest's alert worker - the second holder of its store.
        if let Some(pos) = self
            .alert_workers
            .iter()
            .position(|(n, _)| *n == cursor_key)
        {
            let (_, worker) = self.alert_workers.remove(pos);
            worker.abort();
            // `abort()` only *requests* cancellation - the task keeps its `Store` clone until the
            // runtime actually drops it. Awaiting the handle waits for that to have happened. Skipping
            // this makes the release a race: the acceptance test caught it on the first run, failing
            // with "Database already open" a few microseconds after the abort.
            let _ = worker.await;
        }
        Ok(())
    }

    fn recompose_after_unmount(&mut self, name: &str) -> Result<()> {
        self.recompose()?;
        tracing::info!("nest '{name}' unmounted from the runtime");
        Ok(())
    }

    fn recompose(&mut self) -> Result<()> {
        // Rebuilt from `states`, same as `mount` - so the departed nest, and any dataset co-tenant's
        // `shared_with` entry naming it, both drop out of the roster in the same step its routes do.
        let datasets = live_datasets(
            &self.mount_ctx.dir,
            &self.states,
            &self.mount_ctx.mounts,
            &self.default_tenant,
        );
        self.roster["nests"] = serde_json::json!(build_roster_entries(
            &self.states,
            &datasets,
            &self.default_tenant,
            &self.estimates,
        ));
        self.live.swap(self.compose()?);
        self.persist()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CONFIG_FILE;

    fn mounts_with(publish: &str) -> MountTable {
        toml::from_str(&format!(
            "[runtime]\nname = \"t\"\nchain = \"ethereum\"\nchain_id = 1\n\n\
             [[mounts]]\nalias = \"usdc\"\nnid = \"{}\"\n{publish}",
            "a".repeat(64)
        ))
        .unwrap()
    }

    #[test]
    fn a_mount_publish_table_loads_with_defaults_and_a_bad_one_is_refused() {
        let plain = mounts_with("");
        assert!(plain.mounts[0].publish.is_none());
        assert!(!toml::to_string(&plain).unwrap().contains("publish"));

        let table = mounts_with("\n[mounts.publish]\ntarget = \"s3://bucket/prefix\"\n");
        table.validate_mounts().unwrap();
        let settings = table.mounts[0]
            .publish
            .as_ref()
            .unwrap()
            .settings()
            .unwrap();
        assert_eq!(settings.target, "s3://bucket/prefix");
        assert_eq!(settings.interval, std::time::Duration::from_secs(60));
        assert_eq!(settings.parallelism, crate::publish::DEFAULT_PARALLELISM);

        for bad in [
            "target = \"\"",
            "target = \"/m\"\ninterval = \"soon\"",
            "target = \"/m\"\ninterval = \"0s\"",
            "target = \"/m\"\nparallelism = 0",
            "target = \"/m\"\nparallelism = 17",
        ] {
            let table = mounts_with(&format!("\n[mounts.publish]\n{bad}\n"));
            let err = table.validate_mounts().unwrap_err();
            assert!(
                format!("{err:#}").contains("[mounts.publish]"),
                "{bad}: {err:#}"
            );
        }
    }

    #[tokio::test]
    async fn spawn_publishers_mirrors_each_mount_that_asks_and_skips_the_rest() {
        let root = tempfile::tempdir().unwrap();
        let nid = "b".repeat(64);
        let dataset = MountTable::data_dir(root.path(), &nid);
        std::fs::create_dir_all(dataset.join("abis")).unwrap();
        std::fs::write(
            dataset.join(CONFIG_FILE),
            "[nest]\nname = \"mirrored\"\nchain = \"ethereum\"\nchain_id = 1\n\
             rpc_urls = [\"http://127.0.0.1:1\"]\nschema_version = 1\n\n\
             [[contracts]]\nalias = \"usdc\"\n\
             address = \"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48\"\nabi = \"abis/usdc.json\"\n",
        )
        .unwrap();
        std::fs::write(
            dataset.join("abis/usdc.json"),
            r#"[{"type":"event","name":"Transfer","anonymous":false,"inputs":[{"name":"from","type":"address","indexed":true},{"name":"to","type":"address","indexed":true},{"name":"value","type":"uint256","indexed":false}]}]"#,
        )
        .unwrap();
        std::fs::write(dataset.join("schema.json"), r#"{"tables":[]}"#).unwrap();
        crate::seal::test_set_table_floor(&dataset, 0);
        let row = crate::registry::DecodedRow {
            table: "usdc__transfer".into(),
            params: vec![(
                "value".into(),
                crate::registry::Value::Word32(
                    alloy_primitives::U256::from(5u64).to_be_bytes::<32>(),
                ),
            )],
            block_number: 10,
            block_hash: "0xbh".into(),
            block_timestamp: 1_700_000_010,
            timestamps: true,
            log_index: 0,
            tx_hash: "0xtx".into(),
            address: "0xaa".into(),
        }
        .to_json()
        .to_string();
        crate::seal::seal_range(&dataset, &[row], 10, 10)
            .unwrap()
            .expect("sealed");

        let mirror = tempfile::tempdir().unwrap();
        let mut table = mounts_with("");
        table.mounts[0].nid = nid.clone();
        table.mounts[0].publish = Some(MountPublish {
            target: mirror.path().to_str().unwrap().to_string(),
            interval: "1h".into(),
            parallelism: 2,
        });
        let mut quiet = table.mounts[0].clone();
        quiet.alias = "quiet".into();
        quiet.publish = None;
        // First, so `quiet` is the route the dataset's cursor records under (#1415).
        table.mounts.insert(0, quiet);

        let publishers = spawn_publishers(root.path(), &table.mounts, "default").unwrap();
        let keys: Vec<&str> = publishers.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, ["usdc"]);

        let local = std::fs::read(
            dataset
                .join(crate::seal::SEGMENTS_DIR)
                .join("manifest.json"),
        )
        .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        loop {
            let published = std::fs::read_dir(mirror.path()).ok().and_then(|mut dirs| {
                dirs.next()
                    .and_then(|d| std::fs::read(d.ok()?.path().join("manifest.json")).ok())
            });
            if published.as_deref() == Some(local.as_slice()) {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the mount's dataset was never mirrored"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert_eq!(
            crate::metrics::METRICS
                .nest_if_known("quiet")
                .and_then(|m| m.publish_target())
                .as_deref(),
            mirror.path().to_str(),
            "publish lag is measured where the cursor records sealed_through, under the dataset's first \
             route rather than the nest's name (#1415)"
        );
    }

    /// Write a minimal mounts.toml + one nest dir on the given chain.
    fn write_roost(dir: &Path, chain: &str, chain_id: u64, nest_chain: &str, nest_chain_id: u64) {
        std::fs::write(
            dir.join(MOUNTS_FILE),
            format!(
                "[runtime]\nname = \"test\"\nnests = [\"a\"]\n\n\
                 [[chains]]\nchain = \"{chain}\"\nchain_id = {chain_id}\n\
                 rpc_urls = [\"http://localhost:8545\"]\n"
            ),
        )
        .unwrap();
        let nest = MountTable::nest_dir(dir, "a");
        std::fs::create_dir_all(&nest).unwrap();
        std::fs::write(
            nest.join(CONFIG_FILE),
            format!(
                "[nest]\nname = \"a\"\nchain = \"{nest_chain}\"\nchain_id = {nest_chain_id}\n\
                 rpc_urls = []\n\n[[contracts]]\nalias = \"t\"\naddress = \"0x0000000000000000000000000000000000000001\"\nabi = \"abi.json\"\n"
            ),
        )
        .unwrap();
        // A trivially-valid ABI so Config::load's downstream users don't choke (load itself doesn't read it).
        std::fs::write(nest.join("abi.json"), "[]").unwrap();
    }

    /// Write a nest dir on a given chain under a runtime (for multichain grouping tests).
    fn write_nest_dir(roost_dir: &Path, name: &str, chain: &str, chain_id: u64) {
        let nest = MountTable::nest_dir(roost_dir, name);
        std::fs::create_dir_all(&nest).unwrap();
        std::fs::write(
            nest.join(CONFIG_FILE),
            format!(
                "[nest]\nname = \"{name}\"\nchain = \"{chain}\"\nchain_id = {chain_id}\nrpc_urls = []\n\n\
                 [[contracts]]\nalias = \"t\"\naddress = \"0x0000000000000000000000000000000000000001\"\nabi = \"abi.json\"\n"
            ),
        )
        .unwrap();
        std::fs::write(nest.join("abi.json"), "[]").unwrap();
    }

    /// #1399: boot and a hot mount both stamp `--ipfs-window-deadline` through this, beside the
    /// freshness dial, onto a config that loaded with the default.
    #[test]
    fn operator_settings_reach_a_mounted_config() {
        let d = tempfile::tempdir().unwrap();
        write_nest_dir(d.path(), "n", "arbitrum-one", 42161);
        let mut config = Config::load(&MountTable::nest_dir(d.path(), "n")).unwrap();
        assert_eq!(
            config.ipfs_window_deadline,
            crate::ipfs_resolve::WINDOW_DEADLINE
        );
        let minute = std::time::Duration::from_secs(60);
        let freshness = crate::freshness::Freshness {
            poll_interval: minute,
            finality_only: true,
            poll_interval_explicit: true,
        };
        stamp_operator_settings(&mut config, freshness, std::time::Duration::ZERO);
        assert_eq!(config.ipfs_window_deadline, std::time::Duration::ZERO);
        assert_eq!(config.freshness.poll_interval, minute);
    }

    fn mounted(roost_dir: &Path, name: &str) -> (String, PathBuf, Config) {
        let mounts = MountTable::load(roost_dir).unwrap();
        let p = mounts.dir_for(roost_dir, name);
        let c = Config::load(&p).unwrap();
        (name.to_string(), p, c)
    }

    fn aliases(ds: &Dataset) -> Vec<String> {
        ds.mounts.iter().map(|m| m.alias.clone()).collect()
    }

    /// Build a real, hashable nest under `data/<nid>` and a `mounts.toml` claiming that identity.
    /// Returns the NID it genuinely hashes to.
    fn migrated_nest(root: &Path, alias: &str) -> String {
        let staging = root.join("staging");
        std::fs::create_dir_all(&staging).unwrap();
        std::fs::write(
            staging.join(CONFIG_FILE),
            format!(
                "[nest]\nname = \"{alias}\"\nchain = \"arbitrum-one\"\nchain_id = 42161\n\
                 rpc_urls = []\n\n[[contracts]]\nalias = \"t\"\n\
                 address = \"0x0000000000000000000000000000000000000001\"\nabi = \"abi.json\"\n"
            ),
        )
        .unwrap();
        std::fs::write(staging.join("abi.json"), "[]").unwrap();

        let nid = crate::blob::nest_nid(&staging).expect("a minimal nest must be hashable");
        let dest = MountTable::data_dir(root, &nid);
        std::fs::create_dir_all(dest.parent().unwrap()).unwrap();
        std::fs::rename(&staging, &dest).unwrap();

        std::fs::write(
            root.join(MOUNTS_FILE),
            format!(
                "[runtime]\nname = \"r\"\n\n\
                 [[chains]]\nchain = \"arbitrum-one\"\nchain_id = 42161\nrpc_urls = []\n\n\
                 [[mounts]]\ntenant = \"default\"\nalias = \"{alias}\"\nnid = \"{nid}\"\n"
            ),
        )
        .unwrap();
        nid
    }

    /// The guard that stops `dev` rewriting a dataset's derived artifacts, which used to move the NID
    /// its own mount record claims - so a runtime started twice, with no operator action between the
    /// starts, reported drift on the second. See the call site in `indexer.rs`.
    ///
    /// Both directions matter: an authored nest **must** still be refreshable, or a hand-written
    /// `nuthatch.toml` silently never gets a schema (issue #241 item 2).
    #[test]
    fn only_identity_keyed_dataset_dirs_are_left_alone() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        let nid = "aa11".repeat(16);

        assert!(
            MountTable::is_identity_keyed(&MountTable::data_dir(root, &nid)),
            "data/<64-hex> is a dataset and must not be rewritten"
        );
        assert!(
            !MountTable::is_identity_keyed(&MountTable::nest_dir(root, "usdc")),
            "an authored nest under nests/ must still be refreshable"
        );
        assert!(
            !MountTable::is_identity_keyed(root),
            "a bare project directory (`nuthatch dev` on one nest) must still be refreshable"
        );
        // Shape checks: the right length in the wrong place, and the right place with a non-NID name.
        assert!(!MountTable::is_identity_keyed(
            &root.join("elsewhere").join(&nid)
        ));
        assert!(!MountTable::is_identity_keyed(
            &root.join(DATA_DIR).join("usdc")
        ));
        assert!(!MountTable::is_identity_keyed(
            &root.join(DATA_DIR).join("zz11".repeat(16))
        ));
    }

    /// Stage a second dataset from `nid`'s inputs plus a cosmetic edit, and return the identity it
    /// hashes to. The two datasets then share a data identity and differ in NID - the shape early
    /// cutoff exists for.
    fn cosmetic_sibling(root: &Path, nid: &str) -> String {
        let staging = root.join("staging");
        crate::project::copy_dir(&MountTable::data_dir(root, nid), &staging).unwrap();
        // Inputs only - a freshly installed nest package carries no store and no segments.
        for name in crate::blob::DERIVED_STATE {
            let p = staging.join(name);
            let _ = if p.is_dir() {
                std::fs::remove_dir_all(&p)
            } else {
                std::fs::remove_file(&p)
            };
        }
        // `views/**` is excluded from the data identity, so this moves the NID and nothing else.
        std::fs::create_dir_all(staging.join("views")).unwrap();
        std::fs::write(staging.join("views").join("10-note.sql"), "-- a note\n").unwrap();

        let new_nid = crate::blob::nest_nid(&staging).unwrap();
        std::fs::rename(&staging, MountTable::data_dir(root, &new_nid)).unwrap();
        new_nid
    }

    /// Early cutoff on the mount path (#364): a dataset with no data of its own takes the data of one
    /// whose inputs imply the same bytes.
    #[test]
    fn a_dataset_with_no_data_adopts_a_data_identical_one() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        let old = migrated_nest(root, "usdc");
        seed_history(&MountTable::data_dir(root, &old), 4242);

        let new = cosmetic_sibling(root, &old);
        let dest = MountTable::data_dir(root, &new);
        let adopted = adopt_dataset(root, &dest, &new).unwrap();

        assert_eq!(
            adopted,
            Some(Adoption {
                nid: new.clone(),
                from_nid: old.clone()
            }),
            "a cosmetic edit must adopt the dataset it came from"
        );
        assert_eq!(
            history_head(&dest),
            Some(4242),
            "the data must actually arrive - reporting an adoption that copied nothing is worse than \
             not adopting"
        );
        assert_eq!(
            history_head(&MountTable::data_dir(root, &old)),
            Some(4242),
            "adoption must COPY - the source may still be mounted by another tenant"
        );
        // And it must not have moved the identity it adopted into, or the next start reports drift.
        assert_eq!(
            crate::blob::nest_nid(&dest).unwrap(),
            new,
            "copying derived state must not change what the destination's inputs hash to"
        );
    }

    /// The guard that makes this safe to run on **every** start: a dataset that already holds data is
    /// left exactly as it is. Without it, early cutoff would overwrite a live nest's store with
    /// another dataset's the moment two identities shared a data identity - which is the ordinary case
    /// after an edit, not a rare one.
    #[test]
    fn adoption_never_writes_over_a_dataset_that_holds_data() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        let old = migrated_nest(root, "usdc");
        seed_history(&MountTable::data_dir(root, &old), 4242);

        let new = cosmetic_sibling(root, &old);
        let dest = MountTable::data_dir(root, &new);
        // This one has indexed on its own - a restart, or a partial backfill that was interrupted.
        // A different head from the source's, so "untouched" is checkable rather than assumed.
        seed_history(&dest, 17);

        assert_eq!(
            adopt_dataset(root, &dest, &new).unwrap(),
            None,
            "a dataset holding data must not adopt"
        );
        assert_eq!(history_head(&dest), Some(17), "its store must be untouched");
    }

    /// The dangerous direction, at the point the decision is made: different data identity, no
    /// adoption. `e2e_early_cutoff` drives the same rule through the whole mount path.
    #[test]
    fn a_dataset_whose_data_identity_differs_is_not_adoptable() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        let old = migrated_nest(root, "usdc");
        seed_history(&MountTable::data_dir(root, &old), 4242);

        // A different contract: same shape, different bytes stored.
        let staging = root.join("staging");
        crate::project::copy_dir(&MountTable::data_dir(root, &old), &staging).unwrap();
        std::fs::remove_file(staging.join(crate::config::DB_FILE)).unwrap();
        let cfg = std::fs::read_to_string(staging.join(CONFIG_FILE)).unwrap();
        std::fs::write(
            staging.join(CONFIG_FILE),
            cfg.replace(
                "0x0000000000000000000000000000000000000001",
                "0x0000000000000000000000000000000000000002",
            ),
        )
        .unwrap();
        let new = crate::blob::nest_nid(&staging).unwrap();
        let dest = MountTable::data_dir(root, &new);
        std::fs::rename(&staging, &dest).unwrap();

        assert_eq!(
            adopt_dataset(root, &dest, &new).unwrap(),
            None,
            "a nest indexing a different contract must not inherit another contract's data"
        );
        assert!(
            !dest.join(crate::config::DB_FILE).exists(),
            "and nothing must have been copied into it"
        );
    }

    /// #1369's acceptance: a candidate whose **files recompute** to the wanted identity but whose
    /// **store recorded** a different one must not be adopted.
    ///
    /// That divergence is what a decode change does. `#1364` moved a named tuple's object shape, so
    /// history indexed by the old binary recomputes under the new one to the new registry hash and
    /// passes every check `adoptable` used to make - while the hash inside its store still names the
    /// binary that actually wrote the rows. `guard_registry_identity` catches it at start, so nothing
    /// mixed is ever served, but by then the single adoption is spent and the mount holds a store it
    /// can neither start from nor replace. The operator has to empty it by hand.
    ///
    /// The fixture makes the divergence directly rather than by shipping two binaries: an otherwise
    /// perfectly adoptable dataset that recorded somebody else's hash.
    #[test]
    fn a_candidate_recording_a_different_registry_hash_is_not_adoptable() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        let old = migrated_nest(root, "usdc");
        let src = MountTable::data_dir(root, &old);

        // Everything about this dataset is adoptable except the one fact the old check never read.
        let real = crate::blob::build_manifest(&src, None)
            .unwrap()
            .registry_hash;
        let stale = format!("{:0>64}", "dead");
        assert_ne!(real, stale, "premise: the recorded hash must differ");
        seed_history_recording(&src, 4242, &stale);

        let new = cosmetic_sibling(root, &old);
        let dest = MountTable::data_dir(root, &new);

        // The premise the whole issue rests on: under the old rule this candidate passed.
        let m = crate::blob::build_manifest(&dest, None).unwrap();
        let recomputed = crate::blob::build_manifest(&src, None).unwrap();
        assert_eq!(
            (recomputed.registry_hash.clone(), recomputed.data_identity()),
            (m.registry_hash.clone(), m.data_identity()),
            "premise: the candidate's files must recompute to the wanted identity, or this test \
             proves nothing about the recorded hash"
        );

        assert_eq!(
            adopt_dataset(root, &dest, &new).unwrap(),
            None,
            "a dataset whose store records a different registry hash must not be adopted - the \
             adoption is spent and the mount can never start"
        );
        assert!(
            !dest.join(crate::config::DB_FILE).exists(),
            "and nothing must have been copied into it, so it indexes from scratch"
        );
    }

    /// The other half of #1369's fix: a candidate that records **no** registry hash is skipped too.
    ///
    /// It is the same answer `Adopting::From` gives an unreadable store, for the same reason. A
    /// store predating the identity check (#653) has rows and no recorded hash, so nothing can say
    /// which binary wrote them; `guard_registry_identity` will adopt a hash for it on start and warn
    /// that it was recorded rather than verified. Copying such a store into a fresh mount would
    /// launder that unverified claim into a new identity and spend the adoption on it. A re-index
    /// costs time; this costs an operator a directory they must empty by hand.
    #[test]
    fn a_candidate_recording_no_registry_hash_is_not_adoptable() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        let old = migrated_nest(root, "usdc");
        let src = MountTable::data_dir(root, &old);

        // Deliberately the pre-#653 shape: rows, no recorded identity.
        {
            let s = crate::store::Store::open(&src.join(crate::config::DB_FILE)).unwrap();
            s.set_meta("last_block", "4242").unwrap();
        }
        assert_eq!(
            crate::store::recorded_registry_hash(&src.join(crate::config::DB_FILE)).unwrap(),
            None,
            "premise: this fixture must record no hash at all"
        );
        assert!(
            holds_data(&src, Adopting::From),
            "premise: it must otherwise be a live candidate, or the skip proves nothing"
        );

        let new = cosmetic_sibling(root, &old);
        let dest = MountTable::data_dir(root, &new);
        assert_eq!(
            adopt_dataset(root, &dest, &new).unwrap(),
            None,
            "a dataset that records no registry hash must not be adopted"
        );
        assert!(!dest.join(crate::config::DB_FILE).exists());
    }

    /// A store indexed under the full decode identity records that identity, which for a nest
    /// declaring `[[ipfs]]` or `[[calls]]` is not the event registry's hash the manifest carries.
    /// Comparing it with the manifest's hash alone refused every such dataset its adoption.
    #[test]
    fn a_candidate_recorded_under_the_full_decode_identity_is_adoptable() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        let old = migrated_nest(root, "usdc");
        let src = MountTable::data_dir(root, &old);
        let mut cfg = std::fs::read_to_string(src.join(CONFIG_FILE)).unwrap();
        cfg.push_str("\n[[ipfs]]\nname = \"doc\"\non = \"t__uri_set\"\ncid_column = \"uri\"\n");
        std::fs::write(src.join(CONFIG_FILE), cfg).unwrap();

        let events = crate::blob::build_manifest(&src, None)
            .unwrap()
            .registry_hash;
        let full = decode_identity_of(&src).expect("the inputs are readable");
        assert_ne!(
            events, full,
            "premise: an [[ipfs]] declaration must move the decode identity off the event hash"
        );
        seed_history_recording(&src, 4242, &full);
        crate::store::Store::open(&src.join(crate::config::DB_FILE))
            .unwrap()
            .set_meta(
                crate::store::IDENTITY_FORMULA_KEY,
                crate::store::IDENTITY_FORMULA,
            )
            .unwrap();

        let new = cosmetic_sibling(root, &old);
        let dest = MountTable::data_dir(root, &new);
        assert!(
            adopt_dataset(root, &dest, &new).unwrap().is_some(),
            "a dataset recording the identity its inputs imply must be adopted"
        );
    }

    /// Adoption is all-or-nothing, and a half-copied one is the failure this guards.
    ///
    /// `DERIVED_STATE` is `[nuthatch.redb, segments]`. Copying them into the destination in order
    /// means a fault while `segments/` is in flight leaves a **complete** store carrying
    /// `sealed_through = N` beside a **partial** `segments/`. Rows up to `N` were pruned from hot
    /// when they sealed, so they now exist only in the segments that never arrived; indexing resumes
    /// from the copied `last_block` and never re-fetches them; and `holds_data` is true, so the
    /// dataset can neither adopt again nor start over. `/sql` answers with a permanent silent hole.
    ///
    /// The fault injected here is a dangling symlink inside the source's `segments/`, which is a
    /// deterministic stand-in for the disk-full this exists for: `copy_dir` reaches it after the
    /// store has already been copied, and fails. What must survive it is the destination being back
    /// to inputs-only - and *actually* adoptable, which the second half asserts by clearing the
    /// fault and adopting for real.
    #[cfg(unix)]
    #[test]
    fn a_failed_adoption_leaves_the_dataset_inputs_only_and_still_adoptable() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        let old = migrated_nest(root, "usdc");
        let src = MountTable::data_dir(root, &old);
        seed_history(&src, 4242);
        std::fs::create_dir_all(src.join("segments")).unwrap();
        std::fs::write(src.join("segments").join("0000.parquet"), "sealed rows").unwrap();

        let new = cosmetic_sibling(root, &old);
        let dest = MountTable::data_dir(root, &new);

        // Injected after the sibling is staged, so only the adoption trips over it. Copied after the
        // store, and unreadable: `fs::copy` on a dangling symlink fails with ENOENT.
        std::os::unix::fs::symlink("nowhere", src.join("segments").join("9999.parquet")).unwrap();

        let err = adopt_dataset(root, &dest, &new).unwrap_err();
        assert!(
            format!("{err:#}").contains("adopting"),
            "the failure must name the adoption, got: {err:#}"
        );
        assert!(
            !holds_data(&dest, Adopting::Into),
            "a failed adoption must leave the dataset inputs-only, not half a dataset it can never \
             repair - found {:?}",
            crate::blob::DERIVED_STATE
                .iter()
                .filter(|n| dest.join(n).exists())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            crate::blob::nest_nid(&dest).unwrap(),
            new,
            "and it must not have moved the identity on the way out"
        );

        // "The re-index it replaces is still available once the fault is fixed" - as a claim that is
        // checked, not one the doc comment merely makes.
        std::fs::remove_file(src.join("segments").join("9999.parquet")).unwrap();
        assert_eq!(
            adopt_dataset(root, &dest, &new).unwrap(),
            Some(Adoption {
                nid: new.clone(),
                from_nid: old.clone()
            }),
            "the next start must adopt, or the failure was permanent after all"
        );
        assert_eq!(history_head(&dest), Some(4242));
        assert_eq!(
            std::fs::read_to_string(dest.join("segments").join("0000.parquet")).unwrap(),
            "sealed rows",
            "and the whole set must arrive, not just the store"
        );
        assert!(
            !adopt_staging(&dest).exists(),
            "staging is scratch and must not outlive the adoption"
        );
    }

    /// An empty dataset is not worth adopting, and adopting it would consume the one chance to adopt
    /// the dataset that does hold the history.
    #[test]
    fn an_empty_dataset_is_not_an_adoption_candidate() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        let old = migrated_nest(root, "usdc"); // no store written: inputs only
        let new = cosmetic_sibling(root, &old);
        let dest = MountTable::data_dir(root, &new);

        assert_eq!(
            adopt_dataset(root, &dest, &new).unwrap(),
            None,
            "adopting an empty dataset reports a cutoff and still re-indexes"
        );
    }

    /// Issue #408, the candidate side. The case above covers a dataset with **no** store file; this
    /// one covers a dataset with a store file that holds nothing, which is what a runtime started and
    /// stopped before its first block leaves behind. A presence check cannot tell the two apart, and
    /// the empty one is the case that actually occurs in the field.
    #[test]
    fn a_dataset_whose_store_was_created_but_never_written_is_not_an_adoption_candidate() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        let old = migrated_nest(root, "usdc");
        // A real redb, opened the way a starting cursor opens it, and closed before it indexed
        // anything. Scoped so the handle is dropped: redb holds a file lock for as long as it lives.
        {
            crate::store::Store::open(
                &MountTable::data_dir(root, &old).join(crate::config::DB_FILE),
            )
            .expect("an empty store must be creatable");
        }

        let new = cosmetic_sibling(root, &old);
        let dest = MountTable::data_dir(root, &new);

        assert_eq!(
            adopt_dataset(root, &dest, &new).unwrap(),
            None,
            "a store that was created and never written holds no history, so adopting it burns the \
             one chance to adopt on nothing"
        );
    }

    /// Issue #415, and the one input the two directions must answer differently.
    ///
    /// The candidate side is the half that was wrong: an unreadable store folded into "holds data",
    /// so `adoptable` picked it, `stage_derived_state` copied it, and the destination came out of the
    /// adoption holding a store nothing can read - which then reads as data forever, so it can never
    /// adopt again and must be emptied by hand. A permanent stuck state reached from a transient
    /// fault, and unlike the destination's mistake it is not repaired by a re-index.
    ///
    /// The segments arm is not decoration. `holds_data` used to answer from `segments/` before it
    /// looked at the store at all, so a candidate with sealed segments beside an unreadable store was
    /// adoptable however the error was folded - and the copy lands the unreadable store on the
    /// destination just the same.
    #[test]
    fn an_unreadable_store_answers_the_two_adoption_directions_differently() {
        let d = tempfile::tempdir().unwrap();
        let dir = d.path().join("dataset");
        std::fs::create_dir_all(&dir).unwrap();
        // A file with a store's name that redb cannot open. This is what a truncated write, a
        // half-restored backup or a bad disk leaves, and it is what ten fixtures used to stand in
        // for real history.
        std::fs::write(dir.join(crate::config::DB_FILE), "not a redb").unwrap();

        assert!(
            holds_data(&dir, Adopting::Into),
            "a destination whose store cannot be read must never be written over on a maybe"
        );
        assert!(
            !holds_data(&dir, Adopting::From),
            "and a store nothing can read is the last thing to copy out of"
        );

        std::fs::create_dir_all(dir.join("segments")).unwrap();
        std::fs::write(dir.join("segments").join("0000.parquet"), "sealed rows").unwrap();
        assert!(
            !holds_data(&dir, Adopting::From),
            "sealed segments beside an unreadable store do not rescue the candidate - the copy takes \
             the store too"
        );
        assert!(holds_data(&dir, Adopting::Into));
    }

    /// Issue #473: the `segments/` fallback at the bottom of [`holds_data`] answers when the store
    /// is silent - absent, or opened and empty - and every other test reaches a store that already
    /// says yes (real rows) or no-with-an-opinion (unreadable) before that line is ever run. Delete
    /// the arm and this predicate can only ever see the store, yet 27 other tests kept passing.
    ///
    /// Both silences are covered: no store file at all (the pre-first-block dataset this function's
    /// doc comment describes), and a store that exists and opens clean but was never written to
    /// (`Store::open` creates on the way in, same as an interrupted start leaves behind).
    #[test]
    fn holds_data_falls_through_to_sealed_segments_when_the_store_is_silent() {
        let d = tempfile::tempdir().unwrap();
        let dir = d.path().join("dataset");
        std::fs::create_dir_all(&dir).unwrap();

        assert!(
            !holds_data(&dir, Adopting::Into),
            "no store and no segments/ at all is not data"
        );

        std::fs::create_dir_all(dir.join("segments")).unwrap();
        assert!(
            !holds_data(&dir, Adopting::Into),
            "segments/ that exists but is empty is not data either"
        );

        std::fs::write(dir.join("segments").join("0000.parquet"), "sealed rows").unwrap();
        assert!(
            holds_data(&dir, Adopting::Into),
            "a sealed segment with no store at all must still read as data"
        );
        assert!(
            holds_data(&dir, Adopting::From),
            "and be adoptable-from on that segment alone"
        );

        // A store that opens cleanly and was never written to - `store_holds_rows` answers `Ok(false)`
        // rather than erroring, so this exercises the fall-through after the match, not the error arm
        // `an_unreadable_store_answers_the_two_adoption_directions_differently` already covers.
        std::fs::remove_file(dir.join("segments").join("0000.parquet")).unwrap();
        crate::store::Store::open(&dir.join(crate::config::DB_FILE)).unwrap();
        assert!(
            !holds_data(&dir, Adopting::Into),
            "an empty, cleanly-opened store beside an empty segments/ is not data"
        );

        std::fs::write(dir.join("segments").join("0000.parquet"), "sealed rows").unwrap();
        assert!(
            holds_data(&dir, Adopting::Into),
            "an empty store must not shadow sealed segments sitting beside it"
        );
    }

    /// The same rule driven through `adopt_dataset` rather than asserted on the predicate: an
    /// unreadable candidate must leave the destination inputs-only and **still adoptable**, not
    /// carrying a store that will wedge it.
    #[test]
    fn an_unreadable_candidate_is_not_copied_into_the_destination() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        let old = migrated_nest(root, "usdc");
        let src = MountTable::data_dir(root, &old);
        std::fs::write(src.join(crate::config::DB_FILE), "not a redb").unwrap();

        let new = cosmetic_sibling(root, &old);
        let dest = MountTable::data_dir(root, &new);

        assert_eq!(
            adopt_dataset(root, &dest, &new).unwrap(),
            None,
            "an unreadable store is not history worth spending the one adoption on"
        );
        assert!(
            !dest.join(crate::config::DB_FILE).exists(),
            "and nothing must have been copied in - a store that errors on open reads as data on \
             every later start, so the destination could never adopt again"
        );

        // The claim that this is recoverable, checked rather than asserted in prose: repair the
        // source and the destination adopts on the next start.
        std::fs::remove_file(src.join(crate::config::DB_FILE)).unwrap();
        seed_history(&src, 4242);
        assert_eq!(
            adopt_dataset(root, &dest, &new)
                .unwrap()
                .map(|a| a.from_nid),
            Some(old),
            "declining an unreadable candidate must cost nothing once it is readable"
        );
        assert_eq!(history_head(&dest), Some(4242));
    }

    /// Issue #408, the destination side, and the lasting half of it. An interrupted start leaves the
    /// destination with an empty store of its own; under a presence check that dataset then read as
    /// holding data and could **never** adopt again, including from the sibling that holds the real
    /// history. The cost was permanent rather than one-off, which is what made it worth fixing.
    #[test]
    fn an_interrupted_start_does_not_cost_a_dataset_its_cutoff() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        let old = migrated_nest(root, "usdc");
        let src = MountTable::data_dir(root, &old);
        seed_history(&src, 4242);
        std::fs::create_dir_all(src.join("segments")).unwrap();
        std::fs::write(src.join("segments").join("0000.parquet"), "sealed rows").unwrap();

        let new = cosmetic_sibling(root, &old);
        let dest = MountTable::data_dir(root, &new);
        // The interrupted start: a cursor came up here once, created its store, and got nowhere.
        {
            crate::store::Store::open(&dest.join(crate::config::DB_FILE))
                .expect("an empty store must be creatable");
        }
        // The other path the same interrupted start leaves behind, which #408 flagged and did not
        // trace: `segments/` is created before the first segment is written into it.
        std::fs::create_dir_all(dest.join("segments")).unwrap();

        let adoption = adopt_dataset(root, &dest, &new)
            .unwrap()
            .expect("an empty store is not history, so it must not block the cutoff");
        assert_eq!(adoption.from_nid, old);
        assert_eq!(
            history_head(&dest),
            Some(4242),
            "and the adopted store must replace the empty one"
        );
    }

    /// The baseline: an untouched dataset must report nothing, or the check is just noise.
    #[test]
    fn an_intact_dataset_reports_no_identity_drift() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        migrated_nest(root, "usdc");

        let mounts = MountTable::load(root).unwrap();
        assert_eq!(
            mounts.identity_drift(root),
            vec![],
            "a dataset whose inputs are untouched must not be reported as drifted"
        );
    }

    /// RFC-0032 §3, and the defect this check exists for: `mount()` resolves `data/<nid>` straight
    /// out of the mount record and never recomputes it, so editing a file under a mounted dataset
    /// left the record claiming an identity that no longer described anything. Content addressing
    /// nobody verifies is not content addressing.
    ///
    /// **This test fails against the build before `identity_drift` existed** - nothing anywhere in
    /// `runtime.rs` called `nest_nid`, `nid_of` or `verify_registry_reproduces`.
    #[test]
    fn editing_a_mounted_dataset_in_place_is_caught_as_identity_drift() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        let claimed = migrated_nest(root, "usdc");

        // Exactly the operator action that used to go unnoticed: edit an input under the live dataset.
        let views = MountTable::data_dir(root, &claimed).join("views");
        std::fs::create_dir_all(&views).unwrap();
        std::fs::write(views.join("10-example.sql"), "-- added after mounting\n").unwrap();

        let mounts = MountTable::load(root).unwrap();
        let drift = mounts.identity_drift(root);

        assert_eq!(
            drift.len(),
            1,
            "the edited dataset must be reported: {drift:?}"
        );
        // The canonical mount is displayed tenant-qualified (`<tenant>/<alias>`), which is what an
        // operator sees in the routes, so assert on containment rather than pinning the shape here.
        assert!(
            drift[0].alias.contains("usdc"),
            "the report must name the mount: {}",
            drift[0].alias
        );
        assert_eq!(drift[0].claimed, claimed);
        assert_eq!(
            drift[0].actual.as_deref().map(|a| a != claimed),
            Some(true),
            "the edit must change what the inputs hash to, or the fixture is not exercising anything"
        );
        // The message has to name both identities, or an operator cannot tell what happened.
        let msg = drift[0].to_string();
        assert!(
            msg.contains(&claimed[..12]),
            "message must name the claim: {msg}"
        );
    }

    /// A pre-2.0 nest under `nests/<alias>` claims no identity, so there is nothing to contradict.
    /// Reporting it would make `migrate`'s supported half-migrated state look like a fault.
    #[test]
    fn an_un_migrated_nest_claims_no_identity_so_cannot_drift() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        write_roost(root, "arbitrum-one", 42161, "arbitrum-one", 42161);

        let mounts = MountTable::load(root).unwrap();
        assert_eq!(
            mounts.identity_drift(root),
            vec![],
            "a nest with no nid in its record must not be reported as drifted"
        );
    }

    /// The unmount path rewrites `mounts.toml` from **route keys**, which are tenant-qualified in a
    /// multi-tenant runtime. Matching them against `alias` alone would retain nothing and silently
    /// unmount every co-tenant on the first unmount - so this asserts the survivor, not the casualty.
    #[test]
    fn persisting_after_an_unmount_keeps_the_other_tenants_mount() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        let nid = "aa11".repeat(16);
        std::fs::write(
            root.join(MOUNTS_FILE),
            format!(
                "[runtime]\nname = \"r\"\nchain = \"arbitrum-one\"\nchain_id = 42161\n\
                 rpc_urls = []\n\n\
                 [[mounts]]\ntenant = \"acme\"\nalias = \"usdc\"\nnid = \"{nid}\"\n\n\
                 [[mounts]]\ntenant = \"globex\"\nalias = \"usdc\"\nnid = \"{nid}\"\n"
            ),
        )
        .unwrap();

        // acme unmounts; the runtime persists what is left, by route key.
        persist_mounted_nests(root, &["globex/usdc".to_string()], &[], &[]).unwrap();

        let after = MountTable::load(root).expect("the rewritten file must still load");
        assert_eq!(after.mounts.len(), 1, "the co-tenant's mount was dropped");
        assert_eq!(after.mounts[0].tenant, "globex");
        assert_eq!(after.mounts[0].alias, "usdc");
        assert_eq!(
            after.mounts[0].nid, nid,
            "the surviving mount must still point at the same dataset"
        );
    }

    /// #1545: a runtime that started with nothing mounted has neither `[[mounts]]` nor a `nests`
    /// list. Its file must load, and its first live mount must be written as a record by nid, or the
    /// restart looks for `nests/<name>` and the mount is gone.
    #[test]
    fn an_empty_runtimes_first_mount_is_persisted_by_nid() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        let nid = "cc33".repeat(16);
        std::fs::write(
            root.join(MOUNTS_FILE),
            "[runtime]\nname = \"r\"\n\n\
             [[chains]]\nchain = \"arbitrum-one\"\nchain_id = 42161\nrpc_urls = []\n",
        )
        .unwrap();
        let empty = MountTable::load(root).expect("an empty runtime's file must load");
        assert!(empty.mount_refs().is_empty());
        assert_eq!(empty.chain_endpoints().unwrap().len(), 1);

        let known: MountTable = toml::from_str(&format!(
            "[runtime]\nname = \"r\"\n\n[[mounts]]\nalias = \"usdc\"\nnid = \"{nid}\"\n"
        ))
        .unwrap();
        persist_mounted_nests(root, &["usdc".to_string()], &known.mounts, &[]).unwrap();

        let after = MountTable::load(root).expect("the rewritten file must still load");
        assert_eq!(after.mounts.len(), 1, "the mount was not recorded");
        assert_eq!(after.mounts[0].alias, "usdc");
        assert_eq!(after.mounts[0].nid, nid);
        assert!(
            after.runtime.nests.is_empty(),
            "a 2.0 mount written to the pre-2.0 list: {:?}",
            after.runtime.nests
        );
    }

    /// The unmount has already dropped the suspension. A mounts.toml that cannot be rewritten must
    /// still come back as a failure, or a restart brings the nest back and nobody was told.
    #[tokio::test]
    async fn a_failed_mounts_write_is_not_reported_as_success() {
        let root = tempfile::tempdir().unwrap();
        let nid = "ab".repeat(32);
        std::fs::write(
            root.path().join(MOUNTS_FILE),
            format!(
                "[runtime]\nname = \"prod\"\nchain = \"ethereum\"\nchain_id = 1\n\
                 suspended = [\"usdc\"]\n\n\
                 [[mounts]]\nalias = \"usdc\"\nnid = \"{nid}\"\n"
            ),
        )
        .unwrap();
        let health = Arc::new(crate::health::RuntimeHealth::new());
        let roster = serde_json::json!({"runtime": "prod", "nests": []});
        let mut handles = RuntimeHandles {
            live: crate::serve::LiveRuntime::new(crate::serve::compose_runtime(
                roster.clone(),
                Vec::new(),
                health.clone(),
            )),
            states: Vec::new(),
            alert_workers: Vec::new(),
            publishers: Vec::new(),
            lifecycle: std::collections::HashMap::new(),
            health,
            roster,
            estimates: std::collections::HashMap::new(),
            default_tenant: DEFAULT_TENANT.to_string(),
            suspended: std::collections::BTreeMap::from([("usdc".to_string(), nid.clone())]),
            joining: Default::default(),
            mount_ctx: MountContext {
                dir: root.path().to_path_buf(),
                mounts: Vec::new(),
                sources: std::collections::HashMap::new(),
                endpoint_counts: std::collections::HashMap::new(),
                backfill: None,
                seal_direct: false,
                concurrency: 1,
                ipfs_window_deadline: std::time::Duration::from_secs(1),
                window_override: None,
                admin_enabled: true,
                admin_token: None,
                max_rss_mb: DEFAULT_MAX_RSS_MB,
                freshness: Default::default(),
                chain_freshness: std::collections::HashMap::new(),
                dormant: std::collections::HashMap::new(),
                fail_fast: false,
                cursors: None,
                registry: None,
            },
        };
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(root.path()).unwrap().permissions().mode();
        let mut perms = std::fs::metadata(root.path()).unwrap().permissions();
        perms.set_readonly(true);
        std::fs::set_permissions(root.path(), perms).unwrap();
        struct Unlock<'a>(&'a std::path::Path, u32);
        impl Drop for Unlock<'_> {
            fn drop(&mut self) {
                use std::os::unix::fs::PermissionsExt;
                let mut perms = std::fs::metadata(self.0).unwrap().permissions();
                perms.set_mode(self.1);
                let _ = std::fs::set_permissions(self.0, perms);
            }
        }
        let _unlock = Unlock(root.path(), mode);

        let err = handles
            .unmount("usdc")
            .await
            .expect_err("a mounts.toml that cannot be written is not a successful unmount");
        let err = format!("{err:#}");
        assert!(
            err.contains("will not survive a restart"),
            "the caller must hear that the live change is not durable: {err}"
        );
        assert!(
            !handles.suspended.contains_key("usdc"),
            "the live unmount stays; only the report of success was wrong"
        );
        let on_disk = std::fs::read_to_string(root.path().join(MOUNTS_FILE)).unwrap();
        assert!(
            on_disk.contains("usdc"),
            "the failed rewrite must leave the old table: {on_disk}"
        );
    }

    /// #1565, and the 3.13.0 tyre-kick: a mount's route key depends on its own tenant alone, so it
    /// is the same before and after a restart whatever else is mounted, and a suspended
    /// `tenant/alias` is still found suspended when the table is read back.
    #[test]
    fn route_keys_are_per_mount_and_survive_a_second_tenant() {
        let refs = |t: &MountTable| -> Vec<String> {
            let d = t.tenant_default();
            t.mount_refs().iter().map(|m| m.route_key(&d)).collect()
        };
        let one: MountTable = toml::from_str(
            "[runtime]\nname = \"r\"\n\n[[mounts]]\ntenant = \"acme\"\nalias = \"usdc\"\nnid = \"x\"\n",
        )
        .unwrap();
        assert_eq!(refs(&one), vec!["acme/usdc"], "one non-default tenant");
        let two: MountTable = toml::from_str(
            "[runtime]\nname = \"r\"\n\n\
             [[mounts]]\ntenant = \"acme\"\nalias = \"usdc\"\nnid = \"x\"\n\n\
             [[mounts]]\nalias = \"usdc2\"\nnid = \"y\"\n\n\
             [[mounts]]\ntenant = \"globex\"\nalias = \"usdc\"\nnid = \"x\"\n",
        )
        .unwrap();
        assert_eq!(
            refs(&two),
            vec!["acme/usdc", "usdc2", "globex/usdc"],
            "a second tenant moves nobody"
        );
        let configured: MountTable = toml::from_str(
            "[runtime]\nname = \"r\"\ndefault_tenant = \"acme\"\n\n\
             [[mounts]]\ntenant = \"acme\"\nalias = \"usdc\"\nnid = \"x\"\n",
        )
        .unwrap();
        assert_eq!(
            refs(&configured),
            vec!["usdc"],
            "a configured default tenant keeps its routes"
        );
        let unsaid: MountTable = toml::from_str(
            "[runtime]\nname = \"r\"\ndefault_tenant = \"acme\"\n\n\
             [[mounts]]\nalias = \"usdc\"\nnid = \"x\"\n",
        )
        .unwrap();
        assert_eq!(
            unsaid.mounts[0].tenant, "acme",
            "a record that names no tenant takes the configured default, as a nests entry does"
        );

        let suspended: MountTable = toml::from_str(
            "[runtime]\nname = \"r\"\nsuspended = [\"acme/usdc\"]\n\n\
             [[mounts]]\ntenant = \"acme\"\nalias = \"usdc\"\nnid = \"x\"\n",
        )
        .unwrap();
        let (active, held) = split_suspended(&suspended);
        assert!(
            active.mounts.is_empty(),
            "a suspended tenant mount booted live"
        );
        assert_eq!(held.get("acme/usdc").map(String::as_str), Some("x"));
    }

    /// #1534: a second tenant mounted live has to be written, or the mount vanishes on restart. Keys
    /// are per mount (#1565), so both tenants here are keyed `tenant/alias`.
    #[test]
    fn a_second_tenant_mounted_live_is_written_while_the_file_is_still_single_tenant() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        let acme = "aa11".repeat(16);
        let globex = "bb22".repeat(16);
        std::fs::write(
            root.join(MOUNTS_FILE),
            format!(
                "[runtime]\nname = \"r\"\nchain = \"arbitrum-one\"\nchain_id = 42161\n\
                 rpc_urls = []\n\n\
                 [[mounts]]\ntenant = \"acme\"\nalias = \"usdc\"\nnid = \"{acme}\"\n"
            ),
        )
        .unwrap();
        let known: MountTable = toml::from_str(&format!(
            "[runtime]\nname = \"r\"\nchain = \"arbitrum-one\"\nchain_id = 42161\n\
             rpc_urls = []\n\n\
             [[mounts]]\ntenant = \"acme\"\nalias = \"usdc\"\nnid = \"{acme}\"\n\n\
             [[mounts]]\ntenant = \"globex\"\nalias = \"usdc\"\nnid = \"{globex}\"\n"
        ))
        .unwrap();

        persist_mounted_nests(
            root,
            &["acme/usdc".to_string(), "globex/usdc".to_string()],
            &known.mounts,
            &[],
        )
        .unwrap();

        let after = MountTable::load(root).expect("the rewritten file must still load");
        assert_eq!(
            after.mounts.len(),
            2,
            "the new tenant was dropped: {after:?}"
        );
        assert!(after
            .mounts
            .iter()
            .any(|m| m.tenant == "acme" && m.nid == acme));
        assert!(after
            .mounts
            .iter()
            .any(|m| m.tenant == "globex" && m.alias == "usdc" && m.nid == globex));
    }

    /// RFC-0032 slice 5: the runtime is retired, so a directory still holding the pre-2.0 file is a
    /// **migration that has not been run** - and must say so, rather than reporting a missing
    /// `mounts.toml` an operator never had.
    #[test]
    fn a_pre_2_0_directory_is_pointed_at_migrate() {
        let d = tempfile::tempdir().unwrap();
        std::fs::write(
            d.path().join(LEGACY_ROOST_FILE),
            "[runtime]\nname = \"r\"\nchain = \"arbitrum-one\"\nchain_id = 42161\n\
             rpc_urls = []\nnests = [\"a\"]\n",
        )
        .unwrap();
        let err = MountTable::load(d.path()).unwrap_err().to_string();
        assert!(
            err.contains("nuthatch migrate"),
            "the error must name the fix: {err}"
        );
        assert!(
            err.contains("never re-indexes"),
            "and reassure about the cost: {err}"
        );
        assert!(
            err.contains("retired in 2.0"),
            "and say why the file stopped working: {err}"
        );

        // Once migrated the old file is irrelevant: `mounts.toml` is what is read.
        std::fs::write(
            d.path().join(MOUNTS_FILE),
            format!(
                "[runtime]\nname = \"r\"\nchain = \"arbitrum-one\"\nchain_id = 42161\n\
                 rpc_urls = []\n\n[[mounts]]\nalias = \"a\"\nnid = \"{}\"\n",
                "aa11".repeat(16)
            ),
        )
        .unwrap();
        let mounts = MountTable::load(d.path()).expect("a migrated directory loads");
        assert_eq!(mounts.mounts.len(), 1);
    }

    /// RFC-0032 §4: grouping is by *dataset*, and a runtime may be half-migrated while it happens.
    #[test]
    fn datasets_group_by_identity_across_both_layouts() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        std::fs::write(
            root.join(MOUNTS_FILE),
            "[runtime]\nname = \"r\"\nchain = \"arbitrum-one\"\nchain_id = 42161\n\
             rpc_urls = []\nnests = [\"a\", \"b\", \"legacy\"]\n\n\
             [[mounts]]\nalias = \"a\"\nnid = \"aa11\"\n\n\
             [[mounts]]\nalias = \"b\"\nnid = \"aa11\"\n"
                .replace("aa11", &"aa11".repeat(16)),
        )
        .unwrap();
        let mounts = MountTable::load(root).unwrap();
        let ds = mounts.datasets(root);

        // Two migrated aliases collapse to one dataset; the un-migrated nest keeps its own.
        assert_eq!(ds.len(), 2, "expected 2 datasets, got {ds:?}");
        assert_eq!(aliases(&ds[0]), vec!["a", "b"]);
        assert_eq!(ds[0].refcount(), 2);
        assert_eq!(ds[0].canonical().alias, "a", "the first mount indexes");
        assert!(ds[0].dir.starts_with(root.join(DATA_DIR)));

        assert_eq!(aliases(&ds[1]), vec!["legacy"]);
        assert_eq!(ds[1].refcount(), 1);
        assert_eq!(
            ds[1].nid, None,
            "an un-migrated nest has no identity recorded"
        );
        assert!(ds[1].dir.starts_with(root.join(NESTS_DIR)));
    }

    /// A mount record is untrusted input - `mounts.toml` is operator-editable and a roster may come
    /// from a resolved bundle - so the identity and the path segments are checked at load.
    #[test]
    fn a_mount_record_is_validated() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        let base = "[runtime]\nname = \"r\"\nchain = \"arbitrum-one\"\nchain_id = 42161\n\
                    rpc_urls = []\n\n[[mounts]]\n";
        let good = "aa11".repeat(16);

        for (record, expect) in [
            // Too short, and not hex - a NID is a path segment before it is anything else.
            ("alias = \"a\"\nnid = \"aa\"\n".to_string(), "invalid nest identity"),
            (
                format!("alias = \"a\"\nnid = \"{}\"\n", "zz".repeat(32)),
                "invalid nest identity",
            ),
            (
                format!("alias = \"../esc\"\nnid = \"{good}\"\n"),
                "nest name '../esc' is invalid",
            ),
            (
                format!("alias = \"health\"\nnid = \"{good}\"\n"),
                "reserved",
            ),
            // Root routes too: nested there, the nest's `/` is the root's route and `nest` panics (#2130).
            (
                format!("alias = \"ready\"\nnid = \"{good}\"\n"),
                "reserved",
            ),
            (
                format!("tenant = \"metrics\"\nalias = \"a\"\nnid = \"{good}\"\n"),
                "reserved",
            ),
            // `acme` and `acme/sql` would both serve `/acme/sql`.
            (
                format!(
                    "alias = \"acme\"\nnid = \"{good}\"\n\n[[mounts]]\ntenant = \"acme\"\n\
                     alias = \"sql\"\nnid = \"{good}\"\n"
                ),
                "is also the name of a mount",
            ),
            // The record names a directory; lowering it would point at another one (#2137).
            (
                format!("alias = \"a\"\nnid = \"{}\"\n", good.to_ascii_uppercase()),
                "upper case",
            ),
            // The primary key is (tenant, alias), so the *same* pair twice is the collision.
            (
                format!(
                    "alias = \"a\"\nnid = \"{good}\"\n\n[[mounts]]\nalias = \"a\"\nnid = \"{good}\"\n"
                ),
                "more than once",
            ),
        ] {
            std::fs::write(root.join(MOUNTS_FILE), format!("{base}{record}")).unwrap();
            let err = MountTable::load(root).unwrap_err().to_string();
            assert!(err.contains(expect), "expected {expect:?}, got: {err}");
        }

        // ...and the same alias under two *different* tenants is fine, which is the whole point.
        std::fs::write(
            root.join(MOUNTS_FILE),
            format!(
                "{base}tenant = \"acme\"\nalias = \"a\"\nnid = \"{good}\"\n\n\
                 [[mounts]]\ntenant = \"globex\"\nalias = \"a\"\nnid = \"{good}\"\n"
            ),
        )
        .unwrap();
        let mounts = MountTable::load(root).expect("two tenants may share an alias");
        assert!(mounts.is_multi_tenant());
        assert_eq!(mounts.mount_refs().len(), 2);
    }

    /// `Nid::parse` is the only constructor - this is every caller `RuntimeHandles::mount` now has,
    /// including the admin surface's `POST /_admin/nests`.
    #[test]
    fn nid_parse_rejects_anything_that_is_not_64_hex_characters() {
        let good = "aa11".repeat(16);
        assert_eq!(Nid::parse(&good).unwrap().as_str(), good);
        assert_eq!(
            Nid::parse(&good.to_ascii_uppercase()).unwrap().as_str(),
            good,
            "one dataset, one spelling (#2137)"
        );

        let bad = [
            "abc".to_string(), // the realistic typo: short, and not even all hex
            "zz".repeat(32),   // right length, wrong alphabet
            good[..good.len() - 1].to_string(), // one character short
            "../../../../etc".to_string(), // traversal - not a NID at all
            "/etc/passwd".to_string(), // absolute - Path::join would discard the mount root
        ];
        for bad in &bad {
            let err = Nid::parse(bad).unwrap_err().to_string();
            assert!(
                err.contains("invalid") && err.contains("64 hex"),
                "expected a 64-hex complaint for {bad:?}, got: {err}"
            );
        }
    }

    #[test]
    fn loads_a_valid_roost() {
        let d = tempfile::tempdir().unwrap();
        write_roost(d.path(), "arbitrum-one", 42161, "arbitrum-one", 42161);
        let r = MountTable::load(d.path()).unwrap();
        assert_eq!(r.runtime.nests, vec!["a"]);
        // 2.0 declares chains under `[[chains]]` only - one form, one meaning (RFC-0035 §2).
        assert!(
            r.runtime.chain.is_none(),
            "the top-level shorthand is gone; these fields exist only for `migrate` to read"
        );
        let endpoints = r.chain_endpoints().unwrap();
        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0].chain, "arbitrum-one");
    }

    #[test]
    fn rejects_a_nest_whose_chain_isnt_declared() {
        let d = tempfile::tempdir().unwrap();
        // MountTable declares arbitrum-one; the nest claims mainnet → hard error at grouping.
        write_roost(d.path(), "arbitrum-one", 42161, "mainnet", 1);
        let mounts = MountTable::load(d.path()).unwrap();
        let err = group_by_chain(
            &mounts.chain_endpoints().unwrap(),
            vec![mounted(d.path(), "a")],
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("doesn't declare"), "got: {err}");
    }

    #[test]
    fn multichain_roost_groups_nests_by_chain() {
        let d = tempfile::tempdir().unwrap();
        std::fs::write(
            d.path().join(MOUNTS_FILE),
            "[runtime]\nname = \"multi\"\nnests = [\"a\", \"b\"]\n\n\
             [[chains]]\nchain = \"base\"\nchain_id = 8453\nrpc_urls = [\"http://base\"]\n\n\
             [[chains]]\nchain = \"arbitrum-one\"\nchain_id = 42161\nrpc_urls = [\"http://arb\"]\n",
        )
        .unwrap();
        write_nest_dir(d.path(), "a", "base", 8453);
        write_nest_dir(d.path(), "b", "arbitrum-one", 42161);
        let mounts = MountTable::load(d.path()).unwrap();
        let endpoints = mounts.chain_endpoints().unwrap();
        assert_eq!(endpoints.len(), 2, "two declared chains");
        let groups = group_by_chain(
            &endpoints,
            vec![mounted(d.path(), "a"), mounted(d.path(), "b")],
        )
        .unwrap();
        assert_eq!(groups.len(), 2, "one cursor per chain");
        for g in &groups {
            assert_eq!(g.nests.len(), 1, "each chain has its one nest");
        }
    }

    /// RFC-0035 §2: the pre-2.0 top-level chain form is refused outright in 2.0, and the refusal
    /// names the way forward rather than just the rule. (It used to be accepted, and only *combining*
    /// it with `[[chains]]` was an error.)
    #[test]
    fn the_pre_2_0_top_level_chain_form_is_refused_with_a_pointer() {
        let d = tempfile::tempdir().unwrap();
        std::fs::write(
            d.path().join(MOUNTS_FILE),
            "[runtime]\nname = \"t\"\nchain = \"mainnet\"\nchain_id = 1\n\
             rpc_urls = [\"http://x\"]\nnests = [\"a\"]\n",
        )
        .unwrap();
        let err = MountTable::load(d.path())
            .unwrap()
            .chain_endpoints()
            .unwrap_err()
            .to_string();
        assert!(err.contains("pre-2.0 top-level chain form"), "{err}");
        assert!(
            err.contains("nuthatch migrate"),
            "the refusal must name the fix: {err}"
        );

        // ...and `migrate` can still *read* it, which is the whole reason the fields survive.
        let table = MountTable::load_for_migration(d.path()).unwrap();
        let carried = table
            .chains_from_legacy()
            .expect("migrate must be able to translate it");
        assert_eq!(carried.chain, "mainnet");
        assert_eq!(carried.chain_id, 1);
        assert_eq!(carried.rpc_urls, vec!["http://x".to_string()]);
    }

    #[test]
    fn rejects_unsafe_nest_names() {
        // SEC-10: a nest name that could escape the nests dir or make a surprising route is refused.
        for bad in ["../etc", "a/b", "", "has space"] {
            let d = tempfile::tempdir().unwrap();
            std::fs::write(
                d.path().join(MOUNTS_FILE),
                format!("[runtime]\nname = \"t\"\nchain = \"c\"\nchain_id = 1\nrpc_urls = [\"u\"]\nnests = [\"{bad}\"]\n"),
            )
            .unwrap();
            let err = MountTable::load(d.path()).unwrap_err().to_string();
            assert!(
                err.contains("invalid") || err.contains("reserved"),
                "name {bad:?} should be rejected, got: {err}"
            );
        }
    }

    #[test]
    fn rejects_reserved_and_duplicate_nest_names() {
        let d = tempfile::tempdir().unwrap();
        std::fs::write(
            d.path().join(MOUNTS_FILE),
            "[runtime]\nname = \"t\"\nchain = \"c\"\nchain_id = 1\nrpc_urls = [\"u\"]\nnests = [\"nests\"]\n",
        )
        .unwrap();
        assert!(MountTable::load(d.path())
            .unwrap_err()
            .to_string()
            .contains("reserved"));

        std::fs::write(
            d.path().join(MOUNTS_FILE),
            "[runtime]\nname = \"t\"\nchain = \"c\"\nchain_id = 1\nrpc_urls = [\"u\"]\nnests = [\"a\", \"a\"]\n",
        )
        .unwrap();
        assert!(MountTable::load(d.path())
            .unwrap_err()
            .to_string()
            .contains("more than once"));
    }

    #[test]
    fn a_zero_mount_table_loads_for_prune_but_is_empty() {
        let d = tempfile::tempdir().unwrap();
        std::fs::write(
            d.path().join(MOUNTS_FILE),
            "[runtime]\nname = \"t\"\nchain = \"c\"\nchain_id = 1\nrpc_urls = [\"u\"]\nnests = []\n",
        )
        .unwrap();
        let t = MountTable::load(d.path()).expect("a readable zero-mount table must load");
        assert!(t.mount_refs().is_empty());
    }

    #[test]
    fn footprint_estimate_scales_with_views() {
        fn cfg(extra: &str) -> Config {
            let toml = format!(
                "[nest]\nname = \"n\"\nchain = \"c\"\nchain_id = 1\nrpc_urls = []\n\n\
                 [[contracts]]\nalias = \"t\"\naddress = \"0x1\"\nabi = \"a.json\"\n{extra}"
            );
            toml::from_str(&toml).unwrap()
        }
        // Plain static nest, no labels: just the per-nest base.
        assert_eq!(
            estimate_nest_rss_mb(&cfg(""), false, 0, 0),
            NEST_BASE_RSS_MB
        );
        // Labels present → the exposure view adds a chunk.
        assert_eq!(
            estimate_nest_rss_mb(&cfg(""), true, 0, 0),
            NEST_BASE_RSS_MB + NEST_VIEW_RSS_MB
        );
        // A velocity flag → the velocity view.
        let vel = cfg("\n[flags]\nvelocity_amount = \"1000\"\n");
        assert_eq!(
            estimate_nest_rss_mb(&vel, false, 0, 0),
            NEST_BASE_RSS_MB + NEST_VIEW_RSS_MB
        );
        // A factory → the discovered-child registry.
        let fac = cfg("\n[[templates]]\nname = \"p\"\nabi = \"p.json\"\n\n\
             [[factories]]\nwatch = \"t\"\nevent = \"E\"\nchild_param = \"c\"\ntemplate = \"p\"\n");
        assert_eq!(
            estimate_nest_rss_mb(&fac, false, 0, 0),
            NEST_BASE_RSS_MB + NEST_VIEW_RSS_MB
        );
        // All three loads stack on top of the base.
        let all = cfg(
            "\n[flags]\nvelocity_amount = \"1000\"\n\n[[templates]]\nname = \"p\"\nabi = \"p.json\"\n\n\
             [[factories]]\nwatch = \"t\"\nevent = \"E\"\nchild_param = \"c\"\ntemplate = \"p\"\n",
        );
        assert_eq!(
            estimate_nest_rss_mb(&all, true, 0, 0),
            NEST_BASE_RSS_MB + 3 * NEST_VIEW_RSS_MB
        );
    }

    /// #1437: an entity reading an offchain table is charged for the offchain input it may hold as
    /// well as its own rows, so their allowances add up inside the cursor budget.
    #[test]
    fn an_offchain_entity_is_charged_for_its_offchain_allowance() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("entities")).unwrap();
        std::fs::write(dir.path().join("entities/by_tier.sql"),
            "SELECT t.tier, sum(x.value) AS v FROM usdc__transfer x JOIN offchain__tiers t ON x.\"to\" = t.account GROUP BY t.tier").unwrap();
        std::fs::write(
            dir.path().join("entities/totals.sql"),
            "SELECT \"to\", sum(value) AS v FROM usdc__transfer GROUP BY \"to\"",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("entities.toml"),
            "[[entities]]\nname='by_tier'\nkey=['tier']\nmax_rows=10\n\
             sql='entities/by_tier.sql'\n\
             [[entities]]\nname='totals'\nkey=['to']\nmax_rows=5\n\
             sql='entities/totals.sql'\n",
        )
        .unwrap();
        assert_eq!(declared_entities(dir.path()), (2, 2 * 10 + 5));
    }

    /// RFC-0041 §7 criterion 9: an entity's declared `max_rows` reaches the admission calculation.
    ///
    /// The two terms are priced separately on purpose. A circuit and its thread cost the same as any
    /// built-in view, and the declared state costs whatever the author declared - which is the term
    /// the built-in views do not let anyone choose, and therefore the one that can surprise a budget.
    #[test]
    fn a_declared_entity_costs_its_circuit_and_its_declared_rows() {
        fn cfg(extra: &str) -> Config {
            let toml = format!(
                "[nest]\nname = \"n\"\nchain = \"c\"\nchain_id = 1\nrpc_urls = []\n\n\
                 [[contracts]]\nalias = \"t\"\naddress = \"0x1\"\nabi = \"a.json\"\n{extra}"
            );
            toml::from_str(&toml).expect("fixture config")
        }

        let base = estimate_nest_rss_mb(&cfg(""), false, 0, 0);

        assert_eq!(
            estimate_nest_rss_mb(&cfg(""), false, 1, 0),
            base + ENTITY_CIRCUIT_RSS_MB,
            "a circuit and a thread, at the measured price"
        );

        // A million rows at the per-row estimate is a term nobody would have guessed from the nest's
        // config alone, which is exactly why it has to be in the projection.
        let million = 1_000_000u64;
        assert_eq!(
            estimate_nest_rss_mb(&cfg(""), false, 1, million),
            base + ENTITY_CIRCUIT_RSS_MB + (million * ENTITY_RSS_BYTES_PER_ROW) / (1024 * 1024),
        );

        // Two entities are two circuits, and their rows add - nests on a cursor are accounted
        // together, so entities within a nest must be too.
        assert_eq!(
            estimate_nest_rss_mb(&cfg(""), false, 2, 2 * million),
            base + 2 * ENTITY_CIRCUIT_RSS_MB
                + (2 * million * ENTITY_RSS_BYTES_PER_ROW) / (1024 * 1024),
        );
    }

    /// Criterion 11: nests on one chain share a cursor and are counted **together**, and the refusal
    /// carries the arithmetic an operator needs to act on it.
    #[test]
    fn the_entity_ceiling_is_per_cursor_and_says_how_far_over_it_is() {
        assert!(refuse_over_entity_ceiling(MAX_ENTITIES_PER_CURSOR, 1, "m", "c").is_ok());
        assert!(
            refuse_over_entity_ceiling(MAX_ENTITIES_PER_CURSOR, 8, "m", "c").is_ok(),
            "spread across eight nests is still exactly at the ceiling, not over it"
        );
        assert!(
            refuse_over_entity_ceiling(MAX_ENTITIES_PER_CURSOR + 1, 1, "m", "c").is_err(),
            "one over is over - the boundary, which a wildly-over case cannot see"
        );

        let err = format!(
            "{:#}",
            refuse_over_entity_ceiling(MAX_ENTITIES_PER_CURSOR + 3, 4, "prod", "arbitrum-one")
                .expect_err("three over the ceiling is over the ceiling")
        );
        assert!(err.contains("cursor on arbitrum-one"), "{err}");
        assert!(err.contains("across 4 nest(s)"), "{err}");
        assert!(
            err.contains("drop 3 of them"),
            "the overage must be the real number, not a restatement of the ceiling: {err}"
        );
    }

    /// Boot sums every nest on the chain. A live mount that only prices its own entities is
    /// admitted, and the next restart is what refuses the cursor.
    #[test]
    fn a_live_mount_past_the_entity_ceiling_is_refused() {
        fn write_entities(dir: &std::path::Path, n: usize) {
            std::fs::create_dir_all(dir).unwrap();
            let mut body = String::new();
            for i in 0..n {
                body.push_str(&format!(
                    "[[entities]]\nname = \"e{i}\"\nsql = \"entities/e{i}.sql\"\n\
                     key = [\"id\"]\nmax_rows = 1\n\n"
                ));
            }
            std::fs::write(dir.join("entities.toml"), body).unwrap();
        }
        fn nest_config(chain: &str) -> Config {
            toml::from_str(&format!(
                "[nest]\nname = \"n\"\nchain = \"{chain}\"\nchain_id = 1\nrpc_urls = []\n\n\
                 [[contracts]]\nalias = \"t\"\naddress = \"0x1\"\nabi = \"a.json\"\n"
            ))
            .unwrap()
        }

        let root = tempfile::tempdir().unwrap();
        let held = root.path().join("held");
        let other = root.path().join("other");
        let over = root.path().join("over");
        let exact = root.path().join("exact");
        // 26 on the cursor, plus an alias of that same dataset, plus 20 on another chain.
        // 26 + 7 is over. 26 + 6 is the ceiling. The alias and the other chain must not count.
        write_entities(&held, 26);
        write_entities(&other, 20);
        write_entities(&over, 7);
        write_entities(&exact, 6);
        let held_state = nest_state(&held, "ethereum");
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let mut sources = std::collections::HashMap::new();
        let source: Arc<dyn Source> = Arc::new(crate::source::UnpolledSource);
        sources.insert("ethereum".to_string(), source);
        let mut lifecycle = std::collections::HashMap::new();
        lifecycle.insert("ethereum".to_string(), tx);
        let health = Arc::new(crate::health::RuntimeHealth::new());
        let roster = serde_json::json!({"runtime": "prod", "nests": []});
        let handles = RuntimeHandles {
            live: crate::serve::LiveRuntime::new(crate::serve::compose_runtime(
                roster.clone(),
                Vec::new(),
                health.clone(),
            )),
            states: vec![
                ("usdc".to_string(), held_state.clone()),
                ("acme/usdc".to_string(), held_state),
                ("other".to_string(), nest_state(&other, "optimism")),
            ],
            alert_workers: Vec::new(),
            publishers: Vec::new(),
            lifecycle,
            health,
            roster,
            estimates: std::collections::HashMap::new(),
            default_tenant: DEFAULT_TENANT.to_string(),
            suspended: std::collections::BTreeMap::new(),
            joining: Default::default(),
            mount_ctx: MountContext {
                dir: root.path().to_path_buf(),
                mounts: Vec::new(),
                sources,
                endpoint_counts: std::collections::HashMap::new(),
                backfill: None,
                seal_direct: false,
                concurrency: 1,
                ipfs_window_deadline: std::time::Duration::from_secs(1),
                window_override: None,
                admin_enabled: true,
                admin_token: None,
                max_rss_mb: DEFAULT_MAX_RSS_MB,
                freshness: Default::default(),
                chain_freshness: std::collections::HashMap::new(),
                dormant: std::collections::HashMap::new(),
                fail_fast: false,
                cursors: None,
                registry: None,
            },
        };

        let err = match handles.admit("new", &nest_config("ethereum"), &over) {
            Err(e) => {
                let status = e.downcast_ref::<MountRefusal>().map(MountRefusal::status);
                assert_eq!(status, Some(507), "a refusal, not a 500 (#2139)");
                format!("{e:#}")
            }
            Ok(_) => panic!("33 authored entities on one cursor is over the ceiling"),
        };
        assert!(err.contains("mounts 'prod'"), "{err}");
        assert!(err.contains("cursor on ethereum"), "{err}");
        assert!(err.contains("declares 33"), "{err}");
        assert!(err.contains("across 2 nest(s)"), "{err}");
        assert!(err.contains("over the ceiling of 32"), "{err}");

        if let Err(e) = handles.admit("new", &nest_config("ethereum"), &exact) {
            panic!("32 across the cursor is the ceiling, not past it: {e:#}");
        }
        // The directory is already on the cursor, as `usdc` and `acme/usdc`. A new name for it
        // is an alias, not a second dataset.
        if let Err(e) = handles.admit("alias", &nest_config("ethereum"), &held) {
            panic!("an alias of a dataset already on the cursor is not a second nest: {e:#}");
        }
    }

    /// A `max_rows` large enough to overflow the multiplication must not wrap into a small
    /// projection and be admitted. Saturating is the safe direction: an absurd declaration projects
    /// as absurd and is refused, rather than as nothing and let in.
    #[test]
    fn an_absurd_max_rows_saturates_rather_than_wrapping_into_admission() {
        fn cfg(extra: &str) -> Config {
            let toml = format!(
                "[nest]\nname = \"n\"\nchain = \"c\"\nchain_id = 1\nrpc_urls = []\n\n\
                 [[contracts]]\nalias = \"t\"\naddress = \"0x1\"\nabi = \"a.json\"\n{extra}"
            );
            toml::from_str(&toml).expect("fixture config")
        }

        // Not `u64::MAX`: that wraps to a number still large enough to be refused, so it passes
        // whether the multiplication saturates or not. This is the smallest row count whose product
        // exceeds `u64::MAX` - so wrapping leaves a remainder below the per-row constant itself,
        // which rounds to **0 MB** and would be admitted free.
        //
        // Derived rather than written down: the first version hardcoded a value tuned to a
        // `ENTITY_RSS_BYTES_PER_ROW` of 2,500 and broke the moment that constant was replaced by a
        // measurement. A test that only works for one value of the thing it is testing is a trap
        // laid for whoever changes it next.
        let rows = u64::MAX / ENTITY_RSS_BYTES_PER_ROW + 1;
        assert_eq!(
            rows.wrapping_mul(ENTITY_RSS_BYTES_PER_ROW) / (1024 * 1024),
            0,
            "the premise: wrapping this one projects as nothing at all"
        );
        let projected = estimate_nest_rss_mb(&cfg(""), false, 1, rows);
        assert!(
            projected > DEFAULT_MAX_RSS_MB,
            "an absurd max_rows must project past any budget, not wrap to nothing: {projected} MB"
        );
    }

    /// NEST_BASE_RSS_MB > 0 is the only thing that makes this refusal meaningful. A 400-nest cursor
    /// must project above the 2 GB default ceiling; if the per-nest constant is zeroed out the
    /// estimate collapses to the 120 MB base and the gate lets it through.
    #[test]
    fn a_cursor_with_too_many_nests_trips_the_budget() {
        let cfg: Config = toml::from_str(
            "[nest]\nname = \"n\"\nchain = \"c\"\nchain_id = 1\nrpc_urls = []\n\
             \n[[contracts]]\nalias = \"t\"\naddress = \"0x1\"\nabi = \"a.json\"\n",
        )
        .unwrap();
        let per_nest = estimate_nest_rss_mb(&cfg, false, 0, 0);
        let cursor_mb = RUNTIME_BASE_RSS_MB + 400 * per_nest;
        assert!(
            cursor_mb > DEFAULT_MAX_RSS_MB,
            "400-nest cursor projects {cursor_mb} MB, expected > {DEFAULT_MAX_RSS_MB} MB"
        );
    }

    /// Issue #147, the headline scenario and the acceptance test for RFC-0026: one chain's cursor dies
    /// (a reorg below its sealed watermark), and the other chain's cursor must carry on indexing with
    /// the process still up. Before this, `select_all` returned on the first cursor's death, aborted
    /// every sibling, and exited - so a Base reorg took down a perfectly healthy Arbitrum cursor.
    #[tokio::test]
    async fn a_dead_cursor_does_not_take_its_siblings_down() {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::time::Duration;

        let doomed = tokio::spawn(async {
            Err::<(), anyhow::Error>(anyhow::anyhow!(
                "reorg to block 100 is below the sealed/finalized watermark 200 - a finality \
                 violation this indexer cannot repair"
            ))
        });
        // A cursor that keeps working, ticking a counter so "still indexing" is observable rather
        // than merely "not yet finished".
        let progress = Arc::new(AtomicU64::new(0));
        let p = progress.clone();
        let healthy = tokio::spawn(async move {
            for _ in 0..10_000 {
                p.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Ok(())
        });

        let mut ingests = vec![
            ("base".to_string(), doomed),
            ("arbitrum-one".to_string(), healthy),
        ];
        // The supervisor must NOT return while a healthy cursor is still indexing: returning is what
        // ends the runtime.
        let health = crate::health::RuntimeHealth::new();
        health.register("nest-a", "base");
        health.register("nest-b", "arbitrum-one");
        let (_feed, mut intake) = tokio::sync::mpsc::unbounded_channel();
        let returned = tokio::time::timeout(
            Duration::from_millis(250),
            supervise_cursors(&mut ingests, &mut intake, &health, false),
        )
        .await;
        assert!(
            returned.is_err(),
            "the runtime ended even though a healthy cursor was still indexing"
        );

        // The dead cursor was retired from the set; the healthy one is untouched and still working.
        assert_eq!(ingests.len(), 1, "only the dead cursor should be retired");
        assert_eq!(ingests[0].0, "arbitrum-one");
        assert!(
            !ingests[0].1.is_finished(),
            "the surviving cursor must not have been aborted"
        );
        assert!(
            progress.load(Ordering::Relaxed) > 0,
            "the surviving cursor must keep making progress after its sibling died"
        );
        // The health surface tells the truth about both: the dead chain's nest is quarantined, the
        // living chain's is not (RFC-0026 §5).
        assert_eq!(health.json_for("nest-a").0, "quarantined");
        assert_eq!(health.json_for("nest-b").0, "indexing");
        assert!(
            !health.all_indexing(),
            "a partly-broken mounts is not ready"
        );
        ingests[0].1.abort();
    }

    /// RFC-0026 §6: the runtime exits only once **every** cursor is gone - at that point nothing will
    /// ever advance again, so exiting non-zero under a supervisor beats serving permanently-frozen
    /// data. The error must name every dead chain, since that is the operator's starting point.
    #[tokio::test]
    async fn the_roost_exits_when_the_last_cursor_dies_and_names_every_chain() {
        let a =
            tokio::spawn(async { Err::<(), anyhow::Error>(anyhow::anyhow!("finality violation")) });
        let b = tokio::spawn(async {
            Err::<(), anyhow::Error>(anyhow::anyhow!("a single block exceeds the response cap"))
        });
        let mut ingests = vec![("base".to_string(), a), ("arbitrum-one".to_string(), b)];

        let health = crate::health::RuntimeHealth::new();
        // Open, so a mount could still add a cursor: all-dead ends the runtime regardless.
        let (_feed, mut intake) = tokio::sync::mpsc::unbounded_channel();
        let err = supervise_cursors(&mut ingests, &mut intake, &health, false)
            .await
            .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("base"),
            "should name the first dead chain: {msg}"
        );
        assert!(
            msg.contains("arbitrum-one"),
            "should name the second dead chain: {msg}"
        );
        assert!(ingests.is_empty(), "every cursor should have been retired");
    }

    /// #1545: a runtime with nothing mounted is waiting for its first mount, not finished. It stays
    /// up, and a cursor a mount spawns later is supervised like one started at boot.
    #[tokio::test]
    async fn an_empty_runtime_waits_and_supervises_a_cursor_spawned_later() {
        let health = crate::health::RuntimeHealth::new();
        let (feed, mut intake) = tokio::sync::mpsc::unbounded_channel();
        let mut ingests = Vec::new();
        let mut supervisor = Box::pin(supervise_cursors(&mut ingests, &mut intake, &health, false));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(200), &mut supervisor)
                .await
                .is_err(),
            "an empty runtime ended before anything could be mounted"
        );

        let dies =
            tokio::spawn(async { Err::<(), anyhow::Error>(anyhow::anyhow!("finality violation")) });
        feed.send(("base".to_string(), dies)).unwrap();
        let err = tokio::time::timeout(std::time::Duration::from_secs(5), supervisor)
            .await
            .expect("the spawned cursor was never supervised")
            .unwrap_err();
        assert!(format!("{err:#}").contains("base"), "{err:#}");
        assert!(health.cursor_quarantined("base"));
    }

    /// A cursor that finishes cleanly leaves the runtime up while a mount can still add one, and
    /// ends it once nothing can.
    #[tokio::test]
    async fn a_clean_finish_ends_the_runtime_only_once_the_intake_closes() {
        let health = crate::health::RuntimeHealth::new();
        let (feed, mut intake) = tokio::sync::mpsc::unbounded_channel();
        let mut ingests = vec![("base".to_string(), tokio::spawn(async { Ok(()) }))];
        let mut supervisor = Box::pin(supervise_cursors(&mut ingests, &mut intake, &health, false));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(200), &mut supervisor)
                .await
                .is_err(),
            "a clean finish ended the runtime while the admin API could still mount"
        );
        drop(feed);
        tokio::time::timeout(std::time::Duration::from_secs(5), supervisor)
            .await
            .expect("a closed intake with nothing left must end the runtime")
            .expect("a clean finish is not an error");
    }

    /// A cursor that answers renames in turn from `answers`, and reports what it was asked.
    fn scripted_cursor(answers: Vec<bool>) -> (CursorTx, tokio::task::JoinHandle<Vec<String>>) {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let seen = tokio::spawn(async move {
            let mut answers = answers.into_iter();
            let mut seen = Vec::new();
            while let Some(cmd) = rx.recv().await {
                let indexer::CursorCommand::Rename { from, to, ack } = cmd else {
                    panic!("not a rename: {cmd:?}");
                };
                seen.push(format!("{from}->{to}"));
                ack.unwrap()
                    .send(answers.next().expect("an unscripted rename"))
                    .unwrap();
            }
            seen
        });
        (tx, seen)
    }

    #[tokio::test]
    async fn a_refused_incoming_rename_gives_the_holder_key_back() {
        let (tx, seen) = scripted_cursor(vec![true, false, true]);
        let moved = swap_cursor_keys(&tx, "c", "usdc", Some("usdc__moving"), Some("other")).await;
        drop(tx);
        assert!(moved.is_err());
        assert_eq!(
            seen.await.unwrap(),
            ["usdc->other", "usdc__moving->usdc", "other->usdc"]
        );
    }

    #[tokio::test]
    async fn a_refused_holder_rename_stops_the_swap() {
        let (tx, seen) = scripted_cursor(vec![false]);
        let moved = swap_cursor_keys(&tx, "c", "usdc", Some("usdc__moving"), Some("other")).await;
        drop(tx);
        assert!(moved.is_err());
        assert_eq!(seen.await.unwrap(), ["usdc->other"]);
    }

    /// #1640: boot refuses these, and a live mount of one takes the runtime down at the next start.
    #[test]
    fn a_mount_named_health_or_nests_is_refused() {
        for name in [
            "health",
            "nests",
            "health/usdc",
            "nests/usdc",
            "acme/health",
            "acme/nests",
        ] {
            let err = check_mount_name(name, DEFAULT_TENANT)
                .unwrap_err()
                .to_string();
            assert!(err.contains("reserved"), "{name}: {err}");
        }
        check_mount_name("usdc", DEFAULT_TENANT).unwrap();
        check_mount_name("acme/usdc", DEFAULT_TENANT).unwrap();

        let dir = tempfile::tempdir().unwrap();
        let handles = idle_handles(dir.path());
        for name in ["health", "nests", "acme/health", "health/usdc"] {
            let err = handles.plan_mount(name, None).unwrap_err().to_string();
            assert!(err.contains("reserved"), "plan_mount {name}: {err}");
        }
        assert!(handles.plan_mount("usdc", None).is_ok());
    }

    fn nest_state(dir: &std::path::Path, chain: &str) -> crate::serve::AppState {
        crate::serve::AppState {
            store: Arc::new(crate::store::Store::open(&dir.join("hot.redb")).unwrap()),
            address: None,
            chain: chain.to_string(),
            dir: dir.to_path_buf(),
            balances: crate::views::BalanceView::start().unwrap(),
            exposure: crate::exposure::ExposureView::start(true).unwrap(),
            velocity: crate::velocity::VelocityView::start(true).unwrap(),
            entities: Arc::new(Vec::new()),
            #[cfg(feature = "folds")]
            folds: None,
            threshold: None,
            velocity_threshold: None,
            admin_enabled: false,
            admin_token: None,
            nest_info: Arc::new(serde_json::json!({})),
            tables: Arc::new(vec![]),
            sql_gate: Arc::new(tokio::sync::Semaphore::new(1)),
            sql_queued: Default::default(),
            sql_max_hot_rows: crate::serve::SQL_MAX_HOT_ROWS,
            sql_max_hot_bytes: crate::serve::SQL_MAX_HOT_BYTES,
            sql_max_named_scan_bytes: crate::serve::SQL_MAX_NAMED_SCAN_BYTES,
            cursorless: true,
            seal_span: crate::chains::DEFAULT_SEAL_SPAN,
            freshness: Default::default(),
            surface: Arc::new(crate::allowlist::Surface::default()),
            #[cfg(feature = "counter")]
            counter: None,
            nid: None,
            runtime_health: None,
            address_history: None,
        }
    }

    /// #2129, #2135: boot serves each name its record's surface, bounded by its author's ceiling,
    /// and an open record under a ceiling serves the ceiling.
    #[test]
    fn boot_serves_each_record_within_its_ceiling() {
        use crate::allowlist::SqlAccess;
        let (closed, bounded) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        std::fs::write(
            bounded.path().join(crate::allowlist::CEILING_FILE),
            "[[queries]]\nname = \"total\"\nsql = \"SELECT 1\"\n",
        )
        .unwrap();
        let record = |alias: &str, sql| Mount {
            tenant: DEFAULT_TENANT.to_string(),
            alias: alias.to_string(),
            nid: "ab".repeat(32),
            sql,
            queries: Vec::new(),
            publish: None,
            #[cfg(feature = "counter")]
            counter: None,
        };
        let mut states = vec![
            (
                "closed".to_string(),
                nest_state(closed.path(), "arbitrum-one"),
            ),
            (
                "bounded".to_string(),
                nest_state(bounded.path(), "arbitrum-one"),
            ),
        ];
        let records = [
            record("closed", SqlAccess::Deny),
            record("bounded", SqlAccess::Open),
        ];
        overlay_boot_records(&mut states, &records, &[], DEFAULT_TENANT).unwrap();
        assert_eq!(states[0].1.surface.access, SqlAccess::Deny);
        assert_eq!(states[1].1.surface.access, SqlAccess::Allowlist);
        assert_eq!(states[1].1.surface.queries[0].name, "total");
    }

    /// #1647: every write of mounts.toml a suspend makes still holds the mount, so a crash at any
    /// point leaves it live or suspended, never gone.
    #[tokio::test]
    async fn a_suspend_never_writes_a_table_without_the_mount() {
        let dir = tempfile::tempdir().unwrap();
        let nid = "8a".repeat(32);
        std::fs::write(
            dir.path().join(MOUNTS_FILE),
            format!(
                "[runtime]\nname = \"r\"\n\n[[chains]]\nchain = \"arbitrum-one\"\n\
                 chain_id = 42161\nrpc_urls = []\n\n[[mounts]]\nalias = \"usdc\"\nnid = \"{nid}\"\n"
            ),
        )
        .unwrap();
        let data = MountTable::data_dir(dir.path(), &nid);
        std::fs::create_dir_all(&data).unwrap();
        let mut state = nest_state(&data, "arbitrum-one");
        state.nid = Some(Arc::from(nid.as_str()));
        let mut handles = idle_handles(dir.path());
        handles.states.push(("usdc".to_string(), state));
        handles.mount_ctx.mounts = MountTable::load(dir.path()).unwrap().mounts;
        handles
            .suspend("usdc")
            .await
            .expect_err("premise: with no cursor channel the drain refuses");
        assert!(
            handles.suspended.is_empty(),
            "a suspend whose drain refused left the mount both live and suspended"
        );
        // A cursor that has already stopped: the drain finds it gone and goes on.
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        drop(rx);
        handles.lifecycle.insert("arbitrum-one".to_string(), tx);

        handles.suspend("usdc").await.expect("suspend");
        let writes: Vec<String> = super::persisted_tables()
            .lock()
            .unwrap()
            .iter()
            .filter(|(d, _)| d == dir.path())
            .map(|(_, t)| t.clone())
            .collect();
        assert!(!writes.is_empty(), "premise: the suspend was written");
        for table in &writes {
            let table: MountTable = toml::from_str(table).unwrap();
            assert!(
                table
                    .mounts
                    .iter()
                    .any(|m| m.alias == "usdc" && m.nid == nid)
                    && table.runtime.suspended == ["usdc"],
                "a crash here boots with usdc neither live nor suspended: {table:?}"
            );
        }
        assert_eq!(writes.len(), 1, "one suspend is one write");
    }

    /// #1674: a reclaim that lands between a mount job's fetch and its join must not delete the
    /// dataset the job is about to join.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_reclaim_keeps_the_dataset_a_mount_job_is_about_to_join() {
        use crate::mount_jobs::{MountJob, MountPhase};
        use tower::ServiceExt;

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(MOUNTS_FILE),
            "[runtime]\nname = \"r\"\n\n[[chains]]\nchain = \"arbitrum-one\"\n\
             chain_id = 42161\nrpc_urls = []\n",
        )
        .unwrap();
        let nid = "8b".repeat(32);
        let data = MountTable::data_dir(dir.path(), &nid);
        std::fs::create_dir_all(&data).unwrap();
        std::fs::write(data.join(CONFIG_FILE), "fetched").unwrap();
        let jobs = Arc::new(crate::mount_jobs::MountJobs::load(dir.path()));
        let handles = Arc::new(tokio::sync::Mutex::new(idle_handles(dir.path())));
        let routes = lifecycle_routes(handles.clone(), jobs.clone(), true, None);
        let claimed = jobs
            .claim(MountJob::new("joining", Some(&nid), MountPhase::Accepted))
            .unwrap();

        let answer = Arc::new(std::sync::Mutex::new(None));
        let (answer_hook, jobs_hook, uri) = (
            answer.clone(),
            jobs.clone(),
            format!("/_admin/datasets/{nid}"),
        );
        super::before_mount_join().lock().unwrap().insert(
            (dir.path().to_path_buf(), "joining".to_string()),
            Arc::new(move || {
                let req = axum::http::Request::delete(&uri)
                    .body(axum::body::Body::empty())
                    .unwrap();
                let resp = tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(routes.clone().oneshot(req))
                })
                .unwrap();
                *answer_hook.lock().unwrap() = Some(resp.status());
                // Stop the job before it joins: what is under test is the reclaim's answer.
                jobs_hook.forget("joining").unwrap();
            }),
        );
        struct Clear(std::path::PathBuf);
        impl Drop for Clear {
            fn drop(&mut self) {
                super::before_mount_join()
                    .lock()
                    .unwrap()
                    .remove(&(self.0.clone(), "joining".to_string()));
            }
        }
        let _clear = Clear(dir.path().to_path_buf());
        spawn_mount_job(
            handles.clone(),
            jobs.clone(),
            "joining".into(),
            Some(Nid::parse(&nid).unwrap()),
            claimed.generation,
        )
        .await
        .unwrap();

        assert_eq!(
            *answer.lock().unwrap(),
            Some(axum::http::StatusCode::CONFLICT),
            "a reclaim during the job was not answered kept"
        );
        assert!(
            data.join(CONFIG_FILE).exists(),
            "the reclaim deleted the dataset a mount job was about to join"
        );

        // A job that has finished holds nothing.
        let mut failed = MountJob::new("joining", Some(&nid), MountPhase::Failed);
        failed.reason = Some("refused".into());
        jobs.put(failed).unwrap();
        let req = axum::http::Request::delete(format!("/_admin/datasets/{nid}"))
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = lifecycle_routes(handles, jobs, true, None)
            .oneshot(req)
            .await
            .unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
        assert!(!data.exists(), "a failed job kept its dataset from reclaim");
    }

    fn idle_handles(dir: &std::path::Path) -> RuntimeHandles {
        let health = std::sync::Arc::new(crate::health::RuntimeHealth::new());
        let roster = serde_json::json!({"runtime": "t", "nests": []});
        let live = crate::serve::LiveRuntime::new(crate::serve::compose_runtime(
            roster.clone(),
            Vec::new(),
            health.clone(),
        ));
        RuntimeHandles {
            live,
            states: Vec::new(),
            alert_workers: Vec::new(),
            publishers: Vec::new(),
            lifecycle: std::collections::HashMap::new(),
            health,
            roster,
            estimates: std::collections::HashMap::new(),
            default_tenant: DEFAULT_TENANT.to_string(),
            suspended: std::collections::BTreeMap::new(),
            joining: Default::default(),
            mount_ctx: MountContext {
                dir: dir.to_path_buf(),
                mounts: Vec::new(),
                sources: std::collections::HashMap::new(),
                endpoint_counts: std::collections::HashMap::new(),
                backfill: None,
                seal_direct: false,
                concurrency: 1,
                ipfs_window_deadline: crate::ipfs_resolve::WINDOW_DEADLINE,
                window_override: None,
                admin_enabled: true,
                admin_token: None,
                max_rss_mb: DEFAULT_MAX_RSS_MB,
                freshness: Default::default(),
                chain_freshness: std::collections::HashMap::new(),
                dormant: std::collections::HashMap::new(),
                fail_fast: false,
                cursors: None,
                registry: None,
            },
        }
    }

    /// #1638: forgetting the job in the gap before the join must stop the mount.
    #[tokio::test]
    async fn an_unmount_of_an_accepted_job_is_not_overtaken_by_it() {
        use crate::mount_jobs::{MountJob, MountPhase};
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(MOUNTS_FILE),
            "[runtime]\nname = \"r\"\nchain = \"arbitrum-one\"\nchain_id = 42161\n\
             rpc_urls = []\n",
        )
        .unwrap();
        let jobs = Arc::new(crate::mount_jobs::MountJobs::load(dir.path()));
        let claimed = jobs
            .claim(MountJob::new("usdc", None, MountPhase::Accepted))
            .unwrap();
        let jobs_hook = jobs.clone();
        let hook_key = (dir.path().to_path_buf(), "usdc".to_string());
        super::before_mount_join().lock().unwrap().insert(
            hook_key.clone(),
            Arc::new(move || {
                jobs_hook.forget("usdc").unwrap();
                jobs_hook
                    .claim(MountJob::new("usdc", None, MountPhase::Accepted))
                    .expect("the name is free once the first job is forgotten");
            }),
        );
        struct Clear((std::path::PathBuf, String));
        impl Drop for Clear {
            fn drop(&mut self) {
                super::before_mount_join().lock().unwrap().remove(&self.0);
            }
        }
        let _clear = Clear(hook_key.clone());
        let handles = Arc::new(tokio::sync::Mutex::new(idle_handles(dir.path())));
        spawn_mount_job(
            handles,
            jobs.clone(),
            "usdc".into(),
            None,
            claimed.generation,
        )
        .await
        .unwrap();
        assert_eq!(
            super::mount_entries_in(dir.path()),
            0,
            "the forgotten job must not mount"
        );
        let again = jobs.get("usdc").expect("the replacement claim stays");
        assert_eq!(again.phase, MountPhase::Accepted);

        // The generation is the one claim returned. A worker that reads the map adopts this
        // replacement and mounts it.
        super::before_mount_join().lock().unwrap().remove(&hook_key);
        let claimed = jobs
            .claim(MountJob::new("dai", None, MountPhase::Accepted))
            .unwrap();
        jobs.forget("dai").unwrap();
        let replacement = jobs
            .claim(MountJob::new("dai", None, MountPhase::Accepted))
            .expect("the name is free once the first job is forgotten");
        let handles = Arc::new(tokio::sync::Mutex::new(idle_handles(dir.path())));
        spawn_mount_job(
            handles,
            jobs.clone(),
            "dai".into(),
            None,
            claimed.generation,
        )
        .await
        .unwrap();
        assert_eq!(
            super::mount_entries_in(dir.path()),
            0,
            "the replaced claim must not mount"
        );
        let again = jobs.get("dai").expect("the replacement claim stays");
        assert_eq!(again.phase, MountPhase::Accepted);
        assert_eq!(again.generation, replacement.generation);

        // A mount on this runtime does move its count, so the zeros above mean something.
        let _ = idle_handles(dir.path()).mount("dai", None).await;
        assert_eq!(super::mount_entries_in(dir.path()), 1);
    }

    /// #1638, review: a move worker is fenced as a mount worker is. One whose claim was forgotten
    /// and replaced must leave the replacement's job alone, whatever its own outcome.
    #[tokio::test]
    async fn a_forgotten_move_job_leaves_the_replacement_claim_alone() {
        use crate::mount_jobs::{MountJob, MountPhase};
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(MOUNTS_FILE),
            "[runtime]\nname = \"r\"\nchain = \"arbitrum-one\"\nchain_id = 42161\n\
             rpc_urls = []\n",
        )
        .unwrap();
        let jobs = Arc::new(crate::mount_jobs::MountJobs::load(dir.path()));
        let nid = "7c".repeat(32);
        let mut first = MountJob::new("usdc", Some(&nid), MountPhase::Accepted);
        first.is_move = true;
        let claimed = jobs.claim(first).unwrap();
        jobs.forget("usdc").unwrap();
        let replacement = jobs
            .claim(MountJob::new("usdc", None, MountPhase::Accepted))
            .expect("the name is free once the first job is forgotten");
        assert_ne!(claimed.generation, replacement.generation);
        let handles = Arc::new(tokio::sync::Mutex::new(idle_handles(dir.path())));
        spawn_move_job(
            handles,
            jobs.clone(),
            "usdc".into(),
            Nid::parse(&nid).unwrap(),
            claimed.generation,
        )
        .await
        .unwrap();
        let after = jobs.get("usdc").expect("the replacement claim stays");
        assert_eq!(after.generation, replacement.generation);
        assert_eq!(
            (after.phase, after.reason),
            (MountPhase::Accepted, None),
            "the old move worker wrote its outcome onto a claim that is not its own"
        );
    }

    /// #2221: a `?wait=true` mount of a name an async job is still mounting replaces that job's
    /// claim. The job, fenced by its generation, stands down rather than mounting the name twice.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_wait_mount_over_a_running_job_mounts_once() {
        use crate::mount_jobs::{MountJob, MountPhase};
        use tower::ServiceExt;

        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join(MOUNTS_FILE),
            "[runtime]\nname = \"r\"\nchain = \"ethereum\"\nchain_id = 1\nrpc_urls = []\n",
        )
        .unwrap();
        write_nest_dir(root.path(), "usdc", "ethereum", 1);
        let dir = MountTable::nest_dir(root.path(), "usdc");
        crate::store::Store::open(&dir.join(crate::config::DB_FILE))
            .unwrap()
            .set_meta("last_block", "5")
            .unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let admitted = Arc::new(std::sync::Mutex::new(Vec::new()));
        let cursor = tokio::spawn({
            let admitted = admitted.clone();
            async move {
                while let Some(cmd) = rx.recv().await {
                    if let indexer::CursorCommand::Mount { nest, ack, .. } = cmd {
                        admitted.lock().unwrap().push(nest);
                        let _ = ack.expect("a live mount waits for its ack").send(());
                    }
                }
            }
        });
        let mut idle = idle_handles(root.path());
        let source: Arc<dyn Source> = Arc::new(crate::source::UnpolledSource);
        idle.mount_ctx
            .sources
            .insert("ethereum".to_string(), source);
        idle.lifecycle.insert("ethereum".to_string(), tx);
        let handles = Arc::new(tokio::sync::Mutex::new(idle));
        let jobs = Arc::new(crate::mount_jobs::MountJobs::load(root.path()));
        let claimed = jobs
            .claim(MountJob::new("usdc", None, MountPhase::Accepted))
            .unwrap();

        let routes = lifecycle_routes(handles.clone(), jobs.clone(), true, None);
        let answer = Arc::new(std::sync::Mutex::new(None));
        let answer_hook = answer.clone();
        let hook_key = (root.path().to_path_buf(), "usdc".to_string());
        super::before_mount_join().lock().unwrap().insert(
            hook_key.clone(),
            Arc::new(move || {
                let req = axum::http::Request::post("/_admin/nests?wait=true")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(r#"{"name":"usdc"}"#))
                    .unwrap();
                let resp = tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(routes.clone().oneshot(req))
                })
                .unwrap();
                *answer_hook.lock().unwrap() = Some(resp.status());
            }),
        );
        struct Clear((std::path::PathBuf, String));
        impl Drop for Clear {
            fn drop(&mut self) {
                super::before_mount_join().lock().unwrap().remove(&self.0);
            }
        }
        let _clear = Clear(hook_key);
        spawn_mount_job(
            handles.clone(),
            jobs.clone(),
            "usdc".into(),
            None,
            claimed.generation,
        )
        .await
        .unwrap();

        assert_eq!(
            *answer.lock().unwrap(),
            Some(axum::http::StatusCode::OK),
            "premise: the waited mount landed while the job was between fetch and join"
        );
        assert_eq!(
            super::mount_entries_in(root.path()),
            1,
            "the job mounted a name the waited mount already holds"
        );
        let mut h = handles.lock().await;
        assert_eq!(h.states.iter().filter(|(n, _)| n == "usdc").count(), 1);
        assert_eq!(jobs.get("usdc").unwrap().phase, MountPhase::Live);
        for (_, w) in std::mem::take(&mut h.alert_workers) {
            w.abort();
        }
        drop(h);
        cursor.abort();
        assert_eq!(
            admitted.lock().unwrap().len(),
            1,
            "one nest reached the cursor"
        );
    }

    /// #2233: a commit whose second rename fails takes the first back out, so the destination is
    /// inputs-only and adoptable again rather than a store with no segments beside it.
    #[test]
    fn a_commit_that_fails_part_way_leaves_the_destination_inputs_only() {
        let d = tempfile::tempdir().unwrap();
        let (staging, dataset) = (d.path().join("staging"), d.path().join("dataset"));
        std::fs::create_dir_all(staging.join("segments")).unwrap();
        std::fs::write(staging.join(crate::config::DB_FILE), b"store").unwrap();
        std::fs::write(staging.join("segments").join("manifest.json"), b"{}").unwrap();
        // A non-empty directory in its place makes the second rename fail.
        std::fs::create_dir_all(dataset.join("segments")).unwrap();
        std::fs::write(dataset.join("segments").join("in-the-way"), b"").unwrap();
        std::fs::write(dataset.join(CONFIG_FILE), b"inputs").unwrap();

        let err = commit_derived_state(&staging, &dataset, crate::blob::DERIVED_STATE)
            .expect_err("premise: the segments rename fails");
        assert!(
            format!("{err:#}").contains("committing segments"),
            "{err:#}"
        );
        assert!(
            !dataset.join(crate::config::DB_FILE).exists(),
            "the store landed without its segments: a half-adopted dataset"
        );
        assert!(dataset.join(CONFIG_FILE).exists());
        assert!(
            dataset.join("segments").join("in-the-way").exists(),
            "the unwind removed a directory this commit never moved"
        );
    }

    /// #1768: a refused mount returns only once its store is closed. The alert worker it aborts may
    /// have a delivery on the blocking pool, holding a store clone, and a retry would hit the lock.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_mount_refused_mid_delivery_can_be_retried_at_once() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join(MOUNTS_FILE),
            "[runtime]\nname = \"r\"\nchain = \"ethereum\"\nchain_id = 1\nrpc_urls = []\n",
        )
        .unwrap();
        write_nest_dir(root.path(), "usdc", "ethereum", 1);
        let dir = MountTable::nest_dir(root.path(), "usdc");
        let config = std::fs::read_to_string(dir.join(CONFIG_FILE)).unwrap();
        std::fs::write(
            dir.join(CONFIG_FILE),
            config + "\n[[alerts]]\nkinds = [\"sanction_hit\"]\nurl = \"http://127.0.0.1:9/\"\n",
        )
        .unwrap();
        let db = dir.join(crate::config::DB_FILE);
        {
            // Indexed already, so preparing asks the source nothing; one undeliverable entry, which
            // the worker removes without a network call.
            let store = crate::store::Store::open(&db).unwrap();
            store.set_meta("last_block", "5").unwrap();
            store.outbox_push("not json").unwrap();
        }

        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        crate::store::outbox_holds()
            .lock()
            .unwrap()
            .insert(db.clone(), (entered_tx, release_rx));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let cursor = tokio::spawn(async move {
            let Some(indexer::CursorCommand::Mount { nest, ack, .. }) = rx.recv().await else {
                panic!("the first mount reaches the cursor");
            };
            tokio::task::spawn_blocking(move || entered_rx.recv())
                .await
                .unwrap()
                .expect("the delivery parks on the blocking pool");
            drop((nest, ack));
            std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(500));
                let _ = release_tx.send(());
            });
            let Some(indexer::CursorCommand::Mount { ack, .. }) = rx.recv().await else {
                panic!("the retry reaches the cursor");
            };
            let _ = ack.expect("a live mount waits for its ack").send(());
        });

        let mut handles = idle_handles(root.path());
        let source: Arc<dyn Source> = Arc::new(crate::source::UnpolledSource);
        handles
            .mount_ctx
            .sources
            .insert("ethereum".to_string(), source);
        handles.lifecycle.insert("ethereum".to_string(), tx);

        let err = handles
            .mount_dataset("usdc", None, "usdc", None, dir.clone())
            .await
            .expect_err("the cursor refused it");
        assert!(
            format!("{err:#}").contains("stopped while mounting"),
            "{err:#}"
        );
        handles
            .mount_dataset("usdc", None, "usdc", None, dir.clone())
            .await
            .expect("a retry straight after the refusal opens the store");
        cursor.await.unwrap();
        for (_, w) in std::mem::take(&mut handles.alert_workers) {
            w.abort();
            let _ = w.await;
        }
    }

    /// #1768: a mount that fails to catch up stops its alert worker too. It used to detach it, and
    /// the worker kept the store open until the process exited.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_mount_that_fails_to_prepare_leaves_its_store_closed() {
        let root = tempfile::tempdir().unwrap();
        write_nest_dir(root.path(), "usdc", "ethereum", 1);
        let dir = MountTable::nest_dir(root.path(), "usdc");
        let config = std::fs::read_to_string(dir.join(CONFIG_FILE)).unwrap();
        std::fs::write(
            dir.join(CONFIG_FILE),
            config + "\n[[alerts]]\nkinds = [\"sanction_hit\"]\nurl = \"http://127.0.0.1:9/\"\n",
        )
        .unwrap();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let mut handles = idle_handles(root.path());
        // Never indexed, so preparing asks for the tip, which this source refuses.
        let source: Arc<dyn Source> = Arc::new(crate::source::UnpolledSource);
        handles
            .mount_ctx
            .sources
            .insert("ethereum".to_string(), source);
        handles.lifecycle.insert("ethereum".to_string(), tx);

        let err = handles
            .mount_dataset("usdc", None, "usdc", None, dir.clone())
            .await
            .expect_err("a cold start with no tip cannot be prepared");
        assert!(format!("{err:#}").contains("tip lookup failed"), "{err:#}");
        crate::store::Store::open(&dir.join(crate::config::DB_FILE))
            .expect("the refused mount let go of its store");
    }

    /// A move of `usdc` from one NID to another, against a cursor that admits every nest and
    /// acknowledges every unmount but those of the names in `stuck`, whose acknowledgements it holds
    /// until the driver gives up. Returns the move's error and the names the cursor drained.
    async fn move_against_a_stuck_cursor(
        root: &Path,
        stuck: &'static [&'static str],
    ) -> (
        RuntimeHandles,
        tokio::task::JoinHandle<Vec<String>>,
        anyhow::Error,
    ) {
        std::fs::write(
            root.join(MOUNTS_FILE),
            "[runtime]\nname = \"r\"\nchain = \"ethereum\"\nchain_id = 1\nrpc_urls = []\n",
        )
        .unwrap();
        for nid in [OLD_NID, NEW_NID].map(|b| b.repeat(32)) {
            let dir = MountTable::data_dir(root, &nid);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(
                dir.join(CONFIG_FILE),
                "[nest]\nname = \"usdc\"\nchain = \"ethereum\"\nchain_id = 1\nrpc_urls = []\n\n\
                 [[contracts]]\nalias = \"t\"\naddress = \"0x0000000000000000000000000000000000000001\"\nabi = \"abi.json\"\n",
            )
            .unwrap();
            std::fs::write(dir.join("abi.json"), "[]").unwrap();
            // Indexed already, so preparing asks the source nothing.
            let store = crate::store::Store::open(&dir.join(crate::config::DB_FILE)).unwrap();
            store.set_meta("last_block", "5").unwrap();
        }

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let cursor = tokio::spawn(async move {
            let mut held = Vec::new();
            let mut drained = Vec::new();
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    indexer::CursorCommand::Mount { nest, ack, .. } => {
                        drop(nest);
                        if let Some(ack) = ack {
                            let _ = ack.send(());
                        }
                    }
                    indexer::CursorCommand::Unmount { name, ack }
                        if stuck.contains(&name.as_str()) =>
                    {
                        held.push(ack)
                    }
                    indexer::CursorCommand::Unmount { name, ack } => {
                        drained.push(name);
                        if let Some(ack) = ack {
                            let _ = ack.send(());
                        }
                    }
                    indexer::CursorCommand::Rename { .. } => panic!("a failed drain renamed"),
                }
            }
            drained
        });

        let mut handles = idle_handles(root);
        let source: Arc<dyn Source> = Arc::new(crate::source::UnpolledSource);
        handles
            .mount_ctx
            .sources
            .insert("ethereum".to_string(), source);
        handles.lifecycle.insert("ethereum".to_string(), tx);
        handles
            .mount("usdc", Some(Nid::parse(&OLD_NID.repeat(32)).unwrap()))
            .await
            .expect("mount the old nest");
        let err = handles
            .move_name("usdc", Nid::parse(&NEW_NID.repeat(32)).unwrap())
            .await
            .expect_err("the old nest never came off the cursor");
        (handles, cursor, err)
    }

    const OLD_NID: &str = "a1";
    const NEW_NID: &str = "b2";

    fn recorded(handles: &RuntimeHandles) -> Vec<(String, String)> {
        handles
            .mount_ctx
            .mounts
            .iter()
            .map(|m| (mount_route(m, &handles.default_tenant), m.nid.clone()))
            .collect()
    }

    async fn stop(
        mut handles: RuntimeHandles,
        cursor: tokio::task::JoinHandle<Vec<String>>,
    ) -> Vec<String> {
        handles.lifecycle.clear();
        for (_, w) in std::mem::take(&mut handles.alert_workers) {
            w.abort();
            let _ = w.await;
        }
        drop(handles);
        cursor.await.unwrap()
    }

    /// #1777: a move whose old nest will not come off the cursor backs its staged mount out. It used
    /// to leave the staging name mounted, routed and recorded beside the old nest.
    #[tokio::test(start_paused = true)]
    async fn a_move_whose_drain_fails_backs_out_its_staged_mount() {
        let root = tempfile::tempdir().unwrap();
        let (handles, cursor, err) = move_against_a_stuck_cursor(root.path(), &["usdc"]).await;
        assert!(
            format!("{err:#}").contains("did not acknowledge"),
            "{err:#}"
        );

        let staging = RuntimeHandles::staging_name("usdc");
        let mounted: Vec<_> = handles.states.iter().map(|(n, _)| n.clone()).collect();
        assert_eq!(mounted, ["usdc"], "the staged mount stayed mounted");
        assert_eq!(
            recorded(&handles),
            [("usdc".to_string(), OLD_NID.repeat(32))],
            "the staged mount stayed recorded"
        );
        assert_eq!(
            handles.states[0].1.nid.as_deref(),
            Some(OLD_NID.repeat(32).as_str()),
            "the name no longer serves the old nest"
        );
        assert!(
            !handles.health.render_metrics().contains(&staging),
            "the staged mount is still reported"
        );
        crate::store::Store::open(
            &MountTable::data_dir(root.path(), &NEW_NID.repeat(32)).join(crate::config::DB_FILE),
        )
        .expect("the backed-out mount let go of its store");
        assert_eq!(
            stop(handles, cursor).await,
            [staging],
            "the staged nest was not drained"
        );
    }

    /// #1777, review: when the staged nest will not come off the cursor either, it is still mounted,
    /// so it keeps its record, and the error names both failures.
    #[tokio::test(start_paused = true)]
    async fn a_move_whose_back_out_fails_keeps_the_staged_record_and_says_so() {
        let root = tempfile::tempdir().unwrap();
        let staging = RuntimeHandles::staging_name("usdc");
        let (handles, cursor, err) =
            move_against_a_stuck_cursor(root.path(), &["usdc", "usdc__moving"]).await;
        let err = format!("{err:#}");
        assert!(
            err.contains("unmounting 'usdc'") && err.contains(&format!("unmounting '{staging}'")),
            "the error must name the drain and the back-out: {err}"
        );

        let mounted: Vec<_> = handles.states.iter().map(|(n, _)| n.clone()).collect();
        assert_eq!(
            mounted,
            ["usdc", staging.as_str()],
            "premise: the back-out did not land"
        );
        assert_eq!(
            recorded(&handles),
            [
                ("usdc".to_string(), OLD_NID.repeat(32)),
                (staging.clone(), NEW_NID.repeat(32))
            ],
            "a staged nest still mounted lost its record"
        );
        assert!(stop(handles, cursor).await.is_empty());
    }

    /// A chain whose tip is `tip` and that holds no logs. With `parked`, the first tip call waits
    /// until the test releases it, after saying it has started.
    struct QuietChain {
        tip: u64,
        parked: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
        asked: std::sync::atomic::AtomicBool,
    }

    impl QuietChain {
        fn at(tip: u64) -> Self {
            QuietChain {
                tip,
                parked: None,
                asked: Default::default(),
            }
        }
    }

    #[async_trait::async_trait]
    impl Source for QuietChain {
        async fn tip(&self) -> Result<u64> {
            if let Some((entered, release)) = &self.parked {
                if !self.asked.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    entered.notify_one();
                    release.notified().await;
                }
            }
            Ok(self.tip)
        }
        async fn block_hash(&self, n: u64) -> Result<Option<String>> {
            Ok(Some(format!("0x{n:064x}")))
        }
        async fn logs(
            &self,
            _filter: &crate::source::LogFilter,
            _from: u64,
            _to: u64,
        ) -> Result<Vec<crate::rpc::Log>> {
            Ok(Vec::new())
        }
    }

    /// A cursor that admits every mount it is sent and reports the block each joined at.
    fn admitting_cursor() -> (
        tokio::sync::mpsc::UnboundedSender<indexer::CursorCommand>,
        tokio::task::JoinHandle<Vec<u64>>,
    ) {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let cursor = tokio::spawn(async move {
            let mut joined = Vec::new();
            while let Some(cmd) = rx.recv().await {
                if let indexer::CursorCommand::Mount { nest, next, ack } = cmd {
                    joined.push(next);
                    drop(nest);
                    if let Some(ack) = ack {
                        let _ = ack.send(());
                    }
                }
            }
            joined
        });
        (tx, cursor)
    }

    /// #2131: a nest mounted beside a cursor at block 1,000,000 joins it within one window of there.
    /// It used to join at its cold-start block, and the cursor, advancing from the min of its nests,
    /// held every co-tenant until the newcomer had walked its history.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_live_mount_catches_up_before_it_joins_the_cursor() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join(MOUNTS_FILE),
            "[runtime]\nname = \"r\"\nchain = \"ethereum\"\nchain_id = 1\nrpc_urls = []\n",
        )
        .unwrap();
        write_nest_dir(root.path(), "usdc", "ethereum", 1);
        let dir = MountTable::nest_dir(root.path(), "usdc");
        let co_dir = root.path().join("co");
        std::fs::create_dir_all(&co_dir).unwrap();
        let co_key = "co-2131";
        crate::metrics::METRICS
            .nest(co_key)
            .set_last_block(1_000_000);

        let (tx, cursor) = admitting_cursor();
        let mut handles = idle_handles(root.path());
        let mut co = nest_state(&co_dir, "ethereum");
        co.runtime_health = Some((co_key.to_string(), handles.health.clone()));
        handles.states.push((co_key.to_string(), co));
        let source: Arc<dyn Source> = Arc::new(QuietChain::at(1_000_010));
        handles
            .mount_ctx
            .sources
            .insert("ethereum".to_string(), source);
        handles.lifecycle.insert("ethereum".to_string(), tx);
        handles.mount_ctx.backfill = Some(20_000);
        handles.mount_ctx.window_override = Some(1_000);

        handles
            .mount_dataset("usdc", None, "usdc", None, dir.clone())
            .await
            .expect("mount beside a running cursor");
        handles.lifecycle.clear();
        for (_, w) in std::mem::take(&mut handles.alert_workers) {
            w.abort();
            let _ = w.await;
        }
        drop(handles);
        let joined = cursor.await.unwrap();
        assert_eq!(joined.len(), 1, "one mount reaches the cursor");
        assert!(
            joined[0] + 1_000 > 1_000_000,
            "the nest joined at block {}, more than a window behind the cursor at 1,000,000",
            joined[0]
        );
    }

    /// A runtime whose only source parks its first tip call, a cursor that admits mounts, and a
    /// `usdc` nest that has never indexed, so preparing it asks for that tip.
    fn parked_runtime(
        root: &Path,
    ) -> (
        Arc<tokio::sync::Mutex<RuntimeHandles>>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
    ) {
        std::fs::write(
            root.join(MOUNTS_FILE),
            "[runtime]\nname = \"r\"\nchain = \"ethereum\"\nchain_id = 1\nrpc_urls = []\n",
        )
        .unwrap();
        write_nest_dir(root, "usdc", "ethereum", 1);
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let mut chain = QuietChain::at(1_000);
        chain.parked = Some((entered.clone(), release.clone()));
        let (tx, _cursor) = admitting_cursor();
        let mut handles = idle_handles(root);
        let source: Arc<dyn Source> = Arc::new(chain);
        handles
            .mount_ctx
            .sources
            .insert("ethereum".to_string(), source);
        handles.lifecycle.insert("ethereum".to_string(), tx);
        (Arc::new(tokio::sync::Mutex::new(handles)), entered, release)
    }

    /// #2134: a mount job's catch-up holds no lock another tenant's admin call or shutdown waits on.
    /// The join used to run `prepare` under the runtime's lock, which on `--seal-direct` is the whole
    /// historic backfill.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_mount_jobs_catch_up_does_not_hold_the_runtime_lock() {
        use crate::mount_jobs::{MountJob, MountPhase};
        let root = tempfile::tempdir().unwrap();
        let (handles, entered, release) = parked_runtime(root.path());
        let jobs = Arc::new(crate::mount_jobs::MountJobs::load(root.path()));
        let claimed = jobs
            .claim(MountJob::new("usdc", None, MountPhase::Accepted))
            .unwrap();
        let job = spawn_mount_job(
            handles.clone(),
            jobs.clone(),
            "usdc".into(),
            None,
            claimed.generation,
        );
        entered.notified().await;
        let free = tokio::time::timeout(std::time::Duration::from_secs(5), handles.lock())
            .await
            .is_ok();
        release.notify_one();
        job.await.unwrap();
        assert!(free, "the runtime lock was held while the mount caught up");
        assert_eq!(jobs.get("usdc").unwrap().phase, MountPhase::Live);
        for (_, w) in std::mem::take(&mut handles.lock().await.alert_workers) {
            w.abort();
            let _ = w.await;
        }
    }

    /// #2220: a mount job a restart resumes does not hold the lock `dev` takes to build the service it
    /// binds, so the first bind waits on no one's catch-up.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_resumed_mount_job_does_not_delay_the_first_bind() {
        use crate::mount_jobs::{MountJob, MountPhase};
        let root = tempfile::tempdir().unwrap();
        let (handles, entered, release) = parked_runtime(root.path());
        crate::mount_jobs::MountJobs::load(root.path())
            .put(MountJob::new("usdc", None, MountPhase::Joining))
            .unwrap();
        let jobs = start_mount_jobs(root.path(), &handles, true).await.unwrap();
        entered.notified().await;
        let free = tokio::time::timeout(std::time::Duration::from_secs(5), handles.lock())
            .await
            .is_ok();
        release.notify_one();
        assert!(
            free,
            "the service build waited on a resumed mount's catch-up"
        );
        let live = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while jobs.get("usdc").is_none_or(|j| j.phase != MountPhase::Live) {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await;
        assert!(live.is_ok(), "the resumed job did not finish");
        for (_, w) in std::mem::take(&mut handles.lock().await.alert_workers) {
            w.abort();
            let _ = w.await;
        }
    }

    /// #2134: a mount catching up with no lock held keeps its place. A second mount of its name is
    /// refused until it has joined or been dropped, and admission prices it against the cursor.
    #[tokio::test]
    async fn a_joining_mount_holds_its_name_and_its_budget() {
        let root = tempfile::tempdir().unwrap();
        write_nest_dir(root.path(), "usdc", "ethereum", 1);
        let (tx, _cursor) = admitting_cursor();
        let mut handles = idle_handles(root.path());
        let source: Arc<dyn Source> = Arc::new(QuietChain::at(1_000));
        handles
            .mount_ctx
            .sources
            .insert("ethereum".to_string(), source);
        handles.lifecycle.insert("ethereum".to_string(), tx);
        let Begun::Join(pending) = handles.begin_mount("usdc", None).await.unwrap() else {
            panic!("a nest beside a running cursor is reserved, not mounted");
        };
        let err = handles.plan_mount("usdc", None).unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<MountRefusal>(),
                Some(MountRefusal::Joining { .. })
            ),
            "{err:#}"
        );
        let reserved = handles.joining.list()[0].incoming_mb;
        handles.mount_ctx.max_rss_mb = RUNTIME_BASE_RSS_MB + reserved;
        write_nest_dir(root.path(), "dai", "ethereum", 1);
        let err = handles.mount("dai", None).await.unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<MountRefusal>(),
                Some(MountRefusal::OverBudget { .. })
            ),
            "the joining mount was not priced: {err:#}"
        );
        drop(pending);
        assert!(handles.joining.list().is_empty());
        handles
            .plan_mount("usdc", None)
            .expect("the name is free again");
    }
}
