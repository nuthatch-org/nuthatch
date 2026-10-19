//! The writer-worker role (RFC-0022 §2): the process that actually reconciles.
//!
//! Everything this needs already existed and tested — the control plane, the scheduler, the lease,
//! the fence, [`crate::reconcile::tick`] — and **nothing called it**. `reconcile::tick` had six tests
//! against a live Postgres and no caller, so the compose `writer` service ran an ordinary embedded
//! `dev`: no heartbeat, no lease, no desired state. `--scale writer=2` therefore did not behave as the
//! docs described. This is the missing wire.
//!
//! Worth naming as a failure mode rather than glossing: a well-tested library function with no caller
//! looks exactly like a working feature from the outside, and every test passes.
//!
//! ## The loop
//!
//! Tick, sleep, repeat. Each tick heartbeats, re-reads desired state, plans, and acquires or releases
//! leases. Between ticks the cursors it owns index normally.
//!
//! ## Why a tick failure is a warning, not an exit
//!
//! A control-plane outage must stop *rescheduling*, not *ingestion*. If the database is unreachable
//! this logs and tries again next tick, while the cursors it already holds keep working on leases that
//! have not yet expired. Exiting on a failed tick would convert a control-plane blip into a fleet-wide
//! outage — precisely the coupling the independence between lease and heartbeat exists to avoid.
//!
//! The lease TTL is deliberately several ticks long for the same reason: one missed tick must not hand
//! a cursor to somebody else while its owner is perfectly healthy.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};

use crate::controlplane::ControlPlane;
use crate::reconcile::{tick, CursorHosts, TickOutcome};
use crate::store::HotStore;

/// How often a worker reconciles.
pub const TICK_INTERVAL: Duration = Duration::from_secs(5);

/// Lease TTL, in ticks. A lease must outlive several missed ticks: at 5s a ×6 TTL tolerates half a
/// minute of control-plane trouble before a healthy owner risks losing a cursor it is still indexing.
const LEASE_TTL_TICKS: u64 = 6;

/// The stores a worker can host, one per chain.
///
/// Built once at startup from the nests on disk. A worker offers the chains it *can* run; the
/// scheduler decides which it *should*, and the lease decides which it *does*.
pub struct Hosts {
    stores: Vec<(String, Arc<dyn HotStore>)>,
}

impl Hosts {
    /// One store per chain, from a Postgres hot store shared with the rest of the fleet.
    ///
    /// Keyed by chain rather than by nest because the **cursor** is the unit of ownership (RFC-0021):
    /// every nest on a chain shares one, so they share one lease. A per-nest lease would let two
    /// workers each hold "half" a cursor, which is not a thing that can exist.
    pub fn from_chains(hot_store_url: &str, chains: &[String]) -> Result<Hosts> {
        let mut stores = Vec::new();
        for chain in chains {
            // The schema name has to be a valid identifier, and chain names carry hyphens.
            let ns = format!("cursor_{}", chain.replace('-', "_"));
            let store = crate::pgstore::PgStore::connect(hot_store_url, &ns)
                .with_context(|| format!("cannot open the hot store for chain '{chain}'"))?;
            stores.push((chain.clone(), Arc::new(store) as Arc<dyn HotStore>));
        }
        Ok(Hosts { stores })
    }
}

impl CursorHosts for Hosts {
    fn stores(&self) -> Vec<(String, Arc<dyn HotStore>)> {
        self.stores.clone()
    }
}

/// The nests a worker is actually indexing, keyed by the chain whose cursor authorises them.
///
/// Keyed by **chain**, not by nest, because the lease is per cursor: losing one cursor stops exactly
/// the nests that cursor authorised and leaves the others running. That is the isolation RFC-0021
/// promises, expressed as the shape of this map rather than as care taken at each call site.
struct Running<N = crate::indexer::NestRuntime> {
    by_chain: std::collections::HashMap<String, Vec<N>>,
}

impl<N> Default for Running<N> {
    fn default() -> Self {
        Running {
            by_chain: std::collections::HashMap::new(),
        }
    }
}

/// All `Running` asks of a nest. A trait so a test can stand a bare task in for a `NestRuntime`,
/// which cannot be built without a whole serving state.
trait Ingesting {
    fn abort(&self);
}

impl Ingesting for crate::indexer::NestRuntime {
    fn abort(&self) {
        self.ingest.abort();
    }
}

impl<N: Ingesting> Running<N> {
    /// Stop everything a cursor authorised. Called when its lease is lost or released.
    ///
    /// The store's fence already refuses writes from a stale holder, so nothing here protects
    /// correctness - it stops a task grinding through RPC for a cursor it no longer owns, which is
    /// waste and, in a log, actively misleading.
    fn stop(&mut self, chain: &str) {
        if let Some(rts) = self.by_chain.remove(chain) {
            for rt in &rts {
                rt.abort();
            }
            if !rts.is_empty() {
                tracing::info!(chain = %chain, nests = rts.len(), "stopped nests - cursor no longer held");
            }
        }
    }
}
/// Where the control plane says a declared nest's bundle lives (RFC-0022 §4's fleet-wide pinning).
///
/// `desired_nest` has carried `version` and `bundle_hash` since that RFC; until now nothing read them,
/// which is precisely why a worker could be assigned a nest it had no way to fetch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NestSource {
    pub name: String,
    pub chain: String,
    /// The version to resolve against the registry index when no `bundle_hash` is pinned.
    pub version: Option<String>,
    /// The exact bundle the fleet is pinned to. When set, this is the authority - not the index.
    pub bundle_hash: Option<String>,
}

/// Where a declared nest's project directory is going to come from.
///
/// Extracted as a value rather than done inline because the choice *is* the interesting part: the bug
/// this closes (#250) was a worker being assigned a nest it had no way to locate, and a branch that
/// only exists inside an async fn needing a live control plane and a registry is a branch nobody
/// tests. Deciding is pure; only acting on `Pull` does I/O.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum NestLocation {
    /// Already on this machine - under `--nest-root`, or already pulled at the pinned hash.
    Local(std::path::PathBuf),
    /// Not here, but locatable: fetch it into `target`.
    Pull {
        reference: String,
        pinned_hash: Option<String>,
        target: std::path::PathBuf,
        /// Whether `target` must be cleared first. Only ever true for an unpinned pull, whose target
        /// is a fixed `latest` directory in the worker's own cache; a pinned pull's target is named
        /// by content and so is either absent or already correct.
        replace: bool,
    },
    /// Not here and not locatable, with the reason an operator needs to fix it.
    Unlocatable(String),
}

/// Decide where a declared nest comes from. See [`NestLocation`].
///
/// Two rules, both load-bearing:
///
/// **Local beats the registry.** A nest an operator put under `--nest-root` is a deliberate act -
/// often a hand-edited view or a debug build - and silently replacing it with whatever the registry
/// holds would make the machine's state unreproducible from the outside in exactly the moment
/// someone is trying to understand it.
///
/// **Pulled nests live in a content-addressed cache, not in `--nest-root`.** Pulling into the nest
/// root would be wrong twice over: that mount is read-only in the writer-node topology, and - worse -
/// the pulled copy would then satisfy the local check forever, so **re-pinning the fleet to a new
/// bundle would silently do nothing**. Keyed by hash, a changed pin is a different directory and
/// re-pulls by construction, while an unchanged pin costs no download at all.
pub(crate) fn resolve_nest_dir(
    nests: &NestPaths,
    name: &str,
    source: Option<&NestSource>,
) -> NestLocation {
    let NestPaths {
        root: nest_root,
        cache: cache_root,
        registry,
    } = nests;
    // The control plane checks these on write; rows written before it did are refused here.
    if let Err(e) = crate::runtime::check_mount_name(name, crate::runtime::DEFAULT_TENANT) {
        return NestLocation::Unlocatable(format!("{e:#}"));
    }
    if let Some(hash) = source.and_then(|s| s.bundle_hash.as_deref()) {
        if !crate::distribution::is_nid_shaped(hash) {
            return NestLocation::Unlocatable(format!(
                "pinned bundle hash '{hash}' is not 64 hex characters"
            ));
        }
    }
    let per_nest = nest_root.join(name);
    if per_nest.join(crate::config::CONFIG_FILE).exists() {
        return NestLocation::Local(per_nest);
    }
    if nest_root.join(crate::config::CONFIG_FILE).exists() {
        // A single nest mounted directly at the root - what the compose topology does with `./nest`.
        return NestLocation::Local(nest_root.to_path_buf());
    }
    let src = match (registry, source) {
        (Some(_), Some(s)) => s,
        (None, _) => {
            return NestLocation::Unlocatable(format!(
                "not under --nest-root ({}) and no --registry configured",
                nest_root.display()
            ))
        }
        (Some(_), None) => {
            return NestLocation::Unlocatable(
                "not under --nest-root and the control plane records no source for it".into(),
            )
        }
    };
    match &src.bundle_hash {
        // Pinned: the fetch goes **by that hash**, never through the registry's mutable index - so
        // what a worker indexes is what the operator pinned, and re-tagging a version in the registry
        // cannot silently change it. Already-pulled means already-correct: the directory name *is*
        // the verification.
        Some(hash) => {
            let target = cache_root.join(name).join(hash);
            if target.join(crate::config::CONFIG_FILE).exists() {
                NestLocation::Local(target)
            } else {
                NestLocation::Pull {
                    reference: match &src.version {
                        Some(v) => format!("{name}@{v}"),
                        None => name.to_string(),
                    },
                    pinned_hash: Some(hash.clone()),
                    target,
                    replace: false,
                }
            }
        }
        // Unpinned: whatever the index says today, which may differ from what it said yesterday. So
        // this re-pulls on every start and cannot be cached by content. A fleet that has not pinned
        // is a fleet that accepted whatever is latest, which RFC-0022 §4 already warns about.
        None => NestLocation::Pull {
            reference: match &src.version {
                Some(v) => format!("{name}@{v}"),
                None => name.to_string(),
            },
            pinned_hash: None,
            target: cache_root.join(name).join("unpinned"),
            replace: true,
        },
    }
}

/// Start every declared nest for `chain` that this worker holds the cursor for.
///
/// **Where a nest comes from used to be the open half of this.** RFC-0022 named RFC-0019 - "the
/// registry workers pull nests from" - as the intended source but never read the `version` and
/// `bundle_hash` its own schema recorded, so a worker could be assigned a nest it had no way to
/// locate. That gap is *why* ingestion was never wired (issue #250): you cannot build a nest you
/// cannot find. A worker now takes nests the operator placed under `--nest-root` and pulls the rest
/// from `--registry` at the pinned hash. See [`resolve_nest_dir`].
async fn start_nests_for(
    cp: &ControlPlane,
    running: &mut Running,
    store: Arc<dyn HotStore>,
    nests: &NestPaths,
    chain: &str,
    with_secrets: bool,
) -> Result<usize> {
    if running.by_chain.contains_key(chain) {
        return Ok(0); // already indexing this cursor
    }
    // Before any nest dials: one started on its bundle's RPC would ignore the operator's secret.
    let secrets = if with_secrets {
        chain_secrets(cp, chain)?
    } else {
        HashMap::new()
    };
    let desired: Vec<_> = cp
        .desired()?
        .into_iter()
        .filter(|d| d.chain == chain)
        .collect();

    // Which bundle each declared nest is pinned to, so one that is *not* on this machine can be
    // fetched rather than merely warned about (RFC-0019: "the registry workers pull nests from").
    let sources: std::collections::HashMap<String, NestSource> = cp
        .desired_sources()
        .unwrap_or_default()
        .into_iter()
        .map(|s| (s.name.clone(), s))
        .collect();

    let mut started = Vec::new();
    for d in &desired {
        let dir = match resolve_nest_dir(nests, &d.name, sources.get(&d.name)) {
            NestLocation::Local(dir) => dir,
            NestLocation::Pull {
                reference,
                pinned_hash,
                target,
                replace,
            } => {
                tracing::info!(
                    nest = %d.name, %reference, pinned = pinned_hash.is_some(),
                    "not on this worker - pulling from the registry"
                );
                let reg = nests
                    .registry
                    .as_deref()
                    .expect("a Pull is only produced when a registry is configured");
                // An unpinned pull overwrites a fixed `unpinned` directory, and `blob::load` refuses
                // a non-empty target. Confined to this worker's **own cache** - never `--nest-root`,
                // so no operator-placed file is ever in reach of this - and only for the location the
                // resolver itself named.
                if replace && target.exists() {
                    debug_assert!(target.starts_with(&nests.cache));
                    if let Err(e) = std::fs::remove_dir_all(&target) {
                        tracing::warn!(
                            nest = %d.name, dir = %target.display(),
                            "could not clear the cached copy before re-pulling: {e:#}"
                        );
                        continue;
                    }
                }
                match crate::distribution::load_pinned_from_registry(
                    reg,
                    &reference,
                    pinned_hash.as_deref(),
                    Some(&target),
                )
                .await
                {
                    Ok(()) => target,
                    Err(e) => {
                        tracing::warn!(
                            nest = %d.name, %reference,
                            "could not pull from the registry - not indexing it here: {e:#}"
                        );
                        continue;
                    }
                }
            }
            NestLocation::Unlocatable(why) => {
                // Not fatal, and **loud**: a declared nest this worker cannot find is exactly the
                // silence that let #250 survive. Another worker may have it; this one says so.
                tracing::warn!(
                    nest = %d.name, chain = %chain,
                    "declared nest cannot be located - it will not be indexed here: {why}"
                );
                continue;
            }
        };
        let mut cfg = match crate::config::Config::load(&dir) {
            Ok(c) => c,
            Err(e) => {
                // Not fatal, and **loud**: a declared nest this worker cannot find is exactly the
                // silence that let #250 survive. Another worker may have it; this one says so.
                tracing::warn!(
                    nest = %d.name, chain = %chain, dir = %dir.display(),
                    "declared nest not found on this worker - it will not be indexed here: {e:#}"
                );
                continue;
            }
        };
        if cfg.nest.chain != chain {
            tracing::warn!(
                nest = %d.name, declared = %chain, found = %cfg.nest.chain,
                "nest on disk is for a different chain than the control plane declared - skipping"
            );
            continue;
        }
        if let Some(s) = secrets.get(&d.name) {
            apply_secrets(&d.name, &mut cfg, s);
        }
        let source: Arc<dyn crate::source::Source> =
            Arc::new(crate::rpc::RpcClient::new(cfg.nest.rpc_urls.clone())?);
        match crate::indexer::spawn_nest_on_store(
            source,
            dir,
            cfg,
            store.clone(),
            None,
            false,
            // Sequential seal path: a writer shares its endpoint quota with every other worker in the
            // pool, so a per-nest fan-out here multiplies across the fleet. Conservative until the
            // pool has a shared budget to spend (RFC-0022 has none today).
            1,
        )
        .await
        {
            Ok(rt) => {
                tracing::info!(nest = %d.name, chain = %chain, "indexing (writer pool)");
                started.push(rt);
            }
            Err(e) => tracing::warn!(nest = %d.name, "could not start: {e:#}"),
        }
    }
    let n = started.len();
    if n > 0 {
        running.by_chain.insert(chain.to_string(), started);
    } else if !desired.is_empty() {
        tracing::warn!(
            chain = %chain, declared = desired.len(),
            "holding this cursor but indexing nothing - no declared nest for it is present on this \
             worker (see --nest-root)"
        );
    }
    Ok(n)
}

/// Run the reconcile loop until the process is asked to stop.
///
/// `nest_root` is where this worker looks for the nests it is asked to index - see
/// [`start_nests_for`] for why a path is needed at all, and why pulling bundles from the registry
/// (RFC-0019) is the next step rather than this.
///
/// On shutdown it **releases its leases** rather than letting them expire. A graceful exit that left
/// them held would strand every cursor it owned for a full TTL, which is the difference between a
/// rolling restart being invisible and being a thirty-second outage per worker.
/// Everything that answers "where does a nest come from on this machine": what the operator placed,
/// where to fetch what they did not, and where fetched copies live. One value because the three are
/// only ever meaningful together - see [`resolve_nest_dir`].
#[derive(Debug, Clone)]
pub struct NestPaths {
    /// `--nest-root`: operator-placed nests. Read-only in the writer-node topology, and it wins.
    pub root: std::path::PathBuf,
    /// `--nest-cache`: where pulled bundles land, laid out `<cache>/<name>/<bundle-hash>`.
    pub cache: std::path::PathBuf,
    /// `--registry`: where to pull from. `None` means this worker runs only what is already here.
    pub registry: Option<String>,
}

pub async fn run(
    cp: ControlPlane,
    hosts: Hosts,
    worker_id: &str,
    budget_mb: u64,
    secrets_for_assigned: bool,
    nests: NestPaths,
) -> Result<()> {
    let ttl = TICK_INTERVAL.as_secs() * LEASE_TTL_TICKS;
    tracing::info!(
        worker = %worker_id,
        budget_mb,
        tick_secs = TICK_INTERVAL.as_secs(),
        lease_ttl_secs = ttl,
        chains = hosts.stores.len(),
        "writer worker started (RFC-0022 §2)"
    );

    let mut held: Vec<String> = Vec::new();
    let mut running = Running::default();
    loop {
        tokio::select! {
            _ = tokio::time::sleep(TICK_INTERVAL) => {}
            _ = shutdown() => {
                // Best-effort: a failure here costs a TTL of staleness, never correctness, so it is
                // logged rather than propagated out of a shutdown path.
                for (chain, store) in &hosts.stores {
                    if held.contains(chain) {
                        // Stop indexing *before* releasing: the moment the lease is gone another
                        // worker may take the cursor, and two writers briefly overlapping is the
                        // situation the fence exists to survive rather than one to create casually.
                        running.stop(chain);
                        if let Err(e) = store.release_lease() {
                            tracing::warn!(chain = %chain, "could not release lease on shutdown: {e:#}");
                        }
                    }
                }
                if let Err(e) = cp.deregister(worker_id) {
                    tracing::warn!("could not deregister: {e:#}");
                }
                tracing::info!(worker = %worker_id, released = held.len(), "worker stopped cleanly");
                return Ok(());
            }
        }

        match tick(&cp, &hosts, worker_id, budget_mb, ttl) {
            Ok(outcome) => {
                held = settle(&outcome, &held, &mut running, worker_id);
                report(&outcome, worker_id);

                // **The half that was missing (issue #250).** Holding a cursor means indexing the
                // nests it authorises.
                for chain in &held {
                    if let Some((_, store)) = hosts.stores.iter().find(|(c, _)| c == chain) {
                        if let Err(e) = start_nests_for(
                            &cp,
                            &mut running,
                            store.clone(),
                            &nests,
                            chain,
                            secrets_for_assigned,
                        )
                        .await
                        {
                            tracing::warn!(chain = %chain, "could not start nests: {e:#}");
                        }
                    }
                }
            }
            // Deliberately not fatal - see the module docs. Cursors we already hold keep indexing on
            // leases that have not expired.
            Err(e) => tracing::warn!(
                worker = %worker_id,
                "reconcile tick failed, retrying next tick (held cursors keep working): {e:#}"
            ),
        }
    }
}

/// Take what the tick saw in the leases as what we hold, and stop the nests of every cursor that
/// dropped out - exactly those, leaving the rest running.
fn settle<N: Ingesting>(
    outcome: &TickOutcome,
    before: &[String],
    running: &mut Running<N>,
    worker_id: &str,
) -> Vec<String> {
    let held = outcome.held.clone();
    for chain in before.iter().filter(|c| !held.contains(c)) {
        if !outcome.released.contains(chain) {
            tracing::warn!(
                worker = %worker_id,
                chain = %chain,
                "lost the lease on this cursor to another holder - stopping its nests"
            );
        }
        running.stop(chain);
    }
    held
}

/// Log only what changed or what an operator should act on. A loop that logged every tick would bury
/// the one tick that mattered.
fn report(o: &TickOutcome, worker_id: &str) {
    if !o.acquired.is_empty() {
        tracing::info!(worker = %worker_id, chains = ?o.acquired, "acquired cursors");
    }
    if !o.released.is_empty() {
        tracing::info!(worker = %worker_id, chains = ?o.released, "released cursors");
    }
    if !o.contended.is_empty() {
        // One occurrence is benign - another worker got there first. Persistent contention means two
        // schedulers disagree about the world, which is worth someone looking at.
        tracing::debug!(worker = %worker_id, chains = ?o.contended, "cursors held elsewhere");
    }
    if !o.unplaceable.is_empty() {
        // Warn, every tick, on purpose: an under-served fleet must not look healthy. `GET /plan` says
        // why, and the two reasons need different actions.
        tracing::warn!(
            chains = ?o.unplaceable,
            "cursors nobody can place - see GET /plan on the control plane for the reason"
        );
    }
}

/// The control-plane secrets of the nests declared on `chain`, keyed by nest name (RFC-0022 §5).
/// Called only for a cursor this worker holds, so it never receives another worker's credentials.
pub fn chain_secrets(
    cp: &ControlPlane,
    chain: &str,
) -> Result<HashMap<String, HashMap<String, String>>> {
    let names: Vec<String> = cp
        .desired()?
        .into_iter()
        .filter(|d| d.chain == chain)
        .map(|d| d.name)
        .collect();
    cp.secrets_for(&names)
}

/// Apply a nest's secrets to the config it is about to run with. `rpc_url` is the one key a nest
/// consumes; any other is named in a warning so a typo is not silently ignored.
fn apply_secrets(nest: &str, cfg: &mut crate::config::Config, secrets: &HashMap<String, String>) {
    for (key, value) in secrets {
        match key.as_str() {
            "rpc_url" => cfg.nest.rpc_urls = vec![value.clone()],
            _ => {
                tracing::warn!(nest = %nest, %key, "control-plane secret has no consumer - ignored")
            }
        }
    }
    // Key count only: a log line is the easiest place for a credential to escape to.
    tracing::info!(nest = %nest, keys = secrets.len(), "runtime secrets applied");
}

async fn shutdown() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        if let Ok(mut s) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            s.recv().await;
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {}
        _ = term => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(version: Option<&str>, hash: Option<&str>) -> NestSource {
        NestSource {
            name: "horizon".into(),
            chain: "arbitrum-one".into(),
            version: version.map(str::to_string),
            bundle_hash: hash.map(str::to_string),
        }
    }

    fn hash(c: char) -> String {
        c.to_string().repeat(64)
    }

    fn touch_nest(dir: &std::path::Path) {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(dir.join(crate::config::CONFIG_FILE), "[nest]\n").unwrap();
    }

    /// A resolver call with both roots under one tempdir, so a test never has to name two.
    fn resolve(
        root: &std::path::Path,
        registry: Option<&str>,
        src: Option<&NestSource>,
    ) -> NestLocation {
        let paths = NestPaths {
            root: root.join("nests"),
            cache: root.join("cache"),
            registry: registry.map(str::to_string),
        };
        resolve_nest_dir(&paths, "horizon", src)
    }

    /// A nest the operator put on the box is used **as it is on disk**, registry or no registry.
    /// Silently replacing a hand-edited or debug-built nest with the registry's copy would make the
    /// machine unreproducible from the outside at the exact moment someone is debugging it.
    #[test]
    fn a_local_nest_wins_over_the_registry() {
        let root = tempfile::tempdir().unwrap();
        touch_nest(&root.path().join("nests/horizon"));
        assert_eq!(
            resolve(
                root.path(),
                Some("/some/registry"),
                Some(&source(Some("1.0.0"), Some(&hash('c'))))
            ),
            NestLocation::Local(root.path().join("nests/horizon"))
        );
    }

    /// The compose topology mounts a single nest at `./nest` rather than `./nest/<name>`.
    #[test]
    fn a_single_nest_mounted_at_the_root_is_found() {
        let root = tempfile::tempdir().unwrap();
        touch_nest(&root.path().join("nests"));
        assert_eq!(
            resolve(root.path(), None, None),
            NestLocation::Local(root.path().join("nests"))
        );
    }

    /// The bug this closes (#250): assigned, absent, and previously unrunnable. The pin must reach
    /// the fetch - a `Pull` that drops `bundle_hash` silently downgrades to trusting the index - and
    /// the target must be the cache, never the read-only nest root.
    #[test]
    fn an_absent_nest_is_pulled_at_the_pinned_hash() {
        let root = tempfile::tempdir().unwrap();
        assert_eq!(
            resolve(
                root.path(),
                Some("/some/registry"),
                Some(&source(Some("1.2.3"), Some(&hash('d'))))
            ),
            NestLocation::Pull {
                reference: "horizon@1.2.3".into(),
                pinned_hash: Some(hash('d')),
                target: root.path().join("cache/horizon").join(hash('d')),
                replace: false,
            }
        );
    }

    /// **The reason the cache is content-addressed.** Pulling into the nest root would satisfy the
    /// local check forever, so re-pinning the fleet would silently do nothing - a worker would keep
    /// indexing the old bundle while the control plane said otherwise, which is the worst of both
    /// worlds: no error, wrong data. Keyed by hash, a changed pin is simply a different directory.
    #[test]
    fn re_pinning_re_pulls_while_an_unchanged_pin_reuses_what_was_pulled() {
        let root = tempfile::tempdir().unwrap();
        let old = source(Some("1.0.0"), Some(&hash('a')));
        let NestLocation::Pull { target, .. } = resolve(root.path(), Some("/reg"), Some(&old))
        else {
            panic!("first sight of a pinned nest must pull");
        };
        touch_nest(&target); // as the pull would leave it

        // Same pin: no second download.
        assert_eq!(
            resolve(root.path(), Some("/reg"), Some(&old)),
            NestLocation::Local(target),
            "an unchanged pin must not re-pull"
        );

        // Re-pinned: a different bundle, so a different directory, so a fetch.
        let new = source(Some("2.0.0"), Some(&hash('b')));
        assert!(
            matches!(
                resolve(root.path(), Some("/reg"), Some(&new)),
                NestLocation::Pull { ref pinned_hash, .. } if pinned_hash.as_deref() == Some(hash('b').as_str())
            ),
            "a re-pinned fleet must re-pull, not keep running the bundle it already has"
        );
    }

    /// Unpinned follows a movable pointer, so it cannot be cached by content: it re-pulls every time
    /// and must clear its target, because `blob::load` refuses a non-empty directory.
    #[test]
    fn an_unpinned_nest_re_pulls_into_a_replaceable_directory() {
        let root = tempfile::tempdir().unwrap();
        let loc = resolve(root.path(), Some("/reg"), Some(&source(None, None)));
        let NestLocation::Pull {
            reference,
            pinned_hash,
            target,
            replace,
        } = loc
        else {
            panic!("expected a pull");
        };
        assert_eq!(
            reference, "horizon",
            "no version means the index's `latest`"
        );
        assert_eq!(pinned_hash, None);
        assert!(replace, "a movable pointer's target must be cleared");
        assert!(
            target.starts_with(root.path().join("cache")),
            "anything cleared must be inside the worker's own cache; got {}",
            target.display()
        );

        // Already pulled once - still a pull, because `latest` may have moved since.
        touch_nest(&target);
        assert!(
            matches!(
                resolve(root.path(), Some("/reg"), Some(&source(None, None))),
                NestLocation::Pull { replace: true, .. }
            ),
            "an unpinned nest must be re-fetched, not reused"
        );
    }

    /// Absent and unlocatable must be *stated*, not guessed at: both reasons name the thing an
    /// operator has to change. Silence here is what let #250 survive a release.
    #[test]
    fn an_unlocatable_nest_says_which_half_is_missing() {
        let root = tempfile::tempdir().unwrap();
        let no_registry = resolve(root.path(), None, Some(&source(Some("1"), None)));
        let no_source = resolve(root.path(), Some("/reg"), None);
        match (&no_registry, &no_source) {
            (NestLocation::Unlocatable(a), NestLocation::Unlocatable(b)) => {
                assert!(a.contains("--registry"), "got: {a}");
                assert!(b.contains("no source"), "got: {b}");
            }
            other => panic!("both should be unlocatable, got {other:?}"),
        }
    }

    /// #2140: a declared name and a pinned hash become path segments on every worker, and the
    /// unpinned branch removes its target. One that leaves the nest root or cache is refused.
    #[test]
    fn a_name_or_hash_that_leaves_the_cache_is_unlocatable() {
        let root = tempfile::tempdir().unwrap();
        let paths = NestPaths {
            root: root.path().join("nests"),
            cache: root.path().join("cache"),
            registry: Some("/reg".into()),
        };
        for name in ["../../srv/x", "/srv/x", ".."] {
            let src = NestSource {
                name: name.into(),
                ..source(None, None)
            };
            let loc = resolve_nest_dir(&paths, name, Some(&src));
            assert!(
                matches!(loc, NestLocation::Unlocatable(_)),
                "name {name:?} resolved to {loc:?}"
            );
        }
        for h in ["../../srv/x", "/srv/x", "abc"] {
            let loc = resolve(root.path(), Some("/reg"), Some(&source(Some("1"), Some(h))));
            assert!(
                matches!(loc, NestLocation::Unlocatable(_)),
                "hash {h:?} resolved to {loc:?}"
            );
        }
    }

    /// #2138: an `rpc_url` secret replaces the bundle's RPC in the config the nest dials with.
    #[test]
    fn an_rpc_url_secret_replaces_the_bundles_rpc() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(crate::config::CONFIG_FILE),
            "[nest]\nname = \"horizon\"\nchain = \"arbitrum-one\"\nchain_id = 42161\n\
             rpc_urls = [\"https://bundle.example\"]\n",
        )
        .unwrap();
        let mut cfg = crate::config::Config::load(dir.path()).unwrap();
        let secrets = HashMap::from([
            (
                "rpc_url".to_string(),
                "https://private.example/k".to_string(),
            ),
            ("unused".to_string(), "x".to_string()),
        ]);
        apply_secrets("horizon", &mut cfg, &secrets);
        assert_eq!(
            cfg.nest.rpc_urls,
            vec!["https://private.example/k".to_string()]
        );
    }

    fn strings(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    fn outcome(held: &[&str], acq: &[&str], rel: &[&str]) -> TickOutcome {
        TickOutcome {
            held: strings(held),
            acquired: strings(acq),
            released: strings(rel),
            ..Default::default()
        }
    }

    impl Ingesting for tokio::task::AbortHandle {
        fn abort(&self) {
            tokio::task::AbortHandle::abort(self);
        }
    }

    /// Held state is what the tick saw in the leases, never what this worker remembers acquiring: a
    /// lease can be taken from under a worker between ticks, and a memory cannot see that.
    #[test]
    fn held_state_follows_what_the_tick_saw() {
        let mut running = Running::<tokio::task::AbortHandle>::default();
        let held = settle(
            &outcome(&["mainnet"], &["mainnet"], &[]),
            &[],
            &mut running,
            "w1",
        );
        assert_eq!(held, vec!["mainnet"]);

        let held = settle(
            &outcome(&["arbitrum-one", "mainnet"], &["arbitrum-one"], &[]),
            &held,
            &mut running,
            "w1",
        );
        assert_eq!(held, vec!["arbitrum-one", "mainnet"]);

        let held = settle(
            &outcome(&["arbitrum-one"], &[], &["mainnet"]),
            &held,
            &mut running,
            "w1",
        );
        assert_eq!(held, vec!["arbitrum-one"], "a release drops it");

        // Contended is not held, so shutdown will not try to release a lease this worker never had.
        let mut o = outcome(&[], &[], &[]);
        o.contended = strings(&["mainnet"]);
        assert!(settle(&o, &[], &mut running, "w1").is_empty());
    }

    /// #1934: a cursor the tick no longer holds - its lease taken by another worker, with or without
    /// a refused renewal - leaves `held` on that tick and its ingestion task is aborted, while the
    /// cursor this worker still holds keeps running.
    #[tokio::test]
    async fn a_lost_cursor_is_dropped_and_its_nests_stopped() {
        let mut running = Running::<tokio::task::AbortHandle>::default();
        let mainnet = tokio::spawn(std::future::pending::<()>());
        let arb = tokio::spawn(std::future::pending::<()>());
        running
            .by_chain
            .insert("mainnet".into(), vec![mainnet.abort_handle()]);
        running
            .by_chain
            .insert("arbitrum-one".into(), vec![arb.abort_handle()]);

        let before = strings(&["arbitrum-one", "mainnet"]);
        let mut o = outcome(&["arbitrum-one"], &[], &[]);
        o.lost = strings(&["mainnet"]);
        let held = settle(&o, &before, &mut running, "w1");

        assert_eq!(held, vec!["arbitrum-one"]);
        let stopped = tokio::time::timeout(std::time::Duration::from_secs(5), mainnet)
            .await
            .expect("the lost cursor's task must be aborted on this tick");
        assert!(stopped.unwrap_err().is_cancelled());
        assert!(!running.by_chain.contains_key("mainnet"));

        tokio::task::yield_now().await;
        assert!(!arb.is_finished(), "the cursor still held keeps running");
        assert!(running.by_chain.contains_key("arbitrum-one"));
        arb.abort();
    }

    /// The lease must outlive several missed ticks, or one control-plane blip hands a cursor away from
    /// a worker that is still healthily indexing it.
    #[test]
    fn the_lease_ttl_is_several_ticks_long() {
        let ttl = TICK_INTERVAL.as_secs() * LEASE_TTL_TICKS;
        assert!(
            ttl >= TICK_INTERVAL.as_secs() * 3,
            "a {ttl}s lease on a {}s tick is too tight to survive a hiccup",
            TICK_INTERVAL.as_secs()
        );
    }
}
