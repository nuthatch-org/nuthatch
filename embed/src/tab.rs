//! RFC-0075 S2: a nest as the `/play` worker runs it. `nuthatch-core`'s redb store over whichever
//! backend the worker opened, the three built-in views driven inline, and the node's tip loop:
//! verify the last checkpoint, roll back to the surviving ancestor, index window by window.

use crate::derived::{NestFiles, TabEntity};
use crate::verified::{VerifiedHead, Verifier};
use crate::window::{NoWait, Shelf};
use anyhow::{ensure, Context, Result};
use nuthatch_core::alerts::{AlertRouter, AlertSink};
use nuthatch_core::cursor::{
    detect_reorg, process_window, rollback_reorg, Nest, Processed, Window,
};
use nuthatch_core::drive::Driver;
use nuthatch_core::exposure::ExposureView;
use nuthatch_core::factory::{ChildRegistry, FactorySet};
use nuthatch_core::labels::LabelSet;
use nuthatch_core::metrics::{CursorMetrics, UndecodedTally};
use nuthatch_core::redb_store::Store;
use nuthatch_core::restart::{self, DerivedViews, SealedHistory};
use nuthatch_core::seal::Sinks;
use nuthatch_core::source::Source;
use nuthatch_core::store::{LAST_BLOCK_KEY, START_BLOCK_KEY};
use nuthatch_core::velocity::VelocityView;
use nuthatch_core::views::BalanceView;
use nuthatch_core::window::{fetch_window, Finality, Wanted};
use nuthatch_decode::registry::DecodeRegistry;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Mutex;

pub struct TabNest {
    pub store: Store,
    registry: DecodeRegistry,
    addresses: Vec<String>,
    topic0s: Vec<String>,
    /// A factory nest's rules, and the children they have found (RFC-0009).
    factory: Option<FactorySet>,
    /// Held through a follow, which awaits the source with it borrowed.
    children: futures::lock::Mutex<ChildRegistry>,
    threshold: Option<i128>,
    velocity_cfg: Option<(i128, u64)>,
    pub balances: BalanceView,
    exposure: ExposureView,
    velocity: VelocityView,
    labels: LabelSet,
    router: AlertRouter,
    sealing: Sealing,
    window: u64,
    /// RFC-0077 S7: asked in place of `addresses`' `topic0s` when the host narrowed the nest.
    narrowed: Option<Vec<crate::narrow::Narrowed>>,
    until: Option<u64>,
    pub entities: Vec<TabEntity>,
    pub cursor: Cursor,
    pub budget: crate::budget::TabBudget,
}

/// What `/ready` reports of the cursor: the head it last saw and when, when the seal last moved,
/// and the logs a decoder claimed and could not decode.
#[derive(Default)]
pub struct Cursor {
    pub tip: AtomicU64,
    pub last_poll: AtomicU64,
    pub last_seal_progress: AtomicU64,
    pub undecoded: Mutex<UndecodedTally>,
}

pub fn unix_now() -> u64 {
    nuthatch_core::time::system_now()
        .duration_since(nuthatch_core::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

impl CursorMetrics for Cursor {
    fn set_sealed_through(&self, _block: u64) {
        self.last_seal_progress.store(unix_now(), Relaxed);
    }

    fn add_undecoded(&self, contract: &str, topic0: &str, n: u64) {
        self.undecoded
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .add(contract, topic0, n);
    }
}

/// When and where a tab nest seals.
pub struct Sealing {
    pub finality: Finality,
    pub seal_span: u64,
    pub sinks: std::sync::Arc<dyn Sinks>,
    /// What a warm open rebuilds the views and entities from (RFC-0077 S3).
    pub history: Option<std::sync::Arc<dyn SealedHistory + Send + Sync>>,
    /// A finalized block the host verified, which every seal is held to (RFC-0077 S5).
    pub verified: Option<Verified>,
}

pub struct Verified {
    pub verifier: std::sync::Arc<Verifier>,
    pub head: std::sync::Arc<dyn VerifiedHead + Send + Sync>,
}

impl Sealing {
    /// S2's: nothing finalizes, so nothing seals.
    pub fn never() -> Sealing {
        Sealing {
            finality: Finality::Depth(1_000_000_000),
            seal_span: 1,
            sinks: std::sync::Arc::new(Shelf::default()),
            history: None,
            verified: None,
        }
    }
}

impl TabNest {
    /// A nest over `db`, indexing `addresses`' `topic0s` from `start` in windows of `window` blocks.
    /// Nothing seals yet (S3): a reorg below the sealed watermark is a terminal fault by design.
    pub fn open(
        db: redb::Database,
        registry: DecodeRegistry,
        addresses: Vec<String>,
        topic0s: Vec<String>,
        start: u64,
        window: u64,
    ) -> Result<TabNest> {
        let mut spec = crate::nest::NestSpec::of_registry(registry, addresses, topic0s, start);
        spec.chain.log_window = window;
        Self::open_sealing(Store::from_db(db, None)?, spec, start, Sealing::never())
    }

    /// The nest `spec` describes over a store already opened, from `start` unless the store has
    /// begun already, sealing past `sealing.finality` as a node does. A warm store's views and a
    /// factory's children are rebuilt from `sealing.history` and the hot tail, as a node's are.
    pub fn open_sealing(
        store: Store,
        spec: crate::nest::NestSpec,
        start: u64,
        sealing: Sealing,
    ) -> Result<TabNest> {
        if store.get_meta(START_BLOCK_KEY)?.is_none() {
            store.set_meta(START_BLOCK_KEY, &start.to_string())?;
        }
        let nest = TabNest {
            store,
            balances: BalanceView::start(Driver::Inline)?,
            exposure: ExposureView::start(Driver::Inline, !spec.labels.is_empty())?,
            velocity: VelocityView::start(Driver::Inline, spec.velocity.is_some())?,
            registry: spec.registry,
            addresses: spec.addresses,
            topic0s: spec.topic0s,
            factory: spec.factory,
            children: futures::lock::Mutex::new(ChildRegistry::new()),
            threshold: spec.threshold,
            velocity_cfg: spec.velocity,
            labels: spec.labels,
            router: AlertRouter::new(Vec::<AlertSink>::new()),
            sealing,
            window: spec.chain.log_window.max(1),
            narrowed: spec.narrowed,
            until: spec.until,
            entities: Vec::new(),
            cursor: Cursor::default(),
            budget: crate::budget::TabBudget::default(),
        };
        let mut nest = nest;
        if let (Some(history), Some(_)) = (&nest.sealing.history, nest.last_block()?) {
            let views = DerivedViews {
                labels: &nest.labels,
                balances: &nest.balances,
                exposure: &nest.exposure,
                velocity: &nest.velocity,
                velocity_cfg: nest.velocity_cfg,
            };
            restart::rebuild_views(&**history, &nest.store, &nest.registry, &views)
                .context("rebuilding the views from this nest's stored history")?;
            if let Some(factory) = &nest.factory {
                *nest.children.get_mut() =
                    restart::rebuild_children(&**history, &nest.store, factory).context(
                        "rebuilding the factory's children from this nest's stored history",
                    )?;
            }
        }
        Ok(nest)
    }

    /// Start the entities `files` declares (RFC-0075 S4). A store that has indexed already is warm:
    /// its entities are seeded from `sealing.history` and the hot tail as a node seeds them, and
    /// without a history they stay unavailable, as on a node whose seed did not complete.
    pub fn derive(&mut self, files: &NestFiles) -> Result<()> {
        let through = self.last_block()?;
        self.entities =
            crate::derived::start(files, &self.registry, through.is_some(), self.budget)?;
        if let (Some(history), Some(through)) = (&self.sealing.history, through) {
            restart::seed_entities(
                &**history,
                &self.store,
                &self.registry,
                &mut self.entities,
                through,
                &mut |_| Ok(()),
            )
            .context("seeding the entities from this nest's stored history")?;
        }
        Ok(())
    }

    /// Hold every seal from here on to the finalized block `head` gives (RFC-0077 S5).
    pub fn verify_with(&mut self, verified: Verified) {
        self.sealing.verified = Some(verified);
    }

    /// A factory nest's `{template}__children` views, as the node binds them for `/sql`.
    pub fn children_views(&self) -> Vec<(String, String)> {
        self.factory.as_ref().map_or_else(Vec::new, |f| {
            nuthatch_core::bind::children_views(f, self.registry.timestamps())
        })
    }

    /// The getLogs span the cursor asks for, which `/ready` reports.
    pub fn log_window(&self) -> u64 {
        self.window
    }

    pub fn registry(&self) -> &DecodeRegistry {
        &self.registry
    }

    pub fn last_block(&self) -> Result<Option<u64>> {
        Ok(self
            .store
            .get_meta(LAST_BLOCK_KEY)?
            .and_then(|v| v.parse().ok()))
    }

    fn cursor_nest<'a>(
        &'a self,
        schema: &'a [nuthatch_decode::registry::TableSchema],
        children: Option<&'a mut ChildRegistry>,
    ) -> Nest<'a, TabEntity> {
        Nest {
            name: "tab",
            store: &self.store,
            schema,
            balances: &self.balances,
            exposure: &self.exposure,
            velocity: &self.velocity,
            labels: &self.labels,
            velocity_flag: self.velocity_cfg,
            entities: &self.entities,
            children,
            router: &self.router,
            metrics: &self.cursor,
        }
    }

    /// Follow `tape` until it has nothing new. Returns the block the last rollback resumed from, if
    /// one ran.
    pub async fn follow(&self, tape: &dyn Source) -> Result<Option<u64>> {
        let schema = self.registry.schema();
        let mut children = self.children.lock().await;
        let factory = self.factory.as_ref();
        let start: u64 = self
            .store
            .get_meta(START_BLOCK_KEY)?
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let mut topic0_refused = false;
        let mut rolled = None;
        let verified = self.sealing.verified.as_ref();
        let top = match verified {
            Some(v) => Some(v.head.finalized().await),
            None => None,
        };
        if let Some(v) = verified {
            v.verifier.begin();
            if let (true, Some(top)) = (v.verifier.completes(), &top) {
                if let Ok((n, _)) = top {
                    v.verifier.within_ceiling(*n, &self.store)?;
                }
                // RFC-0081 S4: what an earlier follow indexed above the verified head it then had, or
                // before a reopen, is indexed again now that it can be checked.
                v.verifier.refresh(top, tape, &self.store).await;
                let through = match (self.last_block()?, top) {
                    (Some(last), Ok((n, _))) => Some(last.min(*n)),
                    _ => None,
                };
                if let Some(b) =
                    through.and_then(|t| v.verifier.unchecked(&self.store, t).transpose())
                {
                    let ancestor = b? - 1;
                    tape.forget_cached_above(ancestor);
                    v.verifier.forget_above(ancestor);
                    rolled = Some(rollback_reorg(
                        self.cursor_nest(&schema, factory.map(|_| &mut *children)),
                        ancestor,
                    )?);
                }
            }
        }
        // One pass ends at the head it first saw. Arbitrum makes four blocks a second, and a nest
        // that asks many filters a window would otherwise chase the head and never be idle.
        let mut target: Option<u64> = None;
        if let (Some(until), Some(last)) = (self.until, self.last_block()?) {
            ensure!(
                last <= until,
                "the store has indexed to block {last}, past `until` {until}; open it fresh"
            );
        }
        loop {
            let mut next = self.last_block()?.map_or(start, |b| b + 1);
            if next > 0 {
                if let Some(ancestor) = detect_reorg(tape, &self.store, next - 1).await? {
                    // As the node does: the timestamp cache is keyed by height, and every block above
                    // `ancestor` was replaced (RFC-0029 §6d). Found by RFC-0077 S5's reorg shapes.
                    tape.forget_cached_above(ancestor);
                    if let Some(v) = verified {
                        v.verifier.forget_above(ancestor);
                    }
                    next = rollback_reorg(
                        self.cursor_nest(&schema, factory.map(|_| &mut *children)),
                        ancestor,
                    )?;
                    rolled = Some(next);
                }
            }
            let tip = tape.tip().await?;
            self.cursor.tip.store(tip, Relaxed);
            self.cursor.last_poll.store(unix_now(), Relaxed);
            let target = *target.get_or_insert(self.until.map_or(tip, |u| u.min(tip)));
            if next > tip.min(target) {
                self.balances.flush();
                ensure!(self.balances.is_healthy(), "the balance view died");
                for e in &self.entities {
                    if let Some(f) = nuthatch_core::entity_view::WindowEntity::fault(e) {
                        anyhow::bail!("entity `{}` stopped: {f}", e.name_lower());
                    }
                }
                return Ok(rolled);
            }
            self.budget.check_memory(crate::budget::linear_memory())?;
            let to = (next + self.window - 1).min(tip.min(target));
            let wanted = Wanted {
                registry: &self.registry,
                factory,
                children: &children,
                addresses: &self.addresses,
                topic0s: &self.topic0s,
                topic0_refused: &mut topic0_refused,
            };
            let mut logs = match &self.narrowed {
                Some(n) => crate::narrow::fetch(n, tape, next, to).await?,
                None => fetch_window(wanted, tape, next, to).await?,
            };
            // Again once the logs are held, before anything of the window is committed.
            self.budget.check_memory(crate::budget::linear_memory())?;
            let extra: &dyn nuthatch_core::extra::ExtraRows = match (verified, &top) {
                (Some(v), Some(top)) => {
                    v.verifier.refresh(top, tape, &self.store).await;
                    v.verifier.complete(&mut logs, next, to).await;
                    &*v.verifier
                }
                _ => &nuthatch_core::extra::NoExtraRows,
            };
            let window = Window {
                nest: self.cursor_nest(&schema, factory.map(|_| &mut *children)),
                registry: &self.registry,
                factory,
                threshold: self.threshold,
                finality: self.sealing.finality,
                seal_span: self.sealing.seal_span,
                sinks: &*self.sealing.sinks,
                extra,
                hooks: &nuthatch_core::hooks::NoHooks,
                faults: &nuthatch_core::fault::NoFaults,
                clock: &NoWait,
                deadline: None,
            };
            match process_window(window, tape, &logs, next, to, tip).await? {
                Processed::Stored(_) => {}
                Processed::Rewound(at) => rolled = Some(at),
                Processed::Refused => {}
                Processed::Abandoned => {
                    anyhow::bail!("window {next}..={to} was abandoned with no deadline set")
                }
            }
        }
    }

    /// What the page shows: the cursor, the hot rows and the balances.
    pub fn status(&self, rolled: Option<u64>) -> Result<String> {
        Ok(serde_json::json!({
            "last_block": self.last_block()?,
            "hot_rows": self.store.count()?,
            "resumed_from": rolled,
            "balances": self.balances.top(10),
        })
        .to_string())
    }
}

/// The registry for one ERC-20 at `contract`, the shape a recorded `/play` chain carries.
pub fn erc20(contract: &str) -> Result<DecodeRegistry> {
    DecodeRegistry::build(vec![nuthatch_decode::registry::ContractSpec {
        alias: "tab".into(),
        address: contract.parse()?,
        abi: serde_json::from_str(
            r#"[{"type":"event","name":"Transfer","anonymous":false,"inputs":[
                {"name":"from","type":"address","indexed":true},
                {"name":"to","type":"address","indexed":true},
                {"name":"value","type":"uint256","indexed":false}]}]"#,
        )?,
        events: Vec::new(),
    }])
}

pub const TRANSFER: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

/// The `/play` worker's handle on a nest: its store in OPFS under the NID, or in a byte buffer where
/// the worker has no sync access handles, fed by the recorded chains the page plays to it.
#[cfg(all(target_arch = "wasm32", feature = "js"))]
pub mod worker {
    use super::{erc20, TabNest, TRANSFER};
    use crate::tape::TapeSource;
    use nuthatch_core::backend::Bytes;
    use nuthatch_core::opfs::{self, OpenError, OpfsBackend};
    use wasm_bindgen::prelude::*;

    #[wasm_bindgen]
    pub struct PlayNest {
        nest: TabNest,
        backend: &'static str,
    }

    fn js(e: impl std::fmt::Display) -> JsError {
        JsError::new(&e.to_string())
    }

    #[wasm_bindgen]
    impl PlayNest {
        /// Open nest `nid`'s store at `<nid>/nuthatch.redb` in OPFS. A store another tab holds is
        /// refused with that error's message; no OPFS at all falls back to a byte buffer.
        pub async fn open(nid: String, contract: String, start: u64) -> Result<PlayNest, JsError> {
            let (db, backend) = match opfs_db(&nid).await {
                Ok(db) => (db, "opfs"),
                Err(OpenError::Unavailable(_)) => (
                    redb::Builder::new()
                        .create_with_backend(Bytes::default())
                        .map_err(js)?,
                    "bytes",
                ),
                Err(e) => return Err(js(e)),
            };
            let registry = erc20(&contract).map_err(js)?;
            let nest = TabNest::open(
                db,
                registry,
                vec![contract],
                vec![TRANSFER.into()],
                start,
                1000,
            )
            .map_err(js)?;
            Ok(PlayNest { nest, backend })
        }

        /// `"opfs"` or `"bytes"`.
        pub fn backend(&self) -> String {
            self.backend.into()
        }

        /// Follow the chain in `chain_json` (a [`crate::tape::Chain`]) as far as its tip. A chain whose
        /// blocks differ from the ones indexed is a reorg, and the nest rolls back as a node would.
        pub async fn play(&self, chain_json: String) -> Result<String, JsError> {
            let tape = TapeSource::from_json(&chain_json).map_err(js)?;
            let rolled = self
                .nest
                .follow(&tape)
                .await
                .map_err(|e| js(format!("{e:#}")))?;
            self.nest.status(rolled).map_err(js)
        }
    }

    pub(crate) async fn opfs_db(nid: &str) -> Result<redb::Database, OpenError> {
        let dir = opfs::directory(&opfs::root().await?, nid).await?;
        let backend = OpfsBackend::open(&dir, "nuthatch.redb").await?;
        redb::Builder::new()
            .create_with_backend(backend)
            .map_err(|e| OpenError::Js(e.to_string()))
    }
}
