//! RFC-0075 S2: a nest as the `/play` worker runs it. `nuthatch-core`'s redb store over whichever
//! backend the worker opened, the three built-in views driven inline, and the node's tip loop:
//! verify the last checkpoint, roll back to the surviving ancestor, index window by window.

use crate::derived::{NestFiles, TabEntity};
use crate::window::{NoWait, Shelf};
use anyhow::{ensure, Result};
use nuthatch_core::alerts::{AlertRouter, AlertSink};
use nuthatch_core::cursor::{
    detect_reorg, process_window, rollback_reorg, Nest, Processed, Window,
};
use nuthatch_core::drive::Driver;
use nuthatch_core::exposure::ExposureView;
use nuthatch_core::labels::LabelSet;
use nuthatch_core::metrics::{CursorMetrics, UndecodedTally};
use nuthatch_core::redb_store::Store;
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
    pub balances: BalanceView,
    exposure: ExposureView,
    velocity: VelocityView,
    labels: LabelSet,
    router: AlertRouter,
    sealing: Sealing,
    window: u64,
    pub entities: Vec<TabEntity>,
    pub cursor: Cursor,
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
        .duration_since(std::time::UNIX_EPOCH)
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
}

impl Sealing {
    /// S2's: nothing finalizes, so nothing seals.
    pub fn never() -> Sealing {
        Sealing {
            finality: Finality::Depth(1_000_000_000),
            seal_span: 1,
            sinks: std::sync::Arc::new(Shelf::default()),
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
        Self::open_sealing(
            db,
            registry,
            addresses,
            topic0s,
            start,
            window,
            Sealing::never(),
        )
    }

    /// [`Self::open`], sealing past `sealing.finality` as a node does.
    pub fn open_sealing(
        db: redb::Database,
        registry: DecodeRegistry,
        addresses: Vec<String>,
        topic0s: Vec<String>,
        start: u64,
        window: u64,
        sealing: Sealing,
    ) -> Result<TabNest> {
        let store = Store::from_db(db, None)?;
        if store.get_meta(START_BLOCK_KEY)?.is_none() {
            store.set_meta(START_BLOCK_KEY, &start.to_string())?;
        }
        Ok(TabNest {
            store,
            registry,
            addresses,
            topic0s,
            balances: BalanceView::start(Driver::Inline)?,
            exposure: ExposureView::start(Driver::Inline, false)?,
            velocity: VelocityView::start(Driver::Inline, false)?,
            labels: LabelSet::default(),
            router: AlertRouter::new(Vec::<AlertSink>::new()),
            sealing,
            window: window.max(1),
            entities: Vec::new(),
            cursor: Cursor::default(),
        })
    }

    /// Start the entities `files` declares (RFC-0075 S4). A store that has indexed already is warm,
    /// and its entities unavailable, as on a node.
    pub fn derive(&mut self, files: &NestFiles) -> Result<()> {
        let warm = self.last_block()?.is_some();
        self.entities = crate::derived::start(files, &self.registry, warm)?;
        Ok(())
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

    /// Follow `tape` until it has nothing new. Returns the block the last rollback resumed from, if
    /// one ran.
    pub async fn follow(&self, tape: &dyn Source) -> Result<Option<u64>> {
        let schema = self.registry.schema();
        let nest = || Nest {
            name: "tab",
            store: &self.store,
            schema: &schema,
            balances: &self.balances,
            exposure: &self.exposure,
            velocity: &self.velocity,
            labels: &self.labels,
            velocity_flag: None,
            entities: &self.entities,
            children: None,
            router: &self.router,
            metrics: &self.cursor,
        };
        let start: u64 = self
            .store
            .get_meta(START_BLOCK_KEY)?
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let mut topic0_refused = false;
        let mut rolled = None;
        loop {
            let mut next = self.last_block()?.map_or(start, |b| b + 1);
            if next > 0 {
                if let Some(ancestor) = detect_reorg(tape, &self.store, next - 1).await? {
                    next = rollback_reorg(nest(), ancestor)?;
                    rolled = Some(next);
                }
            }
            let tip = tape.tip().await?;
            self.cursor.tip.store(tip, Relaxed);
            self.cursor.last_poll.store(unix_now(), Relaxed);
            if next > tip {
                self.balances.flush();
                ensure!(self.balances.is_healthy(), "the balance view died");
                for e in &self.entities {
                    if let Some(f) = nuthatch_core::entity_view::WindowEntity::fault(e) {
                        anyhow::bail!("entity `{}` stopped: {f}", e.name_lower());
                    }
                }
                return Ok(rolled);
            }
            let to = (next + self.window - 1).min(tip);
            let wanted = Wanted {
                registry: &self.registry,
                factory: None,
                children: &nuthatch_core::factory::ChildRegistry::new(),
                addresses: &self.addresses,
                topic0s: &self.topic0s,
                topic0_refused: &mut topic0_refused,
            };
            let logs = fetch_window(wanted, tape, next, to).await?;
            let window = Window {
                nest: nest(),
                registry: &self.registry,
                factory: None,
                threshold: None,
                finality: self.sealing.finality,
                seal_span: self.sealing.seal_span,
                sinks: &*self.sealing.sinks,
                extra: &nuthatch_core::extra::NoExtraRows,
                hooks: &nuthatch_core::hooks::NoHooks,
                faults: &nuthatch_core::fault::NoFaults,
                clock: &NoWait,
                deadline: None,
            };
            match process_window(window, tape, &logs, next, to, tip).await? {
                Processed::Stored(_) => {}
                Processed::Rewound(at) => rolled = Some(at),
                Processed::Refused => {}
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
#[cfg(target_arch = "wasm32")]
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
