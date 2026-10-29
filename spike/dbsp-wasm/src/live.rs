//! RFC-0075 S3: a young contract followed from its deploy block over the network, through the
//! node's RPC client on `fetch`, sealing past finality as the node does.

use crate::cellar::Cellar;
use crate::derived::NestFiles;
use crate::query::{sorted, view_names, TabQuery};
use crate::tab::worker::opfs_db;
use crate::tab::{erc20, Sealing, TabNest, TRANSFER};
use nuthatch_core::backend::Bytes;
use nuthatch_core::fetch::Fetch;
use nuthatch_core::opfs::OpenError;
use nuthatch_core::rpc::RpcClient;
use nuthatch_core::store::SEALED_THROUGH_KEY;
use nuthatch_core::window::Finality;
use serde_json::{json, Value};
use std::sync::Arc;
use wasm_bindgen::prelude::*;

// Mainnet's policy, as `src/chains.rs` gives the node.
const MAINNET_FINALITY: u64 = 64;
const MAINNET_SEAL_SPAN: u64 = 1_800;
const MAINNET_LOG_WINDOW: u64 = 20;

#[wasm_bindgen]
pub struct LiveNest {
    nest: TabNest,
    cellar: Arc<Cellar>,
    rpc: RpcClient<Fetch>,
    backend: &'static str,
    files: NestFiles,
}

fn js(e: impl std::fmt::Display) -> JsError {
    JsError::new(&e.to_string())
}

#[wasm_bindgen]
impl LiveNest {
    /// A nest whose store is `<nid>/nuthatch.redb` in OPFS, or a byte buffer where the worker has no
    /// OPFS (Node), as `PlayNest` opens it.
    pub async fn open(
        nid: String,
        rpc_url: String,
        contract: String,
        start: u64,
    ) -> Result<LiveNest, JsError> {
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
        let cellar = Arc::new(Cellar::default());
        let nest = TabNest::open_sealing(
            db,
            erc20(&contract).map_err(js)?,
            vec![contract],
            vec![TRANSFER.into()],
            start,
            MAINNET_LOG_WINDOW,
            Sealing {
                finality: Finality::Depth(MAINNET_FINALITY),
                seal_span: MAINNET_SEAL_SPAN,
                sinks: cellar.clone(),
            },
        )
        .map_err(js)?;
        let rpc = RpcClient::new(vec![rpc_url]).map_err(js)?;
        Ok(LiveNest {
            nest,
            cellar,
            rpc,
            backend,
            files: NestFiles::new(),
        })
    }

    /// Follow the chain to its tip, then report what the gate compares, in the node's shapes.
    pub async fn follow(&self) -> Result<String, JsError> {
        self.nest
            .follow(&self.rpc)
            .await
            .map_err(|e| js(format!("{e:#}")))?;
        let sealed_through: Option<u64> = self
            .nest
            .store
            .get_meta(SEALED_THROUGH_KEY)
            .map_err(js)?
            .and_then(|v| v.parse().ok());
        let balances = &self.nest.balances;
        let items: Vec<_> = balances
            .top(1000)
            .into_iter()
            .map(|(address, balance)| json!({"address": address, "balance": balance.to_string()}))
            .collect();
        Ok(json!({
            "segments": self.cellar.report(),
            "sealed_through": sealed_through,
            "last_block": self.nest.last_block().map_err(js)?,
            "balances": {
                "holders": balances.holders(),
                "count": items.len(),
                "items": items,
                "dropped_over_i128": balances.dropped_over_i128(),
            },
            "rpc_requests": self.rpc.request_count(),
            "segment_bytes": self.cellar.bytes(),
            "first_rows": self.cellar.first_rows(),
            "hot_rows": self.nest.store.count().map_err(js)?,
            "backend": self.backend,
        })
        .to_string())
    }

    /// RFC-0075 S4: start the entities a scaffolded nest declares. `files_json` maps each authored
    /// path (`entities.toml`, `entities/*.sql`, `views/*.sql`) to its text.
    pub fn derive(&mut self, files_json: String) -> Result<(), JsError> {
        let files: NestFiles = serde_json::from_str(&files_json).map_err(js)?;
        self.nest.derive(&files).map_err(|e| js(format!("{e:#}")))?;
        self.files = files;
        Ok(())
    }

    /// What S4's gate compares, in the node's shapes: `/derived/{entity}?limit=1000` for each entity
    /// and `SELECT * FROM <view>` for each authored view, each in a fixed order.
    pub async fn answer(&self) -> Result<String, JsError> {
        let q = TabQuery::bind(
            &self.nest.store,
            &self.cellar,
            &self.nest.registry().schema(),
            &self.nest.entities,
            &self.files,
        )
        .await
        .map_err(|e| js(format!("{e:#}")))?;
        let mut derived = serde_json::Map::new();
        for e in &self.nest.entities {
            let d = e.derived(1000);
            let items = sorted(d["items"].as_array().cloned().unwrap_or_default());
            derived.insert(e.name_lower(), json!({"rows": d["rows"], "items": items}));
        }
        let mut views = serde_json::Map::new();
        for name in view_names(&self.files).map_err(js)? {
            let rows = q
                .rows(&format!("SELECT * FROM {name}"))
                .await
                .map_err(|e| js(format!("{name}: {e:#}")))?;
            views.insert(name, Value::Array(sorted(rows)));
        }
        let sealed_through: Option<u64> = self
            .nest
            .store
            .get_meta(SEALED_THROUGH_KEY)
            .map_err(js)?
            .and_then(|v| v.parse().ok());
        Ok(json!({
            "last_block": self.nest.last_block().map_err(js)?,
            "sealed_through": sealed_through,
            "derived": derived,
            "views": views,
        })
        .to_string())
    }
}
