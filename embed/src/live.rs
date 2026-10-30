//! RFC-0075 S3: a young contract followed from its deploy block over the network, through the
//! node's RPC client on `fetch`, sealing past finality as the node does.

use crate::cellar::Cellar;
use crate::derived::NestFiles;
use crate::query::{sorted, view_names, TabQuery};
use crate::tab::worker::opfs_db;
use crate::tab::TabNest;
use nuthatch_core::backend::Bytes;
use nuthatch_core::fetch::Fetch;
use nuthatch_core::opfs::OpenError;
use nuthatch_core::rpc::RpcClient;
use nuthatch_core::store::SEALED_THROUGH_KEY;
use serde_json::{json, Value};
use std::sync::Arc;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct LiveNest {
    nest: TabNest,
    cellar: Arc<Cellar>,
    rpc: RpcClient<Fetch>,
    backend: &'static str,
    files: NestFiles,
    nid: String,
    poll_interval_secs: Option<u64>,
}

/// One route's answer, as the Service Worker builds a `Response` from it.
#[wasm_bindgen]
pub struct HttpAnswer {
    status: u16,
    content_type: &'static str,
    body: Vec<u8>,
}

#[wasm_bindgen]
impl HttpAnswer {
    #[wasm_bindgen(getter)]
    pub fn status(&self) -> u16 {
        self.status
    }

    /// Empty where the node sends no content type.
    #[wasm_bindgen(getter, js_name = contentType)]
    pub fn content_type(&self) -> String {
        self.content_type.into()
    }

    #[wasm_bindgen(getter)]
    pub fn body(&self) -> Vec<u8> {
        self.body.clone()
    }

    /// The body as text, which every route's is.
    #[wasm_bindgen(getter)]
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

fn encode(s: &str) -> String {
    percent_encoding::utf8_percent_encode(s, percent_encoding::NON_ALPHANUMERIC).to_string()
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
        let (nest, cellar) = crate::young::open(db, &contract, start).map_err(js)?;
        let rpc = RpcClient::new(vec![rpc_url]).map_err(js)?;
        Ok(LiveNest {
            nest,
            cellar,
            rpc,
            backend,
            files: NestFiles::new(),
            nid,
            poll_interval_secs: None,
        })
    }

    /// How often the page follows the chain, which `/ready` reports as a node's `--poll-interval`.
    #[wasm_bindgen(js_name = setPollInterval)]
    pub fn set_poll_interval(&mut self, secs: u64) {
        self.poll_interval_secs = Some(secs);
    }

    /// RFC-0075 S6: the memory ceiling in MiB, 2,048 unless set; a phone's is 600. Set it before
    /// `derive`, which admits entities against it. The allocator holds the whole module to it.
    #[wasm_bindgen(js_name = setMemoryCeiling)]
    pub fn set_memory_ceiling(&mut self, mib: u32) {
        self.nest.budget = crate::budget::TabBudget::mib(mib);
        self.nest.budget.enforce();
    }

    /// RFC-0075 S5: `method` and `target` (path and query) answered as the node's router answers
    /// them, from the same `nuthatch_core::answer` functions.
    pub async fn request(&self, method: String, target: String, body: Vec<u8>) -> HttpAnswer {
        let nest = crate::http::Nest {
            nest: &self.nest,
            cellar: &self.cellar,
            files: &self.files,
            nid: Some(&self.nid),
            version: crate::young::node_version(),
            poll_interval_secs: self.poll_interval_secs,
            log_window: self.nest.log_window(),
        };
        let a = crate::http::answer(&nest, &method, &target, &body).await;
        HttpAnswer {
            status: a.status,
            content_type: crate::http::content_type(&a),
            body: a.body,
        }
    }

    /// `GET /sql?q=…`'s body.
    pub async fn sql(&self, q: String) -> String {
        let target = format!("/sql?q={}", encode(&q));
        self.request("GET".into(), target, Vec::new()).await.text()
    }

    /// `GET /entity/{id}`'s body.
    pub async fn entity(&self, id: String) -> String {
        let target = format!("/entity/{}", encode(&id));
        self.request("GET".into(), target, Vec::new()).await.text()
    }

    /// `GET /derived/{name}`'s body, or `/derived/{name}/{key}`'s with a key.
    pub async fn derived(&self, name: String, key: Option<String>) -> String {
        let mut target = format!("/derived/{}", encode(&name));
        if let Some(key) = key {
            target = format!("{target}/{}", encode(&key));
        }
        self.request("GET".into(), target, Vec::new()).await.text()
    }

    /// Follow the chain to its tip, then report what the gate compares, in the node's shapes.
    pub async fn follow(&self) -> Result<String, JsError> {
        let rolled = self
            .nest
            .follow(&self.rpc)
            .await
            .map_err(|e| js(format!("{e:#}")))?;
        let report =
            crate::young::report(&self.nest, &self.cellar, &self.rpc, rolled, self.backend)
                .map_err(js)?;
        Ok(report.to_string())
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
            self.nest.budget,
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
