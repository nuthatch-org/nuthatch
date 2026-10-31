//! RFC-0075 S3's young contract, opened and reported the same way whichever host carries its RPC:
//! `fetch` in `live.rs`, or a host answering requests in `plain/` (RFC-0077 S0).

use crate::budget::TabBudget;
use crate::cellar::Cellar;
use crate::folds::Folding;
use crate::nest::NestSpec;
use crate::tab::{Sealing, TabNest};
use anyhow::Result;
use nuthatch_core::redb_store::Store;
use nuthatch_core::rpc::RpcClient;
use nuthatch_core::store::SEALED_THROUGH_KEY;
use nuthatch_core::transport::Transport;
use serde_json::{json, Value};
use std::sync::Arc;

/// The binary's version, which `/ready` reports: the tab is built from the same tree.
pub fn node_version() -> &'static str {
    include_str!("../../Cargo.toml")
        .lines()
        .find_map(|l| l.strip_prefix("version = \"")?.strip_suffix('"'))
        .unwrap_or("unknown")
}

/// `contract`'s `Transfer`s from `start` into `db`, sealing into the returned cellar, which keeps
/// its segments in `db` too. A warm `db` comes back with its views rebuilt (RFC-0077 S3).
pub fn open(db: redb::Database, contract: &str, start: u64) -> Result<(TabNest, Arc<Cellar>)> {
    let store = Store::from_db(db, None)?;
    let cellar = Arc::new(Cellar::over(store.clone())?);
    let spec = NestSpec::erc20(contract, start)?;
    let nest = open_nest(store, cellar.clone(), spec, start, TabBudget::default())?;
    Ok((nest, cellar))
}

/// The nest `spec` describes over `store`, from `start` unless the store has begun, sealing into
/// `cellar` on its chain's policy (RFC-0077 S4). A warm store's views are rebuilt through the cold
/// folds, as a node's are, each statement held to `budget`'s share.
pub fn open_nest(
    store: Store,
    cellar: Arc<Cellar>,
    spec: NestSpec,
    start: u64,
    budget: TabBudget,
) -> Result<TabNest> {
    let history = Folding::new(
        cellar.clone(),
        spec.registry.schema(),
        spec.labels.clone(),
        budget,
    );
    let sealing = Sealing {
        finality: spec.chain.finality,
        seal_span: spec.chain.seal_span,
        sinks: cellar,
        history: Some(Arc::new(history)),
        verified: None,
    };
    TabNest::open_sealing(store, spec, start, sealing)
}

/// What S3's gate compares, in the node's shapes, after a follow that resumed from `rolled`.
pub fn report<T: Transport>(
    nest: &TabNest,
    cellar: &Cellar,
    rpc: &RpcClient<T>,
    rolled: Option<u64>,
    backend: &str,
) -> Result<Value> {
    let sealed_through: Option<u64> = nest
        .store
        .get_meta(SEALED_THROUGH_KEY)?
        .and_then(|v| v.parse().ok());
    let balances = &nest.balances;
    let items: Vec<_> = balances
        .top(1000)
        .into_iter()
        .map(|(address, balance)| json!({"address": address, "balance": balance.to_string()}))
        .collect();
    Ok(json!({
        "segments": cellar.report(),
        "sealed_through": sealed_through,
        "last_block": nest.last_block()?,
        "balances": {
            "holders": balances.holders(),
            "count": items.len(),
            "items": items,
            "dropped_over_i128": balances.dropped_over_i128(),
        },
        "rpc_requests": rpc.request_count(),
        "segment_bytes": cellar.bytes(),
        "first_rows": cellar.first_rows(),
        "hot_rows": nest.store.count()?,
        "resumed_from": rolled,
        "backend": backend,
    }))
}
