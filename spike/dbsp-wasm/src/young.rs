//! RFC-0075 S3's young contract, opened and reported the same way whichever host carries its RPC:
//! `fetch` in `live.rs`, or a host answering requests in `plain/` (RFC-0077 S0).

use crate::cellar::Cellar;
use crate::tab::{erc20, Sealing, TabNest, TRANSFER};
use anyhow::Result;
use nuthatch_core::rpc::RpcClient;
use nuthatch_core::store::SEALED_THROUGH_KEY;
use nuthatch_core::transport::Transport;
use nuthatch_core::window::Finality;
use serde_json::{json, Value};
use std::sync::Arc;

// Mainnet's policy, as `src/chains.rs` gives the node.
pub const MAINNET_FINALITY: u64 = 64;
pub const MAINNET_SEAL_SPAN: u64 = 1_800;
pub const MAINNET_LOG_WINDOW: u64 = 20;

/// The binary's version, which `/ready` reports: the tab is built from the same tree.
pub fn node_version() -> &'static str {
    include_str!("../../../Cargo.toml")
        .lines()
        .find_map(|l| l.strip_prefix("version = \"")?.strip_suffix('"'))
        .unwrap_or("unknown")
}

/// `contract`'s `Transfer`s from `start` into `db`, sealing into the returned cellar.
pub fn open(db: redb::Database, contract: &str, start: u64) -> Result<(TabNest, Arc<Cellar>)> {
    let cellar = Arc::new(Cellar::default());
    let nest = TabNest::open_sealing(
        db,
        erc20(contract)?,
        vec![contract.to_string()],
        vec![TRANSFER.into()],
        start,
        MAINNET_LOG_WINDOW,
        Sealing {
            finality: Finality::Depth(MAINNET_FINALITY),
            seal_span: MAINNET_SEAL_SPAN,
            sinks: cellar.clone(),
        },
    )?;
    Ok((nest, cellar))
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
