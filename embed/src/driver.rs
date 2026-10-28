//! What the binary's rotki-mode cursor does for one pass, ported for a host that calls it: advance the
//! finalized head, then for each watched address discover and record windows up to a block.
//!
//! The checks are the cursor's (`address_mode.rs`): the finalized hash is read on both sides of the
//! held one and must agree; every main and trace endpoint must be on one chain at a window's end,
//! before and after the fetch; every block hash a trace named must be the main endpoint's, twice; and
//! nothing fetched is kept from a window that failed any of them.

use anyhow::{anyhow, bail, Context, Result};
use serde_json::{json, Value};
use std::collections::BTreeMap;

use crate::address_discovery::{self, Counted, Discoverer, Pending, Rpc, OUTER_WINDOW};
use crate::address_history::{AddressHistory, FinalConflict};
use crate::host::{Endpoint, HostRpc};

pub type HostDiscoverer = Discoverer<Counted<HostRpc>, Counted<HostRpc>>;

pub fn discoverer(chain_id: u64) -> HostDiscoverer {
    let mut d = Discoverer::new(
        Counted::new(HostRpc(Endpoint::Main)),
        Counted::new(HostRpc(Endpoint::Trace)),
    );
    d.residual = crate::address_residual::residual_chain(chain_id);
    d.mint_logs = crate::address_residual::mint_logs(chain_id);
    d
}

async fn block_hash(rpc: &impl Rpc, block: u64) -> Result<[u8; 32]> {
    let b = rpc
        .call(
            "eth_getBlockByNumber",
            json!([format!("0x{block:x}"), false]),
        )
        .await?;
    let hash = b
        .get("hash")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("block {block} is not on the main endpoint yet"))?;
    address_discovery::word32(hash)
}

/// Move the store's finalized head to the main endpoint's, holding the one recorded before against it.
pub async fn advance_final(d: &HostDiscoverer, history: &AddressHistory) -> Result<u64> {
    let f = d
        .main
        .call("eth_getBlockByNumber", json!(["finalized", false]))
        .await?;
    let finalized =
        address_discovery::hex_u64(&f["number"]).context("the RPC's finalized block")?;
    let hash = block_hash(&d.main, finalized).await?;
    hold_final(d, history).await?;
    if block_hash(&d.main, finalized).await? != hash {
        bail!("finalized block {finalized} changed hash during the pass; trying again");
    }
    if let Err(e) = history.finalize_at(finalized, hash) {
        if e.downcast_ref::<FinalConflict>().is_some() {
            history.set_conflict(&format!("{e}"))?;
        }
        return Err(e);
    }
    Ok(finalized)
}

async fn hold_final(d: &HostDiscoverer, history: &AddressHistory) -> Result<()> {
    let Some(&(block, hash)) = history.finals()?.last() else {
        return Ok(());
    };
    for _ in 0..2 {
        if block_hash(&d.main, block).await? != hash {
            let why = format!("finalized block {block} is now at another hash");
            history.set_conflict(&why)?;
            bail!(why);
        }
    }
    Ok(())
}

async fn same_chain_at(d: &HostDiscoverer, block: u64) -> Result<()> {
    let (m, t) = futures::try_join!(block_hash(&d.main, block), block_hash(&d.trace, block))?;
    if m != t {
        bail!("the main and trace endpoints are not on one chain at block {block}");
    }
    Ok(())
}

async fn check_named(
    d: &HostDiscoverer,
    history: &AddressHistory,
    named: &[(u64, [u8; 32])],
) -> Result<()> {
    for _ in 0..2 {
        for &(block, hash) in named {
            if block_hash(&d.main, block).await? != hash {
                bail!("block {block} is no longer the one this was read against");
            }
        }
    }
    hold_final(d, history).await
}

/// One window of every discovered action for `address`, recorded only if every check held. Windows
/// above the finalized head are refused: S0 serves finalized history only (RFC-0074 §5).
pub async fn window(
    d: &HostDiscoverer,
    history: &AddressHistory,
    address: &str,
    (from, to): (u64, u64),
    actions: &'static [crate::address_history::Action],
) -> Result<()> {
    if history.head()?.is_none_or(|h| to > h) {
        bail!("block {to} is above the finalized head; the embedded module serves finalized history only");
    }
    let pending = Pending::begin(history)?;
    let generation = pending.generation();
    same_chain_at(d, to).await?;
    let found = d
        .discover(history, address, from, to, actions, &pending)
        .await;
    let checked = match check_named(d, history, &pending.named()).await {
        Ok(()) => same_chain_at(d, to).await,
        Err(e) => Err(e),
    };
    pending.persist(history, checked.is_ok())?;
    checked?;
    address_discovery::record_anchored(
        history,
        address,
        (from, to),
        found?,
        generation,
        None,
        actions,
    )
}

/// What one `sync` did.
#[derive(Debug, Default, serde::Serialize)]
pub struct Report {
    pub finalized: u64,
    pub windows: u64,
    pub failed: Vec<String>,
    pub calls: BTreeMap<String, BTreeMap<String, u64>>,
}

/// Cover every watched address from `from` through `to` (at most the finalized head), window by
/// window. A failed window is reported and its address stops for this pass; the others go on.
pub async fn sync(
    d: &HostDiscoverer,
    history: &AddressHistory,
    from: u64,
    to: Option<u64>,
    max_windows: u64,
) -> Result<Report> {
    let mut report = Report {
        finalized: advance_final(d, history).await?,
        ..Report::default()
    };
    let to = to.unwrap_or(report.finalized).min(report.finalized);
    let start = from.max(crate::address_history::traced_from(history.chain_id()));
    for address in history.watched()? {
        let mut through: Option<u64> = None;
        loop {
            if report.windows >= max_windows {
                break;
            }
            let (next, actions, limit) = address_discovery::next_owed(history, &address, start)?;
            if next > to {
                break;
            }
            if through.is_some_and(|t| next <= t) {
                report
                    .failed
                    .push(format!("{address}: coverage did not advance past {next}"));
                break;
            }
            let end = next.saturating_add(OUTER_WINDOW - 1).min(to).min(limit);
            match window(d, history, &address, (next, end), actions).await {
                Ok(()) => {
                    report.windows += 1;
                    through = Some(end);
                }
                Err(e) => {
                    report
                        .failed
                        .push(format!("{address} [{next}, {end}]: {e:#}"));
                    break;
                }
            }
        }
    }
    report.calls.insert("main".into(), d.main.calls());
    report.calls.insert("trace".into(), d.trace.calls());
    Ok(report)
}
