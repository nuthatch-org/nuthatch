//! What the binary's rotki-mode cursor does for one pass, ported for a host that calls it: advance the
//! finalized head, then for each watched address discover and record windows up to a block.
//!
//! The checks are the cursor's (`address_mode.rs`): the finalized hash is read on both sides of the
//! held one and must agree; every main and trace endpoint must be on one chain at a window's end,
//! before and after the fetch; a window above the finalized head is read against its last block's
//! hash, and that hash, every block hash a trace named and the newest tracked hash under each must
//! still be the main endpoint's, twice; nothing fetched is kept from a window that failed any of them;
//! and a pass first walks down the tracked hashes, taking back what a reorg replaced.

use anyhow::{anyhow, bail, Context, Result};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

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

/// The main endpoint's chain head.
pub async fn latest(d: &HostDiscoverer) -> Result<u64> {
    address_discovery::hex_u64(&d.main.call("eth_blockNumber", json!([])).await?)
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

/// `check_anchor` in `address_mode.rs`.
async fn check_anchor(
    d: &HostDiscoverer,
    history: &AddressHistory,
    anchor: Option<(u64, [u8; 32])>,
    named: &[(u64, [u8; 32])],
) -> Result<()> {
    let tracked = history.tracked()?;
    let mut checks: BTreeSet<(u64, [u8; 32])> = BTreeSet::new();
    for &(block, hash) in anchor.iter().chain(named) {
        checks.insert((block, hash));
        if let Some(&under) = tracked.iter().rev().find(|(b, _)| *b < block) {
            checks.insert(under);
        }
    }
    if !checks.is_empty() {
        if let Some(&newest) = tracked.last() {
            checks.insert(newest);
        }
    }
    let held = async {
        for _ in 0..2 {
            for &(block, hash) in &checks {
                if block_hash(&d.main, block).await? != hash {
                    bail!("block {block} is no longer the one this was read against; a reorg is under way");
                }
            }
        }
        Ok(())
    }
    .await;
    hold_final(d, history).await?;
    held
}

/// `check_tracked` in `address_mode.rs`: walk down the tracked hashes to the newest the chain still
/// has, and take back everything above it.
pub async fn check_tracked(
    d: &HostDiscoverer,
    history: &AddressHistory,
    latest: u64,
) -> Result<()> {
    let tracked = history.tracked()?;
    let committed = history.head()?.unwrap_or(0);
    hold_final(d, history).await?;
    let mut moved: Option<u64> = tracked.iter().map(|(b, _)| *b).find(|b| *b > latest);
    let mut base = committed;
    for &(b, h) in tracked.iter().rev().filter(|(b, _)| *b <= latest) {
        if block_hash(&d.main, b).await? == h {
            base = b;
            break;
        }
        moved = Some(b);
    }
    let Some(lowest_moved) = moved else {
        return Ok(());
    };
    if lowest_moved <= committed {
        let why = format!(
            "block {lowest_moved}, under the finalized head {committed}, is now at another hash"
        );
        history.set_conflict(&why)?;
        bail!(why);
    }
    history.invalidate_above(base)
}

/// One window of every discovered action for `address`, recorded only if every check held, and read
/// against its last block's hash when that is above the finalized head.
pub async fn window(
    d: &HostDiscoverer,
    history: &AddressHistory,
    address: &str,
    (from, to): (u64, u64),
    actions: &'static [crate::address_history::Action],
) -> Result<()> {
    let pending = Pending::begin(history)?;
    let generation = pending.generation();
    let anchor = if history.head()?.is_some_and(|h| to <= h) {
        None
    } else {
        Some((to, block_hash(&d.main, to).await?))
    };
    if let Some((b, h)) = anchor {
        pending.anchor(b, h);
    }
    same_chain_at(d, to).await?;
    let found = d
        .discover(history, address, from, to, actions, &pending)
        .await;
    let checked = match check_anchor(d, history, anchor, &pending.named()).await {
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
        anchor,
        actions,
    )
}

/// Answer one `/api` request as the nest does, covering on demand (RFC-0063 §12): an account list for
/// a watched address whose range is not yet covered, or reaches above the finalized head, is synced
/// for that address up to its `endblock` first, in at most `cover_windows` windows, so it is answered
/// whole or refused, and a read above the finalized head has been checked for a reorg.
pub async fn answer(
    d: &HostDiscoverer,
    history: &AddressHistory,
    params: &std::collections::HashMap<String, String>,
    cover_windows: u64,
) -> Value {
    use crate::address_history::{incomplete, respond};
    let first = respond(Some(history), params);
    let get = |k: &str| params.get(k).map(String::as_str);
    if get("module") == Some("block") && get("action") == Some("getblocknobytime") {
        return match get("timestamp").and_then(|t| t.parse::<u64>().ok()) {
            Some(t) => {
                let after = get("closest") == Some("after");
                crate::blocktime::block_by_time(&d.main, history, first, t, after).await
            }
            None => first,
        };
    }
    let (Some(address), Some(end), None) = (
        get("address").filter(|_| get("module") == Some("account")),
        get("endblock").and_then(|e| e.parse::<u64>().ok()),
        get("txhash"),
    ) else {
        return first;
    };
    let unanswered = first["result"]
        .as_str()
        .is_some_and(|r| r.starts_with("NUTHATCH_INCOMPLETE:"));
    let above_final = history.head().ok().flatten().is_none_or(|h| end > h);
    if !(unanswered || above_final) || cover_windows == 0 {
        return first;
    }
    match history.is_watched(address) {
        Ok(true) => {}
        _ => return first,
    }
    let synced = sync(
        d,
        history,
        Some(vec![address.to_string()]),
        get("startblock")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0),
        Some(end),
        cover_windows,
    )
    .await;
    match synced {
        Ok(r) if r.failed.is_empty() => respond(Some(history), params),
        Ok(r) => incomplete(&r.failed.join("; ")),
        Err(e) => incomplete(&format!("{e:#}")),
    }
}

/// What one `sync` did.
#[derive(Debug, Default, serde::Serialize)]
pub struct Report {
    pub finalized: u64,
    pub latest: u64,
    pub windows: u64,
    pub failed: Vec<String>,
    pub calls: BTreeMap<String, BTreeMap<String, u64>>,
}

/// Cover `addresses` (every watched one when `None`) from `from` through `to`, the finalized head when
/// not given and never past the chain head, window by window. A failed window is reported and its
/// address stops for this pass; the others go on.
pub async fn sync(
    d: &HostDiscoverer,
    history: &AddressHistory,
    addresses: Option<Vec<String>>,
    from: u64,
    to: Option<u64>,
    max_windows: u64,
) -> Result<Report> {
    let latest = latest(d).await?;
    check_tracked(d, history, latest).await?;
    let mut report = Report {
        finalized: advance_final(d, history).await?,
        latest,
        ..Report::default()
    };
    let to = to.unwrap_or(report.finalized).min(latest);
    let start = from.max(crate::address_history::traced_from(history.chain_id()));
    let addresses = match addresses {
        Some(a) => a.iter().map(|a| a.to_ascii_lowercase()).collect(),
        None => history.watched()?,
    };
    for address in addresses {
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
