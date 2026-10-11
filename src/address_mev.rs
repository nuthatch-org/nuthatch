//! MEV-Boost payments to a watched address (RFC-0063 §8): the blocks it produced, locally or through
//! a relay, with what the relays say each delivered and the on-chain payment that delivered it.
//!
//! A relay's record is a claim, so it is set beside the payment transaction rather than trusted
//! alone. The rows come from the chain: every block a watched address received the fees of is one.
//! MEV is a label on a row, never a claim about the row set: `relay`, `conflict` (relays disagree), or
//! `unrecorded` (no delivery at the queried relays when last asked). A relay saves its record after
//! the block is published, and can fail to, so `unrecorded` is never zero MEV and is asked again on a
//! slow schedule ([`reask_pass`]).

use anyhow::{anyhow, Context, Result};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::address_discovery::{Counted, Discoverer, Rpc};
use crate::address_history::{Action, AddressHistory, Row};

/// The MEV-Boost relays whose public data API answered on 2026-10-10. bloXroute's max-profit relay,
/// Eden, Manifold and others that no longer answer are left out.
pub const RELAYS: [(&str, &str); 6] = [
    ("flashbots", "https://boost-relay.flashbots.net"),
    ("ultrasound", "https://relay.ultrasound.money"),
    (
        "bloxroute-regulated",
        "https://bloxroute.regulated.blxrbdn.com",
    ),
    ("agnostic", "https://agnostic-relay.net"),
    ("aestus", "https://mainnet.aestus.live"),
    ("titan", "https://titanrelay.xyz"),
];

/// Blocks a pass covers at once for one address.
const MEV_WINDOW: u64 = 50_000;
/// A block without relay evidence is asked again at most this often...
pub const REASK_EVERY_SECS: u64 = 24 * 60 * 60;
/// ...for this long after it was first asked.
pub const REASK_FOR_SECS: u64 = 30 * 24 * 60 * 60;

/// One relay's deliveries for one block: relay name and block in, the bid traces out.
pub type Relays = Arc<
    dyn Fn(&'static str, u64) -> futures::future::BoxFuture<'static, Result<Vec<Value>>>
        + Send
        + Sync,
>;

/// The public data API of [`RELAYS`], over HTTP.
pub fn relays_over_http() -> Result<Relays> {
    let client = reqwest::Client::builder()
        .user_agent(concat!("nuthatch/", env!("CARGO_PKG_VERSION")))
        .timeout(std::time::Duration::from_secs(30))
        .build()?;
    Ok(Arc::new(move |name: &'static str, block: u64| {
        let client = client.clone();
        Box::pin(async move {
            let base = RELAYS
                .iter()
                .find(|(n, _)| *n == name)
                .map(|(_, url)| *url)
                .ok_or_else(|| anyhow!("no relay {name}"))?;
            let resp = client
                .get(format!(
                    "{base}/relay/v1/data/bidtraces/proposer_payload_delivered?block_number={block}"
                ))
                .send()
                .await
                .map_err(|e| e.without_url())
                .with_context(|| format!("relay {name}, block {block}"))?;
            if !resp.status().is_success() {
                anyhow::bail!("relay {name} answered {} for block {block}", resp.status());
            }
            let body: Value = resp
                .json()
                .await
                .map_err(|e| e.without_url())
                .with_context(|| format!("relay {name}, block {block}"))?;
            body.as_array()
                .cloned()
                .ok_or_else(|| anyhow!("relay {name} answered a non-list for block {block}"))
        })
    }))
}

/// Every relay's deliveries for `block`, from the cache or asked now. A relay that cannot be asked
/// fails the lookup: a missing answer is not an absent delivery. Only a finalized block's answer is
/// cached.
async fn relay_record(
    history: &AddressHistory,
    relays: &Relays,
    block: u64,
    deadline: Option<std::time::Instant>,
) -> Result<Map<String, Value>> {
    if let Some(r) = history.relay_record(block)? {
        return Ok(r);
    }
    let record = ask_relays(relays, block, deadline).await?;
    history.cache_relay_record(block, &record)?;
    Ok(record)
}

/// Every relay's deliveries for `block`, asked now and kept nowhere, each ask within `deadline`.
async fn ask_relays(
    relays: &Relays,
    block: u64,
    deadline: Option<std::time::Instant>,
) -> Result<Map<String, Value>> {
    let mut delivered = Vec::new();
    for (name, _) in RELAYS {
        let ask = relays(name, block);
        let traces = match deadline {
            None => ask.await?,
            Some(d) => tokio::time::timeout_at(d.into(), ask)
                .await
                .map_err(|_| anyhow!("not covered within the deadline; try again shortly"))??,
        };
        for trace in traces {
            if trace.get("block_number").and_then(Value::as_str) != Some(block.to_string().as_str())
            {
                continue;
            }
            let field = |k: &str| {
                trace
                    .get(k)
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string()
            };
            delivered.push(json!({
                "relay": name,
                "recipient": field("proposer_fee_recipient").to_ascii_lowercase(),
                "value": field("value"),
                "builder": field("builder_pubkey"),
                "proposer": field("proposer_pubkey"),
                "hash": field("block_hash").to_ascii_lowercase(),
            }));
        }
    }
    let mut record = Map::new();
    record.insert("delivered".into(), Value::Array(delivered));
    Ok(record)
}

/// What the relays say they delivered in one block, if any did, and whether they disagree.
struct Delivery {
    relays: Vec<String>,
    recipient: String,
    value: String,
    builder: String,
    proposer: String,
    conflict: bool,
}

/// The deliveries for the canonical block `hash`, preferring a claim that names `a`: a relay may
/// still hold the delivery of a block a reorg replaced, which says nothing about this one.
fn delivery(record: &Map<String, Value>, hash: &str, a: &str) -> Option<Delivery> {
    let get = |v: &Value, k: &str| v.get(k).and_then(Value::as_str).unwrap_or("").to_string();
    let delivered: Vec<&Value> = record
        .get("delivered")?
        .as_array()?
        .iter()
        .filter(|d| get(d, "hash") == hash)
        .collect();
    let claims: BTreeSet<(String, String)> = delivered
        .iter()
        .map(|d| (get(d, "recipient"), get(d, "value")))
        .collect();
    let chosen = delivered
        .iter()
        .find(|d| get(d, "recipient") == a)
        .or_else(|| delivered.first())?;
    Some(Delivery {
        relays: delivered.iter().map(|d| get(d, "relay")).collect(),
        recipient: get(chosen, "recipient"),
        value: get(chosen, "value"),
        builder: get(chosen, "builder"),
        proposer: get(chosen, "proposer"),
        conflict: claims.len() > 1,
    })
}

/// The relays' word on the canonical block `hash`, as the fields a proven block carries: `mev` is
/// `relay`, `conflict` (they disagree) or `unrecorded` (no delivery for this hash at the queried
/// relays, or a relay that could not be asked in time, which is not taken as an answer). Relay
/// provenance only: no relay claim names or overrides the proposer, and none is payment evidence.
/// `fresh` asks the relays again instead of reading the cached record.
pub async fn block_mev(
    history: &AddressHistory,
    relays: &Relays,
    block: u64,
    hash: &str,
    fresh: bool,
    deadline: Option<std::time::Instant>,
) -> Map<String, Value> {
    let record = if fresh {
        match ask_relays(relays, block, deadline).await {
            Ok(r) => {
                let _ = history.cache_relay_record(block, &r);
                Ok(r)
            }
            Err(e) => Err(e),
        }
    } else {
        relay_record(history, relays, block, deadline).await
    };
    let mut r = Map::new();
    let mut put = |k: &str, v: String| {
        r.insert(k.to_string(), Value::String(v));
    };
    match record
        .ok()
        .and_then(|rec| delivery(&rec, &hash.to_ascii_lowercase(), ""))
    {
        Some(dl) => {
            put("mevRecipient", dl.recipient);
            put(
                "mevReward",
                if dl.conflict { String::new() } else { dl.value },
            );
            put("relays", dl.relays.join(","));
            put("builderPubkey", dl.builder);
            put("proposerPubkey", dl.proposer);
            put("mev", if dl.conflict { "conflict" } else { "relay" }.into());
        }
        None => {
            for k in [
                "mevRecipient",
                "mevReward",
                "relays",
                "builderPubkey",
                "proposerPubkey",
            ] {
                put(k, String::new());
            }
            put("mev", "unrecorded".into());
        }
    }
    r
}

/// The payment legs to `recipient` in one block, from its `trace_block` frames: value transfers to it
/// from the block's fee recipient `miner` (when that is someone else) or anywhere in the block's last
/// transaction, top-level or internal (a payout contract), as `nuthatchProducedBlocks` counts them. A
/// frame under a reverted one moved nothing. Every frame must belong to block `hash`.
pub fn payment_legs(
    frames: &[Value],
    hash: &str,
    miner: &str,
    recipient: &str,
) -> Result<(Vec<String>, alloy_primitives::U256)> {
    let text = |v: &Value, k: &str| {
        v.get(k)
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_ascii_lowercase()
    };
    let (miner, recipient, hash) = (
        miner.to_ascii_lowercase(),
        recipient.to_ascii_lowercase(),
        hash.to_ascii_lowercase(),
    );
    let path = |f: &Value| -> Vec<u64> {
        f.get("traceAddress")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(Value::as_u64).collect())
            .unwrap_or_default()
    };
    let mut reverted: BTreeMap<String, Vec<Vec<u64>>> = BTreeMap::new();
    let mut last: Option<u64> = None;
    for f in frames {
        if text(f, "blockHash") != hash {
            anyhow::bail!("a trace frame of another block");
        }
        let Some(pos) = f.get("transactionPosition").and_then(Value::as_u64) else {
            continue;
        };
        last = Some(last.map_or(pos, |l| l.max(pos)));
        if f.get("error").is_some_and(|e| !e.is_null()) {
            reverted
                .entry(text(f, "transactionHash"))
                .or_default()
                .push(path(f));
        }
    }
    let mut hashes: Vec<String> = Vec::new();
    let mut total = alloy_primitives::U256::ZERO;
    for f in frames {
        let Some(pos) = f.get("transactionPosition").and_then(Value::as_u64) else {
            continue;
        };
        let action = f.get("action").cloned().unwrap_or(Value::Null);
        // A value call, or a self-destruct paying out its balance, as the internal transfers are read.
        let (from, to, value) = match (text(f, "type").as_str(), text(&action, "callType").as_str())
        {
            ("call", "call") => (
                text(&action, "from"),
                text(&action, "to"),
                text(&action, "value"),
            ),
            ("suicide", _) => (
                text(&action, "address"),
                text(&action, "refundAddress"),
                text(&action, "balance"),
            ),
            _ => continue,
        };
        if to != recipient {
            continue;
        }
        let value = alloy_primitives::U256::from_str_radix(value.trim_start_matches("0x"), 16)
            .context("a trace frame's value")?;
        if value.is_zero() {
            continue;
        }
        let tx = text(f, "transactionHash");
        let here = path(f);
        if reverted
            .get(&tx)
            .is_some_and(|ps| ps.iter().any(|p| here.starts_with(p)))
        {
            continue;
        }
        let from_miner = from == miner && miner != recipient;
        if !(from_miner || Some(pos) == last) {
            continue;
        }
        total += value;
        if !hashes.contains(&tx) {
            hashes.push(tx);
        }
    }
    Ok((hashes, total))
}

/// The MEV fields of a block nobody asked the relays about: `unrecorded`, never zero.
pub fn no_mev() -> Map<String, Value> {
    let mut r = Map::new();
    for k in [
        "mevRecipient",
        "mevReward",
        "relays",
        "builderPubkey",
        "proposerPubkey",
    ] {
        r.insert(k.to_string(), Value::String(String::new()));
    }
    r.insert("mev".into(), Value::String("unrecorded".into()));
    r
}

/// What a produced-block pass found: the rows, and the blocks it asked the relays about that named no
/// delivery for the canonical block, which the re-ask schedule takes on.
#[derive(Debug, Default)]
pub struct Produced {
    pub rows: Vec<Row>,
    pub unrecorded: Vec<u64>,
}

/// The produced-block rows of `address` over `[from, to]`, from its stored mined blocks and its
/// normal and internal transactions, all of which must be covered there.
pub async fn produced_blocks<M: Rpc, T: Rpc>(
    d: &Discoverer<Counted<M>, Counted<T>>,
    relays: &Relays,
    history: &AddressHistory,
    address: &str,
    (from, to): (u64, u64),
    asked: Option<&BTreeMap<u64, Map<String, Value>>>,
    deadline: Option<std::time::Instant>,
) -> Result<Produced> {
    let a = address.to_ascii_lowercase();
    let mined = history.rows_in(Action::MinedBlocks, &a, (from, to))?;
    let txs = history.rows_in(Action::TxList, &a, (from, to))?;
    // Builders that pay through a payout contract pay as an internal transfer inside the block's
    // last transaction, so internal transfers are payments too.
    let internals = history.rows_in(Action::TxListInternal, &a, (from, to))?;
    let text = |r: &Map<String, Value>, k: &str| {
        r.get(k).and_then(Value::as_str).unwrap_or("").to_string()
    };
    let number = |r: &Map<String, Value>, k: &str| -> Result<u64> {
        text(r, k)
            .parse()
            .with_context(|| format!("a stored row's {k}"))
    };
    let mined: BTreeMap<u64, &Map<String, Value>> = mined
        .iter()
        .map(|m| Ok((number(m, "blockNumber")?, m)))
        .collect::<Result<_>>()?;
    let mut incoming: BTreeMap<u64, Vec<&Map<String, Value>>> = BTreeMap::new();
    for t in txs.iter().chain(&internals) {
        if text(t, "to") == a && text(t, "value") != "0" && text(t, "isError") != "1" {
            incoming
                .entry(number(t, "blockNumber")?)
                .or_default()
                .push(t);
        }
    }
    let blocks: BTreeSet<u64> = mined.keys().chain(incoming.keys()).copied().collect();

    let mut out = Produced::default();
    for b in blocks {
        // A request's deadline stops the work between candidates, before anything is written.
        if deadline.is_some_and(|d| std::time::Instant::now() >= d) {
            anyhow::bail!("not covered within the deadline; try again shortly");
        }
        let header = d
            .main
            .call("eth_getBlockByNumber", json!([format!("0x{b:x}"), false]))
            .await
            .with_context(|| format!("block {b}"))?;
        let field = |k: &str| {
            header
                .get(k)
                .and_then(Value::as_str)
                .map(str::to_ascii_lowercase)
                .ok_or_else(|| anyhow!("block {b} has no {k}"))
        };
        let (miner, hash) = (field("miner")?, field("hash")?);
        let last = header
            .get("transactions")
            .and_then(Value::as_array)
            .and_then(|t| t.len().checked_sub(1))
            .map(|i| i.to_string());
        // A relay-built block pays the proposer from the builder's fee recipient or, for builders
        // that pay from another account or a payout contract, in the block's last transaction,
        // directly or as an internal transfer.
        // Every leg of the payment: a builder can pay part from its fee recipient and part through a
        // payout contract in the last transaction, and the relay's value is their sum.
        let legs: Vec<&Map<String, Value>> = incoming
            .get(&b)
            .map(|paid| {
                paid.iter()
                    .filter(|t| {
                        (text(t, "from") == miner && miner != a)
                            || Some(text(t, "transactionIndex")) == last
                    })
                    .copied()
                    .collect()
            })
            .unwrap_or_default();
        let payment = legs.first().copied();
        let self_built = mined.get(&b);
        if self_built.is_none() && payment.is_none() {
            continue;
        }
        let record = match asked.and_then(|m| m.get(&b)) {
            Some(r) => r.clone(),
            None => relay_record(history, relays, b, deadline).await?,
        };
        let found = delivery(&record, &hash, &a);
        if found.is_none() {
            out.unrecorded.push(b);
        }
        // A payment makes a block this address's only on a relay's word; without one it waits on
        // the re-ask schedule, which inserts the row if the word comes.
        if self_built.is_none()
            && !found
                .as_ref()
                .is_some_and(|dl| dl.recipient == a || dl.conflict)
        {
            continue;
        }
        let mut r = Map::new();
        let mut put = |k: &str, v: String| {
            r.insert(k.to_string(), Value::String(v));
        };
        put("blockNumber", b.to_string());
        put("blockHash", hash);
        match self_built {
            Some(m) => {
                put("timeStamp", text(m, "timeStamp"));
                put("feeRecipient", a.clone());
                put("blockReward", text(m, "blockReward"));
            }
            None => {
                let p = payment.expect("a block that is not self-built has a payment");
                put("timeStamp", text(p, "timeStamp"));
                put("feeRecipient", miner.clone());
                put("blockReward", d.block_reward(b, &header).await?);
            }
        }
        let mut hashes: Vec<String> = legs.iter().map(|p| text(p, "hash")).collect();
        hashes.dedup();
        put("paymentTx", hashes.join(","));
        let total = legs
            .iter()
            .try_fold(alloy_primitives::U256::ZERO, |sum, p| {
                alloy_primitives::U256::from_str_radix(&text(p, "value"), 10)
                    .map(|v| sum + v)
                    .with_context(|| format!("block {b} payment value"))
            })?;
        put(
            "paymentValue",
            if legs.is_empty() {
                String::new()
            } else {
                total.to_string()
            },
        );
        match found {
            Some(dl) => {
                put("mevRecipient", dl.recipient);
                put(
                    "mevReward",
                    if dl.conflict { String::new() } else { dl.value },
                );
                put("relays", dl.relays.join(","));
                put("builderPubkey", dl.builder);
                put("proposerPubkey", dl.proposer);
                put("mev", if dl.conflict { "conflict" } else { "relay" }.into());
            }
            None => {
                for k in [
                    "mevRecipient",
                    "mevReward",
                    "relays",
                    "builderPubkey",
                    "proposerPubkey",
                ] {
                    put(k, String::new());
                }
                put("mev", "unrecorded".into());
            }
        }
        out.rows.push(Row {
            block: b,
            tx_index: 0,
            position: 0,
            record: r,
        });
    }
    Ok(out)
}

/// One window of the cursor's MEV pass for `a`: covered as far as its mined blocks and its normal and
/// internal transactions are, up to `limit`. Returns whether it covered anything, so the cursor can
/// serve requests between windows and call again.
pub async fn mev_step<M: Rpc, T: Rpc>(
    d: &Discoverer<Counted<M>, Counted<T>>,
    relays: &Relays,
    history: &AddressHistory,
    a: &str,
    start: u64,
    limit: u64,
    now: u64,
) -> Result<bool> {
    use crate::address_discovery::next_uncovered_for;
    let from = next_uncovered_for(history, a, start, &[Action::ProducedBlocks])?;
    let through = |action: Action| -> Result<Option<u64>> {
        Ok(history
            .coverage(action, a)?
            .into_iter()
            .find(|(f, t)| *f <= from && from <= *t)
            .map(|(_, t)| t))
    };
    let (Some(mined), Some(txs), Some(internals)) = (
        through(Action::MinedBlocks)?,
        through(Action::TxList)?,
        through(Action::TxListInternal)?,
    ) else {
        return Ok(false);
    };
    let to = mined
        .min(txs)
        .min(internals)
        .min(from.saturating_add(MEV_WINDOW - 1))
        .min(limit);
    if to < from {
        return Ok(false);
    }
    produced_window(d, relays, history, a, (from, to), now, None).await?;
    Ok(true)
}

/// Cover `[from, to]` of `a`'s produced blocks, its prerequisites covered, and put the blocks without
/// relay evidence on the re-ask schedule in the same transaction, so a crash between the two cannot
/// leave a covered block nothing will ask about again. Only through the finalized head: a relay's
/// answer about a block is used only once the block is final.
pub async fn produced_window<M: Rpc, T: Rpc>(
    d: &Discoverer<Counted<M>, Counted<T>>,
    relays: &Relays,
    history: &AddressHistory,
    a: &str,
    (from, to): (u64, u64),
    now: u64,
    deadline: Option<std::time::Instant>,
) -> Result<()> {
    let head = history.head()?.unwrap_or(0);
    if to > head {
        anyhow::bail!("produced blocks are served through the finalized block {head}, not {to}");
    }
    let generation = history.generation()?;
    // Every read under the deadline, headers and rewards included; the record after it runs to
    // completion.
    let found = crate::address_mode::within(deadline, async {
        let found = produced_blocks(d, relays, history, a, (from, to), None, deadline).await?;
        // The headers were read from the chain and the payments from the store: both must be
        // the finalized chain the store recorded, or a fork switch would join them.
        crate::address_mode::hold_final(&d.main, history).await?;
        Ok(found)
    })
    .await?;
    history.record_produced(
        a,
        &found.rows,
        (from, to),
        generation,
        &found.unrecorded,
        now,
    )
}

/// Blocks one re-ask call takes on, so the cursor serves requests between batches.
const REASK_BATCH: usize = 16;

/// Ask the relays again about the blocks on the re-ask schedule that are due: last asked a day ago,
/// first asked under 30 days ago. What the relays say, the rows it changes (an upgraded row, or a
/// builder payment's row that had none) and the schedule's new state commit in one transaction, so a
/// crash cannot keep the evidence and lose the row. Returns whether due blocks remain.
pub async fn reask_pass<M: Rpc, T: Rpc>(
    d: &Discoverer<Counted<M>, Counted<T>>,
    relays: &Relays,
    history: &AddressHistory,
    watched: &[String],
    now: u64,
) -> Result<bool> {
    let due = overdue(history, now, None)?;
    let more = due.len() > REASK_BATCH;
    for (block, first) in due.into_iter().take(REASK_BATCH) {
        reask_block(d, relays, history, watched, now, block, first, None).await?;
    }
    Ok(more)
}

/// The blocks on the re-ask schedule whose next ask is owed at `now`, within `range` if given.
pub fn overdue(
    history: &AddressHistory,
    now: u64,
    range: Option<(u64, u64)>,
) -> Result<Vec<(u64, u64)>> {
    Ok(history
        .mev_pending()?
        .into_iter()
        .filter(|(b, _, _)| range.is_none_or(|(f, t)| f <= *b && *b <= t))
        .filter(|(_, first, last)| {
            now.saturating_sub(*first) >= REASK_FOR_SECS
                || now.saturating_sub(*last) >= REASK_EVERY_SECS
        })
        .map(|(b, first, _)| (b, first))
        .collect())
}

/// Every owed re-ask in `[from, to]`, before produced blocks there are answered: a row whose check
/// is owed is not settled, however long the nest was down. Each block's reads run under `deadline`.
pub async fn reask_range<M: Rpc, T: Rpc>(
    d: &Discoverer<Counted<M>, Counted<T>>,
    relays: &Relays,
    history: &AddressHistory,
    watched: &[String],
    (from, to): (u64, u64),
    now: u64,
    deadline: Option<std::time::Instant>,
) -> Result<()> {
    for (block, first) in overdue(history, now, Some((from, to)))? {
        reask_block(d, relays, history, watched, now, block, first, deadline).await?;
    }
    Ok(())
}

/// One block's re-ask: the relays asked fresh and the rows recomputed under `deadline`, then the
/// outcome committed in one transaction.
#[allow(clippy::too_many_arguments)]
async fn reask_block<M: Rpc, T: Rpc>(
    d: &Discoverer<Counted<M>, Counted<T>>,
    relays: &Relays,
    history: &AddressHistory,
    watched: &[String],
    now: u64,
    block: u64,
    first: u64,
    deadline: Option<std::time::Instant>,
) -> Result<()> {
    let generation = history.generation()?;
    let (record, changes, unrecorded) = crate::address_mode::within(deadline, async {
        let record = ask_relays(relays, block, deadline).await?;
        let asked = BTreeMap::from([(block, record.clone())]);
        let mut changes = Vec::new();
        let mut unrecorded = false;
        for a in watched {
            let covered = history
                .coverage(Action::ProducedBlocks, a)?
                .iter()
                .any(|(f, t)| *f <= block && block <= *t);
            if !covered {
                continue;
            }
            let found = produced_blocks(
                d,
                relays,
                history,
                a,
                (block, block),
                Some(&asked),
                deadline,
            )
            .await?;
            unrecorded |= !found.unrecorded.is_empty();
            let before = history.rows_in(Action::ProducedBlocks, a, (block, block))?;
            let after: Vec<Map<String, Value>> =
                found.rows.iter().map(|r| r.record.clone()).collect();
            if before != after {
                changes.push((a.clone(), found.rows));
            }
        }
        crate::address_mode::hold_final(&d.main, history).await?;
        Ok((record, changes, unrecorded))
    })
    .await?;
    // Past thirty days a block leaves the schedule whatever the relays said.
    let keep_asking = unrecorded && now.saturating_sub(first) < REASK_FOR_SECS;
    history.apply_reask(block, &record, &changes, keep_asking, now, generation)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::address_partitions::tests::Fake;
    use std::sync::Mutex;

    const A: &str = "0x00000000000000000000000000000000000000aa";
    const BUILDER: &str = "0x00000000000000000000000000000000000000b1";
    const MINER: &str = "0x00000000000000000000000000000000000000d9";

    fn row(block: u64, fields: Value) -> Row {
        let mut record = fields.as_object().unwrap().clone();
        record.insert("blockNumber".into(), json!(block.to_string()));
        Row {
            block,
            tx_index: 0,
            position: 0,
            record,
        }
    }

    /// Relays answering from a table of (relay, block) -> traces, counting what they are asked.
    fn relays(table: Vec<(&'static str, u64, Value)>) -> (Relays, Arc<Mutex<u64>>) {
        let asked = Arc::new(Mutex::new(0));
        let count = asked.clone();
        let relays: Relays = Arc::new(move |name, block| {
            *count.lock().unwrap() += 1;
            let found: Vec<Value> = table
                .iter()
                .filter(|(r, b, _)| *r == name && *b == block)
                .map(|(_, _, t)| t.clone())
                .collect();
            Box::pin(async move { Ok(found) })
        });
        (relays, asked)
    }

    fn hash(block: u64) -> String {
        format!("0x{block:064x}")
    }

    fn trace(block: u64, recipient: &str, value: &str) -> Value {
        json!({"block_number": block.to_string(), "block_hash": hash(block),
               "proposer_fee_recipient": recipient, "value": value,
               "builder_pubkey": "0xbb", "proposer_pubkey": "0xpp"})
    }

    /// Self-built blocks with no relay, with a relay's delivery for a block a reorg replaced, with
    /// two relays that disagree, and with a builder's separate payment; relay-built blocks paid by
    /// the builder's fee recipient and by another account in the last transaction; a payment no
    /// relay knows of; a transfer from someone else; and relays that disagree about who was paid.
    #[tokio::test]
    async fn produced_blocks_join_mined_blocks_payments_and_relays() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::store::Store::open(&dir.path().join("t.redb")).unwrap();
        let h = AddressHistory::open(store, 1, &[A.into()]).unwrap();
        h.set_head(1_000).unwrap();
        let g = h.generation().unwrap();
        let mined = |b: u64, reward: &str| {
            row(
                b,
                json!({"timeStamp": format!("{}", 1000 + 12 * (b - 100)), "blockReward": reward}),
            )
        };
        h.record(
            Action::MinedBlocks,
            A,
            &[mined(100, "5"), mined(104, "6"), mined(107, "8")],
            (100, 199),
            g,
        )
        .unwrap();
        let tx = |block: u64, from: &str, value: &str, index: &str| {
            row(
                block,
                json!({"timeStamp": format!("{}", 1000 + 12 * (block - 100)),
                       "hash": format!("0xp{block}"), "from": from, "to": A, "value": value,
                       "isError": "0", "transactionIndex": index}),
            )
        };
        let other = "0x00000000000000000000000000000000000000c3";
        h.record(
            Action::TxList,
            A,
            &[
                tx(101, BUILDER, "9000", "0"),
                tx(102, other, "1", "0"),
                tx(103, "0x00000000000000000000000000000000000000e4", "7", "0"),
                tx(105, other, "11", "1"),
                tx(106, MINER, "13", "0"),
                tx(107, other, "3", "1"),
            ],
            (100, 199),
            g,
        )
        .unwrap();
        let main = Fake::new(|m, p| {
            let b = crate::address_discovery::hex_u64(&p[0]).unwrap_or(0);
            Ok(match m {
                "eth_getBlockByNumber" => json!({"miner": match b {
                    100 | 104 | 107 => A,
                    101 => BUILDER,
                    103 => "0x00000000000000000000000000000000000000e4",
                    _ => MINER,
                }, "hash": hash(b), "baseFeePerGas": "0x1",
                   "transactions": if b == 105 || b == 107 { json!(["0x01", "0x02"]) } else { json!([]) },
                   "uncles": []}),
                "eth_getBlockReceipts" if p[0] == "0x69" => json!([
                    {"transactionHash": "0x01", "gasUsed": "0x1", "effectiveGasPrice": "0x1"},
                    {"transactionHash": "0x02", "gasUsed": "0x1", "effectiveGasPrice": "0x1"},
                ]),
                "eth_getBlockReceipts" => json!([]),
                other => anyhow::bail!("unexpected {other}"),
            })
        });
        let d = Discoverer::new(
            Counted::new(main),
            Counted::new(Fake::new(|_, _| Ok(json!([])))),
        );
        let mut stale = trace(100, A, "77");
        stale["block_hash"] = json!("0xorphan");
        let (rel, asked) = relays(vec![
            ("flashbots", 100, stale),
            ("ultrasound", 101, trace(101, A, "9000")),
            ("titan", 101, trace(101, A, "9000")),
            ("flashbots", 104, trace(104, A, "6")),
            ("agnostic", 104, trace(104, A, "60")),
            ("aestus", 105, trace(105, A, "11")),
            ("flashbots", 106, trace(106, other, "13")),
            ("ultrasound", 106, trace(106, A, "13")),
            ("aestus", 107, trace(107, A, "3")),
        ]);
        let found = produced_blocks(&d, &rel, &h, A, (100, 199), None, None)
            .await
            .unwrap();
        assert_eq!(
            found.unrecorded,
            [100, 103],
            "100 is self-built without a delivery; 103's payment has none either, and waits for one"
        );
        let rows = found.rows;
        let view: Vec<String> = rows
            .iter()
            .map(|r| {
                let t = |k: &str| r.record[k].as_str().unwrap().to_string();
                format!(
                    "{} {} {} {} {}",
                    r.block,
                    &t("feeRecipient")[40..],
                    t("mev"),
                    t("mevReward"),
                    t("paymentTx")
                )
            })
            .collect();
        assert_eq!(
            view,
            [
                "100 aa unrecorded  ",
                "101 b1 relay 9000 0xp101",
                "104 aa conflict  ",
                "105 d9 relay 11 0xp105",
                "106 d9 conflict  0xp106",
                "107 aa relay 3 0xp107",
            ]
        );
        assert_eq!(rows[1].record["relays"], "ultrasound,titan");
        assert_eq!(rows[5].record["paymentValue"], "3");
        assert_eq!(
            rows[4].record["mevRecipient"], A,
            "of two claims, the one naming this address"
        );
        let first = *asked.lock().unwrap();
        assert_eq!(first, 6 * 7, "every block but 102, every relay once");

        // Asked again, the relays' answers come from the store.
        produced_blocks(&d, &rel, &h, A, (100, 199), None, None)
            .await
            .unwrap();
        assert_eq!(*asked.lock().unwrap(), first);
    }

    /// A relay that cannot be asked fails the lookup; nothing is cached as absent.
    #[tokio::test]
    async fn an_unreachable_relay_is_not_an_absent_delivery() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::store::Store::open(&dir.path().join("t.redb")).unwrap();
        let h = AddressHistory::open(store, 1, &[A.into()]).unwrap();
        h.set_head(1_000).unwrap();
        let down: Relays = Arc::new(|name, _| {
            Box::pin(async move {
                if name == "titan" {
                    anyhow::bail!("titan is down")
                }
                Ok(vec![])
            })
        });
        assert!(relay_record(&h, &down, 100, None).await.is_err());
        assert!(h.relay_record(100).unwrap().is_none());

        // A block past what the nest serves is asked about but not kept: a relay may yet learn
        // of it.
        let (rel, asked) = relays(vec![]);
        relay_record(&h, &rel, 2_000, None).await.unwrap();
        relay_record(&h, &rel, 2_000, None).await.unwrap();
        assert_eq!(*asked.lock().unwrap(), 2 * 6);
        assert!(h.relay_record(2_000).unwrap().is_none());
    }

    /// A chain for the re-ask tests: A built block 100 itself; block 101 was built by BUILDER, who
    /// paid A directly; block 102 was built by a builder paying A through a payout contract, an
    /// internal transfer in the block's last transaction.
    fn evidence_chain(dir: &std::path::Path) -> AddressHistory {
        let store = crate::store::Store::open(&dir.join("t.redb")).unwrap();
        let h = AddressHistory::open(store, 1, &[A.into()]).unwrap();
        h.set_head(1_000).unwrap();
        h
    }

    fn evidence_rows(h: &AddressHistory) {
        let g = h.generation().unwrap();
        h.record(
            Action::MinedBlocks,
            A,
            &[row(100, json!({"timeStamp": "1000", "blockReward": "5"}))],
            (100, 199),
            g,
        )
        .unwrap();
        h.record(
            Action::TxList,
            A,
            &[row(
                101,
                json!({"timeStamp": "1012", "hash": "0xp101", "from": BUILDER, "to": A,
                              "value": "9000", "isError": "0", "transactionIndex": "0"}),
            )],
            (100, 199),
            g,
        )
        .unwrap();
        h.record(
            Action::TxListInternal,
            A,
            &[row(
                102,
                json!({"timeStamp": "1024", "hash": "0xp102",
                              "from": "0xfeeeeee44046c3f61a8cc081e0918ef0de0a7ffc", "to": A,
                              "value": "77", "isError": "0", "transactionIndex": "1"}),
            )],
            (100, 199),
            g,
        )
        .unwrap();
    }

    fn evidence_rpc() -> Discoverer<Counted<Fake>, Counted<Fake>> {
        let main = Fake::new(|m, p| {
            let b = crate::address_discovery::hex_u64(&p[0]).unwrap_or(0);
            Ok(match m {
                "eth_getBlockByNumber" => {
                    json!({"miner": match b { 100 => A, 101 => BUILDER, _ => MINER },
                    "hash": hash(b), "baseFeePerGas": "0x1",
                    "transactions": if b == 102 { json!(["0x01", "0x02"]) } else { json!([]) },
                    "uncles": []})
                }
                "eth_blockNumber" => json!("0x3e8"),
                "eth_getBlockReceipts" if b == 102 => json!([
                    {"transactionHash": "0x01", "gasUsed": "0x1", "effectiveGasPrice": "0x1"},
                    {"transactionHash": "0x02", "gasUsed": "0x1", "effectiveGasPrice": "0x1"},
                ]),
                "eth_getBlockReceipts" => json!([]),
                other => anyhow::bail!("unexpected {other}"),
            })
        });
        Discoverer::new(
            Counted::new(main),
            Counted::new(Fake::new(|_, _| Ok(json!([])))),
        )
    }

    /// Relays that know nothing until `on` is set, then deliveries to A for 100, 101 and 102.
    fn late_relays(on: Arc<std::sync::atomic::AtomicBool>) -> Relays {
        Arc::new(move |name, block| {
            let on = on.load(std::sync::atomic::Ordering::Relaxed);
            Box::pin(async move {
                Ok(
                    if on && name == "ultrasound" && (100..=102).contains(&block) {
                        vec![trace(block, A, "18665203633316530")]
                    } else {
                        vec![]
                    },
                )
            })
        })
    }

    fn view(h: &AddressHistory) -> Vec<String> {
        h.rows_in(Action::ProducedBlocks, A, (100, 199))
            .unwrap()
            .iter()
            .map(|r| {
                format!(
                    "{} {} {}",
                    r["blockNumber"].as_str().unwrap(),
                    r["mev"].as_str().unwrap(),
                    r["paymentTx"].as_str().unwrap()
                )
            })
            .collect()
    }

    /// A relay that saved its record late is asked again on the schedule, across a restart: the
    /// self-built block's row is upgraded and the builder payments, the payout contract's included,
    /// gain the rows they had none of. Never before a day has passed; never after thirty.
    #[tokio::test]
    async fn late_relay_evidence_upgrades_and_inserts_rows_across_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let on = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let t0 = 1_800_000_000;
        {
            let h = evidence_chain(dir.path());
            evidence_rows(&h);
            produced_window(
                &evidence_rpc(),
                &late_relays(on.clone()),
                &h,
                A,
                (100, 199),
                t0,
                None,
            )
            .await
            .unwrap();
            assert_eq!(
                view(&h),
                ["100 unrecorded "],
                "only the self-built block is a row yet"
            );
            let pending: Vec<u64> = h.mev_pending().unwrap().iter().map(|p| p.0).collect();
            assert_eq!(pending, [100, 101, 102]);
        }
        on.store(true, std::sync::atomic::Ordering::Relaxed);
        let h = evidence_chain(dir.path());
        let d = evidence_rpc();
        let rel = late_relays(on.clone());
        let watched = [A.to_string()];
        reask_pass(&d, &rel, &h, &watched, t0 + 3_600)
            .await
            .unwrap();
        assert_eq!(view(&h), ["100 unrecorded "], "not due within the day");
        let g = h.generation().unwrap();
        reask_pass(&d, &rel, &h, &watched, t0 + REASK_EVERY_SECS)
            .await
            .unwrap();
        assert_eq!(
            view(&h),
            ["100 relay ", "101 relay 0xp101", "102 relay 0xp102"]
        );
        assert!(
            h.generation().unwrap() > g,
            "the upgrade moved the generation"
        );
        assert!(h.mev_pending().unwrap().is_empty());

        // A request's deadline stops the work between candidates, before anything is written.
        let g = h.generation().unwrap();
        let past = std::time::Instant::now() - std::time::Duration::from_secs(1);
        let err = produced_window(&d, &rel, &h, A, (100, 199), t0, Some(past))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("deadline"), "{err:#}");
        assert_eq!(h.generation().unwrap(), g, "nothing written");

        // Produced blocks are served only through the finalized head.
        h.set_head(150).unwrap();
        let err = produced_window(&d, &rel, &h, A, (200, 299), t0, None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("finalized block 150"), "{err:#}");
        h.set_head(1_000).unwrap();

        // A block whose evidence never comes is dropped from the schedule after thirty days, with
        // its row left unrecorded, not zero.
        h.mev_absent(150, t0).unwrap();
        on.store(false, std::sync::atomic::Ordering::Relaxed);
        reask_pass(&d, &rel, &h, &watched, t0 + REASK_FOR_SECS)
            .await
            .unwrap();
        assert!(h.mev_pending().unwrap().is_empty());
    }

    /// A produced-block demand's reads, headers and rewards included, stop at its deadline and leave
    /// nothing written, so a slow RPC cannot hold the cursor past it.
    #[tokio::test]
    async fn a_produced_demand_stops_at_its_deadline() {
        struct Slow;
        impl Rpc for Slow {
            async fn call(&self, _: &str, _: Value) -> Result<Value> {
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                Ok(Value::Null)
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let h = evidence_chain(dir.path());
        evidence_rows(&h);
        let d = Discoverer::new(Counted::new(Slow), Counted::new(Slow));
        let on = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let g = h.generation().unwrap();
        let started = std::time::Instant::now();
        let deadline = Some(std::time::Instant::now() + std::time::Duration::from_millis(100));
        let err = produced_window(&d, &late_relays(on), &h, A, (100, 199), 0, deadline)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("deadline"), "{err:#}");
        assert!(started.elapsed() < std::time::Duration::from_secs(2));
        assert_eq!(h.generation().unwrap(), g);
        assert!(h.coverage(Action::ProducedBlocks, A).unwrap().is_empty());
    }

    /// After downtime past a scheduled relay re-ask, a produced-blocks request for the range runs the
    /// owed check first, through the cursor, before the background pass has: a row whose check is
    /// owed is never served as settled.
    #[tokio::test]
    async fn an_owed_reask_runs_before_its_range_is_served() {
        use tower::ServiceExt;
        let dir = tempfile::tempdir().unwrap();
        let on = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let two_days_ago = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 2 * REASK_EVERY_SECS;
        {
            let h = evidence_chain(dir.path());
            evidence_rows(&h);
            let g = h.generation().unwrap();
            for action in [
                Action::TokenTx,
                Action::TokenNftTx,
                Action::Token1155Tx,
                Action::BeaconWithdrawals,
            ] {
                h.record(action, A, &[], (100, 199), g).unwrap();
            }
            produced_window(
                &evidence_rpc(),
                &late_relays(on.clone()),
                &h,
                A,
                (100, 199),
                two_days_ago,
                None,
            )
            .await
            .unwrap();
            assert_eq!(view(&h), ["100 unrecorded "]);
        }
        // The relays have since saved their records; the nest restarts and is asked at once.
        on.store(true, std::sync::atomic::Ordering::Relaxed);
        let h = evidence_chain(dir.path());
        let state = Arc::new(
            crate::address_mode::ModeState::new(
                h.clone(),
                1,
                std::time::Duration::from_secs(300),
                true,
            )
            .with_relays(late_relays(on.clone())),
        );
        let d = Arc::new(evidence_rpc());
        let cursor = tokio::spawn(crate::address_mode::serve_forever(state.clone(), d));
        let resp = crate::address_mode::router(state.clone())
            .oneshot(
                axum::http::Request::get(format!(
                    "/api?module=account&action=nuthatchProducedBlocks&address={A}&startblock=100&endblock=199"
                ))
                .body(axum::body::Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let a: Value = serde_json::from_slice(&bytes).unwrap();
        let mev: Vec<String> = a["result"]
            .as_array()
            .unwrap_or_else(|| panic!("{a}"))
            .iter()
            .map(|r| {
                format!(
                    "{} {}",
                    r["blockNumber"].as_str().unwrap(),
                    r["mev"].as_str().unwrap()
                )
            })
            .collect();
        assert_eq!(mev, ["100 relay", "101 relay", "102 relay"], "{a}");
        assert!(h.mev_pending().unwrap().is_empty());
        cursor.abort();
    }

    /// A builder that pays part from its fee recipient and part through a payout contract in the last
    /// transaction has both legs reported, their sum the payment, as the relay's value is (cold test,
    /// block 26156804).
    #[tokio::test]
    async fn every_leg_of_a_split_payment_is_reported() {
        let dir = tempfile::tempdir().unwrap();
        let h = evidence_chain(dir.path());
        evidence_rows(&h);
        let g = h.generation().unwrap();
        // Block 102 also has a direct transfer from its miner.
        h.record(
            Action::TxList,
            A,
            &[
                row(
                    101,
                    json!({"timeStamp": "1012", "hash": "0xp101", "from": BUILDER, "to": A,
                                "value": "9000", "isError": "0", "transactionIndex": "0"}),
                ),
                row(
                    102,
                    json!({"timeStamp": "1024", "hash": "0xq102", "from": MINER, "to": A,
                                "value": "23", "isError": "0", "transactionIndex": "0"}),
                ),
            ],
            (100, 199),
            g,
        )
        .unwrap();
        let on = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let found = produced_blocks(
            &evidence_rpc(),
            &late_relays(on),
            &h,
            A,
            (102, 102),
            None,
            None,
        )
        .await
        .unwrap();
        let r = &found.rows[0].record;
        assert_eq!(r["paymentTx"], "0xq102,0xp102", "{r:?}");
        assert_eq!(r["paymentValue"], "100");
    }

    /// The background pass builds produced-block coverage as it does every other action's, up to the
    /// finalized head, without waiting for a request (cold test M2: a staker's coverage moved only
    /// inside request deadlines).
    #[tokio::test]
    async fn the_background_pass_builds_produced_blocks() {
        use crate::address_mode::Discovery;
        let dir = tempfile::tempdir().unwrap();
        let h = evidence_chain(dir.path());
        evidence_rows(&h);
        let g = h.generation().unwrap();
        for action in [
            Action::TokenTx,
            Action::TokenNftTx,
            Action::Token1155Tx,
            Action::BeaconWithdrawals,
        ] {
            h.record(action, A, &[], (100, 199), g).unwrap();
        }
        let on = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let state = crate::address_mode::ModeState::new(
            h.clone(),
            1,
            std::time::Duration::from_secs(300),
            true,
        )
        .with_range(100, Some(199), 0)
        .with_relays(late_relays(on));
        evidence_rpc().catch_up(&state, 1_000).await;
        assert_eq!(
            h.coverage(Action::ProducedBlocks, A).unwrap(),
            vec![(100, 199)]
        );
        assert_eq!(
            view(&h),
            ["100 relay ", "101 relay 0xp101", "102 relay 0xp102"]
        );
    }

    /// Headers read from a chain that no longer holds the store's finalized hash are never joined to
    /// the stored payments: neither a produced window nor a re-ask commits, and the conflict is
    /// recorded in the store.
    #[tokio::test]
    async fn produced_blocks_hold_the_finalized_hash() {
        let dir = tempfile::tempdir().unwrap();
        let h = evidence_chain(dir.path());
        evidence_rows(&h);
        h.mev_absent(101, 0).unwrap();
        // Recorded on a fork the RPC no longer serves.
        h.finalize_at(1_000, [7; 32]).unwrap();
        let on = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let (d, rel) = (evidence_rpc(), late_relays(on));
        let err = produced_window(&d, &rel, &h, A, (100, 199), 0, None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("finalized block 1000"), "{err:#}");
        assert!(h.coverage(Action::ProducedBlocks, A).unwrap().is_empty());
        assert!(h.conflict().unwrap().is_some());
        let g = h.generation().unwrap();
        let err = reask_pass(&d, &rel, &h, &[A.to_string()], REASK_EVERY_SECS)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("finalized block 1000"), "{err:#}");
        assert_eq!(h.generation().unwrap(), g);
        assert_eq!(h.mev_pending().unwrap().len(), 1, "still owed");
    }

    /// Produced blocks do not wait behind another address's backfill: a discovery that hangs still
    /// leaves the covered address's produced blocks built in the same pass.
    #[tokio::test]
    async fn produced_blocks_do_not_wait_behind_a_backfill() {
        use crate::address_mode::Discovery;
        struct Hangs;
        impl Rpc for Hangs {
            async fn call(&self, _: &str, _: Value) -> Result<Value> {
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                Ok(Value::Null)
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let h = evidence_chain(dir.path());
        evidence_rows(&h);
        let g = h.generation().unwrap();
        for action in [
            Action::TokenTx,
            Action::TokenNftTx,
            Action::Token1155Tx,
            Action::BeaconWithdrawals,
        ] {
            h.record(action, A, &[], (100, 199), g).unwrap();
        }
        // A second address with nothing covered: its discovery would hang on the trace endpoint.
        h.watch("0x00000000000000000000000000000000000000cc")
            .unwrap();
        let on = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let state = crate::address_mode::ModeState::new(
            h.clone(),
            1,
            std::time::Duration::from_secs(300),
            true,
        )
        .with_range(100, Some(199), 0)
        .with_relays(late_relays(on));
        let main = evidence_rpc().main;
        let d = Discoverer::new(main, Counted::new(Hangs));
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), d.catch_up(&state, 1_000))
            .await;
        assert_eq!(
            h.coverage(Action::ProducedBlocks, A).unwrap(),
            vec![(100, 199)]
        );
    }

    fn frame(
        pos: u64,
        tx: &str,
        path: &[u64],
        from: &str,
        to: &str,
        value: u64,
        error: bool,
    ) -> Value {
        let mut f = json!({
            "blockHash": "0xbb",
            "transactionPosition": pos,
            "transactionHash": tx,
            "traceAddress": path,
            "type": "call",
            "action": {"callType": "call", "from": from, "to": to, "value": format!("0x{value:x}")},
        });
        if error {
            f["error"] = json!("Reverted");
        }
        f
    }

    const PAYOUT: &str = "0x00000000000000000000000000000000000000c1";
    const PROPOSER: &str = "0x00000000000000000000000000000000000000a1";

    #[test]
    fn every_leg_of_a_split_payment_is_counted() {
        let frames = vec![
            // The builder pays part from its fee recipient mid-block...
            frame(3, "0x03", &[], BUILDER, PROPOSER, 5, false),
            // ...and the rest through a payout contract in the last transaction.
            frame(9, "0x09", &[], BUILDER, PAYOUT, 0, false),
            frame(9, "0x09", &[0], PAYOUT, PROPOSER, 7, false),
            // An unrelated transfer to the proposer from someone else is not a payment.
            frame(5, "0x05", &[], PAYOUT, PROPOSER, 100, false),
        ];
        let (txs, total) = payment_legs(&frames, "0xbb", BUILDER, PROPOSER).unwrap();
        assert_eq!(txs, vec!["0x03", "0x09"]);
        assert_eq!(total, alloy_primitives::U256::from(12));
    }

    #[test]
    fn a_leg_under_a_reverted_frame_moved_nothing() {
        let frames = vec![
            frame(9, "0x09", &[], BUILDER, PAYOUT, 0, false),
            frame(9, "0x09", &[0], PAYOUT, PAYOUT, 0, true),
            frame(9, "0x09", &[0, 0], PAYOUT, PROPOSER, 7, false),
            frame(9, "0x09", &[1], PAYOUT, PROPOSER, 2, false),
        ];
        let (_, total) = payment_legs(&frames, "0xbb", BUILDER, PROPOSER).unwrap();
        assert_eq!(total, alloy_primitives::U256::from(2));
    }

    #[test]
    fn a_self_paid_block_counts_only_its_last_transaction() {
        // The proposer is the block's fee recipient: its own transfers are not a builder's payment.
        let frames = vec![
            frame(1, "0x01", &[], PROPOSER, PROPOSER, 4, false),
            frame(2, "0x02", &[], BUILDER, PROPOSER, 6, false),
        ];
        let (txs, total) = payment_legs(&frames, "0xbb", PROPOSER, PROPOSER).unwrap();
        assert_eq!(
            (txs, total),
            (vec!["0x02".to_string()], alloy_primitives::U256::from(6))
        );
    }

    #[test]
    fn a_payout_contract_self_destructing_to_the_proposer_pays_it() {
        let frames = vec![
            frame(9, "0x09", &[], BUILDER, PAYOUT, 0, false),
            json!({"blockHash": "0xbb", "transactionPosition": 9, "transactionHash": "0x09",
                "traceAddress": [0], "type": "suicide",
                "action": {"address": PAYOUT, "refundAddress": PROPOSER, "balance": "0x5"}}),
        ];
        let (txs, total) = payment_legs(&frames, "0xbb", BUILDER, PROPOSER).unwrap();
        assert_eq!(
            (txs, total),
            (vec!["0x09".to_string()], alloy_primitives::U256::from(5))
        );
    }

    #[test]
    fn a_frame_of_another_block_is_refused() {
        let mut f = frame(1, "0x01", &[], BUILDER, PROPOSER, 4, false);
        f["blockHash"] = json!("0xcc");
        assert!(payment_legs(&[f], "0xbb", BUILDER, PROPOSER).is_err());
    }
}
