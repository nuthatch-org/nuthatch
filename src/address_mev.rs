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

/// Every relay's deliveries for `block`, from the cache or asked now (always now when `fresh`). A
/// relay that cannot be asked fails the lookup: a missing answer is not an absent delivery. Only a
/// finalized block's answer is cached.
async fn relay_record(
    history: &AddressHistory,
    relays: &Relays,
    block: u64,
    fresh: bool,
) -> Result<Map<String, Value>> {
    if !fresh {
        if let Some(r) = history.relay_record(block)? {
            return Ok(r);
        }
    }
    let mut delivered = Vec::new();
    for (name, _) in RELAYS {
        for trace in relays(name, block).await? {
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
    history.cache_relay_record(block, &record)?;
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
    fresh: bool,
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
        let payment = incoming.get(&b).and_then(|paid| {
            paid.iter()
                .rev()
                .find(|t| {
                    (text(t, "from") == miner && miner != a)
                        || Some(text(t, "transactionIndex")) == last
                })
                .copied()
        });
        let self_built = mined.get(&b);
        if self_built.is_none() && payment.is_none() {
            continue;
        }
        let record = relay_record(history, relays, b, fresh).await?;
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
        put(
            "paymentTx",
            payment.map(|p| text(p, "hash")).unwrap_or_default(),
        );
        put(
            "paymentValue",
            payment.map(|p| text(p, "value")).unwrap_or_default(),
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

/// The cursor's MEV pass: each watched address is covered as far as both its mined blocks and its
/// normal transactions are, up to `limit`, a window at a time.
pub async fn mev_pass<M: Rpc, T: Rpc>(
    d: &Discoverer<Counted<M>, Counted<T>>,
    relays: &Relays,
    history: &AddressHistory,
    watched: &[String],
    start: u64,
    limit: u64,
    now: u64,
) -> Result<()> {
    use crate::address_discovery::next_uncovered_for;
    for a in watched {
        loop {
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
                break;
            };
            let to = mined
                .min(txs)
                .min(internals)
                .min(from.saturating_add(MEV_WINDOW - 1))
                .min(limit);
            if to < from {
                break;
            }
            produced_window(d, relays, history, a, (from, to), now).await?;
        }
    }
    Ok(())
}

/// Cover `[from, to]` of `a`'s produced blocks, its prerequisites covered, and put the blocks without
/// relay evidence on the re-ask schedule.
pub async fn produced_window<M: Rpc, T: Rpc>(
    d: &Discoverer<Counted<M>, Counted<T>>,
    relays: &Relays,
    history: &AddressHistory,
    a: &str,
    (from, to): (u64, u64),
    now: u64,
) -> Result<()> {
    let generation = history.generation()?;
    let anchor = crate::address_mode::anchor_for(d, history, to).await?;
    let found = produced_blocks(d, relays, history, a, (from, to), false).await?;
    crate::address_mode::check_anchor(d, anchor).await?;
    history.record_anchored(
        Action::ProducedBlocks,
        a,
        &found.rows,
        (from, to),
        generation,
        anchor,
    )?;
    for b in found.unrecorded {
        history.mev_absent(b, now)?;
    }
    Ok(())
}

/// Ask the relays again about each block on the re-ask schedule that is due: one whose last ask is a
/// day old and whose first is under 30 days old. A delivery found now upgrades the row, or inserts
/// one for a builder payment that had none; either way the generation moves.
pub async fn reask_pass<M: Rpc, T: Rpc>(
    d: &Discoverer<Counted<M>, Counted<T>>,
    relays: &Relays,
    history: &AddressHistory,
    watched: &[String],
    now: u64,
) -> Result<()> {
    for (block, first, last) in history.mev_pending()? {
        if now.saturating_sub(first) >= REASK_FOR_SECS {
            history.mev_settled(block)?;
            continue;
        }
        if now.saturating_sub(last) < REASK_EVERY_SECS {
            continue;
        }
        // Asked fresh, so the cache holds the newest answer for the rows below.
        relay_record(history, relays, block, true).await?;
        let mut settled = true;
        for a in watched {
            let covered = history
                .coverage(Action::ProducedBlocks, a)?
                .iter()
                .any(|(f, t)| *f <= block && block <= *t);
            if !covered {
                continue;
            }
            let found = produced_blocks(d, relays, history, a, (block, block), false).await?;
            if !found.unrecorded.is_empty() {
                settled = false;
            }
            let before = history.rows_in(Action::ProducedBlocks, a, (block, block))?;
            let after: Vec<Map<String, Value>> =
                found.rows.iter().map(|r| r.record.clone()).collect();
            if before != after {
                history.replace_block_rows(Action::ProducedBlocks, a, block, &found.rows)?;
            }
        }
        if settled {
            history.mev_settled(block)?;
        } else {
            history.mev_absent(block, now)?;
        }
    }
    Ok(())
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
        let found = produced_blocks(&d, &rel, &h, A, (100, 199), false)
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
        produced_blocks(&d, &rel, &h, A, (100, 199), false)
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
        assert!(relay_record(&h, &down, 100, false).await.is_err());
        assert!(h.relay_record(100).unwrap().is_none());

        // A block past what the nest serves is asked about but not kept: a relay may yet learn
        // of it.
        let (rel, asked) = relays(vec![]);
        relay_record(&h, &rel, 2_000, false).await.unwrap();
        relay_record(&h, &rel, 2_000, false).await.unwrap();
        assert_eq!(*asked.lock().unwrap(), 2 * 6);
        assert!(h.relay_record(2_000).unwrap().is_none());
    }
}
