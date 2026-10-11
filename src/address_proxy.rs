//! The Etherscan `proxy` and `logs` modules on a rotki-mode nest (RFC-0063 §5): the JSON-RPC
//! calls rotki makes of an indexer when its own nodes fail, answered from the nest's RPC in
//! Etherscan's shapes, so a nest can stand in for Etherscan entirely.
//!
//! Nothing here is stored. The nest's own rows are Etherscan account records, from which an RPC
//! transaction or receipt object cannot be rebuilt whole, so every call is forwarded.

use anyhow::{anyhow, bail, Result};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// One JSON-RPC call to the nest's RPC: method, params, result.
pub type Forward =
    Arc<dyn Fn(String, Value) -> futures::future::BoxFuture<'static, Result<Value>> + Send + Sync>;

/// The widest block span a `getLogs` range is asked in. Spans start at [`FIRST_SPAN`] and double
/// while answers stay small, so one answer never holds a dense range: a 10,000-block span of a busy
/// token's transfers is a gigabyte, for a page of a thousand.
const LOG_SPAN: u64 = 10_000;
const FIRST_SPAN: u64 = 16;
/// Etherscan's most logs one page may hold, and the deepest a page may reach.
const MAX_PAGE: u64 = 1_000;
const MAX_DEPTH: u64 = 10_000;
/// The most `eth_getLogs` calls one `getLogs` may cost. A range that needs more answers incomplete
/// rather than half of itself.
const MAX_LOG_CALLS: usize = 64;

/// The answer to a `proxy` or `logs` request, or `None` for any other module.
pub async fn answer(
    q: &HashMap<String, String>,
    chain_id: u64,
    forward: &Forward,
) -> Option<Value> {
    let module = q.get("module").map(String::as_str)?;
    if module != "proxy" && module != "logs" {
        return None;
    }
    if let Some(c) = q.get("chainid") {
        if c.parse::<u64>().ok() != Some(chain_id) {
            return Some(crate::address_history::unsupported(&format!(
                "chainid {c} is not this nest's chain {chain_id}"
            )));
        }
    }
    let action = q.get("action").map(String::as_str).unwrap_or("");
    let result = if module == "proxy" {
        proxy(q, action, forward).await
    } else if action == "getLogs" {
        // A failed range is one the nest could not answer, never an error in the caller's request,
        // so it reads incomplete and the caller asks its next source.
        logs(q, forward)
            .await
            .or_else(|e| Ok(Answer::Incomplete(format!("getLogs: {e:#}"))))
    } else {
        return Some(crate::address_history::unsupported(&format!(
            "logs/{action} is not served by this nest"
        )));
    };
    Some(match result {
        Ok(Answer::Rpc(result)) => json!({ "jsonrpc": "2.0", "id": 1, "result": result }),
        Ok(Answer::Logs(rows)) => json!({ "status": "1", "message": "OK", "result": rows }),
        Ok(Answer::Unsupported(why)) => crate::address_history::unsupported(&why),
        Ok(Answer::Incomplete(why)) => crate::address_history::incomplete(&why),
        Err(e) => json!({ "status": "0", "message": "NOTOK", "result": format!("Error! {e:#}") }),
    })
}

enum Answer {
    Rpc(Value),
    Logs(Vec<Value>),
    Unsupported(String),
    Incomplete(String),
}

async fn proxy(q: &HashMap<String, String>, action: &str, forward: &Forward) -> Result<Answer> {
    let get = |k: &str| {
        q.get(k)
            .map(String::as_str)
            .ok_or_else(|| anyhow!("Missing {k}"))
    };
    let tag = || q.get("tag").map_or("latest", String::as_str);
    let params = match action {
        "eth_blockNumber" => json!([]),
        "eth_getBlockByNumber" => json!([get("tag")?, get("boolean")? == "true"]),
        "eth_getTransactionByHash" | "eth_getTransactionReceipt" => json!([get("txhash")?]),
        "eth_getCode" => json!([get("address")?, tag()]),
        "eth_call" => json!([{ "to": get("to")?, "data": get("data")? }, tag()]),
        other => {
            return Ok(Answer::Unsupported(format!(
                "proxy/{other} is not served by this nest"
            )))
        }
    };
    Ok(Answer::Rpc(forward(action.to_string(), params).await?))
}

/// `logs/getLogs` in Etherscan's shape: every quantity hex, with the block's timestamp and the
/// transaction's gas price and gas used, which `eth_getLogs` does not carry.
async fn logs(q: &HashMap<String, String>, forward: &Forward) -> Result<Answer> {
    let block = |k: &str| -> Result<Option<u64>> {
        let v = q.get(k).map_or("latest", String::as_str);
        Ok(match v {
            "earliest" => Some(0),
            "latest" | "pending" | "finalized" | "safe" => None,
            n => Some(n.parse::<u64>().map_err(|_| anyhow!("invalid {k} `{n}`"))?),
        })
    };
    let tag = |k: &str| q.get(k).map_or("latest", String::as_str).to_string();
    // Etherscan combines topics with and/or operators; eth_getLogs can only AND them.
    if q.iter()
        .any(|(k, v)| k.starts_with("topic") && k.ends_with("_opr") && v != "and")
    {
        return Ok(Answer::Unsupported(
            "getLogs with an `or` topic operator is not served by this nest".into(),
        ));
    }
    let topics: Vec<Value> = (0..4)
        .map(|i| {
            q.get(&format!("topic{i}"))
                .map_or(Value::Null, |t| json!(t))
        })
        .collect();
    let last = topics
        .iter()
        .rposition(|t| !t.is_null())
        .map_or(0, |i| i + 1);
    let number_of = |t: String| async move {
        let b = forward("eth_getBlockByNumber".into(), json!([t, false])).await?;
        crate::address_discovery::hex_u64(&b["number"])
    };
    let from = match block("fromBlock")? {
        Some(n) => n,
        None => number_of(tag("fromBlock")).await?,
    };
    let to = match block("toBlock")? {
        Some(n) => n,
        None => number_of(tag("toBlock")).await?,
    };
    // `page` and `offset` as Etherscan takes them; a caller that sends neither gets the first 1,000.
    let num = |k: &str, default: u64| -> Result<u64> {
        match q.get(k).map(String::as_str).filter(|v| !v.is_empty()) {
            None => Ok(default),
            Some(v) => v.parse::<u64>().map_err(|_| anyhow!("invalid {k} `{v}`")),
        }
    };
    let (page, offset) = (num("page", 1)?, num("offset", MAX_PAGE)?);
    if page == 0 || offset == 0 || offset > MAX_PAGE || page.saturating_mul(offset) > MAX_DEPTH {
        return Ok(Answer::Unsupported(format!(
            "getLogs pages hold 1 to {MAX_PAGE} logs and reach at most {MAX_DEPTH} deep"
        )));
    }
    let need = usize::try_from(page * offset).expect("at most MAX_DEPTH");
    // A range is asked in spans the provider takes, oldest first, and stops once past what the page
    // needs: whatever lies beyond it a caller asks for next.
    let mut logs = Vec::new();
    let (mut start, mut span, mut calls) = (from, FIRST_SPAN, 0);
    while start <= to && logs.len() < need {
        if calls == MAX_LOG_CALLS {
            return Ok(Answer::Incomplete(format!(
                "getLogs over [{from}, {to}] needs more than {MAX_LOG_CALLS} calls to the RPC; ask for a narrower range"
            )));
        }
        calls += 1;
        let end = start.saturating_add(span - 1).min(to);
        let mut filter = Map::new();
        filter.insert("fromBlock".into(), json!(format!("0x{start:x}")));
        filter.insert("toBlock".into(), json!(format!("0x{end:x}")));
        if let Some(a) = q.get("address") {
            filter.insert("address".into(), json!(a));
        }
        filter.insert("topics".into(), Value::Array(topics[..last].to_vec()));
        match forward("eth_getLogs".into(), json!([filter])).await {
            Ok(found) => {
                let Value::Array(found) = found else {
                    bail!("eth_getLogs answered a non-list");
                };
                if found.len() < need / 4 {
                    span = span.saturating_mul(2).min(LOG_SPAN);
                }
                let room = need - logs.len();
                logs.extend(found.into_iter().take(room));
                start = end + 1;
            }
            Err(e) if end > start && crate::address_discovery::range_shaped(&format!("{e:#}")) => {
                let stated = crate::address_discovery::stated_limit(&format!("{e:#}"));
                span = stated.filter(|n| *n < span).unwrap_or(span / 2).max(1);
            }
            Err(e) => return Err(e),
        }
    }
    // The page, exactly, mid-block if need be, as Etherscan answers: rotki continues only on a full
    // page, from its last block, and drops what it already has, so a short page must only ever mean
    // the range is done. A full page inside one block would be asked for again from that block
    // forever, so that is refused.
    let skip = need - usize::try_from(offset).expect("at most MAX_PAGE");
    let mut logs: Vec<Value> = logs.into_iter().skip(skip).collect();
    let full = usize::try_from(offset).expect("at most MAX_PAGE");
    // One log is a page of one, which any caller pages past by `page`.
    if logs.len() >= full && full > 1 {
        let block = |l: &Value| l.get("blockNumber").cloned();
        if block(&logs[0]) == block(&logs[full - 1]) {
            return Ok(Answer::Unsupported(format!(
                "block {} holds at least {full} matching logs, a whole page, so it cannot be paged past",
                block(&logs[0]).unwrap_or(Value::Null)
            )));
        }
        logs.truncate(full);
    }

    let mut paid: BTreeMap<String, (Value, Value)> = BTreeMap::new();
    let mut stamped: BTreeMap<String, Value> = BTreeMap::new();
    let mut rows = Vec::with_capacity(logs.len());
    for log in logs {
        let field = |k: &str| -> Result<Value> {
            log.get(k)
                .filter(|v| !v.is_null())
                .cloned()
                .ok_or_else(|| anyhow!("a log without {k}"))
        };
        let tx = field("transactionHash")?
            .as_str()
            .ok_or_else(|| anyhow!("a log's transactionHash is not a string"))?
            .to_string();
        if !paid.contains_key(&tx) {
            let receipt = forward("eth_getTransactionReceipt".into(), json!([tx])).await?;
            let get = |k: &str| {
                receipt
                    .get(k)
                    .filter(|v| !v.is_null())
                    .cloned()
                    .ok_or_else(|| anyhow!("the receipt of {tx} has no {k}"))
            };
            paid.insert(tx.clone(), (get("effectiveGasPrice")?, get("gasUsed")?));
        }
        let number = field("blockNumber")?;
        let key = number.as_str().unwrap_or_default().to_string();
        let ts = match log.get("blockTimestamp").filter(|v| !v.is_null()) {
            Some(ts) => ts.clone(),
            None => match stamped.get(&key) {
                Some(ts) => ts.clone(),
                None => {
                    let header =
                        forward("eth_getBlockByNumber".into(), json!([number, false])).await?;
                    let ts = header
                        .get("timestamp")
                        .cloned()
                        .ok_or_else(|| anyhow!("block {key} has no timestamp"))?;
                    stamped.insert(key, ts.clone());
                    ts
                }
            },
        };
        if log.get("removed").and_then(Value::as_bool) == Some(true) {
            bail!("the RPC answered a removed log in {tx}");
        }
        let (gas_price, gas_used) = paid[&tx].clone();
        rows.push(json!({
            "address": field("address")?,
            "topics": field("topics")?,
            "data": field("data")?,
            "blockNumber": number,
            "blockHash": field("blockHash")?,
            "timeStamp": ts,
            "gasPrice": gas_price,
            "gasUsed": gas_used,
            "logIndex": field("logIndex")?,
            "transactionHash": tx,
            "transactionIndex": field("transactionIndex")?,
        }));
    }
    Ok(Answer::Logs(rows))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    type Calls = Arc<Mutex<Vec<(String, Value)>>>;

    /// A forwarder answering from `f`, recording every call.
    fn fake(f: impl Fn(&str, &Value) -> Value + Send + Sync + 'static) -> (Forward, Calls) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let seen = calls.clone();
        let f = Arc::new(f);
        let forward: Forward = Arc::new(move |m: String, p: Value| {
            seen.lock().unwrap().push((m.clone(), p.clone()));
            let out = f(&m, &p);
            Box::pin(async move { Ok(out) })
        });
        (forward, calls)
    }

    fn q(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[tokio::test]
    async fn proxy_calls_reach_the_rpc_as_json_rpc_and_come_back_in_etherscans_shape() {
        let (forward, calls) = fake(|m, _| json!(format!("answer to {m}")));
        for (pairs, method, params) in [
            (
                vec![("action", "eth_blockNumber")],
                "eth_blockNumber",
                json!([]),
            ),
            (
                vec![
                    ("action", "eth_getBlockByNumber"),
                    ("tag", "0x10"),
                    ("boolean", "false"),
                ],
                "eth_getBlockByNumber",
                json!(["0x10", false]),
            ),
            (
                vec![("action", "eth_getTransactionReceipt"), ("txhash", "0xaa")],
                "eth_getTransactionReceipt",
                json!(["0xaa"]),
            ),
            (
                vec![("action", "eth_getCode"), ("address", "0xbb")],
                "eth_getCode",
                json!(["0xbb", "latest"]),
            ),
            (
                vec![
                    ("action", "eth_call"),
                    ("to", "0xcc"),
                    ("data", "0x01"),
                    ("tag", "0x5"),
                ],
                "eth_call",
                json!([{"to": "0xcc", "data": "0x01"}, "0x5"]),
            ),
        ] {
            let mut p = q(&pairs);
            p.insert("module".into(), "proxy".into());
            let got = answer(&p, 1, &forward).await.unwrap();
            assert_eq!(got["result"], format!("answer to {method}"), "{method}");
            assert_eq!(got["jsonrpc"], "2.0");
            assert_eq!(
                calls.lock().unwrap().last().unwrap(),
                &(method.to_string(), params)
            );
        }
        let refused = answer(
            &q(&[("module", "proxy"), ("action", "eth_sendRawTransaction")]),
            1,
            &forward,
        )
        .await
        .unwrap();
        assert!(refused["result"]
            .as_str()
            .unwrap()
            .starts_with("NUTHATCH_UNSUPPORTED:"));
        let other_chain = answer(
            &q(&[
                ("module", "proxy"),
                ("action", "eth_blockNumber"),
                ("chainid", "10"),
            ]),
            1,
            &forward,
        )
        .await
        .unwrap();
        assert!(other_chain["result"]
            .as_str()
            .unwrap()
            .contains("chainid 10"));
        assert!(answer(
            &q(&[("module", "account"), ("action", "txlist")]),
            1,
            &forward
        )
        .await
        .is_none());
    }

    /// Etherscan's log carries the block's timestamp and its transaction's gas price and gas used;
    /// a receipt is read once per transaction, and a header only when the log lacks the timestamp.
    #[tokio::test]
    async fn get_logs_adds_what_etherscan_adds() {
        let log = |tx: &str, index: &str, ts: Option<&str>| {
            let mut l = json!({"address": "0xc0", "topics": ["0xt0", "0xt1"], "data": "0x",
                "blockNumber": "0x64", "blockHash": "0xbh", "logIndex": index,
                "transactionHash": tx, "transactionIndex": "0x2", "removed": false});
            if let Some(ts) = ts {
                l["blockTimestamp"] = json!(ts);
            }
            l
        };
        let logs = json!([
            log("0xa", "0x0", Some("0x5f")),
            log("0xa", "0x1", Some("0x5f")),
            log("0xb", "0x2", None)
        ]);
        let (forward, calls) = fake(move |m, p| match m {
            "eth_getLogs" => logs.clone(),
            "eth_getTransactionReceipt" => {
                json!({"effectiveGasPrice": format!("{}9", p[0].as_str().unwrap()), "gasUsed": "0x5208"})
            }
            "eth_getBlockByNumber" => json!({"timestamp": "0x60"}),
            other => panic!("unexpected {other}"),
        });
        let got = answer(
            &q(&[
                ("module", "logs"),
                ("action", "getLogs"),
                ("fromBlock", "100"),
                ("toBlock", "100"),
                ("address", "0xc0"),
                ("topic0", "0xt0"),
                ("topic2", "0xt2"),
                ("topic0_2_opr", "and"),
            ]),
            1,
            &forward,
        )
        .await
        .unwrap();
        assert_eq!(got["status"], "1");
        let rows = got["result"].as_array().unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(
            rows[0],
            json!({"address": "0xc0", "topics": ["0xt0", "0xt1"], "data": "0x", "blockNumber": "0x64",
                "blockHash": "0xbh", "timeStamp": "0x5f", "gasPrice": "0xa9", "gasUsed": "0x5208",
                "logIndex": "0x0", "transactionHash": "0xa", "transactionIndex": "0x2"})
        );
        assert_eq!(rows[2]["timeStamp"], "0x60");
        assert_eq!(rows[2]["gasPrice"], "0xb9");
        let calls = calls.lock().unwrap().clone();
        assert_eq!(
            calls[0],
            (
                "eth_getLogs".into(),
                json!([{"fromBlock": "0x64", "toBlock": "0x64", "address": "0xc0",
                "topics": ["0xt0", null, "0xt2"]}])
            )
        );
        let receipts = calls
            .iter()
            .filter(|(m, _)| m == "eth_getTransactionReceipt")
            .count();
        assert_eq!(receipts, 2, "one receipt per transaction");

        // Past the cap the answer ends at a block boundary, and one block over it is refused.
        let dense = |blocks: Vec<u64>| {
            let logs: Vec<Value> = blocks
                .iter()
                .enumerate()
                .map(|(i, b)| {
                    json!({"address": "0xc0", "topics": [], "data": "0x", "blockNumber": format!("0x{b:x}"),
                        "blockHash": "0xbh", "logIndex": format!("0x{i:x}"), "transactionHash": "0xa",
                        "transactionIndex": "0x0", "blockTimestamp": "0x1"})
                })
                .collect();
            fake(move |m, _| match m {
                "eth_getLogs" => json!(logs),
                _ => json!({"effectiveGasPrice": "0x1", "gasUsed": "0x1"}),
            })
            .0
        };
        let ask = q(&[
            ("module", "logs"),
            ("action", "getLogs"),
            ("fromBlock", "1"),
            ("toBlock", "9"),
        ]);
        // rotki continues only on a full page, from its last block, deduplicating; a short page
        // means the range is done (rotki-v2 node_inquirer.py). So the page is exactly 1,000.
        let mut blocks = vec![100; 999];
        blocks.extend(vec![101; 2]);
        let cut = answer(&ask, 1, &dense(blocks)).await.unwrap();
        let rows = cut["result"].as_array().unwrap();
        assert_eq!(rows.len(), 1_000, "a full page, so the caller asks again");
        assert_eq!(rows[999]["blockNumber"], "0x65", "cut inside block 101");
        let one = answer(&ask, 1, &dense(vec![1; 1001])).await.unwrap();
        assert!(one["result"]
            .as_str()
            .unwrap()
            .starts_with("NUTHATCH_UNSUPPORTED:"));
        // Exactly a page's worth in one block with more after it: continuing from that block would
        // return the same page forever.
        let mut full = vec![7; 1_000];
        full.push(8);
        let looped = answer(&ask, 1, &dense(full)).await.unwrap();
        assert!(
            looped["result"]
                .as_str()
                .unwrap()
                .starts_with("NUTHATCH_UNSUPPORTED:"),
            "{looped}"
        );
        // And with nothing after it: a full page is always continued from its last block.
        let exact = answer(&ask, 1, &dense(vec![7; 1_000])).await.unwrap();
        assert!(
            exact["result"]
                .as_str()
                .unwrap()
                .starts_with("NUTHATCH_UNSUPPORTED:"),
            "{exact}"
        );

        let or = answer(
            &q(&[
                ("module", "logs"),
                ("action", "getLogs"),
                ("topic0", "0xt0"),
                ("topic1", "0xt1"),
                ("topic0_1_opr", "or"),
            ]),
            1,
            &forward,
        )
        .await
        .unwrap();
        assert!(or["result"]
            .as_str()
            .unwrap()
            .starts_with("NUTHATCH_UNSUPPORTED:"));
    }

    /// A forwarder whose `eth_getLogs` refuses spans over `limit` blocks, with one log every 50,000
    /// blocks, counting its calls.
    fn capped(limit: u64) -> (Forward, Arc<Mutex<u64>>) {
        let calls = Arc::new(Mutex::new(0));
        let n = calls.clone();
        let forward: Forward = Arc::new(move |m: String, p: Value| {
            let n = n.clone();
            Box::pin(async move {
                if m != "eth_getLogs" {
                    return Ok(json!({"effectiveGasPrice": "0x1", "gasUsed": "0x1"}));
                }
                *n.lock().unwrap() += 1;
                let from = crate::address_discovery::hex_u64(&p[0]["fromBlock"])?;
                let to = crate::address_discovery::hex_u64(&p[0]["toBlock"])?;
                if to - from + 1 > limit {
                    bail!("query exceeds max range of {limit} blocks");
                }
                Ok(json!((from..=to).filter(|b| b % 50_000 == 0).map(|b| json!({
                    "address": "0xc0", "topics": [], "data": "0x", "blockNumber": format!("0x{b:x}"),
                    "blockHash": "0xbh", "logIndex": "0x0", "transactionHash": format!("0x{b:x}"),
                    "transactionIndex": "0x0", "blockTimestamp": "0x1"})).collect::<Vec<_>>()))
            })
        });
        (forward, calls)
    }

    /// rotki asks for ranges of ~300,000 blocks, and a provider refuses more than it allows in one
    /// call (cold test M3, 2026-10-11). The range is asked in spans the provider takes and answered
    /// whole; one that would cost more calls than allowed is refused, never answered in part.
    #[tokio::test]
    async fn a_long_getlogs_range_is_split_within_the_providers_limit() {
        let ask = q(&[
            ("module", "logs"),
            ("action", "getLogs"),
            ("fromBlock", "1"),
            ("toBlock", "300000"),
        ]);
        let (forward, calls) = capped(10_000);
        let got = answer(&ask, 1, &forward).await.unwrap();
        assert_eq!(got["status"], "1", "{got}");
        let blocks: Vec<&str> = got["result"]
            .as_array()
            .unwrap()
            .iter()
            .map(|l| l["blockNumber"].as_str().unwrap())
            .collect();
        assert_eq!(
            blocks,
            ["0xc350", "0x186a0", "0x249f0", "0x30d40", "0x3d090", "0x493e0"]
        );
        assert_eq!(
            *calls.lock().unwrap(),
            39,
            "spans of 16 blocks doubling to the 10,000 the provider takes, then 10,000 at a time"
        );

        let (forward, calls) = capped(2_000);
        let refused = answer(&ask, 1, &forward).await.unwrap();
        let r = refused["result"].as_str().unwrap();
        assert!(r.starts_with("NUTHATCH_INCOMPLETE:"), "{refused}");
        assert_eq!(*calls.lock().unwrap() as usize, MAX_LOG_CALLS);
    }

    /// A dense range (a busy token's transfers, fifty logs a block over 400,000 blocks) is asked in
    /// spans that start small, so no single answer holds more than a few pages, and the request
    /// stops at the page (cold test B1, 2026-10-11: one request held 1.2 GB for 1,000 rows).
    #[tokio::test]
    async fn a_dense_range_never_holds_more_than_a_few_pages() {
        let largest = Arc::new(Mutex::new(0usize));
        let seen = largest.clone();
        let forward: Forward = Arc::new(move |m: String, p: Value| {
            let seen = seen.clone();
            Box::pin(async move {
                if m != "eth_getLogs" {
                    return Ok(json!({"effectiveGasPrice": "0x1", "gasUsed": "0x1"}));
                }
                let from = crate::address_discovery::hex_u64(&p[0]["fromBlock"])?;
                let to = crate::address_discovery::hex_u64(&p[0]["toBlock"])?;
                let logs: Vec<Value> = (from..=to)
                    .flat_map(|b| (0..50).map(move |i| json!({
                        "address": "0xc0", "topics": [], "data": "0x", "blockNumber": format!("0x{b:x}"),
                        "blockHash": "0xbh", "logIndex": format!("0x{i:x}"), "transactionHash": format!("0x{b:x}{i:02x}"),
                        "transactionIndex": "0x0", "blockTimestamp": "0x1"})))
                    .collect();
                let mut l = seen.lock().unwrap();
                *l = (*l).max(logs.len());
                Ok(json!(logs))
            })
        });
        let ask = q(&[
            ("module", "logs"),
            ("action", "getLogs"),
            ("fromBlock", "25700000"),
            ("toBlock", "26100000"),
        ]);
        let got = answer(&ask, 1, &forward).await.unwrap();
        assert_eq!(got["result"].as_array().unwrap().len(), 1_000);
        assert!(
            *largest.lock().unwrap() <= 1_600,
            "{} logs in one answer",
            largest.lock().unwrap()
        );

        // `page` reaches past the first thousand.
        let second = answer(
            &q(&[
                ("module", "logs"),
                ("action", "getLogs"),
                ("fromBlock", "25700000"),
                ("toBlock", "26100000"),
                ("page", "2"),
                ("offset", "100"),
            ]),
            1,
            &forward,
        )
        .await
        .unwrap();
        let rows = second["result"].as_array().unwrap();
        assert_eq!(rows.len(), 100);
        assert_eq!(
            rows[0]["blockNumber"], "0x18826a2",
            "the 101st log: block 25700002, its first"
        );
    }

    /// A sparse stretch widens the span to the provider's limit; the dense stretch after it is then
    /// refused by the body cap, as the RPC client refuses it, and narrowed rather than held whole.
    #[tokio::test]
    async fn a_dense_stretch_after_a_sparse_one_is_narrowed_by_the_body_cap() {
        let largest = Arc::new(Mutex::new(0usize));
        let seen = largest.clone();
        let forward: Forward = Arc::new(move |m: String, p: Value| {
            let seen = seen.clone();
            Box::pin(async move {
                if m != "eth_getLogs" {
                    return Ok(json!({"effectiveGasPrice": "0x1", "gasUsed": "0x1"}));
                }
                let from = crate::address_discovery::hex_u64(&p[0]["fromBlock"])?;
                let to = crate::address_discovery::hex_u64(&p[0]["toBlock"])?;
                // Empty under 50,000, then 200 logs a block; the cap passes 4,000 in one body.
                let logs: Vec<Value> = (from.max(50_000)..=to)
                    .flat_map(|b| (0..200).map(move |i| json!({
                        "address": "0xc0", "topics": [], "data": "0x", "blockNumber": format!("0x{b:x}"),
                        "blockHash": "0xbh", "logIndex": format!("0x{i:x}"), "transactionHash": format!("0x{b:x}{i:02x}"),
                        "transactionIndex": "0x0", "blockTimestamp": "0x1"})))
                    .take(4_001)
                    .collect();
                if logs.len() > 4_000 && from < to {
                    bail!("response passed the 4194304-byte getLogs body cap; narrowing the range");
                }
                let mut l = seen.lock().unwrap();
                *l = (*l).max(logs.len());
                Ok(json!(logs))
            })
        });
        let ask = q(&[
            ("module", "logs"),
            ("action", "getLogs"),
            ("fromBlock", "0"),
            ("toBlock", "60000"),
        ]);
        let got = answer(&ask, 1, &forward).await.unwrap();
        let rows = got["result"].as_array().unwrap();
        assert_eq!(rows.len(), 1_000, "{got}");
        assert_eq!(rows[0]["blockNumber"], format!("0x{:x}", 50_000));
        assert!(*largest.lock().unwrap() <= 4_000);
    }
}
