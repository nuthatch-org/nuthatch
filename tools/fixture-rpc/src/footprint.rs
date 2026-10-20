//! The chain behind the single-nest RAM budget, backfill throughput and tip lag.
//!
//! The RAM budget is a required check and used to be measured against free public endpoints. Fork
//! PRs never see repository secrets, so an external contribution could not satisfy it, and peak RSS
//! depended on how many blocks a rate-limited endpoint served that morning (issue #260). Serving the
//! chain ourselves fixes both: every run indexes the same logs, so a change in peak RSS is a change
//! in nuthatch.
//!
//! With `moving` (tip-lag.sh) the tip advances one block per `fixture_advance` call, and the chain
//! records when each block appeared and when an `eth_blockNumber` first reported it. Without it the
//! chain is fixed at TIP.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};

use crate::{hex, hex_u64, int, quoted, truthy, Fault, Fixture};

const CHAIN_ID: u64 = 1;
pub const TIP: u64 = 20_000;
pub const LOGS_PER_BLOCK: u64 = 4;
// `Transfer(address,address,uint256)`
const TOPIC0: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const CONTRACT: &str = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
const FINALITY: u64 = 64;

pub struct Footprint {
    moving: bool,
    logs_per_block: u64,
    txs_per_block: u64,
    state: Mutex<State>,
}

struct State {
    tip: u64,
    appeared: HashMap<u64, f64>,
    seen: HashMap<u64, f64>,
}

impl Footprint {
    pub fn new(moving: bool) -> Self {
        Self::dense(moving, TIP, LOGS_PER_BLOCK, 0)
    }

    /// The same chain at another tip and density. `txs_per_block` puts that many hashes in each
    /// header, as a node answers `eth_getBlockByNumber(n, false)`; zero leaves the field out.
    pub fn dense(moving: bool, tip: u64, logs_per_block: u64, txs_per_block: u64) -> Self {
        Self {
            moving,
            logs_per_block,
            txs_per_block,
            state: Mutex::new(State {
                tip,
                appeared: HashMap::new(),
                seen: HashMap::new(),
            }),
        }
    }
}

fn now_ms() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64() * 1000.0)
}

fn word(n: u128) -> String {
    format!("0x{n:064x}")
}

fn logs_for(from: u64, to: u64, tip: u64, per_block: u64) -> Vec<Value> {
    let mut out = Vec::new();
    for b in from.max(1)..=to.min(tip) {
        for i in 0..per_block {
            let idx = u128::from(b * per_block + i);
            out.push(json!({
                "address": CONTRACT,
                "topics": [TOPIC0, word(idx % 512), word((idx * 7 + 3) % 512)],
                // Large enough to exercise the u256-as-text path rather than a small int.
                "data": word(1_000_000_000_000_000_000 * (idx % 97 + 1)),
                "blockNumber": hex(b),
                "blockHash": format!("0x{b:064x}"),
                "transactionHash": word(idx),
                "transactionIndex": hex(i),
                // Per block, as a node numbers them: nuthatch refuses one wider than 20 bits (#2228).
                "logIndex": hex(i),
                "removed": false,
            }));
        }
    }
    out
}

impl Fixture for Footprint {
    fn call(&self, req: &Value) -> Result<Value, Fault> {
        let m = req.get("method");
        let p = req
            .get("params")
            .filter(|p| truthy(p))
            .cloned()
            .unwrap_or_else(|| json!([]));
        let mut s = self.state.lock().unwrap();
        match m.and_then(Value::as_str) {
            Some("eth_chainId") => Ok(json!(hex(CHAIN_ID))),
            Some("eth_blockNumber") => {
                if self.moving {
                    let tip = s.tip;
                    s.seen.entry(tip).or_insert_with(now_ms);
                }
                Ok(json!(hex(s.tip)))
            }
            Some("eth_getBlockByNumber") => {
                let tag = p.get(0).ok_or(Fault::Malformed)?;
                let mut n = match tag.as_str() {
                    Some(t) if t.starts_with("0x") => hex_u64(tag).ok_or(Fault::Malformed)?,
                    _ => s.tip,
                };
                if self.moving {
                    if matches!(tag.as_str(), Some("finalized" | "safe")) {
                        n = s.tip.saturating_sub(FINALITY);
                    }
                    if n > s.tip {
                        return Ok(Value::Null);
                    }
                }
                let mut header = json!({
                    "number": hex(n),
                    "hash": format!("0x{n:064x}"),
                    "parentHash": format!("0x{:064x}", n.saturating_sub(1)),
                    // Deterministic, and monotonic so finality maths behaves.
                    "timestamp": hex(1_600_000_000 + n * 12),
                });
                if self.txs_per_block > 0 {
                    if p.get(1).is_some_and(truthy) {
                        return Err(Fault::Rpc(-32601, "footprint mock: hashes only".into()));
                    }
                    let base = u128::from(n) << 32;
                    let txs: Vec<Value> = (0..u128::from(self.txs_per_block))
                        .map(|i| json!(word(base + i)))
                        .collect();
                    header["transactions"] = Value::Array(txs);
                }
                Ok(header)
            }
            Some("eth_getLogs") => {
                let filter = p.get(0).filter(|f| f.is_object()).ok_or(Fault::Malformed)?;
                let bound = |k: &str, default: u64| match filter.get(k) {
                    None => Ok(default),
                    Some(v) if v == "latest" => Ok(s.tip),
                    Some(v) => hex_u64(v).ok_or(Fault::Malformed),
                };
                let (from, to) = (bound("fromBlock", 0)?, bound("toBlock", s.tip)?);
                Ok(Value::Array(logs_for(from, to, s.tip, self.logs_per_block)))
            }
            Some("fixture_advance") if self.moving => {
                s.tip += 1;
                let at = now_ms();
                let tip = s.tip;
                s.appeared.insert(tip, at);
                Ok(json!({"tip": tip, "at": at}))
            }
            Some("fixture_seen") if self.moving => {
                let n = p.get(0).and_then(int).ok_or(Fault::Malformed)?;
                Ok(s.seen.get(&n).map_or(Value::Null, |t| json!(t)))
            }
            _ => Err(Fault::Rpc(
                -32601,
                format!("footprint mock: unimplemented {}", quoted(m)),
            )),
        }
    }
}
