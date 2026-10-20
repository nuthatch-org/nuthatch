//! The scripted chain behind the cross-nest adoption run (docs/verification.md, GH #569), the
//! authoring eval board and the crash-point tests.
//!
//! The "same headers, different logs" chain `tests/common/tape.rs` plays in-process, over HTTP.
//! Two modes, switched live through `/control/mode`, share every block hash and timestamp:
//!
//!   full    blocks 1..=8 each carry one ERC-20 Transfer on the contract.
//!   pruned  the same headers, so the chain still walks, and zero logs anywhere, ever. A provider
//!           that kept the chain but dropped its receipts.
//!
//! Control surface, plain POST/GET so a walkthrough can `curl` it:
//!
//!   POST /control/mode        {"mode": "full"|"pruned"}
//!   POST /control/tip         {"number": N}   eth_blockNumber and "latest"
//!   POST /control/finalized   {"number": N}   eth_getBlockByNumber("finalized")
//!   POST /control/reset_calls                 clear the eth_getLogs call log
//!   GET  /control/calls                       every eth_getLogs call since the last reset
//!   GET  /control/state                       mode, tip, finalized and contract

use std::sync::Mutex;

use hyper::{Method, StatusCode};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::{hex, hex_u64, int, Fault, Fixture};

const CHAIN_ID: u64 = 42161; // arbitrum-one, per src/chains.rs
const BASE_TS: u64 = 1_700_000_000;
/// Blocks 1..=HISTORY_TIP each carry one Transfer in "full" mode.
pub const HISTORY_TIP: u64 = 8;
const TRANSFER_TOPIC0: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

pub struct Scripted {
    contract: String,
    state: Mutex<State>,
}

struct State {
    mode: Value,
    tip: u64,
    finalized: u64,
    calls: Vec<Value>,
}

impl Scripted {
    pub fn new(contract: &str) -> Self {
        Self {
            contract: contract.to_lowercase(),
            state: Mutex::new(State {
                mode: json!("full"),
                tip: HISTORY_TIP,
                finalized: 0,
                calls: Vec::new(),
            }),
        }
    }

    /// Pinned at startup, for a runner that cannot reach `/control/*` (an `--internal` network).
    pub fn pinned(self, tip: Option<u64>, finalized: Option<u64>) -> Self {
        {
            let mut s = self.state.lock().unwrap();
            if let Some(t) = tip {
                s.tip = t;
            }
            if let Some(f) = finalized {
                s.finalized = f;
            }
        }
        self
    }

    pub fn contract(&self) -> &str {
        &self.contract
    }

    pub fn tip_and_finalized(&self) -> (u64, u64) {
        let s = self.state.lock().unwrap();
        (s.tip, s.finalized)
    }

    fn resolve_tag(&self, tag: &Value) -> Result<u64, Fault> {
        let (tip, finalized) = self.tip_and_finalized();
        match tag.as_str() {
            Some("latest" | "pending") => Ok(tip),
            Some("finalized") => Ok(finalized),
            Some("earliest") => Ok(0),
            _ => quantity(tag),
        }
    }

    fn transfer_log(&self, n: u64) -> Value {
        json!({
            "address": self.contract,
            "topics": [
                TRANSFER_TOPIC0,
                topic_address(&"1".repeat(40)),
                topic_address(&"2".repeat(40)),
            ],
            "data": format!("0x{:064x}", 100 * n),
            "blockNumber": hex(n),
            "blockHash": block_hash(n),
            "transactionHash": format!("0x{}", sha256_hex(&format!("tx:{n}"))),
            "logIndex": "0x0",
        })
    }

    fn get_logs(&self, filter: &Value) -> Result<Value, Fault> {
        let field = |k: &str| filter.get(k).cloned().unwrap_or_else(|| json!("0x0"));
        let from = quantity(&field("fromBlock"))?;
        let to = quantity(&field("toBlock"))?;
        let pruned = {
            let mut s = self.state.lock().unwrap();
            s.calls.push(json!({"from": from, "to": to}));
            s.mode == "pruned"
        };
        if pruned {
            return Ok(json!([]));
        }
        // An address list that does not name the contract matches nothing; an empty one, everything.
        let wanted = match filter.get("address") {
            None | Some(Value::Null) => true,
            Some(Value::Array(a)) if a.is_empty() => true,
            Some(Value::Array(a)) => a
                .iter()
                .any(|x| x.as_str().map(str::to_lowercase).as_deref() == Some(&self.contract)),
            Some(_) => false,
        };
        let mut out = Vec::new();
        if wanted {
            for n in from.max(1)..=to.min(HISTORY_TIP) {
                out.push(self.transfer_log(n));
            }
        }
        Ok(Value::Array(out))
    }
}

impl Fixture for Scripted {
    fn call(&self, req: &Value) -> Result<Value, Fault> {
        let Some(method) = req.get("method") else {
            return Err(err("no method".into()));
        };
        let params = req.get("params").cloned().unwrap_or_else(|| json!([]));
        let first = || params.get(0).ok_or_else(|| err("no params".into()));
        match method.as_str() {
            Some("eth_chainId") => Ok(json!(hex(CHAIN_ID))),
            Some("eth_blockNumber") => Ok(json!(hex(self.tip_and_finalized().0))),
            Some("eth_getBlockByNumber") => Ok(block_header(self.resolve_tag(first()?)?)),
            Some("eth_getLogs") => self.get_logs(first()?),
            _ => Err(err(format!(
                "unsupported method {}",
                method
                    .as_str()
                    .map_or_else(|| method.to_string(), str::to_string)
            ))),
        }
    }

    fn control(&self, method: &Method, path: &str, body: &[u8]) -> Option<(StatusCode, Value)> {
        if *method == Method::GET {
            let s = self.state.lock().unwrap();
            return Some(match path {
                "/control/calls" => (StatusCode::OK, json!({"calls": s.calls})),
                "/control/state" => (
                    StatusCode::OK,
                    json!({"mode": s.mode, "tip": s.tip, "finalized": s.finalized, "contract": self.contract}),
                ),
                _ => not_found(path),
            });
        }
        if *method != Method::POST || !path.starts_with("/control/") {
            return None;
        }
        let body: Value = if body.is_empty() {
            json!({})
        } else {
            match serde_json::from_slice(body) {
                Ok(v) => v,
                Err(_) => return Some((StatusCode::BAD_REQUEST, json!({"error": "not JSON"}))),
            }
        };
        let number = || body.get("number").and_then(int);
        let bad = |what: &str| {
            (
                StatusCode::BAD_REQUEST,
                json!({"error": format!("{what} wanted")}),
            )
        };
        let mut s = self.state.lock().unwrap();
        Some(match path {
            "/control/mode" => match body.get("mode") {
                Some(m) => {
                    s.mode = m.clone();
                    (StatusCode::OK, json!({"mode": s.mode}))
                }
                None => bad("mode"),
            },
            "/control/tip" => match number() {
                Some(n) => {
                    s.tip = n;
                    (StatusCode::OK, json!({"tip": n}))
                }
                None => bad("number"),
            },
            "/control/finalized" => match number() {
                Some(n) => {
                    s.finalized = n;
                    (StatusCode::OK, json!({"finalized": n}))
                }
                None => bad("number"),
            },
            "/control/reset_calls" => {
                s.calls.clear();
                (StatusCode::OK, json!({"reset": true}))
            }
            _ => not_found(path),
        })
    }
}

fn not_found(path: &str) -> (StatusCode, Value) {
    (
        StatusCode::NOT_FOUND,
        json!({"error": format!("no such control endpoint {path}")}),
    )
}

fn err(message: String) -> Fault {
    Fault::Rpc(-32000, message)
}

fn quantity(v: &Value) -> Result<u64, Fault> {
    hex_u64(v).ok_or_else(|| err(format!("not a hex quantity: {v}")))
}

fn sha256_hex(s: &str) -> String {
    Sha256::digest(s.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// A pure function of height, the same in both modes, so they are one walkable chain.
fn block_hash(n: u64) -> String {
    format!("0x{}", sha256_hex(&format!("nuthatch-fixture-block:{n}")))
}

fn topic_address(hex40: &str) -> String {
    format!("0x{hex40:0>64}")
}

fn block_header(n: u64) -> Value {
    json!({
        "number": hex(n),
        "hash": block_hash(n),
        "parentHash": if n > 0 { block_hash(n - 1) } else { format!("0x{}", "00".repeat(32)) },
        "timestamp": hex(BASE_TS + n),
        "logsBloom": format!("0x{}", "00".repeat(256)),
    })
}
