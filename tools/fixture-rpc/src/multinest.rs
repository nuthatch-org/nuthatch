//! The chain behind the dense multi-nest RAM budget and point-read latency.
//!
//! The footprint chain serves one contract, one event and a fixed tip. The ≤2 GB per-cursor budget
//! (CLAUDE.md non-negotiable 2, RFC-0021) is about the adversarial case, which differs three ways:
//!
//!   1. A large ABI: every event of the one the harness writes (the real Uniswap V4 `PoolManager`
//!      ABI), so decode carries one topic0 and one table per event per nest.
//!   2. A high event rate, from many contracts each emitting every block.
//!   3. At tip, not mid-backfill: once the backfill drains, `eth_blockNumber` advances `tip_step`
//!      blocks per call up to `final_tip`, so the cursor spends the rest of the run on the live path.
//!
//! The advance is deterministic in its endpoint, not its schedule: a faster machine gets there
//! sooner, but every run ends at `final_tip` having served the same logs.
//!
//! topic0 is derived from the ABI file the harness wrote, never hardcoded. A drifted topic0 does not
//! fail loudly; it decodes to an empty table, which is a footprint check that measured nothing.

use std::collections::HashMap;
use std::sync::Mutex;

use serde_json::{json, Value};
use tiny_keccak::{Hasher, Keccak};

use crate::{hex, hex_u64, quoted, truthy, Fault, Fixture};

pub struct Config {
    pub contracts: u64,
    pub logs_per_block: u64,
    pub initial_tip: u64,
    pub final_tip: u64,
    pub tip_step: u64,
}

pub struct Event {
    pub topic0: String,
    indexed: Vec<String>,
    data: Vec<String>,
}

pub struct Multinest {
    events: Vec<Event>,
    contracts: Vec<String>,
    by_addr: HashMap<String, u64>,
    cfg: Config,
    state: Mutex<Cursor>,
}

struct Cursor {
    tip: u64,
    served_to: u64,
}

pub fn keccak256(data: &[u8]) -> [u8; 32] {
    let mut k = Keccak::v256();
    k.update(data);
    let mut out = [0u8; 32];
    k.finalize(&mut out);
    out
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// `Name(type,type,...)` with tuples flattened, which is what topic0 hashes.
fn canonical(ev: &Value) -> Result<String, String> {
    fn ty(i: &Value) -> String {
        let t = i["type"].as_str().unwrap_or_default();
        match t.strip_prefix("tuple") {
            Some(suffix) => {
                let inner: Vec<String> = i["components"]
                    .as_array()
                    .map(|c| c.iter().map(ty).collect())
                    .unwrap_or_default();
                format!("({}){suffix}", inner.join(","))
            }
            None => t.to_string(),
        }
    }
    let name = ev["name"].as_str().ok_or("an event without a name")?;
    let inputs = ev["inputs"].as_array().ok_or("an event without inputs")?;
    let tys: Vec<String> = inputs.iter().map(ty).collect();
    Ok(format!("{name}({})", tys.join(",")))
}

/// The events an ABI declares, topic0 derived from each signature. Anonymous events have none.
pub fn load_events(abi: &Value) -> Result<Vec<Event>, String> {
    let mut out = Vec::new();
    for e in abi.as_array().ok_or("the ABI is not a JSON array")? {
        if e.get("type").and_then(Value::as_str) != Some("event")
            || e.get("anonymous").is_some_and(truthy)
        {
            continue;
        }
        let signature = canonical(e)?;
        let (mut indexed, mut data) = (Vec::new(), Vec::new());
        for i in e["inputs"].as_array().into_iter().flatten() {
            let t = i["type"].as_str().unwrap_or_default().to_string();
            // Every value is generated as one 32-byte word, so a type that is not is refused here
            // rather than on the first eth_getLogs.
            encode(&t, 0)?;
            if i.get("indexed").is_some_and(truthy) {
                indexed.push(t);
            } else {
                data.push(t);
            }
        }
        out.push(Event {
            topic0: format!("0x{}", to_hex(&keccak256(signature.as_bytes()))),
            indexed,
            data,
        });
    }
    if out.is_empty() {
        return Err("declares no non-anonymous events".into());
    }
    Ok(out)
}

/// A 256-bit two's-complement word, unprefixed.
fn word(v: i128) -> String {
    if v < 0 {
        format!("{}{:032x}", "f".repeat(32), v as u128)
    } else {
        format!("{v:064x}")
    }
}

fn bits(s: &str) -> Result<u32, String> {
    if s.is_empty() {
        return Ok(256);
    }
    s.parse().map_err(|_| format!("unsupported input type {s}"))
}

/// One static value as a word. Signed types go negative on odd seeds (a V4 `Swap` always has one
/// negative amount), so a decoder that gets the sign wrong fails here rather than in production.
fn encode(sol_type: &str, seed: u64) -> Result<String, String> {
    let unsupported = || format!("unsupported input type {sol_type}");
    let seed = u128::from(seed);
    if sol_type == "address" {
        return Ok(word((seed % 4096 + 1) as i128));
    }
    if sol_type == "bool" {
        return Ok(word((seed % 2) as i128));
    }
    if let Some(b) = sol_type.strip_prefix("int") {
        let b = bits(b).map_err(|_| unsupported())?;
        let mag = (seed % (1u128 << (b.saturating_sub(2)).min(48))) as i128;
        return Ok(word(if seed % 2 == 1 { -mag } else { mag }));
    }
    if let Some(b) = sol_type.strip_prefix("uint") {
        let b = bits(b).map_err(|_| unsupported())?.min(160);
        let v = seed * 1_000_003;
        return Ok(word(if b >= 128 { v } else { v % (1u128 << b) } as i128));
    }
    if sol_type.starts_with("bytes") && sol_type != "bytes" {
        return Ok(word((seed % (1u128 << 32)) as i128));
    }
    // `bytes` and `string` are dynamic; no ABI this backs has them.
    Err(unsupported())
}

impl Multinest {
    pub fn new(events: Vec<Event>, cfg: Config) -> Self {
        let contracts: Vec<String> = (0..cfg.contracts)
            .map(|i| format!("0x{:040x}", i + 1))
            .collect();
        let by_addr = contracts
            .iter()
            .enumerate()
            .map(|(i, a)| (a.clone(), i as u64))
            .collect();
        Self {
            events,
            contracts,
            by_addr,
            state: Mutex::new(Cursor {
                tip: cfg.initial_tip,
                served_to: 0,
            }),
            cfg,
        }
    }

    /// Holds at `initial_tip` until the backfill has drained, then advances `tip_step` per call.
    ///
    /// `dev --backfill N` indexes N blocks back from whatever tip it first observes, so a tip that
    /// moved from the first poll would start the backfill somewhere different every run. Holding
    /// until the cursor has asked for logs up to `initial_tip` pins it to the same blocks.
    fn block_number(&self) -> u64 {
        let mut c = self.state.lock().unwrap();
        if c.served_to >= self.cfg.initial_tip && c.tip < self.cfg.final_tip {
            c.tip = (c.tip + self.cfg.tip_step).min(self.cfg.final_tip);
        }
        c.tip
    }

    fn tip(&self) -> u64 {
        self.state.lock().unwrap().tip
    }

    fn logs(&self, from: u64, to: u64, addresses: &[String]) -> Vec<Value> {
        let want: Vec<&String> = if addresses.is_empty() {
            self.contracts.iter().collect()
        } else {
            addresses
                .iter()
                .filter(|a| self.by_addr.contains_key(*a))
                .collect()
        };
        let n = self.cfg.contracts;
        let lpb = self.cfg.logs_per_block;
        let mut out = Vec::new();
        for b in from.max(1)..=to.min(self.cfg.final_tip) {
            for a in &want {
                let ci = self.by_addr[*a];
                for i in 0..lpb {
                    // Cycle the event types so every table gets rows.
                    let ev = &self.events[((b * lpb + i) % self.events.len() as u64) as usize];
                    let seed = b * 131 + i * 17 + ci * 7;
                    let mut topics = vec![ev.topic0.clone()];
                    for (j, t) in ev.indexed.iter().enumerate() {
                        topics.push(format!("0x{}", enc(t, seed + j as u64 + 1)));
                    }
                    let data: String = ev
                        .data
                        .iter()
                        .enumerate()
                        .map(|(j, t)| enc(t, seed + 100 + j as u64))
                        .collect();
                    let idx = (b * lpb + i) * n + ci;
                    out.push(json!({
                        "address": a,
                        "topics": topics,
                        "data": format!("0x{data}"),
                        "blockNumber": hex(b),
                        "blockHash": format!("0x{b:064x}"),
                        "transactionHash": format!("0x{idx:064x}"),
                        "transactionIndex": hex(i),
                        // Per block, as a node numbers them: nuthatch refuses one wider than 20 bits (#2228).
                        "logIndex": hex(i * n + ci),
                        "removed": false,
                    }));
                }
            }
        }
        out
    }
}

fn enc(t: &str, seed: u64) -> String {
    encode(t, seed).expect("types are checked when the ABI loads")
}

impl Fixture for Multinest {
    fn call(&self, req: &Value) -> Result<Value, Fault> {
        let m = req.get("method");
        let p = req
            .get("params")
            .filter(|p| truthy(p))
            .cloned()
            .unwrap_or_else(|| json!([]));
        match m.and_then(Value::as_str) {
            Some("eth_chainId") => Ok(json!(hex(1))),
            Some("eth_blockNumber") => Ok(json!(hex(self.block_number()))),
            Some("eth_getBlockByNumber") => {
                let tag = p.get(0).ok_or(Fault::Malformed)?;
                let n = match tag.as_str() {
                    Some(t) if t.starts_with("0x") => hex_u64(tag).ok_or(Fault::Malformed)?,
                    _ => self.tip(),
                };
                Ok(json!({
                    "number": hex(n),
                    "hash": format!("0x{n:064x}"),
                    "parentHash": format!("0x{:064x}", n.saturating_sub(1)),
                    "timestamp": hex(1_600_000_000 + n * 12),
                }))
            }
            Some("eth_getLogs") => {
                let filter = p.get(0).filter(|f| f.is_object()).ok_or(Fault::Malformed)?;
                let bound = |k: &str, default: &str| {
                    let v = filter.get(k).cloned().unwrap_or_else(|| json!(default));
                    if v == "latest" {
                        Ok(self.tip())
                    } else {
                        hex_u64(&v).ok_or(Fault::Malformed)
                    }
                };
                let from = bound("fromBlock", "0x0")?;
                let to = bound("toBlock", "latest")?;
                let addresses: Vec<String> = match filter.get("address") {
                    Some(Value::String(a)) => vec![a.to_lowercase()],
                    Some(Value::Array(a)) => a
                        .iter()
                        .map(|x| x.as_str().map(str::to_lowercase).ok_or(Fault::Malformed))
                        .collect::<Result<_, _>>()?,
                    Some(v) if truthy(v) => return Err(Fault::Malformed),
                    _ => Vec::new(),
                };
                {
                    let mut c = self.state.lock().unwrap();
                    c.served_to = c.served_to.max(to);
                }
                Ok(Value::Array(self.logs(from, to, &addresses)))
            }
            _ => Err(Fault::Rpc(
                -32601,
                format!("multinest mock: unimplemented {}", quoted(m)),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keccak_is_ethereums() {
        assert_eq!(
            to_hex(&keccak256(b"Transfer(address,address,uint256)")),
            "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        );
    }

    #[test]
    fn negative_words_are_twos_complement() {
        assert_eq!(word(-1), "f".repeat(64));
        assert_eq!(word(-0), "0".repeat(64));
    }
}
