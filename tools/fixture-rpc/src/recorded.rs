//! A stretch of a real chain, replayed: every header's hash and timestamp in a range and one
//! contract's logs, as `tests/fixtures/tab-parity/record.sh` took them from mainnet, gzipped (RFC-0075 S3).
//!
//! A node and a tab are pointed at the same recording, so neither can see anything the other did not.
//! Headers carry no transactions; a block outside the range is an error, not an empty answer.

use std::collections::BTreeMap;

use serde_json::{json, Value};

use crate::{hex, hex_u64, Fault, Fixture};

/// Mainnet's finality depth, so a `finalized` tag agrees with the depth the node applies.
const FINALITY_DEPTH: u64 = 64;

pub struct Recorded {
    chain_id: u64,
    tip: u64,
    /// number -> (hash, timestamp)
    headers: BTreeMap<u64, (String, String)>,
    logs: Vec<Value>,
}

impl Recorded {
    /// From a recording on disk, gzipped when its name ends `.gz`.
    pub fn from_path(path: &std::path::Path) -> Result<Recorded, String> {
        use std::io::Read;
        let raw = std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?;
        let mut json = String::new();
        if path.extension().is_some_and(|e| e == "gz") {
            flate2::read::GzDecoder::new(&raw[..])
                .read_to_string(&mut json)
                .map_err(|e| format!("{}: {e}", path.display()))?;
        } else {
            json = String::from_utf8(raw).map_err(|e| e.to_string())?;
        }
        Self::from_json(&json)
    }

    /// From the recording's JSON:`{tip, headers: [[number, hash, timestamp]], logs: [log]}`.
    pub fn from_json(json: &str) -> Result<Recorded, String> {
        let v: Value = serde_json::from_str(json).map_err(|e| e.to_string())?;
        let tip = v["tip"].as_u64().ok_or("no tip")?;
        let mut headers = BTreeMap::new();
        for h in v["headers"].as_array().ok_or("no headers")? {
            let n = hex_u64(&h[0]).ok_or("header number")?;
            let hash = h[1].as_str().ok_or("header hash")?.to_string();
            let ts = h[2].as_str().ok_or("header timestamp")?.to_string();
            headers.insert(n, (hash, ts));
        }
        if !headers.contains_key(&tip) {
            return Err(format!("the recording stops short of its tip {tip}"));
        }
        let logs = v["logs"].as_array().ok_or("no logs")?.clone();
        Ok(Recorded {
            chain_id: 1,
            tip,
            headers,
            logs,
        })
    }

    pub fn tip(&self) -> u64 {
        self.tip
    }

    pub fn first(&self) -> u64 {
        *self.headers.keys().next().expect("a recording has headers")
    }

    fn header(&self, n: u64) -> Result<Value, Fault> {
        let (hash, ts) = self
            .headers
            .get(&n)
            .ok_or_else(|| err(format!("block {n} is outside the recording")))?;
        let parent = match n.checked_sub(1).and_then(|p| self.headers.get(&p)) {
            Some((h, _)) => h.clone(),
            None => format!("0x{:064x}", 0),
        };
        Ok(json!({
            "number": hex(n),
            "hash": hash,
            "parentHash": parent,
            "timestamp": ts,
            "transactions": [],
        }))
    }

    fn resolve(&self, tag: &Value) -> Result<u64, Fault> {
        match tag.as_str() {
            Some("latest") | Some("pending") => Ok(self.tip),
            Some("finalized") | Some("safe") => Ok(self.tip - FINALITY_DEPTH),
            Some("earliest") => Ok(self.first()),
            _ => hex_u64(tag).ok_or_else(|| err(format!("not a block: {tag}"))),
        }
    }

    fn get_logs(&self, filter: &Value) -> Result<Value, Fault> {
        let bound = |k: &str, default: u64| match filter.get(k) {
            None | Some(Value::Null) => Ok(default),
            Some(v) => self.resolve(v),
        };
        let from = bound("fromBlock", self.tip)?;
        let to = bound("toBlock", self.tip)?;
        if to > self.tip {
            return Err(err(format!("toBlock {to} is past the tip {}", self.tip)));
        }
        let addresses: Option<Vec<String>> = match filter.get("address") {
            None | Some(Value::Null) => None,
            Some(Value::String(s)) => Some(vec![s.to_lowercase()]),
            Some(Value::Array(a)) if a.is_empty() => None,
            Some(Value::Array(a)) => Some(
                a.iter()
                    .filter_map(|x| x.as_str().map(str::to_lowercase))
                    .collect(),
            ),
            Some(other) => return Err(err(format!("bad address filter {other}"))),
        };
        let topic0s: Option<Vec<String>> = match filter.get("topics").and_then(|t| t.get(0)) {
            None | Some(Value::Null) => None,
            Some(Value::String(s)) => Some(vec![s.to_lowercase()]),
            Some(Value::Array(a)) => Some(
                a.iter()
                    .filter_map(|x| x.as_str().map(str::to_lowercase))
                    .collect(),
            ),
            Some(other) => return Err(err(format!("bad topic filter {other}"))),
        };
        if filter
            .get("topics")
            .and_then(Value::as_array)
            .is_some_and(|t| t.len() > 1 && t[1..].iter().any(|x| !x.is_null()))
        {
            return Err(err("only topic0 filters are recorded".into()));
        }
        let out: Vec<Value> = self
            .logs
            .iter()
            .filter(|l| {
                let n = hex_u64(&l["blockNumber"]).unwrap_or(u64::MAX);
                let addr = l["address"].as_str().unwrap_or("").to_lowercase();
                let t0 = l["topics"][0].as_str().unwrap_or("").to_lowercase();
                (from..=to).contains(&n)
                    && addresses.as_ref().is_none_or(|a| a.contains(&addr))
                    && topic0s.as_ref().is_none_or(|t| t.contains(&t0))
            })
            .cloned()
            .collect();
        Ok(Value::Array(out))
    }
}

impl Fixture for Recorded {
    fn call(&self, req: &Value) -> Result<Value, Fault> {
        let params = req.get("params").cloned().unwrap_or_else(|| json!([]));
        let first = || params.get(0).ok_or_else(|| err("no params".into()));
        match req.get("method").and_then(Value::as_str) {
            Some("eth_chainId") => Ok(json!(hex(self.chain_id))),
            Some("eth_blockNumber") => Ok(json!(hex(self.tip))),
            Some("eth_getBlockByNumber") => self.header(self.resolve(first()?)?),
            Some("eth_getLogs") => self.get_logs(first()?),
            other => Err(err(format!("unsupported method {other:?}"))),
        }
    }
}

fn err(message: String) -> Fault {
    Fault::Rpc(-32000, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    const CHAIN: &str = r#"{"tip": 3, "headers": [["0x1","0xa1","0x10"],["0x2","0xa2","0x1c"],["0x3","0xa3","0x28"]],
        "logs": [{"address":"0xAB","blockNumber":"0x2","logIndex":"0x0","topics":["0xT0"]}]}"#;

    #[test]
    fn headers_chain_and_unrecorded_blocks_are_refused() {
        let r = Recorded::from_json(CHAIN).unwrap();
        let h = r.header(2).ok().unwrap();
        assert_eq!(h["parentHash"], "0xa1");
        assert_eq!(h["timestamp"], "0x1c");
        assert!(r.header(4).is_err());
    }

    #[test]
    fn logs_filter_by_range_address_and_topic0() {
        let r = Recorded::from_json(CHAIN).unwrap();
        let n = |f: Value| r.get_logs(&f).ok().unwrap().as_array().unwrap().len();
        assert_eq!(
            n(json!({"fromBlock": "0x1", "toBlock": "0x3", "address": ["0xab"]})),
            1
        );
        assert_eq!(
            n(json!({"fromBlock": "0x3", "toBlock": "0x3", "address": "0xab"})),
            0
        );
        assert_eq!(
            n(json!({"fromBlock": "0x1", "toBlock": "0x3", "topics": [["0xt0"]]})),
            1
        );
        assert_eq!(
            n(json!({"fromBlock": "0x1", "toBlock": "0x3", "address": "0xcd"})),
            0
        );
    }
}
