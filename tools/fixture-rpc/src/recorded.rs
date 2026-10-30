//! A stretch of a real chain, replayed: every header's hash and timestamp in a range and one
//! contract's logs, as `tests/fixtures/tab-parity/record.sh` took them from mainnet, gzipped (RFC-0075 S3).
//!
//! A node and a tab are pointed at the same recording, so neither can see anything the other did not.
//! Headers carry no transactions; a block outside the range is an error, not an empty answer.
//!
//! A recording may instead hold several eras, as `tests/e2e_embed_shapes.rs` writes the binary's tape
//! chains out (RFC-0077 S5): `{chain_id, eras: [{tip, finalized, headers, logs}]}`, each served in
//! turn as `POST /control/fork` moves on.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::{json, Value};

use crate::{hex, hex_u64, Fault, Fixture};
use hyper::{Method, StatusCode};

/// Mainnet's finality depth, so a `finalized` tag agrees with the depth the node applies.
const FINALITY_DEPTH: u64 = 64;

pub struct Recorded {
    chain_id: u64,
    /// The chain as served before each `POST /control/fork`, the first being the recording.
    eras: Vec<Era>,
    era: AtomicUsize,
    /// Whole headers by number (`--headers`), served in place of the hash and timestamp alone where
    /// their hash is the era's.
    full: BTreeMap<u64, Value>,
}

struct Era {
    tip: u64,
    /// The `finalized` tag: the tip less mainnet's depth unless the era sets it.
    finalized: Option<u64>,
    /// number -> (hash, timestamp)
    headers: BTreeMap<u64, (String, String)>,
    logs: Vec<Value>,
}

impl Era {
    fn from_json(v: &Value) -> Result<Era, String> {
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
        Ok(Era {
            tip,
            finalized: v.get("finalized").and_then(Value::as_u64),
            headers,
            logs: v["logs"].as_array().ok_or("no logs")?.clone(),
        })
    }
}

/// A file's text, gunzipped when its name ends `.gz`.
fn read(path: &std::path::Path) -> Result<String, String> {
    use std::io::Read;
    let raw = std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?;
    if path.extension().is_some_and(|e| e == "gz") {
        let mut json = String::new();
        flate2::read::GzDecoder::new(&raw[..])
            .read_to_string(&mut json)
            .map_err(|e| format!("{}: {e}", path.display()))?;
        Ok(json)
    } else {
        String::from_utf8(raw).map_err(|e| e.to_string())
    }
}

impl Recorded {
    /// From a recording on disk, gzipped when its name ends `.gz`.
    pub fn from_path(path: &std::path::Path) -> Result<Recorded, String> {
        Self::from_json(&read(path)?)
    }

    /// From the recording's JSON: `{tip, headers: [[number, hash, timestamp]], logs: [log]}`, or
    /// `{chain_id, eras: [{tip, finalized, headers, logs}]}`.
    pub fn from_json(json: &str) -> Result<Recorded, String> {
        let v: Value = serde_json::from_str(json).map_err(|e| e.to_string())?;
        let eras = match v.get("eras").and_then(Value::as_array) {
            Some(eras) => eras.iter().map(Era::from_json).collect::<Result<_, _>>()?,
            None => vec![Era::from_json(&v)?],
        };
        if eras.is_empty() {
            return Err("no eras".into());
        }
        Ok(Recorded {
            chain_id: v.get("chain_id").and_then(Value::as_u64).unwrap_or(1),
            eras,
            era: AtomicUsize::new(0),
            full: BTreeMap::new(),
        })
    }

    /// Serve these whole headers (`eth_getBlockByNumber`'s, without transactions) where the era has
    /// the same hash at that height, so a client can hash them (RFC-0077 S5).
    pub fn with_headers(mut self, path: &std::path::Path) -> Result<Recorded, String> {
        let v: Value = serde_json::from_str(&read(path)?).map_err(|e| e.to_string())?;
        for h in v.as_array().ok_or("headers: not an array")? {
            let n = hex_u64(&h["number"]).ok_or("a whole header has no number")?;
            self.full.insert(n, h.clone());
        }
        Ok(self)
    }

    /// The tip served now.
    pub fn tip(&self) -> u64 {
        self.now().tip
    }

    /// Each era's tip, in the order `POST /control/fork` moves through them.
    pub fn tips(&self) -> Vec<u64> {
        self.eras.iter().map(|e| e.tip).collect()
    }

    fn now(&self) -> &Era {
        &self.eras[self.era.load(Ordering::SeqCst)]
    }

    pub fn first(&self) -> u64 {
        *self.eras[0]
            .headers
            .keys()
            .next()
            .expect("a recording has headers")
    }

    fn header(&self, n: u64) -> Result<Value, Fault> {
        let era = self.now();
        let (hash, ts) = era
            .headers
            .get(&n)
            .ok_or_else(|| err(format!("block {n} is outside the recording")))?;
        if let Some(full) = self.full.get(&n).filter(|h| h["hash"] == json!(hash)) {
            let mut h = full.clone();
            h["transactions"] = json!([]);
            return Ok(h);
        }
        let parent = match n.checked_sub(1).and_then(|p| era.headers.get(&p)) {
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
        let tip = self.now().tip;
        match tag.as_str() {
            Some("latest") | Some("pending") => Ok(tip),
            Some("finalized") | Some("safe") => Ok(self
                .now()
                .finalized
                .unwrap_or(tip.saturating_sub(FINALITY_DEPTH))),
            Some("earliest") => Ok(self.first()),
            _ => hex_u64(tag).ok_or_else(|| err(format!("not a block: {tag}"))),
        }
    }

    fn get_logs(&self, filter: &Value) -> Result<Value, Fault> {
        let era = self.now();
        let bound = |k: &str, default: u64| match filter.get(k) {
            None | Some(Value::Null) => Ok(default),
            Some(v) => self.resolve(v),
        };
        let from = bound("fromBlock", era.tip)?;
        let to = bound("toBlock", era.tip)?;
        if to > era.tip {
            return Err(err(format!("toBlock {to} is past the tip {}", era.tip)));
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
        let out: Vec<Value> = era
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

    /// RFC-0075 S4's scripted reorg. Before the fork the recording carries synthetic Transfers in its
    /// last ten blocks; `POST /control/fork` then replaces its last six blocks with others carrying
    /// different Transfers, and adds two, so a follower must take back what the old ones gave.
    pub fn with_fork(mut self) -> Recorded {
        let base = self.eras.remove(0);
        let t = base.tip;
        let mut before = Era {
            tip: t,
            finalized: None,
            headers: base.headers.clone(),
            logs: base.logs.clone(),
        };
        before.logs.extend(tail(&before.headers, t - 9..=t, 0));
        let mut after = Era {
            tip: t + 2,
            finalized: None,
            headers: base.headers,
            logs: base.logs,
        };
        let last_ts = hex_u64(&json!(after.headers[&t].1)).unwrap_or(0);
        for n in t - 5..=t + 2 {
            let ts = after
                .headers
                .get(&n)
                .map(|h| h.1.clone())
                .unwrap_or_else(|| hex(last_ts + 12 * (n - t)));
            let hash = format!(
                "0x{}",
                crate::multinest::keccak256(format!("rfc-0075-s4-fork/{n}").as_bytes())
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>()
            );
            after.headers.insert(n, (hash, ts));
        }
        let kept = tail(&after.headers, t - 9..=t - 6, 0);
        let replaced = tail(&after.headers, t - 5..=t + 2, 1);
        after.logs.extend(kept.into_iter().chain(replaced));
        self.eras = vec![before, after];
        self
    }
}

/// An existing holder of the recorded token, so a tail Transfer lands in a group the sealed rows
/// already hold as well as in new ones.
const HOLDER: &str = "0x78f3b1558952774e4520868cb8e34bf256d6209b";
const CONTRACT: &str = "0x0c26fe5e579c3b007ba16d921ba8204a47c05f1a";
const TRANSFER: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

/// Mints over `blocks`, one or two a block, whose recipients and amounts depend on `variant`.
fn tail(
    headers: &BTreeMap<u64, (String, String)>,
    blocks: std::ops::RangeInclusive<u64>,
    variant: u64,
) -> Vec<Value> {
    let word = |s: &str| format!("0x{:0>64}", s.trim_start_matches("0x"));
    let mut out = Vec::new();
    for n in blocks {
        let (hash, ts) = &headers[&n];
        let count = if (n + variant).is_multiple_of(3) {
            1
        } else {
            2
        };
        for i in 0..count {
            let to = if i == 0 && n.is_multiple_of(2) {
                HOLDER.to_string()
            } else {
                format!(
                    "0x{:040x}",
                    (0xaa00 + 0x1100 * variant) + n % (5 + 2 * variant)
                )
            };
            let value = ((n % 1000) + 1) as u128 * 1_000_000_000_000_000 * (variant as u128 + 1)
                + i as u128;
            let tx =
                crate::multinest::keccak256(format!("rfc-0075-s4-tx/{n}/{i}/{variant}").as_bytes());
            out.push(json!({
                "address": CONTRACT,
                "topics": [TRANSFER, word("0"), word(&to)],
                "data": format!("0x{value:064x}"),
                "blockNumber": hex(n),
                "transactionHash": format!("0x{}", tx.iter().map(|b| format!("{b:02x}")).collect::<String>()),
                "transactionIndex": hex(i),
                "blockHash": hash,
                "logIndex": hex(i),
                "blockTimestamp": ts,
                "removed": false,
            }));
        }
    }
    out
}

impl Fixture for Recorded {
    fn control(&self, method: &Method, path: &str, _: &[u8]) -> Option<(StatusCode, Value)> {
        if *method != Method::POST || path != "/control/fork" {
            return None;
        }
        let era = (self.era.load(Ordering::SeqCst) + 1).min(self.eras.len() - 1);
        self.era.store(era, Ordering::SeqCst);
        Some((
            StatusCode::OK,
            json!({"era": era, "tip": self.eras[era].tip}),
        ))
    }

    fn call(&self, req: &Value) -> Result<Value, Fault> {
        let params = req.get("params").cloned().unwrap_or_else(|| json!([]));
        let first = || params.get(0).ok_or_else(|| err("no params".into()));
        match req.get("method").and_then(Value::as_str) {
            Some("eth_chainId") => Ok(json!(hex(self.chain_id))),
            Some("eth_blockNumber") => Ok(json!(hex(self.now().tip))),
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

    #[test]
    fn eras_move_on_with_their_own_tip_finalized_and_chain_id() {
        let r = Recorded::from_json(
            r#"{"chain_id": 42161, "eras": [
              {"tip": 2, "finalized": 1, "headers": [["0x1","0xa1","0x10"],["0x2","0xa2","0x1c"]], "logs": []},
              {"tip": 3, "headers": [["0x1","0xa1","0x10"],["0x2","0xb2","0x1c"],["0x3","0xb3","0x28"]], "logs": []}]}"#,
        )
        .unwrap();
        let call = |m: &str, p: Value| r.call(&json!({"method": m, "params": p})).ok().unwrap();
        assert_eq!(call("eth_chainId", json!([])), "0xa4b1");
        assert_eq!(
            call("eth_getBlockByNumber", json!(["finalized", false]))["number"],
            "0x1"
        );
        r.control(&Method::POST, "/control/fork", b"").unwrap();
        assert_eq!(call("eth_blockNumber", json!([])), "0x3");
        assert_eq!(
            call("eth_getBlockByNumber", json!(["0x3", false]))["parentHash"],
            "0xb2"
        );
        assert_eq!(r.tips(), [2, 3]);
    }

    #[test]
    fn a_whole_header_is_served_only_where_its_hash_is_the_eras() {
        let dir = std::env::temp_dir().join(format!("fixture-rpc-headers-{}", std::process::id()));
        std::fs::write(
            &dir,
            r#"[{"number":"0x2","hash":"0xa2","receiptsRoot":"0x77"},{"number":"0x3","hash":"0xff","receiptsRoot":"0x78"}]"#,
        )
        .unwrap();
        let r = Recorded::from_json(CHAIN)
            .unwrap()
            .with_headers(&dir)
            .unwrap();
        std::fs::remove_file(&dir).unwrap();
        assert_eq!(r.header(2).ok().unwrap()["receiptsRoot"], "0x77");
        assert_eq!(r.header(2).ok().unwrap()["transactions"], json!([]));
        assert!(r.header(3).ok().unwrap().get("receiptsRoot").is_none());
    }
}
