use anyhow::{anyhow, Context, Result};
use serde_json::Value;

/// A single log entry as returned by `eth_getLogs`.
///
/// `Serialize`/`Deserialize` are here for RFC-0039's recorded tape, which persists whatever a
/// `Source` returned so a benchmark can be replayed from disk. serde is already a dependency of this
/// crate, so this costs nothing new and does not widen the dbsp-free dependency graph #581 extracted
/// this crate to protect.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Log {
    /// Emitting contract. Unused while we filter by a single address in the query, but retained
    /// for multi-contract / ABI-priority decode in later slices.
    #[allow(dead_code)]
    pub address: String,
    pub topics: Vec<String>,
    pub data: String,
    pub block_number: u64,
    pub block_hash: String,
    pub tx_hash: String,
    pub log_index: u64,
}

impl Log {
    /// One entry of an `eth_getLogs` result. Here rather than in the node's RPC client so a tab
    /// (RFC-0065) reads logs exactly as the node does.
    pub fn from_rpc(v: &Value) -> Result<Log> {
        let topics = v
            .get("topics")
            .and_then(Value::as_array)
            .map(|t| {
                t.iter()
                    .filter_map(|x| x.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        Ok(Log {
            address: field_str(v, "address")?,
            topics,
            data: field_str(v, "data").unwrap_or_default(),
            block_number: parse_hex_u64(&field_str(v, "blockNumber")?)?,
            block_hash: field_str(v, "blockHash").unwrap_or_default(),
            tx_hash: field_str(v, "transactionHash")?,
            log_index: parse_hex_u64(&field_str(v, "logIndex")?)?,
        })
    }
}

fn field_str(v: &Value, key: &str) -> Result<String> {
    v.get(key)
        .and_then(Value::as_str)
        .map(String::from)
        .ok_or_else(|| anyhow!("log missing field '{key}'"))
}

pub fn parse_hex_u64(s: &str) -> Result<u64> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    u64::from_str_radix(s, 16).with_context(|| format!("bad hex number '{s}'"))
}
