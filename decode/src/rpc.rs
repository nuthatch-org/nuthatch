use anyhow::{anyhow, bail, Context, Result};
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
    /// `blockTimestamp` as the node stamped it on the log (execution-apis, 2025), when it did. Read
    /// only by a `block_timestamps = "lazy"` nest (#3065), whose rows carry exactly what their own
    /// logs carried, so a lazy row's timestamp is a function of its `eth_getLogs` answer and nothing
    /// else. Skipped when absent, so a tape recorded from a node that does not stamp is unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_timestamp: Option<u64>,
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
                    .map(|x| {
                        x.as_str()
                            .map(String::from)
                            .ok_or_else(|| anyhow!("log topic is not a string: {x}"))
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?
            .unwrap_or_default();
        let log_index = parse_hex_u64(&field_str(v, "logIndex")?)?;
        // `DecodedRow::seq` packs log_index into 20 bits; a wider one would collide with another row.
        if log_index >= 1 << 20 {
            bail!("log index {log_index} does not fit the 20-bit _seq field");
        }
        Ok(Log {
            address: field_str(v, "address")?,
            topics,
            data: field_str(v, "data").unwrap_or_default(),
            block_number: parse_hex_u64(&field_str(v, "blockNumber")?)?,
            block_hash: field_str(v, "blockHash").unwrap_or_default(),
            tx_hash: field_str(v, "transactionHash")?,
            log_index,
            // arb1.arbitrum.io sends `0x0` on every log: absent, not the epoch.
            block_timestamp: v
                .get("blockTimestamp")
                .and_then(Value::as_str)
                .and_then(|s| parse_hex_u64(s).ok())
                .filter(|&t| t != 0),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn rpc_log(topics: Value, log_index: &str) -> Value {
        serde_json::json!({
            "address": "0x11",
            "topics": topics,
            "data": "0x",
            "blockNumber": "0x1",
            "blockHash": "0xbh",
            "transactionHash": "0xtx",
            "logIndex": log_index,
        })
    }

    /// #3065: `blockTimestamp` is read when a node stamps it, and `0x0` (arb1's placeholder) is absent.
    #[test]
    fn a_stamped_log_keeps_its_timestamp_and_a_zero_one_is_absent() {
        let topics = serde_json::json!([]);
        assert_eq!(
            Log::from_rpc(&rpc_log(topics.clone(), "0x0"))
                .unwrap()
                .block_timestamp,
            None
        );
        let mut v = rpc_log(topics, "0x0");
        v["blockTimestamp"] = "0x6553f100".into();
        assert_eq!(
            Log::from_rpc(&v).unwrap().block_timestamp,
            Some(0x6553_f100)
        );
        v["blockTimestamp"] = "0x0".into();
        assert_eq!(Log::from_rpc(&v).unwrap().block_timestamp, None);
    }

    /// #2228: a malformed topic used to vanish, shifting every later topic into the wrong parameter.
    #[test]
    fn a_non_string_topic_is_refused_not_dropped() {
        let ok = Log::from_rpc(&rpc_log(serde_json::json!(["0xaa", "0xbb"]), "0x0")).unwrap();
        assert_eq!(ok.topics, vec!["0xaa", "0xbb"]);
        let err = Log::from_rpc(&rpc_log(serde_json::json!(["0xaa", null, "0xbb"]), "0x0"))
            .expect_err("a null topic must be refused");
        assert!(format!("{err:#}").contains("topic"), "{err:#}");
    }

    /// #2228: `_seq` keeps 20 bits of log_index, so a wider one would alias another row's key.
    #[test]
    fn a_log_index_wider_than_the_seq_field_is_refused() {
        let topics = serde_json::json!([]);
        assert_eq!(
            Log::from_rpc(&rpc_log(topics.clone(), "0xfffff"))
                .unwrap()
                .log_index,
            0xF_FFFF
        );
        let err = Log::from_rpc(&rpc_log(topics, "0x100000")).expect_err("must be refused");
        assert!(format!("{err:#}").contains("20-bit"), "{err:#}");
    }
}
