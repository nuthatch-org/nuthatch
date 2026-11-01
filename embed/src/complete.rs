//! RFC-0081 S4: completeness for a narrowed nest under a verified head (§3.5). A block whose verified
//! header's `logsBloom` admits a filter, and for which the RPC returned no log of it, has its receipts
//! fetched and hashed to the header's `receiptsRoot`; its logs are then taken from them. A forged list
//! fails the root, and a bloom false positive costs one fetch and changes nothing.

use crate::narrow::Narrowed;
use crate::verified::{data, hex, rlp_bytes, rlp_len};
use anyhow::{anyhow, bail, Result};
use nuthatch_core::source::{Log, LogFilter};
use serde_json::Value;

/// A block's receipts, as `eth_getBlockReceipts` answers them, from the host's RPC.
#[async_trait::async_trait(?Send)]
pub trait Receipts {
    async fn block_receipts(&self, block: u64) -> Result<Value>;
}

/// What `complete: true` checks, and the most headers a follow may hash and test for it.
pub struct Completeness {
    pub filters: Vec<Narrowed>,
    pub max_headers: u64,
    pub receipts: std::sync::Arc<dyn Receipts + Send + Sync>,
}

/// Each of the three bloom bits `input` sets: its byte in the 256 and the bit within it.
fn bloom_bits(input: &[u8]) -> [(usize, u8); 3] {
    let h = alloy_primitives::keccak256(input);
    std::array::from_fn(|i| {
        let bit = (((h[2 * i] as usize) << 8) | h[2 * i + 1] as usize) & 2047;
        (255 - bit / 8, 1 << (bit % 8))
    })
}

fn bloom_has(bloom: &[u8], input: &[u8]) -> bool {
    bloom_bits(input).iter().all(|&(i, m)| bloom[i] & m != 0)
}

fn word(s: &str) -> Result<Vec<u8>> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    hex::decode(s).map_err(|e| anyhow!("`{s}` is not hex: {e}"))
}

/// Whether a block whose header carries `bloom` can hold a log `filter` asks for: every position the
/// filter constrains has one of its values in the bloom.
pub fn admits(bloom: &[u8], filter: &LogFilter) -> Result<bool> {
    let any = |values: &[String]| -> Result<bool> {
        if values.is_empty() {
            return Ok(true);
        }
        for v in values {
            if bloom_has(bloom, &word(v)?) {
                return Ok(true);
            }
        }
        Ok(false)
    };
    Ok(any(filter.addresses())?
        && any(filter.topic0s())?
        && filter
            .topics()
            .iter()
            .try_fold(true, |ok, t| Ok::<_, anyhow::Error>(ok && any(t)?))?)
}

fn rlp_list(parts: &[Vec<u8>]) -> Vec<u8> {
    let len = parts.iter().map(Vec::len).sum();
    let mut out = Vec::with_capacity(len + 9);
    rlp_len(&mut out, len, 0xc0);
    parts.iter().for_each(|p| out.extend_from_slice(p));
    out
}

fn rlp_of(b: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(b.len() + 9);
    rlp_bytes(&mut out, b);
    out
}

/// A quantity as RLP takes an integer: big-endian, no leading zeros.
fn int(v: &Value, name: &str) -> Result<Vec<u8>> {
    let mut b = data(v, name)?;
    let lead = b.iter().take_while(|&&x| x == 0).count();
    b.drain(..lead);
    Ok(b)
}

fn field<'a>(v: &'a Value, name: &str) -> Result<&'a str> {
    v.get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("a receipt's log has no {name}"))
}

/// One receipt as its trie leaf holds it, EIP-2718's type byte first where it has one. Its bloom is
/// computed from its logs, which is what a valid receipt's is.
fn encode_receipt(r: &Value) -> Result<Vec<u8>> {
    let kind = match r.get("type") {
        None | Some(Value::Null) => 0,
        Some(_) => *int(r, "type")?.last().unwrap_or(&0),
    };
    let outcome = match r.get("status") {
        Some(Value::String(_)) => int(r, "status")?,
        _ => data(r, "root")?,
    };
    let mut bloom = [0u8; 256];
    let mut logs = Vec::new();
    for l in r["logs"]
        .as_array()
        .ok_or_else(|| anyhow!("a receipt has no logs"))?
    {
        let address = word(field(l, "address")?)?;
        let topics = l["topics"]
            .as_array()
            .ok_or_else(|| anyhow!("a receipt's log has no topics"))?
            .iter()
            .map(|t| word(t.as_str().unwrap_or_default()))
            .collect::<Result<Vec<_>>>()?;
        for input in std::iter::once(&address).chain(&topics) {
            bloom_bits(input).iter().for_each(|&(i, m)| bloom[i] |= m);
        }
        let topics: Vec<Vec<u8>> = topics.iter().map(|t| rlp_of(t)).collect();
        logs.push(rlp_list(&[
            rlp_of(&address),
            rlp_list(&topics),
            rlp_of(&word(field(l, "data")?)?),
        ]));
    }
    let payload = rlp_list(&[
        rlp_of(&outcome),
        rlp_of(&int(r, "cumulativeGasUsed")?),
        rlp_of(&bloom),
        rlp_list(&logs),
    ]);
    Ok(match kind {
        0 => payload,
        k => std::iter::once(k).chain(payload).collect(),
    })
}

/// The root of the trie a block's receipts are kept in, each keyed by the RLP of its index.
pub fn receipts_root(receipts: &[Value]) -> Result<String> {
    let mut items = Vec::with_capacity(receipts.len());
    for (i, r) in receipts.iter().enumerate() {
        let index = (i as u64).to_be_bytes();
        let lead = index.iter().take_while(|&&x| x == 0).count();
        let key = rlp_of(&index[lead..]);
        let nibbles: Vec<u8> = key.iter().flat_map(|b| [b >> 4, b & 15]).collect();
        items.push((nibbles, encode_receipt(r)?));
    }
    items.sort();
    let root = match items.is_empty() {
        true => rlp_of(&[]),
        false => node(&items, 0),
    };
    Ok(hex(alloy_primitives::keccak256(&root).as_slice()))
}

/// The RLP of the trie node over `items`, keys sorted and distinct, at `depth` nibbles in.
fn node(items: &[(Vec<u8>, Vec<u8>)], depth: usize) -> Vec<u8> {
    if let [(key, value)] = items {
        return rlp_list(&[rlp_of(&compact(&key[depth..], true)), rlp_of(value)]);
    }
    let (first, last) = (&items[0].0, &items[items.len() - 1].0);
    let shared = first[depth..]
        .iter()
        .zip(&last[depth..])
        .take_while(|(a, b)| a == b)
        .count();
    if shared > 0 {
        let child = node(items, depth + shared);
        return rlp_list(&[
            rlp_of(&compact(&first[depth..depth + shared], false)),
            reference(&child),
        ]);
    }
    let mut slots: Vec<Vec<u8>> = (0..16u8)
        .map(|n| {
            let group: Vec<_> = items
                .iter()
                .filter(|(k, _)| k.get(depth) == Some(&n))
                .cloned()
                .collect();
            match group.is_empty() {
                true => rlp_of(&[]),
                false => reference(&node(&group, depth + 1)),
            }
        })
        .collect();
    slots.push(match items.iter().find(|(k, _)| k.len() == depth) {
        Some((_, v)) => rlp_of(v),
        None => rlp_of(&[]),
    });
    rlp_list(&slots)
}

/// A child as its parent holds it: inline under 32 bytes, else by its hash.
fn reference(child: &[u8]) -> Vec<u8> {
    match child.len() < 32 {
        true => child.to_vec(),
        false => rlp_of(alloy_primitives::keccak256(child).as_slice()),
    }
}

/// Nibbles in the hex-prefix form a leaf or extension stores its path in.
fn compact(nibbles: &[u8], leaf: bool) -> Vec<u8> {
    let flag = if leaf { 2 } else { 0 } + (nibbles.len() % 2) as u8;
    let mut out = Vec::with_capacity(nibbles.len() / 2 + 1);
    let rest = match nibbles.len() % 2 {
        1 => {
            out.push(flag << 4 | nibbles[0]);
            &nibbles[1..]
        }
        _ => {
            out.push(flag << 4);
            nibbles
        }
    };
    out.extend(rest.chunks(2).map(|p| p[0] << 4 | p[1]));
    out
}

/// Every log in `receipts` that one of `filters` asks for, as `eth_getLogs` would give it: its index
/// counted through the block, its block hash the verified header's.
pub fn logs_of(
    receipts: &[Value],
    block: u64,
    block_hash: &str,
    filters: &[&LogFilter],
) -> Result<Vec<Log>> {
    let mut out = Vec::new();
    let mut index = 0u64;
    for r in receipts {
        for l in r["logs"].as_array().into_iter().flatten() {
            let log = Log {
                address: hex(&word(field(l, "address")?)?),
                topics: l["topics"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|t| Ok(hex(&word(t.as_str().unwrap_or_default())?)))
                    .collect::<Result<_>>()?,
                data: hex(&word(field(l, "data")?)?),
                block_number: block,
                block_hash: block_hash.to_string(),
                tx_hash: String::new(),
                log_index: index,
            };
            index += 1;
            if filters.iter().any(|f| f.matches(&log)) {
                let tx = r
                    .get("transactionHash")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        anyhow!("block {block}'s receipt for a wanted log has no transactionHash")
                    })?;
                out.push(Log {
                    tx_hash: hex(&word(tx)?),
                    ..log
                });
            }
        }
    }
    Ok(out)
}

/// The receipts `eth_getBlockReceipts` answered, refused unless they hash to `root`.
pub fn checked<'a>(answer: &'a Value, block: u64, root: &str) -> Result<&'a [Value]> {
    let receipts = answer.as_array().ok_or_else(|| {
        anyhow!("eth_getBlockReceipts for block {block} answered {answer}, not a list")
    })?;
    let got = receipts_root(receipts)?;
    if !got.eq_ignore_ascii_case(root) {
        bail!(
            "block {block}'s receipts hash to {got}, not the receiptsRoot {root} its verified header gives"
        );
    }
    Ok(receipts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Mainnet block 20,000,000's receipts (types 0 to 3) as `eth_getBlockReceipts` gave them, less
    /// their blooms, with its header's hash, root and bloom.
    fn block() -> Value {
        serde_json::from_str(include_str!("fixtures/receipts-20000000.json")).unwrap()
    }

    const USDT: &str = "0xdac17f958d2ee523a2206206994597c13d831ec7";
    const TRANSFER: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

    fn filter(address: &str) -> LogFilter {
        LogFilter::new(&[address.to_string()], &[TRANSFER.to_string()]).unwrap()
    }

    #[test]
    fn mainnet_receipts_hash_to_their_headers_root() {
        let b = block();
        let root = receipts_root(b["receipts"].as_array().unwrap()).unwrap();
        assert_eq!(root, b["receiptsRoot"]);
        // Block 46,147's one receipt, from before Byzantium, carries a state root in place of a status.
        let early = json!([{"type": "0x0", "cumulativeGasUsed": "0x5208", "logs": [],
            "root": "0x96a8e009d2b88b1483e6941e6812e32263b05683fac202abc622a3e31aed1957"}]);
        assert_eq!(
            receipts_root(early.as_array().unwrap()).unwrap(),
            "0xfe2bf2a941abf41d72637e5b91750332a30283efd40c424dc522b77e6f0ed8c4"
        );
        assert_eq!(
            receipts_root(&[]).unwrap(),
            "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"
        );
    }

    #[test]
    fn a_receipts_list_missing_one_log_fails_the_root_by_name() {
        let mut b = block();
        let root = b["receiptsRoot"].as_str().unwrap().to_string();
        let r = b["receipts"].as_array_mut().unwrap();
        let i = r
            .iter()
            .position(|x| !x["logs"].as_array().unwrap().is_empty())
            .unwrap();
        r[i]["logs"].as_array_mut().unwrap().pop();
        let e = checked(&b["receipts"], 20_000_000, &root)
            .unwrap_err()
            .to_string();
        assert!(
            e.starts_with("block 20000000's receipts hash to 0x") && e.contains(&root),
            "{e}"
        );
    }

    #[test]
    fn the_bloom_admits_what_the_block_logged_and_its_logs_come_out_as_get_logs_gives_them() {
        let b = block();
        let bloom = data(&b, "logsBloom").unwrap();
        assert!(admits(&bloom, &filter(USDT)).unwrap());
        let absent = (1u64..)
            .map(|i| format!("0x{i:040x}"))
            .find(|a| !admits(&bloom, &filter(a)).unwrap())
            .unwrap();
        assert!(!admits(&bloom, &filter(&absent)).unwrap());
        let root = b["receiptsRoot"].as_str().unwrap();
        let receipts = checked(&b["receipts"], 20_000_000, root).unwrap();
        let hash = b["hash"].as_str().unwrap();
        let logs = logs_of(receipts, 20_000_000, hash, &[&filter(USDT)]).unwrap();
        assert!(!logs.is_empty());
        let (mut index, mut seen) = (0, 0);
        for r in receipts {
            for l in r["logs"].as_array().unwrap() {
                if l["address"] == USDT && l["topics"][0] == TRANSFER {
                    let got = &logs[seen];
                    assert_eq!(got.log_index, index);
                    assert_eq!(got.data, l["data"].as_str().unwrap());
                    assert_eq!(got.tx_hash, r["transactionHash"].as_str().unwrap());
                    assert_eq!(got.block_hash, hash);
                    seen += 1;
                }
                index += 1;
            }
        }
        assert_eq!(seen, logs.len());
    }
}
