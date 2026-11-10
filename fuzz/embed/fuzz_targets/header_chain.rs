#![no_main]

//! The engine's verified follow against a hostile RPC (#2743): `verify_chain` and `header_hash`, the
//! RLP of each header an `eth_getBlockByNumber` answer carries, linked by parent hash down from a head
//! the host verified.
//!
//! A chain is built whose parent hashes link, by hashing each header in turn, so the linking checks
//! are reached and not only the first field's hex; then one header may be corrupted. Raw JSON is
//! fuzzed alongside. An `Err` is always fine; a panic or an overflow is a finding.

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use nuthatch_embed::verified::{header_hash, verify_chain};
use serde_json::{json, Value};

const FIELDS: [&str; 21] = [
    "parentHash",
    "sha3Uncles",
    "miner",
    "stateRoot",
    "transactionsRoot",
    "receiptsRoot",
    "logsBloom",
    "difficulty",
    "number",
    "gasLimit",
    "gasUsed",
    "timestamp",
    "extraData",
    "mixHash",
    "nonce",
    "baseFeePerGas",
    "withdrawalsRoot",
    "blobGasUsed",
    "excessBlobGas",
    "parentBeaconBlockRoot",
    "requestsHash",
];

#[derive(Arbitrary, Debug)]
struct Header {
    /// Values for the fields in order, as bytes; the header stops where they run out.
    values: Vec<Vec<u8>>,
    /// Added to the previous block's number.
    step: u8,
    claim_hash: bool,
}

#[derive(Arbitrary, Debug)]
enum Corrupt {
    None,
    Remove(u8, u8),
    Text(u8, u8, String),
    Number(u8, u64),
    Null(u8, u8),
}

#[derive(Arbitrary, Debug)]
struct Input {
    first: u64,
    headers: Vec<Header>,
    corrupt: Corrupt,
    top: Option<String>,
    raw: Vec<u8>,
}

fn hex(b: &[u8]) -> String {
    format!("0x{}", hex::encode(b))
}

fuzz_target!(|input: Input| {
    let mut chain: Vec<Value> = Vec::new();
    let mut number = input.first;
    for (i, h) in input.headers.iter().take(64).enumerate() {
        if i > 0 {
            number = number.wrapping_add(u64::from(h.step));
        }
        let mut v = json!({});
        for (name, value) in FIELDS.iter().zip(&h.values) {
            v[*name] = Value::String(hex(value));
        }
        v["number"] = Value::String(format!("0x{number:x}"));
        if let Some(parent) = chain.last().and_then(|p| header_hash(p).ok()) {
            v["parentHash"] = Value::String(parent);
        }
        if h.claim_hash {
            if let Ok(hash) = header_hash(&v) {
                v["hash"] = Value::String(hash);
            }
        }
        chain.push(v);
    }
    if !chain.is_empty() {
        let n = chain.len();
        let field = |f: u8| FIELDS[f as usize % FIELDS.len()];
        match &input.corrupt {
            Corrupt::None => {}
            Corrupt::Remove(i, f) => {
                chain[*i as usize % n]
                    .as_object_mut()
                    .unwrap()
                    .remove(field(*f));
            }
            Corrupt::Text(i, f, s) => chain[*i as usize % n][field(*f)] = Value::String(s.clone()),
            Corrupt::Number(i, x) => chain[*i as usize % n]["number"] = json!(format!("0x{x:x}")),
            Corrupt::Null(i, f) => chain[*i as usize % n][field(*f)] = Value::Null,
        }
    }
    let top = match &input.top {
        Some(t) => t.clone(),
        None => chain
            .last()
            .and_then(|h| header_hash(h).ok())
            .unwrap_or_default(),
    };
    let _ = verify_chain(&chain, &top);

    if let Ok(Value::Array(raw)) = serde_json::from_slice::<Value>(&input.raw) {
        for h in &raw {
            let _ = header_hash(h);
        }
        let _ = verify_chain(&raw, &top);
    }
});
