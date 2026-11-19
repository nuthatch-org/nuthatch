//! `tokenName`, `tokenSymbol` and `tokenDecimal` on served `tokentx` rows, as Etherscan's carry them.
//!
//! Resolved when a response is built, never stored: the stored rows stay what the window derived
//! (deterministic, re-executable), and these fields are the token contract's answer at the latest
//! block, read through the nest's RPC for the response being served. Each token is read at most once
//! per response (a memo local to the response), and the answers are also kept in a bounded process
//! cache. A token that reverts gets an empty name and symbol and `0` decimals; any other RPC failure
//! fails the response with a clear error rather than serve a guess.

use crate::address_proxy::Forward;
use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

/// Most tokens the process remembers. Past it a token is read once per response and not kept.
pub const CACHE_CAP: usize = 4096;

/// A token's `(name, symbol, decimals)`.
type Meta = (String, String, String);

/// The process cache of token metadata.
#[derive(Default)]
pub struct TokenMeta {
    seen: Mutex<BTreeMap<String, Meta>>,
}

impl TokenMeta {
    /// Fill the three fields on every row of `rows` that names a `contractAddress`.
    pub async fn annotate(&self, forward: &Forward, rows: &mut [Value]) -> Result<()> {
        // The memo of this response: each distinct token is looked up once, whatever the cache holds.
        let mut memo: HashMap<String, Meta> = HashMap::new();
        for row in rows {
            let Some(c) = row.get("contractAddress").and_then(Value::as_str) else {
                continue;
            };
            let c = c.to_ascii_lowercase();
            let meta = match memo.get(&c) {
                Some(m) => m.clone(),
                None => {
                    let cached = self.seen.lock().expect("token meta lock").get(&c).cloned();
                    let m = match cached {
                        Some(m) => m,
                        None => {
                            let m = read(forward, &c).await?;
                            let mut seen = self.seen.lock().expect("token meta lock");
                            if seen.len() < CACHE_CAP {
                                seen.insert(c.clone(), m.clone());
                            }
                            m
                        }
                    };
                    memo.insert(c, m.clone());
                    m
                }
            };
            if let Some(o) = row.as_object_mut() {
                o.insert("tokenName".into(), Value::String(meta.0));
                o.insert("tokenSymbol".into(), Value::String(meta.1));
                o.insert("tokenDecimal".into(), Value::String(meta.2));
            }
        }
        Ok(())
    }
}

async fn read(forward: &Forward, c: &str) -> Result<Meta> {
    let name = string(call(forward, c, "0x06fdde03").await?);
    let symbol = string(call(forward, c, "0x95d89b41").await?);
    let decimals = match call(forward, c, "0x313ce567").await? {
        Some(h) => h
            .get(..64)
            .and_then(|w| crate::address_discovery::decimal(&format!("0x{w}")).ok())
            .unwrap_or_else(|| "0".into()),
        None => "0".into(),
    };
    Ok((name, symbol, decimals))
}

fn string(h: Option<String>) -> String {
    h.and_then(|h| decode_string(&h)).unwrap_or_default()
}

/// A contract's answer to a no-argument call at `latest`: `None` when it reverts or has no such
/// method. Any other failure (a transport error, a rate limit) is an error.
async fn call(forward: &Forward, c: &str, selector: &str) -> Result<Option<String>> {
    match forward(
        "eth_call".into(),
        json!([{"to": c, "data": selector}, "latest"]),
    )
    .await
    {
        Ok(v) => Ok(v
            .as_str()
            .map(|s| s.trim_start_matches("0x").to_string())
            .filter(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_hexdigit()))),
        Err(e) if is_contract_failure(&format!("{e:#}")) => Ok(None),
        Err(e) => Err(e).with_context(|| format!("reading token metadata of {c}")),
    }
}

/// Whether an `eth_call` error is the contract's own failure (a revert, out of gas, a bad opcode) and
/// not the transport's: transport and rate-limit errors are never contract-side, whatever else they say.
fn is_contract_failure(err: &str) -> bool {
    let e = err.to_ascii_lowercase();
    const TRANSPORT: [&str; 7] = [
        "429",
        "timeout",
        "timed out",
        "connection",
        "rate limit",
        "too many requests",
        "unavailable",
    ];
    const CONTRACT: [&str; 11] = [
        "revert",
        "invalid opcode",
        "out of gas",
        "execution error",
        "stack",
        "invalid jump",
        "bad instruction",
        "gas required exceeds",
        "-32015",
        "\"code\":3",
        "code: 3",
    ];
    !TRANSPORT.iter().any(|t| e.contains(t)) && CONTRACT.iter().any(|c| e.contains(c))
}

/// A `name()` or `symbol()` answer: an ABI-encoded string, or a bytes32 padded with zeros.
fn decode_string(hex: &str) -> Option<String> {
    let bytes: Vec<u8> = (0..hex.len() / 2 * 2)
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16))
        .collect::<Result<_, _>>()
        .ok()?;
    let usable = |b: &[u8]| {
        String::from_utf8(b.to_vec())
            .ok()
            .filter(|s| !s.contains('\0'))
    };
    if bytes.len() >= 64 {
        let offset =
            usize::try_from(alloy_primitives::U256::try_from_be_slice(&bytes[..32])?).ok()?;
        let at = bytes.get(offset..offset.checked_add(32)?)?;
        let len = usize::try_from(alloy_primitives::U256::try_from_be_slice(at)?).ok()?;
        let start = offset + 32;
        return usable(bytes.get(start..start.checked_add(len)?)?);
    }
    if bytes.len() == 32 {
        let end = bytes.iter().position(|&b| b == 0).unwrap_or(32);
        return usable(&bytes[..end]);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    fn abi(s: &str) -> String {
        format!("0x{:0>64}{:0>64x}{:0<64}", "20", s.len(), hex::encode(s))
    }

    fn rows(tokens: &[&str]) -> Vec<Value> {
        tokens
            .iter()
            .map(|t| json!({"contractAddress": t, "value": "1"}))
            .collect()
    }

    fn fwd(calls: Arc<AtomicU64>, fail: &'static str) -> Forward {
        Arc::new(move |m: String, p: Value| {
            let calls = calls.clone();
            Box::pin(async move {
                assert_eq!(m, "eth_call");
                assert_eq!(p[1], "latest");
                calls.fetch_add(1, Ordering::SeqCst);
                let to = p[0]["to"].as_str().unwrap().to_string();
                if !fail.is_empty() {
                    anyhow::bail!("{fail}");
                }
                if to.ends_with("a2") {
                    anyhow::bail!("-32000: execution reverted");
                }
                Ok(json!(match p[0]["data"].as_str().unwrap() {
                    "0x06fdde03" => abi("USD Coin"),
                    "0x95d89b41" => abi("USDC"),
                    "0x313ce567" => format!("0x{:0>64}", "6"),
                    d => anyhow::bail!("unexpected {d}"),
                }))
            })
        })
    }

    #[tokio::test]
    async fn rows_carry_metadata_read_once_per_response_and_cached() {
        let (a1, a2) = (format!("0x{:0>40}", "a1"), format!("0x{:0>40}", "a2"));
        let calls = Arc::new(AtomicU64::new(0));
        let f = fwd(calls.clone(), "");
        let meta = TokenMeta::default();
        let mut r = rows(&[&a1, &a1, &a2, &a1]);
        meta.annotate(&f, &mut r).await.unwrap();
        let get = |v: &Value| ["tokenName", "tokenSymbol", "tokenDecimal"].map(|k| v[k].clone());
        assert_eq!(get(&r[0]), ["USD Coin", "USDC", "6"]);
        assert_eq!(get(&r[3]), get(&r[0]));
        assert_eq!(get(&r[2]), ["", "", "0"], "a token that reverts");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            6,
            "3 reads for each of 2 tokens"
        );
        // A second response is served from the process cache.
        let mut r = rows(&[&a1, &a2]);
        meta.annotate(&f, &mut r).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 6);
        assert_eq!(get(&r[0])[1], "USDC");
    }

    #[tokio::test]
    async fn past_the_cache_cap_a_token_is_still_read_once_per_response() {
        let calls = Arc::new(AtomicU64::new(0));
        let f = fwd(calls.clone(), "");
        let meta = TokenMeta::default();
        {
            let mut seen = meta.seen.lock().unwrap();
            for i in 0..CACHE_CAP {
                seen.insert(format!("filler{i}"), Default::default());
            }
        }
        let t = format!("0x{:0>40}", "b1");
        let mut r = rows(&vec![t.as_str(); 500]);
        meta.annotate(&f, &mut r).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(meta.seen.lock().unwrap().len(), CACHE_CAP, "bounded");
    }

    #[tokio::test]
    async fn error_classes() {
        for (msg, ok) in [
            ("-32000: out of gas", true),
            ("VM execution error: stack underflow", true),
            ("429 Too Many Requests: rate limit exceeded", false),
            ("error sending request: connection reset", false),
            ("out of gas, but the request timed out", false),
        ] {
            let f = fwd(Arc::default(), msg);
            let mut r = rows(&[&format!("0x{:0>40}", "a3")]);
            let got = TokenMeta::default().annotate(&f, &mut r).await;
            assert_eq!(got.is_ok(), ok, "{msg}");
            if ok {
                assert_eq!(r[0]["tokenName"], "");
                assert_eq!(r[0]["tokenDecimal"], "0");
            } else {
                assert!(r[0].get("tokenName").is_none(), "no guess on {msg}");
            }
        }
    }

    #[test]
    fn decodes_abi_strings_and_bytes32() {
        assert_eq!(decode_string(&abi("Dai")[2..]).as_deref(), Some("Dai"));
        let mkr = format!("{:0<64}", hex::encode("MKR"));
        assert_eq!(decode_string(&mkr).as_deref(), Some("MKR"));
        assert_eq!(decode_string("zz"), None);
    }
}
