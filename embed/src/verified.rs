//! RFC-0077 S5: sealing held to a finalized block the host verified, as RFC-0065 S5 holds the tab's.
//! The host gives `{number, hash}` (Helios in a browser, its own light client elsewhere); the module
//! fetches every header from the last seal up to it from the untrusted RPC, hashes each one, links
//! them by parent hash down from the verified block, and seals no row whose block hash is not on
//! that chain. Rows above it are still what the RPC said, and completeness is not checked (RFC-0065 §6).

use anyhow::{anyhow, bail, Result};
use nuthatch_core::extra::ExtraRows;
use nuthatch_core::source::Source;
use nuthatch_core::store::{HotStore, SEALED_THROUGH_KEY, START_BLOCK_KEY};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Mutex;

/// What the host says is final, from a source the RPC cannot forge; an error when it has none now.
#[async_trait::async_trait(?Send)]
pub trait VerifiedHead {
    async fn finalized(&self) -> Result<(u64, String)>;
}

#[derive(Default)]
struct State {
    /// What the host answered this follow, once checked: a refusal stands until the next follow.
    asked: Option<(u64, String)>,
    /// The verified block this follow seals under.
    top: Option<(u64, String)>,
    /// Each block's hash, hashed here and linked down from `top`, from the last seal up.
    chain: BTreeMap<u64, String>,
    /// Why nothing more seals, by name, until the next follow asks again.
    refused: Option<String>,
    /// The last time a row held the seal below its block, and why.
    held: Option<String>,
    headers: u64,
}

pub struct Verifier {
    state: Mutex<State>,
}

impl Default for Verifier {
    fn default() -> Self {
        Self::new()
    }
}

impl Verifier {
    pub fn new() -> Verifier {
        Verifier {
            state: Mutex::new(State::default()),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Why sealing stopped, if it did: the verification refused, or a row is off the verified chain.
    pub fn unverified(&self) -> Option<String> {
        let s = self.state();
        s.refused.clone().or_else(|| s.held.clone())
    }

    pub fn report(&self) -> Value {
        let s = self.state();
        serde_json::json!({
            "finalized": s.top.as_ref().map(|(n, h)| serde_json::json!({"number": n, "hash": h})),
            "chain_through": s.chain.keys().next_back(),
            "headers": s.headers,
            "refused": s.refused,
            "held": s.held,
        })
    }

    /// A follow begins: the verification is made again, once, against what the host says now.
    pub fn begin(&self) {
        self.state().asked = None;
    }

    /// Hash the headers from the last seal up to `top`, the host's answer for this follow. A failure
    /// is not an error: it stops sealing, by name, and the cursor carries on.
    pub async fn refresh(
        &self,
        top: &Result<(u64, String)>,
        source: &dyn Source,
        store: &dyn HotStore,
    ) {
        if let Err(e) = self.try_refresh(top, source, store).await {
            self.refuse(format!("{e:#}"));
        }
    }

    async fn try_refresh(
        &self,
        top: &Result<(u64, String)>,
        source: &dyn Source,
        store: &dyn HotStore,
    ) -> Result<()> {
        let floor = floor(store)?;
        let top = {
            let mut s = self.state();
            s.chain = s.chain.split_off(&(floor + 1));
            let asked = top.as_ref().ok().cloned();
            if s.asked.is_some() && s.asked == asked {
                return Ok(());
            }
            s.asked = asked;
            match top {
                Ok((n, h)) => (*n, h.to_ascii_lowercase()),
                Err(e) => bail!("the host gave no verified finalized block: {e:#}"),
            }
        };
        let (from, link) = {
            let mut s = self.state();
            s.top = Some(top.clone());
            s.refused = None;
            if top.0 <= floor
                || s.chain.contains_key(&(floor + 1)) && s.chain.get(&top.0) == Some(&top.1)
            {
                return Ok(());
            }
            match s.chain.last_key_value() {
                Some((&n, h)) if s.chain.contains_key(&(floor + 1)) && n < top.0 => {
                    (n + 1, Some((n, h.clone())))
                }
                _ => {
                    s.chain.clear();
                    (floor + 1, None)
                }
            }
        };
        let blocks: Vec<u64> = (from..=top.0).collect();
        let headers = source.block_headers(&blocks).await?;
        let list = blocks
            .iter()
            .map(|b| {
                headers
                    .get(b)
                    .cloned()
                    .ok_or_else(|| anyhow!("no header for block {b}"))
            })
            .collect::<Result<Vec<_>>>()?;
        let (chain, parent) = verify_chain(&list, &top.1)?;
        let below = match link {
            Some(l) => Some(l),
            None => store.get_block_hash(floor)?.map(|h| (floor, h)),
        };
        if let Some((n, h)) = below {
            if !h.eq_ignore_ascii_case(&parent) {
                bail!("block {}'s parent is {parent}, but block {n} is {h}", n + 1);
            }
        }
        let mut s = self.state();
        s.headers += list.len() as u64;
        s.chain.extend(chain);
        Ok(())
    }

    fn refuse(&self, why: String) {
        let mut s = self.state();
        s.chain.clear();
        s.top = None;
        s.refused = Some(why);
    }
}

/// The highest block already sealed, or the block before the nest's first.
fn floor(store: &dyn HotStore) -> Result<u64> {
    let meta =
        |k| -> Result<Option<u64>> { Ok(store.get_meta(k)?.and_then(|v: String| v.parse().ok())) };
    Ok(match meta(SEALED_THROUGH_KEY)? {
        Some(w) => w,
        None => meta(START_BLOCK_KEY)?.unwrap_or(0).saturating_sub(1),
    })
}

#[async_trait::async_trait]
impl ExtraRows for Verifier {
    fn seal_through(&self, store: &dyn HotStore, finalized_through: u64) -> Result<Option<u64>> {
        let mut s = self.state();
        let Some((top, _)) = s.top.clone() else {
            return Ok(None);
        };
        if s.refused.is_some() {
            return Ok(None);
        }
        let floor = floor(store)?;
        let mut ceiling = finalized_through.min(top);
        if ceiling <= floor {
            return Ok(Some(ceiling));
        }
        let mut off = None;
        store.scan_entities_in_range(floor + 1, ceiling, &mut |block, json| {
            let hash = serde_json::from_str::<Value>(json)
                .ok()
                .and_then(|v| v.get("block_hash")?.as_str().map(str::to_string));
            let on = hash.is_some_and(|h| {
                s.chain
                    .get(&block)
                    .is_some_and(|c| c.eq_ignore_ascii_case(&h))
            });
            if !on {
                off = Some(block);
            }
            on
        })?;
        s.held = off.map(|b| {
            ceiling = b - 1;
            format!("the RPC's logs for block {b} are not on the verified chain")
        });
        Ok(Some(ceiling))
    }
}

/// `headers` as `eth_getBlockByNumber` returns them, consecutive, the highest hashing to `top`. Answers
/// each block's hash by number, and the lowest block's parent hash. As RFC-0065 S5's `chain.rs` checks
/// them in the tab, with its messages.
pub fn verify_chain(headers: &[Value], top: &str) -> Result<(BTreeMap<u64, String>, String)> {
    let mut chain = BTreeMap::new();
    for v in headers {
        let hash = header_hash(v)?;
        let number = quantity(v, "number")?;
        let claimed = v.get("hash").and_then(Value::as_str);
        if let Some(claimed) = claimed {
            if !claimed.eq_ignore_ascii_case(&hash) {
                bail!("block {number} hashes to {hash}, not the {claimed} its RPC gave");
            }
        }
        let parent = data(v, "parentHash")?;
        if chain.insert(number, (hash, hex(&parent))).is_some() {
            bail!("block {number} is given twice");
        }
    }
    let Some((&last, (hash, _))) = chain.last_key_value() else {
        bail!("no headers to verify");
    };
    if !hash.eq_ignore_ascii_case(top) {
        bail!("block {last} is {hash}; the light client verified {top}");
    }
    let mut child: Option<(u64, String)> = None;
    for (&n, (hash, parent)) in chain.iter().rev() {
        if let Some((c, expect)) = &child {
            if n + 1 != *c {
                bail!("no header for block {}", n + 1);
            }
            if !hash.eq_ignore_ascii_case(expect) {
                bail!("block {c}'s parent is {expect}, but block {n} hashes to {hash}");
            }
        }
        child = Some((n, parent.clone()));
    }
    let lowest_parent = child.map(|(_, p)| p).unwrap_or_default();
    Ok((
        chain.into_iter().map(|(n, (h, _))| (n, h)).collect(),
        lowest_parent,
    ))
}

/// The keccak of a header's RLP, the block hash, from the JSON fields an RPC gives. The fields
/// after `nonce` are each present from their fork on, in order.
pub fn header_hash(v: &Value) -> Result<String> {
    const FIXED: [(&str, bool); 15] = [
        ("parentHash", false),
        ("sha3Uncles", false),
        ("miner", false),
        ("stateRoot", false),
        ("transactionsRoot", false),
        ("receiptsRoot", false),
        ("logsBloom", false),
        ("difficulty", true),
        ("number", true),
        ("gasLimit", true),
        ("gasUsed", true),
        ("timestamp", true),
        ("extraData", false),
        ("mixHash", false),
        ("nonce", false),
    ];
    const LATER: [(&str, bool); 6] = [
        ("baseFeePerGas", true),
        ("withdrawalsRoot", false),
        ("blobGasUsed", true),
        ("excessBlobGas", true),
        ("parentBeaconBlockRoot", false),
        ("requestsHash", false),
    ];
    let mut body = Vec::new();
    let mut field = |name: &str, int: bool| -> Result<()> {
        let mut b = data(v, name)?;
        if int {
            let lead = b.iter().take_while(|&&x| x == 0).count();
            b.drain(..lead);
        }
        rlp_bytes(&mut body, &b);
        Ok(())
    };
    for (name, int) in FIXED {
        field(name, int)?;
    }
    for (name, int) in LATER {
        if v.get(name).is_none_or(Value::is_null) {
            break;
        }
        field(name, int)?;
    }
    let mut out = Vec::with_capacity(body.len() + 9);
    rlp_len(&mut out, body.len(), 0xc0);
    out.extend_from_slice(&body);
    Ok(hex(alloy_primitives::keccak256(&out).as_slice()))
}

fn rlp_bytes(out: &mut Vec<u8>, b: &[u8]) {
    if b.len() == 1 && b[0] < 0x80 {
        out.push(b[0]);
    } else {
        rlp_len(out, b.len(), 0x80);
        out.extend_from_slice(b);
    }
}

fn rlp_len(out: &mut Vec<u8>, len: usize, base: u8) {
    if len <= 55 {
        out.push(base + len as u8);
    } else {
        let be = len.to_be_bytes();
        let lead = be.iter().take_while(|&&x| x == 0).count();
        out.push(base + 55 + (be.len() - lead) as u8);
        out.extend_from_slice(&be[lead..]);
    }
}

fn data(v: &Value, name: &str) -> Result<Vec<u8>> {
    let s = v
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("a header has no {name}"))?;
    let s = s.strip_prefix("0x").unwrap_or(s);
    let s = if s.len() % 2 == 1 {
        format!("0{s}")
    } else {
        s.to_string()
    };
    hex::decode(&s).map_err(|e| anyhow!("a header's {name} is not hex: {e}"))
}

fn quantity(v: &Value, name: &str) -> Result<u64> {
    let b = data(v, name)?;
    if b.len() > 8 {
        bail!("a header's {name} is past 64 bits");
    }
    Ok(b.iter().fold(0u64, |n, &x| (n << 8) | x as u64))
}

fn hex(b: &[u8]) -> String {
    format!("0x{}", hex::encode(b))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mainnet blocks 26,239,807 to 26,239,809, as `eth_getBlockByNumber` gave them.
    fn headers() -> Vec<Value> {
        serde_json::from_str(include_str!("fixtures/headers.json")).unwrap()
    }

    fn top(h: &[Value]) -> String {
        h[2]["hash"].as_str().unwrap().to_string()
    }

    #[test]
    fn three_mainnet_headers_hash_to_their_own_hashes_and_link() {
        let h = headers();
        for v in &h {
            assert_eq!(header_hash(v).unwrap(), v["hash"].as_str().unwrap());
        }
        let (chain, parent) = verify_chain(&h, &top(&h)).unwrap();
        assert_eq!(
            chain.keys().copied().collect::<Vec<_>>(),
            [26239807, 26239808, 26239809]
        );
        assert_eq!(parent, h[0]["parentHash"].as_str().unwrap());
    }

    #[test]
    fn a_top_the_light_client_did_not_verify_is_refused() {
        let h = headers();
        let e = verify_chain(&h, &format!("0x{}", "77".repeat(32))).unwrap_err();
        assert!(e.to_string().contains("the light client verified"), "{e}");
    }

    #[test]
    fn an_altered_header_is_refused_even_with_its_hash_rewritten() {
        let mut h = headers();
        h[0]["receiptsRoot"] = Value::from(format!("0x{}", "11".repeat(32)));
        let e = verify_chain(&h, &top(&h)).unwrap_err().to_string();
        assert!(e.contains("block 26239807 hashes to"), "{e}");
        h[0]["hash"] = Value::from(header_hash(&h[0]).unwrap());
        let e = verify_chain(&h, &top(&h)).unwrap_err().to_string();
        assert!(e.contains("block 26239808's parent is"), "{e}");
    }

    #[test]
    fn a_missing_header_is_refused() {
        let mut h = headers();
        let t = top(&h);
        h.remove(1);
        let e = verify_chain(&h, &t).unwrap_err().to_string();
        assert!(e.contains("no header for block 26239808"), "{e}");
    }
}
