//! `getblocknobytime` searched on the main RPC, ported from `address_mode.rs` (`search_by_time` and
//! `descends`), which reaches the RPC through the HTTP proxy's forwarder and so cannot be included as
//! it stands. Same search, same proof: the answer is checked against its neighbour by parent hash, and
//! above the finalized head linked by parent hashes to the head the search began from.

use anyhow::{bail, Result};
use serde_json::{json, Value};

use crate::address_discovery::{hex_u64, Rpc};
use crate::address_history::{self, AddressHistory};

const MAX_LINK: u64 = 512;

struct Header {
    number: u64,
    ts: u64,
    hash: String,
    parent: String,
}

async fn header(rpc: &impl Rpc, at: Value) -> Result<Header> {
    let b = rpc.call("eth_getBlockByNumber", json!([at, false])).await?;
    let field = |k: &str| {
        b.get(k)
            .and_then(Value::as_str)
            .map(str::to_ascii_lowercase)
            .ok_or_else(|| anyhow::anyhow!("block {at} has no {k}"))
    };
    Ok(Header {
        number: hex_u64(&b["number"])?,
        ts: hex_u64(&b["timestamp"])?,
        hash: field("hash")?,
        parent: field("parentHash")?,
    })
}

async fn descends(rpc: &impl Rpc, b: u64, latest: Header, proof: &[(u64, String)]) -> Result<bool> {
    if latest.number < b {
        return Ok(false);
    }
    if latest.number - b > MAX_LINK {
        bail!(
            "block {b} is {} blocks under the head, too far to link",
            latest.number - b
        );
    }
    if proof
        .iter()
        .any(|(n, h)| *n == latest.number && *h != latest.hash)
    {
        return Ok(false);
    }
    let mut parent = latest.parent;
    for n in (b..latest.number).rev() {
        let h = header(rpc, json!(format!("0x{n:x}"))).await?;
        if h.hash != parent || proof.iter().any(|(m, ph)| *m == n && *ph != h.hash) {
            return Ok(false);
        }
        parent = h.parent;
    }
    Ok(true)
}

type Found = (u64, Header, Vec<(u64, String)>);

async fn search_by_time(
    rpc: &impl Rpc,
    t: u64,
    after: bool,
) -> Result<std::result::Result<Found, String>> {
    let at = |n: u64| json!(format!("0x{n:x}"));
    let latest = header(rpc, json!("latest")).await?;
    if t > latest.ts {
        return Ok(Err(format!(
            "timestamp {t} is newer than the chain head {} ({}); a block at or before it may still come",
            latest.number, latest.ts
        )));
    }
    let recent = latest.number.saturating_sub(8);
    let lower = header(rpc, at(recent)).await?;
    let lower_ts = lower.ts;
    let (mut lo, mut hi, mut lo_ts, mut hi_ts) = if t >= lower_ts {
        (recent, latest.number, lower_ts, latest.ts)
    } else {
        let genesis = if recent == 0 {
            lower
        } else {
            header(rpc, at(0)).await?
        };
        if t < genesis.ts {
            return Ok(if after {
                Ok((0, latest, vec![(0, genesis.hash)]))
            } else {
                Err(format!("timestamp {t} is before the first block"))
            });
        }
        (0, recent, genesis.ts, lower_ts)
    };
    let mut bisect = false;
    while lo < hi {
        let width = hi - lo;
        let guess = if bisect || hi_ts <= lo_ts {
            lo + width / 2
        } else {
            let span = u128::from(hi_ts - lo_ts);
            let into = u128::from(t.saturating_sub(lo_ts).min(hi_ts - lo_ts));
            lo + u64::try_from(into * u128::from(width) / span).unwrap_or(width)
        };
        if after {
            let probe = guess.clamp(lo, hi - 1);
            let ts = header(rpc, at(probe)).await?.ts;
            if ts >= t {
                hi = probe;
                hi_ts = ts;
            } else {
                lo = probe + 1;
                lo_ts = ts;
            }
        } else {
            let probe = guess.clamp(lo + 1, hi);
            let ts = header(rpc, at(probe)).await?.ts;
            if ts <= t {
                lo = probe;
                lo_ts = ts;
            } else {
                hi = probe - 1;
                hi_ts = ts;
            }
        }
        bisect = hi - lo > width / 2;
    }
    let b = header(rpc, at(lo)).await?;
    let mut proof = vec![(lo, b.hash.clone())];
    let proven = if after {
        b.ts >= t
            && (lo == 0 || {
                let prev = header(rpc, at(lo - 1)).await?;
                proof.push((lo - 1, prev.hash.clone()));
                prev.ts < t && b.parent == prev.hash
            })
    } else {
        b.ts <= t
            && if lo == latest.number {
                b.ts == t
            } else {
                let next = header(rpc, at(lo + 1)).await?;
                proof.push((lo + 1, next.hash.clone()));
                next.ts > t && next.parent == b.hash
            }
    };
    if !proven {
        bail!("the chain moved during the search; ask again");
    }
    Ok(Ok((lo, latest, proof)))
}

/// The answer `respond` gave from headers, or, where it had none to bracket `t`, one searched on `rpc`.
pub async fn block_by_time(
    rpc: &impl Rpc,
    history: &AddressHistory,
    first: Value,
    t: u64,
    after: bool,
) -> Value {
    let unbracketed = first["result"]
        .as_str()
        .is_some_and(|r| r.starts_with("NUTHATCH_INCOMPLETE:"));
    if !unbracketed || !address_history::strictly_increasing_timestamps(history.chain_id()) {
        return first;
    }
    let committed = history.head().ok().flatten();
    let searched = async {
        match search_by_time(rpc, t, after).await? {
            Ok((b, latest, proof)) => {
                if committed.is_none_or(|c| b + 1 > c) && !descends(rpc, b, latest, &proof).await? {
                    return Ok(Err(format!(
                        "block {b} does not link to the chain head the search began from; ask again"
                    )));
                }
                Ok(Ok(b))
            }
            Err(why) => Ok::<_, anyhow::Error>(Err(why)),
        }
    }
    .await;
    match searched {
        Ok(Ok(b)) => address_history::ok(
            Value::String(b.to_string()),
            history.generation().unwrap_or(0),
        ),
        Ok(Err(why)) => address_history::incomplete(&why),
        Err(e) => address_history::incomplete(&format!("searching the RPC: {e:#}")),
    }
}
