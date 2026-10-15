//! Discovery by the balance residual, for chains where `trace_filter` replays every block it covers
//! (reth caps it at 100 blocks; Base took 47 s per 100, measured 2026-10-14).
//!
//! An externally owned account's balance falls only through transactions it sends, so over any
//! range `balance(end) - balance(before) + own spending - own known income` is exactly the value
//! others sent it, and zero proves nothing came. Outgoing transactions are the blocks where the
//! nonce rises; a positive residual is bisected to single blocks, and only those are traced. Each
//! traced block's credits must equal its residual to the wei, or the window is refused: a wrong fee
//! rule or a lying provider is never served.

use anyhow::{anyhow, bail, Context, Result};
use futures::{StreamExt, TryStreamExt};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

use crate::address_discovery::{hex_u64, word32, Discoverer, Rpc, TxRef};

const RESIDUAL_CONCURRENCY: usize = 8;

/// The smallest backfill window on a residual chain. A window's cost is its activity plus one round
/// trip per bisection level, so halving a busy one saves little and doubles the windows: 64-block
/// windows put one Base account at 7 hours (2026-10-15).
pub const RESIDUAL_MIN_SPAN: u64 = 16_384;

/// Chains discovered by the residual: OP-stack chains, whose `trace_filter` re-executes.
pub fn residual_chain(chain_id: u64) -> bool {
    matches!(chain_id, 10 | 8453)
}

fn wei(v: Option<&Value>) -> Result<u128> {
    match v.and_then(Value::as_str) {
        None => Ok(0),
        Some(s) => u128::from_str_radix(s.trim_start_matches("0x"), 16)
            .with_context(|| format!("not a quantity: {s}")),
    }
}

/// What the account's own transactions in one block spent and brought back.
#[derive(Debug, Default, Clone, Copy)]
struct Own {
    out: u128,
    known_in: u128,
}

/// The value one transaction's call frames credit to `a`, and whether any of it is below the root.
/// Value under a reverted frame never moved.
fn credits(frame: &Value, a: &str, root: bool, failed_above: bool) -> Result<(u128, bool, bool)> {
    let failed = failed_above || frame.get("error").is_some_and(|e| !e.is_null());
    let kind = frame
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_ascii_uppercase();
    let to_a = frame
        .get("to")
        .and_then(Value::as_str)
        .is_some_and(|t| t.eq_ignore_ascii_case(a));
    let value = wei(frame.get("value"))?;
    let moves = !failed
        && to_a
        && value > 0
        && matches!(
            kind.as_str(),
            "CALL" | "SELFDESTRUCT" | "CREATE" | "CREATE2"
        );
    let (mut sum, mut at_root, mut inside) = (0u128, false, false);
    if moves {
        sum = value;
        at_root = root;
        inside = !root;
    }
    for c in frame
        .get("calls")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let (s, _, i) = credits(c, a, false, failed)?;
        sum = sum
            .checked_add(s)
            .ok_or_else(|| anyhow!("credits overflow"))?;
        inside |= i;
    }
    Ok((sum, at_root, inside))
}

impl<M: Rpc, T: Rpc> Discoverer<M, T> {
    /// The normal transactions of `a` (lowercase `0x…`) in `[from, to]` and the hashes whose internal
    /// frames credit it, as `trace_filter` discovery would give them.
    pub(crate) async fn residual_transactions(
        &self,
        a: &str,
        from: u64,
        to: u64,
    ) -> Result<(Vec<TxRef>, Vec<String>)> {
        // Genesis holds no transactions but may allocate a balance or a nonce, so a window from
        // block 0 starts from block 0's state.
        let (lo, prior) = if from == 0 { (1, 0) } else { (from, from - 1) };
        // Another account's code can spend a contract's or a delegated account's balance, and then
        // the residual proves nothing.
        let (code_from, code_to, before, after, bal_before, bal_end) = futures::try_join!(
            self.code_at(a, from),
            self.code_at(a, to),
            self.nonce_at(a, prior),
            self.nonce_at(a, to),
            self.balance_at(a, prior),
            self.balance_at(a, to),
        )?;
        for (b, code) in [(from, code_from), (to, code_to)] {
            if code != "0x" {
                bail!(
                    "{a} has code at block {b}; the balance residual proves nothing for a contract or \
                     a delegated account, so [{from}, {to}] is not recorded"
                );
            }
        }
        if to < lo {
            return Ok((Vec::new(), Vec::new()));
        }

        // Outgoing: every block in the window where the nonce rises, each level's midpoints asked
        // together.
        let mut rises: BTreeMap<u64, u64> = BTreeMap::new();
        let mut level = vec![(lo, to, before, after)];
        while !level.is_empty() {
            let mut split = Vec::new();
            for (s, e, nb, ne) in level {
                if ne < nb {
                    bail!("{a}'s nonce fell between blocks {s} and {e}");
                }
                if nb == ne {
                    continue;
                }
                if s == e {
                    rises.insert(s, ne - nb);
                } else {
                    split.push((s, e, nb, ne));
                }
            }
            let mids: Vec<u64> = futures::stream::iter(
                split
                    .iter()
                    .map(|(s, e, _, _)| s + (e - s) / 2)
                    .collect::<Vec<u64>>(),
            )
            .map(|m| self.nonce_at(a, m))
            .buffered(RESIDUAL_CONCURRENCY)
            .try_collect()
            .await?;
            level = split
                .into_iter()
                .zip(mids)
                .flat_map(|((s, e, nb, ne), nm)| {
                    let m = s + (e - s) / 2;
                    [(s, m, nb, nm), (m + 1, e, nm, ne)]
                })
                .collect();
        }

        let mut roots: BTreeMap<String, TxRef> = BTreeMap::new();
        let mut own: BTreeMap<u64, Own> = BTreeMap::new();
        let sent: Vec<(u64, Value, Vec<Value>)> = futures::stream::iter(rises.clone())
            .map(|(b, rise)| async move {
                let block = self.block_with_txs(b).await?;
                let mine: Vec<Value> = block
                    .get("transactions")
                    .and_then(Value::as_array)
                    .ok_or_else(|| anyhow!("block {b} has no transaction list"))?
                    .iter()
                    .filter(|t| field(t, "from").eq_ignore_ascii_case(a))
                    .cloned()
                    .collect();
                // An EIP-7702 authorisation the account signed raises its nonce too, from anyone's
                // transaction, and leaves its balance open to the delegate's code.
                if mine.len() as u64 != rise {
                    bail!(
                        "{a}'s nonce rose by {rise} in block {b}, which holds {} transactions from it; \
                         an authorisation or something else moved it, so [{from}, {to}] is not recorded",
                        mine.len()
                    );
                }
                let mut receipts = Vec::with_capacity(mine.len());
                for t in &mine {
                    let r = self
                        .main
                        .call("eth_getTransactionReceipt", json!([field(t, "hash")]))
                        .await?;
                    if r.is_null() {
                        bail!("{} has no receipt at the main endpoint", field(t, "hash"));
                    }
                    receipts.push(r);
                }
                Ok::<_, anyhow::Error>((b, block, mine.into_iter().zip(receipts).map(|(t, r)| json!([t, r])).collect()))
            })
            .buffered(RESIDUAL_CONCURRENCY)
            .try_collect()
            .await?;
        for (b, block, pairs) in sent {
            let block_hash = word32(field(&block, "hash"))?;
            let entry = own.entry(b).or_default();
            for pair in pairs {
                let (t, r) = (&pair[0], &pair[1]);
                let ok = field(r, "status") == "0x1";
                let deposit = field(t, "type") == "0x7e";
                let value = wei(t.get("value"))?;
                // A deposit from L1 mints to its sender and pays no fee on L2.
                let fee = if deposit {
                    0
                } else {
                    let gas = wei(r.get("gasUsed"))?;
                    let operator = match r.get("operatorFeeScalar").filter(|v| !v.is_null()) {
                        Some(scalar) => {
                            gas * wei(Some(scalar))? / 1_000_000
                                + wei(r.get("operatorFeeConstant"))?
                        }
                        None => 0,
                    };
                    gas * wei(r.get("effectiveGasPrice"))? + wei(r.get("l1Fee"))? + operator
                };
                entry.out += fee + if ok { value } else { 0 };
                if deposit {
                    entry.known_in += wei(t.get("mint"))?;
                }
                if ok && field(t, "to").eq_ignore_ascii_case(a) {
                    entry.known_in += value;
                }
                let hash = field(t, "hash").to_ascii_lowercase();
                roots.insert(
                    hash.clone(),
                    TxRef {
                        hash,
                        block: b,
                        outgoing: true,
                        failed: !ok,
                        block_hash: Some(block_hash),
                    },
                );
            }
        }

        // Incoming: bisect every range whose residual is positive down to single blocks.
        let residual = |s: u64, e: u64, bb: u128, be: u128| -> i128 {
            let (out, known_in) = own
                .range(s..=e)
                .fold((0u128, 0u128), |(o, k), (_, f)| (o + f.out, k + f.known_in));
            be as i128 - bb as i128 + out as i128 - known_in as i128
        };
        let mut incoming: Vec<(u64, u128)> = Vec::new();
        let mut level = vec![(lo, to, bal_before, bal_end)];
        while !level.is_empty() {
            let mut split = Vec::new();
            for (s, e, bb, be) in level {
                let r = residual(s, e, bb, be);
                if r < 0 {
                    bail!(
                        "{a} spent {} wei more in [{s}, {e}] than its own transactions explain; the \
                         fee rule or the provider is wrong, so [{from}, {to}] is not recorded",
                        -r
                    );
                }
                if r == 0 {
                    continue;
                }
                if s == e {
                    incoming.push((s, r as u128));
                } else {
                    split.push((s, e, bb, be));
                }
            }
            let mids: Vec<u128> = futures::stream::iter(
                split
                    .iter()
                    .map(|(s, e, _, _)| s + (e - s) / 2)
                    .collect::<Vec<u64>>(),
            )
            .map(|m| self.balance_at(a, m))
            .buffered(RESIDUAL_CONCURRENCY)
            .try_collect()
            .await?;
            level = split
                .into_iter()
                .zip(mids)
                .flat_map(|((s, e, bb, be), bm)| {
                    let m = s + (e - s) / 2;
                    [(s, m, bb, bm), (m + 1, e, bm, be)]
                })
                .collect();
        }

        // Trace only those blocks; the books must balance.
        let mut internal: BTreeSet<String> = BTreeSet::new();
        let traced: Vec<(u64, u128, Value, Value)> = futures::stream::iter(incoming)
            .map(|(b, r)| async move {
                let (block, trace) = futures::try_join!(
                    self.block_with_txs(b),
                    self.trace.call(
                        "debug_traceBlockByNumber",
                        json!([format!("0x{b:x}"), {"tracer": "callTracer"}])
                    )
                )?;
                Ok::<_, anyhow::Error>((b, r, block, trace))
            })
            .buffered(RESIDUAL_CONCURRENCY)
            .try_collect()
            .await?;
        for (b, r, block, trace) in traced {
            let txs = block
                .get("transactions")
                .and_then(Value::as_array)
                .ok_or_else(|| anyhow!("block {b} has no transaction list"))?;
            let frames = trace
                .as_array()
                .ok_or_else(|| anyhow!("debug_traceBlockByNumber {b} answered a non-list"))?;
            if frames.len() != txs.len() {
                bail!(
                    "block {b} holds {} transactions and its trace {}; not recording [{from}, {to}]",
                    txs.len(),
                    frames.len()
                );
            }
            let block_hash = word32(field(&block, "hash"))?;
            let mut credited = 0u128;
            for (t, f) in txs.iter().zip(frames) {
                let hash = field(t, "hash").to_ascii_lowercase();
                if let Some(h) = f.get("txHash").and_then(Value::as_str) {
                    if !h.eq_ignore_ascii_case(&hash) {
                        bail!("block {b}'s trace is not in its transactions' order");
                    }
                }
                let root = f.get("result").unwrap_or(f);
                let (sum, at_root, inside) = credits(root, a, true, false)?;
                credited += sum;
                if field(t, "type") == "0x7e" && field(t, "from").eq_ignore_ascii_case(a) {
                    credited += wei(t.get("mint"))?;
                }
                if at_root && !field(t, "from").eq_ignore_ascii_case(a) {
                    roots.entry(hash.clone()).or_insert(TxRef {
                        hash: hash.clone(),
                        block: b,
                        outgoing: false,
                        failed: false,
                        block_hash: Some(block_hash),
                    });
                }
                if inside {
                    internal.insert(hash);
                }
            }
            let expected = r + own.get(&b).map_or(0, |o| o.known_in);
            if credited != expected {
                bail!(
                    "block {b} credits {a} with {credited} wei by its trace, and its balance says \
                     {expected}; not recording [{from}, {to}]"
                );
            }
        }
        Ok((
            roots.into_values().collect(),
            internal.into_iter().collect(),
        ))
    }

    async fn balance_at(&self, a: &str, block: u64) -> Result<u128> {
        let v = self
            .main
            .call("eth_getBalance", json!([a, format!("0x{block:x}")]))
            .await?;
        wei(Some(&v))
    }

    async fn block_with_txs(&self, b: u64) -> Result<Value> {
        let block = self
            .main
            .call("eth_getBlockByNumber", json!([format!("0x{b:x}"), true]))
            .await?;
        if block.is_null() {
            bail!("block {b} is not at the main endpoint");
        }
        if hex_u64(block.get("number").unwrap_or(&Value::Null))? != b {
            bail!("asked for block {b} and was given another");
        }
        Ok(block)
    }
}

fn field<'a>(v: &'a Value, k: &str) -> &'a str {
    v.get(k).and_then(Value::as_str).unwrap_or("")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    const A: &str = "0x00000000000000000000000000000000000000aa";
    const B: &str = "0x00000000000000000000000000000000000000bb";
    const C: &str = "0x00000000000000000000000000000000000000cc";

    /// One transaction on the simulated chain. `inner` are value frames under the root, each
    /// `(to, value, reverted)`.
    #[derive(Clone)]
    struct Tx {
        block: u64,
        from: &'static str,
        to: &'static str,
        value: u128,
        ok: bool,
        fee: (u128, u128, u128),
        inner: Vec<(&'static str, u128, bool)>,
        mint: Option<u128>,
    }

    fn tx(block: u64, from: &'static str, to: &'static str, value: u128) -> Tx {
        Tx {
            block,
            from,
            to,
            value,
            ok: true,
            fee: (21_000, 7, 1_000),
            inner: vec![],
            mint: None,
        }
    }

    /// A chain whose state is derived from its transactions, answering what the residual asks.
    struct Sim {
        txs: Vec<Tx>,
        /// Extra nonce rises at a block, as an authorisation would make.
        auths: Vec<u64>,
        code_from: Option<u64>,
        /// Frames the trace leaves out, by (tx index, inner index).
        hide: Option<(usize, usize)>,
        /// What the receipt claims for `l1Fee` less than was charged.
        understate_l1: u128,
        asked: Mutex<Vec<String>>,
    }

    impl Sim {
        fn new(txs: Vec<Tx>) -> Sim {
            Sim {
                txs,
                auths: vec![],
                code_from: None,
                hide: None,
                understate_l1: 0,
                asked: Mutex::new(vec![]),
            }
        }
        fn hash(i: usize) -> String {
            format!("0x{:064x}", i + 1)
        }
        fn credited(t: &Tx) -> u128 {
            let mut v = if t.ok && t.to == A { t.value } else { 0 };
            if t.ok {
                v += t
                    .inner
                    .iter()
                    .filter(|(to, _, rev)| *to == A && !rev)
                    .map(|(_, v, _)| v)
                    .sum::<u128>();
            }
            if t.from == A {
                v += t.mint.unwrap_or(0);
            }
            v
        }
        fn debited(t: &Tx) -> u128 {
            if t.from != A {
                return 0;
            }
            let fee = if t.mint.is_some() {
                0
            } else {
                t.fee.0 * t.fee.1 + t.fee.2
            };
            fee + if t.ok { t.value } else { 0 }
        }
        fn balance(&self, b: u64) -> u128 {
            1_000_000_000
                + self
                    .txs
                    .iter()
                    .filter(|t| t.block <= b)
                    .map(Self::credited)
                    .sum::<u128>()
                - self
                    .txs
                    .iter()
                    .filter(|t| t.block <= b)
                    .map(Self::debited)
                    .sum::<u128>()
        }
        fn nonce(&self, b: u64) -> u64 {
            (self
                .txs
                .iter()
                .filter(|t| t.block <= b && t.from == A)
                .count()
                + self.auths.iter().filter(|x| **x <= b).count()) as u64
        }
        fn traced(&self) -> Vec<u64> {
            let asked = self.asked.lock().unwrap();
            asked
                .iter()
                .filter_map(|m| m.strip_prefix("trace ").map(|b| b.parse().unwrap()))
                .collect()
        }
    }

    impl Rpc for Sim {
        async fn call(&self, method: &str, params: Value) -> Result<Value> {
            let block_of = |v: &Value| {
                u64::from_str_radix(v.as_str().unwrap().trim_start_matches("0x"), 16).unwrap()
            };
            let q = |v: u128| json!(format!("0x{v:x}"));
            Ok(match method {
                "eth_getCode" => {
                    let b = block_of(&params[1]);
                    json!(if self.code_from.is_some_and(|c| b >= c) {
                        "0xef0100aa"
                    } else {
                        "0x"
                    })
                }
                "eth_getTransactionCount" => q(self.nonce(block_of(&params[1])) as u128),
                "eth_getBalance" => q(self.balance(block_of(&params[1]))),
                "eth_getBlockByNumber" => {
                    let b = block_of(&params[0]);
                    let txs: Vec<Value> = self.txs.iter().enumerate().filter(|(_, t)| t.block == b).map(|(i, t)| {
                        let mut j = json!({"hash": Sim::hash(i), "from": t.from, "to": t.to, "value": q(t.value),
                            "type": if t.mint.is_some() { "0x7e" } else { "0x2" }});
                        if let Some(m) = t.mint { j["mint"] = q(m); }
                        j
                    }).collect();
                    json!({"number": format!("0x{b:x}"), "hash": format!("0x{:064x}", b), "transactions": txs})
                }
                "eth_getTransactionReceipt" => {
                    let i = usize::from_str_radix(
                        params[0].as_str().unwrap().trim_start_matches("0x"),
                        16,
                    )
                    .unwrap()
                        - 1;
                    let t = &self.txs[i];
                    json!({"status": if t.ok { "0x1" } else { "0x0" }, "gasUsed": q(t.fee.0),
                        "effectiveGasPrice": q(t.fee.1), "l1Fee": q(t.fee.2 - self.understate_l1)})
                }
                "debug_traceBlockByNumber" => {
                    let b = block_of(&params[0]);
                    self.asked.lock().unwrap().push(format!("trace {b}"));
                    let frames: Vec<Value> = self.txs.iter().enumerate().filter(|(_, t)| t.block == b).map(|(i, t)| {
                        let calls: Vec<Value> = t.inner.iter().enumerate()
                            .filter(|(k, _)| self.hide != Some((i, *k)))
                            .map(|(_, (to, v, rev))| {
                                let mut f = json!({"type": "CALL", "from": t.to, "to": to, "value": q(*v)});
                                if *rev { f["error"] = json!("execution reverted"); }
                                f
                            }).collect();
                        let mut root = json!({"type": "CALL", "from": t.from, "to": t.to, "value": q(t.value), "calls": calls});
                        if !t.ok { root["error"] = json!("execution reverted"); }
                        json!({"txHash": Sim::hash(i), "result": root})
                    }).collect();
                    json!(frames)
                }
                other => bail!("unexpected {other}"),
            })
        }
    }

    fn discoverer(sim: Sim) -> Discoverer<std::sync::Arc<Sim>, std::sync::Arc<Sim>> {
        let s = std::sync::Arc::new(sim);
        Discoverer::new(s.clone(), s)
    }

    impl<R: Rpc> Rpc for std::sync::Arc<R> {
        async fn call(&self, method: &str, params: Value) -> Result<Value> {
            (**self).call(method, params).await
        }
    }

    fn found(r: &(Vec<TxRef>, Vec<String>)) -> (Vec<(String, bool, bool)>, Vec<String>) {
        (
            r.0.iter()
                .map(|t| (t.hash.clone(), t.outgoing, t.failed))
                .collect(),
            r.1.clone(),
        )
    }

    /// Outgoing from the nonce, incoming from the residual, and only the blocks that credit the
    /// account traced: a failed send, a self-send, a refund inside its own swap, a reverted credit
    /// and a deposit from L1 all land where they belong.
    #[tokio::test]
    async fn the_residual_finds_the_history_and_traces_only_crediting_blocks() {
        let mut failed = tx(300, A, B, 5);
        failed.ok = false;
        let mut swap = tx(400, A, C, 50);
        swap.inner = vec![(A, 20, false)];
        let mut reverted = tx(500, B, C, 0);
        reverted.inner = vec![(A, 99, true)];
        let mut paid = tx(600, B, C, 0);
        paid.inner = vec![(A, 30, false), (B, 1, false)];
        // In a block traced for a real credit, a credit under a reverted parent moved nothing.
        let mut ghost = tx(600, B, C, 0);
        ghost.ok = false;
        ghost.inner = vec![(A, 50, false)];
        let mut deposit = tx(700, A, A, 0);
        deposit.mint = Some(1_000);
        let txs = vec![
            tx(100, A, B, 10), // 0x1 sent
            tx(200, B, A, 40), // 0x2 received
            failed,            // 0x3 sent, failed
            swap,              // 0x4 sent, refunded inside
            reverted,          // 0x5 credits nothing
            paid,              // 0x6 credits inside
            deposit,           // 0x7 a deposit from L1
            tx(800, A, A, 3),  // 0x8 a self-send
            tx(900, B, C, 77),
            ghost, // 0x9 not ours
        ];
        let d = discoverer(Sim::new(txs));
        let r = d.residual_transactions(A, 0, 10_000).await.unwrap();
        let h = Sim::hash;
        let mut want = vec![
            (h(0), true, false),
            (h(1), false, false),
            (h(2), true, true),
            (h(3), true, false),
            (h(6), true, false),
            (h(7), true, false),
        ];
        want.sort();
        assert_eq!(found(&r), (want, vec![h(3), h(5)]));
        let mut traced = d.main.traced();
        traced.sort();
        assert_eq!(
            traced,
            [200, 400, 600],
            "only the blocks whose balance rose past the account's own flows"
        );
    }

    /// A trace that leaves out a credit cannot balance the block's books, and nothing is recorded.
    #[tokio::test]
    async fn a_block_whose_trace_misses_a_credit_is_refused() {
        let mut paid = tx(600, B, C, 0);
        paid.inner = vec![(A, 30, false)];
        let mut sim = Sim::new(vec![tx(100, A, B, 10), paid]);
        sim.hide = Some((1, 0));
        let err = discoverer(sim)
            .residual_transactions(A, 0, 1_000)
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("block 600 credits"), "{err:#}");
    }

    /// A fee the rule undercounts makes the account spend more than its transactions explain.
    #[tokio::test]
    async fn a_fee_the_rule_misses_is_refused() {
        let mut sim = Sim::new(vec![tx(100, A, B, 10)]);
        sim.understate_l1 = 1;
        let err = discoverer(sim)
            .residual_transactions(A, 0, 1_000)
            .await
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("than its own transactions explain"),
            "{err:#}"
        );
    }

    /// A nonce that rises without a transaction from the account is an authorisation it signed.
    #[tokio::test]
    async fn an_unexplained_nonce_rise_is_refused() {
        let mut sim = Sim::new(vec![tx(100, A, B, 10)]);
        sim.auths = vec![100];
        let err = discoverer(sim)
            .residual_transactions(A, 0, 1_000)
            .await
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("nonce rose by 2 in block 100"),
            "{err:#}"
        );
    }

    /// Delegated code lets another account's code spend the balance, so the window is refused.
    #[tokio::test]
    async fn an_account_with_code_is_refused() {
        let mut sim = Sim::new(vec![tx(100, A, B, 10)]);
        sim.code_from = Some(500);
        let err = discoverer(sim)
            .residual_transactions(A, 0, 1_000)
            .await
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("has code at block 1000"),
            "{err:#}"
        );
    }

    /// Random histories, every window placement: what the residual finds is exactly what the chain
    /// holds, and the blocks it traces are exactly those that credit the account.
    #[tokio::test]
    async fn random_histories_are_found_exactly() {
        let mut seed: u64 = 0x5eed;
        let mut next = |n: u64| {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (seed >> 33) % n
        };
        for _ in 0..200 {
            let mut txs = Vec::new();
            for _ in 0..next(12) {
                let block = 1 + next(2_000);
                let mut t = match next(3) {
                    0 => tx(block, A, B, next(50) as u128),
                    1 => tx(block, B, A, next(50) as u128),
                    _ => tx(block, B, C, next(50) as u128),
                };
                t.ok = next(5) != 0;
                for _ in 0..next(3) {
                    t.inner.push((
                        if next(2) == 0 { A } else { C },
                        next(30) as u128,
                        next(4) == 0,
                    ));
                }
                txs.push(t);
            }
            txs.sort_by_key(|t| t.block);
            let (from, to) = {
                let x = next(2_000);
                let y = next(2_000);
                // A quarter of windows start at genesis, whose allocation is not zero.
                (if next(4) == 0 { 0 } else { x.min(y) }, x.max(y))
            };
            let sim = Sim::new(txs.clone());
            let d = discoverer(sim);
            let r = d.residual_transactions(A, from, to).await.unwrap();
            let in_range: Vec<(usize, &Tx)> = txs
                .iter()
                .enumerate()
                .filter(|(_, t)| (from..=to).contains(&t.block))
                .collect();
            let mut want: Vec<(String, bool, bool)> = in_range
                .iter()
                .filter_map(|(i, t)| {
                    if t.from == A {
                        Some((Sim::hash(*i), true, !t.ok))
                    } else if t.ok && t.to == A && t.value > 0 {
                        Some((Sim::hash(*i), false, false))
                    } else {
                        None
                    }
                })
                .collect();
            want.sort();
            let want_internal: Vec<String> = in_range
                .iter()
                .filter(|(_, t)| {
                    t.ok && t
                        .inner
                        .iter()
                        .any(|(to, v, rev)| *to == A && *v > 0 && !rev)
                })
                .map(|(i, _)| Sim::hash(*i))
                .collect();
            assert_eq!(found(&r), (want, want_internal), "window [{from}, {to}]");
            let mut credit_blocks: Vec<u64> = in_range
                .iter()
                .filter(|(_, t)| {
                    t.from != A && Sim::credited(t) > 0
                        || t.from == A
                            && t.ok
                            && t.inner
                                .iter()
                                .any(|(to, v, rev)| *to == A && *v > 0 && !rev)
                })
                .map(|(_, t)| t.block)
                .collect();
            credit_blocks.sort();
            credit_blocks.dedup();
            let mut traced = d.main.traced();
            traced.sort();
            assert_eq!(traced, credit_blocks, "window [{from}, {to}]");
        }
    }
}
