//! The address-history source of a diff (RFC-0071 §15 step 4): the `txlist`, `txlistinternal` and
//! `tokentx` rows an RFC-0063 nest keeps per watched address in its redb store, read beside the SQL
//! source and never through it. Value a transaction or an internal call moved becomes a movement of
//! the chain's own currency, a token of its own that no amount of an ERC-20 is ever added to.

use std::collections::{BTreeMap, HashSet};

use anyhow::{bail, Context, Result};
use serde::Serialize;

use crate::address_history::{Action, DiffRow, DiffRows, DiffSource, DIFF_ACTIONS};
use crate::diff::{group, Flow, Leg, Movement, NestContext, Pos, Reads, Request, TableOut};

/// The token key of the chain's own currency in a flow: never an address, so never an ERC-20's.
pub const NATIVE: &str = "native";

/// The symbol of a chain's own currency, for the chains address history serves.
pub fn native_symbol(chain_id: Option<u64>) -> &'static str {
    match chain_id {
        Some(1 | 10 | 8453 | 42161) => "ETH",
        Some(56) => "BNB",
        Some(100) => "xDAI",
        Some(137) => "POL",
        Some(143) => "MON",
        _ => "native currency",
    }
}

const NOT_SEEN: &str = "Seen by this nest: every transaction, internal transfer and ERC-20 Transfer \
of any token with a watched address as a party. Not seen: anything between two addresses it does not \
watch; gas fees, withdrawals and block rewards; the watched addresses' NFT transfers, which /api \
serves; any balance change made without one of these (a rebase, for one). A reverted row is listed \
and moved nothing. Net flow is the sum of the transfers seen, not a balance.";

/// On chains whose history is proved by the balance residual (RFC-0063 §13), the same, and the one
/// kind of row that method does not list.
const NOT_SEEN_RESIDUAL: &str = "Seen by this nest: every transaction, internal transfer and ERC-20 \
Transfer of any token with a watched address as a party, except an incoming transaction that moves no \
value, which this chain's history does not list. Not seen: anything between two addresses it does not \
watch; gas fees, withdrawals and block rewards; the watched addresses' NFT transfers, which /api \
serves; any balance change made without one of these (a rebase, for one). A reverted row is listed \
and moved nothing. Net flow is the sum of the transfers seen, not a balance.";

pub fn not_seen(chain_id: Option<u64>) -> &'static str {
    if chain_id.is_some_and(crate::address_residual::residual_chain) {
        NOT_SEEN_RESIDUAL
    } else {
        NOT_SEEN
    }
}

/// What the address-history rows in range held.
#[derive(Debug, Serialize)]
pub struct HistoryOut {
    pub watched: Vec<String>,
    pub native_symbol: &'static str,
    pub actions: Vec<ActionOut>,
    /// Transactions that carried value and internal transfers, reverted ones included: the rows
    /// Etherscan's `txlist` (value above zero) and `txlistinternal` list.
    pub native_rows: u64,
    /// The first `top` of them in chain order. `/api` serves every row.
    pub native: Vec<NativeOut>,
}

#[derive(Debug, Serialize)]
pub struct ActionOut {
    pub action: &'static str,
    /// Distinct rows in range: a row filed under two watched addresses is one row.
    pub rows: u64,
    /// Rows read as movements.
    pub moved: u64,
    /// Transactions and internal rows that carried no value.
    pub no_value: u64,
    /// Rows marked failed: listed, and they moved nothing.
    pub reverted: u64,
    /// Copies of a row already read, under another watched address or as an event row.
    pub shared: u64,
}

#[derive(Debug, Serialize)]
pub struct NativeOut {
    pub block: u64,
    pub tx_index: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_index: Option<u64>,
    pub tx_hash: String,
    /// `transaction`, or the internal row's type: `call`, `create`, `create2` or `self-destruct`.
    pub kind: String,
    pub from: String,
    /// The recipient, or the contract a creation made.
    pub to: String,
    /// Wei, as decimal text.
    pub value: String,
    pub reverted: bool,
    pub text: String,
}

impl HistoryOut {
    /// The tables a document lists for these rows, so its counts still sum to its total.
    pub fn tables(&self) -> Vec<TableOut> {
        self.actions
            .iter()
            .filter(|a| a.rows > 0)
            .map(|a| TableOut {
                table: a.action.to_string(),
                reads: Some(match a.action {
                    "txlist" => Reads::NativeValue,
                    "txlistinternal" => Reads::NativeInternal,
                    _ => Reads::Transfer,
                }),
                rows: a.rows,
            })
            .collect()
    }
}

/// The data identity of the address-history nest at `dir`, for the provenance footer. Its partition
/// cache is derived state of up to `cache_mb`, which the identity would otherwise read whole.
pub fn data_identity(dir: &std::path::Path) -> Option<String> {
    crate::blob::build_manifest(dir, Some(&dir.join("partitions")))
        .ok()
        .map(|m| m.data_identity())
}

/// What an address-history nest's config says about it. Its history starts at `[address_history]`'s
/// `start_block`, or where the chain's traces can first be trusted.
pub fn facts(cfg: &crate::config::Config, data_identity: Option<String>) -> crate::diff::Facts {
    let mut f = crate::diff::Facts::from_config(cfg, data_identity);
    f.start_block = cfg.address_history.as_ref().map(|h| {
        h.start_block
            .unwrap_or(0)
            .max(crate::address_history::traced_from(cfg.nest.chain_id))
    });
    f.threshold = None;
    f
}

/// The diff of an address-history nest: the rows read, through the one pipeline every surface uses.
pub async fn produce(
    dir: &std::path::Path,
    facts: crate::diff::Facts,
    rows: &(dyn DiffSource + Sync),
    ask: &crate::diff::Ask,
) -> Result<crate::diff::Produced> {
    let ctx = context(dir, facts, rows.meta());
    let stories = BTreeMap::new();
    let inputs = crate::diff::Inputs {
        tables: &[],
        ctx: &ctx,
        stories: &stories,
        warnings: Vec::new(),
        history: Some(rows),
    };
    crate::diff::produce(&crate::diff::NoTables, inputs, ask).await
}

/// The context of an address-history nest: its config's facts, its store's finality and coverage,
/// and the files beside it.
pub fn context(dir: &std::path::Path, facts: crate::diff::Facts, rows: &DiffRows) -> NestContext {
    let indexed_head = rows
        .coverage
        .iter()
        .map(|(_, _, spans)| spans.last().map(|s| s.1))
        .collect::<Option<Vec<u64>>>()
        .and_then(|ends| ends.into_iter().min());
    let mut ctx = NestContext {
        version: env!("CARGO_PKG_VERSION").to_string(),
        name: facts.name,
        chain: facts.chain,
        chain_id: facts.chain_id,
        data_identity: facts.data_identity,
        aliases: facts.aliases,
        start_block: facts.start_block,
        indexed_head,
        sealed_through: rows.head.unwrap_or(0),
        ..Default::default()
    };
    ctx.add_files(dir);
    ctx
}

/// The span of `spans` that does not cover `[from, to]`, lowest first.
fn first_gap(spans: &[(u64, u64)], (from, to): (u64, u64)) -> Option<(u64, u64)> {
    let mut at = from;
    for &(f, t) in spans {
        if t < at {
            continue;
        }
        if f > at {
            return Some((at, (f - 1).min(to)));
        }
        if t >= to {
            return None;
        }
        at = t + 1;
    }
    (at <= to).then_some((at, to))
}

/// Refuse what the rows cannot answer whole: a store in conflict with the chain, an address whose
/// history cannot be proved, a range one watched address has not been read over, or too many rows.
pub fn check(rows: &DiffRows, req: &Request) -> Result<()> {
    if req.from > req.to {
        bail!(
            "--from {} is after --to {}: a range runs forwards",
            req.from,
            req.to
        );
    }
    if let Some(why) = &rows.conflict {
        bail!("this nest's history stopped matching the chain, so it answers nothing: {why}");
    }
    if rows.watched.is_empty() {
        bail!("this nest watches no address, so it has no history to tell");
    }
    for (a, from) in &rows.unprovable {
        if *from <= req.to {
            bail!(
                "{a} had code from block {}, and this chain's history is proved by its balance, \
                 which another account's code can spend. A diff reaching block {} would be \
                 incomplete; end the range before block {}.",
                group(*from),
                group(req.to),
                group(*from)
            );
        }
    }
    for (a, action, spans) in &rows.coverage {
        if let Some((f, t)) = first_gap(spans, (req.from, req.to)) {
            let have = if spans.is_empty() {
                "none of it".to_string()
            } else {
                spans
                    .iter()
                    .map(|(f, t)| format!("{} to {}", group(*f), group(*t)))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            bail!(
                "blocks {} to {} of {a}'s {} are not indexed (this nest has {have}). A diff over \
                 blocks the nest never read would be empty, not true.",
                group(f),
                group(t),
                action.etherscan()
            );
        }
    }
    if rows.counted > crate::diff::ROW_CAP {
        let span = req.to - req.from + 1;
        let narrower =
            (span as u128 * crate::diff::ROW_CAP as u128 / rows.counted as u128).max(1) as u64;
        bail!(
            "{} hold {} address-history rows, over the diff's cap of {}. Nothing was told rather \
             than a part of it. Try a range of about {} blocks.",
            crate::diff::what_text(req),
            group(rows.counted),
            group(crate::diff::ROW_CAP),
            group(narrower)
        );
    }
    Ok(())
}

fn kind_text(kind: &str) -> &str {
    match kind {
        "transaction" => "transaction value",
        "call" => "internal call",
        "create" => "internal create",
        "create2" => "internal create2",
        "self-destruct" => "self-destruct",
        other => other,
    }
}

fn hex_of(b: &[u8]) -> String {
    format!("0x{}", alloy_primitives::hex::encode(b))
}

/// A native row kept for the listing: its chain-order key, and the row.
type Kept = ((u64, u64, u64, [u8; 32]), DiffRow);

/// Fold the rows into `flow` and say what they held, listing the first `top` native rows in chain
/// order. A row filed under two watched addresses is read once, keyed by what makes it one row on
/// chain: a transaction by its hash, an internal transfer by its hash and trace index, a token transfer
/// by its block and log index, which also matches an event row the SQL source already read.
pub fn fold(
    src: &dyn DiffSource,
    req: &Request,
    ctx: &NestContext,
    flow: &mut Flow,
    top: usize,
) -> Result<HistoryOut> {
    let meta = src.meta();
    let tx = match &req.tx {
        Some(t) => Some(crate::address_history::fixed_hash(t)?),
        None => None,
    };
    let watched: HashSet<[u8; 20]> = meta
        .watched
        .iter()
        .map(|a| crate::address_history::parse_address(a))
        .collect::<Result<_>>()?;
    let mut counts: Vec<ActionOut> = DIFF_ACTIONS
        .iter()
        .map(|a| ActionOut {
            action: a.etherscan(),
            rows: 0,
            moved: 0,
            no_value: 0,
            reverted: 0,
            shared: 0,
        })
        .collect();
    // Only a row with two watched parties can be filed twice, so only those are remembered.
    let mut seen: HashSet<(usize, u64, [u8; 32], u64)> = HashSet::new();
    let mut hashes: BTreeMap<u64, [u8; 32]> = BTreeMap::new();
    let mut kept: Vec<Kept> = Vec::new();
    let mut native_rows = 0u64;
    src.each_row(&mut |r: DiffRow| {
        crate::diff::check_edges(flow, req)?;
        if r.block < req.from || r.block > req.to || tx.is_some_and(|t| t != r.hash) {
            return Ok(());
        }
        if let Some(h) = r.block_hash {
            hashes.insert(r.block, h);
        }
        let code = DIFF_ACTIONS
            .iter()
            .position(|a| *a == r.action)
            .expect("a snapshot reads only DIFF_ACTIONS");
        let c = &mut counts[code];
        let key = match r.action {
            Action::TxList => (code, 0, r.hash, 0),
            Action::TxListInternal => (code, 0, r.hash, r.position),
            _ => (
                code,
                r.block,
                [0; 32],
                r.log_index.with_context(|| {
                    format!("a tokentx row of {} has no logIndex", hex_of(&r.hash))
                })?,
            ),
        };
        let parties: HashSet<&[u8; 20]> = [Some(&r.from), r.to.as_ref(), r.contract.as_ref()]
            .into_iter()
            .flatten()
            .filter(|p| watched.contains(*p))
            .collect();
        let already_event = r.action == Action::TokenTx
            && flow
                .logs
                .as_ref()
                .is_some_and(|logs| logs.contains(&(r.block, key.3)));
        if already_event || (parties.len() > 1 && !seen.insert(key)) {
            c.shared += 1;
            return Ok(());
        }
        c.rows += 1;
        let from = hex_of(&r.from);
        if r.action == Action::TokenTx {
            let token = hex_of(&r.contract.unwrap_or_default());
            c.moved += 1;
            flow.fold(Movement {
                block: r.block,
                pos: Pos::Tx(r.tx_index, Leg::Log(key.3)),
                block_hash: r.block_hash.map(|h| hex_of(&h)).unwrap_or_default(),
                table: r.action.etherscan().to_string(),
                alias: token.clone(),
                token,
                from,
                to: hex_of(&r.to.unwrap_or_default()),
                amount: r.value,
                reads: Reads::Transfer,
            });
            return Ok(());
        }
        let (pos, reads) = match r.action {
            Action::TxList => (Pos::Tx(r.tx_index, Leg::Value), Reads::NativeValue),
            _ => (
                Pos::Tx(r.tx_index, Leg::Trace(r.position)),
                Reads::NativeInternal,
            ),
        };
        if r.failed {
            c.reverted += 1;
        } else if r.value.is_zero() {
            c.no_value += 1;
        } else {
            c.moved += 1;
            flow.fold(Movement {
                block: r.block,
                pos,
                block_hash: r.block_hash.map(|h| hex_of(&h)).unwrap_or_default(),
                table: r.action.etherscan().to_string(),
                token: NATIVE.to_string(),
                alias: NATIVE.to_string(),
                from,
                to: hex_of(&r.to.or(r.contract).unwrap_or_default()),
                amount: r.value,
                reads,
            });
        }
        // Etherscan's txlist lists every transaction; only those that carried value are value rows.
        if r.action == Action::TxList && r.value.is_zero() {
            return Ok(());
        }
        native_rows += 1;
        let rank = match pos {
            Pos::Tx(_, Leg::Trace(t)) => t + 1,
            _ => 0,
        };
        kept.push(((r.block, r.tx_index, rank, r.hash), r));
        if kept.len() >= 2 * top + 16 {
            kept.sort_by_key(|k| k.0);
            kept.truncate(top);
        }
        Ok(())
    })?;
    crate::diff::check_edges(flow, req)?;
    // An internal row carries no block hash; another row of its block may.
    for end in [&mut flow.first, &mut flow.last] {
        if let Some((b, _, h)) = end.as_mut().filter(|e| e.2.is_empty()) {
            if let Some(found) = hashes.get(b) {
                *h = hex_of(found);
            }
        }
    }
    kept.sort_by_key(|k| k.0);
    kept.truncate(top);
    let native = kept.iter().map(|(_, r)| native_out(r, ctx)).collect();
    Ok(HistoryOut {
        watched: meta.watched.clone(),
        native_symbol: native_symbol(ctx.chain_id),
        actions: counts,
        native_rows,
        native,
    })
}

fn native_out(r: &DiffRow, ctx: &NestContext) -> NativeOut {
    let kind = match r.action {
        Action::TxList => "transaction".to_string(),
        _ if r.kind.is_empty() => "call".to_string(),
        _ => r.kind.clone(),
    };
    let from = hex_of(&r.from);
    let to = hex_of(&r.to.or(r.contract).unwrap_or_default());
    let mut text = format!(
        "{} -> {}{}: {}, {}",
        ctx.address_text(&from),
        ctx.address_text(&to),
        if r.to.is_none() { " (created)" } else { "" },
        ctx.amount_text(NATIVE, &r.value.to_string()),
        kind_text(&kind)
    );
    if r.failed {
        text.push_str(", reverted: moved nothing");
    }
    NativeOut {
        block: r.block,
        tx_index: r.tx_index,
        trace_index: (r.action == Action::TxListInternal).then_some(r.position),
        tx_hash: hex_of(&r.hash),
        kind,
        from,
        to,
        value: r.value.to_string(),
        reverted: r.failed,
        text,
    }
}

/// `2 watched addresses (0x…, 0x…)`.
pub fn watched_text(h: &HistoryOut) -> String {
    format!(
        "{} {} ({})",
        group(h.watched.len()),
        if h.watched.len() == 1 {
            "watched address"
        } else {
            "watched addresses"
        },
        h.watched.join(", ")
    )
}

fn plural(n: u64, one: &str, many: &str) -> String {
    format!("{} {}", group(n), if n == 1 { one } else { many })
}

/// The history section as (depth, line): the counts per action, then every native row up to `top`.
pub fn lines(h: &HistoryOut, top: usize) -> Vec<(usize, String)> {
    let mut out = vec![(
        0,
        format!(
            "Native {} of {}, in chain order",
            h.native_symbol,
            watched_text(h)
        ),
    )];
    for a in &h.actions {
        let mut parts = vec![plural(a.rows, "row", "rows")];
        match a.action {
            "tokentx" => parts.push(plural(a.moved, "token transfer", "token transfers")),
            _ => {
                parts.push(format!("{} moved {}", group(a.moved), h.native_symbol));
                parts.push(format!("{} carried none", group(a.no_value)));
                parts.push(format!("{} reverted", group(a.reverted)));
            }
        }
        if a.shared > 0 {
            parts.push(format!(
                "{} also filed under another watched address, read once",
                group(a.shared)
            ));
        }
        out.push((1, format!("{}: {}", a.action, parts.join(", "))));
    }
    if h.native_rows == 0 {
        out.push((
            1,
            format!("No {} moved or listed in range.", h.native_symbol),
        ));
    }
    for n in h.native.iter().take(top) {
        let at = match n.trace_index {
            Some(t) => format!("tx {} trace {t}", n.tx_index),
            None => format!("tx {}", n.tx_index),
        };
        out.push((
            1,
            format!("block {} {at} ({}): {}", group(n.block), n.tx_hash, n.text),
        ));
    }
    let more = h.native_rows.saturating_sub(h.native.len().min(top) as u64);
    if more > 0 {
        out.push((
            1,
            format!(
                "{} not listed: raise --top, or read them at /api",
                plural(more, "more row", "more rows")
            ),
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::U256;
    use serde_json::json;

    const A: [u8; 20] = [0xaa; 20];
    const B: [u8; 20] = [0xbb; 20];
    const C: [u8; 20] = [0xcc; 20];
    const TOKEN: [u8; 20] = [0xdd; 20];
    const H: [u8; 32] = [0x11; 32];

    fn row(
        action: Action,
        under: [u8; 20],
        position: u64,
        from: [u8; 20],
        to: [u8; 20],
    ) -> DiffRow {
        DiffRow {
            action,
            address: under,
            block: 10,
            tx_index: 2,
            position,
            hash: H,
            block_hash: Some([0xbb; 32]),
            from,
            to: Some(to),
            contract: None,
            value: U256::ZERO,
            failed: false,
            kind: String::new(),
            log_index: None,
        }
    }

    fn tx(under: [u8; 20], from: [u8; 20], to: [u8; 20], value: u64, failed: bool) -> DiffRow {
        DiffRow {
            value: U256::from(value),
            failed,
            ..row(Action::TxList, under, 0, from, to)
        }
    }

    fn internal(under: [u8; 20], trace: u64, from: [u8; 20], to: [u8; 20], value: u64) -> DiffRow {
        DiffRow {
            value: U256::from(value),
            kind: "call".into(),
            block_hash: None,
            ..row(Action::TxListInternal, under, trace, from, to)
        }
    }

    fn token(under: [u8; 20], log: u64, from: [u8; 20], to: [u8; 20], value: u64) -> DiffRow {
        DiffRow {
            value: U256::from(value),
            contract: Some(TOKEN),
            log_index: Some(log),
            ..row(Action::TokenTx, under, log << 16, from, to)
        }
    }

    fn rows(rows: Vec<DiffRow>) -> DiffRows {
        DiffRows {
            chain_id: 1,
            watched: vec![hex_of(&A), hex_of(&B)],
            head: Some(100),
            rows,
            ..Default::default()
        }
    }

    fn ctx() -> NestContext {
        NestContext {
            chain_id: Some(1),
            ..Default::default()
        }
    }

    const REQ: Request = Request {
        from: 0,
        to: 100,
        tx: None,
    };

    fn sent(flow: &Flow, token: &str, who: [u8; 20]) -> String {
        flow.sent
            .get(&(token.to_string(), hex_of(&who)))
            .map(|v| v.to_string())
            .unwrap_or_default()
    }

    /// A transfer between two watched addresses is filed under both; the diff reads it once.
    #[test]
    fn a_row_two_watched_addresses_share_is_read_once() {
        let r = rows(vec![
            tx(A, A, B, 5, false),
            internal(A, 3, B, A, 2),
            token(A, 7, A, B, 9),
            tx(B, A, B, 5, false),
            internal(B, 3, B, A, 2),
            token(B, 7, A, B, 9),
        ]);
        let mut flow = Flow::default();
        let h = fold(&r, &REQ, &ctx(), &mut flow, 10).unwrap();
        for a in &h.actions {
            assert_eq!((a.rows, a.moved, a.shared), (1, 1, 1), "{}", a.action);
        }
        assert_eq!(sent(&flow, NATIVE, A), "5");
        assert_eq!(sent(&flow, NATIVE, B), "2");
        assert_eq!(sent(&flow, &hex_of(&TOKEN), A), "9");
        assert_eq!((h.native_rows, h.native.len()), (2, 2));
        assert_eq!(h.tables().iter().map(|t| t.rows).sum::<u64>(), 3);
    }

    /// The chain's own currency is a token of its own: never summed with an ERC-20 of the same
    /// amount, and named by the chain's symbol with the 18 decimals of wei.
    #[test]
    fn native_value_is_never_added_to_a_token() {
        let wei = 1_000_000_000_000_000_000;
        let r = rows(vec![tx(A, A, C, wei, false), token(A, 1, A, C, wei)]);
        let mut flow = Flow::default();
        fold(&r, &REQ, &ctx(), &mut flow, 10).unwrap();
        assert_eq!(flow.token_rows.get(NATIVE), Some(&1));
        assert_eq!(flow.token_rows.get(&hex_of(&TOKEN)), Some(&1));
        assert_eq!(sent(&flow, NATIVE, A), wei.to_string());
        assert_eq!(sent(&flow, &hex_of(&TOKEN), A), wei.to_string());
        assert_eq!(
            ctx().amount_text(NATIVE, "1500000000000000001"),
            "1.500000000000000001 ETH"
        );
    }

    /// A reverted transaction is listed with the value it carried and moves nothing.
    #[test]
    fn a_reverted_row_is_listed_and_moves_nothing() {
        let r = rows(vec![tx(A, A, C, 7, true), internal(A, 1, A, C, 0)]);
        let mut flow = Flow::default();
        let h = fold(&r, &REQ, &ctx(), &mut flow, 10).unwrap();
        assert!(flow.sent.is_empty());
        assert_eq!(h.actions[0].reverted, 1);
        assert_eq!(h.actions[1].no_value, 1);
        assert_eq!(h.native.len(), 2, "Etherscan lists both");
        assert!(h.native[0].reverted);
        assert!(
            h.native[0]
                .text
                .ends_with("transaction value, reverted: moved nothing"),
            "{}",
            h.native[0].text
        );
    }

    /// A `tokentx` row for a log an event table already gave the flow is that log, not a second one.
    #[test]
    fn a_log_the_event_source_read_is_not_counted_again() {
        let r = rows(vec![token(A, 7, A, B, 9)]);
        let mut flow = Flow::default();
        flow.logs = Some([(10, 7)].into_iter().collect());
        let h = fold(&r, &REQ, &ctx(), &mut flow, 10).unwrap();
        assert_eq!((h.actions[2].rows, h.actions[2].shared), (0, 1));
        assert!(flow.sent.is_empty());
    }

    /// The listing is the first `top` native rows in chain order, whatever order the store gave them
    /// in, and counts the rest.
    #[test]
    fn the_listing_keeps_the_first_rows_in_chain_order() {
        let many: Vec<DiffRow> = (0..100u64)
            .rev()
            .map(|i| DiffRow {
                block: 1 + i,
                hash: [i as u8; 32],
                ..tx(A, A, C, 1 + i, false)
            })
            .collect();
        let mut flow = Flow::default();
        let h = fold(&rows(many), &REQ, &ctx(), &mut flow, 3).unwrap();
        assert_eq!(h.native_rows, 100);
        let blocks: Vec<u64> = h.native.iter().map(|n| n.block).collect();
        assert_eq!(blocks, [1, 2, 3]);
        assert_eq!(
            sent(&flow, NATIVE, A),
            (1..=100u64).sum::<u64>().to_string()
        );
    }

    #[test]
    fn a_gap_in_any_watched_address_is_refused_with_what_is_covered() {
        let (a, b) = (hex_of(&A), hex_of(&B));
        let mut r = rows(Vec::new());
        r.coverage = vec![
            (a, Action::TxList, vec![(0, 100)]),
            (b.clone(), Action::TxListInternal, vec![(0, 40), (60, 100)]),
        ];
        let e = check(&r, &REQ).unwrap_err().to_string();
        assert!(
            e.starts_with(&format!(
                "blocks 41 to 59 of {b}'s txlistinternal are not indexed (this nest has 0 to 40, \
                 60 to 100)"
            )),
            "{e}"
        );
        r.coverage.pop();
        check(&r, &REQ).unwrap();
        r.unprovable = vec![(b, 100)];
        let e = check(&r, &REQ).unwrap_err().to_string();
        assert!(e.contains("had code from block 100"), "{e}");
        r.unprovable.clear();
        r.counted = crate::diff::ROW_CAP + 1;
        let e = check(&r, &REQ).unwrap_err().to_string();
        assert!(e.contains("over the diff's cap"), "{e}");
        r.counted = 0;
        r.conflict = Some("a fork".into());
        let e = check(&r, &REQ).unwrap_err().to_string();
        assert!(e.contains("a fork"), "{e}");
    }

    #[test]
    fn first_gap_finds_the_lowest_uncovered_span() {
        assert_eq!(first_gap(&[], (5, 9)), Some((5, 9)));
        assert_eq!(first_gap(&[(0, 9)], (5, 9)), None);
        assert_eq!(first_gap(&[(0, 6)], (5, 9)), Some((7, 9)));
        assert_eq!(first_gap(&[(7, 20)], (5, 9)), Some((5, 6)));
        assert_eq!(first_gap(&[(0, 5), (7, 9)], (5, 9)), Some((6, 6)));
        assert_eq!(first_gap(&[(0, u64::MAX)], (5, u64::MAX)), None);
    }

    /// Dumps what a diff reads of a recorded address-history nest, rows, coverage and finalized
    /// hashes, as JSON a test records into a fresh store; the store itself is mostly caches:
    /// `NUTHATCH_AH_FROM=<nest> NUTHATCH_AH_TO=<file> cargo test --lib dump_for_fixture -- --ignored`.
    #[test]
    #[ignore]
    fn dump_for_fixture() {
        use crate::address_history::{parse_address, AddressHistory};
        let from = std::path::PathBuf::from(std::env::var("NUTHATCH_AH_FROM").unwrap());
        let to = std::path::PathBuf::from(std::env::var("NUTHATCH_AH_TO").unwrap());
        let cfg = crate::config::Config::load(&from).unwrap();
        let src = AddressHistory::reader(
            crate::store::Store::open_existing(&from.join(crate::config::DB_FILE)).unwrap(),
            cfg.nest.chain_id,
        );
        let snapshot = src.diff_snapshot((0, u64::MAX - 1)).unwrap();
        let all = snapshot.meta();
        let rtx = src.begin_read().unwrap();
        let mut rows = Vec::new();
        for a in &all.watched {
            for action in DIFF_ACTIONS {
                let keyed = src
                    .keyed_rows(&rtx, action, &parse_address(a).unwrap(), (0, u64::MAX - 1))
                    .unwrap();
                for ((block, tx_index, position), record) in keyed {
                    rows.push(
                        json!({"action": action.etherscan(), "address": a, "block": block,
                                     "tx_index": tx_index, "position": position, "record": record}),
                    );
                }
            }
        }
        let dump = json!({
            "chain_id": all.chain_id,
            "watched": all.watched,
            "finals": src.finals().unwrap().iter()
                .map(|(b, h)| json!([b, hex_of(h)]))
                .collect::<Vec<_>>(),
            "coverage": all.coverage.iter()
                .map(|(a, action, spans)| json!({"address": a, "action": action.etherscan(), "spans": spans}))
                .collect::<Vec<_>>(),
            "rows": rows,
        });
        let mut s = serde_json::to_string_pretty(&dump).unwrap();
        s.push('\n');
        std::fs::write(to, s).unwrap();
    }

    /// Writes an address-history nest of `$NUTHATCH_AH_ROWS` rows (default 1,990,000, under the row
    /// cap) for two watched addresses, with `$NUTHATCH_AH_POOL` counterparties, for measuring RSS:
    /// `NUTHATCH_AH_BIG=<dir> cargo test --release --lib write_cap_sized_nest -- --ignored`.
    #[test]
    #[ignore]
    fn write_cap_sized_nest() {
        use crate::address_history::{AddressHistory, Row};
        let dir = std::path::PathBuf::from(std::env::var("NUTHATCH_AH_BIG").unwrap());
        let total: u64 = std::env::var("NUTHATCH_AH_ROWS")
            .map(|n| n.parse().unwrap())
            .unwrap_or(1_990_000);
        // Distinct counterparties; by default every row has its own, the worst case for the fold.
        let pool: u64 = std::env::var("NUTHATCH_AH_POOL")
            .map(|n| n.parse().unwrap())
            .unwrap_or(u64::MAX);
        std::fs::create_dir_all(&dir).unwrap();
        let (a, b) = (hex_of(&A), hex_of(&B));
        std::fs::write(
            dir.join(crate::config::CONFIG_FILE),
            format!(
                "[nest]\nname = \"big\"\nchain = \"mainnet\"\nchain_id = 1\n\
                 rpc_urls = [\"https://ethereum-rpc.publicnode.com\"]\nschema_version = 2\n\n\
                 [address_history]\naddresses = [\"{a}\", \"{b}\"]\nstart_block = 1000000\n"
            ),
        )
        .unwrap();
        let h = AddressHistory::open(
            crate::store::Store::open(&dir.join(crate::config::DB_FILE)).unwrap(),
            1,
            &[a.clone(), b.clone()],
        )
        .unwrap();
        let per = total / 6;
        let first = 1_000_000u64;
        let last = first + per / 10;
        for (who, action) in DIFF_ACTIONS.iter().flat_map(|x| [(&a, *x), (&b, *x)]) {
            let rows: Vec<Row> = (0..per)
                .map(|i| {
                    let block = first + i / 10;
                    let other = format!("0x{:040x}", (i + (action as u64) * total) % pool + 1);
                    let (from, to) = if i % 2 == 0 {
                        (other.as_str(), who.as_str())
                    } else {
                        (who.as_str(), other.as_str())
                    };
                    let mut r = json!({"blockNumber": block.to_string(),
                        "hash": format!("0x{:064x}", i + (who == &a) as u64 * total + (action as u64) * 4 * total),
                        "blockHash": format!("0x{block:064x}"), "from": from, "to": to,
                        "value": (i * 1_000_003 + 1).to_string(), "isError": "0",
                        "contractAddress": "", "type": "call"});
                    let position = match action {
                        Action::TokenTx => {
                            r["contractAddress"] = json!(format!("0x{:040x}", 0xee00 + i % 7));
                            r["logIndex"] = json!((i % 10).to_string());
                            (i % 10) << 16
                        }
                        Action::TxListInternal => 1,
                        _ => 0,
                    };
                    Row {
                        block,
                        tx_index: i % 10,
                        position,
                        record: r.as_object().unwrap().clone(),
                    }
                })
                .collect();
            let g = h.generation().unwrap();
            h.record(action, who, &rows, (first, last), g).unwrap();
        }
        h.finalize_at(last + 100, [9; 32]).unwrap();
        println!("blocks {first} to {last}, {} rows", per * 6);
    }
}
