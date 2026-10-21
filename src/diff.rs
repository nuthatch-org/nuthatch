//! `nuthatch diff --flow` (RFC-0071 §8, built first per §15): the token flow of a transaction, a
//! block or a block range, read from the rows a nest already serves and rendered as text, JSON, DOT
//! or Mermaid by fixed rules. Nothing here stores anything, and no model writes a word of it.
//!
//! What reads as flow is decided by topic0 and parameter shape, never by a table or column name: an
//! ERC-20 `Transfer` (two indexed addresses, an unindexed `uint256`), and WETH9's `Deposit` and
//! `Withdrawal` as a mint and a burn of the contract that emitted them (§14: WETH9 emits no
//! `Transfer` when it wraps or unwraps).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Write as _;
use std::future::Future;

use alloy_primitives::{keccak256, U256, U512};
use anyhow::{bail, Context, Result};
use serde::Serialize;
use serde_json::Value;

use crate::analytics::QueryOutput;
use crate::registry::{ColumnSchema, TableKind, TableSchema};

/// The most flow rows one diff will read (RFC-0071 §6). Counted before any row is fetched; above it
/// the diff is refused with the count, never truncated.
pub const ROW_CAP: u64 = 2_000_000;

/// Rows fetched per query. `/sql` answers at most 50,000, so a larger read is paged by key.
const PAGE: u64 = 50_000;

/// Edges per token, and addresses per token in the net-flow section, that the text and the drawings
/// show by default. JSON always carries all of them.
pub const DEFAULT_TOP: usize = 25;

pub const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

/// Printed at the end of every flow, because a short flow must never read as "nothing else moved".
pub const NOT_SEEN: &str = "Not seen by this nest: native ETH, internal calls, tokens it does not \
index, and any balance change made without one of these events (a rebase, for one). Net flow is the \
sum of the transfers seen, not a balance.";

/// Where the rows come from: a local store or a running instance, behind `nuthatch sql`'s backend.
pub trait Source {
    fn query(&self, sql: &str) -> impl Future<Output = Result<QueryOutput>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Reads {
    Transfer,
    Weth9Deposit,
    Weth9Withdrawal,
}

/// A table the flow reads, with its parameters bound by position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowTable {
    pub table: String,
    pub alias: String,
    pub reads: Reads,
    /// The sender column (`Transfer`) or the holder column (`Deposit`, `Withdrawal`).
    pub first: String,
    /// The recipient column, for a `Transfer` only.
    pub second: Option<String>,
    pub amount: String,
    /// The only emitters whose rows are flow; empty for any. A row from another emitter is counted
    /// under other events: a factory alias spans many contracts.
    pub emitters: Vec<String>,
}

#[derive(Debug, Default)]
pub struct Classified {
    pub flow: Vec<FlowTable>,
    /// Every other table, counted in range so each row is accounted for somewhere.
    pub other: Vec<String>,
}

fn topic(signature: &str) -> String {
    format!("0x{}", hex::encode(keccak256(signature.as_bytes())))
}

fn params(t: &TableSchema) -> Vec<&ColumnSchema> {
    t.columns
        .iter()
        .filter(|c| c.sol_type != "implicit")
        .collect()
}

/// Positional and typed: a parameter is matched by where it sits, its Solidity type and whether it
/// is indexed. WETH9 names its `Transfer` parameters `src`, `dst` and `wad`.
fn shape(t: &TableSchema, want: &[(&str, bool)]) -> Option<Vec<String>> {
    let p = params(t);
    (p.len() == want.len()
        && p.iter()
            .zip(want)
            .all(|(c, (ty, idx))| c.sol_type == *ty && c.indexed == *idx))
    .then(|| p.iter().map(|c| c.name.clone()).collect())
}

/// WETH9-style wrappers by chain id: contracts whose wrap and unwrap emit `Deposit` and `Withdrawal`
/// and no `Transfer`, each checked on chain (RFC-0071 §16). The signature alone proves nothing: a
/// Curve gauge emits the same `Deposit` beside its own `Transfer` from the zero address.
pub const WETH9_WRAPPERS: &[(u64, &str)] = &[
    (1, "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"),
    (10, "0x4200000000000000000000000000000000000006"),
    (56, "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"),
    (100, "0xe91d153e0b41518a2ce8dd3d7944fa863463a97d"),
    (137, "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270"),
    (143, "0x3bd359c1119da7da1d913d1c4d2b7c461115433a"),
    (8453, "0x4200000000000000000000000000000000000006"),
];

pub fn wrappers(chain_id: Option<u64>) -> Vec<String> {
    WETH9_WRAPPERS
        .iter()
        .filter(|(id, _)| Some(*id) == chain_id)
        .map(|(_, a)| a.to_string())
        .collect()
}

pub fn classify(tables: &[TableSchema], chain_id: Option<u64>) -> Classified {
    let transfer = topic("Transfer(address,address,uint256)");
    let deposit = topic("Deposit(address,uint256)");
    let withdrawal = topic("Withdrawal(address,uint256)");
    let wrappers = wrappers(chain_id);
    let mut flow = Vec::new();
    let mut other = Vec::new();
    for t in tables {
        let topic0 = t.topic0.to_ascii_lowercase();
        let event = t.kind == TableKind::Event;
        if event && topic0 == transfer {
            if let Some(c) = shape(
                t,
                &[("address", true), ("address", true), ("uint256", false)],
            ) {
                flow.push(FlowTable {
                    table: t.table.clone(),
                    alias: t.alias.clone(),
                    reads: Reads::Transfer,
                    first: c[0].clone(),
                    second: Some(c[1].clone()),
                    amount: c[2].clone(),
                    emitters: Vec::new(),
                });
                continue;
            }
        }
        if event && !wrappers.is_empty() && (topic0 == deposit || topic0 == withdrawal) {
            if let Some(c) = shape(t, &[("address", true), ("uint256", false)]) {
                flow.push(FlowTable {
                    table: t.table.clone(),
                    alias: t.alias.clone(),
                    reads: if topic0 == deposit {
                        Reads::Weth9Deposit
                    } else {
                        Reads::Weth9Withdrawal
                    },
                    first: c[0].clone(),
                    second: None,
                    amount: c[1].clone(),
                    emitters: wrappers.clone(),
                });
                continue;
            }
        }
        other.push(t.table.clone());
    }
    flow.sort_by(|a, b| a.table.cmp(&b.table));
    other.sort();
    Classified { flow, other }
}

/// Which rows: an inclusive block range, optionally narrowed to one transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Request {
    pub from: u64,
    pub to: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx: Option<String>,
}

/// What the nest says about itself, gathered by the caller from the store or the running instance.
#[derive(Debug, Clone, Default)]
pub struct NestContext {
    pub name: String,
    pub chain: String,
    pub chain_id: Option<u64>,
    pub data_identity: Option<String>,
    pub start_block: Option<u64>,
    pub indexed_head: Option<u64>,
    pub sealed_through: u64,
    pub metadata: crate::metadata::MetadataCache,
    pub metadata_sha256: Option<String>,
    /// Label snapshot names (each a content hash), sorted.
    pub label_snapshots: Vec<String>,
    pub labels: BTreeMap<String, Vec<String>>,
    /// Indexed contract addresses (lowercase) to their nest alias.
    pub aliases: BTreeMap<String, String>,
    pub semantic_sha256: Option<String>,
    pub version: String,
}

impl NestContext {
    /// An address with its nest alias and labels, as every format prints one.
    pub fn address_text(&self, address: &str) -> String {
        address_with(
            address,
            self.aliases.get(address).map(String::as_str),
            self.labels.get(address).map(Vec::as_slice).unwrap_or(&[]),
        )
    }

    /// Base units of the token at `token`, scaled by its `metadata.json` decimals when known.
    pub fn amount_text(&self, token: &str, base: &str) -> String {
        let meta = self.metadata.get(token);
        let name = meta
            .and_then(|m| m.symbol.clone())
            .or_else(|| self.aliases.get(token).cloned())
            .unwrap_or_else(|| token.to_string());
        amount_in(base, meta.and_then(|m| m.decimals), &name)
    }
}

/// The one exact-numbers rule (RFC-0071 §4): move the point by known decimals, or say base units.
pub fn amount_in(base: &str, decimals: Option<u8>, name: &str) -> String {
    match decimals {
        Some(d) => format!("{} {name}", scale(base, d)),
        None => format!("{} base units of {name} (decimals unknown)", group(base)),
    }
}

pub fn address_with(address: &str, alias: Option<&str>, labels: &[String]) -> String {
    if address == ZERO_ADDRESS {
        return format!("{address} (zero address)");
    }
    let mut tags: Vec<String> = alias.map(str::to_string).into_iter().collect();
    for l in labels {
        tags.push(format!("label {l}"));
    }
    if tags.is_empty() {
        address.to_string()
    } else {
        format!("{address} [{}]", tags.join("; "))
    }
}

/// Refuse a range the nest does not cover, with the interval it does, rather than answer it empty.
pub fn check_coverage(req: &Request, ctx: &NestContext) -> Result<()> {
    if req.from > req.to {
        bail!(
            "--from {} is after --to {}: a range runs forwards",
            req.from,
            req.to
        );
    }
    let Some(head) = ctx.indexed_head else {
        bail!("this nest has indexed nothing yet, so there is no flow to read");
    };
    let start = ctx.start_block.unwrap_or(0);
    if req.from < start || req.to > head {
        bail!(
            "blocks {} to {} are outside what this nest has indexed, which is blocks {} to {}. A \
             flow over blocks the nest never read would be empty, not true.",
            group(req.from),
            group(req.to),
            group(start),
            group(head)
        );
    }
    Ok(())
}

fn quote(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

pub(crate) fn filter(req: &Request) -> String {
    let mut w = format!(
        "block_number >= {} AND block_number <= {}",
        req.from, req.to
    );
    if let Some(tx) = &req.tx {
        let _ = write!(w, " AND tx_hash = '{tx}'");
    }
    w
}

fn text(v: Option<&Value>) -> Option<String> {
    match v? {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn number(v: Option<&Value>) -> Option<u64> {
    match v? {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

async fn count<S: Source>(src: &S, table: &str, req: &Request) -> Result<u64> {
    let out = src
        .query(&format!(
            "SELECT count(*) AS n FROM {} WHERE {}",
            quote(table),
            filter(req)
        ))
        .await
        .with_context(|| format!("counting {table}"))?;
    complete(&out, table)?;
    number(out.rows.first().and_then(|r| r.get("n")))
        .with_context(|| format!("counting {table}: no count came back"))
}

/// A flow drawn from a partial read would be a wrong number, so an incomplete answer is refused.
pub(crate) fn complete(out: &QueryOutput, table: &str) -> Result<()> {
    if out.tip_unavailable || !out.degraded_tables.is_empty() {
        bail!(
            "the nest could not serve complete data for {table} (the hot tip or a sealed segment \
             could not be read). A flow over a partial read would be wrong, so none is drawn; check \
             the node's logs."
        );
    }
    Ok(())
}

/// One movement of one token, as the chain recorded it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Movement {
    pub block: u64,
    pub log_index: u64,
    pub block_hash: String,
    pub table: String,
    pub token: String,
    pub alias: String,
    pub from: String,
    pub to: String,
    pub amount: U256,
    pub reads: Reads,
}

fn movement(f: &FlowTable, row: &Value) -> Result<Movement> {
    let field = |k: &str| text(row.get(k)).map(|s| s.to_ascii_lowercase());
    let block = number(row.get("block_number")).context("a row with no block_number")?;
    let log_index = number(row.get("log_index")).context("a row with no log_index")?;
    let token = field("address").context("a row with no address")?;
    let first = field("p0").context("a row with no sender or holder")?;
    let raw = field("p2").context("a row with no amount")?;
    let amount = U256::from_str_radix(&raw, 10)
        .with_context(|| format!("{}: amount {raw:?} is not a decimal uint256", f.table))?;
    let (from, to) = match f.reads {
        Reads::Transfer => (first, field("p1").context("a row with no recipient")?),
        Reads::Weth9Deposit => (ZERO_ADDRESS.to_string(), first),
        Reads::Weth9Withdrawal => (first, ZERO_ADDRESS.to_string()),
    };
    Ok(Movement {
        block,
        log_index,
        block_hash: field("block_hash").unwrap_or_default(),
        table: f.table.clone(),
        token,
        alias: f.alias.clone(),
        from,
        to,
        amount,
        reads: f.reads,
    })
}

/// Count the flow rows in range, refuse above the cap, then read them page by page into a [`Flow`].
pub async fn read<S: Source>(
    src: &S,
    classified: &Classified,
    req: &Request,
    cap: u64,
) -> Result<Flow> {
    let mut counts = BTreeMap::new();
    let mut total = 0u64;
    for f in &classified.flow {
        let n = count(src, &f.table, req).await?;
        total += n;
        counts.insert(f.table.clone(), n);
    }
    if total > cap {
        let span = req.to - req.from + 1;
        let narrower = (span as u128 * cap as u128 / total as u128).max(1) as u64;
        bail!(
            "blocks {} to {} hold {} transfer rows, over the flow's cap of {}. Nothing was drawn \
             rather than a part of it. Try a range of about {} blocks.",
            group(req.from),
            group(req.to),
            group(total),
            group(cap),
            group(narrower)
        );
    }
    let mut flow = if req.tx.is_some() {
        Flow::keeping_movements()
    } else {
        Flow::default()
    };
    for f in &classified.flow {
        let second = f.second.as_deref().map(quote).unwrap_or("NULL".into());
        let mut after: Option<(u64, u64)> = None;
        loop {
            let keyset = after
                .map(|(b, l)| {
                    format!(" AND (block_number > {b} OR (block_number = {b} AND log_index > {l}))")
                })
                .unwrap_or_default();
            let sql = format!(
                "SELECT block_number, log_index, block_hash, address, {} AS p0, {second} AS p1, \
                 CAST({} AS VARCHAR) AS p2 FROM {} WHERE {}{keyset} \
                 ORDER BY block_number, log_index LIMIT {PAGE}",
                quote(&f.first),
                quote(&f.amount),
                quote(&f.table),
                filter(req),
            );
            let out = src
                .query(&sql)
                .await
                .with_context(|| format!("reading {}", f.table))?;
            complete(&out, &f.table)?;
            let n = out.rows.len() as u64;
            for row in &out.rows {
                let m = movement(f, row)?;
                after = Some((m.block, m.log_index));
                if f.emitters.is_empty() || f.emitters.contains(&m.token) {
                    flow.add(m);
                } else {
                    *flow.other.entry(f.table.clone()).or_insert(0) += 1;
                }
            }
            if n < PAGE {
                break;
            }
        }
        let want = counts[&f.table];
        let got = flow.table_rows.get(&f.table).copied().unwrap_or(0)
            + flow.other.get(&f.table).copied().unwrap_or(0);
        if got != want {
            bail!(
                "{}: counted {want} rows in range but read {got}; the nest moved under the read, run \
                 it again",
                f.table
            );
        }
    }
    for t in &classified.other {
        let n = count(src, t, req).await?;
        flow.add_other(t, n);
    }
    for f in &classified.flow {
        flow.add_table(f);
    }
    Ok(flow)
}

#[derive(Debug, Clone, Default)]
struct EdgeAcc {
    amount: U512,
    counts: BTreeMap<Reads, u64>,
    first: (u64, u64),
}

/// The fold of every movement read: edges by (token, from, to), sums by (token, address).
#[derive(Debug, Default)]
pub struct Flow {
    edges: HashMap<(String, String, String), EdgeAcc>,
    sent: HashMap<(String, String), U512>,
    received: HashMap<(String, String), U512>,
    token_rows: BTreeMap<String, u64>,
    token_alias: BTreeMap<String, String>,
    table_rows: BTreeMap<String, u64>,
    table_reads: BTreeMap<String, Reads>,
    other: BTreeMap<String, u64>,
    first: Option<(u64, u64, String)>,
    last: Option<(u64, u64, String)>,
    /// Every movement, kept only for one transaction, where each leg matters and the count is small.
    movements: Option<Vec<Movement>>,
}

impl Flow {
    /// A fold that also keeps each movement, for a single transaction.
    pub fn keeping_movements() -> Self {
        Flow {
            movements: Some(Vec::new()),
            ..Default::default()
        }
    }

    pub fn add(&mut self, m: Movement) {
        if let Some(ms) = &mut self.movements {
            ms.push(m.clone());
        }
        *self.table_rows.entry(m.table.clone()).or_insert(0) += 1;
        self.table_reads.insert(m.table.clone(), m.reads);
        *self.token_rows.entry(m.token.clone()).or_insert(0) += 1;
        self.token_alias
            .entry(m.token.clone())
            .or_insert_with(|| m.alias.clone());
        let at = (m.block, m.log_index, m.block_hash.clone());
        if self
            .first
            .as_ref()
            .is_none_or(|f| (f.0, f.1) > (at.0, at.1))
        {
            self.first = Some(at.clone());
        }
        if self.last.as_ref().is_none_or(|l| (l.0, l.1) < (at.0, at.1)) {
            self.last = Some(at);
        }
        let amount = U512::from(m.amount);
        let e = self
            .edges
            .entry((m.token.clone(), m.from.clone(), m.to.clone()))
            .or_insert_with(|| EdgeAcc {
                first: (m.block, m.log_index),
                ..Default::default()
            });
        e.amount += amount;
        *e.counts.entry(m.reads).or_insert(0) += 1;
        e.first = e.first.min((m.block, m.log_index));
        *self
            .sent
            .entry((m.token.clone(), m.from))
            .or_insert(U512::ZERO) += amount;
        *self.received.entry((m.token, m.to)).or_insert(U512::ZERO) += amount;
    }

    /// Each table, or the part of a split table, that the flow counted but did not read.
    pub fn other_counts(&self) -> &BTreeMap<String, u64> {
        &self.other
    }

    /// Record a table's in-range count for a table the flow does not read.
    pub fn add_other(&mut self, table: &str, rows: u64) {
        self.other.insert(table.to_string(), rows);
    }

    /// Mark a flow table as read, so a table with no rows in range is still listed.
    pub fn add_table(&mut self, f: &FlowTable) {
        self.table_rows.entry(f.table.clone()).or_insert(0);
        self.table_reads.insert(f.table.clone(), f.reads);
    }
}

// ---------------------------------------------------------------------------------------------------
// The document: what every format is rendered from, and what `--format json` prints.

#[derive(Debug, Serialize)]
pub struct Document {
    pub kind: &'static str,
    pub nest: NestOut,
    pub request: Request,
    pub coverage: CoverageOut,
    /// Every table, with its rows in range: these sum to `rows_total`.
    pub tables: Vec<TableOut>,
    pub rows_total: u64,
    pub tokens: Vec<TokenOut>,
    pub nodes: Vec<NodeOut>,
    pub edges: Vec<EdgeOut>,
    pub net: Vec<NetOut>,
    /// Each movement in log order, for a single transaction only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transfers: Option<Vec<TransferOut>>,
    /// The tables outside the flow, told by their story lines or grouped.
    pub events: crate::story::Events,
    pub not_seen: &'static str,
    pub provenance: ProvenanceOut,
}

#[derive(Debug, Serialize)]
pub struct NestOut {
    pub name: String,
    pub chain: String,
    pub chain_id: Option<u64>,
    pub data_identity: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CoverageOut {
    pub start_block: Option<u64>,
    pub indexed_head: Option<u64>,
    pub sealed_through: u64,
    #[serde(rename = "final")]
    pub is_final: bool,
}

#[derive(Debug, Serialize)]
pub struct TableOut {
    pub table: String,
    /// `transfer`, `weth9_deposit`, `weth9_withdrawal`, or absent for a table the flow does not read.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reads: Option<Reads>,
    pub rows: u64,
}

#[derive(Debug, Serialize)]
pub struct TokenOut {
    pub address: String,
    pub alias: String,
    pub symbol: Option<String>,
    pub decimals: Option<u8>,
    pub rows: u64,
}

#[derive(Debug, Serialize)]
pub struct NodeOut {
    pub address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct EdgeOut {
    pub n: usize,
    pub token: String,
    pub from: String,
    pub to: String,
    /// Base units, as decimal text: scale by the token's `decimals` to read it.
    pub amount: String,
    pub transfers: u64,
    pub weth9_deposits: u64,
    pub weth9_withdrawals: u64,
    pub first_block: u64,
    pub first_log_index: u64,
}

#[derive(Debug, Serialize)]
pub struct TransferOut {
    pub block: u64,
    pub log_index: u64,
    pub token: String,
    pub from: String,
    pub to: String,
    pub amount: String,
    pub reads: Reads,
}

#[derive(Debug, Serialize)]
pub struct NetOut {
    pub token: String,
    pub address: String,
    pub sent: String,
    pub received: String,
    /// `received - sent`, signed, in base units.
    pub net: String,
}

#[derive(Debug, Serialize)]
pub struct ProvenanceOut {
    pub version: String,
    pub data_identity: Option<String>,
    pub first_row: Option<RowAt>,
    pub last_row: Option<RowAt>,
    pub sealed_through: u64,
    pub metadata_sha256: Option<String>,
    pub label_snapshots: Vec<String>,
    pub semantic_sha256: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct RowAt {
    pub block: u64,
    pub log_index: u64,
    pub block_hash: String,
}

fn signed(received: U512, sent: U512) -> String {
    if received >= sent {
        (received - sent).to_string()
    } else {
        format!("-{}", sent - received)
    }
}

pub fn document(
    flow: &Flow,
    events: crate::story::Events,
    ctx: &NestContext,
    req: &Request,
) -> Document {
    let mut tables: Vec<TableOut> = flow
        .table_rows
        .iter()
        .map(|(t, n)| TableOut {
            table: t.clone(),
            reads: flow.table_reads.get(t).copied(),
            rows: *n,
        })
        .chain(flow.other.iter().map(|(t, n)| TableOut {
            table: t.clone(),
            reads: None,
            rows: *n,
        }))
        .collect();
    tables.sort_by(|a, b| a.table.cmp(&b.table));
    let rows_total = tables.iter().map(|t| t.rows).sum();

    let mut tokens: Vec<TokenOut> = flow
        .token_rows
        .iter()
        .map(|(addr, n)| {
            let meta = ctx.metadata.get(addr);
            TokenOut {
                address: addr.clone(),
                alias: flow.token_alias[addr].clone(),
                symbol: meta.and_then(|m| m.symbol.clone()),
                decimals: meta.and_then(|m| m.decimals),
                rows: *n,
            }
        })
        .collect();
    tokens.sort_by(|a, b| b.rows.cmp(&a.rows).then(a.address.cmp(&b.address)));

    let mut edges: Vec<(&(String, String, String), &EdgeAcc)> = flow.edges.iter().collect();
    edges.sort_by(|a, b| a.1.first.cmp(&b.1.first).then(a.0.cmp(b.0)));
    let edges: Vec<EdgeOut> = edges
        .into_iter()
        .enumerate()
        .map(|(i, ((token, from, to), e))| EdgeOut {
            n: i + 1,
            token: token.clone(),
            from: from.clone(),
            to: to.clone(),
            amount: e.amount.to_string(),
            transfers: e.counts.get(&Reads::Transfer).copied().unwrap_or(0),
            weth9_deposits: e.counts.get(&Reads::Weth9Deposit).copied().unwrap_or(0),
            weth9_withdrawals: e.counts.get(&Reads::Weth9Withdrawal).copied().unwrap_or(0),
            first_block: e.first.0,
            first_log_index: e.first.1,
        })
        .collect();

    let mut nodes: BTreeSet<&str> = BTreeSet::new();
    for e in &edges {
        nodes.insert(&e.from);
        nodes.insert(&e.to);
    }
    let alias_of = |a: &str| {
        ctx.aliases
            .get(a)
            .or_else(|| flow.token_alias.get(a))
            .cloned()
    };
    let nodes = nodes
        .into_iter()
        .map(|a| NodeOut {
            address: a.to_string(),
            alias: alias_of(a),
            labels: ctx.labels.get(a).cloned().unwrap_or_default(),
        })
        .collect();

    let mut keys: BTreeSet<&(String, String)> = flow.sent.keys().collect();
    keys.extend(flow.received.keys());
    let net = keys
        .into_iter()
        .map(|k| {
            let s = flow.sent.get(k).copied().unwrap_or(U512::ZERO);
            let r = flow.received.get(k).copied().unwrap_or(U512::ZERO);
            NetOut {
                token: k.0.clone(),
                address: k.1.clone(),
                sent: s.to_string(),
                received: r.to_string(),
                net: signed(r, s),
            }
        })
        .collect();

    let at = |p: &Option<(u64, u64, String)>| {
        p.as_ref().map(|(b, l, h)| RowAt {
            block: *b,
            log_index: *l,
            block_hash: h.clone(),
        })
    };
    Document {
        kind: "flow",
        nest: NestOut {
            name: ctx.name.clone(),
            chain: ctx.chain.clone(),
            chain_id: ctx.chain_id,
            data_identity: ctx.data_identity.clone(),
        },
        request: req.clone(),
        coverage: CoverageOut {
            start_block: ctx.start_block,
            indexed_head: ctx.indexed_head,
            sealed_through: ctx.sealed_through,
            is_final: req.to <= ctx.sealed_through,
        },
        tables,
        rows_total,
        tokens,
        nodes,
        edges,
        net,
        transfers: flow.movements.as_ref().map(|ms| {
            let mut ms: Vec<&Movement> = ms.iter().collect();
            ms.sort_by_key(|m| (m.block, m.log_index));
            ms.into_iter()
                .map(|m| TransferOut {
                    block: m.block,
                    log_index: m.log_index,
                    token: m.token.clone(),
                    from: m.from.clone(),
                    to: m.to.clone(),
                    amount: m.amount.to_string(),
                    reads: m.reads,
                })
                .collect()
        }),
        events,
        not_seen: NOT_SEEN,
        provenance: ProvenanceOut {
            version: ctx.version.clone(),
            data_identity: ctx.data_identity.clone(),
            first_row: at(&flow.first),
            last_row: at(&flow.last),
            sealed_through: ctx.sealed_through,
            metadata_sha256: ctx.metadata_sha256.clone(),
            label_snapshots: ctx.label_snapshots.clone(),
            semantic_sha256: ctx.semantic_sha256.clone(),
        },
    }
}

// ---------------------------------------------------------------------------------------------------
// Rendering. Every format is a pure function of the document and `top`.

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Format {
    Text,
    Json,
    Dot,
    Mermaid,
}

pub fn render(doc: &Document, format: Format, top: usize) -> String {
    match format {
        Format::Text => text_out(doc, top),
        Format::Json => {
            let mut s = serde_json::to_string_pretty(doc).expect("the document serialises");
            s.push('\n');
            s
        }
        Format::Dot => dot(doc, top),
        Format::Mermaid => mermaid(doc, top),
    }
}

/// `1234567` as `1,234,567`.
pub fn group(n: impl ToString) -> String {
    let s = n.to_string();
    let (sign, digits) = s
        .strip_prefix('-')
        .map(|d| ("-", d))
        .unwrap_or(("", s.as_str()));
    let mut out = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    format!("{sign}{out}")
}

/// Base units moved by `decimals` places, as text: no float, trailing zeros of the fraction trimmed
/// and nothing else.
pub fn scale(base: &str, decimals: u8) -> String {
    let (sign, digits) = base
        .strip_prefix('-')
        .map(|d| ("-", d))
        .unwrap_or(("", base));
    let d = decimals as usize;
    let padded = format!("{digits:0>width$}", width = d + 1);
    let (int, frac) = padded.split_at(padded.len() - d);
    let frac = frac.trim_end_matches('0');
    let int = group(int);
    if frac.is_empty() {
        format!("{sign}{int}")
    } else {
        format!("{sign}{int}.{frac}")
    }
}

struct Names<'a> {
    tokens: BTreeMap<&'a str, &'a TokenOut>,
    nodes: BTreeMap<&'a str, &'a NodeOut>,
}

impl<'a> Names<'a> {
    fn new(doc: &'a Document) -> Self {
        Names {
            tokens: doc.tokens.iter().map(|t| (t.address.as_str(), t)).collect(),
            nodes: doc.nodes.iter().map(|n| (n.address.as_str(), n)).collect(),
        }
    }

    fn token(&self, address: &str) -> String {
        let t = self.tokens[address];
        t.symbol.clone().unwrap_or_else(|| t.alias.clone())
    }

    fn amount(&self, token: &str, base: &str) -> String {
        amount_in(base, self.tokens[token].decimals, &self.token(token))
    }

    fn node(&self, address: &str) -> String {
        let n = self.nodes.get(address);
        address_with(
            address,
            n.and_then(|n| n.alias.as_deref()),
            n.map(|n| n.labels.as_slice()).unwrap_or(&[]),
        )
    }
}

fn events(e: &EdgeOut) -> String {
    let mut parts = Vec::new();
    let plural =
        |n: u64, one: &str, many: &str| format!("{} {}", group(n), if n == 1 { one } else { many });
    if e.transfers > 0 {
        parts.push(plural(e.transfers, "transfer", "transfers"));
    }
    if e.weth9_deposits > 0 {
        parts.push(plural(e.weth9_deposits, "WETH9 Deposit", "WETH9 Deposits"));
    }
    if e.weth9_withdrawals > 0 {
        parts.push(plural(
            e.weth9_withdrawals,
            "WETH9 Withdrawal",
            "WETH9 Withdrawals",
        ));
    }
    parts.join(", ")
}

/// The edges a drawing shows: every token's `top` largest by amount, in first-transfer order, and
/// per token the number left out.
fn drawn(doc: &Document, top: usize) -> (Vec<&EdgeOut>, Vec<(String, usize)>) {
    let mut by_token: BTreeMap<&str, Vec<&EdgeOut>> = BTreeMap::new();
    for e in &doc.edges {
        by_token.entry(&e.token).or_default().push(e);
    }
    let mut keep: BTreeSet<usize> = BTreeSet::new();
    let mut left = Vec::new();
    for t in &doc.tokens {
        let Some(mut es) = by_token.remove(t.address.as_str()) else {
            continue;
        };
        es.sort_by(|a, b| {
            let (x, y) = (
                U512::from_str_radix(&a.amount, 10).unwrap_or_default(),
                U512::from_str_radix(&b.amount, 10).unwrap_or_default(),
            );
            y.cmp(&x).then(a.n.cmp(&b.n))
        });
        if es.len() > top {
            left.push((t.address.clone(), es.len() - top));
        }
        keep.extend(es.iter().take(top).map(|e| e.n));
    }
    let shown = doc.edges.iter().filter(|e| keep.contains(&e.n)).collect();
    (shown, left)
}

fn rows(n: u64) -> String {
    format!("{} {}", group(n), if n == 1 { "row" } else { "rows" })
}

fn heading(doc: &Document) -> String {
    let r = &doc.request;
    let what = match (&r.tx, r.from == r.to) {
        (Some(tx), _) => format!("transaction {tx} in block {}", group(r.from)),
        (None, true) => format!("block {}", group(r.from)),
        (None, false) => format!("blocks {} to {}", group(r.from), group(r.to)),
    };
    let chain = match doc.nest.chain_id {
        Some(id) => format!("{} (chain {id})", doc.nest.chain),
        None => doc.nest.chain.clone(),
    };
    format!("Flow of {what} on {chain}")
}

fn finality(doc: &Document) -> String {
    let r = &doc.request;
    let sealed = doc.coverage.sealed_through;
    if doc.coverage.is_final {
        format!("All final: sealed through {}.", group(sealed))
    } else {
        let from = r.from.max(sealed + 1);
        let span = if from == r.to {
            format!("block {} is", group(from))
        } else {
            format!("blocks {} to {} are", group(from), group(r.to))
        };
        format!(
            "Provisional: {span} not final and may be reorganised. Sealed through {}.",
            group(sealed)
        )
    }
}

fn provenance_lines(doc: &Document) -> Vec<String> {
    let p = &doc.provenance;
    let at = |r: &Option<RowAt>| match r {
        Some(r) => format!(
            "block {} log {} ({})",
            group(r.block),
            r.log_index,
            if r.block_hash.is_empty() {
                "hash not stored"
            } else {
                &r.block_hash
            }
        ),
        None => "none".to_string(),
    };
    vec![
        format!(
            "nuthatch {}; data identity {}; sealed through {}.",
            p.version,
            p.data_identity.as_deref().unwrap_or("unknown"),
            group(p.sealed_through)
        ),
        format!(
            "First row read: {}. Last: {}.",
            at(&p.first_row),
            at(&p.last_row)
        ),
        format!(
            "metadata.json: {}. Label snapshots: {}.",
            p.metadata_sha256
                .as_deref()
                .map(|h| format!("sha256 {h}"))
                .unwrap_or_else(|| "absent".into()),
            if p.label_snapshots.is_empty() {
                "none".to_string()
            } else {
                p.label_snapshots.join(", ")
            }
        ),
        format!(
            "semantic.toml: {}.",
            p.semantic_sha256
                .as_deref()
                .map(|h| format!("sha256 {h}"))
                .unwrap_or_else(|| "absent".into()),
        ),
    ]
}

fn text_out(doc: &Document, top: usize) -> String {
    let names = Names::new(doc);
    let mut o = String::new();
    let _ = writeln!(o, "{}.", heading(doc));
    let flow_rows: u64 = doc
        .tables
        .iter()
        .filter(|t| t.reads.is_some())
        .map(|t| t.rows)
        .sum();
    let flow_tables = doc.tables.iter().filter(|t| t.reads.is_some()).count();
    let _ = writeln!(
        o,
        "Nest {}, data identity {}. {} in range: {} read as flow, {} of other events.",
        doc.nest.name,
        doc.nest.data_identity.as_deref().unwrap_or("unknown"),
        rows(doc.rows_total),
        group(flow_rows),
        group(doc.rows_total - flow_rows)
    );
    let _ = writeln!(o, "{}", finality(doc));
    o.push('\n');

    if flow_tables == 0 {
        let _ = writeln!(
            o,
            "This nest indexes no ERC-20 Transfer and no WETH9 Deposit or Withdrawal, so it has no \
             flow to draw."
        );
    } else if doc.edges.is_empty() {
        let _ = writeln!(o, "No transfers in range.");
    } else {
        let _ = writeln!(o, "Tokens");
        for t in &doc.tokens {
            let d = match t.decimals {
                Some(d) => format!("{d} decimals"),
                None => "decimals unknown: run `nuthatch metadata fetch`".to_string(),
            };
            let _ = writeln!(
                o,
                "  {} {} ({d}), {}",
                names.token(&t.address),
                t.address,
                rows(t.rows)
            );
        }
        o.push('\n');
        if let Some(ts) = &doc.transfers {
            let _ = writeln!(o, "Transfers, in log order");
            for t in ts {
                let kind = match t.reads {
                    Reads::Transfer => "",
                    Reads::Weth9Deposit => " (WETH9 Deposit)",
                    Reads::Weth9Withdrawal => " (WETH9 Withdrawal)",
                };
                let _ = writeln!(
                    o,
                    "  log {}: {} -> {}: {}{kind}",
                    t.log_index,
                    names.node(&t.from),
                    names.node(&t.to),
                    names.amount(&t.token, &t.amount)
                );
            }
            o.push('\n');
        }
        let (shown, left) = drawn(doc, top);
        let _ = writeln!(o, "Edges, in order of first transfer");
        for e in shown {
            let _ = writeln!(
                o,
                "  {}. {} -> {}: {} ({}, first at block {} log {})",
                e.n,
                names.node(&e.from),
                names.node(&e.to),
                names.amount(&e.token, &e.amount),
                events(e),
                group(e.first_block),
                e.first_log_index
            );
        }
        for (token, n) in &left {
            let _ = writeln!(
                o,
                "  {n} smaller edges of {} not shown (all are in --format json)",
                names.token(token)
            );
        }
        o.push('\n');
        let _ = writeln!(
            o,
            "Net flow of the transfers seen, per token (not a balance)"
        );
        for t in &doc.tokens {
            let mut rows: Vec<(&NetOut, U512, bool)> = doc
                .net
                .iter()
                .filter(|n| n.token == t.address && n.net != "0")
                .map(|n| {
                    let neg = n.net.starts_with('-');
                    let mag =
                        U512::from_str_radix(n.net.trim_start_matches('-'), 10).unwrap_or_default();
                    (n, mag, neg)
                })
                .collect();
            let zero = doc
                .net
                .iter()
                .filter(|n| n.token == t.address && n.net == "0")
                .count();
            rows.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.address.cmp(&b.0.address)));
            let _ = writeln!(o, "  {}", names.token(&t.address));
            for (n, _, neg) in rows.iter().take(top) {
                let sign = if *neg { "" } else { "+" };
                let _ = writeln!(
                    o,
                    "    {}  {sign}{}",
                    names.node(&n.address),
                    names.amount(&t.address, &n.net)
                );
            }
            if rows.len() > top {
                let _ = writeln!(o, "    {} more addresses not shown", rows.len() - top);
            }
            if zero > 0 {
                let _ = writeln!(
                    o,
                    "    {zero} {} netted to exactly zero",
                    if zero == 1 { "address" } else { "addresses" }
                );
            }
        }
    }
    o.push('\n');
    o.push_str(&crate::story::text(&doc.events, top));
    let _ = writeln!(o, "{}", doc.not_seen);
    o.push('\n');
    for l in provenance_lines(doc) {
        let _ = writeln!(o, "{l}");
    }
    o
}

fn node_id(address: &str) -> String {
    format!("a{}", address.trim_start_matches("0x"))
}

fn dot_escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

fn drawing_label(names: &Names, address: &str) -> Vec<String> {
    if address == ZERO_ADDRESS {
        return vec!["zero address".into(), address.into()];
    }
    let mut l = vec![address.to_string()];
    if let Some(n) = names.nodes.get(address) {
        if let Some(a) = &n.alias {
            l.push(format!("[{a}]"));
        }
        for x in &n.labels {
            l.push(format!("label {x}"));
        }
    }
    l
}

fn used_nodes<'a>(shown: &[&'a EdgeOut]) -> BTreeSet<&'a str> {
    let mut s = BTreeSet::new();
    for e in shown {
        s.insert(e.from.as_str());
        s.insert(e.to.as_str());
    }
    s
}

fn edge_label(names: &Names, e: &EdgeOut) -> String {
    let mut l = format!("{}. {}", e.n, names.amount(&e.token, &e.amount));
    if e.weth9_deposits + e.weth9_withdrawals > 0 {
        let _ = write!(l, " ({})", events(e));
    }
    l
}

fn dot(doc: &Document, top: usize) -> String {
    let names = Names::new(doc);
    let (shown, left) = drawn(doc, top);
    let mut o = String::new();
    let _ = writeln!(o, "// {}.", heading(doc));
    let _ = writeln!(o, "// {}", finality(doc));
    for l in provenance_lines(doc) {
        let _ = writeln!(o, "// {l}");
    }
    let _ = writeln!(o, "digraph flow {{");
    let _ = writeln!(o, "  rankdir=LR;");
    let _ = writeln!(o, "  node [shape=box, fontname=\"monospace\"];");
    for a in used_nodes(&shown) {
        let label = drawing_label(&names, a)
            .iter()
            .map(|s| dot_escape(s))
            .collect::<Vec<_>>()
            .join("\\n");
        let _ = writeln!(o, "  {} [label=\"{label}\"];", node_id(a));
    }
    for e in &shown {
        let _ = writeln!(
            o,
            "  {} -> {} [label=\"{}\"];",
            node_id(&e.from),
            node_id(&e.to),
            dot_escape(&edge_label(&names, e))
        );
    }
    for (token, n) in &left {
        let _ = writeln!(
            o,
            "  // {n} smaller edges of {} not drawn (all are in --format json)",
            names.token(token)
        );
    }
    let _ = writeln!(o, "}}");
    let _ = writeln!(o, "// {}", doc.not_seen);
    o
}

fn mermaid_escape(s: &str) -> String {
    s.replace('"', "#quot;")
}

fn mermaid(doc: &Document, top: usize) -> String {
    let names = Names::new(doc);
    let (shown, left) = drawn(doc, top);
    let mut o = String::new();
    let _ = writeln!(o, "%% {}.", heading(doc));
    let _ = writeln!(o, "%% {}", finality(doc));
    for l in provenance_lines(doc) {
        let _ = writeln!(o, "%% {l}");
    }
    let _ = writeln!(o, "flowchart LR");
    for a in used_nodes(&shown) {
        let label = drawing_label(&names, a)
            .iter()
            .map(|s| mermaid_escape(s))
            .collect::<Vec<_>>()
            .join("<br/>");
        let _ = writeln!(o, "  {}[\"{label}\"]", node_id(a));
    }
    for e in &shown {
        let _ = writeln!(
            o,
            "  {} -->|\"{}\"| {}",
            node_id(&e.from),
            mermaid_escape(&edge_label(&names, e)),
            node_id(&e.to)
        );
    }
    for (token, n) in &left {
        let _ = writeln!(
            o,
            "  %% {n} smaller edges of {} not drawn (all are in --format json)",
            names.token(token)
        );
    }
    let _ = writeln!(o, "%% {}", doc.not_seen);
    o
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, ty: &str, indexed: bool) -> ColumnSchema {
        ColumnSchema {
            name: name.into(),
            sol_type: ty.into(),
            storage: "string".into(),
            indexed,
            components: Vec::new(),
        }
    }

    fn table(name: &str, alias: &str, signature: &str, cols: Vec<ColumnSchema>) -> TableSchema {
        let mut columns = crate::registry::implicit_columns(true);
        columns.extend(cols);
        TableSchema {
            table: name.into(),
            alias: alias.into(),
            kind: TableKind::Event,
            event: signature.into(),
            topic0: topic(signature),
            function: String::new(),
            selector: String::new(),
            columns,
        }
    }

    fn weth9() -> Vec<TableSchema> {
        vec![
            table(
                "weth__transfer",
                "weth",
                "Transfer(address,address,uint256)",
                vec![
                    col("src", "address", true),
                    col("dst", "address", true),
                    col("wad", "uint256", false),
                ],
            ),
            table(
                "weth__deposit",
                "weth",
                "Deposit(address,uint256)",
                vec![col("dst", "address", true), col("wad", "uint256", false)],
            ),
            table(
                "weth__withdrawal",
                "weth",
                "Withdrawal(address,uint256)",
                vec![col("src", "address", true), col("wad", "uint256", false)],
            ),
        ]
    }

    #[test]
    fn weth9_binds_by_position_and_reads_deposit_and_withdrawal() {
        let c = classify(&weth9(), Some(1));
        assert!(c.other.is_empty(), "{:?}", c.other);
        let by: BTreeMap<&str, &FlowTable> = c.flow.iter().map(|f| (f.table.as_str(), f)).collect();
        let t = by["weth__transfer"];
        assert_eq!(
            (
                t.reads,
                t.first.as_str(),
                t.second.as_deref(),
                t.amount.as_str()
            ),
            (Reads::Transfer, "src", Some("dst"), "wad")
        );
        let d = by["weth__deposit"];
        assert_eq!(
            (d.reads, d.first.as_str(), d.amount.as_str()),
            (Reads::Weth9Deposit, "dst", "wad")
        );
        let w = by["weth__withdrawal"];
        assert_eq!(
            (w.reads, w.first.as_str(), w.amount.as_str()),
            (Reads::Weth9Withdrawal, "src", "wad")
        );
    }

    /// Names that lie: the sender's column is called `to`. Position decides.
    #[test]
    fn a_name_never_decides_which_parameter_is_the_sender() {
        let t = table(
            "odd__transfer",
            "odd",
            "Transfer(address,address,uint256)",
            vec![
                col("to", "address", true),
                col("from", "address", true),
                col("value", "uint256", false),
            ],
        );
        let c = classify(&[t], Some(1));
        assert_eq!(c.flow[0].first, "to");
        assert_eq!(c.flow[0].second.as_deref(), Some("from"));
    }

    #[test]
    fn shapes_outside_the_flow_are_counted_not_drawn() {
        let nft = table(
            "nft__transfer",
            "nft",
            "Transfer(address,address,uint256)",
            vec![
                col("from", "address", true),
                col("to", "address", true),
                col("tokenId", "uint256", true),
            ],
        );
        // Arbitrum lists no wrapper (its WETH wraps with `Transfer`), so no `Deposit` is flow there.
        let pool = table(
            "pool__deposit",
            "pool",
            "Deposit(address,uint256)",
            vec![
                col("user", "address", true),
                col("amount", "uint256", false),
            ],
        );
        let c = classify(&[nft, pool], Some(42161));
        assert!(c.flow.is_empty(), "{:?}", c.flow);
        assert_eq!(c.other, vec!["nft__transfer", "pool__deposit"]);
    }

    const WETH: &str = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2";
    const A: &str = "0x00000000000000000000000000000000000000aa";
    const B: &str = "0x00000000000000000000000000000000000000bb";

    fn m(log: u64, reads: Reads, from: &str, to: &str, amount: u64) -> Movement {
        Movement {
            block: 1,
            log_index: log,
            block_hash: "0xh".into(),
            table: "weth__transfer".into(),
            token: WETH.into(),
            alias: "weth".into(),
            from: from.into(),
            to: to.into(),
            amount: U256::from(amount),
            reads,
        }
    }

    fn ctx() -> NestContext {
        let mut metadata = crate::metadata::MetadataCache::new();
        metadata.insert(
            WETH.into(),
            crate::metadata::TokenMetadata {
                decimals: Some(18),
                symbol: Some("WETH".into()),
                name: None,
            },
        );
        NestContext {
            name: "t".into(),
            chain: "mainnet".into(),
            chain_id: Some(1),
            start_block: Some(1),
            indexed_head: Some(1),
            sealed_through: 1,
            metadata,
            version: "test".into(),
            ..Default::default()
        }
    }

    fn one_block() -> Request {
        Request {
            from: 1,
            to: 1,
            tx: None,
        }
    }

    /// Wrap, pass on, unwrap: a WETH9 round trip leaves nobody holding WETH it no longer has.
    #[test]
    fn a_wrap_and_an_unwrap_net_to_zero() {
        let row = |log: u64, reads, first: &str, second: Option<&str>| {
            let f = FlowTable {
                table: "weth__x".into(),
                alias: "weth".into(),
                reads,
                first: String::new(),
                second: second.map(|_| String::new()),
                amount: String::new(),
                emitters: Vec::new(),
            };
            let v = serde_json::json!({
                "block_number": 1, "log_index": log, "block_hash": "0xh",
                "address": "0xC02aaa39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                "p0": first, "p1": second, "p2": "5000000000000000000",
            });
            movement(&f, &v).unwrap()
        };
        let mut flow = Flow::default();
        flow.add(row(0, Reads::Weth9Deposit, A, None));
        flow.add(row(1, Reads::Transfer, A, Some(B)));
        flow.add(row(2, Reads::Weth9Withdrawal, B, None));
        let doc = document(&flow, Default::default(), &ctx(), &one_block());
        let net: BTreeMap<&str, &str> = doc
            .net
            .iter()
            .map(|n| (n.address.as_str(), n.net.as_str()))
            .collect();
        assert_eq!(net[A], "0");
        assert_eq!(net[B], "0");
        assert_eq!(net[ZERO_ADDRESS], "0");
        let first = &doc.edges[0];
        assert_eq!((first.from.as_str(), first.to.as_str()), (ZERO_ADDRESS, A));
        assert_eq!(first.weth9_deposits, 1);
        let last = &doc.edges[2];
        assert_eq!((last.from.as_str(), last.to.as_str()), (B, ZERO_ADDRESS));
        assert_eq!(last.weth9_withdrawals, 1);
    }

    /// §14: a transfer to the zero address is said as one. Nothing is called burned or minted.
    #[test]
    fn the_zero_address_is_named_and_nothing_is_called_burned() {
        let mut flow = Flow::default();
        flow.add(m(0, Reads::Transfer, ZERO_ADDRESS, A, 7));
        flow.add(m(1, Reads::Transfer, A, ZERO_ADDRESS, 3));
        let doc = document(&flow, Default::default(), &ctx(), &one_block());
        for format in [Format::Text, Format::Dot, Format::Mermaid] {
            let out = render(&doc, format, DEFAULT_TOP).to_lowercase();
            assert!(out.contains("zero address"), "{format:?}: {out}");
            for word in ["burn", "mint", "destroy"] {
                assert!(!out.contains(word), "{format:?} says {word}: {out}");
            }
        }
        let text = render(&doc, Format::Text, DEFAULT_TOP);
        let want = format!("{A} -> {ZERO_ADDRESS} (zero address): 0.000000000000000003 WETH");
        assert!(text.contains(&want), "{text}");
    }

    #[test]
    fn amounts_move_the_point_and_trim_only_trailing_zeros() {
        assert_eq!(scale("1250000000001", 6), "1,250,000.000001");
        assert_eq!(scale("30000000000000000000000000", 18), "30,000,000");
        assert_eq!(scale("2", 18), "0.000000000000000002");
        assert_eq!(scale("-100", 2), "-1");
        assert_eq!(scale("0", 18), "0");
        assert_eq!(scale("1000", 0), "1,000");
        // uint256's maximum, which no float could carry.
        assert_eq!(
            scale(&U256::MAX.to_string(), 18),
            "115,792,089,237,316,195,423,570,985,008,687,907,853,269,984,665,640,564,039,457.\
             584007913129639935"
        );
    }

    /// Answers `count(*)` with a fixed number and anything else with no rows.
    struct Counted(u64);

    impl Source for Counted {
        async fn query(&self, sql: &str) -> Result<QueryOutput> {
            let rows = if sql.starts_with("SELECT count(*)") {
                vec![serde_json::json!({ "n": self.0 })]
            } else {
                Vec::new()
            };
            Ok(QueryOutput {
                rows,
                columns: Vec::new(),
                truncated: false,
                degraded_tables: Default::default(),
                tip_unavailable: false,
                referenced_tables: None,
                offchain: None,
                scan_bound: None,
            })
        }
    }

    #[tokio::test]
    async fn a_range_over_the_cap_is_refused_before_a_row_is_read() {
        let c = classify(&weth9(), Some(1));
        let req = Request {
            from: 100,
            to: 199,
            tx: None,
        };
        // Three flow tables, so 3 x 400 rows against a cap of 1,000.
        let msg = format!(
            "{:#}",
            read(&Counted(400), &c, &req, 1_000).await.unwrap_err()
        );
        assert!(
            msg.contains("hold 1,200 transfer rows, over the flow's cap of 1,000"),
            "{msg}"
        );
        assert!(msg.contains("Try a range of about 83 blocks"), "{msg}");
        // Under the cap it reads; these empty pages then disagree with the count, which is refused.
        let msg = format!(
            "{:#}",
            read(&Counted(300), &c, &req, 1_000).await.unwrap_err()
        );
        assert!(
            msg.contains("counted 300 rows in range but read 0"),
            "{msg}"
        );
        assert!(read(&Counted(0), &c, &req, 1_000).await.is_ok());
    }

    /// Serves two `Deposit` rows from one table: one emitted by WETH, one by a vault that shares the
    /// alias, as factory children do. Every other table is empty.
    struct SharedAlias;

    const VAULT: &str = "0x00000000000000000000000000000000000000cc";

    impl Source for SharedAlias {
        async fn query(&self, sql: &str) -> Result<QueryOutput> {
            let ours = sql.contains("\"weth__deposit\"");
            let row = |emitter: &str, log: u64| {
                serde_json::json!({
                    "block_number": 1, "log_index": log, "block_hash": "0xh",
                    "address": emitter, "p0": A, "p1": null, "p2": "7",
                })
            };
            let rows = match (sql.starts_with("SELECT count(*)"), ours) {
                (true, true) => vec![serde_json::json!({ "n": 2 })],
                (true, false) => vec![serde_json::json!({ "n": 0 })],
                (false, true) => vec![row(WETH, 0), row(VAULT, 1)],
                (false, false) => Vec::new(),
            };
            Ok(QueryOutput {
                rows,
                columns: Vec::new(),
                truncated: false,
                degraded_tables: Default::default(),
                tip_unavailable: false,
                referenced_tables: None,
                offchain: None,
                scan_bound: None,
            })
        }
    }

    /// WETH9 is a property of a contract, not of a signature or an alias: the vault's `Deposit` is
    /// counted, not drawn, while WETH's in the same table is a transfer from the zero address.
    #[tokio::test]
    async fn only_a_listed_wrapper_s_deposit_is_flow() {
        let c = classify(&weth9(), Some(1));
        let flow = read(&SharedAlias, &c, &one_block(), 1_000).await.unwrap();
        let doc = document(&flow, Default::default(), &ctx(), &one_block());
        assert_eq!(doc.edges.len(), 1, "{:?}", doc.edges);
        let e = &doc.edges[0];
        assert_eq!(
            (e.token.as_str(), e.from.as_str(), e.to.as_str()),
            (WETH, ZERO_ADDRESS, A)
        );
        assert!(doc.net.iter().all(|n| n.token == WETH));
        let deposit: Vec<(Option<Reads>, u64)> = doc
            .tables
            .iter()
            .filter(|t| t.table == "weth__deposit")
            .map(|t| (t.reads, t.rows))
            .collect();
        assert_eq!(deposit, vec![(Some(Reads::Weth9Deposit), 1), (None, 1)]);
        assert_eq!(doc.rows_total, 2);
    }

    #[test]
    fn coverage_is_refused_on_either_side() {
        let mut ctx = ctx();
        ctx.start_block = Some(100);
        ctx.indexed_head = Some(200);
        let req = |from, to| Request { from, to, tx: None };
        assert!(check_coverage(&req(100, 200), &ctx).is_ok());
        for (f, t) in [(99, 150), (150, 201), (201, 300)] {
            let e = check_coverage(&req(f, t), &ctx).unwrap_err().to_string();
            assert!(e.contains("which is blocks 100 to 200"), "{e}");
        }
        ctx.indexed_head = None;
        assert!(check_coverage(&req(100, 100), &ctx).is_err());
    }
}
