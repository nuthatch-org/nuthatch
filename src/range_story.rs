//! The range story (RFC-0071 §4, built as §15 step 3): what happened in a block range, a block or a
//! transaction, in a fixed order of sections told by fixed templates. The control plane comes first
//! and is never capped; then movements per token, new holders, approvals, flags and labels, and every
//! other table through [`crate::story::read`]. Nothing here stores anything, and no model writes a word.
//!
//! The catalogue binds parameters by position, type and indexedness, as the flow does: a table is a
//! standard event only if its topic0 and its parameter shape both say so.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use alloy_primitives::{keccak256, U256, U512};
use anyhow::{bail, Context, Result};
use serde::Serialize;
use serde_json::Value;

use crate::diff::{
    self, group, quote, Ask, Classified, CoverageOut, Flow, Format, Inputs, NestContext, NestOut,
    ProvenanceOut, Reads, Request, RowAt, Source, ZERO_ADDRESS,
};
use crate::registry::{TableKind, TableSchema};

/// Items per list (transfers, addresses, approvals, sentences) when `--top` is not given (§4's K).
pub const DEFAULT_TOP: usize = 10;

/// The most transfer rows before the range that the new-holders section will read to tell a new
/// holder from an old one (§6). Above it the section is omitted, with the count and this budget.
pub const NEW_HOLDERS_BUDGET: u64 = 20_000_000;

/// Rows fetched per query; `/sql` answers at most 50,000.
const PAGE: u64 = 50_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    OwnershipTransferred,
    Paused,
    Unpaused,
    Upgraded,
    AdminChanged,
    BeaconUpgraded,
    RoleGranted,
    RoleRevoked,
    RoleAdminChanged,
    Approval,
    ApprovalForAll,
    Erc721Approval,
    Erc721Transfer,
    Erc1155TransferSingle,
    Erc1155TransferBatch,
}

impl Kind {
    fn is_control(self) -> bool {
        matches!(
            self,
            Kind::OwnershipTransferred
                | Kind::Paused
                | Kind::Unpaused
                | Kind::Upgraded
                | Kind::AdminChanged
                | Kind::BeaconUpgraded
                | Kind::RoleGranted
                | Kind::RoleRevoked
                | Kind::RoleAdminChanged
        )
    }

    fn section(self) -> Section {
        match self {
            k if k.is_control() => Section::Control,
            Kind::Erc721Transfer | Kind::Erc1155TransferSingle | Kind::Erc1155TransferBatch => {
                Section::Movements
            }
            _ => Section::Approvals,
        }
    }

    /// Read in full, so counted against the row cap. The rest are told by their first `top` rows.
    fn read_whole(self) -> bool {
        self.is_control() || self == Kind::Approval
    }

    fn event_name(self) -> &'static str {
        match self {
            Kind::OwnershipTransferred => "OwnershipTransferred",
            Kind::Paused => "Paused",
            Kind::Unpaused => "Unpaused",
            Kind::Upgraded => "Upgraded",
            Kind::AdminChanged => "AdminChanged",
            Kind::BeaconUpgraded => "BeaconUpgraded",
            Kind::RoleGranted => "RoleGranted",
            Kind::RoleRevoked => "RoleRevoked",
            Kind::RoleAdminChanged => "RoleAdminChanged",
            Kind::Approval => "Approval (ERC-20)",
            Kind::ApprovalForAll => "ApprovalForAll",
            Kind::Erc721Approval => "Approval (ERC-721)",
            Kind::Erc721Transfer => "Transfer (ERC-721)",
            Kind::Erc1155TransferSingle => "TransferSingle (ERC-1155)",
            Kind::Erc1155TransferBatch => "TransferBatch (ERC-1155)",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Section {
    Movements,
    Control,
    Approvals,
    Events,
}

const A: (&str, bool) = ("address", true);
const U: (&str, bool) = ("address", false);

/// §4's fixed catalogue, beside the flow's `Transfer` and WETH9 shapes in [`diff::classify`].
/// Each parameter's Solidity type and whether it is indexed, in declared order.
type Shape = &'static [(&'static str, bool)];

const CATALOGUE: &[(&str, Shape, Kind)] = &[
    (
        "OwnershipTransferred(address,address)",
        &[A, A],
        Kind::OwnershipTransferred,
    ),
    ("Paused(address)", &[U], Kind::Paused),
    ("Unpaused(address)", &[U], Kind::Unpaused),
    ("Upgraded(address)", &[A], Kind::Upgraded),
    ("AdminChanged(address,address)", &[U, U], Kind::AdminChanged),
    ("BeaconUpgraded(address)", &[A], Kind::BeaconUpgraded),
    (
        "RoleGranted(bytes32,address,address)",
        &[("bytes32", true), A, A],
        Kind::RoleGranted,
    ),
    (
        "RoleRevoked(bytes32,address,address)",
        &[("bytes32", true), A, A],
        Kind::RoleRevoked,
    ),
    (
        "RoleAdminChanged(bytes32,bytes32,bytes32)",
        &[("bytes32", true), ("bytes32", true), ("bytes32", true)],
        Kind::RoleAdminChanged,
    ),
    (
        "Approval(address,address,uint256)",
        &[A, A, ("uint256", false)],
        Kind::Approval,
    ),
    (
        "Approval(address,address,uint256)",
        &[A, A, ("uint256", true)],
        Kind::Erc721Approval,
    ),
    (
        "ApprovalForAll(address,address,bool)",
        &[A, A, ("bool", false)],
        Kind::ApprovalForAll,
    ),
    (
        "Transfer(address,address,uint256)",
        &[A, A, ("uint256", true)],
        Kind::Erc721Transfer,
    ),
    (
        "TransferSingle(address,address,address,uint256,uint256)",
        &[A, A, A, ("uint256", false), ("uint256", false)],
        Kind::Erc1155TransferSingle,
    ),
    (
        "TransferBatch(address,address,address,uint256[],uint256[])",
        &[A, A, A, ("uint256[]", false), ("uint256[]", false)],
        Kind::Erc1155TransferBatch,
    ),
];

/// A table the catalogue tells, with its parameters bound by position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatTable {
    pub table: String,
    pub alias: String,
    pub kind: Kind,
    pub cols: Vec<String>,
}

/// The catalogue tables among `tables`, leaving out any the flow reads.
pub fn catalogue(tables: &[TableSchema], classified: &Classified) -> Vec<CatTable> {
    let flow: BTreeSet<&str> = classified.flow.iter().map(|f| f.table.as_str()).collect();
    let mut out = Vec::new();
    for t in tables {
        if t.kind != TableKind::Event || flow.contains(t.table.as_str()) {
            continue;
        }
        let topic0 = t.topic0.to_ascii_lowercase();
        for (signature, want, kind) in CATALOGUE {
            if diff::topic(signature) != topic0 {
                continue;
            }
            if let Some(cols) = diff::shape(t, want) {
                out.push(CatTable {
                    table: t.table.clone(),
                    alias: t.alias.clone(),
                    kind: *kind,
                    cols,
                });
                break;
            }
        }
    }
    out.sort_by(|a, b| a.table.cmp(&b.table));
    out
}

/// One catalogue row: its position, and its parameters as stored text, in declared order.
#[derive(Debug, Clone)]
struct Row {
    block: u64,
    log: u64,
    tx: String,
    hash: String,
    address: String,
    p: Vec<Option<String>>,
}

impl Row {
    fn at(&self, i: usize) -> Option<&str> {
        self.p.get(i).and_then(|v| v.as_deref())
    }
}

fn row_of(v: &Value, n: usize) -> Result<Row> {
    let s = |k: &str| match v.get(k) {
        Some(Value::String(s)) => Some(s.clone()),
        Some(Value::Null) | None => None,
        Some(other) => Some(other.to_string()),
    };
    Ok(Row {
        block: diff::number(v.get("block_number")).context("a row with no block_number")?,
        log: diff::number(v.get("log_index")).context("a row with no log_index")?,
        tx: s("tx_hash").unwrap_or_default().to_ascii_lowercase(),
        hash: s("block_hash").unwrap_or_default(),
        address: s("address")
            .context("a row with no address")?
            .to_ascii_lowercase(),
        p: (0..n).map(|i| s(&format!("p{i}"))).collect(),
    })
}

/// A table's rows in range in chain order: all of them, or the first `limit`.
async fn read_rows<S: Source>(
    src: &S,
    t: &CatTable,
    req: &Request,
    limit: Option<u64>,
) -> Result<Vec<Row>> {
    let cols: Vec<String> = t
        .cols
        .iter()
        .enumerate()
        .map(|(i, c)| format!("CAST({} AS VARCHAR) AS p{i}", quote(c)))
        .collect();
    let page = limit.unwrap_or(PAGE).min(PAGE);
    let mut rows = Vec::new();
    let mut after: Option<(u64, u64)> = None;
    loop {
        let keyset = after
            .map(|(b, l)| {
                format!(" AND (block_number > {b} OR (block_number = {b} AND log_index > {l}))")
            })
            .unwrap_or_default();
        let sql = format!(
            "SELECT block_number, log_index, tx_hash, block_hash, address, {} FROM {} WHERE {}{keyset} \
             ORDER BY block_number, log_index LIMIT {page}",
            cols.join(", "),
            quote(&t.table),
            diff::filter(req),
        );
        let out = src
            .query(&sql)
            .await
            .with_context(|| format!("reading {}", t.table))?;
        diff::complete(&out, &t.table)?;
        let n = out.rows.len() as u64;
        for v in &out.rows {
            let r = row_of(v, t.cols.len())?;
            after = Some((r.block, r.log));
            rows.push(r);
        }
        if limit.is_some() || n < page {
            return Ok(rows);
        }
    }
}

fn lower(v: Option<&str>) -> Option<String> {
    v.map(str::to_ascii_lowercase)
}

fn addr(ctx: &NestContext, v: Option<&str>) -> String {
    match lower(v) {
        Some(a) => ctx.address_text(&a),
        None => "(none)".into(),
    }
}

/// Role names AccessControl contracts commonly use, recognised by their hash; anything else is
/// printed as the hash it is.
const ROLES: &[&str] = &[
    "ADMIN_ROLE",
    "BURNER_ROLE",
    "GOVERNOR_ROLE",
    "MANAGER_ROLE",
    "MINTER_ROLE",
    "OPERATOR_ROLE",
    "PAUSER_ROLE",
    "SNAPSHOT_ROLE",
    "UPGRADER_ROLE",
];

pub fn role_text(hash: Option<&str>) -> String {
    let Some(h) = lower(hash) else {
        return "role (none)".into();
    };
    if h.trim_start_matches("0x").bytes().all(|b| b == b'0') {
        return format!("role DEFAULT_ADMIN_ROLE ({h})");
    }
    for name in ROLES {
        if format!("0x{}", hex::encode(keccak256(name.as_bytes()))) == h {
            return format!("role {name} ({h})");
        }
    }
    format!("role {h}")
}

fn control_text(kind: Kind, r: &Row, ctx: &NestContext) -> String {
    let c = ctx.address_text(&r.address);
    let a = |i| addr(ctx, r.at(i));
    let what = match kind {
        Kind::OwnershipTransferred => format!("owner changed from {} to {}", a(0), a(1)),
        Kind::Paused => format!("paused by {}", a(0)),
        Kind::Unpaused => format!("unpaused by {}", a(0)),
        Kind::Upgraded => format!("implementation upgraded to {}", a(0)),
        Kind::AdminChanged => format!("proxy admin changed from {} to {}", a(0), a(1)),
        Kind::BeaconUpgraded => format!("beacon upgraded to {}", a(0)),
        Kind::RoleGranted => format!("{} granted to {} by {}", role_text(r.at(0)), a(1), a(2)),
        Kind::RoleRevoked => format!("{} revoked from {} by {}", role_text(r.at(0)), a(1), a(2)),
        Kind::RoleAdminChanged => format!(
            "admin of {} changed from {} to {}",
            role_text(r.at(0)),
            role_text(r.at(1)),
            role_text(r.at(2))
        ),
        _ => unreachable!("not a control-plane kind"),
    };
    format!("{} at {c}: {what}", kind.event_name())
}

fn quoted(v: Option<&str>) -> String {
    v.map(|s| s.escape_debug().to_string())
        .unwrap_or_else(|| "(none)".into())
}

/// The sentence for a row the catalogue tells by its first rows: NFT movements and NFT approvals.
fn first_rows_text(kind: Kind, r: &Row, ctx: &NestContext, alias: &str) -> String {
    let a = |i| addr(ctx, r.at(i));
    match kind {
        Kind::Erc721Transfer => format!("{} -> {}: {alias} token {}", a(0), a(1), quoted(r.at(2))),
        Kind::Erc1155TransferSingle => format!(
            "{} -> {}: {} of {alias} id {} (operator {})",
            a(1),
            a(2),
            quoted(r.at(4)),
            quoted(r.at(3)),
            a(0)
        ),
        Kind::Erc1155TransferBatch => format!(
            "{} -> {}: {alias} ids {} with values {} (operator {})",
            a(1),
            a(2),
            quoted(r.at(3)),
            quoted(r.at(4)),
            a(0)
        ),
        Kind::Erc721Approval => format!(
            "{} approved {} to move {alias} token {}",
            a(0),
            a(1),
            quoted(r.at(2))
        ),
        Kind::ApprovalForAll => {
            if r.at(2).is_some_and(|v| v.eq_ignore_ascii_case("true")) {
                format!("{} approved {} to move all its {alias} tokens", a(0), a(1))
            } else {
                format!(
                    "{} withdrew {}'s approval to move all its {alias} tokens",
                    a(0),
                    a(1)
                )
            }
        }
        _ => unreachable!("told in its own section"),
    }
}

/// §4: an approval of 2^256-1 is named as unlimited, never printed as a 78-digit amount.
pub fn approval_text(
    ctx: &NestContext,
    token: &str,
    owner: &str,
    spender: &str,
    amount: U256,
) -> String {
    let (o, s) = (ctx.address_text(owner), ctx.address_text(spender));
    if amount == U256::MAX {
        let name = token_name(ctx, token);
        format!("{o} approved {s} to spend any amount of {name} (unlimited: 2^256-1)")
    } else if amount.is_zero() {
        format!("{o} set {s}'s allowance to zero")
    } else {
        format!(
            "{o} approved {s} to spend {}",
            ctx.amount_text(token, &amount.to_string())
        )
    }
}

fn token_name(ctx: &NestContext, token: &str) -> String {
    ctx.metadata
        .get(token)
        .and_then(|m| m.symbol.clone())
        .or_else(|| ctx.aliases.get(token).cloned())
        .unwrap_or_else(|| token.to_string())
}

// ---------------------------------------------------------------------------------------------------
// The document.

#[derive(Debug, Serialize)]
pub struct TableOut {
    pub table: String,
    pub section: Section,
    pub rows: u64,
}

#[derive(Debug, Serialize)]
pub struct ControlOut {
    pub block: u64,
    pub log_index: u64,
    pub tx_hash: String,
    pub table: String,
    pub contract: String,
    pub event: Kind,
    pub text: String,
}

#[derive(Debug, Serialize)]
pub struct MoveOut {
    pub block: u64,
    pub log_index: u64,
    pub from: String,
    pub to: String,
    /// Base units, as decimal text.
    pub amount: String,
    pub reads: Reads,
    pub text: String,
}

#[derive(Debug, Serialize)]
pub struct NetLine {
    pub address: String,
    pub sent: String,
    pub received: String,
    /// `received - sent`, signed, in base units.
    pub net: String,
    pub text: String,
}

#[derive(Debug, Serialize)]
pub struct TokenOut {
    pub address: String,
    pub alias: String,
    pub symbol: Option<String>,
    pub decimals: Option<u8>,
    pub rows: u64,
    /// Distinct addresses that sent or received it in range, the zero address included.
    pub addresses: u64,
    pub largest: Vec<MoveOut>,
    /// Largest net flow first; addresses that netted to exactly zero are counted, not listed.
    pub net: Vec<NetLine>,
    pub net_not_listed: u64,
    pub netted_to_zero: u64,
}

#[derive(Debug, Serialize)]
pub struct SentenceOut {
    pub block: u64,
    pub log_index: u64,
    pub text: String,
}

/// A table the catalogue tells by its first rows in chain order.
#[derive(Debug, Serialize)]
pub struct FirstRowsOut {
    pub table: String,
    pub event: Kind,
    pub rows: u64,
    pub told: Vec<SentenceOut>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Counted,
    Omitted,
    NotApplicable,
}

#[derive(Debug, Serialize)]
pub struct HolderOut {
    pub address: String,
    /// Base units received in range.
    pub received: String,
    pub text: String,
}

#[derive(Debug, Serialize)]
pub struct NewHoldersToken {
    pub token: String,
    pub count: u64,
    pub largest: Vec<HolderOut>,
}

#[derive(Debug, Serialize)]
pub struct NewHoldersOut {
    pub status: Status,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// History before this block decides who is new; this nest holds none before `since`.
    pub before_block: u64,
    pub since: u64,
    /// Transfer rows before the range that the section read, or would have.
    pub history_rows: u64,
    pub budget: u64,
    pub tokens: Vec<NewHoldersToken>,
}

#[derive(Debug, Serialize)]
pub struct FirstTableOut {
    pub table: String,
    pub first_block: u64,
}

#[derive(Debug, Serialize)]
pub struct FirstTablesOut {
    pub status: Status,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub tables: Vec<FirstTableOut>,
}

#[derive(Debug, Serialize)]
pub struct ApprovalOut {
    pub block: u64,
    pub log_index: u64,
    pub owner: String,
    pub spender: String,
    /// Base units, as decimal text.
    pub amount: String,
    pub unlimited: bool,
    pub text: String,
}

#[derive(Debug, Serialize)]
pub struct ApprovalsOut {
    pub token: String,
    pub alias: String,
    pub symbol: Option<String>,
    pub decimals: Option<u8>,
    pub rows: u64,
    pub unlimited: u64,
    /// Unlimited approvals first in chain order, then the largest others.
    pub listed: Vec<ApprovalOut>,
}

#[derive(Debug, Serialize)]
pub struct FlagOut {
    pub block: u64,
    pub log_index: u64,
    pub tx_hash: String,
    pub text: String,
}

#[derive(Debug, Serialize)]
pub struct ThresholdOut {
    pub configured: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threshold: Option<String>,
    pub count: u64,
    pub listed: Vec<FlagOut>,
}

#[derive(Debug, Serialize)]
pub struct LabelOut {
    pub address: String,
    pub labels: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct Document {
    pub kind: &'static str,
    pub nest: NestOut,
    pub request: Request,
    pub coverage: CoverageOut,
    /// Every table with rows in range and the section that tells it: these sum to `rows_total`.
    pub tables: Vec<TableOut>,
    pub rows_total: u64,
    /// Items per list, the `--top` this was made with.
    pub top: usize,
    /// Every control-plane event in range, in chain order. Never capped.
    pub control: Vec<ControlOut>,
    pub movements: Vec<TokenOut>,
    pub nft_movements: Vec<FirstRowsOut>,
    pub new_holders: NewHoldersOut,
    pub first_tables: FirstTablesOut,
    pub approvals: Vec<ApprovalsOut>,
    pub nft_approvals: Vec<FirstRowsOut>,
    pub threshold_flags: ThresholdOut,
    /// Every labelled address that appears in a row this diff read.
    pub labels: Vec<LabelOut>,
    /// The tables no section above claims, told by their story lines or grouped.
    pub events: crate::story::Events,
    pub not_seen: &'static str,
    pub provenance: ProvenanceOut,
}

// ---------------------------------------------------------------------------------------------------
// Reading.

fn signed(received: U512, sent: U512) -> (String, U512, bool) {
    if received >= sent {
        ((received - sent).to_string(), received - sent, false)
    } else {
        (format!("-{}", sent - received), sent - received, true)
    }
}

fn earliest(a: &mut Option<(u64, u64, String)>, b: (u64, u64, &str), first: bool) {
    let better = a.as_ref().is_none_or(|x| {
        if first {
            (b.0, b.1) < (x.0, x.1)
        } else {
            (b.0, b.1) > (x.0, x.1)
        }
    });
    if better {
        *a = Some((b.0, b.1, b.2.to_string()));
    }
}

/// One ERC-20 approval: block, log index, owner, spender, amount.
type Approved = (u64, u64, String, String, U256);

/// Count everything the story reads in full, refuse above the cap, then read each section.
pub async fn read<S: Source>(
    src: &S,
    classified: &Classified,
    inputs: Inputs<'_>,
    ask: &Ask,
) -> Result<Document> {
    let (req, ctx, top) = (&ask.req, inputs.ctx, ask.top);
    let cat = catalogue(inputs.tables, classified);

    let mut whole = 0u64;
    for f in &classified.flow {
        whole += diff::count(src, &f.table, req).await?;
    }
    for c in cat.iter().filter(|c| c.kind.read_whole()) {
        whole += diff::count(src, &c.table, req).await?;
    }
    if whole > diff::ROW_CAP {
        let span = req.to - req.from + 1;
        let narrower = (span as u128 * diff::ROW_CAP as u128 / whole as u128).max(1) as u64;
        bail!(
            "{} hold {} transfer, approval and control-plane rows, over the diff's cap of {}. \
             Nothing was told rather than a part of it. Try a range of about {} blocks.",
            diff::what_text(req),
            group(whole),
            group(diff::ROW_CAP),
            group(narrower)
        );
    }

    let flow = diff::read_into(
        src,
        classified,
        req,
        diff::ROW_CAP,
        Flow::keeping_largest(top),
    )
    .await?;
    let other = flow.other_counts().clone();
    let rows_in = |t: &str| other.get(t).copied().unwrap_or(0);
    let mut first = flow.first.clone();
    let mut last = flow.last.clone();
    let mut seen: BTreeSet<String> = flow
        .sent
        .keys()
        .chain(flow.received.keys())
        .map(|(_, a)| a.clone())
        .collect();
    let mut tables: Vec<TableOut> = flow
        .table_rows()
        .iter()
        .filter(|(_, n)| **n > 0)
        .map(|(t, n)| TableOut {
            table: t.clone(),
            section: Section::Movements,
            rows: *n,
        })
        .collect();

    // Control plane: every row, in chain order.
    let mut control = Vec::new();
    for c in cat.iter().filter(|c| c.kind.is_control()) {
        let want = rows_in(&c.table);
        if want == 0 {
            continue;
        }
        let rows = read_rows(src, c, req, None).await?;
        if rows.len() as u64 != want {
            bail!(
                "{}: counted {want} rows in range but read {}; the nest moved under the read, run \
                 it again",
                c.table,
                rows.len()
            );
        }
        for r in &rows {
            earliest(&mut first, (r.block, r.log, &r.hash), true);
            earliest(&mut last, (r.block, r.log, &r.hash), false);
            seen.insert(r.address.clone());
            for v in r.p.iter().flatten() {
                seen.insert(v.to_ascii_lowercase());
            }
            control.push(ControlOut {
                block: r.block,
                log_index: r.log,
                tx_hash: r.tx.clone(),
                table: c.table.clone(),
                contract: r.address.clone(),
                event: c.kind,
                text: control_text(c.kind, r, ctx),
            });
        }
    }
    control.sort_by(|a, b| (a.block, a.log_index, &a.table).cmp(&(b.block, b.log_index, &b.table)));

    // ERC-20 approvals: every row read, per token; unlimited ones first.
    let mut by_token: BTreeMap<String, Vec<Approved>> = BTreeMap::new();
    let mut approval_alias: BTreeMap<String, String> = BTreeMap::new();
    for c in cat.iter().filter(|c| c.kind == Kind::Approval) {
        let want = rows_in(&c.table);
        if want == 0 {
            continue;
        }
        let rows = read_rows(src, c, req, None).await?;
        if rows.len() as u64 != want {
            bail!(
                "{}: counted {want} rows in range but read {}; the nest moved under the read, run \
                 it again",
                c.table,
                rows.len()
            );
        }
        for r in rows {
            let raw = r.at(2).unwrap_or_default().to_string();
            let amount = U256::from_str_radix(&raw, 10)
                .with_context(|| format!("{}: amount {raw:?} is not a decimal uint256", c.table))?;
            let (owner, spender) = (
                lower(r.at(0)).unwrap_or_default(),
                lower(r.at(1)).unwrap_or_default(),
            );
            earliest(&mut first, (r.block, r.log, &r.hash), true);
            earliest(&mut last, (r.block, r.log, &r.hash), false);
            seen.insert(owner.clone());
            seen.insert(spender.clone());
            approval_alias
                .entry(r.address.clone())
                .or_insert_with(|| c.alias.clone());
            by_token
                .entry(r.address.clone())
                .or_default()
                .push((r.block, r.log, owner, spender, amount));
        }
    }
    let mut approvals: Vec<ApprovalsOut> = by_token
        .into_iter()
        .map(|(token, mut rows)| {
            let n = rows.len() as u64;
            let unlimited = rows.iter().filter(|r| r.4 == U256::MAX).count() as u64;
            rows.sort_by(|a, b| {
                let (ua, ub) = (a.4 == U256::MAX, b.4 == U256::MAX);
                ub.cmp(&ua)
                    .then_with(|| {
                        if ua {
                            std::cmp::Ordering::Equal
                        } else {
                            b.4.cmp(&a.4)
                        }
                    })
                    .then((a.0, a.1).cmp(&(b.0, b.1)))
            });
            let meta = ctx.metadata.get(&token);
            ApprovalsOut {
                listed: rows
                    .iter()
                    .take(top)
                    .map(|(b, l, o, s, amt)| ApprovalOut {
                        block: *b,
                        log_index: *l,
                        owner: o.clone(),
                        spender: s.clone(),
                        amount: amt.to_string(),
                        unlimited: *amt == U256::MAX,
                        text: approval_text(ctx, &token, o, s, *amt),
                    })
                    .collect(),
                alias: approval_alias[&token].clone(),
                symbol: meta.and_then(|m| m.symbol.clone()),
                decimals: meta.and_then(|m| m.decimals),
                token,
                rows: n,
                unlimited,
            }
        })
        .collect();
    approvals.sort_by(|a, b| b.rows.cmp(&a.rows).then(a.token.cmp(&b.token)));

    // NFT movements and NFT approvals: counted, and told by their first rows.
    let mut nft_movements = Vec::new();
    let mut nft_approvals = Vec::new();
    for c in cat.iter().filter(|c| !c.kind.read_whole()) {
        let n = rows_in(&c.table);
        if n == 0 {
            continue;
        }
        let rows = read_rows(src, c, req, Some(top as u64)).await?;
        let told = rows
            .iter()
            .map(|r| {
                for v in r.p.iter().flatten() {
                    seen.insert(v.to_ascii_lowercase());
                }
                SentenceOut {
                    block: r.block,
                    log_index: r.log,
                    text: format!(
                        "{}: {}",
                        ctx.address_text(&r.address),
                        first_rows_text(c.kind, r, ctx, &c.alias)
                    ),
                }
            })
            .collect();
        let out = FirstRowsOut {
            table: c.table.clone(),
            event: c.kind,
            rows: n,
            told,
        };
        match c.kind.section() {
            Section::Movements => nft_movements.push(out),
            _ => nft_approvals.push(out),
        }
    }
    for list in [&mut nft_movements, &mut nft_approvals] {
        list.sort_by(|a, b| b.rows.cmp(&a.rows).then(a.table.cmp(&b.table)));
    }

    for c in &cat {
        let n = rows_in(&c.table);
        if n > 0 {
            tables.push(TableOut {
                table: c.table.clone(),
                section: c.kind.section(),
                rows: n,
            });
        }
    }

    let movements = tokens(&flow, ctx, top);
    let new_holders = new_holders(src, classified, &flow, req, ctx, top).await?;

    // Everything else, through the story lines and the grouped fallback.
    let claimed: BTreeSet<&str> = cat.iter().map(|c| c.table.as_str()).collect();
    let rest: BTreeMap<String, u64> = other
        .iter()
        .filter(|(t, _)| !claimed.contains(t.as_str()))
        .map(|(t, n)| (t.clone(), *n))
        .collect();
    for (t, n) in &rest {
        if *n > 0 {
            tables.push(TableOut {
                table: t.clone(),
                section: Section::Events,
                rows: *n,
            });
        }
    }
    tables.sort_by(|a, b| (&a.table, a.section).cmp(&(&b.table, b.section)));
    let plan = crate::story::Plan {
        schema: inputs.tables,
        classified,
        other: &rest,
        stories: inputs.stories,
        warnings: inputs.warnings,
    };
    let events = crate::story::read(src, plan, req, top, ctx).await?;

    let first_tables = first_tables(src, &tables, req, ctx).await?;
    let threshold_flags = threshold_flags(src, req, ctx, top).await?;
    seen.extend(labelled_in_range(src, inputs.tables, &tables, req, ctx).await?);
    let labels = seen
        .iter()
        .filter_map(|a| {
            ctx.labels.get(a).map(|l| LabelOut {
                address: a.clone(),
                labels: l.clone(),
            })
        })
        .collect();

    let at = |p: &Option<(u64, u64, String)>| {
        p.as_ref().map(|(b, l, h)| RowAt {
            block: *b,
            log_index: *l,
            block_hash: h.clone(),
        })
    };
    let rows_total = tables.iter().map(|t| t.rows).sum();
    Ok(Document {
        kind: "story",
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
        top,
        control,
        movements,
        nft_movements,
        new_holders,
        first_tables,
        approvals,
        nft_approvals,
        threshold_flags,
        labels,
        events,
        not_seen: diff::NOT_SEEN,
        provenance: ProvenanceOut {
            version: ctx.version.clone(),
            data_identity: ctx.data_identity.clone(),
            first_row: at(&first),
            last_row: at(&last),
            sealed_through: ctx.sealed_through,
            metadata_sha256: ctx.metadata_sha256.clone(),
            label_snapshots: ctx.label_snapshots.clone(),
            semantic_sha256: ctx.semantic_sha256.clone(),
        },
    })
}

/// Movements per token: the largest transfers and the largest net flows, most rows first.
fn tokens(flow: &Flow, ctx: &NestContext, top: usize) -> Vec<TokenOut> {
    let mut out: Vec<TokenOut> = flow
        .token_rows
        .iter()
        .map(|(token, n)| {
            let meta = ctx.metadata.get(token);
            let mut addresses: BTreeSet<&str> = BTreeSet::new();
            let mut nets = Vec::new();
            let mut zero = 0u64;
            let keys: BTreeSet<&(String, String)> = flow
                .sent
                .keys()
                .chain(flow.received.keys())
                .filter(|(t, _)| t == token)
                .collect();
            for k in keys {
                addresses.insert(&k.1);
                let s = flow.sent.get(k).copied().unwrap_or(U512::ZERO);
                let r = flow.received.get(k).copied().unwrap_or(U512::ZERO);
                let (net, mag, neg) = signed(r, s);
                if mag.is_zero() {
                    zero += 1;
                    continue;
                }
                nets.push((k.1.clone(), s, r, net, mag, neg));
            }
            nets.sort_by(|a, b| b.4.cmp(&a.4).then(a.0.cmp(&b.0)));
            let net_not_listed = nets.len().saturating_sub(top) as u64;
            let net = nets
                .into_iter()
                .take(top)
                .map(|(a, s, r, net, _, neg)| NetLine {
                    text: format!(
                        "{}  {}{}",
                        ctx.address_text(&a),
                        if neg { "" } else { "+" },
                        ctx.amount_text(token, &net)
                    ),
                    address: a,
                    sent: s.to_string(),
                    received: r.to_string(),
                    net,
                })
                .collect();
            let largest = flow
                .largest(token)
                .into_iter()
                .map(|m| MoveOut {
                    block: m.block,
                    log_index: m.log_index,
                    from: m.from.clone(),
                    to: m.to.clone(),
                    amount: m.amount.to_string(),
                    reads: m.reads,
                    text: format!(
                        "{} -> {}: {}{}",
                        ctx.address_text(&m.from),
                        ctx.address_text(&m.to),
                        ctx.amount_text(token, &m.amount.to_string()),
                        match m.reads {
                            Reads::Transfer => "",
                            Reads::Weth9Deposit => " (WETH9 Deposit)",
                            Reads::Weth9Withdrawal => " (WETH9 Withdrawal)",
                        }
                    ),
                })
                .collect();
            TokenOut {
                address: token.clone(),
                alias: flow.token_alias[token].clone(),
                symbol: meta.and_then(|m| m.symbol.clone()),
                decimals: meta.and_then(|m| m.decimals),
                rows: *n,
                addresses: addresses.len() as u64,
                largest,
                net,
                net_not_listed,
                netted_to_zero: zero,
            }
        })
        .collect();
    out.sort_by(|a, b| b.rows.cmp(&a.rows).then(a.address.cmp(&b.address)));
    out
}

/// The receiving column of a flow table, with the filter that keeps only its flow rows.
fn holder_column(f: &diff::FlowTable) -> Option<(String, String)> {
    let col = match f.reads {
        Reads::Transfer => f.second.clone()?,
        Reads::Weth9Deposit => f.first.clone(),
        Reads::Weth9Withdrawal => return None,
    };
    let emitters = if f.emitters.is_empty() {
        String::new()
    } else {
        let list: Vec<String> = f.emitters.iter().map(|a| format!("'{a}'")).collect();
        format!(" AND lower(address) IN ({})", list.join(", "))
    };
    Some((col, emitters))
}

/// §4's first-ever holders: of the addresses that received a token in range, those with no transfer
/// in of it before the range. Under its own budget of history rows; over it, omitted and said why.
async fn new_holders<S: Source>(
    src: &S,
    classified: &Classified,
    flow: &Flow,
    req: &Request,
    ctx: &NestContext,
    top: usize,
) -> Result<NewHoldersOut> {
    let since = ctx.start_block.unwrap_or(0);
    let mut out = NewHoldersOut {
        status: Status::Counted,
        reason: None,
        before_block: req.from,
        since,
        history_rows: 0,
        budget: NEW_HOLDERS_BUDGET,
        tokens: Vec::new(),
    };
    if req.from <= since {
        out.status = Status::NotApplicable;
        out.reason = Some(format!(
            "the range begins at this nest's first indexed block, {}, so the nest holds no earlier \
             transfer to tell a new holder from an old one",
            group(since)
        ));
        return Ok(out);
    }
    let touched: BTreeMap<(&str, &str), U512> = flow
        .received
        .iter()
        .filter(|((_, a), _)| a != ZERO_ADDRESS)
        .map(|((t, a), v)| ((t.as_str(), a.as_str()), *v))
        .collect();
    if touched.is_empty() {
        return Ok(out);
    }
    let receiving: Vec<(&diff::FlowTable, String, String)> = classified
        .flow
        .iter()
        .filter_map(|f| holder_column(f).map(|(c, e)| (f, c, e)))
        .collect();
    for (f, _, emitters) in &receiving {
        let sql = format!(
            "SELECT count(*) AS n FROM {} WHERE block_number < {}{emitters}",
            quote(&f.table),
            req.from
        );
        let q = src
            .query(&sql)
            .await
            .with_context(|| format!("counting {}", f.table))?;
        diff::complete(&q, &f.table)?;
        out.history_rows += diff::number(q.rows.first().and_then(|r| r.get("n"))).unwrap_or(0);
    }
    if out.history_rows > NEW_HOLDERS_BUDGET {
        out.status = Status::Omitted;
        out.reason = Some(format!(
            "telling a new holder from an old one means reading the {} transfer rows before block \
             {}, over this section's budget of {}",
            group(out.history_rows),
            group(req.from),
            group(NEW_HOLDERS_BUDGET)
        ));
        return Ok(out);
    }
    let before = match seen_before(src, &receiving, req, &touched).await {
        Ok(b) => b,
        Err(e) if crate::sql_errors::is_out_of_memory(&format!("{e:#}")) => {
            out.status = Status::Omitted;
            out.reason = Some(format!(
                "telling a new holder from an old one means reading the {} transfer rows before \
                 block {}, and the query engine could not do it within its memory limit",
                group(out.history_rows),
                group(req.from)
            ));
            return Ok(out);
        }
        Err(e) => return Err(e),
    };
    let mut per: BTreeMap<&str, Vec<(&str, U512)>> = BTreeMap::new();
    for (&(t, a), v) in &touched {
        if !before.contains(&(t.to_string(), a.to_string())) {
            per.entry(t).or_default().push((a, *v));
        }
    }
    let rows_of = |t: &str| flow.token_rows.get(t).copied().unwrap_or(0);
    let mut tokens: Vec<NewHoldersToken> = per
        .into_iter()
        .map(|(t, mut hs)| {
            hs.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
            NewHoldersToken {
                token: t.to_string(),
                count: hs.len() as u64,
                largest: hs
                    .iter()
                    .take(top)
                    .map(|(a, v)| HolderOut {
                        address: a.to_string(),
                        received: v.to_string(),
                        text: format!(
                            "{} received {}",
                            ctx.address_text(a),
                            ctx.amount_text(t, &v.to_string())
                        ),
                    })
                    .collect(),
            }
        })
        .collect();
    tokens.sort_by(|a, b| {
        rows_of(&b.token)
            .cmp(&rows_of(&a.token))
            .then(a.token.cmp(&b.token))
    });
    out.tokens = tokens;
    Ok(out)
}

/// The touched (token, holder) pairs that received the token before the range. History is read as
/// distinct pairs, so the engine holds one entry per holder rather than joining two row sets.
async fn seen_before<S: Source>(
    src: &S,
    receiving: &[(&diff::FlowTable, String, String)],
    req: &Request,
    touched: &BTreeMap<(&str, &str), U512>,
) -> Result<BTreeSet<(String, String)>> {
    let mut before = BTreeSet::new();
    for (f, col, emitters) in receiving {
        let h = format!("lower(CAST({} AS VARCHAR))", quote(col));
        let mut after: Option<(String, String)> = None;
        loop {
            let keyset = after
                .as_ref()
                .map(|(t, a)| format!(" WHERE t > '{t}' OR (t = '{t}' AND h > '{a}')"))
                .unwrap_or_default();
            let sql = format!(
                "SELECT t, h FROM (SELECT DISTINCT lower(address) AS t, {h} AS h FROM {tbl} \
                 WHERE block_number < {from}{emitters}){keyset} ORDER BY t, h LIMIT {PAGE}",
                tbl = quote(&f.table),
                from = req.from,
            );
            let q = src
                .query(&sql)
                .await
                .with_context(|| format!("reading {}", f.table))?;
            diff::complete(&q, &f.table)?;
            let n = q.rows.len() as u64;
            for r in &q.rows {
                let (Some(t), Some(a)) = (diff::text(r.get("t")), diff::text(r.get("h"))) else {
                    continue;
                };
                if touched.contains_key(&(t.as_str(), a.as_str())) {
                    before.insert((t.clone(), a.clone()));
                }
                after = Some((t, a));
            }
            if n < PAGE {
                break;
            }
        }
    }
    Ok(before)
}

/// Tables whose first row in this nest is in the range.
async fn first_tables<S: Source>(
    src: &S,
    tables: &[TableOut],
    req: &Request,
    ctx: &NestContext,
) -> Result<FirstTablesOut> {
    let since = ctx.start_block.unwrap_or(0);
    if req.from <= since {
        return Ok(FirstTablesOut {
            status: Status::NotApplicable,
            reason: Some(format!(
                "the range begins at this nest's first indexed block, {}",
                group(since)
            )),
            tables: Vec::new(),
        });
    }
    let names: BTreeSet<&str> = tables.iter().map(|t| t.table.as_str()).collect();
    let mut out = Vec::new();
    for t in names {
        let q = src
            .query(&format!(
                "SELECT block_number FROM {} WHERE block_number < {} LIMIT 1",
                quote(t),
                req.from
            ))
            .await
            .with_context(|| format!("reading {t}"))?;
        diff::complete(&q, t)?;
        if !q.rows.is_empty() {
            continue;
        }
        let q = src
            .query(&format!(
                "SELECT min(block_number) AS b FROM {} WHERE {}",
                quote(t),
                diff::filter(req)
            ))
            .await
            .with_context(|| format!("reading {t}"))?;
        diff::complete(&q, t)?;
        if let Some(b) = diff::number(q.rows.first().and_then(|r| r.get("b"))) {
            out.push(FirstTableOut {
                table: t.to_string(),
                first_block: b,
            });
        }
    }
    Ok(FirstTablesOut {
        status: Status::Counted,
        reason: None,
        tables: out,
    })
}

/// Threshold flags raised in range, where the nest has a threshold configured.
/// Every labelled address in any address column of any table with rows in range, the emitter included.
/// Labels are few and rows many, so each table is asked which labelled addresses it holds.
async fn labelled_in_range<S: Source>(
    src: &S,
    schema: &[TableSchema],
    tables: &[TableOut],
    req: &Request,
    ctx: &NestContext,
) -> Result<BTreeSet<String>> {
    let mut found = BTreeSet::new();
    if ctx.labels.is_empty() {
        return Ok(found);
    }
    let wanted: Vec<String> = ctx.labels.keys().map(|a| format!("'{a}'")).collect();
    let names: BTreeSet<&str> = tables.iter().map(|t| t.table.as_str()).collect();
    for t in schema.iter().filter(|t| names.contains(t.table.as_str())) {
        for c in t
            .columns
            .iter()
            .filter(|c| c.name == "address" || c.sol_type == "address")
        {
            let col = format!("lower(CAST({} AS VARCHAR))", quote(&c.name));
            for chunk in wanted.chunks(500) {
                let q = src
                    .query(&format!(
                        "SELECT DISTINCT {col} AS a FROM {} WHERE {} AND {col} IN ({})",
                        quote(&t.table),
                        diff::filter(req),
                        chunk.join(", ")
                    ))
                    .await
                    .with_context(|| format!("reading {}", t.table))?;
                diff::complete(&q, &t.table)?;
                found.extend(q.rows.iter().filter_map(|r| diff::text(r.get("a"))));
            }
        }
    }
    Ok(found)
}

async fn threshold_flags<S: Source>(
    src: &S,
    req: &Request,
    ctx: &NestContext,
    top: usize,
) -> Result<ThresholdOut> {
    let mut out = ThresholdOut {
        configured: ctx.threshold.is_some(),
        threshold: ctx.threshold.map(|t| t.to_string()),
        count: 0,
        listed: Vec::new(),
    };
    if ctx.threshold.is_none() {
        return Ok(out);
    }
    // The table exists only once a flag has been raised.
    let probe = src
        .query(
            "SELECT count(*) AS n FROM information_schema.tables WHERE table_name = 'threshold_flag'",
        )
        .await
        .context("looking for threshold_flag")?;
    if diff::number(probe.rows.first().and_then(|r| r.get("n"))).unwrap_or(0) == 0 {
        return Ok(out);
    }
    let range = format!(
        "block_number >= {} AND block_number <= {}{}",
        req.from,
        req.to,
        req.tx
            .as_ref()
            .map(|t| format!(" AND lower(tx_hash) = '{t}'"))
            .unwrap_or_default()
    );
    let q = src
        .query(&format!(
            "SELECT count(*) AS n FROM threshold_flag WHERE {range}"
        ))
        .await
        .context("counting threshold_flag")?;
    diff::complete(&q, "threshold_flag")?;
    out.count = diff::number(q.rows.first().and_then(|r| r.get("n"))).unwrap_or(0);
    let q = src
        .query(&format!(
            "SELECT block_number, log_index, tx_hash, \"from\" AS f, \"to\" AS t, \
             CAST(value AS VARCHAR) AS v FROM threshold_flag WHERE {range} \
             ORDER BY block_number, log_index LIMIT {top}"
        ))
        .await
        .context("reading threshold_flag")?;
    diff::complete(&q, "threshold_flag")?;
    let threshold = ctx.threshold.unwrap_or_default().to_string();
    for r in &q.rows {
        let s = |k: &str| diff::text(r.get(k)).unwrap_or_default();
        out.listed.push(FlagOut {
            block: diff::number(r.get("block_number")).unwrap_or(0),
            log_index: diff::number(r.get("log_index")).unwrap_or(0),
            tx_hash: s("tx_hash"),
            text: format!(
                "a transfer of {} base units from {} to {}, at or over the threshold of {}",
                group(s("v")),
                ctx.address_text(&s("f").to_ascii_lowercase()),
                ctx.address_text(&s("t").to_ascii_lowercase()),
                group(&threshold)
            ),
        });
    }
    Ok(out)
}

// ---------------------------------------------------------------------------------------------------
// Rendering: text and Markdown are the same lines, set differently.

struct Block {
    title: String,
    /// (depth, line)
    lines: Vec<(usize, String)>,
}

fn at(block: u64, log: u64) -> String {
    format!("block {} log {log}", group(block))
}

fn plural(n: u64, one: &str, many: &str) -> String {
    format!("{} {}", group(n), if n == 1 { one } else { many })
}

fn token_heading(
    symbol: &Option<String>,
    alias: &str,
    address: &str,
    decimals: Option<u8>,
) -> String {
    let name = symbol.clone().unwrap_or_else(|| alias.to_string());
    let d = match decimals {
        Some(d) => format!("{d} decimals"),
        None => "decimals unknown: run `nuthatch metadata fetch`".into(),
    };
    format!("{name} {address} ({d})")
}

fn blocks(doc: &Document) -> Vec<Block> {
    let mut out = Vec::new();

    let mut lines = Vec::new();
    if doc.control.is_empty() {
        lines.push((0, "None in range.".to_string()));
    }
    for c in &doc.control {
        lines.push((
            0,
            format!(
                "{} (tx {}): {}",
                at(c.block, c.log_index),
                c.tx_hash,
                c.text
            ),
        ));
    }
    out.push(Block {
        title: "Control plane: every ownership, admin, role, pause and upgrade event, uncapped"
            .into(),
        lines,
    });

    let mut lines = Vec::new();
    if doc.movements.is_empty() && doc.nft_movements.is_empty() {
        lines.push((0, "No token movements in range.".to_string()));
    }
    for t in &doc.movements {
        lines.push((
            0,
            format!(
                "{}: {} among {}",
                token_heading(&t.symbol, &t.alias, &t.address, t.decimals),
                plural(t.rows, "movement", "movements"),
                plural(t.addresses, "address", "addresses")
            ),
        ));
        lines.push((1, "Largest transfers".into()));
        for m in &t.largest {
            lines.push((2, format!("{}: {}", at(m.block, m.log_index), m.text)));
        }
        let more = t.rows.saturating_sub(t.largest.len() as u64);
        if more > 0 {
            lines.push((
                2,
                format!(
                    "{} not listed",
                    plural(more, "smaller movement", "smaller movements")
                ),
            ));
        }
        lines.push((
            1,
            "Largest net flow of the transfers seen (not a balance)".into(),
        ));
        for n in &t.net {
            lines.push((2, n.text.clone()));
        }
        if t.net_not_listed > 0 {
            lines.push((
                2,
                format!(
                    "{} not listed",
                    plural(t.net_not_listed, "smaller address", "smaller addresses")
                ),
            ));
        }
        if t.netted_to_zero > 0 {
            lines.push((
                2,
                format!(
                    "{} netted to exactly zero",
                    plural(t.netted_to_zero, "address", "addresses")
                ),
            ));
        }
    }
    for n in &doc.nft_movements {
        lines.push((
            0,
            format!(
                "{}, {}: {}, the first {} in chain order",
                n.table,
                n.event.event_name(),
                plural(n.rows, "row", "rows"),
                n.told.len()
            ),
        ));
        for s in &n.told {
            lines.push((1, format!("{}: {}", at(s.block, s.log_index), s.text)));
        }
    }
    out.push(Block {
        title: "Largest movements, per token (amounts of two tokens are never compared)".into(),
        lines,
    });

    let h = &doc.new_holders;
    let mut lines = Vec::new();
    match h.status {
        Status::NotApplicable => lines.push((
            0,
            format!("Not counted: {}.", h.reason.as_deref().unwrap_or("")),
        )),
        Status::Omitted => lines.push((
            0,
            format!("Omitted: {}.", h.reason.as_deref().unwrap_or("")),
        )),
        Status::Counted if h.tokens.is_empty() => lines.push((
            0,
            "None: every address that received a token in range had received it before.".into(),
        )),
        Status::Counted => {}
    }
    let names: BTreeMap<&str, String> = doc
        .movements
        .iter()
        .map(|t| {
            (
                t.address.as_str(),
                t.symbol.clone().unwrap_or_else(|| t.alias.clone()),
            )
        })
        .collect();
    for t in &h.tokens {
        let name = names
            .get(t.token.as_str())
            .cloned()
            .unwrap_or_else(|| t.token.clone());
        lines.push((0, format!("{name}: {}", group(t.count))));
        for x in &t.largest {
            lines.push((1, x.text.clone()));
        }
        let more = t.count.saturating_sub(t.largest.len() as u64);
        if more > 0 {
            lines.push((1, format!("{} not listed", plural(more, "more", "more"))));
        }
    }
    out.push(Block {
        title: format!(
            "New holders: no transfer in of the token before block {}, in this nest's rows since block {}",
            group(h.before_block),
            group(h.since)
        ),
        lines,
    });

    let f = &doc.first_tables;
    let mut lines = Vec::new();
    match f.status {
        Status::Counted if f.tables.is_empty() => lines.push((
            0,
            "None: every table here has rows before the range.".to_string(),
        )),
        Status::Counted => {}
        _ => lines.push((
            0,
            format!("Not counted: {}.", f.reason.as_deref().unwrap_or("")),
        )),
    }
    for t in &f.tables {
        lines.push((
            0,
            format!("{}: first row at block {}", t.table, group(t.first_block)),
        ));
    }
    out.push(Block {
        title: "First rows: tables whose first row in this nest is in range".into(),
        lines,
    });

    let mut lines = Vec::new();
    if doc.approvals.is_empty() && doc.nft_approvals.is_empty() {
        lines.push((0, "None in range.".to_string()));
    }
    for a in &doc.approvals {
        lines.push((
            0,
            format!(
                "{}: {}, {} unlimited",
                token_heading(&a.symbol, &a.alias, &a.token, a.decimals),
                plural(a.rows, "approval", "approvals"),
                group(a.unlimited)
            ),
        ));
        for x in &a.listed {
            lines.push((1, format!("{}: {}", at(x.block, x.log_index), x.text)));
        }
        let listed_unlimited = a.listed.iter().filter(|x| x.unlimited).count() as u64;
        if a.unlimited > listed_unlimited {
            lines.push((
                1,
                format!(
                    "{} not listed",
                    plural(
                        a.unlimited - listed_unlimited,
                        "more unlimited approval",
                        "more unlimited approvals"
                    )
                ),
            ));
        }
        let rest = a.rows - a.listed.len() as u64 - (a.unlimited - listed_unlimited);
        if rest > 0 {
            lines.push((
                1,
                format!(
                    "{} not listed",
                    plural(rest, "smaller approval", "smaller approvals")
                ),
            ));
        }
    }
    for n in &doc.nft_approvals {
        lines.push((
            0,
            format!(
                "{}, {}: {}, the first {} in chain order",
                n.table,
                n.event.event_name(),
                plural(n.rows, "row", "rows"),
                n.told.len()
            ),
        ));
        for s in &n.told {
            lines.push((1, format!("{}: {}", at(s.block, s.log_index), s.text)));
        }
    }
    out.push(Block {
        title: "Approvals".into(),
        lines,
    });

    let mut lines = Vec::new();
    let t = &doc.threshold_flags;
    if !t.configured {
        lines.push((
            0,
            "Threshold flags: not configured for this nest.".to_string(),
        ));
    } else {
        lines.push((
            0,
            format!(
                "Threshold flags at {} base units: {} in range",
                group(t.threshold.as_deref().unwrap_or("0")),
                group(t.count)
            ),
        ));
        for x in &t.listed {
            lines.push((
                1,
                format!(
                    "{} (tx {}): {}",
                    at(x.block, x.log_index),
                    x.tx_hash,
                    x.text
                ),
            ));
        }
        let more = t.count.saturating_sub(t.listed.len() as u64);
        if more > 0 {
            lines.push((1, format!("{} not listed", plural(more, "more", "more"))));
        }
    }
    if doc.labels.is_empty() {
        lines.push((0, "Labelled addresses seen: none.".to_string()));
    } else {
        lines.push((
            0,
            format!("Labelled addresses seen: {}", group(doc.labels.len())),
        ));
        for l in &doc.labels {
            lines.push((1, format!("{}: {}", l.address, l.labels.join(", "))));
        }
    }
    out.push(Block {
        title: "Flags and labels".into(),
        lines,
    });
    out
}

/// The events section's own text, as (depth, line): [`crate::story::text`] indents by two spaces.
fn event_lines(doc: &Document) -> Vec<(usize, String)> {
    crate::story::text(&doc.events, doc.top)
        .lines()
        .map(|l| {
            let depth = (l.len() - l.trim_start().len()) / 2;
            (depth, l.trim_start().to_string())
        })
        .collect()
}

fn preamble(doc: &Document) -> Vec<String> {
    vec![
        format!(
            "{} on {}.",
            capitalised(&diff::what_text(&doc.request)),
            diff::chain_text(&doc.nest)
        ),
        format!(
            "Nest {}, data identity {}. {} in range across {}.",
            doc.nest.name,
            doc.nest.data_identity.as_deref().unwrap_or("unknown"),
            plural(doc.rows_total, "row", "rows"),
            plural(
                doc.tables
                    .iter()
                    .map(|t| t.table.as_str())
                    .collect::<BTreeSet<_>>()
                    .len() as u64,
                "table",
                "tables"
            )
        ),
        diff::finality_line(
            &doc.request,
            doc.coverage.sealed_through,
            doc.coverage.is_final,
        ),
    ]
}

fn capitalised(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        Some(f) => f.to_uppercase().chain(c).collect(),
        None => String::new(),
    }
}

fn text_out(doc: &Document) -> String {
    let mut o = String::new();
    for l in preamble(doc) {
        let _ = writeln!(o, "{l}");
    }
    for b in blocks(doc) {
        o.push('\n');
        let _ = writeln!(o, "{}", b.title);
        for (d, l) in &b.lines {
            let _ = writeln!(o, "{}{l}", "  ".repeat(d + 1));
        }
    }
    o.push('\n');
    o.push_str(&crate::story::text(&doc.events, doc.top));
    let _ = writeln!(o, "{}", doc.not_seen);
    o.push('\n');
    for l in diff::provenance_lines(&doc.provenance) {
        let _ = writeln!(o, "{l}");
    }
    o
}

/// Characters Markdown would read as markup, so a value cannot become a link or an emphasis.
fn md(s: &str) -> String {
    let mut o = String::with_capacity(s.len());
    for c in s.chars() {
        if matches!(c, '\\' | '`' | '*' | '[' | ']' | '<' | '>' | '|') {
            o.push('\\');
        }
        o.push(c);
    }
    o
}

fn markdown_out(doc: &Document) -> String {
    let mut o = String::new();
    let pre = preamble(doc);
    let _ = writeln!(o, "# {}", md(&pre[0]));
    o.push('\n');
    for l in &pre[1..] {
        let _ = writeln!(o, "{}  ", md(l));
    }
    let mut all = blocks(doc);
    let mut ev = event_lines(doc);
    let title = if ev.is_empty() {
        String::new()
    } else {
        ev.remove(0).1
    };
    all.push(Block {
        title,
        lines: ev
            .into_iter()
            .map(|(d, l)| (d.saturating_sub(1), l))
            .collect(),
    });
    for b in all {
        o.push('\n');
        let _ = writeln!(o, "## {}", md(&b.title));
        if !b.lines.is_empty() {
            o.push('\n');
        }
        for (d, l) in &b.lines {
            let _ = writeln!(o, "{}- {}", "  ".repeat(*d), md(l));
        }
    }
    o.push('\n');
    let _ = writeln!(o, "{}", md(doc.not_seen));
    o.push('\n');
    for l in diff::provenance_lines(&doc.provenance) {
        let _ = writeln!(o, "{}  ", md(&l));
    }
    o
}

pub fn render(doc: &Document, format: Format) -> String {
    match format {
        Format::Json => {
            let mut s = serde_json::to_string_pretty(doc).expect("the document serialises");
            s.push('\n');
            s
        }
        Format::Markdown => markdown_out(doc),
        _ => text_out(doc),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roles_are_named_by_hash_and_otherwise_printed_whole() {
        let minter = format!("0x{}", hex::encode(keccak256(b"MINTER_ROLE")));
        assert_eq!(
            role_text(Some(&minter)),
            format!("role MINTER_ROLE ({minter})")
        );
        let zero = format!("0x{}", "0".repeat(64));
        assert_eq!(
            role_text(Some(&zero)),
            format!("role DEFAULT_ADMIN_ROLE ({zero})")
        );
        let other = format!("0x{}", "ab".repeat(32));
        assert_eq!(role_text(Some(&other)), format!("role {other}"));
    }

    const TOKEN: &str = "0x00000000000000000000000000000000000000cc";
    const OWNER: &str = "0x00000000000000000000000000000000000000aa";
    const SPENDER: &str = "0x00000000000000000000000000000000000000bb";

    fn ctx() -> NestContext {
        let mut ctx = NestContext::default();
        ctx.metadata.insert(
            TOKEN.into(),
            crate::metadata::TokenMetadata {
                decimals: Some(6),
                symbol: Some("USDC".into()),
                name: None,
            },
        );
        ctx
    }

    /// Answers each history count with `history` and every other query with no rows.
    struct History(u64);

    impl Source for History {
        async fn query(&self, sql: &str) -> Result<crate::analytics::QueryOutput> {
            let rows = if sql.starts_with("SELECT count(*) AS n") && sql.contains("block_number <")
            {
                vec![serde_json::json!({ "n": self.0 })]
            } else {
                Vec::new()
            };
            Ok(crate::analytics::QueryOutput {
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

    fn one_receipt() -> (Classified, Flow) {
        let classified = Classified {
            flow: vec![diff::FlowTable {
                table: "usdc__transfer".into(),
                alias: "usdc".into(),
                reads: Reads::Transfer,
                first: "from".into(),
                second: Some("to".into()),
                amount: "value".into(),
                emitters: Vec::new(),
            }],
            other: Vec::new(),
        };
        let mut flow = Flow::default();
        flow.add(diff::Movement {
            block: 150,
            log_index: 0,
            block_hash: "0xh".into(),
            table: "usdc__transfer".into(),
            token: TOKEN.into(),
            alias: "usdc".into(),
            from: OWNER.into(),
            to: SPENDER.into(),
            amount: U256::from(7),
            reads: Reads::Transfer,
        });
        (classified, flow)
    }

    /// §6: telling a new holder from an old one reads the token's history before the range. Over
    /// the section's budget it is omitted, and the line says how many rows and what the budget is.
    #[tokio::test]
    async fn new_holders_over_their_budget_are_omitted_and_say_why() {
        let (classified, flow) = one_receipt();
        let mut ctx = ctx();
        ctx.start_block = Some(100);
        let req = Request {
            from: 150,
            to: 150,
            tx: None,
        };
        let over = new_holders(
            &History(NEW_HOLDERS_BUDGET + 1),
            &classified,
            &flow,
            &req,
            &ctx,
            10,
        )
        .await
        .unwrap();
        assert_eq!(over.status, Status::Omitted);
        assert!(over.tokens.is_empty());
        assert_eq!(
            over.reason.as_deref(),
            Some(
                "telling a new holder from an old one means reading the 20,000,001 transfer rows \
                 before block 150, over this section's budget of 20,000,000"
            )
        );
        let under = new_holders(
            &History(NEW_HOLDERS_BUDGET),
            &classified,
            &flow,
            &req,
            &ctx,
            10,
        )
        .await
        .unwrap();
        assert_eq!(under.status, Status::Counted);
        assert_eq!(
            under.tokens.len(),
            1,
            "nothing in history, so the recipient is new"
        );
        assert_eq!(under.tokens[0].count, 1);
        assert_eq!(under.tokens[0].largest[0].address, SPENDER);
    }

    /// Counts a small history, then refuses the history read as the engine does over its pool.
    struct Exhausted;

    impl Source for Exhausted {
        async fn query(&self, sql: &str) -> Result<crate::analytics::QueryOutput> {
            if sql.contains("DISTINCT") {
                anyhow::bail!("substrate error: Out of Memory Error: Additional allocation failed");
            }
            History(5).query(sql).await
        }
    }

    /// Within its row budget the history read can still exceed the engine's memory: the section is
    /// omitted and says so, and the rest of the diff stands.
    #[tokio::test]
    async fn new_holders_the_engine_cannot_hold_are_omitted_not_fatal() {
        let (classified, flow) = one_receipt();
        let mut ctx = ctx();
        ctx.start_block = Some(100);
        let req = Request {
            from: 150,
            to: 150,
            tx: None,
        };
        let out = new_holders(&Exhausted, &classified, &flow, &req, &ctx, 10)
            .await
            .unwrap();
        assert_eq!(out.status, Status::Omitted);
        assert!(
            out.reason
                .as_deref()
                .unwrap()
                .ends_with("could not do it within its memory limit"),
            "{:?}",
            out.reason
        );
    }

    /// 2^256-1 is the one amount named rather than printed, and one less than it is printed exactly.
    #[test]
    fn only_the_maximum_approval_is_called_unlimited() {
        let unlimited = approval_text(&ctx(), TOKEN, OWNER, SPENDER, U256::MAX);
        assert_eq!(
            unlimited,
            format!("{OWNER} approved {SPENDER} to spend any amount of USDC (unlimited: 2^256-1)")
        );
        let almost = approval_text(&ctx(), TOKEN, OWNER, SPENDER, U256::MAX - U256::from(1));
        assert!(
            !almost.contains("unlimited") && !almost.contains("any amount"),
            "{almost}"
        );
        assert!(
            almost.ends_with(
                "115,792,089,237,316,195,423,570,985,008,687,907,853,269,984,665,640,564,039,457,\
                 584,007,913,129.639934 USDC"
            ),
            "{almost}"
        );
        assert_eq!(
            approval_text(&ctx(), TOKEN, OWNER, SPENDER, U256::ZERO),
            format!("{OWNER} set {SPENDER}'s allowance to zero")
        );
    }
}
