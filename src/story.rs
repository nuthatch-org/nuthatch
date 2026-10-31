//! How a diff tells the tables it does not read as flow (RFC-0071 §17): a table with a `story` line in
//! `semantic.toml` is told one sentence per row, and every other table is grouped by its indexed
//! parameters. Both are fixed rules over the rows; nothing here is stored, and no model writes a word.
//!
//! A story binds parameters by name, unlike the flow's catalogue, which binds by position: the
//! catalogue meets tables it has never seen, while a story is written for one table whose column names
//! the registry pins, and a name that stops matching is a drift warning, not a wrong sentence.

use std::collections::BTreeMap;

use anyhow::{bail, Context, Result};
use serde::Serialize;
use serde_json::Value;

use crate::diff::{self, Classified, NestContext, Request, Source};
use crate::registry::{TableKind, TableSchema};
use crate::semantic::Semantic;

/// Values per indexed parameter that the grouped fallback names.
pub const GROUP_TOP: usize = 3;

/// Whose decimals scale an amount.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Token {
    /// The contract that emitted the row: its `address` column.
    Emitter,
    Address(String),
    /// An address column of the same row.
    Column(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Piece {
    Text(String),
    Value(String),
    Amount(String, Token),
}

#[derive(Debug, Clone)]
pub struct Story {
    pub table: String,
    pub pieces: Vec<Piece>,
    /// Every column the story reads, with its Solidity type and storage.
    types: BTreeMap<String, (String, String)>,
}

fn is_ident(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

fn is_address(s: &str) -> bool {
    s.len() == 42 && s.starts_with("0x") && s[2..].bytes().all(|b| b.is_ascii_hexdigit())
}

/// `{name}`, `{name:amount}`, `{name:amount@0x…}` or `{name:amount@column}`; `{{` and `}}` are braces.
pub fn parse(src: &str) -> std::result::Result<Vec<Piece>, String> {
    let mut out = Vec::new();
    let mut text = String::new();
    let mut chars = src.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '{' if chars.peek() == Some(&'{') => {
                chars.next();
                text.push('{');
            }
            '}' if chars.peek() == Some(&'}') => {
                chars.next();
                text.push('}');
            }
            '}' => return Err("a `}` with no `{` (write `}}` for a brace)".into()),
            '{' => {
                let mut inner = String::new();
                loop {
                    match chars.next() {
                        Some('}') => break,
                        Some('{') | None => return Err(format!("`{{{inner}` is never closed")),
                        Some(c) => inner.push(c),
                    }
                }
                if !text.is_empty() {
                    out.push(Piece::Text(std::mem::take(&mut text)));
                }
                out.push(placeholder(&inner)?);
            }
            c => text.push(c),
        }
    }
    if !text.is_empty() {
        out.push(Piece::Text(text));
    }
    Ok(out)
}

fn placeholder(inner: &str) -> std::result::Result<Piece, String> {
    let (name, spec) = match inner.split_once(':') {
        Some((n, s)) => (n, Some(s)),
        None => (inner, None),
    };
    if !is_ident(name) {
        return Err(format!("`{{{inner}}}` does not start with a column name"));
    }
    let Some(spec) = spec else {
        return Ok(Piece::Value(name.into()));
    };
    let (kind, at) = match spec.split_once('@') {
        Some((k, a)) => (k, Some(a)),
        None => (spec, None),
    };
    if kind != "amount" {
        return Err(format!(
            "`{{{inner}}}`: the only format is `amount`, not `{kind}`"
        ));
    }
    let token = match at {
        None => Token::Emitter,
        Some(a) if a.starts_with("0x") => {
            if !is_address(a) {
                return Err(format!("`{{{inner}}}`: `{a}` is not a 20-byte address"));
            }
            Token::Address(a.to_ascii_lowercase())
        }
        Some(a) if is_ident(a) => Token::Column(a.into()),
        Some(a) => {
            return Err(format!(
                "`{{{inner}}}`: `@{a}` is neither an address nor a column"
            ))
        }
    };
    Ok(Piece::Amount(name.into(), token))
}

fn is_integer(sol_type: &str) -> bool {
    (sol_type.starts_with("uint") || sol_type.starts_with("int")) && !sol_type.contains('[')
}

fn holds_address(sol_type: &str, storage: &str) -> bool {
    sol_type == "address" || (sol_type == "implicit" && storage == "address")
}

fn check(t: &TableSchema, pieces: &[Piece]) -> std::result::Result<Story, String> {
    let cols: BTreeMap<&str, (&str, &str)> = t
        .columns
        .iter()
        .map(|c| (c.name.as_str(), (c.sol_type.as_str(), c.storage.as_str())))
        .collect();
    let mut types = BTreeMap::new();
    let mut need = |name: &str| -> std::result::Result<(String, String), String> {
        let (ty, st) = cols
            .get(name)
            .ok_or_else(|| format!("`{name}` isn't a column of that table"))?;
        let v = (ty.to_string(), st.to_string());
        types.insert(name.to_string(), v.clone());
        Ok(v)
    };
    for p in pieces {
        match p {
            Piece::Text(_) => {}
            Piece::Value(name) => {
                need(name)?;
            }
            Piece::Amount(name, token) => {
                let (ty, _) = need(name)?;
                if !is_integer(&ty) {
                    return Err(format!(
                        "`{{{name}:amount}}` needs an integer parameter, and `{name}` is {ty}"
                    ));
                }
                match token {
                    Token::Emitter => {
                        need("address")?;
                    }
                    Token::Column(c) => {
                        let (ty, st) = need(c)?;
                        if !holds_address(&ty, &st) {
                            return Err(format!(
                                "`@{c}` names the token, so it must be an address column, and \
                                 `{c}` is {ty}"
                            ));
                        }
                    }
                    Token::Address(_) => {}
                }
            }
        }
    }
    Ok(Story {
        table: t.table.clone(),
        pieces: pieces.to_vec(),
        types,
    })
}

/// Every usable story line, and a warning naming each one that is not used. A story for a table the
/// registry lacks is left to [`crate::semantic::drift`], which already names the table.
pub fn compile(schema: &[TableSchema], sem: &Semantic) -> (BTreeMap<String, Story>, Vec<String>) {
    let tables: BTreeMap<&str, &TableSchema> =
        schema.iter().map(|t| (t.table.as_str(), t)).collect();
    let mut stories = BTreeMap::new();
    let mut warnings = Vec::new();
    for (name, ts) in &sem.tables {
        if ts.story.is_empty() {
            continue;
        }
        let Some(t) = tables.get(name.as_str()) else {
            continue;
        };
        match parse(&ts.story).and_then(|p| check(t, &p)) {
            Ok(s) => {
                stories.insert(name.clone(), s);
            }
            Err(why) => warnings.push(format!(
                "the story for `{name}` is not used, so its rows are grouped instead: {why}"
            )),
        }
    }
    (stories, warnings)
}

fn cell(v: Option<&Value>) -> Option<String> {
    match v? {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

fn is_decimal(s: &str) -> bool {
    let d = s.strip_prefix('-').unwrap_or(s);
    !d.is_empty() && d.bytes().all(|b| b.is_ascii_digit())
}

/// One row's sentence. `row` maps each column the story reads to its value as stored.
pub fn tell(story: &Story, row: &BTreeMap<String, Option<String>>, ctx: &NestContext) -> String {
    let get = |name: &str| row.get(name).cloned().flatten();
    let mut out = String::new();
    for p in &story.pieces {
        match p {
            Piece::Text(t) => out.push_str(t),
            Piece::Value(name) => {
                let Some(v) = get(name) else {
                    out.push_str("(none)");
                    continue;
                };
                let (ty, st) = &story.types[name];
                if holds_address(ty, st) {
                    out.push_str(&ctx.address_text(&v.to_ascii_lowercase()));
                } else {
                    out.extend(v.escape_debug());
                }
            }
            Piece::Amount(name, token) => {
                let Some(v) = get(name) else {
                    out.push_str("(none)");
                    continue;
                };
                if !is_decimal(&v) {
                    out.push_str(&format!("{} (not an integer)", v.escape_debug()));
                    continue;
                }
                let at = match token {
                    Token::Emitter => get("address"),
                    Token::Address(a) => Some(a.clone()),
                    Token::Column(c) => get(c),
                };
                match at {
                    Some(a) => out.push_str(&ctx.amount_text(&a.to_ascii_lowercase(), &v)),
                    None => out.push_str(&diff::amount_in(&v, None, "an unnamed token")),
                }
            }
        }
    }
    out
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToldBy {
    Story,
    Grouped,
}

#[derive(Debug, Serialize)]
pub struct SentenceOut {
    pub block: u64,
    pub log_index: u64,
    pub text: String,
}

#[derive(Debug, Serialize)]
pub struct ValueRows {
    pub value: String,
    pub rows: u64,
    /// The value as text prints it: an address with its alias and labels.
    #[serde(skip)]
    pub shown: String,
}

#[derive(Debug, Serialize)]
pub struct GroupOut {
    pub parameter: String,
    pub sol_type: String,
    pub distinct: u64,
    /// The values with the most rows, most first, ties by value; at most [`GROUP_TOP`].
    pub top: Vec<ValueRows>,
}

/// One table the flow does not read, as the diff tells it.
#[derive(Debug, Serialize)]
pub struct EventsOut {
    pub table: String,
    /// The event's signature, or the table's kind (`call`, `block`) when it is not an event.
    pub event: String,
    pub rows: u64,
    pub first_block: u64,
    pub last_block: u64,
    pub emitters: u64,
    pub told_by: ToldBy,
    /// The first `top` rows in chain order, for a table with a story.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sentences: Vec<SentenceOut>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<GroupOut>,
}

/// Everything the diff says about the tables outside the flow.
#[derive(Debug, Default, Serialize)]
pub struct Events {
    pub tables: Vec<EventsOut>,
    /// Rows told one sentence each by a story line.
    pub rows_told: u64,
    pub story_warnings: Vec<String>,
}

fn quote(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn kind_name(t: &TableSchema) -> String {
    match t.kind {
        TableKind::Event => t.event.clone(),
        TableKind::Call => "call".into(),
        TableKind::Block => "block".into(),
        TableKind::State => "state".into(),
    }
}

fn number(v: Option<&Value>) -> Option<u64> {
    match v? {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

async fn rows<S: Source>(src: &S, table: &str, sql: &str) -> Result<Vec<Value>> {
    let out = src
        .query(sql)
        .await
        .with_context(|| format!("reading {table}"))?;
    diff::complete(&out, table)?;
    Ok(out.rows)
}

/// What [`read`] tells: the counts the flow left as other events, and the story lines to tell them by.
pub struct Plan<'a> {
    pub schema: &'a [TableSchema],
    pub classified: &'a Classified,
    pub other: &'a BTreeMap<String, u64>,
    pub stories: &'a BTreeMap<String, Story>,
    pub warnings: Vec<String>,
}

/// Read every table the flow left as other events: a story's first `top` rows, or each indexed
/// parameter's distinct count and most frequent values. Each table's rows are counted again here and
/// must equal the flow's count, so every row in range is still accounted for exactly once.
pub async fn read<S: Source>(
    src: &S,
    plan: Plan<'_>,
    req: &Request,
    top: usize,
    ctx: &NestContext,
) -> Result<Events> {
    let by_name: BTreeMap<&str, &TableSchema> =
        plan.schema.iter().map(|t| (t.table.as_str(), t)).collect();
    let mut events = Events {
        story_warnings: plan.warnings,
        ..Default::default()
    };
    for (table, &counted) in plan.other {
        if counted == 0 {
            continue;
        }
        let t = by_name
            .get(table.as_str())
            .with_context(|| format!("{table} is counted but has no schema"))?;
        let mut filter = diff::filter(req);
        // The listed wrapper's rows of a split table were read as flow; only the rest are here.
        if let Some(f) = plan
            .classified
            .flow
            .iter()
            .find(|f| &f.table == table && !f.emitters.is_empty())
        {
            let list: Vec<String> = f.emitters.iter().map(|a| format!("'{a}'")).collect();
            filter.push_str(&format!(" AND lower(address) NOT IN ({})", list.join(", ")));
        }
        let from = quote(table);
        let stats = rows(
            src,
            table,
            &format!(
                "SELECT count(*) AS n, count(DISTINCT address) AS e, min(block_number) AS b0, \
                 max(block_number) AS b1 FROM {from} WHERE {filter}"
            ),
        )
        .await?;
        let s = stats.first();
        let n = number(s.and_then(|r| r.get("n"))).unwrap_or(0);
        if n != counted {
            bail!(
                "{table}: counted {counted} rows in range but grouped {n}; the nest moved under \
                 the read, run it again"
            );
        }
        let mut out = EventsOut {
            table: table.clone(),
            event: kind_name(t),
            rows: n,
            first_block: number(s.and_then(|r| r.get("b0"))).unwrap_or(0),
            last_block: number(s.and_then(|r| r.get("b1"))).unwrap_or(0),
            emitters: number(s.and_then(|r| r.get("e"))).unwrap_or(0),
            told_by: ToldBy::Grouped,
            sentences: Vec::new(),
            groups: Vec::new(),
        };
        if let Some(story) = plan.stories.get(table.as_str()) {
            let cols: Vec<&String> = story.types.keys().collect();
            let select: Vec<String> = cols
                .iter()
                .enumerate()
                .map(|(i, c)| format!("CAST({} AS VARCHAR) AS c{i}", quote(c)))
                .collect();
            let got = rows(
                src,
                table,
                &format!(
                    "SELECT block_number, log_index, {} FROM {from} WHERE {filter} \
                     ORDER BY block_number, log_index LIMIT {top}",
                    select.join(", ")
                ),
            )
            .await?;
            for r in &got {
                let row: BTreeMap<String, Option<String>> = cols
                    .iter()
                    .enumerate()
                    .map(|(i, c)| (c.to_string(), cell(r.get(format!("c{i}")))))
                    .collect();
                out.sentences.push(SentenceOut {
                    block: number(r.get("block_number")).unwrap_or(0),
                    log_index: number(r.get("log_index")).unwrap_or(0),
                    text: tell(story, &row, ctx),
                });
            }
            out.told_by = ToldBy::Story;
            events.rows_told += n;
        } else {
            for c in t.columns.iter().filter(|c| c.indexed) {
                out.groups
                    .push(group(src, table, &from, &filter, c, ctx).await?);
            }
        }
        events.tables.push(out);
    }
    events
        .tables
        .sort_by(|a, b| b.rows.cmp(&a.rows).then(a.table.cmp(&b.table)));
    Ok(events)
}

async fn group<S: Source>(
    src: &S,
    table: &str,
    from: &str,
    filter: &str,
    c: &crate::registry::ColumnSchema,
    ctx: &NestContext,
) -> Result<GroupOut> {
    let p = quote(&c.name);
    let distinct = rows(
        src,
        table,
        &format!("SELECT count(DISTINCT {p}) AS d FROM {from} WHERE {filter}"),
    )
    .await?;
    let tops = rows(
        src,
        table,
        &format!(
            "SELECT CAST({p} AS VARCHAR) AS v, count(*) AS n FROM {from} WHERE {filter} \
             GROUP BY {p} ORDER BY n DESC, v LIMIT {GROUP_TOP}"
        ),
    )
    .await?;
    let top = tops
        .iter()
        .map(|r| {
            let value = cell(r.get("v")).unwrap_or_default();
            let shown = if c.sol_type == "address" {
                ctx.address_text(&value.to_ascii_lowercase())
            } else {
                value.clone()
            };
            ValueRows {
                value,
                rows: number(r.get("n")).unwrap_or(0),
                shown,
            }
        })
        .collect();
    Ok(GroupOut {
        parameter: c.name.clone(),
        sol_type: c.sol_type.clone(),
        distinct: number(distinct.first().and_then(|r| r.get("d"))).unwrap_or(0),
        top,
    })
}

fn rows_text(n: u64) -> String {
    format!("{} {}", diff::group(n), if n == 1 { "row" } else { "rows" })
}

/// The text of the events section, below the flow.
pub fn text(events: &Events, top: usize) -> String {
    use std::fmt::Write as _;
    let mut o = String::new();
    let total: u64 = events.tables.iter().map(|t| t.rows).sum();
    if events.tables.is_empty() {
        let _ = writeln!(o, "Other events in range: none.");
    } else {
        let n = events.tables.len();
        let _ = writeln!(
            o,
            "Other events in range, not part of the flow: {} in {} {}, {} of them told by a story \
             line.",
            rows_text(total),
            diff::group(n),
            if n == 1 { "table" } else { "tables" },
            diff::group(events.rows_told)
        );
    }
    for t in &events.tables {
        let span = if t.first_block == t.last_block {
            format!("block {}", diff::group(t.first_block))
        } else {
            format!(
                "blocks {} to {}",
                diff::group(t.first_block),
                diff::group(t.last_block)
            )
        };
        let _ = writeln!(
            o,
            "  {}, {}: {} in {span}, from {} {}",
            t.table,
            t.event,
            rows_text(t.rows),
            diff::group(t.emitters),
            if t.emitters == 1 {
                "contract"
            } else {
                "contracts"
            }
        );
        for s in &t.sentences {
            let _ = writeln!(
                o,
                "    block {} log {}: {}",
                diff::group(s.block),
                s.log_index,
                s.text
            );
        }
        let told = t.sentences.len() as u64;
        if t.told_by == ToldBy::Story && t.rows > told {
            let more = t.rows - told;
            let _ = writeln!(
                o,
                "    {} more {} not told here (only the first {top} in chain order are)",
                diff::group(more),
                if more == 1 { "row" } else { "rows" }
            );
        }
        for g in &t.groups {
            // A value on one row only means every value is: naming three of them says nothing.
            if g.top.first().is_some_and(|v| v.rows == 1) {
                let _ = writeln!(
                    o,
                    "    {}: {} distinct, one row each",
                    g.parameter,
                    diff::group(g.distinct)
                );
                continue;
            }
            let named: Vec<String> = g
                .top
                .iter()
                .map(|v| format!("{} ({})", v.shown, rows_text(v.rows)))
                .collect();
            let _ = writeln!(
                o,
                "    {}: {} distinct; most rows {}",
                g.parameter,
                diff::group(g.distinct),
                named.join(", ")
            );
        }
    }
    for w in &events.story_warnings {
        let _ = writeln!(o, "Story line not used: {w}");
    }
    o
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::ColumnSchema;
    use crate::semantic::TableSemantic;

    const USDC: &str = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
    const OWNER: &str = "0x00000000000000000000000000000000000000aa";
    const POOL: &str = "0x00000000000000000000000000000000000000bb";
    const U256_MAX: &str =
        "115792089237316195423570985008687907853269984665640564039457584007913129639935";

    fn col(name: &str, ty: &str, indexed: bool) -> ColumnSchema {
        ColumnSchema {
            name: name.into(),
            sol_type: ty.into(),
            storage: "string".into(),
            indexed,
            components: Vec::new(),
        }
    }

    fn event(name: &str, params: Vec<ColumnSchema>) -> TableSchema {
        let mut columns = crate::registry::implicit_columns(true);
        columns.extend(params);
        TableSchema {
            table: name.into(),
            alias: "vault".into(),
            kind: TableKind::Event,
            event: "E()".into(),
            topic0: "0x01".into(),
            function: String::new(),
            selector: String::new(),
            columns,
        }
    }

    /// `Funded(address indexed to, address indexed pool, address token, uint256 amount, bytes32 tag)`:
    /// the recipient sits first, so a positional binding would read it as the payer.
    fn funded() -> TableSchema {
        event(
            "vault__funded",
            vec![
                col("to", "address", true),
                col("pool", "address", true),
                col("token", "address", false),
                col("amount", "uint256", false),
                col("tag", "bytes32", false),
            ],
        )
    }

    fn sem(story: &str) -> Semantic {
        let mut s = Semantic::default();
        s.tables.insert(
            "vault__funded".into(),
            TableSemantic {
                story: story.into(),
                ..Default::default()
            },
        );
        s
    }

    fn ctx() -> NestContext {
        let mut metadata = crate::metadata::MetadataCache::new();
        metadata.insert(
            USDC.into(),
            crate::metadata::TokenMetadata {
                decimals: Some(6),
                symbol: Some("USDC".into()),
                name: None,
            },
        );
        let mut ctx = NestContext {
            metadata,
            ..Default::default()
        };
        ctx.aliases.insert(POOL.into(), "pool".into());
        ctx
    }

    fn told(story: &str, pairs: &[(&str, &str)]) -> String {
        let (stories, warnings) = compile(&[funded()], &sem(story));
        assert!(warnings.is_empty(), "{warnings:?}");
        let row = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), Some(v.to_string())))
            .collect();
        tell(&stories["vault__funded"], &row, &ctx())
    }

    #[test]
    fn placeholders_parse_and_braces_escape() {
        assert_eq!(
            parse("{{x}} {pool} sent {amount:amount@token}").unwrap(),
            vec![
                Piece::Text("{x} ".into()),
                Piece::Value("pool".into()),
                Piece::Text(" sent ".into()),
                Piece::Amount("amount".into(), Token::Column("token".into())),
            ]
        );
        assert_eq!(
            parse("{amount:amount@0xA0b86991c6218b36c1d19d4a2e9eb0ce3606eB48}").unwrap(),
            vec![Piece::Amount("amount".into(), Token::Address(USDC.into()))]
        );
        assert_eq!(
            parse("{amount:amount}").unwrap(),
            vec![Piece::Amount("amount".into(), Token::Emitter)]
        );
        for (bad, why) in [
            ("{pool", "never closed"),
            ("pool}", "a `}` with no `{`"),
            ("{amount:usd}", "the only format is `amount`"),
            ("{amount:amount@0x12}", "not a 20-byte address"),
            ("{}", "does not start with a column name"),
        ] {
            let e = parse(bad).unwrap_err();
            assert!(e.contains(why), "{bad}: {e}");
        }
    }

    #[test]
    fn a_story_binds_by_name_not_position() {
        let s = told("{pool} paid {to}", &[("pool", POOL), ("to", OWNER)]);
        assert_eq!(s, format!("{POOL} [pool] paid {OWNER}"));
    }

    #[test]
    fn amounts_are_exact_with_decimals_and_base_units_without() {
        let pairs = [
            ("amount", "1250000000001"),
            ("token", USDC),
            ("address", POOL),
        ];
        assert_eq!(
            told("{amount:amount@token}", &pairs),
            "1,250,000.000001 USDC"
        );
        assert_eq!(
            told(&format!("{{amount:amount@{USDC}}}"), &pairs),
            "1,250,000.000001 USDC"
        );
        // The emitter is the vault, whose decimals no metadata.json holds: no guess at 18.
        assert_eq!(
            told("{amount:amount}", &pairs),
            "1,250,000,000,001 base units of pool (decimals unknown)"
        );
        assert_eq!(
            told(
                "{amount:amount@token}",
                &[("amount", U256_MAX), ("token", USDC)]
            ),
            "115,792,089,237,316,195,423,570,985,008,687,907,853,269,984,665,640,564,039,457,584,\
             007,913,129.639935 USDC"
        );
        assert_eq!(told("{amount}", &pairs), "1250000000001");
    }

    #[test]
    fn a_story_that_does_not_fit_is_named_and_not_used() {
        for (story, why) in [
            ("{nobody} paid", "`nobody` isn't a column of that table"),
            (
                "{tag:amount}",
                "`{tag:amount}` needs an integer parameter, and `tag` is bytes32",
            ),
            (
                "{amount:amount@tag}",
                "`@tag` names the token, so it must be an address column, and `tag` is bytes32",
            ),
            ("{amount:amount@ghost}", "`ghost` isn't a column"),
            ("{pool", "never closed"),
        ] {
            let (stories, warnings) = compile(&[funded()], &sem(story));
            assert!(stories.is_empty(), "{story}");
            assert_eq!(warnings.len(), 1, "{story}: {warnings:?}");
            assert!(
                warnings[0].starts_with("the story for `vault__funded` is not used")
                    && warnings[0].contains(why),
                "{story}: {}",
                warnings[0]
            );
            // `dev` prints the same warning through the semantic drift check.
            let drift = crate::semantic::drift(&[funded()], &sem(story));
            assert!(drift.contains(&warnings[0]), "{drift:?}");
        }
        let (stories, warnings) = compile(&[funded()], &sem("{pool} paid {amount:amount@to}"));
        assert!(warnings.is_empty() && stories.len() == 1, "{warnings:?}");
    }

    #[test]
    fn text_values_cannot_break_the_line() {
        let s = told("tagged {tag}", &[("tag", "a\nb\u{1b}")]);
        assert_eq!(s, "tagged a\\nb\\u{1b}");
    }

    /// Serves a stats row per table, 5 rows for `b__e` and 7 for any other, and nothing else.
    struct Counts;

    impl Source for Counts {
        async fn query(&self, sql: &str) -> Result<crate::analytics::QueryOutput> {
            let n = if sql.contains("\"b__e\"") { 5 } else { 7 };
            let rows = if sql.contains("count(*) AS n") {
                vec![serde_json::json!({ "n": n, "e": 1, "b0": 1, "b1": 1 })]
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
                ipc: None,
            })
        }
    }

    /// Most rows first, ties by name; and a count that differs from the flow's is refused, not told.
    #[tokio::test]
    async fn tables_are_ordered_by_rows_then_name_and_counts_must_agree() {
        let schema = [
            event("a__e", Vec::new()),
            event("b__e", Vec::new()),
            event("c__e", Vec::new()),
        ];
        let other: BTreeMap<String, u64> = [("c__e", 7), ("b__e", 5), ("a__e", 7)]
            .map(|(t, n)| (t.to_string(), n))
            .into();
        let classified = Classified::default();
        let stories = BTreeMap::new();
        let req = Request {
            from: 1,
            to: 1,
            tx: None,
        };
        let plan = |other| Plan {
            schema: &schema,
            classified: &classified,
            other,
            stories: &stories,
            warnings: Vec::new(),
        };
        let ev = read(&Counts, plan(&other), &req, 25, &ctx()).await.unwrap();
        let order: Vec<&str> = ev.tables.iter().map(|t| t.table.as_str()).collect();
        assert_eq!(order, ["a__e", "c__e", "b__e"]);

        let mut moved = other.clone();
        moved.insert("b__e".into(), 6);
        let e = read(&Counts, plan(&moved), &req, 25, &ctx())
            .await
            .unwrap_err()
            .to_string();
        assert!(
            e.contains("b__e: counted 6 rows in range but grouped 5"),
            "{e}"
        );
    }
}
