//! RFC-0041 §4 step 2: binding a [`Plan`] to the live decode registry (#870).
//!
//! This is the half of §5.1 the circuit cannot supply. [`entity_circuit`](crate::entity_circuit)
//! turns a plan into operators over [`Row`]s; nothing yet said *which* decoded values become those
//! rows, in what order, and what happens when an entity names a column the nest's ABI does not have.
//!
//! Two jobs, and the second exists because of the first:
//!
//! 1. **Refuse at load.** An entity naming an unknown table or an unknown column is rejected when
//!    the nest starts, not at whatever block first happens to produce that table. A validation that
//!    only fires on live data is not a validation; it is a delayed outage.
//! 2. **Convert a window.** §5.1: *"each decoded window is converted to the circuit's input
//!    relations and applied at weight `+1`"*. [`Binding::window`] is that conversion.
//!
//! ## Column order is the registry's, not the author's
//!
//! A plan's `Expr::Column(i)` indexes the concatenation of the left source's declared columns and
//! then the right's. Which *decoded* value each of those is comes from
//! [`DecodeRegistry::schema`] - implicit columns (`block_number`, `tx_hash`, `_seq` and the rest)
//! first, then the event's own parameters, which is the same order `/tables`, `schema.json` and the
//! MCP schema tool report. There is deliberately no second opinion about a table's shape.

use crate::entity_expr::{Expr, Type};
use crate::entity_offchain::{
    table_of, Column as OffchainColumn, NoOffchain, OffchainSnapshot, OffchainTables,
    OFFCHAIN_NAMESPACE,
};
use crate::entity_plan::{Agg, Plan, Source};
use crate::entity_row::{Row, Scalar};
use alloy_primitives::{I256, U256};
use anyhow::{anyhow, bail, Context, Result};
use nuthatch_decode::registry::{DecodeRegistry, DecodedRow, TableSchema, Value};

/// Where one of a plan's columns comes from in a decoded row.
///
/// Resolved once, at load. The alternative - looking a column up by name for every row of every
/// window - puts a hash lookup per column per row in the ingest path, which is the sort of thing
/// that does not show up in a unit test and does show up in the backfill benchmark.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Extract {
    BlockNumber,
    BlockHash,
    BlockTimestamp,
    TxHash,
    LogIndex,
    Address,
    Seq,
    /// The event's own parameter at this index. Decoded parameters are pushed in decoder-column
    /// order, so the index is stable - and [`Binding::value`] checks the name anyway, because a
    /// silently swapped column is the failure this whole module exists to prevent.
    Param(usize, String),
}

/// Where a bound source's rows come from.
#[derive(Clone, Debug)]
enum Columns {
    /// Decoded windows, through the registry.
    Chain(Vec<Extract>),
    /// Retained offchain snapshots (#1437), fed whole and append-only, never as windows.
    Offchain(Vec<OffchainColumn>),
}

/// One input relation, bound.
#[derive(Clone, Debug)]
pub struct BoundSource {
    pub table: String,
    columns: Columns,
    /// Each column's type as the circuit will see it, or `None` for one that may be NULL, for
    /// [`Binding::check_types`]. A decoded value is never NULL; an offchain cell may be.
    types: Vec<Option<Type>>,
}

impl BoundSource {
    /// How many columns this source contributes to the joined row.
    pub fn width(&self) -> usize {
        match &self.columns {
            Columns::Chain(c) => c.len(),
            Columns::Offchain(c) => c.len(),
        }
    }

    /// The offchain table this source reads, if it is an offchain source.
    pub fn offchain_table(&self) -> Option<&str> {
        match self.columns {
            Columns::Offchain(_) => table_of(&self.table),
            Columns::Chain(_) => None,
        }
    }
}

/// A plan bound to a registry: both sources resolved, every column index checked.
#[derive(Clone, Debug)]
pub struct Binding {
    pub left: BoundSource,
    pub right: Option<BoundSource>,
}

impl Binding {
    /// Resolve `plan` against `registry`, or refuse. A nest with no offchain tables binds this way.
    pub fn bind(plan: &Plan, registry: &DecodeRegistry) -> Result<Self> {
        Self::bind_with_offchain(plan, registry, &NoOffchain)
    }

    /// Resolve `plan` against `registry` and the nest's offchain snapshots, or refuse. The registry
    /// refuses any alias that would decode into `offchain__`, so the namespace alone decides the
    /// kind of source.
    pub fn bind_with_offchain(
        plan: &Plan,
        registry: &DecodeRegistry,
        offchain: &dyn OffchainTables,
    ) -> Result<Self> {
        let schema = registry.schema();
        let bind = |source: &Source| match table_of(&source.table) {
            Some(table) => bind_offchain(source, table, offchain),
            None => bind_source(source, &schema),
        };
        let left = bind(&plan.left)?;
        let right = plan.join.as_ref().map(|j| bind(&j.right)).transpose()?;

        let binding = Binding { left, right };
        binding.check_indices(plan)?;
        binding.check_types(plan)?;
        Ok(binding)
    }

    /// The width of the joined row: left columns then right columns.
    pub fn width(&self) -> usize {
        self.left.width() + self.right.as_ref().map_or(0, BoundSource::width)
    }

    /// Every column index a plan uses must exist in the row that expression will actually see, and
    /// the three kinds of expression see three different rows.
    ///
    /// Out of range, [`Row::get`] returns `Scalar::Null` rather than panicking - deliberately, so a
    /// bad plan cannot take the circuit thread down mid-window. That makes an unchecked index read
    /// as "this delegation had no amount" instead of as an error, which is exactly the kind of
    /// plausible wrong number an indexer must never produce. Hence checking here, at load.
    fn check_indices(&self, plan: &Plan) -> Result<()> {
        let left = self.left.width();
        let joined = self.width();

        if let Some(f) = &plan.left_filter {
            check_expr(f, left, "the left filter", &plan.left.table)?;
        }
        if let (Some(join), Some(right)) = (&plan.join, &self.right) {
            if let Some(f) = &join.right_filter {
                check_expr(f, right.width(), "the right filter", &join.right.table)?;
            }
            if join.on.0 >= left {
                bail!(
                    "the join reads column {} of {}, which has {left} columns",
                    join.on.0,
                    plan.left.table
                );
            }
            if join.on.1 >= right.width() {
                bail!(
                    "the join reads column {} of {}, which has {} columns",
                    join.on.1,
                    join.right.table,
                    right.width()
                );
            }
        }
        for e in &plan.key {
            check_expr(e, joined, "the grouping key", "the joined row")?;
        }
        for a in &plan.aggregates {
            match a {
                Agg::Count => {}
                Agg::Sum(e) | Agg::Min(e) | Agg::Max(e) | Agg::Avg(e) => {
                    check_expr(e, joined, "an aggregate", "the joined row")?
                }
            }
        }
        Ok(())
    }

    /// The type of each output column, key then aggregates, as the circuit fills it (#1598): counts,
    /// sums and averages are integers, a key or a MIN/MAX is its expression's type when that is
    /// certain, and `None` when it is not.
    pub fn output_types(&self, plan: &Plan) -> Vec<Option<Type>> {
        let joined: Vec<Option<Type>> = self
            .left
            .types
            .iter()
            .chain(self.right.iter().flat_map(|r| r.types.iter()))
            .copied()
            .collect();
        let of = |e: &Expr| e.definite_type(&joined).ok().flatten();
        plan.key
            .iter()
            .map(of)
            .chain(plan.aggregates.iter().map(|a| match a {
                Agg::Count | Agg::Sum(_) | Agg::Avg(_) => Some(Type::Int),
                Agg::Min(e) | Agg::Max(e) => of(e),
            }))
            .collect()
    }

    /// Every expression the plan evaluates must type-check against the columns it will see, by the
    /// rules [`Expr::eval`] enforces per row (#1590). The SQL engine's binder can coerce where the
    /// circuit does not, so `check` passing through the engine alone let `fee + '1'` through to fault
    /// on its first row.
    fn check_types(&self, plan: &Plan) -> Result<()> {
        let left = &self.left.types;
        let joined: Vec<Option<Type>> = left
            .iter()
            .chain(self.right.iter().flat_map(|r| r.types.iter()))
            .copied()
            .collect();
        // `admits` keeps a row only on TRUE, and faults on anything but a boolean or NULL.
        let filter = |f: &Expr, cols: &[Option<Type>], what: &str| -> Result<()> {
            match f
                .definite_type(cols)
                .with_context(|| format!("{what} does not type-check"))?
            {
                Some(t) if t != Type::Bool => bail!("{what} must be a condition, but it is {t:?}"),
                _ => Ok(()),
            }
        };
        if let Some(f) = &plan.left_filter {
            filter(f, left, "the filter")?;
        }
        if let (Some(join), Some(right)) = (&plan.join, &self.right) {
            if let Some(f) = &join.right_filter {
                filter(f, &right.types, "the joined table's filter")?;
            }
        }
        for e in &plan.key {
            e.definite_type(&joined)
                .context("a grouping key does not type-check")?;
        }
        for a in &plan.aggregates {
            match a {
                Agg::Count => {}
                Agg::Min(e) | Agg::Max(e) => {
                    e.definite_type(&joined)
                        .context("an aggregate does not type-check")?;
                }
                Agg::Sum(e) | Agg::Avg(e) => {
                    let t = e
                        .definite_type(&joined)
                        .context("an aggregate does not type-check")?;
                    if let Some(t) = t.filter(|t| *t != Type::Int) {
                        bail!("SUM and AVG need integers, got {t:?}");
                    }
                }
            }
        }
        Ok(())
    }

    /// **§5.1.** Split a decoded window into the circuit's two input relations, in the plan's own
    /// column order. Rows belonging to neither source are ignored: a window carries every table the
    /// nest decodes, and an entity reads one or two of them.
    ///
    /// A row that matches a source's table but cannot be converted is an error, not a skip. It means
    /// the registry and the binding disagree about that table, and carrying on would put a NULL
    /// where a value should be.
    pub fn window(&self, rows: &[DecodedRow]) -> Result<(Vec<Row>, Vec<Row>)> {
        let mut left = Vec::new();
        let mut right = Vec::new();
        for row in rows {
            if row.table == self.left.table {
                left.push(self.row(&self.left, row)?);
            }
            // Not `else if`: an entity may legitimately join a table to itself, and dropping the
            // second copy would silently halve the join.
            if let Some(bound) = &self.right {
                if row.table == bound.table {
                    right.push(self.row(bound, row)?);
                }
            }
        }
        Ok((left, right))
    }

    /// The offchain counterpart of [`Self::window`]: one snapshot of `table` as the circuit's two
    /// input relations. Only a side reading that table gets rows; both do when it joins itself.
    pub fn snapshot(
        &self,
        table: &str,
        snapshot: &dyn OffchainSnapshot,
    ) -> Result<(Vec<Row>, Vec<Row>)> {
        let side = |bound: &BoundSource| -> Result<Vec<Row>> {
            match &bound.columns {
                Columns::Offchain(columns) if bound.offchain_table() == Some(table) => snapshot
                    .rows(columns)
                    .with_context(|| format!("converting a snapshot of {}", bound.table)),
                _ => Ok(Vec::new()),
            }
        };
        let left = side(&self.left)?;
        let right = self
            .right
            .as_ref()
            .map(side)
            .transpose()?
            .unwrap_or_default();
        Ok((left, right))
    }

    /// The offchain tables this binding reads, left then right.
    pub fn offchain_tables(&self) -> Vec<&str> {
        std::iter::once(&self.left)
            .chain(self.right.as_ref())
            .filter_map(BoundSource::offchain_table)
            .collect()
    }

    fn row(&self, bound: &BoundSource, row: &DecodedRow) -> Result<Row> {
        let Columns::Chain(columns) = &bound.columns else {
            bail!(
                "{} is an offchain source; it is fed snapshots, not decoded windows",
                bound.table
            )
        };
        columns
            .iter()
            .map(|e| self.value(e, row))
            .collect::<Result<Vec<_>>>()
            .map(Row)
            .with_context(|| format!("converting a row of {}", bound.table))
    }

    fn value(&self, extract: &Extract, row: &DecodedRow) -> Result<Scalar> {
        Ok(match extract {
            Extract::BlockNumber => Scalar::Int(i128::from(row.block_number)),
            Extract::BlockHash => Scalar::Str(row.block_hash.clone()),
            Extract::BlockTimestamp => Scalar::Int(i128::from(row.block_timestamp)),
            Extract::TxHash => Scalar::Str(row.tx_hash.clone()),
            Extract::LogIndex => Scalar::Int(i128::from(row.log_index)),
            Extract::Address => Scalar::Str(row.address.clone()),
            Extract::Seq => Scalar::Int(i128::from(row.seq())),
            Extract::Param(i, name) => {
                let (got, value) = row.params.get(*i).ok_or_else(|| {
                    anyhow!(
                        "{} has no parameter {i} ({name}); the registry and the binding disagree \
                         about this table",
                        row.table
                    )
                })?;
                if got != name {
                    bail!(
                        "{} parameter {i} is {got}, not {name}; the registry and the binding \
                         disagree about this table's column order",
                        row.table
                    );
                }
                scalar(value)?
            }
        })
    }
}

fn bind_source(source: &Source, schema: &[TableSchema]) -> Result<BoundSource> {
    let table =
        resolve(schema, |t| t.table.as_str(), &source.table, "table")?.ok_or_else(|| {
            let mut known: Vec<&str> = schema.iter().map(|t| t.table.as_str()).collect();
            known.sort_unstable();
            anyhow!(
                "no table {} in this nest. Decoded tables: {}",
                source.table,
                known.join(", ")
            )
        })?;

    // The implicit columns come first, so a parameter's index among the parameters is its position
    // in the schema minus however many implicit columns this nest declares. A timestamp-free nest
    // has one fewer (RFC-0029 §6b), which is why this is counted rather than assumed.
    let implicit = table
        .columns
        .iter()
        .take_while(|c| c.sol_type == "implicit")
        .count();

    let columns = source
        .columns
        .iter()
        .map(|name| {
            let position = resolve(&table.columns, |c| c.name.as_str(), name, "column")?
                .map(|c| {
                    table
                        .columns
                        .iter()
                        .position(|x| std::ptr::eq(x, c))
                        .expect("resolved from this slice")
                })
                .ok_or_else(|| {
                    anyhow!(
                        "no column {name} in {}. Its columns are: {}",
                        source.table,
                        table
                            .columns
                            .iter()
                            .map(|c| c.name.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                })?;
            // The registry's spelling from here on: rows are matched and checked against it.
            let column = &table.columns[position];
            let extract = if position < implicit {
                implicit_extract(&column.name)?
            } else {
                Extract::Param(position - implicit, column.name.clone())
            };
            let ty = match extract {
                Extract::BlockNumber
                | Extract::BlockTimestamp
                | Extract::LogIndex
                | Extract::Seq => Type::Int,
                Extract::BlockHash | Extract::TxHash | Extract::Address => Type::Str,
                // As [`scalar`] converts them: every integer is an `i128`, a bool a bool, and
                // everything else the string the SQL surface shows.
                Extract::Param(..) => match column.storage.as_str() {
                    "u64" | "i64" | "word16" | "word32" => Type::Int,
                    "bool" => Type::Bool,
                    _ => Type::Str,
                },
            };
            Ok((extract, Some(ty)))
        })
        .collect::<Result<Vec<_>>>()?;
    let (columns, types) = columns.into_iter().unzip();

    Ok(BoundSource {
        table: table.table.clone(),
        columns: Columns::Chain(columns),
        types,
    })
}

/// The item named `wanted`: an exact match first, else the one whose name matches ignoring case, as
/// every SQL surface reads it (#1591). Two names that differ only in case are both what the SQL means,
/// so choosing one would bind an entity to rows it never asked for; that is refused.
fn resolve<'a, T>(
    items: &'a [T],
    name: impl Fn(&T) -> &str,
    wanted: &str,
    what: &str,
) -> Result<Option<&'a T>> {
    if let Some(exact) = items.iter().find(|i| name(i) == wanted) {
        return Ok(Some(exact));
    }
    let mut matches = items
        .iter()
        .filter(|i| name(i).eq_ignore_ascii_case(wanted));
    let Some(first) = matches.next() else {
        return Ok(None);
    };
    if let Some(other) = matches.find(|i| name(i) != name(first)) {
        bail!(
            "{what} {wanted} matches both {} and {}, which differ only in case. Name it exactly as \
             one of them is spelled",
            name(first),
            name(other)
        );
    }
    Ok(Some(first))
}

fn bind_offchain(
    source: &Source,
    table: &str,
    offchain: &dyn OffchainTables,
) -> Result<BoundSource> {
    let Some(columns) = offchain.bind(table, &source.table, &source.columns)? else {
        let known: Vec<String> = offchain
            .names()
            .iter()
            .map(|t| format!("{OFFCHAIN_NAMESPACE}{t}"))
            .collect();
        bail!(
            "no offchain table {} in this nest. Offchain tables: {}",
            source.table,
            if known.is_empty() {
                "none".into()
            } else {
                known.join(", ")
            }
        )
    };
    let types = columns.iter().map(|_| None).collect();
    Ok(BoundSource {
        table: source.table.clone(),
        columns: Columns::Offchain(columns),
        types,
    })
}

fn implicit_extract(name: &str) -> Result<Extract> {
    Ok(match name {
        "block_number" => Extract::BlockNumber,
        "block_hash" => Extract::BlockHash,
        "block_timestamp" => Extract::BlockTimestamp,
        "tx_hash" => Extract::TxHash,
        "log_index" => Extract::LogIndex,
        "address" => Extract::Address,
        "_seq" => Extract::Seq,
        other => bail!("{other} is declared implicit but is not one this binder knows"),
    })
}

/// The widest column index an expression reads, if it reads any.
fn max_column(e: &Expr) -> Option<usize> {
    match e {
        Expr::Column(i) => Some(*i),
        Expr::Literal(_) => None,
        Expr::Not(a) | Expr::IsNull(a) | Expr::Cast(a, _) => max_column(a),
        Expr::Add(a, b)
        | Expr::Sub(a, b)
        | Expr::Mul(a, b)
        | Expr::Compare(_, a, b)
        | Expr::And(a, b)
        | Expr::Or(a, b) => max_column(a).into_iter().chain(max_column(b)).max(),
        Expr::Coalesce(es) => es.iter().filter_map(max_column).max(),
        Expr::Case { whens, otherwise } => whens
            .iter()
            .flat_map(|(c, v)| [max_column(c), max_column(v)])
            .flatten()
            .chain(otherwise.as_deref().and_then(max_column))
            .max(),
    }
}

fn check_expr(e: &Expr, width: usize, what: &str, whose: &str) -> Result<()> {
    match max_column(e) {
        Some(i) if i >= width => bail!(
            "{what} reads column {i}, but {whose} has {width} column(s). An entity is refused at \
             load rather than reading NULL at the first block that would have used it."
        ),
        _ => Ok(()),
    }
}

/// One decoded value as one exact scalar.
///
/// The wide integer types narrow to `i128` **checked** (§3.3.1, #873): a `uint256` that does not fit
/// is an error here rather than a wrapped number in an entity. RFC-0041 §3.3 admits exact integer
/// and decimal arithmetic and refuses floating point, so there is nothing to fall back to and
/// nothing that would quietly lose precision instead.
fn scalar(value: &Value) -> Result<Scalar> {
    Ok(match value {
        // Rendered exactly as `Value::to_json` renders it, so an entity's key joins against the
        // same string the HTTP and SQL surfaces show. Two spellings of one address is the sort of
        // divergence that makes a correct join return nothing.
        Value::Address(a) => Scalar::Str(format!("0x{}", hex::encode(a))),
        Value::U64(n) => Scalar::Int(i128::from(*n)),
        Value::I64(n) => Scalar::Int(i128::from(*n)),
        Value::Word16(b) => Scalar::Int(narrow(u128::from_be_bytes(*b).to_string())?),
        Value::Word32(b) => Scalar::Int(narrow(U256::from_be_bytes::<32>(*b).to_string())?),
        Value::IWord16(b) => Scalar::Int(i128::from_be_bytes(*b)),
        Value::IWord32(b) => Scalar::Int(narrow(I256::from_be_bytes::<32>(*b).to_string())?),
        Value::Bool(b) => Scalar::Bool(*b),
        Value::Bytes(b) => Scalar::Str(format!("0x{}", hex::encode(b))),
        Value::Str(s) | Value::Json(s) => Scalar::Str(s.clone()),
        Value::Hash32(b) => Scalar::Str(format!("0x{}", hex::encode(b))),
    })
}

fn narrow(decimal: String) -> Result<i128> {
    decimal.parse::<i128>().map_err(|_| {
        anyhow!(
            "{decimal} does not fit i128. An entity faults rather than wrapping or truncating \
             (RFC-0041 §3.3.1)."
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity_circuit::EntityCircuit;
    use crate::entity_expr::Cmp;
    use crate::entity_plan::{Join, Source};
    use nuthatch_decode::registry::ContractSpec;
    use nuthatch_decode::rpc::Log;

    const ERC20: &str = r#"[
        {"type":"event","name":"Transfer","inputs":[
            {"name":"from","type":"address","indexed":true},
            {"name":"to","type":"address","indexed":true},
            {"name":"value","type":"uint256","indexed":false}],"anonymous":false},
        {"type":"event","name":"Approval","inputs":[
            {"name":"owner","type":"address","indexed":true},
            {"name":"spender","type":"address","indexed":true},
            {"name":"value","type":"uint256","indexed":false}],"anonymous":false}
    ]"#;

    const TRANSFER_TOPIC0: &str =
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
    const APPROVAL_TOPIC0: &str =
        "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925";
    const TOKEN: &str = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";

    fn registry() -> DecodeRegistry {
        let abi: alloy_json_abi::JsonAbi = serde_json::from_str(ERC20).unwrap();
        DecodeRegistry::build(vec![ContractSpec {
            alias: "usdc".into(),
            address: TOKEN.parse().unwrap(),
            abi,
            events: Vec::new(),
        }])
        .unwrap()
    }

    fn topic_addr(a: &str) -> String {
        format!("0x{:0>64}", a.trim_start_matches("0x"))
    }
    fn word(hex_digits: &str) -> String {
        format!("0x{:0>64}", hex_digits)
    }

    fn transfer(from: &str, to: &str, value: &str, block: u64, li: u64) -> Log {
        Log {
            address: TOKEN.into(),
            topics: vec![TRANSFER_TOPIC0.into(), topic_addr(from), topic_addr(to)],
            data: word(value),
            block_number: block,
            block_hash: "0xbh".into(),
            tx_hash: "0xtx".into(),
            log_index: li,
        }
    }

    fn approval(owner: &str, spender: &str, value: &str, block: u64, li: u64) -> Log {
        Log {
            address: TOKEN.into(),
            topics: vec![
                APPROVAL_TOPIC0.into(),
                topic_addr(owner),
                topic_addr(spender),
            ],
            data: word(value),
            block_number: block,
            block_hash: "0xbh".into(),
            tx_hash: "0xtx".into(),
            log_index: li,
        }
    }

    fn decode(reg: &DecodeRegistry, logs: &[Log]) -> Vec<DecodedRow> {
        logs.iter()
            .map(|l| {
                reg.decode(l)
                    .unwrap()
                    .unwrap_or_else(|| panic!("no decoder matched {}", l.topics[0]))
            })
            .collect()
    }

    const ALICE: &str = "0x1111111111111111111111111111111111111111";
    const BOB: &str = "0x2222222222222222222222222222222222222222";

    fn transfer_source(columns: &[&str]) -> Source {
        Source {
            table: "usdc__transfer".into(),
            columns: columns.iter().map(|c| c.to_string()).collect(),
        }
    }

    /// #1591: SQL identifiers are case-insensitive on every surface, and `check` binds through one,
    /// so the binder must be too, and must match rows by the registry's own spelling.
    #[test]
    fn a_table_and_column_bind_in_any_case() {
        let reg = registry();
        let plan = Plan {
            left: Source {
                table: "USDC__Transfer".into(),
                columns: vec!["TO".into(), "Value".into(), "BLOCK_NUMBER".into()],
            },
            left_filter: None,
            join: None,
            key: vec![Expr::Column(0)],
            aggregates: vec![Agg::Sum(Expr::Column(1))],
        };
        let binding = Binding::bind(&plan, &reg).unwrap();
        let rows = decode(&reg, &[transfer(ALICE, BOB, "64", 10, 0)]);
        let (left, _) = binding.window(&rows).unwrap();
        assert_eq!(
            left,
            vec![Row(vec![
                Scalar::Str(BOB.into()),
                Scalar::Int(100),
                Scalar::Int(10)
            ])]
        );
    }

    /// Two tables whose names differ only in case are both what a case-insensitive reference means, so
    /// binding one silently would read the wrong contract's rows. The exact spelling still binds.
    #[test]
    fn a_name_matching_two_tables_by_case_is_refused() {
        let abi: alloy_json_abi::JsonAbi = serde_json::from_str(ERC20).unwrap();
        let reg = DecodeRegistry::build(vec![
            ContractSpec {
                alias: "Token".into(),
                address: TOKEN.parse().unwrap(),
                abi: abi.clone(),
                events: Vec::new(),
            },
            ContractSpec {
                alias: "token".into(),
                address: "0x3333333333333333333333333333333333333333"
                    .parse()
                    .unwrap(),
                abi,
                events: Vec::new(),
            },
        ])
        .unwrap();
        let plan = |table: &str| Plan {
            left: Source {
                table: table.into(),
                columns: vec!["to".into(), "value".into()],
            },
            left_filter: None,
            join: None,
            key: vec![Expr::Column(0)],
            aggregates: vec![Agg::Sum(Expr::Column(1))],
        };
        let err = Binding::bind(&plan("TOKEN__transfer"), &reg)
            .unwrap_err()
            .to_string();
        assert!(err.contains("differ only in case"), "{err}");
        let bound = Binding::bind(&plan("token__transfer"), &reg).unwrap();
        assert_eq!(bound.left.table, "token__transfer");
    }

    /// **§5.1, end to end.** Logs decode through the real registry, the binding turns that window
    /// into the circuit's input relation, and the circuit produces the entity. No fixture query and
    /// no hardcoded table name anywhere between the log and the answer - which is the whole of what
    /// #870 said was missing.
    #[test]
    fn a_decoded_window_becomes_an_entity_through_the_binding_and_the_circuit() {
        let reg = registry();
        let plan = Plan {
            left: transfer_source(&["to", "value"]),
            left_filter: Some(Expr::Compare(
                Cmp::Gt,
                Expr::Column(1).into(),
                Expr::Literal(Scalar::Int(0)).into(),
            )),
            join: None,
            key: vec![Expr::Column(0)],
            aggregates: vec![Agg::Sum(Expr::Column(1))],
        };
        let binding = Binding::bind(&plan, &reg).unwrap();

        let rows = decode(
            &reg,
            &[
                transfer(ALICE, BOB, "64", 10, 0), // 100 to bob
                transfer(ALICE, BOB, "1", 10, 1),  // 1 to bob
                transfer(BOB, ALICE, "5", 11, 0),  // 5 to alice
                transfer(ALICE, BOB, "0", 11, 1),  // filtered: zero
                approval(ALICE, BOB, "ff", 11, 2), // a different table entirely
            ],
        );
        let (left, right) = binding.window(&rows).unwrap();
        assert!(right.is_empty(), "no join, so no right relation");
        assert_eq!(left.len(), 4, "the approval is not this entity's table");

        let mut circuit = EntityCircuit::build(plan.clone()).unwrap();
        let mut relation = crate::entity_plan::Relation::new();
        circuit
            .apply(
                &left.iter().map(|r| (r.clone(), 1)).collect::<Vec<_>>(),
                &[],
                &mut relation,
            )
            .unwrap();

        assert_eq!(
            relation.get(&Row(vec![Scalar::Str(BOB.into())])),
            Some(&Row(vec![Scalar::Int(101)])),
            "0x64 + 0x1, with the zero-value transfer filtered out"
        );
        assert_eq!(
            relation.get(&Row(vec![Scalar::Str(ALICE.into())])),
            Some(&Row(vec![Scalar::Int(5)]))
        );
        assert_eq!(
            relation,
            plan.evaluate(&left, &[]).unwrap(),
            "§8 still holds on rows that came from a real decode"
        );
    }

    /// `SELECT t.tier, SUM(x.value) FROM usdc__transfer x JOIN offchain__tiers t ON x.to = t.account
    /// GROUP BY t.tier`
    fn received_by_tier() -> Plan {
        Plan {
            left: transfer_source(&["to", "value"]),
            left_filter: None,
            join: Some(Join {
                right: Source {
                    table: "offchain__tiers".into(),
                    columns: vec!["account".into(), "tier".into()],
                },
                right_filter: None,
                on: (0, 0),
            }),
            key: vec![Expr::Column(3)],
            aggregates: vec![Agg::Sum(Expr::Column(1))],
        }
    }

    #[test]
    fn an_unknown_offchain_table_is_refused_at_load_and_names_the_ones_there_are() {
        let reg = registry();
        let err = format!(
            "{:#}",
            Binding::bind(&received_by_tier(), &reg).unwrap_err()
        );
        assert!(
            err.contains("no offchain table offchain__tiers")
                && err.contains("Offchain tables: none"),
            "{err}"
        );
    }

    /// The refusal that has to happen at load. An entity naming a column the ABI does not have is a
    /// typo or a stale schema, and finding out at the first block that would have used it means an
    /// indexer that started cleanly and then quietly produced nothing.
    #[test]
    fn an_unknown_column_is_refused_at_load_and_the_error_lists_the_real_ones() {
        let reg = registry();
        let plan = Plan {
            left: transfer_source(&["to", "amount"]),
            left_filter: None,
            join: None,
            key: vec![Expr::Column(0)],
            aggregates: vec![Agg::Count],
        };
        let err = format!("{:#}", Binding::bind(&plan, &reg).unwrap_err());
        assert!(err.contains("no column amount"), "{err}");
        assert!(
            err.contains("value"),
            "the error must name the real columns: {err}"
        );
    }

    #[test]
    fn an_unknown_table_is_refused_at_load_and_the_error_lists_the_real_ones() {
        let reg = registry();
        let plan = Plan {
            left: Source {
                table: "usdc__swap".into(),
                columns: vec!["to".into()],
            },
            left_filter: None,
            join: None,
            key: vec![Expr::Column(0)],
            aggregates: vec![Agg::Count],
        };
        let err = format!("{:#}", Binding::bind(&plan, &reg).unwrap_err());
        assert!(err.contains("no table usdc__swap"), "{err}");
        assert!(err.contains("usdc__transfer"), "{err}");
    }

    /// The implicit columns are part of a table's shape, and an entity may group by any of them.
    /// `_seq` in particular is derived, not decoded, so it is the one that proves the binder reads
    /// the row rather than the parameter list.
    #[test]
    fn implicit_columns_bind_to_the_rows_own_fields() {
        let reg = registry();
        let plan = Plan {
            left: transfer_source(&["block_number", "tx_hash", "_seq", "address", "value"]),
            left_filter: None,
            join: None,
            key: vec![Expr::Column(0)],
            aggregates: vec![Agg::Count],
        };
        let binding = Binding::bind(&plan, &reg).unwrap();
        let rows = decode(&reg, &[transfer(ALICE, BOB, "7", 12, 3)]);
        let (left, _) = binding.window(&rows).unwrap();

        assert_eq!(
            left[0],
            Row(vec![
                Scalar::Int(12),
                Scalar::Str("0xtx".into()),
                Scalar::Int((12 << 20) | 3),
                Scalar::Str(TOKEN.into()),
                Scalar::Int(7),
            ]),
            "implicit columns first, in the registry's order, then the parameters"
        );
    }

    /// `Row::get` reads a missing column as NULL rather than panicking, deliberately - a bad plan
    /// must not take the circuit thread down. That makes an out-of-range index read as a missing
    /// value, which is why it has to be refused here instead.
    #[test]
    fn a_column_index_past_the_bound_width_is_refused_rather_than_reading_null() {
        let reg = registry();
        let plan = Plan {
            left: transfer_source(&["to", "value"]),
            left_filter: Some(Expr::Compare(
                Cmp::Gt,
                Expr::Column(7).into(),
                Expr::Literal(Scalar::Int(0)).into(),
            )),
            join: None,
            key: vec![Expr::Column(0)],
            aggregates: vec![Agg::Count],
        };
        let err = format!("{:#}", Binding::bind(&plan, &reg).unwrap_err());
        assert!(err.contains("the left filter reads column 7"), "{err}");
        assert!(err.contains("2 column"), "{err}");
    }

    /// A filter on the right-hand source sees the *right* row, not the joined one. Checking it
    /// against the joined width would admit an index that reads NULL for every row it ever sees.
    #[test]
    fn a_right_filter_is_checked_against_its_own_side_not_the_joined_row() {
        let reg = registry();
        let plan = Plan {
            left: transfer_source(&["to", "value"]),
            left_filter: None,
            join: Some(Join {
                right: transfer_source(&["from"]),
                // Column 2 exists in the joined row and does not exist in the right row.
                right_filter: Some(Expr::Column(2)),
                on: (0, 0),
            }),
            key: vec![Expr::Column(0)],
            aggregates: vec![Agg::Count],
        };
        let err = format!("{:#}", Binding::bind(&plan, &reg).unwrap_err());
        assert!(err.contains("the right filter reads column 2"), "{err}");
    }

    /// §3.3.1 at the decode boundary. A `uint256` larger than `i128` cannot be an exact `Scalar::Int`
    /// and there is no float to fall back to, so it is an error rather than a truncation.
    #[test]
    fn a_uint256_too_large_for_i128_faults_rather_than_truncating() {
        let reg = registry();
        let plan = Plan {
            left: transfer_source(&["to", "value"]),
            left_filter: None,
            join: None,
            key: vec![Expr::Column(0)],
            aggregates: vec![Agg::Sum(Expr::Column(1))],
        };
        let binding = Binding::bind(&plan, &reg).unwrap();
        let huge = "f".repeat(64);
        let rows = decode(&reg, &[transfer(ALICE, BOB, &huge, 10, 0)]);

        let err = format!("{:#}", binding.window(&rows).unwrap_err());
        assert!(err.contains("does not fit i128"), "{err}");
        assert!(
            err.contains("usdc__transfer"),
            "the error must name the table: {err}"
        );
    }

    /// A timestamp-free nest (RFC-0029 §6b) declares one fewer implicit column, so every parameter
    /// sits one position earlier. Counting the implicit columns rather than assuming seven of them
    /// is what makes this work, and this is the only test that can tell the two apart.
    #[test]
    fn a_timestamp_free_nest_binds_parameters_one_column_earlier() {
        let reg = registry().with_timestamps(false);
        let plan = Plan {
            left: transfer_source(&["to", "value"]),
            left_filter: None,
            join: None,
            key: vec![Expr::Column(0)],
            aggregates: vec![Agg::Sum(Expr::Column(1))],
        };
        let binding = Binding::bind(&plan, &reg).unwrap();
        let rows = decode(&reg, &[transfer(ALICE, BOB, "9", 10, 0)]);
        let (left, _) = binding.window(&rows).unwrap();

        assert_eq!(
            left[0],
            Row(vec![Scalar::Str(BOB.into()), Scalar::Int(9)]),
            "the parameters are still `to` and `value`, not whatever sits one place along"
        );

        // And the column it does not declare is refused rather than read as NULL.
        let with_ts = Plan {
            left: transfer_source(&["block_timestamp"]),
            ..plan
        };
        let err = format!("{:#}", Binding::bind(&with_ts, &reg).unwrap_err());
        assert!(err.contains("no column block_timestamp"), "{err}");
    }

    /// The guard behind the bound parameter index. Binding by position is what keeps a name lookup
    /// out of the ingest path; checking the name as the value is read is what stops a registry that
    /// has since reordered its columns from feeding an entity the wrong one under the right name.
    #[test]
    fn a_parameter_that_has_moved_is_an_error_rather_than_the_wrong_column() {
        let reg = registry();
        let plan = Plan {
            left: transfer_source(&["to", "value"]),
            left_filter: None,
            join: None,
            key: vec![Expr::Column(0)],
            aggregates: vec![Agg::Sum(Expr::Column(1))],
        };
        let binding = Binding::bind(&plan, &reg).unwrap();

        let mut row = decode(&reg, &[transfer(ALICE, BOB, "9", 10, 0)]).remove(0);
        row.params.swap(1, 2);

        let err = format!("{:#}", binding.window(&[row]).unwrap_err());
        assert!(err.contains("disagree about this table"), "{err}");
    }

    /// A column index equal to the width is the one an off-by-one produces, and the one a test using
    /// a wildly out-of-range index cannot see.
    #[test]
    fn the_first_index_past_the_last_column_is_refused() {
        let reg = registry();
        let plan = Plan {
            left: transfer_source(&["to", "value"]),
            left_filter: None,
            join: None,
            // Columns 0 and 1 exist. Column 2 is the first that does not.
            key: vec![Expr::Column(2)],
            aggregates: vec![Agg::Count],
        };
        let err = format!("{:#}", Binding::bind(&plan, &reg).unwrap_err());
        assert!(err.contains("the grouping key reads column 2"), "{err}");
        assert!(err.contains("2 column"), "{err}");
    }

    /// Aggregates read the joined row and are checked like everything else. An unchecked aggregate
    /// index is the worst of the three: it does not refuse and it does not error, it sums NULLs and
    /// reports zero.
    #[test]
    fn an_aggregate_reading_a_column_that_does_not_exist_is_refused() {
        let reg = registry();
        let plan = Plan {
            left: transfer_source(&["to", "value"]),
            left_filter: None,
            join: None,
            key: vec![Expr::Column(0)],
            aggregates: vec![Agg::Sum(Expr::Column(5))],
        };
        let err = format!("{:#}", Binding::bind(&plan, &reg).unwrap_err());
        assert!(err.contains("an aggregate reads column 5"), "{err}");
    }

    /// The join reads one column from each side, and each index is into that side's own row.
    #[test]
    fn a_join_column_past_its_own_sides_width_is_refused() {
        let reg = registry();
        let plan = Plan {
            left: transfer_source(&["to"]),
            left_filter: None,
            join: Some(Join {
                right: transfer_source(&["from", "value"]),
                right_filter: None,
                // The left source has one column, so column 1 of it does not exist - even though
                // column 1 of the *joined* row does.
                on: (1, 0),
            }),
            key: vec![Expr::Column(0)],
            aggregates: vec![Agg::Count],
        };
        let err = format!("{:#}", Binding::bind(&plan, &reg).unwrap_err());
        assert!(
            err.contains("the join reads column 1 of usdc__transfer"),
            "{err}"
        );
        assert!(err.contains("1 columns"), "{err}");
    }

    /// An entity may join a table to itself. Feeding the row to only one side would silently halve
    /// the join, which is the sort of wrong answer that looks like a data problem.
    #[test]
    fn a_self_join_puts_each_row_on_both_sides() {
        let reg = registry();
        let plan = Plan {
            left: transfer_source(&["to", "value"]),
            left_filter: None,
            join: Some(Join {
                right: transfer_source(&["to"]),
                right_filter: None,
                on: (0, 0),
            }),
            key: vec![Expr::Column(0)],
            aggregates: vec![Agg::Count],
        };
        let binding = Binding::bind(&plan, &reg).unwrap();
        let rows = decode(&reg, &[transfer(ALICE, BOB, "1", 10, 0)]);
        let (left, right) = binding.window(&rows).unwrap();

        assert_eq!(left.len(), 1);
        assert_eq!(right.len(), 1, "the same row is both sides of a self-join");
    }
}
