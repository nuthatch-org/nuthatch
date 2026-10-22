//! The row-local rule (§4.2). A query that maps each source row to at most one output row, reading
//! only decoded tables, returns the same rows over one chunk or the whole history, which is what
//! makes chunked execution exact. Everything else is refused by name.

use std::collections::BTreeSet;
use std::fmt;
use std::ops::ControlFlow;

use sqlparser::ast::{
    Distinct, Expr, FunctionArguments, GroupByExpr, Ident, ObjectName, Query, Select, SelectItem,
    SetExpr, SetOperator, SetQuantifier, Statement, TableFactor, Visit, Visitor,
};
use sqlparser::dialect::DuckDbDialect;
use sqlparser::parser::Parser;

use crate::spec::FoldKind;

/// Why a query is not row-local. Each variant is one construct §4.2 names, or one the rule implies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    Parse(String),
    NotOneQuery,
    Aggregate(String),
    GroupBy,
    Having,
    Window(String),
    Distinct(&'static str),
    Join,
    OrderBy,
    Limit(&'static str),
    With,
    Subquery,
    SetOperation(String),
    /// A `FROM` item that is not a plain table: a derived table, a table function, `VALUES`.
    Source(String),
    NotADecodedTable(String),
    /// A function outside the scalar allowlist.
    Function(String),
    Columns(String),
    Unsupported(String),
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let what = match self {
            Refusal::Parse(e) => return write!(f, "the time-lapse query does not parse: {e}"),
            Refusal::NotOneQuery => "anything but one SELECT".to_string(),
            Refusal::Aggregate(name) => format!("the aggregate `{name}`"),
            Refusal::GroupBy => "GROUP BY".to_string(),
            Refusal::Having => "HAVING".to_string(),
            Refusal::Window(what) => format!("a window ({what})"),
            Refusal::Distinct(what) => (*what).to_string(),
            Refusal::Join => "a join".to_string(),
            Refusal::OrderBy => "ORDER BY".to_string(),
            Refusal::Limit(what) => (*what).to_string(),
            Refusal::With => "WITH".to_string(),
            Refusal::Subquery => "a subquery".to_string(),
            Refusal::SetOperation(op) => op.clone(),
            Refusal::Source(what) => format!("reading from {what}"),
            Refusal::NotADecodedTable(t) => {
                return write!(
                    f,
                    "`{t}` is not a decoded table of this nest; a time-lapse reads only those"
                )
            }
            Refusal::Function(name) => format!("the function `{name}`"),
            Refusal::Columns(why) => return write!(f, "the time-lapse query's columns: {why}"),
            Refusal::Unsupported(what) => what.clone(),
        };
        write!(f, "a time-lapse query must be row-local, and {what} is not")
    }
}

impl std::error::Error for Refusal {}

/// What a valid query reads and returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Shape {
    pub tables: BTreeSet<String>,
    pub columns: Vec<String>,
    pub has_sign: bool,
    pub has_timestamp: bool,
}

const REQUIRED: [&str; 3] = ["block_number", "log_index", "key"];
const OPTIONAL: [&str; 2] = ["sign", "block_timestamp"];

/// Row-local scalar functions. An allowlist, because a denylist over an engine's growing vocabulary
/// is the gap `entities.rs` learned about in #836.
const SCALAR: &[&str] = &[
    "abs",
    "coalesce",
    "concat",
    "lower",
    "ltrim",
    "nullif",
    "replace",
    "rtrim",
    "substr",
    "substring",
    "trim",
    "upper",
];

/// Names refused as aggregates rather than as unknown functions, so the refusal says what they are.
const AGGREGATES: &[&str] = &[
    "any_value",
    "arg_max",
    "arg_min",
    "array_agg",
    "avg",
    "bool_and",
    "bool_or",
    "count",
    "count_star",
    "first",
    "first_value",
    "last",
    "last_value",
    "list",
    "max",
    "max_by",
    "median",
    "min",
    "min_by",
    "mode",
    "quantile",
    "string_agg",
    "sum",
];

/// Check `sql` against the row-local rule. `decoded` is the nest's decoded tables, from its
/// `schema.json`; an authored view is not one.
pub fn validate(sql: &str, fold: FoldKind, decoded: &BTreeSet<String>) -> Result<Shape, Refusal> {
    let statements =
        Parser::parse_sql(&DuckDbDialect {}, sql).map_err(|e| Refusal::Parse(e.to_string()))?;
    let query = match statements.as_slice() {
        [Statement::Query(q)] => q,
        _ => return Err(Refusal::NotOneQuery),
    };
    let mut walk = Walk {
        decoded,
        tables: BTreeSet::new(),
        arms: Vec::new(),
    };
    walk.query(query)?;
    let columns = walk.arms[0].clone();
    if let Some(other) = walk.arms.iter().find(|a| **a != columns) {
        return Err(Refusal::Columns(format!(
            "the arms of a UNION ALL pair columns by position, and these name them differently: \
             ({}) and ({})",
            columns.join(", "),
            other.join(", ")
        )));
    }
    let amount = fold.amount_column();
    for need in REQUIRED.iter().chain([&amount]) {
        if !columns.iter().any(|c| c == need) {
            return Err(Refusal::Columns(format!(
                "no `{need}` column (a `{}` fold needs block_number, log_index, key and {amount})",
                fold.name()
            )));
        }
    }
    for c in &columns {
        if !(REQUIRED.contains(&c.as_str()) || OPTIONAL.contains(&c.as_str()) || c == amount) {
            return Err(Refusal::Columns(format!(
                "`{c}` is not one of block_number, log_index, key, {amount}, sign, block_timestamp"
            )));
        }
    }
    let mut seen = BTreeSet::new();
    if let Some(dup) = columns.iter().find(|c| !seen.insert(c.as_str())) {
        return Err(Refusal::Columns(format!("`{dup}` is named twice")));
    }
    Ok(Shape {
        tables: walk.tables,
        has_sign: columns.iter().any(|c| c == "sign"),
        has_timestamp: columns.iter().any(|c| c == "block_timestamp"),
        columns,
    })
}

struct Walk<'a> {
    decoded: &'a BTreeSet<String>,
    tables: BTreeSet<String>,
    /// Each SELECT arm's output column names, in order.
    arms: Vec<Vec<String>>,
}

impl Walk<'_> {
    fn query(&mut self, q: &Query) -> Result<(), Refusal> {
        if q.with.is_some() {
            return Err(Refusal::With);
        }
        if q.order_by.is_some() {
            return Err(Refusal::OrderBy);
        }
        if q.limit_clause.is_some() {
            return Err(Refusal::Limit("LIMIT"));
        }
        if q.fetch.is_some() {
            return Err(Refusal::Limit("FETCH"));
        }
        if !q.locks.is_empty()
            || q.for_clause.is_some()
            || q.settings.is_some()
            || q.format_clause.is_some()
            || !q.pipe_operators.is_empty()
        {
            return Err(Refusal::Unsupported(
                "a dialect clause after the query".into(),
            ));
        }
        self.set_expr(&q.body)
    }

    fn set_expr(&mut self, body: &SetExpr) -> Result<(), Refusal> {
        match body {
            SetExpr::Select(s) => self.select(s),
            SetExpr::Query(q) => self.query(q),
            SetExpr::SetOperation {
                left,
                op,
                set_quantifier,
                right,
            } => {
                match (op, set_quantifier) {
                    (SetOperator::Union, SetQuantifier::All) => {}
                    (SetOperator::Union, SetQuantifier::None | SetQuantifier::Distinct) => {
                        return Err(Refusal::Distinct("UNION without ALL, which deduplicates"))
                    }
                    (SetOperator::Union, q) => {
                        return Err(Refusal::SetOperation(format!("UNION {q}")))
                    }
                    (op, _) => return Err(Refusal::SetOperation(op.to_string())),
                }
                self.set_expr(left)?;
                self.set_expr(right)
            }
            SetExpr::Values(_) => Err(Refusal::Source("VALUES".into())),
            SetExpr::Table(_) => Err(Refusal::Source("TABLE".into())),
            _ => Err(Refusal::NotOneQuery),
        }
    }

    fn select(&mut self, s: &Select) -> Result<(), Refusal> {
        if s.distinct
            .as_ref()
            .is_some_and(|d| !matches!(d, Distinct::All))
        {
            return Err(Refusal::Distinct("DISTINCT"));
        }
        if s.top.is_some() {
            return Err(Refusal::Limit("TOP"));
        }
        if !matches!(&s.group_by, GroupByExpr::Expressions(e, m) if e.is_empty() && m.is_empty()) {
            return Err(Refusal::GroupBy);
        }
        if s.having.is_some() {
            return Err(Refusal::Having);
        }
        if s.qualify.is_some() {
            return Err(Refusal::Window("QUALIFY".into()));
        }
        if !s.named_window.is_empty() {
            return Err(Refusal::Window("WINDOW".into()));
        }
        if !s.sort_by.is_empty() {
            return Err(Refusal::OrderBy);
        }
        if s.into.is_some()
            || !s.lateral_views.is_empty()
            || s.prewhere.is_some()
            || !s.connect_by.is_empty()
            || !s.cluster_by.is_empty()
            || !s.distribute_by.is_empty()
        {
            return Err(Refusal::Unsupported("a dialect clause in SELECT".into()));
        }
        match s.from.as_slice() {
            [] => return Err(Refusal::Source("no table".into())),
            [one] if one.joins.is_empty() => self.relation(&one.relation)?,
            _ => return Err(Refusal::Join),
        }
        if let Some(w) = &s.selection {
            row_local(w)?;
        }
        let mut names = Vec::with_capacity(s.projection.len());
        for item in &s.projection {
            let (expr, name) = match item {
                SelectItem::ExprWithAlias { expr, alias } => (expr, Some(ident(alias))),
                SelectItem::UnnamedExpr(expr) => (expr, column_name(expr)),
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => {
                    return Err(Refusal::Columns(
                        "`*` is not allowed; name each column".into(),
                    ))
                }
                SelectItem::ExprWithAliases { .. } => {
                    return Err(Refusal::Columns(
                        "multiple aliases on one expression".into(),
                    ))
                }
            };
            row_local(expr)?;
            names.push(name.ok_or_else(|| {
                Refusal::Columns(format!("`{expr}` has no name; give it one with AS"))
            })?);
        }
        self.arms.push(names);
        Ok(())
    }

    fn relation(&mut self, t: &TableFactor) -> Result<(), Refusal> {
        match t {
            TableFactor::Table {
                name,
                args: None,
                with_hints,
                version: None,
                with_ordinality: false,
                partitions,
                json_path: None,
                sample: None,
                index_hints,
                ..
            } if with_hints.is_empty() && partitions.is_empty() && index_hints.is_empty() => {
                let table = object_name(name);
                if !self.decoded.contains(&table) {
                    return Err(Refusal::NotADecodedTable(table));
                }
                self.tables.insert(table);
                Ok(())
            }
            TableFactor::Table { args: Some(_), .. } | TableFactor::Function { .. } => {
                Err(Refusal::Source("a table function".into()))
            }
            TableFactor::Derived { .. } => Err(Refusal::Source("a derived table".into())),
            TableFactor::NestedJoin { .. } => Err(Refusal::Join),
            _ => Err(Refusal::Source(format!("`{t}`"))),
        }
    }
}

/// Refuse anything in an expression that reads more than its own row.
fn row_local(e: &Expr) -> Result<(), Refusal> {
    struct Check;
    impl Visitor for Check {
        type Break = Refusal;
        fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<Refusal> {
            let refused = match e {
                Expr::Subquery(_) | Expr::InSubquery { .. } | Expr::Exists { .. } => {
                    Some(Refusal::Subquery)
                }
                Expr::Function(f) => {
                    let name = object_name(&f.name);
                    if f.over.is_some() {
                        Some(Refusal::Window(format!("{name} OVER")))
                    } else if AGGREGATES.contains(&name.as_str())
                        || f.filter.is_some()
                        || !f.within_group.is_empty()
                    {
                        Some(Refusal::Aggregate(name))
                    } else if matches!(f.args, FunctionArguments::Subquery(_)) {
                        Some(Refusal::Subquery)
                    } else if !SCALAR.contains(&name.as_str()) {
                        Some(Refusal::Function(name))
                    } else {
                        None
                    }
                }
                _ => None,
            };
            match refused {
                Some(r) => ControlFlow::Break(r),
                None => ControlFlow::Continue(()),
            }
        }
    }
    match e.visit(&mut Check) {
        ControlFlow::Break(r) => Err(r),
        ControlFlow::Continue(()) => Ok(()),
    }
}

/// An unquoted identifier folds to lower case, as the engine folds it.
fn ident(i: &Ident) -> String {
    match i.quote_style {
        Some(_) => i.value.clone(),
        None => i.value.to_lowercase(),
    }
}

fn object_name(n: &ObjectName) -> String {
    match n.0.as_slice() {
        [part] => part.as_ident().map(ident).unwrap_or_else(|| n.to_string()),
        _ => n.to_string(),
    }
}

fn column_name(e: &Expr) -> Option<String> {
    match e {
        Expr::Identifier(i) => Some(ident(i)),
        Expr::CompoundIdentifier(parts) => parts.last().map(ident),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decoded() -> BTreeSet<String> {
        ["transfer", "sync"].iter().map(|s| s.to_string()).collect()
    }

    fn check(sql: &str) -> Result<Shape, Refusal> {
        validate(sql, FoldKind::Sum, &decoded())
    }

    const ROW: &str = "block_number, log_index, \"to\" AS key, value AS delta";

    #[test]
    fn a_row_local_union_of_credit_and_debit_is_accepted() {
        let shape = check(
            "SELECT block_number, log_index, \"to\" AS key, value AS delta, 1 AS sign FROM transfer \
             WHERE \"to\" <> '0x00' \
             UNION ALL \
             SELECT block_number, log_index, \"from\" AS key, value AS delta, -1 AS sign FROM transfer \
             WHERE lower(\"from\") <> '0x00'",
        )
        .unwrap();
        assert_eq!(shape.tables, ["transfer".to_string()].into());
        assert!(shape.has_sign && !shape.has_timestamp);
    }

    #[test]
    fn a_last_fold_reads_value_not_delta() {
        let sql = "SELECT block_number, log_index, pair AS key, reserve0 AS value FROM sync";
        assert!(validate(sql, FoldKind::Last, &decoded()).is_ok());
        assert!(matches!(check(sql), Err(Refusal::Columns(_))));
    }

    #[test]
    fn an_aggregate_is_refused() {
        let r = check(
            "SELECT block_number, log_index, \"to\" AS key, sum(value) AS delta FROM transfer",
        );
        assert_eq!(r, Err(Refusal::Aggregate("sum".into())));
    }

    #[test]
    fn group_by_is_refused() {
        let r = check(&format!("SELECT {ROW} FROM transfer GROUP BY 1, 2, 3, 4"));
        assert_eq!(r, Err(Refusal::GroupBy));
    }

    #[test]
    fn having_is_refused() {
        let r = check(&format!("SELECT {ROW} FROM transfer HAVING value > 0"));
        assert_eq!(r, Err(Refusal::Having));
    }

    #[test]
    fn a_window_function_is_refused() {
        let r = check(
            "SELECT block_number, log_index, \"to\" AS key, \
             row_number() OVER (ORDER BY block_number) AS delta FROM transfer",
        );
        assert_eq!(r, Err(Refusal::Window("row_number OVER".into())));
    }

    #[test]
    fn qualify_is_refused() {
        let r = check(&format!(
            "SELECT {ROW} FROM transfer QUALIFY block_number > 0"
        ));
        assert_eq!(r, Err(Refusal::Window("QUALIFY".into())));
    }

    #[test]
    fn distinct_is_refused() {
        let r = check(&format!("SELECT DISTINCT {ROW} FROM transfer"));
        assert_eq!(r, Err(Refusal::Distinct("DISTINCT")));
    }

    #[test]
    fn union_without_all_is_refused() {
        let r = check(&format!(
            "SELECT {ROW} FROM transfer UNION SELECT {ROW} FROM transfer"
        ));
        assert_eq!(
            r,
            Err(Refusal::Distinct("UNION without ALL, which deduplicates"))
        );
    }

    #[test]
    fn a_join_is_refused() {
        let r = check(&format!(
            "SELECT {ROW} FROM transfer JOIN sync ON transfer.block_number = sync.block_number"
        ));
        assert_eq!(r, Err(Refusal::Join));
    }

    #[test]
    fn a_comma_join_is_refused() {
        let r = check(&format!("SELECT {ROW} FROM transfer, sync"));
        assert_eq!(r, Err(Refusal::Join));
    }

    #[test]
    fn order_by_is_refused() {
        let r = check(&format!("SELECT {ROW} FROM transfer ORDER BY block_number"));
        assert_eq!(r, Err(Refusal::OrderBy));
    }

    #[test]
    fn limit_is_refused() {
        let r = check(&format!("SELECT {ROW} FROM transfer LIMIT 10"));
        assert_eq!(r, Err(Refusal::Limit("LIMIT")));
    }

    #[test]
    fn a_table_that_is_not_decoded_is_refused() {
        let r = check(&format!("SELECT {ROW} FROM lodestar_delegators"));
        assert_eq!(
            r,
            Err(Refusal::NotADecodedTable("lodestar_delegators".into()))
        );
    }

    #[test]
    fn a_derived_table_is_refused() {
        let r = check(&format!("SELECT {ROW} FROM (SELECT * FROM transfer) AS t"));
        assert_eq!(r, Err(Refusal::Source("a derived table".into())));
    }

    #[test]
    fn with_is_refused() {
        let r = check(&format!(
            "WITH t AS (SELECT * FROM transfer) SELECT {ROW} FROM transfer"
        ));
        assert_eq!(r, Err(Refusal::With));
    }

    #[test]
    fn a_subquery_in_an_expression_is_refused() {
        let r = check(&format!(
            "SELECT {ROW} FROM transfer WHERE \"to\" IN (SELECT pair FROM sync)"
        ));
        assert_eq!(r, Err(Refusal::Subquery));
    }

    #[test]
    fn a_function_outside_the_allowlist_is_refused() {
        let r = check(&format!("SELECT {ROW} FROM transfer WHERE random() < 0.5"));
        assert_eq!(r, Err(Refusal::Function("random".into())));
    }

    #[test]
    fn a_missing_column_is_refused() {
        let r = check("SELECT block_number, \"to\" AS key, value AS delta FROM transfer");
        assert!(matches!(r, Err(Refusal::Columns(why)) if why.contains("`log_index`")));
    }

    #[test]
    fn an_extra_column_is_refused() {
        let r = check(&format!("SELECT {ROW}, tx_hash FROM transfer"));
        assert!(matches!(r, Err(Refusal::Columns(why)) if why.contains("`tx_hash`")));
    }

    #[test]
    fn a_wildcard_is_refused() {
        let r = check("SELECT * FROM transfer");
        assert!(matches!(r, Err(Refusal::Columns(why)) if why.contains('*')));
    }

    #[test]
    fn union_arms_in_different_orders_are_refused() {
        let r = check(&format!(
            "SELECT {ROW} FROM transfer UNION ALL \
             SELECT log_index, block_number, \"to\" AS key, value AS delta FROM transfer"
        ));
        assert!(matches!(r, Err(Refusal::Columns(why)) if why.contains("by position")));
    }

    #[test]
    fn two_statements_are_refused() {
        let r = check(&format!("SELECT {ROW} FROM transfer; SELECT 1"));
        assert_eq!(r, Err(Refusal::NotOneQuery));
    }

    #[test]
    fn every_refusal_names_its_construct() {
        let r = check(&format!("SELECT {ROW} FROM transfer LIMIT 10")).unwrap_err();
        assert_eq!(
            r.to_string(),
            "a time-lapse query must be row-local, and LIMIT is not"
        );
    }
}
