//! The SQL a node and an embedding bind `/sql`'s tables with (RFC-0077 S1). Each engine runs it
//! over its own storage, files on a node and bytes in a tab, so both answer a statement alike.

use crate::entity_expr::Type;
use serde_json::Value;
use std::collections::BTreeSet;

/// The SQL type an entity column binds as, from the type its plan gives it (#1598). A column whose
/// type the plan cannot fix, a `CASE` over mixed branches for one, is text.
pub fn relation_type(t: Option<Type>) -> &'static str {
    match t {
        Some(Type::Int) => "HUGEINT",
        Some(Type::Bool) => "BOOLEAN",
        Some(Type::Str) | None => "VARCHAR",
    }
}

/// A maintained relation as a table of its declared types: its rows as text under a hidden name, and
/// a view under its own that casts each column. A marker column keeps the one placeholder row an
/// empty relation needs out of it. Answers the hidden name, its rows and the view's query.
pub fn relation(
    table: &str,
    declared: &[(String, &'static str)],
    staged: &[Value],
) -> (String, Vec<Value>, String) {
    let mut rows: Vec<Value> = staged
        .iter()
        .map(|row| {
            let mut out = serde_json::Map::new();
            for (c, _) in declared {
                out.insert(
                    c.clone(),
                    match row.get(c) {
                        None | Some(Value::Null) => Value::Null,
                        Some(v @ (Value::Bool(_) | Value::String(_))) => v.clone(),
                        Some(v) => Value::String(v.to_string()),
                    },
                );
            }
            out.insert("__present".into(), Value::Bool(true));
            Value::Object(out)
        })
        .collect();
    if rows.is_empty() {
        let mut out: serde_json::Map<String, Value> = declared
            .iter()
            .map(|(c, _)| (c.clone(), Value::Null))
            .collect();
        out.insert("__present".into(), Value::Bool(false));
        rows.push(Value::Object(out));
    }
    let raw = format!("{table}__raw");
    let quote = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
    let columns: Vec<String> = declared
        .iter()
        .map(|(c, t)| format!("CAST({0} AS {t}) AS {0}", quote(c)))
        .collect();
    let view = format!(
        "SELECT {} FROM {} WHERE \"__present\" = 'true'",
        columns.join(", "),
        quote(&raw)
    );
    (raw, rows, view)
}

/// The statement that finds which sealed table holds the event at `block`/`log_index`, as a row `t`.
/// The id names no table, and asking each in turn cost a query per table (#1574). `None` when there
/// are no tables to ask.
pub fn sealed_probe<'a>(
    tables: impl IntoIterator<Item = &'a str>,
    block: u64,
    log_index: u64,
) -> Option<String> {
    let probe = tables
        .into_iter()
        .map(|t| {
            format!(
                "SELECT '{}' AS t FROM \"{t}\" WHERE block_number = {block} AND log_index = {log_index}",
                t.replace('\'', "''")
            )
        })
        .collect::<Vec<_>>();
    (!probe.is_empty()).then(|| format!("{} ORDER BY t LIMIT 1", probe.join(" UNION ALL ")))
}

/// The event at `block`/`log_index` from the one table the probe found, in the shape a single-table
/// query gives.
pub fn sealed_row(table: &str, block: u64, log_index: u64) -> String {
    format!(
        "SELECT * FROM \"{table}\" WHERE block_number = {block} AND log_index = {log_index} LIMIT 1"
    )
}

/// `named` widened to every name those names read through views. `reads` answers the base tables a
/// view reads, or `None` for a name that is not a view or whose body does not parse, which is skipped.
/// Each name is visited once, so a cycle or a deep chain ends.
pub fn through_views(
    named: &BTreeSet<String>,
    mut reads: impl FnMut(&str) -> Option<BTreeSet<String>>,
) -> BTreeSet<String> {
    let mut out = named.clone();
    let mut frontier: Vec<String> = named.iter().cloned().collect();
    while !frontier.is_empty() {
        let mut next = Vec::new();
        for name in frontier.drain(..) {
            let Some(inner) = reads(&name) else {
                continue;
            };
            for t in inner {
                if out.insert(t.clone()) {
                    next.push(t);
                }
            }
        }
        frontier = next;
    }
    out
}

/// The `labels` table's rows from `labels/*.json` snapshots, each `(name, text)` a flat JSON array of
/// `{address, label}`, in the order given. Addresses are lower-cased for a clean join with decoded hex.
pub fn label_rows<'a>(
    snapshots: impl IntoIterator<Item = (String, &'a str)>,
) -> anyhow::Result<Vec<Value>> {
    use anyhow::Context;
    let mut rows = Vec::new();
    for (name, text) in snapshots {
        let doc: Value =
            serde_json::from_str(text).with_context(|| format!("{name} is not JSON"))?;
        for item in doc.as_array().into_iter().flatten() {
            let address = item
                .get("address")
                .and_then(Value::as_str)
                .map(str::to_ascii_lowercase);
            let label = item.get("label").cloned().unwrap_or(Value::Null);
            rows.push(serde_json::json!({ "address": address, "label": label }));
        }
    }
    Ok(rows)
}

/// A factory nest's `{template}__children` views (RFC-0009 §Serving), each `(name, DDL)`: the children
/// every factory producing the template discovered, with their provenance, the earliest discovery
/// per address. A nest without block timestamps has no `discovered_timestamp` to project.
pub fn children_views(
    factories: &crate::factory::FactorySet,
    block_timestamps: bool,
) -> Vec<(String, String)> {
    let (ts, cols) = if block_timestamps {
        (
            "block_timestamp AS discovered_timestamp, ",
            "discovered_timestamp, ",
        )
    } else {
        ("", "")
    };
    let mut by_template: std::collections::BTreeMap<String, Vec<(String, String)>> =
        std::collections::BTreeMap::new();
    for (template, table, child_param) in factories.view_sources() {
        by_template
            .entry(template)
            .or_default()
            .push((table, child_param));
    }
    by_template
        .into_iter()
        .map(|(template, sources)| {
            // `child_param`/`table` are registry-derived (never user text) → no injection surface.
            let selects: Vec<String> = sources
                .iter()
                .map(|(table, cp)| {
                    format!(
                        "SELECT lower(\"{cp}\") AS address, block_number AS discovered_block, \
                         log_index AS discovered_log_index, {ts}\
                         lower(address) AS parent_address FROM \"{table}\""
                    )
                })
                .collect();
            let union = selects.join(" UNION ALL ");
            let ddl = format!(
                "CREATE OR REPLACE VIEW \"{template}__children\" AS \
                 SELECT address, discovered_block, discovered_log_index, {cols}parent_address \
                 FROM ({union}) \
                 QUALIFY row_number() OVER (PARTITION BY address ORDER BY discovered_block, discovered_log_index) = 1"
            );
            (format!("{template}__children"), ddl)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn an_empty_relation_binds_one_row_the_view_leaves_out() {
        let declared = vec![("n".to_string(), "HUGEINT"), ("ok".to_string(), "BOOLEAN")];
        let (raw, rows, view) = relation("e", &declared, &[]);
        assert_eq!(raw, "e__raw");
        assert_eq!(
            rows,
            vec![json!({"n": null, "ok": null, "__present": false})]
        );
        assert_eq!(
            view,
            "SELECT CAST(\"n\" AS HUGEINT) AS \"n\", CAST(\"ok\" AS BOOLEAN) AS \"ok\" FROM \
             \"e__raw\" WHERE \"__present\" = 'true'"
        );
        let (_, rows, _) = relation("e", &declared, &[json!({"n": 7, "ok": true, "x": 1})]);
        assert_eq!(rows, vec![json!({"n": "7", "ok": true, "__present": true})]);
    }

    #[test]
    fn the_probe_quotes_a_table_name_as_a_literal() {
        assert_eq!(sealed_probe([], 1, 2), None);
        assert_eq!(
            sealed_probe(["a", "o'b"], 5, 3).unwrap(),
            "SELECT 'a' AS t FROM \"a\" WHERE block_number = 5 AND log_index = 3 UNION ALL \
             SELECT 'o''b' AS t FROM \"o'b\" WHERE block_number = 5 AND log_index = 3 \
             ORDER BY t LIMIT 1"
        );
    }

    #[test]
    fn a_cycle_of_views_ends() {
        let reads = |name: &str| match name {
            "v" => Some(BTreeSet::from(["w".to_string(), "t".to_string()])),
            "w" => Some(BTreeSet::from(["v".to_string(), "u".to_string()])),
            _ => None,
        };
        let got = through_views(&BTreeSet::from(["v".to_string()]), reads);
        let want: BTreeSet<String> = ["t", "u", "v", "w"].map(String::from).into();
        assert_eq!(got, want);
    }
}
