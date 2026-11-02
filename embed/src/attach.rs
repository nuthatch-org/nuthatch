//! RFC-0081 §3.2: another instance's answers bound into this one's statement. `nh_open`'s `attach`
//! declares each attachment and its tables; a `POST /sql` supplies each table as the Arrow IPC answer
//! the other instance gave (RFC-0079), with that answer's envelope. The module binds them for that
//! statement only, with the types the other instance bound, holds nothing between statements and
//! follows nothing. An answer from another block than this nest's is refused by name.

use anyhow::{bail, ensure, Result};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use base64::Engine as _;
use serde::Deserialize;
use serde_json::Value;
use sqlparser::ast::{ObjectNamePart, Query, TableFactor, Visit, Visitor};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;

/// One entry of `nh_open`'s `attach`.
#[derive(Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct Declared {
    pub name: String,
    pub tables: Vec<String>,
}

fn plain(name: &str) -> bool {
    !name.is_empty()
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        && !name.as_bytes()[0].is_ascii_digit()
}

/// Refused at open: a name that is not a plain identifier, an attachment or table named twice, or
/// a table sharing a name with one of the nest's own. Statements resolve an attached table by its
/// own name, so names must not collide even across attachments.
pub fn check(declared: &[Declared], own: &BTreeSet<String>) -> Result<()> {
    let mut names = BTreeSet::new();
    let mut tables = BTreeSet::new();
    for a in declared {
        ensure!(
            plain(&a.name),
            "attachment `{}` is not a plain identifier",
            a.name
        );
        ensure!(
            names.insert(a.name.to_ascii_lowercase()),
            "attachment `{}` is declared twice",
            a.name
        );
        ensure!(
            !a.tables.is_empty(),
            "attachment `{}` names no tables",
            a.name
        );
        for t in &a.tables {
            let lower = t.to_ascii_lowercase();
            ensure!(
                plain(t) && !lower.ends_with("__raw"),
                "`{}.{t}` is not a plain table name",
                a.name
            );
            ensure!(
                !own.contains(&lower),
                "`{}.{t}`: this nest has a table or view named `{t}` already",
                a.name
            );
            ensure!(
                tables.insert(lower),
                "`{t}` is attached twice; a table's name is unique across attachments",
            );
        }
    }
    Ok(())
}

/// One attached table as a statement supplied it.
pub struct Table {
    pub attachment: String,
    pub name: String,
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Given {
    /// The other instance's Arrow IPC stream, base64.
    arrow: String,
    /// Its envelope: the `x-nuthatch-answer` header's text, or that JSON as an object.
    answer: Value,
}

/// `attach` from a `POST /sql` body, decoded, every table checked against `declared` and its answer's
/// `provenance.as_of` against this nest's `as_of`.
pub fn supplied(
    declared: &[Declared],
    given: &Value,
    as_of: Option<u64>,
) -> Result<Vec<Table>, String> {
    let given: BTreeMap<String, BTreeMap<String, Given>> =
        serde_json::from_value(given.clone()).map_err(|e| format!("`attach`: {e}"))?;
    let mut out = Vec::new();
    for (name, tables) in given {
        let Some(d) = declared.iter().find(|d| d.name.eq_ignore_ascii_case(&name)) else {
            return Err(format!(
                "no attachment `{name}` was declared at nh_open{}",
                listed(declared.iter().map(|d| d.name.as_str()))
            ));
        };
        for (table, g) in tables {
            let Some(t) = d.tables.iter().find(|t| t.eq_ignore_ascii_case(&table)) else {
                return Err(format!(
                    "attachment `{}` declares no table `{table}`{}",
                    d.name,
                    listed(d.tables.iter().map(String::as_str))
                ));
            };
            let qualified = format!("{}.{t}", d.name);
            let answer = match &g.answer {
                Value::String(s) => {
                    serde_json::from_str(s).map_err(|e| format!("`{qualified}`'s answer: {e}"))?
                }
                v => v.clone(),
            };
            if answer["truncated"] == true {
                return Err(format!(
                    "`{qualified}` is a truncated answer, so it is not the other nest's rows"
                ));
            }
            let theirs = answer["provenance"]["as_of"].as_u64();
            if theirs.is_none() || theirs != as_of {
                return Err(format!(
                    "`{qualified}` is the other nest's answer as of block {}, and this nest \
                     answers as of block {}: an attachment is bound only at the block this nest \
                     answers at",
                    block(theirs),
                    block(as_of)
                ));
            }
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(g.arrow.as_bytes())
                .map_err(|e| format!("`{qualified}`'s arrow is not base64: {e}"))?;
            let reader =
                arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
                    .map_err(|e| format!("`{qualified}` is not an Arrow IPC stream: {e}"))?;
            let schema = reader.schema();
            let batches = reader
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("`{qualified}`'s Arrow stream: {e}"))?;
            out.push(Table {
                attachment: d.name.clone(),
                name: t.clone(),
                schema,
                batches,
            });
        }
    }
    Ok(out)
}

fn block(b: Option<u64>) -> String {
    b.map_or("none".into(), |b| b.to_string())
}

fn listed<'a>(names: impl Iterator<Item = &'a str>) -> String {
    let names: Vec<&str> = names.collect();
    if names.is_empty() {
        String::new()
    } else {
        format!(" (declared: {})", names.join(", "))
    }
}

/// The attached tables `sql` names, as `(attachment, table)`. An attached table is named with its
/// attachment, `pools.indexers`: a bare `indexers` is refused unless the statement defines it, and so
/// is a table its attachment does not declare. `None` where the statement does not parse here, which
/// leaves it to the engine.
pub fn references(
    sql: &str,
    declared: &[Declared],
) -> Option<Result<BTreeSet<(String, String)>, String>> {
    if declared.is_empty() {
        return Some(Ok(BTreeSet::new()));
    }
    let stmts =
        sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::DuckDbDialect {}, sql).ok()?;
    let mut w = Refs::default();
    let _ = stmts.visit(&mut w);
    let mut out = BTreeSet::new();
    for parts in w.relations {
        let lower: Vec<String> = parts.iter().map(|p| p.to_ascii_lowercase()).collect();
        match lower.as_slice() {
            [schema, table] => {
                let Some(d) = declared
                    .iter()
                    .find(|d| d.name.eq_ignore_ascii_case(schema))
                else {
                    continue;
                };
                match d.tables.iter().find(|t| t.eq_ignore_ascii_case(table)) {
                    Some(t) => out.insert((d.name.clone(), t.clone())),
                    None => {
                        return Some(Err(format!(
                            "attachment `{}` declares no table `{}`{}",
                            d.name,
                            parts[1],
                            listed(d.tables.iter().map(String::as_str))
                        )))
                    }
                };
            }
            [table] if !w.ctes.contains(table) => {
                if let Some(d) = declared
                    .iter()
                    .find(|d| d.tables.iter().any(|t| t.eq_ignore_ascii_case(table)))
                {
                    return Some(Err(format!(
                        "`{}` is attached: name it `{}.{}`",
                        parts[0], d.name, parts[0]
                    )));
                }
            }
            _ => {}
        }
    }
    Some(Ok(out))
}

/// The unqualified tables `sql` reads that it does not define itself, lowercased; `None` where it
/// does not parse here.
pub fn bare_tables(sql: &str) -> Option<BTreeSet<String>> {
    let stmts =
        sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::DuckDbDialect {}, sql).ok()?;
    let mut w = Refs::default();
    let _ = stmts.visit(&mut w);
    Some(
        w.relations
            .iter()
            .filter(|p| p.len() == 1)
            .map(|p| p[0].to_ascii_lowercase())
            .filter(|t| !w.ctes.contains(t))
            .collect(),
    )
}

#[derive(Default)]
struct Refs {
    relations: Vec<Vec<String>>,
    ctes: BTreeSet<String>,
}

impl Visitor for Refs {
    type Break = ();

    fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<()> {
        if let Some(with) = &q.with {
            for c in &with.cte_tables {
                self.ctes.insert(c.alias.name.value.to_ascii_lowercase());
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, t: &TableFactor) -> ControlFlow<()> {
        if let TableFactor::Table {
            name, args: None, ..
        } = t
        {
            self.relations.push(
                name.0
                    .iter()
                    .map(|p| match p {
                        ObjectNamePart::Identifier(i) => i.value.clone(),
                        other => other.to_string(),
                    })
                    .collect(),
            );
        }
        ControlFlow::Continue(())
    }
}

/// The SQL type an attached column binds as, which is the type the other instance's answer carried.
pub fn sql_type(t: &arrow_schema::DataType) -> Result<String> {
    use arrow_schema::DataType::*;
    Ok(match t {
        Utf8 | Utf8View | LargeUtf8 => "VARCHAR".into(),
        Boolean => "BOOLEAN".into(),
        Int8 => "TINYINT".into(),
        Int16 => "SMALLINT".into(),
        Int32 => "INTEGER".into(),
        Int64 => "BIGINT".into(),
        UInt8 => "UTINYINT".into(),
        UInt16 => "USMALLINT".into(),
        UInt32 => "UINTEGER".into(),
        UInt64 => "UBIGINT".into(),
        Float32 => "FLOAT".into(),
        Float64 => "DOUBLE".into(),
        Decimal128(38, 0) => "HUGEINT".into(),
        Decimal128(p, s) => format!("DECIMAL({p},{s})"),
        other => bail!("{other}, which an attachment does not carry; cast it on the other nest"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn pools() -> Vec<Declared> {
        vec![Declared {
            name: "pools".into(),
            tables: vec!["indexers".into(), "thaws".into()],
        }]
    }

    fn ipc() -> String {
        let schema =
            std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "id",
                arrow_schema::DataType::Utf8,
                false,
            )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![std::sync::Arc::new(arrow_array::StringArray::from(vec![
                "0xab",
            ]))],
        )
        .unwrap();
        let mut w = arrow_ipc::writer::StreamWriter::try_new(Vec::new(), &schema).unwrap();
        w.write(&batch).unwrap();
        w.finish().unwrap();
        base64::engine::general_purpose::STANDARD.encode(w.into_inner().unwrap())
    }

    #[test]
    fn an_attachment_from_another_block_is_refused_by_name() {
        let at = |b: u64| {
            json!({"pools": {"indexers": {"arrow": ipc(),
                "answer": json!({"provenance": {"as_of": b}}).to_string()}}})
        };
        let ok = supplied(&pools(), &at(100), Some(100)).unwrap();
        assert_eq!(ok.len(), 1);
        assert_eq!(ok[0].batches[0].num_rows(), 1);
        let refused = supplied(&pools(), &at(99), Some(100)).err().unwrap();
        assert!(
            refused.contains("`pools.indexers`") && refused.contains("as of block 99"),
            "{refused}"
        );
    }

    #[test]
    fn an_attached_table_is_named_with_its_attachment() {
        let named = |sql: &str| references(sql, &pools()).unwrap();
        assert_eq!(
            named("WITH x AS (SELECT * FROM pools.indexers) SELECT * FROM x, t").unwrap(),
            BTreeSet::from([("pools".to_string(), "indexers".to_string())])
        );
        assert!(named("SELECT * FROM indexers")
            .unwrap_err()
            .contains("pools.indexers"));
        assert!(named("WITH indexers AS (SELECT 1) SELECT * FROM indexers").is_ok());
        assert!(named("SELECT * FROM pools.other")
            .unwrap_err()
            .contains("declares no table `other`"));
    }

    #[test]
    fn names_that_would_collide_are_refused_at_open() {
        let own: BTreeSet<String> = ["indexers".to_string()].into();
        assert!(format!("{:#}", check(&pools(), &own).unwrap_err()).contains("already"));
        let twice = [pools(), pools()].concat();
        assert!(format!("{:#}", check(&twice, &BTreeSet::new()).unwrap_err()).contains("twice"));
    }
}
