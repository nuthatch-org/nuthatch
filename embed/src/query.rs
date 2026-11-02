//! RFC-0075 S4: `views/*.sql` answered over hot ∪ sealed in the tab, by the Burrmill rev the binary
//! pins. A port of `analytics::define_views_bound` and `engine_burrmill`'s `bind_relation` onto
//! Burrmill's store tail (`attach_store`, `register_store_facts`), since a tab's segments are bytes
//! in its cellar and not files.

use crate::budget::TabBudget;
use crate::cellar::{Cellar, Segments, SEGMENTS_PREFIX};
use crate::derived::{NestFiles, TabEntity};
use crate::registry::TableSchema;
use anyhow::{anyhow, Result};
use nuthatch_core::answer::{
    rewrite_temporal, temporal_columns, value_bytes, SQL_MAX_RESULT_BYTES,
};
use nuthatch_core::redb_store::Store;
use nuthatch_core::store::SEALED_THROUGH_KEY;
use serde_json::Value;
use std::collections::BTreeSet;
use std::sync::Arc;

/// Rows of one batch turned into JSON before the byte cap is checked again, as the node's.
const ENCODE_ROWS: usize = 32;

/// What a slice would cost to encode, measured as `value_bytes` measures it, before it is encoded:
/// string and binary payload exactly from offsets and views, every other type (nested ones too) from
/// Arrow's slice size, which over-counts a sliced list's children. A type Arrow cannot size is refused.
fn arrow_bytes(part: &arrow_array::RecordBatch) -> usize {
    use arrow_array::cast::AsArray;
    use arrow_array::Array;
    part.columns()
        .iter()
        .map(|c| {
            if let Some(a) = c.as_string_opt::<i32>() {
                let o = a.value_offsets();
                (o[o.len() - 1] - o[0]) as usize
            } else if let Some(a) = c.as_string_opt::<i64>() {
                let o = a.value_offsets();
                (o[o.len() - 1] - o[0]) as usize
            } else if let Some(a) = c.as_binary_opt::<i32>() {
                let o = a.value_offsets();
                (o[o.len() - 1] - o[0]) as usize
            } else if let Some(a) = c.as_binary_opt::<i64>() {
                let o = a.value_offsets();
                (o[o.len() - 1] - o[0]) as usize
            } else if let Some(a) = c.as_string_view_opt() {
                a.iter().flatten().map(str::len).sum()
            } else if let Some(a) = c.as_binary_view_opt() {
                a.iter().flatten().map(<[u8]>::len).sum()
            } else {
                c.to_data().get_slice_memory_size().unwrap_or(usize::MAX)
            }
        })
        .fold(0usize, usize::saturating_add)
}

/// A batch as JSON rows, with dates and timestamps as the node writes them.
fn encode(batch: &arrow_array::RecordBatch) -> Result<Vec<Value>, burrmill::BurrmillError> {
    let temporal = temporal_columns(
        batch
            .schema()
            .fields()
            .iter()
            .map(|f| (f.name(), f.data_type().to_string())),
    );
    let mut rows = burrmill::df::encode::rows(batch)?;
    rewrite_temporal(&mut rows, &temporal);
    Ok(rows)
}

fn engine_err(e: burrmill::BurrmillError) -> anyhow::Error {
    anyhow!("{e}")
}

pub struct TabQuery {
    engine: burrmill::Engine,
}

impl TabQuery {
    /// One engine with every fact table, entity and authored view bound as the node binds them for
    /// a statement at the store's current watermark, its working memory held to `budget`'s share.
    pub async fn bind(
        store: &Store,
        cellar: &Arc<Cellar>,
        schema: &[TableSchema],
        entities: &[TabEntity],
        files: &NestFiles,
        budget: TabBudget,
        children: &[(String, String)],
    ) -> Result<TabQuery> {
        let mut engine = burrmill::Engine::open_empty_within(budget.query_bytes() as usize)
            .map_err(engine_err)?;
        let sealed_through: u64 = store
            .get_meta(SEALED_THROUGH_KEY)?
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let manifest = cellar.manifest();
        engine
            .attach_store(Arc::new(Segments(cellar.clone())))
            .map_err(engine_err)?;

        let hot = store.hot_rows_by_table()?;
        let cols_of = |table: &str| -> Vec<(String, String)> {
            schema
                .iter()
                .find(|t| t.table == table)
                .map(|t| {
                    t.columns
                        .iter()
                        .map(|c| (c.name.clone(), c.storage.clone()))
                        .collect()
                })
                .unwrap_or_default()
        };
        let mut tables: BTreeSet<String> = schema.iter().map(|t| t.table.clone()).collect();
        tables.extend(manifest.tables.keys().cloned());
        tables.extend(hot.keys().cloned());
        for table in &tables {
            let cols = cols_of(table);
            let sealed: Vec<(String, u64)> = manifest
                .tables
                .get(table)
                .into_iter()
                .flatten()
                .filter(|s| s.to_block <= sealed_through)
                .map(|s| {
                    let len = cellar
                        .len_of(&s.file)?
                        .ok_or_else(|| anyhow!("segment {} is not in the cellar", s.file))?;
                    Ok((key(&s.file), len))
                })
                .collect::<Result<_>>()?;
            // COR-1, as the node: hot rows past the watermark only where cold covers something.
            let hot_rows: Vec<Value> = hot
                .get(table)
                .map(Vec::as_slice)
                .unwrap_or(&[])
                .iter()
                .filter(|r| {
                    sealed.is_empty()
                        || r.get("block_number").and_then(Value::as_u64).unwrap_or(0)
                            > sealed_through
                })
                .cloned()
                .collect();
            if sealed.is_empty() && hot_rows.is_empty() && cols.is_empty() {
                continue;
            }
            engine
                .register_store_facts(table, &cols, sealed, &hot_rows, (None, None))
                .await
                .map_err(engine_err)?;
        }
        for e in entities {
            bind_relation(&mut engine, e)?;
        }
        for (path, text) in files {
            if path.starts_with("views/") && path.ends_with(".sql") {
                define_views(&mut engine, text)?;
            }
        }
        // As the node binds them after its authored views (RFC-0077 S5), and as best-effort.
        let labels = nuthatch_core::bind::label_rows(
            files
                .iter()
                .filter(|(p, _)| p.starts_with("labels/") && p.ends_with(".json"))
                .map(|(p, t)| (p.clone(), t.as_str())),
        )?;
        if !labels.is_empty() {
            engine
                .register_rows("labels", &labels)
                .map_err(engine_err)?;
        }
        for (_, ddl) in children {
            define_views(&mut engine, ddl)?;
        }
        Ok(TabQuery { engine })
    }

    /// RFC-0081 S1: each hydrated table read through its filters' predicate, so a statement sees the
    /// rows the filters select whether the mirror or the RPC brought them.
    pub fn narrow(
        &mut self,
        predicates: &std::collections::BTreeMap<String, String>,
    ) -> Result<()> {
        for (table, predicate) in predicates {
            if self.engine.has_table(table) {
                let t = table.replace('"', "\"\"");
                self.engine
                    .register_view(table, &format!("SELECT * FROM \"{t}\" WHERE {predicate}"))
                    .map_err(engine_err)?;
            }
        }
        Ok(())
    }

    /// RFC-0081 §3.2: another instance's answers bound under their own names for this statement, each
    /// column cast back to the type the answer carried. The cells pass as text, which every carried
    /// type reads back exactly: a decimal as its digits, an integer as its own.
    pub fn attach(&mut self, tables: &[crate::attach::Table]) -> Result<()> {
        for t in tables {
            let mut casts = Vec::new();
            for f in t.schema.fields() {
                let ty = crate::attach::sql_type(f.data_type()).map_err(|e| {
                    anyhow!("`{}.{}` column `{}` is {e}", t.attachment, t.name, f.name())
                })?;
                casts.push((f.name().replace('"', "\"\""), ty));
            }
            let mut rows = Vec::new();
            for b in &t.batches {
                for row in burrmill::df::encode::rows(b).map_err(engine_err)? {
                    let Value::Object(o) = row else { continue };
                    rows.push(Value::Object(
                        o.into_iter()
                            .map(|(k, v)| {
                                let v = match v {
                                    Value::Null | Value::String(_) => v,
                                    other => Value::String(other.to_string()),
                                };
                                (k, v)
                            })
                            .collect(),
                    ));
                }
            }
            let body = if rows.is_empty() {
                let nulls: Vec<String> = casts
                    .iter()
                    .map(|(c, ty)| format!("CAST(NULL AS {ty}) AS \"{c}\""))
                    .collect();
                format!("SELECT {} WHERE false", nulls.join(", "))
            } else {
                let raw = format!("{}__raw", t.name);
                self.engine.register_rows(&raw, &rows).map_err(engine_err)?;
                let cast: Vec<String> = casts
                    .iter()
                    .map(|(c, ty)| format!("CAST(\"{c}\" AS {ty}) AS \"{c}\""))
                    .collect();
                format!("SELECT {} FROM \"{raw}\"", cast.join(", "))
            };
            self.engine
                .register_view(&t.name, &body)
                .map_err(engine_err)?;
        }
        Ok(())
    }

    /// `/sql`'s answer as an Arrow IPC stream (RFC-0079), as the node's `collect_ipc` writes it: at
    /// most `cap` rows and `max_bytes` of stream, and the plan's schema when no batch comes. Answers
    /// the stream, its columns, the rows written and whether more existed.
    pub async fn collect_ipc(
        &self,
        sql: &str,
        cap: usize,
        max_bytes: usize,
    ) -> Result<(Vec<u8>, Vec<String>, usize, bool), burrmill::BurrmillError> {
        use arrow_ipc::writer::StreamWriter;
        let io = |e: arrow_schema::ArrowError| burrmill::BurrmillError::Substrate(e.to_string());
        let names = |s: &arrow_schema::Schema| -> Vec<String> {
            s.fields().iter().map(|f| f.name().clone()).collect()
        };
        let mut writer: Option<StreamWriter<Vec<u8>>> = None;
        let mut columns = Vec::new();
        let mut failed = None;
        let (mut written, mut over) = (0usize, false);
        let mut take = |batch: arrow_array::RecordBatch| {
            let w = match &mut writer {
                Some(w) => w,
                None => match StreamWriter::try_new(Vec::new(), &batch.schema()) {
                    Ok(w) => {
                        columns = names(&batch.schema());
                        writer.insert(w)
                    }
                    Err(e) => {
                        failed = Some(io(e));
                        return Err(burrmill::BurrmillError::Cancelled);
                    }
                },
            };
            let mut start = 0;
            while start < batch.num_rows() {
                if written >= cap || w.get_ref().len() >= max_bytes {
                    over = true;
                    return Err(burrmill::BurrmillError::LimitExceeded("cap".into()));
                }
                let n = (batch.num_rows() - start)
                    .min(ENCODE_ROWS)
                    .min(cap - written);
                if let Err(e) = w.write(&batch.slice(start, n)) {
                    failed = Some(io(e));
                    return Err(burrmill::BurrmillError::Cancelled);
                }
                start += n;
                written += n;
            }
            Ok(())
        };
        #[cfg(not(target_arch = "wasm32"))]
        let r = self.engine.sql_for_each(sql, &mut take);
        #[cfg(target_arch = "wasm32")]
        let r = self.engine.sql_async_for_each(sql, &mut take).await;
        if let Some(e) = failed {
            return Err(e);
        }
        let truncated = match r {
            Ok(()) => false,
            Err(_) if over => true,
            Err(e) => return Err(e),
        };
        let mut w = match writer {
            Some(w) => w,
            None => {
                // No batch came: the plan's schema names the columns and opens the stream.
                let schema = self.engine.result_schema(sql)?;
                columns = names(&schema);
                StreamWriter::try_new(Vec::new(), &schema).map_err(io)?
            }
        };
        w.finish().map_err(io)?;
        Ok((w.into_inner().map_err(io)?, columns, written, truncated))
    }

    /// The rows `/sql` answers `sql` with.
    pub async fn rows(&self, sql: &str) -> Result<Vec<Value>> {
        #[cfg(not(target_arch = "wasm32"))]
        let batches = self.engine.sql(sql).map_err(engine_err)?;
        #[cfg(target_arch = "wasm32")]
        let batches = self.engine.sql_async(sql).await.map_err(engine_err)?;
        let mut out = Vec::new();
        for b in &batches {
            out.extend(encode(b).map_err(engine_err)?);
        }
        Ok(out)
    }

    /// `/sql`'s rows, columns and whether more than `cap` rows were left out, as
    /// `engine_burrmill`'s `collect` gives them.
    pub async fn collect(
        &self,
        sql: &str,
        cap: usize,
    ) -> Result<(Vec<Value>, Vec<String>, bool), burrmill::BurrmillError> {
        let hard = cap + 1;
        let mut bytes = 0usize;
        let mut rows = Vec::new();
        let mut over = false;
        let mut columns: Vec<String> = Vec::new();
        // Streamed and capped by bytes as the node's `engine_burrmill::collect` is, so a wide result
        // stops at `SQL_MAX_RESULT_BYTES` instead of being held whole first (#1650).
        let mut take = |batch: arrow_array::RecordBatch| {
            if columns.is_empty() {
                columns = batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect();
            }
            let mut start = 0;
            while start < batch.num_rows() {
                let n = (batch.num_rows() - start).min(ENCODE_ROWS);
                let part = batch.slice(start, n);
                start += n;
                // A slice that could cross the cap goes a row at a time, so a cell wider than the cap
                // is refused from its Arrow length before it becomes a `Value`.
                let parts = if bytes.saturating_add(arrow_bytes(&part)) < SQL_MAX_RESULT_BYTES {
                    vec![part]
                } else {
                    (0..part.num_rows()).map(|i| part.slice(i, 1)).collect()
                };
                for part in parts {
                    if bytes.saturating_add(arrow_bytes(&part)) >= SQL_MAX_RESULT_BYTES {
                        over = true;
                        return Err(burrmill::BurrmillError::LimitExceeded("cap".into()));
                    }
                    for row in encode(&part)? {
                        bytes += row
                            .as_object()
                            .map(|o| {
                                o.iter()
                                    .map(|(k, v)| k.len() + value_bytes(v))
                                    .sum::<usize>()
                            })
                            .unwrap_or(0);
                        rows.push(row);
                        if rows.len() >= hard || bytes >= SQL_MAX_RESULT_BYTES {
                            over = true;
                            return Err(burrmill::BurrmillError::LimitExceeded("cap".into()));
                        }
                    }
                }
            }
            Ok(())
        };
        #[cfg(not(target_arch = "wasm32"))]
        let r = self.engine.sql_for_each(sql, &mut take);
        #[cfg(target_arch = "wasm32")]
        let r = self.engine.sql_async_for_each(sql, &mut take).await;
        let truncated = match r {
            Ok(()) => false,
            Err(_) if over => true,
            Err(e) => return Err(e),
        };
        rows.truncate(cap);
        if columns.is_empty() {
            if let Ok(described) = self.engine.describe(sql) {
                columns = described.into_iter().map(|(name, _)| name).collect();
            }
        }
        Ok((rows, columns, truncated))
    }

    /// Plan `sql` without running it, as `/explain` does.
    pub fn plan(&self, sql: &str) -> Result<(), burrmill::BurrmillError> {
        self.engine.describe(sql).map(|_| ())
    }
}

/// The base tables `sql` reads, lowercased, followed through the authored views, as the node's
/// `reject_unknown_table_refs` and `expand_through_views` find them. `None` where it does not parse.
pub fn referenced_tables(
    sql: &str,
    files: &NestFiles,
) -> Option<Result<BTreeSet<String>, burrmill::BurrmillError>> {
    let named = match burrmill::inspect::reach(sql) {
        Ok(r) => r.tables,
        Err(burrmill::BurrmillError::Parse(_)) => return None,
        Err(e) => return Some(Err(e)),
    };
    let bodies = view_bodies(files).unwrap_or_default();
    Some(Ok(nuthatch_core::bind::through_views(&named, |name| {
        burrmill::inspect::base_tables(bodies.get(name)?)
    })))
}

/// Each authored view's lowercased name and the query it registers.
fn view_bodies(files: &NestFiles) -> Result<std::collections::BTreeMap<String, String>> {
    let mut out = std::collections::BTreeMap::new();
    for (path, text) in files {
        if path.starts_with("views/") && path.ends_with(".sql") {
            let stmts =
                sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::DuckDbDialect {}, text)?;
            for s in stmts {
                if let sqlparser::ast::Statement::CreateView(v) = s {
                    let name = v.name.to_string().trim_matches('"').to_ascii_lowercase();
                    out.insert(name, v.query.to_string());
                }
            }
        }
    }
    Ok(out)
}

/// Rows in an order neither engine chose: each by its text with the keys sorted, as
/// `tests/e2e_tab_entities.rs` orders the node's.
pub fn sorted(rows: Vec<Value>) -> Vec<Value> {
    let mut out: Vec<(String, Value)> = rows
        .into_iter()
        .map(|r| {
            let canon: std::collections::BTreeMap<&String, &Value> = r
                .as_object()
                .map(|o| o.iter().collect())
                .unwrap_or_default();
            (serde_json::to_string(&canon).unwrap_or_default(), r.clone())
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out.into_iter().map(|(_, r)| r).collect()
}

/// Every view the authored files create, in file then statement order.
pub fn view_names(files: &NestFiles) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for (path, text) in files {
        if path.starts_with("views/") && path.ends_with(".sql") {
            let stmts =
                sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::DuckDbDialect {}, text)?;
            for s in stmts {
                if let sqlparser::ast::Statement::CreateView(v) = s {
                    out.push(v.name.to_string().trim_matches('"').to_ascii_lowercase());
                }
            }
        }
    }
    Ok(out)
}

fn key(file: &str) -> String {
    format!("{SEGMENTS_PREFIX}{file}")
}

/// An entity's relation, bound as the node's `BurrmillSession::bind_relation` binds it.
fn bind_relation(engine: &mut burrmill::Engine, entity: &TabEntity) -> Result<()> {
    let table = entity.name_lower();
    let (raw, rows, view) =
        nuthatch_core::bind::relation(&table, &entity.relation_types(), &entity.rows());
    engine.register_rows(&raw, &rows).map_err(engine_err)?;
    engine.register_view(&table, &view).map_err(engine_err)
}

/// Each `CREATE VIEW` in one authored file, in order, as the node's session registers it.
fn define_views(engine: &mut burrmill::Engine, text: &str) -> Result<()> {
    use sqlparser::ast::Statement;
    let stmts = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::DuckDbDialect {}, text)?;
    for stmt in stmts {
        if let Statement::CreateView(v) = stmt {
            let name = v.name.to_string().trim_matches('"').to_ascii_lowercase();
            engine
                .register_view(&name, &v.query.to_string())
                .map_err(engine_err)?;
        }
    }
    Ok(())
}
