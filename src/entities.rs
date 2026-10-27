//! RFC-0041 slice one: explicit authored incremental-entity declarations and conservative refusal.

use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlparser::ast::{
    self as ast, DuplicateTreatment, FunctionArguments, GroupByExpr, Statement, TableFactor, Visit,
    Visitor,
};
use std::collections::BTreeSet;
use std::ops::ControlFlow;
use std::path::Path;
use std::time::Duration;

pub const ENTITY_FILE: &str = "entities.toml";
pub const ENTITY_COMPILER_ID: &str = "nuthatch-rfc0041-v1";

/// The durable definition of an authored incremental entity. Unlike an ordinary query result it
/// deliberately has no block range: this identifies the compiler contract and is stable while the
/// maintained state advances. A snapshot or cache adds its covered range separately.
#[derive(Debug, Clone)]
pub struct EntityIdentity {
    pub plan: crate::graft::CanonicalPlan,
    /// Reuse keys of earlier entities read by this entity, not their names.
    pub input_entity_keys: Vec<String>,
    /// Resolved decoded-table identities, never just aliases such as `usdc__transfer`.
    pub sources: Vec<crate::graft::SourceIdentity>,
    /// Offchain snapshot tables read (#1437), kept apart from chain sources.
    pub offchain_sources: Vec<OffchainSourceIdentity>,
    /// Ordered because it defines the point-read tuple exposed by the entity.
    pub key: Vec<String>,
    /// Ordered because a relation's output is positional at the compiler boundary.
    pub output_schema: Vec<String>,
    pub engine: String,
}

impl EntityIdentity {
    /// Stable identity for one entity compiler contract. A changed lowerer, engine version, decoded
    /// source, upstream entity, key shape or output shape must rebuild rather than graft. `Derivation` supplies the carefully length-delimited source and input hashing;
    /// this extension adds the parts unique to maintained keyed state.
    pub fn reuse_key(&self) -> String {
        let derivation = crate::graft::Derivation {
            name: String::new(),
            plan: self.plan.clone(),
            input_keys: self.input_entity_keys.clone(),
            sources: self.sources.clone(),
            // This is a definition key. Using a fixed range prevents a newly indexed block from
            // making an otherwise unchanged entity look like a different program.
            range: (0, 0),
            engine: format!("{ENTITY_COMPILER_ID}/{}", self.engine),
            finality: crate::graft::Finality::Final,
        };
        let offchain = self.offchain_sources.iter().flat_map(|s| {
            ["offchain".to_string(), s.table.clone()].into_iter().chain(
                s.columns
                    .iter()
                    .flat_map(|c| [c.name.clone(), c.kind.name().to_string()]),
            )
        });
        let mut hash = Sha256::new();
        hash.update(b"nuthatch-entity-reuse-key-v1\0");
        for part in std::iter::once(derivation.reuse_key())
            .chain(self.key.iter().cloned())
            .chain(self.output_schema.iter().cloned())
            .chain(offchain)
        {
            hash.update((part.len() as u64).to_le_bytes());
            hash.update(part.as_bytes());
        }
        hex::encode(hash.finalize())
    }
}

/// One offchain table an entity reads.
///
/// `snapshots` is what the entity has applied, not part of its definition, so it stays out of
/// [`EntityIdentity::reuse_key`] for the reason that key has no block range: an appended snapshot
/// advances the entity rather than making it a different program. A replaced, removed or reordered
/// snapshot is a rebuild, which [`crate::entity_offchain::advance`] decides from this list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffchainSourceIdentity {
    pub table: String,
    /// The columns the entity reads, in the plan's order, with the kind each binds as.
    pub columns: Vec<crate::entity_offchain::Column>,
    /// Content hashes of the applied snapshots, in append order.
    pub snapshots: Vec<String>,
}

/// Construct the entity definition identity from the engine's parser. A parser failure is represented
/// as raw text by `canonical_plan`, which can only forfeit reuse, never make two meanings collide.
pub fn identity(
    sql: &str,
    input_entity_keys: Vec<String>,
    sources: Vec<crate::graft::SourceIdentity>,
    offchain_sources: Vec<OffchainSourceIdentity>,
    key: Vec<String>,
    output_schema: Vec<String>,
) -> Result<EntityIdentity> {
    let parser = crate::graft::Parser::new()?;
    Ok(EntityIdentity {
        plan: parser.canonical_plan(sql),
        input_entity_keys,
        sources,
        offchain_sources,
        key,
        output_schema,
        engine: parser.engine_version(),
    })
}

#[derive(Debug, Deserialize)]
struct EntityFile {
    #[serde(default)]
    entities: Vec<EntityDecl>,
}

/// One author-declared maintained relation. Parsing is centralised here so startup, `check`, and a
/// future serving surface all act on exactly the file whose bytes form the nest identity.
#[derive(Debug, Clone, Deserialize)]
pub struct EntityDecl {
    pub name: String,
    pub sql: String,
    pub key: Vec<String>,
    pub max_rows: usize,
}

impl EntityDecl {
    /// Resolve the same authored SQL file for validation and runtime startup.
    pub fn read_sql(&self, dir: &Path) -> Result<String> {
        let rel = Path::new(&self.sql);
        if rel.components().count() != 2
            || rel.parent() != Some(Path::new("entities"))
            || rel.extension().and_then(|x| x.to_str()) != Some("sql")
        {
            bail!(
                "sql must name one entities/<name>.sql file; move the SQL into entities/{}.sql and set sql = \"entities/{}.sql\"",
                self.name, self.name
            );
        }
        if rel.file_stem().and_then(|s| s.to_str()) != Some(self.name.as_str()) {
            bail!("entity name must match the declared SQL filename");
        }
        std::fs::read_to_string(dir.join(rel)).with_context(|| format!("cannot read {}", self.sql))
    }
}

/// Validation failures are collected so `nuthatch check` names every bad declaration at once.
#[derive(Debug, Clone)]
pub struct EntityIssue {
    pub name: String,
    pub error: String,
}

/// Whether this directory asks `check` to validate authored incremental state at all. A valid
/// entity-only nest is useful before it has parity checks, so it must not be mistaken for a nest
/// with nothing to validate merely because validation found no errors.
pub fn has_declarations(dir: &Path) -> bool {
    dir.join(ENTITY_FILE).is_file()
        || std::fs::read_dir(dir.join("entities"))
            .ok()
            .into_iter()
            .flatten()
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .any(|path| path.extension().and_then(|x| x.to_str()) == Some("sql"))
}

/// Load the authored entity declarations. An absent manifest means this nest has no incremental
/// entities, which is the ordinary case. A present but malformed manifest is an error rather than an
/// empty list: treating a typo as "no runtime needed" would make maintained state disappear.
pub fn load(dir: &Path) -> Result<Vec<EntityDecl>> {
    let path = dir.join(ENTITY_FILE);
    let raw = match std::fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
    };
    let file: EntityFile =
        toml::from_str(&raw).with_context(|| format!("parse {}", path.display()))?;
    // `[[entites]]` parses as no entities. The nest then maintains nothing and says so only here.
    crate::config::warn_unknown::<EntityFile>(ENTITY_FILE, &raw);
    crate::analytics::hold_relations(dir, &file.entities);
    Ok(file.entities)
}

/// #1656. `[[entites]]` is not a declaration this build reads.
pub fn refuse_unknown_keys(dir: &Path) -> Result<()> {
    crate::config::refuse_unknown_file::<EntityFile>(dir, ENTITY_FILE)
}

pub fn validate(dir: &Path) -> Vec<EntityIssue> {
    let registry = crate::config::Config::load(dir)
        .ok()
        .and_then(|cfg| crate::registry::from_nest(dir, &cfg).ok());
    let schema = registry.as_ref().map(|registry| registry.schema());
    let data = std::cell::OnceCell::new();
    let path = dir.join(ENTITY_FILE);
    let raw = match std::fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return undeclared_files(dir),
        Err(e) => {
            return vec![issue(
                ENTITY_FILE,
                format!("cannot read {}: {e}", path.display()),
            )]
        }
    };
    let file: EntityFile = match toml::from_str(&raw) {
        Ok(file) => file,
        Err(e) => return vec![issue(ENTITY_FILE, format!("invalid entities.toml: {e}"))],
    };
    let mut issues = Vec::new();
    if file.entities.is_empty() {
        issues.push(issue(
            ENTITY_FILE,
            "declares no entities; remove entities.toml until an incremental relation is ready",
        ));
    }
    let mut graph = Vec::new();
    let mut names = BTreeSet::new();
    let mut declared = BTreeSet::new();
    for entity in file.entities {
        let name = entity.name.clone();
        if !names.insert(name.clone()) {
            issues.push(issue(&name, "duplicate entity name"));
        }
        if crate::entity_offchain::table_of(&name).is_some() {
            issues.push(issue(
                &name,
                "entity name is inside the offchain__ namespace and would shadow an offchain table",
            ));
        }
        if entity.key.is_empty() {
            issues.push(issue(&name, "key must name at least one output column"));
        }
        if entity.max_rows == 0 {
            issues.push(issue(&name, "max_rows must be greater than zero"));
        }
        declared.insert(entity.sql.clone());
        let sql = match entity.read_sql(dir) {
            Ok(sql) => sql,
            Err(e) => {
                issues.push(issue(&name, format!("{e:#}")));
                continue;
            }
        };
        if let Err(e) = validate_sql(&sql) {
            issues.push(issue(&name, e.to_string()));
        } else {
            match dependencies(&sql) {
                Ok(deps) => graph.push((name.clone(), deps)),
                Err(e) => issues.push(issue(
                    &name,
                    format!("cannot determine entity dependencies: {e}"),
                )),
            }
            // `dev` starts an entity by lowering and binding it (`indexer::start_entities`), so
            // `check` refuses whatever those refuse, with their words (#1590).
            let plan = match crate::entity_lower::lower(&sql) {
                Ok(plan) => plan,
                Err(e) => {
                    issues.push(issue(&name, format!("{e:#}")));
                    continue;
                }
            };
            if let (Some(registry), Some(schema)) = (&registry, &schema) {
                let data = match data.get_or_init(|| NestData::load(dir)) {
                    Ok(data) => data,
                    Err(e) => {
                        issues.push(issue(&name, format!("{e:#}")));
                        continue;
                    }
                };
                if let Err(e) = bind_as_dev(dir, &name, &plan, registry) {
                    issues.push(issue(&name, format!("{e:#}")));
                    continue;
                }
                let reads = wide_reads(schema, &plan);
                match first_unnarrowable(dir, schema, data, &reads) {
                    Ok(None) => {}
                    Ok(Some(found)) => {
                        issues.push(issue(
                            &name,
                            format!(
                                "{found} does not fit the 128-bit integer the entity reads it \
                                 as; `dev` would fault on that row"
                            ),
                        ));
                        continue;
                    }
                    Err(e) => {
                        issues.push(issue(&name, format!("{e:#}")));
                        continue;
                    }
                }
                let sql = match typed_for_check(&sql, &reads) {
                    Ok(sql) => sql,
                    Err(e) => {
                        issues.push(issue(&name, format!("entity SQL does not bind: {e}")));
                        continue;
                    }
                };
                match crate::analytics::entity_output_columns(dir, schema, &sql) {
                    Ok(columns) => {
                        for key in &entity.key {
                            if !columns
                                .iter()
                                .any(|column| column.eq_ignore_ascii_case(key))
                            {
                                issues.push(issue(
                                    &name,
                                    format!("declared key `{key}` is not an output column"),
                                ));
                            }
                        }
                        if entity.key.len() != entity.key.iter().collect::<BTreeSet<_>>().len() {
                            issues.push(issue(&name, "declared key repeats a column"));
                        }
                        if !entity.key.is_empty() && !issues.iter().any(|i| i.name == name) {
                            match nest_query(
                                dir,
                                schema,
                                data,
                                &sql,
                                crate::analytics::QueryGuard {
                                    // Authoring validation must be bounded too. `max_rows` is an
                                    // executable admission contract, not permission to materialise
                                    // an unlimited reference result during `nuthatch check`.
                                    timeout: Duration::from_secs(60),
                                    max_rows: entity.max_rows.saturating_add(1),
                                    ..Default::default()
                                },
                            ) {
                                Ok(result) => {
                                    let rows = result.rows;
                                    if result.truncated || rows.len() > entity.max_rows {
                                        issues.push(issue(
                                            &name,
                                            format!(
                                                "reference result exceeds declared max_rows ({})",
                                                entity.max_rows
                                            ),
                                        ));
                                        continue;
                                    }
                                    let mut seen = BTreeSet::new();
                                    for row in rows {
                                        let values: Vec<&Value> = entity
                                            .key
                                            .iter()
                                            .filter_map(|key| row.get(key))
                                            .collect();
                                        if values.len() != entity.key.len()
                                            || values.iter().any(|value| value.is_null())
                                        {
                                            issues.push(issue(
                                                &name,
                                                "declared key is nullable in the reference result",
                                            ));
                                            break;
                                        }
                                        if !seen.insert(
                                            serde_json::to_string(&values).unwrap_or_default(),
                                        ) {
                                            issues.push(issue(
                                            &name,
                                            "declared key is not unique in the reference result",
                                        ));
                                            break;
                                        }
                                    }
                                }
                                Err(e) => issues.push(issue(
                                    &name,
                                    format!("entity reference query failed: {e}"),
                                )),
                            }
                        }
                    }
                    Err(e) => issues.push(issue(&name, format!("entity SQL does not bind: {e}"))),
                }
            }
        }
    }
    if let Some(cycle) = dependency_cycle(&graph) {
        issues.push(issue(
            ENTITY_FILE,
            format!(
                "incremental entity dependency cycle: {}",
                cycle.join(" -> ")
            ),
        ));
    }
    for mut missing in undeclared_files(dir) {
        let path = missing
            .name
            .strip_prefix("entities/")
            .unwrap_or(&missing.name)
            .to_string();
        if declared.contains(&format!("entities/{path}")) {
            continue;
        }
        missing.error = "entity SQL file has no entities.toml declaration".into();
        issues.push(missing);
    }
    issues
}

/// Lower and bind each declared entity as `dev` starts it, record its output types, and return the
/// names that bound (#1599): the relations an authored view may read. One that does not bind is left
/// out, and `validate` reports why.
pub(crate) fn hold_declared_relations(dir: &Path) -> Vec<String> {
    let Some(registry) = crate::config::Config::load(dir)
        .ok()
        .and_then(|cfg| crate::registry::from_nest(dir, &cfg).ok())
    else {
        return Vec::new();
    };
    let Ok(decls) = load(dir) else {
        return Vec::new();
    };
    decls
        .into_iter()
        .filter_map(|decl| {
            let sql = decl.read_sql(dir).ok()?;
            let (plan, columns) = crate::entity_lower::lower_with_columns(&sql).ok()?;
            let (binding, _) = bind_as_dev(dir, &decl.name, &plan, &registry).ok()?;
            crate::analytics::hold_relation_types(
                dir,
                &decl.name,
                &columns,
                &binding.output_types(&plan),
            );
            Some(decl.name)
        })
        .collect()
}

/// Bind a lowered entity to this nest the way it is started: its name must not shadow a decoded or
/// offchain table, and every table and column it reads must exist. `dev` and `check` both call this,
/// so neither can accept an entity the other refuses (#1590).
pub(crate) fn bind_as_dev(
    dir: &Path,
    name: &str,
    plan: &crate::entity_plan::Plan,
    registry: &crate::registry::DecodeRegistry,
) -> Result<(crate::entity_bind::Binding, crate::entity_offchain::Tables)> {
    // An entity that shadows a decoded table would silently take that table's name on the
    // analytical surface, so `SELECT * FROM usdc__transfer` would answer from a maintained
    // relation instead of the facts. Refused at load, where it is a typo, rather than at the
    // first query, where it is a mystery.
    if let Some(t) = registry
        .schema()
        .iter()
        .find(|t| t.table.eq_ignore_ascii_case(name))
    {
        bail!(
            "entity `{name}` has the same name as the decoded table `{}`. Rename the entity: on the \
             SQL surface one would shadow the other",
            t.table
        )
    }
    if crate::entity_offchain::table_of(name).is_some() {
        bail!(
            "entity `{name}` is named inside the `{}` namespace, where it would shadow an offchain \
             table on the SQL surface. Rename the entity",
            crate::entity_offchain::OFFCHAIN_NAMESPACE
        )
    }
    // The manifest is read only for an entity that names an offchain table, so a damaged one
    // cannot stop a chain-only nest from starting.
    let offchain = if crate::entity_offchain::reads_offchain(plan) {
        crate::entity_offchain::Tables::load(dir)?
    } else {
        crate::entity_offchain::Tables::none()
    };
    let binding = crate::entity_bind::Binding::bind_with_offchain(plan, registry, &offchain)
        .with_context(|| format!("binding entity `{name}` to this nest's tables"))?;
    Ok((binding, offchain))
}

/// Whether a decoded parameter is sealed as decimal text but read by the entity circuit as a checked
/// `i128`: every integer, not only the wide ones, since a `uint24` is text on the SQL surface too.
fn is_integer(column: &crate::registry::ColumnSchema) -> bool {
    matches!(column.storage.as_str(), "u64" | "i64" | "word16" | "word32")
}

/// Per table the circuit reads, keyed case-insensitively: its name and the wide columns the plan
/// reads from it, across both sides of a self-join. Only these are cast and probed, because the
/// circuit converts nothing else (#1587).
type WideReads = std::collections::BTreeMap<String, (String, BTreeSet<String>)>;

fn wide_reads(
    schema: &[crate::registry::TableSchema],
    plan: &crate::entity_plan::Plan,
) -> WideReads {
    let mut reads = WideReads::new();
    for source in std::iter::once(&plan.left).chain(plan.join.as_ref().map(|j| &j.right)) {
        // The registry keeps a schema entry per decoder, so a table can appear more than once.
        for t in schema
            .iter()
            .filter(|t| t.table.eq_ignore_ascii_case(&source.table))
        {
            let read = t.columns.iter().filter(|c| {
                is_integer(c)
                    && source
                        .columns
                        .iter()
                        .any(|s| s.eq_ignore_ascii_case(&c.name))
            });
            reads
                .entry(t.table.to_ascii_lowercase())
                .or_insert_with(|| (t.table.clone(), BTreeSet::new()))
                .1
                .extend(read.map(|c| c.name.clone()));
        }
    }
    reads.retain(|_, (_, cols)| !cols.is_empty());
    reads
}

/// `sql` as `check` binds it (#1587): each wide column the circuit reads as `HUGEINT`, the checked
/// `i128` it narrows them to. The analytical views keep the exact decimal string, which no aggregate
/// accepts. Table qualifiers are dropped first, since the lowerer reads only the table name and
/// `main.t` would otherwise reach past the typed CTE. Entity SQL admits no CTEs of its own.
fn typed_for_check(sql: &str, reads: &WideReads) -> Result<String> {
    if reads.is_empty() {
        return Ok(sql.to_string());
    }
    let mut statements =
        sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::DuckDbDialect {}, sql)?;
    let mut qualified = false;
    let _ = ast::visit_relations_mut(&mut statements, |name| {
        if name.0.len() > 1 {
            name.0.drain(..name.0.len() - 1);
            qualified = true;
        }
        ControlFlow::<()>::Continue(())
    });
    let sql = match statements.as_slice() {
        [statement] if qualified => statement.to_string(),
        _ => sql.to_string(),
    };
    let ctes: Vec<String> = reads
        .values()
        .map(|(table, cols)| {
            let replaced: Vec<String> = cols
                .iter()
                .map(|c| format!("CAST({0} AS HUGEINT) AS {0}", quote_ident(c)))
                .collect();
            format!(
                "{0} AS (SELECT * REPLACE ({1}) FROM main.{0})",
                quote_ident(table),
                replaced.join(", ")
            )
        })
        .collect();
    Ok(format!("WITH {}\n{sql}", ctes.join(",\n")))
}

fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// The first stored value the circuit could not narrow, as `table.column = value` (#1587). The
/// circuit converts every column it reads on every row it is fed, before any `WHERE`, so this scans
/// whole columns rather than what the entity's own query keeps. A probe that cannot finish is an
/// error, not a pass: an unscanned column proves nothing.
/// What `check` reads an entity's inputs from: sealed history and, when the store is free, the hot
/// tail, since the circuit seeds from both (#1587). `dev` holding the store leaves the hot tail
/// unread, and `check` says so rather than implying it looked.
struct NestData {
    hot: crate::analytics::HotRows,
    sealed_through: u64,
}

impl NestData {
    /// Sealed history only when there is no store yet, or when a running nuthatch holds it and so
    /// feeds that tail to its entities itself. Any other failure to read the store is an error: an
    /// unread hot tail must not pass as a checked one.
    ///
    /// The store is opened as `nuthatch sql` opens it, so for the length of the scan a starting `dev`
    /// is refused its lock; the scan is bounded to what `/sql` will read for the same reason.
    fn load(dir: &Path) -> Result<Self> {
        let cold = NestData {
            hot: Default::default(),
            sealed_through: u64::MAX,
        };
        let db = dir.join(crate::config::DB_FILE);
        if !db.exists() {
            return Ok(cold);
        }
        let store = match crate::store::Store::open_existing(&db) {
            Ok(store) => store,
            Err(e) if held_by_another_process(&e) => {
                tracing::warn!(
                    "entities checked against sealed history only: a running nuthatch holds {}, and \
                     feeds its hot tail to its entities itself",
                    db.display()
                );
                return Ok(cold);
            }
            Err(e) => return Err(e.context("reading the hot store to check entities against it")),
        };
        let hot = store
            .hot_rows_by_table_bounded(crate::serve::SQL_MAX_HOT_ROWS)
            .context("reading the hot store to check entities against it")?;
        Ok(NestData {
            hot,
            sealed_through: store.sealed_through(),
        })
    }
}

/// Whether opening the store failed only because another process has it open.
fn held_by_another_process(e: &anyhow::Error) -> bool {
    e.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<redb::DatabaseError>(),
            Some(redb::DatabaseError::DatabaseAlreadyOpen)
        )
    })
}

/// A query over the nest's data with every declared table bound. The declared schema gives a table
/// that has never been sealed its empty typed view: without it a fresh nest, which is when an author
/// runs `check`, failed every entity with "failed to prepare query".
fn nest_query(
    dir: &Path,
    schema: &[crate::registry::TableSchema],
    data: &NestData,
    sql: &str,
    guard: crate::analytics::QueryGuard,
) -> Result<crate::analytics::QueryOutput> {
    crate::analytics::query_hot_cold(dir, sql, guard, &data.hot, data.sealed_through, schema)
}

fn first_unnarrowable(
    dir: &Path,
    schema: &[crate::registry::TableSchema],
    data: &NestData,
    reads: &WideReads,
) -> Result<Option<String>> {
    for (table, cols) in reads.values() {
        for col in cols {
            let c = quote_ident(col);
            let probe = format!(
                "SELECT CAST({c} AS VARCHAR) AS v FROM {} \
                 WHERE {c} IS NOT NULL AND TRY_CAST({c} AS HUGEINT) IS NULL LIMIT 1",
                quote_ident(table)
            );
            let guard = crate::analytics::QueryGuard {
                timeout: Duration::from_secs(60),
                max_rows: 1,
                ..Default::default()
            };
            let out = nest_query(dir, schema, data, &probe, guard).with_context(|| {
                format!("checking {table}.{col} fits the entity's integer type")
            })?;
            if out.degraded() {
                bail!(
                    "checking {table}.{col} fits the entity's integer type: some of its segments \
                     could not be read"
                );
            }
            if let Some(v) = out.rows.first().and_then(|r| r.get("v")) {
                return Ok(Some(format!("{table}.{col} = {v}")));
            }
        }
    }
    Ok(None)
}

/// Aggregates whose maintenance under insert **and retraction** the v1 lowerer can express.
///
/// Short by design and an **allowlist**, which is the whole point (#836). The refusal list used to
/// enumerate what was forbidden - `MEDIAN`, `MODE`, `PERCENTILE_*` - over a vocabulary the engine
/// owns and grows, and it was wrong in both the ways `analytics.rs` predicts a denylist is wrong. About
/// **coverage**: this build knows 88 distinct aggregate names, of which the list named three, so
/// `quantile_cont`, `arg_max`, `string_agg`, `list`, `first`, `histogram` and the rest were admitted
/// as incrementally maintainable. And about **spelling**: `PERCENTILE_CONT` is the SQL-standard alias
/// while `quantile_cont` is the name DuckDB used, so the list blocked the alias and admitted the
/// real thing.
///
/// `count_star` is the name the parse gives `count(*)` (`entity_lower::canonical_function_name`).
const INCREMENTAL_AGGREGATES: &[&str] = &["sum", "min", "max", "avg", "count", "count_star"];

/// Every name DuckDB 1.5's catalogue classifies as an aggregate: what the gate treats as one.
///
/// Frozen from `duckdb_functions()` when the gate stopped asking DuckDB, and not checked against
/// any engine since: DuckDB is no longer linked. A name
/// here and not in [`INCREMENTAL_AGGREGATES`] is refused; an aggregate some later engine adds is not
/// here, and is refused at lowering instead, which admits only the six.
const AGGREGATES: &[&str] = &[
    "any_value",
    "approx_count_distinct",
    "approx_quantile",
    "approx_top_k",
    "arbitrary",
    "arg_max",
    "arg_max_null",
    "arg_max_nulls_last",
    "arg_min",
    "arg_min_null",
    "arg_min_nulls_last",
    "argmax",
    "argmin",
    "array_agg",
    "avg",
    "bit_and",
    "bit_or",
    "bit_xor",
    "bitstring_agg",
    "bool_and",
    "bool_or",
    "corr",
    "count",
    "count_if",
    "count_star",
    "countif",
    "covar_pop",
    "covar_samp",
    "cume_dist",
    "dense_rank",
    "entropy",
    "favg",
    "fill",
    "first",
    "first_value",
    "fsum",
    "group_concat",
    "histogram",
    "histogram_exact",
    "kahan_sum",
    "kurtosis",
    "kurtosis_pop",
    "lag",
    "last",
    "last_value",
    "lead",
    "list",
    "listagg",
    "mad",
    "max",
    "max_by",
    "mean",
    "median",
    "min",
    "min_by",
    "mode",
    "nth_value",
    "ntile",
    "percent_rank",
    "product",
    "quantile",
    "quantile_cont",
    "quantile_disc",
    "rank",
    "rank_dense",
    "regr_avgx",
    "regr_avgy",
    "regr_count",
    "regr_intercept",
    "regr_r2",
    "regr_slope",
    "regr_sxx",
    "regr_sxy",
    "regr_syy",
    "reservoir_quantile",
    "row_number",
    "sem",
    "skewness",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "string_agg",
    "sum",
    "sum_no_overflow",
    "sumkahan",
    "var_pop",
    "var_samp",
    "variance",
];

/// Which of `names` are aggregates.
fn aggregates_among(names: &BTreeSet<String>) -> BTreeSet<String> {
    names
        .iter()
        .filter(|n| AGGREGATES.contains(&n.as_str()))
        .cloned()
        .collect()
}

/// What the gate reads off a parsed statement, in one walk of all of it: subqueries, CTE bodies and
/// table function arguments included.
#[derive(Default)]
struct Facts {
    volatile: bool,
    /// Every call, by its canonical name (`entity_lower::canonical_function_name`). A window call is
    /// not one.
    functions: BTreeSet<String>,
    /// A scalar `(SELECT ...)`, `IN (SELECT ...)`, `EXISTS`, or a quantified comparison. A derived
    /// table in `FROM` is not one: it is an ordinary relation.
    expression_subquery: bool,
    distinct_aggregate: bool,
    tables: BTreeSet<String>,
}

impl Facts {
    fn of(statements: &[Statement]) -> Self {
        let mut facts = Facts::default();
        for statement in statements {
            let _ = statement.visit(&mut facts);
        }
        facts
    }

    fn call(&mut self, name: String) {
        self.volatile |= crate::graft::VOLATILE_FUNCTIONS.contains(&name.as_str());
        self.functions.insert(name);
    }
}

impl Visitor for Facts {
    type Break = ();

    fn pre_visit_expr(&mut self, e: &ast::Expr) -> ControlFlow<()> {
        use ast::Expr as E;
        match e {
            // A bare `current_date` can parse as a column, and the volatile list names it;
            // `t.current_date` is an ordinary column.
            E::Identifier(i) => {
                self.volatile |= crate::graft::VOLATILE_FUNCTIONS
                    .contains(&i.value.to_ascii_lowercase().as_str());
            }
            E::Function(f) if f.over.is_none() => match &f.args {
                FunctionArguments::None => {
                    if let [name] = crate::entity_lower::object_name_parts(&f.name).as_slice() {
                        self.volatile |= crate::graft::VOLATILE_FUNCTIONS
                            .contains(&name.to_ascii_lowercase().as_str());
                    }
                }
                FunctionArguments::List(list) => {
                    self.distinct_aggregate |=
                        matches!(list.duplicate_treatment, Some(DuplicateTreatment::Distinct));
                    self.call(crate::entity_lower::canonical_function_name(f));
                }
                FunctionArguments::Subquery(_) => {
                    self.expression_subquery = true;
                    self.call(crate::entity_lower::canonical_function_name(f));
                }
            },
            E::Subquery(_) | E::Exists { .. } | E::InSubquery { .. } => {
                self.expression_subquery = true
            }
            E::AnyOp { right, .. } | E::AllOp { right, .. }
                if matches!(right.as_ref(), E::Subquery(_)) =>
            {
                self.expression_subquery = true
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, t: &TableFactor) -> ControlFlow<()> {
        if let TableFactor::Table { name, args, .. } = t {
            let mut parts = crate::entity_lower::object_name_parts(name);
            match args {
                None => self.tables.extend(parts.pop()),
                Some(_) => self.call(parts.pop().unwrap_or_default().to_ascii_lowercase()),
            }
        }
        ControlFlow::Continue(())
    }
}

/// How many grouping sets a `GROUP BY` makes: a plain list is one, however long.
fn grouping_sets(group_by: &GroupByExpr) -> usize {
    let GroupByExpr::Expressions(exprs, _) = group_by else {
        return 0;
    };
    exprs
        .iter()
        .map(|e| match e {
            ast::Expr::Rollup(sets) => sets.len() + 1,
            ast::Expr::Cube(sets) => 1 << sets.len().min(usize::BITS as usize - 1),
            ast::Expr::GroupingSets(sets) => sets.len(),
            _ => 1,
        })
        .fold(if exprs.is_empty() { 0 } else { 1 }, usize::saturating_mul)
}

const ONE_SELECT: &str = "entity must contain exactly one SELECT; keep other SQL as views/*.sql";

fn validate_sql(sql: &str) -> Result<()> {
    let statements = match crate::entity_lower::parse(sql) {
        Ok(statements) => statements,
        Err(_) if uses_sample(sql) => {
            bail!("USING SAMPLE is not incremental v1 SQL; keep this as views/*.sql")
        }
        Err(e) => bail!("{ONE_SELECT} ({e})"),
    };
    let Some((select, _)) = (match statements.as_slice() {
        [Statement::Query(query)] => crate::entity_lower::select_of(query),
        _ => None,
    }) else {
        bail!("{ONE_SELECT}")
    };
    let facts = Facts::of(&statements);
    // `CURRENT_DATE` can arrive as an unqualified column reference rather than a function call,
    // and this walk treats both as the call. A text matcher that gets this wrong turns time into silently
    // frozen state.
    if facts.volatile {
        bail!("volatile functions are not incremental v1 SQL; keep this as views/*.sql")
    }
    // **The allowlist, and the control meant to outlive the token pass below** (#836).
    //
    // Admits only the aggregates the v1 lowerer can maintain, and the name comes from the parsed
    // AST, so `"median"(x)` cannot spell its way past it either.
    for aggregate in aggregates_among(&facts.functions) {
        if !INCREMENTAL_AGGREGATES.contains(&aggregate.as_str()) {
            bail!(
                "`{aggregate}` is not an aggregate incremental v1 can maintain (only {}); \
                 keep this as views/*.sql",
                INCREMENTAL_AGGREGATES.join(", ")
            )
        }
    }
    if facts.expression_subquery {
        bail!(
            "correlated and scalar subqueries are not incremental v1 SQL; keep this as views/*.sql"
        )
    }
    if facts.distinct_aggregate {
        bail!("DISTINCT aggregates are not incremental v1 SQL; keep this as views/*.sql")
    }
    if select.is_some_and(|s| grouping_sets(&s.group_by) > 1) {
        bail!("GROUPING SETS/ROLLUP/CUBE are not incremental v1 SQL; keep this as views/*.sql")
    }

    // Kept *beside* the allowlist rather than replaced by it: two independent controls that must both
    // pass, so a gap in either is covered. These are syntax forms, not function names, so the
    // catalogue above cannot see them.
    let tokens = sql_tokens(sql);
    for (needle, why) in [
        ("DISTINCT", "DISTINCT"),
        ("LIMIT", "LIMIT"),
        ("OVER", "window functions"),
        ("RECURSIVE", "recursive CTEs"),
        ("OUTER", "outer joins"),
        ("EXISTS", "correlated subqueries"),
    ] {
        if tokens.iter().any(|token| token == needle) {
            bail!("{why} is not incremental v1 SQL; keep this as views/*.sql")
        }
    }
    if tokens
        .windows(2)
        .any(|pair| pair[0] == "ORDER" && pair[1] == "BY")
    {
        bail!("ORDER BY is not incremental v1 SQL; keep this as views/*.sql")
    }
    if tokens
        .windows(2)
        .any(|pair| matches!(pair[0].as_str(), "LEFT" | "RIGHT" | "FULL") && pair[1] == "JOIN")
    {
        bail!("outer joins are not incremental v1 SQL; keep this as views/*.sql")
    }
    if tokens.windows(2).any(|pair| {
        (matches!(pair[0].as_str(), "MEDIAN" | "MODE") || pair[0].starts_with("PERCENTILE_"))
            && pair[1] == "("
    }) {
        bail!("holistic aggregates are not incremental v1 SQL; keep this as views/*.sql")
    }
    Ok(())
}

/// `USING SAMPLE`, which sqlparser does not parse: recognised here so its refusal still says what it
/// is rather than that the statement does not parse.
pub(crate) fn uses_sample(sql: &str) -> bool {
    sql_tokens(sql)
        .windows(2)
        .any(|pair| pair[0] == "USING" && pair[1] == "SAMPLE")
}

/// SQL tokens relevant to the refusal list. The parser owns the statement-shape gate above;
/// this only recognises constructs whose AST forms are deliberately not yet lowered. Quoted text and
/// comments are discarded first, so an entity may quite safely produce the string `"ORDER BY"`.
fn sql_tokens(sql: &str) -> Vec<String> {
    #[derive(Clone, Copy, PartialEq)]
    enum State {
        Code,
        /// Inside `"..."` - a name, so its characters accumulate into the current token.
        Ident,
        Quote(char),
        LineComment,
        BlockComment,
    }

    let mut state = State::Code;
    let mut current = String::new();
    let mut tokens = Vec::new();
    let chars: Vec<char> = sql.chars().collect();
    let mut at = 0;
    let flush = |current: &mut String, tokens: &mut Vec<String>| {
        if !current.is_empty() {
            tokens.push(current.to_ascii_uppercase());
            current.clear();
        }
    };
    while at < chars.len() {
        let ch = chars[at];
        let next = chars.get(at + 1).copied();
        match state {
            State::Code if ch == '-' && next == Some('-') => {
                flush(&mut current, &mut tokens);
                state = State::LineComment;
                at += 1;
            }
            State::Code if ch == '/' && next == Some('*') => {
                flush(&mut current, &mut tokens);
                state = State::BlockComment;
                at += 1;
            }
            // A single quote opens a string literal, whose contents are text and are discarded. A
            // double quote opens a **quoted identifier**, whose contents are a *name* - discarding
            // those is what let `"median"(v)` past the refusal list (#836). `analytics.rs` learned
            // the same lesson from `"read_csv"('/etc/passwd')` and fixed it by stripping the quotes
            // rather than the contents; this does the same.
            State::Code if ch == '\'' => {
                flush(&mut current, &mut tokens);
                state = State::Quote(ch);
            }
            State::Code if ch == '"' => {
                flush(&mut current, &mut tokens);
                state = State::Ident;
            }
            State::Code if ch.is_ascii_alphanumeric() || ch == '_' => current.push(ch),
            State::Code if ch == '(' => {
                flush(&mut current, &mut tokens);
                tokens.push("(".into());
            }
            State::Code => flush(&mut current, &mut tokens),
            State::Ident if ch == '"' && next == Some('"') => {
                current.push('"');
                at += 1;
            }
            State::Ident if ch == '"' => state = State::Code,
            State::Ident => current.push(ch),
            State::Quote(quote) if ch == quote && next == Some(quote) => at += 1,
            State::Quote(quote) if ch == quote => state = State::Code,
            State::Quote(_) => {}
            State::LineComment if ch == '\n' => state = State::Code,
            State::LineComment => {}
            State::BlockComment if ch == '*' && next == Some('/') => {
                state = State::Code;
                at += 1;
            }
            State::BlockComment => {}
        }
        at += 1;
    }
    flush(&mut current, &mut tokens);
    tokens
}

/// Referenced relations from the same parse the statement-shape gate reads. The caller resolves
/// these names against fact tables and earlier entities to form the entity DAG.
pub fn dependencies(sql: &str) -> Result<Vec<String>> {
    let statements = crate::entity_lower::parse(sql).map_err(|e| anyhow!("{e}"))?;
    Ok(Facts::of(&statements).tables.into_iter().collect())
}

/// Return one named cycle among entity-to-entity dependencies. Fact tables are absent from `nodes`
/// and therefore terminate a walk; only declared entities can form an invalid recursive graph.
pub fn dependency_cycle(nodes: &[(String, Vec<String>)]) -> Option<Vec<String>> {
    let graph: std::collections::BTreeMap<_, _> = nodes.iter().cloned().collect();
    fn visit(
        node: &str,
        graph: &std::collections::BTreeMap<String, Vec<String>>,
        visiting: &mut Vec<String>,
        done: &mut BTreeSet<String>,
    ) -> Option<Vec<String>> {
        if let Some(at) = visiting.iter().position(|n| n == node) {
            let mut cycle = visiting[at..].to_vec();
            cycle.push(node.into());
            return Some(cycle);
        }
        if !done.insert(node.into()) {
            return None;
        }
        visiting.push(node.into());
        if let Some(deps) = graph.get(node) {
            for dep in deps {
                if graph.contains_key(dep) {
                    if let Some(cycle) = visit(dep, graph, visiting, done) {
                        return Some(cycle);
                    }
                }
            }
        }
        visiting.pop();
        None
    }
    let mut done = BTreeSet::new();
    for node in graph.keys() {
        if let Some(cycle) = visit(node, &graph, &mut Vec::new(), &mut done) {
            return Some(cycle);
        }
    }
    None
}

fn undeclared_files(dir: &Path) -> Vec<EntityIssue> {
    let Ok(entries) = std::fs::read_dir(dir.join("entities")) else {
        return Vec::new();
    };
    entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("sql"))
        .map(|p| {
            issue(
                &p.strip_prefix(dir).unwrap_or(&p).display().to_string(),
                "entity SQL file has no entities.toml declaration",
            )
        })
        .collect()
}

fn issue(name: &str, error: impl Into<String>) -> EntityIssue {
    EntityIssue {
        name: name.into(),
        error: error.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nest() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("entities")).unwrap();
        dir
    }

    fn configured_nest() -> tempfile::TempDir {
        let dir = nest();
        std::fs::write(
            dir.path().join("nuthatch.toml"),
            "[nest]\nname = \"entity-test\"\nchain = \"mainnet\"\nchain_id = 1\nrpc_urls = []\n",
        )
        .unwrap();
        dir
    }

    fn source(contract: &str) -> crate::graft::SourceIdentity {
        crate::graft::SourceIdentity {
            table: "facts".into(),
            chain_id: 1,
            contract: contract.into(),
            event_signature: "Fact(uint256)".into(),
            abi_hash: "ab".repeat(32),
            schema_version: 1,
        }
    }

    #[test]
    fn declaration_is_ast_parsed_and_rejects_non_incremental_shapes() {
        let dir = nest();
        std::fs::write(
            dir.path().join(ENTITY_FILE),
            "[[entities]]\nname='totals'\nsql='entities/totals.sql'\nkey=['owner']\nmax_rows=10\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("entities/totals.sql"),
            "SELECT owner, count(*) AS n FROM facts GROUP BY owner",
        )
        .unwrap();
        assert!(validate(dir.path()).is_empty());

        std::fs::write(
            dir.path().join("entities/totals.sql"),
            "SELECT 1 AS owner; SELECT 2",
        )
        .unwrap();
        assert!(validate(dir.path())
            .iter()
            .any(|i| i.error.contains("exactly one SELECT")));

        std::fs::write(
            dir.path().join("entities/totals.sql"),
            "SELECT DISTINCT 1 AS owner",
        )
        .unwrap();
        assert!(validate(dir.path())
            .iter()
            .any(|i| i.error.contains("DISTINCT")));
        std::fs::write(
            dir.path().join("entities/totals.sql"),
            "SELECT random() AS owner",
        )
        .unwrap();
        assert!(validate(dir.path())
            .iter()
            .any(|i| i.error.contains("volatile")));

        std::fs::write(
            dir.path().join("entities/totals.sql"),
            "SELECT current_date AS owner",
        )
        .unwrap();
        assert!(validate(dir.path())
            .iter()
            .any(|i| i.error.contains("volatile")));

        std::fs::write(
            dir.path().join("entities/totals.sql"),
            "SELECT l.owner FROM lefts AS l LEFT OUTER JOIN rights AS r ON l.owner = r.owner",
        )
        .unwrap();
        assert!(validate(dir.path())
            .iter()
            .any(|i| i.error.contains("outer joins")));

        std::fs::write(
            dir.path().join("entities/totals.sql"),
            "SELECT count(DISTINCT owner) AS owner FROM facts",
        )
        .unwrap();
        assert!(validate(dir.path())
            .iter()
            .any(|i| i.error.contains("DISTINCT")));

        std::fs::write(
            dir.path().join("entities/totals.sql"),
            "SELECT owner, count(*) AS n FROM facts WHERE note = 'ORDER BY' GROUP BY owner \
             -- LIMIT is prose here\n",
        )
        .unwrap();
        assert!(validate(dir.path()).is_empty());
    }

    /// #836 corrected the second half of this. A single-quoted **string** is text and is rightly
    /// discarded; a double-quoted **identifier** is a *name*, and discarding it is what let
    /// `"median"(v)` past the refusal list. The identifier now survives tokenisation.
    ///
    /// The cost is over-refusal: an entity that quotes a reserved word as a column alias - `SELECT x
    /// AS "limit"` - is now refused. That is the trade `analytics.rs` already makes explicitly for
    /// the same reason, and it is the safe direction.
    #[test]
    fn a_quoted_identifier_is_a_name_and_survives_tokenisation() {
        assert_eq!(
            sql_tokens("SELECT 'ORDER BY', \"LIMIT\" -- DISTINCT\n/* RANDOM() */"),
            vec!["SELECT", "LIMIT"],
            "the literal and the comments go; the identifier stays"
        );
        assert_eq!(
            sql_tokens("SELECT \"me\"\"dian\"(x)"),
            vec!["SELECT", "ME\"DIAN", "(", "X"],
            "an escaped inner quote is part of the name"
        );
    }

    /// #836 - the refusal list must be **closed**: every construct v1 cannot maintain is refused,
    /// and the check is that not one of them slips through.
    ///
    /// The list this replaces refused 1 of these 13. It named `MEDIAN`, `MODE` and `PERCENTILE_*`
    /// over a vocabulary the engine owns and grows - DuckDB knew 88 aggregate names - so the real
    /// spellings (`quantile_cont`) were admitted while the SQL-standard alias was blocked, and any
    /// of them could be hidden behind a double quote regardless.
    #[test]
    fn every_ineligible_construct_is_refused() {
        let ineligible: &[(&str, &str)] = &[
            ("median", "SELECT 1 AS k, median(v) AS m FROM (VALUES (1),(2)) t(v)"),
            ("quoted median", "SELECT 1 AS k, \"median\"(v) AS m FROM (VALUES (1),(2)) t(v)"),
            ("quantile_cont", "SELECT 1 AS k, quantile_cont(v, 0.5) AS m FROM (VALUES (1),(2)) t(v)"),
            // The SQL-standard alias for the same aggregate. Present because the *old* denylist
            // named `PERCENTILE_*` and missed `quantile_cont`; this list must not have the
            // inverse hole. It is refused only because the lowering renames the alias to
            // `quantile_cont`, as DuckDB's parser did - see
            // `the_allowlist_depends_on_the_parser_canonicalising_aliases`.
            ("percentile_cont alias", "SELECT 1 AS k, percentile_cont(0.5) WITHIN GROUP (ORDER BY v) AS m FROM (VALUES (1),(2)) t(v)"),
            ("quantile_disc", "SELECT 1 AS k, quantile_disc(v, 0.5) AS m FROM (VALUES (1),(2)) t(v)"),
            ("approx_quantile", "SELECT 1 AS k, approx_quantile(v, 0.5) AS m FROM (VALUES (1),(2)) t(v)"),
            ("arg_max", "SELECT 1 AS k, arg_max(v, v) AS m FROM (VALUES (1),(2)) t(v)"),
            ("first", "SELECT 1 AS k, first(v) AS m FROM (VALUES (1),(2)) t(v)"),
            ("string_agg", "SELECT 1 AS k, string_agg(v::VARCHAR, ',') AS m FROM (VALUES (1),(2)) t(v)"),
            ("list", "SELECT 1 AS k, list(v) AS m FROM (VALUES (1),(2)) t(v)"),
            ("histogram", "SELECT 1 AS k, histogram(v) AS m FROM (VALUES (1),(2)) t(v)"),
            ("any_value", "SELECT 1 AS k, any_value(v) AS m FROM (VALUES (1),(2)) t(v)"),
            ("in-subquery", "SELECT v AS k FROM (VALUES (1),(2)) t(v) WHERE v IN (SELECT 1)"),
            ("scalar subquery", "SELECT v AS k, (SELECT max(w) FROM (VALUES (9)) u(w)) AS m FROM (VALUES (1)) t(v)"),
            // A *single* grouping set is deliberately absent: `GROUP BY GROUPING SETS ((v))`
            // serialises byte-identically to `GROUP BY v` because it is the same query. The
            // NULL-padding forms are the ones v1 cannot maintain.
            ("two grouping sets", "SELECT a AS k, sum(b) AS m FROM (VALUES (1,2)) t(a,b) GROUP BY GROUPING SETS ((a),(b))"),
            ("rollup", "SELECT a AS k, sum(b) AS m FROM (VALUES (1,2)) t(a,b) GROUP BY ROLLUP (a,b)"),
            ("cube", "SELECT a AS k, sum(b) AS m FROM (VALUES (1,2)) t(a,b) GROUP BY CUBE (a,b)"),
            ("using sample", "SELECT v AS k FROM (VALUES (1),(2)) t(v) USING SAMPLE 1"),
            ("distinct aggregate", "SELECT 1 AS k, count(DISTINCT v) AS m FROM (VALUES (1),(2)) t(v)"),
        ];
        let admitted: Vec<&str> = ineligible
            .iter()
            .filter(|(_, sql)| validate_sql(sql).is_ok())
            .map(|(name, _)| *name)
            .collect();
        assert!(
            admitted.is_empty(),
            "admitted as incrementally maintainable: {admitted:?}"
        );
    }

    /// **The allowlist's closure rests on the parser, not on the catalogue** - pinned here because
    /// nothing else states it and a replacement engine must reproduce it (RFC-0042 slice 3, #966).
    ///
    /// `percentile_cont` had **zero rows** in DuckDB's `duckdb_functions()`, so a gate classifying
    /// names by the catalogue refused it only because DuckDB's parser rewrote the alias to
    /// `quantile_cont` first (shown by a DuckDB oracle test, removed with DuckDB). sqlparser
    /// preserves the source spelling, which is the common design, so the port reproduces the
    /// rewrite itself; without it a quantile would reach a DBSP circuit that cannot maintain it,
    /// with every other test here still green.
    #[test]
    fn the_allowlist_depends_on_the_parser_canonicalising_aliases() {
        let err = validate_sql(
            "SELECT k, percentile_cont(0.5) WITHIN GROUP (ORDER BY v) AS m \
             FROM (VALUES (1,2)) t(k,v) GROUP BY k",
        )
        .expect_err("a quantile must not be admitted as incrementally maintainable");
        assert!(
            format!("{err:#}").contains("quantile_cont"),
            "the refusal must come from the aggregate allowlist, not an unrelated gate: {err:#}"
        );
    }

    /// The other side of the same gate: the allowlist must not refuse what v1 *can* maintain, or
    /// authors route around it. A closed list that refuses everything is not a win.
    #[test]
    fn the_maintainable_aggregates_are_still_admitted() {
        for sql in [
            "SELECT k, sum(v) AS s FROM (VALUES (1,2)) t(k,v) GROUP BY k",
            "SELECT k, count(*) AS n FROM (VALUES (1,2)) t(k,v) GROUP BY k",
            "SELECT k, min(v) AS a, max(v) AS b, avg(v) AS c FROM (VALUES (1,2)) t(k,v) GROUP BY k",
            "SELECT lower(s) AS k FROM (VALUES ('A')) t(s)",
        ] {
            assert!(
                validate_sql(sql).is_ok(),
                "wrongly refused: {sql} -> {:?}",
                validate_sql(sql)
            );
        }
    }

    #[test]
    fn every_entity_sql_file_requires_a_declaration() {
        let dir = nest();
        std::fs::write(
            dir.path().join("entities/forgotten.sql"),
            "SELECT 1 AS owner",
        )
        .unwrap();
        let issues = validate(dir.path());
        assert_eq!(issues.len(), 1);
        assert!(issues[0].error.contains("no entities.toml declaration"));
    }

    #[test]
    fn load_is_empty_only_when_the_manifest_is_absent() {
        let dir = nest();
        assert!(load(dir.path()).unwrap().is_empty());
        std::fs::write(
            dir.path().join(ENTITY_FILE),
            "[[entities]]\nname='totals'\nsql='entities/totals.sql'\nkey=['owner']\nmax_rows=10\n",
        )
        .unwrap();
        let declarations = load(dir.path()).unwrap();
        assert_eq!(declarations.len(), 1);
        assert_eq!(declarations[0].name, "totals");
        assert_eq!(declarations[0].key, vec!["owner"]);
    }

    #[test]
    fn empty_entity_manifest_is_not_a_successful_no_op() {
        let dir = nest();
        std::fs::write(dir.path().join(ENTITY_FILE), "# nothing yet\n").unwrap();
        assert!(validate(dir.path())
            .iter()
            .any(|issue| issue.error.contains("declares no entities")));
    }

    #[test]
    fn entity_name_and_filename_are_one_derivation_identity() {
        let dir = nest();
        std::fs::write(
            dir.path().join(ENTITY_FILE),
            "[[entities]]\nname='a'\nsql='entities/b.sql'\nkey=['x']\nmax_rows=1\n",
        )
        .unwrap();
        std::fs::write(dir.path().join("entities/b.sql"), "SELECT 1 AS x").unwrap();
        assert!(validate(dir.path())
            .iter()
            .any(|i| i.error.contains("match the declared SQL filename")));
    }

    #[test]
    fn declared_key_must_be_unique_in_the_reference_result() {
        let dir = wide_nest();
        // Grouped by indexer but keyed by its count, and two indexers share a count.
        std::fs::write(
            dir.path().join(ENTITY_FILE),
            "[[entities]]\nname='rewards'\nsql='entities/rewards.sql'\nkey=['n']\nmax_rows=10\n",
        )
        .unwrap();
        let rows = [
            wide_row("0x00000000000000000000000000000000000000a1", "1", "0", 10),
            wide_row("0x00000000000000000000000000000000000000b2", "2", "0", 11),
        ];
        crate::seal::seal_range(dir.path(), &rows, 10, 11).unwrap();
        let issues = rewards_issues(
            dir.path(),
            "SELECT indexer, count(*) AS n FROM svc__collected GROUP BY indexer",
        );
        assert!(
            issues
                .iter()
                .any(|issue| issue.error.contains("not unique")),
            "expected duplicate key to be refused, got {issues:?}"
        );
    }

    #[test]
    fn reference_result_cannot_exceed_declared_max_rows() {
        let dir = wide_nest();
        std::fs::write(
            dir.path().join(ENTITY_FILE),
            "[[entities]]\nname='rewards'\nsql='entities/rewards.sql'\nkey=['indexer']\nmax_rows=1\n",
        )
        .unwrap();
        let rows = [
            wide_row("0x00000000000000000000000000000000000000a1", "1", "0", 10),
            wide_row("0x00000000000000000000000000000000000000b2", "2", "0", 11),
        ];
        crate::seal::seal_range(dir.path(), &rows, 10, 11).unwrap();
        let issues = rewards_issues(
            dir.path(),
            "SELECT indexer, count(*) AS n FROM svc__collected GROUP BY indexer",
        );
        assert!(
            issues
                .iter()
                .any(|issue| issue.error.contains("exceeds declared max_rows (1)")),
            "expected oversized reference result to be refused, got {issues:?}"
        );
    }

    #[test]
    fn dependencies_come_from_the_parsed_ast() {
        assert_eq!(
            dependencies("SELECT a.x FROM facts a JOIN earlier e ON a.x = e.x").unwrap(),
            vec!["earlier", "facts"]
        );
    }

    #[test]
    fn dependency_cycle_names_the_actual_entity_loop() {
        let nodes = vec![
            ("a".into(), vec!["facts".into(), "b".into()]),
            ("b".into(), vec!["c".into()]),
            ("c".into(), vec!["a".into()]),
        ];
        assert_eq!(
            dependency_cycle(&nodes),
            Some(vec!["a".into(), "b".into(), "c".into(), "a".into()])
        );
    }

    #[test]
    fn validation_reports_a_declared_entity_cycle() {
        let dir = nest();
        std::fs::write(dir.path().join(ENTITY_FILE), "[[entities]]\nname='a'\nsql='entities/a.sql'\nkey=['x']\nmax_rows=1\n[[entities]]\nname='b'\nsql='entities/b.sql'\nkey=['x']\nmax_rows=1\n").unwrap();
        std::fs::write(dir.path().join("entities/a.sql"), "SELECT x FROM b").unwrap();
        std::fs::write(dir.path().join("entities/b.sql"), "SELECT x FROM a").unwrap();
        let issues = validate(dir.path());
        assert!(
            issues.iter().any(|i| i.error.contains("a -> b -> a")),
            "{issues:?}"
        );
    }

    #[test]
    fn reuse_key_changes_for_every_entity_contract_input() {
        let base = identity(
            "SELECT owner FROM facts",
            vec!["upstream".into()],
            vec![source("0xaaa")],
            Vec::new(),
            vec!["owner".into()],
            vec!["owner".into()],
        )
        .unwrap();
        let key = base.reuse_key();
        assert_ne!(
            key,
            identity(
                "SELECT other FROM facts",
                vec!["upstream".into()],
                vec![source("0xaaa")],
                Vec::new(),
                vec!["owner".into()],
                vec!["owner".into()]
            )
            .unwrap()
            .reuse_key()
        );
        assert_ne!(
            key,
            identity(
                "SELECT owner FROM facts",
                vec!["other-upstream".into()],
                vec![source("0xaaa")],
                Vec::new(),
                vec!["owner".into()],
                vec!["owner".into()]
            )
            .unwrap()
            .reuse_key()
        );
        assert_ne!(
            key,
            identity(
                "SELECT owner FROM facts",
                vec!["upstream".into()],
                vec![source("0xbbb")],
                Vec::new(),
                vec!["owner".into()],
                vec!["owner".into()]
            )
            .unwrap()
            .reuse_key()
        );
        assert_ne!(
            key,
            identity(
                "SELECT owner FROM facts",
                vec!["upstream".into()],
                vec![source("0xaaa")],
                Vec::new(),
                vec!["id".into()],
                vec!["owner".into()]
            )
            .unwrap()
            .reuse_key()
        );
        assert_ne!(
            key,
            identity(
                "SELECT owner FROM facts",
                vec!["upstream".into()],
                vec![source("0xaaa")],
                Vec::new(),
                vec!["owner".into()],
                vec!["owner".into(), "amount".into()]
            )
            .unwrap()
            .reuse_key()
        );
    }

    /// #1437: `check` validates an entity over an offchain table against its snapshots, reference
    /// query included. `by_count` is only non-unique if that query really ran over the offchain rows.
    #[test]
    fn check_validates_an_offchain_entity_against_its_snapshots() {
        let dir = configured_nest();
        let csv = dir.path().join("prices.csv");
        std::fs::write(&csv, "symbol,price_e8\nETH,250000000000\nBTC,7\n").unwrap();
        crate::offchain::drop_file(dir.path(), &csv, "prices").unwrap();
        std::fs::write(
            dir.path().join(ENTITY_FILE),
            "[[entities]]\nname='quotes'\nsql='entities/quotes.sql'\nkey=['symbol']\nmax_rows=10\n\
             [[entities]]\nname='by_count'\nsql='entities/by_count.sql'\nkey=['n']\nmax_rows=10\n\
             [[entities]]\nname='offchain__x'\nsql='entities/offchain__x.sql'\nkey=['k']\nmax_rows=1\n",
        )
        .unwrap();
        let grouped = "SELECT symbol, count(*) AS n FROM offchain__prices GROUP BY symbol";
        std::fs::write(dir.path().join("entities/quotes.sql"), grouped).unwrap();
        std::fs::write(dir.path().join("entities/by_count.sql"), grouped).unwrap();
        std::fs::write(dir.path().join("entities/offchain__x.sql"), "SELECT 1 AS k").unwrap();

        let issues = validate(dir.path());
        assert!(!issues.iter().any(|i| i.name == "quotes"), "{issues:?}");
        assert!(
            issues
                .iter()
                .any(|i| i.name == "by_count" && i.error.contains("not unique")),
            "{issues:?}"
        );
        assert!(
            issues
                .iter()
                .any(|i| i.name == "offchain__x" && i.error.contains("namespace")),
            "{issues:?}"
        );
    }

    /// A nest with one event carrying two `uint256`s, `tokensRewards` and `unused`, and an entity
    /// declared over it whose SQL the caller writes.
    fn wide_nest() -> tempfile::TempDir {
        let dir = nest();
        std::fs::create_dir_all(dir.path().join("abis")).unwrap();
        std::fs::write(
            dir.path().join("abis/svc.json"),
            r#"[{"type":"event","name":"Collected","anonymous":false,"inputs":[
                {"name":"indexer","type":"address","indexed":true},
                {"name":"tokensRewards","type":"uint256","indexed":false},
                {"name":"unused","type":"uint256","indexed":false}]}]"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("nuthatch.toml"),
            "[nest]\nname=\"svc\"\nchain=\"mainnet\"\nchain_id=1\nrpc_urls=[]\n\
             [[contracts]]\nalias=\"svc\"\naddress=\"0x00000000000000000000000000000000000000aa\"\n\
             abi=\"abis/svc.json\"\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join(ENTITY_FILE),
            "[[entities]]\nname='rewards'\nsql='entities/rewards.sql'\nkey=['indexer']\nmax_rows=10\n",
        )
        .unwrap();
        dir
    }

    fn wide_row(indexer: &str, amount: &str, unused: &str, block: u64) -> String {
        format!(
            r#"{{"table":"svc__collected","indexer":"{indexer}","tokensRewards":"{amount}","unused":"{unused}","block_number":{block},"log_index":0,"block_timestamp":1,"tx_hash":"0xt","address":"0x00000000000000000000000000000000000000aa"}}"#
        )
    }

    fn rewards_issues(dir: &Path, sql: &str) -> Vec<EntityIssue> {
        std::fs::write(dir.join("entities/rewards.sql"), sql).unwrap();
        validate(dir)
    }

    /// #1587: the circuit reads a `uint256` as a checked `i128`, so `check` must too. It bound the
    /// column as its decimal string and refused `SUM(tokensRewards)`, which `dev` maintains exactly.
    #[test]
    fn check_binds_a_wide_integer_the_way_the_circuit_reads_it() {
        let dir = wide_nest();
        let (a, b) = (
            "0x00000000000000000000000000000000000000a1",
            "0x00000000000000000000000000000000000000b2",
        );
        let rows = [
            wide_row(a, "5", "0", 10),
            wide_row(a, "7", "0", 11),
            wide_row(b, "1", "0", 12),
        ];
        crate::seal::seal_range(dir.path(), &rows, 10, 12).unwrap();
        let plain =
            "SELECT indexer, SUM(tokensRewards) AS total FROM svc__collected GROUP BY indexer";
        assert!(rewards_issues(dir.path(), plain).is_empty());

        // The lowerer ignores a table qualifier, so `check` must not let one reach the untyped view.
        let filtered = "SELECT indexer, SUM(tokensRewards) AS total FROM main.svc__collected \
                        WHERE block_number < 13 GROUP BY indexer";
        let issues = rewards_issues(dir.path(), filtered);
        assert!(issues.is_empty(), "{issues:?}");

        // The circuit converts only the columns it reads, so a column it never reads may hold
        // anything, on either side of a self-join.
        let huge = "9".repeat(40);
        crate::seal::seal_range(dir.path(), &[wide_row(b, "1", &huge, 13)], 13, 13).unwrap();
        let self_join = "SELECT l.indexer, SUM(l.tokensRewards) AS total FROM svc__collected l \
                         JOIN svc__collected r ON l.indexer = r.indexer GROUP BY l.indexer";
        let issues = rewards_issues(dir.path(), self_join);
        assert!(issues.is_empty(), "{issues:?}");

        // Past i128 the circuit faults rather than truncating, and it converts the row before the
        // `WHERE` that would drop it, so `check` must refuse it though the query never keeps it.
        crate::seal::seal_range(dir.path(), &[wide_row(b, &huge, "0", 14)], 14, 14).unwrap();
        let issues = rewards_issues(dir.path(), filtered);
        assert!(
            issues
                .iter()
                .any(|i| i.name == "rewards" && i.error.contains("does not fit")),
            "a value the circuit cannot hold must not pass check: {issues:?}"
        );
    }

    /// #1599: an authored view that reads an entity checks exactly when `dev` would serve it, on a
    /// nest that has indexed nothing yet, and a column the entity does not have still fails.
    #[test]
    fn a_view_over_an_entity_checks() {
        let dir = wide_nest();
        std::fs::write(
            dir.path().join("entities/rewards.sql"),
            "SELECT indexer, SUM(tokensRewards) AS total FROM svc__collected GROUP BY indexer",
        )
        .unwrap();
        std::fs::create_dir_all(dir.path().join("views")).unwrap();
        let view = |sql: &str| {
            std::fs::write(dir.path().join("views/10-top.sql"), sql).unwrap();
            let cfg = crate::config::Config::load(dir.path()).unwrap();
            let schema = crate::registry::from_nest(dir.path(), &cfg)
                .unwrap()
                .schema();
            crate::analytics::validate_nest_views(dir.path(), &schema)
        };
        let issues = view("CREATE VIEW top AS SELECT indexer, total + 1 AS next FROM rewards;");
        assert!(issues.is_empty(), "{issues:?}");
        let issues = view("CREATE VIEW top AS SELECT indexer, missing FROM rewards;");
        assert!(
            !issues.is_empty(),
            "a column the entity lacks must still fail"
        );
    }

    /// The circuit seeds from the hot tail as well as sealed history, so a value only the hot store
    /// holds must be probed too.
    #[test]
    fn check_probes_the_hot_tail() {
        let dir = wide_nest();
        let a = "0x00000000000000000000000000000000000000a1";
        crate::seal::seal_range(dir.path(), &[wide_row(a, "1", "0", 10)], 10, 10).unwrap();
        let sql =
            "SELECT indexer, SUM(tokensRewards) AS total FROM svc__collected GROUP BY indexer";
        assert!(rewards_issues(dir.path(), sql).is_empty());
        {
            let store =
                crate::store::Store::open(&dir.path().join(crate::config::DB_FILE)).unwrap();
            store.set_meta("sealed_through", "10").unwrap();
            store
                .put_entity(
                    &crate::store::Store::entity_key(20, 0),
                    &wide_row(a, &"9".repeat(40), "0", 20),
                )
                .unwrap();
        }
        let issues = rewards_issues(dir.path(), sql);
        assert!(
            issues.iter().any(|i| i.error.contains("does not fit")),
            "an overflow in the hot tail must not pass check: {issues:?}"
        );
    }

    /// Only a store another process holds falls back to sealed history; one that cannot be read is an
    /// issue, not a silently unchecked hot tail.
    #[test]
    fn only_a_held_store_falls_back_to_sealed_history() {
        let dir = wide_nest();
        let a = "0x00000000000000000000000000000000000000a1";
        crate::seal::seal_range(dir.path(), &[wide_row(a, "1", "0", 10)], 10, 10).unwrap();
        let sql =
            "SELECT indexer, SUM(tokensRewards) AS total FROM svc__collected GROUP BY indexer";
        let db = dir.path().join(crate::config::DB_FILE);
        {
            let _held = crate::store::Store::open(&db).unwrap();
            let issues = rewards_issues(dir.path(), sql);
            assert!(
                issues.is_empty(),
                "a held store is dev's to read: {issues:?}"
            );
        }
        std::fs::write(&db, b"not a redb file").unwrap();
        let issues = rewards_issues(dir.path(), sql);
        assert!(
            issues.iter().any(|i| i.error.contains("hot store")),
            "an unreadable store must not pass as checked: {issues:?}"
        );
    }

    /// #1590: `check` refuses what `dev` refuses at start, because both lower and bind through the
    /// same code. Each of these passed `check` and stopped `dev`.
    #[test]
    fn check_refuses_what_dev_would_not_start() {
        let dir = wide_nest();
        for (sql, says) in [
            (
                "SELECT indexer, count(*) AS n FROM svc__collected GROUP BY indexer \
                 HAVING count(*) > 1",
                "HAVING",
            ),
            (
                "SELECT indexer, count(*) AS n FROM svc__elsewhere GROUP BY indexer",
                "no table svc__elsewhere",
            ),
            (
                "SELECT indexer, count(*) AS n FROM svc__collected GROUP BY indexer, unused",
                "must be the same set",
            ),
            (
                "SELECT indexer, sum(tokensRewards + '1') AS total FROM svc__collected \
                 GROUP BY indexer",
                "arithmetic needs Int, got Int and Str",
            ),
            (
                "SELECT indexer, sum(indexer) AS total FROM svc__collected GROUP BY indexer",
                "SUM and AVG need integers",
            ),
            (
                "SELECT indexer, count(*) AS n FROM svc__collected WHERE tokensRewards \
                 GROUP BY indexer",
                "must be a condition",
            ),
        ] {
            let issues = rewards_issues(dir.path(), sql);
            assert!(
                issues
                    .iter()
                    .any(|i| i.name == "rewards" && format!("{i:?}").contains(says)),
                "{sql}: {issues:?}"
            );
        }

        std::fs::write(
            dir.path().join(ENTITY_FILE),
            "[[entities]]\nname='SVC__Collected'\nsql='entities/SVC__Collected.sql'\nkey=['indexer']\n\
             max_rows=10\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("entities/SVC__Collected.sql"),
            "SELECT indexer, count(*) AS n FROM svc__collected GROUP BY indexer",
        )
        .unwrap();
        std::fs::remove_file(dir.path().join("entities/rewards.sql")).unwrap();
        let issues = validate(dir.path());
        assert!(
            issues
                .iter()
                .any(|i| i.error.contains("same name as the decoded table")),
            "{issues:?}"
        );
    }

    /// Typing applies only to SQL the lowerer accepts, so it cannot admit a shape `dev` refuses:
    /// `HAVING` over a wide column still fails to bind, as it did before #1587.
    #[test]
    fn typing_does_not_admit_what_the_lowerer_refuses() {
        let dir = wide_nest();
        let rows = [wide_row(
            "0x00000000000000000000000000000000000000a1",
            "5",
            "0",
            10,
        )];
        crate::seal::seal_range(dir.path(), &rows, 10, 10).unwrap();
        let having = "SELECT indexer, SUM(tokensRewards) AS total FROM svc__collected \
                      GROUP BY indexer HAVING SUM(tokensRewards) > 0";
        assert!(crate::entity_lower::lower(having).is_err());
        let issues = rewards_issues(dir.path(), having);
        assert!(issues.iter().any(|i| i.name == "rewards"), "{issues:?}");
    }

    /// The registry keeps a schema entry per decoder, so a table can appear twice; the engine refuses
    /// a repeated CTE name, and a table named in two cases is still one table to it.
    #[test]
    fn a_repeated_table_gets_one_typed_cte() {
        let dir = wide_nest();
        let cfg = crate::config::Config::load(dir.path()).unwrap();
        let mut schema = crate::registry::from_nest(dir.path(), &cfg)
            .unwrap()
            .schema();
        let mut shouting = schema[0].clone();
        shouting.table = shouting.table.to_ascii_uppercase();
        schema.push(schema[0].clone());
        schema.push(shouting);
        let sql = "SELECT indexer, SUM(tokensRewards) AS s FROM svc__collected GROUP BY indexer";
        let reads = wide_reads(&schema, &crate::entity_lower::lower(sql).unwrap());
        let typed = typed_for_check(sql, &reads).unwrap();
        assert_eq!(typed.matches(" AS (SELECT").count(), 1, "{typed}");
        assert!(
            !typed.contains("unused"),
            "only the columns the plan reads: {typed}"
        );
    }

    /// #1437: the offchain table and the columns it is read through are the definition; the
    /// snapshots applied are the version, and appending one must not make a different program.
    #[test]
    fn an_offchain_source_keys_by_definition_not_by_applied_snapshots() {
        use crate::entity_offchain::{Column, Kind};
        let key = |table: &str, kind: Kind, snapshots: &[&str]| {
            identity(
                "SELECT symbol FROM offchain__prices",
                Vec::new(),
                Vec::new(),
                vec![OffchainSourceIdentity {
                    table: table.into(),
                    columns: vec![Column {
                        name: "price_e8".into(),
                        kind,
                    }],
                    snapshots: snapshots.iter().map(|s| s.to_string()).collect(),
                }],
                vec!["symbol".into()],
                vec!["symbol".into()],
            )
            .unwrap()
            .reuse_key()
        };
        let base = key("prices", Kind::Int, &["a"]);
        assert_eq!(
            base,
            key("prices", Kind::Int, &["a", "b"]),
            "an append is a delta"
        );
        assert_ne!(base, key("quotes", Kind::Int, &["a"]));
        assert_ne!(base, key("prices", Kind::Str, &["a"]));
    }
}
