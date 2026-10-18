//! The governed semantic layer's model (RFC-0016 §2): what `semantic.toml` holds, how `init` seeds it
//! from the registry and how `add` merges onto an authored one. Reading and writing the file, and
//! composing it for the MCP, stay in the node.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use nuthatch_decode::registry::{TableKind, TableSchema, L1_BLOCKS_TABLE};

/// The authored semantic layer for a nest. Deserialized from `semantic.toml`; also produced by
/// [`generate`] from the registry for `init` to write.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Semantic {
    #[serde(default = "one")]
    pub schema_version: u32,
    #[serde(default)]
    pub nest: NestSemantic,
    /// Per-table meaning, keyed by table name (`{alias}__{event}`). BTreeMap for stable ordering, so
    /// the generated file and the composed doc are deterministic (Tier-A goldenable).
    #[serde(default, rename = "table")]
    pub tables: BTreeMap<String, TableSemantic>,
    /// Per-authored-view meaning, keyed by view name (RFC-0018 §1) - the derivations the nest exists to
    /// answer. Rendered into `/schema`/the MCP exactly like tables, so an agent *sees* `top_recipients`
    /// and what it means rather than rediscovering it.
    #[serde(default, rename = "view")]
    pub views: BTreeMap<String, ViewSemantic>,
}

fn one() -> u32 {
    1
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NestSemantic {
    #[serde(default)]
    pub description: String,
}

/// What one authored SQL view (`views/*.sql`) computes (RFC-0018 §1). The view's *shape* (columns) is
/// introspected from Burrmill at query time - the author only has to say what it *means*.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ViewSemantic {
    #[serde(default)]
    pub description: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TableSemantic {
    #[serde(default)]
    pub description: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub grain: String,
    /// Per-column description, keyed by column name.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub columns: BTreeMap<String, String>,
    /// Derived-not-authored: the SQL footguns of this table. Regenerated from the registry, so they
    /// stay correct even when the author edits everything else.
    #[serde(default, skip_serializing_if = "Footguns::is_empty")]
    pub footguns: Footguns,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Footguns {
    /// Columns whose names are SQL reserved words - must be double-quoted (`"from"`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reserved_words: Vec<String>,
    /// Columns holding integers wider than 64 bits, stored as exact text. Arithmetic must use the
    /// derived `{col}_dec` companion, never the raw text column.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub big_ints: Vec<String>,
    /// The subset of `big_ints` whose storage exceeds `DECIMAL(38,0)`'s range (int/uint wider than 128
    /// bits - e.g. a Uniswap-v3 `sqrtPriceX96` uint160): their `{col}_dec` companion is **NULL**
    /// whenever the value has more than 38 digits. `SUM({col}_dec)` is the values that fit, and
    /// `WHERE NOT {col}_overflow` is that same sum. A price that never fits wants
    /// `CAST({col} AS DOUBLE)`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub overflows_dec: Vec<String>,
    /// Columns holding a Solidity `bool`, stored as exact text `'true'`/`'false'`, not a SQL boolean
    /// (#539). A direct comparison (`{col} = true`) or boolean op (`AND`/`NOT`) implicitly casts and
    /// works; a function requiring a uniform type across its arguments (`COALESCE`, `CASE`,
    /// `bool_and`/`bool_or`, `UNION`) does not, and fails to build with "an explicit cast is
    /// required". Write `{col} = 'true'` or `CAST({col} AS BOOLEAN)`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bools: Vec<String>,
}

impl Footguns {
    pub fn is_empty(&self) -> bool {
        self.reserved_words.is_empty()
            && self.big_ints.is_empty()
            && self.overflows_dec.is_empty()
            && self.bools.is_empty()
    }
}

/// SQL reserved words that also turn up as EVM event parameter names - a column with one of these
/// names must be double-quoted in every dialect. Kept deliberately small and high-signal (the ones
/// that actually collide with real ABIs) rather than the full 200-word SQL keyword list.
const SQL_RESERVED: &[&str] = &[
    "from",
    "to",
    "in",
    "order",
    "group",
    "select",
    "where",
    "case",
    "when",
    "then",
    "else",
    "end",
    "default",
    "table",
    "index",
    "column",
    "references",
    "primary",
    "key",
    "all",
    "and",
    "or",
    "not",
    "null",
    "like",
    "limit",
    "offset",
    "values",
    "user",
    "grant",
    "check",
    "unique",
    "desc",
    "asc",
];

/// A big-integer storage kind (uint/int > 64-bit) - the columns that get a `{col}_dec` companion and
/// must not be summed/compared as raw text. Mirrors `analytics::is_bigint`.
fn is_bigint_storage(storage: &str) -> bool {
    storage == "word16" || storage == "word32"
}

/// A bool storage kind - a Solidity `bool` stored as exact text `'true'`/`'false'`, not a SQL
/// boolean. Mirrors `seal::rows_to_batch`, which types it the same as every other
/// non-numeric Solidity value: text.
fn is_bool_storage(storage: &str) -> bool {
    storage == "bool"
}

/// Derive the footguns for one table purely from its registry schema. Always correct by construction.
pub fn derive_footguns(table: &TableSchema) -> Footguns {
    let mut reserved_words = Vec::new();
    let mut big_ints = Vec::new();
    let mut overflows_dec = Vec::new();
    let mut bools = Vec::new();
    for col in &table.columns {
        if SQL_RESERVED.contains(&col.name.to_ascii_lowercase().as_str()) {
            reserved_words.push(col.name.clone());
        }
        if is_bigint_storage(&col.storage) {
            big_ints.push(col.name.clone());
            // `word32` = int/uint 129-256 bit: its max (up to ~1e77) exceeds `DECIMAL(38,0)`, so the
            // derived `_dec` is NULL for values with >38 digits (sqrtPriceX96, price accumulators).
            // `word16` (≤128 bit) fits comfortably for realistic values, so it keeps the plain `_dec`
            // guidance and is *not* flagged here.
            if col.storage == "word32" {
                overflows_dec.push(col.name.clone());
            }
        }
        if is_bool_storage(&col.storage) {
            bools.push(col.name.clone());
        }
    }
    Footguns {
        reserved_words,
        big_ints,
        overflows_dec,
        bools,
    }
}

/// The honest fallback marker appended to every generated (un-edited) description, so an author can
/// tell at a glance what still needs their attention and a reader knows the text is machine-seeded.
const SEEDED: &str = "(seeded from the ABI - edit semantic.toml to improve this)";

/// Generate a `Semantic` from the registry: ABI-seeded descriptions plus derived footguns. This is
/// what `init` writes. Descriptions are honest placeholders; footguns are authoritative.
pub fn generate(schema: &[TableSchema], nest_name: &str, chain: &str) -> Semantic {
    let mut tables = BTreeMap::new();
    for t in schema {
        let mut columns = BTreeMap::new();
        for col in &t.columns {
            if col.sol_type == "implicit" {
                continue; // implicit columns are documented once, in the composed doc, not per-nest.
            }
            let desc = if col.storage == "bool" {
                // #539: the old wording ("the `enabled` bool parameter") read as a promise that the
                // column *is* a SQL boolean. It stores exact text `'true'`/`'false'` instead, so say so
                // here - the one seeded description every column gets whether or not the author ever
                // opens semantic.toml.
                format!(
                    "The `{0}` bool parameter - stored as exact text `'true'`/`'false'`, not a SQL \
                     boolean. `{0} = true` and `AND`/`NOT` implicitly cast and work; `COALESCE`, \
                     `CASE`, `bool_and`/`bool_or` and `UNION` do not. Write `{0} = 'true'` or \
                     `CAST({0} AS BOOLEAN)`. {SEEDED}",
                    col.name
                )
            } else {
                format!("The `{}` {} parameter. {SEEDED}", col.name, col.sol_type)
            };
            columns.insert(col.name.clone(), desc);
        }
        let footguns = derive_footguns(t);
        // `[[ipfs]]` tables are Call-kind with no selector; seeding them as `[[calls]]` results told an
        // agent a typed-rows table was "one row per sampled block".
        let has = |name: &str| t.columns.iter().any(|c| c.name == name);
        let ipfs_document =
            t.selector.is_empty() && has("cid") && has("content") && has("verified");
        let ipfs_rows = t.selector.is_empty() && has("document_log_index") && has("element");
        let (description, grain) = match t.kind {
            TableKind::Call if ipfs_rows => (
                format!(
                    "Typed rows from the proven IPFS documents an `[[ipfs]]` declaration resolves, one per \
                     element, written with their document. {SEEDED}"
                ),
                "one row per element of a proven document".to_string(),
            ),
            TableKind::Call if ipfs_document => (
                format!(
                    "IPFS documents an `[[ipfs]]` declaration resolved, each proven against its CID. {SEEDED}"
                ),
                "one row per document a block's rows name".to_string(),
            ),
            TableKind::Call => (
                format!(
                    "Result of the `{}` call (selector `{}`). {SEEDED}",
                    t.table, t.selector
                ),
                "one row per sampled block this declaration fires at".to_string(),
            ),
            TableKind::Block if t.table == L1_BLOCKS_TABLE => (
                format!(
                    "The L1 block each block carrying this nest's logs was sequenced against. {SEEDED}"
                ),
                "one row per block that carries a row in another table".to_string(),
            ),
            TableKind::Block => (
                format!("One row per block on this nest. {SEEDED}"),
                "one row per block".to_string(),
            ),
            TableKind::State => (
                format!("Storage writes for `{}`. {SEEDED}", t.alias),
                "one row per storage write".to_string(),
            ),
            TableKind::Event => (
                format!(
                    "`{}` events emitted by the `{}` contract. {SEEDED}",
                    t.event, t.alias
                ),
                format!("one row per {} event", t.event),
            ),
        };
        tables.insert(
            t.table.clone(),
            TableSemantic {
                description,
                grain,
                columns,
                footguns,
            },
        );
    }
    Semantic {
        schema_version: 1,
        nest: NestSemantic {
            description: format!("The `{nest_name}` nest on {chain}. {SEEDED}"),
        },
        tables,
        // Authored views are seeded per-scaffolded-view by `init` (RFC-0018 §1b), not generated from
        // the registry - the registry has no views.
        views: BTreeMap::new(),
    }
}

/// Merge freshly-`generate`d semantics onto an existing (possibly author-edited) file: keep the
/// author's descriptions/grain/columns wherever they exist, but always take the **freshly-derived
/// footguns** (they must never go stale) and add entries for any new tables. Used by `add`, so
/// growing a nest never clobbers authored meaning yet always keeps the footguns correct.
pub fn merge(existing: Semantic, generated: Semantic) -> Semantic {
    let mut out = existing;
    for (table, gen_ts) in generated.tables {
        match out.tables.get_mut(&table) {
            Some(cur) => {
                // Authored text wins; derived footguns are always refreshed.
                cur.footguns = gen_ts.footguns;
                for (col, desc) in gen_ts.columns {
                    cur.columns.entry(col).or_insert(desc);
                }
                if cur.grain.is_empty() {
                    cur.grain = gen_ts.grain;
                }
                if cur.description.is_empty() {
                    cur.description = gen_ts.description;
                }
            }
            None => {
                out.tables.insert(table, gen_ts);
            }
        }
    }
    if out.nest.description.is_empty() {
        out.nest.description = generated.nest.description;
    }
    out
}

/// Declared alias rename (#671). Moves `[table.<old>__*]` keys to `[table.<new>__*]`, keeping
/// authored prose. `merge` must not do this itself: it cannot tell a rename from a removal.
pub fn rekey_alias(sem: &mut Semantic, old: &str, new: &str) {
    let prefix = format!("{old}__");
    let moving: Vec<String> = sem
        .tables
        .keys()
        .filter(|k| k.starts_with(&prefix) || *k == old)
        .cloned()
        .collect();
    for k in moving {
        if let Some(ts) = sem.tables.remove(&k) {
            let nk = if k == old {
                new.to_string()
            } else {
                format!("{new}__{}", k.strip_prefix(&prefix).unwrap_or(&k))
            };
            sem.tables.insert(nk, ts);
        }
    }
}

/// `semantic.toml`'s text for `sem`, as `init` and `add` write it.
pub fn render(sem: &Semantic) -> Result<String> {
    let header = "# semantic.toml - what this nest's data *means* (RFC-0016). Read by the MCP `schema`\n\
                  # tool, the admin UI, and the scaffolded skill. Edit descriptions freely; the\n\
                  # `[table.*.footguns]` are DERIVED from the ABI and regenerated - leave them be.\n\n";
    let body = toml::to_string_pretty(sem).context("serialise semantic.toml")?;
    // When no views are described yet, seed a commented `[view.*]` stub (RFC-0018 §1b) so an author who
    // uncomments a `views/*.sql` knows where to say what it means. A comment can't be represented in the
    // serde model, so it's appended as trailing text - inert until uncommented.
    let view_stub = if sem.views.is_empty() {
        "\n# Authored views (views/*.sql) are described here so the MCP/`/schema` can render them:\n\
         # [view.your_view_name]\n\
         # description = \"What this derivation computes.\"\n"
    } else {
        ""
    };
    Ok(format!("{header}{body}{view_stub}"))
}
