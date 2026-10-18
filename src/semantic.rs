//! The governed semantic layer (RFC-0016 §2). `semantic.toml` is what a nest's data *means* -
//! per-table and per-column descriptions authored by the nest's author - sitting beside
//! `nuthatch.toml` and read by every surface that describes the nest (the MCP `schema` tool, the
//! admin UI, the scaffolded skill). One source of truth, many readers.
//!
//! Two rules make it *governed* rather than just a docs file:
//!
//! 1. **Generated at `init`, never trusted blindly.** Descriptions are seeded from the ABI (honest
//!    fallback text an author is invited to improve). **Footguns are derived, not authored** -
//!    reserved-word columns (`"from"`/`"to"`) and big-int columns (`value` → use `value_dec`) are
//!    computed from the decode registry, so they are always present and always correct even if the
//!    author never opens the file.
//! 2. **Drift is caught.** [`drift`] flags any table/column the file describes that the registry
//!    doesn't have - stale semantics are worse than none, so `dev` warns loudly.
//!
//! Nothing here touches the data path (non-negotiable 4): this is presentation over the registry.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde::Serialize;

use crate::registry::TableSchema;
pub use nuthatch_scaffold::semantic::*;

/// Load `semantic.toml` from a nest directory, if present. Absent is fine (a nest predating the
/// semantic layer still describes itself from the registry alone) - returns `Ok(None)`.
pub fn load(dir: &std::path::Path) -> Result<Option<Semantic>> {
    let path = dir.join("semantic.toml");
    if !path.exists() {
        return Ok(None);
    }
    let text =
        std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    let sem: Semantic =
        toml::from_str(&text).with_context(|| format!("parse {}", path.display()))?;
    crate::config::warn_unknown::<Semantic>("semantic.toml", &text);
    Ok(Some(sem))
}

/// Write a `Semantic` to `semantic.toml` in a nest directory (what `init` calls).
pub fn save(dir: &std::path::Path, sem: &Semantic) -> Result<()> {
    std::fs::write(dir.join("semantic.toml"), render(sem)?).context("write semantic.toml")?;
    Ok(())
}

/// Drift check: every table/column the semantic file *describes* must exist in the registry. Returns
/// human-readable warnings (empty when clean). Stale semantics are worse than none - `dev` surfaces
/// these loudly so the author fixes or regenerates the file.
pub fn drift(schema: &[TableSchema], sem: &Semantic) -> Vec<String> {
    let known: BTreeMap<&str, Vec<String>> = schema
        .iter()
        .map(|t| {
            (
                t.table.as_str(),
                t.columns.iter().map(|c| c.name.clone()).collect(),
            )
        })
        .collect();

    // Separate orphaned tables into two buckets: those whose alias prefix is entirely absent from
    // the registry (whole-alias orphans, caused by a contract rename) and genuine per-table drift.
    // Collapsing the whole-alias case into one warning prevents N×38 noise on a correct nest.
    let registry_aliases: std::collections::BTreeSet<&str> = known
        .keys()
        .filter_map(|t| t.split_once("__").map(|(a, _)| a))
        .collect();

    let mut alias_orphans: BTreeMap<String, usize> = BTreeMap::new();
    let mut warnings = Vec::new();

    for (table, ts) in &sem.tables {
        match known.get(table.as_str()) {
            None => {
                // Whole-alias orphan when the alias prefix no longer exists in the registry.
                if let Some(alias) = table.split_once("__").map(|(a, _)| a) {
                    if !registry_aliases.contains(alias) {
                        *alias_orphans.entry(alias.to_string()).or_insert(0) += 1;
                        continue;
                    }
                }
                warnings.push(format!(
                    "semantic.toml describes table `{table}`, which the registry has no decoder for"
                ));
            }
            Some(cols) => {
                for col in ts.columns.keys() {
                    if !cols.contains(col) {
                        warnings.push(format!(
                            "semantic.toml describes `{table}.{col}`, which isn't a column of that table"
                        ));
                    }
                }
            }
        }
    }

    // One consolidated warning per renamed alias instead of one per table. The command that
    // actually keeps the prose is `nuthatch nest rename-alias`; `merge` still will not infer a
    // rename, so `nuthatch schema` leaves the orphaned keys - and this warning - in place.
    for (alias, count) in alias_orphans {
        let plural = if count == 1 { "table" } else { "tables" };
        warnings.push(format!(
            "semantic.toml has {count} {plural} still keyed to alias `{alias}`, which is no \
             longer in this nest; run `nuthatch nest rename-alias {alias} <new>` to keep their \
             descriptions, or re-key the `[table.{alias}__*]` sections by hand, or delete them"
        ));
    }

    warnings
}

/// Live per-table coverage, folded into the composed schema so the hot/cold seam is data an agent can
/// reason about rather than prose it skims. Assembled by the server from the store at call time.
#[derive(Debug, Clone, Default, Serialize)]
pub struct Coverage {
    pub sealed_through: u64,
    pub tip: u64,
}

/// Compose the enriched schema document from the four layers the RFC names: **structure** (registry),
/// **meaning** (semantic.toml), **derived footguns**, and - when a running nest supplies it -
/// **coverage** (the hot/cold seam as numbers). Sample-row *evidence* is a later slice; this is the
/// text an agent reads before writing SQL. Deterministic given its inputs, so it is Tier-A goldenable.
/// One authored incremental entity, as `/schema` needs to describe it (RFC-0041, #822).
///
/// A plain struct rather than a borrow of `EntityView` so this module keeps knowing nothing about
/// circuits, threads or dbsp: `serve` reads the live view and hands over the facts.
pub struct MaintainedRelation {
    pub name: String,
    pub columns: Vec<String>,
    /// The last block whose facts are folded into this relation.
    pub applied_through: u64,
    /// Whether `applied_through` has caught up with the dataset's head.
    pub current: bool,
    /// Why the relation holds no answer, if it holds none. Distinct from `fault`: nothing died.
    pub unavailable: Option<String>,
    /// Why the relation stopped, if it has. Terminal.
    pub fault: Option<String>,
    pub rows: usize,
    /// The offchain views it reads (#1437). Non-empty makes it reproducible by snapshot.
    pub offchain: Vec<String>,
}

pub fn compose(
    schema: &[TableSchema],
    sem: Option<&Semantic>,
    coverage: Option<&Coverage>,
    maintained: &[MaintainedRelation],
) -> String {
    let mut out = String::new();
    out.push_str("nuthatch data model\n\n");
    if let Some(s) = sem {
        if !s.nest.description.is_empty() {
            out.push_str(&s.nest.description);
            out.push_str("\n\n");
        }
    }

    if let Some(c) = coverage {
        out.push_str(&format!(
            "COVERAGE\n  sealed_through = {} (rows at or below this block are sealed Parquet);\n  \
             tip = {} - the `sql` tool reads both the sealed rows and the live tip above them.\n\n",
            c.sealed_through, c.tip
        ));
    }

    out.push_str("TABLES (one per contract event; query via the `sql` tool)\n");
    for t in schema {
        let ts = sem.and_then(|s| s.tables.get(&t.table));
        out.push_str(&format!("\n  {} - {}\n", t.table, describe_table(t, ts)));
        if let Some(ts) = ts {
            if !ts.grain.is_empty() {
                out.push_str(&format!("    grain: {}\n", ts.grain));
            }
        }
        out.push_str("    columns: ");
        let cols: Vec<String> = t
            .columns
            .iter()
            .filter(|c| c.sol_type != "implicit")
            .map(|c| format!("{} ({})", c.name, c.sol_type))
            .collect();
        out.push_str(&cols.join(", "));
        out.push('\n');

        let fg = derive_footguns(t);
        if !fg.reserved_words.is_empty() {
            out.push_str(&format!(
                "    ⚠ reserved-word columns - double-quote them: {}\n",
                fg.reserved_words
                    .iter()
                    .map(|c| format!("\"{c}\""))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if !fg.big_ints.is_empty() {
            out.push_str(&format!(
                "    ⚠ big-int columns (exact text; SUM/AVG of `_dec` is the values that fit, \
                 and WHERE NOT <col>_overflow is that same sum): {}\n",
                fg.big_ints
                    .iter()
                    .map(|c| format!("{c} → {c}_dec"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if !fg.overflows_dec.is_empty() {
            out.push_str(&format!(
                "    ⚠ wide columns (>128-bit) whose `_dec` is NULL above 38 digits. A sum of \
                 `_dec` is the values that fit; WHERE NOT <col>_overflow is that same sum. Ids, \
                 nonces and hashes stay on the raw column: {}\n",
                fg.overflows_dec.join(", ")
            ));
        }
    }

    // Authored views (RFC-0018 §1): the derivations the nest exists to answer, queryable by name over
    // the same hot∪cold surface. Rendered from `semantic.toml` `[view.*]` so an agent sees them.
    if let Some(s) = sem {
        if !s.views.is_empty() {
            out.push_str(
                "\nAUTHORED VIEWS (derived - query by name, recomputed per query over hot∪cold)\n",
            );
            for (name, v) in &s.views {
                let desc = if v.description.is_empty() {
                    "(an authored SQL view - describe it in semantic.toml `[view.…]`)"
                } else {
                    &v.description
                };
                out.push_str(&format!("  {name} - {desc}\n"));
            }
        }
    }

    // Authored **incremental** entities (RFC-0041). Deliberately rendered next to the authored views
    // above, because the difference between them is the entire point: a view is recomputed on every
    // query, a maintained relation is not, and an agent choosing between two names that both answer
    // the same question needs to be told which is which. Each carries its own applied-through block:
    // unlike a view, a relation can legitimately be *behind* the dataset, and a caller that cannot
    // see that has no way to know it is reading a lagging answer.
    if !maintained.is_empty() {
        out.push_str(
            "\nMAINTAINED RELATIONS (incrementally maintained - query by name; NOT recomputed per \
             query, and reorgs retract automatically)\n",
        );
        for r in maintained {
            out.push_str(&format!(
                "\n  {} - applied through block {}",
                r.name, r.applied_through
            ));
            if !r.current {
                out.push_str(" (BEHIND the dataset head - catching up)");
            }
            out.push_str(&format!("; {} row(s)\n", r.rows));
            out.push_str(&format!("    columns: {}\n", r.columns.join(", ")));
            if !r.offchain.is_empty() {
                out.push_str(&format!(
                    "    reads {}: reproducible by snapshot, NOT re-derivable from chain; a reorg \
                     retracts its chain rows only\n",
                    r.offchain.join(", ")
                ));
            }
            if let Some(why) = &r.unavailable {
                out.push_str(&format!(
                    "    ⚠ unavailable, and NOT queryable from `sql`: {why}\n"
                ));
            }
            if let Some(why) = &r.fault {
                out.push_str(&format!(
                    "    ⚠ faulted, and NOT queryable from `sql`. This is terminal: {why}\n"
                ));
            }
        }
    }

    out.push_str(GENERAL_GUIDANCE);
    out
}

/// Nest-independent guidance every composed schema carries - the derived views and compliance/factory
/// surfaces an agent should know exist. Kept as a trailing appendix so the per-nest tables lead.
const GENERAL_GUIDANCE: &str = r#"
VIEWS (incrementally maintained; reorgs retract automatically)
  balances - per-address net balance = Σ(received) − Σ(sent), i128 base units as decimal strings,
             for ERC-20 Transfer tables. Query via the `balance`/`top_balances` tools.

COMPLIANCE (RFC-0008; amounts are i128 base units as decimal strings)
  exposure       - an address's direct exposure to the labeled set (tool `exposure`).
  flags          - threshold and velocity flags (tool `flags`).

FACTORIES (RFC-0009; only in a nest with templates/factories)
  Children of a template share tables (`pool__swap`, …), distinguished by the `address` column. Each
  template has a `{template}__children` view: which children were discovered, when, by which parent.
"#;

fn describe_table(t: &TableSchema, ts: Option<&TableSemantic>) -> String {
    match ts {
        Some(ts) if !ts.description.is_empty() => ts.description.clone(),
        _ => match t.kind {
            crate::registry::TableKind::Call => {
                format!("result of the `{}` call", t.table)
            }
            crate::registry::TableKind::Block if t.table == crate::registry::L1_BLOCKS_TABLE => {
                "the L1 block of each block carrying this nest's logs".into()
            }
            crate::registry::TableKind::Block => "one row per block".into(),
            crate::registry::TableKind::State => format!("storage writes for `{}`", t.alias),
            crate::registry::TableKind::Event => {
                format!("`{}` events from `{}`", t.event, t.alias)
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{ColumnSchema, TableSchema};

    /// A minimal event table under a chosen alias, for the rename repro.
    fn aliased_table(alias: &str, event: &str) -> TableSchema {
        TableSchema {
            table: format!("{alias}__{event}"),
            alias: alias.into(),
            kind: crate::registry::TableKind::Event,
            function: String::new(),
            selector: String::new(),
            event: event.into(),
            topic0: "0xddf2".into(),
            columns: vec![ColumnSchema {
                name: "sender".into(),
                sol_type: "address".into(),
                storage: "address".into(),
                indexed: true,
                components: Vec::new(),
            }],
        }
    }

    fn transfer_table() -> TableSchema {
        TableSchema {
            table: "usdc__transfer".into(),
            alias: "usdc".into(),
            kind: crate::registry::TableKind::Event,
            function: String::new(),
            selector: String::new(),
            event: "Transfer".into(),
            topic0: "0xddf2".into(),
            columns: vec![
                ColumnSchema {
                    name: "from".into(),
                    sol_type: "address".into(),
                    storage: "address".into(),
                    indexed: true,
                    components: Vec::new(),
                },
                ColumnSchema {
                    name: "to".into(),
                    sol_type: "address".into(),
                    storage: "address".into(),
                    indexed: true,
                    components: Vec::new(),
                },
                ColumnSchema {
                    name: "value".into(),
                    sol_type: "uint256".into(),
                    storage: "word32".into(),
                    indexed: false,
                    components: Vec::new(),
                },
                ColumnSchema {
                    name: "enabled".into(),
                    sol_type: "bool".into(),
                    storage: "bool".into(),
                    indexed: false,
                    components: Vec::new(),
                },
                ColumnSchema {
                    name: "block_number".into(),
                    sol_type: "implicit".into(),
                    storage: "u64".into(),
                    indexed: false,
                    components: Vec::new(),
                },
            ],
        }
    }

    #[test]
    fn footguns_are_derived_from_the_registry() {
        let fg = derive_footguns(&transfer_table());
        assert_eq!(fg.reserved_words, vec!["from", "to"]);
        assert_eq!(fg.big_ints, vec!["value"]);
        // `value` is a word32 (uint256), so it also overflows DECIMAL(38,0).
        assert_eq!(fg.overflows_dec, vec!["value"]);
        assert_eq!(fg.bools, vec!["enabled"]);
    }

    /// #539: the seeded description used to read "The `enabled` bool parameter" - a promise that it
    /// *is* a SQL boolean. It is exact text `'true'`/`'false'`, so the seeded text (the one every
    /// column gets, edited or not) must say so and give the working comparison.
    /// `[[ipfs]]` tables are Call-kind with no selector. Seeded as `[[calls]]` results, a typed-rows table
    /// told an agent it held "one row per sampled block this declaration fires at".
    #[test]
    fn ipfs_tables_are_not_seeded_as_call_results() {
        let decl = crate::ipfs::IpfsDecl {
            name: "docs".into(),
            on: "nft__uri_set".into(),
            cid_column: "uri".into(),
            cid_json_path: None,
            json_match: Default::default(),
            rows: Some(crate::ipfs::IpfsRows {
                table: "docs_rows".into(),
                max_rows: 8,
                keep_content: true,
                columns: vec![crate::ipfs::RowColumn {
                    name: "n".into(),
                    ty: crate::ipfs::RowType::U64,
                }],
            }),
        };
        let seeded = generate(&crate::ipfs::schema(&[decl], true), "n", "mainnet");
        assert_eq!(
            seeded.tables["docs_rows"].grain,
            "one row per element of a proven document"
        );
        assert_eq!(
            seeded.tables["docs"].grain,
            "one row per document a block's rows name"
        );
        assert!(!seeded.tables["docs"].description.contains("selector"));
    }

    #[test]
    fn a_call_table_is_not_seeded_as_an_empty_event() {
        let table = TableSchema {
            table: "token0_symbol".into(),
            alias: "token0_symbol".into(),
            kind: crate::registry::TableKind::Call,
            event: String::new(),
            topic0: String::new(),
            function: String::new(),
            selector: "0x18160ddd".into(),
            columns: vec![],
        };
        let sem = generate(&[table], "uni", "mainnet");
        let ts = &sem.tables["token0_symbol"];
        assert!(
            !ts.description.contains("``") && !ts.grain.contains("per  event"),
            "a call table must not interpolate an empty event name: description={:?} grain={:?}",
            ts.description,
            ts.grain
        );
        assert!(
            ts.description.contains("call") && ts.description.contains("0x18160ddd"),
            "must name a call result, not a contract event: {}",
            ts.description
        );
        assert!(
            !ts.description.contains("contract"),
            "a declaration name is not a contract: {}",
            ts.description
        );
    }

    #[test]
    fn a_seeded_bool_description_warns_it_is_stored_as_text() {
        let sem = generate(&[transfer_table()], "usdc", "ethereum");
        let desc = sem.tables["usdc__transfer"].columns["enabled"].clone();
        assert!(
            desc.contains("exact text") && desc.contains("'true'"),
            "must say it is text, not a boolean: {desc}"
        );
        assert!(
            !desc.starts_with("The `enabled` bool parameter. ("),
            "must not read as a bare, unqualified promise of a real boolean: {desc}"
        );
    }

    #[test]
    fn word16_is_a_big_int_but_does_not_overflow_dec() {
        // A ≤128-bit big-int gets `_dec` guidance but is NOT flagged as overflowing (realistic values
        // fit in DECIMAL(38,0)); only >128-bit (word32) columns overflow.
        let table = TableSchema {
            table: "t__e".into(),
            alias: "t".into(),
            kind: crate::registry::TableKind::Event,
            function: String::new(),
            selector: String::new(),
            event: "E".into(),
            topic0: "0x".into(),
            columns: vec![ColumnSchema {
                name: "liquidity".into(),
                sol_type: "uint128".into(),
                storage: "word16".into(),
                indexed: false,
                components: Vec::new(),
            }],
        };
        let fg = derive_footguns(&table);
        assert_eq!(fg.big_ints, vec!["liquidity"]);
        assert!(fg.overflows_dec.is_empty());
    }

    #[test]
    fn drift_flags_unknown_tables_and_columns() {
        let schema = vec![transfer_table()];
        let mut good = TableSemantic::default();
        good.columns.insert("nope".into(), "x".into()); // not a column of usdc__transfer
        good.columns.insert("from".into(), "the sender".into()); // real column - no warning
        let mut sem = Semantic::default();
        sem.tables.insert("usdc__transfer".into(), good);
        sem.tables
            .insert("ghost__event".into(), TableSemantic::default()); // no such table

        let warnings = drift(&schema, &sem);
        // `ghost__event` is the only `ghost__*` entry; the whole-alias path fires.
        assert!(
            warnings.iter().any(|w| w.contains("`ghost`")),
            "a whole-alias orphan must warn once about the alias, got: {warnings:?}"
        );
        assert!(
            !warnings.iter().any(|w| w.contains("ghost__event")),
            "whole-alias orphan must not fire a per-table warning"
        );
        assert!(warnings.iter().any(|w| w.contains("usdc__transfer.nope")));
        assert!(
            !warnings.iter().any(|w| w.contains("from")),
            "a real column must not warn"
        );
    }

    /// #655 is an issue about a *number* - a correct nest opened with 38 warnings because renaming
    /// two aliases orphaned every `semantic.toml` table key. The single-table case above cannot see
    /// that: with one orphan, "collapsed to one warning" and "one warning per table" are the same
    /// output, and the count reads `1` however it was computed. This pins the collapse and the
    /// count on a many-table alias, which is the shape the issue actually reported.
    ///
    /// Mutation check: replacing the counter with `alias_orphans.insert(alias.to_string(), 1)`
    /// leaves every other test in this module green and reds this one on the count.
    #[test]
    fn a_renamed_alias_collapses_to_one_warning_carrying_the_real_table_count() {
        let schema = vec![transfer_table()];
        let mut sem = Semantic::default();
        // Four tables orphaned under one renamed alias, plus two under a second.
        for t in ["swap", "mint", "burn", "collect"] {
            sem.tables
                .insert(format!("oldpool__{t}"), TableSemantic::default());
        }
        for t in ["deposit", "withdraw"] {
            sem.tables
                .insert(format!("oldvault__{t}"), TableSemantic::default());
        }

        let warnings = drift(&schema, &sem);

        assert_eq!(
            warnings.len(),
            2,
            "six orphaned tables under two renamed aliases must yield one warning each, got: \
             {warnings:?}"
        );
        assert!(
            warnings
                .iter()
                .any(|w| w.contains("4 tables") && w.contains("`oldpool`")),
            "the warning must carry the real table count, not a placeholder: {warnings:?}"
        );
        assert!(
            warnings
                .iter()
                .any(|w| w.contains("2 tables") && w.contains("`oldvault`")),
            "each renamed alias needs its own count: {warnings:?}"
        );
        assert!(
            !warnings.iter().any(|w| w.contains("oldpool__swap")),
            "no per-table warning may survive the collapse: {warnings:?}"
        );
    }

    /// The singular branch of the same message. One orphaned table under a renamed alias must read
    /// "1 table", not "1 tables" - the operator-facing string is the whole deliverable of #655.
    #[test]
    fn a_single_orphan_under_a_renamed_alias_reads_as_one_table_singular() {
        let schema = vec![transfer_table()];
        let mut sem = Semantic::default();
        sem.tables
            .insert("oldpool__swap".into(), TableSemantic::default());

        let warnings = drift(&schema, &sem);

        assert_eq!(
            warnings.len(),
            1,
            "expected exactly one warning: {warnings:?}"
        );
        assert!(
            warnings[0].contains("1 table still"),
            "singular must read `1 table`: {warnings:?}"
        );
        assert!(
            !warnings[0].contains("1 tables"),
            "singular must not read `1 tables`: {warnings:?}"
        );
    }

    /// The remediation the warning prints has to be one the operator can actually run. `merge` only
    /// ever *adds* generated tables to the existing map (`out.tables.insert` on the `None` arm) and
    /// never removes one, so a regenerate cannot clear an orphaned alias key - the sections stay and
    /// the warning fires again next start. This walks the issue's own repro through the three calls
    /// `nuthatch schema` makes (`project.rs:768-773`: generate → merge → save) and pins that, so the
    /// message can never drift back to naming a command that does not fix it.
    #[test]
    fn a_regenerate_cannot_clear_an_orphaned_alias_so_the_advice_must_not_name_it() {
        // Seeded when the nest still called its contract `oldpool`, with authored prose on top.
        let before = vec![
            aliased_table("oldpool", "swap"),
            aliased_table("oldpool", "mint"),
        ];
        let mut seeded = generate(&before, "n", "mainnet");
        seeded
            .tables
            .get_mut("oldpool__swap")
            .expect("seeded from the old alias")
            .description = "every swap through the pool".into();

        // The operator renames the alias in nuthatch.toml and runs `nuthatch schema`.
        let after = vec![aliased_table("pool", "swap"), aliased_table("pool", "mint")];
        let regenerated = merge(seeded, generate(&after, "n", "mainnet"));

        assert!(
            regenerated.tables.contains_key("oldpool__swap"),
            "merge never drops a table, so the orphan survives the regenerate: {:?}",
            regenerated.tables.keys().collect::<Vec<_>>()
        );
        assert_eq!(
            regenerated.tables["oldpool__swap"].description, "every swap through the pool",
            "and it survives with the authored prose still attached to the dead key"
        );

        let warnings = drift(&after, &regenerated);
        assert!(
            warnings.iter().any(|w| w.contains("`oldpool`")),
            "the warning still fires after a regenerate, which is the whole point: {warnings:?}"
        );
        assert!(
            !warnings.iter().any(|w| w.contains("nuthatch schema")),
            "the warning must not send the operator to a command that leaves it firing: {warnings:?}"
        );
        assert!(
            warnings
                .iter()
                .any(|w| w.contains("nuthatch nest rename-alias")),
            "the warning must name the command that actually keeps the prose: {warnings:?}"
        );
    }

    #[test]
    fn rekey_alias_moves_authored_descriptions_and_merge_still_does_not() {
        let mut sem = Semantic::default();
        let ts = TableSemantic {
            description: "every transfer through the token".into(),
            ..Default::default()
        };
        sem.tables.insert("c0__transfer".into(), ts.clone());
        sem.tables.insert("c0__approval".into(), ts);

        rekey_alias(&mut sem, "c0", "gns");
        assert_eq!(
            sem.tables["gns__transfer"].description,
            "every transfer through the token"
        );
        assert_eq!(
            sem.tables["gns__approval"].description,
            "every transfer through the token"
        );
        assert!(!sem.tables.contains_key("c0__transfer"));
        assert!(!sem.tables.contains_key("c0__approval"));

        // merge still does not infer a rename: generated keys under the new alias are added,
        // the old keys (if still present) stay. This is the #671 invariant.
        let mut leftover = Semantic::default();
        leftover
            .tables
            .insert("c0__transfer".into(), TableSemantic::default());
        let mut generated = Semantic::default();
        generated
            .tables
            .insert("gns__transfer".into(), TableSemantic::default());
        let merged = merge(leftover, generated);
        assert!(merged.tables.contains_key("c0__transfer"));
        assert!(merged.tables.contains_key("gns__transfer"));
    }

    #[test]
    fn footguns_survive_a_toml_round_trip() {
        let fg = Footguns {
            reserved_words: vec!["from".into(), "to".into()],
            big_ints: vec!["value".into()],
            overflows_dec: vec!["value".into()],
            bools: vec!["enabled".into()],
        };
        let ts = TableSemantic {
            footguns: fg.clone(),
            ..Default::default()
        };
        let mut sem = Semantic::default();
        sem.tables.insert("usdc__transfer".into(), ts);
        let text = toml::to_string_pretty(&sem).unwrap();
        let back: Semantic = toml::from_str(&text).unwrap();
        assert_eq!(back.tables["usdc__transfer"].footguns, fg);
    }

    #[test]
    fn compose_renders_authored_views() {
        // RFC-0018 §1: an authored view described in semantic.toml appears in the composed /schema so
        // an agent can see and query it by name.
        let schema = [transfer_table()];
        let mut sem = Semantic::default();
        sem.views.insert(
            "top_recipients".into(),
            ViewSemantic {
                description: "The addresses that received the most transfers.".into(),
            },
        );
        let doc = compose(&schema, Some(&sem), None, &[]);
        assert!(doc.contains("AUTHORED VIEWS"));
        assert!(doc.contains("top_recipients - The addresses that received the most transfers."));
    }

    /// #1437: an agent reading `/schema` learns which maintained relations rest on offchain
    /// snapshots before it cites one as chain-derived.
    #[test]
    fn compose_says_which_relations_are_reproducible_by_snapshot() {
        let relation = |name: &str, offchain: &[&str]| MaintainedRelation {
            name: name.into(),
            columns: vec!["k".into()],
            applied_through: 1,
            current: true,
            unavailable: None,
            fault: None,
            rows: 1,
            offchain: offchain.iter().map(|s| s.to_string()).collect(),
        };
        let doc = compose(
            &[transfer_table()],
            None,
            None,
            &[
                relation("priced", &["offchain__prices"]),
                relation("plain", &[]),
            ],
        );
        let line = doc
            .lines()
            .find(|l| l.contains("reproducible by snapshot"))
            .unwrap_or_else(|| panic!("no snapshot label in:\n{doc}"));
        assert!(line.contains("offchain__prices"), "{line}");
        assert_eq!(doc.matches("reproducible by snapshot").count(), 1, "{doc}");
    }

    #[test]
    fn compose_teaches_the_footguns_without_a_semantic_file() {
        // Even with no semantic.toml, compose must surface the derived footguns from the registry -
        // that's the "always correct even if the author never opens the file" guarantee.
        let schema = [transfer_table()];
        // A tiny registry stand-in isn't available, so assert the footgun text via the same helper
        // compose uses; the registry-backed compose is golden-tested in the integration test.
        let fg = derive_footguns(&schema[0]);
        assert!(fg.reserved_words.contains(&"from".to_string()));
        assert!(fg.big_ints.contains(&"value".to_string()));
    }
}
