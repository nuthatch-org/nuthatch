//! RFC-0020: nest lifecycle - the compatible-vs-breaking classifier (slice 1).
//!
//! An update `vN → vN+1` is **compatible** when every existing downstream query keeps working with
//! unchanged meaning, and **breaking** otherwise. Settled rule (RFC-0020): compatible = *additive
//! only* (a new table or column; nothing existing removed, renamed, retyped, or semantically
//! changed); breaking = anything a consumer can observe as removed/renamed/retyped/re-meant. A
//! conservative default falls to **breaking** when in doubt.
//!
//! This slice classifies over the **schema surface** - `schema.json`, the decoded event tables and
//! their columns, which is the concrete machine-readable contract a consumer queries (`SELECT … FROM
//! c0__transfer`). Later slices act on the verdict: compatible → hot-swap behind the same endpoint;
//! breaking → a new versioned endpoint run alongside the old. This slice only *decides*; it moves no
//! data and touches no serving path.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// The classification of an update.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Additive-only (or no change): safe to hot-swap behind the same endpoint.
    Compatible,
    /// A consumer-observable break: needs a new versioned endpoint, run alongside the old.
    Breaking,
}

/// A single schema difference between two versions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change {
    TableAdded(String),
    TableRemoved(String),
    ColumnAdded {
        table: String,
        column: String,
    },
    ColumnRemoved {
        table: String,
        column: String,
    },
    ColumnRetyped {
        table: String,
        column: String,
        from: String,
        to: String,
    },
}

impl Change {
    /// Additive changes preserve every existing query; everything else is consumer-observable and
    /// therefore breaking.
    pub fn is_breaking(&self) -> bool {
        !matches!(self, Change::TableAdded(_) | Change::ColumnAdded { .. })
    }

    /// A one-line, human-legible description for CLI/logs.
    pub fn describe(&self) -> String {
        match self {
            Change::TableAdded(t) => format!("table `{t}` added"),
            Change::TableRemoved(t) => format!("table `{t}` removed"),
            Change::ColumnAdded { table, column } => format!("column `{table}.{column}` added"),
            Change::ColumnRemoved { table, column } => format!("column `{table}.{column}` removed"),
            Change::ColumnRetyped {
                table,
                column,
                from,
                to,
            } => format!("column `{table}.{column}` retyped {from} → {to}"),
        }
    }
}

/// The full result: a verdict plus the changes that produced it (deterministically ordered).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Classification {
    pub verdict: Verdict,
    pub changes: Vec<Change>,
}

impl Classification {
    /// The subset of changes that force a breaking verdict.
    pub fn breaking_changes(&self) -> impl Iterator<Item = &Change> {
        self.changes.iter().filter(|c| c.is_breaking())
    }

    /// The additive (compatible) changes.
    pub fn additive_changes(&self) -> impl Iterator<Item = &Change> {
        self.changes.iter().filter(|c| !c.is_breaking())
    }
}

/// A column's observable type - the pair a consumer sees. `indexed` is deliberately excluded: it
/// affects query cost, not shape or meaning, so flipping it is not a break.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ColType {
    sol_type: String,
    storage: String,
}

impl ColType {
    fn describe(&self) -> String {
        format!("{}({})", self.sol_type, self.storage)
    }
}

/// A normalized schema: `table → (column → type)`, ordered for deterministic diffs.
type Schema = BTreeMap<String, BTreeMap<String, ColType>>;

#[derive(Deserialize)]
struct SchemaDoc {
    #[serde(default)]
    tables: Vec<TableDoc>,
}

#[derive(Deserialize)]
struct TableDoc {
    table: String,
    #[serde(default)]
    columns: Vec<ColDoc>,
}

#[derive(Deserialize)]
struct ColDoc {
    name: String,
    #[serde(default)]
    sol_type: String,
    #[serde(default)]
    storage: String,
}

/// Parse a `schema.json` document into the normalized [`Schema`].
fn parse_schema(json: &str) -> Result<Schema> {
    let doc: SchemaDoc = serde_json::from_str(json).context("parsing schema.json")?;
    let mut schema = Schema::new();
    for t in doc.tables {
        let cols = t
            .columns
            .into_iter()
            .map(|c| {
                (
                    c.name,
                    ColType {
                        sol_type: c.sol_type,
                        storage: c.storage,
                    },
                )
            })
            .collect();
        schema.insert(t.table, cols);
    }
    Ok(schema)
}

/// Classify `old → new` over two normalized schemas. Pure and deterministic.
fn classify(old: &Schema, new: &Schema) -> Classification {
    let mut changes = Vec::new();

    let tables: BTreeSet<&String> = old.keys().chain(new.keys()).collect();
    for table in tables {
        match (old.get(table), new.get(table)) {
            (None, Some(_)) => changes.push(Change::TableAdded(table.clone())),
            (Some(_), None) => changes.push(Change::TableRemoved(table.clone())),
            (Some(old_cols), Some(new_cols)) => {
                let cols: BTreeSet<&String> = old_cols.keys().chain(new_cols.keys()).collect();
                for column in cols {
                    match (old_cols.get(column), new_cols.get(column)) {
                        (None, Some(_)) => changes.push(Change::ColumnAdded {
                            table: table.clone(),
                            column: column.clone(),
                        }),
                        (Some(_), None) => changes.push(Change::ColumnRemoved {
                            table: table.clone(),
                            column: column.clone(),
                        }),
                        (Some(a), Some(b)) if a != b => changes.push(Change::ColumnRetyped {
                            table: table.clone(),
                            column: column.clone(),
                            from: a.describe(),
                            to: b.describe(),
                        }),
                        _ => {}
                    }
                }
            }
            (None, None) => unreachable!("table came from the union of both key sets"),
        }
    }

    let verdict = if changes.iter().any(Change::is_breaking) {
        Verdict::Breaking
    } else {
        Verdict::Compatible
    };
    Classification { verdict, changes }
}

/// Classify an update from two `schema.json` documents. Errors only on malformed JSON - a real fault,
/// distinct from a *breaking* verdict.
pub fn classify_schemas(old_json: &str, new_json: &str) -> Result<Classification> {
    let old = parse_schema(old_json)?;
    let new = parse_schema(new_json)?;
    Ok(classify(&old, &new))
}

/// Resolve a `schema.json` from a path that is either a nest directory or the file itself.
fn schema_path(p: &Path) -> PathBuf {
    if p.is_dir() {
        p.join("schema.json")
    } else {
        p.to_path_buf()
    }
}

/// Classify an update between two nests, each given as a nest directory or a `schema.json` path.
/// Shared by `nest diff` (slice 1) and the hot-upgrade gate (slice 2b).
pub fn classify_paths(old: &Path, new: &Path) -> Result<Classification> {
    let old_json = std::fs::read_to_string(schema_path(old))
        .with_context(|| format!("reading old schema from {}", old.display()))?;
    let new_json = std::fs::read_to_string(schema_path(new))
        .with_context(|| format!("reading new schema from {}", new.display()))?;
    classify_schemas(&old_json, &new_json)
}

/// Has the new version's indexed head reached the old version's? The flip condition a compatible hot
/// upgrade polls (RFC-0020 slice 2b): serving can atomically swap once the new version is at least as
/// current as the old, with no visible regression. The new version must have **actually indexed**
/// (`Some`) - waiting for that avoids a premature flip before the new indexer commits anything; a
/// missing old head counts as 0 (nothing to catch up to).
pub fn caught_up(new_head: Option<u64>, old_head: Option<u64>) -> bool {
    matches!(new_head, Some(n) if n >= old_head.unwrap_or(0))
}

/// The result of attempting segment reuse for a compatible upgrade (RFC-0020 slice 4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReuseOutcome {
    /// Decode unchanged - the old version's sealed segments were mounted into the new nest and its
    /// sealed watermark set, so the new indexer resumes past this range instead of re-indexing history.
    Reused {
        sealed_through: u64,
        segments: usize,
    },
    /// Not reusable - the new version must index history itself. Carries a human reason.
    NotReusable(String),
}

/// The decode-registry content hash a nest's `schema.json` pins - the key that decides segment reuse.
/// `None` if there's no `schema.json` or it lacks the field.
fn schema_registry_hash(dir: &Path) -> Result<Option<String>> {
    #[derive(Deserialize)]
    struct Head {
        #[serde(default)]
        registry_hash: Option<String>,
    }
    let Ok(raw) = std::fs::read_to_string(schema_path(dir)) else {
        return Ok(None);
    };
    Ok(serde_json::from_str::<Head>(&raw)
        .ok()
        .and_then(|h| h.registry_hash))
}

/// The true no-*re-index* optimization (RFC-0020 slice 4): when a compatible update leaves the **decode
/// registry unchanged**, the old version's sealed segments are byte-identical to what the new version
/// would produce, so mount them into the new nest instead of re-indexing. Copies `old/segments/*` →
/// `new/segments/` and sets the new store's sealed watermark, so the new indexer resumes *past* the
/// reused range (`resume_from_watermark`). A changed decode, or nothing sealed yet, falls back to a
/// normal index - [`ReuseOutcome::NotReusable`]. Content-addressing makes this sound: reused segments
/// carry their own hashes and are re-verifiable; this is a capability subgraphs structurally lack.
///
/// Call this **before** either version's indexer opens the stores (redb is single-writer).
pub fn reuse_segments(old_dir: &Path, new_dir: &Path) -> Result<ReuseOutcome> {
    let old_hash = schema_registry_hash(old_dir)?;
    if old_hash.is_none() || old_hash != schema_registry_hash(new_dir)? {
        return Ok(ReuseOutcome::NotReusable(
            "decode registry changed - history must be re-indexed with the new decoding".into(),
        ));
    }

    let old_seg = old_dir.join(crate::seal::SEGMENTS_DIR);
    if !old_seg.join(crate::seal::MANIFEST_FILE).exists() {
        return Ok(ReuseOutcome::NotReusable(
            "nothing sealed yet in the old version - nothing to reuse".into(),
        ));
    }

    // Read the *old* version's watermark and indexed layout before anything is copied. Opened and
    // dropped here, before the indexer opens it (redb is single-writer).
    //
    // Non-creating (#413, #469): a probe must not be able to answer itself. A directory can reach
    // here with sealed segments but no store yet - the manifest check above only proves segments
    // exist, not that a cursor ever opened this store - and a creating open would read
    // `sealed_through = 0` back from the empty file it had just written, set the new store's
    // watermark to 0, and litter the old directory with a store nobody indexed.
    let (old_sealed, old_layout) = {
        let s = crate::store::Store::open_existing(&old_dir.join(crate::config::DB_FILE))
            .with_context(|| {
                format!(
                    "reusing segments from {} needs its sealed watermark, but there is no store \
                     there to read it from",
                    old_dir.display()
                )
            })?;
        (
            s.sealed_through(),
            s.get_meta(crate::store::INDEXED_LAYOUT_KEY)?,
        )
    };
    // The registry hash cannot see a swap of indexed positions (#2157). A store from before the
    // layout was recorded has none, and is taken at its registry hash as before.
    if let Some(found) = old_layout {
        let found = serde_json::from_str(&found)
            .with_context(|| format!("unreadable indexed layout in {}", old_dir.display()))?;
        let config = crate::config::Config::load(new_dir)?;
        let want = crate::indexer::indexed_layout(&crate::registry::from_nest(new_dir, &config)?);
        let changes = crate::indexer::indexed_layout_changes(&found, &want);
        if !changes.is_empty() {
            return Ok(ReuseOutcome::NotReusable(format!(
                "indexed parameters changed - history must be re-indexed:\n{}",
                changes.join("\n")
            )));
        }
    }

    // Copy every sealed file (the manifest + each content-addressed .parquet) into the new nest.
    let new_seg = new_dir.join(crate::seal::SEGMENTS_DIR);
    std::fs::create_dir_all(&new_seg).with_context(|| format!("creating {}", new_seg.display()))?;
    let mut segments = 0usize;
    for entry in std::fs::read_dir(&old_seg)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            let name = entry.file_name();
            std::fs::copy(entry.path(), new_seg.join(&name))
                .with_context(|| format!("copying segment {}", name.to_string_lossy()))?;
            if name != std::ffi::OsStr::new(crate::seal::MANIFEST_FILE) {
                segments += 1;
            }
        }
    }

    {
        let s = crate::store::Store::open(&new_dir.join(crate::config::DB_FILE))?;
        s.set_meta("sealed_through", &old_sealed.to_string())?;
    }
    Ok(ReuseOutcome::Reused {
        sealed_through: old_sealed,
        segments,
    })
}

/// `nuthatch nest diff <old> <new>`: classify an update between two nests (each a nest dir or a
/// `schema.json` path) and print the verdict with its reasons. Slice 1 is decision-only - it prints
/// what a later slice will *act* on (compatible → same endpoint; breaking → a new one).
pub fn diff_cli(old: &Path, new: &Path) -> Result<()> {
    let c = classify_paths(old, new)?;

    let additive = c.additive_changes().count();
    match c.verdict {
        Verdict::Compatible => {
            println!(
                "✓ compatible - {additive} additive change(s), nothing removed/retyped. Safe to \
                 hot-swap behind the same endpoint."
            );
            for ch in c.additive_changes() {
                println!("  + {}", ch.describe());
            }
        }
        Verdict::Breaking => {
            println!(
                "✗ breaking - a consumer-observable change. Serve on a NEW versioned endpoint, run \
                 it alongside the old, and let downstream migrate on their clock."
            );
            for ch in c.breaking_changes() {
                println!("  - {}", ch.describe());
            }
            if additive > 0 {
                println!("  ({additive} additive change(s) also present)");
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // A tiny two-table schema: `c0__transfer(from,to,value)` and `c0__approval(owner,spender)`.
    fn base() -> String {
        r#"{
          "registry_hash": "0xabc",
          "tables": [
            {"table":"c0__transfer","alias":"c0","event":"Transfer","topic0":"0x0","columns":[
              {"name":"from","sol_type":"address","storage":"address","indexed":true},
              {"name":"to","sol_type":"address","storage":"address","indexed":true},
              {"name":"value","sol_type":"uint256","storage":"word32","indexed":false}
            ]},
            {"table":"c0__approval","alias":"c0","event":"Approval","topic0":"0x1","columns":[
              {"name":"owner","sol_type":"address","storage":"address","indexed":true},
              {"name":"spender","sol_type":"address","storage":"address","indexed":true}
            ]}
          ]
        }"#
        .to_string()
    }

    fn verdict(old: &str, new: &str) -> Classification {
        classify_schemas(old, new).unwrap()
    }

    #[test]
    fn identical_is_compatible_with_no_changes() {
        let c = verdict(&base(), &base());
        assert_eq!(c.verdict, Verdict::Compatible);
        assert!(c.changes.is_empty());
    }

    #[test]
    fn reuse_refused_when_decode_changes_or_nothing_sealed() {
        let old = tempfile::tempdir().unwrap();
        let new = tempfile::tempdir().unwrap();
        std::fs::write(
            old.path().join("schema.json"),
            r#"{"registry_hash":"0xAAA","tables":[]}"#,
        )
        .unwrap();
        // Different decode hash → not reusable, before touching any store or segments.
        std::fs::write(
            new.path().join("schema.json"),
            r#"{"registry_hash":"0xBBB","tables":[]}"#,
        )
        .unwrap();
        match reuse_segments(old.path(), new.path()).unwrap() {
            ReuseOutcome::NotReusable(why) => {
                assert!(why.contains("decode registry changed"), "{why}")
            }
            other => panic!("expected NotReusable, got {other:?}"),
        }
        // Same decode hash but nothing sealed in the old version → still nothing to reuse.
        std::fs::write(
            new.path().join("schema.json"),
            r#"{"registry_hash":"0xAAA","tables":[]}"#,
        )
        .unwrap();
        match reuse_segments(old.path(), new.path()).unwrap() {
            ReuseOutcome::NotReusable(why) => assert!(why.contains("nothing sealed"), "{why}"),
            other => panic!("expected NotReusable (nothing sealed), got {other:?}"),
        }
    }

    /// #413/#469: the old version's store is opened to read its sealed watermark, and a creating
    /// open answers that read with an empty file it just wrote instead of refusing. Reachable without
    /// a live cursor: `holds_data` counts a non-empty `segments/` as data before it ever looks for a
    /// store (`runtime.rs`), so a directory with sealed segments but no `nuthatch.redb` is not a
    /// contradiction - it is what a source dataset looks like the moment segments are sealed but
    /// before any indexer has opened the store here (or after a store was deleted out from under
    /// segments that are still valid, since they are content-addressed and outlive it).
    #[test]
    fn reuse_refuses_an_old_dir_with_sealed_segments_but_no_store() {
        let old = tempfile::tempdir().unwrap();
        let new = tempfile::tempdir().unwrap();
        std::fs::write(
            old.path().join("schema.json"),
            r#"{"registry_hash":"0xAAA","tables":[]}"#,
        )
        .unwrap();
        std::fs::write(
            new.path().join("schema.json"),
            r#"{"registry_hash":"0xAAA","tables":[]}"#,
        )
        .unwrap();
        // Sealed segments present, but no store - `holds_data` would already call this a dataset.
        let old_seg = old.path().join(crate::seal::SEGMENTS_DIR);
        std::fs::create_dir_all(&old_seg).unwrap();
        std::fs::write(old_seg.join(crate::seal::MANIFEST_FILE), "{}").unwrap();
        let old_db = old.path().join(crate::config::DB_FILE);
        assert!(!old_db.exists(), "fixture must start with no store");

        let err = reuse_segments(old.path(), new.path())
            .expect_err("no store to read the watermark from must refuse, not invent one")
            .to_string();
        assert!(
            err.contains(&old.path().display().to_string()),
            "the error must name the directory that has no store: {err}"
        );
        assert!(
            !old_db.exists(),
            "a refused read must not leave a nuthatch.redb behind in the source directory - that \
             litters it and, per issue #408/#415, can make it look unadoptable ever after"
        );
    }

    /// #2157: swapping which parameter is indexed keeps the registry hash, so the old segments hold
    /// the values in other columns than the new version decodes them into.
    #[test]
    fn reuse_declines_when_the_indexed_positions_swap() {
        let old = tempfile::tempdir().unwrap();
        let new = tempfile::tempdir().unwrap();
        for dir in [old.path(), new.path()] {
            std::fs::write(
                dir.join("schema.json"),
                r#"{"registry_hash":"0xAAA","tables":[]}"#,
            )
            .unwrap();
        }
        let old_seg = old.path().join(crate::seal::SEGMENTS_DIR);
        std::fs::create_dir_all(&old_seg).unwrap();
        std::fs::write(old_seg.join(crate::seal::MANIFEST_FILE), "{}").unwrap();
        {
            let s = crate::store::Store::open(&old.path().join(crate::config::DB_FILE)).unwrap();
            s.set_meta("sealed_through", "93").unwrap();
            s.set_meta(crate::store::INDEXED_LAYOUT_KEY, r#"{"e__e":[0]}"#)
                .unwrap();
        }
        let abi = |indexed: usize| {
            let p = |i: usize, n: &str| {
                format!(
                    r#"{{"name":"{n}","type":"uint256","indexed":{}}}"#,
                    i == indexed
                )
            };
            format!(
                r#"[{{"type":"event","name":"E","anonymous":false,"inputs":[{},{}]}}]"#,
                p(0, "a"),
                p(1, "b")
            )
        };
        std::fs::create_dir_all(new.path().join("abis")).unwrap();
        std::fs::write(
            new.path().join(crate::config::CONFIG_FILE),
            "[nest]\nname = \"swap\"\nchain = \"gnosis\"\nchain_id = 100\nrpc_urls = []\n\n\
             [[contracts]]\nalias = \"e\"\naddress = \"0x00000000000000000000000000000000000000aa\"\n\
             abi = \"abis/e.json\"\n",
        )
        .unwrap();

        std::fs::write(new.path().join("abis/e.json"), abi(1)).unwrap();
        match reuse_segments(old.path(), new.path()).unwrap() {
            ReuseOutcome::NotReusable(why) => assert!(
                why.contains("e__e: indexed parameter positions [0] stored, [1] configured"),
                "{why}"
            ),
            other => panic!("expected NotReusable, got {other:?}"),
        }
        assert!(
            !new.path().join(crate::seal::SEGMENTS_DIR).exists(),
            "a declined reuse must not copy segments"
        );

        std::fs::write(new.path().join("abis/e.json"), abi(0)).unwrap();
        assert_eq!(
            reuse_segments(old.path(), new.path()).unwrap(),
            ReuseOutcome::Reused {
                sealed_through: 93,
                segments: 0
            },
            "the same layout still reuses"
        );
    }

    #[test]
    fn caught_up_predicate() {
        assert!(caught_up(Some(100), Some(100)), "reached");
        assert!(caught_up(Some(101), Some(100)), "ahead");
        assert!(!caught_up(Some(99), Some(100)), "behind");
        assert!(
            !caught_up(None, None),
            "new not started yet - don't flip prematurely"
        );
        assert!(!caught_up(None, Some(1)), "new not started, old has data");
        assert!(caught_up(Some(1), None), "new started, old empty");
    }

    #[test]
    fn added_table_and_column_are_compatible() {
        // Same two tables, but `approval` gains a `note` column and a whole new `c0__mint` table
        // appears - both additive, nothing existing touched.
        let new = r#"{
          "tables": [
            {"table":"c0__transfer","columns":[
              {"name":"from","sol_type":"address","storage":"address","indexed":true},
              {"name":"to","sol_type":"address","storage":"address","indexed":true},
              {"name":"value","sol_type":"uint256","storage":"word32","indexed":false}
            ]},
            {"table":"c0__approval","columns":[
              {"name":"owner","sol_type":"address","storage":"address","indexed":true},
              {"name":"spender","sol_type":"address","storage":"address","indexed":true},
              {"name":"note","sol_type":"string","storage":"string","indexed":false}
            ]},
            {"table":"c0__mint","columns":[
              {"name":"amount","sol_type":"uint256","storage":"word32","indexed":false}
            ]}
          ]
        }"#;
        let c = verdict(&base(), new);
        assert_eq!(c.verdict, Verdict::Compatible, "changes: {:?}", c.changes);
        assert_eq!(c.additive_changes().count(), 2);
        assert_eq!(c.breaking_changes().count(), 0);
    }

    #[test]
    fn removed_table_is_breaking() {
        // Drop the approval table entirely.
        let new = r#"{"tables":[
            {"table":"c0__transfer","columns":[
              {"name":"from","sol_type":"address","storage":"address"},
              {"name":"to","sol_type":"address","storage":"address"},
              {"name":"value","sol_type":"uint256","storage":"word32"}
            ]}
          ]}"#;
        let c = verdict(&base(), new);
        assert_eq!(c.verdict, Verdict::Breaking);
        assert!(c
            .changes
            .contains(&Change::TableRemoved("c0__approval".to_string())));
    }

    #[test]
    fn removed_column_is_breaking() {
        let new = r#"{"tables":[
            {"table":"c0__transfer","columns":[
              {"name":"from","sol_type":"address","storage":"address"},
              {"name":"value","sol_type":"uint256","storage":"word32"}
            ]},
            {"table":"c0__approval","columns":[
              {"name":"owner","sol_type":"address","storage":"address"},
              {"name":"spender","sol_type":"address","storage":"address"}
            ]}
          ]}"#;
        let c = verdict(&base(), new);
        assert_eq!(c.verdict, Verdict::Breaking);
        assert!(c.changes.contains(&Change::ColumnRemoved {
            table: "c0__transfer".to_string(),
            column: "to".to_string(),
        }));
    }

    #[test]
    fn retyped_column_is_breaking() {
        // `value` goes uint256(word32) → uint128(word16): a consumer-observable retype.
        let new = base().replace(
            r#"{"name":"value","sol_type":"uint256","storage":"word32","indexed":false}"#,
            r#"{"name":"value","sol_type":"uint128","storage":"word16","indexed":false}"#,
        );
        let c = verdict(&base(), &new);
        assert_eq!(c.verdict, Verdict::Breaking);
        assert!(c.changes.iter().any(|ch| matches!(
            ch,
            Change::ColumnRetyped { table, column, .. }
            if table == "c0__transfer" && column == "value"
        )));
    }

    #[test]
    fn indexed_flag_change_alone_is_compatible() {
        // Flipping `indexed` changes query cost, not shape/meaning → not a break.
        let new = base().replace(
            r#"{"name":"from","sol_type":"address","storage":"address","indexed":true}"#,
            r#"{"name":"from","sol_type":"address","storage":"address","indexed":false}"#,
        );
        let c = verdict(&base(), &new);
        assert_eq!(c.verdict, Verdict::Compatible, "changes: {:?}", c.changes);
        assert!(c.changes.is_empty());
    }

    #[test]
    fn mixed_additive_and_breaking_is_breaking() {
        // Add a column (additive) AND remove a table (breaking) → the whole update is breaking.
        let new = r#"{"tables":[
            {"table":"c0__transfer","columns":[
              {"name":"from","sol_type":"address","storage":"address"},
              {"name":"to","sol_type":"address","storage":"address"},
              {"name":"value","sol_type":"uint256","storage":"word32"},
              {"name":"note","sol_type":"string","storage":"string"}
            ]}
          ]}"#;
        let c = verdict(&base(), new);
        assert_eq!(c.verdict, Verdict::Breaking);
        assert_eq!(c.additive_changes().count(), 1); // the added `note`
        assert!(c.breaking_changes().count() >= 1); // the removed approval table
    }

    #[test]
    fn malformed_json_errors_rather_than_misclassifies() {
        assert!(classify_schemas("not json", &base()).is_err());
        assert!(classify_schemas(&base(), "{").is_err());
    }
}
