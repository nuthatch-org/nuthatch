//! `nuthatch check` - run a nest's invariant/parity checks (RFC-0002 §5).
//!
//! Each `checks/<name>.sql` is a read-only query run over the nest's sealed data (the same Burrmill
//! surface as `/sql`, so it sees the per-event tables *and* the nest's derived views). Its result is
//! compared to a recorded expected fixture `checks/expected/<name>.json`. For the Horizon nest those
//! fixtures are the deployed subgraph's answers at a pinned block, so this is a parity check; the
//! framework itself is generic (any nest can ship invariant checks).
//!
//! Hermetic by design - it compares against committed fixtures, not a live endpoint, so it runs in
//! CI with no network. `--update` re-records the fixtures from current results (authoring, run once
//! against known-good sealed data). Refreshing fixtures from a live subgraph is a nest-side chore,
//! not this command's job.

use anyhow::{bail, Context, Result};
use serde_json::Value;
use std::path::{Path, PathBuf};

use crate::analytics;
use crate::cli::CheckArgs;

pub fn check(args: CheckArgs) -> Result<()> {
    let dir = PathBuf::from(&args.dir);

    #[cfg(feature = "folds")]
    if args.folds {
        return check_folds(&dir, args.from_genesis);
    }

    // A misspelt key is dropped and its default used (#1582, #1656). Here an author learns.
    crate::config::refuse_unknown_file::<crate::config::Config>(&dir, crate::config::CONFIG_FILE)?;
    crate::config::refuse_unknown_file::<crate::runtime::MountTable>(
        &dir,
        crate::runtime::MOUNTS_FILE,
    )?;
    crate::entities::refuse_unknown_keys(&dir)?;
    crate::config::refuse_unknown_file::<crate::allowlist::Ceiling>(
        &dir,
        crate::allowlist::CEILING_FILE,
    )?;
    crate::config::refuse_unknown_file::<crate::semantic::Semantic>(&dir, "semantic.toml")?;
    crate::config::refuse_unknown_file::<crate::maintained::Declaration>(
        &dir,
        crate::maintained::DECLARATION_FILE,
    )?;

    // Grafting (RFC-0033) is reported **before** the parity checks, and before the no-checks bail: a
    // nest with no `checks/*.sql` is the common case, and its author still deserves to know which of
    // their views can never be reused. Reporting it after the bail made this dead code for most
    // nests - found by running the command rather than by the tests, which supplied a checks dir.
    let graft = crate::graft::report(&dir);
    if let Some(cycle) = &graft.cycle {
        bail!(
            "this nest's derivations form a cycle: {cycle}. A derivation may read decoded events and \
             other derivations, never itself."
        );
    }
    for (view, why) in &graft.never_graftable {
        println!("! view {view} can never be reused across an edit: it {why}");
    }
    for view in &graft.uncanonical {
        println!(
            "! view {view}: plan could not be canonicalised, so only a byte-identical edit matches"
        );
    }

    let checks = collect_checks(&dir, args.name.as_deref())?;

    let mut failures = 0usize;

    // RFC-0018 §1: authored views are validated as part of `check` - a broken view, or one that
    // references a table/column the registry no longer has (**drift**), fails loudly with a
    // fuzzy-matched fix hint instead of vanishing silently. This runs before the parity checks so a
    // drifted view is caught even if it's the reason a parity check would fail.
    //
    // Deliberately **before** the no-checks bail below, and evaluated regardless of whether it finds
    // anything (#539): a nest with views and no `checks/*.sql` yet is a normal intermediate state,
    // and it's exactly when an author most wants this validator. The old order bailed on the empty
    // `checks/` dir first, which made view drift invisible until a check happened to exist.
    let views_validated = nest_schema(&dir)?.map(|schema| {
        let issues = analytics::validate_nest_views(&dir, &schema);
        let n = issues.len();
        for issue in issues {
            let hint = issue
                .hint
                .map(|h| format!("\n    hint: {h}"))
                .unwrap_or_default();
            let first = issue.error.lines().next().unwrap_or(&issue.error);
            println!("✗ view {}: {first}{hint}", issue.file);
            failures += 1;
        }
        n
    });
    let entity_issues = crate::entities::validate(&dir);
    for issue in &entity_issues {
        println!("✗ incremental entity {}: {}", issue.name, issue.error);
        failures += 1;
    }
    let maintained = crate::maintained::refusals(&dir)?;
    for (view, why) in &maintained {
        println!("✗ maintained view {view}: {why}");
        failures += 1;
    }
    let has_maintained = crate::maintained::read(&dir)?.is_some_and(|d| !d.view.is_empty());

    if checks.is_empty() {
        let has_views = !analytics::nest_view_files(&dir).is_empty();
        let has_entities = crate::entities::has_declarations(&dir);
        // Only truly nothing to check - no checks, and either no views or no schema to validate them
        // against - keeps the original bail. A nest with views that were actually validated above
        // got a real answer instead, even with `checks/` empty.
        if (!has_views || views_validated.is_none()) && !has_entities && !has_maintained {
            bail!(
                "no checks found in {} (expected checks/*.sql)",
                dir.join("checks").display()
            );
        }
        if failures > 0 {
            bail!("{failures} authored-definition issue(s) found");
        }
        println!("✓ no checks/*.sql, but all authored view(s) and entity declaration(s) validate cleanly");
        return Ok(());
    }

    let expected_dir = dir.join("checks").join("expected");
    if args.update {
        std::fs::create_dir_all(&expected_dir)
            .with_context(|| format!("cannot create {}", expected_dir.display()))?;
    }

    for (name, sql_path) in &checks {
        let sql = std::fs::read_to_string(sql_path)
            .with_context(|| format!("cannot read {}", sql_path.display()))?;
        let got = match analytics::query(&dir, &sql) {
            Ok(rows) => rows,
            Err(e) => {
                println!("✗ {name}: query failed - {e:#}");
                failures += 1;
                continue;
            }
        };
        let exp_path = expected_dir.join(format!("{name}.json"));

        if args.update {
            std::fs::write(&exp_path, serde_json::to_string_pretty(&got)?)
                .with_context(|| format!("cannot write {}", exp_path.display()))?;
            println!("● {name}: recorded {} row(s)", got.len());
            continue;
        }

        let expected: Vec<Value> = match std::fs::read_to_string(&exp_path) {
            Ok(raw) => serde_json::from_str(&raw)
                .with_context(|| format!("corrupt fixture {}", exp_path.display()))?,
            Err(_) => {
                println!(
                    "✗ {name}: no expected fixture - run `nuthatch check --update` to record it"
                );
                failures += 1;
                continue;
            }
        };

        match diff(&expected, &got) {
            None => println!("✓ {name}: {} row(s) match", got.len()),
            Some(msg) => {
                println!("✗ {name}: {msg}");
                failures += 1;
            }
        }
    }

    if failures > 0 {
        bail!("{failures}/{} check(s) failed", checks.len());
    }
    println!("✓ all {} check(s) passed", checks.len());
    Ok(())
}

/// `nuthatch check --folds`: every fold's checkpoints recompute from the sealed facts, or the ones
/// that do not are named.
#[cfg(feature = "folds")]
fn check_folds(dir: &Path, from_genesis: bool) -> Result<()> {
    let schema = nest_schema(dir)?.unwrap_or_default();
    let set = crate::folds::FoldSet::load(dir, &schema)?;
    if set.folds.is_empty() {
        bail!("{} has no folds/", dir.display());
    }
    let failures = set.verify(dir, &schema, from_genesis)?;
    for line in &failures {
        println!("✗ {line}");
    }
    if !failures.is_empty() {
        bail!("{} checkpoint(s) do not recompute", failures.len());
    }
    println!(
        "✓ {} fold(s) recompute {}",
        set.folds.len(),
        if from_genesis {
            "from genesis"
        } else {
            "over the last window"
        }
    );
    Ok(())
}

/// The nest's decode-registry table schemas, for view drift-validation. `None` if the dir isn't a
/// nest (no config), and view validation is then skipped. A config `dev` would refuse is an error
/// (#2687): skipping on it let `check` pass a nest that does not start.
fn nest_schema(dir: &Path) -> Result<Option<Vec<crate::registry::TableSchema>>> {
    if !dir.join(crate::config::CONFIG_FILE).exists() {
        return Ok(None);
    }
    let cfg = crate::config::Config::load(dir)?;
    let reg = crate::registry::from_nest(dir, &cfg).context("building the decode registry")?;
    Ok(Some(reg.schema()))
}

/// Every `checks/<name>.sql` (sorted), optionally filtered to names containing `filter`.
fn collect_checks(dir: &Path, filter: Option<&str>) -> Result<Vec<(String, PathBuf)>> {
    let checks_dir = dir.join("checks");
    let entries = match std::fs::read_dir(&checks_dir) {
        Ok(e) => e,
        Err(_) => return Ok(Vec::new()),
    };
    let mut out: Vec<(String, PathBuf)> = entries
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "sql"))
        .filter_map(|p| {
            let name = p.file_stem()?.to_string_lossy().into_owned();
            match filter {
                Some(f) if !name.contains(f) => None,
                _ => Some((name, p)),
            }
        })
        .collect();
    out.sort();
    Ok(out)
}

/// Compare expected vs actual result sets. Returns None if identical, else a human diff of the first
/// discrepancy. Row order is significant - checks should `ORDER BY` for a deterministic comparison.
fn diff(expected: &[Value], got: &[Value]) -> Option<String> {
    if expected.len() != got.len() {
        return Some(format!(
            "row count differs: expected {}, got {}",
            expected.len(),
            got.len()
        ));
    }
    for (i, (e, g)) in expected.iter().zip(got).enumerate() {
        if e != g {
            return Some(format!(
                "row {i} differs:\n    expected: {e}\n    got:      {g}"
            ));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn diff_detects_count_and_value_mismatches() {
        let a = vec![json!({"x": 1}), json!({"x": 2})];
        assert!(diff(&a, &a).is_none());
        assert!(diff(&a, &a[..1]).unwrap().contains("row count"));
        let b = vec![json!({"x": 1}), json!({"x": 9})];
        assert!(diff(&a, &b).unwrap().contains("row 1 differs"));
    }

    /// A minimal nest with no `[[contracts]]` - `Config::load` and `DecodeRegistry::from_nest` both
    /// succeed with an empty schema, so `nest_schema` returns `Some(vec![])` without needing any ABI
    /// fixture. Enough to drive view validation on self-contained views that reference no real table.
    fn write_minimal_nest(dir: &Path) {
        std::fs::write(
            dir.join("nuthatch.toml"),
            "[nest]\nname = \"t\"\nchain = \"mainnet\"\nchain_id = 1\nrpc_urls = []\n",
        )
        .unwrap();
    }

    /// #539 fix 3: a nest with views and no `checks/*.sql` yet is a normal intermediate state, not an
    /// error - `check` must still validate the views instead of bailing on the missing directory.
    #[test]
    fn no_checks_directory_still_validates_a_clean_view() {
        let dir = tempfile::tempdir().unwrap();
        write_minimal_nest(dir.path());
        std::fs::create_dir_all(dir.path().join("views")).unwrap();
        std::fs::write(
            dir.path().join("views/10-ok.sql"),
            "CREATE VIEW ok AS SELECT 1 AS x;",
        )
        .unwrap();

        let result = check(CheckArgs {
            name: None,
            dir: dir.path().display().to_string(),
            update: false,
            #[cfg(feature = "folds")]
            folds: false,
            #[cfg(feature = "folds")]
            from_genesis: false,
        });
        assert!(
            result.is_ok(),
            "a clean view with no checks/ must not bail on the missing directory: {result:?}"
        );
    }

    /// The other half: a broken view with no `checks/*.sql` must fail *because of the view*, not
    /// disappear behind (or be misreported as) the old "no checks found" bail.
    #[test]
    fn no_checks_directory_still_reports_a_broken_view() {
        let dir = tempfile::tempdir().unwrap();
        write_minimal_nest(dir.path());
        std::fs::create_dir_all(dir.path().join("views")).unwrap();
        std::fs::write(
            dir.path().join("views/10-broken.sql"),
            "CREATE VIEW broken AS SELECT * FROM nonexistent_table;",
        )
        .unwrap();

        let err = check(CheckArgs {
            name: None,
            dir: dir.path().display().to_string(),
            update: false,
            #[cfg(feature = "folds")]
            folds: false,
            #[cfg(feature = "folds")]
            from_genesis: false,
        })
        .unwrap_err()
        .to_string();
        assert!(
            !err.contains("no checks found"),
            "must not be the generic empty-checks bail: {err}"
        );
        assert!(err.contains("authored-definition issue"), "{err}");
    }

    /// #2687: a `nuthatch.toml` that `dev` refuses at load made every bind check skip, so `check`
    /// passed a nest with a refused webhook and an entity over a table that does not exist.
    #[test]
    fn a_config_dev_refuses_fails_check_with_the_load_error() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("nuthatch.toml"),
            "[nest]\nname = \"t\"\nchain = \"mainnet\"\nchain_id = 1\nrpc_urls = []\n\
             [[webhooks]]\nname = \"fast\"\ntable = \"t\"\nurl = \"http://127.0.0.1:8099/\"\n\
             finality = \"tip\"\n",
        )
        .unwrap();
        std::fs::create_dir_all(dir.path().join("entities")).unwrap();
        std::fs::write(
            dir.path().join("entities/totals.sql"),
            "SELECT owner, count(*) AS n FROM missing GROUP BY owner",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("entities.toml"),
            "[[entities]]\nname='totals'\nsql='entities/totals.sql'\nkey=['owner']\nmax_rows=10\n",
        )
        .unwrap();

        let err = check(CheckArgs {
            name: None,
            dir: dir.path().display().to_string(),
            update: false,
            #[cfg(feature = "folds")]
            folds: false,
            #[cfg(feature = "folds")]
            from_genesis: false,
        })
        .unwrap_err();
        assert!(format!("{err:#}").contains("#577"), "{err:#}");
    }

    /// Unchanged: a nest with neither `checks/*.sql` nor any views has truly nothing to check.
    #[test]
    fn no_checks_and_no_views_still_bails() {
        let dir = tempfile::tempdir().unwrap();
        write_minimal_nest(dir.path());

        let err = check(CheckArgs {
            name: None,
            dir: dir.path().display().to_string(),
            update: false,
            #[cfg(feature = "folds")]
            folds: false,
            #[cfg(feature = "folds")]
            from_genesis: false,
        })
        .unwrap_err()
        .to_string();
        assert!(err.contains("no checks found"), "{err}");
    }

    /// #1656: a key no struct reads is dropped, and `check` agrees. `sq1 = "deny"` then leaves
    /// `/sql` open.
    #[test]
    fn a_misspelt_key_in_the_other_config_files_fails_check() {
        let err_of = |file: &str, body: &str, needle: &str| {
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(dir.path().join(file), body).unwrap();
            let err = check(CheckArgs {
                name: None,
                dir: dir.path().display().to_string(),
                update: false,
                #[cfg(feature = "folds")]
                folds: false,
                #[cfg(feature = "folds")]
                from_genesis: false,
            })
            .unwrap_err()
            .to_string();
            assert!(err.contains(needle), "{file}: {err}");
        };
        err_of(
            "mounts.toml",
            "[runtime]\nname = \"r\"\nchain = \"mainnet\"\nchain_id = 1\n\n\
             [[mounts]]\nalias = \"usdc\"\nnid = \"aa\"\nsq1 = \"deny\"\n",
            "sq1 (did you mean `sql`?)",
        );
        err_of(
            "entities.toml",
            "[[entites]]\nname = \"holder\"\nsql = \"entities/holder.sql\"\n\
             key = [\"id\"]\nmax_rows = 1\n",
            "entites (did you mean `entities`?)",
        );
        err_of(
            "queries.toml",
            "queres = []\n",
            "queres (did you mean `queries`?)",
        );
        err_of(
            "semantic.toml",
            "schema_versio = 1\n",
            "schema_versio (did you mean `schema_version`?)",
        );
    }

    /// The same files, spelled as shipped, must not fail check. A false positive fails every nest.
    #[test]
    fn shipped_config_files_declare_only_keys_nuthatch_reads() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let read = |rel: &str| std::fs::read_to_string(root.join(rel)).unwrap();
        for rel in [
            "tests/fixtures/upgrade/config-4.0/mounts.toml",
            "tests/fixtures/upgrade/v3.13.2/runtime/mounts.toml",
        ] {
            let unknown = crate::config::unknown_paths::<crate::runtime::MountTable>(&read(rel));
            assert_eq!(unknown, Vec::<String>::new(), "{rel}: {unknown:?}");
        }
        let nid = "2bca092694c5833d2fecec20983b307f23c3a5507402c39bab4d3fab91231576";
        let old = format!("tests/fixtures/upgrade/v3.13.2/runtime/data/{nid}/entities.toml");
        for rel in [
            "tests/fixtures/upgrade/config-4.0/nest/entities.toml",
            old.as_str(),
        ] {
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(dir.path().join("entities.toml"), read(rel)).unwrap();
            crate::entities::refuse_unknown_keys(dir.path())
                .unwrap_or_else(|e| panic!("{rel}: {e}"));
        }
        for rel in ["obib-case2/semantic.toml", "obib-case3/semantic.toml"] {
            let unknown = crate::config::unknown_paths::<crate::semantic::Semantic>(&read(rel));
            assert_eq!(unknown, Vec::<String>::new(), "{rel}: {unknown:?}");
        }
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("queries.toml"),
            "[[queries]]\nname = \"held\"\nsql = \"select 1\"\n",
        )
        .unwrap();
        crate::config::refuse_unknown_file::<crate::allowlist::Ceiling>(dir.path(), "queries.toml")
            .unwrap();
    }

    #[test]
    fn no_checks_directory_still_validates_a_clean_entity() {
        let dir = tempfile::tempdir().unwrap();
        write_minimal_nest(dir.path());
        std::fs::create_dir_all(dir.path().join("entities")).unwrap();
        std::fs::write(
            dir.path().join("entities.toml"),
            "[[entities]]\nname = \"constant\"\nsql = \"entities/constant.sql\"\nkey = [\"id\"]\nmax_rows = 1\n",
        )
        .unwrap();
        // A shape `dev` would start: a real table, and SQL the lowerer accepts (#1590).
        std::fs::create_dir_all(dir.path().join("abis")).unwrap();
        std::fs::write(
            dir.path().join("abis/t.json"),
            r#"[{"type":"event","name":"Seen","anonymous":false,"inputs":[
                {"name":"who","type":"address","indexed":true}]}]"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("nuthatch.toml"),
            "[nest]\nname = \"t\"\nchain = \"mainnet\"\nchain_id = 1\nrpc_urls = []\n\
             [[contracts]]\nalias = \"t\"\naddress = \"0x00000000000000000000000000000000000000aa\"\n\
             abi = \"abis/t.json\"\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("entities/constant.sql"),
            "SELECT who AS id, count(*) AS n FROM t__seen GROUP BY who",
        )
        .unwrap();

        let result = check(CheckArgs {
            name: None,
            dir: dir.path().display().to_string(),
            update: false,
            #[cfg(feature = "folds")]
            folds: false,
            #[cfg(feature = "folds")]
            from_genesis: false,
        });
        assert!(
            result.is_ok(),
            "a clean entity with no checks/ must not bail on the missing directory: {result:?}"
        );
    }

    #[test]
    fn collect_filters_and_sorts() {
        let dir = tempfile::tempdir().unwrap();
        let checks = dir.path().join("checks");
        std::fs::create_dir_all(&checks).unwrap();
        std::fs::write(checks.join("parity_rewards.sql"), "SELECT 1").unwrap();
        std::fs::write(checks.join("parity_allocs.sql"), "SELECT 1").unwrap();
        std::fs::write(checks.join("other.sql"), "SELECT 1").unwrap();

        let all = collect_checks(dir.path(), None).unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].0, "other"); // sorted
        let parity = collect_checks(dir.path(), Some("parity")).unwrap();
        assert_eq!(parity.len(), 2);
        assert!(parity.iter().all(|(n, _)| n.contains("parity")));
    }
}
