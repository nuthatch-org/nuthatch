//! RFC-0053's compatibility surface is an optional lane. The delightful core (RFC-0015) is not.
//!
//! Chief, 2026-09-11: *"nuthatch's default and golden path should remain the delightful core that
//! remains perfect and pristine"*, and *"graphql is just an optional serving lane"*. Both are true of
//! the tree today. This file is what makes them stay true, on the pattern `payment_absent.rs`
//! established for RFC-0046 §1: a boundary nobody can cross by accident is a gate, not a paragraph.
//!
//! The golden path is `init 0xAddr` -> `dev` -> `/entities`, `/entity/{id}`, `/sql`. Four properties
//! hold it apart from the Graph dialect, and each one here can fail:
//!
//! 1. `init` writes no Graph artefact, on either of its paths, so a scaffolded nest has nothing to
//!    serve the lane from.
//! 2. `graph/schema.graphql` is read at exactly one production site - inside the request handler - so
//!    no startup path can come to depend on it.
//! 3. The Graph dialect is not configurable. No flag on `dev` or `serve` switches it on or off,
//!    because the artefact is the opt-in and a second gate is a second thing to get wrong.
//! 4. The data is the same underneath. The lane compiles to SQL and goes through the same
//!    `run_sql_query` that `/sql` uses, over the same views: no second store, no GraphQL-shaped table
//!    and no write path. RFC-0053's own words - a compiler over stored state, not a second indexer.
//!
//! The serving half - every golden-path endpoint answering on a nest with no Graph schema, and the
//! Graph routes answering a named refusal rather than an empty success - lives next to the router it
//! has to bind, as `serve::tests::the_core_serves_with_no_graph_schema_present`.

use std::path::PathBuf;

use clap::CommandFactory;
use nuthatch::cli::Cli;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Production source: the crate's `src/` and the decode crate, minus `#[cfg(test)]` bodies.
///
/// Borrowed in spirit from `payment_absent.rs`, which needed the same distinction for the same reason:
/// a test fixture writing the artefact is fine, and a production site writing it is the thing being
/// measured.
fn production_files() -> Vec<PathBuf> {
    let mut out = Vec::new();
    for dir in ["src", "decode/src"] {
        let mut stack = vec![root().join(dir)];
        while let Some(d) = stack.pop() {
            let Ok(rd) = std::fs::read_dir(&d) else {
                continue;
            };
            for e in rd.flatten() {
                let p = e.path();
                if p.is_dir() {
                    stack.push(p);
                } else if p.extension().is_some_and(|x| x == "rs") {
                    out.push(p);
                }
            }
        }
    }
    out.sort();
    out
}

/// Strip `#[cfg(test)] mod tests { .. }` by brace depth, so a fixture inside it is not production.
fn strip_test_modules(src: &str) -> String {
    let mut out = String::with_capacity(src.len());
    let b = src.as_bytes();
    let mut i = 0usize;
    while i < src.len() {
        if src[i..].starts_with("#[cfg(test)]") {
            // Skip to the module's opening brace, then past its matching close.
            let Some(rel) = src[i..].find('{') else { break };
            let mut j = i + rel;
            let mut depth = 0usize;
            while j < b.len() {
                match b[j] {
                    b'{' => depth += 1,
                    b'}' => {
                        depth -= 1;
                        if depth == 0 {
                            j += 1;
                            break;
                        }
                    }
                    _ => {}
                }
                j += 1;
            }
            i = j;
            continue;
        }
        let ch = src[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

/// 1. `init` writes no Graph artefact.
///
/// Measured on the pinned drop-in target rather than asserted: `init --from-subgraph Qmbi5Bd7…`
/// produces `abis llms.txt nuthatch.toml schema.json semantic.toml views` and no `graph/`. The lane is
/// off on a fresh nest because there is nothing for it to read, which is a stronger guarantee than a
/// default-false flag.
#[test]
fn init_writes_no_graph_artefact() {
    let src = strip_test_modules(&std::fs::read_to_string(root().join("src/project.rs")).unwrap());
    assert!(
        !src.contains("graph/schema.graphql") && !src.contains(r#"join("graph")"#),
        "src/project.rs scaffolds a Graph artefact, so `init` alone would switch the lane on"
    );
}

/// 2. No startup path reads the Graph schema.
///
/// Two production sites: `port-emit` writes it, the handler reads it per request. A third means
/// something is touching it at boot or on a path the golden path shares, and the lane has stopped
/// being optional.
#[test]
fn the_graph_artefact_is_touched_at_exactly_two_production_sites() {
    let mut sites: Vec<(String, usize)> = Vec::new();
    for f in production_files() {
        let src = strip_test_modules(&std::fs::read_to_string(&f).unwrap());
        // `join("graph")` is the filesystem access. A doc comment or an error message naming the path
        // is neither a read nor a write, and counting those made this assertion about prose.
        let n = src.matches(r#"join("graph")"#).count();
        if n > 0 {
            sites.push((f.strip_prefix(root()).unwrap().display().to_string(), n));
        }
    }
    // Two sites, and exactly two: `port-emit` writes it, the handler reads it. A third means something
    // else has come to depend on the artefact, and the lane has stopped being optional.
    assert_eq!(
        sites,
        vec![
            ("src/port_emit.rs".to_string(), 1),
            ("src/serve.rs".to_string(), 1),
        ],
        "the Graph artefact must be written by `port-emit` and read by the handler, and nowhere else"
    );
}

/// 3. The Graph dialect is not configurable.
///
/// Nothing on `dev` or `serve` turns it on or off. Root `global = true` args are accepted on every
/// subcommand and are not in `sub.get_arguments()`, so the root is checked too - the mistake
/// `payment_absent.rs` documents.
#[test]
fn dev_and_serve_take_no_graph_flag() {
    fn assert_not_graph(where_: &str, arg: &clap::Arg) {
        let id = arg.get_id().as_str().to_ascii_lowercase();
        let long = arg.get_long().unwrap_or_default().to_ascii_lowercase();
        for token in ["graphql", "subgraph-compat", "graph-compat", "compat"] {
            assert!(
                !id.contains(token) && !long.contains(token),
                "{where_} takes `--{long}`: the lane is gated by the artefact, not by a flag"
            );
        }
    }
    let cmd = Cli::command();
    for arg in cmd.get_arguments() {
        assert_not_graph("nuthatch (global)", arg);
    }
    for name in ["dev", "serve"] {
        let sub = cmd
            .find_subcommand(name)
            .unwrap_or_else(|| panic!("{name} subcommand missing"));
        for arg in sub.get_arguments() {
            assert_not_graph(&format!("nuthatch {name}"), arg);
        }
    }
}

/// 4. One set of data underneath, and the lane only reads it.
///
/// The handler compiles the dialect to SQL and hands it to `run_sql_query_at` - the same helper
/// `/sql` reaches through `run_sql_query`, with the same admission and read-only attach.
/// A second store, a GraphQL-shaped table
/// or any write would all show up as the lane no longer going through that one door.
#[test]
fn the_graph_lane_reads_through_the_same_sql_path() {
    let src = strip_test_modules(&std::fs::read_to_string(root().join("src/serve.rs")).unwrap());
    let handler = src
        .split_once("async fn graph_rows(")
        .expect("graph_rows must exist")
        .1;
    let body = &handler[..handler.find("\n}\n").unwrap_or(handler.len())];
    assert!(
        body.contains("run_sql_query_at("),
        "the Graph lane must read through `run_sql_query_at`, the same path `/sql` takes, so it inherits \
         RFC-0034 admission and the read-only attach rather than opening a second door.\n\nIf you moved \
         the call behind a helper, that is fine: point this assertion at the helper and satisfy yourself \
         the lane still goes through admission. If you replaced it, do not - one set of data underneath \
         is the property this gate exists for.\n\n{body}"
    );
    // `/sql`'s own lane, JSON or Arrow (RFC-0079), reaches the same shared function.
    let lane = src.split_once("async fn sql_answer(").unwrap().1;
    let lane = &lane[..lane.find("\n}\n").unwrap()];
    assert!(lane.contains("run_sql_query_at(s, q.q, q.max_rows, None, None, arrow).await"));
    let shared = src.split_once("async fn run_sql_query_at(").unwrap().1;
    let shared = &shared[..shared.find("\n}\n").unwrap()];
    assert!(
        shared.contains("admit(&s)"),
        "both lanes must retain SQL admission"
    );
    // Admission is one helper since RFC-0073 FR2 put tenant shares in front of the gate.
    let admission = src.split_once("async fn gate_admit(").unwrap().1;
    let admission = &admission[..admission.find("\n}\n").unwrap()];
    assert!(
        admission.contains("s.sql_gate"),
        "admission must go through the cursor's gate"
    );
    assert!(
        admission.contains("SQL_ADMISSION_WAIT"),
        "admission must remain bounded"
    );
    assert!(
        shared.contains("limits.timeout"),
        "execution must remain bounded"
    );
    for forbidden in [
        "begin_write",
        "put_entity",
        "set_meta",
        "INSERT ",
        "UPDATE ",
        "DELETE ",
    ] {
        assert!(
            !body.contains(forbidden),
            "the Graph lane must not write: found `{forbidden}`"
        );
    }

    // And the serving half is wired through the real router rather than asserted here. Read from the
    // **raw** source, not the stripped copy: the probe lives inside `#[cfg(test)] mod tests`, so
    // stripping test modules removes the very thing being looked for. It did, and this assertion failed
    // while the probe was sitting in the file.
    let raw = std::fs::read_to_string(root().join("src/serve.rs")).unwrap();
    assert!(
        raw.contains("fn the_core_serves_with_no_graph_schema_present"),
        "the router probe is missing from serve.rs - a tree gate with no serving probe proves the \
         source shape and not the behaviour"
    );
}

/// The golden path's own surface is untouched by any of this.
///
/// Named explicitly so deleting one is a red test rather than a quiet narrowing. These are the routes
/// RFC-0015's two-minute demo walks.
#[test]
fn the_golden_path_routes_are_all_still_mounted() {
    let src = std::fs::read_to_string(root().join("src/serve.rs")).unwrap();
    for route in [
        r#".route("/", get(summary))"#,
        r#".route("/health", get("#,
        r#".route("/ready", get(ready))"#,
        r#".route("/tables", get(tables))"#,
        r#".route("/schema", get(schema_doc))"#,
        r#".route("/entities", get(entities))"#,
        r#".route("/entity/{id}", get(entity))"#,
        r#".route("/sql", get(sql_get)"#,
    ] {
        assert!(
            src.contains(route),
            "the golden path lost `{route}` - the core is not pristine if it shrank"
        );
    }
}

/// RFC-0045 stage 2 (#1436): a price pull runs only as its own host-scheduled command. A call from
/// the indexer, the seal loop or a query handler would put a provider's outage or latency on the
/// chain cursor or a caller's request, which is the inline shape RFC-0037 is still paying for.
#[test]
fn an_offchain_pull_runs_only_from_its_own_command() {
    let mut callers = Vec::new();
    for path in production_files() {
        let src = strip_test_modules(&std::fs::read_to_string(&path).unwrap());
        let calls = src.matches("pull_json(").count();
        let definitions = src.matches("fn pull_json(").count();
        if calls > definitions {
            callers.push(path.strip_prefix(root()).unwrap().display().to_string());
        }
    }
    assert_eq!(
        callers,
        ["src/main.rs"],
        "offchain::pull_json must be reached only from the `offchain pull` dispatch"
    );
    let main = strip_test_modules(&std::fs::read_to_string(root().join("src/main.rs")).unwrap());
    assert!(
        main.contains("cli::OffchainWhat::Pull(args)"),
        "the dispatch arm moved"
    );
}
