//! RFC-0016 §1 - the eval harness (Tier A: deterministic, CI-gated, no LLM).
//!
//! This is the measurement spine for the agent-grade MCP work. It builds the fixture nest on the
//! tape infra, seals a deterministic range, then runs every question in `eval/questions.toml`
//! through the *same* hot∪cold SQL surface an agent's `sql` tool hits - asserting each known-correct
//! query returns its declared `expect`. That proves the oracle: the SQL and expected answers are
//! correct against the fixture *before* any agent is ever scored against them (Tier B compares an
//! agent's query result to the same `expect`). If this test is green, the question set is a valid
//! scoreboard; if the surface regresses, this goes red before any agent eval runs.
//!
//! Comparison is order-normalised (multiset) and numeric-tolerant - a DECIMAL `"5500"` equals the
//! number 5500 - exactly the equivalence the RFC specifies for scoring.

mod common;

use std::sync::Arc;
use std::time::Duration;

use nuthatch::{analytics, indexer};
use serde::Deserialize;
use serde_json::Value;

use common::tape::*;

fn guard() -> analytics::QueryGuard {
    analytics::QueryGuard {
        timeout: Duration::from_secs(10),
        max_rows: 100_000,
        ..Default::default()
    }
}

#[derive(Deserialize)]
struct QuestionSet {
    #[serde(default)]
    question: Vec<Question>,
}

#[derive(Deserialize)]
struct Question {
    id: String,
    class: String,
    sql: String,
    /// A compact JSON array of the expected result rows.
    expect: String,
    // The natural-language ask - the Tier-B input; unused by the deterministic gate but part of the
    // committed spec, so keep it in the struct (and off the dead-code warning).
    #[allow(dead_code)]
    question: String,
}

/// Build the deterministic fixture nest: `usdc__transfer` over 10 blocks -
///   blocks 1..5 : a1 → a2, value 100*b     blocks 6..10: a2 → a3, value 100*b
/// with 1..7 sealed and 8..10 hot. Returns the running nest handle so the caller can query and abort.
async fn build_fixture(dir: &std::path::Path) -> indexer::NestRuntime {
    let cfg = scaffold_nest(dir, "usdc", USDC);
    // The tape scaffolder writes no schema.json; real `init` does, and it's what drives the typed
    // view - the reserved-word columns (`from`/`to`) and the derived big-int helper (`value_dec`).
    // Generate it here so the fixture's SQL surface matches a real nest exactly.
    write_fixture_schema(dir, &cfg);
    let tape = Arc::new(TapeSource::new());
    let (a1, a2, a3) = (account(1), account(2), account(3));

    for b in 1..=5u64 {
        tape.insert_block(
            b,
            transfers_block(
                b,
                0,
                1_700_000_000 + b,
                USDC,
                &[(a1.as_str(), a2.as_str(), (100 * b) as u128)],
            ),
        );
    }
    for b in 6..=10u64 {
        tape.insert_block(
            b,
            transfers_block(
                b,
                0,
                1_700_000_000 + b,
                USDC,
                &[(a2.as_str(), a3.as_str(), (100 * b) as u128)],
            ),
        );
    }
    tape.advance_tip_to(10);

    // Small getLogs window so sealing has a boundary short of the full range.
    let rt = indexer::spawn_nest(
        tape.clone(),
        dir.to_path_buf(),
        cfg,
        None,
        false,
        1,
        Some(2),
        false,
        None,
    )
    .await
    .expect("spawn_nest");
    let store = rt.state.store.clone();

    let landed = wait_until(POLL_TIMEOUT, || {
        store.get_meta("last_block").ok().flatten().as_deref() == Some("10")
    })
    .await;
    assert!(landed, "fixture did not index to the tip");

    // Finality advances, but ten rows is below SEAL_DIRECT_BATCH so the tip path holds
    // them in the hot store (#1067). The oracle queries hot∪cold, so the answers are
    // the same ten transfers either way; padding to force a seal would change COUNT(*)
    // and MIN(value) and invalidate questions.toml.
    tape.advance_finalized_to(7);
    tape.insert_block(11, empty_block(11, 0, 1_700_000_100));
    tape.advance_tip_to(11);
    let caught = wait_until(POLL_TIMEOUT, || {
        store.get_meta("last_block").ok().flatten().as_deref() == Some("11")
    })
    .await;
    assert!(caught, "fixture did not index the post-finality block");

    rt
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_question_oracle_is_correct_against_the_fixture() {
    let dir = tempfile::tempdir().unwrap();
    let rt = build_fixture(dir.path()).await;
    let store = rt.state.store.clone();

    let set: QuestionSet = toml::from_str(
        &std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/eval/questions.toml"))
            .expect("read eval/questions.toml"),
    )
    .expect("parse questions.toml");
    assert!(
        set.question.len() >= 15,
        "expected the full pinned question set, got {}",
        set.question.len()
    );

    let hot = store.hot_rows_by_table().unwrap();
    let sealed_through = store.sealed_through();

    let mut failures = Vec::new();
    for q in &set.question {
        let expected: Vec<Value> = serde_json::from_str::<Value>(&q.expect)
            .ok()
            .and_then(|v| v.as_array().cloned())
            .unwrap_or_else(|| panic!("question '{}' has a non-array expect", q.id));

        let out =
            match analytics::query_hot_cold(dir.path(), &q.sql, guard(), &hot, sealed_through, &[])
            {
                Ok(o) => o,
                Err(e) => {
                    failures.push(format!("[{}] query errored: {e:#}", q.id));
                    continue;
                }
            };
        if !results_equal(&expected, &out.rows) {
            failures.push(format!(
                "[{}] ({}) mismatch\n    sql:      {}\n    expected: {}\n    actual:   {}",
                q.id,
                q.class,
                q.sql,
                Value::Array(expected),
                Value::Array(out.rows),
            ));
        }
    }

    rt.ingest.abort();
    if let Some(w) = rt.alert_worker {
        w.abort();
    }

    assert!(
        failures.is_empty(),
        "eval oracle failures ({}/{}):\n{}",
        failures.len(),
        set.question.len(),
        failures.join("\n")
    );
}

/// Write `schema.json` the way `init`/`add` do, so the fixture's analytics views are typed (derived
/// `*_dec` columns, honest reserved-word columns) rather than the schema-less skeleton.
fn write_fixture_schema(dir: &std::path::Path, cfg: &nuthatch::config::Config) {
    let registry = nuthatch::registry::from_nest(dir, cfg).expect("build registry");
    let schema = registry.schema();
    std::fs::write(
        dir.join("schema.json"),
        serde_json::to_string_pretty(&serde_json::json!({
            "registry_hash": format!("0x{}", hex::encode(registry.hash())),
            "tables": &schema,
        }))
        .unwrap(),
    )
    .expect("write schema.json");
}

/// Multiset equality: every expected row must match a distinct actual row (order-independent), and
/// the counts must be equal. An expected row matches an actual row when every expected field is
/// present and numeric-tolerantly equal (extra actual columns are ignored).
fn results_equal(expected: &[Value], actual: &[Value]) -> bool {
    if expected.len() != actual.len() {
        return false;
    }
    let mut used = vec![false; actual.len()];
    for exp in expected {
        let Some(pos) = actual
            .iter()
            .enumerate()
            .position(|(i, act)| !used[i] && row_matches(exp, act))
        else {
            return false;
        };
        used[pos] = true;
    }
    true
}

fn row_matches(expected: &Value, actual: &Value) -> bool {
    let (Some(e), Some(a)) = (expected.as_object(), actual.as_object()) else {
        return values_equal(expected, actual);
    };
    e.iter()
        .all(|(k, ev)| a.get(k).is_some_and(|av| values_equal(ev, av)))
}

/// Numeric-tolerant scalar equality: if both sides look numeric (a JSON number, or a string that
/// parses as one - DECIMAL columns come back as numeric strings), compare as f64; else compare their
/// string forms. Addresses and hashes ("0x…") never parse as f64, so they fall to exact string match.
fn values_equal(a: &Value, b: &Value) -> bool {
    match (as_f64(a), as_f64(b)) {
        (Some(x), Some(y)) => (x - y).abs() < 1e-9,
        _ => scalar_string(a) == scalar_string(b),
    }
}

fn as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        // "0x…" hex parses via f64::from_str? No - it returns Err, so addresses stay string-compared.
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn scalar_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

// -------------------------------------------------------------------------------------------
// Tier B enablement (#815).
//
// Tier B needs a real agent talking to a real MCP server, and the fixture only ever existed inside
// a `tempfile::tempdir()` that vanished when this test returned. `build_fixture` was already
// parameterised by directory, so the missing piece was small: materialise it somewhere durable so
// `nuthatch serve --dir <fixture>` can serve it and `nuthatch mcp` can bridge to it.
//
// `serve` rather than `dev` is deliberate. The fixture is a sealed, finished nest with no chain
// behind it - exactly the cursorless role, which until #1025 reported `ready:false` forever and
// would have made the fixture unusable behind anything that gates on `/ready`.
//
// **Not a scoring harness.** This writes the board; who plays and how the score is recorded is
// `eval/README.md`'s runbook. Deliberately `#[ignore]`d: it writes outside the target directory.
// -------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "writes a durable fixture for a Tier B run; set EVAL_FIXTURE_DIR"]
async fn materialise_the_eval_fixture() {
    let dir = std::env::var("EVAL_FIXTURE_DIR").unwrap_or_else(|_| {
        panic!(
            "set EVAL_FIXTURE_DIR to the directory the fixture should be written to, e.g.\n\
             \n\
             \x20  EVAL_FIXTURE_DIR=/tmp/nuthatch-eval-nest \\\n\
             \x20  cargo test --test eval_harness materialise_the_eval_fixture -- --ignored --nocapture\n"
        )
    });
    let dir = std::path::PathBuf::from(dir);
    assert!(
        !dir.exists() || std::fs::read_dir(&dir).map(|mut d| d.next().is_none()).unwrap_or(false),
        "{} already exists and is not empty. Refusing to build a fixture over it: a half-overwritten \
         nest would score an agent against a board nobody can reproduce.",
        dir.display()
    );
    std::fs::create_dir_all(&dir).expect("create fixture dir");

    let rt = build_fixture(&dir).await;
    let store = rt.state.store.clone();
    let sealed_through = store.sealed_through();

    // Prove the board before handing it to a player: every oracle must still answer correctly here,
    // exactly as Tier A asserts against the ephemeral copy. A fixture that is subtly different from
    // the one the oracle was proved against is worse than no fixture.
    let set: QuestionSet = toml::from_str(
        &std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/eval/questions.toml"))
            .expect("read eval/questions.toml"),
    )
    .expect("parse questions.toml");
    // **`results_equal`, not a row count** (review of #815). The first version compared only
    // `rows.len()`, so a fixture answering `[11]` where the oracle says `[10]` would pass and still
    // print a usable fixture path and question-set hash - certifying a wrong board while claiming to
    // prove it. That is the failure the comment above claims to prevent, committed in the code that
    // claims to prevent it.
    //
    // This is the same order-normalised, numeric-tolerant comparison Tier A scores with, reused
    // rather than reimplemented: a second comparison would be a second opinion about what "correct"
    // means, and the two could drift.
    let hot = store.hot_rows_by_table().unwrap();
    let mut failures = Vec::new();
    for q in &set.question {
        let expected: Vec<Value> = serde_json::from_str::<Value>(&q.expect)
            .ok()
            .and_then(|v| v.as_array().cloned())
            .unwrap_or_else(|| panic!("question '{}' has a non-array expect", q.id));
        match analytics::query_hot_cold(&dir, &q.sql, guard(), &hot, sealed_through, &[]) {
            Err(e) => failures.push(format!("[{}] query errored: {e:#}", q.id)),
            Ok(out) => {
                if !results_equal(&expected, &out.rows) {
                    failures.push(format!(
                        "[{}] ({}) mismatch\n    sql:      {}\n    expected: {}\n    actual:   {}",
                        q.id,
                        q.class,
                        q.sql,
                        Value::Array(expected),
                        Value::Array(out.rows),
                    ));
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "the durable fixture does not answer the oracle it was built from ({}/{} questions). \
         Refusing to print a fixture path or a question-set hash for a board that would score an \
         agent against the wrong answers:\n{}",
        failures.len(),
        set.question.len(),
        failures.join("\n")
    );

    // The runner needs the exact question-set hash for `eval-report.json`.
    // sha256, matching how every other content hash in this repo is spelled.
    let raw = std::fs::read(concat!(env!("CARGO_MANIFEST_DIR"), "/eval/questions.toml")).unwrap();
    let hash = {
        use sha2::{Digest, Sha256};
        let mut h = Sha256::new();
        h.update(&raw);
        format!("{:x}", h.finalize())
    };

    let ingest = rt.ingest;
    ingest.abort();
    let _ = ingest.await;

    println!("\nfixture written to      {}", dir.display());
    println!("questions               {}", set.question.len());
    println!("sealed_through          {sealed_through}");
    println!("question_set_hash       {hash}");
    println!(
        "\nserve it with:\n  nuthatch serve --dir {} --listen 127.0.0.1:8299\n",
        dir.display()
    );
}
