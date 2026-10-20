//! RFC-0016's Tier-B MCP evaluation, run without leaking its oracle to the subject.
//!
//! The runner may read questions.toml. Each subject is a new, restricted Claude process rooted in a
//! freshly-created directory, configured with only nuthatch's MCP server. Its prompt is assembled
//! from a generic instruction and the question field, never sql, expect, class, fixture details, or
//! repository source.

use crate::pyjson::{self, float, Style};
use crate::{median, parse_py_float, py_list_repr, quote_plus, root, Checks};
use clap::Parser;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::time::Duration;

fn die(message: impl std::fmt::Display) -> String {
    format!("tier-b runner: {message}")
}

// A direct port of tests/eval_harness.rs:results_equal, row_matches and values_equal. Keep the
// equivalence in lock-step with Tier A: a score cannot acquire a second definition of correctness
// merely because it is keyed.
fn scalar_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => pyjson::dumps(other, Style::Compact),
    }
}

fn number(value: &Value) -> Option<f64> {
    match value {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => parse_py_float(s),
        _ => None,
    }
}

fn values_equal(left: &Value, right: &Value) -> bool {
    if let (Some(l), Some(r)) = (number(left), number(right)) {
        return (l - r).abs() < 1e-9;
    }
    scalar_string(left) == scalar_string(right)
}

fn row_matches(expected: &Value, actual: &Value) -> bool {
    match (expected, actual) {
        (Value::Object(e), Value::Object(a)) => e
            .iter()
            .all(|(k, v)| a.get(k).is_some_and(|got| values_equal(v, got))),
        _ => values_equal(expected, actual),
    }
}

pub fn results_equal(expected: &[Value], actual: &[Value]) -> bool {
    if expected.len() != actual.len() {
        return false;
    }
    let mut used = vec![false; actual.len()];
    for row in expected {
        match (0..actual.len()).find(|&i| !used[i] && row_matches(row, &actual[i])) {
            Some(i) => used[i] = true,
            None => return false,
        }
    }
    true
}

/// Why `/sql` gave no rows. The two look alike and are opposites (#1051).
#[derive(Debug)]
pub enum SqlError {
    /// The scorer could not obtain a verdict. Fatal to the run: a zero must be earned, and an
    /// unreachable nest used to publish a schema-valid 0/15 with nothing to say why.
    Unavailable(String),
    /// The server answered, and the answer is that the subject's SQL is wrong. An ordinary failed
    /// verdict, and the most diagnostic one there is.
    Rejected(String),
}

#[derive(Debug)]
pub enum RunError {
    Unavailable(String),
    Subject(String),
}

impl std::fmt::Display for RunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunError::Unavailable(m) => write!(f, "ScoringUnavailable: {m}"),
            RunError::Subject(m) => write!(f, "RuntimeError: {m}"),
        }
    }
}

fn http_get(url: &str) -> Result<String, String> {
    let agent = ureq::AgentBuilder::new()
        .timeout(Duration::from_secs(35))
        .build();
    agent
        .get(url)
        .call()
        .map_err(|e| e.to_string())?
        .into_string()
        .map_err(|e| e.to_string())
}

pub fn sql_rows(url: &str, query: &str, limit: usize) -> Result<Vec<Value>, SqlError> {
    let full = format!(
        "{url}/sql?q={}&max_rows={}",
        quote_plus(query),
        quote_plus(&limit.to_string())
    );
    parse_sql_response(http_get(&full))
}

/// Every response that is not the shape asked for is the scorer failing to obtain a verdict, and
/// they all leave by the same door: `{"rows": null}` used to read as an ordinary wrong answer.
pub fn parse_sql_response(body: Result<String, String>) -> Result<Vec<Value>, SqlError> {
    let body = body.map_err(SqlError::Unavailable)?;
    let payload: Value = serde_json::from_str(&body)
        .map_err(|e| SqlError::Unavailable(format!("JSONDecodeError: {e}")))?;
    let Value::Object(map) = payload else {
        return Err(SqlError::Unavailable(format!(
            "response is {}, not an object",
            py_type(&payload)
        )));
    };
    if let Some(detail) = map.get("error") {
        // `{"error": null}` is a response we do not understand, not a server saying the query is bad.
        return match detail {
            Value::String(s) if !s.trim().is_empty() => Err(SqlError::Rejected(s.clone())),
            other => Err(SqlError::Unavailable(format!(
                "malformed error payload: {}",
                pyjson::dumps(other, Style::Default)
            ))),
        };
    }
    let Some(rows) = map.get("rows") else {
        let keys: Vec<String> = {
            let mut k: Vec<String> = map.keys().cloned().collect();
            k.sort();
            k
        };
        return Err(SqlError::Unavailable(format!(
            "response has no 'rows' key: {}",
            py_list_repr(&keys)
        )));
    };
    let Value::Array(rows) = rows else {
        return Err(SqlError::Unavailable(format!(
            "'rows' is {}, not an array",
            py_type(rows)
        )));
    };
    if !rows.iter().all(Value::is_object) {
        return Err(SqlError::Unavailable(
            "'rows' contains an entry that is not an object".into(),
        ));
    }
    Ok(rows.clone())
}

fn py_type(v: &Value) -> &'static str {
    match v {
        Value::Null => "NoneType",
        Value::Bool(_) => "bool",
        Value::Number(n) if n.is_f64() => "float",
        Value::Number(_) => "int",
        Value::String(_) => "str",
        Value::Array(_) => "list",
        Value::Object(_) => "dict",
    }
}

#[derive(Parser, Debug, Clone)]
#[command(
    about = "Run RFC-0016's Tier-B MCP evaluation without leaking its oracle to the subject."
)]
pub struct Args {
    /// nuthatch serve URL
    #[arg(long, required_unless_present = "self_test")]
    pub url: Option<String>,
    #[arg(long, default_value_os_t = root().join("target/debug/nuthatch"))]
    pub nuthatch: PathBuf,
    #[arg(long, default_value = "claude")]
    pub claude: String,
    /// Claude model selector
    #[arg(long, default_value = "sonnet")]
    pub model: String,
    /// provider default used by Claude Code; the CLI has no sampling override
    #[arg(long, default_value_t = 1.0)]
    pub temperature: f64,
    #[arg(long, default_value_t = 3)]
    pub runs: i64,
    #[arg(long, default_value_t = 1.0)]
    pub max_budget_usd: f64,
    #[arg(long, default_value_t = 120)]
    pub timeout: u64,
    /// independent subject processes to run concurrently
    #[arg(long, default_value_t = 1)]
    pub workers: usize,
    #[arg(long, default_value_os_t = root().join("eval/eval-report.json"))]
    pub report: PathBuf,
    /// prove the scorer's honesty with no key, no nest and no model
    #[arg(long)]
    pub self_test: bool,
}

pub struct SubjectRun {
    pub model: Option<String>,
    pub tool_calls: usize,
    pub queries: Vec<String>,
}

pub fn subject_run(question: &str, args: &Args) -> Result<SubjectRun, String> {
    let config = pyjson::dumps(
        &json!({"mcpServers": {"nuthatch": {
            "command": args.nuthatch.to_string_lossy(),
            "args": ["mcp", "--url", args.url.as_deref().unwrap_or_default()],
        }}}),
        Style::Default,
    );
    let prompt = format!(
        "Use the nuthatch MCP tools to answer this question. Use the sql tool to obtain the data. \
         Give the result only after you have queried it.\n\n{question}"
    );
    // This directory is intentionally neither the repository nor the fixture. --restricted removes
    // built-in code/command tools; --tools '' keeps it so.
    let cwd = tempfile::Builder::new()
        .prefix("nuthatch-eval-subject-")
        .tempdir()
        .map_err(|e| e.to_string())?;
    let command: Vec<String> = [
        args.claude.as_str(),
        "-p",
        "--model",
        &args.model,
        "--restricted",
        "--strict-mcp-config",
        "--tools",
        "",
        "--allowed-tools",
        "mcp__nuthatch__*",
        "--no-session-persistence",
        "--output-format",
        "stream-json",
        "--verbose",
        "--max-budget-usd",
        &pyjson::float_repr(args.max_budget_usd),
        "--system-prompt",
        "Use only the supplied MCP tools to answer the user's question.",
        "--mcp-config",
        &config,
        "--",
        &prompt,
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let completed = crate::proc::run(
        &command,
        Some(cwd.path()),
        Duration::from_secs(args.timeout),
    )?;
    if !completed.status.success() {
        let err = completed.stderr.trim();
        return Err(if err.is_empty() {
            "subject process failed".into()
        } else {
            err.into()
        });
    }
    Ok(parse_stream(&completed.stdout))
}

pub fn parse_stream(stdout: &str) -> SubjectRun {
    let (mut tool_calls, mut queries, mut model) = (0, Vec::new(), None);
    for line in stdout.lines() {
        let Ok(Value::Object(event)) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if event.get("type").and_then(Value::as_str) == Some("system")
            && event.get("subtype").and_then(Value::as_str) == Some("init")
        {
            model = event.get("model").and_then(Value::as_str).map(String::from);
        }
        let contents = event
            .get("message")
            .and_then(|m| m.get("content"))
            .and_then(Value::as_array);
        for content in contents.into_iter().flatten() {
            if content.get("type").and_then(Value::as_str) != Some("tool_use") {
                continue;
            }
            tool_calls += 1;
            if content.get("name").and_then(Value::as_str) == Some("mcp__nuthatch__sql") {
                if let Some(q) = content
                    .get("input")
                    .and_then(|i| i.get("query"))
                    .and_then(Value::as_str)
                {
                    queries.push(q.to_string());
                }
            }
        }
    }
    SubjectRun {
        model,
        tool_calls,
        queries,
    }
}

pub struct Question {
    pub id: String,
    pub class: String,
    pub question: String,
    pub expect: String,
}

/// Run one isolated subject and score only its final SQL invocation.
pub fn evaluate_question(
    question: &Question,
    subject: impl FnOnce(&str) -> Result<SubjectRun, String>,
    sql: impl Fn(&str) -> Result<Vec<Value>, SqlError>,
) -> Result<(Option<String>, Value), RunError> {
    let run = subject(&question.question).map_err(RunError::Subject)?;
    let (mut final_rows, mut query_error) = (Vec::new(), None);
    if let Some(last) = run.queries.last() {
        // `Unavailable` is deliberately not caught: a scorer that cannot reach the nest has not
        // discovered that the agent is wrong, it has discovered that it cannot tell.
        match sql(last) {
            Ok(rows) => final_rows = rows,
            Err(SqlError::Rejected(why)) => query_error = Some(why),
            Err(SqlError::Unavailable(why)) => return Err(RunError::Unavailable(why)),
        }
    }
    let expected: Value = serde_json::from_str(&question.expect)
        .map_err(|e| RunError::Subject(format!("question {} expect: {e}", question.id)))?;
    let expected = expected.as_array().cloned().unwrap_or_default();
    let passed =
        !run.queries.is_empty() && query_error.is_none() && results_equal(&expected, &final_rows);
    let attempts = run.queries.len();
    Ok((
        run.model,
        json!({
            "id": question.id, "class": question.class, "passed": passed,
            "first_try": passed && attempts == 1,
            "sql_attempts": attempts, "tool_calls": run.tool_calls,
            // Why a zero is a zero: an invented table, the `value`/`value_dec` footgun, or the
            // `"from"`/`"to"` reserved words. Unrecoverable once the transcripts are gone.
            "final_query": run.queries.last(),
            "final_rows": if passed { Value::Null } else { Value::Array(final_rows) },
            "query_error": query_error,
        }),
    ))
}

pub fn validate_report(report: &Value) -> Result<(), String> {
    let keys = |v: &Value| -> Vec<String> {
        let mut k: Vec<String> = v
            .as_object()
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default();
        k.sort();
        k
    };
    let sorted = |names: &[&str]| -> Vec<String> {
        let mut k: Vec<String> = names.iter().map(|s| s.to_string()).collect();
        k.sort();
        k
    };
    let required = [
        "date",
        "commit",
        "model",
        "temperature",
        "question_set_hash",
        "runs",
        "summary",
        "results",
    ];
    if keys(report) != sorted(&required) {
        return Err(die("generated report does not match its top-level schema"));
    }
    if report["runs"].as_f64().is_none_or(|r| r < 3.0) {
        return Err(die("refusing to publish fewer than three runs"));
    }
    let summary = &report["summary"];
    for key in [
        "first_try_pass_rate",
        "overall_pass_rate",
        "mean_sql_attempts",
    ] {
        if !summary.get(key).is_some_and(Value::is_number) {
            return Err(die(format!("generated report has invalid summary.{key}")));
        }
    }
    let result_keys = sorted(&[
        "id",
        "class",
        "passed",
        "first_try",
        "sql_attempts",
        "tool_calls",
        "final_query",
        "final_rows",
        "query_error",
    ]);
    for result in report["results"].as_array().into_iter().flatten() {
        if keys(result) != result_keys {
            return Err(die("generated result does not match its schema"));
        }
    }
    Ok(())
}

fn load_questions() -> Result<Vec<Question>, String> {
    let path = root().join("eval/questions.toml");
    let raw =
        std::fs::read_to_string(&path).map_err(|e| die(format!("{}: {e}", path.display())))?;
    let doc: toml::Value = toml::from_str(&raw).map_err(die)?;
    let field = |q: &toml::Value, k: &str| -> Result<String, String> {
        q.get(k)
            .and_then(toml::Value::as_str)
            .map(String::from)
            .ok_or_else(|| die(format!("question is missing {k}")))
    };
    doc.get("question")
        .and_then(toml::Value::as_array)
        .ok_or_else(|| die("questions.toml has no [[question]]"))?
        .iter()
        .map(|q| {
            Ok(Question {
                id: field(q, "id")?,
                class: field(q, "class")?,
                question: field(q, "question")?,
                expect: field(q, "expect")?,
            })
        })
        .collect()
}

pub fn run(args: &Args) -> Result<(), String> {
    if args.runs < 3 {
        return Err(die("a published baseline requires at least three runs"));
    }
    if !args.nuthatch.is_file() {
        return Err(die(format!(
            "MCP binary does not exist: {}",
            args.nuthatch.display()
        )));
    }
    let url = args.url.clone().unwrap_or_default();
    let questions = load_questions()?;
    let raw = std::fs::read(root().join("eval/questions.toml")).map_err(|e| e.to_string())?;
    let question_hash: String = Sha256::digest(&raw)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let commit = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(root())
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .ok_or("git rev-parse HEAD failed")?;

    let mut all_runs: Vec<Vec<Value>> = Vec::new();
    let mut observed_models = std::collections::BTreeSet::new();
    for n in 0..args.runs {
        println!("run {}/{}", n + 1, args.runs);
        // Processes are separate subjects whether concurrent or not. One is the published default:
        // the MCP SQL surface allows two concurrent queries, and an eval must not manufacture
        // guard rejections by load testing it.
        let next = std::sync::atomic::AtomicUsize::new(0);
        let slots: std::sync::Mutex<Vec<Option<Value>>> =
            std::sync::Mutex::new(vec![None; questions.len()]);
        let models = std::sync::Mutex::new(&mut observed_models);
        let failure: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);
        std::thread::scope(|s| {
            for _ in 0..args.workers.max(1) {
                s.spawn(|| loop {
                    let i = next.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if i >= questions.len() || failure.lock().unwrap().is_some() {
                        return;
                    }
                    let q = &questions[i];
                    match evaluate_question(
                        q,
                        |text| subject_run(text, args),
                        |query| sql_rows(&url, query, 200),
                    ) {
                        Ok((model, outcome)) => {
                            if let Some(m) = model.filter(|m| !m.is_empty()) {
                                models.lock().unwrap().insert(m);
                            }
                            println!(
                                "  {}: {} ({} SQL, {} tools)",
                                q.id,
                                if outcome["passed"] == true {
                                    "pass"
                                } else {
                                    "fail"
                                },
                                outcome["sql_attempts"],
                                outcome["tool_calls"]
                            );
                            slots.lock().unwrap()[i] = Some(outcome);
                        }
                        Err(e) => {
                            failure.lock().unwrap().get_or_insert(e.to_string());
                        }
                    }
                });
            }
        });
        if let Some(e) = failure.into_inner().unwrap() {
            return Err(e);
        }
        all_runs.push(slots.into_inner().unwrap().into_iter().flatten().collect());
    }

    if observed_models.len() != 1 {
        let seen: Vec<String> = observed_models.iter().cloned().collect();
        return Err(die(format!(
            "model selector was not pinned: observed {}",
            py_list_repr(&seen)
        )));
    }
    let flag = |v: &Value| if v == &Value::Bool(true) { 1.0 } else { 0.0 };
    let count = |v: &Value| v.as_f64().unwrap_or(0.0);
    let mut results = Vec::new();
    for (position, question) in questions.iter().enumerate() {
        let samples: Vec<&Value> = all_runs.iter().map(|run| &run[position]).collect();
        let passed = median(
            &samples
                .iter()
                .map(|s| flag(&s["passed"]))
                .collect::<Vec<_>>(),
        ) != 0.0;
        // Diagnostic fields cannot be median'd. Take them from a run that agrees with the median
        // verdict, so a published zero still says which query failed.
        let witness = samples
            .iter()
            .find(|s| s["passed"] == Value::Bool(passed))
            .unwrap_or(&samples[0]);
        results.push(json!({
            "id": question.id, "class": question.class,
            "passed": passed,
            "first_try": median(&samples.iter().map(|s| flag(&s["first_try"])).collect::<Vec<_>>()) != 0.0,
            "sql_attempts": median(&samples.iter().map(|s| count(&s["sql_attempts"])).collect::<Vec<_>>()) as i64,
            "tool_calls": median(&samples.iter().map(|s| count(&s["tool_calls"])).collect::<Vec<_>>()) as i64,
            "final_query": witness["final_query"],
            "final_rows": if passed { Value::Null } else { witness["final_rows"].clone() },
            "query_error": witness["query_error"],
        }));
    }
    let per_run = |f: &dyn Fn(&Value) -> f64| -> Vec<f64> {
        all_runs
            .iter()
            .map(|outcomes| outcomes.iter().map(f).sum::<f64>() / outcomes.len() as f64)
            .collect()
    };
    let first = per_run(&|o| flag(&o["first_try"]));
    let overall = per_run(&|o| flag(&o["passed"]));
    let attempts = per_run(&|o| count(&o["sql_attempts"]));
    let mut classes: Vec<&String> = questions.iter().map(|q| &q.class).collect();
    classes.sort();
    classes.dedup();
    let mut by_class = Map::new();
    for cls in classes {
        let rates: Vec<f64> = all_runs
            .iter()
            .map(|run| {
                let mine: Vec<&Value> = run.iter().filter(|o| o["class"] == **cls).collect();
                mine.iter().map(|o| flag(&o["passed"])).sum::<f64>() / mine.len() as f64
            })
            .collect();
        by_class.insert(cls.clone(), float(median(&rates)));
    }
    let report = json!({
        "date": crate::today(true), "commit": commit,
        "model": observed_models.into_iter().next(), "temperature": float(args.temperature),
        "question_set_hash": question_hash, "runs": args.runs,
        "summary": {
            "first_try_pass_rate": float(median(&first)),
            "overall_pass_rate": float(median(&overall)),
            "mean_sql_attempts": float(median(&attempts)),
            "by_class": by_class,
        },
        "results": results,
    });
    validate_report(&report)?;
    std::fs::write(&args.report, pyjson::dumps(&report, Style::Indent2) + "\n")
        .map_err(|e| e.to_string())?;
    println!("wrote {}", args.report.display());
    Ok(())
}

/// The two properties #1051 is about, proved without a key, a nest, or a model. Both defects it
/// replaced were invisible because nothing exercised them.
pub fn self_test() -> Checks {
    let mut c = Checks::default();

    // An unreachable scorer must raise, not return an empty result set that scores as a failure.
    match sql_rows("http://127.0.0.1:9", "SELECT 1", 200) {
        Ok(_) => c.check(
            "unreachable scorer raises",
            false,
            "returned instead of raising",
        ),
        Err(SqlError::Unavailable(_)) => {
            c.check("unreachable scorer raises ScoringUnavailable", true, "")
        }
        Err(e) => c.check(
            "unreachable scorer raises ScoringUnavailable",
            false,
            &format!("raised {e:?}"),
        ),
    }

    let served = |body: &str| match parse_sql_response(Ok(body.to_string())) {
        Ok(rows) => ("returned", Some(rows)),
        Err(SqlError::Unavailable(_)) => ("fatal", None),
        Err(SqlError::Rejected(_)) => ("QueryRejected", None),
    };
    for (label, body) in [
        ("no rows key", r#"{"unexpected": 1}"#),
        ("rows is null", r#"{"rows": null}"#),
        ("rows is a string", r#"{"rows": "not rows"}"#),
        ("a row is not an object", r#"{"rows": [1, 2]}"#),
        ("body is not an object", "null"),
        ("body is a list", r#"[{"n": 1}]"#),
        ("error is null", r#"{"error": null}"#),
        ("error is an object", r#"{"error": {}}"#),
        ("error is blank", r#"{"error": "   "}"#),
    ] {
        let (outcome, _) = served(body);
        c.check(
            &format!("shape mismatch raises ({label})"),
            outcome == "fatal",
            &format!("got {outcome}"),
        );
    }

    // ...and a well-formed response must still be returned, so the checks above are not passing
    // because everything raises.
    let (outcome, rows) = served(r#"{"rows": [{"n": 8}]}"#);
    c.check(
        "a well-formed response is returned",
        outcome == "returned" && rows == Some(vec![json!({"n": 8})]),
        &format!("got {outcome} {rows:?}"),
    );

    // ...and a real error string must still be a verdict, or the checks above pass by making
    // everything fatal, which is the opposite over-correction.
    match parse_sql_response(Ok(
        r#"{"error": "Table with name invented does not exist"}"#.into(),
    )) {
        Ok(_) => c.check(
            "a real error string is a rejected query",
            false,
            "returned instead",
        ),
        Err(SqlError::Rejected(_)) => c.check("a real error string is a rejected query", true, ""),
        Err(e) => c.check(
            "a real error string is a rejected query",
            false,
            &format!("{e:?}"),
        ),
    }

    // The regression at the site it lives: catching `Unavailable` around the call in
    // `evaluate_question` would score it as a failed question while every probe above passed.
    let question = Question {
        id: "q".into(),
        class: "c".into(),
        question: "how many?".into(),
        expect: "[]".into(),
    };
    let canned = |queries: &[&str]| {
        let queries: Vec<String> = queries.iter().map(|s| s.to_string()).collect();
        move |_: &str| {
            Ok(SubjectRun {
                model: Some("m".into()),
                tool_calls: 1,
                queries,
            })
        }
    };
    match evaluate_question(&question, canned(&["SELECT 1"]), |q| {
        sql_rows("http://127.0.0.1:9", q, 200)
    }) {
        Ok(_) => c.check(
            "evaluate_question propagates a scoring failure",
            false,
            "scored it instead of raising",
        ),
        Err(RunError::Unavailable(_)) => {
            c.check("evaluate_question propagates a scoring failure", true, "")
        }
        Err(e) => c.check(
            "evaluate_question propagates a scoring failure",
            false,
            &format!("raised {e}"),
        ),
    }

    // The commonest agent mistake, an invented table name, must be a diagnosed failure rather than
    // an aborted run.
    match evaluate_question(&question, canned(&["SELECT * FROM invented"]), |_| {
        Err(SqlError::Rejected(
            "Table with name invented does not exist".into(),
        ))
    }) {
        Ok((_, result)) => c.check(
            "a rejected query is a diagnosed failure, not an aborted run",
            result["passed"] == false
                && result["final_query"] == "SELECT * FROM invented"
                && result["query_error"]
                    .as_str()
                    .is_some_and(|e| e.contains("invented")),
            &result.to_string(),
        ),
        Err(e) => c.check(
            "a rejected query is a diagnosed failure, not an aborted run",
            false,
            &format!("raised {e}"),
        ),
    }

    // ...and a reachable scorer must still produce a verdict.
    let question_ok = Question {
        expect: r#"[{"n": 1}]"#.into(),
        ..question
    };
    match evaluate_question(&question_ok, canned(&["SELECT 1"]), |_| {
        Ok(vec![json!({"n": 1})])
    }) {
        Ok((_, result)) => c.check(
            "a reachable scorer still yields a verdict",
            result["passed"] == true && result["final_query"] == "SELECT 1",
            &result.to_string(),
        ),
        Err(e) => c.check(
            "a reachable scorer still yields a verdict",
            false,
            &e.to_string(),
        ),
    }

    // A report whose results omit the diagnostic fields must be refused, so a future run cannot
    // publish another undiagnosable zero...
    let mut stub = json!({
        "date": "2026-01-01", "commit": "0".repeat(40), "model": "m", "temperature": 1.0,
        "question_set_hash": "0".repeat(64), "runs": 3,
        "summary": {"first_try_pass_rate": 0.0, "overall_pass_rate": 0.0,
                    "mean_sql_attempts": 1.0, "by_class": {}},
        "results": [{"id": "q", "class": "c", "passed": false, "first_try": false,
                     "sql_attempts": 1, "tool_calls": 1}],
    });
    match validate_report(&stub) {
        Ok(()) => c.check(
            "report without final_query is refused",
            false,
            "accepted it",
        ),
        Err(_) => c.check("report without final_query is refused", true, ""),
    }
    // ...and accepted once they are present, so that refusal is not for the wrong reason.
    let r = stub["results"][0].as_object_mut().unwrap();
    r.insert("final_query".into(), json!("SELECT 1"));
    r.insert("final_rows".into(), json!([]));
    r.insert("query_error".into(), Value::Null);
    match validate_report(&stub) {
        Ok(()) => c.check("report with final_query is accepted", true, ""),
        Err(e) => c.check("report with final_query is accepted", false, &e),
    }

    c.finish();
    c
}
