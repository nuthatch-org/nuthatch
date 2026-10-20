//! #1749 - `scripts/release-gate.sh` must fail a binary that refuses one of the recorded queries,
//! pass one that answers them all, and fail a time regression past its stated bound or an answer
//! that differs from its baseline's (#1772).
//!
//! A tiny nest is sealed once from the fixture chain by the real binary, then each case runs the
//! real script against a copy of it with a query set of its own: the gate serves the copy with
//! `serve`, which takes the redb's exclusive lock, so two cases cannot share one directory.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use nuthatch_fixture_rpc::scripted::Scripted;

const ERC20_TRANSFER_ABI: &str = r#"[{"anonymous":false,"inputs":[
  {"indexed":true,"name":"from","type":"address"},
  {"indexed":true,"name":"to","type":"address"},
  {"indexed":false,"name":"value","type":"uint256"}],"name":"Transfer","type":"event"}]"#;

/// The fixture chain's defaults: blocks 1..=8 carry one Transfer each.
const CONTRACT: &str = "0x000000000000000000000000000000000000c0de";
const TIP: u64 = 9;
const FINALIZED: u64 = 8;
const TRANSFERS: u64 = 8;

struct Reaped(Child);

impl Drop for Reaped {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn get(url: &str) -> Option<String> {
    let out = Command::new("curl")
        .args(["-fsS", "-m", "8", url])
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).into_owned())
}

fn poll<T>(what: &str, secs: u64, mut f: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        if let Some(v) = f() {
            return v;
        }
        assert!(
            Instant::now() < deadline,
            "timed out after {secs}s waiting for {what}"
        );
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// A sealed fixture nest, built once per test binary, and the name of its one table.
fn template() -> &'static (tempfile::TempDir, String) {
    static T: OnceLock<(tempfile::TempDir, String)> = OnceLock::new();
    T.get_or_init(|| {
        let dir = tempfile::tempdir().unwrap();
        let rpc = nuthatch_fixture_rpc::spawn(
            Scripted::new(CONTRACT).pinned(Some(TIP), Some(FINALIZED)),
            "127.0.0.1:0",
        )
        .expect("fixture RPC");
        let rpc_port = rpc.port();

        let nest = dir.path().join("nest");
        let abi = dir.path().join("erc20.abi.json");
        std::fs::write(&abi, ERC20_TRANSFER_ABI).unwrap();
        let init = Command::new(env!("CARGO_BIN_EXE_nuthatch"))
            .args(["init", CONTRACT, "--chain", "arbitrum-one"])
            .args(["--rpc", &format!("http://127.0.0.1:{rpc_port}/")])
            .arg("--abi")
            .arg(&abi)
            .arg("--dir")
            .arg(&nest)
            .output()
            .expect("run init");
        assert!(
            init.status.success(),
            "init failed:\n{}",
            String::from_utf8_lossy(&init.stderr)
        );

        let port = free_port();
        let mut dev = Reaped(
            Command::new(env!("CARGO_BIN_EXE_nuthatch"))
                .args(["dev", "--dir"])
                .arg(&nest)
                .args(["--listen", &format!("127.0.0.1:{port}"), "--seal-direct"])
                // The fixture chain advances while the nest runs; the 5-minute default would sit it out.
                .args(["--poll-interval", "1s"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn dev"),
        );
        let api = format!("http://127.0.0.1:{port}");
        poll("the nest to seal the fixture history", 120, || {
            let body = get(&format!("{api}/sql?q=SELECT%201"))?;
            let v: serde_json::Value = serde_json::from_str(&body).ok()?;
            v["provenance"]["sealed_through"]
                .as_u64()
                .filter(|&n| n >= FINALIZED)
        });
        let tables: serde_json::Value =
            serde_json::from_str(&poll("the tables listing", 30, || {
                get(&format!("{api}/tables"))
            }))
            .unwrap();
        let table = tables["tables"][0]["name"]
            .as_str()
            .or_else(|| tables["tables"][0]["table"].as_str())
            .expect("a table")
            .to_string();
        let _ = Command::new("kill")
            .args(["-TERM", &dev.0.id().to_string()])
            .status();
        let _ = dev.0.wait();
        assert!(
            nest.join("nuthatch.redb").is_file(),
            "dev left no redb to serve"
        );
        (dir, table)
    })
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap().flatten() {
        let path = entry.path();
        let dest = to.join(entry.file_name());
        if path.is_dir() {
            copy_dir(&path, &dest);
        } else {
            std::fs::copy(&path, &dest).unwrap();
        }
    }
}

struct Case {
    dir: tempfile::TempDir,
    nest: PathBuf,
    table: String,
}

fn case() -> Case {
    let (template, table) = template();
    let dir = tempfile::tempdir().unwrap();
    let nest = dir.path().join("nest");
    copy_dir(&template.path().join("nest"), &nest);
    Case {
        dir,
        nest,
        table: table.clone(),
    }
}

impl Case {
    fn set(&self, lines: &[(&str, String)]) -> PathBuf {
        let path = self.dir.path().join("set.tsv");
        let mut body = String::from("# id\tconsumer\tsite\tsql\n");
        for (id, sql) in lines {
            body.push_str(&format!(
                "{id}\ttest\ttests/release_gate_script.rs\t{sql}\n"
            ));
        }
        std::fs::write(&path, body).unwrap();
        path
    }

    fn counts(&self) -> String {
        format!("SELECT count(*) AS n FROM \"{}\"", self.table)
    }

    fn gate(&self, set: &Path, extra: &[&str], env: &[(&str, &str)]) -> (Output, String) {
        let mut cmd = Command::new(root().join("scripts/release-gate.sh"));
        calm(&mut cmd);
        cmd.args(["--passes", "1", "--out"])
            .arg(self.dir.path().join("out"))
            .args(extra)
            .arg(env!("CARGO_BIN_EXE_nuthatch"))
            .arg(&self.nest)
            .arg(set);
        for (k, v) in env {
            cmd.env(k, v);
        }
        let out = cmd.output().expect("run release-gate.sh");
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        (out, text)
    }
}

fn line_for<'a>(text: &'a str, id: &str) -> &'a str {
    text.lines()
        .find(|l| l.split_whitespace().nth(1) == Some(id))
        .unwrap_or_else(|| panic!("no result line for `{id}` in:\n{text}"))
}

#[test]
fn a_query_the_binary_refuses_fails_the_gate_and_is_named() {
    let c = case();
    let set = c.set(&[
        ("answers", c.counts()),
        ("refused", "SELECT a FROM no_such_table".to_string()),
    ]);
    let (out, text) = c.gate(&set, &[], &[]);
    assert_eq!(out.status.code(), Some(1), "{text}");
    assert!(
        line_for(&text, "answers").starts_with("ok "),
        "the answering query must still be reported as answering:\n{text}"
    );
    let refused = line_for(&text, "refused");
    assert!(
        refused.starts_with("FAIL ") && refused.contains("no_such_table"),
        "{refused}"
    );
    assert!(text.contains("RESULT: FAIL - failed: refused"), "{text}");
}

#[test]
fn a_set_the_binary_answers_passes_and_records_a_baseline() {
    let c = case();
    let set = c.set(&[
        ("answers", c.counts()),
        ("history", format!("SELECT * FROM \"{}\"", c.table)),
    ]);
    let baseline = c.dir.path().join("baseline.tsv");
    let (out, text) = c.gate(
        &set,
        &[
            "--passes",
            "2",
            "--write-baseline",
            baseline.to_str().unwrap(),
        ],
        &[],
    );
    assert_eq!(out.status.code(), Some(0), "{text}");
    assert!(text.contains("RESULT: PASS"), "{text}");
    let answers = line_for(&text, "answers");
    assert_eq!(
        answers.split_whitespace().nth(2),
        Some("1"),
        "ROWS is the rows returned, one for a count: {answers}"
    );
    // The copy is served with its sealed history, not empty: every fixture transfer comes back.
    let history = line_for(&text, "history");
    assert_eq!(
        history.split_whitespace().nth(2),
        Some(TRANSFERS.to_string().as_str()),
        "{history}"
    );
    let recorded = std::fs::read_to_string(&baseline).unwrap();
    let row = recorded
        .lines()
        .find(|l| l.starts_with("answers\t"))
        .unwrap_or_else(|| panic!("no baseline row:\n{recorded}"));
    let cols: Vec<&str> = row.split('\t').collect();
    assert_eq!(&cols[1..4], ["test", "ok", "1"], "{row}");
    assert!(cols[4].parse::<i64>().is_ok(), "a median time: {row}");
}

/// The bound decides, not the comparison: the same baseline passes under the default slack and
/// fails with the slack taken away. The baseline time is -1 so any real time is slower than it.
#[test]
fn a_regression_past_the_bound_fails_and_one_inside_it_does_not() {
    let c = case();
    let set = c.set(&[("answers", c.counts())]);
    let baseline = c.dir.path().join("baseline.tsv");
    std::fs::write(
        &baseline,
        "# a hand-made baseline\nanswers\ttest\tok\t1\t-1\t-\t\n",
    )
    .unwrap();
    let base = baseline.to_str().unwrap();

    let (out, text) = c.gate(&set, &["--baseline", base], &[]);
    assert_eq!(
        out.status.code(),
        Some(0),
        "inside the default bound:\n{text}"
    );

    let (out, text) = c.gate(
        &set,
        &["--baseline", base],
        &[("GATE_QUERY_SLACK_MS", "0"), ("GATE_P99_SLACK_MS", "0")],
    );
    assert_eq!(
        out.status.code(),
        Some(1),
        "past a zero-slack bound:\n{text}"
    );
    let answers = line_for(&text, "answers");
    assert!(
        answers.starts_with("SLOW ") && answers.contains("regressed"),
        "{answers}"
    );
    assert!(text.contains("p99") && text.contains("REGRESSED"), "{text}");
}

impl Case {
    /// Defines `gate_probe` over the fixture table and `gate_rows` over literals. `wrong` stands
    /// in for a candidate that answers block 5 as 50, sorts on `s` the other way round, is 1e-13
    /// off on a float, and returns `gate_rows` in the reverse order, as a VALUES list keeps its
    /// order. `gate_numbers` answers 2^53 + 1 as 2^53 and 1 as 1.0.
    fn probe(&self, wrong: bool) {
        let views = self.nest.join("views");
        std::fs::create_dir_all(&views).unwrap();
        let numbers = if wrong {
            "CAST(9007199254740992 AS BIGINT) AS big, CAST(1 AS DOUBLE) AS one"
        } else {
            "CAST(9007199254740993 AS BIGINT) AS big, CAST(1 AS BIGINT) AS one"
        };
        std::fs::write(
            views.join("92-gate-numbers.sql"),
            format!("CREATE VIEW gate_numbers AS SELECT {numbers};\n"),
        )
        .unwrap();
        let (block, s, third, rows) = if wrong {
            (
                "CASE WHEN block_number = 5 THEN 50 ELSE block_number END",
                "100 - CAST(block_number AS BIGINT)",
                "CAST(block_number AS DOUBLE) / 3 + CAST(0.0000000000001 AS DOUBLE)",
                "(3, 'c'), (2, 'b'), (1, 'a')",
            )
        } else {
            (
                "block_number",
                "CAST(block_number AS BIGINT)",
                "CAST(block_number AS DOUBLE) / 3",
                "(1, 'a'), (2, 'b'), (3, 'c')",
            )
        };
        std::fs::write(
            views.join("90-gate-probe.sql"),
            format!(
                "CREATE VIEW gate_probe AS SELECT {block} AS block, {s} AS s, {third} AS third \
                 FROM \"{}\";\n",
                self.table
            ),
        )
        .unwrap();
        std::fs::write(
            views.join("91-gate-rows.sql"),
            format!("CREATE VIEW gate_rows AS SELECT * FROM (VALUES {rows}) AS v(k, label);\n"),
        )
        .unwrap();
    }

    /// Writes a baseline from the right answers, then gates the wrong ones against it, both runs
    /// with `extra`.
    fn against_a_wrong_candidate(&self, set: &Path, extra: &[&str]) -> (Output, String) {
        let baseline = self.dir.path().join("baseline.tsv");
        let base_out = self.dir.path().join("baseline-out");
        self.probe(false);
        let mut args = vec![
            "--out",
            base_out.to_str().unwrap(),
            "--write-baseline",
            baseline.to_str().unwrap(),
        ];
        args.extend_from_slice(extra);
        let (out, text) = self.gate(set, &args, &[]);
        assert_eq!(out.status.code(), Some(0), "the baseline run:\n{text}");
        self.probe(true);
        let mut args = vec!["--baseline", baseline.to_str().unwrap()];
        args.extend_from_slice(extra);
        self.gate(set, &args, &[])
    }
}

/// #1772: a statement that answers, but not what production answered, fails and is named, with
/// the first row at which the two differ. Row order is part of the answer only under a top-level
/// ORDER BY: a statement without one passes with its rows reversed (the quoted ORDER BY is a
/// literal, not a clause), and so does one whose ORDER BY is in a subquery (#1984). A float 1e-13
/// off differs too, as a number or cast to text (#1883).
#[test]
fn an_answer_that_differs_from_the_baseline_fails_with_its_first_differing_row() {
    let c = case();
    let set = c.set(&[
        ("value", "SELECT block FROM gate_probe".to_string()),
        (
            "ordered",
            "SELECT third FROM gate_probe ORDER BY s".to_string(),
        ),
        (
            "unordered",
            "SELECT k, label FROM gate_rows WHERE 'ORDER BY' <> ''".to_string(),
        ),
        (
            "nested",
            "SELECT block FROM (SELECT block, s FROM gate_probe WHERE block NOT IN (5, 50) ORDER BY s)"
                .to_string(),
        ),
        (
            "float",
            "SELECT third, CAST(third * CAST('1e22' AS DOUBLE) AS VARCHAR) AS text FROM gate_probe"
                .to_string(),
        ),
        (
            "volatile",
            "SELECT block FROM gate_probe WHERE block < 10".to_string(),
        ),
    ]);
    // Volatile is held to its row count, and block 5 answered as 50 is a row fewer.
    let body = std::fs::read_to_string(&set).unwrap();
    std::fs::write(&set, format!("# volatile: volatile a row short\n{body}")).unwrap();
    let (out, text) = c.against_a_wrong_candidate(&set, &[]);
    assert_eq!(out.status.code(), Some(1), "{text}");
    let value = line_for(&text, "value");
    assert!(
        value.starts_with("FAIL ") && value.contains("answer differs"),
        "{value}"
    );
    assert!(
        text.contains("first differing row, row 5:")
            && text.contains("candidate: {\"block\":50}")
            && text.contains("baseline:  {\"block\":5}"),
        "the first differing row of each side:\n{text}"
    );
    let ordered = line_for(&text, "ordered");
    assert!(
        ordered.starts_with("FAIL ") && ordered.contains("answer differs (rows compared in order)"),
        "{ordered}"
    );
    assert!(
        line_for(&text, "unordered").starts_with("ok "),
        "only the order differs, and it has no top-level ORDER BY:\n{text}"
    );
    assert!(
        line_for(&text, "nested").starts_with("ok "),
        "only the order differs, and its ORDER BY is not top-level:\n{text}"
    );
    let float = line_for(&text, "float");
    assert!(
        float.starts_with("FAIL ") && float.contains("answer differs"),
        "1e-13 off is a different answer:\n{text}"
    );
    let volatile = line_for(&text, "volatile");
    assert!(
        volatile.starts_with("FAIL ")
            && volatile.contains("answer differs: 7 rows against the baseline's 8"),
        "{volatile}"
    );
    assert!(
        text.contains("RESULT: FAIL - answer differs: value, ordered, float, volatile"),
        "{text}"
    );
}

/// A statement tagged volatile in the set is held to its row count, so a different answer with
/// the same number of rows passes; untagged, it fails (the test above).
#[test]
fn a_volatile_statement_that_answers_differently_passes_on_its_row_count() {
    let c = case();
    let set = c.set(&[
        ("changing", "SELECT block FROM gate_probe".to_string()),
        (
            "unordered",
            "SELECT k, label FROM gate_rows WHERE 'ORDER BY' <> ''".to_string(),
        ),
    ]);
    let body = std::fs::read_to_string(&set).unwrap();
    std::fs::write(
        &set,
        format!("# volatile: changing its answer moves with the clock\n{body}"),
    )
    .unwrap();
    let (out, text) = c.against_a_wrong_candidate(&set, &[]);
    assert_eq!(out.status.code(), Some(0), "{text}");
    let changing = line_for(&text, "changing");
    assert!(
        changing.starts_with("ok ") && changing.contains("volatile, so compared on its row count"),
        "{changing}"
    );
    assert!(line_for(&text, "unordered").starts_with("ok "), "{text}");
    assert!(
        text.contains("1 match, 0 differ, 1 compared on row count only"),
        "{text}"
    );
}

/// #1883: numbers jq could fold together under a lossy number model still differ: an integer past
/// 2^53 one apart, and an integer that comes back as a float of the same value.
#[test]
fn numbers_a_lossy_parse_would_fold_together_still_differ() {
    let c = case();
    let set = c.set(&[
        ("big", "SELECT big FROM gate_numbers".to_string()),
        ("one", "SELECT one FROM gate_numbers".to_string()),
    ]);
    let (out, text) = c.against_a_wrong_candidate(&set, &[]);
    assert_eq!(out.status.code(), Some(1), "{text}");
    for id in ["big", "one"] {
        let line = line_for(&text, id);
        assert!(
            line.starts_with("FAIL ") && line.contains("answer differs"),
            "{line}"
        );
    }
    assert!(
        text.contains("candidate: {\"big\":9007199254740992}")
            && text.contains("baseline:  {\"big\":9007199254740993}")
            && text.contains("candidate: {\"one\":1.0}")
            && text.contains("baseline:  {\"one\":1}"),
        "{text}"
    );
}

/// A jq before 1.7 parses every number to a double, which folds those numbers together, so the
/// gate will not run on one: a setup fault, not a verdict.
#[test]
fn a_jq_that_folds_numbers_is_a_setup_fault() {
    let c = case();
    let bin = c.dir.path().join("old-jq");
    std::fs::create_dir_all(&bin).unwrap();
    let shim = bin.join("jq");
    std::fs::write(
        &shim,
        "#!/bin/sh\n[ \"$1\" = --version ] && { echo jq-1.6; exit 0; }\nexit 99\n",
    )
    .unwrap();
    let mut perms = std::fs::metadata(&shim).unwrap().permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut perms, 0o755);
    std::fs::set_permissions(&shim, perms).unwrap();
    let path = format!("{}:{}", bin.display(), std::env::var("PATH").unwrap());
    let set = c.set(&[("answers", c.counts())]);
    let (out, text) = c.gate(&set, &[], &[("PATH", &path)]);
    assert_eq!(out.status.code(), Some(2), "{text}");
    assert!(text.contains("jq-1.6") && text.contains("1.7"), "{text}");
}

/// #1773: production serves two statements at once. At `--concurrency 2` the set goes out a pair at
/// a time, both statements of a pair in flight together, and each keeps its own row and time.
#[test]
fn at_concurrency_two_a_pair_is_in_flight_together_and_both_are_recorded() {
    let c = case();
    // Slow enough on eight rows that two run one after the other cannot overlap by accident.
    let slow = |salt: u32| {
        let from: Vec<String> = (0..6).map(|i| format!("\"{}\" t{i}", c.table)).collect();
        format!(
            "SELECT count(DISTINCT {}) AS n FROM {}",
            (0..6)
                .map(|i| format!("t{i}.block_number * {}", 10u64.pow(i) + u64::from(salt)))
                .collect::<Vec<_>>()
                .join(" + "),
            from.join(", ")
        )
    };
    let set = c.set(&[("first", slow(0)), ("second", slow(1))]);
    let (out, text) = c.gate(&set, &["--concurrency", "2"], &[]);
    assert_eq!(out.status.code(), Some(0), "{text}");
    assert!(text.contains("concurrency 2"), "{text}");
    for id in ["first", "second"] {
        let line = line_for(&text, id);
        assert!(line.starts_with("ok "), "{line}");
        assert_eq!(line.split_whitespace().nth(2), Some("1"), "{line}");
    }

    // group<TAB>id<TAB>start ms<TAB>end ms, one line per statement sent.
    let schedule = std::fs::read_to_string(c.dir.path().join("out/schedule-pass-1.tsv")).unwrap();
    let rows: Vec<Vec<&str>> = schedule.lines().map(|l| l.split('\t').collect()).collect();
    assert_eq!(rows.len(), 2, "{schedule}");
    assert_eq!(rows[0][0], rows[1][0], "one pair, one group: {schedule}");
    let span = |r: &Vec<&str>| (r[2].parse::<u64>().unwrap(), r[3].parse::<u64>().unwrap());
    let ((s0, e0), (s1, e1)) = (span(&rows[0]), span(&rows[1]));
    assert!(
        s0 < e1 && s1 < e0,
        "the pair ran one after the other, not together: {schedule}"
    );
}

/// At `--concurrency 2` each statement's answer is still captured and compared as it is one at a
/// time. Each pair holds one statement that differs and one that matches, so an answer recorded
/// against its partner's id fails on both.
#[test]
fn at_concurrency_two_an_answer_that_differs_still_fails_and_is_named() {
    let c = case();
    let set = c.set(&[
        (
            "unordered",
            "SELECT k, label FROM gate_rows WHERE 'ORDER BY' <> ''".to_string(),
        ),
        ("value", "SELECT block FROM gate_probe".to_string()),
        (
            "ordered",
            "SELECT third FROM gate_probe ORDER BY s".to_string(),
        ),
        ("by_key", "SELECT k FROM gate_rows ORDER BY k".to_string()),
    ]);
    let (out, text) = c.against_a_wrong_candidate(&set, &["--concurrency", "2"]);
    assert_eq!(out.status.code(), Some(1), "{text}");
    assert!(text.contains("concurrency 2"), "{text}");
    let value = line_for(&text, "value");
    assert!(
        value.starts_with("FAIL ") && value.contains("answer differs"),
        "{value}"
    );
    assert!(
        text.contains("first differing row, row 5:")
            && text.contains("candidate: {\"block\":50}")
            && text.contains("baseline:  {\"block\":5}"),
        "the first differing row of each side:\n{text}"
    );
    let ordered = line_for(&text, "ordered");
    assert!(
        ordered.starts_with("FAIL ") && ordered.contains("answer differs (rows compared in order)"),
        "{ordered}"
    );
    assert!(line_for(&text, "unordered").starts_with("ok "), "{text}");
    assert!(line_for(&text, "by_key").starts_with("ok "), "{text}");
    assert!(
        text.contains("2 match, 2 differ")
            && text.contains("RESULT: FAIL - answer differs: value, ordered"),
        "{text}"
    );

    let schedule = std::fs::read_to_string(c.dir.path().join("out/schedule-pass-1.tsv")).unwrap();
    let groups: Vec<(&str, &str)> = schedule
        .lines()
        .map(|l| {
            let f: Vec<&str> = l.split('\t').collect();
            (f[0], f[1])
        })
        .collect();
    assert_eq!(
        groups,
        [
            ("1", "unordered"),
            ("1", "value"),
            ("2", "ordered"),
            ("2", "by_key")
        ],
        "{schedule}"
    );
}

/// A copy without its redb serves no sealed history; that is a broken rig, not a verdict on the
/// binary, so it exits 2 rather than 1 and never starts a server.
#[test]
fn a_copy_without_its_redb_is_a_setup_fault_not_a_failure() {
    let c = case();
    std::fs::remove_file(c.nest.join("nuthatch.redb")).unwrap();
    let set = c.set(&[("answers", c.counts())]);
    let (out, text) = c.gate(&set, &[], &[]);
    assert_eq!(out.status.code(), Some(2), "{text}");
    assert!(text.contains("no nuthatch.redb"), "{text}");
}

/// The serving process's peak RSS is held to the per-cursor budget. A one-megabyte ceiling no real
/// server fits under must fail a set that otherwise answers, and name the peak.
#[test]
fn a_peak_rss_over_the_budget_fails_a_set_that_answers() {
    let c = case();
    let set = c.set(&[("answers", c.counts())]);
    let (out, text) = c.gate(&set, &[], &[("GATE_MAX_RSS_MB", "1")]);
    assert_eq!(out.status.code(), Some(1), "{text}");
    assert!(line_for(&text, "answers").starts_with("ok "), "{text}");
    assert!(text.contains("peak RSS") && text.contains("OVER"), "{text}");

    let (out, text) = c.gate(&set, &[], &[]);
    assert_eq!(out.status.code(), Some(0), "{text}");
    assert!(
        text.contains("peak RSS"),
        "the peak is reported on a pass too:\n{text}"
    );
}

/// A `serve` wrapper that writes a fake `<proc>/<its pid>/status`, the next VmHWM from `hwm_kb` on
/// each start, and serves the real binary as its child. Like the real one, the status is gone once
/// the wrapper is stopped, so a read after the kill finds nothing (#1833).
fn hwm_wrapper(c: &Case, hwm_kb: &[u64]) -> (PathBuf, PathBuf) {
    use std::os::unix::fs::PermissionsExt;
    let proc_root = c.dir.path().join("proc");
    std::fs::create_dir_all(&proc_root).unwrap();
    let list = hwm_kb.iter().map(u64::to_string).collect::<Vec<_>>();
    let wrapper = c.dir.path().join("nuthatch-hwm");
    std::fs::write(
        &wrapper,
        format!(
            "#!/usr/bin/env bash\n\
             set -eu\n\
             [ \"${{1:-}}\" = serve ] || exec '{real}' \"$@\"\n\
             n=$(cat '{proc}/starts' 2>/dev/null || echo 0)\n\
             echo $((n + 1)) >'{proc}/starts'\n\
             hwm=$(echo '{list}' | cut -d' ' -f$((n + 1)))\n\
             mkdir -p '{proc}/'$$\n\
             printf 'Name:\\tnuthatch\\nVmPeak:\\t 9999999 kB\\nVmHWM:\\t %s kB\\nVmRSS:\\t 1 kB\\n' \"$hwm\" >'{proc}/'$$/status\n\
             if [ \"$(cat '{proc}/clash' 2>/dev/null)\" = $((n + 1)) ]; then rm '{proc}/clash'; sh -c 'echo address already in use >&2; exit 1' & else '{real}' \"$@\" & fi\n\
             child=$!\n\
             trap 'rm -rf \"{proc}/'$$'\"; kill $child 2>/dev/null; wait $child; exit 0' TERM INT\n\
             rc=0; wait $child || rc=$?\n\
             # A serve that exits unstopped (a port clash the gate retries) gives its slot back.\n\
             echo $n >'{proc}/starts'; rm -rf '{proc}/'$$; exit $rc\n",
            real = env!("CARGO_BIN_EXE_nuthatch"),
            proc = proc_root.display(),
            list = list.join(" "),
        ),
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
    (wrapper, proc_root)
}

fn gate_with(
    c: &Case,
    bin: &Path,
    set: &Path,
    passes: &str,
    env: &[(&str, &str)],
) -> (Output, String) {
    let mut cmd = Command::new(root().join("scripts/release-gate.sh"));
    calm(&mut cmd);
    cmd.args(["--passes", passes, "--out"])
        .arg(c.dir.path().join("out"))
        .arg(bin)
        .arg(&c.nest)
        .arg(set);
    for (k, v) in env {
        cmd.env(k, v);
    }
    let out = cmd.output().expect("run release-gate.sh");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out, text)
}

/// The half-second sampler missed a spike by ~180 MiB (#1833). The kernel's high-water mark, read as
/// each server stops, is the peak and decides the verdict, the largest over every server kept. The
/// middle of three passes holds it, so keeping the first or the last would pass.
#[test]
fn the_kernel_high_water_mark_over_every_server_is_the_peak_and_the_verdict() {
    let c = case();
    let set = c.set(&[("answers", c.counts())]);
    let (bin, proc_root) = hwm_wrapper(&c, &[1_000_000, 3_000_000, 1_500_000]);
    let proc_env = proc_root.to_str().unwrap();
    let (out, text) = gate_with(&c, &bin, &set, "3", &[("GATE_PROC_ROOT", proc_env)]);
    assert_eq!(out.status.code(), Some(1), "{text}");
    assert!(line_for(&text, "answers").starts_with("ok "), "{text}");
    assert!(
        text.contains("release-gate: peak RSS 2929 MiB, budget 2048 MiB: OVER\n"),
        "{text}"
    );
    assert!(text.contains("RESULT: FAIL - failed: peak RSS"), "{text}");
    let from = text
        .lines()
        .find(|l| l.starts_with("release-gate: peak RSS from "))
        .unwrap_or_else(|| panic!("no line naming where the peak came from:\n{text}"));
    assert!(from.contains("(VmHWM) 2929 MiB"), "{from}");
    let sampled: u64 = from
        .split("sampled every 0.5 s, ")
        .nth(1)
        .and_then(|s| s.split(" MiB").next())
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("the sampled figure belongs alongside: {from}"));
    assert!(sampled < 2048, "{from}");

    let (bin, proc_root) = hwm_wrapper(&c, &[1_000_000]);
    std::fs::remove_file(proc_root.join("starts")).unwrap();
    let (out, text) = gate_with(
        &c,
        &bin,
        &set,
        "1",
        &[("GATE_PROC_ROOT", proc_root.to_str().unwrap())],
    );
    assert_eq!(out.status.code(), Some(0), "{text}");
    assert!(
        text.contains("release-gate: peak RSS 976 MiB, budget 2048 MiB\n"),
        "{text}"
    );
}

/// A port clash makes the gate start that pass's server again. The retry must carry the pass's own
/// high-water mark, not shift every later pass along by one.
#[test]
fn a_port_clash_retry_keeps_each_pass_high_water_mark() {
    let c = case();
    let set = c.set(&[("answers", c.counts())]);
    let (bin, proc_root) = hwm_wrapper(&c, &[1_000_000, 3_000_000, 1_500_000]);
    std::fs::write(proc_root.join("clash"), "2").unwrap();
    let proc_env = proc_root.to_str().unwrap();
    let (out, text) = gate_with(&c, &bin, &set, "3", &[("GATE_PROC_ROOT", proc_env)]);
    assert!(
        !proc_root.join("clash").exists(),
        "the clash never happened:\n{text}"
    );
    assert_eq!(out.status.code(), Some(1), "{text}");
    assert!(
        text.contains("release-gate: peak RSS 2929 MiB, budget 2048 MiB: OVER\n"),
        "{text}"
    );
    assert!(!text.contains("VmHWM unread"), "{text}");
}

/// Where /proc exists but a server's status is gone, the run says so rather than reporting a
/// high-water mark that covers fewer servers than it ran.
#[test]
fn a_server_whose_status_is_gone_is_counted_as_unread() {
    let c = case();
    let set = c.set(&[("answers", c.counts())]);
    let proc_root = c.dir.path().join("empty-proc");
    std::fs::create_dir_all(&proc_root).unwrap();
    let (out, text) = gate_with(
        &c,
        Path::new(env!("CARGO_BIN_EXE_nuthatch")),
        &set,
        "1",
        &[("GATE_PROC_ROOT", proc_root.to_str().unwrap())],
    );
    assert_eq!(out.status.code(), Some(0), "{text}");
    assert!(text.contains("VmHWM unread for 1 server(s)"), "{text}");
}

/// Starts a gate script with its output in a file; `finish` kills it if it overruns, so a lock that
/// deadlocks fails the test rather than hanging it.
fn run_bounded(mut cmd: Command, dir: &Path, name: &str) -> Child {
    let out = std::fs::File::create(dir.join(format!("{name}.out"))).unwrap();
    let err = out.try_clone().unwrap();
    cmd.stdout(out).stderr(err);
    cmd.spawn().expect("spawn a gate script")
}

fn finish(mut child: Child, dir: &Path, name: &str, secs: u64) -> (Option<i32>, String) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    let status = loop {
        if let Some(s) = child.try_wait().unwrap() {
            break s;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "{name} still running after {secs}s:\n{}",
                std::fs::read_to_string(dir.join(format!("{name}.out"))).unwrap_or_default()
            );
        }
        std::thread::sleep(Duration::from_millis(200));
    };
    let text = std::fs::read_to_string(dir.join(format!("{name}.out"))).unwrap();
    (status.code(), text)
}

/// The calls `release-gate-run.sh` makes, answered from `$FAKE_GH_DIR`: `releases` (one
/// `<tag> full|pre|draft` per line, newest first), `status-<sha>` (the state of the newest gate
/// status on a commit; absent is none) and `bin-<tag>/nuthatch` (the binary a release ships). A
/// tag's commit is `sha-<tag>`. Posted statuses are appended to `posted`, and with their context
/// first to `posted-ctx`; downloads to `downloaded`. `status-<sha>-<context, / as _>` answers for
/// one context before `status-<sha>` answers for all.
const FAKE_GH: &str = r#"#!/usr/bin/env bash
set -euo pipefail
d=$FAKE_GH_DIR
case "$1 $2" in
  "api -X")
    sha=${4##*/} state="" desc="" ctx=""
    shift 4
    while [ $# -ge 2 ]; do
      case "$2" in
        state=*) state=${2#state=} ;;
        description=*) desc=${2#description=} ;;
        context=*) ctx=${2#context=} ;;
      esac
      shift 2
    done
    echo "$sha $state $desc" >>"$d/posted"
    echo "$ctx $sha $state $desc" >>"$d/posted-ctx" ;;
  "api "*)
    case "$2" in
      */statuses)
        sha=${2%/statuses}; sha=${sha##*/}
        ctx=$(printf '%s' "$*" | sed -n 's/.*\.context == "\([^"]*\)".*/\1/p' | head -n 1)
        cat "$d/status-$sha-${ctx//\//_}" 2>/dev/null || cat "$d/status-$sha" 2>/dev/null || echo none ;;
      */commits/*) echo "sha-${2##*/}" ;;
      *) echo "fake gh: unexpected api call: $*" >&2; exit 3 ;;
    esac ;;
  "release list")
    pre=1
    for a in "$@"; do [ "$a" != --exclude-pre-releases ] || pre=0; done
    while read -r t kind; do
      [ "$kind" != draft ] || continue
      [ "$kind" != pre ] || [ "$pre" -eq 1 ] || continue
      echo "$t"
    done <"$d/releases" ;;
  "release download")
    t=$3 out=""
    while [ $# -gt 0 ]; do
      if [ "$1" = -D ]; then out=$2; fi
      shift
    done
    asset=nuthatch-x86_64-unknown-linux-gnu.tar.gz
    echo "$t" >>"$d/downloaded"
    tar -czf "$out/$asset" -C "$d/bin-$t" nuthatch
    if command -v sha256sum >/dev/null; then
      (cd "$out" && sha256sum "$asset" >"$asset.sha256")
    else
      (cd "$out" && shasum -a 256 "$asset" >"$asset.sha256")
    fi ;;
  *) echo "fake gh: unexpected call: $*" >&2; exit 3 ;;
esac
"#;

struct Releases {
    dir: PathBuf,
}

/// Fake releases of the real binary. Each ships a wrapper that serves the real binary as its child
/// after holding `pad` bytes itself, so the RSS the gate samples (the wrapper's) differs by release.
fn releases(c: &Case, list: &[(&str, &str, usize)], gated: &[(&str, &str)]) -> Releases {
    use std::os::unix::fs::PermissionsExt;
    let dir = c.dir.path().join("gh");
    let fakes = dir.join("fakes");
    std::fs::create_dir_all(&fakes).unwrap();
    let gh = fakes.join("gh");
    std::fs::write(&gh, FAKE_GH).unwrap();
    std::fs::set_permissions(&gh, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut listing = String::new();
    for (tag, kind, pad) in list {
        listing.push_str(&format!("{tag} {kind}\n"));
        let bin_dir = dir.join(format!("bin-{tag}"));
        std::fs::create_dir_all(&bin_dir).unwrap();
        let real = env!("CARGO_BIN_EXE_nuthatch");
        let wrapper = bin_dir.join("nuthatch");
        std::fs::write(
            &wrapper,
            format!(
                "#!/usr/bin/env bash\n\
                 [ \"${{1:-}}\" = serve ] || exec '{real}' \"$@\"\n\
                 pad=$(head -c {pad} /dev/zero | tr '\\0' x)\n\
                 '{real}' \"$@\" &\n\
                 child=$!\n\
                 trap 'kill $child 2>/dev/null; wait $child; exit 0' TERM INT\n\
                 wait $child\n"
            ),
        )
        .unwrap();
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    std::fs::write(dir.join("releases"), listing).unwrap();
    for (tag, state) in gated {
        std::fs::write(dir.join(format!("status-sha-{tag}")), format!("{state}\n")).unwrap();
    }
    Releases { dir }
}

impl Releases {
    fn read(&self, name: &str) -> String {
        std::fs::read_to_string(self.dir.join(name)).unwrap_or_default()
    }

    fn runner(
        &self,
        c: &Case,
        set: &Path,
        args: &[&str],
        env: &[(&str, &str)],
    ) -> (Option<i32>, String) {
        let mut cmd = Command::new(root().join("scripts/release-gate-run.sh"));
        calm(&mut cmd);
        cmd.args(args)
            .env(
                "PATH",
                format!(
                    "{}:{}",
                    self.dir.join("fakes").display(),
                    std::env::var("PATH").unwrap()
                ),
            )
            .env("FAKE_GH_DIR", &self.dir)
            .env("GATE_STATE", c.dir.path().join("state"))
            .env("GATE_NEST", &c.nest)
            .env("GATE_SET", set)
            .env("GATE_PASSES", "1")
            .env("GATE_REPO", "test/nuthatch")
            .env_remove("GATE_LOCK_HELD");
        for (k, v) in env {
            cmd.env(k, v);
        }
        let child = run_bounded(cmd, c.dir.path(), "runner");
        finish(child, c.dir.path(), "runner", 120)
    }
}

/// On 2026-10-03 production peaked at 3324 MiB over the 2048 budget with every statement answered,
/// and the runner posted error ("no baseline") without measuring the candidate, so nothing could be
/// gated while production was over. Production's peak is reported; the verdict is the candidate's.
#[test]
fn production_over_its_rss_budget_is_still_a_baseline_for_the_candidate() {
    let c = case();
    let set = c.set(&[("answers", c.counts())]);
    let r = releases(
        &c,
        &[("v4.3.0-rc1", "pre", 0), ("v4.2.0", "full", 160_000_000)],
        &[],
    );
    let (code, text) = r.runner(&c, &set, &["v4.3.0-rc1"], &[("GATE_MAX_RSS_MB", "100")]);
    assert_eq!(code, Some(0), "{text}");
    let posted = r.read("posted");
    let verdict = posted.lines().last().unwrap_or_default();
    assert!(
        verdict.starts_with("sha-v4.3.0-rc1 success ") && verdict.contains("v4.2.0"),
        "{posted}\n{text}"
    );
    assert!(
        verdict.contains("MiB"),
        "production's own peak belongs in the candidate's status: {verdict}"
    );
    assert!(text.contains("over the 100 MiB budget"), "{text}");
    // The copy has no PROVENANCE, so production is a guess, and the status says so.
    assert!(verdict.contains("no PROVENANCE"), "{verdict}");
    assert!(text.contains("has no PROVENANCE"), "{text}");
}

/// A statement production fails as well cannot be a regression, and the output says so, but the
/// candidate is still failed for refusing it: its verdict is its own.
#[test]
fn a_statement_production_also_fails_still_fails_the_candidate_and_is_not_a_regression() {
    let c = case();
    let set = c.set(&[
        ("answers", c.counts()),
        ("refused", "SELECT a FROM no_such_table".to_string()),
    ]);
    let r = releases(&c, &[("v4.3.0-rc1", "pre", 0), ("v4.2.0", "full", 0)], &[]);
    let (code, text) = r.runner(&c, &set, &["v4.3.0-rc1"], &[]);
    assert_eq!(code, Some(1), "{text}");
    let posted = r.read("posted");
    let verdict = posted.lines().last().unwrap_or_default();
    assert!(
        verdict.starts_with("sha-v4.3.0-rc1 failure ") && verdict.contains("refused"),
        "{posted}\n{text}"
    );
    assert!(
        text.contains("production v4.2.0 fails refused itself, so it cannot regress"),
        "{text}"
    );
}

/// On 2026-10-03 `--poll` walked back through every past release without a status, posting error
/// on v4.1.0 and then v4.0.2. Only a release newer than production is gated; a prerelease of
/// production's own version is older than it.
#[test]
fn poll_never_gates_a_release_at_or_below_production() {
    let c = case();
    let set = c.set(&[("answers", c.counts())]);
    let r = releases(
        &c,
        &[
            ("v4.2.0", "full", 0),
            ("v4.2.0-rc1", "pre", 0),
            ("v4.1.0", "full", 0),
            ("v4.0.2", "full", 0),
        ],
        &[("v4.2.0", "success")],
    );
    let (code, text) = r.runner(&c, &set, &["--poll"], &[]);
    assert_eq!(code, Some(0), "{text}");
    assert_eq!(r.read("posted"), "", "nothing is gated:\n{text}");
    assert_eq!(r.read("downloaded"), "", "nothing is fetched:\n{text}");
    assert_eq!(text, "", "nothing to gate is quiet");
}

/// A prerelease of a newer version is gated, compared by number: v4.10 is newer than v4.9.
#[test]
fn poll_gates_a_prerelease_of_a_newer_version() {
    let c = case();
    let set = c.set(&[("answers", c.counts())]);
    let r = releases(
        &c,
        &[
            ("v4.10.0-rc.1", "pre", 0),
            ("v4.9.0", "full", 0),
            ("v4.8.0", "full", 0),
        ],
        &[("v4.9.0", "success")],
    );
    // The runner holds the copy's lock itself from before the refresh, so a gate run by hand cannot
    // slip in between the refresh and the two measurements.
    let lock = std::fs::canonicalize(c.dir.path())
        .unwrap()
        .join("nest.gate-lock.d/pid");
    let refresh = format!("[ \"$(cat '{}')\" = \"$PPID\" ]", lock.display());
    let (code, text) = r.runner(
        &c,
        &set,
        &["--poll"],
        &[("GATE_LOCK_PORTABLE", "1"), ("GATE_REFRESH", &refresh)],
    );
    assert_eq!(code, Some(0), "{text}");
    let posted = r.read("posted");
    assert!(
        posted
            .lines()
            .last()
            .unwrap_or_default()
            .starts_with("sha-v4.10.0-rc.1 success "),
        "{posted}\n{text}"
    );
    assert!(!posted.contains("sha-v4.8.0"), "{posted}");
}

/// A hand-run gate against the copy the runner was using made the runner's `serve` fail to start
/// on the redb lock. Two gate runs against one copy wait for each other; both answer.
#[test]
fn two_gate_runs_against_one_copy_serialise() {
    for portable in [false, true] {
        let c = case();
        let set = c.set(&[("answers", c.counts())]);
        let start = |name: &str| {
            let mut cmd = Command::new(root().join("scripts/release-gate.sh"));
            calm(&mut cmd);
            cmd.args(["--passes", "2", "--out"])
                .arg(c.dir.path().join(name))
                .arg(env!("CARGO_BIN_EXE_nuthatch"))
                .arg(&c.nest)
                .arg(&set)
                .env_remove("GATE_LOCK_HELD");
            if portable {
                cmd.env("GATE_LOCK_PORTABLE", "1");
            }
            run_bounded(cmd, c.dir.path(), name)
        };
        let a = start("first");
        let b = start("second");
        let (code_a, text_a) = finish(a, c.dir.path(), "first", 300);
        let (code_b, text_b) = finish(b, c.dir.path(), "second", 300);
        assert_eq!(code_a, Some(0), "portable={portable}:\n{text_a}");
        assert_eq!(code_b, Some(0), "portable={portable}:\n{text_b}");
        assert!(
            format!("{text_a}{text_b}").contains("waiting"),
            "one run waited for the other (portable={portable}):\n{text_a}\n{text_b}"
        );
        assert!(
            !c.dir.path().join("nest.gate-lock.d").exists(),
            "the lock is released on exit"
        );
    }
}

/// Nothing releases a mkdir lock for a holder that was killed, so a lock whose pid has gone is
/// broken rather than waited on for ever.
#[test]
fn a_lock_left_by_a_killed_run_is_broken() {
    let c = case();
    let set = c.set(&[("answers", c.counts())]);
    let mut gone = Command::new("true").spawn().unwrap();
    let pid = gone.id();
    gone.wait().unwrap();
    let held = c.dir.path().join("nest.gate-lock.d");
    std::fs::create_dir(&held).unwrap();
    std::fs::write(held.join("pid"), format!("{pid}\n")).unwrap();
    let mut cmd = Command::new(root().join("scripts/release-gate.sh"));
    calm(&mut cmd);
    cmd.args(["--passes", "1", "--out"])
        .arg(c.dir.path().join("out"))
        .arg(env!("CARGO_BIN_EXE_nuthatch"))
        .arg(&c.nest)
        .arg(&set)
        .env("GATE_LOCK_PORTABLE", "1")
        .env_remove("GATE_LOCK_HELD");
    let child = run_bounded(cmd, c.dir.path(), "gate");
    let (code, text) = finish(child, c.dir.path(), "gate", 120);
    assert_eq!(code, Some(0), "{text}");
    assert!(text.contains("breaking"), "{text}");
}

/// On 2026-10-04 v4.3.1 was gated against v4.3.0, the latest release, while the allocations nest
/// ran 4.2.1, so the statements 4.3.1 fixed read as answers that differ. Production is the version
/// the refreshed copy's PROVENANCE records.
#[test]
fn production_is_the_version_the_refreshed_copy_records_not_the_latest_release() {
    let c = case();
    let set = c.set(&[("answers", c.counts())]);
    let r = releases(
        &c,
        &[
            ("v4.3.1", "full", 0),
            ("v4.3.0", "full", 0),
            ("v4.2.1", "full", 0),
        ],
        &[],
    );
    let provenance = c.nest.join("PROVENANCE");
    assert!(
        !provenance.exists(),
        "the refresh writes it, not the fixture"
    );
    let refresh = format!(
        "printf 'taken_at=2026-10-04T00:00:00Z\\nversion=4.2.1\\nsealed_through=8\\n' >'{}'",
        provenance.display()
    );
    let (code, text) = r.runner(&c, &set, &["v4.3.1"], &[("GATE_REFRESH", &refresh)]);
    assert_eq!(code, Some(0), "{text}");
    assert_eq!(r.read("downloaded"), "v4.2.1\nv4.3.1\n", "{text}");
    let posted = r.read("posted");
    assert!(
        posted.lines().all(|l| !l.contains("v4.3.0")),
        "{posted}\n{text}"
    );
    let verdict = posted.lines().last().unwrap_or_default();
    assert!(
        verdict.starts_with("sha-v4.3.1 success ") && verdict.contains("(against v4.2.1)"),
        "{posted}\n{text}"
    );
    assert!(text.contains("PROVENANCE records"), "{text}");
}

/// `--poll`'s "newer than production" uses the same production: a candidate at or below what the
/// copy records production running is not gated, though it is newer than the latest full release.
#[test]
fn poll_measures_newer_against_the_production_the_copy_records() {
    let c = case();
    let set = c.set(&[("answers", c.counts())]);
    let r = releases(
        &c,
        &[
            ("v4.3.0-rc.2", "pre", 0),
            ("v4.3.0-rc.1", "pre", 0),
            ("v4.2.1", "full", 0),
        ],
        &[],
    );
    std::fs::write(c.nest.join("PROVENANCE"), "version=4.3.0-rc.2\n").unwrap();
    let (code, text) = r.runner(&c, &set, &["--poll"], &[]);
    assert_eq!(code, Some(0), "{text}");
    assert_eq!(r.read("posted"), "", "nothing is gated:\n{text}");
    assert_eq!(r.read("downloaded"), "", "nothing is fetched:\n{text}");
    assert_eq!(text, "", "nothing to gate is quiet");
}

/// Production rolled to the candidate between the poll and the refresh: the refreshed copy names
/// the candidate itself, so there is nothing to gate and nothing is posted.
#[test]
fn poll_leaves_a_candidate_production_was_rolled_to_during_the_refresh() {
    let c = case();
    let set = c.set(&[("answers", c.counts())]);
    let r = releases(&c, &[("v4.3.0-rc.2", "pre", 0), ("v4.2.1", "full", 0)], &[]);
    let provenance = c.nest.join("PROVENANCE");
    std::fs::write(&provenance, "version=4.2.1\n").unwrap();
    let refresh = format!("printf 'version=4.3.0-rc.2\\n' >'{}'", provenance.display());
    let (code, text) = r.runner(&c, &set, &["--poll"], &[("GATE_REFRESH", &refresh)]);
    assert_eq!(code, Some(0), "{text}");
    assert_eq!(r.read("posted"), "", "nothing is gated:\n{text}");
    assert_eq!(r.read("downloaded"), "", "nothing is fetched:\n{text}");
    assert!(text.contains("no longer newer"), "{text}");
}

/// `--production` still overrides the copy's PROVENANCE.
#[test]
fn production_flag_overrides_the_copys_provenance() {
    let c = case();
    let set = c.set(&[("answers", c.counts())]);
    let r = releases(
        &c,
        &[
            ("v4.3.1", "full", 0),
            ("v4.3.0", "full", 0),
            ("v4.2.1", "full", 0),
        ],
        &[],
    );
    std::fs::write(c.nest.join("PROVENANCE"), "version=4.2.1\n").unwrap();
    let (code, text) = r.runner(&c, &set, &["v4.3.1", "--production", "v4.3.0"], &[]);
    assert_eq!(code, Some(0), "{text}");
    assert_eq!(r.read("downloaded"), "v4.3.0\nv4.3.1\n", "{text}");
}

/// A PROVENANCE whose version is not a release version is not guessed around: the run exits 2 and
/// posts one error status on the candidate naming the version, by tag or after a poll's refresh.
#[test]
fn a_provenance_version_that_is_not_a_release_posts_one_error_naming_it() {
    for poll in [false, true] {
        let c = case();
        let set = c.set(&[("answers", c.counts())]);
        let r = releases(&c, &[("v4.3.1", "full", 0), ("v4.2.1", "full", 0)], &[]);
        let provenance = c.nest.join("PROVENANCE");
        let (args, refresh): (&[&str], String) = if poll {
            std::fs::write(&provenance, "version=4.2.1\n").unwrap();
            let refresh = format!(
                "printf 'version=dirty-build\\n' >'{}'",
                provenance.display()
            );
            (&["--poll"], refresh)
        } else {
            std::fs::write(&provenance, "version=dirty-build\n").unwrap();
            (&["v4.3.1"], String::new())
        };
        let env: &[(&str, &str)] = if poll {
            &[("GATE_REFRESH", &refresh)]
        } else {
            &[]
        };
        let (code, text) = r.runner(&c, &set, args, env);
        assert_eq!(code, Some(2), "poll={poll}: {text}");
        let posted = r.read("posted");
        assert_eq!(posted.lines().count(), 1, "poll={poll}: {posted}\n{text}");
        assert!(
            posted.starts_with("sha-v4.3.1 error ") && posted.contains("dirty-build"),
            "poll={poll}: {posted}\n{text}"
        );
        assert_eq!(r.read("downloaded"), "", "poll={poll}: {text}");
    }
}

/// Before a poll has chosen a candidate there is no commit to post on: the run exits 2 and says so.
#[test]
fn a_bad_provenance_before_a_poll_chooses_exits_loud_with_no_status() {
    let c = case();
    let set = c.set(&[("answers", c.counts())]);
    let r = releases(&c, &[("v4.3.1", "full", 0), ("v4.2.1", "full", 0)], &[]);
    std::fs::write(c.nest.join("PROVENANCE"), "version=dirty-build\n").unwrap();
    let (code, text) = r.runner(&c, &set, &["--poll"], &[]);
    assert_eq!(code, Some(2), "{text}");
    assert_eq!(r.read("posted"), "", "{text}");
    assert!(
        text.contains("dirty-build") && text.contains("no candidate is chosen yet"),
        "{text}"
    );
}

// --- every production nest (#1794) ---

/// The ThinkPad's gate config for these runs: per nest, its own copy of the fixture nest, its own
/// set and its own production environment, and a refresh kind. Returns the config's path.
/// A nest's name, its set's `(id, sql)` lines, its env file's body and its refresh kind.
type NestRow<'a> = (&'a str, &'a [(&'a str, String)], &'a str, &'a str);

fn nests_conf(c: &Case, nests: &[NestRow]) -> PathBuf {
    let mut conf = String::from("# name copy set env refresh\n");
    for (name, lines, env, refresh) in nests {
        let copy = c.dir.path().join(name);
        copy_dir(&c.nest, &copy);
        let set = c.dir.path().join(format!("{name}-queries.tsv"));
        let mut body = String::from("# id\tconsumer\tsite\tsql\n");
        for (id, sql) in *lines {
            body.push_str(&format!(
                "{id}\ttest\ttests/release_gate_script.rs\t{sql}\n"
            ));
        }
        std::fs::write(&set, body).unwrap();
        let env_file = c.dir.path().join(format!("{name}.env"));
        std::fs::write(&env_file, format!("# read from {name}'s unit\n{env}")).unwrap();
        conf.push_str(&format!(
            "{name}\t{}\t{}\t{}\t{refresh}\n",
            copy.display(),
            set.display(),
            env_file.display()
        ));
    }
    let path = c.dir.path().join("nests.conf");
    std::fs::write(&path, conf).unwrap();
    path
}

const ENV_TWO: &str = "NUTHATCH_SQL_MAX_CONCURRENCY=2\nNUTHATCH_ENGINE=burrmill\n";
const ENV_ONE: &str = "NUTHATCH_SQL_MAX_CONCURRENCY=1\nNUTHATCH_ENGINE=burrmill\n";

fn posted_for<'a>(posted: &'a str, context: &str) -> Vec<&'a str> {
    posted
        .lines()
        .filter(|l| l.split(' ').next() == Some(context))
        .collect()
}

/// On 2026-10-03 the QoS nest refused its daily views all day and the gate, which ran the
/// allocations nest alone, never saw it. Every nest in the config is gated, each under its own
/// environment and refresh, and each posts its own status; the run's exit is the worst of theirs.
#[test]
fn every_nest_in_the_config_is_gated_and_posts_its_own_status() {
    use std::os::unix::fs::PermissionsExt;
    let c = case();
    let answers = [("answers", c.counts())];
    let refused = [
        ("answers", c.counts()),
        ("daily", "SELECT a FROM no_such_view".to_string()),
    ];
    let conf = nests_conf(
        &c,
        &[
            ("alloc-nest", &answers, ENV_TWO, "helsinki"),
            ("qos-nest", &refused, ENV_ONE, "local"),
        ],
    );
    let r = releases(&c, &[("v4.3.0-rc1", "pre", 0), ("v4.2.0", "full", 0)], &[]);
    let refresh_log = c.dir.path().join("refreshed");
    let refresh = c.dir.path().join("fake-refresh");
    std::fs::write(
        &refresh,
        format!(
            "#!/usr/bin/env bash\necho \"$* $GATE_NEST\" >>'{}'\n",
            refresh_log.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&refresh, std::fs::Permissions::from_mode(0o755)).unwrap();
    let set = c.set(&[("unused", c.counts())]);
    let (code, text) = r.runner(
        &c,
        &set,
        &["v4.3.0-rc1"],
        &[
            ("GATE_NESTS", conf.to_str().unwrap()),
            ("GATE_REFRESH_SCRIPT", refresh.to_str().unwrap()),
        ],
    );
    assert_eq!(code, Some(1), "the worst nest decides:\n{text}");
    let posted = r.read("posted-ctx");
    let alloc = posted_for(&posted, "release-gate/alloc-nest");
    let qos = posted_for(&posted, "release-gate/qos-nest");
    assert_eq!(alloc.len(), 2, "pending then a verdict:\n{posted}");
    assert!(
        alloc[1].starts_with("release-gate/alloc-nest sha-v4.3.0-rc1 success "),
        "{posted}\n{text}"
    );
    assert_eq!(qos.len(), 2, "pending then a verdict:\n{posted}");
    assert!(
        qos[1].starts_with("release-gate/qos-nest sha-v4.3.0-rc1 failure ")
            && qos[1].contains("daily"),
        "{posted}\n{text}"
    );
    assert!(qos[0].contains("against the qos-nest copy"), "{posted}");
    // Each nest ran under its own environment file, at its own concurrency.
    let qos_env = c.dir.path().join("qos-nest.env");
    let from = format!("(from {})", qos_env.display());
    assert!(
        text.lines()
            .any(|l| l.starts_with("[qos-nest] ") && l.contains(&from)),
        "the candidate's run:\n{text}"
    );
    let runs = c.dir.path().join("state/runs");
    let production = std::fs::read_dir(&runs)
        .unwrap()
        .flatten()
        .find(|e| e.file_name().to_string_lossy().contains("-qos-nest-"))
        .map(|e| std::fs::read_to_string(e.path().join("production.txt")).unwrap())
        .unwrap_or_default();
    assert!(
        production.contains(&from),
        "production's run:\n{production}"
    );
    assert!(
        text.contains("[qos-nest] release-gate: concurrency 1"),
        "{text}"
    );
    assert!(
        text.contains("[alloc-nest] release-gate: concurrency 2"),
        "{text}"
    );
    let refreshed = std::fs::read_to_string(&refresh_log).unwrap_or_default();
    assert_eq!(
        refreshed,
        format!(
            "alloc-nest {}\n--local qos-nest {}\n",
            c.dir.path().join("alloc-nest").display(),
            c.dir.path().join("qos-nest").display()
        ),
        "{text}"
    );
}

/// A poll gates, nest by nest, a release that nest has no status for: the allocations nest gated
/// the candidate already, so only the QoS nest gates it now.
#[test]
fn poll_gates_each_nest_that_lacks_its_own_status() {
    let c = case();
    let answers = [("answers", c.counts())];
    let conf = nests_conf(
        &c,
        &[
            ("alloc-nest", &answers, ENV_TWO, "none"),
            ("qos-nest", &answers, ENV_TWO, "none"),
        ],
    );
    let r = releases(&c, &[("v4.3.0-rc1", "pre", 0), ("v4.2.0", "full", 0)], &[]);
    std::fs::write(
        r.dir.join("status-sha-v4.3.0-rc1-release-gate_alloc-nest"),
        "success\n",
    )
    .unwrap();
    for nest in ["alloc-nest", "qos-nest"] {
        std::fs::write(
            c.dir.path().join(nest).join("PROVENANCE"),
            "version=4.2.0\n",
        )
        .unwrap();
    }
    let set = c.set(&[("unused", c.counts())]);
    let (code, text) = r.runner(
        &c,
        &set,
        &["--poll"],
        &[("GATE_NESTS", conf.to_str().unwrap())],
    );
    assert_eq!(code, Some(0), "{text}");
    let posted = r.read("posted-ctx");
    assert!(
        posted_for(&posted, "release-gate/alloc-nest").is_empty(),
        "{posted}\n{text}"
    );
    let qos = posted_for(&posted, "release-gate/qos-nest");
    assert!(
        qos.last()
            .is_some_and(|l| l.starts_with("release-gate/qos-nest sha-v4.3.0-rc1 success ")),
        "{posted}\n{text}"
    );
    assert!(
        !text.contains("[alloc-nest]"),
        "a nest with nothing to gate is quiet:\n{text}"
    );
}

/// A config line that does not parse gates nothing, rather than half the nests.
#[test]
fn a_malformed_nests_config_gates_nothing() {
    let c = case();
    let answers = [("answers", c.counts())];
    let conf = nests_conf(&c, &[("alloc-nest", &answers, ENV_TWO, "none")]);
    let mut body = std::fs::read_to_string(&conf).unwrap();
    body.push_str("qos-nest /nowhere /nowhere.tsv /nowhere.env none stray\n");
    std::fs::write(&conf, body).unwrap();
    let r = releases(&c, &[("v4.3.0-rc1", "pre", 0), ("v4.2.0", "full", 0)], &[]);
    let set = c.set(&[("unused", c.counts())]);
    let (code, text) = r.runner(
        &c,
        &set,
        &["v4.3.0-rc1"],
        &[("GATE_NESTS", conf.to_str().unwrap())],
    );
    assert_eq!(code, Some(2), "{text}");
    assert!(text.contains("line 3"), "{text}");
    assert_eq!(r.read("posted"), "", "{text}");
    assert_eq!(r.read("downloaded"), "", "{text}");
}

/// The environment `serve` runs under is the nest's file alone: what it sets is applied, and a
/// NUTHATCH_* setting the caller happens to carry is not.
#[test]
fn a_nests_environment_file_replaces_the_callers() {
    use std::os::unix::fs::PermissionsExt;
    let c = case();
    let set = c.set(&[("answers", c.counts())]);
    let seen = c.dir.path().join("serve-env");
    let bin = c.dir.path().join("nuthatch");
    std::fs::write(
        &bin,
        format!(
            "#!/usr/bin/env bash\n\
             [ \"${{1:-}}\" != serve ] || env | grep '^NUTHATCH_' | sort >'{}'\n\
             exec '{}' \"$@\"\n",
            seen.display(),
            env!("CARGO_BIN_EXE_nuthatch")
        ),
    )
    .unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    let env = c.dir.path().join("nest.env");
    std::fs::write(
        &env,
        "# read from the unit\nNUTHATCH_SQL_MAX_CONCURRENCY=1\nNUTHATCH_SQL_MEMO_BYTES=1048576\n",
    )
    .unwrap();
    let gate = |extra_env: &[(&str, &str)]| {
        let mut cmd = Command::new(root().join("scripts/release-gate.sh"));
        calm(&mut cmd);
        cmd.args(["--passes", "1", "--env"])
            .arg(&env)
            .arg("--out")
            .arg(c.dir.path().join("out"))
            .arg(&bin)
            .arg(&c.nest)
            .arg(&set);
        for (k, v) in extra_env {
            cmd.env(k, v);
        }
        let out = cmd.output().unwrap();
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        (out.status.code(), text)
    };
    let (code, text) = gate(&[("NUTHATCH_MAX_RSS", "1GB")]);
    assert_eq!(code, Some(0), "{text}");
    assert_eq!(
        std::fs::read_to_string(&seen).unwrap(),
        "NUTHATCH_SQL_MAX_CONCURRENCY=1\nNUTHATCH_SQL_MEMO_BYTES=1048576\n",
        "{text}"
    );

    std::fs::write(&env, "RUST_LOG=debug\n").unwrap();
    let (code, text) = gate(&[]);
    assert_eq!(code, Some(2), "{text}");
    assert!(text.contains("not a NUTHATCH_*=VALUE line"), "{text}");

    // An empty read is a fault; a unit that sets nothing says so, and serves on the defaults.
    std::fs::write(&env, "# read from the unit\n").unwrap();
    let (code, text) = gate(&[]);
    assert_eq!(code, Some(2), "{text}");
    assert!(text.contains("no NUTHATCH_* settings"), "{text}");

    std::fs::write(
        &env,
        "# read from the unit\n# none: the unit sets no NUTHATCH_* settings\n",
    )
    .unwrap();
    let (code, text) = gate(&[("NUTHATCH_MAX_RSS", "1GB")]);
    assert_eq!(code, Some(0), "{text}");
    assert_eq!(std::fs::read_to_string(&seen).unwrap(), "", "{text}");
}

/// The set lives in the private kittiwake repo, so the runner has no default to fall back on: an
/// unset `GATE_SET` is a setup fault that names the variable, before anything is fetched or posted.
#[test]
fn an_unset_gate_set_is_a_setup_fault_that_names_it() {
    let dir = tempfile::tempdir().unwrap();
    let fakes = dir.path().join("fakes");
    std::fs::create_dir_all(&fakes).unwrap();
    let gh = fakes.join("gh");
    std::fs::write(&gh, FAKE_GH).unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&gh, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    std::fs::write(dir.path().join("releases"), "v4.3.0-rc1 pre\n").unwrap();
    let mut cmd = Command::new(root().join("scripts/release-gate-run.sh"));
    calm(&mut cmd);
    cmd.arg("v4.3.0-rc1")
        .env(
            "PATH",
            format!("{}:{}", fakes.display(), std::env::var("PATH").unwrap()),
        )
        .env("FAKE_GH_DIR", dir.path())
        .env("GATE_STATE", dir.path().join("state"))
        .env_remove("GATE_SET")
        .env_remove("GATE_LOCK_HELD");
    let child = run_bounded(cmd, dir.path(), "runner");
    let (code, text) = finish(child, dir.path(), "runner", 30);
    assert_eq!(code, Some(2), "{text}");
    assert!(
        text.contains("GATE_SET is not set") && text.contains("nuthatch-gate/alloc-queries.tsv"),
        "{text}"
    );
    assert!(!dir.path().join("posted").exists(), "{text}");
    assert!(!dir.path().join("downloaded").exists(), "{text}");
}

/// A load source of the tests' own, so no case reads the machine it runs on: the gate waits for a
/// quiet box and a case run on a busy one would wait with it (#1898).
fn load_file(c: &Case, name: &str, one_minute: &str) -> PathBuf {
    let path = c.dir.path().join(name);
    std::fs::write(&path, format!("{one_minute} 5.00 5.00 1/100 1\n")).unwrap();
    path
}

fn calm(cmd: &mut Command) {
    static CALM: OnceLock<tempfile::NamedTempFile> = OnceLock::new();
    let file = CALM.get_or_init(|| {
        let f = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(f.path(), "0.10 0.10 0.10 1/100 1\n").unwrap();
        f
    });
    cmd.env("GATE_LOADAVG_FILE", file.path())
        .env("GATE_NCPU", "4");
}

/// After `delay`, writes `one_minute` into the fake load file; with `until` set, waits first for
/// that file to exist (a server's log: the run has started serving).
fn set_load_later(
    file: &Path,
    one_minute: &str,
    until: Option<PathBuf>,
    delay: Duration,
) -> std::thread::JoinHandle<()> {
    let file = file.to_path_buf();
    let body = format!("{one_minute} 5.00 5.00 1/100 1\n");
    std::thread::spawn(move || {
        if let Some(p) = until {
            let deadline = Instant::now() + Duration::from_secs(120);
            while !p.exists() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(20));
            }
        }
        std::thread::sleep(delay);
        std::fs::write(file, body).unwrap();
    })
}

/// #1898: the 4.8.0 gate measured at a load average of 26 and called the noise a regression. A box
/// that stays loaded past the wait is a setup fault: exit 2, nothing served, no verdict.
#[test]
fn a_loaded_box_is_waited_on_and_then_a_setup_fault_not_a_verdict() {
    let c = case();
    let set = c.set(&[("answers", c.counts())]);
    let load = load_file(&c, "loadavg", "26.00");
    let started = Instant::now();
    let (out, text) = c.gate(
        &set,
        &[],
        &[
            ("GATE_LOADAVG_FILE", load.to_str().unwrap()),
            ("GATE_NCPU", "4"),
            ("GATE_MAX_LOAD_WAIT", "3"),
            ("GATE_LOAD_POLL_SECS", "0.2"),
        ],
    );
    assert_eq!(out.status.code(), Some(2), "{text}");
    assert!(
        started.elapsed() >= Duration::from_secs(2),
        "it did not wait:\n{text}"
    );
    assert!(text.contains("26.00") && text.contains("load"), "{text}");
    assert!(
        !text.contains("RESULT: FAIL") && !text.contains("RESULT: PASS"),
        "{text}"
    );
    assert!(
        !c.dir.path().join("out/serve-pass-1.log").exists(),
        "a server was started on a loaded box"
    );
}

#[test]
fn a_box_that_quietens_during_the_wait_is_measured() {
    let c = case();
    let set = c.set(&[("answers", c.counts())]);
    let load = load_file(&c, "loadavg", "26.00");
    // Timed from `out/answers`, made just before the gate's first load read. Timed from the spawn,
    // a gate slower than a second to start read 0.10 first and never waited.
    let later = set_load_later(
        &load,
        "0.10",
        Some(c.dir.path().join("out/answers")),
        Duration::from_secs(1),
    );
    let (out, text) = c.gate(
        &set,
        &[],
        &[
            ("GATE_LOADAVG_FILE", load.to_str().unwrap()),
            ("GATE_NCPU", "4"),
            ("GATE_MAX_LOAD_WAIT", "60"),
            ("GATE_LOAD_POLL_SECS", "0.2"),
        ],
    );
    later.join().unwrap();
    assert_eq!(out.status.code(), Some(0), "{text}");
    assert!(text.contains("waited"), "{text}");
}

/// Load that arrives after the start turns what would be a regression into a setup fault, and no
/// baseline is written from it. The control, on a quiet box, is the regression it would have been.
#[test]
fn load_rising_mid_run_turns_a_regression_into_a_setup_fault() {
    let c = case();
    let set = c.set(&[("answers", c.counts())]);
    let baseline = c.dir.path().join("baseline.tsv");
    std::fs::write(&baseline, "# hand-made\nanswers\ttest\tok\t1\t-1\t-\t\n").unwrap();
    let base = baseline.to_str().unwrap();
    let zero_slack = [("GATE_QUERY_SLACK_MS", "0"), ("GATE_P99_SLACK_MS", "0")];

    let (out, text) = c.gate(&set, &["--baseline", base], &zero_slack);
    assert_eq!(
        out.status.code(),
        Some(1),
        "the control, on a quiet box:\n{text}"
    );

    let load = load_file(&c, "loadavg", "0.10");
    let later = set_load_later(
        &load,
        "26.00",
        Some(c.dir.path().join("loaded-out/serve-pass-1.log")),
        Duration::ZERO,
    );
    let written = c.dir.path().join("written.tsv");
    let loaded_out = c.dir.path().join("loaded-out");
    let mut env = vec![
        ("GATE_LOADAVG_FILE", load.to_str().unwrap()),
        ("GATE_NCPU", "4"),
        ("GATE_LOAD_SAMPLE_SECS", "0.2"),
        ("GATE_LOAD_MAX_SHARE", "0.01"),
    ];
    env.extend_from_slice(&zero_slack);
    let (out, text) = c.gate(
        &set,
        &[
            "--out",
            loaded_out.to_str().unwrap(),
            "--baseline",
            base,
            "--write-baseline",
            written.to_str().unwrap(),
        ],
        &env,
    );
    later.join().unwrap();
    assert_eq!(out.status.code(), Some(2), "{text}");
    assert!(
        text.contains("RESULT: SETUP FAULT") && text.contains("load"),
        "{text}"
    );
    assert!(!text.contains("RESULT: FAIL"), "{text}");
    assert!(
        !written.exists(),
        "a baseline was written from a loaded run"
    );
}

/// A quiet box passes as before, and the run says what the load was: at the start and over the run.
#[test]
fn a_quiet_box_passes_and_the_run_records_its_load() {
    let c = case();
    let set = c.set(&[("answers", c.counts())]);
    let (out, text) = c.gate(&set, &[], &[]);
    assert_eq!(out.status.code(), Some(0), "{text}");
    assert!(text.contains("box load 0.10 over 4 cores"), "{text}");
    assert!(text.contains("load during the run"), "{text}");
    assert!(c.dir.path().join("out/load.tsv").is_file());
}

/// No readable load is not a reason to refuse: the gate says so and measures, as it does for an
/// optional integration that is not configured.
#[test]
fn an_unreadable_load_source_is_noted_and_the_gate_runs() {
    let c = case();
    let set = c.set(&[("answers", c.counts())]);
    let gone = c.dir.path().join("no-such-loadavg");
    let (out, text) = c.gate(&set, &[], &[("GATE_LOADAVG_FILE", gone.to_str().unwrap())]);
    assert_eq!(out.status.code(), Some(0), "{text}");
    assert!(text.contains("load could not be read"), "{text}");
}

/// The defaults, on the gate box's shape: 32 cores idling near load 4 (0.12 per core), where the
/// 4.8.0 incident's 26 was 0.81 per core. A start at 4 passes; 26 is refused at the start, and 26
/// arriving mid-run is a setup fault too, on the defaults alone.
#[test]
fn the_defaults_catch_the_incident_on_a_32_core_box_both_ways() {
    let c = case();
    let set = c.set(&[("answers", c.counts())]);

    let loaded = load_file(&c, "loaded", "26.00");
    let (out, text) = c.gate(
        &set,
        &[],
        &[
            ("GATE_LOADAVG_FILE", loaded.to_str().unwrap()),
            ("GATE_NCPU", "32"),
            ("GATE_MAX_LOAD_WAIT", "0"),
        ],
    );
    assert_eq!(out.status.code(), Some(2), "26 at the start:\n{text}");

    let load = load_file(&c, "loadavg", "4.00");
    let later = set_load_later(
        &load,
        "26.00",
        Some(c.dir.path().join("out/serve-pass-1.log")),
        Duration::ZERO,
    );
    let (out, text) = c.gate(
        &set,
        &[],
        &[
            ("GATE_LOADAVG_FILE", load.to_str().unwrap()),
            ("GATE_NCPU", "32"),
            ("GATE_LOAD_SAMPLE_SECS", "0.2"),
            ("GATE_LOAD_MAX_SHARE", "0.01"),
        ],
    );
    later.join().unwrap();
    assert!(
        text.contains("box load 4.00 over 32 cores"),
        "4 passes the start:\n{text}"
    );
    assert_eq!(out.status.code(), Some(2), "26 mid-run:\n{text}");
    assert!(text.contains("RESULT: SETUP FAULT"), "{text}");
}
