//! #1713 - the daily parity timer on Helsinki, and #1718 - the head mode it runs beside the sealed one.
//!
//! The wrapper (`deploy/parity/nuthatch-parity.sh`) runs against a fake `lodestar-parity.sh` whose
//! exit codes and output come from the state directory, and a fake `curl` that records each Discord
//! post. The head-mode tests run the real `scripts/lodestar-parity.sh` against a fake `curl` for the
//! nest and a `file://` gateway for the subgraph, so only the pin rule is exercised.

use std::path::{Path, PathBuf};
use std::process::Command;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn make_executable(p: &Path) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o755)).unwrap();
}

fn set_mode(p: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(p, std::fs::Permissions::from_mode(mode)).unwrap();
}

/// `<mode>-exits` holds the exit codes of successive runs, consumed one per call; `<mode>-out` is
/// printed after the PIN line.
const FAKE_PARITY: &str = r#"#!/usr/bin/env bash
mode=${PARITY_MODE:-sealed}
echo "$mode" >> "$FAKE_STATE/calls"
[ -n "${GRAPH_API_KEY:-}" ] || { echo "FAIL GRAPH_API_KEY is unset" >&2; exit 1; }
codes=$(cat "$FAKE_STATE/$mode-exits" 2>/dev/null || echo 0)
set -- $codes
code=${1:-0}
shift || true
echo "$*" > "$FAKE_STATE/$mode-exits"
if [ "$mode" = head ]; then pin=1000; else pin=900; fi
echo "PIN mode=$mode block=$pin version=4.2.0"
[ -f "$FAKE_STATE/$mode-out" ] && cat "$FAKE_STATE/$mode-out"
exit "$code"
"#;

const FAKE_CURL: &str = r#"#!/usr/bin/env bash
data=""
while [ $# -gt 0 ]; do
  case "$1" in -d|--data|--data-binary) data=$2; shift ;; esac
  shift
done
[ -f "$FAKE_STATE/curl-fail" ] && exit 22
printf '%s\n' "$data" >> "$FAKE_STATE/posts"
"#;

struct Rig {
    _root: tempfile::TempDir,
    path: String,
    state: PathBuf,
    logs: PathBuf,
    env_file: PathBuf,
    hook: PathBuf,
    parity: PathBuf,
}

fn rig() -> Rig {
    let root = tempfile::tempdir().unwrap();
    let fakes = root.path().join("fakes");
    let state = root.path().join("state");
    let logs = root.path().join("logs");
    for d in [&fakes, &state] {
        std::fs::create_dir_all(d).unwrap();
    }
    let curl = fakes.join("curl");
    std::fs::write(&curl, FAKE_CURL).unwrap();
    make_executable(&curl);
    let parity = root.path().join("lodestar-parity.sh");
    std::fs::write(&parity, FAKE_PARITY).unwrap();
    make_executable(&parity);
    let env_file = root.path().join("parity.env");
    std::fs::write(&env_file, "GRAPH_API_KEY=k3y\n").unwrap();
    set_mode(&env_file, 0o600);
    let hook = root.path().join("hook");
    std::fs::write(&hook, "https://discord.example/api/webhooks/1/x\n").unwrap();
    set_mode(&hook, 0o600);
    let path = format!("{}:{}", fakes.display(), std::env::var("PATH").unwrap());
    Rig {
        _root: root,
        path,
        state,
        logs,
        env_file,
        hook,
        parity,
    }
}

fn exits(r: &Rig, mode: &str, codes: &str) {
    std::fs::write(r.state.join(format!("{mode}-exits")), codes).unwrap();
}

fn out(r: &Rig, mode: &str, body: &str) {
    std::fs::write(r.state.join(format!("{mode}-out")), body).unwrap();
}

fn run(r: &Rig) -> (i32, String) {
    run_with(r, &[])
}

fn run_with(r: &Rig, extra: &[(&str, &str)]) -> (i32, String) {
    let mut cmd = Command::new("bash");
    cmd.arg(root().join("deploy/parity/nuthatch-parity.sh"))
        .env("PATH", &r.path)
        .env("FAKE_STATE", &r.state)
        .env("PARITY_SCRIPT", &r.parity)
        .env("PARITY_ENV_FILE", &r.env_file)
        .env("PARITY_WEBHOOK_FILE", &r.hook)
        .env("PARITY_LOG_DIR", &r.logs)
        .env("PARITY_HEAD_RETRY_SECS", "0")
        .env_remove("GRAPH_API_KEY")
        .env_remove("PARITY_MODES");
    for (k, v) in extra {
        cmd.env(k, v);
    }
    let o = cmd.output().expect("run nuthatch-parity.sh");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    );
    (o.status.code().unwrap_or(-1), text)
}

fn posts(r: &Rig) -> Vec<String> {
    std::fs::read_to_string(r.state.join("posts"))
        .unwrap_or_default()
        .lines()
        .map(str::to_string)
        .collect()
}

fn runs_tsv(r: &Rig) -> String {
    std::fs::read_to_string(r.logs.join("runs.tsv")).unwrap_or_default()
}

fn logs_matching(r: &Rig, suffix: &str) -> Vec<PathBuf> {
    std::fs::read_dir(&r.logs)
        .map(|d| {
            d.filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| p.to_string_lossy().ends_with(suffix))
                .collect()
        })
        .unwrap_or_default()
}

#[test]
fn a_disagreement_posts_one_line_naming_the_pin_version_and_comparison() {
    let r = rig();
    exits(&r, "sealed", "1");
    out(
        &r,
        "sealed",
        "subgraph _meta.block.number=950 pin=900\nlodestar_allocations nest=10 subgraph_isLegacy_false=11 DIFF\nFAIL subgraph comparison failed\n",
    );
    let (code, text) = run(&r);
    assert_eq!(code, 1, "{text}");
    let p = posts(&r);
    assert_eq!(p.len(), 1, "one post, got {p:?}\n{text}");
    let line = &p[0];
    assert!(line.starts_with("{\"content\":\""), "{line}");
    assert!(line.contains("sealed"), "{line}");
    assert!(line.contains("900"), "the pin is missing: {line}");
    assert!(line.contains("4.2.0"), "the version is missing: {line}");
    assert!(
        line.contains("lodestar_allocations nest=10 subgraph_isLegacy_false=11 DIFF"),
        "the failing comparison is missing: {line}"
    );
    assert_eq!(logs_matching(&r, "-sealed.log").len(), 1, "{text}");
    assert!(
        runs_tsv(&r).contains("\tsealed\t1\t900\t4.2.0"),
        "{}",
        runs_tsv(&r)
    );
}

#[test]
fn known_differences_are_logged_and_not_posted() {
    let r = rig();
    exits(&r, "sealed", "2");
    exits(&r, "head", "2");
    let (code, text) = run(&r);
    assert_eq!(code, 2, "{text}");
    assert!(posts(&r).is_empty(), "exit 2 posted: {:?}", posts(&r));
    let tsv = runs_tsv(&r);
    assert!(tsv.contains("\tsealed\t2\t900"), "{tsv}");
    assert!(tsv.contains("\thead\t2\t1000"), "{tsv}");
}

#[test]
fn both_modes_run_and_are_recorded() {
    let r = rig();
    let (code, text) = run(&r);
    assert_eq!(code, 0, "{text}");
    let calls = std::fs::read_to_string(r.state.join("calls")).unwrap();
    assert_eq!(calls, "sealed\nhead\n");
    assert_eq!(logs_matching(&r, "-sealed.log").len(), 1);
    assert_eq!(logs_matching(&r, "-head.log").len(), 1);
    let log = std::fs::read_to_string(&logs_matching(&r, "-head.log")[0]).unwrap();
    assert!(log.contains("PIN mode=head block=1000"), "{log}");
    assert!(log.contains("exit 0"), "the exit code is not kept: {log}");
    assert!(posts(&r).is_empty());
}

#[test]
fn a_head_that_cannot_be_reached_is_retried_then_recorded_unposted() {
    let r = rig();
    exits(&r, "head", "3 3 3");
    let (code, text) = run(&r);
    assert_eq!(code, 3, "{text}");
    assert!(posts(&r).is_empty(), "{:?}", posts(&r));
    let tsv = runs_tsv(&r);
    assert_eq!(tsv.matches("\thead\t3\t").count(), 3, "{tsv}");
    assert!(
        text.contains("could not be brought to the same head"),
        "{text}"
    );
}

#[test]
fn a_head_reached_on_retry_counts() {
    let r = rig();
    exits(&r, "head", "3 0");
    let (code, text) = run(&r);
    assert_eq!(code, 0, "{text}");
    let tsv = runs_tsv(&r);
    assert!(tsv.contains("\thead\t3\t"), "{tsv}");
    assert!(tsv.contains("\thead\t0\t"), "{tsv}");
}

#[test]
fn a_head_disagreement_pages_as_well() {
    let r = rig();
    exits(&r, "head", "1");
    out(
        &r,
        "head",
        "  total_rewards 10/11 epochs agree from 1195 DIFF\n",
    );
    let (code, text) = run(&r);
    assert_eq!(code, 1, "{text}");
    let p = posts(&r);
    assert_eq!(p.len(), 1, "{p:?}");
    assert!(p[0].contains("head"), "{}", p[0]);
    assert!(p[0].contains("1000"), "{}", p[0]);
    assert!(
        p[0].contains("total_rewards 10/11 epochs agree from 1195 DIFF"),
        "{}",
        p[0]
    );
}

/// A known difference printed before the real one must not be what the page names.
#[test]
fn the_page_names_the_disagreement_not_a_known_difference() {
    let r = rig();
    exits(&r, "sealed", "1");
    out(
        &r,
        "sealed",
        "    12 nest-only rows were not collected by GraphTallyCollector, which the subgraph does not record KNOWN-DIFF (#1114, #1983)\n  deposits nest=4 subgraph=5 matched=4 DIFF\n",
    );
    let (code, text) = run(&r);
    assert_eq!(code, 1, "{text}");
    let p = posts(&r);
    assert_eq!(p.len(), 1, "{p:?}");
    assert!(
        p[0].contains("deposits nest=4 subgraph=5 matched=4 DIFF"),
        "{}",
        p[0]
    );
    assert!(!p[0].contains("KNOWN-DIFF"), "{}", p[0]);
}

/// An exit the script does not define is not a pass and not a known difference.
#[test]
fn an_undefined_exit_status_pages() {
    let r = rig();
    exits(&r, "sealed", "137");
    let (code, text) = run(&r);
    assert_eq!(code, 1, "{text}");
    assert_eq!(posts(&r).len(), 1, "{text}");
    assert!(posts(&r)[0].contains("137"), "{:?}", posts(&r));
}

#[test]
fn a_missing_key_pages_rather_than_passing() {
    let r = rig();
    std::fs::write(&r.env_file, "NEST_URL=http://127.0.0.1:8107\n").unwrap();
    let (code, text) = run(&r);
    assert_eq!(code, 1, "{text}");
    assert!(text.contains("GRAPH_API_KEY"), "{text}");
    let p = posts(&r);
    assert_eq!(p.len(), 1, "{p:?}");
    assert!(p[0].contains("GRAPH_API_KEY"), "{}", p[0]);
    assert!(
        !r.state.join("calls").exists(),
        "the comparison ran without a key"
    );
}

#[test]
fn an_empty_mode_list_is_not_a_pass() {
    let r = rig();
    let (code, text) = run_with(&r, &[("PARITY_MODES", " ")]);
    assert_eq!(code, 1, "{text}");
    assert!(text.contains("PARITY_MODES is empty"), "{text}");
    assert_eq!(posts(&r).len(), 1, "{text}");
}

#[test]
fn a_readable_env_file_is_refused() {
    let r = rig();
    set_mode(&r.env_file, 0o644);
    let (code, text) = run(&r);
    assert_eq!(code, 1, "{text}");
    assert!(text.contains("0600"), "{text}");
    assert!(!r.state.join("calls").exists());
}

#[test]
fn the_key_never_reaches_the_log_or_the_post() {
    let r = rig();
    exits(&r, "sealed", "1");
    let (code, text) = run(&r);
    assert_eq!(code, 1, "{text}");
    assert_eq!(posts(&r).len(), 1, "{text}");
    assert!(!text.contains("k3y"), "{text}");
    for p in posts(&r) {
        assert!(!p.contains("k3y"), "{p}");
    }
    for l in logs_matching(&r, ".log") {
        let body = std::fs::read_to_string(&l).unwrap();
        assert!(!body.contains("k3y"), "{body}");
    }
}

#[test]
fn an_unreadable_webhook_is_loud() {
    let r = rig();
    exits(&r, "sealed", "1");
    std::fs::remove_file(&r.hook).unwrap();
    let (code, text) = run(&r);
    assert_eq!(code, 1, "{text}");
    assert!(text.contains("cannot read"), "{text}");
}

#[test]
fn a_failed_post_is_loud() {
    let r = rig();
    exits(&r, "sealed", "1");
    std::fs::write(r.state.join("curl-fail"), "").unwrap();
    let (code, text) = run(&r);
    assert_eq!(code, 1, "{text}");
    assert!(text.contains("could not post"), "{text}");
}

#[test]
fn a_quote_in_the_failing_line_still_makes_valid_json() {
    let r = rig();
    exits(&r, "sealed", "1");
    out(
        &r,
        "sealed",
        "FAIL nest at \"http://127.0.0.1:8107\" did not answer /ready\\n\n",
    );
    let (code, text) = run(&r);
    assert_eq!(code, 1, "{text}");
    let p = posts(&r);
    assert_eq!(p.len(), 1, "{p:?}");
    let v: serde_json::Value = serde_json::from_str(&p[0]).expect("the post is not JSON");
    assert!(
        v["content"]
            .as_str()
            .unwrap()
            .contains("\"http://127.0.0.1:8107\""),
        "{v}"
    );
}

#[test]
fn old_logs_are_pruned_and_new_ones_kept() {
    let r = rig();
    std::fs::create_dir_all(&r.logs).unwrap();
    let old = r.logs.join("20200101T000000Z-sealed.log");
    std::fs::write(&old, "exit 0\n").unwrap();
    let t = Command::new("touch")
        .args(["-t", "202001010000"])
        .arg(&old)
        .status()
        .unwrap();
    assert!(t.success());
    let (code, text) = run(&r);
    assert_eq!(code, 0, "{text}");
    assert!(!old.exists(), "a log past the retention survived");
    assert_eq!(logs_matching(&r, "-sealed.log").len(), 1);
}

// --- the head mode in scripts/lodestar-parity.sh (#1718) ---

const NEST_CURL: &str = r#"#!/usr/bin/env bash
for a in "$@"; do
  case "$a" in
    */ready) printf '{"ready":true,"version":"4.2.0","last_block":1000,"sealed_through":900}'; exit 0 ;;
    */sql) printf '{"columns":["n"],"rows":[{"n":5}]}'; exit 0 ;;
    */metrics)
      [ -n "${FAKE_NO_REORG_METRICS:-}" ] && { printf 'nuthatch_rows_sealed_total 3\n'; exit 0; }
      printf '# TYPE nuthatch_reorgs_total counter\nnuthatch_reorgs_total 2\nnuthatch_checkpoints_missed_total 0\n'; exit 0 ;;
  esac
done
exit 7
"#;

const NETWORK_SG: &str = "DZz4kDTdmzWLWsV373w2bSmoar3umKKH9y82SUKr5qmp";

fn parity(mode: Option<&str>, gateway_body: &str, extra: &[(&str, &str)]) -> (i32, String) {
    let root_dir = tempfile::tempdir().unwrap();
    let fakes = root_dir.path().join("fakes");
    std::fs::create_dir_all(&fakes).unwrap();
    let curl = fakes.join("curl");
    std::fs::write(&curl, NEST_CURL).unwrap();
    make_executable(&curl);
    let gw = root_dir.path().join("gw");
    let sg = gw.join(format!("api/k/subgraphs/id/{NETWORK_SG}"));
    std::fs::create_dir_all(sg.parent().unwrap()).unwrap();
    std::fs::write(&sg, gateway_body).unwrap();
    let mut cmd = Command::new("bash");
    cmd.arg(root().join("scripts/lodestar-parity.sh"))
        .env(
            "PATH",
            format!("{}:{}", fakes.display(), std::env::var("PATH").unwrap()),
        )
        .env("NEST_URL", "http://127.0.0.1:9")
        .env("GRAPH_API_KEY", "k")
        .env("GRAPH_GATEWAY", format!("file://{}", gw.display()))
        .env("PARITY_COMPARE", crate::parity_epoch_shift::compare_bin())
        .env_remove("PINNED_BLOCK")
        .env_remove("PARITY_MODE");
    if let Some(m) = mode {
        cmd.env("PARITY_MODE", m);
    }
    for (k, v) in extra {
        cmd.env(k, v);
    }
    let o = cmd.output().expect("run lodestar-parity.sh");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    );
    (o.status.code().unwrap_or(-1), text)
}

fn meta(block: u64) -> String {
    format!("{{\"data\":{{\"_meta\":{{\"block\":{{\"number\":{block}}}}}}}}}")
}

#[test]
fn head_mode_pins_at_last_block() {
    let (_, text) = parity(Some("head"), &meta(950), &[]);
    assert!(
        text.contains("PIN mode=head block=1000 version=4.2.0"),
        "{text}"
    );
}

#[test]
fn sealed_mode_still_pins_at_sealed_through() {
    let (_, text) = parity(None, &meta(850), &[]);
    assert!(
        text.contains("PIN mode=sealed block=900 version=4.2.0"),
        "{text}"
    );
}

/// The subgraph behind the nest's head is a comparison that did not happen: not a pass, and not a
/// disagreement worth a page.
#[test]
fn head_mode_with_the_subgraph_behind_is_not_measured() {
    let (code, text) = parity(Some("head"), &meta(950), &[]);
    assert_eq!(code, 3, "{text}");
    assert!(text.contains("NOT MEASURED"), "{text}");
}

/// The gateway can answer `_meta` from one indexer and the pinned query from another that is behind.
#[test]
fn head_mode_with_an_indexer_short_of_the_pin_is_not_measured() {
    let body = r#"{"errors":[{"message":"Failed to decode `block.number` value: `subgraph QmX has only indexed up to block number 990 and data for block number 1000 is therefore not yet available`"}]}"#;
    let (code, text) = parity(Some("head"), body, &[]);
    assert_eq!(code, 3, "{text}");
}

/// The sealed pin is hours old, so a subgraph short of it is a fault that pages, but it compared
/// nothing, so it is not run rather than a disagreement (#1818).
#[test]
fn sealed_mode_with_the_subgraph_behind_is_not_run() {
    let (code, text) = parity(None, &meta(850), &[]);
    assert_eq!(code, 4, "{text}");
    assert!(text.contains("NOT RUN"), "{text}");
    assert!(text.contains("below pin 900"), "{text}");
}

#[test]
fn head_mode_refuses_an_explicit_pin() {
    let (code, text) = parity(Some("head"), &meta(2000), &[("PINNED_BLOCK", "850")]);
    assert_eq!(code, 1, "{text}");
    assert!(
        text.contains("PINNED_BLOCK cannot be combined with PARITY_MODE=head"),
        "{text}"
    );
}

#[test]
fn head_mode_needs_the_reorg_counters() {
    let (code, text) = parity(Some("head"), &meta(2000), &[("FAKE_NO_REORG_METRICS", "1")]);
    assert_eq!(code, 1, "{text}");
    assert!(text.contains("nuthatch_reorgs_total"), "{text}");
}

#[test]
fn an_unknown_mode_is_refused() {
    let (code, text) = parity(Some("tip"), &meta(2000), &[]);
    assert_eq!(code, 1, "{text}");
    assert!(text.contains("PARITY_MODE"), "{text}");
}

// --- #1818: the subgraph side failing to answer is a rig fault, not a disagreement ---

/// The wrapper names the reason and says the run did not happen; it does not say the sides disagree.
#[test]
fn a_rig_fault_pages_not_run_with_the_reason() {
    let r = rig();
    exits(&r, "sealed", "4");
    out(
        &r,
        "sealed",
        "subgraph _meta.block.number=950 pin=900\nNOT RUN subgraph graphql error: [{'message': 'auth error: API key not found'}]\nFAIL later noise\n",
    );
    let (code, text) = run(&r);
    assert_eq!(code, 4, "{text}");
    let p = posts(&r);
    assert_eq!(p.len(), 1, "{p:?}\n{text}");
    assert!(p[0].contains("PARITY NOT RUN (sealed): "), "{}", p[0]);
    assert!(p[0].contains("auth error: API key not found"), "{}", p[0]);
    assert!(!p[0].contains("PARITY FAIL"), "{}", p[0]);
    assert!(
        runs_tsv(&r).contains("\tsealed\t4\t900"),
        "{}",
        runs_tsv(&r)
    );
}

/// A head-mode rig fault pages too, and is not retried as a head that could not be reached.
#[test]
fn a_head_rig_fault_pages_and_is_not_retried() {
    let r = rig();
    exits(&r, "head", "4 0");
    out(&r, "head", "NOT RUN subgraph HTTP 500: upstream exploded\n");
    let (code, text) = run(&r);
    assert_eq!(code, 4, "{text}");
    let p = posts(&r);
    assert_eq!(p.len(), 1, "{p:?}");
    assert!(p[0].contains("PARITY NOT RUN (head): "), "{}", p[0]);
    assert!(p[0].contains("HTTP 500"), "{}", p[0]);
    assert_eq!(
        runs_tsv(&r).matches("\thead\t").count(),
        1,
        "{}",
        runs_tsv(&r)
    );
}

/// A disagreement in one mode outranks a rig fault in the other.
#[test]
fn a_disagreement_outranks_a_rig_fault() {
    let r = rig();
    exits(&r, "sealed", "1");
    out(
        &r,
        "sealed",
        "lodestar_allocations nest=10 subgraph_isLegacy_false=11 DIFF\n",
    );
    exits(&r, "head", "4");
    out(&r, "head", "NOT RUN subgraph HTTP 502: bad gateway\n");
    let (code, text) = run(&r);
    assert_eq!(code, 1, "{text}");
    let p = posts(&r);
    assert_eq!(p.len(), 2, "{p:?}");
    assert!(p[0].contains("PARITY FAIL (sealed)"), "{}", p[0]);
    assert!(!p[0].contains("NOT RUN"), "{}", p[0]);
    assert!(p[1].contains("PARITY NOT RUN (head)"), "{}", p[1]);
}

/// Answers each request with the first route whose needle appears in its request line, headers or
/// body; anything unmatched is a 404.
fn serve(routes: Vec<(&'static str, u16, String)>) -> String {
    use std::io::{BufRead, BufReader, Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let mut reader = BufReader::new(stream);
            let mut req = String::new();
            let mut len = 0usize;
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                    break;
                }
                if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    len = v.trim().parse().unwrap_or(0);
                }
                req.push_str(&line);
            }
            let mut body = vec![0u8; len];
            let _ = reader.read_exact(&mut body);
            req.push_str(&String::from_utf8_lossy(&body));
            let (status, answer) = routes
                .iter()
                .find(|(needle, _, _)| req.contains(needle))
                .map(|(_, s, b)| (*s, b.clone()))
                .unwrap_or((404, "no route".into()));
            let mut stream = reader.into_inner();
            let _ = write!(
                stream,
                "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{answer}",
                answer.len()
            );
        }
    });
    format!("http://{addr}")
}

#[test]
fn an_auth_error_is_a_rig_fault_not_a_disagreement() {
    let body = r#"{"errors":[{"message":"auth error: API key not found"}]}"#;
    for mode in [None, Some("head")] {
        let (code, text) = parity(mode, body, &[]);
        assert_eq!(code, 4, "{mode:?}: {text}");
        assert!(text.contains("NOT RUN"), "{mode:?}: {text}");
        assert!(
            text.contains("auth error: API key not found"),
            "{mode:?}: {text}"
        );
        assert!(
            !text.contains("subgraph comparison failed"),
            "{mode:?}: {text}"
        );
    }
}

#[test]
fn an_http_500_from_the_gateway_is_a_rig_fault() {
    let gw = serve(vec![("", 500, "upstream exploded".into())]);
    let (code, text) = parity(None, "", &[("GRAPH_GATEWAY", &gw)]);
    assert_eq!(code, 4, "{text}");
    assert!(text.contains("NOT RUN"), "{text}");
    assert!(text.contains("HTTP 500"), "{text}");
}

#[test]
fn a_gateway_that_does_not_answer_is_a_rig_fault() {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let gw = format!("http://127.0.0.1:{port}");
    let (code, text) = parity(None, "", &[("GRAPH_GATEWAY", &gw)]);
    assert_eq!(code, 4, "{text}");
    assert!(text.contains("NOT RUN"), "{text}");
}

#[test]
fn a_malformed_answer_is_a_rig_fault() {
    let (code, text) = parity(None, "<html>502 Bad Gateway</html>", &[]);
    assert_eq!(code, 4, "{text}");
    assert!(text.contains("NOT RUN"), "{text}");
    assert!(text.contains("malformed"), "{text}");
}

/// The file gateway only answers for the key `k`, so another key's error names the path it missed.
#[test]
fn the_key_never_reaches_a_rig_fault_reason() {
    let (code, text) = parity(None, &meta(2000), &[("GRAPH_API_KEY", "s3cretkey")]);
    assert_eq!(code, 4, "{text}");
    assert!(text.contains("NOT RUN"), "{text}");
    assert!(!text.contains("s3cretkey"), "{text}");
}

/// A disagreement already seen stays a disagreement when the gateway fails later in the run.
#[test]
fn a_real_disagreement_still_fails_even_if_the_gateway_then_fails() {
    let gw = serve(vec![
        ("_meta", 200, meta(2000)),
        (
            "allocations(",
            200,
            r#"{"data":{"allocations":[{"id":"0x1"}]}}"#.into(),
        ),
        (
            "lodestar_disputes",
            200,
            r#"{"columns":["id"],"rows":[]}"#.into(),
        ),
        ("", 500, "upstream exploded".into()),
    ]);
    let (code, text) = parity(None, "", &[("GRAPH_GATEWAY", &gw), ("NEST_URL", &gw)]);
    assert_eq!(code, 1, "{text}");
    assert!(
        text.contains("lodestar_allocations nest=5 subgraph_isLegacy_false=1 DIFF"),
        "{text}"
    );
}
