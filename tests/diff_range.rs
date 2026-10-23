//! The range story, `nuthatch diff` without `--flow` (RFC-0071 §15 step 3), over the Euler fixture and
//! a control-plane fixture: goldens, the uncapped control plane, `--since-last`, the three surfaces
//! giving one document, and the reorg property for a range above the sealed boundary.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use crate::diff_flow::{assert_golden, copy_dir, diff, nest as euler_nest};

const DAI_TX: &str = "0xc310a0affe2169d1f6feec1c63dbc7f7c62a887fa48795d327d4d2da2d6b111d";

/// RFC-0071 step 3's control-plane fixture: mainnet blocks 26,241,000 to 26,243,100, sealed.
const START: u64 = 26_241_000;
const LAST: u64 = 26_243_100;
/// The transaction that deploys an upgradeable system: Upgraded, OwnershipTransferred, RoleGranted,
/// RoleRevoked.
const DEPLOY_BLOCK: &str = "26243031";
const DEPLOY_TX: &str = "0x639e88f0ffa722a7e871fe13dff7e3f9e998f3e7107c1af44b9aba1d4baf7e2e";
const LATER: &[&str] = &["--from", "26242000", "--to", "26243100"];

fn control_fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/diff/control")
}

/// The control-plane fixture with a store whose watermarks say `sealed` and `last`.
fn control_nest(sealed: u64, last: u64) -> tempfile::TempDir {
    let tmp = tempfile::tempdir().unwrap();
    for f in [
        "nuthatch.toml",
        "metadata.json",
        "schema.json",
        "llms.txt",
        "semantic.toml",
    ] {
        std::fs::copy(control_fixture().join(f), tmp.path().join(f)).unwrap();
    }
    for d in ["abis", "segments", "views", "labels"] {
        if control_fixture().join(d).is_dir() {
            copy_dir(&control_fixture().join(d), &tmp.path().join(d));
        }
    }
    let store = nuthatch::store::Store::open(&tmp.path().join("nuthatch.redb")).unwrap();
    store
        .set_metas(
            &[
                ("start_block", &START.to_string()),
                ("last_block", &last.to_string()),
                ("sealed_through", &sealed.to_string()),
            ],
            &[],
        )
        .unwrap();
    tmp
}

fn golden(name: &str, got: &str) {
    assert_golden(&format!("../../control/golden/{name}"), got);
}

fn json(out: &str) -> serde_json::Value {
    serde_json::from_str(out).unwrap_or_else(|e| panic!("{e}: {out}"))
}

#[test]
fn without_flow_the_story_is_told() {
    let nest = euler_nest();
    let (ok, out, err) = diff(nest.path(), &["--tx", DAI_TX, "--block", "16817996"]);
    assert!(ok, "{err}");
    assert_golden("story-dai-tx.txt", &out);
    for (format, ext) in [("markdown", "md"), ("json", "json")] {
        let (ok, out, err) = diff(
            nest.path(),
            &["--tx", DAI_TX, "--block", "16817996", "--format", format],
        );
        assert!(ok, "{format}: {err}");
        assert_golden(&format!("story-dai-tx.{ext}"), &out);
    }
    let (ok, out, err) = diff(nest.path(), &["--from", "16818024", "--to", "16818085"]);
    assert!(ok, "{err}");
    assert_golden("story-range.txt", &out);
}

#[test]
fn the_control_plane_fixture_matches_its_goldens() {
    let nest = control_nest(LAST, LAST);
    let (ok, out, err) = diff(nest.path(), LATER);
    assert!(ok, "{err}");
    golden("later.txt", &out);
    for (format, ext) in [("markdown", "md"), ("json", "json")] {
        let (ok, out, err) = diff(nest.path(), &[LATER, &["--format", format]].concat());
        assert!(ok, "{format}: {err}");
        golden(&format!("later.{ext}"), &out);
    }
    let (ok, out, err) = diff(nest.path(), &["--tx", DEPLOY_TX, "--block", DEPLOY_BLOCK]);
    assert!(ok, "{err}");
    golden("deploy-tx.txt", &out);
    let (ok, out, err) = diff(
        nest.path(),
        &["--from", &START.to_string(), "--to", &LAST.to_string()],
    );
    assert!(ok, "{err}");
    golden("whole.txt", &out);
}

fn rows_in(doc: &serde_json::Value, section: &str) -> u64 {
    doc["tables"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|t| t["section"] == section)
        .map(|t| t["rows"].as_u64().unwrap())
        .sum()
}

/// §4: a diff that hid an `Upgraded` under a cap would fail its main job. With `--top 1` every other
/// list is cut to one item, and the control plane still holds every control-plane row in range.
#[test]
fn the_control_plane_is_never_capped() {
    let nest = control_nest(LAST, LAST);
    let whole = ["--from", "26241000", "--to", "26243100", "--format", "json"];
    let (ok, out, err) = diff(nest.path(), &[&whole[..], &["--top", "1"]].concat());
    assert!(ok, "{err}");
    let doc = json(&out);
    let control = doc["control"].as_array().unwrap();
    assert!(control.len() > 1, "the fixture holds more than one");
    assert_eq!(control.len() as u64, rows_in(&doc, "control"));
    for kind in [
        "paused",
        "unpaused",
        "upgraded",
        "admin_changed",
        "ownership_transferred",
        "role_granted",
        "role_revoked",
        "role_admin_changed",
    ] {
        assert!(
            control.iter().any(|c| c["event"] == kind),
            "{kind} missing from {control:#?}"
        );
    }
    let mut at: Vec<(u64, u64)> = control
        .iter()
        .map(|c| {
            (
                c["block"].as_u64().unwrap(),
                c["log_index"].as_u64().unwrap(),
            )
        })
        .collect();
    let sorted = {
        let mut s = at.clone();
        s.sort();
        s
    };
    assert_eq!(at, sorted, "control-plane events are in chain order");
    at.dedup();
    for t in doc["movements"].as_array().unwrap() {
        assert!(t["largest"].as_array().unwrap().len() <= 1);
        assert!(t["net"].as_array().unwrap().len() <= 1);
    }
    for a in doc["approvals"].as_array().unwrap() {
        assert!(a["listed"].as_array().unwrap().len() <= 1);
    }
    // And the text says so too: one line per control-plane event, whatever `--top` is.
    let (ok, text, err) = diff(
        nest.path(),
        &["--from", "26241000", "--to", "26243100", "--top", "1"],
    );
    assert!(ok, "{err}");
    let lines = text
        .lines()
        .filter(|l| l.starts_with("  block ") && l.contains("(tx 0x"))
        .count();
    assert_eq!(lines, control.len(), "{text}");
}

#[test]
fn every_row_is_in_one_section_and_unlimited_approvals_are_named() {
    let nest = control_nest(LAST, LAST);
    let (ok, out, err) = diff(nest.path(), &[LATER, &["--format", "json"]].concat());
    assert!(ok, "{err}");
    let doc = json(&out);
    let sum: u64 = doc["tables"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["rows"].as_u64().unwrap())
        .sum();
    assert_eq!(sum, doc["rows_total"].as_u64().unwrap());
    let events: u64 = doc["events"]["tables"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["rows"].as_u64().unwrap())
        .sum();
    assert_eq!(events, rows_in(&doc, "events"));
    let link = doc["approvals"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["alias"] == "link")
        .expect("LINK approvals in range");
    let unlimited = link["unlimited"].as_u64().unwrap();
    assert!(unlimited > 0, "the fixture holds unlimited LINK approvals");
    for a in link["listed"].as_array().unwrap() {
        let max = a["amount"] == alloy_primitives::U256::MAX.to_string();
        assert_eq!(a["unlimited"], max);
        let text = a["text"].as_str().unwrap();
        assert_eq!(
            text.contains("any amount of LINK (unlimited: 2^256-1)"),
            max,
            "{text}"
        );
    }
}

fn state_dir_files(state: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![state.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).into_iter().flatten().flatten() {
            if e.file_type().unwrap().is_dir() {
                stack.push(e.path());
            } else {
                out.push(e.path());
            }
        }
    }
    out.sort();
    out
}

fn since_last(dir: &Path, state: &Path) -> (bool, String, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_nuthatch"))
        .args(["diff", "--since-last", "--format", "json", "--dir"])
        .arg(dir)
        .args(["--url", "http://127.0.0.1:9"])
        .env("XDG_STATE_HOME", state)
        .output()
        .unwrap();
    (
        out.status.success(),
        String::from_utf8(out.stdout).unwrap(),
        String::from_utf8(out.stderr).unwrap(),
    )
}

/// §3: the cursor lives in the operator's state directory keyed by data identity, never in the
/// nest directory, so using it leaves the NID alone; and each run starts where the last one ended
/// and stops at the sealed boundary, so blocks above it are never skipped.
#[test]
fn since_last_keeps_its_cursor_outside_the_nest_and_runs_on_from_it() {
    let nest = control_nest(26_242_000, LAST);
    let state = tempfile::tempdir().unwrap();
    let before = nuthatch::blob::build_manifest(nest.path(), None).unwrap();
    let listing = |d: &Path| state_dir_files(d);
    let files_before = listing(nest.path());

    let (ok, out, err) = since_last(nest.path(), state.path());
    assert!(ok, "{err}");
    let doc = json(&out);
    assert_eq!(doc["request"]["from"], START);
    assert_eq!(doc["request"]["to"], 26_242_000);
    assert_eq!(doc["coverage"]["final"], true);

    let cursor = state
        .path()
        .join("nuthatch/diff")
        .join(before.data_identity());
    assert_eq!(listing(state.path()), vec![cursor.clone()]);
    assert_eq!(std::fs::read_to_string(&cursor).unwrap(), "26242000\n");
    assert_eq!(
        listing(nest.path()),
        files_before,
        "the nest directory changed"
    );
    let after = nuthatch::blob::build_manifest(nest.path(), None).unwrap();
    assert_eq!(after.nid(), before.nid(), "the NID moved");

    // Nothing sealed since: nothing printed, and the cursor stays.
    let (ok, out, err) = since_last(nest.path(), state.path());
    assert!(ok, "{err}");
    assert!(out.is_empty(), "{out}");
    assert!(err.contains("Nothing sealed since the last diff"), "{err}");

    // The seal moves on: the next diff begins at the block after the last one ended.
    let store = nuthatch::store::Store::open(&nest.path().join("nuthatch.redb")).unwrap();
    store
        .set_metas(&[("sealed_through", "26242500")], &[])
        .unwrap();
    drop(store);
    let (ok, out, err) = since_last(nest.path(), state.path());
    assert!(ok, "{err}");
    let doc = json(&out);
    assert_eq!(doc["request"]["from"], 26_242_001);
    assert_eq!(doc["request"]["to"], 26_242_500);
    assert_eq!(std::fs::read_to_string(&cursor).unwrap(), "26242500\n");
}

#[test]
fn final_only_refuses_a_range_above_the_sealed_boundary() {
    let nest = control_nest(26_243_000, LAST);
    let (ok, out, err) = diff(nest.path(), &[LATER, &["--final-only"]].concat());
    assert!(!ok, "{out}");
    assert!(out.is_empty());
    assert!(
        err.contains("--final-only: blocks 26,243,001 to 26,243,100 are not final"),
        "{err}"
    );
    let (ok, out, err) = diff(nest.path(), &[LATER, &["--format", "json"]].concat());
    assert!(ok, "{err}");
    assert_eq!(json(&out)["coverage"]["final"], false);
    let (ok, out, err) = diff(nest.path(), LATER);
    assert!(ok, "{err}");
    assert!(
        out.contains("Provisional: blocks 26,243,001 to 26,243,100 are not final"),
        "{out}"
    );
}

/// A nest with a threshold configured: the flags raised in range are counted and listed, and a range
/// with none says so rather than leaving the line out.
#[test]
fn threshold_flags_raised_in_range_are_listed() {
    let nest = control_nest(LAST, LAST + 2);
    let toml = nest.path().join("nuthatch.toml");
    let cfg = std::fs::read_to_string(&toml).unwrap();
    std::fs::write(&toml, format!("{cfg}\n[flags]\nthreshold = \"1000\"\n")).unwrap();
    let store = nuthatch::store::Store::open_existing(&nest.path().join("nuthatch.redb")).unwrap();
    let flag = |b: u64, log: u64, value: &str| {
        (
            format!("{b:012}-{log:06}-thr"),
            serde_json::json!({
                "table": "threshold_flag", "kind": "threshold_flag", "rule": "threshold",
                "block_number": b, "log_index": log, "tx_hash": format!("0x{:064x}", b * 100 + log),
                "from": "0x28c6c06298d514db089934071355e5743bf21d60",
                "to": "0x00000000000000000000000000000000000000bb",
                "value": value, "threshold": "1000",
            })
            .to_string(),
        )
    };
    store
        .commit_window(
            &[flag(LAST + 1, 3, "1500"), flag(LAST + 2, 9, "123456789")],
            None,
            LAST + 2,
        )
        .unwrap();
    drop(store);
    let range = ["--from", "26243050", "--to", "26243102"];
    let (ok, out, err) = diff(nest.path(), &range);
    assert!(ok, "{err}");
    assert!(
        out.contains("  Threshold flags at 1,000 base units: 2 in range\n"),
        "{out}"
    );
    assert!(
        out.contains(
            "a transfer of 123,456,789 base units from 0x28c6c06298d514db089934071355e5743bf21d60 \
             [label exchange] to 0x00000000000000000000000000000000000000bb, at or over the \
             threshold of 1,000"
        ),
        "{out}"
    );
    let (ok, out, err) = diff(nest.path(), &["--from", "26243050", "--to", "26243100"]);
    assert!(ok, "{err}");
    assert!(
        out.contains("  Threshold flags at 1,000 base units: 0 in range\n"),
        "{out}"
    );
}

// ---------------------------------------------------------------------------------------------------
// The three surfaces.

pub(crate) fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// One HTTP/1.0 GET: the body as sent, and the status.
pub(crate) fn http_get(port: u16, path: &str) -> std::io::Result<(u16, String)> {
    let mut s = std::net::TcpStream::connect(("127.0.0.1", port))?;
    write!(s, "GET {path} HTTP/1.0\r\nHost: 127.0.0.1:{port}\r\n\r\n")?;
    let mut raw = Vec::new();
    s.read_to_end(&mut raw)?;
    let raw = String::from_utf8(raw).unwrap();
    let (head, body) = raw.split_once("\r\n\r\n").unwrap_or((&raw, ""));
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    Ok((status, body.to_string()))
}

struct Served {
    child: std::process::Child,
    port: u16,
    _dir: tempfile::TempDir,
}

impl Drop for Served {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn serve(dir: tempfile::TempDir) -> Served {
    let port = free_port();
    let child = Command::new(env!("CARGO_BIN_EXE_nuthatch"))
        .args(["serve", "--dir"])
        .arg(dir.path())
        .args(["--listen", &format!("127.0.0.1:{port}")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let served = Served {
        child,
        port,
        _dir: dir,
    };
    for _ in 0..600 {
        if http_get(port, "/health").is_ok_and(|(s, _)| s == 200) {
            return served;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    panic!("serve did not come up on {port}");
}

/// `tools/call diff` through `nuthatch mcp`, returning the tool's text.
pub(crate) fn mcp_diff(port: u16, args: serde_json::Value) -> String {
    let mut child = Command::new(env!("CARGO_BIN_EXE_nuthatch"))
        .args(["mcp", "--url", &format!("http://127.0.0.1:{port}")])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let call = serde_json::json!({
        "jsonrpc": "2.0", "id": 2, "method": "tools/call",
        "params": { "name": "diff", "arguments": args },
    });
    let mut stdin = child.stdin.take().unwrap();
    writeln!(stdin, "{call}").unwrap();
    drop(stdin);
    let out = child.wait_with_output().unwrap();
    let line = String::from_utf8(out.stdout).unwrap();
    let resp: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(resp["result"]["isError"], false, "{resp:#}");
    resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap()
        .to_string()
}

pub(crate) fn cli_over_http(port: u16, args: &[&str]) -> String {
    let empty = tempfile::tempdir().unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_nuthatch"))
        .arg("diff")
        .arg("--dir")
        .arg(empty.path())
        .args(["--url", &format!("http://127.0.0.1:{port}")])
        .args(args)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

/// §3: the CLI's `--format json`, `GET /diff` and the MCP tool give one document, byte for byte.
#[test]
fn the_cli_the_route_and_the_mcp_tool_give_the_same_document() {
    let local = control_nest(26_243_000, LAST);
    let served = serve(control_nest(26_243_000, LAST));
    let cases: Vec<(Vec<&str>, String, serde_json::Value)> = vec![
        (
            LATER.to_vec(),
            "/diff?from=26242000&to=26243100".into(),
            serde_json::json!({ "from": 26242000, "to": 26243100 }),
        ),
        (
            vec!["--tx", DEPLOY_TX, "--block", DEPLOY_BLOCK, "--top", "3"],
            format!("/diff?tx={DEPLOY_TX}&block={DEPLOY_BLOCK}&top=3"),
            serde_json::json!({ "tx": DEPLOY_TX, "block": 26243031, "top": 3 }),
        ),
        (
            vec!["--flow", "--block", DEPLOY_BLOCK],
            format!("/diff?flow=true&block={DEPLOY_BLOCK}"),
            serde_json::json!({ "flow": true, "block": 26243031 }),
        ),
    ];
    for (args, path, mcp_args) in cases {
        let (ok, cli, err) = diff(local.path(), &[&args[..], &["--format", "json"]].concat());
        assert!(ok, "{args:?}: {err}");
        let (status, route) = http_get(served.port, &path).unwrap();
        assert_eq!(status, 200, "{path}: {route}");
        assert_eq!(route, cli, "{path}: the route and the CLI differ");
        let tool = mcp_diff(served.port, mcp_args);
        assert_eq!(tool, cli, "{path}: the MCP tool and the CLI differ");
    }
    // The CLI pointed at the instance prints the route's document, and its text is the local text.
    let over = cli_over_http(served.port, &[LATER, &["--format", "json"]].concat());
    let (_, cli, _) = diff(local.path(), &[LATER, &["--format", "json"]].concat());
    assert_eq!(over, cli);
    let over = cli_over_http(served.port, LATER);
    let (_, cli, _) = diff(local.path(), LATER);
    assert_eq!(over, cli);
}

#[test]
fn the_route_refuses_what_it_cannot_answer() {
    let served = serve(control_nest(26_243_000, LAST));
    for (path, why) in [
        (format!("/diff?tx={DEPLOY_TX}"), "pass block"),
        (
            "/diff?from=1&to=2".to_string(),
            "outside what this nest has indexed",
        ),
        (
            "/diff?from=26242000&to=26243100&final_only=true".to_string(),
            "--final-only",
        ),
        (
            "/diff?block=26243031&format=dot".to_string(),
            "draw the flow",
        ),
        (
            "/diff?block=26243031&top=9223372036854775808".to_string(),
            "the most items per list",
        ),
    ] {
        let (status, body) = http_get(served.port, &path).unwrap();
        assert_eq!(status, 400, "{path}: {body}");
        assert!(body.contains(why), "{path}: {body}");
    }
}

// ---------------------------------------------------------------------------------------------------
// Reorgs above the sealed boundary.

/// The rows a diff reads, through the same engine `nuthatch sql` uses on a local store.
struct Local<'a> {
    dir: &'a Path,
    store: &'a nuthatch::store::Store,
    tables: &'a [nuthatch::registry::TableSchema],
}

impl nuthatch::diff::Source for Local<'_> {
    async fn query(&self, sql: &str) -> anyhow::Result<nuthatch::analytics::QueryOutput> {
        let hot = self.store.hot_rows_by_table()?;
        nuthatch::analytics::query_hot_cold(
            self.dir,
            sql,
            nuthatch::analytics::QueryGuard {
                timeout: std::time::Duration::from_secs(30),
                max_rows: 50_000,
            },
            &hot,
            self.store.sealed_through(),
            self.tables,
        )
    }
}

async fn story_json(dir: &Path, store: &nuthatch::store::Store, from: u64, to: u64) -> String {
    use nuthatch::diff;
    let cfg = nuthatch::config::Config::load(dir).unwrap();
    let reg = nuthatch::registry::from_nest(dir, &cfg).unwrap();
    let tables = nuthatch::indexer::full_schema(&reg, &cfg);
    let id = nuthatch::blob::build_manifest(dir, None)
        .unwrap()
        .data_identity();
    let ctx =
        diff::NestContext::gather(dir, store, diff::Facts::from_config(&cfg, Some(id))).unwrap();
    let (stories, warnings) = diff::stories(dir, &tables).unwrap();
    let ask = diff::Ask {
        req: diff::Request { from, to, tx: None },
        mode: diff::Mode::Story,
        top: 10,
        final_only: false,
    };
    let src = Local {
        dir,
        store,
        tables: &tables,
    };
    let inputs = diff::Inputs {
        tables: &tables,
        ctx: &ctx,
        stories: &stories,
        warnings,
        history: None,
    };
    diff::produce(&src, inputs, &ask)
        .await
        .unwrap()
        .render(diff::Format::Json, 10)
        .unwrap()
}

const LINK: &str = "0x514910771af9ca656af840dff83e8264ecf986ca";
const PAUSABLE: &str = "0xebcb31a8465d6130c744c41060e7dcbfa17d1d8a";

/// Block `b`'s rows on fork `fork`: LINK transfers and an approval, and on some blocks a `Paused`.
fn block_rows(b: u64, fork: u64) -> Vec<(String, String)> {
    let hash = format!("0x{:062x}{fork:02x}", b);
    let addr = |n: u64| format!("0x{:040x}", 0x1000 + (n % 7) + fork * 16);
    let mut rows = Vec::new();
    for log in 0..(1 + (b + fork) % 3) {
        rows.push((
            format!("{b:012}-{log:06}"),
            serde_json::json!({
                "table": "link__transfer", "block_number": b, "block_hash": hash,
                "tx_hash": format!("0x{:060x}{log:04x}", b), "log_index": log, "_seq": log,
                "address": LINK, "from": addr(b + log), "to": addr(b + log + 3),
                "value": ((b % 1000) * 1_000_000_000_000_000 + log + fork).to_string(),
            })
            .to_string(),
        ));
    }
    rows.push((
        format!("{b:012}-{:06}", 10),
        serde_json::json!({
            "table": "link__approval", "block_number": b, "block_hash": hash,
            "tx_hash": format!("0x{:060x}{:04x}", b, 10), "log_index": 10, "_seq": 10,
            "address": LINK, "owner": addr(b), "spender": addr(b + 1),
            "value": if (b + fork).is_multiple_of(2) { alloy_primitives::U256::MAX.to_string() } else { "5".into() },
        })
        .to_string(),
    ));
    if (b + fork).is_multiple_of(4) {
        rows.push((
            format!("{b:012}-{:06}", 11),
            serde_json::json!({
                "table": "pausable__paused", "block_number": b, "block_hash": hash,
                "tx_hash": format!("0x{:060x}{:04x}", b, 11), "log_index": 11, "_seq": 11,
                "address": PAUSABLE, "account": addr(b),
            })
            .to_string(),
        ));
    }
    rows
}

fn commit(store: &nuthatch::store::Store, blocks: impl IntoIterator<Item = (u64, u64)>) {
    let mut last = 0;
    let mut rows = Vec::new();
    for (b, fork) in blocks {
        rows.extend(block_rows(b, fork));
        last = b;
    }
    store.commit_window(&rows, None, last).unwrap();
}

const TIP: u64 = LAST + 12;

proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig { cases: 12, ..Default::default() })]

    /// Blocks above the sealed boundary live in the hot store. After a reorg of any depth there,
    /// the story of a range crossing the boundary equals the one told from scratch on a store that
    /// only ever held the new canonical chain, and it is labelled provisional.
    #[test]
    fn a_reorg_above_the_sealed_boundary_tells_the_canonical_story(depth in 1u64..=12, fork in 1u64..=3) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let reorged = control_nest(LAST, TIP);
        let store = nuthatch::store::Store::open_existing(&reorged.path().join("nuthatch.redb")).unwrap();
        commit(&store, (LAST + 1..=TIP).map(|b| (b, 0)));
        let fork_at = TIP - depth;
        store.rollback_to(fork_at).unwrap();
        commit(&store, (fork_at + 1..=TIP).map(|b| (b, fork)));

        let fresh = control_nest(LAST, TIP);
        let canon = nuthatch::store::Store::open_existing(&fresh.path().join("nuthatch.redb")).unwrap();
        commit(&canon, (LAST + 1..=TIP).map(|b| (b, if b > fork_at { fork } else { 0 })));

        let from = LAST - 50;
        let got = rt.block_on(story_json(reorged.path(), &store, from, TIP));
        let want = rt.block_on(story_json(fresh.path(), &canon, from, TIP));
        proptest::prop_assert_eq!(&got, &want);
        let doc = json(&got);
        proptest::prop_assert_eq!(&doc["coverage"]["final"], &serde_json::json!(false));
        let hot_rows: u64 = (LAST + 1..=TIP)
            .map(|b| block_rows(b, if b > fork_at { fork } else { 0 }).len() as u64)
            .sum();
        proptest::prop_assert!(doc["rows_total"].as_u64().unwrap() >= hot_rows);
    }
}

#[test]
fn a_label_seen_only_in_an_ordinary_table_is_listed() {
    let nest = euler_nest();
    let steth = "0xae7ab96520de3a18e5e111b5eaab095312d7fe84";
    std::fs::create_dir_all(nest.path().join("labels")).unwrap();
    std::fs::write(
        nest.path().join("labels/test.json"),
        format!(r#"[{{"address":"{steth}","label":"lido"}}]"#),
    )
    .unwrap();
    let (ok, out, err) = diff(
        nest.path(),
        &["--from", "16818024", "--to", "16818085", "--format", "json"],
    );
    assert!(ok, "{err}");
    let doc = json(&out);
    let listed: Vec<&str> = doc["labels"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|l| l["address"].as_str())
        .collect();
    assert_eq!(listed, [steth], "{}", doc["labels"]);
}
