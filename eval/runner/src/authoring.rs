//! RFC-0017's authoring evaluation: can an agent build a working nest from nothing?
//!
//! RFC-0016 §1 measures **runtime** knowledge - an agent with MCP tools querying a nest that already
//! exists. This measures **authoring** knowledge: the builder skill plus a shell, a contract address,
//! and nothing else. RFC-0017 fixes the three criteria and this file does not reinvent them: `init`
//! succeeds, `dev` reaches the pinned tip, one canned question answers correctly - "scored
//! mechanically (exit codes + result comparison)". Nothing here reads the agent's prose.
//!
//! The board is proven before anyone plays on it: `tests/authoring_eval_board.rs` walks this same
//! scenario with a scripted reference solution and must be green in CI first.

use crate::proc::{self, returncode};
use crate::pyjson::{self, float, float_repr, Style};
use crate::{head, parse_py_float, py_format, py_list_repr, quote_plus, root, tail, Checks};
use clap::Parser;
use serde_json::{json, Map, Value};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn scenario_path() -> PathBuf {
    root().join("eval/authoring.toml")
}

fn erc20_transfer_abi() -> String {
    pyjson::dumps(
        &json!([{
            "anonymous": false, "name": "Transfer", "type": "event",
            "inputs": [
                {"indexed": true, "name": "from", "type": "address"},
                {"indexed": true, "name": "to", "type": "address"},
                {"indexed": false, "name": "value", "type": "uint256"},
            ],
        }]),
        Style::Default,
    )
}

#[derive(Debug)]
pub enum Error {
    /// The runner refuses, with a sentence for whoever ran it.
    Fatal(String),
    /// The scorer could not obtain a verdict, as distinct from obtaining a failing one (#1051): an
    /// agent that built nothing and a scorer that could not look are not the same result.
    Unavailable(String),
    Other(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Fatal(m) | Error::Other(m) => f.write_str(m),
            Error::Unavailable(m) => write!(f, "ScoringUnavailable: {m}"),
        }
    }
}

fn die(message: impl std::fmt::Display) -> Error {
    Error::Fatal(format!("authoring runner: {message}"))
}

// --- comparison, ported verbatim from tests/eval_harness.rs -------------------------------------
// Kept in lock-step deliberately: a score must not acquire a second definition of correctness
// merely because it belongs to a different eval.
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
    if let (Some(a), Some(b)) = (number(left), number(right)) {
        return (a - b).abs() <= 1e-9 * a.abs().max(b.abs()).max(1.0);
    }
    scalar_string(left) == scalar_string(right)
}

/// Python `==` on decoded JSON: `1 == 1.0` and `True == 1`.
fn py_eq(a: &Value, b: &Value) -> bool {
    let numeric = |v: &Value| match v {
        Value::Bool(x) => Some(if *x { 1.0 } else { 0.0 }),
        Value::Number(n) => n.as_f64(),
        _ => None,
    };
    match (a, b) {
        (Value::Number(x), Value::Number(y)) if !x.is_f64() && !y.is_f64() => {
            x.to_string() == y.to_string()
        }
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(p, q)| py_eq(p, q))
        }
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len() && x.iter().all(|(k, v)| y.get(k).is_some_and(|w| py_eq(v, w)))
        }
        _ => match (numeric(a), numeric(b)) {
            (Some(x), Some(y)) => x == y,
            _ => a == b,
        },
    }
}

fn row_matches(expected: &Value, actual: &Value) -> bool {
    let Value::Object(e) = expected else {
        return py_eq(expected, actual);
    };
    let Value::Object(a) = actual else {
        return false;
    };
    e.iter()
        .all(|(k, v)| a.get(k).is_some_and(|got| values_equal(v, got)))
}

pub fn results_equal(expected: &[Value], actual: &Value) -> bool {
    let Value::Array(actual) = actual else {
        return false;
    };
    if expected.len() != actual.len() {
        return false;
    }
    let mut remaining: Vec<&Value> = actual.iter().collect();
    for want in expected {
        match remaining.iter().position(|got| row_matches(want, got)) {
            Some(i) => {
                remaining.remove(i);
            }
            None => return false,
        }
    }
    true
}

// --- the fixture chain and the nest under test ---------------------------------------------------
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .expect("bind a loopback port")
        .port()
}

fn http_get(url: &str, timeout: Duration) -> Result<String, String> {
    ureq::AgentBuilder::new()
        .timeout(timeout)
        .build()
        .get(url)
        .call()
        .map_err(|e| e.to_string())?
        .into_string()
        .map_err(|e| e.to_string())
}

pub fn http_json(url: &str) -> Result<Value, Error> {
    let body = http_get(url, Duration::from_secs(8)).map_err(Error::Unavailable)?;
    serde_json::from_str(&body).map_err(|e| Error::Unavailable(format!("JSONDecodeError: {e}")))
}

fn post(url: &str, payload: &Value) -> Result<(), Error> {
    ureq::AgentBuilder::new()
        .timeout(Duration::from_secs(5))
        .build()
        .post(url)
        .send_string(&pyjson::dumps(payload, Style::Default))
        .map(|_| ())
        .map_err(|e| Error::Other(format!("POST {url}: {e}")))
}

/// Poll until `probe` returns something, or the deadline passes.
///
/// `strict`: a poll where **every** attempt failed re-raises the last error rather than reporting a
/// tidy `None`, which the caller would otherwise record as a failed criterion - a scorer failure
/// written down as an agent failure. Where a probe legitimately means "not there" (is `serve` up at
/// all) the lenient form is right, so this is opt-in.
pub fn wait_for<T, E>(
    seconds: f64,
    mut probe: impl FnMut() -> Result<Option<T>, E>,
    strict: bool,
) -> Result<Option<T>, E> {
    let deadline = Instant::now() + Duration::from_secs_f64(seconds);
    let (mut last, mut any_ok) = (None, false);
    while Instant::now() < deadline {
        match probe() {
            Ok(Some(v)) => return Ok(Some(v)),
            Ok(None) => any_ok = true,
            Err(e) => last = Some(e),
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    match last {
        Some(e) if strict && !any_ok => Err(e),
        _ => Ok(None),
    }
}

/// A child killed and waited on when it goes out of scope.
pub struct Reaped(pub Child);

impl Drop for Reaped {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// The fixture chain on host loopback, pinned through `/control/*`.
fn start_fixture_chain(scenario: &Value) -> Result<(Reaped, String, u16), Error> {
    let port = free_port();
    let chain = Command::new("python3")
        .arg(root().join("scripts/fixture_rpc.py"))
        .args(["--port", &port.to_string(), "--contract"])
        .arg(scenario["contract"].as_str().unwrap_or_default())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| Error::Other(format!("spawn the fixture chain: {e}")))?;
    let chain = Reaped(chain);
    let base = format!("http://127.0.0.1:{port}");
    let up = wait_for(
        30.0,
        || http_json(&format!("{base}/control/state")).map(|v| (!v.is_null()).then_some(v)),
        false,
    )?;
    if up.is_none() {
        return Err(die(
            "the fixture chain never came up; nothing can be scored",
        ));
    }
    post(
        &format!("{base}/control/tip"),
        &json!({"number": scenario["tip"]}),
    )?;
    post(
        &format!("{base}/control/finalized"),
        &json!({"number": scenario["finalized"]}),
    )?;
    Ok((chain, format!("{base}/"), port))
}

// --- the subject ---------------------------------------------------------------------------------
/// What a report may honestly claim about the boundary that produced it. One function, used by
/// both the report and the self-test, because a test that re-implements the rule proves only that
/// it can compute the same expression twice.
pub fn isolation_mode(docker_image: Option<&str>) -> &'static str {
    if docker_image.is_some_and(|s| !s.is_empty()) {
        "enforced-by-runner"
    } else {
        "operator-asserted"
    }
}

fn run_argv(argv: &[String], timeout: Duration) -> (i32, String) {
    match proc::run(argv, None, timeout) {
        Ok(out) => (returncode(&out.status), out.stdout),
        Err(e) => (1, format!("<{e}>")),
    }
}

fn sh(argv: &[&str], check: bool) -> Result<String, Error> {
    let argv: Vec<String> = argv.iter().map(|s| s.to_string()).collect();
    let (code, out) = run_argv(&argv, Duration::from_secs(180));
    if check && code != 0 {
        return Err(die(format!(
            "{} failed: {}",
            argv[..argv.len().min(4)].join(" "),
            head(out.trim(), 200)
        )));
    }
    Ok(out)
}

/// Confinement the **runner** owns, on a network with no route out.
///
/// * **The repository is not mounted.** Only the workdir is bound, so `eval/authoring.toml` and the
///   board test are unreachable by construction. Probing cannot prove the absence of a capability.
/// * **No internet.** The network is Docker `--internal`; a tinyproxy straddles it with an
///   allow-list of the model API, so the subject can think but cannot read the manual.
///
/// The fixture chain runs *inside* that network, so it is pinned at startup (`--tip`/`--finalized`)
/// rather than through `/control/*`: the runner cannot reach it either.
pub struct DockerIsolation {
    image: String,
    network: String,
    egress: String,
    fixture: String,
    proxy: String,
    proxy_image: String,
    pub rpc_port: u16,
    pub rpc: String,
    proxy_url: String,
}

impl DockerIsolation {
    pub fn enter(image: &str, scenario: &Value, run_id: &str) -> Result<Self, Error> {
        let fixture = format!("nuthatch-fixture-{run_id}");
        let proxy = format!("nuthatch-proxy-{run_id}");
        let iso = DockerIsolation {
            image: image.to_string(),
            network: format!("nuthatch-eval-{run_id}"),
            egress: format!("nuthatch-egress-{run_id}"),
            rpc: format!("http://{fixture}:8945/"),
            proxy_url: format!("http://{proxy}:8888"),
            fixture,
            proxy,
            proxy_image: "nuthatch-eval-proxy".into(),
            rpc_port: 8945,
        };
        // A missing prerequisite should be a sentence, not a registry error deep inside setup.
        for (image, place) in [
            (&iso.image, "eval/image"),
            (&iso.proxy_image, "eval/image/proxy"),
        ] {
            let argv = ["docker", "image", "inspect", image].map(String::from);
            if run_argv(&argv, Duration::from_secs(180)).0 != 0 {
                return Err(die(format!(
                    "the image '{image}' is not built. Run:  docker build -t {image} {place}"
                )));
            }
        }
        let script = root().join("scripts/fixture_rpc.py");
        let script = script.to_string_lossy();
        sh(
            &["docker", "network", "create", "--internal", &iso.network],
            true,
        )?;
        // A second, ordinary network for the proxy's outward half only. The subject is never on it.
        sh(&["docker", "network", "create", &iso.egress], true)?;
        sh(
            &[
                "docker",
                "run",
                "-d",
                "--rm",
                "--name",
                &iso.proxy,
                "--network",
                &iso.network,
                &iso.proxy_image,
            ],
            true,
        )?;
        sh(
            &["docker", "network", "connect", &iso.egress, &iso.proxy],
            true,
        )?;
        // A container that has been created is not a proxy that is listening. Polled, not slept,
        // so a genuinely broken proxy still fails.
        let mut listening = false;
        for _ in 0..60 {
            let argv = [
                "docker",
                "run",
                "--rm",
                "--network",
                &iso.network,
                &iso.image,
                "sh",
                "-c",
                &format!(
                    "curl -sS -m 3 -o /dev/null {} 2>&1 || exit 1",
                    iso.proxy_url
                ),
            ]
            .map(String::from);
            if run_argv(&argv, Duration::from_secs(30)).0 == 0 {
                listening = true;
                break;
            }
            std::thread::sleep(Duration::from_secs(1));
        }
        if !listening {
            return Err(die(format!(
                "the egress proxy '{}' never started listening on {}.\n\
                 Check `docker logs {}`; the image is {}.",
                iso.proxy, iso.proxy_url, iso.proxy, iso.proxy_image
            )));
        }
        let tip = scenario["tip"].to_string();
        let finalized = scenario["finalized"].to_string();
        let mount = format!("{script}:{script}:ro");
        sh(
            &[
                "docker",
                "run",
                "-d",
                "--rm",
                "--name",
                &iso.fixture,
                "--network",
                &iso.network,
                "-v",
                &mount,
                &iso.image,
                "python3",
                &script,
                "--port",
                &iso.rpc_port.to_string(),
                "--bind",
                "0.0.0.0",
                "--contract",
                scenario["contract"].as_str().unwrap_or_default(),
                "--tip",
                &tip,
                "--finalized",
                &finalized,
            ],
            true,
        )?;
        Ok(iso)
    }

    pub fn argv(&self, workdir: &Path) -> Vec<String> {
        let w = workdir.to_string_lossy();
        vec![
            "docker".into(),
            "run".into(),
            "--rm".into(),
            "--network".into(),
            self.network.clone(),
            "-v".into(),
            format!("{w}:{w}"),
            "-w".into(),
            w.to_string(),
            "-e".into(),
            format!("HTTPS_PROXY={}", self.proxy_url),
            "-e".into(),
            format!("HTTP_PROXY={}", self.proxy_url),
            "-e".into(),
            format!("NO_PROXY={},localhost,127.0.0.1", self.fixture),
            self.image.clone(),
        ]
    }

    fn in_box(&self, workdir: &Path, script: &str) -> (i32, String) {
        let mut argv = self.argv(workdir);
        argv.extend(["sh".into(), "-c".into(), script.to_string()]);
        run_argv(&argv, Duration::from_secs(180))
    }

    /// Reject an unusable image **before** any score is recorded: a broken environment must not
    /// publish a zero that reads as a failing agent.
    pub fn preflight(&self, workdir: &Path) -> Result<(), Error> {
        let allowed = workdir.join("allowed");
        std::fs::write(&allowed, "readable").map_err(|e| Error::Other(e.to_string()))?;
        let (code, out) = self.in_box(workdir, &format!("cat {}", allowed.display()));
        if code != 0 || !out.contains("readable") {
            // The commonest cause is not the image: macOS Docker Desktop does not share
            // `/var/folders`, where mktemp puts things, so the mount yields an empty directory.
            let listing = self
                .in_box(workdir, &format!("ls -a {} 2>&1", workdir.display()))
                .1;
            let said = head(out.trim(), 120);
            let sees = head(listing.trim(), 160);
            return Err(die(format!(
                "the image cannot read {} - the subject's own workdir, at its\n\
                 host path. The subject could not author anything there.\n  \
                 cat said: {}\n  \
                 the container sees: {}\n\
                 \nIf the container sees an EMPTY directory, the host is not sharing that path with\n\
                 Docker rather than the image being wrong - macOS Docker Desktop does not share\n\
                 /var/folders, where mktemp puts things. Set TMPDIR to a shared location, or run\n\
                 the eval on Linux.",
                allowed.display(),
                if said.is_empty() { "(nothing)" } else { &said },
                if sees.is_empty() { "(nothing)" } else { &sees },
            )));
        }

        let (_, out) = self.in_box(
            workdir,
            &format!("cat {} 2>/dev/null || true", scenario_path().display()),
        );
        if !out.trim().is_empty() {
            return Err(die(format!(
                "the image can read {}; the repository must not be mounted into it",
                scenario_path().display()
            )));
        }

        let mut reached = false;
        for tool in ["curl", "wget", "python3"] {
            let fetch = match tool {
                "curl" => format!("curl -fsS -m 5 {}control/state", self.rpc),
                "wget" => format!("wget -qO- -T 5 {}control/state", self.rpc),
                _ => format!(
                    "python3 -c \"import urllib.request as u;print(u.urlopen('{}control/state',timeout=5).read().decode())\"",
                    self.rpc
                ),
            };
            let (code, out) = self.in_box(
                workdir,
                &format!("command -v {tool} >/dev/null 2>&1 || exit 97; {fetch}"),
            );
            if code == 97 {
                continue;
            }
            if code == 0 && out.contains("mode") {
                reached = true;
                break;
            }
        }
        if !reached {
            return Err(die(format!(
                "the image cannot reach the fixture at {}. Either it has none of curl, wget or \
                 python3 - in which case reachability cannot be established and is not assumed - \
                 or the fixture container did not come up.",
                self.rpc
            )));
        }

        let (_, out) = self.in_box(workdir, "command -v claude >/dev/null && echo yes");
        if !out.contains("yes") {
            return Err(die(
                "the image has no `claude` on PATH; every run would fail for the image's reasons",
            ));
        }

        // The subject must reach its own model, and nothing else. Either half alone is a broken
        // eval: without egress `claude` cannot run, with open egress it can read the docs.
        let (_, out) = self.in_box(
            workdir,
            "curl -sS -m 15 -o /dev/null -w '%{http_code}' https://api.anthropic.com/v1/messages 2>&1 || true",
        );
        let got = out.trim();
        if !["200", "400", "401", "403", "404", "405"]
            .iter()
            .any(|c| got.ends_with(c))
        {
            return Err(die(format!(
                "the subject cannot reach the model API through the egress proxy.\n  \
                 got: {}\n\
                 \n`claude` would be unable to run, and every criterion would score false for the\n\
                 harness's reasons rather than the agent's. Check the proxy container is up and that\n\
                 `{}` is built (eval/image/proxy).",
                tail(got, 160),
                self.proxy_image
            )));
        }

        let (_, out) = self.in_box(
            workdir,
            "curl -sS -m 12 -o /dev/null -w '%{http_code}' https://example.com 2>&1 || true",
        );
        let got = out.trim();
        if ["200", "301", "302"].iter().any(|c| got.ends_with(c)) {
            return Err(die(
                "the subject can reach the open internet. It could read nuthatch's documentation and\n\
                 score without the builder skill teaching it anything, which is the thing this eval\n\
                 measures. The proxy's allow-list is not denying - check FilterDefaultDeny.",
            ));
        }
        Ok(())
    }
}

impl Drop for DockerIsolation {
    fn drop(&mut self) {
        let _ = sh(&["docker", "rm", "-f", &self.fixture], false);
        let _ = sh(&["docker", "rm", "-f", &self.proxy], false);
        let _ = sh(&["docker", "network", "rm", &self.network], false);
        let _ = sh(&["docker", "network", "rm", &self.egress], false);
    }
}

/// The sandbox command for *this* run's workdir and RPC port. A template, because a static prefix
/// pointed at a workdir that did not exist yet (#1050). The workdir must be mounted at the same
/// absolute path: the subject is handed host paths for the ABI and the nest.
pub fn sandbox_argv(template: &str, workdir: &Path, rpc_port: u16) -> Result<Vec<String>, Error> {
    let filled = py_format(
        template,
        &[
            ("workdir", workdir.to_string_lossy().into_owned()),
            ("rpc_port", rpc_port.to_string()),
        ],
    )
    .map_err(Error::Other)?;
    shlex::split(&filled).ok_or_else(|| Error::Other("ValueError: No closing quotation".into()))
}

/// Check an operator's sandbox is usable, and does not obviously expose the repository.
///
/// Three things must hold together: it reads the subject's workdir at its host path; it cannot
/// read this repository, probed at several paths because the board test carries the expected total
/// and the canned query verbatim; and it reaches the fixture RPC on loopback. A sanity check, not
/// enforcement: a wrapper that rejects exactly these probes passes them all.
pub fn verify_sandbox(template: &str, rpc_port: u16) -> Result<(), Error> {
    let tmp = tempfile::Builder::new()
        .prefix("nuthatch-sandbox-check-")
        .tempdir()
        .map_err(|e| Error::Other(e.to_string()))?;
    let workdir = tmp.path();
    let allowed = workdir.join("allowed");
    std::fs::write(&allowed, "readable").map_err(|e| Error::Other(e.to_string()))?;
    let argv = sandbox_argv(template, workdir, rpc_port)?;
    let run = |script: &str| -> (i32, String) {
        let mut full = argv.clone();
        full.extend(["sh".into(), "-c".into(), script.to_string()]);
        match proc::run(&full, Some(workdir), Duration::from_secs(180)) {
            Ok(out) => (returncode(&out.status), out.stdout),
            Err(e) => (1, format!("<{e}>")),
        }
    };

    let (code, body) = run(&format!("cat {}", allowed.display()));
    if code != 0 || !body.contains("readable") {
        return Err(die(format!(
            "the sandbox cannot read {} - the subject's own workdir, at its\n\
             host path. The ABI and nest paths handed to the subject would be meaningless inside\n\
             it, so it could not author anything. Mount the workdir at the SAME absolute path,\n\
             e.g. --sandbox 'docker run --rm -v {{workdir}}:{{workdir}} -w {{workdir}} ...'\n  \
             got: {}",
            allowed.display(),
            head(body.trim(), 200)
        )));
    }

    let root = root();
    let mut leaks = Vec::new();
    for path in [
        scenario_path(),
        root.join("tests/authoring_eval_board.rs"),
        root.join("eval/runner/src/authoring.rs"),
        root.join("eval/questions.toml"),
    ] {
        let (code, body) = run(&format!("cat {}", path.display()));
        if code == 0 && !body.trim().is_empty() {
            leaks.push(path.display().to_string());
        }
    }
    let (code, body) = run(&format!("ls {}", root.display()));
    if code == 0 && !body.trim().is_empty() {
        leaks.push(format!("{} (listable)", root.display()));
    }
    if !leaks.is_empty() {
        return Err(die(format!(
            "the sandbox does not isolate this repository. The subject can read:\n  {}\
             \n\nEach of these gives away the answer - the scenario file carries the expected\n\
             result, and the board test carries the same fixture values, the expected total and\n\
             the canned query verbatim. An agent that finds any one scores 3/3 without building\n\
             a nest, and the report records the run as sandboxed.\n\
             \n\
             A prefix that merely execs (`env`, `nice`) is not a sandbox. Use one that confines\n\
             the filesystem - a container, `bwrap --ro-bind`, `sandbox-exec -f`.",
            leaks.join("\n  ")
        )));
    }

    // Reachability is proved, never assumed: none of these tools existing is a refusal, not a pass.
    let (mut state, mut reached) = (None, false);
    for probe in [
        format!("curl -fsS -m 5 http://127.0.0.1:{rpc_port}/control/state"),
        format!("wget -qO- -T 5 http://127.0.0.1:{rpc_port}/control/state"),
        format!(
            "python3 -c \"import urllib.request as u;print(u.urlopen('http://127.0.0.1:{rpc_port}/control/state',timeout=5).read().decode())\""
        ),
    ] {
        let tool = probe.split_whitespace().next().unwrap_or_default();
        let (code, body) = run(&format!("command -v {tool} >/dev/null 2>&1 || exit 97; {probe}"));
        if code == 97 {
            continue;
        }
        let ok = code == 0 && body.contains("mode");
        state = Some(body);
        if ok {
            reached = true;
            break;
        }
    }
    if !reached {
        let why = match &state {
            None => "None of curl, wget or python3 exists inside it, so reachability cannot be\n\
                     established - and it is not assumed. The subject must make HTTP requests from\n\
                     in there, so the image needs one of them."
                .to_string(),
            Some(s) => format!("  got: {}", head(s.trim(), 200)),
        };
        return Err(die(format!(
            "the sandbox could not reach the fixture RPC on 127.0.0.1:{rpc_port}.\n\n{why}\
             \n\nThe subject would be unable to index and every run would fail criteria 2 and 3\n\
             for a reason that has nothing to do with the agent. Give the sandbox host loopback,\n\
             e.g. --sandbox 'docker run --rm --network host -v {{workdir}}:{{workdir}} -w {{workdir}} ...'"
        )));
    }
    Ok(())
}

/// Start a process as the leader of a new session, and take its group id **now**: after the leader
/// has been waited on, asking for it fails and the surviving children are never signalled.
pub fn spawn_group(cmd: &mut Command) -> std::io::Result<(Child, i32)> {
    // SAFETY: setsid is async-signal-safe and touches no state shared with the parent.
    unsafe {
        cmd.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = cmd.spawn()?;
    let pgid = child.id() as i32;
    Ok((child, pgid))
}

/// What reaping needs from the OS, so EPERM can be exercised without changing ownership.
pub trait Group {
    fn killpg(&mut self, pgid: i32, sig: i32) -> std::io::Result<()>;
    /// Whether the leader exited within `grace`.
    fn wait(&mut self, grace: Duration) -> bool;
    /// `ps -axo pgid=,stat=`.
    fn ps(&mut self) -> Result<String, String>;
}

pub struct RealGroup<'a>(pub &'a mut Child);

impl Group for RealGroup<'_> {
    fn killpg(&mut self, pgid: i32, sig: i32) -> std::io::Result<()> {
        // SAFETY: plain syscall on an integer process group.
        if unsafe { libc::killpg(pgid, sig) } == -1 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    fn wait(&mut self, grace: Duration) -> bool {
        proc::wait_timeout(self.0, grace).is_some()
    }

    fn ps(&mut self) -> Result<String, String> {
        ps()
    }
}

fn ps() -> Result<String, String> {
    let argv = ["ps", "-axo", "pgid=,stat="].map(String::from);
    let out = proc::run(&argv, None, Duration::from_secs(5))?;
    if !out.status.success() {
        return Err(format!(
            "CalledProcessError: ps exited {}",
            returncode(&out.status)
        ));
    }
    Ok(out.stdout)
}

#[derive(Debug)]
pub enum ReapError {
    /// A live group we may not signal: cleanup cannot be claimed.
    Denied,
    /// The process table could not be read, which is never evidence of absence.
    Inspect(String),
    Os(std::io::Error),
}

/// EPERM is ambiguous: zombies hold no descriptors, but a live forbidden group may. A failed or
/// malformed inspection is an error, never evidence of absence. `Z+` is still a zombie.
pub fn group_has_live_members(listing: &str, pgid: i32) -> Result<bool, ReapError> {
    let mut live = false;
    for line in listing.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() != 2 || !fields[0].bytes().all(|b| b.is_ascii_digit()) {
            return Err(ReapError::Inspect(format!(
                "cannot inspect process group {pgid}: malformed ps row {line:?}"
            )));
        }
        if fields[0].parse::<i64>().ok() == Some(pgid as i64) && !fields[1].starts_with('Z') {
            live = true;
        }
    }
    Ok(live)
}

fn live(g: &mut impl Group, pgid: i32) -> Result<bool, ReapError> {
    let listing = g.ps().map_err(ReapError::Inspect)?;
    group_has_live_members(&listing, pgid)
}

/// Kill a subject's entire process group and wait for it to be gone: `SIGTERM` so a `dev` closes
/// its store, then `SIGKILL`. A stray `dev` left in the group would still hold the redb lock.
pub fn reap_group(g: &mut impl Group, pgid: i32, grace: Duration) -> Result<(), ReapError> {
    for sig in [libc::SIGTERM, libc::SIGKILL] {
        if let Err(e) = g.killpg(pgid, sig) {
            if !matches!(e.raw_os_error(), Some(libc::ESRCH) | Some(libc::EPERM)) {
                return Err(ReapError::Os(e));
            }
        }
        if !g.wait(grace) {
            continue;
        }
        match g.killpg(pgid, 0) {
            Ok(()) => {}
            Err(e) if e.raw_os_error() == Some(libc::ESRCH) => return Ok(()),
            // macOS can report EPERM for a zombie-only group (#1462), and also for a live group we
            // cannot signal, so denial alone never proves cleanup.
            Err(e) if e.raw_os_error() == Some(libc::EPERM) => {
                if !live(g, pgid)? {
                    return Ok(());
                }
            }
            Err(e) => return Err(ReapError::Os(e)),
        }
    }
    match g.killpg(pgid, libc::SIGKILL) {
        Ok(()) => Ok(()),
        Err(e) if e.raw_os_error() == Some(libc::ESRCH) => Ok(()),
        Err(e) if e.raw_os_error() == Some(libc::EPERM) => {
            if live(g, pgid)? {
                Err(ReapError::Denied)
            } else {
                Ok(())
            }
        }
        Err(e) => Err(ReapError::Os(e)),
    }
}

pub struct Completed {
    pub returncode: i32,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Parser, Debug, Clone)]
#[command(
    about = "Run RFC-0017's authoring evaluation: can an agent build a working nest from nothing?"
)]
pub struct Args {
    #[arg(long)]
    pub self_test: bool,
    /// Confine the subject in this container image. The runner builds the whole `docker run`
    /// itself, mounting only the workdir, so this repository is unreachable BY CONSTRUCTION rather
    /// than as far as anyone probed. The only mode whose reports claim enforced isolation.
    #[arg(long)]
    pub docker_image: Option<String>,
    /// An operator-supplied command TEMPLATE, with {workdir} and {rpc_port} substituted per run -
    /// e.g. "docker run --rm --network host -v {workdir}:{workdir} -w {workdir} <image>". The
    /// workdir must be mounted at the SAME absolute path, because the subject is handed host paths
    /// for the ABI and nest. Sanity-checked, but NOT enforcement: a wrapper that rejects the probes
    /// and permits everything else passes them all. Reports from this mode are labelled
    /// `isolation: operator-asserted` and must not be published as enforced.
    #[arg(long)]
    pub sandbox: Option<String>,
    #[arg(long, default_value_os_t = root().join("target/release/nuthatch"))]
    pub nuthatch: PathBuf,
    #[arg(long, default_value = "claude")]
    pub claude: String,
    #[arg(long, default_value = "sonnet")]
    pub model: String,
    #[arg(long, default_value_t = 3)]
    pub runs: i64,
    #[arg(long, default_value_t = 900.0)]
    pub timeout: f64,
    #[arg(long, default_value_t = 5.0)]
    pub max_budget_usd: f64,
    #[arg(long, default_value_os_t = root().join("eval/authoring-report.json"))]
    pub report: PathBuf,
}

/// One agent with the builder skill and a shell, and nothing else it is not given.
///
/// It must not be able to reach the criteria, the expected result, or this repository, and that is
/// a property of the sandbox, not of this function: changing a working directory is not isolation.
#[allow(clippy::too_many_arguments)]
fn subject_run(
    scenario: &Value,
    workdir: &Path,
    rpc: &str,
    abi: &Path,
    nest: &Path,
    rpc_port: u16,
    args: &Args,
    isolation: Option<&DockerIsolation>,
) -> Result<(Option<String>, Completed), Error> {
    let statement = py_format(
        scenario["task"]["statement"].as_str().unwrap_or_default(),
        &[
            (
                "contract",
                scenario["contract"].as_str().unwrap_or_default().into(),
            ),
            (
                "chain",
                scenario["chain"].as_str().unwrap_or_default().into(),
            ),
            ("rpc", rpc.into()),
            ("abi", abi.to_string_lossy().into_owned()),
            ("dir", nest.to_string_lossy().into_owned()),
        ],
    )
    .map_err(Error::Other)?;
    let skill = root().join("skills/nuthatch-builder");
    if !skill.is_dir() {
        return Err(die(format!(
            "the builder skill is not at {}; there is nothing to evaluate",
            skill.display()
        )));
    }
    copy_tree(&skill, &workdir.join(".claude/skills/nuthatch-builder"))
        .map_err(|e| Error::Other(format!("copy the builder skill: {e}")))?;

    let mut command = match isolation {
        Some(iso) => iso.argv(workdir),
        None => sandbox_argv(
            args.sandbox.as_deref().unwrap_or_default(),
            workdir,
            rpc_port,
        )?,
    };
    command.extend(
        [
            args.claude.as_str(),
            "-p",
            "--model",
            &args.model,
            "--no-session-persistence",
            "--output-format",
            "stream-json",
            "--verbose",
            "--max-budget-usd",
            &float_repr(args.max_budget_usd),
            "--system-prompt",
            "You are working in a shell. The nuthatch builder skill is installed; consult it.",
            "--",
            &statement,
        ]
        .map(String::from),
    );
    // Its own process group, **and actually killed** on every path. A subject that backgrounded
    // `nuthatch dev` otherwise left it holding the nest's exclusive redb lock, and the scorer's
    // `serve` could not open the store. Reaped before scoring, so the lock is provably gone.
    let (mut child, pgid) = spawn_group(
        Command::new(&command[0])
            .args(&command[1..])
            .current_dir(workdir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped()),
    )
    .map_err(|e| Error::Other(format!("spawn the subject: {e}")))?;
    let outcome = proc::communicate(&mut child, Duration::from_secs_f64(args.timeout));
    reap_group(&mut RealGroup(&mut child), pgid, Duration::from_secs(5))
        .map_err(|e| Error::Other(format!("reap the subject's process group: {e:?}")))?;
    let completed = match outcome {
        Some(out) => Completed {
            returncode: returncode(&out.status),
            stdout: out.stdout,
            stderr: out.stderr,
        },
        None => Completed {
            returncode: -1,
            stdout: String::new(),
            stderr: format!("subject exceeded {}s", float_repr(args.timeout)),
        },
    };
    let mut model = None;
    for line in completed.stdout.lines() {
        let Ok(Value::Object(event)) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if event.get("type").and_then(Value::as_str) == Some("system")
            && event.get("subtype").and_then(Value::as_str) == Some("init")
        {
            model = event.get("model").and_then(Value::as_str).map(String::from);
        }
    }
    Ok((model, completed))
}

fn copy_tree(from: &Path, to: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if std::fs::metadata(entry.path())?.is_dir() {
            copy_tree(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), &target)?;
        }
    }
    Ok(())
}

// --- scoring: three facts, no prose --------------------------------------------------------------
fn result(id: &Value, passed: bool, detail: impl Into<Value>) -> Map<String, Value> {
    let mut m = Map::new();
    m.insert("id".into(), id.clone());
    m.insert("passed".into(), Value::Bool(passed));
    m.insert("detail".into(), detail.into());
    m
}

fn py_value(v: &Value) -> String {
    match v {
        Value::Null => "None".into(),
        other => pyjson::dumps(other, Style::Default),
    }
}

/// Score the nest the subject left behind. `http` is how the scorer reads `serve`.
pub fn score(
    scenario: &Value,
    nest: &Path,
    api: &str,
    unservable: Option<&str>,
    http: &dyn Fn(&str) -> Result<Value, Error>,
) -> Result<Vec<Value>, Error> {
    // A nest with no hot store is a finished fact, not a scorer problem: the agent did not index.
    let indexed = nest.join("nuthatch.redb").exists();
    let unreadable = if indexed {
        unservable.map(String::from)
    } else {
        Some("the nest has no hot store: it was scaffolded but never indexed".to_string())
    };
    let mut results = Vec::new();
    for criterion in scenario["criterion"].as_array().into_iter().flatten() {
        let kind = criterion["kind"].as_str().unwrap_or_default();
        let cid = &criterion["id"];
        if let Some(why) = &unreadable {
            if kind != "nest-exists" {
                results.push(Value::Object(result(cid, false, why.as_str())));
                continue;
            }
        }
        match kind {
            "nest-exists" => {
                let missing: Vec<String> = ["nuthatch.toml", "schema.json"]
                    .iter()
                    .filter(|f| !nest.join(f).exists())
                    .map(|f| f.to_string())
                    .collect();
                let detail = if missing.is_empty() {
                    "ok".to_string()
                } else {
                    format!("missing {}", py_list_repr(&missing))
                };
                results.push(Value::Object(result(cid, missing.is_empty(), detail)));
            }
            "sealed-through" => {
                // Wait for the watermark to **reach the target**, not merely to be readable: the
                // first reading is commonly 0 while the store opens, and scoring it is a false zero.
                let want = criterion["value"].as_f64().unwrap_or_default();
                let got = wait_for(
                    180.0,
                    || {
                        let payload = http(&format!("{api}/sql?q=select%201"))?;
                        let Value::Object(payload) = payload else {
                            return Err(Error::Unavailable(
                                "AttributeError: the /sql response is not an object".into(),
                            ));
                        };
                        let sealed = match payload.get("provenance") {
                            None => Value::Null,
                            Some(Value::Object(p)) => {
                                p.get("sealed_through").cloned().unwrap_or(Value::Null)
                            }
                            Some(_) => {
                                return Err(Error::Unavailable(
                                    "AttributeError: provenance is not an object".into(),
                                ))
                            }
                        };
                        Ok(sealed.as_f64().filter(|n| *n >= want).map(|_| sealed))
                    },
                    true,
                )?
                .unwrap_or(Value::Null);
                let passed = got.as_f64() == Some(want);
                results.push(Value::Object(result(
                    cid,
                    passed,
                    format!(
                        "sealed_through={}, wanted {}",
                        py_value(&got),
                        py_value(&criterion["value"])
                    ),
                )));
            }
            "sql" => {
                let Some(table) = resolve_table(api, http)? else {
                    results.push(Value::Object(result(cid, false, "no table to query")));
                    continue;
                };
                let query = criterion["sql"]
                    .as_str()
                    .unwrap_or_default()
                    .replace("{table}", &table);
                let payload = http(&format!("{api}/sql?q={}", quote_plus(&query)))?;
                let Value::Object(payload) = payload else {
                    return Err(Error::Unavailable(
                        "AttributeError: the /sql response is not an object".into(),
                    ));
                };
                if let Some(error) = payload.get("error") {
                    // This query is ours and the board proves it on every commit, so a rejection
                    // means the agent built something structurally different: an authoring failure.
                    let Value::String(error) = error else {
                        return Err(Error::Other(format!(
                            "TypeError: /sql error is not a string: {}",
                            py_value(error)
                        )));
                    };
                    let mut m = result(cid, false, format!("query error: {}", head(error, 120)));
                    m.insert("query_error".into(), Value::String(error.clone()));
                    results.push(Value::Object(m));
                    continue;
                }
                let rows = payload.get("rows").cloned().unwrap_or(Value::Null);
                let shaped = rows
                    .as_array()
                    .is_some_and(|r| r.iter().all(Value::is_object));
                if !shaped {
                    return Err(Error::Unavailable(format!(
                        "/sql returned a 'rows' that is not an array of objects: {}",
                        py_type(&rows)
                    )));
                }
                let expected: Value =
                    serde_json::from_str(criterion["expect"].as_str().unwrap_or("[]"))
                        .map_err(|e| Error::Other(format!("criterion expect: {e}")))?;
                let expected = expected.as_array().cloned().unwrap_or_default();
                let passed = results_equal(&expected, &rows);
                let detail = if passed {
                    "ok".to_string()
                } else {
                    format!("got {}", head(&pyjson::dumps(&rows, Style::Default), 200))
                };
                let mut m = result(cid, passed, detail);
                m.insert("final_rows".into(), if passed { Value::Null } else { rows });
                results.push(Value::Object(m));
            }
            other => {
                return Err(die(format!("unknown criterion kind '{other}'")));
            }
        }
    }
    Ok(results)
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

/// The agent names its own alias, so the table is discovered rather than assumed: a hardcoded
/// `c0__transfer` would fail a good nest for picking a nicer name.
fn resolve_table(
    api: &str,
    http: &dyn Fn(&str) -> Result<Value, Error>,
) -> Result<Option<String>, Error> {
    let listing = http(&format!("{api}/tables"))?;
    let names: Vec<String> = listing
        .get("tables")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|t| {
            let name = t
                .get("name")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty());
            name.or_else(|| t.get("table").and_then(Value::as_str))
                .filter(|s| !s.is_empty())
                .map(String::from)
        })
        .collect();
    Ok(if names.len() == 1 {
        Some(names[0].clone())
    } else {
        None
    })
}

/// One subject, one score. The chain lives wherever the subject can reach it: inside the isolated
/// Docker network for the enforced mode, on host loopback for an operator-supplied sandbox.
fn one_run(
    scenario: &Value,
    args: &Args,
    isolation: Option<&DockerIsolation>,
) -> Result<(Option<String>, Vec<Value>), Error> {
    let (_chain, rpc, rpc_port) = match isolation {
        Some(iso) => (None, iso.rpc.clone(), iso.rpc_port),
        None => {
            let (chain, rpc, port) = start_fixture_chain(scenario)?;
            (Some(chain), rpc, port)
        }
    };
    let tmp = tempfile::Builder::new()
        .prefix("nuthatch-authoring-")
        .tempdir()
        .map_err(|e| Error::Other(e.to_string()))?;
    let work = tmp.path();
    let abi = work.join("erc20.json");
    std::fs::write(&abi, erc20_transfer_abi()).map_err(|e| Error::Other(e.to_string()))?;
    let nest = work.join("nest");

    let (model, completed) =
        subject_run(scenario, work, &rpc, &abi, &nest, rpc_port, args, isolation)?;
    if completed.returncode != 0 {
        eprintln!(
            "  subject exited {}: {}",
            completed.returncode,
            head(completed.stderr.trim(), 200)
        );
    }

    // `serve`, never `dev`: a scorer's `dev` would either lose the redb lock to a subject that
    // complied, or index on behalf of one that did not. `serve` owns no cursor and never advances
    // one, so it can only report what the subject actually indexed.
    let api_port = free_port();
    let api = format!("http://127.0.0.1:{api_port}");
    let mut unservable = None;
    let mut _serve = None;
    if nest.join("nuthatch.toml").exists() {
        let child = Command::new(&args.nuthatch)
            .args(["serve", "--dir"])
            .arg(&nest)
            .args(["--listen", &format!("127.0.0.1:{api_port}")])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| Error::Other(format!("spawn serve: {e}")))?;
        let mut serve = Reaped(child);
        let up = wait_for(
            60.0,
            || http_json(&format!("{api}/tables")).map(|v| (!v.is_null()).then_some(v)),
            false,
        )?;
        if up.is_none() {
            // The subject's process group was reaped before this, so nothing of its can be holding
            // a lock: an unservable nest is an authoring failure and scores as one.
            let exit = match serve.0.try_wait() {
                Ok(Some(status)) => returncode(&status).to_string(),
                _ => "still running".into(),
            };
            unservable = Some(format!(
                "`serve` could not open the nest the subject built (exit {exit})"
            ));
        }
        _serve = Some(serve);
    }
    let results = score(scenario, &nest, &api, unservable.as_deref(), &http_json)?;
    Ok((model, results))
}

pub fn load_scenario() -> Result<Value, Error> {
    let raw = std::fs::read_to_string(scenario_path())
        .map_err(|e| Error::Other(format!("{}: {e}", scenario_path().display())))?;
    let doc: toml::Value = toml::from_str(&raw).map_err(|e| Error::Other(e.to_string()))?;
    serde_json::to_value(doc).map_err(|e| Error::Other(e.to_string()))
}

fn run_id() -> String {
    let mut bytes = [0u8; 4];
    if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
        use std::io::Read;
        let _ = f.read_exact(&mut bytes);
    }
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn run(args: &Args) -> Result<(), Error> {
    if args.runs < 3 {
        return Err(die("refusing to publish fewer than three runs"));
    }
    let docker = args.docker_image.as_deref().filter(|s| !s.is_empty());
    let sandbox = args.sandbox.as_deref().filter(|s| !s.is_empty());
    if docker.is_some() == sandbox.is_some() {
        return Err(die(
            "give exactly one of --docker-image or --sandbox.\n\
             \n  \
             --docker-image IMG   the runner builds the container itself, mounting only the\n                       \
             workdir. This repository is unreachable BY CONSTRUCTION, and\n                       \
             only this mode's reports claim enforced isolation.\n\
             \n  \
             --sandbox TEMPLATE   your own confinement. Sanity-checked, but not enforcement:\n                       \
             probing cannot prove the absence of a capability, and a wrapper\n                       \
             that rejects exactly the probes and permits everything else\n                       \
             passes every one. Labelled `isolation: operator-asserted`.\n\
             \n\
             Neither is a default, because a subject that can read eval/authoring.toml scores 3/3\n\
             without building anything and the failure is silent and flattering.",
        ));
    }

    let scenario = load_scenario()?;
    // Both modes are probed for usability, because a confinement that cannot read the workdir or
    // reach the chain produces a false zero. Only `--sandbox` is also probed for isolation.
    {
        let (_probe_chain, _, probe_port) = start_fixture_chain(&scenario)?;
        if let Some(template) = sandbox {
            verify_sandbox(template, probe_port)?;
        }
    }

    let commit = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(root())
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default();

    let mut runs: Vec<Vec<Value>> = Vec::new();
    let mut model = None;
    {
        let isolation = match docker {
            Some(image) => Some(DockerIsolation::enter(image, &scenario, &run_id())?),
            None => None,
        };
        if let Some(iso) = &isolation {
            let pre = tempfile::Builder::new()
                .prefix("nuthatch-preflight-")
                .tempdir()
                .map_err(|e| Error::Other(e.to_string()))?;
            iso.preflight(pre.path())?;
        }
        for n in 0..args.runs {
            println!("run {}/{}", n + 1, args.runs);
            let (m, results) = one_run(&scenario, args, isolation.as_ref())?;
            model = m;
            for r in &results {
                println!(
                    "  {} {}: {}",
                    if r["passed"] == true { "PASS" } else { "FAIL" },
                    r["id"].as_str().unwrap_or_default(),
                    r["detail"].as_str().unwrap_or_default()
                );
            }
            runs.push(results);
        }
    }

    let mut per_criterion = Map::new();
    for c in scenario["criterion"].as_array().into_iter().flatten() {
        let passes = runs
            .iter()
            .filter(|run| {
                run.iter()
                    .find(|r| r["id"] == c["id"])
                    .is_some_and(|r| r["passed"] == true)
            })
            .count();
        per_criterion.insert(
            c["id"].as_str().unwrap_or_default().to_string(),
            float(passes as f64 / runs.len() as f64),
        );
    }
    let end_to_end = runs
        .iter()
        .filter(|run| run.iter().all(|r| r["passed"] == true))
        .count() as f64
        / runs.len() as f64;
    let report = json!({
        "date": crate::today(false), "commit": commit, "model": model,
        "runs": args.runs, "scenario": "authoring (RFC-0017)",
        // The score is only as trustworthy as the isolation: a reader must see what confined it.
        "sandbox": match sandbox {
            Some(s) => s.to_string(),
            None => format!("docker:{}", docker.unwrap_or_default()),
        },
        "isolation": isolation_mode(docker),
        "summary": {
            "end_to_end_pass_rate": float(end_to_end),
            "by_criterion": per_criterion,
        },
        "runs_detail": runs,
    });
    std::fs::write(&args.report, pyjson::dumps(&report, Style::Indent2) + "\n")
        .map_err(|e| Error::Other(e.to_string()))?;
    println!("wrote {}", args.report.display());
    Ok(())
}

fn fake_nest(dir: &Path, redb: Option<&str>) -> PathBuf {
    let nest = dir.join("nest");
    std::fs::create_dir_all(&nest).unwrap();
    std::fs::write(nest.join("nuthatch.toml"), "").unwrap();
    std::fs::write(nest.join("schema.json"), "{}").unwrap();
    if let Some(body) = redb {
        std::fs::write(nest.join("nuthatch.redb"), body).unwrap();
    }
    nest
}

fn script(dir: &Path, name: &str, body: &str) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let path = dir.join(name);
    std::fs::write(&path, body).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

fn passed_map(results: &[Value]) -> Map<String, Value> {
    results
        .iter()
        .map(|r| {
            (
                r["id"].as_str().unwrap_or_default().to_string(),
                r["passed"].clone(),
            )
        })
        .collect()
}

struct FakeGroup {
    listing: Result<String, String>,
    signals: Vec<i32>,
}

impl Group for FakeGroup {
    fn killpg(&mut self, _pgid: i32, sig: i32) -> std::io::Result<()> {
        self.signals.push(sig);
        Err(std::io::Error::from_raw_os_error(libc::EPERM))
    }
    fn wait(&mut self, _grace: Duration) -> bool {
        true
    }
    fn ps(&mut self) -> Result<String, String> {
        self.listing.clone()
    }
}

fn which(tool: &str) -> bool {
    std::env::var_os("PATH")
        .is_some_and(|paths| std::env::split_paths(&paths).any(|p| p.join(tool).is_file()))
}

/// Prove the runner's properties with no key, no model and no network. Written on day one rather
/// than after a published zero, because #1051 is what the alternative looks like.
pub fn self_test() -> Checks {
    let mut c = Checks::default();
    let tmp = |prefix: &str| tempfile::Builder::new().prefix(prefix).tempdir().unwrap();

    let scenario = load_scenario().expect("the scenario loads");
    let criteria = scenario["criterion"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let ids: Vec<String> = criteria
        .iter()
        .map(|c| c["id"].as_str().unwrap_or_default().to_string())
        .collect();
    c.check(
        "the scenario carries RFC-0017's three criteria",
        ids == ["init-succeeds", "reaches-pinned-tip", "canned-question"],
        &format!("got {}", py_list_repr(&ids)),
    );
    let by_kind = |k: &str| criteria.iter().find(|c| c["kind"] == k).cloned().unwrap();
    let tip_criterion = by_kind("sealed-through");
    c.check(
        "the sealed-through criterion agrees with the chain's finality pin",
        tip_criterion["value"] == scenario["finalized"],
        &format!(
            "criterion {} vs chain {}",
            tip_criterion["value"], scenario["finalized"]
        ),
    );

    // An unreachable scorer must raise, never mark a criterion failed.
    match http_json("http://127.0.0.1:9/tables") {
        Ok(_) => c.check(
            "an unreachable scorer raises",
            false,
            "returned instead of raising",
        ),
        Err(Error::Unavailable(_)) => {
            c.check("an unreachable scorer raises ScoringUnavailable", true, "")
        }
        Err(e) => c.check(
            "an unreachable scorer raises ScoringUnavailable",
            false,
            &e.to_string(),
        ),
    }

    // `"3600"` against `3600` proves nothing: the string fallback matches them with numeric
    // comparison removed. A float discriminates, and floats are what an AVG or a division returns.
    let expected: Vec<Value> =
        serde_json::from_str::<Value>(by_kind("sql")["expect"].as_str().unwrap())
            .unwrap()
            .as_array()
            .cloned()
            .unwrap();
    c.check(
        "a DECIMAL string equals its number",
        results_equal(
            &expected,
            &json!([{"n": 8, "lo": 1, "hi": 8, "total": 3600.0}]),
        ) && values_equal(&json!("3600"), &json!(3600.0))
            && !values_equal(&json!("3600"), &json!("3600.0000001e3")),
        "",
    );
    c.check(
        "a wrong total does not pass",
        !results_equal(
            &expected,
            &json!([{"n": 8, "lo": 1, "hi": 8, "total": 3599}]),
        ),
        "",
    );
    c.check(
        "a missing column does not pass",
        !results_equal(&expected, &json!([{"n": 8, "lo": 1, "hi": 8}])),
        "",
    );
    c.check(
        "the builder skill the subject is given exists",
        root().join("skills/nuthatch-builder/SKILL.md").is_file(),
        "",
    );

    // A subject that scaffolded a nest and never indexed it must score, with criteria 2 and 3
    // false, rather than reach for an API that is not there. The port is dead on purpose.
    {
        let dir = tmp("nuthatch-authoring-selftest-");
        let nest = fake_nest(dir.path(), None);
        match score(&scenario, &nest, "http://127.0.0.1:9", None, &http_json) {
            Ok(results) => {
                let passed = passed_map(&results);
                c.check(
                    "an unindexed nest scores rather than aborting",
                    Value::Object(passed.clone())
                        == json!({"init-succeeds": true, "reaches-pinned-tip": false, "canned-question": false}),
                    &Value::Object(passed).to_string(),
                );
            }
            Err(e) => c.check(
                "an unindexed nest scores rather than aborting",
                false,
                &format!("aborted the run: {e}"),
            ),
        }
    }

    let finalized = scenario["finalized"].clone();
    for (label, body) in [
        ("rows is null", json!({"rows": null})),
        ("rows is a string", json!({"rows": "x"})),
        ("a row is not an object", json!({"rows": [1]})),
    ] {
        let dir = tmp("nuthatch-shape-");
        let nest = fake_nest(dir.path(), Some(""));
        let fin = finalized.clone();
        let fake = move |url: &str| -> Result<Value, Error> {
            Ok(if url.ends_with("/tables") {
                json!({"tables": [{"name": "t"}]})
            } else if url.ends_with("q=select%201") {
                json!({"provenance": {"sealed_through": fin}})
            } else {
                body.clone()
            })
        };
        let name = format!("a malformed /sql shape is fatal ({label})");
        match score(&scenario, &nest, "http://example.invalid", None, &fake) {
            Ok(_) => c.check(&name, false, "scored it instead"),
            Err(Error::Unavailable(_)) => c.check(&name, true, ""),
            Err(e) => c.check(&name, false, &e.to_string()),
        }
    }

    // A prefix that merely execs is not a sandbox: `--sandbox env` satisfied the old non-empty
    // check while handing the subject the whole filesystem.
    match verify_sandbox("env", 9) {
        Ok(()) => c.check(
            "a pass-through prefix is rejected as a sandbox",
            false,
            "accepted `env`",
        ),
        Err(e) => c.check(
            "a pass-through prefix is rejected as a sandbox",
            e.to_string().contains("does not isolate"),
            &format!("refused for another reason: {e}"),
        ),
    }
    // ...and the probe is two-sided, or a sandbox that cannot run at all reads as isolation.
    match verify_sandbox("false", 9) {
        Ok(()) => c.check(
            "an unusable sandbox is rejected, not mistaken for isolation",
            false,
            "accepted it",
        ),
        Err(e) => c.check(
            "an unusable sandbox is rejected, not mistaken for isolation",
            e.to_string().contains("the subject's own workdir"),
            &format!("refused for the wrong reason: {e}"),
        ),
    }

    // Hiding the answer key but exposing the rest of the repository must still be refused: the
    // board test carries the expected total and the canned query verbatim.
    {
        let dir = tmp("nuthatch-leakbox-");
        let gate = script(
            dir.path(),
            "leaky.sh",
            "#!/bin/sh\n\
             # Hides the scenario file and nothing else - the partial sandbox that must not pass.\n\
             case \"$*\" in *authoring.toml*) exit 13 ;; esac\n\
             exec \"$@\"\n",
        );
        match verify_sandbox(&format!("{} ", gate.display()), 9) {
            Ok(()) => c.check(
                "hiding only the answer key is not isolation",
                false,
                "accepted a partial sandbox",
            ),
            Err(e) => c.check(
                "hiding only the answer key is not isolation",
                e.to_string().contains("does not isolate this repository"),
                &format!("refused for another reason: {}", head(&e.to_string(), 160)),
            ),
        }
    }

    // A sandbox with no way to make an HTTP request is refused, not waved through.
    let root_path = root();
    {
        let dir = tmp("nuthatch-notools-");
        let gate = script(
            dir.path(),
            "notools.sh",
            &format!(
                "#!/bin/sh\n\
                 # Confines the repository and hides every HTTP client, leaving the rest usable - i.e.\n\
                 # a working image on a network the fixture is not on.\n\
                 case \"$*\" in *{}*) exit 13 ;; esac\n\
                 case \"$*\" in *curl*|*wget*|*urllib*) exit 97 ;; esac\n\
                 exec \"$@\"\n",
                root_path.display()
            ),
        );
        match verify_sandbox(&format!("{} ", gate.display()), 9) {
            Ok(()) => c.check(
                "a sandbox with no HTTP client is refused",
                false,
                "accepted it",
            ),
            Err(e) => c.check(
                "a sandbox with no HTTP client is refused",
                e.to_string().contains("could not reach the fixture RPC"),
                &format!("refused for another reason: {}", head(&e.to_string(), 160)),
            ),
        }
    }

    // The positive case, against a real fixture chain: a `verify_sandbox` that refused everything
    // would satisfy every check above while making the eval unrunnable.
    match start_fixture_chain(&scenario) {
        Ok((_chain, _, port)) => {
            let dir = tmp("nuthatch-fakebox-");
            let gate = script(
                dir.path(),
                "gate.sh",
                &format!(
                    "#!/bin/sh\n\
                     # NOT a sandbox. It rejects command lines naming the repository and runs\n\
                     # everything else, only to prove the usability legs are satisfiable.\n\
                     case \"$*\" in *{}*) exit 13 ;; esac\n\
                     exec \"$@\"\n",
                    root_path.display()
                ),
            );
            match verify_sandbox(&format!("{} ", gate.display()), port) {
                Ok(()) => c.check("a confining sandbox is accepted", true, ""),
                Err(e) => c.check(
                    "a confining sandbox is accepted",
                    false,
                    &format!("rejected a working sandbox: {}", head(&e.to_string(), 200)),
                ),
            }
        }
        Err(e) => c.check(
            "a confining sandbox is accepted",
            false,
            &format!("the fixture chain did not start: {e}"),
        ),
    }

    // The claim a report makes about itself must match the mode that produced it.
    for (image, want) in [
        (Some("img"), "enforced-by-runner"),
        (None, "operator-asserted"),
    ] {
        let label = isolation_mode(image);
        c.check(
            &format!("a {want} report is labelled {want}"),
            label == want,
            &format!("labelled {label}"),
        );
    }

    // A run must be impossible without isolation: a subject that can read eval/authoring.toml
    // scores 3/3 without building anything.
    let no_sandbox = Args {
        self_test: false,
        docker_image: None,
        sandbox: None,
        nuthatch: std::env::current_exe().unwrap(),
        claude: "claude".into(),
        model: "sonnet".into(),
        runs: 3,
        timeout: 900.0,
        max_budget_usd: 5.0,
        report: PathBuf::from("/dev/null"),
    };
    match run(&no_sandbox) {
        Ok(()) => c.check("a run without a sandbox is refused", false, "it ran anyway"),
        Err(e) => c.check(
            "a run without a sandbox is refused",
            e.to_string()
                .contains("exactly one of --docker-image or --sandbox"),
            &format!("exited for another reason: {e}"),
        ),
    }

    // (a) The sealed-through probe must wait for the target, not score a store still at 0.
    {
        let dir = tmp("nuthatch-seal-");
        let nest = fake_nest(dir.path(), Some("x"));
        let target = tip_criterion["value"].clone();
        let readings = std::cell::Cell::new(0);
        let rows = Value::Array(expected.clone());
        let fake = |url: &str| -> Result<Value, Error> {
            if url.ends_with("/tables") {
                return Ok(json!({"tables": [{"name": "t"}]}));
            }
            let n = readings.get();
            readings.set(n + 1);
            let sealed = if n < 3 { json!(0) } else { target.clone() };
            Ok(json!({"provenance": {"sealed_through": sealed}, "rows": rows}))
        };
        let passed = score(&scenario, &nest, "http://x", None, &fake)
            .map(|r| passed_map(&r))
            .unwrap_or_default();
        c.check(
            "a nest still sealing is waited for, not scored at zero",
            passed.get("reaches-pinned-tip") == Some(&Value::Bool(true)),
            &Value::Object(passed).to_string(),
        );
    }

    // (b) `wait_for(strict)` re-raises when every attempt failed, tested on the function itself:
    // routed through the scorer it passed with the strict branch removed.
    let always_raises =
        || -> Result<Option<()>, Error> { Err(Error::Unavailable("serve died".into())) };
    match wait_for(0.5, always_raises, true) {
        Ok(_) => c.check(
            "wait_for(strict) re-raises rather than reporting None",
            false,
            "returned",
        ),
        Err(_) => c.check(
            "wait_for(strict) re-raises rather than reporting None",
            true,
            "",
        ),
    }
    c.check(
        "wait_for without strict still reports None",
        matches!(wait_for(0.5, always_raises, false), Ok(None)),
        "",
    );
    // A probe that answers but never reaches its target timed out; it did not fail to look.
    c.check(
        "a probe that answers but never satisfies reports None",
        matches!(
            wait_for(0.5, || Ok::<Option<()>, Error>(None), true),
            Ok(None)
        ),
        "",
    );

    // A store that exists but cannot be served is the agent's failure and must score.
    {
        let dir = tmp("nuthatch-corrupt-");
        let nest = fake_nest(dir.path(), Some("not a database"));
        match score(
            &scenario,
            &nest,
            "http://127.0.0.1:9",
            Some("`serve` could not open it"),
            &http_json,
        ) {
            Ok(results) => {
                let passed = passed_map(&results);
                c.check(
                    "an unservable nest scores rather than aborting",
                    Value::Object(passed.clone())
                        == json!({"init-succeeds": true, "reaches-pinned-tip": false, "canned-question": false}),
                    &Value::Object(passed).to_string(),
                );
            }
            Err(e) => c.check(
                "an unservable nest scores rather than aborting",
                false,
                &format!("aborted: {e}"),
            ),
        }
    }

    // EPERM is reproducible without changing ownership or relying on launchd's reaping schedule.
    // SAFETY: getpgrp cannot fail.
    let own = unsafe { libc::getpgrp() };
    c.check(
        "the process inspector sees its own live group",
        ps().ok()
            .and_then(|l| group_has_live_members(&l, own).ok())
            .unwrap_or(false),
        "",
    );
    for (label, listing, denied) in [
        ("zombie-only", "42 Z\n42 Z+\n99 ?E\n", false),
        ("absent", "99 S\n", false),
        ("live", "42 S\n", true),
        ("mixed", "42 Z\n42 S+\n", true),
        ("unknown-state", "42 ?E\n", true),
    ] {
        let mut g = FakeGroup {
            listing: Ok(listing.into()),
            signals: Vec::new(),
        };
        let raised = matches!(
            reap_group(&mut g, 42, Duration::from_secs(5)),
            Err(ReapError::Denied)
        );
        c.check(
            &format!("an EPERM {label} group is classified safely"),
            raised == denied && g.signals.contains(&libc::SIGKILL) == denied,
            "",
        );
    }
    for (label, listing) in [
        ("failed", Err("CalledProcessError".to_string())),
        ("timed-out", Err("TimeoutExpired".to_string())),
        ("malformed", Ok("42\n".to_string())),
    ] {
        let mut g = FakeGroup {
            listing,
            signals: Vec::new(),
        };
        let raised = matches!(
            reap_group(&mut g, 42, Duration::from_secs(5)),
            Err(ReapError::Inspect(_))
        );
        c.check(
            &format!("a {label} process inspection does not claim cleanup"),
            raised,
            "",
        );
    }

    // A subject that backgrounds a long-lived child must not leave it holding the redb lock. With
    // a positive control: the first version's child never started, so it passed with killpg gone.
    for (label, timeout) in [("normal exit", 30.0), ("timeout", 1.0)] {
        let dir = tmp("nuthatch-reap-");
        let marker = dir.path().join("alive");
        let child = script(
            dir.path(),
            "child.sh",
            &format!(
                "#!/bin/sh\nwhile :; do touch '{}'; sleep 0.05; done\n",
                marker.display()
            ),
        );
        let parent = script(
            dir.path(),
            "parent.sh",
            &format!(
                "#!/bin/sh\n'{}' &\nsleep {}\n",
                child.display(),
                if timeout < 5.0 { "30" } else { "0.4" }
            ),
        );
        let (mut proc_, pgid) = spawn_group(
            Command::new(&parent)
                .stdout(Stdio::null())
                .stderr(Stdio::null()),
        )
        .expect("spawn the reap fixture");

        let alive = wait_for(10.0, || Ok::<_, ()>(marker.exists().then_some(())), false);
        c.check(
            &format!("the reap check has a live child to kill ({label})"),
            matches!(alive, Ok(Some(()))),
            "the child never started, so the reap assertion below would be vacuous",
        );

        let _ = proc::wait_timeout(&mut proc_, Duration::from_secs_f64(timeout));
        let _ = reap_group(&mut RealGroup(&mut proc_), pgid, Duration::from_secs(5));
        let _ = std::fs::remove_file(&marker);
        std::thread::sleep(Duration::from_millis(500));
        c.check(
            &format!("a backgrounded child is reaped ({label})"),
            !marker.exists(),
            "the child is still running and would hold the redb lock",
        );
    }

    // The enforced mode, exercised against a live container when Docker and a stub image exist.
    // The skip is printed so a green run cannot be mistaken for a verified one.
    match std::env::var("NUTHATCH_EVAL_STUB_IMAGE") {
        Ok(stub) if !stub.is_empty() && which("docker") => {
            let id = format!("selftest{}", std::process::id());
            match DockerIsolation::enter(&stub, &scenario, &id) {
                Ok(iso) => {
                    let work = tmp("nuthatch-iso-");
                    std::fs::write(work.path().join("allowed"), "readable").unwrap();
                    let (_, out) = iso.in_box(
                        work.path(),
                        &format!("cat {} 2>/dev/null", scenario_path().display()),
                    );
                    c.check(
                        "enforced: the repository is unreadable",
                        out.trim().is_empty(),
                        &format!("leaked {}", head(out.trim(), 80)),
                    );
                    let (_, out) = iso.in_box(
                        work.path(),
                        &format!("curl -fsS -m 5 {}control/state", iso.rpc),
                    );
                    c.check(
                        "enforced: the fixture chain is reachable",
                        out.contains("mode"),
                        &head(out.trim(), 120),
                    );
                    let (_, out) = iso.in_box(
                        work.path(),
                        "curl -sS -m 5 https://example.com >/dev/null 2>&1 && echo LEAK || echo none",
                    );
                    c.check(
                        "enforced: there is no route to the internet",
                        !out.contains("LEAK"),
                        out.trim(),
                    );
                }
                Err(e) => c.check(
                    "enforced: the isolated network stands up",
                    false,
                    &head(&e.to_string(), 200),
                ),
            }
        }
        _ => println!("  skip enforced-mode checks (set NUTHATCH_EVAL_STUB_IMAGE and have docker)"),
    }

    c.finish();
    c
}
