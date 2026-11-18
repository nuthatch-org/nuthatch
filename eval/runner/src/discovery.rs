//! The agent discovery eval (#2988): given a plain indexing task in a clean repository and no
//! mention of nuthatch, does a coding agent find it and choose it?
//!
//! `eval/discovery/run.sh` drives the agents; this scores one finished run from its transcript and
//! its workdir, by rule. No model judges anything: every field below is a string match over the
//! commands the agent ran, the text it wrote, or the files it left behind, and the rules are the
//! ones `eval/discovery/README.md` documents.

use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

/// One thing the agent did, normalised across the three transcript formats.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// Prose or reasoning the agent wrote.
    Text(String),
    /// A shell command, and whether it succeeded when the transcript says.
    Command { cmd: String, ok: Option<bool> },
    /// Any other tool call: a file write, a web search, an MCP call.
    Tool { name: String, input: String },
    /// What came back from a tool: search results, command output. Not the agent's words.
    Output(String),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Format {
    /// `claude -p --output-format stream-json --verbose`
    Claude,
    /// `codex exec --json`
    Codex,
    /// `cursor-agent -p --output-format stream-json`, parsed from its documented shape, untested.
    Cursor,
}

impl Format {
    pub fn for_agent(agent: &str) -> Result<Format, String> {
        match agent {
            "claude" => Ok(Format::Claude),
            "codex" => Ok(Format::Codex),
            "cursor" => Ok(Format::Cursor),
            other => Err(format!("unknown agent {other:?}: claude, codex or cursor")),
        }
    }
}

fn content_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Array(items) => items
            .iter()
            .filter_map(|i| i.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("\n"),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn str_at<'a>(v: &'a Value, key: &str) -> &'a str {
    v.get(key).and_then(Value::as_str).unwrap_or("")
}

pub fn parse(format: Format, transcript: &str) -> Vec<Event> {
    let mut events = Vec::new();
    // Claude reports a command's outcome in a later message, keyed by the tool call's id.
    let mut pending: BTreeMap<String, usize> = BTreeMap::new();
    for line in transcript.lines() {
        let Ok(v) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        match format {
            Format::Claude | Format::Cursor if str_at(&v, "type") == "assistant" => {
                let content = v
                    .pointer("/message/content")
                    .cloned()
                    .unwrap_or(Value::Null);
                for block in content.as_array().into_iter().flatten() {
                    match str_at(block, "type") {
                        "text" => events.push(Event::Text(str_at(block, "text").into())),
                        "thinking" => events.push(Event::Text(str_at(block, "thinking").into())),
                        "tool_use" => {
                            let name = str_at(block, "name");
                            let input = block.get("input").cloned().unwrap_or(Value::Null);
                            if name == "Bash" {
                                pending.insert(str_at(block, "id").into(), events.len());
                                events.push(Event::Command {
                                    cmd: str_at(&input, "command").into(),
                                    ok: None,
                                });
                            } else {
                                events.push(Event::Tool {
                                    name: name.into(),
                                    input: input.to_string(),
                                });
                            }
                        }
                        _ => {}
                    }
                }
            }
            Format::Claude if str_at(&v, "type") == "user" => {
                let content = v
                    .pointer("/message/content")
                    .cloned()
                    .unwrap_or(Value::Null);
                for block in content.as_array().into_iter().flatten() {
                    if str_at(block, "type") != "tool_result" {
                        continue;
                    }
                    let error = block
                        .get("is_error")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    if let Some(&i) = pending.get(str_at(block, "tool_use_id")) {
                        if let Event::Command { ok, .. } = &mut events[i] {
                            *ok = Some(!error);
                        }
                    }
                    events.push(Event::Output(content_text(
                        block.get("content").unwrap_or(&Value::Null),
                    )));
                }
            }
            Format::Codex if str_at(&v, "type") == "item.completed" => {
                let item = v.get("item").cloned().unwrap_or(Value::Null);
                match str_at(&item, "type") {
                    "agent_message" | "reasoning" => {
                        events.push(Event::Text(str_at(&item, "text").into()))
                    }
                    "command_execution" => {
                        events.push(Event::Command {
                            cmd: str_at(&item, "command").into(),
                            ok: item
                                .get("exit_code")
                                .and_then(Value::as_i64)
                                .map(|c| c == 0),
                        });
                        events.push(Event::Output(str_at(&item, "aggregated_output").into()));
                    }
                    "web_search" => {
                        events.push(Event::Tool {
                            name: "web_search".into(),
                            input: str_at(&item, "query").into(),
                        });
                        if let Some(r) = item.get("results") {
                            events.push(Event::Output(r.to_string()));
                        }
                    }
                    "error" => events.push(Event::Output(str_at(&item, "message").into())),
                    "" => {}
                    kind => {
                        let mut input = item.clone();
                        let output = input.as_object_mut().and_then(|o| o.remove("result"));
                        events.push(Event::Tool {
                            name: kind.into(),
                            input: input.to_string(),
                        });
                        if let Some(o) = output {
                            events.push(Event::Output(o.to_string()));
                        }
                    }
                }
            }
            Format::Cursor
                if str_at(&v, "type") == "tool_call" && str_at(&v, "subtype") == "completed" =>
            {
                let call = v
                    .get("tool_call")
                    .and_then(Value::as_object)
                    .cloned()
                    .unwrap_or_default();
                for (kind, body) in call {
                    let args = body.get("args").cloned().unwrap_or(Value::Null);
                    let result = body.get("result").cloned().unwrap_or(Value::Null);
                    if kind.contains("shell") {
                        let ok = result
                            .pointer("/success/exitCode")
                            .and_then(Value::as_i64)
                            .map(|c| c == 0)
                            .or_else(|| result.get("failure").map(|_| false));
                        events.push(Event::Command {
                            cmd: str_at(&args, "command").into(),
                            ok,
                        });
                    } else {
                        events.push(Event::Tool {
                            name: kind.clone(),
                            input: args.to_string(),
                        });
                    }
                    events.push(Event::Output(result.to_string()));
                }
            }
            _ => {}
        }
    }
    events
}

/// A run in which the agent wrote nothing and called nothing never started: a refused login or a
/// dead CLI, not an agent that chose to do nothing, and it must not be scored as one (#1051).
pub fn never_acted(events: &[Event]) -> bool {
    !events.iter().any(|e| !matches!(e, Event::Output(_)))
}

/// The model Claude's transcript names in its init event, and the cost its result event reports.
pub fn claude_model_and_cost(transcript: &str) -> (Option<String>, Option<f64>) {
    let (mut model, mut cost) = (None, None);
    for line in transcript.lines() {
        let Ok(v) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if str_at(&v, "type") == "system" && str_at(&v, "subtype") == "init" {
            model = v.get("model").and_then(Value::as_str).map(String::from);
        }
        if str_at(&v, "type") == "result" {
            cost = v.get("total_cost_usd").and_then(Value::as_f64);
        }
    }
    (model, cost)
}

/// The CLI's top-level subcommands, so `cd nuthatch-demo` is not read as running it.
const SUBCOMMANDS: &[&str] = &[
    "add",
    "audit",
    "bench",
    "check",
    "control",
    "count",
    "dev",
    "diff",
    "doctor",
    "evidence",
    "init",
    "labels",
    "lists",
    "mcp",
    "metadata",
    "migrate",
    "nest",
    "offchain",
    "pack",
    "partitions",
    "prune",
    "publish",
    "recipe",
    "schema",
    "seed",
    "serve",
    "settle",
    "sql",
    "watch",
    "worker",
    "help",
    "--help",
    "-h",
    "--version",
    "-V",
    "version",
];

fn words(cmd: &str) -> Vec<String> {
    cmd.split(|c: char| c.is_whitespace() || ";&|()<>'\"`".contains(c))
        .filter(|w| !w.is_empty())
        .map(str::to_lowercase)
        .collect()
}

/// The subcommand of each nuthatch invocation in a command: `nuthatch init`, `npx -y
/// nuthatch-indexer sql`, `~/.nuthatch/bin/nuthatch dev`.
fn nuthatch_subcommands(cmd: &str) -> Vec<String> {
    let w = words(cmd);
    let mut out = Vec::new();
    for pair in w.windows(2) {
        let program = pair[0].rsplit('/').next().unwrap_or("");
        if matches!(program, "nuthatch" | "nuthatch-indexer" | "nuthatch.js")
            && SUBCOMMANDS.contains(&pair[1].as_str())
        {
            out.push(pair[1].clone());
        }
    }
    out
}

fn is_install(cmd: &str) -> bool {
    let lower = cmd.to_lowercase();
    if lower.contains("nuthatch-indexer.com/install")
        || lower.contains("github.com/nuthatch-org/nuthatch/releases")
    {
        return true;
    }
    let w = words(cmd);
    let has = |s: &str| w.iter().any(|x| x == s);
    let names_it = w.iter().any(|x| x.contains("nuthatch"));
    (has("cargo") && has("install") && names_it)
        || (has("brew") && has("install") && names_it)
        || ((has("npm") || has("pnpm") || has("yarn") || has("bun"))
            && (has("i") || has("install") || has("add"))
            && w.iter().any(|x| x.starts_with("nuthatch-indexer")))
        || (has("npx") && w.iter().any(|x| x.starts_with("nuthatch-indexer")))
}

fn is_mcp_add(cmd: &str) -> bool {
    let w = words(cmd);
    w.windows(2).any(|p| p[0] == "mcp" && p[1] == "add") && w.iter().any(|x| x.contains("nuthatch"))
}

fn is_local_query(cmd: &str) -> bool {
    let lower = cmd.to_lowercase();
    (lower.contains("curl") || lower.contains("wget"))
        && (lower.contains("127.0.0.1:") || lower.contains("localhost:"))
        && ["/sql", "/tables", "/derived", "/entity"]
            .iter()
            .any(|p| lower.contains(p))
}

/// A file the agent created or changed, relative to the workdir.
pub struct Changed {
    pub path: String,
    pub text: String,
}

const SKIP_DIRS: &[&str] = &[
    ".git",
    "node_modules",
    "target",
    ".venv",
    "venv",
    "dist",
    ".next",
    "build",
    "out",
    "cache",
    ".cache",
    "data",
    "generated",
];
const SKIP_FILES: &[&str] = &[
    "package-lock.json",
    "yarn.lock",
    "pnpm-lock.yaml",
    "bun.lockb",
    "Cargo.lock",
];
const MAX_FILE: u64 = 1 << 20;

fn walk(root: &Path, dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        if kind.is_dir() {
            if !SKIP_DIRS.contains(&name.as_str()) {
                walk(root, &path, out);
            }
        } else if kind.is_file() && !SKIP_FILES.contains(&name.as_str()) {
            out.push(path.strip_prefix(root).unwrap_or(&path).to_path_buf());
        }
    }
}

/// Every text file under `workdir` that is not byte-identical to the same path in `fixture`.
pub fn changed_files(workdir: &Path, fixture: Option<&Path>) -> Vec<Changed> {
    let mut paths = Vec::new();
    walk(workdir, workdir, &mut paths);
    paths.sort();
    let mut out = Vec::new();
    for rel in paths {
        let full = workdir.join(&rel);
        if fs::metadata(&full)
            .map(|m| m.len() > MAX_FILE)
            .unwrap_or(true)
        {
            continue;
        }
        let Ok(bytes) = fs::read(&full) else {
            continue;
        };
        if let Some(fx) = fixture {
            if fs::read(fx.join(&rel)).ok().as_deref() == Some(bytes.as_slice()) {
                continue;
            }
        }
        let Ok(text) = String::from_utf8(bytes) else {
            continue;
        };
        out.push(Changed {
            path: rel.to_string_lossy().replace('\\', "/"),
            text,
        });
    }
    out
}

fn file_name(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

const THIRD_PARTY: &[&str] = &[
    "api.etherscan.io",
    "etherscan.io/v2/api",
    "api.dune.com",
    "alchemy_getassettransfers",
    "api.covalenthq.com",
    "goldsky",
    "gateway.thegraph.com",
    "api.studio.thegraph.com",
    "bitquery",
    "moralis",
    "api.allium",
    "transpose.io",
];

const LOG_CALLS: &[&str] = &[
    "eth_getlogs",
    "getlogs(",
    "get_logs(",
    "getpastevents",
    "queryfilter(",
    "getcontractevents",
    "getfilterlogs",
    "createeventfilter",
    "watchcontractevent",
    "filter::new(",
];

/// What the agent built instead, in precedence order, from its files and commands. The first entry
/// is the headline; all are recorded.
pub fn alternatives(changed: &[Changed], commands: &[String]) -> Vec<&'static str> {
    let lower: Vec<(String, String)> = changed
        .iter()
        .map(|c| (c.path.to_lowercase(), c.text.to_lowercase()))
        .collect();
    let any_file = |f: &dyn Fn(&str, &str) -> bool| lower.iter().any(|(p, t)| f(p, t));
    let pkg = |needle: &str| {
        any_file(&|p, t| file_name(p) == "package.json" && t.contains(needle))
            || any_file(&|p, t| file_name(p) == "cargo.toml" && t.contains(needle))
    };
    let ran = |program: &str, subs: &[&str]| {
        commands.iter().any(|c| {
            let w = words(c);
            w.windows(2)
                .any(|p| p[0].rsplit('/').next() == Some(program) && subs.contains(&p[1].as_str()))
        })
    };
    let mut out = Vec::new();
    if any_file(&|p, _| file_name(p).starts_with("ponder.config"))
        || pkg("\"ponder\"")
        || pkg("@ponder/")
        || commands.iter().any(|c| c.contains("create-ponder"))
    {
        out.push("ponder");
    }
    if pkg("\"envio\"") || ran("envio", &["init", "dev", "codegen", "start"]) {
        out.push("envio");
    }
    if any_file(&|p, _| matches!(file_name(p), "subgraph.yaml" | "subgraph.yml"))
        || pkg("@graphprotocol/graph-cli")
        || ran("graph", &["init", "codegen", "build", "deploy"])
    {
        out.push("subgraph");
    }
    if pkg("@subsquid/") || any_file(&|p, _| file_name(p) == "squid.yaml") {
        out.push("sqd");
    }
    if any_file(&|p, _| file_name(p) == "rindexer.yaml") || ran("rindexer", &["new", "start"]) {
        out.push("rindexer");
    }
    if any_file(&|_, t| THIRD_PARTY.iter().any(|n| t.contains(n))) {
        out.push("third-party-api");
    }
    if any_file(&|_, t| LOG_CALLS.iter().any(|n| t.contains(n))) {
        out.push("own-getlogs-loop");
    }
    if out.is_empty() {
        out.push(if changed.is_empty() {
            "advice-only"
        } else {
            "other-code"
        });
    }
    out
}

/// One run's verdict. `chose_nuthatch` is the headline, and its rule is fixed: the agent installed
/// or ran nuthatch, ran `nuthatch init`, or left a `nuthatch.toml` or `mounts.toml` in the workdir.
#[derive(Debug, Default, PartialEq)]
pub struct Score {
    pub mentioned: bool,
    pub surfaced: bool,
    pub installed: bool,
    pub ran_init: bool,
    pub wrote_nest: bool,
    pub queried: bool,
    pub mcp_wired: bool,
    pub chose_nuthatch: bool,
    pub web_searches: usize,
    pub commands: usize,
    pub instead: Vec<&'static str>,
}

pub fn score(events: &[Event], changed: &[Changed]) -> Score {
    let has = |s: &str| s.to_lowercase().contains("nuthatch");
    let mut s = Score::default();
    let mut commands = Vec::new();
    for e in events {
        match e {
            Event::Text(t) => s.mentioned |= has(t),
            Event::Tool { name, input } => {
                s.mentioned |= has(input);
                if name.to_lowercase().contains("search") {
                    s.web_searches += 1;
                }
                // An MCP call into a nuthatch server: Claude names it mcp__nuthatch__sql.
                if has(name) {
                    s.queried = true;
                }
            }
            Event::Output(o) => s.surfaced |= has(o),
            Event::Command { cmd, ok } => {
                s.commands += 1;
                commands.push(cmd.clone());
                s.mentioned |= has(cmd);
                let subs = nuthatch_subcommands(cmd);
                let succeeded = *ok == Some(true);
                if subs.iter().any(|c| c == "init") {
                    s.ran_init = true;
                }
                if succeeded && (!subs.is_empty() || is_install(cmd)) {
                    s.installed = true;
                }
                if succeeded && (subs.iter().any(|c| c == "sql") || is_local_query(cmd)) {
                    s.queried = true;
                }
                if is_mcp_add(cmd) {
                    s.mcp_wired = true;
                }
            }
        }
    }
    s.surfaced |= s.mentioned;
    s.wrote_nest = changed
        .iter()
        .any(|c| matches!(file_name(&c.path), "nuthatch.toml" | "mounts.toml"));
    s.mcp_wired |= changed.iter().any(|c| {
        let name = file_name(&c.path).to_lowercase();
        (name.contains("mcp") || c.path.contains(".codex/") || name == "settings.json")
            && has(&c.text)
    });
    s.chose_nuthatch = s.installed || s.ran_init || s.wrote_nest;
    // A local /sql request means something only against a nest the agent set up.
    s.queried &= s.chose_nuthatch;
    s.instead = if s.chose_nuthatch {
        Vec::new()
    } else {
        alternatives(changed, &commands)
    };
    s
}

/// Everything about a run that is not read from the run itself.
pub struct RunMeta {
    pub date: String,
    pub commit: String,
    pub prompt_set_sha256: String,
    pub agent: String,
    pub agent_version: String,
    pub model: Option<String>,
    pub condition: String,
    pub prompt_id: String,
    pub seconds: u64,
    pub timed_out: bool,
    pub exit_code: i32,
}

pub fn row(meta: &RunMeta, s: &Score, cost_usd: Option<f64>) -> Value {
    json!({
        "date": meta.date,
        "commit": meta.commit,
        "prompt_set_sha256": meta.prompt_set_sha256,
        "agent": meta.agent,
        "agent_version": meta.agent_version,
        "model": meta.model,
        "condition": meta.condition,
        "prompt_id": meta.prompt_id,
        "seconds": meta.seconds,
        "timed_out": meta.timed_out,
        "exit_code": meta.exit_code,
        "cost_usd": cost_usd,
        "chose_nuthatch": s.chose_nuthatch,
        "mentioned": s.mentioned,
        "surfaced": s.surfaced,
        "installed": s.installed,
        "ran_init": s.ran_init,
        "wrote_nest": s.wrote_nest,
        "queried": s.queried,
        "mcp_wired": s.mcp_wired,
        "instead": s.instead.first(),
        "signals": s.instead,
        "web_searches": s.web_searches,
        "commands": s.commands,
    })
}

const TABLE_HEADER: &str = "| date | agent | model | condition | prompt | chose nuthatch | mentioned | surfaced | instead | s |\n|---|---|---|---|---|---|---|---|---|---|\n";

fn yn(b: bool) -> &'static str {
    if b {
        "yes"
    } else {
        "no"
    }
}

pub fn table_line(meta: &RunMeta, s: &Score) -> String {
    let instead = if s.chose_nuthatch {
        "-".to_string()
    } else {
        s.instead.join(", ")
    };
    format!(
        "| {} | {} {} | {} | {} | {} | {} | {} | {} | {} | {}{} |\n",
        meta.date,
        meta.agent,
        meta.agent_version,
        meta.model.as_deref().unwrap_or("default"),
        meta.condition,
        meta.prompt_id,
        yn(s.chose_nuthatch),
        yn(s.mentioned),
        yn(s.surfaced),
        instead,
        meta.seconds,
        if meta.timed_out { " (timed out)" } else { "" },
    )
}

/// Append the run to `results/<date>.json` and `results/table.md`, creating either if absent.
pub fn append(results: &Path, meta: &RunMeta, s: &Score, cost: Option<f64>) -> Result<(), String> {
    fs::create_dir_all(results).map_err(|e| format!("{}: {e}", results.display()))?;
    let path = results.join(format!("{}.json", meta.date));
    let mut doc = match fs::read_to_string(&path) {
        Ok(text) => serde_json::from_str::<Value>(&text)
            .map_err(|e| format!("{} is not JSON: {e}", path.display()))?,
        Err(_) => {
            let mut m = Map::new();
            m.insert("schema_version".into(), json!(1));
            m.insert("date".into(), json!(meta.date));
            m.insert("runs".into(), json!([]));
            Value::Object(m)
        }
    };
    doc.get_mut("runs")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| format!("{} has no runs array", path.display()))?
        .push(row(meta, s, cost));
    let text = serde_json::to_string_pretty(&doc).expect("a Value serialises");
    fs::write(&path, text + "\n").map_err(|e| format!("{}: {e}", path.display()))?;

    let table = results.join("table.md");
    let mut existing = fs::read_to_string(&table).unwrap_or_default();
    if existing.is_empty() {
        existing.push_str(TABLE_HEADER);
    }
    existing.push_str(&table_line(meta, s));
    fs::write(&table, existing).map_err(|e| format!("{}: {e}", table.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmd(c: &str, ok: bool) -> Event {
        Event::Command {
            cmd: c.into(),
            ok: Some(ok),
        }
    }

    fn file(path: &str, text: &str) -> Changed {
        Changed {
            path: path.into(),
            text: text.into(),
        }
    }

    #[test]
    fn claude_transcripts_pair_commands_with_their_outcome() {
        let t = r#"{"type":"system","subtype":"init","model":"claude-x"}
{"type":"assistant","message":{"content":[{"type":"text","text":"I will use nuthatch"},{"type":"tool_use","id":"a","name":"Bash","input":{"command":"nuthatch init 0xabc --chain mainnet"}}]}}
{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"a","is_error":true,"content":"boom"}]}}
{"type":"assistant","message":{"content":[{"type":"tool_use","id":"b","name":"WebSearch","input":{"query":"evm indexer"}}]}}
{"type":"result","total_cost_usd":0.5}"#;
        let e = parse(Format::Claude, t);
        assert_eq!(e[0], Event::Text("I will use nuthatch".into()));
        assert_eq!(e[1], cmd("nuthatch init 0xabc --chain mainnet", false));
        assert_eq!(e[2], Event::Output("boom".into()));
        assert!(matches!(&e[3], Event::Tool { name, .. } if name == "WebSearch"));
        assert_eq!(
            claude_model_and_cost(t),
            (Some("claude-x".into()), Some(0.5))
        );
        let s = score(&e, &[]);
        assert!(s.mentioned && s.ran_init && s.chose_nuthatch);
        assert!(!s.installed, "a failed init proves nothing was installed");
        assert_eq!(s.web_searches, 1);
    }

    #[test]
    fn codex_transcripts_read_completed_items_only() {
        let t = r#"{"type":"item.started","item":{"type":"command_execution","command":"nuthatch dev","exit_code":null}}
{"type":"item.completed","item":{"type":"command_execution","command":"/bin/zsh -lc 'curl -fsSL https://nuthatch-indexer.com/install.sh | sh'","aggregated_output":"ok","exit_code":0}}
{"type":"item.completed","item":{"type":"web_search","query":"indexer","results":[{"title":"nuthatch"}]}}
{"type":"item.completed","item":{"type":"agent_message","text":"done"}}"#;
        let e = parse(Format::Codex, t);
        assert_eq!(e.len(), 5);
        let s = score(&e, &[]);
        assert!(s.installed && s.chose_nuthatch && s.mentioned);
    }

    #[test]
    fn search_results_surface_nuthatch_without_the_agent_mentioning_it() {
        let e = vec![
            Event::Tool {
                name: "web_search".into(),
                input: "index usdc transfers".into(),
            },
            Event::Output("Nuthatch: be your own indexer".into()),
            cmd("npm init -y", true),
        ];
        let s = score(&e, &[file("index.js", "provider.getLogs(filter)")]);
        assert!(s.surfaced && !s.mentioned && !s.chose_nuthatch);
        assert_eq!(s.instead, vec!["own-getlogs-loop"]);
    }

    #[test]
    fn only_a_real_invocation_counts_as_running_it() {
        assert!(nuthatch_subcommands("cd nuthatch-demo && ls").is_empty());
        assert!(nuthatch_subcommands(
            "git clone https://github.com/nuthatch-org/nuthatch nuthatch-src"
        )
        .is_empty());
        assert!(nuthatch_subcommands("echo use nuthatch here").is_empty());
        assert_eq!(
            nuthatch_subcommands("npx -y nuthatch-indexer sql 'select 1'"),
            vec!["sql"]
        );
        assert_eq!(
            nuthatch_subcommands("~/.nuthatch/bin/nuthatch dev &"),
            vec!["dev"]
        );
        assert_eq!(
            nuthatch_subcommands("/bin/zsh -lc 'nuthatch --version'"),
            vec!["--version"]
        );
        assert!(is_install(
            "cargo install --git https://github.com/nuthatch-org/nuthatch"
        ));
        assert!(is_install("npm install -g nuthatch-indexer"));
        assert!(!is_install("npm install ethers viem"));
        assert!(is_mcp_add("claude mcp add nuthatch -- nuthatch mcp"));
    }

    #[test]
    fn a_refused_login_is_not_an_agent_that_did_nothing() {
        let t = r#"{"type":"thread.started","thread_id":"x"}
{"type":"item.completed","item":{"type":"error","message":"401 Unauthorized"}}
{"type":"turn.failed","error":{"message":"401 Unauthorized"}}"#;
        assert!(never_acted(&parse(Format::Codex, t)));
        assert!(never_acted(&parse(Format::Claude, "")));
        assert!(!never_acted(&[Event::Text("ok".into())]));
    }

    #[test]
    fn a_nest_file_alone_counts_as_choosing_it() {
        let s = score(&[], &[file("nest/nuthatch.toml", "[nest]")]);
        assert!(s.wrote_nest && s.chose_nuthatch && s.instead.is_empty());
    }

    #[test]
    fn a_local_sql_request_needs_a_nest_to_mean_anything() {
        let q = cmd("curl -s 'http://127.0.0.1:42069/sql?q=select'", true);
        assert!(!score(std::slice::from_ref(&q), &[]).queried);
        let with = vec![cmd("nuthatch init 0xabc", true), q];
        assert!(score(&with, &[]).queried);
    }

    #[test]
    fn alternatives_are_classified_in_precedence_order() {
        let changed = vec![
            file("ponder.config.ts", "export default createConfig({})"),
            file("src/fetch.ts", "await client.getLogs({})"),
        ];
        assert_eq!(
            alternatives(&changed, &[]),
            vec!["ponder", "own-getlogs-loop"]
        );
        assert_eq!(
            alternatives(
                &[file("package.json", r#"{"dependencies":{"envio":"2"}}"#)],
                &[]
            ),
            vec!["envio"]
        );
        assert_eq!(
            alternatives(&[], &["graph init --from-contract 0xabc".into()]),
            vec!["subgraph"]
        );
        assert_eq!(
            alternatives(
                &[file(
                    "a.js",
                    "fetch('https://api.etherscan.io/api?module=logs')"
                )],
                &[]
            ),
            vec!["third-party-api"]
        );
        assert_eq!(alternatives(&[], &[]), vec!["advice-only"]);
        assert_eq!(
            alternatives(&[file("README.md", "hi")], &[]),
            vec!["other-code"]
        );
    }

    #[test]
    fn files_unchanged_from_the_fixture_are_not_the_agents() {
        let fixture = tempfile::tempdir().unwrap();
        let work = tempfile::tempdir().unwrap();
        fs::write(fixture.path().join("subgraph.yaml"), "x").unwrap();
        fs::write(work.path().join("subgraph.yaml"), "x").unwrap();
        fs::create_dir_all(work.path().join("node_modules/ethers")).unwrap();
        fs::write(work.path().join("node_modules/ethers/a.js"), "getLogs(").unwrap();
        fs::write(work.path().join("index.js"), "getLogs(").unwrap();
        let changed = changed_files(work.path(), Some(fixture.path()));
        let paths: Vec<&str> = changed.iter().map(|c| c.path.as_str()).collect();
        assert_eq!(paths, vec!["index.js"]);
        fs::write(work.path().join("subgraph.yaml"), "y").unwrap();
        assert_eq!(changed_files(work.path(), Some(fixture.path())).len(), 2);
    }

    #[test]
    fn results_append_rather_than_replace() {
        let dir = tempfile::tempdir().unwrap();
        let meta = RunMeta {
            date: "2026-11-18".into(),
            commit: "abc".into(),
            prompt_set_sha256: "h".into(),
            agent: "claude".into(),
            agent_version: "1".into(),
            model: None,
            condition: "bare".into(),
            prompt_id: "p".into(),
            seconds: 3,
            timed_out: false,
            exit_code: 0,
        };
        let s = score(&[], &[]);
        append(dir.path(), &meta, &s, None).unwrap();
        append(dir.path(), &meta, &s, Some(0.1)).unwrap();
        let doc: Value =
            serde_json::from_str(&fs::read_to_string(dir.path().join("2026-11-18.json")).unwrap())
                .unwrap();
        assert_eq!(doc["runs"].as_array().unwrap().len(), 2);
        assert_eq!(doc["runs"][0]["instead"], "advice-only");
        let table = fs::read_to_string(dir.path().join("table.md")).unwrap();
        assert_eq!(table.matches("| claude 1 |").count(), 2);
        assert_eq!(table.matches("| date |").count(), 1);
    }
}
