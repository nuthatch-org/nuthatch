//! #3021 - the discovery skill is the only nuthatch text an agent with nothing installed reads, so
//! every command it tells the agent to run is parsed here against clap's own command tree (no
//! network, nothing executed). A subcommand or flag the binary lacks fails the build, and so does a
//! foreground `dev`, which would block an agent's shell. The MCP configs the plugin ships are held
//! to the same parse.

use clap::Parser;
use nuthatch::cli::Cli;
use std::path::PathBuf;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Split one shell line into words: single and double quotes group, nothing else is interpreted.
fn words(line: &str) -> Vec<String> {
    let (mut out, mut cur, mut quote, mut any) = (Vec::new(), String::new(), None, false);
    for c in line.chars() {
        match (quote, c) {
            (Some(q), c) if c == q => quote = None,
            (Some(_), c) => cur.push(c),
            (None, '\'' | '"') => {
                quote = Some(c);
                any = true;
            }
            (None, c) if c.is_whitespace() => {
                if any || !cur.is_empty() {
                    out.push(std::mem::take(&mut cur));
                    any = false;
                }
            }
            (None, c) => cur.push(c),
        }
    }
    if any || !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// The nuthatch arguments in `words`, if it runs nuthatch: directly, through `npx`, or as the
/// command after `claude mcp add <name> --`.
fn nuthatch_args(w: &[String]) -> Option<Vec<String>> {
    let w = match w.iter().position(|x| x == "--") {
        Some(i) if w.first().map(String::as_str) == Some("claude") => &w[i + 1..],
        _ => w,
    };
    match w.first()?.as_str() {
        "nuthatch" => Some(w[1..].to_vec()),
        "npx" => {
            let i = w.iter().position(|x| x == "nuthatch-indexer")?;
            Some(w[i + 1..].to_vec())
        }
        _ => None,
    }
}

/// Every command line in a fenced block and every inline backtick span.
fn commands(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut in_fence = false;
    for line in text.lines() {
        if line.trim_start().starts_with("```") {
            in_fence = !in_fence;
        } else if in_fence {
            let l = line.trim();
            if !l.is_empty() && !l.starts_with('#') {
                out.push(l.to_string());
            }
        } else {
            for (i, span) in line.split('`').enumerate() {
                if i % 2 == 1 {
                    out.push(span.to_string());
                }
            }
        }
    }
    out
}

fn parse(args: &[String]) -> Result<Cli, String> {
    Cli::try_parse_from(std::iter::once("nuthatch".to_string()).chain(args.iter().cloned()))
        .map_err(|e| e.to_string())
}

#[test]
fn every_command_in_the_discovery_skill_parses() {
    let text = std::fs::read_to_string(root().join("skills/evm-indexing/SKILL.md")).unwrap();
    let mut seen: Vec<Vec<String>> = Vec::new();
    for cmd in commands(&text) {
        let Some(args) = nuthatch_args(&words(&cmd)) else {
            continue;
        };
        // A bare mention such as `nuthatch` alone is prose, not a command.
        if args.is_empty() {
            continue;
        }
        if let Err(e) = parse(&args) {
            panic!("SKILL.md says `{cmd}`, which the binary rejects:\n{e}");
        }
        if args[0] == "dev" {
            assert!(
                args.iter().any(|a| a == "--detach"),
                "SKILL.md runs `{cmd}` in the foreground, which blocks an agent's shell"
            );
        }
        seen.push(args);
    }
    let has = |first: &str, flag: &str| {
        seen.iter()
            .any(|a| a[0] == first && (flag.is_empty() || a.iter().any(|x| x == flag)))
    };
    assert!(has("init", "--dir"), "the quick start must init into --dir");
    assert!(has("dev", "--detach"), "the quick start must detach dev");
    assert!(has("sql", ""), "the quick start must end in a query");
    assert!(
        has("mcp", "--authoring"),
        "the MCP line must use --authoring"
    );
    assert!(
        !text.contains("nuthatch dev --backfill"),
        "a bare foreground dev must not come back"
    );
}

#[test]
fn the_shipped_mcp_configs_run_npx_and_parse() {
    let configs = ["mcp.json", "plugin/.claude-plugin/plugin.json"];
    for file in configs {
        let v: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(root().join(file)).unwrap()).unwrap();
        let s = &v["mcpServers"]["nuthatch"];
        assert_eq!(s["command"], "npx", "{file} must not need a binary on PATH");
        let mut w = vec!["npx".to_string()];
        w.extend(
            s["args"]
                .as_array()
                .unwrap()
                .iter()
                .map(|a| a.as_str().unwrap().to_string()),
        );
        let args = nuthatch_args(&w)
            .unwrap_or_else(|| panic!("{file}: npx does not run nuthatch-indexer"));
        assert!(
            args.iter().any(|a| a == "--authoring"),
            "{file}: no --authoring"
        );
        parse(&args).unwrap_or_else(|e| panic!("{file}: {e}"));
    }
    let server: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(root().join("server.json")).unwrap())
            .unwrap();
    let npm = server["packages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["registryType"] == "npm")
        .expect("an npm entry");
    let mut args = Vec::new();
    for a in npm["packageArguments"].as_array().unwrap() {
        if a["type"] == "named" {
            args.push(a["name"].as_str().unwrap().to_string());
            // A bare flag such as `--authoring` has no value or default (#3021).
            if let Some(v) = a["value"].as_str().or(a["default"].as_str()) {
                args.push(v.to_string());
            }
        } else {
            args.push(a["value"].as_str().unwrap().to_string());
        }
    }
    assert!(args.iter().any(|a| a == "--authoring"));
    parse(&args).unwrap_or_else(|e| panic!("server.json npm entry: {e}"));
}

#[test]
fn print_config_offers_the_authoring_form() {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_nuthatch"))
        .args(["mcp", "--print-config"])
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        text.contains("npx -y nuthatch-indexer mcp --authoring --dir nest"),
        "{text}"
    );
}
