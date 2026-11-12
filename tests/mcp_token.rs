//! RFC-0073 FR13 through the real binary: `nuthatch mcp --token-env` reads the token from the
//! environment, sends it as a bearer, and never prints it.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::process::{Command, Stdio};

const TOKEN: &str = "mcp-canary-4be19d0f";

fn nuthatch() -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_nuthatch"));
    c.env_remove("NUTHATCH_MCP_TOKEN");
    c
}

#[test]
fn print_config_names_the_variable_and_never_the_token() {
    let out = nuthatch()
        .args(["mcp", "--print-config", "--token-env", "NEST_TOKEN"])
        .env("NEST_TOKEN", TOKEN)
        .env("NUTHATCH_MCP_TOKEN", TOKEN)
        .output()
        .unwrap();
    assert!(out.status.success());
    let all = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!all.contains(TOKEN), "{all}");
    assert!(all.contains("--token-env NEST_TOKEN"), "{all}");
}

#[test]
fn a_named_variable_that_is_unset_refuses_to_start() {
    let out = nuthatch()
        .args(["mcp", "--token-env", "NO_SUCH_TOKEN_VAR"])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("NO_SUCH_TOKEN_VAR"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// A one-shot nest that refuses the request and echoes its Authorization header in the body.
fn echoing_nest() -> (u16, std::thread::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut auth = String::new();
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            if line == "\r\n" || line.is_empty() {
                break;
            }
            if let Some(v) = line.to_ascii_lowercase().strip_prefix("authorization:") {
                auth = line[line.len() - v.len()..].trim().to_string();
            }
        }
        let body = format!("bad credentials: {auth}");
        write!(
            stream,
            "HTTP/1.1 401 Unauthorized\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        )
        .unwrap();
        let _ = reader.read_to_end(&mut Vec::new());
        auth
    });
    (port, handle)
}

#[test]
fn the_bearer_is_sent_and_an_echo_of_it_is_redacted() {
    for (flag, var) in [(true, "NEST_TOKEN"), (false, "NUTHATCH_MCP_TOKEN")] {
        let (port, nest) = echoing_nest();
        let mut cmd = nuthatch();
        cmd.args(["mcp", "--url", &format!("http://127.0.0.1:{port}")]);
        if flag {
            cmd.args(["--token-env", var]);
        }
        let mut child = cmd
            .env(var, TOKEN)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let call = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": { "name": "tables", "arguments": {} },
        });
        let mut stdin = child.stdin.take().unwrap();
        writeln!(stdin, "{call}").unwrap();
        drop(stdin);
        let out = child.wait_with_output().unwrap();
        assert_eq!(nest.join().unwrap(), format!("Bearer {TOKEN}"), "{var}");
        let all = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(
            all.contains("HTTP 401: bad credentials: Bearer [redacted]"),
            "{all}"
        );
        assert!(!all.contains(TOKEN), "{all}");
    }
}

#[test]
fn malformed_json_gets_a_parse_error_and_the_stdio_loop_recovers() {
    let mut child = nuthatch()
        .args(["mcp", "--url", "http://127.0.0.1:1"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let mut stdin = child.stdin.take().unwrap();
    writeln!(
        stdin,
        "{}",
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": { "protocolVersion": "1999-01-01" }
        })
    )
    .unwrap();
    // Raw text, not `json!`: the first line is missing its closing brace on purpose.
    let malformed = r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"tables","arguments":{}}"#;
    let ping = r#"{"jsonrpc":"2.0","id":3,"method":"ping"}"#;
    writeln!(stdin, "{malformed}").unwrap();
    writeln!(stdin, "{ping}").unwrap();
    writeln!(
        stdin,
        "{}",
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "initialize",
            "params": { "protocolVersion": "2025-06-18" }
        })
    )
    .unwrap();
    drop(stdin);

    let out = child.wait_with_output().unwrap();
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    let responses: Vec<serde_json::Value> = stdout
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(responses.len(), 4, "{stdout}");
    assert_eq!(responses[0]["id"], 1);
    assert_eq!(responses[0]["result"]["protocolVersion"], "2025-06-18");
    assert_eq!(responses[1]["id"], serde_json::Value::Null);
    assert_eq!(responses[1]["error"]["code"], -32700);
    assert_eq!(responses[1]["error"]["message"], "Parse error");
    assert_eq!(
        responses[2]["id"], 3,
        "the loop handles the valid request after malformed input"
    );
    assert_eq!(responses[2]["result"], serde_json::json!({}));
    assert_eq!(responses[3]["id"], 4);
    assert_eq!(responses[3]["result"]["protocolVersion"], "2025-06-18");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("nuthatch mcp: malformed JSON-RPC message"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}
