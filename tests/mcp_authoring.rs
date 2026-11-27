//! #2983: from an empty directory to rows, through `nuthatch mcp --authoring` alone. The client
//! here is a plain JSON-RPC pipe: no shell, no `init`, no `dev` run by the test itself.

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{channel, Receiver};
use std::time::{Duration, Instant};

use nuthatch_fixture_rpc::scripted::Scripted;
use serde_json::{json, Value};

const ERC20_TRANSFER_ABI: &str = r#"[{"anonymous":false,"inputs":[
  {"indexed":true,"name":"from","type":"address"},
  {"indexed":true,"name":"to","type":"address"},
  {"indexed":false,"name":"value","type":"uint256"}],"name":"Transfer","type":"event"}]"#;

const CONTRACT: &str = "0x000000000000000000000000000000000000c0de";

struct Client {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: Receiver<String>,
    next: u64,
}

impl Client {
    fn start(args: &[String]) -> Client {
        let mut child = Command::new(env!("CARGO_BIN_EXE_nuthatch"))
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn nuthatch mcp");
        let stdin = child.stdin.take();
        let stdout = child.stdout.take().unwrap();
        let (tx, lines) = channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if tx.send(line).is_err() {
                    return;
                }
            }
        });
        Client {
            child,
            stdin,
            lines,
            next: 0,
        }
    }

    fn request(&mut self, method: &str, params: Value) -> Value {
        self.next += 1;
        let msg = json!({ "jsonrpc": "2.0", "id": self.next, "method": method, "params": params });
        let stdin = self.stdin.as_mut().unwrap();
        writeln!(stdin, "{msg}").unwrap();
        stdin.flush().unwrap();
        let line = self
            .lines
            .recv_timeout(Duration::from_secs(120))
            .unwrap_or_else(|_| panic!("no answer to {method}"));
        let resp: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(resp["id"], self.next, "{resp}");
        resp
    }

    fn call(&mut self, tool: &str, arguments: Value) -> (bool, String) {
        let resp = self.request(
            "tools/call",
            json!({ "name": tool, "arguments": arguments }),
        );
        let text = resp["result"]["content"][0]["text"]
            .as_str()
            .unwrap_or_else(|| panic!("{resp}"))
            .to_string();
        (resp["result"]["isError"] == true, text)
    }
}

impl Drop for Client {
    /// End of input is how a client stops the server, and the indexer it started goes with it.
    fn drop(&mut self) {
        drop(self.stdin.take());
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if let Ok(Some(_)) = self.child.try_wait() {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[test]
fn an_mcp_client_goes_from_no_nest_to_rows_with_no_shell() {
    let rpc =
        nuthatch_fixture_rpc::spawn(Scripted::new(CONTRACT), "127.0.0.1:0").expect("fixture RPC");
    let rpc_url = format!("http://127.0.0.1:{}/", rpc.port());
    let control = |path: &str, n: u64| {
        let ok = Command::new("curl")
            .args(["-fsS", "-m", "5", "-XPOST"])
            .arg(format!("{rpc_url}control/{path}"))
            .args(["-d", &format!("{{\"number\": {n}}}")])
            .status()
            .unwrap()
            .success();
        assert!(ok, "pin {path}");
    };
    control("tip", 9);
    control("finalized", 8);

    let work = tempfile::tempdir().unwrap();
    let abi = work.path().join("erc20.json");
    std::fs::write(&abi, ERC20_TRANSFER_ABI).unwrap();
    let nest = work.path().join("nest");
    let url = format!("http://127.0.0.1:{}", free_port());

    let mut client = Client::start(&[
        "mcp".into(),
        "--url".into(),
        url.clone(),
        "--authoring".into(),
        "--dir".into(),
        nest.to_string_lossy().into_owned(),
    ]);
    let init = client.request("initialize", json!({ "protocolVersion": "2025-06-18" }));
    assert!(
        init["result"]["instructions"]
            .as_str()
            .is_some_and(|s| s.contains("EVM") && s.contains("init_nest")),
        "{init}"
    );
    let tools = client.request("tools/list", json!({}));
    let names: Vec<&str> = tools["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|t| t["name"].as_str())
        .collect();
    for want in ["init_nest", "start_indexing", "index_status", "sql"] {
        assert!(names.contains(&want), "{want} not in {names:?}");
    }

    let (failed, text) = client.call("sql", json!({ "query": "SELECT 1" }));
    assert!(failed, "nothing is served yet: {text}");

    let (failed, text) = client.call(
        "init_nest",
        json!({ "address": CONTRACT, "chain": "arbitrum-one", "alias": "tok",
                "rpc": rpc_url, "abi": abi.to_string_lossy() }),
    );
    assert!(!failed, "{text}");
    assert!(nest.join("nuthatch.toml").exists(), "{text}");

    let (failed, text) = client.call("start_indexing", json!({ "poll_interval": 1 }));
    assert!(!failed, "{text}");

    let deadline = Instant::now() + Duration::from_secs(120);
    let rows = loop {
        let (failed, text) = client.call(
            "sql",
            json!({ "query": "SELECT COUNT(*) AS n FROM tok__transfer" }),
        );
        let n = (!failed)
            .then(|| text.lines().filter(|l| l.starts_with("| ")).nth(1))
            .flatten()
            .and_then(|l| l.trim_start_matches("| ").trim().parse::<u64>().ok());
        if let Some(n) = n.filter(|&n| n >= 8) {
            break n;
        }
        assert!(Instant::now() < deadline, "no rows in time: {text}");
        std::thread::sleep(Duration::from_millis(500));
    };
    assert!(rows >= 8);

    let (failed, text) = client.call("index_status", json!({}));
    assert!(!failed, "{text}");
    let status: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(status["indexer"]["running"], true, "{status}");

    drop(client);
    let deadline = Instant::now() + Duration::from_secs(10);
    while Command::new("curl")
        .args(["-fsS", "-m", "2"])
        .arg(format!("{url}/health"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap()
        .success()
    {
        assert!(
            Instant::now() < deadline,
            "the indexer outlived the server that started it"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}
