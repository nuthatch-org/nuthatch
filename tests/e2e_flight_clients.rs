//! RFC-0079 §16: Flight SQL from the real binary, `nuthatch dev --flight`, over a runtime of two
//! mounts of the recorded tab-parity contract that differ only in a view naming each. Two clients:
//!
//! - arrow-flight's `FlightSqlServiceClient`, always. It shares nuthatch's crate versions, so it
//!   proves the wiring more than the protocol.
//! - Apache Arrow ADBC's Flight SQL driver, in Go (`tools/flight-client`), when
//!   `NUTHATCH_FLIGHT_GO_CLIENT` names its binary; `NUTHATCH_REQUIRE_FLIGHT_GO` fails the test
//!   without it. It is the driver every ADBC binding loads, written by people who are not us.
//!
//! Both compare a statement's rows with `POST /<route>/sql`'s JSON, pick the mount by
//! `x-nuthatch-route`, are refused with `/sql`'s JSON error in the gRPC status, and list the
//! route's tables through the catalogue calls.

#![cfg(feature = "flight")]

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::CommandGetTables;
use futures::TryStreamExt;
use nuthatch_fixture_rpc::recorded::Recorded;
use serde_json::{json, Value};

const DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/tab-parity");
const CONTRACT: &str = "0x0c26fe5e579c3b007ba16d921ba8204a47c05f1a";
const DEPLOY: u64 = 26_239_807;
const ABI: &str = r#"[{"type":"event","name":"Transfer","anonymous":false,"inputs":[
  {"name":"from","type":"address","indexed":true},
  {"name":"to","type":"address","indexed":true},
  {"name":"value","type":"uint256","indexed":false}]}]"#;
const STATEMENT: &str = "SELECT block_number, log_index, tx_hash, \"from\", \"to\", value, \
                         value_dec, value_overflow FROM tab__transfer ORDER BY block_number, log_index";

struct Reaped(Child);

impl Drop for Reaped {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn nuthatch() -> Command {
    Command::new(env!("CARGO_BIN_EXE_nuthatch"))
}

/// `POST <api>/<route>/sql`, its status and body.
fn post_sql(api: &str, route: &str, q: &str) -> (u16, Value) {
    let out = Command::new("curl")
        .args([
            "-s",
            "-m",
            "30",
            "-w",
            "\n%{http_code}",
            "-H",
            "content-type: application/json",
        ])
        .args(["-d", &json!({ "q": q }).to_string()])
        .arg(format!("{api}/{route}/sql"))
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&out.stdout);
    let (body, code) = text.rsplit_once('\n').unwrap_or(("", "0"));
    (
        code.trim().parse().unwrap_or(0),
        serde_json::from_str(body).unwrap_or(Value::Null),
    )
}

/// A nest of the recorded contract whose `which_nest` view answers `which`, at `data/<nid>`.
fn mount(runtime: &Path, rpc: &str, which: &str) -> String {
    let abi = runtime.join("abi.json");
    std::fs::write(&abi, ABI).unwrap();
    let staged = runtime.join("data").join(which);
    let init = nuthatch()
        .args(["init", CONTRACT, "--chain", "mainnet", "--alias", "tab"])
        .args(["--rpc", rpc, "--start-block", &DEPLOY.to_string()])
        .arg("--abi")
        .arg(&abi)
        .arg("--dir")
        .arg(&staged)
        .output()
        .unwrap();
    assert!(
        init.status.success(),
        "{}",
        String::from_utf8_lossy(&init.stderr)
    );
    std::fs::write(
        staged.join("views/90-which.sql"),
        format!("CREATE VIEW which_nest AS SELECT '{which}' AS which;\n"),
    )
    .unwrap();
    let inspect = nuthatch()
        .args(["nest", "inspect"])
        .arg(&staged)
        .arg("--json")
        .output()
        .unwrap();
    let nid = serde_json::from_slice::<Value>(&inspect.stdout).unwrap()["nid"]
        .as_str()
        .unwrap()
        .to_string();
    std::fs::rename(&staged, runtime.join("data").join(&nid)).unwrap();
    format!("\n[[mounts]]\nalias = \"{which}\"\nnid = \"{nid}\"\n")
}

async fn client(
    addr: &str,
    route: Option<&str>,
) -> FlightSqlServiceClient<tonic::transport::Channel> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut c = FlightSqlServiceClient::new(channel);
    c.set_token("t".into());
    if let Some(r) = route {
        c.set_header("x-nuthatch-route", r);
    }
    c
}

/// A statement's batches, or the status it was refused with.
async fn run(
    c: &mut FlightSqlServiceClient<tonic::transport::Channel>,
    q: &str,
) -> Result<Vec<arrow::array::RecordBatch>, tonic::Status> {
    let info = c.execute(q.to_string(), None).await?;
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    Ok(c.do_get(ticket).await?.try_collect().await?)
}

fn rows(batches: &[arrow::array::RecordBatch]) -> Value {
    let mut out = Vec::new();
    for b in batches {
        out.extend(burrmill::df::encode::rows(b).unwrap());
    }
    json!(out)
}

async fn rust_client(flight: &str, api: &str) {
    let mut a = client(flight, Some("a")).await;
    let mut b = client(flight, Some("b")).await;

    let (code, want) = post_sql(api, "a", STATEMENT);
    assert_eq!(code, 200, "{want}");
    assert_eq!(
        want["truncated"], false,
        "premise: the whole answer: {want}"
    );
    assert!(
        want["count"].as_u64().unwrap() > 0,
        "premise: rows to compare: {want}"
    );
    let got = run(&mut a, STATEMENT).await.unwrap();
    assert_eq!(rows(&got), want["rows"], "Flight's rows are not /sql's");

    for (c, which) in [(&mut a, "a"), (&mut b, "b")] {
        let got = run(c, "SELECT which FROM which_nest").await.unwrap();
        assert_eq!(
            rows(&got),
            json!([{ "which": which }]),
            "x-nuthatch-route {which}"
        );
    }

    let mut root = client(flight, None).await;
    let err = run(&mut root, "SELECT 1").await.unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::NotFound,
        "a runtime has no /sql at its root: {err}"
    );

    let err = run(&mut a, "SELECT no_such_column FROM tab__transfer")
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument, "{err}");
    let body: Value = serde_json::from_str(err.message()).expect("the status carries /sql's JSON");
    assert!(
        body["error"].as_str().unwrap().contains("no_such_column"),
        "{body}"
    );

    let info = a
        .get_tables(CommandGetTables {
            include_schema: true,
            table_name_filter_pattern: Some("tab%".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    let batches: Vec<_> = a.do_get(ticket).await.unwrap().try_collect().await.unwrap();
    let listed: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(listed, 1, "only tab__transfer matches tab%");
    let schema = arrow::ipc::convert::try_schema_from_ipc_buffer(
        arrow::array::AsArray::as_binary::<i32>(batches[0].column_by_name("table_schema").unwrap())
            .value(0),
    )
    .unwrap();
    let (_, all) = post_sql(api, "a", "SELECT * FROM tab__transfer LIMIT 1");
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        json!(names),
        all["columns"],
        "GetTables' schema is the statement's"
    );
}

#[test]
fn flight_sql_clients_get_what_sql_answers() {
    let go = std::env::var_os("NUTHATCH_FLIGHT_GO_CLIENT");
    if go.is_none() && std::env::var_os("NUTHATCH_REQUIRE_FLIGHT_GO").is_some() {
        panic!(
            "NUTHATCH_REQUIRE_FLIGHT_GO is set but NUTHATCH_FLIGHT_GO_CLIENT is not - the ADBC \
             client would have silently skipped"
        );
    }
    let fixture = Recorded::from_path(Path::new(&format!("{DIR}/chain.json.gz"))).unwrap();
    let tip = fixture.tip();
    let rpc = nuthatch_fixture_rpc::spawn(fixture, "127.0.0.1:0").expect("fixture RPC");

    let dir = tempfile::tempdir().unwrap();
    let runtime = dir.path().join("runtime");
    std::fs::create_dir_all(runtime.join("data")).unwrap();
    let mut table = format!(
        "[runtime]\nname = \"flight\"\n\n[[chains]]\nchain = \"mainnet\"\nchain_id = 1\n\
         rpc_urls = [\"{}\"]\n",
        rpc.url()
    );
    for which in ["a", "b"] {
        table.push_str(&mount(&runtime, &rpc.url(), which));
    }
    std::fs::write(runtime.join("mounts.toml"), table).unwrap();

    let (port, flight_port) = (free_port(), free_port());
    let api = format!("http://127.0.0.1:{port}");
    let flight = format!("127.0.0.1:{flight_port}");
    let _dev = Reaped(
        nuthatch()
            .args(["dev", "--dir"])
            .arg(&runtime)
            .args([
                "--listen",
                &format!("127.0.0.1:{port}"),
                "--flight",
                &flight,
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    // Mainnet's finality is 64 blocks deep, and the recording's tip never moves.
    let deadline = Instant::now() + Duration::from_secs(120);
    for route in ["a", "b"] {
        loop {
            let (_, v) = post_sql(&api, route, "SELECT 1");
            let p = &v["provenance"];
            if p["as_of"].as_u64() == Some(tip) && p["sealed_through"].as_u64() == Some(tip - 64) {
                break;
            }
            assert!(Instant::now() < deadline, "{route} never sealed: {v}");
            std::thread::sleep(Duration::from_millis(250));
        }
    }

    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(rust_client(&flight, &api));

    match go {
        Some(bin) => {
            let status = Command::new(bin)
                .args(["-flight", &flight, "-http", &api, "-token", "t"])
                .status()
                .unwrap();
            assert!(
                status.success(),
                "the ADBC Flight SQL client failed: {status}"
            );
        }
        None => eprintln!(
            "SKIPPED the ADBC client: set NUTHATCH_FLIGHT_GO_CLIENT to tools/flight-client's binary"
        ),
    }
}
