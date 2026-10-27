//! `nuthatch nest inspect` (RFC-0073 FR10): a nest directory and the bundle made from it describe the
//! same nest, the NID is the one `nest nid` prints, and every egress is listed without its key.

use std::path::Path;
use std::process::Command;

const KEY: &str = "abcDEF1234567890abcdef12";

fn nuthatch(args: &[&str]) -> std::process::Output {
    let out = Command::new(env!("CARGO_BIN_EXE_nuthatch"))
        .args(args)
        .env("NO_COLOR", "1")
        .output()
        .expect("running nuthatch");
    assert!(
        out.status.success(),
        "nuthatch {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    out
}

fn write_nest(nest: &Path) {
    std::fs::create_dir_all(nest.join("abis")).unwrap();
    std::fs::create_dir_all(nest.join("entities")).unwrap();
    std::fs::create_dir_all(nest.join("offchain")).unwrap();
    std::fs::copy(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/erc20.json"),
        nest.join("abis/t.json"),
    )
    .unwrap();
    std::fs::write(
        nest.join("nuthatch.toml"),
        format!(
            r#"[nest]
name = "inspected"
chain = "mainnet"
chain_id = 1
rpc_urls = ["https://eth-mainnet.g.alchemy.com/v2/{KEY}", "https://eth.drpc.org"]

[[contracts]]
alias = "t"
address = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
abi = "abis/t.json"
start_block = 100
events = ["Transfer"]

[[templates]]
name = "child"
abi = "abis/t.json"
events = ["Transfer"]

[[factories]]
watch = "t"
event = "Transfer"
child_param = "to"
template = "child"

[[calls]]
name = "supply"
contract = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
calldata = "0x18160ddd"
every = 100

[[ipfs]]
name = "docs"
on = "t__transfer"
cid_column = "to"

[[webhooks]]
name = "hook"
table = "t__transfer"
url = "https://hooks.example.com/in"
secret = "s3cret"

[[alerts]]
kinds = ["threshold"]
url = "https://alerts.example.net/x"
"#
        ),
    )
    .unwrap();
    std::fs::write(
        nest.join("entities.toml"),
        "[[entities]]\nname = \"totals\"\nsql = \"entities/totals.sql\"\nkey = [\"to\"]\nmax_rows = 10\n",
    )
    .unwrap();
    std::fs::write(
        nest.join("entities/totals.sql"),
        "SELECT \"to\", count(*) AS n FROM t__transfer GROUP BY \"to\"\n",
    )
    .unwrap();
    // One pulled snapshot (egress) and one dropped file (a local path, not egress).
    std::fs::write(
        nest.join("offchain/manifest.json"),
        r#"{"tables":{"prices":[
{"hash":"aa","file":"aa.parquet","rows":1,"columns":["x"],"source":"https://api.example.org/prices?[redacted]","ingested_at":"2026-10-01T00:00:00Z","tool_version":"x"},
{"hash":"bb","file":"bb.parquet","rows":1,"columns":["x"],"source":"/local/prices.csv","ingested_at":"2026-10-01T00:00:00Z","tool_version":"x"}]}}"#,
    )
    .unwrap();
}

fn inspect(target: &Path) -> (serde_json::Value, String) {
    let out = nuthatch(&["nest", "inspect", target.to_str().unwrap(), "--json"]);
    let raw = String::from_utf8(out.stdout).unwrap();
    (serde_json::from_str(&raw).unwrap(), raw)
}

#[test]
fn a_nest_and_its_bundle_inspect_alike() {
    let tmp = tempfile::tempdir().unwrap();
    let nest = tmp.path().join("nest");
    write_nest(&nest);
    let bundle = tmp.path().join("n.bundle");
    nuthatch(&[
        "nest",
        "bundle",
        nest.to_str().unwrap(),
        "--out",
        bundle.to_str().unwrap(),
        "--allow-secrets",
    ]);

    let (from_dir, raw_dir) = inspect(&nest);
    let (mut from_bundle, raw_bundle) = inspect(&bundle);

    assert_eq!(from_dir["schema"], "nuthatch.nest-inspect/1");
    assert!(from_dir["blob_hash"].is_null(), "{raw_dir}");
    assert_eq!(from_bundle["blob_hash"].as_str().unwrap().len(), 64);
    from_bundle["blob_hash"] = serde_json::Value::Null;
    assert_eq!(from_dir, from_bundle, "{raw_dir}\n---\n{raw_bundle}");

    let nid = nuthatch(&["nest", "nid", "--dir", nest.to_str().unwrap()]);
    assert_eq!(
        from_dir["nid"],
        String::from_utf8(nid.stdout).unwrap().trim(),
        "inspect's NID must be the one `nest nid` prints"
    );

    for raw in [&raw_dir, &raw_bundle] {
        assert!(!raw.contains(KEY), "a key reached the output: {raw}");
        assert!(
            !raw.contains("s3cret"),
            "a secret reached the output: {raw}"
        );
    }
    let e = &from_dir["egress"];
    assert_eq!(
        e["rpc_urls"],
        serde_json::json!([
            {"url": "https://eth-mainnet.g.alchemy.com", "redacted": true},
            {"url": "https://eth.drpc.org", "redacted": false},
        ])
    );
    assert_eq!(
        e["webhooks"],
        serde_json::json!([{"name": "hook", "host": "https://hooks.example.com", "secret": true}])
    );
    assert_eq!(
        e["alerts"],
        serde_json::json!([{"kinds": ["threshold"], "host": "https://alerts.example.net"}])
    );
    assert_eq!(e["ipfs"]["declarations"], serde_json::json!(["docs"]));
    assert!(
        !e["ipfs"]["gateways"].as_array().unwrap().is_empty(),
        "an [[ipfs]] nest reaches the default gateways: {raw_dir}"
    );
    assert_eq!(
        e["offchain"],
        serde_json::json!([{"table": "prices", "host": "https://api.example.org"}])
    );

    assert_eq!(
        from_dir["extraction"],
        serde_json::json!([{"declaration": "[[calls]] supply", "needs": "archive"}])
    );
    assert_eq!(
        from_dir["entities"],
        serde_json::json!([{"name": "totals", "key": ["to"], "max_rows": 10}])
    );
    assert_eq!(
        from_dir["factories"],
        serde_json::json!([{"watch": "t", "event": "Transfer", "child_param": "to", "template": "child"}])
    );
    assert_eq!(from_dir["contracts"][0]["start_block"], 100);
    assert_eq!(
        from_dir["templates"][0]["events"],
        serde_json::json!(["Transfer"])
    );
}

#[test]
fn the_text_form_names_every_egress_without_a_key() {
    let tmp = tempfile::tempdir().unwrap();
    let nest = tmp.path().join("nest");
    write_nest(&nest);
    let out = nuthatch(&["nest", "inspect", nest.to_str().unwrap()]);
    let text = String::from_utf8(out.stdout).unwrap();
    assert!(!text.contains(KEY), "{text}");
    for want in [
        "rpc:             https://eth-mainnet.g.alchemy.com (key redacted)",
        "webhook:         https://hooks.example.com",
        "alert sink:      https://alerts.example.net",
        "ipfs gateway:",
        "offchain source: https://api.example.org (prices)",
        "needs archive RPC: [[calls]] supply",
    ] {
        assert!(text.contains(want), "missing {want:?} in:\n{text}");
    }
}
