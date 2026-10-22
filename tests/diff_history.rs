//! `nuthatch diff` over an address-history nest (RFC-0071 §15 step 4): the Euler exploiter's EOA and
//! attack contract, mainnet blocks 16,817,996 to 16,837,739, recorded once from Alchemy by `nuthatch
//! dev` and dumped to `history.json`. The gate holds the diff's native rows against Etherscan's answers
//! for the same addresses and range, pinned beside it; the goldens hold every format; the three
//! surfaces give one document; and a reorg above the finalized head tells the canonical story.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use crate::diff_flow::{assert_golden, diff};

const START: u64 = 16_817_996;
const END: u64 = 16_837_739;
const ATTACK: &str = "0x036cec1a199234fc02f72d29e596a09440825f1c";
const EOA: &str = "0xb2698c2d99ad2c302a95a8db26b08d17a77cedd4";
/// `ethToDeployer()`: the attack contract sends 8,080.50 ETH to the EOA, a row both are filed under.
const PAYOUT_TX: &str = "0xe237c57ccf8664411cae71a50b1e94b21dde7a0e096f0cceaa7f432db8151366";
const PAYOUT_BLOCK: &str = "16818034";

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/diff/history")
}

fn golden(name: &str, got: &str) {
    assert_golden(&format!("../../history/golden/{name}"), got);
}

fn json(out: &str) -> serde_json::Value {
    serde_json::from_str(out).unwrap_or_else(|e| panic!("{e}: {out}"))
}

/// Record the dumped rows, coverage and finalized hashes into `dir`'s store, through the writes the
/// cursor itself makes.
pub(crate) fn record(dir: &Path, dump: &serde_json::Value) {
    use nuthatch::address_history::{Action, AddressHistory, Row};
    let watched: Vec<String> = serde_json::from_value(dump["watched"].clone()).unwrap();
    let store = nuthatch::store::Store::open(&dir.join("nuthatch.redb")).unwrap();
    let h = AddressHistory::open(store, dump["chain_id"].as_u64().unwrap(), &watched).unwrap();
    for c in dump["coverage"].as_array().unwrap() {
        let (address, action) = (
            c["address"].as_str().unwrap(),
            Action::from_etherscan(c["action"].as_str().unwrap()).unwrap(),
        );
        for span in c["spans"].as_array().unwrap() {
            let (f, t) = (span[0].as_u64().unwrap(), span[1].as_u64().unwrap());
            let rows: Vec<Row> = dump["rows"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|r| r["address"] == address && r["action"] == c["action"])
                .filter(|r| (f..=t).contains(&r["block"].as_u64().unwrap()))
                .map(|r| Row {
                    block: r["block"].as_u64().unwrap(),
                    tx_index: r["tx_index"].as_u64().unwrap(),
                    position: r["position"].as_u64().unwrap(),
                    record: r["record"].as_object().unwrap().clone(),
                })
                .collect();
            let g = h.generation().unwrap();
            h.record(action, address, &rows, (f, t), g).unwrap();
        }
    }
    for f in dump["finals"].as_array().unwrap() {
        let mut hash = [0u8; 32];
        hex::decode_to_slice(f[1].as_str().unwrap().trim_start_matches("0x"), &mut hash).unwrap();
        h.finalize_at(f[0].as_u64().unwrap(), hash).unwrap();
    }
}

/// The fixture as `dev` left it: its config, the token metadata, and a store holding the recording.
pub(crate) fn history_nest() -> tempfile::TempDir {
    let tmp = tempfile::tempdir().unwrap();
    for f in ["nuthatch.toml", "metadata.json"] {
        std::fs::copy(fixture().join(f), tmp.path().join(f)).unwrap();
    }
    let dump = json(&std::fs::read_to_string(fixture().join("history.json")).unwrap());
    record(tmp.path(), &dump);
    tmp
}

fn whole() -> Vec<String> {
    vec![
        "--from".into(),
        START.to_string(),
        "--to".into(),
        END.to_string(),
    ]
}

fn args<'a>(base: &'a [String], more: &[&'a str]) -> Vec<&'a str> {
    base.iter()
        .map(String::as_str)
        .chain(more.iter().copied())
        .collect()
}

/// Etherscan's rows for one address and action, as pinned beside the fixture.
fn etherscan(address: &str, action: &str) -> Vec<serde_json::Value> {
    let path = fixture().join(format!("etherscan/{address}-{action}.json"));
    let v = json(&std::fs::read_to_string(path).unwrap());
    assert_eq!(v["status"], "1", "{address} {action}: {v}");
    v["result"].as_array().unwrap().clone()
}

type Line = (u64, String, String, String, String, String, bool);

/// A native row as the gate compares it: block, transaction, from, to (or the contract a creation
/// made), wei, kind, reverted.
fn ours(n: &serde_json::Value) -> Line {
    (
        n["block"].as_u64().unwrap(),
        n["tx_hash"].as_str().unwrap().into(),
        n["from"].as_str().unwrap().into(),
        n["to"].as_str().unwrap().into(),
        n["value"].as_str().unwrap().into(),
        n["kind"].as_str().unwrap().into(),
        n["reverted"].as_bool().unwrap(),
    )
}

fn theirs(r: &serde_json::Value, kind: &str) -> Line {
    let to = match r["to"].as_str().unwrap() {
        "" => r["contractAddress"].as_str().unwrap(),
        t => t,
    };
    (
        r["blockNumber"].as_str().unwrap().parse().unwrap(),
        r["hash"].as_str().unwrap().to_ascii_lowercase(),
        r["from"].as_str().unwrap().to_ascii_lowercase(),
        to.to_ascii_lowercase(),
        r["value"].as_str().unwrap().into(),
        if kind.is_empty() {
            r["type"].as_str().unwrap().into()
        } else {
            kind.into()
        },
        r["isError"] == "1",
    )
}

/// The gate (RFC-0071 §11 S5): for each watched address over the range, the diff's internal-transfer
/// lines are Etherscan's `txlistinternal` and its transaction-value lines are Etherscan's `txlist` rows
/// that carried value, row for row and wei for wei. Token transfers are held to `tokentx` the same way.
#[test]
fn the_native_rows_are_etherscans_row_for_row_and_wei_for_wei() {
    let nest = history_nest();
    let (ok, out, err) = diff(
        nest.path(),
        &args(&whole(), &["--format", "json", "--top", "100000"]),
    );
    assert!(ok, "{err}");
    let doc = json(&out);
    let h = &doc["history"];
    let native = h["native"].as_array().unwrap();
    assert_eq!(native.len() as u64, h["native_rows"].as_u64().unwrap());
    let touches = |n: &serde_json::Value, a: &str| n["from"] == a || n["to"] == a;
    let mut internal_union = std::collections::BTreeSet::new();
    let mut value_union = std::collections::BTreeSet::new();
    for a in [ATTACK, EOA] {
        let mut want: Vec<Line> = etherscan(a, "txlistinternal")
            .iter()
            .map(|r| theirs(r, ""))
            .collect();
        let mut got: Vec<Line> = native
            .iter()
            .filter(|n| n["kind"] != "transaction" && touches(n, a))
            .map(ours)
            .collect();
        want.sort();
        got.sort();
        assert_eq!(got, want, "{a}: internal transfers");
        internal_union.extend(want);

        let mut want: Vec<Line> = etherscan(a, "txlist")
            .iter()
            .filter(|r| r["value"] != "0")
            .map(|r| theirs(r, "transaction"))
            .collect();
        let mut got: Vec<Line> = native
            .iter()
            .filter(|n| n["kind"] == "transaction" && touches(n, a))
            .map(ours)
            .collect();
        want.sort();
        got.sort();
        assert_eq!(got, want, "{a}: transaction values");
        value_union.extend(want);
    }
    assert_eq!(native.len(), internal_union.len() + value_union.len());

    // Every row Etherscan lists for either address is one row of the diff's counts, once. Etherscan
    // gives no log index and no unique trace id, so a row both addresses list is matched by its fields,
    // and a key one list holds twice (two equal transfers in one transaction) counts twice.
    let actions = h["actions"].as_array().unwrap();
    for (i, action) in ["txlist", "txlistinternal", "tokentx"].iter().enumerate() {
        let mut union: std::collections::BTreeMap<String, usize> = Default::default();
        for a in [ATTACK, EOA] {
            let mut here: std::collections::BTreeMap<String, usize> = Default::default();
            for r in etherscan(a, action) {
                let key = ["hash", "from", "to", "value", "contractAddress", "traceId"]
                    .map(|k| r[k].to_string())
                    .join("|");
                *here.entry(key).or_default() += 1;
            }
            for (k, n) in here {
                let u = union.entry(k).or_default();
                *u = (*u).max(n);
            }
        }
        assert_eq!(actions[i]["action"], *action);
        assert_eq!(
            actions[i]["rows"].as_u64().unwrap(),
            union.values().sum::<usize>() as u64,
            "{action}"
        );
    }

    // And each token's net flow per address is Etherscan's, to the base unit.
    for a in [ATTACK, EOA] {
        let mut want: std::collections::BTreeMap<String, i128> = Default::default();
        for r in etherscan(a, "tokentx") {
            let v: i128 = r["value"].as_str().unwrap().parse().unwrap();
            let token = r["contractAddress"].as_str().unwrap().to_ascii_lowercase();
            if r["to"] == a {
                *want.entry(token.clone()).or_default() += v;
            }
            if r["from"] == a {
                *want.entry(token).or_default() -= v;
            }
        }
        let got: std::collections::BTreeMap<String, i128> = doc["movements"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|t| t["address"] != "native")
            .filter_map(|t| {
                let n = t["net"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|n| n["address"] == a)?;
                Some((
                    t["address"].as_str().unwrap().to_string(),
                    n["net"].as_str().unwrap().parse().unwrap(),
                ))
            })
            .collect();
        want.retain(|_, v| *v != 0);
        assert_eq!(got, want, "{a}: token net flow");
    }
}

#[test]
fn the_history_fixture_matches_its_goldens_twice() {
    let nest = history_nest();
    for _ in 0..2 {
        let (ok, out, err) = diff(nest.path(), &args(&whole(), &[]));
        assert!(ok, "{err}");
        golden("story-range.txt", &out);
        for (format, ext) in [("markdown", "md"), ("json", "json")] {
            let (ok, out, err) = diff(nest.path(), &args(&whole(), &["--format", format]));
            assert!(ok, "{format}: {err}");
            golden(&format!("story-range.{ext}"), &out);
        }
        let (ok, out, err) = diff(nest.path(), &["--tx", PAYOUT_TX, "--block", PAYOUT_BLOCK]);
        assert!(ok, "{err}");
        golden("story-payout-tx.txt", &out);
        for (format, ext) in [("text", "txt"), ("json", "json"), ("dot", "dot")] {
            let (ok, out, err) = diff(
                nest.path(),
                &args(&whole(), &["--flow", "--format", format]),
            );
            assert!(ok, "{format}: {err}");
            golden(&format!("flow-range.{ext}"), &out);
        }
        let (ok, out, err) = diff(
            nest.path(),
            &["--flow", "--tx", PAYOUT_TX, "--block", PAYOUT_BLOCK],
        );
        assert!(ok, "{err}");
        golden("flow-payout-tx.txt", &out);
    }
}

#[test]
fn a_range_the_nest_has_not_read_is_refused_and_so_is_final_only_above_the_head() {
    let nest = history_nest();
    let (ok, out, err) = diff(nest.path(), &["--from", "16817990", "--to", "16818000"]);
    assert!(!ok, "{out}");
    assert!(err.contains("blocks 16,817,990 to 16,817,995 of"), "{err}");
    assert!(
        err.contains("not indexed (this nest has 16,817,996 to 16,837,739)"),
        "{err}"
    );
    let (ok, _, err) = diff(nest.path(), &args(&whole(), &["--final-only"]));
    assert!(ok, "the recording is final: {err}");
}

// ---------------------------------------------------------------------------------------------------
// The three surfaces.

struct Dev {
    child: std::process::Child,
    port: u16,
    _dir: tempfile::TempDir,
}

impl Drop for Dev {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// `nuthatch dev` on a copy of the fixture with an RPC that answers nothing: covered history still
/// serves, and nothing the cursor could fetch can change the store under the test.
fn dev(dir: tempfile::TempDir) -> Dev {
    let port = crate::diff_range::free_port();
    let child = Command::new(env!("CARGO_BIN_EXE_nuthatch"))
        .args(["dev", "--dir"])
        .arg(dir.path())
        .args(["--rpc", "http://127.0.0.1:9"])
        .args(["--listen", &format!("127.0.0.1:{port}")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let dev = Dev {
        child,
        port,
        _dir: dir,
    };
    for _ in 0..600 {
        if crate::diff_range::http_get(port, "/health").is_ok_and(|(s, _)| s == 200) {
            return dev;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    panic!("dev did not come up on {port}");
}

/// §3 for an address-history nest: the CLI's local read, `GET /diff` and the MCP tool give one
/// document, byte for byte.
#[test]
fn the_cli_the_route_and_the_mcp_tool_give_the_same_history_document() {
    let local = history_nest();
    let served = dev(history_nest());
    let range = format!("from={START}&to={END}");
    let cases: Vec<(Vec<String>, String, serde_json::Value)> = vec![
        (
            whole(),
            format!("/diff?{range}"),
            serde_json::json!({ "from": START, "to": END }),
        ),
        (
            vec![
                "--tx".into(),
                PAYOUT_TX.into(),
                "--block".into(),
                PAYOUT_BLOCK.into(),
            ],
            format!("/diff?tx={PAYOUT_TX}&block={PAYOUT_BLOCK}"),
            serde_json::json!({ "tx": PAYOUT_TX, "block": 16818034 }),
        ),
        (
            [whole(), vec!["--flow".into(), "--top".into(), "3".into()]].concat(),
            format!("/diff?{range}&flow=true&top=3"),
            serde_json::json!({ "from": START, "to": END, "flow": true, "top": 3 }),
        ),
    ];
    for (a, path, mcp_args) in cases {
        let (ok, cli, err) = diff(local.path(), &args(&a, &["--format", "json"]));
        assert!(ok, "{a:?}: {err}");
        let (status, route) = crate::diff_range::http_get(served.port, &path).unwrap();
        assert_eq!(status, 200, "{path}: {route}");
        assert_eq!(route, cli, "{path}: the route and the CLI differ");
        let tool = crate::diff_range::mcp_diff(served.port, mcp_args);
        assert_eq!(tool, cli, "{path}: the MCP tool and the CLI differ");
    }
    let over = crate::diff_range::cli_over_http(served.port, &args(&whole(), &[]));
    let (_, cli, _) = diff(local.path(), &args(&whole(), &[]));
    assert_eq!(over, cli, "the CLI over --url prints the local text");
    let over = crate::diff_range::cli_over_http(
        served.port,
        &["--flow", "--tx", PAYOUT_TX, "--block", PAYOUT_BLOCK],
    );
    let (_, cli, _) = diff(
        local.path(),
        &["--flow", "--tx", PAYOUT_TX, "--block", PAYOUT_BLOCK],
    );
    assert_eq!(over, cli, "the flow over --url is the route's");
}

// ---------------------------------------------------------------------------------------------------
// Reorgs above the finalized head.

const ME: &str = "0x00000000000000000000000000000000000000aa";
const HEAD: u64 = 105;
const TIP: u64 = 111;

/// One block's rows on `fork`: a value transaction and an internal transfer into the watched
/// address, and a token transfer out of it, all fork-specific in hash and amount.
fn block_rows(
    b: u64,
    fork: u64,
) -> Vec<(
    nuthatch::address_history::Action,
    nuthatch::address_history::Row,
)> {
    use nuthatch::address_history::{Action, Row};
    let hash = format!("0x{:064x}", b * 1000 + fork);
    let bh = format!("0x{:064x}", b * 7 + fork);
    let rec = |v: serde_json::Value| v.as_object().unwrap().clone();
    vec![
        (
            Action::TxList,
            Row {
                block: b,
                tx_index: 0,
                position: 0,
                record: rec(
                    serde_json::json!({"blockNumber": b.to_string(), "blockHash": bh,
                    "hash": hash, "from": "0x00000000000000000000000000000000000000bb", "to": ME,
                    "value": (b * 10 + fork).to_string(), "isError": "0", "contractAddress": ""}),
                ),
            },
        ),
        (
            Action::TxListInternal,
            Row {
                block: b,
                tx_index: 0,
                position: 2,
                record: rec(
                    serde_json::json!({"blockNumber": b.to_string(), "hash": hash,
                    "from": "0x00000000000000000000000000000000000000cc", "to": ME,
                    "value": (fork + 1).to_string(), "type": "call", "isError": "0",
                    "contractAddress": ""}),
                ),
            },
        ),
        (
            Action::TokenTx,
            Row {
                block: b,
                tx_index: 0,
                position: 3 << 16,
                record: rec(
                    serde_json::json!({"blockNumber": b.to_string(), "blockHash": bh,
                    "hash": hash, "from": ME, "to": "0x00000000000000000000000000000000000000dd",
                    "value": (b + fork).to_string(),
                    "contractAddress": "0x00000000000000000000000000000000000000ee",
                    "logIndex": "3"}),
                ),
            },
        ),
    ]
}

fn record_blocks(h: &nuthatch::address_history::AddressHistory, blocks: &[(u64, u64)]) {
    use nuthatch::address_history::Action;
    let (lo, hi) = (blocks[0].0, blocks[blocks.len() - 1].0);
    for action in [Action::TxList, Action::TxListInternal, Action::TokenTx] {
        let rows: Vec<_> = blocks
            .iter()
            .flat_map(|(b, f)| block_rows(*b, *f))
            .filter(|(a, _)| *a == action)
            .map(|(_, r)| r)
            .collect();
        let g = h.generation().unwrap();
        h.record(action, ME, &rows, (lo, hi), g).unwrap();
    }
}

fn store(dir: &Path) -> nuthatch::address_history::AddressHistory {
    let s = nuthatch::store::Store::open(&dir.join("nuthatch.redb")).unwrap();
    let h = nuthatch::address_history::AddressHistory::open(s, 1, &[ME.into()]).unwrap();
    h.finalize_at(HEAD, [7; 32]).unwrap();
    h
}

fn story(
    dir: &Path,
    h: &nuthatch::address_history::AddressHistory,
    final_only: bool,
) -> anyhow::Result<String> {
    use nuthatch::diff;
    let facts = diff::Facts {
        name: "reorg".into(),
        chain: "mainnet".into(),
        chain_id: Some(1),
        start_block: Some(100),
        ..Default::default()
    };
    let rows = h.diff_snapshot((101, TIP))?;
    let ask = diff::Ask {
        req: diff::Request {
            from: 101,
            to: TIP,
            tx: None,
        },
        mode: diff::Mode::Story,
        top: 10,
        final_only,
    };
    let rt = tokio::runtime::Builder::new_current_thread().build()?;
    let produced = rt.block_on(nuthatch::diff_history::produce(dir, facts, &rows, &ask))?;
    produced.render(diff::Format::Json, 10)
}

proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig::with_cases(12))]

    /// Rows above the finalized head are read, labelled provisional, and after a reorg of any depth
    /// that stays above it, tell the story a store holding only the new chain tells.
    #[test]
    fn a_reorg_above_the_finalized_head_tells_the_canonical_story(depth in 1u64..=6, fork in 1u64..=3) {
        let first: Vec<(u64, u64)> = (101..=TIP).map(|b| (b, 0)).collect();
        let fork_at = TIP - depth;
        let canonical: Vec<(u64, u64)> = (101..=TIP)
            .map(|b| (b, if b > fork_at { fork } else { 0 }))
            .collect();

        let reorged = tempfile::tempdir().unwrap();
        let h = store(reorged.path());
        record_blocks(&h, &first);
        let before = story(reorged.path(), &h, false).unwrap();
        h.invalidate_above(fork_at).unwrap();
        record_blocks(&h, &canonical[(fork_at - 100) as usize..]);
        let after = story(reorged.path(), &h, false).unwrap();

        let fresh = tempfile::tempdir().unwrap();
        let f = store(fresh.path());
        record_blocks(&f, &canonical);
        let want = story(fresh.path(), &f, false).unwrap();

        proptest::prop_assert_eq!(&after, &want);
        proptest::prop_assert_ne!(&after, &before, "the reorg changed nothing the story tells");
        let doc: serde_json::Value = serde_json::from_str(&after).unwrap();
        proptest::prop_assert_eq!(&doc["coverage"]["final"], false);
        let e = story(reorged.path(), &h, true).unwrap_err().to_string();
        proptest::prop_assert!(e.contains("not final (finalized through 105)"), "{}", e);
    }
}
