//! `nuthatch diff --flow` over RFC-0071 S0's Euler nest, through the real binary (RFC-0071 §15).
//!
//! The fixture is S0's nest with the segments a seal-direct backfill of the six attack blocks wrote.
//! Every case is compared with a committed golden byte for byte, so the same files are asserted on
//! every platform CI runs; the version is the only substitution.

use std::path::{Path, PathBuf};
use std::process::Command;

const DAI_TX: &str = "0xc310a0affe2169d1f6feec1c63dbc7f7c62a887fa48795d327d4d2da2d6b111d";
const ATTACKER: &str = "0x036cec1a199234fc02f72d29e596a09440825f1c";
const WETH: &str = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2";

const TXS: &[(&str, &str, &str)] = &[
    ("dai-tx", DAI_TX, "16817996"),
    (
        "weth-tx",
        "0x47ac3527d02e6b9631c77fad1cdee7bfa77a8a7bfd4880dccbda5146ace4088f",
        "16818024",
    ),
    (
        "unwrap-tx",
        "0x68eb306c558d45e777c8d588e611db5323a6a5021a9f551bc0be915ef5e24691",
        "16818032",
    ),
    (
        "wbtc-tx",
        "0x71a908be0bef6174bccc3d493becdfd28395d78898e355d451cb52f7bac38617",
        "16818057",
    ),
    (
        "wsteth-tx",
        "0x62bd3d31a7b75c098ccf28bc4d4af8c4a191b4b9e451fab4232258079e8b18c4",
        "16818062",
    ),
    (
        "usdc-tx",
        "0x465a6780145f1efe3ab52f94c006065575712d2003d83d85481f3d110ed131d9",
        "16818065",
    ),
    (
        "steth-tx",
        "0x3097830e9921e4063d334acb82f6a79374f76f0b1a8f857e89b89bc58df1f311",
        "16818085",
    ),
];

pub(crate) fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/diff/euler")
}

pub(crate) fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        let dest = to.join(e.file_name());
        if e.file_type().unwrap().is_dir() {
            copy_dir(&e.path(), &dest);
        } else {
            std::fs::copy(e.path(), dest).unwrap();
        }
    }
}

/// The fixture as `dev` would leave it after sealing these blocks: segments on disk and a store whose
/// watermarks say so. The store is written here because a seal-direct backfill keeps none.
pub(crate) fn nest() -> tempfile::TempDir {
    let tmp = tempfile::tempdir().unwrap();
    for f in ["nuthatch.toml", "metadata.json"] {
        std::fs::copy(fixture().join(f), tmp.path().join(f)).unwrap();
    }
    for d in ["abis", "segments"] {
        copy_dir(&fixture().join(d), &tmp.path().join(d));
    }
    let store = nuthatch::store::Store::open(&tmp.path().join("nuthatch.redb")).unwrap();
    store
        .set_metas(
            &[
                ("start_block", "16817996"),
                ("last_block", "16818085"),
                ("sealed_through", "16818085"),
            ],
            &[],
        )
        .unwrap();
    tmp
}

pub(crate) fn diff(dir: &Path, args: &[&str]) -> (bool, String, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_nuthatch"))
        .arg("diff")
        .arg("--dir")
        .arg(dir)
        // Nothing should answer here: a case that fell through to HTTP must fail, not pass.
        .args(["--url", "http://127.0.0.1:9"])
        .args(args)
        .output()
        .unwrap();
    (
        out.status.success(),
        String::from_utf8(out.stdout).unwrap(),
        String::from_utf8(out.stderr).unwrap(),
    )
}

fn unversioned(s: &str) -> String {
    let v = env!("CARGO_PKG_VERSION");
    s.replace(&format!("nuthatch {v};"), "nuthatch {version};")
        .replace(
            &format!("\"version\": \"{v}\""),
            "\"version\": \"{version}\"",
        )
}

pub(crate) fn assert_golden(name: &str, got: &str) {
    let path = fixture().join("golden").join(name);
    if std::env::var_os("NUTHATCH_BLESS_DIFF").is_some() {
        std::fs::write(&path, unversioned(got)).unwrap();
        return;
    }
    let want = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
    let got = unversioned(got);
    if got != want {
        let line = got
            .lines()
            .zip(want.lines())
            .position(|(a, b)| a != b)
            .map(|i| i + 1);
        panic!(
            "{name} differs from its golden (first differing line {line:?}).\n--- got ---\n{got}"
        );
    }
}

#[test]
fn every_attack_transaction_matches_its_golden() {
    let nest = nest();
    for (name, tx, block) in TXS {
        let (ok, out, err) = diff(nest.path(), &["--flow", "--tx", tx, "--block", block]);
        assert!(ok, "{name}: {err}");
        assert_golden(&format!("{name}.txt"), &out);
    }
}

#[test]
fn the_dai_transaction_matches_in_every_format() {
    let nest = nest();
    for (format, ext) in [("json", "json"), ("dot", "dot"), ("mermaid", "mmd")] {
        let (ok, out, err) = diff(
            nest.path(),
            &[
                "--flow", "--tx", DAI_TX, "--block", "16817996", "--format", format,
            ],
        );
        assert!(ok, "{format}: {err}");
        assert_golden(&format!("dai-tx.{ext}"), &out);
    }
}

/// §14: the attack contract unwrapped 19,639.62 WETH through WETH9 `Withdrawal`, which emits no
/// `Transfer`. Read as a burn, its net WETH over the attack blocks is zero, not that figure.
#[test]
fn the_unwrapped_weth_is_not_left_with_the_attacker() {
    let nest = nest();
    let range = ["--flow", "--from", "16818024", "--to", "16818057"];
    let (ok, out, err) = diff(nest.path(), &[&range[..], &["--format", "json"]].concat());
    assert!(ok, "{err}");
    let doc: serde_json::Value = serde_json::from_str(&out).unwrap();
    let net = doc["net"]
        .as_array()
        .unwrap()
        .iter()
        .find(|n| n["token"] == WETH && n["address"] == ATTACKER)
        .expect("the attacker's WETH net flow");
    assert_eq!(net["received"], "61448430227250683598741");
    assert_eq!(net["net"], "0");
    let unwrapped = doc["edges"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["token"] == WETH && e["from"] == ATTACKER && e["to"] == ZERO)
        .expect("the attacker's WETH9 withdrawals");
    assert_eq!(unwrapped["amount"], "19639624727250683598741");
    assert_eq!(unwrapped["weth9_withdrawals"], 2);
    let rows: u64 = doc["tables"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["rows"].as_u64().unwrap())
        .sum();
    assert_eq!(rows, doc["rows_total"].as_u64().unwrap());
    assert_eq!(rows, 2256, "every row in range is counted in some table");

    let (ok, out, err) = diff(nest.path(), &range);
    assert!(ok, "{err}");
    assert_golden("weth-range.txt", &out);
}

const ZERO: &str = "0x0000000000000000000000000000000000000000";

#[test]
fn a_range_outside_the_nest_is_refused_with_what_it_covers() {
    let nest = nest();
    for range in [["16817990", "16818000"], ["16818080", "16818090"]] {
        let (ok, out, err) = diff(
            nest.path(),
            &["--flow", "--from", range[0], "--to", range[1]],
        );
        assert!(!ok, "{range:?} answered: {out}");
        assert!(out.is_empty());
        assert!(
            err.contains("which is blocks 16,817,996 to 16,818,085"),
            "{err}"
        );
    }
}

#[test]
fn a_tx_with_no_block_and_no_answering_rpc_names_block() {
    let nest = nest();
    let (ok, _, err) = diff(
        nest.path(),
        &["--flow", "--tx", DAI_TX, "--rpc", "http://127.0.0.1:9"],
    );
    assert!(!ok);
    assert!(err.contains("--block"), "{err}");
}
