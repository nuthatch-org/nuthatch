//! RFC-0077 S5's parity gate, the node's half. The binary's tests build their chains in code on
//! `TapeSource`; this writes those chains out, era by era, as fixture-rpc recordings, beside each
//! shape's nest and the requests it is asked, under `tests/fixtures/embed-shapes/<shape>/`. Then the
//! real binary follows each recording through fixture-rpc, era by era, and its answers must be the
//! ones `expected.json` pins. The module's half (`embed/hosts/node/shapes.mjs`) is held to the same
//! pins, through the same recordings.
//!
//! `NUTHATCH_PIN_EMBED_SHAPES=1` writes the recordings, nests, requests and pins from this run;
//! otherwise the recordings, nests and requests must be what this file builds, so a pin cannot drift
//! from the code that made it.

mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use common::tape::*;
use nuthatch_fixture_rpc::recorded::Recorded;
use serde_json::{json, Value};

const DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/embed-shapes");
const SCAFFOLD: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/scaffold");
/// As `tests/e2e_tab_answers.rs` masks them: the clock, the version, the NID and the getLogs pacing.
const MASKED: [&str; 7] = [
    "fetch_window_blocks",
    "version",
    "last_poll_unixtime",
    "seconds_since_poll",
    "seconds_since_seal_progress",
    "seconds_since_progress",
    "nid",
];

/// One shape: its nest, the chain as each era serves it, and what is asked after each era.
struct Shape {
    name: &'static str,
    chain_id: u64,
    /// Where the nest's files are: a directory already in the tree, or written by `write`.
    nest: Nest,
    eras: Vec<(u64, u64, Vec<(u64, BlockFixture)>)>,
    requests: Vec<Value>,
}

enum Nest {
    Tree(&'static str),
    Written(Box<dyn Fn(&Path)>),
}

fn get(path: &str) -> Value {
    json!({"method": "GET", "path": path})
}

fn sql(q: &str) -> Value {
    let enc: String = q
        .bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect();
    get(&format!("/sql?q={enc}"))
}

/// What every shape is asked of each table: its rows in chain order, and its count.
fn tables(names: &[&str]) -> Vec<Value> {
    names
        .iter()
        .flat_map(|t| {
            [
                sql(&format!("SELECT count(*) AS n FROM {t}")),
                sql(&format!(
                    "SELECT * FROM {t} ORDER BY block_number, log_index LIMIT 200"
                )),
            ]
        })
        .collect()
}

fn common_requests(names: &[&str]) -> Vec<Value> {
    let mut r = vec![get("/ready")];
    r.extend(tables(names));
    r.extend([
        get("/balances?limit=20"),
        get("/entities?limit=5"),
        get("/derived"),
    ]);
    r
}

fn snapshot(tape: &TapeSource) -> (u64, u64, Vec<(u64, BlockFixture)>) {
    tape.snapshot()
}

fn arbitrum_toml(name: &str, extra: &str) -> String {
    format!(
        "[nest]\nname = \"{name}\"\nchain = \"arbitrum-one\"\nchain_id = 42161\nrpc_urls = []\n\n\
         [[contracts]]\nalias = \"{name}\"\naddress = \"{USDC}\"\nstart_block = 1\n\
         abi = \"abis/erc20.json\"\nevents = [\"Transfer\"]\n{extra}"
    )
}

fn erc20_abi(dir: &Path) {
    std::fs::create_dir_all(dir.join("abis")).unwrap();
    std::fs::copy(
        concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/erc20.json"),
        dir.join("abis/erc20.json"),
    )
    .unwrap();
}

/// `tests/e2e_reorg.rs`'s blocks.
fn canonical_block(b: u64) -> BlockFixture {
    transfers_block(
        b,
        0,
        1_700_000_000 + b,
        USDC,
        &[(account(1).as_str(), account(2).as_str(), (100 * b) as u128)],
    )
}

fn replacement_block(b: u64) -> BlockFixture {
    transfers_block(
        b,
        1,
        1_700_000_500 + b,
        USDC,
        &[(
            account(3).as_str(),
            account(4).as_str(),
            (7_000 + b) as u128,
        )],
    )
}

/// `tests/e2e_reorg.rs`'s property: a 24-block chain reorged above a fork, one case per depth band,
/// the depths its `reorg_depth` draws at the ends and the middle of each.
fn reorg(depth: u64) -> Shape {
    const LEN: u64 = 24;
    let tape = TapeSource::new();
    for b in 1..=LEN {
        tape.insert_block(b, canonical_block(b));
    }
    tape.advance_tip_to(LEN);
    let before = snapshot(&tape);
    let fork = LEN - depth;
    tape.reorg(fork, ((fork + 1)..=LEN).map(replacement_block).collect());
    let after = snapshot(&tape);
    Shape {
        name: Box::leak(format!("reorg-depth-{depth}").into_boxed_str()),
        chain_id: 42161,
        nest: Nest::Written(Box::new(|d| {
            erc20_abi(d);
            std::fs::write(d.join("nuthatch.toml"), arbitrum_toml("usdc", "")).unwrap();
        })),
        eras: vec![before, after],
        requests: common_requests(&["usdc__transfer"]),
    }
}

/// `tests/e2e_reorg.rs`'s `reorg_above_the_seal_boundary_leaves_segments_byte_identical`: blocks 1..=8
/// sealed (block 8 padded to a seal batch), then a reorg above the boundary, which must not halt.
fn reorg_above_seal() -> Shape {
    let tape = TapeSource::new();
    for b in 1..=10 {
        let mut fx = canonical_block(b);
        if b == 8 {
            pad_address_to_seal(&mut fx, USDC, 7);
        }
        tape.insert_block(b, fx);
    }
    tape.advance_tip_to(10);
    let landed = snapshot(&tape);
    tape.advance_finalized_to(8);
    tape.insert_block(11, empty_block(11, 0, 1_700_000_111));
    tape.insert_block(12, empty_block(12, 0, 1_700_000_112));
    tape.advance_tip_to(12);
    let sealed = snapshot(&tape);
    let mut replacement: Vec<BlockFixture> = (11..=12).map(replacement_block).collect();
    replacement.push(replacement_block(13));
    tape.reorg(10, replacement);
    let reorged = snapshot(&tape);
    let mut requests = common_requests(&["usdc__transfer"]);
    requests.push(sql(
        "SELECT block_number, count(*) AS n, sum(CAST(value AS DOUBLE)) AS v FROM usdc__transfer \
         GROUP BY 1 ORDER BY 1",
    ));
    Shape {
        name: "reorg-above-seal",
        chain_id: 42161,
        nest: Nest::Written(Box::new(|d| {
            erc20_abi(d);
            std::fs::write(d.join("nuthatch.toml"), arbitrum_toml("usdc", "")).unwrap();
        })),
        eras: vec![landed, sealed, reorged],
        requests,
    }
}

/// `tests/e2e_follower.rs`'s derived nest: threshold and velocity flags, a labelled address, a chain
/// entity, blocks 1..=8 with block 2 padded so its range seals. Its offchain entity is left out: an
/// embedding binds no offchain tables (RFC-0077 S1).
fn labels() -> Shape {
    let block = |b: u64, variant: u64, value: u128| {
        transfers_block(
            b,
            variant,
            1_700_000_000 + b,
            USDC,
            &[(account(1).as_str(), account(2).as_str(), value)],
        )
    };
    let tape = TapeSource::new();
    for b in 1..=8u64 {
        let mut fx = block(b, 0, 100 * b as u128);
        if b == 2 {
            pad_address_to_seal(&mut fx, USDC, 1);
        }
        tape.insert_block(b, fx);
    }
    tape.advance_tip_to(8);
    let landed = snapshot(&tape);
    tape.advance_finalized_to(2);
    for b in 9..=10 {
        tape.insert_block(
            b,
            transfers_block(
                b,
                0,
                1_700_000_000 + b,
                USDC,
                &[(account(2).as_str(), account(3).as_str(), 50)],
            ),
        );
    }
    tape.advance_tip_to(10);
    let sealed = snapshot(&tape);
    let mut requests = common_requests(&["usdc__transfer"]);
    requests.extend([
        sql("SELECT * FROM labels ORDER BY address"),
        sql(
            "SELECT l.label, count(*) AS n FROM usdc__transfer t JOIN labels l \
             ON t.\"to\" = l.address GROUP BY 1 ORDER BY 1",
        ),
        get(&format!("/balance/{}", account(2))),
        get("/derived/received?limit=10"),
        get(&format!("/derived/received/{}", account(3))),
    ]);
    Shape {
        name: "labels",
        chain_id: 42161,
        nest: Nest::Written(Box::new(|d| {
            erc20_abi(d);
            std::fs::write(
                d.join("nuthatch.toml"),
                arbitrum_toml(
                    "usdc",
                    "\n[flags]\nthreshold = \"150\"\nvelocity_amount = \"300\"\nvelocity_window = 100\n",
                ),
            )
            .unwrap();
            std::fs::create_dir_all(d.join("labels")).unwrap();
            std::fs::write(
                d.join("labels/fixture.json"),
                json!([{"address": account(2), "label": "mixer"}]).to_string(),
            )
            .unwrap();
            std::fs::create_dir_all(d.join("entities")).unwrap();
            std::fs::write(
                d.join("entities.toml"),
                "[[entities]]\nname = \"received\"\nsql = \"entities/received.sql\"\nkey = [\"to\"]\nmax_rows = 50000\n",
            )
            .unwrap();
            std::fs::write(
                d.join("entities/received.sql"),
                "SELECT t.to, SUM(t.value) FROM usdc__transfer t GROUP BY t.to",
            )
            .unwrap();
        })),
        eras: vec![landed, sealed],
        requests,
    }
}

/// A factory nest as `tests/e2e_nest_inspect.rs` declares one: a parent ERC-20 whose `Transfer`'s `to`
/// is a child of the `child` template. Two children are found; a reorg then takes the second's
/// discovery back and finds a third instead.
fn factory() -> Shape {
    let child = |n: u64| account(0xc000 + n);
    let parent = |b: u64, variant: u64, to: &str| {
        transfers_block(
            b,
            variant,
            1_700_000_000 + b,
            USDC,
            &[(account(1).as_str(), to, 1)],
        )
    };
    let child_block = |b: u64, variant: u64, c: &str, value: u128| {
        transfers_block(
            b,
            variant,
            1_700_000_000 + b,
            c,
            &[(account(3).as_str(), account(4).as_str(), value)],
        )
    };
    let tape = TapeSource::new();
    tape.insert_block(1, parent(1, 0, &child(1)));
    tape.insert_block(2, child_block(2, 0, &child(1), 20));
    tape.insert_block(3, parent(3, 0, &child(2)));
    for b in 4..=6 {
        let mut fx = child_block(b, 0, &child(1), 10 * b as u128);
        let other = child_block(b, 0, &child(2), 100 * b as u128);
        fx.logs.extend(other.logs.into_iter().map(|mut l| {
            l.log_index += 1;
            l.tx_hash = format!("0x{:064x}", (b << 20) | l.log_index);
            l
        }));
        tape.insert_block(b, fx);
    }
    tape.advance_tip_to(6);
    let found = snapshot(&tape);
    tape.reorg(
        2,
        vec![
            parent(3, 1, &child(3)),
            child_block(4, 1, &child(3), 7),
            child_block(5, 1, &child(1), 5),
            child_block(6, 1, &child(3), 9),
            child_block(7, 1, &child(1), 11),
        ],
    );
    let reorged = snapshot(&tape);
    let mut requests = common_requests(&["f__transfer", "child__transfer"]);
    requests.push(sql("SELECT * FROM child__children ORDER BY address"));
    Shape {
        name: "factory",
        chain_id: 42161,
        nest: Nest::Written(Box::new(|d| {
            erc20_abi(d);
            let toml = format!(
                "[nest]\nname = \"factory\"\nchain = \"arbitrum-one\"\nchain_id = 42161\nrpc_urls = []\n\n\
                 [[contracts]]\nalias = \"f\"\naddress = \"{USDC}\"\nstart_block = 1\n\
                 abi = \"abis/erc20.json\"\nevents = [\"Transfer\"]\n\n\
                 [[templates]]\nname = \"child\"\nabi = \"abis/erc20.json\"\nevents = [\"Transfer\"]\n\n\
                 [[factories]]\nwatch = \"f\"\nevent = \"Transfer\"\nchild_param = \"to\"\ntemplate = \"child\"\n"
            );
            std::fs::write(d.join("nuthatch.toml"), toml).unwrap();
        })),
        eras: vec![found, reorged],
        requests,
    }
}

/// A Uniswap-V2 pair's `Sync` and `Transfer` over 2,000 mainnet blocks from the scaffold's start, so
/// mainnet's 1,800-block span cuts a segment past its 64-block depth.
fn pair_chain(start: u64, pair: &str) -> TapeSource {
    let tape = TapeSource::new();
    for b in start..start + 2_000 {
        let i = b - start;
        let ts = 1_760_000_000 + 12 * i;
        let mut fx = match i % 7 {
            0 => sync_block(b, 0, ts, pair, &[(1_000 + i as u128, 2_000 - i as u128)]),
            3 => transfers_block(
                b,
                0,
                ts,
                pair,
                &[(
                    account(i % 5).as_str(),
                    account(1 + i % 3).as_str(),
                    i as u128,
                )],
            ),
            _ => empty_block(b, 0, ts),
        };
        if i % 7 == 0 && i % 2 == 0 {
            let t = transfers_block(
                b,
                0,
                ts,
                pair,
                &[(account(9).as_str(), account(i % 4).as_str(), 3 * i as u128)],
            );
            fx.logs.extend(t.logs.into_iter().map(|mut l| {
                l.log_index = 1;
                l.tx_hash = format!("0x{:064x}", (b << 20) | 1);
                l
            }));
        }
        tape.insert_block(b, fx);
    }
    tape.advance_tip_to(start + 1_999);
    tape
}

fn pair() -> Shape {
    let tape = pair_chain(26_212_134, "0x9d593420cc4936b8a8435cdfa0d6bbde9bdff9a4");
    let mut requests = common_requests(&["uniswap_v2pair__sync", "uniswap_v2pair__transfer"]);
    requests.push(sql(
        "SELECT block_number, reserve0, reserve1 FROM uniswap_v2pair__sync \
         ORDER BY block_number DESC LIMIT 3",
    ));
    Shape {
        name: "pair",
        chain_id: 1,
        nest: Nest::Tree("scaffold/pair"),
        eras: vec![snapshot(&tape)],
        requests,
    }
}

/// The two-contract scaffold, its start blocks moved together so one recording covers both: USDC's
/// 2018 deploy block is four million blocks before the pool's, which no recording carries.
fn two() -> Shape {
    let (usdc, pool) = (
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc",
    );
    let start = 10_008_355;
    let tape = TapeSource::new();
    for b in start..start + 2_000 {
        let i = b - start;
        let ts = 1_590_000_000 + 13 * i;
        let fx = match i % 5 {
            0 => transfers_block(
                b,
                0,
                ts,
                usdc,
                &[(
                    account(1).as_str(),
                    account(2 + i % 3).as_str(),
                    1_000 * i as u128,
                )],
            ),
            2 => sync_block(b, 0, ts, pool, &[(i as u128, 10 * i as u128)]),
            _ => empty_block(b, 0, ts),
        };
        tape.insert_block(b, fx);
    }
    tape.advance_tip_to(start + 1_999);
    let mut requests = common_requests(&["usdc__transfer", "pool__sync"]);
    requests.push(get(&format!("/balance/{}", account(3))));
    Shape {
        name: "two",
        chain_id: 1,
        nest: Nest::Written(Box::new(move |d| {
            copy_dir(&Path::new(SCAFFOLD).join("two"), d);
            let toml = std::fs::read_to_string(d.join("nuthatch.toml"))
                .unwrap()
                .replace("start_block = 6082465", &format!("start_block = {start}"));
            std::fs::write(d.join("nuthatch.toml"), toml).unwrap();
        })),
        eras: vec![snapshot(&tape)],
        requests,
    }
}

fn shapes() -> Vec<Shape> {
    let mut v = vec![pair(), two(), labels(), factory(), reorg_above_seal()];
    // One depth from each of `reorg_depth`'s bands, at their edges and middles.
    for depth in [1, 2, 3, 7, 12, 13, 18, 23] {
        v.push(reorg(depth));
    }
    v
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap().flatten() {
        let p = e.path();
        if p.is_dir() {
            copy_dir(&p, &to.join(e.file_name()));
        } else {
            std::fs::copy(&p, to.join(e.file_name())).unwrap();
        }
    }
}

fn hex(n: u64) -> String {
    format!("0x{n:x}")
}

/// The recording fixture-rpc serves: one era per snapshot, every block up to its tip.
fn recording(s: &Shape) -> Value {
    let eras: Vec<Value> = s
        .eras
        .iter()
        .map(|(tip, finalized, blocks)| {
            let headers: Vec<Value> = blocks
                .iter()
                .map(|(n, b)| json!([hex(*n), b.hash, hex(b.timestamp)]))
                .collect();
            let logs: Vec<Value> = blocks
                .iter()
                .flat_map(|(_, b)| &b.logs)
                .map(|l| {
                    json!({
                        "address": l.address,
                        "topics": l.topics,
                        "data": l.data,
                        "blockNumber": hex(l.block_number),
                        "blockHash": l.block_hash,
                        "transactionHash": l.tx_hash,
                        "transactionIndex": hex(l.log_index),
                        "logIndex": hex(l.log_index),
                        "removed": false,
                    })
                })
                .collect();
            json!({"tip": tip, "finalized": finalized, "headers": headers, "logs": logs})
        })
        .collect();
    json!({"chain_id": s.chain_id, "eras": eras})
}

fn gz(text: &str) -> Vec<u8> {
    use std::io::Write;
    let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
    e.write_all(text.as_bytes()).unwrap();
    e.finish().unwrap()
}

fn gunzip(bytes: &[u8]) -> String {
    use std::io::Read;
    let mut s = String::new();
    flate2::read::GzDecoder::new(bytes)
        .read_to_string(&mut s)
        .unwrap();
    s
}

/// Every file under `dir`, by relative path.
fn files(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    fn walk(root: &Path, rel: &Path, out: &mut BTreeMap<String, Vec<u8>>) {
        for e in std::fs::read_dir(root.join(rel)).unwrap().flatten() {
            let r = rel.join(e.file_name());
            if e.path().is_dir() {
                walk(root, &r, out);
            } else {
                out.insert(
                    r.to_string_lossy().into_owned(),
                    std::fs::read(e.path()).unwrap(),
                );
            }
        }
    }
    let mut out = BTreeMap::new();
    if dir.exists() {
        walk(dir, Path::new(""), &mut out);
    }
    out
}

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

fn mask(body: &str) -> String {
    let mut out = body.to_string();
    for key in MASKED {
        let needle = format!("\"{key}\":");
        let mut from = 0;
        while let Some(at) = out[from..].find(&needle) {
            let start = from + at + needle.len();
            let rest = &out[start..];
            let len = match rest.strip_prefix('"') {
                Some(text) => text.find('"').map_or(rest.len(), |i| i + 2),
                None => rest.find([',', '}']).unwrap_or(rest.len()),
            };
            out.replace_range(start..start + len, "\"<masked>\"");
            from = start;
        }
    }
    out
}

fn curl(url: &str, method: &str, body: Option<&str>, out: &Path) -> (u16, String) {
    let mut cmd = Command::new("curl");
    cmd.args(["-s", "-m", "30", "-X", method, "-o"])
        .arg(out)
        .args(["-w", "%{http_code} %{content_type}"]);
    if let Some(b) = body {
        cmd.args(["--data-binary", b]);
    }
    let o = cmd.arg(url).output().unwrap();
    let meta = String::from_utf8(o.stdout).unwrap();
    let (status, ct) = meta.split_once(' ').unwrap_or((&meta, ""));
    (status.parse().unwrap_or(0), ct.to_string())
}

fn answer(api: &str, req: &Value, scratch: &Path) -> Value {
    let body_file = scratch.join("body");
    let (status, ct) = curl(
        &format!("{api}{}", req["path"].as_str().unwrap()),
        req["method"].as_str().unwrap(),
        req["body"].as_str(),
        &body_file,
    );
    json!({
        "method": req["method"],
        "path": req["path"],
        "status": status,
        "content_type": ct,
        "body": mask(&std::fs::read_to_string(&body_file).unwrap_or_default()),
    })
}

fn ready(api: &str, scratch: &Path) -> Option<Value> {
    let f = scratch.join("ready");
    let (status, _) = curl(&format!("{api}/ready"), "GET", None, &f);
    (status == 200)
        .then(|| serde_json::from_slice(&std::fs::read(&f).ok()?).ok())
        .flatten()
}

/// The node at `tip` on this era's chain, with nothing more to do: the era's newest row read back with
/// its hash, the cursor there, and the seal unmoved over two polls.
fn settle(api: &str, tip: u64, newest: Option<(String, String)>, scratch: &Path, log: &Path) {
    let deadline = Instant::now() + Duration::from_secs(180);
    let mut last = None;
    loop {
        let r = ready(api, scratch);
        let on_chain = newest.as_ref().is_none_or(|(key, hash)| {
            let f = scratch.join("newest");
            curl(&format!("{api}/entity/{key}"), "GET", None, &f).0 == 200
                && std::fs::read_to_string(&f).is_ok_and(|b| b.contains(hash.as_str()))
        });
        let at = on_chain
            && r.as_ref()
                .is_some_and(|r| r["last_block"].as_u64() == Some(tip));
        let now = r
            .as_ref()
            .map(|r| (r["last_block"].clone(), r["sealed_through"].clone()));
        if at && now == last {
            return;
        }
        last = if at { now } else { None };
        assert!(
            Instant::now() < deadline,
            "the node never settled at {tip}: {r:?}\n{}",
            std::fs::read_to_string(log).unwrap_or_default()
        );
        std::thread::sleep(Duration::from_millis(2_500));
    }
}

/// The node's answers to `requests` after each era, from the real binary over the recording.
fn node_answers(dir: &Path, nest: &Path) -> Vec<Vec<Value>> {
    let fixture = Recorded::from_path(&dir.join("tape.json.gz")).unwrap();
    let tips = fixture.tips();
    let tape: Value =
        serde_json::from_str(&gunzip(&std::fs::read(dir.join("tape.json.gz")).unwrap())).unwrap();
    let newest: Vec<Option<(String, String)>> = tape["eras"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| {
            let num =
                |v: &Value| u64::from_str_radix(v.as_str()?.trim_start_matches("0x"), 16).ok();
            e["logs"]
                .as_array()?
                .iter()
                .filter_map(|l| {
                    Some((
                        num(&l["blockNumber"])?,
                        num(&l["logIndex"])?,
                        l["blockHash"].as_str()?.to_string(),
                    ))
                })
                .max()
                .map(|(b, i, h)| (format!("{b:012}-{i:06}"), h))
        })
        .collect();
    let rpc = nuthatch_fixture_rpc::spawn(fixture, "127.0.0.1:0").expect("fixture RPC");
    let scratch = tempfile::tempdir().unwrap();
    let run = scratch.path().join("nest");
    copy_dir(nest, &run);
    let port = free_port();
    let api = format!("http://127.0.0.1:{port}");
    let log = scratch.path().join("dev.log");
    let _dev = Reaped(
        Command::new(env!("CARGO_BIN_EXE_nuthatch"))
            .args(["dev", "--dir"])
            .arg(&run)
            .args(["--rpc", &rpc.url()])
            .args(["--listen", &format!("127.0.0.1:{port}")])
            .args(["--poll-interval", "1s"])
            .stdout(std::fs::File::create(&log).unwrap())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let requests: Vec<Value> =
        serde_json::from_slice(&std::fs::read(dir.join("requests.json")).unwrap()).unwrap();
    let mut out = Vec::new();
    for (era, tip) in tips.iter().enumerate() {
        if era > 0 {
            curl(
                &format!("{}control/fork", rpc.url()),
                "POST",
                None,
                &scratch.path().join("fork"),
            );
        }
        settle(&api, *tip, newest[era].clone(), scratch.path(), &log);
        out.push(
            requests
                .iter()
                .map(|r| answer(&api, r, scratch.path()))
                .collect(),
        );
    }
    out
}

#[test]
fn the_node_answers_every_shape_as_pinned() {
    let pin = std::env::var_os("NUTHATCH_PIN_EMBED_SHAPES").is_some();
    let only = std::env::var("NUTHATCH_EMBED_SHAPE").ok();
    let mut index = Vec::new();
    for s in shapes() {
        index.push(s.name);
        if only.as_deref().is_some_and(|o| o != s.name) {
            continue;
        }
        let dir = PathBuf::from(DIR).join(s.name);
        let built = tempfile::tempdir().unwrap();
        let nest_rel = match &s.nest {
            Nest::Tree(rel) => format!("../../{rel}"),
            Nest::Written(write) => {
                write(&built.path().join("nest"));
                "nest".to_string()
            }
        };
        let tape = serde_json::to_string(&recording(&s)).unwrap();
        let requests = serde_json::to_string_pretty(&s.requests).unwrap() + "\n";
        let shape = serde_json::to_string_pretty(&json!({"nest": nest_rel})).unwrap() + "\n";
        if pin {
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("tape.json.gz"), gz(&tape)).unwrap();
            std::fs::write(dir.join("requests.json"), &requests).unwrap();
            std::fs::write(dir.join("shape.json"), &shape).unwrap();
            if matches!(s.nest, Nest::Written(_)) {
                copy_dir(&built.path().join("nest"), &dir.join("nest"));
            }
        }
        assert_eq!(
            gunzip(&std::fs::read(dir.join("tape.json.gz")).unwrap()),
            tape,
            "{}: the recording is not the chain this file builds",
            s.name
        );
        assert_eq!(
            std::fs::read_to_string(dir.join("requests.json")).unwrap(),
            requests
        );
        assert_eq!(
            std::fs::read_to_string(dir.join("shape.json")).unwrap(),
            shape
        );
        if matches!(s.nest, Nest::Written(_)) {
            assert_eq!(
                files(&dir.join("nest")),
                files(&built.path().join("nest")),
                "{}: the nest is not the one this file writes",
                s.name
            );
        }
        let got = Value::from(
            node_answers(&dir, &dir.join(&nest_rel))
                .into_iter()
                .map(Value::from)
                .collect::<Vec<_>>(),
        );
        let pinned = dir.join("expected.json");
        if pin {
            std::fs::write(&pinned, serde_json::to_string_pretty(&got).unwrap() + "\n").unwrap();
        }
        let expected: Value = serde_json::from_slice(&std::fs::read(&pinned).unwrap()).unwrap();
        for (era, (g, e)) in got
            .as_array()
            .unwrap()
            .iter()
            .zip(expected.as_array().unwrap())
            .enumerate()
        {
            for (g, e) in g.as_array().unwrap().iter().zip(e.as_array().unwrap()) {
                assert_eq!(g, e, "{} era {era}: the node's answer moved", s.name);
            }
        }
        assert_eq!(got, expected, "{}: the node's answers moved", s.name);
    }
    let listed = serde_json::to_string_pretty(&index).unwrap() + "\n";
    if pin && only.is_none() {
        std::fs::write(PathBuf::from(DIR).join("shapes.json"), &listed).unwrap();
    }
    if only.is_none() {
        assert_eq!(
            std::fs::read_to_string(PathBuf::from(DIR).join("shapes.json")).unwrap(),
            listed
        );
    }
}
