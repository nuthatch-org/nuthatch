//! #1819 - a fee collection in the unobserved window before an epoch's first observed event is filed
//! one epoch early by the nest. `scripts/lodestar-parity.sh` excuses that shift only when it is exact.
//!
//! One HTTP server plays both the nest and the gateway for a whole sealed run. The fixture is built so
//! that every other comparison passes and every measured boundary holds, so the exit status turns on
//! the epoch value fields alone, or on the escrow collections the tests at the end add (#1114, #1983).

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Command;
use std::sync::OnceLock;

use serde_json::{json, Value};

/// The comparison binary `scripts/lodestar-parity.sh` runs, built once from `tools/lodestar-parity`.
pub(crate) fn compare_bin() -> PathBuf {
    static BIN: OnceLock<PathBuf> = OnceLock::new();
    BIN.get_or_init(|| {
        let target = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("lodestar-parity");
        let built = Command::new(env!("CARGO"))
            .args(["build", "--locked", "--quiet", "--manifest-path"])
            .arg(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tools/lodestar-parity/Cargo.toml"))
            .arg("--target-dir")
            .arg(&target)
            .status()
            .expect("run cargo");
        assert!(built.success(), "building tools/lodestar-parity failed");
        target.join("debug/lodestar-parity-compare")
    })
    .clone()
}

fn decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < b.len() => {
                if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                    out.push(v);
                    i += 2;
                } else {
                    out.push(b'%');
                }
            }
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Answers each request with the first route whose needle appears in the decoded request line or
/// body; anything unmatched is a 404.
fn serve(routes: Vec<(&'static str, String)>) -> String {
    use std::io::{BufRead, BufReader, Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let mut reader = BufReader::new(stream);
            let mut req = String::new();
            let mut len = 0usize;
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                    break;
                }
                if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    len = v.trim().parse().unwrap_or(0);
                }
                req.push_str(&decode(&line));
            }
            let mut body = vec![0u8; len];
            let _ = reader.read_exact(&mut body);
            req.push_str(&String::from_utf8_lossy(&body));
            let (status, answer) = routes
                .iter()
                .find(|(needle, _)| req.contains(needle))
                .map(|(_, b)| (200, b.clone()))
                .unwrap_or((404, format!("no route for {req}")));
            let mut stream = reader.into_inner();
            let _ = write!(
                stream,
                "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{answer}",
                answer.len()
            );
        }
    });
    format!("http://{addr}")
}

const FIELDS: [(&str, &str); 6] = [
    ("total_rewards", "totalRewards"),
    ("total_indexer_rewards", "totalIndexerRewards"),
    ("total_delegator_rewards", "totalDelegatorRewards"),
    ("signalled_tokens", "signalledTokens"),
    ("query_fees_collected", "queryFeesCollected"),
    ("curator_query_fees", "curatorQueryFees"),
];

/// Each epoch sits on one side of a measured boundary (signal and curator fees from 1, query fees
/// from 290, rewards from 1195); 1392 is the open epoch.
const EPOCHS: [u32; 13] = [
    1, 2, 289, 290, 291, 1194, 1195, 1301, 1302, 1380, 1390, 1391, 1392,
];

struct Fixture {
    /// (epoch, nest column) -> nest minus subgraph.
    deltas: BTreeMap<(u32, &'static str), i64>,
    /// The source of epoch 1391's start.
    source_1391: &'static str,
    /// Collector of the one collection both sides hold.
    matched_collector: &'static str,
    /// (tx hash, log_index, payer, collector) of collections only the nest holds.
    nest_only: Vec<(String, u32, &'static str, &'static str)>,
}

/// GraphTallyCollector on Arbitrum One, the one collector whose collections the subgraph records.
const GTC: &str = "0x8f69f5c07477ac46fbc491b1e6d91e2bb0111a9e";

impl Fixture {
    /// Every boundary above epoch 1 disagrees just below itself and nowhere above; signal and curator
    /// fees agree everywhere, which is what a boundary of 1 claims.
    fn new() -> Self {
        let mut deltas = BTreeMap::new();
        for f in [
            "total_rewards",
            "total_indexer_rewards",
            "total_delegator_rewards",
        ] {
            deltas.insert((1194, f), 1);
        }
        deltas.insert((289, "query_fees_collected"), 1);
        Fixture {
            deltas,
            source_1391: "observed",
            matched_collector: GTC,
            nest_only: Vec::new(),
        }
    }

    fn nest_only(
        mut self,
        tx: u8,
        log_index: u32,
        payer: &'static str,
        collector: &'static str,
    ) -> Self {
        self.nest_only.push((
            format!("0x{}", format!("{tx:02x}").repeat(32)),
            log_index,
            payer,
            collector,
        ));
        self
    }

    /// `net` and `curators` of epoch 1391's collections filed in 1390 instead.
    fn shift(mut self, net: i64, curators: i64) -> Self {
        self.deltas.insert((1390, "query_fees_collected"), net);
        self.deltas.insert((1391, "query_fees_collected"), -net);
        self.deltas.insert((1390, "curator_query_fees"), curators);
        self.deltas.insert((1391, "curator_query_fees"), -curators);
        self
    }

    fn delta(mut self, epoch: u32, col: &'static str, d: i64) -> Self {
        self.deltas.insert((epoch, col), d);
        self
    }

    fn run(&self) -> (i32, String) {
        let base = 1_000_000i64;
        let nest_epochs: Vec<Value> = EPOCHS
            .iter()
            .map(|&e| {
                let mut row = json!({"id": e.to_string(), "start_block": 0, "end_block": 0});
                for (col, _) in FIELDS {
                    let d = self.deltas.get(&(e, col)).copied().unwrap_or(0);
                    row[col] = json!((base + d).to_string());
                }
                row
            })
            .collect();
        let sg_epochs: Vec<Value> = EPOCHS
            .iter()
            .map(|&e| {
                let mut row = json!({"id": e.to_string()});
                for (_, field) in FIELDS {
                    row[field] = json!(base.to_string());
                }
                row
            })
            .collect();
        let rows = |v: Value| json!({"rows": v}).to_string();
        let data = |k: &str, v: Value| json!({"data": {k: v}}).to_string();
        let n = |n: usize| rows(json!([{"n": n}]));
        let tx_a = format!("0x{}", "aa".repeat(32));
        let tx_b = format!("0x{}", "bb".repeat(32));
        let mut collected = vec![
            json!({"tx_hash": tx_a, "log_index": 5, "payer": "0x01", "collector": self.matched_collector}),
        ];
        for (tx, li, payer, collector) in &self.nest_only {
            collected.push(
                json!({"tx_hash": tx, "log_index": li, "payer": payer, "collector": collector}),
            );
        }
        let srv = serve(vec![
            (
                "GET /ready",
                json!({"ready": true, "version": "4.3.1", "last_block": 1000, "sealed_through": 900})
                    .to_string(),
            ),
            ("SELECT count(*) AS n FROM lodestar_allocations", n(2)),
            ("SELECT count(*) AS n FROM lodestar_epochs", n(EPOCHS.len())),
            ("SELECT count(*) AS n FROM lodestar_disputes", n(1)),
            ("SELECT count(*) AS n FROM lodestar_escrow_transactions", n(2)),
            ("SELECT id FROM lodestar_disputes", rows(json!([{"id": "0xd1"}]))),
            ("SELECT * FROM lodestar_epochs", rows(json!(nest_epochs))),
            (
                "FROM epoch_boundaries",
                rows(json!([
                    {"epoch": "1390", "start_block": 500, "last_seen": 590, "boundary_source": "observed"},
                    {"epoch": "1391", "start_block": 600, "last_seen": 690, "boundary_source": self.source_1391},
                ])),
            ),
            // Only the exact window, (1390's last_seen, 1391's start) at the pin, has an answer.
            (
                "block_number > 590 AND block_number < 600 AND block_number <= 900",
                rows(json!([
                    {"block_number": 595, "net": "40", "curators": "4"},
                    {"block_number": 598, "net": "60", "curators": "6"},
                ])),
            ),
            (
                "SELECT type, count(*)",
                rows(json!([{"type": "Deposit", "n": 1}, {"type": "EscrowCollected", "n": collected.len()}])),
            ),
            (
                "AS n FROM escrow__escrow_collected",
                rows(json!([{"lo": 100, "hi": 100, "n": collected.len()}])),
            ),
            (
                "collector FROM escrow__escrow_collected",
                rows(json!(collected)),
            ),
            (
                "AS n FROM escrow__deposit",
                rows(json!([{"lo": 100, "hi": 100, "n": 1}])),
            ),
            (
                "collector FROM escrow__deposit",
                rows(
                    json!([{"tx_hash": tx_b, "log_index": 3, "payer": "0x01", "collector": "0x02"}]),
                ),
            ),
            (
                "_meta",
                data("_meta", json!({"block": {"number": 2000}})),
            ),
            (
                "allocations(",
                data("allocations", json!([{"id": "0x1"}, {"id": "0x2"}])),
            ),
            (
                "disputes(",
                data(
                    "disputes",
                    json!([{"id": "0xd1", "type": "Query", "isLegacy": false}]),
                ),
            ),
            ("epoches(", data("epoches", json!(sg_epochs))),
            (
                "paymentsEscrowTransactions(",
                data(
                    "paymentsEscrowTransactions",
                    json!([
                        {"id": format!("{tx_b}03000000"), "type": "deposit"},
                        {"id": format!("{tx_a}06000000"), "type": "redeem"},
                    ]),
                ),
            ),
        ]);
        let o = Command::new("bash")
            .arg(
                std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("scripts/lodestar-parity.sh"),
            )
            .env("NEST_URL", &srv)
            .env("GRAPH_GATEWAY", &srv)
            .env("GRAPH_API_KEY", "k")
            .env("PARITY_COMPARE", compare_bin())
            .env_remove("PINNED_BLOCK")
            .env_remove("PARITY_MODE")
            .env_remove("EPOCH_PARITY_FROM")
            .output()
            .expect("run lodestar-parity.sh");
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&o.stdout),
            String::from_utf8_lossy(&o.stderr)
        );
        (o.status.code().unwrap_or(-1), text)
    }
}

/// The fixture itself, with nothing shifted, must reach the end of the run, clean.
#[test]
fn the_fixture_without_a_shift_is_clean() {
    let (code, text) = Fixture::new().run();
    assert_eq!(code, 0, "{text}");
    assert!(text.contains("parity CLEAN at block 900"), "{text}");
    assert!(
        text.contains("query_fees_collected 9/9 epochs agree from 290 OK"),
        "{text}"
    );
    assert!(
        text.contains("signalled_tokens 12/12 epochs agree from 1 OK"),
        "{text}"
    );
    assert!(!text.contains("boundary shift"), "{text}");
}

/// 60 net and 6 to curators of epoch 1391 were collected from block 598, inside the window between
/// 1390's last observation (590) and 1391's observed start (600).
#[test]
fn a_shifted_pair_is_a_known_difference_naming_the_epochs() {
    let (code, text) = Fixture::new().shift(60, 6).run();
    assert_eq!(code, 2, "{text}");
    assert!(
        text.contains("query_fees_collected 7/9 epochs agree from 290 KNOWN-DIFF (#1819)"),
        "{text}"
    );
    assert!(
        text.contains(
            "epochs 1390-1391 are a boundary shift netting to 0: 60 of epoch 1391 filed in 1390, collections from block 598"
        ),
        "{text}"
    );
    assert!(
        text.contains("curator_query_fees@1390-1391"),
        "the summary does not name the epochs: {text}"
    );
    assert!(!text.contains("BOUNDARY"), "{text}");
}

#[test]
fn a_shifted_pair_plus_a_real_discrepancy_fails() {
    let (code, text) = Fixture::new()
        .shift(60, 6)
        .delta(1380, "query_fees_collected", 5)
        .run();
    assert_eq!(code, 1, "{text}");
    assert!(
        text.contains("query_fees_collected 6/9 epochs agree from 290 DIFF"),
        "{text}"
    );
    assert!(text.contains("epoch 1380 nest=1000005"), "{text}");
    assert!(
        text.contains("epochs 1390-1391 are a boundary shift"),
        "{text}"
    );
}

/// Netting to zero is not enough: 50 is no tail of the window's collections (60 or 100).
#[test]
fn a_pair_that_nets_out_but_is_not_in_the_window_fails() {
    let (code, text) = Fixture::new().shift(50, 6).run();
    assert_eq!(code, 1, "{text}");
    assert!(
        text.contains("query_fees_collected 7/9 epochs agree from 290 DIFF"),
        "{text}"
    );
    assert!(!text.contains("boundary shift"), "{text}");
}

/// Each boundary traces, but 1390 is 60 high and 1391 only 59 low: one wei is nobody's.
#[test]
fn a_pair_that_traces_but_does_not_net_to_zero_fails() {
    let (code, text) = Fixture::new()
        .shift(60, 6)
        .delta(1391, "query_fees_collected", -59)
        .run();
    assert_eq!(code, 1, "{text}");
    assert!(
        text.contains("query_fees_collected 7/9 epochs agree from 290 DIFF"),
        "{text}"
    );
}

/// 100 net is the tail from block 595 and 6 to curators the tail from 598: no one block moved both.
#[test]
fn the_two_fee_fields_must_agree_on_where_the_boundary_fell() {
    let (code, text) = Fixture::new().shift(100, 6).run();
    assert_eq!(code, 1, "{text}");
    assert!(!text.contains("boundary shift"), "{text}");
}

/// The nest files a window's collections early, never late.
#[test]
fn a_shift_into_the_later_epoch_fails() {
    let (code, text) = Fixture::new().shift(-60, -6).run();
    assert_eq!(code, 1, "{text}");
    assert!(!text.contains("boundary shift"), "{text}");
}

/// An exact start leaves no window, so nothing can have been filed early across it.
#[test]
fn no_shift_is_excused_across_an_exact_boundary() {
    let mut f = Fixture::new().shift(60, 6);
    f.source_1391 = "l1-exact";
    let (code, text) = f.run();
    assert_eq!(code, 1, "{text}");
    assert!(!text.contains("boundary shift"), "{text}");
}

/// Fees that also agree at 289 make 290 too high: the constant would exclude comparable data.
#[test]
fn a_fee_boundary_set_too_high_fails_the_self_check() {
    let (code, text) = Fixture::new().delta(289, "query_fees_collected", 0).run();
    assert_eq!(code, 1, "{text}");
    assert!(
        text.contains(
            "BOUNDARY query_fees_collected: the lowest epoch at which the property holds is 1, not the configured 290 - the constant is too high"
        ),
        "{text}"
    );
}

/// Curator fees that disagree at epoch 1 make 1 too low: it claims more than it can show.
#[test]
fn a_fee_boundary_set_too_low_fails_the_self_check() {
    let (code, text) = Fixture::new().delta(1, "curator_query_fees", 1).run();
    assert_eq!(code, 1, "{text}");
    assert!(
        text.contains(
            "BOUNDARY curator_query_fees: the lowest epoch at which the property holds is 2, not the configured 1 - the constant is too low"
        ),
        "{text}"
    );
}

/// Signal is a gate now that the epoch starts are exact: a pair that once read as boundary drift fails.
#[test]
fn a_signalled_tokens_adjacent_pair_fails() {
    let (code, text) = Fixture::new()
        .delta(1, "signalled_tokens", 7)
        .delta(2, "signalled_tokens", -7)
        .run();
    assert_eq!(code, 1, "{text}");
    assert!(
        text.contains("signalled_tokens 10/12 epochs agree from 1 DIFF"),
        "{text}"
    );
    assert!(text.contains("epoch 1 nest=1000007"), "{text}");
}

const SELF_COLLECTOR: &str = "0xd4b1775640d9c4ba2ce5bf2b3201b35c4031aa47";
const EOA_PAYER: &str = "0xdde4cffd3d9052a9cb618fc05a1cd02be1f2f467";
const EOA_COLLECTOR: &str = "0x709ec1920bec7702ab3af63dcbf93e9d1f299e09";

/// #1114: an EOA that collects from its own escrow is nest-only, and the subgraph is the one that
/// skips it.
#[test]
fn a_self_collection_is_a_known_difference() {
    let (code, text) = Fixture::new()
        .nest_only(0xc1, 11, SELF_COLLECTOR, SELF_COLLECTOR)
        .run();
    assert_eq!(code, 2, "{text}");
    assert!(
        text.contains("1 nest-only rows were not collected by GraphTallyCollector, which the subgraph does not record KNOWN-DIFF (#1114, #1983)"),
        "{text}"
    );
    assert!(text.contains("escrow_non_tally_collections"), "{text}");
}

/// #1983: an EOA collecting from a different payer falls under the same rule as #1114.
#[test]
fn a_collection_by_another_eoa_is_a_known_difference() {
    let (code, text) = Fixture::new()
        .nest_only(0xc1, 11, SELF_COLLECTOR, SELF_COLLECTOR)
        .nest_only(0xc2, 51, EOA_PAYER, EOA_COLLECTOR)
        .run();
    assert_eq!(code, 2, "{text}");
    assert!(
        text.contains("2 nest-only rows were not collected by GraphTallyCollector"),
        "{text}"
    );
    assert!(
        text.contains(&format!("collector={EOA_COLLECTOR}")),
        "{text}"
    );
}

/// A GraphTallyCollector collection the subgraph lacks is what parity exists to catch, even beside
/// rows the rule explains.
#[test]
fn a_nest_only_tally_collection_fails() {
    let (code, text) = Fixture::new()
        .nest_only(0xc2, 51, EOA_PAYER, EOA_COLLECTOR)
        .nest_only(0xc3, 7, EOA_PAYER, GTC)
        .run();
    assert_eq!(code, 1, "{text}");
    assert!(
        text.contains("1 nest-only rows were collected by GraphTallyCollector and are unexplained"),
        "{text}"
    );
}

/// The rule rests on the address: a matched redeem from any other collector means it is wrong.
#[test]
fn a_matched_collection_by_another_collector_fails() {
    let mut f = Fixture::new();
    f.matched_collector = EOA_COLLECTOR;
    let (code, text) = f.run();
    assert_eq!(code, 1, "{text}");
    assert!(
        text.contains("1 matched collections were not collected by GraphTallyCollector"),
        "{text}"
    );
}
