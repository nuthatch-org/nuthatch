//! The chain check (#3096): `nuthatch audit chain`, opt-in and run by hand, never by `dev`.
//!
//! The ingest path believes the logs `eth_getLogs` serves, and a history source (`--history-source
//! xatu`) believes its dataset. This re-reads each sealed block in a range from an RPC as the block
//! itself: its header must hash to the block's hash and follow the block before it, its
//! transactions and receipts must hash to the header's roots, and its receipts' logs must make the
//! header's logs bloom. The nest's sealed rows for the block must then be exactly the receipts' logs
//! as the nest's registry decodes them, from that block. So whatever wrote a sealed row, an RPC or
//! a dataset, a row that differs from the chain is named by block, log and field.
//!
//! The check proves the rows are what the block the endpoint calls canonical commits to; that the
//! endpoint's chain is the canonical one is a matter of which endpoint is asked, so `--rpc` takes
//! one the operator trusts, such as their own node. A difference is reported, never repaired:
//! sealed segments are immutable. It costs two requests per block: the block with its
//! transactions, and its receipts.

use crate::address_discovery::Rpc;
use crate::address_partitions::{checked_body, served_header, Body, Broken};
use crate::registry::{DecodedRow, TableSchema};
use crate::sealed_audit::{canonical, Auditor};
use alloy_primitives::{keccak256, Bloom, B256};
use anyhow::{Context, Result};
use futures::StreamExt;
use serde::Serialize;
use serde_json::json;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Blocks whose sealed rows are read at once.
const CHUNK: u64 = 100;
/// Blocks asked of the RPC at once.
const IN_FLIGHT: usize = 8;
/// Problems a text report prints; `--json` carries them all.
const PRINTED: usize = 50;

/// One way a block, or the nest's rows for it, differs from what the chain commits to.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Problem {
    pub block: u64,
    /// The header field the difference breaks: `hash`, `parent_hash`, `transactions_root`,
    /// `receipts_root`, `logs_bloom`, `block_hash` (the rows came from another block) or `logs`.
    pub field: &'static str,
    pub detail: String,
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub from: u64,
    pub to: u64,
    pub blocks: u64,
    /// Sealed rows compared with the receipts' logs.
    pub rows: usize,
    pub problems: Vec<Problem>,
}

/// A block as the RPC served it: checked against its own commitments, or the commitment it broke.
enum Served {
    Checked {
        header: alloy_consensus::Header,
        hash: B256,
        body: Body,
    },
    /// The header does not hash to the hash the RPC gave, so nothing it commits to can be believed.
    BadHeader(Problem),
    /// The header holds, and the body does not.
    BadBody {
        header: alloy_consensus::Header,
        hash: B256,
        problem: Problem,
    },
}

async fn serve(rpc: &impl Rpc, b: u64) -> Result<Served> {
    let v = rpc
        .call("eth_getBlockByNumber", json!([format!("0x{b:x}"), true]))
        .await
        .with_context(|| format!("block {b}"))?;
    let broke = |e: anyhow::Error| -> Result<Problem> {
        let broken = e.downcast::<Broken>()?;
        Ok(Problem {
            block: b,
            field: broken.commitment.field(),
            detail: broken.to_string(),
        })
    };
    let (header, hash) = match served_header(&v, b) {
        Ok(h) => h,
        Err(e) => return Ok(Served::BadHeader(broke(e)?)),
    };
    Ok(match checked_body(rpc, &header, hash, &v).await {
        Ok(body) => Served::Checked { header, hash, body },
        Err(e) => Served::BadBody {
            header,
            hash,
            problem: broke(e)?,
        },
    })
}

/// A transaction's hash from its own bytes, which the transactions root commits to, rather than the
/// `hash` the RPC served beside them. Its network encoding wraps a typed transaction's EIP-2718
/// bytes in an RLP string; a legacy transaction's encoding is its EIP-2718 bytes.
fn tx_hash(tx: &alloy_consensus::TxEnvelope) -> B256 {
    use alloy_rlp::Encodable;
    let mut raw = Vec::new();
    tx.encode(&mut raw);
    let mut payload = raw.as_slice();
    match alloy_rlp::Header::decode(&mut payload) {
        Ok(h) if !h.list => keccak256(payload),
        _ => keccak256(&raw),
    }
}

/// The receipts' logs the nest's registry decodes, by log index, as [`canonical`] writes them.
fn decoded_logs(auditor: &Auditor, b: u64, hash: B256, body: &Body) -> BTreeMap<u64, String> {
    let mut out = BTreeMap::new();
    let mut index = 0u64;
    for (tx, receipt) in body.txs.iter().zip(&body.receipts) {
        for log in receipt.logs() {
            let log = crate::rpc::Log {
                address: format!("{:#x}", log.address),
                topics: log.topics().iter().map(|t| format!("{t:#x}")).collect(),
                data: format!("0x{}", hex::encode(&log.data.data)),
                block_number: b,
                block_hash: format!("{hash:#x}"),
                tx_hash: format!("{:#x}", tx_hash(tx)),
                log_index: index,
            };
            index += 1;
            // `decode` matches the nest's contract decoders only, as the indexer's own decode does.
            // Only rows of the audited tables, so a row kind the sealed side is not read for can
            // never show as a receipt log "no sealed row holds".
            if let Ok(Some(row)) = auditor.registry.decode(&log) {
                if auditor.schema.iter().any(|t| t.table == row.table) {
                    out.insert(row.log_index, row_text(&row));
                }
            }
        }
    }
    out
}

/// A row as both sides compare it: the block hash is checked on its own, and the timestamp comes
/// from the header rather than the log.
fn row_text(row: &DecodedRow) -> String {
    let mut row = row.clone();
    row.block_hash.clear();
    canonical(&row)
}

/// The audited tables' sealed rows in `from..=to`, by block.
fn sealed_rows(
    dir: &Path,
    schema: &[TableSchema],
    from: u64,
    to: u64,
) -> Result<BTreeMap<u64, Vec<DecodedRow>>> {
    let _lease = crate::seal::read_lease(dir);
    let manifest = crate::seal::load_manifest(dir)?;
    let mut out: BTreeMap<u64, Vec<DecodedRow>> = BTreeMap::new();
    let want = |b: u64| b >= from && b <= to;
    for table in schema {
        let Some(segments) = manifest.tables.get(&table.table) else {
            continue;
        };
        for segment in segments
            .iter()
            .filter(|s| s.from_block <= to && s.to_block >= from)
        {
            crate::seal::visit_segment_rows(dir, segment, table, Some(&want), &mut |row| {
                out.entry(row.block_number).or_default().push(row);
                Ok(())
            })?;
        }
    }
    Ok(out)
}

/// Compare block `b` as the chain has it with the nest's rows for it.
fn compare(
    auditor: &Auditor,
    b: u64,
    served: Served,
    stored: Vec<DecodedRow>,
    parent: &mut Option<B256>,
    problems: &mut Vec<Problem>,
) {
    let (header, hash, body) = match served {
        Served::BadHeader(p) => {
            *parent = None;
            problems.push(p);
            return;
        }
        Served::BadBody {
            header,
            hash,
            problem: p,
        } => {
            problems.push(p);
            (header, hash, None)
        }
        Served::Checked { header, hash, body } => (header, hash, Some(body)),
    };
    let mut problem = |field, detail| {
        problems.push(Problem {
            block: b,
            field,
            detail,
        })
    };
    if let Some(p) = parent.filter(|p| *p != header.parent_hash) {
        problem(
            "parent_hash",
            format!(
                "block {b}'s parent is {:#x}, and block {} is {p:#x}",
                header.parent_hash,
                b - 1
            ),
        );
    }
    *parent = Some(hash);
    let ours = format!("{hash:#x}");
    // Hex is compared without regard to case: a writer's casing is not a difference of block.
    let other = |r: &&DecodedRow| !r.block_hash.eq_ignore_ascii_case(&ours);
    let elsewhere = stored.iter().filter(other).count();
    if elsewhere > 0 {
        let theirs = stored.iter().find(other).unwrap();
        problem(
            "block_hash",
            format!(
                "{elsewhere} sealed row(s) came from block {}, and the chain's block {b} is {ours}: \
                 the endpoint does not serve that block at height {b}",
                theirs.block_hash
            ),
        );
    }
    // Rows from a body that does not hash to its header cannot be told apart from the truth.
    let Some(body) = body else { return };
    let mut bloom = Bloom::default();
    for log in body.receipts.iter().flat_map(|r| r.logs()) {
        bloom.accrue_log(log);
    }
    if bloom != header.logs_bloom {
        problem(
            "logs_bloom",
            format!("block {b}'s receipts' logs do not make its logs bloom"),
        );
    }
    let mut chain = decoded_logs(auditor, b, hash, &body);
    for row in &stored {
        let i = row.log_index;
        let text = row_text(row);
        match chain.remove(&i) {
            Some(want) if want == text => {}
            Some(want) => problem(
                "logs",
                format!("block {b} log {i}: sealed {text}, the receipt has {want}"),
            ),
            None => problem(
                "logs",
                format!("block {b} log {i}: sealed {text}, and no receipt has it"),
            ),
        }
    }
    for (i, want) in chain {
        problem(
            "logs",
            format!("block {b} log {i}: the receipt has {want}, and no sealed row holds it"),
        );
    }
}

/// Check every block in `from..=to` against `rpc`, in order.
pub async fn verify_range(
    dir: &Path,
    auditor: &Auditor,
    rpc: &impl Rpc,
    from: u64,
    to: u64,
) -> Result<Report> {
    let mut report = Report {
        from,
        to,
        blocks: 0,
        rows: 0,
        problems: Vec::new(),
    };
    let mut parent = None;
    let mut at = from;
    while at <= to {
        let end = at.saturating_add(CHUNK - 1).min(to);
        let read = {
            let (dir, schema) = (dir.to_path_buf(), auditor.schema.clone());
            move || sealed_rows(&dir, &schema, at, end)
        };
        let mut stored = tokio::task::spawn_blocking(read)
            .await
            .context("the sealed read panicked")??;
        let mut served = futures::stream::iter(at..=end)
            .map(|b| serve(rpc, b))
            .buffered(IN_FLIGHT);
        for b in at..=end {
            let block = served.next().await.expect("one answer per block")?;
            let rows = stored.remove(&b).unwrap_or_default();
            report.rows += rows.len();
            report.blocks += 1;
            compare(auditor, b, block, rows, &mut parent, &mut report.problems);
        }
        let Some(next) = end.checked_add(1) else {
            break;
        };
        at = next;
    }
    Ok(report)
}

/// `nuthatch audit chain`.
pub async fn run_cli(args: crate::cli::AuditChainArgs) -> Result<()> {
    let dir = PathBuf::from(&args.dir);
    let config = crate::config::Config::load(&dir)?;
    let auditor = Auditor::new(&dir, &config)?;
    let urls = match &args.rpc {
        Some(url) => vec![url.clone()],
        None => config.nest.rpc_urls.clone(),
    };
    anyhow::ensure!(
        !urls.is_empty(),
        "this nest names no RPC endpoint; pass --rpc <URL>"
    );
    let endpoints: Vec<String> = urls.iter().map(|u| crate::rpc::redact_url(u)).collect();
    let rpc = crate::rpc::RpcClient::new(urls)?;
    rpc.verify_chain_ids(config.nest.chain_id).await?;
    let watermark = crate::sealed_audit::watermark(&dir);
    if watermark.is_none() {
        // stderr: `--json` owns stdout.
        eprintln!("the hot store is in use or absent; checking up to the furthest sealed segment");
    }
    crate::sealed_audit::check_sealed(&dir, &auditor, watermark, args.from, args.to)?;
    let report = verify_range(&dir, &auditor, &rpc, args.from, args.to).await?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        for p in report.problems.iter().take(PRINTED) {
            println!("  {} {}: {}", p.block, p.field, p.detail);
        }
        if report.problems.len() > PRINTED {
            println!(
                "  ... and {} more (--json lists them all)",
                report.problems.len() - PRINTED
            );
        }
        println!(
            "chain check {}: blocks {}..={}, {} block(s), {} sealed row(s), {} JSON-RPC request(s) \
             to {}",
            if report.problems.is_empty() {
                "clean"
            } else {
                "FAILED"
            },
            report.from,
            report.to,
            report.blocks,
            report.rows,
            rpc.request_count(),
            endpoints.join(", ")
        );
    }
    anyhow::ensure!(
        report.problems.is_empty(),
        "{} difference(s) between the sealed rows and the chain; sealed data is unchanged",
        report.problems.len()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::address_partitions::tests::Fake;
    use alloy_consensus::{proofs, Header, Signed, TxEip1559, TxEnvelope};
    use alloy_primitives::{Address, Log, Signature, TxKind};
    use serde_json::Value;

    const USDC: &str = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";

    fn word(n: u64) -> String {
        format!("0x{n:064x}")
    }

    /// One block as an RPC serves it.
    #[derive(Clone)]
    struct Made {
        hash: B256,
        block: Value,
        receipts: Value,
        tx_hashes: Vec<B256>,
    }

    /// Block `n` after `parent`, one EIP-1559 transaction per log in `logs`, its header committing
    /// to all of it (to an empty logs bloom instead when `bloom` is false).
    fn make(n: u64, parent: B256, logs: &[Log], timestamp: u64, bloom: bool) -> Made {
        let (mut envs, mut txs, mut receipts, mut tx_hashes) = (vec![], vec![], vec![], vec![]);
        let mut all = Bloom::default();
        for (i, log) in logs.iter().enumerate() {
            let tx = TxEip1559 {
                chain_id: 1,
                nonce: n * 100 + i as u64,
                gas_limit: 50_000,
                max_fee_per_gas: 10,
                max_priority_fee_per_gas: 1,
                to: TxKind::Call(Address::repeat_byte(0xa0)),
                ..Default::default()
            };
            let signed = Signed::new_unhashed(tx, Signature::test_signature());
            let hash = *signed.hash();
            let env = TxEnvelope::Eip1559(signed);
            let from = Address::repeat_byte(0x11);
            txs.push(
                serde_json::to_value(alloy_rpc_types_eth::Transaction {
                    inner: alloy_consensus::transaction::Recovered::new_unchecked(
                        env.clone(),
                        from,
                    ),
                    block_hash: None,
                    block_number: Some(n),
                    transaction_index: Some(i as u64),
                    effective_gas_price: Some(8),
                    block_timestamp: None,
                })
                .unwrap(),
            );
            let mut one = Bloom::default();
            one.accrue_log(log);
            all.accrue_log(log);
            receipts.push(json!({"type": "0x2", "status": "0x1",
                "cumulativeGasUsed": format!("0x{:x}", 30_000 * (i + 1)),
                "logs": [{"address": format!("{:#x}", log.address), "topics": log.topics(),
                    "data": format!("0x{}", hex::encode(&log.data.data)),
                    "blockNumber": format!("0x{n:x}"), "transactionHash": hash,
                    "transactionIndex": format!("0x{i:x}"), "logIndex": format!("0x{i:x}"),
                    "removed": false}],
                "logsBloom": one, "transactionHash": hash,
                "transactionIndex": format!("0x{i:x}"), "blockNumber": format!("0x{n:x}"),
                "gasUsed": "0x7530", "effectiveGasPrice": "0x8", "from": from,
                "to": Address::repeat_byte(0xa0), "contractAddress": null}));
            envs.push(env);
            tx_hashes.push(hash);
        }
        let primitive: Vec<_> = receipts
            .iter()
            .map(|r| {
                serde_json::from_value::<alloy_rpc_types_eth::TransactionReceipt>(r.clone())
                    .unwrap()
                    .into_primitives_receipt()
                    .inner
            })
            .collect();
        let header = Header {
            parent_hash: parent,
            number: n,
            timestamp,
            base_fee_per_gas: Some(7),
            transactions_root: proofs::calculate_transaction_root(&envs),
            receipts_root: proofs::calculate_receipt_root(&primitive),
            logs_bloom: if bloom { all } else { Bloom::default() },
            ..Header::default()
        };
        let hash = header.hash_slow();
        let mut block = serde_json::to_value(&header).unwrap();
        block["hash"] = json!(hash);
        block["transactions"] = json!(txs);
        block["uncles"] = json!([]);
        Made {
            hash,
            block,
            receipts: json!(receipts),
            tx_hashes,
        }
    }

    fn transfer(auditor: &Auditor, amount: u64) -> Log {
        Log::new_unchecked(
            USDC.parse().unwrap(),
            vec![
                auditor.schema[0].topic0.parse().unwrap(),
                word(1).parse().unwrap(),
                word(2).parse().unwrap(),
            ],
            word(amount).parse::<B256>().unwrap().0.to_vec().into(),
        )
    }

    /// Blocks 1..=3, block 2 holding one transfer of 5.
    fn chain(auditor: &Auditor) -> Vec<Made> {
        chain_with_bloom(auditor, true)
    }

    /// [`chain`], block 2's header committing to an empty logs bloom unless `bloom`.
    fn chain_with_bloom(auditor: &Auditor, bloom: bool) -> Vec<Made> {
        let one = make(1, B256::repeat_byte(9), &[], 100, true);
        let two = make(2, one.hash, &[transfer(auditor, 5)], 112, bloom);
        let three = make(3, two.hash, &[], 124, true);
        vec![one, two, three]
    }

    fn rpc(blocks: Vec<Made>) -> Fake {
        Fake::new(move |m, p| {
            Ok(match m {
                "eth_getBlockByNumber" => {
                    let n = crate::address_discovery::hex_u64(&p[0])?;
                    blocks[n as usize - 1].block.clone()
                }
                "eth_getBlockReceipts" => {
                    let want: B256 = serde_json::from_value(p[0].clone())?;
                    let b = blocks.iter().find(|b| b.hash == want);
                    b.map_or(Value::Null, |b| b.receipts.clone())
                }
                other => anyhow::bail!("unexpected {other}"),
            })
        })
    }

    /// Seal the transfer of `amount` in block 2 of `chain` as the indexer would, or nothing.
    fn seal(d: &Path, auditor: &Auditor, chain: &[Made], amount: Option<u64>) {
        let rows: Vec<String> = amount
            .into_iter()
            .map(|amount| {
                let log = crate::rpc::Log {
                    address: USDC.into(),
                    topics: vec![auditor.schema[0].topic0.clone(), word(1), word(2)],
                    data: word(amount),
                    block_number: 2,
                    block_hash: format!("{:#x}", chain[1].hash),
                    tx_hash: format!("{:#x}", chain[1].tx_hashes[0]),
                    log_index: 0,
                };
                let row = auditor.registry.decode(&log).unwrap().unwrap();
                row.to_json().to_string()
            })
            .collect();
        crate::seal::test_set_table_floor(d, 0);
        crate::seal::seal_range(d, &rows, 1, 3).unwrap();
    }

    fn fields(r: &Report) -> Vec<(u64, &'static str)> {
        r.problems.iter().map(|p| (p.block, p.field)).collect()
    }

    #[tokio::test]
    async fn a_good_range_is_clean_at_two_requests_a_block() {
        let dir = tempfile::tempdir().unwrap();
        let auditor = crate::sealed_audit::tests::nest(dir.path(), "");
        let chain = chain(&auditor);
        seal(dir.path(), &auditor, &chain, Some(5));
        let rpc = rpc(chain);
        let r = verify_range(dir.path(), &auditor, &rpc, 1, 3)
            .await
            .unwrap();
        assert_eq!(r.problems, [], "{r:?}");
        assert_eq!((r.blocks, r.rows), (3, 1));
        assert_eq!(rpc.count("eth_getBlockByNumber"), 3);
        assert_eq!(rpc.count("eth_getBlockReceipts"), 3);
    }

    #[tokio::test]
    async fn a_tampered_or_missing_sealed_log_names_the_block_and_the_log() {
        for (amount, says) in [
            (Some(6), "the receipt has"),
            (None, "no sealed row holds it"),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let auditor = crate::sealed_audit::tests::nest(dir.path(), "");
            let chain = chain(&auditor);
            seal(dir.path(), &auditor, &chain, amount);
            let r = verify_range(dir.path(), &auditor, &rpc(chain), 1, 3)
                .await
                .unwrap();
            assert_eq!(fields(&r), [(2, "logs")], "{r:?}");
            assert!(r.problems[0].detail.contains("block 2 log 0"), "{r:?}");
            assert!(r.problems[0].detail.contains(says), "{r:?}");
        }
    }

    #[tokio::test]
    async fn a_served_block_that_breaks_its_own_commitments_is_named_by_field() {
        type Tamper = fn(&Auditor, &mut Vec<Made>);
        let cases: [(Tamper, &[(u64, &str)]); 3] = [
            (
                |_, c| c[1].block["timestamp"] = json!("0x71"),
                &[(2, "hash")],
            ),
            (
                |_, c| c[1].receipts[0]["logs"][0]["data"] = json!(word(6)),
                &[(2, "receipts_root")],
            ),
            (
                |_, c| c[2] = make(3, B256::repeat_byte(7), &[], 124, true),
                &[(3, "parent_hash")],
            ),
        ];
        for (tamper, want) in cases {
            let dir = tempfile::tempdir().unwrap();
            let auditor = crate::sealed_audit::tests::nest(dir.path(), "");
            let mut chain = chain(&auditor);
            seal(dir.path(), &auditor, &chain, Some(5));
            tamper(&auditor, &mut chain);
            let r = verify_range(dir.path(), &auditor, &rpc(chain), 1, 3)
                .await
                .unwrap();
            assert_eq!(fields(&r), want, "{r:?}");
        }
    }

    /// A header whose hash and roots hold, and whose logs bloom the receipts' logs do not make.
    #[tokio::test]
    async fn a_logs_bloom_the_receipts_do_not_make_is_named() {
        let dir = tempfile::tempdir().unwrap();
        let auditor = crate::sealed_audit::tests::nest(dir.path(), "");
        let chain = chain_with_bloom(&auditor, false);
        seal(dir.path(), &auditor, &chain, Some(5));
        let r = verify_range(dir.path(), &auditor, &rpc(chain), 1, 3)
            .await
            .unwrap();
        assert_eq!(fields(&r), [(2, "logs_bloom")], "{r:?}");
    }

    /// The nest sealed block 2 from a fork; the chain's block 2 is another block with no transfer.
    #[tokio::test]
    async fn a_row_from_a_reorged_block_is_reported_not_repaired() {
        let dir = tempfile::tempdir().unwrap();
        let auditor = crate::sealed_audit::tests::nest(dir.path(), "");
        let mut chain = chain(&auditor);
        seal(dir.path(), &auditor, &chain, Some(5));
        let catalogue = dir
            .path()
            .join(crate::seal::SEGMENTS_DIR)
            .join(crate::seal::MANIFEST_FILE);
        let manifest = std::fs::read(&catalogue).unwrap();
        chain[1] = make(2, chain[0].hash, &[], 113, true);
        chain[2] = make(3, chain[1].hash, &[], 124, true);
        let r = verify_range(dir.path(), &auditor, &rpc(chain), 1, 3)
            .await
            .unwrap();
        assert_eq!(fields(&r), [(2, "block_hash"), (2, "logs")], "{r:?}");
        assert!(r.problems[1].detail.contains("no receipt has it"), "{r:?}");
        assert_eq!(
            std::fs::read(&catalogue).unwrap(),
            manifest,
            "the sealed catalogue is untouched"
        );
    }

    #[test]
    fn a_transactions_hash_is_its_own_bytes_hash() {
        let tx = TxEip1559 {
            chain_id: 1,
            nonce: 3,
            ..Default::default()
        };
        let signed = Signed::new_unhashed(tx, Signature::test_signature());
        let want = *signed.hash();
        assert_eq!(tx_hash(&TxEnvelope::Eip1559(signed)), want);
        let legacy = Signed::new_unhashed(
            alloy_consensus::TxLegacy::default(),
            Signature::test_signature(),
        );
        let want = *legacy.hash();
        assert_eq!(tx_hash(&TxEnvelope::Legacy(legacy)), want);
    }
}
