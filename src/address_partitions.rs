//! Block partitions (RFC-0063 §8): each Ethereum block's header and withdrawals, read once on our
//! boxes and published as immutable Parquet files, so a rotki-mode nest that opts in downloads them
//! instead of reading one body per block itself.
//!
//! A partition is believed only once the nest has re-derived it: every header hashes to the next
//! one's parent, the last is the block the nest's own RPC returns at that height, at or below
//! finality, and every withdrawal list hashes to its header's withdrawals root. The manifest a
//! publisher writes is for operators; nothing in it becomes coverage.

use alloy_consensus::{proofs, Header, Transaction as _, TxEnvelope};
use alloy_primitives::{keccak256, B256, U256};
use alloy_rlp::{Decodable, Encodable};
use alloy_rpc_types_eth::Withdrawal;
use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{Array, BinaryArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;

use crate::address_discovery::{static_reward, BlockFound, Rpc};
use crate::address_history::Row;
use crate::publish::Mirror;

/// Blocks per partition. Partitions start at multiples of it, so a block's partition is arithmetic.
pub const SPAN: u64 = 10_000;
const LAYOUT: &str = "address-history/v1";

/// The partition holding `block`, inclusive.
pub fn span_of(block: u64) -> (u64, u64) {
    let from = block - block % SPAN;
    (from, from + SPAN - 1)
}

pub fn partition_key(chain_id: u64, from: u64) -> String {
    format!("{LAYOUT}/{chain_id}/{from:010}.parquet")
}

pub fn manifest_key(chain_id: u64) -> String {
    format!("{LAYOUT}/{chain_id}/manifest.json")
}

/// One block as published: its header and, from Shanghai, its withdrawal list, each RLP-encoded as
/// the chain hashes them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Published {
    pub number: u64,
    pub header: Vec<u8>,
    pub withdrawals: Option<Vec<u8>>,
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("number", DataType::UInt64, false),
        Field::new("header", DataType::Binary, false),
        Field::new("withdrawals", DataType::Binary, true),
    ]))
}

/// The same blocks always encode to the same bytes, so two builds of one range publish one file.
pub fn encode(blocks: &[Published]) -> Result<Vec<u8>> {
    use parquet::arrow::ArrowWriter;
    use parquet::basic::{Compression, ZstdLevel};
    use parquet::file::properties::WriterProperties;
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(UInt64Array::from_iter_values(
                blocks.iter().map(|b| b.number),
            )),
            Arc::new(BinaryArray::from_iter_values(
                blocks.iter().map(|b| b.header.as_slice()),
            )),
            Arc::new(BinaryArray::from_iter(
                blocks.iter().map(|b| b.withdrawals.as_deref()),
            )),
        ],
    )?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_created_by("nuthatch address-history partition v1".into())
        .set_dictionary_enabled(false)
        .set_max_row_group_row_count(Some(SPAN as usize))
        .build();
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(buf)
}

pub fn decode(bytes: &[u8]) -> Result<Vec<Published>> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(bytes))
        .context("not a partition file")?
        .build()?;
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch?;
        let col = |name: &str| {
            batch
                .column_by_name(name)
                .ok_or_else(|| anyhow!("a partition without a `{name}` column"))
        };
        let numbers = col("number")?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| anyhow!("`number` is not u64"))?;
        let headers = col("header")?
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| anyhow!("`header` is not binary"))?;
        let withdrawals = col("withdrawals")?
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| anyhow!("`withdrawals` is not binary"))?;
        for i in 0..batch.num_rows() {
            out.push(Published {
                number: numbers.value(i),
                header: headers.value(i).to_vec(),
                withdrawals: (!withdrawals.is_null(i)).then(|| withdrawals.value(i).to_vec()),
            });
        }
    }
    Ok(out)
}

/// Whether a chain's `withdrawalsRoot` commits to beacon withdrawals. On the OP stack it is the
/// message passer's storage root from Isthmus on, and the block lists no withdrawals.
pub fn beacon_withdrawals(chain_id: u64) -> bool {
    !matches!(chain_id, 10 | 8453)
}

/// One block for publication, read from `rpc` and checked against itself: its fields must hash to
/// the hash the RPC gave, and its withdrawals to its withdrawals root.
pub async fn fetch(rpc: &impl Rpc, chain_id: u64, b: u64) -> Result<Published> {
    let v = rpc
        .call("eth_getBlockByNumber", json!([format!("0x{b:x}"), false]))
        .await
        .with_context(|| format!("block {b}"))?;
    if v.is_null() {
        bail!("the RPC has no block {b}");
    }
    let header: Header =
        serde_json::from_value(v.clone()).with_context(|| format!("block {b}'s header"))?;
    let claimed: B256 =
        serde_json::from_value(v["hash"].clone()).with_context(|| format!("block {b}'s hash"))?;
    if header.number != b {
        bail!(
            "asked for block {b}, the RPC answered block {}",
            header.number
        );
    }
    if header.hash_slow() != claimed {
        bail!("block {b}'s header fields do not hash to {claimed}");
    }
    let withdrawals = match header.withdrawals_root {
        Some(_) if !beacon_withdrawals(chain_id) => {
            if v["withdrawals"].as_array().is_some_and(|w| !w.is_empty()) {
                bail!("block {b} lists withdrawals, which chain {chain_id} never has");
            }
            None
        }
        Some(root) => {
            let ws: Vec<Withdrawal> = serde_json::from_value(v["withdrawals"].clone())
                .with_context(|| format!("block {b}'s withdrawals"))?;
            if proofs::calculate_withdrawals_root(&ws) != root {
                bail!("block {b}'s withdrawals do not hash to its withdrawals root");
            }
            let mut raw = Vec::new();
            alloy_rlp::encode_list(&ws, &mut raw);
            Some(raw)
        }
        None => None,
    };
    let mut raw = Vec::new();
    header.encode(&mut raw);
    Ok(Published {
        number: b,
        header: raw,
        withdrawals,
    })
}

/// [`fetch`], retried after a pause: a provider's brief refusal partway through a span should cost a
/// wait, not the span.
async fn fetch_patiently(rpc: &impl Rpc, chain_id: u64, b: u64) -> Result<Published> {
    let mut pause = std::time::Duration::from_secs(15);
    for _ in 0..3 {
        match fetch(rpc, chain_id, b).await {
            Ok(p) => return Ok(p),
            Err(e) => tracing::warn!("block {b}: {e:#}; trying again in {}s", pause.as_secs()),
        }
        tokio::time::sleep(pause).await;
        pause *= 2;
    }
    fetch(rpc, chain_id, b).await
}

/// A block whose header and withdrawals a partition carried and the nest has checked.
#[derive(Debug, Clone)]
pub struct VerifiedBlock {
    pub hash: B256,
    pub header: Header,
    pub withdrawals: Vec<Withdrawal>,
}

/// Check that `blocks` are exactly `[from, to]`, each the parent of the next, the last hashing to
/// `anchor`, and each withdrawal list the one its header commits to.
pub fn verify(
    blocks: &[Published],
    from: u64,
    to: u64,
    anchor: B256,
    chain_id: u64,
) -> Result<Vec<VerifiedBlock>> {
    let want = to - from + 1;
    if blocks.len() as u64 != want {
        bail!(
            "the partition for [{from}, {to}] holds {} blocks, not {want}",
            blocks.len()
        );
    }
    let mut out: Vec<VerifiedBlock> = Vec::with_capacity(blocks.len());
    for (i, p) in blocks.iter().enumerate() {
        let n = from + i as u64;
        let mut raw = p.header.as_slice();
        let header = Header::decode(&mut raw).with_context(|| format!("block {n}'s header"))?;
        if p.number != n || header.number != n || !raw.is_empty() {
            bail!("the partition's row {i} is not block {n}");
        }
        if let Some(parent) = out.last() {
            if header.parent_hash != parent.hash {
                bail!("block {n} does not follow block {}", n - 1);
            }
        }
        let withdrawals = match (header.withdrawals_root, &p.withdrawals) {
            (Some(root), Some(list)) => {
                let mut raw = list.as_slice();
                let ws = Vec::<Withdrawal>::decode(&mut raw)
                    .with_context(|| format!("block {n}'s withdrawals"))?;
                if !raw.is_empty() || proofs::calculate_withdrawals_root(&ws) != root {
                    bail!("block {n}'s withdrawals do not hash to its withdrawals root");
                }
                ws
            }
            (None, None) => Vec::new(),
            (Some(_), None) if !beacon_withdrawals(chain_id) => Vec::new(),
            (Some(_), None) => bail!("block {n} commits to withdrawals the partition lacks"),
            (None, Some(_)) => bail!("block {n} carries withdrawals its header does not commit to"),
        };
        out.push(VerifiedBlock {
            hash: keccak256(&p.header),
            header,
            withdrawals,
        });
    }
    let last = out
        .last()
        .expect("a partition holds at least one block")
        .hash;
    if last != anchor {
        bail!("the partition's block {to} is {last}, and the RPC's is {anchor}");
    }
    Ok(out)
}

/// What Etherscan's `getminedblocks` calls `blockReward`, from the block's own transactions,
/// receipts and ommers, each checked against the verified header before it is used.
pub async fn verified_reward(rpc: &impl Rpc, block: &VerifiedBlock) -> Result<U256> {
    let h = &block.header;
    let n = h.number;
    let hash = format!("{:#x}", block.hash);
    let body = rpc
        .call("eth_getBlockByHash", json!([hash, true]))
        .await
        .with_context(|| format!("block {n}'s transactions"))?;
    if body.is_null() {
        bail!("the RPC has no block {hash}");
    }
    let txs: Vec<alloy_rpc_types_eth::Transaction> =
        serde_json::from_value(body["transactions"].clone())
            .with_context(|| format!("block {n}'s transactions"))?;
    let txs: Vec<TxEnvelope> = txs.into_iter().map(|t| t.into_inner()).collect();
    if proofs::calculate_transaction_root(&txs) != h.transactions_root {
        bail!("block {n}'s transactions do not hash to its transactions root");
    }
    let receipts: Vec<alloy_rpc_types_eth::TransactionReceipt> = serde_json::from_value(
        rpc.call("eth_getBlockReceipts", json!([hash]))
            .await
            .with_context(|| format!("block {n}'s receipts"))?,
    )
    .with_context(|| format!("block {n}'s receipts"))?;
    let receipts: Vec<_> = receipts
        .into_iter()
        .map(|r| r.into_primitives_receipt().inner)
        .collect();
    if receipts.len() != txs.len() || proofs::calculate_receipt_root(&receipts) != h.receipts_root {
        bail!("block {n}'s receipts do not hash to its receipts root");
    }
    let uncles = body["uncles"].as_array().map_or(0, Vec::len);
    let mut ommers = Vec::with_capacity(uncles);
    for i in 0..uncles {
        let u = rpc
            .call(
                "eth_getUncleByBlockHashAndIndex",
                json!([hash, format!("0x{i:x}")]),
            )
            .await
            .with_context(|| format!("block {n}'s ommer {i}"))?;
        ommers.push(
            serde_json::from_value::<Header>(u)
                .with_context(|| format!("block {n}'s ommer {i}"))?,
        );
    }
    if proofs::calculate_ommers_root(&ommers) != h.ommers_hash {
        bail!("block {n}'s ommers do not hash to its ommers hash");
    }
    let base_fee = h.base_fee_per_gas.unwrap_or(0);
    let mut fees = U256::ZERO;
    let mut before = 0u64;
    for (tx, r) in txs.iter().zip(&receipts) {
        let cumulative = r.cumulative_gas_used();
        let used = cumulative
            .checked_sub(before)
            .ok_or_else(|| anyhow!("block {n}'s cumulative gas goes backwards"))?;
        before = cumulative;
        let tip = tx
            .effective_tip_per_gas(base_fee)
            .ok_or_else(|| anyhow!("block {n} includes a transaction priced under its base fee"))?;
        fees += U256::from(used) * U256::from(tip);
    }
    let fixed = static_reward(n);
    Ok(fixed + fees + fixed / U256::from(32) * U256::from(ommers.len()))
}

/// Where partitions come from, and the disposable local copies kept of them.
pub struct Source {
    mirror: Box<dyn Mirror>,
    chain_id: u64,
    cache: PathBuf,
    budget: u64,
}

impl Source {
    pub fn open(url: &str, chain_id: u64, cache: PathBuf, budget: u64) -> Result<Source> {
        Ok(Source {
            mirror: crate::publish::open_mirror(url)?,
            chain_id,
            cache,
            budget,
        })
    }

    fn cached(&self, from: u64) -> PathBuf {
        self.cache.join(format!("{from:010}.parquet"))
    }

    /// The partition starting at `from`, from the cache unless `fresh`, else from the mirror;
    /// `None` when the mirror has no such partition. A download lands under a temporary name and is
    /// renamed into place whole, so an interrupted one leaves nothing that reads as a partition.
    pub async fn fetch(&self, from: u64, fresh: bool) -> Result<Option<Vec<u8>>> {
        let path = self.cached(from);
        if !fresh {
            match std::fs::read(&path) {
                Ok(b) => {
                    self.evict()?;
                    return Ok(Some(b));
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
            }
        }
        let Some(bytes) = self.mirror.get(&partition_key(self.chain_id, from)).await? else {
            return Ok(None);
        };
        std::fs::create_dir_all(&self.cache)
            .with_context(|| format!("creating {}", self.cache.display()))?;
        let part = path.with_extension("part");
        std::fs::write(&part, &bytes).with_context(|| format!("writing {}", part.display()))?;
        std::fs::rename(&part, &path).with_context(|| format!("renaming {}", part.display()))?;
        self.evict()?;
        Ok(Some(bytes))
    }

    pub fn discard(&self, from: u64) {
        let _ = std::fs::remove_file(self.cached(from));
    }

    /// Delete the oldest partitions until the cache is within its budget.
    fn evict(&self) -> Result<()> {
        let mut files = Vec::new();
        for e in std::fs::read_dir(&self.cache)? {
            let e = e?;
            let m = e.metadata()?;
            if e.path().extension().is_some_and(|x| x == "parquet") {
                files.push((m.modified()?, m.len(), e.path()));
            }
        }
        let mut total: u64 = files.iter().map(|(_, len, _)| len).sum();
        files.sort();
        for (_, len, path) in files {
            if total <= self.budget {
                break;
            }
            std::fs::remove_file(&path)?;
            total -= len;
        }
        Ok(())
    }
}

/// What one verified partition holds for the addresses that lacked it, clipped to `clip`.
#[derive(Debug, Default)]
pub struct Ingested {
    pub found: BTreeMap<String, BlockFound>,
    pub timestamps: Vec<(u64, u64)>,
}

/// Download (or reuse) the partition starting at `from`, verify it against `rpc`, and read out the
/// withdrawals and produced blocks of `addresses` within `clip`. A partition that fails verification
/// from the cache is downloaded once more before it is given up on.
pub async fn ingest(
    rpc: &impl Rpc,
    source: &Source,
    from: u64,
    addresses: &[String],
    clip: (u64, u64),
) -> Result<Ingested> {
    let to = from + SPAN - 1;
    let anchor = rpc
        .call("eth_getBlockByNumber", json!([format!("0x{to:x}"), false]))
        .await
        .with_context(|| format!("block {to}, to anchor its partition"))?;
    let anchor: B256 = serde_json::from_value(anchor["hash"].clone())
        .with_context(|| format!("the RPC's hash of block {to}"))?;
    let mut verified = None;
    for fresh in [false, true] {
        let Some(bytes) = source.fetch(from, fresh).await? else {
            bail!("the mirror has no partition for blocks [{from}, {to}]");
        };
        match decode(&bytes).and_then(|blocks| verify(&blocks, from, to, anchor, source.chain_id)) {
            Ok(v) => {
                verified = Some(v);
                break;
            }
            Err(e) if !fresh => {
                tracing::warn!("address history: cached partition [{from}, {to}] refused, downloading it again: {e:#}");
                source.discard(from);
            }
            Err(e) => {
                source.discard(from);
                return Err(e.context(format!("the partition for blocks [{from}, {to}]")));
            }
        }
    }
    let verified = verified.expect("the loop returns or verifies");
    let watched: BTreeSet<String> = addresses.iter().map(|a| a.to_ascii_lowercase()).collect();
    let mut out = Ingested {
        found: watched
            .iter()
            .map(|a| (a.clone(), BlockFound::default()))
            .collect(),
        timestamps: Vec::new(),
    };
    for block in verified
        .iter()
        .filter(|b| clip.0 <= b.header.number && b.header.number <= clip.1)
    {
        let (n, ts) = (block.header.number, block.header.timestamp);
        out.timestamps.push((n, ts));
        for (i, w) in block.withdrawals.iter().enumerate() {
            let to = format!("{:#x}", w.address);
            if let Some(f) = out.found.get_mut(&to) {
                f.withdrawals.push(withdrawal_row(n, ts, i as u64, w, to));
            }
        }
        let miner = format!("{:#x}", block.header.beneficiary);
        if let Some(f) = out.found.get_mut(&miner) {
            let reward = verified_reward(rpc, block).await?;
            let mut r = Map::new();
            r.insert("blockNumber".into(), Value::String(n.to_string()));
            r.insert("timeStamp".into(), Value::String(ts.to_string()));
            r.insert("blockReward".into(), Value::String(reward.to_string()));
            f.mined.push(Row {
                block: n,
                tx_index: 0,
                position: 0,
                record: r,
            });
        }
    }
    Ok(out)
}

fn withdrawal_row(block: u64, ts: u64, position: u64, w: &Withdrawal, address: String) -> Row {
    let mut r = Map::new();
    let mut put = |k: &str, v: String| {
        r.insert(k.to_string(), Value::String(v));
    };
    put("withdrawalIndex", w.index.to_string());
    put("validatorIndex", w.validator_index.to_string());
    put("address", address);
    put("amount", w.amount.to_string());
    put("blockNumber", block.to_string());
    put("timestamp", ts.to_string());
    Row {
        block,
        tx_index: 0,
        position,
        record: r,
    }
}

/// The publisher's list of what it has written. For operators: a nest never reads it.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Manifest {
    pub version: u32,
    pub chain_id: u64,
    pub span: u64,
    pub partitions: BTreeMap<u64, ManifestEntry>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct ManifestEntry {
    pub to: u64,
    pub key: String,
    pub bytes: u64,
    pub sha256: String,
    pub last_hash: String,
    /// The store's ETag for the object as uploaded, when it gives one. Absent in manifests written
    /// before it was recorded, which are then checked on size alone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub e_tag: Option<String>,
}

/// The block `rpc` calls finalized.
pub async fn finalized(rpc: &impl Rpc) -> Result<u64> {
    let b = rpc
        .call("eth_getBlockByNumber", json!(["finalized", false]))
        .await?;
    crate::address_discovery::hex_u64(&b["number"]).context("the RPC's finalized block")
}

/// Whether the object at `key` is still the one `entry` describes: by size and, where the store
/// gives one, ETag, which a HEAD answers; or by downloading it and checking its sha256.
async fn intact(
    mirror: &dyn Mirror,
    key: &str,
    entry: &ManifestEntry,
    verify_existing: bool,
) -> Result<bool> {
    if verify_existing {
        return Ok(mirror
            .get(key)
            .await?
            .is_some_and(|bytes| crate::publish::sha256_hex(&bytes) == entry.sha256));
    }
    Ok(mirror.head(key).await?.is_some_and(|h| {
        h.size == entry.bytes
            && match (&entry.e_tag, &h.e_tag) {
                (Some(recorded), Some(now)) => recorded == now,
                _ => true,
            }
    }))
}

/// What a build wrote.
#[derive(Debug, Default)]
pub struct Built {
    pub written: Vec<(u64, u64)>,
    pub skipped: usize,
    /// Partitions the manifest listed whose object no longer matched, and were built again.
    pub rebuilt: usize,
    pub bytes: u64,
    pub blocks: u64,
}

/// Write every partition in `[from, to]` that `mirror` lacks, each verified as a nest would verify
/// it, then the manifest. `to` must be finalized. A partition already listed and present is left as
/// it is, so an interrupted build resumes where it stopped.
pub async fn build(
    rpc: &impl Rpc,
    target: &str,
    chain_id: u64,
    (from, to): (u64, u64),
    concurrency: usize,
    verify_existing: bool,
    mut progress: impl FnMut(u64, u64, u64),
) -> Result<Built> {
    let mirror = crate::publish::open_mirror(target)?;
    if from % SPAN != 0 || (to + 1) % SPAN != 0 || to < from {
        bail!(
            "partitions are whole spans of {SPAN} blocks: --from must be a multiple of {SPAN} and --to \
             one less than a multiple"
        );
    }
    let finalized = finalized(rpc).await?;
    if to > finalized {
        bail!("block {to} is past the RPC's finalized block {finalized}; partitions are immutable");
    }
    let mut manifest = match mirror.get(&manifest_key(chain_id)).await? {
        Some(raw) => {
            let m: Manifest = serde_json::from_slice(&raw).context("the mirror's manifest")?;
            if m.chain_id != chain_id || m.span != SPAN {
                bail!(
                    "the mirror's manifest is for chain {} in spans of {}",
                    m.chain_id,
                    m.span
                );
            }
            m
        }
        None => Manifest {
            version: 1,
            chain_id,
            span: SPAN,
            partitions: BTreeMap::new(),
        },
    };
    let mut built = Built::default();
    for start in (from..=to).step_by(SPAN as usize) {
        let end = start + SPAN - 1;
        let key = partition_key(chain_id, start);
        if let Some(entry) = manifest.partitions.get(&start) {
            if intact(mirror.as_ref(), &key, entry, verify_existing).await? {
                built.skipped += 1;
                continue;
            }
            tracing::warn!("partition {key} does not match its manifest entry; building it again");
            built.rebuilt += 1;
        }
        let blocks: Vec<Published> = futures::stream::iter(start..=end)
            .map(|b| fetch_patiently(rpc, chain_id, b))
            .buffered(concurrency)
            .try_collect()
            .await?;
        let last = keccak256(&blocks.last().expect("a span holds blocks").header);
        verify(&blocks, start, end, last, chain_id)
            .context("the blocks the RPC gave do not form a chain")?;
        let bytes = encode(&blocks)?;
        mirror.put(&key, &bytes).await?;
        let e_tag = mirror.head(&key).await?.and_then(|h| h.e_tag);
        manifest.partitions.insert(
            start,
            ManifestEntry {
                to: end,
                key,
                bytes: bytes.len() as u64,
                sha256: crate::publish::sha256_hex(&bytes),
                last_hash: format!("{last:#x}"),
                e_tag,
            },
        );
        mirror
            .put(
                &manifest_key(chain_id),
                &serde_json::to_vec_pretty(&manifest)?,
            )
            .await?;
        built.written.push((start, end));
        built.bytes += bytes.len() as u64;
        built.blocks += SPAN;
        progress(start, end, bytes.len() as u64);
    }
    Ok(built)
}

/// `nuthatch partitions`.
pub async fn run(args: &crate::cli::PartitionsArgs) -> Result<()> {
    let rpc = crate::address_discovery::Counted::new(crate::rpc::RpcClient::with_fallbacks(
        args.rpc.clone(),
        Vec::new(),
    )?);
    rpc.inner().verify_chain_ids(args.chain_id).await?;
    let started = std::time::Instant::now();
    if args.proposers {
        let beacon = crate::address_proposers::Beacon::new(
            args.beacon_url.clone(),
            args.beacon_cache.clone(),
        )?;
        let built = crate::address_proposers::build(
            &rpc,
            &args.out,
            args.chain_id,
            (args.from, args.to),
            &beacon,
            args.concurrency,
            |lo, end, bytes| {
                let secs = started.elapsed().as_secs_f64();
                println!("proposers [{lo}, {end}] {bytes} bytes, {secs:.0}s in");
            },
        )
        .await?;
        let secs = started.elapsed().as_secs_f64().max(0.001);
        println!(
            "{} catalogue files written, {} already there; {} bytes for {} blocks, {:.0} blocks/s",
            built.written.len(),
            built.skipped,
            built.bytes,
            built.blocks,
            built.blocks as f64 / secs
        );
        for (method, n) in rpc.calls() {
            println!("  {method}: {n}");
        }
        return Ok(());
    }
    let built = build(
        &rpc,
        &args.out,
        args.chain_id,
        (args.from, args.to),
        args.concurrency,
        args.verify_existing,
        |from, to, bytes| {
            let secs = started.elapsed().as_secs_f64();
            println!(
                "[{from}, {to}] {bytes} bytes, {} bytes a block, {secs:.0}s in",
                bytes / SPAN
            );
        },
    )
    .await?;
    let secs = started.elapsed().as_secs_f64().max(0.001);
    println!(
        "{} partitions written ({} of them rebuilt), {} already there; {} bytes for {} blocks ({} a block), {:.0} blocks/s",
        built.written.len(),
        built.rebuilt,
        built.skipped,
        built.bytes,
        built.blocks,
        built.bytes.checked_div(built.blocks).unwrap_or(0),
        built.blocks as f64 / secs
    );
    for (method, n) in rpc.calls() {
        println!("  {method}: {n}");
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    use alloy_primitives::Address;
    use std::sync::Mutex;

    pub(crate) type Answer = Box<dyn Fn(&str, &Value) -> Result<Value> + Send + Sync>;

    /// An RPC answering from a function, counting what it is asked.
    pub(crate) struct Fake {
        pub(crate) answer: Answer,
        pub(crate) asked: Mutex<BTreeMap<String, u64>>,
    }

    impl Fake {
        pub(crate) fn new(
            f: impl Fn(&str, &Value) -> Result<Value> + Send + Sync + 'static,
        ) -> Fake {
            Fake {
                answer: Box::new(f),
                asked: Mutex::new(BTreeMap::new()),
            }
        }

        pub(crate) fn count(&self, method: &str) -> u64 {
            self.asked.lock().unwrap().get(method).copied().unwrap_or(0)
        }
    }

    impl Rpc for Fake {
        async fn call(&self, method: &str, params: Value) -> Result<Value> {
            *self.asked.lock().unwrap().entry(method.into()).or_default() += 1;
            (self.answer)(method, &params)
        }
    }

    pub(crate) const PAID: Address = Address::repeat_byte(0xa1);
    pub(crate) const MINER: Address = Address::repeat_byte(0xb2);

    /// Blocks `[from, to]` of a made-up chain: every block from Shanghai's shape on, each block with
    /// three withdrawals, the middle one to [`PAID`], and every hundredth block mined by [`MINER`].
    pub(crate) fn synthetic(from: u64, to: u64) -> Vec<(Header, Vec<Withdrawal>)> {
        let mut parent = keccak256(from.to_be_bytes());
        let mut out = Vec::new();
        for n in from..=to {
            let ws: Vec<Withdrawal> = (0..3)
                .map(|i| Withdrawal {
                    index: n * 3 + i,
                    validator_index: 1_000 + i,
                    address: if i == 1 {
                        PAID
                    } else {
                        Address::repeat_byte(0x33)
                    },
                    amount: 10 + n % 7,
                })
                .collect();
            let header = Header {
                parent_hash: parent,
                beneficiary: if n % 100 == 0 {
                    MINER
                } else {
                    Address::repeat_byte(0x44)
                },
                number: n,
                timestamp: 1_600_000_000 + 12 * n,
                base_fee_per_gas: Some(7),
                withdrawals_root: Some(proofs::calculate_withdrawals_root(&ws)),
                ..Header::default()
            };
            parent = header.hash_slow();
            out.push((header, ws));
        }
        out
    }

    pub(crate) fn published(chain: &[(Header, Vec<Withdrawal>)]) -> Vec<Published> {
        chain
            .iter()
            .map(|(h, ws)| {
                let mut header = Vec::new();
                h.encode(&mut header);
                let mut list = Vec::new();
                alloy_rlp::encode_list(ws, &mut list);
                Published {
                    number: h.number,
                    header,
                    withdrawals: h.withdrawals_root.map(|_| list),
                }
            })
            .collect()
    }

    /// An RPC for the made-up chain `[0, head]`, finalized throughout, with empty block bodies.
    pub(crate) fn chain_rpc(head: u64) -> Fake {
        let chain = synthetic(0, head);
        Fake::new(move |m, p| {
            let block = |n: u64| {
                let (h, ws) = &chain[n as usize];
                let mut v = serde_json::to_value(h).unwrap();
                v["hash"] = json!(h.hash_slow());
                v["withdrawals"] = json!(ws);
                v["transactions"] = json!([]);
                v["uncles"] = json!([]);
                v
            };
            Ok(match m {
                "eth_getBlockByNumber" if p[0] == "finalized" => block(head),
                "eth_getBlockByNumber" => match crate::address_discovery::hex_u64(&p[0]) {
                    Ok(n) if n <= head => block(n),
                    _ => Value::Null,
                },
                "eth_getBlockByHash" => {
                    let want: B256 = serde_json::from_value(p[0].clone())?;
                    chain
                        .iter()
                        .position(|(h, _)| h.hash_slow() == want)
                        .map_or(Value::Null, |n| block(n as u64))
                }
                "eth_getBlockReceipts" => json!([]),
                other => bail!("unexpected {other}"),
            })
        })
    }

    #[test]
    fn a_partition_encodes_to_the_same_bytes_every_time_and_decodes_back() {
        let blocks = published(&synthetic(0, 99));
        let a = encode(&blocks).unwrap();
        assert_eq!(a, encode(&blocks).unwrap());
        assert_eq!(decode(&a).unwrap(), blocks);
    }

    #[test]
    fn a_whole_partition_verifies_and_every_tampering_is_refused() {
        let chain = synthetic(100, 199);
        let blocks = published(&chain);
        let anchor = chain.last().unwrap().0.hash_slow();
        let v = verify(&blocks, 100, 199, anchor, 1).unwrap();
        assert_eq!(v.len(), 100);
        assert_eq!(v[5].withdrawals[1].address, PAID);

        let reencode = |h: &Header| {
            let mut raw = Vec::new();
            h.encode(&mut raw);
            raw
        };
        let list = |ws: &[Withdrawal]| {
            let mut raw = Vec::new();
            alloy_rlp::encode_list(ws, &mut raw);
            raw
        };
        type Spoil = Box<dyn Fn(&mut Vec<Published>)>;
        let w = |n: usize| chain[n].1.clone();
        let cases: Vec<(&str, Spoil)> = vec![
            ("withdrawals do not hash", {
                let ws = w(10);
                Box::new(move |b| b[10].withdrawals = Some(list(&ws[..2])))
            }),
            ("withdrawals do not hash", {
                let mut ws = w(10);
                ws[1].amount += 1;
                Box::new(move |b| b[10].withdrawals = Some(list(&ws)))
            }),
            ("withdrawals do not hash", {
                let mut ws = w(10);
                ws.swap(0, 2);
                Box::new(move |b| b[10].withdrawals = Some(list(&ws)))
            }),
            ("does not follow block 120", {
                let mut h = chain[20].0.clone();
                h.timestamp += 1;
                let raw = reencode(&h);
                Box::new(move |b| b[20].header = raw.clone())
            }),
            (
                "holds 99 blocks",
                Box::new(|b| {
                    b.pop();
                }),
            ),
            (
                "holds 99 blocks",
                Box::new(|b| {
                    b.remove(50);
                }),
            ),
            ("is not block 130", Box::new(|b| b.swap(30, 31))),
            ("does not follow block 139", {
                let mut h = chain[40].0.clone();
                h.parent_hash = B256::repeat_byte(9);
                let raw = reencode(&h);
                Box::new(move |b| b[40].header = raw.clone())
            }),
            (
                "commits to withdrawals the partition lacks",
                Box::new(|b| {
                    b[60].withdrawals = None;
                }),
            ),
            ("the RPC's is", {
                let mut h = chain[99].0.clone();
                h.gas_used = 1;
                let raw = reencode(&h);
                Box::new(move |b| b[99].header = raw.clone())
            }),
        ];
        for (want, spoil) in cases {
            let mut b = blocks.clone();
            spoil(&mut b);
            let err = verify(&b, 100, 199, anchor, 1).unwrap_err();
            assert!(format!("{err:#}").contains(want), "{want}: {err:#}");
        }
        let mut pre = chain[0].0.clone();
        pre.withdrawals_root = None;
        let one = vec![Published {
            number: 100,
            header: reencode(&pre),
            withdrawals: blocks[0].withdrawals.clone(),
        }];
        let err = verify(&one, 100, 100, pre.hash_slow(), 1).unwrap_err();
        assert!(format!("{err:#}").contains("does not commit to"), "{err:#}");
    }

    /// An OP-stack header's withdrawals root after Isthmus is a storage root over no withdrawals: the
    /// partition carries the header alone there, and nowhere else.
    #[tokio::test]
    async fn an_op_stack_withdrawals_root_is_not_a_withdrawals_commitment() {
        let header = Header {
            number: 5,
            timestamp: 1_750_000_000,
            base_fee_per_gas: Some(7),
            withdrawals_root: Some(B256::repeat_byte(0x5a)),
            ..Header::default()
        };
        let rpc_listing = |ws: Vec<Withdrawal>| {
            let h = header.clone();
            Fake::new(move |_, _| {
                let mut v = serde_json::to_value(&h).unwrap();
                v["hash"] = json!(h.hash_slow());
                v["withdrawals"] = json!(ws);
                Ok(v)
            })
        };
        let empty = rpc_listing(Vec::new());
        for chain in [10, 8453] {
            let p = fetch(&empty, chain, 5).await.unwrap();
            assert_eq!(p.withdrawals, None);
            let v = verify(std::slice::from_ref(&p), 5, 5, header.hash_slow(), chain).unwrap();
            assert!(v[0].withdrawals.is_empty());
            let err = verify(&[p], 5, 5, header.hash_slow(), 1).unwrap_err();
            assert!(format!("{err:#}").contains("lacks"), "{err:#}");
        }
        let err = fetch(&empty, 1, 5).await.unwrap_err();
        assert!(format!("{err:#}").contains("do not hash"), "{err:#}");
        let listed = rpc_listing(synthetic(0, 0)[0].1.clone());
        let err = fetch(&listed, 10, 5).await.unwrap_err();
        assert!(format!("{err:#}").contains("never has"), "{err:#}");
    }

    /// Two builds of one range publish identical files, and a build that finds them already there
    /// reads no block again.
    #[tokio::test]
    async fn a_build_is_deterministic_and_resumes() {
        let rpc = chain_rpc(2 * SPAN + 5);
        let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let target = |d: &tempfile::TempDir| d.path().to_str().unwrap().to_string();
        let built = build(
            &rpc,
            &target(&a),
            1,
            (0, 2 * SPAN - 1),
            4,
            false,
            |_, _, _| {},
        )
        .await
        .unwrap();
        assert_eq!(built.written, vec![(0, SPAN - 1), (SPAN, 2 * SPAN - 1)]);
        build(
            &rpc,
            &target(&b),
            1,
            (0, 2 * SPAN - 1),
            4,
            false,
            |_, _, _| {},
        )
        .await
        .unwrap();
        for from in [0, SPAN] {
            let key = partition_key(1, from);
            assert_eq!(
                std::fs::read(a.path().join(&key)).unwrap(),
                std::fs::read(b.path().join(&key)).unwrap(),
                "{key}"
            );
        }
        let reads = rpc.count("eth_getBlockByNumber");
        let again = build(
            &rpc,
            &target(&a),
            1,
            (0, 2 * SPAN - 1),
            4,
            false,
            |_, _, _| {},
        )
        .await
        .unwrap();
        assert_eq!((again.written.len(), again.skipped), (0, 2));
        assert_eq!(
            rpc.count("eth_getBlockByNumber"),
            reads + 1,
            "only the finality check"
        );

        let err = build(
            &rpc,
            &target(&a),
            1,
            (0, 3 * SPAN - 1),
            4,
            false,
            |_, _, _| {},
        )
        .await
        .unwrap_err();
        assert!(
            format!("{err:#}").contains("past the RPC's finalized"),
            "{err:#}"
        );
        let err = build(&rpc, &target(&a), 1, (5, SPAN - 1), 4, false, |_, _, _| {})
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("whole spans"), "{err:#}");
    }

    /// A resumed build checks each listed partition: a truncated one by size, an altered one by the
    /// store's ETag where it gives one, and any of them by sha256 under `verify_existing`. A
    /// partition that does not match is built again and its entry rewritten.
    #[tokio::test]
    async fn a_resumed_build_rebuilds_a_damaged_partition() {
        let rpc = chain_rpc(2 * SPAN + 5);
        let range = (0, 2 * SPAN - 1);
        let dir = tempfile::tempdir().unwrap();
        let fs = dir.path().to_str().unwrap().to_string();
        build(&rpc, &fs, 1, range, 4, false, |_, _, _| {})
            .await
            .unwrap();
        let path = |from: u64| dir.path().join(partition_key(1, from));
        let (first, second) = (
            std::fs::read(path(0)).unwrap(),
            std::fs::read(path(SPAN)).unwrap(),
        );

        std::fs::write(path(0), &first[..first.len() / 2]).unwrap();
        let mut altered = second.clone();
        altered[100] ^= 0xff;
        std::fs::write(path(SPAN), &altered).unwrap();
        let cheap = build(&rpc, &fs, 1, range, 4, false, |_, _, _| {})
            .await
            .unwrap();
        assert_eq!(
            (cheap.rebuilt, cheap.skipped),
            (1, 1),
            "the truncation shows in the size"
        );
        assert_eq!(std::fs::read(path(0)).unwrap(), first);
        assert_eq!(
            std::fs::read(path(SPAN)).unwrap(),
            altered,
            "a filesystem gives no ETag"
        );

        let thorough = build(&rpc, &fs, 1, range, 4, true, |_, _, _| {})
            .await
            .unwrap();
        assert_eq!((thorough.rebuilt, thorough.skipped), (1, 1));
        assert_eq!(std::fs::read(path(SPAN)).unwrap(), second);

        // A store that gives ETags shows a same-size alteration without a download.
        let store = "memory://partitions-resume";
        build(&rpc, store, 1, range, 4, false, |_, _, _| {})
            .await
            .unwrap();
        let mirror = crate::publish::open_mirror(store).unwrap();
        let key = partition_key(1, SPAN);
        let mut bytes = mirror.get(&key).await.unwrap().unwrap();
        bytes[100] ^= 0xff;
        mirror.put(&key, &bytes).await.unwrap();
        let etag = build(&rpc, store, 1, range, 4, false, |_, _, _| {})
            .await
            .unwrap();
        assert_eq!((etag.rebuilt, etag.skipped), (1, 1));
        assert_eq!(mirror.get(&key).await.unwrap().unwrap(), second);
    }

    /// A manifest written before ETags were recorded still reads, and is checked on size.
    #[test]
    fn a_manifest_without_etags_still_reads() {
        let old = r#"{"version": 1, "chain_id": 1, "span": 10000, "partitions": {"0": {"to": 9999,
            "key": "address-history/v1/1/0000000000.parquet", "bytes": 5, "sha256": "ab",
            "last_hash": "0x01"}}}"#;
        let m: Manifest = serde_json::from_str(old).unwrap();
        assert_eq!(m.partitions[&0].e_tag, None);
        assert!(!serde_json::to_string(&m).unwrap().contains("e_tag"));
    }

    fn source(mirror: &std::path::Path, cache: &std::path::Path, budget: u64) -> Source {
        Source::open(mirror.to_str().unwrap(), 1, cache.to_path_buf(), budget).unwrap()
    }

    #[tokio::test]
    async fn ingest_reads_the_watched_rows_within_its_clip() {
        let rpc = chain_rpc(SPAN + 5);
        let (m, c) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        build(
            &rpc,
            m.path().to_str().unwrap(),
            1,
            (0, SPAN - 1),
            4,
            false,
            |_, _, _| {},
        )
        .await
        .unwrap();
        let src = source(m.path(), c.path(), u64::MAX);
        let paid = format!("{PAID:#x}");
        let miner = format!("{MINER:#x}");
        let got = ingest(&rpc, &src, 0, &[paid.clone(), miner.clone()], (250, 449))
            .await
            .unwrap();
        assert_eq!(got.timestamps.len(), 200);
        assert_eq!(got.timestamps[0], (250, 1_600_000_000 + 12 * 250));
        let w = &got.found[&paid].withdrawals;
        assert_eq!(w.len(), 200);
        assert_eq!(
            w[0].record,
            json!({"withdrawalIndex": "751", "validatorIndex": "1001", "address": paid,
                   "amount": "15", "blockNumber": "250", "timestamp": "1600003000"})
            .as_object()
            .unwrap()
            .clone()
        );
        let mined: Vec<u64> = got.found[&miner].mined.iter().map(|r| r.block).collect();
        assert_eq!(mined, [300, 400]);
        assert_eq!(
            got.found[&miner].mined[0].record["blockReward"],
            "5000000000000000000"
        );
        assert_eq!(
            rpc.count("eth_getBlockByHash"),
            2,
            "a body only for each block the miner made"
        );
    }

    /// A reward is computed only from a body that hashes to the verified header: an extra
    /// transaction, receipt or ommer the RPC slips in is refused, not counted.
    #[tokio::test]
    async fn a_reward_is_refused_unless_the_body_matches_the_header() {
        let chain = synthetic(0, 0);
        let block = &verify(&published(&chain), 0, 0, chain[0].0.hash_slow(), 1).unwrap()[0];
        let honest = chain_rpc(0);
        assert_eq!(
            verified_reward(&honest, block).await.unwrap().to_string(),
            "5000000000000000000",
            "block 0: the frontier issuance and nothing else"
        );
        let tx = json!({"type": "0x0", "nonce": "0x0", "gasPrice": "0x9", "gas": "0x5208",
            "to": format!("{PAID:#x}"), "value": "0x0", "input": "0x", "v": "0x1b", "r": "0x1",
            "s": "0x1", "hash": format!("{:#x}", B256::repeat_byte(5)),
            "from": format!("{MINER:#x}"), "blockHash": format!("{:#x}", block.hash),
            "blockNumber": "0x0", "transactionIndex": "0x0"});
        let receipt = json!({"type": "0x0", "status": "0x1", "cumulativeGasUsed": "0x5208",
            "logs": [], "logsBloom": format!("0x{}", "00".repeat(256)),
            "transactionHash": format!("{:#x}", B256::repeat_byte(5)), "transactionIndex": "0x0",
            "blockHash": format!("{:#x}", block.hash), "blockNumber": "0x0", "gasUsed": "0x5208",
            "effectiveGasPrice": "0x9", "from": format!("{MINER:#x}"),
            "to": format!("{PAID:#x}"), "contractAddress": null});
        let uncle = serde_json::to_value(Header::default()).unwrap();
        type Patch = Box<dyn Fn(&str, Value) -> Value + Send + Sync>;
        let cases: Vec<(&str, Patch)> = vec![
            ("transactions do not hash", {
                let tx = tx.clone();
                Box::new(move |m, mut v| {
                    if m == "eth_getBlockByHash" {
                        v["transactions"] = json!([tx]);
                    }
                    v
                })
            }),
            ("receipts do not hash", {
                let receipt = receipt.clone();
                Box::new(move |m, v| {
                    if m == "eth_getBlockReceipts" {
                        json!([receipt])
                    } else {
                        v
                    }
                })
            }),
            ("ommers do not hash", {
                let uncle = uncle.clone();
                Box::new(move |m, mut v| match m {
                    "eth_getBlockByHash" => {
                        v["uncles"] = json!([format!("{:#x}", B256::repeat_byte(6))]);
                        v
                    }
                    "eth_getUncleByBlockHashAndIndex" => uncle.clone(),
                    _ => v,
                })
            }),
        ];
        for (want, patch) in cases {
            let inner = chain_rpc(0);
            let rpc = Fake::new(move |m, p| {
                let v = match m {
                    "eth_getUncleByBlockHashAndIndex" => Value::Null,
                    _ => (inner.answer)(m, p)?,
                };
                Ok(patch(m, v))
            });
            let err = verified_reward(&rpc, block).await.unwrap_err();
            assert!(format!("{err:#}").contains(want), "{want}: {err:#}");
        }
    }

    /// A partition the mirror lacks fails; a cached copy that no longer verifies, as an interrupted
    /// or damaged download would leave it, is fetched again; the cache keeps within its budget.
    #[tokio::test]
    async fn missing_damaged_and_evicted_partitions() {
        let rpc = chain_rpc(3 * SPAN + 5);
        let (m, c) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        build(
            &rpc,
            m.path().to_str().unwrap(),
            1,
            (0, 2 * SPAN - 1),
            4,
            false,
            |_, _, _| {},
        )
        .await
        .unwrap();
        let one = std::fs::metadata(m.path().join(partition_key(1, 0)))
            .unwrap()
            .len();
        let src = source(m.path(), c.path(), one + one / 2);
        let err = ingest(&rpc, &src, 2 * SPAN, &[], (2 * SPAN, 3 * SPAN - 1))
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("has no partition"), "{err:#}");

        ingest(&rpc, &src, 0, &[], (0, 9)).await.unwrap();
        let cached = c.path().join(format!("{:010}.parquet", 0));
        let whole = std::fs::read(&cached).unwrap();
        std::fs::write(&cached, &whole[..whole.len() / 2]).unwrap();
        std::fs::write(c.path().join("0000010000.part"), b"half a download").unwrap();
        ingest(&rpc, &src, 0, &[], (0, 9)).await.unwrap();
        assert_eq!(
            std::fs::read(&cached).unwrap(),
            whole,
            "fetched again whole"
        );

        ingest(&rpc, &src, SPAN, &[], (SPAN, SPAN + 9))
            .await
            .unwrap();
        assert!(
            !cached.exists(),
            "the older partition is evicted to stay within budget"
        );
        assert!(c.path().join(format!("{SPAN:010}.parquet")).exists());

        let corrupt = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(corrupt.path().join("address-history/v1/1")).unwrap();
        std::fs::write(corrupt.path().join(partition_key(1, 0)), &whole[..100]).unwrap();
        let fresh = tempfile::tempdir().unwrap();
        let bad = source(corrupt.path(), fresh.path(), u64::MAX);
        assert!(ingest(&rpc, &bad, 0, &[], (0, 9)).await.is_err());

        // A smaller budget on a later run is honoured on a cache hit, and a partition larger than
        // the whole budget is used without being kept.
        let tight = source(m.path(), c.path(), one / 2);
        let kept = || {
            std::fs::read_dir(c.path())
                .unwrap()
                .filter(|e| {
                    e.as_ref()
                        .unwrap()
                        .path()
                        .extension()
                        .is_some_and(|x| x == "parquet")
                })
                .count()
        };
        ingest(&rpc, &tight, SPAN, &[], (SPAN, SPAN + 9))
            .await
            .unwrap();
        assert_eq!(kept(), 0, "the hit itself is evicted");
        ingest(&rpc, &tight, 0, &[], (0, 9)).await.unwrap();
        assert_eq!(kept(), 0, "too big to keep");
    }

    fn live() -> Option<crate::rpc::RpcClient> {
        let url = std::env::var("NUTHATCH_RPC").ok()?;
        Some(crate::rpc::RpcClient::with_fallbacks(vec![url], Vec::new()).unwrap())
    }

    /// Mainnet blocks of every header shape, and the rewards Etherscan gives three of them.
    #[tokio::test]
    #[ignore = "needs NUTHATCH_RPC on Ethereum mainnet"]
    async fn live_headers_and_rewards_match_mainnet() {
        let rpc = live().expect("NUTHATCH_RPC");
        for b in [
            1_000_000, 12_000_000, 14_000_136, 17_100_000, 20_000_206, 23_000_000,
        ] {
            let p = fetch(&rpc, 1, b).await.unwrap();
            let next = fetch(&rpc, 1, b + 1).await.unwrap();
            let anchor = keccak256(&next.header);
            let v = verify(&[p, next], b, b + 1, anchor, 1).unwrap();
            assert_eq!(v[0].header.number, b);
        }
        for (b, want) in [
            (20_000_206u64, "4449486205961004"),
            (14_000_136, "2137894551482662487"),
            (15_342_722, "2149854197642085641"),
        ] {
            let p = fetch(&rpc, 1, b).await.unwrap();
            let anchor = keccak256(&p.header);
            let v = verify(&[p], b, b, anchor, 1).unwrap();
            assert_eq!(
                verified_reward(&rpc, &v[0]).await.unwrap().to_string(),
                want,
                "{b}"
            );
        }
    }
}
