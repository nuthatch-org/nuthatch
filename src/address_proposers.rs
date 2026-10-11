//! The proposer catalogue (RFC-0063 §8, plan C): which validator proposed each Ethereum block since
//! Dencun, proven from the execution chain itself.
//!
//! From Cancun every execution header carries `parent_beacon_block_root` (EIP-4788): the root of the
//! beacon block that produced its parent. So the beacon header of execution block N is authenticated
//! by hashing it (SSZ, the five-field unsigned `BeaconBlockHeader`) and comparing it with the root
//! execution block N+1 commits to, and its `proposer_index` is then the validator that proposed N.
//! The proof is relative to the execution anchor the nest's own RPC calls finalized, exactly as the
//! block partitions are; a locally validating execution-and-consensus node setup removes that trust.
//!
//! Every row of an interval is verified before any is filtered by validator, so a tampered row
//! naming another validator fails the interval instead of hiding a proposal.

use alloy_consensus::Header;
use alloy_primitives::B256;
use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{Array, BinaryArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::address_discovery::Rpc;
use crate::address_partitions::{VerifiedBlock, SPAN};

/// The first execution block of Cancun/Deneb on mainnet, and the beacon slot that produced it.
pub const DENEB_BLOCK: u64 = 19_426_587;
pub const DENEB_SLOT: u64 = 8_626_176;
/// Mainnet beacon genesis and slot length: a post-merge execution block's timestamp is its slot's.
pub const GENESIS_TIME: u64 = 1_606_824_023;
pub const SECONDS_PER_SLOT: u64 = 12;
/// The first block the catalogue covers: slot 12,777,208 (2025-10-11), about a year before it was
/// first built. Earlier proposals are provable the same way but not catalogued yet, so a range
/// before it is refused, never answered empty.
pub const CATALOGUE_FROM_BLOCK: u64 = 23_551_570;
pub const CATALOGUE_FROM_SLOT: u64 = 12_777_208;
/// The mainnet forks the proof is pinned to, by first epoch. Deneb, Electra and Fulu keep the
/// beacon header and EIP-4788 as they are; a later fork is refused by the execution header's shape.
pub const FORKS: [(&str, u64); 3] = [("deneb", 269_568), ("electra", 364_032), ("fulu", 411_392)];

const LAYOUT: &str = "address-history/v1";

pub fn catalogue_key(chain_id: u64, from: u64) -> String {
    format!("{LAYOUT}/{chain_id}/proposers/{from:010}.parquet")
}

pub fn catalogue_manifest_key(chain_id: u64) -> String {
    format!("{LAYOUT}/{chain_id}/proposers/manifest.json")
}

/// The blocks of the span starting at `from` the catalogue covers, or `None` before it starts.
pub fn provable(from: u64) -> Option<(u64, u64)> {
    let to = from + SPAN - 1;
    (to >= CATALOGUE_FROM_BLOCK).then(|| (from.max(CATALOGUE_FROM_BLOCK), to))
}

/// The pinned fork a slot belongs to.
pub fn fork_at(slot: u64) -> Option<&'static str> {
    let epoch = slot / 32;
    FORKS
        .iter()
        .rev()
        .find(|(_, first)| epoch >= *first)
        .map(|(name, _)| *name)
}

/// One row of the catalogue: an execution block and the beacon header that produced it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProposerRow {
    pub number: u64,
    pub hash: [u8; 32],
    pub slot: u64,
    pub proposer_index: u64,
    pub parent_root: [u8; 32],
    pub state_root: [u8; 32],
    pub body_root: [u8; 32],
}

fn sha256_pair(a: &[u8; 32], b: &[u8; 32]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(a);
    h.update(b);
    h.finalize().into()
}

fn uint64_leaf(v: u64) -> [u8; 32] {
    let mut leaf = [0u8; 32];
    leaf[..8].copy_from_slice(&v.to_le_bytes());
    leaf
}

/// SSZ `hash_tree_root` of the unsigned `BeaconBlockHeader {slot, proposer_index, parent_root,
/// state_root, body_root}`: five 32-byte leaves, uint64s little-endian and zero-padded, padded to
/// eight with zero leaves and merkleised with SHA-256.
pub fn header_root(row: &ProposerRow) -> [u8; 32] {
    let zero = [0u8; 32];
    let leaves = [
        uint64_leaf(row.slot),
        uint64_leaf(row.proposer_index),
        row.parent_root,
        row.state_root,
        row.body_root,
        zero,
        zero,
        zero,
    ];
    let l1: Vec<[u8; 32]> = leaves
        .chunks(2)
        .map(|p| sha256_pair(&p[0], &p[1]))
        .collect();
    let l2 = [sha256_pair(&l1[0], &l1[1]), sha256_pair(&l1[2], &l1[3])];
    sha256_pair(&l2[0], &l2[1])
}

fn schema() -> Arc<Schema> {
    let u = |n: &str| Field::new(n, DataType::UInt64, false);
    let b = |n: &str| Field::new(n, DataType::Binary, false);
    Arc::new(Schema::new(vec![
        u("number"),
        b("hash"),
        u("slot"),
        u("proposer_index"),
        b("parent_root"),
        b("state_root"),
        b("body_root"),
    ]))
}

/// The same rows always encode to the same bytes.
pub fn encode(rows: &[ProposerRow]) -> Result<Vec<u8>> {
    use parquet::arrow::ArrowWriter;
    use parquet::basic::{Compression, ZstdLevel};
    use parquet::file::properties::WriterProperties;
    let u =
        |f: fn(&ProposerRow) -> u64| Arc::new(UInt64Array::from_iter_values(rows.iter().map(f)));
    let b = |f: fn(&ProposerRow) -> &[u8; 32]| {
        Arc::new(BinaryArray::from_iter_values(
            rows.iter().map(|r| f(r).as_slice()),
        ))
    };
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            u(|r| r.number),
            b(|r| &r.hash),
            u(|r| r.slot),
            u(|r| r.proposer_index),
            b(|r| &r.parent_root),
            b(|r| &r.state_root),
            b(|r| &r.body_root),
        ],
    )?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_created_by("nuthatch proposer catalogue v1".into())
        .set_dictionary_enabled(false)
        .set_max_row_group_row_count(Some(SPAN as usize))
        .build();
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(buf)
}

pub fn decode(bytes: &[u8]) -> Result<Vec<ProposerRow>> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(bytes))
        .context("not a proposer catalogue file")?
        .build()?;
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch?;
        let col = |name: &str| {
            batch
                .column_by_name(name)
                .ok_or_else(|| anyhow!("a catalogue without a `{name}` column"))
        };
        let u = |name: &str| -> Result<UInt64Array> {
            col(name)?
                .as_any()
                .downcast_ref::<UInt64Array>()
                .cloned()
                .ok_or_else(|| anyhow!("`{name}` is not u64"))
        };
        let b = |name: &str| -> Result<BinaryArray> {
            col(name)?
                .as_any()
                .downcast_ref::<BinaryArray>()
                .cloned()
                .ok_or_else(|| anyhow!("`{name}` is not binary"))
        };
        let (number, hash, slot, proposer) =
            (u("number")?, b("hash")?, u("slot")?, u("proposer_index")?);
        let (parent, state, body) = (b("parent_root")?, b("state_root")?, b("body_root")?);
        let root = |a: &BinaryArray, i: usize, name: &str| -> Result<[u8; 32]> {
            a.value(i)
                .try_into()
                .map_err(|_| anyhow!("row {i}'s {name} is not 32 bytes"))
        };
        for i in 0..batch.num_rows() {
            out.push(ProposerRow {
                number: number.value(i),
                hash: root(&hash, i, "hash")?,
                slot: slot.value(i),
                proposer_index: proposer.value(i),
                parent_root: root(&parent, i, "parent_root")?,
                state_root: root(&state, i, "state_root")?,
                body_root: root(&body, i, "body_root")?,
            });
        }
    }
    Ok(out)
}

/// A block whose proposer the catalogue proved.
#[derive(Debug, Clone)]
pub struct Proven {
    pub block: VerifiedBlock,
    pub slot: u64,
    pub proposer_index: u64,
}

/// The execution header shape the proof is pinned to: Cancun's beacon root, and no field a later fork
/// added. A header from an unknown fork is refused, not guessed at.
fn pinned_shape(h: &Header) -> Result<B256> {
    let n = h.number;
    if h.block_access_list_hash.is_some() || h.slot_number.is_some() {
        bail!("block {n} is from a fork after Fulu, which the proposer proof is not pinned to");
    }
    h.parent_beacon_block_root
        .ok_or_else(|| anyhow!("block {n} carries no parent beacon block root"))
}

/// Check `rows` against `blocks` (the verified execution blocks `[lo, to]`) and `successor` (block
/// `to + 1`, authenticated and linked to block `to` by the caller): exactly one row per height, each
/// naming its block's hash, each hashing to the root the next execution header commits to, and each
/// slot the one its block's timestamp names. Every row is checked before any is used.
pub fn verify(
    rows: &[ProposerRow],
    blocks: &[VerifiedBlock],
    successor: &Header,
    (lo, to): (u64, u64),
) -> Result<Vec<Proven>> {
    if lo < DENEB_BLOCK {
        bail!("the proposer catalogue starts at Dencun, block {DENEB_BLOCK}");
    }
    let want = to - lo + 1;
    if rows.len() as u64 != want || blocks.len() as u64 != want {
        bail!(
            "the catalogue for [{lo}, {to}] holds {} rows for {} blocks, not {want}",
            rows.len(),
            blocks.len()
        );
    }
    if successor.number != to + 1 || successor.parent_hash != blocks[blocks.len() - 1].hash {
        bail!(
            "block {} is not the successor of block {to}",
            successor.number
        );
    }
    let mut out = Vec::with_capacity(rows.len());
    let mut last_slot = 0u64;
    for (i, (row, block)) in rows.iter().zip(blocks).enumerate() {
        let n = lo + i as u64;
        if row.number != n || block.header.number != n {
            bail!("the catalogue's row {i} is not block {n}");
        }
        if row.hash != block.hash.0 {
            bail!("the catalogue names block {n} at another hash");
        }
        let next = blocks.get(i + 1).map_or(successor, |b| &b.header);
        let committed = pinned_shape(next)?;
        if header_root(row) != committed.0 {
            bail!(
                "block {n}'s beacon header does not hash to the root block {} commits to",
                n + 1
            );
        }
        let slot_time = GENESIS_TIME + row.slot * SECONDS_PER_SLOT;
        if row.slot < DENEB_SLOT || slot_time != block.header.timestamp {
            bail!(
                "block {n}'s beacon slot {} is not its timestamp's {}",
                row.slot,
                block.header.timestamp
            );
        }
        if fork_at(row.slot).is_none() {
            bail!("block {n}'s slot {} is before the pinned forks", row.slot);
        }
        if i > 0 && row.slot <= last_slot {
            bail!("block {n}'s slot does not follow its parent's");
        }
        last_slot = row.slot;
        out.push(Proven {
            block: block.clone(),
            slot: row.slot,
            proposer_index: row.proposer_index,
        });
    }
    Ok(out)
}

/// The block `rpc` calls finalized, read by that tag, its header checked to hash to the hash given:
/// the anchor a locally built span must reach.
pub async fn finalized_header(rpc: &impl Rpc) -> Result<(Header, B256)> {
    let v = rpc
        .call("eth_getBlockByNumber", json!(["finalized", false]))
        .await
        .context("the finalized block")?;
    let n =
        crate::address_discovery::hex_u64(&v["number"]).context("the finalized block's number")?;
    checked_header(v, n)
}

/// Execution block `n`'s header from `rpc`, checked to hash to the hash the RPC gave.
pub async fn header_at(rpc: &impl Rpc, n: u64) -> Result<(Header, B256)> {
    let v = rpc
        .call("eth_getBlockByNumber", json!([format!("0x{n:x}"), false]))
        .await
        .with_context(|| format!("block {n}"))?;
    checked_header(v, n)
}

fn checked_header(v: Value, n: u64) -> Result<(Header, B256)> {
    if v.is_null() {
        bail!("the RPC has no block {n}");
    }
    let header: Header =
        serde_json::from_value(v.clone()).with_context(|| format!("block {n}'s header"))?;
    let claimed: B256 =
        serde_json::from_value(v["hash"].clone()).with_context(|| format!("block {n}'s hash"))?;
    if header.number != n || header.hash_slow() != claimed {
        bail!("block {n}'s header does not hash to {claimed}");
    }
    Ok((header, claimed))
}

/// The execution blocks `[lo, to]` read one by one from `rpc`, each linked to the next, for spans no
/// partition covers. Linked only to each other: the caller anchors the last to a finalized successor.
pub async fn blocks_from_rpc(
    rpc: &impl Rpc,
    (lo, to): (u64, u64),
    concurrency: usize,
) -> Result<Vec<VerifiedBlock>> {
    use futures::{StreamExt, TryStreamExt};
    let got: Vec<(Header, B256)> = futures::stream::iter(lo..=to)
        .map(|n| header_at(rpc, n))
        .buffered(concurrency.max(1))
        .try_collect()
        .await?;
    let mut out: Vec<VerifiedBlock> = Vec::with_capacity(got.len());
    for (header, hash) in got {
        if let Some(prev) = out.last() {
            if header.parent_hash != prev.hash {
                bail!(
                    "block {} does not follow block {}",
                    header.number,
                    prev.header.number
                );
            }
        }
        out.push(VerifiedBlock {
            hash,
            header,
            withdrawals: Vec::new(),
        });
    }
    Ok(out)
}

/// Where beacon headers come from when a catalogue is built: a beacon node's API, a local cache of
/// headers by slot, or both. Nothing from either is believed until [`verify`] has checked it.
pub struct Beacon {
    url: Option<String>,
    client: reqwest::Client,
    cache: Option<PathBuf>,
    loaded: std::sync::Mutex<BTreeMap<u64, Option<BTreeMap<u64, ProposerRow>>>>,
}

/// Slots per file of the local header cache, `slots-<first>.jsonl.gz`.
const CACHE_SLOTS: u64 = 10_000;

impl Beacon {
    pub fn new(url: Option<String>, cache: Option<PathBuf>) -> Result<Beacon> {
        if url.is_none() && cache.is_none() {
            bail!("a beacon source needs a URL or a header cache");
        }
        Ok(Beacon {
            url: url.map(|u| u.trim_end_matches('/').to_string()),
            client: reqwest::Client::builder()
                .user_agent(concat!("nuthatch/", env!("CARGO_PKG_VERSION")))
                .timeout(std::time::Duration::from_secs(30))
                .build()?,
            cache,
            loaded: std::sync::Mutex::new(BTreeMap::new()),
        })
    }

    fn parse(slot_hint: Option<u64>, m: &Value) -> Result<ProposerRow> {
        let num = |k: &str| -> Result<u64> {
            m.get(k)
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("a beacon header without {k}"))?
                .parse()
                .with_context(|| format!("a beacon header's {k}"))
        };
        let root = |k: &str| -> Result<[u8; 32]> {
            let s = m
                .get(k)
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("a beacon header without {k}"))?;
            let v: B256 = s
                .parse()
                .with_context(|| format!("a beacon header's {k}"))?;
            Ok(v.0)
        };
        let slot = num("slot")?;
        if slot_hint.is_some_and(|h| h != slot) {
            bail!(
                "asked for slot {}, the beacon answered slot {slot}",
                slot_hint.unwrap_or(0)
            );
        }
        Ok(ProposerRow {
            number: 0,
            hash: [0; 32],
            slot,
            proposer_index: num("proposer_index")?,
            parent_root: root("parent_root")?,
            state_root: root("state_root")?,
            body_root: root("body_root")?,
        })
    }

    /// The cached header for `slot`, if the cache file for its range exists and holds it.
    fn cached(&self, slot: u64) -> Result<Option<ProposerRow>> {
        let Some(dir) = &self.cache else {
            return Ok(None);
        };
        let first = slot - slot % CACHE_SLOTS;
        let mut loaded = self.loaded.lock().expect("beacon cache lock");
        if !loaded.contains_key(&first) {
            // One cached range at a time: a catalogue build walks slots in order.
            loaded.clear();
            let path = dir.join(format!("slots-{first}.jsonl.gz"));
            let rows = match std::fs::File::open(&path) {
                Ok(f) => {
                    use std::io::BufRead;
                    let mut rows = BTreeMap::new();
                    for line in std::io::BufReader::new(flate2::read::GzDecoder::new(f)).lines() {
                        let v: Value = serde_json::from_str(&line?)
                            .with_context(|| format!("a line of {}", path.display()))?;
                        if v.get("missed").and_then(Value::as_bool) == Some(true) {
                            continue;
                        }
                        let row = Beacon::parse(None, &v["header"])?;
                        rows.insert(row.slot, row);
                    }
                    Some(rows)
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
            };
            loaded.insert(first, rows);
        }
        Ok(loaded
            .get(&first)
            .and_then(|r| r.as_ref())
            .and_then(|r| r.get(&slot).cloned()))
    }

    /// The beacon header with root `root`, expected at `slot`: from the cache when it holds one with
    /// that root, else from the beacon API by root. `None` when neither has it.
    pub async fn header(&self, slot: u64, root: [u8; 32]) -> Result<Option<ProposerRow>> {
        if let Some(row) = self.cached(slot)? {
            if header_root(&row) == root {
                return Ok(Some(row));
            }
        }
        let Some(url) = &self.url else {
            return Ok(None);
        };
        let mut pause = std::time::Duration::from_secs(2);
        for _ in 0..6 {
            let resp = self
                .client
                .get(format!(
                    "{url}/eth/v1/beacon/headers/0x{}",
                    hex::encode(root)
                ))
                .send()
                .await
                .map_err(|e| e.without_url())
                .with_context(|| format!("the beacon header of slot {slot}"))?;
            match resp.status().as_u16() {
                200 => {
                    let body: Value = resp.json().await.map_err(|e| e.without_url())?;
                    return Beacon::parse(Some(slot), &body["data"]["header"]["message"]).map(Some);
                }
                404 => return Ok(None),
                429 | 500..=599 => {
                    tokio::time::sleep(pause).await;
                    pause *= 2;
                }
                s => bail!("the beacon answered {s} for the header of slot {slot}"),
            }
        }
        bail!("the beacon kept refusing the header of slot {slot}")
    }
}

/// The catalogue rows for `blocks` (`[lo, to]`) and `successor`, built from `beacon` and verified as
/// a consumer would verify them. A header neither source has fails the span: historical data that
/// is unavailable is never an empty proposal.
pub async fn build_rows(
    beacon: &Beacon,
    blocks: &[VerifiedBlock],
    successor: &Header,
    (lo, to): (u64, u64),
    concurrency: usize,
) -> Result<Vec<ProposerRow>> {
    use futures::{StreamExt, TryStreamExt};
    let wanted: Vec<(u64, [u8; 32], u64, u64)> = blocks
        .iter()
        .enumerate()
        .map(|(i, b)| {
            let next = blocks.get(i + 1).map_or(successor, |b| &b.header);
            let root = pinned_shape(next)?.0;
            let ts = b.header.timestamp;
            let slot = ts
                .checked_sub(GENESIS_TIME)
                .filter(|d| d % SECONDS_PER_SLOT == 0)
                .map(|d| d / SECONDS_PER_SLOT)
                .ok_or_else(|| anyhow!("block {}'s timestamp names no slot", b.header.number))?;
            Ok((b.header.number, root, slot, i as u64))
        })
        .collect::<Result<_>>()?;
    let rows: Vec<ProposerRow> = futures::stream::iter(wanted)
        .map(|(n, root, slot, i)| async move {
            let mut row = beacon.header(slot, root).await?.ok_or_else(|| {
                anyhow!("no beacon source has the header of block {n} (slot {slot})")
            })?;
            row.number = n;
            row.hash = blocks[i as usize].hash.0;
            Ok::<_, anyhow::Error>(row)
        })
        .buffered(concurrency.max(1))
        .try_collect()
        .await?;
    verify(&rows, blocks, successor, (lo, to))?;
    Ok(rows)
}

/// What a catalogue build wrote.
#[derive(Debug, Default)]
pub struct BuiltCatalogue {
    pub written: Vec<(u64, u64)>,
    pub skipped: usize,
    pub bytes: u64,
    pub blocks: u64,
}

/// The publisher's list of catalogue files. For operators: a nest never reads it.
#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Default)]
pub struct CatalogueManifest {
    pub version: u32,
    pub chain_id: u64,
    pub span: u64,
    pub files: BTreeMap<u64, crate::address_partitions::ManifestEntry>,
}

/// Write the proposer catalogue for every span of `[from, to]` from Dencun on that `target` lacks,
/// each built from the verified block partition already in `target` and `beacon`'s headers, and
/// verified as a nest verifies it. A span's last block is proven by its successor, which the RPC must
/// call finalized, so `to + 1` must be. A file already listed and present is left as it is, so an
/// interrupted build resumes where it stopped.
pub async fn build(
    rpc: &impl Rpc,
    target: &str,
    chain_id: u64,
    (from, to): (u64, u64),
    beacon: &Beacon,
    concurrency: usize,
    mut progress: impl FnMut(u64, u64, u64),
) -> Result<BuiltCatalogue> {
    use crate::address_partitions::{
        decode as decode_blocks, partition_key, verify as verify_blocks,
    };
    if chain_id != 1 {
        bail!("the proposer catalogue is for Ethereum mainnet");
    }
    if from % SPAN != 0 || (to + 1) % SPAN != 0 || to < from {
        bail!("catalogue files are whole spans of {SPAN} blocks: --from a multiple, --to one less");
    }
    let finalized = crate::address_partitions::finalized(rpc).await?;
    if to + 1 > finalized {
        bail!("block {} is past the RPC's finalized block {finalized}; the catalogue for block {to} needs its finalized successor", to + 1);
    }
    let mirror = crate::publish::open_mirror(target)?;
    let mut manifest = match mirror.get(&catalogue_manifest_key(chain_id)).await? {
        Some(raw) => serde_json::from_slice(&raw).context("the mirror's catalogue manifest")?,
        None => CatalogueManifest {
            version: 1,
            chain_id,
            span: SPAN,
            files: BTreeMap::new(),
        },
    };
    let mut built = BuiltCatalogue::default();
    for start in (from..=to).step_by(SPAN as usize) {
        let Some((lo, end)) = provable(start) else {
            continue;
        };
        let key = catalogue_key(chain_id, start);
        if let Some(entry) = manifest.files.get(&start) {
            if mirror
                .head(&key)
                .await?
                .is_some_and(|h| h.size == entry.bytes)
            {
                built.skipped += 1;
                continue;
            }
        }
        let raw = mirror
            .get(&partition_key(chain_id, start))
            .await?
            .ok_or_else(|| {
                anyhow!("the mirror has no block partition for [{start}, {end}] to build from")
            })?;
        let (successor, _) = header_at(rpc, end + 1).await?;
        let blocks = verify_blocks(
            &decode_blocks(&raw)?,
            start,
            end,
            successor.parent_hash,
            chain_id,
        )
        .with_context(|| format!("the block partition for [{start}, {end}]"))?;
        let blocks = &blocks[(lo - start) as usize..];
        let rows = build_rows(beacon, blocks, &successor, (lo, end), concurrency)
            .await
            .with_context(|| format!("the catalogue for [{lo}, {end}]"))?;
        let bytes = encode(&rows)?;
        mirror.put(&key, &bytes).await?;
        let e_tag = mirror.head(&key).await?.and_then(|h| h.e_tag);
        manifest.files.insert(
            start,
            crate::address_partitions::ManifestEntry {
                to: end,
                key,
                bytes: bytes.len() as u64,
                sha256: crate::publish::sha256_hex(&bytes),
                last_hash: format!("{:#x}", blocks[blocks.len() - 1].hash),
                e_tag,
            },
        );
        mirror
            .put(
                &catalogue_manifest_key(chain_id),
                &serde_json::to_vec_pretty(&manifest)?,
            )
            .await?;
        built.written.push((lo, end));
        built.bytes += bytes.len() as u64;
        built.blocks += end - lo + 1;
        progress(lo, end, bytes.len() as u64);
    }
    Ok(built)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    fn b(s: &str) -> [u8; 32] {
        s.parse::<B256>().unwrap().0
    }

    /// The first Deneb beacon block (slot 8,626,176), which produced execution block 19,426,587;
    /// execution block 19,426,588 commits to its root. Read from Lodestar's public beacon API and the
    /// chain on 2026-10-11.
    pub(crate) fn dencun_row() -> ProposerRow {
        ProposerRow {
            number: 19_426_587,
            hash: b("0xf8e2f40d98fe5862bc947c8c83d34799c50fb344d7445d020a8a946d891b62ee"),
            slot: 8_626_176,
            proposer_index: 1_278_077,
            parent_root: b("0xb35bb80bc5f4e3d8f19b62f6274add24dca334db242546c3024403027aaf6412"),
            state_root: b("0x5048e34dd7541e330f9c71fef2704651b940f46c5324225598ce1a32241238cc"),
            body_root: b("0x7341cd007d893fd223ebc90d7bde383986da170224ae75a26d4fb381b4088b82"),
        }
    }

    #[test]
    fn the_first_deneb_header_hashes_to_the_root_its_successor_commits_to() {
        assert_eq!(
            header_root(&dencun_row()),
            b("0xa471c7622a976313a61e01b01212dcea6acd71f351618734928dcabe4aba62fe")
        );
        assert_eq!(
            GENESIS_TIME + dencun_row().slot * SECONDS_PER_SLOT,
            0x65f1b057,
            "block 19,426,587's timestamp"
        );
    }

    #[test]
    fn any_changed_header_field_changes_the_root() {
        let base = header_root(&dencun_row());
        let mut fields: Vec<ProposerRow> = Vec::new();
        for f in 0..5 {
            let mut r = dencun_row();
            match f {
                0 => r.slot += 1,
                1 => r.proposer_index += 1,
                2 => r.parent_root[0] ^= 1,
                3 => r.state_root[31] ^= 1,
                _ => r.body_root[7] ^= 1,
            }
            fields.push(r);
        }
        for r in fields {
            assert_ne!(header_root(&r), base, "{r:?}");
        }
    }

    #[test]
    fn rows_round_trip_through_the_file_format() {
        let mut rows = vec![dencun_row()];
        let mut second = dencun_row();
        second.number += 1;
        second.slot += 1;
        rows.push(second);
        let bytes = encode(&rows).unwrap();
        assert_eq!(decode(&bytes).unwrap(), rows);
        assert_eq!(
            encode(&rows).unwrap(),
            bytes,
            "the same rows encode to the same bytes"
        );
    }

    #[test]
    fn forks_are_pinned_by_epoch() {
        assert_eq!(fork_at(DENEB_SLOT), Some("deneb"));
        assert_eq!(fork_at(DENEB_SLOT - 1), None);
        assert_eq!(fork_at(364_032 * 32), Some("electra"));
        assert_eq!(fork_at(411_392 * 32 + 5), Some("fulu"));
        assert_eq!(
            provable(23_550_000),
            Some((CATALOGUE_FROM_BLOCK, 23_559_999))
        );
        assert_eq!(provable(23_540_000), None);
        assert_eq!(
            GENESIS_TIME + CATALOGUE_FROM_SLOT * SECONDS_PER_SLOT,
            0x68e9c3f7,
            "block 23,551,570's timestamp"
        );
    }

    /// `n` linked execution blocks from Dencun on, every other slot missed, with the beacon headers
    /// that produced them, and the successor of the last. Block i is proposed by validator 100 + i.
    pub(crate) fn chain(n: usize) -> (Vec<VerifiedBlock>, Vec<ProposerRow>, Header) {
        chain_from(DENEB_BLOCK, DENEB_SLOT, n)
    }

    /// [`chain`] from execution block `first`, produced at slot `slot`.
    pub(crate) fn chain_from(
        first: u64,
        slot0: u64,
        n: usize,
    ) -> (Vec<VerifiedBlock>, Vec<ProposerRow>, Header) {
        let mut blocks: Vec<VerifiedBlock> = Vec::new();
        let mut rows: Vec<ProposerRow> = Vec::new();
        let mut beacon_parent = [7u8; 32];
        let mut parent_hash = B256::repeat_byte(9);
        for i in 0..=n {
            let slot = slot0 + 2 * i as u64;
            let header = Header {
                number: first + i as u64,
                parent_hash,
                timestamp: GENESIS_TIME + slot * SECONDS_PER_SLOT,
                beneficiary: alloy_primitives::Address::repeat_byte(i as u8 + 1),
                parent_beacon_block_root: Some(B256::from(beacon_parent)),
                base_fee_per_gas: Some(1),
                withdrawals_root: Some(alloy_consensus::constants::EMPTY_WITHDRAWALS),
                blob_gas_used: Some(0),
                excess_blob_gas: Some(0),
                ..Default::default()
            };
            let hash = header.hash_slow();
            if i == n {
                return (blocks, rows, header);
            }
            let row = ProposerRow {
                number: header.number,
                hash: hash.0,
                slot,
                proposer_index: 100 + i as u64,
                parent_root: beacon_parent,
                state_root: [i as u8; 32],
                body_root: [i as u8 + 50; 32],
            };
            beacon_parent = header_root(&row);
            parent_hash = hash;
            rows.push(row);
            blocks.push(VerifiedBlock {
                hash,
                header,
                withdrawals: Vec::new(),
            });
        }
        unreachable!()
    }

    fn span(n: usize) -> (u64, u64) {
        (DENEB_BLOCK, DENEB_BLOCK + n as u64 - 1)
    }

    #[test]
    fn a_consistent_catalogue_proves_every_proposer() {
        let (blocks, rows, successor) = chain(5);
        let proven = verify(&rows, &blocks, &successor, span(5)).unwrap();
        let who: Vec<u64> = proven.iter().map(|p| p.proposer_index).collect();
        assert_eq!(who, vec![100, 101, 102, 103, 104]);
    }

    #[test]
    fn a_tampered_row_naming_another_validator_fails_the_whole_span() {
        // The tampered row is not one a watched validator would match: the span must still fail,
        // or a filter-first reader would serve a falsely complete empty answer.
        let (blocks, mut rows, successor) = chain(5);
        rows[2].proposer_index = 999;
        assert!(verify(&rows, &blocks, &successor, span(5)).is_err());
    }

    #[test]
    fn every_corrupted_header_field_is_refused() {
        for f in 0..4 {
            let (blocks, mut rows, successor) = chain(4);
            match f {
                0 => rows[1].slot += 2,
                1 => rows[1].parent_root[0] ^= 1,
                2 => rows[1].state_root[3] ^= 1,
                _ => rows[1].body_root[3] ^= 1,
            }
            assert!(
                verify(&rows, &blocks, &successor, span(4)).is_err(),
                "field {f}"
            );
        }
    }

    #[test]
    fn missing_duplicate_and_substituted_rows_are_refused() {
        let (blocks, rows, successor) = chain(4);
        let mut missing = rows.clone();
        missing.remove(2);
        assert!(verify(&missing, &blocks, &successor, span(4)).is_err());
        let mut duplicated = rows.clone();
        duplicated[2] = duplicated[1].clone();
        assert!(verify(&duplicated, &blocks, &successor, span(4)).is_err());
        // A real header from another block, renumbered: its root is not the one committed here.
        let mut substituted = rows.clone();
        substituted[2] = ProposerRow {
            number: rows[2].number,
            hash: rows[2].hash,
            ..rows[3].clone()
        };
        assert!(verify(&substituted, &blocks, &successor, span(4)).is_err());
        let mut wrong_hash = rows.clone();
        wrong_hash[1].hash[0] ^= 1;
        assert!(verify(&wrong_hash, &blocks, &successor, span(4)).is_err());
        let mut short = rows.clone();
        short.pop();
        assert!(
            verify(&short, &blocks, &successor, span(4)).is_err(),
            "the last row missing"
        );
        let mut wrong_height = rows;
        wrong_height[1].number += 1;
        assert!(verify(&wrong_height, &blocks, &successor, span(4)).is_err());
    }

    #[test]
    fn the_last_block_needs_its_real_successor() {
        let (blocks, rows, mut successor) = chain(3);
        let mut other = successor.clone();
        other.parent_beacon_block_root = Some(B256::repeat_byte(1));
        assert!(
            verify(&rows, &blocks, &other, span(3)).is_err(),
            "another root"
        );
        successor.parent_hash = B256::repeat_byte(2);
        assert!(
            verify(&rows, &blocks, &successor, span(3)).is_err(),
            "not linked"
        );
    }

    #[test]
    fn a_header_from_a_later_fork_is_refused() {
        let (blocks, rows, mut successor) = chain(2);
        successor.block_access_list_hash = Some(B256::ZERO);
        let e = verify(&rows, &blocks, &successor, span(2)).unwrap_err();
        assert!(format!("{e:#}").contains("after Fulu"), "{e:#}");
    }

    #[test]
    fn nothing_before_dencun_is_proven() {
        let (blocks, rows, successor) = chain(2);
        assert!(verify(&rows, &blocks, &successor, (DENEB_BLOCK - 1, DENEB_BLOCK)).is_err());
    }

    #[test]
    fn a_slot_that_is_not_the_blocks_timestamp_is_refused() {
        // A consistent beacon chain whose slots are shifted against the execution timestamps.
        let (mut blocks, mut rows, successor) = chain(2);
        blocks[0].header.timestamp += SECONDS_PER_SLOT;
        rows[0].hash = blocks[0].header.hash_slow().0;
        blocks[0].hash = B256::from(rows[0].hash);
        assert!(verify(&rows, &blocks, &successor, span(2)).is_err());
    }

    pub(crate) fn write_cache(dir: &std::path::Path, rows: &[ProposerRow], missed: &[u64]) {
        use std::io::Write;
        let first = rows[0].slot - rows[0].slot % CACHE_SLOTS;
        let f = std::fs::File::create(dir.join(format!("slots-{first}.jsonl.gz"))).unwrap();
        let mut gz = flate2::write::GzEncoder::new(f, flate2::Compression::default());
        let h = |r: &[u8; 32]| format!("0x{}", hex::encode(r));
        for r in rows {
            let line = json!({"slot": r.slot, "root": h(&header_root(r)), "canonical": true, "header": {
                "slot": r.slot.to_string(), "proposer_index": r.proposer_index.to_string(),
                "parent_root": h(&r.parent_root), "state_root": h(&r.state_root), "body_root": h(&r.body_root)}});
            writeln!(gz, "{line}").unwrap();
        }
        for s in missed {
            writeln!(gz, "{}", json!({"slot": s, "missed": true})).unwrap();
        }
        gz.finish().unwrap();
    }

    #[tokio::test]
    async fn a_catalogue_is_built_from_the_header_cache_and_verified() {
        let (blocks, rows, successor) = chain(4);
        let dir = tempfile::tempdir().unwrap();
        write_cache(dir.path(), &rows, &[DENEB_SLOT + 1, DENEB_SLOT + 3]);
        let beacon = Beacon::new(None, Some(dir.path().to_path_buf())).unwrap();
        let built = build_rows(&beacon, &blocks, &successor, span(4), 4)
            .await
            .unwrap();
        assert_eq!(built, rows);
    }

    #[tokio::test]
    async fn a_header_no_source_has_fails_the_span_rather_than_reading_as_no_proposal() {
        let (blocks, rows, successor) = chain(4);
        let dir = tempfile::tempdir().unwrap();
        // The cache holds a forged header for block 2's slot: its root is not the committed one.
        let mut forged = rows.clone();
        forged[2].proposer_index = 7;
        write_cache(dir.path(), &forged, &[]);
        let beacon = Beacon::new(None, Some(dir.path().to_path_buf())).unwrap();
        let e = build_rows(&beacon, &blocks, &successor, span(4), 4)
            .await
            .unwrap_err();
        assert!(format!("{e:#}").contains("no beacon source has"), "{e:#}");
    }
}
