//! Seed a nest that has not indexed from a published mirror, instead of an RPC backfill
//! (RFC-0052 §8).
//!
//! The mirror's catalogue and each table's provisional tail (the `_seed/` snapshot) are downloaded,
//! every file is checked against its content address, and the store is left exactly as a
//! `--seal-direct` backfill leaves it on finishing at the snapshot's `complete_through`. The first
//! `dev` is then a warm start: it folds the segments and follows the chain from the next block.
//!
//! The hashes prove each file is the one the mirror's own catalogue names, so nothing changed in
//! transit or at rest. They prove nothing about who wrote the catalogue or whether it is true to the
//! chain. That is decided by whose mirror the operator points at.

use anyhow::{bail, Context, Result};
use futures::StreamExt;
use std::path::Path;

use crate::config::Config;
use crate::indexer::{LAST_BLOCK_KEY, SEALED_THROUGH_KEY, START_BLOCK_KEY};
use crate::publish::{self, Mirror, SeedEnvelope};
use crate::seal::{self, Manifest, Segment, MANIFEST_FILE};
use crate::store::Store;

/// Present from the moment a seed installs a catalogue until it has stamped the store complete. A
/// store holding it is half-seeded: `dev` refuses it and `seed` finishes it.
pub const SEED_PENDING_KEY: &str = "seed_pending";
const PARALLELISM: usize = 8;
/// Far above any segment a seal writes; it bounds what a hostile mirror can put on disk (#2202).
const MAX_SEGMENT_BYTES: u64 = 4 << 30;
/// Reads of the catalogue and the snapshot that may disagree before giving up: a publisher writes
/// the first, then the second, and a pass is short.
const SNAPSHOT_ATTEMPTS: usize = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeedReport {
    pub dataset: String,
    pub segments: usize,
    /// Segments downloaded now; the rest were already on disk from an interrupted run.
    pub fetched: usize,
    pub fetched_bytes: u64,
    pub indexed_from: u64,
    pub complete_through: u64,
}

pub async fn seed(dir: &Path, from: &str) -> Result<SeedReport> {
    let config = Config::load(dir)?;
    if seal::shared_store(dir).is_some() {
        bail!(
            "{} is a mounted dataset, and seed only fills a nest's own directory",
            dir.display()
        );
    }
    let db = dir.join(crate::config::DB_FILE);
    refuse_an_indexed_nest(&db)?;
    // `schema.json` is part of the data identity, and `dev` writes a missing one at its first start,
    // so the publisher's identity includes it. Only a missing one: regenerating a present file with
    // this binary would move the identity away from a publisher whose release wrote it differently.
    if !dir.join("schema.json").exists() {
        crate::project::refresh_stale_artifacts(dir, &config)?;
    }

    let (dataset, _, _, chain_id) = publish::identity_of(dir)?;
    let mirror = publish::open_mirror(from)?;
    let (snapshot, published) = read_snapshot(mirror.as_ref(), &dataset).await?;
    check_snapshot(&snapshot, &dataset, chain_id, &config)?;
    let (catalogue, wanted) = local_catalogue(published, &snapshot)?;

    let (fetched, fetched_bytes) = download(mirror.as_ref(), dir, &dataset, &wanted).await?;
    let store = Store::open(&db)?;
    store.set_meta(SEED_PENDING_KEY, &dataset)?;
    seal::install_manifest(dir, &catalogue)?;
    crate::indexer::stamp_fresh_store(&store, dir, &config)?;
    let through = snapshot.complete_through.to_string();
    let from = snapshot.indexed_from.to_string();
    store.set_metas(
        &[
            (START_BLOCK_KEY, &from),
            (SEALED_THROUGH_KEY, &through),
            (LAST_BLOCK_KEY, &through),
        ],
        &[SEED_PENDING_KEY],
    )?;

    Ok(SeedReport {
        dataset,
        segments: wanted.len(),
        fetched,
        fetched_bytes,
        indexed_from: snapshot.indexed_from,
        complete_through: snapshot.complete_through,
    })
}

pub async fn run(dir: &Path, from: &str) -> Result<()> {
    let r = seed(dir, from).await?;
    println!("dataset           {}", r.dataset);
    println!(
        "history           blocks {}..={}",
        r.indexed_from, r.complete_through
    );
    println!(
        "segments          {} ({} fetched, {} bytes)",
        r.segments, r.fetched, r.fetched_bytes
    );
    println!(
        "\nSeeded. `nuthatch dev --dir {}` now follows the chain from block {}.",
        dir.display(),
        r.complete_through + 1
    );
    println!(
        "Every file matches the mirror's catalogue. Who wrote that catalogue, and whether it is true \
         to the chain, is not something a hash can show."
    );
    Ok(())
}

fn refuse_an_indexed_nest(db: &Path) -> Result<()> {
    if !db.exists() {
        return Ok(());
    }
    let store = Store::open_existing(db)
        .context("opening this nest's store (is the nest running? stop it first)")?;
    if store.get_meta(SEED_PENDING_KEY)?.is_some() {
        return Ok(());
    }
    if store.get_meta(LAST_BLOCK_KEY)?.is_some() || store.sealed_through() > 0 {
        bail!(
            "this nest has already indexed. Seeding fills a nest that holds no data; to replace \
             what is here, remove `nuthatch.redb` and `segments/` first."
        );
    }
    Ok(())
}

async fn read_snapshot(mirror: &dyn Mirror, dataset: &str) -> Result<(SeedEnvelope, Manifest)> {
    for attempt in 0..SNAPSHOT_ATTEMPTS {
        if attempt > 0 {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        let Some(seed_bytes) = mirror
            .get(&format!("{dataset}/{}", publish::SEED_FILE))
            .await?
        else {
            if mirror
                .head(&format!("{dataset}/publish.json"))
                .await?
                .is_none()
            {
                bail!(
                    "the mirror holds no dataset {dataset}. Its name is this nest's data identity, \
                     so either nothing was published there or this directory differs from the publisher's: \
                     an edit, a stray file, or a schema.json written by another release."
                );
            }
            bail!(
                "the mirror holds dataset {dataset} but no seed snapshot. A snapshot is written by a \
                 running nest's `--publish-target`, or by `publish sync` on a stopped one, from a \
                 release that has `nuthatch seed`."
            );
        };
        let snapshot: SeedEnvelope =
            serde_json::from_slice(&seed_bytes).context("corrupt seed snapshot")?;
        let catalogue = mirror
            .get(&format!("{dataset}/{MANIFEST_FILE}"))
            .await?
            .context("the mirror has a seed snapshot but no catalogue")?;
        if publish::sha256_hex(&catalogue) == snapshot.catalogue_sha256 {
            let published =
                serde_json::from_slice(&catalogue).context("corrupt mirror catalogue")?;
            return Ok((snapshot, published));
        }
    }
    bail!(
        "the mirror's catalogue and its seed snapshot disagreed {SNAPSHOT_ATTEMPTS} times running. \
         The publisher writes one and then the other; if it is not mid-pass, its last pass failed."
    )
}

fn check_snapshot(s: &SeedEnvelope, dataset: &str, chain_id: u64, config: &Config) -> Result<()> {
    if s.layout_version != 1 {
        bail!(
            "seed snapshot layout_version is {}, and this binary reads 1",
            s.layout_version
        );
    }
    if publish::tails_digest(&s.tails) != s.tails_sha256 {
        bail!(
            "the seed snapshot's tails do not match its own digest: an entry was lost or changed, and \
             seeding from it would leave a table missing rows"
        );
    }
    if s.data_identity != dataset {
        bail!(
            "the seed snapshot is for dataset {}, not this nest's {dataset}",
            s.data_identity
        );
    }
    if s.chain_id != chain_id {
        bail!(
            "the seed snapshot is for chain {}, and this nest is on {chain_id}",
            s.chain_id
        );
    }
    let declared = config.contracts.iter().filter_map(|c| c.start_block).min();
    if let Some(declared) = declared.filter(|d| s.indexed_from > *d) {
        bail!(
            "the mirror's history begins at block {}, and this nest declares block {declared}. A \
             nest seeded from it would be missing {} blocks and would not know.",
            s.indexed_from,
            s.indexed_from - declared
        );
    }
    Ok(())
}

/// One file to have on disk: where the mirror keeps it, and the catalogue entry it must hash to.
struct Wanted {
    key: String,
    segment: Segment,
}

/// The catalogue this nest will hold, and the files behind it. Nothing in the mirror's catalogue
/// reaches a path: a file's local name is rebuilt from its table and hash, both checked first.
fn local_catalogue(mut published: Manifest, s: &SeedEnvelope) -> Result<(Manifest, Vec<Wanted>)> {
    let mut wanted = Vec::new();
    for (table, segs) in published.tables.iter_mut() {
        for seg in segs.iter_mut() {
            if seg.provisional {
                bail!("the mirror's catalogue lists a provisional segment of {table}");
            }
            canonical(table, seg, s.complete_through)?;
            wanted.push(Wanted {
                key: publish::parquet_key(table, &seg.hash),
                segment: seg.clone(),
            });
        }
    }
    for (table, tail) in &s.tails {
        let mut tail = tail.clone();
        if !tail.provisional {
            bail!("the seed snapshot's tail of {table} is not provisional");
        }
        canonical(table, &mut tail, s.complete_through)?;
        wanted.push(Wanted {
            key: publish::seed_tail_key(table),
            segment: tail.clone(),
        });
        published
            .tables
            .entry(table.clone())
            .or_default()
            .push(tail);
    }
    for (table, segs) in published.tables.iter_mut() {
        segs.sort_by_key(|s| (s.from_block, s.to_block));
        for pair in segs.windows(2) {
            if pair[1].from_block <= pair[0].to_block {
                bail!(
                    "the mirror lists segments of {table} that overlap at blocks {}..={}; seeding it \
                     would count those rows twice",
                    pair[1].from_block,
                    pair[0].to_block
                );
            }
        }
    }
    Ok((published, wanted))
}

fn canonical(table: &str, seg: &mut Segment, complete_through: u64) -> Result<()> {
    let table_ok = !table.is_empty()
        && table
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_');
    let hash_ok = seg.hash.len() == 64
        && seg
            .hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
    if !table_ok || !hash_ok {
        bail!(
            "the mirror names a table or hash this binary would not have written: {table:?}, {:?}",
            seg.hash
        );
    }
    if seg.from_block > seg.to_block || seg.to_block > complete_through {
        bail!(
            "the mirror's segment {} of {table} covers blocks {}..={}, outside a snapshot complete \
             through {complete_through}",
            seg.hash,
            seg.from_block,
            seg.to_block
        );
    }
    seg.file = format!("{table}-{}.parquet", seg.hash);
    Ok(())
}

/// Bring every wanted file onto disk, each checked against its hash. Returns how many were fetched
/// and their bytes; a file already there and intact is left alone, so a second run resumes.
async fn download(
    mirror: &dyn Mirror,
    dir: &Path,
    dataset: &str,
    wanted: &[Wanted],
) -> Result<(usize, u64)> {
    let segments = dir.join(seal::SEGMENTS_DIR);
    std::fs::create_dir_all(&segments)
        .with_context(|| format!("creating {}", segments.display()))?;
    let segments = &segments;
    let results: Vec<Result<u64>> = futures::stream::iter(wanted.iter().map(|w| async move {
        let dest = segments.join(&w.segment.file);
        if let Ok(have) = std::fs::read(&dest) {
            if publish::sha256_hex(&have) == w.segment.hash {
                return Ok(0);
            }
        }
        let tmp = dest.with_extension("parquet.tmp");
        let got = mirror
            .get_file(&format!("{dataset}/{}", w.key), &tmp, MAX_SEGMENT_BYTES)
            .await;
        let got = match got {
            Ok(Some(h)) => h,
            Ok(None) => bail!("the mirror's catalogue names {} but it is not there", w.key),
            Err(e) => {
                let _ = std::fs::remove_file(&tmp);
                return Err(e);
            }
        };
        if got != w.segment.hash {
            let _ = std::fs::remove_file(&tmp);
            if w.segment.provisional {
                bail!(
                    "{} is not the tail the seed snapshot names. The publisher rewrites tails as \
                     it seals; run seed again, it keeps what it has fetched.",
                    w.key
                );
            }
            bail!("{} does not hash to its content address", w.key);
        }
        let len = std::fs::metadata(&tmp)
            .with_context(|| format!("reading {}", tmp.display()))?
            .len();
        std::fs::rename(&tmp, &dest).with_context(|| format!("installing {}", dest.display()))?;
        Ok(len)
    }))
    .buffer_unordered(PARALLELISM)
    .collect()
    .await;
    let mut fetched = 0;
    let mut bytes = 0;
    for r in results {
        let n = r?;
        if n > 0 {
            fetched += 1;
            bytes += n;
        }
    }
    Ok((fetched, bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn segment(hash: &str, from: u64, to: u64) -> Segment {
        serde_json::from_value(serde_json::json!({
            "hash": hash, "from_block": from, "to_block": to, "rows": 1, "file": "../../etc/passwd",
        }))
        .unwrap()
    }

    #[test]
    fn a_catalogue_entry_never_chooses_its_own_path() {
        let hash = "ab".repeat(32);
        let mut seg = segment(&hash, 1, 5);
        canonical("usdc__transfer", &mut seg, 5).unwrap();
        assert_eq!(seg.file, format!("usdc__transfer-{hash}.parquet"));

        for table in ["../x", "a/b", "", "a.b"] {
            canonical(table, &mut segment(&hash, 1, 5), 5)
                .expect_err("a table name that could leave the segments directory");
        }
        for bad in ["../../x", &"AB".repeat(32), &"ab".repeat(31)] {
            canonical("t", &mut segment(bad, 1, 5), 5).expect_err("a hash that is not one");
        }
    }

    fn envelope(complete_through: u64) -> SeedEnvelope {
        serde_json::from_value(serde_json::json!({
            "layout_version": 1, "chain_id": 1, "data_identity": "d", "indexed_from": 1,
            "complete_through": complete_through, "catalogue_sha256": "c", "tails": {},
            "tails_sha256": "",
        }))
        .unwrap()
    }

    #[test]
    fn a_catalogue_listing_rows_twice_is_refused() {
        let (a, b) = ("ab".repeat(32), "cd".repeat(32));
        for segs in [
            vec![segment(&a, 1, 5), segment(&a, 1, 5)],
            vec![segment(&a, 1, 5), segment(&b, 5, 9)],
            vec![segment(&b, 4, 9), segment(&a, 1, 5)],
        ] {
            let mut m = Manifest::default();
            m.tables.insert("t".into(), segs);
            let err = local_catalogue(m, &envelope(9))
                .err()
                .expect("overlapping segments");
            assert!(format!("{err:#}").contains("twice"), "{err:#}");
        }
        let mut m = Manifest::default();
        m.tables
            .insert("t".into(), vec![segment(&b, 6, 9), segment(&a, 1, 5)]);
        local_catalogue(m, &envelope(9)).expect("adjacent segments are fine");
    }

    #[test]
    fn a_segment_past_the_snapshot_is_refused() {
        let hash = "ab".repeat(32);
        canonical("t", &mut segment(&hash, 1, 6), 5)
            .expect_err("rows above complete_through would be fetched again and counted twice");
        canonical("t", &mut segment(&hash, 6, 5), 9).expect_err("an inverted range");
    }
}
