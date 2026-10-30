//! A published mirror as a seeder reads it (RFC-0052 §8): the seed snapshot's envelope, the keys
//! files sit under, and the checks `nuthatch seed` makes before it trusts a catalogue. A node seeds
//! its directory through them and an embedding its host's storage (RFC-0077 S4).
//!
//! The hashes prove each file is the one the mirror's own catalogue names, so nothing changed in
//! transit or at rest. They prove nothing about who wrote the catalogue or whether it is true to the
//! chain. That is decided by whose mirror the operator points at.

use crate::seal::Manifest;
use anyhow::{bail, Result};
use nuthatch_seal::cut::Segment;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// The seed snapshot's envelope, under the dataset prefix.
pub const SEED_FILE: &str = "_seed/seed.json";

/// The catalogue, under the dataset prefix.
pub const CATALOGUE_FILE: &str = "manifest.json";

/// Present from the moment a seed installs a catalogue until it has stamped the store complete. A
/// store holding it is half-seeded: `dev` refuses it and `seed` finishes it.
pub const SEED_PENDING_KEY: &str = "seed_pending";

/// A bundle or a mirrored file whose bytes are not the ones claimed: a content address, a file hash,
/// the decode registry or the NID did not check out. Typed so a caller can report it as such.
#[derive(Debug)]
pub struct Unverified(pub String);

impl std::fmt::Display for Unverified {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Unverified {}

pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// Where a sealed segment sits under the dataset prefix.
pub fn parquet_key(table: &str, hash: &str) -> String {
    format!("{table}/{hash}.parquet")
}

/// Where a table's provisional tail sits in the seed snapshot. Keyed by table, not by hash: the tail
/// is rewritten at every seal, and a hash-keyed object per rewrite would never stop accumulating.
/// Not named `.parquet`, so no reader's glob over the dataset can count a tail as a segment.
pub fn seed_tail_key(table: &str) -> String {
    format!("_seed/{table}.tail")
}

/// What `nuthatch seed` needs beyond the mirror itself: each table's provisional tail, which the
/// mirror withholds, and the block the whole of it is complete through. Outside the reader contract;
/// `_seed/` is rewritten in place and only this envelope says which bytes it should hold.
#[derive(Debug, Serialize, Deserialize)]
pub struct SeedEnvelope {
    pub layout_version: u32,
    pub chain_id: u64,
    pub data_identity: String,
    /// The block the publisher's history begins at, so a `--backfill N` mirror cannot pass for a
    /// full one.
    pub indexed_from: u64,
    /// No row at or below this block is missing from the catalogue plus `tails`.
    pub complete_through: u64,
    /// SHA-256 of the `manifest.json` these tails complete. A seeder that reads another retries.
    pub catalogue_sha256: String,
    /// [`tails_digest`] of `tails`, so a snapshot that lost an entry on the way is refused rather than
    /// seeding a nest that silently lacks that table's tail. Not authentication: an editor can recompute it.
    pub tails_sha256: String,
    pub tails: BTreeMap<String, Segment>,
}

/// Each tail's table, hash and range, in table order.
pub fn tails_digest(tails: &BTreeMap<String, Segment>) -> String {
    let mut h = Sha256::new();
    for (table, s) in tails {
        h.update(format!(
            "{table}\0{}\0{}\0{}\0{}\n",
            s.hash, s.from_block, s.to_block, s.rows
        ));
    }
    hex::encode(h.finalize())
}

/// A snapshot this reader can seed `dataset` on `chain_id` from, for a nest whose earliest declared
/// start block is `declared`.
pub fn check_snapshot(
    s: &SeedEnvelope,
    dataset: &str,
    chain_id: u64,
    declared: Option<u64>,
) -> Result<()> {
    if s.layout_version != 1 {
        bail!(
            "seed snapshot layout_version is {}, and this binary reads 1",
            s.layout_version
        );
    }
    if tails_digest(&s.tails) != s.tails_sha256 {
        return Err(Unverified(
            "the seed snapshot's tails do not match its own digest: an entry was lost or changed, and \
             seeding from it would leave a table missing rows"
                .into(),
        )
        .into());
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

/// One file to have: where the mirror keeps it, and the catalogue entry it must hash to.
#[derive(Clone)]
pub struct Wanted {
    pub key: String,
    pub segment: Segment,
}

/// The catalogue this nest will hold, and the files behind it. Nothing in the mirror's catalogue
/// reaches a path: a file's local name is rebuilt from its table and hash, both checked first.
pub fn local_catalogue(
    mut published: Manifest,
    s: &SeedEnvelope,
) -> Result<(Manifest, Vec<Wanted>)> {
    let mut wanted = Vec::new();
    for (table, segs) in published.tables.iter_mut() {
        for seg in segs.iter_mut() {
            if seg.provisional {
                bail!("the mirror's catalogue lists a provisional segment of {table}");
            }
            canonical(table, seg, s.complete_through)?;
            wanted.push(Wanted {
                key: parquet_key(table, &seg.hash),
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
            key: seed_tail_key(table),
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

pub fn canonical(table: &str, seg: &mut Segment, complete_through: u64) -> Result<()> {
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

/// RFC-0068 §3's postcard digest, as `/play` computes it: SHA-256 over `schema.json`'s bytes, then one
/// sorted `table hash from to rows` line per entry of the cut, base64url without padding.
pub fn postcard_digest(schema_json: &[u8], cut: &Manifest) -> String {
    let mut lines: Vec<String> = cut
        .tables
        .iter()
        .flat_map(|(t, segs)| {
            segs.iter().map(move |s| {
                format!(
                    "{t} {} {} {} {}\n",
                    s.hash, s.from_block, s.to_block, s.rows
                )
            })
        })
        .collect();
    lines.sort();
    let mut h = Sha256::new();
    h.update(schema_json);
    for l in lines {
        h.update(l.as_bytes());
    }
    base64url(&h.finalize())
}

fn base64url(bytes: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let n = chunk.iter().fold(0u32, |n, b| n << 8 | u32::from(*b)) << (8 * (3 - chunk.len()));
        for i in 0..=chunk.len() {
            out.push(A[(n >> (18 - 6 * i) & 63) as usize] as char);
        }
    }
    out
}

/// `published` cut as a postcard pins it: each table's sealed entries at or below its cut, and nothing
/// for a table the cut does not name.
pub fn cut_catalogue(published: &Manifest, cuts: &BTreeMap<String, u64>) -> Manifest {
    let mut out = published.clone();
    for (t, segs) in out.tables.iter_mut() {
        let at = cuts.get(t).copied();
        segs.retain(|s| !s.provisional && at.is_some_and(|at| s.to_block <= at));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seg(hash: char, from: u64, to: u64, rows: usize, provisional: bool) -> Segment {
        serde_json::from_value(serde_json::json!({
            "hash": hash.to_string().repeat(64), "from_block": from, "to_block": to, "rows": rows,
            "file": "f", "provisional": provisional,
        }))
        .unwrap()
    }

    /// The digest `/play`'s `postcard.js` computes for the same catalogue, from Node's crypto.
    #[test]
    fn a_postcard_digest_is_the_one_play_computes() {
        let mut m = Manifest::default();
        m.tables.insert(
            "a".into(),
            vec![seg('1', 1, 5, 3, false), seg('3', 6, 9, 1, true)],
        );
        m.tables.insert("b".into(), vec![seg('2', 6, 9, 4, false)]);
        m.tables.insert("c".into(), vec![seg('4', 1, 2, 1, false)]);
        let mut cuts = BTreeMap::new();
        cuts.insert("a".to_string(), 9);
        cuts.insert("b".to_string(), 9);
        let cut = cut_catalogue(&m, &cuts);
        assert!(
            cut.tables["c"].is_empty(),
            "a table the cut does not name admits nothing"
        );
        assert_eq!(
            cut.tables["a"].len(),
            1,
            "a provisional entry is never pinned"
        );
        let hex = |c: char| c.to_string().repeat(64);
        assert_eq!(cut.tables["a"][0].hash, hex('1'));
        assert_eq!(
            postcard_digest(br#"{"tables":[]}"#, &cut),
            "YgaCaHn3ZKtBKPrgoI6mR35KJEtsggDxtkJ80fF1A3A"
        );
    }
}
