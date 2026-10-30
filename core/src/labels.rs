//! Labels: user-supplied labeled address sets - the first `annotation` kind (RFC-0008 C1). A label
//! attaches a name (e.g. `exchange`, `mixer`, `treasury`) to an address. Labels are the substrate the
//! direct-exposure view joins against ("how much has address X transacted with the labeled set?").
//!
//! Labels are **list-as-data**, the same discipline sanctions lists will use in C2: an import writes a
//! **content-addressed snapshot** (`labels/<sha256>.json`) whose hash is a stable, reproducible name
//! for exactly that set of (address, label) pairs. Loading merges every snapshot in the directory, so
//! imports are append-only. A snapshot is a plain JSON array of `{address, label}` - the flat shape
//! Burrmill reads directly, so `/sql` can query `labels` (see `analytics::define_views`). No screening
//! API, no phone-home: importing is a host-side, out-of-band act; the data path only ever reads.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::Path;

/// Directory (under a nest) holding content-addressed label snapshots.
pub const LABELS_DIR: &str = "labels";
/// Human-readable provenance index at the nest root: which snapshots were imported, when, from where.
const INDEX_FILE: &str = "labels.manifest.json";

/// One (address, label) pair. `address` is normalised to a lowercase `0x…40hex` string on import, so
/// membership checks against decoded transfer addresses (also lowercase hex) are a plain equality.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LabelEntry {
    pub address: String,
    pub label: String,
}

/// A merged, queryable set of labels: address → the labels attached to it (an address may carry more
/// than one, e.g. a single address labeled by two different imports). Built by [`load`].
#[derive(Debug, Default, Clone)]
pub struct LabelSet {
    by_address: HashMap<String, Vec<String>>,
}

impl LabelSet {
    /// The labels attached to `address` (already-lowercased). Empty slice if none - the common case,
    /// so this is the hot-path membership check for the exposure view.
    pub fn labels_of(&self, address: &str) -> &[String] {
        self.by_address
            .get(address)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// True if any label is attached to `address`.
    pub fn is_labeled(&self, address: &str) -> bool {
        self.by_address.contains_key(address)
    }

    /// Number of distinct labeled addresses.
    pub fn len(&self) -> usize {
        self.by_address.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_address.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Vec<String>)> {
        self.by_address.iter()
    }

    /// Merge one snapshot's text, as [`load`] merges each file; `false` when it is not one.
    pub fn merge_snapshot(&mut self, text: &str) -> bool {
        let Ok(entries) = serde_json::from_str::<Vec<LabelEntry>>(text) else {
            return false;
        };
        for e in entries {
            self.insert(e.address, e.label);
        }
        true
    }

    fn insert(&mut self, address: String, label: String) {
        let labels = self.by_address.entry(address).or_default();
        if !labels.contains(&label) {
            labels.push(label);
        }
    }
}

/// One recorded import, for provenance (`labels.manifest.json`).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ImportRecord {
    snapshot: String,
    entries: usize,
    source: String,
    imported_at: String,
}

/// Import a label file (CSV or JSON) into `dir`: parse → normalise → write a content-addressed
/// snapshot under `labels/` → record provenance. Returns the snapshot hash and the entry count.
/// Idempotent by content address: re-importing the identical set changes nothing on disk.
pub fn import(dir: &Path, source: &Path) -> Result<(String, usize)> {
    let raw = std::fs::read_to_string(source)
        .with_context(|| format!("cannot read label file {}", source.display()))?;
    let entries = parse(&raw, source)?;
    if entries.is_empty() {
        bail!(
            "no valid (address, label) entries found in {}",
            source.display()
        );
    }

    let (hash, canonical) = snapshot_bytes(&entries);
    let labels_dir = dir.join(LABELS_DIR);
    std::fs::create_dir_all(&labels_dir)
        .with_context(|| format!("cannot create {}", labels_dir.display()))?;
    std::fs::write(labels_dir.join(format!("{hash}.json")), &canonical)
        .context("failed to write label snapshot")?;

    record_import(dir, &hash, entries.len(), source)?;
    Ok((hash, entries.len()))
}

/// Load and merge every label snapshot under `dir/labels/` into a queryable [`LabelSet`]. Missing
/// directory → empty set (labels are optional). A single corrupt snapshot is skipped with a warning
/// rather than failing the whole load - one bad file shouldn't blind the indexer to the rest.
pub fn load(dir: &Path) -> LabelSet {
    let mut set = LabelSet::default();
    let labels_dir = dir.join(LABELS_DIR);
    let Ok(read) = std::fs::read_dir(&labels_dir) else {
        return set;
    };
    for entry in read.filter_map(|e| e.ok()) {
        let path = entry.path();
        if path.extension().is_none_or(|x| x != "json") {
            continue;
        }
        let read = std::fs::read_to_string(&path).is_ok_and(|s| set.merge_snapshot(&s));
        if !read {
            tracing::warn!("skipping unreadable label snapshot {}", path.display());
        }
    }
    set
}

/// Deterministically serialise entries to canonical bytes and their sha256 content address. Entries
/// are de-duplicated and sorted (by address, then label) so the *same set* always hashes identically
/// regardless of input order or duplicates - the property that makes the hash a reproducible name.
fn snapshot_bytes(entries: &[LabelEntry]) -> (String, Vec<u8>) {
    let mut sorted: Vec<LabelEntry> = entries.to_vec();
    sorted.sort_by(|a, b| {
        a.address
            .cmp(&b.address)
            .then_with(|| a.label.cmp(&b.label))
    });
    sorted.dedup();
    // Compact, key-ordered JSON (serde serialises struct fields in declaration order: address, label).
    let bytes = serde_json::to_vec(&sorted).expect("LabelEntry serialises");
    let hash = hex::encode(Sha256::digest(&bytes));
    (hash, bytes)
}

/// Parse a label file. JSON (array of `{address,label}` or an object map `{"0xabc":"label"}`) is tried
/// first; anything else is treated as CSV (`address,label` per line, optional header). Rows without a
/// valid address are skipped. Labels containing commas aren't supported in CSV - use JSON for those.
fn parse(raw: &str, source: &Path) -> Result<Vec<LabelEntry>> {
    let trimmed = raw.trim_start();
    if trimmed.starts_with('[') || trimmed.starts_with('{') {
        return parse_json(trimmed);
    }
    // CSV fallback (also covers files with a .csv extension that happen to start with an address).
    let _ = source;
    Ok(parse_csv(raw))
}

fn parse_json(raw: &str) -> Result<Vec<LabelEntry>> {
    // Try the array-of-objects shape first.
    if let Ok(rows) = serde_json::from_str::<Vec<RawEntry>>(raw) {
        return Ok(rows
            .into_iter()
            .filter_map(|r| normalise(&r.address, &r.label))
            .collect());
    }
    // Then the compact object-map shape {"0xaddr": "label"}.
    if let Ok(map) = serde_json::from_str::<HashMap<String, String>>(raw) {
        return Ok(map
            .into_iter()
            .filter_map(|(addr, label)| normalise(&addr, &label))
            .collect());
    }
    bail!("label JSON must be an array of {{address,label}} objects or an {{address: label}} map")
}

fn parse_csv(raw: &str) -> Vec<LabelEntry> {
    raw.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .filter_map(|line| {
            let (addr, label) = line.split_once(',')?;
            // Skip an obvious header row.
            if addr.trim().eq_ignore_ascii_case("address") {
                return None;
            }
            normalise(addr.trim(), label.trim())
        })
        .collect()
}

/// Validate + normalise one entry: address to lowercase `0x…40hex`, label trimmed and non-empty.
fn normalise(address: &str, label: &str) -> Option<LabelEntry> {
    let label = label.trim();
    if label.is_empty() {
        return None;
    }
    let a = address.trim().to_ascii_lowercase();
    let a = a.strip_prefix("0x").unwrap_or(&a);
    if a.len() != 40 || !a.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    Some(LabelEntry {
        address: format!("0x{a}"),
        label: label.to_string(),
    })
}

#[derive(Deserialize)]
struct RawEntry {
    address: String,
    label: String,
}

/// Append an import to the provenance index (best-effort - provenance, not correctness; the snapshots
/// themselves are the source of truth). Uses wall-clock time only for the human-readable record.
/// A set already recorded keeps its first record: the index is a nest input, so rewriting it for the
/// same set would move the nest's identity (#2368).
fn record_import(dir: &Path, hash: &str, entries: usize, source: &Path) -> Result<()> {
    let index_path = dir.join(INDEX_FILE);
    let mut records: Vec<ImportRecord> = std::fs::read_to_string(&index_path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default();
    if records.iter().any(|r| r.snapshot == hash) {
        return Ok(());
    }
    records.push(ImportRecord {
        snapshot: hash.to_string(),
        entries,
        source: source.display().to_string(),
        imported_at: now_rfc3339(),
    });
    let json = serde_json::to_string_pretty(&records)?;
    std::fs::write(&index_path, json).context("failed to write labels manifest")?;
    Ok(())
}

/// A coarse timestamp for the provenance record. Not on any correctness path - the content address is.
fn now_rfc3339() -> String {
    use crate::time::UNIX_EPOCH;
    let secs = crate::time::system_now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("unix:{secs}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_csv_with_header_and_normalises_addresses() {
        let csv = "address,label\n0xAAAABBBBCCCCDDDDEEEEFFFF0000111122223333,exchange\n\
                   0xaaaabbbbccccddddeeeeffff0000111122223333 , mixer \n";
        let entries = parse_csv(csv);
        assert_eq!(entries.len(), 2);
        // Both rows are the same (checksummed vs lowercased) address, normalised identically.
        assert!(entries
            .iter()
            .all(|e| e.address == "0xaaaabbbbccccddddeeeeffff0000111122223333"));
        assert_eq!(entries[0].label, "exchange");
        assert_eq!(entries[1].label, "mixer");
    }

    #[test]
    fn parses_json_array_and_map_shapes() {
        let arr = r#"[{"address":"0x1111111111111111111111111111111111111111","label":"a"}]"#;
        assert_eq!(parse_json(arr).unwrap().len(), 1);
        let map = r#"{"0x2222222222222222222222222222222222222222":"b"}"#;
        assert_eq!(parse_json(map).unwrap().len(), 1);
    }

    #[test]
    fn rejects_malformed_addresses() {
        assert!(normalise("0xnothex", "x").is_none());
        assert!(normalise("0x1234", "x").is_none()); // too short
        assert!(normalise("0x1111111111111111111111111111111111111111", " ").is_none());
        // empty label
    }

    #[test]
    fn snapshot_is_content_addressed_and_order_independent() {
        let a = vec![
            LabelEntry {
                address: "0x02".into(),
                label: "y".into(),
            },
            LabelEntry {
                address: "0x01".into(),
                label: "x".into(),
            },
        ];
        let b = vec![
            LabelEntry {
                address: "0x01".into(),
                label: "x".into(),
            },
            LabelEntry {
                address: "0x02".into(),
                label: "y".into(),
            },
            LabelEntry {
                address: "0x01".into(),
                label: "x".into(),
            }, // duplicate
        ];
        // Same logical set, different order + a dup → identical content address.
        assert_eq!(snapshot_bytes(&a).0, snapshot_bytes(&b).0);
    }

    #[test]
    fn import_then_load_round_trips_and_merges() {
        let dir = tempfile::tempdir().unwrap();
        let f1 = dir.path().join("l1.csv");
        std::fs::write(&f1, "0x1111111111111111111111111111111111111111,exchange\n").unwrap();
        let f2 = dir.path().join("l2.json");
        std::fs::write(
            &f2,
            r#"[{"address":"0x1111111111111111111111111111111111111111","label":"cex"},
                {"address":"0x2222222222222222222222222222222222222222","label":"mixer"}]"#,
        )
        .unwrap();
        import(dir.path(), &f1).unwrap();
        import(dir.path(), &f2).unwrap();

        let set = load(dir.path());
        assert_eq!(set.len(), 2, "two distinct labeled addresses");
        // The first address carries both labels (merged across snapshots).
        let mut labels = set
            .labels_of("0x1111111111111111111111111111111111111111")
            .to_vec();
        labels.sort();
        assert_eq!(labels, vec!["cex".to_string(), "exchange".to_string()]);
        assert!(set.is_labeled("0x2222222222222222222222222222222222222222"));
        assert!(!set.is_labeled("0x9999999999999999999999999999999999999999"));
    }

    /// #2368: the index is a nest input, so the same set imported again, from anywhere, leaves it as it
    /// was. A new set is still recorded.
    #[test]
    fn reimporting_the_same_set_leaves_the_index_alone() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = (dir.path().join("a.csv"), dir.path().join("b.csv"));
        std::fs::write(&a, "0x1111111111111111111111111111111111111111,exchange\n").unwrap();
        std::fs::write(&b, "0x1111111111111111111111111111111111111111,exchange\n").unwrap();
        import(dir.path(), &a).unwrap();
        let index = std::fs::read(dir.path().join(INDEX_FILE)).unwrap();
        import(dir.path(), &b).unwrap();
        assert_eq!(std::fs::read(dir.path().join(INDEX_FILE)).unwrap(), index);

        std::fs::write(&b, "0x2222222222222222222222222222222222222222,mixer\n").unwrap();
        import(dir.path(), &b).unwrap();
        let records: Vec<ImportRecord> =
            serde_json::from_slice(&std::fs::read(dir.path().join(INDEX_FILE)).unwrap()).unwrap();
        assert_eq!(records.len(), 2);
    }
}
