//! Sanctions / watch lists as **content-addressed data** (RFC-0008 C2). A list snapshot is exactly a
//! set of EVM addresses, written as `lists/<sha256>.json` - the hash is a reproducible name for that
//! set.
//!
//! Fetching is **host-side and out-of-band** - never in the data path, never a phone-home during
//! indexing. `lists fetch` downloads (or reads a local `--file`) whatever the source returns - OFAC's
//! SDN.XML, a CSV, a plain address dump - and extracts every `0x…40hex` address from it. Deliberately
//! crypto-address-only: no name/entity fuzzy matching (a false-precision trap, and out of scope). The
//! operator owns the list's provenance; nuthatch owns making the exact set it used reproducible.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::Path;

/// Directory (under a nest) holding content-addressed list snapshots.
pub const LISTS_DIR: &str = "lists";
/// Provenance index at the nest root: which lists were fetched, when, from where.
const INDEX_FILE: &str = "lists.manifest.json";

/// A known list source and its default machine-readable URL. The extractor is source-agnostic (it
/// scrapes `0x…` addresses from whatever bytes come back), so these are conveniences, not a contract -
/// `--url`/`--file` override them, and the snapshot records the actual source used.
pub fn default_url(source: &str) -> Option<&'static str> {
    match source {
        // OFAC's Specially Designated Nationals list (XML carries "Digital Currency Address" fields).
        "ofac-sdn" => Some("https://www.treasury.gov/ofac/downloads/sdn.xml"),
        // EU consolidated financial sanctions list.
        "eu-consolidated" => Some(
            "https://webgate.ec.europa.eu/fsd/fsf/public/files/xmlFullSanctionsList_1_1/content",
        ),
        _ => None,
    }
}

/// One recorded fetch, for provenance (`lists.manifest.json`).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FetchRecord {
    list: String,
    snapshot: String,
    addresses: usize,
    source: String,
    fetched_at: String,
}

/// Fetch a list into `dir`: obtain the bytes (download `url`, or read `file`), extract EVM addresses,
/// and write a content-addressed snapshot under `lists/`. Returns `(snapshot hash, address count)`.
/// Idempotent by content address - the same address set always writes the same file.
pub async fn fetch(
    dir: &Path,
    list: &str,
    url: Option<&str>,
    file: Option<&Path>,
) -> Result<(String, usize)> {
    let raw = match (file, url) {
        (Some(path), _) => std::fs::read_to_string(path)
            .with_context(|| format!("cannot read list file {}", path.display()))?,
        (None, Some(u)) => download(u).await?,
        (None, None) => match default_url(list) {
            Some(u) => download(u).await?,
            None => bail!(
                "unknown list '{list}' and no --url/--file given (known: ofac-sdn, eu-consolidated)"
            ),
        },
    };
    let source = file
        .map(|p| p.display().to_string())
        .or_else(|| url.map(str::to_string))
        .or_else(|| default_url(list).map(str::to_string))
        .unwrap_or_else(|| "unknown".to_string());

    let addresses = extract_addresses(&raw);
    if addresses.is_empty() {
        bail!("no EVM addresses (0x…40hex) found in the fetched list '{list}'");
    }
    let (hash, canonical) = snapshot_bytes(&addresses);
    let lists_dir = dir.join(LISTS_DIR);
    std::fs::create_dir_all(&lists_dir)
        .with_context(|| format!("cannot create {}", lists_dir.display()))?;
    std::fs::write(lists_dir.join(format!("{hash}.json")), &canonical)
        .context("failed to write list snapshot")?;
    record_fetch(dir, list, &hash, addresses.len(), &source)?;
    Ok((hash, addresses.len()))
}

/// Load a list snapshot's addresses by content hash. The addresses are already normalised + sorted.
pub fn load(dir: &Path, hash: &str) -> Result<Vec<String>> {
    let path = dir.join(LISTS_DIR).join(format!("{hash}.json"));
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("no list snapshot {} - fetch it first", path.display()))?;
    serde_json::from_str(&raw).context("corrupt list snapshot")
}

/// Every list snapshot present under `dir/lists/`, as `(hash, address count)` - for `lists list`.
pub fn snapshots(dir: &Path) -> Vec<(String, usize)> {
    let Ok(read) = std::fs::read_dir(dir.join(LISTS_DIR)) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for entry in read.filter_map(|e| e.ok()) {
        let path = entry.path();
        if path.extension().is_none_or(|x| x != "json") {
            continue;
        }
        let Some(hash) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        let count = std::fs::read_to_string(&path)
            .ok()
            .and_then(|s| serde_json::from_str::<Vec<String>>(&s).ok())
            .map(|v| v.len())
            .unwrap_or(0);
        out.push((hash.to_string(), count));
    }
    out.sort();
    out
}

/// Extract every distinct `0x`-prefixed 40-hex-digit address from arbitrary text (XML, CSV, JSON, a
/// plain dump), normalised lowercase, sorted. A hand-rolled scan avoids a regex dependency and is
/// exact: `0x` then exactly 40 hex chars, not part of a longer hex run (so a 64-hex hash isn't
/// mis-read as an address).
pub fn extract_addresses(text: &str) -> Vec<String> {
    let bytes = text.as_bytes();
    let mut set = std::collections::BTreeSet::new();
    let mut i = 0;
    while i + 2 <= bytes.len() {
        if bytes[i] == b'0' && (bytes[i + 1] == b'x' || bytes[i + 1] == b'X') {
            let start = i + 2;
            let mut j = start;
            while j < bytes.len() && bytes[j].is_ascii_hexdigit() {
                j += 1;
            }
            // Exactly 40 hex digits, and not immediately followed by more hex (guards against a
            // longer hex string whose first 40 chars happen to look address-shaped).
            if j - start == 40 {
                let addr = format!("0x{}", text[start..j].to_ascii_lowercase());
                set.insert(addr);
            }
            i = j.max(i + 1);
        } else {
            i += 1;
        }
    }
    set.into_iter().collect()
}

/// Deterministically serialise an address set to canonical bytes + its sha256. Sorted + de-duplicated
/// so the same set always hashes identically regardless of input order.
fn snapshot_bytes(addresses: &[String]) -> (String, Vec<u8>) {
    let mut sorted: Vec<String> = addresses.to_vec();
    sorted.sort();
    sorted.dedup();
    let bytes = serde_json::to_vec(&sorted).expect("addresses serialise");
    let hash = hex::encode(Sha256::digest(&bytes));
    (hash, bytes)
}

async fn download(url: &str) -> Result<String> {
    let shown = crate::rpc::redact_url(url);
    tracing::info!("fetching list from {shown} (host-side, out-of-band)…");
    let resp = reqwest::Client::new()
        .get(url)
        .header("user-agent", "nuthatch")
        .send()
        .await
        .map_err(|e| e.without_url())
        .with_context(|| format!("request to {shown} failed"))?
        .error_for_status()
        .map_err(|e| e.without_url())
        .with_context(|| format!("list source {shown} returned an error status"))?;
    resp.text()
        .await
        .map_err(|e| e.without_url())
        .context("reading list response body")
}

fn record_fetch(dir: &Path, list: &str, hash: &str, count: usize, source: &str) -> Result<()> {
    let index_path = dir.join(INDEX_FILE);
    let mut records: Vec<FetchRecord> = std::fs::read_to_string(&index_path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default();
    records.retain(|r| !(r.list == list && r.snapshot == hash));
    records.push(FetchRecord {
        list: list.to_string(),
        snapshot: hash.to_string(),
        addresses: count,
        source: source.to_string(),
        fetched_at: now_rfc3339(),
    });
    std::fs::write(&index_path, serde_json::to_string_pretty(&records)?)
        .context("failed to write lists manifest")?;
    Ok(())
}

fn now_rfc3339() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("unix:{secs}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_addresses_from_mixed_text() {
        // Two 40-hex addresses (one upper-cased in the source), a 64-hex tx hash (must NOT match),
        // and a duplicate. Result is lowercased, de-duplicated, sorted. Built programmatically so the
        // hex-run lengths are exactly right.
        let addr1 = format!("0x{}", "ab".repeat(20)); // 40 hex
        let addr2 = format!("0x{}", "cd".repeat(20)); // 40 hex, sorts after addr1
        let tx_hash = format!("0x{}", "12".repeat(32)); // 64 hex - not an address
        let text = format!(
            "<a>{}</a> tx={tx_hash} <a>{addr2}</a> dup {addr1}",
            addr1.to_uppercase()
        );
        let addrs = extract_addresses(&text);
        assert_eq!(
            addrs,
            vec![addr1, addr2],
            "lowercased, de-duplicated, sorted; the 64-hex hash is not an address"
        );
    }

    #[test]
    fn snapshot_is_content_addressed_and_order_independent() {
        let a = extract_addresses(
            "0x1111111111111111111111111111111111111111 0x2222222222222222222222222222222222222222",
        );
        let b = extract_addresses("0x2222222222222222222222222222222222222222 0x1111111111111111111111111111111111111111 0x2222222222222222222222222222222222222222");
        assert_eq!(snapshot_bytes(&a).0, snapshot_bytes(&b).0);
    }

    #[tokio::test]
    async fn fetch_from_file_writes_a_loadable_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("sdn.txt");
        std::fs::write(
            &src,
            "sanctioned: 0x1111111111111111111111111111111111111111\n",
        )
        .unwrap();
        let (hash, count) = fetch(dir.path(), "ofac-sdn", None, Some(&src))
            .await
            .unwrap();
        assert_eq!(count, 1);
        let loaded = load(dir.path(), &hash).unwrap();
        assert_eq!(
            loaded,
            vec!["0x1111111111111111111111111111111111111111".to_string()]
        );
        assert_eq!(snapshots(dir.path()).len(), 1);
    }
}
