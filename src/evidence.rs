//! An evidence bundle (#2944): one answer from a nest, with what someone else needs to get it again.
//! `make` asks a running nest for a statement as of a sealed block and records the nest's identity,
//! the block, the rows and a hash of them; `verify` asks a nest with the same identity again and says
//! whether the answer is the same. Both go through the HTTP API, like any client.

use anyhow::{anyhow, bail, Context, Result};
use ed25519_dalek::{Signer, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::path::Path;

/// Rows a bundle may carry. A larger answer is refused rather than truncated.
pub const MAX_ROWS: usize = 10_000;

#[derive(Debug, Serialize, Deserialize)]
pub struct Bundle {
    pub evidence: Body,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<crate::pack::Signature>,
}

/// The signed part. Field order is fixed, so its JSON encoding is deterministic.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Body {
    pub chain: String,
    pub chain_id: u64,
    pub nid: String,
    pub registry_hash: String,
    pub statement: String,
    pub block: u64,
    /// The time the block was placed from, when the bundle was asked for at a time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time: Option<u64>,
    pub rows: Vec<Value>,
    /// sha256 of the rows in canonical order: each row's JSON (keys sorted), sorted.
    pub rows_sha256: String,
    /// The label set the rows read, where they read `labels` (#2961): a NID does not pin one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub labels_sha256: Option<String>,
    pub made_with: String,
}

/// The rows' hash, independent of the order the engine returned them in.
pub fn rows_hash(rows: &[Value]) -> String {
    let mut lines: Vec<String> = rows.iter().map(|r| r.to_string()).collect();
    lines.sort();
    let mut h = Sha256::new();
    for line in &lines {
        h.update(line.as_bytes());
        h.update(b"\n");
    }
    hex::encode(h.finalize())
}

/// What `/nest` says identifies the nest.
struct Identity {
    chain: String,
    chain_id: u64,
    nid: String,
    registry_hash: String,
    labels_sha256: Option<String>,
}

async fn identity(client: &reqwest::Client, url: &str) -> Result<Identity> {
    let v: Value = get(client, &format!("{url}/nest")).await?;
    let text = |k: &str| {
        v[k].as_str()
            .map(String::from)
            .ok_or_else(|| anyhow!("/nest has no `{k}`"))
    };
    Ok(Identity {
        chain: text("chain")?,
        chain_id: v["chain_id"]
            .as_u64()
            .ok_or_else(|| anyhow!("/nest has no `chain_id`"))?,
        nid: text("nid")?,
        registry_hash: text("registry_hash")?,
        labels_sha256: v["labels_sha256"].as_str().map(String::from),
    })
}

async fn get(client: &reqwest::Client, url: &str) -> Result<Value> {
    let resp = client.get(url).send().await.map_err(|e| e.without_url())?;
    let status = resp.status();
    let v: Value = resp.json().await.map_err(|e| e.without_url())?;
    if !status.is_success() {
        bail!("{}", v["error"].as_str().unwrap_or("the nest refused"));
    }
    Ok(v)
}

/// The statement's rows as of `block`, refused if they are not the whole, sealed answer, and the
/// label set they read.
async fn answer(
    client: &reqwest::Client,
    url: &str,
    q: &str,
    block: u64,
) -> Result<(Vec<Value>, Option<String>)> {
    let resp = client
        .post(format!("{url}/sql"))
        .json(&json!({ "q": q, "at_block": block, "max_rows": MAX_ROWS + 1 }))
        .send()
        .await
        .map_err(|e| e.without_url())?;
    let status = resp.status();
    let v: Value = resp.json().await.map_err(|e| e.without_url())?;
    if !status.is_success() {
        bail!("{}", v["error"].as_str().unwrap_or("the query was refused"));
    }
    let sealed = v["provenance"]["sealed_through"].as_u64().unwrap_or(0);
    if block > sealed {
        bail!("block {block} is not sealed yet (sealed through {sealed}): rows above it can still reorg");
    }
    for flag in ["truncated", "degraded", "tip_unavailable"] {
        if v[flag] == Value::Bool(true) {
            bail!("the answer is {flag}, so it is not the whole answer");
        }
    }
    let rows = v["rows"]
        .as_array()
        .cloned()
        .ok_or_else(|| anyhow!("the answer has no rows"))?;
    if rows.len() > MAX_ROWS {
        bail!("the answer has more than {MAX_ROWS} rows; aggregate it first");
    }
    let labels = v["provenance"]["labels_sha256"].as_str().map(String::from);
    Ok((rows, labels))
}

/// The block `/at` places `time` at.
async fn place(client: &reqwest::Client, url: &str, time: u64) -> Result<u64> {
    let at = get(client, &format!("{url}/at?time={time}")).await?;
    at["block"]
        .as_u64()
        .ok_or_else(|| anyhow!("{time} is before any block this nest holds a time for"))
}

/// Ask the nest at `url` for `q` as of `block`, or as of `time` placed by `/at`.
pub async fn make(
    url: &str,
    q: &str,
    block: Option<u64>,
    time: Option<u64>,
    key: Option<&Path>,
) -> Result<Bundle> {
    let client = reqwest::Client::new();
    let id = identity(&client, url).await?;
    let block = match (block, time) {
        (Some(b), None) => b,
        (None, Some(t)) => place(&client, url, t).await?,
        _ => bail!("give --at-block or --at-time"),
    };
    let (rows, labels_sha256) = answer(&client, url, q, block).await?;
    let evidence = Body {
        chain: id.chain,
        chain_id: id.chain_id,
        nid: id.nid,
        registry_hash: id.registry_hash,
        statement: q.to_string(),
        block,
        time,
        rows_sha256: rows_hash(&rows),
        labels_sha256,
        rows,
        made_with: format!("nuthatch {}", env!("CARGO_PKG_VERSION")),
    };
    let signature = match key {
        Some(path) => {
            let sk = crate::pack::load_signing_key(path)?;
            let sig = sk.sign(&serde_json::to_vec(&evidence)?);
            Some(crate::pack::Signature {
                pubkey: hex::encode(sk.verifying_key().to_bytes()),
                sig: hex::encode(sig.to_bytes()),
            })
        }
        None => None,
    };
    Ok(Bundle {
        evidence,
        signature,
    })
}

/// What `verify` found, field by field.
#[derive(Debug, Default)]
pub struct Report {
    pub mismatches: Vec<String>,
    pub signature: Option<bool>,
}

impl Report {
    pub fn ok(&self) -> bool {
        self.mismatches.is_empty() && self.signature != Some(false)
    }
}

/// Check `bundle` against the nest at `url`: the same identity, the same label set where the rows read
/// one, the same block for its time, the same rows at that block, and, given a trusted key, a
/// signature by it.
pub async fn verify(bundle: &Bundle, url: &str, trusted: Option<&VerifyingKey>) -> Result<Report> {
    let mut report = Report::default();
    let e = &bundle.evidence;
    if rows_hash(&e.rows) != e.rows_sha256 {
        report
            .mismatches
            .push("the rows do not hash to rows_sha256".into());
    }
    if let Some(pk) = trusted {
        report.signature = Some(bundle.signature.as_ref().is_some_and(|s| {
            let bytes = hex::decode(&s.sig)
                .ok()
                .and_then(|b| <[u8; 64]>::try_from(b).ok());
            s.pubkey == hex::encode(pk.to_bytes())
                && bytes.is_some_and(|b| {
                    serde_json::to_vec(e).is_ok_and(|body| {
                        pk.verify(&body, &ed25519_dalek::Signature::from_bytes(&b))
                            .is_ok()
                    })
                })
        }));
    }
    let client = reqwest::Client::new();
    let id = identity(&client, url).await?;
    for (field, theirs, ours) in [
        ("chain", &e.chain, &id.chain),
        ("nid", &e.nid, &id.nid),
        ("registry_hash", &e.registry_hash, &id.registry_hash),
    ] {
        if theirs != ours {
            report.mismatches.push(format!(
                "{field}: the bundle says {theirs}, this nest is {ours}"
            ));
        }
    }
    if e.chain_id != id.chain_id {
        report.mismatches.push(format!(
            "chain_id: the bundle says {}, this nest is {}",
            e.chain_id, id.chain_id
        ));
    }
    if e.labels_sha256.is_some() && e.labels_sha256 != id.labels_sha256 {
        report.mismatches.push(format!(
            "labels: the rows read label set {}, this nest holds {}",
            e.labels_sha256.as_deref().unwrap_or("none"),
            id.labels_sha256.as_deref().unwrap_or("none")
        ));
    }
    if !report.mismatches.is_empty() {
        return Ok(report);
    }
    if let Some(t) = e.time {
        let placed = place(&client, url, t).await?;
        if placed != e.block {
            report.mismatches.push(format!(
                "time: {t} places at block {placed}, the bundle says {}",
                e.block
            ));
        }
    }
    let (rows, _) = answer(&client, url, &e.statement, e.block).await?;
    let got = rows_hash(&rows);
    if got != e.rows_sha256 {
        report.mismatches.push(format!(
            "rows at block {}: the bundle hashes to {}, this nest answers {got}",
            e.block, e.rows_sha256
        ));
    }
    Ok(report)
}

/// `nuthatch evidence make|verify`.
pub async fn run(args: crate::cli::EvidenceArgs) -> Result<()> {
    match args.what {
        crate::cli::EvidenceWhat::Make(a) => {
            let bundle = make(&a.url, &a.query, a.at_block, a.at_time, a.key.as_deref()).await?;
            let text = serde_json::to_string_pretty(&bundle)?;
            match &a.out {
                Some(path) => std::fs::write(path, text + "\n")
                    .with_context(|| format!("cannot write {}", path.display()))?,
                None => println!("{text}"),
            }
            eprintln!(
                "evidence: {} row(s) at block {}, rows {}",
                bundle.evidence.rows.len(),
                bundle.evidence.block,
                bundle.evidence.rows_sha256
            );
            Ok(())
        }
        crate::cli::EvidenceWhat::Verify(a) => {
            let raw = std::fs::read_to_string(&a.bundle)
                .with_context(|| format!("cannot read {}", a.bundle.display()))?;
            let bundle: Bundle = serde_json::from_str(&raw).context("not an evidence bundle")?;
            let trusted = a
                .pubkey
                .as_deref()
                .map(crate::pack::parse_pubkey)
                .transpose()?;
            let report = verify(&bundle, &a.url, trusted.as_ref()).await?;
            for m in &report.mismatches {
                println!("MISMATCH {m}");
            }
            match report.signature {
                Some(true) => println!("signature: valid for the trusted key"),
                Some(false) => println!("signature: NOT valid for the trusted key"),
                None => println!("signature: not checked (no --pubkey)"),
            }
            if report.ok() {
                println!(
                    "match: {} row(s) at block {} reproduce",
                    bundle.evidence.rows.len(),
                    bundle.evidence.block
                );
                Ok(())
            } else {
                bail!("the bundle does not verify against this nest")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_rows_hash_ignores_row_order_but_not_content() {
        let a = [json!({"n": 1, "who": "x"}), json!({"who": "y", "n": 2})];
        let b = [a[1].clone(), a[0].clone()];
        assert_eq!(rows_hash(&a), rows_hash(&b));
        assert_ne!(
            rows_hash(&a),
            rows_hash(&[a[0].clone(), json!({"who": "y", "n": 3})])
        );
        assert_ne!(rows_hash(&a), rows_hash(&a[..1]));
    }
}
