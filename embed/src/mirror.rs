//! RFC-0077 S4: a nest's sealed history hydrated from a published mirror, as `nuthatch seed` fills a
//! node's directory (RFC-0052 §8), with the seed's own checks from `nuthatch_core::mirror`. The
//! catalogue and the seed snapshot arrive as text; each segment is appended by the host to the pack
//! and read back here against its content address before anything names it.
//!
//! Optionally the catalogue is pinned by a postcard (RFC-0068 §3): cut at each table's block, it must
//! hash with `schema.json` to the postcard's digest, and no seed tail is read, so the nest is what the
//! publisher had sealed and does not follow the chain past it.

use crate::budget::TabBudget;
use crate::cellar::Cellar;
use crate::derived::NestFiles;
use crate::nest::NestSpec;
use anyhow::{anyhow, bail, Context, Result};
use futures::StreamExt;
use nuthatch_core::mirror::{
    check_snapshot, cut_catalogue, local_catalogue, postcard_digest, sha256_hex, SeedEnvelope,
    Unverified, Wanted, CATALOGUE_FILE, SEED_FILE, SEED_PENDING_KEY,
};
use nuthatch_core::redb_store::Store;
use nuthatch_core::seal::Manifest;
use nuthatch_core::store::{LAST_BLOCK_KEY, SEALED_THROUGH_KEY, START_BLOCK_KEY};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;

/// Segments in flight at once, as `nuthatch seed` fetches them.
const PARALLELISM: usize = 8;
/// Far above any segment a seal writes; it bounds what a hostile mirror can put in storage (#2202).
const MAX_SEGMENT_BYTES: u64 = 4 << 30;
/// The catalogue, the snapshot and `schema.json` are text a few MiB at most.
const MAX_TEXT_BYTES: u64 = 64 << 20;
/// Reads of the catalogue and the snapshot that may disagree before giving up.
const SNAPSHOT_ATTEMPTS: usize = 5;

/// `nh_open`'s `mirror`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MirrorConfig {
    /// The published directory's base; the dataset is the directory under it.
    pub base: String,
    pub postcard: Option<Postcard>,
}

/// A postcard's pin: each table's cut, and the digest the cut catalogue must hash to.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Postcard {
    pub cuts: BTreeMap<String, u64>,
    pub digest: String,
}

/// What a hydration did, for `nh_open`'s caller to see.
#[derive(Debug, Clone)]
pub struct Hydrated {
    pub dataset: String,
    pub segments: usize,
    pub bytes: u64,
    pub indexed_from: u64,
    pub complete_through: u64,
    pub pinned: bool,
}

impl Hydrated {
    pub fn to_json(&self) -> Value {
        json!({
            "dataset": self.dataset,
            "segments": self.segments,
            "bytes": self.bytes,
            "indexed_from": self.indexed_from,
            "complete_through": self.complete_through,
            "pinned": self.pinned,
        })
    }
}

/// A host's GETs. `text` answers `None` for a 404; `append` writes the body at the pack's end and
/// answers where it went.
#[async_trait::async_trait(?Send)]
pub trait MirrorHost {
    async fn text(&self, url: &str, max_bytes: u64) -> Result<Option<String>>;
    async fn append(&self, url: &str, max_bytes: u64) -> Result<Option<(u64, u64)>>;
    async fn rest(&self, ms: u64);
}

/// Whether `store` is a hydration that did not finish, which starts again from nothing.
pub fn pending(store: &Store) -> Result<bool> {
    Ok(store.get_meta(SEED_PENDING_KEY)?.is_some())
}

/// Whether `store` holds history already, so a mirror has nothing to add.
pub fn indexed(store: &Store) -> Result<bool> {
    Ok(!pending(store)? && store.get_meta(LAST_BLOCK_KEY)?.is_some())
}

/// Refuse, before anything is fetched, a nest whose declared state the ceiling cannot hold.
pub fn admit(files: &NestFiles, budget: TabBudget) -> Result<()> {
    let rows = crate::derived::declared_rows(files)?;
    budget
        .admit_entities(rows)
        .map_err(|e| anyhow!("refused before anything was downloaded from the mirror: {e}"))
}

fn url(base: &str, dataset: &str, key: &str) -> String {
    format!("{}/{dataset}/{key}", base.trim_end_matches('/'))
}

async fn need_text(host: &dyn MirrorHost, url: &str, what: &str) -> Result<String> {
    host.text(url, MAX_TEXT_BYTES)
        .await?
        .ok_or_else(|| anyhow!("the mirror has no {what} at {url}"))
}

/// The snapshot and the catalogue it completes, as `nuthatch seed` reads them.
async fn read_snapshot(
    host: &dyn MirrorHost,
    base: &str,
    dataset: &str,
) -> Result<(SeedEnvelope, Manifest, String)> {
    for attempt in 0..SNAPSHOT_ATTEMPTS {
        if attempt > 0 {
            host.rest(1_000).await;
        }
        let Some(seed) = host
            .text(&url(base, dataset, SEED_FILE), MAX_TEXT_BYTES)
            .await?
        else {
            if host
                .text(&url(base, dataset, "publish.json"), MAX_TEXT_BYTES)
                .await?
                .is_none()
            {
                bail!(
                    "the mirror holds no dataset {dataset}. Its name is this nest's data identity, \
                     so either nothing was published there or these files differ from the \
                     publisher's: an edit, a stray file, or a schema.json written by another release."
                );
            }
            bail!(
                "the mirror holds dataset {dataset} but no seed snapshot. A snapshot is written by a \
                 running nest's `--publish-target`, or by `publish sync` on a stopped one, from a \
                 release that has `nuthatch seed`."
            );
        };
        let snapshot: SeedEnvelope =
            serde_json::from_str(&seed).context("corrupt seed snapshot")?;
        let catalogue = need_text(host, &url(base, dataset, CATALOGUE_FILE), "catalogue").await?;
        if sha256_hex(catalogue.as_bytes()) == snapshot.catalogue_sha256 {
            let published = serde_json::from_str(&catalogue).context("corrupt mirror catalogue")?;
            return Ok((snapshot, published, catalogue));
        }
    }
    bail!(
        "the mirror's catalogue and its seed snapshot disagreed {SNAPSHOT_ATTEMPTS} times running. \
         The publisher writes one and then the other; if it is not mid-pass, its last pass failed."
    )
}

/// Hydrate `store` and `cellar` from `mirror`: the nest's history through the snapshot's
/// `complete_through`, or through the postcard's cut, every file checked against its address.
pub async fn hydrate(
    host: &dyn MirrorHost,
    store: &Store,
    cellar: &Cellar,
    spec: &NestSpec,
    files: &NestFiles,
    mirror: &MirrorConfig,
) -> Result<Hydrated> {
    let dataset = spec.data_identity(files)?;
    let base = mirror.base.as_str();
    let (mut snapshot, mut published, _) = read_snapshot(host, base, &dataset).await?;
    check_snapshot(&snapshot, &dataset, spec.chain.chain_id, spec.start)?;
    // RFC-0081 S1's hybrid open: only the tables the filters' events decode into, each segment whole,
    // since its hash covers the whole file. Which ones depends on no filter's value.
    if let Some(keep) = &spec.hydrate {
        published.tables.retain(|t, _| keep.contains(t));
        snapshot.tails.retain(|t, _| keep.contains(t));
    }
    let indexed_from = snapshot.indexed_from;
    let (catalogue, wanted, complete_through) = match &mirror.postcard {
        None => {
            let (catalogue, wanted) = local_catalogue(published, &snapshot)?;
            (catalogue, wanted, snapshot.complete_through)
        }
        Some(pin) => {
            let schema =
                need_text(host, &url(base, &dataset, "schema.json"), "schema.json").await?;
            let cut = cut_catalogue(&published, &pin.cuts);
            let digest = postcard_digest(schema.as_bytes(), &cut);
            if digest != pin.digest {
                return Err(Unverified(format!(
                    "the mirror's catalogue, cut where the postcard says, and its schema.json hash \
                     to {digest}, and the postcard says {}: the mirror does not hold what the \
                     postcard pins",
                    pin.digest
                ))
                .into());
            }
            // The cut is checked; the tails are left out, as a postcard reads none.
            let mut bare = snapshot;
            bare.tails.clear();
            let through = pin.cuts.values().copied().max().unwrap_or(0);
            bare.complete_through = through;
            let (catalogue, wanted) = local_catalogue(cut, &bare)?;
            (catalogue, wanted, through)
        }
    };

    store.set_meta(SEED_PENDING_KEY, &dataset)?;
    if !cellar.manifest().tables.is_empty() {
        cellar.reset_packed()?;
    }
    let placed = download(host, cellar, base, &dataset, &wanted).await?;
    let bytes = placed.iter().map(|(_, _, len)| len).sum();
    cellar.install_packed(&catalogue, &placed)?;
    let through = complete_through.to_string();
    let from = indexed_from.to_string();
    store.set_metas(
        &[
            (START_BLOCK_KEY, &from),
            (SEALED_THROUGH_KEY, &through),
            (LAST_BLOCK_KEY, &through),
        ],
        &[SEED_PENDING_KEY],
    )?;
    Ok(Hydrated {
        dataset,
        segments: placed.len(),
        bytes,
        indexed_from,
        complete_through,
        pinned: mirror.postcard.is_some(),
    })
}

/// Every wanted file appended to the pack and read back against its hash: where each one went.
async fn download(
    host: &dyn MirrorHost,
    cellar: &Cellar,
    base: &str,
    dataset: &str,
    wanted: &[Wanted],
) -> Result<Vec<(String, u64, u64)>> {
    let fetches = wanted.iter().map(|w| async move {
        let at = host
            .append(&url(base, dataset, &w.key), MAX_SEGMENT_BYTES)
            .await?
            .ok_or_else(|| anyhow!("the mirror's catalogue names {} but it is not there", w.key))?;
        Ok::<_, anyhow::Error>((w, at))
    });
    let mut results = futures::stream::iter(fetches).buffered(PARALLELISM);
    let mut placed = Vec::with_capacity(wanted.len());
    while let Some(r) = results.next().await {
        let (w, (at, len)) = r?;
        let got = cellar.pack_hash(at, len)?;
        if got != w.segment.hash {
            let msg = if w.segment.provisional {
                format!(
                    "{} is not the tail the seed snapshot names. The publisher rewrites tails as it \
                     seals; open the nest again, and it hydrates afresh.",
                    w.key
                )
            } else {
                format!("{} does not hash to its content address", w.key)
            };
            return Err(Unverified(msg).into());
        }
        placed.push((w.segment.file.clone(), at, len));
    }
    Ok(placed)
}
