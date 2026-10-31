//! A follower on another host (RFC-0076 FR17): everything a follower beside the writer reads from
//! the writer's directory, it takes from the writer's mirror and registry instead, each file checked
//! against the hash that names it before anything reads it.
//!
//! The follower keeps a directory of its own, laid out as a runtime's: each nest the writer mounts
//! is installed at `data/<nid>` from the registry by its NID, verified as `nest load` verifies; each
//! segment a catalogue names lands at `segments/<hash>.parquet`, where [`crate::seal::segment_path`]
//! already looks for a runtime's shared segments; each dataset's offchain snapshots land under its
//! `offchain/`. Nothing in it is the writer's, and nothing is shared with the writer.
//!
//! **A segment is only ever renamed into place once its bytes hash to its name**, so a file present
//! there has been verified, and a delta whose catalogue names a file not present is not applied.

use crate::publish::Mirror;
use crate::seal::{Manifest, Segment};
use anyhow::{bail, Context, Result};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Under a dataset's mirror prefix: provisional segments, which the published catalogue leaves out
/// and only followers fetch. Not named `.parquet`, so no reader's glob over the prefix counts one.
pub const PROVISIONAL_DIR: &str = "_provisional";
/// Under a dataset's mirror prefix: its offchain snapshots and their catalogue.
pub const OFFCHAIN_DIR: &str = "_offchain";
/// The most one fetched segment may be.
const SEGMENT_CAP: u64 = 16 << 30;
/// The most an offchain catalogue may be.
const OFFCHAIN_CATALOGUE_CAP: u64 = 64 << 20;

/// The mirror key a provisional segment is uploaded under.
pub fn provisional_key(hash: &str) -> String {
    format!("{PROVISIONAL_DIR}/{hash}.seg")
}

pub fn offchain_key(file: &str) -> String {
    format!("{OFFCHAIN_DIR}/{file}")
}

pub const OFFCHAIN_CATALOGUE: &str = "_offchain/manifest.json";
/// Under a dataset's mirror prefix: written by a publisher with `followers = true`, so a follower can
/// tell a mirror without its files from one that has not uploaded them yet.
pub const FOLLOWERS_MARKER: &str = "_followers.json";

/// Where a follower's sources are.
pub struct Remote {
    mirror: Box<dyn Mirror>,
    registry: String,
    /// The per-cursor budget, which a follower with no `mounts.toml` of the writer's is given.
    pub max_rss_mb: u64,
    /// How long a fetched segment no copy names is kept before it is deleted from the cache: the
    /// same grace a writer keeps a folded segment for its followers.
    pub cache_grace: std::time::Duration,
    /// Cached files whose bytes have been checked against their names since the last reverify.
    verified: Mutex<std::collections::HashSet<PathBuf>>,
    refused: Mutex<Vec<String>>,
}

/// A segment a catalogue names that the mirror did not have, or that did not verify.
#[derive(Debug)]
pub struct NotFetched {
    pub missing: Vec<String>,
    pub refused: Vec<String>,
}

impl std::fmt::Display for NotFetched {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.missing.is_empty() {
            write!(f, "the mirror does not have {}", self.missing.join(", "))?;
        }
        if !self.refused.is_empty() {
            if !self.missing.is_empty() {
                f.write_str("; ")?;
            }
            write!(
                f,
                "{} did not hash to what the catalogue names, and were refused",
                self.refused.join(", ")
            )?;
        }
        Ok(())
    }
}

impl std::error::Error for NotFetched {}

/// A dataset name the writer sent is a plain `data/<name>`, nothing that could reach outside the
/// follower's directory.
pub fn checked_dataset(dataset: &str) -> Result<()> {
    match dataset.split_once('/') {
        Some((crate::runtime::DATA_DIR, name)) => crate::runtime::safe_segment(name, "dataset"),
        _ => bail!("the writer names a dataset '{dataset}' that is not data/<name>"),
    }
}

/// Every segment `catalogue` names that `root`'s segment store does not hold, with its table. A
/// cached file not checked since the last [`Remote::reverify`] is hashed first, and one that does
/// not hash to its name is removed and counted missing. Blocking: it may read whole files.
pub fn missing(remote: &Remote, root: &Path, catalogue: &str) -> Result<Vec<(String, Segment)>> {
    let m: Manifest = serde_json::from_str(catalogue).context("a corrupt catalogue on the feed")?;
    let store = root.join(crate::seal::SEGMENTS_DIR);
    let mut out: Vec<(String, Segment)> = Vec::new();
    for (table, segments) in m.tables {
        for s in segments {
            if !is_hash(&s.hash) {
                bail!(
                    "the catalogue names a segment hash '{}' that is not one",
                    s.hash
                );
            }
            if !remote.holds(&store.join(format!("{}.parquet", s.hash)), &s.hash)?
                && !out.iter().any(|(_, o)| o.hash == s.hash)
            {
                out.push((table.clone(), s));
            }
        }
    }
    Ok(out)
}

fn is_hash(h: &str) -> bool {
    h.len() == 64 && h.bytes().all(|b| b.is_ascii_hexdigit())
}

/// The sha256 of the file at `path`, read a block at a time.
fn file_hash(path: &Path) -> std::io::Result<String> {
    use sha2::Digest;
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    let mut hasher = sha2::Sha256::new();
    let mut buf = vec![0u8; 1 << 16];
    loop {
        match file.read(&mut buf)? {
            0 => return Ok(hex::encode(hasher.finalize())),
            n => hasher.update(&buf[..n]),
        }
    }
}

/// Beside the cached nests: what the registry installed of each one's offchain snapshots.
const VERIFIED_DIR: &str = ".installed";

fn is_offchain(path: &str) -> bool {
    path.split('/').next() == Some(crate::offchain::DIR)
}

/// The NID the cached nest at `dir` computes to, by the identity code the registry check uses, with
/// its offchain snapshots as the registry installed them: the follower brings those up to the
/// writer's from the mirror, each checked against its hash there.
fn cached_nid(dir: &Path, installed: &Path) -> Result<String> {
    let mut manifest = crate::blob::build_manifest(dir, None)?;
    let offchain: Vec<nuthatch_identity::FileEntry> = match std::fs::read(installed) {
        Ok(raw) => serde_json::from_slice(&raw)
            .with_context(|| format!("a corrupt {}", installed.display()))?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(manifest.nid());
        }
        Err(e) => return Err(e).with_context(|| format!("reading {}", installed.display())),
    };
    manifest.files.retain(|f| !is_offchain(&f.path));
    manifest.files.extend(offchain);
    manifest.files.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(manifest.nid())
}

/// The most cache files [`Remote::refused`] remembers.
const REFUSED_KEPT: usize = 16;

#[derive(Debug)]
struct Refused(String);

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} did not hash to its name", self.0)
    }
}

impl std::error::Error for Refused {}

impl Remote {
    pub(crate) fn mirror(&self) -> &dyn Mirror {
        self.mirror.as_ref()
    }

    pub fn open(mirror: &str, registry: &str, max_rss_mb: u64) -> Result<Remote> {
        Ok(Remote {
            mirror: crate::publish::open_mirror(mirror)
                .with_context(|| "opening the mirror".to_string())?,
            registry: registry.to_string(),
            max_rss_mb,
            cache_grace: crate::seal::FOLLOWER_GRACE,
            verified: Default::default(),
            refused: Default::default(),
        })
    }

    /// Whether `path` holds the bytes `hash` names. A file is hashed the first time it is asked
    /// about after [`Remote::reverify`], since anything may have written to the cache meanwhile, and
    /// one that does not hash to its name is removed, reported, and taken as absent.
    pub fn holds(&self, path: &Path, hash: &str) -> Result<bool> {
        let lock = |m: &'_ Mutex<std::collections::HashSet<PathBuf>>| {
            m.lock().unwrap_or_else(|e| e.into_inner()).contains(path)
        };
        if lock(&self.verified) {
            return Ok(true);
        }
        let got = match file_hash(path) {
            Ok(got) => got,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(e).with_context(|| format!("hashing {}", path.display())),
        };
        if got.eq_ignore_ascii_case(hash) {
            self.verified
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(path.to_path_buf());
            return Ok(true);
        }
        tracing::warn!(
            "{}: the cached file does not hash to its name; removed, and fetched again",
            path.display()
        );
        crate::feed_metrics::metrics().mirror_refused();
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e).with_context(|| format!("removing {}", path.display())),
        }
        let mut refused = self.refused.lock().unwrap_or_else(|e| e.into_inner());
        refused.push(path.display().to_string());
        let over = refused.len().saturating_sub(REFUSED_KEPT);
        refused.drain(..over);
        Ok(false)
    }

    /// Check every cached file again on its next use: called as a follower takes fresh snapshots.
    pub fn reverify(&self) {
        self.verified
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }

    /// Cached files found not to hash to their names and removed, most recent last.
    pub fn refused(&self) -> Vec<String> {
        self.refused
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Write `key` to a temporary file beside `dest`, and rename it there only if its bytes hash
    /// to `hash`. `Ok(false)` when the mirror does not have it.
    async fn fetch_verified(&self, key: &str, dest: &Path, hash: &str) -> Result<bool> {
        let dir = dest.parent().context("a destination with no directory")?;
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        let tmp = tempfile::Builder::new()
            .prefix(".incoming-")
            .tempfile_in(dir)
            .with_context(|| format!("a temporary file in {}", dir.display()))?;
        let Some(got) = self.mirror.get_file(key, tmp.path(), SEGMENT_CAP).await? else {
            return Ok(false);
        };
        if !got.eq_ignore_ascii_case(hash) {
            crate::feed_metrics::metrics().mirror_refused();
            return Err(Refused(key.to_string()).into());
        }
        let len = tmp.as_file().metadata().map(|m| m.len()).unwrap_or(0);
        tmp.persist(dest)
            .with_context(|| format!("moving a verified file to {}", dest.display()))?;
        self.verified
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(dest.to_path_buf());
        crate::feed_metrics::metrics().mirror_fetched(len);
        Ok(true)
    }

    /// Whether the publisher of `prefix` publishes for followers (`followers = true`).
    pub async fn publishes_for_followers(&self, prefix: &str) -> Result<bool> {
        Ok(self
            .mirror
            .head(&format!("{prefix}/{FOLLOWERS_MARKER}"))
            .await?
            .is_some())
    }

    /// Install the nest `nid` at `root/dataset` from the registry, verified, unless it is there.
    /// A nest already in the cache is reused only if it still computes to `nid`; one that does not,
    /// edited or left by another release, is reported and replaced by a verified install.
    pub async fn install(&self, root: &Path, dataset: &str, nid: &str) -> Result<()> {
        checked_dataset(dataset)?;
        let target = root.join(dataset);
        let installed = root
            .join(VERIFIED_DIR)
            .join(format!("{}.json", dataset.trim_start_matches("data/")));
        if target.join(crate::config::CONFIG_FILE).exists() {
            let why = match cached_nid(&target, &installed) {
                Ok(found) if found == nid => return Ok(()),
                Ok(found) => format!("it computes to NID {found}"),
                Err(e) => format!("its identity cannot be worked out: {e:#}"),
            };
            tracing::warn!(
                "{}: the cached nest is not {nid}, {why}; installing it again from the registry",
                target.display()
            );
            self.refuse(format!("{dataset}: the cached nest is not {nid}: {why}"));
            let aside = tempfile::Builder::new()
                .prefix(".stale-")
                .tempdir_in(root.join(crate::runtime::DATA_DIR))
                .context("a place to move the stale nest aside")?;
            std::fs::rename(&target, aside.path().join("nest"))
                .with_context(|| format!("moving {} aside", target.display()))?;
        }
        crate::distribution::install_by_nid(&self.registry, nid, &target)
            .await
            .with_context(|| format!("installing nest {nid} from the registry"))?;
        // What the registry installed, so that offchain snapshots later brought up to the writer's
        // do not read as an edit: they are checked against the mirror's hashes instead.
        let manifest = crate::blob::build_manifest(&target, None)?;
        if manifest.nid() != nid {
            bail!(
                "the registry's nest installed as {}, not {nid}",
                manifest.nid()
            );
        }
        let offchain: Vec<_> = manifest
            .files
            .into_iter()
            .filter(|f| is_offchain(&f.path))
            .collect();
        std::fs::create_dir_all(root.join(VERIFIED_DIR))?;
        std::fs::write(&installed, serde_json::to_vec(&offchain)?)
            .with_context(|| format!("writing {}", installed.display()))?;
        Ok(())
    }

    fn refuse(&self, what: String) {
        let mut refused = self.refused.lock().unwrap_or_else(|e| e.into_inner());
        refused.push(what);
        let over = refused.len().saturating_sub(REFUSED_KEPT);
        refused.drain(..over);
    }

    /// Fetch every one of `segments` into `root`'s segment store, each checked against its hash. A
    /// provisional segment is looked for where the publisher puts provisional ones and then where a
    /// final one goes, a final one the other way round: one may have aged out of being provisional.
    pub async fn fetch(
        &self,
        root: &Path,
        prefix: &str,
        segments: &[(String, Segment)],
    ) -> Result<()> {
        let store = root.join(crate::seal::SEGMENTS_DIR);
        let mut failed = NotFetched {
            missing: Vec::new(),
            refused: Vec::new(),
        };
        for (table, s) in segments {
            let dest = store.join(format!("{}.parquet", s.hash));
            if self.holds(&dest, &s.hash)? {
                continue;
            }
            let final_key = format!("{prefix}/{}", crate::publish::parquet_key(table, &s.hash));
            let provisional = format!("{prefix}/{}", provisional_key(&s.hash));
            let keys = if s.provisional {
                [provisional, final_key]
            } else {
                [final_key, provisional]
            };
            let mut got = false;
            for key in &keys {
                match self.fetch_verified(key, &dest, &s.hash).await {
                    Ok(true) => {
                        got = true;
                        break;
                    }
                    Ok(false) => {}
                    Err(e) if e.is::<Refused>() => {
                        failed.refused.push(s.file.clone());
                        got = true;
                        break;
                    }
                    Err(e) => return Err(e),
                }
            }
            if !got {
                crate::feed_metrics::metrics().mirror_missing();
                failed.missing.push(s.file.clone());
            }
        }
        if failed.missing.is_empty() && failed.refused.is_empty() {
            return Ok(());
        }
        Err(failed.into())
    }

    /// Bring `root/dataset`'s offchain snapshots up to the mirror's: every file the mirror's
    /// offchain catalogue names, checked against its hash, then the catalogue itself. Whether it
    /// changed anything.
    pub async fn sync_offchain(&self, root: &Path, dataset: &str, prefix: &str) -> Result<bool> {
        checked_dataset(dataset)?;
        let dir = root.join(dataset);
        let Some(raw) = self
            .mirror
            .get(&format!("{prefix}/{OFFCHAIN_CATALOGUE}"))
            .await?
        else {
            return Ok(false);
        };
        if raw.len() as u64 > OFFCHAIN_CATALOGUE_CAP {
            bail!("the mirror's offchain catalogue is larger than {OFFCHAIN_CATALOGUE_CAP} bytes");
        }
        let path = crate::offchain::catalogue_path(&dir);
        let same = std::fs::read(&path).ok().as_deref() == Some(raw.as_slice());
        let mut changed = !same;
        let catalogue: crate::offchain::Catalogue =
            serde_json::from_slice(&raw).context("a corrupt offchain catalogue on the mirror")?;
        for snapshot in catalogue.tables.values().flatten() {
            let stem = snapshot.file.strip_suffix(".parquet").unwrap_or("");
            crate::runtime::safe_segment(stem, "offchain snapshot file")?;
            if !is_hash(&snapshot.hash) {
                bail!("offchain snapshot {} has no hash", snapshot.file);
            }
            let dest = crate::offchain::segment_path(&dir, snapshot);
            if self.holds(&dest, &snapshot.hash)? {
                continue;
            }
            changed = true;
            let key = format!("{prefix}/{}", offchain_key(&snapshot.file));
            if !self.fetch_verified(&key, &dest, &snapshot.hash).await? {
                bail!(
                    "the mirror's offchain catalogue names {}, which it does not have",
                    key
                );
            }
        }
        if same {
            return Ok(changed);
        }
        let parent = path
            .parent()
            .context("an offchain catalogue with no directory")?;
        std::fs::create_dir_all(parent)?;
        let tmp = tempfile::NamedTempFile::new_in(parent)?;
        std::fs::write(tmp.path(), &raw)?;
        tmp.persist(&path)
            .with_context(|| format!("writing {}", path.display()))?;
        // Read back through the loader, which refuses what an offchain catalogue may not name.
        crate::offchain::load(&dir)?;
        Ok(true)
    }
}
