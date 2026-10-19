//! `nuthatch prune` - reclaim the disk of datasets nothing mounts any more (RFC-0032 §5, slice 4).
//!
//! Unmounting a nest deletes its **mount record**, never its data. That is deliberate and it is the
//! whole point of the design: re-backfilling is precisely the cost identity-keyed storage exists to
//! avoid, so an accidental unmount must not trigger one. Unmount/remount is free.
//!
//! The price is disk held by nothing. This is the command that reclaims it - explicitly, on an
//! operator's schedule, never automatically. A background collector would eventually delete an
//! operator's history on a timer they had forgotten about.
//!
//! **Collectability is derived, not stored.** A dataset directory with no mount record naming its
//! identity is collectable; that is the whole rule. There is no marker file and no "collectable
//! since" timestamp, for the same reason the refcount is a count over the table rather than a
//! number kept beside it: anything stored separately eventually disagrees with the thing it
//! describes, and here the disagreement deletes data.
//!
//! **Listing is the default and deleting needs `--yes`**, which is the opposite of `migrate
//! --dry-run`. The asymmetry is intentional: migrating moves data and is recoverable, pruning
//! removes indexed history and is not. The dangerous direction should be the one you have to ask
//! for.

use crate::runtime::{MountTable, DATA_DIR};
use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

/// A dataset on disk that no mount refers to.
#[derive(Debug, PartialEq, Eq)]
pub struct Collectable {
    /// The nest identity the directory is named for.
    pub nid: String,
    pub dir: PathBuf,
    /// Bytes it is holding, so the report says what pruning would actually buy.
    pub bytes: u64,
}

/// Total bytes under `dir`, following no symlinks.
fn size_of(dir: &Path) -> u64 {
    let mut total = 0;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in rd.flatten() {
            match e.file_type() {
                Ok(t) if t.is_dir() => stack.push(e.path()),
                Ok(t) if t.is_file() => total += e.metadata().map(|m| m.len()).unwrap_or(0),
                _ => {} // a symlink is not ours to size, and never ours to follow
            }
        }
    }
    total
}

/// Every dataset under `data/` that no mount record refers to (RFC-0032 §5).
///
/// Reads the mount table first and fails loudly if it cannot: a runtime whose `mounts.toml` is
/// unreadable has *no* known mounts, and treating that as "nothing is mounted" would make every
/// dataset collectable at once. The one failure this command must never have.
pub fn collectable(dir: &Path) -> Result<Vec<Collectable>> {
    let mounts = MountTable::load(dir)
        .with_context(|| format!("reading the mount table of {}", dir.display()))?;
    let mounted: std::collections::HashSet<&str> =
        mounts.mounts.iter().map(|m| m.nid.as_str()).collect();
    let mounting = mounting(dir)?;

    let data = dir.join(DATA_DIR);
    let Ok(entries) = std::fs::read_dir(&data) else {
        return Ok(Vec::new()); // nothing migrated yet - nothing to collect
    };

    let mut out = Vec::new();
    for e in entries.flatten() {
        if !e.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let nid = e.file_name().to_string_lossy().to_string();
        // Only ever consider directories that *look* like datasets. A stray file or a directory an
        // operator dropped in here by hand is not ours to delete.
        if !is_nid(&nid) {
            continue;
        }
        if mounted.contains(nid.as_str()) || mounting.contains(&nid) {
            continue;
        }
        let path = e.path();
        let bytes = size_of(&path);
        out.push(Collectable {
            nid,
            dir: path,
            bytes,
        });
    }
    out.sort_by(|a, b| a.nid.cmp(&b.nid));
    Ok(out)
}

fn is_nid(name: &str) -> bool {
    name.len() == 64 && name.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Every dataset directory under `data/`, mounted or not, and every adoption staging beside one,
/// keyed by the NID each belongs to. An unmounted dataset is kept so a remount is free, so the
/// segments its manifest names are as live as a mounted one's (#2128).
fn dataset_dirs(dir: &Path) -> Result<Vec<(String, PathBuf)>> {
    let data = dir.join(DATA_DIR);
    let entries = match std::fs::read_dir(&data) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e).with_context(|| format!("listing {}", data.display())),
    };
    let mut out = Vec::new();
    for e in entries {
        let e = e.with_context(|| format!("listing {}", data.display()))?;
        if !e.file_type().is_ok_and(|t| t.is_dir()) {
            continue;
        }
        let name = e.file_name().to_string_lossy().to_string();
        let nid = name.strip_suffix(".adopting").unwrap_or(&name);
        if is_nid(nid) {
            out.push((nid.to_string(), e.path()));
        }
    }
    out.sort();
    Ok(out)
}

/// The datasets an unfinished mount job names. A job installs `data/<nid>` before the join records
/// it, and a restart resumes the job, so these are not collectable either way (#2133).
fn mounting(dir: &Path) -> Result<std::collections::HashSet<String>> {
    let file = dir.join(crate::mount_jobs::JOBS_FILE);
    let jobs: Vec<crate::mount_jobs::MountJob> = match std::fs::read(&file) {
        Ok(raw) => serde_json::from_slice(&raw)
            .with_context(|| format!("reading {}; nothing was collected", file.display()))?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(e) => return Err(e).with_context(|| format!("reading {}", file.display())),
    };
    Ok(jobs
        .into_iter()
        .filter(|j| !j.phase.finished())
        .filter_map(|j| j.nid)
        .collect())
}

/// Every segment hash any surviving dataset still references (RFC-0033 §11a).
///
/// **Derived from the manifests, never stored.** A stored count drifts, and here drift deletes bytes
/// another dataset is reading - the one data-loss failure mode in the shared store, and the same
/// reasoning as RFC-0032 §5's dataset refcount.
fn referenced_segments(surviving: &[PathBuf]) -> Result<std::collections::HashSet<String>> {
    let mut out = std::collections::HashSet::new();
    for dataset in surviving {
        // Skipping a manifest that will not read made every segment it names an orphan.
        let manifest = crate::seal::load_manifest(dataset).with_context(|| {
            format!(
                "reading the manifest of {}; nothing was collected",
                dataset.display()
            )
        })?;
        for segs in manifest.tables.values() {
            for s in segs {
                out.insert(s.hash.clone());
            }
        }
    }
    Ok(out)
}

/// Whether a running process holds this dataset's store, which redb locks exclusively. A placeholder
/// or corrupt store is not held and answers false.
fn store_is_held(dataset: &Path) -> bool {
    let db = dataset.join(crate::config::DB_FILE);
    db.exists()
        && crate::store::Store::open_existing(&db).is_err_and(|e| {
            e.chain().any(|c| {
                matches!(
                    c.downcast_ref::<redb::DatabaseError>(),
                    Some(redb::DatabaseError::DatabaseAlreadyOpen)
                )
            })
        })
}

/// Shared segments no surviving dataset references, with their sizes.
fn orphan_segments(dir: &Path) -> Result<Vec<(PathBuf, u64)>> {
    let surviving: Vec<PathBuf> = dataset_dirs(dir)?.into_iter().map(|(_, p)| p).collect();
    let referenced = referenced_segments(&surviving)?;
    let Ok(entries) = std::fs::read_dir(dir.join(crate::seal::SEGMENTS_DIR)) else {
        return Ok(Vec::new());
    };
    Ok(entries
        .flatten()
        .filter(|e| e.path().extension().is_some_and(|x| x == "parquet"))
        .filter_map(|e| {
            let hash = e.path().file_stem()?.to_string_lossy().to_string();
            if referenced.contains(&hash) {
                return None;
            }
            Some((e.path(), e.metadata().ok()?.len()))
        })
        .collect())
}

fn human(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut v = bytes as f64;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    if u == 0 {
        format!("{bytes} B")
    } else {
        format!("{v:.1} {}", UNITS[u])
    }
}

/// `nuthatch prune <dir> [--yes]`: report collectable datasets, and with `--yes` remove them.
pub fn run(dir: &Path, yes: bool) -> Result<()> {
    let found = collectable(dir)?;
    if found.is_empty() {
        return collect_leftover_segments(dir, yes);
    }

    let total: u64 = found.iter().map(|c| c.bytes).sum();
    println!(
        "{} dataset(s) in {} are mounted by nothing, holding {}:\n",
        found.len(),
        dir.display(),
        human(total)
    );
    for c in &found {
        println!("  data/{}  {}", &c.nid[..12], human(c.bytes));
    }
    println!();

    if !yes {
        println!(
            "Nothing was deleted. This is indexed history: re-creating it means a full backfill, \
             which is the cost identity-keyed storage exists to avoid.\n\
             Re-mounting any of these is free while the data is still here.\n\n\
             Run `nuthatch prune --dir {} --yes` to remove them.",
            dir.display()
        );
        return Ok(());
    }

    let _running = refuse_while_running(dir)?;

    // **Manifest first, bytes second** (RFC-0033 §11a). Removing the dataset - and with it the
    // manifest referencing its segments - before collecting orphans means an interrupted prune leaves
    // unreferenced bytes (recoverable, just disk) rather than dangling references (not).
    for c in &found {
        std::fs::remove_dir_all(&c.dir).with_context(|| format!("removing {}", c.dir.display()))?;
        // An adoption killed part-way leaves a staging sibling. It is not a dataset, so `collectable`
        // never lists it, and it would otherwise outlive the only thing that clears it.
        let _ = std::fs::remove_dir_all(crate::runtime::adopt_staging(&c.dir));
        println!("Removed data/{}", &c.nid[..12]);
    }
    crate::crash::point("prune:after-datasets");

    let orphans = orphan_segments(dir)?;
    let mut freed_segments = 0u64;
    for (path, size) in &orphans {
        if crate::seal::remove_segment_if_unheld(path)? {
            freed_segments += size;
        }
    }
    if !orphans.is_empty() {
        println!(
            "Removed {} shared segment(s) no dataset referenced ({}).",
            orphans.len(),
            human(freed_segments)
        );
    }

    println!("\nFreed {}.", human(total + freed_segments));
    Ok(())
}

/// A query in a running runtime can have planned against a shared segment a fold has since
/// replaced, and its lease does not reach this process. The returned lock keeps one from starting.
fn refuse_while_running(dir: &Path) -> Result<std::fs::File> {
    let lock = crate::runtime::lock_runtime_dir(dir, true)?;
    // Every dataset, not only recorded ones: a live mount opens its store before it is recorded (#2133).
    if let Some((nid, _)) = dataset_dirs(dir)?
        .into_iter()
        .find(|(nid, path)| *path == MountTable::data_dir(dir, nid) && store_is_held(path))
    {
        anyhow::bail!(
            "data/{} is open in a running runtime: removing shared segments now could delete one a \
             query there is about to read. Stop the runtime, then prune again. Nothing was deleted.",
            &nid[..nid.len().min(12)]
        );
    }
    Ok(lock)
}

/// Why nothing is collectable, given that nothing is: a dataset an unfinished job names is not
/// mounted yet (#2249).
fn settled(dir: &Path) -> Result<String> {
    let recorded = MountTable::load(dir)?;
    let mid_mount = mounting(dir)?
        .iter()
        .filter(|nid| !recorded.mounts.iter().any(|m| m.nid == **nid))
        .filter(|nid| MountTable::data_dir(dir, nid).is_dir())
        .count();
    Ok(match mid_mount {
        0 => "every dataset is mounted".to_string(),
        n => format!("every dataset is mounted or being mounted ({n} mid-mount)"),
    })
}

/// With no dataset left to remove, the segments no mounted dataset references: what a prune killed
/// between removing its datasets and its orphan pass leaves behind, and what a fold replaces (#1717).
fn collect_leftover_segments(dir: &Path, yes: bool) -> Result<()> {
    let orphans = orphan_segments(dir)?;
    let settled = settled(dir)?;
    if orphans.is_empty() {
        println!("Nothing to prune in {} - {settled}.", dir.display());
        return Ok(());
    }
    let bytes: u64 = orphans.iter().map(|(_, size)| size).sum();
    println!(
        "In {}, {settled}, and {} shared segment(s) no dataset references hold {}.",
        dir.display(),
        orphans.len(),
        human(bytes)
    );
    if !yes {
        println!(
            "Nothing was deleted. Run `nuthatch prune --dir {} --yes` to remove them.",
            dir.display()
        );
        return Ok(());
    }
    let _running = refuse_while_running(dir)?;
    for (path, _) in &orphans {
        std::fs::remove_file(path).with_context(|| format!("removing {}", path.display()))?;
    }
    println!("Removed them. Freed {}.", human(bytes));
    Ok(())
}

/// What reclaiming one dataset did (#1547).
#[derive(Debug, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum Reclaim {
    Reclaimed {
        nid: String,
        bytes: u64,
    },
    /// Another mount still names this dataset, so nothing was touched.
    Kept {
        nid: String,
        mounted_by: Vec<String>,
    },
    /// No dataset by that identity is on disk.
    Absent {
        nid: String,
    },
}

/// Reclaim one unmounted dataset while its runtime is running (#1547).
///
/// Narrower than [`run`], which refuses while any store is open: an orphan left by a live fold may
/// be one a running query is about to read. The segments removed here are only those this dataset's
/// own manifest names and no surviving manifest does, and nothing can be reading those once its
/// mount is gone.
pub fn reclaim(dir: &Path, nid: &str) -> Result<Reclaim> {
    let mounts = MountTable::load(dir)
        .with_context(|| format!("reading the mount table of {}", dir.display()))?;
    let mounted_by: Vec<String> = mounts
        .mounts
        .iter()
        .filter(|m| m.nid == nid)
        .map(|m| format!("{}/{}", m.tenant, m.alias))
        .collect();
    if !mounted_by.is_empty() {
        return Ok(Reclaim::Kept {
            nid: nid.to_string(),
            mounted_by,
        });
    }
    let data = MountTable::data_dir(dir, nid);
    if !data.is_dir() {
        return Ok(Reclaim::Absent {
            nid: nid.to_string(),
        });
    }
    if store_is_held(&data) {
        anyhow::bail!(
            "data/{} is still open in this process; nothing was deleted",
            &nid[..nid.len().min(12)]
        );
    }

    let surviving: Vec<PathBuf> = dataset_dirs(dir)?
        .into_iter()
        .filter(|(other, _)| other != nid)
        .map(|(_, p)| p)
        .collect();
    let referenced = referenced_segments(&surviving)?;
    let own = crate::seal::load_manifest(&data)
        .with_context(|| format!("reading the manifest of data/{}", &nid[..nid.len().min(12)]))?;
    let segments = dir.join(crate::seal::SEGMENTS_DIR);
    let exclusive: std::collections::BTreeMap<PathBuf, u64> = own
        .tables
        .values()
        .flatten()
        .filter(|s| !referenced.contains(&s.hash))
        .map(|s| segments.join(format!("{}.parquet", s.hash)))
        // Named by a seal whose manifest is not installed yet (#1644).
        .filter(|p| !crate::seal::segment_held(p))
        .filter_map(|p| {
            let len = std::fs::metadata(&p).ok()?.len();
            Some((p, len))
        })
        .collect();

    // Manifest first, bytes second, as `run` does: an interrupted reclaim leaves unreferenced
    // segments for `prune`, never a manifest naming segments that are gone.
    let mut bytes = size_of(&data);
    std::fs::remove_dir_all(&data).with_context(|| format!("removing {}", data.display()))?;
    let _ = std::fs::remove_dir_all(crate::runtime::adopt_staging(&data));
    for (path, len) in &exclusive {
        if crate::seal::remove_segment_if_unheld(path)? {
            bytes += len;
        }
    }
    Ok(Reclaim::Reclaimed {
        nid: nid.to_string(),
        bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{Mount, MOUNTS_FILE};

    /// A mounts with `mounted` recorded and `orphans` sitting in `data/` with no record.
    fn fixture(dir: &Path, mounted: &[&str], orphans: &[&str]) {
        let mut mounts =
            "[runtime]\nname = \"r\"\nchain = \"arbitrum-one\"\nchain_id = 42161\nrpc_urls = []\n"
                .to_string();
        for (i, nid) in mounted.iter().enumerate() {
            mounts.push_str(&format!(
                "\n[[mounts]]\nalias = \"n{i}\"\nnid = \"{nid}\"\n"
            ));
        }
        std::fs::write(dir.join(MOUNTS_FILE), mounts).unwrap();
        for nid in mounted.iter().chain(orphans) {
            let d = dir.join(DATA_DIR).join(nid);
            std::fs::create_dir_all(&d).unwrap();
            std::fs::write(d.join("nuthatch.redb"), vec![0u8; 1024]).unwrap();
        }
    }

    fn nid(seed: &str) -> String {
        seed.repeat(64 / seed.len())
    }

    #[test]
    fn only_unmounted_datasets_are_collectable() {
        let d = tempfile::tempdir().unwrap();
        let (a, b) = (nid("aa"), nid("bb"));
        fixture(d.path(), &[&a], &[&b]);

        let found = collectable(d.path()).unwrap();
        assert_eq!(found.len(), 1, "expected only the orphan, got {found:?}");
        assert_eq!(found[0].nid, b);
        assert_eq!(found[0].bytes, 1024);
    }

    /// Two mounts sharing one dataset, one of them unmounted: the refcount is still 1, so the data
    /// must NOT be collectable. Getting this wrong deletes a tenant's data because a *different*
    /// tenant unmounted.
    #[test]
    fn a_dataset_another_mount_still_wants_is_not_collectable() {
        let d = tempfile::tempdir().unwrap();
        let shared = nid("cc");
        std::fs::write(
            d.path().join(MOUNTS_FILE),
            format!(
                "[runtime]\nname = \"r\"\nchain = \"arbitrum-one\"\nchain_id = 42161\nrpc_urls = []\n\n\
                 [[mounts]]\ntenant = \"globex\"\nalias = \"usdc\"\nnid = \"{shared}\"\n"
            ),
        )
        .unwrap();
        let dir = d.path().join(DATA_DIR).join(&shared);
        std::fs::create_dir_all(&dir).unwrap();

        assert!(
            collectable(d.path()).unwrap().is_empty(),
            "a dataset one tenant still mounts was offered up for deletion"
        );
    }

    /// Listing must never delete. This is the guard between an operator typing `prune` to see what
    /// is there and losing indexed history.
    #[test]
    fn listing_deletes_nothing_and_yes_deletes_everything_collectable() {
        let d = tempfile::tempdir().unwrap();
        let (a, b) = (nid("aa"), nid("bb"));
        fixture(d.path(), &[&a], &[&b]);
        let orphan = d.path().join(DATA_DIR).join(&b);

        run(d.path(), false).unwrap();
        assert!(orphan.is_dir(), "listing deleted a dataset");

        run(d.path(), true).unwrap();
        assert!(
            !orphan.exists(),
            "--yes did not remove the collectable dataset"
        );
        assert!(
            d.path().join(DATA_DIR).join(&a).is_dir(),
            "prune removed a MOUNTED dataset"
        );
    }

    /// An adoption killed part-way leaves `data/<nid>.adopting/` - a full copy of a dataset's derived
    /// state. `adopt_dataset` clears it on the dataset's next start, but a dataset that is unmounted
    /// and pruned never has one, so the scratch would sit there at dataset size forever.
    #[test]
    fn a_pruned_dataset_takes_its_adoption_staging_with_it() {
        let d = tempfile::tempdir().unwrap();
        let (a, b) = (nid("aa"), nid("bb"));
        fixture(d.path(), &[&a], &[&b]);
        let staging = crate::runtime::adopt_staging(&d.path().join(DATA_DIR).join(&b));
        std::fs::create_dir_all(&staging).unwrap();
        std::fs::write(staging.join("nuthatch.redb"), vec![0u8; 1024]).unwrap();
        let kept = crate::runtime::adopt_staging(&d.path().join(DATA_DIR).join(&a));
        std::fs::create_dir_all(&kept).unwrap();

        assert!(
            collectable(d.path())
                .unwrap()
                .iter()
                .all(|c| c.nid.len() == 64 && c.nid != format!("{b}.adopting")),
            "staging must never be listed as a dataset"
        );
        run(d.path(), true).unwrap();
        assert!(
            !staging.exists(),
            "the pruned dataset's staging outlived it, and nothing else ever clears it"
        );
        assert!(
            kept.is_dir(),
            "prune removed the staging of a dataset it did not collect"
        );
    }

    /// A directory that is not a dataset is not ours to delete, however tempting the tidy-up.
    #[test]
    fn foreign_directories_are_left_alone() {
        let d = tempfile::tempdir().unwrap();
        let a = nid("aa");
        fixture(d.path(), &[&a], &[]);
        for stray in ["notes", "aa", &"ff".repeat(20)] {
            std::fs::create_dir_all(d.path().join(DATA_DIR).join(stray)).unwrap();
        }

        assert!(collectable(d.path()).unwrap().is_empty());
        run(d.path(), true).unwrap();
        for stray in ["notes", "aa", &"ff".repeat(20)] {
            assert!(
                d.path().join(DATA_DIR).join(stray).is_dir(),
                "prune deleted {stray}, which is not a dataset"
            );
        }
    }

    /// A mount table with zero mounts is readable and valid: every dataset in `data/` is
    /// collectable. The emptier the runtime, the more there is to reclaim.
    #[test]
    fn zero_mount_table_makes_every_dataset_collectable() {
        let d = tempfile::tempdir().unwrap();
        let (a, b) = (nid("aa"), nid("bb"));
        // Write an empty mount table and create two datasets.
        std::fs::write(
            d.path().join(MOUNTS_FILE),
            "[runtime]\nname = \"r\"\nchain = \"arbitrum-one\"\nchain_id = 42161\nrpc_urls = []\n",
        )
        .unwrap();
        for nid_ in [&a, &b] {
            let dir = d.path().join(DATA_DIR).join(nid_);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("nuthatch.redb"), vec![0u8; 512]).unwrap();
        }

        let found = collectable(d.path()).expect("zero-mount table must not be refused");
        assert_eq!(found.len(), 2, "both datasets are collectable: {found:?}");

        run(d.path(), true).expect("prune --yes on a zero-mount runtime must succeed");
        assert!(
            !d.path().join(DATA_DIR).join(&a).exists(),
            "dataset a was not removed"
        );
        assert!(
            !d.path().join(DATA_DIR).join(&b).exists(),
            "dataset b was not removed"
        );
    }

    /// An unreadable mount table means "we do not know what is mounted", never "nothing is". The
    /// difference is every dataset in the runtime.
    #[test]
    fn an_unreadable_mount_table_refuses_rather_than_collecting_everything() {
        let d = tempfile::tempdir().unwrap();
        let a = nid("aa");
        fixture(d.path(), &[&a], &[]);
        std::fs::write(d.path().join(MOUNTS_FILE), "this is not toml {{{").unwrap();

        assert!(
            collectable(d.path()).is_err(),
            "an unparseable mounts.toml must refuse, not offer every dataset for deletion"
        );
        assert!(run(d.path(), true).is_err());
        assert!(
            d.path().join(DATA_DIR).join(&a).is_dir(),
            "data was deleted"
        );
    }

    /// RFC-0033 §11a slice B, **the test that matters**: pruning one of two datasets that share
    /// segments must leave the other's data readable.
    ///
    /// This is the one data-loss failure mode in the shared store. A `prune` that deleted a dataset
    /// directory wholesale - which is what it did before slice A - would take bytes another dataset
    /// is still reading.
    #[test]
    fn pruning_one_dataset_does_not_delete_a_segment_another_still_references() {
        use crate::seal::{Segment, SEGMENTS_DIR};
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        let (keep, drop_) = (nid("aa"), nid("bb"));

        // Two datasets. Both reference `shared`; only the pruned one references `lonely`.
        let shared_hash = "11".repeat(32);
        let lonely_hash = "22".repeat(32);
        std::fs::create_dir_all(root.join(SEGMENTS_DIR)).unwrap();
        for h in [&shared_hash, &lonely_hash] {
            std::fs::write(
                root.join(SEGMENTS_DIR).join(format!("{h}.parquet")),
                b"bytes",
            )
            .unwrap();
        }
        let seg = |h: &str| Segment {
            hash: h.to_string(),
            from_block: 1,
            to_block: 2,
            rows: 1,
            file: format!("t-{h}.parquet"),
            registry_snapshot: None,
            provisional: false,
            writer_profile: crate::seal::ORIGINAL_WRITER_PROFILE.to_string(),
            input_hash: None,
        };
        for (nid_, hashes) in [
            (&keep, vec![shared_hash.clone()]),
            (&drop_, vec![shared_hash.clone(), lonely_hash.clone()]),
        ] {
            let dir = MountTable::data_dir(root, nid_);
            std::fs::create_dir_all(dir.join(SEGMENTS_DIR)).unwrap();
            let mut m = crate::seal::Manifest::default();
            m.tables
                .insert("t".to_string(), hashes.iter().map(|h| seg(h)).collect());
            std::fs::write(
                dir.join(SEGMENTS_DIR).join("manifest.json"),
                serde_json::to_string(&m).unwrap(),
            )
            .unwrap();
        }

        // Only `keep` is mounted, so `drop_` is collectable.
        std::fs::write(
            root.join(MOUNTS_FILE),
            format!(
                "[runtime]\nname = \"r\"\n\n[[chains]]\nchain = \"arbitrum-one\"\n\
                 chain_id = 42161\nrpc_urls = []\n\n[[mounts]]\nalias = \"a\"\nnid = \"{keep}\"\n"
            ),
        )
        .unwrap();

        run(root, true).unwrap();

        let shared = root
            .join(SEGMENTS_DIR)
            .join(format!("{shared_hash}.parquet"));
        let lonely = root
            .join(SEGMENTS_DIR)
            .join(format!("{lonely_hash}.parquet"));
        assert!(
            shared.exists(),
            "prune deleted a segment the surviving dataset still references - this is the data-loss \
             failure mode the shared store was designed around"
        );
        assert!(
            !lonely.exists(),
            "a segment nothing references should have been collected"
        );
        assert!(
            MountTable::data_dir(root, &keep).is_dir(),
            "the mounted dataset survives"
        );
        assert!(!MountTable::data_dir(root, &drop_).exists());
    }

    /// #1547: a running runtime reclaims one dataset. It takes the dataset and the segments only
    /// that dataset names, keeps a segment a mounted dataset shares, and keeps an orphan nothing
    /// names, since that may be one a live fold replaced and a running query is still reading.
    #[test]
    fn reclaim_takes_only_what_the_dataset_alone_references() {
        use crate::seal::{Segment, SEGMENTS_DIR};
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        let (keep, drop_) = (nid("aa"), nid("bb"));
        let (shared, lonely, foreign) = ("11".repeat(32), "22".repeat(32), "33".repeat(32));
        std::fs::create_dir_all(root.join(SEGMENTS_DIR)).unwrap();
        for h in [&shared, &lonely, &foreign] {
            std::fs::write(
                root.join(SEGMENTS_DIR).join(format!("{h}.parquet")),
                b"bytes",
            )
            .unwrap();
        }
        let seg = |h: &str| Segment {
            hash: h.to_string(),
            from_block: 1,
            to_block: 2,
            rows: 1,
            file: format!("t-{h}.parquet"),
            registry_snapshot: None,
            provisional: false,
            writer_profile: crate::seal::ORIGINAL_WRITER_PROFILE.to_string(),
            input_hash: None,
        };
        for (nid_, hashes) in [
            (&keep, vec![shared.clone()]),
            (&drop_, vec![shared.clone(), lonely.clone(), lonely.clone()]),
        ] {
            let dir = MountTable::data_dir(root, nid_);
            std::fs::create_dir_all(dir.join(SEGMENTS_DIR)).unwrap();
            let mut m = crate::seal::Manifest::default();
            m.tables
                .insert("t".to_string(), hashes.iter().map(|h| seg(h)).collect());
            std::fs::write(
                dir.join(SEGMENTS_DIR).join("manifest.json"),
                serde_json::to_string(&m).unwrap(),
            )
            .unwrap();
        }
        std::fs::write(
            root.join(MOUNTS_FILE),
            format!(
                "[runtime]\nname = \"r\"\n\n[[chains]]\nchain = \"arbitrum-one\"\n\
                 chain_id = 42161\nrpc_urls = []\n\n[[mounts]]\nalias = \"a\"\nnid = \"{keep}\"\n"
            ),
        )
        .unwrap();

        match reclaim(root, &drop_).unwrap() {
            Reclaim::Reclaimed { nid, bytes } => {
                assert_eq!(nid, drop_);
                assert!(
                    bytes >= 5,
                    "the freed bytes include the lonely segment: {bytes}"
                );
            }
            other => panic!("expected Reclaimed, got {other:?}"),
        }
        let at = |h: &str| root.join(SEGMENTS_DIR).join(format!("{h}.parquet"));
        assert!(!MountTable::data_dir(root, &drop_).exists());
        assert!(
            !at(&lonely).exists(),
            "the dataset's own segment stays behind"
        );
        assert!(
            at(&shared).exists(),
            "a segment the mounted dataset reads was deleted"
        );
        assert!(
            at(&foreign).exists(),
            "reclaim swept an orphan it cannot prove unread; that is `prune`'s job, offline"
        );
        assert!(MountTable::data_dir(root, &keep).is_dir());
    }

    /// #1547: an unrecorded dataset whose store this process still holds is refused, and nothing is
    /// deleted. A failed persist after a live mount would leave exactly that.
    #[test]
    fn reclaim_refuses_a_dataset_whose_store_is_still_open() {
        let d = tempfile::tempdir().unwrap();
        let (_keep, drop_, _lonely) = shared_fixture(d.path(), "{\"tables\":{}}");
        let db = MountTable::data_dir(d.path(), &drop_).join(crate::config::DB_FILE);
        let _ = std::fs::remove_file(&db);
        let held = crate::store::Store::open(&db).unwrap();

        let err = reclaim(d.path(), &drop_).expect_err("a held store must refuse");
        assert!(format!("{err:#}").contains("still open"), "{err:#}");
        assert!(MountTable::data_dir(d.path(), &drop_).is_dir());
        drop(held);
    }

    /// #1547: a dataset a mount record still names is kept and the answer says by whom; one that is
    /// not on disk is reported absent.
    #[test]
    fn reclaim_keeps_a_named_dataset_and_reports_an_absent_one() {
        let d = tempfile::tempdir().unwrap();
        let (a, b) = (nid("aa"), nid("bb"));
        fixture(d.path(), &[&a], &[]);
        assert_eq!(
            reclaim(d.path(), &a).unwrap(),
            Reclaim::Kept {
                nid: a.clone(),
                mounted_by: vec!["default/n0".to_string()],
            }
        );
        assert!(MountTable::data_dir(d.path(), &a).is_dir());
        assert_eq!(
            reclaim(d.path(), &b).unwrap(),
            Reclaim::Absent { nid: b.clone() }
        );
    }

    /// A shared-store fixture: `keep` mounted with `keep_manifest` as its manifest text, `drop_`
    /// unmounted, and one segment only `drop_` references.
    fn shared_fixture(root: &Path, keep_manifest: &str) -> (String, String, PathBuf) {
        use crate::seal::SEGMENTS_DIR;
        let (keep, drop_) = (nid("aa"), nid("bb"));
        let lonely_hash = "22".repeat(32);
        std::fs::create_dir_all(root.join(SEGMENTS_DIR)).unwrap();
        let lonely = root
            .join(SEGMENTS_DIR)
            .join(format!("{lonely_hash}.parquet"));
        std::fs::write(&lonely, b"bytes").unwrap();
        for (nid_, manifest) in [(&keep, keep_manifest), (&drop_, "{\"tables\":{}}")] {
            let dir = MountTable::data_dir(root, nid_);
            std::fs::create_dir_all(dir.join(SEGMENTS_DIR)).unwrap();
            std::fs::write(dir.join(SEGMENTS_DIR).join("manifest.json"), manifest).unwrap();
        }
        std::fs::write(
            root.join(MOUNTS_FILE),
            format!(
                "[runtime]\nname = \"r\"\n\n[[chains]]\nchain = \"arbitrum-one\"\n\
                 chain_id = 42161\nrpc_urls = []\n\n[[mounts]]\nalias = \"a\"\nnid = \"{keep}\"\n"
            ),
        )
        .unwrap();
        (keep, drop_, lonely)
    }

    /// `referenced_segments` skipped a mounted dataset whose manifest would not read, so every segment it
    /// names counted as an orphan and was deleted: data loss with a zero exit code.
    #[test]
    fn an_unreadable_manifest_of_a_mounted_dataset_stops_collection() {
        let d = tempfile::tempdir().unwrap();
        let (_, _, lonely) = shared_fixture(d.path(), "not json");
        let err = run(d.path(), true).expect_err("an unreadable surviving manifest must refuse");
        assert!(format!("{err:#}").contains("manifest"), "{err:#}");
        assert!(
            lonely.exists(),
            "nothing may be collected past a manifest nobody could read"
        );
    }

    /// A query in a running runtime can plan against a shared segment a fold has since replaced. Its
    /// lease is in that process, so prune must not collect while any mounted store is held.
    #[test]
    fn prune_refuses_while_a_runtime_holds_a_mounted_store() {
        let d = tempfile::tempdir().unwrap();
        let (keep, drop_, lonely) = shared_fixture(d.path(), "{\"tables\":{}}");
        let held = crate::store::Store::open(
            &MountTable::data_dir(d.path(), &keep).join(crate::config::DB_FILE),
        )
        .unwrap();

        let err = run(d.path(), true).expect_err("a held store must refuse");
        assert!(format!("{err:#}").contains("running runtime"), "{err:#}");
        assert!(
            lonely.exists(),
            "no shared segment may go while a runtime could read it"
        );
        assert!(
            MountTable::data_dir(d.path(), &drop_).exists(),
            "the refusal comes before anything is deleted"
        );

        drop(held);
        run(d.path(), true).expect("with the runtime stopped, prune proceeds");
        assert!(!lonely.exists());
    }

    /// Mount, unmount, prune, and the mount record's identity is all it ever took to find the data.
    #[test]
    fn unmounting_makes_a_dataset_collectable_and_remounting_makes_it_safe_again() {
        let d = tempfile::tempdir().unwrap();
        let a = nid("aa");
        fixture(d.path(), &[&a], &[]);
        assert!(collectable(d.path()).unwrap().is_empty());

        // Unmount: the record goes, the data stays.
        std::fs::write(
            d.path().join(MOUNTS_FILE),
            "[runtime]\nname = \"r\"\nchain = \"arbitrum-one\"\nchain_id = 42161\n\
             rpc_urls = []\nnests = [\"placeholder\"]\n",
        )
        .unwrap();
        assert_eq!(collectable(d.path()).unwrap().len(), 1);
        assert!(
            d.path().join(DATA_DIR).join(&a).is_dir(),
            "unmount deleted data - collection must be deferred"
        );

        // Remount the same identity: collectable again becomes empty, with no backfill involved.
        let mut mounts = MountTable::load(d.path()).unwrap();
        mounts.runtime.nests.clear();
        mounts.mounts.push(Mount {
            tenant: "default".into(),
            alias: "back".into(),
            nid: a.clone(),
            sql: Default::default(),
            queries: Vec::new(),
            publish: None,
            #[cfg(feature = "counter")]
            counter: None,
        });
        std::fs::write(
            d.path().join(MOUNTS_FILE),
            toml::to_string_pretty(&mounts).unwrap(),
        )
        .unwrap();
        assert!(collectable(d.path()).unwrap().is_empty());
    }

    /// A manifest at `data/<name>/segments/manifest.json` naming `hashes` in table `t`.
    fn manifest_naming(root: &Path, name: &str, hashes: &[&str]) {
        let dir = root
            .join(DATA_DIR)
            .join(name)
            .join(crate::seal::SEGMENTS_DIR);
        std::fs::create_dir_all(&dir).unwrap();
        let mut m = crate::seal::Manifest::default();
        m.tables.insert(
            "t".to_string(),
            hashes
                .iter()
                .map(|h| crate::seal::Segment {
                    hash: h.to_string(),
                    from_block: 1,
                    to_block: 2,
                    rows: 1,
                    file: format!("t-{h}.parquet"),
                    registry_snapshot: None,
                    provisional: false,
                    writer_profile: crate::seal::ORIGINAL_WRITER_PROFILE.to_string(),
                    input_hash: None,
                })
                .collect(),
        );
        std::fs::write(
            dir.join("manifest.json"),
            serde_json::to_string(&m).unwrap(),
        )
        .unwrap();
    }

    /// #2128: an unmounted dataset is kept so a remount is free, so a segment it shares with the one
    /// reclaimed is not the reclaimed one's alone. Nor is one an adoption is staging.
    #[test]
    fn reclaim_keeps_a_segment_an_unmounted_dataset_or_an_adoption_still_names() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        let (kept, adopting, drop_) = (nid("aa"), nid("cc"), nid("bb"));
        let (shared, staged, lonely) = ("11".repeat(32), "33".repeat(32), "22".repeat(32));
        fixture(root, &[], &[&kept, &drop_]);
        let at = |h: &str| {
            root.join(crate::seal::SEGMENTS_DIR)
                .join(format!("{h}.parquet"))
        };
        std::fs::create_dir_all(root.join(crate::seal::SEGMENTS_DIR)).unwrap();
        for h in [&shared, &staged, &lonely] {
            std::fs::write(at(h), b"bytes").unwrap();
        }
        manifest_naming(root, &kept, &[&shared]);
        manifest_naming(root, &format!("{adopting}.adopting"), &[&staged]);
        manifest_naming(root, &drop_, &[&shared, &staged, &lonely]);

        assert!(matches!(
            reclaim(root, &drop_).unwrap(),
            Reclaim::Reclaimed { .. }
        ));
        assert!(
            at(&shared).exists(),
            "reclaim deleted a segment the unmounted, kept dataset still names"
        );
        assert!(
            at(&staged).exists(),
            "reclaim deleted a segment an adoption is staging"
        );
        assert!(
            !at(&lonely).exists(),
            "the dataset's own segment stays behind"
        );
    }

    /// #2133: a mount job that has installed `data/<nid>` but not yet recorded it names the dataset
    /// in `mount-jobs.json`, and a restart resumes it, so prune keeps it. A failed job keeps nothing.
    #[test]
    fn prune_keeps_a_dataset_an_unfinished_mount_job_names() {
        let d = tempfile::tempdir().unwrap();
        let (joining, failed) = (nid("aa"), nid("bb"));
        fixture(d.path(), &[], &[&joining, &failed]);
        std::fs::write(
            d.path().join(crate::mount_jobs::JOBS_FILE),
            format!(
                "[{{\"name\":\"j\",\"nid\":\"{joining}\",\"phase\":\"joining\",\"since_unixtime\":0}},\
                 {{\"name\":\"f\",\"nid\":\"{failed}\",\"phase\":\"failed\",\"since_unixtime\":0}}]"
            ),
        )
        .unwrap();

        run(d.path(), true).unwrap();
        assert!(
            MountTable::data_dir(d.path(), &joining).is_dir(),
            "prune deleted the dataset a mount job is about to record"
        );
        assert!(!MountTable::data_dir(d.path(), &failed).exists());

        std::fs::write(d.path().join(crate::mount_jobs::JOBS_FILE), "not json").unwrap();
        assert!(run(d.path(), true).is_err(), "unknown jobs must refuse");
        assert!(MountTable::data_dir(d.path(), &joining).is_dir());
    }

    /// #2133: a store open in a runtime with every recorded mount suspended is one being mounted.
    #[test]
    fn prune_refuses_while_a_runtime_holds_an_unrecorded_store() {
        let d = tempfile::tempdir().unwrap();
        let mounting = nid("aa");
        fixture(d.path(), &[], &[&mounting]);
        let db = MountTable::data_dir(d.path(), &mounting).join(crate::config::DB_FILE);
        std::fs::remove_file(&db).unwrap();
        let held = crate::store::Store::open(&db).unwrap();

        let err = run(d.path(), true).expect_err("a held store must refuse");
        assert!(format!("{err:#}").contains("running runtime"), "{err:#}");
        assert!(MountTable::data_dir(d.path(), &mounting).is_dir());
        drop(held);
    }

    /// #2249: a waited mount's fetch lands in `data/<nid>` with no store open and no job naming it.
    /// A runtime holds its directory's lock meanwhile, and prune refuses on it.
    #[test]
    fn prune_refuses_while_a_runtime_holds_the_directory() {
        let d = tempfile::tempdir().unwrap();
        let fetched = nid("aa");
        fixture(d.path(), &[], &[&fetched]);
        let db = MountTable::data_dir(d.path(), &fetched).join(crate::config::DB_FILE);
        std::fs::remove_file(&db).unwrap();
        let running = crate::runtime::lock_runtime_dir(d.path(), false).unwrap();

        let err = run(d.path(), true).expect_err("a running runtime must refuse");
        assert!(
            format!("{err:#}").contains("a runtime is running"),
            "{err:#}"
        );
        assert!(MountTable::data_dir(d.path(), &fetched).is_dir());

        drop(running);
        run(d.path(), true).unwrap();
        assert!(!MountTable::data_dir(d.path(), &fetched).exists());
    }

    /// #2249: with nothing collectable because the one unrecorded dataset is mid-mount, prune says so
    /// rather than calling every dataset mounted.
    #[test]
    fn a_dataset_mid_mount_is_not_reported_as_mounted() {
        let d = tempfile::tempdir().unwrap();
        let (mounted, joining) = (nid("aa"), nid("bb"));
        fixture(d.path(), &[&mounted], &[&joining]);
        assert_eq!(settled(d.path()).unwrap(), "every dataset is mounted");
        std::fs::write(
            d.path().join(crate::mount_jobs::JOBS_FILE),
            format!("[{{\"name\":\"j\",\"nid\":\"{joining}\",\"phase\":\"joining\",\"since_unixtime\":0}}]"),
        )
        .unwrap();
        assert!(collectable(d.path()).unwrap().is_empty());
        assert_eq!(
            settled(d.path()).unwrap(),
            "every dataset is mounted or being mounted (1 mid-mount)"
        );
    }
}
