//! `nuthatch migrate` - move a runtime from the name-keyed layout to identity-keyed datasets
//! (RFC-0032 §8, slice 1).
//!
//! Before: `<root>/nests/<name>/` - an operator's label decides where data lives. After:
//! `<root>/data/<nid>/`, addressed by what the nest *is*, with `mounts.toml` carrying `[[mounts]]`
//! records mapping alias → identity.
//!
//! **Data is moved, never re-indexed.** If a migration ever needs a backfill, the migration is wrong.
//!
//! Two properties matter more than speed:
//!
//! - **Idempotent.** Running it twice is a no-op. Running it after adding one nest migrates only that
//!   nest. There is no "already migrated" error to work around.
//! - **Refuses rather than guesses.** A nest whose inputs no longer reproduce its own manifest is
//!   named and skipped, and the whole run reports a failure. A half-migrated mounts still serves,
//!   because [`crate::runtime::MountTable::dir_for`] resolves both layouts.
//!
//! **Deviation from RFC-0032 §12, recorded deliberately.** The RFC specified copy-then-verify-then-
//! swap. This uses `rename` when source and destination share a filesystem - which they do, both
//! being under the runtime root - and falls back to copy-verify-remove only across devices. A rename is
//! atomic: it cannot produce the half-written destination the copy path exists to guard against, and
//! it does not require double the disk of an indexed history. The RFC's *intent* was "never lose
//! data"; rename serves it better than the mechanism the RFC named.

use crate::blob;
use crate::runtime::{
    adoptable, Mount, MountTable, DATA_DIR, LEGACY_ROOST_FILE, MOUNTS_FILE, NESTS_DIR,
};
use anyhow::{bail, Context, Result};
use sha2::Digest;
use std::path::{Path, PathBuf};

/// What migrating one nest would do. Computed for every nest before anything moves, so `--dry-run`
/// and the real run share one code path and a plan is never a separate, drifting implementation.
#[derive(Debug, PartialEq, Eq)]
pub enum Plan {
    /// Already addressed by identity, at the identity it should have. Nothing to do.
    AlreadyMigrated { alias: String, nid: String },
    /// Move `nests/<alias>` to `data/<nid>`.
    Move {
        alias: String,
        nid: String,
        from: PathBuf,
        to: PathBuf,
        /// When this alias was already serving a *different* identity, how the new nest's schema
        /// compares (RFC-0033 §9). `None` when there is nothing to compare against.
        breaking: Option<Vec<String>>,
    },
    /// `data/<nid>` already holds this exact nest, mounted under another alias. The migration has
    /// found a **pre-existing double-index**: two names, byte-identical inputs, two backfills of the
    /// same chain data. Both aliases end up serving one dataset.
    Merge {
        alias: String,
        nid: String,
        with: String,
        from: PathBuf,
    },
    /// The nest's identity is new, but an existing dataset was built from inputs that produce
    /// **byte-identical data** (RFC-0033 §5, early cutoff). Adopt it rather than re-indexing the
    /// chain: the package changed, the data did not.
    Adopt {
        alias: String,
        nid: String,
        from_nid: String,
        from: PathBuf,
        to: PathBuf,
        /// The incoming nest's own package directory. `from` supplies the data;
        /// this supplies what the operator authored (views/, semantic.toml).
        /// Without it an ADOPT copies the *source* package wholesale and the
        /// adopting nest's own files are silently dropped (#587).
        package: PathBuf,
    },
    /// Something is wrong with the nest and it will not be touched.
    Refuse { alias: String, why: String },
}

impl Plan {
    /// One line, in the order an operator reads: what happens, to what, and where it lands.
    fn describe(&self) -> String {
        match self {
            Plan::AlreadyMigrated { alias, nid } => {
                format!("  {alias}: already at data/{} - nothing to do", &nid[..12])
            }
            Plan::Adopt {
                alias,
                nid,
                from_nid,
                ..
            } => format!(
                "  {alias}: ADOPT data/{} -> data/{} - identity moved, data did not (no re-index)",
                &from_nid[..12],
                &nid[..12]
            ),
            Plan::Move {
                alias,
                nid,
                breaking,
                ..
            } => {
                let base = format!("  {alias}: nests/{alias} -> data/{}", &nid[..12]);
                match breaking {
                    Some(reasons) if !reasons.is_empty() => format!(
                        "{base}\n      BREAKING for consumers of `{alias}`:\n        - {}",
                        reasons.join("\n        - ")
                    ),
                    _ => base,
                }
            }
            Plan::Merge {
                alias, nid, with, ..
            } => format!(
                "  {alias}: MERGE into data/{} - identical to '{with}', which had its own backfill",
                &nid[..12]
            ),
            Plan::Refuse { alias, why } => format!("  {alias}: REFUSED - {why}"),
        }
    }
}

/// Build the full plan without touching anything.
///
/// Computing every nest's identity up front is what makes [`Plan::Merge`] detectable at all: two
/// nests only turn out to be the same nest once both have been hashed.
pub fn plan(dir: &Path) -> Result<Vec<Plan>> {
    let mounts = MountTable::load_for_migration(dir)?;
    let mut plans = Vec::with_capacity(mounts.runtime.nests.len());
    // Identity -> the first alias that claimed it, for merge detection within this run.
    let mut claimed: std::collections::HashMap<String, String> = std::collections::HashMap::new();

    // `mount_refs()`, **not** `mounts.nests`: once a directory is migrated the mount records are
    // authoritative and `nests` is empty, so iterating the list would see nothing on a second run and
    // then write an empty mount table - turning "migrate twice" from a no-op into data loss.
    for alias in mounts
        .mount_refs()
        .iter()
        .map(|m| m.alias.clone())
        .collect::<Vec<_>>()
    {
        let alias = &alias;
        let recorded = mounts.mounts.iter().find(|m| &m.alias == alias);
        let legacy = dir.join(NESTS_DIR).join(alias);

        // Already migrated: a record exists, its dataset is on disk, and **nothing is staged**.
        //
        // The staged check is what lets `migrate` double as the upgrade path (RFC-0033 §9): a new
        // version of an already-mounted nest arrives in `nests/<alias>`, and if its identity differs
        // from the record this is an upgrade, not a no-op. Without it the staged directory is
        // silently ignored and the operator is told "nothing to do" while holding a new version.
        //
        // Re-hashing only when something is staged keeps the ordinary case cheap - re-deriving every
        // nest's identity on every run would cost a registry rebuild each time for no decision it
        // could change.
        if let Some(m) = recorded {
            let staged_differs =
                legacy.is_dir() && nid_of(&legacy).map(|n| n != m.nid).unwrap_or(false);
            if dir.join(DATA_DIR).join(&m.nid).is_dir() && !staged_differs {
                claimed.insert(m.nid.clone(), alias.clone());
                plans.push(Plan::AlreadyMigrated {
                    alias: alias.clone(),
                    nid: m.nid.clone(),
                });
                continue;
            }
        }

        if !legacy.is_dir() {
            plans.push(Plan::Refuse {
                alias: alias.clone(),
                why: format!("{} does not exist", legacy.display()),
            });
            continue;
        }

        let nid = match nid_of(&legacy) {
            Ok(nid) => nid,
            Err(e) => {
                plans.push(Plan::Refuse {
                    alias: alias.clone(),
                    why: format!("{e:#}"),
                });
                continue;
            }
        };

        let dest = dir.join(DATA_DIR).join(&nid);
        match claimed.get(&nid) {
            // Another alias in this same mounts is the same nest, byte for byte.
            Some(other) => plans.push(Plan::Merge {
                alias: alias.clone(),
                nid: nid.clone(),
                with: other.clone(),
                from: legacy,
            }),
            None if dest.is_dir() => {
                // The dataset exists but no alias in this run claimed it - a previous partial run.
                claimed.insert(nid.clone(), alias.clone());
                plans.push(Plan::Merge {
                    alias: alias.clone(),
                    nid,
                    with: "an existing dataset".to_string(),
                    from: legacy,
                });
            }
            None => {
                claimed.insert(nid.clone(), alias.clone());
                // Early cutoff (RFC-0033 §5) before falling back to a move: if some existing dataset
                // already holds what this nest would index, adopt it rather than moving a legacy
                // directory that may be empty and would then re-backfill.
                let adopt = blob::build_manifest(&legacy, None)
                    .ok()
                    .and_then(|m| adoptable(dir, &m, &nid));
                match adopt {
                    Some((from_nid, from)) => plans.push(Plan::Adopt {
                        alias: alias.clone(),
                        nid,
                        from_nid,
                        from,
                        to: dest,
                        package: legacy.clone(),
                    }),
                    None => {
                        let breaking = breaking_against_current(dir, &mounts, alias, &legacy);
                        plans.push(Plan::Move {
                            alias: alias.clone(),
                            nid,
                            from: legacy,
                            to: dest,
                            breaking,
                        })
                    }
                }
            }
        }
    }
    Ok(plans)
}

/// Whether replacing `alias`'s current dataset with the nest staged at `staged` would **break
/// consumers** (RFC-0033 §9), and how.
///
/// This is what `nest diff` used to tell an operator who remembered to run it. Grafting makes the
/// *data* free; it says nothing about whether a dashboard's query still resolves. So the runtime
/// reports it at the moment the identity actually changes, rather than leaving it to a command.
///
/// `None` when there is nothing to compare against - a first migration, or a dataset with no
/// `schema.json`. Comparison failures are `None` too: an unreadable schema is not evidence of a
/// breaking change, and inventing one would train operators to ignore the warning.
fn breaking_against_current(
    dir: &Path,
    mounts: &MountTable,
    alias: &str,
    staged: &Path,
) -> Option<Vec<String>> {
    let current_nid = &mounts.mounts.iter().find(|m| m.alias == alias)?.nid;
    let old =
        std::fs::read_to_string(MountTable::data_dir(dir, current_nid).join("schema.json")).ok()?;
    let new = std::fs::read_to_string(staged.join("schema.json")).ok()?;
    let class = crate::lifecycle::classify_schemas(&old, &new).ok()?;
    if class.verdict != crate::lifecycle::Verdict::Breaking {
        return None;
    }
    Some(
        class
            .changes
            .iter()
            .filter(|c| c.is_breaking())
            .map(|c| c.describe())
            .collect(),
    )
}

/// A nest's identity, refusing one whose packed manifest no longer matches its inputs.
///
/// A nest installed from a bundle carries `manifest.json`. If the inputs have drifted from it, the
/// nest is not the nest it claims to be, and silently giving it a fresh identity would hide an edit
/// somebody made to a supposedly-pinned deploy unit.
pub fn nid_of(nest: &Path) -> Result<String> {
    let manifest_path = nest.join("manifest.json");
    if manifest_path.is_file() {
        let raw = std::fs::read_to_string(&manifest_path)
            .with_context(|| format!("reading {}", manifest_path.display()))?;
        let packed: blob::Manifest = serde_json::from_str(&raw)
            .with_context(|| format!("parsing {}", manifest_path.display()))?;
        blob::verify_registry_reproduces(nest, &packed)
            .context("this nest's inputs no longer reproduce the manifest it was packed with")?;
    }
    blob::nest_nid(nest)
}

/// `nuthatch migrate <dir> [--dry-run]`: apply the plan (RFC-0032 §8).
///
/// Returns an error if any nest was refused, *after* migrating every nest that was fine - a bad nest
/// must not hold the rest hostage, and `dir_for` means a partially-migrated mounts still serves.
pub fn run(dir: &Path, dry_run: bool, allow_breaking: bool) -> Result<()> {
    let plans = plan(dir)?;
    println!(
        "{} {} nest(s) in {}\n",
        if dry_run {
            "Would migrate"
        } else {
            "Migrating"
        },
        plans.len(),
        dir.display()
    );
    for p in &plans {
        println!("{}", p.describe());
    }
    println!();

    // A breaking change stops the run **before** anything moves, unless it is explicitly accepted.
    // Warning and proceeding was the old behaviour of `nest diff`, which relied on somebody having run
    // it; refusing by default puts the decision in front of the operator at the moment it matters.
    let breaking: Vec<&Plan> = plans
        .iter()
        .filter(|p| matches!(p, Plan::Move { breaking: Some(r), .. } if !r.is_empty()))
        .collect();
    if !breaking.is_empty() && !allow_breaking && !dry_run {
        bail!(
            "{} mount(s) would break consumers (listed above). Nothing was changed.\n\
             The data is safe either way - this is about queries, not bytes. Re-run with \
             `--allow-breaking` once the consumers are ready, or mount the new nest under a \
             different alias and migrate them across.",
            breaking.len()
        );
    }

    if dry_run {
        println!("Dry run: nothing was changed. Re-run without --dry-run to apply.");
        return Ok(());
    }

    // Everything an un-migrated mounts held belongs to the default tenant (RFC-0032 §6): migration is
    // a **relabel, not a migration** - no data moves between tenants, nothing re-indexes, and
    // enabling hosted tenancy later is another relabel rather than a second migration.
    let tenant = MountTable::load_for_migration(dir)?.tenant_default();
    let mut records: Vec<Mount> = Vec::new();
    let mut refused: Vec<&str> = Vec::new();
    for p in &plans {
        match p {
            Plan::AlreadyMigrated { alias, nid } => records.push(Mount {
                tenant: tenant.clone(),
                alias: alias.clone(),
                nid: nid.clone(),
                // Migration never invents a security posture: an existing deployment keeps the
                // arbitrary-/sql behaviour it already had. Bounding the surface is an operator's
                // deliberate act (RFC-0034), not something a layout change does to them.
                sql: crate::allowlist::SqlAccess::Open,
                queries: Vec::new(),
                publish: None,
                #[cfg(feature = "counter")]
                counter: None,
            }),
            Plan::Move {
                alias,
                nid,
                from,
                to,
                ..
            } => {
                relocate(from, to)?;
                records.push(Mount {
                    tenant: tenant.clone(),
                    alias: alias.clone(),
                    nid: nid.clone(),
                    sql: crate::allowlist::SqlAccess::Open,
                    queries: Vec::new(),
                    publish: None,
                    #[cfg(feature = "counter")]
                    counter: None,
                });
            }
            Plan::Merge {
                alias, nid, from, ..
            } => {
                // The destination already holds this exact nest. The duplicate's *inputs* are
                // identical by definition, so the only thing lost is its separately-indexed data -
                // which is why the duplicate is moved aside rather than deleted. An operator who
                // wants the disk back can remove it once they have seen the merge happen.
                let aside = from.with_extension("merged-duplicate");
                relocate(from, &aside)?;
                println!(
                    "  {alias}: duplicate data kept at {} (safe to delete once verified)",
                    aside.display()
                );
                records.push(Mount {
                    tenant: tenant.clone(),
                    alias: alias.clone(),
                    nid: nid.clone(),
                    sql: crate::allowlist::SqlAccess::Open,
                    queries: Vec::new(),
                    publish: None,
                    #[cfg(feature = "counter")]
                    counter: None,
                });
            }
            Plan::Adopt {
                alias,
                nid,
                from,
                to,
                package,
                ..
            } => {
                // A **copy**, not a move or a rename: the source dataset may still be mounted by
                // another alias or another tenant, and early cutoff must never take data away from a
                // mount that is using it. Costs disk; the alternative costs a full backfill.
                //
                // Only the source's derived state: its authored files are the old version's, and a
                // file the new version deleted would come back (#587, #2362). The package supplies
                // every authored file.
                crate::runtime::stage_derived_state(from, to).with_context(|| {
                    format!("adopting {} into {}", from.display(), to.display())
                })?;
                let overlaid = overlay_package(package, to).with_context(|| {
                    format!(
                        "copying {}'s authored files into {}",
                        package.display(),
                        to.display()
                    )
                })?;
                let landed = blob::nest_nid(to)?;
                if landed != *nid {
                    std::fs::remove_dir_all(to).ok();
                    bail!(
                        "adopting {alias} produced {} hashing to {landed}, not {nid}",
                        to.display()
                    );
                }
                if overlaid > 0 {
                    println!(
                        "  {alias}: adopted without re-indexing ({overlaid} authored file(s) kept)"
                    );
                } else {
                    println!("  {alias}: adopted without re-indexing");
                }
                records.push(Mount {
                    tenant: tenant.clone(),
                    alias: alias.clone(),
                    nid: nid.clone(),
                    sql: crate::allowlist::SqlAccess::Open,
                    queries: Vec::new(),
                    publish: None,
                    #[cfg(feature = "counter")]
                    counter: None,
                });
            }
            Plan::Refuse { alias, .. } => refused.push(alias),
        }
    }

    // Slice C (RFC-0033 §11a): pull the migrated datasets' segments into the shared store, so two
    // nests holding byte-identical segments stop holding two copies.
    let (mut shared, mut reclaimed) = (0usize, 0usize);
    for r in &records {
        let (m, d) = relocate_segments(dir, &MountTable::data_dir(dir, &r.nid))?;
        shared += m;
        reclaimed += d;
    }
    if shared > 0 {
        println!(
            "Shared {shared} segment(s) into {}/ ({reclaimed} duplicate cop{} reclaimed).",
            crate::seal::SEGMENTS_DIR,
            if reclaimed == 1 { "y" } else { "ies" }
        );
    }

    let removed_legacy = write_mounts(dir, &records)?;
    println!(
        "\nWrote {} mount record(s) to {MOUNTS_FILE}.",
        records.len()
    );
    if removed_legacy {
        println!("Removed the pre-2.0 {LEGACY_ROOST_FILE} - this directory is now 2.0 shaped.");
    }

    // An empty `nests/` left standing reads as "half of it didn't work". `remove_dir` refuses a
    // non-empty directory, so this can only ever tidy up - never delete a nest.
    if std::fs::remove_dir(dir.join(NESTS_DIR)).is_ok() {
        println!("Removed the now-empty {NESTS_DIR}/ directory.");
    }

    if !refused.is_empty() {
        bail!(
            "migrated what could be migrated, but refused: {}. \
             The mounts still serves - un-migrated nests resolve through the old layout.",
            refused.join(", ")
        );
    }
    Ok(())
}

/// Move a directory, preferring an atomic rename. See the module docs for why this is not the
/// copy-then-swap RFC-0032 §12 described.
/// Copy the adopting nest's authored files over an adopted dataset.
///
/// Returns how many files were overlaid, so the caller can say so rather than
/// claiming work it did not do.
///
/// Data is excluded by name rather than by an allowlist of package files: a nest
/// may carry anything its author put there — `views/`, `semantic.toml`, an
/// `llms.txt`, a `.claude/skills/` tree — and an allowlist would quietly drop
/// whatever it had not been taught about, which is the bug being fixed. Only the
/// things that are unambiguously the *source's* data are skipped.
///
/// That set is [`crate::blob::DERIVED_STATE`] rather than a literal here, and the
/// reuse is load-bearing rather than tidiness. `DERIVED_STATE` is defined as "what
/// *is* the data - the set early cutoff copies when one identity adopts another's
/// dataset", which is exactly the question this function asks, and it is held to
/// `blob::EXCLUDE` by a test. A second copy of the same two names would be correct
/// today and silently wrong the day the derived set grows: a new data file would
/// be copied here as though the operator had authored it - the very class of bug
/// #587 is about.
fn overlay_package(package: &Path, dest: &Path) -> Result<usize> {
    if !package.is_dir() {
        return Ok(0);
    }
    let skip = crate::blob::DERIVED_STATE;
    let mut copied = 0usize;
    for entry in
        std::fs::read_dir(package).with_context(|| format!("reading {}", package.display()))?
    {
        let entry = entry?;
        let name = entry.file_name();
        if skip.iter().any(|s| name == std::ffi::OsStr::new(s)) {
            continue;
        }
        let (src, dst) = (entry.path(), dest.join(&name));
        if src.is_dir() {
            crate::project::copy_dir(&src, &dst)
                .with_context(|| format!("copying {}", src.display()))?;
            copied += walk_count(&dst);
        } else {
            if let Some(parent) = dst.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::copy(&src, &dst).with_context(|| format!("copying {}", src.display()))?;
            copied += 1;
        }
    }
    Ok(copied)
}

/// Files under a directory, for reporting only.
fn walk_count(dir: &Path) -> usize {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return 0;
    };
    rd.flatten()
        .map(|e| {
            if e.path().is_dir() {
                walk_count(&e.path())
            } else {
                1
            }
        })
        .sum()
}

fn relocate(from: &Path, to: &Path) -> Result<()> {
    if let Some(parent) = to.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("cannot create {}", parent.display()))?;
    }
    if std::fs::rename(from, to).is_ok() {
        return Ok(());
    }
    // Cross-device, or a rename the platform refused. Copy, then remove the source only once the
    // copy is complete - so an interruption leaves the source intact and the run simply repeatable.
    crate::project::copy_dir(from, to)
        .with_context(|| format!("copying {} to {}", from.display(), to.display()))?;
    std::fs::remove_dir_all(from)
        .with_context(|| format!("removing {} after copying it", from.display()))?;
    Ok(())
}

/// Relocate a dataset's per-dataset segments into the shared store (RFC-0033 §11a, slice C).
///
/// **Hard-links where it can, copies otherwise.** A link means two datasets holding byte-identical
/// segments collapse to one copy on disk with no read and no rewrite; a copy is the fallback across
/// filesystems. The per-dataset file is deliberately left in place: `segment_path` prefers the shared
/// copy and falls back to the local one, so a half-relocated dataset reads correctly throughout.
///
/// Idempotent by construction - the destination is the content hash, so a segment another dataset
/// already contributed is skipped rather than rewritten.
fn relocate_segments(root: &Path, dataset: &Path) -> Result<(usize, usize)> {
    let Ok(manifest) = crate::seal::load_manifest(dataset) else {
        return Ok((0, 0));
    };
    let store = root.join(crate::seal::SEGMENTS_DIR);
    std::fs::create_dir_all(&store).context("creating the shared segment store")?;

    let mut moved = 0usize;
    let mut reclaimed = 0usize;
    for segs in manifest.tables.values() {
        for s in segs {
            let dest = store.join(format!("{}.parquet", s.hash));
            let src = dataset.join(crate::seal::SEGMENTS_DIR).join(&s.file);
            if !src.exists() {
                continue;
            }
            if !dest.exists() && std::fs::hard_link(&src, &dest).is_err() {
                std::fs::copy(&src, &dest)
                    .with_context(|| format!("copying {} into the shared store", s.file))?;
            }

            // **Then drop the per-dataset copy**, which is what actually reclaims the disk.
            //
            // Verified on 881 MB of real production data before this existed: linking alone shares
            // nothing that already exists. Two nests migrated separately hold two *distinct inodes*
            // of identical bytes, and linking one of them into the store only avoids creating a
            // third. The duplication between the two datasets survived, and the total on disk did
            // not move.
            //
            // This is a deletion during a migration, so it is gated rather than assumed:
            //
            // - if `src` and `dest` are the **same inode** (the hard-link path) the bytes cannot go
            //   anywhere - unlinking one name of a two-name file is free;
            // - otherwise the shared copy is **re-hashed and checked against the manifest** before
            //   anything is removed. A mismatched or unreadable shared copy leaves the local one
            //   exactly where it is.
            //
            // `segment_path` prefers the shared copy and falls back to the local one, so a dataset
            // reads correctly at every point in this sequence.
            if safe_to_drop_local(&src, &dest, &s.hash) {
                std::fs::remove_file(&src)
                    .with_context(|| format!("removing the duplicate copy of {}", s.file))?;
                reclaimed += 1;
            }
            moved += 1;
        }
    }
    Ok((moved, reclaimed))
}

/// Whether the per-dataset copy may be removed now the shared store holds these bytes.
///
/// Two ways to be sure, and nothing else counts. Same inode means unlinking one name of a two-name
/// file cannot lose data. Otherwise the shared copy is re-read and hashed against what the manifest
/// says it should be - because the alternative is deleting an operator's only copy on the strength of
/// a filename.
fn safe_to_drop_local(src: &Path, dest: &Path, hash: &str) -> bool {
    if let (Ok(a), Ok(b)) = (std::fs::metadata(src), std::fs::metadata(dest)) {
        use std::os::unix::fs::MetadataExt;
        if a.ino() == b.ino() && a.dev() == b.dev() {
            return true;
        }
    }
    match std::fs::read(dest) {
        Ok(bytes) => hex::encode(sha2::Sha256::digest(&bytes)) == hash,
        Err(_) => false,
    }
}

/// Rewrite `mounts.toml` with the mount records, temp-then-rename so a crash cannot truncate it.
fn write_mounts(dir: &Path, records: &[Mount]) -> Result<bool> {
    // Read whichever file this directory has - the migration's whole job is that it may still be the
    // pre-2.0 one - and always *write* the 2.0 name.
    let legacy = dir.join(LEGACY_ROOST_FILE);
    let source = if dir.join(MOUNTS_FILE).exists() {
        dir.join(MOUNTS_FILE)
    } else {
        legacy.clone()
    };
    let path = dir.join(MOUNTS_FILE);
    let raw = std::fs::read_to_string(&source)
        .with_context(|| format!("reading {} to record mounts", source.display()))?;
    let mut mounts: MountTable = toml::from_str(&raw)
        .with_context(|| format!("parsing {} before rewriting it", source.display()))?;
    mounts.mounts = records.to_vec();
    // Carry a pre-2.0 top-level chain across into `[[chains]]` (RFC-0035 §2). Without this the
    // migration writes a file that parses and then refuses to start - a migration that produces an
    // outage is worse than one that refuses.
    if let Some(endpoint) = mounts.chains_from_legacy() {
        mounts.chains = vec![endpoint];
        mounts.runtime.chain = None;
        mounts.runtime.chain_id = None;
        mounts.runtime.rpc_urls.clear();
    }
    // `nests` was the pre-2.0 way to say what is mounted; the records are authoritative now, and a
    // stale list beside them would lie.
    mounts.runtime.nests.clear();
    let out = toml::to_string_pretty(&mounts).context("serialising mounts.toml")?;
    let tmp = path.with_extension("toml.tmp");
    std::fs::write(&tmp, out).with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, &path).with_context(|| format!("replacing {}", path.display()))?;
    // The pre-2.0 file goes only once its replacement is durably in place, so an interrupted run
    // leaves a directory that still loads rather than one that loads as neither.
    let mut removed_legacy = false;
    if source == legacy && legacy.exists() {
        std::fs::remove_file(&legacy)
            .with_context(|| format!("removing the pre-2.0 {}", legacy.display()))?;
        removed_legacy = true;
    }
    Ok(removed_legacy)
}

#[cfg(test)]
mod tests {
    fn walkdir(p: &Path) -> Vec<String> {
        let mut out = Vec::new();
        let mut stack = vec![p.to_path_buf()];
        while let Some(d) = stack.pop() {
            if let Ok(rd) = std::fs::read_dir(&d) {
                for e in rd.flatten() {
                    if e.path().is_dir() {
                        stack.push(e.path());
                    } else {
                        out.push(e.path().display().to_string());
                    }
                }
            }
        }
        out
    }

    use super::*;

    /// The `last_block` [`write_roost`] seeds into nest `i`'s store. Distinct per nest, so an
    /// assertion that the right history arrived cannot be satisfied by another nest's.
    fn fixture_head(i: usize) -> u64 {
        1000 + i as u64
    }

    /// A mounts with `n` nests, all on one chain, in the pre-2.0 layout.
    fn write_roost(dir: &Path, nests: &[(&str, &str)]) {
        let names: Vec<String> = nests.iter().map(|(n, _)| format!("\"{n}\"")).collect();
        std::fs::write(
            dir.join(MOUNTS_FILE),
            format!(
                "[runtime]\nname = \"r\"\nchain = \"arbitrum-one\"\nchain_id = 42161\n\
                 rpc_urls = []\nnests = [{}]\n",
                names.join(", ")
            ),
        )
        .unwrap();
        for (i, (name, addr)) in nests.iter().enumerate() {
            let nest = dir.join(NESTS_DIR).join(name);
            std::fs::create_dir_all(&nest).unwrap();
            std::fs::write(
                nest.join("nuthatch.toml"),
                format!(
                    "[nest]\nname = \"{name}\"\nchain = \"arbitrum-one\"\nchain_id = 42161\n\
                     rpc_urls = []\n\n[[contracts]]\nalias = \"t\"\naddress = \"{addr}\"\n\
                     abi = \"abi.json\"\n"
                ),
            )
            .unwrap();
            std::fs::write(nest.join("abi.json"), "[]").unwrap();
            // Indexed data, to prove migration moves it rather than re-deriving it. A real store
            // with rows: the text file this replaced was unopenable, so it read as history through
            // the error path and the adoption tests passed without a readable store ever existing
            // (issue #415).
            crate::runtime::seed_history(&nest, fixture_head(i));
        }
    }

    #[test]
    fn a_dry_run_changes_nothing() {
        let d = tempfile::tempdir().unwrap();
        write_roost(
            d.path(),
            &[("a", "0x0000000000000000000000000000000000000001")],
        );
        let before = std::fs::read_to_string(d.path().join(MOUNTS_FILE)).unwrap();

        run(d.path(), true, false).unwrap();

        assert!(d.path().join(NESTS_DIR).join("a").is_dir(), "source moved");
        assert!(!d.path().join(DATA_DIR).exists(), "destination created");
        assert_eq!(
            before,
            std::fs::read_to_string(d.path().join(MOUNTS_FILE)).unwrap(),
            "mounts.toml was rewritten by a dry run"
        );
    }

    /// The core of slice 1: data moves to an identity-keyed directory, byte for byte, and the runtime
    /// resolves through the records afterwards.
    #[test]
    fn migration_moves_data_and_never_re_indexes() {
        let d = tempfile::tempdir().unwrap();
        write_roost(
            d.path(),
            &[
                ("a", "0x0000000000000000000000000000000000000001"),
                ("b", "0x0000000000000000000000000000000000000002"),
            ],
        );
        run(d.path(), false, false).unwrap();

        let mounts = MountTable::load(d.path()).unwrap();
        assert_eq!(mounts.mounts.len(), 2);
        for (i, alias) in ["a", "b"].into_iter().enumerate() {
            let dir = mounts.dir_for(d.path(), alias);
            assert!(
                dir.starts_with(d.path().join(DATA_DIR)),
                "{alias} did not move under data/"
            );
            // The indexed data came across untouched. Had migration re-derived anything, this is
            // where it would show - and the head is per-nest, so a crossed pair fails here too.
            assert_eq!(
                crate::runtime::history_head(&dir),
                Some(fixture_head(i)),
                "{alias}'s indexed history did not arrive intact"
            );
            assert!(
                !d.path().join(NESTS_DIR).join(alias).exists(),
                "{alias} was left behind in the old layout"
            );
        }
        // Two different nests, two identities.
        assert_ne!(mounts.mounts[0].nid, mounts.mounts[1].nid);
    }

    /// The regression that made this test earn its keep: clearing `nests` when the mount records
    /// became authoritative meant a **second** `migrate` saw nothing to do and wrote an empty mount
    /// table - turning idempotency into data loss. Asserted explicitly, not just via the byte
    /// comparison below, so a future refactor gets told what broke rather than that two strings
    /// differ.
    #[test]
    fn migrating_twice_does_not_empty_the_mount_table() {
        let d = tempfile::tempdir().unwrap();
        write_roost(
            d.path(),
            &[("a", "0x0000000000000000000000000000000000000001")],
        );
        run(d.path(), false, false).unwrap();
        assert_eq!(MountTable::load(d.path()).unwrap().mounts.len(), 1);

        run(d.path(), false, false).unwrap();
        let after = MountTable::load(d.path()).unwrap();
        assert_eq!(
            after.mounts.len(),
            1,
            "a second migrate emptied the mount table - the directory would then serve nothing"
        );
        assert_eq!(after.mounts[0].alias, "a");
    }

    #[test]
    fn migrating_twice_is_a_no_op() {
        let d = tempfile::tempdir().unwrap();
        write_roost(
            d.path(),
            &[("a", "0x0000000000000000000000000000000000000001")],
        );
        run(d.path(), false, false).unwrap();
        let after_first = std::fs::read_to_string(d.path().join(MOUNTS_FILE)).unwrap();
        let nid = MountTable::load(d.path()).unwrap().mounts[0].nid.clone();

        run(d.path(), false, false).unwrap();

        assert_eq!(
            after_first,
            std::fs::read_to_string(d.path().join(MOUNTS_FILE)).unwrap()
        );
        assert_eq!(MountTable::load(d.path()).unwrap().mounts[0].nid, nid);
        assert_eq!(
            crate::runtime::history_head(&d.path().join(DATA_DIR).join(&nid)),
            Some(fixture_head(0))
        );
    }

    /// Two aliases over byte-identical inputs are one nest that was indexed twice. The migration is
    /// where that becomes visible, and both aliases end up on one dataset.
    #[test]
    fn two_names_for_one_nest_merge_into_one_dataset() {
        let d = tempfile::tempdir().unwrap();
        // Same contract, same ABI - the nest *name* differs, and a nest's name is in its config, so
        // write both configs identically and let the alias differ only in the runtime.
        write_roost(
            d.path(),
            &[
                ("a", "0x0000000000000000000000000000000000000001"),
                ("clone", "0x0000000000000000000000000000000000000001"),
            ],
        );
        // Make the inputs byte-identical: `nest.name` is an authored input, so it must match too.
        let a = std::fs::read_to_string(d.path().join(NESTS_DIR).join("a").join("nuthatch.toml"))
            .unwrap();
        std::fs::write(
            d.path().join(NESTS_DIR).join("clone").join("nuthatch.toml"),
            &a,
        )
        .unwrap();

        let plans = plan(d.path()).unwrap();
        assert!(
            matches!(plans[1], Plan::Merge { .. }),
            "identical inputs under two aliases must be detected as one nest, got {:?}",
            plans[1]
        );

        run(d.path(), false, false).unwrap();
        let mounts = MountTable::load(d.path()).unwrap();
        assert_eq!(
            mounts.mounts[0].nid, mounts.mounts[1].nid,
            "both aliases must land on one identity"
        );
        assert_eq!(
            mounts.dir_for(d.path(), "a"),
            mounts.dir_for(d.path(), "clone"),
            "two doors, one room"
        );
    }

    /// RFC-0033 §5, slice 5's stated acceptance: **a cosmetic edit re-indexes nothing.**
    ///
    /// The nest is migrated, then edited in a way that cannot change a stored byte (a doc change and
    /// an authored view, neither of which the indexing path reads). The NID moves - it must, the
    /// package really did change - and the new identity adopts the existing dataset instead of
    /// resolving to an empty directory and re-backfilling the chain.
    #[test]
    fn a_cosmetic_edit_adopts_the_existing_dataset_instead_of_re_indexing() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        write_roost(root, &[("a", "0x0000000000000000000000000000000000000001")]);
        std::fs::write(root.join(NESTS_DIR).join("a").join("llms.txt"), "docs v1\n").unwrap();
        run(root, false, false).unwrap();

        let first = MountTable::load(root).unwrap().mounts[0].nid.clone();
        let dataset = root.join(DATA_DIR).join(&first);
        assert_eq!(
            crate::runtime::history_head(&dataset),
            Some(fixture_head(0)),
            "the fixture's indexed history should be in place"
        );

        // Stage a "new" nest the way a re-published bundle would arrive, **then** make the cosmetic
        // edit in the staged copy only. Editing before the copy would leave both sides identical and
        // the test would pass without the exclusion list doing anything - which is exactly how the
        // first version of this test passed against a build with the exclusions removed.
        let staged = root.join(NESTS_DIR).join("a");
        crate::project::copy_dir(&dataset, &staged).unwrap();
        std::fs::remove_file(staged.join("nuthatch.redb")).ok(); // a fresh bundle carries no data
        std::fs::write(staged.join("llms.txt"), "docs v2, completely rewritten\n").unwrap();
        std::fs::create_dir_all(staged.join("views")).unwrap();
        std::fs::write(
            staged.join("views/10-v.sql"),
            "CREATE VIEW v AS SELECT 1 -- authored logic, never materialised",
        )
        .unwrap();
        let mut mounts = MountTable::load(root).unwrap();
        mounts.mounts.clear();
        mounts.runtime.nests = vec!["a".into()];
        std::fs::write(
            root.join(MOUNTS_FILE),
            toml::to_string_pretty(&mounts).unwrap(),
        )
        .unwrap();

        let plans = plan(root).unwrap();
        assert!(
            matches!(plans[0], Plan::Adopt { .. }),
            "a cosmetic edit must adopt, not move an empty staging dir: {:?}",
            plans[0]
        );
        let Plan::Adopt { nid, from_nid, .. } = &plans[0] else {
            unreachable!()
        };
        assert_ne!(nid, &first, "the NID must move - the package changed");
        assert_eq!(
            from_nid, &first,
            "and it must adopt the dataset it came from"
        );

        run(root, false, false).unwrap();
        let after = MountTable::load(root).unwrap().mounts[0].nid.clone();
        assert_ne!(after, first);
        assert_eq!(
            crate::runtime::history_head(&root.join(DATA_DIR).join(&after)),
            Some(fixture_head(0)),
            "the indexed history must be present under the new identity - if it is absent, the \
             nest re-backfills, which is the entire cost early cutoff exists to remove"
        );
        assert!(
            root.join(DATA_DIR).join(&first).is_dir(),
            "adoption must COPY - the source may still be mounted by another tenant"
        );
    }

    /// The dangerous direction: a nest whose inputs genuinely change the data must **not** adopt.
    #[test]
    fn a_substantive_edit_does_not_adopt() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        write_roost(root, &[("a", "0x0000000000000000000000000000000000000001")]);
        run(root, false, false).unwrap();
        let first = MountTable::load(root).unwrap().mounts[0].nid.clone();

        // A different contract address: different data, whatever the rest of the nest says.
        let staged = root.join(NESTS_DIR).join("a");
        crate::project::copy_dir(&root.join(DATA_DIR).join(&first), &staged).unwrap();
        let cfg = std::fs::read_to_string(staged.join("nuthatch.toml")).unwrap();
        std::fs::write(
            staged.join("nuthatch.toml"),
            cfg.replace(
                "0x0000000000000000000000000000000000000001",
                "0x0000000000000000000000000000000000000002",
            ),
        )
        .unwrap();
        let mut mounts = MountTable::load(root).unwrap();
        mounts.mounts.clear();
        mounts.runtime.nests = vec!["a".into()];
        std::fs::write(
            root.join(MOUNTS_FILE),
            toml::to_string_pretty(&mounts).unwrap(),
        )
        .unwrap();

        assert!(
            matches!(plan(root).unwrap()[0], Plan::Move { .. }),
            "a different contract must not adopt another contract's data"
        );
    }

    /// RFC-0033 §9, slice 6: **the runtime tells you a change is breaking, at the moment it happens.**
    ///
    /// `nest diff` carried this information and relied on somebody remembering to run it. Grafting
    /// makes the *data* free; it says nothing about whether a consumer's query still resolves. So the
    /// classification moves to the moment the identity actually changes, and stops the run by default.
    #[test]
    fn a_breaking_schema_change_is_named_and_refused() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        write_roost(root, &[("a", "0x0000000000000000000000000000000000000001")]);
        let schema = r#"{"tables":[{"table":"a__transfer","columns":[{"name":"from","sol_type":"address"},{"name":"value","sol_type":"uint256"}]}]}"#;
        std::fs::write(root.join(NESTS_DIR).join("a").join("schema.json"), schema).unwrap();
        run(root, false, false).unwrap();
        let nid = MountTable::load(root).unwrap().mounts[0].nid.clone();

        // Re-stage the same alias with a column dropped: compatible data, broken consumers.
        let staged = root.join(NESTS_DIR).join("a");
        crate::project::copy_dir(&root.join(DATA_DIR).join(&nid), &staged).unwrap();
        std::fs::write(
            staged.join("schema.json"),
            r#"{"tables":[{"table":"a__transfer","columns":[{"name":"from","sol_type":"address"}]}]}"#,
        )
        .unwrap();

        let plans = plan(root).unwrap();
        let Plan::Move { breaking, .. } = &plans[0] else {
            panic!("expected a Move, got {:?}", plans[0]);
        };
        let reasons = breaking
            .as_ref()
            .expect("the drop must be classified as breaking");
        assert!(
            reasons.iter().any(|r| r.contains("value")),
            "the incompatibility must be named, not merely counted: {reasons:?}"
        );

        // Refused by default, and nothing moved.
        let err = run(root, false, false).unwrap_err().to_string();
        assert!(err.contains("break consumers"), "{err}");
        assert!(err.contains("Nothing was changed"), "{err}");
        assert!(
            err.contains("--allow-breaking"),
            "the refusal must name the way forward: {err}"
        );
        assert_eq!(
            MountTable::load(root).unwrap().mounts[0].nid,
            nid,
            "a refused run must not have changed the mount"
        );

        // ...and it proceeds when the operator says so.
        run(root, false, true).expect("--allow-breaking proceeds");
        assert_ne!(MountTable::load(root).unwrap().mounts[0].nid, nid);
    }

    /// The control: a **compatible** change must not be flagged, or the warning becomes noise and
    /// gets ignored - which is worse than not having it.
    #[test]
    fn an_additive_schema_change_is_not_breaking() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        write_roost(root, &[("a", "0x0000000000000000000000000000000000000001")]);
        std::fs::write(
            root.join(NESTS_DIR).join("a").join("schema.json"),
            r#"{"tables":[{"table":"a__transfer","columns":[{"name":"from","sol_type":"address"}]}]}"#,
        )
        .unwrap();
        run(root, false, false).unwrap();
        let nid = MountTable::load(root).unwrap().mounts[0].nid.clone();

        let staged = root.join(NESTS_DIR).join("a");
        crate::project::copy_dir(&root.join(DATA_DIR).join(&nid), &staged).unwrap();
        std::fs::write(
            staged.join("schema.json"),
            r#"{"tables":[{"table":"a__transfer","columns":[{"name":"from","sol_type":"address"},{"name":"added","sol_type":"address"}]}]}"#,
        )
        .unwrap();

        let Plan::Move { breaking, .. } = &plan(root).unwrap()[0] else {
            panic!("expected a Move");
        };
        assert!(
            breaking.is_none(),
            "an added column is compatible and must not be flagged: {breaking:?}"
        );
        run(root, false, false).expect("a compatible change proceeds without a flag");
    }

    /// RFC-0033 §11a slice C: **two nests with byte-identical segments hold one copy on disk.**
    ///
    /// This is the cross-nest reuse chris asked for ("if two entity hashes across any nest are the
    /// same, you can reuse the data across"), and note what makes it work: the nests differ - two
    /// names, two identities, two datasets - and still share the bytes, because the *segment* is what
    /// they have in common, not the nest.
    #[test]
    fn two_different_nests_with_identical_segments_hold_one_copy() {
        use crate::seal::{Manifest, Segment, SEGMENTS_DIR};
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        write_roost(
            root,
            &[
                ("alpha", "0x0000000000000000000000000000000000000001"),
                ("beta", "0x0000000000000000000000000000000000000002"),
            ],
        );

        // Both nests sealed the same rows, so both hold the same segment bytes under the same hash -
        // exactly what RFC-0009's determinism guarantees for the same contract and binary.
        let hash = "ab".repeat(32);
        let bytes = b"identical parquet bytes";
        for alias in ["alpha", "beta"] {
            let seg_dir = root.join(NESTS_DIR).join(alias).join(SEGMENTS_DIR);
            std::fs::create_dir_all(&seg_dir).unwrap();
            let file = format!("t-{hash}.parquet");
            std::fs::write(seg_dir.join(&file), bytes).unwrap();
            let mut m = Manifest::default();
            m.tables.insert(
                "t".to_string(),
                vec![Segment {
                    hash: hash.clone(),
                    from_block: 1,
                    to_block: 2,
                    rows: 1,
                    file,
                    registry_snapshot: None,
                    provisional: false,
                    writer_profile: crate::seal::ORIGINAL_WRITER_PROFILE.to_string(),
                    input_hash: None,
                }],
            );
            std::fs::write(
                seg_dir.join("manifest.json"),
                serde_json::to_string(&m).unwrap(),
            )
            .unwrap();
        }

        run(root, false, false).unwrap();

        // Two distinct datasets...
        let table = MountTable::load(root).unwrap();
        assert_eq!(table.mounts.len(), 2);
        assert_ne!(
            table.mounts[0].nid, table.mounts[1].nid,
            "two different nests must keep two identities"
        );

        // ...and exactly one copy of the shared bytes.
        let store = root.join(SEGMENTS_DIR);
        let copies: Vec<_> = std::fs::read_dir(&store)
            .unwrap()
            .flatten()
            .filter(|e| e.path().extension().is_some_and(|x| x == "parquet"))
            .collect();
        assert_eq!(
            copies.len(),
            1,
            "two nests with identical segments should hold ONE copy, got {copies:?}"
        );
        assert_eq!(
            std::fs::read(store.join(format!("{hash}.parquet"))).unwrap(),
            bytes,
            "the shared copy must be the same bytes both nests sealed"
        );
    }

    /// The bug the Lodestar box found, and the tempdir test missed (RFC-0033 §11a slice C).
    ///
    /// Two nests migrated from **separately copied** directories hold two *distinct inodes* of
    /// identical bytes - which is what any real deployment looks like. Linking one of them into the
    /// shared store only avoids creating a third copy; the duplication between the two datasets
    /// survives, and the disk does not move. Measured on 881 MB of real production data: 251 segments
    /// "shared", zero bytes reclaimed.
    ///
    /// The earlier test passed because its fixture only ever had one copy to begin with.
    #[test]
    fn migrating_two_nests_actually_reclaims_the_duplicate_bytes() {
        use crate::seal::{Manifest, Segment, SEGMENTS_DIR};
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        write_roost(
            root,
            &[
                ("alpha", "0x0000000000000000000000000000000000000001"),
                ("beta", "0x0000000000000000000000000000000000000002"),
            ],
        );

        // The **real** hash of the bytes, not a made-up one: `safe_to_drop_local` re-hashes the
        // shared copy before removing a local one, and correctly refuses when they disagree. A
        // fixture with a fake hash exercises the refusal, not the reclaim.
        let bytes = vec![7u8; 4096];
        let hash = hex::encode(sha2::Sha256::digest(&bytes));
        for alias in ["alpha", "beta"] {
            let seg_dir = root.join(NESTS_DIR).join(alias).join(SEGMENTS_DIR);
            std::fs::create_dir_all(&seg_dir).unwrap();
            let file = format!("t-{hash}.parquet");
            // Written separately, so these are two distinct inodes - exactly as two nests copied
            // onto a box would be.
            std::fs::write(seg_dir.join(&file), &bytes).unwrap();
            let mut m = Manifest::default();
            m.tables.insert(
                "t".to_string(),
                vec![Segment {
                    hash: hash.clone(),
                    from_block: 1,
                    to_block: 2,
                    rows: 1,
                    file,
                    registry_snapshot: None,
                    provisional: false,
                    writer_profile: crate::seal::ORIGINAL_WRITER_PROFILE.to_string(),
                    input_hash: None,
                }],
            );
            std::fs::write(
                seg_dir.join("manifest.json"),
                serde_json::to_string(&m).unwrap(),
            )
            .unwrap();
        }

        let count_parquet = |p: &Path| -> usize {
            walkdir(p)
                .into_iter()
                .filter(|f| f.ends_with(".parquet"))
                .count()
        };
        assert_eq!(
            count_parquet(root),
            2,
            "the fixture must start with two copies"
        );

        run(root, false, false).unwrap();

        // One copy, in the shared store, and **no per-dataset duplicates left behind**.
        assert_eq!(
            count_parquet(root),
            1,
            "two nests with identical segments must end up holding ONE copy - linking without \
             dropping the local copy reclaims nothing, which is what the box proved"
        );
        assert!(
            root.join(SEGMENTS_DIR)
                .join(format!("{hash}.parquet"))
                .exists(),
            "the surviving copy must be the shared one"
        );
        assert_eq!(
            std::fs::read(root.join(SEGMENTS_DIR).join(format!("{hash}.parquet"))).unwrap(),
            bytes,
            "and it must still be the right bytes"
        );

        // Both datasets still resolve their segment through the shared store.
        let table = MountTable::load(root).unwrap();
        assert_eq!(table.mounts.len(), 2);
        for m in &table.mounts {
            let ds = MountTable::data_dir(root, &m.nid);
            let p = crate::seal::segment_path(&ds, &format!("t-{hash}.parquet"), &hash);
            assert!(
                p.exists(),
                "dataset {} cannot find its segment: {p:?}",
                &m.nid[..8]
            );
        }
    }

    /// The guard on the deletion, pinned because it is the difference between reclaiming a duplicate
    /// and destroying an operator's only copy.
    ///
    /// This was found the useful way round: the first version of the test above used a made-up hash,
    /// and `safe_to_drop_local` refused to delete anything - correctly.
    #[test]
    fn a_local_copy_is_never_dropped_against_an_unverifiable_shared_one() {
        let d = tempfile::tempdir().unwrap();
        let bytes = b"the real bytes";
        let real = hex::encode(sha2::Sha256::digest(bytes));

        let src = d.path().join("local.parquet");
        let dest = d.path().join("shared.parquet");
        std::fs::write(&src, bytes).unwrap();

        // Shared copy absent: nothing to fall back to, so keep the local one.
        assert!(!safe_to_drop_local(&src, &dest, &real));

        // Shared copy present but **different bytes**: refuse. This is the case that would otherwise
        // delete good data in favour of bad.
        std::fs::write(&dest, b"different bytes entirely").unwrap();
        assert!(
            !safe_to_drop_local(&src, &dest, &real),
            "a shared copy whose hash does not match the manifest must not authorise a deletion"
        );

        // Shared copy present and correct: now it is safe.
        std::fs::write(&dest, bytes).unwrap();
        assert!(safe_to_drop_local(&src, &dest, &real));

        // Same inode is safe without re-reading anything - unlinking one name of a two-name file
        // cannot lose the bytes.
        let linked = d.path().join("linked.parquet");
        std::fs::hard_link(&src, &linked).unwrap();
        assert!(safe_to_drop_local(&src, &linked, "not-even-a-hash"));
    }

    /// A refusal must not hold the healthy nests hostage, and the runtime must still serve.
    #[test]
    fn a_broken_nest_is_refused_and_the_rest_still_migrate() {
        let d = tempfile::tempdir().unwrap();
        write_roost(
            d.path(),
            &[
                ("good", "0x0000000000000000000000000000000000000001"),
                ("broken", "0x0000000000000000000000000000000000000002"),
            ],
        );
        // Remove the config so the nest cannot be hashed at all.
        std::fs::remove_file(
            d.path()
                .join(NESTS_DIR)
                .join("broken")
                .join("nuthatch.toml"),
        )
        .unwrap();

        let err = run(d.path(), false, false).unwrap_err().to_string();
        assert!(
            err.contains("broken"),
            "the refusal must name the nest: {err}"
        );

        let mounts = MountTable::load(d.path()).unwrap();
        assert_eq!(
            mounts.mounts.len(),
            1,
            "the healthy nest should have migrated"
        );
        assert_eq!(mounts.mounts[0].alias, "good");
        // Mixed layout: one resolved by identity, one still by name. Both resolve.
        assert!(mounts
            .dir_for(d.path(), "good")
            .starts_with(d.path().join(DATA_DIR)));
        assert!(mounts
            .dir_for(d.path(), "broken")
            .starts_with(d.path().join(NESTS_DIR)));
    }

    /// #2362: an adopt takes the source's data and nothing it authored, so a file the new version
    /// deleted stays deleted and the dataset hashes to the NID its mount record claims.
    #[test]
    fn an_adopt_does_not_bring_back_a_file_the_new_version_deleted() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path();
        write_roost(root, &[("a", "0x0000000000000000000000000000000000000001")]);
        let nest = root.join(NESTS_DIR).join("a");
        std::fs::create_dir_all(nest.join("views")).unwrap();
        std::fs::write(nest.join("views/old.sql"), "CREATE VIEW old AS SELECT 1").unwrap();
        run(root, false, false).unwrap();
        let first = MountTable::load(root).unwrap().mounts[0].nid.clone();
        let dataset = root.join(DATA_DIR).join(&first);
        let data = std::fs::read(dataset.join(crate::config::DB_FILE)).unwrap();

        // The new version drops the view: same data identity, new NID.
        let staged = root.join(NESTS_DIR).join("a");
        crate::project::copy_dir(&dataset, &staged).unwrap();
        std::fs::remove_file(staged.join(crate::config::DB_FILE)).unwrap();
        std::fs::remove_file(staged.join("views/old.sql")).unwrap();
        let staged_nid = nid_of(&staged).unwrap();
        let mut mounts = MountTable::load(root).unwrap();
        mounts.mounts.clear();
        mounts.runtime.nests = vec!["a".into()];
        std::fs::write(
            root.join(MOUNTS_FILE),
            toml::to_string_pretty(&mounts).unwrap(),
        )
        .unwrap();
        assert!(matches!(plan(root).unwrap()[0], Plan::Adopt { .. }));
        run(root, false, false).unwrap();

        let after = MountTable::load(root).unwrap().mounts[0].nid.clone();
        assert_eq!(after, staged_nid);
        let new_dir = root.join(DATA_DIR).join(&after);
        assert!(!new_dir.join("views/old.sql").exists());
        assert_eq!(
            std::fs::read(new_dir.join(crate::config::DB_FILE)).unwrap(),
            data
        );
        assert_eq!(crate::blob::nest_nid(&new_dir).unwrap(), after);
    }
}
