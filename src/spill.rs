//! A session's private spill directory (#1165): made exclusively, removed when the session is
//! dropped, and swept by pid after a process that died holding one.

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// One session's spill directory. Two sessions writing one directory overwrite each other's
/// spilled blocks, which under DuckDB was 25 SEGVs in a minute at four permits; the cache hands a
/// session to a query for its whole duration, so a concurrent query opens a session of its own.
pub(crate) struct SpillDir(pub(crate) PathBuf);

impl Drop for SpillDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Remove spill directories left by processes that are no longer running.
///
/// The cache lives in a `static`, and Rust does not drop statics at exit, so a cached connection's
/// [`SpillDir`] guard never runs on the way out and its directory outlives the process. One empty
/// directory per instance is not much, but a nest restarting every fifteen seconds under a fault -
/// which is exactly what #1165 looked like - would leave one behind each time, with whatever it had
/// spilled inside. Ownership is by pid: a directory whose pid still has a `/proc` entry belongs to a
/// live process and is left alone. Where `/proc` is not there to ask (macOS, dev machines), nothing
/// is swept, because guessing by age could delete a running instance's spill under it.
fn sweep_dead_spill_dirs(parent: &Path) {
    if !Path::new("/proc").is_dir() {
        return;
    }
    let Ok(entries) = std::fs::read_dir(parent) else {
        return;
    };
    for e in entries.flatten() {
        let Some(pid) = spill_dir_pid(&e.file_name().to_string_lossy()) else {
            continue;
        };
        if !Path::new(&format!("/proc/{pid}")).exists() {
            let _ = std::fs::remove_dir_all(e.path());
        }
    }
}

pub(crate) const PREFIX: &str = "nuthatch-spill-";

/// Whether a name is one spill makes, `<prefix><pid>-<seq>` in ASCII digits and nothing else. The
/// second prefix is what releases before 4.1 called theirs. `engine.js` matches the same shape.
pub(crate) fn is_spill_dir_name(name: &str) -> bool {
    let Some(rest) = name
        .strip_prefix(PREFIX)
        .or_else(|| name.strip_prefix("nuthatch-duckdb-"))
    else {
        return false;
    };
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    rest.split_once('-')
        .is_some_and(|(pid, seq)| digits(pid) && digits(seq))
}

/// The pid a spill directory's name carries, or `None` for a name spill did not make.
fn spill_dir_pid(name: &str) -> Option<u32> {
    if !is_spill_dir_name(name) {
        return None;
    }
    let rest = name.split_once('-')?.1.split_once('-')?.1;
    rest.split('-').next()?.parse().ok()
}

/// The in-process half of a spill directory's name; the other half is the PID.
pub(crate) static SPILL_SEQ: AtomicU64 = AtomicU64::new(0);

fn spill_parent() -> PathBuf {
    crate::analytics_budget::from_env()
        .temp_directory
        .unwrap_or_else(default_spill_parent)
}

/// Where spill goes unless `analytics.temp_directory` says. Not the process temp dir on Linux: that is
/// often a tmpfs, where spill is RAM the per-cursor budget does not count (Jules on #1583). The user's
/// cache directory is on disk; the temp dir remains the fallback where there is no home.
fn default_spill_parent() -> PathBuf {
    spill_parent_for(
        cfg!(target_os = "linux"),
        std::env::var_os("XDG_CACHE_HOME"),
        std::env::var_os("HOME"),
    )
}

fn spill_parent_for(
    linux: bool,
    xdg_cache: Option<std::ffi::OsString>,
    home: Option<std::ffi::OsString>,
) -> PathBuf {
    if linux {
        let cache = xdg_cache
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
            .or_else(|| {
                home.filter(|v| !v.is_empty())
                    .map(|h| PathBuf::from(h).join(".cache"))
            });
        if let Some(cache) = cache {
            return cache.join("nuthatch");
        }
    }
    std::env::temp_dir()
}

/// A directory no other session in this process (or any other) will write to.
///
/// Created **exclusively**: `create_dir`, not `create_dir_all`, and a name that already exists is
/// skipped for the next sequence number. The name alone is not enough. A process that dies without
/// dropping its static cache leaves its directories behind, the sweep keeps any whose PID is live,
/// and a later process handed that same PID by the kernel would otherwise start its sequence at zero
/// and write into the dead process's `-0` - the collision this whole change exists to remove, back
/// through PID reuse. Refusing an existing path makes the directory this instance's by construction,
/// whatever is left on disk. The parent is `analytics.temp_directory` when set, else the process
/// temp dir; the instance name is unchanged (#1165).
pub(crate) fn new_spill_dir() -> Result<SpillDir> {
    static SWEPT: std::sync::Once = std::sync::Once::new();
    SWEPT.call_once(|| {
        // The temp dir too: it was the default parent before 3.13.2, and may hold dead ones.
        sweep_dead_spill_dirs(&std::env::temp_dir());
        let parent = spill_parent();
        if parent != std::env::temp_dir() {
            sweep_dead_spill_dirs(&parent);
        }
    });
    let parent = spill_parent();
    if !parent.exists() {
        std::fs::create_dir_all(&parent)
            .with_context(|| format!("creating the spill parent {}", parent.display()))?;
    }
    create_exclusive(&parent, &SPILL_SEQ)
}

fn create_exclusive(parent: &Path, seq: &AtomicU64) -> Result<SpillDir> {
    loop {
        let path = parent.join(format!(
            "{PREFIX}{}-{}",
            std::process::id(),
            seq.fetch_add(1, Ordering::Relaxed)
        ));
        match std::fs::create_dir(&path) {
            Ok(()) => return Ok(SpillDir(path)),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => {
                return Err(e)
                    .with_context(|| format!("creating the spill directory {}", path.display()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #2663: exactly the names spill makes, so a nest file that only looks like one keeps counting
    /// toward the NID. `engine.js`'s `SPILL_DIR` must agree with every row.
    #[test]
    fn only_the_names_spill_makes_are_spill_names() {
        for (name, spill) in [
            ("nuthatch-spill-4242-0", true),
            ("nuthatch-duckdb-4242-17", true),
            ("nuthatch-spill-99999999999-0", true),
            ("nuthatch-spill-4242", false),
            ("nuthatch-spill-1-notes.md", false),
            ("nuthatch-spill-+5-0", false),
            ("nuthatch-spill--0", false),
            ("nuthatch-spill-notes.md", false),
            ("nuthatch-spillover-1-0", false),
        ] {
            assert_eq!(is_spill_dir_name(name), spill, "{name}");
        }
        assert_eq!(spill_dir_pid("nuthatch-spill-4242-0"), Some(4242));
        assert_eq!(spill_dir_pid("nuthatch-spill-99999999999-0"), None);
    }

    /// Jules on #1583: on Linux the temp dir is often a tmpfs, where spill is RAM outside the budget.
    #[test]
    fn spill_defaults_to_the_cache_dir_on_linux_and_the_temp_dir_without_a_home() {
        let s = |x: &str| Some(std::ffi::OsString::from(x));
        assert_eq!(
            spill_parent_for(true, s("/x"), s("/h")),
            PathBuf::from("/x/nuthatch")
        );
        assert_eq!(
            spill_parent_for(true, None, s("/h")),
            PathBuf::from("/h/.cache/nuthatch")
        );
        assert_eq!(spill_parent_for(true, s(""), None), std::env::temp_dir());
        assert_eq!(
            spill_parent_for(false, s("/x"), s("/h")),
            std::env::temp_dir()
        );
    }

    /// Jules on #1182: the spill path was keyed by PID and an in-process counter and created with
    /// `create_dir_all`, so a directory left by a dead process whose PID the kernel handed back would
    /// be reused, and two instances would share one spill directory again. A name that exists is
    /// refused and the sequence advances; the pre-made directories stand in for the dead process's.
    /// Mutation-checked: with `create_dir_all` back in place this fails.
    ///
    /// Its own parent and counter (#1956): every Burrmill session in the test binary draws from
    /// `SPILL_SEQ`, and one that took a planted name deleted the planted file when it dropped.
    #[test]
    fn a_spill_directory_that_already_exists_is_never_reused() {
        let parent = tempfile::tempdir().unwrap();
        let seq = AtomicU64::new(0);
        let planted: Vec<PathBuf> = (0..8)
            .map(|n| {
                parent
                    .path()
                    .join(format!("{PREFIX}{}-{n}", std::process::id()))
            })
            .collect();
        for p in &planted {
            std::fs::create_dir(p).unwrap();
            std::fs::write(p.join("someone-elses.tmp"), b"x").unwrap();
        }
        let mine = create_exclusive(parent.path(), &seq).unwrap();
        assert!(
            !planted.contains(&mine.0),
            "an existing directory was handed out as a fresh spill directory: {}",
            mine.0.display()
        );
        assert!(mine.0.exists() && mine.0.read_dir().unwrap().next().is_none());
        for p in &planted {
            assert!(
                p.join("someone-elses.tmp").exists(),
                "the other process's spill file was disturbed"
            );
        }
    }
}
