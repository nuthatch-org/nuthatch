//! Asynchronous live mounts (#1544).
//!
//! A mount that fetches its nest from a registry can take minutes, longer than an HTTP client will
//! wait, and a caller that timed out had no record of what happened. A mount is now a job: accepted,
//! then fetching, then joining its cursor, then live or failed with a reason. The jobs sit beside the
//! runtime's lock rather than behind it, so reading one never waits on the mount it describes.
//!
//! Unfinished and failed jobs are written to [`JOBS_FILE`], so a restart resumes the one and still
//! reports the other. A live mount is already recorded in `mounts.toml`, which is where a restart
//! reads it from, so a live job is kept in memory only.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub use crate::distribution::FailureCode;

/// Where unfinished and failed mount jobs are kept, beside `mounts.toml`.
pub const JOBS_FILE: &str = "mount-jobs.json";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MountPhase {
    Accepted,
    Fetching,
    /// Filling the dataset from its mirror before it indexes (RFC-0073 FR16).
    Seeding,
    Joining,
    Live,
    Failed,
    /// Suspended by the operator (#1548): off its cursor, answering 503, until resumed.
    Suspended,
}

impl MountPhase {
    pub fn finished(self) -> bool {
        matches!(
            self,
            MountPhase::Live | MountPhase::Failed | MountPhase::Suspended
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MountJob {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nid: Option<String>,
    pub phase: MountPhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Why a failed job failed, in a form a caller can branch on (RFC-0073 FR15).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<FailureCode>,
    pub since_unixtime: u64,
    /// A move to `nid` rather than a mount of it, so a restart resumes it as a move (#1549).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub is_move: bool,
    /// Which claim this record is. `forget` drops the record; a later claim must not reuse this,
    /// or the worker that was forgotten mounts over the new one (#1638).
    #[serde(default)]
    pub generation: u64,
    /// Seed the dataset from its mirror before it indexes (RFC-0073 FR16).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub seed: bool,
    /// The mirror the request named, kept so a restart resumes the job with it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publish: Option<crate::runtime::MountPublish>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed_progress: Option<crate::seed::SeedProgress>,
}

impl MountJob {
    pub fn new(name: &str, nid: Option<&str>, phase: MountPhase) -> MountJob {
        MountJob {
            name: name.to_string(),
            nid: nid.map(str::to_string),
            phase,
            reason: None,
            code: None,
            since_unixtime: now_unix(),
            is_move: false,
            generation: 0,
            seed: false,
            publish: None,
            seed_progress: None,
        }
    }
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Every mount the runtime knows by name: the live ones, and the jobs still running or failed.
pub struct MountJobs {
    file: PathBuf,
    jobs: std::sync::Mutex<BTreeMap<String, MountJob>>,
    /// The runtime's default tenant, for refusing a name that spells it out before a job starts.
    default_tenant: String,
    /// Never reused, including after `forget`, so a worker can tell its claim from the next one.
    next_generation: std::sync::atomic::AtomicU64,
}

impl MountJobs {
    /// Read the jobs a previous run left. A file that will not parse is reported and set aside rather
    /// than refusing the boot: losing a job's history is better than a runtime that will not start.
    pub fn load(dir: &Path) -> MountJobs {
        let file = dir.join(JOBS_FILE);
        let jobs = match std::fs::read(&file) {
            Ok(raw) => match serde_json::from_slice::<Vec<MountJob>>(&raw) {
                Ok(list) => list.into_iter().map(|j| (j.name.clone(), j)).collect(),
                Err(e) => {
                    tracing::warn!(
                        "{} is unreadable ({e}); earlier mount jobs are not resumed",
                        file.display()
                    );
                    BTreeMap::new()
                }
            },
            Err(_) => BTreeMap::new(),
        };
        let next = jobs
            .values()
            .map(|j| j.generation)
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        MountJobs {
            file,
            jobs: std::sync::Mutex::new(jobs),
            default_tenant: crate::runtime::DEFAULT_TENANT.to_string(),
            next_generation: std::sync::atomic::AtomicU64::new(next),
        }
    }

    pub fn with_default_tenant(mut self, tenant: &str) -> MountJobs {
        self.default_tenant = tenant.to_string();
        self
    }

    pub fn default_tenant(&self) -> &str {
        &self.default_tenant
    }

    pub fn get(&self, name: &str) -> Option<MountJob> {
        self.jobs.lock().unwrap().get(name).cloned()
    }

    pub fn list(&self) -> Vec<MountJob> {
        self.jobs.lock().unwrap().values().cloned().collect()
    }

    /// The jobs a restart must pick up again.
    pub fn unfinished(&self) -> Vec<MountJob> {
        self.list()
            .into_iter()
            .filter(|j| !j.phase.finished())
            .collect()
    }

    pub fn put(&self, job: MountJob) -> Result<()> {
        let mut jobs = self.jobs.lock().unwrap();
        jobs.insert(job.name.clone(), job);
        self.persist(&jobs)
    }

    /// Record `job` unless one is already running for its name, which is returned instead. One step
    /// under the lock, so of several identical requests exactly one starts a worker.
    ///
    /// A failed write rolls the claim back. No worker was started, so a retry must be able to take
    /// the name. A finished job this claim replaced is put back.
    pub fn claim(&self, mut job: MountJob) -> std::result::Result<MountJob, ClaimError> {
        let mut jobs = self.jobs.lock().unwrap();
        if let Some(running) = jobs.get(&job.name).filter(|j| !j.phase.finished()) {
            return Err(ClaimError::Running(Box::new(running.clone())));
        }
        job.generation = self
            .next_generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let previous = jobs.insert(job.name.clone(), job.clone());
        if let Err(e) = self.persist(&jobs) {
            match previous {
                Some(old) => {
                    jobs.insert(job.name.clone(), old);
                }
                None => {
                    jobs.remove(&job.name);
                }
            }
            return Err(ClaimError::Persist(e));
        }
        Ok(job)
    }

    /// Whether `generation` is still the claim on `name`. False after `forget`, and false for a
    /// later claim of the same name.
    pub fn owns(&self, name: &str, generation: u64) -> bool {
        self.jobs
            .lock()
            .unwrap()
            .get(name)
            .is_some_and(|job| job.generation == generation)
    }

    /// [`advance`](Self::advance) that leaves a different claim alone (#1638).
    pub fn advance_if_owned(
        &self,
        name: &str,
        generation: u64,
        phase: MountPhase,
        reason: Option<String>,
    ) -> Result<()> {
        let mut jobs = self.jobs.lock().unwrap();
        if let Some(job) = jobs
            .get_mut(name)
            .filter(|job| job.generation == generation)
        {
            job.phase = phase;
            job.reason = reason;
            job.code = None;
            job.since_unixtime = now_unix();
            return self.persist(&jobs);
        }
        Ok(())
    }

    /// Record how far an owned job's seed has got. Kept in memory until the job next moves on: a
    /// restart reruns the seed, which counts again from what is on disk.
    pub fn seed_progress_if_owned(
        &self,
        name: &str,
        generation: u64,
        progress: &crate::seed::SeedProgress,
    ) {
        let mut jobs = self.jobs.lock().unwrap();
        if let Some(job) = jobs
            .get_mut(name)
            .filter(|job| job.generation == generation)
        {
            job.seed_progress = Some(progress.clone());
        }
    }

    /// End an owned job `failed` with `e`'s text and its [`FailureCode`].
    pub fn fail_if_owned(&self, name: &str, generation: u64, e: &anyhow::Error) -> Result<()> {
        let mut jobs = self.jobs.lock().unwrap();
        if let Some(job) = jobs
            .get_mut(name)
            .filter(|job| job.generation == generation)
        {
            job.phase = MountPhase::Failed;
            job.reason = Some(format!("{e:#}"));
            job.code = Some(FailureCode::of(e));
            job.since_unixtime = now_unix();
            return self.persist(&jobs);
        }
        Ok(())
    }

    /// Move a job on. A reason is kept only for a failure. Memory updates even when the file
    /// cannot be written, and the error is the caller's to report.
    pub fn advance(&self, name: &str, phase: MountPhase, reason: Option<String>) -> Result<()> {
        let mut jobs = self.jobs.lock().unwrap();
        if let Some(job) = jobs.get_mut(name) {
            job.phase = phase;
            job.reason = reason;
            job.code = None;
            job.since_unixtime = now_unix();
        }
        self.persist(&jobs)
    }

    pub fn forget(&self, name: &str) -> Result<()> {
        let mut jobs = self.jobs.lock().unwrap();
        if jobs.remove(name).is_some() {
            self.persist(&jobs)?;
        }
        Ok(())
    }

    /// The job is already changed in memory. The error says the file does not know, and the caller
    /// decides not to call that a success.
    fn persist(&self, jobs: &BTreeMap<String, MountJob>) -> Result<()> {
        let kept: Vec<&MountJob> = jobs
            .values()
            .filter(|j| j.phase != MountPhase::Live)
            .collect();
        write_atomically(&self.file, &kept).with_context(|| {
            format!(
                "mount jobs changed but {} could not be written; a restart will not see them",
                self.file.display()
            )
        })
    }
}

/// Why [`MountJobs::claim`] did not start a job. A running job is the caller's conflict. A failed
/// write leaves no claim: the name is free, or it still holds the finished job it had.
#[derive(Debug)]
pub enum ClaimError {
    Running(Box<MountJob>),
    Persist(anyhow::Error),
}

fn write_atomically(file: &Path, jobs: &[&MountJob]) -> Result<()> {
    let tmp = file.with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(jobs)?)
        .with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, file).with_context(|| format!("replacing {}", file.display()))?;
    Ok(())
}

/// Remove fetches a killed process left staged under `data/`. Only safe before any job runs.
pub fn clear_stale_fetches(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir.join(crate::runtime::DATA_DIR)) else {
        return;
    };
    for e in entries.flatten() {
        if e.file_name().to_string_lossy().starts_with(".fetch-") {
            if let Err(err) = std::fs::remove_dir_all(e.path()) {
                tracing::warn!("could not clear {}: {err}", e.path().display());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    /// Directory writes fail until this drops, which puts the mode it had back.
    struct ReadonlyDir {
        path: PathBuf,
        mode: u32,
    }

    impl ReadonlyDir {
        fn new(path: &Path) -> Self {
            let mode = std::fs::metadata(path).unwrap().permissions().mode();
            let mut perms = std::fs::metadata(path).unwrap().permissions();
            perms.set_readonly(true);
            std::fs::set_permissions(path, perms).unwrap();
            Self {
                path: path.to_path_buf(),
                mode,
            }
        }
    }

    impl Drop for ReadonlyDir {
        fn drop(&mut self) {
            let mut perms = std::fs::metadata(&self.path).unwrap().permissions();
            perms.set_mode(self.mode);
            let _ = std::fs::set_permissions(&self.path, perms);
        }
    }

    #[test]
    fn a_restart_resumes_unfinished_jobs_reports_failed_ones_and_forgets_live_ones() {
        let d = tempfile::tempdir().unwrap();
        let jobs = MountJobs::load(d.path());
        jobs.put(MountJob::new("a", Some("aa"), MountPhase::Fetching))
            .unwrap();
        jobs.put(MountJob::new("b", Some("bb"), MountPhase::Accepted))
            .unwrap();
        jobs.advance("b", MountPhase::Failed, Some("no such nid".into()))
            .unwrap();
        jobs.put(MountJob::new("c", None, MountPhase::Live))
            .unwrap();

        let again = MountJobs::load(d.path());
        let names: Vec<String> = again.unfinished().into_iter().map(|j| j.name).collect();
        assert_eq!(names, vec!["a".to_string()]);
        let b = again.get("b").expect("a failed job is still reported");
        assert_eq!(b.phase, MountPhase::Failed);
        assert_eq!(b.reason.as_deref(), Some("no such nid"));
        assert!(
            again.get("c").is_none(),
            "a live mount belongs to mounts.toml, not here"
        );

        again.forget("b").unwrap();
        assert!(MountJobs::load(d.path()).get("b").is_none());
    }

    /// RFC-0073 FR16: a restart resumes a seed with the mirror it was asked for.
    #[test]
    fn a_seed_job_keeps_its_mirror_through_a_restart() {
        let d = tempfile::tempdir().unwrap();
        let jobs = MountJobs::load(d.path());
        let mut job = MountJob::new("a", Some("aa"), MountPhase::Accepted);
        job.seed = true;
        job.publish = Some(crate::runtime::MountPublish {
            target: "s3://bucket/mirror".into(),
            interval: "60s".into(),
            parallelism: 4,
            followers: false,
            failover: None,
        });
        let job = jobs.claim(job).unwrap();
        jobs.advance_if_owned("a", job.generation, MountPhase::Seeding, None)
            .unwrap();

        let again = MountJobs::load(d.path());
        let resumed = again.unfinished();
        assert_eq!(resumed.len(), 1);
        assert_eq!(resumed[0].phase, MountPhase::Seeding);
        assert!(resumed[0].seed);
        assert_eq!(
            resumed[0].publish.as_ref().map(|p| p.target.as_str()),
            Some("s3://bucket/mirror")
        );
    }

    /// RFC-0073 FR15: a failure carries its code in the job, through a restart, and a retry clears it.
    #[test]
    fn a_failed_job_carries_its_code_and_a_retry_clears_it() {
        let d = tempfile::tempdir().unwrap();
        let jobs = MountJobs::load(d.path());
        let job = jobs
            .claim(MountJob::new("a", Some("aa"), MountPhase::Accepted))
            .unwrap();
        let denied = anyhow::Error::new(crate::distribution::RegistryError::for_test(
            FailureCode::AccessDenied,
        ))
        .context("fetching nid aa from the registry");
        jobs.fail_if_owned("a", job.generation, &denied).unwrap();

        let again = MountJobs::load(d.path()).get("a").unwrap();
        assert_eq!(again.code, Some(FailureCode::AccessDenied));
        assert_eq!(serde_json::json!(again)["code"], "access_denied");

        let untyped = anyhow::anyhow!("the chain's cursor has died");
        jobs.fail_if_owned("a", job.generation, &untyped).unwrap();
        assert_eq!(jobs.get("a").unwrap().code, Some(FailureCode::Other));

        jobs.advance_if_owned("a", job.generation, MountPhase::Fetching, None)
            .unwrap();
        let retried = jobs.get("a").unwrap();
        assert_eq!(retried.code, None);
        assert!(serde_json::json!(retried).get("code").is_none());
    }

    #[test]
    fn a_failed_jobs_write_is_not_reported_as_success() {
        let d = tempfile::tempdir().unwrap();
        std::fs::write(d.path().join(JOBS_FILE), b"[]").unwrap();
        let jobs = MountJobs::load(d.path());
        let _unlock = ReadonlyDir::new(d.path());

        let err = jobs
            .put(MountJob::new("a", None, MountPhase::Accepted))
            .expect_err("a mount-jobs.json that cannot be written is not a recorded job");
        let err = format!("{err:#}");
        assert!(
            err.contains("a restart will not see them"),
            "the caller must hear that the job is not durable: {err}"
        );
        assert_eq!(
            jobs.get("a").map(|j| j.phase),
            Some(MountPhase::Accepted),
            "the live job stays; only the report of success was wrong"
        );
        let on_disk = std::fs::read_to_string(d.path().join(JOBS_FILE)).unwrap();
        assert_eq!(on_disk, "[]", "the failed rewrite must leave the old table");
    }

    #[test]
    fn a_claim_whose_write_fails_does_not_block_the_retry() {
        let d = tempfile::tempdir().unwrap();
        std::fs::write(d.path().join(JOBS_FILE), b"[]").unwrap();
        let jobs = MountJobs::load(d.path());
        let unlock = ReadonlyDir::new(d.path());

        let err = jobs
            .claim(MountJob::new("a", None, MountPhase::Accepted))
            .expect_err("a claim that cannot be written is not a started job");
        assert!(
            matches!(err, ClaimError::Persist(_)),
            "the failure is the write, not a running job"
        );
        drop(unlock);
        assert!(
            jobs.claim(MountJob::new("a", None, MountPhase::Accepted))
                .is_ok(),
            "the failed claim must not still be running"
        );
    }

    #[test]
    fn a_claim_whose_write_fails_puts_the_finished_job_back() {
        let d = tempfile::tempdir().unwrap();
        let jobs = MountJobs::load(d.path());
        jobs.put(MountJob::new("a", Some("old"), MountPhase::Failed))
            .unwrap();
        let _unlock = ReadonlyDir::new(d.path());

        let err = jobs
            .claim(MountJob::new("a", Some("new"), MountPhase::Accepted))
            .expect_err("a claim that cannot be written is not a started job");
        assert!(matches!(err, ClaimError::Persist(_)));
        let kept = jobs.get("a").expect("the finished job stays");
        assert_eq!(kept.phase, MountPhase::Failed);
        assert_eq!(kept.nid.as_deref(), Some("old"));
    }

    #[test]
    fn only_one_of_several_identical_claims_wins() {
        let d = tempfile::tempdir().unwrap();
        let jobs = MountJobs::load(d.path());
        let job = || MountJob::new("race", Some("aa"), MountPhase::Accepted);
        assert!(jobs.claim(job()).is_ok());
        let running = jobs
            .claim(job())
            .expect_err("a second claim while the first runs");
        let ClaimError::Running(running) = running else {
            panic!("a second claim is the running job, not a write failure");
        };
        assert_eq!(running.phase, MountPhase::Accepted);
        jobs.advance("race", MountPhase::Failed, Some("x".into()))
            .unwrap();
        assert!(
            jobs.claim(job()).is_ok(),
            "a finished job does not block a new one"
        );
    }

    #[test]
    fn an_unreadable_jobs_file_does_not_stop_the_runtime() {
        let d = tempfile::tempdir().unwrap();
        std::fs::write(d.path().join(JOBS_FILE), b"{not json").unwrap();
        assert!(MountJobs::load(d.path()).list().is_empty());
    }

    #[test]
    fn stale_fetches_are_cleared_and_datasets_are_not() {
        let d = tempfile::tempdir().unwrap();
        let data = d.path().join(crate::runtime::DATA_DIR);
        std::fs::create_dir_all(data.join(".fetch-abc/nest")).unwrap();
        std::fs::create_dir_all(data.join("aa".repeat(32))).unwrap();
        clear_stale_fetches(d.path());
        assert!(!data.join(".fetch-abc").exists());
        assert!(data.join("aa".repeat(32)).is_dir());
    }
}
