//! `nuthatch dev --detach` and `nuthatch stop` (#2987): run `dev` in the background, return once it
//! serves, and stop it again. The log and the record of the process live outside the nest, because
//! every file inside one counts toward its NID.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::cli_error::{tag, Code};

/// How long a detached `dev` has to answer `/ready` with a first poll behind it.
const READY_WITHIN: Duration = Duration::from_secs(120);
/// How long `stop` waits for the process to finish its shutdown.
const STOP_WITHIN: Duration = Duration::from_secs(30);
const RECORD: &str = "dev.json";
const LOG: &str = "dev.log";

#[derive(Serialize, Deserialize)]
struct Record {
    pid: u32,
    dir: PathBuf,
    url: String,
    log: PathBuf,
}

/// `$XDG_STATE_HOME/nuthatch/dev/<hash of the nest's path>/`, else under `~/.local/state`.
pub fn state_dir_from(
    xdg_state: Option<std::ffi::OsString>,
    home: Option<std::ffi::OsString>,
    nest: &Path,
) -> Option<PathBuf> {
    use sha2::Digest as _;
    let base = xdg_state
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
        .or_else(|| {
            home.map(PathBuf::from)
                .filter(|p| p.is_absolute())
                .map(|h| h.join(".local").join("state"))
        })?;
    let key = hex::encode(&sha2::Sha256::digest(nest.as_os_str().as_encoded_bytes())[..8]);
    Some(base.join("nuthatch").join("dev").join(key))
}

fn state_dir(nest: &Path) -> Result<PathBuf> {
    state_dir_from(
        std::env::var_os("XDG_STATE_HOME"),
        std::env::var_os("HOME"),
        nest,
    )
    .context("--detach keeps its log in $XDG_STATE_HOME or $HOME, and neither is set")
}

fn nest_path(dir: &str) -> Result<PathBuf> {
    let path = std::fs::canonicalize(dir).with_context(|| format!("no directory at {dir}"));
    let path = path.map_err(|e| tag(Code::NoNest, e))?;
    let is_nest = [
        crate::config::CONFIG_FILE,
        crate::runtime::MOUNTS_FILE,
        crate::runtime::LEGACY_ROOST_FILE,
    ]
    .iter()
    .any(|f| path.join(f).exists());
    if !is_nest {
        return Err(tag(
            Code::NoNest,
            anyhow::anyhow!(
                "no {} or {} in {}",
                crate::config::CONFIG_FILE,
                crate::runtime::MOUNTS_FILE,
                path.display()
            ),
        ));
    }
    Ok(path)
}

/// Where to ask a server bound to `listen`: a wildcard bind answers on loopback.
fn local_url(listen: &str) -> String {
    let (host, port) = listen.rsplit_once(':').unwrap_or((listen, "8288"));
    let host = match host {
        "0.0.0.0" | "" => "127.0.0.1",
        "[::]" => "[::1]",
        h => h,
    };
    format!("http://{host}:{port}")
}

fn read_record(state: &Path) -> Option<Record> {
    let raw = std::fs::read_to_string(state.join(RECORD)).ok()?;
    serde_json::from_str(&raw).ok()
}

/// Whether `pid` is a running `nuthatch`. A recycled pid owned by something else is not ours to stop.
fn is_running_nuthatch(pid: u32) -> bool {
    let Ok(pid_t) = libc::pid_t::try_from(pid) else {
        return false;
    };
    // SAFETY: signal 0 checks for the process without sending anything.
    if unsafe { libc::kill(pid_t, 0) } != 0 {
        return false;
    }
    match std::process::Command::new("ps")
        .args(["-o", "stat=,comm=", "-p", &pid.to_string()])
        .output()
    {
        // A zombie has exited and only waits for its new parent to reap it.
        Ok(out) if out.status.success() => {
            let line = String::from_utf8_lossy(&out.stdout);
            let line = line.trim();
            !line.starts_with('Z') && line.ends_with("nuthatch")
        }
        // A process that exists, and no `ps` to say what it is.
        _ => true,
    }
}

fn log_tail(log: &Path, lines: usize) -> String {
    let text = std::fs::read_to_string(log).unwrap_or_default();
    let all: Vec<&str> = text.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

/// Whether a `/ready` answer means the nest is serving and has polled its chain once.
fn is_ready(status: u16, body: &serde_json::Value) -> bool {
    status == 200
        && body
            .get("last_poll_unixtime")
            .and_then(|v| v.as_u64())
            .is_none_or(|t| t > 0)
}

/// Start `nuthatch dev` with the same arguments, less `--detach` and `--json`, in its own session,
/// and return once it serves.
pub async fn start(dir: &str, listen: &str, json: bool) -> Result<()> {
    let nest = nest_path(dir)?;
    let state = state_dir(&nest)?;
    std::fs::create_dir_all(&state).with_context(|| format!("creating {}", state.display()))?;
    if let Some(r) = read_record(&state).filter(|r| is_running_nuthatch(r.pid)) {
        bail!(
            "nuthatch dev is already running for {} (pid {}, {}); `nuthatch stop --dir {}` stops it",
            nest.display(),
            r.pid,
            r.url,
            nest.display()
        );
    }
    let log_path = state.join(LOG);
    let log = std::fs::File::create(&log_path)
        .with_context(|| format!("creating {}", log_path.display()))?;
    let args: Vec<std::ffi::OsString> = std::env::args_os()
        .skip(1)
        .filter(|a| a != "--detach" && a != "--json")
        .collect();
    let mut cmd = std::process::Command::new(std::env::current_exe()?);
    cmd.args(&args)
        .stdin(std::process::Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log)
        .env("NO_COLOR", "1");
    {
        use std::os::unix::process::CommandExt as _;
        // SAFETY: setsid is async-signal-safe. A new session has no controlling terminal, so
        // closing the shell that started it sends it no SIGHUP.
        unsafe {
            cmd.pre_exec(|| {
                libc::setsid();
                Ok(())
            });
        }
    }
    let mut child = cmd.spawn().context("starting nuthatch dev")?;
    let url = local_url(listen);
    let record = Record {
        pid: child.id(),
        dir: nest.clone(),
        url: url.clone(),
        log: log_path.clone(),
    };
    std::fs::write(state.join(RECORD), serde_json::to_vec(&record)?)?;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()?;
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait()? {
            let _ = std::fs::remove_file(state.join(RECORD));
            let err = anyhow::anyhow!(
                "nuthatch dev exited ({status}) before it was serving. The end of {}:\n{}",
                log_path.display(),
                log_tail(&log_path, 20)
            );
            // The child's own status already names its failure class.
            let code = Code::ALL
                .into_iter()
                .find(|c| Some(c.exit_code()) == status.code())
                .unwrap_or(Code::Error);
            return Err(tag(code, err));
        }
        if let Ok(resp) = client.get(format!("{url}/ready")).send().await {
            let status = resp.status().as_u16();
            let body = resp.json().await.unwrap_or_default();
            if is_ready(status, &body) {
                break;
            }
        }
        if started.elapsed() > READY_WITHIN {
            let _ = child.kill();
            let _ = child.wait();
            let _ = std::fs::remove_file(state.join(RECORD));
            bail!(
                "nuthatch dev was not serving at {url} after {}s, so it was stopped. The end of {}:\n{}",
                READY_WITHIN.as_secs(),
                log_path.display(),
                log_tail(&log_path, 20)
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let stop = format!("nuthatch stop --dir {}", nest.display());
    if json {
        println!(
            "{}",
            serde_json::json!({
                "pid": record.pid,
                "url": url,
                "dir": nest,
                "log": log_path,
                "stop": stop,
            })
        );
    } else {
        println!("✓ nuthatch dev is serving at {url} (pid {})", record.pid);
        println!("  log:   {}", log_path.display());
        println!("  stop:  {stop}");
    }
    Ok(())
}

/// `nuthatch stop`: SIGTERM to the detached `dev` for `dir`, then wait for it to finish.
pub async fn stop(dir: &str, json: bool) -> Result<()> {
    let nest = std::fs::canonicalize(dir)
        .with_context(|| format!("no directory at {dir}"))
        .map_err(|e| tag(Code::NoNest, e))?;
    let state = state_dir(&nest)?;
    let Some(record) = read_record(&state).filter(|r| is_running_nuthatch(r.pid)) else {
        let _ = std::fs::remove_file(state.join(RECORD));
        return Err(tag(
            Code::NestNotRunning,
            anyhow::anyhow!(
                "no nuthatch dev started with --detach is running for {}",
                nest.display()
            ),
        ));
    };
    let pid = libc::pid_t::try_from(record.pid).context("pid out of range")?;
    // SAFETY: a plain signal to a pid just checked to be a running nuthatch.
    if unsafe { libc::kill(pid, libc::SIGTERM) } != 0 {
        bail!("could not signal pid {}", record.pid);
    }
    let started = Instant::now();
    while is_running_nuthatch(record.pid) {
        if started.elapsed() > STOP_WITHIN {
            bail!(
                "pid {} is still running {}s after SIGTERM; its log is {}",
                record.pid,
                STOP_WITHIN.as_secs(),
                record.log.display()
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let _ = std::fs::remove_file(state.join(RECORD));
    if json {
        println!(
            "{}",
            serde_json::json!({"stopped": record.pid, "dir": nest, "log": record.log})
        );
    } else {
        println!(
            "✓ stopped nuthatch dev (pid {}) for {}",
            record.pid,
            nest.display()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_state_lives_outside_the_nest_and_differs_per_nest() {
        let home = Some(std::ffi::OsString::from("/home/u"));
        let a = state_dir_from(None, home.clone(), Path::new("/n/a")).unwrap();
        let b = state_dir_from(None, home.clone(), Path::new("/n/b")).unwrap();
        assert!(a.starts_with("/home/u/.local/state/nuthatch/dev"), "{a:?}");
        assert_ne!(a, b);
        let xdg = state_dir_from(Some("/x".into()), home, Path::new("/n/a")).unwrap();
        assert!(xdg.starts_with("/x/nuthatch/dev"), "{xdg:?}");
        assert!(state_dir_from(Some("rel".into()), None, Path::new("/n/a")).is_none());
    }

    #[test]
    fn a_wildcard_bind_is_asked_on_loopback() {
        assert_eq!(local_url("0.0.0.0:9000"), "http://127.0.0.1:9000");
        assert_eq!(local_url("127.0.0.1:8288"), "http://127.0.0.1:8288");
        assert_eq!(local_url("[::]:8288"), "http://[::1]:8288");
    }

    #[test]
    fn ready_means_serving_with_a_first_poll_behind_it() {
        let polled = serde_json::json!({"ready": true, "last_poll_unixtime": 5});
        let unpolled = serde_json::json!({"ready": true, "last_poll_unixtime": 0});
        assert!(is_ready(200, &polled));
        assert!(!is_ready(200, &unpolled));
        assert!(!is_ready(503, &polled));
        // A runtime's root `/ready` carries no poll time of its own.
        assert!(is_ready(200, &serde_json::json!({"ready": true})));
    }
}
