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
    /// The executable spawned, which is what identifies the process later, whatever it is named.
    exe: PathBuf,
    /// When it started, so a later process given the same pid is not taken for it.
    started: u64,
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

/// What the pid in a record is now. A recycled pid running something else is not ours to stop.
#[derive(Debug, PartialEq)]
enum Identity {
    /// No such process, a zombie, another user's, or a later process given the same pid.
    Gone,
    /// The process started then, running the executable the record names.
    Ours,
    Other(PathBuf),
    /// The process started then, and what it runs could not be read: never signalled on a guess.
    Unconfirmed(String),
}

fn identify(pid: u32, exe: &Path, started: u64) -> Identity {
    let Ok(pid_t) = libc::pid_t::try_from(pid) else {
        return Identity::Gone;
    };
    // SAFETY: signal 0 checks for the process without sending anything.
    if unsafe { libc::kill(pid_t, 0) } != 0 {
        // EPERM is a process of another user, which this user did not start.
        return Identity::Gone;
    }
    match process_start(pid_t) {
        Ok(Some(t)) if t == started => {}
        Ok(_) => return Identity::Gone,
        Err(why) => return Identity::Unconfirmed(why),
    }
    match executable_of(pid_t) {
        Ok(path) if same_executable(&path, exe) => Identity::Ours,
        Ok(path) => Identity::Other(path),
        Err(why) => Identity::Unconfirmed(why),
    }
}

/// When the process started, in the kernel's own units, or `None` for a zombie: it has exited, and
/// waits only for a parent that may never reap it.
#[cfg(target_os = "linux")]
fn process_start(pid: libc::pid_t) -> std::result::Result<Option<u64>, String> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .map_err(|e| format!("/proc/{pid}/stat could not be read: {e}"))?;
    // The command name may hold spaces and parentheses; the fields after its last `)` do not.
    let rest = stat
        .rsplit_once(')')
        .map(|(_, r)| r)
        .ok_or_else(|| format!("/proc/{pid}/stat has no command name"))?;
    let fields: Vec<&str> = rest.split_whitespace().collect();
    // Field 3 is the state, field 22 the start time in clock ticks since boot.
    if fields.first() == Some(&"Z") {
        return Ok(None);
    }
    fields
        .get(19)
        .and_then(|t| t.parse().ok())
        .map(Some)
        .ok_or_else(|| format!("/proc/{pid}/stat has no start time"))
}

#[cfg(target_os = "macos")]
fn process_start(pid: libc::pid_t) -> std::result::Result<Option<u64>, String> {
    let mut info = std::mem::MaybeUninit::<libc::proc_bsdinfo>::zeroed();
    let size = std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int;
    // SAFETY: the buffer is a proc_bsdinfo, and its size is the size passed.
    let n = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTBSDINFO,
            0,
            info.as_mut_ptr().cast(),
            size,
        )
    };
    if n != size {
        let err = std::io::Error::last_os_error();
        // A zombie answers kill(pid, 0), and proc_pidinfo no longer finds it.
        if err.raw_os_error() == Some(libc::ESRCH) {
            return Ok(None);
        }
        return Err(format!("proc_pidinfo failed: {err}"));
    }
    // SAFETY: proc_pidinfo filled all of it.
    let info = unsafe { info.assume_init() };
    if info.pbi_status == libc::SZOMB {
        return Ok(None);
    }
    Ok(Some(
        info.pbi_start_tvsec * 1_000_000 + info.pbi_start_tvusec,
    ))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn process_start(_: libc::pid_t) -> std::result::Result<Option<u64>, String> {
    Err("this platform gives no way to read when a process started".into())
}

fn same_executable(running: &Path, recorded: &Path) -> bool {
    let canon = |p: &Path| std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf());
    running == recorded || canon(running) == canon(recorded)
}

#[cfg(target_os = "linux")]
fn executable_of(pid: libc::pid_t) -> std::result::Result<PathBuf, String> {
    let link = std::fs::read_link(format!("/proc/{pid}/exe"))
        .map_err(|e| format!("/proc/{pid}/exe could not be read: {e}"))?;
    // A binary replaced on disk since it started still names its old path.
    let shown = link.to_string_lossy();
    Ok(PathBuf::from(
        shown.strip_suffix(" (deleted)").unwrap_or(&shown),
    ))
}

#[cfg(target_os = "macos")]
fn executable_of(pid: libc::pid_t) -> std::result::Result<PathBuf, String> {
    use std::os::unix::ffi::OsStrExt as _;
    let mut buf = vec![0u8; libc::PROC_PIDPATHINFO_MAXSIZE as usize];
    // SAFETY: the buffer is as long as the size passed.
    let n = unsafe { libc::proc_pidpath(pid, buf.as_mut_ptr().cast(), buf.len() as u32) };
    if n <= 0 {
        return Err(format!(
            "proc_pidpath failed: {}",
            std::io::Error::last_os_error()
        ));
    }
    buf.truncate(n as usize);
    Ok(PathBuf::from(std::ffi::OsStr::from_bytes(&buf)))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn executable_of(_: libc::pid_t) -> std::result::Result<PathBuf, String> {
    Err("this platform gives no way to read a process's executable".into())
}

/// The record's process, if it is still ours. One that cannot be identified stops the command:
/// neither a second start nor a signal is safe on a guess.
fn still_ours(record: &Record, record_path: &Path) -> Result<bool> {
    verdict(
        identify(record.pid, &record.exe, record.started),
        record,
        record_path,
    )
}

fn verdict(identity: Identity, record: &Record, record_path: &Path) -> Result<bool> {
    match identity {
        Identity::Ours => Ok(true),
        Identity::Gone | Identity::Other(_) => Ok(false),
        Identity::Unconfirmed(why) => bail!(
            "pid {} from {} is alive, and whether it is the nuthatch dev recorded there cannot \
             be confirmed ({why}), so it is not signalled. Check it with `ps -p {}`; once it is \
             not running, remove {}",
            record.pid,
            record_path.display(),
            record.pid,
            record_path.display()
        ),
    }
}

/// How long a child that is not yet serving gets to finish after SIGTERM.
const TERM_GRACE: Duration = Duration::from_secs(10);

#[derive(Debug, PartialEq)]
enum Stopped {
    Exited,
    Killed,
}

/// SIGTERM, as `stop` sends, and SIGKILL only if the child has not exited within [`TERM_GRACE`].
fn stop_child(child: &mut std::process::Child) -> Stopped {
    stop_child_within(child, TERM_GRACE)
}

fn stop_child_within(child: &mut std::process::Child, grace: Duration) -> Stopped {
    if let Ok(pid) = libc::pid_t::try_from(child.id()) {
        // SAFETY: a plain signal to our own child, not yet reaped.
        unsafe { libc::kill(pid, libc::SIGTERM) };
        let until = Instant::now() + grace;
        while Instant::now() < until {
            if let Ok(Some(_)) = child.try_wait() {
                return Stopped::Exited;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    let _ = child.kill();
    let _ = child.wait();
    Stopped::Killed
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

async fn get_json(client: &reqwest::Client, url: &str) -> Option<(u16, serde_json::Value)> {
    let resp = client.get(url).send().await.ok()?;
    let status = resp.status().as_u16();
    Some((status, resp.json().await.unwrap_or_default()))
}

/// Whether the instance at `url` serves with a first poll behind it. A runtime's root `/ready`
/// carries no poll time of its own, so each mount it lists at `/nests` must report one.
async fn serving(client: &reqwest::Client, url: &str) -> bool {
    let Some((status, body)) = get_json(client, &format!("{url}/ready")).await else {
        return false;
    };
    if !is_ready(status, &body) {
        return false;
    }
    let mounts = match get_json(client, &format!("{url}/nests")).await {
        Some((200, nests)) => nests["nests"].as_array().cloned().unwrap_or_default(),
        // A solo nest has no roster, and its own `/ready` was the answer.
        _ => return true,
    };
    for mount in &mounts {
        let base = match (mount["base_path"].as_str(), mount["name"].as_str()) {
            (Some(path), _) => path.to_string(),
            (None, Some(name)) => format!("/{name}"),
            (None, None) => continue,
        };
        match get_json(client, &format!("{url}{base}/ready")).await {
            Some((status, body))
                if status == 200 && body["last_poll_unixtime"].as_u64().is_some_and(|t| t > 0) => {}
            _ => return false,
        }
    }
    true
}

/// Start `nuthatch dev` with the same arguments, less `--detach` and `--json`, in its own session,
/// and return once it serves.
pub async fn start(dir: &str, listen: &str, json: bool) -> Result<()> {
    let nest = nest_path(dir)?;
    let state = state_dir(&nest)?;
    std::fs::create_dir_all(&state).with_context(|| format!("creating {}", state.display()))?;
    let record_path = state.join(RECORD);
    if let Some(r) = read_record(&state) {
        if still_ours(&r, &record_path)? {
            bail!(
                "nuthatch dev is already running for {} (pid {}, {}); `nuthatch stop --dir {}` \
                 stops it",
                nest.display(),
                r.pid,
                r.url,
                nest.display()
            );
        }
    }
    // Whatever answers `/ready` on a taken port is not this nest, so the bind is checked first.
    if let Err(e) = std::net::TcpListener::bind(listen) {
        if e.kind() == std::io::ErrorKind::AddrInUse {
            return Err(tag(
                Code::PortInUse,
                anyhow::anyhow!("{listen} is already in use: another process listens there"),
            ));
        }
    }
    let log_path = state.join(LOG);
    let log = std::fs::File::create(&log_path)
        .with_context(|| format!("creating {}", log_path.display()))?;
    let args: Vec<std::ffi::OsString> = std::env::args_os()
        .skip(1)
        .filter(|a| a != "--detach" && a != "--json")
        .collect();
    let exe = std::env::current_exe().context("finding this executable")?;
    let exe = std::fs::canonicalize(&exe).unwrap_or(exe);
    let mut cmd = std::process::Command::new(&exe);
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
    let began = match libc::pid_t::try_from(child.id()).map(process_start) {
        Ok(Ok(Some(t))) => t,
        _ => {
            stop_child(&mut child);
            bail!("could not read when the nuthatch dev just started began, so it was stopped");
        }
    };
    let url = local_url(listen);
    let record = Record {
        pid: child.id(),
        dir: nest.clone(),
        url: url.clone(),
        log: log_path.clone(),
        exe,
        started: began,
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
            // A bind that lost a race after the pre-check is the same failure.
            let code = if log_tail(&log_path, 20).contains("Address already in use") {
                Code::PortInUse
            } else {
                Code::ALL
                    .into_iter()
                    .find(|c| Some(c.exit_code()) == status.code())
                    .unwrap_or(Code::Error)
            };
            return Err(tag(code, err));
        }
        if serving(&client, &url).await {
            break;
        }
        if started.elapsed() > READY_WITHIN {
            let how = match stop_child(&mut child) {
                Stopped::Exited => "stopped with SIGTERM",
                Stopped::Killed => "killed with SIGKILL, as SIGTERM had not ended it",
            };
            let _ = std::fs::remove_file(state.join(RECORD));
            bail!(
                "nuthatch dev was not serving at {url} after {}s, so it was {how}. The end of {}:\n{}",
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
    let record_path = state.join(RECORD);
    let record = match read_record(&state) {
        Some(r) if still_ours(&r, &record_path)? => Some(r),
        _ => None,
    };
    let Some(record) = record else {
        let _ = std::fs::remove_file(&record_path);
        return Err(tag(
            Code::NestNotRunning,
            anyhow::anyhow!(
                "no nuthatch dev started with --detach is running for {}",
                nest.display()
            ),
        ));
    };
    let pid = libc::pid_t::try_from(record.pid).context("pid out of range")?;
    // SAFETY: a plain signal to a pid just identified as the recorded executable.
    if unsafe { libc::kill(pid, libc::SIGTERM) } != 0 {
        bail!("could not signal pid {}", record.pid);
    }
    let started = Instant::now();
    while identify(record.pid, &record.exe, record.started) == Identity::Ours {
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

    /// A runtime's root answers 200 before its mounts have polled; `--detach` must not.
    #[tokio::test]
    async fn a_runtime_serves_once_every_mount_has_polled() {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc;
        let polled = Arc::new(AtomicU64::new(0));
        let p = polled.clone();
        let app = axum::Router::new()
            .route(
                "/ready",
                axum::routing::get(|| async { axum::Json(serde_json::json!({"ready": true})) }),
            )
            .route(
                "/nests",
                axum::routing::get(|| async {
                    axum::Json(serde_json::json!({"nests": [
                        {"name": "a", "base_path": "/a"}, {"name": "b"}
                    ]}))
                }),
            )
            .route(
                "/a/ready",
                axum::routing::get(|| async {
                    axum::Json(serde_json::json!({"ready": true, "last_poll_unixtime": 7}))
                }),
            )
            .route(
                "/b/ready",
                axum::routing::get(move || {
                    let t = p.load(Ordering::SeqCst);
                    async move {
                        axum::Json(serde_json::json!({"ready": true, "last_poll_unixtime": t}))
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await });
        let client = reqwest::Client::new();
        assert!(!serving(&client, &url).await, "mount b has not polled");
        polled.store(9, Ordering::SeqCst);
        assert!(serving(&client, &url).await);
    }

    #[test]
    fn this_process_is_identified_by_its_executable_not_its_name() {
        let pid = std::process::id();
        let exe = std::env::current_exe().unwrap();
        let began = process_start(pid as libc::pid_t).unwrap().unwrap();
        assert_eq!(identify(pid, &exe, began), Identity::Ours);
        let other = identify(pid, Path::new("/usr/bin/true"), began);
        assert!(matches!(other, Identity::Other(_)), "{other:?}");
    }

    /// After a reboot, or once the pid is reused, the pid names a process that started at another
    /// time: that is not the recorded dev, whatever it runs.
    #[test]
    fn a_pid_that_started_at_another_time_is_gone() {
        let pid = std::process::id();
        let exe = std::env::current_exe().unwrap();
        let began = process_start(pid as libc::pid_t).unwrap().unwrap();
        assert_eq!(identify(pid, &exe, began + 1), Identity::Gone);
    }

    /// Another user's process was not started by this one.
    #[test]
    fn another_users_pid_is_gone() {
        // SAFETY: geteuid has no preconditions.
        if unsafe { libc::geteuid() } == 0 {
            return;
        }
        let exe = std::env::current_exe().unwrap();
        assert_eq!(identify(1, &exe, 0), Identity::Gone);
    }

    /// A crashed dev nobody reaps stays a zombie; it has exited, and must not block start or stop.
    #[test]
    fn a_zombie_is_gone() {
        let mut child = std::process::Command::new("true").spawn().unwrap();
        let pid = child.id();
        let began = process_start(pid as libc::pid_t).unwrap().unwrap_or(0);
        let exe = std::fs::canonicalize("/usr/bin/true").unwrap_or_else(|_| "/bin/true".into());
        let until = Instant::now() + Duration::from_secs(10);
        while process_start(pid as libc::pid_t) != Ok(None) && Instant::now() < until {
            std::thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(
            process_start(pid as libc::pid_t),
            Ok(None),
            "the child is a zombie"
        );
        assert_eq!(identify(pid, &exe, began), Identity::Gone);
        child.wait().unwrap();
    }

    /// Only a live process of ours whose executable cannot be read is refused.
    #[test]
    fn an_unconfirmed_identity_is_refused_not_signalled() {
        let record = Record {
            pid: 1,
            dir: "/n".into(),
            url: "http://127.0.0.1:1".into(),
            log: "/l".into(),
            exe: "/x".into(),
            started: 0,
        };
        let path = Path::new("/s/dev.json");
        let err = verdict(Identity::Unconfirmed("no".into()), &record, path).unwrap_err();
        assert!(format!("{err:#}").contains("not signalled"), "{err:#}");
        assert!(!verdict(Identity::Gone, &record, path).unwrap());
        assert!(verdict(Identity::Ours, &record, path).unwrap());
    }

    /// The start timeout stops `dev` as `stop` does, by SIGTERM, and kills only what ignores it.
    #[test]
    fn a_child_is_asked_to_stop_before_it_is_killed() {
        let spawn = |script: &str| {
            std::process::Command::new("sh")
                .args(["-c", script])
                .spawn()
                .unwrap()
        };
        let mut polite = spawn("trap 'exit 0' TERM; while :; do sleep 0.05; done");
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(
            stop_child_within(&mut polite, Duration::from_secs(5)),
            Stopped::Exited
        );
        let mut stubborn = spawn("trap '' TERM; while :; do sleep 0.05; done");
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(
            stop_child_within(&mut stubborn, Duration::from_millis(500)),
            Stopped::Killed
        );
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
