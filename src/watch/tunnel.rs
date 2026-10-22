//! `--ssh HOST`: an `ssh -N -L` forward to a nest that listens only on another host's loopback.
//! Ported from `nuthatch-tui-client`.

use std::io::{self, Read};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

/// `BatchMode` because a password prompt would land in the middle of the screen, and
/// `ExitOnForwardFailure` so that a forward which cannot bind is an exit rather than a quiet ssh
/// session forwarding nothing.
pub struct Tunnel {
    program: String,
    host: String,
    forward: String,
    pub local_url: String,
    local_port: u16,
    child: Child,
    opened_at: Instant,
    failures: u32,
    retry_at: Option<Instant>,
    last_error: String,
}

/// A forward that has stayed up this long is healthy, and its next failure starts the backoff over.
const SETTLED: Duration = Duration::from_secs(60);

impl Tunnel {
    pub fn open(program: &str, host: &str, nest_url: &str) -> Result<Self> {
        let mut url =
            reqwest::Url::parse(nest_url).with_context(|| format!("'{nest_url}' is not a URL"))?;
        let remote_host = url
            .host_str()
            .with_context(|| format!("'{nest_url}' names no host"))?
            .to_owned();
        let remote_port = url
            .port_or_known_default()
            .with_context(|| format!("'{nest_url}' names no port"))?;
        let local_port = TcpListener::bind("127.0.0.1:0")?.local_addr()?.port();
        url.set_host(Some("127.0.0.1"))?;
        url.set_port(Some(local_port))
            .map_err(|_| anyhow::anyhow!("cannot set a port on '{nest_url}'"))?;
        let forward = format!("127.0.0.1:{local_port}:{remote_host}:{remote_port}");
        let child =
            spawn_ssh(program, host, &forward).with_context(|| format!("starting {program}"))?;
        let mut tunnel = Self {
            program: program.to_owned(),
            host: host.to_owned(),
            forward,
            local_url: url.to_string().trim_end_matches('/').to_owned(),
            local_port,
            child,
            opened_at: Instant::now(),
            failures: 0,
            retry_at: None,
            last_error: String::new(),
        };
        tunnel.wait_until_listening(Duration::from_secs(15))?;
        Ok(tunnel)
    }

    fn wait_until_listening(&mut self, limit: Duration) -> Result<()> {
        let started = Instant::now();
        let address = ([127, 0, 0, 1], self.local_port).into();
        loop {
            if let Some(status) = self.child.try_wait()? {
                let reason = self.stderr();
                anyhow::bail!("ssh to {} exited ({status}): {reason}", self.host);
            }
            if TcpStream::connect_timeout(&address, Duration::from_millis(200)).is_ok() {
                return Ok(());
            }
            anyhow::ensure!(
                started.elapsed() < limit,
                "ssh to {} had not opened the forward after {} s",
                self.host,
                limit.as_secs()
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn stderr(&mut self) -> String {
        let mut text = String::new();
        if let Some(mut stderr) = self.child.stderr.take() {
            let _ = stderr.read_to_string(&mut text);
        }
        let text = text.trim();
        if text.is_empty() {
            "no message".into()
        } else {
            text.lines().last().unwrap_or(text).to_owned()
        }
    }

    /// Called every loop: notices ssh exiting and reopens the forward with a doubling backoff.
    /// Returns what the footer should say while the forward is down.
    pub fn supervise(&mut self) -> Option<String> {
        if let Some(at) = self.retry_at {
            let now = Instant::now();
            if now < at {
                return Some(format!(
                    "ssh to {} exited: {}. Reopening in {} s",
                    self.host,
                    self.last_error,
                    // A countdown rounds up, or the first second reads "in 0 s".
                    (at - now).as_secs_f64().ceil() as u64
                ));
            }
            self.retry_at = None;
            match spawn_ssh(&self.program, &self.host, &self.forward) {
                Ok(child) => {
                    self.child = child;
                    self.opened_at = now;
                }
                Err(error) => {
                    self.last_error = error.to_string();
                    self.schedule_retry();
                }
            }
            return Some(format!("ssh to {}: reopening the forward", self.host));
        }
        match self.child.try_wait() {
            Ok(None) => {
                if self.opened_at.elapsed() > SETTLED {
                    self.failures = 0;
                }
                None
            }
            Ok(Some(_)) => {
                self.last_error = self.stderr();
                self.schedule_retry();
                self.supervise()
            }
            Err(error) => Some(format!("ssh to {}: {error}", self.host)),
        }
    }

    fn schedule_retry(&mut self) {
        self.retry_at = Some(Instant::now() + backoff(self.failures));
        self.failures += 1;
    }
}

impl Drop for Tunnel {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn backoff(failures: u32) -> Duration {
    Duration::from_secs((1u64 << failures.min(5)).min(30))
}

fn ssh_args(host: &str, forward: &str) -> Vec<String> {
    [
        "-N",
        "-o",
        "BatchMode=yes",
        "-o",
        "ExitOnForwardFailure=yes",
        "-o",
        "ServerAliveInterval=15",
        "-o",
        "ServerAliveCountMax=2",
        "-L",
        forward,
        host,
    ]
    .map(String::from)
    .to_vec()
}

fn spawn_ssh(program: &str, host: &str, forward: &str) -> io::Result<Child> {
    Command::new(program)
        .args(ssh_args(host, forward))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ssh_is_asked_for_a_batch_mode_forward_that_fails_loudly() {
        let args = ssh_args("box", "127.0.0.1:1:127.0.0.1:8288");
        for want in ["BatchMode=yes", "ExitOnForwardFailure=yes", "-N", "-L"] {
            assert!(
                args.iter().any(|a| a == want),
                "{want} missing from {args:?}"
            );
        }
        assert_eq!(args.last().map(String::as_str), Some("box"));
    }

    #[test]
    fn the_tunnel_backs_off_doubling_to_half_a_minute() {
        let secs: Vec<u64> = (0..8).map(|f| backoff(f).as_secs()).collect();
        assert_eq!(secs, vec![1, 2, 4, 8, 16, 30, 30, 30]);
    }

    /// A stand-in for ssh: a shell script that listens on the forward's local port (or exits with
    /// a refusal), so the tunnel's handling is tested without a host to reach.
    #[cfg(unix)]
    fn fake_ssh(dir: &std::path::Path, fail: bool) -> String {
        use std::os::unix::fs::PermissionsExt;
        let path = dir.join("ssh");
        let body = if fail {
            "#!/bin/sh\necho 'Permission denied (publickey).' >&2\nexit 255\n".to_string()
        } else {
            // The forward is the argument after -L: 127.0.0.1:<port>:host:port.
            "#!/bin/sh\nwhile [ \"$1\" != -L ]; do shift; done\nport=$(echo \"$2\" | cut -d: -f2)\n\
             exec nc -l 127.0.0.1 \"$port\"\n"
                .to_string()
        };
        std::fs::write(&path, body).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path.to_string_lossy().into_owned()
    }

    /// [`Tunnel::open`] on the stand-in, again if the spawn met ETXTBSY: another test's fork can still
    /// hold the just-written script open for writing for the instant before its own exec.
    #[cfg(unix)]
    fn open(ssh: &str, url: &str) -> Result<Tunnel> {
        for _ in 0..20 {
            match Tunnel::open(ssh, "box", url) {
                Err(e) if format!("{e:#}").contains("Text file busy") => {
                    std::thread::sleep(Duration::from_millis(50))
                }
                other => return other,
            }
        }
        Tunnel::open(ssh, "box", url)
    }

    #[cfg(unix)]
    #[test]
    fn a_refused_key_is_reported_before_the_screen_opens() {
        let dir = tempfile::tempdir().unwrap();
        let ssh = fake_ssh(dir.path(), true);
        let err = open(&ssh, "http://127.0.0.1:8288")
            .err()
            .expect("a refused key must fail the open");
        let text = format!("{err:#}");
        assert!(text.contains("Permission denied"), "{text}");
    }

    #[cfg(unix)]
    #[test]
    fn the_tunnel_rewrites_the_url_onto_its_local_end() {
        if Command::new("nc").arg("-h").output().is_err() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let ssh = fake_ssh(dir.path(), false);
        let tunnel = open(&ssh, "http://10.0.0.5:8288").unwrap();
        assert!(
            tunnel.forward.ends_with(":10.0.0.5:8288"),
            "{}",
            tunnel.forward
        );
        assert_eq!(
            tunnel.local_url,
            format!("http://127.0.0.1:{}", tunnel.local_port)
        );
    }
}
