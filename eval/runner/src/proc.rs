//! Child processes with Python's `communicate(timeout=...)` semantics: both pipes to EOF and the
//! exit, all inside the deadline.

use std::io::Read;
use std::os::unix::process::ExitStatusExt;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

pub struct Output {
    pub status: ExitStatus,
    pub stdout: String,
    pub stderr: String,
}

/// Python's `returncode`: the exit code, or minus the signal that killed it.
pub fn returncode(status: &ExitStatus) -> i32 {
    status
        .code()
        .unwrap_or_else(|| -status.signal().unwrap_or(0))
}

fn reader<R: Read + Send + 'static>(pipe: Option<R>) -> mpsc::Receiver<String> {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut buf = Vec::new();
        if let Some(mut p) = pipe {
            let _ = p.read_to_end(&mut buf);
        }
        let _ = tx.send(
            String::from_utf8_lossy(&buf)
                .replace("\r\n", "\n")
                .replace('\r', "\n"),
        );
    });
    rx
}

/// Wait for the child to exit and close both pipes. `None` on timeout, child left running.
pub fn communicate(child: &mut Child, timeout: Duration) -> Option<Output> {
    let deadline = Instant::now() + timeout;
    let out = reader(child.stdout.take());
    let err = reader(child.stderr.take());
    let remaining = || deadline.saturating_duration_since(Instant::now());
    let stdout = out.recv_timeout(remaining()).ok()?;
    let stderr = err.recv_timeout(remaining()).ok()?;
    let status = wait_timeout(child, remaining())?;
    Some(Output {
        status,
        stdout,
        stderr,
    })
}

pub fn wait_timeout(child: &mut Child, timeout: Duration) -> Option<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            return Some(status);
        }
        if Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// `subprocess.run(argv, capture_output=True, text=True, timeout=...)`, killing on timeout.
pub fn run(
    argv: &[String],
    cwd: Option<&std::path::Path>,
    timeout: Duration,
) -> Result<Output, String> {
    let (program, args) = argv.split_first().ok_or("empty command")?;
    let mut cmd = Command::new(program);
    cmd.args(args).stdout(Stdio::piped()).stderr(Stdio::piped());
    if let Some(dir) = cwd {
        cmd.current_dir(dir);
    }
    let mut child = cmd.spawn().map_err(|e| format!("FileNotFoundError: {e}"))?;
    match communicate(&mut child, timeout) {
        Some(out) => Ok(out),
        None => {
            let _ = child.kill();
            let _ = child.wait();
            Err(format!(
                "TimeoutExpired: Command '{argv:?}' timed out after {} seconds",
                timeout.as_secs_f64()
            ))
        }
    }
}
