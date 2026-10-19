//! RFC-0046 S3: the back office.
//!
//! The nest already served. This process drains `authorisations.jsonl`, hands each row to an
//! operator `--exec` (or a test double), and records what happened. It holds no query path and
//! does not run inside `dev`/`serve`.

use anyhow::{bail, Context, Result};
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use crate::counter::{LOG, PAYERS, SPENT};

/// What happened when the back office tried to move money.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Settled,
    /// The token (or the operator script) refused. Do not retry; the payer's promise did not clear.
    Failed(String),
    /// Transport, a missing binary, a crash. Leave the row pending.
    Deferred(String),
}

pub trait Submit {
    fn submit(&self, row: &serde_json::Value) -> Outcome;
    /// One outcome per row, in order. The default submits them one at a time.
    fn submit_batch(&self, rows: &[serde_json::Value]) -> Vec<Outcome> {
        rows.iter().map(|row| self.submit(row)).collect()
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct DrainReport {
    pub settled: u64,
    pub failed: u64,
    pub deferred: u64,
    pub remaining: u64,
}

/// Drain pending authorisations. Settled and failed rows leave the pending log and land in
/// `spent.jsonl` so the nest still refuses the nonce. Deferred rows stay pending.
pub fn drain(dir: &Path, submit: &dyn Submit) -> Result<DrainReport> {
    drain_batched(dir, submit, 1)
}

/// [`drain`], handing the submitter up to `batch` rows at a time (RFC-0048 item 5). A batch's
/// outcomes are journalled before the next batch is submitted.
pub fn drain_batched(dir: &Path, submit: &dyn Submit, batch: usize) -> Result<DrainReport> {
    let settler_lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(dir.join("settler.lock"))?;
    settler_lock
        .try_lock()
        .context("another settler is already running")?;
    let pending_path = dir.join(LOG);
    // The counter appends whole lines under the queue lock, so a length taken under it ends on a
    // line boundary. Rows appended after it wait for the next run.
    let snapshot = {
        let _guard = crate::counter::lock_queue(dir)?;
        match std::fs::metadata(&pending_path) {
            Ok(meta) => meta.len(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
            Err(e) => return Err(e).with_context(|| format!("stat {}", pending_path.display())),
        }
    };
    // Only the settler replaces the queue and the counter only appends to it, so the replacement is
    // this run's deferred rows, written as their outcomes arrive, followed by whatever the counter
    // appended past the snapshot. Nothing proportional to the queue is held in memory.
    let tmp = pending_path.with_extension("jsonl.tmp");
    let mut kept = std::io::BufWriter::new(
        std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .with_context(|| format!("write {}", tmp.display()))?,
    );
    let mut report = DrainReport::default();
    let batch = batch.max(1);
    let mut chunk = Vec::with_capacity(batch);
    let mut recorded = recorded_outcomes(dir, &pending_keys(&pending_path, snapshot)?)?;
    if snapshot > 0 {
        let file = std::fs::File::open(&pending_path)
            .with_context(|| format!("read {}", pending_path.display()))?;
        for line in std::io::BufReader::new(std::io::Read::take(file, snapshot)).lines() {
            let Some(row) = parse_row(&pending_path, line)? else {
                continue;
            };
            // `spent.jsonl` is an outcome journal as well as the counter's compact replay set. If a
            // prior run reached the durable journal but crashed before rewriting the pending queue,
            // finalise that result. Calling the operator command a second time would be a second
            // attempt to move the same money.
            match recorded.get(&authorisation_key(&row)).cloned().flatten() {
                Some(outcome) => journal(dir, &row, outcome, true, &mut report, &mut kept)?,
                None => chunk.push(row),
            }
            if chunk.len() == batch {
                submit_chunk(
                    dir,
                    submit,
                    &mut chunk,
                    &mut recorded,
                    &mut report,
                    &mut kept,
                )?;
            }
        }
    }
    submit_chunk(
        dir,
        submit,
        &mut chunk,
        &mut recorded,
        &mut report,
        &mut kept,
    )?;
    // New payments may have arrived while the external command ran. Take them under the same
    // cross-process lock as the server's append.
    let _guard = crate::counter::lock_queue(dir)?;
    finish_queue(&pending_path, &tmp, kept, snapshot, &mut report)?;
    Ok(report)
}

fn submit_chunk(
    dir: &Path,
    submit: &dyn Submit,
    chunk: &mut Vec<serde_json::Value>,
    recorded: &mut Recorded,
    report: &mut DrainReport,
    kept: &mut std::io::BufWriter<std::fs::File>,
) -> Result<()> {
    if chunk.is_empty() {
        return Ok(());
    }
    let mut outcomes = submit.submit_batch(chunk).into_iter();
    for row in chunk.drain(..) {
        let outcome = outcomes
            .next()
            .unwrap_or_else(|| Outcome::Deferred("the submitter returned no outcome".into()));
        // What this run journals counts too, as it did when every row reread the journal.
        if !matches!(outcome, Outcome::Deferred(_)) {
            recorded
                .entry(authorisation_key(&row))
                .or_insert_with(|| Some(outcome.clone()));
        }
        journal(dir, &row, outcome, false, report, kept)?;
    }
    Ok(())
}

fn journal(
    dir: &Path,
    row: &serde_json::Value,
    outcome: Outcome,
    was_recorded: bool,
    report: &mut DrainReport,
    kept: &mut std::io::BufWriter<std::fs::File>,
) -> Result<()> {
    let (result, detail) = match outcome {
        Outcome::Deferred(_) => {
            report.deferred += 1;
            report.remaining += 1;
            writeln!(kept, "{row}").context("write the replacement queue")?;
            return Ok(());
        }
        Outcome::Settled => ("settled", String::new()),
        Outcome::Failed(detail) => ("failed", detail),
    };
    let _guard = crate::counter::lock_queue(dir)?;
    if !was_recorded {
        append_jsonl(&dir.join(SPENT), &spent_row(row, result, &detail))?;
    }
    append_jsonl(&dir.join(PAYERS), &payer_row(row, result, &detail))?;
    if result == "settled" {
        report.settled += 1;
    } else {
        report.failed += 1;
    }
    Ok(())
}

fn authorisation_key(row: &serde_json::Value) -> [String; 3] {
    ["network", "payer", "nonce"]
        .map(|key| row[key].as_str().expect("validated pending row").to_owned())
}

/// `--exec`: stdin is one pending row. Exit 0 settled, 2 failed, anything else deferred.
/// The command must be idempotent by `(network, payer, nonce)`: a process can die after external
/// settlement succeeds but before its local journal write. On retry the command must reconcile
/// that authorisation's prior transaction and return its outcome, without submitting a new debit.
pub struct ExecSubmit {
    pub command: String,
}

impl Submit for ExecSubmit {
    fn submit(&self, row: &serde_json::Value) -> Outcome {
        match run_exec(&self.command, format!("{row}\n"), |_| {}) {
            Err(why) => Outcome::Deferred(why),
            Ok((status, _)) if status.success() => Outcome::Settled,
            Ok((status, stderr)) if status.code() == Some(2) => {
                Outcome::Failed(if stderr.is_empty() {
                    "exec exited 2".into()
                } else {
                    stderr
                })
            }
            Ok((status, _)) => Outcome::Deferred(format!("exec exited {}", exit_code(status))),
        }
    }
}

/// Longest stdout line kept from an operator command. A longer line is skipped, never buffered.
const EXEC_LINE_LIMIT: usize = 64 * 1024;
/// Stderr kept as a failure's detail; the rest is drained and dropped.
const EXEC_STDERR_LIMIT: u64 = 4 * 1024;

/// Run `command` with `input` on stdin, handing each stdout line to `line` as it arrives. stdin is
/// written from its own thread, so a command that writes before it reads cannot deadlock on a full
/// pipe, and nothing the command prints is retained beyond one line and [`EXEC_STDERR_LIMIT`].
fn run_exec(
    command: &str,
    input: String,
    mut line: impl FnMut(&str),
) -> std::result::Result<(std::process::ExitStatus, String), String> {
    let mut child = Command::new("sh")
        .arg("-c")
        .arg(command)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawn: {e}"))?;
    let stdin = child.stdin.take();
    let writer = std::thread::spawn(move || -> std::io::Result<()> {
        if let Some(mut stdin) = stdin {
            stdin.write_all(input.as_bytes())?;
        }
        Ok(())
    });
    let stderr = child.stderr.take();
    let drain = std::thread::spawn(move || {
        let mut kept = Vec::new();
        if let Some(mut stderr) = stderr {
            let _ = std::io::Read::read_to_end(
                &mut std::io::Read::take(&mut stderr, EXEC_STDERR_LIMIT),
                &mut kept,
            );
            let _ = std::io::copy(&mut stderr, &mut std::io::sink());
        }
        String::from_utf8_lossy(&kept).trim().to_string()
    });
    if let Some(stdout) = child.stdout.take() {
        let mut reader = std::io::BufReader::new(stdout);
        let mut buf = Vec::new();
        let mut overlong = false;
        loop {
            buf.clear();
            match std::io::Read::take(&mut reader, EXEC_LINE_LIMIT as u64)
                .read_until(b'\n', &mut buf)
            {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
            // A short read without a newline is the last line; a full one is part of a long line.
            let whole = buf.last() == Some(&b'\n') || buf.len() < EXEC_LINE_LIMIT;
            if whole && !overlong {
                if let Ok(text) = std::str::from_utf8(&buf) {
                    line(text.trim_end());
                }
            }
            overlong = !whole;
        }
    }
    let status = child.wait().map_err(|e| format!("wait: {e}"))?;
    let stderr = drain.join().unwrap_or_default();
    match writer.join() {
        Ok(Ok(())) => Ok((status, stderr)),
        Ok(Err(e)) => Err(format!("write authorisations to exec: {e}")),
        Err(_) => Err("the exec stdin writer panicked".into()),
    }
}

fn exit_code(status: std::process::ExitStatus) -> String {
    status
        .code()
        .map(|c| c.to_string())
        .unwrap_or_else(|| "signal".into())
}

fn parse_outcome(line: &str) -> Option<([String; 3], Outcome)> {
    let result = serde_json::from_str::<serde_json::Value>(line).ok()?;
    let field = |key: &str| {
        result
            .get(key)
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
    };
    let key = [field("network")?, field("payer")?, field("nonce")?];
    let outcome = match field("outcome").as_deref() {
        Some("settled") => Outcome::Settled,
        Some("failed") => Outcome::Failed(
            field("detail")
                .filter(|d| !d.is_empty())
                .unwrap_or_else(|| "exec reported failed".into()),
        ),
        _ => return None,
    };
    Some((key, outcome))
}

/// `--exec` with `--batch`: stdin is up to N pending rows, one per line; stdout is one line per row
/// the command reached a result for, `{"network","payer","nonce","outcome","detail"}` with outcome
/// `settled` or `failed`. A row it does not report stays pending whatever the exit code, so a
/// command that settles half a batch and dies still has that half journalled. Idempotency is
/// [`ExecSubmit`]'s contract.
pub struct BatchExecSubmit {
    pub command: String,
}

impl Submit for BatchExecSubmit {
    fn submit(&self, row: &serde_json::Value) -> Outcome {
        self.submit_batch(std::slice::from_ref(row))
            .pop()
            .unwrap_or_else(|| Outcome::Deferred("exec reported no outcome".into()))
    }

    fn submit_batch(&self, rows: &[serde_json::Value]) -> Vec<Outcome> {
        let wanted: std::collections::HashSet<[String; 3]> =
            rows.iter().map(authorisation_key).collect();
        let mut reported = std::collections::HashMap::new();
        let input: String = rows.iter().map(|row| format!("{row}\n")).collect();
        let result = run_exec(&self.command, input, |line| {
            if let Some((key, outcome)) = parse_outcome(line) {
                if wanted.contains(&key) {
                    reported.insert(key, outcome);
                }
            }
        });
        // Outcomes the command reported stand even when it then failed: money may have moved.
        let why = match result {
            Ok((status, _)) => format!(
                "exec exited {} without reporting this row",
                exit_code(status)
            ),
            Err(why) => why,
        };
        rows.iter()
            .map(|row| {
                reported
                    .remove(&authorisation_key(row))
                    .unwrap_or_else(|| Outcome::Deferred(why.clone()))
            })
            .collect()
    }
}

pub fn run(args: crate::cli::SettleArgs) -> Result<()> {
    let dir = PathBuf::from(&args.dir);
    if args.dry_run && args.exec.is_some() {
        bail!("pass --dry-run or --exec, not both");
    }
    if args.dry_run {
        let pending = read_jsonl(&dir.join(LOG))?;
        println!("{} pending authorisation(s)", pending.len());
        for row in &pending {
            println!("{row}");
        }
        return Ok(());
    }
    let Some(command) = args.exec else {
        bail!("nuthatch settle needs --exec (or --dry-run to list the queue)");
    };
    let report = match usize::try_from(args.batch)? {
        1 => drain(&dir, &ExecSubmit { command })?,
        batch => drain_batched(&dir, &BatchExecSubmit { command }, batch)?,
    };
    println!(
        "settled {}  failed {}  deferred {}  remaining {}",
        report.settled, report.failed, report.deferred, report.remaining
    );
    Ok(())
}

fn spent_row(row: &serde_json::Value, outcome: &str, detail: &str) -> serde_json::Value {
    serde_json::json!({
        "network": row.get("network").cloned().unwrap_or(serde_json::Value::Null),
        "payer": row.get("payer").cloned().unwrap_or(serde_json::Value::Null),
        "nonce": row.get("nonce").cloned().unwrap_or(serde_json::Value::Null),
        "outcome": outcome,
        "detail": detail,
    })
}

#[cfg(test)]
thread_local! {
    static JOURNAL_SCANS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The first outcome `spent.jsonl` records for each authorisation, keyed `(network, payer, nonce)`.
type Recorded = std::collections::HashMap<[String; 3], Option<Outcome>>;

/// Every authorisation in the first `snapshot` bytes of the queue. A line that will not parse is
/// skipped here; the drain itself stops on it.
fn pending_keys(path: &Path, snapshot: u64) -> Result<std::collections::HashSet<[String; 3]>> {
    let mut keys = std::collections::HashSet::new();
    if snapshot == 0 {
        return Ok(keys);
    }
    let file = std::fs::File::open(path).with_context(|| format!("read {}", path.display()))?;
    for line in std::io::BufReader::new(std::io::Read::take(file, snapshot)).lines() {
        let line = line.with_context(|| format!("read {}", path.display()))?;
        let row = serde_json::from_str::<serde_json::Value>(&line).ok();
        keys.extend(row.as_ref().and_then(string_key));
    }
    Ok(keys)
}

/// One pass over the journal for the whole drain (#2178), keeping only what `pending` names, so it
/// holds the queue's size rather than the journal's.
fn recorded_outcomes(
    dir: &Path,
    pending: &std::collections::HashSet<[String; 3]>,
) -> Result<Recorded> {
    #[cfg(test)]
    JOURNAL_SCANS.with(|s| s.set(s.get() + 1));
    let mut out = Recorded::new();
    if pending.is_empty() {
        return Ok(out);
    }
    let path = dir.join(SPENT);
    let file = match std::fs::File::open(&path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
        Ok(file) => file,
    };
    for line in std::io::BufReader::new(file).lines() {
        let line = line.with_context(|| format!("read {}", path.display()))?;
        if line.trim().is_empty() {
            continue;
        }
        let recorded: serde_json::Value =
            serde_json::from_str(&line).with_context(|| format!("parse {}", path.display()))?;
        let Some(key) = string_key(&recorded).filter(|k| pending.contains(k)) else {
            continue;
        };
        if out.contains_key(&key) {
            continue;
        }
        let outcome = match recorded.get("outcome").and_then(serde_json::Value::as_str) {
            // Old spent rows were created before S3, so carry no external-result claim.
            None => None,
            Some("settled") => Some(Outcome::Settled),
            Some("failed") => Some(Outcome::Failed(
                recorded
                    .get("detail")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("recorded failure")
                    .to_owned(),
            )),
            Some(other) => bail!("unknown recorded settlement outcome {other:?}"),
        };
        out.insert(key, outcome);
    }
    Ok(out)
}

/// A pending row's three fields are validated strings, so a journal row without them matches none.
fn string_key(row: &serde_json::Value) -> Option<[String; 3]> {
    let field = |key: &str| row.get(key)?.as_str().map(str::to_owned);
    Some([field("network")?, field("payer")?, field("nonce")?])
}

fn payer_row(row: &serde_json::Value, outcome: &str, detail: &str) -> serde_json::Value {
    serde_json::json!({
        "network": row.get("network").cloned().unwrap_or(serde_json::Value::Null),
        "payer": row.get("payer").cloned().unwrap_or(serde_json::Value::Null),
        "nonce": row.get("nonce").cloned().unwrap_or(serde_json::Value::Null),
        "outcome": outcome,
        "detail": detail,
    })
}

fn read_jsonl(path: &Path) -> Result<Vec<serde_json::Value>> {
    let file = match std::fs::File::open(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
        Ok(f) => f,
    };
    let mut out = Vec::new();
    for line in std::io::BufReader::new(file).lines() {
        out.extend(parse_row(path, line)?);
    }
    Ok(out)
}

fn parse_row(path: &Path, line: std::io::Result<String>) -> Result<Option<serde_json::Value>> {
    let line = line.with_context(|| format!("read {}", path.display()))?;
    if line.trim().is_empty() {
        return Ok(None);
    }
    // This is a money queue, not an opportunistic event feed. Skipping a corrupt line would
    // quietly forget an authorisation the nest has already honoured. Stop and let the
    // operator repair the file instead.
    let row = serde_json::from_str(&line).with_context(|| format!("parse {}", path.display()))?;
    validate_row(&row).with_context(|| format!("validate {}", path.display()))?;
    Ok(Some(row))
}

/// Append the queue past `snapshot` to the replacement and install it. A crash before the rename
/// leaves the old queue, whose completed rows the journals finalise on the next run.
fn finish_queue(
    path: &Path,
    tmp: &Path,
    mut kept: std::io::BufWriter<std::fs::File>,
    snapshot: u64,
    report: &mut DrainReport,
) -> Result<()> {
    match std::fs::File::open(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
        Ok(mut file) => {
            std::io::Seek::seek(&mut file, std::io::SeekFrom::Start(snapshot))
                .with_context(|| format!("seek {}", path.display()))?;
            for line in std::io::BufReader::new(file).lines() {
                let Some(row) = parse_row(path, line)? else {
                    continue;
                };
                writeln!(kept, "{row}").with_context(|| format!("write {}", tmp.display()))?;
                report.remaining += 1;
            }
        }
    }
    kept.into_inner()
        .map_err(|e| e.into_error())
        .and_then(|f| f.sync_data())
        .with_context(|| format!("sync {}", tmp.display()))?;
    if report.remaining == 0 {
        std::fs::remove_file(tmp).with_context(|| format!("remove {}", tmp.display()))?;
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e).with_context(|| format!("remove {}", path.display())),
        }
    } else {
        std::fs::rename(tmp, path).with_context(|| format!("rename {}", tmp.display()))?;
    }
    sync_parent(path)
}

fn append_jsonl(path: &Path, row: &serde_json::Value) -> Result<()> {
    let existed = path
        .try_exists()
        .with_context(|| format!("stat {}", path.display()))?;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("append {}", path.display()))?;
    writeln!(file, "{row}").with_context(|| format!("write {}", path.display()))?;
    file.sync_data()
        .with_context(|| format!("sync {}", path.display()))?;
    // A durable file body is not enough for a newly-created outcome file. Its directory entry
    // must survive the same crash boundary as the queue rewrite below.
    if !existed {
        sync_parent(path)?;
    }
    Ok(())
}

#[cfg(test)]
fn write_jsonl(path: &Path, rows: &[serde_json::Value]) -> Result<()> {
    if rows.is_empty() {
        match std::fs::remove_file(path) {
            Ok(()) => {
                sync_parent(path)?;
                return Ok(());
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e).with_context(|| format!("remove {}", path.display())),
        }
    }
    let tmp = path.with_extension("jsonl.tmp");
    {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .with_context(|| format!("write {}", tmp.display()))?;
        for row in rows {
            writeln!(file, "{row}").with_context(|| format!("write {}", tmp.display()))?;
        }
        file.sync_data()
            .with_context(|| format!("sync {}", tmp.display()))?;
    }
    std::fs::rename(&tmp, path).with_context(|| format!("rename {}", tmp.display()))?;
    sync_parent(path)?;
    Ok(())
}

fn validate_row(row: &serde_json::Value) -> Result<()> {
    for key in ["network", "payer", "nonce", "authorisation"] {
        if row.get(key).and_then(serde_json::Value::as_str).is_none() {
            bail!("pending row has no string {key}");
        }
    }
    Ok(())
}

fn sync_parent(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .context("queue path has no parent directory")?;
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .with_context(|| format!("sync directory {}", parent.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Scripted {
        answers: std::sync::Mutex<std::collections::VecDeque<Outcome>>,
    }

    impl Submit for Scripted {
        fn submit(&self, _row: &serde_json::Value) -> Outcome {
            self.answers
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Outcome::Deferred("empty script".into()))
        }
    }

    fn row(payer: &str, nonce: &str) -> serde_json::Value {
        serde_json::json!({
            "network": "testnet",
            "payer": payer,
            "nonce": nonce,
            "authorisation": "hdr",
        })
    }

    #[test]
    fn a_settled_row_leaves_the_queue_and_stays_spent() {
        let dir = tempfile::tempdir().unwrap();
        write_jsonl(&dir.path().join(LOG), &[row("0xaa", "0x01")]).unwrap();
        let report = drain(
            dir.path(),
            &Scripted {
                answers: std::sync::Mutex::new(vec![Outcome::Settled].into()),
            },
        )
        .unwrap();
        assert_eq!(
            report,
            DrainReport {
                settled: 1,
                failed: 0,
                deferred: 0,
                remaining: 0
            }
        );
        assert!(
            !dir.path().join(LOG).exists(),
            "an empty queue is no file, not an empty one"
        );
        assert!(crate::counter::nonce_is_spent(dir.path(), "testnet", "0xaa", "0x01").unwrap());
        let payers = std::fs::read_to_string(dir.path().join(PAYERS)).unwrap();
        assert!(payers.contains("\"outcome\":\"settled\""), "{payers}");
    }

    #[test]
    fn a_failed_row_is_spent_and_named_on_the_payer() {
        let dir = tempfile::tempdir().unwrap();
        write_jsonl(&dir.path().join(LOG), &[row("0xaa", "0x01")]).unwrap();
        drain(
            dir.path(),
            &Scripted {
                answers: std::sync::Mutex::new(
                    vec![Outcome::Failed("insufficient funds".into())].into(),
                ),
            },
        )
        .unwrap();
        assert!(crate::counter::nonce_is_spent(dir.path(), "testnet", "0xaa", "0x01").unwrap());
        assert!(crate::counter::payer_has_failed(dir.path(), "testnet", "0xaa").unwrap());
        assert!(!dir.path().join(LOG).exists());
    }

    #[test]
    fn a_deferred_row_stays_pending_and_is_not_a_credit_hit() {
        let dir = tempfile::tempdir().unwrap();
        write_jsonl(&dir.path().join(LOG), &[row("0xaa", "0x01")]).unwrap();
        let report = drain(
            dir.path(),
            &Scripted {
                answers: std::sync::Mutex::new(vec![Outcome::Deferred("rpc down".into())].into()),
            },
        )
        .unwrap();
        assert_eq!(report.deferred, 1);
        assert_eq!(report.remaining, 1);
        assert!(crate::counter::nonce_is_spent(dir.path(), "testnet", "0xaa", "0x01").unwrap());
        assert!(!crate::counter::payer_has_failed(dir.path(), "testnet", "0xaa").unwrap());
        let pending = std::fs::read_to_string(dir.path().join(LOG)).unwrap();
        assert!(pending.contains("0x01"), "{pending}");
    }

    #[test]
    fn exec_exit_codes_are_the_three_outcomes() {
        let ok = ExecSubmit {
            command: "cat >/dev/null; exit 0".into(),
        };
        assert_eq!(ok.submit(&row("0xaa", "0x01")), Outcome::Settled);
        let no = ExecSubmit {
            command: "cat >/dev/null; echo insufficient >&2; exit 2".into(),
        };
        match no.submit(&row("0xaa", "0x01")) {
            Outcome::Failed(d) => assert!(d.contains("insufficient"), "{d}"),
            other => panic!("{other:?}"),
        }
        let miss = ExecSubmit {
            command: "cat >/dev/null; exit 7".into(),
        };
        match miss.submit(&row("0xaa", "0x01")) {
            Outcome::Deferred(d) => assert!(d.contains("7"), "{d}"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_corrupt_pending_row_stops_the_drain_without_dropping_it() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(LOG), b"not json\n").unwrap();
        let err = drain(
            dir.path(),
            &Scripted {
                answers: std::sync::Mutex::new(Vec::new().into()),
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("parse"), "{err:#}");
        assert_eq!(std::fs::read(dir.path().join(LOG)).unwrap(), b"not json\n");
    }

    #[test]
    fn an_incomplete_pending_row_stops_the_drain_without_submitting_it() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(LOG), b"{\"network\":\"testnet\"}\n").unwrap();
        let err = drain(
            dir.path(),
            &Scripted {
                answers: std::sync::Mutex::new(Vec::new().into()),
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("validate"), "{err:#}");
    }

    #[test]
    fn an_outcome_journalled_before_a_crash_is_not_submitted_twice() {
        let dir = tempfile::tempdir().unwrap();
        let pending = row("0xaa", "0x01");
        write_jsonl(&dir.path().join(LOG), std::slice::from_ref(&pending)).unwrap();
        append_jsonl(&dir.path().join(SPENT), &spent_row(&pending, "settled", "")).unwrap();
        let report = drain(
            dir.path(),
            &Scripted {
                answers: std::sync::Mutex::new(Vec::new().into()),
            },
        )
        .unwrap();
        assert_eq!(report.settled, 1);
        assert!(!dir.path().join(LOG).exists());
    }

    /// #2178: the journal was rescanned from the start for every pending row.
    #[test]
    fn a_drain_reads_the_spent_journal_once() {
        let dir = tempfile::tempdir().unwrap();
        let pending: Vec<_> = (0..5).map(|n| row("0xaa", &format!("0x{n:02}"))).collect();
        write_jsonl(&dir.path().join(LOG), &pending).unwrap();
        for n in 10..20 {
            let old = row("0xbb", &format!("0x{n:02}"));
            append_jsonl(&dir.path().join(SPENT), &spent_row(&old, "settled", "")).unwrap();
        }
        append_jsonl(
            &dir.path().join(SPENT),
            &spent_row(&pending[3], "failed", "no"),
        )
        .unwrap();
        JOURNAL_SCANS.with(|s| s.set(0));
        let report = drain(
            dir.path(),
            &Scripted {
                answers: std::sync::Mutex::new(vec![Outcome::Settled; 4].into()),
            },
        )
        .unwrap();
        assert_eq!((report.settled, report.failed), (4, 1));
        assert_eq!(JOURNAL_SCANS.with(|s| s.get()), 1);
    }

    #[test]
    fn payment_recorded_while_settling_survives_queue_replacement() {
        struct ConcurrentSale<'a>(&'a Path);
        impl Submit for ConcurrentSale<'_> {
            fn submit(&self, _: &serde_json::Value) -> Outcome {
                let (config, header) = crate::counter::x402::tests::payment_for_http_test();
                crate::counter::verify_and_record(self.0, &config, &header, crate::counter::now())
                    .unwrap();
                Outcome::Settled
            }
        }
        let dir = tempfile::tempdir().unwrap();
        write_jsonl(&dir.path().join(LOG), &[row("old-payer", "old-nonce")]).unwrap();
        let report = drain(dir.path(), &ConcurrentSale(dir.path())).unwrap();
        assert_eq!(report.settled, 1);
        assert_eq!(report.remaining, 1);
        let pending = read_jsonl(&dir.path().join(LOG)).unwrap();
        assert_ne!(pending[0]["payer"], "old-payer");
        let (config, header) = crate::counter::x402::tests::payment_for_http_test();
        assert_eq!(
            crate::counter::verify_and_record(dir.path(), &config, &header, crate::counter::now()),
            Err(crate::counter::x402::Refusal::AlreadyUsed)
        );
    }

    #[test]
    fn a_second_settler_cannot_submit_the_same_queue() {
        struct SecondSettler<'a>(&'a Path);
        impl Submit for SecondSettler<'_> {
            fn submit(&self, _: &serde_json::Value) -> Outcome {
                let err = drain(self.0, self).unwrap_err();
                assert!(err.to_string().contains("another settler"), "{err:#}");
                Outcome::Settled
            }
        }
        let dir = tempfile::tempdir().unwrap();
        write_jsonl(&dir.path().join(LOG), &[row("payer", "nonce")]).unwrap();
        assert_eq!(
            drain(dir.path(), &SecondSettler(dir.path()))
                .unwrap()
                .settled,
            1
        );
    }

    #[test]
    fn a_batched_drain_submits_in_chunks_and_journals_every_row() {
        struct Chunks(std::sync::Mutex<Vec<usize>>);
        impl Submit for Chunks {
            fn submit(&self, _: &serde_json::Value) -> Outcome {
                panic!("a batched drain must not fall back to one row at a time")
            }
            fn submit_batch(&self, rows: &[serde_json::Value]) -> Vec<Outcome> {
                self.0.lock().unwrap().push(rows.len());
                rows.iter().map(|_| Outcome::Settled).collect()
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<_> = (1..=5).map(|n| row("0xaa", &format!("0x0{n}"))).collect();
        write_jsonl(&dir.path().join(LOG), &rows).unwrap();
        let chunks = Chunks(std::sync::Mutex::new(Vec::new()));
        let report = drain_batched(dir.path(), &chunks, 2).unwrap();
        assert_eq!(*chunks.0.lock().unwrap(), [2, 2, 1]);
        assert_eq!((report.settled, report.remaining), (5, 0));
        for n in 1..=5 {
            let nonce = format!("0x0{n}");
            assert!(crate::counter::nonce_is_spent(dir.path(), "testnet", "0xaa", &nonce).unwrap());
        }
    }

    #[test]
    fn batch_exec_journals_what_it_reported_and_defers_the_rest() {
        let rows = [
            row("0xaa", "0x01"),
            row("0xbb", "0x02"),
            row("0xcc", "0x03"),
        ];
        let exec = BatchExecSubmit {
            command: concat!(
                "cat >/dev/null; ",
                r#"echo '{"network":"testnet","payer":"0xaa","nonce":"0x01","outcome":"settled"}'; "#,
                "echo 'not json'; ",
                r#"echo '{"network":"testnet","payer":"0xbb","nonce":"0x02","outcome":"failed","detail":"insufficient"}'; "#,
                "exit 9"
            )
            .into(),
        };
        let outcomes = exec.submit_batch(&rows);
        assert_eq!(outcomes[0], Outcome::Settled);
        assert_eq!(outcomes[1], Outcome::Failed("insufficient".into()));
        match &outcomes[2] {
            Outcome::Deferred(why) => assert!(why.contains("exited 9"), "{why}"),
            other => panic!("{other:?}"),
        }

        let dir = tempfile::tempdir().unwrap();
        write_jsonl(&dir.path().join(LOG), &rows).unwrap();
        let report = drain_batched(dir.path(), &exec, 3).unwrap();
        assert_eq!(
            report,
            DrainReport {
                settled: 1,
                failed: 1,
                deferred: 1,
                remaining: 1
            }
        );
        assert!(crate::counter::payer_has_failed(dir.path(), "testnet", "0xbb").unwrap());
        let pending = read_jsonl(&dir.path().join(LOG)).unwrap();
        assert_eq!(pending[0]["payer"], "0xcc");
    }

    #[test]
    fn exec_output_is_bounded_and_an_overlong_line_is_skipped() {
        let exec = BatchExecSubmit {
            command: concat!(
                "cat >/dev/null; ",
                "head -c 300000 /dev/zero | tr '\\0' x; echo; ",
                "head -c 300000 /dev/zero | tr '\\0' e >&2; ",
                r#"echo '{"network":"testnet","payer":"0xaa","nonce":"0x01","outcome":"settled"}'; "#,
                r#"echo '{"network":"testnet","payer":"0xzz","nonce":"0x09","outcome":"settled"}'"#,
            )
            .into(),
        };
        assert_eq!(
            exec.submit_batch(&[row("0xaa", "0x01")]),
            [Outcome::Settled]
        );

        let failing = ExecSubmit {
            command: "cat >/dev/null; head -c 300000 /dev/zero | tr '\\0' e >&2; exit 2".into(),
        };
        match failing.submit(&row("0xaa", "0x01")) {
            Outcome::Failed(detail) => {
                assert!(detail.starts_with("eee"), "{detail}");
                assert!(
                    detail.len() <= EXEC_STDERR_LIMIT as usize,
                    "{}",
                    detail.len()
                );
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_command_that_writes_before_reading_a_large_batch_does_not_deadlock() {
        let rows: Vec<_> = (0..2000)
            .map(|n| row("0xaa", &format!("0x{n:04x}")))
            .collect();
        let exec = BatchExecSubmit {
            command: "head -c 1000000 /dev/zero | tr '\\0' y; echo; cat >/dev/null".into(),
        };
        let outcomes = exec.submit_batch(&rows);
        assert_eq!(outcomes.len(), rows.len());
        assert!(outcomes.iter().all(|o| matches!(o, Outcome::Deferred(_))));
    }
}
