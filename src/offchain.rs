//! Immutable local file snapshots (RFC-0045 stage 1).
//!
//! This is intentionally outside the chain catalogue: importing a file must make it queryable, not
//! make it an input to replay. The catalogue is content-addressed and records enough provenance to
//! reproduce a result from the retained snapshot.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub const DIR: &str = "offchain";
const SEGMENTS: &str = "segments";
const MANIFEST: &str = "manifest.json";

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Catalogue {
    #[serde(default)]
    pub tables: BTreeMap<String, Vec<Snapshot>>,
    /// Last host-side pull outcome per table. This is deliberately beside, not inside, the immutable
    /// snapshot list: a failed attempt must not rewrite history merely to say it failed.
    #[serde(default)]
    pub refreshes: BTreeMap<String, Refresh>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Refresh {
    /// The pulled URL with its query string redacted, as [`redacted`] renders it.
    pub source: String,
    pub attempted_at: String,
    pub succeeded_at: Option<String>,
    pub error: Option<String>,
    /// The operator's declared cadence: past this age the status view reads stale even with no
    /// failure recorded, so a timer that stopped firing is visible.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stale_after_secs: Option<u64>,
    /// The last successful pull's served-bytes SHA-256 and the snapshot it resolved to. Recorded here
    /// as well as on a snapshot the pull creates, because identical content deduplicates onto an
    /// existing snapshot, possibly a dropped file's, whose record says nothing about this fetch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fetched_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub hash: String,
    pub file: String,
    pub rows: usize,
    pub columns: Vec<String>,
    pub source: String,
    pub ingested_at: String,
    pub tool_version: String,
    /// SHA-256 of the bytes the source served, before conversion (RFC-0045 §6). Pulls only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fetched_sha256: Option<String>,
}

pub fn catalogue_path(dir: &Path) -> PathBuf {
    dir.join(DIR).join(MANIFEST)
}

pub fn segment_path(dir: &Path, snapshot: &Snapshot) -> PathBuf {
    dir.join(DIR).join(SEGMENTS).join(&snapshot.file)
}

pub fn load(dir: &Path) -> Result<Catalogue> {
    let path = catalogue_path(dir);
    match std::fs::read_to_string(&path) {
        Ok(raw) => serde_json::from_str(&raw).context("corrupt offchain provenance manifest"),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Catalogue::default()),
        Err(e) => Err(e).with_context(|| format!("cannot read {}", path.display())),
    }
}

/// Ingest one local snapshot. The source is read now and never during indexing or query execution.
pub fn drop_file(dir: &Path, source: &Path, table: &str) -> Result<()> {
    validate_table(table)?;
    let (bytes, rows, columns) = read_source(source)?;
    seal_snapshot(
        dir,
        table,
        &source.display().to_string(),
        None,
        bytes,
        rows,
        columns,
    )
    .map(|_| ())
}

/// Runtime settings for one pull. None of it is written to the nest: a provider URL, key or header
/// is an access path, not part of what the nest is.
pub struct PullOptions {
    pub max_bytes: u64,
    /// Per attempt, response body included.
    pub timeout: Duration,
    pub attempts: u32,
    /// Doubled after each transient failure.
    pub backoff: Duration,
    pub headers: Vec<(String, String)>,
    pub allow_loopback_http: bool,
    pub stale_after_secs: Option<u64>,
}

impl Default for PullOptions {
    fn default() -> Self {
        Self {
            max_bytes: 16 * 1024 * 1024,
            timeout: Duration::from_secs(30),
            attempts: 3,
            backoff: Duration::from_secs(1),
            headers: Vec::new(),
            allow_loopback_http: false,
            stale_after_secs: None,
        }
    }
}

/// Fetch one JSON-array feed outside the chain cursor, then append it through the same immutable
/// snapshot path as [`drop_file`]. A failure records itself and leaves every earlier snapshot as it
/// was, so the table goes stale rather than wrong.
pub async fn pull_json(dir: &Path, source: &str, table: &str, opts: &PullOptions) -> Result<()> {
    validate_table(table)?;
    let url = checked_source(source, opts.allow_loopback_http)?;
    let recorded = redacted(&url);
    tracing::info!(source = %recorded, table, "fetching offchain snapshot out of band");
    let result = async {
        let body = fetch_bounded(&url, opts).await?;
        let fetched = hex::encode(Sha256::digest(&body));
        let (bytes, rows, columns) = read_json_bytes(&body)?;
        let snapshot = seal_snapshot(
            dir,
            table,
            &recorded,
            Some(fetched.clone()),
            bytes,
            rows,
            columns,
        )?;
        anyhow::Ok((fetched, snapshot))
    }
    .await;
    let outcome = match &result {
        Ok(fetch) => Ok(fetch.clone()),
        Err(e) => Err(format!("{e:#}")),
    };
    record_refresh(dir, table, &recorded, outcome, opts.stale_after_secs)?;
    result
        .map(|_| ())
        .with_context(|| format!("pulling offchain__{table} from {recorded}"))
}

/// Resolve `NAME=ENV_VAR` pairs at run time. The error names the variable, never a value.
pub fn resolve_header_env(specs: &[String]) -> Result<Vec<(String, String)>> {
    specs
        .iter()
        .map(|spec| {
            let (name, var) = spec
                .split_once('=')
                .filter(|(n, v)| !n.is_empty() && !v.is_empty())
                .with_context(|| format!("--header-env `{spec}` must be NAME=ENV_VAR"))?;
            match std::env::var(var) {
                Ok(value) if !value.is_empty() => Ok((name.to_string(), value)),
                _ => bail!("--header-env {name}: environment variable {var} is unset or empty"),
            }
        })
        .collect()
}

fn checked_source(source: &str, allow_loopback_http: bool) -> Result<reqwest::Url> {
    let url = reqwest::Url::parse(source).context("offchain source is not a valid URL")?;
    if !url.username().is_empty() || url.password().is_some() {
        bail!("offchain source must not carry credentials in the URL; pass them with --header-env");
    }
    match url.scheme() {
        "https" => Ok(url),
        "http" if allow_loopback_http && is_loopback(&url) => Ok(url),
        "http" if allow_loopback_http => {
            bail!("--allow-loopback-http permits http only to localhost, 127.0.0.0/8 or ::1")
        }
        "http" => bail!("offchain source must be https; plain http is refused"),
        other => bail!("offchain source scheme `{other}` is not supported; use https"),
    }
}

fn is_loopback(url: &reqwest::Url) -> bool {
    match url.host_str() {
        Some(h) if h.eq_ignore_ascii_case("localhost") => true,
        Some(h) => h
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback()),
        None => false,
    }
}

/// The URL as provenance records it: a query string can carry a provider key, so it is dropped.
fn redacted(url: &reqwest::Url) -> String {
    let mut shown = url.clone();
    let had_query = shown.query().is_some();
    shown.set_query(None);
    shown.set_fragment(None);
    let mut out = shown.to_string();
    if had_query {
        out.push_str("?[redacted]");
    }
    out
}

enum Fetch {
    Transient(anyhow::Error),
    Permanent(anyhow::Error),
}

async fn fetch_bounded(url: &reqwest::Url, opts: &PullOptions) -> Result<Vec<u8>> {
    // Redirects are refused, not followed: one could step from https down to http.
    let client = reqwest::Client::builder()
        .timeout(opts.timeout)
        .redirect(reqwest::redirect::Policy::none())
        .user_agent(concat!("nuthatch/", env!("CARGO_PKG_VERSION")))
        .build()?;
    let attempts = opts.attempts.max(1);
    let mut attempt = 1;
    loop {
        match fetch_once(&client, url, opts).await {
            Ok(body) => return Ok(body),
            Err(Fetch::Transient(e)) if attempt < attempts => {
                let wait = opts.backoff * 2u32.pow((attempt - 1).min(5));
                tracing::warn!(
                    "offchain fetch attempt {attempt} failed, retrying in {wait:?}: {e:#}"
                );
                tokio::time::sleep(wait).await;
                attempt += 1;
            }
            Err(Fetch::Transient(e) | Fetch::Permanent(e)) => {
                return Err(e.context(format!("after {attempt} attempt(s)")));
            }
        }
    }
}

async fn fetch_once(
    client: &reqwest::Client,
    url: &reqwest::Url,
    opts: &PullOptions,
) -> std::result::Result<Vec<u8>, Fetch> {
    let mut request = client.get(url.clone());
    for (name, value) in &opts.headers {
        request = request.header(name, value);
    }
    // `without_url`: reqwest's error text carries the full URL, query string and all.
    let mut response = request
        .send()
        .await
        .map_err(|e| classify(e.without_url()))?;
    let status = response.status();
    if status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        return Err(Fetch::Transient(anyhow::anyhow!(
            "source answered HTTP {status}"
        )));
    }
    if !status.is_success() {
        let hint = if status.is_redirection() {
            "; redirects are not followed, so configure the final URL"
        } else {
            ""
        };
        return Err(Fetch::Permanent(anyhow::anyhow!(
            "source answered HTTP {status}{hint}"
        )));
    }
    if let Some(len) = response
        .content_length()
        .filter(|&len| len > opts.max_bytes)
    {
        return Err(Fetch::Permanent(anyhow::anyhow!(
            "response is {len} bytes, over the {} byte limit",
            opts.max_bytes
        )));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| classify(e.without_url()))?
    {
        if (body.len() + chunk.len()) as u64 > opts.max_bytes {
            return Err(Fetch::Permanent(anyhow::anyhow!(
                "response exceeds the {} byte limit",
                opts.max_bytes
            )));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn classify(e: reqwest::Error) -> Fetch {
    if e.is_timeout() || e.is_connect() || e.is_request() {
        Fetch::Transient(e.into())
    } else {
        Fetch::Permanent(e.into())
    }
}

fn seal_snapshot(
    dir: &Path,
    table: &str,
    source: &str,
    fetched_sha256: Option<String>,
    bytes: Vec<u8>,
    rows: usize,
    columns: Vec<String>,
) -> Result<String> {
    if columns.is_empty() {
        bail!("offchain source {source} has no columns");
    }
    let hash = hex::encode(Sha256::digest(&bytes));
    let out_dir = dir.join(DIR).join(SEGMENTS);
    let file = format!("{table}-{hash}.parquet");
    let out = out_dir.join(&file);
    // A file here that is not these bytes was torn by an in-place write before #2179.
    if out.exists() && std::fs::read(&out).ok().as_deref() != Some(&bytes[..]) {
        std::fs::remove_file(&out).with_context(|| format!("removing torn {}", out.display()))?;
    }
    crate::seal::publish_durable(&out_dir, &file, &bytes)?;
    let resolved = hash.clone();
    with_manifest(dir, |catalogue| {
        refuse_case_collision(catalogue, table)?;
        let snapshots = catalogue.tables.entry(table.to_string()).or_default();
        if snapshots.iter().any(|s| s.hash == hash) {
            return Ok(false);
        }
        snapshots.push(Snapshot {
            hash,
            file,
            rows,
            columns,
            source: source.to_string(),
            ingested_at: now_stamp(),
            tool_version: env!("CARGO_PKG_VERSION").to_string(),
            fetched_sha256,
        });
        Ok(true)
    })?;
    println!("sealed offchain snapshot for offchain__{table}");
    Ok(resolved)
}

/// **A case-insensitive collision is refused, not merged.**
///
/// The manifest is a `BTreeMap` and would happily hold both `Prices` and `prices` as separate
/// tables. `define_offchain_views` then creates `offchain__Prices` and `offchain__prices`, and a
/// view's name is lowercased when it is defined and resolved case-insensitively - so the second
/// `CREATE OR REPLACE VIEW` replaces the first and one table silently answers with the other's rows.
///
/// Refusing is the honest half of the fix. Lower-casing the name instead would make the two the
/// same table, which is a guess about intent: an operator who dropped `Prices` and `prices` may
/// well have meant two datasets, and quietly concatenating them is the same silent wrongness in
/// a different place. The error names the existing table so the choice is theirs. A table known
/// only from a failed pull counts, because its status view would collide the same way.
fn refuse_case_collision(catalogue: &Catalogue, table: &str) -> Result<()> {
    if let Some(existing) = catalogue
        .tables
        .keys()
        .chain(catalogue.refreshes.keys())
        .find(|k| k.as_str() != table && k.eq_ignore_ascii_case(table))
    {
        bail!(
            "offchain table `{table}` collides with `{existing}`, which is already in the \
             manifest: SQL view names are case-insensitive, so both would resolve to the same \
             view and one would silently answer with the other's rows. Rename one of them."
        );
    }
    Ok(())
}

/// One read-modify-write of the manifest, under an exclusive lock: two timers pulling different
/// tables in the same minute would otherwise each save a manifest missing the other's entry.
/// `edit` returns whether it changed anything.
fn with_manifest(dir: &Path, edit: impl FnOnce(&mut Catalogue) -> Result<bool>) -> Result<()> {
    let root = dir.join(DIR);
    std::fs::create_dir_all(&root).with_context(|| format!("cannot create {}", root.display()))?;
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(root.join(".manifest.lock"))
        .context("opening the offchain manifest lock")?;
    lock.lock().context("locking the offchain manifest")?;
    let mut catalogue = load(dir)?;
    if edit(&mut catalogue)? {
        save(dir, &catalogue)?;
    }
    Ok(())
}

fn read_source(path: &Path) -> Result<(Vec<u8>, usize, Vec<String>)> {
    let ext = path
        .extension()
        .and_then(|x| x.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    match ext.as_str() {
        "csv" => read_csv(path),
        "json" => read_json(path),
        "parquet" => read_parquet(path),
        _ => bail!(
            "offchain source {} must be CSV, JSON, or Parquet",
            path.display()
        ),
    }
}

fn read_csv(path: &Path) -> Result<(Vec<u8>, usize, Vec<String>)> {
    let mut reader =
        csv::Reader::from_path(path).with_context(|| format!("reading {}", path.display()))?;
    let headers = reader
        .headers()?
        .iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
    validate_columns(&headers)?;
    let mut rows = Vec::new();
    for record in reader.records() {
        let record = record?;
        if record.len() != headers.len() {
            bail!(
                "CSV row has {} fields; header has {}",
                record.len(),
                headers.len()
            );
        }
        let mut obj = Map::new();
        for (name, value) in headers.iter().zip(record.iter()) {
            obj.insert(name.clone(), Value::String(value.to_string()));
        }
        rows.push(Value::Object(obj));
    }
    Ok((
        crate::seal::write_snapshot_parquet(&rows)?,
        rows.len(),
        headers,
    ))
}

fn read_json(path: &Path) -> Result<(Vec<u8>, usize, Vec<String>)> {
    let raw = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    read_json_bytes(&raw)
}

fn read_json_bytes(raw: &[u8]) -> Result<(Vec<u8>, usize, Vec<String>)> {
    let rows: Vec<Value> =
        serde_json::from_slice(raw).context("JSON source must be an array of objects")?;
    let mut columns = std::collections::BTreeSet::new();
    for row in &rows {
        let Some(obj) = row.as_object() else {
            bail!("JSON source must contain objects, not scalar values");
        };
        columns.extend(obj.keys().cloned());
    }
    let columns = columns.into_iter().collect::<Vec<_>>();
    validate_columns(&columns)?;
    Ok((
        crate::seal::write_snapshot_parquet(&rows)?,
        rows.len(),
        columns,
    ))
}

/// **One file description, read twice, rather than one path opened twice.**
///
/// This used to build the metadata from `File::open(path)` and then take the bytes from a separate
/// `std::fs::read(path)`. A source replaced between those two calls - a feed rewriting its export,
/// an operator re-running a job - would be described by the first file's row count and columns while
/// the stored snapshot held the second file's bytes, and the manifest would state something the
/// segment does not contain. Nothing about that is detectable afterwards, because the hash is taken
/// over the bytes that won.
///
/// The path is resolved once, here, and everything else happens on [`read_parquet_handle`], which
/// has no path to re-open. That is deliberate: a test can assert the property, but a signature that
/// cannot express the defect is worth more than a test that watches for it.
fn read_parquet(path: &Path) -> Result<(Vec<u8>, usize, Vec<String>)> {
    let file = std::fs::File::open(path).with_context(|| format!("reading {}", path.display()))?;
    read_parquet_handle(file).with_context(|| format!("reading {}", path.display()))
}

/// Metadata and bytes from one open file description.
///
/// `try_clone` shares the description, so both reads see the same inode whatever happens to the name
/// it was opened under. The offset is shared with it, hence the explicit rewind before taking the
/// bytes.
fn read_parquet_handle(mut file: std::fs::File) -> Result<(Vec<u8>, usize, Vec<String>)> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::io::{Read, Seek, SeekFrom};
    let builder = ParquetRecordBatchReaderBuilder::try_new(file.try_clone()?)
        .context("invalid Parquet source")?;
    let columns = builder
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect::<Vec<_>>();
    validate_columns(&columns)?;
    let rows = builder.metadata().file_metadata().num_rows() as usize;
    drop(builder);
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok((bytes, rows, columns))
}

fn validate_table(table: &str) -> Result<()> {
    if table.is_empty()
        || !table
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
    {
        bail!("offchain table name must contain only letters, digits, and '_'");
    }
    if table.to_ascii_lowercase().ends_with("__status") {
        bail!(
            "offchain table name may not end in `__status`: `offchain__<table>__status` is the \
             status view of a pulled table, and this name would collide with one"
        );
    }
    Ok(())
}
fn validate_columns(columns: &[String]) -> Result<()> {
    if columns.iter().any(|c| c.is_empty()) {
        bail!("offchain source has an empty column name");
    }
    Ok(())
}
fn save(dir: &Path, catalogue: &Catalogue) -> Result<()> {
    crate::seal::replace_durable(&catalogue_path(dir), &serde_json::to_vec_pretty(catalogue)?)
        .context("installing offchain provenance manifest")
}

/// `outcome` is the served-bytes hash and resolved snapshot on success, or the error text. A failure
/// keeps the last success's time and fetch, so the status still names what is being served.
fn record_refresh(
    dir: &Path,
    table: &str,
    source: &str,
    outcome: std::result::Result<(String, String), String>,
    stale_after_secs: Option<u64>,
) -> Result<()> {
    with_manifest(dir, |catalogue| {
        refuse_case_collision(catalogue, table)?;
        let now = now_stamp();
        let previous = catalogue.refreshes.get(table);
        let (succeeded_at, fetched_sha256, snapshot, error) = match outcome {
            Ok((fetched, snapshot)) => (Some(now.clone()), Some(fetched), Some(snapshot), None),
            Err(error) => (
                previous.and_then(|r| r.succeeded_at.clone()),
                previous.and_then(|r| r.fetched_sha256.clone()),
                previous.and_then(|r| r.snapshot.clone()),
                Some(error),
            ),
        };
        catalogue.refreshes.insert(
            table.to_string(),
            Refresh {
                source: source.to_string(),
                attempted_at: now,
                succeeded_at,
                error,
                stale_after_secs,
                fetched_sha256,
                snapshot,
            },
        );
        Ok(true)
    })
}
fn now_stamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    format!(
        "unix:{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn serve(app: axum::Router) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (base, server)
    }

    fn local() -> PullOptions {
        PullOptions {
            allow_loopback_http: true,
            backoff: Duration::from_millis(5),
            ..Default::default()
        }
    }

    const PRICES: &str = r#"[{"token":"WETH","price":"3210"}]"#;

    #[tokio::test]
    async fn a_pull_appends_one_snapshot_per_distinct_body_with_redacted_provenance() {
        use axum::{routing::get, Router};
        let (base, server) = serve(Router::new().route("/prices", get(|| async { PRICES }))).await;
        let dir = tempfile::tempdir().unwrap();
        let source = format!("{base}/prices?apikey=SEKRET");

        pull_json(dir.path(), &source, "prices", &local())
            .await
            .unwrap();
        pull_json(dir.path(), &source, "prices", &local())
            .await
            .unwrap();
        server.abort();

        let catalogue = load(dir.path()).unwrap();
        let snapshots = &catalogue.tables["prices"];
        assert_eq!(
            snapshots.len(),
            1,
            "an unchanged body appends nothing twice"
        );
        assert_eq!(snapshots[0].source, format!("{base}/prices?[redacted]"));
        assert_eq!(
            snapshots[0].fetched_sha256.as_deref(),
            Some(hex::encode(Sha256::digest(PRICES.as_bytes())).as_str()),
            "provenance hashes the bytes the source served, not the converted Parquet"
        );
        let raw = std::fs::read_to_string(catalogue_path(dir.path())).unwrap();
        assert!(
            !raw.contains("SEKRET"),
            "a query-string key reached the manifest: {raw}"
        );
        let rows = crate::analytics::query(dir.path(), "SELECT token, price FROM offchain__prices")
            .unwrap();
        assert_eq!(rows[0]["price"], "3210");
    }

    /// Identical content deduplicates onto an existing snapshot, here a dropped file's. The pull must
    /// still record what the source served, without rewriting the drop's own record (Jules on #1448).
    #[tokio::test]
    async fn a_pull_deduplicated_onto_a_dropped_snapshot_still_records_its_fetch() {
        use axum::{routing::get, Router};
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("prices.json");
        std::fs::write(&input, PRICES).unwrap();
        drop_file(dir.path(), &input, "prices").unwrap();
        let (base, server) = serve(Router::new().route("/p", get(|| async { PRICES }))).await;
        pull_json(dir.path(), &format!("{base}/p"), "prices", &local())
            .await
            .unwrap();
        server.abort();

        let catalogue = load(dir.path()).unwrap();
        let snapshots = &catalogue.tables["prices"];
        assert_eq!(snapshots.len(), 1, "identical content is one snapshot");
        assert_eq!(
            snapshots[0].source,
            input.display().to_string(),
            "the drop's record is untouched"
        );
        let refresh = &catalogue.refreshes["prices"];
        assert_eq!(
            refresh.snapshot.as_deref(),
            Some(snapshots[0].hash.as_str())
        );
        assert_eq!(
            refresh.fetched_sha256.as_deref(),
            Some(hex::encode(Sha256::digest(PRICES.as_bytes())).as_str()),
            "the served bytes' hash must be in the manifest even when no snapshot was appended"
        );
    }

    #[tokio::test]
    async fn a_failed_refresh_keeps_the_last_snapshot_and_reads_stale() {
        use axum::{http::StatusCode, routing::get, Router};
        let (good, server) = serve(Router::new().route("/p", get(|| async { PRICES }))).await;
        let dir = tempfile::tempdir().unwrap();
        pull_json(dir.path(), &format!("{good}/p"), "prices", &local())
            .await
            .unwrap();
        server.abort();
        let (bad, server) = serve(Router::new().route(
            "/p",
            get(|| async { (StatusCode::INTERNAL_SERVER_ERROR, "down") }),
        ))
        .await;

        let err = pull_json(dir.path(), &format!("{bad}/p"), "prices", &local())
            .await
            .unwrap_err();
        server.abort();
        assert!(format!("{err:#}").contains("HTTP 500"), "{err:#}");
        assert_eq!(load(dir.path()).unwrap().tables["prices"].len(), 1);

        let rows =
            crate::analytics::query(dir.path(), "SELECT price FROM offchain__prices").unwrap();
        assert_eq!(
            rows[0]["price"], "3210",
            "the last good price still answers"
        );
        let status = crate::analytics::query(
            dir.path(),
            "SELECT stale, succeeded_at, error, snapshot FROM offchain__prices__status",
        )
        .unwrap();
        assert_eq!(status[0]["stale"], true);
        assert!(
            status[0]["succeeded_at"].is_string(),
            "the last success stays named"
        );
        assert!(
            status[0]["snapshot"].is_string(),
            "and so does the snapshot being served"
        );
        assert!(status[0]["error"].as_str().unwrap().contains("HTTP 500"));
    }

    #[tokio::test]
    async fn a_first_ever_failure_is_unavailable_not_an_empty_table() {
        let dir = tempfile::tempdir().unwrap();
        let opts = PullOptions {
            attempts: 1,
            ..local()
        };
        assert!(
            pull_json(dir.path(), "http://127.0.0.1:1/p", "prices", &opts)
                .await
                .is_err()
        );

        assert!(
            crate::analytics::query(dir.path(), "SELECT * FROM offchain__prices").is_err(),
            "no snapshot must not read as an empty, healthy price table"
        );
        let status = crate::analytics::query(
            dir.path(),
            "SELECT stale, succeeded_at FROM offchain__prices__status",
        )
        .unwrap();
        assert_eq!(status[0]["stale"], true);
        assert!(status[0]["succeeded_at"].is_null());
    }

    #[tokio::test]
    async fn transient_failures_are_retried_and_a_permanent_one_is_not() {
        use axum::response::IntoResponse;
        use axum::{http::StatusCode, routing::get, Router};
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let app = Router::new()
            .route(
                "/flaky",
                get(move || {
                    let n = seen.fetch_add(1, Ordering::SeqCst);
                    async move {
                        if n < 2 {
                            (StatusCode::SERVICE_UNAVAILABLE, "").into_response()
                        } else {
                            PRICES.into_response()
                        }
                    }
                }),
            )
            .route("/gone", get(|| async { (StatusCode::NOT_FOUND, "") }));
        let (base, server) = serve(app).await;
        let dir = tempfile::tempdir().unwrap();

        pull_json(dir.path(), &format!("{base}/flaky"), "prices", &local())
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 3, "two 503s, then the answer");

        let err = pull_json(dir.path(), &format!("{base}/gone"), "gone", &local())
            .await
            .unwrap_err();
        server.abort();
        assert!(format!("{err:#}").contains("after 1 attempt(s)"), "{err:#}");
    }

    #[tokio::test]
    async fn an_oversized_response_is_refused_and_appends_nothing() {
        use axum::{routing::get, Router};
        let big = format!("[{}]", vec![r#"{"t":"x"}"#; 400].join(","));
        let (base, server) =
            serve(Router::new().route("/p", get(move || async move { big }))).await;
        let dir = tempfile::tempdir().unwrap();
        let opts = PullOptions {
            max_bytes: 1024,
            ..local()
        };

        let err = pull_json(dir.path(), &format!("{base}/p"), "prices", &opts)
            .await
            .unwrap_err();
        server.abort();
        assert!(format!("{err:#}").contains("byte limit"), "{err:#}");
        assert!(!load(dir.path()).unwrap().tables.contains_key("prices"));
    }

    #[tokio::test]
    async fn a_slow_source_times_out() {
        use axum::{routing::get, Router};
        let app = Router::new().route(
            "/p",
            get(|| async {
                tokio::time::sleep(Duration::from_secs(5)).await;
                PRICES
            }),
        );
        let (base, server) = serve(app).await;
        let dir = tempfile::tempdir().unwrap();
        let opts = PullOptions {
            timeout: Duration::from_millis(200),
            attempts: 1,
            ..local()
        };
        let started = std::time::Instant::now();
        let err = pull_json(dir.path(), &format!("{base}/p"), "prices", &opts)
            .await
            .unwrap_err();
        server.abort();
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "the timeout did not bound the attempt"
        );
        assert!(
            format!("{err:#}").to_lowercase().contains("timed out"),
            "{err:#}"
        );
    }

    #[tokio::test]
    async fn a_redirect_is_refused_rather_than_followed() {
        use axum::{
            http::{header, StatusCode},
            routing::get,
            Router,
        };
        let app = Router::new().route(
            "/p",
            get(|| async {
                (
                    StatusCode::FOUND,
                    [(header::LOCATION, "http://127.0.0.1:1/p")],
                    "",
                )
            }),
        );
        let (base, server) = serve(app).await;
        let dir = tempfile::tempdir().unwrap();
        let err = pull_json(dir.path(), &format!("{base}/p"), "prices", &local())
            .await
            .unwrap_err();
        server.abort();
        assert!(
            format!("{err:#}").contains("redirects are not followed"),
            "{err:#}"
        );
    }

    #[tokio::test]
    async fn neither_a_query_key_nor_a_header_reaches_the_manifest_even_on_failure() {
        use axum::{http::StatusCode, routing::get, Router};
        let (base, server) =
            serve(Router::new().route("/p", get(|| async { (StatusCode::BAD_GATEWAY, "") }))).await;
        let dir = tempfile::tempdir().unwrap();
        let opts = PullOptions {
            attempts: 1,
            headers: vec![("authorization".into(), "Bearer HEADERSEKRET".into())],
            ..local()
        };
        let err = pull_json(
            dir.path(),
            &format!("{base}/p?apikey=QUERYSEKRET"),
            "prices",
            &opts,
        )
        .await
        .unwrap_err();
        // An unreachable host fails inside reqwest, whose error text carries the URL.
        let unreachable = pull_json(
            dir.path(),
            "http://127.0.0.1:1/p?apikey=QUERYSEKRET",
            "prices",
            &opts,
        )
        .await
        .unwrap_err();
        server.abort();

        let raw = std::fs::read_to_string(catalogue_path(dir.path())).unwrap();
        for text in [raw, format!("{err:#}"), format!("{unreachable:#}")] {
            assert!(!text.contains("SEKRET"), "a secret leaked: {text}");
        }
    }

    #[test]
    fn plain_http_needs_the_loopback_exception_and_the_exception_is_loopback_only() {
        assert!(checked_source("https://feed.example/p", false).is_ok());
        assert!(checked_source("http://127.0.0.1:9/p", false).is_err());
        assert!(checked_source("http://127.0.0.1:9/p", true).is_ok());
        assert!(checked_source("http://[::1]:9/p", true).is_ok());
        assert!(checked_source("http://localhost:9/p", true).is_ok());
        assert!(checked_source("http://feed.example/p", true).is_err());
        assert!(checked_source("ftp://feed.example/p", true).is_err());
        assert!(checked_source("https://user:pass@feed.example/p", false).is_err());
    }

    #[test]
    fn a_success_older_than_the_declared_cadence_reads_stale() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("p.json");
        std::fs::write(&input, PRICES).unwrap();
        drop_file(dir.path(), &input, "old").unwrap();
        drop_file(dir.path(), &input, "fresh").unwrap();
        let now = now_stamp();
        let refresh = |succeeded: &str| Refresh {
            source: "https://feed.example/p".into(),
            attempted_at: succeeded.to_string(),
            succeeded_at: Some(succeeded.to_string()),
            error: None,
            stale_after_secs: Some(3600),
            fetched_sha256: None,
            snapshot: None,
        };
        with_manifest(dir.path(), |c| {
            c.refreshes.insert("old".into(), refresh("unix:1"));
            c.refreshes.insert("fresh".into(), refresh(&now));
            Ok(true)
        })
        .unwrap();

        let old =
            crate::analytics::query(dir.path(), "SELECT stale FROM offchain__old__status").unwrap();
        let fresh =
            crate::analytics::query(dir.path(), "SELECT stale FROM offchain__fresh__status")
                .unwrap();
        assert_eq!(
            old[0]["stale"], true,
            "a timer that stopped firing must show"
        );
        assert_eq!(fresh[0]["stale"], false);
    }

    /// Timers for different feeds can fire in the same minute; without the manifest lock their
    /// read-modify-writes race, losing entries or tearing the shared temporary file.
    #[test]
    fn concurrent_refreshes_of_different_tables_lose_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let writers: Vec<_> = (0..16)
            .map(|i| {
                let d = dir.path().to_path_buf();
                std::thread::spawn(move || {
                    for _ in 0..5 {
                        record_refresh(
                            &d,
                            &format!("t{i}"),
                            "https://feed.example/p",
                            Ok(("fetched".into(), "snapshot".into())),
                            None,
                        )
                        .unwrap();
                    }
                })
            })
            .collect();
        for w in writers {
            w.join().unwrap();
        }
        assert_eq!(load(dir.path()).unwrap().refreshes.len(), 16);
    }

    #[test]
    fn a_table_name_ending_in_the_status_suffix_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("p.csv");
        std::fs::write(&input, "token\nWETH\n").unwrap();
        let err = drop_file(dir.path(), &input, "prices__Status")
            .unwrap_err()
            .to_string();
        assert!(err.contains("__status"), "{err}");
    }

    #[test]
    fn a_missing_header_variable_is_named_and_its_value_never_is() {
        let err = resolve_header_env(&["authorization=NUTHATCH_TEST_UNSET_1436".into()])
            .unwrap_err()
            .to_string();
        assert!(err.contains("NUTHATCH_TEST_UNSET_1436"), "{err}");
        assert!(resolve_header_env(&["no-equals-sign".into()]).is_err());
    }

    #[test]
    fn csv_snapshot_is_content_addressed_and_records_provenance() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("prices.csv");
        std::fs::write(&input, "token,price\nWETH,3210\n").unwrap();
        drop_file(dir.path(), &input, "prices").unwrap();

        let catalogue = load(dir.path()).unwrap();
        let snapshot = &catalogue.tables["prices"][0];
        assert_eq!(snapshot.rows, 1);
        assert_eq!(snapshot.columns, ["token", "price"]);
        assert_eq!(snapshot.source, input.display().to_string());
        assert!(dir
            .path()
            .join(DIR)
            .join(SEGMENTS)
            .join(&snapshot.file)
            .exists());

        drop_file(dir.path(), &input, "prices").unwrap();
        assert_eq!(load(dir.path()).unwrap().tables["prices"].len(), 1);
    }

    #[test]
    fn case_only_table_names_are_refused_in_either_order() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("prices.csv");
        std::fs::write(&input, "token,price\nWETH,3210\n").unwrap();
        drop_file(dir.path(), &input, "Prices").unwrap();

        // The mixed-case name first, the lowercase one second - the mirror of the sibling test
        // above, because a guard that only compares one direction would pass one and fail the other.
        let err = drop_file(dir.path(), &input, "prices").unwrap_err();
        assert!(err.to_string().contains("collides with"), "{err:#}");
    }

    #[test]
    fn imported_snapshot_joins_sealed_chain_data_on_the_normal_sql_surface() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("prices.csv");
        std::fs::write(&input, "token,price\nWETH,3210\n").unwrap();
        drop_file(dir.path(), &input, "prices").unwrap();

        crate::seal::seal_range(
            dir.path(),
            &[r#"{"table":"dex__swap","token":"WETH","amount":"2","block_number":10,"log_index":0,"tx_hash":"0xabc"}"#.to_string()],
            10,
            10,
        )
        .unwrap();

        let rows = crate::analytics::query(
            dir.path(),
            r#"SELECT s.token, s.amount, p.price
               FROM "dex__swap" s JOIN offchain__prices p USING (token)"#,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["token"], "WETH");
        assert_eq!(rows[0]["amount"], "2");
        assert_eq!(rows[0]["price"], "3210");
    }

    #[test]
    fn removing_offchain_snapshots_does_not_prevent_chain_data_from_answering() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("prices.json");
        std::fs::write(&input, r#"[{"token":"WETH","price":3210}]"#).unwrap();
        drop_file(dir.path(), &input, "prices").unwrap();
        crate::seal::seal_range(
            dir.path(),
            &[r#"{"table":"dex__swap","token":"WETH","block_number":10,"log_index":0,"tx_hash":"0xabc"}"#.to_string()],
            10,
            10,
        )
        .unwrap();
        std::fs::remove_dir_all(dir.path().join(DIR)).unwrap();

        let rows = crate::analytics::query(dir.path(), r#"SELECT token FROM "dex__swap""#).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["token"], "WETH");
    }

    /// Two table names differing only in case would become one view, and the later
    /// `CREATE OR REPLACE VIEW` would make one table answer with the other's rows. Refused rather
    /// than merged, because merging is a guess about what the operator meant.
    #[test]
    fn a_case_only_table_name_collision_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("p.csv");
        std::fs::write(&src, "token\nWETH\n").unwrap();

        drop_file(dir.path(), &src, "prices").expect("the first drop defines the table");

        let err = drop_file(dir.path(), &src, "Prices")
            .expect_err("a case-only variant must be refused, not silently shadow the first")
            .to_string();
        assert!(err.contains("collides with"), "{err}");
        assert!(
            err.contains("prices"),
            "the error must name the existing table: {err}"
        );

        // And the first table is untouched by the refusal.
        let cat = load(dir.path()).unwrap();
        assert!(cat.tables.contains_key("prices"));
        assert!(!cat.tables.contains_key("Prices"));
    }

    /// The same name is not a collision with itself - re-dropping a table must keep working.
    #[test]
    fn re_dropping_the_same_table_is_not_a_collision() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.csv");
        std::fs::write(&a, "token\nWETH\n").unwrap();
        let b = dir.path().join("b.csv");
        std::fs::write(&b, "token\nUSDC\n").unwrap();
        drop_file(dir.path(), &a, "prices").unwrap();
        drop_file(dir.path(), &b, "prices").expect("a second snapshot of the same table is normal");
        assert_eq!(load(dir.path()).unwrap().tables["prices"].len(), 2);
    }

    /// The manifest must describe the bytes that were stored. Reading metadata from one open of the
    /// path and the bytes from another lets the source change in between, so the row count and
    /// columns end up describing a file the snapshot does not contain.
    #[test]
    fn parquet_metadata_and_bytes_come_from_one_file() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("p.parquet");
        write_parquet_fixture(&src, &["a", "b"], 3);

        let (bytes, rows, columns) = read_parquet(&src).unwrap();
        assert_eq!(rows, 3);
        assert_eq!(columns, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(
            bytes,
            std::fs::read(&src).unwrap(),
            "the bytes returned must be the file that was described"
        );
    }

    /// The property the fix rests on, asserted against `read_parquet_handle` itself rather than
    /// against the file system in isolation: the handle is opened, the **path** is then replaced by
    /// a genuinely different file, and the metadata and bytes that come back must both describe the
    /// version the handle was opened on.
    ///
    /// This is the test the first attempt got wrong twice. `File::create` truncates the same inode
    /// in place, so a "replacement" written that way is visible through an open handle and proves
    /// the opposite; the replacement has to be a `rename`, which is what an atomic publish does. And
    /// asserting only that `read_parquet`'s bytes match the file on disk passes whether or not the
    /// bytes were re-read by path, because in that test nothing changes in between - the mutation
    /// survived, which is how the gap was found.
    #[test]
    fn metadata_and_bytes_describe_the_version_the_handle_was_opened_on() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("p.parquet");
        write_parquet_fixture(&src, &["a", "b"], 3);
        let three = std::fs::read(&src).unwrap();

        let handle = std::fs::File::open(&src).unwrap();

        // An atomic publish of a different file over the same name, after the open.
        let replacement = dir.path().join("p.parquet.new");
        write_parquet_fixture(&replacement, &["a", "b"], 9);
        std::fs::rename(&replacement, &src).unwrap();
        assert_ne!(
            three,
            std::fs::read(&src).unwrap(),
            "the fixture must actually differ, or this proves nothing"
        );

        let (bytes, rows, columns) = read_parquet_handle(handle).unwrap();
        assert_eq!(
            rows, 3,
            "the row count must be the opened version's, not the path's"
        );
        assert_eq!(columns, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(
            bytes, three,
            "the bytes stored must be the same version the row count describes"
        );
    }

    fn write_parquet_fixture(path: &std::path::Path, cols: &[&str], rows: usize) {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;
        let schema = Arc::new(Schema::new(
            cols.iter()
                .map(|c| Field::new(*c, DataType::Utf8, false))
                .collect::<Vec<_>>(),
        ));
        let arrays: Vec<arrow::array::ArrayRef> = cols
            .iter()
            .map(|c| {
                Arc::new(StringArray::from(
                    (0..rows).map(|i| format!("{c}{i}")).collect::<Vec<_>>(),
                )) as arrow::array::ArrayRef
            })
            .collect();
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        let file = std::fs::File::create(path).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(file, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }

    /// #2179: a snapshot written in place and cut short by a crash kept its content-addressed name,
    /// and every later pull of the same body saw the name and catalogued the torn file.
    #[test]
    fn a_torn_snapshot_under_its_content_address_is_replaced_by_the_next_pull() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("p.json");
        std::fs::write(&input, PRICES).unwrap();
        drop_file(dir.path(), &input, "prices").unwrap();
        let snapshot = load(dir.path()).unwrap().tables["prices"][0].clone();
        let path = segment_path(dir.path(), &snapshot);
        let whole = std::fs::read(&path).unwrap();
        std::fs::write(&path, &whole[..whole.len() / 2]).unwrap();

        drop_file(dir.path(), &input, "prices").unwrap();
        assert_eq!(
            std::fs::read(&path).unwrap(),
            whole,
            "the torn file survived a pull of the same body"
        );
    }

    /// #2179: the catalogue's bytes are fsynced before its rename and its directory entry after.
    #[test]
    fn the_catalogue_is_installed_durably() {
        use crate::seal::Durable;
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("p.json");
        std::fs::write(&input, PRICES).unwrap();
        let trace = crate::seal::capture_durable(|| {
            drop_file(dir.path(), &input, "prices").unwrap();
        });
        let root = dir.path().join(DIR);
        let synced = trace
            .iter()
            .position(|e| matches!(e, Durable::FileSync(p) if p.parent() == Some(root.as_path())));
        let installed = trace
            .iter()
            .rposition(|e| matches!(e, Durable::ManifestDir(p) if p == &root));
        assert!(
            matches!((synced, installed), (Some(s), Some(i)) if s < i),
            "trace: {trace:?}"
        );
    }
}
