//! RFC-0052 S1: mirror sealed, non-provisional segments to a prefix.
//!
//! The publisher is a level-triggered reconciler: local catalogue versus remote catalogue.
//! Parquet goes first, then schema, then the catalogue (conditional), then `publish.json`.
//! `BundleStore` is not reused; this talks filesystem paths or `object_store` directly.

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use crate::seal::{self, Manifest, Segment, MANIFEST_FILE};

/// What one `sync` actually wrote, so a second run can assert it only touched `publish.json`.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SyncReport {
    pub dataset: String,
    pub sealed_through: Option<u64>,
    pub uploaded: Vec<String>,
    pub uploaded_bytes: u64,
    pub skipped: usize,
}

#[derive(Debug, Serialize, Deserialize)]
struct PublishEnvelope {
    layout_version: u32,
    nuthatch_version: String,
    chain_id: u64,
    data_identity: String,
    nid: String,
    bundle_hash: String,
    sealed_through: Option<u64>,
    published_at: String,
    catalogue_sha256: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema_sha256: Option<String>,
    tables: Vec<String>,
}

/// What a HEAD says about one object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Head {
    pub(crate) size: u64,
    /// Unquoted. Not necessarily content-derived: an in-memory store counts writes.
    pub(crate) e_tag: Option<String>,
}

#[async_trait]
pub(crate) trait Mirror: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
    async fn put(&self, key: &str, bytes: &[u8]) -> Result<()>;
    async fn put_file(&self, key: &str, src: &Path) -> Result<()>;
    /// Create if `expected` is `None`; otherwise replace only when the current bytes match.
    async fn put_if(&self, key: &str, bytes: &[u8], expected: Option<&[u8]>) -> Result<()>;
    async fn head(&self, key: &str) -> Result<Option<Head>>;
    /// [`Mirror::head`] for a content check, which a filesystem answers by hashing the object.
    async fn head_content(&self, key: &str) -> Result<Option<Head>> {
        self.head(key).await
    }
    /// Whether [`Mirror::head_content`]'s ETag is computed from the bytes, rather than whatever a
    /// store chose to send, which only the operator can vouch for.
    fn content_etags(&self) -> bool {
        false
    }
    /// Write `key` to `dest`, refusing more than `cap` bytes, and return the sha256 of what was written.
    async fn get_file(&self, key: &str, dest: &Path, cap: u64) -> Result<Option<String>> {
        let Some(bytes) = self.get(key).await? else {
            return Ok(None);
        };
        if bytes.len() as u64 > cap {
            bail!("{key} is larger than {cap} bytes");
        }
        std::fs::write(dest, &bytes).with_context(|| format!("writing {}", dest.display()))?;
        Ok(Some(sha256_hex(&bytes)))
    }
}

/// The most an HTTP mirror may send for a document read into memory (a catalogue, a seed snapshot).
const MAX_HTTP_DOCUMENT: u64 = 256 << 20;

/// Read a response a chunk at a time into `sink`, refusing more than `cap` bytes: a mirror or bundle
/// URL is not trusted to end (#2202).
pub(crate) async fn read_capped(
    mut resp: reqwest::Response,
    what: &str,
    cap: u64,
    mut sink: impl FnMut(&[u8]) -> Result<()>,
) -> Result<u64> {
    let mut n = 0u64;
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|e| e.without_url())
        .with_context(|| format!("reading {what}"))?
    {
        n += chunk.len() as u64;
        if n > cap {
            bail!("{what} is larger than {cap} bytes");
        }
        sink(&chunk)?;
    }
    Ok(n)
}

struct FsMirror {
    root: PathBuf,
}

impl FsMirror {
    fn path(&self, key: &str) -> PathBuf {
        self.root.join(key)
    }
}

#[async_trait]
impl Mirror for FsMirror {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        match std::fs::read(self.path(key)) {
            Ok(b) => Ok(Some(b)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).with_context(|| format!("reading {}", key)),
        }
    }

    async fn put(&self, key: &str, bytes: &[u8]) -> Result<()> {
        let path = self.path(key);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        std::fs::write(&path, bytes).with_context(|| format!("writing {}", key))
    }

    async fn put_file(&self, key: &str, src: &Path) -> Result<()> {
        let path = self.path(key);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        std::fs::copy(src, &path)
            .with_context(|| format!("copying {} to {}", src.display(), key))?;
        Ok(())
    }

    async fn put_if(&self, key: &str, bytes: &[u8], expected: Option<&[u8]>) -> Result<()> {
        let path = self.path(key);
        let lock = publication_lock(&format!("fs:{}", path.display()));
        let _g = lock.lock().await;
        let _cross = fs_exclusive_lock(&path)?;
        cas_bytes(self.get(key).await?, expected, key)?;
        self.put(key, bytes).await
    }

    async fn head(&self, key: &str) -> Result<Option<Head>> {
        match std::fs::metadata(self.path(key)) {
            Ok(m) => Ok(Some(Head {
                size: m.len(),
                e_tag: None,
            })),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).with_context(|| format!("stat {}", key)),
        }
    }

    async fn head_content(&self, key: &str) -> Result<Option<Head>> {
        let Some(head) = self.head(key).await? else {
            return Ok(None);
        };
        Ok(Some(Head {
            e_tag: Some(s3_etag(&self.path(key))?),
            ..head
        }))
    }

    fn content_etags(&self) -> bool {
        true
    }
}

/// A mirror read over plain HTTP(S), which is what a public bucket serves. Read-only: `nuthatch seed`
/// is its one caller, and nothing publishes through it.
struct HttpMirror {
    base: String,
    client: reqwest::Client,
}

impl HttpMirror {
    fn new(base: &str) -> Result<Self> {
        let client = reqwest::Client::builder()
            .user_agent(concat!("nuthatch/", env!("CARGO_PKG_VERSION")))
            .build()
            .context("building the HTTP client")?;
        Ok(Self {
            base: base.trim_end_matches('/').to_string(),
            client,
        })
    }

    async fn request(
        &self,
        method: reqwest::Method,
        key: &str,
    ) -> Result<Option<reqwest::Response>> {
        // `without_url`: a presigned or keyed mirror URL must not reach a log through an error.
        let resp = self
            .client
            .request(method, format!("{}/{key}", self.base))
            .send()
            .await
            .map_err(|e| e.without_url())
            .with_context(|| format!("fetching {key}"))?;
        match resp.status() {
            s if s.is_success() => Ok(Some(resp)),
            reqwest::StatusCode::NOT_FOUND => Ok(None),
            // A bucket that forbids listing answers 403 for an absent key, so 403 cannot be read
            // as "not there".
            s => bail!("fetching {key}: the mirror answered {s}"),
        }
    }
}

#[async_trait]
impl Mirror for HttpMirror {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let Some(resp) = self.request(reqwest::Method::GET, key).await? else {
            return Ok(None);
        };
        let mut bytes = Vec::new();
        read_capped(resp, key, MAX_HTTP_DOCUMENT, |c| {
            bytes.extend_from_slice(c);
            Ok(())
        })
        .await?;
        Ok(Some(bytes))
    }

    async fn get_file(&self, key: &str, dest: &Path, cap: u64) -> Result<Option<String>> {
        use std::io::Write;
        let Some(resp) = self.request(reqwest::Method::GET, key).await? else {
            return Ok(None);
        };
        let mut file =
            std::fs::File::create(dest).with_context(|| format!("creating {}", dest.display()))?;
        let mut hasher = Sha256::new();
        read_capped(resp, key, cap, |c| {
            hasher.update(c);
            file.write_all(c)
                .with_context(|| format!("writing {}", dest.display()))
        })
        .await?;
        Ok(Some(hex::encode(hasher.finalize())))
    }

    async fn put(&self, key: &str, _bytes: &[u8]) -> Result<()> {
        bail!("an http mirror is read-only; cannot write {key}")
    }

    async fn put_file(&self, key: &str, _src: &Path) -> Result<()> {
        bail!("an http mirror is read-only; cannot write {key}")
    }

    async fn put_if(&self, key: &str, _bytes: &[u8], _expected: Option<&[u8]>) -> Result<()> {
        bail!("an http mirror is read-only; cannot write {key}")
    }

    async fn head(&self, key: &str) -> Result<Option<Head>> {
        Ok(self
            .request(reqwest::Method::HEAD, key)
            .await?
            .map(|resp| Head {
                size: resp.content_length().unwrap_or(0),
                e_tag: None,
            }))
    }
}

#[cfg(feature = "object-store")]
struct ObjMirror {
    inner: std::sync::Arc<dyn object_store::ObjectStore>,
    prefix: object_store::path::Path,
}

#[cfg(feature = "object-store")]
impl ObjMirror {
    fn from_locator(locator: &str) -> Result<Self> {
        use object_store::path::Path as ObjPath;
        if let Some(rest) = locator.strip_prefix("memory://") {
            return Ok(Self {
                inner: memory_store(locator),
                prefix: ObjPath::from(rest.trim_start_matches('/')),
            });
        }
        let url = url::Url::parse(locator)
            .with_context(|| format!("parsing publish target {locator:?}"))?;
        let opts = std::env::vars().map(|(k, v)| (k.to_ascii_lowercase(), v));
        let (store, path) = object_store::parse_url_opts(&url, opts)
            .with_context(|| format!("opening publish target {locator:?}"))?;
        Ok(Self {
            inner: std::sync::Arc::from(store),
            prefix: path,
        })
    }

    fn key(&self, key: &str) -> object_store::path::Path {
        let mut p = self.prefix.clone();
        for part in key.split('/') {
            if !part.is_empty() {
                p = p.child(part);
            }
        }
        p
    }
}

#[cfg(feature = "object-store")]
#[async_trait]
impl Mirror for ObjMirror {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        use object_store::ObjectStore as _;
        match self.inner.get(&self.key(key)).await {
            Ok(g) => Ok(Some(g.bytes().await.context("reading object")?.to_vec())),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(anyhow::Error::new(e).context(key.to_string())),
        }
    }

    async fn put(&self, key: &str, bytes: &[u8]) -> Result<()> {
        use object_store::ObjectStore as _;
        self.inner
            .put(
                &self.key(key),
                object_store::PutPayload::from(bytes.to_vec()),
            )
            .await
            .with_context(|| format!("putting {key}"))?;
        Ok(())
    }

    /// Streams `src` in [`PART_BUFFER`] parts, one in flight, so an upload holds one part and never
    /// the segment (RFC-0052 §3.4's `publish_headroom`).
    async fn put_file(&self, key: &str, src: &Path) -> Result<()> {
        use object_store::ObjectStore as _;
        use std::io::Read as _;
        let read = |file: &mut std::fs::File| -> Result<Vec<u8>> {
            let mut part = Vec::with_capacity(PART_BUFFER);
            file.take(PART_BUFFER as u64)
                .read_to_end(&mut part)
                .with_context(|| format!("reading {}", src.display()))?;
            Ok(part)
        };
        let mut file =
            std::fs::File::open(src).with_context(|| format!("opening {}", src.display()))?;
        let first = read(&mut file)?;
        if first.len() < PART_BUFFER {
            return self.put(key, &first).await;
        }
        let loc = self.key(key);
        let mut upload = self
            .inner
            .put_multipart(&loc)
            .await
            .with_context(|| format!("starting upload of {key}"))?;
        let mut part = first;
        while !part.is_empty() {
            let sent = upload.put_part(part.into()).await;
            let next = sent
                .map_err(anyhow::Error::from)
                .and_then(|()| read(&mut file));
            match next {
                Ok(p) => part = p,
                Err(e) => {
                    let _ = upload.abort().await;
                    return Err(e.context(format!("uploading {key}")));
                }
            }
        }
        upload
            .complete()
            .await
            .with_context(|| format!("completing upload of {key}"))?;
        Ok(())
    }

    async fn put_if(&self, key: &str, bytes: &[u8], expected: Option<&[u8]>) -> Result<()> {
        use object_store::{ObjectStore as _, PutMode, PutOptions, UpdateVersion};
        let lock = publication_lock(&format!("obj:{}:{key}", self.prefix));
        let _g = lock.lock().await;
        let loc = self.key(key);
        match expected {
            None => self
                .inner
                .put_opts(
                    &loc,
                    object_store::PutPayload::from(bytes.to_vec()),
                    PutOptions {
                        mode: PutMode::Create,
                        ..PutOptions::default()
                    },
                )
                .await
                .map_err(|e| match e {
                    object_store::Error::AlreadyExists { .. } => anyhow::anyhow!(
                        "refusing to publish {key}: it already exists (another publisher holds this dataset)"
                    ),
                    other => anyhow::Error::new(other).context(key.to_string()),
                })
                .map(|_| ()),
            Some(exp) => {
                let got = self.inner.get(&loc).await.map_err(|e| match e {
                    object_store::Error::NotFound { .. } => anyhow::anyhow!(
                        "refusing to publish {key}: expected an existing catalogue and found none"
                    ),
                    other => anyhow::Error::new(other).context(key.to_string()),
                })?;
                let etag = got.meta.e_tag.clone();
                let version = got.meta.version.clone();
                let body = got.bytes().await.context("reading catalogue for CAS")?;
                if body.as_ref() != exp {
                    bail!(
                        "refusing to publish {key}: the remote catalogue changed since this run started"
                    );
                }
                self.inner
                    .put_opts(
                        &loc,
                        object_store::PutPayload::from(bytes.to_vec()),
                        PutOptions {
                            mode: PutMode::Update(UpdateVersion {
                                e_tag: etag,
                                version,
                            }),
                            ..PutOptions::default()
                        },
                    )
                    .await
                    .map_err(|e| match e {
                        object_store::Error::Precondition { .. } => anyhow::anyhow!(
                            "refusing to publish {key}: the remote catalogue changed since this run started"
                        ),
                        other => anyhow::Error::new(other).context(key.to_string()),
                    })
                    .map(|_| ())
            }
        }
    }

    async fn head(&self, key: &str) -> Result<Option<Head>> {
        use object_store::ObjectStore as _;
        match self.inner.head(&self.key(key)).await {
            Ok(m) => Ok(Some(Head {
                size: m.size,
                e_tag: m
                    .e_tag
                    .map(|t| t.trim_start_matches("W/").trim_matches('"').to_string()),
            })),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(anyhow::Error::new(e).context(key.to_string())),
        }
    }
}

fn publication_lock(id: &str) -> std::sync::Arc<tokio::sync::Mutex<()>> {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, OnceLock};
    static LOCKS: OnceLock<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>> = OnceLock::new();
    let mut map = LOCKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("publish locks");
    map.entry(id.to_string())
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone()
}

/// Exclusive `flock` on `path.lock`, so two `nuthatch publish sync` processes cannot
/// both pass the catalogue CAS. Dropping the file releases the lock.
struct FsExclusiveLock {
    _file: std::fs::File,
}

fn fs_exclusive_lock(path: &Path) -> Result<FsExclusiveLock> {
    let mut lock_path = path.as_os_str().to_os_string();
    lock_path.push(".lock");
    let lock_path = PathBuf::from(lock_path);
    if let Some(parent) = lock_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("opening {}", lock_path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
        if rc != 0 {
            bail!("flock {} failed", lock_path.display());
        }
    }
    Ok(FsExclusiveLock { _file: file })
}

fn cas_bytes(current: Option<Vec<u8>>, expected: Option<&[u8]>, key: &str) -> Result<()> {
    match (expected, current.as_deref()) {
        (None, Some(_)) => bail!(
            "refusing to publish {key}: it already exists (another publisher holds this dataset)"
        ),
        (Some(exp), Some(got)) if exp != got => {
            bail!("refusing to publish {key}: the remote catalogue changed since this run started")
        }
        (Some(_), None) => {
            bail!("refusing to publish {key}: expected an existing catalogue and found none")
        }
        _ => Ok(()),
    }
}

#[cfg(feature = "object-store")]
type MemoryStores =
    std::sync::Mutex<HashMap<String, std::sync::Arc<dyn object_store::ObjectStore>>>;

#[cfg(feature = "object-store")]
fn memory_stores() -> &'static MemoryStores {
    static STORES: std::sync::OnceLock<MemoryStores> = std::sync::OnceLock::new();
    STORES.get_or_init(Default::default)
}

#[cfg(feature = "object-store")]
fn memory_store(locator: &str) -> std::sync::Arc<dyn object_store::ObjectStore> {
    memory_stores()
        .lock()
        .expect("memory publish store")
        .entry(locator.to_string())
        .or_insert_with(|| std::sync::Arc::new(object_store::memory::InMemory::new()))
        .clone()
}

/// The ETag S3 gives `src` uploaded as [`ObjMirror::put_file`] uploads it: the MD5 of a file shorter
/// than one part, else the MD5 of the parts' MD5s and the part count.
fn s3_etag(src: &Path) -> Result<String> {
    use md5::{Digest, Md5};
    use std::io::Read as _;
    let mut file =
        std::fs::File::open(src).with_context(|| format!("opening {}", src.display()))?;
    let mut parts = Vec::new();
    let mut total = 0u64;
    loop {
        let mut part = Md5::new();
        let n = std::io::copy(&mut (&mut file).take(PART_BUFFER as u64), &mut part)
            .with_context(|| format!("reading {}", src.display()))?;
        if n == 0 && !parts.is_empty() {
            break;
        }
        total += n;
        parts.push(part.finalize());
        if n < PART_BUFFER as u64 {
            break;
        }
    }
    if total < PART_BUFFER as u64 {
        return Ok(hex::encode(parts[0]));
    }
    let mut whole = Md5::new();
    for part in &parts {
        whole.update(part);
    }
    Ok(format!("{}-{}", hex::encode(whole.finalize()), parts.len()))
}

/// Whether `e_tag` is an MD5-derived ETag at all. An encrypted S3 object's is not, though it looks it.
fn is_md5_etag(e_tag: &str) -> bool {
    let (digest, parts) = match e_tag.split_once('-') {
        Some((d, n)) => (d, Some(n)),
        None => (e_tag, None),
    };
    digest.len() == 32
        && digest.bytes().all(|b| b.is_ascii_hexdigit())
        && parts.is_none_or(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
}

pub(crate) fn open_mirror(target: &str) -> Result<Box<dyn Mirror>> {
    let t = target.trim();
    if t.starts_with("s3://") || t.starts_with("memory://") {
        #[cfg(feature = "object-store")]
        {
            return Ok(Box::new(ObjMirror::from_locator(t)?));
        }
        #[cfg(not(feature = "object-store"))]
        {
            bail!(
                "object-store publish targets need a build with `--features object-store` \
                 (the default binary has this)"
            );
        }
    }
    #[cfg(test)]
    if let Some(name) = t.strip_prefix("test://") {
        return tests::test_mirror(name);
    }
    if t.starts_with("https://") || t.starts_with("http://") {
        return Ok(Box::new(HttpMirror::new(t)?));
    }
    Ok(Box::new(FsMirror {
        root: PathBuf::from(t),
    }))
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

pub(crate) fn parquet_key(table: &str, hash: &str) -> String {
    format!("{table}/{hash}.parquet")
}

fn want_entries(manifest: &Manifest) -> Vec<(String, &Segment)> {
    let mut out = Vec::new();
    for (table, segs) in &manifest.tables {
        for s in segs {
            if !s.provisional {
                out.push((table.clone(), s));
            }
        }
    }
    out.sort_by(|a, b| {
        (a.1.from_block, a.1.to_block, &a.1.hash).cmp(&(b.1.from_block, b.1.to_block, &b.1.hash))
    });
    out
}

async fn remote_present(
    mirror: &dyn Mirror,
    prefix: &str,
    remote: &Manifest,
) -> Result<BTreeSet<(String, String)>> {
    let mut have = BTreeSet::new();
    for (table, segs) in &remote.tables {
        for s in segs.iter().filter(|s| !s.provisional) {
            let key = format!("{prefix}/{}", parquet_key(table, &s.hash));
            if mirror.head(&key).await?.is_some() {
                have.insert((table.clone(), s.hash.clone()));
            }
        }
    }
    Ok(have)
}

fn read_local(dir: &Path) -> Result<(Vec<u8>, Manifest)> {
    let local_path = dir.join(seal::SEGMENTS_DIR).join(MANIFEST_FILE);
    let local_bytes = match std::fs::read(&local_path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            serde_json::to_vec(&Manifest::default())?
        }
        Err(e) => return Err(e).context("reading local catalogue"),
    };
    let local = serde_json::from_slice(&local_bytes).context("corrupt local catalogue")?;
    Ok((local_bytes, local))
}

/// The catalogue the mirror publishes: the local one less its provisional entries, which are never
/// uploaded (#1851). The local bytes verbatim when there are none.
fn published_catalogue(local_bytes: &[u8], local: &Manifest) -> Result<Vec<u8>> {
    if !local.tables.values().flatten().any(|s| s.provisional) {
        return Ok(local_bytes.to_vec());
    }
    let mut published = local.clone();
    for segs in published.tables.values_mut() {
        segs.retain(|s| !s.provisional);
    }
    Ok(serde_json::to_string_pretty(&published)?.into_bytes())
}

/// The seed snapshot's envelope, under the dataset prefix.
pub(crate) const SEED_FILE: &str = "_seed/seed.json";

/// Where a table's provisional tail sits in the seed snapshot. Keyed by table, not by hash: the tail
/// is rewritten at every seal, and a hash-keyed object per rewrite would never stop accumulating.
/// Not named `.parquet`, so no reader's glob over the dataset can count a tail as a segment.
pub(crate) fn seed_tail_key(table: &str) -> String {
    format!("_seed/{table}.tail")
}

/// What `nuthatch seed` needs beyond the mirror itself: each table's provisional tail, which the
/// mirror withholds, and the block the whole of it is complete through. Outside the reader contract;
/// `_seed/` is rewritten in place and only this envelope says which bytes it should hold.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SeedEnvelope {
    pub(crate) layout_version: u32,
    pub(crate) chain_id: u64,
    pub(crate) data_identity: String,
    /// The block the publisher's history begins at, so a `--backfill N` mirror cannot pass for a
    /// full one.
    pub(crate) indexed_from: u64,
    /// No row at or below this block is missing from the catalogue plus `tails`.
    pub(crate) complete_through: u64,
    /// SHA-256 of the `manifest.json` these tails complete. A seeder that reads another retries.
    pub(crate) catalogue_sha256: String,
    /// [`tails_digest`] of `tails`, so a snapshot that lost an entry on the way is refused rather than
    /// seeding a nest that silently lacks that table's tail. Not authentication: an editor can recompute it.
    pub(crate) tails_sha256: String,
    pub(crate) tails: std::collections::BTreeMap<String, Segment>,
}

impl SeedEnvelope {
    /// `None` when nothing is sealed yet.
    fn of(
        local: &Manifest,
        catalogue: &[u8],
        data_identity: &str,
        chain_id: u64,
        indexed_from: u64,
        watermark: u64,
    ) -> Option<Self> {
        // Every row at or below the publisher's seal watermark is in the catalogue, so a quiet nest
        // is complete far past its last row, and a seeder need not scan the gap again.
        let last_row = local.tables.values().flatten().map(|s| s.to_block).max()?;
        let complete_through = last_row.max(watermark);
        let tails: std::collections::BTreeMap<_, _> = local
            .tables
            .iter()
            .filter_map(|(t, segs)| Some((t.clone(), segs.iter().find(|s| s.provisional)?.clone())))
            .collect();
        Some(Self {
            layout_version: 1,
            chain_id,
            data_identity: data_identity.to_string(),
            indexed_from,
            complete_through,
            catalogue_sha256: sha256_hex(catalogue),
            tails_sha256: tails_digest(&tails),
            tails,
        })
    }
}

/// Each tail's table, hash and range, in table order.
pub(crate) fn tails_digest(tails: &std::collections::BTreeMap<String, Segment>) -> String {
    let mut h = Sha256::new();
    for (table, s) in tails {
        h.update(format!(
            "{table}\0{}\0{}\0{}\0{}\n",
            s.hash, s.from_block, s.to_block, s.rows
        ));
    }
    hex::encode(h.finalize())
}

/// The publisher's start block and seal watermark, read from its store. Only a stopped nest's can be
/// opened; a running one reports both through its metrics instead.
fn stopped_publisher(dir: &Path) -> (Option<u64>, u64) {
    let Ok(store) = crate::store::Store::open_existing(&dir.join(crate::config::DB_FILE)) else {
        return (None, 0);
    };
    let start = store
        .get_meta(crate::indexer::START_BLOCK_KEY)
        .ok()
        .flatten()
        .and_then(|v| v.parse().ok());
    (start, store.sealed_through())
}

/// Write the seed snapshot: changed tails first, then the envelope naming them. Returns what it put.
async fn put_seed(mirror: &dyn Mirror, dir: &Path, seed: &SeedEnvelope) -> Result<Vec<String>> {
    let prefix = |k: &str| format!("{}/{k}", seed.data_identity);
    let remote_bytes = mirror.get(&prefix(SEED_FILE)).await?;
    let remote: Option<SeedEnvelope> = remote_bytes
        .as_deref()
        .and_then(|b| serde_json::from_slice(b).ok());
    let mut put = Vec::new();
    for (table, seg) in &seed.tails {
        let held = remote.as_ref().and_then(|r| r.tails.get(table));
        if held.is_some_and(|h| h.hash == seg.hash) {
            continue;
        }
        let src = seal::segment_path(dir, &seg.file, &seg.hash);
        if !src.exists() {
            // A seal folded this tail after the catalogue was read. The seal wakes the next pass.
            tracing::debug!("seed snapshot skipped: {table}'s tail was folded mid-pass");
            return Ok(put);
        }
        let key = seed_tail_key(table);
        mirror.put_file(&prefix(&key), &src).await?;
        put.push(key);
    }
    let bytes = serde_json::to_vec_pretty(seed)?;
    if remote_bytes.as_deref() != Some(bytes.as_slice()) {
        mirror.put(&prefix(SEED_FILE), &bytes).await?;
        put.push(SEED_FILE.into());
    }
    Ok(put)
}

/// The entries of `want` a pass would upload, and how many it would skip.
async fn missing_entries<'a>(
    mirror: &dyn Mirror,
    dataset: &str,
    want: &[(String, &'a Segment)],
    remote_catalogue: Option<&[u8]>,
) -> Result<(Vec<(String, &'a Segment)>, usize)> {
    let remote: Manifest = match remote_catalogue {
        Some(b) => serde_json::from_slice(b).context("corrupt remote catalogue")?,
        None => Manifest::default(),
    };
    let have = remote_present(mirror, dataset, &remote).await?;
    let (held, missing): (Vec<_>, Vec<_>) = want
        .iter()
        .cloned()
        .partition(|(t, s)| have.contains(&(t.clone(), s.hash.clone())));
    Ok((missing, held.len()))
}

pub(crate) fn identity_of(dir: &Path) -> Result<(String, String, String, u64)> {
    let m = crate::blob::build_manifest(dir, None)?;
    let cfg = crate::config::Config::load(dir)?;
    Ok((m.data_identity(), m.nid(), m.blob_hash(), cfg.nest.chain_id))
}

fn rfc3339_utc(secs: u64) -> String {
    let z = secs as i64;
    let days = z.div_euclid(86400);
    let rem = z.rem_euclid(86400) as u64;
    let hour = rem / 3600;
    let min = (rem % 3600) / 60;
    let sec = rem % 60;
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}T{hour:02}:{min:02}:{sec:02}Z")
}

pub(crate) fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097) as u32;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m, d)
}

/// Reconcile `dir`'s sealed catalogue onto `target/<data_identity>/`.
pub async fn sync(dir: &Path, target: &str, dry_run: bool) -> Result<SyncReport> {
    sync_with(dir, target, dry_run, DEFAULT_PARALLELISM, None).await
}

/// [`sync`] with `parallelism` uploads in flight, reporting progress to `metrics` when given.
pub async fn sync_with(
    dir: &Path,
    target: &str,
    dry_run: bool,
    parallelism: usize,
    metrics: Option<&crate::metrics::NestMetrics>,
) -> Result<SyncReport> {
    // Before the catalogue: a seal writes its catalogue entry before its watermark, so a watermark
    // read first can only understate what the catalogue read after it holds.
    let (indexed_from, watermark) = match metrics {
        Some(m) => (m.indexed_from(), m.sealed_through()),
        None => stopped_publisher(dir),
    };
    let (local_bytes, local) = read_local(dir)?;
    let catalogue = published_catalogue(&local_bytes, &local)?;
    let (data_identity, nid, bundle_hash, chain_id) = identity_of(dir)?;
    let mirror = open_mirror(target)?;
    let prefix = |k: &str| format!("{data_identity}/{k}");

    let remote_bytes = mirror.get(&prefix(MANIFEST_FILE)).await?;
    let want = want_entries(&local);
    let (missing, skipped) = missing_entries(
        mirror.as_ref(),
        &data_identity,
        &want,
        remote_bytes.as_deref(),
    )
    .await?;

    if dry_run {
        return Ok(SyncReport {
            dataset: data_identity,
            sealed_through: want.iter().map(|(_, s)| s.to_block).max(),
            uploaded: missing
                .iter()
                .map(|(t, s)| parquet_key(t, &s.hash))
                .collect(),
            uploaded_bytes: 0,
            skipped,
        });
    }

    if let Some(m) = metrics {
        m.set_publish_lag_blocks(
            missing
                .iter()
                .map(|(_, s)| s.to_block - s.from_block + 1)
                .sum(),
        );
        m.set_publish_pending(missing.len() as u64);
    }
    let mirror_ref = mirror.as_ref();
    let attempts: Vec<(String, Result<u64>)> =
        futures::stream::iter(missing.iter().map(|(table, seg)| {
            let src = seal::segment_path(dir, &seg.file, &seg.hash);
            let name = parquet_key(table, &seg.hash);
            let key = prefix(&name);
            async move {
                let upload = async {
                    let len = match std::fs::metadata(&src) {
                        Ok(meta) => meta.len(),
                        Err(_) => bail!(
                            "catalogue names {} but the file is missing at {}",
                            seg.file,
                            src.display()
                        ),
                    };
                    mirror_ref.put_file(&key, &src).await?;
                    if let Some(m) = metrics {
                        m.publish_uploaded(len);
                    }
                    Ok(len)
                };
                (name, upload.await)
            }
        }))
        .buffer_unordered(parallelism.max(1))
        .collect()
        .await;
    let mut parquet = Vec::with_capacity(attempts.len());
    let mut failed = Vec::new();
    for (name, attempt) in attempts {
        match attempt {
            Ok(len) => parquet.push((name, len)),
            Err(e) => failed.push((name, e)),
        }
    }
    if !failed.is_empty() {
        failed.sort_by(|a, b| a.0.cmp(&b.0));
        return Err(UploadFailures(failed).into());
    }
    parquet.sort();
    let uploaded_bytes = parquet.iter().map(|(_, len)| len).sum();
    let mut uploaded: Vec<String> = parquet.into_iter().map(|(name, _)| name).collect();

    let schema_path = dir.join("schema.json");
    let schema_bytes = std::fs::read(&schema_path).ok();
    let schema_sha = schema_bytes.as_ref().map(|b| sha256_hex(b));
    if let Some(bytes) = &schema_bytes {
        let remote_schema = mirror.get(&prefix("schema.json")).await?;
        if remote_schema.as_deref() != Some(bytes.as_slice()) {
            mirror.put(&prefix("schema.json"), bytes).await?;
            uploaded.push("schema.json".into());
        }
    }

    let cat_key = prefix(MANIFEST_FILE);
    if remote_bytes.as_deref() != Some(catalogue.as_slice()) {
        mirror
            .put_if(&cat_key, &catalogue, remote_bytes.as_deref())
            .await?;
        uploaded.push(MANIFEST_FILE.into());
    }

    let sealed_through = want.iter().map(|(_, s)| s.to_block).max();
    let tables: Vec<String> = local.tables.keys().cloned().collect();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let envelope = PublishEnvelope {
        layout_version: 1,
        nuthatch_version: env!("CARGO_PKG_VERSION").to_string(),
        chain_id,
        data_identity: data_identity.clone(),
        nid,
        bundle_hash,
        sealed_through,
        published_at: rfc3339_utc(now),
        catalogue_sha256: sha256_hex(&catalogue),
        schema_sha256: schema_sha,
        tables,
    };
    let env_bytes = serde_json::to_vec_pretty(&envelope)?;
    mirror.put(&prefix("publish.json"), &env_bytes).await?;
    uploaded.push("publish.json".into());

    // Last, so a seed snapshot never names a catalogue the mirror does not hold yet.
    if let Some(indexed_from) = indexed_from {
        let seed = SeedEnvelope::of(
            &local,
            &catalogue,
            &data_identity,
            chain_id,
            indexed_from,
            watermark,
        );
        if let Some(seed) = seed {
            uploaded.extend(put_seed(mirror.as_ref(), dir, &seed).await?);
        }
    }

    Ok(SyncReport {
        dataset: data_identity,
        sealed_through,
        uploaded,
        uploaded_bytes,
        skipped,
    })
}

/// HEAD every published file, comparing its size and, on a filesystem or when `etag_md5` declares the
/// store's ETags content MD5s, its ETag with the local segment. Returns how many were checked. `deep`
/// downloads and re-hashes instead, ignoring `etag_md5`.
pub async fn verify(dir: &Path, target: &str, deep: bool, etag_md5: bool) -> Result<usize> {
    let (data_identity, nid, bundle_hash, chain_id) = identity_of(dir)?;
    let mirror = open_mirror(target)?;
    let prefix = |k: &str| format!("{data_identity}/{k}");
    let remote_bytes = mirror
        .get(&prefix(MANIFEST_FILE))
        .await?
        .context("remote catalogue missing")?;
    let local_bytes = std::fs::read(dir.join(seal::SEGMENTS_DIR).join(MANIFEST_FILE))
        .context("reading local catalogue")?;
    let local: Manifest =
        serde_json::from_slice(&local_bytes).context("corrupt local catalogue")?;
    let catalogue = published_catalogue(&local_bytes, &local)?;
    if remote_bytes != catalogue {
        bail!("remote catalogue is not the local one less its provisional entries");
    }
    let env_bytes = mirror
        .get(&prefix("publish.json"))
        .await?
        .context("remote publish.json missing")?;
    let env: PublishEnvelope =
        serde_json::from_slice(&env_bytes).context("corrupt remote publish.json")?;
    let want = want_entries(&local);
    let sealed_through = want.iter().map(|(_, s)| s.to_block).max();
    let tables: Vec<String> = local.tables.keys().cloned().collect();
    if env.layout_version != 1 {
        bail!(
            "publish.json layout_version is {}, expected 1",
            env.layout_version
        );
    }
    if env.data_identity != data_identity {
        bail!("publish.json data_identity does not match this nest");
    }
    if env.nid != nid {
        bail!("publish.json nid does not match this nest");
    }
    if env.bundle_hash != bundle_hash {
        bail!("publish.json bundle_hash does not match this nest");
    }
    if env.chain_id != chain_id {
        bail!("publish.json chain_id does not match this nest");
    }
    if env.sealed_through != sealed_through {
        bail!("publish.json sealed_through does not match the local catalogue");
    }
    if env.tables != tables {
        bail!("publish.json tables do not match the local catalogue");
    }
    let cat_sha = sha256_hex(&catalogue);
    if env.catalogue_sha256 != cat_sha {
        bail!("publish.json catalogue_sha256 does not match the catalogue");
    }
    if let Ok(local_schema) = std::fs::read(dir.join("schema.json")) {
        let remote_schema = mirror
            .get(&prefix("schema.json"))
            .await?
            .context("remote schema.json missing")?;
        if remote_schema != local_schema {
            bail!("schema.json does not match local");
        }
        let schema_sha = sha256_hex(&local_schema);
        if env.schema_sha256.as_deref() != Some(schema_sha.as_str()) {
            bail!("publish.json schema_sha256 does not match schema.json");
        }
    }
    let mut unverified = Vec::new();
    // A store's ETag semantics are not visible over HEAD, so only the operator can say they are MD5s.
    let compare = etag_md5 || mirror.content_etags();
    for (table, seg) in &want {
        let key = prefix(&parquet_key(table, &seg.hash));
        let head = if deep {
            mirror.head(&key).await?
        } else {
            mirror.head_content(&key).await?
        }
        .with_context(|| format!("missing {key}"))?;
        let src = seal::segment_path(dir, &seg.file, &seg.hash);
        let local_size = std::fs::metadata(&src)
            .with_context(|| format!("stat {}", src.display()))?
            .len();
        if head.size != local_size {
            bail!(
                "{key} is {} bytes remotely, {local_size} locally",
                head.size
            );
        }
        if deep {
            let remote = mirror
                .get(&key)
                .await?
                .with_context(|| format!("downloading {key}"))?;
            let local_file = std::fs::read(&src)?;
            if sha256_hex(&remote) != sha256_hex(&local_file) {
                bail!("{key} hash does not match the local segment");
            }
            continue;
        }
        match head.e_tag.filter(|t| compare && is_md5_etag(t)) {
            Some(remote) => {
                let expected = s3_etag(&src)?;
                if !remote.eq_ignore_ascii_case(&expected) {
                    bail!(
                        "{key} has ETag {remote} but the local segment gives {expected}: either its \
                         bytes differ from the local segment or the store's ETag is not \
                         content-derived (SSE-KMS and some S3-compatible stores), and \
                         `nuthatch publish verify --deep` tells them apart"
                    );
                }
            }
            None => unverified.push(key),
        }
    }
    if let Some(first) = unverified.first() {
        if etag_md5 {
            bail!(
                "{} object(s) match by size but the store gave them no MD5 ETag to compare \
                 (first: {first}); run --deep to compare the bytes",
                unverified.len()
            );
        }
        bail!(
            "{} object(s) match by size but were not content-checked (first: {first}): pass \
             --etag-md5 if this store's ETags are the MD5 of each object, or run --deep to compare \
             the bytes",
            unverified.len()
        );
    }
    Ok(want.len())
}

/// What `nuthatch publish status` reports. Reading it puts nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Status {
    pub target: String,
    pub dataset: String,
    pub local_sealed_through: Option<u64>,
    pub remote_sealed_through: Option<u64>,
    pub pending_segments: usize,
    pub pending_bytes: u64,
    /// A fault the next pass would meet, as far as this read can see one.
    pub last_error: Option<String>,
}

/// Compare `dir`'s catalogue with the mirror at `target` without writing to either.
pub async fn status(dir: &Path, target: &str) -> Result<Status> {
    let (_, local) = read_local(dir)?;
    let (dataset, ..) = identity_of(dir)?;
    let mirror = open_mirror(target)?;
    let remote_catalogue = mirror.get(&format!("{dataset}/{MANIFEST_FILE}")).await?;
    let want = want_entries(&local);
    let (missing, _) = missing_entries(
        mirror.as_ref(),
        &dataset,
        &want,
        remote_catalogue.as_deref(),
    )
    .await?;
    let mut last_error = None;
    let remote_sealed_through = match mirror.get(&format!("{dataset}/publish.json")).await? {
        None => None,
        Some(b) => match serde_json::from_slice::<PublishEnvelope>(&b) {
            Ok(env) => env.sealed_through,
            Err(e) => {
                last_error = Some(format!("corrupt remote publish.json: {e}"));
                None
            }
        },
    };
    let mut pending_bytes = 0;
    for (_, seg) in &missing {
        let src = seal::segment_path(dir, &seg.file, &seg.hash);
        match std::fs::metadata(&src) {
            Ok(m) => pending_bytes += m.len(),
            Err(_) => {
                last_error = Some(format!(
                    "catalogue names {} but the file is missing at {}",
                    seg.file,
                    src.display()
                ))
            }
        }
    }
    Ok(Status {
        target: target.to_string(),
        dataset,
        local_sealed_through: want.iter().map(|(_, s)| s.to_block).max(),
        remote_sealed_through,
        pending_segments: missing.len(),
        pending_bytes,
        last_error,
    })
}

/// `nuthatch publish status`.
pub async fn run_status(dir: &Path, target: &str) -> Result<()> {
    // First, so an operator sees where this nest is going even if the store cannot be reached.
    println!("target          {target}");
    let s = status(dir, target).await?;
    let block = |b: Option<u64>| b.map_or("-".to_string(), |b| b.to_string());
    println!("dataset         {}", s.dataset);
    println!("sealed_through  local {}", block(s.local_sealed_through));
    println!("                remote {}", block(s.remote_sealed_through));
    println!(
        "pending         {} segment(s), {} bytes",
        s.pending_segments, s.pending_bytes
    );
    println!(
        "last error      {}",
        s.last_error.as_deref().unwrap_or("none")
    );
    Ok(())
}

/// `nuthatch publish sync`.
pub async fn run_sync(dir: &Path, target: &str, dry_run: bool) -> Result<()> {
    let report = sync(dir, target, dry_run).await?;
    let verb = if dry_run { "would upload" } else { "uploaded" };
    println!(
        "dataset {} sealed_through {} {verb} {} skipped {}",
        report.dataset,
        report
            .sealed_through
            .map_or("none".to_string(), |b| b.to_string()),
        report.uploaded.len(),
        report.skipped
    );
    Ok(())
}

/// `dir`'s provisional tails, marked final when `apply` (#1851). The mirror withholds a provisional
/// segment until its table's next seal folds it, which on a finished chain never comes; this is the
/// operator saying it will not.
pub fn finalise(dir: &Path, apply: bool) -> Result<Vec<(String, Segment)>> {
    if !apply {
        let local = seal::load_manifest(dir)?;
        return Ok(local
            .tables
            .into_iter()
            .flat_map(|(t, segs)| segs.into_iter().map(move |s| (t.clone(), s)))
            .filter(|(_, s)| s.provisional)
            .collect());
    }
    // Held across the rewrite: a seal racing it would install a catalogue from before the flip.
    let db = dir.join(crate::config::DB_FILE);
    let _writer = if db.exists() {
        Some(crate::store::Store::open_existing(&db).with_context(|| {
            format!(
                "cannot hold {}: stop the nest before finalising its tails",
                db.display()
            )
        })?)
    } else {
        None
    };
    seal::finalise_provisional(dir)
}

/// `nuthatch publish finalise`.
pub fn run_finalise(dir: &Path, yes: bool) -> Result<()> {
    let tails = finalise(dir, yes)?;
    for (table, s) in &tails {
        println!(
            "{table}  blocks {}..{}  {} row(s)  {}",
            s.from_block, s.to_block, s.rows, s.hash
        );
    }
    match (tails.is_empty(), yes) {
        (true, _) => println!("no provisional segments"),
        (false, true) => println!(
            "finalised {} segment(s); the next `publish sync` uploads them",
            tails.len()
        ),
        (false, false) => println!(
            "{} provisional segment(s); run again with --yes to mark them final",
            tails.len()
        ),
    }
    Ok(())
}

/// `nuthatch publish verify`.
pub async fn run_verify(dir: &Path, target: &str, deep: bool, etag_md5: bool) -> Result<()> {
    let objects = verify(dir, target, deep, etag_md5).await?;
    println!("ok, {objects} object(s)");
    Ok(())
}

/// Objects uploaded at once when nobody said otherwise (RFC-0052 §3.4).
pub const DEFAULT_PARALLELISM: usize = 2;
/// The most of one segment an object-store upload holds at once.
pub const PART_BUFFER: usize = 8 * 1024 * 1024;
/// Consecutive failures on one object before the mirror reports itself dead-lettered.
const DEAD_LETTER_AFTER: u32 = 5;
/// Where a failure outside any one object's upload is counted.
const CATALOGUE: &str = "catalogue";

/// The objects a pass could not upload, by key. The pass wrote no catalogue.
#[derive(Debug)]
pub struct UploadFailures(pub Vec<(String, anyhow::Error)>);

impl std::fmt::Display for UploadFailures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let keys: Vec<&str> = self.0.iter().map(|(k, _)| k.as_str()).collect();
        write!(
            f,
            "{} object(s) failed to upload: {}",
            keys.len(),
            keys.join(", ")
        )?;
        if let Some((key, e)) = self.0.first() {
            write!(f, " ({key}: {e:#})")?;
        }
        Ok(())
    }
}

impl std::error::Error for UploadFailures {}

/// Consecutive failures per object. Every missing object is attempted on every pass that reaches
/// the uploads, so an object absent from a pass's failures has been published.
#[derive(Debug, Default)]
struct Failures(HashMap<String, u32>);

impl Failures {
    fn succeeded(&mut self) {
        self.0.clear();
    }

    /// Counts a failed pass and returns the objects it has just dead-lettered.
    fn failed(&mut self, e: &anyhow::Error) -> Vec<String> {
        let failed: Vec<String> = match e.downcast_ref::<UploadFailures>() {
            Some(UploadFailures(objects)) => objects.iter().map(|(k, _)| k.clone()).collect(),
            None => vec![CATALOGUE.to_string()],
        };
        // A pass that failed before uploading anything says nothing about the objects.
        if failed != [CATALOGUE] {
            self.0.retain(|key, _| failed.contains(key));
        }
        let mut dead = Vec::new();
        for key in failed {
            let count = self.0.entry(key.clone()).or_default();
            *count += 1;
            if *count == DEAD_LETTER_AFTER {
                dead.push(key);
            }
        }
        dead
    }

    fn dead(&self) -> bool {
        self.0.values().any(|n| *n >= DEAD_LETTER_AFTER)
    }
}

/// How a running nest mirrors itself (RFC-0052 S2). Operator config, never the nest's identity.
#[derive(Debug, Clone)]
pub struct Settings {
    pub target: String,
    pub interval: std::time::Duration,
    pub parallelism: usize,
}

/// A running mirror. Dropping it stops the reconciler and cancels any upload in flight.
pub struct Publisher {
    stop: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Drop for Publisher {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

/// Start mirroring `dir`, reporting under `nest`. The reconciler owns a thread and a runtime, so a
/// slow or blocking store never holds a worker the ingestion loop needs; a seal only signals it.
pub fn spawn(dir: PathBuf, nest: String, settings: Settings) -> Result<Publisher> {
    // A zero interval makes `reconcile` sync in a loop. The CLI and `[mounts.publish]` refuse it
    // already; this holds for every caller that builds `Settings` itself.
    if settings.interval.is_zero() {
        bail!("publish interval must be longer than zero");
    }
    open_mirror(&settings.target)?;
    let (stop, stopped) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name(format!("publish-{nest}"))
        .spawn(move || {
            match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime.block_on(reconcile(dir, nest, settings, stopped)),
                Err(e) => tracing::error!("publishing {nest} did not start: {e}"),
            }
        })
        .context("starting the publisher thread")?;
    Ok(Publisher { stop: Some(stop) })
}

async fn reconcile(
    dir: PathBuf,
    nest: String,
    settings: Settings,
    mut stopped: tokio::sync::oneshot::Receiver<()>,
) {
    let metrics = crate::metrics::METRICS.nest(&nest);
    metrics.set_publish_enabled(&settings.target);
    let mut changes = seal::manifest_changes(&dir);
    let mut failures = Failures::default();
    loop {
        let pass = sync_with(
            &dir,
            &settings.target,
            false,
            settings.parallelism,
            Some(metrics.as_ref()),
        );
        tokio::select! {
            _ = &mut stopped => return,
            result = pass => match result {
                Ok(report) => {
                    metrics.publish_succeeded(report.sealed_through);
                    failures.succeeded();
                }
                Err(e) => {
                    let dead = failures.failed(&e);
                    metrics.publish_failed(failures.dead());
                    for object in &dead {
                        tracing::error!(
                            "publishing {nest}: {object} has failed {DEAD_LETTER_AFTER} times in a \
                             row; dead-lettered, retrying at a tenth of the rate until it succeeds: {e:#}"
                        );
                    }
                    if dead.is_empty() && !failures.dead() {
                        tracing::warn!("publishing {nest} failed, will retry: {e:#}");
                    }
                }
            },
        }
        let wait = if failures.dead() {
            settings.interval.saturating_mul(10)
        } else {
            settings.interval
        };
        // Level-triggered: a missed wake-up costs one interval, never an object.
        tokio::select! {
            _ = &mut stopped => return,
            _ = tokio::time::sleep(wait) => {}
            changed = changes.changed() => {
                if changed.is_err() {
                    tokio::time::sleep(wait).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{DecodedRow, Value as DecodedValue};
    use crate::seal::{seal_range, test_set_table_floor};
    use serde_json::json;

    #[cfg(feature = "object-store")]
    mod largest {
        use futures::stream::BoxStream;
        use object_store::path::Path;
        use object_store::{
            GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
            PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, UploadPart,
        };
        use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
        use std::sync::Arc;

        /// An in-memory store that remembers the largest payload any single request carried.
        #[derive(Debug, Default)]
        pub struct Largest {
            pub inner: object_store::memory::InMemory,
            pub most: Arc<AtomicUsize>,
        }

        impl std::fmt::Display for Largest {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "Largest")
            }
        }

        #[derive(Debug)]
        struct Parts(Box<dyn MultipartUpload>, Arc<AtomicUsize>);

        #[async_trait::async_trait]
        impl MultipartUpload for Parts {
            fn put_part(&mut self, data: PutPayload) -> UploadPart {
                self.1.fetch_max(data.content_length(), SeqCst);
                self.0.put_part(data)
            }
            async fn complete(&mut self) -> Result<PutResult> {
                self.0.complete().await
            }
            async fn abort(&mut self) -> Result<()> {
                self.0.abort().await
            }
        }

        #[async_trait::async_trait]
        impl ObjectStore for Largest {
            async fn put_opts(
                &self,
                location: &Path,
                payload: PutPayload,
                opts: PutOptions,
            ) -> Result<PutResult> {
                self.most.fetch_max(payload.content_length(), SeqCst);
                self.inner.put_opts(location, payload, opts).await
            }
            async fn put_multipart_opts(
                &self,
                location: &Path,
                opts: PutMultipartOptions,
            ) -> Result<Box<dyn MultipartUpload>> {
                let upload = self.inner.put_multipart_opts(location, opts).await?;
                Ok(Box::new(Parts(upload, self.most.clone())))
            }
            async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
                self.inner.get_opts(location, options).await
            }
            async fn delete(&self, location: &Path) -> Result<()> {
                self.inner.delete(location).await
            }
            fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
                self.inner.list(prefix)
            }
            async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
                self.inner.list_with_delimiter(prefix).await
            }
            async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
                self.inner.copy(from, to).await
            }
            async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
                self.inner.copy_if_not_exists(from, to).await
            }
        }
    }

    #[cfg(feature = "object-store")]
    #[tokio::test]
    async fn an_object_store_upload_never_holds_more_than_one_part() {
        use object_store::ObjectStore as _;
        let store = std::sync::Arc::new(largest::Largest::default());
        let mirror = ObjMirror {
            inner: store.clone(),
            prefix: object_store::path::Path::from("mirror"),
        };
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("segment.parquet");
        let bytes: Vec<u8> = (0..(PART_BUFFER * 5 / 2))
            .map(|i| (i % 251) as u8)
            .collect();
        std::fs::write(&src, &bytes).unwrap();

        mirror.put_file("t/segment.parquet", &src).await.unwrap();

        let got = store
            .inner
            .get(&object_store::path::Path::from("mirror/t/segment.parquet"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert!(
            got.as_ref() == bytes.as_slice(),
            "the object differs from the file"
        );
        let most = store.most.load(std::sync::atomic::Ordering::SeqCst);
        assert!(
            most <= PART_BUFFER,
            "one request carried {most} bytes of a {} byte file",
            bytes.len()
        );
    }
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
    use std::time::Duration;

    type Shared = std::sync::Arc<dyn Mirror>;

    fn test_mirrors() -> &'static std::sync::Mutex<std::collections::HashMap<String, Shared>> {
        static MIRRORS: std::sync::OnceLock<
            std::sync::Mutex<std::collections::HashMap<String, Shared>>,
        > = std::sync::OnceLock::new();
        MIRRORS.get_or_init(Default::default)
    }

    pub(super) fn test_mirror(name: &str) -> Result<Box<dyn Mirror>> {
        let mirror = test_mirrors()
            .lock()
            .unwrap()
            .get(name)
            .cloned()
            .with_context(|| format!("no test mirror {name}"))?;
        Ok(Box::new(Delegate(mirror)))
    }

    fn install(name: &str, mirror: Shared) -> String {
        test_mirrors()
            .lock()
            .unwrap()
            .insert(name.to_string(), mirror);
        format!("test://{name}")
    }

    struct Delegate(Shared);

    #[async_trait]
    impl Mirror for Delegate {
        async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
            self.0.get(key).await
        }
        async fn put(&self, key: &str, bytes: &[u8]) -> Result<()> {
            self.0.put(key, bytes).await
        }
        async fn put_file(&self, key: &str, src: &Path) -> Result<()> {
            self.0.put_file(key, src).await
        }
        async fn put_if(&self, key: &str, bytes: &[u8], expected: Option<&[u8]>) -> Result<()> {
            self.0.put_if(key, bytes, expected).await
        }
        async fn head(&self, key: &str) -> Result<Option<Head>> {
            self.0.head(key).await
        }
        async fn head_content(&self, key: &str) -> Result<Option<Head>> {
            self.0.head_content(key).await
        }
        fn content_etags(&self) -> bool {
            self.0.content_etags()
        }
    }

    /// Blocks its thread outright in `put_file`, as a synchronous call on a slow disk does.
    struct Blocking;

    #[async_trait]
    impl Mirror for Blocking {
        async fn get(&self, _: &str) -> Result<Option<Vec<u8>>> {
            Ok(None)
        }
        async fn put(&self, _: &str, _: &[u8]) -> Result<()> {
            Ok(())
        }
        async fn put_file(&self, _: &str, _: &Path) -> Result<()> {
            std::thread::sleep(Duration::from_secs(5));
            Ok(())
        }
        async fn put_if(&self, _: &str, _: &[u8], _: Option<&[u8]>) -> Result<()> {
            Ok(())
        }
        async fn head(&self, _: &str) -> Result<Option<Head>> {
            Ok(None)
        }
    }

    struct Failing;

    #[async_trait]
    impl Mirror for Failing {
        async fn get(&self, _: &str) -> Result<Option<Vec<u8>>> {
            Ok(None)
        }
        async fn put(&self, _: &str, _: &[u8]) -> Result<()> {
            Ok(())
        }
        async fn put_file(&self, key: &str, _: &Path) -> Result<()> {
            bail!("refusing {key}")
        }
        async fn put_if(&self, _: &str, _: &[u8], _: Option<&[u8]>) -> Result<()> {
            Ok(())
        }
        async fn head(&self, _: &str) -> Result<Option<Head>> {
            Ok(None)
        }
    }

    #[derive(Default)]
    struct Counting {
        in_flight: AtomicUsize,
        most: AtomicUsize,
    }

    #[async_trait]
    impl Mirror for Counting {
        async fn get(&self, _: &str) -> Result<Option<Vec<u8>>> {
            Ok(None)
        }
        async fn put(&self, _: &str, _: &[u8]) -> Result<()> {
            Ok(())
        }
        async fn put_file(&self, _: &str, _: &Path) -> Result<()> {
            let now = self.in_flight.fetch_add(1, SeqCst) + 1;
            self.most.fetch_max(now, SeqCst);
            tokio::time::sleep(Duration::from_millis(50)).await;
            self.in_flight.fetch_sub(1, SeqCst);
            Ok(())
        }
        async fn put_if(&self, _: &str, _: &[u8], _: Option<&[u8]>) -> Result<()> {
            Ok(())
        }
        async fn head(&self, _: &str) -> Result<Option<Head>> {
            Ok(None)
        }
    }

    async fn eventually(what: &str, done: impl Fn() -> bool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while !done() {
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for {what}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    fn settings(target: String, interval: Duration, parallelism: usize) -> Settings {
        Settings {
            target,
            interval,
            parallelism,
        }
    }

    #[test]
    fn a_zero_interval_never_starts_a_publisher() {
        let nest = sealed_nest();
        let mirror = tempfile::tempdir().unwrap();
        let target = mirror.path().to_str().unwrap().to_string();
        let err = spawn(
            nest.path().to_path_buf(),
            "publish-zero".into(),
            settings(target, Duration::ZERO, 2),
        )
        .err()
        .expect("a zero interval started a publisher");
        assert!(err.to_string().contains("longer than zero"), "{err}");
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(
            std::fs::read_dir(mirror.path()).unwrap().count(),
            0,
            "a refused publisher still wrote to its target"
        );
    }

    #[tokio::test]
    async fn a_seal_wakes_the_publisher_long_before_its_interval() {
        let nest = sealed_nest();
        let mirror = tempfile::tempdir().unwrap();
        let target = mirror.path().to_str().unwrap().to_string();
        let _publisher = spawn(
            nest.path().to_path_buf(),
            "publish-wake".into(),
            settings(target, Duration::from_secs(3600), 2),
        )
        .unwrap();
        let (identity, _, _, _) = identity_of(nest.path()).unwrap();
        let remote = mirror.path().join(&identity).join(MANIFEST_FILE);
        let local = nest.path().join(seal::SEGMENTS_DIR).join(MANIFEST_FILE);
        let matches = || std::fs::read(&remote).ok() == std::fs::read(&local).ok();
        eventually("the first pass", matches).await;
        seal_range(nest.path(), &[row(12, 9)], 12, 12)
            .unwrap()
            .expect("sealed");
        eventually("a seal to wake the mirror inside its hour", matches).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_blocking_store_never_holds_the_callers_runtime() {
        let nest = sealed_nest();
        let target = install("blocking", std::sync::Arc::new(Blocking));
        let _publisher = spawn(
            nest.path().to_path_buf(),
            "publish-blocking".into(),
            settings(target, Duration::from_secs(3600), 1),
        )
        .unwrap();
        // Long enough for the publisher to be inside its five-second put.
        std::thread::sleep(Duration::from_millis(500));
        let started = std::time::Instant::now();
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the caller's runtime waited {:?} on the store",
            started.elapsed()
        );
        let started = std::time::Instant::now();
        seal_range(nest.path(), &[row(12, 9)], 12, 12)
            .unwrap()
            .expect("sealed");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "a seal waited {:?} on the store",
            started.elapsed()
        );
    }

    fn refused(keys: &[&str]) -> anyhow::Error {
        UploadFailures(
            keys.iter()
                .map(|k| (k.to_string(), anyhow::anyhow!("refused")))
                .collect(),
        )
        .into()
    }

    #[test]
    fn an_object_failing_beside_others_keeps_its_own_count() {
        let mut failures = Failures::default();
        for pass in 1..DEAD_LETTER_AFTER {
            let keys: &[&str] = if pass % 2 == 0 { &["a", "b"] } else { &["a"] };
            assert!(failures.failed(&refused(keys)).is_empty());
        }
        assert_eq!(
            failures.failed(&refused(&["a", "b"])),
            vec!["a".to_string()]
        );
        assert!(failures.dead());
    }

    #[test]
    fn a_catalogue_failure_neither_resets_nor_counts_an_object() {
        let mut failures = Failures::default();
        for _ in 1..DEAD_LETTER_AFTER {
            failures.failed(&refused(&["a"]));
        }
        failures.failed(&anyhow::anyhow!("bucket unreachable"));
        assert!(!failures.dead());
        assert_eq!(failures.failed(&refused(&["a"])), vec!["a".to_string()]);
    }

    #[test]
    fn an_object_absent_from_a_failed_pass_starts_counting_again() {
        let mut failures = Failures::default();
        for _ in 1..DEAD_LETTER_AFTER {
            failures.failed(&refused(&["a"]));
        }
        // Every missing object is attempted on each pass, so a pass that failed only `b` uploaded `a`.
        failures.failed(&refused(&["b"]));
        assert_eq!(
            failures.0.get("a"),
            None,
            "a kept its count through the pass that uploaded it"
        );
        failures.failed(&refused(&["a"]));
        assert_eq!(failures.0.get("a"), Some(&1));
        assert!(!failures.dead());
    }

    #[derive(Default)]
    struct RefusingFirst {
        refused: std::sync::Mutex<Option<String>>,
        stored: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait]
    impl Mirror for RefusingFirst {
        async fn get(&self, _: &str) -> Result<Option<Vec<u8>>> {
            Ok(None)
        }
        async fn put(&self, _: &str, _: &[u8]) -> Result<()> {
            Ok(())
        }
        async fn put_file(&self, key: &str, _: &Path) -> Result<()> {
            let mut refused = self.refused.lock().unwrap();
            if refused.is_none() {
                *refused = Some(key.to_string());
                bail!("refusing {key}");
            }
            self.stored.lock().unwrap().push(key.to_string());
            Ok(())
        }
        async fn put_if(&self, _: &str, _: &[u8], _: Option<&[u8]>) -> Result<()> {
            Ok(())
        }
        async fn head(&self, _: &str) -> Result<Option<Head>> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn one_refused_object_does_not_stop_the_others_uploading() {
        let nest = sealed_nest();
        seal_range(nest.path(), &[row(12, 9)], 12, 12)
            .unwrap()
            .expect("sealed");
        let mirror = std::sync::Arc::new(RefusingFirst::default());
        let target = install("refusing-first", mirror.clone());
        let e = sync_with(nest.path(), &target, false, 1, None)
            .await
            .unwrap_err();
        let UploadFailures(failed) = e.downcast_ref::<UploadFailures>().expect("per object");
        let refused = mirror.refused.lock().unwrap().clone().unwrap();
        assert_eq!(failed.len(), 1);
        assert!(
            refused.ends_with(&failed[0].0),
            "{refused} vs {}",
            failed[0].0
        );
        assert!(
            !mirror.stored.lock().unwrap().is_empty(),
            "nothing uploaded after the refusal"
        );
    }

    #[tokio::test]
    async fn repeated_failures_on_one_object_dead_letter_the_mirror() {
        let nest = sealed_nest();
        let target = install("failing", std::sync::Arc::new(Failing));
        let _publisher = spawn(
            nest.path().to_path_buf(),
            "publish-failing".into(),
            settings(target, Duration::from_millis(5), 2),
        )
        .unwrap();
        let metrics = crate::metrics::METRICS.nest("publish-failing");
        eventually("the mirror to dead-letter", || {
            metrics.publish_dead_letter()
        })
        .await;
        assert!(
            metrics.publish_errors() >= u64::from(DEAD_LETTER_AFTER),
            "{} errors",
            metrics.publish_errors()
        );
        assert!(crate::metrics::METRICS
            .render()
            .contains("nuthatch_publish_dead_letter{nest=\"publish-failing\"} 1"));
    }

    #[tokio::test]
    async fn uploads_run_in_parallel_up_to_the_bound_and_no_further() {
        let nest = tempfile::tempdir().unwrap();
        write_nest(nest.path());
        test_set_table_floor(nest.path(), 0);
        for block in 10..14 {
            seal_range(nest.path(), &[row(block, block)], block, block)
                .unwrap()
                .expect("sealed");
        }
        let counting = std::sync::Arc::new(Counting::default());
        let target = install("counting", counting.clone());
        let report = sync_with(nest.path(), &target, false, 2, None)
            .await
            .unwrap();
        let parquet = report
            .uploaded
            .iter()
            .filter(|k| k.ends_with(".parquet"))
            .count();
        assert_eq!(parquet, 4);
        assert_eq!(counting.most.load(SeqCst), 2);
    }

    fn row(block: u64, value: u64) -> String {
        DecodedRow {
            table: "usdc__transfer".into(),
            params: vec![(
                "value".into(),
                DecodedValue::Word32(alloy_primitives::U256::from(value).to_be_bytes::<32>()),
            )],
            block_number: block,
            block_hash: "0xbh".into(),
            block_timestamp: 1_700_000_000 + block,
            timestamps: true,
            log_index: 0,
            tx_hash: "0xtx".into(),
            address: "0xaa".into(),
        }
        .to_json()
        .to_string()
    }

    fn write_nest(dir: &Path) {
        std::fs::write(
            dir.join("nuthatch.toml"),
            r#"[nest]
name = "t"
chain = "ethereum"
chain_id = 1
rpc_urls = ["http://127.0.0.1:1"]
schema_version = 1

[[contracts]]
alias = "usdc"
address = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
abi = "abis/usdc.json"
"#,
        )
        .unwrap();
        std::fs::create_dir_all(dir.join("abis")).unwrap();
        std::fs::write(
            dir.join("abis/usdc.json"),
            r#"[{"type":"event","name":"Transfer","anonymous":false,"inputs":[{"name":"from","type":"address","indexed":true},{"name":"to","type":"address","indexed":true},{"name":"value","type":"uint256","indexed":false}]}]"#,
        )
        .unwrap();
        std::fs::write(
            dir.join("schema.json"),
            serde_json::to_string(&json!({"tables":[]})).unwrap(),
        )
        .unwrap();
    }

    fn sealed_nest() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        write_nest(dir.path());
        test_set_table_floor(dir.path(), 0);
        seal_range(dir.path(), &[row(10, 5), row(11, 7)], 10, 11)
            .unwrap()
            .expect("sealed");
        dir
    }

    fn parquet_rows(dir: &Path) -> i64 {
        use parquet::file::reader::{FileReader, SerializedFileReader};
        fn walk(p: &Path, acc: &mut i64) {
            if p.is_dir() {
                for e in std::fs::read_dir(p).unwrap() {
                    walk(&e.unwrap().path(), acc);
                }
            } else if p.extension().and_then(|s| s.to_str()) == Some("parquet") {
                let r = SerializedFileReader::new(std::fs::File::open(p).unwrap()).unwrap();
                *acc += r.metadata().file_metadata().num_rows();
            }
        }
        let mut n = 0;
        walk(dir, &mut n);
        n
    }

    #[tokio::test]
    async fn sync_makes_the_remote_catalogue_byte_identical_and_the_rows_match() {
        let nest = sealed_nest();
        let mirror = tempfile::tempdir().unwrap();
        let report = sync(nest.path(), mirror.path().to_str().unwrap(), false)
            .await
            .unwrap();
        assert!(report.uploaded.iter().any(|k| k.ends_with(".parquet")));
        assert!(report.uploaded.iter().any(|k| k == MANIFEST_FILE));
        assert!(report.uploaded.iter().any(|k| k == "publish.json"));

        let local =
            std::fs::read(nest.path().join(seal::SEGMENTS_DIR).join(MANIFEST_FILE)).unwrap();
        let remote =
            std::fs::read(mirror.path().join(&report.dataset).join(MANIFEST_FILE)).unwrap();
        assert_eq!(remote, local, "remote catalogue must be the local bytes");

        assert_eq!(
            parquet_rows(&nest.path().join(seal::SEGMENTS_DIR)),
            parquet_rows(&mirror.path().join(&report.dataset)),
            "mirrored parquet must carry the same rows as the sealed directory"
        );
    }

    #[tokio::test]
    async fn a_second_sync_reuploads_a_deleted_segment() {
        let nest = sealed_nest();
        let mirror = tempfile::tempdir().unwrap();
        let target = mirror.path().to_str().unwrap();
        let first = sync(nest.path(), target, false).await.unwrap();
        let parquet = first
            .uploaded
            .iter()
            .find(|k| k.ends_with(".parquet"))
            .expect("first sync uploaded parquet")
            .clone();
        std::fs::remove_file(mirror.path().join(&first.dataset).join(&parquet)).unwrap();
        let second = sync(nest.path(), target, false).await.unwrap();
        assert!(
            second.uploaded.iter().any(|k| k == &parquet),
            "missing object must be put again, got {:?}",
            second.uploaded
        );
        verify(nest.path(), target, false, false).await.unwrap();
    }

    #[tokio::test]
    async fn a_second_sync_only_puts_publish_json() {
        let nest = sealed_nest();
        let mirror = tempfile::tempdir().unwrap();
        let target = mirror.path().to_str().unwrap();
        sync(nest.path(), target, false).await.unwrap();
        let second = sync(nest.path(), target, false).await.unwrap();
        assert_eq!(
            second.uploaded,
            vec!["publish.json".to_string()],
            "idempotent: only the envelope, whose published_at moves, is rewritten"
        );
    }

    #[tokio::test]
    async fn provisional_segments_are_not_uploaded() {
        let dir = tempfile::tempdir().unwrap();
        write_nest(dir.path());
        // Default floor is 1000; one row is provisional.
        seal_range(dir.path(), &[row(1, 1)], 1, 1)
            .unwrap()
            .expect("sealed");
        let manifest = seal::load_manifest(dir.path()).unwrap();
        assert!(
            manifest.tables["usdc__transfer"][0].provisional,
            "premise: the sealed row is under the floor"
        );
        let mirror = tempfile::tempdir().unwrap();
        let report = sync(dir.path(), mirror.path().to_str().unwrap(), false)
            .await
            .unwrap();
        assert!(
            !report.uploaded.iter().any(|k| k.ends_with(".parquet")),
            "a provisional segment must not be published"
        );
        verify(dir.path(), mirror.path().to_str().unwrap(), false, false)
            .await
            .unwrap();
    }

    /// One final segment of two rows, then a provisional one-row tail, at a floor of two.
    fn nest_with_a_provisional_tail() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        write_nest(dir.path());
        test_set_table_floor(dir.path(), 2);
        seal_range(dir.path(), &[row(1, 1), row(2, 2)], 1, 2)
            .unwrap()
            .expect("sealed");
        seal_range(dir.path(), &[row(3, 3)], 3, 3)
            .unwrap()
            .expect("sealed");
        let segs = &seal::load_manifest(dir.path()).unwrap().tables["usdc__transfer"];
        assert_eq!(
            segs.iter().map(|s| s.provisional).collect::<Vec<_>>(),
            [false, true],
            "premise: a final segment and a provisional tail"
        );
        dir
    }

    fn published_entries(dataset: &Path) -> Vec<(String, Segment)> {
        let m: Manifest =
            serde_json::from_slice(&std::fs::read(dataset.join(MANIFEST_FILE)).unwrap()).unwrap();
        m.tables
            .into_iter()
            .flat_map(|(t, segs)| segs.into_iter().map(move |s| (t.clone(), s)))
            .collect()
    }

    /// #1851: the mirror withheld the provisional object but its catalogue still named it.
    #[tokio::test]
    async fn every_published_catalogue_entry_is_an_object_the_mirror_holds() {
        let nest = nest_with_a_provisional_tail();
        let mirror = tempfile::tempdir().unwrap();
        let target = mirror.path().to_str().unwrap();
        let report = sync(nest.path(), target, false).await.unwrap();
        let dataset = mirror.path().join(&report.dataset);
        let entries = published_entries(&dataset);
        assert_eq!(entries.len(), 1, "only the final segment: {entries:?}");
        for (table, seg) in &entries {
            assert!(!seg.provisional);
            assert!(
                dataset.join(parquet_key(table, &seg.hash)).is_file(),
                "the published catalogue names {table}/{} and the mirror does not hold it",
                seg.hash
            );
        }
        verify(nest.path(), target, false, false).await.unwrap();
        let second = sync(nest.path(), target, false).await.unwrap();
        assert_eq!(second.uploaded, vec!["publish.json".to_string()]);
    }

    /// #1927: a quiet nest's provisional tail never folds, so its watermark sits far past its last
    /// final segment. That gap is not work the publisher owes: the gauge counts the blocks of the final
    /// segments the mirror lacks, and reads 0 once every one is there.
    #[tokio::test]
    async fn lag_is_zero_when_every_final_segment_is_published() {
        let nest = nest_with_a_provisional_tail();
        let mirror = tempfile::tempdir().unwrap();
        let target = mirror.path().to_str().unwrap();
        let m = crate::metrics::METRICS.nest("publish-lag-quiet");
        m.set_publish_enabled(target);
        m.set_sealed_through(1_000);
        let report = sync_with(nest.path(), target, false, 1, Some(m.as_ref()))
            .await
            .unwrap();
        assert_eq!(
            m.publish_lag_blocks(),
            2,
            "the final segment, until the mirror holds it"
        );
        m.publish_succeeded(report.sealed_through);
        assert_eq!(report.sealed_through, Some(2));
        assert_eq!(m.publish_lag_blocks(), 0, "nothing final is unpublished");
    }

    #[tokio::test]
    async fn after_finalise_every_catalogued_row_is_fetchable_from_the_mirror() {
        let nest = nest_with_a_provisional_tail();
        let mirror = tempfile::tempdir().unwrap();
        let target = mirror.path().to_str().unwrap();
        sync(nest.path(), target, false).await.unwrap();

        let finalised = finalise(nest.path(), true).unwrap();
        assert_eq!(finalised.len(), 1, "one provisional tail: {finalised:?}");
        let report = sync(nest.path(), target, false).await.unwrap();
        let dataset = mirror.path().join(&report.dataset);

        let local = seal::load_manifest(nest.path()).unwrap();
        let local_rows: usize = local.tables.values().flatten().map(|s| s.rows).sum();
        let entries = published_entries(&dataset);
        let published_rows: usize = entries.iter().map(|(_, s)| s.rows).sum();
        assert_eq!((local_rows, published_rows), (3, 3));
        for (table, seg) in &entries {
            assert!(dataset.join(parquet_key(table, &seg.hash)).is_file());
        }
        assert_eq!(
            parquet_rows(&dataset),
            3,
            "every catalogued row is in the mirror"
        );
        assert_eq!(report.sealed_through, Some(3));
        verify(nest.path(), target, false, false).await.unwrap();
    }

    #[test]
    fn finalise_lists_without_yes_and_refuses_while_the_nest_is_held() {
        let nest = nest_with_a_provisional_tail();
        let before =
            std::fs::read(nest.path().join(seal::SEGMENTS_DIR).join(MANIFEST_FILE)).unwrap();
        assert_eq!(finalise(nest.path(), false).unwrap().len(), 1);
        let held = crate::store::Store::open(&nest.path().join(crate::config::DB_FILE)).unwrap();
        let err = finalise(nest.path(), true).unwrap_err();
        assert!(format!("{err:#}").contains("stop the nest"), "{err:#}");
        assert_eq!(
            std::fs::read(nest.path().join(seal::SEGMENTS_DIR).join(MANIFEST_FILE)).unwrap(),
            before,
            "neither the listing nor the refused run may touch the catalogue"
        );
        drop(held);
        assert_eq!(finalise(nest.path(), true).unwrap().len(), 1);
    }

    #[tokio::test]
    async fn verify_accepts_a_faithful_mirror() {
        let nest = sealed_nest();
        let mirror = tempfile::tempdir().unwrap();
        let target = mirror.path().to_str().unwrap();
        sync(nest.path(), target, false).await.unwrap();
        verify(nest.path(), target, true, false).await.unwrap();
    }

    #[tokio::test]
    async fn dry_run_writes_nothing() {
        let nest = sealed_nest();
        let mirror = tempfile::tempdir().unwrap();
        let report = sync(nest.path(), mirror.path().to_str().unwrap(), true)
            .await
            .unwrap();
        assert!(!report.uploaded.is_empty());
        assert!(
            std::fs::read_dir(mirror.path()).unwrap().next().is_none(),
            "dry-run must not create the prefix"
        );
    }

    #[tokio::test]
    async fn verify_fails_if_publish_json_is_gone() {
        let nest = sealed_nest();
        let mirror = tempfile::tempdir().unwrap();
        let target = mirror.path().to_str().unwrap();
        let report = sync(nest.path(), target, false).await.unwrap();
        std::fs::remove_file(mirror.path().join(&report.dataset).join("publish.json")).unwrap();
        let err = verify(nest.path(), target, false, false).await.unwrap_err();
        assert!(
            err.to_string().contains("publish.json"),
            "wanted a publish.json failure, got {err}"
        );
    }

    #[tokio::test]
    async fn verify_fails_if_the_envelope_nid_is_wrong() {
        let nest = sealed_nest();
        let mirror = tempfile::tempdir().unwrap();
        let target = mirror.path().to_str().unwrap();
        let report = sync(nest.path(), target, false).await.unwrap();
        let path = mirror.path().join(&report.dataset).join("publish.json");
        let mut env: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        env["nid"] = serde_json::json!("0".repeat(64));
        std::fs::write(&path, serde_json::to_vec_pretty(&env).unwrap()).unwrap();
        let err = verify(nest.path(), target, false, false).await.unwrap_err();
        assert!(
            err.to_string().contains("nid"),
            "wanted an nid failure, got {err}"
        );
    }

    #[tokio::test]
    async fn verify_fails_if_schema_json_is_gone() {
        let nest = sealed_nest();
        let mirror = tempfile::tempdir().unwrap();
        let target = mirror.path().to_str().unwrap();
        let report = sync(nest.path(), target, false).await.unwrap();
        std::fs::remove_file(mirror.path().join(&report.dataset).join("schema.json")).unwrap();
        let err = verify(nest.path(), target, false, false).await.unwrap_err();
        assert!(
            err.to_string().contains("schema.json"),
            "wanted a schema.json failure, got {err}"
        );
    }

    #[cfg(feature = "object-store")]
    #[tokio::test]
    async fn a_second_memory_sync_only_puts_publish_json() {
        let nest = sealed_nest();
        let target = "memory://s1-idempotent";
        sync(nest.path(), target, false).await.unwrap();
        let second = sync(nest.path(), target, false).await.unwrap();
        assert_eq!(second.uploaded, vec!["publish.json".to_string()]);
        verify(nest.path(), target, true, false).await.unwrap();
    }

    #[cfg(feature = "object-store")]
    #[tokio::test]
    async fn object_store_put_if_refuses_a_replaced_catalogue() {
        let m = ObjMirror::from_locator("memory://s1-put-if").unwrap();
        m.put("manifest.json", b"old").await.unwrap();
        let err = m
            .put_if("manifest.json", b"new", Some(b"stale"))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("changed"),
            "wanted a changed-catalogue refusal, got {err}"
        );
    }

    #[tokio::test]
    async fn concurrent_put_if_does_not_regress_the_catalogue() {
        let dir = tempfile::tempdir().unwrap();
        let m = FsMirror {
            root: dir.path().to_path_buf(),
        };
        m.put("manifest.json", b"old").await.unwrap();
        let a = m.put_if("manifest.json", b"aaa", Some(b"old"));
        let b = m.put_if("manifest.json", b"bbb", Some(b"old"));
        let (ra, rb) = tokio::join!(a, b);
        assert!(
            ra.is_ok() ^ rb.is_ok(),
            "exactly one CAS should win, got {ra:?} {rb:?}"
        );
        let got = m.get("manifest.json").await.unwrap().unwrap();
        assert!(
            got == b"aaa" || got == b"bbb",
            "winner must be one of the two new catalogues, got {got:?}"
        );
    }

    #[cfg(feature = "object-store")]
    mod s3_etags {
        use futures::stream::BoxStream;
        use md5::{Digest, Md5};
        use object_store::path::Path;
        use object_store::{
            GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
            PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, UploadPart,
        };
        use std::collections::HashMap;
        use std::sync::{Arc, Mutex};

        type Tags = Arc<Mutex<HashMap<Path, String>>>;

        /// An in-memory store whose HEAD answers with the ETag S3 computes, quoted as S3 sends it.
        #[derive(Debug, Default)]
        pub struct S3Etags {
            pub inner: object_store::memory::InMemory,
            tags: Tags,
        }

        impl S3Etags {
            /// Answers HEAD for `location` with `tag`, leaving the bytes alone, as SSE-KMS does.
            pub fn set_tag(&self, location: &str, tag: &str) {
                self.tags
                    .lock()
                    .unwrap()
                    .insert(Path::from(location), tag.to_string());
            }
        }

        impl std::fmt::Display for S3Etags {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "S3Etags")
            }
        }

        fn md5_of(payload: &PutPayload) -> [u8; 16] {
            let mut h = Md5::new();
            for chunk in payload {
                h.update(chunk);
            }
            h.finalize().into()
        }

        #[derive(Debug)]
        struct Parts {
            upload: Box<dyn MultipartUpload>,
            location: Path,
            digests: Vec<[u8; 16]>,
            tags: Tags,
        }

        #[async_trait::async_trait]
        impl MultipartUpload for Parts {
            fn put_part(&mut self, data: PutPayload) -> UploadPart {
                self.digests.push(md5_of(&data));
                self.upload.put_part(data)
            }
            async fn complete(&mut self) -> Result<PutResult> {
                let done = self.upload.complete().await?;
                let whole = Md5::digest(self.digests.concat());
                let tag = format!("{}-{}", hex::encode(whole), self.digests.len());
                self.tags.lock().unwrap().insert(self.location.clone(), tag);
                Ok(done)
            }
            async fn abort(&mut self) -> Result<()> {
                self.upload.abort().await
            }
        }

        #[async_trait::async_trait]
        impl ObjectStore for S3Etags {
            async fn put_opts(
                &self,
                location: &Path,
                payload: PutPayload,
                opts: PutOptions,
            ) -> Result<PutResult> {
                let tag = hex::encode(md5_of(&payload));
                let done = self.inner.put_opts(location, payload, opts).await?;
                self.tags.lock().unwrap().insert(location.clone(), tag);
                Ok(done)
            }
            async fn put_multipart_opts(
                &self,
                location: &Path,
                opts: PutMultipartOptions,
            ) -> Result<Box<dyn MultipartUpload>> {
                let upload = self.inner.put_multipart_opts(location, opts).await?;
                Ok(Box::new(Parts {
                    upload,
                    location: location.clone(),
                    digests: Vec::new(),
                    tags: self.tags.clone(),
                }))
            }
            async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
                self.inner.get_opts(location, options).await
            }
            async fn head(&self, location: &Path) -> Result<ObjectMeta> {
                let mut meta = self.inner.head(location).await?;
                meta.e_tag = self
                    .tags
                    .lock()
                    .unwrap()
                    .get(location)
                    .map(|t| format!("\"{t}\""));
                Ok(meta)
            }
            async fn delete(&self, location: &Path) -> Result<()> {
                self.inner.delete(location).await
            }
            fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
                self.inner.list(prefix)
            }
            async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
                self.inner.list_with_delimiter(prefix).await
            }
            async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
                self.inner.copy(from, to).await
            }
            async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
                self.inner.copy_if_not_exists(from, to).await
            }
        }
    }

    fn a_parquet_key(report: &SyncReport) -> String {
        report
            .uploaded
            .iter()
            .find(|k| k.ends_with(".parquet"))
            .expect("the sync uploaded parquet")
            .clone()
    }

    /// Rewrites one object in place with bytes of the same length, as an operator with a
    /// credential and `aws s3 cp` would.
    #[cfg(feature = "object-store")]
    async fn replace_in_store(store: &dyn object_store::ObjectStore, path: &str) {
        let path = object_store::path::Path::from(path);
        let mut bytes = store
            .get(&path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .to_vec();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xff;
        store.put(&path, bytes.into()).await.unwrap();
    }

    #[cfg(feature = "object-store")]
    #[tokio::test]
    async fn shallow_verify_catches_an_object_replaced_with_same_length_bytes() {
        let nest = sealed_nest();
        let store = std::sync::Arc::new(s3_etags::S3Etags::default());
        let target = "memory://s4-replaced";
        memory_stores()
            .lock()
            .unwrap()
            .insert(target.to_string(), store.clone());
        let report = sync(nest.path(), target, false).await.unwrap();
        verify(nest.path(), target, false, true)
            .await
            .expect("premise: with --etag-md5 shallow verify passes on the faithful mirror");

        let parquet = a_parquet_key(&report);
        let key = format!("{}/{parquet}", report.dataset);
        replace_in_store(store.as_ref(), &format!("s4-replaced/{key}")).await;

        let err = verify(nest.path(), target, false, true).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains(&key) && msg.contains("bytes differ"),
            "wanted a content mismatch naming {key}, got {err:#}"
        );
    }

    #[cfg(feature = "object-store")]
    #[tokio::test]
    async fn shallow_verify_fails_closed_on_an_etag_that_is_not_content_derived() {
        let nest = sealed_nest();
        let store = std::sync::Arc::new(s3_etags::S3Etags::default());
        let target = "memory://s4-opaque-etag";
        memory_stores()
            .lock()
            .unwrap()
            .insert(target.to_string(), store.clone());
        let report = sync(nest.path(), target, false).await.unwrap();
        let key = format!("{}/{}", report.dataset, a_parquet_key(&report));
        // MD5-shaped and quoted like S3's, but derived from nothing in the object.
        store.set_tag(
            &format!("s4-opaque-etag/{key}"),
            "5f2b51ca2fdc5baa31ec02e002f69aec",
        );

        let err = verify(nest.path(), target, false, true).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains(&key) && msg.contains("--deep"),
            "wanted a failure naming {key} and pointing at --deep, got {err:#}"
        );
        verify(nest.path(), target, true, true)
            .await
            .expect("the bytes are unchanged, so the failure was the ETag");
    }

    #[cfg(feature = "object-store")]
    #[tokio::test]
    async fn shallow_verify_does_not_pass_a_store_without_content_etags() {
        let nest = sealed_nest();
        let target = "memory://s4-counter-etags";
        let report = sync(nest.path(), target, false).await.unwrap();
        let key = format!("{}/{}", report.dataset, a_parquet_key(&report));
        replace_in_store(
            memory_store(target).as_ref(),
            &format!("s4-counter-etags/{key}"),
        )
        .await;

        let err = verify(nest.path(), target, false, true).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("no MD5 ETag") && msg.contains("--deep"),
            "an InMemory ETag counts writes, so even --etag-md5 cannot compare it: {err:#}"
        );
        let deep = verify(nest.path(), target, true, false).await.unwrap_err();
        assert!(deep.to_string().contains(&key), "{deep:#}");
    }

    #[tokio::test]
    async fn shallow_verify_catches_a_replaced_file_on_a_filesystem_mirror() {
        let nest = sealed_nest();
        let mirror = tempfile::tempdir().unwrap();
        let target = mirror.path().to_str().unwrap();
        let report = sync(nest.path(), target, false).await.unwrap();
        verify(nest.path(), target, false, false).await.unwrap();

        let key = format!("{}/{}", report.dataset, a_parquet_key(&report));
        let path = mirror.path().join(&key);
        let mut bytes = std::fs::read(&path).unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xff;
        std::fs::write(&path, bytes).unwrap();

        let err = verify(nest.path(), target, false, false).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains(&key) && msg.contains("bytes differ"),
            "wanted a content mismatch naming {key}, got {err:#}"
        );
    }

    #[cfg(feature = "object-store")]
    fn s3_etag_target(name: &str) -> (std::sync::Arc<s3_etags::S3Etags>, String) {
        let store = std::sync::Arc::new(s3_etags::S3Etags::default());
        let target = format!("memory://{name}");
        memory_stores()
            .lock()
            .unwrap()
            .insert(target.clone(), store.clone());
        (store, target)
    }

    #[cfg(feature = "object-store")]
    #[tokio::test]
    async fn without_etag_md5_a_faithful_bucket_fails_closed() {
        let nest = sealed_nest();
        let (_store, target) = s3_etag_target("s4-unflagged");
        sync(nest.path(), &target, false).await.unwrap();

        let err = verify(nest.path(), &target, false, false)
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("not content-checked")
                && msg.contains("--etag-md5")
                && msg.contains("--deep"),
            "wanted a fail-closed error naming both remedies, got {err:#}"
        );
    }

    #[cfg(feature = "object-store")]
    #[tokio::test]
    async fn without_etag_md5_a_bucket_still_fails_a_size_mismatch() {
        use object_store::ObjectStore as _;
        let nest = sealed_nest();
        let (store, target) = s3_etag_target("s4-unflagged-size");
        let report = sync(nest.path(), &target, false).await.unwrap();
        let key = format!("{}/{}", report.dataset, a_parquet_key(&report));
        let path = object_store::path::Path::from(format!("s4-unflagged-size/{key}"));
        let mut bytes = store
            .get(&path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .to_vec();
        bytes.push(0);
        store.put(&path, bytes.into()).await.unwrap();

        let err = verify(nest.path(), &target, false, false)
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains(&key) && msg.contains("bytes remotely"),
            "wanted a size mismatch naming {key}, got {err:#}"
        );
    }

    #[cfg(feature = "object-store")]
    #[tokio::test]
    async fn doctor_publish_fails_on_a_replaced_object_and_passes_a_faithful_one() {
        let nest = sealed_nest();
        let (store, target) = s3_etag_target("s4-doctor");
        let report = sync(nest.path(), &target, false).await.unwrap();
        let doctor = |etag_md5: bool| {
            crate::doctor::run(crate::cli::DoctorArgs {
                rpc: Vec::new(),
                dir: nest.path().to_string_lossy().into_owned(),
                address: None,
                json: false,
                catalogue: false,
                publish: Some(target.clone()),
                publish_etag_md5: etag_md5,
            })
        };
        doctor(true)
            .await
            .expect("a faithful mirror passes with --publish-etag-md5");
        assert!(
            doctor(false).await.is_err(),
            "without --publish-etag-md5 a bucket fails closed"
        );

        let key = format!("{}/{}", report.dataset, a_parquet_key(&report));
        replace_in_store(store.as_ref(), &format!("s4-doctor/{key}")).await;
        assert!(
            doctor(true).await.is_err(),
            "a replaced object must fail doctor"
        );
    }

    /// Expected values computed outside Rust: `split -b 8388608`, `md5` each part, `md5` of the
    /// concatenated binary digests.
    #[test]
    fn the_multipart_etag_is_the_md5_of_the_part_md5s() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("f.bin");
        let bytes: Vec<u8> = (0..20_971_520u32).map(|i| (i % 251) as u8).collect();
        std::fs::write(&src, &bytes).unwrap();
        assert_eq!(s3_etag(&src).unwrap(), "0e7f77975c09731444156f23125696f6-3");

        std::fs::write(&src, b"abc").unwrap();
        assert_eq!(s3_etag(&src).unwrap(), "900150983cd24fb0d6963f7d28e17f72");
    }

    #[cfg(feature = "object-store")]
    #[tokio::test]
    async fn put_file_parts_give_the_etag_verify_expects() {
        let store = std::sync::Arc::new(s3_etags::S3Etags::default());
        let mirror = ObjMirror {
            inner: store.clone(),
            prefix: object_store::path::Path::from("mirror"),
        };
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("segment.parquet");
        let bytes: Vec<u8> = (0..(PART_BUFFER * 5 / 2))
            .map(|i| (i % 251) as u8)
            .collect();
        std::fs::write(&src, &bytes).unwrap();
        mirror.put_file("t/segment.parquet", &src).await.unwrap();
        let head = mirror.head("t/segment.parquet").await.unwrap().unwrap();
        assert_eq!(head.e_tag, Some(s3_etag(&src).unwrap()));
    }

    #[test]
    fn only_md5_shaped_etags_are_compared() {
        assert!(is_md5_etag("900150983cd24fb0d6963f7d28e17f72"));
        assert!(is_md5_etag("0e7f77975c09731444156f23125696f6-3"));
        assert!(!is_md5_etag("7"), "InMemory's write counter");
        assert!(!is_md5_etag("0e7f77975c09731444156f23125696f6-"));
        assert!(!is_md5_etag("1a2b-3c4d-5e6f"));
    }

    fn files_under(dir: &Path) -> Vec<(PathBuf, Vec<u8>)> {
        let mut out = Vec::new();
        for e in std::fs::read_dir(dir).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                out.extend(files_under(&p));
            } else {
                out.push((p.clone(), std::fs::read(&p).unwrap()));
            }
        }
        out.sort();
        out
    }

    #[tokio::test]
    async fn status_counts_what_a_partial_mirror_still_needs_and_writes_nothing() {
        let nest = sealed_nest();
        let mirror = tempfile::tempdir().unwrap();
        let target = mirror.path().to_str().unwrap();
        let first = sync(nest.path(), target, false).await.unwrap();
        for (block, value) in [(12, 9), (13, 4)] {
            seal_range(nest.path(), &[row(block, value)], block, block)
                .unwrap()
                .expect("sealed");
        }
        let local = seal::load_manifest(nest.path()).unwrap();
        let unpublished: u64 = local.tables["usdc__transfer"]
            .iter()
            .filter(|s| s.from_block >= 12)
            .map(|s| {
                std::fs::metadata(seal::segment_path(nest.path(), &s.file, &s.hash))
                    .unwrap()
                    .len()
            })
            .sum();
        let before = files_under(mirror.path());

        let s = status(nest.path(), target).await.unwrap();

        assert_eq!(
            files_under(mirror.path()),
            before,
            "status wrote to the mirror"
        );
        assert_eq!(s.target, target);
        assert_eq!(s.dataset, first.dataset);
        assert_eq!(s.local_sealed_through, Some(13));
        assert_eq!(s.remote_sealed_through, Some(11));
        assert_eq!(s.pending_segments, 2);
        assert_eq!(s.pending_bytes, unpublished);
        assert_eq!(s.last_error, None);

        sync(nest.path(), target, false).await.unwrap();
        let caught_up = status(nest.path(), target).await.unwrap();
        assert_eq!(caught_up.remote_sealed_through, Some(13));
        assert_eq!(
            (caught_up.pending_segments, caught_up.pending_bytes),
            (0, 0)
        );
    }

    /// #2202: an HTTP mirror's object is streamed to disk and refused past its cap, never buffered whole.
    #[tokio::test]
    async fn an_http_mirror_object_past_its_cap_is_refused() {
        let app = axum::Router::new().route(
            "/d/seg.parquet",
            axum::routing::get(|| async { vec![7u8; 4096] }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mirror = HttpMirror::new(&base).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("seg.parquet.tmp");

        let err = mirror
            .get_file("d/seg.parquet", &dest, 1024)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("larger than 1024 bytes"), "{err}");
        let got = mirror.get_file("d/seg.parquet", &dest, 4096).await.unwrap();
        assert_eq!(got, Some(sha256_hex(&[7u8; 4096])));
        assert_eq!(std::fs::read(&dest).unwrap(), vec![7u8; 4096]);
        server.abort();
    }
}
