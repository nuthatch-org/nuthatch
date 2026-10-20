//! RFC-0019: the nest registry and distribution client.
//!
//! RFC-0012 made a nest a content-addressed `.bundle`; this gives the blob a home to be *published*
//! to and *pulled* from by name. A [`BundleStore`] holds immutable blobs keyed by their content
//! address, plus a thin index mapping `name@version → hash` (with a movable `latest` pointer per
//! name). The store is **decoupled** from the binary and **never mandatory**: `nest load <dir|file|
//! url>` (RFC-0012) keeps working with no registry in the loop, and a self-built bundle never touches
//! one. nuthatch *pulls*; it never *becomes* the registry.
//!
//! Slice 1 (this file) ships the filesystem-backed store ([`FsStore`] - "a directory is a registry"),
//! the zero-dependency, self-hosted-first default. An S3-compatible object-store backend (slice 2)
//! and private nests + auth (slice 3) land behind the same [`BundleStore`] trait, so callers never
//! change.
//!
//! A pulled blob is verified against its resolved content address by the RFC-0012 install path - so a
//! registry pull is exactly as safe as an `--expect`ed file load. And the store only ever holds
//! authored bundles: **no runtime secret is ever written here** - nest runtime credentials (RFC-0019
//! §4, credential kind *b*) are injected at mount, never bundled.

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use std::path::{Path, PathBuf};

/// The movable per-name pointer to the most recently published version - the one mutable thing in an
/// otherwise append-only store.
pub const LATEST: &str = "latest";

/// A parsed `name[@version]` registry reference. No `@` resolves the movable `latest`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NestRef {
    pub name: String,
    /// `None` = the movable `latest` pointer.
    pub version: Option<String>,
}

impl NestRef {
    /// Parse `name` or `name@version`. Both tokens are validated so a reference can never traverse the
    /// index tree (the resolution-side path-traversal guard, mirroring blob.rs `checked_join`).
    pub fn parse(s: &str) -> Result<NestRef> {
        let (name, version) = match s.split_once('@') {
            Some((n, v)) => (n, Some(v.to_string())),
            None => (s, None),
        };
        validate_token(name).with_context(|| format!("nest name in reference {s:?}"))?;
        if let Some(v) = &version {
            validate_token(v).with_context(|| format!("version in reference {s:?}"))?;
        }
        Ok(NestRef {
            name: name.to_string(),
            version,
        })
    }

    /// The index key to resolve: an explicit version, or `latest`.
    pub fn version_key(&self) -> &str {
        self.version.as_deref().unwrap_or(LATEST)
    }
}

/// A registry name/version token: non-empty, single-segment, no path separators, no `.`/`..`, no `@`,
/// no control chars. This is what keeps `name@version` from escaping the index tree.
fn validate_token(t: &str) -> Result<()> {
    if t.is_empty() {
        bail!("empty");
    }
    if t == "." || t == ".." {
        bail!("{t:?} is not a valid name/version");
    }
    if t.chars()
        .any(|c| matches!(c, '/' | '\\' | '@') || c.is_control())
    {
        bail!("{t:?} contains an illegal character (no `/`, `\\`, `@`, or control chars)");
    }
    Ok(())
}

/// The message shown when a registry *denies* access - a private nest fetched (or published) without a
/// credential, or with one the store rejected (RFC-0019 §3). Kept in one place so every backend says
/// the same helpful thing, and distinct from "not found" so a private nest never masquerades as absent.
fn access_denied(subject: &str) -> String {
    format!(
        "{subject}: access denied by the registry - this nest is private, or your registry credential \
         was rejected. For S3, check your AWS_* env (keys, region, AWS_ENDPOINT); for a filesystem \
         registry, check directory permissions."
    )
}

/// A content-addressed bundle store: immutable blobs keyed by hash, plus a thin name→hash index. Both
/// a local directory ([`FsStore`]) and an S3 bucket ([`ObjectStore`]) implement this; callers never see
/// which. Async because object storage is - the FS impl just does its (fast) sync work in an async fn.
#[async_trait]
pub trait BundleStore: Send + Sync {
    /// Store a blob's bytes under its content address. Idempotent - the same hash is the same blob, so
    /// re-publishing identical bytes is a no-op (dedup is free).
    async fn put_blob(&self, hash: &str, bytes: &[u8]) -> Result<()>;
    /// Fetch a blob's bytes by content address.
    async fn get_blob(&self, hash: &str) -> Result<Vec<u8>>;
    /// Point `name@version` at a hash (the caller advances `latest` separately). The only mutation.
    async fn set_ref(&self, name: &str, version: &str, hash: &str) -> Result<()>;
    /// Resolve `name@version` to a hash. Errors *loudly* when the name/version is unknown.
    async fn get_ref(&self, name: &str, version: &str) -> Result<String>;
    /// Point a nest identity (NID, RFC-0032 §3) at the hash of a bundle computed to it (#1551).
    async fn set_nid_ref(&self, nid: &str, hash: &str) -> Result<()>;
    /// Resolve a NID to a bundle hash. Errors *loudly* when no bundle was published under it.
    async fn get_nid_ref(&self, nid: &str) -> Result<String>;
}

/// A filesystem-backed [`BundleStore`] - "a directory is a registry." The zero-dependency,
/// self-hosted-first default. Layout:
/// ```text
/// <root>/blobs/<hash>.bundle       immutable, content-addressed
/// <root>/index/<name>/<version>    a file whose contents are the hash
/// <root>/index/<name>/latest       the movable pointer
/// <root>/nids/<nid>                 the hash of a bundle whose manifest computes to that NID
/// ```
pub struct FsStore {
    root: PathBuf,
}

impl FsStore {
    pub fn new(root: impl Into<PathBuf>) -> FsStore {
        FsStore { root: root.into() }
    }

    fn blob_path(&self, hash: &str) -> PathBuf {
        self.root.join("blobs").join(format!("{hash}.bundle"))
    }

    fn ref_path(&self, name: &str, version: &str) -> PathBuf {
        self.root.join("index").join(name).join(version)
    }

    fn nid_path(&self, nid: &str) -> PathBuf {
        self.root.join("nids").join(nid)
    }
}

#[async_trait]
impl BundleStore for FsStore {
    async fn put_blob(&self, hash: &str, bytes: &[u8]) -> Result<()> {
        let path = self.blob_path(hash);
        if path.exists() {
            return Ok(()); // content-addressed: identical bytes already present, nothing to do
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        std::fs::write(&path, bytes).with_context(|| format!("writing blob {}", path.display()))?;
        Ok(())
    }

    async fn get_blob(&self, hash: &str) -> Result<Vec<u8>> {
        let path = self.blob_path(hash);
        std::fs::read(&path).map_err(|e| map_io_err(e, &format!("blob {hash}"), &path))
    }

    async fn set_ref(&self, name: &str, version: &str, hash: &str) -> Result<()> {
        let path = self.ref_path(name, version);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        std::fs::write(&path, hash).with_context(|| format!("writing ref {}", path.display()))?;
        Ok(())
    }

    async fn get_ref(&self, name: &str, version: &str) -> Result<String> {
        let path = self.ref_path(name, version);
        let raw = std::fs::read_to_string(&path)
            .map_err(|e| map_io_err(e, &format!("nest '{name}@{version}'"), &path))?;
        Ok(raw.trim().to_string())
    }

    async fn set_nid_ref(&self, nid: &str, hash: &str) -> Result<()> {
        let path = self.nid_path(nid);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        std::fs::write(&path, hash).with_context(|| format!("writing ref {}", path.display()))?;
        Ok(())
    }

    async fn get_nid_ref(&self, nid: &str) -> Result<String> {
        let path = self.nid_path(nid);
        let raw = std::fs::read_to_string(&path)
            .map_err(|e| map_io_err(e, &format!("nid {nid}"), &path))?;
        Ok(raw.trim().to_string())
    }
}

/// Map a filesystem read error into a legible registry error: a missing file is "not found", a
/// permission error is the shared [`access_denied`] message (a private FS registry), anything else
/// keeps its context. Read-side only - where the private-vs-absent distinction matters to a puller.
fn map_io_err(e: std::io::Error, subject: &str, path: &Path) -> anyhow::Error {
    match e.kind() {
        std::io::ErrorKind::NotFound => {
            anyhow::anyhow!("{subject} not found in this registry")
        }
        std::io::ErrorKind::PermissionDenied => anyhow::anyhow!(access_denied(subject)),
        _ => anyhow::Error::new(e).context(format!("reading {}", path.display())),
    }
}

/// Open a store from a `--registry` locator by scheme:
/// - `s3://bucket/prefix` (and `memory://…` for tests) → the object-store backend ([`ObjStore`],
///   built in by default; `--no-default-features` removes it).
/// - `http(s)://…` → a *remote index* registry, not built yet - refused loudly (a raw `http` URL to a
///   `.bundle` is still `nest load <url>`, RFC-0012, not a registry).
/// - anything else (a path, or `file://…`) → the filesystem store ([`FsStore`]).
pub fn open(locator: &str) -> Result<Box<dyn BundleStore>> {
    match locator.split_once("://").map(|(s, _)| s) {
        Some("s3") | Some("memory") => open_object_store(locator),
        Some("http") | Some("https") => bail!(
            "remote HTTP registries aren't built yet (RFC-0019) - use a filesystem path or an \
             object-store URL (s3://bucket/prefix)"
        ),
        _ => Ok(Box::new(FsStore::new(locator))), // a plain path (or file://…)
    }
}

#[cfg(feature = "object-store")]
fn open_object_store(locator: &str) -> Result<Box<dyn BundleStore>> {
    Ok(Box::new(ObjStore::from_locator(locator)?))
}

#[cfg(not(feature = "object-store"))]
fn open_object_store(_locator: &str) -> Result<Box<dyn BundleStore>> {
    bail!(
        "object-store registries (s3://…) need the `object-store` feature. It ships **on** by \
         default, so this binary was built with `--no-default-features` (or an explicit feature list \
         that omits it) - rebuild with the default features, or use a filesystem registry path"
    )
}

/// The result of a publish: where the bundle now lives in the registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishOutcome {
    pub name: String,
    pub version: String,
    pub hash: String,
    pub nid: String,
}

/// Publish a `.bundle` file to a store under `name@version`, advancing `latest`. Returns the blob's
/// content address. `name` defaults to the manifest's nest name; `version` defaults to `h<hash12>` (a
/// content-honest label - semantic versioning meaning is RFC-0020's concern, layered on top).
pub async fn publish(
    store: &dyn BundleStore,
    bundle_file: &Path,
    name: Option<&str>,
    version: Option<&str>,
) -> Result<PublishOutcome> {
    let bytes = std::fs::read(bundle_file)
        .with_context(|| format!("reading bundle {}", bundle_file.display()))?;
    let manifest = crate::blob::bundle_manifest(bundle_file)
        .with_context(|| format!("reading manifest of {}", bundle_file.display()))?;
    let hash = manifest.blob_hash();
    let nid = manifest.nid();

    let name = name.map(str::to_string).unwrap_or(manifest.nest_name);
    if is_nid_shaped(&name) {
        bail!("nest name {name:?} is shaped like a NID, so `nest load` would read it as one - choose another name");
    }
    let version = version
        .map(str::to_string)
        .unwrap_or_else(|| format!("h{}", &hash[..12]));
    validate_token(&name).context("resolved nest name")?;
    validate_token(&version).context("resolved version")?;

    store.put_blob(&hash, &bytes).await?;
    store.set_ref(&name, &version, &hash).await?;
    store.set_ref(&name, LATEST, &hash).await?;
    store.set_nid_ref(&nid, &hash).await?;
    Ok(PublishOutcome {
        name,
        version,
        hash,
        nid,
    })
}

/// Resolve a `name[@version]` reference and fetch its bundle bytes. Returns `(content-address, bytes)`.
pub async fn pull(store: &dyn BundleStore, r: &NestRef) -> Result<(String, Vec<u8>)> {
    let hash = store.get_ref(&r.name, r.version_key()).await?;
    let bytes = store.get_blob(&hash).await?;
    Ok((hash, bytes))
}

/// 64 hex characters: what `nuthatch nest nid` prints, and what `nest load` resolves by NID.
pub fn is_nid_shaped(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Fetch the bundle published under a NID (#1551). The NID index is mutable like any ref, so the
/// bundle's own manifest must compute to the NID asked for, or it is refused.
pub async fn pull_by_nid(store: &dyn BundleStore, nid: &str) -> Result<(String, Vec<u8>)> {
    let nid = nid.to_ascii_lowercase();
    let hash = store.get_nid_ref(&nid).await?;
    let bytes = store.get_blob(&hash).await?;
    let tmp = tempfile::tempdir().context("temp dir for pulled bundle")?;
    let bundle_file = tmp.path().join("pulled.bundle");
    std::fs::write(&bundle_file, &bytes).context("writing pulled bundle")?;
    let actual = crate::blob::bundle_manifest(&bundle_file)
        .with_context(|| format!("reading the manifest of bundle {hash}"))?
        .nid();
    if actual != nid {
        bail!("the registry maps nid {nid} to bundle {hash}, whose nid is {actual} - refusing it");
    }
    Ok((hash, bytes))
}

/// `nuthatch nest publish <bundle> --registry <path> [--as <name[@version]>]`: publish a bundle and
/// print where to find it.
pub async fn publish_cli(registry: &str, bundle_file: &Path, as_ref: Option<&str>) -> Result<()> {
    let store = open(registry)?;
    let (name, version) = match as_ref {
        Some(s) => {
            let r = NestRef::parse(s)?;
            (Some(r.name), r.version)
        }
        None => (None, None),
    };
    let out = publish(
        store.as_ref(),
        bundle_file,
        name.as_deref(),
        version.as_deref(),
    )
    .await?;
    println!("✓ published {}@{}", out.name, out.version);
    println!("  hash:  {}", out.hash);
    println!("  nid:   {}", out.nid);
    println!(
        "  load:  nuthatch nest load {}@{} --registry {registry}",
        out.name, out.version
    );
    Ok(())
}

/// `nuthatch nest load <name[@version]> --registry <path>`: resolve, fetch, verify, and install a nest
/// from a store. The fetched blob is verified against the resolved hash by the RFC-0012 install path.
pub async fn load_from_registry(
    registry: &str,
    reference: &str,
    target: Option<&Path>,
) -> Result<()> {
    load_pinned_from_registry(registry, reference, None, target).await
}

/// [`load_from_registry`], but honouring a **content address the caller already decided on**.
///
/// The distinction matters and is not cosmetic. `pull` asks the registry's *mutable index* which hash
/// `name@version` means today; `blob::load` then checks the bytes against that answer. That is
/// internal consistency, not authenticity - it proves the registry served the blob it named, and a
/// registry that re-points `v1.2.3` at different bytes passes it every time.
///
/// When the control plane has pinned a `bundle_hash`, that pin - not the index - is the authority:
/// we fetch **by hash**, so the index is not consulted at all and cannot be used to substitute a
/// different nest. Retagging a version in the registry then changes nothing on any worker until an
/// operator changes the pin, which is the entire point of recording one.
///
/// With no pin we fall back to resolving the reference, and are exactly as trustworthy as the
/// registry - which is why RFC-0022 §4 asks fleets to pin.
pub async fn load_pinned_from_registry(
    registry: &str,
    reference: &str,
    pinned_hash: Option<&str>,
    target: Option<&Path>,
) -> Result<()> {
    let store = open(registry)?;
    let (hash, bytes) = match pinned_hash {
        Some(h) => {
            let bytes = store.get_blob(h).await.with_context(|| {
                format!("fetching pinned bundle {h} (the pin names a bundle this registry does not have)")
            })?;
            (h.to_string(), bytes)
        }
        None if is_nid_shaped(reference) => pull_by_nid(store.as_ref(), reference).await?,
        None => {
            let r = NestRef::parse(reference)?;
            pull(store.as_ref(), &r).await?
        }
    };
    let tmp = tempfile::tempdir().context("temp dir for pulled bundle")?;
    let bundle_file = tmp.path().join("pulled.bundle");
    std::fs::write(&bundle_file, &bytes).context("writing pulled bundle")?;
    // Reuse RFC-0012's load: it extracts, checks the format, hashes every file, asserts the content
    // address equals `hash`, and reproduces the decode registry. A pull is a hash-checked load.
    crate::blob::load(
        bundle_file.to_str().context("non-utf8 temp path")?,
        target,
        Some(&hash),
    )
    .await
}

/// Fetch the bundle published under `nid` and install it at `target`, verified as `nest load` does
/// (#1543). Nothing is written beside `target` until the registry has served a bundle computing to
/// `nid`, and an install that fails part-way leaves nothing behind.
pub async fn install_by_nid(registry: &str, nid: &str, target: &Path) -> Result<()> {
    let store = open(registry)?;
    let (hash, bytes) = pull_by_nid(store.as_ref(), nid).await?;
    let tmp = tempfile::tempdir().context("temp dir for pulled bundle")?;
    let bundle_file = tmp.path().join("pulled.bundle");
    std::fs::write(&bundle_file, &bytes).context("writing pulled bundle")?;
    let parent = target
        .parent()
        .with_context(|| format!("{} has no parent", target.display()))?;
    std::fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    // Staged beside the target so the final step is a rename on one filesystem. `prune` never
    // lists it, since the name is not a NID, and dropping the guard removes it.
    let staging = tempfile::Builder::new()
        .prefix(".fetch-")
        .tempdir_in(parent)
        .with_context(|| format!("staging a fetch in {}", parent.display()))?;
    let installed = staging.path().join("nest");
    crate::blob::load(
        bundle_file.to_str().context("non-utf8 temp path")?,
        Some(&installed),
        Some(&hash),
    )
    .await?;
    if let Err(e) = std::fs::rename(&installed, target) {
        // Another mount of the same NID installed it first; a NID names one content, so theirs is ours.
        if !target.join(crate::config::CONFIG_FILE).exists() {
            return Err(e)
                .with_context(|| format!("moving the fetched nest to {}", target.display()));
        }
    }
    Ok(())
}

/// The `AWS_*` variables, lower-cased, as `object_store::parse_url_opts` options. Lower case because
/// it matches keys only in lower case; `AWS_*` only because it also takes bare aliases such as
/// `endpoint` and `bucket`, which an unrelated shell variable would otherwise set (#2198). The
/// prefix is checked before lower-casing, so `aws_endpoint` in any other case is not config either.
#[cfg(feature = "object-store")]
pub(crate) fn object_store_options(
    vars: impl Iterator<Item = (String, String)>,
) -> impl Iterator<Item = (String, String)> {
    vars.filter(|(k, _)| k.starts_with("AWS_"))
        .map(|(k, v)| (k.to_ascii_lowercase(), v))
}

/// The S3-compatible registry backend (RFC-0019 slice 2), behind the `object-store` feature so the
/// default embedded binary never pulls the S3 dep tree. Same [`BundleStore`] contract as [`FsStore`];
/// same key layout (`<prefix>/blobs/<hash>.bundle`, `<prefix>/index/<name>/<version>`).
#[cfg(feature = "object-store")]
mod object_store_impl {
    use super::*;
    use object_store::path::Path as ObjPath;
    use object_store::ObjectStore as _;
    use std::sync::Arc;

    /// An S3-compatible [`BundleStore`] over the `object_store` crate. Exercised with an in-memory
    /// store in tests and with MinIO/S3/R2 live (config via `AWS_*` env, incl. `AWS_ENDPOINT`).
    pub struct ObjStore {
        inner: Arc<dyn object_store::ObjectStore>,
        prefix: ObjPath,
    }

    impl ObjStore {
        pub fn from_locator(locator: &str) -> Result<ObjStore> {
            // `memory://<prefix>` is an ephemeral, per-instance store for tests.
            if let Some(rest) = locator.strip_prefix("memory://") {
                return Ok(ObjStore {
                    inner: Arc::new(object_store::memory::InMemory::new()),
                    prefix: ObjPath::from(rest.trim_start_matches('/')),
                });
            }
            let url = url::Url::parse(locator)
                .with_context(|| format!("parsing registry URL {locator:?}"))?;
            // `parse_url_opts` matches option keys **case-sensitively and in lower case**, while every
            // operator on earth exports the AWS convention in upper case (`AWS_ACCESS_KEY_ID`). Passing
            // `std::env::vars()` raw therefore matched nothing: credentials were silently ignored and
            // the S3 client fell through to EC2 instance metadata, spending ~5s on ten retries against
            // 169.254.169.254 before failing with "Host is down" - an error naming an address the
            // operator never configured.
            //
            // Found by the RFC-0019 live verification against Hetzner Object Storage; the backend was
            // effectively unusable as documented, including by our own error text ("check your AWS_*
            // env").
            let opts = crate::distribution::object_store_options(std::env::vars());
            let (store, path) = object_store::parse_url_opts(&url, opts)
                .with_context(|| format!("opening object-store registry {locator:?}"))?;
            Ok(ObjStore {
                inner: Arc::from(store),
                prefix: path,
            })
        }

        fn blob_key(&self, hash: &str) -> ObjPath {
            self.prefix.child("blobs").child(format!("{hash}.bundle"))
        }

        fn ref_key(&self, name: &str, version: &str) -> ObjPath {
            self.prefix.child("index").child(name).child(version)
        }

        fn nid_key(&self, nid: &str) -> ObjPath {
            self.prefix.child("nids").child(nid)
        }
    }

    /// Map an object-store error into a legible registry error: `NotFound` → "not found",
    /// `PermissionDenied`/`Unauthenticated` (a private bucket, missing/rejected creds) → the shared
    /// [`access_denied`] message, anything else keeps its context. So a private nest fails *loudly* and
    /// distinctly, never as a bare "not found".
    fn map_obj_err(e: object_store::Error, subject: &str) -> anyhow::Error {
        match e {
            object_store::Error::NotFound { .. } => {
                anyhow::anyhow!("{subject} not found in this registry")
            }
            object_store::Error::PermissionDenied { .. }
            | object_store::Error::Unauthenticated { .. } => {
                anyhow::anyhow!(access_denied(subject))
            }
            other => anyhow::Error::new(other).context(subject.to_string()),
        }
    }

    #[async_trait]
    impl BundleStore for ObjStore {
        async fn put_blob(&self, hash: &str, bytes: &[u8]) -> Result<()> {
            let key = self.blob_key(hash);
            // Content-addressed → immutable: if it's already there, the bytes are identical.
            if self.inner.head(&key).await.is_ok() {
                return Ok(());
            }
            self.inner
                .put(&key, object_store::PutPayload::from(bytes.to_vec()))
                .await
                .map_err(|e| map_obj_err(e, &format!("blob {hash}")))?;
            Ok(())
        }

        async fn get_blob(&self, hash: &str) -> Result<Vec<u8>> {
            let key = self.blob_key(hash);
            let got = self
                .inner
                .get(&key)
                .await
                .map_err(|e| map_obj_err(e, &format!("blob {hash}")))?;
            Ok(got.bytes().await.context("reading blob bytes")?.to_vec())
        }

        async fn set_ref(&self, name: &str, version: &str, hash: &str) -> Result<()> {
            let key = self.ref_key(name, version);
            self.inner
                .put(
                    &key,
                    object_store::PutPayload::from(hash.as_bytes().to_vec()),
                )
                .await
                .map_err(|e| map_obj_err(e, &format!("nest '{name}@{version}'")))?;
            Ok(())
        }

        async fn get_ref(&self, name: &str, version: &str) -> Result<String> {
            let key = self.ref_key(name, version);
            let got = self
                .inner
                .get(&key)
                .await
                .map_err(|e| map_obj_err(e, &format!("nest '{name}@{version}'")))?;
            let raw = got.bytes().await.context("reading ref")?;
            Ok(String::from_utf8(raw.to_vec())
                .context("ref is not valid utf-8")?
                .trim()
                .to_string())
        }

        async fn set_nid_ref(&self, nid: &str, hash: &str) -> Result<()> {
            self.inner
                .put(
                    &self.nid_key(nid),
                    object_store::PutPayload::from(hash.as_bytes().to_vec()),
                )
                .await
                .map_err(|e| map_obj_err(e, &format!("nid {nid}")))?;
            Ok(())
        }

        async fn get_nid_ref(&self, nid: &str) -> Result<String> {
            let got = self
                .inner
                .get(&self.nid_key(nid))
                .await
                .map_err(|e| map_obj_err(e, &format!("nid {nid}")))?;
            let raw = got.bytes().await.context("reading ref")?;
            Ok(String::from_utf8(raw.to_vec())
                .context("ref is not valid utf-8")?
                .trim()
                .to_string())
        }
    }
}

#[cfg(feature = "object-store")]
use object_store_impl::ObjStore;

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal but valid nest dir (config + one ABI + a `marker` file), bundled to a single-file
    /// `.bundle` at `out`. Mirrors blob.rs's fixture so publish exercises a real, verifiable bundle;
    /// `marker` varies an authored input so two fixtures get distinct content addresses.
    fn write_bundle_fixture(out: &Path, marker: &str) -> String {
        write_bundle_fixture_with_nid(out, marker).0
    }

    /// [`write_bundle_fixture`], plus the NID `nuthatch nest nid` prints for the source directory.
    fn write_bundle_fixture_with_nid(out: &Path, marker: &str) -> (String, String) {
        let nest = tempfile::tempdir().unwrap();
        std::fs::write(
            nest.path().join(crate::config::CONFIG_FILE),
            r#"[nest]
name = "t"
chain = "arbitrum-one"
chain_id = 42161
rpc_urls = ["https://x"]
schema_version = 1

[[contracts]]
alias = "c"
address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
abi = "abis/c.json"
"#,
        )
        .unwrap();
        std::fs::create_dir_all(nest.path().join("abis")).unwrap();
        std::fs::write(
            nest.path().join("abis/c.json"),
            r#"[{"type":"event","name":"Transfer","anonymous":false,"inputs":[{"name":"from","type":"address","indexed":true},{"name":"to","type":"address","indexed":true},{"name":"value","type":"uint256","indexed":false}]}]"#,
        )
        .unwrap();
        std::fs::write(nest.path().join("llms.txt"), marker).unwrap();
        crate::blob::bundle(nest.path(), Some(out), false, false).unwrap();
        (
            crate::blob::bundle_manifest(out).unwrap().blob_hash(),
            crate::blob::nest_nid(nest.path()).unwrap(),
        )
    }

    /// #1551: the NID `nest nid` prints for a directory pulls, from a registry, the bundle made
    /// from that directory.
    #[tokio::test]
    async fn a_nid_pulls_the_bundle_it_was_computed_from() {
        let reg = tempfile::tempdir().unwrap();
        let registry = reg.path().to_str().unwrap();
        let store = open(registry).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("t.bundle");
        let (hash, nid) = write_bundle_fixture_with_nid(&f, "by-nid");
        let out = publish(store.as_ref(), &f, Some("horizon"), Some("1.0.0"))
            .await
            .unwrap();
        assert_eq!(
            out.nid, nid,
            "publish recorded another nid than `nest nid` prints"
        );
        assert_eq!(pull_by_nid(store.as_ref(), &nid).await.unwrap().0, hash);

        let target = tempfile::tempdir().unwrap();
        let installed = target.path().join("nest");
        load_from_registry(registry, &nid, Some(&installed))
            .await
            .unwrap();
        assert_eq!(marker_of(&installed), "by-nid");
    }

    /// #1551: the NID index is a mutable ref. One pointed at a bundle that computes to another NID
    /// is refused, naming both, and nothing is installed.
    #[tokio::test]
    async fn a_nid_ref_pointing_at_another_nest_is_refused() {
        let reg = tempfile::tempdir().unwrap();
        let registry = reg.path().to_str().unwrap();
        let store = open(registry).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = (dir.path().join("a.bundle"), dir.path().join("b.bundle"));
        let (_, nid_a) = write_bundle_fixture_with_nid(&a, "asked-for");
        let (hash_b, nid_b) = write_bundle_fixture_with_nid(&b, "substituted");
        publish(store.as_ref(), &a, Some("a"), None).await.unwrap();
        publish(store.as_ref(), &b, Some("b"), None).await.unwrap();
        store.set_nid_ref(&nid_a, &hash_b).await.unwrap();

        let target = tempfile::tempdir().unwrap();
        let installed = target.path().join("nest");
        let err = load_from_registry(registry, &nid_a, Some(&installed))
            .await
            .expect_err("a substituted bundle must be refused");
        let err = format!("{err:#}");
        assert!(err.contains(&nid_a) && err.contains(&nid_b), "{err}");
        assert!(!installed.join(crate::config::CONFIG_FILE).exists());
    }

    /// #1551: a NID nothing was published under fails loudly, and a nest name shaped like a NID is
    /// refused at publish, since `nest load` would read it as a NID.
    #[tokio::test]
    async fn an_unknown_nid_fails_and_a_nid_shaped_name_cannot_be_published() {
        let reg = tempfile::tempdir().unwrap();
        let store = open(reg.path().to_str().unwrap()).unwrap();
        let err = pull_by_nid(store.as_ref(), &"b".repeat(64))
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("not found"), "{err:#}");

        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("t.bundle");
        write_bundle_fixture(&f, "x");
        let err = publish(store.as_ref(), &f, Some(&"c".repeat(64)), None)
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("shaped like a NID"), "{err:#}");
    }

    #[test]
    fn parses_refs_and_rejects_traversal() {
        assert_eq!(
            NestRef::parse("horizon").unwrap(),
            NestRef {
                name: "horizon".into(),
                version: None
            }
        );
        let r = NestRef::parse("horizon@1.2.0").unwrap();
        assert_eq!(r.name, "horizon");
        assert_eq!(r.version.as_deref(), Some("1.2.0"));
        assert_eq!(r.version_key(), "1.2.0");
        assert_eq!(NestRef::parse("horizon").unwrap().version_key(), "latest");
        // Path-traversal / illegal tokens are refused on both sides of the `@`.
        for bad in ["../evil", "a/b", "", "..", "a@b@c", "x/../y@1"] {
            assert!(NestRef::parse(bad).is_err(), "{bad:?} should be rejected");
        }
    }

    #[tokio::test]
    async fn fs_store_round_trips_blobs_and_refs() {
        let root = tempfile::tempdir().unwrap();
        let store = FsStore::new(root.path());
        store.put_blob("abc123", b"hello").await.unwrap();
        assert_eq!(store.get_blob("abc123").await.unwrap(), b"hello");
        store.set_ref("n", "1.0.0", "abc123").await.unwrap();
        assert_eq!(store.get_ref("n", "1.0.0").await.unwrap(), "abc123");
        // Unknown ref and unknown blob both fail loudly.
        assert!(store.get_ref("n", "9.9.9").await.is_err());
        assert!(store.get_blob("deadbeef").await.is_err());
    }

    #[test]
    fn access_denied_message_is_helpful_and_distinct() {
        let m = access_denied("nest 'secret@1.0.0'");
        assert!(m.contains("private"), "got: {m}");
        assert!(m.contains("AWS_"), "got: {m}");
        assert!(m.contains("access denied"), "got: {m}");
        // Distinct from "not found" so a private nest never reads as merely absent.
        assert!(!m.contains("not found"), "got: {m}");
    }

    /// A filesystem registry whose ref file we can't read (a private FS registry) surfaces the clear
    /// "access denied / private" message, not a bare "not found". Root bypasses file perms, so skip
    /// there; perms are restored so the tempdir cleans up regardless.
    #[cfg(unix)]
    #[tokio::test]
    async fn fs_store_maps_permission_denied_to_a_clear_message() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let store = FsStore::new(root.path());
        store.set_ref("secret", "1.0.0", "abc123").await.unwrap();
        let path = root.path().join("index/secret/1.0.0");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000)).unwrap();
        if std::fs::read(&path).is_ok() {
            // Running as root - file perms don't apply; the test can't be meaningful.
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).ok();
            return;
        }
        let err = store
            .get_ref("secret", "1.0.0")
            .await
            .unwrap_err()
            .to_string();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).ok();
        assert!(err.contains("access denied"), "got: {err}");
        assert!(
            !err.contains("not found"),
            "a denied read must not read as absent; got: {err}"
        );
    }

    /// The marker `write_bundle_fixture` baked into an installed nest, so a test can say *which*
    /// bundle it got rather than merely that it got one.
    fn marker_of(installed: &std::path::Path) -> String {
        std::fs::read_to_string(installed.join("llms.txt")).expect("installed nest has llms.txt")
    }

    #[tokio::test]
    async fn publish_then_pull_and_load_round_trips() {
        let reg = tempfile::tempdir().unwrap();
        let registry = reg.path().to_str().unwrap();
        let bundle = tempfile::tempdir().unwrap();
        let bundle_file = bundle.path().join("t.bundle");
        let hash = write_bundle_fixture(&bundle_file, "one");

        let store = open(registry).unwrap();
        let out = publish(store.as_ref(), &bundle_file, Some("horizon"), Some("1.0.0"))
            .await
            .unwrap();
        assert_eq!(out.hash, hash);

        // Pull by explicit version and by latest → same hash, same bytes as the original bundle.
        let by_version = pull(store.as_ref(), &NestRef::parse("horizon@1.0.0").unwrap())
            .await
            .unwrap();
        let by_latest = pull(store.as_ref(), &NestRef::parse("horizon").unwrap())
            .await
            .unwrap();
        assert_eq!(by_version.0, hash);
        assert_eq!(by_latest.0, hash);
        assert_eq!(by_version.1, std::fs::read(&bundle_file).unwrap());

        // A registry load installs a runnable, hash-verified nest.
        let target = tempfile::tempdir().unwrap();
        let installed = target.path().join("nest");
        load_from_registry(registry, "horizon@1.0.0", Some(&installed))
            .await
            .unwrap();
        assert!(installed.join(crate::config::CONFIG_FILE).exists());
        assert!(installed.join("abis/c.json").exists());
    }

    /// The property that makes a pin worth recording: **a re-pointed index cannot change what a
    /// pinned worker runs.**
    ///
    /// `pull` asks the registry's mutable index what `name@version` means *today*, and `blob::load`
    /// then checks the bytes against that answer - internal consistency, not authenticity. A registry
    /// that moves `1.0.0` onto different bytes passes that check every time. So this re-points the
    /// version, exactly as a compromised or merely careless registry would, and asserts the two
    /// callers diverge: the unpinned one follows the move, the pinned one does not.
    #[tokio::test]
    async fn a_pinned_hash_beats_a_registry_that_repoints_the_version() {
        let reg = tempfile::tempdir().unwrap();
        let registry = reg.path().to_str().unwrap();
        let store = open(registry).unwrap();

        let dir = tempfile::tempdir().unwrap();
        let honest_file = dir.path().join("honest.bundle");
        let swapped_file = dir.path().join("swapped.bundle");
        let honest = write_bundle_fixture(&honest_file, "the-nest-the-operator-pinned");
        let swapped = write_bundle_fixture(&swapped_file, "something-else-entirely");
        assert_ne!(honest, swapped);

        publish(store.as_ref(), &honest_file, Some("horizon"), Some("1.0.0"))
            .await
            .unwrap();
        // Both blobs exist in the registry; only the *pointer* is moved. That is the whole attack:
        // no blob is tampered with, so every content-address check downstream still passes.
        publish(
            store.as_ref(),
            &swapped_file,
            Some("horizon"),
            Some("9.9.9"),
        )
        .await
        .unwrap();
        store.set_ref("horizon", "1.0.0", &swapped).await.unwrap();

        // Unpinned: trusts the index, and gets whatever it now says.
        let unpinned_root = tempfile::tempdir().unwrap();
        let unpinned = unpinned_root.path().join("nest");
        load_pinned_from_registry(registry, "horizon@1.0.0", None, Some(&unpinned))
            .await
            .unwrap();
        assert_eq!(
            marker_of(&unpinned),
            "something-else-entirely",
            "without a pin the index is the authority - if this ever stops following the move, the \
             fallback path has quietly changed meaning"
        );

        // Pinned: fetches by content address, so the moved pointer is never consulted.
        let pinned_root = tempfile::tempdir().unwrap();
        let pinned = pinned_root.path().join("nest");
        load_pinned_from_registry(registry, "horizon@1.0.0", Some(&honest), Some(&pinned))
            .await
            .unwrap();
        assert_eq!(
            marker_of(&pinned),
            "the-nest-the-operator-pinned",
            "a pinned worker ran what the registry pointed at, not what the operator pinned"
        );
    }

    /// A pin naming a bundle the registry does not hold must fail *closed* - never fall back to
    /// resolving the reference, which would silently reinstate the hole the pin exists to close.
    #[tokio::test]
    async fn a_pin_the_registry_cannot_serve_fails_rather_than_falling_back() {
        let reg = tempfile::tempdir().unwrap();
        let registry = reg.path().to_str().unwrap();
        let store = open(registry).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("t.bundle");
        write_bundle_fixture(&f, "present");
        publish(store.as_ref(), &f, Some("horizon"), Some("1.0.0"))
            .await
            .unwrap();

        let target_root = tempfile::tempdir().unwrap();
        let target = target_root.path().join("nest");
        let err = load_pinned_from_registry(
            registry,
            "horizon@1.0.0",
            Some(&"a".repeat(64)),
            Some(&target),
        )
        .await
        .expect_err("a pin the registry cannot serve must not resolve the reference instead");
        assert!(
            format!("{err:#}").contains("pinned"),
            "the error should say the pin is the thing that failed; got: {err:#}"
        );
        assert!(
            !target.join(crate::config::CONFIG_FILE).exists(),
            "nothing may be installed when the pinned bundle is unavailable"
        );
    }

    #[tokio::test]
    async fn republish_is_idempotent_and_versions_coexist_latest_moves() {
        let reg = tempfile::tempdir().unwrap();
        let registry = reg.path().to_str().unwrap();
        let store = open(registry).unwrap();

        let b1 = tempfile::tempdir().unwrap();
        let f1 = b1.path().join("a.bundle");
        let h1 = write_bundle_fixture(&f1, "v1");

        // Publish v1 twice → idempotent (same blob, no error), latest = h1.
        publish(store.as_ref(), &f1, Some("n"), Some("1.0.0"))
            .await
            .unwrap();
        publish(store.as_ref(), &f1, Some("n"), Some("1.0.0"))
            .await
            .unwrap();
        assert_eq!(store.get_ref("n", "latest").await.unwrap(), h1);

        // A *different* bundle (distinct inputs → distinct content address) published as v2 → both
        // versions resolve; latest moves to v2.
        let b2 = tempfile::tempdir().unwrap();
        let f2 = b2.path().join("b.bundle");
        let h2 = write_bundle_fixture(&f2, "v2");
        assert_ne!(
            h1, h2,
            "distinct inputs must yield distinct content addresses"
        );
        publish(store.as_ref(), &f2, Some("n"), Some("2.0.0"))
            .await
            .unwrap();
        assert_eq!(store.get_ref("n", "1.0.0").await.unwrap(), h1);
        assert_eq!(store.get_ref("n", "2.0.0").await.unwrap(), h2);
        assert_eq!(store.get_ref("n", "latest").await.unwrap(), h2);
    }

    #[tokio::test]
    async fn unknown_ref_fails_loudly() {
        let reg = tempfile::tempdir().unwrap();
        let registry = reg.path().to_str().unwrap();
        let err = load_from_registry(registry, "nope@1.0.0", None)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("not found"), "got: {err}");
    }

    #[test]
    fn http_registry_locator_is_refused_for_now() {
        // `open` returns a boxed trait object (not `Debug`), so match rather than `unwrap_err`.
        let err = match open("https://registry.example") {
            Err(e) => e.to_string(),
            Ok(_) => panic!("an http registry locator should be refused for now"),
        };
        assert!(err.contains("aren't built yet"), "got: {err}");
    }

    /// The object-store backend, exercised against an in-memory store (no infra) - same round trip as
    /// the FS path. InMemory is per-instance, so `store` is reused for publish + pull (each `open()`
    /// would mint a fresh, empty store). Live MinIO/S3 is a VPS integration concern.
    #[cfg(feature = "object-store")]
    #[tokio::test]
    async fn object_store_memory_round_trips() {
        let bundle = tempfile::tempdir().unwrap();
        let bundle_file = bundle.path().join("t.bundle");
        let hash = write_bundle_fixture(&bundle_file, "obj");

        let store = open("memory://reg").unwrap();
        let out = publish(store.as_ref(), &bundle_file, Some("horizon"), Some("1.0.0"))
            .await
            .unwrap();
        assert_eq!(out.hash, hash);

        let (h, bytes) = pull(store.as_ref(), &NestRef::parse("horizon@1.0.0").unwrap())
            .await
            .unwrap();
        assert_eq!(h, hash);
        assert_eq!(bytes, std::fs::read(&bundle_file).unwrap());

        // Idempotent re-publish; latest resolves.
        publish(store.as_ref(), &bundle_file, Some("horizon"), Some("1.0.0"))
            .await
            .unwrap();
        assert_eq!(store.get_ref("horizon", "latest").await.unwrap(), hash);
        // Unknown ref fails loudly.
        assert!(store.get_ref("horizon", "9.9.9").await.is_err());
        // #1551: the NID index lives in this store too.
        let (h, _) = pull_by_nid(store.as_ref(), &out.nid).await.unwrap();
        assert_eq!(h, hash);
        assert!(store.get_nid_ref(&"d".repeat(64)).await.is_err());
    }

    /// The AWS convention is **upper case** (`AWS_ACCESS_KEY_ID`), but `object_store::parse_url_opts`
    /// matches option keys in lower case. Handing it `std::env::vars()` raw therefore matched nothing:
    /// credentials were silently dropped, the client fell through to EC2 instance metadata, and the
    /// operator got a five-second stall and "Host is down (os error 64)" naming 169.254.169.254 - an
    /// address they never configured.
    ///
    /// Found by the RFC-0019 live verification against Hetzner Object Storage. Every operator sets the
    /// upper-case form; our own error text tells them to ("check your AWS_* env"). This pins the
    /// lower-casing so the backend cannot silently become unusable again.
    #[cfg(feature = "object-store")]
    #[test]
    fn upper_case_aws_env_is_recognised_as_object_store_config() {
        use object_store::aws::AmazonS3ConfigKey;
        use std::str::FromStr;

        // What we now pass to `parse_url_opts`, in miniature.
        let lower = |k: &str| k.to_ascii_lowercase();

        for key in [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_ENDPOINT",
            "AWS_REGION",
        ] {
            assert!(
                AmazonS3ConfigKey::from_str(&lower(key)).is_ok(),
                "{key} must be recognised once lower-cased - it is what every operator exports"
            );
            // And the raw upper-case form is *not* recognised, which is the bug this guards.
            assert!(
                AmazonS3ConfigKey::from_str(key).is_err(),
                "{key} parsing upper-case would mean this guard is no longer needed - re-check the fix"
            );
        }

        // Unrelated environment (PATH, HOME, …) must stay ignored rather than becoming config.
        assert!(AmazonS3ConfigKey::from_str(&lower("PATH")).is_err());
    }

    /// The S3 builder takes bare aliases (`endpoint`, `token`, `bucket`, `region`), so an unrelated
    /// `ENDPOINT` in the shell would redirect every upload. Only `AWS_*` reaches it (#2198).
    #[cfg(feature = "object-store")]
    #[test]
    fn only_aws_variables_configure_the_object_store() {
        use object_store::aws::AmazonS3ConfigKey;
        use std::str::FromStr;
        assert!(
            AmazonS3ConfigKey::from_str("endpoint").is_ok(),
            "the alias this guards"
        );
        let env = [
            ("ENDPOINT", "http://other"),
            ("TOKEN", "t"),
            ("BUCKET", "elsewhere"),
            ("REGION", "r"),
            ("PATH", "/bin"),
            ("AWS_ENDPOINT", "http://minio:9000"),
            ("AWS_ACCESS_KEY_ID", "id"),
            ("aws_endpoint", "http://lower"),
            ("Aws_Endpoint", "http://mixed"),
        ]
        .map(|(k, v)| (k.to_string(), v.to_string()));
        let opts: Vec<(String, String)> = object_store_options(env.into_iter()).collect();
        assert_eq!(
            opts,
            [
                ("aws_endpoint".to_string(), "http://minio:9000".to_string()),
                ("aws_access_key_id".to_string(), "id".to_string()),
            ]
        );
    }

    #[cfg(not(feature = "object-store"))]
    #[test]
    fn s3_locator_needs_the_feature() {
        let err = match open("s3://bucket/prefix") {
            Err(e) => e.to_string(),
            Ok(_) => panic!("s3:// should require the object-store feature"),
        };
        assert!(err.contains("object-store"), "got: {err}");
    }
}
