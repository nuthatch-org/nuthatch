//! Content-addressed nest packaging (RFC-0012 §4): turn a nest directory into a **bundle** - its
//! *authored inputs* canonicalised and pinned by a Merkle-root hash, so a nest becomes a portable
//! deploy unit. `nest bundle` writes a single `.bundle` file (`bundle` here); `nest load` verifies +
//! installs one (`load`), resolving a `.bundle` file, an `http(s)` URL, or an unpacked bundle dir.
//!
//! The blob pins **inputs** (`nuthatch.toml`, ABIs, views, labels, skills, `schema.json`, `llms.txt`),
//! never build artifacts (the generated decode registry) or sealed data (`segments/`, `nuthatch.redb`).
//! Instead the manifest records the *expected* `registry_hash`; a `mount` regenerates the registry from
//! the packed inputs and asserts it matches - extending determinism from the data path (RFC-0009's
//! content-addressed segments) to the *authoring* path: same inputs + same generator → same blob →
//! same decode, verifiably. The blob hash is `sha256` over the **canonical** manifest (fixed field
//! order, files sorted by path, compact encoding), reusing the seal-manifest discipline, not new crypto.

use crate::config::{Config, DB_FILE};
use anyhow::{bail, Context, Result};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

#[cfg(test)]
use nuthatch_identity::{affects_data, without_address_history};
pub use nuthatch_identity::{FileEntry, Manifest, BLOB_FORMAT_VERSION};

/// Files/dirs never included in a blob: the hot store and sealed data are *derived*, not authored, and
/// including them would make the hash depend on runtime state instead of inputs. Matched by exact name
/// at any depth.
#[cfg(not(feature = "folds"))]
const EXCLUDE: &[&str] = &[DB_FILE, "segments", ".git", ".DS_Store"];
/// RFC-0059: fold checkpoints are derived state. Excluded only where folds exist, because a name
/// matched at any depth would otherwise move the NID of a default-build nest that owns such a directory.
#[cfg(feature = "folds")]
const EXCLUDE: &[&str] = &[DB_FILE, "segments", "checkpoints", ".git", ".DS_Store"];

/// The **derived state** a dataset accumulates by indexing: the hot store and the sealed segments.
///
/// Named separately from [`EXCLUDE`] because it is read in the other direction. `EXCLUDE` answers
/// "what is not an input"; this answers "what *is* the data" - the set early cutoff copies when one
/// identity adopts another's dataset ([`crate::runtime::adopt_dataset`]).
///
/// It is a **subset of [`EXCLUDE`]**, and that is what makes adoption safe: copying only these can
/// never change what the destination's inputs hash to, so an adopted dataset still is the identity its
/// mount record claims. `derived_state_is_excluded_from_the_identity` holds the two together.
pub const DERIVED_STATE: &[&str] = &[DB_FILE, "segments"];

/// Recursively collect the authored input files under `root`, relative-pathed and sorted, skipping the
/// [`EXCLUDE`] set (and `skip`, e.g. the output dir when it sits inside the nest). Deterministic order.
pub(crate) fn collect_files(root: &Path, skip: Option<&Path>) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let mut entries: Vec<_> = std::fs::read_dir(&dir)
            .with_context(|| format!("reading {}", dir.display()))?
            .collect::<std::io::Result<_>>()?;
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let path = entry.path();
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if EXCLUDE.iter().any(|x| *x == name) {
                continue;
            }
            // RFC-0062's copies are derived state written beside `segments/`. Only that directory, at
            // the root, so a deeper authored one keeps its place in the identity.
            if dir == root && name == crate::maintained::COPIES_DIR && entry.file_type()?.is_dir() {
                continue;
            }
            if let Some(skip) = skip {
                if path == skip {
                    continue;
                }
            }
            let ft = entry.file_type()?;
            if ft.is_dir() {
                stack.push(path);
            } else if ft.is_file() {
                out.push(path);
            }
            // Symlinks are deliberately ignored - a blob must be self-contained.
        }
    }
    out.sort();
    Ok(out)
}

/// Build the manifest for the nest at `dir` without writing anything - hashes every authored input and
/// records the regenerated `registry_hash`. Shared by `pack` and (later) `mount`'s verify.
pub fn build_manifest(dir: &Path, skip_out: Option<&Path>) -> Result<Manifest> {
    let config = Config::load(dir).context("loading nest config for pack")?;
    // Regenerate the decode registry from the *inputs* (toml + ABIs) so the manifest pins what a mount
    // must reproduce - never a stored artifact.
    let registry = crate::registry::from_nest(dir, &config).context("building decode registry")?;
    let registry_hash = hex::encode(registry.hash());

    let files = collect_files(dir, skip_out)?
        .into_iter()
        .map(|path| {
            let rel = path
                .strip_prefix(dir)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/"); // stable path separator across platforms
            let bytes = std::fs::read(&path).with_context(|| format!("reading {rel}"))?;
            Ok((rel, bytes))
        })
        .collect::<Result<Vec<_>>>()?;

    if files.is_empty() {
        bail!("nothing to pack in {} (no input files)", dir.display());
    }

    nuthatch_identity::manifest(
        config.nest.name,
        config.nest.schema_version,
        env!("CARGO_PKG_VERSION").to_string(),
        registry_hash,
        files,
    )
}

/// The NID of the nest at `dir`, computed from its authored inputs (RFC-0032 §3).
///
/// This is how an *unpacked* nest directory - the thing an operator actually has on disk - gets the
/// identity its data is stored under. It regenerates the decode registry, so it costs what a mount
/// costs and is not something to call in a loop.
pub fn nest_nid(dir: &Path) -> Result<String> {
    Ok(build_manifest(dir, None)
        .with_context(|| format!("computing the nest identity of {}", dir.display()))?
        .nid())
}

/// `nuthatch nest bundle <dir> [--out <path>] [--as-dir]`: bundle a nest into a single portable,
/// content-addressed `.bundle` file holding its authored inputs plus `manifest.json`. With `--as-dir`,
/// write an unpacked bundle *directory* instead (handy for inspecting contents). Prints the bundle's
/// content address. Default output is `<nest-name>-<hash12>.bundle` beside the nest.
pub fn bundle(dir: &Path, out: Option<&Path>, as_dir: bool, allow_secrets: bool) -> Result<()> {
    if Config::load(dir).is_ok_and(|c| c.address_history.is_some()) {
        bail!(
            "{} declares [address_history]: an address-history nest watches private accounts and \
             is not bundled",
            crate::config::CONFIG_FILE
        );
    }
    let found = bundled_credentials(dir);
    if !found.is_empty() {
        let list = found
            .iter()
            .map(|f| format!("  - {f}"))
            .collect::<Vec<_>>()
            .join("\n");
        if !allow_secrets {
            bail!(
                "{} would publish {} credential(s) with it, because the bundle carries it verbatim:\n\
                 {list}\n\
                 Remove them before bundling (an RPC endpoint can be passed at run time with --rpc), \
                 or pass --allow-secrets if publishing them is intended.",
                crate::config::CONFIG_FILE,
                found.len()
            );
        }
        eprintln!("warning: bundling credential(s) because of --allow-secrets:\n{list}");
    }
    let manifest = build_manifest(dir, None)?;
    let hash = manifest.blob_hash();
    let default_out = |ext: &str| {
        let parent = dir.parent().unwrap_or_else(|| Path::new("."));
        parent.join(format!("{}-{}.{ext}", manifest.nest_name, &hash[..12]))
    };

    if as_dir {
        let out_dir = out
            .map(Path::to_path_buf)
            .unwrap_or_else(|| default_out("nest"));
        // If the chosen output dir is *inside* the nest, rebuild the manifest excluding it (so the
        // bundle doesn't try to pack itself). Rare, but a foot-gun worth closing.
        let (manifest, hash) = if out_dir.starts_with(dir) {
            let m = build_manifest(dir, Some(&out_dir))?;
            let h = m.blob_hash();
            (m, h)
        } else {
            (manifest, hash)
        };
        write_bundle_dir(dir, &out_dir, &manifest)?;
        println!("bundled nest '{}' (unpacked)", manifest.nest_name);
        println!("  dir:      {}", out_dir.display());
        println!("  hash:     {hash}");
        println!("  registry: {}", manifest.registry_hash);
        println!("  files:    {}", manifest.files.len());
        return Ok(());
    }

    let out_file = out
        .map(Path::to_path_buf)
        .unwrap_or_else(|| default_out("bundle"));
    write_bundle(dir, &manifest, &out_file)?;
    println!("bundled nest '{}'", manifest.nest_name);
    println!("  bundle:   {}", out_file.display());
    println!("  hash:     {hash}");
    println!("  registry: {}", manifest.registry_hash);
    println!("  files:    {}", manifest.files.len());
    println!();
    println!("tip: share this .bundle (a URL, or the file). Anyone can run your exact nest with");
    println!(
        "     `nuthatch nest load <file-or-url>` - every file is verified against the manifest,"
    );
    println!(
        "     and the decode registry is reproduced from the inputs. Pin it with `--expect {}`.",
        &hash[..12]
    );
    Ok(())
}

/// Materialise a bundle's files (from the nest `src`) plus `manifest.json` into `out_dir` - the
/// unpacked on-disk layout shared by the `--as-dir` form and, tarred, the `.bundle`.
fn write_bundle_dir(src: &Path, out_dir: &Path, manifest: &Manifest) -> Result<()> {
    std::fs::create_dir_all(out_dir)
        .with_context(|| format!("creating blob dir {}", out_dir.display()))?;
    for f in &manifest.files {
        let dst = out_dir.join(&f.path);
        if let Some(p) = dst.parent() {
            std::fs::create_dir_all(p)?;
        }
        std::fs::copy(src.join(&f.path), &dst).with_context(|| format!("copying {}", f.path))?;
    }
    // Pretty-print the *stored* manifest for human readability; the blob hash is over the canonical
    // (compact) bytes, so on-disk formatting never affects identity.
    std::fs::write(
        out_dir.join("manifest.json"),
        serde_json::to_string_pretty(manifest)?,
    )
    .context("writing manifest.json")?;
    Ok(())
}

/// Write a single-file `.bundle`: a tar of `manifest.json` + every manifest file, read from the nest
/// `src`. The bundle's *identity* is `manifest.blob_hash()` (over the canonical manifest), so the tar's
/// own byte layout is immaterial - a load re-verifies each file against the manifest regardless.
fn write_bundle(src: &Path, manifest: &Manifest, out_file: &Path) -> Result<()> {
    let file = std::fs::File::create(out_file)
        .with_context(|| format!("creating bundle {}", out_file.display()))?;
    let mut ar = tar::Builder::new(file);

    let manifest_bytes = serde_json::to_vec_pretty(manifest)?;
    let mut header = tar::Header::new_gnu();
    header.set_size(manifest_bytes.len() as u64);
    header.set_mode(0o644);
    header.set_mtime(0);
    header.set_cksum();
    ar.append_data(&mut header, "manifest.json", &manifest_bytes[..])
        .context("adding manifest.json to bundle")?;

    for f in &manifest.files {
        ar.append_path_with_name(src.join(&f.path), Path::new(&f.path))
            .with_context(|| format!("adding {} to bundle", f.path))?;
    }
    ar.finish().context("finalising bundle")?;
    Ok(())
}

/// Read + parse a blob's `manifest.json`.
pub fn load_manifest(blob_dir: &Path) -> Result<Manifest> {
    let raw = std::fs::read_to_string(blob_dir.join("manifest.json"))
        .with_context(|| format!("reading blob manifest in {}", blob_dir.display()))?;
    serde_json::from_str(&raw).context("parsing blob manifest")
}

/// The manifest of a single-file `.bundle`, read without installing it - extract, parse, done. The
/// registry (RFC-0019) uses this to key a blob by its content address (`.blob_hash()`) and to read its
/// nest name for the default `name@version`, all without touching the install path.
pub fn bundle_manifest(bundle_file: &Path) -> Result<Manifest> {
    let tmp = tempfile::tempdir().context("temp dir for bundle manifest")?;
    let blob_dir = tmp.path().join("unpacked");
    extract_bundle(bundle_file, &blob_dir)?;
    load_manifest(&blob_dir)
}

/// Join a **manifest-declared** relative path onto `base`, refusing anything that could escape it - an
/// bundle is a distributable, hash-resolved deploy unit (RFC-0012 §4/§5), so its file paths are
/// untrusted input. Only `Normal` path components are allowed: an absolute path (which `Path::join`
/// would let *replace* the base), a `..` parent, a root/prefix, or a bare `.` are all rejected. This is
/// the zip-slip / absolute-path-escape guard for `load`.
pub(crate) fn checked_join(base: &Path, rel: &str) -> Result<PathBuf> {
    let rel_path = Path::new(rel);
    if rel_path.is_absolute() {
        bail!("blob file path {rel:?} is absolute - refusing (path-traversal guard)");
    }
    for comp in rel_path.components() {
        if !matches!(comp, std::path::Component::Normal(_)) {
            bail!(
                "blob file path {rel:?} has an illegal component - refusing (path-traversal guard)"
            );
        }
    }
    Ok(base.join(rel_path))
}

/// `nuthatch nest load <bundle> [--dir <target>] [--expect <hash>]`: load a bundle into a runnable
/// nest. `bundle` may be a `.bundle` file, an `http(s)://` URL to one, or an already-unpacked bundle
/// directory. A URL or file is resolved to a local bundle dir (fetch + untar), then verified and
/// installed by [`install_verified`]. The fetch is the *only* network touch and only when you pass a
/// URL - a local `.bundle` or dir loads fully offline.
pub async fn load(bundle: &str, target: Option<&Path>, expect: Option<&str>) -> Result<()> {
    if bundle.starts_with("http://") || bundle.starts_with("https://") {
        let bytes = reqwest::get(bundle)
            .await
            .with_context(|| format!("fetching bundle from {bundle}"))?
            .error_for_status()
            .with_context(|| format!("fetching bundle from {bundle}"))?
            .bytes()
            .await
            .context("reading fetched bundle bytes")?;
        let tmp = tempfile::tempdir().context("temp dir for fetched bundle")?;
        let bundle_file = tmp.path().join("fetched.bundle");
        std::fs::write(&bundle_file, &bytes).context("writing fetched bundle")?;
        let blob_dir = tmp.path().join("unpacked");
        extract_bundle(&bundle_file, &blob_dir)?;
        return install_verified(&blob_dir, target, expect);
    }
    let path = Path::new(bundle);
    if path.is_dir() {
        // Already an unpacked bundle directory (e.g. `bundle --as-dir` output) - install straight from it.
        return install_verified(path, target, expect);
    }
    if path.is_file() {
        let tmp = tempfile::tempdir().context("temp dir for bundle")?;
        let blob_dir = tmp.path().join("unpacked");
        extract_bundle(path, &blob_dir)?;
        return install_verified(&blob_dir, target, expect);
    }
    bail!(
        "nothing to load at '{bundle}' - expected a .bundle file, an http(s):// URL, or a bundle directory"
    );
}

/// Whether a URL's host is loopback - the one case that needs no warning, since it cannot reach
/// anything the operator's own machine cannot already reach. Parsed by hand (no `url` dep): scheme,
/// then authority up to the first `/`, `?` or `#`, then userinfo and port stripped.
/// The credentials `nuthatch.toml` would carry into a bundle (#1589), each described with its URL
/// redacted. A config that will not parse yields nothing here; the manifest build reports it.
fn bundled_credentials(dir: &Path) -> Vec<String> {
    let Ok(config) = crate::config::Config::load(dir) else {
        return Vec::new();
    };
    let shown = crate::rpc::redact_url;
    let mut found = Vec::new();
    for url in config
        .nest
        .rpc_urls
        .iter()
        .filter(|u| url_carries_credential(u))
    {
        found.push(format!("rpc_urls: {} (a key in the URL)", shown(url)));
    }
    for w in &config.webhooks {
        if w.secret.is_some() {
            found.push(format!("webhook '{}': its HMAC secret", w.name));
        }
        if url_carries_credential(&w.url) {
            found.push(format!(
                "webhook '{}': {} (a key in the URL)",
                w.name,
                shown(&w.url)
            ));
        }
    }
    for a in config
        .alerts
        .iter()
        .filter(|a| url_carries_credential(&a.url))
    {
        found.push(format!("alert sink: {} (a key in the URL)", shown(&a.url)));
    }
    found
}

/// Whether a URL looks like it embeds a credential: userinfo, a key-named query parameter, or a path
/// segment shaped like a token (long, and mixing letters with digits), as provider keys and Slack
/// hooks are. `/rpc` and `/ext/bc/C/rpc` are not.
fn url_carries_credential(url: &str) -> bool {
    let Ok(u) = reqwest::Url::parse(url) else {
        return false;
    };
    if !u.username().is_empty() || u.password().is_some() {
        return true;
    }
    if u.query_pairs().any(|(k, _)| {
        let k = k.to_ascii_lowercase();
        ["key", "token", "secret"].iter().any(|w| k.ends_with(w))
            || ["auth", "pass", "password", "sig"].contains(&k.as_str())
    }) {
        return true;
    }
    let segments: Vec<&str> = u.path_segments().into_iter().flatten().collect();
    segments.iter().enumerate().any(|(i, s)| {
        s.len() >= 20
            && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
            && s.chars().any(|c| c.is_ascii_digit())
            && s.chars().any(|c| c.is_ascii_alphabetic())
            // Avalanche's `/ext/bc/<blockchain id>/rpc`: a public chain id, not a key.
            && !(i > 1 && segments[i - 2] == "ext" && segments[i - 1] == "bc")
    })
}

fn is_loopback_url(url: &str) -> bool {
    let after_scheme = url.split_once("://").map(|(_, r)| r).unwrap_or(url);
    let authority = after_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or(after_scheme);
    // `user:pass@host:port` → `host:port`
    let host_port = authority
        .rsplit_once('@')
        .map(|(_, h)| h)
        .unwrap_or(authority);
    // Strip the port, minding IPv6 literals (`[::1]:8080`).
    let host = if let Some(rest) = host_port.strip_prefix('[') {
        rest.split_once(']').map(|(h, _)| h).unwrap_or(rest)
    } else {
        host_port
            .split_once(':')
            .map(|(h, _)| h)
            .unwrap_or(host_port)
    };
    let host = host.to_ascii_lowercase();
    host == "localhost"
        || host == "::1"
        || host
            .parse::<std::net::Ipv4Addr>()
            .is_ok_and(|ip| ip.is_loopback())
        || host
            .parse::<std::net::Ipv6Addr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// Whether a URL points at the link-local range - `169.254.0.0/16` or `fe80::/10`, which includes
/// every cloud provider's instance-metadata endpoint (`169.254.169.254`, `fd00:ec2::254`).
///
/// Parsed from the host rather than resolved: resolving would itself be a request, and a name that
/// resolves to link-local is a DNS-rebinding problem this warning cannot solve anyway. The address
/// is read in every spelling a URL parser accepts - decimal, hex, octal, IPv4-mapped (#2142).
fn is_link_local_url(url: &str) -> bool {
    let rest = url.split_once("://").map_or(url, |(_, r)| r);
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let host_port = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    if let Some(v6) = host_port.strip_prefix('[') {
        let v6 = v6.split([']', '%']).next().unwrap_or("");
        return v6.parse::<std::net::Ipv6Addr>().is_ok_and(|v6| {
            let seg = v6.segments();
            // fe80::/10 link-local, plus the fd00:ec2::/32 metadata range AWS uses.
            v6.to_ipv4().is_some_and(|v4| v4.is_link_local())
                || (seg[0] & 0xffc0) == 0xfe80
                || (seg[0] == 0xfd00 && seg[1] == 0x0ec2)
        });
    }
    let host = host_port.split(':').next().unwrap_or("");
    url_ipv4(host).is_some_and(|v4| v4.is_link_local())
}

/// An IPv4 host as the WHATWG URL parser reads it: one to four dot-separated parts, each decimal,
/// `0x` hex or leading-zero octal, the last filling the remaining bytes.
fn url_ipv4(host: &str) -> Option<std::net::Ipv4Addr> {
    let host = host.strip_suffix('.').unwrap_or(host);
    let parts: Vec<&str> = host.split('.').collect();
    if parts.len() > 4 {
        return None;
    }
    let mut nums = Vec::with_capacity(parts.len());
    for p in &parts {
        let n = if let Some(hex) = p.strip_prefix("0x").or_else(|| p.strip_prefix("0X")) {
            if hex.is_empty() {
                0
            } else {
                u64::from_str_radix(hex, 16).ok()?
            }
        } else if p.len() > 1 && p.starts_with('0') {
            u64::from_str_radix(&p[1..], 8).ok()?
        } else {
            p.parse::<u64>().ok()?
        };
        nums.push(n);
    }
    let (last, init) = nums.split_last()?;
    if init.iter().any(|&n| n > 255) || *last >= 1u64 << (8 * (5 - nums.len())) {
        return None;
    }
    let addr = init
        .iter()
        .enumerate()
        .fold(*last, |acc, (i, &n)| acc + (n << (8 * (3 - i))));
    Some(std::net::Ipv4Addr::from(u32::try_from(addr).ok()?))
}

/// Warn about every non-loopback URL a just-installed nest declares (audit L6). Best-effort: a config
/// that will not parse is a louder problem the next step reports, so it is silently skipped here.
/// The outbound endpoints a nest declares, classified. Returned rather than only logged so the
/// classification is *reachable from a test* - a mutation that stopped ranking link-local targets
/// survived a test that called `is_link_local_url` directly, which proved the helper worked and
/// nothing about whether anything used it. Same defect as a control that is unit-tested and unwired.
fn classify_outbound(dir: &Path) -> Vec<(&'static str, String, bool)> {
    let Ok(config) = crate::config::Config::load(dir) else {
        return Vec::new();
    };
    let mut out: Vec<(&'static str, String)> = Vec::new();
    for w in &config.webhooks {
        out.push(("webhook", w.url.clone()));
    }
    for a in &config.alerts {
        out.push(("alert sink", a.url.clone()));
    }
    for r in &config.nest.rpc_urls {
        out.push(("rpc", r.clone()));
    }
    out.into_iter()
        .filter(|(_, u)| !is_loopback_url(u))
        .map(|(k, u)| {
            let ll = is_link_local_url(&u);
            (k, u, ll)
        })
        .collect()
}

fn warn_outbound_urls(dir: &Path) {
    let external = classify_outbound(dir);
    if external.is_empty() {
        return;
    }
    tracing::warn!(
        "this nest declares {} non-loopback outbound endpoint(s) - review before running it:",
        external.len()
    );
    for (kind, url, link_local) in &external {
        // Redact any credentials in the URL: this line goes to logs, and provider URLs carry keys.
        let shown = crate::rpc::redact_url(url);
        // **Audit finding 7**: rank the targets. A bundle is fetched from a URL, so these endpoints
        // are untrusted input, and "non-loopback" flattens a cloud-metadata address into the same
        // line as a Slack webhook. The link-local range is where an SSRF actually pays - instance
        // credentials live there - so it gets its own line rather than being one of twelve to skim.
        if *link_local {
            tracing::error!(
                "  {kind}: {shown}  <-- LINK-LOCAL/METADATA ADDRESS. A nest has no legitimate reason \
                 to reach this; on a cloud host it is where instance credentials are served. Do not \
                 run this nest unless you know exactly why it is there."
            );
        } else {
            tracing::warn!("  {kind}: {shown}");
        }
    }
}

/// Untar a `.bundle` into `dest`. `tar`'s `unpack` refuses entries that would escape `dest` (its own
/// zip-slip guard); [`install_verified`] then re-checks every *manifest-declared* file with
/// [`checked_join`], so extraction and install are both bounded - defence in depth on untrusted input.
fn extract_bundle(bundle_file: &Path, dest: &Path) -> Result<()> {
    let file = std::fs::File::open(bundle_file)
        .with_context(|| format!("opening bundle {}", bundle_file.display()))?;
    std::fs::create_dir_all(dest).with_context(|| format!("creating {}", dest.display()))?;
    tar::Archive::new(file)
        .unpack(dest)
        .with_context(|| format!("unpacking bundle {}", bundle_file.display()))?;
    Ok(())
}

/// Verify a resolved blob directory and install it as a runnable nest. Verification is three-fold, all
/// local (RFC-0012 §5): the manifest's format version is understood, every file's bytes hash to what
/// the manifest claims (integrity), and - after install - the decode registry *regenerated from the
/// installed inputs* equals the manifest's `registry_hash` (the nest decodes exactly as authored). With
/// `expect`, the blob's own content address is asserted too, so you load *that* bundle and no other.
pub fn install_verified(
    blob_dir: &Path,
    target: Option<&Path>,
    expect: Option<&str>,
) -> Result<()> {
    let manifest = load_manifest(blob_dir)?;

    // Format gate - reject a blob authored by a newer nuthatch, exactly as `config.rs` rejects a newer
    // schema_version. A too-new manifest may hash/verify by rules this build doesn't know.
    if manifest.blob_format_version > BLOB_FORMAT_VERSION {
        bail!(
            "blob needs manifest format v{} but this build understands up to v{} - upgrade nuthatch",
            manifest.blob_format_version,
            BLOB_FORMAT_VERSION
        );
    }

    // Content-address check (optional): the blob you asked for is the blob you got.
    let hash = manifest.blob_hash();
    if let Some(want) = expect {
        if hash != want {
            bail!("blob hash mismatch: expected {want}, got {hash}");
        }
    }

    // Integrity: every file's bytes hash to the manifest's claim.
    for f in &manifest.files {
        let bytes = std::fs::read(checked_join(blob_dir, &f.path)?)
            .with_context(|| format!("blob is missing declared file {}", f.path))?;
        let got = hex::encode(Sha256::digest(&bytes));
        if got != f.sha256 {
            bail!(
                "blob file {} is corrupt: manifest {}, actual {got}",
                f.path,
                f.sha256
            );
        }
    }

    let target = match target {
        Some(t) => t.to_path_buf(),
        None => PathBuf::from(&manifest.nest_name),
    };
    if target.exists() && std::fs::read_dir(&target)?.next().is_some() {
        bail!("target {} exists and is not empty", target.display());
    }
    for f in &manifest.files {
        let dst = checked_join(&target, &f.path)?;
        if let Some(p) = dst.parent() {
            std::fs::create_dir_all(p)?;
        }
        std::fs::copy(checked_join(blob_dir, &f.path)?, &dst)
            .with_context(|| format!("installing {}", f.path))?;
    }

    // The load-bearing check: regenerate the decode registry from the *installed* inputs and assert it
    // matches the manifest. Same inputs + same generator → same decode, verifiably.
    verify_registry_reproduces(&target, &manifest)?;

    // Audit L6 (defence in depth): a freshly-loaded nest is third-party config, and its `nuthatch.toml`
    // decides where this process makes *outbound* requests - webhook/alert sinks it POSTs decoded rows
    // to, and RPC endpoints it fetches from. A hostile or careless bundle can therefore point those at
    // an internal address the operator never intended (SSRF), or exfiltrate indexed data to a third
    // party. Warn, don't refuse: external sinks are the normal case and only the operator knows which
    // are wanted. The point is that they see the list once, at load, rather than discovering it in
    // outbound traffic later.
    warn_outbound_urls(&target);

    println!(
        "loaded nest '{}' → {}",
        manifest.nest_name,
        target.display()
    );
    println!("  hash:     {hash}");
    println!(
        "  registry: {} (reproduced from inputs ✓)",
        manifest.registry_hash
    );
    println!();
    println!(
        "tip: it's yours to run - `nuthatch dev --dir {}`. It decodes byte-for-byte as the author",
        target.display()
    );
    println!("     bundled it (every file hashed, registry reproduced from inputs).");
    Ok(())
}

/// Verify that a nest dir's inputs reproduce the `registry_hash` a manifest claims - the check `load`
/// runs. Kept here so `bundle` and load share one definition of "does this blob decode as promised".
pub fn verify_registry_reproduces(dir: &Path, manifest: &Manifest) -> Result<()> {
    let config = Config::load(dir)?;
    let regen = hex::encode(crate::registry::from_nest(dir, &config)?.hash());
    if regen != manifest.registry_hash {
        bail!(
            "registry hash mismatch: manifest claims {}, inputs regenerate {} - the blob was authored \
             by a different generator version",
            manifest.registry_hash,
            regen
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    /// Audit L6: loopback endpoints need no warning; anything else does. The parser has to cope with
    /// the shapes real config carries - ports, credentials, paths, IPv6 literals - because a URL
    /// mis-classified as loopback is a warning the operator never sees.
    #[test]
    fn loopback_urls_are_recognised_and_everything_else_is_not() {
        for local in [
            "http://localhost:8288/hook",
            "http://LOCALHOST/hook",
            "http://127.0.0.1:9000",
            "https://127.1.2.3/x",
            "http://[::1]:8288/rpc",
            "http://user:pass@localhost:8288/hook",
            "http://localhost",
        ] {
            assert!(is_loopback_url(local), "should be loopback: {local}");
        }
        for external in [
            "https://hooks.example.com/abc",
            "http://10.0.0.5:8080/internal", // private, but NOT loopback - SSRF-relevant
            "http://169.254.169.254/latest/meta-data", // cloud metadata: the classic SSRF target
            "https://evil.test/exfil",
            "http://[2001:db8::1]:80/",
            "http://user:pass@evil.test/exfil",
            "https://localhost.evil.test/", // suffix trickery must not read as loopback
        ] {
            assert!(
                !is_loopback_url(external),
                "should NOT be loopback: {external}"
            );
        }
    }

    use super::*;
    use crate::config::CONFIG_FILE;

    /// SEC-1: a blob is untrusted, distributable input - `mount` must refuse a manifest whose file
    /// paths would escape the target (zip-slip `../` or an absolute path, which `Path::join` would let
    /// *replace* the base). Otherwise mounting a hostile blob is an arbitrary file write → RCE.
    #[test]
    fn mount_refuses_path_traversal() {
        for evil_path in [
            "../escaped.txt",
            "/tmp/nuthatch-escape.txt",
            "a/../../escaped.txt",
        ] {
            let blob = tempfile::tempdir().unwrap();
            let manifest = Manifest {
                blob_format_version: BLOB_FORMAT_VERSION,
                nest_name: "evil".into(),
                schema_version: 1,
                generator_version: "x".into(),
                registry_hash: "00".into(),
                files: vec![FileEntry {
                    path: evil_path.into(),
                    sha256: "0".repeat(64),
                }],
            };
            std::fs::write(
                blob.path().join("manifest.json"),
                serde_json::to_string(&manifest).unwrap(),
            )
            .unwrap();
            let target = tempfile::tempdir().unwrap();
            let err = install_verified(blob.path(), Some(target.path()), None)
                .unwrap_err()
                .to_string();
            assert!(
                err.contains("path-traversal"),
                "path {evil_path:?} should be refused, got: {err}"
            );
        }
    }

    /// A minimal nest dir (config + one ABI) for exercising bundle/load.
    fn write_nest(dir: &Path) {
        std::fs::write(
            dir.join(CONFIG_FILE),
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
        std::fs::create_dir_all(dir.join("abis")).unwrap();
        std::fs::write(
            dir.join("abis/c.json"),
            r#"[{"type":"event","name":"Transfer","anonymous":false,"inputs":[{"name":"from","type":"address","indexed":true},{"name":"to","type":"address","indexed":true},{"name":"value","type":"uint256","indexed":false}]}]"#,
        )
        .unwrap();
        std::fs::write(dir.join("llms.txt"), "how to query this nest\n").unwrap();
    }

    #[test]
    fn a_url_credential_is_told_from_a_public_path() {
        for keyed in [
            "https://mainnet.infura.io/v3/9aa3d95b3bc440fa88ea12eaa4456161",
            "https://eth-mainnet.g.alchemy.com/v2/Xk2mP9qLr4TzW8vN3bY6cD1e",
            "https://lb.drpc.org/ogrpc?network=ethereum&dkey=abc",
            "https://user:pass@rpc.example.com",
            "https://hooks.example.com/services/x7Yz4Q9mW2rT8kL5pN3vJ6hG",
        ] {
            assert!(url_carries_credential(keyed), "{keyed}");
        }
        for public in [
            "https://arb1.arbitrum.io/rpc",
            "https://ethereum-rpc.publicnode.com",
            "https://rpc.ankr.com/eth",
            "https://api.avax.network/ext/bc/C/rpc",
            "https://api.avax.network/ext/bc/2q9e4r6Mu3U68nU1fYjgbR6JvwrRx36CohpAX5UQxse55x1Q5/rpc",
            "https://h.example/in?signature=required",
            "http://127.0.0.1:8469/hooks",
            "https://x",
        ] {
            assert!(!url_carries_credential(public), "{public}");
        }
    }

    /// #1589: `nuthatch.toml` is bundled verbatim, so `bundle` refuses one that holds a credential
    /// unless told otherwise, and the NID of a clean nest is untouched by the check.
    #[test]
    fn bundle_refuses_a_nest_carrying_credentials() {
        let src = tempfile::tempdir().unwrap();
        write_nest(src.path());
        let clean_nid = build_manifest(src.path(), None).unwrap().nid();
        let out = tempfile::tempdir().unwrap();
        bundle(src.path(), Some(&out.path().join("a.bundle")), false, false).unwrap();

        let config = std::fs::read_to_string(src.path().join(CONFIG_FILE)).unwrap();
        std::fs::write(
            src.path().join(CONFIG_FILE),
            config.replace(
                "rpc_urls = [\"https://x\"]",
                "rpc_urls = [\"https://mainnet.infura.io/v3/9aa3d95b3bc440fa88ea12eaa4456161\", \"https://alice:SECRETPW@rpc.example.com\"]",
            ) + "\n[[webhooks]]\nname = \"w\"\ntable = \"c__transfer\"\nurl = \"https://h.example/in\"\nsecret = \"FAKE_HMAC\"\n",
        )
        .unwrap();
        let err = bundle(src.path(), Some(&out.path().join("b.bundle")), false, false)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("webhook 'w'") && err.contains("secret"),
            "{err}"
        );
        assert!(err.contains("https://mainnet.infura.io"), "{err}");
        assert!(
            !err.contains("9aa3d95b"),
            "the refusal must not print the key: {err}"
        );
        assert!(
            err.contains("https://rpc.example.com") && !err.contains("SECRETPW"),
            "{err}"
        );
        assert!(!out.path().join("b.bundle").exists());
        bundle(src.path(), Some(&out.path().join("c.bundle")), false, true).unwrap();
        assert!(
            out.path().join("c.bundle").exists(),
            "--allow-secrets bundles anyway"
        );

        std::fs::write(src.path().join(CONFIG_FILE), config).unwrap();
        assert_eq!(build_manifest(src.path(), None).unwrap().nid(), clean_nid);
    }

    /// RFC-0066 S0: the identities of fixed nests, pinned before the identity left this crate. A tab
    /// computes these with the same code, so a moved byte here re-indexes every published nest.
    #[test]
    fn the_identity_of_fixed_nests_is_pinned() {
        let a = tempfile::tempdir().unwrap();
        write_nest(a.path());
        let config = std::fs::read_to_string(a.path().join(CONFIG_FILE)).unwrap();
        std::fs::write(
            a.path().join(CONFIG_FILE),
            format!("{config}\n# kept\n[address_history]\naddresses = [\"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\"]\n"),
        )
        .unwrap();
        for (path, text) in [
            ("views/10-a.sql", "CREATE VIEW a AS SELECT 1;\n"),
            ("views-old.sql", "-- beside views/, not in it\n"),
            ("labels/x.json", "{}"),
            (".gitignore", "data/\n"),
        ] {
            let p = a.path().join(path);
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(p, text).unwrap();
        }
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let got: Vec<_> = [
            a.path().to_path_buf(),
            root.join("docs/bench/nests/usdc-120"),
            root.join("tests/fixtures/upgrade/config-4.0/nest"),
        ]
        .iter()
        .map(|d| {
            let m = build_manifest(d, None).unwrap_or_else(|e| panic!("{}: {e:#}", d.display()));
            (m.nid(), m.data_identity())
        })
        .collect();
        let want = [
            (
                "a0be1608eff7220aaf762ca91a2b987b15c5ab8127f350d1ef67af150fcd1522",
                "1bc66ea1d7ce3e8eb21b2e9c1b9156d1688e77d9c1d7d9c7fc767c86add0d1f0",
            ),
            (
                "9dd6d3c6a305597bd84e87c8a4b406d8e1b867ff67a066e8537a5ee67ae1adde",
                "36850efe8751a23007fd87d7b8bd3f17feefb5f6f8dffac6ec2de1d739422139",
            ),
            (
                "9fac46172d71472f25f6f8f93d1dc27c995195cff238854a43854210704b33fc",
                "b52c44602be972b99191267aac8601223b4c4f76a94b044f9ef50759cf3e9274",
            ),
        ];
        let want: Vec<_> = want
            .iter()
            .map(|(n, d)| (n.to_string(), d.to_string()))
            .collect();
        assert_eq!(got, want);
    }

    #[test]
    fn manifest_is_deterministic_and_pins_the_registry_hash() {
        let a = tempfile::tempdir().unwrap();
        write_nest(a.path());
        let m1 = build_manifest(a.path(), None).unwrap();
        let m2 = build_manifest(a.path(), None).unwrap();
        // Same inputs → byte-identical canonical manifest → identical blob hash (the determinism rule).
        assert_eq!(m1.blob_hash(), m2.blob_hash());
        assert_eq!(m1.canonical_bytes(), m2.canonical_bytes());
        // The manifest pins the regenerated decode registry, and it verifies against the inputs.
        let config = Config::load(a.path()).unwrap();
        let expected = hex::encode(
            crate::registry::from_nest(a.path(), &config)
                .unwrap()
                .hash(),
        );
        assert_eq!(m1.registry_hash, expected);
        verify_registry_reproduces(a.path(), &m1).unwrap();
        // Files are sorted and exclude nothing authored (config + abi + llms.txt = 3).
        assert_eq!(m1.files.len(), 3);
        assert!(m1.files.windows(2).all(|w| w[0].path <= w[1].path));
    }

    /// RFC-0032 §3: the NID is a **storage key**, so it must survive a nuthatch upgrade. The blob
    /// hash does not - it carries `generator_version` by design - and using it to key datasets would
    /// orphan every dataset on every release. This test is the difference between the two.
    #[test]
    fn the_nid_survives_a_nuthatch_upgrade() {
        let a = tempfile::tempdir().unwrap();
        write_nest(a.path());
        let mut old = build_manifest(a.path(), None).unwrap();
        old.generator_version = "1.0.2".into();
        let mut new = old.clone();
        new.generator_version = "2.0.0".into();

        // The blob hash is *supposed* to move: it pins which binary produced the bundle.
        assert_ne!(
            old.blob_hash(),
            new.blob_hash(),
            "the blob hash must still pin the producer"
        );
        // The identity must not. Same inputs, same decode, same nest, same data.
        assert_eq!(
            old.nid(),
            new.nid(),
            "upgrading nuthatch changed a nest's identity - every dataset would be orphaned and \
             re-indexed on upgrade. `generator_version` must stay out of the NID; the generator's \
             *behaviour* is already pinned by `registry_hash`."
        );
    }

    /// The other half of the contract, and the one refcounting depends on: any authored edit yields
    /// a different identity, so divergence forks its own dataset instead of contaminating a shared
    /// one. A NID that ignored inputs would pass the test above and be catastrophic.
    #[test]
    fn any_authored_edit_changes_the_nid() {
        let a = tempfile::tempdir().unwrap();
        write_nest(a.path());
        let before = nest_nid(a.path()).unwrap();

        // A non-decode input: docs.
        std::fs::write(a.path().join("llms.txt"), "different docs\n").unwrap();
        let after_docs = nest_nid(a.path()).unwrap();
        assert_ne!(
            before, after_docs,
            "editing an authored input must fork the identity"
        );

        // An input that changes *decode*, which also moves `registry_hash` inside the manifest.
        std::fs::write(
            a.path().join("abis/c.json"),
            r#"[{"type":"event","name":"Approval","anonymous":false,"inputs":[{"name":"owner","type":"address","indexed":true}]}]"#,
        )
        .unwrap();
        let after_abi = nest_nid(a.path()).unwrap();
        assert_ne!(
            after_docs, after_abi,
            "changing the ABI must fork the identity"
        );

        // And a NID is never a blob hash, even for the same nest - the domain separator sees to it.
        let m = build_manifest(a.path(), None).unwrap();
        assert_ne!(m.nid(), m.blob_hash());
    }

    /// The invariant adoption rests on (#364): everything early cutoff copies is something the
    /// identity ignores. Break it and `runtime::adopt_dataset` would change what the destination
    /// hashes to while filling it - an adoption that immediately reports itself as identity drift.
    #[test]
    fn derived_state_is_excluded_from_the_identity() {
        for name in DERIVED_STATE {
            assert!(
                EXCLUDE.contains(name),
                "'{name}' is copied by adoption but counted in the identity - copying it would move \
                 the NID of the dataset that adopted it"
            );
        }
        // And it is the whole of the derived state, not a subset: a dataset's data is its hot store
        // and its segments. `.git`/`.DS_Store` are excluded for a different reason and are not data.
        assert!(
            DERIVED_STATE.contains(&DB_FILE) && DERIVED_STATE.contains(&"segments"),
            "adoption must carry both halves of a dataset - hot store and sealed segments"
        );
    }

    #[test]
    fn hidden_files_do_not_move_the_data_identity() {
        let a = tempfile::tempdir().unwrap();
        write_nest(a.path());
        let before = build_manifest(a.path(), None).unwrap();
        std::fs::create_dir_all(a.path().join(".duckdb/extensions")).unwrap();
        std::fs::write(a.path().join(".duckdb/extensions/icu"), b"x").unwrap();
        std::fs::write(a.path().join(".gitignore"), b"target/\n").unwrap();
        std::fs::create_dir_all(a.path().join("abis/.cache")).unwrap();
        std::fs::write(a.path().join("abis/.cache/x"), b"y").unwrap();
        let after = build_manifest(a.path(), None).unwrap();
        assert_eq!(
            after.data_identity(),
            before.data_identity(),
            "litter moved the data identity"
        );
        assert_ne!(
            after.nid(),
            before.nid(),
            "the NID still counts every input"
        );
    }

    /// RFC-0033 §5. The whole point of early cutoff: a cosmetic edit moves the **package** identity
    /// and leaves the **data** identity alone, so the new NID can adopt the old dataset instead of
    /// re-indexing the chain.
    #[test]
    fn a_cosmetic_edit_moves_the_nid_but_not_the_data_identity() {
        let a = tempfile::tempdir().unwrap();
        write_nest(a.path());
        std::fs::create_dir_all(a.path().join("views")).unwrap();
        std::fs::create_dir_all(a.path().join("entities")).unwrap();
        std::fs::write(a.path().join("views/10-v.sql"), "CREATE VIEW v AS SELECT 1").unwrap();
        let before = build_manifest(a.path(), None).unwrap();

        for (file, contents, what) in [
            ("llms.txt", "completely different docs\n", "documentation"),
            (
                "views/10-v.sql",
                "CREATE VIEW renamed AS SELECT 1 -- and a comment",
                "an authored view, which is never materialised",
            ),
            (
                "semantic.toml",
                "[nest]\ndescription = \"x\"\n",
                "descriptions",
            ),
            (
                "entities.toml",
                "[[entities]]\nname = \"delegations\"\nsql = \"entities/delegations.sql\"\nkey = [\"delegator\"]\nmax_rows = 100\n",
                "an incremental entity declaration",
            ),
            (
                "entities/delegations.sql",
                "SELECT delegator, count(*) AS n FROM events GROUP BY delegator",
                "an incremental entity definition",
            ),
        ] {
            std::fs::write(a.path().join(file), contents).unwrap();
            let after = build_manifest(a.path(), None).unwrap();
            assert_ne!(
                before.nid(),
                after.nid(),
                "editing {what} must still move the NID - the package really did change"
            );
            assert_eq!(
                before.data_identity(),
                after.data_identity(),
                "editing {what} must NOT move the data identity, or the chain re-indexes for nothing"
            );
        }
    }

    /// The dangerous direction, and the one that must never regress: anything that changes what is
    /// **stored** must move the data identity, or a dataset gets adopted that should have differed.
    #[test]
    fn anything_that_changes_stored_data_moves_the_data_identity() {
        let a = tempfile::tempdir().unwrap();
        write_nest(a.path());
        let before = build_manifest(a.path(), None).unwrap().data_identity();

        // The ABI: decoding differently is different data, and it also moves `registry_hash`.
        std::fs::write(
            a.path().join("abis/c.json"),
            r#"[{"type":"event","name":"Approval","anonymous":false,"inputs":[{"name":"owner","type":"address","indexed":true}]}]"#,
        )
        .unwrap();
        let after_abi = build_manifest(a.path(), None).unwrap().data_identity();
        assert_ne!(before, after_abi, "a changed ABI is different data");

        // The config: a different contract address indexes a different contract.
        let b = tempfile::tempdir().unwrap();
        write_nest(b.path());
        let cfg = std::fs::read_to_string(b.path().join(CONFIG_FILE)).unwrap();
        std::fs::write(
            b.path().join(CONFIG_FILE),
            cfg.replace(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            ),
        )
        .unwrap();
        assert_ne!(
            before,
            build_manifest(b.path(), None).unwrap().data_identity(),
            "a different contract address is different data"
        );
    }

    /// RFC-0034 §3 + §5, the sequencing that made phase 2 affordable: editing the **author's query
    /// ceiling** must move the NID (it is an authored input) and **not** the data identity - or every
    /// security tweak re-indexes the chain, which is the exact problem phase 2 was sequenced behind
    /// early cutoff to avoid.
    #[test]
    fn editing_the_query_ceiling_does_not_force_a_re_index() {
        let a = tempfile::tempdir().unwrap();
        write_nest(a.path());
        std::fs::write(
            a.path().join("queries.toml"),
            "[[queries]]\nname = \"total\"\nsql = \"SELECT count(*) FROM t\"\n",
        )
        .unwrap();
        let before = build_manifest(a.path(), None).unwrap();

        // A security tweak: narrow what the nest sanctions.
        std::fs::write(
            a.path().join("queries.toml"),
            "[[queries]]\nname = \"total\"\nsql = \"SELECT count(*) FROM t WHERE ok\"\n",
        )
        .unwrap();
        let after = build_manifest(a.path(), None).unwrap();

        assert_ne!(
            before.nid(),
            after.nid(),
            "the ceiling is an authored input - the package really did change"
        );
        assert_eq!(
            before.data_identity(),
            after.data_identity(),
            "a ceiling edit must not move the data identity, or tightening a nest's query surface \
             costs a full re-index of the chain"
        );
    }

    /// The exclusion list is the one place a mistake costs correctness rather than speed, so its
    /// membership is pinned rather than left to a reader's memory.
    #[test]
    fn the_non_data_exclusion_list_is_exactly_what_it_claims() {
        for excluded in [
            "views/10-a.sql",
            "queries.toml",
            "graph/history.toml",
            "entities.toml",
            "entities/delegations.sql",
            "views/nested/deep.sql",
            "semantic.toml",
            "llms.txt",
            "README.md",
            ".claude/skills/x.md",
            "maintained.toml",
        ] {
            assert!(!affects_data(excluded), "{excluded} should be excluded");
        }
        for included in [
            "nuthatch.toml",
            "abis/erc20.json",
            "schema.json",
            "labels/exchanges.json",
            // Near-misses that must NOT be swept up by a prefix match.
            "views.sql",
            "my-views/x.sql",
            "semantic.toml.bak",
            "docs/README.md",
            "maintained.toml.bak",
        ] {
            assert!(affects_data(included), "{included} must affect data");
        }
    }

    fn address_nest(dir: &Path, history: &str) {
        std::fs::write(
            dir.join(CONFIG_FILE),
            format!(
                "[nest]\nname = \"rotki\"\nchain = \"mainnet\"\nchain_id = 1\n\
                 rpc_urls = [\"https://x\"]\nschema_version = 2\n{history}"
            ),
        )
        .unwrap();
    }

    /// RFC-0063: the watched accounts and the poll cadence change at runtime and are private, so
    /// neither moves the NID or the data identity, and the nest is not bundled at all.
    #[test]
    fn address_history_stays_out_of_the_identity_and_the_bundle() {
        let alice = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045";
        let bob = "0x00000000219ab540356cbb839cbe05303d7705fa";
        let variants = [
            format!("\n[address_history]\naddresses = [\"{alice}\"]\n"),
            format!(
                "\n[address_history] # rotki\naddresses = [\"{alice}\", \"{bob}\"]\npoll_interval = \"1m\"\n"
            ),
            format!("\n[ address_history ]\naddresses = [\"{bob}\"]\nstart_block = 7\n"),
            format!("\n[\"address_history\"]\r\naddresses = [\r\n  \"{bob}\",\r\n]\r\n"),
        ];
        let mut ids = Vec::new();
        for v in &variants {
            let d = tempfile::tempdir().unwrap();
            address_nest(d.path(), v);
            let m = build_manifest(d.path(), None).unwrap();
            ids.push((m.nid(), m.data_identity()));
        }
        assert!(ids.windows(2).all(|w| w[0] == w[1]), "{ids:?}");

        let d = tempfile::tempdir().unwrap();
        address_nest(d.path(), &variants[0]);
        let out = tempfile::tempdir().unwrap();
        let err = bundle(d.path(), Some(&out.path().join("b")), true, false).unwrap_err();
        assert!(err.to_string().contains("not bundled"), "{err}");
        assert!(!out.path().join("b").exists());

        // The cut takes the table and nothing after it: an edit in a table that follows still counts.
        let d = tempfile::tempdir().unwrap();
        address_nest(
            d.path(),
            &format!("{}\n[extract]\nblocks = true\n", variants[0]),
        );
        assert_ne!(build_manifest(d.path(), None).unwrap().nid(), ids[0].0);
    }

    /// The exclusion only ever cuts `[address_history]`: a file without it hashes as its raw bytes,
    /// so no existing nest's NID moves. The upgrade golden test pins one such NID end to end.
    #[test]
    fn a_config_without_address_history_hashes_as_written() {
        for path in [
            "tests/fixtures/network-nest/nuthatch.toml",
            "tests/fixtures/upgrade/config-4.0/nest/nuthatch.toml",
        ] {
            let raw = std::fs::read(Path::new(env!("CARGO_MANIFEST_DIR")).join(path)).unwrap();
            assert_eq!(without_address_history(raw.clone()).unwrap(), raw, "{path}");
        }
        let a = tempfile::tempdir().unwrap();
        write_nest(a.path());
        let m = build_manifest(a.path(), None).unwrap();
        let raw = std::fs::read(a.path().join(CONFIG_FILE)).unwrap();
        let entry = m.files.iter().find(|f| f.path == CONFIG_FILE).unwrap();
        assert_eq!(entry.sha256, hex::encode(Sha256::digest(&raw)));
    }

    /// Comments directly above the next table are that table's, and survive the cut byte for byte.
    #[test]
    fn the_cut_keeps_the_next_tables_comments() {
        let before = "[nest]\nname = \"n\"\n\n[address_history]\naddresses = []\n# inside\npoll_interval = \"1m\"\n\n# about extract\n[extract]\nblocks = true\n";
        let after = "[nest]\nname = \"n\"\n\n\n# about extract\n[extract]\nblocks = true\n";
        assert_eq!(
            String::from_utf8(without_address_history(before.as_bytes().to_vec()).unwrap())
                .unwrap(),
            after
        );
    }

    #[test]
    fn an_inline_address_history_is_refused_rather_than_half_cut() {
        for form in [
            "address_history = { addresses = [] }\n[nest]\nname = \"n\"\n",
            "address_history.addresses = []\n[nest]\nname = \"n\"\n",
        ] {
            let err = without_address_history(form.as_bytes().to_vec()).unwrap_err();
            assert!(
                err.to_string().contains("table of its own"),
                "{form}: {err}"
            );
        }
        let kept = without_address_history(
            b"[address_history]\naddresses = []\n\n[nest]\nname = \"n\"\n".to_vec(),
        )
        .unwrap();
        assert_eq!(kept, b"\n[nest]\nname = \"n\"\n");
    }

    /// RFC-0062: declaring a maintained view moves the NID, as any authored input does, and not the
    /// data identity, so a nest that declares one still seeds from a mirror published without it.
    /// The copies are derived state and move neither; a deeper directory of the same name is authored.
    #[test]
    fn maintained_views_move_the_nid_and_not_the_data() {
        let a = tempfile::tempdir().unwrap();
        write_nest(a.path());
        let plain = build_manifest(a.path(), None).unwrap();
        std::fs::write(
            a.path().join("maintained.toml"),
            "[[view]]\nname = \"bet\"\n",
        )
        .unwrap();
        let declared = build_manifest(a.path(), None).unwrap();
        assert_ne!(plain.nid(), declared.nid());
        assert_eq!(plain.data_identity(), declared.data_identity());

        let copies = a.path().join("maintained").join("bet");
        std::fs::create_dir_all(&copies).unwrap();
        std::fs::write(copies.join("ab.parquet"), b"PAR1").unwrap();
        let built = build_manifest(a.path(), None).unwrap();
        assert_eq!(built.nid(), declared.nid(), "a copy is not an input");
        assert_eq!(built.data_identity(), declared.data_identity());

        std::fs::create_dir_all(a.path().join("views").join("maintained")).unwrap();
        std::fs::write(a.path().join("views/maintained/x.sql"), "SELECT 1").unwrap();
        assert_ne!(
            build_manifest(a.path(), None).unwrap().nid(),
            declared.nid(),
            "an authored directory of the same name below the root"
        );
    }

    #[test]
    fn a_changed_input_changes_the_blob_hash() {
        let a = tempfile::tempdir().unwrap();
        write_nest(a.path());
        let before = build_manifest(a.path(), None).unwrap().blob_hash();
        // Touch an authored input.
        std::fs::write(a.path().join("llms.txt"), "different docs\n").unwrap();
        let after = build_manifest(a.path(), None).unwrap().blob_hash();
        assert_ne!(
            before, after,
            "the blob hash is content-addressed over its inputs"
        );
    }

    #[test]
    fn pack_then_mount_round_trips_and_verifies() {
        let src = tempfile::tempdir().unwrap();
        write_nest(src.path());
        let blob = tempfile::tempdir().unwrap();
        bundle(src.path(), Some(blob.path()), true, false).unwrap();

        let manifest = load_manifest(blob.path()).unwrap();
        let target = tempfile::tempdir().unwrap();
        // Install with the correct expected hash → a runnable nest whose registry reproduces.
        install_verified(
            blob.path(),
            Some(target.path()),
            Some(&manifest.blob_hash()),
        )
        .unwrap();
        assert!(target.path().join(CONFIG_FILE).exists());
        assert!(target.path().join("abis/c.json").exists());
        verify_registry_reproduces(target.path(), &manifest).unwrap();
    }

    #[tokio::test]
    async fn bundle_file_loads_and_verifies() {
        // The headline path: write a single-file `.bundle`, then load it from that file → a runnable
        // nest, hash-verified, registry reproduced. Exercises write_bundle → extract_bundle → install.
        let src = tempfile::tempdir().unwrap();
        write_nest(src.path());
        let out = tempfile::tempdir().unwrap();
        let bundle_file = out.path().join("t.bundle");
        let manifest = build_manifest(src.path(), None).unwrap();
        write_bundle(src.path(), &manifest, &bundle_file).unwrap();
        assert!(bundle_file.is_file(), "bundle is a single file");

        let target = tempfile::tempdir().unwrap();
        let installed = target.path().join("nest");
        load(
            bundle_file.to_str().unwrap(),
            Some(&installed),
            Some(&manifest.blob_hash()),
        )
        .await
        .unwrap();
        assert!(installed.join(CONFIG_FILE).exists());
        assert!(installed.join("abis/c.json").exists());
        verify_registry_reproduces(&installed, &manifest).unwrap();

        // A wrong --expect is refused even via the file→load path.
        let t2 = tempfile::tempdir().unwrap();
        assert!(load(
            bundle_file.to_str().unwrap(),
            Some(t2.path()),
            Some("deadbeef")
        )
        .await
        .is_err());
    }

    #[test]
    fn mount_rejects_a_tampered_file_and_a_wrong_hash() {
        let src = tempfile::tempdir().unwrap();
        write_nest(src.path());
        let blob = tempfile::tempdir().unwrap();
        bundle(src.path(), Some(blob.path()), true, false).unwrap();

        // Wrong expected hash → refuse before touching disk.
        let t0 = tempfile::tempdir().unwrap();
        assert!(install_verified(blob.path(), Some(t0.path()), Some("deadbeef")).is_err());

        // Tamper a file's bytes without updating the manifest → integrity check fails.
        std::fs::write(blob.path().join("llms.txt"), "tampered\n").unwrap();
        let t1 = tempfile::tempdir().unwrap();
        let err = install_verified(blob.path(), Some(t1.path()), None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("corrupt"), "got: {err}");
    }

    #[test]
    fn mount_rejects_a_newer_blob_format() {
        let src = tempfile::tempdir().unwrap();
        write_nest(src.path());
        let blob = tempfile::tempdir().unwrap();
        bundle(src.path(), Some(blob.path()), true, false).unwrap();
        // Rewrite the manifest claiming a future format version.
        let mut m = load_manifest(blob.path()).unwrap();
        m.blob_format_version = BLOB_FORMAT_VERSION + 1;
        std::fs::write(
            blob.path().join("manifest.json"),
            serde_json::to_string_pretty(&m).unwrap(),
        )
        .unwrap();
        let t = tempfile::tempdir().unwrap();
        let err = install_verified(blob.path(), Some(t.path()), None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("format v"), "got: {err}");
    }

    #[test]
    fn derived_files_are_excluded() {
        let a = tempfile::tempdir().unwrap();
        write_nest(a.path());
        // Simulate a run: a hot store + sealed segments appear. Neither must enter the blob.
        std::fs::write(a.path().join(DB_FILE), b"redb bytes").unwrap();
        std::fs::create_dir_all(a.path().join("segments")).unwrap();
        std::fs::write(a.path().join("segments/x.parquet"), b"parquet").unwrap();
        let m = build_manifest(a.path(), None).unwrap();
        assert!(m
            .files
            .iter()
            .all(|f| f.path != DB_FILE && !f.path.starts_with("segments/")));
        assert_eq!(m.files.len(), 3, "still just the 3 authored inputs");
    }

    /// **A malicious bundle cannot escape its destination through a symlink.**
    ///
    /// `checked_join` is lexical: it rejects `..` and absolute paths in *manifest-declared* names and
    /// cannot see a symlink at all. A bundle is fetched from a URL (`nest load https://…`), so its tar
    /// is untrusted, and the classic attack is a symlink entry followed by a file entry written
    /// *through* it.
    ///
    /// We are safe because `tar-rs` refuses the second write - not because of anything in this file.
    /// That is a dependency's guarantee, which is exactly the kind that disappears in a version bump
    /// or when someone reaches for `set_overwrite`. Pinned here so it fails loudly if it ever does.
    #[test]
    fn a_bundle_cannot_escape_its_destination_through_a_symlink() {
        let tmp = tempfile::tempdir().unwrap();
        let outside = tmp.path().join("outside");
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(outside.join("victim.txt"), b"original").unwrap();

        let bundle = tmp.path().join("evil.tar");
        {
            let f = std::fs::File::create(&bundle).unwrap();
            let mut ar = tar::Builder::new(f);
            // 1. a symlink that points out of the destination
            let mut h = tar::Header::new_gnu();
            h.set_entry_type(tar::EntryType::Symlink);
            h.set_size(0);
            h.set_mode(0o777);
            h.set_cksum();
            ar.append_link(&mut h.clone(), "escape", "../outside")
                .unwrap();
            // 2. a regular file written *through* it
            let data = b"PWNED";
            let mut h2 = tar::Header::new_gnu();
            h2.set_size(data.len() as u64);
            h2.set_mode(0o644);
            h2.set_cksum();
            ar.append_data(&mut h2, "escape/victim.txt", &data[..])
                .unwrap();
            ar.finish().unwrap();
        }

        let dest = tmp.path().join("unpacked");
        let r = extract_bundle(&bundle, &dest);
        let victim = std::fs::read_to_string(outside.join("victim.txt")).unwrap_or_default();
        assert!(
            r.is_err(),
            "unpacking a symlink-escape bundle must fail, not silently write outside"
        );
        assert_eq!(
            victim, "original",
            "a bundle wrote through a symlink and escaped its destination"
        );
    }

    /// **Audit finding 7.** A link-local target must be called out, not listed among a dozen ordinary
    /// endpoints. That range is where instance credentials are served, and a bundle is untrusted input.
    #[test]
    fn link_local_targets_are_recognised() {
        for hostile in [
            "http://169.254.169.254/latest/meta-data",
            "http://169.254.169.254",
            "https://user:pw@169.254.1.1/x",
            "http://[fd00:ec2::254]/latest/meta-data",
            "http://[fe80::1]/x",
        ] {
            assert!(is_link_local_url(hostile), "must be flagged: {hostile}");
        }
        for ordinary in [
            "https://hooks.slack.com/services/x",
            "https://eth.drpc.org",
            "http://192.168.1.5:8080/hook",
            "http://10.44.1.1:5433",
        ] {
            assert!(
                !is_link_local_url(ordinary),
                "must not be flagged - a private LAN address is normal for a self-hosted fleet: {ordinary}"
            );
        }
    }

    /// #2142: every spelling a URL parser accepts for the metadata address is the metadata address.
    #[test]
    fn link_local_targets_are_recognised_in_every_spelling() {
        for hostile in [
            "http://2852039166/latest/meta-data",
            "http://0xa9fea9fe/",
            "http://0xA9.0xFE.0xA9.0xFE/",
            "http://0251.0376.0251.0376/",
            "http://169.254.43518/",
            "http://169.16689662/",
            "http://169.254.169.254./",
            "http://[::ffff:169.254.169.254]/",
            "http://[::ffff:a9fe:a9fe]:80/",
            "http://u:p@[fe80::1%25en0]/x",
        ] {
            assert!(is_link_local_url(hostile), "must be flagged: {hostile}");
        }
        for ordinary in [
            "http://2130706433/",
            "http://[::ffff:10.0.0.1]/",
            "http://1.2.3.4.5/",
            "http://169.254.169.254.example.com/",
            "http://4294967296/",
        ] {
            assert!(
                !is_link_local_url(ordinary),
                "must not be flagged: {ordinary}"
            );
        }
    }

    /// The ranking must be **reachable from the nest**, not merely implemented.
    ///
    /// Written after a mutation that stopped ranking link-local targets survived a test calling
    /// `is_link_local_url` directly - proving the helper worked and nothing about whether anything
    /// used it. This goes from a nest directory on disk through the real classification path.
    #[test]
    fn a_nest_declaring_a_metadata_endpoint_is_ranked_from_its_config() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("abis")).unwrap();
        std::fs::write(dir.path().join("abis/t.json"), "[]").unwrap();
        std::fs::write(
            dir.path().join(crate::config::CONFIG_FILE),
            r#"
[nest]
name = "hostile"
chain = "mainnet"
chain_id = 1
rpc_urls = ["https://eth.drpc.org"]

[[contracts]]
alias = "t"
address = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
abi = "abis/t.json"

[[alerts]]
kinds = ["sanction_hit"]
url = "http://169.254.169.254/latest/meta-data/iam/security-credentials/"
"#,
        )
        .unwrap();

        let found = classify_outbound(dir.path());
        let metadata: Vec<_> = found.iter().filter(|(_, _, ll)| *ll).collect();
        assert_eq!(
            metadata.len(),
            1,
            "the metadata endpoint must be ranked as link-local: {found:?}"
        );
        assert_eq!(metadata[0].0, "alert sink");
        // …and the ordinary RPC endpoint alongside it must not be.
        assert!(
            found.iter().any(|(k, _, ll)| *k == "rpc" && !*ll),
            "a normal rpc endpoint must not be flagged: {found:?}"
        );
    }
}
