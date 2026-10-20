//! Scaffold a nest from a Graph Protocol subgraph manifest (#241 item 5).
//!
//! A subgraph manifest already states almost everything a nest needs: which
//! contracts, on which chain, from which block, and — crucially — which ABIs,
//! pinned by CID. For a codebase of beacon proxies those pinned ABIs are
//! strictly better than Sourcify, which returns the proxy ABI carrying none of
//! the events the contract actually emits. The manifest is the only good
//! source, and it is already content-addressed.
//!
//! Content-addressed **and now verified** (RFC-0037 slice 1, #642's sibling): every body is hashed
//! back to its CID by [`crate::cid`] before it is believed. This was written after a real gateway
//! answered HTTP 200 with the prose "Unable to retrieve content within timeout period" - and a
//! second answered with a different error string - for a CID that a third served correctly. Both
//! would have been vendored as the nest's manifest, because the only check was that the body was
//! non-empty.
//!
//! A multi-block file is proven by re-encoding it in Kubo's default layout, or from a CAR of its
//! blocks (RFC-0037 slice 6). One that neither proves is accepted here with a loud UNVERIFIED
//! warning, never silently; a nest's resolver refuses it instead.
//!
//! What the manifest **cannot** tell us is which template a factory creates:
//! that lives in the mapping WASM, as a `Template.create(address)` call. So
//! this module infers what it defensibly can and reports the rest by name,
//! with candidates, rather than guessing. A 70%-correct scaffold plus an
//! honest account of the other 30% is the deliverable — a scaffold that
//! quietly invents factory rules would be worse than none.
//!
//! The parse is deliberately tolerant. Manifests are machine-written by
//! `graph build`, but they are also hand-edited, and they vary across
//! `specVersion`s. A missing field becomes a note, not an error; the only
//! fatal conditions are "this isn't YAML" and "this has no dataSources".

use anyhow::{anyhow, bail, Context, Result};
use serde_json::Value;
use std::collections::BTreeMap;
use yaml_rust2::{Yaml, YamlLoader};

/// Public IPFS gateways tried in order. The Graph's own gateway first: it is
/// the authoritative host for published manifests and is fastest for them.
pub const DEFAULT_IPFS_GATEWAYS: &[&str] = &[
    "https://ipfs.thegraph.com/api/v0/cat?arg=",
    "https://ipfs.io/ipfs/",
    // Cloudflare retired `cloudflare-ipfs.com` - it no longer resolves at all, so it was a
    // guaranteed-dead third try. Pinata's public gateway answers the path-prefix form, which is
    // what this list needs: `dweb.link` and `w3s.link` redirect to a `<cid>.ipfs.<host>` subdomain
    // that prefix concatenation cannot express.
    "https://gateway.pinata.cloud/ipfs/",
];

/// Where a manifest's ABI actually lives (#1321).
///
/// A *deployed* subgraph pins every ABI by CID, because that is what `graph deploy` produces. A
/// subgraph **repository** - the thing a builder has before they deploy, and exactly when a nest is
/// most useful to them - references its ABIs as `file: ./abis/Token.json` instead. Both are ordinary
/// manifests; only one of them was fetchable before this.
#[derive(Debug, Clone, PartialEq)]
pub enum AbiLink {
    /// `file: { "/": "/ipfs/<cid>" }`. Content-addressed, so it can be fetched from anywhere.
    Cid(String),
    /// `file: ./abis/Token.json`. Relative to the manifest's own directory, and meaningful **only**
    /// when the manifest itself came from disk - see [`ManifestHome`].
    Path(String),
}

impl AbiLink {
    /// How to name this link in a message an operator reads.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Cid(c) => c,
            Self::Path(p) => p,
        }
    }
}

/// One ABI referenced by a manifest.
#[derive(Debug, Clone, PartialEq)]
pub struct AbiRef {
    pub name: String,
    pub link: AbiLink,
}

/// Where a manifest was read from, which is what decides whether a `file:` reference means anything.
///
/// This is the same rule [`Origin`] already enforces for URLs, applied to the filesystem. A manifest
/// fetched from a public gateway is a document a stranger wrote; letting it name a local path would
/// let them read files off the operator's machine, which is a worse version of the fetch-a-URL
/// problem `Origin::Manifest` exists to refuse.
#[derive(Debug, Clone, PartialEq)]
pub enum ManifestHome {
    /// Fetched over IPFS or HTTP. A `file:` reference is refused, loudly.
    Remote,
    /// Read from disk. A `file:` reference resolves inside this directory and nowhere else.
    Local(std::path::PathBuf),
}

/// A `dataSources[]` or `templates[]` entry, reduced to what a nest needs.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ManifestSource {
    /// `ethereum` / `ethereum/contract` for indexable sources; `file/ipfs` and
    /// friends are carried through so we can report why they were skipped.
    pub kind: String,
    pub name: String,
    pub network: Option<String>,
    pub address: Option<String>,
    pub start_block: Option<u64>,
    /// Which entry of `mapping.abis` is *this* source's own ABI.
    pub abi_name: Option<String>,
    pub abis: Vec<AbiRef>,
    /// `source.endBlock`, where the subgraph stops. A nest has no way to express one, so this
    /// exists to be *reported* - diverging from the manifest silently is the thing this module
    /// is not allowed to do.
    pub end_block: Option<u64>,
    /// True when the source declares `blockHandlers` or `callHandlers`. A block handler's nearest
    /// equivalent is `[extract] blocks` and a call handler's is `[extract] top_level_calls` - and a
    /// source carrying only them parses to an empty event list, which in `[[contracts]]` means
    /// "index every event in the ABI", the opposite of what the subgraph did.
    pub has_block_handlers: bool,
    pub has_call_handlers: bool,
    /// Event signatures with whitespace removed, e.g.
    /// `Transfer(indexedaddress,indexedaddress,uint256)` - folded scalars are collapsed so the
    /// signature matches the ABI-derived one character for character. Only [`event_name`] reads
    /// this, and it stops at the first `(`.
    pub events: Vec<String>,
}

impl ManifestSource {
    /// True for sources that index EVM logs. `file/ipfs` dataSources index the
    /// *content* behind a CID, which nuthatch does not do — it indexes the
    /// metadata hash as a column value and stops there.
    pub fn is_evm(&self) -> bool {
        self.kind.is_empty() || self.kind.starts_with("ethereum")
    }

    /// The CID of this source's own ABI, resolved through `mapping.abis`.
    ///
    /// Falls back to the first entry when the named one is absent. See [`Self::abi_is_fallback`]:
    /// the fallback is a guess, and a guess this module makes has to be reported.
    pub fn own_abi(&self) -> Option<&AbiRef> {
        match &self.abi_name {
            Some(want) => self
                .abis
                .iter()
                .find(|a| &a.name == want)
                .or_else(|| self.abis.first()),
            // No `source.abi` (common on templates from older specVersions):
            // fall back to the entry named after the source, then the first.
            None => self
                .abis
                .iter()
                .find(|a| a.name == self.name)
                .or_else(|| self.abis.first()),
        }
    }

    /// True when `source.abi` names an entry `mapping.abis` does not contain, so [`Self::own_abi`]
    /// fell back to the first one. The manifest asked for a specific ABI and we vendored a
    /// different one; that is exactly the kind of divergence this module is supposed to say out
    /// loud rather than absorb.
    pub fn abi_is_fallback(&self) -> bool {
        match &self.abi_name {
            Some(want) => !self.abis.iter().any(|a| &a.name == want) && !self.abis.is_empty(),
            None => false,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Manifest {
    pub spec_version: Option<String>,
    pub description: Option<String>,
    pub data_sources: Vec<ManifestSource>,
    pub templates: Vec<ManifestSource>,
}

impl Manifest {
    /// The network every EVM source agrees on. A manifest may in principle mix
    /// networks; a nest is single-chain, so disagreement is a hard error rather
    /// than a silent pick — indexing against the wrong chain corrupts state.
    pub fn network(&self) -> Result<String> {
        let mut seen: Vec<&str> = self
            .data_sources
            .iter()
            .filter(|d| d.is_evm())
            .filter_map(|d| d.network.as_deref())
            .collect();
        seen.sort_unstable();
        seen.dedup();
        match seen.as_slice() {
            [] => bail!("manifest declares no network on any EVM dataSource"),
            [one] => Ok((*one).to_string()),
            many => bail!(
                "manifest spans {} networks ({}) - a nest indexes exactly one chain, so import \
                 the sources for a single network or split the subgraph",
                many.len(),
                many.join(", ")
            ),
        }
    }
}

// ── Fetching ─────────────────────────────────────────────────────────────

/// Who supplied a reference - which decides whether it may be a URL at all.
///
/// **This distinction is the security boundary of subgraph import.** The argument an operator types
/// is theirs to point anywhere they like. Every other reference - the ABI links inside the manifest -
/// arrives from a document fetched off a public gateway, and is therefore attacker-controlled input
/// that happens to be handed to an HTTP client. Letting those be URLs turns `init --from-subgraph`
/// into a request forgery: a manifest carrying `file: {"/": "http://169.254.169.254/…"}` would make
/// the operator's own machine fetch cloud-instance credentials and write the reply into `abis/`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    /// The `--from-subgraph` argument. May be a CID or a URL; the operator chose it.
    Operator,
    /// A link read out of a manifest. **CID only** - never a URL, whatever it claims to be.
    Manifest,
}

/// The most sources one manifest may declare.
///
/// Every dataSource and template costs a gateway fetch and a file under `abis/`, and the
/// manifest - not the operator - decides how many there are. A 16 MiB document (the
/// [`MAX_FETCH_BYTES`] ceiling) holds on the order of 100k minimal entries, which is 100k
/// requests issued from the operator's address. Real manifests are tens; the POA DAO stack
/// that motivated this feature has 19.
const MAX_SOURCES: usize = 512;

/// A CID is base58btc or base32 - alphanumeric throughout. Anything else is a path, a query, a
/// scheme, or an attempt at one, and none of those belong in a gateway URL we build by
/// concatenation. The upper bound matters for the same reason: shape alone would accept a
/// multi-megabyte alphanumeric run, which we would then concatenate into three gateway URLs and
/// echo back inside an error message. CIDv0 is 46 characters, CIDv1 base32 is 59; 80 leaves room
/// for longer multihashes. There is deliberately no lower bound - a short string is merely a
/// 404 at the gateway, and inventing a minimum would reject identifiers that are legitimately
/// short without buying anything.
fn is_cid_shaped(s: &str) -> bool {
    !s.is_empty() && s.len() <= 80 && s.chars().all(|c| c.is_ascii_alphanumeric())
}

/// Accepts a bare CID, `ipfs://<cid>` or `/ipfs/<cid>` from anywhere, and a full http(s) URL only
/// from the operator (see [`Origin`]). Returns the list of URLs to try in order.
pub fn candidate_urls(source: &str, gateways: &[String], origin: Origin) -> Result<Vec<String>> {
    let s = source.trim();
    if s.is_empty() {
        bail!("empty subgraph source");
    }
    if s.starts_with("http://") || s.starts_with("https://") {
        return match origin {
            Origin::Operator => Ok(vec![s.to_string()]),
            Origin::Manifest => bail!(
                "this manifest asks us to fetch a URL rather than a CID: {s:?}. Refusing - a \
                 manifest comes from a public gateway, so honouring that would let whoever wrote it \
                 choose what your machine connects to. Only the --from-subgraph argument you type \
                 may be a URL."
            ),
        };
    }
    let cid = s
        .strip_prefix("ipfs://")
        .or_else(|| s.strip_prefix("/ipfs/"))
        .unwrap_or(s)
        .trim_matches('/');
    if !is_cid_shaped(cid) {
        bail!("'{source}' is not a CID or URL - expected e.g. QmVPhL… or ipfs://QmVPhL…");
    }
    Ok(gateways.iter().map(|g| format!("{g}{cid}")).collect())
}

/// Whether a fetched document was proven to be the one its CID names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Proof {
    Verified,
    /// Fetched but not proven: an operator's URL, which names a location rather than a content, or a
    /// file that neither its bytes nor its blocks could prove. The reason says which.
    Unproven(String),
}

pub struct Fetched {
    pub body: String,
    pub proof: Proof,
}

/// Every gateway that answered sent bytes nothing proved, and none large enough to be taken unproven.
/// `init` refuses it; a resolver counts it as unverified.
#[derive(Debug)]
pub struct NothingProved(pub String);

impl std::fmt::Display for NothingProved {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for NothingProved {}

/// Read a manifest, from disk when `source` names a file that exists and over IPFS otherwise.
///
/// Returning the [`ManifestHome`] alongside the text is the point: every later decision about a
/// `file:` ABI reference turns on where the manifest came from, and threading that as a value rather
/// than re-deriving it means the two cannot drift apart.
///
/// **Disk wins only if the path actually exists.** A CID cannot be a path and a path cannot be a
/// CID, so there is no ambiguity to resolve - but checking existence rather than shape means a
/// mistyped path falls through to the gateway and gets the old "not a CID or URL" error, which names
/// the real problem.
pub async fn read_manifest(source: &str, gateways: &[String]) -> Result<(String, ManifestHome)> {
    let as_path = std::path::Path::new(source);
    if as_path.is_file() {
        let text = std::fs::read_to_string(as_path)
            .with_context(|| format!("cannot read the manifest at {source}"))?;
        // The parent of the manifest, canonicalised once here so every later comparison is against a
        // real path rather than one carrying `..` or a symlink that would compare unequal.
        let home = as_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .canonicalize()
            .with_context(|| format!("cannot resolve the directory holding {source}"))?;
        return Ok((text, ManifestHome::Local(home)));
    }
    let text = fetch_ipfs(source, gateways, Origin::Operator).await?;
    Ok((text, ManifestHome::Remote))
}

/// Resolve a manifest's `file:` ABI reference against the directory the manifest came from (#1321).
///
/// Two refusals, and both matter for the same reason `Origin::Manifest` refuses a URL: the manifest
/// is a document, and a document must not be able to choose what this process reads.
///
/// - A **remote** manifest has no directory, so a `file:` reference is refused outright. Honouring
///   one would let whoever published the CID name a path on the operator's machine, which is the
///   fetch-a-URL problem with the blast radius pointing inward instead of outward.
/// - A **local** manifest's reference must stay inside its own directory. `../../../../etc/passwd`
///   is the obvious shape; a symlink pointing out of the tree is the one that is not obvious, which
///   is why both sides are canonicalised before they are compared rather than the string being
///   inspected for `..`.
pub fn resolve_abi_path(home: &ManifestHome, rel: &str) -> Result<std::path::PathBuf> {
    let dir = match home {
        ManifestHome::Remote => bail!(
            "this manifest references an ABI as a local file (`{rel}`), but it was fetched rather \
             than read from disk. Refusing - a fetched manifest naming a path on your machine is a \
             manifest choosing what this process reads. Pass the repository's subgraph.yaml as a \
             path if you have it, or use a deployed CID."
        ),
        ManifestHome::Local(d) => d,
    };
    // Canonical here rather than trusted from the caller: a home under a symlink, such as macOS's
    // `/var`, would otherwise refuse every file inside it.
    let dir = dir
        .canonicalize()
        .with_context(|| format!("cannot resolve the manifest's directory {}", dir.display()))?;
    let joined = dir.join(rel);
    let full = joined.canonicalize().with_context(|| {
        format!(
            "the manifest references `{rel}`, which is not there - expected {}",
            joined.display()
        )
    })?;
    if !full.starts_with(&dir) {
        bail!(
            "the manifest references `{rel}`, which resolves to {} - outside the manifest's own \
             directory ({}). Refusing: a manifest may name its own files and no others.",
            full.display(),
            dir.display()
        );
    }
    Ok(full)
}

/// Fetch a document, trying each gateway until one answers. Gateways fail
/// often and individually, so a single failure is never fatal; only running
/// out of them is, and then we say which ones we tried.
///
/// An unproven document is accepted loudly, which suits `init` vendoring an operator's manifest. A
/// nest's resolver calls [`fetch_ipfs_proven`] instead, and stores only what verified.
pub async fn fetch_ipfs(source: &str, gateways: &[String], origin: Origin) -> Result<String> {
    let fetched = fetch_ipfs_proven(source, gateways, origin).await?;
    if let (Proof::Unproven(why), Some(_)) = (&fetched.proof, cid_of(source)) {
        tracing::warn!(
            "{source}: {} bytes accepted UNVERIFIED ({why})",
            fetched.body.len()
        );
    }
    Ok(fetched.body)
}

/// [`fetch_ipfs`], saying whether the document was proven.
///
/// The file bytes come first, proven by re-encoding. When they cannot be, or every gateway sent
/// something else, the blocks are asked for as a CAR and each is checked against the CID naming it.
/// A document over the caps is refused with [`crate::cid::OverCap`].
pub async fn fetch_ipfs_proven(
    source: &str,
    gateways: &[String],
    origin: Origin,
) -> Result<Fetched> {
    fetch_ipfs_proven_within(source, gateways, origin, FETCH_TIMEOUT).await
}

/// Each request's timeout in [`fetch_ipfs_proven`], body included.
pub const FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// [`fetch_ipfs_proven`] with each request, body included, cut off at `timeout`.
pub async fn fetch_ipfs_proven_within(
    source: &str,
    gateways: &[String],
    origin: Origin,
    timeout: std::time::Duration,
) -> Result<Fetched> {
    let urls = candidate_urls(source, gateways, origin)?;
    // The CID the document must hash to. `None` only for an operator-supplied URL, which addresses a
    // location rather than a content, and therefore commits to nothing we can check.
    let named = cid_of(source);
    let expect = named
        .as_deref()
        .and_then(|c| crate::cid::Cid::parse(c).ok());
    let client = reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .context("failed to build HTTP client")?;

    let mut failures = Vec::new();
    let mut unproven = None;
    let mut answered = false;
    let mut over_cap = None;
    for url in &urls {
        // The failures are stored and served by `/ipfs/gave-up`, and a gateway may carry a key.
        let shown = crate::rpc::redact_url(url);
        let body = match client.get(url).send().await {
            Ok(resp) if resp.status().is_success() => match read_capped(resp).await {
                // A CID can name an empty (or whitespace-only) file. Let the proof below decide;
                // rejecting it here makes the resolver retry immutable bytes for half an hour.
                Ok(body) if !body.trim().is_empty() || expect.is_some() => body,
                Ok(_) => {
                    failures.push(format!("{shown}: empty body"));
                    continue;
                }
                Err(e) => {
                    if e.is::<crate::cid::OverCap>() {
                        over_cap = Some(e.to_string());
                    }
                    failures.push(format!("{shown}: {e}"));
                    continue;
                }
            },
            Ok(resp) => {
                failures.push(format!("{shown}: HTTP {}", resp.status()));
                continue;
            }
            Err(e) => {
                failures.push(format!("{shown}: {}", e.without_url()));
                continue;
            }
        };

        // RFC-0037 slice 1. Until this existed, a gateway answering HTTP 200 with *anything* was
        // believed - and gateways really do answer 200 with prose like "Unable to retrieve content
        // within timeout period", which would have been vendored into the nest as its manifest.
        let Some(cid) = expect.as_ref() else {
            return Ok(Fetched {
                body,
                proof: Proof::Unproven("an operator's URL names a location, not a content".into()),
            });
        };
        answered = true;
        match crate::cid::verify(cid, body.as_bytes()) {
            Ok(()) => {
                return Ok(Fetched {
                    body,
                    proof: Proof::Verified,
                })
            }
            Err(e) if e.is::<crate::cid::Unprovable>() => {
                failures.push(format!("{shown}: {e}"));
                unproven.get_or_insert((body, e.to_string()));
            }
            Err(e) => failures.push(format!("{shown}: {e}")),
        }
    }

    // Only a gateway that answered is worth asking for blocks: one that failed outright has no CAR.
    if let (Some(cid), Some(name), true) = (expect.as_ref(), named.as_deref(), answered) {
        for url in gateways.iter().filter_map(|g| car_url(g, name)) {
            match fetch_car(&client, &url, cid).await {
                Ok(body) => {
                    return Ok(Fetched {
                        body,
                        proof: Proof::Verified,
                    })
                }
                Err(e) => {
                    if e.is::<crate::cid::OverCap>() {
                        over_cap = Some(e.to_string());
                    }
                    failures.push(format!("{}: {e:#}", crate::rpc::redact_url(&url)));
                }
            }
        }
    }
    if let Some(why) = over_cap {
        return Err(crate::cid::OverCap(format!("'{source}' refused: {why}")).into());
    }
    if let Some((body, why)) = unproven {
        return Ok(Fetched {
            body,
            proof: Proof::Unproven(why),
        });
    }
    let failures = failures.join("\n  ");
    if answered {
        return Err(NothingProved(format!(
            "nothing proved '{source}' from any gateway that answered:\n  {failures}"
        ))
        .into());
    }
    Err(anyhow!(
        "could not fetch '{source}' from any gateway:\n  {failures}"
    ))
}

/// The trustless CAR form of a path gateway (`https://host/ipfs/`), per the IPFS trustless gateway
/// spec. A Kubo RPC prefix (`…/api/v0/cat?arg=`) has no such form and is skipped.
fn car_url(gateway: &str, cid: &str) -> Option<String> {
    (!gateway.contains('?')).then(|| format!("{gateway}{cid}?format=car"))
}

/// A CAR's worth of room over the file cap, for the block framing around the bytes.
const MAX_CAR_BYTES: usize = MAX_FETCH_BYTES + MAX_FETCH_BYTES / 16;

async fn fetch_car(client: &reqwest::Client, url: &str, cid: &crate::cid::Cid) -> Result<String> {
    let resp = client
        .get(url)
        .header(reqwest::header::ACCEPT, "application/vnd.ipld.car")
        .send()
        .await
        .map_err(|e| e.without_url())?;
    if !resp.status().is_success() {
        bail!("HTTP {}", resp.status());
    }
    // The Graph's path gateway ignores `format=car` and sends the file, which proves nothing.
    let is_car = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("application/vnd.ipld.car"));
    if !is_car {
        bail!("answered without a CAR");
    }
    let car = read_capped_bytes(resp, MAX_CAR_BYTES).await?;
    let content = crate::cid::content_from_car(cid, &car, &crate::cid::CAPS)?;
    String::from_utf8(content).context("the verified document is not UTF-8")
}

/// The CID a source names, if it names one. An `http(s)` URL does not.
fn cid_of(source: &str) -> Option<String> {
    let s = source.trim();
    if s.starts_with("http://") || s.starts_with("https://") {
        return None;
    }
    let cid = s
        .strip_prefix("ipfs://")
        .or_else(|| s.strip_prefix("/ipfs/"))
        .unwrap_or(s)
        .trim_matches('/');
    is_cid_shaped(cid).then(|| cid.to_string())
}

/// The most we will hold in memory for one manifest or ABI.
///
/// A gateway is a third party, and the reply size is its choice rather than ours. Streaming with a
/// ceiling means a hostile or broken one costs a bounded amount of RAM instead of whatever it feels
/// like sending - the same reasoning as the `/sql` result cap, applied to the one place `init`
/// trusts someone else's server. Real manifests are kilobytes; the largest ABI we have seen is far
/// under a megabyte.
const MAX_FETCH_BYTES: usize = 16 * 1024 * 1024;

/// Read a response body, refusing past [`MAX_FETCH_BYTES`] rather than buffering whatever arrives.
async fn read_capped(resp: reqwest::Response) -> Result<String> {
    String::from_utf8(read_capped_bytes(resp, MAX_FETCH_BYTES).await?)
        .context("response is not UTF-8")
}

async fn read_capped_bytes(resp: reqwest::Response, cap: usize) -> Result<Vec<u8>> {
    use futures::StreamExt;
    let mut out: Vec<u8> = Vec::new();
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk
            .map_err(|e| e.without_url())
            .context("reading response body")?;
        if out.len() + chunk.len() > cap {
            return Err(crate::cid::OverCap(format!(
                "response exceeds {} MiB - refusing to buffer it",
                cap / 1024 / 1024
            ))
            .into());
        }
        out.extend_from_slice(&chunk);
    }
    Ok(out)
}

// ── Parsing ──────────────────────────────────────────────────────────────

fn as_str(y: &Yaml) -> Option<String> {
    match y {
        Yaml::String(s) => Some(s.clone()),
        Yaml::Integer(i) => Some(i.to_string()),
        Yaml::Real(r) => Some(r.clone()),
        _ => None,
    }
}

fn get<'a>(y: &'a Yaml, key: &str) -> Option<&'a Yaml> {
    y.as_hash()
        .and_then(|h| h.get(&Yaml::String(key.to_string())))
}

fn get_str(y: &Yaml, key: &str) -> Option<String> {
    get(y, key).and_then(as_str)
}

/// A manifest link is `{ "/": "/ipfs/Qm…" }`. Return the bare CID.
fn link_cid(y: &Yaml) -> Option<String> {
    let raw = get(y, "/").and_then(as_str)?;
    Some(
        raw.trim_start_matches("/ipfs/")
            .trim_matches('/')
            .to_string(),
    )
}

/// A `mapping.abis[].file`, in either of the two forms a manifest may carry (#1321).
///
/// Before this, the plain-string form returned `None` and the surrounding `filter_map` **dropped the
/// entry entirely** - so a repository manifest did not merely fail to import, it imported with its
/// ABIs silently missing. Returning the path instead makes the two forms two cases rather than one
/// case and one hole.
fn abi_link(y: &Yaml) -> Option<AbiLink> {
    if let Some(cid) = link_cid(y) {
        return Some(AbiLink::Cid(cid));
    }
    let path = as_str(y)?;
    (!path.trim().is_empty()).then(|| AbiLink::Path(path.trim().to_string()))
}

/// Make a manifest-supplied string safe to print, and bounded.
///
/// The import's report is the deliverable as much as the config is - an operator reads it to
/// decide which warnings to act on. Names come from the manifest, so a `\r` can erase the line
/// above and a `\x1b[` sequence can repaint it: a source that was skipped can be made to look
/// like one that was imported. Controls become their escape form, and the length is capped so a
/// single name cannot scroll the rest of the report off the screen.
fn sanitise(s: &str) -> String {
    const MAX: usize = 120;
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c.is_control() {
            out.push_str(&format!("\\u{{{:x}}}", c as u32));
        } else {
            out.push(c);
        }
        if out.len() >= MAX {
            out.push('…');
            break;
        }
    }
    out
}

/// Whether `mapping.<key>` lists at least one handler.
fn declares(mapping: Option<&Yaml>, key: &str) -> bool {
    mapping
        .and_then(|m| get(m, key))
        .and_then(|h| h.as_vec())
        .is_some_and(|v| !v.is_empty())
}

fn parse_source(y: &Yaml) -> ManifestSource {
    let source = get(y, "source");
    let mapping = get(y, "mapping");

    let abis = mapping
        .and_then(|m| get(m, "abis"))
        .and_then(|a| a.as_vec())
        .map(|entries| {
            entries
                .iter()
                .filter_map(|e| {
                    Some(AbiRef {
                        name: sanitise(&get_str(e, "name")?),
                        link: get(e, "file").and_then(abi_link)?,
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    let events = mapping
        .and_then(|m| get(m, "eventHandlers"))
        .and_then(|h| h.as_vec())
        .map(|entries| {
            entries
                .iter()
                .filter_map(|e| get_str(e, "event"))
                // Folded scalars (`event: >-`) arrive with the newline already
                // resolved to a space; collapse the rest so the signature
                // matches the ABI-derived one character for character.
                .map(|s| s.split_whitespace().collect::<Vec<_>>().join(""))
                .collect()
        })
        .unwrap_or_default();

    ManifestSource {
        // Sanitised at the boundary, so no unescaped copy exists downstream to be printed.
        kind: sanitise(&get_str(y, "kind").unwrap_or_default()),
        name: sanitise(&get_str(y, "name").unwrap_or_default()),
        network: get_str(y, "network"),
        address: source.and_then(|s| get_str(s, "address")),
        start_block: source
            .and_then(|s| get(s, "startBlock"))
            .and_then(|b| match b {
                Yaml::Integer(i) if *i >= 0 => Some(*i as u64),
                other => as_str(other).and_then(|s| s.parse().ok()),
            }),
        abi_name: source.and_then(|s| get_str(s, "abi")),
        abis,
        end_block: source
            .and_then(|s| get(s, "endBlock"))
            .and_then(|b| match b {
                Yaml::Integer(i) if *i >= 0 => Some(*i as u64),
                other => as_str(other).and_then(|s| s.parse().ok()),
            }),
        has_block_handlers: declares(mapping, "blockHandlers"),
        has_call_handlers: declares(mapping, "callHandlers"),
        events,
    }
}

pub fn parse_manifest(text: &str) -> Result<Manifest> {
    let docs = YamlLoader::load_from_str(text).context("subgraph manifest is not valid YAML")?;
    let doc = docs
        .first()
        .ok_or_else(|| anyhow!("subgraph manifest is empty"))?;

    let collect = |key: &str| -> Vec<ManifestSource> {
        get(doc, key)
            .and_then(|v| v.as_vec())
            .map(|v| v.iter().map(parse_source).collect())
            .unwrap_or_default()
    };

    let manifest = Manifest {
        spec_version: get_str(doc, "specVersion"),
        description: get_str(doc, "description"),
        data_sources: collect("dataSources"),
        templates: collect("templates"),
    };

    if manifest.data_sources.is_empty() && manifest.templates.is_empty() {
        bail!("manifest declares neither dataSources nor templates - is this a subgraph manifest?");
    }
    let declared = manifest.data_sources.len() + manifest.templates.len();
    if declared > MAX_SOURCES {
        bail!(
            "manifest declares {declared} sources, more than the {MAX_SOURCES} this import will \
             follow. Each one costs a gateway fetch and a file on disk, and the manifest chooses \
             how many there are - a hostile one would have your machine make the requests. If a \
             real subgraph is this large, import it in pieces with `nuthatch init` per contract."
        );
    }
    Ok(manifest)
}

// ── Aliases ──────────────────────────────────────────────────────────────

/// `GovernanceFactory` → `governance_factory`, within nuthatch's
/// `[a-z][a-z0-9_]*` alias grammar. Collisions are resolved by the caller.
pub fn to_alias(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 4);
    let chars: Vec<char> = name.chars().collect();
    for (i, &c) in chars.iter().enumerate() {
        if c.is_ascii_uppercase() {
            let prev_lower =
                i > 0 && (chars[i - 1].is_ascii_lowercase() || chars[i - 1].is_ascii_digit());
            let next_lower = chars.get(i + 1).is_some_and(|n| n.is_ascii_lowercase());
            if i > 0 && (prev_lower || next_lower) && !out.ends_with('_') {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
        } else if c.is_ascii_alphanumeric() {
            out.push(c);
        } else if !out.ends_with('_') && !out.is_empty() {
            out.push('_');
        }
    }
    let out = out.trim_matches('_').to_string();
    // Must start with a letter.
    match out.chars().next() {
        Some(c) if c.is_ascii_lowercase() => out,
        Some(_) => format!("c_{out}"),
        None => "c0".to_string(),
    }
}

/// Make `alias` unique against `taken`, appending `_2`, `_3`, … as needed.
///
/// Every name this hands out is recorded, not just the base it was derived from. Counting only
/// bases is not enough, because the suffix form is itself a name a manifest can ask for:
/// `Pool`, `Pool`, `Pool 2` aliases to `pool`, `pool_2`, and then — with a base-only counter —
/// `pool_2` again, because `pool_2` had never been counted. Two contracts would own
/// `abis/pool_2.json` and the second write would silently clobber the first, leaving a real
/// contract decoded against another contract's ABI. The manifest chooses these names, so the
/// collision is arranged, not stumbled into.
pub fn dedupe_alias(alias: &str, taken: &mut BTreeMap<String, u32>) -> String {
    let mut n = *taken
        .entry(alias.to_string())
        .and_modify(|n| *n += 1)
        .or_insert(1);
    let mut candidate = if n == 1 {
        alias.to_string()
    } else {
        format!("{alias}_{n}")
    };
    // Walk past anything already issued under a different base.
    while taken.contains_key(&candidate) && candidate != *alias {
        n += 1;
        candidate = format!("{alias}_{n}");
    }
    taken.insert(alias.to_string(), n);
    taken.entry(candidate.clone()).or_insert(1);
    candidate
}

// ── Factory inference ────────────────────────────────────────────────────

/// One template we could not wire to a creating event, with whatever
/// candidates we saw — so the user edits a shortlist rather than reads WASM.
#[derive(Debug, Clone, PartialEq)]
pub struct UnresolvedTemplate {
    pub template: String,
    pub candidates: Vec<String>,
}

/// A factory rule we are confident enough to write.
#[derive(Debug, Clone, PartialEq)]
pub struct InferredFactory {
    pub watch: String,
    pub event: String,
    pub child_param: String,
    pub template: String,
    /// Why we believe this, shown in the import report. An inference the user
    /// cannot audit is an inference they cannot correct.
    pub because: String,
}

/// An address-typed event parameter, resolved from a vendored ABI.
#[derive(Debug, Clone)]
pub struct AddressParam {
    pub watch_alias: String,
    pub event: String,
    pub param: String,
}

/// Pull every address-typed event parameter out of a vendored ABI.
pub fn address_params(watch_alias: &str, abi: &Value) -> Vec<AddressParam> {
    let mut out = Vec::new();
    let Some(items) = abi.as_array() else {
        return out;
    };
    for item in items {
        if item.get("type").and_then(Value::as_str) != Some("event") {
            continue;
        }
        let Some(event) = item.get("name").and_then(Value::as_str) else {
            continue;
        };
        let Some(inputs) = item.get("inputs").and_then(Value::as_array) else {
            continue;
        };
        for input in inputs {
            if input.get("type").and_then(Value::as_str) != Some("address") {
                continue;
            }
            let Some(param) = input.get("name").and_then(Value::as_str) else {
                continue;
            };
            if param.is_empty() {
                continue;
            }
            out.push(AddressParam {
                watch_alias: watch_alias.to_string(),
                event: event.to_string(),
                param: param.to_string(),
            });
        }
    }
    out
}

fn normalise(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

const CREATION_VERBS: &[&str] = &[
    "created",
    "deployed",
    "registered",
    "added",
    "new",
    "launched",
    "spawned",
    "initialised",
    "initialized",
    "cloned",
    "minted",
];

/// Score how strongly `param` on `event` looks like it announces `template`.
///
/// Deliberately conservative: an exact param↔template name match is the only
/// signal strong enough to stand alone. Everything else needs corroboration
/// from a creation-shaped event name, and anything ambiguous is reported
/// rather than guessed.
fn score(template: &str, p: &AddressParam) -> (u32, String) {
    let t = normalise(template);
    let param = normalise(&p.param);
    let event = normalise(&p.event);
    let creation_verb = CREATION_VERBS.iter().find(|v| event.contains(*v));

    if param == t {
        return (
            100,
            format!("param `{}` names the template exactly", p.param),
        );
    }
    if event.contains(&t) && creation_verb.is_some() {
        return (
            70,
            format!(
                "event `{}` names the template and reads as a creation event",
                p.event
            ),
        );
    }
    if (param.contains(&t) || t.contains(&param)) && creation_verb.is_some() {
        return (
            50,
            format!(
                "param `{}` overlaps the template name on a creation event `{}`",
                p.param, p.event
            ),
        );
    }
    (0, String::new())
}

/// Confidence floor for writing a rule unasked. Below this we report.
const MIN_SCORE: u32 = 50;

/// Match templates to creating events. Returns rules we stand behind, plus
/// every template we could not resolve with its candidate shortlist.
///
/// Two rules govern what gets written, and both exist because the alternative
/// is a config that looks authoritative and indexes the wrong thing:
///
/// - **A template with tied top candidates is not resolved.** Two parameters
///   that look equally like its creator make the choice a coin flip.
/// - **A parameter creates at most one template.** `passkeyAccountFactoryBeacon`
///   substring-matches both `PasskeyAccountFactory` and `PasskeyAccount`; at
///   most one of those is true, so the stronger claim takes it and the other
///   is reported with the param as a candidate.
pub fn infer_factories(
    templates: &[String],
    params: &[AddressParam],
) -> (Vec<InferredFactory>, Vec<UnresolvedTemplate>) {
    // Pass 1: per-template shortlist, with the tie rule applied.
    struct Claim<'a> {
        template: &'a String,
        score: u32,
        /// Length of the normalised template name. Breaks score ties between
        /// templates whose names nest inside one another: against
        /// `passkeyAccountFactoryBeacon`, `PasskeyAccountFactory` is a longer
        /// and therefore more specific match than `PasskeyAccount`.
        specificity: usize,
        why: String,
        param: &'a AddressParam,
        shortlist: Vec<String>,
    }
    let mut claims: Vec<Claim> = Vec::new();
    let mut unresolved: Vec<UnresolvedTemplate> = Vec::new();

    for template in templates {
        let mut scored: Vec<(u32, String, &AddressParam)> = params
            .iter()
            .map(|p| {
                let (s, why) = score(template, p);
                (s, why, p)
            })
            .filter(|(s, _, _)| *s >= MIN_SCORE)
            .collect();
        scored.sort_by_key(|s| std::cmp::Reverse(s.0));
        let shortlist: Vec<String> = scored
            .iter()
            .take(4)
            .map(|(_, _, p)| format!("{}.{} → {}", p.watch_alias, p.event, p.param))
            .collect();

        let top_is_unique = match scored.as_slice() {
            [] => false,
            [_] => true,
            [first, second, ..] => first.0 > second.0,
        };
        if top_is_unique {
            let (s, why, p) = &scored[0];
            claims.push(Claim {
                template,
                score: *s,
                specificity: normalise(template).len(),
                why: why.clone(),
                param: p,
                shortlist,
            });
        } else {
            unresolved.push(UnresolvedTemplate {
                template: template.clone(),
                candidates: shortlist,
            });
        }
    }

    // Pass 2: resolve contention over the same parameter. Strongest claim wins;
    // an equal-scored rival means neither is defensible, so both are reported.
    let mut rules = Vec::new();
    for (i, claim) in claims.iter().enumerate() {
        let key = (
            &claim.param.watch_alias,
            &claim.param.event,
            &claim.param.param,
        );
        let mine = (claim.score, claim.specificity);
        let rival = claims.iter().enumerate().find(|(j, other)| {
            *j != i
                && (
                    &other.param.watch_alias,
                    &other.param.event,
                    &other.param.param,
                ) == key
                && (other.score, other.specificity) >= mine
        });
        match rival {
            None => rules.push(InferredFactory {
                watch: claim.param.watch_alias.clone(),
                event: claim.param.event.clone(),
                child_param: claim.param.param.clone(),
                template: claim.template.clone(),
                because: claim.why.clone(),
            }),
            Some((_, winner)) => unresolved.push(UnresolvedTemplate {
                template: claim.template.clone(),
                candidates: if claim.shortlist.is_empty() {
                    Vec::new()
                } else {
                    let mut c = claim.shortlist.clone();
                    c.push(format!(
                        "(`{}` was claimed by template `{}` on a stronger match)",
                        claim.param.param, winner.template
                    ));
                    c
                },
            }),
        }
    }
    (rules, unresolved)
}

/// Event *name* from a manifest signature: `Transfer(indexed address,…)` → `Transfer`.
pub fn event_name(signature: &str) -> &str {
    signature.split('(').next().unwrap_or(signature).trim()
}

#[cfg(test)]
mod tests {
    // ---------------------------------------------------------------------------------------
    // #1321: a repository manifest read from disk, with `file:` ABI references.
    // ---------------------------------------------------------------------------------------

    /// A subgraph repository: `subgraph.yaml` beside an `abis/` directory, which is the shape a
    /// builder has *before* they deploy - and exactly when a nest is most useful to them.
    #[cfg(test)]
    fn repo_fixture() -> tempfile::TempDir {
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(d.path().join("abis")).unwrap();
        std::fs::write(
            d.path().join("abis/Token.json"),
            r#"[{"type":"event","name":"T"}]"#,
        )
        .unwrap();
        std::fs::write(
            d.path().join("subgraph.yaml"),
            "dataSources:\n  - kind: ethereum/contract\n    name: Token\n    network: mainnet\n\
             \n    source:\n      address: '0x0000000000000000000000000000000000000001'\n\
             \n      abi: Token\n    mapping:\n      abis:\n        - name: Token\n\
             \n          file: ./abis/Token.json\n",
        )
        .unwrap();
        d
    }

    /// The plain-string `file:` form parses as a path rather than vanishing.
    ///
    /// It used to return `None` from `link_cid` and be dropped by the surrounding `filter_map`, so a
    /// repository manifest did not fail to import - it imported with its ABIs silently absent, which
    /// is the worse of the two.
    #[test]
    fn a_repository_manifest_keeps_its_file_abi_references() {
        let d = repo_fixture();
        let text = std::fs::read_to_string(d.path().join("subgraph.yaml")).unwrap();
        let man = parse_manifest(&text).unwrap();
        assert_eq!(
            man.data_sources[0].abis,
            vec![AbiRef {
                name: "Token".into(),
                link: AbiLink::Path("./abis/Token.json".into()),
            }],
            "the file: entry was dropped instead of parsed"
        );
    }

    /// Both forms still parse, so adding the second did not cost the first.
    #[test]
    fn a_deployed_manifest_still_pins_by_cid() {
        let man = parse_manifest(
            "dataSources:\n  - kind: ethereum/contract\n    name: T\n    network: mainnet\n\
             \n    source:\n      address: '0x01'\n    mapping:\n      abis:\n\
             \n        - name: T\n          file:\n            /: /ipfs/QmAbc123\n",
        )
        .unwrap();
        assert_eq!(
            man.data_sources[0].abis[0].link,
            AbiLink::Cid("QmAbc123".into())
        );
    }

    #[tokio::test]
    async fn a_manifest_that_exists_on_disk_is_read_from_disk() {
        let d = repo_fixture();
        let path = d.path().join("subgraph.yaml");
        let (text, home) = read_manifest(path.to_str().unwrap(), &[]).await.unwrap();
        assert!(text.contains("dataSources"));
        match home {
            ManifestHome::Local(dir) => assert_eq!(dir, d.path().canonicalize().unwrap()),
            ManifestHome::Remote => panic!("a file that exists was fetched instead of read"),
        }
        // And it resolves its own ABI.
        let got = resolve_abi_path(
            &ManifestHome::Local(d.path().canonicalize().unwrap()),
            "./abis/Token.json",
        )
        .unwrap();
        assert!(got.ends_with("abis/Token.json"), "{}", got.display());
    }

    /// **A fetched manifest may not name a local file.** Same rule as `Origin::Manifest` refusing a
    /// URL, pointed the other way: honouring it would let whoever published the CID choose what this
    /// process reads off the operator's machine.
    #[test]
    fn a_remote_manifest_cannot_reference_a_local_file() {
        let err = resolve_abi_path(&ManifestHome::Remote, "./abis/Token.json")
            .expect_err("a fetched manifest has no directory to resolve against");
        let msg = err.to_string();
        assert!(msg.contains("fetched rather than read from disk"), "{msg}");
    }

    /// And a local manifest may name its own files and no others. `..` is the obvious shape; a
    /// symlink out of the tree is the one that is not, which is why both sides are canonicalised
    /// rather than the string being inspected for dots.
    #[test]
    fn a_local_manifest_cannot_reach_outside_its_own_directory() {
        let outer = tempfile::tempdir().unwrap();
        std::fs::write(outer.path().join("secret.json"), "[]").unwrap();
        let inner = outer.path().join("repo");
        std::fs::create_dir_all(inner.join("abis")).unwrap();
        std::fs::write(inner.join("abis/Token.json"), "[]").unwrap();
        let home = ManifestHome::Local(inner.canonicalize().unwrap());

        let err =
            resolve_abi_path(&home, "../secret.json").expect_err("a traversal must not resolve");
        assert!(
            err.to_string()
                .contains("outside the manifest's own directory"),
            "{err}"
        );

        // And the legitimate one still works, or the guard is a wall.
        assert!(resolve_abi_path(&home, "./abis/Token.json").is_ok());
    }

    /// A home reached through a symlink, as every macOS tempdir is, still resolves its own files. The
    /// comparison must not depend on the caller having canonicalised the home.
    #[cfg(unix)]
    #[test]
    fn a_home_reached_through_a_symlink_still_resolves_its_own_files() {
        let outer = tempfile::tempdir().unwrap();
        let real = outer.path().join("real");
        std::fs::create_dir_all(real.join("abis")).unwrap();
        std::fs::write(real.join("abis/Token.json"), "[]").unwrap();
        let link = outer.path().join("link");
        std::os::unix::fs::symlink(&real, &link).unwrap();

        let got = resolve_abi_path(&ManifestHome::Local(link), "./abis/Token.json")
            .expect("a file inside the manifest's own directory must resolve");
        assert!(got.ends_with("real/abis/Token.json"), "{}", got.display());
    }

    /// The escape a `..` check does not see, in its own test rather than after the `..` assertion.
    ///
    /// Kept separate deliberately: run together, a mutation removing the canonicalisation and one
    /// removing the containment check both die on the **first** assertion, so one test would prove
    /// one thing twice and the symlink case would never be exercised at all.
    #[cfg(unix)]
    #[test]
    fn a_symlink_out_of_the_tree_is_not_a_way_around_the_guard() {
        let outer = tempfile::tempdir().unwrap();
        std::fs::write(outer.path().join("secret.json"), "[]").unwrap();
        let inner = outer.path().join("repo");
        std::fs::create_dir_all(inner.join("abis")).unwrap();
        let home = ManifestHome::Local(inner.canonicalize().unwrap());

        std::os::unix::fs::symlink(
            outer.path().join("secret.json"),
            inner.join("abis/escape.json"),
        )
        .unwrap();
        // No `..` anywhere, every component inside the tree. Only resolving the link tells them
        // apart, which is why the path is canonicalised rather than inspected.
        let err = resolve_abi_path(&home, "./abis/escape.json")
            .expect_err("a symlink out of the tree must not resolve");
        assert!(
            err.to_string()
                .contains("outside the manifest's own directory"),
            "{err}"
        );
    }

    /// A path that is not there falls through to the gateways and gets the old error, which names
    /// the real problem - a mistyped path is not a CID either.
    #[tokio::test]
    async fn a_path_that_does_not_exist_is_not_silently_treated_as_a_cid() {
        let err = read_manifest(
            "./no/such/subgraph.yaml",
            &["https://example.invalid/".into()],
        )
        .await
        .expect_err("neither a file nor a CID");
        assert!(err.to_string().contains("not a CID or URL"), "{err}");
    }

    use super::*;

    /// A gateway serving `file` for a plain request and `car` for `?format=car`. Without a CAR it
    /// answers `?format=car` with the file under a JSON type, which is what The Graph's does.
    async fn trustless_gateway(
        file: Vec<u8>,
        car: Option<Vec<u8>>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        use axum::{http::header, response::IntoResponse, routing::get, Router};
        let app = Router::new().route(
            "/ipfs/{cid}",
            get(move |uri: axum::http::Uri| {
                let (file, car) = (file.clone(), car.clone());
                async move {
                    match (uri.query(), car) {
                        (Some("format=car"), Some(car)) => (
                            [(header::CONTENT_TYPE, "application/vnd.ipld.car; version=1")],
                            car,
                        )
                            .into_response(),
                        _ => ([(header::CONTENT_TYPE, "application/json")], file).into_response(),
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{addr}/ipfs/"), handle)
    }

    fn document(n: usize) -> Vec<u8> {
        (0..n).map(|i| b'a' + (i * 7 % 26) as u8).collect()
    }

    const LARGE: usize = 600 * 1024;

    #[tokio::test]
    async fn an_empty_document_is_proven_when_its_cid_names_empty_bytes() {
        let (gateway, handle) = trustless_gateway(Vec::new(), None).await;
        let got = fetch_ipfs_proven(
            "QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH",
            &[gateway],
            Origin::Manifest,
        )
        .await
        .unwrap();
        assert_eq!(got.proof, Proof::Verified);
        assert_eq!(got.body, "");
        handle.abort();
    }

    #[tokio::test]
    async fn a_whitespace_document_is_proven_without_trimming_its_bytes() {
        let file = b" \n\t";
        let cid = crate::cid::cid_v0_for(file);
        let (gateway, handle) = trustless_gateway(file.to_vec(), None).await;
        let got = fetch_ipfs_proven(&cid, &[gateway], Origin::Manifest)
            .await
            .unwrap();
        assert_eq!(got.proof, Proof::Verified);
        assert_eq!(got.body.as_bytes(), file);
        handle.abort();
    }

    #[tokio::test]
    async fn an_empty_gateway_response_cannot_replace_a_nonempty_document() {
        let file = b"[1,2,3]";
        let cid = crate::cid::cid_v0_for(file);
        let (empty, empty_handle) = trustless_gateway(Vec::new(), None).await;
        assert!(
            fetch_ipfs_proven(&cid, std::slice::from_ref(&empty), Origin::Manifest)
                .await
                .is_err()
        );
        let (good, good_handle) = trustless_gateway(file.to_vec(), None).await;
        let got = fetch_ipfs_proven(&cid, &[empty, good], Origin::Manifest)
            .await
            .unwrap();
        assert_eq!(got.proof, Proof::Verified);
        assert_eq!(got.body.as_bytes(), file);
        empty_handle.abort();
        good_handle.abort();
    }

    /// The failure text is stored and served by `/ipfs/gave-up`, so a keyed gateway must not reach
    /// it, whether the key is userinfo or in the path, and whether the gateway answered or not (#2207).
    #[tokio::test]
    async fn a_gateway_failure_names_the_host_and_never_its_key() {
        let cid = crate::cid::cid_v0_for(b"[1,2,3]");
        let (answering, handle) = trustless_gateway(Vec::new(), None).await;
        let keyed = answering.replacen("http://", "http://PROJECT:SECRET1@", 1);
        let refused = "http://PROJECT:SECRET2@127.0.0.1:1/SECRET3/ipfs/".to_string();
        let err = fetch_ipfs_proven(&cid, &[keyed, refused], Origin::Manifest)
            .await
            .err()
            .expect("an empty answer proves nothing and the other gateway is closed");
        handle.abort();
        let text = format!("{err:#}");
        for secret in ["SECRET1", "SECRET2", "SECRET3"] {
            assert!(!text.contains(secret), "{secret} leaked: {text}");
        }
        assert!(
            text.contains("http://127.0.0.1:1"),
            "the host still names the gateway: {text}"
        );
    }

    #[tokio::test]
    async fn an_empty_operator_url_still_has_no_document_or_proof() {
        let (gateway, handle) = trustless_gateway(Vec::new(), None).await;
        let err = fetch_ipfs_proven(&format!("{gateway}manifest"), &[], Origin::Operator)
            .await
            .err()
            .expect("an unaddressed empty response is still refused");
        assert!(err.to_string().contains("empty body"), "{err:#}");
        handle.abort();
    }

    #[tokio::test]
    async fn a_large_document_in_kubos_default_layout_is_proven_from_its_file_bytes() {
        let file = document(LARGE);
        let (cid, _) = crate::cid::dag_for_tests(&file, crate::cid::DEFAULT_CHUNK);
        let (gateway, handle) = trustless_gateway(file.clone(), None).await;
        let got = fetch_ipfs_proven(&cid, &[gateway], Origin::Manifest)
            .await
            .unwrap();
        assert_eq!(got.proof, Proof::Verified);
        assert_eq!(got.body.as_bytes(), file);
        handle.abort();
    }

    #[tokio::test]
    async fn a_document_in_another_layout_is_proven_from_its_blocks() {
        let file = document(LARGE);
        let (cid, car) = crate::cid::dag_for_tests(&file, 100_000);
        let (gateway, handle) = trustless_gateway(file.clone(), Some(car)).await;
        let got = fetch_ipfs_proven(&cid, &[gateway], Origin::Manifest)
            .await
            .unwrap();
        assert_eq!(
            got.proof,
            Proof::Verified,
            "the blocks prove what re-encoding cannot"
        );
        assert_eq!(got.body.as_bytes(), file);
        handle.abort();
    }

    /// Under one default chunk a body can still be a multi-block file cut smaller: 200 KiB in 100 KiB
    /// leaves re-encodes to nothing, and only its blocks prove it (#1373 review).
    #[tokio::test]
    async fn a_small_document_in_another_layout_is_proven_from_its_blocks() {
        let file = document(200 * 1024);
        let (cid, car) = crate::cid::dag_for_tests(&file, 100 * 1024);
        let (gateway, handle) = trustless_gateway(file.clone(), Some(car)).await;
        let got = fetch_ipfs_proven(&cid, &[gateway], Origin::Manifest)
            .await
            .unwrap();
        assert_eq!(
            got.proof,
            Proof::Verified,
            "a small file cut into smaller leaves is proven by its blocks"
        );
        assert_eq!(got.body.as_bytes(), file);
        handle.abort();
    }

    /// The same small file with no blocks to prove it is refused, not accepted unproven: under one
    /// default chunk a body that does not re-encode is far likelier a gateway's error page than a file
    /// cut small, and `init` vendors whatever this accepts.
    #[tokio::test]
    async fn a_small_document_that_neither_re_encodes_nor_has_blocks_is_refused() {
        let file = document(200 * 1024);
        let (cid, _) = crate::cid::dag_for_tests(&file, 100 * 1024);
        let (gateway, handle) = trustless_gateway(file, None).await;
        let gateways = [gateway];
        let refused = fetch_ipfs_proven(&cid, &gateways, Origin::Manifest).await;
        assert!(
            matches!(&refused, Err(e) if e.is::<NothingProved>()),
            "a small body proven by nothing must be refused as unproven, not as unfetched"
        );
        assert!(fetch_ipfs(&cid, &gateways, Origin::Manifest).await.is_err());
        handle.abort();
    }

    /// A gateway that answers `?format=car` with the file has proven nothing: the document comes back
    /// unproven, and `init`'s `fetch_ipfs` still accepts it loudly.
    #[tokio::test]
    async fn a_gateway_without_a_car_leaves_another_layout_unproven() {
        let file = document(LARGE);
        let (cid, _) = crate::cid::dag_for_tests(&file, 100_000);
        let (gateway, handle) = trustless_gateway(file.clone(), None).await;
        let gateways = [gateway];
        let got = fetch_ipfs_proven(&cid, &gateways, Origin::Manifest)
            .await
            .unwrap();
        assert!(
            matches!(&got.proof, Proof::Unproven(why) if why.contains("UNVERIFIED")),
            "{:?}",
            got.proof
        );
        let accepted = fetch_ipfs(&cid, &gateways, Origin::Manifest).await.unwrap();
        assert_eq!(accepted.as_bytes(), file);
        handle.abort();
    }

    #[tokio::test]
    async fn a_car_with_a_tampered_block_is_not_proof() {
        let file = document(LARGE);
        let (cid, mut car) = crate::cid::dag_for_tests(&file, 100_000);
        let last = car.len() - 1;
        car[last] ^= 1;
        let (gateway, handle) = trustless_gateway(file, Some(car)).await;
        let got = fetch_ipfs_proven(&cid, &[gateway], Origin::Manifest)
            .await
            .unwrap();
        assert!(matches!(got.proof, Proof::Unproven(_)), "{:?}", got.proof);
        handle.abort();
    }

    #[tokio::test]
    async fn a_document_over_the_cap_is_refused_as_over_cap() {
        let cid = crate::cid::cid_v0_for(b"anything");
        let (gateway, handle) = trustless_gateway(document(MAX_FETCH_BYTES + 1), None).await;
        let err = fetch_ipfs_proven(&cid, &[gateway], Origin::Manifest)
            .await
            .err()
            .expect("an oversize body must be refused");
        assert!(err.is::<crate::cid::OverCap>(), "{err:#}");
        handle.abort();
    }

    const MANIFEST: &str = r#"
specVersion: 0.0.9
description: test fixture
dataSources:
  - kind: ethereum
    name: GovernanceFactory
    network: arbitrum-one
    source:
      abi: GovernanceFactory
      address: '0x24Fd3b269905AF10A6E5c67D93F0502Cd11Af875'
      startBlock: 447059876
    mapping:
      abis:
        - file:
            /: /ipfs/Qmco6j6G3fpC1VVoBFFYjTY6hvJxUxUrtaqgFCftA6RW4s
          name: GovernanceFactory
      eventHandlers:
        - event: >-
            ModuleDeployed(indexed bytes32,indexed
            bytes32,address,address,bool,address)
          handler: handleModuleDeployed
  - kind: file/ipfs
    name: OrgMetadata
    network: arbitrum-one
    mapping:
      abis: []
templates:
  - kind: ethereum/contract
    name: HybridVoting
    network: arbitrum-one
    source:
      abi: HybridVoting
    mapping:
      abis:
        - file:
            /: /ipfs/QmHybridVotingAbiCid000000000000000000000000
          name: HybridVoting
      eventHandlers:
        - event: VoteCast(indexed address,indexed uint256,uint8)
          handler: handleVoteCast
"#;

    #[test]
    fn parses_data_sources_templates_and_links() {
        let m = parse_manifest(MANIFEST).unwrap();
        assert_eq!(m.spec_version.as_deref(), Some("0.0.9"));
        assert_eq!(m.data_sources.len(), 2);
        assert_eq!(m.templates.len(), 1);

        // A template's `eventHandlers` say which of its ABI's events the subgraph actually decoded,
        // and the import carries them into `[[templates]].events`. Asserted here because the parse
        // is the *source* of that allowlist: the import discarded it for as long as this data was
        // parsed and unread, and an empty list here would silently restore the superset decode.
        assert_eq!(
            m.templates[0].events,
            vec!["VoteCast(indexedaddress,indexeduint256,uint8)"],
            "a template's event handlers must survive the parse"
        );
        assert_eq!(event_name(&m.templates[0].events[0]), "VoteCast");

        let ds = &m.data_sources[0];
        assert_eq!(ds.name, "GovernanceFactory");
        assert_eq!(
            ds.address.as_deref(),
            Some("0x24Fd3b269905AF10A6E5c67D93F0502Cd11Af875")
        );
        assert_eq!(ds.start_block, Some(447_059_876));
        assert_eq!(
            ds.own_abi().unwrap().link.as_str(),
            "Qmco6j6G3fpC1VVoBFFYjTY6hvJxUxUrtaqgFCftA6RW4s"
        );
    }

    #[test]
    fn folded_event_signatures_are_collapsed() {
        // `event: >-` wraps long signatures across lines. If we keep the
        // newline the signature never matches the ABI and every handler looks
        // unknown, which is a silent, total failure.
        let m = parse_manifest(MANIFEST).unwrap();
        assert_eq!(
            m.data_sources[0].events[0],
            "ModuleDeployed(indexedbytes32,indexedbytes32,address,address,bool,address)"
        );
        assert_eq!(event_name(&m.data_sources[0].events[0]), "ModuleDeployed");
    }

    #[test]
    fn file_data_sources_are_not_evm() {
        let m = parse_manifest(MANIFEST).unwrap();
        assert!(m.data_sources[0].is_evm());
        assert!(!m.data_sources[1].is_evm());
    }

    #[test]
    fn network_is_unanimous_or_an_error() {
        let m = parse_manifest(MANIFEST).unwrap();
        assert_eq!(m.network().unwrap(), "arbitrum-one");

        let mixed = MANIFEST.replace(
            "network: arbitrum-one\n    source:\n      abi: HybridVoting",
            "network: mainnet\n    source:\n      abi: HybridVoting",
        );
        let m2 = parse_manifest(&mixed).unwrap();
        // Templates don't vote on the nest's chain — only dataSources do.
        assert_eq!(m2.network().unwrap(), "arbitrum-one");
    }

    #[test]
    fn rejects_non_manifests() {
        assert!(parse_manifest("just: a map").is_err());
        assert!(parse_manifest("").is_err());
    }

    #[test]
    fn aliases_follow_the_nest_grammar() {
        for (input, want) in [
            ("GovernanceFactory", "governance_factory"),
            ("PoaManagerSatellite", "poa_manager_satellite"),
            ("ERC20", "erc20"),
            // A digit before a capital is a word boundary — this is what keeps
            // `ERC20Token` from collapsing into `erc20token`.
            ("ERC20Token", "erc20_token"),
            ("Uniswap V3 Pool", "uniswap_v3_pool"),
            ("hybrid-voting", "hybrid_voting"),
            // Leading digit can't start an alias, so it gets the `c_` prefix.
            ("9Lives", "c_9_lives"),
        ] {
            let got = to_alias(input);
            assert_eq!(got, want, "alias for {input}");
            let mut chars = got.chars();
            assert!(matches!(chars.next(), Some(c) if c.is_ascii_lowercase()));
            assert!(chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'));
        }
    }

    #[test]
    fn duplicate_aliases_are_suffixed() {
        let mut taken = BTreeMap::new();
        assert_eq!(dedupe_alias("pool", &mut taken), "pool");
        assert_eq!(dedupe_alias("pool", &mut taken), "pool_2");
        assert_eq!(dedupe_alias("pool", &mut taken), "pool_3");
    }

    fn param(watch: &str, event: &str, p: &str) -> AddressParam {
        AddressParam {
            watch_alias: watch.into(),
            event: event.into(),
            param: p.into(),
        }
    }

    #[test]
    fn infers_a_rule_when_the_param_names_the_template() {
        let params = vec![param("factory", "PoolCreated", "pool")];
        let (rules, unresolved) = infer_factories(&["Pool".into()], &params);
        assert_eq!(unresolved.len(), 0);
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].child_param, "pool");
        assert_eq!(rules[0].watch, "factory");
        assert_eq!(rules[0].event, "PoolCreated");
    }

    #[test]
    fn reports_rather_than_guesses_when_two_params_tie() {
        // OrgDeployed announces ten modules; nothing in the manifest says which
        // one is the HybridVoting. A coin flip written into someone's config is
        // worse than a line telling them to choose.
        let params = vec![
            param("deployer", "OrgDeployed", "hybridVoting"),
            param("deployer", "OrgDeployed", "quickJoin"),
        ];
        let (rules, unresolved) = infer_factories(&["Executor".into()], &params);
        assert!(rules.is_empty());
        assert_eq!(unresolved.len(), 1);
        assert_eq!(unresolved[0].template, "Executor");
    }

    #[test]
    fn an_exact_param_match_wins_against_a_weaker_candidate() {
        let params = vec![
            param("deployer", "OrgDeployed", "hybridVoting"),
            param("deployer", "OrgDeployed", "somethingElse"),
        ];
        let (rules, _) = infer_factories(&["HybridVoting".into()], &params);
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].child_param, "hybridVoting");
    }

    #[test]
    fn one_param_cannot_create_two_templates() {
        // Caught running the real POA manifest: `passkeyAccountFactoryBeacon`
        // substring-matches both PasskeyAccountFactory and PasskeyAccount, and
        // both were being written as rules. At most one is true.
        let params = vec![param(
            "poa_manager",
            "InfrastructureDeployed",
            "passkeyAccountFactoryBeacon",
        )];
        let (rules, unresolved) = infer_factories(
            &["PasskeyAccountFactory".into(), "PasskeyAccount".into()],
            &params,
        );
        assert_eq!(rules.len(), 1, "exactly one template may claim the param");
        assert_eq!(unresolved.len(), 1);
        // The loser is told who took it, so the user can correct the choice.
        assert!(unresolved[0]
            .candidates
            .iter()
            .any(|c| c.contains("claimed by template")));
    }

    #[test]
    fn a_stronger_claim_beats_a_weaker_one_on_the_same_param() {
        // Exact name match (100) must outrank substring overlap (50).
        let params = vec![param("f", "PoolCreated", "pool")];
        let (rules, _) = infer_factories(&["PoolCreatedThing".into(), "Pool".into()], &params);
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].template, "Pool");
    }

    #[test]
    fn unrelated_addresses_never_become_rules() {
        // `to`/`from` on a Transfer are addresses, but nothing to do with a
        // template. Inventing a factory from them would index garbage.
        let params = vec![
            param("token", "Transfer", "from"),
            param("token", "Transfer", "to"),
        ];
        let (rules, unresolved) = infer_factories(&["Pool".into()], &params);
        assert!(rules.is_empty());
        assert_eq!(unresolved[0].candidates.len(), 0);
    }

    #[test]
    fn address_params_are_read_from_the_abi() {
        let abi: Value = serde_json::from_str(
            r#"[
              {"type":"event","name":"PoolCreated","inputs":[
                {"name":"token0","type":"address","indexed":true},
                {"name":"fee","type":"uint24","indexed":true},
                {"name":"pool","type":"address","indexed":false}]},
              {"type":"function","name":"owner","inputs":[]}
            ]"#,
        )
        .unwrap();
        let got = address_params("factory", &abi);
        assert_eq!(got.len(), 2);
        assert_eq!(got[1].param, "pool");
        assert_eq!(got[1].event, "PoolCreated");
    }

    /// **The one that matters.** A manifest is fetched from a public gateway, so its ABI links are
    /// attacker-controlled strings handed to an HTTP client. Before the `Origin` split they were
    /// passed through verbatim whenever they looked like a URL, which made
    /// `init --from-subgraph <hostile-cid>` fetch whatever the manifest's author chose - including
    /// `169.254.169.254`, where a cloud host serves instance credentials - and write the reply into
    /// `abis/`. Reproduced end to end from the manifest, not asserted against the helper in isolation.
    #[test]
    fn a_manifest_cannot_make_us_fetch_a_url() {
        let m = r#"
specVersion: 0.0.5
dataSources:
  - kind: ethereum/contract
    name: Evil
    network: mainnet
    source:
      address: "0x0000000000000000000000000000000000000001"
      startBlock: 1
    mapping:
      abis:
        - name: Evil
          file:
            /: "http://169.254.169.254/latest/meta-data/iam/security-credentials/"
      eventHandlers:
        - event: Transfer(address,address,uint256)
"#;
        let man = parse_manifest(m).unwrap();
        let link = man.data_sources[0].abis[0].link.as_str();
        let err = candidate_urls(link, &["https://gw/".into()], Origin::Manifest)
            .expect_err("a manifest link that is a URL must be refused");
        let err = format!("{err:#}");
        assert!(err.contains("169.254.169.254"), "name the target: {err}");
        assert!(
            err.contains("--from-subgraph"),
            "say which reference may be a URL: {err}"
        );
    }

    /// The operator's own argument is theirs to point anywhere - that is the whole difference.
    #[test]
    fn the_operator_may_still_pass_a_url() {
        let urls = candidate_urls("https://example.com/subgraph.yaml", &[], Origin::Operator)
            .expect("an operator-supplied URL is intentional");
        assert_eq!(urls, vec!["https://example.com/subgraph.yaml"]);
    }

    /// A CID is concatenated onto a gateway prefix, so anything that is not alphanumeric can reach
    /// outside the path the gateway meant to serve - `../`, a query, a second scheme.
    #[test]
    fn a_cid_that_is_not_cid_shaped_is_refused() {
        for bad in [
            "../../etc/passwd",
            "Qm..%2f..%2fadmin",
            "Qm?redirect=http://evil",
            "Qm#frag",
            "Qm x",
            "",
        ] {
            assert!(
                candidate_urls(bad, &["https://gw/".into()], Origin::Manifest).is_err(),
                "{bad:?} should not be accepted as a CID"
            );
        }
        assert!(candidate_urls(
            "QmVPhLwuv9zn2c761Ua7eJ",
            &["https://gw/".into()],
            Origin::Manifest
        )
        .is_ok());
    }

    /// The alias a manifest can arrange to collide with. `Pool`, `Pool`, `Pool 2` walks the
    /// base counter to `pool_2` and then asks for `pool_2` directly - which a base-only counter
    /// hands out a second time, putting two contracts on one `abis/pool_2.json`.
    #[test]
    fn a_manifest_cannot_arrange_two_sources_onto_one_alias() {
        let mut taken = BTreeMap::new();
        let issued: Vec<String> = ["Pool", "Pool", "Pool 2", "Pool_2", "Pool"]
            .iter()
            .map(|n| dedupe_alias(&to_alias(n), &mut taken))
            .collect();
        let unique: std::collections::BTreeSet<&String> = issued.iter().collect();
        assert_eq!(
            unique.len(),
            issued.len(),
            "every alias must be unique, got {issued:?}"
        );
        assert_eq!(issued[0], "pool");
        assert_eq!(issued[1], "pool_2");
    }

    /// Three ways the import can quietly diverge from the manifest, each of which has to reach
    /// the operator as a note rather than being absorbed: an `endBlock` a nest cannot express,
    /// handlers the import does not write config for, and a `source.abi` naming an entry that is not
    /// there. Asserted on the parse, which is what the reporting reads.
    #[test]
    fn divergences_from_the_manifest_are_visible() {
        let m = r#"
specVersion: 0.0.5
dataSources:
  - kind: ethereum/contract
    name: Blocky
    network: mainnet
    source:
      address: "0x0000000000000000000000000000000000000001"
      startBlock: 1
      endBlock: 500
      abi: NotListed
    mapping:
      abis:
        - name: Actual
          file:
            /: QmAbc
      blockHandlers:
        - handler: handleBlock
"#;
        let ds = &parse_manifest(m).unwrap().data_sources[0];
        assert_eq!(
            ds.end_block,
            Some(500),
            "endBlock must be carried to report it"
        );
        assert!(ds.has_block_handlers, "blockHandlers must be noticed");
        assert!(!ds.has_call_handlers);
        assert!(
            ds.events.is_empty(),
            "no eventHandlers means an empty allowlist"
        );
        assert!(
            ds.abi_is_fallback(),
            "`abi: NotListed` is absent from mapping.abis, so own_abi() guessed"
        );
        assert_eq!(ds.own_abi().unwrap().name, "Actual");
    }

    /// A manifest that declares more sources than we will follow is refused before the first
    /// fetch, because each source is a request issued from the operator's address.
    #[test]
    fn an_oversized_manifest_is_refused_before_any_fetch() {
        let mut m = String::from("specVersion: 0.0.5\ndataSources:\n");
        for i in 0..(MAX_SOURCES + 1) {
            m.push_str(&format!(
                "  - kind: ethereum/contract\n    name: C{i}\n    network: mainnet\n    source:\n      address: \"0x0000000000000000000000000000000000000001\"\n"
            ));
        }
        let err = format!("{:#}", parse_manifest(&m).expect_err("must refuse"));
        assert!(err.contains("more than the"), "explain the limit: {err}");
    }

    /// A name is manifest-chosen and ends up in the report an operator reads to decide what to
    /// act on. Escape sequences there can erase or repaint lines that were already printed.
    #[test]
    fn manifest_names_cannot_repaint_the_report() {
        let m = "specVersion: 0.0.5\ndataSources:\n  - kind: ethereum/contract\n    name: \"Good\\r\\u001b[2KEvil\"\n    network: mainnet\n    source:\n      address: \"0x0000000000000000000000000000000000000001\"\n";
        let parsed = parse_manifest(m).unwrap();
        let name = &parsed.data_sources[0].name;
        assert!(
            !name.contains('\r') && !name.contains('\u{1b}'),
            "controls must not survive parsing: {name:?}"
        );
    }

    /// Shape without size still lets a manifest hand us a multi-megabyte "CID" to concatenate
    /// into three gateway URLs and echo back in an error.
    #[test]
    fn an_absurdly_long_cid_is_refused() {
        let huge = "Q".repeat(100_000);
        assert!(candidate_urls(&huge, &["https://gw/".into()], Origin::Manifest).is_err());
        assert!(candidate_urls(
            "QmVPhLwuv9zn2c761Ua7eJAFXBmrNsW32XUYsj1GKtNX4X",
            &["https://gw/".into()],
            Origin::Manifest
        )
        .is_ok());
    }

    #[test]
    fn cid_urls_are_built_for_every_gateway() {
        let gws: Vec<String> = DEFAULT_IPFS_GATEWAYS
            .iter()
            .map(|s| s.to_string())
            .collect();
        let urls = candidate_urls(
            "QmVPhLwuv9zn2c761Ua7eJAFXBmrNsW32XUYsj1GKtNX4X",
            &gws,
            Origin::Manifest,
        )
        .unwrap();
        assert_eq!(urls.len(), gws.len());
        assert!(urls[0].ends_with("QmVPhLwuv9zn2c761Ua7eJAFXBmrNsW32XUYsj1GKtNX4X"));

        // ipfs:// and /ipfs/ prefixes resolve to the same CID.
        for form in ["ipfs://QmAbc", "/ipfs/QmAbc", "QmAbc"] {
            assert!(
                candidate_urls(form, &gws, Origin::Manifest).unwrap()[0].ends_with("QmAbc"),
                "{form}"
            );
        }
        // A full URL is used verbatim, not gatewayed - but only when the operator supplied it.
        assert_eq!(
            candidate_urls("https://example.com/subgraph.yaml", &gws, Origin::Operator).unwrap(),
            vec!["https://example.com/subgraph.yaml".to_string()]
        );
        assert!(candidate_urls("", &gws, Origin::Operator).is_err());
        assert!(candidate_urls("not a cid", &gws, Origin::Operator).is_err());
    }
}
