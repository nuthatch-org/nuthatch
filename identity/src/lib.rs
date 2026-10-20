//! A nest's identity (RFC-0032 §3, RFC-0033 §5), computed from its files' bytes and nothing else, so a
//! browser tab that holds a nest as a map of path to bytes gets the identity a node gives the same
//! directory (RFC-0066 S0). Walking a directory, and deciding what in it is derived state, stays with
//! the caller.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Blob manifest schema version. Bumped only on an incompatible manifest-shape change; a blob whose
/// version this build doesn't understand is rejected on mount (like `schema_version` in `config.rs`).
pub const BLOB_FORMAT_VERSION: u32 = 1;

/// The nest's config, the one input whose bytes the identity does not take verbatim.
pub const CONFIG_FILE: &str = "nuthatch.toml";

/// One packed input file: its path relative to the nest root and the hash of its bytes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileEntry {
    pub path: String,
    pub sha256: String,
}

/// The blob manifest - the content-addressed declaration of a nest's inputs. Field order here IS the
/// canonical order (serde preserves declaration order); `files` is sorted by path. Do not reorder
/// fields without bumping [`BLOB_FORMAT_VERSION`] - the order is load-bearing for the blob hash.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Manifest {
    pub blob_format_version: u32,
    pub nest_name: String,
    pub schema_version: u32,
    /// The nuthatch version that produced (and can reproduce) this blob's decode registry.
    pub generator_version: String,
    /// The expected decode-registry hash - a mount regenerates the registry from `files` and asserts
    /// it equals this. Hex, no `0x` (matches the seal manifest's convention).
    pub registry_hash: String,
    /// Every authored input, sorted by `path`. A Merkle layer: each file hashed, the sorted list
    /// then folded into the blob hash via the canonical manifest.
    pub files: Vec<FileEntry>,
}

/// The manifest of a nest whose authored inputs are `files`, each a path relative to the nest root
/// (`/`-separated) and its bytes as read. `registry_hash` is the decode registry's, hex.
pub fn manifest(
    nest_name: String,
    schema_version: u32,
    generator_version: String,
    registry_hash: String,
    files: impl IntoIterator<Item = (String, Vec<u8>)>,
) -> Result<Manifest> {
    let mut files = files
        .into_iter()
        .map(|(path, bytes)| {
            let bytes = input_bytes(&path, bytes)?;
            Ok(FileEntry {
                sha256: hex::encode(Sha256::digest(&bytes)),
                path,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    // Sorted as a directory walk sorts paths, by component: `views/a.sql` before `views-old.sql`,
    // which a plain string sort would put the other way round.
    files.sort_by(|a, b| a.path.split('/').cmp(b.path.split('/')));
    Ok(Manifest {
        blob_format_version: BLOB_FORMAT_VERSION,
        nest_name,
        schema_version,
        generator_version,
        registry_hash,
        files,
    })
}

impl Manifest {
    /// The canonical byte serialization the blob hash is taken over: compact JSON (no incidental
    /// whitespace), fixed field order, `files` pre-sorted. Deterministic across machines.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        // `to_vec` is compact (no pretty whitespace); struct field order is fixed; `files` is sorted at
        // build time. serde_json preserves map/struct key order as declared, so this is stable.
        serde_json::to_vec(self).expect("manifest serializes")
    }

    /// The blob hash: `sha256` of the canonical manifest bytes, hex-encoded. This is the nest's
    /// content address - the thing `mount <hash>` resolves.
    pub fn blob_hash(&self) -> String {
        hex::encode(Sha256::digest(self.canonical_bytes()))
    }

    /// The **nest identity (NID)** - the key a dataset is stored under (RFC-0032 §3).
    ///
    /// Deliberately *not* [`blob_hash`](Self::blob_hash). The blob hash pins the bundle **a
    /// particular binary produced**, and it includes `generator_version`, which changes on every
    /// nuthatch release. That is right for a bundle and fatal for a storage key: upgrading the
    /// binary would hand every nest a new identity, orphan every dataset, and re-index the lot -
    /// precisely the cost RFC-0032 exists to avoid, and a contradiction of the drop-in upgrade
    /// property we have already proven in production.
    ///
    /// So the NID hashes the manifest with `generator_version` neutralised, and **nothing is lost**:
    /// the *behavioural* part of the generator is already pinned by `registry_hash`, which `load`
    /// regenerates from the inputs and asserts. A new binary that decodes differently moves the
    /// registry hash and therefore the NID - which is correct. A new binary that decodes identically
    /// leaves the nest the same nest, and it keeps its data.
    ///
    /// Domain-separated from the blob hash so the two can never be confused by a caller that has one
    /// hex string and no context.
    pub fn nid(&self) -> String {
        let mut canonical = self.clone();
        canonical.generator_version = NID_GENERATOR_SENTINEL.to_string();
        let mut h = Sha256::new();
        h.update(NID_DOMAIN);
        h.update(canonical.canonical_bytes());
        hex::encode(h.finalize())
    }

    /// The **data identity**: what this nest's inputs imply about the bytes it will store
    /// (RFC-0033 §5, early cutoff).
    ///
    /// Two nests with the same data identity produce byte-identical segments over the same range, so
    /// one may **adopt** the other's dataset instead of re-indexing the chain. That is the whole
    /// mechanism: a cosmetic edit - a comment, a view rename, a doc change - moves the NID, and the
    /// data identity stays put, so the new identity resolves onto data that already exists.
    ///
    /// It is a *second, coarser* identity, not a hole cut in the NID. The NID remains the package
    /// identity that mounts, refcounting and sharing key on (RFC-0032); this one is only ever
    /// consulted to answer "may this dataset be adopted?". Carving an exception into the NID itself
    /// is what RFC-0034 §4 rejected, and this is not that.
    ///
    /// Includes `registry_hash`, which pins the decode: a generator that decodes differently moves it
    /// even when every authored byte is unchanged.
    pub fn data_identity(&self) -> String {
        let mut h = Sha256::new();
        h.update(b"nuthatch-data-identity-v1\0");
        h.update(self.schema_version.to_le_bytes());
        let mut feed = |s: &str| {
            h.update((s.len() as u64).to_le_bytes());
            h.update(s.as_bytes());
        };
        feed(&self.registry_hash);
        // `files` is sorted by path at build time, so this is deterministic.
        for f in self.files.iter().filter(|f| affects_data(&f.path)) {
            feed(&f.path);
            feed(&f.sha256);
        }
        hex::encode(h.finalize())
    }
}

/// Domain separator for [`Manifest::nid`]. Two hashes over near-identical bytes must never collide
/// into meaning the same thing, and a bare `sha256` of a manifest already means "blob hash".
const NID_DOMAIN: &[u8] = b"nuthatch-nid-v1\0";

/// What `generator_version` is replaced with before hashing a NID. A fixed sentinel rather than an
/// empty string, so the field's *absence* from the identity is explicit in the hashed bytes.
const NID_GENERATOR_SENTINEL: &str = "<not-part-of-identity>";

/// An input file's bytes as the identity sees them: verbatim, except that `nuthatch.toml` loses its
/// `[address_history]` table (RFC-0063). The watched accounts change at runtime and are the user's
/// private business, so they are not part of what the nest is.
pub fn input_bytes(path: &str, bytes: Vec<u8>) -> Result<Vec<u8>> {
    if path == CONFIG_FILE {
        return without_address_history(bytes);
    }
    Ok(bytes)
}

/// Cut the `[address_history]` table out of a `nuthatch.toml`, leaving every other byte as written.
/// A file without the table comes back unchanged, so no existing nest's identity moves.
pub fn without_address_history(bytes: Vec<u8>) -> Result<Vec<u8>> {
    const TABLE: &str = "address_history";
    let text = String::from_utf8(bytes).context("nuthatch.toml is not UTF-8")?;
    let mut parsed: toml::Table = toml::from_str(&text).context("parsing nuthatch.toml")?;
    if parsed.remove(TABLE).is_none() {
        return Ok(text.into_bytes());
    }
    let mut out = String::with_capacity(text.len());
    let mut skipping = false;
    // Comments and blank lines met while skipping are held back: those directly above the next
    // header belong to that table, not to this one.
    let mut held = String::new();
    for line in text.split_inclusive('\n') {
        let t = line.trim_start();
        if let Some(name) = table_header(t) {
            let was_skipping = skipping;
            // `[address_history.sweep]` and `.mirror` are sub-tables of it, so they go too.
            skipping = name
                .split('.')
                .next()
                .map(|k| k.trim().trim_matches(['"', '\'']))
                == Some(TABLE);
            if skipping {
                held.clear();
                continue;
            }
            if was_skipping {
                out.push_str(&held);
                held.clear();
            }
        }
        if !skipping {
            out.push_str(line);
        } else if t.trim_end().is_empty() || t.starts_with('#') {
            held.push_str(line);
        } else {
            held.clear();
        }
    }
    // The cut is textual, so check it removed exactly the table: an inline or dotted form would leave
    // something behind, or take a neighbour with it.
    let left: toml::Table =
        toml::from_str(&out).context("nuthatch.toml without [address_history]")?;
    if left != parsed {
        bail!(
            "write [address_history] in nuthatch.toml as a table of its own (a `[address_history]` \
             header), not inline or as dotted keys, so it can be kept out of the nest's identity"
        );
    }
    Ok(out.into_bytes())
}

/// The key a `[table]` header line names, unquoted and trimmed: `[ a ]`, `["a"]` and `['a']` all name
/// `a`. `None` for anything else, an array-of-tables header included.
fn table_header(line: &str) -> Option<String> {
    let rest = line.strip_prefix('[')?;
    if rest.starts_with('[') {
        return None;
    }
    let name = rest[..rest.find(']')?].trim();
    let unquoted = ['"', '\'']
        .iter()
        .find_map(|q| name.strip_prefix(*q).and_then(|n| n.strip_suffix(*q)))
        .unwrap_or(name);
    Some(unquoted.to_string())
}

/// Authored inputs that provably cannot change a nest's **stored data** (RFC-0033 §5).
///
/// This list is the load-bearing part of [`Manifest::data_identity`], and it is deliberately written
/// as an exclusion list rather than an inclusion one, because the two directions fail differently:
///
/// - Wrongly **including** something here that does affect data → a dataset is adopted when it should
///   have differed → **wrong data**. Catastrophic.
/// - Wrongly **omitting** something that does not → no adoption → a re-index. Slow, and correct.
///
/// So every entry needs a reason that survives a hostile reading, and when in doubt an input stays in
/// the identity. Each one below is justified by "the indexing path never reads it":
///
/// - `views/**` - authored SQL is **not materialised**. `analytics::define_nest_views` defines views
///   per query on an ephemeral in-memory Burrmill over the segments and the hot tip; nothing persists
///   them, so no view can influence a byte that is stored.
/// - `entities.toml` and `entities/**` - RFC-0041 declarations and definitions affect derived entity
///   state only. Slice one validates them, and slice two rebuilds that state from the adopted facts;
///   they must move the package NID without forcing the decoded dataset to be re-indexed.
/// - `semantic.toml` - descriptions and derived footguns, read by the MCP/semantic surface only.
/// - `llms.txt`, `README.md` - documentation.
/// - `.claude/**` - the scaffolded agent skill.
///
/// Deliberately **not** excluded, though a reader might expect them to be: `labels/**` (labels drive
/// exposure annotations, which are stored), `schema.json` (derived from the config, and it moves with
/// it anyway), and anything under `abis/` (the decode itself).
const NON_DATA_INPUTS: &[&str] = &[
    "views/",
    "entities/",
    // The author's query ceiling (RFC-0034 §3). An authored input, so it is in the NID - but it
    // cannot change a stored byte, and if it moved the *data* identity then every security tweak
    // would re-index the chain, which is precisely what phase 2 was sequenced behind early cutoff to
    // avoid.
    "queries.toml",
    // RFC-0060: the Graph history read policy, read by serving alone. Excluded in every build so the
    // data identity never depends on compile features; a default build refuses the nest anyway.
    "graph/history.toml",
    // RFC-0059: folds are read at query time over stored facts and never change a stored byte.
    "folds/",
    // RFC-0062: which views are answered from copies. A copy is a cache of a view's answer, so
    // declaring one changes no stored byte and must not stop a nest seeding from a mirror without it.
    "maintained.toml",
    "entities.toml",
    "semantic.toml",
    "llms.txt",
    "README.md",
    ".claude/",
];

/// A hidden file or directory is never a data input: no indexing path reads one, and nests accumulate
/// them (an engine's extension cache, `.gitignore`), which made two copies of one nest unable to
/// find each other's mirror. Only the data identity skips them; the NID still counts every input.
pub fn affects_data(path: &str) -> bool {
    if path.split('/').any(|part| part.starts_with('.')) {
        return false;
    }
    !NON_DATA_INPUTS.iter().any(|p| {
        if let Some(dir) = p.strip_suffix('/') {
            path == dir || path.starts_with(p)
        } else {
            path == *p
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(path: &str) -> (String, Vec<u8>) {
        (path.to_string(), path.as_bytes().to_vec())
    }

    #[test]
    fn files_sort_as_a_directory_walk_does() {
        let m = manifest(
            "n".into(),
            1,
            "0".into(),
            "00".into(),
            [
                file("views-old.sql"),
                file("views/a.sql"),
                file("abis/c.json"),
            ],
        )
        .unwrap();
        let paths: Vec<_> = m.files.iter().map(|f| f.path.as_str()).collect();
        assert_eq!(paths, ["abis/c.json", "views/a.sql", "views-old.sql"]);
    }

    #[test]
    fn the_config_is_hashed_without_its_address_history() {
        let plain = "[nest]\nname = \"n\"\n";
        let with = format!("{plain}\n[address_history]\naddresses = []\n");
        let hash = |text: &str| {
            manifest(
                "n".into(),
                1,
                "0".into(),
                "00".into(),
                [(CONFIG_FILE.into(), text.into())],
            )
            .unwrap()
            .files[0]
                .sha256
                .clone()
        };
        assert_eq!(hash(&with), hash(&format!("{plain}\n")));
        assert_ne!(hash(&with), hash(plain));
    }

    /// The documented `[address_history.sweep]` and `.mirror` sub-tables are part of the table (#2188).
    #[test]
    fn the_address_history_sub_tables_are_cut_with_it() {
        let plain = "[nest]\nname = \"n\"\n";
        let with = format!(
            "{plain}\n[address_history]\naddresses = []\n\n[address_history.sweep]\n\
             from_block = 1\n\n[ address_history . mirror ]\nurl = \"m\"\n\n[sink]\nx = 1\n"
        );
        let cut = String::from_utf8(without_address_history(with.into_bytes()).unwrap()).unwrap();
        assert_eq!(cut, format!("{plain}\n\n[sink]\nx = 1\n"));
    }
}
