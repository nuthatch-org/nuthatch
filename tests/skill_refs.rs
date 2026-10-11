//! RFC-0017 §S1 - the drift gate for the builder skill. A skill that lies about flag names is worse
//! than no skill (the same reason stale semantics are worse than none, RFC-0016 §2), so CI enforces
//! two invariants:
//!   1. the committed `cli-reference.md` is byte-identical to what the binary generates now, and
//!   2. every `--flag` mentioned in the *authored* skill files is a real flag (present in the
//!      reference) - no hallucinated flags.
//!
//! RFC-0044 S1 extends (2) to `skills/nuthatch-subgraph-port/` as well: a port skill that lies
//! about `--from-subgraph` is the same failure class.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

fn skill_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(nuthatch::skill::SKILL_DIR)
}

fn authored_skill_md() -> Vec<(String, PathBuf)> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut out = Vec::new();
    for dir in nuthatch::skill::AUTHORED_SKILL_DIRS {
        collect_skill_md(&root.join(dir), &mut out);
    }
    out
}

fn collect_skill_md(dir: &Path, out: &mut Vec<(String, PathBuf)>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_skill_md(&path, out);
            continue;
        }
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        if path.extension().and_then(|e| e.to_str()) == Some("md") && name != "cli-reference.md" {
            out.push((name, path));
        }
    }
}

#[test]
fn committed_cli_reference_is_not_stale() {
    let committed = std::fs::read_to_string(skill_dir().join("cli-reference.md"))
        .expect("cli-reference.md must be committed");
    let fresh = nuthatch::skill::generate_cli_reference();
    assert_eq!(
        committed, fresh,
        "cli-reference.md is out of date - run `nuthatch skill-refs` and commit the result"
    );
}

#[test]
fn authored_files_only_mention_real_flags() {
    // Every `--flag` the reference documents (the source of truth).
    let reference = nuthatch::skill::generate_cli_reference();
    let real: BTreeSet<String> = flags_in(&reference);
    assert!(real.contains("--chain") && real.contains("--seal-direct"));

    // Scan every authored skill file (everything except the generated reference), including
    // the subgraph-port skill (RFC-0044).
    let mut offenders = Vec::new();
    for (name, path) in authored_skill_md() {
        let text = std::fs::read_to_string(&path).unwrap();
        for flag in flags_in(&text) {
            // `--url` etc. are all real; a flag not in the reference is a hallucination.
            if !real.contains(&flag) {
                offenders.push(format!("{name}: `{flag}` is not a real nuthatch flag"));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "authored skill files reference nonexistent flags:\n{}",
        offenders.join("\n")
    );
}

/// Extract `--flag` tokens (long options) from text. A flag is `--` followed by a lowercase letter and
/// then letters/digits/hyphens; trailing punctuation is trimmed by the character class.
fn flags_in(text: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let bytes = text.as_bytes();
    let mut i = 0;
    while i + 2 < bytes.len() {
        if bytes[i] == b'-' && bytes[i + 1] == b'-' && bytes[i + 2].is_ascii_lowercase() {
            let start = i;
            i += 2;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'-') {
                i += 1;
            }
            out.insert(text[start..i].to_string());
        } else {
            i += 1;
        }
    }
    out
}

// ── The other direction for flags (NIG-124, continuing #353) ─────────────────────────────────
//
// `authored_files_only_mention_real_flags` catches a *hallucinated* flag. It cannot catch the
// opposite: a real flag `cli-reference.md` never names - which sounds impossible, since the
// reference is rendered from clap and should be complete by construction. It wasn't:
// `generate_cli_reference` only walked `Cli::command()`'s *subcommands*. A `#[arg(global = true)]`
// flag declared directly on `Cli` is real - accepted on every subcommand, shown in `--help` - and
// was invisible to the renderer, because nothing ever visited the root command's own arguments.
// Confirmed live: adding such a flag left every test in this file green, including
// `committed_cli_reference_is_not_stale`, because the stale copy and the fresh copy omitted it
// identically. `SKILL.md`'s golden rule #1 tells an agent "read cli-reference.md ... if a flag
// isn't here, it doesn't exist" - so a flag the renderer drops breaks that promise exactly the way
// `[[templates]].events` broke `config-reference.md`'s.
//
// The check below re-derives "real" independently of `generate_cli_reference`, the same way
// `config_keys_in` re-parses `src/config.rs` instead of trusting `Config`'s own `Serialize` output:
// it walks clap's command tree itself rather than reusing the generator's walk, so a bug shared
// between the generator and this test can't make both agree on the wrong answer. `src/skill.rs`
// now also renders the root's own arguments, closing the hole this test exists to keep closed.
#[test]
fn cli_reference_names_every_real_flag() {
    let reference = nuthatch::skill::generate_cli_reference();
    let documented = flags_in(&reference);

    let mut missing = Vec::new();
    collect_real_flags(
        &<nuthatch::cli::Cli as clap::CommandFactory>::command(),
        "nuthatch",
        &documented,
        &mut missing,
    );

    assert!(
        missing.is_empty(),
        "cli-reference.md never mentions these real flags:\n{}",
        missing.join("\n")
    );
}

/// Walk every non-hidden long flag on `cmd`, including `cmd`'s own arguments (the thing the
/// generator skipped for the root command), recording any absent from `documented`. Recurses into
/// non-hidden subcommands under `path`, matching the hide semantics `generate_cli_reference` uses -
/// a hidden subcommand or flag is a deliberate exclusion (e.g. `skill-refs` itself), not drift.
fn collect_real_flags(
    cmd: &clap::Command,
    path: &str,
    documented: &BTreeSet<String>,
    missing: &mut Vec<String>,
) {
    for arg in cmd.get_arguments() {
        if arg.is_hide_set() {
            continue;
        }
        if let Some(long) = arg.get_long() {
            let flag = format!("--{long}");
            if !documented.contains(&flag) {
                missing.push(format!("{flag} (on `{path}`)"));
            }
        }
    }
    for sub in cmd.get_subcommands() {
        if sub.is_hide_set() {
            continue;
        }
        collect_real_flags(
            sub,
            &format!("{path} {}", sub.get_name()),
            documented,
            missing,
        );
    }
}

/// RFC-0017 §S1, extended per issue #137 (C2): every `nuthatch_*` metric name an authored skill file
/// mentions must be a real series the binary emits. A stale metric name (`nuthatch_tip` for
/// `nuthatch_tip_height`) is the same failure class as a hallucinated flag - an agent greps a scrape
/// for a name that isn't there and concludes the nest is broken. The source of truth is
/// `Metrics::render()`, exactly as `cli-reference.md` is the source of truth for flags.
#[test]
fn authored_files_only_mention_real_metrics() {
    // The canonical set: every `nuthatch_*` name the exposition can emit. Register a nest first so the
    // per-nest `nuthatch_nest_*` series are present in the render too.
    nuthatch::metrics::METRICS.nest("__drift_probe__");
    let mut real = metric_names_in(&nuthatch::metrics::METRICS.render());
    // **`Metrics::render()` stopped being the whole exposition.** `/metrics` appends the entity
    // series from `serve::ENTITY_SERIES` after it, so a name documented in the builder skill was
    // reported as not existing while being emitted on every scrape. The gate reads the emitter's own
    // table rather than a second list, because two lists is how this drifts back.
    for (name, _) in nuthatch::serve::ENTITY_SERIES {
        real.insert((*name).to_string());
    }
    assert!(real.contains("nuthatch_tip_height") && real.contains("nuthatch_rss_bytes"));
    assert!(
        real.contains("nuthatch_entity_current"),
        "the entity series must be in the canonical set, or the skill cannot document them"
    );

    let mut offenders = Vec::new();
    for (name, path) in authored_skill_md() {
        let text = std::fs::read_to_string(&path).unwrap();
        for metric in metric_names_in(&text) {
            if !real.contains(&metric) {
                offenders.push(format!("{name}: `{metric}` is not a real nuthatch metric"));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "authored skill files reference nonexistent metrics:\n{}",
        offenders.join("\n")
    );
}

/// Extract `nuthatch_<...>` metric-name tokens (lowercase/digit/underscore tail, trailing underscores
/// trimmed so markdown emphasis doesn't leak in). Byte-based so it never slices a multibyte boundary.
fn metric_names_in(text: &str) -> BTreeSet<String> {
    const PREFIX: &[u8] = b"nuthatch_";
    let mut out = BTreeSet::new();
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i..].starts_with(PREFIX) {
            let start = i;
            i += PREFIX.len();
            while i < bytes.len()
                && (bytes[i].is_ascii_lowercase() || bytes[i].is_ascii_digit() || bytes[i] == b'_')
            {
                i += 1;
            }
            let mut end = i;
            while end > start + PREFIX.len() && bytes[end - 1] == b'_' {
                end -= 1;
            }
            // The token is pure ASCII by construction, so this slice is always valid UTF-8.
            out.insert(String::from_utf8_lossy(&bytes[start..end]).into_owned());
        } else {
            i += 1;
        }
    }
    out
}

// ── The other direction: a real key the reference never mentions (#353) ──────────────
//
// The two tests above catch a *hallucinated* flag or metric. They cannot catch the opposite —
// something real that the skill never names — and that is the failure that actually shipped:
// `[[templates]].events` (#347) was a live config key documented nowhere a user or an agent reads,
// with CI green throughout. An agent that cannot see a key will not use it, and will confidently
// write config without it.
//
// Enumeration is the hard half. This scans `src/*.rs` for `pub` fields on structs that derive
// `Deserialize`, which is the honest definition of "config": a key is authored config exactly when
// it can be deserialised from a file. That rule drops server-assembled types (`Coverage` is
// `Serialize`-only) without needing an entry in the opt-out list, and it fails loudly when a field
// is added — the property the issue asked for, and the reason this scans source rather than
// serialising a populated `Config`, where a `skip_serializing_if` field would vanish and the test
// would pass for the wrong reason.

/// Config surfaces the reference claims to mirror, and the structs that matter in each.
/// `None` means every `Deserialize` struct in the file.
const CONFIG_SOURCES: &[(&str, Option<&[&str]>)] = &[
    ("src/config.rs", None),
    ("src/semantic.rs", None),
    // Mount config lives here since RFC-0032 retired the roost; the rest of runtime.rs is not config.
    (
        "src/runtime.rs",
        Some(&["Mount", "MountTable", "RuntimeMeta", "ChainEndpoint"]),
    ),
    // `queries.toml` (RFC-0034 phase 2, #353): the author's ceiling. `Surface` also lives here but is
    // not `Deserialize` (it's the mount-side runtime type, not something read from a file).
    ("src/allowlist.rs", Some(&["Ceiling", "NamedQuery"])),
    // Nested under `nuthatch.toml` but defined in their own files. Missing here is how a new
    // `[[calls]]` key ships undocumented, the same hole #713 is for `allowlist.rs`.
    ("src/calls.rs", Some(&["CallDecl"])),
    ("src/ipfs.rs", Some(&["IpfsDecl"])),
    ("src/entities.rs", Some(&["EntityFile", "EntityDecl"])),
    // RFC-0062: `maintained.toml`, the views a nest answers from stored copies.
    ("src/maintained.rs", Some(&["Declaration", "DeclaredView"])),
    // RFC-0046 S2's optional counter. Feature-gated off by default, but `[counter]` under a mount is
    // operator config like any other when the feature is on, and an undocumented price is exactly
    // the key an operator most needs to find.
    ("src/counter.rs", Some(&["Config", "Network"])),
    // RFC-0060: `graph/history.toml`, the Graph history read policy. Operator config, but only a
    // `graph` build reads it; see its entries in KNOWN_UNDOCUMENTED.
    ("src/graph_history.rs", Some(&["Policy"])),
    // RFC-0059: `folds/folds.toml`, authored nest config that only a `folds` build reads; see its
    // entries in KNOWN_UNDOCUMENTED.
    ("src/folds.rs", Some(&["FoldsToml", "FoldDecl"])),
    // RFC-0063: `[address_history]` in `nuthatch.toml`.
    ("src/address_history.rs", Some(&["AddressHistoryConfig"])),
];

/// `src/*.rs` files that derive `Deserialize` for wire formats, snapshots, or HTTP bodies - not
/// operator-authored nest/runtime config. A reason is required: an empty excuse is how the next
/// config file lands on this list by accident.
const NOT_OPERATOR_CONFIG: &[(&str, &str)] = &[
    ("src/count.rs", "the per-machine head-count answer, not nest config"), // RFC-0054 hook
    ("src/address_mode.rs", "the /api/watch request body, not nest config"),
    (
        "src/address_partitions.rs",
        "the partition manifest a publisher writes, not nest config",
    ),
    (
        "src/address_proposers.rs",
        "the proposer catalogue manifest a publisher writes, not nest config",
    ),
    ("src/tape.rs", "RFC-0039 tape bytes, not nest config"),
    (
        "src/mount_jobs.rs",
        "mount-jobs.json: live-mount progress the runtime writes and resumes (#1544), never operator input",
    ),
    (
        // RFC-0037 slice 6. `Head` reads a stored row's table, block and log index back out of the hot
        // store to plan which documents it names. It is never operator input.
        "src/ipfs_resolve.rs",
        "stored hot-store rows read back by the resolver, not nest config",
    ),
    (
        // RFC-0053 S0's migration validator. `Operation` is one entry in a **corpus file the
        // caller supplies on the command line** - the queries they want checked - not nest config
        // an operator authors and nothing under `nuthatch.toml` nests it. The operator surface for
        // this is the CLI subcommand, which `cli-reference.md` documents.
        "src/graph_validate.rs",
        "a caller-supplied query corpus, not nest config",
    ),
    (
        // RFC-0045 stage 1. `Catalogue`/`Snapshot` are the provenance manifest that
        // `nuthatch offchain drop` *writes* - content hash, source path, ingest time, tool version.
        // An operator never authors it, and nothing under `nuthatch.toml` nests it; the operator
        // surface for this feature is the CLI subcommand, which `cli-reference.md` documents.
        "src/offchain.rs",
        "the generated offchain provenance manifest, written by the tool and not authored",
    ),
    (
        // RFC-0052. `PublishEnvelope` is `publish.json`, written by `nuthatch publish sync`.
        // The operator names a `--target`; they never author this file.
        "src/publish.rs",
        "the generated publish.json envelope, written by the tool and not authored",
    ),
    (
        // Not serde's `Deserialize` at all - this is rkyv's, required by `dbsp::DBData` so an entity
        // circuit can archive its own row type (#870). Nothing here is ever read from a nest's TOML.
        // The gate matches on the derive *name*, which cannot tell the two apart; classifying it is
        // the intended way to say so.
        "src/entity_row.rs",
        "rkyv archival for DBSP row state, not nest config",
    ),
    (
        // Same story as `entity_row.rs`: rkyv's `Deserialize`, not serde's, because the linear
        // aggregate accumulator is a DBSP value and has to archive itself (#870).
        "src/entity_circuit.rs",
        "rkyv archival for DBSP aggregate state, not nest config",
    ),
    (
        "src/blob.rs",
        "content-addressed bundle manifest, not nest config",
    ),
    (
        "src/seal.rs",
        "sealed-segment catalogue, written by the indexer",
    ),
    ("src/pack.rs", "compliance-pack manifest, not nest config"),
    ("src/labels.rs", "imported label snapshots, not nest toml"),
    ("src/lists.rs", "fetched list snapshots, not nest toml"),
    ("src/metadata.rs", "cached token metadata, not nest toml"),
    ("src/lifecycle.rs", "HTTP request bodies for the admin API"),
    (
        "src/control_api.rs",
        "HTTP request bodies for the control plane",
    ),
    ("src/serve.rs", "HTTP request bodies for the query API"),
    (
        // The decoded `X-PAYMENT` header a caller presents (RFC-0046 S1, #1218). It arrives on a
        // request and is never read from a nest's TOML - and must not become a config key, because
        // a price that lives in nest identity is a price a self-hoster cannot turn off, which is
        // the line #1217's gate exists to hold.
        "src/x402.rs",
        "an inbound x402 authorisation header, not nest config",
    ),
];

/// Keys that are deliberately absent from the reference, each with the reason it is absent.
///
/// This list is the useful artifact, not an escape hatch: a key belongs here only when *not*
/// documenting it is the decision. It is checked for staleness below, so an entry that becomes
/// documented fails rather than lingering.
const KNOWN_UNDOCUMENTED: &[(&str, &str)] = &[
    // RFC-0060 §5.6 (#1438): the network facade stays invisible to a default build, which refuses these
    // by name. They are documented with the network nest, not in the builder reference every nest reads.
    ("CallDecl.on_any", "`graph` builds only (RFC-0060 §5.6): a default build refuses it by name"),
    ("CallDecl.canonical", "`graph` builds only (RFC-0060 §5.6): a default build refuses it by name"),
    ("Policy.version", "`graph/history.toml`, which only a `graph` build reads (RFC-0060 §5.6)"),
    ("Policy.first_block", "`graph/history.toml`, which only a `graph` build reads (RFC-0060 §5.6)"),
    ("Policy.max_head_age_seconds", "`graph/history.toml`, which only a `graph` build reads (RFC-0060 §5.6)"),
    ("Policy.max_block_distance", "`graph/history.toml`, which only a `graph` build reads (RFC-0060 §5.6)"),
    ("FoldsToml.fold", "`folds/folds.toml`, which only a `folds` build reads (RFC-0059 packaging)"),
    ("FoldDecl.name", "`folds/folds.toml`, which only a `folds` build reads (RFC-0059 packaging)"),
    ("FoldDecl.key", "`folds/folds.toml`, which only a `folds` build reads (RFC-0059 packaging)"),
    ("FoldDecl.carry", "`folds/folds.toml`, which only a `folds` build reads (RFC-0059 packaging)"),
    ("FoldDecl.max_rows", "`folds/folds.toml`, which only a `folds` build reads (RFC-0059 packaging)"),
    // `Config.extract` is documented now (RFC-0038 §5 added `top_level_calls`, which works on
    // ordinary RPC), so its excuse is gone. The node-gated fields below keep theirs.
    ("Extract.blocks", "field of `[extract]`; sourceable from ordinary RPC (RFC-0036) but not yet written up"),
    ("Extract.state", "field of `[extract]`, same reason"),
    ("Extract.traces", "field of `[extract]`, same reason"),
    ("Extract.selectors", "field of `[extract]`, same reason"),
    ("Extract.unbounded", "field of `[extract]`, same reason"),
    // `Config.calls` was excused here as "parses but is never executed (#268)". It executes now, so
    // the excuse expired and `[[calls]]` is documented in the reference instead.
    ("Config.state_rpc_urls", "`#[serde(skip)]` - the tier-3 archive endpoint comes from `--state-rpc` and is deliberately never a config key, because it carries an API key and this file is pinned into the nest's content address"),
    ("Config.ipfs_gateways", "`#[serde(skip)]` - IPFS gateways come from `--ipfs`. A gateway is an *access path*: two operators resolving one CID through different gateways must get the same bytes, so it must not enter the nest's content address"),
    ("Config.freshness", "`#[serde(skip)]` - the poll interval and finality-only mode come from `--poll-interval` / `--finality-only` (RFC-0040, #1173). How often an operator asks is not what the nest is: two nests differing only in cadence hold identical rows and must share one content address."),
    ("Config.ipfs_window_deadline", "`#[serde(skip)]` - how long a seal-direct window waits for one IPFS document comes from `--ipfs-window-deadline` (#1399). Waiting longer or giving up sooner changes which documents seal, not what the nest is, and the reference names the flag beside `[[ipfs]]`."),
    ("Config.route", "`#[serde(skip)]` - the route a runtime serves a nest under, stamped by the runtime before `build_nest` so its cursor, health and metrics are keyed by it (#1415). Where a nest is mounted is not what it is, and an author never writes it."),
    ("Config.read_only_role", "`#[serde(skip)]` - the `serve` role, set by `serve_role` before `build_nest` so a `[[calls]]` nest it will never sample is not refused for lacking an archive endpoint (#1167). A role is not data identity and is never a config key."),
];

#[test]
fn config_sources_names_every_operator_config_file() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let listed: std::collections::BTreeSet<&str> = CONFIG_SOURCES.iter().map(|(f, _)| *f).collect();
    let exempt: std::collections::BTreeMap<&str, &str> =
        NOT_OPERATOR_CONFIG.iter().copied().collect();

    assert!(
        listed.contains("src/allowlist.rs"),
        "deleting the allowlist.rs row is #713: the next queries.toml-shaped file will escape \
         the same way. Put it back, or this gate is decorative."
    );

    let src = root.join("src");
    let mut missing = Vec::new();
    for entry in std::fs::read_dir(&src).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        let rel = format!("src/{}", path.file_name().unwrap().to_string_lossy());
        let text = std::fs::read_to_string(&path).unwrap();
        if !text.contains("Deserialize") {
            continue;
        }
        if listed.contains(rel.as_str()) || exempt.contains_key(rel.as_str()) {
            continue;
        }
        missing.push(format!(
            "{rel} derives Deserialize and is in neither CONFIG_SOURCES nor NOT_OPERATOR_CONFIG"
        ));
    }
    assert!(
        missing.is_empty(),
        "a Deserialize file in src/*.rs is operator config until named otherwise (#713):\n{}",
        missing.join("\n")
    );

    let stale: Vec<&str> = NOT_OPERATOR_CONFIG
        .iter()
        .filter(|(f, _)| listed.contains(f))
        .map(|(f, _)| *f)
        .collect();
    assert!(
        stale.is_empty(),
        "NOT_OPERATOR_CONFIG overlaps CONFIG_SOURCES, so the completeness check cannot fail: {stale:?}"
    );
}

#[test]
fn config_reference_names_every_real_config_key() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let reference = std::fs::read_to_string(skill_dir().join("config-reference.md"))
        .expect("config-reference.md must be committed");

    let excused: std::collections::BTreeMap<&str, &str> =
        KNOWN_UNDOCUMENTED.iter().copied().collect();
    let mut undocumented = Vec::new();
    let mut all_keys = Vec::new();

    for (file, only) in CONFIG_SOURCES {
        let src = std::fs::read_to_string(root.join(file)).unwrap_or_else(|e| {
            panic!("{file} is named as a config surface but cannot be read: {e}")
        });
        for (struct_name, key) in config_keys_in(&src, *only) {
            let qualified = format!("{struct_name}.{key}");
            all_keys.push(qualified.clone());
            if names_key(&reference, &key) || excused.contains_key(qualified.as_str()) {
                continue;
            }
            undocumented.push(format!(
                "{qualified} (from {file}) — a real config key the reference never names"
            ));
        }
    }

    assert!(
        all_keys.len() > 40,
        "the scanner found only {} keys, so it has stopped matching the structs - fix the scan \
         rather than the assertion, or this gate silently passes forever",
        all_keys.len()
    );
    assert!(
        undocumented.is_empty(),
        "config keys exist that `config-reference.md` never mentions. An agent cannot use a key it \
         cannot see. Document each, or add it to KNOWN_UNDOCUMENTED with the reason:\n{}",
        undocumented.join("\n")
    );

    // An opt-out that is no longer needed must not linger: it would hide the next real gap.
    let stale: Vec<&str> = KNOWN_UNDOCUMENTED
        .iter()
        .filter(|(k, _)| {
            let key = k.split_once('.').map(|(_, f)| f).unwrap_or(k);
            all_keys.iter().any(|q| q == k) && names_key(&reference, key)
        })
        .map(|(k, _)| *k)
        .collect();
    assert!(
        stale.is_empty(),
        "these keys are documented now, so remove them from KNOWN_UNDOCUMENTED: {stale:?}"
    );
}

/// Whether the reference documents `key` **as a TOML key** — an assignment (`key = …`) or a table
/// header component (`[key]`, `[[key]]`, `[table.key.columns]`).
///
/// Deliberately not a plain token search. Mentioning a key in prose or in a trailing `#` comment is
/// not documenting it, and a looser match makes the gate self-defeating: while writing this, the
/// comment "the subset of big_ints too wide for DECIMAL(38,0)" was enough to keep the test green
/// after `big_ints = […]` had been deleted outright. A drift gate that passes when the thing it
/// guards is gone is the exact failure #353 is about.
fn names_key(reference: &str, key: &str) -> bool {
    reference.lines().any(|line| {
        let line = line.trim();
        // `key = value`, ignoring anything after a `#` comment marker.
        let code = line.split('#').next().unwrap_or("").trim();
        if let Some((lhs, _)) = code.split_once('=') {
            if lhs.trim() == key {
                return true;
            }
        }
        // `[key]` / `[[key]]` / `[table.key.columns]` - dot-separated header components.
        if let Some(header) = code
            .strip_prefix("[[")
            .and_then(|h| h.strip_suffix("]]"))
            .or_else(|| code.strip_prefix('[').and_then(|h| h.strip_suffix(']')))
        {
            return header.split('.').any(|part| part == key);
        }
        false
    })
}

/// `pub` field names on `Deserialize` structs, honouring `#[serde(rename = "…")]` — the TOML key is
/// what the reference has to name, not the Rust identifier.
fn config_keys_in(src: &str, only: Option<&[&str]>) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for (idx, _) in src.match_indices("pub struct ") {
        let head = &src[..idx];
        // The derive list is the attribute block immediately above the struct.
        let derives = head.rsplit("#[derive(").next().unwrap_or("");
        let derives = derives.split(")]").next().unwrap_or("");
        let after = &src[idx + "pub struct ".len()..];
        let name: String = after
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        if let Some(list) = only {
            if !list.contains(&name.as_str()) {
                continue;
            }
        }
        // Only a struct that can be deserialised is authored config; the rest is output.
        if !derives.contains("Deserialize") || !head.trim_end().ends_with("]") {
            continue;
        }
        let Some(body_start) = after.find('{') else {
            continue;
        };
        let Some(body_len) = after[body_start..].find("\n}") else {
            continue;
        };
        let body = &after[body_start..body_start + body_len];

        for (fidx, _) in body.match_indices("pub ") {
            let rest = &body[fidx + 4..];
            let field: String = rest
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            // A field, not `pub fn` / `pub struct` nested in a doc comment.
            if field.is_empty() || !rest[field.len()..].starts_with(':') {
                continue;
            }
            // `#[serde(rename = "x")]` on the lines just above renames the TOML key.
            let preceding = &body[..fidx];
            let key = preceding
                .rsplit("#[serde(")
                .next()
                .and_then(|attr| attr.split(")]").next())
                .filter(|attr| !attr.contains('\n') || attr.matches('\n').count() < 3)
                .and_then(|attr| attr.split("rename = \"").nth(1))
                .and_then(|r| r.split('"').next())
                .map(|s| s.to_string())
                .filter(|_| {
                    // Only if that attribute block is the one attached to this field.
                    preceding
                        .rsplit("#[serde(")
                        .next()
                        .is_some_and(|a| !a.contains("pub "))
                })
                .unwrap_or_else(|| field.clone());
            out.push((name.clone(), key));
        }
    }
    out
}
