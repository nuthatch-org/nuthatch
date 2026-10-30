//! RFC-0077 S1: every crate `embed/` shares with the binary resolves to the binary's version.
//!
//! `embed/` cannot join a workspace with the binary, which would apply its dbsp patch to the binary,
//! so each keeps its own `Cargo.lock` and the two drift. RFC-0075 S3's gate first failed that way: the
//! tab resolved parquet 59.3.2 and the binary 59.3.0, and the version string parquet writes into every
//! file moved the segment hash. A crate is shared when the binary's lockfile holds a version of it in
//! the same semver-compatible line; another line of the same name is a different crate to Cargo.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

/// The lockfiles that build the engine. `embed/plain/host` is wasmtime driving the module, not part
/// of it.
const EMBED_LOCKS: &[&str] = &["embed/Cargo.lock", "embed/plain/Cargo.lock"];

/// Crates allowed to differ, each with the reason it cannot follow the binary.
const MAY_DIFFER: &[(&str, &str)] = &[
    (
        "wasm-bindgen",
        "pinned to the wasm-bindgen CLI the wasm-tools action installs; the binary never builds it",
    ),
    ("wasm-bindgen-futures", "as wasm-bindgen"),
    ("wasm-bindgen-macro", "as wasm-bindgen"),
    ("wasm-bindgen-macro-support", "as wasm-bindgen"),
    ("wasm-bindgen-shared", "as wasm-bindgen"),
    ("js-sys", "as wasm-bindgen"),
    ("web-sys", "as wasm-bindgen"),
];

type Resolved = BTreeMap<String, BTreeSet<String>>;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Every version of every crate a lockfile resolves.
fn resolved(lock: &str) -> Resolved {
    let path = root().join(lock);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
    let doc: toml::Value =
        toml::from_str(&text).unwrap_or_else(|e| panic!("{} is not TOML: {e}", path.display()));
    let mut out = Resolved::new();
    for p in doc["package"].as_array().expect("a [[package]] array") {
        let name = p["name"].as_str().expect("a package name");
        let version = p["version"].as_str().expect("a package version");
        out.entry(name.to_string())
            .or_default()
            .insert(version.to_string());
    }
    out
}

/// The part of a version Cargo treats as compatible: the major, or the minor under 1.0, or the patch
/// under 0.1.
fn line(version: &str) -> String {
    let core = version.split(['+', '-']).next().unwrap_or(version);
    let parts: Vec<&str> = core.split('.').collect();
    match parts.as_slice() {
        ["0", "0", patch, ..] => format!("0.0.{patch}"),
        ["0", minor, ..] => format!("0.{minor}"),
        [major, ..] => major.to_string(),
        [] => version.to_string(),
    }
}

/// `(crate, the engine's version, the binary's)` for each shared crate that resolves differently.
fn drift(binary: &Resolved, engine: Resolved) -> Vec<(String, String, String)> {
    let mut out = Vec::new();
    for (name, versions) in engine {
        if MAY_DIFFER.iter().any(|(n, _)| *n == name) {
            continue;
        }
        let Some(theirs) = binary.get(&name) else {
            continue;
        };
        for v in versions {
            let same_line: Vec<&str> = theirs
                .iter()
                .filter(|b| line(b) == line(&v))
                .map(String::as_str)
                .collect();
            if !same_line.is_empty() && !same_line.contains(&v.as_str()) {
                out.push((name.clone(), v, same_line.join(" or ")));
            }
        }
    }
    out
}

#[test]
fn every_crate_the_engine_shares_with_the_binary_is_the_binarys_version() {
    let binary = resolved("Cargo.lock");
    let mut failures = Vec::new();
    for lock in EMBED_LOCKS {
        for (name, ours, theirs) in drift(&binary, resolved(lock)) {
            failures.push(format!("{lock}: {name} {ours}, the binary has {theirs}"));
        }
    }
    assert!(
        failures.is_empty(),
        "{} crate(s) resolve differently from the binary. Align each with `cargo update -p \
         <name>@<version> --precise <the binary's>` beside that lockfile:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn only_the_same_compatible_line_is_shared() {
    assert_eq!(line("0.2.17"), "0.2");
    assert_eq!(line("1.29.0"), "1");
    assert_eq!(line("0.0.3"), "0.0.3");
    assert_eq!(line("1.1.2+spec-1.1.0"), "1");
    let set = |v: &[&str]| {
        v.iter()
            .map(|s| s.to_string())
            .collect::<BTreeSet<String>>()
    };
    let binary = Resolved::from([
        ("getrandom".into(), set(&["0.2.16", "0.3.4"])),
        ("bitflags".into(), set(&["2.13.0"])),
    ]);
    let engine = Resolved::from([
        ("getrandom".into(), set(&["0.2.17", "0.3.4"])),
        ("bitflags".into(), set(&["1.3.2"])),
        ("wasm-bindgen".into(), set(&["0.2.129"])),
    ]);
    assert_eq!(
        drift(&binary, engine),
        vec![("getrandom".into(), "0.2.17".into(), "0.2.16".into())]
    );
}

#[test]
fn every_lock_it_reads_resolves_something() {
    for lock in EMBED_LOCKS {
        assert!(!resolved(lock).is_empty(), "{lock} resolves nothing");
    }
}
