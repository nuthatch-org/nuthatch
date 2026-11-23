//! #3097: the CycloneDX SBOM attached to every release artifact, held on every PR.
//!
//! The 2026-10-08 purge (#2002) removed the last BOM tooling in this tree: RFC-0042's hand-run
//! native-BOM scripts and the test that drove one of them. They answered one question once, ran on
//! one developer's machine (#975: a developer-local path, GNU-only flags, an arbitrary report picked)
//! and nothing ran them between uses, so each use found them broken. An SBOM that only runs on a tag
//! would go the same way and fail on release day. So tools/sbom/src/sbom.rs is compiled into this
//! test and run against the real tree, for every build the release matrix ships, on every PR.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command;

#[allow(dead_code)]
#[path = "../tools/sbom/src/sbom.rs"]
mod sbom;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn host() -> String {
    let out = Command::new(std::env::var("RUSTC").unwrap_or_else(|_| "rustc".into()))
        .current_dir(root())
        .arg("-vV")
        .output()
        .expect("run rustc -vV");
    String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .find_map(|l| l.strip_prefix("host: "))
        .expect("rustc -vV names its host")
        .to_string()
}

/// The feature selections release.yml's build matrix ships, read from the workflow so a new variant
/// is covered the day it is added.
fn variants() -> Vec<Vec<String>> {
    let yml = std::fs::read_to_string(root().join(".github/workflows/release.yml")).unwrap();
    let set: BTreeSet<String> = yml
        .lines()
        .filter_map(|l| l.trim().strip_prefix("features: '"))
        .filter_map(|l| l.strip_suffix('\''))
        .map(str::to_string)
        .collect();
    assert!(
        set.len() >= 4 && set.contains(""),
        "release.yml's matrix should name the default build and at least three feature builds; \
         the scan found {set:?}"
    );
    set.iter()
        .map(|f| f.split_whitespace().map(str::to_string).collect())
        .collect()
}

fn generate(cargo_args: &[String]) -> sbom::Sbom {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    let target = host();
    sbom::generate(&sbom::Request {
        cargo: &cargo,
        manifest: &root().join("Cargo.toml"),
        target: &target,
        cargo_args,
        artifact: Some("nuthatch-test.tar.gz"),
    })
    .unwrap_or_else(|e| panic!("tools/sbom failed for {cargo_args:?}: {e}"))
}

fn allow() -> BTreeSet<String> {
    sbom::deny_allow_list(&std::fs::read_to_string(root().join("deny.toml")).unwrap()).unwrap()
}

fn names(b: &sbom::Sbom) -> BTreeSet<&str> {
    b.components.values().map(|c| c.name.as_str()).collect()
}

/// Acceptance: the licences an SBOM names are the ones deny.toml allows. cargo-deny checks the
/// default build only, so this is also the first check of the scaled, graph and flight builds'.
#[test]
fn every_release_build_names_only_licences_deny_toml_allows() {
    let allow = allow();
    for args in variants() {
        let b = generate(&args);
        assert!(
            b.components.len() > 300,
            "{args:?}: only {} components - the `cargo tree` scan has stopped matching",
            b.components.len()
        );
        assert_eq!(b.root.name, "nuthatch");
        assert_eq!(b.root.version, env!("CARGO_PKG_VERSION"));
        let refused = sbom::refused(&b, &allow);
        assert!(
            refused.is_empty(),
            "{args:?}: deny.toml does not allow these licences, yet the build links them:\n  {}",
            refused.join("\n  ")
        );
    }
}

/// Acceptance: the same commit yields a byte-identical SBOM. Nothing of the run or the runner in it:
/// no timestamp, no random serial number, no checkout path.
#[test]
fn the_sbom_is_byte_identical_and_names_no_checkout_path() {
    let a = sbom::to_json(&generate(&[]));
    let b = sbom::to_json(&generate(&[]));
    assert!(a == b, "two runs on one commit wrote different SBOMs");
    let checkout = root().canonicalize().unwrap();
    for leak in [env!("CARGO_MANIFEST_DIR"), checkout.to_str().unwrap()] {
        assert!(!a.contains(leak), "the SBOM names the checkout path {leak}");
    }
    for unstable in ["\"timestamp\"", "\"serialNumber\""] {
        assert!(!a.contains(unstable), "the SBOM carries {unstable}");
    }
    assert!(a.contains("\"bomFormat\": \"CycloneDX\"") && a.contains("\"specVersion\": \"1.5\""));
}

/// An SBOM per artifact is only worth having if each lists what that build links. Each optional
/// feature's own crates appear in its build's SBOM and not in the default one.
#[test]
fn each_build_lists_its_own_features_crates() {
    let default = generate(&[]);
    let default_names = names(&default);
    for (feature, own) in [
        ("graph", "Inflector"),
        ("flight", "tonic"),
        ("postgres-store", "postgres"),
    ] {
        let args: Vec<String> = ["--features", feature].map(String::from).to_vec();
        assert!(
            variants().contains(&args),
            "release.yml no longer ships `--features {feature}`; update this test"
        );
        assert!(
            !default_names.contains(own),
            "{own} is in the default build's SBOM"
        );
        assert!(
            names(&generate(&args)).contains(own),
            "{own} is missing from the `{feature}` build's SBOM"
        );
    }
}

/// The SBOM is read from the resolved graph, with Cargo.lock as the record of what each crate is:
/// every registry crate carries the lockfile's checksum, nothing is listed that the lockfile does not
/// hold, and the lockfile holds crates no build links (today `fastrlp`, MPL-2.0, behind a feature
/// nothing enables), which an SBOM read from the lockfile alone would have claimed.
#[test]
fn components_come_from_the_resolved_graph_and_carry_the_lockfile_checksum() {
    let lock = sbom::parse_lock(&std::fs::read_to_string(root().join("Cargo.lock")).unwrap());
    let b = generate(&[]);
    let mut unbuilt = 0;
    for l in &lock {
        if !b
            .components
            .values()
            .chain([&b.root])
            .any(|c| c.name == l.name && c.version == l.version)
        {
            unbuilt += 1;
        }
    }
    assert!(
        unbuilt > 0,
        "every Cargo.lock entry is in the SBOM - is it reading the lockfile instead of the graph?"
    );
    for c in b.components.values() {
        match &c.source {
            sbom::Source::Registry => {
                let entry = lock
                    .iter()
                    .find(|l| l.name == c.name && l.version == c.version)
                    .unwrap_or_else(|| panic!("{} {} is not in Cargo.lock", c.name, c.version));
                assert!(
                    c.checksum.is_some() && c.checksum == entry.checksum,
                    "{} {} does not carry Cargo.lock's checksum",
                    c.name,
                    c.version
                );
            }
            sbom::Source::Git { rev, .. } => assert_eq!(rev.len(), 40, "{} rev {rev}", c.name),
            sbom::Source::Path(p) => assert!(
                !Path::new(p).is_absolute() && root().join(p).join("Cargo.toml").is_file(),
                "{} has path {p}",
                c.name
            ),
            sbom::Source::Toolchain => assert_eq!(c.name, "rust-std"),
        }
    }
    // Scope: what is linked into the binary is `required`; a proc macro is only run by the build.
    let scope = |n: &str| {
        b.components
            .values()
            .find(|c| c.name == n)
            .unwrap_or_else(|| panic!("{n} is not in the SBOM"))
            .runtime
    };
    assert!(scope("serde") && scope("rust-std"));
    assert!(!scope("serde_derive"), "a proc macro is listed as linked");
}

/// The licence check reads SPDX as cargo-deny does: `OR` needs one allowed arm, `AND` every arm,
/// `WITH` must be allowed as written, and Cargo's legacy `/` means `OR`.
#[test]
fn licence_expressions_are_judged_as_deny_toml_reads_them() {
    let allow = allow();
    assert!(allow.contains("MIT") && allow.contains("Apache-2.0 WITH LLVM-exception"));
    assert!(!allow.iter().any(|l| l.contains("GPL") || l.contains("MPL")));
    let ok = |e: &str| sbom::licence_allowed(&sbom::normalise_licence(e).unwrap(), &allow);
    for yes in [
        "MIT",
        "MIT/Apache-2.0",
        "Apache-2.0 / MIT",
        "(MIT OR Apache-2.0) AND Unicode-3.0",
        "Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT",
        "MPL-2.0 OR MIT",
    ] {
        assert_eq!(ok(yes), Ok(true), "{yes}");
    }
    for no in [
        "MPL-2.0",
        "MIT AND MPL-2.0",
        "GPL-3.0-only WITH Classpath-exception-2.0",
        "(GPL-2.0 OR LGPL-2.1) AND MIT",
    ] {
        assert_eq!(ok(no), Ok(false), "{no}");
    }
    for bad in ["MIT OR", "(MIT", "MIT Apache-2.0"] {
        assert!(ok(bad).is_err(), "{bad} parsed");
    }
}
