//! #3097: a CycloneDX 1.5 JSON SBOM for one release artifact.
//!
//! What goes in is decided by Cargo, not by this file. Two `cargo tree` runs against the artifact's
//! own manifest, target and feature selection give the graph Cargo's resolver actually builds:
//! `-e normal,build` is everything compiled to make the artifact, `-e normal,no-proc-macro` is what
//! is linked into it. A component in the second set has scope `required`; one only in the first
//! (a build script's or a proc macro's dependency) has scope `excluded`. Cargo.lock adds what
//! `cargo tree` does not print: the registry checksum and a git dependency's full commit. The pinned
//! toolchain adds the standard library, which every artifact links.
//!
//! The lockfile alone is the wrong input. It lists every package any feature on any platform could
//! pull in, so an SBOM read from it names crates that are not in the artifact: today `fastrlp`
//! (MPL-2.0, behind a `ruint` feature nothing enables) would appear, and the SBOM would contradict
//! deny.toml, which never sees it because cargo-deny resolves the same graph this does.
//!
//! **Byte-identical for one commit, target and feature selection.** No timestamp and no
//! `serialNumber` (both optional in CycloneDX, and the second is a random UUID by convention), path
//! dependencies are written relative to the lockfile rather than to the runner's checkout, and every
//! list is sorted. The commit and the run are what the provenance attestation over this file binds.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component as PathPart, Path, PathBuf};
use std::process::Command;

/// One artifact's build, as the release workflow describes it.
pub struct Request<'a> {
    /// The `cargo` to run `cargo tree` with.
    pub cargo: &'a str,
    /// The Cargo.toml the artifact is built from. Its Cargo.lock must sit beside it.
    pub manifest: &'a Path,
    /// The target triple the artifact is built for.
    pub target: &'a str,
    /// The feature selection, passed to `cargo tree` exactly as the build passes it to `cargo build`.
    pub cargo_args: &'a [String],
    /// The released file this SBOM describes, recorded by name only.
    pub artifact: Option<&'a str>,
}

/// Where a component's source came from.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Source {
    /// crates.io.
    Registry,
    /// A git dependency: the repository URL without its query, and the full commit Cargo.lock pins.
    Git { url: String, rev: String },
    /// A path dependency, relative to the lockfile's directory, `/`-separated.
    Path(String),
    /// The Rust standard library of the toolchain that built the artifact.
    Toolchain,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Component {
    pub name: String,
    pub version: String,
    pub source: Source,
    /// The declared licence, as an SPDX expression (Cargo's legacy `/` becomes `OR`).
    pub licence: Option<String>,
    /// Cargo.lock's SHA-256 of the `.crate` file, for a registry crate.
    pub checksum: Option<String>,
    /// Linked into the artifact, rather than only run while building it.
    pub runtime: bool,
}

impl Component {
    pub fn bom_ref(&self) -> String {
        match &self.source {
            Source::Path(p) => format!("path:{p}"),
            Source::Toolchain => STD_REF.to_string(),
            _ => self
                .purl()
                .expect("registry and git components have a purl"),
        }
    }

    /// None for a path dependency: `pkg:cargo/<name>` claims a crates.io name, and a first-party
    /// crate of ours that is not published there must not be matched against one that is.
    pub fn purl(&self) -> Option<String> {
        let base = format!("pkg:cargo/{}@{}", self.name, self.version);
        match &self.source {
            Source::Registry => Some(base),
            Source::Git { url, rev } => Some(format!(
                "{base}?vcs_url={}",
                percent_encode(&format!("git+{url}@{rev}"))
            )),
            Source::Path(_) => None,
            Source::Toolchain => Some(format!("pkg:generic/{STD_REF}@{}", self.version)),
        }
    }
}

/// The toolchain the artifact was built with, from `rustc -vV` in the manifest's directory (so
/// rust-toolchain.toml's pin applies).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rustc {
    pub release: String,
    pub commit: String,
}

pub struct Sbom {
    pub root: Component,
    pub components: BTreeMap<String, Component>,
    pub edges: BTreeMap<String, BTreeSet<String>>,
    pub rustc: Rustc,
    pub target: String,
    pub cargo_args: Vec<String>,
    pub artifact: Option<String>,
}

const STD_REF: &str = "rust-std";

fn run(cmd: &mut Command) -> Result<String, String> {
    let shown = format!("{cmd:?}");
    let out = cmd
        .output()
        .map_err(|e| format!("cannot run {shown}: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "{shown} failed ({}):\n{}",
            out.status,
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    String::from_utf8(out.stdout).map_err(|e| format!("{shown} printed non-UTF-8: {e}"))
}

pub fn generate(req: &Request) -> Result<Sbom, String> {
    let manifest = req
        .manifest
        .canonicalize()
        .map_err(|e| format!("cannot resolve {}: {e}", req.manifest.display()))?;
    let dir = manifest.parent().expect("a manifest has a directory");
    let lock_path = dir.join("Cargo.lock");
    let lock_text = std::fs::read_to_string(&lock_path)
        .map_err(|e| format!("cannot read {}: {e}", lock_path.display()))?;
    let lock = parse_lock(&lock_text);

    let tree = |edges: &str, prefix: &str, format: &str| {
        let mut cmd = Command::new(req.cargo);
        cmd.current_dir(dir)
            .args(["tree", "--locked", "--manifest-path"])
            .arg(&manifest)
            .args(["--target", req.target, "-e", edges, "--prefix", prefix])
            .args(["--format", format, "--color", "never"])
            .args(req.cargo_args);
        run(&mut cmd)
    };
    let all = tree("normal,build", "depth", "{p}|{l}")?;
    let linked = tree("normal,no-proc-macro", "none", "{p}")?;

    let mut components: BTreeMap<String, Component> = BTreeMap::new();
    let mut edges: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut stack: Vec<String> = Vec::new();
    let mut root = None;
    for line in all.lines().filter(|l| !l.trim().is_empty()) {
        let line = line.strip_suffix(" (*)").unwrap_or(line);
        let digits = line.bytes().take_while(u8::is_ascii_digit).count();
        let depth: usize = line[..digits]
            .parse()
            .map_err(|_| format!("`cargo tree --prefix depth` line without a depth: {line}"))?;
        let (package, licence) = line[digits..]
            .split_once('|')
            .ok_or_else(|| format!("`cargo tree` line without a licence field: {line}"))?;
        let mut c = resolve(package, &lock, dir)?;
        c.licence = normalise_licence(licence);
        let r = c.bom_ref();
        if depth == 0 {
            if root.is_some() {
                return Err("`cargo tree` printed two roots".into());
            }
            root = Some(c.clone());
        } else {
            let parent = stack
                .get(depth - 1)
                .ok_or_else(|| format!("`cargo tree` skipped a level at: {line}"))?;
            edges.entry(parent.clone()).or_default().insert(r.clone());
        }
        edges.entry(r.clone()).or_default();
        stack.truncate(depth);
        stack.push(r.clone());
        if let Some(seen) = components.get(&r) {
            if *seen != c {
                return Err(format!("two different packages share the reference {r}"));
            }
        }
        components.insert(r, c);
    }
    let mut root = root.ok_or("`cargo tree` printed nothing")?;

    for line in linked.lines().filter(|l| !l.trim().is_empty()) {
        let line = line.strip_suffix(" (*)").unwrap_or(line);
        let r = resolve(line, &lock, dir)?.bom_ref();
        components
            .get_mut(&r)
            .ok_or_else(|| format!("{r} is linked but not in the build graph"))?
            .runtime = true;
    }
    root.runtime = true;
    let root_ref = root.bom_ref();
    components.remove(&root_ref);

    let rustc = rustc(dir)?;
    let std = Component {
        name: STD_REF.into(),
        version: rustc.release.clone(),
        source: Source::Toolchain,
        licence: Some("MIT OR Apache-2.0".into()),
        checksum: None,
        runtime: true,
    };
    edges.entry(root_ref).or_default().insert(std.bom_ref());
    edges.insert(std.bom_ref(), BTreeSet::new());
    components.insert(std.bom_ref(), std);

    Ok(Sbom {
        root,
        components,
        edges,
        rustc,
        target: req.target.to_string(),
        cargo_args: req.cargo_args.to_vec(),
        artifact: req.artifact.map(str::to_string),
    })
}

fn rustc(dir: &Path) -> Result<Rustc, String> {
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".into());
    let out = run(Command::new(rustc).current_dir(dir).arg("-vV"))?;
    let field = |k: &str| {
        out.lines()
            .find_map(|l| l.strip_prefix(k))
            .map(|v| v.trim().to_string())
            .ok_or_else(|| format!("`rustc -vV` printed no `{k}`:\n{out}"))
    };
    Ok(Rustc {
        release: field("release:")?,
        commit: field("commit-hash:")?,
    })
}

#[derive(Debug, Default)]
pub struct LockPackage {
    pub name: String,
    pub version: String,
    pub source: Option<String>,
    pub checksum: Option<String>,
}

/// The `[[package]]` tables of a Cargo.lock. The format is Cargo's own and flat: one `key = "value"`
/// per line, so this needs no TOML parser.
pub fn parse_lock(text: &str) -> Vec<LockPackage> {
    let mut out: Vec<LockPackage> = Vec::new();
    for line in text.lines() {
        if line == "[[package]]" {
            out.push(LockPackage::default());
            continue;
        }
        let (Some(p), Some((k, v))) = (out.last_mut(), line.split_once(" = \"")) else {
            continue;
        };
        let v = v.trim_end_matches('"').to_string();
        match k {
            "name" => p.name = v,
            "version" => p.version = v,
            "source" => p.source = Some(v),
            "checksum" => p.checksum = Some(v),
            _ => {}
        }
    }
    out
}

/// One `{p}` from `cargo tree`: `name vX.Y.Z[ (proc-macro)][ (source)]`, where the source is absent
/// for crates.io, a URL for git and a directory for a path dependency. Matched to exactly one
/// Cargo.lock entry, or refused.
fn resolve(package: &str, lock: &[LockPackage], lock_dir: &Path) -> Result<Component, String> {
    let bad = || format!("cannot read `cargo tree` package `{package}`");
    let p = package.trim().replacen(" (proc-macro)", "", 1);
    let p = p.as_str();
    let (name, rest) = p.split_once(' ').ok_or_else(bad)?;
    let rest = rest.strip_prefix('v').ok_or_else(bad)?;
    let (version, shown) = match rest.split_once(' ') {
        Some((v, s)) => (
            v,
            Some(
                s.strip_prefix('(')
                    .and_then(|s| s.strip_suffix(')'))
                    .ok_or_else(bad)?,
            ),
        ),
        None => (rest, None),
    };
    let named = |l: &&LockPackage| l.name == name && l.version == version;
    let candidates: Vec<&LockPackage> = match shown {
        None => lock
            .iter()
            .filter(named)
            .filter(|l| l.source.as_deref().is_some_and(is_crates_io))
            .collect(),
        Some(s) if s.starts_with('/') => lock
            .iter()
            .filter(named)
            .filter(|l| l.source.is_none())
            .collect(),
        Some(s) => {
            let want = s.split('#').next().unwrap_or(s);
            lock.iter()
                .filter(named)
                .filter(|l| {
                    l.source
                        .as_deref()
                        .and_then(|s| s.strip_prefix("git+"))
                        .is_some_and(|s| s.split('#').next() == Some(want))
                })
                .collect()
        }
    };
    let [hit] = candidates[..] else {
        return Err(format!(
            "`{package}` matches {} Cargo.lock entries, not one",
            candidates.len()
        ));
    };
    let source = match (shown, hit.source.as_deref()) {
        (Some(dir), None) => Source::Path(relative(Path::new(dir), lock_dir)?),
        (_, Some(s)) if is_crates_io(s) => Source::Registry,
        (_, Some(s)) => {
            let s = s.strip_prefix("git+").ok_or_else(bad)?;
            let (url, rev) = s.split_once('#').ok_or_else(bad)?;
            Source::Git {
                url: url.split('?').next().unwrap_or(url).to_string(),
                rev: rev.to_string(),
            }
        }
        (None, None) => return Err(bad()),
    };
    Ok(Component {
        name: name.to_string(),
        version: version.to_string(),
        checksum: match source {
            Source::Registry => hit.checksum.clone(),
            _ => None,
        },
        source,
        licence: None,
        runtime: false,
    })
}

fn is_crates_io(source: &str) -> bool {
    source == "registry+https://github.com/rust-lang/crates.io-index"
        || source == "sparse+https://index.crates.io/"
}

/// `dir` relative to `base`, `/`-separated, so the SBOM does not carry the runner's checkout path.
fn relative(dir: &Path, base: &Path) -> Result<String, String> {
    let dir = dir
        .canonicalize()
        .map_err(|e| format!("cannot resolve {}: {e}", dir.display()))?;
    let base: Vec<PathPart> = base.components().collect();
    let parts: Vec<PathPart> = dir.components().collect();
    let common = base.iter().zip(&parts).take_while(|(a, b)| a == b).count();
    let mut rel = PathBuf::new();
    for _ in common..base.len() {
        rel.push("..");
    }
    for p in &parts[common..] {
        rel.push(p.as_os_str());
    }
    let rel = rel.to_string_lossy().replace('\\', "/");
    Ok(if rel.is_empty() { ".".into() } else { rel })
}

fn percent_encode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/' | b':' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// Cargo accepts `MIT/Apache-2.0`, which is not SPDX; it means `MIT OR Apache-2.0`.
pub fn normalise_licence(declared: &str) -> Option<String> {
    let s = declared.replace('/', " OR ");
    let s = s.split_whitespace().collect::<Vec<_>>().join(" ");
    (!s.is_empty()).then_some(s)
}

/// deny.toml's `[licenses] allow` list. Read by hand for the same reason the lockfile is: the
/// shape is one quoted string per entry, and a TOML parser would be this tool's first dependency.
pub fn deny_allow_list(deny_toml: &str) -> Result<BTreeSet<String>, String> {
    let mut section = "";
    let mut in_allow = false;
    let mut out = BTreeSet::new();
    for line in deny_toml.lines() {
        let code = line.split('#').next().unwrap_or("").trim();
        if code.starts_with('[') && !in_allow {
            section = code;
            continue;
        }
        if section == "[licenses]" && !in_allow && code.starts_with("allow") {
            in_allow = true;
        }
        if in_allow {
            let mut rest = code.split_once('[').map_or(code, |(_, r)| r);
            while let Some((_, after)) = rest.split_once('"') {
                let (value, tail) = after
                    .split_once('"')
                    .ok_or("unterminated string in allow")?;
                out.insert(value.to_string());
                rest = tail;
            }
            if code.ends_with(']') {
                break;
            }
        }
    }
    if out.is_empty() {
        return Err("deny.toml has no `[licenses] allow` list".into());
    }
    Ok(out)
}

/// Whether an SPDX expression can be satisfied from `allow`: an `OR` needs one allowed arm, an
/// `AND` needs every one, and `X WITH Y` must be allowed as written, as deny.toml lists it.
pub fn licence_allowed(expr: &str, allow: &BTreeSet<String>) -> Result<bool, String> {
    let spaced = expr.replace('(', " ( ").replace(')', " ) ");
    let tokens: Vec<&str> = spaced.split_whitespace().collect();
    let mut at = 0;
    let ok = or(&tokens, &mut at, allow)?;
    if at != tokens.len() {
        return Err(format!("trailing tokens in licence `{expr}`"));
    }
    Ok(ok)
}

fn or(t: &[&str], at: &mut usize, allow: &BTreeSet<String>) -> Result<bool, String> {
    let mut ok = and(t, at, allow)?;
    while t.get(*at) == Some(&"OR") {
        *at += 1;
        ok |= and(t, at, allow)?;
    }
    Ok(ok)
}

fn and(t: &[&str], at: &mut usize, allow: &BTreeSet<String>) -> Result<bool, String> {
    let mut ok = atom(t, at, allow)?;
    while t.get(*at) == Some(&"AND") {
        *at += 1;
        ok &= atom(t, at, allow)?;
    }
    Ok(ok)
}

fn atom(t: &[&str], at: &mut usize, allow: &BTreeSet<String>) -> Result<bool, String> {
    let tok = *t.get(*at).ok_or("licence expression ends early")?;
    *at += 1;
    if tok == "(" {
        let ok = or(t, at, allow)?;
        if t.get(*at) != Some(&")") {
            return Err("unbalanced `(` in licence".into());
        }
        *at += 1;
        return Ok(ok);
    }
    if matches!(tok, ")" | "AND" | "OR" | "WITH") {
        return Err(format!("unexpected `{tok}` in licence"));
    }
    let id = tok.trim_end_matches('+');
    if t.get(*at) == Some(&"WITH") {
        let exception = *t.get(*at + 1).ok_or("`WITH` without an exception")?;
        *at += 2;
        return Ok(allow.contains(&format!("{id} WITH {exception}")));
    }
    Ok(allow.contains(id))
}

/// Every component, root and standard library included, whose licence deny.toml would refuse, with
/// the reason. Empty is the only acceptable answer.
pub fn refused(sbom: &Sbom, allow: &BTreeSet<String>) -> Vec<String> {
    std::iter::once(&sbom.root)
        .chain(sbom.components.values())
        .filter_map(|c| {
            let at = format!("{}@{}", c.name, c.version);
            match c.licence.as_deref().map(|l| licence_allowed(l, allow)) {
                None => Some(format!("{at}: declares no licence")),
                Some(Ok(true)) => None,
                Some(Ok(false)) => Some(format!("{at}: `{}`", c.licence.as_deref().unwrap())),
                Some(Err(e)) => Some(format!("{at}: {e}")),
            }
        })
        .collect()
}

enum J {
    S(String),
    N(u64),
    A(Vec<J>),
    O(Vec<(&'static str, J)>),
}

fn s(v: impl Into<String>) -> J {
    J::S(v.into())
}

fn prop(name: &str, value: impl Into<String>) -> J {
    J::O(vec![("name", s(name)), ("value", s(value))])
}

fn component_json(c: &Component, kind: &str) -> J {
    let mut o = vec![
        ("type", s(kind)),
        ("bom-ref", s(c.bom_ref())),
        ("name", s(&c.name)),
        ("version", s(&c.version)),
        ("scope", s(if c.runtime { "required" } else { "excluded" })),
    ];
    if let Some(l) = &c.licence {
        o.push(("licenses", J::A(vec![J::O(vec![("expression", s(l))])])));
    }
    if let Some(p) = c.purl() {
        o.push(("purl", s(p)));
    }
    if let Some(h) = &c.checksum {
        o.push((
            "hashes",
            J::A(vec![J::O(vec![("alg", s("SHA-256")), ("content", s(h))])]),
        ));
    }
    match &c.source {
        Source::Git { url, rev } => o.push((
            "externalReferences",
            J::A(vec![J::O(vec![
                ("type", s("vcs")),
                ("url", s(format!("{url}#{rev}"))),
            ])]),
        )),
        Source::Path(p) => o.push(("properties", J::A(vec![prop("nuthatch:path", p)]))),
        Source::Registry | Source::Toolchain => {}
    }
    J::O(o)
}

/// The CycloneDX 1.5 JSON document, pretty-printed and ending in a newline.
pub fn to_json(sbom: &Sbom) -> String {
    let mut properties = vec![
        prop("nuthatch:target", &sbom.target),
        prop("nuthatch:cargo-args", sbom.cargo_args.join(" ")),
        prop("nuthatch:rustc", &sbom.rustc.release),
        prop("nuthatch:rustc-commit", &sbom.rustc.commit),
    ];
    if let Some(a) = &sbom.artifact {
        properties.push(prop("nuthatch:artifact", a));
    }
    let doc = J::O(vec![
        ("bomFormat", s("CycloneDX")),
        ("specVersion", s("1.5")),
        ("version", J::N(1)),
        (
            "metadata",
            J::O(vec![
                (
                    "tools",
                    J::O(vec![(
                        "components",
                        J::A(vec![J::O(vec![
                            ("type", s("application")),
                            ("name", s("nuthatch-sbom")),
                            ("description", s("tools/sbom at the release commit")),
                        ])]),
                    )]),
                ),
                ("component", component_json(&sbom.root, "application")),
                ("properties", J::A(properties)),
            ]),
        ),
        (
            "components",
            J::A(
                sbom.components
                    .values()
                    .map(|c| component_json(c, "library"))
                    .collect(),
            ),
        ),
        (
            "dependencies",
            J::A(
                sbom.edges
                    .iter()
                    .map(|(r, on)| {
                        J::O(vec![
                            ("ref", s(r)),
                            ("dependsOn", J::A(on.iter().map(s).collect())),
                        ])
                    })
                    .collect(),
            ),
        ),
    ]);
    let mut out = String::new();
    write(&doc, 0, &mut out);
    out.push('\n');
    out
}

fn write(j: &J, indent: usize, out: &mut String) {
    let pad = |n: usize| "  ".repeat(n);
    match j {
        J::S(v) => {
            out.push('"');
            for ch in v.chars() {
                match ch {
                    '"' => out.push_str("\\\""),
                    '\\' => out.push_str("\\\\"),
                    '\n' => out.push_str("\\n"),
                    '\r' => out.push_str("\\r"),
                    '\t' => out.push_str("\\t"),
                    c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
                    c => out.push(c),
                }
            }
            out.push('"');
        }
        J::N(n) => out.push_str(&n.to_string()),
        J::A(items) if items.is_empty() => out.push_str("[]"),
        J::A(items) => {
            out.push_str("[\n");
            for (i, item) in items.iter().enumerate() {
                out.push_str(&pad(indent + 1));
                write(item, indent + 1, out);
                out.push_str(if i + 1 < items.len() { ",\n" } else { "\n" });
            }
            out.push_str(&pad(indent));
            out.push(']');
        }
        J::O(fields) => {
            out.push_str("{\n");
            for (i, (k, v)) in fields.iter().enumerate() {
                out.push_str(&pad(indent + 1));
                out.push_str(&format!("\"{k}\": "));
                write(v, indent + 1, out);
                out.push_str(if i + 1 < fields.len() { ",\n" } else { "\n" });
            }
            out.push_str(&pad(indent));
            out.push('}');
        }
    }
}
