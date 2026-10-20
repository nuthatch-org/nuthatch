//! The compliance-pack manifest (RFC-0008 C6): a signed declaration of a nest's decode registry,
//! flag thresholds and alert sinks - the trust interface between an operator and its auditor.
//! `pack build` assembles it from the nest's config; `pack verify` checks it against a trusted key.

use anyhow::{anyhow, bail, Context, Result};
use ed25519_dalek::{Signer, SigningKey, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

pub const PACK_FILE: &str = "compliance-pack.toml";

/// The full manifest as written to `compliance-pack.toml`: the signed body plus an optional signature.
#[derive(Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub pack: Body,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<Signature>,
}

/// The signed portion. Field order is fixed (serde serialises structs in declaration order), so its
/// canonical JSON encoding - what we sign - is deterministic.
///
/// Unknown fields are refused: an older manifest can carry `screening` and `components`
/// entries this build no longer checks, and verifying it while ignoring them would overstate it.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Body {
    pub name: String,
    pub created: String,
    /// The decode-registry content hash - the data model the annotations were computed against.
    pub registry_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flags: Option<FlagsEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub alerts: Vec<AlertEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FlagsEntry {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threshold: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub velocity_amount: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub velocity_window: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AlertEntry {
    pub kinds: Vec<String>,
    pub url: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Signature {
    pub pubkey: String,
    pub sig: String,
}

/// An ed25519 keypair, stored in a local JSON file (no key service - the RFC's constraint).
#[derive(Serialize, Deserialize)]
struct KeyFile {
    secret: String,
    public: String,
}

/// `nuthatch pack keygen --out <file>` - generate a signing keypair into a local JSON file.
pub fn keygen(out: &Path) -> Result<()> {
    let mut seed = [0u8; 32];
    getrandom::fill(&mut seed).map_err(|e| anyhow!("OS randomness unavailable: {e}"))?;
    let sk = SigningKey::from_bytes(&seed);
    let kf = KeyFile {
        secret: hex::encode(sk.to_bytes()),
        public: hex::encode(sk.verifying_key().to_bytes()),
    };
    std::fs::write(out, serde_json::to_string_pretty(&kf)?)
        .with_context(|| format!("cannot write key file {}", out.display()))?;
    println!(
        "✓ wrote keypair to {} (public {})",
        out.display(),
        kf.public
    );
    println!("  keep the secret safe; distribute the public key so auditors can verify your packs");
    Ok(())
}

fn load_signing_key(path: &Path) -> Result<SigningKey> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("cannot read key file {}", path.display()))?;
    let kf: KeyFile = serde_json::from_str(&raw).context("corrupt key file")?;
    let bytes: [u8; 32] = hex::decode(&kf.secret)
        .ok()
        .and_then(|b| b.try_into().ok())
        .ok_or_else(|| anyhow!("key file secret is not 32 hex bytes"))?;
    Ok(SigningKey::from_bytes(&bytes))
}

/// The bytes we sign / verify: the body's canonical JSON. Deterministic because struct field order is
/// fixed and every value is a string/int/array (no float or map-ordering ambiguity).
fn signing_bytes(body: &Body) -> Result<Vec<u8>> {
    serde_json::to_vec(body).context("failed to canonicalise manifest body")
}

/// `nuthatch pack build [--key <file>]` - assemble the manifest from the nest's config, sign it if a
/// key is given, and write `compliance-pack.toml`.
pub fn build(dir: &Path, key: Option<&Path>, created: &str) -> Result<()> {
    let body = describe(dir, created)?;

    let signature = match key {
        Some(k) => {
            let sk = load_signing_key(k)?;
            let sig = sk.sign(&signing_bytes(&body)?);
            Some(Signature {
                pubkey: hex::encode(sk.verifying_key().to_bytes()),
                sig: hex::encode(sig.to_bytes()),
            })
        }
        None => None,
    };

    let manifest = Manifest {
        pack: body,
        signature,
    };
    let toml = toml::to_string_pretty(&manifest).context("failed to serialise manifest")?;
    let path = dir.join(PACK_FILE);
    std::fs::write(&path, toml).with_context(|| format!("cannot write {}", path.display()))?;
    println!("✓ wrote {}", path.display());
    if key.is_some() {
        println!("  signed - auditors can `nuthatch pack verify --pubkey <your public key>`");
    } else {
        println!("  unsigned - `pack verify` refuses it; pass --key <file> to sign (see `nuthatch pack keygen`)");
    }
    Ok(())
}

/// The body a pack for the nest at `dir` carries: what `build` signs and `verify` compares against.
fn describe(dir: &Path, created: &str) -> Result<Body> {
    let config = crate::config::Config::load(dir)?;
    let registry = crate::registry::from_nest(dir, &config)?;

    let flags = if config.flags.threshold.is_some()
        || config.flags.velocity_amount.is_some()
        || config.flags.velocity_window.is_some()
    {
        Some(FlagsEntry {
            threshold: config.flags.threshold.clone(),
            velocity_amount: config.flags.velocity_amount.clone(),
            velocity_window: config.flags.velocity_window,
        })
    } else {
        None
    };

    let alerts = config
        .alerts
        .iter()
        .map(|a| AlertEntry {
            kinds: a.kinds.clone(),
            url: a.url.clone(),
        })
        .collect();

    Ok(Body {
        name: config.nest.name.clone(),
        created: created.to_string(),
        registry_hash: hex::encode(registry.hash()),
        flags,
        alerts,
    })
}

/// The outcome of `pack verify`, so it's testable without parsing stdout.
#[derive(Debug, Default)]
pub struct VerifyReport {
    /// `None` when the pack is unsigned, which fails: a pack nobody signed vouches for nothing.
    pub signature: Option<bool>,
    /// `None` when no `nuthatch.toml` sits beside the pack, so there was no nest to compare it with.
    pub describes_nest: Option<bool>,
    pub problems: Vec<String>,
}

impl VerifyReport {
    pub fn ok(&self) -> bool {
        self.problems.is_empty() && self.signature == Some(true)
    }
}

/// A public key as an auditor is handed it: 64 hex characters, or a file holding them (or holding a
/// `pack keygen` key file's `public`).
pub fn parse_pubkey(arg: &str) -> Result<VerifyingKey> {
    let text = match std::fs::read_to_string(arg) {
        Ok(raw) => match serde_json::from_str::<Value>(&raw) {
            Ok(v) => v["public"]
                .as_str()
                .ok_or_else(|| anyhow!("{arg} has no `public` key"))?
                .to_string(),
            Err(_) => raw.trim().to_string(),
        },
        Err(_) => arg.trim().to_string(),
    };
    let pk: [u8; 32] = hex::decode(text.strip_prefix("0x").unwrap_or(&text))
        .ok()
        .and_then(|b| b.try_into().ok())
        .ok_or_else(|| anyhow!("--pubkey is not 32 hex bytes or a file holding them"))?;
    VerifyingKey::from_bytes(&pk).context("invalid public key")
}

/// `nuthatch pack verify --pubkey <key>` - check the signature against the key the auditor was given
/// (never the one the pack carries), and that the pack still describes the nest beside it.
pub fn verify(dir: &Path, trusted: &VerifyingKey) -> Result<VerifyReport> {
    let path = dir.join(PACK_FILE);
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("no {} - run `nuthatch pack build` first", path.display()))?;
    let manifest: Manifest = toml::from_str(&raw).context("corrupt compliance-pack.toml")?;
    let mut report = VerifyReport::default();

    match &manifest.signature {
        Some(sig) => {
            let good = check_signature(&manifest.pack, sig, trusted).unwrap_or(false);
            report.signature = Some(good);
            if !good {
                report.problems.push(format!(
                    "signature does not verify against {}",
                    hex::encode(trusted.to_bytes())
                ));
            }
        }
        None => report.problems.push("the pack is unsigned".into()),
    }

    if dir.join(crate::config::CONFIG_FILE).exists() {
        let differs = describe(dir, &manifest.pack.created).and_then(|now| {
            let (a, b) = (
                serde_json::to_value(&manifest.pack)?,
                serde_json::to_value(now)?,
            );
            Ok(["name", "registry_hash", "flags", "alerts"]
                .into_iter()
                .filter(|k| a.get(k) != b.get(k))
                .collect::<Vec<_>>())
        });
        match differs {
            Ok(d) if d.is_empty() => report.describes_nest = Some(true),
            Ok(d) => {
                report.describes_nest = Some(false);
                report.problems.push(format!(
                    "the pack no longer describes this nest: {} differ",
                    d.join(", ")
                ));
            }
            Err(e) => {
                report.describes_nest = Some(false);
                report
                    .problems
                    .push(format!("cannot read the nest beside the pack: {e:#}"));
            }
        }
    }

    Ok(report)
}

fn check_signature(body: &Body, sig: &Signature, trusted: &VerifyingKey) -> Result<bool> {
    let sb: [u8; 64] = hex::decode(&sig.sig)
        .ok()
        .and_then(|b| b.try_into().ok())
        .ok_or_else(|| anyhow!("signature is not 64 hex bytes"))?;
    let signature = ed25519_dalek::Signature::from_bytes(&sb);
    Ok(trusted.verify(&signing_bytes(body)?, &signature).is_ok())
}

/// CLI entry: build/verify/keygen dispatch.
pub fn run(args: crate::cli::PackArgs, created: &str) -> Result<()> {
    match args.what {
        crate::cli::PackWhat::Keygen(a) => keygen(&PathBuf::from(&a.out)),
        crate::cli::PackWhat::Build(a) => build(
            &PathBuf::from(&a.dir),
            a.key.as_deref().map(Path::new),
            created,
        ),
        crate::cli::PackWhat::Verify(a) => {
            let report = verify(&PathBuf::from(&a.dir), &parse_pubkey(&a.pubkey)?)?;
            match report.signature {
                Some(true) => println!("✓ signature verifies against the given key"),
                Some(false) => println!("✗ signature does NOT verify against the given key"),
                None => println!("✗ unsigned manifest"),
            }
            match report.describes_nest {
                Some(true) => println!("✓ the pack describes the nest beside it"),
                Some(false) => println!("✗ the pack does NOT describe the nest beside it"),
                None => {
                    println!("· no nest config beside the pack; only the signature was checked")
                }
            }
            for p in &report.problems {
                println!("  {p}");
            }
            if report.ok() {
                println!("PASS");
                Ok(())
            } else {
                bail!("pack verification FAILED")
            }
        }
    }
}

/// The sha256 of a file's bytes - the content address used throughout the pack.
#[allow(dead_code)]
pub fn file_hash(path: &Path) -> Result<String> {
    let bytes = std::fs::read(path).with_context(|| format!("cannot read {}", path.display()))?;
    Ok(hex::encode(Sha256::digest(&bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body() -> Body {
        Body {
            name: "t".into(),
            created: "unix:1".into(),
            registry_hash: "abcd".into(),
            flags: None,
            alerts: vec![],
        }
    }

    #[test]
    fn sign_and_verify_roundtrip() {
        let mut seed = [7u8; 32];
        seed[0] = 42;
        let sk = SigningKey::from_bytes(&seed);
        let vk = sk.verifying_key();
        let b = body();
        let sig = sk.sign(&signing_bytes(&b).unwrap());
        let signature = Signature {
            pubkey: hex::encode(vk.to_bytes()),
            sig: hex::encode(sig.to_bytes()),
        };
        assert!(
            check_signature(&b, &signature, &vk).unwrap(),
            "genuine signature verifies"
        );

        // Tamper with the body → signature no longer verifies.
        let mut tampered = body();
        tampered.registry_hash = "ffff".into();
        assert!(
            !check_signature(&tampered, &signature, &vk).unwrap(),
            "tampered body fails"
        );
    }

    fn nest() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(crate::config::CONFIG_FILE),
            r#"
[nest]
name = "audit-nest"
chain = "mainnet"
chain_id = 1
rpc_urls = ["https://rpc.example"]

[[contracts]]
alias = "usdc"
address = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
abi = "abis/usdc.json"

[flags]
threshold = "1000"
"#,
        )
        .unwrap();
        std::fs::create_dir_all(dir.path().join("abis")).unwrap();
        std::fs::write(
            dir.path().join("abis/usdc.json"),
            r#"[{"type":"event","name":"Transfer","anonymous":false,"inputs":[{"name":"from","type":"address","indexed":true},{"name":"to","type":"address","indexed":true},{"name":"value","type":"uint256","indexed":false}]}]"#,
        )
        .unwrap();
        dir
    }

    fn keypair(dir: &Path, name: &str) -> (PathBuf, VerifyingKey) {
        let keyfile = dir.join(name);
        keygen(&keyfile).unwrap();
        let vk = parse_pubkey(keyfile.to_str().unwrap()).unwrap();
        (keyfile, vk)
    }

    #[test]
    fn build_then_verify_a_signed_pack() {
        let dir = nest();
        let (keyfile, vk) = keypair(dir.path(), "key.json");
        build(dir.path(), Some(&keyfile), "unix:1").unwrap();

        let report = verify(dir.path(), &vk).unwrap();
        assert_eq!(report.signature, Some(true), "signed pack verifies");
        assert_eq!(report.describes_nest, Some(true));
        assert!(report.ok(), "problems: {:?}", report.problems);
        let hex_key = parse_pubkey(&hex::encode(vk.to_bytes())).unwrap();
        assert!(verify(dir.path(), &hex_key).unwrap().ok());

        let path = dir.path().join(PACK_FILE);
        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(raw.contains("\"1000\""), "{raw}");
        std::fs::write(&path, raw.replace("\"1000\"", "\"999\"")).unwrap();
        let report = verify(dir.path(), &vk).unwrap();
        assert_eq!(report.signature, Some(false), "an edited body fails");
        assert!(!report.ok());
    }

    /// #2186: an edited pack re-signed with a fresh key carries that key, and must still fail.
    #[test]
    fn a_pack_re_signed_by_another_key_fails() {
        let dir = nest();
        let (_, trusted) = keypair(dir.path(), "operator.json");
        let (forger, _) = keypair(dir.path(), "forger.json");
        build(dir.path(), Some(&forger), "unix:1").unwrap();
        let report = verify(dir.path(), &trusted).unwrap();
        assert_eq!(report.signature, Some(false));
        assert!(!report.ok());
    }

    /// #2186: an unsigned pack vouches for nothing.
    #[test]
    fn an_unsigned_pack_fails() {
        let dir = nest();
        let (_, vk) = keypair(dir.path(), "key.json");
        build(dir.path(), None, "unix:1").unwrap();
        let report = verify(dir.path(), &vk).unwrap();
        assert_eq!(report.signature, None);
        assert!(!report.ok(), "{report:?}");
    }

    /// #2186: a genuine pack for an older config no longer describes the nest beside it.
    #[test]
    fn a_pack_that_no_longer_describes_its_nest_fails() {
        let dir = nest();
        let (keyfile, vk) = keypair(dir.path(), "key.json");
        build(dir.path(), Some(&keyfile), "unix:1").unwrap();
        let cfg = dir.path().join(crate::config::CONFIG_FILE);
        let raw = std::fs::read_to_string(&cfg).unwrap();
        std::fs::write(&cfg, raw.replace("\"1000\"", "\"5000\"")).unwrap();
        let report = verify(dir.path(), &vk).unwrap();
        assert_eq!(report.signature, Some(true));
        assert_eq!(report.describes_nest, Some(false));
        assert!(
            report.problems.iter().any(|p| p.contains("flags")),
            "{:?}",
            report.problems
        );
        assert!(!report.ok());
    }

    #[test]
    fn a_manifest_with_screening_entries_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(PACK_FILE),
            r#"
[pack]
name = "t"
created = "unix:1"
registry_hash = "abcd"

[[pack.screening]]
list_snapshot = "deadbeef"
addresses = 3
"#,
        )
        .unwrap();
        let vk = SigningKey::from_bytes(&[1u8; 32]).verifying_key();
        let err = format!("{:#}", verify(dir.path(), &vk).unwrap_err());
        assert!(err.contains("screening"), "{err}");
    }
}
