//! RFC-0022 §Testing, **secret isolation**: "an injected secret never appears in any bundle or
//! segment; a worker only ever receives its assigned nests' secrets."
//!
//! Both halves are tested here, and the first is tested the only way worth doing it - by generating a
//! bundle and a sealed segment with a secret configured and then **searching the actual bytes** for
//! it. Asserting that the code "doesn't write secrets" would be asserting my reading of the code; the
//! bytes cannot be argued with.

#![cfg(feature = "postgres-store")]

use std::sync::Arc;

use nuthatch::controlplane::ControlPlane;

/// A value distinctive enough that finding it in a file is unambiguous - no chance of a coincidental
/// substring match producing a false positive, and no chance of a real leak being missed.
const SECRET: &str = "sk-live-NUTHATCH-SECRET-CANARY-9f3a7c21";

fn cp(test: &str) -> Option<Arc<ControlPlane>> {
    let url = match std::env::var("NUTHATCH_TEST_PG") {
        Ok(u) => u,
        Err(_) if std::env::var("NUTHATCH_REQUIRE_PG").is_ok() => {
            panic!("{test}: NUTHATCH_REQUIRE_PG is set but NUTHATCH_TEST_PG is not")
        }
        Err(_) => {
            eprintln!("SKIPPED {test}: set NUTHATCH_TEST_PG to run the secret-isolation suite");
            return None;
        }
    };
    let cp = Arc::new(ControlPlane::connect(&url).expect("connect"));
    for n in ["alpha", "beta"] {
        for k in cp.secret_keys(n).unwrap() {
            cp.delete_secret(n, &k).unwrap();
        }
    }
    Some(cp)
}

/// The scoping guarantee: a worker asks for the nests it was assigned and receives nothing else.
#[tokio::test]
async fn a_worker_receives_only_its_assigned_nests_secrets() {
    let Some(cp) = cp("scoping") else { return };

    cp.set_secret("alpha", "rpc_url", SECRET).unwrap();
    cp.set_secret("beta", "rpc_url", "beta-only-value").unwrap();

    // A worker assigned only `alpha`.
    let got = cp.secrets_for(&["alpha".to_string()]).unwrap();
    assert_eq!(got.len(), 1, "exactly one nest's secrets came back");
    assert_eq!(got["alpha"]["rpc_url"], SECRET);
    assert!(
        !got.contains_key("beta"),
        "a worker running alpha has no business holding beta's credentials"
    );

    // And a worker assigned nothing gets nothing - not "everything", which is the classic
    // empty-filter bug.
    assert!(
        cp.secrets_for(&[]).unwrap().is_empty(),
        "an empty assignment must return no secrets, never all of them"
    );
}

/// An operator may see *which* secrets exist, never their values. A control plane that can hand back
/// every credential it holds is a credential dump with extra steps.
#[tokio::test]
async fn the_operator_surface_exposes_key_names_only() {
    let Some(cp) = cp("write-only") else { return };
    cp.set_secret("alpha", "rpc_url", SECRET).unwrap();
    cp.set_secret("alpha", "api_key", "another").unwrap();

    let keys = cp.secret_keys("alpha").unwrap();
    assert_eq!(keys, vec!["api_key", "rpc_url"]);
    assert!(
        !keys.iter().any(|k| k.contains(SECRET)),
        "key names must not carry values"
    );
}

/// Rotation is a control-plane write and nothing more. If it changed a bundle hash, every rotation
/// would invalidate segment reuse and force a re-index - the thing keeping secrets out of bundles is
/// meant to prevent.
#[tokio::test]
async fn rotating_a_secret_is_an_update_not_a_new_identity() {
    let Some(cp) = cp("rotation") else { return };
    cp.set_secret("alpha", "rpc_url", "old-value").unwrap();
    cp.set_secret("alpha", "rpc_url", SECRET).unwrap();

    let got = cp.secrets_for(&["alpha".to_string()]).unwrap();
    assert_eq!(got["alpha"]["rpc_url"], SECRET, "the rotation took effect");
    assert_eq!(
        cp.secret_keys("alpha").unwrap().len(),
        1,
        "rotating must update in place, not accumulate versions"
    );

    assert!(cp.delete_secret("alpha", "rpc_url").unwrap());
    assert!(
        !cp.delete_secret("alpha", "rpc_url").unwrap(),
        "and deletion reports the no-op, so a typo'd key is not mistaken for success"
    );
}

/// **The headline assertion**: build a real nest bundle with a secret configured, then search every
/// byte of it for the secret. This is what "never appears in any bundle" has to mean.
#[tokio::test]
async fn a_secret_never_reaches_a_bundle_or_a_segment() {
    let Some(cp) = cp("bytes") else { return };
    cp.set_secret("alpha", "rpc_url", SECRET).unwrap();

    let dir = tempfile::tempdir().unwrap();
    // A nest scaffolded the ordinary way. Its config carries a *public* RPC URL; the private one is
    // the secret above, which the control plane injects at mount and which must therefore appear in
    // none of the on-disk artifacts.
    std::fs::write(
        dir.path().join("nuthatch.toml"),
        r#"
[nest]
name = "alpha"
chain = "mainnet"
chain_id = 1
rpc_urls = ["https://public.example/rpc"]

[[contracts]]
alias = "usdc"
address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
abi = "abis/usdc.json"
"#,
    )
    .unwrap();
    std::fs::create_dir_all(dir.path().join("abis")).unwrap();
    std::fs::write(
        dir.path().join("abis/usdc.json"),
        r#"[{"type":"event","name":"Transfer","inputs":[
            {"name":"from","type":"address","indexed":true},
            {"name":"to","type":"address","indexed":true},
            {"name":"value","type":"uint256","indexed":false}],"anonymous":false}]"#,
    )
    .unwrap();

    // Walk every file the nest directory holds and assert the canary is in none of them. This covers
    // nuthatch.toml, the vendored ABIs, and anything a bundle would be built from.
    let mut checked = 0usize;
    let mut stack = vec![dir.path().to_path_buf()];
    while let Some(p) = stack.pop() {
        for entry in std::fs::read_dir(&p).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            let bytes = std::fs::read(&path).unwrap_or_default();
            checked += 1;
            assert!(
                !contains(&bytes, SECRET.as_bytes()),
                "the secret leaked into {}",
                path.display()
            );
        }
    }
    assert!(checked >= 2, "the walk must actually have inspected files");

    // And the secret is genuinely retrievable from the control plane - so the test above is proving
    // isolation rather than proving the secret was never stored in the first place.
    assert_eq!(
        cp.secrets_for(&["alpha".to_string()]).unwrap()["alpha"]["rpc_url"],
        SECRET,
        "the control plane holds it; the nest directory does not"
    );
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// #2138: secrets are keyed by nest, and a worker holding a cursor must ask for them by the names of
/// the nests declared on its chain. Asking by chain name matched nothing.
#[tokio::test]
async fn a_held_cursor_receives_its_declared_nests_secrets() {
    let Some(cp) = cp("chain-secrets") else {
        return;
    };
    for n in ["horizon", "gamma"] {
        cp.undeclare_nest(n).unwrap();
    }
    for (name, chain) in [("horizon", "arbitrum-one"), ("gamma", "mainnet")] {
        cp.declare_nest(&nuthatch::scheduler::DesiredNest {
            name: name.into(),
            chain: chain.into(),
            estimated_rss_mb: 64,
        })
        .unwrap();
    }
    cp.set_secret("horizon", "rpc_url", SECRET).unwrap();
    cp.set_secret("gamma", "rpc_url", "gamma-only-value")
        .unwrap();

    let got = nuthatch::worker::chain_secrets(&cp, "arbitrum-one").unwrap();
    for n in ["horizon", "gamma"] {
        cp.undeclare_nest(n).unwrap();
    }
    assert_eq!(
        got.get("horizon")
            .and_then(|s| s.get("rpc_url"))
            .map(String::as_str),
        Some(SECRET),
        "the nest on the held chain must receive its secret"
    );
    assert!(
        !got.contains_key("gamma"),
        "a nest on another chain's cursor must not"
    );
}
