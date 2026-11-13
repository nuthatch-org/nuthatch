//! `--write-token-env` as `nuthatch dev` takes it: refused where it could do nothing, and a token that is
//! missing or too short stops an address-history nest before it binds, without printing the token.

use std::process::{Command, Stdio};

mod common;

fn dev(dir: &std::path::Path, extra: &[&str], env: &[(&str, &str)]) -> (bool, String) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_nuthatch"));
    cmd.arg("dev")
        .arg("--dir")
        .arg(dir)
        .args(extra)
        .env("DO_NOT_TRACK", "1")
        .stdin(Stdio::null());
    for (k, v) in env {
        cmd.env(k, v);
    }
    let out = cmd.output().expect("run nuthatch");
    (
        out.status.success(),
        format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        ),
    )
}

fn address_nest(dir: &std::path::Path) {
    std::fs::write(
        dir.join("nuthatch.toml"),
        "[nest]\nname = \"r\"\nchain = \"mainnet\"\nchain_id = 1\nrpc_urls = [\"http://127.0.0.1:1\"]\n\n\
         [address_history]\naddresses = []\n",
    )
    .unwrap();
}

#[test]
fn the_write_token_flag_is_refused_on_an_event_nest_and_in_a_runtime() {
    let nest = tempfile::tempdir().unwrap();
    common::tape::scaffold_nest(nest.path(), "usdc", common::tape::USDC);
    let (ok, out) = dev(nest.path(), &["--write-token-env", "NEST_TOKEN"], &[]);
    assert!(!ok);
    assert!(
        out.contains("--write-token-env serves an address-history nest"),
        "{out}"
    );

    let runtime = tempfile::tempdir().unwrap();
    std::fs::write(runtime.path().join("mounts.toml"), "").unwrap();
    let (ok, out) = dev(runtime.path(), &["--write-token-env", "NEST_TOKEN"], &[]);
    assert!(!ok);
    assert!(
        out.contains("--write-token-env serves an address-history nest, which runs alone"),
        "{out}"
    );
}

#[test]
fn an_address_history_nest_refuses_a_missing_short_or_pointless_token_at_startup() {
    let dir = tempfile::tempdir().unwrap();
    address_nest(dir.path());
    let flag = ["--write-token-env", "NEST_TOKEN", "--listen", "127.0.0.1:0"];
    let (ok, out) = dev(dir.path(), &flag, &[]);
    assert!(!ok);
    assert!(out.contains("NEST_TOKEN is unset or empty"), "{out}");

    let (ok, out) = dev(dir.path(), &flag, &[("NEST_TOKEN", "hunter2")]);
    assert!(!ok);
    assert!(out.contains("at least 16"), "{out}");
    assert!(!out.contains("hunter2"), "the token was printed: {out}");

    let long = "0123456789abcdef0123456789abcdef";
    let (ok, out) = dev(
        dir.path(),
        &["--write-token-env", "NEST_TOKEN", "--listen", "0.0.0.0:0"],
        &[("NEST_TOKEN", long)],
    );
    assert!(!ok);
    assert!(out.contains("does nothing off a loopback bind"), "{out}");
    assert!(!out.contains(long), "the token was printed: {out}");
}
