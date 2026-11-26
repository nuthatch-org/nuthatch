//! #3093 - the partner runbook says which test covers each command. A renamed or deleted test would
//! leave that claim pointing at nothing, so this walks every `path.rs::name` the page cites and checks
//! the file holds `fn name`, and that every whole-file citation and local link resolves. It runs no
//! server and none of the cited tests.

use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn runbook() -> String {
    std::fs::read_to_string(repo_root().join("docs/partner-hosting-runbook.md"))
        .expect("docs/partner-hosting-runbook.md")
}

/// Backtick spans naming a Rust file, with the function after `::` when there is one.
fn citations(md: &str) -> Vec<(String, Option<String>)> {
    md.split('`')
        .skip(1)
        .step_by(2)
        .filter(|span| {
            let file = span.split("::").next().unwrap_or("");
            (file.starts_with("tests/") || file.starts_with("src/")) && file.ends_with(".rs")
        })
        .map(|span| match span.split_once("::") {
            Some((file, name)) => (file.to_string(), Some(name.to_string())),
            None => (span.to_string(), None),
        })
        .collect()
}

#[test]
fn every_cited_test_exists() {
    let md = runbook();
    let cited = citations(&md);
    assert!(cited.len() >= 20, "only {} citations found", cited.len());
    for (file, name) in cited {
        let path = repo_root().join(&file);
        let src =
            std::fs::read_to_string(&path).unwrap_or_else(|_| panic!("{file} does not exist"));
        if let Some(name) = name {
            assert!(
                src.contains(&format!("fn {name}(")),
                "{file} has no `fn {name}`, which the runbook cites"
            );
        }
    }
}

#[test]
fn local_links_resolve() {
    let md = runbook();
    for part in md.split("](").skip(1) {
        let target = part.split(')').next().unwrap();
        if target.starts_with("http") {
            continue;
        }
        let file = target.split('#').next().unwrap();
        assert!(
            repo_root().join("docs").join(file).exists(),
            "docs/{file} does not exist, linked from the runbook"
        );
    }
}

#[test]
fn it_does_not_claim_the_strict_isolation_review() {
    let md = runbook();
    assert!(md.contains("**Not claimed.**"));
    assert!(md.contains("FR8"));
}
