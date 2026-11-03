//! `live-endpoints.yml` probes what `.github/workflows/shipped-endpoints.sh` reads out of the chain
//! registry, and runs only on a schedule. When RFC-0077 S4 moved the registry into core the script
//! kept reading the old file, extracted nothing, and the weekly job failed judging nothing (#2595).
//! This runs the script on every PR and holds its output to the registry itself.

use std::collections::BTreeSet;
use std::process::Command;

#[test]
fn the_endpoint_extractor_reads_every_endpoint_the_registry_ships() {
    let root = env!("CARGO_MANIFEST_DIR");
    let out = Command::new("bash")
        .arg(".github/workflows/shipped-endpoints.sh")
        .current_dir(root)
        .output()
        .expect("bash must be on PATH");
    assert!(
        out.status.success(),
        "the extractor failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let extracted: BTreeSet<(String, String)> = String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .map(|l| {
            let (c, u) = l.split_once('\t').expect("chain<TAB>url");
            (c.to_string(), u.to_string())
        })
        .collect();
    let shipped: BTreeSet<(String, String)> = nuthatch_core::chains::all()
        .iter()
        .flat_map(|c| {
            c.rpc_urls
                .iter()
                .map(|u| (c.name.to_string(), u.to_string()))
        })
        .collect();
    assert_eq!(extracted, shipped);
}
