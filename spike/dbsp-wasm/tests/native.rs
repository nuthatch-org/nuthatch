//! The native half of the S0 gate: the same circuits, the same inputs, on aarch64. `run-node.mjs`
//! prints the wasm32 figures; the digests must match.

use std::time::Instant;

#[test]
fn steps() {
    let (steps, parity) = dbsp_wasm_spike::run_steps().unwrap();
    assert_eq!(steps[0], vec![(2, 1), (4, 1), (6, 1)]);
    assert_eq!(steps[1], vec![(4, -1), (8, 1)]);
    assert_eq!(parity, vec![((0, 18), 1), ((1, 16), 1)], "{parity:?}");
}

#[test]
fn stress() {
    let (groups, total) = dbsp_wasm_spike::run_stress(2_000, 100).unwrap();
    eprintln!("native stress: groups={groups} total={total}");
    assert_eq!(groups, 1024);
}

#[test]
fn entity() {
    let t = Instant::now();
    let r = dbsp_wasm_spike::entity::run_entity(1_000, 100, 250, |_| {}).unwrap();
    let ms = t.elapsed().as_millis();
    eprintln!(
        "native entity: windows={} left +{} -{} right +{} -{} groups={} digest={:016x} state_bytes={:?} in {ms} ms",
        r.windows,
        r.left_inserted,
        r.left_retracted,
        r.right_inserted,
        r.right_retracted,
        r.groups,
        r.digest,
        r.state_bytes
    );
    for l in &r.first_rows {
        eprintln!("  {l}");
    }
    assert!(r.oracle_agrees);
}

/// Finds the first window at which the circuit and the oracle part, for debugging the harness.
#[test]
#[ignore]
fn entity_every_window() {
    let r = dbsp_wasm_spike::entity::run_entity(300, 100, 1, |_| {});
    eprintln!("{r:?}");
    assert!(r.is_ok());
}
