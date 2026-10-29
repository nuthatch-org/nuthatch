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
fn views_agree_across_drivers() {
    use nuthatch_core::drive::Driver;
    let want = r#"[("0xa", 70), ("0xb", 30), ("0xmint", -100)]"#;
    assert_eq!(dbsp_wasm_spike::run_views(Driver::Threaded).unwrap(), want);
    assert_eq!(dbsp_wasm_spike::run_views(Driver::Inline).unwrap(), want);
}

/// `run-node.mjs` holds the wasm32 run to this same string.
const WINDOW: &str = concat!(
    "committed=2 rows_sha256=8b6b04dba6c9e66507c3a23852d42c97a102850215b6e22b2b3c36e19eb3f491 ",
    r#"balances=[("0x1111111111111111111111111111111111111111", 60), "#,
    r#"("0x2222222222222222222222222222222222222222", 40), "#,
    r#"("0x0000000000000000000000000000000000000000", -100)] "#,
    r#"last=Some("12") sealed_through=12 hot=0 segments=["#,
    r#""tok__transfer 10..=10 rows=1 4c63483c430d40b3e42a93728158d6fbc52a9ca2f258e52269563612b722c2e7", "#,
    r#""tok__transfer 12..=12 rows=1 43ec70ec6f116bb227890db1d079a646eaa677d0ef208fb56c6e5f44e6d34a1f"]"#,
);

#[test]
fn a_window_through_core() {
    let got = dbsp_wasm_spike::window::run_window().unwrap();
    eprintln!("native window: {got}");
    assert_eq!(got, WINDOW);
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

/// RFC-0075 S2: `run-node.mjs` holds the wasm32 transcript to this digest.
const REORG_CASES: usize = 12;
const REORG_TRANSCRIPT_SHA256: &str =
    "4f3b8bec690c201973ce15e64987659f67dd912547767241f31121111e96592a";

#[test]
fn the_reorg_property_converges_in_every_band() {
    let t0 = Instant::now();
    let transcript = dbsp_wasm_spike::reorg::run_property(REORG_CASES).unwrap();
    eprintln!("{transcript}\nin {:?}", t0.elapsed());
    assert_eq!(
        nuthatch_seal::content_hash(transcript.as_bytes()),
        REORG_TRANSCRIPT_SHA256
    );
}
