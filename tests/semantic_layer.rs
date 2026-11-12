//! RFC-0016 §2 - the governed semantic layer, integration-tested against a real registry.
//!
//! Unit tests in `src/semantic.rs` cover the pure helpers with hand-built schemas; this exercises
//! the whole composition against a decode registry built from a real ERC-20 ABI, so the derived
//! footguns, the generated descriptions, and the composed schema doc are all proven end-to-end.

mod common;

use common::tape::*;

/// Build the registry + generated semantics for a scaffolded USDC nest.
fn fixture() -> (
    Vec<nuthatch::registry::TableSchema>,
    nuthatch::semantic::Semantic,
) {
    let dir = tempfile::tempdir().unwrap();
    let cfg = scaffold_nest(dir.path(), "usdc", USDC);
    let registry = nuthatch::registry::from_nest(dir.path(), &cfg).expect("build registry");
    let schema = registry.schema();
    let sem = nuthatch::semantic::generate(&schema, "usdc", "arbitrum-one");
    (schema, sem)
}

#[test]
fn generated_semantics_have_no_drift_against_their_own_registry() {
    let (schema, sem) = fixture();
    assert!(
        nuthatch::semantic::drift(&schema, &sem).is_empty(),
        "freshly generated semantics must match the registry exactly"
    );
}

#[test]
fn transfer_table_footguns_are_derived_correctly() {
    let (schema, _) = fixture();
    let transfer = schema
        .iter()
        .find(|t| t.table == "usdc__transfer")
        .expect("usdc__transfer table");
    let fg = nuthatch::semantic::derive_footguns(transfer);
    assert!(
        fg.reserved_words.contains(&"from".to_string())
            && fg.reserved_words.contains(&"to".to_string()),
        "from/to are reserved-word columns"
    );
    assert_eq!(fg.big_ints, vec!["value"], "value is the uint256 big-int");
}

#[test]
fn composed_schema_teaches_footguns_and_the_coverage_seam() {
    let (schema, sem) = fixture();
    let coverage = nuthatch::semantic::Coverage {
        sealed_through: 7,
        tip: 10,
        first_indexed: None,
        history_from_deployment: None,
    };
    let doc = nuthatch::semantic::compose(
        &schema,
        Some(&sem),
        Some(&coverage),
        &[],
        nuthatch::semantic::Gates::ALL,
    );

    // Coverage seam, stated as numbers.
    assert!(
        doc.contains("sealed_through = 7"),
        "coverage sealed_through"
    );
    assert!(doc.contains("tip = 10"), "coverage tip");
    // The two footguns an agent trips on, taught inline.
    assert!(
        doc.contains("reserved-word columns") && doc.contains("\"from\""),
        "must teach the reserved-word columns"
    );
    assert!(
        doc.contains("value → value_dec"),
        "must teach the big-int _dec companion"
    );
    // The nest's per-table meaning and the general guidance both present.
    assert!(doc.contains("usdc__transfer"), "names the real table");
    assert!(
        doc.contains("VIEWS"),
        "carries the general guidance appendix"
    );
}
