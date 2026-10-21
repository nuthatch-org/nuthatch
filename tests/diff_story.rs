//! `story` lines and the grouped fallback in `nuthatch diff` (RFC-0071 §17), over the Euler fixture.
//! The fallback's goldens are `diff_flow`'s, which carry no semantic.toml; these add one.

use std::path::Path;

use crate::diff_flow::{assert_golden, diff, fixture, nest};

const STEPS: &[&str] = &[
    "--flow", "--from", "16818062", "--to", "16818085", "--top", "5",
];
const WHOLE: &[&str] = &["--flow", "--from", "16817996", "--to", "16818085"];

fn with_stories(dir: &Path) {
    std::fs::copy(fixture().join("stories.toml"), dir.join("semantic.toml")).unwrap();
}

#[test]
fn story_lines_tell_their_rows_and_the_rest_are_grouped() {
    let nest = nest();
    with_stories(nest.path());
    let (ok, out, err) = diff(nest.path(), STEPS);
    assert!(ok, "{err}");
    assert!(err.is_empty(), "a clean semantic.toml warns nothing: {err}");
    assert_golden("stories-range.txt", &out);
    let (ok, out, err) = diff(nest.path(), &[STEPS, &["--format", "json"]].concat());
    assert!(ok, "{err}");
    assert_golden("stories-range.json", &out);
}

fn rows(v: &serde_json::Value) -> u64 {
    v["rows"].as_u64().unwrap()
}

#[test]
fn every_row_is_counted_once_whether_told_or_grouped() {
    let nest = nest();
    with_stories(nest.path());
    let (ok, out, err) = diff(nest.path(), &[WHOLE, &["--format", "json"]].concat());
    assert!(ok, "{err}");
    let doc: serde_json::Value = serde_json::from_str(&out).unwrap();
    let tables = doc["tables"].as_array().unwrap();
    let total: u64 = tables.iter().map(rows).sum();
    assert_eq!(total, doc["rows_total"].as_u64().unwrap());
    let other: u64 = tables
        .iter()
        .filter(|t| t.get("reads").is_none())
        .map(rows)
        .sum();
    let events = doc["events"]["tables"].as_array().unwrap();
    assert_eq!(events.iter().map(rows).sum::<u64>(), other);
    let told: u64 = events
        .iter()
        .filter(|t| t["told_by"] == "story")
        .map(rows)
        .sum();
    assert_eq!(told, doc["events"]["rows_told"].as_u64().unwrap());
    assert!(told > 0 && told < other, "told {told} of {other}");
    assert_eq!(total, 6910, "S0's nest holds 6,910 rows over these blocks");
}

#[test]
fn a_story_that_does_not_fit_its_table_is_a_warning_and_its_rows_are_grouped() {
    let nest = nest();
    std::fs::write(
        nest.path().join("semantic.toml"),
        "[table.dai__approval]\nstory = \"{owner} approved {nobody}\"\n\n\
         [table.usdc__approval]\nstory = \"{owner:amount} by {spender}\"\n\n\
         [table.steth__submitted]\nstory = \"{sender} sent {amount:amount@amount}\"\n",
    )
    .unwrap();
    let (ok, out, err) = diff(nest.path(), STEPS);
    assert!(ok, "{err}");
    for (table, why) in [
        ("dai__approval", "`nobody` isn't a column of that table"),
        (
            "usdc__approval",
            "`{owner:amount}` needs an integer parameter, and `owner` is address",
        ),
        (
            "steth__submitted",
            "`@amount` names the token, so it must be an address column, and `amount` is uint256",
        ),
    ] {
        let warning = format!("the story for `{table}` is not used");
        assert!(
            err.contains(&warning) && err.contains(why),
            "{table}: {err}"
        );
        assert!(
            out.contains(&format!("Story line not used: {warning}")),
            "{out}"
        );
    }
    let doc: serde_json::Value =
        serde_json::from_str(&diff(nest.path(), &[STEPS, &["--format", "json"]].concat()).1)
            .unwrap();
    assert_eq!(doc["events"]["rows_told"], 0);
    let usdc = doc["events"]["tables"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["table"] == "usdc__approval")
        .expect("usdc approvals in range");
    assert_eq!(usdc["told_by"], "grouped");
}

/// §15 puts `story` in semantic.toml because it is outside the data identity: adding or editing one
/// moves the NID, as any authored input does, and never asks for a re-index.
#[test]
fn a_story_line_moves_the_nid_but_not_the_data_identity() {
    let nest = nest();
    let ids = |dir: &Path| {
        let m = nuthatch::blob::build_manifest(dir, None).unwrap();
        (m.nid(), m.data_identity())
    };
    let (nid0, data0) = ids(nest.path());
    with_stories(nest.path());
    let (nid1, data1) = ids(nest.path());
    let path = nest.path().join("semantic.toml");
    let edited = std::fs::read_to_string(&path)
        .unwrap()
        .replace("approved", "let");
    std::fs::write(&path, edited).unwrap();
    let (nid2, data2) = ids(nest.path());
    assert_eq!(data0, data1, "adding a story re-indexes");
    assert_eq!(data1, data2, "editing a story re-indexes");
    assert_ne!(nid0, nid1);
    assert_ne!(nid1, nid2);
    // A file that does move the data identity, as a control: the same check is not blind.
    std::fs::write(nest.path().join("abis/extra.json"), "[]").unwrap();
    assert_ne!(ids(nest.path()).1, data2);
}
