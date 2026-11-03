//! What a kept store was opened under (#2619). Its cursor resumes from the last block it indexed, so
//! reopening it under other filters would skip every block at or below that one for the new filters,
//! and reopening it under `verified` would keep segments sealed under an unverified head. The store
//! records both on its first open, and a reopen under others is refused.

use crate::narrow::{FilterSpec, Privacy};
use anyhow::{bail, Result};
use nuthatch_core::redb_store::Store;
use nuthatch_core::store::LAST_BLOCK_KEY;
use serde_json::{json, Value};

const OPENED_UNDER_KEY: &str = "opened_under";

/// The filters and `verified` setting as one comparable value: filter order and the case of a value
/// change nothing about which logs are fetched, so neither changes the record.
fn scope(filters: Option<&[FilterSpec]>, verified: bool) -> Value {
    let filters = filters.map(|fs| {
        let mut out: Vec<Value> = fs
            .iter()
            .map(|f| {
                let args: serde_json::Map<String, Value> = f
                    .args
                    .iter()
                    .map(|(k, vs)| {
                        let mut vs: Vec<String> =
                            vs.iter().map(|v| v.to_ascii_lowercase()).collect();
                        vs.sort();
                        vs.dedup();
                        (k.clone(), json!(vs))
                    })
                    .collect();
                json!({
                    "contract": f.contract,
                    "event": f.event,
                    "from": f.from,
                    "args": args,
                    "public": f.privacy == Privacy::Public,
                })
            })
            .collect();
        out.sort_by_key(|v| v.to_string());
        out
    });
    json!({"filters": filters, "verified": verified})
}

/// Admit `store` for a nest opened under `filters` and `verified`, recording them on a store's first
/// open. A store that has indexed but carries no record predates it, and nothing says what it was
/// indexed under, so it is refused as a mismatch would be.
pub fn admit(store: &Store, filters: Option<&[FilterSpec]>, verified: bool) -> Result<()> {
    let now = scope(filters, verified);
    match store.get_meta(OPENED_UNDER_KEY)? {
        Some(held) => {
            let held: Value = serde_json::from_str(&held)?;
            if held != now {
                let what = match (
                    held["filters"] == now["filters"],
                    held["verified"] == now["verified"],
                ) {
                    (false, false) => "other filters and another `verified` setting",
                    (false, true) => "other filters",
                    _ => "another `verified` setting",
                };
                bail!(
                    "this store was indexed under {held} and is opened under {what} ({now}): its \
                     history would resume past blocks never fetched for them, so open it fresh"
                );
            }
        }
        None if store.get_meta(LAST_BLOCK_KEY)?.is_some() => bail!(
            "this store has indexed and records no filters or `verified` setting it was opened \
             under, so a reopen under {now} cannot be checked against them: open it fresh"
        ),
        None => store.set_meta(OPENED_UNDER_KEY, &now.to_string())?,
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn store() -> Store {
        let db = redb::Builder::new()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .unwrap();
        Store::from_db(db, None).unwrap()
    }

    fn filter(who: &str) -> FilterSpec {
        FilterSpec {
            contract: "token".into(),
            event: "Moved".into(),
            from: None,
            args: BTreeMap::from([("from".to_string(), vec![who.to_string()])]),
            privacy: Privacy::Private,
        }
    }

    #[test]
    fn a_store_reopened_under_its_own_filters_is_admitted() {
        let s = store();
        let a = [filter("0xAA")];
        admit(&s, Some(&a), false).unwrap();
        s.set_meta(LAST_BLOCK_KEY, "90").unwrap();
        admit(&s, Some(&[filter("0xaa")]), false).unwrap();
    }

    #[test]
    fn a_store_reopened_under_other_filters_is_refused() {
        let s = store();
        admit(&s, Some(&[filter("0xaa")]), false).unwrap();
        s.set_meta(LAST_BLOCK_KEY, "90").unwrap();
        let e = admit(&s, Some(&[filter("0xbb")]), false)
            .unwrap_err()
            .to_string();
        assert!(
            e.contains("other filters") && e.ends_with("open it fresh"),
            "{e}"
        );
        let e = admit(&s, None, false).unwrap_err().to_string();
        assert!(e.contains("other filters"), "{e}");
    }

    #[test]
    fn a_store_sealed_unverified_is_refused_under_verified() {
        let s = store();
        admit(&s, None, false).unwrap();
        s.set_meta(LAST_BLOCK_KEY, "90").unwrap();
        let e = admit(&s, None, true).unwrap_err().to_string();
        assert!(e.contains("another `verified` setting"), "{e}");
    }

    #[test]
    fn a_store_that_indexed_before_the_record_is_refused() {
        let s = store();
        s.set_meta(LAST_BLOCK_KEY, "90").unwrap();
        let e = admit(&s, Some(&[filter("0xaa")]), false)
            .unwrap_err()
            .to_string();
        assert!(e.ends_with("open it fresh"), "{e}");
    }

    #[test]
    fn an_empty_store_is_recorded_on_its_first_open() {
        let s = store();
        admit(&s, None, false).unwrap();
        s.set_meta(LAST_BLOCK_KEY, "90").unwrap();
        admit(&s, None, false).unwrap();
    }
}
