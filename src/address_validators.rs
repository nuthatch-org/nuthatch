//! Watched validators and the blocks the proposer catalogue proved they proposed (RFC-0063 plan C),
//! kept beside the address history in the same store, keyed by validator rather than address.
//!
//! A span is recorded for the validators that lacked it in one transaction, rows and coverage
//! together, and only while each is still watched and nothing removed rows since the span was read.
//! Coverage is a claim that every block the validator proposed in it is stored; an empty span is
//! such a claim too, made only after the whole span was proven.

use anyhow::{anyhow, bail, Context, Result};
use redb::{ReadableTable, TableDefinition};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

use crate::address_history::{incomplete, unsupported, MAX_OFFSET};
use crate::store::Store;

const WATCHED: TableDefinition<u64, u8> = TableDefinition::new("av_watched");
/// Validators removed at runtime, so the configuration that first listed one does not bring it back.
const UNWATCHED: TableDefinition<u64, u8> = TableDefinition::new("av_unwatched");
/// (validator, block) big-endian -> the stored row: `{"row": {...}, "meta": {...}}`.
const BLOCKS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("av_blocks");
/// validator -> its covered spans, as JSON `[[from, to], ...]`.
const COVERAGE: TableDefinition<u64, &[u8]> = TableDefinition::new("av_coverage");
const META: TableDefinition<&str, u64> = TableDefinition::new("av_meta");
const GENERATION: &str = "generation";

fn block_key(validator: u64, block: u64) -> [u8; 16] {
    let mut k = [0u8; 16];
    k[..8].copy_from_slice(&validator.to_be_bytes());
    k[8..].copy_from_slice(&block.to_be_bytes());
    k
}

/// One proven block of one validator, as served, and the bookkeeping that is not served: when its
/// relays were first and last asked, and whether its reward is still owed.
#[derive(Debug, Clone, PartialEq)]
pub struct Stored {
    pub block: u64,
    pub row: Map<String, Value>,
    pub meta: Map<String, Value>,
}

#[derive(Clone)]
pub struct Validators {
    store: Store,
}

fn merge(spans: &mut Vec<(u64, u64)>, (lo, hi): (u64, u64)) {
    spans.push((lo, hi));
    spans.sort_unstable();
    let mut out: Vec<(u64, u64)> = Vec::with_capacity(spans.len());
    for &(f, t) in spans.iter() {
        match out.last_mut() {
            Some(last) if f <= last.1.saturating_add(1) => last.1 = last.1.max(t),
            _ => out.push((f, t)),
        }
    }
    *spans = out;
}

fn read_spans(raw: Option<&[u8]>) -> Result<Vec<(u64, u64)>> {
    match raw {
        None => Ok(Vec::new()),
        Some(r) => serde_json::from_slice(r).context("a validator's coverage"),
    }
}

impl Validators {
    /// Open the tables and add `seed` (the configured validators) to the watched set; one removed at
    /// runtime stays removed.
    pub fn open(store: Store, seed: &[u64]) -> Result<Validators> {
        let wtx = store.database().begin_write()?;
        wtx.open_table(BLOCKS)?;
        wtx.open_table(COVERAGE)?;
        wtx.open_table(META)?;
        {
            let dropped = wtx.open_table(UNWATCHED)?;
            let mut w = wtx.open_table(WATCHED)?;
            for &v in seed {
                if dropped.get(v)?.is_none() {
                    w.insert(v, 1)?;
                }
            }
        }
        wtx.commit()?;
        Ok(Validators { store })
    }

    pub fn watched(&self) -> Result<Vec<u64>> {
        let rtx = self.store.database().begin_read()?;
        let t = rtx.open_table(WATCHED)?;
        t.iter()?.map(|e| Ok(e?.0.value())).collect()
    }

    pub fn watch(&self, validator: u64) -> Result<()> {
        let wtx = self.store.database().begin_write()?;
        wtx.open_table(WATCHED)?.insert(validator, 1)?;
        wtx.open_table(UNWATCHED)?.remove(validator)?;
        wtx.commit()?;
        Ok(())
    }

    /// Stop watching `validator`, dropping its rows and coverage. The generation moves, so a span
    /// read before the removal cannot record for it afterwards.
    pub fn unwatch(&self, validator: u64) -> Result<()> {
        let wtx = self.store.database().begin_write()?;
        {
            wtx.open_table(WATCHED)?.remove(validator)?;
            wtx.open_table(UNWATCHED)?.insert(validator, 1)?;
            wtx.open_table(COVERAGE)?.remove(validator)?;
            let mut t = wtx.open_table(BLOCKS)?;
            let keys: Vec<Vec<u8>> = t
                .range(
                    block_key(validator, 0).as_slice()..=block_key(validator, u64::MAX).as_slice(),
                )?
                .map(|e| e.map(|(k, _)| k.value().to_vec()))
                .collect::<std::result::Result<_, _>>()?;
            for k in keys {
                t.remove(k.as_slice())?;
            }
            let mut m = wtx.open_table(META)?;
            let g = m.get(GENERATION)?.map_or(0, |v| v.value());
            m.insert(GENERATION, g + 1)?;
        }
        wtx.commit()?;
        Ok(())
    }

    pub fn generation(&self) -> Result<u64> {
        let rtx = self.store.database().begin_read()?;
        Ok(rtx
            .open_table(META)?
            .get(GENERATION)?
            .map_or(0, |v| v.value()))
    }

    pub fn coverage(&self, validator: u64) -> Result<Vec<(u64, u64)>> {
        let rtx = self.store.database().begin_read()?;
        let t = rtx.open_table(COVERAGE)?;
        let raw = t.get(validator)?;
        read_spans(raw.as_ref().map(|v| v.value()))
    }

    /// The first block at or after `from` that `validator` lacks.
    pub fn next_uncovered(&self, validator: u64, from: u64) -> Result<u64> {
        Ok(self
            .coverage(validator)?
            .into_iter()
            .find(|(f, t)| *f <= from && from <= *t)
            .map_or(from, |(_, t)| t + 1))
    }

    /// Whether `validator` is covered over all of `[lo, hi]`.
    pub fn covers(&self, validator: u64, (lo, hi): (u64, u64)) -> Result<bool> {
        Ok(self
            .coverage(validator)?
            .iter()
            .any(|(f, t)| *f <= lo && hi <= *t))
    }

    /// Record `span` as complete for each of `validators`, with the blocks each proposed in it, in
    /// one transaction. Refused whole if the generation moved since `generation` was read or any of
    /// them is no longer watched: either way the claim no longer describes what is stored.
    pub fn record_span(
        &self,
        span: (u64, u64),
        validators: &[u64],
        found: &BTreeMap<u64, Vec<Stored>>,
        generation: u64,
    ) -> Result<()> {
        let wtx = self.store.database().begin_write()?;
        {
            let now = wtx
                .open_table(META)?
                .get(GENERATION)?
                .map_or(0, |v| v.value());
            if now != generation {
                bail!(
                    "a validator was removed while [{}, {}] was read; reading it again",
                    span.0,
                    span.1
                );
            }
            let watched = wtx.open_table(WATCHED)?;
            for v in validators {
                if watched.get(*v)?.is_none() {
                    bail!("validator {v} is no longer watched");
                }
            }
            let mut blocks = wtx.open_table(BLOCKS)?;
            for v in validators {
                for s in found.get(v).into_iter().flatten() {
                    if s.block < span.0 || s.block > span.1 {
                        bail!(
                            "block {} is outside the span [{}, {}]",
                            s.block,
                            span.0,
                            span.1
                        );
                    }
                    let value = serde_json::to_vec(&json!({"row": s.row, "meta": s.meta}))?;
                    blocks.insert(block_key(*v, s.block).as_slice(), value.as_slice())?;
                }
            }
            let mut cov = wtx.open_table(COVERAGE)?;
            for v in validators {
                let mut spans = read_spans(cov.get(*v)?.as_ref().map(|r| r.value()))?;
                merge(&mut spans, span);
                cov.insert(*v, serde_json::to_vec(&spans)?.as_slice())?;
            }
            // New rows or coverage change what a page walk sees: a walk begun before must restart.
            wtx.open_table(META)?.insert(GENERATION, now + 1)?;
        }
        wtx.commit()?;
        Ok(())
    }

    /// Replace one stored row (a reward or relay answer that arrived later), if its validator is still
    /// watched and the row is still there.
    pub fn update(&self, validator: u64, stored: &Stored) -> Result<()> {
        let wtx = self.store.database().begin_write()?;
        {
            if wtx.open_table(WATCHED)?.get(validator)?.is_none() {
                return Ok(());
            }
            let mut t = wtx.open_table(BLOCKS)?;
            let key = block_key(validator, stored.block);
            if t.get(key.as_slice())?.is_none() {
                return Ok(());
            }
            let value = serde_json::to_vec(&json!({"row": stored.row, "meta": stored.meta}))?;
            t.insert(key.as_slice(), value.as_slice())?;
            let mut m = wtx.open_table(META)?;
            let g = m.get(GENERATION)?.map_or(0, |v| v.value());
            m.insert(GENERATION, g + 1)?;
        }
        wtx.commit()?;
        Ok(())
    }

    /// Every stored row of every watched validator, for the retry pass.
    pub fn all(&self) -> Result<Vec<(u64, Stored)>> {
        let rtx = self.store.database().begin_read()?;
        let t = rtx.open_table(BLOCKS)?;
        t.iter()?
            .map(|e| {
                let (k, v) = e?;
                let k = k.value();
                let validator = u64::from_be_bytes(k[..8].try_into().expect("key holds 16 bytes"));
                let block = u64::from_be_bytes(k[8..].try_into().expect("key holds 16 bytes"));
                Ok((validator, decode_stored(block, v.value())?))
            })
            .collect()
    }

    /// The rows of `validator` in `[lo, hi]`, ascending.
    fn rows(&self, validator: u64, (lo, hi): (u64, u64)) -> Result<Vec<Stored>> {
        let rtx = self.store.database().begin_read()?;
        let t = rtx.open_table(BLOCKS)?;
        t.range(block_key(validator, lo).as_slice()..=block_key(validator, hi).as_slice())?
            .map(|e| {
                let (k, v) = e?;
                let block = u64::from_be_bytes(k.value()[8..].try_into().expect("16-byte key"));
                decode_stored(block, v.value())
            })
            .collect()
    }
}

fn decode_stored(block: u64, raw: &[u8]) -> Result<Stored> {
    let v: Value = serde_json::from_slice(raw).context("a stored validator block")?;
    let obj = |k: &str| -> Result<Map<String, Value>> {
        v.get(k)
            .and_then(Value::as_object)
            .cloned()
            .ok_or_else(|| anyhow!("a stored validator block without {k}"))
    };
    Ok(Stored {
        block,
        row: obj("row")?,
        meta: obj("meta")?,
    })
}

/// `account/nuthatchValidatorBlocks`: every block `validator` proposed in `[startblock, endblock]`,
/// complete or refused. `supported_from` is the first block the catalogue can prove. The answer also
/// carries `supportedFrom` and `verifiedThrough`, the end of the covered span holding the start.
pub fn respond(
    validators: &Validators,
    supported_from: u64,
    params: &std::collections::HashMap<String, String>,
) -> Value {
    let get = |k: &str| params.get(k).map(String::as_str).filter(|s| !s.is_empty());
    let num = |k: &str| -> std::result::Result<Option<u64>, String> {
        get(k)
            .map(|s| s.parse::<u64>().map_err(|_| format!("invalid {k} `{s}`")))
            .transpose()
    };
    let error = |why: String| json!({ "status": "0", "message": "NOTOK", "result": format!("Error! {why}") });
    let parsed = (|| {
        let validator = num("validator")?.ok_or("Missing or invalid validator")?;
        let start = num("startblock")?.unwrap_or(0);
        let end = num("endblock")?;
        let page = num("page")?.unwrap_or(1);
        let offset = num("offset")?.unwrap_or(crate::address_history::DEFAULT_OFFSET);
        if page == 0 || offset == 0 || offset > MAX_OFFSET {
            return Err(format!(
                "page and offset start at 1; offset is at most {MAX_OFFSET}"
            ));
        }
        let desc = match get("sort") {
            None | Some("asc") => false,
            Some("desc") => true,
            Some(s) => return Err(format!("invalid sort `{s}`")),
        };
        Ok((
            validator,
            start,
            end,
            page,
            offset,
            desc,
            num("generation")?,
        ))
    })();
    let (validator, start, end, page, offset, desc, generation) = match parsed {
        Ok(p) => p,
        Err(e) => return error(e),
    };
    if start < supported_from {
        let mut v = unsupported(&format!(
            "proposals are proven from block {supported_from} (history starts at block {supported_from}); ask from there and use another source before it"
        ));
        v["supportedFrom"] = json!(supported_from.to_string());
        return v;
    }
    let answer = (|| -> Result<Value> {
        // The generation is read first and again last: a removal or late update between the two
        // reads the answer again rather than serving a mix.
        let before = validators.generation()?;
        if page > 1 && generation.is_none() {
            return Ok(incomplete(
                "pages after the first carry the generation page 1 returned",
            ));
        }
        if generation.is_some_and(|g| g != before) {
            return Ok(incomplete(
                "the validator's blocks changed between pages; start again from page 1",
            ));
        }
        if !validators.watched()?.contains(&validator) {
            return Ok(incomplete(&format!("validator {validator} is not watched")));
        }
        let through = validators
            .coverage(validator)?
            .into_iter()
            .find(|(f, t)| *f <= start && start <= *t)
            .map(|(_, t)| t);
        let Some(through) = through else {
            return Ok(incomplete(&format!(
                "validator {validator}'s proposals are not proven from block {start} yet"
            )));
        };
        let end = end.unwrap_or(through);
        if end < start {
            return Ok(json!({"status": "0", "message": "No transactions found", "result": []}));
        }
        if end > through {
            let mut v = incomplete(&format!(
                "validator {validator}'s proposals are proven through block {through}, not {end}"
            ));
            v["verifiedThrough"] = json!(through.to_string());
            return Ok(v);
        }
        let mut rows = validators.rows(validator, (start, end))?;
        if desc {
            rows.reverse();
        }
        let skip = usize::try_from((page - 1).saturating_mul(offset)).unwrap_or(usize::MAX);
        let take = usize::try_from(offset).unwrap_or(usize::MAX);
        let result: Vec<Value> = rows
            .into_iter()
            .skip(skip)
            .take(take)
            .map(|s| Value::Object(s.row))
            .collect();
        if validators.generation()? != before {
            return Ok(incomplete(
                "the validator's blocks changed while they were read; ask again",
            ));
        }
        let mut v = crate::address_history::ok(Value::Array(result), before);
        v["supportedFrom"] = json!(supported_from.to_string());
        v["verifiedThrough"] = json!(through.to_string());
        Ok(v)
    })();
    answer.unwrap_or_else(|e| error(format!("{e:#}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn open(seed: &[u64]) -> (tempfile::TempDir, Validators) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        (dir, Validators::open(store, seed).unwrap())
    }

    fn stored(block: u64) -> Stored {
        let mut row = Map::new();
        row.insert("blockNumber".into(), json!(block.to_string()));
        Stored {
            block,
            row,
            meta: Map::new(),
        }
    }

    fn ask(v: &Validators, q: &[(&str, &str)]) -> Value {
        let q: HashMap<String, String> = q
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        respond(v, 100, &q)
    }

    #[test]
    fn a_covered_empty_span_is_a_complete_empty_answer_and_beyond_it_is_refused() {
        let (_d, v) = open(&[7]);
        let g = v.generation().unwrap();
        v.record_span((100, 199), &[7], &BTreeMap::new(), g)
            .unwrap();
        let a = ask(
            &v,
            &[
                ("validator", "7"),
                ("startblock", "100"),
                ("endblock", "199"),
            ],
        );
        assert_eq!(a["status"], "1");
        assert_eq!(a["result"], json!([]));
        assert_eq!(a["verifiedThrough"], "199");
        let a = ask(
            &v,
            &[
                ("validator", "7"),
                ("startblock", "100"),
                ("endblock", "200"),
            ],
        );
        assert!(
            a["result"]
                .as_str()
                .unwrap()
                .starts_with("NUTHATCH_INCOMPLETE"),
            "{a}"
        );
        let a = ask(&v, &[("validator", "7"), ("startblock", "99")]);
        assert!(
            a["result"]
                .as_str()
                .unwrap()
                .starts_with("NUTHATCH_UNSUPPORTED"),
            "{a}"
        );
    }

    #[test]
    fn an_uncovered_or_unwatched_validator_is_refused() {
        let (_d, v) = open(&[7]);
        let a = ask(&v, &[("validator", "7"), ("startblock", "100")]);
        assert!(
            a["result"]
                .as_str()
                .unwrap()
                .starts_with("NUTHATCH_INCOMPLETE"),
            "{a}"
        );
        let a = ask(&v, &[("validator", "8"), ("startblock", "100")]);
        assert!(a["result"].as_str().unwrap().contains("not watched"), "{a}");
    }

    #[test]
    fn a_removal_while_a_span_is_read_refuses_its_record() {
        let (_d, v) = open(&[7, 8]);
        let g = v.generation().unwrap();
        v.unwatch(8).unwrap();
        v.watch(8).unwrap();
        let mut found = BTreeMap::new();
        found.insert(8, vec![stored(150)]);
        assert!(v.record_span((100, 199), &[7, 8], &found, g).is_err());
        assert!(
            v.coverage(7).unwrap().is_empty(),
            "nothing of the span is kept"
        );
    }

    #[test]
    fn rows_page_in_order_and_a_changed_generation_refuses_the_next_page() {
        let (_d, v) = open(&[7]);
        let g = v.generation().unwrap();
        let mut found = BTreeMap::new();
        found.insert(7, vec![stored(120), stored(110), stored(130)]);
        v.record_span((100, 199), &[7], &found, g).unwrap();
        let first = ask(
            &v,
            &[
                ("validator", "7"),
                ("startblock", "100"),
                ("endblock", "199"),
                ("offset", "2"),
            ],
        );
        let blocks: Vec<&str> = first["result"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["blockNumber"].as_str().unwrap())
            .collect();
        assert_eq!(blocks, ["110", "120"]);
        let gen = first["generation"].as_str().unwrap().to_string();
        v.update(7, &stored(130)).unwrap();
        let second = ask(
            &v,
            &[
                ("validator", "7"),
                ("startblock", "100"),
                ("endblock", "199"),
                ("offset", "2"),
                ("page", "2"),
                ("generation", &gen),
            ],
        );
        assert!(
            second["result"]
                .as_str()
                .unwrap()
                .contains("changed between pages"),
            "{second}"
        );
    }

    #[test]
    fn coverage_extended_between_pages_restarts_the_walk() {
        let (_d, v) = open(&[7]);
        let g = v.generation().unwrap();
        let mut found = BTreeMap::new();
        found.insert(7, vec![stored(110), stored(120), stored(130)]);
        v.record_span((100, 149), &[7], &found, g).unwrap();
        let first = ask(
            &v,
            &[
                ("validator", "7"),
                ("startblock", "100"),
                ("sort", "desc"),
                ("offset", "2"),
            ],
        );
        assert_eq!(first["status"], "1", "{first}");
        let gen = first["generation"].as_str().unwrap().to_string();
        let mut later = BTreeMap::new();
        later.insert(7, vec![stored(160)]);
        v.record_span((150, 199), &[7], &later, v.generation().unwrap())
            .unwrap();
        let second = ask(
            &v,
            &[
                ("validator", "7"),
                ("startblock", "100"),
                ("sort", "desc"),
                ("offset", "2"),
                ("page", "2"),
                ("generation", &gen),
            ],
        );
        assert!(
            second["result"]
                .as_str()
                .unwrap()
                .contains("changed between pages"),
            "{second}"
        );
        let bare = ask(
            &v,
            &[
                ("validator", "7"),
                ("startblock", "100"),
                ("offset", "2"),
                ("page", "2"),
            ],
        );
        assert!(
            bare["result"]
                .as_str()
                .unwrap()
                .contains("carry the generation"),
            "{bare}"
        );
    }

    #[test]
    fn unwatching_drops_rows_and_coverage_and_the_seed_does_not_bring_it_back() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.redb");
        {
            let v = Validators::open(Store::open(&path).unwrap(), &[7]).unwrap();
            let g = v.generation().unwrap();
            let mut found = BTreeMap::new();
            found.insert(7, vec![stored(150)]);
            v.record_span((100, 199), &[7], &found, g).unwrap();
            v.unwatch(7).unwrap();
            assert!(v.coverage(7).unwrap().is_empty());
            assert!(v.all().unwrap().is_empty());
        }
        let v = Validators::open(Store::open(&path).unwrap(), &[7]).unwrap();
        assert!(v.watched().unwrap().is_empty());
    }

    #[test]
    fn the_next_uncovered_block_follows_the_span_holding_the_start() {
        let (_d, v) = open(&[7]);
        assert_eq!(v.next_uncovered(7, 100).unwrap(), 100);
        let g = v.generation().unwrap();
        v.record_span((100, 149), &[7], &BTreeMap::new(), g)
            .unwrap();
        assert_eq!(v.next_uncovered(7, 100).unwrap(), 150);
        assert_eq!(v.next_uncovered(7, 200).unwrap(), 200);
    }

    #[test]
    fn coverage_spans_merge() {
        let mut s = vec![(1, 5)];
        merge(&mut s, (6, 9));
        merge(&mut s, (20, 30));
        assert_eq!(s, vec![(1, 9), (20, 30)]);
    }
}
