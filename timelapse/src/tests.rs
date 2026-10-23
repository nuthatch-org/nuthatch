use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray, StringViewArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use proptest::prelude::*;

use super::*;

fn spec(fold: &str, from: u64, to: u64, frames: u64, top: usize) -> Spec {
    let amount = if fold == "sum" { "delta" } else { "value" };
    Spec::parse(&format!(
        "title = \"t\"\nnest = \"n\"\nfrom_block = {from}\nto_block = {to}\nframes = {frames}\n\
         top = {top}\n[quantity]\nfold = \"{fold}\"\n\
         sql = \"SELECT block_number, log_index, k AS key, a AS {amount}, s AS sign FROM t\"\n"
    ))
    .unwrap()
}

fn shape(spec: &Spec) -> Shape {
    validate(
        &spec.quantity.sql,
        spec.quantity.fold,
        &["t".to_string()].into(),
    )
    .unwrap()
}

#[derive(Debug, Clone)]
struct Event {
    block: u64,
    log: u64,
    key: String,
    amount: i64,
    sign: i64,
}

/// Segments that tile `0..=last` at the given ends, with row counts from the events.
fn catalogue(events: &[Event], ends: &[u64], last: u64) -> Catalogue {
    let mut ends: Vec<u64> = ends.iter().copied().filter(|e| *e < last).collect();
    ends.push(last);
    ends.sort_unstable();
    ends.dedup();
    let mut from = 0;
    let mut segs = Vec::new();
    for e in ends {
        let rows = events
            .iter()
            .filter(|v| v.block >= from && v.block <= e)
            .count() as u64;
        segs.push(Segment {
            hash: format!("{from}"),
            from_block: from,
            to_block: e,
            rows,
            file: String::new(),
            provisional: false,
        });
        from = e + 1;
    }
    Catalogue {
        tables: [("t".to_string(), segs)].into(),
    }
}

fn batch(events: &[Event], fold: FoldKind, view: bool) -> RecordBatch {
    let keys: ArrayRef = if view {
        Arc::new(StringViewArray::from_iter_values(
            events.iter().map(|e| e.key.as_str()),
        ))
    } else {
        Arc::new(StringArray::from_iter_values(
            events.iter().map(|e| e.key.as_str()),
        ))
    };
    let amounts: ArrayRef = Arc::new(Int64Array::from_iter_values(
        events.iter().map(|e| e.amount),
    ));
    let schema = Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("log_index", DataType::UInt64, false),
        Field::new("key", keys.data_type().clone(), false),
        Field::new(fold.amount_column(), DataType::Int64, false),
        Field::new("sign", DataType::Int64, false),
    ]);
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt64Array::from_iter_values(
                events.iter().map(|e| e.block),
            )),
            Arc::new(UInt64Array::from_iter_values(events.iter().map(|e| e.log))),
            keys,
            amounts,
            Arc::new(Int64Array::from_iter_values(events.iter().map(|e| e.sign))),
        ],
    )
    .unwrap()
}

/// Run the pass as a driver would: each chunk's rows, cut into batches of `batch_rows`.
fn replay(
    spec: &Spec,
    cat: &Catalogue,
    chunk_rows: u64,
    batch_rows: usize,
    events: &[Event],
) -> Recording {
    let shape = shape(spec);
    let plan = Plan::new(spec, &shape, cat, chunk_rows).unwrap();
    let mut r = Replay::new(spec, &plan, &shape);
    for (n, &(lo, hi)) in plan.chunks.iter().enumerate() {
        r.begin_chunk(lo, hi);
        let rows: Vec<Event> = events
            .iter()
            .filter(|e| e.block >= lo && e.block <= hi)
            .cloned()
            .collect();
        for (m, part) in rows.chunks(batch_rows.max(1)).enumerate() {
            r.push_batch(&batch(part, spec.quantity.fold, (n + m) % 2 == 1))
                .unwrap();
        }
    }
    r.finish()
}

fn ev(block: u64, log: u64, key: &str, amount: i64) -> Event {
    Event {
        block,
        log,
        key: key.into(),
        amount,
        sign: 1,
    }
}

/// §4.3, and what Jules once read the other way: the pass starts at the first sealed block, so an
/// event before `from_block` is in the first frame, and no frame closes outside the range.
#[test]
fn history_before_from_block_is_folded_but_never_framed() {
    let s = spec("sum", 100, 199, 4, 3);
    let events = vec![ev(5, 0, "early", 40), ev(130, 0, "late", 7)];
    let cat = catalogue(&events, &[49, 99], 199);
    let rec = replay(&s, &cat, 1_000, 10, &events);
    assert_eq!(rec.frames.len(), 4);
    assert!(rec.frames.iter().all(|f| (100..=199).contains(&f.block)));
    assert_eq!(rec.frames[0].block, 124);
    assert_eq!(rec.frames[0].total, "40");
    assert_eq!(
        rec.frames[0].top,
        vec![("early".to_string(), "40".to_string())]
    );
    assert_eq!(rec.frames[1].total, "47");
}

#[test]
fn the_frame_line_is_s0s_encoding() {
    let f = Frame {
        block: 9,
        ts: None,
        live: 2,
        total: "-3".into(),
        top: vec![("0xab\"c".into(), "5".into())],
    };
    assert_eq!(
        f.line(0),
        "{\"i\":0,\"block\":9,\"ts\":null,\"live\":2,\"total\":\"-3\",\"top\":[[\"0xab\\\"c\",\"5\"]]}\n"
    );
}

#[test]
fn rows_out_of_order_are_refused() {
    let s = spec("sum", 0, 99, 1, 3);
    let events = vec![ev(10, 1, "a", 1)];
    let cat = catalogue(&events, &[], 99);
    let shape = shape(&s);
    let plan = Plan::new(&s, &shape, &cat, 1_000).unwrap();
    let mut r = Replay::new(&s, &plan, &shape);
    r.push_batch(&batch(&[ev(10, 1, "a", 1)], FoldKind::Sum, false))
        .unwrap();
    let err = r
        .push_batch(&batch(&[ev(10, 0, "a", 1)], FoldKind::Sum, false))
        .unwrap_err();
    assert!(err.to_string().contains("out of order"), "{err}");
}

#[test]
fn a_last_fold_refuses_two_values_for_one_key_in_one_log() {
    let s = spec("last", 0, 99, 1, 3);
    let events = vec![ev(10, 1, "a", 1), ev(10, 1, "a", 2)];
    let cat = catalogue(&events, &[], 99);
    let shape = shape(&s);
    let plan = Plan::new(&s, &shape, &cat, 1_000).unwrap();
    let mut r = Replay::new(&s, &plan, &shape);
    let err = r
        .push_batch(&batch(&events, FoldKind::Last, false))
        .unwrap_err();
    assert!(err.to_string().contains("two values"), "{err}");
}

#[test]
fn a_sign_of_minus_one_debits() {
    let s = spec("sum", 0, 99, 1, 3);
    let mut debit = ev(20, 0, "a", 3);
    debit.sign = -1;
    let events = vec![ev(10, 0, "a", 5), debit];
    let cat = catalogue(&events, &[], 99);
    let rec = replay(&s, &cat, 1_000, 10, &events);
    assert_eq!(rec.frames[0].top, vec![("a".to_string(), "2".to_string())]);
}

fn keys() -> impl Strategy<Value = String> {
    prop_oneof![
        (0u8..6).prop_map(|n| format!("0x{:040x}", n as u128 * 0x1_0000_0001)),
        (0u8..4).prop_map(|n| format!("k{n}")),
    ]
}

fn events() -> impl Strategy<Value = Vec<Event>> {
    prop::collection::vec((0u64..400, keys(), -50i64..50, prop::bool::ANY), 0..120).prop_map(
        move |mut raw| {
            raw.sort_by_key(|r| r.0);
            raw.into_iter()
                .enumerate()
                .map(|(i, (block, key, amount, neg))| Event {
                    block,
                    // A unique log per row, so a `last` fold's order is fully determined.
                    log: i as u64,
                    key,
                    amount,
                    sign: if neg { -1 } else { 1 },
                })
                .collect()
        },
    )
}

proptest! {
    /// Gate 4: chunk boundaries never change the frames, for either fold, however the pass is cut
    /// into chunks and each chunk into batches.
    #[test]
    fn chunk_boundaries_never_change_the_frames(
        last in prop::bool::ANY,
        events in events(),
        ends in prop::collection::vec(0u64..400, 0..12),
        from in 0u64..300,
        frames in 1u64..40,
        chunk_rows in 1u64..60,
        batch_rows in 1usize..17,
    ) {
        let fold = if last { "last" } else { "sum" };
        let events: Vec<Event> = if last {
            events.into_iter().map(|mut e| { e.amount = e.amount.abs(); e.sign = 1; e }).collect()
        } else {
            events
        };
        let to = 399;
        prop_assume!(frames <= to - from + 1);
        let s = spec(fold, from, to, frames, 3);
        let cat = catalogue(&events, &ends, to);
        let whole = replay(&s, &cat, u64::MAX, usize::MAX, &events);
        let cut = replay(&s, &cat, chunk_rows, batch_rows, &events);
        prop_assert_eq!(&cut.jsonl, &whole.jsonl);
        prop_assert_eq!(cut.frames_sha256, whole.frames_sha256);
    }
}

#[test]
fn the_property_test_sees_more_than_one_chunk() {
    let s = spec("sum", 100, 399, 30, 3);
    let events: Vec<Event> = (0..200).map(|i| ev(i * 2, i, "a", 1)).collect();
    let cat = catalogue(&events, &[50, 150, 250], 399);
    let shape = shape(&s);
    assert!(Plan::new(&s, &shape, &cat, 5).unwrap().chunks.len() > 10);
}

#[test]
fn identity_matches_what_nuthatch_identity_computes_from_the_same_inputs() {
    let files: std::collections::BTreeMap<String, Vec<u8>> = [
        (
            "nuthatch.toml".to_string(),
            b"[nest]\nname = \"n\"\nschema_version = 2\n".to_vec(),
        ),
        ("views/a.sql".to_string(), b"SELECT 1".to_vec()),
    ]
    .into();
    let got = manifest::identity(files.clone()).unwrap();
    let m = nuthatch_identity::manifest("n".into(), 2, "x".into(),
        hex::encode(nuthatch_decode::registry::DecodeRegistry::build(vec![]).unwrap().hash()),
        files,
    ).unwrap();
    assert_eq!(got.nid, m.nid());
    assert_eq!(got.data_identity, m.data_identity());
}
