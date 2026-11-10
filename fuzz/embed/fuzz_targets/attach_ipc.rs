#![no_main]

//! The engine's `attach` against a hostile caller (#2743): `attach::supplied`, which base64-decodes
//! another instance's Arrow IPC stream from a `POST /sql` body and reads every batch of it.
//!
//! Most inputs start from a real stream, written here from fuzzed columns, then truncated or have
//! bytes overwritten, so the reader gets past the first message. Raw bytes and a raw `attach` body are
//! fuzzed alongside. An `Err` is always fine; a panic or a runaway allocation is a finding.

use arbitrary::Arbitrary;
use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use base64::Engine;
use libfuzzer_sys::fuzz_target;
use nuthatch_embed::attach::{supplied, Declared};
use serde_json::{json, Value};
use std::sync::Arc;

#[derive(Arbitrary, Debug)]
enum Bytes {
    Raw(Vec<u8>),
    Stream {
        ints: Vec<Option<i64>>,
        texts: Vec<Option<String>>,
        blobs: Vec<Vec<u8>>,
        batches: u8,
        truncate: Option<u16>,
        overwrite: Vec<(u16, u8)>,
    },
}

#[derive(Arbitrary, Debug)]
struct Input {
    bytes: Bytes,
    as_of: Option<u64>,
    answer_as_of: Option<u64>,
    answer_as_text: bool,
    raw_body: Vec<u8>,
}

fn stream(
    ints: &[Option<i64>],
    texts: &[Option<String>],
    blobs: &[Vec<u8>],
    batches: u8,
) -> Vec<u8> {
    let rows = ints.len().min(texts.len()).min(blobs.len()).min(256);
    let schema = Arc::new(Schema::new(vec![
        Field::new("i", DataType::Int64, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("b", DataType::Binary, false),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(ints[..rows].to_vec())),
        Arc::new(StringArray::from(texts[..rows].to_vec())),
        Arc::new(BinaryArray::from_iter_values(blobs[..rows].iter())),
    ];
    let batch = RecordBatch::try_new(schema.clone(), columns).expect("columns match the schema");
    let mut out = Vec::new();
    let mut w = StreamWriter::try_new(&mut out, &schema).expect("writer");
    for _ in 0..batches % 4 {
        w.write(&batch).expect("write");
    }
    w.finish().expect("finish");
    drop(w);
    out
}

fuzz_target!(|input: Input| {
    let declared = vec![Declared {
        name: "pools".into(),
        tables: vec!["t".into()],
    }];
    let bytes = match &input.bytes {
        Bytes::Raw(b) => b.clone(),
        Bytes::Stream {
            ints,
            texts,
            blobs,
            batches,
            truncate,
            overwrite,
        } => {
            let mut b = stream(ints, texts, blobs, *batches);
            for &(at, x) in overwrite.iter().take(16) {
                if !b.is_empty() {
                    let n = b.len();
                    b[at as usize % n] = x;
                }
            }
            if let Some(t) = truncate {
                b.truncate(*t as usize);
            }
            b
        }
    };
    let answer = json!({"provenance": {"as_of": input.answer_as_of}});
    let answer = if input.answer_as_text {
        Value::String(answer.to_string())
    } else {
        answer
    };
    let given = json!({"pools": {"t": {
        "arrow": base64::engine::general_purpose::STANDARD.encode(&bytes),
        "answer": answer,
    }}});
    let _ = supplied(&declared, &given, input.as_of.or(input.answer_as_of));

    if let Ok(body) = serde_json::from_slice::<Value>(&input.raw_body) {
        let _ = supplied(&declared, &body, input.as_of);
    }
});
