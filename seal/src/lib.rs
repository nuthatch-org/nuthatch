//! The bytes nuthatch seals, and nothing about where they go.
//!
//! Extracted from the main crate's `seal.rs` for RFC-0065 S2, so a browser tab can seal a range to
//! the same content address a native nest would. The filesystem, the manifest on disk, leases and
//! the fold decision stay in the main crate; this is rows in, Parquet bytes and their hash out.

use anyhow::{Context, Result};
use arrow::array::{Array, ArrayRef, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::SortingColumn;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::sync::Arc;

/// Writer settings this crate seals with: zstd level 3, blooms on address/topic/hash columns,
/// dictionary off on near-unique 32-byte hashes, rows sorted by `(block_number, log_index)`.
pub const WRITER_PROFILE: &str = "zstd-bloom-v1";

/// A segment's content address: the sha256 of its file bytes, in lower-case hex.
pub fn content_hash(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// Sort `rows`, build the batch and write it: the one path from a table's rows to segment bytes.
pub fn seal_rows(rows: &mut [Value]) -> Result<Vec<u8>> {
    sort_rows_for_seal(rows);
    write_parquet(&rows_to_batch(rows)?)
}

/// A segment's rows back as the JSON objects `rows_to_batch` was given, so a provisional segment
/// can be folded by re-sealing (#1150). The inverse of `rows_to_batch` exactly: a `UInt64` column
/// becomes a number, a `Utf8` value a string, and a null is an absent key - which is what
/// `rows_to_batch` maps an absent key *to*, so a fold round-trips to the bytes a single seal of the
/// same rows produces.
pub fn segment_rows(bytes: bytes::Bytes) -> Result<Vec<Value>> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .context("reading segment metadata")?
        .build()
        .context("building segment reader")?;
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.context("reading segment batch")?;
        let schema = batch.schema();
        for i in 0..batch.num_rows() {
            let mut obj = serde_json::Map::new();
            for (c, field) in schema.fields().iter().enumerate() {
                let col = batch.column(c);
                if col.is_null(i) {
                    continue;
                }
                let v = match field.data_type() {
                    DataType::UInt64 => {
                        let a = col
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .context("UInt64 column of the wrong array type")?;
                        Value::from(a.value(i))
                    }
                    DataType::Utf8 => {
                        let a = col
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .context("Utf8 column of the wrong array type")?;
                        Value::from(a.value(i))
                    }
                    other => anyhow::bail!(
                        "the segment has a {other} column, which this project never writes"
                    ),
                };
                obj.insert(field.name().clone(), v);
            }
            out.push(Value::Object(obj));
        }
    }
    Ok(out)
}

/// Build an Arrow batch from a table's JSON rows. `block_number`, `log_index`, `_seq` and
/// `block_timestamp` are UInt64; every other column is Utf8 (canonical text: hex, decimal, or
/// string). The query companions `*_dec` / `*_overflow` are not written here.
pub fn rows_to_batch(rows: &[Value]) -> Result<RecordBatch> {
    let mut columns: BTreeSet<String> = BTreeSet::new();
    for r in rows {
        if let Some(obj) = r.as_object() {
            columns.extend(obj.keys().cloned());
        }
    }
    let columns: Vec<String> = columns.into_iter().collect();

    let mut fields = Vec::with_capacity(columns.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    for col in &columns {
        if col == "block_number" || col == "log_index" || col == "_seq" || col == "block_timestamp"
        {
            let vals: Vec<u64> = rows
                .iter()
                .map(|r| r.get(col).and_then(Value::as_u64).unwrap_or(0))
                .collect();
            fields.push(Field::new(col, DataType::UInt64, false));
            arrays.push(Arc::new(UInt64Array::from(vals)));
        } else {
            let vals: Vec<Option<String>> = rows
                .iter()
                .map(|r| match r.get(col) {
                    Some(Value::String(s)) => Some(s.clone()),
                    None | Some(Value::Null) => None,
                    Some(other) => Some(other.to_string()),
                })
                .collect();
            fields.push(Field::new(col, DataType::Utf8, true));
            arrays.push(Arc::new(StringArray::from(vals)));
        }
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .context("failed to build record batch")
}

/// Row order the catalogue promises: `(block_number, log_index)`. `tx_index` is not a column we
/// write, so it is not in the key. Rows sharing those coordinates use their canonical JSON as a
/// deterministic tie-breaker, so folding a provisional segment cannot make identical logical rows
/// seal to different bytes merely by presenting them in another order. Applied before
/// `rows_to_batch` so the footer sort metadata is true of the bytes, not just declared.
pub fn sort_rows_for_seal(rows: &mut [Value]) {
    rows.sort_by(|a, b| {
        let key = |r: &Value| {
            (
                r.get("block_number").and_then(Value::as_u64).unwrap_or(0),
                r.get("log_index").and_then(Value::as_u64).unwrap_or(0),
            )
        };
        key(a)
            .cmp(&key(b))
            .then_with(|| a.to_string().cmp(&b.to_string()))
    });
}

/// Columns [`WRITER_PROFILE`] gives a bloom filter.
pub fn bloom_column(name: &str) -> bool {
    matches!(
        name,
        "address"
            | "from"
            | "to"
            | "owner"
            | "spender"
            | "topic0"
            | "block_hash"
            | "tx_hash"
            | "hash"
    ) || name.ends_with("_hash")
        || name.ends_with("_address")
}

/// Columns [`WRITER_PROFILE`] writes without dictionary encoding: near-unique 32-byte hashes.
pub fn dictionary_off(name: &str) -> bool {
    matches!(name, "block_hash" | "tx_hash" | "hash") || name.ends_with("_hash")
}

/// The Parquet writer every sealed segment and offchain snapshot goes through, at [`WRITER_PROFILE`].
pub fn write_parquet(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    let zstd = ZstdLevel::try_new(3).expect("zstd level 3 is valid");
    let mut builder = WriterProperties::builder().set_compression(Compression::ZSTD(zstd));
    let schema = batch.schema();
    let mut sorting = Vec::new();
    for name in ["block_number", "log_index"] {
        if let Some((idx, _)) = schema.column_with_name(name) {
            sorting.push(SortingColumn {
                column_idx: idx as i32,
                descending: false,
                nulls_first: false,
            });
        }
    }
    if !sorting.is_empty() {
        builder = builder.set_sorting_columns(Some(sorting));
    }
    for field in schema.fields() {
        let path = ColumnPath::from(field.name().as_str());
        if bloom_column(field.name()) {
            builder = builder.set_column_bloom_filter_enabled(path.clone(), true);
        }
        if dictionary_off(field.name()) {
            builder = builder.set_column_dictionary_enabled(path, false);
        }
    }
    let props = builder.build();
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))
        .context("failed to create parquet writer")?;
    writer.write(batch).context("failed to write batch")?;
    writer.close().context("failed to finalise parquet")?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_segment_reads_back_as_the_rows_it_was_sealed_from() {
        let mut rows: Vec<Value> = vec![
            serde_json::json!({"block_number": 2, "log_index": 0, "to": "0xbb", "value": "5"}),
            serde_json::json!({"block_number": 1, "log_index": 3, "to": "0xaa"}),
        ];
        let bytes = seal_rows(&mut rows).unwrap();
        assert_eq!(segment_rows(bytes.into()).unwrap(), rows);
    }
}
