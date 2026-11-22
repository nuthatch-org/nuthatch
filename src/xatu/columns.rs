//! The dataset's column maps: reading its block and logs files into what `eth_getLogs` and the
//! block headers would have said.

use super::chunks::Chunk;
use super::Log;
use crate::source::LogFilter;
use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{Array, ArrayRef, BinaryArray, UInt64Array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, TimeUnit};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use std::borrow::Cow;
use std::collections::HashSet;

/// What the block file says about a chunk.
pub(super) struct Blocks {
    pub(super) hashes: Vec<String>,
    pub(super) timestamps: Vec<u64>,
    /// Whether the file carried `parent_hash` and the chain through it was checked. Older files (mainnet
    /// before about block 26.4M) do not carry it, and are anchored at both ends instead.
    pub(super) chained: bool,
}

fn open(bytes: Bytes) -> Result<ParquetRecordBatchReaderBuilder<Bytes>> {
    Ok(ParquetRecordBatchReaderBuilder::try_new(bytes)?)
}

fn col(batch: &arrow::record_batch::RecordBatch, name: &str) -> Result<ArrayRef> {
    batch
        .column_by_name(name)
        .cloned()
        .ok_or_else(|| anyhow!("column `{name}` is missing"))
}

fn as_u64(a: &ArrayRef) -> Result<UInt64Array> {
    let c = cast(a, &DataType::UInt64)?;
    Ok(c.as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| anyhow!("not an integer column"))?
        .clone())
}

/// Any of the binary-ish physical types a Parquet writer may have used for the hex text.
fn as_binary(a: &ArrayRef) -> Result<BinaryArray> {
    let c = cast(a, &DataType::Binary)?;
    Ok(c.as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| anyhow!("not a binary column"))?
        .clone())
}

/// The dataset stores hex as `0x…` text. Anything else is a format this reader was not written for,
/// and must be refused rather than guessed at.
pub(super) fn hex_text(b: &[u8]) -> Result<Cow<'_, str>> {
    let s = std::str::from_utf8(b).context("a hex value is not text")?;
    if !s.starts_with("0x") {
        bail!("a hex value does not start with 0x");
    }
    Ok(if s.bytes().any(|c| c.is_ascii_uppercase()) {
        Cow::Owned(s.to_ascii_lowercase())
    } else {
        Cow::Borrowed(s)
    })
}

pub(super) fn parse_blocks(bytes: Bytes, start: u64, len: u64) -> Result<Blocks> {
    let builder = open(bytes)?;
    let has_parent = builder.schema().field_with_name("parent_hash").is_ok();
    let schema = builder.parquet_schema();
    let names: &[&str] = if has_parent {
        &[
            "block_number",
            "block_hash",
            "parent_hash",
            "block_date_time",
        ]
    } else {
        &["block_number", "block_hash", "block_date_time"]
    };
    let mask = ProjectionMask::columns(schema, names.iter().copied());
    let reader = builder.with_projection(mask).build()?;
    let mut rows: Vec<(u64, String, Option<String>, u64)> = Vec::new();
    for batch in reader {
        let batch = batch?;
        let n = as_u64(&col(&batch, "block_number")?)?;
        let h = as_binary(&col(&batch, "block_hash")?)?;
        let p = has_parent
            .then(|| col(&batch, "parent_hash").and_then(|c| as_binary(&c)))
            .transpose()?;
        let t = col(&batch, "block_date_time")?;
        // Seconds since the epoch, whatever unit the writer used.
        let div = match t.data_type() {
            DataType::Timestamp(TimeUnit::Second, _) => 1,
            DataType::Timestamp(TimeUnit::Millisecond, _) => 1_000,
            DataType::Timestamp(TimeUnit::Microsecond, _) => 1_000_000,
            DataType::Timestamp(TimeUnit::Nanosecond, _) => 1_000_000_000,
            other => bail!("block_date_time is {other}, not a timestamp"),
        };
        let t = cast(&t, &DataType::Int64)?;
        let t = t
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| anyhow!("timestamp column did not cast"))?;
        for i in 0..n.len() {
            if n.is_null(i) || h.is_null(i) || t.is_null(i) {
                bail!("a block row has a null in it");
            }
            let secs = u64::try_from(t.value(i) / div).context("negative timestamp")?;
            rows.push((
                n.value(i),
                hex_text(h.value(i))?.into_owned(),
                // Present in newer files; in some older ones the column exists and is all null.
                match &p {
                    Some(p) if !p.is_null(i) => Some(hex_text(p.value(i))?.into_owned()),
                    _ => None,
                },
                secs,
            ));
        }
    }
    rows.sort_by_key(|r| r.0);
    let chained = rows.iter().all(|r| r.2.is_some());
    if rows.len() as u64 != len {
        bail!("the block file has {} rows, expected {len}", rows.len());
    }
    for (i, r) in rows.iter().enumerate() {
        if r.0 != start + i as u64 {
            bail!("the block file skips or repeats block {}", start + i as u64);
        }
        if r.3 == 0 {
            bail!("block {} has a zero timestamp", r.0);
        }
        if i > 0
            && r.2
                .as_deref()
                .is_some_and(|p| !p.eq_ignore_ascii_case(&rows[i - 1].1))
        {
            bail!("block {} does not extend block {}", r.0, r.0 - 1);
        }
    }
    Ok(Blocks {
        hashes: rows.iter().map(|r| r.1.clone()).collect(),
        timestamps: rows.iter().map(|r| r.3).collect(),
        chained,
    })
}

const LOG_COLUMNS: [&str; 9] = [
    "block_number",
    "log_index",
    "transaction_hash",
    "address",
    "topic0",
    "topic1",
    "topic2",
    "topic3",
    "data",
];

pub(super) fn check_logs_schema(bytes: &Bytes) -> Result<()> {
    let builder = open(bytes.clone())?;
    let schema = builder.schema();
    for c in LOG_COLUMNS {
        schema
            .field_with_name(c)
            .map_err(|_| anyhow!("the logs file has no `{c}` column"))?;
    }
    Ok(())
}

/// Every log of the chunk that `filter` asks for, in (block, log index) order, normalised to what
/// `eth_getLogs` returns: lowercase `0x` hex, empty data as `0x`, the block hash joined from the
/// block file (the dataset's logs carry none).
pub(super) fn extract_logs(c: &Chunk, filter: &LogFilter, len: u64) -> Result<Vec<Log>> {
    let builder = open(c.logs.clone())?;
    let schema = builder.parquet_schema();
    let mask = ProjectionMask::columns(schema, LOG_COLUMNS);
    let reader = builder
        .with_projection(mask)
        .with_batch_size(16_384)
        .build()?;
    let lower = |v: &[String]| -> Option<HashSet<String>> {
        (!v.is_empty()).then(|| v.iter().map(|s| s.to_ascii_lowercase()).collect())
    };
    let (want_addr, want_t0) = (lower(filter.addresses()), lower(filter.topic0s()));
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch?;
        let number = as_u64(&col(&batch, "block_number")?)?;
        let index = as_u64(&col(&batch, "log_index")?)?;
        let tx = as_binary(&col(&batch, "transaction_hash")?)?;
        let addr = as_binary(&col(&batch, "address")?)?;
        let topics = [
            as_binary(&col(&batch, "topic0")?)?,
            as_binary(&col(&batch, "topic1")?)?,
            as_binary(&col(&batch, "topic2")?)?,
            as_binary(&col(&batch, "topic3")?)?,
        ];
        let data = as_binary(&col(&batch, "data")?)?;
        for i in 0..batch.num_rows() {
            if number.is_null(i) || index.is_null(i) || tx.is_null(i) || addr.is_null(i) {
                bail!("a log row has a null in a required column");
            }
            let a = hex_text(addr.value(i))?;
            if want_addr.as_ref().is_some_and(|w| !w.contains(a.as_ref())) {
                continue;
            }
            if let Some(w) = &want_t0 {
                if topics[0].is_null(i) || !w.contains(hex_text(topics[0].value(i))?.as_ref()) {
                    continue;
                }
            }
            let bn = number.value(i);
            if bn < c.start || bn >= c.start + len {
                bail!("a log is for block {bn}, outside the chunk");
            }
            let mut t = Vec::new();
            for col in &topics {
                if col.is_null(i) {
                    break;
                }
                t.push(hex_text(col.value(i))?.into_owned());
            }
            let log = Log {
                address: a.into_owned(),
                topics: t,
                data: if data.is_null(i) || data.value(i).is_empty() {
                    "0x".to_string()
                } else {
                    hex_text(data.value(i))?.into_owned()
                },
                block_number: bn,
                block_hash: c.hashes[(bn - c.start) as usize].clone(),
                tx_hash: hex_text(tx.value(i))?.into_owned(),
                log_index: index.value(i),
            };
            if filter.matches(&log) {
                out.push(log);
            }
        }
    }
    out.sort_by_key(|l| (l.block_number, l.log_index));
    if out
        .windows(2)
        .any(|w| (w[0].block_number, w[0].log_index) == (w[1].block_number, w[1].log_index))
    {
        bail!("the logs file repeats a (block, log index)");
    }
    Ok(out)
}
