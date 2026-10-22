//! A chunk's rows read straight from the engine's Arrow batches. S0 went through JSON rows and lost
//! about a third of its time to it (RFC-0070 §13).

use alloy_primitives::I256;
use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{Array, ArrayRef, AsArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Int64Type, UInt64Type};
use arrow::record_batch::RecordBatch;

/// A text column in any of Arrow's three string layouts.
pub enum Text {
    Utf8(ArrayRef),
    Large(ArrayRef),
    View(ArrayRef),
}

impl Text {
    pub fn of(col: &ArrayRef, name: &str) -> Result<Text> {
        Ok(match col.data_type() {
            DataType::Utf8 => Text::Utf8(col.clone()),
            DataType::LargeUtf8 => Text::Large(col.clone()),
            DataType::Utf8View => Text::View(col.clone()),
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Decimal128(..)
            | DataType::Decimal256(..) => Text::Utf8(
                cast(col, &DataType::Utf8).with_context(|| format!("column `{name}` as text"))?,
            ),
            other => bail!("column `{name}` is {other}; the fold reads integers and text"),
        })
    }

    pub fn get(&self, i: usize) -> Option<&str> {
        let a = match self {
            Text::Utf8(a) | Text::Large(a) | Text::View(a) => a,
        };
        if a.is_null(i) {
            return None;
        }
        Some(match self {
            Text::Utf8(a) => a.as_string::<i32>().value(i),
            Text::Large(a) => a.as_string::<i64>().value(i),
            Text::View(a) => a.as_string_view().value(i),
        })
    }
}

/// An integer column as `u64`, refusing a negative or null value by name.
pub struct Unsigned(ArrayRef);

impl Unsigned {
    pub fn of(col: &ArrayRef, name: &str) -> Result<Unsigned> {
        let a = match col.data_type() {
            DataType::UInt64 => col.clone(),
            t if t.is_integer() => {
                cast(col, &DataType::UInt64).with_context(|| format!("column `{name}`"))?
            }
            other => bail!("column `{name}` is {other}, not an integer"),
        };
        Ok(Unsigned(a))
    }

    pub fn get(&self, i: usize) -> Option<u64> {
        (!self.0.is_null(i)).then(|| self.0.as_primitive::<UInt64Type>().value(i))
    }
}

/// The optional `sign`: 1 or -1, nothing else.
pub struct Sign(ArrayRef);

impl Sign {
    pub fn of(col: &ArrayRef) -> Result<Sign> {
        if !col.data_type().is_integer() {
            bail!("column `sign` is {}, not an integer", col.data_type());
        }
        Ok(Sign(cast(col, &DataType::Int64)?))
    }

    pub fn apply(&self, i: usize, amount: I256) -> Result<I256> {
        let s = (!self.0.is_null(i)).then(|| self.0.as_primitive::<Int64Type>().value(i));
        match s {
            Some(1) => Ok(amount),
            Some(-1) => amount.checked_neg().context("a negated amount overflows"),
            other => bail!("sign is {other:?}; only 1 and -1 are meaningful"),
        }
    }
}

pub fn column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef> {
    batch
        .column_by_name(name)
        .ok_or_else(|| anyhow!("the query returned no `{name}` column"))
}

pub fn amount(text: &str) -> Result<I256> {
    I256::from_dec_str(text)
        .map_err(|e| anyhow!("amount `{text}` is not a signed 256-bit integer: {e}"))
}
