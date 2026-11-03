//! Sizing a slice of an answer before it goes into an Arrow IPC stream, shared by the node's and the
//! engine's `collect_ipc` so both hold the result byte cap the same way (#2589, #2599, #2616).

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{ArrowError, DataType};

/// `part` with each view column's data buffers cut down to the rows it holds. The IPC writer writes
/// a sliced view array's variadic buffers whole, so uncompacted one row carries the batch's strings.
pub fn compact(part: &RecordBatch) -> Result<RecordBatch, ArrowError> {
    if !part.columns().iter().any(|c| is_view(c.data_type())) {
        return Ok(part.clone());
    }
    let columns: Vec<ArrayRef> = part
        .columns()
        .iter()
        .map(|c| match c.data_type() {
            DataType::Utf8View => Arc::new(c.as_string_view().gc()) as ArrayRef,
            DataType::BinaryView => Arc::new(c.as_binary_view().gc()) as ArrayRef,
            _ => c.clone(),
        })
        .collect();
    RecordBatch::try_new(part.schema(), columns)
}

/// What a compacted `part` takes in an IPC stream, near enough to decide whether to write it whole.
/// A type whose slice cannot be sized counts its whole buffers, which errs toward fewer rows at once.
pub fn slice_bytes(part: &RecordBatch) -> usize {
    part.columns()
        .iter()
        .map(|c| {
            let data = c.to_data();
            if is_view(c.data_type()) {
                // After `compact`, the buffers are this slice's own; the views alone undercount.
                data.get_buffer_memory_size()
            } else {
                data.get_slice_memory_size()
                    .unwrap_or_else(|_| data.get_buffer_memory_size())
            }
        })
        .fold(0usize, usize::saturating_add)
}

fn is_view(t: &DataType) -> bool {
    matches!(t, DataType::Utf8View | DataType::BinaryView)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::StringViewArray;
    use arrow_schema::{Field, Schema};

    #[test]
    fn a_compacted_view_slice_is_sized_by_its_own_strings() {
        let big = "A".repeat(100_000);
        let col = StringViewArray::from_iter_values(std::iter::repeat_n(big.as_str(), 32));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::Utf8View,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap();
        let one = compact(&batch.slice(0, 1)).unwrap();
        let bytes = slice_bytes(&one);
        assert!(
            (100_000..200_000).contains(&bytes),
            "one 100 KB row sized at {bytes}"
        );
    }
}
