//! Stored and decoded rows to what the views are fed. One conversion each way, so a retraction
//! cancels the insertion it undoes.

use crate::exposure::{self, ExposureBatch};
use crate::labels::LabelSet;
use crate::velocity::{self, VelocityBatch};
use crate::views::{self, WeightedBatch};
use anyhow::{Context, Result};
use nuthatch_decode::registry::{DecodedRow, TableSchema};

/// Stored JSON rows to decoded rows, against a nest's table schemas.
///
/// One conversion, and every caller uses it - the reorg path, the restart seed, and whatever needs it
/// next. A second one written separately is how a retraction stops cancelling its insertion (#864).
///
/// A row of a table the registry does not decode (a threshold flag, a `[[calls]]` or `[[ipfs]]` row)
/// is skipped: ingest never feeds one to an entity or a view, so nothing seeds or retracts it.
pub fn decode_stored_rows(schema: &[TableSchema], stored: &[String]) -> Result<Vec<DecodedRow>> {
    let mut rows = Vec::with_capacity(stored.len());
    for raw in stored {
        let value: serde_json::Value =
            serde_json::from_str(raw).context("an unparseable row in the hot store")?;
        let Some(table) = value.get("table").and_then(serde_json::Value::as_str) else {
            continue;
        };
        let Some(table_schema) = schema.iter().find(|t| t.table == table) else {
            continue;
        };
        rows.push(DecodedRow::from_stored(&value, table_schema)?);
    }
    Ok(rows)
}

/// A row's transfer as the derived views read it: (from, to, value), or `None` for any other row
/// and for a value past i128 (#814). Ingest feeds the views through these two calls, so a reorg
/// retracts and a restart replays through them too, whatever the token names its columns.
pub fn view_transfer(row: &DecodedRow) -> Option<(String, String, i128)> {
    let (from, to, value, _) = row.erc20_transfer_fields()?;
    let value = value.as_deref().and_then(views::transfer_value)?;
    Some((from, to, value))
}

/// A transfer `view_transfer` passes over for its value alone, counted as `dropped_over_i128`.
pub fn oversized_transfer(row: &DecodedRow) -> bool {
    row.erc20_transfer_fields()
        .and_then(|(_, _, value, _)| value)
        .is_some_and(|v| views::transfer_value(&v).is_none())
}

/// Build a weight −1 balance retraction batch from rolled-back rows (reorg).
pub fn retraction_batch(rows: &[DecodedRow]) -> WeightedBatch {
    rows.iter()
        .filter_map(view_transfer)
        .flat_map(|(from, to, v)| views::transfer_deltas(&from, &to, v, -1))
        .collect()
}

/// Build a weight −1 exposure retraction batch from rolled-back rows (reorg), so a reorged
/// flag/exposure retracts exactly like a balance.
pub fn exposure_retraction_batch(rows: &[DecodedRow], labels: &LabelSet) -> ExposureBatch {
    rows.iter()
        .filter_map(view_transfer)
        .flat_map(|(from, to, v)| exposure::exposure_deltas(&from, &to, v, -1, labels))
        .collect()
}

/// Build a weight −1 velocity retraction batch from rolled-back rows (reorg), so a reorged velocity
/// flag drops.
pub fn velocity_retraction_batch(rows: &[DecodedRow], window: u64) -> VelocityBatch {
    rows.iter()
        .filter_map(|row| view_transfer(row).map(|t| (row.block_number, t)))
        .flat_map(|(block, (from, _, v))| velocity::velocity_deltas(&from, block, v, -1, window))
        .collect()
}
