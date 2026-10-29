//! What `src/entity_bind.rs` reads of `entity_offchain`, for a tab, which has no offchain tables
//! (RFC-0075 §11). Every type is uninhabited, so an entity naming `offchain__<t>` is refused by the
//! binder's own "no offchain table" error, as a node with none refuses it.

use crate::entity_row::Row;
use anyhow::Result;

pub use nuthatch_decode::registry::OFFCHAIN_NAMESPACE;

/// As the binary's: case-insensitive on the prefix, the table as written.
pub fn table_of(source: &str) -> Option<&str> {
    let head = source.get(..OFFCHAIN_NAMESPACE.len())?;
    let table = &source[OFFCHAIN_NAMESPACE.len()..];
    (head.eq_ignore_ascii_case(OFFCHAIN_NAMESPACE) && !table.is_empty()).then_some(table)
}

#[derive(Clone, Debug)]
pub enum Column {}

pub enum Snapshot {}

impl Snapshot {
    pub fn rows(&self, _: &[Column]) -> Result<Vec<Row>> {
        match *self {}
    }
}

pub enum Table {}

impl Table {
    pub fn bind(&self, _: &str, _: &[String]) -> Result<Vec<Column>> {
        match *self {}
    }
}

pub struct Tables;

impl Tables {
    pub fn none() -> Self {
        Tables
    }

    pub fn table(&self, _: &str) -> Result<Option<Table>> {
        Ok(None)
    }

    pub fn names(&self) -> Vec<&str> {
        Vec::new()
    }
}
