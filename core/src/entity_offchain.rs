//! What the entity binder reads of a nest's offchain tables (RFC-0045, #1437), as traits. The node
//! implements them over its sealed snapshot files; an embedding with none binds through [`NoOffchain`],
//! so an entity naming `offchain__<t>` is refused by the binder's own "no offchain table" error.

use crate::entity_row::Row;
use anyhow::Result;

pub use nuthatch_decode::registry::OFFCHAIN_NAMESPACE;

/// The offchain table an entity source names, if it names one.
///
/// Case-insensitive, as `/sql` resolves it. The table after the prefix is returned as written, and
/// the node's catalogue resolves it case-insensitively too.
pub fn table_of(source: &str) -> Option<&str> {
    // `get`, not indexing: a quoted identifier may put a multibyte character across the boundary.
    let head = source.get(..OFFCHAIN_NAMESPACE.len())?;
    let table = &source[OFFCHAIN_NAMESPACE.len()..];
    (head.eq_ignore_ascii_case(OFFCHAIN_NAMESPACE) && !table.is_empty()).then_some(table)
}

/// What an offchain column's values become in an entity. §3.3 has no float, and there is no
/// decimal scalar yet, so a feed carrying either is refused at load rather than rounded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Str,
    Int,
    Bool,
}

impl Kind {
    pub fn name(self) -> &'static str {
        match self {
            Kind::Str => "text",
            Kind::Int => "integer",
            Kind::Bool => "boolean",
        }
    }
}

/// One column an entity reads from an offchain table, and the kind every snapshot carrying it has.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub kind: Kind,
}

/// A nest's offchain catalogue as the binder sees it.
pub trait OffchainTables {
    /// The columns `source` reads of `table`, refused at load if they do not resolve, or `None` when
    /// the nest has no such table.
    fn bind(&self, table: &str, source: &str, columns: &[String]) -> Result<Option<Vec<Column>>>;

    /// Every offchain table the nest has.
    fn names(&self) -> Vec<&str>;
}

/// One snapshot of an offchain table.
pub trait OffchainSnapshot {
    /// Its rows as entity rows of `columns`, in order.
    fn rows(&self, columns: &[Column]) -> Result<Vec<Row>>;
}

/// No offchain tables, for a caller that binds chain sources only.
pub struct NoOffchain;

impl OffchainTables for NoOffchain {
    fn bind(&self, _: &str, _: &str, _: &[String]) -> Result<Option<Vec<Column>>> {
        Ok(None)
    }

    fn names(&self) -> Vec<&str> {
        Vec::new()
    }
}
