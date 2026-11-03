//! `[mounts.sync]` (RFC-0080 §5.6): what a client may ask of a mount's shapes. Off unless written.
//!
//! A shape names a table and constrains columns, and a follower evaluates it against every delta for
//! every subscriber, so the operator lists the tables and columns that may be named, and bounds how
//! many shapes a connection holds and how many values one term lists.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[cfg(feature = "sync-server")]
use crate::shape::Shape;

/// `[mounts.sync]`.
///
/// `pin` (§5.6) is not here: a key this table does not know is refused, so a config that asks for a
/// pin the binary cannot honour fails to load rather than serving every user every user's rows.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MountSync {
    /// The tables a shape may name, each with the columns it may constrain. A table listed with
    /// columns is named with a term on one of them (#2603); one listed with none is named whole.
    pub tables: BTreeMap<String, Vec<String>>,
    #[serde(default = "default_max_shapes")]
    pub max_shapes_per_connection: usize,
    #[serde(default = "default_max_values")]
    pub max_values_per_term: usize,
}

fn default_max_shapes() -> usize {
    16
}

fn default_max_values() -> usize {
    1000
}

/// The most either bound may be set to, so a typo cannot lift it past what a follower can hold.
const MAX_SHAPES_CEILING: usize = 1024;
const MAX_VALUES_CEILING: usize = 100_000;

/// Why a shape was refused. Each says which part, so a client can fix it.
#[cfg(feature = "sync-server")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    Table(String),
    Column {
        table: String,
        column: String,
    },
    EmptyTerm(String),
    Values {
        column: String,
        got: usize,
        max: usize,
    },
    TooManyShapes {
        max: usize,
    },
    /// A table listed with columns, named with no term on any of them.
    Unconstrained(String),
}

#[cfg(feature = "sync-server")]
impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Refusal::Table(t) => write!(f, "table `{t}` is not shapeable on this mount"),
            Refusal::Column { table, column } => {
                write!(
                    f,
                    "column `{column}` of `{table}` is not shapeable on this mount"
                )
            }
            Refusal::EmptyTerm(c) => write!(f, "the term on `{c}` lists no values"),
            Refusal::Values { column, got, max } => {
                write!(
                    f,
                    "the term on `{column}` lists {got} values, at most {max}"
                )
            }
            Refusal::TooManyShapes { max } => {
                write!(f, "a connection may hold at most {max} shapes")
            }
            Refusal::Unconstrained(t) => write!(
                f,
                "`{t}` is shapeable by its listed columns only, so a shape of it names one"
            ),
        }
    }
}

#[cfg(feature = "sync-server")]
impl std::error::Error for Refusal {}

impl MountSync {
    /// Whether the configuration itself is sound.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.tables.is_empty() {
            anyhow::bail!("lists no tables, so nothing could be shaped");
        }
        if !(1..=MAX_SHAPES_CEILING).contains(&self.max_shapes_per_connection) {
            anyhow::bail!(
                "max_shapes_per_connection must be between 1 and {MAX_SHAPES_CEILING}, not {}",
                self.max_shapes_per_connection
            );
        }
        if !(1..=MAX_VALUES_CEILING).contains(&self.max_values_per_term) {
            anyhow::bail!(
                "max_values_per_term must be between 1 and {MAX_VALUES_CEILING}, not {}",
                self.max_values_per_term
            );
        }
        Ok(())
    }

    /// Whether a connection already holding `held` shapes may add `shape`.
    #[cfg(feature = "sync-server")]
    pub fn admit(&self, held: usize, shape: &Shape) -> Result<(), Refusal> {
        if held >= self.max_shapes_per_connection {
            return Err(Refusal::TooManyShapes {
                max: self.max_shapes_per_connection,
            });
        }
        let Some(columns) = self.tables.get(&shape.table) else {
            return Err(Refusal::Table(shape.table.clone()));
        };
        if !columns.is_empty() && shape.terms.is_empty() && shape.ranges.is_empty() {
            return Err(Refusal::Unconstrained(shape.table.clone()));
        }
        let columns_named = shape.terms.keys().chain(shape.ranges.keys());
        for column in columns_named {
            if !columns.contains(column) {
                return Err(Refusal::Column {
                    table: shape.table.clone(),
                    column: column.clone(),
                });
            }
        }
        for (column, values) in &shape.terms {
            if values.is_empty() {
                return Err(Refusal::EmptyTerm(column.clone()));
            }
            if values.len() > self.max_values_per_term {
                return Err(Refusal::Values {
                    column: column.clone(),
                    got: values.len(),
                    max: self.max_values_per_term,
                });
            }
        }
        Ok(())
    }
}

#[cfg(all(test, feature = "sync-server"))]
mod tests {
    use super::*;

    fn sync() -> MountSync {
        toml::from_str(
            r#"
            max_values_per_term = 3
            max_shapes_per_connection = 2
            [tables]
            swaps = ["pool", "block"]
            transfers = []
            "#,
        )
        .unwrap()
    }

    fn shape(json: &str) -> Shape {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn the_bounds_default_to_what_the_rfc_shows() {
        let s: MountSync = toml::from_str("[tables]\nswaps = [\"pool\"]\n").unwrap();
        assert_eq!(
            (s.max_shapes_per_connection, s.max_values_per_term),
            (16, 1000)
        );
        s.validate().unwrap();
    }

    #[test]
    fn a_key_the_binary_does_not_know_is_refused_so_a_pin_cannot_be_ignored() {
        let pinned = "[tables]\nswaps = [\"pool\"]\npin = { header = \"x-nuthatch-pin\", column = \"owner\" }\n";
        assert!(toml::from_str::<MountSync>(pinned).is_err());
    }

    #[test]
    fn a_config_that_lifts_a_bound_past_its_ceiling_or_lists_nothing_is_unsound() {
        let base = sync();
        assert!(MountSync {
            tables: BTreeMap::new(),
            ..base.clone()
        }
        .validate()
        .is_err());
        for bad in [0, MAX_SHAPES_CEILING + 1] {
            let s = MountSync {
                max_shapes_per_connection: bad,
                ..base.clone()
            };
            assert!(s.validate().is_err(), "{bad} shapes");
        }
        for bad in [0, MAX_VALUES_CEILING + 1] {
            let s = MountSync {
                max_values_per_term: bad,
                ..base.clone()
            };
            assert!(s.validate().is_err(), "{bad} values");
        }
        base.validate().unwrap();
    }

    #[test]
    fn a_shape_is_admitted_only_inside_what_the_operator_listed() {
        let s = sync();
        let ok = |json| s.admit(0, &shape(json));
        assert_eq!(
            ok(r#"{"table":"swaps","where":{"pool":["0xa","0xb"]}}"#),
            Ok(())
        );
        assert_eq!(
            ok(r#"{"table":"swaps","where":{"block":{"gte":10},"pool":["0xa"]}}"#),
            Ok(())
        );
        assert_eq!(
            ok(r#"{"table":"transfers"}"#),
            Ok(()),
            "a listed table, whole"
        );
        assert_eq!(
            ok(r#"{"table":"mints","where":{}}"#),
            Err(Refusal::Table("mints".into()))
        );
        assert_eq!(
            ok(r#"{"table":"swaps","where":{"sender":["0xa"]}}"#),
            Err(Refusal::Column {
                table: "swaps".into(),
                column: "sender".into()
            })
        );
        assert_eq!(
            ok(r#"{"table":"transfers","where":{"from":["0xa"]}}"#),
            Err(Refusal::Column {
                table: "transfers".into(),
                column: "from".into()
            }),
            "a table listed with no columns takes no terms"
        );
        assert_eq!(
            ok(r#"{"table":"swaps","where":{"block":{"gte":1},"sender":{"lte":1}}}"#),
            Err(Refusal::Column {
                table: "swaps".into(),
                column: "sender".into()
            }),
            "a range names a column as a term does"
        );
        assert_eq!(
            ok(r#"{"table":"swaps","where":{"pool":[]}}"#),
            Err(Refusal::EmptyTerm("pool".into()))
        );
        assert_eq!(
            ok(r#"{"table":"swaps","where":{"pool":["a","b","c","d"]}}"#),
            Err(Refusal::Values {
                column: "pool".into(),
                got: 4,
                max: 3
            })
        );
        assert_eq!(
            s.admit(2, &shape(r#"{"table":"swaps","where":{"pool":["0xa"]}}"#)),
            Err(Refusal::TooManyShapes { max: 2 })
        );
    }

    /// #2603: a table listed with columns is named with a term on one of them; only a table listed
    /// with none may be named whole, as the config reference says.
    #[test]
    fn a_table_listed_with_columns_is_not_named_whole() {
        let s = sync();
        for whole in [r#"{"table":"swaps"}"#, r#"{"table":"swaps","where":{}}"#] {
            assert!(s.admit(0, &shape(whole)).is_err(), "{whole}");
        }
        assert_eq!(
            s.admit(
                0,
                &shape(r#"{"table":"swaps","where":{"block":{"gte":1}}}"#)
            ),
            Ok(()),
            "a range is a term"
        );
        assert_eq!(s.admit(0, &shape(r#"{"table":"transfers"}"#)), Ok(()));
    }
}
