//! RFC-0041 §3.3: the expression half of the entity lowering (#870).
//!
//! Expressions over [`Row`], evaluated by column *index* rather than by name - the plan carries the
//! shape, so the same evaluator serves any admitted entity. This is the piece slice 0 did not have:
//! its predicates were Rust functions matching one AST, so nothing could be lowered but that one.
//!
//! Two properties are load-bearing and neither is the obvious behaviour of a naive evaluator.
//!
//! **Overflow faults** (§3.3.1). Every arithmetic operation is checked. A `sum` whose running total
//! leaves `i128` is an error at the row that carried it past, not a total that resumes from the other
//! end of the number line. Burrmill errors here, as Postgres does (a BIGINT `+` past `i64` is an
//! arithmetic overflow error), but DataFusion has wrapped by default (arrow-datafusion#17539) - so
//! "whatever the engine does" is not a specification, and the contract lives on the entity.
//!
//! **NULL is unknown, not false.** SQL's three-valued logic, implemented rather than approximated:
//! `NULL AND false` is `false` while `NULL AND true` is `NULL`, and a predicate that evaluates to
//! `NULL` excludes its row without being an error. Getting this wrong produces a relation that
//! differs from the SQL engine only on rows with nulls in them, which is the hardest kind of divergence to
//! notice and precisely what the parity gate in §8 exists to catch.

use crate::entity_row::{Row, Scalar};
use anyhow::{bail, Result};

/// Cast targets in the v1 subset. Deliberately three: §3.3 has no float, and a date/time type would
/// need a volatile-function story it does not yet have.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Type {
    Int,
    Str,
    Bool,
}

/// A comparison. Separate from [`Expr`] so the lowerer cannot invent one the evaluator lacks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cmp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// An expression over a positional row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Expr {
    Column(usize),
    Literal(Scalar),
    Add(Box<Expr>, Box<Expr>),
    Sub(Box<Expr>, Box<Expr>),
    Mul(Box<Expr>, Box<Expr>),
    Compare(Cmp, Box<Expr>, Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    IsNull(Box<Expr>),
    /// `CASE WHEN c THEN v ... ELSE e END`. A `WHEN` whose condition is `NULL` does not match -
    /// only `TRUE` does.
    Case {
        whens: Vec<(Expr, Expr)>,
        otherwise: Option<Box<Expr>>,
    },
    Coalesce(Vec<Expr>),
    Cast(Box<Expr>, Type),
}

impl Expr {
    pub fn eval(&self, row: &Row) -> Result<Scalar> {
        match self {
            Expr::Column(i) => Ok(row.get(*i).clone()),
            Expr::Literal(s) => Ok(s.clone()),

            Expr::Add(a, b) => arith(self, a, b, row, i128::checked_add),
            Expr::Sub(a, b) => arith(self, a, b, row, i128::checked_sub),
            Expr::Mul(a, b) => arith(self, a, b, row, i128::checked_mul),

            Expr::Compare(op, a, b) => {
                let (l, r) = (a.eval(row)?, b.eval(row)?);
                // NULL compared with anything is unknown, including with NULL. `IS NULL` is the
                // operator for that question and it is spelled separately.
                if l.is_null() || r.is_null() {
                    return Ok(Scalar::Null);
                }
                if std::mem::discriminant(&l) != std::mem::discriminant(&r) {
                    bail!("cannot compare {l:?} with {r:?}: the entity subset has no implicit cross-type coercion")
                }
                let ord = l.cmp(&r);
                Ok(Scalar::Bool(match op {
                    Cmp::Eq => ord.is_eq(),
                    Cmp::Ne => ord.is_ne(),
                    Cmp::Lt => ord.is_lt(),
                    Cmp::Le => ord.is_le(),
                    Cmp::Gt => ord.is_gt(),
                    Cmp::Ge => ord.is_ge(),
                }))
            }

            // Three-valued AND: false dominates, so `NULL AND false` is false. Evaluating both sides
            // rather than short-circuiting keeps a type error on the other side visible - a silently
            // skipped bad expression is a lowering bug that only shows up on some rows.
            Expr::And(a, b) => Ok(match (truth(&a.eval(row)?)?, truth(&b.eval(row)?)?) {
                (Some(false), _) | (_, Some(false)) => Scalar::Bool(false),
                (Some(true), Some(true)) => Scalar::Bool(true),
                _ => Scalar::Null,
            }),
            // Three-valued OR: true dominates.
            Expr::Or(a, b) => Ok(match (truth(&a.eval(row)?)?, truth(&b.eval(row)?)?) {
                (Some(true), _) | (_, Some(true)) => Scalar::Bool(true),
                (Some(false), Some(false)) => Scalar::Bool(false),
                _ => Scalar::Null,
            }),
            Expr::Not(a) => Ok(match truth(&a.eval(row)?)? {
                Some(b) => Scalar::Bool(!b),
                None => Scalar::Null,
            }),

            Expr::IsNull(a) => Ok(Scalar::Bool(a.eval(row)?.is_null())),

            Expr::Case { whens, otherwise } => {
                for (cond, value) in whens {
                    if truth(&cond.eval(row)?)? == Some(true) {
                        return value.eval(row);
                    }
                }
                match otherwise {
                    Some(e) => e.eval(row),
                    // SQL: a CASE with no matching WHEN and no ELSE is NULL, not an error.
                    None => Ok(Scalar::Null),
                }
            }

            Expr::Coalesce(args) => {
                for a in args {
                    let v = a.eval(row)?;
                    if !v.is_null() {
                        return Ok(v);
                    }
                }
                Ok(Scalar::Null)
            }

            Expr::Cast(a, ty) => cast(a.eval(row)?, *ty),
        }
    }
}

impl Expr {
    /// The type this expression has on every row it does not fault on, when that is certain; or the
    /// refusal [`Expr::eval`] raises on **every** row (#1590).
    ///
    /// Deliberately partial. `Some(t)` means each evaluation either faults or yields a non-null `t`,
    /// which holds for columns that are never NULL, non-null literals and the eager operators over
    /// them. `CASE`, `COALESCE`, NULL and a column that may be NULL give `None`: which branch runs,
    /// and whether a NULL short-circuits, depends on the row, and a check that tried to follow it
    /// either refused entities that run or passed ones that do not. So a refusal here is always
    /// right, and what this cannot judge still faults loudly at runtime, as it always has.
    ///
    /// `cols` holds each column's type, or `None` for one that may be NULL. The guarantee covers a
    /// well-bound expression, every column index checked, evaluated on rows that match `cols`.
    pub fn definite_type(&self, cols: &[Option<Type>]) -> Result<Option<Type>> {
        let need = |what: &str, got: Option<Type>, want: Type| match got {
            Some(t) if t != want => bail!("{what} needs {want:?}, got {t:?}"),
            _ => Ok(()),
        };
        Ok(match self {
            Expr::Column(i) => *cols
                .get(*i)
                .ok_or_else(|| anyhow::anyhow!("column {i} is outside the row"))?,
            Expr::Literal(s) => match s {
                Scalar::Null => None,
                Scalar::Bool(_) => Some(Type::Bool),
                Scalar::Int(_) => Some(Type::Int),
                Scalar::Str(_) => Some(Type::Str),
            },
            Expr::Add(a, b) | Expr::Sub(a, b) | Expr::Mul(a, b) => {
                // NULL returns before either type is read, so only two certain operands can fault.
                match (a.definite_type(cols)?, b.definite_type(cols)?) {
                    (Some(x), Some(y)) if x != Type::Int || y != Type::Int => {
                        bail!("arithmetic needs Int, got {x:?} and {y:?}")
                    }
                    (Some(_), Some(_)) => Some(Type::Int),
                    _ => None,
                }
            }
            Expr::Compare(_, a, b) => {
                let (a, b) = (a.definite_type(cols)?, b.definite_type(cols)?);
                match (a, b) {
                    (Some(x), Some(y)) if x != y => bail!(
                        "a comparison of {x:?} with {y:?}: the entity subset has no implicit coercion"
                    ),
                    (Some(_), Some(_)) => Some(Type::Bool),
                    _ => None,
                }
            }
            Expr::And(a, b) | Expr::Or(a, b) => {
                let (a, b) = (a.definite_type(cols)?, b.definite_type(cols)?);
                need("a logical operator", a, Type::Bool)?;
                need("a logical operator", b, Type::Bool)?;
                (a.is_some() && b.is_some()).then_some(Type::Bool)
            }
            Expr::Not(a) => {
                let a = a.definite_type(cols)?;
                need("NOT", a, Type::Bool)?;
                a
            }
            Expr::IsNull(a) => {
                a.definite_type(cols)?;
                Some(Type::Bool)
            }
            Expr::Case { .. } | Expr::Coalesce(_) => None,
            Expr::Cast(a, ty) => {
                let a = a.definite_type(cols)?;
                if a == Some(Type::Str) && *ty == Type::Bool {
                    bail!("cast from VARCHAR to BOOLEAN is not in the entity subset")
                }
                a.map(|_| *ty)
            }
        })
    }
}

/// Checked integer arithmetic with NULL propagation. §3.3.1: an unrepresentable result is a fault.
fn arith(
    whole: &Expr,
    a: &Expr,
    b: &Expr,
    row: &Row,
    op: fn(i128, i128) -> Option<i128>,
) -> Result<Scalar> {
    let (l, r) = (a.eval(row)?, b.eval(row)?);
    if l.is_null() || r.is_null() {
        return Ok(Scalar::Null);
    }
    let (Some(x), Some(y)) = (l.as_int(), r.as_int()) else {
        bail!("arithmetic on non-integer operands {l:?} and {r:?}")
    };
    match op(x, y) {
        Some(v) => Ok(Scalar::Int(v)),
        // Deliberately loud and specific. A wrap here would produce a *plausible* number of the
        // wrong sign or magnitude, stored and sealed as canonical - the failure absent data never
        // has, because absent data announces itself (RFC-0041 §3.3.1).
        None => bail!(
            "arithmetic overflow evaluating {whole:?} on {x} and {y}: the result does not fit i128. \
             An entity faults rather than wrapping."
        ),
    }
}

/// SQL truth: `Some(bool)` for known, `None` for unknown (NULL). A non-boolean is a lowering bug.
fn truth(s: &Scalar) -> Result<Option<bool>> {
    match s {
        Scalar::Bool(b) => Ok(Some(*b)),
        Scalar::Null => Ok(None),
        other => bail!("expected a boolean in a logical position, got {other:?}"),
    }
}

/// Casts refuse rather than truncate or silently NULL (§3.3.1). `TRY_CAST` is not in the v1 subset.
fn cast(v: Scalar, ty: Type) -> Result<Scalar> {
    if v.is_null() {
        return Ok(Scalar::Null);
    }
    Ok(match (&v, ty) {
        (Scalar::Int(_), Type::Int)
        | (Scalar::Str(_), Type::Str)
        | (Scalar::Bool(_), Type::Bool) => v,
        (Scalar::Int(i), Type::Str) => Scalar::Str(i.to_string()),
        (Scalar::Bool(b), Type::Str) => Scalar::Str(b.to_string()),
        (Scalar::Bool(b), Type::Int) => Scalar::Int(i128::from(*b)),
        (Scalar::Str(s), Type::Int) => match s.parse::<i128>() {
            Ok(i) => Scalar::Int(i),
            Err(_) => bail!("cast to INTEGER refused: `{s}` is not an exact integer"),
        },
        (Scalar::Int(i), Type::Bool) => Scalar::Bool(*i != 0),
        (Scalar::Str(_), Type::Bool) => {
            bail!("cast from VARCHAR to BOOLEAN is not in the entity subset")
        }
        // Unreachable - NULL returned above - but spelled out rather than caught by a wildcard, so
        // adding a Scalar variant is a compile error here instead of a silent passthrough.
        (Scalar::Null, _) => Scalar::Null,
    })
}

/// Does this predicate admit the row? **Only `TRUE` does** - `NULL` excludes it, as SQL's `WHERE`
/// does, and without being an error.
pub fn admits(pred: &Expr, row: &Row) -> Result<bool> {
    Ok(truth(&pred.eval(row)?)? == Some(true))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(i: usize) -> Box<Expr> {
        Box::new(Expr::Column(i))
    }
    fn int(i: i128) -> Box<Expr> {
        Box::new(Expr::Literal(Scalar::Int(i)))
    }
    fn null() -> Box<Expr> {
        Box::new(Expr::Literal(Scalar::Null))
    }
    fn row(v: Vec<Scalar>) -> Row {
        Row(v)
    }

    #[test]
    fn arithmetic_is_exact_and_by_column_index() {
        let e = Expr::Add(col(0), Expr::Mul(col(1), int(2)).into());
        let r = row(vec![Scalar::Int(10), Scalar::Int(5)]);
        assert_eq!(e.eval(&r).unwrap(), Scalar::Int(20));
    }

    /// §3.3.1, the contract that cannot be retrofitted.
    #[test]
    fn overflow_is_an_error_and_never_a_wrap() {
        let e = Expr::Add(int(i128::MAX), int(1));
        let err = e.eval(&row(vec![])).unwrap_err().to_string();
        assert!(err.contains("overflow"), "{err}");
        assert!(err.contains("faults rather than wrapping"), "{err}");

        // The wrap this refuses would be `i128::MIN` - a plausible number, of the wrong sign.
        assert_eq!(i128::MAX.wrapping_add(1), i128::MIN);
    }

    #[test]
    fn overflow_in_a_subtraction_or_product_faults_too() {
        for e in [
            Expr::Sub(int(i128::MIN), int(1)),
            Expr::Mul(int(i128::MAX), int(2)),
        ] {
            assert!(e.eval(&row(vec![])).is_err(), "{e:?} must fault");
        }
    }

    #[test]
    fn null_propagates_through_arithmetic_rather_than_defaulting_to_zero() {
        let e = Expr::Add(col(0), int(1));
        assert_eq!(e.eval(&row(vec![Scalar::Null])).unwrap(), Scalar::Null);
    }

    /// The half a naive evaluator gets wrong. `NULL AND false` is **false**, because false dominates
    /// - the row is excluded regardless of what the unknown turns out to be.
    #[test]
    fn three_valued_logic_matches_sql() {
        let t = || Box::new(Expr::Literal(Scalar::Bool(true)));
        let f = || Box::new(Expr::Literal(Scalar::Bool(false)));
        let n = null;
        let r = row(vec![]);

        assert_eq!(Expr::And(n(), f()).eval(&r).unwrap(), Scalar::Bool(false));
        assert_eq!(Expr::And(n(), t()).eval(&r).unwrap(), Scalar::Null);
        assert_eq!(Expr::Or(n(), t()).eval(&r).unwrap(), Scalar::Bool(true));
        assert_eq!(Expr::Or(n(), f()).eval(&r).unwrap(), Scalar::Null);
        assert_eq!(Expr::Not(n()).eval(&r).unwrap(), Scalar::Null);
        assert_eq!(Expr::Not(t()).eval(&r).unwrap(), Scalar::Bool(false));
    }

    #[test]
    fn comparison_with_null_is_unknown_not_equal() {
        let r = row(vec![Scalar::Null]);
        assert_eq!(
            Expr::Compare(Cmp::Eq, col(0), null()).eval(&r).unwrap(),
            Scalar::Null,
            "NULL = NULL is unknown; IS NULL is the operator for that question"
        );
        assert_eq!(Expr::IsNull(col(0)).eval(&r).unwrap(), Scalar::Bool(true));
    }

    /// `WHERE` admits only TRUE. An unknown predicate excludes the row and is not an error.
    #[test]
    fn a_null_predicate_excludes_the_row_without_erroring() {
        let p = Expr::Compare(Cmp::Gt, col(0), int(0));
        assert!(admits(&p, &row(vec![Scalar::Int(1)])).unwrap());
        assert!(!admits(&p, &row(vec![Scalar::Int(-1)])).unwrap());
        assert!(!admits(&p, &row(vec![Scalar::Null])).unwrap());
    }

    #[test]
    fn case_needs_a_true_when_and_is_null_without_an_else() {
        let e = Expr::Case {
            whens: vec![
                (Expr::Compare(Cmp::Gt, col(0), int(10)), *int(1)),
                (Expr::Compare(Cmp::Gt, col(0), int(5)), *int(2)),
            ],
            otherwise: None,
        };
        assert_eq!(e.eval(&row(vec![Scalar::Int(20)])).unwrap(), Scalar::Int(1));
        assert_eq!(e.eval(&row(vec![Scalar::Int(7)])).unwrap(), Scalar::Int(2));
        assert_eq!(e.eval(&row(vec![Scalar::Int(1)])).unwrap(), Scalar::Null);
        // A NULL condition does not match - only TRUE does.
        assert_eq!(e.eval(&row(vec![Scalar::Null])).unwrap(), Scalar::Null);
    }

    #[test]
    fn coalesce_takes_the_first_non_null() {
        let e = Expr::Coalesce(vec![*null(), *col(0), *int(9)]);
        assert_eq!(e.eval(&row(vec![Scalar::Int(3)])).unwrap(), Scalar::Int(3));
        assert_eq!(e.eval(&row(vec![Scalar::Null])).unwrap(), Scalar::Int(9));
    }

    #[test]
    fn a_cast_that_cannot_represent_its_input_refuses() {
        // §3.3.1 again: no truncation, no silent NULL. TRY_CAST is not in the subset.
        let bad = Expr::Cast(
            Box::new(Expr::Literal(Scalar::Str("12.5".into()))),
            Type::Int,
        );
        let err = bad.eval(&row(vec![])).unwrap_err().to_string();
        assert!(err.contains("refused"), "{err}");

        let good = Expr::Cast(Box::new(Expr::Literal(Scalar::Str("42".into()))), Type::Int);
        assert_eq!(good.eval(&row(vec![])).unwrap(), Scalar::Int(42));
        // NULL casts to NULL, which is not the same thing as a failed cast.
        let n = Expr::Cast(null(), Type::Int);
        assert_eq!(n.eval(&row(vec![])).unwrap(), Scalar::Null);
    }

    #[test]
    fn cross_type_comparison_is_refused_rather_than_coerced() {
        // Implicit coercion is where engines quietly disagree; the subset has none.
        let e = Expr::Compare(Cmp::Eq, col(0), int(1));
        assert!(e.eval(&row(vec![Scalar::Str("1".into())])).is_err());
    }
}

#[cfg(test)]
mod definite_type_tests {
    use super::*;

    fn col(i: usize) -> Box<Expr> {
        Box::new(Expr::Column(i))
    }
    fn lit(s: Scalar) -> Box<Expr> {
        Box::new(Expr::Literal(s))
    }
    fn s(v: &str) -> Box<Expr> {
        lit(Scalar::Str(v.into()))
    }
    fn i(v: i128) -> Box<Expr> {
        lit(Scalar::Int(v))
    }
    fn null() -> Box<Expr> {
        lit(Scalar::Null)
    }
    fn gt0() -> Expr {
        Expr::Compare(Cmp::Gt, col(0), i(0))
    }

    /// Held against the evaluator: whatever is refused fails on every row. Over an Int, a Str and a
    /// Bool column that are never NULL, and a fourth that may be.
    #[test]
    fn a_refusal_fails_on_every_row() {
        let cols = [Some(Type::Int), Some(Type::Str), Some(Type::Bool), None];
        let rows: Vec<Row> = [
            (5, Scalar::Null),
            (0, Scalar::Str("v".into())),
            (-1, Scalar::Int(2)),
        ]
        .into_iter()
        .map(|(v, maybe)| {
            Row(vec![
                Scalar::Int(v),
                Scalar::Str("x".into()),
                Scalar::Bool(v > 0),
                maybe,
            ])
        })
        .collect();
        let refused = [
            ("fee + '1'", Expr::Add(col(0), s("1"))),
            (
                "(fee + 1) * sym",
                Expr::Mul(Box::new(Expr::Add(col(0), i(1))), col(1)),
            ),
            ("fee = sym", Expr::Compare(Cmp::Eq, col(0), col(1))),
            ("flag AND fee", Expr::And(col(2), col(0))),
            ("NOT sym", Expr::Not(col(1))),
            ("CAST(sym AS BOOLEAN)", Expr::Cast(col(1), Type::Bool)),
            (
                "(fee + 'x') IS NULL",
                Expr::IsNull(Box::new(Expr::Add(col(0), s("x")))),
            ),
        ];
        for (name, e) in refused {
            assert!(e.definite_type(&cols).is_err(), "{name} must be refused");
            for r in &rows {
                assert!(
                    e.eval(r).is_err(),
                    "{name} was refused but evaluates on {r:?}"
                );
            }
        }

        // Judged at runtime instead: each depends on the row, and some run cleanly.
        let left_alone = [
            ("NULL + '1'", Expr::Add(null(), s("1"))),
            ("maybe + 1", Expr::Add(col(3), i(1))),
            (
                "COALESCE(fee, fee + '1')",
                Expr::Coalesce(vec![Expr::Column(0), Expr::Add(col(0), s("1"))]),
            ),
            (
                "CASE WHEN fee > 0 THEN fee + 'x' ELSE 0 END",
                Expr::Case {
                    whens: vec![(gt0(), Expr::Add(col(0), s("x")))],
                    otherwise: Some(i(0)),
                },
            ),
            (
                "CAST(NULL AS VARCHAR) = fee",
                Expr::Compare(Cmp::Eq, Box::new(Expr::Cast(null(), Type::Str)), col(0)),
            ),
        ];
        for (name, e) in left_alone {
            assert!(
                e.definite_type(&cols).is_ok(),
                "{name} must be left to runtime"
            );
        }
        assert_eq!(gt0().definite_type(&cols).unwrap(), Some(Type::Bool));
        assert_eq!(Expr::Add(col(3), i(1)).definite_type(&cols).unwrap(), None);
    }
}
