//! The query allowlist - a bounded public surface (RFC-0034 phase 1).
//!
//! `/sql` accepts arbitrary SQL. For a local developer that *is* the product; for a public endpoint it
//! is an open analytical query engine over an operator's disk. The guards that exist (concurrency 2,
//! a 30 s timeout, 50,000 result rows, 2,000,000 hot rows scanned, 16 KB of query text) are **node
//! self-protection, not a security boundary** - they bound the damage of one query and say nothing
//! about *which* queries a nest is willing to answer.
//!
//! This is where an operator says which. It lives in **mount config**, not in the nest manifest, so
//! changing it does not change the nest's identity and nothing re-indexes (RFC-0034 §2). Phase 2 adds
//! an author's ceiling in the manifest, and is gated on grafting for exactly that reason.
//!
//! ## The client sends a name, never text
//!
//! A listed query is a **named, parameterised statement**. The caller supplies `name` plus typed
//! arguments; it never supplies SQL. Matching caller-supplied SQL against patterns - a regex, a prefix,
//! an "is it a SELECT" check - is the shape of every SQL-filter bypass ever written, and we already
//! have one such check for a different purpose. This is not that.
//!
//! ## Why only `int` and `address` in phase 1
//!
//! Both have a **total validating parse** into a form with no escaping hazard: an `int` becomes
//! decimal digits, an `address` is `0x` plus 40 hex characters and cannot contain a quote. Rendering
//! either into SQL is safe by construction rather than by careful escaping.
//!
//! `text` is deliberately absent. Free text needs escaping, escaping needs to be right in every
//! dialect and every context (string literal, identifier, `LIKE` pattern), and "we escaped it
//! carefully" is how this class of bug ships. When a text parameter is genuinely needed it should
//! arrive as a bound parameter through the query layer, which is a change to `analytics.rs`, not a
//! cleverer `replace()` here.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

/// How much SQL surface a mount exposes (RFC-0034 §2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SqlAccess {
    /// Arbitrary `/sql`, bounded only by the node guards. The default, and the right one for a local
    /// `nuthatch dev` - exploration is the point.
    #[default]
    Open,
    /// No SQL at all. `/sql` and `/explain` are refused; the typed routes (`/tables`, `/entity/{id}`,
    /// `/balances`, …) still serve.
    Deny,
    /// Only the declared queries answer, by name.
    Allowlist,
}

/// A parameter's type. Deliberately tiny - see the module docs on why `text` is absent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ParamType {
    /// A signed 64-bit integer, rendered as decimal digits, parenthesised when negative.
    Int,
    /// A 20-byte hex address, rendered as a quoted lowercase literal.
    Address,
}

impl ParamType {
    /// Validate and render `raw` as a SQL literal, or say why it is not one.
    ///
    /// Every branch is a *total parse into a safe shape*, never an escape of an arbitrary string.
    fn render(&self, raw: &str) -> Result<String> {
        match self {
            ParamType::Int => {
                let n: i64 = raw
                    .parse()
                    .map_err(|_| anyhow::anyhow!("expected an integer, got {raw:?}"))?;
                // Bare, `100-{n}` with n = -1 would render `100--1` and comment out the rest of the line.
                Ok(if n < 0 {
                    format!("({n})")
                } else {
                    n.to_string()
                })
            }
            ParamType::Address => {
                let hex = raw.strip_prefix("0x").or_else(|| raw.strip_prefix("0X"));
                match hex {
                    Some(h) if h.len() == 40 && h.bytes().all(|b| b.is_ascii_hexdigit()) => {
                        Ok(format!("'0x{}'", h.to_ascii_lowercase()))
                    }
                    _ => bail!("expected a 0x-prefixed 20-byte address, got {raw:?}"),
                }
            }
        }
    }
}

/// One query a mount is willing to answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamedQuery {
    /// What a caller asks for: `GET /<mount>/q/<name>`.
    pub name: String,
    /// The statement, with `{param}` placeholders for every declared parameter.
    pub sql: String,
    /// Parameter name -> type. Every placeholder must be declared and every declaration used.
    #[serde(default)]
    pub params: std::collections::BTreeMap<String, ParamType>,
}

impl NamedQuery {
    /// Validate this query in isolation (RFC-0034 §7).
    ///
    /// Checked at **load**, not at first call: a nest facing the public must fail to start rather
    /// than serve a surface whose parameters turn out to be undeclared the first time someone asks.
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty()
            || !self
                .name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            bail!(
                "query name '{}' is invalid (allowed: letters, digits, '_', '-')",
                self.name
            );
        }
        for p in self.params.keys() {
            if p.is_empty() || !p.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
                bail!(
                    "query '{}': parameter name '{p}' is invalid (allowed: letters, digits, '_')",
                    self.name
                );
            }
        }

        // Every placeholder declared, and every declaration used. The first half closes a hole - an
        // undeclared `{x}` would otherwise survive into the SQL as literal text. The second half
        // catches a typo that would silently make a parameter unreachable.
        let used = placeholders(&self.sql)?;
        for p in &used {
            if !self.params.contains_key(p) {
                bail!(
                    "query '{}' uses {{{p}}} but does not declare it in `params`",
                    self.name
                );
            }
        }
        for p in self.params.keys() {
            if !used.contains(p) {
                bail!(
                    "query '{}' declares parameter '{p}' but never uses {{{p}}}",
                    self.name
                );
            }
        }
        Ok(())
    }

    /// Render this query with `args`, or say what is wrong with them.
    ///
    /// Missing and unknown arguments are both errors. Ignoring an unknown one would let a caller
    /// believe a filter applied when it did not.
    pub fn render(&self, args: &std::collections::HashMap<String, String>) -> Result<String> {
        for k in args.keys() {
            if !self.params.contains_key(k) {
                bail!(
                    "unknown parameter '{k}' (this query takes: {})",
                    self.param_list()
                );
            }
        }
        let mut sql = self.sql.clone();
        for (name, ty) in &self.params {
            let raw = args.get(name).ok_or_else(|| {
                anyhow::anyhow!(
                    "missing parameter '{name}' (this query takes: {})",
                    self.param_list()
                )
            })?;
            let rendered = ty
                .render(raw)
                .map_err(|e| anyhow::anyhow!("parameter '{name}': {e}"))?;
            sql = sql.replace(&format!("{{{name}}}"), &rendered);
        }
        Ok(sql)
    }

    /// `name: type, name: type` - for an error that tells a caller what to send instead.
    pub fn param_list(&self) -> String {
        if self.params.is_empty() {
            return "no parameters".to_string();
        }
        self.params
            .iter()
            .map(|(n, t)| {
                format!(
                    "{n}: {}",
                    match t {
                        ParamType::Int => "int",
                        ParamType::Address => "address",
                    }
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Every `{name}` in `sql`, refusing anything that is not a well-formed placeholder.
///
/// A stray `{` is an error rather than something to skip past: silently ignoring it is how a typo'd
/// placeholder ends up as literal text inside a query an operator believed was parameterised.
fn placeholders(sql: &str) -> Result<std::collections::BTreeSet<String>> {
    let mut out = std::collections::BTreeSet::new();
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'{' {
            let Some(end) = sql[i + 1..].find('}').map(|e| i + 1 + e) else {
                bail!("unclosed '{{' in the query text");
            };
            let name = &sql[i + 1..end];
            if name.is_empty() || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
                bail!("'{{{name}}}' is not a valid placeholder (allowed: letters, digits, '_')");
            }
            out.insert(name.to_string());
            i = end + 1;
        } else {
            i += 1;
        }
    }
    Ok(out)
}

/// A mount's whole SQL surface: the access mode plus, in allowlist mode, the queries.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Surface {
    pub access: SqlAccess,
    pub queries: Vec<NamedQuery>,
}

impl Surface {
    pub fn get(&self, name: &str) -> Option<&NamedQuery> {
        self.queries.iter().find(|q| q.name == name)
    }

    /// The declared names, for a refusal that tells a caller what they *can* ask (RFC-0016's
    /// errors-as-prompts style). A refusal that only says "no" makes an agent guess.
    pub fn names(&self) -> Vec<&str> {
        self.queries.iter().map(|q| q.name.as_str()).collect()
    }

    /// Whether free-form `/sql` and `/explain` are answerable.
    pub fn free_form_allowed(&self) -> bool {
        self.access == SqlAccess::Open
    }

    /// Validate the surface as a whole (RFC-0034 §7).
    pub fn validate(&self, mount: &str) -> Result<()> {
        let mut seen = std::collections::HashSet::new();
        for q in &self.queries {
            q.validate()
                .map_err(|e| anyhow::anyhow!("mount '{mount}': {e}"))?;
            if !seen.insert(&q.name) {
                bail!("mount '{mount}': query '{}' is declared twice", q.name);
            }
        }
        match self.access {
            // The trap this closes: an operator adds queries, believes the nest is locked down, and
            // it is still answering arbitrary SQL because they did not also set the mode. A security
            // control that silently does not apply is worse than none, so this is a refusal.
            SqlAccess::Open if !self.queries.is_empty() => bail!(
                "mount '{mount}' declares {} allowed quer{} but leaves `sql = \"open\"`, so \
                 arbitrary /sql is still answered. Set `sql = \"allowlist\"` to enforce them, or \
                 remove them.",
                self.queries.len(),
                if self.queries.len() == 1 { "y" } else { "ies" }
            ),
            SqlAccess::Allowlist if self.queries.is_empty() => bail!(
                "mount '{mount}' sets `sql = \"allowlist\"` but declares no queries, so it would \
                 answer nothing. Use `sql = \"deny\"` if that is the intent."
            ),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn q(name: &str, sql: &str, params: &[(&str, ParamType)]) -> NamedQuery {
        NamedQuery {
            name: name.into(),
            sql: sql.into(),
            params: params.iter().map(|(n, t)| (n.to_string(), *t)).collect(),
        }
    }

    fn args(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn a_parameter_is_rendered_only_from_a_total_parse() {
        let query = q(
            "holder",
            "SELECT * FROM t WHERE addr = {who} LIMIT {n}",
            &[("who", ParamType::Address), ("n", ParamType::Int)],
        );
        query.validate().unwrap();

        let sql = query
            .render(&args(&[
                ("who", "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
                ("n", "25"),
            ]))
            .unwrap();
        assert_eq!(
            sql,
            "SELECT * FROM t WHERE addr = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' LIMIT 25"
        );
    }

    #[test]
    fn a_negative_int_cannot_form_a_comment() {
        let query = q(
            "x",
            "SELECT * FROM t WHERE amount > 100-{n} AND owner = {who}",
            &[("n", ParamType::Int), ("who", ParamType::Address)],
        );
        let sql = query
            .render(&args(&[
                ("n", "-1"),
                ("who", "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
            ]))
            .unwrap();
        assert!(!sql.contains("--"), "{sql}");
        assert_eq!(
            sql,
            "SELECT * FROM t WHERE amount > 100-(-1) AND owner = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'"
        );
    }

    /// The assertion the whole design exists for: a caller cannot get SQL of their own into the
    /// statement. Each of these is refused by the *parse*, not by escaping.
    #[test]
    fn no_argument_can_inject_sql() {
        let query = q(
            "holder",
            "SELECT * FROM t WHERE addr = {who} LIMIT {n}",
            &[("who", ParamType::Address), ("n", ParamType::Int)],
        );
        for (param, evil) in [
            ("n", "1; DROP TABLE t"),
            ("n", "1 OR 1=1"),
            ("n", "1/**/UNION/**/SELECT"),
            ("n", ""),
            ("who", "0x' OR '1'='1"),
            ("who", "'; ATTACH 'evil.db'; --"),
            ("who", "0xAAAA"),
            ("who", "0xZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ"),
        ] {
            let mut a = args(&[
                ("who", "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
                ("n", "1"),
            ]);
            a.insert(param.to_string(), evil.to_string());
            let err = query
                .render(&a)
                .expect_err(&format!("{param}={evil:?} must be refused"))
                .to_string();
            assert!(
                err.contains(param),
                "the refusal should name the parameter: {err}"
            );
        }
    }

    #[test]
    fn missing_and_unknown_arguments_are_both_errors() {
        let query = q("top", "SELECT * FROM t LIMIT {n}", &[("n", ParamType::Int)]);

        let err = query.render(&args(&[])).unwrap_err().to_string();
        assert!(err.contains("missing parameter 'n'"), "{err}");
        assert!(
            err.contains("n: int"),
            "the error must say what to send: {err}"
        );

        // Silently ignoring an unknown argument would let a caller believe a filter applied.
        let err = query
            .render(&args(&[("n", "1"), ("limit", "5")]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("unknown parameter 'limit'"), "{err}");
    }

    #[test]
    fn placeholders_and_declarations_must_agree() {
        let undeclared = q("a", "SELECT {x}", &[]);
        assert!(undeclared
            .validate()
            .unwrap_err()
            .to_string()
            .contains("does not declare it"));

        let unused = q("a", "SELECT 1", &[("x", ParamType::Int)]);
        assert!(unused
            .validate()
            .unwrap_err()
            .to_string()
            .contains("never uses"));

        // A stray brace is an error, not something to skip: a typo'd placeholder must not survive
        // into the SQL as literal text.
        for bad in ["SELECT {", "SELECT {a b}", "SELECT {}", "SELECT {a-b}"] {
            let query = q("a", bad, &[]);
            assert!(query.validate().is_err(), "{bad:?} should not validate");
        }
    }

    /// The configuration trap: queries declared but not enforced. Refused, because a security
    /// control that silently does not apply is worse than none.
    #[test]
    fn declaring_queries_without_enforcing_them_is_refused() {
        let s = Surface {
            access: SqlAccess::Open,
            queries: vec![q("a", "SELECT 1", &[])],
        };
        let err = s.validate("usdc").unwrap_err().to_string();
        assert!(err.contains("still answered"), "{err}");
        assert!(
            err.contains("allowlist"),
            "the error must say the fix: {err}"
        );

        // ...and the mirror: enforcing an empty list would answer nothing at all.
        let s = Surface {
            access: SqlAccess::Allowlist,
            queries: vec![],
        };
        let err = s.validate("usdc").unwrap_err().to_string();
        assert!(err.contains("declares no queries"), "{err}");
        assert!(
            err.contains("deny"),
            "the error must name the alternative: {err}"
        );
    }

    #[test]
    fn duplicate_query_names_are_refused() {
        let s = Surface {
            access: SqlAccess::Allowlist,
            queries: vec![q("a", "SELECT 1", &[]), q("a", "SELECT 2", &[])],
        };
        assert!(s
            .validate("usdc")
            .unwrap_err()
            .to_string()
            .contains("declared twice"));
    }

    #[test]
    fn free_form_is_allowed_only_when_open() {
        for (access, allowed) in [
            (SqlAccess::Open, true),
            (SqlAccess::Deny, false),
            (SqlAccess::Allowlist, false),
        ] {
            let s = Surface {
                access,
                queries: vec![],
            };
            assert_eq!(s.free_form_allowed(), allowed, "{access:?}");
        }
    }

    // ---- phase 2: the author's ceiling ----

    fn ceiling(qs: &[NamedQuery]) -> Ceiling {
        Ceiling {
            queries: qs.to_vec(),
        }
    }

    /// RFC-0034 §3: narrowing is the only permitted direction.
    #[test]
    fn a_mount_may_narrow_the_ceiling_but_never_widen_it() {
        let c = ceiling(&[
            q(
                "holders",
                "SELECT * FROM t LIMIT {n}",
                &[("n", ParamType::Int)],
            ),
            q("total", "SELECT count(*) FROM t", &[]),
        ]);

        // Narrowing: expose one of the two.
        let narrow = Surface {
            access: SqlAccess::Allowlist,
            queries: vec![q("total", "SELECT count(*) FROM t", &[])],
        };
        narrow
            .validate_within(&c, "usdc")
            .expect("narrowing is fine");

        // Exposing everything the author sanctioned is also fine - the ceiling is a maximum.
        let all = Surface {
            access: SqlAccess::Allowlist,
            queries: c.queries.clone(),
        };
        all.validate_within(&c, "usdc")
            .expect("the full ceiling is fine");

        // Widening by adding a name the author never sanctioned.
        let wider = Surface {
            access: SqlAccess::Allowlist,
            queries: vec![q("secrets", "SELECT * FROM t", &[])],
        };
        let err = wider.validate_within(&c, "usdc").unwrap_err().to_string();
        assert!(err.contains("did not sanction"), "{err}");
        assert!(
            err.contains("holders") && err.contains("total"),
            "the refusal must say what IS sanctioned: {err}"
        );
    }

    /// The dangerous widening, and the one a name-only check would miss: keep a sanctioned **name**
    /// and change what it does. The surface still reads as within the ceiling.
    #[test]
    fn redefining_a_sanctioned_name_is_still_widening() {
        let c = ceiling(&[q("total", "SELECT count(*) FROM t", &[])]);

        for (label, forged) in [
            (
                "different statement",
                q("total", "SELECT * FROM secrets", &[]),
            ),
            (
                "smuggled parameter",
                q(
                    "total",
                    "SELECT count(*) FROM t LIMIT {n}",
                    &[("n", ParamType::Int)],
                ),
            ),
        ] {
            let s = Surface {
                access: SqlAccess::Allowlist,
                queries: vec![forged],
            };
            let err = s.validate_within(&c, "usdc").unwrap_err().to_string();
            assert!(err.contains("redefines"), "{label} must be refused: {err}");
        }
    }

    /// #2135: an open surface names no queries, so a name check alone passes it against any
    /// ceiling while it answers any statement. Under a ceiling it serves the ceiling.
    #[test]
    fn an_open_mount_under_a_ceiling_serves_the_ceiling() {
        let c = ceiling(&[q("total", "SELECT count(*) FROM t", &[])]);
        let served = Surface::default().within(&c, "usdc").unwrap();
        assert_eq!(served.access, SqlAccess::Allowlist);
        assert_eq!(served.queries, c.queries);

        let deny = Surface {
            access: SqlAccess::Deny,
            queries: vec![],
        };
        assert_eq!(deny.clone().within(&c, "usdc").unwrap(), deny);
        let narrow = Surface {
            access: SqlAccess::Allowlist,
            queries: vec![],
        };
        assert_eq!(narrow.clone().within(&c, "usdc").unwrap(), narrow);
    }

    /// Only a missing `queries.toml` means no ceiling. One that exists and cannot be read is an
    /// error, or the mount would serve free-form SQL past its author's ceiling.
    #[test]
    fn an_unreadable_ceiling_is_an_error_not_an_absent_one() {
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir(d.path().join(CEILING_FILE)).unwrap();
        assert!(Ceiling::load(d.path()).is_err());
    }

    /// No ceiling declared → phase 1's behaviour, unchanged. An author who has not opted in must not
    /// find their nest suddenly unmountable.
    #[test]
    fn a_nest_without_a_ceiling_is_unconstrained() {
        let d = tempfile::tempdir().unwrap();
        assert!(Ceiling::load(d.path()).unwrap().is_none());

        // And an empty ceiling is not the same as an absent one: it sanctions nothing.
        let empty = ceiling(&[]);
        let s = Surface {
            access: SqlAccess::Allowlist,
            queries: vec![q("anything", "SELECT 1", &[])],
        };
        let err = s.validate_within(&empty, "usdc").unwrap_err().to_string();
        assert!(err.contains("(none)"), "{err}");
    }

    #[test]
    fn a_ceiling_is_validated_when_it_is_read() {
        let d = tempfile::tempdir().unwrap();
        // An undeclared placeholder is as wrong in a ceiling as in a mount.
        std::fs::write(
            d.path().join(CEILING_FILE),
            "[[queries]]\nname = \"bad\"\nsql = \"SELECT {x}\"\n",
        )
        .unwrap();
        assert!(Ceiling::load(d.path())
            .unwrap_err()
            .to_string()
            .contains("does not declare"));

        std::fs::write(
            d.path().join(CEILING_FILE),
            "[[queries]]\nname = \"a\"\nsql = \"SELECT 1\"\n\n[[queries]]\nname = \"a\"\nsql = \"SELECT 2\"\n",
        )
        .unwrap();
        assert!(Ceiling::load(d.path())
            .unwrap_err()
            .to_string()
            .contains("twice"));

        // The happy path round-trips.
        std::fs::write(
            d.path().join(CEILING_FILE),
            "[[queries]]\nname = \"total\"\nsql = \"SELECT count(*) FROM t\"\n",
        )
        .unwrap();
        let c = Ceiling::load(d.path()).unwrap().unwrap();
        assert_eq!(c.queries.len(), 1);
        assert_eq!(c.queries[0].name, "total");
    }

    /// A query name is a path segment (`/<mount>/q/<name>`), so it gets the same charset as a nest
    /// name and a tenant.
    #[test]
    fn a_query_name_is_a_path_segment() {
        for bad in ["../escape", "a/b", "", "a b"] {
            assert!(
                q(bad, "SELECT 1", &[]).validate().is_err(),
                "{bad:?} should not validate as a query name"
            );
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Phase 2: the author's ceiling (RFC-0034 §3)
// ---------------------------------------------------------------------------------------------

/// The file a nest author declares their sanctioned query surface in.
///
/// **Its own file, deliberately, and this is the load-bearing detail.** The ceiling is an *authored
/// input*, so it belongs in the bundle and therefore in the NID - §3 is explicit about that. But
/// `nuthatch.toml` is part of the **data identity** (RFC-0033 §5), so putting the ceiling there would
/// move the data identity on every security tweak, defeat early cutoff, and force the chain to
/// re-index. A separate file is excluded from the data identity exactly as `views/` is: in the NID,
/// out of the data.
pub const CEILING_FILE: &str = "queries.toml";

/// What a nest's **author** sanctions being asked of it (RFC-0034 §3).
///
/// A mount may narrow within this, never widen it. The point is that a published nest becomes
/// self-describing about what it answers - a property a registry needs and an operator cannot supply
/// for somebody else's nest.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ceiling {
    #[serde(default)]
    pub queries: Vec<NamedQuery>,
}

impl Ceiling {
    /// Read `queries.toml` from a nest directory. Absent → no ceiling declared, and a mount may
    /// expose whatever its operator chooses (phase 1's behaviour, unchanged).
    pub fn load(nest_dir: &std::path::Path) -> Result<Option<Ceiling>> {
        let path = nest_dir.join(CEILING_FILE);
        // Only a missing file means no ceiling; one that cannot be read must not open the mount.
        let raw = match std::fs::read_to_string(&path) {
            Ok(raw) => raw,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
        };
        let ceiling: Ceiling =
            toml::from_str(&raw).with_context(|| format!("parsing {}", path.display()))?;
        crate::config::warn_unknown::<Ceiling>(CEILING_FILE, &raw);
        for q in &ceiling.queries {
            q.validate()
                .map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
        }
        let mut seen = std::collections::HashSet::new();
        for q in &ceiling.queries {
            if !seen.insert(&q.name) {
                bail!("{}: query '{}' is declared twice", path.display(), q.name);
            }
        }
        Ok(Some(ceiling))
    }

    fn get(&self, name: &str) -> Option<&NamedQuery> {
        self.queries.iter().find(|q| q.name == name)
    }
}

impl Surface {
    /// Refuse a mount that exposes more than its author sanctioned (RFC-0034 §3).
    ///
    /// Narrowing is the whole permitted direction. Two ways to widen, both refused:
    ///
    /// 1. **A name the ceiling does not contain.** Obvious.
    /// 2. **A name the ceiling contains, redefined.** Less obvious and more dangerous: an operator
    ///    could keep `holder_balance` and change its SQL to select something the author never
    ///    sanctioned, while the surface still *reads* as within the ceiling. So the statement and its
    ///    parameters must match too, not just the name.
    pub fn validate_within(&self, ceiling: &Ceiling, mount: &str) -> Result<()> {
        for q in &self.queries {
            let Some(sanctioned) = ceiling.get(&q.name) else {
                bail!(
                    "mount '{mount}' exposes query '{}', which this nest's author did not sanction. \
                     A mount may narrow the author's ceiling, never widen it. Sanctioned: {}",
                    q.name,
                    if ceiling.queries.is_empty() {
                        "(none)".to_string()
                    } else {
                        ceiling
                            .queries
                            .iter()
                            .map(|c| c.name.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    }
                );
            };
            if sanctioned.sql != q.sql || sanctioned.params != q.params {
                bail!(
                    "mount '{mount}' redefines query '{}': the name is sanctioned but the statement \
                     is not the author's. Widening by redefinition is still widening.",
                    q.name
                );
            }
        }
        Ok(())
    }

    /// What a mount serves under its author's ceiling. `open` names no narrowing, so it serves the
    /// ceiling itself, the widest a mount may be (#2135).
    pub fn within(self, ceiling: &Ceiling, mount: &str) -> Result<Surface> {
        self.validate_within(ceiling, mount)?;
        Ok(match self.access {
            SqlAccess::Open => Surface {
                access: SqlAccess::Allowlist,
                queries: ceiling.queries.clone(),
            },
            SqlAccess::Deny | SqlAccess::Allowlist => self,
        })
    }
}
