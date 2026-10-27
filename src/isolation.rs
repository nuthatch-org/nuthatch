//! What `[runtime] isolation = "strict"` changes about a query (RFC-0073 FR8). The threat model is
//! `docs/security/shared-runtime.md`; nothing here is claimed until its independent review has run.
//!
//! Strict mode adds two things to the guards every `/sql` statement already passes. Each tenant's
//! queries run in their own session, memory pool and memo entries (`analytics::isolated`), and a
//! statement may call only the functions [`ALLOWED_FUNCTIONS`] names.

use anyhow::{anyhow, bail, Result};
use serde::{Deserialize, Serialize};

/// How far one runtime keeps its tenants' queries apart.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Isolation {
    /// Mounts of one dataset share its session, memory pool and memo, and a statement may call any
    /// function the engine registers.
    #[default]
    Shared,
    /// Each tenant's queries get their own session, pool and memo entries, and a statement may call
    /// only [`ALLOWED_FUNCTIONS`].
    Strict,
}

impl Isolation {
    pub fn is_shared(&self) -> bool {
        *self == Isolation::Shared
    }

    /// The partition a mount's queries run in: its tenant under strict isolation, none otherwise.
    pub fn partition(&self, tenant: &str) -> Option<std::sync::Arc<str>> {
        (*self == Isolation::Strict).then(|| std::sync::Arc::from(tenant))
    }
}

/// Every function the engine registers that a strict runtime lets a statement call. Together with
/// [`WITHHELD_FUNCTIONS`] it is exactly what Burrmill registers, which
/// `the_function_lists_name_every_registered_function` holds, so a function added upstream fails
/// that test before it can reach a tenant.
pub const ALLOWED_FUNCTIONS: &[&str] = &[
    "abs",
    "acos",
    "acosh",
    "address_bytes",
    "address_text",
    "any_match",
    "any_value",
    "approx_distinct",
    "approx_median",
    "approx_percentile_cont",
    "approx_percentile_cont_with_weight",
    "array_add",
    "array_agg",
    "array_any_match",
    "array_any_value",
    "array_append",
    "array_avg",
    "array_cat",
    "array_compact",
    "array_concat",
    "array_contains",
    "array_dims",
    "array_distance",
    "array_distinct",
    "array_element",
    "array_empty",
    "array_except",
    "array_extract",
    "array_filter",
    "array_first",
    "array_has",
    "array_has_all",
    "array_has_any",
    "array_indexof",
    "array_intersect",
    "array_join",
    "array_length",
    "array_max",
    "array_min",
    "array_ndims",
    "array_normalize",
    "array_pop_back",
    "array_pop_front",
    "array_position",
    "array_positions",
    "array_prepend",
    "array_product",
    "array_push_back",
    "array_push_front",
    "array_reduce",
    "array_remove",
    "array_remove_all",
    "array_remove_n",
    "array_repeat",
    "array_replace",
    "array_replace_all",
    "array_replace_n",
    "array_resize",
    "array_reverse",
    "array_scale",
    "array_slice",
    "array_sort",
    "array_subtract",
    "array_sum",
    "array_to_string",
    "array_transform",
    "array_union",
    "arrays_overlap",
    "arrays_zip",
    "arrow_cast",
    "arrow_try_cast",
    "arrow_typeof",
    "ascii",
    "asin",
    "asinh",
    "atan",
    "atan2",
    "atanh",
    "avg",
    "bit_and",
    "bit_length",
    "bit_or",
    "bit_xor",
    "bool_and",
    "bool_or",
    "btrim",
    "burrmill_hugeint",
    "burrmill_intdiv",
    "burrmill_round_int",
    "burrmill_substr",
    "cardinality",
    "cast_to_type",
    "cbrt",
    "ceil",
    "char_length",
    "character_length",
    "checked_sum_text",
    "chr",
    "coalesce",
    "concat",
    "concat_ws",
    "contains",
    "corr",
    "cos",
    "cosh",
    "cosine_distance",
    "cot",
    "count",
    "covar",
    "covar_pop",
    "covar_samp",
    "cume_dist",
    "current_date",
    "current_time",
    "current_timestamp",
    "date_bin",
    "date_diff",
    "date_format",
    "date_part",
    "date_trunc",
    "datediff",
    "datepart",
    "datetrunc",
    "decode",
    "degrees",
    "dense_rank",
    "dot_product",
    "element_at",
    "empty",
    "encode",
    "ends_with",
    "error",
    "exp",
    "factorial",
    "find_in_set",
    "first_value",
    "flatten",
    "floor",
    "format",
    "from_hex",
    "from_json",
    "from_unixtime",
    "gcd",
    "generate_series",
    "get_field",
    "greatest",
    "grouping",
    "hex",
    "ifnull",
    "initcap",
    "inner_product",
    "instr",
    "isnan",
    "iszero",
    "json_extract",
    "json_extract_string",
    "json_type",
    "lag",
    "last_value",
    "lcm",
    "lead",
    "least",
    "left",
    "len",
    "length",
    "levenshtein",
    "list",
    "list_add",
    "list_any_match",
    "list_any_value",
    "list_append",
    "list_avg",
    "list_cat",
    "list_compact",
    "list_concat",
    "list_contains",
    "list_dims",
    "list_distance",
    "list_distinct",
    "list_element",
    "list_empty",
    "list_except",
    "list_extract",
    "list_filter",
    "list_first",
    "list_has",
    "list_has_all",
    "list_has_any",
    "list_indexof",
    "list_intersect",
    "list_join",
    "list_length",
    "list_max",
    "list_ndims",
    "list_normalize",
    "list_pop_back",
    "list_pop_front",
    "list_position",
    "list_positions",
    "list_prepend",
    "list_product",
    "list_push_back",
    "list_push_front",
    "list_reduce",
    "list_remove",
    "list_remove_all",
    "list_remove_n",
    "list_repeat",
    "list_replace",
    "list_replace_all",
    "list_replace_n",
    "list_resize",
    "list_reverse",
    "list_scale",
    "list_slice",
    "list_sort",
    "list_subtract",
    "list_sum",
    "list_to_string",
    "list_transform",
    "list_union",
    "list_zip",
    "ln",
    "log",
    "log10",
    "log2",
    "lower",
    "lpad",
    "ltrim",
    "make_array",
    "make_date",
    "make_list",
    "make_time",
    "map",
    "map_entries",
    "map_extract",
    "map_keys",
    "map_values",
    "max",
    "mean",
    "median",
    "min",
    "named_struct",
    "nanvl",
    "now",
    "nth_value",
    "ntile",
    "nullif",
    "nvl",
    "nvl2",
    "octet_length",
    "overlay",
    "percent_rank",
    "percentile_cont",
    "pi",
    "position",
    "pow",
    "power",
    "printf",
    "quantile_cont",
    "radians",
    "rand",
    "random",
    "range",
    "rank",
    "reduce",
    "regexp_count",
    "regexp_extract",
    "regexp_instr",
    "regexp_like",
    "regexp_match",
    "regexp_replace",
    "regr_avgx",
    "regr_avgy",
    "regr_count",
    "regr_intercept",
    "regr_r2",
    "regr_slope",
    "regr_sxx",
    "regr_sxy",
    "regr_syy",
    "repeat",
    "replace",
    "reverse",
    "right",
    "round",
    "row",
    "row_number",
    "rpad",
    "rtrim",
    "sign",
    "signum",
    "sin",
    "sinh",
    "split_part",
    "sqrt",
    "starts_with",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "str_split",
    "strftime",
    "string_agg",
    "string_split",
    "string_to_array",
    "string_to_list",
    "strpos",
    "struct",
    "substr",
    "substr_index",
    "substring",
    "substring_index",
    "sum",
    "tan",
    "tanh",
    "to_char",
    "to_date",
    "to_hex",
    "to_json",
    "to_local_time",
    "to_time",
    "to_timestamp",
    "to_timestamp_micros",
    "to_timestamp_millis",
    "to_timestamp_nanos",
    "to_timestamp_seconds",
    "to_unixtime",
    "today",
    "translate",
    "trim",
    "trunc",
    "try",
    "try_cast_to_type",
    "typeof",
    "unhex",
    "union_extract",
    "union_tag",
    "upper",
    "uuid",
    "var",
    "var_pop",
    "var_population",
    "var_samp",
    "var_sample",
    "with_metadata",
    "xor",
];

/// Registered, and refused under strict isolation: each describes the server or its storage rather
/// than computing on a value.
pub const WITHHELD_FUNCTIONS: &[&str] = &["arrow_field", "arrow_metadata", "version"];

/// Names a statement may use that are not in the registry: Burrmill rewrites each into an allowed
/// function before planning, or the planner reads it as a form of its own (`unnest`).
pub const DIALECT_FUNCTIONS: &[&str] = &[
    "arg_max",
    "arg_min",
    "array_reverse_sort",
    "century",
    "day",
    "dayofmonth",
    "dayofweek",
    "dayofyear",
    "decade",
    "epoch",
    "first",
    "hour",
    "if",
    "isodow",
    "last",
    "list_reverse_sort",
    "max_by",
    "microsecond",
    "millennium",
    "millisecond",
    "min_by",
    "minute",
    "month",
    "quarter",
    "regexp_full_match",
    "regexp_matches",
    "second",
    "struct_pack",
    "unnest",
    "week",
    "weekofyear",
    "year",
];

/// The host functions nuthatch itself registers, in a build that carries them.
fn host_functions() -> Vec<&'static str> {
    #[cfg(feature = "graph")]
    {
        crate::analytics_scalars::names().collect()
    }
    #[cfg(not(feature = "graph"))]
    {
        Vec::new()
    }
}

fn allowed(name: &str) -> bool {
    ALLOWED_FUNCTIONS.contains(&name)
        || DIALECT_FUNCTIONS.contains(&name)
        || host_functions().contains(&name)
}

/// The registered functions neither list names. Empty unless the engine grew one, in which case a
/// strict session refuses to open rather than offer it.
pub(crate) fn unlisted(registered: &[String]) -> Vec<String> {
    registered
        .iter()
        .filter(|n| {
            !ALLOWED_FUNCTIONS.contains(&n.as_str())
                && !WITHHELD_FUNCTIONS.contains(&n.as_str())
                && !host_functions().contains(&n.as_str())
        })
        .cloned()
        .collect()
}

/// Refuse a statement that calls a function the allow-list does not name, or that cannot be parsed
/// well enough to say. Read from Burrmill's canonical text, so a list comprehension is seen as the
/// calls it becomes.
pub(crate) fn refuse_unlisted_functions(sql: &str) -> Result<()> {
    use sqlparser::ast::{visit_expressions, Expr};
    use std::ops::ControlFlow;
    let unreadable = || {
        anyhow!("strict isolation refuses a statement it cannot parse for the functions it calls")
    };
    let canonical = burrmill::inspect::canonical(sql).ok_or_else(unreadable)?;
    let statements =
        sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::DuckDbDialect {}, &canonical)
            .map_err(|_| unreadable())?;
    let mut refused = None;
    let _ = visit_expressions(&statements, |e| {
        if let Expr::Function(f) = e {
            let parts = &f.name.0;
            let name = parts
                .last()
                .map(|p| p.to_string().trim_matches('"').to_lowercase())
                .unwrap_or_default();
            if parts.len() != 1 || !allowed(&name) {
                refused = Some(f.name.to_string());
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    });
    if let Some(name) = refused {
        bail!(
            "function `{name}` is not on this runtime's allow-list (isolation = \"strict\"); \
             docs/security/shared-runtime.md lists what is"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// FR8: the lists and the registry are the same set, so a function Burrmill or DataFusion adds
    /// fails here, and one they drop leaves a stale name that fails here too.
    #[test]
    fn the_function_lists_name_every_registered_function() {
        let session = crate::analytics::engine().open_bare().unwrap();
        let registered: std::collections::BTreeSet<String> =
            session.function_names().into_iter().collect();
        let listed: std::collections::BTreeSet<String> = ALLOWED_FUNCTIONS
            .iter()
            .chain(WITHHELD_FUNCTIONS)
            .chain(&host_functions())
            .map(|s| s.to_string())
            .collect();
        let new: Vec<_> = registered.difference(&listed).collect();
        let gone: Vec<_> = listed.difference(&registered).collect();
        assert!(
            new.is_empty() && gone.is_empty(),
            "the engine registers functions the FR8 lists do not name: {new:?}; the lists name \
             functions it no longer registers: {gone:?}. Review each new one for what it can reach \
             before adding it to ALLOWED_FUNCTIONS or WITHHELD_FUNCTIONS in src/isolation.rs, and \
             update docs/security/shared-runtime.md"
        );
        assert!(unlisted(&registered.into_iter().collect::<Vec<_>>()).is_empty());
        for name in WITHHELD_FUNCTIONS {
            assert!(!allowed(name), "{name} is both allowed and withheld");
        }
        for name in DIALECT_FUNCTIONS {
            assert!(
                !listed.contains(*name),
                "{name} is registered, so it is not a dialect name"
            );
        }
    }

    #[test]
    fn a_new_registration_is_unlisted() {
        assert_eq!(
            unlisted(&["upper".to_string(), "read_everything".to_string()]),
            vec!["read_everything".to_string()]
        );
    }

    #[test]
    fn strict_refuses_withheld_unknown_qualified_and_unparsed_calls() {
        for sql in [
            "SELECT version()",
            "SELECT \"VERSION\"()",
            "SELECT arrow_metadata(x) FROM t",
            "SELECT upper(x) FROM t WHERE x IN (SELECT version())",
            "SELECT sum(x) OVER (PARTITION BY version()) FROM t",
            "SELECT [version() FOR x IN [1]]",
            "SELECT list_transform([1], y -> version())",
            "SELECT input_file_name() FROM t",
            "SELECT read_text('/etc/hosts')",
            "SELECT main.upper(x) FROM t",
            "SELECT datafusion.public.upper(x) FROM t",
            "SELEC 1",
        ] {
            assert!(
                refuse_unlisted_functions(sql).is_err(),
                "must be refused: {sql}"
            );
        }
    }

    #[test]
    fn strict_allows_ordinary_analytical_sql() {
        for sql in [
            "SELECT count(*), sum(value_dec), max(block_number) FROM usdc__transfer",
            "SELECT \"from\", lower(\"to\") FROM usdc__transfer WHERE value_dec > 100 LIMIT 5",
            "SELECT date_trunc('day', to_timestamp(block_timestamp)) AS d, count(*) FROM t GROUP BY 1",
            "SELECT year(ts), arg_max(x, y), first(x) FROM t GROUP BY 1",
            "SELECT row_number() OVER (ORDER BY block_number) FROM t",
            "SELECT * FROM generate_series(1, 10)",
            "SELECT [lower(x) FOR x IN ['A', 'B']]",
            "SELECT CAST(x AS BIGINT), EXTRACT(year FROM ts), SUBSTRING(s FROM 2) FROM t",
            "WITH c AS (SELECT coalesce(x, 0) AS x FROM t) SELECT avg(x) FROM c",
        ] {
            refuse_unlisted_functions(sql).unwrap_or_else(|e| panic!("{sql}: {e:#}"));
        }
    }
}
