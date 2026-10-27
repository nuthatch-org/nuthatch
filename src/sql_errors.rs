//! Errors as prompts (RFC-0016 §3). A failed agent query is a teaching opportunity: instead of
//! relaying a raw `Binder Error: …` and costing a round-trip to rediscover what `schema` already
//! knows, we classify the failure against the registry and append a one-line, actionable hint so the
//! agent self-corrects in one shot. The raw engine message is always preserved (we never lie about
//! what the engine said); the hint is appended.
//!
//! This is pure text-in / text-out over the schema - nothing here touches the data path. It enriches
//! the `/sql` (and thus MCP `sql`) error surface and the `nuthatch sql` REPL alike.

use crate::registry::TableSchema;
use crate::semantic::derive_footguns;

/// The engine ran out of its memory budget. Binder/parser/catalog echoes of the phrase are the
/// caller's text, not an OOM.
pub fn is_out_of_memory(raw: &str) -> bool {
    raw.contains("Out of Memory Error")
        && !raw.contains("Binder Error")
        && !raw.contains("Parser Error")
        && !raw.contains("Catalog Error")
}

/// Classify an engine error for `query` against the nest `schema`, returning an actionable hint if the
/// failure matches a known class (`None` otherwise - an unrecognised error is relayed raw, unadorned).
/// The classes mirror the RFC-0016 §4 table. Burrmill opens each DataFusion error it recognises with
/// the first line DuckDB gave the same fault and keeps DataFusion's text below it (burrmill's
/// `df/errors.rs`), so the classes key on that line. `through_burrmill` drives each fault for real.
pub fn enrich(raw: &str, query: &str, schema: &[TableSchema]) -> Option<String> {
    // Unknown table: `Catalog Error: Table with name <X> does not exist!`
    if let Some(name) = between(raw, "Table with name ", " does not exist") {
        let name = name.trim();
        // #663: `name` can be a table this nest genuinely declares - the config is correct, the event
        // just has never fired on this chain (or the call/state read has never populated). That is a
        // different fault from a typo or a table nobody declared, and conflating them is exactly what
        // left an operator staring at a bare catalog error with nothing explaining why. `define_views`
        // should already turn this into an empty view rather than a missing table (this schema is the
        // live one, not a possibly-stale `schema.json`), so reaching this branch for a declared table
        // means the empty-view path itself didn't cover this query - still worth naming precisely.
        if let Some(t) = schema.iter().find(|t| t.table == name) {
            return Some(match t.kind {
                crate::registry::TableKind::Event => format!(
                    "`{name}` is declared (event `{}`) but has no data yet - it has likely never \
                     fired on this chain. The table exists once the event does; until then it reads \
                     as empty, not absent.",
                    t.event
                ),
                _ => format!(
                    "`{name}` is declared but has no data yet - it has likely never been populated on \
                     this chain. The table exists once it is; until then it reads as empty, not absent."
                ),
            });
        }
        let tables: Vec<&str> = schema.iter().map(|t| t.table.as_str()).collect();
        return Some(match closest(name, &tables) {
            Some(c) => format!(
                "no table `{name}`; the closest is `{c}`. Call the `schema` tool for the full list."
            ),
            None => format!("no table `{name}`. Call the `schema` tool for the list of tables."),
        });
    }

    // Unknown column: `Binder Error: Referenced column "<X>" not found in FROM clause!`
    if let Some(col) = quoted_after(raw, "Referenced column ") {
        // A big-int helper the agent guessed wrong: they wrote `foo` but meant the `foo_dec` companion,
        // or vice-versa. Suggest the sibling if it exists before a generic fuzzy match.
        let all_cols: Vec<String> = schema
            .iter()
            .flat_map(|t| t.columns.iter().map(|c| c.name.clone()))
            .collect();
        let big_ints: Vec<String> = schema
            .iter()
            .flat_map(|t| derive_footguns(t).big_ints)
            .collect();
        // The engine lists the fields in scope; a hint drawn from every table's columns can name one
        // this query cannot see, so every suggestion below is held to that scope, or not made.
        let in_scope = fields_in_scope(raw);
        if big_ints.iter().any(|b| format!("{b}_dec") == col)
            && !all_cols.contains(&col.to_string())
            && in_scope.iter().any(|c| c == col.trim_end_matches("_dec"))
        {
            return Some(format!(
                "`{col}` is derived on the fly - it isn't a stored column, but you *can* select it. If \
                 the binder rejected it, the base column exists as `{}` (exact text).",
                col.trim_end_matches("_dec")
            ));
        }
        // A `{col}_dec` whose *base* column the schema doesn't know either: the schema is very likely
        // stale - e.g. the author hand-added a `[[templates]]`/`[[factories]]` to `nuthatch.toml` (whose
        // `{template}__{event}` tables `init`/`add` never generated a schema for), so the derived `_dec`
        // columns were never created. Point at the regen command, not a fuzzy typo match.
        if let Some(base) = col.strip_suffix("_dec") {
            if !all_cols.iter().any(|c| c == base) {
                return Some(format!(
                    "`{col}` doesn't exist because its base column `{base}` isn't in the schema. If you \
                     hand-edited `nuthatch.toml` (e.g. added a factory template), run `nuthatch schema` \
                     to regenerate `schema.json` and the derived `_dec` columns, then retry."
                ));
            }
        }
        let refs: Vec<&str> = in_scope.iter().map(String::as_str).collect();
        return Some(match closest(&col, &refs) {
            Some(c) => format!(
                "no column `{col}`; the closest is `{c}`. Call `schema` for this table's columns."
            ),
            None => format!("no column `{col}`. Call `schema` for the columns."),
        });
    }

    // Reserved word: `Parser Error: syntax error at or near …` when a reserved-word column appears
    // unquoted in the query. The token the parser names is not reliably the column, so we detect via
    // the schema: a reserved-word column mentioned bare is the culprit.
    if raw.contains("syntax error") {
        for t in schema {
            for rc in derive_footguns(t).reserved_words {
                if mentions_unquoted(query, &rc) {
                    return Some(format!(
                        "`{rc}` is a SQL reserved word and a column of `{}` - double-quote it: SELECT \"{rc}\" …",
                        t.table
                    ));
                }
            }
        }
    }

    // Big-int arithmetic on the raw text column: `Binder Error: No function matches … 'sum(VARCHAR)'`.
    if raw.contains("No function matches") && raw.contains("VARCHAR") {
        for t in schema {
            for bc in derive_footguns(t).big_ints {
                if mentions_in_aggregate(query, &bc) {
                    let overflows = derive_footguns(t).overflows_dec.iter().any(|c| c == &bc);
                    return Some(if overflows {
                        format!(
                            "`{bc}` is an exact-text big integer (uint/int > 64-bit). For amounts \
                             that fit in 38 digits, use `{bc}_dec`. For ids, nonces and hashes \
                             `{bc}_dec` is NULL - use the raw `{bc}` column."
                        )
                    } else {
                        format!(
                            "`{bc}` is an exact-text big integer (uint/int > 64-bit); use `{bc}_dec` for \
                             SUM/AVG/comparisons, not the raw column."
                        )
                    });
                }
            }
        }
    }

    // #539: a Solidity bool stored as exact text `'true'`/`'false'`, used somewhere that needs a real
    // BOOLEAN. `enabled = true`/`AND`/`NOT` implicitly cast against a VARCHAR and just work, but a
    // function requiring a uniform type across its arguments does not:
    //   `Binder Error: Cannot mix values of type VARCHAR and BOOLEAN in COALESCE operator - an
    //    explicit cast is required` (COALESCE, CASE, UNION - order of the two types in the message
    //    varies with the construct, CASE naming its ELSE type first, so check both), or
    //   `Binder Error: No function matches the given name and argument types 'bool_and(VARCHAR)'`
    //    (an aggregate that only accepts BOOLEAN, e.g. bool_and/bool_or).
    let mixed_types = raw.contains("Cannot mix values of type")
        && raw.contains("VARCHAR")
        && raw.contains("BOOLEAN");
    let bool_aggregate = raw.contains("No function matches")
        && (raw.contains("bool_and(VARCHAR)") || raw.contains("bool_or(VARCHAR)"));
    if mixed_types || bool_aggregate {
        let lowered = query.to_ascii_lowercase();
        for t in schema {
            for bc in derive_footguns(t).bools {
                if contains_word(&lowered, &bc.to_ascii_lowercase()) {
                    return Some(format!(
                        "`{bc}` is a Solidity bool stored as exact text `'true'`/`'false'`, not a SQL \
                         boolean; direct comparisons (`{bc} = true`) and `AND`/`NOT` implicitly cast \
                         and work, but COALESCE/CASE/bool_and/bool_or/UNION need matching types - write \
                         `{bc} = 'true'` or `CAST({bc} AS BOOLEAN)`."
                    ));
                }
            }
        }
    }

    // RFC-0047 C4: a query that cannot run in its budget names the keys. Ingestion is not degraded.
    if is_out_of_memory(raw) {
        if raw.contains("max_temp_directory_size") {
            return Some(
                "this query exceeded analytics.max_temp_size (NUTHATCH_ANALYTICS_MAX_TEMP_SIZE). \
                 It cannot run in its disk budget; block processing is not degraded. Raise \
                 analytics.max_temp_size or reduce the query. That cap is disk, not RAM, and does \
                 not change the 2 GiB cursor equation."
                    .into(),
            );
        }
        return Some(
            "this query exceeded the SQL engine's memory limit: NUTHATCH_BURRMILL_MEMORY_LIMIT when it \
             is set, otherwise analytics.memory_limit (NUTHATCH_ANALYTICS_MEMORY_LIMIT, default \
             512MB). It cannot run in its budget; block processing is not degraded. Lower the \
             query, or raise the limit in effect only if (sql_permits × that limit) + \
             ingestion_reservation still fits the 2 GiB per-cursor ceiling."
                .into(),
        );
    }

    // #433: a sealed segment whose *data region* is corrupt but whose Parquet footer is intact binds
    // cleanly and then fails at execution, with a Parquet decode error that names no file, so an
    // operator cannot tell a bad file from a bad query. Matched last, so a precise hint always wins,
    // and only on the engine's own text (see `parquet_read_failure`): a match on caller input would
    // tell an operator their healthy nest holds a corrupt file.
    //
    // Deliberately without an integrity scan. Hashing the nest's segments would name the exact file,
    // but it would put an unbounded, caller-triggered sweep on the query path - the cost bound #476
    // and #478 are already about, reachable by anyone who can send a query.
    if parquet_read_failure(raw) {
        // Case-folded, like the sibling `mentions_unquoted`: Burrmill resolves unquoted identifiers
        // case-insensitively, so `FROM USDC__Transfer` is a valid way to name `usdc__transfer`, and
        // failing to fold here would drop the table name - the exact "names nothing" complaint #433
        // was filed about.
        let lowered = query.to_ascii_lowercase();
        let touched: Vec<&str> = schema
            .iter()
            .map(|t| t.table.as_str())
            .filter(|t| contains_word(&lowered, &t.to_ascii_lowercase()))
            .collect();
        let which = match touched.as_slice() {
            [] => "a sealed segment behind this query".to_string(),
            [one] => format!("a sealed segment of `{one}`"),
            many => format!("a sealed segment of one of `{}`", many.join("`, `")),
        };
        return Some(format!(
            "{which} binds but cannot be read - its Parquet footer is intact, so the view built over \
             it and the failure landed at execution instead. This is a corrupt file on disk, not a \
             problem with your query: re-running it unchanged fails the same way. Restart the nest \
             and the startup integrity pass hashes every segment against the manifest. A segment in \
             this nest's own directory is quarantined and the table then serves the rows that \
             remain; a segment in a runtime's *shared* store is reported in the log and deliberately \
             left in place, because other datasets reference those bytes (RFC-0033 §11a) - that one \
             is yours to remove once you know what else it feeds."
        ));
    }

    None
}

/// The substring strictly between the first occurrence of `a` and the next occurrence of `b` after it.
fn between<'a>(s: &'a str, a: &str, b: &str) -> Option<&'a str> {
    let start = s.find(a)? + a.len();
    let rest = &s[start..];
    let end = rest.find(b)?;
    Some(&rest[..end])
}

/// A Parquet decode failure in the engine's own words. Caller text reaches an error only as a name
/// or inside quotes, so the phrase must not follow a quote, nor sit in a binder, parser or catalog
/// error. A file that is missing rather than corrupt reads as a Parquet error too, and is not this.
fn parquet_read_failure(raw: &str) -> bool {
    raw.find("Parquet error:")
        .is_some_and(|at| !raw[..at].contains(['"', '\'']))
        && !raw.contains("No such file or directory")
        && !raw.contains("Binder Error")
        && !raw.contains("Parser Error")
        && !raw.contains("Catalog Error")
}

/// The columns DataFusion lists as in scope, from `Valid fields are t.a, t.b.`, unqualified.
fn fields_in_scope(raw: &str) -> Vec<String> {
    let Some(at) = raw.find("Valid fields are ") else {
        return Vec::new();
    };
    let line = raw[at + "Valid fields are ".len()..]
        .lines()
        .next()
        .unwrap_or("");
    line.trim_end_matches('.')
        .split(", ")
        .map(|f| {
            f.rsplit('.')
                .next()
                .unwrap_or(f)
                .trim_matches('"')
                .to_string()
        })
        .filter(|f| !f.is_empty())
        .collect()
}

/// The text inside the first pair of double-quotes that appears after `marker`.
fn quoted_after(s: &str, marker: &str) -> Option<String> {
    let after = &s[s.find(marker)? + marker.len()..];
    let open = after.find('"')? + 1;
    let rest = &after[open..];
    let close = rest.find('"')?;
    Some(rest[..close].to_string())
}

/// Does `col` appear in `query` as a bare (un-double-quoted) identifier? If it were quoted the query
/// would have parsed, so a reserved-word column that shows up in the text unquoted is the culprit.
fn mentions_unquoted(query: &str, col: &str) -> bool {
    let q = query.to_ascii_lowercase();
    let c = col.to_ascii_lowercase();
    contains_word(&q, &c) && !q.contains(&format!("\"{c}\""))
}

/// Does `col` (raw) appear inside an aggregate call in `query`? Matches `sum(col)`, `avg(col,` etc.
/// after stripping whitespace/quotes, with a closing `)`/`,` so `col` isn't a prefix of `col_dec`.
fn mentions_in_aggregate(query: &str, col: &str) -> bool {
    let stripped: String = query
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '"')
        .collect::<String>()
        .to_ascii_lowercase();
    let c = col.to_ascii_lowercase();
    for op in ["sum(", "avg(", "min(", "max(", "total("] {
        for close in [")", ","] {
            if stripped.contains(&format!("{op}{c}{close}")) {
                return true;
            }
        }
    }
    false
}

/// Whole-word containment: `col` bounded by non-alphanumeric/underscore (so `to` doesn't match
/// `token`). Cheap and dependency-free.
fn contains_word(haystack: &str, word: &str) -> bool {
    let bytes = haystack.as_bytes();
    let mut from = 0;
    while let Some(pos) = haystack[from..].find(word) {
        let i = from + pos;
        let before_ok = i == 0 || !is_ident(bytes[i - 1]);
        let after = i + word.len();
        let after_ok = after >= bytes.len() || !is_ident(bytes[after]);
        if before_ok && after_ok {
            return true;
        }
        from = i + word.len();
    }
    false
}

fn is_ident(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// The closest real candidate to `name`. Tries, in order: an exact (case-insensitive) hit; then
/// **containment** - the most common agent slip is dropping the `{alias}__` prefix (`transfers` for
/// `usdc__transfer`) or pluralising, so a candidate that contains the de-pluralised guess wins; then
/// **Levenshtein** within a sane budget (≤ 3, or half the name) for genuine typos (`valu` → `value`).
/// `None` if nothing is close enough - suggestions therefore always come from the real schema, never
/// hallucinated.
fn closest<'a>(name: &str, candidates: &[&'a str]) -> Option<&'a str> {
    let n = name.to_ascii_lowercase();
    let n_sing = n.strip_suffix('s').unwrap_or(&n);

    if let Some(c) = candidates.iter().find(|c| c.eq_ignore_ascii_case(name)) {
        return Some(c);
    }
    // Containment on the de-pluralised guess (`transfer` ⊆ `usdc__transfer`). Prefer the shortest
    // matching candidate - the most specific.
    if let Some(c) = candidates
        .iter()
        .filter(|c| {
            let cl = c.to_ascii_lowercase();
            (n.len() >= 3 && cl.contains(&n)) || (n_sing.len() >= 3 && cl.contains(n_sing))
        })
        .min_by_key(|c| c.len())
    {
        return Some(c);
    }
    // Genuine typo: nearest by edit distance, within budget.
    let budget = (name.len() / 2).max(3);
    candidates
        .iter()
        .map(|c| (*c, levenshtein(&n, &c.to_ascii_lowercase())))
        .filter(|(_, d)| *d <= budget)
        .min_by_key(|(_, d)| *d)
        .map(|(c, _)| c)
}

/// Classic Levenshtein edit distance (two-row DP). Small strings, so O(n·m) is fine.
fn levenshtein(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut cur = vec![0; b.len() + 1];
    for (i, ca) in a.iter().enumerate() {
        cur[0] = i + 1;
        for (j, cb) in b.iter().enumerate() {
            let cost = if ca == cb { 0 } else { 1 };
            cur[j + 1] = (prev[j + 1] + 1).min(cur[j] + 1).min(prev[j] + cost);
        }
        std::mem::swap(&mut prev, &mut cur);
    }
    prev[b.len()]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{ColumnSchema, TableSchema};

    pub(super) fn schema() -> Vec<TableSchema> {
        vec![TableSchema {
            table: "usdc__transfer".into(),
            alias: "usdc".into(),
            kind: crate::registry::TableKind::Event,
            function: String::new(),
            selector: String::new(),
            event: "Transfer".into(),
            topic0: "0xddf2".into(),
            columns: vec![
                ColumnSchema {
                    name: "from".into(),
                    sol_type: "address".into(),
                    storage: "address".into(),
                    indexed: true,
                    components: Vec::new(),
                },
                ColumnSchema {
                    name: "to".into(),
                    sol_type: "address".into(),
                    storage: "address".into(),
                    indexed: true,
                    components: Vec::new(),
                },
                ColumnSchema {
                    name: "value".into(),
                    sol_type: "uint256".into(),
                    storage: "word32".into(),
                    indexed: false,
                    components: Vec::new(),
                },
                ColumnSchema {
                    name: "enabled".into(),
                    sol_type: "bool".into(),
                    storage: "bool".into(),
                    indexed: false,
                    components: Vec::new(),
                },
                ColumnSchema {
                    name: "block_number".into(),
                    sol_type: "implicit".into(),
                    storage: "u64".into(),
                    indexed: false,
                    components: Vec::new(),
                },
            ],
        }]
    }

    /// Burrmill's message for a segment whose pages were overwritten and footer left intact
    /// (`through_burrmill::a_segment_corrupt_past_its_footer`): it binds, and the read names nothing.
    const CORRUPT: &str =
        "query failed: substrate error: Parquet error: External: Parquet argument \
                           error: Parquet error: Unexpected struct field type 15";

    /// #433: the page-corrupt-segment failure.
    #[test]
    fn a_page_corrupt_segment_is_named_as_a_bad_file_not_a_bad_query() {
        let raw = CORRUPT;
        let hint = enrich(raw, "SELECT count(*) FROM usdc__transfer", &schema()).unwrap();
        // Names the table whose segments to suspect.
        assert!(
            hint.contains("usdc__transfer"),
            "must name the queried table: {hint}"
        );
        // Says whose fault it is. The operator's next move differs entirely from a bad query.
        assert!(
            hint.contains("corrupt file on disk"),
            "must say this is a file, not a query: {hint}"
        );
        assert!(
            hint.contains("Restart"),
            "must point at the pass that identifies the file: {hint}"
        );
        // `doctor` does not run `verify_and_quarantine` - only `build_nest` does - so it must not be
        // offered as the remedy.
        assert!(
            !hint.contains("doctor"),
            "must not recommend a command that does not run the integrity pass: {hint}"
        );
        // And the remedy must not over-promise: `verify_and_quarantine` deliberately refuses to
        // quarantine a segment in a runtime's shared store (RFC-0033 §11a), so telling every
        // operator that restarting clears it would be a restart loop for anyone on that layout.
        assert!(
            hint.contains("shared"),
            "must say the shared-store case is not auto-quarantined: {hint}"
        );
    }

    /// The classifier must not fire on the caller's own text. The engine echoes query text back in
    /// binder errors, so a bare substring match would let anyone who can send a query make a healthy
    /// nest report a corrupt file - and would shadow the hint the query actually needed.
    #[test]
    fn a_query_echoing_the_phrase_is_not_reported_as_a_corrupt_file() {
        let raw = r#"Binder Error: Referenced column "Parquet error: x" not found in FROM clause!"#;
        let hint = enrich(
            raw,
            r#"SELECT "Parquet error: x" FROM usdc__transfer"#,
            &schema(),
        )
        .unwrap();
        assert!(
            !hint.contains("corrupt file on disk"),
            "a binder error must not be read as segment corruption: {hint}"
        );
        assert!(
            hint.contains("no column"),
            "and the precise classifier must still win: {hint}"
        );
    }

    /// The quote rule itself is load-bearing, not just the ordering.
    ///
    /// `a_query_echoing_the_phrase_is_not_reported_as_a_corrupt_file` uses a `Binder Error`, which an
    /// earlier classifier claims - so it passes whether or not this branch looks at quotes. This one
    /// carries the phrase in a message no earlier classifier matches (Burrmill's own text for the
    /// cast), so it reaches the corrupt-file branch and only the quote before the phrase turns it away.
    #[test]
    fn the_corrupt_file_classifier_requires_the_engine_prefix_not_just_the_phrase() {
        let raw = "substrate error: Execution error: Conversion Error: Could not convert string \
                   'Parquet error: x' to INT32";
        let hint = enrich(raw, "SELECT CAST(x AS INT) FROM usdc__transfer", &schema());
        assert!(
            !hint
                .as_deref()
                .unwrap_or("")
                .contains("corrupt file on disk"),
            "only the engine-prefixed form means segment corruption; caller text must never reach \
             this branch: {hint:?}"
        );
    }

    /// A segment that vanished twice under one query (#1162) surfaces as a Parquet error too, but
    /// the file is missing, not corrupt, and telling the operator to hunt a bad file is wrong.
    #[test]
    fn a_missing_segment_is_not_called_corrupt() {
        let raw = "query failed: substrate error: Parquet error: Parquet error: Failed to fetch \
                   metadata for file data/x/segments/a.parquet: Object Store error: Object at \
                   location /data/x/segments/a.parquet not found: No such file or directory (os \
                   error 2)";
        let hint = enrich(raw, "SELECT 1 FROM usdc__transfer", &schema());
        assert!(
            !hint.as_deref().unwrap_or("").contains("corrupt file"),
            "{hint:?}"
        );
    }

    /// Burrmill resolves unquoted identifiers case-insensitively, so the table must still be named.
    #[test]
    fn the_corrupt_segment_hint_names_the_table_whatever_its_casing() {
        let raw = CORRUPT;
        let hint = enrich(raw, "SELECT count(*) FROM USDC__Transfer", &schema()).unwrap();
        assert!(
            hint.contains("`usdc__transfer`"),
            "must fold case before matching table names: {hint}"
        );
    }

    /// A query naming no known table still gets the class, without inventing a table name.
    #[test]
    fn a_page_corrupt_segment_hint_survives_an_unrecognised_query() {
        let raw = CORRUPT;
        let hint = enrich(raw, "SELECT 1", &schema()).unwrap();
        assert!(
            hint.contains("a sealed segment behind this query"),
            "{hint}"
        );
        assert!(!hint.contains("usdc__transfer"), "invents no table: {hint}");
    }

    #[test]
    fn unknown_table_suggests_the_closest_real_table() {
        let raw = "Catalog Error: Table with name transfers does not exist!";
        let hint = enrich(raw, "SELECT count(*) FROM transfers", &schema()).unwrap();
        assert!(hint.contains("no table `transfers`"));
        assert!(hint.contains("usdc__transfer"), "suggests the real table");
    }

    /// #663: a table the nest genuinely declares (present in `schema` - the live registry, not
    /// `schema.json`) hitting the catalog error is a different fault from a typo, and must read
    /// differently. Before this, `closest("usdc__transfer", [..., "usdc__transfer"])` matched itself
    /// and said "no table `usdc__transfer`; the closest is `usdc__transfer`" - true, and useless.
    #[test]
    fn a_declared_table_with_no_data_yet_is_told_apart_from_an_unknown_one() {
        let raw = "Catalog Error: Table with name usdc__transfer does not exist!";
        let hint = enrich(raw, "SELECT count(*) FROM usdc__transfer", &schema()).unwrap();
        assert!(
            hint.contains("declared"),
            "must say this table IS declared, not just closest-matched to itself: {hint}"
        );
        assert!(
            hint.contains("Transfer"),
            "names the event that would create it: {hint}"
        );
        assert!(
            hint.contains("never fired") || hint.contains("has no data"),
            "must explain WHY it's missing, per #663's acceptance bar: {hint}"
        );
        assert!(
            !hint.contains("the closest is"),
            "an exact declared match is not a fuzzy suggestion: {hint}"
        );
    }

    #[test]
    fn unknown_column_suggests_the_closest_real_column() {
        // As Burrmill words it: DataFusion's own text, with the fields in scope, follows.
        let raw = "Binder Error: Referenced column \"valu\" not found in FROM clause!\n\
                   Schema error: No field named valu. Did you mean 'usdc__transfer.value'?\n\
                   Valid fields are usdc__transfer.from, usdc__transfer.to, usdc__transfer.value.";
        let hint = enrich(raw, "SELECT valu FROM usdc__transfer", &schema()).unwrap();
        assert!(hint.contains("no column `valu`"));
        assert!(hint.contains("value"), "suggests value");
    }

    /// The hint suggested `amount`, from another table, for a query on one without it.
    #[test]
    fn unknown_column_suggests_only_a_column_in_scope() {
        let raw = "Binder Error: Referenced column \"tox\" not found in FROM clause!\n\
                   Valid fields are x.value, x.address.";
        let hint = enrich(raw, "SELECT tox FROM x", &schema()).unwrap();
        assert!(
            !hint.contains("`to`"),
            "suggested a column out of scope: {hint}"
        );
        let raw = "Binder Error: Referenced column \"valeu\" not found in FROM clause!\n\
                   Valid fields are x.value, x.address.";
        let hint = enrich(raw, "SELECT valeu FROM x", &schema()).unwrap();
        assert!(hint.contains("the closest is `value`"), "{hint}");
        // With no scope to go on, no column is suggested at all, rather than one from any table.
        let raw = "Binder Error: Referenced column \"tox\" not found in FROM clause!";
        let hint = enrich(raw, "SELECT tox FROM x", &schema()).unwrap();
        assert!(!hint.contains("closest"), "{hint}");
        // Nor is a `_dec` column's base named as present when this query cannot see it.
        let raw = "Binder Error: Referenced column \"value_dec\" not found in FROM clause!\n\
                   Valid fields are x.owner, x.spender.";
        let hint = enrich(raw, "SELECT value_dec FROM x", &schema()).unwrap();
        assert!(!hint.contains("exists as"), "{hint}");
    }

    #[test]
    fn reserved_word_column_is_told_to_double_quote() {
        let raw = r#"Parser Error: syntax error at or near "FROM""#;
        let hint = enrich(raw, "SELECT from FROM usdc__transfer", &schema()).unwrap();
        assert!(hint.contains("reserved word"));
        assert!(hint.contains("\"from\""), "shows the quoted form");
    }

    #[test]
    fn bigint_aggregate_is_pointed_at_the_dec_companion() {
        let raw =
            "Binder Error: No function matches the given name and argument types 'sum(VARCHAR)'.";
        let hint = enrich(raw, "SELECT sum(value) FROM usdc__transfer", &schema()).unwrap();
        assert!(hint.contains("value_dec"), "points at value_dec");
        assert!(
            hint.contains("ids, nonces"),
            "uint256 value overflows decimal: {hint}"
        );
    }

    /// #539: the issue's own repro, `COALESCE(enabled, false)`. Burrmill restates it in this wording,
    /// and `through_burrmill` runs the query to prove it.
    #[test]
    fn bool_column_in_coalesce_is_explained_not_left_as_a_raw_type_error() {
        let raw = "Binder Error: Cannot mix values of type VARCHAR and BOOLEAN in COALESCE \
                    operator - an explicit cast is required";
        let hint = enrich(
            raw,
            "SELECT pool, COALESCE(enabled, false) AS override_enabled FROM usdc__transfer",
            &schema(),
        )
        .unwrap();
        assert!(hint.contains("`enabled`"), "names the bool column: {hint}");
        assert!(
            hint.contains("'true'") || hint.contains("CAST"),
            "gives the working alternative: {hint}"
        );
    }

    /// The message names the two types in the opposite order for `CASE` - the classifier must not be
    /// order-sensitive.
    #[test]
    fn bool_column_in_case_is_explained_regardless_of_type_order_in_the_message() {
        let raw = "Binder Error: Cannot mix values of type BOOLEAN and VARCHAR in CASE expression \
                    - an explicit cast is required";
        let hint = enrich(
            raw,
            "SELECT CASE WHEN true THEN enabled ELSE false END FROM usdc__transfer",
            &schema(),
        )
        .unwrap();
        assert!(hint.contains("`enabled`"), "{hint}");
    }

    /// The other real shape: an aggregate that only accepts BOOLEAN (`bool_and`/`bool_or`).
    #[test]
    fn bool_column_in_a_bool_aggregate_is_explained() {
        let raw = "Binder Error: No function matches the given name and argument types \
                    'bool_and(VARCHAR)'. You might need to add explicit type casts.";
        let hint = enrich(
            raw,
            "SELECT bool_and(enabled) FROM usdc__transfer",
            &schema(),
        )
        .unwrap();
        assert!(hint.contains("`enabled`"), "{hint}");
    }

    /// A COALESCE type-mismatch between two columns that are *not* the bool footgun must not be
    /// misattributed to `enabled` just because it mentions VARCHAR/BOOLEAN.
    #[test]
    fn a_mixed_type_error_naming_no_bool_column_gets_no_bool_hint() {
        let raw = "Binder Error: Cannot mix values of type VARCHAR and BOOLEAN in COALESCE \
                    operator - an explicit cast is required";
        let hint = enrich(
            raw,
            "SELECT COALESCE(\"from\", false) FROM usdc__transfer",
            &schema(),
        );
        assert!(
            hint.is_none_or(|h| !h.contains("enabled")),
            "must not name a column the query never mentioned"
        );
    }

    #[test]
    fn a_quoted_reserved_word_query_is_not_flagged() {
        // If the agent already quoted "from", a *different* syntax error must not be misread as the
        // reserved-word case.
        let raw = r#"Parser Error: syntax error at or near ")""#;
        let hint = enrich(
            raw,
            r#"SELECT "from" FROM usdc__transfer WHERE )"#,
            &schema(),
        );
        assert!(hint.is_none(), "quoted from is not the culprit");
    }

    #[test]
    fn a_dec_column_with_no_known_base_points_at_schema_regen() {
        // Hand-added factory template: `amount0_dec` is queried but the schema never learned about the
        // `amount0` base column (no `nuthatch schema` after editing the toml). Hint at the regen, not a
        // fuzzy typo match.
        let raw = r#"Binder Error: Referenced column "amount0_dec" not found in FROM clause!"#;
        let hint = enrich(raw, "SELECT sum(amount0_dec) FROM pool__swap", &schema()).unwrap();
        assert!(
            hint.contains("nuthatch schema"),
            "points at the regen command: {hint}"
        );
        assert!(hint.contains("amount0"), "names the missing base column");
    }

    #[test]
    fn an_unrecognised_error_gets_no_hint() {
        assert!(enrich("Some internal error", "SELECT 1", &schema()).is_none());
    }

    #[test]
    fn an_oom_names_analytics_memory_limit() {
        let hint = enrich(
            "Out of Memory Error: failed to allocate data of size 1.0 MiB (512.0 MiB/512.0 MiB used)",
            "SELECT 1",
            &schema(),
        )
        .unwrap();
        // Both keys, and which one wins: the Burrmill limit replaces the general one when set (#1683).
        for key in ["NUTHATCH_BURRMILL_MEMORY_LIMIT", "analytics.memory_limit"] {
            assert!(hint.contains(key), "must name {key}: {hint}");
        }
        assert!(hint.contains("when it is set, otherwise"), "{hint}");
        assert!(
            hint.contains("block processing is not degraded"),
            "ingestion liveness is the point: {hint}"
        );
    }

    #[test]
    fn a_temp_size_oom_names_max_temp_size() {
        let hint = enrich(
            "Out of Memory Error: failed to offload data block of size 256.0 KiB (0 bytes/0 bytes used).\nThis limit was set by the 'max_temp_directory_size' setting.",
            "SELECT 1",
            &schema(),
        )
        .unwrap();
        assert!(
            hint.contains("analytics.max_temp_size"),
            "must name the disk key: {hint}"
        );
        assert!(
            !hint.contains("analytics.memory_limit") || hint.contains("max_temp_size"),
            "{hint}"
        );
    }

    #[test]
    fn a_binder_echo_of_oom_is_not_a_budget_failure() {
        let raw =
            r#"Binder Error: Referenced column "Out of Memory Error" not found in FROM clause!"#;
        let hint = enrich(raw, r#"SELECT "Out of Memory Error""#, &schema()).unwrap();
        assert!(
            hint.contains("no column"),
            "must stay a missing-column hint: {hint}"
        );
        assert!(!hint.contains("analytics.memory_limit"), "{hint}");
    }

    #[test]
    fn suggestions_only_ever_come_from_the_schema() {
        // A wild table name gets no suggestion rather than a hallucinated one.
        let raw = "Catalog Error: Table with name zzzzzzzzzz does not exist!";
        let hint = enrich(raw, "SELECT * FROM zzzzzzzzzz", &schema()).unwrap();
        assert!(hint.contains("no table"));
        assert!(!hint.contains("usdc__transfer"), "too far to suggest");
    }
}

/// Each class, from the error Burrmill actually raises for the fault rather than a remembered string.
#[cfg(test)]
mod through_burrmill {
    use super::enrich;
    use crate::engine::{Died, Engine, FactWindow, Session};

    fn session() -> Box<dyn Session> {
        let s = crate::engine_burrmill::BurrmillEngine.open_bare().unwrap();
        let rows = [
            serde_json::json!({"block_number": 1, "from": "0xa", "to": "0xb", "value": "5", "enabled": "true"}),
            serde_json::json!({"block_number": 2, "from": "0xc", "to": "0xd", "value": "7", "enabled": "false"}),
        ];
        let refs: Vec<&serde_json::Value> = rows.iter().collect();
        s.load_hot("usdc__transfer", &refs).unwrap();
        assert!(s
            .bind_facts("usdc__transfer", &[], &[], true, FactWindow::default())
            .unwrap());
        s
    }

    fn fails(s: &dyn Session, sql: &str) -> String {
        match s.collect(sql, Some((10, crate::engine::SQL_MAX_RESULT_BYTES))) {
            Ok(_) => panic!("`{sql}` answered"),
            Err(Died::Binding(e) | Died::Executing(e)) => format!("{e:#}"),
        }
    }

    fn hint(sql: &str) -> String {
        let s = session();
        let raw = fails(s.as_ref(), sql);
        enrich(&raw, sql, &super::tests::schema()).unwrap_or_else(|| panic!("no hint for: {raw}"))
    }

    #[test]
    fn an_unknown_table() {
        let h = hint("SELECT count(*) FROM transfers");
        assert!(h.contains("the closest is `usdc__transfer`"), "{h}");
    }

    #[test]
    fn an_unknown_column_is_matched_against_the_fields_in_scope() {
        let h = hint("SELECT valu FROM usdc__transfer");
        assert!(h.contains("the closest is `value`"), "{h}");
    }

    #[test]
    fn an_unknown_column_with_no_near_match_or_a_quoted_name() {
        let h = hint("SELECT zzzz FROM usdc__transfer");
        assert!(h.starts_with("no column `zzzz`."), "{h}");
        let h = hint("SELECT \"Total Value\" FROM usdc__transfer");
        assert!(h.starts_with("no column `Total Value`."), "{h}");
    }

    #[test]
    fn a_dec_column_whose_base_is_in_scope() {
        let h = hint("SELECT \"from\" FROM usdc__transfer WHERE value_dec > 1");
        assert!(h.contains("derived on the fly"), "{h}");
    }

    #[test]
    fn a_bare_reserved_word() {
        let h = hint("SELECT from FROM usdc__transfer");
        assert!(h.contains("reserved word"), "{h}");
    }

    #[test]
    fn a_big_integer_summed_as_text() {
        let h = hint("SELECT sum(value) FROM usdc__transfer");
        assert!(h.contains("value_dec"), "{h}");
    }

    #[test]
    fn a_text_bool_in_coalesce_case_and_bool_and() {
        for sql in [
            "SELECT COALESCE(enabled, false) FROM usdc__transfer",
            "SELECT CASE WHEN true THEN enabled ELSE false END FROM usdc__transfer",
            "SELECT bool_and(enabled) FROM usdc__transfer",
            "SELECT bool_or(enabled) FROM usdc__transfer",
        ] {
            let h = hint(sql);
            assert!(h.contains("Solidity bool"), "{sql}: {h}");
        }
    }

    /// #433 through the engine: pages destroyed, footer intact, so the segment binds and the read fails.
    #[test]
    fn a_segment_corrupt_past_its_footer() {
        let s = session();
        let dir = tempfile::tempdir().unwrap();
        let seg = dir.path().join("seg.parquet");
        s.write_parquet("usdc__transfer", &seg).unwrap();
        let mut bytes = std::fs::read(&seg).unwrap();
        let len = bytes.len();
        let footer = u32::from_le_bytes(bytes[len - 8..len - 4].try_into().unwrap()) as usize;
        bytes[4..len - 8 - footer].fill(0xFF);
        std::fs::write(&seg, &bytes).unwrap();

        let c = crate::engine_burrmill::BurrmillEngine.open_bare().unwrap();
        c.load_hot("usdc__transfer", &[]).unwrap();
        assert!(c
            .bind_facts("usdc__transfer", &[], &[seg], false, FactWindow::default())
            .unwrap());
        let sql = "SELECT max(value) FROM usdc__transfer";
        let raw = fails(c.as_ref(), sql);
        let h = enrich(&raw, sql, &super::tests::schema()).unwrap_or_default();
        assert!(h.contains("corrupt file on disk"), "{raw}");
        assert!(h.contains("`usdc__transfer`"), "{h}");
    }

    /// The phrase quoted back from the caller's own text is not a corrupt file.
    #[test]
    fn the_phrase_in_caller_text_is_not_a_corrupt_file() {
        let s = session();
        for sql in [
            "SELECT CAST('Parquet error: x' AS INTEGER) FROM usdc__transfer",
            "SELECT \"Parquet error: x\" FROM usdc__transfer",
        ] {
            let raw = fails(s.as_ref(), sql);
            let h = enrich(&raw, sql, &super::tests::schema()).unwrap_or_default();
            assert!(!h.contains("corrupt file"), "{sql}: {h}");
        }
    }
}
