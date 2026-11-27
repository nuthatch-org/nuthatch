//! Bounds on SQL text, checked before anything parses it.
//!
//! sqlparser is a recursive-descent parser, and two shapes of input cost it far more than their
//! length suggests:
//!
//! - **Nesting it does not count.** The parser's recursion limit covers expressions and queries but
//!   not types, so `STRUCT(a STRUCT(b ...))`, `ARRAY<ARRAY<...>>` and `INT[][]...` recurse once per
//!   level with nothing to stop them. A few thousand levels overflow a 2 MiB statement thread, and a
//!   stack overflow aborts the process rather than failing the request.
//! - **Retries that multiply.** When `CAST(`, `EXTRACT(` and the other keyword forms fail to parse,
//!   the parser rolls back and tries the same tokens again as a plain function call, which parses
//!   the inner text a second time. Nested N deep, a failure at the bottom costs 2^N. A well-formed
//!   47-deep `CAST(... AS BIGINT)` parses in under a millisecond; at 48 the recursion limit is the
//!   failure at the bottom and the parse does not return. The same blow-up needs only about twenty
//!   levels when the text is simply malformed (`CAST(` twenty times and no closing).
//!
//! [`check`] answers both before any other parse sees the text, and costs one tokenizer pass plus
//! at most one bounded parse:
//!
//! 1. A token scan refuses brackets nested deeper than [`MAX_NESTING`] and a run of more than
//!    [`MAX_SUBSCRIPTS`] `[]` suffixes. The tokenizer is iterative, so this cannot recurse.
//! 2. A parse through [`Bounded`], sqlparser's DuckDB dialect with a counter on every expression
//!    prefix. Ordinary SQL makes well under one prefix call per token (0.3 at most across the views
//!    in this tree); the multiplied retries make millions. Past [`budget`] calls, or past the
//!    query's deadline, every further prefix fails at once, so each pending retry fails in constant
//!    time and the parse returns. A statement that parses within the budget will parse within it
//!    again wherever else it is parsed, because every later parse takes the same path.
//! 3. A statement that fails this parse for an ordinary reason (a syntax error, or syntax that only
//!    Burrmill's own rewrites accept) has not shown what the later parsers will spend on it, so it
//!    is held to [`MAX_KEYWORD_CALLS`] nested keyword calls, which keeps the worst retry cost to a
//!    few milliseconds.
//!
//! None of this sits in the data path: it decides only whether a statement is parsed at all.

use std::cell::Cell;
use std::time::Instant;

use sqlparser::ast::Expr;
use sqlparser::dialect::{Dialect, DuckDbDialect};
use sqlparser::keywords::Keyword;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Token, TokenWithSpan, Tokenizer};

/// Deepest nesting of `(`, `[` and `ARRAY<` / `STRUCT<` / `MAP<` accepted. Matches what the parser
/// already accepts for an expression: 47 nested parentheses or function calls parse and 48 hit
/// its recursion limit. Types had no limit at all.
pub const MAX_NESTING: usize = 47;

/// Longest run of `[]`, `[n]` or `[i]` suffixes accepted on one type or value.
pub const MAX_SUBSCRIPTS: usize = 32;

/// Nested keyword calls (`CAST(`, `EXTRACT(`, ...) accepted in a statement that did not parse here.
/// A failure at the bottom of twelve costs about 2^12 small parses: a few milliseconds.
pub const MAX_KEYWORD_CALLS: usize = 12;

/// Why a statement is not parsed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// Brackets nest deeper than [`MAX_NESTING`].
    TooNested,
    /// More than [`MAX_SUBSCRIPTS`] `[]` suffixes in a row.
    TooManySubscripts,
    /// More than [`MAX_KEYWORD_CALLS`] nested keyword calls in a statement that did not parse.
    TooManyKeywordCalls,
    /// The parse made more than [`budget`] prefix calls.
    TooComplex,
    /// The query's deadline passed while it was being parsed.
    Deadline,
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Refusal::TooNested => write!(
                f,
                "query nests parentheses, brackets or types more than {MAX_NESTING} deep \
                 and cannot be parsed"
            ),
            Refusal::TooManySubscripts => write!(
                f,
                "query chains more than {MAX_SUBSCRIPTS} `[]` suffixes on one type or value \
                 and cannot be parsed"
            ),
            Refusal::TooManyKeywordCalls => write!(
                f,
                "query does not parse, and nests more than {MAX_KEYWORD_CALLS} calls such as \
                 CAST( inside one another, which cannot be parsed within its budget"
            ),
            Refusal::TooComplex => {
                write!(
                    f,
                    "query is too complex and cannot be parsed within the parser's budget"
                )
            }
            Refusal::Deadline => write!(f, "query ran past its deadline while being parsed"),
        }
    }
}

impl std::error::Error for Refusal {}

/// Whether `sql` may be parsed: `Ok` for a statement every parser in the process can handle in
/// bounded time and stack, including one with a syntax error that a later parse will report.
pub fn check(sql: &str, deadline: Option<Instant>) -> Result<(), Refusal> {
    let Ok(tokens) = Tokenizer::new(&DuckDbDialect {}, sql).tokenize_with_location() else {
        // Nothing past the tokenizer gets further on text it cannot tokenize.
        return Ok(());
    };
    let scan = scan(&tokens)?;
    let bounded = Bounded {
        budget: budget(tokens.len()),
        deadline,
        ..Bounded::default()
    };
    let parsed = Parser::new(&bounded)
        .with_tokens_with_locations(tokens)
        .parse_statements();
    if let Some(trip) = bounded.tripped.get() {
        return Err(trip);
    }
    if parsed.is_err() && scan.keyword_calls > MAX_KEYWORD_CALLS {
        return Err(Refusal::TooManyKeywordCalls);
    }
    Ok(())
}

/// The prefix calls a statement of `tokens` tokens may make: more than ten times what the views in
/// this tree need, and a few milliseconds of parsing at the longest statement `/sql` accepts.
pub fn budget(tokens: usize) -> u64 {
    (tokens as u64).saturating_mul(2).saturating_add(4096)
}

#[derive(Debug, Default)]
struct Scan {
    /// The most keyword calls open at once.
    keyword_calls: usize,
}

fn scan(tokens: &[TokenWithSpan]) -> Result<Scan, Refusal> {
    let is_keyword = |t: Option<&Token>, words: Option<&[Keyword]>| match t {
        Some(Token::Word(w)) if w.quote_style.is_none() && w.keyword != Keyword::NoKeyword => {
            words.is_none_or(|words| words.contains(&w.keyword))
        }
        _ => false,
    };
    // One entry per open `(` or `[`: whether a keyword call opened it.
    let mut open: Vec<bool> = Vec::with_capacity(MAX_NESTING + 1);
    let (mut keyword_calls, mut max_keyword_calls) = (0usize, 0usize);
    // Open `ARRAY<`, `STRUCT<` and `MAP<`.
    let mut angles = 0usize;
    // `[]` / `[n]` / `[i]` groups in a row, and whether the open `[` is still one of those.
    let mut subscripts = 0usize;
    let mut simple_group = false;
    let mut previous: Option<&Token> = None;
    for token in tokens.iter().map(|t| &t.token) {
        if matches!(token, Token::Whitespace(_)) {
            continue;
        }
        match token {
            Token::LParen | Token::LBracket => {
                let call = *token == Token::LParen && is_keyword(previous, None);
                open.push(call);
                if call {
                    keyword_calls += 1;
                    max_keyword_calls = max_keyword_calls.max(keyword_calls);
                }
                if open.len() + angles > MAX_NESTING {
                    return Err(Refusal::TooNested);
                }
                simple_group = *token == Token::LBracket;
            }
            Token::RParen | Token::RBracket => {
                if open.pop() == Some(true) {
                    keyword_calls -= 1;
                }
                if *token == Token::RBracket && simple_group {
                    subscripts += 1;
                    if subscripts > MAX_SUBSCRIPTS {
                        return Err(Refusal::TooManySubscripts);
                    }
                } else {
                    subscripts = 0;
                }
                simple_group = false;
            }
            Token::Lt
                if is_keyword(
                    previous,
                    Some(&[Keyword::ARRAY, Keyword::STRUCT, Keyword::MAP]),
                ) =>
            {
                angles += 1;
                if open.len() + angles > MAX_NESTING {
                    return Err(Refusal::TooNested);
                }
            }
            Token::Gt => angles = angles.saturating_sub(1),
            Token::ShiftRight => angles = angles.saturating_sub(2),
            _ => {}
        }
        // A run is `[`, then at most one number or name, then `]`. Anything else ends it.
        let in_run = matches!(token, Token::LBracket | Token::RBracket)
            || (simple_group
                && previous == Some(&Token::LBracket)
                && matches!(token, Token::Number(..) | Token::Word(_)));
        if !in_run {
            simple_group = false;
            subscripts = 0;
        }
        previous = Some(token);
    }
    Ok(Scan {
        keyword_calls: max_keyword_calls,
    })
}

/// sqlparser's DuckDB dialect, unchanged except that every expression prefix is counted against a
/// budget and the query's deadline. Once either is spent, every further prefix fails at once.
///
/// It reports itself as [`DuckDbDialect`] (`dialect()`), so the parser's dialect checks answer as
/// they do for DuckDB, and it forwards every method `DuckDbDialect` overrides. A method added to
/// `DuckDbDialect` by a later sqlparser must be forwarded here too; `the_bounded_dialect_parses_as_duckdb`
/// compares the two over the views in this tree.
#[derive(Debug, Default)]
pub(crate) struct Bounded {
    inner: DuckDbDialect,
    budget: u64,
    deadline: Option<Instant>,
    calls: Cell<u64>,
    tripped: Cell<Option<Refusal>>,
}

impl Bounded {
    fn spent(&self) -> Option<Refusal> {
        let calls = self.calls.get() + 1;
        self.calls.set(calls);
        if calls > self.budget {
            return Some(Refusal::TooComplex);
        }
        // Reading the clock every call would be most of this hook's cost.
        if calls.is_multiple_of(256) && self.deadline.is_some_and(|d| Instant::now() >= d) {
            return Some(Refusal::Deadline);
        }
        None
    }
}

impl Dialect for Bounded {
    fn dialect(&self) -> std::any::TypeId {
        self.inner.dialect()
    }

    fn parse_prefix(&self, parser: &mut Parser) -> Option<Result<Expr, ParserError>> {
        if let Some(trip) = self.tripped.get().or_else(|| self.spent()) {
            self.tripped.set(Some(trip));
            // The one error every speculative parse passes up rather than retrying around.
            return Some(Err(ParserError::RecursionLimitExceeded));
        }
        self.inner.parse_prefix(parser)
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        self.inner.is_identifier_start(ch)
    }
    fn is_identifier_part(&self, ch: char) -> bool {
        self.inner.is_identifier_part(ch)
    }
    fn supports_trailing_commas(&self) -> bool {
        self.inner.supports_trailing_commas()
    }
    fn supports_filter_during_aggregation(&self) -> bool {
        self.inner.supports_filter_during_aggregation()
    }
    fn supports_group_by_expr(&self) -> bool {
        self.inner.supports_group_by_expr()
    }
    fn supports_bitwise_shift_operators(&self) -> bool {
        self.inner.supports_bitwise_shift_operators()
    }
    fn supports_named_fn_args_with_eq_operator(&self) -> bool {
        self.inner.supports_named_fn_args_with_eq_operator()
    }
    fn supports_named_fn_args_with_assignment_operator(&self) -> bool {
        self.inner.supports_named_fn_args_with_assignment_operator()
    }
    fn supports_dictionary_syntax(&self) -> bool {
        self.inner.supports_dictionary_syntax()
    }
    fn support_map_literal_syntax(&self) -> bool {
        self.inner.support_map_literal_syntax()
    }
    fn supports_lambda_functions(&self) -> bool {
        self.inner.supports_lambda_functions()
    }
    fn allow_extract_single_quotes(&self) -> bool {
        self.inner.allow_extract_single_quotes()
    }
    fn supports_explain_with_utility_options(&self) -> bool {
        self.inner.supports_explain_with_utility_options()
    }
    fn supports_load_extension(&self) -> bool {
        self.inner.supports_load_extension()
    }
    fn supports_array_typedef_with_brackets(&self) -> bool {
        self.inner.supports_array_typedef_with_brackets()
    }
    fn supports_from_first_select(&self) -> bool {
        self.inner.supports_from_first_select()
    }
    fn supports_order_by_all(&self) -> bool {
        self.inner.supports_order_by_all()
    }
    fn supports_select_wildcard_exclude(&self) -> bool {
        self.inner.supports_select_wildcard_exclude()
    }
    fn supports_notnull_operator(&self) -> bool {
        self.inner.supports_notnull_operator()
    }
    fn supports_install(&self) -> bool {
        self.inner.supports_install()
    }
    fn supports_detach(&self) -> bool {
        self.inner.supports_detach()
    }
    fn supports_select_wildcard_replace(&self) -> bool {
        self.inner.supports_select_wildcard_replace()
    }
    fn supports_comma_separated_trim(&self) -> bool {
        self.inner.supports_comma_separated_trim()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn casts(n: usize) -> String {
        format!("SELECT {}1{}", "CAST(".repeat(n), " AS BIGINT)".repeat(n))
    }

    /// Runs `f` on a thread with a statement thread's 2 MiB stack and fails, rather than hangs, if
    /// it has not returned in `within`.
    fn on_small_stack<T: Send + 'static>(
        within: Duration,
        f: impl FnOnce() -> T + Send + 'static,
    ) -> (T, Duration) {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn(move || {
                let started = Instant::now();
                let out = f();
                let _ = tx.send((out, started.elapsed()));
            })
            .unwrap();
        rx.recv_timeout(within)
            .expect("returned within the time allowed")
    }

    #[test]
    fn a_47_deep_cast_is_accepted() {
        let (out, _) = on_small_stack(Duration::from_secs(5), || check(&casts(47), None));
        assert_eq!(out, Ok(()));
    }

    #[test]
    fn a_48_deep_cast_is_refused_quickly() {
        for n in [48, 49, 64, 500] {
            let (out, took) =
                on_small_stack(Duration::from_secs(5), move || check(&casts(n), None));
            assert!(out.is_err(), "{n}: {out:?}");
            assert!(took < Duration::from_secs(1), "{n}: {took:?}");
        }
        assert_eq!(check(&casts(48), None), Err(Refusal::TooNested));
    }

    #[test]
    fn an_unclosed_nest_of_casts_is_refused_quickly() {
        // Malformed text costs the parser 2^N well inside the nesting bound: 22 levels took 5 s.
        for n in [16, 22, 30, 47] {
            let sql = format!("SELECT {}1", "CAST(".repeat(n));
            let (out, took) = on_small_stack(Duration::from_secs(5), move || check(&sql, None));
            assert!(out.is_err(), "{n}: {out:?}");
            assert!(took < Duration::from_secs(1), "{n}: {took:?}");
        }
        // And with the nesting under the bound and the failure being the recursion limit.
        let sql = format!(
            "SELECT {}{}1{}",
            "CAST(".repeat(30),
            "NOT ".repeat(20),
            " AS BIGINT)".repeat(30)
        );
        let (out, took) = on_small_stack(Duration::from_secs(5), move || check(&sql, None));
        assert!(out.is_err(), "{out:?}");
        assert!(took < Duration::from_secs(1), "{took:?}");
    }

    #[test]
    fn thousands_of_array_suffixes_are_refused() {
        let sql = format!("SELECT CAST(NULL AS INT{})", "[]".repeat(7000));
        let (out, took) = on_small_stack(Duration::from_secs(5), move || check(&sql, None));
        assert_eq!(out, Err(Refusal::TooManySubscripts));
        assert!(took < Duration::from_secs(1), "{took:?}");
        let sql = format!("SELECT CAST(NULL AS INT{})", "[]".repeat(MAX_SUBSCRIPTS));
        assert_eq!(check(&sql, None), Ok(()));
        assert_eq!(check("SELECT CAST(NULL AS INT[3][][4])", None), Ok(()));
        assert_eq!(check("SELECT x[1][i][2:3] FROM t", None), Ok(()));
    }

    #[test]
    fn deeply_nested_types_are_refused_before_they_reach_the_stack() {
        // Each of these overflowed a 2 MiB stack inside the parser, which aborts the process.
        for sql in [
            format!(
                "SELECT CAST(NULL AS {}INT{})",
                "STRUCT(a ".repeat(3000),
                ")".repeat(3000)
            ),
            format!(
                "SELECT CAST(NULL AS {}INT{})",
                "ARRAY<".repeat(3000),
                ">".repeat(3000)
            ),
            format!("SELECT {}1{}", "[".repeat(3000), "]".repeat(3000)),
        ] {
            let (out, _) = on_small_stack(Duration::from_secs(5), move || check(&sql, None));
            assert_eq!(out, Err(Refusal::TooNested));
        }
        assert_eq!(
            check(
                "SELECT CAST(NULL AS STRUCT(a STRUCT(b INT[]), c MAP(INT, INT)))",
                None
            ),
            Ok(())
        );
        assert_eq!(
            check("SELECT CAST(NULL AS ARRAY<ARRAY<INT>>)", None),
            Ok(())
        );
    }

    #[test]
    fn a_passed_deadline_stops_the_parse() {
        // Well-formed, inside every bound, and long enough to reach the clock check.
        let sql = format!("SELECT {}", vec!["1"; 2000].join(", "));
        let past = Instant::now() - Duration::from_secs(1);
        assert_eq!(check(&sql, Some(past)), Err(Refusal::Deadline));
        assert_eq!(
            check(&sql, Some(Instant::now() + Duration::from_secs(60))),
            Ok(())
        );
    }

    #[test]
    fn a_syntax_error_is_left_for_the_parser_to_report() {
        assert_eq!(check("SELECT FROM WHERE", None), Ok(()));
        assert_eq!(check("SELECT CAST(1 AS", None), Ok(()));
        // List comprehensions parse only after Burrmill rewrites them.
        assert_eq!(check("SELECT [x * 2 FOR x IN [1, 2, 3]]", None), Ok(()));
        assert_eq!(check("SELECT '((((' AS s -- [[[[", None), Ok(()));
    }

    /// The bounded dialect must parse exactly as DuckDB's does, or the bound would refuse (or admit)
    /// a different language from the one the engine runs.
    #[test]
    fn the_bounded_dialect_parses_as_duckdb() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let mut files = Vec::new();
        let mut dirs = vec![
            root.join("examples"),
            root.join("tests/fixtures"),
            root.join("templates"),
        ];
        while let Some(dir) = dirs.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    dirs.push(path);
                } else if path.extension().is_some_and(|e| e == "sql") {
                    files.push(path);
                }
            }
        }
        assert!(files.len() > 20, "found {} SQL files", files.len());
        for file in files {
            let sql = std::fs::read_to_string(&file).unwrap();
            let duck = Parser::parse_sql(&DuckDbDialect {}, &sql).map_err(|e| e.to_string());
            let bounded = Bounded {
                budget: u64::MAX,
                ..Bounded::default()
            };
            let ours = Parser::parse_sql(&bounded, &sql).map_err(|e| e.to_string());
            assert_eq!(ours, duck, "{}", file.display());
            assert_eq!(check(&sql, None), Ok(()), "{}", file.display());
            let tokens = Tokenizer::new(&DuckDbDialect {}, &sql)
                .tokenize()
                .unwrap()
                .len();
            assert!(
                bounded.calls.get() * 4 < budget(tokens),
                "{}: {} prefix calls for {tokens} tokens",
                file.display(),
                bounded.calls.get()
            );
        }
    }
}
