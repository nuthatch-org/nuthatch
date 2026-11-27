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
//! A third shape is a run of `INTERVAL`: each one parses its value as a prefix, outside the
//! recursion limit, so a thousand of them in a row overflowed the stack inside the bound itself.
//!
//! [`check`] answers all three before any other parse sees the text, and costs one tokenizer pass
//! plus at most three bounded parses:
//!
//! 0. Text with a token after a `;` is refused. The read-only surface accepts one statement and
//!    refuses the rest later anyway, but the parses before that refusal (Burrmill's included) would
//!    read every statement, while the bounded parses here can stop at the first one they do not
//!    read, such as `EXPLAIN FORMAT indent`, and so never measure what follows.
//! 1. A token scan refuses brackets nested deeper than [`MAX_NESTING`], a run of more than
//!    [`MAX_SUBSCRIPTS`] `[]` suffixes and a run of more than [`MAX_INTERVAL_RUN`] `INTERVAL`s. The
//!    tokenizer is iterative, so this cannot recurse.
//! 2. A parse through [`Bounded`], sqlparser's DuckDB dialect with a counter on every expression
//!    prefix. Ordinary SQL makes well under one prefix call per token (0.3 at most across the views
//!    in this tree); the multiplied retries make millions. Past [`budget`] calls, or past the
//!    query's deadline, every further prefix fails at once, so each pending retry fails in constant
//!    time and the parse returns. A statement that parses within the budget will parse within it
//!    again wherever else it is parsed in the same language, because every later parse takes the
//!    same path, and so does one that fails within it.
//!
//!    The text is read in each language a parser in this process reads it in: sqlparser's DuckDB
//!    dialect as written (nuthatch's own walks), Burrmill's dialect as written (its `inspect`), and
//!    Burrmill's dialect after the text rewrites its engine makes first ([`as_burrmill_reads`]).
//!    They differ: `last_value(x IGNORE NULLS)` fails early in the first and parses in the other
//!    two, so what follows it is measured only by those.
//! 3. A statement that fails the engine's parse for an ordinary reason (a syntax error, or a
//!    rewrite of Burrmill's that the tokens here do not mirror exactly) has not shown what every
//!    later parser will spend on it, so it is held to [`MAX_KEYWORD_CALLS`] retrying forms: keyword
//!    calls, `CASE`s and `INTERVAL`s open inside one another, and `(a) ->` lambdas, together. That
//!    keeps the worst retry cost to tens of milliseconds. A statement no parse here reads can still
//!    be one the engine reads (`EXPLAIN FORMAT indent SELECT ...` is DataFusion's own syntax), so
//!    this count is what bounds the engine's parse of it.
//!
//! None of this sits in the data path: it decides only whether a statement is parsed at all.

use std::cell::Cell;
use std::time::Instant;

use sqlparser::ast::{Expr, Statement};
use sqlparser::dialect::{Dialect, DuckDbDialect, Precedence};
use sqlparser::keywords::Keyword;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Token, TokenWithSpan, Tokenizer};

/// Deepest nesting of `(`, `[` and `ARRAY<` / `STRUCT<` / `MAP<` accepted. Matches what the parser
/// already accepts for an expression: 47 nested parentheses or function calls parse and 48 hit
/// its recursion limit. Types had no limit at all.
pub const MAX_NESTING: usize = 47;

/// Longest run of `[]`, `[n]` or `[i]` suffixes accepted on one type or value.
pub const MAX_SUBSCRIPTS: usize = 32;

/// Retrying forms accepted in a statement that did not parse here: the most keyword calls
/// (`CAST(`, `EXTRACT(`, ...), `CASE`s and `INTERVAL`s open at once, plus every `(a) ->` lambda.
/// The parser tries each of them a second way when the first fails: a keyword call or `CASE` as a
/// plain name, an `INTERVAL` the same, a lambda as a parenthesised expression. A failure at the
/// bottom of twelve costs about 2^12 small parses, which DataFusion's parser does in under 100 ms.
pub const MAX_KEYWORD_CALLS: usize = 12;

/// Most `INTERVAL`s accepted in a row, with nothing between them but unary operators. Each parses
/// the next as its value, outside the parser's recursion limit, so a thousand overflowed a 2 MiB
/// stack. An interval written to be read never holds a second one.
pub const MAX_INTERVAL_RUN: usize = 2;

/// Why a statement is not parsed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// A token follows a `;`: more than one statement.
    Stacked,
    /// Brackets nest deeper than [`MAX_NESTING`].
    TooNested,
    /// More than [`MAX_SUBSCRIPTS`] `[]` suffixes in a row.
    TooManySubscripts,
    /// More than [`MAX_KEYWORD_CALLS`] retrying forms in a statement that did not parse.
    TooManyKeywordCalls,
    /// More than [`MAX_INTERVAL_RUN`] `INTERVAL`s in a row.
    TooManyIntervals,
    /// The parse made more than [`budget`] prefix calls.
    TooComplex,
    /// The query's deadline passed while it was being parsed.
    Deadline,
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // As `analytics::reject_statement_stacking` words it, which refuses the same text later.
            Refusal::Stacked => write!(
                f,
                "the read-only SQL surface accepts a single statement; `;`-stacking is refused"
            ),
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
                "query does not parse, and holds more than {MAX_KEYWORD_CALLS} calls such as \
                 CAST(, CASE or INTERVAL nested inside one another or lambdas such as (a) ->, \
                 which cannot be parsed within its budget"
            ),
            Refusal::TooManyIntervals => write!(
                f,
                "query chains more than {MAX_INTERVAL_RUN} INTERVAL keywords into one another \
                 and cannot be parsed"
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
    // A trailing `;`, with only whitespace or comments after it, ends the one statement.
    let mut after_semicolon = tokens
        .iter()
        .skip_while(|t| t.token != Token::SemiColon)
        .skip(1);
    if after_semicolon.any(|t| !matches!(t.token, Token::Whitespace(_))) {
        return Err(Refusal::Stacked);
    }
    let scan = scan(&tokens)?;
    let rewritten = as_burrmill_reads(&tokens);
    // Whether the text parsed, or the refusal if the parse ran out of budget or time.
    let parse = |burrmill: bool, tokens: Vec<TokenWithSpan>| {
        let bounded = Bounded {
            budget: budget(tokens.len()),
            deadline,
            burrmill,
            ..Bounded::default()
        };
        let parsed = Parser::new(&bounded)
            .with_tokens_with_locations(tokens)
            .parse_statements();
        match bounded.tripped.get() {
            Some(trip) => Err(trip),
            None => Ok(parsed.is_ok()),
        }
    };
    // As written, in each dialect, then as Burrmill's engine reads it.
    parse(false, tokens.clone())?;
    let mut engine_parsed = parse(true, tokens)?;
    if let Some(rewritten) = rewritten {
        engine_parsed = parse(true, rewritten)?;
    }
    if !engine_parsed && scan.retrying + scan.lambdas > MAX_KEYWORD_CALLS {
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
    /// The most retrying forms open at once: keyword calls, `CASE`s and `INTERVAL`s.
    retrying: usize,
    /// `->` after a `)`: a lambda with its parameters in parentheses.
    lambdas: usize,
}

/// An open `(`, `[` or `CASE`, and how many retrying forms it holds open.
struct Open {
    /// A `(` or `[`, closed by its bracket; otherwise a `CASE`, closed by `END`.
    bracket: bool,
    retrying: usize,
}

fn scan(tokens: &[TokenWithSpan]) -> Result<Scan, Refusal> {
    let is_keyword = |t: Option<&Token>, words: Option<&[Keyword]>| match t {
        Some(Token::Word(w)) if w.quote_style.is_none() && w.keyword != Keyword::NoKeyword => {
            words.is_none_or(|words| words.contains(&w.keyword))
        }
        _ => false,
    };
    let mut open: Vec<Open> = Vec::with_capacity(MAX_NESTING + 1);
    let (mut brackets, mut retrying, mut max_retrying) = (0usize, 0usize, 0usize);
    // Open `ARRAY<`, `STRUCT<` and `MAP<`.
    let mut angles = 0usize;
    // `[]` / `[n]` / `[i]` groups in a row, and whether the open `[` is still one of those.
    let mut subscripts = 0usize;
    let mut simple_group = false;
    let mut previous: Option<&Token> = None;
    let (mut intervals, mut lambdas) = (0usize, 0usize);
    // `INTERVAL`s whose value has not yet ended or opened a bracket or `CASE`. When it opens one,
    // that one holds them open as well as itself: in `INTERVAL CAST(` nested, both are retried at
    // every level.
    let mut pending_intervals = 0usize;
    for token in tokens.iter().map(|t| &t.token) {
        if matches!(token, Token::Whitespace(_)) {
            continue;
        }
        // A run of `INTERVAL` goes on through unary operators, which recurse within the limit, and
        // ends at anything else.
        if is_keyword(Some(token), Some(&[Keyword::INTERVAL])) {
            intervals += 1;
            if intervals > MAX_INTERVAL_RUN {
                return Err(Refusal::TooManyIntervals);
            }
        } else if !is_unary(token) {
            intervals = 0;
        }
        if *token == Token::Arrow && previous == Some(&Token::RParen) {
            lambdas += 1;
        }
        let case = is_keyword(Some(token), Some(&[Keyword::CASE]));
        // A `(` or `[`, and whether a keyword opened it; or a `CASE`.
        let opens = match token {
            // `INTERVAL (` is one form, counted as the `INTERVAL` below.
            Token::LParen => Some((
                true,
                is_keyword(previous, None) && !is_keyword(previous, Some(&[Keyword::INTERVAL])),
            )),
            Token::LBracket => Some((true, false)),
            _ if case => Some((false, true)),
            _ => None,
        };
        if let Some((bracket, retries)) = opens {
            let held = usize::from(retries) + pending_intervals;
            open.push(Open {
                bracket,
                retrying: held,
            });
            retrying += held;
            max_retrying = max_retrying.max(retrying);
        }
        match token {
            Token::LParen | Token::LBracket => {
                brackets += 1;
                if brackets + angles > MAX_NESTING {
                    return Err(Refusal::TooNested);
                }
                simple_group = *token == Token::LBracket;
            }
            Token::RParen | Token::RBracket => {
                // Closes any `CASE` left open inside it, then itself.
                while let Some(o) = open.pop() {
                    retrying -= o.retrying;
                    if o.bracket {
                        brackets -= 1;
                        break;
                    }
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
            _ if is_keyword(Some(token), Some(&[Keyword::END]))
                && open.last().is_some_and(|o| !o.bracket) =>
            {
                retrying -= open.pop().map_or(0, |o| o.retrying);
            }
            Token::Lt
                if is_keyword(
                    previous,
                    Some(&[Keyword::ARRAY, Keyword::STRUCT, Keyword::MAP]),
                ) =>
            {
                angles += 1;
                if brackets + angles > MAX_NESTING {
                    return Err(Refusal::TooNested);
                }
            }
            Token::Gt => angles = angles.saturating_sub(1),
            Token::ShiftRight => angles = angles.saturating_sub(2),
            _ => {}
        }
        // An `INTERVAL`'s value goes on through unary operators and keywords (`CAST`, `ARRAY`,
        // `NOT`, ...) until it opens something or ends.
        pending_intervals = if is_keyword(Some(token), Some(&[Keyword::INTERVAL])) {
            pending_intervals + 1
        } else if opens.is_some() {
            0
        } else if is_unary(token) || is_keyword(Some(token), None) {
            pending_intervals
        } else {
            0
        };
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
        retrying: max_retrying,
        lambdas,
    })
}

/// A token that can only begin a prefix operator's operand here: `-`, `+`, `~`, `@`, `!`, `|/`,
/// `||/` and `NOT`.
fn is_unary(token: &Token) -> bool {
    match token {
        Token::Minus
        | Token::Plus
        | Token::Tilde
        | Token::AtSign
        | Token::ExclamationMark
        | Token::DoubleExclamationMark
        | Token::PGSquareRoot
        | Token::PGCubeRoot => true,
        Token::Word(w) => w.quote_style.is_none() && w.keyword == Keyword::NOT,
        _ => false,
    }
}

/// sqlparser's DuckDB dialect, unchanged except that every expression prefix is counted against a
/// budget and the query's deadline. Once either is spent, every further prefix fails at once. With
/// `burrmill`, it is Burrmill's dialect instead (`burrmill::df::dialect::Duck`, private to that
/// crate): DuckDB's with `IGNORE NULLS` inside a call, `^@`, `GLOB`, `IS [NOT] DISTINCT FROM` bound
/// as DuckDB binds it, and `PIVOT` refused by name.
///
/// It reports itself as [`DuckDbDialect`] (`dialect()`), so the parser's dialect checks answer as
/// they do for DuckDB, and it forwards every method `DuckDbDialect` overrides. A method added to
/// `DuckDbDialect` by a later sqlparser, or to Burrmill's dialect by a later pin, must be followed
/// here too; `the_bounded_dialect_parses_as_duckdb` compares them over the views in this tree.
#[derive(Debug, Default)]
pub(crate) struct Bounded {
    inner: DuckDbDialect,
    burrmill: bool,
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

    // Burrmill's own, as its dialect has them.
    fn supports_window_function_null_treatment_arg(&self) -> bool {
        self.burrmill || self.inner.supports_window_function_null_treatment_arg()
    }

    fn parse_statement(&self, parser: &mut Parser) -> Option<Result<Statement, ParserError>> {
        if self.burrmill {
            if let Token::Word(w) = &parser.peek_token_ref().token {
                if matches!(w.keyword, Keyword::PIVOT | Keyword::UNPIVOT) {
                    return Some(Err(ParserError::ParserError(format!(
                        "{} is not supported here",
                        w.value.to_uppercase()
                    ))));
                }
            }
        }
        self.inner.parse_statement(parser)
    }

    fn get_next_precedence(&self, parser: &Parser) -> Option<Result<u8, ParserError>> {
        if self.burrmill && burrmill_like(&parser.peek_token_ref().token) {
            return Some(Ok(self.prec_value(Precedence::Like)));
        }
        self.inner.get_next_precedence(parser)
    }

    fn parse_infix(
        &self,
        parser: &mut Parser,
        expr: &Expr,
        precedence: u8,
    ) -> Option<Result<Expr, ParserError>> {
        if !self.burrmill {
            return self.inner.parse_infix(parser, expr, precedence);
        }
        match &parser.peek_token_ref().token {
            Token::CaretAt => {
                parser.next_token();
                let right = parser.parse_subexpr(self.prec_value(Precedence::Like));
                return Some(right.map(|right| starts_with(expr.clone(), right)));
            }
            token if burrmill_like(token) => {
                return Some(Err(ParserError::ParserError(
                    "GLOB is not supported here".into(),
                )));
            }
            _ => {}
        }
        let not = if parser.parse_keywords(&[Keyword::IS, Keyword::DISTINCT, Keyword::FROM]) {
            false
        } else if parser.parse_keywords(&[
            Keyword::IS,
            Keyword::NOT,
            Keyword::DISTINCT,
            Keyword::FROM,
        ]) {
            true
        } else {
            return None;
        };
        let right = match parser.parse_subexpr(self.prec_value(Precedence::Is)) {
            Ok(right) => Box::new(right),
            Err(e) => return Some(Err(e)),
        };
        let left = Box::new(expr.clone());
        Some(Ok(if not {
            Expr::IsNotDistinctFrom(left, right)
        } else {
            Expr::IsDistinctFrom(left, right)
        }))
    }
}

/// `^@` and `GLOB`, which Burrmill's dialect binds as tightly as `LIKE`.
fn burrmill_like(token: &Token) -> bool {
    match token {
        Token::CaretAt => true,
        Token::Word(w) => w.quote_style.is_none() && w.value.eq_ignore_ascii_case("glob"),
        _ => false,
    }
}

/// `a ^@ b` as Burrmill's dialect reads it.
fn starts_with(left: Expr, right: Expr) -> Expr {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName,
    };
    Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new("starts_with")]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: [left, right]
                .into_iter()
                .map(|a| FunctionArg::Unnamed(FunctionArgExpr::Expr(a)))
                .collect(),
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

/// The text rewrites Burrmill makes before its parser sees a statement, made here on tokens, so the
/// bounded parse reads what its engine reads: `[e FOR x IN l]` as `list_transform(l, lambda x: e)`
/// (with `IF p` as a `list_filter(l, lambda x: p)` in front), `AS [NOT] MATERIALIZED (` as `AS (`,
/// and an `ASOF` before a join left out. `None` when none of them applies.
fn as_burrmill_reads(tokens: &[TokenWithSpan]) -> Option<Vec<TokenWithSpan>> {
    let word = |t: &TokenWithSpan, k: Keyword| matches!(&t.token, Token::Word(w) if w.quote_style.is_none() && w.keyword == k);
    let significant: Vec<usize> = (0..tokens.len())
        .filter(|&i| !matches!(tokens[i].token, Token::Whitespace(_)))
        .collect();
    let at = |i: usize, k: Keyword| significant.get(i).is_some_and(|&j| word(&tokens[j], k));
    let snowflake_asof = tokens.iter().any(|t| word(t, Keyword::MATCH_CONDITION));
    let mut dropped = vec![false; tokens.len()];
    for i in 0..significant.len() {
        if !snowflake_asof
            && at(i, Keyword::ASOF)
            && (at(i + 1, Keyword::JOIN) || at(i + 1, Keyword::LEFT))
        {
            dropped[significant[i]] = true;
        }
        if at(i, Keyword::AS) {
            let m = if at(i + 1, Keyword::NOT) {
                i + 2
            } else {
                i + 1
            };
            let paren = significant
                .get(m + 1)
                .is_some_and(|&j| tokens[j].token == Token::LParen);
            if at(m, Keyword::MATERIALIZED) && paren {
                (i + 1..=m).for_each(|k| dropped[significant[k]] = true);
            }
        }
    }
    let tokens: Vec<TokenWithSpan> = tokens
        .iter()
        .zip(&dropped)
        .filter(|(_, d)| !**d)
        .map(|(t, _)| t.clone())
        .collect();
    // Each `[` with the index of its `]`, and each `(` with its `)`.
    let mut close = vec![usize::MAX; tokens.len()];
    let (mut brackets, mut parens) = (Vec::new(), Vec::new());
    for (i, t) in tokens.iter().enumerate() {
        let (stack, closes) = match t.token {
            Token::LBracket => (&mut brackets, false),
            Token::LParen => (&mut parens, false),
            Token::RBracket => (&mut brackets, true),
            Token::RParen => (&mut parens, true),
            _ => continue,
        };
        if !closes {
            stack.push(i);
        } else if let Some(open) = stack.pop() {
            close[open] = i;
        }
    }
    let mut out = Vec::with_capacity(tokens.len() + 16);
    let comprehensions = Comprehensions {
        tokens: &tokens,
        close: &close,
    }
    .emit(0, tokens.len(), &mut out);
    (comprehensions || dropped.contains(&true)).then_some(out)
}

struct Comprehensions<'a> {
    tokens: &'a [TokenWithSpan],
    close: &'a [usize],
}

impl Comprehensions<'_> {
    /// `tokens[from..to]` into `out`, each comprehension rewritten; whether there was one. Recurses
    /// once per nested `[`, which the scan has held to [`MAX_NESTING`].
    fn emit(&self, from: usize, to: usize, out: &mut Vec<TokenWithSpan>) -> bool {
        let mut found = false;
        let mut i = from;
        while i < to {
            let end = self.close[i];
            if self.tokens[i].token != Token::LBracket || end >= to {
                out.push(self.tokens[i].clone());
                i += 1;
                continue;
            }
            let push = |out: &mut Vec<TokenWithSpan>, words: &[Token]| {
                for t in words {
                    out.push(TokenWithSpan::wrap(t.clone()));
                }
            };
            let lambda = |var: &Token| {
                [
                    Token::Comma,
                    Token::make_keyword("lambda"),
                    Token::Whitespace(sqlparser::tokenizer::Whitespace::Space),
                    var.clone(),
                    Token::Colon,
                ]
            };
            if let Some((for_at, var, list, pred)) = self.comprehension(i, end) {
                found = true;
                push(
                    out,
                    &[Token::make_word("list_transform", None), Token::LParen],
                );
                if let Some(pred) = pred {
                    push(out, &[Token::make_word("list_filter", None), Token::LParen]);
                    self.emit(list, pred, out);
                    push(out, &lambda(&var));
                    self.emit(pred + 1, end, out);
                    push(out, &[Token::RParen]);
                } else {
                    self.emit(list, end, out);
                }
                push(out, &lambda(&var));
                self.emit(i + 1, for_at, out);
                push(out, &[Token::RParen]);
            } else {
                out.push(self.tokens[i].clone());
                found |= self.emit(i + 1, end, out);
                out.push(self.tokens[end].clone());
            }
            i = end + 1;
        }
        found
    }

    /// For the `[` at `open` closed at `end`, when it holds `e FOR x IN l [IF p]`: where `FOR` is,
    /// `x`, where `l` starts and where `IF` is.
    fn comprehension(
        &self,
        open: usize,
        end: usize,
    ) -> Option<(usize, Token, usize, Option<usize>)> {
        let next = |mut i: usize| {
            while i < end && matches!(self.tokens[i].token, Token::Whitespace(_)) {
                i += 1;
            }
            i
        };
        let is = |i: usize, k: Keyword| {
            i < end
                && matches!(&self.tokens[i].token, Token::Word(w) if w.quote_style.is_none() && w.keyword == k)
        };
        // The first `FOR` outside any parentheses or nested brackets, then at the same level `IF`.
        let top = |from: usize, k: Keyword| {
            let mut i = from;
            while i < end {
                if is(i, k) {
                    return Some(i);
                }
                i = match self.tokens[i].token {
                    Token::LBracket | Token::LParen if self.close[i] < end => self.close[i] + 1,
                    _ => i + 1,
                };
            }
            None
        };
        let for_at = top(open + 1, Keyword::FOR)?;
        let var_at = next(for_at + 1);
        let var = match self.tokens.get(var_at).map(|t| &t.token) {
            Some(Token::Word(w)) if var_at < end => Token::Word(w.clone()),
            _ => return None,
        };
        let in_at = next(var_at + 1);
        if !is(in_at, Keyword::IN) || next(open + 1) == for_at {
            return None;
        }
        let list = in_at + 1;
        let pred = top(list, Keyword::IF);
        let list_end = pred.unwrap_or(end);
        if next(list) >= list_end || pred.is_some_and(|p| next(p + 1) >= end) {
            return None;
        }
        Some((for_at, var, list, pred))
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

    fn lambdas(n: usize) -> String {
        format!("{}1 +", "(a) -> ".repeat(n))
    }

    #[test]
    fn a_lambda_chain_after_burrmill_only_syntax_is_refused_quickly() {
        // Each prefix parses in Burrmill's dialect and not in sqlparser's DuckDB dialect, so a parse
        // in that dialect alone stopped at it and never reached the lambdas, which double per
        // level: 26 of them kept the engine's parse going for well over 15 s.
        for prefix in [
            "SELECT last_value(x IGNORE NULLS) OVER () AS v, ",
            "SELECT 'ab' ^@ 'a' AS v, ",
            "SELECT [y * 2 FOR y IN [1, 2]] AS v, ",
            "SELECT [y FOR y IN [1, 2] IF y > 1] AS v, ",
            "WITH t AS MATERIALIZED (SELECT 1 AS y) SELECT ",
            "SELECT ",
        ] {
            let sql = format!("{prefix}{}", lambdas(26));
            let (out, took) = on_small_stack(Duration::from_secs(5), move || check(&sql, None));
            assert_eq!(out, Err(Refusal::TooComplex), "{prefix}");
            assert!(took < Duration::from_secs(1), "{prefix}: {took:?}");
        }
        for sql in [
            "SELECT last_value(x IGNORE NULLS) OVER (ORDER BY t) AS v FROM t",
            "SELECT 'ab' ^@ 'a' AS v",
            "SELECT list_transform([1, 2], (a) -> a + 1) AS v",
        ] {
            assert_eq!(check(sql, None), Ok(()), "{sql}");
        }
    }

    #[test]
    fn lambdas_in_a_statement_no_dialect_parses_count_against_the_fallback() {
        let sql = format!("SELECT FROM WHERE 1, {}", lambdas(MAX_KEYWORD_CALLS + 1));
        let (out, took) = on_small_stack(Duration::from_secs(5), move || check(&sql, None));
        assert_eq!(out, Err(Refusal::TooManyKeywordCalls));
        assert!(took < Duration::from_secs(1), "{took:?}");
        let sql = format!("SELECT FROM WHERE 1, {}", lambdas(MAX_KEYWORD_CALLS));
        assert_eq!(check(&sql, None), Ok(()));
    }

    #[test]
    fn a_run_of_intervals_is_refused_without_overflowing_the_stack() {
        // A thousand `INTERVAL`s overflowed a 2 MiB stack inside the bounded parse, which aborts
        // the process. Now each is an error, and this thread goes on to answer the next statement.
        for sql in [
            format!("SELECT {}1", "INTERVAL ".repeat(1000)),
            format!("SELECT {}1", "INTERVAL - ".repeat(1000)),
            format!("SELECT {}'1' DAY", "INTERVAL NOT ".repeat(1000)),
        ] {
            let (out, took) = on_small_stack(Duration::from_secs(5), move || {
                (check(&sql, None), check("SELECT 1", None))
            });
            assert_eq!(out, (Err(Refusal::TooManyIntervals), Ok(())));
            assert!(took < Duration::from_secs(1), "{took:?}");
        }
        // The deepest the runs can still stack, under the recursion limit, returns.
        for sql in [
            format!(
                "SELECT {}1{}",
                "INTERVAL INTERVAL (".repeat(46),
                ")".repeat(46)
            ),
            format!(
                "SELECT {}1{}",
                "INTERVAL - INTERVAL CASE WHEN 1 THEN ".repeat(100),
                " END".repeat(100)
            ),
        ] {
            let (out, _) = on_small_stack(Duration::from_secs(5), move || check(&sql, None));
            assert_eq!(out, Ok(()));
        }
        assert_eq!(
            check(
                "SELECT INTERVAL '1' DAY + INTERVAL 2 HOUR - INTERVAL (3) MINUTE, -INTERVAL 1 DAY",
                None
            ),
            Ok(())
        );
    }

    fn explain_cases(prefix: &str, n: usize) -> String {
        format!("EXPLAIN FORMAT indent SELECT {}1", prefix.repeat(n))
    }

    #[test]
    fn nested_case_and_interval_behind_syntax_no_parse_here_reads_are_refused_quickly() {
        // `FORMAT indent` is DataFusion's syntax, so every parse here stops at it, and the levels
        // after it are retried at each one by the engine's parse: 20 took about 4 s, and 24 did not
        // return within 20 s.
        for prefix in [
            "INTERVAL CASE WHEN 1 THEN ",
            "CASE WHEN 1 THEN ",
            "INTERVAL NOT CASE WHEN 1 THEN ",
            "INTERVAL - INTERVAL CASE WHEN 1 THEN ",
            "INTERVAL CAST(",
            "INTERVAL NOT CAST(",
            "INTERVAL (",
            "CAST(",
        ] {
            for n in [MAX_KEYWORD_CALLS + 1, 24, 26] {
                let sql = explain_cases(prefix, n);
                let (out, took) = on_small_stack(Duration::from_secs(5), move || check(&sql, None));
                assert_eq!(out, Err(Refusal::TooManyKeywordCalls), "{prefix} x{n}");
                assert!(took < Duration::from_secs(1), "{prefix} x{n}: {took:?}");
            }
            // The most levels accepted, which the engine's own bound reads in well under a second.
            let most = (1..=MAX_KEYWORD_CALLS)
                .take_while(|&n| check(&explain_cases(prefix, n), None).is_ok())
                .last()
                .unwrap_or(0);
            assert!(most >= 4, "{prefix}: {most}");
            let sql = explain_cases(prefix, most);
            let (_, took) = on_small_stack(Duration::from_secs(10), move || {
                burrmill::df::check_expr_bounds(&sql).is_ok()
            });
            assert!(took < Duration::from_secs(1), "{prefix} x{most}: {took:?}");
        }
        // Each `CASE` closed before the next opens is one level, however many there are.
        let sql = format!(
            "EXPLAIN FORMAT indent SELECT {}1",
            "CASE WHEN 1 THEN INTERVAL CASE WHEN 2 THEN 3 END END, ".repeat(40)
        );
        assert_eq!(check(&sql, None), Ok(()));
    }

    #[test]
    fn a_second_statement_is_refused_before_it_is_parsed() {
        // A second statement no parse here reads, after a first one every parse reads.
        for n in [24, 26] {
            let sql = format!(
                "SELECT 1; {}",
                explain_cases("INTERVAL CASE WHEN 1 THEN ", n)
            );
            let (out, took) = on_small_stack(Duration::from_secs(5), move || {
                (check(&sql, None), check("SELECT 1", None))
            });
            assert_eq!(out, (Err(Refusal::Stacked), Ok(())), "x{n}");
            assert!(took < Duration::from_secs(1), "x{n}: {took:?}");
        }
        for sql in [
            "SELECT 1; SELECT 2",
            "SELECT 1;;SELECT 2",
            "SELECT ';'; SELECT 2",
            "SELECT 1 /* ; */; SELECT 2",
        ] {
            assert_eq!(check(sql, None), Err(Refusal::Stacked), "{sql}");
        }
        for sql in [
            "SELECT 1;",
            "SELECT 1;  \n",
            "SELECT 1; -- trailing comment",
            "SELECT 1; /* trailing */",
            "SELECT ';' AS s",
            r#"SELECT 1 AS ";""#,
        ] {
            assert_eq!(check(sql, None), Ok(()), "{sql}");
        }
    }

    /// The tokens as Burrmill's dialect parses them.
    fn parse_burrmill(tokens: Vec<TokenWithSpan>) -> Result<Vec<Statement>, ParserError> {
        let bounded = Bounded {
            budget: u64::MAX,
            burrmill: true,
            ..Bounded::default()
        };
        Parser::new(&bounded)
            .with_tokens_with_locations(tokens)
            .parse_statements()
    }

    #[test]
    fn the_token_rewrites_read_as_burrmills_text_rewrites() {
        let tokens = |sql: &str| {
            Tokenizer::new(&DuckDbDialect {}, sql)
                .tokenize_with_location()
                .unwrap()
        };
        // As written, and as Burrmill's `listcomp` and `asof` modules write it before parsing.
        for (sql, burrmill) in [
            (
                "SELECT [x * 2 FOR x IN [1, 2, 3]]",
                "SELECT list_transform([1, 2, 3], lambda x: x * 2)",
            ),
            (
                "SELECT [x * 2 FOR x IN [1, 2, 3] IF x > 1]",
                "SELECT list_transform(list_filter([1, 2, 3], lambda x: x > 1), lambda x: x * 2)",
            ),
            (
                "SELECT [[y + 1 FOR y IN x] FOR x IN [[1], [2]]]",
                "SELECT list_transform([[1], [2]], lambda x: list_transform(x, lambda y: y + 1))",
            ),
            (
                "WITH a AS NOT MATERIALIZED (SELECT 1) SELECT * FROM a",
                "WITH a AS (SELECT 1) SELECT * FROM a",
            ),
            (
                "SELECT * FROM t ASOF LEFT JOIN u USING (k)",
                "SELECT * FROM t LEFT JOIN u USING (k)",
            ),
        ] {
            let ours = as_burrmill_reads(&tokens(sql)).expect(sql);
            let theirs = Parser::parse_sql(&DuckDbDialect {}, burrmill).unwrap();
            if !sql.contains("ASOF") {
                // What Burrmill's own rewrite parses to, printed back.
                let canonical = burrmill::inspect::canonical(sql);
                assert_eq!(canonical, Some(theirs[0].to_string()), "{sql}");
            }
            assert_eq!(parse_burrmill(ours).unwrap(), theirs, "{sql}");
        }
        for sql in [
            "SELECT [1, 2, 3]",
            "SELECT list_transform([1], lambda x: 'for x in [2]') FROM t",
            "SELECT [x FOR x IN] FROM t",
        ] {
            assert!(as_burrmill_reads(&tokens(sql)).is_none(), "{sql}");
        }
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
            // And in Burrmill's mode as Burrmill's dialect does, where `ASOF` does not change the
            // statement on the way.
            let tokens = Tokenizer::new(&DuckDbDialect {}, &sql)
                .tokenize_with_location()
                .unwrap();
            if !sql.to_ascii_lowercase().contains("asof") {
                let ours = parse_burrmill(tokens.clone());
                let theirs = burrmill::inspect::parse(&sql);
                assert_eq!(ours.is_ok(), theirs.is_ok(), "{}", file.display());
                if let (Ok(ours), Ok(theirs)) = (ours, theirs) {
                    assert_eq!(ours, theirs, "{}", file.display());
                }
            }
            // As the engine reads it: where Burrmill parses its rewrite, so do the tokens here.
            if let Some(rewritten) = as_burrmill_reads(&tokens) {
                if burrmill::inspect::canonical(&sql).is_some() {
                    assert!(parse_burrmill(rewritten).is_ok(), "{}", file.display());
                }
            }
            // A file of several views is several statements, which `check` refuses together; each
            // on its own is accepted.
            let mut statements = vec![String::new()];
            for t in &tokens {
                match t.token {
                    Token::SemiColon => statements.push(String::new()),
                    ref t => statements.last_mut().unwrap().push_str(&t.to_string()),
                }
            }
            statements.retain(|s| {
                Tokenizer::new(&DuckDbDialect {}, s)
                    .tokenize()
                    .unwrap()
                    .iter()
                    .any(|t| !matches!(t, Token::Whitespace(_)))
            });
            if statements.len() > 1 {
                assert_eq!(
                    check(&sql, None),
                    Err(Refusal::Stacked),
                    "{}",
                    file.display()
                );
            }
            for one in &statements {
                assert_eq!(check(one, None), Ok(()), "{}: {one}", file.display());
            }
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
