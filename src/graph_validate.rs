//! RFC-0053 S0: the migration validator (#1264).
//!
//! Runs a corpus of real GraphQL operations against a reference Graph endpoint and against a nest,
//! and reports every way the two answers differ. It is the instrument #1212 needs as well, which is
//! why the sprint builds it once: RFC-0044 §10 wants a ported nest diffed against a reference field
//! by field, and this is that diff with the operations supplied by the caller.
//!
//! **The property that shapes the whole module: a clean result must not be obtainable by dropping
//! unsupported selections.** The obvious way to write a validator like this is to walk the two
//! responses and compare the keys they have in common, which is exactly wrong - a nest that answers
//! `{ id }` for a query asking `{ id, volumeUSD }` would then be reported as agreeing, and the
//! caller would migrate on the strength of it. So the comparison is driven by **what the query
//! asked for**, never by what either side happened to return, and a field that is requested and
//! absent is a divergence with its own name rather than a silence.

use anyhow::Context;
use serde_json::{Map, Value};
use std::collections::BTreeSet;
use std::fmt;

/// One requested field, as a path from the operation root.
///
/// Held as an owned path rather than a borrowed slice because divergences outlive the walk and are
/// sorted and printed; the corpora involved are small enough that this is not worth optimising.
pub type Path = Vec<String>;

fn render(path: &Path) -> String {
    path.join(".")
}

/// A selection set: the tree of fields an operation actually asked for.
///
/// The validator is driven by this rather than by either response, so an unsupported selection
/// cannot quietly leave the comparison.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Selection {
    /// Child selections by field name. A leaf has none.
    pub fields: std::collections::BTreeMap<String, Selection>,
}

impl Selection {
    pub fn leaf() -> Self {
        Selection::default()
    }

    /// Build a selection tree from `("a.b", "a.c")`-style paths, which is how a corpus file and the
    /// tests both express one.
    pub fn from_paths<I, S>(paths: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut root = Selection::default();
        for p in paths {
            let mut node = &mut root;
            for part in p.as_ref().split('.').filter(|s| !s.is_empty()) {
                node = node.fields.entry(part.to_string()).or_default();
            }
        }
        root
    }

    /// Derive the selection from the operation's own text.
    ///
    /// **This is the load-bearing half of the "cannot come back clean" property, not a
    /// convenience.** [`Selection::from_paths`] makes the caller enumerate what to compare, and a
    /// caller who omits a path gets a clean run for a field nobody checked - the same failure the
    /// module exists to prevent, moved one layer up into the corpus. Parsing the query means the
    /// comparison covers what was actually asked, and a corpus cannot silently under-specify.
    ///
    /// Deliberately a **selection-set** parser and not a GraphQL parser. It reads field names and
    /// nesting and skips what cannot change which fields were requested: arguments, directives,
    /// variable definitions and the operation header. Two constructs genuinely can change it and are
    /// therefore refused rather than ignored - see [`SelectionError`] - because guessing at them is
    /// how a field quietly leaves the comparison.
    pub fn from_query(query: &str) -> Result<Self, SelectionError> {
        let mut p = Cursor {
            src: query.as_bytes(),
            i: 0,
        };
        p.skip_trivia();
        // A named fragment cannot be resolved without spreading it, and a spread cannot be resolved
        // without the fragment: refusing both together is the only honest position.
        if query.contains("fragment ") {
            return Err(SelectionError::Fragment);
        }
        // Skip an operation header (`query Foo($x: Int)`) up to the first selection set.
        while p.i < p.src.len() && p.peek() != Some(b'{') {
            p.i += 1;
        }
        if p.peek() != Some(b'{') {
            return Err(SelectionError::NoSelectionSet);
        }
        p.parse_set()
    }

    fn is_leaf(&self) -> bool {
        self.fields.is_empty()
    }
}

/// Why a selection could not be derived from an operation's text.
///
/// Every variant is a **refusal**, never a silent best effort. A selection this parser cannot be
/// sure of is a comparison that might omit a field, and a validator that omits a field can come back
/// clean while the nest is wrong - the one outcome RFC-0053 S0 must not produce.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectionError {
    /// Named fragments change which fields an operation requests, and resolving a spread needs the
    /// fragment definition. Refused rather than skipped: skipping a spread drops every field it
    /// carries out of the comparison.
    Fragment,
    /// An inline fragment (`... on Pool { id }`) is the same problem in miniature.
    InlineFragment,
    /// No `{` at all, so there is nothing to compare.
    NoSelectionSet,
    /// Braces do not balance; the text is not an operation.
    Unbalanced,
    /// A field alias (`vol: volumeUSD`) makes the response key differ from the schema field, so
    /// comparing by field name would look in the wrong place in both answers.
    Alias { alias: String },
}

impl fmt::Display for SelectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SelectionError::Fragment => write!(
                f,
                "named fragments are not supported; inline the fields so the comparison covers them"
            ),
            SelectionError::InlineFragment => write!(
                f,
                "inline fragments are not supported; inline the fields so the comparison covers them"
            ),
            SelectionError::NoSelectionSet => write!(f, "the operation has no selection set"),
            SelectionError::Unbalanced => write!(f, "the operation's braces do not balance"),
            SelectionError::Alias { alias } => write!(
                f,
                "field alias `{alias}` changes the response key; aliases are not supported"
            ),
        }
    }
}

impl std::error::Error for SelectionError {}

/// A byte cursor over one operation's text.
struct Cursor<'a> {
    src: &'a [u8],
    i: usize,
}

impl Cursor<'_> {
    fn peek(&self) -> Option<u8> {
        self.src.get(self.i).copied()
    }

    /// Whitespace, commas (insignificant in GraphQL) and `#` comments.
    fn skip_trivia(&mut self) {
        loop {
            match self.peek() {
                Some(c) if c.is_ascii_whitespace() || c == b',' => self.i += 1,
                Some(b'#') => {
                    while let Some(c) = self.peek() {
                        self.i += 1;
                        if c == b'\n' {
                            break;
                        }
                    }
                }
                _ => return,
            }
        }
    }

    /// Skip a balanced `(..)` argument list, respecting string literals so a `)` inside one does not
    /// end it early.
    fn skip_parens(&mut self) -> Result<(), SelectionError> {
        let mut depth = 0usize;
        loop {
            match self.peek() {
                None => return Err(SelectionError::Unbalanced),
                Some(b'"') => self.skip_string(),
                Some(b'(') => {
                    depth += 1;
                    self.i += 1;
                }
                Some(b')') => {
                    depth -= 1;
                    self.i += 1;
                    if depth == 0 {
                        return Ok(());
                    }
                }
                Some(_) => self.i += 1,
            }
        }
    }

    fn skip_string(&mut self) {
        self.i += 1; // opening quote
        while let Some(c) = self.peek() {
            self.i += 1;
            if c == b'\\' {
                self.i += 1; // escaped char
            } else if c == b'"' {
                return;
            }
        }
    }

    fn name(&mut self) -> String {
        let start = self.i;
        while let Some(c) = self.peek() {
            if c.is_ascii_alphanumeric() || c == b'_' {
                self.i += 1;
            } else {
                break;
            }
        }
        String::from_utf8_lossy(&self.src[start..self.i]).into_owned()
    }

    /// Parse one `{ .. }` selection set into a [`Selection`].
    fn parse_set(&mut self) -> Result<Selection, SelectionError> {
        debug_assert_eq!(self.peek(), Some(b'{'));
        self.i += 1;
        let mut out = Selection::default();
        loop {
            self.skip_trivia();
            match self.peek() {
                None => return Err(SelectionError::Unbalanced),
                Some(b'}') => {
                    self.i += 1;
                    return Ok(out);
                }
                // `...` - a spread or an inline fragment. Both change the requested field set.
                Some(b'.') => return Err(SelectionError::InlineFragment),
                Some(b'@') => {
                    // A directive on the enclosing field; skip its name and any arguments.
                    self.i += 1;
                    let _ = self.name();
                    self.skip_trivia();
                    if self.peek() == Some(b'(') {
                        self.skip_parens()?;
                    }
                }
                Some(c) if c.is_ascii_alphabetic() || c == b'_' => {
                    let name = self.name();
                    self.skip_trivia();
                    if self.peek() == Some(b':') {
                        // `alias: field` - the response key is the alias, not the field, so
                        // comparing by field name would look in the wrong place on both sides.
                        return Err(SelectionError::Alias { alias: name });
                    }
                    if self.peek() == Some(b'(') {
                        self.skip_parens()?;
                        self.skip_trivia();
                    }
                    while self.peek() == Some(b'@') {
                        self.i += 1;
                        let _ = self.name();
                        self.skip_trivia();
                        if self.peek() == Some(b'(') {
                            self.skip_parens()?;
                        }
                        self.skip_trivia();
                    }
                    let child = if self.peek() == Some(b'{') {
                        self.parse_set()?
                    } else {
                        Selection::leaf()
                    };
                    // Merge rather than overwrite: the same field may be selected twice with
                    // different children, and dropping either would shrink the comparison.
                    let slot = out.fields.entry(name).or_default();
                    merge(slot, child);
                }
                Some(_) => self.i += 1,
            }
        }
    }
}

fn merge(into: &mut Selection, from: Selection) {
    for (k, v) in from.fields {
        let slot = into.fields.entry(k).or_default();
        merge(slot, v);
    }
}

/// How the two answers differ at one place.
///
/// Every variant is a *reported* mismatch. There is deliberately no `Ignored` or `Skipped`: a
/// selection the nest cannot answer is [`Divergence::Missing`], which counts, rather than a
/// silence that lets the run come back clean.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Divergence {
    /// The query asked for this field and the nest did not return it.
    Missing { path: String },
    /// The nest returned a field the reference did not, so the shapes differ even though nothing
    /// the caller asked for is absent.
    Extra { path: String },
    /// Both answered, with different values.
    Value {
        path: String,
        reference: String,
        nest: String,
    },
    /// Both answered, with different JSON types - a string where the reference had a number, most
    /// often a scalar serialisation difference and the thing RFC-0053 §Values is about.
    Type {
        path: String,
        reference: String,
        nest: String,
    },
    /// A list the caller asked for came back a different length. Reported separately from `Value`
    /// because pagination and ordering bugs look like this and want finding by name.
    Length {
        path: String,
        reference: usize,
        nest: usize,
    },
}

impl fmt::Display for Divergence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Divergence::Missing { path } => {
                write!(f, "{path}: requested, and the nest did not return it")
            }
            Divergence::Extra { path } => {
                write!(f, "{path}: the nest returned it, the reference did not")
            }
            Divergence::Value {
                path,
                reference,
                nest,
            } => write!(f, "{path}: reference {reference}, nest {nest}"),
            Divergence::Type {
                path,
                reference,
                nest,
            } => write!(f, "{path}: reference is {reference}, nest is {nest}"),
            Divergence::Length {
                path,
                reference,
                nest,
            } => write!(
                f,
                "{path}: reference has {reference} items, nest has {nest}"
            ),
        }
    }
}

/// The result of comparing one operation's two answers.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Report {
    pub divergences: Vec<Divergence>,
    /// Every path the operation asked for, so a reader can see the comparison's breadth rather than
    /// inferring it from the absence of complaints.
    pub compared: Vec<String>,
}

impl Report {
    pub fn is_clean(&self) -> bool {
        self.divergences.is_empty()
    }
}

/// Compare one operation's two answers, driven by what the operation asked for.
pub fn compare(selection: &Selection, reference: &Value, nest: &Value) -> Report {
    let mut report = Report::default();
    walk(selection, reference, nest, &mut Vec::new(), &mut report);
    report.divergences.sort();
    report.divergences.dedup();
    report.compared.sort();
    report.compared.dedup();
    report
}

fn type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "list",
        Value::Object(_) => "object",
    }
}

fn walk(sel: &Selection, reference: &Value, nest: &Value, path: &mut Path, out: &mut Report) {
    // Lists first: a selection applies to every element, and the two sides are zipped by position
    // because ordering is part of what RFC-0053 promises. A length difference is reported and the
    // common prefix is still compared, so one short page does not hide a wrong value in row 0.
    if let (Value::Array(a), Value::Array(b)) = (reference, nest) {
        if a.len() != b.len() {
            out.divergences.push(Divergence::Length {
                path: render(path),
                reference: a.len(),
                nest: b.len(),
            });
        }
        for (i, (ra, rb)) in a.iter().zip(b.iter()).enumerate() {
            path.push(format!("[{i}]"));
            walk(sel, ra, rb, path, out);
            path.pop();
        }
        return;
    }

    if sel.is_leaf() {
        out.compared.push(render(path));
        compare_scalar(reference, nest, path, out);
        return;
    }

    // **A container mismatch is reported where it happens, not as a missing child.**
    //
    // Both sides answered this field; they answered it with different *kinds* of thing - an object
    // against a string, or a list against an object. Coercing the odd one out with
    // `as_object().unwrap_or(&empty)` would turn that into `Missing` for every child selection
    // underneath, which names the wrong place and the wrong fault: it reads as "the nest omitted
    // `pool.token.symbol`" when what happened is that the nest's `pool.token` is not an object at
    // all. That is exactly the schema incompatibility the separate `Type` variant exists to name.
    // Raised by review of this change.
    if !matches!((reference, nest), (Value::Object(_), Value::Object(_))) {
        out.compared.push(render(path));
        if reference != nest {
            out.divergences.push(Divergence::Type {
                path: render(path),
                reference: type_name(reference).to_string(),
                nest: type_name(nest).to_string(),
            });
        }
        // Equal and not an object: both sides gave the same answer here and there is nothing
        // below it to compare. A null parent has no children in GraphQL, and two nulls agree -
        // descending anyway would coerce both to `{}` and report every selected child as missing
        // from a nest that answered exactly what the reference did.
        return;
    }

    // An object with children: drive from the selection, so a requested field that neither side
    // returned is still reported.
    let empty = Map::new();
    let ra = reference.as_object().unwrap_or(&empty);
    let rb = nest.as_object().unwrap_or(&empty);

    for (name, child) in &sel.fields {
        path.push(name.clone());
        match (ra.get(name), rb.get(name)) {
            (_, None) => {
                // **The load-bearing branch.** Requested and absent from the nest is a divergence,
                // whether or not the reference had it. Treating this as "nothing to compare" is
                // precisely how a validator reaches agreement by dropping what it cannot answer.
                out.compared.push(render(path));
                out.divergences
                    .push(Divergence::Missing { path: render(path) });
            }
            (None, Some(_)) => {
                out.compared.push(render(path));
                out.divergences
                    .push(Divergence::Extra { path: render(path) });
            }
            (Some(x), Some(y)) => walk(child, x, y, path, out),
        }
        path.pop();
    }

    // Fields the nest volunteered that the operation never asked for. The shapes differ, and a
    // caller comparing responses byte for byte would see it, so it is reported rather than ignored.
    let requested: BTreeSet<&String> = sel.fields.keys().collect();
    for name in rb.keys() {
        if !requested.contains(name) && !ra.contains_key(name) {
            path.push(name.clone());
            out.divergences
                .push(Divergence::Extra { path: render(path) });
            path.pop();
        }
    }
}

fn compare_scalar(reference: &Value, nest: &Value, path: &mut Path, out: &mut Report) {
    if reference == nest {
        return;
    }
    if type_name(reference) != type_name(nest) {
        out.divergences.push(Divergence::Type {
            path: render(path),
            reference: type_name(reference).to_string(),
            nest: type_name(nest).to_string(),
        });
        return;
    }
    out.divergences.push(Divergence::Value {
        path: render(path),
        reference: reference.to_string(),
        nest: nest.to_string(),
    });
}

/// One operation in a corpus file.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct Operation {
    pub name: String,
    pub query: String,
    /// Variables, passed through to both endpoints unchanged so the two answers are comparable.
    #[serde(default)]
    pub variables: serde_json::Value,
}

/// What one operation produced when run against both endpoints.
#[derive(Debug)]
pub struct Outcome {
    pub name: String,
    /// `Err` when the operation could not be compared at all - a selection this parser refuses, or
    /// an endpoint that failed. **Never treated as agreement**: the run's exit status counts these
    /// alongside divergences, because "we could not check it" and "it matched" must not look the
    /// same to the caller.
    pub result: Result<Report, String>,
}

/// Post one operation and return `data`, or the transport/GraphQL error as text.
async fn post(
    client: &reqwest::Client,
    url: &str,
    op: &Operation,
) -> Result<serde_json::Value, String> {
    let mut body = serde_json::json!({"query": op.query});
    if !op.variables.is_null() {
        body["variables"] = op.variables.clone();
    }
    // A gateway URL carries its API key in the path.
    let shown = crate::rpc::redact_url(url);
    let resp = client
        .post(url)
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("{shown}: {}", e.without_url()))?;
    let status = resp.status();
    let value: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("{shown}: response was not JSON ({})", e.without_url()))?;
    if !status.is_success() {
        return Err(format!("{shown}: HTTP {status}"));
    }
    // A GraphQL endpoint can return 200 with an `errors` array and a partial `data`. Comparing the
    // partial answer would be comparing a failure to a success, so it is reported as an error.
    if let Some(errors) = value.get("errors").and_then(|e| e.as_array()) {
        if !errors.is_empty() {
            return Err(format!(
                "{shown}: {}",
                serde_json::Value::Array(errors.clone())
            ));
        }
    }
    Ok(value
        .get("data")
        .cloned()
        .unwrap_or(serde_json::Value::Null))
}

/// Run a whole corpus against both endpoints.
pub async fn run(args: crate::cli::GraphValidateArgs) -> anyhow::Result<()> {
    let raw = std::fs::read_to_string(&args.corpus)
        .with_context(|| format!("reading corpus {}", args.corpus))?;
    let corpus: Vec<Operation> =
        serde_json::from_str(&raw).with_context(|| format!("parsing corpus {}", args.corpus))?;
    if corpus.is_empty() {
        anyhow::bail!(
            "corpus {} contains no operations; an empty corpus would pass without comparing anything",
            args.corpus
        );
    }
    let client = reqwest::Client::new();
    let mut outcomes = Vec::new();
    for op in &corpus {
        let result = match Selection::from_query(&op.query) {
            Err(e) => Err(format!("selection: {e}")),
            Ok(sel) => match (
                post(&client, &args.reference, op).await,
                post(&client, &args.nest, op).await,
            ) {
                (Err(e), _) => Err(format!("reference: {e}")),
                (_, Err(e)) => Err(format!("nest: {e}")),
                (Ok(a), Ok(b)) => Ok(compare(&sel, &a, &b)),
            },
        };
        outcomes.push(Outcome {
            name: op.name.clone(),
            result,
        });
    }

    let mut failed = 0usize;
    for o in &outcomes {
        match &o.result {
            Err(e) => {
                failed += 1;
                println!("✗ {}: not compared - {e}", o.name);
            }
            Ok(r) if r.is_clean() => println!("✓ {} ({} fields)", o.name, r.compared.len()),
            Ok(r) => {
                failed += 1;
                println!("✗ {} ({} fields)", o.name, r.compared.len());
                for d in &r.divergences {
                    println!("    {d}");
                }
            }
        }
    }
    println!(
        "{}/{} operations agree",
        outcomes.len() - failed,
        outcomes.len()
    );
    if failed > 0 {
        anyhow::bail!("{failed} of {} operations did not agree", outcomes.len());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sel(paths: &[&str]) -> Selection {
        Selection::from_paths(paths)
    }

    // --- the corpus runner ---------------------------------------------------------------

    /// An empty corpus must not pass. A run that compared nothing and reported success is the
    /// clean-by-omission failure at its largest possible scale.
    #[tokio::test]
    async fn an_empty_corpus_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let corpus = dir.path().join("c.json");
        std::fs::write(&corpus, "[]").unwrap();
        let err = run(crate::cli::GraphValidateArgs {
            corpus: corpus.display().to_string(),
            reference: "http://127.0.0.1:1".into(),
            nest: "http://127.0.0.1:1".into(),
        })
        .await
        .expect_err("an empty corpus must not report success");
        assert!(err.to_string().contains("no operations"), "{err}");
    }

    /// An operation that could not be compared - a refused selection, a dead endpoint - counts
    /// against the run. "We could not check it" and "it matched" must not look the same.
    #[tokio::test]
    async fn an_uncomparable_operation_fails_the_run_rather_than_passing_it() {
        let dir = tempfile::tempdir().unwrap();
        let corpus = dir.path().join("c.json");
        std::fs::write(
            &corpus,
            serde_json::to_string(&json!([{
                "name": "aliased",
                // Refused by the selection parser, so it is never even sent.
                "query": "{ pool { vol: volumeUSD } }"
            }]))
            .unwrap(),
        )
        .unwrap();
        let err = run(crate::cli::GraphValidateArgs {
            corpus: corpus.display().to_string(),
            reference: "http://127.0.0.1:1".into(),
            nest: "http://127.0.0.1:1".into(),
        })
        .await
        .expect_err("an operation that could not be compared must fail the run");
        assert!(err.to_string().contains("did not agree"), "{err}");
    }

    // --- Selection::from_query -------------------------------------------------------------

    #[test]
    fn a_query_yields_the_fields_it_asked_for() {
        let q = r#"
            query Pools($first: Int) {
              pools(first: $first, orderBy: volumeUSD) {
                id
                volumeUSD
                token0 { symbol decimals }
              }
            }
        "#;
        assert_eq!(
            Selection::from_query(q).unwrap(),
            sel(&[
                "pools.id",
                "pools.volumeUSD",
                "pools.token0.symbol",
                "pools.token0.decimals",
            ])
        );
    }

    /// Arguments can contain braces, parentheses and quoted strings. Losing track inside one would
    /// silently truncate the selection, which is the failure this whole module guards against.
    #[test]
    fn arguments_do_not_disturb_the_selection() {
        let q = r#"{ pools(where: {id_in: ["0x)", "0x{"]}, first: 5) { id } }"#;
        assert_eq!(Selection::from_query(q).unwrap(), sel(&["pools.id"]));
    }

    #[test]
    fn comments_and_commas_are_trivia() {
        let q = "{ pool { id, # the address\n  volumeUSD } }";
        assert_eq!(
            Selection::from_query(q).unwrap(),
            sel(&["pool.id", "pool.volumeUSD"])
        );
    }

    #[test]
    fn the_same_field_selected_twice_keeps_both_sets_of_children() {
        let q = "{ pool { token0 { symbol } token0 { decimals } } }";
        assert_eq!(
            Selection::from_query(q).unwrap(),
            sel(&["pool.token0.symbol", "pool.token0.decimals"]),
            "merging must not drop either selection"
        );
    }

    /// **Refusals, not best efforts.** Each of these changes which fields an operation requests, so
    /// skipping one would drop fields out of the comparison and let the run come back clean.
    #[test]
    fn a_named_fragment_is_refused_rather_than_skipped() {
        let q = "query { pool { ...PoolBits } } fragment PoolBits on Pool { id volumeUSD }";
        assert_eq!(
            Selection::from_query(q).unwrap_err(),
            SelectionError::Fragment
        );
    }

    #[test]
    fn an_inline_fragment_is_refused_rather_than_skipped() {
        let q = "{ pool { ... on Pool { id } } }";
        assert_eq!(
            Selection::from_query(q).unwrap_err(),
            SelectionError::InlineFragment
        );
    }

    /// An alias makes the response key differ from the field name, so a comparison keyed on the
    /// field would look in the wrong place in *both* answers and find nothing in either - agreement
    /// by mutual absence.
    #[test]
    fn an_alias_is_refused_because_the_response_key_is_not_the_field_name() {
        let q = "{ pool { vol: volumeUSD } }";
        assert_eq!(
            Selection::from_query(q).unwrap_err(),
            SelectionError::Alias {
                alias: "vol".into()
            }
        );
    }

    #[test]
    fn text_with_no_selection_set_is_refused() {
        assert_eq!(
            Selection::from_query("query Pools").unwrap_err(),
            SelectionError::NoSelectionSet
        );
    }

    #[test]
    fn unbalanced_braces_are_refused() {
        assert_eq!(
            Selection::from_query("{ pool { id }").unwrap_err(),
            SelectionError::Unbalanced
        );
    }

    /// The two halves joined up: a parsed operation drives the comparison, and a nest that cannot
    /// answer one of its fields is still reported.
    #[test]
    fn a_parsed_operation_still_catches_a_dropped_selection() {
        let q = "{ pool { id volumeUSD } }";
        let s = Selection::from_query(q).unwrap();
        let report = compare(
            &s,
            &json!({"pool": {"id": "0x1", "volumeUSD": "9"}}),
            &json!({"pool": {"id": "0x1"}}),
        );
        assert_eq!(
            report.divergences,
            vec![Divergence::Missing {
                path: "pool.volumeUSD".into()
            }]
        );
    }

    /// The property the whole module exists for, and the one the issue names: a clean result must
    /// not be obtainable by dropping selections the nest cannot answer.
    #[test]
    fn a_field_the_nest_cannot_answer_is_a_divergence_not_a_silence() {
        let s = sel(&["pool.id", "pool.volumeUSD"]);
        let reference = json!({"pool": {"id": "0x1", "volumeUSD": "123.45"}});
        let nest = json!({"pool": {"id": "0x1"}});

        let report = compare(&s, &reference, &nest);
        assert!(
            !report.is_clean(),
            "dropping an unsupported selection must not produce a clean run: {:?}",
            report.divergences
        );
        assert_eq!(
            report.divergences,
            vec![Divergence::Missing {
                path: "pool.volumeUSD".into()
            }]
        );
        assert!(
            report.compared.contains(&"pool.volumeUSD".to_string()),
            "the field must be counted as compared, not quietly left out of the denominator: {:?}",
            report.compared
        );
    }

    /// The same, when *neither* side returns it. A validator driven by the intersection of the two
    /// responses would call this agreement; there is nothing to intersect.
    #[test]
    fn a_requested_field_absent_from_both_sides_is_still_reported() {
        let s = sel(&["pool.id", "pool.feesUSD"]);
        let reference = json!({"pool": {"id": "0x1"}});
        let nest = json!({"pool": {"id": "0x1"}});
        let report = compare(&s, &reference, &nest);
        assert_eq!(
            report.divergences,
            vec![Divergence::Missing {
                path: "pool.feesUSD".into()
            }],
            "two silences are not an agreement"
        );
    }

    #[test]
    fn identical_answers_are_clean() {
        let s = sel(&["pool.id", "pool.volumeUSD"]);
        let v = json!({"pool": {"id": "0x1", "volumeUSD": "123.45"}});
        let report = compare(&s, &v, &v);
        assert!(report.is_clean(), "{:?}", report.divergences);
        assert_eq!(report.compared, vec!["pool.id", "pool.volumeUSD"]);
    }

    /// A scalar serialisation difference is the failure RFC-0053 §Values is about: `"1"` and `1`
    /// are not the same answer to a client, and a validator that coerces would hide it.
    #[test]
    fn a_string_where_the_reference_had_a_number_is_a_type_divergence() {
        let s = sel(&["pool.tick"]);
        let report = compare(
            &s,
            &json!({"pool": {"tick": 100}}),
            &json!({"pool": {"tick": "100"}}),
        );
        assert_eq!(
            report.divergences,
            vec![Divergence::Type {
                path: "pool.tick".into(),
                reference: "number".into(),
                nest: "string".into(),
            }]
        );
    }

    #[test]
    fn differing_values_of_the_same_type_are_reported_with_both_sides() {
        let s = sel(&["pool.volumeUSD"]);
        let report = compare(
            &s,
            &json!({"pool": {"volumeUSD": "100"}}),
            &json!({"pool": {"volumeUSD": "101"}}),
        );
        assert_eq!(
            report.divergences,
            vec![Divergence::Value {
                path: "pool.volumeUSD".into(),
                reference: "\"100\"".into(),
                nest: "\"101\"".into(),
            }]
        );
    }

    /// Lists are zipped by position, because ordering is part of the promise. A short page is
    /// reported *and* the overlapping rows are still compared, so a wrong row 0 is not masked by a
    /// length complaint.
    #[test]
    fn a_short_list_reports_length_and_still_compares_the_rows_it_has() {
        let s = sel(&["pools.id"]);
        let report = compare(
            &s,
            &json!({"pools": [{"id": "a"}, {"id": "b"}]}),
            &json!({"pools": [{"id": "z"}]}),
        );
        assert!(report.divergences.contains(&Divergence::Length {
            path: "pools".into(),
            reference: 2,
            nest: 1,
        }));
        assert!(
            report.divergences.contains(&Divergence::Value {
                path: "pools.[0].id".into(),
                reference: "\"a\"".into(),
                nest: "\"z\"".into(),
            }),
            "the rows that do overlap must still be compared: {:?}",
            report.divergences
        );
    }

    #[test]
    fn a_field_the_nest_volunteered_is_reported_as_extra() {
        let s = sel(&["pool.id"]);
        let report = compare(
            &s,
            &json!({"pool": {"id": "0x1"}}),
            &json!({"pool": {"id": "0x1", "surprise": 1}}),
        );
        assert_eq!(
            report.divergences,
            vec![Divergence::Extra {
                path: "pool.surprise".into()
            }]
        );
    }

    /// A container answered as a different kind of thing is a `Type` divergence *at the container*,
    /// not a `Missing` for every child underneath. Both sides answered `pool.token`; they disagree
    /// about what it is. Naming the children would point at the wrong place and call a schema
    /// incompatibility an omission. Raised in review of this change.
    #[test]
    fn an_object_answered_as_a_scalar_is_a_type_divergence_at_the_container() {
        let s = sel(&["pool.token.symbol"]);
        let report = compare(
            &s,
            &json!({"pool": {"token": {"symbol": "WETH"}}}),
            &json!({"pool": {"token": "0x1"}}),
        );
        assert_eq!(
            report.divergences,
            vec![Divergence::Type {
                path: "pool.token".into(),
                reference: "object".into(),
                nest: "string".into(),
            }],
            "the fault is at pool.token, not a missing pool.token.symbol"
        );
    }

    #[test]
    fn a_list_answered_as_an_object_is_a_type_divergence_at_the_container() {
        let s = sel(&["pools.id"]);
        let report = compare(
            &s,
            &json!({"pools": [{"id": "a"}]}),
            &json!({"pools": {"id": "a"}}),
        );
        assert_eq!(
            report.divergences,
            vec![Divergence::Type {
                path: "pools".into(),
                reference: "list".into(),
                nest: "object".into(),
            }]
        );
    }

    /// Null is a legitimate answer for a nullable field, and two nulls agree even where the
    /// operation selected children of them.
    #[test]
    fn two_nulls_under_a_nested_selection_agree() {
        let s = sel(&["pool.token.symbol"]);
        let report = compare(
            &s,
            &json!({"pool": {"token": null}}),
            &json!({"pool": {"token": null}}),
        );
        assert!(report.is_clean(), "{:?}", report.divergences);
    }

    #[test]
    fn a_null_against_an_object_is_a_type_divergence() {
        let s = sel(&["pool.token.symbol"]);
        let report = compare(
            &s,
            &json!({"pool": {"token": {"symbol": "WETH"}}}),
            &json!({"pool": {"token": null}}),
        );
        assert_eq!(
            report.divergences,
            vec![Divergence::Type {
                path: "pool.token".into(),
                reference: "object".into(),
                nest: "null".into(),
            }]
        );
    }

    /// Nested selections keep their full path, so a report names the place rather than the leaf.
    #[test]
    fn nested_paths_are_reported_in_full() {
        let s = sel(&["pool.token0.symbol"]);
        let report = compare(
            &s,
            &json!({"pool": {"token0": {"symbol": "WETH"}}}),
            &json!({"pool": {"token0": {}}}),
        );
        assert_eq!(
            report.divergences,
            vec![Divergence::Missing {
                path: "pool.token0.symbol".into()
            }]
        );
    }
}
