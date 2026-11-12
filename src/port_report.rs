//! RFC-0044 S1: classify a subgraph's `schema.graphql` and mappings, emit the port report.
//!
//! Nothing here runs in the data path and nothing executes AssemblyScript. The mappings are
//! read as text. The report is the deliverable; scaffolding a nest is RFC-0044 S2
//! (`port_emit`).

use anyhow::{bail, Context, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use yaml_rust2::{Yaml, YamlLoader};

/// RFC-0044 §5a. Ordered so `max` is the most severe class a field can hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Class {
    Exact,
    CallDerived,
    FixedPoint,
    Unreachable,
}

impl Class {
    pub fn as_str(self) -> &'static str {
        match self {
            Class::Exact => "exact",
            Class::CallDerived => "call-derived",
            Class::FixedPoint => "fixed-point",
            Class::Unreachable => "unreachable",
        }
    }

    pub fn heading(self) -> &'static str {
        match self {
            Class::Exact => "exact",
            Class::CallDerived => "call-derived",
            Class::FixedPoint => "fixed point",
            Class::Unreachable => "unreachable",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Citation {
    pub file: String,
    pub line: usize,
}

impl Citation {
    pub fn display(&self) -> String {
        format!("{}:{}", self.file, self.line)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldRow {
    pub entity: String,
    pub field: String,
    pub class: Class,
    pub citation: Citation,
    pub reason: String,
    /// Declared `T!` rather than `T`. The emitter needs it to decide whether a single literal can answer
    /// for every row (#1316).
    pub non_null: bool,
    /// For a `@derivedFrom` field, the entity it looks up and the forward field it keys on.
    pub derived_from: Option<(String, String)>,
}

impl FieldRow {
    pub fn name(&self) -> String {
        format!("{}.{}", self.entity, self.field)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
    pub source: String,
    pub fields: Vec<FieldRow>,
    /// Handlers the manifest names that no mapping file defines.
    pub missing_handlers: Vec<String>,
    /// Whether any mapping source was read. A deployment CID carries compiled WASM only, and then every
    /// row says nothing about the mapping (#1947).
    pub mapping_source: bool,
}

impl Report {
    pub fn counts(&self) -> [usize; 4] {
        let mut n = [0; 4];
        for f in &self.fields {
            n[f.class as usize] += 1;
        }
        n
    }
}

/// Classify `dir` and print the report to stdout. Hidden `nuthatch port-report` entry point.
pub fn print_report(dir: &Path) -> Result<()> {
    let report = classify_dir(dir)?;
    print!("{}", render_report(&report));
    Ok(())
}

/// Read `schema.graphql` and the mappings under `dir`, classify every entity field.
pub fn classify_dir(dir: &Path) -> Result<Report> {
    let schema_path = dir.join("schema.graphql");
    let schema_text = std::fs::read_to_string(&schema_path)
        .with_context(|| format!("read {}", schema_path.display()))?;
    let schema = parse_schema(&schema_text)?;
    let mappings = load_mappings(dir)?;
    let missing_handlers: Vec<String> = mappings
        .declared_handlers
        .iter()
        .filter(|h| !mappings.functions.contains_key(*h))
        .cloned()
        .collect();
    let mapping_source = if mappings.declared_handlers.is_empty() {
        !mappings.functions.is_empty()
    } else {
        missing_handlers.len() < mappings.declared_handlers.len()
    };
    let mut fields = classify(&schema, &mappings);
    if !mapping_source {
        for f in fields
            .iter_mut()
            .filter(|f| f.reason == NO_WRITER && f.citation.file == "schema.graphql")
        {
            f.reason = "not classified: no mapping source was read (a deployment CID carries \
                        compiled WASM, not the `.ts` this report reads)"
                .into();
        }
    }
    let source = dir
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(".")
        .to_string();
    Ok(Report {
        source,
        fields,
        missing_handlers,
        mapping_source,
    })
}

const NO_WRITER: &str = "no mapping writes this field";

pub fn render_report(report: &Report) -> String {
    let mut out = String::new();
    out.push_str("# Port report\n\n");
    out.push_str(
        "Every entity field in `schema.graphql`, classified against the mappings. Nothing has been \
         scaffolded. A field this report calls exact must match byte-for-byte; a field it names as \
         fixed point, call-derived or unreachable will not reproduce, and the reason is on that row. \
         An unpredicted divergence is a defect in this report.\n\n",
    );
    out.push_str(&format!("Source: `{}`\n\n", report.source));
    out.push_str(
        "Path: `--from-subgraph`. The proxy trap does not apply: the manifest pins implementation ABIs.\n\n",
    );
    if !report.mapping_source {
        out.push_str(
            "**Nothing was classified.** No mapping source was read: the manifest's handlers are not \
             defined in any `.ts` file here. A deployment CID carries compiled WASM, and this report \
             reads AssemblyScript source, so every class below except `@derivedFrom` says nothing \
             about the subgraph. Point `--dir` at the subgraph's source repository.\n\n",
        );
    } else if !report.missing_handlers.is_empty() {
        out.push_str(&format!(
            "**Partly classified.** These handlers are named in the manifest and defined in no mapping \
             file read here, so a field only they write reads as unwritten: {}.\n\n",
            report
                .missing_handlers
                .iter()
                .map(|h| format!("`{h}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    let [exact, call, fixed, unreachable] = report.counts();
    out.push_str("## Summary\n\n");
    out.push_str("| Class | Count |\n|---|---:|\n");
    out.push_str(&format!("| exact | {exact} |\n"));
    out.push_str(&format!("| call-derived | {call} |\n"));
    out.push_str(&format!("| fixed point | {fixed} |\n"));
    out.push_str(&format!("| unreachable | {unreachable} |\n\n"));

    let groups = [
        (
            Class::Exact,
            "A pure function of decoded events. Port as a view or entity; byte-identical.",
        ),
        (
            Class::CallDerived,
            "Reads contract state at the row's block. Port as `[[calls]]` (RFC-0038 §3). Needs `--state-rpc`.",
        ),
        (
            Class::FixedPoint,
            "Reads back stored entity output. A nest can converge; the number will be different. This field will not reproduce.",
        ),
        (
            Class::Unreachable,
            "Will not be ported. This field will not reproduce.",
        ),
    ];
    for (class, blurb) in groups {
        out.push_str(&format!("## {}\n\n", class.heading()));
        out.push_str(blurb);
        out.push_str("\n\n");
        let rows: Vec<&FieldRow> = report.fields.iter().filter(|f| f.class == class).collect();
        if rows.is_empty() {
            out.push_str("_none_\n\n");
            continue;
        }
        out.push_str("| Field | Citation | Why |\n|---|---|---|\n");
        for row in rows {
            out.push_str(&format!(
                "| `{}` | `{}` | {} |\n",
                row.name(),
                row.citation.display(),
                escape_table(&row.reason)
            ));
        }
        out.push('\n');
    }

    out.push_str("## Traps\n\n");
    out.push_str(
        "These are the ones the three hand ports paid for, and they apply on this path:\n\n",
    );
    out.push_str(
        "- The proxy trap applies to `nuthatch init 0xAddr` and **not** to `--from-subgraph`. \
         This report is the latter.\n",
    );
    out.push_str(
        "- `[[factories]] watch` takes a contract alias or template name, never an address, \
         whatever `config-reference.md` shows.\n",
    );
    out.push_str(
        "- One proxy may need several ABIs across its history. Horizon renamed every staking \
         event; a nest carrying only the current ABI loses 366 million blocks silently.\n",
    );
    out.push_str(
        "- The snake_caser explodes acronyms: `ServiceURIUpdate` becomes `service_u_r_i_update`.\n",
    );
    out.push_str(
        "- Verify against the chain (`cast call` on the canonical getters), not the gateway. \
         On-chain sentinels (`deactivationRound = 2^256-1`) map to a view's `null`.\n",
    );
    out
}

fn escape_table(s: &str) -> String {
    s.replace('|', "\\|").replace('\n', " ")
}

// ── Schema ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct Schema {
    entities: Vec<Entity>,
    fulltext: Vec<Fulltext>,
}

#[derive(Debug, Clone)]
struct Entity {
    name: String,
    fields: Vec<SchemaField>,
}

#[derive(Debug, Clone)]
struct SchemaField {
    name: String,
    line: usize,
    derived_from: Option<String>,
    /// `BigDecimal!` rather than `BigDecimal`. Load-bearing for the constant case: graph-node refuses to
    /// save a row whose non-nullable field is unset, so a non-nullable field has a value on every stored
    /// row - which is what lets a single literal answer for all of them.
    non_null: bool,
    /// The type with list brackets and `!` removed: `Trade` for `[Trade!]!`.
    named_type: String,
}

#[derive(Debug, Clone)]
struct Fulltext {
    name: String,
    line: usize,
}

fn parse_schema(text: &str) -> Result<Schema> {
    let mut entities = Vec::new();
    let mut fulltext = Vec::new();
    let chars: Vec<(usize, char)> = {
        let mut line = 1usize;
        let mut out = Vec::new();
        for c in text.chars() {
            out.push((line, c));
            if c == '\n' {
                line += 1;
            }
        }
        out
    };
    let mut i = 0;
    while i < chars.len() {
        skip_ws_and_graphql_trivia(&chars, &mut i);
        if i >= chars.len() {
            break;
        }
        if ident_at(&chars, i, "type") {
            let after = i + 4;
            if after < chars.len() && !is_ident_continue(chars[after].1) {
                i = after;
                skip_ws_and_graphql_trivia(&chars, &mut i);
                let Some(name) = take_ident(&chars, &mut i) else {
                    i += 1;
                    continue;
                };
                let header_line = chars[i.min(chars.len() - 1)].0;
                let header_end = find_char(&chars, i, '{').unwrap_or(chars.len());
                let header: String = chars[i.min(chars.len())..header_end]
                    .iter()
                    .map(|(_, c)| *c)
                    .collect();
                if name == "_Schema_" {
                    // Directive-only in this dialect: the next `{` belongs to the
                    // following type, not to `_Schema_`.
                    let next = find_next_type_decl(&chars, i).unwrap_or(chars.len());
                    let header: String = chars[i.min(chars.len())..next]
                        .iter()
                        .map(|(_, c)| *c)
                        .collect();
                    collect_fulltext(&header, header_line, &mut fulltext);
                    i = next;
                    continue;
                }
                if !header.contains("@entity") {
                    if let Some(open) = find_char(&chars, i, '{') {
                        if let Some(close) = match_brace(&chars, open) {
                            i = close + 1;
                            continue;
                        }
                    }
                    i += 1;
                    continue;
                }
                let Some(open) = find_char(&chars, i, '{') else {
                    i += 1;
                    continue;
                };
                let Some(close) = match_brace(&chars, open) else {
                    bail!("unclosed entity `{name}` in schema.graphql");
                };
                let body: String = chars[open + 1..close].iter().map(|(_, c)| *c).collect();
                let body_start_line = chars[open].0;
                let fields = parse_fields(&body, body_start_line);
                entities.push(Entity { name, fields });
                i = close + 1;
                continue;
            }
        }
        i += 1;
    }
    if entities.is_empty() {
        bail!("schema.graphql declares no @entity types");
    }
    Ok(Schema { entities, fulltext })
}

fn collect_fulltext(header: &str, header_line: usize, out: &mut Vec<Fulltext>) {
    let mut rest = header;
    while let Some(at) = rest.find("@fulltext") {
        rest = &rest[at + "@fulltext".len()..];
        if let Some(name) = directive_arg(rest, "name") {
            let line = header_line + header[..header.len() - rest.len()].matches('\n').count();
            out.push(Fulltext { name, line });
        }
    }
}

fn directive_arg(after_directive: &str, key: &str) -> Option<String> {
    let pat = format!("{key}:");
    let at = after_directive.find(&pat)?;
    let s = after_directive[at + pat.len()..].trim_start();
    if let Some(stripped) = s.strip_prefix('"') {
        let end = stripped.find('"')?;
        return Some(stripped[..end].to_string());
    }
    let ident: String = s
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    if ident.is_empty() {
        None
    } else {
        Some(ident)
    }
}

/// Field definitions in an entity body, in order, with the line each starts on.
///
/// **Not line-based.** GraphQL puts no significance on newlines, and
/// `type Token @entity { id: ID! derivedETH: BigDecimal! }` is as valid as the multi-line form.
/// Splitting on lines and taking the first `name:` from each recorded only `id` and dropped every
/// later field on the line - silently, which for a report whose whole promise is "every field,
/// classified" means the author is told about a field that does not exist and not told about one
/// that does (#1210).
///
/// A field starts at an identifier followed by `:` **at depth zero**: not inside `[..]` (list
/// types), not inside `(..)` (a directive's arguments, which contain their own colons, as
/// `@derivedFrom(field: "pool")` does), and not inside a string or a `#` comment. Everything from
/// one field start to the next is that field's type and directives, so a directive on its own line
/// still attaches to the field above it.
fn parse_fields(body: &str, start_line: usize) -> Vec<SchemaField> {
    let starts = field_starts(body);
    let mut fields = Vec::new();
    for (idx, (name, at)) in starts.iter().enumerate() {
        let end = starts.get(idx + 1).map(|(_, n)| *n).unwrap_or(body.len());
        let segment = &body[*at..end];
        let dirs: String = segment
            .match_indices('@')
            .map(|(i, _)| segment[i..].trim())
            .collect::<Vec<_>>()
            .join(" ");
        // `start_line` is the 1-based line the `{` is on and `line_of` is 1-based within the
        // body, so the two overlap on that first line. The old per-line loop had the same overlap
        // (`start_line + offset + 1`) and reported every schema field one line below itself.
        let line = start_line + line_of(body, *at) - 1;
        // The declared type, between the `:` and the end of this field's segment or its first directive.
        let ty = segment
            .split_once(':')
            .map(|(_, rest)| rest.split('@').next().unwrap_or(rest))
            .map(|t| t.split('#').next().unwrap_or(t).trim().to_string())
            .unwrap_or_default();
        flush_field(&mut fields, Some((name.clone(), line)), &dirs, &ty);
    }
    fields
}

/// `(name, byte offset)` for every field definition in the body.
fn field_starts(body: &str) -> Vec<(String, usize)> {
    let bytes = body.as_bytes();
    let mut out: Vec<(String, usize)> = Vec::new();
    let mut brackets = 0i32;
    let mut parens = 0i32;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'#' => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
                continue;
            }
            b'"' => {
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' {
                    if bytes[i] == b'\\' {
                        i += 1;
                    }
                    i += 1;
                }
                i += 1;
                continue;
            }
            b'[' => brackets += 1,
            b']' => brackets -= 1,
            b'(' => parens += 1,
            b')' => parens -= 1,
            _ => {}
        }
        let at_word_start =
            i == 0 || !(bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_');
        if brackets == 0
            && parens == 0
            && at_word_start
            && (bytes[i].is_ascii_alphabetic() || bytes[i] == b'_')
        {
            let mut k = i + 1;
            while k < bytes.len() && (bytes[k].is_ascii_alphanumeric() || bytes[k] == b'_') {
                k += 1;
            }
            let mut n = k;
            while n < bytes.len() && bytes[n].is_ascii_whitespace() {
                n += 1;
            }
            if n < bytes.len() && bytes[n] == b':' {
                out.push((body[i..k].to_string(), i));
                i = n + 1;
                continue;
            }
            i = k;
            continue;
        }
        i += 1;
    }
    out
}

fn flush_field(
    fields: &mut Vec<SchemaField>,
    pending: Option<(String, usize)>,
    dirs: &str,
    ty: &str,
) {
    let Some((name, line)) = pending else {
        return;
    };
    if name.starts_with('_') && name != "id" {
        // skip graph-node internals if any
    }
    let derived_from = directive_arg(dirs, "field").filter(|_| dirs.contains("@derivedFrom"));
    fields.push(SchemaField {
        name,
        line,
        derived_from,
        non_null: ty.ends_with('!'),
        named_type: ty
            .trim_matches(|c: char| c == '[' || c == ']' || c == '!' || c.is_whitespace())
            .to_string(),
    });
}

fn skip_ws_and_graphql_trivia(chars: &[(usize, char)], i: &mut usize) {
    loop {
        if *i >= chars.len() {
            return;
        }
        let c = chars[*i].1;
        if c.is_whitespace() {
            *i += 1;
            continue;
        }
        if c == '#' {
            while *i < chars.len() && chars[*i].1 != '\n' {
                *i += 1;
            }
            continue;
        }
        if c == '"' {
            // skip "..." or """..."""
            if chars.get(*i + 1).map(|x| x.1) == Some('"')
                && chars.get(*i + 2).map(|x| x.1) == Some('"')
            {
                *i += 3;
                while *i + 2 < chars.len()
                    && !(chars[*i].1 == '"' && chars[*i + 1].1 == '"' && chars[*i + 2].1 == '"')
                {
                    *i += 1;
                }
                *i = (*i + 3).min(chars.len());
            } else {
                *i += 1;
                while *i < chars.len() && chars[*i].1 != '"' {
                    if chars[*i].1 == '\\' {
                        *i += 2;
                    } else {
                        *i += 1;
                    }
                }
                if *i < chars.len() {
                    *i += 1;
                }
            }
            continue;
        }
        return;
    }
}

fn ident_at(chars: &[(usize, char)], i: usize, want: &str) -> bool {
    for (k, wc) in (i..).zip(want.chars()) {
        if k >= chars.len() || chars[k].1 != wc {
            return false;
        }
    }
    true
}

fn is_ident_continue(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

fn take_ident(chars: &[(usize, char)], i: &mut usize) -> Option<String> {
    if *i >= chars.len() {
        return None;
    }
    let c = chars[*i].1;
    if !(c.is_ascii_alphabetic() || c == '_') {
        return None;
    }
    let start = *i;
    *i += 1;
    while *i < chars.len() && is_ident_continue(chars[*i].1) {
        *i += 1;
    }
    Some(chars[start..*i].iter().map(|(_, c)| *c).collect())
}

fn find_next_type_decl(chars: &[(usize, char)], start: usize) -> Option<usize> {
    let mut i = start;
    let mut paren = 0i32;
    let mut bracket = 0i32;
    let mut brace = 0i32;
    while i < chars.len() {
        skip_ws_and_graphql_trivia(chars, &mut i);
        if i >= chars.len() {
            return None;
        }
        match chars[i].1 {
            '(' => paren += 1,
            ')' => paren -= 1,
            '[' => bracket += 1,
            ']' => bracket -= 1,
            '{' => brace += 1,
            '}' => brace -= 1,
            _ => {
                if paren == 0
                    && bracket == 0
                    && brace == 0
                    && ident_at(chars, i, "type")
                    && (i == 0 || !is_ident_continue(chars[i - 1].1))
                {
                    let after = i + 4;
                    if after >= chars.len() || !is_ident_continue(chars[after].1) {
                        return Some(i);
                    }
                }
            }
        }
        i += 1;
    }
    None
}

fn find_char(chars: &[(usize, char)], start: usize, want: char) -> Option<usize> {
    let mut i = start;
    while i < chars.len() {
        skip_ws_and_graphql_trivia(chars, &mut i);
        if i >= chars.len() {
            return None;
        }
        if chars[i].1 == want {
            return Some(i);
        }
        // Don't skip past `{` looking for `{` from a later token: if this isn't it, advance one.
        if chars[i].1 == '{' || chars[i].1 == '}' {
            return if chars[i].1 == want { Some(i) } else { None };
        }
        i += 1;
    }
    None
}

fn match_brace(chars: &[(usize, char)], open: usize) -> Option<usize> {
    if open >= chars.len() || chars[open].1 != '{' {
        return None;
    }
    let mut depth = 0;
    let mut i = open;
    while i < chars.len() {
        let c = chars[i].1;
        if c == '"' {
            skip_ws_and_graphql_trivia(chars, &mut i);
            continue;
        }
        if c == '#' {
            while i < chars.len() && chars[i].1 != '\n' {
                i += 1;
            }
            continue;
        }
        if c == '{' {
            depth += 1;
        } else if c == '}' {
            depth -= 1;
            if depth == 0 {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

// ── Mappings ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HandlerKind {
    Event,
    Call,
    Block,
    Helper,
}

#[derive(Debug, Clone)]
pub(crate) struct FunctionInfo {
    pub name: String,
    pub kind: HandlerKind,
    pub assignments: Vec<Assignment>,
    calls: BTreeSet<String>,
    contract_call: Option<Citation>,
    has_loop_load: bool,
    /// Loads an entity and reads a field off it **anywhere**, not only inside a loop (#1274).
    ///
    /// `classes.md` says a field that reads back stored entity output is fixed point, and it does
    /// not say "in a loop". The loop was only ever the shape the first three hand-ports happened to
    /// contain. A single `Pool.load(..)` followed by a field read is the same thing said in fewer
    /// lines, and reading it as exact promises byte-identical output for a number that is derived
    /// from state the nest computed for itself.
    reads_loaded_entity_field: bool,
    field_reads: Vec<(String, String)>, // (entity, field)
    /// Source order, so `fetchTokenSymbol(event.params.token0)` can map the helper's bind
    /// argument back onto the triggering row.
    param_names: Vec<String>,
    /// The declared return type's leading name: `Bribe` for `): Bribe {` and for `): Bribe | null {`.
    return_type: Option<String>,
    /// `<Template>.create(<arg>)` calls, with the template's manifest name and the argument as written.
    pub template_creates: Vec<(String, String, Citation)>,
    pub body: String,
    file: String,
    body_start_line: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct Assignment {
    pub receiver: String,
    pub entity: String,
    pub field: String,
    pub citation: Citation,
    pub expr: String,
    /// Whether the receiver is known to be an entity at all. `x.field = ..` where `x` is not a
    /// resolved binding is only an entity write if something saves `x`; without that, the
    /// unique-field fallback in [`classify`] would read any plain object assignment as a write to
    /// whichever entity happens to own that field name.
    pub receiver_is_entity: bool,
}

/// An `eventHandlers` entry: which source and event the named handler is wired to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HandlerBinding {
    pub handler: String,
    pub source: String,
    pub event: String,
}

#[derive(Debug, Clone)]
pub(crate) struct Mappings {
    pub functions: BTreeMap<String, FunctionInfo>,
    pub handlers: Vec<HandlerBinding>,
    /// Every handler the manifest names, of any kind.
    pub declared_handlers: BTreeSet<String>,
}

/// A contract read the mapping actually makes, tied to the Call-derived field it feeds.
///
/// `signature` is the ABI form (`symbol()`, `decimals()`), taken from `.try_*` / `.bind`.
/// `contract_arg` is the bind argument after substituting the helper's parameters for the
/// caller's, still in mapping syntax (`event.params.token0`). Emit turns that into a
/// `contract_column` or a literal address. A call we cannot trace is not returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MappingCall {
    pub entity: String,
    pub field: String,
    pub handler: String,
    pub signature: String,
    pub contract_arg: String,
    pub args: Vec<String>,
    pub citation: Citation,
}

pub(crate) fn load_mappings(dir: &Path) -> Result<Mappings> {
    let mut handler_kinds: BTreeMap<String, HandlerKind> = BTreeMap::new();
    let mut handler_bindings: Vec<HandlerBinding> = Vec::new();
    let mut yaml_files: Vec<PathBuf> = Vec::new();
    let mut templates: BTreeSet<String> = BTreeSet::new();
    for name in ["subgraph.yaml", "subgraph.yml"] {
        let p = dir.join(name);
        if p.exists() {
            if let Ok(text) = std::fs::read_to_string(&p) {
                collect_manifest_hints(
                    dir,
                    &text,
                    &mut handler_kinds,
                    &mut handler_bindings,
                    &mut yaml_files,
                    &mut templates,
                );
            }
        }
    }
    let mut files: BTreeSet<PathBuf> = yaml_files.into_iter().collect();
    walk_ts(dir, &mut files);
    let mut functions = BTreeMap::new();
    for path in &files {
        let Ok(text) = std::fs::read_to_string(path) else {
            continue;
        };
        // Node scripts (templatify, hardhat) sit next to mappings. They are not AssemblyScript.
        if text.contains("require(\"") || text.contains("require('") {
            continue;
        }
        let rel = path
            .strip_prefix(dir)
            .unwrap_or(path)
            .to_string_lossy()
            .replace('\\', "/");
        let imported = template_imports(&text);
        for mut func in parse_functions(&text, &rel) {
            if let Some(kind) = handler_kinds.get(&func.name) {
                func.kind = *kind;
            }
            func.template_creates = std::mem::take(&mut func.template_creates)
                .into_iter()
                .filter_map(|(ident, arg, at)| {
                    let name = imported.get(&ident).cloned().unwrap_or(ident);
                    templates.contains(&name).then_some((name, arg, at))
                })
                .collect();
            functions.insert(func.name.clone(), func);
        }
    }
    bind_helper_returns(&mut functions);
    Ok(Mappings {
        functions,
        handlers: handler_bindings,
        declared_handlers: handler_kinds.into_keys().collect(),
    })
}

/// `import { Pool as PoolTemplate } from '../generated/templates'`, as alias → manifest name.
fn template_imports(text: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let mut rest = text;
    while let Some(at) = rest.find("import") {
        rest = &rest[at + "import".len()..];
        let Some(end) = rest.find(';').or_else(|| rest.find('\n')) else {
            break;
        };
        let stmt = &rest[..end];
        if !stmt.contains("generated/templates") {
            continue;
        }
        let (Some(open), Some(close)) = (stmt.find('{'), stmt.find('}')) else {
            continue;
        };
        for item in stmt[open + 1..close].split(',') {
            let item = item.trim();
            let (name, alias) = item.split_once(" as ").unwrap_or((item, item));
            if !name.trim().is_empty() {
                out.insert(alias.trim().to_string(), name.trim().to_string());
            }
        }
    }
    out
}

/// `let bribe = getBribe(..)` makes `bribe` whatever `getBribe` is declared to return. Without it a
/// getOrCreate helper's `= ZERO_INT` was the only write seen (Bunni's `Bribe.amount`, S0 #1941).
fn bind_helper_returns(functions: &mut BTreeMap<String, FunctionInfo>) {
    let returns: BTreeMap<String, String> = functions
        .iter()
        .filter_map(|(n, f)| f.return_type.clone().map(|t| (n.clone(), t)))
        .collect();
    for func in functions.values_mut() {
        for i in 0..func.assignments.len() {
            if !func.assignments[i].entity.is_empty() {
                continue;
            }
            let receiver = func.assignments[i].receiver.clone();
            let Some(expr) = plain_local_expr(&func.body, &receiver) else {
                continue;
            };
            let callee: String = expr
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            if !expr[callee.len()..].starts_with('(') {
                continue;
            }
            if let Some(ty) = returns.get(&callee) {
                func.assignments[i].entity = ty.clone();
            }
        }
    }
}

fn collect_manifest_hints(
    dir: &Path,
    text: &str,
    kinds: &mut BTreeMap<String, HandlerKind>,
    bindings: &mut Vec<HandlerBinding>,
    files: &mut Vec<PathBuf>,
    templates: &mut BTreeSet<String>,
) {
    let Ok(docs) = YamlLoader::load_from_str(text) else {
        return;
    };
    let Some(doc) = docs.first() else {
        return;
    };
    for key in ["dataSources", "templates"] {
        let Some(arr) = get_yaml(doc, key).and_then(|v| v.as_vec()) else {
            continue;
        };
        for src in arr {
            if key == "templates" {
                if let Some(name) = get_yaml(src, "name").and_then(|v| v.as_str()) {
                    templates.insert(name.to_string());
                }
            }
            let Some(mapping) = get_yaml(src, "mapping") else {
                continue;
            };
            if let Some(file) = mapping_file_path(dir, mapping) {
                files.push(file);
            }
            take_handlers(mapping, "eventHandlers", HandlerKind::Event, kinds);
            take_handlers(mapping, "callHandlers", HandlerKind::Call, kinds);
            take_handlers(mapping, "blockHandlers", HandlerKind::Block, kinds);
            let source = get_yaml(src, "name").and_then(|v| v.as_str()).unwrap_or("");
            take_event_bindings(mapping, source, bindings);
        }
    }
}

fn get_yaml<'a>(y: &'a Yaml, key: &str) -> Option<&'a Yaml> {
    y.as_hash()?
        .iter()
        .find(|(k, _)| k.as_str() == Some(key))
        .map(|(_, v)| v)
}

fn mapping_file_path(dir: &Path, mapping: &Yaml) -> Option<PathBuf> {
    let file = get_yaml(mapping, "file")?;
    let rel = match file {
        Yaml::String(s) => s.clone(),
        Yaml::Hash(_) => return None, // CID link; local tree has the path form
        _ => return None,
    };
    let p = dir.join(rel.trim_start_matches("./"));
    p.exists().then_some(p)
}

fn take_event_bindings(mapping: &Yaml, source: &str, bindings: &mut Vec<HandlerBinding>) {
    let Some(arr) = get_yaml(mapping, "eventHandlers").and_then(|v| v.as_vec()) else {
        return;
    };
    for entry in arr {
        let Some(handler) = get_yaml(entry, "handler").and_then(|v| v.as_str()) else {
            continue;
        };
        let event = get_yaml(entry, "event")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let event_name = event.split('(').next().unwrap_or(event).trim();
        if event_name.is_empty() || source.is_empty() {
            continue;
        }
        bindings.push(HandlerBinding {
            handler: handler.to_string(),
            source: source.to_string(),
            event: event_name.to_string(),
        });
    }
}

fn take_handlers(
    mapping: &Yaml,
    key: &str,
    kind: HandlerKind,
    kinds: &mut BTreeMap<String, HandlerKind>,
) {
    let Some(arr) = get_yaml(mapping, key).and_then(|v| v.as_vec()) else {
        return;
    };
    for entry in arr {
        if let Some(name) = get_yaml(entry, "handler").and_then(|v| v.as_str()) {
            kinds
                .entry(name.to_string())
                .and_modify(|k| {
                    if kind == HandlerKind::Block {
                        *k = HandlerKind::Block;
                    }
                })
                .or_insert(kind);
        }
    }
}

impl PartialOrd for HandlerKind {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HandlerKind {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        fn rank(k: HandlerKind) -> u8 {
            match k {
                HandlerKind::Helper => 0,
                HandlerKind::Event => 1,
                HandlerKind::Call => 2,
                HandlerKind::Block => 3,
            }
        }
        rank(*self).cmp(&rank(*other))
    }
}

fn walk_ts(dir: &Path, files: &mut BTreeSet<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if path.is_dir() {
            if matches!(
                name.as_ref(),
                "node_modules" | "generated" | "build" | "tests" | ".git" | "abis"
            ) {
                continue;
            }
            walk_ts(&path, files);
        } else if name.ends_with(".ts") || name.ends_with(".as") {
            files.insert(path);
        }
    }
}

fn parse_functions(text: &str, file: &str) -> Vec<FunctionInfo> {
    let stripped = strip_ts_comments(text);
    let mut out = Vec::new();
    let bytes = stripped.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if !stripped.is_char_boundary(i) {
            i += 1;
            continue;
        }
        if let Some(name_range) = match_function_name(&stripped, i) {
            let name = stripped[name_range.clone()].to_string();
            let after_name = name_range.end;
            if let Some(brace) = stripped[after_name..].find('{') {
                let open = after_name + brace;
                if let Some(close) = match_ts_brace(&stripped, open) {
                    let sig = &stripped[after_name..open];
                    let params = parse_params(sig);
                    let param_names = param_names_of(sig);
                    let body = &stripped[open + 1..close];
                    let start_line = line_of(&stripped, open);
                    let mut func =
                        analyse_function(name, file, body, start_line, params, param_names);
                    func.return_type = return_type_of(sig);
                    out.push(func);
                    i = close + 1;
                    continue;
                }
            }
            i = after_name;
            continue;
        }
        if let Some((class, open)) = match_class(&stripped, i) {
            if let Some(close) = match_ts_brace(&stripped, open) {
                out.extend(parse_class_methods(&stripped, &class, open, close, file));
                i = close + 1;
                continue;
            }
        }
        i += 1;
    }
    out
}

/// `class NAME ... {`, giving the name and the offset of the opening brace.
fn match_class(text: &str, i: usize) -> Option<(String, usize)> {
    let bytes = text.as_bytes();
    if !text.is_char_boundary(i) || !text[i..].starts_with("class ") {
        return None;
    }
    if i > 0 && (bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_') {
        return None;
    }
    let mut k = i + "class ".len();
    skip_ws_str(text, &mut k);
    let name = take_ident_str(text, &mut k)?;
    // `extends Base` and `implements X` sit between the name and the brace, but nothing else does.
    let open = k + text[k..].find('{')?;
    if text[k..open].contains([';', '(', '=']) {
        return None;
    }
    Some((name, open))
}

/// The methods of one class, each as a function named `Class.method`. The Messari SDK writes only from
/// these (395 morpho fields unseen in S0, #1941); a property's declared type binds `this.<prop>`.
fn parse_class_methods(
    text: &str,
    class: &str,
    open: usize,
    close: usize,
    file: &str,
) -> Vec<FunctionInfo> {
    const MODIFIERS: [&str; 9] = [
        "public",
        "private",
        "protected",
        "static",
        "readonly",
        "get",
        "set",
        "export",
        "async",
    ];
    let bytes = text.as_bytes();
    let mut props: BTreeMap<String, String> = BTreeMap::new();
    let mut methods: Vec<(String, usize, usize, usize)> = Vec::new();
    let mut i = open + 1;
    while i < close {
        skip_ws_str(text, &mut i);
        if i >= close {
            break;
        }
        let mut k = i;
        let mut name = take_ident_str(text, &mut k);
        while let Some(n) = name.as_deref().filter(|n| MODIFIERS.contains(n)) {
            let mut look = k;
            skip_ws_str(text, &mut look);
            // A modifier is only one when an identifier follows it: `get(): T` is a method named `get`.
            if look < close && (bytes[look].is_ascii_alphabetic() || bytes[look] == b'_') {
                k = look;
                name = take_ident_str(text, &mut k);
            } else {
                name = Some(n.to_string());
                break;
            }
        }
        let Some(name) = name.filter(|n| !matches!(n.as_str(), "if" | "for" | "while" | "switch"))
        else {
            // Not a member start: move to the next line.
            i = text[i..close].find('\n').map_or(close, |n| i + n + 1);
            continue;
        };
        skip_ws_str(text, &mut k);
        while k < close && (bytes[k] == b'!' || bytes[k] == b'?') {
            k += 1;
        }
        skip_ws_str(text, &mut k);
        if k < close && bytes[k] == b'(' {
            let Some(paren) = match_ts_delim(text, k, b'(', b')') else {
                break;
            };
            let Some(brace) = text[paren..close].find('{').map(|b| paren + b) else {
                break;
            };
            let Some(end) = match_ts_brace(text, brace) else {
                break;
            };
            methods.push((name, k, brace, end));
            i = end + 1;
            continue;
        }
        if k < close && bytes[k] == b':' {
            k += 1;
            skip_ws_str(text, &mut k);
            if let Some(ty) = take_ident_str(text, &mut k) {
                props.insert(name, ty);
            }
        }
        // A property, with or without an initialiser, ends at its `;` or line end.
        i = text[k.min(close)..close]
            .find([';', '\n'])
            .map_or(close, |n| k + n + 1);
    }
    methods
        .into_iter()
        .map(|(name, sig_at, brace, end)| {
            let sig = &text[sig_at..brace];
            let mut bindings = props.clone();
            bindings.extend(parse_params(sig));
            let mut func = analyse_function(
                format!("{class}.{name}"),
                file,
                &text[brace + 1..end],
                line_of(text, brace),
                bindings,
                param_names_of(sig),
            );
            func.return_type = return_type_of(sig);
            func
        })
        .collect()
}

/// The leading type name after a signature's closing paren: `(..): Bribe | null` gives `Bribe`.
fn return_type_of(sig: &str) -> Option<String> {
    let close = sig.rfind(')')?;
    let rest = sig[close + 1..].trim_start().strip_prefix(':')?;
    let mut k = 0;
    let rest = rest.trim_start();
    take_ident_str(rest, &mut k)
}

fn match_function_name(text: &str, i: usize) -> Option<std::ops::Range<usize>> {
    if !text.is_char_boundary(i) {
        return None;
    }
    let rest = &text[i..];
    // Word-boundary `function NAME`.
    let at = rest.find("function")?;
    if at != 0 {
        return None;
    }
    if i > 0 {
        let prev = text.as_bytes()[i - 1];
        if prev.is_ascii_alphanumeric() || prev == b'_' {
            return None;
        }
    }
    let mut k = i + 8;
    let bytes = text.as_bytes();
    while k < bytes.len() && bytes[k].is_ascii_whitespace() {
        k += 1;
    }
    let start = k;
    if k >= bytes.len() || !(bytes[k].is_ascii_alphabetic() || bytes[k] == b'_') {
        return None;
    }
    k += 1;
    while k < bytes.len() && (bytes[k].is_ascii_alphanumeric() || bytes[k] == b'_') {
        k += 1;
    }
    Some(start..k)
}

fn match_ts_brace(text: &str, open: usize) -> Option<usize> {
    match_ts_delim(text, open, b'{', b'}')
}

fn match_ts_delim(text: &str, open: usize, o: u8, c: u8) -> Option<usize> {
    let bytes = text.as_bytes();
    if open >= bytes.len() || bytes[open] != o {
        return None;
    }
    let mut depth = 0;
    let mut i = open;
    let mut in_str: Option<u8> = None;
    while i < bytes.len() {
        let b = bytes[i];
        if let Some(q) = in_str {
            if b == b'\\' {
                i += 2;
                continue;
            }
            if b == q {
                in_str = None;
            }
            i += 1;
            continue;
        }
        if b == b'\'' || b == b'"' || b == b'`' {
            in_str = Some(b);
            i += 1;
            continue;
        }
        if b == o {
            depth += 1;
        } else if b == c {
            depth -= 1;
            if depth == 0 {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

fn line_of(text: &str, idx: usize) -> usize {
    1 + text[..idx].bytes().filter(|b| *b == b'\n').count()
}

fn strip_ts_comments(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    let mut in_str: Option<u8> = None;
    while i < bytes.len() {
        let b = bytes[i];
        if let Some(q) = in_str {
            out.push(b as char);
            if b == b'\\' && i + 1 < bytes.len() {
                out.push(bytes[i + 1] as char);
                i += 2;
                continue;
            }
            if b == q {
                in_str = None;
            }
            i += 1;
            continue;
        }
        if b == b'\'' || b == b'"' || b == b'`' {
            in_str = Some(b);
            out.push(b as char);
            i += 1;
            continue;
        }
        if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
            while i < bytes.len() && bytes[i] != b'\n' {
                out.push(' ');
                i += 1;
            }
            continue;
        }
        if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            i += 2;
            out.push(' ');
            out.push(' ');
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                out.push(if bytes[i] == b'\n' { '\n' } else { ' ' });
                i += 1;
            }
            if i + 1 < bytes.len() {
                out.push(' ');
                out.push(' ');
                i += 2;
            }
            continue;
        }
        out.push(b as char);
        i += 1;
    }
    out
}

fn param_names_of(sig: &str) -> Vec<String> {
    let start = match sig.find('(') {
        Some(i) => i + 1,
        None => return Vec::new(),
    };
    let end = match sig[start..].find(')') {
        Some(i) => start + i,
        None => return Vec::new(),
    };
    let mut out = Vec::new();
    for part in sig[start..end].split(',') {
        let part = part.trim();
        let Some(colon) = part.find(':') else {
            continue;
        };
        let var = part[..colon].trim();
        if !var.is_empty() {
            out.push(var.to_string());
        }
    }
    out
}

fn parse_params(sig: &str) -> BTreeMap<String, String> {
    let start = match sig.find('(') {
        Some(i) => i + 1,
        None => return BTreeMap::new(),
    };
    let end = match sig[start..].find(')') {
        Some(i) => start + i,
        None => return BTreeMap::new(),
    };
    let mut out = BTreeMap::new();
    for part in sig[start..end].split(',') {
        let part = part.trim();
        let Some(colon) = part.find(':') else {
            continue;
        };
        let var = part[..colon].trim();
        let ty = part[colon + 1..]
            .trim()
            .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
            .next()
            .unwrap_or("");
        if !var.is_empty() && !ty.is_empty() {
            out.insert(var.to_string(), ty.to_string());
        }
    }
    out
}

fn analyse_function(
    name: String,
    file: &str,
    body: &str,
    body_start_line: usize,
    params: BTreeMap<String, String>,
    param_names: Vec<String>,
) -> FunctionInfo {
    let mut bindings = params;
    bindings.extend(collect_bindings(body));
    let mut assignments = collect_assignments(body, file, body_start_line, &bindings);
    assignments.extend(collect_constructor_ids(
        body,
        file,
        body_start_line,
        &bindings,
    ));
    let calls = collect_calls(body);
    let contract_call = find_contract_call(body, file, body_start_line);
    let has_loop_load = loop_reads_a_loaded_entity_field(body, &bindings);
    let field_reads = collect_field_reads(body, &bindings);
    let reads_loaded_entity_field = returns_a_loaded_entity_field(body, &bindings);
    FunctionInfo {
        name,
        kind: HandlerKind::Helper,
        assignments,
        calls,
        contract_call,
        has_loop_load,
        reads_loaded_entity_field,
        field_reads,
        param_names,
        return_type: None,
        template_creates: collect_template_creates(body, file, body_start_line),
        body: body.to_string(),
        file: file.to_string(),
        body_start_line,
    }
}

/// `Ident.create(arg)` and `Ident.createWithContext(arg, ctx)`, unfiltered: the caller keeps only the
/// identifiers that name a manifest template.
fn collect_template_creates(
    body: &str,
    file: &str,
    body_start_line: usize,
) -> Vec<(String, String, Citation)> {
    let bytes = body.as_bytes();
    let mut out = Vec::new();
    for method in [".create(", ".createWithContext("] {
        let mut from = 0;
        while let Some(rel) = body[from..].find(method) {
            let dot = from + rel;
            from = dot + method.len();
            let mut b = dot;
            while b > 0 && (bytes[b - 1].is_ascii_alphanumeric() || bytes[b - 1] == b'_') {
                b -= 1;
            }
            if b == dot || (b > 0 && bytes[b - 1] == b'.') {
                continue;
            }
            let args = take_paren_list(body, dot + method.len() - 1);
            let Some(arg) = args.first() else {
                continue;
            };
            out.push((
                body[b..dot].to_string(),
                collapse_ws(arg.trim()),
                Citation {
                    file: file.to_string(),
                    line: body_start_line + line_of(body, dot) - 1,
                },
            ));
        }
    }
    out
}

fn collect_bindings(body: &str) -> BTreeMap<String, String> {
    let mut bind = BTreeMap::new();
    // function params live on the signature, which is outside `body`. Bindings from
    // `let x = new Token` / `let x = Token.load` inside the body are what we get here.
    // Parameter bindings are recovered in `parse_functions` via a second pass... so fold
    // them in here by scanning a reconstructed head: callers pass body only. We also
    // accept `changetype<Token>(...)`.
    let re_new = regex_find(
        body,
        r"(?:let|const|var)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*new\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(",
    );
    for (var, ent) in re_new {
        bind.insert(var, ent);
    }
    let re_load = regex_find(
        body,
        r"(?:let|const|var)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*([A-Za-z_][A-Za-z0-9_]*)\s*\.\s*load\s*\(",
    );
    for (var, ent) in re_load {
        bind.insert(var, ent);
    }
    let mut i = 0;
    while i < body.len() {
        if let Some((var, ent, next)) = match_bare_new(body, i) {
            bind.insert(var, ent);
            i = next;
            continue;
        }
        if let Some((var, ent, next)) = match_let_create(body, i) {
            bind.insert(var, ent);
            i = next;
            continue;
        }
        i += 1;
    }
    bind
}

/// Tiny two-group finder. Not a general regex engine; the patterns above are fixed.
fn regex_find(text: &str, kind: &str) -> Vec<(String, String)> {
    // kind is a tag, not a regex, so the matchers stay reviewable.
    let mut out = Vec::new();
    match kind {
        k if k.contains("new") => {
            let bytes = text.as_bytes();
            let mut i = 0;
            while i < bytes.len() {
                if let Some((var, ent, next)) = match_let_new(text, i) {
                    out.push((var, ent));
                    i = next;
                } else {
                    i += 1;
                }
            }
        }
        _ => {
            let bytes = text.as_bytes();
            let mut i = 0;
            while i < bytes.len() {
                if let Some((var, ent, next)) = match_let_load(text, i) {
                    out.push((var, ent));
                    i = next;
                } else {
                    i += 1;
                }
            }
        }
    }
    out
}

fn match_let_new(text: &str, i: usize) -> Option<(String, String, usize)> {
    if !text.is_char_boundary(i) {
        return None;
    }
    let rest = &text[i..];
    let kw = if rest.starts_with("let ") {
        4
    } else if rest.starts_with("const ") {
        6
    } else if rest.starts_with("var ") {
        4
    } else {
        return None;
    };
    if i > 0 {
        let p = text.as_bytes()[i - 1];
        if p.is_ascii_alphanumeric() || p == b'_' {
            return None;
        }
    }
    let mut k = i + kw;
    let var = take_ident_str(text, &mut k)?;
    skip_ws_str(text, &mut k);
    if !text[k..].starts_with('=') {
        return None;
    }
    k += 1;
    skip_ws_str(text, &mut k);
    if !text[k..].starts_with("new ") {
        return None;
    }
    k += 4;
    skip_ws_str(text, &mut k);
    let ent = take_ident_str(text, &mut k)?;
    skip_ws_str(text, &mut k);
    if !text[k..].starts_with('(') {
        return None;
    }
    Some((var, ent, k))
}

fn match_bare_new(text: &str, i: usize) -> Option<(String, String, usize)> {
    if !text.is_char_boundary(i) {
        return None;
    }
    let bytes = text.as_bytes();
    if i > 0 {
        let p = bytes[i - 1];
        if p.is_ascii_alphanumeric() || p == b'_' {
            return None;
        }
    }
    let mut k = i;
    let var = take_ident_str(text, &mut k)?;
    skip_ws_str(text, &mut k);
    if !text[k..].starts_with('=') {
        return None;
    }
    k += 1;
    skip_ws_str(text, &mut k);
    if !text[k..].starts_with("new ") {
        return None;
    }
    k += 4;
    skip_ws_str(text, &mut k);
    let ent = take_ident_str(text, &mut k)?;
    skip_ws_str(text, &mut k);
    if !text[k..].starts_with('(') {
        return None;
    }
    Some((var, ent, k))
}

fn match_let_create(text: &str, i: usize) -> Option<(String, String, usize)> {
    if !text.is_char_boundary(i) {
        return None;
    }
    let rest = &text[i..];
    let kw = if rest.starts_with("let ") {
        4
    } else if rest.starts_with("const ") {
        6
    } else if rest.starts_with("var ") {
        4
    } else {
        return None;
    };
    if i > 0 {
        let p = text.as_bytes()[i - 1];
        if p.is_ascii_alphanumeric() || p == b'_' {
            return None;
        }
    }
    let mut k = i + kw;
    let var = take_ident_str(text, &mut k)?;
    skip_ws_str(text, &mut k);
    if !text[k..].starts_with('=') {
        return None;
    }
    k += 1;
    skip_ws_str(text, &mut k);
    if text[k..].starts_with("createOrLoad") {
        k += "createOrLoad".len();
    } else if text[k..].starts_with("create") {
        k += "create".len();
    } else {
        return None;
    }
    let ent = take_ident_str(text, &mut k)?;
    if !ent.chars().next().is_some_and(|c| c.is_ascii_uppercase()) {
        return None;
    }
    skip_ws_str(text, &mut k);
    if !text[k..].starts_with('(') {
        return None;
    }
    Some((var, ent, k))
}

fn match_let_load(text: &str, i: usize) -> Option<(String, String, usize)> {
    if !text.is_char_boundary(i) {
        return None;
    }
    let rest = &text[i..];
    let kw = if rest.starts_with("let ") {
        4
    } else if rest.starts_with("const ") {
        6
    } else if rest.starts_with("var ") {
        4
    } else {
        return None;
    };
    if i > 0 {
        let p = text.as_bytes()[i - 1];
        if p.is_ascii_alphanumeric() || p == b'_' {
            return None;
        }
    }
    let mut k = i + kw;
    let var = take_ident_str(text, &mut k)?;
    skip_ws_str(text, &mut k);
    if !text[k..].starts_with('=') {
        return None;
    }
    k += 1;
    skip_ws_str(text, &mut k);
    let ent = take_ident_str(text, &mut k)?;
    skip_ws_str(text, &mut k);
    if !text[k..].starts_with('.') {
        return None;
    }
    k += 1;
    skip_ws_str(text, &mut k);
    if !text[k..].starts_with("load") {
        return None;
    }
    k += 4;
    skip_ws_str(text, &mut k);
    if !text[k..].starts_with('(') {
        return None;
    }
    Some((var, ent, k))
}

fn take_ident_str(text: &str, i: &mut usize) -> Option<String> {
    if !text.is_char_boundary(*i) {
        return None;
    }
    let bytes = text.as_bytes();
    if *i >= bytes.len() {
        return None;
    }
    if !(bytes[*i].is_ascii_alphabetic() || bytes[*i] == b'_') {
        return None;
    }
    let start = *i;
    *i += 1;
    while *i < bytes.len() && (bytes[*i].is_ascii_alphanumeric() || bytes[*i] == b'_') {
        *i += 1;
    }
    Some(text[start..*i].to_string())
}

fn skip_ws_str(text: &str, i: &mut usize) {
    let bytes = text.as_bytes();
    while *i < bytes.len() && bytes[*i].is_ascii_whitespace() {
        *i += 1;
    }
}

fn collect_assignments(
    body: &str,
    file: &str,
    body_start_line: usize,
    bindings: &BTreeMap<String, String>,
) -> Vec<Assignment> {
    let saved = collect_saved_idents(body);
    let mut out = Vec::new();
    let bytes = body.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if let Some((var, field, eq_at)) = match_assign(body, i) {
            let entity = bindings.get(&var).cloned().unwrap_or_default();
            let receiver_is_entity = bindings.contains_key(&var) || saved.contains(&var);
            let mut expr = resolve_local(body, &take_expr(body, eq_at + 1));
            // Rewritten as the `.plus` / `.minus` a running total is recognised by.
            match bytes[eq_at - 1] {
                b'+' => expr = format!("{var}.{field}.plus({expr})"),
                b'-' => expr = format!("{var}.{field}.minus({expr})"),
                _ => {}
            }
            let line = body_start_line + line_of(body, eq_at) - 1;
            out.push(Assignment {
                receiver: var,
                entity,
                field,
                citation: Citation {
                    file: file.to_string(),
                    line,
                },
                expr: collapse_ws(&expr),
                receiver_is_entity,
            });
            i = eq_at + 1;
            continue;
        }
        i += 1;
    }
    out
}

/// Identifiers this body calls `.save()` on. In graph-ts an entity is only persisted by `save()`,
/// so a receiver that is saved is an entity even when the binding collector could not see where it
/// came from (`getOrCreateToken(..)` and friends). A receiver that is never saved is a plain
/// object, and its fields are not entity writes.
fn collect_saved_idents(body: &str) -> BTreeSet<String> {
    let bytes = body.as_bytes();
    let mut out = BTreeSet::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'.' {
            let mut k = i + 1;
            skip_ws_str(body, &mut k);
            if body[k..].starts_with("save") {
                let mut n = k + 4;
                skip_ws_str(body, &mut n);
                if n < bytes.len() && bytes[n] == b'(' {
                    // Walk back over the receiver identifier.
                    let mut e = i;
                    while e > 0 && bytes[e - 1].is_ascii_whitespace() {
                        e -= 1;
                    }
                    let mut b = e;
                    while b > 0 && (bytes[b - 1].is_ascii_alphanumeric() || bytes[b - 1] == b'_') {
                        b -= 1;
                    }
                    if b < e && !bytes[b].is_ascii_digit() && body.is_char_boundary(b) {
                        out.insert(body[b..e].to_string());
                    }
                }
            }
        }
        i += 1;
    }
    out
}

fn match_assign(text: &str, i: usize) -> Option<(String, String, usize)> {
    if !text.is_char_boundary(i) {
        return None;
    }
    let bytes = text.as_bytes();
    if i > 0 {
        let p = bytes[i - 1];
        if p.is_ascii_alphanumeric() || p == b'_' {
            return None;
        }
    }
    let mut k = i;
    let var = take_ident_str(text, &mut k)?;
    skip_ws_str(text, &mut k);
    if k >= bytes.len() || bytes[k] != b'.' {
        return None;
    }
    k += 1;
    skip_ws_str(text, &mut k);
    let field = take_ident_str(text, &mut k)?;
    skip_ws_str(text, &mut k);
    // `+=` and `-=` are writes too; the caller reads the operator off the byte before the `=`.
    if k + 1 < bytes.len() && (bytes[k] == b'+' || bytes[k] == b'-') && bytes[k + 1] == b'=' {
        k += 1;
    }
    if k >= bytes.len() || bytes[k] != b'=' {
        return None;
    }
    if k + 1 < bytes.len() && (bytes[k + 1] == b'=' || bytes[k + 1] == b'>') {
        return None;
    }
    Some((var, field, k))
}

fn take_expr(text: &str, start: usize) -> String {
    let bytes = text.as_bytes();
    let mut i = start;
    let mut depth = 0i32;
    let mut in_str: Option<u8> = None;
    while i < bytes.len() {
        let b = bytes[i];
        if let Some(q) = in_str {
            if b == b'\\' {
                i += 2;
                continue;
            }
            if b == q {
                in_str = None;
            }
            i += 1;
            continue;
        }
        if b == b'\'' || b == b'"' || b == b'`' {
            in_str = Some(b);
            i += 1;
            continue;
        }
        if b == b'(' || b == b'[' || b == b'{' {
            depth += 1;
        } else if b == b')' || b == b']' || b == b'}' {
            if depth == 0 {
                break;
            }
            depth -= 1;
        } else if b == b';' && depth == 0 {
            break;
        } else if b == b'\n' && depth == 0 {
            // Keep a wrapped RHS (`=\n  fetchTokenSymbol(...)`). Stop on a new
            // assignment, `entity.save()`, or a blank line.
            let mut j = i + 1;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() && bytes[j] != b'\n' {
                j += 1;
            }
            if continues_expr(text, j) {
                i += 1;
                continue;
            }
            break;
        }
        i += 1;
    }
    text[start..i].trim().to_string()
}

fn continues_expr(text: &str, j: usize) -> bool {
    let bytes = text.as_bytes();
    if j >= bytes.len() {
        return false;
    }
    let b = bytes[j];
    if b == b'.' || b == b'+' || b == b'(' {
        return true;
    }
    if !(b.is_ascii_alphabetic() || b == b'_') {
        return false;
    }
    if match_assign(text, j).is_some() || is_entity_save(text, j) {
        return false;
    }
    let mut k = j;
    match take_ident_str(text, &mut k).as_deref() {
        Some(
            "let" | "const" | "var" | "return" | "if" | "for" | "while" | "function" | "export",
        ) => false,
        Some(_) => true,
        None => false,
    }
}

fn is_entity_save(text: &str, i: usize) -> bool {
    let mut k = i;
    if take_ident_str(text, &mut k).is_none() {
        return false;
    }
    skip_ws_str(text, &mut k);
    let bytes = text.as_bytes();
    if k >= bytes.len() || bytes[k] != b'.' {
        return false;
    }
    k += 1;
    skip_ws_str(text, &mut k);
    take_ident_str(text, &mut k).as_deref() == Some("save")
}

fn collapse_ws(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// `const decimals = fetchTokenDecimals(addr); token.decimals = decimals` should still
/// see the helper, not the local name.
fn resolve_local(body: &str, expr: &str) -> String {
    let ident = expr.trim();
    if ident.is_empty()
        || !ident.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        || ident.chars().next().is_some_and(|c| c.is_ascii_digit())
    {
        return expr.to_string();
    }
    let mut i = 0;
    while i < body.len() {
        if !body.is_char_boundary(i) {
            i += 1;
            continue;
        }
        let rest = &body[i..];
        let kw = if rest.starts_with("let ") {
            4
        } else if rest.starts_with("const ") {
            6
        } else if rest.starts_with("var ") {
            4
        } else {
            i += 1;
            continue;
        };
        if i > 0 {
            let p = body.as_bytes()[i - 1];
            if p.is_ascii_alphanumeric() || p == b'_' {
                i += 1;
                continue;
            }
        }
        let mut k = i + kw;
        let Some(var) = take_ident_str(body, &mut k) else {
            i += 1;
            continue;
        };
        if var != ident {
            i += 1;
            continue;
        }
        skip_ws_str(body, &mut k);
        if !body[k..].starts_with('=') {
            i += 1;
            continue;
        }
        let rhs = take_expr(body, k + 1);
        if rhs != ident {
            return collapse_ws(&rhs);
        }
        i += 1;
    }
    expr.to_string()
}

fn collect_constructor_ids(
    body: &str,
    file: &str,
    body_start_line: usize,
    _bindings: &BTreeMap<String, String>,
) -> Vec<Assignment> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < body.len() {
        let hit = match_let_new(body, i).or_else(|| match_bare_new(body, i));
        if let Some((var, ent, next)) = hit {
            let mut k = next;
            skip_ws_str(body, &mut k);
            if body[k..].starts_with('(') {
                let expr = take_expr(body, k + 1);
                let line = body_start_line + line_of(body, k) - 1;
                out.push(Assignment {
                    receiver: var,
                    entity: ent,
                    field: "id".into(),
                    citation: Citation {
                        file: file.to_string(),
                        line,
                    },
                    expr: collapse_ws(&expr),
                    // `new Entity(..)` names the entity outright.
                    receiver_is_entity: true,
                });
            }
            i = next;
            continue;
        }
        i += 1;
    }
    out
}

fn collect_calls(body: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let bytes = body.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if (i == 0
            || !(bytes[i - 1].is_ascii_alphanumeric()
                || bytes[i - 1] == b'_'
                || bytes[i - 1] == b'.'))
            && (bytes[i].is_ascii_alphabetic() || bytes[i] == b'_')
        {
            let mut k = i + 1;
            while k < bytes.len() && (bytes[k].is_ascii_alphanumeric() || bytes[k] == b'_') {
                k += 1;
            }
            let mut j = k;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if j < bytes.len() && bytes[j] == b'(' {
                let name = &body[i..k];
                if !matches!(
                    name,
                    "if" | "for"
                        | "while"
                        | "switch"
                        | "return"
                        | "new"
                        | "load"
                        | "save"
                        | "BigInt"
                        | "BigDecimal"
                        | "Address"
                        | "Bytes"
                        | "changetype"
                        | "require"
                ) {
                    out.insert(name.to_string());
                }
            }
            i = k;
            continue;
        }
        i += 1;
    }
    out
}

fn find_contract_call(body: &str, file: &str, body_start_line: usize) -> Option<Citation> {
    let needles = [".bind(", "ethereum.call(", ".try_"];
    let mut best: Option<usize> = None;
    for n in needles {
        if let Some(at) = body.find(n) {
            best = Some(best.map_or(at, |b| b.min(at)));
        }
    }
    best.map(|at| Citation {
        file: file.to_string(),
        line: body_start_line + line_of(body, at) - 1,
    })
}

/// A fixed point is a loop that **reads entity state it has loaded**, not a body that merely
/// contains a loop somewhere and a `.load(` somewhere.
///
/// `has_loop(body) && body.contains(".load(")` was body-wide, so a load outside every loop set the
/// class, and a loop that only loads and saves - which reproduces exactly - set it too. Both
/// report a field as non-reproducible when it is not, which is the wrong direction to be wrong in
/// for a port report: the author does the work of hand-checking a field that was fine.
///
/// `findEthPerToken` is the shape this exists for. It loops over whitelisted pools, loads each, and
/// reads a stored field off the loaded entity, so its answer depends on state derived from earlier
/// blocks. A loop with no such read has no such dependency. A braceless single-statement loop is
/// not treated as a fixed point rather than guessed at.
fn loop_reads_a_loaded_entity_field(body: &str, bindings: &BTreeMap<String, String>) -> bool {
    let bytes = body.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let at_word_start =
            i == 0 || !(bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_');
        let kw = if body.is_char_boundary(i) && body[i..].starts_with("for") {
            3
        } else if body.is_char_boundary(i) && body[i..].starts_with("while") {
            5
        } else {
            0
        };
        if at_word_start && kw > 0 {
            let after = i + kw;
            let boundary = after >= bytes.len()
                || !(bytes[after].is_ascii_alphanumeric() || bytes[after] == b'_');
            if boundary {
                if let Some(inner) = loop_body(body, after) {
                    if inner.contains(".load(") && !collect_field_reads(inner, bindings).is_empty()
                    {
                        return true;
                    }
                }
            }
        }
        i += 1;
    }
    false
}

/// The braced body of the loop whose keyword ends at `after`, or `None` when the header does not
/// have the `( .. ) { .. }` shape. A nested loop is reached by the outer scan continuing past here.
fn loop_body(body: &str, after: usize) -> Option<&str> {
    let bytes = body.as_bytes();
    let mut k = after;
    skip_ws_str(body, &mut k);
    if k >= bytes.len() || bytes[k] != b'(' {
        return None;
    }
    let close = match_ts_delim(body, k, b'(', b')')?;
    let mut b = close + 1;
    skip_ws_str(body, &mut b);
    if b >= bytes.len() || bytes[b] != b'{' {
        return None;
    }
    let end = match_ts_brace(body, b)?;
    Some(&body[b + 1..end])
}

/// Whether a `return` in this body hands back a field read off a loaded entity.
///
/// **Two independent facts are not one fact.** The first version of this asked only whether the body
/// contained `.load(` *and* whether it read any entity field anywhere - which are unrelated
/// questions. A helper that loads a pool, ignores it, and returns `ZERO_BD` satisfied both and was
/// marked fixed point, so a caller's genuinely exact field was reported as unreproducible. Raised in
/// review of #1274.
///
/// Still a syntactic approximation rather than dataflow: it asks whether a returned expression
/// mentions a binding that some field read is taken off. That is enough to separate "loads and
/// returns the stored value" from "loads and returns a constant", which is the distinction the class
/// turns on, and it errs toward fixed point only when a returned expression really does name the
/// loaded entity.
fn returns_a_loaded_entity_field(body: &str, bindings: &BTreeMap<String, String>) -> bool {
    if !body.contains(".load(") {
        return false;
    }
    // The locals a field is actually read off, e.g. `pool` in `pool.token0Price`.
    if collect_field_reads(body, bindings).is_empty() {
        return false;
    }
    let read_from: BTreeSet<&String> = bindings
        .keys()
        // A read off this binding: the name followed by `.`.
        .filter(|name| body.contains(&format!("{name}.")))
        .collect();
    if read_from.is_empty() {
        return false;
    }
    let mut rest = body;
    while let Some(at) = rest.find("return") {
        let after = &rest[at + "return".len()..];
        // Word boundary, so `returnValue` is not a return statement.
        let boundary = after
            .as_bytes()
            .first()
            .is_none_or(|c| !(c.is_ascii_alphanumeric() || *c == b'_'));
        if boundary {
            let end = after.find(['\n', ';']).unwrap_or(after.len());
            let expr = &after[..end];
            if read_from.iter().any(|b| expr.contains(&format!("{b}."))) {
                return true;
            }
        }
        rest = &rest[at + "return".len()..];
    }
    false
}

fn collect_field_reads(body: &str, bindings: &BTreeMap<String, String>) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let bytes = body.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if (i == 0 || !(bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_'))
            && (bytes[i].is_ascii_alphabetic() || bytes[i] == b'_')
        {
            let mut k = i + 1;
            while k < bytes.len() && (bytes[k].is_ascii_alphanumeric() || bytes[k] == b'_') {
                k += 1;
            }
            let var = &body[i..k];
            let mut j = k;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if j < bytes.len() && bytes[j] == b'.' {
                j += 1;
                while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                    j += 1;
                }
                let start = j;
                if j < bytes.len() && (bytes[j].is_ascii_alphabetic() || bytes[j] == b'_') {
                    j += 1;
                    while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_')
                    {
                        j += 1;
                    }
                    let field = &body[start..j];
                    let mut n = j;
                    while n < bytes.len() && bytes[n].is_ascii_whitespace() {
                        n += 1;
                    }
                    let is_call = n < bytes.len() && bytes[n] == b'(';
                    let is_assign = n < bytes.len()
                        && bytes[n] == b'='
                        && !(n + 1 < bytes.len() && bytes[n + 1] == b'=');
                    if !is_call && !is_assign && var != "event" {
                        if let Some(entity) = bindings.get(var) {
                            if field != "id" && field != "save" {
                                out.push((entity.clone(), field.to_string()));
                            }
                        }
                    }
                }
            }
            i = k;
            continue;
        }
        i += 1;
    }
    out
}

// ── Classify ────────────────────────────────────────────────────────────────

fn classify(schema: &Schema, mappings: &Mappings) -> Vec<FieldRow> {
    let entity_names: BTreeSet<&str> = schema.entities.iter().map(|e| e.name.as_str()).collect();
    let unique_fields = unique_field_owners(schema);
    let functions = &mappings.functions;
    let contract_helpers = contract_returning_helpers(functions);

    let mut writers: BTreeMap<(String, String), Vec<(String, Assignment)>> = BTreeMap::new();
    for func in functions.values() {
        for a in &func.assignments {
            // The unique-field fallback answers *which* entity, never *whether* the receiver is
            // one. Without that guard `let metadata = loadMetadata(); metadata.symbol = ..` is
            // recorded as a write to `Token.symbol` whenever `symbol` is uniquely owned, and the
            // field is then reported exact on the strength of an unrelated object assignment.
            let entity = if entity_names.contains(a.entity.as_str()) {
                a.entity.clone()
            } else if !a.receiver_is_entity {
                continue;
            } else if let Some(owner) = unique_fields.get(&a.field) {
                owner.clone()
            } else {
                continue;
            };
            if !entity_names.contains(entity.as_str()) {
                continue;
            }
            let mut a2 = a.clone();
            a2.entity = entity.clone();
            writers
                .entry((entity, a.field.clone()))
                .or_default()
                .push((func.name.clone(), a2));
        }
    }

    // Return class of a helper: what `x = helper(...)` inherits. Not applied to every
    // assignment inside a mixed event handler. Walk the call graph so a wrapper around
    // a contract-call helper is itself call-derived.
    let mut fn_class: BTreeMap<String, Class> = BTreeMap::new();
    for (name, func) in functions {
        let mut c = Class::Exact;
        if func.contract_call.is_some() {
            c = Class::CallDerived;
        }
        // **The seed, and why it is not only the loop.** `field_class` cannot resolve this: a
        // field is fixed point *because* something reads it back, so promoting the writer from the
        // read field's class is circular and has no seed. `has_loop_load` was that seed, and it is
        // too narrow - `getNativePriceInUSD` does one `Pool.load(..)` and reads a field off it, so
        // it stayed exact while its sibling `findNativePerToken`, which loads inside a loop, was
        // caught. Identical shape, one field over (#1274).
        if func.has_loop_load || func.reads_loaded_entity_field {
            c = c.max(Class::FixedPoint);
        }
        if func.kind == HandlerKind::Block {
            c = Class::Unreachable;
        }
        fn_class.insert(name.clone(), c);
    }
    propagate_fn_class(functions, &mut fn_class);

    let mut field_class: BTreeMap<(String, String), (Class, Citation, String)> = BTreeMap::new();

    for ft in &schema.fulltext {
        field_class.insert(
            ("_Schema_".into(), ft.name.clone()),
            (
                Class::Unreachable,
                Citation {
                    file: "schema.graphql".into(),
                    line: ft.line,
                },
                format!("`@fulltext` search index `{}`", ft.name),
            ),
        );
    }

    for ent in &schema.entities {
        for f in &ent.fields {
            let key = (ent.name.clone(), f.name.clone());
            if let Some(src) = &f.derived_from {
                field_class.insert(
                    key,
                    (
                        Class::Exact,
                        Citation {
                            file: "schema.graphql".into(),
                            line: f.line,
                        },
                        format!("`@derivedFrom(field: \"{src}\")` - reverse lookup, a SQL join"),
                    ),
                );
                continue;
            }
            let Some(sites) = writers.get(&key) else {
                field_class.insert(
                    key,
                    (
                        Class::Unreachable,
                        Citation {
                            file: "schema.graphql".into(),
                            line: f.line,
                        },
                        NO_WRITER.into(),
                    ),
                );
                continue;
            };
            let (worst, citation, reason) =
                worst_writer(sites, functions, &fn_class, &field_class, &contract_helpers);
            field_class.insert(key, (worst, citation, reason));
        }
    }

    // A helper that reads a fixed-point field returns a fixed-point value. Call-derived
    // (decimals as a scale factor) does not leak: RFC-0038 §6a prices stay exact.
    for _ in 0..16 {
        let mut changed = false;
        let snapshot = field_class.clone();
        for (name, func) in functions {
            let mut bump = false;
            for (ent, field) in &func.field_reads {
                if snapshot
                    .get(&(ent.clone(), field.clone()))
                    .is_some_and(|(c, _, _)| *c == Class::FixedPoint)
                {
                    bump = true;
                }
            }
            for (field, owner) in &unique_fields {
                if (func
                    .assignments
                    .iter()
                    .any(|a| a.expr.contains(&format!(".{field}")))
                    || func.field_reads.iter().any(|(_, f)| f == field))
                    && snapshot
                        .get(&(owner.clone(), field.clone()))
                        .is_some_and(|(c, _, _)| *c == Class::FixedPoint)
                {
                    bump = true;
                }
            }
            if bump {
                let cur = fn_class.get(name).copied().unwrap_or(Class::Exact);
                if cur < Class::FixedPoint {
                    fn_class.insert(name.clone(), Class::FixedPoint);
                    changed = true;
                }
            }
        }
        if propagate_fn_class(functions, &mut fn_class) {
            changed = true;
        }
        for (key, sites) in &writers {
            let (worst, citation, reason) =
                worst_writer(sites, functions, &fn_class, &snapshot, &contract_helpers);
            let new = (worst, citation, reason);
            if field_class.get(key) != Some(&new) {
                changed = true;
            }
            field_class.insert(key.clone(), new);
        }
        if !changed {
            break;
        }
    }

    let mut rows: Vec<FieldRow> = field_class
        .into_iter()
        .map(|((entity, field), (class, citation, reason))| {
            let declared = schema
                .entities
                .iter()
                .find(|e| e.name == entity)
                .and_then(|e| e.fields.iter().find(|f| f.name == field));
            FieldRow {
                entity,
                field,
                class,
                citation,
                reason,
                non_null: declared.is_some_and(|f| f.non_null),
                derived_from: declared.and_then(|f| {
                    f.derived_from
                        .clone()
                        .map(|back| (f.named_type.clone(), back))
                }),
            }
        })
        .collect();
    rows.sort_by(|a, b| a.entity.cmp(&b.entity).then(a.field.cmp(&b.field)));
    rows
}

fn propagate_fn_class(
    functions: &BTreeMap<String, FunctionInfo>,
    fn_class: &mut BTreeMap<String, Class>,
) -> bool {
    let mut any = false;
    for _ in 0..16 {
        let mut changed = false;
        for (name, func) in functions {
            let mut c = fn_class.get(name).copied().unwrap_or(Class::Exact);
            for callee in &func.calls {
                if let Some(cc) = fn_class.get(callee) {
                    c = c.max(*cc);
                }
            }
            if fn_class.get(name).copied() != Some(c) {
                fn_class.insert(name.clone(), c);
                changed = true;
                any = true;
            }
        }
        if !changed {
            break;
        }
    }
    any
}

fn worst_writer(
    sites: &[(String, Assignment)],
    functions: &BTreeMap<String, FunctionInfo>,
    fn_class: &BTreeMap<String, Class>,
    field_class: &BTreeMap<(String, String), (Class, Citation, String)>,
    helpers: &BTreeSet<String>,
) -> (Class, Citation, String) {
    let mut worst = Class::Exact;
    // A write in a handler over one in a helper: a getOrCreate helper's `= ZERO_INT` initialises the
    // field, and the handler's write is the value it holds.
    let first = sites
        .iter()
        .find(|(f, _)| {
            functions
                .get(f)
                .is_some_and(|f| matches!(f.kind, HandlerKind::Event | HandlerKind::Call))
        })
        .unwrap_or(&sites[0]);
    let mut citation = first.1.citation.clone();
    let mut reason = format!("assigned from `{}`", first.1.expr);
    for (fn_name, asg) in sites {
        let c = class_of_assignment(fn_name, asg, functions, fn_class, field_class, helpers);
        if c > worst {
            worst = c;
            // Cite the line that *decided* the class. For a call-derived field that is the
            // `bind`/`try_` in some helper, not the `token.symbol = fetchTokenSymbol(..)` that
            // called it: a reader following the assignment site finds no contract call there and
            // cannot check the claim (#1210). The assignment stays in the reason, so the row still
            // says where the field is written.
            citation = deciding_call_citation(fn_name, asg, functions, fn_class, helpers)
                .unwrap_or_else(|| asg.citation.clone());
            reason = reason_for(c, fn_name, asg, functions.get(fn_name));
            if citation != asg.citation {
                reason.push_str(&format!("; assigned at `{}`", asg.citation.display()));
            }
        }
    }
    (worst, citation, reason)
}

/// The contract-call site that makes this assignment call-derived, following the helper chain.
///
/// `None` when the assignment's own expression carries the call (its citation is already the right
/// line) or when nothing in the chain records one.
fn deciding_call_citation(
    fn_name: &str,
    asg: &Assignment,
    functions: &BTreeMap<String, FunctionInfo>,
    fn_class: &BTreeMap<String, Class>,
    helpers: &BTreeSet<String>,
) -> Option<Citation> {
    if expr_has_contract_call(&asg.expr) {
        return None;
    }
    let func = functions.get(fn_name)?;
    let expanded = expand_locals(&func.body, &asg.expr, LOCAL_DEPTH);
    if let Some(h) = helpers.iter().find(|h| reads_through_helper(&expanded, h)) {
        return functions.get(h).and_then(|h| h.contract_call.clone());
    }
    for callee in &func.calls {
        if !expr_calls(&asg.expr, callee) {
            continue;
        }
        if fn_class.get(callee) != Some(&Class::CallDerived) {
            continue;
        }
        if let Some(c) = contract_call_in_chain(callee, functions, &mut BTreeSet::new()) {
            return Some(c);
        }
    }
    None
}

/// First `contract_call` reachable from `name`, breadth of the call graph in name order so the
/// answer does not depend on iteration luck. `seen` stops a cycle.
fn contract_call_in_chain(
    name: &str,
    functions: &BTreeMap<String, FunctionInfo>,
    seen: &mut BTreeSet<String>,
) -> Option<Citation> {
    if !seen.insert(name.to_string()) {
        return None;
    }
    let func = functions.get(name)?;
    if let Some(c) = &func.contract_call {
        return Some(c.clone());
    }
    for callee in &func.calls {
        if let Some(c) = contract_call_in_chain(callee, functions, seen) {
            return Some(c);
        }
    }
    None
}

fn class_of_assignment(
    fn_name: &str,
    asg: &Assignment,
    functions: &BTreeMap<String, FunctionInfo>,
    fn_class: &BTreeMap<String, Class>,
    field_class: &BTreeMap<(String, String), (Class, Citation, String)>,
    helpers: &BTreeSet<String>,
) -> Class {
    let Some(func) = functions.get(fn_name) else {
        return Class::Exact;
    };
    if func.kind == HandlerKind::Block {
        return Class::Unreachable;
    }
    let mut c = Class::Exact;
    // `getPeeranhaNFT().getAchievementsNFTConfig(id)` is a contract read even though the bind lives in
    // the helper, and it usually arrives through a local, hence the expansion (S0 #1941).
    let expanded = expand_locals(&func.body, &asg.expr, LOCAL_DEPTH);
    if helpers.iter().any(|h| reads_through_helper(&expanded, h)) {
        c = Class::CallDerived;
    }
    // **A contract bound to a local counts too.** `expr_has_contract_call` only sees a bind, an
    // `ethereum.call` or a `try_` in the expression it is given, and the commonest shape puts the bind on
    // its own line - `let contract = ERC20.bind(id)` then `token.symbol = contract.symbol()`. That RHS
    // carried no marker and the field classified exact, promising byte-for-byte output for a contract
    // call with the call printed in the report's own Why column (#1296).
    if expr_reads_contract(&asg.expr, &bound_contract_locals(&func.body)) {
        c = Class::CallDerived;
    }
    for callee in &func.calls {
        if expr_calls(&asg.expr, callee) {
            if let Some(cc) = fn_class.get(callee) {
                // **`f(x).id` is `x`, whatever else `f` read.**
                //
                // The rule applied here is "does the callee read contract state". The rule that belongs
                // is "does *this field's value* depend on contract state", and for the id of an entity the
                // callee keyed by one of its own arguments the answer is no - the contract reads populate
                // sibling fields of that entity, not this one. Carbon's
                // `newTrade.sourceToken = findOrCreateToken(event.params.sourceToken, ..).id` is the case,
                // and `newTrade.trader` two lines above it - the identical shape through a helper that
                // happens to make no call - is the proof the old rule was accidental (#1294).
                if *cc == Class::CallDerived
                    && is_id_of_call(&asg.expr, callee)
                    && functions
                        .get(callee)
                        .is_some_and(|c| returned_id_is_contract_free(c, functions))
                {
                    continue;
                }
                c = c.max(*cc);
            }
        }
    }
    // Only this assignment's RHS. A mixed event handler's fn_class is not applied
    // to every write; an own-field fold next to a fixed-point copy stays exact.
    //
    // **Every class, not only `FixedPoint`.** This tested `== Class::FixedPoint`, so a read of a
    // *call-derived* field propagated nothing at all:
    //
    // ```ts
    // token0.decimals = fetchTokenDecimals(..)                                  // call-derived
    // swap.amount0 = convertTokenToDecimal(event.params.amount0, token0.decimals)  // was: exact
    // ```
    //
    // `exact` promises a pure function of decoded events, and that field needs a contract read. Four
    // such fields on Uniswap V4 name a call-derived field in their own expression, and a porter reading
    // `exact` has no reason to pass `--state-rpc` - after which every token amount is off by
    // `10^decimals`, which is a wrong number rather than a missing one (#1310).
    //
    // `max` keeps the ordering: a field reading both a call-derived and a fixed-point value takes the
    // stronger class, and a read of an *exact* field changes nothing.
    //
    // **Expanded through locals.** The dependency usually arrives by way of one:
    //
    // ```ts
    // let amount0    = convertTokenToDecimal(event.params.amount0, token0.decimals)
    // let amount0Abs = amount0.lt(ZERO_BD) ? amount0.times(MINUS_ONE) : amount0
    // pool.volumeToken0 = pool.volumeToken0.plus(amount0Abs)
    // ```
    //
    // The assignment names no classified field, so the direct test above sees nothing while the total
    // still cannot be computed without `decimals`. Ten more fields on Uniswap V4 (#1310).
    for (ent, field) in &func.field_reads {
        if expr_reads_field(&expanded, field) {
            if let Some((cl, _, _)) = field_class.get(&(ent.clone(), field.clone())) {
                c = c.max(*cl);
            }
        }
    }
    c
}

/// How many hops of local substitution to follow. Uniswap's deepest price chain is three
/// (`volumeUSD` <- `amountTotalUSDTracked` <- `getTrackedAmountUSD(..)` <- `derivedETH`); the bound is
/// generous and terminates rather than guessing.
const LOCAL_DEPTH: usize = 6;

/// `expr` with bare locals replaced by what the body assigned them, to `depth` hops.
///
/// Substitution rather than analysis: the only question asked of the result is whether it *reads* a
/// fixed-point field, and textual expansion answers that without a type system. Each local is expanded
/// once - `seen` is both the cycle guard and the bound on growth - so a local reused five times does not
/// multiply the string five times over.
///
/// Over-expanding is the safe direction here. If a local's contribution turns out not to matter, the
/// field is called fixed point when it is exact: an under-promise, which costs a porter a hand-check. The
/// reverse costs a promise of byte-for-byte output that cannot be kept.
fn expand_locals(body: &str, expr: &str, depth: usize) -> String {
    let mut out = expr.to_string();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for _ in 0..depth {
        let mut grew = false;
        for ident in bare_idents(&out) {
            if seen.contains(&ident) {
                continue;
            }
            let Some(rhs) = local_assignment(body, &ident) else {
                continue;
            };
            seen.insert(ident.clone());
            out = substitute_ident(&out, &ident, &format!("({rhs})"));
            grew = true;
        }
        if !grew {
            break;
        }
    }
    out
}

/// Every identifier in an expression, as candidates for local substitution.
///
/// Deliberately **not** filtered to non-field-access positions. That filter was here and it was
/// redundant: `substitute_ident` refuses to rewrite an identifier preceded by `.`, so a field name that
/// collides with a local is never substituted whatever this returns. A mutation removing the filter
/// survived, which is how the redundancy showed up - two guards for one property, only one of them
/// reachable. `a_field_name_colliding_with_a_local_is_not_expanded` holds the property and
/// `substitute_ident`'s boundary check is what enforces it.
fn bare_idents(expr: &str) -> Vec<String> {
    let b = expr.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < b.len() {
        if !(b[i].is_ascii_alphabetic() || b[i] == b'_') {
            i += 1;
            continue;
        }
        let start = i;
        while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
            i += 1;
        }
        out.push(expr[start..i].to_string());
    }
    out.sort();
    out.dedup();
    out
}

/// What `let <name> = ..` assigned, as written, or `None` if the body declares no such local.
fn local_assignment(body: &str, name: &str) -> Option<String> {
    // Joined, so a declaration a formatter wrapped is read whole. Reading `let key =` on its own recorded
    // an **empty** right-hand side, which then looked contract-free (Jules, #1298).
    for line in join_wrapped_declarations(body) {
        let line = line.as_str();
        let t = line.trim_start();
        // `continue`, not `?`. An earlier version returned from the whole function here, so it only
        // ever resolved a local declared on the body's first line - which made the expansion a no-op
        // and cost a wrong diagnosis on #1309 before it was noticed.
        let Some(rest) = t.strip_prefix("let ").or_else(|| t.strip_prefix("const ")) else {
            continue;
        };
        let Some((lhs, rhs)) = rest.split_once('=') else {
            continue;
        };
        if lhs.split(':').next().unwrap_or(lhs).trim() != name {
            continue;
        }
        let rhs = collapse_ws(rhs.trim().trim_end_matches(';'));
        if rhs.is_empty() {
            return None;
        }
        return Some(rhs);
    }
    None
}

/// Replace whole-identifier occurrences of `name`, leaving `xname` and `.name` alone.
fn substitute_ident(expr: &str, name: &str, with: &str) -> String {
    let b = expr.as_bytes();
    let mut out = String::with_capacity(expr.len());
    let mut i = 0usize;
    while i < expr.len() {
        if expr[i..].starts_with(name) {
            let before_ok = i == 0
                || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_' || b[i - 1] == b'.');
            let after = i + name.len();
            let after_ok =
                after == b.len() || !(b[after].is_ascii_alphanumeric() || b[after] == b'_');
            if before_ok && after_ok {
                out.push_str(with);
                i = after;
                continue;
            }
        }
        let ch = expr[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

fn expr_calls(expr: &str, name: &str) -> bool {
    let needle = format!("{name}(");
    if let Some(at) = expr.find(&needle) {
        if at > 0 {
            let p = expr.as_bytes()[at - 1];
            if p.is_ascii_alphanumeric() || p == b'_' || p == b'.' {
                return false;
            }
        }
        return true;
    }
    false
}

fn expr_reads_field(expr: &str, field: &str) -> bool {
    let needle = format!(".{field}");
    let bytes = expr.as_bytes();
    let mut from = 0;
    while let Some(rel) = expr[from..].find(&needle) {
        let at = from + rel;
        let after = at + needle.len();
        let boundary =
            after == bytes.len() || !(bytes[after].is_ascii_alphanumeric() || bytes[after] == b'_');
        if boundary {
            let mut n = after;
            while n < bytes.len() && bytes[n].is_ascii_whitespace() {
                n += 1;
            }
            let is_call = n < bytes.len() && bytes[n] == b'(';
            let is_assign = n < bytes.len()
                && bytes[n] == b'='
                && !(n + 1 < bytes.len() && bytes[n + 1] == b'=');
            if !is_call && !is_assign {
                return true;
            }
        }
        from = at + 1;
    }
    false
}

/// Does this expression read a contract, directly or through a local bound earlier in the body?
fn expr_reads_contract(expr: &str, bound: &BTreeSet<String>) -> bool {
    expr_has_contract_call(expr) || bound.iter().any(|v| calls_method_on(expr, v))
}

/// Is `v.<method>(` present in `expr`, with `v` a whole identifier?
///
/// A method call, not a mention: `contract.symbol()` is a read, `contract` passed as an argument is not,
/// and `myContract.x()` must not match a bound local called `contract`.
fn calls_method_on(expr: &str, v: &str) -> bool {
    let b = expr.as_bytes();
    let mut from = 0usize;
    while let Some(rel) = expr[from..].find(v) {
        let at = from + rel;
        from = at + v.len();
        let before_ok = at == 0
            || !(b[at - 1].is_ascii_alphanumeric() || b[at - 1] == b'_' || b[at - 1] == b'.');
        if !before_ok || !expr[from..].starts_with('.') {
            continue;
        }
        let after = &expr[from + 1..];
        let method: String = after
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        if !method.is_empty() && after[method.len()..].starts_with('(') {
            return true;
        }
    }
    false
}

/// Locals bound to a contract in this body: `let c = ERC20Contract.bind(addr)`.
///
/// `expr_has_contract_call` only sees a bind, an `ethereum.call` or a `try_` **in the expression it is
/// given**. The commonest shape puts the bind on its own line and the read on another, so
/// `token.symbol = contract.symbol()` contained no marker and classified exact - promising byte-for-byte
/// output for a contract call, with the call printed in the report's own Why column (#1296). Every
/// Uniswap fixture this was built against wrote `contract.try_symbol()` inline, so no test could see it.
fn bound_contract_locals(body: &str) -> BTreeSet<String> {
    let declared = |t: &str| {
        let rest = t
            .trim_start()
            .strip_prefix("let ")
            .or_else(|| t.trim_start().strip_prefix("const "))?;
        let (name, rhs) = rest.split_once('=')?;
        // `let c: ERC20Contract = ..` carries a type annotation.
        let name = name.split(':').next().unwrap_or(name).trim();
        (!name.is_empty() && name.chars().all(|c| c.is_alphanumeric() || c == '_')).then(|| {
            (
                name.to_string(),
                // **No comment stripping here.** `parse_functions` strips the whole source before any
                // `FunctionInfo` exists, so a body never carries one and a second stripper on this
                // right-hand side is a guard that cannot fire - the mutation removing it stayed green,
                // which is how I found out. `a_body_carries_no_comments` holds the property this relies
                // on (Jules, #1298).
                rhs.trim().trim_end_matches(';').trim().to_string(),
            )
        })
    };
    // **Statements, not lines.** A declaration that wraps - `let contract =\n  ERC20.bind(id)` - put the
    // bind on a line with no `let`, so the local went unrecorded and a later `contract.symbol()` read as
    // an ordinary method call (Jules, #1298). Prettier wraps a long bind exactly that way.
    let joined = join_wrapped_declarations(body);
    let mut out = BTreeSet::new();
    for line in &joined {
        if let Some((name, rhs)) = declared(line) {
            if rhs.contains(".bind(") {
                out.insert(name);
            }
        }
    }
    // **An alias of a bound contract is a bound contract.** `let c = ERC20.bind(id); let alias = c` left
    // `alias` unrecorded, so `alias.symbol()` read as an ordinary method call and the field stayed exact
    // (Jules, #1298).
    //
    // One forward pass, not a fixed point: a local cannot be used before it is declared, so source order
    // is dependency order and a chain of copies is already in it. A loop here was unfalsifiable - the
    // mutation reducing it to one pass stayed green, because no valid mapping can need the second.
    for line in &joined {
        if let Some((name, rhs)) = declared(line) {
            if out.contains(&rhs) {
                out.insert(name);
            }
        }
    }
    out
}

/// The body's lines, with a wrapped declaration joined onto one.
///
/// A line whose trimmed form ends with `=`, `(` or `,` is continued on the next: those are the three ways
/// a formatter breaks a declaration, and joining them is what lets a line-shaped rule see the whole
/// statement. Nothing else is merged, so a line-oriented caller still sees ordinary statements one apiece.
fn join_wrapped_declarations(body: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut pending = String::new();
    for line in body.lines() {
        let t = line.trim();
        if !pending.is_empty() {
            pending.push(' ');
            pending.push_str(t);
        } else {
            pending = t.to_string();
        }
        if pending.ends_with('=') || pending.ends_with('(') || pending.ends_with(',') {
            continue;
        }
        out.push(std::mem::take(&mut pending));
    }
    if !pending.is_empty() {
        out.push(pending);
    }
    out
}

/// Is the assignment exactly `callee(..).id`?
///
/// The call has to be the whole receiver and `.id` the whole of what is taken off it. Anything further
/// is left alone (`f(x).id.toHex()`, `f(x).symbol`, a call nested inside a larger expression), because
/// then the value is no longer simply the entity's id.
fn is_id_of_call(expr: &str, callee: &str) -> bool {
    let e = collapse_ws(expr);
    let e = e.trim();
    // **The call's own closing paren, not the end of the string.** A prefix-and-suffix test accepted
    // `findToken(id).id.owner.id`, whose value is not the returned entity's id at all, and the exemption
    // was granted on the strength of the two ends matching (Jules, #1298). Stripping `callee` as a whole
    // identifier also stops `findTokenOrCreate(..)` reading as `findToken`.
    if !e.starts_with(callee) {
        return false;
    }
    // No separate "is the next character a paren" check: `match_paren` refuses unless the byte at `open`
    // is one, so `findTokenOrCreate(..)` against `findToken` fails there. The mutation removing such a
    // check stayed green, which is how it got noticed.
    let open = callee.len();
    let Some(close) = match_paren(e, open) else {
        return false;
    };
    &e[close + 1..] == ".id"
}

/// Is the id of every entity this function can return demonstrably free of a contract read?
///
/// Tied to the **returned** entity, and unresolvable is a refusal rather than a pass. A body-wide scan
/// let an unrelated `new Audit(id)` vouch for a returned `new Token(contract.canonical(id))`, exempting a
/// field whose value does come off a contract (Jules on #1298) - the over-promising direction, which
/// #1296 was also about.
///
/// A constant id is as exempt as a parameter one: neither depends on contract state, which is the only
/// question the class turns on. An earlier version also required at least one id to be a bare parameter,
/// which added nothing the class needed and would have rejected a perfectly reproducible constant.
fn returned_id_is_contract_free(
    callee: &FunctionInfo,
    functions: &BTreeMap<String, FunctionInfo>,
) -> bool {
    match returned_entity_id_exprs(callee, functions, RETURN_DEPTH) {
        Some(ids) if !ids.is_empty() => {
            let bound = bound_contract_locals(&callee.body);
            !ids.iter().any(|e| expr_reads_contract(e, &bound))
        }
        _ => false,
    }
}

/// The id expression of every entity this function can return, or `None` if any return path cannot be
/// resolved.
fn returned_entity_id_exprs(
    callee: &FunctionInfo,
    functions: &BTreeMap<String, FunctionInfo>,
    depth: usize,
) -> Option<Vec<String>> {
    if depth == 0 {
        return None;
    }
    let mut out = Vec::new();
    for r in return_exprs(&callee.body) {
        let e = collapse_ws(&r);
        let e = e.trim();
        if let Some(id) = constructed_or_loaded_id(e) {
            out.push(resolve_local_chain(&callee.body, &callee.param_names, &id)?);
            continue;
        }
        // `return <local>`: whatever that local was constructed or loaded with.
        if !e.is_empty() && e.chars().all(|c| c.is_alphanumeric() || c == '_') {
            let id = constructor_arg_for(&callee.body, e)
                .or_else(|| load_arg_for(&callee.body, e))
                .or_else(|| create_arg_for(&callee.body, e))?;
            out.push(resolve_local_chain(&callee.body, &callee.param_names, &id)?);
            continue;
        }
        // `return <helper>(..)`: the helper's own return paths.
        let name: String = e
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        if !name.is_empty() && e[name.len()..].starts_with('(') {
            let inner = functions.get(&name)?;
            // **The helper's return, in the caller's terms.** Recursing and taking the inner expressions
            // verbatim loses the provenance this whole function exists to track: `makeToken(key) { return
            // new Token(key) }` called as `makeToken(contract.canonical(id))` yielded `key`, which the
            // caller then checked against *its* locals, found contract-free, and granted the `.id`
            // exemption to a value that came straight off a contract call (Jules, #1298).
            let args = take_paren_list(e, name.len());
            // Arity must match, or a parameter survives unsubstituted and resolves against whatever the
            // caller happens to have bound under that name.
            if args.len() != inner.param_names.len() {
                return None;
            }
            // **Checked in the context it was written.** `returned_id_is_contract_free` tests the
            // expressions against the *outer* function's bound locals, so
            // `makeToken(id) { let c = ERC20.bind(id); return new Token(c.canonical(id)) }` handed back
            // `c.canonical(id)` - a name the outer body never binds - and it read as an ordinary method
            // call on nothing in particular (Jules, #1298). The inner body is where `c` means a contract,
            // so that is where the question gets asked.
            let inner_bound = bound_contract_locals(&inner.body);
            for expr in returned_entity_id_exprs(inner, functions, depth - 1)? {
                if expr_reads_contract(&expr, &inner_bound) {
                    return None;
                }
                let mut in_callers_terms = expr;
                for (param, arg) in inner.param_names.iter().zip(&args) {
                    in_callers_terms = substitute_ident(&in_callers_terms, param, arg.trim());
                }
                out.push(in_callers_terms);
            }
            continue;
        }
        return None;
    }
    (!out.is_empty()).then_some(out)
}

/// `new Entity(<id>)` or `Entity.load(<id>)` at the head of an expression, giving `<id>`.
fn constructed_or_loaded_id(e: &str) -> Option<String> {
    if let Some(rest) = e.strip_prefix("new ") {
        let mut k = 0usize;
        let rest = rest.trim_start();
        let ident: String = rest
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        k += ident.len();
        if !ident.is_empty() && rest[k..].starts_with('(') {
            return Some(collapse_ws(&take_expr(rest, k + 1)));
        }
        return None;
    }
    let ident: String = e
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    if !ident.is_empty() && e[ident.len()..].starts_with(".load(") {
        return Some(collapse_ws(&take_expr(e, ident.len() + ".load(".len())));
    }
    None
}

/// Follow a bare local to the expression it ultimately came from, or `None` if it cannot be followed.
///
/// **Transitive, and `None` is a refusal.** This resolved one step, so
/// `let key = contract.canonical(id); let finalKey = key; return new Token(finalKey)` resolved `finalKey`
/// to `key`, stopped, and `key` looked contract-free on its face - granting the `.id` exemption to an id
/// that did come off a contract (Jules on #1298). A chain longer than `ALIAS_DEPTH`, a cycle, or an
/// identifier that is neither a local nor a parameter all return `None`, and the caller treats that as
/// unresolved rather than as safe.
fn resolve_local_chain(body: &str, params: &[String], expr: &str) -> Option<String> {
    let mut e = expr.trim().to_string();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for _ in 0..ALIAS_DEPTH {
        if e.is_empty() || !e.chars().all(|c| c.is_alphanumeric() || c == '_') {
            // No longer a bare local: this is the expression the id came from.
            return Some(e);
        }
        if !seen.insert(e.clone()) {
            // `let a = b; let b = a;` - terminate rather than spin, and refuse.
            return None;
        }
        let mut next: Option<String> = None;
        // The same joined representation as `bound_contract_locals` and `local_assignment`: one reading of
        // what a declaration is, or the three disagree on a wrapped one.
        for line in join_wrapped_declarations(body) {
            let line = line.as_str();
            let t = line.trim_start();
            let Some(rest) = t.strip_prefix("let ").or_else(|| t.strip_prefix("const ")) else {
                continue;
            };
            let Some((name, rhs)) = rest.split_once('=') else {
                continue;
            };
            if name.split(':').next().unwrap_or(name).trim() == e {
                next = Some(collapse_ws(rhs.trim().trim_end_matches(';')));
                break;
            }
        }
        match next {
            Some(n) => e = n,
            // Not declared as a local. A **parameter** is a legitimate terminus: `Token.load(id)` with
            // `id` a parameter means the id *is* the caller's argument, which is the exempt case and the
            // whole point of the rule. Anything else - an import, a module constant we cannot see - is
            // unresolved, and the caller must not assume it is contract-free.
            None if params.iter().any(|p| p == &e) => return Some(e),
            None => return None,
        }
    }
    // Deeper than the bound. Unresolved, which refuses rather than guesses.
    None
}

/// How far to follow `return <helper>(..)` before giving up. A chain deeper than this is unresolvable,
/// which refuses the exemption rather than guessing at it.
const RETURN_DEPTH: usize = 4;

fn expr_has_contract_call(expr: &str) -> bool {
    expr.contains(".bind(") || expr.contains("ethereum.call(") || expr.contains(".try_")
}

/// Functions that hand back a bound contract: `return PeeranhaNFT.bind(..)`, or a local bound to one.
fn contract_returning_helpers(functions: &BTreeMap<String, FunctionInfo>) -> BTreeSet<String> {
    functions
        .iter()
        .filter(|(_, f)| {
            let bound = bound_contract_locals(&f.body);
            return_exprs(&f.body).iter().any(|r| {
                let r = r.trim();
                r.contains(".bind(") || bound.contains(r)
            })
        })
        .map(|(n, _)| n.clone())
        .collect()
}

/// Is a method called on what `helper(..)` returns? `(getNFT()).config(id)` counts, which is how a
/// local holding the helper's result reads once expanded.
fn reads_through_helper(expr: &str, helper: &str) -> bool {
    let b = expr.as_bytes();
    let mut from = 0;
    while let Some(rel) = expr[from..].find(helper) {
        let at = from + rel;
        from = at + helper.len();
        if at > 0 && (b[at - 1].is_ascii_alphanumeric() || b[at - 1] == b'_' || b[at - 1] == b'.') {
            continue;
        }
        let Some(close) = match_paren(expr, from) else {
            continue;
        };
        let rest = expr[close + 1..].trim_start_matches(|c: char| c == ')' || c.is_whitespace());
        let Some(after) = rest.strip_prefix('.') else {
            continue;
        };
        let method: String = after
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        if !method.is_empty() && after[method.len()..].trim_start().starts_with('(') {
            return true;
        }
    }
    false
}

fn unique_field_owners(schema: &Schema) -> BTreeMap<String, String> {
    let mut counts: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for e in &schema.entities {
        for f in &e.fields {
            counts
                .entry(f.name.clone())
                .or_default()
                .push(e.name.clone());
        }
    }
    counts
        .into_iter()
        .filter_map(|(f, ents)| {
            if ents.len() == 1 {
                Some((f, ents[0].clone()))
            } else {
                None
            }
        })
        .collect()
}

fn reason_for(
    class: Class,
    fn_name: &str,
    asg: &Assignment,
    func: Option<&FunctionInfo>,
) -> String {
    match class {
        Class::Unreachable if func.is_some_and(|f| f.kind == HandlerKind::Block) => {
            format!("written from blockHandler `{fn_name}`; nuthatch indexes logs")
        }
        Class::CallDerived => {
            if let Some(c) = func.and_then(|f| f.contract_call.as_ref()) {
                format!("`{fn_name}` reads contract state (`{}`)", c.display())
            } else {
                format!(
                    "`{fn_name}` reads contract state; assigned from `{}`",
                    asg.expr
                )
            }
        }
        Class::FixedPoint => {
            format!(
                "`{fn_name}` reads stored entity output (`{}`); a nest can converge, this will not reproduce",
                asg.expr
            )
        }
        Class::Exact => format!("assigned from `{}`", asg.expr),
        Class::Unreachable => format!("assigned in `{fn_name}` from `{}`", asg.expr),
    }
}

// ── Mapping calls (RFC-0044 S2) ─────────────────────────────────────────────

/// Contract reads that feed Call-derived fields. Each entry cites the `.try_*` / `.bind` line,
/// not a guessed getter. Fields the classifier did not call Call-derived are skipped, so dropping
/// the read from the mapping drops the entry.
pub fn mapping_calls(dir: &Path) -> Result<Vec<MappingCall>> {
    let report = classify_dir(dir)?;
    let mappings = load_mappings(dir)?;
    Ok(calls_from_mappings(&report, &mappings))
}

pub(crate) fn calls_from_mappings(report: &Report, mappings: &Mappings) -> Vec<MappingCall> {
    let call_fields: BTreeSet<(String, String)> = report
        .fields
        .iter()
        .filter(|f| f.class == Class::CallDerived)
        .map(|f| (f.entity.clone(), f.field.clone()))
        .collect();
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for func in mappings.functions.values() {
        if func.kind == HandlerKind::Block {
            continue;
        }
        for asg in &func.assignments {
            if !call_fields.contains(&(asg.entity.clone(), asg.field.clone())) {
                continue;
            }
            if let Some(call) = derive_mapping_call(func, asg, mappings) {
                let key = (
                    call.entity.clone(),
                    call.field.clone(),
                    call.signature.clone(),
                    call.contract_arg.clone(),
                    call.args.clone(),
                );
                if seen.insert(key) {
                    out.push(call);
                }
            }
        }
    }
    out.sort_by(|a, b| {
        a.entity
            .cmp(&b.entity)
            .then(a.field.cmp(&b.field))
            .then(a.contract_arg.cmp(&b.contract_arg))
            .then(a.signature.cmp(&b.signature))
    });
    out
}

fn derive_mapping_call(
    writer: &FunctionInfo,
    asg: &Assignment,
    mappings: &Mappings,
) -> Option<MappingCall> {
    if let Some(call) = call_from_body(writer, asg, None, &asg.expr) {
        return Some(call);
    }
    let mut seen = BTreeSet::new();
    seen.insert(writer.name.clone());
    call_via_helpers(writer, asg, mappings, writer, &asg.expr, &mut seen)
}

/// A wrapper that only forwards (`fetchTokenSymbol` → `readTokenSymbol` → `try_symbol`)
/// still has to emit the [[calls]] stanza; otherwise classify and emit disagree.
fn call_via_helpers(
    func: &FunctionInfo,
    asg: &Assignment,
    mappings: &Mappings,
    writer: &FunctionInfo,
    expr: &str,
    seen: &mut BTreeSet<String>,
) -> Option<MappingCall> {
    let at_writer = func.name == writer.name;
    for callee in &func.calls {
        if at_writer && !expr_calls(expr, callee) {
            continue;
        }
        if !seen.insert(callee.clone()) {
            continue;
        }
        let Some(helper) = mappings.functions.get(callee) else {
            continue;
        };
        let invoke = if at_writer {
            expr.to_string()
        } else {
            return_exprs(&func.body)
                .into_iter()
                .find(|r| expr_calls(r, callee))
                .unwrap_or_else(|| func.body.clone())
        };
        let caller_args = call_arg_list(&invoke, callee);
        let found = if helper.contract_call.is_some() || helper.body.contains(".try_") {
            call_from_body(helper, asg, Some(writer), &asg.expr)
        } else {
            call_via_helpers(helper, asg, mappings, writer, &invoke, seen)
        };
        if let Some(call) = found {
            let contract_arg = subst_params(&call.contract_arg, &helper.param_names, &caller_args);
            let args = call
                .args
                .iter()
                .map(|a| subst_params(a, &helper.param_names, &caller_args))
                .collect();
            return Some(MappingCall {
                handler: writer.name.clone(),
                contract_arg,
                args,
                ..call
            });
        }
    }
    None
}

fn call_from_body(
    func: &FunctionInfo,
    asg: &Assignment,
    writer: Option<&FunctionInfo>,
    expr: &str,
) -> Option<MappingCall> {
    let sites = find_try_sites(&func.body, &func.file, func.body_start_line);
    let use_returns = writer.is_some() || func.kind == HandlerKind::Helper;
    let site = pick_try_site(&sites, &func.body, expr, use_returns)?;
    let bind_arg = site
        .bind_arg
        .clone()
        .or_else(|| bind_arg_for(&func.body, &site.receiver))?;
    let sig_types: Vec<&str> = site
        .args
        .iter()
        .map(|a| sol_type_of_expr(a, func))
        .collect::<Option<Vec<_>>>()?;
    let signature = if sig_types.is_empty() {
        format!("{}()", site.method)
    } else {
        format!("{}({})", site.method, sig_types.join(","))
    };
    Some(MappingCall {
        entity: asg.entity.clone(),
        field: asg.field.clone(),
        handler: writer.unwrap_or(func).name.clone(),
        signature,
        contract_arg: bind_arg,
        args: site.args.clone(),
        citation: site.citation.clone(),
    })
}

/// The `.try_*` that actually feeds `expr` (the assignment) or, in a helper, the returned value.
/// A helper that calls `try_symbol` and then returns `try_decimals` must not emit `symbol()` for
/// the decimals field.
fn pick_try_site<'a>(
    sites: &'a [TrySite],
    body: &str,
    expr: &str,
    use_returns: bool,
) -> Option<&'a TrySite> {
    if sites.is_empty() {
        return None;
    }
    let locals = locals_bound_to_try(body, sites);
    if let Some(site) = site_for_text(expr, sites, &locals) {
        return Some(site);
    }
    if use_returns {
        for ret in return_exprs(body) {
            if let Some(site) = site_for_text(&ret, sites, &locals) {
                return Some(site);
            }
        }
        if sites.len() == 1 {
            return sites.first();
        }
    }
    None
}

fn site_for_text<'a>(
    text: &str,
    sites: &'a [TrySite],
    locals: &BTreeMap<String, &'a TrySite>,
) -> Option<&'a TrySite> {
    for site in sites {
        if ident_in(text, &format!("try_{}", site.method)) {
            return Some(site);
        }
    }
    for (var, site) in locals {
        if uses_local(text, var) {
            return Some(site);
        }
    }
    None
}

fn locals_bound_to_try<'a>(body: &str, sites: &'a [TrySite]) -> BTreeMap<String, &'a TrySite> {
    let mut map = BTreeMap::new();
    let mut i = 0;
    while i < body.len() {
        if let Some((var, rhs, next)) = match_let_rhs(body, i) {
            if let Some(site) = sites
                .iter()
                .find(|s| ident_in(&rhs, &format!("try_{}", s.method)))
            {
                map.insert(var, site);
            }
            i = next.max(i + 1);
            continue;
        }
        i += 1;
    }
    map
}

fn match_let_rhs(text: &str, i: usize) -> Option<(String, String, usize)> {
    if !text.is_char_boundary(i) {
        return None;
    }
    let rest = &text[i..];
    let kw = if rest.starts_with("let ") {
        4
    } else if rest.starts_with("const ") {
        6
    } else if rest.starts_with("var ") {
        4
    } else {
        return None;
    };
    if i > 0 {
        let p = text.as_bytes()[i - 1];
        if p.is_ascii_alphanumeric() || p == b'_' {
            return None;
        }
    }
    let mut k = i + kw;
    let var = take_ident_str(text, &mut k)?;
    skip_ws_str(text, &mut k);
    if k >= text.len() || text.as_bytes()[k] != b'=' {
        return None;
    }
    if k + 1 < text.len() {
        let n = text.as_bytes()[k + 1];
        if n == b'=' || n == b'>' {
            return None;
        }
    }
    let rhs = take_expr(text, k + 1);
    Some((var, collapse_ws(&rhs), k + 1))
}

fn return_exprs(body: &str) -> Vec<String> {
    let mut out = Vec::new();
    let bytes = body.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if body[i..].starts_with("return") {
            let before_ok =
                i == 0 || !(bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_');
            let after = i + 6;
            let after_ok = after >= bytes.len()
                || !(bytes[after].is_ascii_alphanumeric() || bytes[after] == b'_');
            if before_ok && after_ok {
                let expr = take_expr(body, after);
                if !expr.is_empty() {
                    out.push(collapse_ws(&expr));
                }
            }
        }
        i += 1;
    }
    out
}

fn ident_in(text: &str, ident: &str) -> bool {
    let bytes = text.as_bytes();
    let needle = ident.as_bytes();
    if needle.is_empty() || needle.len() > bytes.len() {
        return false;
    }
    let mut i = 0;
    while i + needle.len() <= bytes.len() {
        if &bytes[i..i + needle.len()] == needle {
            let before_ok = i == 0 || {
                let p = bytes[i - 1];
                !(p.is_ascii_alphanumeric() || p == b'_')
            };
            let after_ok = i + needle.len() == bytes.len() || {
                let n = bytes[i + needle.len()];
                !(n.is_ascii_alphanumeric() || n == b'_')
            };
            if before_ok && after_ok {
                return true;
            }
        }
        i += 1;
    }
    false
}

fn uses_local(text: &str, var: &str) -> bool {
    let bytes = text.as_bytes();
    let needle = var.as_bytes();
    if needle.is_empty() || needle.len() > bytes.len() {
        return false;
    }
    let mut i = 0;
    while i + needle.len() <= bytes.len() {
        if &bytes[i..i + needle.len()] == needle {
            let before_ok = i == 0 || {
                let p = bytes[i - 1];
                !(p.is_ascii_alphanumeric() || p == b'_' || p == b'.')
            };
            let after_ok = i + needle.len() == bytes.len() || {
                let n = bytes[i + needle.len()];
                !(n.is_ascii_alphanumeric() || n == b'_')
            };
            if before_ok && after_ok {
                return true;
            }
        }
        i += 1;
    }
    false
}

#[derive(Debug, Clone)]
struct TrySite {
    method: String,
    receiver: String,
    bind_arg: Option<String>,
    args: Vec<String>,
    citation: Citation,
}

fn find_try_sites(body: &str, file: &str, body_start_line: usize) -> Vec<TrySite> {
    let mut out = Vec::new();
    let bytes = body.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let rest = &body[i..];
        let Some(at) = rest.find(".try_") else {
            break;
        };
        let dot = i + at;
        let method_start = dot + 5;
        let mut k = method_start;
        while k < bytes.len() && (bytes[k].is_ascii_alphanumeric() || bytes[k] == b'_') {
            k += 1;
        }
        if k == method_start {
            i = method_start;
            continue;
        }
        let method = body[method_start..k].to_string();
        let mut j = k;
        skip_ws_str(body, &mut j);
        if j >= bytes.len() || bytes[j] != b'(' {
            i = k;
            continue;
        }
        let args_raw = take_paren_list(body, j);
        let receiver = ident_before(body, dot).unwrap_or_default();
        let bind_arg = chained_bind_arg(body, dot);
        let line = body_start_line + line_of(body, dot) - 1;
        out.push(TrySite {
            method,
            receiver,
            bind_arg,
            args: args_raw,
            citation: Citation {
                file: file.to_string(),
                line,
            },
        });
        i = k;
    }
    out
}

fn chained_bind_arg(body: &str, try_dot: usize) -> Option<String> {
    // `ERC20.bind(addr).try_symbol()` - the bind sits immediately before this `.try_`.
    let before = &body[..try_dot];
    let bind_at = before.rfind(".bind(")?;
    let close = match_paren(body, bind_at + 5)?;
    if close >= try_dot {
        return None;
    }
    if !body[close + 1..try_dot].trim().is_empty() {
        return None;
    }
    let inner = body[bind_at + 6..close].trim();
    if inner.is_empty() {
        return None;
    }
    Some(collapse_ws(inner))
}

fn bind_arg_for(body: &str, receiver: &str) -> Option<String> {
    if receiver.is_empty() {
        return first_bind_arg(body);
    }
    // `let contract = ERC20.bind(tokenAddress)`
    let mut i = 0;
    while i < body.len() {
        if !body.is_char_boundary(i) {
            i += 1;
            continue;
        }
        if let Some((var, arg, next)) = match_bind_assign(body, i) {
            if var == receiver {
                return Some(arg);
            }
            i = next;
            continue;
        }
        i += 1;
    }
    first_bind_arg(body)
}

fn first_bind_arg(body: &str) -> Option<String> {
    let at = body.find(".bind(")?;
    let close = match_paren(body, at + 5)?;
    let inner = body[at + 6..close].trim();
    if inner.is_empty() {
        None
    } else {
        Some(collapse_ws(inner))
    }
}

fn match_bind_assign(text: &str, i: usize) -> Option<(String, String, usize)> {
    if !text.is_char_boundary(i) {
        return None;
    }
    let rest = &text[i..];
    let kw = if rest.starts_with("let ") {
        4
    } else if rest.starts_with("const ") {
        6
    } else if rest.starts_with("var ") {
        4
    } else {
        return None;
    };
    if i > 0 {
        let p = text.as_bytes()[i - 1];
        if p.is_ascii_alphanumeric() || p == b'_' {
            return None;
        }
    }
    let mut k = i + kw;
    let var = take_ident_str(text, &mut k)?;
    skip_ws_str(text, &mut k);
    if !text[k..].starts_with('=') {
        return None;
    }
    k += 1;
    skip_ws_str(text, &mut k);
    let _ty = take_ident_str(text, &mut k)?;
    skip_ws_str(text, &mut k);
    if !text[k..].starts_with(".bind(") {
        return None;
    }
    let open = k + 5;
    let close = match_paren(text, open)?;
    let arg = collapse_ws(text[open + 1..close].trim());
    Some((var, arg, close + 1))
}

fn match_paren(text: &str, open: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    if open >= bytes.len() || bytes[open] != b'(' {
        return None;
    }
    let mut depth = 0;
    let mut i = open;
    let mut in_str: Option<u8> = None;
    while i < bytes.len() {
        let b = bytes[i];
        if let Some(q) = in_str {
            if b == b'\\' {
                i += 2;
                continue;
            }
            if b == q {
                in_str = None;
            }
            i += 1;
            continue;
        }
        if b == b'\'' || b == b'"' || b == b'`' {
            in_str = Some(b);
            i += 1;
            continue;
        }
        if b == b'(' {
            depth += 1;
        } else if b == b')' {
            depth -= 1;
            if depth == 0 {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

fn take_paren_list(text: &str, open: usize) -> Vec<String> {
    let Some(close) = match_paren(text, open) else {
        return Vec::new();
    };
    let inner = text[open + 1..close].trim();
    if inner.is_empty() {
        return Vec::new();
    }
    split_args(inner)
}

fn split_args(inner: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut depth = 0i32;
    let mut in_str: Option<u8> = None;
    let bytes = inner.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if let Some(q) = in_str {
            if b == b'\\' {
                i += 2;
                continue;
            }
            if b == q {
                in_str = None;
            }
            i += 1;
            continue;
        }
        if b == b'\'' || b == b'"' || b == b'`' {
            in_str = Some(b);
            i += 1;
            continue;
        }
        if b == b'(' || b == b'[' {
            depth += 1;
        } else if b == b')' || b == b']' {
            depth -= 1;
        } else if b == b',' && depth == 0 {
            let part = inner[start..i].trim();
            if !part.is_empty() {
                out.push(collapse_ws(part));
            }
            start = i + 1;
        }
        i += 1;
    }
    let part = inner[start..].trim();
    if !part.is_empty() {
        out.push(collapse_ws(part));
    }
    out
}

fn ident_before(text: &str, dot: usize) -> Option<String> {
    let bytes = text.as_bytes();
    let mut i = dot;
    while i > 0 && bytes[i - 1].is_ascii_whitespace() {
        i -= 1;
    }
    if i == 0 {
        return None;
    }
    let end = i;
    while i > 0 && (bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_') {
        i -= 1;
    }
    if i == end {
        return None;
    }
    Some(text[i..end].to_string())
}

fn call_arg_list(expr: &str, name: &str) -> Vec<String> {
    let needle = format!("{name}(");
    let Some(at) = expr.find(&needle) else {
        return Vec::new();
    };
    if at > 0 {
        let p = expr.as_bytes()[at - 1];
        if p.is_ascii_alphanumeric() || p == b'_' || p == b'.' {
            return Vec::new();
        }
    }
    take_paren_list(expr, at + name.len())
}

fn subst_params(expr: &str, params: &[String], args: &[String]) -> String {
    let ident = strip_converters(expr);
    if let Some(idx) = params.iter().position(|p| p == &ident) {
        if let Some(arg) = args.get(idx) {
            return strip_converters(arg);
        }
    }
    strip_converters(expr)
}

/// Drop `.toHex()` / `.toHexString()` / `.toString()` so `event.params.token0.toHex()` is still
/// the `token0` column.
pub(crate) fn strip_converters(expr: &str) -> String {
    let mut s = collapse_ws(expr);
    loop {
        let next = s
            .strip_suffix(".toHex()")
            .or_else(|| s.strip_suffix(".toHexString()"))
            .or_else(|| s.strip_suffix(".toString()"))
            .or_else(|| s.strip_suffix(".toI32()"))
            .or_else(|| s.strip_suffix(".toU32()"));
        match next {
            Some(n) => s = n.trim().to_string(),
            None => break,
        }
    }
    s
}

/// Event column that identifies `entity` in this function: an `id` assignment, else
/// `new Entity(event.params.x)` / `Entity.load(event.params.x)`.
/// The **literal** id of a singleton entity, as a SQL string literal, if the mapping constructs or loads
/// it with a constant.
///
/// `new PoolManager('1')` and `Bundle.load('1')` are how a subgraph writes a protocol-wide singleton -
/// one row, one known id. `entity_id_event_column` looks for a decoded column and finds none, so every
/// accumulator on such an entity was reported as *"nothing in the mapping identifies which `PoolManager` a
/// row belongs to, so there is no key to group by"* - which is true and the wrong conclusion: the key is
/// not in the data because it is in the mapping (#1313).
///
/// Returned already quoted and escaped, because it goes straight into a `SELECT` list.
pub(crate) fn entity_id_literal(entity: &str, func: &FunctionInfo) -> Option<String> {
    // An explicit `x.id = '1'` wins over the constructor argument, the same precedence
    // `entity_id_event_column` uses.
    for asg in &func.assignments {
        if asg.entity == entity && asg.field == "id" {
            if let Some(lit) = string_literal(&asg.expr) {
                return Some(lit);
            }
        }
    }
    let body = &func.body;
    let mut i = 0usize;
    while i < body.len() {
        let hit = match_let_new(body, i)
            .or_else(|| match_bare_new(body, i))
            .or_else(|| match_let_load(body, i));
        if let Some((_var, ent, next)) = hit {
            if ent == entity {
                let mut k = next;
                skip_ws_str(body, &mut k);
                if body[k..].starts_with('(') {
                    if let Some(lit) = string_literal(&take_expr(body, k + 1)) {
                        return Some(lit);
                    }
                }
            }
            i = next.max(i + 1);
            continue;
        }
        i += 1;
    }
    None
}

/// A single-quoted or double-quoted string literal, re-emitted as a SQL literal.
///
/// Nothing else: a numeric literal would be an id of a different type, and a template string or a
/// concatenation is a composed key, which is a separate piece of work. Quotes inside are doubled, so an id
/// containing one cannot terminate the literal early.
fn string_literal(expr: &str) -> Option<String> {
    let e = collapse_ws(expr).trim().to_string();
    for quote in ['\'', '"'] {
        if e.len() >= 2 && e.starts_with(quote) && e.ends_with(quote) {
            let inner = &e[1..e.len() - 1];
            if inner.contains(quote) {
                // An escaped quote inside - not a shape to guess at.
                return None;
            }
            return Some(format!("'{}'", inner.replace('\'', "''")));
        }
    }
    None
}

pub(crate) fn entity_id_event_column(
    entity: &str,
    func: &FunctionInfo,
    functions: &BTreeMap<String, FunctionInfo>,
) -> Option<String> {
    for asg in &func.assignments {
        if asg.entity == entity && asg.field == "id" {
            if let Some(col) = assignment_event_column(asg, func, functions) {
                return Some(col);
            }
        }
    }
    id_from_new_or_load(entity, &func.body)
}

fn id_from_new_or_load(entity: &str, body: &str) -> Option<String> {
    let mut i = 0;
    while i < body.len() {
        if let Some((_var, ent, next)) = match_let_new(body, i).or_else(|| match_bare_new(body, i))
        {
            if ent == entity {
                let mut k = next;
                skip_ws_str(body, &mut k);
                if body[k..].starts_with('(') {
                    let arg = take_expr(body, k + 1);
                    if let Some(col) = event_column(&arg) {
                        return Some(col);
                    }
                    // `new Pool(poolId)`, with `poolId` a local aliasing the parameter (#1277).
                    if let Some(col) = local_root(&arg)
                        .and_then(|v| plain_local_expr(body, &v))
                        .and_then(|e| event_column(&e))
                    {
                        return Some(col);
                    }
                }
            }
            i = next.max(i + 1);
            continue;
        }
        if let Some((_var, ent, next)) = match_let_load(body, i) {
            if ent == entity {
                let mut k = next;
                skip_ws_str(body, &mut k);
                if body[k..].starts_with('(') {
                    let arg = take_expr(body, k + 1);
                    if let Some(col) = event_column(&arg) {
                        return Some(col);
                    }
                    // `new Pool(poolId)`, with `poolId` a local aliasing the parameter (#1277).
                    if let Some(col) = local_root(&arg)
                        .and_then(|v| plain_local_expr(body, &v))
                        .and_then(|e| event_column(&e))
                    {
                        return Some(col);
                    }
                }
            }
            i = next.max(i + 1);
            continue;
        }
        i += 1;
    }
    None
}

/// Event-table column this assignment copies. Constructor ids (`new Token(event.params.token0)`)
/// use the constructor argument; a local that only aliases that argument is chased for `id`.
/// An accumulation: `x.f = x.f.plus(<operand>)` or `.minus(...)`, where the receiver names the same
/// field being written (RFC-0044 S5, #1214).
///
/// This is the shape a subgraph uses for a running total, and it is the one field shape that is
/// **better** as an RFC-0041 incremental entity than as a view. A view cannot express it at all: the
/// decoded table holds each event's delta, not the total, so the overlay's `last()` fold would
/// answer with the most recent delta. As `sum(operand) GROUP BY id` it is exactly right, and `sum`
/// is one of the six aggregates the v1 lowerer can maintain under insert and retraction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Accumulation {
    /// `.minus(..)`, which renders as `sum(-col)` rather than a second aggregate.
    pub negated: bool,
    pub operand: String,
}

pub(crate) fn accumulation(asg: &Assignment) -> Option<Accumulation> {
    let e = collapse_ws(&asg.expr);
    for (op, negated) in [(".plus(", false), (".minus(", true)] {
        let Some(at) = e.find(op) else { continue };
        let (receiver, rest) = e.split_at(at);
        // The receiver must be exactly the field currently being assigned. `a.total = b.total.plus(x)`
        // is not an accumulation of `a.total`, and treating it as one would sum the wrong column.
        if receiver != format!("{}.{}", asg.receiver, asg.field) {
            continue;
        }
        let inner = rest[op.len()..].strip_suffix(')')?.trim();
        // One flat argument only. A nested call is not an event column, and admitting it here would
        // put us back to guessing at an expression we cannot render - the defect in #1248.
        if inner.is_empty() || inner.contains('(') || inner.contains(')') || inner.contains(',') {
            return None;
        }
        return Some(Accumulation {
            negated,
            operand: inner.to_string(),
        });
    }
    None
}

pub(crate) fn assignment_event_column(
    asg: &Assignment,
    func: &FunctionInfo,
    functions: &BTreeMap<String, FunctionInfo>,
) -> Option<String> {
    expr_event_column(&asg.expr, func, functions)
}

/// The same ladder, on an expression rather than a whole assignment.
///
/// Split out so a *part* of a composed expression can be resolved the same way the whole of a simple one
/// is: `transaction.id + '-' + event.logIndex.toString()` is two of these and a literal, and resolving
/// the parts by a second, simpler rule would be a second set of answers to maintain.
pub(crate) fn expr_event_column(
    expr: &str,
    func: &FunctionInfo,
    functions: &BTreeMap<String, FunctionInfo>,
) -> Option<String> {
    let asg = &Assignment {
        receiver: String::new(),
        entity: String::new(),
        field: String::new(),
        citation: Citation {
            file: String::new(),
            line: 0,
        },
        expr: expr.to_string(),
        receiver_is_entity: false,
    };
    if let Some(col) = event_column(&asg.expr) {
        return Some(col);
    }
    // **Not gated on `id` any more, and an entity reference resolves like any other alias.** A
    // reference to another entity is that entity's id, which is the expression it was loaded with, and
    // `local_root` already strips a trailing `.id` - so `swap.pool = pool.id` needs no special case,
    // only permission to follow the chain for a field that is not called `id`.
    //
    // `tick.pool = poolId` is as much `event.params.id` as `tick.id = tickId` is; the old gate was
    // narrower than the reasoning behind it. `plain_local_expr` already refuses a name bound more than
    // once, so broadening this cannot silently pick the wrong binding.
    if let Some(ident) = local_root(&asg.expr) {
        if let Some(col) = event_column_through_aliases(&func.body, &ident) {
            return Some(col);
        }
    }
    // **A one-hop read of another entity's field, when that field is this event's own value.**
    //
    // ```ts
    // const transaction = loadTransaction(event)   // sets transaction.timestamp = event.block.timestamp
    // swap.timestamp = transaction.timestamp       // so this *is* `block_timestamp`, on this row
    // ```
    //
    // `local_root` strips a trailing `.id` and nothing else, so this reached no column and 15 fields on
    // Uniswap V4 were reported as having none (#1313).
    //
    // **Only when the field is written from the event in this handler or a helper it calls.** That is the
    // whole safety condition: `loadTransaction` overwrites `timestamp` from the current event before
    // returning, so the value is this event's. A field written only in a *different* handler -
    // `poolDayData.open = pool.token0Price`, where the price was set when the pool was initialised - is a
    // read of stored state and needs a join to that entity as of this block, not a column. Those stay
    // unresolved here rather than being answered with the wrong row's value.
    entity_field_event_column(&asg.expr, func, functions)
}

/// One piece of a composed expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ConcatPart {
    /// A string literal, as written. Quoting for SQL is the emitter's job.
    Literal(String),
    /// An event column name, to be resolved against the triggering table.
    Column(String),
}

/// A composed value: `transaction.id + '-' + event.logIndex.toString()`, or the template literal
/// `` `${transactionHash.toHexString()}-${logIndex.toString()}` `` that `eventId` returns.
///
/// This is the commonest id shape a subgraph writes - an event entity's primary key is almost always the
/// transaction hash and the log index with a separator - and seven entities on Uniswap V4 had **no `id`
/// at all** because of it. An id is not one field among many: without it the emitted view cannot fold on
/// the entity, so a missing `id` costs every other field on that view its `last()`.
///
/// **All or nothing.** One part that resolves to neither a literal nor a column and the whole expression
/// is unresolved, because a composed key with a piece missing is a different key, and a different key is
/// a wrong row rather than a missing one.
pub(crate) fn expr_concat_parts(
    expr: &str,
    func: &FunctionInfo,
    functions: &BTreeMap<String, FunctionInfo>,
) -> Option<Vec<ConcatPart>> {
    let e = collapse_ws(expr).trim().to_string();
    // A call to a one-line helper is the expression it returns. `eventId(event.transaction.hash,
    // event.logIndex)` is how three of Uniswap V4's event entities spell their primary key, and without
    // this it reads as an opaque call.
    let e = inline_single_return(&e, functions).unwrap_or(e);
    // No "at least two pieces" rule: a single-hole template literal - `` `${x}` `` - is the same value as
    // `x` and resolves to the same column, and refusing it would only mean one spelling answered and
    // another did not. `split_concat` already refuses anything that is not a concatenation at all.
    let pieces = split_concat(&e)?;
    let mut out = Vec::new();
    for piece in pieces {
        let t = piece.trim();
        if let Some(lit) = plain_string_literal(t) {
            out.push(ConcatPart::Literal(lit));
            continue;
        }
        out.push(ConcatPart::Column(expr_event_column(t, func, functions)?));
    }
    Some(out)
}

/// `event.transaction.hash.concatI32(event.logIndex.toI32())`, the id `graph init` writes on every event
/// entity, or `event.transaction.hash.concat(Bytes.fromI32(event.logIndex.toI32()))`: graph-ts defines
/// `Bytes.concatI32(x)` as `concat(ByteArray.fromI32(x))`, and `Bytes.fromI32` as that same `ByteArray`.
/// Either is the hash's bytes followed by the log index as an i32, little-endian.
#[cfg(feature = "graph")]
pub(crate) fn is_tx_log_bytes_id(
    expr: &str,
    func: &FunctionInfo,
    functions: &BTreeMap<String, FunctionInfo>,
) -> bool {
    let e = collapse_ws(expr).trim().to_string();
    let split = [".concatI32(", ".concat("].iter().find_map(|op| {
        let at = e.find(op)?;
        let open = at + op.len() - 1;
        // The call must be the whole expression: `x.concatI32(y).toHex()` is a string, not these bytes.
        (match_paren(&e, open)? == e.len() - 1)
            .then(|| (*op, e[..at].trim(), e[open + 1..e.len() - 1].trim()))
    });
    let Some((op, receiver, arg)) = split else {
        return false;
    };
    let int_expr = if op == ".concatI32(" {
        arg
    } else {
        let open = "Bytes.fromI32".len();
        match arg.strip_prefix("Bytes.fromI32(") {
            Some(inner) if match_paren(arg, open) == Some(arg.len() - 1) => {
                inner[..inner.len() - 1].trim()
            }
            _ => return false,
        }
    };
    // `toI32()` is what fixes the width at four bytes. Neither side may carry another converter, which
    // `expr_event_column` would strip: a hex string is not the hash's bytes.
    let Some(index) = int_expr.strip_suffix(".toI32()").map(str::trim) else {
        return false;
    };
    if [receiver, index].iter().any(|s| strip_converters(s) != *s) {
        return false;
    }
    expr_event_column(receiver, func, functions).as_deref() == Some("tx_hash")
        && expr_event_column(index, func, functions).as_deref() == Some("log_index")
}

/// A call to a helper whose whole body is one `return`, as that expression with the arguments in place.
///
/// **One statement only.** A helper that does anything else - a branch, a load, a `save` - is not a pure
/// function of its arguments, and substituting its return expression would drop whatever else it did. One
/// `return` and nothing else is the case where the substitution is the whole meaning of the call.
///
/// Arity must match exactly. A defaulted parameter would leave an unsubstituted name behind, which would
/// then resolve against the *caller's* locals if one happened to share the name.
fn inline_single_return(expr: &str, functions: &BTreeMap<String, FunctionInfo>) -> Option<String> {
    let open = expr.find('(')?;
    let name = expr[..open].trim();
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') || name.is_empty() {
        return None;
    }
    // The call has to be the whole expression, not the head of a larger one.
    if match_paren(expr, open)? != expr.len() - 1 {
        return None;
    }
    let f = functions.get(name)?;
    let args = take_paren_list(expr, open);
    if args.len() != f.param_names.len() {
        return None;
    }
    let mut body = String::new();
    for line in f.body.lines() {
        let t = line.trim();
        if t.is_empty() || t.starts_with("//") {
            continue;
        }
        if !body.is_empty() {
            return None;
        }
        body = t.to_string();
    }
    let mut ret = body.strip_prefix("return ")?.trim().to_string();
    ret = ret.trim_end_matches(';').trim().to_string();
    for (param, arg) in f.param_names.iter().zip(&args) {
        ret = substitute_ident(&ret, param, arg.trim());
    }
    Some(ret)
}

/// The pieces of a `+` chain, or of a template literal's text and `${…}` holes.
///
/// Returns `None` for anything that is not a concatenation, so a caller cannot mistake a single value for
/// a one-piece chain. Quotes, parentheses and brackets are tracked, so a `+` inside a literal or inside a
/// call's arguments does not split - `a + f(x + 1)` is two pieces, not three.
fn split_concat(e: &str) -> Option<Vec<String>> {
    if let Some(inner) = e
        .strip_prefix('`')
        .and_then(|r| r.strip_suffix('`'))
        .filter(|_| e.len() >= 2)
    {
        return Some(template_pieces(inner));
    }
    let b = e.as_bytes();
    let (mut depth, mut quote, mut start) = (0i32, None::<u8>, 0usize);
    let mut out: Vec<String> = Vec::new();
    let mut i = 0usize;
    while i < b.len() {
        let c = b[i];
        match quote {
            Some(q) => {
                if c == b'\\' {
                    i += 2;
                    continue;
                }
                if c == q {
                    quote = None;
                }
            }
            None => match c {
                b'\'' | b'"' | b'`' => quote = Some(c),
                b'(' | b'[' | b'{' => depth += 1,
                b')' | b']' | b'}' => depth -= 1,
                b'+' if depth == 0 => {
                    out.push(e[start..i].to_string());
                    start = i + 1;
                }
                _ => {}
            },
        }
        i += 1;
    }
    if out.is_empty() {
        return None;
    }
    out.push(e[start..].to_string());
    Some(out)
}

/// A template literal's literal runs and its `${…}` expressions, in order.
fn template_pieces(inner: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = inner;
    while let Some(at) = rest.find("${") {
        if at > 0 {
            out.push(format!("'{}'", &rest[..at]));
        }
        let after = &rest[at + 2..];
        let mut depth = 1i32;
        let mut end = after.len();
        for (i, c) in after.char_indices() {
            match c {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        end = i;
                        break;
                    }
                }
                _ => {}
            }
        }
        out.push(after[..end].to_string());
        rest = after.get(end + 1..).unwrap_or("");
    }
    if !rest.is_empty() {
        out.push(format!("'{rest}'"));
    }
    out
}

/// The text of a single- or double-quoted literal, with no escapes in it.
///
/// An escape is refused rather than interpreted: the emitter puts this text into SQL, and guessing what
/// `\n` meant in AssemblyScript would put a different separator in the key.
fn plain_string_literal(t: &str) -> Option<String> {
    for q in ['\'', '"'] {
        if t.len() >= 2 && t.starts_with(q) && t.ends_with(q) {
            let inner = &t[1..t.len() - 1];
            if inner.contains(q) || inner.contains('\\') {
                return None;
            }
            return Some(inner.to_string());
        }
    }
    None
}

/// `local.field` resolved to an event column, when a reachable function writes that field from the event.
///
/// `functions` is searched only through `func.calls` - this handler and the helpers it calls - because a
/// write in an unrelated handler is a different event's value.
fn entity_field_event_column(
    expr: &str,
    func: &FunctionInfo,
    functions: &BTreeMap<String, FunctionInfo>,
) -> Option<String> {
    let e = collapse_ws(&strip_converters(expr));
    let e = e.trim();
    let (recv, field) = e.rsplit_once('.')?;
    if field.is_empty()
        || !field.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        || !recv.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        || recv.is_empty()
    {
        return None;
    }
    // **This body only, and only a receiver bound fresh in it.**
    //
    // The first version of this took the handler plus every function it calls, matched a local by name
    // and entity type, and accepted an event assignment from any of those bodies. Jules found the
    // counter-example on #1316: a handler that does `let transaction = Transaction.load(id)`, calls a
    // helper that happens to bind its own local of the same name and entity and writes
    // `transaction.timestamp = event.block.timestamp`, and the read resolves to `block_timestamp`
    // while the handler's receiver still holds whatever an earlier block stored. A name and a type are
    // not a value flow.
    //
    // So: the binding must be `new Entity(..)` in this body, which is the one case where nothing was
    // read back from the store, and the assignment must be in this body too. A `load()`ed receiver
    // carries an earlier block's value and is refused; the helper-return shape needs the returned
    // value tracked through the call and is not this change.
    if let Some((entity, fresh)) = local_entity_binding(&func.body, recv) {
        if !fresh {
            return None;
        }
        // **Every** assignment to this receiver's field, not every assignment to that entity's field.
        // A second local of the same entity is a different row, and its assignment says nothing about
        // this one (Jules, #1316). And every one of them, not the first that happens to be an event
        // column: one later assignment from anything else and the value at the read is not this event's.
        let mut col = None;
        for asg in &func.assignments {
            if asg.field != field || asg.entity != entity || asg.receiver != recv {
                continue;
            }
            col = Some(event_column(&asg.expr)?);
        }
        return col;
    }
    // The receiver is the return value of a helper this body calls: `const transaction =
    // loadTransaction(event)`. That is the common shape and it is followable, because the entity comes
    // from the helper's own binding rather than from a name that matched.
    let rhs = local_assignment(&func.body, recv)?;
    // No "does this body call it" check: `rhs` is the local's own right-hand side, taken from this body,
    // so a bare identifier before `(` is one `collect_calls` has already seen. A method call keeps its
    // dot and matches no function name.
    let callee = rhs.split('(').next()?.trim();
    returned_field_event_column(functions.get(callee)?, field)
}

/// The event column a field of the entity a helper returns is known to hold, on every path out of it.
///
/// `loadTransaction` is the shape: load by the transaction hash, `new` it if absent, then set
/// `timestamp` from `event.block.timestamp` before returning. The value is this event's whether or not
/// the row already existed, and that is what makes the caller's `transaction.timestamp` exact.
fn returned_field_event_column(helper: &FunctionInfo, field: &str) -> Option<String> {
    let var = returned_local(&helper.body)?;
    let (entity, _) = local_entity_binding(&helper.body, &var)?;
    // `id` is never assigned: it is the argument the row was bound with. Every binding of that local has
    // to carry the same argument, because a `load` on one path and a `new` on another each set it.
    if field == "id" {
        let args = binding_arguments(&helper.body, &var);
        let first = args.first()?;
        // Every binding, agreeing. One `new` on its own is as sound as a `load` and a `new` that carry the
        // same argument; two that disagree mean the id depends on which path ran.
        if args.iter().any(|a| a != first) {
            return None;
        }
        return event_column(first);
    }
    // Assigned from an event, every time, and at least once at the body's top level so that it happens
    // on every path rather than only the one an `if` took.
    // **A returned row escapes, so freshness exempts nothing here.**
    // `assignment_reaches_every_stored_row` lets a terminator before the assignment pass when the receiver
    // is `new`, because a row this function never saved is not a stored row. That argument is about rows
    // *this* function stores, and it does not hold for one it hands back: `let transaction = new
    // Transaction(id); if (event.skip) return transaction; transaction.timestamp = ..` returns a row with
    // the field unset, and the caller saves it (Jules, #1316). So any terminator before the assignment
    // refuses, fresh or not.
    let body_lines: Vec<&str> = helper.body.lines().collect();
    let mut col = None;
    let mut unconditional = false;
    for asg in &helper.assignments {
        // The returned local, by name. A helper may load and return `transaction` while also creating an
        // unrelated `other` of the same entity and writing `other.timestamp` from the event; matching on
        // the entity alone reported that as the returned row's value (Jules, #1316).
        if asg.field != field || asg.entity != entity || asg.receiver != var {
            continue;
        }
        let c = event_column(&asg.expr)?;
        if col.as_ref().is_some_and(|prev| prev != &c) {
            return None;
        }
        let idx = asg
            .citation
            .line
            .checked_sub(helper.body_start_line)
            .unwrap_or(usize::MAX);
        let escapes_unset =
            idx >= body_lines.len() || body_lines[..idx].iter().any(|l| aborts_the_handler(l));
        unconditional |= !escapes_unset && assignment_reaches_every_stored_row(helper, asg, false);
        col = Some(c);
    }
    if unconditional {
        col
    } else {
        None
    }
}

/// The local a function returns, from `return x` or `return x as Entity`.
fn returned_local(body: &str) -> Option<String> {
    for line in body.lines() {
        let t = line.trim();
        let Some(rest) = t.strip_prefix("return ") else {
            continue;
        };
        let v = rest.trim().trim_end_matches(';').trim();
        let v = v.split(" as ").next().unwrap_or(v).trim();
        if !v.is_empty() && v.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Some(v.to_string());
        }
    }
    None
}

/// Every argument a local was bound with, across `new Entity(..)` and `Entity.load(..)` alike.
fn binding_arguments(body: &str, var: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut i = 0usize;
    while i < body.len() {
        let hit = match_let_new(body, i)
            .or_else(|| match_bare_new(body, i))
            .or_else(|| match_let_create(body, i))
            .or_else(|| match_let_load(body, i));
        if let Some((v, _ent, open)) = hit {
            if v == var {
                if let Some(a) = take_paren_list(body, open).into_iter().next() {
                    args.push(collapse_ws(&strip_converters(&a)).trim().to_string());
                }
            }
            i = open.max(i + 1);
            continue;
        }
        i += 1;
    }
    args
}

/// Whether every **stored** row of the receiver carries what this assignment wrote.
///
/// Brace depth alone is not path coverage, and Jules was right about both ways it fails on #1316: an
/// earlier `return` skips a depth-zero assignment, and `if (cond) pool.f = ZERO` sits at depth zero
/// while being conditional. What closes it is not control-flow analysis but **persistence**:
///
/// - a row only exists once something saves it, so an early `return` before the save stores nothing and
///   leaves no row missing the field. Uniswap V4's `handleInitialize` returns three times before
///   `pool.save()`, and none of those paths leaves a `Pool` behind;
/// - a row *can* be stored without the field if a `save()` runs before the assignment. So every
///   `<recv>.save()` in this body has to come after it, and there has to be at least one - a function
///   that never saves hands the row to its caller, and the value then depends on a path this cannot see.
///
/// Still requires depth zero and refuses a braceless control header, because those say the assignment
/// did not run at all on some path that did reach the save.
pub(crate) fn assignment_reaches_every_stored_row(
    func: &FunctionInfo,
    asg: &Assignment,
    same_on_every_path: bool,
) -> bool {
    let Some(idx) = asg.citation.line.checked_sub(func.body_start_line) else {
        return false;
    };
    let lines: Vec<&str> = func.body.lines().collect();
    if idx >= lines.len() {
        return false;
    }
    let mut depth = 0i32;
    for line in &lines[..idx] {
        for c in line.chars() {
            match c {
                '{' => depth += 1,
                '}' => depth -= 1,
                _ => {}
            }
        }
    }
    // **Inside a block is fine when the block is the one that creates the row** (#1326).
    //
    // ```ts
    // let token0 = Token.load(id)
    // if (token0 === null) {
    //   token0 = new Token(id)
    //   token0.totalValueLockedUSDUntracked = ZERO_BD   // every Token that exists came through here
    // }
    // ```
    //
    // This is the commonest shape in subgraph-land - load, guard on null, construct, initialise every
    // field - and the depth test refused all of it. A row that skipped the branch was created by an
    // earlier execution of the same branch, so it holds what the branch put there; the depth rule was
    // asking whether *this* invocation assigned it, which is the wrong question.
    if depth != 0 {
        // **Only when the value does not depend on which invocation ran.** The creation guard's argument
        // is that a row which skipped the branch was created by an earlier execution of it, so it holds
        // what the branch put there. For a literal that is the same value. For an **event column** it is
        // an *earlier event's* value, and reading it now is stale - `loadTransaction` setting
        // `transaction.timestamp` inside its null guard is exactly that, and a caller reading it back is
        // not reading this event's timestamp.
        if !same_on_every_path {
            return false;
        }
        let Some((open, close)) = enclosing_block(&lines, idx) else {
            return false;
        };
        // **At the block's own level, not merely somewhere inside it.** Scanning the whole block counted a
        // construction nested in a further branch: `if (a) { if (b) { pool = new Pool(id) }
        // pool.collectedFeesUSD = ZERO_BD }` assigns without constructing whenever `a` holds and `b` does
        // not, and an existing row is then saved without the field (Jules, #1326). A construction that
        // shares the assignment's level runs on every path that reaches it.
        if !constructed_at_this_level(&lines, open, idx, &asg.receiver) {
            return false;
        }
        // **No terminator check between the construction and the assignment**, though the first version
        // had one and it cost the field this change exists for. Uniswap V4 constructs `token0`, fetches
        // its decimals and returns if they are null - before the constants. That `return` abandons a row
        // nothing saved, because the save-ordering rule below already establishes that no
        // `<recv>.save()` precedes the assignment. A terminator before it therefore cannot leave a
        // *stored* row unassigned, and refusing on one asks a question already answered.
        let _ = close;
    }
    // `if (cond) pool.f = ZERO` on one line, or the header on the line above with no brace opened.
    if governed_by_a_braceless_header(&lines, idx) {
        return false;
    }
    // **A loaded row already exists, and needs no save to keep its old value.** That is why "every save
    // comes after the assignment" is not enough on its own (Jules, #1316): with
    // `let pool = Pool.load(id)!; if (event.skip) return; pool.f = ZERO_BD; pool.save()`, the early return
    // leaves a stored row that never saw the assignment. A row bound by `new` has nothing stored yet, so a
    // return before the save genuinely stores nothing - Uniswap V4's `handleInitialize` does
    // `const pool = new Pool(poolId)` and then returns twice on null decimals, leaving no `Pool` behind.
    // Skipped inside a creation guard: the block itself constructs the row, so an earlier terminator
    // abandoned an invocation that created nothing rather than leaving a row unassigned.
    let in_creation_guard = depth != 0;
    let fresh = local_entity_binding(&func.body, &asg.receiver).is_some_and(|(_, fresh)| fresh);
    if !in_creation_guard && !fresh && lines[..idx].iter().any(|l| aborts_the_handler(l)) {
        return false;
    }
    // Every save of this receiver, after the assignment, and at least one.
    let needle = format!("{}.save()", asg.receiver);
    let mut saves = 0usize;
    for (n, line) in lines.iter().enumerate() {
        if !line.contains(&needle) {
            continue;
        }
        if n <= idx {
            return false;
        }
        saves += 1;
    }
    saves > 0
}

/// The line range of the innermost block containing `idx`, as `[open_line, close_line)`.
fn enclosing_block(lines: &[&str], idx: usize) -> Option<(usize, usize)> {
    // The open-brace stack is the depth; a separate counter would only restate its length.
    let mut opens: Vec<usize> = Vec::new();
    for (n, line) in lines.iter().enumerate() {
        for c in line.chars() {
            match c {
                '{' => opens.push(n),
                '}' => {
                    let open = opens.pop()?;
                    if n >= idx && open <= idx {
                        return Some((open, n));
                    }
                }
                _ => {}
            }
        }
    }
    None
}

/// Whether `var` is constructed at the top level of the block opening on `open`, before `idx`.
///
/// Relative depth, so a construction inside a nested branch of that block does not count: it runs on some
/// paths to the assignment and not others, which is the whole distinction.
fn constructed_at_this_level(lines: &[&str], open: usize, idx: usize, var: &str) -> bool {
    let mut rel = 0i32;
    for (n, line) in lines.iter().enumerate().take(idx).skip(open) {
        if n > open && rel == 0 && constructs(line, var) {
            return true;
        }
        for c in line.chars() {
            match c {
                '{' => rel += 1,
                '}' => rel -= 1,
                _ => {}
            }
        }
        // The block's own body sits one brace in; anything deeper is a nested branch.
        rel -= i32::from(n == open);
    }
    false
}

/// Whether this line binds `var` with `new Entity(..)`, with or without a declaration keyword.
fn constructs(line: &str, var: &str) -> bool {
    let t = line.trim();
    let rest = t
        .strip_prefix("let ")
        .or_else(|| t.strip_prefix("const "))
        .or_else(|| t.strip_prefix("var "))
        .unwrap_or(t);
    rest.strip_prefix(var)
        .map(str::trim_start)
        .and_then(|r| r.strip_prefix('='))
        .map(str::trim_start)
        .is_some_and(|r| r.starts_with("new "))
}

/// Whether a line can end the handler before the next statement runs.
///
/// `return` is the obvious one. `assert(..)` and `throw` are the others (Jules, #1316): AssemblyScript's
/// `assert` throws on a false condition, and a throw is a deterministic error whose block graph-node
/// discards - so a row loaded from an earlier block keeps what that block stored, having never reached the
/// assignment. Textual, and deliberately so: this is a guard on what the classifier may claim, and a
/// terminator it cannot recognise has to read as one it cannot rule out.
fn aborts_the_handler(line: &str) -> bool {
    let t = line.trim();
    let keyword = |k: &str| {
        t == k
            || t.strip_prefix(k)
                .is_some_and(|r| r.starts_with([' ', ';', '(']))
    };
    keyword("return") || keyword("throw") || keyword("assert")
}

/// Whether the statement on `idx` is the body of a conditional or loop that opened no block.
fn governed_by_a_braceless_header(lines: &[&str], idx: usize) -> bool {
    const HEADS: [&str; 4] = ["if", "for", "while", "else"];
    let is_head = |t: &str| {
        HEADS.iter().any(|h| {
            t == *h
                || t.strip_prefix(h).is_some_and(|r| r.starts_with([' ', '(']))
                || t.strip_prefix("} ")
                    .is_some_and(|r| r == *h || r.starts_with(&format!("{h} ")))
        })
    };
    let own = lines[idx].trim();
    if is_head(own) {
        return true;
    }
    // The nearest statement above, skipping blanks and line comments.
    for prev in lines[..idx].iter().rev() {
        let t = prev.trim();
        if t.is_empty() || t.starts_with("//") {
            continue;
        }
        return is_head(t) && !t.ends_with('{');
    }
    false
}

/// The entity a local holds, and whether it was bound fresh rather than read back from the store.
///
/// `new Entity(..)` and `Entity.create(..)` are fresh: every field of that row is whatever this
/// invocation puts there. `Entity.load(..)` is not - the row already existed, written by some earlier
/// block, so its fields are stored state.
fn local_entity_binding(body: &str, var: &str) -> Option<(String, bool)> {
    let mut i = 0usize;
    while i < body.len() {
        let fresh = match_let_new(body, i)
            .or_else(|| match_bare_new(body, i))
            .or_else(|| match_let_create(body, i));
        let hit = match fresh {
            Some((v, ent, next)) => Some((v, ent, next, true)),
            None => match_let_load(body, i).map(|(v, ent, next)| (v, ent, next, false)),
        };
        if let Some((v, ent, next, fresh)) = hit {
            if v == var {
                return Some((ent, fresh));
            }
            i = next.max(i + 1);
            continue;
        }
        i += 1;
    }
    None
}

/// The expression an entity local was built from, or the expression a plain local aliases.
///
/// `Entity.load(X)`, `new Entity(X)` and `Entity.create(X)` all name the entity's id in `X`; a local
/// that holds no entity aliases its own right-hand side (#1277).
fn entity_id_expr(body: &str, ident: &str) -> Option<String> {
    constructor_arg_for(body, ident)
        .or_else(|| load_arg_for(body, ident))
        .or_else(|| create_arg_for(body, ident))
        .or_else(|| plain_local_expr(body, ident))
}

/// Follow an alias chain to the event parameter at the end of it, if there is one.
///
/// **One hop is not enough, and that is not a corner case.** Uniswap V4's swap mapping writes
/// `const poolId = event.params.poolId.toHexString()`, then `const pool = Pool.load(poolId)`, then
/// `swap.pool = pool.id` - so the parameter is two aliases from the assignment, and resolving one hop
/// recovered `Pool.token0` (loaded directly from a parameter) while leaving `Swap.pool` unanswerable.
///
/// Bounded and cycle-guarded: a malformed mapping must not spin here, and `plain_local_expr` already
/// refuses a name bound more than once, so the chain is unambiguous at every step or it stops.
fn event_column_through_aliases(body: &str, ident: &str) -> Option<String> {
    let mut seen: Vec<String> = Vec::new();
    let mut cur = ident.to_string();
    for _ in 0..ALIAS_DEPTH {
        if let Some(col) = event_column(&cur) {
            return Some(col);
        }
        let root = local_root(&cur)?;
        if seen.contains(&root) {
            return None;
        }
        seen.push(root.clone());
        cur = entity_id_expr(body, &root)?;
    }
    None
}

/// Alias hops followed before giving up. Three is the deepest the Uniswap V4 mappings need
/// (`pool.id` -> `poolId` -> `event.params.poolId`); the rest is headroom, not a measurement.
const ALIAS_DEPTH: usize = 8;

/// The event/call handler that wrote this, or a handler that reaches this helper. Block handlers
/// are never a table source.
///
/// **The search is transitive, because one hop is not how real subgraphs are written.** This used to
/// look for an event handler calling the helper *directly*. Uniswap V4 routes every handler through
/// an indirection for testability - `handleSwap` calls `handleSwapHelper(event, config)`, and that
/// calls `loadTransaction`, `updatePoolDayData` and the rest - so the handler is two hops away and
/// nothing those helpers wrote reached a column. It was not one field either: an entity whose every
/// assignment lives two hops down got no view at all, `Transaction` among them, while
/// `event.block.number` and `event.block.timestamp` were already mapped to implicit columns and
/// would have bound perfectly well (#1277).
///
/// Breadth-first from each event handler, with a visited set: a mapping may call a helper from two
/// handlers, and `arrakis.ts` and `euler.ts` both call shared utilities, so the call graph is a DAG
/// at best and cyclic at worst.
pub(crate) fn event_handler_for<'a>(
    func: &'a FunctionInfo,
    mappings: &'a Mappings,
) -> Option<&'a FunctionInfo> {
    if func.kind == HandlerKind::Event || func.kind == HandlerKind::Call {
        return Some(func);
    }
    if func.kind == HandlerKind::Block {
        return None;
    }
    // Deterministic: `functions` is a BTreeMap, so two handlers reaching the same helper always
    // resolve to the same one rather than to whichever the iterator happened to yield.
    for handler in mappings.functions.values() {
        if handler.kind != HandlerKind::Event && handler.kind != HandlerKind::Call {
            continue;
        }
        let mut seen: BTreeSet<&str> = BTreeSet::new();
        let mut queue: Vec<&str> = handler.calls.iter().map(String::as_str).collect();
        while let Some(name) = queue.pop() {
            if !seen.insert(name) {
                continue;
            }
            if name == func.name {
                return Some(handler);
            }
            if let Some(next) = mappings.functions.get(name) {
                queue.extend(next.calls.iter().map(String::as_str));
            }
        }
    }
    None
}

/// `const <var> = <expr>` where `<expr>` is a plain expression rather than an entity constructor.
///
/// The entity-aware lookups below only find a local that holds an *entity* - `new Pool(..)`,
/// `Pool.load(..)`, `Pool.create(..)`. A local that merely aliases an event parameter was invisible,
/// and `Pool.id` is exactly that shape:
///
/// ```text
/// const poolId = event.params.id.toHexString()   // poolManager.ts:37
/// const pool = new Pool(poolId)                  // poolManager.ts:68
/// ```
///
/// Without this the emitter reported the primary key of all 132,765 pools as having no corresponding
/// decoded column, while `pool_manager__initialize.id` held it already hex-encoded, and every field
/// the `pool` view did answer became unalignable against a reference (#1277).
///
/// The returned expression is handed to [`event_column`], which is strict: `strip_converters` drops
/// only the representation-only suffixes, and any arithmetic residue after the parameter name yields
/// no column at all (#1248). So a local holding a *transformed* parameter still resolves to nothing
/// rather than to the wrong column.
///
/// **A name declared more than once resolves to nothing.** Scanning cannot tell which declaration is
/// in scope at the use site, and the first textual one is not it:
///
/// ```text
/// const poolId = event.params.a.toHexString()
/// if (..) {
///   const poolId = event.params.b.toHexString()
///   const pool = new Pool(poolId)            // uses b, not a
/// }
/// ```
///
/// Returning the outer declaration would emit `Pool.id` from column `a` - a view answering the wrong
/// column under a report promising byte-identical, which is #1248's fault in a new place. Refusing
/// leaves the field reported as reaching no column, which is what it did before this fallback
/// existed. Raised by review of this change.
pub(crate) fn plain_local_expr(body: &str, var: &str) -> Option<String> {
    let mut found: Vec<String> = Vec::new();
    let mut i = 0;
    while i < body.len() {
        if !body.is_char_boundary(i) {
            i += 1;
            continue;
        }
        let rest = &body[i..];
        let kw = if rest.starts_with("let ") || rest.starts_with("var ") {
            4
        } else if rest.starts_with("const ") {
            6
        } else {
            i += 1;
            continue;
        };
        // Not a keyword if it is the tail of a longer identifier.
        if i > 0 {
            let p = body.as_bytes()[i - 1];
            if p.is_ascii_alphanumeric() || p == b'_' {
                i += 1;
                continue;
            }
        }
        let mut k = i + kw;
        let Some(v) = take_ident_str(body, &mut k) else {
            i += 1;
            continue;
        };
        skip_ws_str(body, &mut k);
        if !body[k..].starts_with('=') {
            i += 1;
            continue;
        }
        k += 1;
        skip_ws_str(body, &mut k);
        if v == var {
            let end = body[k..]
                .find([';', '\n'])
                .map(|o| k + o)
                .unwrap_or(body.len());
            let e = collapse_ws(&body[k..end]);
            if !e.is_empty() {
                found.push(e);
            }
        }
        i = k.max(i + 1);
    }
    // Exactly one declaration, or nothing. See the shadowing note above.
    match found.len() {
        1 => found.pop(),
        _ => None,
    }
}

fn local_root(expr: &str) -> Option<String> {
    let e = strip_converters(expr);
    let e = e.strip_suffix(".id").unwrap_or(&e).trim();
    if e.is_empty() {
        return None;
    }
    if !e.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return None;
    }
    if e.chars().next()?.is_ascii_digit() {
        return None;
    }
    Some(e.to_string())
}

fn constructor_arg_for(body: &str, var: &str) -> Option<String> {
    let mut i = 0;
    while i < body.len() {
        let hit = match_let_new(body, i).or_else(|| match_bare_new(body, i));
        if let Some((v, _ent, next)) = hit {
            if v == var {
                let mut k = next;
                skip_ws_str(body, &mut k);
                if body[k..].starts_with('(') {
                    return Some(collapse_ws(&take_expr(body, k + 1)));
                }
            }
            i = next.max(i + 1);
            continue;
        }
        i += 1;
    }
    None
}

fn load_arg_for(body: &str, var: &str) -> Option<String> {
    arg_after_match(body, var, match_let_load)
}

fn create_arg_for(body: &str, var: &str) -> Option<String> {
    arg_after_match(body, var, match_let_create)
}

type ArgumentMatcher = fn(&str, usize) -> Option<(String, String, usize)>;

fn arg_after_match(body: &str, var: &str, matcher: ArgumentMatcher) -> Option<String> {
    let mut i = 0;
    while i < body.len() {
        if let Some((v, _ent, next)) = matcher(body, i) {
            if v == var {
                let mut k = next;
                skip_ws_str(body, &mut k);
                if body[k..].starts_with('(') {
                    return Some(collapse_ws(&take_expr(body, k + 1)));
                }
            }
            i = next.max(i + 1);
            continue;
        }
        i += 1;
    }
    None
}

/// Map a mapping expression onto a nest column (`token0`, `address`, …). `None` if it is not
/// a field of the triggering event - emit then refuses to guess.
pub(crate) fn event_column(expr: &str) -> Option<String> {
    let e = strip_converters(expr);
    if let Some(rest) = e.strip_prefix("event.params.") {
        let mut k = 0;
        let name = take_ident_str(rest, &mut k)?;
        if name.is_empty() {
            return None;
        }
        // **Nothing may follow the parameter name.** This used to take the leading identifier and
        // discard the rest, so `event.params.fee.neg()`, `.times(x)` and `.plus(y)` all resolved to
        // the `fee` column - a value with the wrong sign, the wrong scale, or a missing addend,
        // emitted into a view whose report promises the field is byte-identical (#1248). An
        // operation this function cannot represent must yield no column at all, so the caller can
        // report the field as skipped rather than answer it wrongly. `strip_converters` has already
        // removed the representation-only suffixes, which are the ones that genuinely do not change
        // the value; a residue here is arithmetic, an array index, or something else unmodelled.
        if !rest[k..].trim().is_empty() {
            return None;
        }
        // **The parameter name as written, not snake_cased** (#1250). `snake_case` builds the
        // *table* name from the event name (`{alias}__{snake_case(event.name)}`); a column is the
        // ABI parameter name verbatim, because `EventDecoder::new` takes `p.name.clone()`.
        // Snake-casing it meant every camelCase parameter named a column that does not exist, so a
        // view over `sqrtPriceX96` or `tickSpacing` asked for `sqrt_price_x96` and did not bind at
        // all. The only fixture that bound an emitted view used `token0`, which snake_cases to
        // itself, which is why the whole class was invisible.
        return Some(name);
    }
    match e.as_str() {
        "event.address" => Some("address".into()),
        "event.block.timestamp" => Some("block_timestamp".into()),
        "event.block.number" => Some("block_number".into()),
        "event.transaction.hash" => Some("tx_hash".into()),
        "event.logIndex" => Some("log_index".into()),
        _ => None,
    }
}

fn sol_type_of_expr(expr: &str, func: &FunctionInfo) -> Option<&'static str> {
    let e = strip_converters(expr);
    if e.starts_with("event.params.") || e == "event.address" {
        return Some("address");
    }
    if func.param_names.iter().any(|p| p == &e) {
        return Some("address");
    }
    if e.starts_with("Address.") || e.starts_with("Address.from") {
        return Some("address");
    }
    None
}

fn literal_address(expr: &str) -> Option<String> {
    let e = strip_converters(expr);
    // Address.fromString('0x…') / Address.fromHexString("0x…")
    for prefix in ["Address.fromString(", "Address.fromHexString("] {
        if let Some(rest) = e.strip_prefix(prefix) {
            let rest = rest.trim_start_matches(['\'', '"']).trim_end_matches(')');
            let rest = rest.trim_end_matches(['\'', '"', ' ', ')']);
            if rest.starts_with("0x") && rest.len() == 42 {
                return Some(rest.to_ascii_lowercase());
            }
        }
    }
    if e.starts_with("0x") && e.len() == 42 {
        return Some(e.to_ascii_lowercase());
    }
    None
}

pub(crate) fn resolved_contract(expr: &str) -> ResolvedContract {
    if let Some(col) = event_column(expr) {
        return ResolvedContract::Column(col);
    }
    if let Some(addr) = literal_address(expr) {
        return ResolvedContract::Address(addr);
    }
    ResolvedContract::Unknown
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ResolvedContract {
    Column(String),
    Address(String),
    Unknown,
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn schema_and_mappings(schema: &str, file: &str, mapping: &str) -> (Schema, Mappings) {
        let schema = parse_schema(schema).unwrap();
        let mut functions = BTreeMap::new();
        for func in parse_functions(mapping, file) {
            functions.insert(func.name.clone(), func);
        }
        (
            schema,
            Mappings {
                functions,
                handlers: Vec::new(),
                declared_handlers: BTreeSet::new(),
            },
        )
    }

    fn class_of(rows: &[FieldRow], entity: &str, field: &str) -> Class {
        rows.iter()
            .find(|r| r.entity == entity && r.field == field)
            .unwrap_or_else(|| panic!("missing {entity}.{field}"))
            .class
    }

    fn reason_of(rows: &[FieldRow], entity: &str, field: &str) -> String {
        rows.iter()
            .find(|r| r.entity == entity && r.field == field)
            .unwrap()
            .reason
            .clone()
    }

    #[test]
    fn constructor_id_maps_to_the_event_column() {
        let schema = r#"
type Token @entity {
  id: ID!
  symbol: String!
}
type Pool @entity {
  id: ID!
  token0: Token!
}
"#;
        let mapping = r#"
export function handlePoolCreated(event: PoolCreated): void {
  let token0 = new Token(event.params.token0.toHex())
  token0.symbol = 'x'
  token0.save()
  let pool = new Pool(event.params.pool.toHex())
  pool.token0 = token0.id
  pool.save()
}
"#;
        let (_schema, mappings) = schema_and_mappings(schema, "src/mappings/core.ts", mapping);
        let func = mappings.functions.get("handlePoolCreated").unwrap();
        let token_id = func
            .assignments
            .iter()
            .find(|a| a.entity == "Token" && a.field == "id")
            .expect("constructor id");
        assert_eq!(
            assignment_event_column(token_id, func, &mappings.functions).as_deref(),
            Some("token0")
        );
        let pool_id = func
            .assignments
            .iter()
            .find(|a| a.entity == "Pool" && a.field == "id")
            .expect("pool constructor id");
        assert_eq!(
            assignment_event_column(pool_id, func, &mappings.functions).as_deref(),
            Some("pool")
        );
    }

    /// A foreign key is the referenced entity's id, which is the expression it was loaded with - and
    /// that is **two aliases** away from the event parameter, not one.
    ///
    /// Uniswap V4's swap mapping writes `const poolId = event.params.poolId.toHexString()`, then
    /// `const pool = Pool.load(poolId)`, then `swap.pool = pool.id`. Resolving a single hop recovered
    /// `Pool.token0` (loaded straight from a parameter) and left `Swap.pool` unanswerable, which is why
    /// the chain is followed rather than one step taken.
    ///
    /// Worth more than the column: S2 aggregates a `@derivedFrom` list off the **child's**
    /// back-reference with no column on the parent, so resolving `Swap.pool` also makes `Pool.swaps`
    /// answerable.
    #[test]
    fn a_foreign_key_resolves_through_an_alias_chain() {
        let schema = r#"
type Pool @entity { id: ID! }
type Swap @entity { id: ID! pool: Pool! }
"#;
        let mapping = r#"
export function handleSwap(event: SwapEvent): void {
  const poolId = event.params.poolId.toHexString()
  const pool = Pool.load(poolId)
  if (pool == null) { return }
  let swap = new Swap(event.transaction.hash.toHexString())
  swap.pool = pool.id
  swap.save()
}
"#;
        let (_schema, mappings) = schema_and_mappings(schema, "src/mappings/swap.ts", mapping);
        let func = mappings.functions.get("handleSwap").unwrap();
        let fk = func
            .assignments
            .iter()
            .find(|a| a.entity == "Swap" && a.field == "pool")
            .expect("the foreign key assignment");
        assert_eq!(
            assignment_event_column(fk, func, &mappings.functions).as_deref(),
            Some("poolId"),
            "`pool.id` -> `Pool.load(poolId)` -> `event.params.poolId`"
        );

        // A chain that ends nowhere stays unresolved rather than guessing a column.
        let mapping = r#"
export function handleSwap(event: SwapEvent): void {
  const pool = Pool.load(somethingElse())
  let swap = new Swap('x')
  swap.pool = pool.id
  swap.save()
}
"#;
        let (_schema, mappings) = schema_and_mappings(schema, "src/mappings/swap.ts", mapping);
        let func = mappings.functions.get("handleSwap").unwrap();
        let fk = func
            .assignments
            .iter()
            .find(|a| a.entity == "Swap" && a.field == "pool")
            .expect("the foreign key assignment");
        assert_eq!(assignment_event_column(fk, func, &mappings.functions), None);

        // And a self-referential chain terminates instead of spinning.
        let mapping = r#"
export function handleSwap(event: SwapEvent): void {
  const a = b
  const b = a
  let swap = new Swap('x')
  swap.pool = a
  swap.save()
}
"#;
        let (_schema, mappings) = schema_and_mappings(schema, "src/mappings/swap.ts", mapping);
        let func = mappings.functions.get("handleSwap").unwrap();
        let fk = func
            .assignments
            .iter()
            .find(|a| a.entity == "Swap" && a.field == "pool")
            .expect("the foreign key assignment");
        assert_eq!(assignment_event_column(fk, func, &mappings.functions), None);
    }

    #[test]
    fn event_assignment_is_exact() {
        let schema = r#"
type Swap @entity {
  id: ID!
  amount0: BigDecimal!
  timestamp: BigInt!
}
"#;
        let mapping = r#"
export function handleSwap(event: SwapEvent): void {
  let swap = new Swap(event.transaction.hash.toHex())
  swap.amount0 = event.params.amount0
  swap.timestamp = event.block.timestamp
  swap.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/mappings/core.ts", mapping);
        let rows = classify(&schema, &mappings);
        assert_eq!(class_of(&rows, "Swap", "amount0"), Class::Exact);
        assert_eq!(class_of(&rows, "Swap", "timestamp"), Class::Exact);
        assert_eq!(class_of(&rows, "Swap", "id"), Class::Exact);
        assert!(reason_of(&rows, "Swap", "amount0").contains("event.params.amount0"));
    }

    #[test]
    fn local_holding_a_call_is_still_call_derived() {
        let schema = r#"
type Token @entity {
  id: ID!
  decimals: BigInt!
}
"#;
        let mapping = r#"
export function fetchTokenDecimals(tokenAddress: Address): BigInt {
  let contract = ERC20.bind(tokenAddress)
  return contract.try_decimals().value
}
export function handlePoolCreated(event: PoolCreated): void {
  let token0 = new Token(event.params.token0.toHex())
  const decimals = fetchTokenDecimals(event.params.token0)
  token0.decimals = decimals
  token0.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/factory.ts", mapping);
        let rows = classify(&schema, &mappings);
        assert_eq!(class_of(&rows, "Token", "decimals"), Class::CallDerived);
    }

    /// Jules on #1242. `metadata` is a plain object with no binding, so before the guard the
    /// unique-field fallback took `metadata.symbol = ..` for a write to `Token.symbol` on the sole
    /// evidence that `symbol` is uniquely owned, and reported the field exact.
    #[test]
    fn an_unresolved_receiver_that_is_never_saved_is_not_an_entity_write() {
        let schema_src = r#"
type Token @entity {
  id: ID!
  symbol: String!
}
"#;
        let plain_object = r#"
export function handleTransfer(event: Transfer): void {
  let metadata = loadMetadata(event.address)
  metadata.symbol = event.params.symbol
}
"#;
        let (schema, mappings) = schema_and_mappings(schema_src, "src/token.ts", plain_object);
        let rows = classify(&schema, &mappings);
        assert_eq!(
            class_of(&rows, "Token", "symbol"),
            Class::Unreachable,
            "an assignment to an object nothing saves is not a write to Token.symbol: {}",
            reason_of(&rows, "Token", "symbol")
        );
        assert!(
            reason_of(&rows, "Token", "symbol").contains("no mapping writes this field"),
            "{}",
            reason_of(&rows, "Token", "symbol")
        );

        // Saved, so it is an entity, and the unique field then says which one. This is the case
        // the fallback exists for, and it must keep working.
        let saved = r#"
export function handleTransfer(event: Transfer): void {
  let token = getOrMakeToken(event.address)
  token.symbol = event.params.symbol
  token.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(schema_src, "src/token.ts", saved);
        let rows = classify(&schema, &mappings);
        assert_eq!(class_of(&rows, "Token", "symbol"), Class::Exact);
        assert!(reason_of(&rows, "Token", "symbol").contains("event.params.symbol"));
    }

    #[test]
    fn create_or_load_binds_the_entity() {
        let schema = r#"
type Transcoder @entity {
  id: ID!
  serviceURI: String!
}
"#;
        let mapping = r#"
export function serviceURIUpdate(event: ServiceURIUpdate): void {
  let transcoder = createOrLoadTranscoder(event.params.addr.toHex())
  transcoder.serviceURI = event.params.serviceURI
  transcoder.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/serviceRegistry.ts", mapping);
        let rows = classify(&schema, &mappings);
        assert_eq!(class_of(&rows, "Transcoder", "serviceURI"), Class::Exact);
        assert!(reason_of(&rows, "Transcoder", "serviceURI").contains("event.params.serviceURI"));
    }

    #[test]
    fn contract_try_is_call_derived() {
        let schema = r#"
type Token @entity {
  id: ID!
  symbol: String!
}
"#;
        let mapping = r#"
export function fetchTokenSymbol(tokenAddress: Address): string {
  let contract = ERC20.bind(tokenAddress)
  let result = contract.try_symbol()
  if (!result.reverted) {
    return result.value
  }
  return 'unknown'
}

export function handlePoolCreated(event: PoolCreated): void {
  let token0 = new Token(event.params.token0.toHex())
  token0.symbol = fetchTokenSymbol(event.params.token0)
  token0.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/common/token.ts", mapping);
        let rows = classify(&schema, &mappings);
        assert_eq!(class_of(&rows, "Token", "symbol"), Class::CallDerived);
        assert!(reason_of(&rows, "Token", "symbol").contains("contract state"));
    }

    /// A one-hop read of another entity's field resolves to this row's column.
    ///
    /// ```ts
    /// const transaction = loadTransaction(event)   // sets transaction.timestamp = event.block.timestamp
    /// swap.timestamp = transaction.timestamp       // so this *is* `block_timestamp`, on this row
    /// ```
    ///
    /// `local_root` strips a trailing `.id` and nothing else, so this reached no column and ten fields on
    /// Uniswap V4 - `timestamp` and `transaction` across five event entities - were reported as having
    /// none (#1313).
    #[test]
    fn a_one_hop_read_of_an_entity_field_resolves_to_a_column() {
        let schema = r#"
type Transaction @entity {
  id: ID!
  timestamp: BigInt!
}
type Swap @entity {
  id: ID!
  timestamp: BigInt!
}
"#;
        let mapping = r#"
export function loadTransaction(event: ethereum.Event): Transaction {
  let transaction = new Transaction(event.transaction.hash.toHex())
  transaction.timestamp = event.block.timestamp
  transaction.save()
  return transaction
}

export function handleSwap(event: SwapEvent): void {
  let transaction = loadTransaction(event)
  let swap = new Swap(event.transaction.hash.toHex())
  swap.timestamp = transaction.timestamp
  swap.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/utils.ts", mapping);
        let func = mappings.functions.get("handleSwap").expect("handler");
        let asg = func
            .assignments
            .iter()
            .find(|a| a.entity == "Swap" && a.field == "timestamp")
            .expect("the assignment");
        assert_eq!(
            assignment_event_column(asg, func, &mappings.functions).as_deref(),
            Some("block_timestamp"),
            "the helper writes it from this event, so it is this row's column"
        );
        let rows = classify(&schema, &mappings);
        assert_eq!(class_of(&rows, "Swap", "timestamp"), Class::Exact);
    }

    /// A helper's same-named local is a different binding (Jules, #1316).
    ///
    /// ```ts
    /// const transaction = Transaction.load(id)   // an earlier block wrote this row
    /// touch(event)                               // binds its own `transaction`, sets it from the event
    /// swap.timestamp = transaction.timestamp     // still the earlier block's value
    /// ```
    ///
    /// The first version of the one-hop read matched the receiver by variable name and entity type across
    /// the handler and every function it called, so the helper's assignment answered for the handler's
    /// receiver. A name and a type are not a value flow.
    #[test]
    fn a_helpers_same_named_local_does_not_answer_for_a_loaded_receiver() {
        let schema = r#"
type Transaction @entity {
  id: ID!
  timestamp: BigInt!
}
type Swap @entity {
  id: ID!
  timestamp: BigInt!
}
"#;
        let mapping = r#"
export function touch(event: ethereum.Event): void {
  let transaction = new Transaction(event.transaction.hash.toHex())
  transaction.timestamp = event.block.timestamp
  transaction.save()
}

export function handleSwap(event: SwapEvent): void {
  let transaction = Transaction.load(event.transaction.hash.toHex())
  touch(event)
  let swap = new Swap(event.transaction.hash.toHex())
  swap.timestamp = transaction.timestamp
  swap.save()
}
"#;
        let (_schema, mappings) = schema_and_mappings(schema, "src/utils.ts", mapping);
        let func = mappings.functions.get("handleSwap").expect("handler");
        let asg = func
            .assignments
            .iter()
            .find(|a| a.entity == "Swap" && a.field == "timestamp")
            .expect("the assignment");
        assert_eq!(
            assignment_event_column(asg, func, &mappings.functions),
            None,
            "a loaded receiver holds whatever an earlier block stored, whatever a helper called its own \
             local"
        );
    }

    /// A field the helper sets on only one path out is not the event's value on the other.
    ///
    /// `loadTransaction` is exact because it overwrites `timestamp` *after* the `if`. Move that line
    /// inside the branch and the already-existing row keeps the timestamp of the block that created it.
    #[test]
    fn a_field_the_helper_sets_on_one_path_only_reaches_no_column() {
        let schema = r#"
type Transaction @entity {
  id: ID!
  timestamp: BigInt!
}
type Swap @entity {
  id: ID!
  timestamp: BigInt!
}
"#;
        let conditional = r#"
export function loadTransaction(event: ethereum.Event): Transaction {
  let transaction = Transaction.load(event.transaction.hash.toHex())
  if (transaction === null) {
    transaction = new Transaction(event.transaction.hash.toHex())
    transaction.timestamp = event.block.timestamp
  }
  transaction.save()
  return transaction
}

export function handleSwap(event: SwapEvent): void {
  let transaction = loadTransaction(event)
  let swap = new Swap(event.transaction.hash.toHex())
  swap.timestamp = transaction.timestamp
  swap.save()
}
"#;
        let (_schema, mappings) = schema_and_mappings(schema, "src/utils.ts", conditional);
        let func = mappings.functions.get("handleSwap").expect("handler");
        let asg = func
            .assignments
            .iter()
            .find(|a| a.entity == "Swap" && a.field == "timestamp")
            .expect("the assignment");
        assert_eq!(
            assignment_event_column(asg, func, &mappings.functions),
            None,
            "set inside the `if`, so a row that already existed keeps the older block's timestamp"
        );

        // The same helper with that line after the branch is the real `loadTransaction`, and it is exact.
        let unconditional = conditional.replace(
            "    transaction = new Transaction(event.transaction.hash.toHex())\n    transaction.timestamp = event.block.timestamp\n  }",
            "    transaction = new Transaction(event.transaction.hash.toHex())\n  }\n  transaction.timestamp = event.block.timestamp",
        );
        assert_ne!(unconditional, conditional, "the fixture edit must apply");
        let (_schema, mappings) = schema_and_mappings(schema, "src/utils.ts", &unconditional);
        let func = mappings.functions.get("handleSwap").expect("handler");
        let asg = func
            .assignments
            .iter()
            .find(|a| a.entity == "Swap" && a.field == "timestamp")
            .expect("the assignment");
        assert_eq!(
            assignment_event_column(asg, func, &mappings.functions).as_deref(),
            Some("block_timestamp"),
            "overwritten on every path out, so the caller's read is this event's value"
        );
    }

    /// A loaded receiver in the handler itself is stored state, whatever the handler later writes.
    ///
    /// The assignment list carries no order relative to the read, so `pool.x` read before
    /// `pool.x = event.params.v` and read after it look the same from here. Refusing a loaded receiver
    /// outright is what makes that unanswerable rather than a coin toss.
    #[test]
    fn a_loaded_receiver_in_the_handler_is_not_this_events_value() {
        let schema = r#"
type Pool @entity {
  id: ID!
  tick: BigInt!
}
type Swap @entity {
  id: ID!
  tick: BigInt!
}
"#;
        let mapping = r#"
export function handleSwap(event: SwapEvent): void {
  let pool = Pool.load(event.address.toHex())
  let swap = new Swap(event.transaction.hash.toHex())
  swap.tick = pool.tick
  pool.tick = event.params.tick
  pool.save()
  swap.save()
}
"#;
        let (_schema, mappings) = schema_and_mappings(schema, "src/pool.ts", mapping);
        let func = mappings.functions.get("handleSwap").expect("handler");
        let asg = func
            .assignments
            .iter()
            .find(|a| a.entity == "Swap" && a.field == "tick")
            .expect("the assignment");
        assert_eq!(
            assignment_event_column(asg, func, &mappings.functions),
            None,
            "the read happens before the write, and nothing here can tell that from the other order"
        );
    }

    /// A composed id: the transaction hash and the log index with a separator.
    ///
    /// The commonest primary key a subgraph writes, and seven entities on Uniswap V4 had **no `id` at
    /// all** because of it - which costs every other field on those views its `last()` fold, not just the
    /// id.
    #[test]
    fn a_composed_id_resolves_to_its_parts() {
        let schema = r#"
type Transaction @entity {
  id: ID!
}
type Swap @entity {
  id: ID!
}
"#;
        // Three spellings of one key: a `+` chain through a loaded entity's id, a template literal, and a
        // call to a helper that returns one.
        let mapping = r#"
export function eventId(transactionHash: Bytes, logIndex: BigInt): string {
  return `${transactionHash.toHexString()}-${logIndex.toString()}`
}

export function loadTransaction(event: ethereum.Event): Transaction {
  let transaction = new Transaction(event.transaction.hash.toHexString())
  transaction.save()
  return transaction
}

export function handleSwap(event: SwapEvent): void {
  let transaction = loadTransaction(event)
  let swap = new Swap(transaction.id + '-' + event.logIndex.toString())
  swap.id = transaction.id + '-' + event.logIndex.toString()
  swap.save()
}

export function handleOther(event: SwapEvent): void {
  let swap = new Swap(eventId(event.transaction.hash, event.logIndex))
  swap.id = eventId(event.transaction.hash, event.logIndex)
  swap.save()
}

export function handleThird(event: SwapEvent): void {
  let swap = new Swap('x')
  swap.id = `${event.transaction.hash.toHexString()}-${event.logIndex.toString()}`
  swap.save()
}
"#;
        let (_schema, mappings) = schema_and_mappings(schema, "src/swap.ts", mapping);
        let want = vec![
            ConcatPart::Column("tx_hash".into()),
            ConcatPart::Literal("-".into()),
            ConcatPart::Column("log_index".into()),
        ];
        for handler in ["handleSwap", "handleOther", "handleThird"] {
            let func = mappings.functions.get(handler).expect(handler);
            let asg = func
                .assignments
                .iter()
                .find(|a| a.entity == "Swap" && a.field == "id")
                .unwrap_or_else(|| panic!("{handler} must assign Swap.id"));
            assert_eq!(
                expr_concat_parts(&asg.expr, func, &mappings.functions).as_deref(),
                Some(want.as_slice()),
                "{handler}: `{}` must resolve to the hash, the separator and the log index",
                asg.expr
            );
        }
    }

    /// A single-hole template literal is the value inside it.
    #[test]
    fn a_single_hole_template_literal_resolves_to_its_column() {
        let schema = r#"
type Swap @entity {
  id: ID!
}
"#;
        let mapping = r#"
export function handleSwap(event: SwapEvent): void {
  let swap = new Swap('x')
  swap.id = `${event.transaction.hash.toHexString()}`
  swap.save()
}
"#;
        let (_schema, mappings) = schema_and_mappings(schema, "src/swap.ts", mapping);
        let func = mappings.functions.get("handleSwap").expect("handler");
        let asg = func
            .assignments
            .iter()
            .find(|a| a.entity == "Swap" && a.field == "id")
            .expect("the assignment");
        assert_eq!(
            expr_concat_parts(&asg.expr, func, &mappings.functions).as_deref(),
            Some([ConcatPart::Column("tx_hash".into())].as_slice()),
            "one spelling of a value must not answer while another does not"
        );
    }

    /// Arity must match, or an unsubstituted parameter resolves against the **caller's** locals.
    ///
    /// The wrong-key shape: the helper takes a separator it was not given, the caller happens to have a
    /// local of that name bound to an event parameter, and the id silently becomes hash-plus-fee.
    #[test]
    fn a_helper_called_with_too_few_arguments_is_not_inlined() {
        let schema = r#"
type Swap @entity {
  id: ID!
}
"#;
        let mapping = r#"
export function mkId(hash: Bytes, sep: string): string {
  return `${hash.toHexString()}${sep}`
}

export function handleSwap(event: SwapEvent): void {
  let sep = event.params.fee
  let swap = new Swap('x')
  swap.id = mkId(event.transaction.hash)
  swap.save()
}
"#;
        let (_schema, mappings) = schema_and_mappings(schema, "src/swap.ts", mapping);
        let func = mappings.functions.get("handleSwap").expect("handler");
        let asg = func
            .assignments
            .iter()
            .find(|a| a.entity == "Swap" && a.field == "id")
            .expect("the assignment");
        assert_eq!(
            expr_concat_parts(&asg.expr, func, &mappings.functions),
            None,
            "`sep` was never passed, and the caller's local of that name is not what the helper meant"
        );
    }

    /// A **fresh** row returned before its assignment still reaches the caller unset (Jules, #1316).
    ///
    /// The freshness exemption is about rows a function *stores*: one it never saved is not a stored row,
    /// so a terminator before the assignment leaves nothing behind. A returned row is the other case - it
    /// escapes, and the caller saves it - so the exemption must not apply on that path.
    #[test]
    fn a_fresh_row_returned_before_its_assignment_reaches_no_column() {
        let schema = r#"
type Transaction @entity {
  id: ID!
  timestamp: BigInt!
}
type Swap @entity {
  id: ID!
  timestamp: BigInt!
}
"#;
        let mapping = r#"
export function loadTransaction(event: ethereum.Event): Transaction {
  let transaction = new Transaction(event.transaction.hash.toHex())
  if (event.logIndex.equals(BigInt.zero())) {
    return transaction
  }
  transaction.timestamp = event.block.timestamp
  transaction.save()
  return transaction
}

export function handleSwap(event: SwapEvent): void {
  let transaction = loadTransaction(event)
  let swap = new Swap(event.transaction.hash.toHex())
  swap.timestamp = transaction.timestamp
  swap.save()
}
"#;
        let (_schema, mappings) = schema_and_mappings(schema, "src/utils.ts", mapping);
        let func = mappings.functions.get("handleSwap").expect("handler");
        let asg = func
            .assignments
            .iter()
            .find(|a| a.entity == "Swap" && a.field == "timestamp")
            .expect("the assignment");
        assert_eq!(
            assignment_event_column(asg, func, &mappings.functions),
            None,
            "the early-return path hands the caller a row whose timestamp was never set"
        );
    }

    /// A helper that does more than return is not a pure function of its arguments.
    #[test]
    fn a_helper_that_does_more_than_return_is_not_inlined() {
        let schema = r#"
type Swap @entity {
  id: ID!
}
"#;
        let mapping = r#"
export function eventId(transactionHash: Bytes, logIndex: BigInt): string {
  log.info('making an id', [])
  return `${transactionHash.toHexString()}-${logIndex.toString()}`
}

export function handleSwap(event: SwapEvent): void {
  let swap = new Swap('x')
  swap.id = eventId(event.transaction.hash, event.logIndex)
  swap.save()
}
"#;
        let (_schema, mappings) = schema_and_mappings(schema, "src/swap.ts", mapping);
        let func = mappings.functions.get("handleSwap").expect("handler");
        let asg = func
            .assignments
            .iter()
            .find(|a| a.entity == "Swap" && a.field == "id")
            .expect("the assignment");
        assert_eq!(
            expr_concat_parts(&asg.expr, func, &mappings.functions),
            None,
            "substituting the return would drop whatever else the helper did"
        );
    }

    /// One unresolved piece refuses the whole key.
    #[test]
    fn a_composed_id_with_an_unresolved_piece_resolves_to_nothing() {
        let schema = r#"
type Swap @entity {
  id: ID!
}
"#;
        // `whatever` is bound to a contract call, so it is no part of a key a column can answer.
        let mapping = r#"
export function handleSwap(event: SwapEvent): void {
  let whatever = contract.someValue()
  let swap = new Swap('x')
  swap.id = event.transaction.hash.toHexString() + '-' + whatever
  swap.save()
}
"#;
        let (_schema, mappings) = schema_and_mappings(schema, "src/swap.ts", mapping);
        let func = mappings.functions.get("handleSwap").expect("handler");
        let asg = func
            .assignments
            .iter()
            .find(|a| a.entity == "Swap" && a.field == "id")
            .expect("the assignment");
        assert_eq!(
            expr_concat_parts(&asg.expr, func, &mappings.functions),
            None,
            "a key missing a piece is a different key, which is a wrong row rather than a missing one"
        );
    }

    /// Another local of the same entity is a different row (Jules, #1316).
    ///
    /// ```ts
    /// let transaction = Transaction.load(id)        // returned, and still stored state
    /// let other = new Transaction(otherId)
    /// other.timestamp = event.block.timestamp       // says nothing about `transaction`
    /// return transaction
    /// ```
    ///
    /// The scan matched on entity and field, so `other`'s assignment answered for the row the helper
    /// actually returns.
    #[test]
    fn another_local_of_the_same_entity_does_not_answer_for_the_returned_row() {
        let schema = r#"
type Transaction @entity {
  id: ID!
  timestamp: BigInt!
}
type Swap @entity {
  id: ID!
  timestamp: BigInt!
}
"#;
        let mapping = r#"
export function loadTransaction(event: ethereum.Event): Transaction {
  let transaction = Transaction.load(event.transaction.hash.toHex())
  let other = new Transaction(event.block.hash.toHex())
  other.timestamp = event.block.timestamp
  other.save()
  return transaction
}

export function handleSwap(event: SwapEvent): void {
  let transaction = loadTransaction(event)
  let swap = new Swap(event.transaction.hash.toHex())
  swap.timestamp = transaction.timestamp
  swap.save()
}
"#;
        let (_schema, mappings) = schema_and_mappings(schema, "src/utils.ts", mapping);
        let func = mappings.functions.get("handleSwap").expect("handler");
        let asg = func
            .assignments
            .iter()
            .find(|a| a.entity == "Swap" && a.field == "timestamp")
            .expect("the assignment");
        assert_eq!(
            assignment_event_column(asg, func, &mappings.functions),
            None,
            "the returned row is the loaded one, and nothing wrote its timestamp in this invocation"
        );
    }

    /// A second local of the same entity in the handler is a different row too.
    #[test]
    fn another_local_of_the_same_entity_does_not_answer_in_the_handler() {
        let schema = r#"
type Tick @entity {
  id: ID!
  liquidityNet: BigInt!
}
type Swap @entity {
  id: ID!
  liquidityNet: BigInt!
}
"#;
        let mapping = r#"
export function handleSwap(event: SwapEvent): void {
  let upper = new Tick(event.params.upper.toString())
  let lower = new Tick(event.params.lower.toString())
  lower.liquidityNet = event.params.liquidity
  let swap = new Swap(event.transaction.hash.toHex())
  swap.liquidityNet = upper.liquidityNet
  swap.save()
}
"#;
        let (_schema, mappings) = schema_and_mappings(schema, "src/pool.ts", mapping);
        let func = mappings.functions.get("handleSwap").expect("handler");
        let asg = func
            .assignments
            .iter()
            .find(|a| a.entity == "Swap" && a.field == "liquidityNet")
            .expect("the assignment");
        assert_eq!(
            assignment_event_column(asg, func, &mappings.functions),
            None,
            "`lower` was written and `upper` was read, and they are not the same row"
        );
    }

    /// A helper whose returned row is bound with two different ids has no one id.
    #[test]
    fn a_returned_row_bound_with_two_different_ids_reaches_no_column() {
        let schema = r#"
type Transaction @entity {
  id: ID!
}
type Swap @entity {
  id: ID!
  transaction: Transaction!
}
"#;
        let mapping = r#"
export function loadTransaction(event: ethereum.Event): Transaction {
  let transaction = Transaction.load(event.transaction.hash.toHex())
  if (transaction === null) {
    transaction = new Transaction(event.block.hash.toHex())
  }
  transaction.save()
  return transaction
}

export function handleSwap(event: SwapEvent): void {
  let transaction = loadTransaction(event)
  let swap = new Swap(event.transaction.hash.toHex())
  swap.transaction = transaction.id
  swap.save()
}
"#;
        let (_schema, mappings) = schema_and_mappings(schema, "src/utils.ts", mapping);
        let func = mappings.functions.get("handleSwap").expect("handler");
        let asg = func
            .assignments
            .iter()
            .find(|a| a.entity == "Swap" && a.field == "transaction")
            .expect("the assignment");
        assert_eq!(
            assignment_event_column(asg, func, &mappings.functions),
            None,
            "the transaction hash on one path and the block hash on the other is not one column"
        );
    }

    /// A helper that writes the field from two different columns has no one answer for it.
    ///
    /// The last write wins at runtime. Nothing here knows which line ran last, so the honest answer is
    /// none rather than whichever the scan reached first.
    #[test]
    fn a_field_written_from_two_different_columns_reaches_no_column() {
        let schema = r#"
type Transaction @entity {
  id: ID!
  timestamp: BigInt!
}
type Swap @entity {
  id: ID!
  timestamp: BigInt!
}
"#;
        let mapping = r#"
export function loadTransaction(event: ethereum.Event): Transaction {
  let transaction = new Transaction(event.transaction.hash.toHex())
  transaction.timestamp = event.block.timestamp
  transaction.timestamp = event.block.number
  transaction.save()
  return transaction
}

export function handleSwap(event: SwapEvent): void {
  let transaction = loadTransaction(event)
  let swap = new Swap(event.transaction.hash.toHex())
  swap.timestamp = transaction.timestamp
  swap.save()
}
"#;
        let (_schema, mappings) = schema_and_mappings(schema, "src/utils.ts", mapping);
        let func = mappings.functions.get("handleSwap").expect("handler");
        let asg = func
            .assignments
            .iter()
            .find(|a| a.entity == "Swap" && a.field == "timestamp")
            .expect("the assignment");
        assert_eq!(
            assignment_event_column(asg, func, &mappings.functions),
            None,
            "two columns for one field is not one column"
        );
    }

    /// A field written only in a **different** handler is not this row's value.
    ///
    /// `poolDayData.open = pool.token0Price`, where the price was set when the pool was initialised. The
    /// value is reproducible in principle but it needs a join to that entity as of this block, not a
    /// column - and answering with a column would be answering from the wrong row.
    #[test]
    fn a_read_of_another_handlers_state_resolves_to_no_column() {
        let schema = r#"
type Pool @entity {
  id: ID!
  token0Price: BigDecimal!
}
type PoolDayData @entity {
  id: ID!
  open: BigDecimal!
}
"#;
        let mapping = r#"
export function handleInitialize(event: Initialize): void {
  let pool = new Pool(event.params.id.toHex())
  pool.token0Price = event.params.price
  pool.save()
}

export function updatePoolDayData(event: Swap): void {
  let pool = Pool.load(event.params.id.toHex())!
  let poolDayData = new PoolDayData(event.params.id.toHex())
  poolDayData.open = pool.token0Price
  poolDayData.save()
}
"#;
        let (_schema, mappings) = schema_and_mappings(schema, "src/utils.ts", mapping);
        let func = mappings
            .functions
            .get("updatePoolDayData")
            .expect("handler");
        let asg = func
            .assignments
            .iter()
            .find(|a| a.entity == "PoolDayData" && a.field == "open")
            .expect("the assignment");
        assert_eq!(
            assignment_event_column(asg, func, &mappings.functions),
            None,
            "`handleInitialize` is not reachable from here, so its column is a different event's"
        );
    }

    /// The receiver's **entity** has to match, not only the field name.
    ///
    /// Two unrelated entities with a `timestamp` would otherwise answer for each other, and the column
    /// would be right only by luck. Tightening this dropped one of eleven gained fields on Uniswap V4,
    /// which is to say it removed a genuine false positive.
    #[test]
    fn a_same_named_field_on_another_entity_does_not_answer() {
        let schema = r#"
type Receipt @entity {
  id: ID!
  stamp: BigInt!
}
type Swap @entity {
  id: ID!
  stamp: BigInt!
}
type Other @entity {
  id: ID!
  stamp: BigInt!
}
"#;
        let mapping = r#"
export function handleSwap(event: SwapEvent): void {
  let other = new Other(event.params.pool.toHex())
  other.stamp = event.block.timestamp
  other.save()

  let receipt = new Receipt(event.transaction.hash.toHex())
  let swap = new Swap(event.transaction.hash.toHex())
  swap.stamp = receipt.stamp
  swap.save()
}
"#;
        let (_schema, mappings) = schema_and_mappings(schema, "src/utils.ts", mapping);
        let func = mappings.functions.get("handleSwap").expect("handler");
        let asg = func
            .assignments
            .iter()
            .find(|a| a.entity == "Swap" && a.field == "stamp")
            .expect("the assignment");
        assert_eq!(
            assignment_event_column(asg, func, &mappings.functions),
            None,
            "`Other.stamp` is written from the event, but `receipt` is not an `Other`"
        );
    }

    /// A read of a **call-derived** field makes the reading field call-derived too.
    ///
    /// The propagation rule tested `== Class::FixedPoint`, so a call-derived field read propagated
    /// nothing. `Swap.amount0 = convertTokenToDecimal(event.params.amount0, token0.decimals)` was
    /// classified `exact` - a pure function of decoded events - while needing a contract read for
    /// `decimals`. Four such fields on Uniswap V4, and a porter reading `exact` has no reason to pass
    /// `--state-rpc`, after which every token amount is off by `10^decimals`: a wrong number rather
    /// than a missing one (#1310).
    #[test]
    fn a_read_of_a_call_derived_field_propagates() {
        let schema = r#"
type Token @entity {
  id: ID!
  decimals: BigInt!
}
type Swap @entity {
  id: ID!
  amount0: BigDecimal!
  logIndex: BigInt!
}
"#;
        let mapping = r#"
export function fetchTokenDecimals(addr: Address): BigInt {
  let contract = ERC20.bind(addr)
  return contract.decimals()
}

export function convertTokenToDecimal(raw: BigInt, decimals: BigInt): BigDecimal {
  return raw.toBigDecimal().div(exponentToBigDecimal(decimals))
}

export function handleSwap(event: SwapEvent): void {
  let token0 = new Token(event.params.currency0.toHex())
  token0.decimals = fetchTokenDecimals(event.params.currency0)
  token0.save()

  let swap = new Swap(event.transaction.hash.toHex())
  swap.amount0 = convertTokenToDecimal(event.params.amount0, token0.decimals)
  swap.logIndex = event.logIndex
  swap.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/utils.ts", mapping);
        let rows = classify(&schema, &mappings);

        assert_eq!(
            class_of(&rows, "Token", "decimals"),
            Class::CallDerived,
            "the premise: decimals is read from the contract"
        );
        assert_eq!(
            class_of(&rows, "Swap", "amount0"),
            Class::CallDerived,
            "a field that needs `decimals` is not a pure function of decoded events: {}",
            reason_of(&rows, "Swap", "amount0")
        );

        // The field beside it, reading only the event, is untouched - so the rule has not swallowed
        // every write in a handler that happens to make a call somewhere.
        assert_eq!(
            class_of(&rows, "Swap", "logIndex"),
            Class::Exact,
            "an event-only field in the same handler stays exact"
        );
    }

    /// The dependency usually arrives through a local, and that counts too.
    ///
    /// `pool.volumeToken0.plus(amount0Abs)` names no classified field. `amount0Abs` came from `amount0`,
    /// which came from `convertTokenToDecimal(event.params.amount0, token0.decimals)`, so the total cannot
    /// be computed without a contract read. Ten more fields on Uniswap V4 beyond the four direct ones
    /// (#1310).
    ///
    /// The helper that resolves this had a bug worth remembering: it used `?` where it needed `continue`,
    /// so it gave up on the first line of the body that was not a `let` and only ever resolved a local
    /// declared first. That made the expansion a silent no-op, and I read the zero-field result as
    /// evidence about the premise rather than about my own code.
    #[test]
    fn a_dependency_through_a_local_propagates() {
        let schema = r#"
type Token @entity {
  id: ID!
  decimals: BigInt!
}
type Pool @entity {
  id: ID!
  volumeToken0: BigDecimal!
  txCount: BigInt!
  liquidity: BigInt!
}
"#;
        let mapping = r#"
export function fetchTokenDecimals(addr: Address): BigInt {
  let contract = ERC20.bind(addr)
  return contract.decimals()
}

export function convertTokenToDecimal(raw: BigInt, decimals: BigInt): BigDecimal {
  return raw.toBigDecimal().div(exponentToBigDecimal(decimals))
}

export function handleSwap(event: SwapEvent): void {
  let token0 = new Token(event.params.currency0.toHex())
  token0.decimals = fetchTokenDecimals(event.params.currency0)
  token0.save()

  let pool = new Pool(event.params.pool.toHex())
  let amount0 = convertTokenToDecimal(event.params.amount0, token0.decimals)
  let amount0Abs = amount0.times(BigDecimal.fromString('-1'))
  pool.volumeToken0 = pool.volumeToken0.plus(amount0Abs)
  pool.txCount = pool.txCount.plus(ONE_BI)
  pool.liquidity = pool.liquidity.plus(event.params.liquidityDelta)
  pool.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/utils.ts", mapping);
        let rows = classify(&schema, &mappings);

        assert_eq!(
            class_of(&rows, "Pool", "volumeToken0"),
            Class::CallDerived,
            "two local hops to `decimals`: {}",
            reason_of(&rows, "Pool", "volumeToken0")
        );
        // The two accumulators beside it depend on nothing but the event, and must not be swept along.
        assert_eq!(
            class_of(&rows, "Pool", "txCount"),
            Class::Exact,
            "a counter is a pure function of the events: {}",
            reason_of(&rows, "Pool", "txCount")
        );
        assert_eq!(
            class_of(&rows, "Pool", "liquidity"),
            Class::Exact,
            "a raw event delta is exact: {}",
            reason_of(&rows, "Pool", "liquidity")
        );
    }

    /// A field name that collides with a local name is not expanded as one.
    ///
    /// `bare_idents` returns `a` in `a.b` and not `b`, because `b` is a field access that
    /// `expr_reads_field` already sees. Without that guard, a field called `rate` would be looked up as a
    /// local, find an unrelated `let rate = ..` in the same body, and take that local's class - so a field
    /// reading only exact state would be reported call-derived on the strength of a name collision.
    ///
    /// A mutation dropping the guard survived until this existed.
    #[test]
    fn a_field_name_colliding_with_a_local_is_not_expanded() {
        let schema = r#"
type Token @entity {
  id: ID!
  rate: BigDecimal!
  derived: BigDecimal!
}
type Pool @entity {
  id: ID!
  doubled: BigDecimal!
}
"#;
        let mapping = r#"
export function findEthPerToken(t: Token): BigDecimal {
  let other = Token.load(t.id)
  return other.derived
}

export function handleInit(event: InitEvent): void {
  let token0 = new Token(event.params.currency0.toHex())
  token0.rate = event.params.declaredRate.toBigDecimal()
  token0.derived = findEthPerToken(token0)
  token0.save()
}

export function handleSwap(event: SwapEvent): void {
  let token0 = Token.load(event.params.currency0.toHex())
  // A local whose *name* is also a field name, bound to a fixed-point value. Expanding the field
  // access `token0.rate` as if it were this local would make `pool.doubled` fixed point.
  let rate = token0.derived
  let pool = new Pool(event.params.pool.toHex())
  pool.doubled = token0.rate.times(BigDecimal.fromString('2'))
  pool.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/utils.ts", mapping);
        let rows = classify(&schema, &mappings);

        assert_eq!(
            class_of(&rows, "Token", "rate"),
            Class::Exact,
            "the premise: the *field* is written from the event"
        );
        assert_eq!(
            class_of(&rows, "Token", "derived"),
            Class::FixedPoint,
            "and the local `rate` is bound to this, which is not exact"
        );
        assert_eq!(
            class_of(&rows, "Pool", "doubled"),
            Class::Exact,
            "reads the exact field `token0.rate`; the fixed-point local `rate` in the same body must not \
             lend it a class: {}",
            reason_of(&rows, "Pool", "doubled")
        );
    }

    /// The stronger class wins when a field reads both.
    ///
    /// `max` over the classes, not first-one-found: a field reading a call-derived *and* a fixed-point
    /// value is fixed point, because that is the promise that cannot be kept.
    #[test]
    fn a_field_reading_both_takes_the_stronger_class() {
        let schema = r#"
type Token @entity {
  id: ID!
  decimals: BigInt!
  derivedETH: BigDecimal!
}
type Pool @entity {
  id: ID!
  tvlUSD: BigDecimal!
  scaled: BigDecimal!
}
"#;
        let mapping = r#"
export function fetchTokenDecimals(addr: Address): BigInt {
  let contract = ERC20.bind(addr)
  return contract.decimals()
}

export function findEthPerToken(token: Token): BigDecimal {
  let other = Token.load(token.id)
  return other.derivedETH
}

export function handleSwap(event: SwapEvent): void {
  let token0 = new Token(event.params.currency0.toHex())
  token0.decimals = fetchTokenDecimals(event.params.currency0)
  token0.derivedETH = findEthPerToken(token0)
  token0.save()

  let pool = new Pool(event.params.pool.toHex())
  pool.scaled = event.params.amount0.toBigDecimal().div(token0.decimals.toBigDecimal())
  pool.tvlUSD = token0.decimals.toBigDecimal().times(token0.derivedETH)
  pool.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/utils.ts", mapping);
        let rows = classify(&schema, &mappings);
        assert_eq!(
            class_of(&rows, "Pool", "scaled"),
            Class::CallDerived,
            "reads only the call-derived field: {}",
            reason_of(&rows, "Pool", "scaled")
        );
        assert_eq!(
            class_of(&rows, "Pool", "tvlUSD"),
            Class::FixedPoint,
            "reads both, so the stronger class wins: {}",
            reason_of(&rows, "Pool", "tvlUSD")
        );
    }

    #[test]
    fn multiline_helper_rhs_is_call_derived() {
        let schema = r#"
type Token @entity {
  id: ID!
  symbol: String!
  name: String!
}
"#;
        let mapping = r#"
export function fetchTokenSymbol(tokenAddress: Address): string {
  let contract = ERC20.bind(tokenAddress)
  return contract.try_symbol().value
}
export function handlePoolCreated(event: PoolCreated): void {
  let token0 = new Token(event.params.token0.toHex())
  token0.symbol =
    fetchTokenSymbol(event.params.token0)
  token0.name = event.params.name
  token0.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/common/token.ts", mapping);
        let rows = classify(&schema, &mappings);
        assert_eq!(class_of(&rows, "Token", "symbol"), Class::CallDerived);
        assert_eq!(class_of(&rows, "Token", "name"), Class::Exact);
        assert!(reason_of(&rows, "Token", "symbol").contains("fetchTokenSymbol"));
    }

    /// Jules on #1242. The citation must be the line that decided the class. For a call-derived
    /// field that is the `bind`/`try_` inside the helper, possibly several calls down, not the
    /// assignment that called it - a reader following the assignment site finds no contract call
    /// there and cannot check the claim.
    #[test]
    fn a_call_derived_field_cites_the_contract_call_not_the_assignment() {
        let schema = r#"
type Token @entity {
  id: ID!
  symbol: String!
}
"#;
        // Two hops: the assignment calls `fetchTokenSymbol`, which calls `readTokenSymbol`, and
        // only the innermost one binds the contract.
        let mapping = r#"
function readTokenSymbol(tokenAddress: Address): string {
  let contract = ERC20.bind(tokenAddress)
  return contract.try_symbol().value
}

export function fetchTokenSymbol(tokenAddress: Address): string {
  return readTokenSymbol(tokenAddress)
}

export function handlePoolCreated(event: PoolCreated): void {
  let token = new Token(event.params.token0.toHex())
  token.symbol = fetchTokenSymbol(event.params.token0)
  token.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/token.ts", mapping);
        let rows = classify(&schema, &mappings);
        assert_eq!(class_of(&rows, "Token", "symbol"), Class::CallDerived);
        let row = rows
            .iter()
            .find(|r| r.entity == "Token" && r.field == "symbol")
            .unwrap();
        assert_eq!(
            row.citation.line,
            3,
            "the citation must be the ERC20.bind line, not the assignment: {}",
            row.citation.display()
        );
        assert!(
            row.reason.contains("assigned at"),
            "and the assignment site must not be lost from the row: {}",
            row.reason
        );
    }

    /// When the call is on the assignment's own line there is nothing to redirect to.
    #[test]
    fn an_inline_contract_call_still_cites_its_own_assignment() {
        let schema = "type Token @entity {\n  id: ID!\n  symbol: String!\n}\n";
        let mapping = r#"
export function handlePoolCreated(event: PoolCreated): void {
  let token = new Token(event.params.token0.toHex())
  token.symbol = ERC20.bind(event.params.token0).try_symbol().value
  token.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/token.ts", mapping);
        let rows = classify(&schema, &mappings);
        assert_eq!(class_of(&rows, "Token", "symbol"), Class::CallDerived);
        let row = rows
            .iter()
            .find(|r| r.entity == "Token" && r.field == "symbol")
            .unwrap();
        assert_eq!(row.citation.line, 4, "{}", row.citation.display());
        assert!(
            !row.reason.contains("assigned at"),
            "no redirection, so no second location to name: {}",
            row.reason
        );
    }

    /// Jules on #1242 read the propagation as caller to callee: a blockHandler making every helper
    /// it calls unreachable, so a shared helper would poison an event handler's field.
    ///
    /// Two things stop it, and reversing the propagation direction leaves this test green, so the
    /// direction is not the one that matters. `propagate_fn_class` does run callee to caller. But
    /// the load-bearing rule is in `class_of_assignment`: a helper's class reaches an assignment
    /// only when that assignment's own right-hand side calls it, and `swap.amount0 =
    /// event.params.amount0` calls nothing. A helper's class is what `x = helper(..)` inherits, not
    /// a property of every write inside it.
    #[test]
    fn a_helper_shared_with_a_block_handler_does_not_poison_the_event_path() {
        let schema = r#"
type Swap @entity {
  id: ID!
  amount0: BigDecimal!
}
type BlockStat @entity {
  id: ID!
  n: BigInt!
}
"#;
        let mapping = r#"
export function record(swap: Swap, event: SwapEvent): void {
  swap.amount0 = event.params.amount0
  swap.save()
}

export function handleBlock(block: ethereum.Block): void {
  let s = new BlockStat(block.number.toString())
  s.n = block.number
  s.save()
  record(s as Swap, block as SwapEvent)
}

export function handleSwap(event: SwapEvent): void {
  let swap = new Swap(event.transaction.hash.toHex())
  record(swap, event)
}
"#;
        let (schema, mut mappings) = schema_and_mappings(schema, "src/x.ts", mapping);
        // The manifest is what marks a blockHandler and these fixtures have none, so set it here
        // or the scenario is not exercised at all and the test passes without touching the path.
        mappings.functions.get_mut("handleBlock").unwrap().kind = HandlerKind::Block;
        assert!(
            mappings.functions["handleBlock"].calls.contains("record")
                && mappings.functions["handleSwap"].calls.contains("record"),
            "the helper has to be shared for this to be testing anything"
        );

        let rows = classify(&schema, &mappings);
        assert_eq!(
            class_of(&rows, "BlockStat", "n"),
            Class::Unreachable,
            "the blockHandler's own writes are still unreachable"
        );
        assert_eq!(
            class_of(&rows, "Swap", "amount0"),
            Class::Exact,
            "the event path through the shared helper stays exact: {}",
            reason_of(&rows, "Swap", "amount0")
        );
    }

    #[test]
    fn nested_helper_is_call_derived() {
        let schema = r#"
type Token @entity {
  id: ID!
  symbol: String!
}
"#;
        let mapping = r#"
export function readSymbol(tokenAddress: Address): string {
  let contract = ERC20.bind(tokenAddress)
  return contract.try_symbol().value
}
export function fetchSymbol(tokenAddress: Address): string {
  return readSymbol(tokenAddress)
}
export function handlePoolCreated(event: PoolCreated): void {
  let token0 = new Token(event.params.token0.toHex())
  token0.symbol = fetchSymbol(event.params.token0)
  token0.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/common/token.ts", mapping);
        let rows = classify(&schema, &mappings);
        assert_eq!(class_of(&rows, "Token", "symbol"), Class::CallDerived);
        assert!(reason_of(&rows, "Token", "symbol").contains("contract state"));
    }

    const LOOP_SCHEMA: &str = r#"
type Token @entity {
  id: ID!
  derivedETH: BigDecimal!
}
type Pool @entity {
  id: ID!
  token1Price: BigDecimal!
  liquidity: BigInt!
}
"#;

    /// Jules on #1242. `has_loop && body.contains(".load(")` was body-wide, so a `.load()` sitting
    /// outside every loop still made the helper a fixed point and every field written from it
    /// non-reproducible. The class only reaches a field through a call, so the helper is called.
    #[test]
    fn a_load_outside_every_loop_is_not_a_fixed_point() {
        let mapping = r#"
export function sumAmounts(event: Mint): BigDecimal {
  const pool = Pool.load(event.address.toHex())
  let total = ZERO_BD
  for (let i = 0; i < event.params.amounts.length; ++i) {
    total = total.plus(event.params.amounts[i])
  }
  return total
}

export function handleMint(event: Mint): void {
  let token = new Token(event.params.token.toHex())
  token.derivedETH = sumAmounts(event)
  token.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(LOOP_SCHEMA, "src/pool.ts", mapping);
        let rows = classify(&schema, &mappings);
        assert_eq!(
            class_of(&rows, "Token", "derivedETH"),
            Class::Exact,
            "the load is outside the loop, so nothing depends on earlier-block state: {}",
            reason_of(&rows, "Token", "derivedETH")
        );
    }

    /// A loop that loads and writes, reading nothing off what it loaded, reproduces exactly.
    #[test]
    fn a_loop_that_loads_without_reading_a_field_is_not_a_fixed_point() {
        let mapping = r#"
export function countPools(event: Sync): BigDecimal {
  let n = ZERO_BD
  for (let i = 0; i < event.params.pools.length; ++i) {
    const pool = Pool.load(event.params.pools[i])
    if (pool) {
      n = n.plus(ONE_BD)
    }
  }
  return n
}

export function handleSync(event: Sync): void {
  let token = new Token(event.params.token.toHex())
  token.derivedETH = countPools(event)
  token.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(LOOP_SCHEMA, "src/pool.ts", mapping);
        let rows = classify(&schema, &mappings);
        assert_eq!(
            class_of(&rows, "Token", "derivedETH"),
            Class::Exact,
            "a loop that only loads and counts has no cross-block dependency: {}",
            reason_of(&rows, "Token", "derivedETH")
        );
    }

    /// And the guard must not have removed the class: the same shape with a read of the loaded
    /// entity's stored field is still a fixed point.
    #[test]
    fn a_loop_reading_a_loaded_entitys_field_is_still_a_fixed_point() {
        let mapping = r#"
export function priceFromPools(event: Sync): BigDecimal {
  let priceSoFar = ZERO_BD
  for (let i = 0; i < event.params.pools.length; ++i) {
    const pool = Pool.load(event.params.pools[i])
    if (pool) {
      priceSoFar = priceSoFar.plus(pool.token1Price)
    }
  }
  return priceSoFar
}

export function handleSync(event: Sync): void {
  let token = new Token(event.params.token.toHex())
  token.derivedETH = priceFromPools(event)
  token.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(LOOP_SCHEMA, "src/pool.ts", mapping);
        let rows = classify(&schema, &mappings);
        assert_eq!(
            class_of(&rows, "Token", "derivedETH"),
            Class::FixedPoint,
            "reading pool.token1Price inside the loop is the dependency: {}",
            reason_of(&rows, "Token", "derivedETH")
        );
    }

    #[test]
    fn find_eth_per_token_is_fixed_point() {
        let schema = r#"
type Token @entity {
  id: ID!
  derivedETH: BigDecimal!
  whitelistPools: [Pool!]!
}
type Pool @entity {
  id: ID!
  token0: Token!
  token1: Token!
  token0Price: BigDecimal!
  token1Price: BigDecimal!
  totalValueLockedToken0: BigDecimal!
  totalValueLockedToken1: BigDecimal!
  liquidity: BigInt!
}
type Bundle @entity {
  id: ID!
  ethPriceUSD: BigDecimal!
}
"#;
        let mapping = r#"
export function findEthPerToken(token: Token): BigDecimal {
  let whiteList = token.whitelistPools
  let priceSoFar = ZERO_BD
  for (let i = 0; i < whiteList.length; ++i) {
    const pool = Pool.load(whiteList[i])
    if (pool) {
      if (pool.token0 == token.id) {
        const token1 = Token.load(pool.token1)
        if (token1) {
          priceSoFar = pool.token1Price.times(token1.derivedETH as BigDecimal)
        }
      }
    }
  }
  return priceSoFar
}

export function handleSwap(event: SwapEvent): void {
  let token0 = Token.load(event.address.toHex())!
  token0.derivedETH = findEthPerToken(token0 as Token)
  token0.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/common/pricing.ts", mapping);
        let rows = classify(&schema, &mappings);
        assert_eq!(class_of(&rows, "Token", "derivedETH"), Class::FixedPoint);
        let reason = reason_of(&rows, "Token", "derivedETH");
        assert!(
            reason.contains("will not reproduce"),
            "fixed-point reason must say it will not reproduce, got {reason}"
        );
        assert!(reason.contains("pricing.ts") || reason.contains("findEthPerToken"));
    }

    #[test]
    fn assignment_reading_fixed_point_field_is_fixed_point() {
        let schema = r#"
type Token @entity { id: ID! derivedETH: BigDecimal! }
type Pool @entity { id: ID! usd: BigDecimal! }
"#;
        let mapping = r#"
export function findEthPerToken(token: Token): BigDecimal {
  let priceSoFar = ZERO_BD
  for (let i = 0; i < 1; ++i) {
    const pool = Pool.load(token.id)
    if (pool) {
      const other = Token.load(pool.id)
      if (other) {
        priceSoFar = other.derivedETH
      }
    }
  }
  return priceSoFar
}

export function handleSwap(event: SwapEvent): void {
  let token = Token.load(event.address.toHex())!
  token.derivedETH = findEthPerToken(token as Token)
  token.save()
  let pool = Pool.load(event.address.toHex())!
  pool.usd = token.derivedETH
  pool.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/common/pricing.ts", mapping);
        let rows = classify(&schema, &mappings);
        assert_eq!(class_of(&rows, "Token", "derivedETH"), Class::FixedPoint);
        assert_eq!(class_of(&rows, "Pool", "usd"), Class::FixedPoint);
    }

    /// Jules' counterexample on #1274: loading an entity and reading a field off it is not enough
    /// on its own. If the helper hands back a constant, the caller's field really is exact, and
    /// calling it fixed point costs a porter a hand check on a field that was never in doubt.
    #[test]
    fn a_helper_that_loads_but_returns_a_constant_stays_exact() {
        let schema = r#"
type Pool @entity {
  id: ID!
  token0Price: BigDecimal!
}
type Bundle @entity {
  id: ID!
  ethPriceUSD: BigDecimal!
}
"#;
        let mapping = r#"
export function getNativePriceInUSD(): BigDecimal {
  let pool = Pool.load(STABLE_POOL)
  if (pool) {
    log.info('price seen {}', [pool.token0Price.toString()])
  }
  return ZERO_BD
}

export function handleSwap(event: SwapEvent): void {
  let bundle = new Bundle('1')
  bundle.ethPriceUSD = getNativePriceInUSD()
  bundle.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/utils/pricing.ts", mapping);
        let rows = classify(&schema, &mappings);
        assert_eq!(
            class_of(&rows, "Bundle", "ethPriceUSD"),
            Class::Exact,
            "the loaded field never reaches the return; the helper hands back a constant"
        );
    }

    /// The #1274 case in the shape it was found in: two sibling helpers in one pricing module,
    /// one loading inside a loop and one not, both reading back stored entity output. Before the
    /// fix the loop one was fixed point and the point-load one was exact, which is a difference in
    /// how the mapping happens to be written rather than in what the field means.
    #[test]
    fn a_point_load_helper_is_classified_like_its_loop_loading_sibling() {
        let schema = r#"
type Pool @entity {
  id: ID!
  token0Price: BigDecimal!
  liquidity: BigInt!
}
type Token @entity {
  id: ID!
  derivedETH: BigDecimal!
}
type Bundle @entity {
  id: ID!
  ethPriceUSD: BigDecimal!
}
"#;
        // `findNativePerToken` loads inside a loop; `getNativePriceInUSD` does a single load and
        // reads a field off the result. Both read back stored output.
        let mapping = r#"
export function findNativePerToken(token: Token): BigDecimal {
  for (let i = 0; i < 1; ++i) {
    const pool = Pool.load(token.id)
    if (pool) { return pool.token0Price }
  }
  return ZERO_BD
}

export function getNativePriceInUSD(): BigDecimal {
  let pool = Pool.load(STABLE_POOL)
  if (pool) { return pool.token0Price }
  return ZERO_BD
}

export function handleSwap(event: SwapEvent): void {
  let token = Token.load(event.address.toHex())!
  token.derivedETH = findNativePerToken(token)
  token.save()
  let bundle = new Bundle('1')
  bundle.ethPriceUSD = getNativePriceInUSD()
  bundle.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/utils/pricing.ts", mapping);
        let rows = classify(&schema, &mappings);
        assert_eq!(
            class_of(&rows, "Token", "derivedETH"),
            Class::FixedPoint,
            "the loop-loading sibling was already correct"
        );
        assert_eq!(
            class_of(&rows, "Bundle", "ethPriceUSD"),
            Class::FixedPoint,
            "the point-loading sibling reads back the same stored output and must match it"
        );
    }

    /// **This assertion was flipped by #1274, and the flip is the point of that issue.**
    ///
    /// It used to assert `Exact`. `classes.md` defines fixed point as "reads back own or another
    /// entity's prior output", and `getEthPriceInUSD` returns `usdcPool.token1Price` - another
    /// entity's stored output - so the document and the test disagreed, and the test won by being
    /// executable. Running S1 against the real Uniswap V4 mainnet subgraph is what surfaced it: the
    /// identical shape one field over, `getNativePriceInUSD`, was reported exact while its sibling
    /// `findNativePerToken` was correctly fixed point, and the only difference between them is that
    /// one loads inside a loop and the other does not.
    ///
    /// The direction of the error is why this resolves toward fixed point rather than away.
    /// Over-classifying costs a porter a hand check on a field that was fine. Under-classifying
    /// tells them a field is byte-identical when it will not reproduce, which is the single
    /// experience RFC-0044 exists to prevent.
    #[test]
    fn a_price_read_back_off_a_loaded_pool_is_fixed_point() {
        let schema = r#"
type Pool @entity {
  id: ID!
  token0: Token!
  token1Price: BigDecimal!
  token0Price: BigDecimal!
}
type Token @entity { id: ID! }
type Bundle @entity {
  id: ID!
  ethPriceUSD: BigDecimal!
}
"#;
        let mapping = r#"
export function getEthPriceInUSD(): BigDecimal {
  let usdcPool = Pool.load(Address.fromString(stablePool))
  if (usdcPool) {
    if (usdcPool.token0 == Address.fromString(REFERENCE_TOKEN)) return usdcPool.token1Price
    else return usdcPool.token0Price
  }
  return ZERO_BD
}

export function handleSwap(event: SwapEvent): void {
  let bundle = new Bundle('1')
  bundle.ethPriceUSD = getEthPriceInUSD()
  bundle.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/common/pricing.ts", mapping);
        let rows = classify(&schema, &mappings);
        assert_eq!(
            class_of(&rows, "Bundle", "ethPriceUSD"),
            Class::FixedPoint,
            "the helper returns a price read back off a stored Pool, which classes.md calls \
             reading back another entity's prior output"
        );
    }

    #[test]
    fn derived_from_is_exact_with_schema_citation() {
        let schema = r#"
type Pool @entity {
  id: ID!
  swaps: [Swap!]! @derivedFrom(field: "pool")
}
type Swap @entity {
  id: ID!
  pool: Pool!
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/x.ts", "");
        let rows = classify(&schema, &mappings);
        assert_eq!(class_of(&rows, "Pool", "swaps"), Class::Exact);
        let row = rows.iter().find(|r| r.field == "swaps").unwrap();
        assert_eq!(row.citation.file, "schema.graphql");
        assert!(row.reason.contains("@derivedFrom"));
    }

    #[test]
    fn fulltext_is_unreachable() {
        let schema = r#"
type _Schema_
  @fulltext(
    name: "tokenSearch"
    language: en
    algorithm: rank
    include: [{ entity: "Token", fields: [{ name: "symbol" }] }]
  )

type Token @entity {
  id: ID!
  symbol: String!
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/x.ts", "");
        let rows = classify(&schema, &mappings);
        assert_eq!(
            class_of(&rows, "_Schema_", "tokenSearch"),
            Class::Unreachable
        );
        assert!(reason_of(&rows, "_Schema_", "tokenSearch").contains("@fulltext"));
        assert_eq!(class_of(&rows, "Token", "symbol"), Class::Unreachable);
        assert!(reason_of(&rows, "Token", "symbol").contains("no mapping writes"));
    }

    #[test]
    fn schema_underscore_does_not_swallow_the_next_entity() {
        let schema = r#"
type _Schema_
  @fulltext(
    name: "tokenSearch"
    language: en
    algorithm: rank
  )

type Bundle @entity {
  id: ID!
  ethPriceUSD: BigDecimal!
}

type Token @entity {
  id: ID!
  symbol: String!
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/x.ts", "");
        let rows = classify(&schema, &mappings);
        assert_eq!(class_of(&rows, "Bundle", "id"), Class::Unreachable);
        assert_eq!(class_of(&rows, "Bundle", "ethPriceUSD"), Class::Unreachable);
        assert_eq!(class_of(&rows, "Token", "symbol"), Class::Unreachable);
        assert_eq!(
            class_of(&rows, "_Schema_", "tokenSearch"),
            Class::Unreachable
        );
    }

    #[test]
    fn fold_of_own_field_with_event_stays_exact() {
        let schema = r#"
type Pool @entity {
  id: ID!
  totalValueLockedToken0: BigDecimal!
}
"#;
        let mapping = r#"
export function handleSwap(event: SwapEvent): void {
  let pool = Pool.load(event.address.toHex())!
  pool.totalValueLockedToken0 = pool.totalValueLockedToken0.plus(event.params.amount0)
  pool.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/core.ts", mapping);
        let rows = classify(&schema, &mappings);
        assert_eq!(
            class_of(&rows, "Pool", "totalValueLockedToken0"),
            Class::Exact
        );
    }

    #[test]
    fn worst_class_wins_across_writers() {
        let schema = r#"
type Token @entity {
  id: ID!
  derivedETH: BigDecimal!
}
type Pool @entity {
  id: ID!
  token1: Token!
  token1Price: BigDecimal!
}
"#;
        let mapping = r#"
export function findEthPerToken(token: Token): BigDecimal {
  for (let i = 0; i < 1; ++i) {
    const pool = Pool.load(token.id)
    const token1 = Token.load(pool.token1)
    return pool.token1Price.times(token1.derivedETH)
  }
  return ZERO_BD
}
export function handlePoolCreated(event: PoolCreated): void {
  let token0 = new Token(event.params.token0.toHex())
  token0.derivedETH = ZERO_BD
  token0.save()
}
export function handleSwap(event: SwapEvent): void {
  let token0 = Token.load(event.address.toHex())!
  token0.derivedETH = findEthPerToken(token0)
  token0.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/pricing.ts", mapping);
        let rows = classify(&schema, &mappings);
        assert_eq!(class_of(&rows, "Token", "derivedETH"), Class::FixedPoint);
    }

    #[test]
    fn report_names_every_schema_field() {
        let schema = r#"
type Token @entity {
  id: ID!
  symbol: String!
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/x.ts", "");
        let rows = classify(&schema, &mappings);
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn render_says_non_exact_will_not_reproduce() {
        let schema = r#"
type Token @entity { id: ID! leftover: String! }
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/x.ts", "");
        let rows = classify(&schema, &mappings);
        let report = Report {
            source: "fixture".into(),
            fields: rows,
            missing_handlers: Vec::new(),
            mapping_source: true,
        };
        let text = render_report(&report);
        assert!(text.contains("will not reproduce"));
        assert!(text.contains("--from-subgraph"));
        assert!(text.contains("watch"));
        assert!(text.contains("service_u_r_i_update"));
        assert!(!text.contains("nuthatch init 0x") || text.contains("not"));
    }

    /// Jules on #1242. GraphQL puts no meaning on newlines. Splitting the body by line and taking
    /// the first `name:` from each recorded `id` and silently dropped every field beside it, so the
    /// report told the author about fields that were not there and never mentioned ones that were.
    #[test]
    fn a_compact_entity_declaration_keeps_every_field() {
        let schema = parse_schema(
            "type Token @entity { id: ID! derivedETH: BigDecimal! symbol: String! }\n",
        )
        .unwrap();
        let token = schema.entities.iter().find(|e| e.name == "Token").unwrap();
        let names: Vec<&str> = token.fields.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, ["id", "derivedETH", "symbol"], "{names:?}");
    }

    /// A colon inside a directive's arguments or a list type is not a field boundary, and a `#`
    /// comment is not a field at all.
    #[test]
    fn a_directive_argument_colon_is_not_a_field() {
        let schema = parse_schema(
            "type Pool @entity {\n  id: ID!\n  # ignored: NotAField\n  ticks: [Tick!]! \n               swaps: [Swap!]! @derivedFrom(field: \"pool\")\n  fee: BigInt!\n}\n",
        )
        .unwrap();
        let pool = schema.entities.iter().find(|e| e.name == "Pool").unwrap();
        let names: Vec<&str> = pool.fields.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, ["id", "ticks", "swaps", "fee"], "{names:?}");
        let swaps = pool.fields.iter().find(|f| f.name == "swaps").unwrap();
        assert_eq!(
            swaps.derived_from.as_deref(),
            Some("pool"),
            "the directive must still attach to its field"
        );
    }

    /// The citation is the line the field is on, and a compact declaration puts several on one.
    #[test]
    fn a_field_citation_is_its_own_line() {
        let schema =
            parse_schema("\n\ntype Token @entity {\n  id: ID!\n  symbol: String!\n}\n").unwrap();
        let token = schema.entities.iter().find(|e| e.name == "Token").unwrap();
        let line_of_field = |n: &str| token.fields.iter().find(|f| f.name == n).unwrap().line;
        assert_eq!(line_of_field("id"), 4, "id is on line 4");
        assert_eq!(line_of_field("symbol"), 5, "symbol is on line 5");
    }

    #[test]
    fn schema_parse_rejects_empty() {
        assert!(parse_schema("enum Foo { A }").is_err());
    }

    #[test]
    fn mapping_call_signature_cites_the_try_line() {
        let schema = r#"
type Token @entity {
  id: ID!
  symbol: String!
}
"#;
        let mapping = r#"
export function fetchTokenSymbol(tokenAddress: Address): string {
  let contract = ERC20.bind(tokenAddress)
  let result = contract.try_symbol()
  if (!result.reverted) {
    return result.value
  }
  return 'unknown'
}

export function handlePoolCreated(event: PoolCreated): void {
  let token0 = new Token(event.params.token0.toHex())
  token0.symbol = fetchTokenSymbol(event.params.token0)
  token0.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/common/token.ts", mapping);
        let rows = classify(&schema, &mappings);
        let calls = calls_from_mappings(
            &Report {
                source: "t".into(),
                fields: rows,
                missing_handlers: Vec::new(),
                mapping_source: true,
            },
            &mappings,
        );
        assert_eq!(calls.len(), 1, "{calls:?}");
        assert_eq!(calls[0].signature, "symbol()");
        assert_eq!(calls[0].contract_arg, "event.params.token0");
        assert_eq!(calls[0].citation.file, "src/common/token.ts");
        assert_eq!(
            calls[0].citation.line, 4,
            "try_symbol is line 4 of the mapping"
        );
        assert_eq!(calls[0].handler, "handlePoolCreated");
    }

    #[test]
    fn helper_with_two_reads_emits_the_returned_method() {
        let schema = r#"
type Token @entity {
  id: ID!
  decimals: BigInt!
}
"#;
        let mapping = r#"
export function fetchTokenDecimals(tokenAddress: Address): BigInt {
  let contract = ERC20.bind(tokenAddress)
  let _sym = contract.try_symbol()
  return contract.try_decimals().value
}

export function handlePoolCreated(event: PoolCreated): void {
  let token0 = new Token(event.params.token0.toHex())
  token0.decimals = fetchTokenDecimals(event.params.token0)
  token0.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/common/token.ts", mapping);
        let rows = classify(&schema, &mappings);
        let calls = calls_from_mappings(
            &Report {
                source: "t".into(),
                fields: rows,
                missing_handlers: Vec::new(),
                mapping_source: true,
            },
            &mappings,
        );
        assert_eq!(calls.len(), 1, "{calls:?}");
        assert_eq!(
            calls[0].signature, "decimals()",
            "the returned try_decimals must win over the earlier try_symbol: {calls:?}"
        );
        assert_eq!(calls[0].contract_arg, "event.params.token0");
        assert_eq!(
            calls[0].citation.line, 5,
            "must cite try_decimals, not try_symbol, got {}",
            calls[0].citation.line
        );
    }

    #[test]
    fn two_inline_reads_attribute_each_field_to_its_try() {
        let schema = r#"
type Token @entity {
  id: ID!
  symbol: String!
  decimals: BigInt!
}
"#;
        let mapping = r#"
export function handlePoolCreated(event: PoolCreated): void {
  let token0 = new Token(event.params.token0.toHex())
  let contract = ERC20.bind(event.params.token0)
  token0.symbol = contract.try_symbol().value
  token0.decimals = contract.try_decimals().value
  token0.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/mappings/core.ts", mapping);
        let rows = classify(&schema, &mappings);
        let calls = calls_from_mappings(
            &Report {
                source: "t".into(),
                fields: rows,
                missing_handlers: Vec::new(),
                mapping_source: true,
            },
            &mappings,
        );
        let sig = |field: &str| {
            calls
                .iter()
                .find(|c| c.field == field)
                .map(|c| c.signature.as_str())
                .unwrap_or("missing")
        };
        assert_eq!(sig("symbol"), "symbol()", "{calls:?}");
        assert_eq!(sig("decimals"), "decimals()", "{calls:?}");
    }

    #[test]
    fn mapping_call_vanishes_when_the_try_is_dropped() {
        let schema = r#"
type Token @entity {
  id: ID!
  symbol: String!
}
"#;
        let mapping = r#"
export function fetchTokenSymbol(tokenAddress: Address): string {
  return 'unknown'
}

export function handlePoolCreated(event: PoolCreated): void {
  let token0 = new Token(event.params.token0.toHex())
  token0.symbol = fetchTokenSymbol(event.params.token0)
  token0.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/common/token.ts", mapping);
        let rows = classify(&schema, &mappings);
        assert_eq!(class_of(&rows, "Token", "symbol"), Class::Exact);
        let calls = calls_from_mappings(
            &Report {
                source: "t".into(),
                fields: rows,
                missing_handlers: Vec::new(),
                mapping_source: true,
            },
            &mappings,
        );
        assert!(
            calls.is_empty(),
            "dropping try_symbol must not invent a [[calls]]: {calls:?}"
        );
    }

    #[test]
    fn citation_is_the_assignment_line() {
        let schema = r#"
type Swap @entity {
  id: ID!
  amount0: BigDecimal!
}
"#;
        let mapping = "export function handleSwap(event: SwapEvent): void {\n  let swap = new Swap('x')\n  swap.amount0 = event.params.amount0\n}\n";
        let (schema, mappings) = schema_and_mappings(schema, "src/core.ts", mapping);
        let rows = classify(&schema, &mappings);
        let row = rows.iter().find(|r| r.field == "amount0").unwrap();
        assert_eq!(row.citation.file, "src/core.ts");
        assert_eq!(row.citation.line, 3);
    }
    /// A contract bound to a local, read on a later line, is a contract read.
    ///
    /// `expr_has_contract_call` only looks at the expression it is given, and the commonest shape puts
    /// the `bind` on its own line:
    ///
    /// ```ts
    /// let contract = ERC20Contract.bind(id)
    /// token.symbol = contract.symbol()
    /// ```
    ///
    /// So the RHS carried no marker and the field classified **exact** - a promise of byte-for-byte
    /// output, with the contract call printed in the report's own Why column. Every Uniswap fixture
    /// wrote `contract.try_symbol()` inline, so no test could see it. Carbon read 95 exact and 0
    /// call-derived when the truth was 92 and 3 (#1296).
    #[test]
    fn a_read_through_a_bound_contract_local_is_call_derived() {
        let schema = r#"
type Token @entity {
  id: ID!
  symbol: String!
  createdAt: BigInt!
  label: String!
  other: String!
}
"#;
        let mapping = r#"
function describe(id: Address): Wrapper {
  return new Wrapper(id)
}

export function handleCreated(event: TokenCreated): void {
  let contract = ERC20Contract.bind(event.params.token)
  let notAContract = describe(event.params.token)
  let myContract = describe(event.params.token)
  let token = new Token(event.params.token.toHex())
  token.symbol = contract.symbol()
  token.createdAt = event.block.timestamp
  token.label = notAContract.label()
  token.other = myContract.other()
  token.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/utils.ts", mapping);
        let rows = classify(&schema, &mappings);

        assert_eq!(
            class_of(&rows, "Token", "symbol"),
            Class::CallDerived,
            "a read off a bound contract is a contract read: {}",
            reason_of(&rows, "Token", "symbol")
        );
        // #1242: the citation is the line that decided the class, which is the `bind`.
        assert!(
            reason_of(&rows, "Token", "symbol").contains("contract state"),
            "{}",
            reason_of(&rows, "Token", "symbol")
        );

        // The rule must not swallow every method call on every local.
        assert_eq!(
            class_of(&rows, "Token", "label"),
            Class::Exact,
            "`notAContract` is not bound to a contract: {}",
            reason_of(&rows, "Token", "label")
        );
        // And a bound local named `contract` must not match `myContract`, which merely ends in it.
        assert_eq!(
            class_of(&rows, "Token", "other"),
            Class::Exact,
            "`myContract` is a different identifier from `contract`: {}",
            reason_of(&rows, "Token", "other")
        );
        assert_eq!(class_of(&rows, "Token", "createdAt"), Class::Exact);
    }

    /// `f(x).id` is `x`, whatever `f` reads from a contract.
    ///
    /// `bancorprotocol/carbon-subgraph` - the pinned drop-in target (#1284) - writes
    /// `newTrade.sourceToken = findOrCreateToken(event.params.sourceToken, ...).id`, and
    /// `findOrCreateToken` loads or constructs its `Token` from that same argument. The contract reads
    /// inside it populate `symbol`, `name` and `decimals`: sibling fields of a different entity. They
    /// have no bearing on this field's value.
    ///
    /// The classifier applied *does the callee read contract state* when the rule is *does this field's
    /// value depend on contract state*, and called both token references call-derived. That the rule was
    /// accidental rather than intended is visible in the same handler: `newTrade.trader` is the identical
    /// shape through `findOrCreateUser`, and classifies exact only because that helper makes no call
    /// (#1294).
    #[test]
    fn the_id_of_an_entity_constructed_from_an_argument_is_exact() {
        let schema = r#"
type Trade @entity {
  id: ID!
  sourceToken: Token!
  trader: User!
  feeAmount: BigInt!
}
type Token @entity {
  id: ID!
  symbol: String!
}
type User @entity {
  id: ID!
}
"#;
        let mapping = r#"
function createERC20Token(id: Address, timestamp: BigInt): Token {
  let contract = ERC20Contract.bind(id)
  let token = new Token(id)
  token.symbol = contract.symbol()
  token.save()
  return token
}

export function findOrCreateToken(id: Address, timestamp: BigInt): Token {
  let token = Token.load(id)
  if (token != null) {
    return token
  }
  return createERC20Token(id, timestamp)
}

export function findOrCreateUser(id: Address, timestamp: BigInt): User {
  let user = new User(id)
  user.save()
  return user
}

export function handleTokensTraded(event: TokensTraded): void {
  let newTrade = new Trade(event.transaction.hash.toHex())
  newTrade.trader = findOrCreateUser(event.params.trader, event.block.timestamp).id
  newTrade.sourceToken = findOrCreateToken(
    event.params.sourceToken,
    event.block.timestamp
  ).id
  newTrade.feeAmount = event.params.tradingFeeAmount
  newTrade.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/utils.ts", mapping);
        let rows = classify(&schema, &mappings);

        // The field under test, and the control that already passed for the wrong reason.
        assert_eq!(
            class_of(&rows, "Trade", "sourceToken"),
            Class::Exact,
            "`findOrCreateToken(x).id` is `x`: {}",
            reason_of(&rows, "Trade", "sourceToken")
        );
        assert_eq!(
            class_of(&rows, "Trade", "trader"),
            Class::Exact,
            "the identical shape through a helper that makes no call"
        );
        assert_eq!(class_of(&rows, "Trade", "feeAmount"), Class::Exact);

        // And the sibling the contract read actually decides is still call-derived, so the fix has not
        // simply stopped the rule firing.
        assert_eq!(
            class_of(&rows, "Token", "symbol"),
            Class::CallDerived,
            "the field the contract read does decide must keep its class"
        );
    }

    /// The other half: a returned entity whose id is **not** an argument stays call-derived.
    ///
    /// Without this the fix would be "stop inheriting on `.id`", which would promise byte-identical
    /// output for a value read off a contract.
    #[test]
    fn the_id_of_an_entity_keyed_by_a_contract_read_stays_call_derived() {
        let schema = r#"
type Trade @entity {
  id: ID!
  pool: Pool!
}
type Pool @entity {
  id: ID!
}
"#;
        let mapping = r#"
export function findOrCreatePool(addr: Address): Pool {
  let contract = Factory.bind(addr)
  let pool = new Pool(contract.getPool(addr).toHex())
  pool.save()
  return pool
}

export function handleTokensTraded(event: TokensTraded): void {
  let newTrade = new Trade(event.transaction.hash.toHex())
  newTrade.pool = findOrCreatePool(event.params.pool).id
  newTrade.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/utils.ts", mapping);
        let rows = classify(&schema, &mappings);
        assert_eq!(
            class_of(&rows, "Trade", "pool"),
            Class::CallDerived,
            "the id itself came off a contract: {}",
            reason_of(&rows, "Trade", "pool")
        );
    }

    /// A callee that keys by a parameter on one branch and by a **contract read** on another stays
    /// call-derived.
    ///
    /// The disqualifying test used `expr_has_contract_call`, which cannot see a contract bound to a local
    /// on an earlier line - the same blindness as #1296 - so a mutation removing it survived. The id rule
    /// and the class rule now ask the same question.
    #[test]
    fn a_callee_that_keys_by_a_contract_read_on_any_branch_stays_call_derived() {
        let schema = r#"
type Trade @entity {
  id: ID!
  token: Token!
}
type Token @entity {
  id: ID!
}
"#;
        let mapping = r#"
export function findOrCreateToken(id: Address): Token {
  let token = Token.load(id)
  if (token != null) {
    return token
  }
  let contract = Registry.bind(id)
  return new Token(contract.canonical(id))
}

export function handleTokensTraded(event: TokensTraded): void {
  let newTrade = new Trade(event.transaction.hash.toHex())
  newTrade.token = findOrCreateToken(event.params.sourceToken).id
  newTrade.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/utils.ts", mapping);
        let rows = classify(&schema, &mappings);
        assert_eq!(
            class_of(&rows, "Trade", "token"),
            Class::CallDerived,
            "one branch keys the entity by a contract read, so the id is not the argument: {}",
            reason_of(&rows, "Trade", "token")
        );
    }

    /// A helper's return is provenance only once it is put in the caller's terms (Jules, #1298).
    ///
    /// ```ts
    /// function makeToken(key: string): Token { return new Token(key) }
    /// function findToken(id: Address): Token { return makeToken(contract.canonical(id)) }
    /// trade.token = findToken(event.params.sourceToken).id
    /// ```
    ///
    /// Recursing into `makeToken` and taking its return expressions verbatim yielded `key`. Checked
    /// against `findToken`'s own locals that looks contract-free, so the `.id` exemption was granted to an
    /// id that came straight off `contract.canonical(..)`.
    #[test]
    fn a_helpers_return_carries_the_callers_arguments() {
        let schema = r#"
type Trade @entity {
  id: ID!
  token: Token!
}
type Token @entity {
  id: ID!
}
"#;
        let mapping = r#"
export function makeToken(key: string): Token {
  return new Token(key)
}

export function findToken(id: Address): Token {
  let contract = Registry.bind(id)
  return makeToken(contract.canonical(id))
}

export function handleTokensTraded(event: TokensTraded): void {
  let newTrade = new Trade(event.transaction.hash.toHex())
  newTrade.token = findToken(event.params.sourceToken).id
  newTrade.save()
}
"#;
        let (parsed, mappings) = schema_and_mappings(schema, "src/utils.ts", mapping);
        let rows = classify(&parsed, &mappings);
        assert_eq!(
            class_of(&rows, "Trade", "token"),
            Class::CallDerived,
            "the id the helper returns is `contract.canonical(id)`, whatever the inner function calls \
             its parameter: {}",
            reason_of(&rows, "Trade", "token")
        );

        // The same shape with a contract-free argument is still exact, so the rule is about provenance
        // rather than about there being a helper call at all.
        let clean = mapping.replace("contract.canonical(id)", "id.toHexString()");
        assert_ne!(clean, mapping, "the fixture edit must apply");
        let (parsed, mappings) = schema_and_mappings(schema, "src/utils.ts", &clean);
        let rows = classify(&parsed, &mappings);
        assert_eq!(
            class_of(&rows, "Trade", "token"),
            Class::Exact,
            "an argument that reads no contract keeps the exemption: {}",
            reason_of(&rows, "Trade", "token")
        );
    }

    /// The `.id` exemption is for a call and `.id`, nothing else (Jules, #1298).
    ///
    /// A prefix-and-suffix test accepted anything starting with the call and ending in `.id`, so
    /// `findToken(id).id.owner.id` - whose value is some other entity's id - carried the exemption on the
    /// strength of its two ends.
    #[test]
    fn the_id_exemption_needs_the_whole_expression() {
        assert!(is_id_of_call("findToken(id).id", "findToken"));
        assert!(
            is_id_of_call("findToken(f(a, b)).id", "findToken"),
            "a nested call inside the arguments is still one call"
        );
        assert!(
            !is_id_of_call("findToken(id).id.owner.id", "findToken"),
            "the value is the owner's id, not the returned entity's"
        );
        assert!(
            !is_id_of_call("findToken(id).id.toHexString()", "findToken"),
            "anything taken off the id is no longer the id"
        );
        assert!(
            !is_id_of_call("findToken(id).symbol", "findToken"),
            "another field of the returned entity is not its id"
        );
        assert!(
            !is_id_of_call("findTokenOrCreate(id).id", "findToken"),
            "the callee has to be the whole name"
        );
        assert!(
            !is_id_of_call("wrap(findToken(id)).id", "findToken"),
            "the call has to be the head of the expression"
        );
    }

    /// A wrapped declaration resolves whole through the alias chain too (Jules, #1298).
    ///
    /// ```ts
    /// let key =
    ///   contract.canonical(id)
    /// let finalKey = key
    /// return new Token(finalKey)
    /// ```
    ///
    /// `resolve_local_chain` read raw lines, so `let key =` recorded an **empty** right-hand side and the
    /// contract read vanished - the id then looked contract-free and carried the `.id` exemption. Three
    /// rules here read declarations and they now share one representation of what a declaration is.
    #[test]
    fn a_wrapped_declaration_resolves_through_the_alias_chain() {
        let schema = r#"
type Trade @entity {
  id: ID!
  token: Token!
}
type Token @entity {
  id: ID!
}
"#;
        let mapping = r#"
export function findOrCreateToken(id: Address): Token {
  let contract = Registry.bind(id)
  let key =
    contract.canonical(id)
  let finalKey = key
  return new Token(finalKey)
}

export function handleTokensTraded(event: TokensTraded): void {
  let newTrade = new Trade(event.transaction.hash.toHex())
  newTrade.token = findOrCreateToken(event.params.sourceToken).id
  newTrade.save()
}
"#;
        let (parsed, mappings) = schema_and_mappings(schema, "src/utils.ts", mapping);
        let rows = classify(&parsed, &mappings);
        assert_eq!(
            class_of(&rows, "Trade", "token"),
            Class::CallDerived,
            "the id is `contract.canonical(id)` however the declaration was wrapped: {}",
            reason_of(&rows, "Trade", "token")
        );

        // Wrapped and contract-free is still exact, so this is about the read and not about the wrapping.
        let clean = mapping.replace("contract.canonical(id)", "id.toHexString()");
        assert_ne!(clean, mapping, "the fixture edit must apply");
        let (parsed, mappings) = schema_and_mappings(schema, "src/utils.ts", &clean);
        let rows = classify(&parsed, &mappings);
        assert_eq!(
            class_of(&rows, "Trade", "token"),
            Class::Exact,
            "a wrapped declaration of a contract-free id keeps the exemption: {}",
            reason_of(&rows, "Trade", "token")
        );
    }

    /// A parsed body carries no comments, which is what several rules here quietly rely on.
    ///
    /// `parse_functions` strips the source before building any `FunctionInfo`, so every line-oriented rule
    /// in this module - alias matching, the terminator scan, brace depth - sees code only. Asserted rather
    /// than assumed, because a reviewer reasonably read `bound_contract_locals` as vulnerable to a
    /// trailing comment and the answer is not visible from that function.
    #[test]
    fn a_body_carries_no_comments() {
        let mapping = r#"
export function handleTransfer(event: Transfer): void {
  let c = ERC20.bind(event.params.token) // a trailing comment
  /* and a block one */
  let alias = c // another
  alias.symbol()
}
"#;
        let (_schema, mappings) = schema_and_mappings(
            "type Token @entity { id: ID! }
",
            "src/token.ts",
            mapping,
        );
        let body = &mappings
            .functions
            .get("handleTransfer")
            .expect("handler")
            .body;
        assert!(
            !body.contains("//") && !body.contains("/*"),
            "a comment reached a function body, and every line rule in this module assumes none do:\n\
             {body}"
        );
    }

    /// An alias of a bound contract is a bound contract (Jules, #1298).
    ///
    /// `bound_contract_locals` recorded only the name assigned directly from `.bind(`, so
    /// `let c = ERC20.bind(id); let alias = c` left `alias` unrecorded and `alias.symbol()` read as an
    /// ordinary method call on nothing in particular.
    #[test]
    fn an_alias_of_a_bound_contract_is_still_a_contract() {
        let schema = r#"
type Token @entity {
  id: ID!
  symbol: String!
}
"#;
        // Two hops, so a one-step alias rule would still miss the second.
        let mapping = r#"
export function handleTransfer(event: Transfer): void {
  let c =
    ERC20.bind(event.params.token)
  let alias = c // keep this handle
  let second = alias
  let token = new Token(event.params.token.toHexString())
  token.symbol = second.symbol()
  token.save()
}
"#;
        let (parsed, mappings) = schema_and_mappings(schema, "src/token.ts", mapping);
        let rows = classify(&parsed, &mappings);
        assert_eq!(
            class_of(&rows, "Token", "symbol"),
            Class::CallDerived,
            "a copy of a bound contract still reads the contract: {}",
            reason_of(&rows, "Token", "symbol")
        );
    }

    /// The inner helper's own contract binding travels with its return (Jules, #1298).
    ///
    /// ```ts
    /// function makeToken(id: Address): Token {
    ///   let c = ERC20.bind(id)
    ///   return new Token(c.canonical(id))
    /// }
    /// function findToken(id: Address): Token { return makeToken(id) }
    /// ```
    ///
    /// Substituting the arguments produced `c.canonical(id)` in the *outer* context, where nothing binds
    /// `c` - so it read as an ordinary method call and the `.id` exemption was granted to an id that comes
    /// off contract state. The inner body is where `c` means a contract, so that is where it is asked.
    #[test]
    fn an_inner_helpers_contract_binding_is_not_lost_in_the_caller() {
        let schema = r#"
type Trade @entity {
  id: ID!
  token: Token!
}
type Token @entity {
  id: ID!
}
"#;
        let mapping = r#"
export function makeToken(id: Address): Token {
  let c = ERC20.bind(id)
  return new Token(c.canonical(id))
}

export function findToken(id: Address): Token {
  return makeToken(id)
}

export function handleTokensTraded(event: TokensTraded): void {
  let newTrade = new Trade(event.transaction.hash.toHex())
  newTrade.token = findToken(event.params.sourceToken).id
  newTrade.save()
}
"#;
        let (parsed, mappings) = schema_and_mappings(schema, "src/utils.ts", mapping);
        let rows = classify(&parsed, &mappings);
        assert_eq!(
            class_of(&rows, "Trade", "token"),
            Class::CallDerived,
            "the id is `c.canonical(id)` and `c` is a bound contract, two calls down: {}",
            reason_of(&rows, "Trade", "token")
        );

        // The same nesting without the contract read stays exact, so this is about provenance rather than
        // about the depth of the call chain.
        let clean = mapping.replace(
            "let c = ERC20.bind(id)
  return new Token(c.canonical(id))",
            "return new Token(id.toHexString())",
        );
        assert_ne!(clean, mapping, "the fixture edit must apply");
        let (parsed, mappings) = schema_and_mappings(schema, "src/utils.ts", &clean);
        let rows = classify(&parsed, &mappings);
        assert_eq!(
            class_of(&rows, "Trade", "token"),
            Class::Exact,
            "two hops through contract-free helpers keep the exemption: {}",
            reason_of(&rows, "Trade", "token")
        );
    }

    /// A helper called with too few arguments leaves a parameter that resolves against the caller.
    ///
    /// `makeToken(key, salt)` returns `new Token(salt)`. Called with one argument, `salt` survives
    /// substitution and is then looked up in the *caller*, which happens to bind a contract-free local of
    /// that name - so an id that is really the helper's own second parameter reads as exact.
    #[test]
    fn a_helper_called_with_too_few_arguments_keeps_no_provenance() {
        let schema = r#"
type Trade @entity {
  id: ID!
  token: Token!
}
type Token @entity {
  id: ID!
}
"#;
        let mapping = r#"
export function makeToken(key: string, salt: string): Token {
  return new Token(salt)
}

export function findToken(id: Address): Token {
  let salt = id.toHexString()
  let contract = Registry.bind(id)
  return makeToken(contract.canonical(id))
}

export function handleTokensTraded(event: TokensTraded): void {
  let newTrade = new Trade(event.transaction.hash.toHex())
  newTrade.token = findToken(event.params.sourceToken).id
  newTrade.save()
}
"#;
        let (parsed, mappings) = schema_and_mappings(schema, "src/utils.ts", mapping);
        let rows = classify(&parsed, &mappings);
        assert_eq!(
            class_of(&rows, "Trade", "token"),
            Class::CallDerived,
            "`salt` was never passed, and the caller's local of that name is not what the helper meant: \
             {}",
            reason_of(&rows, "Trade", "token")
        );
    }

    /// An unrelated parameter-keyed entity must not vouch for the one the function returns.
    ///
    /// Jules's counterexample on #1298, verbatim: the helper constructs `new Audit(id)` - parameter-keyed
    /// and contract-free - binds a contract, puts the read in a local, and returns `new Token(key)`. The
    /// body-wide scan saw `"id"` among the ids and granted the exemption, so `helper(x).id` reported exact
    /// for a value that does come off a contract.
    ///
    /// Two things it needs: the ids have to come from the **returned** entity, and a bare local id has to
    /// be resolved to what it holds, or `key` looks contract-free on its face.
    #[test]
    fn an_unrelated_parameter_keyed_entity_does_not_vouch_for_the_returned_one() {
        let schema = r#"
type Trade @entity {
  id: ID!
  token: Token!
}
type Token @entity {
  id: ID!
}
type Audit @entity {
  id: ID!
}
"#;
        let mapping = r#"
export function findOrCreateToken(id: Address): Token {
  let audit = new Audit(id)
  audit.save()
  let contract = Registry.bind(id)
  let key = contract.canonical(id)
  return new Token(key)
}

export function handleTokensTraded(event: TokensTraded): void {
  let newTrade = new Trade(event.transaction.hash.toHex())
  newTrade.token = findOrCreateToken(event.params.sourceToken).id
  newTrade.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/utils.ts", mapping);
        let rows = classify(&schema, &mappings);
        assert_eq!(
            class_of(&rows, "Trade", "token"),
            Class::CallDerived,
            "the returned Token's id came off a contract, whatever the Audit's id did: {}",
            reason_of(&rows, "Trade", "token")
        );
    }

    /// A return path the resolver cannot follow refuses the exemption rather than guessing.
    #[test]
    fn an_unresolvable_return_path_refuses_the_exemption() {
        let schema = r#"
type Trade @entity {
  id: ID!
  token: Token!
}
type Token @entity {
  id: ID!
}
"#;
        let mapping = r#"
export function findOrCreateToken(id: Address): Token {
  let contract = Registry.bind(id)
  return tokenFromSomewhereElse(contract, id)
}

export function handleTokensTraded(event: TokensTraded): void {
  let newTrade = new Trade(event.transaction.hash.toHex())
  newTrade.token = findOrCreateToken(event.params.sourceToken).id
  newTrade.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/utils.ts", mapping);
        let rows = classify(&schema, &mappings);
        assert_eq!(
            class_of(&rows, "Trade", "token"),
            Class::CallDerived,
            "an unknown helper is unresolvable, so the exemption must not apply: {}",
            reason_of(&rows, "Trade", "token")
        );
    }

    /// An alias chain is followed to its end, not one step.
    ///
    /// Jules's second counterexample on #1298, verbatim: `let key = contract.canonical(id)` then
    /// `let finalKey = key` then `return new Token(finalKey)`. Resolving one step gave `key`, which reads
    /// as contract-free on its face, so the `.id` exemption was granted to an id that did come off a
    /// contract. The resolver now follows the chain to `contract.canonical(id)`.
    #[test]
    fn an_alias_chain_is_followed_to_its_end() {
        let schema = r#"
type Trade @entity {
  id: ID!
  token: Token!
}
type Token @entity {
  id: ID!
}
"#;
        let mapping = r#"
export function findOrCreateToken(id: Address): Token {
  let contract = Registry.bind(id)
  let key = contract.canonical(id)
  let finalKey = key
  return new Token(finalKey)
}

export function handleTokensTraded(event: TokensTraded): void {
  let newTrade = new Trade(event.transaction.hash.toHex())
  newTrade.token = findOrCreateToken(event.params.sourceToken).id
  newTrade.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/utils.ts", mapping);
        let rows = classify(&schema, &mappings);
        assert_eq!(
            class_of(&rows, "Trade", "token"),
            Class::CallDerived,
            "two aliases from the contract read, and it is still a contract read: {}",
            reason_of(&rows, "Trade", "token")
        );
    }

    /// A cycle terminates and refuses, rather than spinning.
    #[test]
    fn an_alias_cycle_refuses_the_exemption() {
        let schema = r#"
type Trade @entity {
  id: ID!
  token: Token!
}
type Token @entity {
  id: ID!
}
"#;
        let mapping = r#"
export function findOrCreateToken(id: Address): Token {
  let a = b
  let b = a
  return new Token(a)
}

export function handleTokensTraded(event: TokensTraded): void {
  let newTrade = new Trade(event.transaction.hash.toHex())
  newTrade.token = findOrCreateToken(event.params.sourceToken).id
  newTrade.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/utils.ts", mapping);
        let rows = classify(&schema, &mappings);
        // The class itself is not the point - terminating is. `findOrCreateToken` makes no contract call
        // here, so the field is exact either way; what must not happen is a hang.
        let _ = class_of(&rows, "Trade", "token");
    }

    /// A name that is neither a local nor a parameter refuses the exemption.
    ///
    /// `new Token(SOME_MODULE_CONST)` is very likely contract-free, and we cannot see the declaration to
    /// know it. Refusing under-promises, which costs a porter a hand-check; granting it on a guess would
    /// promise byte-for-byte output for an id we never resolved. A mutation treating an unresolved name as
    /// the resolved expression survived until this existed.
    #[test]
    fn an_unresolvable_name_refuses_the_exemption() {
        let schema = r#"
type Trade @entity {
  id: ID!
  token: Token!
}
type Token @entity {
  id: ID!
}
"#;
        let mapping = r#"
export function findOrCreateToken(id: Address): Token {
  let contract = Registry.bind(id)
  contract.touch(id)
  return new Token(SOME_MODULE_CONST)
}

export function handleTokensTraded(event: TokensTraded): void {
  let newTrade = new Trade(event.transaction.hash.toHex())
  newTrade.token = findOrCreateToken(event.params.sourceToken).id
  newTrade.save()
}
"#;
        let (schema, mappings) = schema_and_mappings(schema, "src/utils.ts", mapping);
        let rows = classify(&schema, &mappings);
        assert_eq!(
            class_of(&rows, "Trade", "token"),
            Class::CallDerived,
            "the id was never resolved, so the callee's class stands: {}",
            reason_of(&rows, "Trade", "token")
        );
    }
}
