//! #769 - a documented command is a claim, and claims want a checker rather than a good memory
//! (`scripts/version-check.sh`'s line, generalised from version strings to commands and flags).
//!
//! `public/llms.txt` told coding agents to run `nuthatch roost` for five releases after the
//! subcommand was removed in 2.0. This walks every `nuthatch ...` invocation written in backticks
//! across the doc set and resolves it against clap's own command tree (`Cli::command()`, the same
//! ground truth `src/skill.rs::generate_cli_reference` renders from) - not a hand-kept list, which
//! would just be `CONFIG_SOURCES` again with the same failure mode.
//!
//! Scoped to commands and flags only, per the issue - not prose, not numbers (#767's problem).
//!
//! Deliberately anchored on backtick-formatted invocations, not bare mentions of the word
//! "nuthatch": these are what a reader copies and runs, the same reasoning `version-check.sh` gives
//! for anchoring on the container-tag lines rather than every version-shaped number in a file. A
//! flag is only checked when it appears inside such an invocation - the wider doc set (RFCs,
//! benchmarks, operator runbooks) mentions plenty of non-nuthatch `--flags` (docker, curl, grep,
//! cargo) that a blind whole-text scan would flag as fiction. `tests/skill_refs.rs` already runs
//! that blind scan, safely, because its scope is the authored skill packages (builder + subgraph
//! port). This file does not widen that one.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use clap::CommandFactory;
use nuthatch::cli::Cli;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Deliberate historical or explanatory mentions of a command/flag the binary no longer accepts.
/// Every entry needs the file it appears in, the exact unresolved word, how many times it is
/// expected to appear, and why it stays. Keying on `(file, token)` alone would exempt *every*
/// mention of that token in that file forever, not just the historical one that justified the
/// entry - acceptance #1 says "any documented file", and a file that already carries an entry is
/// not an exception to that. Pinning the count closes it: a new mention pushes the actual count
/// above the declared one and fails the build same as an unlisted file would; a removed mention
/// pulls it below and fails too, so a dead entry can't sit unnoticed (`allowlist_has_no_stale_entries`
/// no longer needs to exist as a separate check - a stale entry is just the actual-count-0 case).
const ALLOWED: &[(&str, &str, usize, &str)] = &[
    (
        "docs/operators.md",
        "roost",
        1,
        "the pre-2.0 -> 2.0 migration table maps the retired `nuthatch roost dev` to its \
         replacement; the mapping needs both names",
    ),
    (
        "docs/kicking-the-tyres.md",
        "roost",
        1,
        "A deliberate historical mention: the page cites `llms.txt` telling agents to run `nuthatch \
         roost` for five releases after 2.0 removed it, as a worked example of documentation rotting \
         faster than code. Naming the incident is the point, so this is a fact about a past release \
         rather than an instruction. Fittingly, this check caught it in the very page that cites it",
    ),
    (
        "docs/operators.md",
        "--flight",
        // A real flag in a `flight` build, so only the default build sees it as unresolved.
        if cfg!(feature = "flight") { 0 } else { 1 },
        "Flight SQL (RFC-0079 S2) is behind the off-by-default `flight` feature, and the page says \
         so in the sentence that names the flag",
    ),
];

/// A finding: an unresolved subcommand word or flag, where it was found, and what was wrong.
#[derive(Debug, Clone)]
struct Finding {
    file: String,
    line: usize,
    bad_token: String,
    detail: String,
}

impl Finding {
    fn is_allowed(&self) -> bool {
        ALLOWED
            .iter()
            .any(|(f, tok, _, _)| *f == self.file && *tok == self.bad_token)
    }
}

/// Doc files in scope: `README.md`, the product docs under `docs/`, and the authored files of
/// `skills/nuthatch-builder/` and `skills/nuthatch-subgraph-port/` (RFC-0017 / RFC-0044).
/// RFCs, release notes, benchmarks, decisions, validation logs and date-named files are records of
/// what was true when written, so a command they name may since have gone.
/// `cli-reference.md` itself is excluded: it's rendered from `Cli::command()` by `src/skill.rs`,
/// the same ground truth this file checks against, so it can't drift by construction -
/// `tests/skill_refs.rs::committed_cli_reference_is_not_stale` is the gate for that file
/// specifically. `tests/skill_refs.rs` already drift-checks the *other* skill files' flags against
/// the reference, both directions - walking them here too is a second, weaker copy of half of that
/// check, but the only one that also validates subcommand *names*, which the flags-only check can't.
fn doc_files() -> Vec<PathBuf> {
    let root = repo_root();
    let mut out = vec![root.join("README.md")];
    collect_md(&root.join("docs"), &mut out);
    for dir in nuthatch::skill::AUTHORED_SKILL_DIRS {
        collect_md(&root.join(dir), &mut out);
    }
    out.retain(|p| p.file_name().and_then(|n| n.to_str()) != Some("cli-reference.md"));
    out.retain(|p| is_product_doc(&rel(&root, p)));
    out
}

const RECORD_DIRS: [&str; 5] = [
    "docs/rfcs/",
    "docs/releases/",
    "docs/bench/",
    "docs/decisions/",
    "docs/validation/",
];

fn is_product_doc(rel: &str) -> bool {
    if RECORD_DIRS.iter().any(|d| rel.starts_with(d)) {
        return false;
    }
    let name = rel.rsplit('/').next().unwrap_or(rel);
    !has_date(name)
}

/// A `YYYY-MM-DD` anywhere in the name, as in `security-audit-2026-07-31.md`.
fn has_date(name: &str) -> bool {
    let b = name.as_bytes();
    b.windows(10).any(|w| {
        w.iter().enumerate().all(|(i, c)| match i {
            4 | 7 => *c == b'-',
            _ => c.is_ascii_digit(),
        })
    })
}

#[test]
fn records_are_out_of_scope_and_product_docs_are_in() {
    assert!(is_product_doc("README.md"));
    assert!(is_product_doc("docs/operators.md"));
    assert!(is_product_doc("docs/launch/show-hn.md"));
    assert!(is_product_doc("skills/nuthatch-builder/SKILL.md"));
    assert!(!is_product_doc(
        "docs/rfcs/0012-multi-nest-runtime-and-nest-packaging.md"
    ));
    assert!(!is_product_doc("docs/releases/v4.1.0.md"));
    assert!(!is_product_doc("docs/security-audit-2026-07-31.md"));
    let files: Vec<String> = doc_files().iter().map(|p| rel(&repo_root(), p)).collect();
    assert!(
        files.iter().any(|f| f == "docs/operators.md") && files.iter().any(|f| f == "README.md"),
        "the product docs must still be walked: {files:?}"
    );
}

fn collect_md(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_md(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("md") {
            out.push(path);
        }
    }
}

/// Every backtick-delimited span in `text`, paired with the 1-indexed line it starts on: fenced
/// ``` blocks (content between the fences, language tag stripped) and single-backtick inline spans.
/// A naive `` ` `` split would treat the closing fence of a code block as an inline-span opener;
/// fences are found and removed first so the remaining single backticks are all genuine inline code.
fn backtick_spans(text: &str) -> Vec<(usize, String)> {
    let mut spans = Vec::new();
    let mut in_fence = false;
    let mut fence_start_line = 0;
    let mut fence_buf = String::new();
    let mut line_no = 0;

    for line in text.lines() {
        line_no += 1;
        let trimmed = line.trim_start();
        if trimmed.starts_with("```") {
            if in_fence {
                spans.push((fence_start_line, std::mem::take(&mut fence_buf)));
                in_fence = false;
            } else {
                in_fence = true;
                fence_start_line = line_no + 1;
                fence_buf.clear();
            }
            continue;
        }
        if in_fence {
            fence_buf.push_str(line);
            fence_buf.push('\n');
            continue;
        }
        // Inline spans: split on single backticks, odd-indexed segments are span content.
        for (i, seg) in line.split('`').enumerate() {
            if i % 2 == 1 && !seg.is_empty() {
                spans.push((line_no, seg.to_string()));
            }
        }
    }
    spans
}

/// Walk one `nuthatch ...` invocation's tokens against the clap tree starting at `root`, recording
/// any subcommand word that doesn't resolve and any `--flag` not present anywhere in the tree.
/// Stops requiring subcommand resolution the moment a leaf (a command with no further subcommands)
/// is reached - the rest of the tokens are that leaf's own args/positionals, out of scope here.
/// A semver-shaped token: three dot-separated numeric parts, with any `-pre` / `+build` tail
/// ignored. Deliberately narrow - `3.0.0-alpha.1` yes, `2.5` and `v3` no - because the point is to
/// recognise the exact thing `--version` prints, not to excuse every numeric word from checking.
fn is_version_token(tok: &str) -> bool {
    let core = tok.split(['-', '+']).next().unwrap_or(tok);
    let parts: Vec<&str> = core.split('.').collect();
    parts.len() == 3
        && parts
            .iter()
            .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
}

fn walk_invocation(
    tokens: &[(usize, &str)],
    root: &clap::Command,
    real_flags: &BTreeSet<String>,
    file: &str,
    findings: &mut Vec<Finding>,
) {
    let mut cmd = root;
    let mut resolving = true; // still expecting subcommand words
    let mut i = 0;
    while i < tokens.len() {
        let (line, tok) = tokens[i];
        if tok == "#" {
            break; // shell comment: nothing after this is a real token
        }
        if let Some(stripped) = tok.strip_prefix("--") {
            let name = stripped.split('=').next().unwrap_or(stripped);
            let flag = format!("--{name}");
            if !real_flags.contains(&flag) {
                findings.push(Finding {
                    file: file.to_string(),
                    line,
                    bad_token: flag.clone(),
                    detail: format!("`{flag}` is not a flag `nuthatch` accepts anywhere"),
                });
            }
            i += 1;
            continue;
        }
        if resolving && std::ptr::eq(cmd, root) && is_version_token(tok) {
            // `nuthatch 3.0.0-alpha.1` - the shape `--version` prints, and the line every
            // validation report opens with. That is the binary naming itself, not an invocation.
            // No subcommand in this CLI is version-shaped, so nothing real is skipped here.
            //
            // Only at the root: after a real subcommand a version-shaped word is a positional
            // value, which the arm below already stops resolving on. #1070 - the #790 tyre-kicking
            // report could not be committed without reddening this check, so it sat untracked on
            // one laptop for a week instead. A report that cannot enter the repository is a report
            // nobody has.
            resolving = false;
            i += 1;
            continue;
        }
        if resolving && tok.starts_with('<') {
            // A doc placeholder (`<old>`, `<blob>`, ...), not a further subcommand word - the
            // author is showing shape, not a literal invocation past this point.
            resolving = false;
            i += 1;
            continue;
        }
        if resolving && cmd.has_subcommands() {
            // Hidden is a `--help` rendering choice (`skill-refs`), not a parser refusal - a
            // hidden subcommand is still real and must resolve here.
            let single = cmd.get_subcommands().find(|s| s.get_name() == tok);
            // `list|add` shorthand: valid only if every alternative names a real subcommand at
            // this position. Which branch was meant is ambiguous, so - like a placeholder - this
            // is where subcommand resolution stops, not a further chain to descend.
            let alternation_ok = tok.contains('|')
                && tok
                    .split('|')
                    .all(|part| cmd.get_subcommands().any(|s| s.get_name() == part));
            match single {
                Some(next) => {
                    cmd = next;
                }
                None if alternation_ok => {
                    resolving = false;
                }
                None => {
                    findings.push(Finding {
                        file: file.to_string(),
                        line,
                        bad_token: tok.to_string(),
                        detail: format!(
                            "`{tok}` is not a subcommand of `nuthatch{}`",
                            invocation_path(root, cmd)
                        ),
                    });
                    resolving = false;
                }
            }
        } else {
            // Either already at a leaf, or we already gave up after an unknown word above -
            // remaining bare words are positional values, not further subcommands to validate.
            resolving = false;
        }
        i += 1;
    }
}

/// The invocation path from `root` down to `cmd`, e.g. " audit report", for error messages.
fn invocation_path(root: &clap::Command, cmd: &clap::Command) -> String {
    if std::ptr::eq(root, cmd) {
        return String::new();
    }
    // Small tree, small doc corpus - a BFS per finding is cheap and avoids threading path state
    // through the recursive walk above.
    fn find_path<'a>(node: &'a clap::Command, target: &str, path: &mut Vec<&'a str>) -> bool {
        for sub in node.get_subcommands() {
            path.push(sub.get_name());
            if sub.get_name() == target || find_path(sub, target, path) {
                return true;
            }
            path.pop();
        }
        false
    }
    let mut path = Vec::new();
    find_path(root, cmd.get_name(), &mut path);
    if path.is_empty() {
        format!(" {}", cmd.get_name())
    } else {
        format!(" {}", path.join(" "))
    }
}

/// Every long flag anywhere in the tree the parser accepts, independent of `src/skill.rs`'s own
/// renderer - same reasoning as `tests/skill_refs.rs::collect_real_flags`: a bug shared between the
/// renderer and this check must not be able to make both agree on a wrong answer. Hidden is not
/// filtered here either, for the same reason as subcommands: it hides an arg from `--help`, it does
/// not stop the parser accepting it.
///
/// `--help`/`--version` are seeded explicitly: they're clap-synthesised rather than declared on
/// `Cli`, and `Command::get_arguments()` only reports them once the command has been `build()`-ed -
/// which `CommandFactory::command()` does not guarantee. Both are real on every subcommand.
fn real_flags() -> BTreeSet<String> {
    fn walk(cmd: &clap::Command, out: &mut BTreeSet<String>) {
        for arg in cmd.get_arguments() {
            if let Some(long) = arg.get_long() {
                out.insert(format!("--{long}"));
            }
        }
        for sub in cmd.get_subcommands() {
            walk(sub, out);
        }
    }
    let mut out = BTreeSet::new();
    out.insert("--help".to_string());
    out.insert("--version".to_string());
    walk(&Cli::command(), &mut out);
    out
}

/// Find every `nuthatch ...` invocation in `text` and check its subcommand chain and flags. An
/// invocation starts at a bare `nuthatch` token (exact match - `nuthatch.toml`, `nuthatch-builder`,
/// `nuthatch:2.5.0`, `nuthatch_tip_height` are all distinct whitespace-delimited tokens and never
/// match) that begins a shell segment - the first token on a line, or straight after `&&`/`||`/`;`/
/// `|`/`--`/`sudo` - and runs until the next such separator or end of line. The segment-start
/// requirement is what tells `journalctl -u nuthatch -f` and `docker run --name nuthatch ...` (a
/// service/container *name* argument) apart from an actual invocation, and lets `claude mcp add
/// nuthatch -- nuthatch mcp --url ...` resolve only the second `nuthatch` - the one after `--` that
/// is actually being run.
///
/// A line ending in `\` continues the invocation onto the next physical line (#782). Flags on the
/// continuation are attributed to *that* line, not the line the `nuthatch` token sat on.
fn check_text(text: &str, file: &str, real_flags: &BTreeSet<String>, findings: &mut Vec<Finding>) {
    let root = Cli::command();
    for (start_line, span) in backtick_spans(text) {
        let tokens = token_stream(&span, start_line);
        let mut i = 0;
        let mut at_segment_start = true;
        while i < tokens.len() {
            match &tokens[i] {
                StreamTok::Eol => {
                    at_segment_start = true;
                    i += 1;
                }
                StreamTok::Word { word, .. } if word == "nuthatch" && at_segment_start => {
                    let mut end = i + 1;
                    while end < tokens.len() {
                        match &tokens[end] {
                            StreamTok::Eol => break,
                            StreamTok::Word { word: w, .. }
                                if matches!(w.as_str(), "&&" | "||" | ";" | "|" | "--")
                                    || w == "nuthatch" =>
                            {
                                break;
                            }
                            StreamTok::Word { .. } => end += 1,
                        }
                    }
                    let slice: Vec<(usize, &str)> = tokens[i + 1..end]
                        .iter()
                        .filter_map(|t| match t {
                            StreamTok::Word { line, word } => Some((*line, word.as_str())),
                            StreamTok::Eol => None,
                        })
                        .collect();
                    walk_invocation(&slice, &root, real_flags, file, findings);
                    i = end;
                    at_segment_start = true;
                }
                StreamTok::Word { word, .. }
                    if matches!(word.as_str(), "&&" | "||" | ";" | "|" | "--")
                        || word == "sudo" =>
                {
                    at_segment_start = true;
                    i += 1;
                }
                StreamTok::Word { .. } => {
                    at_segment_start = false;
                    i += 1;
                }
            }
        }
    }
}

enum StreamTok {
    Word { line: usize, word: String },
    Eol,
}

/// Physical lines become a token stream. A trailing `\` swallows the line break so the next
/// line's tokens continue the same invocation; any other newline is an `Eol` that ends it (#782).
fn token_stream(span: &str, start_line: usize) -> Vec<StreamTok> {
    let mut out = Vec::new();
    for (offset, line) in span.lines().enumerate() {
        let line_no = start_line + offset;
        let cont = line.trim_end().ends_with('\\');
        let body = if cont {
            line.trim_end().trim_end_matches('\\')
        } else {
            line
        };
        for word in body.split_whitespace() {
            out.push(StreamTok::Word {
                line: line_no,
                word: word.to_string(),
            });
        }
        if !cont {
            out.push(StreamTok::Eol);
        }
    }
    out
}

fn rel(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

#[test]
fn every_documented_command_and_flag_is_real() {
    let root = repo_root();
    let real = real_flags();
    let mut findings = Vec::new();

    for path in doc_files() {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let file = rel(&root, &path);
        check_text(&text, &file, &real, &mut findings);
    }

    let offenders: Vec<String> = findings
        .iter()
        .filter(|f| !f.is_allowed())
        .map(|f| format!("{}:{}: {}", f.file, f.line, f.detail))
        .collect();

    assert!(
        offenders.is_empty(),
        "documented commands/flags the binary does not accept - fix the docs, fix the binary, or \
         add a reasoned entry to ALLOWED in tests/doc_command_check.rs:\n{}",
        offenders.join("\n")
    );

    // Every entry's declared count must match the number of unresolved mentions actually found -
    // in either direction. A count too low (including zero - a stale entry matching nothing) is
    // the old `allowlist_has_no_stale_entries` check; a count too high is the bug that check never
    // caught: an entry keyed on `(file, token)` alone exempts every future mention in that file,
    // not just the one that justified it, so acceptance #1 ("any documented file") silently stops
    // holding for files that already carry an entry. Pinning the count closes both directions.
    let miscounted = miscounted_allowlist_entries(&findings);
    assert!(
        miscounted.is_empty(),
        "ALLOWED counts in tests/doc_command_check.rs no longer match the doc set:\n{}",
        miscounted.join("\n")
    );
}

/// Entries in `ALLOWED` whose declared count doesn't match the number of unresolved mentions
/// actually present in `findings` for that `(file, token)` pair - see the assertion in
/// `every_documented_command_and_flag_is_real` for why both directions matter.
fn miscounted_allowlist_entries(findings: &[Finding]) -> Vec<String> {
    let mut actual_counts: BTreeMap<(&str, &str), usize> = BTreeMap::new();
    for f in findings {
        *actual_counts
            .entry((f.file.as_str(), f.bad_token.as_str()))
            .or_insert(0) += 1;
    }
    ALLOWED
        .iter()
        .filter_map(|(file, tok, declared, _)| {
            let actual = actual_counts.get(&(*file, *tok)).copied().unwrap_or(0);
            (actual != *declared).then(|| {
                format!(
                    "{file}: `{tok}` allow-listed for exactly {declared} mention(s), found {actual} \
                     - a new mention needs its own reviewed entry, not a free ride on this one's \
                     count; a removed mention needs the count brought down (or the entry deleted if \
                     it hits zero)"
                )
            })
        })
        .collect()
}

// ── Proving the mechanism can fail before it is trusted (acceptance #3) ──────────────────────
//
// These run against synthetic fixtures, not the real doc tree, so they stay meaningful even after
// every current ALLOWED entry above is one day deleted. Each is the literal regression test named
// in the issue: reintroducing a removed subcommand into a documented file must fail the build.

#[test]
fn detects_a_reintroduced_removed_subcommand() {
    let real = real_flags();
    let mut findings = Vec::new();
    check_text(
        "Run `nuthatch roost dev` to serve every mounted nest.",
        "docs/example.md",
        &real,
        &mut findings,
    );
    assert!(
        findings.iter().any(|f| f.bad_token == "roost"),
        "reintroducing `nuthatch roost` into a documented file must be detected"
    );
}

#[test]
fn detects_an_unreal_flag() {
    let real = real_flags();
    let mut findings = Vec::new();
    check_text(
        "```sh\nnuthatch dev --this-flag-does-not-exist\n```",
        "docs/example.md",
        &real,
        &mut findings,
    );
    assert!(
        findings
            .iter()
            .any(|f| f.bad_token == "--this-flag-does-not-exist"),
        "a nonexistent flag inside a real invocation must be detected"
    );
}

/// #782: a `\`-continued fenced block used to drop every flag on the next physical line. The
/// flag is attributed to the line it is written on (3, after the opening fence), not the line
/// `nuthatch` sat on.
#[test]
fn a_backslash_continuation_still_sees_the_flag_on_the_next_line() {
    let real = real_flags();
    let mut findings = Vec::new();
    check_text(
        "```sh\nnuthatch bench backfill --dir x --from 1 --to 2 \\\n  --not-a-real-nuthatch-flag\n```\n",
        "docs/example.md",
        &real,
        &mut findings,
    );
    let hit = findings
        .iter()
        .find(|f| f.bad_token == "--not-a-real-nuthatch-flag")
        .unwrap_or_else(|| panic!("continuation flag was invisible: {findings:?}"));
    assert_eq!(
        hit.line, 3,
        "must point at the flag, not the invocation start"
    );
}

#[test]
fn a_real_multi_level_invocation_is_clean() {
    let real = real_flags();
    let mut findings = Vec::new();
    check_text(
        "```sh\nnuthatch audit report --dir . --from 100 --to 200\nnuthatch labels import addrs.csv --alias sanctioned\n```",
        "docs/example.md",
        &real,
        &mut findings,
    );
    assert!(
        findings.is_empty(),
        "real subcommand chains and flags must not be flagged: {findings:?}"
    );
}

#[test]
fn positional_values_after_a_leaf_are_not_mistaken_for_subcommands() {
    let real = real_flags();
    let mut findings = Vec::new();
    check_text(
        "`nuthatch init 0xA0b86991c6218b36c1D19D4a2e9Eb0cE3606eB48 --alias usdc`",
        "docs/example.md",
        &real,
        &mut findings,
    );
    assert!(
        findings.is_empty(),
        "an address positional after `init` must not be checked as a subcommand: {findings:?}"
    );
}

#[test]
fn a_bare_mention_of_the_product_name_is_not_an_invocation() {
    let real = real_flags();
    let mut findings = Vec::new();
    check_text(
        "nuthatch.toml, nuthatch-builder, and ghcr.io/nuthatch-org/nuthatch:2.5.0 are not commands.",
        "docs/example.md",
        &real,
        &mut findings,
    );
    assert!(
        findings.is_empty(),
        "prose mentions of the product name must not be parsed as invocations: {findings:?}"
    );
}

#[test]
fn a_version_string_is_not_a_subcommand() {
    let real = real_flags();
    let mut findings = Vec::new();
    check_text(
        "Binary: `nuthatch 3.0.0-alpha.1`, built 2026-08-28. Also `nuthatch 3.1.0`.",
        "docs/validation/example.md",
        &real,
        &mut findings,
    );
    assert!(
        findings.is_empty(),
        "a version string is the binary naming itself, not a subcommand: {findings:?}"
    );

    // The narrowness is the point: a bare word in the same position is still checked, or the
    // guard would be an amnesty on the first token rather than on version strings.
    let mut still_checked = Vec::new();
    check_text(
        "Run `nuthatch roost dev` to start.",
        "docs/validation/example.md",
        &real,
        &mut still_checked,
    );
    assert_eq!(
        still_checked.len(),
        1,
        "an unreal subcommand in the same position must still be reported: {still_checked:?}"
    );
}

#[test]
fn an_allowlisted_mention_is_suppressed_only_in_its_own_file() {
    assert!(
        ALLOWED
            .iter()
            .any(|(f, t, _, _)| *f == "docs/operators.md" && *t == "roost"),
        "sanity: the allow-list this test exercises must still name the operators.md roost entry"
    );
    let finding = Finding {
        file: "docs/operators.md".to_string(),
        line: 1,
        bad_token: "roost".to_string(),
        detail: String::new(),
    };
    assert!(finding.is_allowed());

    let same_word_elsewhere = Finding {
        file: "docs/some-other-file.md".to_string(),
        line: 1,
        bad_token: "roost".to_string(),
        detail: String::new(),
    };
    assert!(
        !same_word_elsewhere.is_allowed(),
        "an allow-list entry is pinned to its file - the same word elsewhere must still fail"
    );
}

/// The gap NIG-341's review found: `is_allowed()` alone (the check above) treats every mention of
/// an allow-listed token in its file as fine, so a *second*, brand-new `roost` mention landing in
/// `docs/operators.md` - already exempt for the historical migration-table one - would pass
/// silently, against acceptance #1 ("reintroducing `nuthatch roost` into any documented file fails
/// the build"). `miscounted_allowlist_entries` is the fix: it compares the declared count against
/// how many times the pair actually shows up.
#[test]
fn a_second_mention_in_an_already_allowlisted_file_is_not_free() {
    let declared = ALLOWED
        .iter()
        .find(|(f, t, _, _)| *f == "docs/operators.md" && *t == "roost")
        .map(|(_, _, count, _)| *count)
        .expect("sanity: the allow-list this test exercises must still name the entry");
    assert_eq!(
        declared, 1,
        "this test's synthetic two-mention fixture assumes the real entry declares exactly one"
    );

    // `miscounted_allowlist_entries` walks the whole real `ALLOWED` table, not just the pair under
    // test, so a findings list built from scratch reads every *other* entry as a zero-actual
    // mismatch too - filter down to the one entry this test is about before asserting on it.
    let concerns_operators_roost =
        |m: &String| m.contains("docs/operators.md") && m.contains("roost");

    let one_mention = vec![Finding {
        file: "docs/operators.md".to_string(),
        line: 1,
        bad_token: "roost".to_string(),
        detail: String::new(),
    }];
    assert!(
        !miscounted_allowlist_entries(&one_mention)
            .iter()
            .any(concerns_operators_roost),
        "the declared, single historical mention must not itself be flagged"
    );

    let two_mentions = vec![
        Finding {
            file: "docs/operators.md".to_string(),
            line: 1,
            bad_token: "roost".to_string(),
            detail: String::new(),
        },
        Finding {
            file: "docs/operators.md".to_string(),
            line: 200,
            bad_token: "roost".to_string(),
            detail: String::new(),
        },
    ];
    let miscounted = miscounted_allowlist_entries(&two_mentions);
    assert!(
        miscounted
            .iter()
            .any(|m| m.contains("docs/operators.md") && m.contains("roost")),
        "a second, new `roost` mention in the already-exempt file must be reported, not waved \
         through on the first mention's count: {miscounted:?}"
    );
}
