//! Groups the top-level subcommands in `nuthatch --help` by how a stranger encounters them
//! (#674), instead of the flat, declaration-ordered `Commands:` list clap renders by default.
//!
//! clap has no built-in way to put subcommands under more than one heading - `Arg::help_heading`
//! groups *flags*, and `Command::subcommand_help_heading` only renames the single "Commands:"
//! heading, it doesn't split it (clap-rs/clap#1553 is still open). So [`render_top_level_help`]
//! doesn't hand-format anything: it takes clap's own rendered `--help` text and re-slices the
//! `Commands:` block into the groups in [`GROUPS`], line by line. The one-line summaries, their
//! wrapping, and column alignment are exactly what clap generated from the doc comments - only
//! which heading each line sits under changes.

use clap::Command;

/// `(heading, subcommand names)`, in the order both the heading and its members should appear.
/// `cli::tests::every_visible_subcommand_has_exactly_one_help_group` asserts this stays exhaustive
/// over `Cli`'s real visible subcommands - a subcommand added to `cli::Command` without a matching
/// entry here fails that test instead of silently landing in the wrong place.
pub const GROUPS: &[(&str, &[&str])] = &[
    ("CORE", &["init", "add", "dev", "sql", "mcp"]),
    (
        "OPERATING",
        &[
            "serve", "watch", "doctor", "check", "diff", "schema", "bench", "settle",
        ],
    ),
    ("SCALED", &["worker", "control"]),
    ("COMPLIANCE", &["labels", "lists", "pack", "audit"]),
    (
        "ADVANCED",
        &[
            "recipe",
            "metadata",
            "offchain",
            "publish",
            "seed",
            "partitions",
            "nest",
            "migrate",
            "prune",
            "count", // RFC-0054 hook
        ],
    ),
];

fn group_index(name: &str) -> Option<usize> {
    GROUPS.iter().position(|(_, names)| names.contains(&name))
}

/// Render `cmd`'s help as clap would, then regroup the `Commands:` block per [`GROUPS`]. Anything
/// clap adds that isn't in `GROUPS` (namely its own synthetic `help` entry) is folded onto the end
/// of the last group instead of dropped, matching where it already sits today: directly after the
/// last real subcommand, with no heading of its own.
pub fn render_top_level_help(cmd: Command) -> String {
    let mut cmd = cmd;
    let full = cmd.render_help().to_string();

    const HEADING: &str = "Commands:\n";
    let Some(heading_at) = full.find(HEADING) else {
        return full; // no subcommands to regroup
    };
    let prefix = &full[..heading_at];
    let block_start = heading_at + HEADING.len();
    let block_end = full[block_start..]
        .find("\n\n")
        .map(|i| block_start + i)
        .unwrap_or(full.len());
    let block = &full[block_start..block_end];
    let suffix = &full[block_end..];

    let mut grouped: Vec<Vec<String>> = vec![Vec::new(); GROUPS.len()];
    let mut unheaded: Vec<String> = Vec::new();
    // Which bucket the line currently being appended to belongs to: `Some(i)` indexes `grouped`,
    // `None` after an unmapped entry (e.g. `help`) means the next continuation line goes to
    // `unheaded` instead.
    let mut current: Option<Option<usize>> = None;

    for line in block.lines() {
        let starts_entry =
            line.len() > 2 && line.as_bytes()[..2] == *b"  " && line.as_bytes()[2] != b' ';
        if starts_entry {
            let name = line.split_whitespace().next().unwrap_or("");
            let idx = group_index(name);
            match idx {
                Some(i) => grouped[i].push(line.to_string()),
                None => unheaded.push(line.to_string()),
            }
            current = Some(idx);
        } else if let Some(bucket) = current {
            let entry = match bucket {
                Some(i) => grouped[i].last_mut(),
                None => unheaded.last_mut(),
            };
            if let Some(entry) = entry {
                entry.push('\n');
                entry.push_str(line);
            }
        }
    }

    let mut sections: Vec<String> = GROUPS
        .iter()
        .zip(grouped)
        .filter(|(_, lines)| !lines.is_empty())
        .map(|((heading, _), lines)| format!("{heading}:\n{}", lines.join("\n")))
        .collect();

    if !unheaded.is_empty() {
        match sections.last_mut() {
            Some(last) => {
                last.push('\n');
                last.push_str(&unheaded.join("\n"));
            }
            None => sections.push(unheaded.join("\n")),
        }
    }

    format!("{prefix}{}{suffix}", sections.join("\n\n"))
}

/// Renders a `tracing` event as one `  · message` line, in the idiom `init` and `add` already use
/// for their own output (#695).
///
/// The default `nuthatch=info` filter means every `tracing::warn!` in the crate prints during `init`
/// with its own timestamp, level and ANSI colouring, straight through the `→`/`✓` block. A clean run
/// never shows one; a stranger with a slow public endpoint gets the chain-id verification warnings
/// from `rpc.rs`, which is exactly the run where the tool should look most composed.
///
/// **Suppressing them would be the wrong fix.** They are well-written and they are the only signal
/// that a probe is struggling, which is worth knowing while `init` appears to hang. So the message
/// survives verbatim and only its presentation changes: no timestamp, no level, no colour, and the
/// same two-space indent as the ticks it sits between.
pub struct PrettyLine;

impl<S, N> tracing_subscriber::fmt::FormatEvent<S, N> for PrettyLine
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    N: for<'a> tracing_subscriber::fmt::FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &tracing_subscriber::fmt::FmtContext<'_, S, N>,
        mut writer: tracing_subscriber::fmt::format::Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        write!(writer, "  · ")?;
        ctx.field_format().format_fields(writer.by_ref(), event)?;
        writeln!(writer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::Cli;
    use clap::CommandFactory;

    #[test]
    fn groups_are_named_once_each_and_nonempty() {
        let mut seen = std::collections::HashSet::new();
        for (heading, names) in GROUPS {
            assert!(seen.insert(*heading), "duplicate heading `{heading}`");
            assert!(!names.is_empty(), "`{heading}` has no members");
        }
    }

    #[test]
    fn regrouped_help_still_names_every_visible_command_once() {
        let rendered = render_top_level_help(Cli::command());
        for (_, names) in GROUPS {
            for name in *names {
                let occurrences = rendered
                    .lines()
                    .filter(|l| l.trim_start().starts_with(name) && l.starts_with("  "))
                    .count();
                assert_eq!(
                    occurrences, 1,
                    "`{name}` should appear exactly once in --help"
                );
            }
        }
        // clap's synthetic `help` entry survives the regroup instead of being dropped.
        assert!(rendered.lines().any(|l| l.trim_start().starts_with("help")));
    }

    #[test]
    fn regrouped_help_preserves_usage_and_options() {
        let plain = Cli::command().render_help().to_string();
        let regrouped = render_top_level_help(Cli::command());

        let usage_line = plain.lines().find(|l| l.starts_with("Usage:")).unwrap();
        assert!(regrouped.contains(usage_line));

        let options_at = plain.find("\nOptions:\n").unwrap();
        assert_eq!(
            &regrouped[regrouped.find("\nOptions:\n").unwrap()..],
            &plain[options_at..]
        );
    }

    #[test]
    fn headings_appear_in_declared_order() {
        let rendered = render_top_level_help(Cli::command());
        let positions: Vec<usize> = GROUPS
            .iter()
            .filter_map(|(heading, _)| rendered.find(&format!("{heading}:\n")))
            .collect();
        let mut sorted = positions.clone();
        sorted.sort_unstable();
        assert_eq!(positions, sorted, "headings must render in GROUPS order");
    }
}
