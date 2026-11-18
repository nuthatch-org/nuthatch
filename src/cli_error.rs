//! What a failed command tells a script (#2987): a short, stable error code, the exit status it maps
//! to, and with `--json` one object on stderr. The human `Error:` text is unchanged by any of it.

use std::fmt;

/// The documented failure classes. The strings and the exit statuses are a contract: add a code, never
/// renumber or rename one. `docs/cli-errors.md` is the reader's copy of this table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Code {
    /// Anything not classified below.
    Error,
    /// A command line clap refused.
    Usage,
    NoNest,
    NestNotRunning,
    SqlInvalid,
    AbiNotFound,
    ChainNotDetected,
    RpcRefused,
    RpcRangeTooWide,
}

/// The table in `nuthatch --help`.
pub const EXIT_STATUS_HELP: &str = "\
Exit status:
  0  success
  1  error
  2  usage
  3  no_nest
  4  nest_not_running
  5  sql_invalid
  6  abi_not_found
  7  chain_not_detected
  8  rpc_refused
  9  rpc_range_too_wide
With --json, an error is one JSON object on stderr with its code and a next step (docs/cli-errors.md).";

impl Code {
    pub const ALL: [Code; 9] = [
        Code::Error,
        Code::Usage,
        Code::NoNest,
        Code::NestNotRunning,
        Code::SqlInvalid,
        Code::AbiNotFound,
        Code::ChainNotDetected,
        Code::RpcRefused,
        Code::RpcRangeTooWide,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Code::Error => "error",
            Code::Usage => "usage",
            Code::NoNest => "no_nest",
            Code::NestNotRunning => "nest_not_running",
            Code::SqlInvalid => "sql_invalid",
            Code::AbiNotFound => "abi_not_found",
            Code::ChainNotDetected => "chain_not_detected",
            Code::RpcRefused => "rpc_refused",
            Code::RpcRangeTooWide => "rpc_range_too_wide",
        }
    }

    /// 2 is clap's own usage status, so it is kept for usage.
    pub fn exit_code(self) -> i32 {
        match self {
            Code::Error => 1,
            Code::Usage => 2,
            Code::NoNest => 3,
            Code::NestNotRunning => 4,
            Code::SqlInvalid => 5,
            Code::AbiNotFound => 6,
            Code::ChainNotDetected => 7,
            Code::RpcRefused => 8,
            Code::RpcRangeTooWide => 9,
        }
    }

    /// The step to take when the error carries none more specific.
    pub fn next(self) -> &'static str {
        match self {
            Code::Error => "read the message; `nuthatch <command> --help` lists the flags",
            Code::Usage => "run `nuthatch <command> --help` for the flags it takes",
            Code::NoNest => {
                "run `nuthatch init 0xADDRESS` here, or pass --dir with the nest's directory"
            }
            Code::NestNotRunning => {
                "run `nuthatch dev --detach` in the nest's directory, or pass --url to a running one"
            }
            Code::SqlInvalid => {
                "fix the query: `SELECT table_name FROM information_schema.tables` lists the tables"
            }
            Code::AbiNotFound => {
                "pass --abi path/to/abi.json, or set ETHERSCAN_API_KEY for a verified contract"
            }
            Code::ChainNotDetected => "pass --chain <name>, or --rpc with an endpoint on its chain",
            Code::RpcRefused => {
                "pass --rpc with an endpoint that answers; `nuthatch doctor --rpc <url>` probes one"
            }
            Code::RpcRangeTooWide => {
                "`nuthatch doctor --rpc <url>` prints the largest safe --window; pass it to dev"
            }
        }
    }
}

/// An error carrying a [`Code`], displayed exactly as the error it wraps: its message is the inner
/// error's, and its sources are the inner error's sources, so `Error: …` prints byte for byte as
/// it did untagged.
pub struct Coded {
    pub code: Code,
    pub next: Option<String>,
    inner: anyhow::Error,
}

impl fmt::Display for Coded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&*self.inner, f)
    }
}

impl fmt::Debug for Coded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.inner, f)
    }
}

impl std::error::Error for Coded {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.inner.source()
    }
}

pub fn tag(code: Code, err: anyhow::Error) -> anyhow::Error {
    anyhow::Error::new(Coded {
        code,
        next: None,
        inner: err,
    })
}

pub fn tag_with_next(code: Code, next: String, err: anyhow::Error) -> anyhow::Error {
    anyhow::Error::new(Coded {
        code,
        next: Some(next),
        inner: err,
    })
}

pub trait CodeExt<T> {
    fn code(self, code: Code) -> anyhow::Result<T>;
}

impl<T, E: Into<anyhow::Error>> CodeExt<T> for Result<T, E> {
    fn code(self, code: Code) -> anyhow::Result<T> {
        self.map_err(|e| tag(code, e.into()))
    }
}

/// What a command reaches, which decides how an untagged transport error reads.
#[derive(Debug, Clone, Copy, Default)]
pub struct Reaches {
    /// An RPC endpoint: its refusals are `rpc_refused` or `rpc_range_too_wide`.
    pub rpc: bool,
    /// A running nest's API: a refused connection is `nest_not_running`.
    pub api: bool,
}

/// The code and next step for `err`: the outermost tag, else what its transport errors say.
pub fn classify(err: &anyhow::Error, reaches: Reaches) -> (Code, String) {
    if let Some(c) = err.chain().find_map(|e| e.downcast_ref::<Coded>()) {
        let next = c.next.clone().unwrap_or_else(|| c.code.next().to_string());
        return (c.code, next);
    }
    let code = transport_code(err, reaches).unwrap_or(Code::Error);
    (code, code.next().to_string())
}

fn transport_code(err: &anyhow::Error, reaches: Reaches) -> Option<Code> {
    use crate::rpc::FailureClass;
    if reaches.rpc {
        match crate::rpc::class_of(err) {
            Some(FailureClass::Narrowable {
                escalated_from_rate_limit: false,
                ..
            }) => return Some(Code::RpcRangeTooWide),
            Some(_) => return Some(Code::RpcRefused),
            None => {}
        }
    }
    let unreachable = err.chain().any(|e| {
        e.downcast_ref::<reqwest::Error>()
            .is_some_and(|r| r.is_connect() || r.is_timeout())
    });
    match (unreachable, reaches.api, reaches.rpc) {
        (true, true, _) => Some(Code::NestNotRunning),
        (true, false, true) => Some(Code::RpcRefused),
        _ => None,
    }
}

/// What `cmd` reaches, and whether it was asked for `--json`.
pub fn for_command(cmd: &crate::cli::Command) -> (Reaches, bool) {
    use crate::cli::Command as C;
    let rpc = Reaches {
        rpc: true,
        api: false,
    };
    let api = Reaches {
        rpc: false,
        api: true,
    };
    match cmd {
        C::Sql(a) => (api, a.json),
        C::Diff(_) => (api, false),
        C::Mcp(_) => (api, false),
        #[cfg(feature = "watch")]
        C::Watch(a) => (api, a.json),
        C::Init(a) => (rpc, a.json),
        C::Add(a) => (rpc, a.json),
        C::Schema(a) => (Reaches::default(), a.json),
        C::Dev(a) => (rpc, a.json),
        C::Stop(a) => (Reaches::default(), a.json),
        C::Doctor(a) => (Reaches::default(), a.json),
        _ => (Reaches::default(), false),
    }
}

/// A command line clap refused. Asked for `--json`, the refusal is the same one object as any other
/// error, with the `usage` code; otherwise clap prints and exits as it always has.
pub fn refuse_usage(e: clap::Error, argv: &[String]) -> ! {
    use clap::error::ErrorKind;
    let printed_on_request = matches!(e.kind(), ErrorKind::DisplayHelp | ErrorKind::DisplayVersion);
    if printed_on_request || !argv.iter().skip(1).any(|a| a == "--json") {
        e.exit()
    }
    let message = e.render().to_string();
    let message = message.trim().trim_start_matches("error: ");
    eprintln!(
        "{}",
        serde_json::json!({
            "error": {
                "code": Code::Usage.as_str(),
                "exit": Code::Usage.exit_code(),
                "message": message,
                "next": Code::Usage.next(),
            }
        })
    );
    std::process::exit(Code::Usage.exit_code())
}

/// The one object `--json` puts on stderr in place of `Error: …`.
pub fn json(err: &anyhow::Error, code: Code, next: &str) -> serde_json::Value {
    serde_json::json!({
        "error": {
            "code": code.as_str(),
            "exit": code.exit_code(),
            "message": format!("{err:#}"),
            "next": next,
        }
    })
}

/// Print `err` the way the command asked and return the status to exit with.
pub fn report(err: &anyhow::Error, reaches: Reaches, as_json: bool) -> i32 {
    // The caller exits, which flushes no buffer.
    let _ = std::io::Write::flush(&mut std::io::stdout());
    let (code, next) = classify(err, reaches);
    if as_json {
        eprintln!("{}", json(err, code, &next));
    } else {
        // What `fn main() -> Result<()>` prints, so the human form is unchanged.
        eprintln!("Error: {err:?}");
    }
    code.exit_code()
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context as _;

    #[test]
    fn every_code_has_its_own_name_and_exit_status() {
        let mut names = std::collections::HashSet::new();
        let mut exits = std::collections::HashSet::new();
        for c in Code::ALL {
            assert!(names.insert(c.as_str()), "{c:?}");
            assert!(exits.insert(c.exit_code()), "{c:?}");
            assert!(!c.next().is_empty(), "{c:?}");
        }
    }

    /// The table a script reads in docs/cli-errors.md. Changing a row breaks every caller of it.
    #[test]
    fn the_codes_map_to_their_documented_exit_statuses() {
        let table: Vec<(&str, i32)> = Code::ALL
            .iter()
            .map(|c| (c.as_str(), c.exit_code()))
            .collect();
        assert_eq!(
            table,
            [
                ("error", 1),
                ("usage", 2),
                ("no_nest", 3),
                ("nest_not_running", 4),
                ("sql_invalid", 5),
                ("abi_not_found", 6),
                ("chain_not_detected", 7),
                ("rpc_refused", 8),
                ("rpc_range_too_wide", 9),
            ]
        );
        let doc = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("docs/cli-errors.md"),
        )
        .unwrap();
        for (name, exit) in table {
            assert!(
                doc.contains(&format!("| {exit} | `{name}` |")),
                "docs/cli-errors.md has no row for {name} = {exit}"
            );
            assert!(
                EXIT_STATUS_HELP.contains(&format!("\n  {exit}  {name}\n")),
                "--help has no row for {name} = {exit}"
            );
        }
    }

    #[test]
    fn a_tag_prints_exactly_as_the_error_it_wraps() {
        let make = || {
            Err::<(), _>(std::io::Error::other("disk on fire"))
                .context("reading the thing")
                .unwrap_err()
        };
        let plain = make();
        let tagged = tag(Code::NoNest, make());
        assert_eq!(format!("{tagged:?}"), format!("{plain:?}"));
        assert_eq!(format!("{tagged:#}"), format!("{plain:#}"));
        assert_eq!(format!("{tagged}"), format!("{plain}"));
    }

    #[test]
    fn a_tag_survives_context_added_above_it() {
        let err = Err::<(), _>(anyhow::anyhow!("no nuthatch.toml"))
            .code(Code::NoNest)
            .context("loading the nest")
            .unwrap_err();
        assert_eq!(classify(&err, Reaches::default()).0, Code::NoNest);
    }

    #[test]
    fn an_untagged_error_is_the_general_code() {
        let err = anyhow::anyhow!("something else");
        let (code, next) = classify(
            &err,
            Reaches {
                rpc: true,
                api: true,
            },
        );
        assert_eq!(code, Code::Error);
        assert_eq!(next, Code::Error.next());
    }

    #[test]
    fn an_rpc_classification_reads_as_refused_or_too_wide_only_where_rpc_is_reached() {
        use crate::rpc::{ClassifiedError, FailureClass};
        let wide = || {
            anyhow::Error::new(ClassifiedError {
                class: FailureClass::Narrowable {
                    suggested: None,
                    escalated_from_rate_limit: false,
                },
                detail: "query returned more than 10000 results".into(),
            })
            .context("getLogs 1..=100000")
        };
        let rpc = Reaches {
            rpc: true,
            api: false,
        };
        assert_eq!(classify(&wide(), rpc).0, Code::RpcRangeTooWide);
        assert_eq!(classify(&wide(), Reaches::default()).0, Code::Error);
        let refused = anyhow::Error::new(ClassifiedError {
            class: FailureClass::Terminal,
            detail: "401 Unauthorized".into(),
        });
        assert_eq!(classify(&refused, rpc).0, Code::RpcRefused);
    }

    #[test]
    fn the_json_form_carries_code_exit_message_and_next() {
        let err = tag_with_next(
            Code::SqlInvalid,
            "the column is `value`".into(),
            anyhow::anyhow!("no column valu"),
        );
        let (code, next) = classify(&err, Reaches::default());
        let v = json(&err, code, &next);
        assert_eq!(v["error"]["code"], "sql_invalid");
        assert_eq!(v["error"]["exit"], 5);
        assert_eq!(v["error"]["message"], "no column valu");
        assert_eq!(v["error"]["next"], "the column is `value`");
    }
}
