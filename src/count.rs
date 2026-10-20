//! The head count (RFC-0054): an opt-in ping at `init`, off until a person answers yes.
//!
//! Every line outside this file that reaches it ends in `RFC-0054 hook`, and
//! `scripts/delete-count.sh` removes exactly those lines and this file. CI builds and tests what is
//! left (A4), so "remove it and a self-hoster loses nothing" is a job rather than a claim.

use std::io::{BufRead, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::cli::InitArgs;

pub const ENDPOINT: &str = "https://count.nuthatch-indexer.com";
pub const TOTALS: &str = "https://www.nuthatch-indexer.com/count";
const USER_AGENT: &str = "nuthatch-count/1";
/// One ceiling for everything a single command sends, so `init` is never more than this slower.
const CEILING: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Source {
    Addresses,
    From,
    Subgraph,
}

impl Source {
    pub fn of(args: &InitArgs) -> Self {
        if args.from.is_some() {
            Source::From
        } else if args.from_subgraph.is_some() {
            Source::Subgraph
        } else {
            Source::Addresses
        }
    }
}

/// The nest an `init` just created, as far as the payload is allowed to know it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Nest {
    pub chain: u64,
    pub source: Source,
}

impl Nest {
    pub fn new(chain_id: u64, source: Source) -> Self {
        Nest {
            chain: registry_chain(chain_id),
            source,
        }
    }
}

/// A chain id outside the built-in registry can single out one operator, so it is sent as 0.
pub fn registry_chain(chain_id: u64) -> u64 {
    if crate::chains::lookup_by_id(chain_id).is_some() {
        chain_id
    } else {
        0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Event {
    /// `None` from `count on`, which has no nest; the keys are then absent, not null.
    Counted(Option<Nest>),
    Init(Nest),
}

/// The v1 payload. A new field is an RFC amendment and a `v` bump, and A3 fails until it is one.
#[derive(Debug, PartialEq, Serialize)]
pub struct Payload {
    v: u8,
    event: &'static str,
    version: &'static str,
    os: &'static str,
    arch: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    chain: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<Source>,
}

impl Event {
    pub fn payload(self) -> Payload {
        let (event, nest) = match self {
            Event::Counted(nest) => ("counted", nest),
            Event::Init(nest) => ("init", Some(nest)),
        };
        Payload {
            v: 1,
            event,
            version: env!("CARGO_PKG_VERSION"),
            os: std::env::consts::OS,
            arch: std::env::consts::ARCH,
            chain: nest.map(|n| n.chain),
            source: nest.map(|n| n.source),
        }
    }

    pub fn json(self) -> String {
        serde_json::to_string(&self.payload()).expect("the payload is plain data")
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Answer {
    pub counted: bool,
    pub asked_at: String,
}

impl Answer {
    pub fn load(path: &Path) -> Option<Answer> {
        toml::from_str(&std::fs::read_to_string(path).ok()?).ok()
    }

    pub fn store(&self, path: &Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let body = toml::to_string(self).expect("the answer is plain data");
        std::fs::write(
            path,
            format!("# nuthatch count - see `nuthatch count --help`\n{body}"),
        )
    }

    fn new(counted: bool) -> Answer {
        Answer {
            counted,
            asked_at: today(),
        }
    }
}

fn today() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let (y, m, d) = crate::publish::civil_from_days((secs / 86_400) as i64);
    format!("{y:04}-{m:02}-{d:02}")
}

/// `$XDG_CONFIG_HOME/nuthatch/count.toml`, else `$HOME/.config/nuthatch/count.toml`. `None` when
/// neither is usable: with nowhere to keep the answer, the question would be asked every time.
pub fn config_path_from(
    xdg: Option<std::ffi::OsString>,
    home: Option<std::ffi::OsString>,
) -> Option<PathBuf> {
    let xdg = xdg.map(PathBuf::from).filter(|p| p.is_absolute());
    let base = xdg.or_else(|| {
        home.map(PathBuf::from)
            .filter(|p| p.is_absolute())
            .map(|h| h.join(".config"))
    })?;
    Some(base.join("nuthatch").join("count.toml"))
}

pub fn config_path() -> Option<PathBuf> {
    config_path_from(
        std::env::var_os("XDG_CONFIG_HOME"),
        std::env::var_os("HOME"),
    )
}

/// What `init` does about the count, decided before anything is printed or sent.
#[derive(Debug, PartialEq, Eq)]
pub enum Decision {
    Silent,
    Ask,
    Send,
}

/// `CI` and `NUTHATCH_NO_COUNT` silence a stored yes as well as the question: a CI job's `init`
/// is a test run, not a nest someone made.
pub fn decide(stored: Option<&Answer>, interactive: bool, ci: bool, opted_out: bool) -> Decision {
    if ci || opted_out {
        return Decision::Silent;
    }
    match stored {
        Some(a) if a.counted => Decision::Send,
        Some(_) => Decision::Silent,
        None if interactive => Decision::Ask,
        None => Decision::Silent,
    }
}

pub fn prompt_text(nest: Nest) -> String {
    format!(
        "\nnuthatch has no idea how many people use it. Can it count you?\n\n\
         If you say yes it will send this, once now and once per future `init`, to\n\
         {ENDPOINT} and nothing else, ever:\n\n  {}\n\n\
         No identifier, no addresses. The handler stores no IP, header or body. The totals\n\
         are public at {TOTALS}. `nuthatch count off` reverses\n\
         this.\n\nCount me? [y/N] ",
        Event::Init(nest).json()
    )
}

/// Asks once and stores the answer, whatever it is, so a no is never asked again.
pub fn ask(path: &Path, nest: Nest, mut input: impl BufRead, mut output: impl Write) -> bool {
    let _ = write!(output, "{}", prompt_text(nest));
    let _ = output.flush();
    let mut line = String::new();
    let yes = input.read_line(&mut line).is_ok()
        && matches!(line.trim().to_ascii_lowercase().as_str(), "y" | "yes");
    if let Err(e) = Answer::new(yes).store(path) {
        tracing::debug!(
            "count: could not store the answer at {}: {e}",
            path.display()
        );
    }
    yes
}

/// One attempt per event under one shared ceiling. Never a `Result`: nothing may react to it.
pub async fn send_to(url: &str, events: &[Event]) {
    let work = async {
        let client = match reqwest::Client::builder().user_agent(USER_AGENT).build() {
            Ok(c) => c,
            Err(e) => return tracing::debug!("count: no client: {e}"),
        };
        for ev in events {
            if let Err(e) = client.post(url).json(&ev.payload()).send().await {
                tracing::debug!("count: {e}");
            }
        }
    };
    if tokio::time::timeout(CEILING, work).await.is_err() {
        tracing::debug!("count: gave up after {CEILING:?}");
    }
}

/// Captured before `init` runs, asked after it has succeeded and printed its last line.
pub struct Pending {
    dir: PathBuf,
    source: Source,
}

impl Pending {
    pub fn of(args: &InitArgs) -> Self {
        Pending {
            dir: crate::project::target_dir(args),
            source: Source::of(args),
        }
    }

    pub async fn ask(self) {
        let Some(path) = config_path() else { return };
        // Input already waiting is the next line of a paste, not an answer to a question not yet
        // printed (#2235): leave it to the shell and ask at a later init.
        let interactive = std::io::stdin().is_terminal()
            && std::io::stdout().is_terminal()
            && !typed_ahead(&std::io::stdin());
        let decision = decide(
            Answer::load(&path).as_ref(),
            interactive,
            std::env::var_os("CI").is_some(),
            std::env::var_os("NUTHATCH_NO_COUNT").is_some(),
        );
        let nest = Nest::new(chain_in(&self.dir), self.source);
        match decision {
            Decision::Silent => {}
            Decision::Send => send_to(ENDPOINT, &[Event::Init(nest)]).await,
            Decision::Ask => {
                if ask(&path, nest, std::io::stdin().lock(), std::io::stdout()) {
                    send_to(ENDPOINT, &[Event::Counted(Some(nest)), Event::Init(nest)]).await;
                }
            }
        }
    }
}

/// Whether `input` has bytes ready to read right now.
#[cfg(unix)]
fn typed_ahead(input: &impl std::os::fd::AsRawFd) -> bool {
    let mut fd = libc::pollfd {
        fd: input.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    let ready = unsafe { libc::poll(&mut fd, 1, 0) };
    ready > 0 && fd.revents & libc::POLLIN != 0
}

#[cfg(not(unix))]
fn typed_ahead<T>(_: &T) -> bool {
    false
}

/// Read back from what `init` wrote, so every arm of `init` is counted without each passing it on.
fn chain_in(dir: &Path) -> u64 {
    std::fs::read_to_string(dir.join("nuthatch.toml"))
        .ok()
        .and_then(|s| s.parse::<toml::Table>().ok())
        .and_then(|t| t.get("nest")?.get("chain_id")?.as_integer())
        .and_then(|id| u64::try_from(id).ok())
        .unwrap_or(0)
}

/// Opt in to, or out of, the head count (RFC-0054). Off until you say yes.
///
/// With a yes, `nuthatch init` sends one small JSON object per nest created, with no identifier and
/// no addresses: `nuthatch count payload` prints it. With no action, prints where things stand.
#[derive(Debug, clap::Args)]
pub struct CountArgs {
    #[command(subcommand)]
    pub action: Option<CountAction>,
}

#[derive(Debug, clap::Subcommand)]
pub enum CountAction {
    /// Say yes. Sends one `counted` event now, with no chain and no source.
    On,
    /// Say no. Nothing is sent, ever, until you say `on`.
    Off,
    /// Print both objects this machine can send, populated.
    Payload,
    /// Delete the stored answer, so the next `init` may ask again.
    Forget,
}

pub async fn run(args: CountArgs) -> anyhow::Result<()> {
    let path = config_path().ok_or_else(|| {
        anyhow::anyhow!(
            "neither XDG_CONFIG_HOME nor HOME is set, so there is nowhere to keep an answer"
        )
    })?;
    match args.action {
        None => {
            let state = match Answer::load(&path) {
                Some(a) if a.counted => format!("on (since {})", a.asked_at),
                Some(a) => format!("off (since {})", a.asked_at),
                None => "never asked".to_string(),
            };
            println!("count:   {state}");
            println!("answer:  {}", path.display());
            println!("sends:   {}", Event::Init(cwd_nest()).json());
            println!("to:      {ENDPOINT}");
        }
        Some(CountAction::On) => {
            Answer::new(true).store(&path)?;
            println!("✓ counted. `nuthatch count off` reverses this.");
            send_to(ENDPOINT, &[Event::Counted(None)]).await;
        }
        Some(CountAction::Off) => {
            Answer::new(false).store(&path)?;
            println!("✓ not counted. Nothing is sent.");
        }
        Some(CountAction::Payload) => {
            println!("{}", Event::Counted(None).json());
            println!("{}", Event::Init(cwd_nest()).json());
        }
        Some(CountAction::Forget) => match std::fs::remove_file(&path) {
            Ok(()) => println!("✓ forgot {}", path.display()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => println!("nothing stored"),
            Err(e) => return Err(e.into()),
        },
    }
    Ok(())
}

/// The nest in the current directory if there is one; otherwise mainnet, as an example.
fn cwd_nest() -> Nest {
    let chain = match chain_in(Path::new(".")) {
        0 => 1,
        id => id,
    };
    Nest::new(chain, Source::Addresses)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn keys(json: &str) -> BTreeSet<String> {
        let v: serde_json::Value = serde_json::from_str(json).unwrap();
        v.as_object().unwrap().keys().cloned().collect()
    }

    fn set(ks: &[&str]) -> BTreeSet<String> {
        ks.iter().map(|s| s.to_string()).collect()
    }

    const SOURCES: [Source; 3] = [Source::Addresses, Source::From, Source::Subgraph];

    // #2235: a pasted next line is already waiting when the question would be asked.
    #[cfg(unix)]
    #[test]
    fn input_waiting_before_the_question_is_seen() {
        use std::os::fd::FromRawFd;
        let mut fds = [0; 2];
        assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
        let (read, mut write) = unsafe {
            (
                std::fs::File::from_raw_fd(fds[0]),
                std::fs::File::from_raw_fd(fds[1]),
            )
        };
        assert!(!typed_ahead(&read));
        write.write_all(b"nuthatch dev --backfill 300\n").unwrap();
        assert!(typed_ahead(&read));
    }

    fn init_args(argv: &[&str]) -> InitArgs {
        use clap::Parser;
        match crate::cli::Cli::try_parse_from(argv).unwrap().command {
            crate::cli::Command::Init(a) => a,
            _ => unreachable!(),
        }
    }

    // #2189: `--from` with the default `--dir` writes the nest under the source's name, so the chain is
    // read from there, not from whatever `./nuthatch.toml` happens to be.
    #[test]
    fn the_chain_is_read_from_the_directory_init_wrote() {
        let from = init_args(&[
            "nuthatch",
            "init",
            "--from",
            "https://github.com/o/mynest.git",
        ]);
        assert_eq!(Pending::of(&from).dir, PathBuf::from("mynest"));
        let given = init_args(&["nuthatch", "init", "--from", "./a", "--dir", "b"]);
        assert_eq!(Pending::of(&given).dir, PathBuf::from("b"));
        let addresses = init_args(&[
            "nuthatch",
            "init",
            "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        ]);
        assert_eq!(Pending::of(&addresses).dir, PathBuf::from("."));
    }

    // A3: the field set is the RFC's, exactly. Adding one fails here until the RFC is amended.
    #[test]
    fn an_init_payload_carries_exactly_the_v1_fields_for_every_chain_and_source() {
        for chain in crate::chains::all() {
            for source in SOURCES {
                let json = Event::Init(Nest::new(chain.chain_id, source)).json();
                assert_eq!(
                    keys(&json),
                    set(&["v", "event", "version", "os", "arch", "chain", "source"]),
                    "{json}"
                );
                let v: serde_json::Value = serde_json::from_str(&json).unwrap();
                assert_eq!(v["v"], 1);
                assert_eq!(v["event"], "init");
                assert_eq!(v["chain"], chain.chain_id);
            }
        }
    }

    #[test]
    fn a_chain_outside_the_registry_is_sent_as_zero() {
        let unregistered = 987_654_321;
        assert!(crate::chains::lookup_by_id(unregistered).is_none());
        let v: serde_json::Value =
            serde_json::from_str(&Event::Init(Nest::new(unregistered, Source::Addresses)).json())
                .unwrap();
        assert_eq!(v["chain"], 0);
    }

    #[test]
    fn a_bare_counted_payload_has_no_chain_or_source_keys_not_nulls() {
        let json = Event::Counted(None).json();
        assert_eq!(
            keys(&json),
            set(&["v", "event", "version", "os", "arch"]),
            "{json}"
        );
        assert!(!json.contains("null"), "{json}");
        assert!(json.contains(r#""event":"counted""#));
    }

    #[test]
    fn a_counted_after_init_carries_the_same_chain_and_source_as_the_init_that_follows() {
        let nest = Nest::new(42161, Source::Subgraph);
        let counted: serde_json::Value =
            serde_json::from_str(&Event::Counted(Some(nest)).json()).unwrap();
        let init: serde_json::Value = serde_json::from_str(&Event::Init(nest).json()).unwrap();
        assert_eq!(counted["event"], "counted");
        assert_eq!(counted["chain"], init["chain"]);
        assert_eq!(counted["source"], init["source"]);
        assert_eq!(init["source"], "subgraph");
    }

    #[test]
    fn the_prompt_shows_the_payload_it_asks_about() {
        let nest = Nest::new(1, Source::Addresses);
        let text = prompt_text(nest);
        assert!(text.contains(&Event::Init(nest).json()));
        assert!(text.contains(ENDPOINT));
        assert!(text.trim_end().ends_with("[y/N]"));
    }

    #[test]
    fn nothing_is_asked_or_sent_without_a_terminal_or_under_ci_or_the_opt_out() {
        let yes = Answer::new(true);
        let no = Answer::new(false);
        assert_eq!(decide(None, false, false, false), Decision::Silent);
        assert_eq!(decide(None, true, true, false), Decision::Silent);
        assert_eq!(decide(None, true, false, true), Decision::Silent);
        assert_eq!(decide(Some(&yes), true, true, false), Decision::Silent);
        assert_eq!(decide(Some(&yes), true, false, true), Decision::Silent);
        assert_eq!(decide(Some(&no), true, false, false), Decision::Silent);
        assert_eq!(decide(None, true, false, false), Decision::Ask);
        // A stored yes counts a scripted `init` too: the person already said yes.
        assert_eq!(decide(Some(&yes), false, false, false), Decision::Send);
    }

    #[test]
    fn enter_and_anything_but_yes_is_no() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nuthatch/count.toml");
        let nest = Nest::new(1, Source::Addresses);
        for (typed, want) in [
            ("\n", false),
            ("", false),
            ("n\n", false),
            ("yeah\n", false),
            ("Y\n", true),
            (" yes \n", true),
        ] {
            let got = ask(&path, nest, typed.as_bytes(), Vec::new());
            assert_eq!(got, want, "{typed:?}");
            assert_eq!(Answer::load(&path).unwrap().counted, want);
        }
    }

    // A6: the answer is stored whatever it is, so the second `init` is never asked.
    #[test]
    fn a_stored_answer_means_the_question_is_asked_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nuthatch/count.toml");
        let nest = Nest::new(1, Source::Addresses);
        assert_eq!(
            decide(Answer::load(&path).as_ref(), true, false, false),
            Decision::Ask
        );
        let mut out = Vec::new();
        ask(&path, nest, "\n".as_bytes(), &mut out);
        assert!(String::from_utf8(out).unwrap().contains("Count me?"));
        assert_eq!(
            decide(Answer::load(&path).as_ref(), true, false, false),
            Decision::Silent
        );
        std::fs::remove_file(&path).unwrap();
        assert_eq!(
            decide(Answer::load(&path).as_ref(), true, false, false),
            Decision::Ask
        );
        let stored = std::fs::read_to_string(dir.path().join("nuthatch/count.toml"));
        assert!(stored.is_err());
    }

    #[test]
    fn the_answer_file_is_the_documented_shape() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("count.toml");
        Answer {
            counted: false,
            asked_at: "2026-09-11".into(),
        }
        .store(&path)
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "# nuthatch count - see `nuthatch count --help`\ncounted = false\nasked_at = \"2026-09-11\"\n"
        );
    }

    #[test]
    fn the_answer_lives_under_xdg_config_home_then_home() {
        let p = |x: Option<&str>, h: Option<&str>| {
            config_path_from(x.map(Into::into), h.map(Into::into))
        };
        assert_eq!(
            p(Some("/x"), Some("/h")),
            Some(PathBuf::from("/x/nuthatch/count.toml"))
        );
        assert_eq!(
            p(None, Some("/h")),
            Some(PathBuf::from("/h/.config/nuthatch/count.toml"))
        );
        assert_eq!(
            p(Some("relative"), Some("/h")),
            Some(PathBuf::from("/h/.config/nuthatch/count.toml"))
        );
        assert_eq!(p(None, None), None);
    }

    #[test]
    fn the_chain_is_read_back_from_the_nest_init_wrote() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(chain_in(dir.path()), 0);
        std::fs::write(
            dir.path().join("nuthatch.toml"),
            "[nest]\nname = \"x\"\nchain_id = 42161\n",
        )
        .unwrap();
        assert_eq!(chain_in(dir.path()), 42161);
    }

    /// Accepts one connection, returns the raw request, answers 204.
    async fn one_request() -> (String, tokio::task::JoinHandle<String>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut chunk = [0u8; 4096];
            loop {
                let n = sock.read(&mut chunk).await.unwrap();
                buf.extend_from_slice(&chunk[..n]);
                let text = String::from_utf8_lossy(&buf).to_string();
                if let Some(end) = text.find("\r\n\r\n") {
                    let len = text[..end]
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    if buf.len() >= end + 4 + len || n == 0 {
                        break;
                    }
                }
            }
            sock.write_all(b"HTTP/1.1 204 No Content\r\nconnection: close\r\n\r\n")
                .await
                .unwrap();
            String::from_utf8(buf).unwrap()
        });
        (url, handle)
    }

    #[tokio::test]
    async fn what_goes_on_the_wire_is_the_payload_and_a_fixed_user_agent() {
        let (url, handle) = one_request().await;
        let ev = Event::Init(Nest::new(1, Source::From));
        send_to(&url, &[ev]).await;
        let req = handle.await.unwrap();
        let (head, body) = req.split_once("\r\n\r\n").unwrap();
        assert_eq!(body, ev.json());
        assert!(head.starts_with("POST / HTTP/1.1"), "{head}");
        let head = head.to_ascii_lowercase();
        assert!(head.contains("user-agent: nuthatch-count/1"), "{head}");
        let names: BTreeSet<&str> = head
            .lines()
            .skip(1)
            .filter_map(|l| l.split_once(':'))
            .map(|(n, _)| n)
            .collect();
        assert_eq!(
            names,
            BTreeSet::from([
                "accept",
                "content-length",
                "content-type",
                "host",
                "user-agent"
            ]),
            "{head}"
        );
    }

    // A2's timing half: a receiver that never answers costs at most the ceiling, once.
    #[tokio::test]
    async fn a_receiver_that_never_answers_costs_one_ceiling_for_all_events() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let _hold = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((sock, _)) = listener.accept().await {
                held.push(sock);
            }
        });
        let nest = Nest::new(1, Source::Addresses);
        let started = std::time::Instant::now();
        send_to(&url, &[Event::Counted(Some(nest)), Event::Init(nest)]).await;
        let took = started.elapsed();
        assert!(
            took >= CEILING && took < CEILING + Duration::from_millis(500),
            "{took:?}"
        );
    }

    #[tokio::test]
    async fn a_refused_connection_returns_quietly_and_at_once() {
        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        let started = std::time::Instant::now();
        send_to(&format!("http://127.0.0.1:{port}"), &[Event::Counted(None)]).await;
        assert!(started.elapsed() < Duration::from_secs(1));
    }
}
