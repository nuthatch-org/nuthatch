//! `nuthatch watch` (RFC-0069): a read-only view of a running nest, from the endpoints it already
//! serves and nothing else. On a terminal it draws the engine room; piped, or under `TERM=dumb`, it
//! writes one line per nest per interval.

pub mod line;
pub mod picker;
pub mod prom;
pub mod screen;
pub mod series;
pub mod state;
pub mod theme;
pub mod tunnel;

use std::io::{BufRead, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use tokio::sync::mpsc;

use state::{App, Read};

#[derive(clap::Args, Debug)]
pub struct WatchArgs {
    /// The nest's API: a solo `dev`, or a runtime's root, whose mounts are each watched. Defaults
    /// to http://127.0.0.1:8288, or to the named nest's.
    #[arg(long)]
    pub url: Option<String>,

    /// Reach the nest through an ssh forward to this host, for a nest bound to loopback there. Runs
    /// `ssh -N -L` in batch mode and reopens it if it drops.
    #[arg(long, value_name = "HOST")]
    pub ssh: Option<String>,

    /// Take --url and --ssh from this entry in ~/.config/nuthatch/watch.toml. With no target named
    /// and nests configured there, the screen offers the list.
    #[arg(long, value_name = "NAME")]
    pub nest: Option<String>,

    /// Seconds between reads (and between lines when piped). By default the screen reads `/ready`
    /// every second and `/metrics` every two; a line comes every poll interval of the nest, held
    /// between 2 and 30 s.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    pub interval: Option<u64>,

    /// Append every body read to this file, one JSON object per line, for --replay.
    #[arg(long, value_name = "FILE")]
    pub record: Option<PathBuf>,

    /// Draw from a recording made with --record instead of a live nest.
    #[arg(long, value_name = "FILE", conflicts_with_all = ["record", "url", "ssh", "nest"])]
    pub replay: Option<PathBuf>,

    /// Replay at this multiple of the recorded pace.
    #[arg(long, default_value_t = 1.0, requires = "replay")]
    pub speed: f64,

    /// Stop after this many seconds.
    #[arg(long, value_name = "SECS")]
    pub exit_after: Option<u64>,
}

pub async fn run(args: WatchArgs) -> Result<()> {
    let on_screen =
        std::io::stdout().is_terminal() && std::env::var("TERM").map_or(true, |t| t != "dumb");
    if let Some(path) = &args.replay {
        if !on_screen {
            bail!("--replay draws the screen and needs a terminal");
        }
        return run_replay(path, args.speed, args.exit_after).await;
    }
    let nests = picker::load_nests()?;
    let mut nest = args.nest.clone();
    if on_screen && nest.is_none() && args.url.is_none() && args.ssh.is_none() && !nests.is_empty()
    {
        match picker::pick_nest(&nests, &theme::Theme::detect())? {
            Some(name) => nest = Some(name),
            None => return Ok(()),
        }
    }
    let target = picker::resolve(
        args.url.as_deref(),
        args.ssh.as_deref(),
        nest.as_deref(),
        &nests,
    )?;
    let url = target
        .url
        .clone()
        .unwrap_or_else(|| picker::DEFAULT_URL.into());
    let mut tunnel = match &target.ssh {
        Some(host) => Some(tunnel::Tunnel::open("ssh", host, &url)?),
        None => None,
    };
    let base = tunnel.as_ref().map_or(url.clone(), |t| t.local_url.clone());
    let recorder = open_recorder(args.record.as_deref())?;
    if on_screen {
        let described = match &target.ssh {
            Some(host) => format!("{url} via {host}"),
            None => url,
        };
        run_live_screen(
            &base,
            described,
            args.interval,
            recorder,
            tunnel.as_mut(),
            args.exit_after,
        )
        .await
    } else {
        run_lines(&base, args.interval, recorder, args.exit_after).await
    }
}

fn open_recorder(path: Option<&Path>) -> Result<Option<std::fs::File>> {
    path.map(|path| {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("opening {}", path.display()))
    })
    .transpose()
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// The watcher is a client like any other, so a proxy's log names it.
fn client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .user_agent(concat!("nuthatch-watch/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("building the HTTP client")
}

pub async fn get(client: &reqwest::Client, base: &str, route: &str) -> Read {
    fetch(client, base, route, "", None).await
}

/// A read of `route` with `query` appended, recorded under `route` alone, and with the admin token
/// as a bearer header when one is given: never in the URL, where a proxy's log would keep it.
async fn fetch(
    client: &reqwest::Client,
    base: &str,
    route: &str,
    query: &str,
    token: Option<&str>,
) -> Read {
    let t_ms = unix_ms();
    let mut request = client.get(format!("{base}{route}{query}"));
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let (status, body) = match request.send().await {
        Ok(response) => {
            let status = response.status().as_u16();
            (status, response.text().await.unwrap_or_default())
        }
        Err(_) => (0, String::new()),
    };
    Read {
        t_ms,
        route: route.to_string(),
        status,
        body,
        cycle: None,
    }
}

/// One pass over every route the line reads: `/ready`, `/`, `/metrics` and `/nests` together, then
/// each mount's `/<name>/ready` when `/nests` lists any.
async fn read_cycle(client: &reqwest::Client, base: &str, cycle: u64) -> Vec<Read> {
    let (ready, root, metrics, nests) = tokio::join!(
        get(client, base, "/ready"),
        get(client, base, "/"),
        get(client, base, "/metrics"),
        get(client, base, "/nests"),
    );
    let mut probe = App::default();
    probe.apply(&nests);
    let routes: Vec<String> = probe
        .mounts()
        .unwrap_or_default()
        .iter()
        .map(|m| format!("/{}/ready", m.name))
        .collect();
    let per_nest = futures::future::join_all(routes.iter().map(|r| get(client, base, r))).await;
    let mut reads = vec![ready, root, metrics, nests];
    reads.extend(per_nest);
    for r in &mut reads {
        r.cycle = Some(cycle);
    }
    reads
}

async fn run_lines(
    base: &str,
    interval: Option<u64>,
    mut recorder: Option<std::fs::File>,
    exit_after: Option<u64>,
) -> Result<()> {
    let client = client()?;
    let started = Instant::now();
    let mut app = App::default();
    let mut stdout = std::io::stdout().lock();
    for cycle in 1.. {
        let began = Instant::now();
        for read in read_cycle(&client, base, cycle).await {
            if let Some(file) = recorder.as_mut() {
                writeln!(file, "{}", serde_json::to_string(&read)?)?;
            }
            app.apply(&read);
        }
        for l in line::lines(&app) {
            // A closed pipe (`| head`) is the reader leaving, not a fault.
            if writeln!(stdout, "{l}").is_err() {
                return Ok(());
            }
        }
        if stdout.flush().is_err() {
            return Ok(());
        }
        let every = Duration::from_secs(
            interval.unwrap_or_else(|| app.poll_interval_secs().unwrap_or(2).clamp(2, 30)),
        );
        let wait = every.saturating_sub(began.elapsed());
        if exit_after.is_some_and(|s| started.elapsed() + wait > Duration::from_secs(s)) {
            return Ok(());
        }
        tokio::time::sleep(wait).await;
    }
    Ok(())
}

/// The screen's reads: `/ready` (or each mount's), `/` and `/_admin/requests` every `every` seconds,
/// `/metrics` every two of those unless --interval set one period for all, and `/nests` every thirty.
async fn poll_screen(
    client: reqwest::Client,
    base: String,
    interval: Option<u64>,
    token: Option<String>,
    tx: mpsc::UnboundedSender<Read>,
) {
    // Where each `/_admin/requests` read left off, so the next asks only for what is new.
    let mut since: std::collections::BTreeMap<String, u64> = Default::default();
    let every = interval.unwrap_or(1);
    let metrics_every = if interval.is_some() { 1 } else { 2 };
    let nests_every = (30 / every).max(1);
    let mut mounts: Option<Vec<String>> = None;
    let mut tick = tokio::time::interval(Duration::from_secs(every));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    for n in 0u64.. {
        tick.tick().await;
        let mut routes: Vec<String> = Vec::new();
        if n.is_multiple_of(nests_every) {
            let read = get(&client, &base, "/nests").await;
            let mut probe = App::default();
            probe.apply(&read);
            if probe.shape_known() {
                mounts = Some(
                    probe
                        .mounts()
                        .unwrap_or_default()
                        .into_iter()
                        .map(|m| m.name)
                        .collect(),
                );
            }
            if tx.send(read).is_err() {
                return;
            }
        }
        match &mounts {
            Some(m) if !m.is_empty() => routes.extend(m.iter().map(|m| format!("/{m}/ready"))),
            _ => routes.extend(["/ready".to_string(), "/".to_string()]),
        }
        if n.is_multiple_of(metrics_every) {
            routes.push("/metrics".into());
        }
        let feeds: Vec<String> = match &mounts {
            Some(m) if !m.is_empty() => m.iter().map(|m| format!("/{m}/_admin/requests")).collect(),
            _ => vec!["/_admin/requests".to_string()],
        };
        let queries: Vec<String> = feeds
            .iter()
            .map(|f| format!("?since={}", since.get(f).copied().unwrap_or(0)))
            .collect();
        let (reads, fed) = tokio::join!(
            futures::future::join_all(routes.iter().map(|r| get(&client, &base, r))),
            futures::future::join_all(feeds.iter().zip(&queries).map(|(f, q)| fetch(
                &client,
                &base,
                f,
                q,
                token.as_deref()
            ))),
        );
        for read in &fed {
            if let Some(last) = serde_json::from_str::<serde_json::Value>(&read.body)
                .ok()
                .and_then(|v| v.get("last_seq").and_then(serde_json::Value::as_u64))
            {
                since.insert(read.route.clone(), last);
            }
        }
        for read in reads.into_iter().chain(fed) {
            if tx.send(read).is_err() {
                return;
            }
        }
    }
}

async fn run_live_screen(
    base: &str,
    described: String,
    interval: Option<u64>,
    mut recorder: Option<std::fs::File>,
    mut tunnel: Option<&mut tunnel::Tunnel>,
    exit_after: Option<u64>,
) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel::<Read>();
    let token = std::env::var("NUTHATCH_ADMIN_TOKEN")
        .ok()
        .filter(|t| !t.is_empty());
    tokio::spawn(poll_screen(
        client()?,
        base.to_string(),
        interval,
        token,
        tx,
    ));
    let every = interval.unwrap_or(1);
    let footer = format!(
        "reading {described} · /ready every {every} s, /metrics every {} s · n N mount · q quits",
        if interval.is_some() { every } else { 2 }
    );
    let mut app = App::default();
    let mut terminal = ratatui::init();
    let started = Instant::now();
    let result = (|| -> Result<()> {
        loop {
            while let Ok(read) = rx.try_recv() {
                if let Some(file) = recorder.as_mut() {
                    writeln!(file, "{}", serde_json::to_string(&read)?)?;
                }
                app.apply(&read);
            }
            let note = tunnel.as_mut().and_then(|t| t.supervise());
            let screen = screen::Screen {
                app: &app,
                theme: theme::Theme::detect(),
                now_ms: unix_ms(),
                stale_after_ms: screen::stale_after_ms(every),
                footer: note.unwrap_or_else(|| footer.clone()),
            };
            terminal.draw(|frame| screen.draw(frame))?;
            if exit_after.is_some_and(|s| started.elapsed() >= Duration::from_secs(s)) {
                return Ok(());
            }
            if !handle_keys(&mut app)? {
                return Ok(());
            }
        }
    })();
    ratatui::restore();
    result
}

/// Waits up to a quarter second for a key. False when the key asks to quit.
fn handle_keys(app: &mut App) -> Result<bool> {
    tokio::task::block_in_place(|| -> Result<bool> {
        if !event::poll(Duration::from_millis(250))? {
            return Ok(true);
        }
        if let Event::Key(key) = event::read()? {
            if key.kind != KeyEventKind::Press {
                return Ok(true);
            }
            match key.code {
                KeyCode::Char('q') | KeyCode::Esc => return Ok(false),
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    return Ok(false)
                }
                KeyCode::Char('n') => app.step(true),
                KeyCode::Char('N') => app.step(false),
                _ => {}
            }
        }
        Ok(true)
    })
}

pub fn load_recording(path: &Path) -> Result<Vec<Read>> {
    let file = std::fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut reads = Vec::new();
    for (n, line) in std::io::BufReader::new(file).lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let read: Read = serde_json::from_str(&line)
            .with_context(|| format!("{} line {}", path.display(), n + 1))?;
        reads.push(read);
    }
    reads.sort_by_key(|r| r.t_ms);
    Ok(reads)
}

async fn run_replay(path: &Path, speed: f64, exit_after: Option<u64>) -> Result<()> {
    let reads = load_recording(path)?;
    let Some(origin_ms) = reads.first().map(|r| r.t_ms) else {
        bail!("{} holds no reads", path.display());
    };
    let speed = if speed > 0.0 { speed } else { 1.0 };
    let footer = format!(
        "replaying {} at {speed}x, recorded from a live nest from {} UTC · q quits",
        path.file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default(),
        utc(origin_ms / 1000)
    );
    let mut app = App::default();
    let mut terminal = ratatui::init();
    let started = Instant::now();
    let mut pending = reads.into_iter().peekable();
    let result = (|| -> Result<()> {
        loop {
            let now_ms = origin_ms + (started.elapsed().as_secs_f64() * 1000.0 * speed) as u64;
            while let Some(read) = pending.next_if(|r| r.t_ms <= now_ms) {
                app.apply(&read);
            }
            let screen = screen::Screen {
                app: &app,
                theme: theme::Theme::detect(),
                now_ms,
                stale_after_ms: screen::stale_after_ms(1),
                footer: footer.clone(),
            };
            terminal.draw(|frame| screen.draw(frame))?;
            if exit_after.is_some_and(|s| started.elapsed() >= Duration::from_secs(s)) {
                return Ok(());
            }
            if !handle_keys(&mut app)? {
                return Ok(());
            }
        }
    })();
    ratatui::restore();
    result
}

/// `YYYY-MM-DD HH:MM:SS` from unix seconds (Hinnant's civil-from-days).
pub fn utc(secs: u64) -> String {
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

/// The watcher's own source, for the tests that hold it to its rules.
#[cfg(test)]
pub(crate) const SOURCES: &[(&str, &str)] = &[
    ("line.rs", include_str!("line.rs")),
    ("mod.rs", include_str!("mod.rs")),
    ("picker.rs", include_str!("picker.rs")),
    ("prom.rs", include_str!("prom.rs")),
    ("screen.rs", include_str!("screen.rs")),
    ("series.rs", include_str!("series.rs")),
    ("state.rs", include_str!("state.rs")),
    ("theme.rs", include_str!("theme.rs")),
    ("tunnel.rs", include_str!("tunnel.rs")),
];

#[cfg(test)]
mod tests;
