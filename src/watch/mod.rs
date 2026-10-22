//! `nuthatch watch` (RFC-0069): a read-only view of a running nest, from the endpoints it already
//! serves and nothing else.

pub mod line;
pub mod prom;
pub mod state;

use std::io::Write;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};

use state::{App, Read};

#[derive(clap::Args, Debug)]
pub struct WatchArgs {
    /// The nest's API: a solo `dev`, or a runtime's root, whose mounts are each watched.
    #[arg(long, default_value = "http://127.0.0.1:8288")]
    pub url: String,

    /// Seconds between lines. Defaults to the nest's own poll interval, held between 2 and 30 s.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    pub interval: Option<u64>,

    /// Append every body read to this file, one JSON object per line.
    #[arg(long, value_name = "FILE")]
    pub record: Option<PathBuf>,

    /// Stop after this many seconds.
    #[arg(long, value_name = "SECS")]
    pub exit_after: Option<u64>,
}

pub async fn run(args: WatchArgs) -> Result<()> {
    run_lines(args).await
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// What the watcher sends with every request: it is a client like any other, so a proxy's log
/// names it.
fn client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .user_agent(concat!("nuthatch-watch/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("building the HTTP client")
}

pub async fn get(client: &reqwest::Client, base: &str, route: &str) -> Read {
    let t_ms = unix_ms();
    let (status, body) = match client.get(format!("{base}{route}")).send().await {
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
    let mounts = probe.mounts().unwrap_or_default();
    let routes: Vec<String> = mounts
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

async fn run_lines(args: WatchArgs) -> Result<()> {
    let base = args.url.trim_end_matches('/').to_string();
    let client = client()?;
    let mut recorder = match &args.record {
        Some(path) => Some(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .with_context(|| format!("opening {}", path.display()))?,
        ),
        None => None,
    };
    let started = std::time::Instant::now();
    let mut app = App::default();
    let mut stdout = std::io::stdout().lock();
    for cycle in 1.. {
        let began = std::time::Instant::now();
        for read in read_cycle(&client, &base, cycle).await {
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
            args.interval
                .unwrap_or_else(|| app.poll_interval_secs().unwrap_or(2).clamp(2, 30)),
        );
        if args
            .exit_after
            .is_some_and(|s| started.elapsed() + every.saturating_sub(began.elapsed()) > Duration::from_secs(s))
        {
            return Ok(());
        }
        tokio::time::sleep(every.saturating_sub(began.elapsed())).await;
    }
    Ok(())
}
