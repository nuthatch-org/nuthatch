//! RFC-0069 S0: the engine room, a prototype of `nuthatch watch`.
//!
//! A read-only client of `/ready`, `/`, `/metrics` and `/nests`, drawn with ratatui. Live reads and
//! a recording replayed with `--replay` go through the same `App::apply`, so a replayed screen is
//! the screen those bodies would have drawn. Not merged: `wire.patch` compiles it into the binary
//! behind the `watch` feature for S0's measurements.

use std::collections::VecDeque;
use std::io::{BufRead, IsTerminal, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Frame;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;

const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
const CURSOR_BUDGET_BYTES: f64 = 2.0 * GIB;

#[derive(clap::Args, Debug)]
pub struct WatchArgs {
    /// The nest's API.
    #[arg(long, default_value = "http://127.0.0.1:8288")]
    pub url: String,

    /// Refresh every panel at this many seconds instead of the defaults (`/ready` and `/` 1 s,
    /// `/metrics` 2 s).
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    pub interval: Option<u64>,

    /// Append every body read to this file, one JSON object per line, for `--replay`.
    #[arg(long, value_name = "FILE")]
    pub record: Option<PathBuf>,

    /// Draw from a recording instead of a live nest.
    #[arg(long, value_name = "FILE", conflicts_with = "record")]
    pub replay: Option<PathBuf>,

    /// Replay at this multiple of the recorded pace.
    #[arg(long, default_value_t = 1.0)]
    pub speed: f64,

    /// Quit after this many seconds of wall clock.
    #[arg(long, value_name = "SECS")]
    pub exit_after: Option<u64>,
}

/// One read of one route: what a live poll produced, or one line of a recording.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Read {
    t_ms: u64,
    route: String,
    /// 0 when nothing answered.
    status: u16,
    body: String,
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Unix milliseconds as the screen should see them: the wall clock live, the recording's own
/// clock on replay.
struct Clock {
    origin_ms: u64,
    started: Instant,
    speed: f64,
}

impl Clock {
    fn now_ms(&self) -> u64 {
        self.origin_ms + (self.started.elapsed().as_secs_f64() * 1000.0 * self.speed) as u64
    }
}

pub async fn run(args: WatchArgs) -> Result<()> {
    if !std::io::stdout().is_terminal() {
        bail!("`nuthatch watch` draws a screen and needs a terminal");
    }
    let (tx, mut rx) = mpsc::unbounded_channel::<Read>();
    let base = args.url.trim_end_matches('/').to_string();
    let every = |default: u64| Duration::from_secs(args.interval.unwrap_or(default));
    let (clock, source) = match &args.replay {
        Some(path) => {
            let reads = load_recording(path)?;
            let Some(first) = reads.first() else {
                bail!("{} holds no reads", path.display());
            };
            let origin_ms = first.t_ms;
            let speed = if args.speed > 0.0 { args.speed } else { 1.0 };
            tokio::spawn(replay(reads, speed, tx));
            let name = path.file_name().map(|n| n.to_string_lossy().into_owned());
            (
                Clock {
                    origin_ms,
                    started: Instant::now(),
                    speed,
                },
                Source::Replay {
                    file: name.unwrap_or_default(),
                    speed,
                    origin_ms,
                },
            )
        }
        None => {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .context("building the HTTP client")?;
            let token = std::env::var("NUTHATCH_ADMIN_TOKEN")
                .ok()
                .filter(|t| !t.is_empty());
            let mut routes = vec![
                ("/ready", every(1)),
                ("/", every(1)),
                ("/metrics", every(2)),
                ("/nests", every(30)),
            ];
            if token.is_some() {
                routes.push(("/_admin/requests", every(1)));
            }
            let recorder = match &args.record {
                Some(path) => Some(
                    std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(path)
                        .with_context(|| format!("opening {}", path.display()))?,
                ),
                None => None,
            };
            let (fan_tx, mut fan_rx) = mpsc::unbounded_channel::<Read>();
            for (route, period) in routes {
                tokio::spawn(poll(
                    client.clone(),
                    base.clone(),
                    route,
                    period,
                    token.clone(),
                    fan_tx.clone(),
                ));
            }
            tokio::spawn(async move {
                let mut recorder = recorder;
                while let Some(read) = fan_rx.recv().await {
                    if let Some(file) = recorder.as_mut() {
                        if let Ok(line) = serde_json::to_string(&read) {
                            let _ = writeln!(file, "{line}");
                        }
                    }
                    if tx.send(read).is_err() {
                        break;
                    }
                }
            });
            (
                Clock {
                    origin_ms: unix_ms(),
                    started: Instant::now(),
                    speed: 1.0,
                },
                Source::Live {
                    url: base.clone(),
                    token: token.is_some(),
                    every: every(1).as_secs(),
                },
            )
        }
    };

    let mut app = App::new(source);
    let mut terminal = ratatui::init();
    let started = Instant::now();
    let result = (|| -> Result<()> {
        loop {
            while let Ok(read) = rx.try_recv() {
                app.apply(read);
            }
            let now = clock.now_ms();
            terminal.draw(|frame| app.draw(frame, now))?;
            if let Some(secs) = args.exit_after {
                if started.elapsed() >= Duration::from_secs(secs) {
                    return Ok(());
                }
            }
            let pressed = tokio::task::block_in_place(|| -> Result<bool> {
                if !event::poll(Duration::from_millis(250))? {
                    return Ok(false);
                }
                Ok(match event::read()? {
                    Event::Key(key) if key.kind == KeyEventKind::Press => {
                        matches!(key.code, KeyCode::Char('q') | KeyCode::Esc)
                            || (key.code == KeyCode::Char('c')
                                && key.modifiers.contains(KeyModifiers::CONTROL))
                    }
                    _ => false,
                })
            })?;
            if pressed {
                return Ok(());
            }
        }
    })();
    ratatui::restore();
    result
}

async fn poll(
    client: reqwest::Client,
    base: String,
    route: &'static str,
    period: Duration,
    token: Option<String>,
    tx: mpsc::UnboundedSender<Read>,
) {
    let mut tick = tokio::time::interval(period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let mut request = client.get(format!("{base}{route}"));
        if route.starts_with("/_admin") {
            if let Some(token) = &token {
                request = request.bearer_auth(token);
            }
        }
        let t_ms = unix_ms();
        let (status, body) = match request.send().await {
            Ok(response) => {
                let status = response.status().as_u16();
                (status, response.text().await.unwrap_or_default())
            }
            Err(_) => (0, String::new()),
        };
        let read = Read {
            t_ms,
            route: route.to_string(),
            status,
            body,
        };
        if tx.send(read).is_err() {
            return;
        }
    }
}

fn load_recording(path: &PathBuf) -> Result<Vec<Read>> {
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

async fn replay(reads: Vec<Read>, speed: f64, tx: mpsc::UnboundedSender<Read>) {
    let started = Instant::now();
    let origin = reads.first().map_or(0, |r| r.t_ms);
    for read in reads {
        let due = Duration::from_secs_f64((read.t_ms - origin) as f64 / 1000.0 / speed);
        if let Some(wait) = due.checked_sub(started.elapsed()) {
            tokio::time::sleep(wait).await;
        }
        if tx.send(read).is_err() {
            return;
        }
    }
}

// --- Prometheus text, as much of it as the panels read. ---

type Series = (String, Vec<(String, String)>, f64);

#[derive(Debug, Default, Clone)]
struct Metrics {
    series: Vec<Series>,
}

impl Metrics {
    fn parse(text: &str) -> Self {
        let mut series = Vec::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (head, value) = match line.rfind(' ') {
                Some(i) => (&line[..i], &line[i + 1..]),
                None => continue,
            };
            let Ok(value) = value.parse::<f64>() else {
                continue;
            };
            let (name, labels) = match head.find('{') {
                Some(i) => (
                    &head[..i],
                    parse_labels(&head[i + 1..head.len().saturating_sub(1)]),
                ),
                None => (head, Vec::new()),
            };
            series.push((name.to_string(), labels, value));
        }
        Self { series }
    }

    /// The value of the series with every label in `want`, and no others asked about.
    fn get(&self, name: &str, want: &[(&str, &str)]) -> Option<f64> {
        self.series
            .iter()
            .find(|(n, labels, _)| {
                n == name
                    && want
                        .iter()
                        .all(|(k, v)| labels.iter().any(|(lk, lv)| lk == k && lv == v))
            })
            .map(|(_, _, v)| *v)
    }

    fn label_values(&self, name: &str, label: &str) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for (n, labels, _) in &self.series {
            if n != name {
                continue;
            }
            for (k, v) in labels {
                if k == label && !out.contains(v) {
                    out.push(v.clone());
                }
            }
        }
        out
    }
}

fn parse_labels(s: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut chars = s.chars().peekable();
    loop {
        let key: String = chars.by_ref().take_while(|c| *c != '=').collect();
        let key = key.trim_start_matches(',').trim().to_string();
        if key.is_empty() || chars.next() != Some('"') {
            break;
        }
        let mut value = String::new();
        while let Some(c) = chars.next() {
            match c {
                '\\' => {
                    if let Some(next) = chars.next() {
                        value.push(if next == 'n' { '\n' } else { next });
                    }
                }
                '"' => break,
                c => value.push(c),
            }
        }
        out.push((key, value));
    }
    out
}

// --- State. ---

enum Source {
    Live {
        url: String,
        token: bool,
        every: u64,
    },
    Replay {
        file: String,
        speed: f64,
        origin_ms: u64,
    },
}

#[derive(Default)]
struct RouteState {
    body: Option<(u64, Value)>,
    last_status: Option<(u64, u16)>,
}

struct App {
    source: Source,
    ready: RouteState,
    root: RouteState,
    nests: RouteState,
    requests_status: Option<u16>,
    metrics: Option<(u64, Metrics)>,
    prev_metrics: Option<(u64, Metrics)>,
    /// One per read that saw a newer poll: the blocks that poll brought.
    arrivals: VecDeque<u64>,
    last_poll: Option<u64>,
    last_block: Option<u64>,
    /// One per advance of `sealed_through`: the blocks it advanced.
    stamps: VecDeque<u64>,
    last_sealed: Option<u64>,
}

const STRIP: usize = 160;

impl App {
    fn new(source: Source) -> Self {
        Self {
            source,
            ready: RouteState::default(),
            root: RouteState::default(),
            nests: RouteState::default(),
            requests_status: None,
            metrics: None,
            prev_metrics: None,
            arrivals: VecDeque::new(),
            last_poll: None,
            last_block: None,
            stamps: VecDeque::new(),
            last_sealed: None,
        }
    }

    fn apply(&mut self, read: Read) {
        let t = read.t_ms;
        match read.route.as_str() {
            "/ready" => {
                self.ready.last_status = Some((t, read.status));
                // A 503 still carries the reasons; only a non-answer is nothing.
                if let Ok(body) = serde_json::from_str::<Value>(&read.body) {
                    self.observe_ready(&body);
                    self.ready.body = Some((t, body));
                }
            }
            "/" => {
                self.root.last_status = Some((t, read.status));
                if read.status == 200 {
                    if let Ok(body) = serde_json::from_str::<Value>(&read.body) {
                        self.root.body = Some((t, body));
                    }
                }
            }
            "/nests" => {
                self.nests.last_status = Some((t, read.status));
                if read.status == 200 {
                    if let Ok(body) = serde_json::from_str::<Value>(&read.body) {
                        self.nests.body = Some((t, body));
                    }
                }
            }
            "/metrics" if read.status == 200 => {
                let parsed = Metrics::parse(&read.body);
                self.prev_metrics = self.metrics.replace((t, parsed));
            }
            "/_admin/requests" => self.requests_status = Some(read.status),
            _ => {}
        }
    }

    fn observe_ready(&mut self, body: &Value) {
        let poll = body.get("last_poll_unixtime").and_then(Value::as_u64);
        let block = body.get("last_block").and_then(Value::as_u64);
        if let (Some(poll), Some(block)) = (poll, block) {
            if let (Some(prev_poll), Some(prev_block)) = (self.last_poll, self.last_block) {
                if poll > prev_poll {
                    push_capped(&mut self.arrivals, block.saturating_sub(prev_block));
                }
            }
            self.last_poll = Some(poll);
            self.last_block = Some(block);
        }
        if let Some(sealed) = body.get("sealed_through").and_then(Value::as_u64) {
            if let Some(prev) = self.last_sealed {
                if sealed > prev {
                    push_capped(&mut self.stamps, sealed - prev);
                }
            }
            self.last_sealed = Some(sealed);
        }
    }

    fn ready_u64(&self, key: &str) -> Option<u64> {
        self.ready.body.as_ref()?.1.get(key)?.as_u64()
    }

    fn ready_bool(&self, key: &str) -> bool {
        self.ready
            .body
            .as_ref()
            .and_then(|(_, b)| b.get(key))
            .and_then(Value::as_bool)
            .unwrap_or(false)
    }

    fn root_str(&self, key: &str) -> Option<String> {
        let v = self.root.body.as_ref()?.1.get(key)?;
        v.as_str()
            .map(str::to_string)
            .or_else(|| (!v.is_null()).then(|| v.to_string()))
    }

    fn nest_name(&self) -> Option<String> {
        let (_, m) = self.metrics.as_ref()?;
        m.label_values("nuthatch_nest_last_block", "nest")
            .into_iter()
            .next()
    }

    /// A series for this nest, or the process-wide one on a nest too old to label it.
    fn metric(&self, nest_name: &str, global: &str) -> Option<f64> {
        let (_, m) = self.metrics.as_ref()?;
        if let Some(nest) = self.nest_name() {
            if let Some(v) = m.get(nest_name, &[("nest", nest.as_str())]) {
                return Some(v);
            }
        }
        m.get(global, &[])
    }

    fn delta(&self, name: &str, labels: &[(&str, &str)]) -> Option<(f64, f64)> {
        let (t1, now) = self.metrics.as_ref()?;
        let (t0, prev) = self.prev_metrics.as_ref()?;
        let secs = (*t1 as f64 - *t0 as f64) / 1000.0;
        Some((now.get(name, labels)? - prev.get(name, labels)?, secs))
    }

    fn cursors(&self) -> usize {
        let Some((_, body)) = &self.nests.body else {
            return 1;
        };
        let list = body
            .as_array()
            .or_else(|| body.get("nests").and_then(Value::as_array));
        let mut chains: Vec<&str> = list
            .into_iter()
            .flatten()
            .filter_map(|n| n.get("chain").and_then(Value::as_str))
            .collect();
        chains.sort_unstable();
        chains.dedup();
        chains.len().max(1)
    }

    fn stale(&self, now: u64) -> bool {
        match (&self.ready.body, &self.ready.last_status) {
            (_, Some((_, 0))) => true,
            (Some((t, _)), _) => now.saturating_sub(*t) > 5_000,
            (None, _) => false,
        }
    }

    fn state(&self, now: u64) -> (&'static str, Ink) {
        if self.ready.body.is_none() {
            return ("NO SIGNAL", Ink::Faint);
        }
        if self.stale(now) {
            return ("STALE", Ink::Rust);
        }
        if self.ready_bool("quarantined") {
            return ("QUARANTINED", Ink::Rust);
        }
        let views_failed = self
            .ready
            .body
            .as_ref()
            .and_then(|(_, b)| b.get("views_failed"))
            .and_then(Value::as_array)
            .is_some_and(|a| !a.is_empty());
        let attention = [
            "stalled",
            "wedged",
            "initial_poll_failed",
            "seal_faulted",
            "seal_direct_stalled",
            "tip_seal_stalled",
            "entities_stalled",
        ]
        .iter()
        .any(|k| self.ready_bool(k));
        if attention || views_failed || !self.ready_bool("ready") {
            return ("ATTENTION", Ink::Ember);
        }
        if self.ready_bool("seal_direct_active")
            || self.ready_u64("lag_blocks").is_some_and(|l| l > 10)
        {
            return ("BACKFILL", Ink::Brass);
        }
        ("LIVE", Ink::Verdigris)
    }

    // --- Drawing. ---

    fn draw(&self, frame: &mut Frame, now: u64) {
        let theme = Theme::detect();
        let [plate, top, middle, bottom, _, foot] = Layout::vertical([
            Constraint::Length(1),
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Length(5),
            Constraint::Min(0),
            Constraint::Length(1),
        ])
        .areas(frame.area());
        let halves = |area| {
            Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)]).areas(area)
        };
        let [tip, boiler] = halves(top);
        let [press, feed] = halves(middle);
        let [reorg, dispatch] = halves(bottom);

        frame.render_widget(Paragraph::new(self.nameplate(&theme, now)), plate);
        self.panel(
            frame,
            &theme,
            tip,
            "THE TIP",
            self.the_tip(&theme, now, tip.width),
        );
        self.panel(
            frame,
            &theme,
            press,
            "THE PRESS",
            self.the_press(&theme, press.width),
        );
        self.panel(frame, &theme, reorg, "REORG", self.reorg(&theme, now));
        self.panel(
            frame,
            &theme,
            boiler,
            "BOILER",
            self.boiler(&theme, boiler.width),
        );
        self.panel(frame, &theme, feed, "FEED LINES", self.feed_lines(&theme));
        self.panel(frame, &theme, dispatch, "DISPATCH", self.dispatch(&theme));
        frame.render_widget(Paragraph::new(self.footer(&theme)), foot);
    }

    fn panel(
        &self,
        frame: &mut Frame,
        theme: &Theme,
        area: Rect,
        title: &str,
        lines: Vec<Line<'static>>,
    ) {
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(theme.fg(Ink::Seam))
            .title(Span::styled(
                format!(" {title} "),
                theme.fg(Ink::Brass).add_modifier(Modifier::BOLD),
            ));
        frame.render_widget(Paragraph::new(lines).block(block), area);
    }

    fn nameplate(&self, theme: &Theme, now: u64) -> Line<'static> {
        let (state, ink) = self.state(now);
        let name = self.nest_name().unwrap_or_else(|| "NO SIGNAL".into());
        let chain = self.root_str("chain").unwrap_or_else(|| "NO SIGNAL".into());
        let version = self
            .ready
            .body
            .as_ref()
            .and_then(|(_, b)| b.get("version"))
            .and_then(Value::as_str)
            .map(|v| format!("nuthatch {v}"))
            .unwrap_or_else(|| "NO SIGNAL".into());
        let mode = self
            .ready
            .body
            .as_ref()
            .and_then(|(_, b)| b.pointer("/freshness/mode"))
            .and_then(Value::as_str)
            .unwrap_or("NO SIGNAL")
            .to_string();
        let nid = self
            .root_str("nid")
            .map(|n| n.chars().take(12).collect::<String>())
            .unwrap_or_else(|| "NO SIGNAL".into());
        let sep = || Span::styled("  ·  ", theme.fg(Ink::Seam));
        Line::from(vec![
            Span::styled(
                " THE ENGINE ROOM  ",
                theme.fg(Ink::BrassHi).add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                name.to_uppercase(),
                theme.fg(Ink::Body).add_modifier(Modifier::BOLD),
            ),
            sep(),
            Span::styled(chain, theme.fg(Ink::Copper)),
            sep(),
            Span::styled(version, theme.fg(Ink::Dim)),
            sep(),
            Span::styled(format!("freshness {mode}"), theme.fg(Ink::Dim)),
            sep(),
            Span::styled(format!("nid {nid}"), theme.fg(Ink::Dim)),
            sep(),
            Span::styled(format!(" {state} "), theme.state(ink)),
        ])
    }

    fn the_tip(&self, theme: &Theme, now: u64, width: u16) -> Vec<Line<'static>> {
        let tip = self.ready_u64("tip");
        let indexed = self.ready_u64("last_block");
        let lag = self.ready_u64("lag_blocks");
        let interval = self
            .ready
            .body
            .as_ref()
            .and_then(|(_, b)| b.pointer("/freshness/poll_interval_secs"))
            .and_then(Value::as_u64);
        let since = self
            .ready_u64("seconds_since_poll")
            .zip(self.ready.body.as_ref().map(|(t, _)| *t));
        let poll = match (interval, since) {
            (Some(every), Some((since, read_at))) => {
                let since = since + now.saturating_sub(read_at) / 1000;
                let next = every.saturating_sub(since);
                let next = if next == 0 {
                    "due".to_string()
                } else {
                    format!("in {next} s")
                };
                vec![
                    theme.value(format!("every {every} s")),
                    theme.dim(format!("  ·  last {since} s ago  ·  next ")),
                    theme.value(next),
                ]
            }
            _ => vec![theme.no_signal()],
        };
        let strip_w = width.saturating_sub(15) as usize;
        vec![
            row(theme, "TIP", vec![opt_block(theme, tip, Ink::BrassHi)]),
            row(
                theme,
                "INDEXED",
                vec![opt_block(theme, indexed, Ink::Copper)],
            ),
            row(
                theme,
                "LAG",
                match lag {
                    Some(l) => vec![theme.value(format!("{} blocks", thousands(l)))],
                    None => vec![theme.no_signal()],
                },
            ),
            row(theme, "POLL", poll),
            row(
                theme,
                "ARRIVALS",
                vec![arrival_strip(theme, &self.arrivals, strip_w)],
            ),
            Line::from(Span::styled(
                "            one mark per poll seen; height is the blocks it brought",
                theme.fg(Ink::Faint).add_modifier(Modifier::DIM),
            )),
        ]
    }

    fn the_press(&self, theme: &Theme, width: u16) -> Vec<Line<'static>> {
        let sealed = self.ready_u64("sealed_through").filter(|s| *s > 0);
        let seal_lag = self.ready_u64("seal_lag_blocks");
        let rows = self.metric(
            "nuthatch_nest_rows_sealed_total",
            "nuthatch_rows_sealed_total",
        );
        let bytes = self.metric(
            "nuthatch_nest_sealed_segments_bytes",
            "nuthatch_sealed_segments_bytes",
        );
        let mut lines = vec![
            row(
                theme,
                "SEALED",
                match sealed {
                    Some(s) => vec![theme.dim("through ".into()), theme.value(thousands(s))],
                    None => vec![theme.no_signal(), theme.dim("  nothing sealed yet".into())],
                },
            ),
            row(
                theme,
                "SEAL LAG",
                match seal_lag {
                    Some(l) => vec![theme.value(format!("{} blocks", thousands(l)))],
                    None => vec![theme.no_signal()],
                },
            ),
            row(
                theme,
                "OUTPUT",
                vec![
                    rows.map_or_else(
                        || theme.no_signal(),
                        |r| theme.value(format!("{} rows this run", thousands(r as u64))),
                    ),
                    theme.dim("  ·  ".into()),
                    bytes.map_or_else(
                        || theme.no_signal(),
                        |b| theme.value(format!("{} on disk", human_bytes(b))),
                    ),
                ],
            ),
        ];
        if self.ready_bool("seal_direct_active") {
            let fetched = self.ready_u64("seal_direct_fetched");
            let done = self.ready_u64("seal_direct_completed");
            let target = self.ready_u64("seal_direct_target");
            lines.push(row(
                theme,
                "DIRECT",
                vec![
                    theme.dim("fetched ".into()),
                    opt_block(theme, fetched, Ink::Copper),
                    theme.dim("  sealed ".into()),
                    opt_block(theme, done, Ink::Copper),
                    theme.dim("  of ".into()),
                    opt_block(theme, target, Ink::Copper),
                ],
            ));
        } else {
            lines.push(row(
                theme,
                "DIRECT",
                vec![theme.dim("no seal-direct pass running".into())],
            ));
        }
        lines.push(row(
            theme,
            "STAMPS",
            vec![stamp_strip(
                theme,
                &self.stamps,
                width.saturating_sub(15) as usize,
            )],
        ));
        lines.push(Line::from(Span::styled(
            "            one mark per advance of sealed_through",
            theme.fg(Ink::Faint).add_modifier(Modifier::DIM),
        )));
        lines
    }

    fn reorg(&self, theme: &Theme, now: u64) -> Vec<Line<'static>> {
        let count = self.metric("nuthatch_nest_reorgs_total", "nuthatch_reorgs_total");
        let depth = self.metric(
            "nuthatch_nest_last_reorg_depth_blocks",
            "nuthatch_nest_last_reorg_depth_blocks",
        );
        let at = self.metric(
            "nuthatch_nest_last_reorg_unixtime",
            "nuthatch_nest_last_reorg_unixtime",
        );
        vec![
            row(
                theme,
                "REORGS",
                vec![count.map_or_else(
                    || theme.no_signal(),
                    |c| theme.value(format!("{} since the process started", thousands(c as u64))),
                )],
            ),
            row(
                theme,
                "LAST",
                vec![
                    theme.dim("depth ".into()),
                    depth.map_or_else(|| theme.no_signal(), |d| theme.value(format!("{d} blocks"))),
                    theme.dim("  ·  age ".into()),
                    at.map_or_else(
                        || theme.no_signal(),
                        |a| theme.value(format!("{} s", (now / 1000).saturating_sub(a as u64))),
                    ),
                ],
            ),
        ]
    }

    fn boiler(&self, theme: &Theme, width: u16) -> Vec<Line<'static>> {
        let rss = self
            .metrics
            .as_ref()
            .and_then(|(_, m)| m.get("nuthatch_rss_bytes", &[]));
        let cursors = self.cursors();
        let budget = CURSOR_BUDGET_BYTES * cursors as f64;
        let gauge_w = width.saturating_sub(15).min(50) as usize;
        let gauge = match rss {
            Some(rss) => {
                let filled = ((rss / budget) * gauge_w as f64)
                    .round()
                    .clamp(0.0, gauge_w as f64) as usize;
                let ink = if rss > budget { Ink::Rust } else { Ink::Brass };
                vec![
                    Span::styled("▓".repeat(filled), theme.fg(ink)),
                    Span::styled("░".repeat(gauge_w - filled), theme.fg(Ink::Seam)),
                ]
            }
            None => vec![theme.no_signal()],
        };
        let reading = match rss {
            Some(rss) => vec![
                theme.value(human_bytes(rss)),
                theme.dim(format!(
                    " of {} ({} cursor{} x 2 GiB, process RSS)",
                    human_bytes(budget),
                    cursors,
                    if cursors == 1 { "" } else { "s" }
                )),
            ],
            None => vec![theme.no_signal()],
        };
        let cpu = self
            .delta("nuthatch_process_cpu_seconds_total", &[])
            .and_then(|(d, secs)| {
                (secs > 0.0).then(|| format!("{:.1}% of a core", 100.0 * d / secs))
            });
        let reserved = self.metric(
            "nuthatch_nest_analytics_pool_reserved_bytes",
            "nuthatch_analytics_pool_reserved_bytes",
        );
        let peak = self.metric(
            "nuthatch_nest_analytics_pool_peak_bytes",
            "nuthatch_analytics_pool_peak_bytes",
        );
        let hot = self.metric("nuthatch_nest_hot_store_bytes", "nuthatch_hot_store_bytes");
        let sealed = self.metric(
            "nuthatch_nest_sealed_segments_bytes",
            "nuthatch_sealed_segments_bytes",
        );
        let b =
            |v: Option<f64>| v.map_or_else(|| theme.no_signal(), |v| theme.value(human_bytes(v)));
        vec![
            row(theme, "RSS", gauge),
            row(theme, "", reading),
            row(
                theme,
                "CPU",
                vec![cpu.map_or_else(|| theme.no_signal(), |c| theme.value(c))],
            ),
            row(
                theme,
                "POOL",
                vec![
                    theme.dim("reserved ".into()),
                    b(reserved),
                    theme.dim("  ·  peak ".into()),
                    b(peak),
                ],
            ),
            row(
                theme,
                "STORES",
                vec![
                    theme.dim("hot ".into()),
                    b(hot),
                    theme.dim("  ·  sealed ".into()),
                    b(sealed),
                ],
            ),
        ]
    }

    fn feed_lines(&self, theme: &Theme) -> Vec<Line<'static>> {
        let Some((_, m)) = &self.metrics else {
            return vec![row(theme, "", vec![theme.no_signal()])];
        };
        let endpoints = m.label_values("nuthatch_rpc_endpoint_requests_total", "endpoint");
        if endpoints.is_empty() {
            return vec![row(theme, "", vec![theme.no_signal()])];
        }
        let mut lines = vec![Line::from(Span::styled(
            format!(
                " {:<30} {:>7} {:>6} {:>6} {:>9}",
                "ENDPOINT", "REQ", "FAIL", "RETRY", "MEAN"
            ),
            theme.fg(Ink::Faint).add_modifier(Modifier::BOLD),
        ))];
        for e in &endpoints {
            let l = [("endpoint", e.as_str())];
            let total = m
                .get("nuthatch_rpc_endpoint_requests_total", &l)
                .unwrap_or(0.0);
            let fails = m.get("nuthatch_rpc_endpoint_failures_total", &l);
            let retries = m.get("nuthatch_rpc_endpoint_retries_total", &l);
            let d_req = self
                .delta("nuthatch_rpc_endpoint_requests_total", &l)
                .map(|d| d.0);
            let d_fail = self
                .delta("nuthatch_rpc_endpoint_failures_total", &l)
                .map(|d| d.0);
            let d_retry = self
                .delta("nuthatch_rpc_endpoint_retries_total", &l)
                .map(|d| d.0);
            let d_sum = self
                .delta("nuthatch_rpc_request_duration_seconds_sum", &l)
                .map(|d| d.0);
            let d_count = self
                .delta("nuthatch_rpc_request_duration_seconds_count", &l)
                .map(|d| d.0);
            let lit = d_req.is_some_and(|d| d > 0.0);
            let mean = match (d_sum, d_count) {
                (Some(s), Some(c)) if c > 0.0 => format!("{:.0} ms", 1000.0 * s / c),
                (Some(_), Some(_)) => "idle".into(),
                _ => "NO SIGNAL".into(),
            };
            let name: String = e.chars().take(30).collect();
            let mut spans = vec![
                Span::styled(
                    format!(" {name:<30} "),
                    if lit {
                        theme.fg(Ink::BrassHi)
                    } else {
                        theme.fg(Ink::Dim)
                    },
                ),
                theme.value(format!("{:>7}", thousands(total as u64))),
                Span::styled(
                    format!(" {:>6}", fails.map_or("-".into(), |f| thousands(f as u64))),
                    if d_fail.is_some_and(|d| d > 0.0) {
                        theme.fg(Ink::Rust)
                    } else {
                        theme.fg(Ink::Copper)
                    },
                ),
                theme.value(format!(
                    " {:>6}",
                    retries.map_or("-".into(), |r| thousands(r as u64))
                )),
                theme.value(format!(" {mean:>9}")),
            ];
            if d_retry.is_some_and(|d| d > 0.0) {
                spans.push(Span::styled(
                    "  FAILOVER",
                    theme.fg(Ink::Ember).add_modifier(Modifier::BOLD),
                ));
            }
            lines.push(Line::from(spans));
        }
        lines.push(Line::from(Span::styled(
            " totals since start  ·  lit: asked last interval  ·  mean: last interval",
            theme.fg(Ink::Faint).add_modifier(Modifier::DIM),
        )));
        lines
    }

    fn dispatch(&self, theme: &Theme) -> Vec<Line<'static>> {
        let feed = match (&self.source, self.requests_status) {
            (Source::Live { token: false, .. }, _) | (Source::Replay { .. }, None) => vec![
                Span::styled(
                    "NO TOKEN",
                    theme.fg(Ink::Ember).add_modifier(Modifier::BOLD),
                ),
                theme.dim("  the request feed needs NUTHATCH_ADMIN_TOKEN".into()),
            ],
            (_, Some(200)) => vec![theme.value("reading /_admin/requests".into())],
            (_, Some(status)) => vec![
                Span::styled(
                    "NOT SERVED",
                    theme.fg(Ink::Ember).add_modifier(Modifier::BOLD),
                ),
                theme.dim(format!(
                    "  /_admin/requests answered {status}; it arrives with S3"
                )),
            ],
            (_, None) => vec![theme.no_signal()],
        };
        let http = self
            .metrics
            .as_ref()
            .and_then(|(_, m)| m.get("nuthatch_http_requests_total", &[]));
        let sql = self
            .metrics
            .as_ref()
            .and_then(|(_, m)| m.get("nuthatch_sql_queries_total", &[]));
        let rate = self
            .delta("nuthatch_http_requests_total", &[])
            .and_then(|(d, secs)| (secs > 0.0).then(|| format!("  ·  {:.1}/s", d / secs)));
        vec![
            row(theme, "FEED", feed),
            row(
                theme,
                "HTTP",
                vec![
                    http.map_or_else(
                        || theme.no_signal(),
                        |h| theme.value(format!("{} requests", thousands(h as u64))),
                    ),
                    theme.dim(rate.unwrap_or_default()),
                    theme.dim("  (this watcher's included)".into()),
                ],
            ),
            row(
                theme,
                "SQL",
                vec![sql.map_or_else(
                    || theme.no_signal(),
                    |s| theme.value(format!("{} queries", thousands(s as u64))),
                )],
            ),
        ]
    }

    fn footer(&self, theme: &Theme) -> Line<'static> {
        let text = match &self.source {
            Source::Live { url, every, .. } => {
                format!(" reading {url}  ·  /ready and / every {every} s, /metrics every {} s  ·  q quits", every * 2)
            }
            Source::Replay {
                file,
                speed,
                origin_ms,
            } => format!(
                " replaying {file} at {speed}x, recorded from a live nest at {} UTC  ·  q quits",
                utc(*origin_ms / 1000)
            ),
        };
        Line::from(Span::styled(text, theme.fg(Ink::Faint)))
    }
}

fn push_capped(strip: &mut VecDeque<u64>, v: u64) {
    if strip.len() == STRIP {
        strip.pop_front();
    }
    strip.push_back(v);
}

fn row(theme: &Theme, label: &str, mut value: Vec<Span<'static>>) -> Line<'static> {
    let mut spans = vec![Span::styled(format!(" {label:<10} "), theme.label())];
    spans.append(&mut value);
    Line::from(spans)
}

fn opt_block(theme: &Theme, v: Option<u64>, ink: Ink) -> Span<'static> {
    match v {
        Some(v) => Span::styled(thousands(v), theme.fg(ink).add_modifier(Modifier::BOLD)),
        None => theme.no_signal(),
    }
}

fn arrival_strip(theme: &Theme, marks: &VecDeque<u64>, width: usize) -> Span<'static> {
    if marks.is_empty() {
        return Span::styled("no poll seen yet", theme.fg(Ink::Faint));
    }
    // Two cells a mark, so neighbouring polls read as separate events.
    const BARS: [char; 6] = ['▃', '▄', '▅', '▆', '▇', '█'];
    let s: String = marks
        .iter()
        .skip(marks.len().saturating_sub(width / 2))
        .flat_map(|&b| {
            let mark = if b == 0 {
                '·'
            } else {
                BARS[(b as usize - 1).min(5)]
            };
            [mark, ' ']
        })
        .collect();
    Span::styled(s, theme.fg(Ink::Copper))
}

/// `YYYY-MM-DD HH:MM:SS` from unix seconds (Hinnant's civil-from-days).
fn utc(secs: u64) -> String {
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

fn stamp_strip(theme: &Theme, marks: &VecDeque<u64>, width: usize) -> Span<'static> {
    if marks.is_empty() {
        return Span::styled("none since this watcher started", theme.fg(Ink::Faint));
    }
    let s: String = marks
        .iter()
        .skip(marks.len().saturating_sub(width))
        .map(|_| '▮')
        .collect();
    Span::styled(s, theme.fg(Ink::Brass))
}

fn thousands(v: u64) -> String {
    let s = v.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

fn human_bytes(b: f64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = b;
    let mut unit = 0;
    while v >= 1024.0 && unit < UNITS.len() - 1 {
        v /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{v:.0} B")
    } else {
        format!("{v:.1} {}", UNITS[unit])
    }
}

// --- The Foundry palette (steampunk-ui, terminal annex). ---

#[derive(Clone, Copy)]
enum Ink {
    Body,
    Dim,
    Faint,
    Copper,
    Brass,
    BrassHi,
    Verdigris,
    Ember,
    Rust,
    Seam,
}

#[derive(Clone, Copy)]
enum ColorMode {
    True,
    Indexed,
    None,
}

struct Theme {
    mode: ColorMode,
}

impl Theme {
    fn detect() -> Self {
        let mode = if std::env::var_os("NO_COLOR").is_some_and(|v| !v.is_empty()) {
            ColorMode::None
        } else if std::env::var("COLORTERM")
            .is_ok_and(|v| v.contains("truecolor") || v.contains("24bit"))
        {
            ColorMode::True
        } else {
            ColorMode::Indexed
        };
        Self { mode }
    }

    fn color(&self, ink: Ink) -> Option<Color> {
        let (rgb, index) = match ink {
            Ink::Body => ((232, 223, 205), 187),
            Ink::Dim => ((162, 147, 122), 144),
            Ink::Faint => ((157, 143, 117), 144),
            Ink::Copper => ((205, 124, 72), 173),
            Ink::Brass => ((201, 162, 39), 178),
            Ink::BrassHi => ((240, 216, 145), 222),
            Ink::Verdigris => ((95, 158, 125), 72),
            Ink::Ember => ((232, 146, 42), 172),
            Ink::Rust => ((232, 105, 81), 209),
            Ink::Seam => ((77, 66, 49), 59),
        };
        match self.mode {
            ColorMode::True => Some(Color::Rgb(rgb.0, rgb.1, rgb.2)),
            ColorMode::Indexed => Some(Color::Indexed(index)),
            ColorMode::None => None,
        }
    }

    fn fg(&self, ink: Ink) -> Style {
        match self.color(ink) {
            Some(c) => Style::new().fg(c),
            None => Style::new(),
        }
    }

    fn label(&self) -> Style {
        self.fg(Ink::Faint).add_modifier(Modifier::BOLD)
    }

    fn value(&self, s: String) -> Span<'static> {
        Span::styled(s, self.fg(Ink::Copper))
    }

    fn dim(&self, s: String) -> Span<'static> {
        Span::styled(s, self.fg(Ink::Dim))
    }

    fn no_signal(&self) -> Span<'static> {
        Span::styled(
            "NO SIGNAL",
            self.fg(Ink::Faint).add_modifier(Modifier::BOLD),
        )
    }

    /// The state word: reversed, so it carries without colour.
    fn state(&self, ink: Ink) -> Style {
        self.fg(ink)
            .add_modifier(Modifier::REVERSED | Modifier::BOLD)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_parse_labels_and_values() {
        let m = Metrics::parse(
            "# HELP x\nnuthatch_rss_bytes 50987008\nnuthatch_rpc_endpoint_requests_total{endpoint=\"eth.drpc.org\"} 6\n",
        );
        assert_eq!(m.get("nuthatch_rss_bytes", &[]), Some(50_987_008.0));
        assert_eq!(
            m.get(
                "nuthatch_rpc_endpoint_requests_total",
                &[("endpoint", "eth.drpc.org")]
            ),
            Some(6.0)
        );
        assert_eq!(
            m.label_values("nuthatch_rpc_endpoint_requests_total", "endpoint"),
            vec!["eth.drpc.org"]
        );
    }

    #[test]
    fn utc_formats_known_instants() {
        assert_eq!(utc(0), "1970-01-01 00:00:00");
        assert_eq!(utc(1_700_000_000), "2023-11-14 22:13:20");
    }

    #[test]
    fn absent_seal_reads_no_signal_not_zero() {
        let mut app = App::new(Source::Replay {
            file: String::new(),
            speed: 1.0,
            origin_ms: 0,
        });
        app.apply(Read {
            t_ms: 1,
            route: "/ready".into(),
            status: 200,
            body: r#"{"ready":true,"sealed_through":0,"seal_lag_blocks":null}"#.into(),
        });
        let theme = Theme {
            mode: ColorMode::None,
        };
        let text: String = app
            .the_press(&theme, 80)
            .iter()
            .flat_map(|l| l.spans.iter().map(|s| s.content.to_string()))
            .collect();
        assert!(text.contains("NO SIGNAL"));
        assert!(!text.contains("through 0"));
    }
}
