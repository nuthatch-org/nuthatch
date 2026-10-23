//! The engine room, drawn. Every figure comes from [`App`]; nothing here reads a clock but the
//! `now` it is handed, so a recording draws the same screen every time.

use std::collections::VecDeque;

use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Frame;
use serde_json::Value;

use super::series;
use super::state::{App, Cursor, Dispatch, NestView, RouteState, Served};
use super::theme::{Ink, Theme};

/// Below this the panels stack into one column of readouts and the strips go (RFC-0069 §9).
pub const FULL_WIDTH: u16 = 80;
pub const FULL_HEIGHT: u16 = 24;
/// Below this only the nameplate and the numbers.
pub const TERSE_WIDTH: u16 = 60;
/// A panel this wide inside its border has room for the long forms and the hints.
const ROOMY: u16 = 50;
/// A cursor gets its own column when the screen gives each at least this many.
const SIDE_BY_SIDE: u16 = 70;

/// How long `/ready` may go unanswered before the screen says the nest is not being heard.
pub fn stale_after_ms(read_every_secs: u64) -> u64 {
    (3 * read_every_secs * 1000).max(5_000)
}

pub struct Screen<'a> {
    pub app: &'a App,
    pub theme: Theme,
    pub now_ms: u64,
    pub stale_after_ms: u64,
    pub footer: String,
}

/// The room a panel's readouts have.
#[derive(Clone, Copy)]
struct Fit {
    /// Columns inside the border.
    width: u16,
    /// The arrival and stamp strips, and the one-line hints under them.
    strips: bool,
    /// Long forms of the readouts.
    roomy: bool,
    /// Numbers only: the narrowest screens.
    terse: bool,
}

impl Screen<'_> {
    pub fn draw(&self, frame: &mut Frame) {
        let area = frame.area();
        if let Some(cursors) = self.app.cursor_groups().filter(|c| !c.is_empty()) {
            self.draw_runtime(frame, area, &cursors);
        } else if area.width >= FULL_WIDTH && area.height >= FULL_HEIGHT {
            self.draw_full(frame, area);
        } else {
            self.draw_compact(frame, area);
        }
    }

    fn draw_full(&self, frame: &mut Frame, area: Rect) {
        let view = self.app.view();
        let [plate, top, middle, bottom, foot] = Layout::vertical([
            Constraint::Length(1),
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Min(4),
            Constraint::Length(1),
        ])
        .areas(area);
        let halves = |area| {
            Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)]).areas(area)
        };
        let [tip, boiler] = halves(top);
        let [press, feed] = halves(middle);
        let [reorg, dispatch] = halves(bottom);
        let fit = |r: Rect| {
            let width = r.width.saturating_sub(2);
            Fit {
                width,
                strips: true,
                roomy: width >= ROOMY,
                terse: false,
            }
        };

        frame.render_widget(Paragraph::new(self.nameplate(&view, false)), plate);
        self.panel(frame, tip, "THE TIP", self.the_tip(&view, fit(tip)));
        self.panel(frame, press, "THE PRESS", self.the_press(&view, fit(press)));
        self.panel(frame, boiler, "BOILER", self.boiler(fit(boiler)));
        self.panel(frame, feed, "FEED LINES", self.feed_lines(fit(feed)));
        self.panel(frame, reorg, "REORG", self.reorg(&view, fit(reorg)));
        self.panel(
            frame,
            dispatch,
            "DISPATCH",
            self.dispatch(&view, fit(dispatch), dispatch.height.saturating_sub(2)),
        );
        frame.render_widget(Paragraph::new(self.footer()), foot);
    }

    /// One column, no borders, no strips: the readouts stacked under the nameplate.
    fn draw_compact(&self, frame: &mut Frame, area: Rect) {
        let view = self.app.view();
        let fit = Fit {
            width: area.width,
            strips: false,
            roomy: area.width >= TERSE_WIDTH,
            terse: area.width < TERSE_WIDTH,
        };
        let mut lines = vec![self.nameplate(&view, true)];
        lines.extend(self.the_tip(&view, fit));
        lines.extend(self.the_press(&view, fit));
        lines.extend(self.boiler(fit));
        lines.extend(self.feed_lines(fit));
        lines.extend(self.reorg(&view, fit));
        lines.extend(self.dispatch(&view, fit, 0));
        let [body, foot] =
            Layout::vertical([Constraint::Min(0), Constraint::Length(1)]).areas(area);
        frame.render_widget(Paragraph::new(lines), body);
        frame.render_widget(Paragraph::new(self.footer()), foot);
    }

    fn panel(&self, frame: &mut Frame, area: Rect, title: &str, lines: Vec<Line<'static>>) {
        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(self.theme.fg(Ink::Seam))
            .title(Span::styled(
                format!(" {title} "),
                self.theme.fg(Ink::Brass).add_modifier(Modifier::BOLD),
            ));
        frame.render_widget(Paragraph::new(lines).block(block), area);
    }

    fn row(&self, label: &str, mut value: Vec<Span<'static>>) -> Line<'static> {
        let mut spans = vec![Span::styled(format!(" {label:<9} "), self.theme.label())];
        spans.append(&mut value);
        Line::from(spans)
    }

    fn opt_block(&self, v: Option<u64>, ink: Ink) -> Span<'static> {
        match v {
            Some(v) => Span::styled(
                thousands(v),
                self.theme.fg(ink).add_modifier(Modifier::BOLD),
            ),
            None => self.theme.no_signal(),
        }
    }

    fn hint(&self, text: &str) -> Line<'static> {
        Line::from(Span::styled(
            format!("           {text}"),
            self.theme.fg(Ink::Faint).add_modifier(Modifier::DIM),
        ))
    }

    fn stale(&self, view: &NestView) -> bool {
        match (view.ready.read_at(), view.ready.last_status) {
            (_, Some((_, 0))) => true,
            (Some(t), _) => self.now_ms.saturating_sub(t) > self.stale_after_ms,
            (None, _) => false,
        }
    }

    pub fn state(&self, view: &NestView) -> (&'static str, Ink) {
        let ready = view.ready;
        if ready.value().is_none() {
            return ("NO SIGNAL", Ink::Faint);
        }
        if self.stale(view) {
            return ("STALE", Ink::Rust);
        }
        if ready.bool("quarantined") {
            return ("QUARANTINED", Ink::Rust);
        }
        let views_failed = ready
            .value()
            .and_then(|b| b.get("views_failed"))
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
        .any(|k| ready.bool(k));
        if attention || views_failed || !ready.bool("ready") {
            return ("ATTENTION", Ink::Ember);
        }
        if ready.bool("seal_direct_active") || ready.lag().is_some_and(|l| l > 10) {
            return ("BACKFILL", Ink::Brass);
        }
        ("LIVE", Ink::Verdigris)
    }

    fn nameplate(&self, view: &NestView, short: bool) -> Line<'static> {
        let t = &self.theme;
        let (state, ink) = self.state(view);
        let name = view.label.clone().unwrap_or_else(|| "NO SIGNAL".into());
        let chain = view.chain.clone().unwrap_or_else(|| "NO SIGNAL".into());
        let version = view.ready.str("version");
        let mode = view
            .ready
            .value()
            .and_then(|b| b.pointer("/freshness/mode"))
            .and_then(Value::as_str)
            .map(str::to_string);
        let sep = || Span::styled(" · ", t.fg(Ink::Seam));
        let mut spans = vec![
            Span::styled(
                format!(" {} ", name.to_uppercase()),
                t.fg(Ink::Body).add_modifier(Modifier::BOLD),
            ),
            Span::styled(format!(" {state} "), t.state(ink)),
            sep(),
            Span::styled(chain, t.fg(Ink::Copper)),
            sep(),
        ];
        let mounts = self.app.mounts().filter(|m| m.len() > 1);
        if short {
            spans.push(t.dim(version.unwrap_or_else(|| "NO SIGNAL".into())));
            if let Some(m) = mounts {
                spans.push(sep());
                spans.push(t.dim(format!(
                    "{}/{}",
                    self.app.selected.min(m.len() - 1) + 1,
                    m.len()
                )));
            }
            return Line::from(spans);
        }
        spans.push(t.dim(version.map_or_else(|| "NO SIGNAL".into(), |v| format!("nuthatch {v}"))));
        spans.push(sep());
        spans.push(t.dim(format!(
            "freshness {}",
            mode.unwrap_or_else(|| "NO SIGNAL".into())
        )));
        // `/`'s nid belongs to a solo nest; a runtime's root has no `/`.
        if let Some(nid) = self
            .app
            .mounts()
            .is_none()
            .then(|| self.app.root.str("nid"))
            .flatten()
        {
            spans.push(sep());
            spans.push(t.dim(format!("nid {}", nid.chars().take(12).collect::<String>())));
        }
        if let Some(m) = mounts {
            spans.push(sep());
            spans.push(t.dim(format!(
                "mount {} of {} (n, N)",
                self.app.selected.min(m.len() - 1) + 1,
                m.len()
            )));
        }
        Line::from(spans)
    }

    /// The poll interval, the last poll's age and the countdown to the next.
    fn poll(&self, ready: &RouteState, roomy: bool) -> Vec<Span<'static>> {
        let t = &self.theme;
        let interval = ready.pointer_u64("/freshness/poll_interval_secs");
        let since = ready.u64("seconds_since_poll").zip(ready.read_at());
        // A cursor that has not polled yet serves an age of 0, which is not a poll.
        let since = since.filter(|_| ready.u64("last_poll_unixtime") != Some(0));
        // `/ready` carries no tip timestamp, so lag in seconds would be invented; the poll's age and
        // the countdown to the next are what it does carry.
        match (interval, since) {
            (Some(every), Some((since, read_at))) => {
                let since = since + self.now_ms.saturating_sub(read_at) / 1000;
                let next = every.saturating_sub(since);
                let next = if next == 0 {
                    "due".to_string()
                } else if roomy {
                    format!("in {next} s")
                } else {
                    format!("{next} s")
                };
                if roomy {
                    vec![
                        t.value(format!("every {every} s")),
                        t.dim(format!(" · last {since} s ago · next ")),
                        t.value(next),
                    ]
                } else {
                    vec![
                        t.value(format!("{every} s")),
                        t.dim(format!(": {since} s ago, next ")),
                        t.value(next),
                    ]
                }
            }
            _ => vec![t.no_signal()],
        }
    }

    fn the_tip(&self, view: &NestView, fit: Fit) -> Vec<Line<'static>> {
        let t = &self.theme;
        let ready = view.ready;
        let poll = self.poll(ready, fit.roomy);
        let mut lines = vec![
            self.row("TIP", vec![self.opt_block(ready.tip(), Ink::BrassHi)]),
            self.row(
                "INDEXED",
                vec![self.opt_block(ready.indexed(), Ink::Copper)],
            ),
            self.row(
                "LAG",
                match ready.lag() {
                    Some(l) => vec![t.value(format!("{} blocks", thousands(l)))],
                    None => vec![t.no_signal()],
                },
            ),
            self.row("POLL", poll),
        ];
        if fit.strips {
            let marks = view.track.map(|tr| &tr.arrivals);
            lines.push(self.row(
                "ARRIVALS",
                vec![arrival_strip(
                    t,
                    marks,
                    fit.width.saturating_sub(12) as usize,
                )],
            ));
            if fit.roomy {
                lines.push(self.hint("one mark a poll; the taller, the more blocks"));
            }
        }
        lines
    }

    fn the_press(&self, view: &NestView, fit: Fit) -> Vec<Line<'static>> {
        let t = &self.theme;
        let ready = view.ready;
        let sealed = ready.u64("sealed_through").filter(|s| *s > 0);
        let rows = self.app.nest_metric(view, series::NEST_ROWS_SEALED);
        let bytes = self.app.nest_metric(view, series::NEST_SEALED_BYTES);
        let mut lines = vec![
            self.row(
                "SEALED",
                match sealed {
                    Some(s) => vec![t.dim("through "), t.value(thousands(s))],
                    None => vec![t.no_signal(), t.dim(" nothing sealed yet")],
                },
            ),
            self.row(
                "SEAL LAG",
                match ready.u64("seal_lag_blocks") {
                    Some(l) => vec![t.value(format!("{} blocks", thousands(l)))],
                    None => vec![t.no_signal()],
                },
            ),
            self.row(
                "OUTPUT",
                vec![
                    rows.map_or_else(
                        || t.no_signal(),
                        |r| t.value(format!("{} rows", thousands(r as u64))),
                    ),
                    t.dim(" · "),
                    bytes.map_or_else(
                        || t.no_signal(),
                        |b| {
                            t.value(if fit.roomy {
                                format!("{} on disk", human_bytes(b))
                            } else {
                                human_bytes(b)
                            })
                        },
                    ),
                ],
            ),
        ];
        if ready.bool("seal_direct_active") {
            lines.push(self.row(
                "DIRECT",
                vec![
                    self.opt_block(ready.u64("seal_direct_completed"), Ink::Copper),
                    t.dim(" of "),
                    self.opt_block(ready.u64("seal_direct_target"), Ink::Copper),
                    t.dim(", fetched "),
                    self.opt_block(ready.u64("seal_direct_fetched"), Ink::Copper),
                ],
            ));
        } else if fit.strips {
            lines.push(self.row("DIRECT", vec![t.dim("no seal-direct pass")]));
        }
        if fit.strips {
            let marks = view.track.map(|tr| &tr.stamps);
            lines.push(self.row(
                "STAMPS",
                vec![stamp_strip(t, marks, fit.width.saturating_sub(12) as usize)],
            ));
            if fit.roomy {
                lines.push(self.hint("one mark an advance of sealed_through"));
            }
        }
        lines
    }

    fn boiler(&self, fit: Fit) -> Vec<Line<'static>> {
        let t = &self.theme;
        let app = self.app;
        let rss = app.metric(series::RSS, &[]);
        let budget = app.budget_bytes();
        let cursors = app.cursors();
        let gauge_w = fit.width.saturating_sub(12).min(50) as usize;
        let mut lines = Vec::new();
        if fit.strips {
            lines.push(self.row(
                "RSS",
                match (rss, budget) {
                    (Some(rss), Some(budget)) if budget > 0.0 => {
                        let filled = ((rss / budget) * gauge_w as f64)
                            .round()
                            .clamp(0.0, gauge_w as f64)
                            as usize;
                        let ink = if rss > budget { Ink::Rust } else { Ink::Brass };
                        vec![
                            Span::styled("▓".repeat(filled), t.fg(ink)),
                            Span::styled("░".repeat(gauge_w - filled), t.fg(Ink::Seam)),
                        ]
                    }
                    _ => vec![t.no_signal()],
                },
            ));
        }
        let mut reading = match rss {
            Some(rss) => vec![t.value(human_bytes(rss))],
            None => vec![t.no_signal()],
        };
        reading.push(t.dim(" of "));
        reading.push(budget.map_or_else(|| t.no_signal(), |b| t.value(human_bytes(b))));
        if fit.roomy {
            if let Some(c) = cursors {
                reading.push(t.dim(format!(
                    " · process · {c} cursor{}",
                    if c == 1 { "" } else { "s" }
                )));
            }
        }
        lines.push(self.row(if fit.strips { "" } else { "RSS" }, reading));
        lines.push(self.row(
            "CPU",
            vec![app.cpu_percent().map_or_else(
                || t.no_signal(),
                |c| t.value(format!("{c:.1}% of a core")),
            )],
        ));
        let view = app.view();
        let b = |v: Option<f64>| v.map_or_else(|| t.no_signal(), |v| t.value(human_bytes(v)));
        if !fit.terse {
            lines.push(self.row(
                "POOL",
                vec![
                    t.dim("reserved "),
                    b(app.nest_metric(&view, series::NEST_POOL_RESERVED)),
                    t.dim(" · peak "),
                    b(app.nest_metric(&view, series::NEST_POOL_PEAK)),
                ],
            ));
        }
        lines.push(self.row(
            "HOT STORE",
            vec![b(app.nest_metric(&view, series::NEST_HOT_BYTES))],
        ));
        lines
    }

    fn feed_lines(&self, fit: Fit) -> Vec<Line<'static>> {
        let t = &self.theme;
        let app = self.app;
        let endpoints = app.rpc_endpoints();
        if endpoints.is_empty() {
            return vec![self.row("RPC", vec![t.no_signal()])];
        }
        if fit.terse {
            let total: f64 = endpoints
                .iter()
                .filter_map(|e| app.metric(series::RPC_REQUESTS, &[("endpoint", e)]))
                .sum();
            let up = app.rpc_up().map_or_else(
                || t.no_signal(),
                |(up, of)| t.value(format!("{up}/{of} up")),
            );
            return vec![self.row(
                "RPC",
                vec![
                    up,
                    t.dim(" · "),
                    t.value(format!("{} req", compact(total as u64))),
                ],
            )];
        }
        // The name takes what the numeric columns leave; RETRY goes first when room is short, and a
        // retry still lights FAILOVER.
        let retry_col = fit.roomy;
        let numbers = 7 + 6 + if retry_col { 6 } else { 0 } + 12;
        let name_w = (fit.width as usize)
            .saturating_sub(numbers + 2)
            .clamp(6, 30);
        let head = if name_w >= 8 { "ENDPOINT" } else { "RPC" };
        let mut header = format!(" {head:<name_w$} {:>6} {:>5}", "REQ", "FAIL");
        if retry_col {
            header.push_str(&format!(" {:>5}", "RETRY"));
        }
        header.push_str(&format!(" {:>11}", "MEAN"));
        let mut lines = vec![Line::from(Span::styled(
            header,
            t.fg(Ink::Faint).add_modifier(Modifier::BOLD),
        ))];
        for e in &endpoints {
            let l = [("endpoint", e.as_str())];
            let moved = |s| app.delta(s, &l).is_some_and(|(d, _)| d > 0.0);
            let lit = moved(series::RPC_REQUESTS);
            let mean = match app.latency.get(e) {
                Some(lat) if lit => format!("{:.0} ms", lat.mean_ms),
                Some(lat) => format!(
                    "{:.0} ms {}",
                    lat.mean_ms,
                    short_age(self.now_ms.saturating_sub(lat.at_ms) / 1000)
                ),
                None => "-".into(),
            };
            let count = |s| {
                app.metric(s, &l)
                    .map_or("-".into(), |v: f64| compact(v as u64))
            };
            let mut spans = vec![
                Span::styled(
                    format!(" {:<name_w$} ", clip(e, name_w)),
                    if lit {
                        t.fg(Ink::BrassHi)
                    } else {
                        t.fg(Ink::Dim)
                    },
                ),
                t.value(format!("{:>6}", count(series::RPC_REQUESTS))),
                Span::styled(
                    format!(" {:>5}", count(series::RPC_FAILURES)),
                    if moved(series::RPC_FAILURES) {
                        t.alarm(Ink::Rust)
                    } else {
                        t.fg(Ink::Copper)
                    },
                ),
            ];
            if retry_col {
                spans.push(t.value(format!(" {:>5}", count(series::RPC_RETRIES))));
            }
            spans.push(t.value(format!(" {mean:>11}")));
            if moved(series::RPC_RETRIES) {
                spans.push(Span::styled(" FAILOVER", t.alarm(Ink::Ember)));
            }
            lines.push(Line::from(spans));
        }
        lines
    }

    fn reorg(&self, view: &NestView, fit: Fit) -> Vec<Line<'static>> {
        let t = &self.theme;
        let count = self.app.nest_metric(view, series::NEST_REORGS);
        let depth = self.app.nest_metric(view, series::NEST_LAST_REORG_DEPTH);
        let at = self.app.nest_metric(view, series::NEST_LAST_REORG_AT);
        let since = if fit.roomy {
            " since the process started"
        } else {
            " since start"
        };
        let last = match (depth, at, count) {
            (Some(d), Some(at), _) => vec![
                t.value(format!("{} blocks", thousands(d as u64))),
                t.dim(" rolled back, "),
                t.value(short_age((self.now_ms / 1000).saturating_sub(at as u64))),
                t.dim(" ago"),
            ],
            // The gauges are absent until a reorg, so with none counted that absence is the answer.
            (None, None, Some(0.0)) => vec![t.dim("none")],
            _ => vec![t.no_signal()],
        };
        vec![
            self.row(
                "REORGS",
                vec![count.map_or_else(
                    || t.no_signal(),
                    |c| t.value(format!("{}{since}", thousands(c as u64))),
                )],
            ),
            self.row("LAST", last),
        ]
    }

    /// The requests the nest served, newest first, under the rate and median of the last minute.
    fn dispatch(&self, view: &NestView, fit: Fit, rows: u16) -> Vec<Line<'static>> {
        let feeds: Vec<(Option<&str>, &Dispatch)> =
            view.dispatch.map(|d| (None, d)).into_iter().collect();
        self.dispatch_of(&feeds, fit, rows)
    }

    /// DISPATCH over one feed or several, each tagged with its mount when there are several.
    fn dispatch_of(
        &self,
        feeds: &[(Option<&str>, &Dispatch)],
        fit: Fit,
        rows: u16,
    ) -> Vec<Line<'static>> {
        let t = &self.theme;
        let heard: Vec<&(Option<&str>, &Dispatch)> = feeds
            .iter()
            .filter(|(_, d)| d.status.is_some_and(|s| s != 0))
            .collect();
        let Some(first) = heard.first() else {
            return vec![self.row("REQUESTS", vec![t.no_signal()])];
        };
        let open: Vec<&(Option<&str>, &Dispatch)> = heard
            .iter()
            .copied()
            .filter(|(_, d)| d.status == Some(200))
            .collect();
        if open.is_empty() {
            let status = first.1.status.unwrap_or(0);
            let (word, why) = if status == 401 {
                (
                    "NO TOKEN",
                    " pass --dir or set NUTHATCH_ADMIN_TOKEN".to_string(),
                )
            } else {
                ("NOT SERVED", format!(" /_admin/requests answered {status}"))
            };
            let mut v = vec![Span::styled(word, t.alarm(Ink::Ember))];
            if fit.roomy {
                v.push(t.dim(why));
            }
            return vec![self.row("REQUESTS", v)];
        }
        let mut served: Vec<(Option<&str>, &Served)> = open
            .iter()
            .flat_map(|(m, d)| d.served.iter().map(move |s| (*m, s)))
            .collect();
        served.sort_by_key(|(_, s)| (s.unix_ms, s.seq));
        const WINDOW_MS: u64 = 60_000;
        let mut recent: Vec<f64> = served
            .iter()
            .filter(|(_, s)| s.unix_ms + WINDOW_MS >= self.now_ms)
            .map(|(_, s)| s.ms)
            .collect();
        recent.sort_by(f64::total_cmp);
        let mut summary = if recent.is_empty() {
            vec![t.dim("none in the last minute")]
        } else {
            vec![
                t.value(format!(
                    "{:.1}/s",
                    recent.len() as f64 * 1000.0 / WINDOW_MS as f64
                )),
                t.dim(" · p50 "),
                t.value(format!("{} ms", millis(recent[recent.len() / 2]))),
            ]
        };
        if fit.roomy && !recent.is_empty() {
            summary.push(t.dim(format!(" · {} in the last minute", recent.len())));
        }
        let mut lines = vec![self.row("REQUESTS", summary)];
        let route_w = (fit.width as usize)
            .saturating_sub(5 + 4 + 9 + 3)
            .clamp(8, 60);
        for (mount, s) in served.iter().rev().take(rows.saturating_sub(1) as usize) {
            let age = short_age(self.now_ms.saturating_sub(s.unix_ms) / 1000);
            let mut what = match mount {
                Some(m) => format!("{m} {}", s.route),
                None => s.route.clone(),
            };
            if let Some(n) = &s.name {
                what.push(' ');
                what.push_str(n);
            }
            lines.push(Line::from(vec![
                t.dim(format!(" {age:>4} ")),
                Span::styled(
                    format!("{:<route_w$}", clip(&what, route_w)),
                    t.fg(Ink::Body),
                ),
                Span::styled(
                    format!(" {:>3}", s.status),
                    if s.status >= 500 {
                        t.alarm(Ink::Rust)
                    } else if s.status >= 400 {
                        t.alarm(Ink::Ember)
                    } else {
                        t.fg(Ink::Copper)
                    },
                ),
                t.value(format!(" {:>6} ms", millis(s.ms))),
            ]));
        }
        lines
    }

    // --- A runtime: one engine room per cursor (RFC-0069 §8). ---

    /// The worst of several nests' states, so a cursor or a runtime is never cheerier than its worst
    /// mount.
    fn worst(&self, views: &[NestView]) -> (&'static str, Ink) {
        let rank = |s: &str| match s {
            "QUARANTINED" => 5,
            "STALE" => 4,
            "ATTENTION" => 3,
            "NO SIGNAL" => 2,
            "BACKFILL" => 1,
            _ => 0,
        };
        views
            .iter()
            .map(|v| self.state(v))
            .max_by_key(|(s, _)| rank(s))
            .unwrap_or(("NO SIGNAL", Ink::Faint))
    }

    fn views_of<'b>(&'b self, cursor: &Cursor) -> Vec<NestView<'b>> {
        cursor
            .mounts
            .iter()
            .map(|m| self.app.mount_view(m, cursor.chain.clone()))
            .collect()
    }

    fn runtime_plate(&self, cursors: &[Cursor], shown_one: bool) -> Line<'static> {
        let t = &self.theme;
        let views: Vec<NestView> = cursors.iter().flat_map(|c| self.views_of(c)).collect();
        let (state, ink) = self.worst(&views);
        let name = self
            .app
            .nests
            .str("runtime")
            .unwrap_or_else(|| "runtime".into());
        let version = views.iter().find_map(|v| v.ready.str("version"));
        let sep = || Span::styled(" · ", t.fg(Ink::Seam));
        let mut spans = vec![
            Span::styled(
                format!(" {} ", name.to_uppercase()),
                t.fg(Ink::Body).add_modifier(Modifier::BOLD),
            ),
            Span::styled(format!(" {state} "), t.state(ink)),
            sep(),
            t.dim(format!(
                "{} cursor{} · {} mount{}",
                cursors.len(),
                if cursors.len() == 1 { "" } else { "s" },
                views.len(),
                if views.len() == 1 { "" } else { "s" }
            )),
            sep(),
            t.dim(version.map_or_else(|| "NO SIGNAL".into(), |v| format!("nuthatch {v}"))),
        ];
        if shown_one && cursors.len() > 1 {
            spans.push(sep());
            spans.push(t.dim(format!(
                "cursor {} of {}, n N",
                self.app.selected.min(cursors.len() - 1) + 1,
                cursors.len()
            )));
        }
        Line::from(spans)
    }

    /// One cursor's tip, which its mounts share, and each mount's own position on it. The tip is
    /// read from the mounts' own `/<name>/ready`: the process-global gauge reports whichever cursor
    /// polled last.
    fn cursor_lines(&self, cursor: &Cursor, fit: Fit) -> Vec<Line<'static>> {
        let t = &self.theme;
        let views = self.views_of(cursor);
        let tip = views.iter().filter_map(|v| v.ready.tip()).max();
        let polled = views
            .iter()
            .find(|v| v.ready.value().is_some())
            .map(|v| v.ready)
            .unwrap_or(views[0].ready);
        let mut tip_row = vec![self.opt_block(tip, Ink::BrassHi), t.dim("  ")];
        tip_row.extend(self.poll(polled, fit.width >= 72));
        let mut lines = vec![self.row("TIP", tip_row)];
        if fit.strips {
            let marks = views[0].track.map(|tr| &tr.arrivals);
            lines.push(self.row(
                "ARRIVALS",
                vec![arrival_strip(
                    t,
                    marks,
                    fit.width.saturating_sub(12) as usize,
                )],
            ));
        }
        let name_w = (fit.width as usize)
            .saturating_sub(12 + 8 + 12 + 8 + 12 + 1)
            .clamp(6, 24);
        lines.push(Line::from(Span::styled(
            format!(
                " {:<name_w$} {:>11} {:>7} {:>11} {:>7}  {}",
                "NEST", "INDEXED", "LAG", "SEALED", "REORGS", "STATE"
            ),
            t.fg(Ink::Faint).add_modifier(Modifier::BOLD),
        )));
        for v in &views {
            let (state, ink) = self.state(v);
            let num = |n: Option<u64>| n.map_or_else(|| "NO SIGNAL".to_string(), thousands);
            let sealed = v.ready.u64("sealed_through").filter(|s| *s > 0);
            let reorgs = self
                .app
                .nest_metric(v, series::NEST_REORGS)
                .map(|r| r as u64);
            lines.push(Line::from(vec![
                Span::styled(
                    format!(
                        " {:<name_w$}",
                        clip(v.label.as_deref().unwrap_or_default(), name_w)
                    ),
                    t.fg(Ink::Body),
                ),
                t.value(format!(" {:>11}", num(v.ready.indexed()))),
                t.value(format!(" {:>7}", num(v.ready.lag()))),
                t.value(format!(" {:>11}", num(sealed))),
                t.value(format!(" {:>7}", num(reorgs))),
                t.dim("  "),
                Span::styled(state, t.state(ink)),
            ]));
        }
        lines
    }

    fn draw_runtime(&self, frame: &mut Frame, area: Rect, cursors: &[Cursor]) {
        let full = area.width >= FULL_WIDTH && area.height >= FULL_HEIGHT;
        let side_by_side = area.width / cursors.len() as u16 >= SIDE_BY_SIDE;
        let shown: Vec<&Cursor> = if side_by_side {
            cursors.iter().collect()
        } else {
            vec![&cursors[self.app.selected.min(cursors.len() - 1)]]
        };
        if !full {
            let fit = Fit {
                width: area.width,
                strips: false,
                roomy: area.width >= TERSE_WIDTH,
                terse: area.width < TERSE_WIDTH,
            };
            let mut lines = vec![self.runtime_plate(cursors, false)];
            for c in cursors {
                lines.push(Line::from(Span::styled(
                    format!("CURSOR {}", c.chain.as_deref().unwrap_or("NO SIGNAL")),
                    self.theme.fg(Ink::Brass).add_modifier(Modifier::BOLD),
                )));
                lines.extend(self.cursor_lines(c, fit));
            }
            lines.extend(self.boiler(fit));
            lines.extend(self.feed_lines(fit));
            lines.extend(self.dispatch_of(&self.runtime_feeds(cursors), fit, 0));
            let [body, foot] =
                Layout::vertical([Constraint::Min(0), Constraint::Length(1)]).areas(area);
            frame.render_widget(Paragraph::new(lines), body);
            frame.render_widget(Paragraph::new(self.footer()), foot);
            return;
        }
        let deepest = shown.iter().map(|c| c.mounts.len()).max().unwrap_or(1) as u16;
        let [plate, row, middle, bottom, foot] = Layout::vertical([
            Constraint::Length(1),
            Constraint::Length(5 + deepest),
            Constraint::Length(8),
            Constraint::Min(4),
            Constraint::Length(1),
        ])
        .areas(area);
        let fit = |r: Rect| {
            let width = r.width.saturating_sub(2);
            Fit {
                width,
                strips: true,
                roomy: width >= ROOMY,
                terse: false,
            }
        };
        frame.render_widget(
            Paragraph::new(self.runtime_plate(cursors, !side_by_side)),
            plate,
        );
        let columns = Layout::horizontal(
            shown
                .iter()
                .map(|_| Constraint::Ratio(1, shown.len() as u32))
                .collect::<Vec<_>>(),
        )
        .split(row);
        for (c, col) in shown.iter().zip(columns.iter()) {
            let title = format!("CURSOR {}", c.chain.as_deref().unwrap_or("NO SIGNAL"));
            self.panel(frame, *col, &title, self.cursor_lines(c, fit(*col)));
        }
        let [boiler, feed] =
            Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)])
                .areas(middle);
        self.panel(frame, boiler, "BOILER", self.boiler(fit(boiler)));
        self.panel(frame, feed, "FEED LINES", self.feed_lines(fit(feed)));
        self.panel(
            frame,
            bottom,
            "DISPATCH",
            self.dispatch_of(
                &self.runtime_feeds(cursors),
                fit(bottom),
                bottom.height.saturating_sub(2),
            ),
        );
        frame.render_widget(Paragraph::new(self.footer()), foot);
    }

    /// Every mount's `/_admin/requests`, tagged with the mount.
    fn runtime_feeds<'b>(&'b self, cursors: &'b [Cursor]) -> Vec<(Option<&'b str>, &'b Dispatch)> {
        cursors
            .iter()
            .flat_map(|c| c.mounts.iter())
            .filter_map(|m| self.app.dispatch.get(m).map(|d| (Some(m.as_str()), d)))
            .collect()
    }

    fn footer(&self) -> Line<'static> {
        Line::from(Span::styled(
            format!(" {}", self.footer),
            self.theme.fg(Ink::Faint),
        ))
    }
}

fn arrival_strip(theme: &Theme, marks: Option<&VecDeque<u64>>, width: usize) -> Span<'static> {
    let Some(marks) = marks.filter(|m| !m.is_empty()) else {
        return theme.faint("no poll seen yet");
    };
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

fn stamp_strip(theme: &Theme, marks: Option<&VecDeque<u64>>, width: usize) -> Span<'static> {
    let Some(marks) = marks.filter(|m| !m.is_empty()) else {
        return theme.faint("none seen yet");
    };
    let s: String = marks
        .iter()
        .skip(marks.len().saturating_sub(width))
        .map(|_| '▮')
        .collect();
    Span::styled(s, theme.fg(Ink::Brass))
}

fn clip(s: &str, w: usize) -> String {
    if s.chars().count() <= w {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(w.saturating_sub(1)).collect();
        out.push('…');
        out
    }
}

fn short_age(secs: u64) -> String {
    match secs {
        s if s < 60 => format!("{s}s"),
        s if s < 3600 => format!("{}m", s / 60),
        s => format!("{}h", s / 3600),
    }
}

pub fn thousands(v: u64) -> String {
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

/// Milliseconds to the precision a reader can use: a tenth below ten, whole above.
fn millis(ms: f64) -> String {
    if ms < 10.0 {
        format!("{ms:.1}")
    } else {
        format!("{ms:.0}")
    }
}

/// A count in at most six characters.
fn compact(v: u64) -> String {
    match v {
        v if v < 100_000 => v.to_string(),
        v if v < 10_000_000 => format!("{}k", v / 1000),
        v => format!("{}M", v / 1_000_000),
    }
}

pub fn human_bytes(b: f64) -> String {
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
