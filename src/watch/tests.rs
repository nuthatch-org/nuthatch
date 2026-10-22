//! The engine room drawn from recorded bodies. `tests/fixtures/watch/*.jsonl` are reads a live nest
//! served (see their README); the snapshots beside them are what those reads draw. Set
//! `NUTHATCH_WATCH_SNAPSHOTS=update` to rewrite the snapshots after a deliberate change.

use std::path::PathBuf;

use ratatui::backend::{CrosstermBackend, TestBackend};
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Color;
use ratatui::{Terminal, TerminalOptions, Viewport};

use super::screen::{stale_after_ms, Screen};
use super::state::{App, Read};
use super::theme::{ColorMode, Theme};
use super::{load_recording, SOURCES};

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/watch")
        .join(name)
}

/// Every read of a recording applied, and the instant just after its last.
fn replayed(name: &str) -> (App, u64) {
    let reads = load_recording(&fixture(name)).unwrap();
    let mut app = App::default();
    for r in &reads {
        app.apply(r);
    }
    let now = reads.last().unwrap().t_ms + 500;
    (app, now)
}

fn screen(app: &App, now: u64, mode: ColorMode) -> Screen<'_> {
    Screen {
        app,
        theme: Theme { mode },
        now_ms: now,
        stale_after_ms: stale_after_ms(1),
        footer: "replaying a fixture".into(),
    }
}

fn buffer(app: &App, now: u64, mode: ColorMode, w: u16, h: u16) -> Buffer {
    let mut terminal = Terminal::new(TestBackend::new(w, h)).unwrap();
    let s = screen(app, now, mode);
    terminal.draw(|f| s.draw(f)).unwrap();
    terminal.backend().buffer().clone()
}

fn text(buf: &Buffer) -> String {
    let area = buf.area;
    let mut out = String::new();
    for y in 0..area.height {
        let mut line = String::new();
        for x in 0..area.width {
            line.push_str(buf[(x, y)].symbol());
        }
        out.push_str(line.trim_end());
        out.push('\n');
    }
    out
}

fn snapshot(name: &str, actual: &str) {
    let path = fixture(&format!("snapshots/{name}.txt"));
    if std::env::var("NUTHATCH_WATCH_SNAPSHOTS").as_deref() == Ok("update") {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, actual).unwrap();
        return;
    }
    let expected = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{}: {e} (NUTHATCH_WATCH_SNAPSHOTS=update)", path.display()));
    assert_eq!(
        actual,
        expected,
        "{name} drew differently from {}",
        path.display()
    );
}

#[test]
fn the_solo_engine_room_at_three_sizes() {
    let (app, now) = replayed("solo.jsonl");
    for (w, h) in [(120, 40), (80, 24), (60, 20), (50, 16)] {
        let drawn = text(&buffer(&app, now, ColorMode::True, w, h));
        snapshot(&format!("solo-{w}x{h}"), &drawn);
    }
}

#[test]
fn a_runtime_draws_its_selected_mount() {
    let (mut app, now) = replayed("runtime.jsonl");
    for (w, h) in [(120, 40), (80, 24), (60, 20)] {
        let drawn = text(&buffer(&app, now, ColorMode::True, w, h));
        snapshot(&format!("runtime-{w}x{h}"), &drawn);
    }
    app.step(true);
    let drawn = text(&buffer(&app, now, ColorMode::True, 120, 40));
    snapshot("runtime-120x40-second-mount", &drawn);
}

/// The snapshots must show what the bodies say, not merely what they said last time: the figures
/// any reader would check first are read straight out of the recording and found on the screen.
#[test]
fn the_screen_shows_the_recordings_own_figures() {
    let reads = load_recording(&fixture("solo.jsonl")).unwrap();
    let last_ready = reads.iter().rev().find(|r| r.route == "/ready").unwrap();
    let body: serde_json::Value = serde_json::from_str(&last_ready.body).unwrap();
    let (app, now) = replayed("solo.jsonl");
    let drawn = text(&buffer(&app, now, ColorMode::True, 120, 40));
    for key in ["tip", "last_block", "sealed_through"] {
        let v = super::screen::thousands(body[key].as_u64().unwrap());
        assert!(drawn.contains(&v), "{key} {v} not on screen:\n{drawn}");
    }
    let name = app.solo_name().unwrap().to_uppercase();
    assert!(drawn.contains(&name), "{name} not on screen");
}

fn sgr_colour_codes(bytes: &[u8]) -> Vec<String> {
    let s = String::from_utf8_lossy(bytes);
    let mut out = Vec::new();
    let mut rest = &s[..];
    while let Some(at) = rest.find("\x1b[") {
        let seq = &rest[at + 2..];
        let end = seq
            .find(|c: char| c.is_ascii_alphabetic())
            .unwrap_or(seq.len());
        if seq[end..].starts_with('m') {
            let params: Vec<&str> = seq[..end].split(';').collect();
            let mut i = 0;
            while i < params.len() {
                let p: u32 = params[i].parse().unwrap_or(0);
                match p {
                    38 | 48 | 58 => {
                        out.push(params[i..].join(";"));
                        break;
                    }
                    30..=37 | 40..=47 | 90..=97 | 100..=107 => out.push(p.to_string()),
                    _ => {}
                }
                i += 1;
            }
        }
        rest = &seq[end..];
    }
    out
}

#[derive(Clone, Default)]
struct Sink(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for Sink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn escapes(app: &App, now: u64, mode: ColorMode, w: u16, h: u16) -> Vec<u8> {
    let sink = Sink::default();
    let backend = CrosstermBackend::new(sink.clone());
    let mut terminal = Terminal::with_options(
        backend,
        TerminalOptions {
            viewport: Viewport::Fixed(Rect::new(0, 0, w, h)),
        },
    )
    .unwrap();
    let s = screen(app, now, mode);
    terminal.draw(|f| s.draw(f)).unwrap();
    drop(terminal);
    let bytes = sink.0.lock().unwrap().clone();
    bytes
}

#[test]
fn no_color_writes_no_colour_escape() {
    for fixture in ["solo.jsonl", "runtime.jsonl"] {
        let (app, now) = replayed(fixture);
        for (w, h) in [(120, 40), (80, 24), (60, 20)] {
            let bytes = escapes(&app, now, ColorMode::None, w, h);
            assert!(bytes.len() > 1000, "nothing drawn");
            let codes = sgr_colour_codes(&bytes);
            assert!(codes.is_empty(), "{fixture} {w}x{h}: {codes:?}");
        }
        // The same screen in colour does write them, so the check above can see one.
        let bytes = escapes(&app, now, ColorMode::True, 120, 40);
        assert!(!sgr_colour_codes(&bytes).is_empty());
    }
}

/// The state word and the warnings survive without colour: reversed or bold, never colour alone.
#[test]
fn no_color_keeps_the_state_word_visible() {
    let (app, now) = replayed("solo.jsonl");
    let buf = buffer(&app, now, ColorMode::None, 120, 40);
    let line: String = (0..buf.area.width).map(|x| buf[(x, 0)].symbol()).collect();
    let word = ["LIVE", "BACKFILL", "ATTENTION", "STALE"]
        .into_iter()
        .find(|w| line.contains(w))
        .unwrap_or_else(|| panic!("no state word in {line}"));
    let x = line.find(word).unwrap() as u16;
    assert!(buf[(x, 0)]
        .modifier
        .contains(ratatui::style::Modifier::REVERSED));
}

/// No background anywhere, and in 256-colour mode never one of the 16 named colours, whose look
/// is the terminal theme's and not the palette's.
#[test]
fn colours_come_from_the_palette() {
    for fixture in ["solo.jsonl", "runtime.jsonl"] {
        let (app, now) = replayed(fixture);
        for mode in [ColorMode::True, ColorMode::Indexed] {
            let buf = buffer(&app, now, mode, 120, 40);
            for cell in buf.content() {
                assert_eq!(cell.bg, Color::Reset, "a background was set");
                match (mode, cell.fg) {
                    (_, Color::Reset) => {}
                    (ColorMode::True, Color::Rgb(..)) => {}
                    (ColorMode::Indexed, Color::Indexed(i)) if i >= 16 => {}
                    (m, c) => panic!("{c:?} in {m:?} mode"),
                }
            }
        }
    }
}

#[test]
fn no_colour_literal_outside_the_theme() {
    for (file, text) in SOURCES {
        if *file == "theme.rs" || *file == "tests.rs" {
            continue;
        }
        for needle in ["Color::", "Rgb(", "Indexed(", "\\x1b[", "\\u{1b}"] {
            assert!(!text.contains(needle), "{file} holds {needle}");
        }
    }
}

#[test]
fn sources_lists_every_file_in_the_module() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/watch");
    let mut on_disk: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".rs") && n != "tests.rs")
        .collect();
    on_disk.sort();
    let listed: Vec<String> = SOURCES.iter().map(|(n, _)| n.to_string()).collect();
    assert_eq!(on_disk, listed);
}

fn read(t_ms: u64, route: &str, status: u16, body: &str) -> Read {
    Read {
        t_ms,
        route: route.into(),
        status,
        body: body.into(),
        cycle: None,
    }
}

/// Each panel with its source taken away: `NO SIGNAL`, and never the zero the field would
/// otherwise have defaulted to.
#[test]
fn every_panel_says_no_signal_for_an_absent_field() {
    let (app, now) = replayed("solo.jsonl");
    let reads = load_recording(&fixture("solo.jsonl")).unwrap();
    let ready_body = &reads
        .iter()
        .rev()
        .find(|r| r.route == "/ready")
        .unwrap()
        .body;
    let cases: [(&str, &str); 5] = [
        ("tip", " TIP       NO SIGNAL"),
        ("last_block", " INDEXED   NO SIGNAL"),
        ("lag_blocks", " LAG       NO SIGNAL"),
        ("seal_lag_blocks", " SEAL LAG  NO SIGNAL"),
        ("seconds_since_poll", " POLL      NO SIGNAL"),
    ];
    for (key, want) in cases {
        let mut body: serde_json::Value = serde_json::from_str(ready_body).unwrap();
        body.as_object_mut().unwrap().remove(key);
        let mut a = App::default();
        for r in &reads {
            a.apply(r);
        }
        a.apply(&read(now - 400, "/ready", 200, &body.to_string()));
        let drawn = text(&buffer(&a, now, ColorMode::None, 120, 40));
        assert!(drawn.contains(want), "{key}: want {want:?} in\n{drawn}");
    }

    // The metrics-fed panels: with no /metrics at all, each says so rather than drawing 0.
    let mut a = App::default();
    for r in reads.iter().filter(|r| r.route != "/metrics") {
        a.apply(r);
    }
    let drawn = text(&buffer(&a, now, ColorMode::None, 120, 40));
    for want in [
        " RSS       NO SIGNAL",
        " CPU       NO SIGNAL",
        " REORGS    NO SIGNAL",
        " RPC       NO SIGNAL",
        " OUTPUT    NO SIGNAL",
    ] {
        assert!(drawn.contains(want), "want {want:?} in\n{drawn}");
    }
    assert!(
        !drawn.contains("0 B"),
        "a zero byte count with no /metrics:\n{drawn}"
    );
    drop(app);
}

/// At a 12 s poll most two-second intervals ask an endpoint nothing (S0, finding 4): the mean is
/// the last interval that asked anything, with its age, not `idle`.
#[test]
fn rpc_mean_keeps_the_last_interval_that_asked_anything() {
    let m = |req: u32, sum: f64| {
        format!(
            "nuthatch_rpc_endpoint_requests_total{{endpoint=\"e\"}} {req}\n\
             nuthatch_rpc_request_duration_seconds_sum{{endpoint=\"e\"}} {sum}\n\
             nuthatch_rpc_request_duration_seconds_count{{endpoint=\"e\"}} {req}\n"
        )
    };
    let mut app = App::default();
    app.apply(&read(0, "/metrics", 200, &m(10, 1.0)));
    app.apply(&read(2_000, "/metrics", 200, &m(12, 1.17)));
    app.apply(&read(4_000, "/metrics", 200, &m(12, 1.17)));
    app.apply(&read(6_000, "/metrics", 200, &m(12, 1.17)));
    let lat = app.latency["e"];
    assert_eq!(lat.at_ms, 2_000);
    assert!((lat.mean_ms - 85.0).abs() < 1e-6, "{}", lat.mean_ms);
    let drawn = text(&buffer(&app, 16_500, ColorMode::None, 120, 40));
    assert!(drawn.contains("85 ms 14s"), "{drawn}");
}

/// A screen that stops hearing `/ready` says so, and one that is hearing it does not.
#[test]
fn a_nest_that_stops_answering_is_stale() {
    let (mut app, now) = replayed("solo.jsonl");
    let s = screen(&app, now, ColorMode::None);
    assert_ne!(s.state(&app.view()).0, "STALE");
    let s = screen(&app, now + 10_000, ColorMode::None);
    assert_eq!(s.state(&app.view()).0, "STALE");
    app.apply(&read(now, "/ready", 0, ""));
    let s = screen(&app, now, ColorMode::None);
    assert_eq!(s.state(&app.view()).0, "STALE");
}
