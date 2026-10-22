//! Named nests in `~/.config/nuthatch/watch.toml`, and the list shown when the file names some and
//! the command line names none. Ported from `nuthatch-tui-client`, whose `nests.toml` has the same
//! shape less its `decimals`, which served the `/sql` feed this client does not have.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Flex, Layout};
use ratatui::style::Modifier;
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, List, ListItem, ListState};
use ratatui::Frame;
use serde::Deserialize;

use super::theme::{Ink, Theme};

pub const DEFAULT_URL: &str = "http://127.0.0.1:8288";

/// Where a nest listens, and the ssh host to reach it through when that is only on the host's
/// loopback.
#[derive(Debug, Deserialize, Clone, Default, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct NestTarget {
    pub url: Option<String>,
    pub ssh: Option<String>,
}

pub fn config_path() -> Option<PathBuf> {
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .filter(|dir| !dir.is_empty())
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))?;
    Some(base.join("nuthatch").join("watch.toml"))
}

pub fn parse_nests(text: &str) -> Result<BTreeMap<String, NestTarget>> {
    Ok(toml::from_str(text)?)
}

/// The configured nests, or none when the file does not exist.
pub fn load_nests() -> Result<BTreeMap<String, NestTarget>> {
    let Some(path) = config_path() else {
        return Ok(BTreeMap::new());
    };
    match std::fs::read_to_string(&path) {
        Ok(text) => parse_nests(&text).with_context(|| format!("reading {}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(BTreeMap::new()),
        Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
    }
}

/// Flags win over the named entry, which wins over the default listener.
pub fn resolve(
    url: Option<&str>,
    ssh: Option<&str>,
    nest: Option<&str>,
    nests: &BTreeMap<String, NestTarget>,
) -> Result<NestTarget> {
    let named = match nest {
        Some(name) => nests.get(name).cloned().with_context(|| {
            if nests.is_empty() {
                format!("no nest called '{name}': no nests are configured in watch.toml")
            } else {
                let known = nests.keys().cloned().collect::<Vec<_>>().join(", ");
                format!("no nest called '{name}'; configured: {known}")
            }
        })?,
        None => NestTarget::default(),
    };
    Ok(NestTarget {
        url: Some(
            url.map(str::to_string)
                .or(named.url)
                .unwrap_or_else(|| DEFAULT_URL.into())
                .trim_end_matches('/')
                .to_string(),
        ),
        ssh: ssh.map(str::to_string).or(named.ssh),
    })
}

pub struct Picker {
    names: Vec<String>,
    lines: Vec<String>,
    selected: usize,
}

pub enum Picked {
    Nest(String),
    Quit,
}

impl Picker {
    pub fn new(nests: &BTreeMap<String, NestTarget>) -> Self {
        let width = nests.keys().map(String::len).max().unwrap_or_default();
        Self {
            names: nests.keys().cloned().collect(),
            lines: nests
                .iter()
                .map(|(name, nest)| {
                    let url = nest.url.as_deref().unwrap_or(DEFAULT_URL);
                    match &nest.ssh {
                        Some(host) => format!("{name:<width$}  {url} via {host}"),
                        None => format!("{name:<width$}  {url}"),
                    }
                })
                .collect(),
            selected: 0,
        }
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> Option<Picked> {
        let last = self.names.len().saturating_sub(1);
        match key.code {
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                return Some(Picked::Quit);
            }
            KeyCode::Char('q') | KeyCode::Esc => return Some(Picked::Quit),
            KeyCode::Enter => return self.names.get(self.selected).cloned().map(Picked::Nest),
            KeyCode::Down | KeyCode::Char('j') => self.selected = (self.selected + 1).min(last),
            KeyCode::Up | KeyCode::Char('k') => self.selected = self.selected.saturating_sub(1),
            KeyCode::Home | KeyCode::Char('g') => self.selected = 0,
            KeyCode::End | KeyCode::Char('G') => self.selected = last,
            _ => {}
        }
        None
    }

    pub fn draw(&self, frame: &mut Frame, theme: &Theme) {
        let area = frame.area();
        let rows: Vec<ListItem> = self
            .lines
            .iter()
            .map(|line| ListItem::new(Line::styled(line.clone(), theme.fg(Ink::Body))))
            .collect();
        let mut state = ListState::default().with_selected(Some(self.selected));
        let height = (self.lines.len() as u16 + 2).min(area.height);
        let [list] = Layout::vertical([Constraint::Length(height)])
            .flex(Flex::Center)
            .areas(area);
        frame.render_stateful_widget(
            List::new(rows)
                .highlight_style(theme.state(Ink::Brass))
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_style(theme.fg(Ink::Seam))
                        .title(ratatui::text::Span::styled(
                            " CHOOSE A NEST  ↑↓  enter  q ",
                            theme.fg(Ink::Brass).add_modifier(Modifier::BOLD),
                        )),
                ),
            list,
            &mut state,
        );
    }
}

/// Runs the picker on a screen of its own, released before the chosen nest's tunnel is opened, so
/// anything ssh prints lands on the ordinary terminal rather than over the engine room.
pub fn pick_nest(nests: &BTreeMap<String, NestTarget>, theme: &Theme) -> Result<Option<String>> {
    let mut picker = Picker::new(nests);
    let mut terminal = ratatui::init();
    let result = (|| loop {
        terminal.draw(|frame| picker.draw(frame, theme))?;
        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    match picker.handle_key(key) {
                        Some(Picked::Nest(name)) => return Ok(Some(name)),
                        Some(Picked::Quit) => return Ok(None),
                        None => {}
                    }
                }
            }
        }
    })();
    ratatui::restore();
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    const NESTS: &str = r#"
[lodestar]
url = "http://127.0.0.1:8288/"
ssh = "helsinki"

[pulse]
url = "http://100.83.44.63:8124"
"#;

    #[test]
    fn a_named_nest_supplies_url_and_host_and_flags_override_it() {
        let nests = parse_nests(NESTS).unwrap();
        let t = resolve(None, None, Some("lodestar"), &nests).unwrap();
        assert_eq!(t.url.as_deref(), Some("http://127.0.0.1:8288"));
        assert_eq!(t.ssh.as_deref(), Some("helsinki"));
        let t = resolve(Some("http://x:1"), Some("other"), Some("lodestar"), &nests).unwrap();
        assert_eq!(t.url.as_deref(), Some("http://x:1"));
        assert_eq!(t.ssh.as_deref(), Some("other"));
        let t = resolve(None, None, None, &nests).unwrap();
        assert_eq!(t.url.as_deref(), Some(DEFAULT_URL));
        let err = resolve(None, None, Some("nope"), &nests).unwrap_err();
        assert!(
            err.to_string().contains("configured: lodestar, pulse"),
            "{err}"
        );
    }

    #[test]
    fn the_sql_feeds_decimals_are_refused_by_name() {
        let err = parse_nests("[a]\nurl = \"http://x\"\n[a.decimals]\namount = 6\n").unwrap_err();
        assert!(format!("{err:#}").contains("decimals"), "{err:#}");
    }

    #[test]
    fn the_picker_lists_the_configured_nests_and_returns_the_chosen_one() {
        let nests = parse_nests(NESTS).unwrap();
        let mut p = Picker::new(&nests);
        assert_eq!(p.lines[0], "lodestar  http://127.0.0.1:8288/ via helsinki");
        let key = |c| KeyEvent::new(c, KeyModifiers::NONE);
        assert!(p.handle_key(key(KeyCode::Char('j'))).is_none());
        assert!(p.handle_key(key(KeyCode::Char('j'))).is_none());
        match p.handle_key(key(KeyCode::Enter)) {
            Some(Picked::Nest(n)) => assert_eq!(n, "pulse"),
            _ => panic!("enter picks the selection"),
        }
        assert!(matches!(
            p.handle_key(key(KeyCode::Char('q'))),
            Some(Picked::Quit)
        ));
    }
}
