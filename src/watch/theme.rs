//! The Foundry palette (steampunk-ui, terminal annex). The only place a colour is named: truecolour
//! where the terminal says it has it, the computed 256-colour cube otherwise, never one of the 16
//! named ANSI colours, never a background, and nothing at all under `NO_COLOR`.

use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Span;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ink {
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColorMode {
    True,
    Indexed,
    None,
}

#[derive(Clone, Copy, Debug)]
pub struct Theme {
    pub mode: ColorMode,
}

impl Theme {
    pub fn detect() -> Self {
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

    pub fn fg(&self, ink: Ink) -> Style {
        match self.color(ink) {
            Some(c) => Style::new().fg(c),
            None => Style::new(),
        }
    }

    pub fn label(&self) -> Style {
        self.fg(Ink::Faint).add_modifier(Modifier::BOLD)
    }

    pub fn value(&self, s: impl Into<String>) -> Span<'static> {
        Span::styled(s.into(), self.fg(Ink::Copper))
    }

    pub fn dim(&self, s: impl Into<String>) -> Span<'static> {
        Span::styled(s.into(), self.fg(Ink::Dim))
    }

    pub fn faint(&self, s: impl Into<String>) -> Span<'static> {
        Span::styled(s.into(), self.fg(Ink::Faint))
    }

    pub fn no_signal(&self) -> Span<'static> {
        Span::styled(
            "NO SIGNAL",
            self.fg(Ink::Faint).add_modifier(Modifier::BOLD),
        )
    }

    /// A state word, reversed so it carries without colour.
    pub fn state(&self, ink: Ink) -> Style {
        self.fg(ink)
            .add_modifier(Modifier::REVERSED | Modifier::BOLD)
    }

    /// A warning word (`FAILOVER`, `STALE`), bold so it carries without colour.
    pub fn alarm(&self, ink: Ink) -> Style {
        self.fg(ink).add_modifier(Modifier::BOLD)
    }
}
