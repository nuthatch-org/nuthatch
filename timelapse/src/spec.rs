//! The spec (§4.2): a small TOML file kept outside the nest, so it touches neither identity.

use anyhow::{bail, ensure, Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FoldKind {
    /// Each row adds a signed delta to its key.
    Sum,
    /// Each row sets its key's value; the latest `(block_number, log_index)` wins.
    Last,
}

impl FoldKind {
    pub fn name(self) -> &'static str {
        match self {
            FoldKind::Sum => "sum",
            FoldKind::Last => "last",
        }
    }

    /// The column the fold reads its amount from.
    pub fn amount_column(self) -> &'static str {
        match self {
            FoldKind::Sum => "delta",
            FoldKind::Last => "value",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Quantity {
    pub fold: FoldKind,
    pub sql: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Spec {
    pub title: String,
    pub nest: String,
    pub from_block: u64,
    pub to_block: u64,
    #[serde(default)]
    pub frames: Option<u64>,
    #[serde(default)]
    pub step_blocks: Option<u64>,
    /// Playback only: it never enters the frames or their hash.
    #[serde(default)]
    pub fps: Option<u32>,
    pub top: usize,
    pub quantity: Quantity,
}

impl Spec {
    pub fn parse(text: &str) -> Result<Spec> {
        let spec: Spec = toml::from_str(text).context("the time-lapse spec does not parse")?;
        ensure!(spec.top > 0, "`top` must be positive");
        ensure!(
            spec.to_block >= spec.from_block,
            "`to_block` {} is below `from_block` {}",
            spec.to_block,
            spec.from_block
        );
        spec.step()?;
        Ok(spec)
    }

    /// Blocks per frame. A remainder of the range past the last whole frame is not played.
    pub fn step(&self) -> Result<u64> {
        let span = self.to_block - self.from_block + 1;
        let step = match (self.frames, self.step_blocks) {
            (Some(_), Some(_)) => bail!("give `frames` or `step_blocks`, not both"),
            (None, None) => bail!("give `frames` or `step_blocks`"),
            (Some(0), _) | (_, Some(0)) => bail!("`frames` and `step_blocks` must be positive"),
            (Some(f), None) => span / f,
            (None, Some(s)) => s,
        };
        ensure!(
            step > 0 && step <= span,
            "{} frames do not fit in {span} blocks",
            self.frames.unwrap_or(0)
        );
        Ok(step)
    }

    /// Each frame's closing block: frame `i` holds every event at or below `closes[i]`.
    pub fn closes(&self) -> Result<Vec<u64>> {
        let step = self.step()?;
        let n = self
            .frames
            .unwrap_or((self.to_block - self.from_block + 1) / step);
        Ok((0..n)
            .map(|i| self.from_block + (i + 1) * step - 1)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(extra: &str) -> String {
        format!(
            "title = \"t\"\nnest = \"n\"\nfrom_block = 100\nto_block = 199\ntop = 3\n{extra}\n\
             [quantity]\nfold = \"sum\"\nsql = \"SELECT 1\"\n"
        )
    }

    #[test]
    fn frames_and_step_blocks_derive_the_same_closes() {
        let a = Spec::parse(&spec("frames = 4")).unwrap();
        let b = Spec::parse(&spec("step_blocks = 25")).unwrap();
        assert_eq!(a.closes().unwrap(), vec![124, 149, 174, 199]);
        assert_eq!(a.closes().unwrap(), b.closes().unwrap());
    }

    #[test]
    fn both_or_neither_is_refused() {
        assert!(Spec::parse(&spec("frames = 4\nstep_blocks = 25")).is_err());
        assert!(Spec::parse(&spec("")).is_err());
        assert!(Spec::parse(&spec("frames = 101")).is_err());
    }
}
