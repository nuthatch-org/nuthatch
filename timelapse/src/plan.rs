//! One pass, planned from the catalogue alone (§4.3 steps 1 and 2).

use std::collections::BTreeMap;

use anyhow::{bail, ensure, Context, Result};
use serde::Deserialize;

use crate::spec::Spec;
use crate::validate::Shape;

/// A catalogue entry from `segments/manifest.json`.
#[derive(Debug, Clone, Deserialize)]
pub struct Segment {
    pub hash: String,
    pub from_block: u64,
    pub to_block: u64,
    pub rows: u64,
    /// The file name under `segments/` on a node; a mirror keys segments by table and hash instead.
    #[serde(default)]
    pub file: String,
    #[serde(default)]
    pub provisional: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Catalogue {
    pub tables: BTreeMap<String, Vec<Segment>>,
}

impl Catalogue {
    pub fn parse(bytes: &[u8]) -> Result<Catalogue> {
        serde_json::from_slice(bytes).context("segments/manifest.json does not parse")
    }
}

/// One table the query reads, with its final segments that can hold rows at or below the last frame.
#[derive(Debug, Clone)]
pub struct Source {
    pub table: String,
    pub segments: Vec<Segment>,
}

#[derive(Debug, Clone)]
pub struct Plan {
    pub closes: Vec<u64>,
    pub step: u64,
    /// The first sealed block of any table read. The pass starts here, not at `from_block`: the state
    /// at `from_block` is the sum of everything before it.
    pub start: u64,
    /// The last frame's closing block.
    pub end: u64,
    pub sources: Vec<Source>,
    /// Consecutive inclusive block ranges covering `start..=end`.
    pub chunks: Vec<(u64, u64)>,
}

impl Plan {
    /// Chunks are cut on frame closes, and before the range on segment ends, and hold at most
    /// `chunk_rows` catalogue rows unless one cut-to-cut span alone holds more.
    pub fn new(spec: &Spec, shape: &Shape, catalogue: &Catalogue, chunk_rows: u64) -> Result<Plan> {
        let closes = spec.closes()?;
        let step = spec.step()?;
        let end = *closes.last().context("no frames")?;
        let watermark = catalogue
            .tables
            .values()
            .flatten()
            .map(|s| s.to_block)
            .max()
            .context("the catalogue holds no segments")?;
        let mut sources = Vec::new();
        for table in &shape.tables {
            let segs = catalogue
                .tables
                .get(table)
                .with_context(|| format!("table {table} has no sealed segments"))?;
            // A provisional segment is replaced at the table's next seal, and the hot tail can still
            // reorg, so neither is read.
            let through = segs
                .iter()
                .filter(|s| s.provisional)
                .map(|s| s.from_block.saturating_sub(1))
                .min()
                .unwrap_or(watermark);
            ensure!(
                end <= through,
                "the replay needs block {end} of {table}, which is final only through {through}"
            );
            let mut segments: Vec<Segment> = segs
                .iter()
                .filter(|s| !s.provisional && s.from_block <= end)
                .cloned()
                .collect();
            segments.sort_by_key(|s| s.from_block);
            sources.push(Source {
                table: table.clone(),
                segments,
            });
        }
        let all = || sources.iter().flat_map(|s| s.segments.iter());
        let Some(start) = all().map(|s| s.from_block).min() else {
            bail!("no final segment at or below block {end}");
        };
        let start = start.min(spec.from_block);

        let mut cuts: Vec<u64> = all()
            .map(|s| s.to_block)
            .filter(|b| *b < spec.from_block && *b >= start)
            .chain(closes.iter().copied())
            .collect();
        cuts.sort_unstable();
        cuts.dedup();
        let rows = |a: u64, b: u64| -> u64 {
            all()
                .filter(|s| s.from_block <= b && s.to_block >= a)
                .map(|s| s.rows)
                .sum()
        };
        let mut chunks = Vec::new();
        let (mut lo, mut prev) = (start, None::<u64>);
        for &c in &cuts {
            if let Some(p) = prev {
                if rows(lo, c) > chunk_rows {
                    chunks.push((lo, p));
                    lo = p + 1;
                }
            }
            prev = Some(c);
        }
        chunks.push((lo, end));
        Ok(Plan {
            closes,
            step,
            start,
            end,
            sources,
            chunks,
        })
    }

    /// The source's segments that can hold rows in `lo..=hi`.
    pub fn overlapping(source: &Source, lo: u64, hi: u64) -> impl Iterator<Item = &Segment> {
        source
            .segments
            .iter()
            .filter(move |s| s.from_block <= hi && s.to_block >= lo)
    }

    /// The statement each chunk runs, with its tables bound to the chunk's block window. The order
    /// is the fold's; `key` within a log only makes the stream reproducible.
    pub fn chunk_sql(sql: &str) -> String {
        format!("SELECT * FROM ({sql}) AS q ORDER BY block_number, log_index, key")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seg(from: u64, to: u64, rows: u64, provisional: bool) -> Segment {
        Segment {
            hash: format!("{from}-{to}"),
            from_block: from,
            to_block: to,
            rows,
            file: String::new(),
            provisional,
        }
    }

    fn spec(from: u64, to: u64, frames: u64) -> Spec {
        Spec::parse(&format!(
            "title = \"t\"\nnest = \"n\"\nfrom_block = {from}\nto_block = {to}\nframes = {frames}\n\
             top = 3\n[quantity]\nfold = \"sum\"\nsql = \"\"\n"
        ))
        .unwrap()
    }

    fn shape() -> Shape {
        Shape {
            tables: ["t".to_string()].into(),
            columns: vec![],
            has_sign: false,
            has_timestamp: false,
        }
    }

    fn catalogue(segs: Vec<Segment>) -> Catalogue {
        Catalogue {
            tables: [("t".to_string(), segs)].into(),
        }
    }

    /// §4.3: the pass starts at the nest's first sealed block, not at `from_block`, and frames close
    /// only inside the range.
    #[test]
    fn the_pass_starts_at_the_first_sealed_block_not_at_from_block() {
        let cat = catalogue(vec![seg(10, 99, 5, false), seg(100, 299, 5, false)]);
        let plan = Plan::new(&spec(200, 299, 4), &shape(), &cat, 1_000).unwrap();
        assert_eq!(plan.start, 10);
        assert_eq!(plan.chunks.first().unwrap().0, 10);
        assert!(plan.closes.iter().all(|c| (200..=299).contains(c)));
        assert_eq!(plan.closes, vec![224, 249, 274, 299]);
    }

    #[test]
    fn chunks_cover_the_pass_without_gaps_and_end_on_frame_closes() {
        let segs = (0..30).map(|i| seg(i * 10, i * 10 + 9, 7, false)).collect();
        let plan = Plan::new(&spec(100, 299, 20), &shape(), &catalogue(segs), 20).unwrap();
        assert!(plan.chunks.len() > 1);
        assert_eq!(plan.chunks[0].0, plan.start);
        assert_eq!(plan.chunks.last().unwrap().1, plan.end);
        for w in plan.chunks.windows(2) {
            assert_eq!(w[0].1 + 1, w[1].0);
        }
        for (_, hi) in &plan.chunks {
            assert!(plan.closes.contains(hi) || *hi < 100);
        }
    }

    #[test]
    fn a_provisional_segment_is_skipped_and_the_refusal_names_the_block() {
        let cat = catalogue(vec![seg(0, 99, 5, false), seg(100, 149, 5, true)]);
        let err = Plan::new(&spec(0, 199, 2), &shape(), &cat, 1_000).unwrap_err();
        assert_eq!(
            err.to_string(),
            "the replay needs block 199 of t, which is final only through 99"
        );
        let plan = Plan::new(&spec(0, 99, 2), &shape(), &cat, 1_000).unwrap();
        assert_eq!(plan.sources[0].segments.len(), 1);
    }

    #[test]
    fn a_range_above_the_sealed_watermark_is_refused() {
        let cat = catalogue(vec![seg(0, 99, 5, false)]);
        let err = Plan::new(&spec(0, 199, 2), &shape(), &cat, 1_000).unwrap_err();
        assert!(err.to_string().contains("needs block 199"), "{err}");
    }
}
