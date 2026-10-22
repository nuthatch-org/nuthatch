//! `manifest.json` (§4.4): what a recording was made from, so anyone holding the nest can recompute
//! `frames_sha256` and compare.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::plan::Plan;
use crate::spec::Spec;
use crate::{Recording, FOLD_VERSION};

/// A nest's NID and data identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Identity {
    pub nid: String,
    pub data_identity: String,
}

#[derive(Deserialize)]
struct Config {
    nest: NestSection,
}

#[derive(Deserialize)]
struct NestSection {
    name: String,
    #[serde(default = "one")]
    schema_version: u32,
}

fn one() -> u32 {
    1
}

#[derive(Deserialize)]
struct SchemaDoc {
    registry_hash: String,
}

/// The identity of a nest whose authored inputs are `files`, by path relative to the nest root, as
/// `nuthatch_identity` computes it. The registry hash is the one `schema.json` records rather than one
/// regenerated from the config and ABIs, which only the main crate can do: a `schema.json` written by
/// a different decode would give a different identity from the node's.
pub fn identity(files: BTreeMap<String, Vec<u8>>) -> Result<Identity> {
    let config: Config = toml::from_str(
        std::str::from_utf8(files.get("nuthatch.toml").context("no nuthatch.toml")?)
            .context("nuthatch.toml is not UTF-8")?,
    )
    .context("nuthatch.toml")?;
    let schema: SchemaDoc =
        serde_json::from_slice(files.get("schema.json").context("no schema.json")?)
            .context("schema.json has no registry_hash")?;
    let registry = schema.registry_hash.trim_start_matches("0x").to_string();
    let m = nuthatch_identity::manifest(
        config.nest.name,
        config.nest.schema_version,
        // Part of neither identity.
        "timelapse".into(),
        registry,
        files,
    )?;
    Ok(Identity {
        nid: m.nid(),
        data_identity: m.data_identity(),
    })
}

#[derive(Debug, Serialize)]
pub struct SegmentRead {
    pub table: String,
    pub hash: String,
    pub from_block: u64,
    pub to_block: u64,
}

/// Field order is fixed by the struct, so the file is the same bytes however serde_json is built.
#[derive(Debug, Serialize)]
pub struct Manifest {
    pub title: String,
    pub nest: String,
    pub nid: Option<String>,
    pub data_identity: Option<String>,
    pub registry_hash: Option<String>,
    pub spec_sha256: String,
    pub fold: &'static str,
    pub fold_version: &'static str,
    /// Where the pass began: the first sealed block of any table read, at or before `from_block`.
    pub first_block: u64,
    pub from_block: u64,
    /// The last frame's closing block.
    pub to_block: u64,
    pub step_blocks: u64,
    pub frames: usize,
    pub top: usize,
    pub fps: Option<u32>,
    pub segments_read: Vec<SegmentRead>,
    pub frames_sha256: String,
}

impl Manifest {
    pub fn new(
        spec: &Spec,
        spec_bytes: &[u8],
        plan: &Plan,
        recording: &Recording,
        identity: Option<&Identity>,
        registry_hash: Option<String>,
    ) -> Manifest {
        Manifest {
            title: spec.title.clone(),
            nest: spec.nest.clone(),
            nid: identity.map(|i| i.nid.clone()),
            data_identity: identity.map(|i| i.data_identity.clone()),
            registry_hash,
            spec_sha256: crate::sha256_hex(spec_bytes),
            fold: spec.quantity.fold.name(),
            fold_version: FOLD_VERSION,
            first_block: plan.start,
            from_block: spec.from_block,
            to_block: plan.end,
            step_blocks: plan.step,
            frames: recording.frames.len(),
            top: spec.top,
            fps: spec.fps,
            segments_read: plan
                .sources
                .iter()
                .flat_map(|s| {
                    s.segments.iter().map(|g| SegmentRead {
                        table: s.table.clone(),
                        hash: g.hash.clone(),
                        from_block: g.from_block,
                        to_block: g.to_block,
                    })
                })
                .collect(),
            frames_sha256: recording.frames_sha256.clone(),
        }
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("the manifest serializes") + "\n"
    }
}
