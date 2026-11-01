//! RFC-0077 S4: any nest. Its `nuthatch.toml`, ABIs and labels, read through the node's own parsing
//! in `nuthatch_core::nest_config`, on the chain policy `nuthatch_core::chains` gives the node.

use crate::derived::NestFiles;
use anyhow::{anyhow, bail, Context, Result};
use nuthatch_core::chains;
use nuthatch_core::factory::FactorySet;
use nuthatch_core::labels::{LabelSet, LABELS_DIR};
use nuthatch_core::nest_config::NestConfig;
use nuthatch_core::window::Finality;
use nuthatch_decode::registry::DecodeRegistry;

pub const CONFIG_FILE: &str = "nuthatch.toml";

/// When a chain's blocks are final, how far a held range may go unsealed, and the getLogs span.
#[derive(Clone, Copy, Debug)]
pub struct ChainPolicy {
    pub chain_id: u64,
    pub finality: Finality,
    pub seal_span: u64,
    pub log_window: u64,
}

impl ChainPolicy {
    /// `chain`'s registered policy, or the node's fallback for a chain it does not ship.
    pub fn of(chain: &str, chain_id: u64) -> ChainPolicy {
        match chains::lookup(chain) {
            Some(c) => ChainPolicy {
                chain_id: c.chain_id,
                finality: c.finality,
                seal_span: c.seal_span,
                log_window: c.log_window,
            },
            None => ChainPolicy {
                chain_id,
                finality: chains::UNREGISTERED_FINALITY,
                seal_span: chains::DEFAULT_SEAL_SPAN,
                log_window: chains::UNREGISTERED_WINDOW,
            },
        }
    }
}

/// What a nest indexes and how, read from its files.
pub struct NestSpec {
    pub name: String,
    pub schema_version: u32,
    pub registry: DecodeRegistry,
    pub addresses: Vec<String>,
    pub topic0s: Vec<String>,
    pub factory: Option<FactorySet>,
    pub labels: LabelSet,
    pub threshold: Option<i128>,
    pub velocity: Option<(i128, u64)>,
    pub chain: ChainPolicy,
    /// The earliest declared start block, where one is declared.
    pub start: Option<u64>,
    /// Each contract's declared start block, by alias.
    pub starts: std::collections::BTreeMap<String, u64>,
    /// RFC-0077 S7: the filters a narrowed nest asks for in place of its contracts' every log.
    pub narrowed: Option<Vec<crate::narrow::Narrowed>>,
    /// RFC-0077 S7: the last block the cursor indexes, so the nest answers as of it.
    pub until: Option<u64>,
    /// RFC-0081 S1: a hybrid open hydrates only these tables from its mirror, each whole.
    pub hydrate: Option<std::collections::BTreeSet<String>>,
}

impl NestSpec {
    /// The nest `files` describes: `nuthatch.toml`, the ABIs it names and any `labels/*.json`.
    pub fn from_files(files: &NestFiles) -> Result<NestSpec> {
        let raw = files
            .get(CONFIG_FILE)
            .ok_or_else(|| anyhow!("the nest's files hold no {CONFIG_FILE}"))?;
        let config = NestConfig::parse(raw)?;
        let registry = config.registry(files)?;
        let factory = FactorySet::resolved(config.declared(), &registry)?;
        let factory = (!factory.is_empty()).then_some(factory);
        let mut labels = LabelSet::default();
        for (path, text) in files {
            let in_labels = path
                .strip_prefix(LABELS_DIR)
                .and_then(|p| p.strip_prefix('/'))
                .is_some_and(|p| !p.contains('/') && p.ends_with(".json"));
            // Skipped unread as the node skips it, with nothing said: a module has no log to say it in.
            if in_labels {
                labels.merge_snapshot(text);
            }
        }
        // A factory nest asks by address in `fetch_window`; an empty list marks it, as on a node.
        let addresses = if factory.is_some() {
            Vec::new()
        } else {
            registry
                .addresses()
                .iter()
                .map(|a| format!("0x{}", hex::encode(a)))
                .collect()
        };
        let topic0s = registry
            .topic0s()
            .iter()
            .map(|t| format!("0x{}", hex::encode(t)))
            .collect();
        Ok(NestSpec {
            name: config.nest.name.clone(),
            schema_version: config.nest.schema_version,
            addresses,
            topic0s,
            factory,
            labels,
            threshold: config.flags.threshold_amount(),
            velocity: config.flags.velocity(),
            chain: ChainPolicy::of(&config.nest.chain, config.nest.chain_id),
            start: config.contracts.iter().filter_map(|c| c.start_block).min(),
            starts: config
                .contracts
                .iter()
                .filter_map(|c| Some((c.alias.clone(), c.start_block?)))
                .collect(),
            narrowed: None,
            until: None,
            hydrate: None,
            registry,
        })
    }

    /// RFC-0075's young contract: one ERC-20's `Transfer` on mainnet from `start`.
    pub fn erc20(contract: &str, start: u64) -> Result<NestSpec> {
        Ok(Self::of_registry(
            crate::tab::erc20(contract)?,
            vec![contract.to_string()],
            vec![crate::tab::TRANSFER.into()],
            start,
        ))
    }

    /// `addresses`' `topic0s` on mainnet from `start`, decoded by `registry`, with no factory,
    /// labels or flags.
    pub fn of_registry(
        registry: DecodeRegistry,
        addresses: Vec<String>,
        topic0s: Vec<String>,
        start: u64,
    ) -> NestSpec {
        NestSpec {
            name: "tab".into(),
            schema_version: 1,
            registry,
            addresses,
            topic0s,
            factory: None,
            labels: LabelSet::default(),
            threshold: None,
            velocity: None,
            chain: ChainPolicy::of("mainnet", 1),
            start: Some(start),
            starts: Default::default(),
            narrowed: None,
            until: None,
            hydrate: None,
        }
    }

    /// The nest's data identity (RFC-0033 §5), computed from `files` as a node computes it from its
    /// directory: the registry it builds, and every file that can change a stored byte.
    pub fn data_identity(&self, files: &NestFiles) -> Result<String> {
        Ok(self.manifest(files)?.data_identity())
    }

    /// The nest's NID (RFC-0032 §3), the one a node's `/sql` names in `provenance.nid`, and its data
    /// identity, both from `files` as given.
    pub fn identity(&self, files: &NestFiles) -> Result<(String, String)> {
        let manifest = self.manifest(files)?;
        Ok((manifest.nid(), manifest.data_identity()))
    }

    fn manifest(&self, files: &NestFiles) -> Result<nuthatch_identity::Manifest> {
        nuthatch_identity::manifest(
            self.name.clone(),
            self.schema_version,
            crate::young::node_version().to_string(),
            hex::encode(self.registry.hash()),
            files
                .iter()
                .map(|(p, text)| (p.clone(), text.as_bytes().to_vec())),
        )
        .context("computing the nest's identity from its files")
    }

    /// Where a nest that has not indexed starts, refused when it declares nowhere.
    pub fn start_block(&self) -> Result<u64> {
        match self.start {
            Some(s) => Ok(s),
            None => bail!(
                "no contract in nuthatch.toml declares a start_block, and an embedded nest starts \
                 only from one, or from a mirror's history"
            ),
        }
    }
}
