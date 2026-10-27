//! `nuthatch nest inspect` (RFC-0073 FR10): what a bundle or nest directory is and where it would
//! reach, read without a runtime and without the network. The JSON form is versioned by
//! [`SCHEMA`]; a field is only ever added under the same version, never renamed or removed.

use crate::config::{Config, GRAPH_SCHEMA_VERSION};
use anyhow::{bail, Context, Result};
use serde::Serialize;
use std::path::{Path, PathBuf};

pub const SCHEMA: &str = "nuthatch.nest-inspect/1";

/// Every outbound endpoint a nest declares, raw. The one collection `nest load`'s warning and
/// `nest inspect` both read, so neither can list an egress the other misses.
#[derive(Debug, Default)]
pub struct Egress {
    pub rpc_urls: Vec<String>,
    pub webhooks: Vec<WebhookSink>,
    pub alerts: Vec<AlertSink>,
    pub ipfs_declarations: Vec<String>,
    /// The gateways the `[[ipfs]]` declarations resolve through unless `--ipfs` names others.
    pub ipfs_gateways: Vec<String>,
    pub offchain: Vec<OffchainSource>,
    /// Kept apart rather than failing the whole collection: a bundle with a corrupt offchain
    /// manifest must not silence the load warning about its other endpoints.
    pub offchain_error: Option<String>,
}

#[derive(Debug)]
pub struct WebhookSink {
    pub name: String,
    pub url: String,
    pub secret: bool,
}

#[derive(Debug)]
pub struct AlertSink {
    pub kinds: Vec<String>,
    pub url: String,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct OffchainSource {
    pub table: String,
    pub url: String,
}

impl Egress {
    pub fn of(dir: &Path, config: &Config) -> Egress {
        let ipfs_declarations: Vec<String> = config.ipfs.iter().map(|i| i.name.clone()).collect();
        let ipfs_gateways = if ipfs_declarations.is_empty() {
            Vec::new()
        } else if config.ipfs_gateways.is_empty() {
            crate::subgraph_import::DEFAULT_IPFS_GATEWAYS
                .iter()
                .map(|g| g.to_string())
                .collect()
        } else {
            config.ipfs_gateways.clone()
        };
        // A dropped file's source is a local path; only a pulled URL is egress.
        let (catalogue, offchain_error) = match crate::offchain::load(dir) {
            Ok(c) => (c, None),
            Err(e) => (Default::default(), Some(format!("{e:#}"))),
        };
        let is_url = |s: &str| s.starts_with("https://") || s.starts_with("http://");
        let mut offchain: Vec<OffchainSource> = catalogue
            .tables
            .iter()
            .flat_map(|(t, snaps)| snaps.iter().map(move |s| (t, &s.source)))
            .chain(catalogue.refreshes.iter().map(|(t, r)| (t, &r.source)))
            .filter(|(_, s)| is_url(s))
            .map(|(t, s)| OffchainSource {
                table: t.clone(),
                url: s.clone(),
            })
            .collect();
        offchain.sort();
        offchain.dedup();
        Egress {
            rpc_urls: config.nest.rpc_urls.clone(),
            webhooks: config
                .webhooks
                .iter()
                .map(|w| WebhookSink {
                    name: w.name.clone(),
                    url: w.url.clone(),
                    secret: w.secret.is_some(),
                })
                .collect(),
            alerts: config
                .alerts
                .iter()
                .map(|a| AlertSink {
                    kinds: a.kinds.clone(),
                    url: a.url.clone(),
                })
                .collect(),
            ipfs_declarations,
            ipfs_gateways,
            offchain,
            offchain_error,
        }
    }

    /// Every URL, labelled by kind, in the order the load warning prints them.
    pub fn urls(&self) -> Vec<(&'static str, &str)> {
        let mut out: Vec<(&'static str, &str)> = Vec::new();
        out.extend(self.webhooks.iter().map(|w| ("webhook", w.url.as_str())));
        out.extend(self.alerts.iter().map(|a| ("alert sink", a.url.as_str())));
        out.extend(self.rpc_urls.iter().map(|u| ("rpc", u.as_str())));
        out.extend(
            self.ipfs_gateways
                .iter()
                .map(|u| ("ipfs gateway", u.as_str())),
        );
        out.extend(
            self.offchain
                .iter()
                .map(|o| ("offchain source", o.url.as_str())),
        );
        out
    }
}

#[derive(Debug, Serialize)]
pub struct Inspection {
    pub schema: &'static str,
    pub nid: String,
    pub data_identity: String,
    /// The bundle's content address; `None` for a nest directory, which has no bundle yet.
    pub blob_hash: Option<String>,
    pub name: String,
    pub chain: String,
    pub chain_id: u64,
    pub contracts: Vec<ContractView>,
    pub templates: Vec<TemplateView>,
    pub factories: Vec<FactoryView>,
    pub entities: Vec<EntityView>,
    pub features: Vec<FeatureNeed>,
    pub extraction: Vec<ExtractionNeed>,
    pub egress: EgressView,
}

#[derive(Debug, Serialize)]
pub struct ContractView {
    pub alias: String,
    pub address: String,
    pub start_block: Option<u64>,
    pub events: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct TemplateView {
    pub name: String,
    pub events: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct FactoryView {
    pub watch: String,
    pub event: String,
    pub child_param: String,
    pub template: String,
}

#[derive(Debug, Serialize)]
pub struct EntityView {
    pub name: String,
    pub key: Vec<String>,
    pub max_rows: usize,
}

#[derive(Debug, Serialize)]
pub struct FeatureNeed {
    pub declaration: String,
    pub feature: &'static str,
}

/// `needs` is `archive` (historical `eth_call`), `trace` (call traces) or `state` (storage diffs).
#[derive(Debug, Serialize)]
pub struct ExtractionNeed {
    pub declaration: String,
    pub needs: &'static str,
}

#[derive(Debug, Serialize)]
pub struct EgressView {
    pub rpc_urls: Vec<RpcView>,
    pub webhooks: Vec<WebhookView>,
    pub alerts: Vec<AlertView>,
    pub ipfs: IpfsView,
    pub offchain: Vec<OffchainView>,
}

/// `url` is the scheme and host alone when `redacted`, since the rest of it carries a key.
#[derive(Debug, Serialize)]
pub struct RpcView {
    pub url: String,
    pub redacted: bool,
}

#[derive(Debug, Serialize)]
pub struct WebhookView {
    pub name: String,
    pub host: String,
    pub secret: bool,
}

#[derive(Debug, Serialize)]
pub struct AlertView {
    pub kinds: Vec<String>,
    pub host: String,
}

#[derive(Debug, Serialize)]
pub struct IpfsView {
    pub declarations: Vec<String>,
    pub gateways: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct OffchainView {
    pub table: String,
    pub host: String,
}

/// Inspect `target`: a `.bundle` file, an unpacked bundle directory, or a nest directory. A bundle is
/// verified and installed into a temporary directory exactly as `nest load` installs it.
pub fn inspect(target: &Path) -> Result<Inspection> {
    let s = target.to_string_lossy();
    if s.starts_with("http://") || s.starts_with("https://") {
        bail!(
            "nest inspect reads a local bundle or directory and never fetches; download it first"
        );
    }
    if target.is_file() {
        let tmp = tempfile::tempdir().context("temp dir for bundle")?;
        let blob_dir = tmp.path().join("unpacked");
        crate::blob::extract_bundle(target, &blob_dir)?;
        return inspect_blob(&blob_dir, tmp.path());
    }
    if !target.is_dir() {
        bail!(
            "nothing to inspect at '{}' - expected a .bundle file, a bundle directory or a nest directory",
            target.display()
        );
    }
    if target.join("manifest.json").is_file() {
        let tmp = tempfile::tempdir().context("temp dir for bundle")?;
        return inspect_blob(target, tmp.path());
    }
    let manifest = crate::blob::build_manifest(target, None)?;
    describe(target, &manifest, None)
}

/// The inspection of a bundle already checked and installed, as the admin API's bundle routes hold
/// one.
pub fn checked(bundle: &crate::blob::CheckedBundle) -> Result<Inspection> {
    describe(
        &bundle.nest_dir(),
        &bundle.manifest,
        Some(bundle.hash.clone()),
    )
}

fn inspect_blob(blob_dir: &Path, scratch: &Path) -> Result<Inspection> {
    let manifest = crate::blob::load_manifest(blob_dir)?;
    crate::blob::verify_blob(blob_dir, &manifest, None)?;
    let nest: PathBuf = scratch.join("nest");
    crate::blob::install_files(blob_dir, &manifest, &nest)?;
    describe(&nest, &manifest, Some(manifest.blob_hash()))
}

fn describe(
    dir: &Path,
    manifest: &crate::blob::Manifest,
    blob_hash: Option<String>,
) -> Result<Inspection> {
    let config = Config::load(dir)?;
    let entities = crate::entities::load(dir)?
        .into_iter()
        .map(|e| EntityView {
            name: e.name,
            key: e.key,
            max_rows: e.max_rows,
        })
        .collect();

    let mut features = Vec::new();
    if config.nest.schema_version == GRAPH_SCHEMA_VERSION {
        features.push(FeatureNeed {
            declaration: format!("nuthatch.toml schema_version {GRAPH_SCHEMA_VERSION}"),
            feature: "graph",
        });
    }
    if dir.join("graph/history.toml").exists() {
        features.push(FeatureNeed {
            declaration: "graph/history.toml".into(),
            feature: "graph",
        });
    }
    if dir.join("folds").exists() {
        features.push(FeatureNeed {
            declaration: "folds/".into(),
            feature: "folds",
        });
    }

    let mut extraction: Vec<ExtractionNeed> = config
        .calls
        .iter()
        .map(|c| ExtractionNeed {
            declaration: format!("[[calls]] {}", c.name),
            needs: "archive",
        })
        .collect();
    if config.extract.traces {
        extraction.push(ExtractionNeed {
            declaration: "[extract] traces".into(),
            needs: "trace",
        });
    }
    if config.extract.state {
        extraction.push(ExtractionNeed {
            declaration: "[extract] state".into(),
            needs: "state",
        });
    }
    if config.address_history.is_some() {
        extraction.push(ExtractionNeed {
            declaration: "[address_history]".into(),
            needs: "trace",
        });
    }

    let egress = Egress::of(dir, &config);
    if let Some(e) = &egress.offchain_error {
        bail!("cannot read the offchain sources: {e}");
    }
    let host = |u: &str| crate::rpc::redact_url(u);
    let egress = EgressView {
        rpc_urls: egress
            .rpc_urls
            .iter()
            .map(|u| {
                let redacted = crate::blob::url_carries_credential(u);
                RpcView {
                    url: if redacted { host(u) } else { u.clone() },
                    redacted,
                }
            })
            .collect(),
        webhooks: egress
            .webhooks
            .iter()
            .map(|w| WebhookView {
                name: w.name.clone(),
                host: host(&w.url),
                secret: w.secret,
            })
            .collect(),
        alerts: egress
            .alerts
            .iter()
            .map(|a| AlertView {
                kinds: a.kinds.clone(),
                host: host(&a.url),
            })
            .collect(),
        ipfs: IpfsView {
            declarations: egress.ipfs_declarations,
            gateways: egress.ipfs_gateways.iter().map(|g| host(g)).collect(),
        },
        offchain: egress
            .offchain
            .iter()
            .map(|o| OffchainView {
                table: o.table.clone(),
                host: host(&o.url),
            })
            .collect(),
    };

    Ok(Inspection {
        schema: SCHEMA,
        nid: manifest.nid(),
        data_identity: manifest.data_identity(),
        blob_hash,
        name: config.nest.name,
        chain: config.nest.chain,
        chain_id: config.nest.chain_id,
        contracts: config
            .contracts
            .into_iter()
            .map(|c| ContractView {
                alias: c.alias,
                address: c.address,
                start_block: c.start_block,
                events: c.events,
            })
            .collect(),
        templates: config
            .templates
            .into_iter()
            .map(|t| TemplateView {
                name: t.name,
                events: t.events,
            })
            .collect(),
        factories: config
            .factories
            .into_iter()
            .map(|f| FactoryView {
                watch: f.watch,
                event: f.event,
                child_param: f.child_param,
                template: f.template,
            })
            .collect(),
        entities,
        features,
        extraction,
        egress,
    })
}

/// `nuthatch nest inspect <bundle|dir> [--json]`.
pub fn run(target: &Path, json: bool) -> Result<()> {
    let i = inspect(target)?;
    if json {
        println!("{}", serde_json::to_string_pretty(&i)?);
        return Ok(());
    }
    let all = |v: &[String]| {
        if v.is_empty() {
            "all".to_string()
        } else {
            v.join(", ")
        }
    };
    println!("nest '{}' on {} (chain id {})", i.name, i.chain, i.chain_id);
    println!("  nid:           {}", i.nid);
    println!("  data identity: {}", i.data_identity);
    if let Some(h) = &i.blob_hash {
        println!("  bundle hash:   {h}");
    }
    println!("contracts:");
    for c in &i.contracts {
        let from = c.start_block.map_or("tip".to_string(), |b| b.to_string());
        println!(
            "  {} {} from {from}, events: {}",
            c.alias,
            c.address,
            all(&c.events)
        );
    }
    for t in &i.templates {
        println!("template {}: events {}", t.name, all(&t.events));
    }
    for f in &i.factories {
        println!(
            "factory: {}.{} announces `{}` as a {}",
            f.watch, f.event, f.child_param, f.template
        );
    }
    for e in &i.entities {
        println!(
            "entity {}: key ({}), max_rows {}",
            e.name,
            e.key.join(", "),
            e.max_rows
        );
    }
    for f in &i.features {
        println!("needs `--features {}`: {}", f.feature, f.declaration);
    }
    for x in &i.extraction {
        println!("needs {} RPC: {}", x.needs, x.declaration);
    }
    let e = &i.egress;
    println!("egress:");
    for r in &e.rpc_urls {
        let note = if r.redacted { " (key redacted)" } else { "" };
        println!("  rpc:             {}{note}", r.url);
    }
    for w in &e.webhooks {
        let secret = if w.secret { ", signed" } else { "" };
        println!("  webhook:         {} ({}{secret})", w.host, w.name);
    }
    for a in &e.alerts {
        println!("  alert sink:      {} ({})", a.host, a.kinds.join(", "));
    }
    for g in &e.ipfs.gateways {
        println!("  ipfs gateway:    {g}");
    }
    if !e.ipfs.declarations.is_empty() {
        println!("  ipfs documents:  {}", e.ipfs.declarations.join(", "));
    }
    for o in &e.offchain {
        println!("  offchain source: {} ({})", o.host, o.table);
    }
    Ok(())
}
