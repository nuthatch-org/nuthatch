//! Factory patterns and the dynamic child registry (RFC-0009, step 1).
//!
//! A *factory* watches a contract's event (`PoolCreated`) and indexes the child contract it announces
//! under a *template* ABI - the dynamic-data-sources capability, without which Uniswap-class protocols
//! are unindexable. This module is the foundation: the validated rule set ([`FactorySet`]) and the
//! discovered-child registry ([`ChildRegistry`]). Both are pure and deterministic - the registry state
//! at block B is a fold over factory events ≤ B, so it reproduces and its content hash is stable. The
//! ingestion regimes (tip, sequential/pipelined backfill, ExEx) that *consume* this land in later
//! steps; here we build the state they read and prove its reorg + determinism properties.

use anyhow::{bail, Result};
use nuthatch_decode::registry::{snake_case, DecodeRegistry, DecodedRow, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};

/// One discovered child contract and its provenance. `discovered_timestamp` (from the shipped
/// `block_timestamp` implicit column) lets the `{template}__children` view answer "pools created this
/// week" without a join. `depth` is 1 for a factory on a static contract; deeper for nested factories.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildEntry {
    pub template: String,
    pub address: String,
    pub discovered_block: u64,
    pub discovered_log_index: u64,
    pub discovered_timestamp: u64,
    pub parent_address: String,
    pub depth: u8,
}

/// A validated factory rule, keyed internally by the announcing table name (`{watch}__{event}`).
#[derive(Debug, Clone)]
struct FactoryRule {
    child_param: String,
    template: String,
    start: Option<u64>,
    /// Depth of the *watched* source: 0 for a static contract, or the depth of the watched template.
    watch_depth: u8,
}

/// The validated set of factory rules for a nest. Built from config with reference checks; the
/// ABI-level checks (the event exists, `child_param` is an address parameter) are enforced when the
/// decode registry gains template decoders (a later step) - a rule here is structurally sound.
#[derive(Debug, Default, Clone)]
pub struct FactorySet {
    /// announcing table (`{watch}__{event_snake}`) → rule.
    /// Announcing table → **every** rule fired by that event.
    ///
    /// A `Vec`, not a single rule, because one event legitimately announces several children. The
    /// motivating shape from issue #241 is `OrgDeployed`, which carries ten module addresses in ten
    /// named params; the obvious encoding is ten factory rules on one event. This was a
    /// `HashMap<String, FactoryRule>` keyed by `{watch}__{event}`, so those ten silently collapsed to
    /// **one** - last declaration wins, ordering-dependent, and the startup log still reported the
    /// full count. Every other template got tables and a `__children` view and stayed empty forever.
    rules: HashMap<String, Vec<FactoryRule>>,
    /// Every declared template name (a factory's `template` must be one of these).
    template_names: HashSet<String>,
    /// True if any template requests the topic0-only backfill filter (RFC-0009 §4 override).
    force_topic0: bool,
}

/// What a nest declares that the rules are built from: its contract aliases, its templates and its
/// `[[factories]]`. The node fills it from its config.
#[derive(Debug, Default, Clone)]
pub struct Declared {
    pub contracts: Vec<String>,
    pub templates: Vec<TemplateDecl>,
    pub factories: Vec<FactoryDecl>,
}

#[derive(Debug, Clone)]
pub struct TemplateDecl {
    pub name: String,
    /// The template asks for the topic0-only backfill filter (RFC-0009 §4 override).
    pub topic0_only: bool,
}

#[derive(Debug, Clone)]
pub struct FactoryDecl {
    /// The alias of the watched contract (or a template, for nested factories).
    pub watch: String,
    /// The announcing event name, or its full signature when the ABI overloads it.
    pub event: String,
    /// The event parameter holding the child contract address.
    pub child_param: String,
    pub template: String,
    /// Only honour discoveries at or after this block.
    pub start: Option<u64>,
}

impl FactorySet {
    /// [`Self::build_with`], each rule's announcing table taken from the decoders `registry` actually
    /// registered (#2158). An overloaded event's table carries a topic0 suffix, so the name alone is
    /// refused as ambiguous and the rule must give the full signature.
    pub fn resolved(config: impl Into<Declared>, registry: &DecodeRegistry) -> Result<Self> {
        Self::build_with(&config.into(), |f| announcing_table(f, registry))
    }

    /// [`Self::build_with`], each announcing table named `{watch}__{event}`.
    pub fn build(config: impl Into<Declared>) -> Result<Self> {
        Self::build_with(&config.into(), |f| {
            Ok(format!("{}__{}", f.watch, snake_case(&f.event)))
        })
    }

    /// Build + validate the factory rules, each announcing table named by `table_of`. Errors on a rule
    /// referencing an unknown `watch` alias/template or an unknown `template`, on a template naming
    /// an existing contract alias (ambiguous table namespace), or on nesting deeper than the depth-3
    /// ceiling.
    pub fn build_with(
        config: &Declared,
        table_of: impl Fn(&FactoryDecl) -> Result<String>,
    ) -> Result<Self> {
        let contract_aliases: HashSet<&str> = config.contracts.iter().map(String::as_str).collect();
        let template_names: HashSet<String> =
            config.templates.iter().map(|t| t.name.clone()).collect();

        for t in &config.templates {
            if contract_aliases.contains(t.name.as_str()) {
                bail!(
                    "template '{}' collides with a contract alias - table namespaces would clash",
                    t.name
                );
            }
        }

        // Depth of each watchable source: contracts are 0; templates are 1 + the depth of whatever
        // discovers them (resolved iteratively, since a template may be watched by another factory).
        let mut watch_depth: HashMap<String, u8> = HashMap::new();
        for a in &contract_aliases {
            watch_depth.insert((*a).to_string(), 0);
        }

        let mut rules = HashMap::new();
        for f in &config.factories {
            if !contract_aliases.contains(f.watch.as_str())
                && !template_names.contains(f.watch.as_str())
            {
                bail!(
                    "factory watches '{}', which is neither a contract alias nor a template",
                    f.watch
                );
            }
            if !template_names.contains(f.template.as_str()) {
                bail!(
                    "factory produces template '{}', which is not declared under [[templates]]",
                    f.template
                );
            }
            let table = table_of(f)?;
            // A duplicate `(watch, event, child_param, template)` is a copy-paste error rather than a
            // second child, and silently indexing it twice would double every discovery.
            let entry = rules.entry(table).or_insert_with(Vec::new);
            if entry
                .iter()
                .any(|r: &FactoryRule| r.child_param == f.child_param && r.template == f.template)
            {
                bail!(
                    "two identical factory rules watch '{}' event '{}' for child_param '{}' → \
                     template '{}' - remove the duplicate",
                    f.watch,
                    f.event,
                    f.child_param,
                    f.template
                );
            }
            entry.push(FactoryRule {
                child_param: f.child_param.clone(),
                template: f.template.clone(),
                start: f.start,
                // Filled once all sources' depths are known (below).
                watch_depth: 0,
            });
        }

        // Resolve template depths: a template watched by a factory whose source has depth d gets
        // depth d+1. Iterate to a fixpoint (chains are short - the ceiling is 3); a template can be
        // produced by at most the declared factories, so this terminates.
        for _ in 0..3 {
            for f in &config.factories {
                let src = *watch_depth.get(&f.watch).unwrap_or(&0);
                let child = src + 1;
                let cur = watch_depth.entry(f.template.clone()).or_insert(child);
                if child > *cur {
                    *cur = child;
                }
            }
        }
        // Stamp each rule's watch depth and enforce the depth-3 ceiling on the produced child.
        for f in &config.factories {
            let table = table_of(f)?;
            let src_depth = *watch_depth.get(&f.watch).unwrap_or(&0);
            if src_depth + 1 > 3 {
                bail!(
                    "factory chain exceeds the depth-3 ceiling (template '{}' at depth {})",
                    f.template,
                    src_depth + 1
                );
            }
            if let Some(rs) = rules.get_mut(&table) {
                for rule in rs.iter_mut().filter(|r| r.template == f.template) {
                    rule.watch_depth = src_depth;
                }
            }
        }

        let force_topic0 = config.templates.iter().any(|t| t.topic0_only);

        Ok(Self {
            rules,
            template_names,
            force_topic0,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// Whether a template forces the topic0-only backfill filter (RFC-0009 §4 per-template override).
    pub fn force_topic0(&self) -> bool {
        self.force_topic0
    }

    /// Every declared template name.
    pub fn templates(&self) -> &HashSet<String> {
        &self.template_names
    }

    /// The announcing table names (`{watch}__{event}`) - the tables to fold on a restart rebuild.
    pub fn factory_tables(&self) -> Vec<String> {
        self.rules.keys().cloned().collect()
    }

    /// `(template, announcing_table, child_param)` for every rule - the sources the analytics layer
    /// unions into each `{template}__children` view (RFC-0009 §Serving).
    pub fn view_sources(&self) -> Vec<(String, String, String)> {
        self.rules
            .iter()
            .flat_map(|(table, rs)| {
                rs.iter()
                    .map(move |r| (r.template.clone(), table.clone(), r.child_param.clone()))
            })
            .collect()
    }

    /// Discover a child from a *stored* factory-event row (JSON), for the warm-restart rebuild. Same
    /// semantics as [`discover`] but reading the persisted columns rather than a live `DecodedRow`.
    /// Returns **every** child this row announces - one event may name several (issue #241).
    pub fn discover_stored(&self, table: &str, row: &serde_json::Value) -> Vec<ChildEntry> {
        let Some(rules) = self.rules.get(table) else {
            return Vec::new();
        };
        let Some(block) = row.get("block_number").and_then(serde_json::Value::as_u64) else {
            return Vec::new();
        };
        rules
            .iter()
            .filter(|r| r.start.is_none_or(|s| block >= s))
            .filter_map(|rule| {
                let address = row.get(&rule.child_param)?.as_str()?.to_ascii_lowercase();
                // A param that is absent, or not an address, means *this* rule did not fire - it must
                // not stop the sibling rules on the same event from firing.
                if !address.starts_with("0x") || address.len() != 42 {
                    return None;
                }
                Some(ChildEntry {
                    template: rule.template.clone(),
                    address,
                    discovered_block: block,
                    discovered_log_index: row
                        .get("log_index")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or(0),
                    discovered_timestamp: row
                        .get("block_timestamp")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or(0),
                    parent_address: row
                        .get("address")
                        .and_then(|a| a.as_str())
                        .unwrap_or("")
                        .to_string(),
                    depth: rule.watch_depth + 1,
                })
            })
            .collect()
    }

    /// If `row` is a factory-announcement event, the child contract it discovers - else `None`. A
    /// pure function of the decoded row and the rule set (the fold step the registry is built from).
    /// Every child this row announces - a `Vec`, because one event may name several (issue #241).
    /// Empty when the row is not a factory announcement, which is the common case.
    pub fn discover(&self, row: &DecodedRow) -> Vec<ChildEntry> {
        let Some(rules) = self.rules.get(&row.table) else {
            return Vec::new();
        };
        rules
            .iter()
            .filter(|r| r.start.is_none_or(|s| row.block_number >= s))
            .filter_map(|rule| {
                let address = row.params.iter().find_map(|(name, v)| {
                    if name != &rule.child_param {
                        return None;
                    }
                    match v {
                        Value::Address(a) => Some(format!("0x{}", hex::encode(a))),
                        _ => None,
                    }
                })?;
                Some(ChildEntry {
                    template: rule.template.clone(),
                    address,
                    discovered_block: row.block_number,
                    discovered_log_index: row.log_index,
                    discovered_timestamp: row.block_timestamp,
                    parent_address: row.address.clone(),
                    depth: rule.watch_depth + 1,
                })
            })
            .collect()
    }
}

/// The table `registry` decodes a factory rule's event into: by name, or by full signature when the
/// name is overloaded.
pub fn announcing_table(f: &FactoryDecl, registry: &DecodeRegistry) -> Result<String> {
    let by_signature = f.event.contains('(');
    let want: String = f.event.chars().filter(|c| !c.is_whitespace()).collect();
    let matches: Vec<_> = registry
        .tables()
        .into_iter()
        .filter(|d| d.alias == f.watch)
        .filter(|d| {
            if by_signature {
                d.signature == want
            } else {
                let name = d.signature.split('(').next().unwrap_or_default();
                snake_case(name) == snake_case(&f.event)
            }
        })
        .collect();
    match matches.as_slice() {
        [d] => Ok(d.table.clone()),
        [] => bail!(
            "factory watches '{}' event '{}', which '{}' does not decode: it is not in the ABI, \
             or the `events` allowlist leaves it out. No child would ever be discovered.",
            f.watch,
            f.event,
            f.watch
        ),
        many => bail!(
            "factory watches '{}' event '{}', which is overloaded: write the full signature, one of {}",
            f.watch,
            f.event,
            many.iter()
                .map(|d| format!("\"{}\"", d.signature))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

/// The registry of discovered children, keyed by address (`BTreeMap` for deterministic iteration).
/// `version` (RFC-0009 §3's `filter_version`) is a monotonic counter bumped whenever the set changes,
/// so the pipelined backfill can tell which fetched windows predate a discovery.
#[derive(Debug, Default, Clone)]
pub struct ChildRegistry {
    children: BTreeMap<String, ChildEntry>,
    version: u64,
}

impl ChildRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a discovered child. Returns true (and bumps `version`) if it was newly added; a repeat
    /// discovery of the same address is idempotent - the earliest discovery wins and nothing changes.
    pub fn insert(&mut self, entry: ChildEntry) -> bool {
        if self.children.contains_key(&entry.address) {
            return false;
        }
        self.children.insert(entry.address.clone(), entry);
        self.version += 1;
        true
    }

    /// Register every child of `other` this registry does not already know.
    pub fn extend(&mut self, other: ChildRegistry) {
        for entry in other.children.into_values() {
            self.insert(entry);
        }
    }

    /// Fill `discovered_timestamp` from a block-timestamp map when discovery ran before the stamp
    /// (#765: timestamps are fetched only for blocks that produced a kept row). Does not bump
    /// `version` - the set of children is unchanged.
    pub fn apply_timestamps(&mut self, ts: &HashMap<u64, u64>) {
        for child in self.children.values_mut() {
            if child.discovered_timestamp == 0 {
                if let Some(&t) = ts.get(&child.discovered_block) {
                    child.discovered_timestamp = t;
                }
            }
        }
    }

    pub fn contains(&self, address: &str) -> bool {
        self.children.contains_key(address)
    }

    pub fn get(&self, address: &str) -> Option<&ChildEntry> {
        self.children.get(address)
    }

    /// The template a discovered address belongs to (for routing its logs to the right decoder).
    pub fn template_of(&self, address: &str) -> Option<&str> {
        self.children.get(address).map(|e| e.template.as_str())
    }

    /// All discovered child addresses (sorted).
    pub fn addresses(&self) -> Vec<&str> {
        self.children.keys().map(String::as_str).collect()
    }

    /// Discovered children of one template (sorted).
    pub fn addresses_for_template(&self, template: &str) -> Vec<&str> {
        self.children
            .values()
            .filter(|e| e.template == template)
            .map(|e| e.address.as_str())
            .collect()
    }

    /// All discovered children (sorted by address) - for the `{template}__children` view.
    pub fn entries(&self) -> impl Iterator<Item = &ChildEntry> {
        self.children.values()
    }

    /// Reorg: drop every child discovered strictly above `block` (the announcing factory event was
    /// rolled back). Bumps `version` if anything changed. Returns how many were removed. The child's
    /// own event rows are covered by the hot store's block-range rollback - this handles the registry.
    pub fn rollback_to(&mut self, block: u64) -> u64 {
        let doomed: Vec<String> = self
            .children
            .values()
            .filter(|e| e.discovered_block > block)
            .map(|e| e.address.clone())
            .collect();
        if doomed.is_empty() {
            return 0;
        }
        for a in &doomed {
            self.children.remove(a);
        }
        self.version += 1;
        doomed.len() as u64
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn len(&self) -> usize {
        self.children.len()
    }

    pub fn is_empty(&self) -> bool {
        self.children.is_empty()
    }

    /// A content hash of the registry state - the `registry_snapshot` written into each sealed
    /// segment's manifest entry (a later step) so a segment records exactly which discovered set
    /// produced it. Deterministic: a fold over the same factory events yields the same hash,
    /// independent of `version` (which counts changes, not state).
    pub fn hash(&self) -> String {
        let mut h = Sha256::new();
        for e in self.children.values() {
            h.update(e.address.as_bytes());
            h.update([0]);
            h.update(e.template.as_bytes());
            h.update(e.discovered_block.to_be_bytes());
            h.update(e.discovered_log_index.to_be_bytes());
            h.update(e.parent_address.as_bytes());
            h.update([e.depth]);
        }
        hex::encode(h.finalize())
    }
}
