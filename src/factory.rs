//! Factory patterns and the dynamic child registry (RFC-0009, step 1).
//!
//! A *factory* watches a contract's event (`PoolCreated`) and indexes the child contract it announces
//! under a *template* ABI - the dynamic-data-sources capability, without which Uniswap-class protocols
//! are unindexable. This module is the foundation: the validated rule set ([`FactorySet`]) and the
//! discovered-child registry ([`ChildRegistry`]). Both are pure and deterministic - the registry state
//! at block B is a fold over factory events ≤ B, so it reproduces and its content hash is stable. The
//! ingestion regimes (tip, sequential/pipelined backfill, ExEx) that *consume* this land in later
//! steps; here we build the state they read and prove its reorg + determinism properties.

use crate::config::Config;
use crate::registry::{snake_case, DecodeRegistry, DecodedRow, Value};
use anyhow::{bail, Result};
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

impl FactorySet {
    /// [`FactorySet::build`], with each rule's announcing table taken from the decoders `registry`
    /// actually registered (#2158). An overloaded event's table carries a topic0 suffix, so the
    /// name alone is refused as ambiguous and the rule must give the full signature.
    pub fn resolved(config: &Config, registry: &DecodeRegistry) -> Result<Self> {
        Self::build_with(config, |f| announcing_table(f, registry))
    }

    /// Build + validate the factory rules from a nest config. Errors on a rule referencing an unknown
    /// `watch` alias/template or an unknown `template`, on a template naming an existing contract
    /// alias (ambiguous table namespace), or on nesting deeper than the depth-3 ceiling.
    pub fn build(config: &Config) -> Result<Self> {
        Self::build_with(config, |f| {
            Ok(format!("{}__{}", f.watch, snake_case(&f.event)))
        })
    }

    fn build_with(
        config: &Config,
        table_of: impl Fn(&crate::config::Factory) -> Result<String>,
    ) -> Result<Self> {
        let contract_aliases: HashSet<&str> =
            config.contracts.iter().map(|c| c.alias.as_str()).collect();
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

        let force_topic0 = config
            .templates
            .iter()
            .any(|t| t.filter.as_deref() == Some("topic0"));

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
fn announcing_table(f: &crate::config::Factory, registry: &DecodeRegistry) -> Result<String> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::Value;
    use proptest::prelude::*;

    /// Deterministic, unique child address for a pool discovered on `branch` at `(block, i)`. Distinct
    /// branches never collide; the same (branch, block, i) always yields the same address - so a fold
    /// over the same factory events reproduces the same registry.
    fn child_addr(branch: u8, block: u64, i: u64) -> String {
        let key: u128 = ((branch as u128) << 100) | ((block as u128) << 20) | i as u128;
        format!("0x{key:040x}")
    }

    fn discover_pools(reg: &mut ChildRegistry, branch: u8, block: u64, n: u64) {
        for i in 0..n {
            reg.insert(ChildEntry {
                template: "pool".into(),
                address: child_addr(branch, block, i),
                discovered_block: block,
                discovered_log_index: i,
                discovered_timestamp: 0,
                parent_address: "0xfactory".into(),
                depth: 1,
            });
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(96))]
        /// RFC-0009 step 4: the child registry converges under reorg. Discovering pools along a
        /// prefix chain, reorging at a fork point, then applying an alternate branch yields exactly
        /// the registry state (content hash) of building the winning chain directly - the same
        /// convergence property the hot store has, now for the discovered set.
        #[test]
        fn child_registry_reorg_converges(
            prefix in prop::collection::vec(0u64..3, 1..8),   // pools created per block, branch A
            branch in prop::collection::vec(0u64..3, 0..6),   // alternate branch B after the fork
            fork_back in 0usize..8,
        ) {
            // Reorged registry: apply the full prefix (A), roll back to the fork, apply branch (B).
            let mut reorged = ChildRegistry::new();
            for (b, &n) in prefix.iter().enumerate() {
                discover_pools(&mut reorged, 0, b as u64, n);
            }
            let fork = (prefix.len().saturating_sub(fork_back)).saturating_sub(1) as u64;
            reorged.rollback_to(fork);
            for (j, &n) in branch.iter().enumerate() {
                discover_pools(&mut reorged, 1, fork + 1 + j as u64, n);
            }

            // Canonical registry: prefix (A) up to the fork, then branch (B) - built directly.
            let mut canonical = ChildRegistry::new();
            for (b, &n) in prefix.iter().enumerate() {
                if (b as u64) <= fork {
                    discover_pools(&mut canonical, 0, b as u64, n);
                }
            }
            for (j, &n) in branch.iter().enumerate() {
                discover_pools(&mut canonical, 1, fork + 1 + j as u64, n);
            }

            prop_assert_eq!(reorged.hash(), canonical.hash());
        }
    }

    fn cfg(toml: &str) -> Config {
        toml::from_str(toml).unwrap()
    }

    const UNIV3: &str = r#"
[nest]
name = "univ3"
chain = "mainnet"
chain_id = 1
rpc_urls = ["https://rpc.example"]

[[contracts]]
alias = "factory"
address = "0x1f98431c8ad98523631ae4a59f267346ea31f984"
abi = "abis/factory.json"

[[templates]]
name = "pool"
abi = "abis/pool.json"

[[factories]]
watch = "factory"
event = "PoolCreated"
child_param = "pool"
template = "pool"
"#;

    fn pool_created_row(block: u64, log: u64, pool: &str) -> DecodedRow {
        let mut addr = [0u8; 20];
        let bytes = hex::decode(pool.trim_start_matches("0x")).unwrap();
        addr.copy_from_slice(&bytes);
        DecodedRow {
            table: "factory__pool_created".into(),
            params: vec![("pool".into(), Value::Address(addr))],
            block_number: block,
            block_hash: "0xbh".into(),
            block_timestamp: 1_700_000_000 + block,
            timestamps: true,
            log_index: log,
            tx_hash: "0xtx".into(),
            address: "0x1f98431c8ad98523631ae4a59f267346ea31f984".into(),
        }
    }

    /// #2158: an overloaded event's table carries a topic0 suffix, so a rule keyed by the bare name
    /// never matched a decoded row. The rule now resolves against the registry, and the bare name of
    /// an overloaded or unknown event is refused rather than discovering nothing.
    #[test]
    fn a_rule_on_an_overloaded_event_resolves_to_its_registered_table() {
        use crate::registry::{ContractSpec, TemplateSpec};
        let abi = |json: &str| serde_json::from_str(json).unwrap();
        let registry = DecodeRegistry::build_with_templates(
            vec![ContractSpec {
                alias: "factory".into(),
                address: "0x1f98431c8ad98523631ae4a59f267346ea31f984"
                    .parse()
                    .unwrap(),
                abi: abi(r#"[
                    {"type":"event","name":"PoolCreated","anonymous":false,"inputs":[
                        {"name":"pool","type":"address","indexed":false}]},
                    {"type":"event","name":"PoolCreated","anonymous":false,"inputs":[
                        {"name":"pool","type":"address","indexed":false},
                        {"name":"fee","type":"uint256","indexed":false}]}
                ]"#),
                events: Vec::new(),
            }],
            vec![TemplateSpec {
                name: "pool".into(),
                abi: abi(r#"[{"type":"event","name":"Swap","anonymous":false,"inputs":[]}]"#),
                events: Vec::new(),
            }],
        )
        .unwrap();
        let table = registry
            .tables()
            .into_iter()
            .find(|d| d.signature == "PoolCreated(address,uint256)")
            .unwrap()
            .table
            .clone();
        assert_ne!(table, "factory__pool_created");

        let err = |toml: &str| match FactorySet::resolved(&cfg(toml), &registry) {
            Err(e) => format!("{e:#}"),
            Ok(_) => String::new(),
        };
        assert!(
            err(UNIV3).contains("PoolCreated(address)"),
            "{}",
            err(UNIV3)
        );
        let unknown = UNIV3.replace(r#"event = "PoolCreated""#, r#"event = "Nope""#);
        assert!(err(&unknown).contains("Nope"), "{}", err(&unknown));

        let full = UNIV3.replace(
            r#"event = "PoolCreated""#,
            r#"event = "PoolCreated(address, uint256)""#,
        );
        let fs = FactorySet::resolved(&cfg(&full), &registry).unwrap();
        let pool = "0xaaaabbbbccccddddeeeeffff0000111122223333";
        let mut row = pool_created_row(100, 3, pool);
        row.table = table.clone();
        assert_eq!(fs.discover(&row).len(), 1);
        assert_eq!(fs.factory_tables(), vec![table]);
    }
    #[test]
    fn validates_references() {
        assert!(FactorySet::build(&cfg(UNIV3)).is_ok());

        // watch a non-existent alias.
        let bad = UNIV3.replace(r#"watch = "factory""#, r#"watch = "nope""#);
        assert!(FactorySet::build(&cfg(&bad)).is_err());

        // produce an undeclared template.
        let bad = UNIV3.replace(r#"template = "pool""#, r#"template = "ghost""#);
        assert!(FactorySet::build(&cfg(&bad)).is_err());

        // a template colliding with a contract alias.
        let bad = UNIV3.replace(r#"name = "pool""#, r#"name = "factory""#);
        assert!(FactorySet::build(&cfg(&bad)).is_err());
    }

    #[test]
    fn discovers_child_from_factory_event() {
        let fs = FactorySet::build(&cfg(UNIV3)).unwrap();
        let pool = "0xaaaabbbbccccddddeeeeffff0000111122223333";
        let child = fs.discover(&pool_created_row(100, 3, pool)).remove(0);
        assert_eq!(child.template, "pool");
        assert_eq!(child.address, pool);
        assert_eq!(child.discovered_block, 100);
        assert_eq!(child.discovered_log_index, 3);
        assert_eq!(child.depth, 1);
        assert_eq!(
            child.parent_address,
            "0x1f98431c8ad98523631ae4a59f267346ea31f984"
        );

        // A non-factory row discovers nothing.
        let mut other = pool_created_row(100, 3, pool);
        other.table = "factory__other_event".into();
        assert!(fs.discover(&other).is_empty());
    }

    /// #765: discovery now runs before the stamp, so a child is inserted with timestamp 0 and
    /// filled in afterwards. Filling must not look like a set change.
    #[test]
    fn apply_timestamps_fills_zero_without_bumping_version() {
        let fs = FactorySet::build(&cfg(UNIV3)).unwrap();
        let mut reg = ChildRegistry::new();
        let a = "0x00000000000000000000000000000000000000a1";
        let mut child = fs.discover(&pool_created_row(10, 0, a)).remove(0);
        child.discovered_timestamp = 0;
        assert!(reg.insert(child));
        assert_eq!(reg.version(), 1);
        let ts = HashMap::from([(10u64, 1_700_000_010u64), (99, 1)]);
        reg.apply_timestamps(&ts);
        assert_eq!(reg.get(a).unwrap().discovered_timestamp, 1_700_000_010);
        assert_eq!(reg.version(), 1, "the set of children is unchanged");
        reg.apply_timestamps(&HashMap::from([(10u64, 999u64)]));
        assert_eq!(
            reg.get(a).unwrap().discovered_timestamp,
            1_700_000_010,
            "an already-stamped child is left alone"
        );
    }

    #[test]
    fn registry_insert_dedup_and_version() {
        let fs = FactorySet::build(&cfg(UNIV3)).unwrap();
        let mut reg = ChildRegistry::new();
        let a = "0x00000000000000000000000000000000000000a1";
        let b = "0x00000000000000000000000000000000000000b2";

        assert!(reg.insert(fs.discover(&pool_created_row(10, 0, a)).remove(0)));
        assert_eq!(reg.version(), 1);
        // Re-discovering the same child is idempotent - no version bump.
        assert!(!reg.insert(fs.discover(&pool_created_row(11, 0, a)).remove(0)));
        assert_eq!(reg.version(), 1);
        assert!(reg.insert(fs.discover(&pool_created_row(12, 0, b)).remove(0)));
        assert_eq!(reg.version(), 2);
        assert_eq!(reg.len(), 2);
        assert!(reg.contains(a));
        assert_eq!(reg.template_of(b), Some("pool"));
        assert_eq!(reg.addresses_for_template("pool").len(), 2);
    }

    #[test]
    fn rollback_removes_children_above_block_and_is_deterministic() {
        let fs = FactorySet::build(&cfg(UNIV3)).unwrap();
        let a = "0x00000000000000000000000000000000000000a1";
        let b = "0x00000000000000000000000000000000000000b2";

        // Registry built by folding two discoveries (blocks 10, 20), then reorg to block 15.
        let mut reorged = ChildRegistry::new();
        reorged.insert(fs.discover(&pool_created_row(10, 0, a)).remove(0));
        reorged.insert(fs.discover(&pool_created_row(20, 0, b)).remove(0));
        assert_eq!(reorged.rollback_to(15), 1); // child b (block 20) dropped
        assert!(reorged.contains(a) && !reorged.contains(b));

        // A registry built canonically (only the surviving discovery) has the identical content hash
        // - reorg convergence: registry state at B is a pure fold over factory events ≤ B.
        let mut canonical = ChildRegistry::new();
        canonical.insert(fs.discover(&pool_created_row(10, 0, a)).remove(0));
        assert_eq!(reorged.hash(), canonical.hash());
    }

    #[test]
    fn nested_factory_depth() {
        // factory → pool (depth 1), pool → position (depth 2).
        let nested = format!(
            "{UNIV3}\n[[templates]]\nname = \"position\"\nabi = \"abis/pos.json\"\n\n\
             [[factories]]\nwatch = \"pool\"\nevent = \"PositionOpened\"\nchild_param = \"pos\"\ntemplate = \"position\"\n"
        );
        let fs = FactorySet::build(&cfg(&nested)).unwrap();
        // A pool-emitted PositionOpened discovers a depth-2 child.
        let mut addr = [0u8; 20];
        addr[19] = 9;
        let row = DecodedRow {
            table: "pool__position_opened".into(),
            params: vec![("pos".into(), Value::Address(addr))],
            block_number: 50,
            block_hash: "0xbh".into(),
            block_timestamp: 1,
            timestamps: true,
            log_index: 0,
            tx_hash: "0xtx".into(),
            address: "0xpool".into(),
        };
        assert_eq!(fs.discover(&row).remove(0).depth, 2);
    }

    /// **Issue #241 item 1.** One event announcing several children must produce **all** of them.
    ///
    /// This is not a hypothetical shape: `OrgDeployed` on the POA stack carries ten module addresses
    /// in ten named params, and ten factory rules on one event is the obvious encoding. Before this,
    /// `rules` was a `HashMap` keyed by `{watch}__{event}`, so all ten collapsed to **one** - last
    /// declaration wins, ordering-dependent - while startup still logged the full rule count. Every
    /// other template got tables and a `__children` view and stayed empty forever, with no warning.
    const MULTI: &str = r#"
[nest]
name = "poa"
chain = "arbitrum-one"
chain_id = 42161
rpc_urls = ["https://rpc.example"]

[[contracts]]
alias = "org_registry"
address = "0x1f98431c8ad98523631ae4a59f267346ea31f984"
abi = "abis/registry.json"

[[templates]]
name = "switchable_beacon"
abi = "abis/beacon.json"

[[templates]]
name = "module_proxy"
abi = "abis/proxy.json"

[[factories]]
watch = "org_registry"
event = "ContractRegistered"
child_param = "beacon"
template = "switchable_beacon"

[[factories]]
watch = "org_registry"
event = "ContractRegistered"
child_param = "proxy"
template = "module_proxy"
"#;

    fn registered_row(block: u64, beacon: &str, proxy: &str) -> DecodedRow {
        DecodedRow {
            table: "org_registry__contract_registered".into(),
            params: vec![
                (
                    "beacon".into(),
                    Value::Address(
                        hex::decode(beacon.trim_start_matches("0x"))
                            .unwrap()
                            .try_into()
                            .unwrap(),
                    ),
                ),
                (
                    "proxy".into(),
                    Value::Address(
                        hex::decode(proxy.trim_start_matches("0x"))
                            .unwrap()
                            .try_into()
                            .unwrap(),
                    ),
                ),
            ],
            block_number: block,
            block_hash: "0xbb".into(),
            block_timestamp: 1,
            timestamps: true,
            log_index: 0,
            tx_hash: "0xtt".into(),
            address: "0x1f98431c8ad98523631ae4a59f267346ea31f984".into(),
        }
    }

    #[test]
    fn one_event_can_announce_several_children() {
        let fs = FactorySet::build(&cfg(MULTI)).unwrap();
        let beacon = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let proxy = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

        let mut found = fs.discover(&registered_row(100, beacon, proxy));
        found.sort_by(|a, b| a.template.cmp(&b.template));
        assert_eq!(
            found.len(),
            2,
            "both rules on this event must fire - got {found:?}"
        );
        assert_eq!(found[0].template, "module_proxy");
        assert_eq!(found[0].address, proxy);
        assert_eq!(found[1].template, "switchable_beacon");
        assert_eq!(found[1].address, beacon);

        // The `{template}__children` views must have a source per rule, or the second template's view
        // is created and stays empty - which is how this bug presented.
        let mut srcs: Vec<String> = fs.view_sources().into_iter().map(|(t, _, _)| t).collect();
        srcs.sort();
        assert_eq!(srcs, vec!["module_proxy", "switchable_beacon"]);
    }

    /// The stored-row path (warm restart) must agree with the live path, or a restart silently loses
    /// the children it discovered before.
    #[test]
    fn the_warm_restart_path_also_finds_every_child() {
        let fs = FactorySet::build(&cfg(MULTI)).unwrap();
        let row = serde_json::json!({
            "block_number": 100,
            "log_index": 0,
            "address": "0x1f98431c8ad98523631ae4a59f267346ea31f984",
            "beacon": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "proxy": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        });
        let found = fs.discover_stored("org_registry__contract_registered", &row);
        assert_eq!(found.len(), 2, "warm restart must match live discovery");
    }

    /// A rule whose param is missing must not suppress its siblings - each rule stands alone.
    #[test]
    fn a_rule_whose_param_is_absent_does_not_block_the_others() {
        let fs = FactorySet::build(&cfg(MULTI)).unwrap();
        let row = serde_json::json!({
            "block_number": 100,
            "log_index": 0,
            "address": "0x1f98431c8ad98523631ae4a59f267346ea31f984",
            "proxy": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        });
        let found = fs.discover_stored("org_registry__contract_registered", &row);
        assert_eq!(found.len(), 1, "the rule that *can* fire still fires");
        assert_eq!(found[0].template, "module_proxy");
    }

    /// A genuinely identical rule is a copy-paste error, and indexing it twice would double every
    /// discovery. Refused, loudly - which is the one case where the old silent-overwrite was benign
    /// and is now the only case that errors.
    #[test]
    fn an_identical_duplicate_rule_is_refused() {
        let dup = MULTI.replace(
            r#"child_param = "proxy"
template = "module_proxy""#,
            r#"child_param = "beacon"
template = "switchable_beacon""#,
        );
        let err = FactorySet::build(&cfg(&dup)).unwrap_err().to_string();
        assert!(
            err.contains("identical factory rules"),
            "must name the problem: {err}"
        );
    }
}
