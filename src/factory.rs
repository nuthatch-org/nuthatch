//! Factory patterns and the dynamic child registry (RFC-0009, step 1).
//!
//! The rules and the registry are `nuthatch_core::factory` (RFC-0075 §6); what stays here is reading
//! them out of a nest's config.

use crate::config::Config;
use nuthatch_core::factory::Declared;

pub use nuthatch_core::factory::{ChildEntry, ChildRegistry, FactorySet};

impl From<&Config> for Declared {
    fn from(config: &Config) -> Self {
        nuthatch_core::nest_config::declared(
            &config.contracts,
            &config.templates,
            &config.factories,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{DecodeRegistry, DecodedRow, Value};
    use proptest::prelude::*;
    use std::collections::HashMap;

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
