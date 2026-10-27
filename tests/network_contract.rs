//! Source-anchored fixture replay, not a claim of live parity or production readiness.
use nuthatch::{analytics, config::Config, registry};
use serde_json::{json, Value};
use std::{path::PathBuf, time::Duration};

// Whole-history fixture correctness is not the production request-latency contract.
// Serialized replays still exceeded five seconds on CI. Keep a bounded test-only
// allowance; production guards and their deadline tests remain unchanged.
const REPLAY_QUERY_TIMEOUT: Duration = Duration::from_secs(30);

// These are whole-view correctness replays, not a concurrent-load benchmark. Keep their
// independent sessions from competing for the runner while a query deadline runs.
fn replay_slot() -> std::sync::MutexGuard<'static, ()> {
    static SLOT: std::sync::Mutex<()> = std::sync::Mutex::new(());
    SLOT.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[test]
fn sparse_delegation_fold_matches_the_original_event_by_event() {
    let _replay = replay_slot();
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("views")).unwrap();
    let mut rows = Vec::new();
    for indexer in ["a", "b", "no-rewards"] {
        for seq in 1..=200 {
            let reward_event = indexer != "no-rewards" && seq % 7 == 1;
            let reward = if reward_event {
                "100000000000000000000000000000000000"
            } else {
                "0"
            };
            let delta = if reward_event {
                "0"
            } else if seq % 3 == 0 {
                "-100000000000000000000000000000000000"
            } else {
                "100000000000000000000000000000000000"
            };
            rows.push(format!("('{indexer}',{},{},{},CAST('{delta}' AS BIGNUM),'{reward}',CAST({} AS BIGNUM),CAST({} AS BIGNUM),{seq},'{}')", seq / 4, seq % 4, seq + 100, seq % 9, seq % 5, (seq % 3) * 500000));
        }
    }
    std::fs::write(dir.path().join("views/00-ordered.sql"), format!("CREATE VIEW delegation_ordered AS SELECT * FROM (VALUES {}) AS v(indexer,block_number,log_index,block_timestamp,delta,reward,shares,thawing,seq,cut);",rows.join(","))).unwrap();
    let sql = include_str!("../tests/fixtures/network-nest/views/41-delegation.sql");
    let fold = &sql[sql.find("CREATE VIEW delegation_ledger AS").unwrap()
        ..sql.find("CREATE VIEW indexer_delegation AS").unwrap()];
    std::fs::write(dir.path().join("views/10-fold.sql"), fold).unwrap();
    std::fs::write(
        dir.path().join("views/20-reference.sql"),
        include_str!("fixtures/network-clients/delegation-ledger-reference.sql"),
    )
    .unwrap();
    let result = analytics::query_hot_cold(dir.path(), "SELECT count(*) AS differing FROM ((SELECT * FROM delegation_ledger EXCEPT ALL SELECT * FROM delegation_ledger_reference) UNION ALL (SELECT * FROM delegation_ledger_reference EXCEPT ALL SELECT * FROM delegation_ledger))", analytics::QueryGuard { timeout: REPLAY_QUERY_TIMEOUT, max_rows: 1, ..Default::default() }, &analytics::HotRows::new(), 0, &[]).unwrap();
    assert!(!result.degraded(), "{result:?}");
    assert_eq!(result.rows, vec![json!({"differing":0})]);
    let count = analytics::query_hot_cold(
        dir.path(),
        "SELECT count(*) AS rows FROM delegation_ledger",
        analytics::QueryGuard {
            timeout: REPLAY_QUERY_TIMEOUT,
            max_rows: 1,
            ..Default::default()
        },
        &analytics::HotRows::new(),
        0,
        &[],
    )
    .unwrap();
    assert_eq!(count.rows, vec![json!({"rows":600})]);
}

#[test]
fn sparse_lock_fold_matches_the_original_event_by_event() {
    let _replay = replay_slot();
    let kinds = [
        "deposit",
        "allocate",
        "close",
        "legacy_lock",
        "legacy_withdraw",
        "lock",
        "withdraw",
        "slash",
        "provision_slash",
    ];
    let mut rows = Vec::new();
    for indexer in ["a", "b"] {
        for seq in 0..300 {
            let kind = if seq % 3 == 0 {
                kinds[(seq / 3) % kinds.len()]
            } else {
                "allocate"
            };
            let amount = if seq % 7 == 0 {
                "0".to_owned()
            } else {
                format!("{}000000000000000000000000000000", seq % 19 + 1)
            };
            rows.push(format!(
                "('{indexer}',CAST({} AS UBIGINT),CAST({} AS UBIGINT),'{kind}','{amount}',{})",
                seq / 4,
                seq % 4,
                seq + 100
            ));
        }
    }
    let sql = include_str!("../tests/fixtures/network-nest/views/45-indexer.sql");
    let fold = &sql[sql.find("CREATE VIEW indexer_lock_ledger AS").unwrap()
        ..sql.find("CREATE VIEW indexer_lock_state AS").unwrap()];
    // The fold over these events, and over none.
    let nest = |events: String| {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("views")).unwrap();
        std::fs::write(dir.path().join("views/00-events.sql"), events).unwrap();
        std::fs::write(dir.path().join("views/10-fold.sql"), fold).unwrap();
        std::fs::write(
            dir.path().join("views/20-reference.sql"),
            include_str!("fixtures/network-clients/indexer-lock-ledger-reference.sql"),
        )
        .unwrap();
        dir
    };
    let one = |dir: &std::path::Path, sql: &str| {
        let out = analytics::query_hot_cold(
            dir,
            sql,
            analytics::QueryGuard {
                timeout: REPLAY_QUERY_TIMEOUT,
                max_rows: 1,
                ..Default::default()
            },
            &analytics::HotRows::new(),
            0,
            &[],
        )
        .unwrap();
        assert!(!out.degraded(), "{out:?}");
        out.rows
    };
    const COLUMNS: &str = "v(indexer,block_number,log_index,kind,tokens,until)";
    let full = nest(format!(
        "CREATE VIEW indexer_lock_event AS SELECT * FROM (VALUES {}) AS {COLUMNS};",
        rows.join(",")
    ));
    assert_eq!(
        one(full.path(), "SELECT count(*) AS differing FROM ((SELECT * FROM indexer_lock_ledger EXCEPT ALL SELECT * FROM indexer_lock_ledger_reference) UNION ALL (SELECT * FROM indexer_lock_ledger_reference EXCEPT ALL SELECT * FROM indexer_lock_ledger))"),
        vec![json!({"differing":0})],
        "every intermediate stake, allocation and lock state must match"
    );
    assert_eq!(
        one(
            full.path(),
            "SELECT count(*) AS rows FROM indexer_lock_ledger"
        ),
        vec![json!({"rows":600})]
    );
    let empty = nest(format!(
        "CREATE VIEW indexer_lock_event AS SELECT * FROM (VALUES {}) AS {COLUMNS} WHERE false;",
        rows[0]
    ));
    assert_eq!(
        one(
            empty.path(),
            "SELECT count(*) AS rows FROM indexer_lock_ledger"
        ),
        vec![json!({"rows":0})]
    );
}

#[test]
fn network_startup_validator_binds_the_same_scalar_functions_as_queries() {
    let _replay = replay_slot();
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/network-nest");
    let config = Config::load(&source).unwrap();
    let mut schema = registry::from_nest(&source, &config).unwrap().schema();
    schema.extend(nuthatch::calls::schema(&config.calls, true));
    let issues = analytics::validate_nest_views(&source, &schema);
    assert!(issues.is_empty(), "{issues:#?}");
}

#[test]
fn allocation_clock_only_defaults_empty_or_reverted_reads() {
    let _replay = replay_slot();
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/network-nest");
    let config = Config::load(&source).unwrap();
    let mut schema = registry::from_nest(&source, &config).unwrap().schema();
    schema.extend(nuthatch::calls::schema(&config.calls, true));
    let mut hot = analytics::HotRows::from([(
        "epoch_manager_l1_block".into(),
        vec![
            json!({"block_number":1,"block_hash":"h1","result":"0x","reverted":false}),
            json!({"block_number":2,"block_hash":"h2","result":"0x","reverted":true}),
            json!({"block_number":3,"block_hash":"h3","result":format!("0x{:064x}",123),"reverted":false}),
        ],
    )]);
    let query = |hot: &analytics::HotRows| {
        analytics::query_hot_cold(
            &source,
            "SELECT block_number, l1_block FROM allocation_l1_clock ORDER BY block_number",
            analytics::QueryGuard {
                timeout: REPLAY_QUERY_TIMEOUT,
                max_rows: 10,
                ..Default::default()
            },
            hot,
            0,
            &schema,
        )
    };
    let result = query(&hot).unwrap();
    assert!(!result.degraded(), "{result:?}");
    assert_eq!(
        result.rows,
        vec![
            json!({"block_number":1,"l1_block":0}),
            json!({"block_number":2,"l1_block":0}),
            json!({"block_number":3,"l1_block":123}),
        ]
    );
    hot.get_mut("epoch_manager_l1_block").unwrap()[2]["result"] = json!("0x01");
    assert!(
        query(&hot).is_err(),
        "malformed successful reads must not become zero"
    );
}

#[test]
#[ignore = "operator check: requires NETWORK_COLD_REPLAY_DIR holding independently ingested genesis segments"]
fn independently_ingested_cold_history_matches_genesis_reference() {
    let _replay = replay_slot();
    let replay = PathBuf::from(std::env::var_os("NETWORK_COLD_REPLAY_DIR").expect(
        "set NETWORK_COLD_REPLAY_DIR to the retained first-million-block ingestion directory",
    ));
    assert!(replay.join("segments/manifest.json").is_file());
    let manifest: Value =
        serde_json::from_slice(&std::fs::read(replay.join("segments/manifest.json")).unwrap())
            .unwrap();
    let replay_through = manifest["tables"]
        .as_object()
        .unwrap()
        .values()
        .flat_map(|parts| parts.as_array().unwrap())
        .map(|part| part["to_block"].as_u64().unwrap())
        .max()
        .unwrap();
    assert!(
        replay_through >= 43_440_000,
        "capture must cover the first million blocks"
    );
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/network-nest");
    let config = Config::load(&source).unwrap();
    let mut schema = registry::from_nest(&source, &config).unwrap().schema();
    schema.extend(nuthatch::calls::schema(&config.calls, true));
    // Copy into a disposable read fixture; never manufacture a hot store or edit
    // the operator's retained segment directory to make it look like a live nest.
    let dir = tempfile::tempdir().unwrap();
    for (name, input) in [
        ("views", source.join("views")),
        ("segments", replay.join("segments")),
    ] {
        std::fs::create_dir(dir.path().join(name)).unwrap();
        for entry in std::fs::read_dir(input).unwrap() {
            let entry = entry.unwrap();
            assert!(entry.file_type().unwrap().is_file());
            std::fs::copy(entry.path(), dir.path().join(name).join(entry.file_name())).unwrap();
        }
    }
    for capture in [
        include_str!("../tests/fixtures/network-nest/validation/genesis-parity.json"),
        include_str!("../tests/fixtures/network-nest/validation/genesis-million-reference.json"),
        include_str!("../tests/fixtures/network-nest/validation/backfill-84m-reference.json"),
        include_str!("../tests/fixtures/network-nest/validation/backfill-123m-reference.json"),
        include_str!(
            "../tests/fixtures/network-nest/validation/backfill-123m-indexers-reference.json"
        ),
        include_str!("../tests/fixtures/network-nest/validation/backfill-129m-reference.json"),
        include_str!(
            "../tests/fixtures/network-nest/validation/backfill-129m-deployments-reference.json"
        ),
    ] {
        let evidence: Value = serde_json::from_str(capture).unwrap();
        if evidence["block"].as_u64().unwrap() > replay_through {
            continue;
        }
        for (root, table) in [
            ("graphNetwork", "graph_network"),
            ("epoches", "epoch"),
            ("indexers", "indexer"),
            ("subgraphDeployments", "subgraph_deployment"),
        ] {
            if evidence["reference"].get(root).is_none() {
                continue;
            }
            eprintln!("cold parity: block {} root {root}", evidence["block"]);
            let expected = &evidence["reference"][root];
            let first = if root != "graphNetwork" {
                &expected[0]
            } else {
                expected
            };
            let fields = first
                .as_object()
                .unwrap()
                .keys()
                .map(|key| format!("\"{key}\""))
                .collect::<Vec<_>>()
                .join(", ");
            let started = std::time::Instant::now();
            let result = analytics::query_hot_cold_at(
                dir.path(),
                &format!("SELECT {fields} FROM {table} ORDER BY id"),
                analytics::QueryGuard {
                    timeout: REPLAY_QUERY_TIMEOUT,
                    max_rows: 1000,
                    ..Default::default()
                },
                &analytics::HotRows::new(),
                u64::MAX,
                &schema,
                evidence["block"].as_u64().unwrap(),
            )
            .unwrap_or_else(|error| panic!("cold {root} at {}: {error:#}", evidence["block"]));
            eprintln!("cold {root} completed in {:?}", started.elapsed());
            assert!(!result.degraded(), "{root}: {result:?}");
            assert!(!result.truncated, "{root}: {result:?}");
            let actual = if root != "graphNetwork" {
                json!(result.rows)
            } else {
                assert_eq!(result.rows.len(), 1);
                result.rows[0].clone()
            };
            if let Some(rows) = expected.as_array() {
                let actual_rows = actual.as_array().unwrap();
                assert_eq!(actual_rows.len(), rows.len(), "cold {root} row count");
                for (actual, expected) in actual_rows.iter().zip(rows) {
                    for (field, value) in expected.as_object().unwrap() {
                        assert_eq!(
                            &actual[field], value,
                            "cold {root} id={} field={field}",
                            expected["id"]
                        );
                    }
                }
            } else {
                assert_eq!(&actual, expected, "cold {root}");
            }
        }
    }
    let clocks = analytics::query_hot_cold(
        dir.path(),
        "SELECT count(*) AS clocks FROM allocation_l1_clock WHERE l1_block >= 0",
        analytics::QueryGuard {
            timeout: REPLAY_QUERY_TIMEOUT,
            max_rows: 1,
            ..Default::default()
        },
        &analytics::HotRows::new(),
        u64::MAX,
        &schema,
    )
    .unwrap();
    assert!(!clocks.degraded(), "{clocks:?}");
}

#[test]
fn capacity_uses_the_last_refresh_event_and_its_protocol_parameters() {
    let _replay = replay_slot();
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/network-nest");
    let config = Config::load(&source).unwrap();
    let mut schema = registry::from_nest(&source, &config).unwrap().schema();
    schema.extend(nuthatch::calls::schema(&config.calls, true));
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("views")).unwrap();
    for entry in std::fs::read_dir(source.join("views")).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(
            entry.path(),
            dir.path().join("views").join(entry.file_name()),
        )
        .unwrap();
    }
    let mut hot = analytics::HotRows::new();
    let mut event = |table: &str, block: u64, fields: Value| {
        let mut row = json!({"block_number":block,"block_hash":format!("hash{block}"),
            "log_index":1,"block_timestamp":1000+block,"indexer":"i","serviceProvider":"i",
            "verifier":"0xb2bb92d0de618878e438b55d5846cfecd9301105"});
        row.as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        hot.entry(table.into()).or_default().push(row);
    };
    event(
        "staking_legacy__stake_deposited",
        1,
        json!({"tokens":"100"}),
    );
    event(
        "staking_legacy__stake_delegated",
        2,
        json!({"tokens":"300","shares":"30"}),
    );
    event(
        "staking_parameters__parameter_updated",
        3,
        json!({"param":"delegationRatio"}),
    );
    event(
        "delegation_ratio_read",
        3,
        json!({"result":format!("0x{:064x}",2),"reverted":false}),
    );
    event(
        "horizon_staking__horizon_stake_deposited",
        4,
        json!({"tokens":"50"}),
    );
    event(
        "staking_legacy__stake_locked",
        5,
        json!({"tokens":"10","until":100}),
    );
    event(
        "horizon_staking__max_thawing_period_set",
        6,
        json!({"maxThawingPeriod":"10"}),
    );
    event(
        "horizon_staking__provision_created",
        7,
        json!({"tokens":"40","maxVerifierCut":"0","thawingPeriod":"0"}),
    );
    event(
        "subgraph_service__allocation_created",
        8,
        json!({"tokens":"20","allocationId":"a"}),
    );
    event(
        "staking_legacy__allocation_created",
        9,
        json!({"tokens":"10","allocationID":"b"}),
    );
    event(
        "horizon_staking__provision_thawed",
        10,
        json!({"tokens":"5"}),
    );
    event(
        "horizon_staking__tokens_undelegated",
        11,
        json!({"tokens":"50","shares":"5"}),
    );
    event(
        "horizon_staking__provision_slashed",
        12,
        json!({"tokens":"10"}),
    );
    let query = |block| {
        let result = analytics::query_hot_cold_at(
            dir.path(),
            "SELECT \"delegatedCapacity\", \"tokenCapacity\", \"availableStake\" FROM indexer",
            analytics::QueryGuard {
                timeout: REPLAY_QUERY_TIMEOUT,
                max_rows: 10,
                ..Default::default()
            },
            &hot,
            0,
            &schema,
            block,
        )
        .unwrap();
        assert!(!result.degraded(), "{result:?}");
        result.rows[0].clone()
    };
    for (block, delegated, capacity, available) in [
        (1, "0", "100", "100"),
        (2, "0", "100", "100"),
        (3, "0", "100", "100"),
        (4, "0", "100", "100"),
        (5, "300", "450", "440"),
        (6, "300", "450", "440"),
        (7, "80", "120", "120"),
        (8, "80", "120", "100"),
        (9, "80", "120", "100"),
        (10, "80", "115", "95"),
        (11, "80", "115", "95"),
        (12, "60", "85", "65"),
    ] {
        assert_eq!(
            query(block),
            json!({"delegatedCapacity":delegated,
            "tokenCapacity":capacity,"availableStake":available}),
            "block {block}"
        );
    }
}

#[test]
fn independently_indexed_allocations_match_reference_including_arbitrum_l1_numbers() {
    let _replay = replay_slot();
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/network-nest");
    let config = Config::load(&source).unwrap();
    let mut schema = registry::from_nest(&source, &config).unwrap().schema();
    schema.extend(nuthatch::calls::schema(&config.calls, true));
    let facts: Value = serde_json::from_str(include_str!(
        "fixtures/network-clients/legacy-allocation-facts.json"
    ))
    .unwrap();
    let reference: Value = serde_json::from_str(include_str!(
        "../tests/fixtures/network-nest/validation/legacy-allocations-reference.json"
    ))
    .unwrap();
    let mut hot: analytics::HotRows = serde_json::from_value(facts["tables"].clone()).unwrap();
    assert_eq!(hot.values().map(Vec::len).sum::<usize>(), 12);
    assert_eq!(facts["block"], reference["block"]);
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("views")).unwrap();
    std::fs::copy(
        source.join("views/20-allocations.sql"),
        dir.path().join("views/allocations.sql"),
    )
    .unwrap();
    let query = |hot: &analytics::HotRows, sql: &str| {
        analytics::query_hot_cold_at(
            dir.path(),
            sql,
            analytics::QueryGuard {
                timeout: REPLAY_QUERY_TIMEOUT,
                max_rows: 100,
                ..Default::default()
            },
            hot,
            0,
            &schema,
            200_000_000,
        )
    };
    for (root, status) in [("active", "Active"), ("closed", "Closed")] {
        let expected = &reference["response"]["data"][root];
        let fields = expected[0]
            .as_object()
            .unwrap()
            .keys()
            .map(|key| format!("\"{key}\""))
            .collect::<Vec<_>>()
            .join(", ");
        let result = query(
            &hot,
            &format!(
                "SELECT {fields} FROM allocation_lifecycle WHERE status = '{status}' ORDER BY id"
            ),
        )
        .unwrap();
        assert!(!result.degraded(), "{result:?}");
        assert_eq!(result.rows.len(), 2);
        assert_eq!(json!(result.rows), *expected, "{status}");
    }
    hot.get_mut("epoch_manager_l1_block").unwrap()[0]["block_hash"] = json!("wrong-fork");
    let error = query(
        &hot,
        "SELECT \"createdAtBlockNumber\" FROM allocation_lifecycle ORDER BY id",
    )
    .unwrap_err();
    assert!(format!("{error:#}").contains("missing pinned EpochManager.blockNum"));
}

#[test]
fn captured_genesis_facts_match_same_block_network_subgraph() {
    let _replay = replay_slot();
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/network-nest");
    let config = Config::load(&source).unwrap();
    let mut schema = registry::from_nest(&source, &config).unwrap().schema();
    schema.extend(nuthatch::calls::schema(&config.calls, true));
    let facts: Value =
        serde_json::from_str(include_str!("fixtures/network-clients/genesis-facts.json")).unwrap();
    let evidence: Value = serde_json::from_str(include_str!(
        "../tests/fixtures/network-nest/validation/genesis-parity.json"
    ))
    .unwrap();
    let hot: analytics::HotRows = serde_json::from_value(facts["tables"].clone()).unwrap();
    assert_eq!(hot.values().map(Vec::len).sum::<usize>(), 76);
    assert_eq!(facts["toBlock"], evidence["block"]);
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("views")).unwrap();
    for entry in std::fs::read_dir(source.join("views")).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(
            entry.path(),
            dir.path().join("views").join(entry.file_name()),
        )
        .unwrap();
    }
    for (root, table) in [("graphNetwork", "graph_network"), ("epoches", "epoch")] {
        let expected = &evidence["reference"][root];
        let first = if root == "epoches" {
            &expected[0]
        } else {
            expected
        };
        let fields = first
            .as_object()
            .unwrap()
            .keys()
            .map(|key| format!("\"{key}\""))
            .collect::<Vec<_>>()
            .join(", ");
        let result = analytics::query_hot_cold_at(
            dir.path(),
            &format!("SELECT {fields} FROM {table} ORDER BY id"),
            analytics::QueryGuard {
                timeout: REPLAY_QUERY_TIMEOUT,
                max_rows: 1000,
                ..Default::default()
            },
            &hot,
            0,
            &schema,
            evidence["block"].as_u64().unwrap(),
        )
        .unwrap();
        assert!(!result.degraded(), "{root}: {result:?}");
        let actual = if root == "epoches" {
            json!(result.rows)
        } else {
            assert_eq!(result.rows.len(), 1);
            result.rows[0].clone()
        };
        assert_eq!(&actual, expected, "{root}");
    }
    let controls: Value = serde_json::from_str(include_str!(
        "../tests/fixtures/network-nest/validation/genesis-controls.json"
    ))
    .unwrap();
    let mut hot = hot;
    hot.insert("governor_read".into(), vec![controls["call"].clone()]);
    let control_query = |hot: &analytics::HotRows| {
        analytics::query_hot_cold_at(
            dir.path(),
            "SELECT id, controller, governor, \"pauseGuardian\" FROM graph_network",
            analytics::QueryGuard {
                timeout: REPLAY_QUERY_TIMEOUT,
                max_rows: 10,
                ..Default::default()
            },
            hot,
            0,
            &schema,
            42_460_000,
        )
    };
    let actual = control_query(&hot).unwrap();
    assert!(!actual.degraded(), "{actual:?}");
    assert_eq!(actual.rows, vec![controls["reference"].clone()]);
    hot.get_mut("governor_read").unwrap()[0]["block_hash"] = json!("wrong-fork");
    let error = format!("{:#}", control_query(&hot).unwrap_err());
    assert!(
        error.contains("missing pinned Controller.getGovernor"),
        "{error}"
    );
}

#[test]
fn network_contract_addresses_and_supply_follow_historical_events() {
    let _replay = replay_slot();
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/network-nest");
    let config = Config::load(&source).unwrap();
    let mut schema = registry::from_nest(&source, &config).unwrap().schema();
    schema.extend(nuthatch::calls::schema(&config.calls, true));
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("views")).unwrap();
    for entry in std::fs::read_dir(source.join("views")).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(
            entry.path(),
            dir.path().join("views").join(entry.file_name()),
        )
        .unwrap();
    }
    let zero = "0x0000000000000000000000000000000000000000";
    let replacement = "0x1111111111111111111111111111111111111111";
    let mut hot = analytics::HotRows::new();
    hot.insert("graph_token__transfer".into(), vec![
        json!({"block_number":1,"block_hash":"h1","log_index":0,"from":zero,"to":"owner","value":"1000000000000000000000000000000"}),
        json!({"block_number":2,"block_hash":"h2","log_index":0,"from":"owner","to":zero,"value":"7"}),
        json!({"block_number":2,"block_hash":"h2","log_index":1,"from":zero,"to":zero,"value":"999"}),
    ]);
    hot.insert("controller__set_contract_proxy".into(), vec![json!({"block_number":2,"block_hash":"h2","log_index":2,
        "id":"0x1df41cd916959d1163dc8f0671a666ea8a3e434c13e40faef527133b5d167034","contractAddress":replacement})]);
    // This pre-epoch fixture still needs the pinned reads its refresh events
    // declare. Do not rely on projection pruning to hide absent source facts.
    hot.insert(
        "epoch_manager_l1_block".into(),
        vec![
            json!({"block_number":1,"block_hash":"h1","result":"0x","reverted":false}),
            json!({"block_number":2,"block_hash":"h2","result":"0x","reverted":false}),
        ],
    );
    let query = |block| {
        analytics::query_hot_cold_at(dir.path(),
        "SELECT staking, \"totalSupply\", \"totalGRTMinted\", \"totalGRTBurned\" FROM graph_network",
        analytics::QueryGuard { timeout: REPLAY_QUERY_TIMEOUT, max_rows: 100,     ..Default::default() }, &hot, 0, &schema, block).unwrap().rows
    };
    assert!(query(0).is_empty());
    assert_eq!(
        query(1)[0]["staking"],
        "0x00669a4cf01450b64e8a2a20e9b1fcb71e61ef03"
    );
    assert_eq!(
        query(2),
        vec![
            json!({"staking":replacement,"totalSupply":"999999999999999999999999999993",
        "totalGRTMinted":"1000000000000000000000000000000","totalGRTBurned":"7"})
        ]
    );
    assert_eq!(
        query(1)[0]["totalSupply"],
        "1000000000000000000000000000000"
    );
}

#[test]
fn network_clock_refreshes_on_token_events_but_not_unrelated_logs() {
    let _replay = replay_slot();
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/network-nest");
    let config = Config::load(&source).unwrap();
    let mut schema = registry::from_nest(&source, &config).unwrap().schema();
    schema.extend(nuthatch::calls::schema(&config.calls, true));
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("views")).unwrap();
    for entry in std::fs::read_dir(source.join("views")).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(
            entry.path(),
            dir.path().join("views").join(entry.file_name()),
        )
        .unwrap();
    }
    let mut hot = analytics::HotRows::new();
    hot.insert("epoch_manager__epoch_length_update".into(), vec![
        json!({"block_number":1000,"block_hash":"a","log_index":0,"epoch":"1","epochLength":"10"}),
        // Shortening at L1 108 does not advance epoch in the update handler itself.
        json!({"block_number":2000,"block_hash":"b","log_index":0,"epoch":"1","epochLength":"5"}),
    ]);
    hot.insert("epoch_manager_l1_block".into(), vec![
        json!({"block_number":1000,"block_hash":"a","result":format!("0x{:064x}",100),"reverted":false}),
        json!({"block_number":2000,"block_hash":"b","result":format!("0x{:064x}",108),"reverted":false}),
        json!({"block_number":3000,"block_hash":"c","result":format!("0x{:064x}",121),"reverted":false}),
    ]);
    hot.insert("graph_token__approval".into(), vec![
        json!({"block_number":3000,"block_hash":"c","log_index":0,"owner":"a","spender":"b","value":"1"}),
    ]);
    hot.insert("horizon_staking__horizon_stake_deposited".into(), vec![
        json!({"block_number":3000,"block_hash":"c","log_index":1,"serviceProvider":"a","tokens":"123456789012345678901234567890"}),
    ]);
    // No RPC refresh: this uses the stored L1 108 and creates epoch 2 after shortening.
    hot.insert("curation__signalled".into(), vec![
        json!({"block_number":2001,"block_hash":"d","log_index":0,"curator":"a","subgraphDeploymentID":"b","tokens":"1","signal":"1","curationTax":"0"}),
    ]);
    let query = |hot: &analytics::HotRows, block| {
        analytics::query_hot_cold_at(
            dir.path(),
            "SELECT * FROM epoch_bounds ORDER BY epoch",
            analytics::QueryGuard {
                timeout: REPLAY_QUERY_TIMEOUT,
                max_rows: 100,
                ..Default::default()
            },
            hot,
            0,
            &schema,
            block,
        )
    };
    assert_eq!(
        query(&hot, 1000).unwrap().rows,
        vec![json!({"id":"1","epoch":1,"startBlock":100,"endBlock":110})]
    );
    assert_eq!(
        query(&hot, 2000).unwrap().rows,
        vec![json!({"id":"1","epoch":1,"startBlock":100,"endBlock":105})]
    );
    let result = query(&hot, 3000).unwrap();
    assert!(!result.degraded(), "{result:?}");
    assert_eq!(
        result.rows,
        vec![
            json!({"id":"1","epoch":1,"startBlock":100,"endBlock":105}),
            json!({"id":"2","epoch":2,"startBlock":105,"endBlock":110}),
            json!({"id":"5","epoch":5,"startBlock":120,"endBlock":125}),
        ]
    );
    let financial = analytics::query_hot_cold_at(dir.path(),
        "SELECT id, \"signalledTokens\", \"stakeDeposited\", \"totalRewards\" FROM epoch ORDER BY id",
        analytics::QueryGuard { timeout: REPLAY_QUERY_TIMEOUT, max_rows: 100,     ..Default::default() }, &hot, 0, &schema, 3000).unwrap();
    assert!(!financial.degraded(), "{financial:?}");
    assert_eq!(
        financial.rows,
        vec![
            json!({"id":"1","signalledTokens":"0","stakeDeposited":"0","totalRewards":"0"}),
            json!({"id":"2","signalledTokens":"1","stakeDeposited":"0","totalRewards":"0"}),
            json!({"id":"5","signalledTokens":"0","stakeDeposited":"123456789012345678901234567890","totalRewards":"0"}),
        ]
    );
    let network = analytics::query_hot_cold_at(
        dir.path(), "SELECT id, \"currentEpoch\", \"epochLength\", \"epochCount\", \"currentL1BlockNumber\", \"totalTokensStaked\", \"isPaused\" FROM graph_network",
        analytics::QueryGuard { timeout: REPLAY_QUERY_TIMEOUT, max_rows: 100,     ..Default::default() }, &hot, 0, &schema, 3000).unwrap();
    assert!(!network.degraded(), "{network:?}");
    assert_eq!(
        network.rows,
        vec![
            json!({"id":"1","currentEpoch":5,"epochLength":5,"epochCount":3,"currentL1BlockNumber":"121","totalTokensStaked":"123456789012345678901234567890","isPaused":false})
        ]
    );
    let mut saved_clock = hot.clone();
    for (block, l1, table, fields, expected) in [
        (
            4000,
            122,
            "graph_token__approval",
            json!({"owner":"a","spender":"b","value":"1"}),
            "121",
        ),
        (
            4001,
            123,
            "graph_token__transfer",
            json!({"from":"a","to":"a","value":"1"}),
            "121",
        ),
        (
            4002,
            123,
            "graph_token__transfer",
            json!({"from":"0x0000000000000000000000000000000000000000","to":"a","value":"1"}),
            "123",
        ),
        (
            4003,
            124,
            "graph_token__transfer",
            json!({"from":"a","to":"b","value":"1"}),
            "123",
        ),
        (
            4004,
            126,
            "graph_token__approval",
            json!({"owner":"a","spender":"b","value":"1"}),
            "126",
        ),
        // POI presentation reads the network clock, but only epoch creation
        // persists it. Deferred rewards need not emit another saving event.
        (
            4005,
            127,
            "subgraph_service__p_o_i_presented",
            json!({"allocationId":"allocation","poi":"poi"}),
            "126",
        ),
        (
            4006,
            131,
            "subgraph_service__p_o_i_presented",
            json!({"allocationId":"allocation","poi":"poi"}),
            "131",
        ),
        (
            4007,
            132,
            "staking_legacy__delegation_parameters_updated",
            json!({"indexer":"a","indexingRewardCut":"1","queryFeeCut":"2","cooldownBlocks":"0"}),
            "131",
        ),
        // Creating an indexer saves its counter on GraphNetwork, while later
        // parameter changes only save the indexer unless an epoch is created.
        (
            4008,
            133,
            "staking_legacy__delegation_parameters_updated",
            json!({"indexer":"new","indexingRewardCut":"1","queryFeeCut":"2","cooldownBlocks":"0"}),
            "133",
        ),
        (
            4009,
            134,
            "staking_legacy__delegation_parameters_updated",
            json!({"indexer":"new","indexingRewardCut":"1","queryFeeCut":"2","cooldownBlocks":"0"}),
            "133",
        ),
        (
            4010,
            136,
            "staking_legacy__delegation_parameters_updated",
            json!({"indexer":"new","indexingRewardCut":"1","queryFeeCut":"2","cooldownBlocks":"0"}),
            "136",
        ),
    ] {
        let mut event =
            json!({"block_number":block,"block_hash":format!("h{block}"),"log_index":0});
        event
            .as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        saved_clock.entry(table.into()).or_default().push(event);
        saved_clock.get_mut("epoch_manager_l1_block").unwrap().push(json!({
            "block_number":block,"block_hash":format!("h{block}"),"result":format!("0x{l1:064x}"),"reverted":false
        }));
        let actual = analytics::query_hot_cold_at(
            dir.path(),
            "SELECT \"currentL1BlockNumber\" FROM graph_network",
            analytics::QueryGuard {
                timeout: REPLAY_QUERY_TIMEOUT,
                max_rows: 10,
                ..Default::default()
            },
            &saved_clock,
            0,
            &schema,
            block,
        )
        .unwrap();
        assert_eq!(
            actual.rows[0]["currentL1BlockNumber"], expected,
            "block {block}"
        );
    }
    hot.get_mut("epoch_manager_l1_block").unwrap()[2]["block_hash"] = json!("wrong-fork");
    let error = format!("{:#}", query(&hot, 3000).unwrap_err());
    assert!(error.contains("requires a pinned"), "{error}");
}

#[test]
fn disputes_apply_linked_rejection_draw_and_horizon_cancellation_at_their_blocks() {
    let _replay = replay_slot();
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/network-nest");
    let config = Config::load(&source).unwrap();
    let mut schema = registry::from_nest(&source, &config).unwrap().schema();
    schema.extend(nuthatch::calls::schema(&config.calls, true));
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("views")).unwrap();
    for name in ["20-allocations.sql", "50-disputes.sql"] {
        std::fs::copy(
            source.join("views").join(name),
            dir.path().join("views").join(name),
        )
        .unwrap();
    }
    let mut hot = analytics::HotRows::new();
    let mut event = |table: &str, block: u64, index: u64, fields: Value| {
        let mut row = json!({"block_number":block,"block_hash":format!("hash{block}"),
            "block_timestamp":1000+block,"log_index":index,"indexer":"i","fisherman":"f"});
        row.as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        hot.entry(table.into()).or_default().push(row);
    };
    for (index, id) in [(0, "a"), (1, "b")] {
        event(
            "dispute_legacy__query_dispute_created",
            1,
            index,
            json!({"disputeID":id,"subgraphDeploymentID":"deployment","tokens":"10","attestation":"0x"}),
        );
    }
    event(
        "dispute_legacy__dispute_linked",
        2,
        0,
        json!({"disputeID1":"a","disputeID2":"b"}),
    );
    event(
        "dispute_legacy__dispute_accepted",
        3,
        0,
        json!({"disputeID":"a","tokens":"25"}),
    );
    for (index, id) in [(0, "c"), (1, "d")] {
        event(
            "dispute_horizon__query_dispute_created",
            4,
            index,
            json!({"disputeId":id,"subgraphDeploymentId":"deployment","tokens":"20","cancellableAt":"1500","attestation":"0x"}),
        );
    }
    event(
        "dispute_horizon__dispute_linked",
        5,
        0,
        json!({"disputeId1":"c","disputeId2":"d"}),
    );
    event(
        "dispute_horizon__dispute_drawn",
        6,
        0,
        json!({"disputeId":"c","tokens":"20"}),
    );
    event(
        "dispute_horizon__query_dispute_created",
        7,
        0,
        json!({"disputeId":"e","subgraphDeploymentId":"deployment","tokens":"20","cancellableAt":"1500","attestation":"0x"}),
    );
    event(
        "dispute_horizon__dispute_cancelled",
        8,
        0,
        json!({"disputeId":"e","tokens":"20"}),
    );
    let query = |block| {
        let result = analytics::query_hot_cold_at(
            dir.path(),
            "SELECT * FROM dispute ORDER BY id",
            analytics::QueryGuard {
                timeout: REPLAY_QUERY_TIMEOUT,
                max_rows: 100,
                ..Default::default()
            },
            &hot,
            0,
            &schema,
            block,
        )
        .unwrap();
        assert!(!result.degraded(), "{result:?}");
        result.rows
    };
    assert_eq!(query(1)[0]["type"], "SingleQuery");
    assert_eq!(query(2)[0]["status"], "Undecided");
    assert_eq!(query(2)[0]["type"], "Conflicting");
    let old = query(3);
    assert_eq!(old[0]["status"], "Accepted");
    assert_eq!(old[0]["tokensRewarded"], "15");
    assert_eq!(old[1]["status"], "Rejected");
    assert_eq!(old[1]["tokensRewarded"], "0");
    assert_eq!(old[1]["closedAt"], 1003);
    let latest = query(8);
    assert_eq!(latest[2]["status"], "Draw");
    assert_eq!(latest[3]["status"], "Draw");
    assert_eq!(latest[4]["status"], "Cancelled");
    assert_eq!(latest[4]["cancellableAt"], "1500");
    assert_eq!(latest[4]["isLegacy"], false);
    assert_eq!(query(4)[2]["status"], "Undecided");
}

#[test]
fn protocol_parameters_require_matching_reads_and_horizon_clear_wins_in_order() {
    let _replay = replay_slot();
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/network-nest");
    let config = Config::load(&source).unwrap();
    let mut schema = registry::from_nest(&source, &config).unwrap().schema();
    schema.extend(nuthatch::calls::schema(&config.calls, true));
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("views")).unwrap();
    std::fs::copy(
        source.join("views/07-protocol-parameters.sql"),
        dir.path().join("views/params.sql"),
    )
    .unwrap();
    let mut hot = analytics::HotRows::from([
        (
            "staking_parameters__parameter_updated".into(),
            vec![
                json!({"block_number":10,"block_hash":"h10","log_index":0,"param":"thawingPeriod"}),
                json!({"block_number":10,"block_hash":"h10","log_index":1,"param":"minimumIndexerStake"}),
            ],
        ),
        (
            "thawing_period_read".into(),
            vec![
                json!({"block_number":10,"block_hash":"h10","result":format!("0x{:064x}",20),"reverted":false}),
            ],
        ),
        (
            "minimum_indexer_stake_read".into(),
            vec![
                json!({"block_number":10,"block_hash":"h10","result":format!("0x{}","ff".repeat(32)),"reverted":false}),
            ],
        ),
        (
            "horizon_staking__thawing_period_cleared".into(),
            vec![json!({"block_number":11,"block_hash":"h11","log_index":0})],
        ),
    ]);
    let query = |hot: &analytics::HotRows, block| {
        analytics::query_hot_cold_at(
            dir.path(),
            "SELECT * FROM protocol_parameters ORDER BY parameter",
            analytics::QueryGuard {
                timeout: REPLAY_QUERY_TIMEOUT,
                max_rows: 100,
                ..Default::default()
            },
            hot,
            0,
            &schema,
            block,
        )
    };
    let before = query(&hot, 10).unwrap();
    assert!(!before.degraded(), "{before:?}");
    assert_eq!(
        before.rows[0]["value"],
        "115792089237316195423570985008687907853269984665640564039457584007913129639935"
    );
    assert_eq!(before.rows[1]["value"], "20");
    assert_eq!(query(&hot, 11).unwrap().rows[1]["value"], "0");
    hot.get_mut("minimum_indexer_stake_read").unwrap()[0]["block_hash"] = json!("orphan");
    let error = format!("{:#}", query(&hot, 10).unwrap_err());
    assert!(
        error.contains("missing pinned minimumIndexerStake"),
        "{error}"
    );
}

#[test]
fn controller_pause_and_ownership_are_independent_historical_state() {
    let _replay = replay_slot();
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/network-nest");
    let config = Config::load(&source).unwrap();
    let mut schema = registry::from_nest(&source, &config).unwrap().schema();
    schema.extend(nuthatch::calls::schema(&config.calls, true));
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("views")).unwrap();
    std::fs::copy(
        source.join("views/06-controller.sql"),
        dir.path().join("views/controller.sql"),
    )
    .unwrap();
    let hot = analytics::HotRows::from([
        (
            "controller__new_ownership".into(),
            vec![json!({"block_number":10,"log_index":0,"from":"old","to":"new"})],
        ),
        (
            "controller__pause_changed".into(),
            vec![
                json!({"block_number":8,"block_hash":"h8","log_index":0,"isPaused":true}),
                json!({"block_number":9,"block_hash":"h9","log_index":0,"isPaused":false}),
                json!({"block_number":11,"log_index":0,"isPaused":true}),
                json!({"block_number":12,"log_index":0,"isPaused":false}),
            ],
        ),
        (
            "governor_read".into(),
            vec![
                json!({"block_number":8,"block_hash":"h8","result":format!("0x{}{}", "00".repeat(12), "11".repeat(20)),"reverted":false}),
                json!({"block_number":9,"block_hash":"h9","result":format!("0x{}{}", "00".repeat(12), "22".repeat(20)),"reverted":false}),
            ],
        ),
        (
            "controller__partial_pause_changed".into(),
            vec![json!({"block_number":11,"log_index":1,"isPaused":true})],
        ),
    ]);
    let query = |block| {
        let result = analytics::query_hot_cold_at(
            dir.path(),
            "SELECT * FROM controller_state",
            analytics::QueryGuard {
                timeout: REPLAY_QUERY_TIMEOUT,
                max_rows: 100,
                ..Default::default()
            },
            &hot,
            0,
            &schema,
            block,
        )
        .unwrap();
        assert!(!result.degraded(), "{result:?}");
        result.rows
    };
    assert!(query(7).is_empty());
    assert_eq!(query(8)[0]["governor"], format!("0x{}", "11".repeat(20)));
    assert_eq!(query(9)[0]["governor"], format!("0x{}", "11".repeat(20)));
    assert_eq!(query(10)[0]["governor"], "new");
    assert_eq!(query(10)[0]["isPaused"], false);
    assert_eq!(query(11)[0]["isPaused"], true);
    assert_eq!(query(12)[0]["isPaused"], false);
    assert_eq!(query(12)[0]["isPartialPaused"], true);
    assert_eq!(query(11)[0]["isPaused"], true);
}

#[test]
fn epoch_length_changes_keep_the_old_epoch_start_and_require_pinned_l1_reads() {
    let _replay = replay_slot();
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/network-nest");
    let config = Config::load(&source).unwrap();
    let mut schema = registry::from_nest(&source, &config).unwrap().schema();
    schema.extend(nuthatch::calls::schema(&config.calls, true));
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("views")).unwrap();
    std::fs::copy(
        source.join("views/05-epoch-schedule.sql"),
        dir.path().join("views/epochs.sql"),
    )
    .unwrap();
    let mut hot = analytics::HotRows::new();
    for (block, l1_block, epoch, length) in
        [(1000, 100, 1, 10), (2000, 115, 2, 20), (3000, 151, 4, 5)]
    {
        hot.entry("epoch_manager__epoch_length_update".into())
            .or_default()
            .push(
                json!({"block_number":block,"block_hash":format!("hash{block}"),"block_timestamp":block,"log_index":1,
                "epoch":epoch.to_string(),"epochLength":length.to_string()}),
            );
        hot.entry("epoch_manager_l1_block".into()).or_default().push(
            json!({"block_number":block,"block_hash":format!("hash{block}"),"block_timestamp":block,"result":format!("0x{l1_block:064x}"),"reverted":false}));
    }
    let query = |hot: &analytics::HotRows, block| {
        analytics::query_hot_cold_at(
            dir.path(),
            "SELECT epoch, length, start_block FROM epoch_schedule ORDER BY seq",
            analytics::QueryGuard {
                timeout: REPLAY_QUERY_TIMEOUT,
                max_rows: 100,
                ..Default::default()
            },
            hot,
            0,
            &schema,
            block,
        )
    };
    let rows = query(&hot, 3000).unwrap();
    assert!(!rows.degraded(), "{rows:?}");
    assert_eq!(
        rows.rows,
        vec![
            json!({"epoch":1,"length":10,"start_block":100}),
            json!({"epoch":2,"length":20,"start_block":110}),
            json!({"epoch":4,"length":5,"start_block":150}),
        ]
    );
    assert_eq!(query(&hot, 1000).unwrap().rows.len(), 1);
    hot.get_mut("epoch_manager_l1_block").unwrap().pop();
    let error = format!("{:#}", query(&hot, 3000).unwrap_err());
    assert!(error.contains("requires a successful pinned"), "{error}");
}

#[test]
fn delegation_rewards_use_the_pool_and_cut_at_the_event_not_the_query_head() {
    let _replay = replay_slot();
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/network-nest");
    let config = Config::load(&source).unwrap();
    let mut schema = registry::from_nest(&source, &config).unwrap().schema();
    schema.extend(nuthatch::calls::schema(&config.calls, true));
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("views")).unwrap();
    for entry in std::fs::read_dir(source.join("views")).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(
            entry.path(),
            dir.path().join("views").join(entry.file_name()),
        )
        .unwrap();
    }
    let mut hot = analytics::HotRows::new();
    let mut event = |table: &str, block: u64, index: u64, fields: Value| {
        let mut row = json!({"block_number":block,"block_hash":format!("hash{block}"),
            "block_timestamp":1000+block,"log_index":index,"indexer":"i","serviceProvider":"i"});
        row.as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        hot.entry(table.into()).or_default().push(row);
    };
    event(
        "rewards__rewards_assigned",
        1,
        1,
        json!({"amount":"100","allocationID":"a"}),
    );
    event(
        "staking_legacy__stake_delegated",
        2,
        1,
        json!({"tokens":"1000","shares":"100"}),
    );
    event(
        "staking_legacy__delegation_parameters_updated",
        3,
        1,
        json!({"indexingRewardCut":"250000","queryFeeCut":"0","cooldownBlocks":"0"}),
    );
    event(
        "rewards__rewards_assigned",
        3,
        2,
        json!({"amount":"100","allocationID":"a"}),
    );
    event(
        "staking_legacy__delegation_parameters_updated",
        3,
        3,
        json!({"indexingRewardCut":"1000000","queryFeeCut":"0","cooldownBlocks":"0"}),
    );
    event(
        "rewards__rewards_assigned",
        3,
        4,
        json!({"amount":"100","allocationID":"a"}),
    );
    event(
        "horizon_staking__tokens_undelegated",
        4,
        1,
        json!({"tokens":"75","shares":"10","verifier":"service"}),
    );
    event(
        "horizon_staking__delegated_tokens_withdrawn",
        5,
        1,
        json!({"tokens":"75","verifier":"service"}),
    );
    let service = "0xb2bb92d0de618878e438b55d5846cfecd9301105";
    event(
        "horizon_staking__provision_created",
        3,
        5,
        json!({"verifier":service,"tokens":"1","maxVerifierCut":"0","thawingPeriod":"0"}),
    );
    event(
        "staking_legacy__delegation_parameters_updated",
        6,
        1,
        json!({"indexingRewardCut":"250000","queryFeeCut":"0","cooldownBlocks":"0"}),
    );
    event(
        "rewards__rewards_assigned",
        6,
        2,
        json!({"amount":"50","allocationID":"a"}),
    );
    event(
        "horizon_staking__tokens_delegated",
        7,
        1,
        json!({"verifier":service,"tokens":"100","shares":"10"}),
    );
    event(
        "horizon_staking__tokens_delegated",
        7,
        2,
        json!({"verifier":"other","tokens":"999","shares":"9"}),
    );
    event(
        "horizon_staking__tokens_undelegated",
        8,
        1,
        json!({"verifier":service,"tokens":"40","shares":"4"}),
    );
    event(
        "horizon_staking__delegated_tokens_withdrawn",
        9,
        1,
        json!({"verifier":service,"tokens":"25"}),
    );
    event(
        "horizon_staking__delegation_slashed",
        10,
        1,
        json!({"verifier":service,"tokens":"8"}),
    );
    event(
        "horizon_staking__tokens_to_delegation_pool_added",
        11,
        1,
        json!({"verifier":service,"tokens":"20"}),
    );
    event(
        "staking_legacy__stake_delegated",
        9,
        2,
        json!({"indexer":"j","serviceProvider":"j","tokens":"40","shares":"4"}),
    );
    event(
        "horizon_staking__tokens_delegated",
        10,
        2,
        json!({"indexer":"j","serviceProvider":"j","verifier":service,"tokens":"60","shares":"6"}),
    );
    let query = |block, sql| {
        let result = analytics::query_hot_cold_at(
            dir.path(),
            sql,
            analytics::QueryGuard {
                timeout: REPLAY_QUERY_TIMEOUT,
                max_rows: 100,
                ..Default::default()
            },
            &hot,
            0,
            &schema,
            block,
        )
        .unwrap();
        assert!(!result.degraded(), "{result:?}");
        result.rows
    };
    let sql = "SELECT * FROM indexer_delegation";
    assert_eq!(query(1, sql)[0]["delegatedTokens"], "0");
    assert_eq!(query(3, sql)[0]["delegatedTokens"], "1075");
    assert_eq!(query(4, sql)[0]["delegatedTokens"], "1075");
    assert_eq!(query(4, sql)[0]["delegatedThawingTokens"], "75");
    assert_eq!(query(4, sql)[0]["delegatorShares"], "90");
    assert_eq!(query(5, sql)[0]["delegatedTokens"], "1000");
    assert_eq!(query(5, sql)[0]["delegatedThawingTokens"], "0");
    let rewards = query(5, "SELECT CAST(indexer_reward AS VARCHAR) AS reward FROM delegation_ledger WHERE reward <> '0' ORDER BY block_number, log_index");
    assert_eq!(
        rewards,
        vec![
            json!({"reward":"100"}),
            json!({"reward":"25"}),
            json!({"reward":"100"})
        ]
    );
    assert_eq!(query(2, sql)[0]["delegatedTokens"], "1000");
    let provision =
        format!("SELECT * FROM provision WHERE indexer = 'i' AND \"dataService\" = '{service}'");
    assert!(query(2, &provision).is_empty());
    assert_eq!(query(3, &provision)[0]["delegatedTokens"], "1075");
    assert_eq!(query(5, &provision)[0]["delegatedTokens"], "1075");
    assert_eq!(query(6, &provision)[0]["delegatedTokens"], "1113");
    assert_eq!(query(8, &provision)[0]["delegatedTokens"], "1213");
    assert_eq!(query(8, &provision)[0]["delegatorShares"], "106");
    assert_eq!(query(8, &provision)[0]["delegatedThawingTokens"], "40");
    let latest = &query(11, &provision)[0];
    assert_eq!(latest["delegatedTokens"], "1200");
    assert_eq!(latest["delegatedThawingTokens"], "15");
    assert_eq!(latest["tokensSlashedDelegationPool"], "8");
    let other = query(
        11,
        "SELECT * FROM provision WHERE \"dataService\" = 'other'",
    );
    assert_eq!(other[0]["delegatedTokens"], "999");
    assert_eq!(other[0]["delegatorShares"], "9");
    let created_by_delegation = query(10, "SELECT * FROM provision WHERE indexer = 'j'");
    assert_eq!(created_by_delegation[0]["delegatedTokens"], "100");
    assert_eq!(created_by_delegation[0]["delegatorShares"], "10");
}

#[test]
fn indexer_registration_migration_and_stake_are_reconstructed_at_each_block() {
    let _replay = replay_slot();
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/network-nest");
    let config = Config::load(&source).unwrap();
    let mut schema = registry::from_nest(&source, &config).unwrap().schema();
    schema.extend(nuthatch::calls::schema(&config.calls, true));
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("views")).unwrap();
    for entry in std::fs::read_dir(source.join("views")).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(
            entry.path(),
            dir.path().join("views").join(entry.file_name()),
        )
        .unwrap();
    }
    let mut hot = analytics::HotRows::new();
    let mut event = |table: &str, block: u64, fields: Value| {
        let mut row = json!({"block_number":block,"block_hash":format!("hash{block}"),
            "block_timestamp":1000+block,"log_index":1,"indexer":"i","serviceProvider":"i"});
        row.as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        hot.entry(table.into()).or_default().push(row);
    };
    event(
        "staking_legacy__stake_deposited",
        1,
        json!({"tokens":"100"}),
    );
    event(
        "subgraph_service__service_provider_registered",
        2,
        json!({"data":"0x"}),
    );
    let tuple = alloy_dyn_abi::DynSolValue::Tuple(vec![
        alloy_dyn_abi::DynSolValue::String("https://indexer.example".into()),
        alloy_dyn_abi::DynSolValue::String("u10".into()),
        alloy_dyn_abi::DynSolValue::Address(alloy_primitives::Address::ZERO),
    ]);
    event(
        "subgraph_service__service_provider_registered",
        3,
        json!({"data":format!("0x{}",hex::encode(tuple.abi_encode_params()))}),
    );
    event(
        "horizon_staking__horizon_stake_deposited",
        4,
        json!({"tokens":"30"}),
    );
    event(
        "horizon_staking__provision_created",
        4,
        json!({"tokens":"50","verifier":"service","maxVerifierCut":"1","thawingPeriod":"20"}),
    );
    event(
        "horizon_staking__provision_slashed",
        5,
        json!({"tokens":"2","verifier":"service"}),
    );
    event(
        "horizon_staking__horizon_stake_withdrawn",
        6,
        json!({"tokens":"5"}),
    );
    event(
        "staking_legacy__allocation_created",
        7,
        json!({"allocationID":"a","subgraphDeploymentID":format!("0x{}","00".repeat(32)),"tokens":"20","epoch":"1"}),
    );
    event(
        "staking_legacy__stake_locked",
        8,
        json!({"tokens":"70","until":"2000"}),
    );
    event("staking_legacy__stake_slashed", 9, json!({"tokens":"40"}));
    event(
        "staking_legacy__allocation_closed_f672",
        10,
        json!({"allocationID":"a","tokens":"20","epoch":"2","poi":format!("0x{}", "00".repeat(32))}),
    );
    event("staking_legacy__stake_slashed", 11, json!({"tokens":"30"}));
    event(
        "staking_legacy__stake_withdrawn",
        12,
        json!({"tokens":"53"}),
    );
    event(
        "horizon_staking__horizon_stake_deposited",
        13,
        json!({"tokens":"100"}),
    );
    event(
        "staking_legacy__stake_locked",
        14,
        json!({"tokens":"30","until":"3000"}),
    );
    event(
        "horizon_staking__horizon_stake_locked",
        15,
        json!({"tokens":"50","until":"4000"}),
    );
    event(
        "horizon_staking__horizon_stake_withdrawn",
        16,
        json!({"tokens":"10"}),
    );
    event(
        "horizon_staking__delegation_fee_cut_set",
        17,
        json!({"verifier":"service","paymentType":"0","feeCut":"100000"}),
    );
    event(
        "horizon_staking__delegation_fee_cut_set",
        18,
        json!({"verifier":"other-service","paymentType":"2","feeCut":"300000"}),
    );
    event(
        "staking_legacy__delegation_parameters_updated",
        19,
        json!({"queryFeeCut":"123","indexingRewardCut":"456","cooldownBlocks":"789"}),
    );
    let query = |block| {
        let result = analytics::query_hot_cold_at(
            dir.path(),
            "SELECT * FROM indexer",
            analytics::QueryGuard {
                timeout: REPLAY_QUERY_TIMEOUT,
                max_rows: 100,
                ..Default::default()
            },
            &hot,
            0,
            &schema,
            block,
        )
        .unwrap();
        assert!(!result.degraded(), "{result:?}");
        assert_eq!(result.rows.len(), 1);
        result.rows[0].clone()
    };
    assert_eq!(query(1)["isLegacy"], true);
    assert_eq!(
        query(2)["isLegacy"],
        true,
        "malformed metadata does not migrate the indexer"
    );
    assert_eq!(query(3)["isLegacy"], false);
    let latest = query(7);
    assert_eq!(
        latest["isLegacy"], false,
        "legacy allocation creation loads, but does not reclassify, the indexer"
    );
    assert_eq!(latest["createdAt"], 1001);
    assert_eq!(latest["stakedTokens"], "123");
    assert_eq!(latest["provisionedTokens"], "48");
    assert_eq!(latest["allocatedTokens"], "20");
    assert_eq!(latest["allocationCount"], 1);
    assert_eq!(latest["totalAllocationCount"], "1");
    assert_eq!(query(1)["stakedTokens"], "100");
    assert_eq!(query(8)["lockedTokens"], "70");
    assert_eq!(query(9)["lockedTokens"], "63");
    assert_eq!(query(9)["legacyLockedTokens"], "63");
    assert_eq!(query(9)["tokensLockedUntil"], 2000);
    assert_eq!(query(11)["lockedTokens"], "53");
    assert_eq!(query(12)["lockedTokens"], "0");
    assert_eq!(query(12)["legacyTokensLockedUntil"], 0);
    assert_eq!(query(15)["lockedTokens"], "50");
    assert_eq!(query(15)["legacyLockedTokens"], "30");
    assert_eq!(query(16)["lockedTokens"], "0");
    assert_eq!(query(16)["tokensLockedUntil"], 0);
    assert_eq!(query(16)["legacyLockedTokens"], "30");
    assert_eq!(query(16)["legacyTokensLockedUntil"], 3000);
    assert_eq!(query(16)["queryFeeCut"], 1000000);
    assert_eq!(query(18)["queryFeeCut"], 900000);
    assert_eq!(query(18)["indexingRewardCut"], 700000);
    assert_eq!(query(19)["legacyQueryFeeCut"], 123);
    assert_eq!(query(19)["legacyIndexingRewardCut"], 456);
    assert_eq!(query(19)["delegatorParameterCooldown"], 789);
    assert_eq!(query(19)["queryFeeCut"], 900000);
}

#[test]
fn publishing_versions_and_deployment_creation_follow_the_upstream_event_sequence() {
    let _replay = replay_slot();
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/network-nest");
    let config = Config::load(&source).unwrap();
    let mut schema = registry::from_nest(&source, &config).unwrap().schema();
    schema.extend(nuthatch::calls::schema(&config.calls, true));
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("views")).unwrap();
    for entry in std::fs::read_dir(source.join("views")).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(
            entry.path(),
            dir.path().join("views").join(entry.file_name()),
        )
        .unwrap();
    }
    let mut hot = analytics::HotRows::new();
    let first = format!("0x{}", "00".repeat(32));
    let second = format!("0x{}", "11".repeat(32));
    let mut event = |table: &str, block: u64, index: u64, fields: Value| {
        let mut row = json!({"block_number":block,"block_hash":format!("hash{block}"),
            "block_timestamp":1000+block,"log_index":index});
        row.as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        hot.entry(table.into()).or_default().push(row);
    };
    event(
        "gns__subgraph_received_from_l1",
        1,
        1,
        json!({"_l2SubgraphID":"256"}),
    );
    // Upstream ignores denials before the deployment exists.
    event(
        "rewards__rewards_denylist_updated",
        1,
        2,
        json!({"subgraphDeploymentID":first,"sinceBlock":"1"}),
    );
    event(
        "gns__subgraph_published_42a5",
        2,
        1,
        json!({"subgraphID":"256","subgraphDeploymentID":first}),
    );
    event(
        "gns__subgraph_version_updated",
        2,
        2,
        json!({"subgraphID":"256","subgraphDeploymentID":first}),
    );
    event(
        "gns__subgraph_version_updated",
        3,
        1,
        json!({"subgraphID":"256","subgraphDeploymentID":second}),
    );
    event(
        "rewards__rewards_denylist_updated",
        3,
        2,
        json!({"subgraphDeploymentID":first,"sinceBlock":"3"}),
    );
    event(
        "rewards__rewards_denylist_updated",
        4,
        1,
        json!({"subgraphDeploymentID":first,"sinceBlock":"0"}),
    );
    let query = |block, sql| {
        let result = analytics::query_hot_cold_at(
            dir.path(),
            sql,
            analytics::QueryGuard {
                timeout: REPLAY_QUERY_TIMEOUT,
                max_rows: 100,
                ..Default::default()
            },
            &hot,
            0,
            &schema,
            block,
        )
        .unwrap();
        assert!(!result.degraded(), "{result:?}");
        result.rows
    };
    assert_eq!(query(1, "SELECT * FROM subgraph")[0]["versionCount"], "0");
    assert!(query(1, "SELECT * FROM subgraph_deployment").is_empty());
    let versions = query(3, "SELECT * FROM subgraph_version ORDER BY version");
    assert_eq!(versions.len(), 2);
    assert_eq!(versions[0]["id"], "5R-0");
    assert_eq!(versions[0]["subgraphDeployment"], first);
    assert_eq!(versions[1]["id"], "5R-1");
    assert_eq!(versions[1]["subgraphDeployment"], second);
    let subgraph = query(3, "SELECT * FROM subgraph");
    assert_eq!(subgraph[0]["id"], "5R");
    assert_eq!(subgraph[0]["createdAt"], 1001);
    assert_eq!(subgraph[0]["versionCount"], "2");
    let deployments = query(3, "SELECT * FROM subgraph_deployment ORDER BY id");
    assert_eq!(deployments.len(), 2);
    assert_eq!(deployments[0]["deniedAt"], 3);
    assert_eq!(deployments[0]["createdAt"], 1002);
    assert_eq!(deployments[0]["stakedTokens"], "0");
    assert_eq!(deployments[1]["deniedAt"], 0);
    assert_eq!(
        query(4, "SELECT * FROM subgraph_deployment ORDER BY id")[0]["deniedAt"],
        0
    );
    assert_eq!(
        query(2, "SELECT * FROM subgraph_deployment")[0]["deniedAt"],
        0
    );
}

#[test]
fn fee_splits_curation_and_rewards_preserve_event_order_without_double_counting() {
    let _replay = replay_slot();
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/network-nest");
    let config = Config::load(&source).unwrap();
    let mut schema = registry::from_nest(&source, &config).unwrap().schema();
    schema.extend(nuthatch::calls::schema(&config.calls, true));
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("views")).unwrap();
    for entry in std::fs::read_dir(source.join("views")).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(
            entry.path(),
            dir.path().join("views").join(entry.file_name()),
        )
        .unwrap();
    }
    let mut hot = analytics::HotRows::new();
    let deployment = format!("0x{}", "00".repeat(32));
    let mut event = |table: &str, block: u64, index: u64, fields: Value| {
        let mut row = json!({"block_number":block,"block_hash":format!("hash{block}"),
            "block_timestamp":1000+block,"log_index":index,"indexer":"i","serviceProvider":"i"});
        let deployment_field = if table.starts_with("subgraph_service__") {
            "subgraphDeploymentId"
        } else {
            "subgraphDeploymentID"
        };
        row[deployment_field] = json!(deployment);
        row.as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        hot.entry(table.into()).or_default().push(row);
    };
    event(
        "staking_legacy__stake_deposited",
        1,
        0,
        json!({"tokens":"100"}),
    );
    event(
        "staking_legacy__allocation_created",
        1,
        1,
        json!({"allocationID":"old","tokens":"20","epoch":"1"}),
    );
    event(
        "subgraph_service__allocation_created",
        2,
        1,
        json!({"allocationId":"new","tokens":"30","currentEpoch":"2"}),
    );
    for block in [1, 2] {
        event(
            "epoch_manager_l1_block",
            block,
            0,
            json!({"result":format!("0x{:064x}",100 + block),"reverted":false}),
        );
    }
    event(
        "curation__signalled",
        2,
        2,
        json!({"tokens":"100","curationTax":"10","signal":"50"}),
    );
    event(
        "staking_parameters__parameter_updated",
        3,
        1,
        json!({"param":"protocolPercentage"}),
    );
    event(
        "protocol_percentage_read",
        3,
        1,
        json!({"result":format!("0x{:064x}",100000),"reverted":false}),
    );
    event(
        "subgraph_service__query_fees_collected",
        3,
        2,
        json!({"allocationId":"new","tokensCollected":"1000","tokensCurators":"200"}),
    );
    event(
        "staking_legacy__allocation_collected",
        4,
        1,
        json!({"allocationID":"old","rebateFees":"7","curationFees":"3"}),
    );
    event(
        "staking_legacy__rebate_collected",
        4,
        2,
        json!({"allocationID":"old","queryFees":"8","curationFees":"4","delegationRewards":"0","queryRebates":"8"}),
    );
    event(
        "rewards__rewards_assigned",
        4,
        3,
        json!({"allocationID":"old","amount":"10"}),
    );
    event(
        "rewards__horizon_rewards_assigned",
        4,
        4,
        json!({"allocationID":"old","amount":"15"}),
    );
    event(
        "rewards__horizon_rewards_assigned",
        4,
        5,
        json!({"allocationID":"new","amount":"300"}),
    );
    event(
        "subgraph_service__indexing_rewards_collected",
        4,
        6,
        json!({"allocationId":"new","tokensRewards":"300","tokensIndexerRewards":"240","tokensDelegationRewards":"60","poi":"0x00"}),
    );
    event(
        "curation__burned",
        5,
        1,
        json!({"tokens":"20","signal":"10"}),
    );
    // The changed percentage only applies after its ParameterUpdated log, even in the same block.
    event(
        "horizon_staking__tokens_to_delegation_pool_added",
        5,
        2,
        json!({"verifier":"0xb2bb92d0de618878e438b55d5846cfecd9301105","tokens":"100"}),
    );
    event(
        "horizon_staking__delegation_fee_cut_set",
        5,
        3,
        json!({"verifier":"0xb2bb92d0de618878e438b55d5846cfecd9301105","paymentType":"0","feeCut":"200000"}),
    );
    event(
        "subgraph_service__query_fees_collected",
        6,
        1,
        json!({"allocationId":"new","tokensCollected":"1000","tokensCurators":"200"}),
    );
    event(
        "horizon_staking__delegation_slashed",
        7,
        1,
        json!({"verifier":"0xb2bb92d0de618878e438b55d5846cfecd9301105","tokens":"100"}),
    );
    event(
        "subgraph_service__query_fees_collected",
        7,
        2,
        json!({"allocationId":"new","tokensCollected":"1000","tokensCurators":"200"}),
    );
    event(
        "staking_legacy__rebate_claimed",
        7,
        3,
        json!({"allocationID":"old","tokens":"3","delegationFees":"1"}),
    );
    event(
        "staking_parameters__parameter_updated",
        6,
        2,
        json!({"param":"protocolPercentage"}),
    );
    event(
        "protocol_percentage_read",
        6,
        2,
        json!({"result":format!("0x{:064x}",200000),"reverted":false}),
    );
    event(
        "subgraph_service__query_fees_collected",
        6,
        3,
        json!({"allocationId":"new","tokensCollected":"1000","tokensCurators":"200"}),
    );
    let query = |block, sql| {
        let result = analytics::query_hot_cold_at(
            dir.path(),
            sql,
            analytics::QueryGuard {
                timeout: REPLAY_QUERY_TIMEOUT,
                max_rows: 100,
                ..Default::default()
            },
            &hot,
            0,
            &schema,
            block,
        )
        .unwrap();
        assert!(!result.degraded(), "fixture query degraded: {result:?}");
        result.rows
    };
    assert_eq!(query(5, "SELECT id FROM allocation_creation").len(), 2);
    let fees = query(5, "SELECT * FROM allocation ORDER BY id");
    assert_eq!(
        fees.len(),
        2,
        "allocation projection must retain both creations"
    );
    assert_eq!(fees[0]["queryFeesCollected"], "700");
    assert_eq!(fees[0]["indexingRewards"], "300");
    assert_eq!(fees[0]["indexingIndexerRewards"], "240");
    assert_eq!(fees[0]["indexingDelegatorRewards"], "60");
    assert_eq!(fees[1]["queryFeesCollected"], "15");
    assert_eq!(fees[1]["indexingRewards"], "25");
    assert_eq!(fees[1]["indexingIndexerRewards"], "25");
    assert_eq!(fees[1]["indexingDelegatorRewards"], "0");
    let provision_rewards = query(4, "SELECT \"rewardsEarned\", \"indexerIndexingRewards\", \"delegatorIndexingRewards\", \"delegatedTokens\" FROM provision");
    assert_eq!(
        provision_rewards,
        vec![
            json!({"rewardsEarned":"300", "indexerIndexingRewards":"265",
        "delegatorIndexingRewards":"60", "delegatedTokens":"0"})
        ]
    );
    let financials = query(5, "SELECT * FROM deployment_financials");
    assert_eq!(
        financials[0]["ipfsHash"],
        "QmNLei78zWmzUdbeRB3CiUfAizWUrbeeZh5K1rhAQKCh51"
    );
    assert_eq!(financials[0]["stakedTokens"], "50");
    assert_eq!(financials[0]["signalledTokens"], "277");
    assert_eq!(financials[0]["signalAmount"], "40");
    assert_eq!(financials[0]["queryFeesAmount"], "715");
    assert_eq!(financials[0]["indexingRewardAmount"], "325");
    assert_eq!(
        query(6, "SELECT * FROM allocation WHERE id = 'new'")[0]["queryFeesCollected"],
        "2000"
    );
    let rebates = query(7, "SELECT id, \"queryFeeRebates\", \"delegationFees\", \"distributedRebates\" FROM allocation ORDER BY id");
    assert_eq!(
        rebates,
        vec![
            json!({"id":"new","queryFeeRebates":"2340","delegationFees":"260","distributedRebates":"2600"}),
            json!({"id":"old","queryFeeRebates":"3","delegationFees":"1","distributedRebates":"8"})
        ]
    );
    let provision_fees = query(7, "SELECT \"queryFeesCollected\", \"indexerQueryFees\", \"delegatorQueryFees\" FROM provision");
    assert_eq!(
        provision_fees,
        vec![
            json!({"queryFeesCollected":"2600","indexerQueryFees":"2340","delegatorQueryFees":"260"})
        ]
    );
    for (block, collected, rebates, delegator) in [
        (3, "700", "700", "0"),
        (4, "715", "708", "0"),
        (7, "2615", "2351", "261"),
    ] {
        let indexer_fees = query(block, "SELECT id, \"queryFeesCollected\", \"queryFeeRebates\", \"delegatorQueryFees\" FROM indexer");
        assert_eq!(
            indexer_fees,
            vec![json!({"id":"i", "queryFeesCollected":collected,
                "queryFeeRebates":rebates, "delegatorQueryFees":delegator})],
            "indexer fees at block {block} must accumulate legacy rebates, not replace them"
        );
    }
    assert_eq!(
        query(2, "SELECT * FROM deployment_financials")[0]["signalledTokens"],
        "90"
    );
}

#[test]
fn provision_history_separates_thawing_deprovisioning_and_staged_parameters() {
    let _replay = replay_slot();
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/network-nest");
    let config = Config::load(&source).unwrap();
    let mut schema = registry::from_nest(&source, &config).unwrap().schema();
    schema.extend(nuthatch::calls::schema(&config.calls, true));
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("views")).unwrap();
    for entry in std::fs::read_dir(source.join("views")).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(
            entry.path(),
            dir.path().join("views").join(entry.file_name()),
        )
        .unwrap();
    }
    let mut hot = analytics::HotRows::new();
    let service = "0xb2bb92d0de618878e438b55d5846cfecd9301105";
    let indexer = format!("0x{}", "11".repeat(20));
    let large = "99999999999999999999999999999999999999";
    let mut event = |table: &str, block: u64, fields: Value| {
        let mut row = json!({"block_number":block,"log_index":1,"block_timestamp":1000+block,
            "serviceProvider":indexer,"indexer":indexer,"verifier":service});
        row.as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        hot.entry(table.into()).or_default().push(row);
    };
    event(
        "subgraph_service__service_provider_registered",
        7,
        json!({"data":"0x"}),
    );
    event(
        "horizon_staking__delegation_fee_cut_set",
        8,
        json!({"paymentType":"2","feeCut":"250000"}),
    );
    event(
        "horizon_staking__delegation_fee_cut_set",
        9,
        json!({"paymentType":"0","feeCut":"100000"}),
    );
    event(
        "horizon_staking__provision_created",
        10,
        json!({"tokens":large,"maxVerifierCut":"100","thawingPeriod":"20"}),
    );
    event(
        "subgraph_service__allocation_created",
        10,
        json!({"tokens":"8"}),
    );
    event(
        "horizon_staking__provision_thawed",
        11,
        json!({"tokens":"5"}),
    );
    event(
        "horizon_staking__provision_parameters_staged",
        11,
        json!({"maxVerifierCut":"200","thawingPeriod":"30"}),
    );
    event(
        "subgraph_service__allocation_resized",
        11,
        json!({"oldTokens":"8","newTokens":"9"}),
    );
    event(
        "horizon_staking__tokens_deprovisioned",
        12,
        json!({"tokens":"2"}),
    );
    event(
        "horizon_staking__provision_parameters_set",
        12,
        json!({"maxVerifierCut":"200","thawingPeriod":"30"}),
    );
    event(
        "subgraph_service__allocation_closed",
        12,
        json!({"tokens":"9"}),
    );
    event(
        "horizon_staking__provision_slashed",
        13,
        json!({"tokens":"1"}),
    );
    // A second verifier has its own provision and does not inherit the first one's allocations.
    event(
        "horizon_staking__provision_created",
        13,
        json!({"verifier":"0xother","tokens":"17","maxVerifierCut":"9","thawingPeriod":"8"}),
    );
    event(
        "horizon_staking__thaw_request_created",
        14,
        json!({"verifier":"0xthaw","requestType":"0","thawingUntil":"2000"}),
    );
    event(
        "horizon_staking__thaw_request_created",
        15,
        json!({"verifier":"0xthaw","requestType":"0","thawingUntil":"1900"}),
    );
    event(
        "horizon_staking__thaw_request_created",
        16,
        json!({"verifier":"0xdelegation","requestType":"1","thawingUntil":"9000"}),
    );
    let tuple = alloy_dyn_abi::DynSolValue::Tuple(vec![
        alloy_dyn_abi::DynSolValue::String("https://indexer.example".into()),
        alloy_dyn_abi::DynSolValue::String("u10".into()),
        alloy_dyn_abi::DynSolValue::Address(alloy_primitives::Address::ZERO),
    ]);
    event(
        "subgraph_service__service_provider_registered",
        17,
        json!({"data":format!("0x{}", hex::encode(tuple.abi_encode_params()))}),
    );
    let destination = format!("0x{}", "22".repeat(20));
    event(
        "subgraph_service__rewards_destination_set",
        18,
        json!({"rewardsDestination":destination}),
    );
    event(
        "subgraph_service__service_provider_registered",
        19,
        json!({"data":"0x"}),
    );
    let query = |block| {
        analytics::query_hot_cold_at(
            dir.path(),
            "SELECT * FROM provision ORDER BY id",
            analytics::QueryGuard {
                timeout: REPLAY_QUERY_TIMEOUT,
                max_rows: 100,
                ..Default::default()
            },
            &hot,
            0,
            &schema,
            block,
        )
        .unwrap()
        .rows
    };
    assert!(
        query(7).is_empty(),
        "malformed registration creates no provision"
    );
    let registered = query(8);
    assert_eq!(registered.len(), 1);
    assert_eq!(registered[0]["createdAt"], "1008");
    assert_eq!(registered[0]["tokensProvisioned"], "0");
    assert_eq!(registered[0]["indexingRewardsCut"], "750000");
    assert_eq!(registered[0]["queryFeeCut"], "1000000");
    assert_eq!(query(9)[0]["queryFeeCut"], "900000");
    let before = &query(11)[0];
    assert_eq!(before["id"], format!("{indexer}-{service}"));
    assert_eq!(before["tokensProvisioned"], large);
    assert_eq!(before["tokensThawing"], "5");
    assert_eq!(before["tokensAllocated"], "9");
    assert_eq!(before["allocationCount"], 1);
    assert_eq!(before["maxVerifierCut"], "100");
    assert_eq!(before["thawingPeriod"], "20");
    assert_eq!(before["maxVerifierCutPending"], "200");
    assert_eq!(before["indexingFeeCut"], "1000000");
    assert_eq!(before["indexingRewardsCut"], "750000");
    let after = query(13);
    assert_eq!(after.len(), 2);
    assert_eq!(
        after[0]["tokensProvisioned"],
        "99999999999999999999999999999999999996"
    );
    assert_eq!(after[0]["tokensThawing"], "3");
    assert_eq!(after[0]["tokensAllocated"], "0");
    assert_eq!(after[0]["allocationCount"], 0);
    assert_eq!(after[0]["totalAllocationCount"], "1");
    assert_eq!(after[0]["maxVerifierCut"], "200");
    assert_eq!(after[0]["tokensSlashedServiceProvider"], "1");
    assert_eq!(after[1]["tokensAllocated"], "0");
    // Rewind must reconstruct the earlier state, not reuse the latest parameter cache.
    assert_eq!(query(10)[0]["tokensThawing"], "0");
    let thaws = query(16);
    assert_eq!(
        thaws.len(),
        3,
        "delegation thaw does not create a provision"
    );
    let thaw = thaws
        .iter()
        .find(|row| row["dataService"] == "0xthaw")
        .unwrap();
    assert_eq!(thaw["createdAt"], "1014");
    assert_eq!(thaw["thawingUntil"], "2000");
    assert_eq!(thaw["tokensProvisioned"], "0");
    assert_eq!(thaw["tokensThawing"], "0");
    assert_eq!(query(16)[0]["url"], "");
    assert_eq!(query(16)[0]["rewardsDestination"], "0x00000000");
    assert_eq!(query(17)[0]["url"], "https://indexer.example");
    assert_eq!(query(17)[0]["geoHash"], "u10");
    assert_eq!(
        query(17)[0]["rewardsDestination"],
        format!("0x{}", "00".repeat(20))
    );
    assert_eq!(query(19)[0]["url"], "https://indexer.example");
    assert_eq!(query(19)[0]["rewardsDestination"], destination);
    let after_registration = query(19);
    let other = after_registration
        .iter()
        .find(|row| row["dataService"] == "0xother")
        .unwrap();
    assert_eq!(other["url"], "");
    assert_eq!(other["rewardsDestination"], "0x00000000");
}

#[test]
fn issuance_follows_the_allocator_and_denylist_changes_are_historical() {
    let _replay = replay_slot();
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/network-nest");
    let config = Config::load(&source).unwrap();
    let mut schema = registry::from_nest(&source, &config).unwrap().schema();
    schema.extend(nuthatch::calls::schema(&config.calls, true));
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("views")).unwrap();
    std::fs::write(
        dir.path().join("views/rewards.sql"),
        include_str!("../tests/fixtures/network-nest/views/30-rewards.sql"),
    )
    .unwrap();
    let mut hot = analytics::HotRows::new();
    let mut event = |table: &str, block: u64, fields: Value| {
        let mut row =
            json!({"block_number":block,"block_hash":format!("hash{block}"),"log_index":1});
        row.as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        hot.entry(table.into()).or_default().push(row);
    };
    event(
        "rewards__parameter_updated",
        10,
        json!({"param":"issuancePerBlock"}),
    );
    event(
        "allocated_issuance_read",
        10,
        json!({"result":"0x","reverted":true}),
    );
    event(
        "legacy_issuance_read",
        10,
        json!({"result":format!("0x{:064x}",120730000000000000000u128),"reverted":false}),
    );
    let target = "0x971b9d3d0ae3eca029cab5ea1fb0f72c85e6a525";
    event(
        "issuance_allocator__target_allocation_updated",
        20,
        json!({"target":target,"newSelfMintingRate":"96584000000000000000"}),
    );
    event(
        "issuance_allocator__target_allocation_updated",
        21,
        json!({"target":"0xother","newSelfMintingRate":"123"}),
    );
    event(
        "rewards__rewards_denylist_updated",
        12,
        json!({"subgraphDeploymentID":"d","sinceBlock":"12"}),
    );
    event(
        "rewards__rewards_denylist_updated",
        22,
        json!({"subgraphDeploymentID":"d","sinceBlock":"0"}),
    );
    event(
        "rewards__parameter_updated",
        25,
        json!({"param":"issuancePerBlock"}),
    );
    // A full-width value catches floating-point coercion while decoding the getter result.
    event(
        "allocated_issuance_read",
        25,
        json!({"result":format!("0x{}","f".repeat(64)),"reverted":false}),
    );
    event(
        "legacy_issuance_read",
        25,
        json!({"result":format!("0x{:064x}",120730000000000000000u128),"reverted":false}),
    );
    let query = |block, sql| {
        analytics::query_hot_cold_at(
            dir.path(),
            sql,
            analytics::QueryGuard {
                timeout: REPLAY_QUERY_TIMEOUT,
                max_rows: 100,
                ..Default::default()
            },
            &hot,
            0,
            &schema,
            block,
        )
        .unwrap()
        .rows
    };
    assert_eq!(
        query(10, "SELECT * FROM network_issuance")[0]["networkGRTIssuancePerBlock"],
        "120730000000000000000"
    );
    assert_eq!(
        query(21, "SELECT * FROM network_issuance")[0]["networkGRTIssuancePerBlock"],
        "96584000000000000000"
    );
    assert_eq!(
        query(25, "SELECT * FROM network_issuance")[0]["networkGRTIssuancePerBlock"],
        "115792089237316195423570985008687907853269984665640564039457584007913129639935"
    );
    assert!(query(11, "SELECT * FROM deployment_denial").is_empty());
    assert_eq!(
        query(12, "SELECT * FROM deployment_denial")[0]["deniedAt"],
        12
    );
    assert_eq!(
        query(22, "SELECT * FROM deployment_denial")[0]["deniedAt"],
        0
    );
}

#[test]
fn escrow_history_matches_the_upstream_balance_signer_and_redemption_rules() {
    let _replay = replay_slot();
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/network-nest");
    let config = Config::load(&source).unwrap();
    let schema = registry::from_nest(&source, &config).unwrap().schema();
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("views")).unwrap();
    std::fs::write(
        dir.path().join("views/escrow.sql"),
        include_str!("../tests/fixtures/network-nest/views/10-escrow.sql"),
    )
    .unwrap();
    let payer = format!("0x{}", "11".repeat(20));
    let receiver = format!("0x{}", "22".repeat(20));
    let signer = format!("0x{}", "33".repeat(20));
    let service = format!("0x{}", "44".repeat(20));
    let allocation = format!("0x{}", "55".repeat(20));
    let tx = format!("0x{}", "66".repeat(32));
    let collector = "0x8f69f5c07477ac46fbc491b1e6d91e2bb0111a9e";
    let account = format!("{payer}{}{}", &collector[2..], &receiver[2..]);
    let mut hot = analytics::HotRows::new();
    let mut event = |table: &str, block: u64, index: u64, fields: Value| {
        let mut row = json!({"block_number":block, "log_index":index, "block_timestamp":1000+block,
            "tx_hash":tx, "payer":payer, "collector":collector, "receiver":receiver});
        row.as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        hot.entry(table.into()).or_default().push(row);
    };
    // The 38-digit line, where Burrmill refuses past it; exact to the last digit.
    let large = "99999999999999999999999999999999999999";
    event("escrow__deposit", 10, 1, json!({"tokens":large}));
    event(
        "escrow__thaw",
        11,
        1,
        json!({"tokens":"4","thawEndTimestamp":"2000"}),
    );
    event("escrow__escrow_collected", 12, 1, json!({"tokens":"2"}));
    event(
        "tally__payment_collected",
        12,
        258,
        json!({"tokens":"2","dataService":service,
        "collectionId":format!("0x{}{}", "00".repeat(12), &allocation[2..])}),
    );
    event(
        "tally__signer_authorized",
        10,
        2,
        json!({"signer":signer,"authorizer":payer}),
    );
    event(
        "tally__signer_thawing",
        11,
        2,
        json!({"signer":signer,"authorizer":payer,"thawEndTimestamp":"2000"}),
    );
    // Re-authorisation preserves the existing thaw deadline in the upstream mapping.
    event(
        "tally__signer_authorized",
        12,
        3,
        json!({"signer":signer,"authorizer":payer}),
    );
    event("escrow__withdraw", 13, 1, json!({"tokens":"4"}));
    event(
        "tally__signer_revoked",
        13,
        2,
        json!({"signer":signer,"authorizer":payer}),
    );
    let query = |block, sql| {
        let out = analytics::query_hot_cold_at(
            dir.path(),
            sql,
            analytics::QueryGuard {
                timeout: REPLAY_QUERY_TIMEOUT,
                max_rows: 100,
                ..Default::default()
            },
            &hot,
            0,
            &schema,
            block,
        )
        .unwrap();
        assert!(!out.degraded() && !out.truncated);
        out.rows
    };
    let row = &query(10, "SELECT * FROM payments_escrow_account")[0];
    assert_eq!(row["id"], account);
    assert_eq!(row["balance"], large);
    assert_eq!(row["totalAmountThawing"], "0");
    let row = &query(12, "SELECT * FROM payments_escrow_account")[0];
    assert_eq!(row["balance"], "99999999999999999999999999999999999997");
    assert_eq!(row["totalAmountThawing"], "4");
    let row = &query(12, "SELECT * FROM signer")[0];
    assert_eq!(row["isAuthorized"], true);
    assert_eq!(row["thawEndTimestamp"], "2000");
    let row = &query(13, "SELECT * FROM signer")[0];
    assert_eq!(row["isAuthorized"], false);
    assert_eq!(row["thawEndTimestamp"], "0");
    let row = &query(13, "SELECT * FROM payments_escrow_account")[0];
    assert_eq!(row["balance"], "99999999999999999999999999999999999993");
    assert_eq!(row["totalAmountThawing"], "0");
    let transactions = query(
        12,
        "SELECT * FROM payments_escrow_transaction WHERE type = 'redeem'",
    );
    assert_eq!(transactions.len(), 1);
    assert_eq!(transactions[0]["id"], format!("{tx}02010000"));
    assert_eq!(transactions[0]["allocationId"], allocation);
    assert_eq!(transactions[0]["collector"], service);
    assert_eq!(transactions[0]["escrowAccount"], account);
    assert_eq!(transactions[0]["timestamp"], "1012");
    assert!(query(
        11,
        "SELECT * FROM payments_escrow_transaction WHERE type = 'redeem'"
    )
    .is_empty());
}

#[test]
fn allocation_lifecycle_keeps_legacy_history_and_uses_the_closure_block_epoch() {
    let _replay = replay_slot();
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/network-nest");
    let config = Config::load(&source).unwrap();
    let mut schema = registry::from_nest(&source, &config).unwrap().schema();
    schema.extend(nuthatch::calls::schema(&config.calls, true));
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("views")).unwrap();
    for entry in std::fs::read_dir(source.join("views")).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(
            entry.path(),
            dir.path().join("views").join(entry.file_name()),
        )
        .unwrap();
    }
    let mut hot = analytics::HotRows::new();
    let mut event = |table: &str, block: u64, fields: Value| {
        let mut row = json!({"block_number":block,"block_hash":format!("hash{block}"),
            "log_index":1,"block_timestamp":1000+block});
        row.as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        hot.entry(table.into()).or_default().push(row);
    };
    event(
        "staking_legacy__allocation_created",
        10,
        json!({"allocationID":"legacy", "indexer":"i", "subgraphDeploymentID":"d", "tokens":"20", "epoch":"1"}),
    );
    event(
        "staking_legacy__allocation_closed_7203",
        15,
        json!({"allocationID":"legacy", "epoch":"2", "poi":"legacy-poi", "effectiveAllocation":"9"}),
    );
    event(
        "subgraph_service__allocation_created",
        20,
        json!({"allocationId":"new", "indexer":"i", "subgraphDeploymentId":"d", "tokens":"30", "currentEpoch":"3"}),
    );
    event(
        "subgraph_service__allocation_resized",
        21,
        json!({"allocationId":"new", "newTokens":"40"}),
    );
    event(
        "subgraph_service__p_o_i_presented",
        22,
        json!({"allocationId":"new", "poi":"new-poi"}),
    );
    event(
        "subgraph_service__indexing_rewards_collected",
        21,
        json!({"allocationId":"new", "tokensRewards":"7", "poi":"pre-reo-poi", "log_index":2}),
    );
    event(
        "subgraph_service__indexing_rewards_collected",
        23,
        json!({"allocationId":"new", "tokensRewards":"8", "poi":"must-not-overwrite"}),
    );
    // No IndexingRewardsCollected event accompanies this forced close.
    event(
        "subgraph_service__allocation_closed",
        25,
        json!({"allocationId":"new", "forceClosed":true}),
    );
    event(
        "allocation_close_epoch",
        25,
        json!({"result":format!("0x{:064x}",4), "reverted":false}),
    );
    for (l2, l1) in [(10, 100), (15, 103), (20, 105), (25, 107)] {
        event(
            "epoch_manager_l1_block",
            l2,
            json!({"result":format!("0x{l1:064x}"),"reverted":false}),
        );
    }
    let query = |block, sql| {
        analytics::query_hot_cold_at(
            dir.path(),
            sql,
            analytics::QueryGuard {
                timeout: REPLAY_QUERY_TIMEOUT,
                max_rows: 100,
                ..Default::default()
            },
            &hot,
            0,
            &schema,
            block,
        )
        .unwrap()
        .rows
    };
    let old = query(12, "SELECT * FROM allocation WHERE id = 'legacy'");
    assert_eq!(old[0]["status"], "Active");
    assert_eq!(old[0]["isLegacy"], true);
    assert_eq!(old[0]["createdAtEpoch"], 1);
    assert_eq!(old[0]["createdAtBlockNumber"], 100);
    assert!(old[0]["closedAtEpoch"].is_null());
    let old = query(15, "SELECT * FROM allocation WHERE id = 'legacy'");
    assert_eq!(old[0]["status"], "Closed");
    assert_eq!(old[0]["closedAtEpoch"], 2);
    assert_eq!(old[0]["closedAtBlockNumber"], 103);
    assert_eq!(old[0]["effectiveAllocation"], "9");
    assert_eq!(old[0]["poi"], "legacy-poi");
    assert!(old[0]["forceClosed"].is_null());
    let active = query(21, "SELECT * FROM allocation WHERE id = 'new'");
    assert_eq!(active[0]["allocatedTokens"], "40");
    assert_eq!(active[0]["activeForIndexer"], "i");
    assert_eq!(active[0]["isLegacy"], false);
    assert_eq!(active[0]["poi"], "pre-reo-poi");
    assert_eq!(
        query(23, "SELECT poi FROM allocation WHERE id = 'new'")[0]["poi"],
        "new-poi"
    );
    let closed = query(25, "SELECT * FROM allocation WHERE id = 'new'");
    assert_eq!(closed[0]["status"], "Closed");
    assert_eq!(closed[0]["closedAtEpoch"], 4);
    assert_eq!(closed[0]["createdAtBlockNumber"], 105);
    assert_eq!(closed[0]["closedAtBlockNumber"], 107);
    assert_eq!(closed[0]["poi"], "new-poi");
    assert_eq!(closed[0]["forceClosed"], true);
    assert!(closed[0]["activeForIndexer"].is_null());
    hot.remove("allocation_close_epoch");
    let missing = analytics::query_hot_cold_at(
        dir.path(),
        "SELECT \"closedAtEpoch\" FROM allocation WHERE id = 'new'",
        analytics::QueryGuard {
            timeout: REPLAY_QUERY_TIMEOUT,
            max_rows: 100,
            ..Default::default()
        },
        &hot,
        0,
        &schema,
        25,
    );
    let error = format!("{:#}", missing.unwrap_err());
    assert!(error.contains("missing pinned EpochManager"), "{error}");
}
