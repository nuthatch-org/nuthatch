//! RFC-0044 S2: a mapping with one `eth_call` must emit a `[[calls]]` whose signature traces to
//! that line; an Exact field must appear in a view; dropping the call must drop the stanza.

use std::path::{Path, PathBuf};

fn one_call_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("skills/nuthatch-subgraph-port/fixtures/one-call")
}

fn four_classes_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("skills/nuthatch-subgraph-port/fixtures/four-classes")
}

const FACTORY_ABI: &str = r#"[{"type":"event","name":"PoolCreated","anonymous":false,"inputs":[
    {"name":"token0","type":"address","indexed":true},
    {"name":"token1","type":"address","indexed":true},
    {"name":"fee","type":"uint24","indexed":true},
    {"name":"tickSpacing","type":"int24","indexed":false},
    {"name":"pool","type":"address","indexed":false}]}]"#;

const POOL_ABI: &str = r#"[{"type":"event","name":"Swap","anonymous":false,"inputs":[
    {"name":"sender","type":"address","indexed":true},
    {"name":"recipient","type":"address","indexed":true},
    {"name":"amount0","type":"int256","indexed":false},
    {"name":"amount1","type":"int256","indexed":false},
    {"name":"sqrtPriceX96","type":"uint160","indexed":false},
    {"name":"liquidity","type":"uint128","indexed":false},
    {"name":"tick","type":"int24","indexed":false}]}]"#;

fn write_imported_nest(dir: &Path, with_pool: bool) {
    std::fs::create_dir_all(dir.join("abis")).unwrap();
    std::fs::write(dir.join("abis/factory.json"), FACTORY_ABI).unwrap();
    let mut toml = String::from(
        r#"[nest]
name = "port-emit-test"
chain = "arbitrum-one"
chain_id = 42161
rpc_urls = ["http://127.0.0.1:1"]

[[contracts]]
alias = "factory"
address = "0x1f98431c8ad98523631ae4a59f267346ea31f984"
start_block = 1
abi = "abis/factory.json"
events = ["PoolCreated"]
"#,
    );
    if with_pool {
        std::fs::write(dir.join("abis/pool.json"), POOL_ABI).unwrap();
        toml.push_str(
            r#"
[[templates]]
name = "pool"
abi = "abis/pool.json"
events = ["Swap"]

[[factories]]
watch = "factory"
event = "PoolCreated"
child_param = "pool"
template = "pool"
"#,
        );
    }
    std::fs::write(dir.join("nuthatch.toml"), toml).unwrap();
    nuthatch::project::regen(nuthatch::cli::SchemaArgs {
        dir: dir.display().to_string(),
    })
    .expect("regen nest artifacts");
}

fn select_sql(sql: &str) -> String {
    sql.lines()
        .filter(|l| !l.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let to = dst.join(entry.file_name());
        if entry.path().is_dir() {
            copy_dir(&entry.path(), &to);
        } else {
            std::fs::copy(entry.path(), to).unwrap();
        }
    }
}

#[test]
fn one_eth_call_emits_calls_whose_signature_traces_to_that_line() {
    let nest = tempfile::tempdir().unwrap();
    write_imported_nest(nest.path(), false);
    let result = nuthatch::port_emit::emit(&one_call_dir(), nest.path()).expect("emit");

    assert_eq!(
        result.calls.len(),
        1,
        "one try_symbol must produce one [[calls]], got {:?}",
        result
            .calls
            .iter()
            .map(|c| &c.decl.name)
            .collect::<Vec<_>>()
    );
    let call = &result.calls[0];
    assert_eq!(
        call.decl.signature.as_deref(),
        Some("symbol()"),
        "signature must be the method try_symbol called, not a guess"
    );
    assert_eq!(call.decl.on.as_deref(), Some("factory__pool_created"));
    assert_eq!(call.decl.contract_column.as_deref(), Some("{token0}"));
    assert!(
        call.citation.file.ends_with("core.ts"),
        "citation file {}",
        call.citation.file
    );
    assert_eq!(
        call.citation.line, 8,
        "must cite the try_symbol line in fixtures/one-call/src/mappings/core.ts, got {}",
        call.citation.line
    );

    let toml = std::fs::read_to_string(nest.path().join("nuthatch.toml")).unwrap();
    assert!(toml.contains("[[calls]]"), "{toml}");
    assert!(toml.contains("signature = \"symbol()\""), "{toml}");
    assert!(toml.contains("on = \"factory__pool_created\""), "{toml}");
}

#[test]
fn exact_field_appears_in_a_view() {
    let nest = tempfile::tempdir().unwrap();
    write_imported_nest(nest.path(), false);
    let result = nuthatch::port_emit::emit(&one_call_dir(), nest.path()).unwrap();
    let token = result
        .views
        .iter()
        .find(|v| v.entity == "Token")
        .expect("Token view");
    assert!(
        token.exact_fields.iter().any(|f| f == "id"),
        "Token.id is exact: {:?}",
        token.exact_fields
    );
    let select = select_sql(&token.sql);
    assert!(
        select.contains("AS \"id\""),
        "Token.id must be a SELECT column, not a comment:\n{}",
        token.sql
    );
    assert!(
        select.contains("\"token0\""),
        "constructor id is event.params.token0:\n{}",
        token.sql
    );
    assert!(
        !select.contains("port_placeholder"),
        "Token.id was omitted from the projection:\n{}",
        token.sql
    );
    let on_disk = std::fs::read_to_string(nest.path().join("views").join(&token.file)).unwrap();
    assert!(
        select_sql(&on_disk).contains("AS \"id\""),
        "on-disk view must project id:\n{on_disk}"
    );
    assert!(
        !on_disk.to_ascii_lowercase().contains("symbol")
            || on_disk.contains("Call-derived")
            || on_disk.contains("call-derived"),
        "Token.symbol is call-derived and must not be selected as an exact column:\n{on_disk}"
    );
}

/// A `@derivedFrom` reverse relation is **answered**, and it must never become a column.
///
/// It stores nothing: the value is a reverse lookup that RFC-0053 S2 lowers to a JSON aggregation over
/// the forward reference. Counting it unanswered understated the overlay by 8 of 92 on the pinned target
/// and would have sent a porter hunting for columns that must not exist (#1284).
#[test]
fn a_derived_from_relation_counts_as_answered_and_reaches_no_column() {
    let schema = r#"
type Pair @entity {
  id: ID!
  token0: String!
  trades: [Trade!]! @derivedFrom(field: "pair")
}
type Trade @entity {
  id: ID!
  pair: Pair!
}
"#;
    // Both ends need a view with an `id`: the lookup is a subquery over `trade` keyed on `pair`.
    let mapping = r#"
export function handlePoolCreated(event: PoolCreated): void {
  let pair = new Pair(event.params.pool.toHex())
  pair.token0 = event.params.token0.toHex()
  pair.save()
  let trade = new Trade(event.params.token1.toHex())
  trade.pair = event.params.pool.toHex()
  trade.save()
}
"#;
    let (nest, result) = emitted_nest(schema, mapping);

    assert_eq!(
        result.coverage.derived,
        1,
        "`Pair.trades` is a reverse lookup: {}",
        result.coverage.summary()
    );
    assert!(
        result
            .coverage
            .summary()
            .contains("1 derived by reverse lookup"),
        "the sentence must say so: {}",
        result.coverage.summary()
    );

    // And it is counted as answered rather than as a hole.
    assert_eq!(
        result.coverage.answered(),
        result.coverage.in_views + result.coverage.derived,
        "{}",
        result.coverage.summary()
    );

    // It must not appear in any view's projection: a stored column for a reverse relation would be a
    // second copy of the truth, drifting from the forward reference.
    for v in &result.views {
        assert!(
            !v.exact_fields.iter().any(|f| f == "trades"),
            "{}: `trades` must not be a column\n{}",
            v.entity,
            v.sql
        );
    }
    let readme = std::fs::read_to_string(nest.path().join("README.md")).unwrap();
    assert!(
        readme.contains("derived by reverse lookup"),
        "README must carry the clause:\n{readme}"
    );
}

/// #1947: a reverse lookup over an entity no view answers cannot be asked for. Morpho's line counted all
/// 75 as answered while `{ accounts { deposits { id } } }` failed with `no table deposit`.
#[test]
fn a_derived_from_relation_over_an_entity_with_no_view_is_not_answered() {
    let schema = r#"
type Pair @entity {
  id: ID!
  token0: String!
  trades: [Trade!]! @derivedFrom(field: "pair")
}
type Trade @entity {
  id: ID!
  pair: Pair!
}
"#;
    let mapping = r#"
export function handlePoolCreated(event: PoolCreated): void {
  let pair = new Pair(event.params.pool.toHex())
  pair.token0 = event.params.token0.toHex()
  pair.save()
}
"#;
    let (_nest, result) = emitted_nest(schema, mapping);
    assert!(!result.views.iter().any(|v| v.entity == "Trade"));
    assert_eq!(result.coverage.derived, 0, "{}", result.coverage.summary());
    assert_eq!(
        result.coverage.unanswered(),
        1,
        "`Pair.trades` is promised and unanswerable: {}",
        result.coverage.summary()
    );
}

/// #1947 (S0 defect 4): a view the emitter could not give an `id` binds, and every GraphQL query on it
/// fails. Its fields are not answers.
#[test]
fn a_view_without_an_id_column_does_not_count_as_answered() {
    let schema = r#"
type Pool @entity {
  id: ID!
  plain: BigInt!
}
"#;
    let mapping = r#"
export function handlePoolCreated(event: PoolCreated): void {
  let pool = new Pool(event.transaction.hash.concatI32(event.block.number.toI32()))
  pool.plain = event.params.fee
  pool.save()
}
"#;
    let (nest, result) = emitted_nest(schema, mapping);
    let view = result
        .views
        .iter()
        .find(|v| v.entity == "Pool")
        .expect("view");
    assert!(
        !view.exact_fields.contains(&"id".to_string()),
        "the fixture must yield a view without an id: {:?}",
        view.exact_fields
    );
    assert_eq!(result.views_without_id, vec!["Pool".to_string()]);
    assert_eq!(result.coverage.in_views, 0, "{}", result.coverage.summary());
    assert!(
        !result.coverage.summary().contains("(100%)"),
        "{}",
        result.coverage.summary()
    );
    let readme = std::fs::read_to_string(nest.path().join("README.md")).unwrap();
    assert!(readme.contains("no `id` column"), "{readme}");
}

/// Both spellings of `graph init`'s id render it; anything that is not those bytes leaves the view id-less.
#[cfg(feature = "graph")]
#[test]
fn the_concat_i32_log_index_id_is_rendered_and_near_misses_are_not() {
    let schema = "type Pool @entity {\n  id: Bytes!\n  plain: BigInt!\n}\n";
    let rendered = [
        "event.transaction.hash.concatI32(event.logIndex.toI32())",
        "event.transaction.hash.concatI32( event.logIndex.toI32() )",
        "event.transaction.hash.concat(Bytes.fromI32(event.logIndex.toI32()))",
    ];
    let refused = [
        "event.transaction.hash.concatI32(event.block.number.toI32())",
        "event.transaction.hash.concatI32(event.logIndex.toI32()).toHexString()",
        "event.transaction.hash.toHexString().concat(event.logIndex.toString())",
        "event.transaction.hash.concat(event.params.pool)",
        "event.params.pool.concatI32(event.logIndex.toI32())",
    ];
    for (id, want) in rendered
        .iter()
        .map(|e| (e, true))
        .chain(refused.iter().map(|e| (e, false)))
    {
        let mapping = format!(
            "export function handlePoolCreated(event: PoolCreated): void {{\n  let pool = new Pool({id})\n  pool.plain = event.params.fee\n  pool.save()\n}}\n"
        );
        let (_nest, result) = emitted_nest(schema, &mapping);
        let view = result
            .views
            .iter()
            .find(|v| v.entity == "Pool")
            .expect("view");
        assert_eq!(
            view.exact_fields.contains(&"id".to_string()),
            want,
            "{id}:\n{}",
            view.sql
        );
        if want {
            assert!(
                select_sql(&view.sql).contains("\"tx_hash\" || lpad(to_hex("),
                "{id}:\n{}",
                view.sql
            );
        }
    }
}

/// #1947: from a deployment CID the subgraph directory holds a manifest, a schema and compiled WASM.
/// Nothing is classified, so there is no coverage to report and the overlay must refuse, not print 100%.
#[test]
fn a_subgraph_with_no_mapping_source_is_refused_without_a_coverage_figure() {
    let subgraph = tempfile::tempdir().unwrap();
    std::fs::copy(
        one_call_dir().join("subgraph.yaml"),
        subgraph.path().join("subgraph.yaml"),
    )
    .unwrap();
    std::fs::write(
        subgraph.path().join("schema.graphql"),
        "type Pair @entity {\n  id: ID!\n  trades: [Trade!]! @derivedFrom(field: \"pair\")\n}\n\
         type Trade @entity {\n  id: ID!\n  pair: Pair!\n}\n",
    )
    .unwrap();
    let nest = tempfile::tempdir().unwrap();
    write_imported_nest(nest.path(), false);
    let before = std::fs::read_to_string(nest.path().join("nuthatch.toml")).unwrap();

    let err = nuthatch::port_emit::emit(subgraph.path(), nest.path())
        .expect_err("nothing was classified, so emit must fail");
    let msg = format!("{err:#}");
    assert!(msg.contains("no mapping source"), "{msg}");
    assert!(msg.contains("handlePoolCreated"), "{msg}");
    assert!(!msg.contains('%'), "no coverage figure: {msg}");
    assert!(!nest.path().join("README.md").exists());
    assert_eq!(
        std::fs::read_to_string(nest.path().join("nuthatch.toml")).unwrap(),
        before,
        "the nest must be left as the importer wrote it"
    );
}

/// The manifest for a factory that creates `Pool` children, as a deployed subgraph names it.
const TEMPLATE_MANIFEST: &str = r#"specVersion: 0.0.8
dataSources:
  - kind: ethereum/contract
    name: Factory
    network: arbitrum-one
    source:
      address: "0x1F98431c8aD98523631AE4a59f267346ea31F984"
      abi: Factory
      startBlock: 1
    mapping:
      kind: ethereum/events
      apiVersion: 0.0.7
      language: wasm/assemblyscript
      file: ./src/mappings/core.ts
      entities: [Token]
      abis:
        - name: Factory
          file: ./abis/factory.json
      eventHandlers:
        - event: PoolCreated(indexed address,indexed address,indexed uint24,int24,address)
          handler: handlePoolCreated
templates:
  - kind: ethereum/contract
    name: Pool
    network: arbitrum-one
    source:
      abi: Pool
    mapping:
      kind: ethereum/events
      apiVersion: 0.0.7
      language: wasm/assemblyscript
      file: ./src/mappings/core.ts
      entities: [Token]
      abis:
        - name: Pool
          file: ./abis/pool.json
      eventHandlers:
        - event: Swap(indexed address,indexed address,int256,int256,uint160,uint128,int24)
          handler: handleSwap
"#;

/// A nest as `init --from-subgraph` leaves it when it could not pick the creating event: the template
/// is declared and nothing feeds it.
fn write_nest_with_unwired_template(dir: &Path) {
    write_imported_nest(dir, true);
    let toml = std::fs::read_to_string(dir.join("nuthatch.toml")).unwrap();
    let (head, _) = toml.split_once("[[factories]]").expect("factory stanza");
    std::fs::write(dir.join("nuthatch.toml"), head).unwrap();
}

/// S0 defect 5 (#1941): 9 of 17 templates had no factory rule, and the mapping's
/// `Template.create(event.params.x)` line that decides it was ignored even when the source was there.
#[test]
fn a_template_create_in_the_mapping_wires_the_factory() {
    let subgraph = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(subgraph.path().join("src/mappings")).unwrap();
    std::fs::write(subgraph.path().join("subgraph.yaml"), TEMPLATE_MANIFEST).unwrap();
    std::fs::write(
        subgraph.path().join("schema.graphql"),
        "type Token @entity {\n  id: ID!\n}\n",
    )
    .unwrap();
    std::fs::write(
        subgraph.path().join("src/mappings/core.ts"),
        r#"import { Pool as PoolTemplate } from '../../generated/templates'

export function handlePoolCreated(event: PoolCreated): void {
  let token = new Token(event.params.token0.toHex())
  token.save()
  PoolTemplate.create(event.params.pool)
}

export function handleSwap(event: Swap): void {
}
"#,
    )
    .unwrap();
    let nest = tempfile::tempdir().unwrap();
    write_nest_with_unwired_template(nest.path());

    let result = nuthatch::port_emit::emit(subgraph.path(), nest.path()).expect("emit");
    let config = nuthatch::config::Config::load(nest.path()).unwrap();
    assert_eq!(config.factories.len(), 1, "{:?}", config.factories);
    let f = &config.factories[0];
    assert_eq!(
        (
            f.watch.as_str(),
            f.event.as_str(),
            f.child_param.as_str(),
            f.template.as_str()
        ),
        ("factory", "PoolCreated", "pool", "pool")
    );
    assert_eq!(result.factories.len(), 1);
    assert_eq!(
        result.factories[0].citation.display(),
        "src/mappings/core.ts:6"
    );

    // A second run finds the rule already there and adds nothing.
    let again = nuthatch::port_emit::emit(subgraph.path(), nest.path()).expect("emit");
    assert!(again.factories.is_empty());
    let config = nuthatch::config::Config::load(nest.path()).unwrap();
    assert_eq!(config.factories.len(), 1, "{:?}", config.factories);
}

/// A child address that is not a parameter of the handler's own event is named, never guessed.
#[test]
fn a_template_create_from_a_value_that_is_not_an_event_param_is_named_not_wired() {
    let subgraph = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(subgraph.path().join("src/mappings")).unwrap();
    std::fs::write(subgraph.path().join("subgraph.yaml"), TEMPLATE_MANIFEST).unwrap();
    std::fs::write(
        subgraph.path().join("schema.graphql"),
        "type Token @entity {\n  id: ID!\n}\n",
    )
    .unwrap();
    std::fs::write(
        subgraph.path().join("src/mappings/core.ts"),
        r#"export function handlePoolCreated(event: PoolCreated): void {
  let token = new Token(event.params.token0.toHex())
  token.save()
  Pool.create(Factory.bind(event.address).getPool(event.params.token0))
}
"#,
    )
    .unwrap();
    let nest = tempfile::tempdir().unwrap();
    write_nest_with_unwired_template(nest.path());

    let result = nuthatch::port_emit::emit(subgraph.path(), nest.path()).expect("emit");
    let config = nuthatch::config::Config::load(nest.path()).unwrap();
    assert!(config.factories.is_empty(), "{:?}", config.factories);
    assert!(result.factories.is_empty());
    assert_eq!(
        result.unwired_templates.len(),
        1,
        "{:?}",
        result.unwired_templates
    );
    assert_eq!(result.unwired_templates[0].template, "Pool");
}

/// The invariant the coverage figure rests on: a view projects exactly the fields it lists.
///
/// `EmittedView::exact_fields` feeds two things - the generated check's projection and the coverage
/// figure - so a field listed but not projected reads as verified *and* as answered while DuckDB
/// cannot answer it. That divergence is #1248. It is now impossible by construction, because the list
/// is the key set of the same map the SQL is rendered from, and this test is what holds that true if
/// anyone reintroduces a parallel list.
#[test]
fn every_emitted_view_projects_exactly_the_fields_it_lists() {
    /// Names the outer projection exposes, for both shapes `exact_select_sql` emits: the grouped fold
    /// (`last(...) AS "x"` over a union, with a bare `"id"`) and the single-arm form (`"col" AS "x"`).
    /// `__present__` markers are internal and not fields.
    fn projected(sql: &str) -> std::collections::BTreeSet<String> {
        let body = sql
            .split_once(" AS\n")
            .map(|(_, rest)| rest)
            .unwrap_or(sql)
            .trim_start();
        let head = body
            .split_once("\nFROM")
            .map(|(h, _)| h)
            .unwrap_or(body)
            .trim_start()
            .trim_start_matches("SELECT");
        let mut out = std::collections::BTreeSet::new();
        for part in head.split(",\n") {
            let part = part.trim().trim_end_matches(';');
            let name = match part.rsplit_once(" AS ") {
                Some((_, alias)) => alias.trim(),
                // a bare projected column, which is how `id` is emitted in the fold
                None => part,
            };
            let name = name.trim().trim_matches('"');
            if name.is_empty()
                || name.starts_with("__present__")
                || !name.contains(char::is_alphabetic)
            {
                continue;
            }
            out.insert(name.to_string());
        }
        out
    }

    // Three shapes: one field and one table, a fold over several fields, and an overlay with a
    // running total split out to an incremental entity.
    for (label, schema, mapping) in [
        ("one field", None, None),
        ("accumulator split", Some(ACCUM_SCHEMA), Some(ACCUM_MAPPING)),
        ("operations", Some(OPS_SCHEMA), Some(OPS_MAPPING)),
    ] {
        let (_nest, result) = match (schema, mapping) {
            (Some(s), Some(m)) => emitted_nest(s, m),
            _ => {
                let nest = tempfile::tempdir().unwrap();
                write_imported_nest(nest.path(), false);
                let r = nuthatch::port_emit::emit(&one_call_dir(), nest.path()).unwrap();
                (nest, r)
            }
        };
        assert!(
            !result.views.is_empty(),
            "{label}: nothing was emitted, so this case proves nothing"
        );
        for v in &result.views {
            let listed: std::collections::BTreeSet<String> =
                v.exact_fields.iter().cloned().collect();
            let select = v
                .sql
                .split_once("CREATE VIEW")
                .map(|(_, rest)| rest)
                .unwrap_or_else(|| panic!("{label}/{}: no CREATE VIEW:\n{}", v.entity, v.sql));
            assert_eq!(
                projected(select),
                listed,
                "{label}/{}: the projection and `exact_fields` disagree\n{}",
                v.entity,
                v.sql
            );
            assert!(
                !listed.is_empty(),
                "{label}/{}: an emitted view with no listed field",
                v.entity
            );
        }
    }
}

/// `README.md` is the report **plus what this overlay actually answers**.
///
/// It used to be the report verbatim, which is what this test asserted. The report's summary table
/// describes the mapping - a field is `exact` when it is a pure function of decoded events, a claim
/// about reproducibility in principle. On the RFC-0044 S3 acceptance port it said `exact 205` while
/// the overlay answered **27**, and nothing on disk or on stdout said so, so the artefacts read as a
/// finished port (#1277).
#[test]
fn readme_carries_the_report_and_the_overlay_coverage() {
    let nest = tempfile::tempdir().unwrap();
    write_imported_nest(nest.path(), false);
    let result = nuthatch::port_emit::emit(&one_call_dir(), nest.path()).unwrap();
    let readme = std::fs::read_to_string(nest.path().join("README.md")).unwrap();

    assert!(
        readme.starts_with(&result.report),
        "the report must still be there, whole and first:\n{readme}"
    );
    assert!(readme.contains("# Port report"));
    assert!(readme.contains("call-derived"));
    assert!(readme.contains("`Token.symbol`"));

    // This fixture has one exact field (`Token.id`) and the view answers it, so the honest figure is
    // 1 of 1. Asserted as the rendered sentence rather than as `contains("coverage")`, because the
    // sentence is what a porter reads and a count that drifts from the artefacts is the whole defect.
    assert_eq!(
        result.coverage,
        nuthatch::port_emit::Coverage {
            classified_exact: 1,
            in_views: 1,
            incremental: 0,
            derived: 0,
        },
        "coverage must be counted from the artefacts"
    );
    assert!(
        readme.contains(
            "1 of 1 fields the report calls exact are answered (100%): 1 in views, 0 maintained \
             incrementally, 0 derived by reverse lookup, 0 not answered at all"
        ),
        "README must carry the figure as a sentence:\n{readme}"
    );
    assert!(
        readme.contains("## Overlay coverage"),
        "and under a heading a reader can find:\n{readme}"
    );
}

/// The figure has to move when the overlay gets worse, or it is decoration.
///
/// `Token.tally` is written by the mapping from an arithmetic expression over an event parameter, so
/// the classifier calls it exact - a pure function of decoded events - and the emitter cannot render
/// it into a column. That is the exact shape of the 178 fields the S3 acceptance port promised and
/// could not answer (#1277), and the coverage line must stop reading 100%.
#[test]
fn an_unanswered_exact_field_moves_the_coverage_figure() {
    let subgraph = tempfile::tempdir().unwrap();
    copy_dir(&one_call_dir(), subgraph.path());

    let schema = subgraph.path().join("schema.graphql");
    let text = std::fs::read_to_string(&schema).unwrap();
    let text = text.replace("  symbol: String!", "  symbol: String!\n  tally: BigInt!");
    assert!(text.contains("tally"), "schema edit did not apply");
    std::fs::write(&schema, &text).unwrap();

    let mapping = subgraph.path().join("src/mappings/core.ts");
    let m = std::fs::read_to_string(&mapping).unwrap();
    // Asserted rather than assumed: the fixture binds `token0`, not `token`, and an edit that matches
    // nothing leaves a field the classifier calls *unreachable* instead of exact - which moves no
    // coverage and makes this test vacuous. It happened while writing it.
    let m2 = m.replace(
        "  token0.save()",
        "  token0.tally = event.params.fee.plus(event.params.fee)\n  token0.save()",
    );
    assert_ne!(m2, m, "mapping edit did not apply:\n{m}");
    std::fs::write(&mapping, &m2).unwrap();

    let nest = tempfile::tempdir().unwrap();
    write_imported_nest(nest.path(), false);
    let result = nuthatch::port_emit::emit(subgraph.path(), nest.path()).unwrap();
    let readme = std::fs::read_to_string(nest.path().join("README.md")).unwrap();

    assert_eq!(
        result.coverage.classified_exact,
        2,
        "the fixture must promise two exact fields, got {}",
        result.coverage.summary()
    );
    assert_eq!(
        result.coverage.unanswered(),
        1,
        "and exactly one must be unanswered, got {}",
        result.coverage.summary()
    );
    assert!(
        readme.contains("1 of 2 fields the report calls exact are answered (50%)"),
        "README must carry the moved figure:\n{}",
        result.coverage.summary()
    );
    assert!(
        !readme.contains("(100%)"),
        "an overlay with an unanswered exact field must not read 100%:\n{}",
        result.coverage.summary()
    );
}

#[test]
fn dropping_the_call_from_the_mapping_drops_the_calls_stanza() {
    let subgraph = tempfile::tempdir().unwrap();
    copy_dir(&one_call_dir(), subgraph.path());
    let mapping = subgraph.path().join("src/mappings/core.ts");
    let original = std::fs::read_to_string(&mapping).unwrap();
    assert!(
        original.contains("try_symbol"),
        "fixture must contain the call this test drops"
    );
    let mutated = original.replace(
        "  let contract = ERC20.bind(tokenAddress)\n  let result = contract.try_symbol()\n  if (!result.reverted) {\n    return result.value\n  }\n  return 'unknown'\n",
        "  return 'unknown'\n",
    );
    assert!(!mutated.contains("try_symbol"), "{mutated}");
    std::fs::write(&mapping, mutated).unwrap();

    let nest = tempfile::tempdir().unwrap();
    write_imported_nest(nest.path(), false);
    let result = nuthatch::port_emit::emit(subgraph.path(), nest.path()).unwrap();
    assert!(
        result.calls.is_empty(),
        "dropping try_symbol must vanish the [[calls]], got {:?}",
        result
            .calls
            .iter()
            .map(|c| (c.decl.signature.clone(), c.citation.display()))
            .collect::<Vec<_>>()
    );
    let toml = std::fs::read_to_string(nest.path().join("nuthatch.toml")).unwrap();
    assert!(
        !toml.contains("[[calls]]"),
        "nuthatch.toml must not invent a [[calls]] after the mapping call was dropped:\n{toml}"
    );
}

#[test]
fn parameterized_call_is_refused_but_the_rest_of_the_overlay_still_lands() {
    let subgraph = tempfile::tempdir().unwrap();
    copy_dir(&one_call_dir(), subgraph.path());
    let mapping = subgraph.path().join("src/mappings/core.ts");
    let original = std::fs::read_to_string(&mapping).unwrap();
    std::fs::write(
        &mapping,
        original.replace(
            "contract.try_symbol()",
            "contract.try_balanceOf(event.params.tokenId)",
        ),
    )
    .unwrap();

    let nest = tempfile::tempdir().unwrap();
    write_imported_nest(nest.path(), false);
    let result = nuthatch::port_emit::emit(subgraph.path(), nest.path())
        .expect("one unpinnable read must not cost the author the whole overlay");

    // Still refused, and still not guessed.
    assert!(
        result.calls.is_empty(),
        "a parameterized read has no ABI parameter types, so no signature may be emitted: {:?}",
        result
            .calls
            .iter()
            .map(|c| &c.decl.name)
            .collect::<Vec<_>>()
    );
    let skipped = result
        .skipped_calls
        .iter()
        .find(|s| s.signature.contains("balanceOf"))
        .expect("the skipped read must be reported by name, not dropped silently");
    assert!(skipped.why.contains("refusing to guess"), "{}", skipped.why);
    assert!(
        skipped.citation.file.contains("core.ts"),
        "{:?}",
        skipped.citation
    );

    // And the rest of the port is there, which is what aborting used to destroy.
    assert!(!result.views.is_empty(), "views must still be emitted");
    assert!(
        nest.path().join("README.md").exists(),
        "the README carrying the port report must still be written"
    );
    assert!(
        nest.path().join("checks/port_views.sql").exists(),
        "the checks must still be written"
    );
}

#[test]
fn four_classes_exact_sqrt_price_lands_in_a_view() {
    let nest = tempfile::tempdir().unwrap();
    write_imported_nest(nest.path(), true);
    let result = nuthatch::port_emit::emit(&four_classes_dir(), nest.path()).unwrap();
    let pool = result
        .views
        .iter()
        .find(|v| v.entity == "Pool")
        .expect("Pool view");
    assert!(
        pool.exact_fields.iter().any(|f| f == "sqrtPrice"),
        "Pool.sqrtPrice is exact: {:?}",
        pool.exact_fields
    );
    assert!(
        pool.sql.contains("sqrtPrice") || pool.sql.contains("sqrt_price"),
        "Pool.sqrtPrice must appear in a view:\n{}",
        pool.sql
    );
    assert!(
        result
            .calls
            .iter()
            .any(|c| c.decl.signature.as_deref() == Some("symbol()")),
        "four-classes still has try_symbol: {:?}",
        result
            .calls
            .iter()
            .map(|c| &c.decl.signature)
            .collect::<Vec<_>>()
    );
    assert!(
        result
            .calls
            .iter()
            .any(|c| c.decl.signature.as_deref() == Some("decimals()")),
        "four-classes still has try_decimals"
    );
}

#[test]
fn two_triggering_tables_keep_every_exact_field() {
    let subgraph = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(subgraph.path().join("src/mappings")).unwrap();
    std::fs::write(
        subgraph.path().join("schema.graphql"),
        r#"type Token @entity {
  id: ID!
  symbol: String!
  name: String!
}
"#,
    )
    .unwrap();
    std::fs::write(
        subgraph.path().join("subgraph.yaml"),
        r#"specVersion: 0.0.8
dataSources:
  - kind: ethereum/contract
    name: Factory
    network: arbitrum-one
    source:
      address: "0x1F98431c8aD98523631AE4a59f267346ea31F984"
      abi: Factory
      startBlock: 1
    mapping:
      kind: ethereum/events
      apiVersion: 0.0.7
      language: wasm/assemblyscript
      file: ./src/mappings/core.ts
      entities: [Token]
      abis:
        - name: Factory
          file: ./abis/factory.json
      eventHandlers:
        - event: PoolCreated(indexed address,indexed address,indexed uint24,int24,address)
          handler: handlePoolCreated
        - event: TokenUpdated(indexed address,string)
          handler: handleTokenUpdated
"#,
    )
    .unwrap();
    std::fs::write(
        subgraph.path().join("src/mappings/core.ts"),
        r#"import { PoolCreated, TokenUpdated } from '../../generated/Factory/Factory'
import { Token } from '../../generated/schema'

export function handlePoolCreated(event: PoolCreated): void {
  let token = new Token(event.params.token0.toHex())
  token.symbol = event.params.token0.toHex()
  token.save()
}

export function handleTokenUpdated(event: TokenUpdated): void {
  let token = Token.load(event.params.token.toHex())!
  token.name = event.params.name
  token.save()
}
"#,
    )
    .unwrap();

    let nest = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(nest.path().join("abis")).unwrap();
    let abi = r#"[{"type":"event","name":"PoolCreated","anonymous":false,"inputs":[
    {"name":"token0","type":"address","indexed":true},
    {"name":"token1","type":"address","indexed":true},
    {"name":"fee","type":"uint24","indexed":true},
    {"name":"tickSpacing","type":"int24","indexed":false},
    {"name":"pool","type":"address","indexed":false}]},
{"type":"event","name":"TokenUpdated","anonymous":false,"inputs":[
    {"name":"token","type":"address","indexed":true},
    {"name":"name","type":"string","indexed":false}]}]"#;
    std::fs::write(nest.path().join("abis/factory.json"), abi).unwrap();
    std::fs::write(
        nest.path().join("nuthatch.toml"),
        r#"[nest]
name = "port-emit-two-tables"
chain = "arbitrum-one"
chain_id = 42161
rpc_urls = ["http://127.0.0.1:1"]

[[contracts]]
alias = "factory"
address = "0x1f98431c8ad98523631ae4a59f267346ea31f984"
start_block = 1
abi = "abis/factory.json"
events = ["PoolCreated", "TokenUpdated"]
"#,
    )
    .unwrap();
    nuthatch::project::regen(nuthatch::cli::SchemaArgs {
        dir: nest.path().display().to_string(),
    })
    .expect("regen nest artifacts");

    let result = nuthatch::port_emit::emit(subgraph.path(), nest.path()).unwrap();
    let token = result
        .views
        .iter()
        .find(|v| v.entity == "Token")
        .expect("Token view");
    assert!(
        token.exact_fields.iter().any(|f| f == "symbol"),
        "Token.symbol is exact: {:?}",
        token.exact_fields
    );
    assert!(
        token.exact_fields.iter().any(|f| f == "name"),
        "Token.name is exact: {:?}",
        token.exact_fields
    );
    let select = select_sql(&token.sql);
    assert!(
        select.contains("AS \"symbol\""),
        "Token.symbol must be a SELECT column:\n{}",
        token.sql
    );
    assert!(
        select.contains("AS \"name\""),
        "Token.name must be a SELECT column:\n{}",
        token.sql
    );
    assert!(
        select.contains("factory__pool_created"),
        "PoolCreated table must appear:\n{}",
        token.sql
    );
    assert!(
        select.contains("factory__token_updated"),
        "TokenUpdated table must appear:\n{}",
        token.sql
    );
    assert!(
        select.contains("UNION ALL"),
        "two tables overlay with UNION ALL, not a guessed JOIN:\n{}",
        token.sql
    );
    assert!(
        select.contains("GROUP BY") && (select.contains("last(") || select.contains("arg_max")),
        "entity view must fold to last-per-id, not raw event rows:\n{}",
        token.sql
    );
    assert!(
        !select.to_ascii_lowercase().contains(" join "),
        "must not invent a JOIN:\n{}",
        token.sql
    );
}

#[test]
fn emitted_nest_loads_and_views_validate() {
    let nest = tempfile::tempdir().unwrap();
    write_imported_nest(nest.path(), false);
    nuthatch::port_emit::emit(&one_call_dir(), nest.path()).unwrap();
    nuthatch::config::Config::load(nest.path())
        .expect("emitted nest must load without hand editing");
    let result = nuthatch::check::check(nuthatch::cli::CheckArgs {
        name: None,
        dir: nest.path().display().to_string(),
        update: false,
        #[cfg(feature = "folds")]
        folds: false,
        #[cfg(feature = "folds")]
        from_genesis: false,
    });
    assert!(
        result.is_ok(),
        "emitted nest must pass nuthatch check: {result:?}"
    );
}

// ---------------------------------------------------------------------------------------------
// #1248: an Exact field must be answered correctly or named, never answered wrongly.
// ---------------------------------------------------------------------------------------------

/// A throwaway subgraph whose mappings are supplied inline. The fixtures on disk are the curated
/// ports; these cases are about right-hand sides the emitter must refuse, so they belong with the
/// test rather than in the fixture set the drift gate watches.
fn subgraph_with(schema: &str, mapping: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("src/mappings")).unwrap();
    std::fs::copy(
        one_call_dir().join("subgraph.yaml"),
        dir.path().join("subgraph.yaml"),
    )
    .unwrap();
    std::fs::write(dir.path().join("schema.graphql"), schema).unwrap();
    std::fs::write(dir.path().join("src/mappings/core.ts"), mapping).unwrap();
    dir
}

fn emitted_nest(
    schema: &str,
    mapping: &str,
) -> (tempfile::TempDir, nuthatch::port_emit::EmitResult) {
    let subgraph = subgraph_with(schema, mapping);
    let nest = tempfile::tempdir().unwrap();
    write_imported_nest(nest.path(), false);
    let result = nuthatch::port_emit::emit(subgraph.path(), nest.path()).expect("emit");
    (nest, result)
}

const OPS_SCHEMA: &str = r#"
type Pool @entity {
  id: ID!
  plain: BigInt!
  negated: BigInt!
  scaled: BigInt!
  summed: BigInt!
}
"#;

/// Every parameter here is one that `snake_case` maps to itself (`fee`, `token1`, `pool`).
/// That is deliberate: a camelCase parameter emits a column name the decoded table does not have,
/// so the view fails to bind for a reason that has nothing to do with what these tests assert.
/// That is #1250, filed separately and fixed separately.
const OPS_MAPPING: &str = r#"
export function handlePoolCreated(event: PoolCreated): void {
  let pool = new Pool(event.params.pool.toHex())
  pool.id = event.params.pool.toHex()
  pool.plain = event.params.fee
  pool.negated = event.params.token1.neg()
  pool.scaled = event.params.token1.times(BigInt.fromI32(1000))
  pool.summed = event.params.token1.plus(event.params.fee)
  pool.save()
}
"#;

#[test]
fn an_operation_on_an_event_param_is_never_answered_by_the_bare_column() {
    let (_nest, result) = emitted_nest(OPS_SCHEMA, OPS_MAPPING);
    let view = result
        .views
        .iter()
        .find(|v| v.entity == "Pool")
        .expect("a Pool view");
    let sql = select_sql(&view.sql);

    // The defect: each of these resolved to `"fee"`, so the view answered with the wrong sign, a
    // factor of 1000 out, or an addend short - under a report promising byte-identical.
    for field in ["negated", "scaled", "summed"] {
        assert!(
            !sql.contains(&format!("AS \"{field}\"")),
            "`{field}` applies an operation the emitter cannot render, so it must not be projected \
             at all; the view answered it anyway:\n{sql}"
        );
        assert!(
            !view.exact_fields.contains(&field.to_string()),
            "`{field}` is not in the view, so exact_fields must not advertise it: {:?}",
            view.exact_fields
        );
        assert!(
            result
                .skipped_fields
                .iter()
                .any(|s| s.entity == "Pool" && s.field == field),
            "`{field}` must be named in skipped_fields, not silently dropped: {:?}",
            result
                .skipped_fields
                .iter()
                .map(|s| s.name())
                .collect::<Vec<_>>()
        );
    }

    // And the field that genuinely is a bare parameter still lands, so this is a refusal of
    // unrenderable expressions and not a refusal of everything.
    assert!(
        sql.contains("AS \"plain\""),
        "a bare event param must still be projected:\n{sql}"
    );
    assert!(view.exact_fields.contains(&"plain".to_string()));
}

#[test]
fn an_exact_field_that_reaches_no_column_is_named_rather_than_dropped() {
    let schema = r#"
type Pool @entity {
  id: ID!
  accumulated: BigInt!
}
"#;
    // The operand is itself an expression, so this is neither a renderable view column nor an
    // accumulation #1214 can turn into `sum(col)`: `accumulation` takes one flat argument only,
    // precisely so an operand it cannot render does not become a sum over the wrong thing.
    let mapping = r#"
export function handlePoolCreated(event: PoolCreated): void {
  let pool = new Pool(event.params.pool.toHex())
  pool.id = event.params.pool.toHex()
  pool.accumulated = pool.accumulated.plus(event.params.fee.times(BigInt.fromI32(2)))
  pool.save()
}
"#;
    let (_nest, result) = emitted_nest(schema, mapping);
    assert!(
        result
            .skipped_fields
            .iter()
            .any(|s| s.entity == "Pool" && s.field == "accumulated"),
        "an accumulation reaches no column and must be named: {:?}",
        result
            .skipped_fields
            .iter()
            .map(|s| s.name())
            .collect::<Vec<_>>()
    );
    let view = result.views.iter().find(|v| v.entity == "Pool").unwrap();
    assert!(
        !view.exact_fields.contains(&"accumulated".to_string()),
        "exact_fields must not advertise a field the SQL does not contain: {:?}",
        view.exact_fields
    );
}

#[test]
fn the_generated_check_names_the_promised_columns_rather_than_star() {
    let (nest, result) = emitted_nest(OPS_SCHEMA, OPS_MAPPING);
    let check = std::fs::read_to_string(nest.path().join("checks/port_views.sql")).unwrap();
    let view = result.views.iter().find(|v| v.entity == "Pool").unwrap();

    assert!(
        !check.contains("SELECT * FROM"),
        "`SELECT *` binds whatever the view happens to contain and cannot see a missing promised \
         column, which is how #1248 passed every generated check:\n{check}"
    );
    for field in &view.exact_fields {
        assert!(
            check.contains(&format!("\"{field}\"")),
            "the check must name `{field}` so DuckDB refuses to bind if the view ever stops \
             projecting it:\n{check}"
        );
    }
}

/// Pins the *mechanism* the fix depends on: DuckDB refuses to bind a projection naming a column the
/// view does not have, so a check written that way can see a missing promised column.
///
/// **Read this together with its sibling above, and do not mistake one for the other.** Reverting
/// the projection to `SELECT *` kills `the_generated_check_names_the_promised_columns_rather_than_star`
/// and leaves this one green, because breaking the check by hand still names an absent column
/// whichever way the generator wrote it. So the sibling is the regression test for the change, and
/// this is the test that says why naming columns is worth doing at all. Neither is sufficient alone:
/// without the sibling nothing notices the generator regressing, and without this one the sibling is
/// asserting on a substring whose consequence nobody has demonstrated.
#[test]
fn the_generated_check_fails_if_a_promised_column_is_missing() {
    let (nest, _result) = emitted_nest(OPS_SCHEMA, OPS_MAPPING);
    let clean = nuthatch::check::check(nuthatch::cli::CheckArgs {
        name: None,
        dir: nest.path().display().to_string(),
        update: false,
        #[cfg(feature = "folds")]
        folds: false,
        #[cfg(feature = "folds")]
        from_genesis: false,
    });
    assert!(
        clean.is_ok(),
        "the emitted nest must pass its own check: {clean:?}"
    );

    // Now break exactly the invariant #1248 broke: promise a column the view does not project.
    let path = nest.path().join("checks/port_views.sql");
    let sql = std::fs::read_to_string(&path).unwrap();
    let broken = sql.replace("FROM (SELECT ", "FROM (SELECT \"negated\", ");
    assert_ne!(
        broken, sql,
        "the check must have a projection to break:\n{sql}"
    );
    std::fs::write(&path, &broken).unwrap();

    let result = nuthatch::check::check(nuthatch::cli::CheckArgs {
        name: None,
        dir: nest.path().display().to_string(),
        update: false,
        #[cfg(feature = "folds")]
        folds: false,
        #[cfg(feature = "folds")]
        from_genesis: false,
    });
    assert!(
        result.is_err(),
        "a check naming a column the view does not project must fail to bind, and did not:\n{broken}"
    );
}

// ---------------------------------------------------------------------------------------------
// #1250: a column name is resolved against the decoded schema, never snake_cased into existence.
// ---------------------------------------------------------------------------------------------

/// The regression test the fixture choice hid. `tickSpacing` is a camelCase ABI parameter, so the
/// decoded column is `tickSpacing`; the emitter used to ask for `tick_spacing` and the view did not
/// bind at all. **This binds rather than greps**, because the whole class of defect was invisible to
/// a test that only inspects SQL text: `four_classes_exact_sqrt_price_lands_in_a_view` exercises
/// `sqrtPriceX96` and passes on the broken code.
#[test]
fn a_camel_case_parameter_yields_a_view_that_binds() {
    let schema = r#"
type Pool @entity {
  id: ID!
  spacing: BigInt!
}
"#;
    let mapping = r#"
export function handlePoolCreated(event: PoolCreated): void {
  let pool = new Pool(event.params.pool.toHex())
  pool.id = event.params.pool.toHex()
  pool.spacing = event.params.tickSpacing
  pool.save()
}
"#;
    let (nest, result) = emitted_nest(schema, mapping);
    let view = result.views.iter().find(|v| v.entity == "Pool").unwrap();
    let sql = select_sql(&view.sql);

    // **What each assertion below actually guards, because they are not interchangeable.**
    //
    // The bind proves the emitted view is loadable, which is what the old code failed outright with
    // `Binder Error: Referenced column "tick_spacing" not found in FROM clause!`. It does *not*
    // catch a return to snake-casing on its own: `resolve_column` refuses a name the table does not
    // have, so the field would simply be skipped and the smaller view would bind perfectly well.
    // Measured, not assumed - restoring `snake_case` leaves this assertion green and prints
    // `✓ port_views: 1 row(s) match`.
    //
    // So the two below it are the ones that catch that regression: the field has to *land*, named
    // as the ABI names it, and nothing may be skipped. Read together, the three say the view loads
    // and carries the field, which is the whole claim.
    let check = nuthatch::check::check(nuthatch::cli::CheckArgs {
        name: None,
        dir: nest.path().display().to_string(),
        update: false,
        #[cfg(feature = "folds")]
        folds: false,
        #[cfg(feature = "folds")]
        from_genesis: false,
    });
    assert!(
        check.is_ok(),
        "a camelCase parameter must produce a view that binds: {check:?}\n{sql}"
    );

    assert!(
        sql.contains("\"tickSpacing\""),
        "the decoded column is the ABI parameter verbatim:\n{sql}"
    );
    assert!(
        !sql.contains("tick_spacing"),
        "`tick_spacing` is not a column of this table and never was:\n{sql}"
    );
    assert!(
        result.skipped_fields.is_empty(),
        "`spacing` resolves to a real column, so nothing should be skipped: {:?}",
        result
            .skipped_fields
            .iter()
            .map(|s| s.name())
            .collect::<Vec<_>>()
    );
}

/// A name matching no column of the table is skipped by name, not written into a view that cannot
/// bind. `resolve_column` has no fuzzy fallback, so this also pins that a near-miss is refused
/// rather than resolved to something adjacent.
#[test]
fn a_name_that_matches_no_column_is_skipped_rather_than_emitted() {
    // `PoolCreated` has no `amount0`; the Swap ABI does, and this nest does not import it.
    let schema = r#"
type Pool @entity {
  id: ID!
  missing: BigInt!
}
"#;
    let mapping = r#"
export function handlePoolCreated(event: PoolCreated): void {
  let pool = new Pool(event.params.pool.toHex())
  pool.id = event.params.pool.toHex()
  pool.missing = event.params.amount0
  pool.save()
}
"#;
    let (nest, result) = emitted_nest(schema, mapping);
    assert!(
        result
            .skipped_fields
            .iter()
            .any(|s| s.entity == "Pool" && s.field == "missing"),
        "a parameter this event does not have must be named as skipped: {:?}",
        result
            .skipped_fields
            .iter()
            .map(|s| s.name())
            .collect::<Vec<_>>()
    );
    let check = nuthatch::check::check(nuthatch::cli::CheckArgs {
        name: None,
        dir: nest.path().display().to_string(),
        update: false,
        #[cfg(feature = "folds")]
        folds: false,
        #[cfg(feature = "folds")]
        from_genesis: false,
    });
    assert!(
        check.is_ok(),
        "skipping the field must leave a nest that still checks: {check:?}"
    );
}

// ---------------------------------------------------------------------------------------------
// #1214 / RFC-0044 S5: a running total is maintained incrementally, not folded in a view.
// ---------------------------------------------------------------------------------------------

const ACCUM_SCHEMA: &str = r#"
type Pool @entity {
  id: ID!
  totalFees: BigInt!
  latest: BigInt!
}
"#;

const ACCUM_MAPPING: &str = r#"
export function handlePoolCreated(event: PoolCreated): void {
  let pool = new Pool(event.params.pool.toHex())
  pool.id = event.params.pool.toHex()
  pool.totalFees = pool.totalFees.plus(event.params.fee)
  pool.latest = event.params.tickSpacing
  pool.save()
}
"#;

/// A field maintained incrementally is **answered**, and the figure has to say so separately.
///
/// Counting it as unanswered would under-report a port that did the right thing with its running
/// totals, and folding it into the view count would hide where the field actually lives. A mutation
/// setting `incremental: 0` survived this file before this test existed, which is how the gap showed
/// up: the two fixtures the coverage tests used had no materialised field between them (#1277).
#[test]
fn the_coverage_figure_counts_an_incremental_field_as_answered() {
    let (nest, result) = emitted_nest(ACCUM_SCHEMA, ACCUM_MAPPING);

    // `id` and `latest` are latest-value and land in the view; `totalFees` is a running total and is
    // maintained in `entities/pool.sql`. All three are classified exact.
    assert_eq!(
        result.coverage,
        nuthatch::port_emit::Coverage {
            classified_exact: 3,
            in_views: 2,
            incremental: 1,
            derived: 0,
        },
        "got {}",
        result.coverage.summary()
    );
    assert_eq!(
        result.coverage.answered(),
        3,
        "{}",
        result.coverage.summary()
    );
    assert_eq!(
        result.coverage.unanswered(),
        0,
        "nothing here is unanswered: {}",
        result.coverage.summary()
    );

    let readme = std::fs::read_to_string(nest.path().join("README.md")).unwrap();
    assert!(
        readme.contains(
            "3 of 3 fields the report calls exact are answered (100%): 2 in views, 1 maintained \
             incrementally, 0 derived by reverse lookup, 0 not answered at all"
        ),
        "the README must say where the third field went:\n{readme}"
    );
}

/// A unit increment is `count(*)`, not a sum of a column.
///
/// `pool.txCount = pool.txCount.plus(ONE_BI)` adds one per event. The matcher required the accumulated
/// operand to be a decoded column, so every counter was rejected and the field fell back to its
/// *initialising* assignment (`= ZERO_BI`) - which is why 45 fields on Uniswap V4 read as "zero
/// initialiser" rather than "running total" (#1310).
///
/// No `_overflow` companion, deliberately: that flag exists because an event param is sealed as exact
/// decimal text and `TRY_CAST` to `DECIMAL(38,0)` yields NULL past 38 digits, which `sum` would skip in
/// silence. A row count casts nothing, so a flag here would always be 0 and its presence would imply
/// something could overflow.
#[test]
fn a_unit_increment_is_emitted_as_a_row_count() {
    const SCHEMA: &str = r#"
type Pool @entity {
  id: ID!
  txCount: BigInt!
  liquidity: BigInt!
}
"#;
    const MAPPING: &str = r#"
export function handlePoolCreated(event: PoolCreated): void {
  let pool = new Pool(event.params.pool.toHex())
  pool.id = event.params.pool.toHex()
  pool.txCount = pool.txCount.plus(ONE_BI)
  pool.liquidity = pool.liquidity.plus(event.params.fee)
  pool.save()
}
"#;
    let (nest, result) = emitted_nest(SCHEMA, MAPPING);
    let entity = result
        .entities
        .iter()
        .find(|e| e.entity == "Pool")
        .unwrap_or_else(|| panic!("no Pool entity: {:?}", result.entities));
    assert!(
        entity.fields.contains(&"txCount".to_string()),
        "the counter must be maintained incrementally: {:?}",
        entity.fields
    );

    let sql = std::fs::read_to_string(nest.path().join("entities/pool_totals.sql")).unwrap();
    assert!(
        sql.contains(r#"count(*) AS "txCount""#),
        "a unit increment counts rows:\n{sql}"
    );
    assert!(
        !sql.contains(r#""txCount_overflow""#),
        "a row count cannot overflow, so the flag must not be emitted:\n{sql}"
    );
    // The column-sourced total beside it sums the column, which the circuit reads as an integer.
    assert!(
        sql.contains(r#"sum("fee") AS "liquidity""#),
        "a column total sums its column:\n{sql}"
    );
    // The comment says which it is, since the two read very differently to a porter.
    assert!(
        sql.contains("counts rows of"),
        "the provenance comment must say it counts rather than sums:\n{sql}"
    );
}

/// A constant that is not one is named rather than folded.
///
/// `plus(TWO_BI)` is `2 * count(*)`, which is expressible and is a second shape to get right. Guessing at
/// it would be the #1248 defect again - an operation discarded and a plausible number answered in its place.
#[test]
fn a_non_unit_constant_increment_is_skipped_by_name() {
    const SCHEMA: &str = r#"
type Pool @entity {
  id: ID!
  pairCount: BigInt!
}
"#;
    const MAPPING: &str = r#"
export function handlePoolCreated(event: PoolCreated): void {
  let pool = new Pool(event.params.pool.toHex())
  pool.id = event.params.pool.toHex()
  pool.pairCount = pool.pairCount.plus(TWO_BI)
  pool.save()
}
"#;
    let (_nest, result) = emitted_nest(SCHEMA, MAPPING);
    assert!(
        result
            .entities
            .iter()
            .all(|e| !e.fields.contains(&"pairCount".to_string())),
        "a non-unit constant must not be folded as if it were one: {:?}",
        result.entities
    );
    assert!(
        result.skipped_fields.iter().any(|s| s.field == "pairCount")
            || result.coverage.unanswered() > 0,
        "and it must be named rather than vanishing: {:?}",
        result.skipped_fields
    );
}

/// The gate that decides the slice: the emitted entity has to satisfy RFC-0041's v1 shape rules and
/// bind. `nuthatch check` runs both, so this is the real validator rather than a substring.
#[test]
fn an_accumulated_field_is_emitted_as_an_incremental_entity_that_validates() {
    let (nest, result) = emitted_nest(ACCUM_SCHEMA, ACCUM_MAPPING);

    let entity = result
        .entities
        .iter()
        .find(|e| e.entity == "Pool")
        .unwrap_or_else(|| {
            panic!(
                "a running total must become an entity: {:?}",
                result.entities
            )
        });
    assert_eq!(entity.fields, vec!["totalFees".to_string()]);
    // One table, grouped by its key: the shape the circuit maintains (#1590). It reads the column as
    // a checked 128-bit integer and faults on a value it cannot hold, so there is no cast to
    // narrow silently and no overflow flag to carry.
    assert!(
        entity.sql.contains("sum(\"fee\") AS \"totalFees\"")
            && entity.sql.contains("FROM \"factory__pool_created\"")
            && !entity.sql.contains("TRY_CAST")
            && !entity.sql.contains("_overflow"),
        "the total is the sum of the deltas, read as the circuit reads them:\n{}",
        entity.sql
    );
    assert!(
        nuthatch::entity_lower::lower(entity.sql.as_str()).is_ok(),
        "dev lowers it:\n{}",
        entity.sql
    );

    // Declared, and declared consistently: `entities.toml` requires the name to match the file stem
    // and the path to be exactly `entities/<name>.sql`.
    let toml = std::fs::read_to_string(nest.path().join("entities.toml")).unwrap();
    assert!(toml.contains("name = \"pool_totals\""), "{toml}");
    assert!(
        toml.contains("sql = \"entities/pool_totals.sql\""),
        "{toml}"
    );
    assert!(nest.path().join("entities/pool_totals.sql").is_file());

    // RFC-0041's shape gate and the binder, via the real validator.
    let issues = nuthatch::entities::validate(nest.path());
    assert!(
        issues.is_empty(),
        "the emitted entity must satisfy RFC-0041 v1: {:?}",
        issues
            .iter()
            .map(|i| format!("{}: {}", i.name, i.error))
            .collect::<Vec<_>>()
    );
    let check = nuthatch::check::check(nuthatch::cli::CheckArgs {
        name: None,
        dir: nest.path().display().to_string(),
        update: false,
        #[cfg(feature = "folds")]
        folds: false,
        #[cfg(feature = "folds")]
        from_genesis: false,
    });
    assert!(check.is_ok(), "the ported nest must still check: {check:?}");
}

/// The accumulation must not *also* appear in the view, where `last()` would answer with the most
/// recent delta instead of the total - a wrong number rather than a missing one. The view keeps the
/// latest-value field, and the file says where the other one went.
#[test]
fn the_running_total_is_absent_from_the_view_and_the_view_says_where_it_went() {
    let (_nest, result) = emitted_nest(ACCUM_SCHEMA, ACCUM_MAPPING);
    let view = result.views.iter().find(|v| v.entity == "Pool").unwrap();

    assert!(
        !view.exact_fields.contains(&"totalFees".to_string()),
        "a running total is not a view field: {:?}",
        view.exact_fields
    );
    assert!(
        view.exact_fields.contains(&"latest".to_string()),
        "the latest-value field still belongs in the view: {:?}",
        view.exact_fields
    );
    assert!(
        view.sql
            .contains("a running total answered by `pool_totals`"),
        "RFC-0044 §6 wants the artefact to say which of the two a field landed in:\n{}",
        view.sql
    );
    assert!(
        !result.skipped_fields.iter().any(|s| s.field == "totalFees"),
        "a field that landed as an entity is not skipped: {:?}",
        result
            .skipped_fields
            .iter()
            .map(|s| s.name())
            .collect::<Vec<_>>()
    );
}

/// The absence case, and the sprint's theme. A port with no running totals must leave **no**
/// `entities.toml` behind: one that declares nothing is itself a `check` error, so emitting an empty
/// file would turn a nest that is merely view-shaped into a nest that fails its own validation.
#[test]
fn a_port_with_no_running_totals_leaves_no_entities_file() {
    let (nest, result) = emitted_nest(OPS_SCHEMA, OPS_MAPPING);
    assert!(
        result.entities.is_empty(),
        "nothing here accumulates: {:?}",
        result.entities
    );
    assert!(
        !nest.path().join("entities.toml").exists(),
        "an entities.toml declaring nothing fails `nuthatch check`"
    );
    let check = nuthatch::check::check(nuthatch::cli::CheckArgs {
        name: None,
        dir: nest.path().display().to_string(),
        update: false,
        #[cfg(feature = "folds")]
        folds: false,
        #[cfg(feature = "folds")]
        from_genesis: false,
    });
    assert!(
        check.is_ok(),
        "a nest with no entities is still a nest: {check:?}"
    );
}

/// `port-emit` is deliberately re-runnable into the same nest. When a mapping that once had an
/// accumulation loses it, the old generated declaration and SQL must disappear together; otherwise
/// `has_declarations` still starts the entity runtime from a stale file.
#[test]
fn rerunning_without_a_running_total_removes_the_prior_generated_entity() {
    let subgraph = subgraph_with(ACCUM_SCHEMA, ACCUM_MAPPING);
    let nest = tempfile::tempdir().unwrap();
    write_imported_nest(nest.path(), false);
    nuthatch::port_emit::emit(subgraph.path(), nest.path()).expect("initial emitting port");
    assert!(nest.path().join("entities.toml").is_file());
    assert!(nest.path().join("entities/pool_totals.sql").is_file());

    std::fs::write(subgraph.path().join("schema.graphql"), OPS_SCHEMA).unwrap();
    std::fs::write(subgraph.path().join("src/mappings/core.ts"), OPS_MAPPING).unwrap();
    let result = nuthatch::port_emit::emit(subgraph.path(), nest.path()).expect("re-emitting port");

    assert!(
        result.entities.is_empty(),
        "nothing here accumulates: {:?}",
        result.entities
    );
    assert!(
        !nest.path().join("entities.toml").exists(),
        "the former declaration must not survive an empty re-emission"
    );
    assert!(
        !nest.path().join("entities").exists(),
        "the former generated SQL directory must not keep the nest entity-backed"
    );
}

/// A field accumulated by two event tables sums **both**, as two arms of one entity.
///
/// It used to name the second contribution as skipped - "one entity is one relation in v1" - which was
/// honest but left the total wrong unless an operator hand-wrote the arm. 45 fields on Uniswap V4 were
/// gated on it (#1313). Each arm projects its own fields and zero for the others, the arms are
/// `UNION ALL`ed, and one outer aggregate folds them by key.
#[test]
fn a_field_accumulated_by_two_tables_sums_both_arms() {
    let subgraph = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(subgraph.path().join("src/mappings")).unwrap();
    std::fs::write(
        subgraph.path().join("schema.graphql"),
        r#"type Pool @entity {
  id: ID!
  totalFees: BigInt!
  swapVolume: BigInt!
}

"#,
    )
    .unwrap();
    std::fs::write(
        subgraph.path().join("subgraph.yaml"),
        r#"specVersion: 0.0.8
dataSources:
  - kind: ethereum/contract
    name: Factory
    mapping:
      file: ./src/mappings/core.ts
      eventHandlers:
        - event: PoolCreated(indexed address,indexed address,indexed uint24,int24,address)
          handler: handlePoolCreated
templates:
  - kind: ethereum/contract
    name: Pool
    mapping:
      file: ./src/mappings/core.ts
      eventHandlers:
        - event: Swap(indexed address,indexed address,int256,int256,uint160,uint128,int24)
          handler: handlePoolSwap
"#,
    )
    .unwrap();
    std::fs::write(
        subgraph.path().join("src/mappings/core.ts"),
        r#"export function handlePoolCreated(event: PoolCreated): void {
  let pool = new Pool(event.params.pool.toHex())
  pool.id = event.params.pool.toHex()
  pool.totalFees = pool.totalFees.plus(event.params.fee)
  pool.save()
}

export function handlePoolSwap(event: Swap): void {
  let pool = new Pool(event.address.toHex())
  pool.id = event.address.toHex()
  pool.totalFees = pool.totalFees.plus(event.params.amount0)
  pool.swapVolume = pool.swapVolume.plus(event.params.amount1)
  pool.save()
}
"#,
    )
    .unwrap();

    let nest = tempfile::tempdir().unwrap();
    write_imported_nest(nest.path(), true);
    let result = nuthatch::port_emit::emit(subgraph.path(), nest.path()).expect("emit");

    // **Two tables, so a view, not an entity** (#1595). The circuit reads one table, so a `UNION ALL`
    // entity could never start; RFC-0041 keeps what it cannot maintain as a view.
    assert!(
        !result.entities.iter().any(|e| e.entity == "Pool"),
        "a multi-arm total is not an entity: {:?}",
        result.entities
    );
    let view = std::fs::read_to_string(nest.path().join("views/30-pool_totals.sql")).unwrap();
    assert!(
        view.contains("CREATE VIEW \"pool_totals\"") && view.matches("UNION ALL").count() == 1,
        "two arms, one union, in the totals view:\n{view}"
    );
    for table in ["factory__pool_created", "pool__swap"] {
        assert!(
            view.contains(&format!("FROM \"{table}\"")),
            "`{table}` must be an arm, not a skipped field:\n{view}"
        );
    }
    // Each arm contributes the field it writes, through the checked cast, and the total folds once.
    assert!(
        view.contains("TRY_CAST(\"fee\" AS DECIMAL(38,0))")
            && view.contains("TRY_CAST(\"amount0\" AS DECIMAL(38,0))")
            && view.contains("sum(\"totalFees\") AS \"totalFees\""),
        "each arm casts its own column and the outer aggregate sums across arms:\n{view}"
    );
    // **`swapVolume` is written by the swap arm only**, so the `pool_created` arm has to fill it - and it
    // must fill it with **zero**. `sum` skips NULLs, so a NULL fill makes the total NULL for any key that
    // appears only in arms which do not write the field, where graph-node has 0.
    assert!(
        view.contains("CAST(0 AS DECIMAL(38,0)) AS \"swapVolume\"")
            && !view.contains("CAST(NULL AS"),
        "the arm that does not write `swapVolume` contributes zero, never NULL:\n{view}"
    );
    // **The overflow flag folds with `max`, not `min`.** `min` would report "no overflow" as soon as any
    // single arm was clean, which is the wrong way round for a flag that means "some contributing row
    // could not be represented".
    assert!(
        view.contains("max(\"totalFees_overflow\")"),
        "the overflow flag is an OR across arms, so `max`:\n{view}"
    );
    assert!(
        !result.skipped_fields.iter().any(|f| {
            f.entity == "Pool" && f.field == "totalFees" && f.why.contains("one relation")
        }),
        "nothing should still be skipped for being a second relation: {:?}",
        result.skipped_fields
    );
    // Reported and counted as answered, like any view.
    assert!(
        result.views.iter().any(|v| v.file == "30-pool_totals.sql"
            && v.exact_fields.contains(&"totalFees".to_string())
            && v.exact_fields.contains(&"swapVolume".to_string())),
        "the totals view is an emitted view: {:?}",
        result.views.iter().map(|v| &v.file).collect::<Vec<_>>()
    );
    // The whole nest checks, views and all, which is what `dev` loads.
    let check = nuthatch::check::check(nuthatch::cli::CheckArgs {
        name: None,
        dir: nest.path().display().to_string(),
        update: false,
        #[cfg(feature = "folds")]
        folds: false,
        #[cfg(feature = "folds")]
        from_genesis: false,
    });
    assert!(check.is_ok(), "the ported nest must check: {check:?}");
}

/// A singleton the mapping keys with a constant gets that constant as its key.
///
/// `new Protocol('1')` has no key *in the data*, because the key is in the mapping. The emitter reported
/// "nothing in the mapping identifies which `Protocol` a row belongs to, so there is no key to group by" -
/// true as written and the wrong conclusion (#1313).
///
/// Neither pinned target exercises this: Uniswap's `PoolManager` is keyed by a local holding an address,
/// and Carbon's `Protocol` likewise. So this test is the only thing that holds the shape, which is worth
/// saying out loud rather than leaving a reader to assume a fixture covers it.
#[test]
fn a_singleton_keyed_by_a_constant_groups_by_that_constant() {
    const SCHEMA: &str = r#"
type Protocol @entity {
  id: ID!
  txCount: BigInt!
  totalFees: BigInt!
}
"#;
    const MAPPING: &str = r#"
export function handlePoolCreated(event: PoolCreated): void {
  let protocol = new Protocol('1')
  protocol.txCount = protocol.txCount.plus(ONE_BI)
  protocol.totalFees = protocol.totalFees.plus(event.params.fee)
  protocol.save()
}
"#;
    let (_nest, result) = emitted_nest(SCHEMA, MAPPING);
    let entity = result
        .entities
        .iter()
        .find(|e| e.entity == "Protocol")
        .unwrap_or_else(|| {
            panic!(
                "a singleton's totals must be emitted: {:?}",
                result.entities
            )
        });

    assert!(
        entity.sql.contains("'1' AS \"id\""),
        "the constant is the key:\n{}",
        entity.sql
    );
    assert!(
        entity.sql.contains("GROUP BY '1'"),
        "and it still groups, so one row comes out:\n{}",
        entity.sql
    );
    assert!(
        entity.sql.contains("AS \"txCount\"")
            && entity.sql.contains("AS \"totalFees\"")
            && nuthatch::entity_lower::lower(entity.sql.as_str()).is_ok(),
        "both totals fold, in a shape dev lowers:\n{}",
        entity.sql
    );
    assert!(
        !result
            .skipped_fields
            .iter()
            .any(|f| f.entity == "Protocol" && f.why.contains("no key to group by")),
        "nothing should still be skipped for want of a key: {:?}",
        result.skipped_fields
    );
}

/// A key that is neither a column nor a constant is still named rather than guessed at.
///
/// Uniswap keys `PoolManager` by `poolManagerAddress`, a local holding an address that the function body
/// does not declare - it comes from a constants module. Resolving that would mean reading imports, and
/// guessing would mean grouping a protocol-wide total by something arbitrary.
#[test]
fn a_key_that_is_neither_a_column_nor_a_constant_is_named() {
    const SCHEMA: &str = r#"
type Protocol @entity {
  id: ID!
  txCount: BigInt!
}
"#;
    const MAPPING: &str = r#"
export function handlePoolCreated(event: PoolCreated): void {
  let protocol = new Protocol(PROTOCOL_ADDRESS)
  protocol.txCount = protocol.txCount.plus(ONE_BI)
  protocol.save()
}
"#;
    let (_nest, result) = emitted_nest(SCHEMA, MAPPING);
    assert!(
        result.entities.iter().all(|e| e.entity != "Protocol"),
        "an unresolvable key must not be guessed at: {:?}",
        result.entities
    );
    assert!(
        result
            .skipped_fields
            .iter()
            .any(|f| f.entity == "Protocol" && f.why.contains("no key to group by")),
        "and it must be named: {:?}",
        result.skipped_fields
    );
}

/// An arm that does not write a field contributes **zero**, not NULL.
///
/// `sum` skips NULLs, so for a key that appears only in arms which do not write the field, a NULL fill
/// makes the total come back NULL where graph-node has 0. A mutation changing the fill to NULL survived
/// until this existed: with every key present in every arm the two are indistinguishable, because
/// `sum(NULL, 5)` and `sum(0, 5)` are both 5.
#[test]
fn an_arm_that_does_not_write_a_field_contributes_zero() {
    const SCHEMA: &str = r#"
type Pool @entity {
  id: ID!
  created: BigInt!
  swapped: BigInt!
}
"#;
    const MAPPING: &str = r#"
export function handlePoolCreated(event: PoolCreated): void {
  let pool = new Pool(event.params.pool.toHex())
  pool.id = event.params.pool.toHex()
  pool.created = pool.created.plus(event.params.fee)
  pool.save()
}
"#;
    // One table, two fields, only one of which this arm writes - so `swapped` is zero-filled here.
    let (_nest, result) = emitted_nest(SCHEMA, MAPPING);
    let Some(entity) = result.entities.iter().find(|e| e.entity == "Pool") else {
        // `swapped` is written by no mapping at all, so it is not an accumulator and there is nothing
        // to zero-fill. Assert the shape on the field that is, rather than passing vacuously.
        panic!("Pool must be emitted: {:?}", result.entities);
    };
    assert!(
        entity.sql.contains("sum(\"fee\") AS \"created\""),
        "the arm that writes the field sums its column:\n{}",
        entity.sql
    );
    // The zero-fill only appears when an entity has more than one arm, so the two-arm test above is where
    // it is observable. Pinned there as a literal, because the difference between 0 and NULL is the whole
    // point and a reader cannot see it from `sum`.
    assert!(
        !entity.sql.contains("NULL"),
        "a missing contribution must never be NULL - `sum` would skip it and the total would be NULL \
         where graph-node has 0:\n{}",
        entity.sql
    );
}

#[test]
fn an_accumulation_from_another_entity_receiver_is_not_emitted_for_this_entity() {
    let mapping = r#"export function handlePoolCreated(event: PoolCreated): void {
  let pool = new Pool(event.params.pool.toHex())
  let other = new Pool(event.params.token0.toHex())
  pool.id = event.params.pool.toHex()
  other.id = event.params.token0.toHex()
  pool.totalFees = other.totalFees.plus(event.params.fee)
  pool.save()
  other.save()
}
"#;
    let (_nest, result) = emitted_nest(ACCUM_SCHEMA, mapping);
    assert!(
        result.entities.is_empty(),
        "a different receiver's value is not this entity's running total: {:?}",
        result.entities
    );
}

#[test]
fn a_nonempty_reemission_removes_entities_no_longer_generated() {
    let initial_schema = r#"type Pool @entity {
  id: ID!
  totalFees: BigInt!
}

type Token @entity {
  id: ID!
  totalSupply: BigInt!
}
"#;
    let initial_mapping = r#"export function handlePoolCreated(event: PoolCreated): void {
  let pool = new Pool(event.params.pool.toHex())
  let token = new Token(event.params.token0.toHex())
  pool.id = event.params.pool.toHex()
  token.id = event.params.token0.toHex()
  pool.totalFees = pool.totalFees.plus(event.params.fee)
  token.totalSupply = token.totalSupply.plus(event.params.fee)
  pool.save()
  token.save()
}
"#;
    let subgraph = subgraph_with(initial_schema, initial_mapping);
    let nest = tempfile::tempdir().unwrap();
    write_imported_nest(nest.path(), false);
    nuthatch::port_emit::emit(subgraph.path(), nest.path()).expect("initial emitting port");
    assert!(nest.path().join("entities/pool_totals.sql").is_file());
    assert!(nest.path().join("entities/token_totals.sql").is_file());
    std::fs::write(
        nest.path().join("entities/manual.sql"),
        "SELECT id FROM factory__pool_created",
    )
    .unwrap();
    let mut manifest = std::fs::read_to_string(nest.path().join("entities.toml")).unwrap();
    manifest.push_str(
        "\n[[entities]]\nname = \"manual\"\nsql = \"entities/manual.sql\"\nkey = [\"id\"]\nmax_rows = 100\n",
    );
    std::fs::write(nest.path().join("entities.toml"), manifest).unwrap();

    std::fs::write(subgraph.path().join("schema.graphql"), ACCUM_SCHEMA).unwrap();
    std::fs::write(subgraph.path().join("src/mappings/core.ts"), ACCUM_MAPPING).unwrap();
    nuthatch::port_emit::emit(subgraph.path(), nest.path()).expect("re-emitting port");

    assert!(nest.path().join("entities/pool_totals.sql").is_file());
    assert!(
        !nest.path().join("entities/token_totals.sql").exists(),
        "a generator-owned entity omitted by a non-empty re-emission must disappear"
    );
    let manifest = std::fs::read_to_string(nest.path().join("entities.toml")).unwrap();
    assert!(manifest.contains("name = \"manual\""));
    assert!(nest.path().join("entities/manual.sql").is_file());
}

/// #1277. A local that only aliases an event parameter is the shape every `Pool.id` takes in the
/// real Uniswap V4 subgraph:
///
/// ```text
/// const poolId = event.params.id.toHexString()
/// const pool = new Pool(poolId)
/// ```
///
/// The emitter resolved the constructor argument with `event_column` alone, which sees a bare
/// identifier and refuses, so the primary key of all 132,765 pools was reported as *"no decoded
/// column corresponds to it"* while the column sat in `pool_manager__initialize` already
/// hex-encoded. Without an id in the view, every other field the view answered was unalignable
/// against a reference, which is why this one field decided the acceptance port.
#[test]
fn an_id_from_a_local_aliasing_an_event_param_reaches_its_column() {
    let schema = r#"
type Pool @entity {
  id: ID!
  plain: BigInt!
}
"#;
    let mapping = r#"
export function handlePoolCreated(event: PoolCreated): void {
  const poolId = event.params.pool.toHexString()
  const pool = new Pool(poolId)
  pool.plain = event.params.fee
  pool.save()
}
"#;
    let (nest, result) = emitted_nest(schema, mapping);
    let view = result.views.iter().find(|v| v.entity == "Pool").unwrap();
    let sql = select_sql(&view.sql);

    assert!(
        sql.contains("AS \"id\""),
        "`Pool.id` must bind to the column its local aliases (#1277):\n{sql}"
    );
    assert!(
        sql.contains("\"pool\""),
        "and the column is `pool`, as the ABI names it:\n{sql}"
    );
    assert!(
        !result
            .skipped_fields
            .iter()
            .any(|s| s.name().contains("id")),
        "`Pool.id` must not be skipped: {:?}",
        result
            .skipped_fields
            .iter()
            .map(|s| s.name())
            .collect::<Vec<_>>()
    );

    // The view has to load, not merely mention the column.
    let check = nuthatch::check::check(nuthatch::cli::CheckArgs {
        name: None,
        dir: nest.path().display().to_string(),
        update: false,
        #[cfg(feature = "folds")]
        folds: false,
        #[cfg(feature = "folds")]
        from_genesis: false,
    });
    assert!(
        check.is_ok(),
        "the emitted view must bind: {check:?}\n{sql}"
    );
}

/// #1277. An entity whose every exact field reaches no column used to get
/// `CREATE VIEW "<entity>" AS SELECT 1 AS port_placeholder`, so that `nuthatch check` had a table to
/// bind. On the RFC-0044 S3 acceptance port that was nine of nineteen entities, `Bundle` and
/// `PoolManager` among them, and `SELECT count(*) FROM "bundle"` answered `1` with a full provenance
/// block. The reference has exactly one `Bundle`, so a comparison on row counts agreed with it.
///
/// `Bundle` here is the real shape: a literal id and a constant, neither of which is a column.
const NO_COLUMN_SCHEMA: &str = r#"
type Pool @entity {
  id: ID!
  plain: BigInt!
}
type Bundle @entity {
  id: ID!
  ethPriceUSD: BigDecimal!
}
"#;

const NO_COLUMN_MAPPING: &str = r#"
export function handlePoolCreated(event: PoolCreated): void {
  let pool = new Pool(event.params.pool.toHex())
  pool.id = event.params.pool.toHex()
  pool.plain = event.params.fee
  pool.save()
  let bundle = new Bundle('1')
  bundle.ethPriceUSD = ZERO_BD
  bundle.save()
}
"#;

#[test]
fn an_entity_whose_fields_reach_no_column_gets_no_view_at_all() {
    let (nest, result) = emitted_nest(NO_COLUMN_SCHEMA, NO_COLUMN_MAPPING);

    assert!(
        result.views.iter().all(|v| v.entity != "Bundle"),
        "no view may be emitted for Bundle: {:?}",
        result.views.iter().map(|v| &v.entity).collect::<Vec<_>>()
    );
    assert!(
        result
            .entities_without_views
            .contains(&"Bundle".to_string()),
        "and it must be named rather than silently absent: {:?}",
        result.entities_without_views
    );
    assert!(
        !nest.path().join("views/20-bundle.sql").exists(),
        "no file either - a view returning 1 is worse than no table"
    );

    // Pool still lands. The change must not cost an entity that does resolve.
    assert!(
        result.views.iter().any(|v| v.entity == "Pool"),
        "Pool resolves `plain` and must still be emitted"
    );

    // The check must not name Bundle. Its projection for such a view was `*`, which binds whatever
    // the view holds and so asserted nothing while reading `binds: true`.
    let check = std::fs::read_to_string(nest.path().join("checks/port_views.sql")).unwrap();
    assert!(
        !check.contains("'Bundle'"),
        "the check may not claim an entity that has no view:\n{check}"
    );
    assert!(
        !check.contains("SELECT * FROM"),
        "and no row of it may project `*`:\n{check}"
    );
    let expected =
        std::fs::read_to_string(nest.path().join("checks/expected/port_views.json")).unwrap();
    assert!(
        !expected.contains("Bundle"),
        "nor may the committed expectation:\n{expected}"
    );
}

/// A nest emitted before that change carries nine placeholder views. Re-emitting has to remove the
/// one for an entity that now emits none, or the stale table survives and nothing re-examines it.
#[test]
fn reemission_removes_a_stale_generated_view() {
    let subgraph = subgraph_with(NO_COLUMN_SCHEMA, NO_COLUMN_MAPPING);
    let nest = tempfile::tempdir().unwrap();
    write_imported_nest(nest.path(), false);
    let views = nest.path().join("views");
    std::fs::create_dir_all(&views).unwrap();

    // Exactly what the previous emitter wrote, header and all.
    let stale = views.join("20-bundle.sql");
    std::fs::write(
        &stale,
        "-- Exact fields of `Bundle`. Call-derived, fixed-point and unreachable fields are not \
         here; see README.md.\n\nCREATE VIEW \"bundle\" AS SELECT 1 AS port_placeholder;\n",
    )
    .unwrap();
    // And an operator's own view, which must survive.
    let mine = views.join("30-mine.sql");
    std::fs::write(&mine, "CREATE VIEW \"mine\" AS SELECT 1 AS ok;\n").unwrap();

    nuthatch::port_emit::emit(subgraph.path(), nest.path()).expect("emit");

    assert!(
        !stale.exists(),
        "the stale generated view for Bundle must be removed"
    );
    assert!(
        mine.exists(),
        "a view the operator wrote must not be removed"
    );
}

/// #1277. A field assigned inside a helper that is itself called by a helper reaches no column,
/// because `event_handler_for` looks only one hop: it finds an event handler that calls the function
/// directly. The Uniswap V4 subgraph routes every handler through that indirection -
/// `handleSwap` -> `handleSwapHelper` -> `loadTransaction` - so everything `loadTransaction` writes
/// is lost, `Transaction.blockNumber` and `Transaction.timestamp` among them, even though
/// `event.block.number` and `event.block.timestamp` are both mapped to implicit columns already.
#[test]
fn a_field_written_two_helper_hops_from_the_handler_still_reaches_its_column() {
    let schema = r#"
type Pool @entity {
  id: ID!
  createdAt: BigInt!
}
"#;
    let mapping = r#"
export function stamp(event: PoolCreated): void {
  let pool = new Pool(event.params.pool.toHex())
  pool.id = event.params.pool.toHex()
  pool.createdAt = event.block.timestamp
  pool.save()
}
export function handlePoolCreatedHelper(event: PoolCreated): void {
  stamp(event)
}
export function handlePoolCreated(event: PoolCreated): void {
  handlePoolCreatedHelper(event)
}
"#;
    let (_nest, result) = emitted_nest(schema, mapping);
    let view = result
        .views
        .iter()
        .find(|v| v.entity == "Pool")
        .expect("a Pool view");
    let sql = select_sql(&view.sql);
    assert!(
        sql.contains("\"block_timestamp\" AS \"createdAt\""),
        "`event.block.timestamp` two hops from the handler must still bind (#1277):\n{sql}"
    );
}

/// Raised by review of #1279. `plain_local_expr` scanned from the top and returned the first
/// declaration of the name, which is not the one in scope at the use site:
///
/// ```text
/// const poolId = event.params.token1.toHexString()   // outer
/// if (..) {
///   const poolId = event.params.pool.toHexString()   // shadows it
///   const pool = new Pool(poolId)                    // uses this one
/// }
/// ```
///
/// Emitting `Pool.id` from `token1` there would answer the wrong column under a report promising
/// byte-identical - #1248's fault in a new place. Scanning cannot resolve scope, so an ambiguous
/// name resolves to nothing and the field is reported as reaching no column, exactly as it was
/// before the alias fallback existed.
#[test]
fn a_shadowed_local_is_refused_rather_than_resolved_to_the_wrong_one() {
    let schema = r#"
type Pool @entity {
  id: ID!
  plain: BigInt!
}
"#;
    let mapping = r#"
export function handlePoolCreated(event: PoolCreated): void {
  const poolId = event.params.token1.toHexString()
  if (event.params.fee.gt(BigInt.zero())) {
    const poolId = event.params.pool.toHexString()
    const pool = new Pool(poolId)
    pool.plain = event.params.fee
    pool.save()
  }
}
"#;
    let (_nest, result) = emitted_nest(schema, mapping);
    let view = result.views.iter().find(|v| v.entity == "Pool");
    if let Some(view) = view {
        let sql = select_sql(&view.sql);
        assert!(
            !sql.contains("\"token1\" AS \"id\""),
            "the outer `poolId` must not answer `Pool.id`; the inner one shadows it:\n{sql}"
        );
    }
    assert!(
        result
            .skipped_fields
            .iter()
            .any(|s| s.entity == "Pool" && s.field == "id"),
        "an ambiguous local must leave `Pool.id` reported as reaching no column: {:?}",
        result
            .skipped_fields
            .iter()
            .map(|s| s.name())
            .collect::<Vec<_>>()
    );
}

// ---------------------------------------------------------------------------------------------
// #1313: a field the mappings only ever set to a constant.
// ---------------------------------------------------------------------------------------------

/// Ten of Uniswap V4's forty-four initialiser-reported fields are this shape: `pool.collectedFeesUSD
/// = ZERO_BD` at creation and nothing in any mapping ever touches it again. Searching for a decoded
/// column finds none, and the field was reported unanswered - a missing number where the exact one
/// is known and is zero.
const CONSTANT_SCHEMA: &str = r#"
type Pool @entity {
  id: ID!
  plain: BigInt!
  collectedFeesUSD: BigDecimal!
}
"#;

const CONSTANT_MAPPING: &str = r#"
export function handlePoolCreated(event: PoolCreated): void {
  let pool = new Pool(event.params.pool.toHexString())
  pool.plain = event.params.fee
  pool.collectedFeesUSD = ZERO_BD
  pool.save()
}
"#;

#[test]
fn a_field_the_mappings_only_ever_set_to_a_constant_is_projected_as_that_constant() {
    let (nest, result) = emitted_nest(CONSTANT_SCHEMA, CONSTANT_MAPPING);
    let view = result.views.iter().find(|v| v.entity == "Pool").unwrap();
    let sql = select_sql(&view.sql);

    assert!(
        sql.contains("0 AS \"collectedFeesUSD\""),
        "the constant must be projected as its literal:\n{sql}"
    );
    assert!(
        view.exact_fields.contains(&"collectedFeesUSD".to_string()),
        "and listed, so the generated check projects it and coverage counts it: {:?}",
        view.exact_fields
    );
    assert!(
        !result
            .skipped_fields
            .iter()
            .any(|s| s.name().contains("collectedFeesUSD")),
        "a field answered exactly must not also be named unanswered: {:?}",
        result
            .skipped_fields
            .iter()
            .map(|s| s.name())
            .collect::<Vec<_>>()
    );
    // Projected once, in the outer select. A per-arm literal needs a presence marker that is `TRUE`
    // unconditionally, and a guard that cannot fail is worse than no guard: it reads as one.
    assert_eq!(
        sql.matches("AS \"collectedFeesUSD\"").count(),
        1,
        "the literal belongs in one place:\n{sql}"
    );
    assert!(
        !sql.contains("__present__collectedFeesUSD"),
        "and needs no presence marker, having no arm that could omit it:\n{sql}"
    );

    let check = nuthatch::check::check(nuthatch::cli::CheckArgs {
        name: None,
        dir: nest.path().display().to_string(),
        update: false,
        #[cfg(feature = "folds")]
        folds: false,
        #[cfg(feature = "folds")]
        from_genesis: false,
    });
    assert!(
        check.is_ok(),
        "the emitted view must bind: {check:?}\n{sql}"
    );
}

#[test]
fn a_field_assigned_two_different_constants_is_not_constant() {
    let mapping = r#"
export function handlePoolCreated(event: PoolCreated): void {
  let pool = new Pool(event.params.pool.toHexString())
  pool.plain = event.params.fee
  pool.collectedFeesUSD = ZERO_BD
  pool.save()
}
export function handleOther(event: PoolCreated): void {
  let pool = new Pool(event.params.pool.toHexString())
  pool.collectedFeesUSD = ONE_BD
  pool.save()
}
"#;
    let (_nest, result) = emitted_nest(CONSTANT_SCHEMA, mapping);
    let view = result.views.iter().find(|v| v.entity == "Pool").unwrap();
    let sql = select_sql(&view.sql);

    assert!(
        !sql.contains("AS \"collectedFeesUSD\""),
        "a field that is 0 on one path and 1 on another is not constant, and guessing either is a \
         wrong number rather than a missing one:\n{sql}"
    );
    assert!(
        !view.exact_fields.contains(&"collectedFeesUSD".to_string()),
        "nor may it be listed as answered: {:?}",
        view.exact_fields
    );
}

#[test]
fn a_field_also_assigned_a_real_value_is_not_constant() {
    let mapping = r#"
export function handlePoolCreated(event: PoolCreated): void {
  let pool = new Pool(event.params.pool.toHexString())
  pool.plain = event.params.fee
  pool.collectedFeesUSD = ZERO_BD
  pool.collectedFeesUSD = event.params.tickSpacing
  pool.save()
}
"#;
    let (_nest, result) = emitted_nest(CONSTANT_SCHEMA, mapping);
    let view = result.views.iter().find(|v| v.entity == "Pool").unwrap();
    let sql = select_sql(&view.sql);

    assert!(
        !sql.contains("0 AS \"collectedFeesUSD\""),
        "initialised to zero and later assigned a decoded value is not a constant - freezing it at \
         zero is the worst outcome available here:\n{sql}"
    );
    assert!(
        sql.contains("last(\"collectedFeesUSD\""),
        "it resolves through its column like any other field:\n{sql}"
    );
}

#[test]
fn a_constant_on_an_entity_with_no_column_is_named_rather_than_claimed() {
    let (_nest, result) = emitted_nest(NO_COLUMN_SCHEMA, NO_COLUMN_MAPPING);

    assert!(
        result.views.iter().all(|v| v.entity != "Bundle"),
        "a constant is not a row source, so `Bundle` still gets no view: {:?}",
        result.views.iter().map(|v| &v.entity).collect::<Vec<_>>()
    );
    let named = result
        .skipped_fields
        .iter()
        .find(|s| s.name() == "Bundle.ethPriceUSD")
        .unwrap_or_else(|| {
            panic!(
                "the constant must be named unanswered, not silently dropped: {:?}",
                result
                    .skipped_fields
                    .iter()
                    .map(|s| s.name())
                    .collect::<Vec<_>>()
            )
        });
    assert!(
        named.why.contains("no row"),
        "and the reason must say why a known value is still unanswered: {}",
        named.why
    );
}

// ---------------------------------------------------------------------------------------------
// #1313 limit 3: a composed id reaches a SQL expression rather than a column.
// ---------------------------------------------------------------------------------------------

/// Five of Uniswap V4's event entities key on the transaction hash and the log index. Without the `id`
/// the emitted view cannot fold on the entity at all, so the cost of a missing composed key is every
/// other field on that view, not only the key.
#[test]
fn a_composed_id_is_emitted_as_a_concatenation() {
    let schema = r#"
type Swap @entity {
  id: ID!
  plain: BigInt!
}
"#;
    let mapping = r#"
export function eventId(transactionHash: Bytes, logIndex: BigInt): string {
  return `${transactionHash.toHexString()}-${logIndex.toString()}`
}

export function handlePoolCreated(event: PoolCreated): void {
  let swap = new Swap(eventId(event.transaction.hash, event.logIndex))
  swap.id = eventId(event.transaction.hash, event.logIndex)
  swap.plain = event.params.fee
  swap.save()
}
"#;
    let (nest, result) = emitted_nest(schema, mapping);
    let view = result.views.iter().find(|v| v.entity == "Swap").unwrap();
    let sql = select_sql(&view.sql);

    assert!(
        sql.contains(
            "CAST(\"tx_hash\" AS VARCHAR) || '-' || CAST(\"log_index\" AS VARCHAR) AS \"id\""
        ),
        "the composed id must be emitted as the concatenation of its parts:\n{sql}"
    );
    assert!(
        view.exact_fields.contains(&"id".to_string()),
        "and be listed: {:?}",
        view.exact_fields
    );
    // With an `id` the view folds, which is what the key is worth beyond itself.
    assert!(
        sql.contains("GROUP BY \"id\""),
        "an answerable id must re-enable the fold:\n{sql}"
    );

    let check = nuthatch::check::check(nuthatch::cli::CheckArgs {
        name: None,
        dir: nest.path().display().to_string(),
        update: false,
        #[cfg(feature = "folds")]
        folds: false,
        #[cfg(feature = "folds")]
        from_genesis: false,
    });
    assert!(
        check.is_ok(),
        "the emitted view must bind: {check:?}\n{sql}"
    );
}

/// A concatenation of literals alone is not a column-backed value.
///
/// `'a' + 'b'` is the same string on every row, so attributing it to the handler's triggering table would
/// give an entity a view whose only content is a constant - one row with no event behind it, which is the
/// placeholder shape #1277 removed. It has to be named, like any other unanswerable field.
#[test]
fn a_concatenation_of_literals_alone_is_not_a_column() {
    let schema = r#"
type Marker @entity {
  id: ID!
}
"#;
    let mapping = r#"
export function handlePoolCreated(event: PoolCreated): void {
  let marker = new Marker('a' + '-' + 'b')
  marker.id = 'a' + '-' + 'b'
  marker.save()
}
"#;
    let (nest, result) = emitted_nest(schema, mapping);

    assert!(
        result.views.iter().all(|v| v.entity != "Marker"),
        "a literal-only id is not a row source: {:?}",
        result.views.iter().map(|v| &v.entity).collect::<Vec<_>>()
    );
    assert!(
        result
            .entities_without_views
            .contains(&"Marker".to_string()),
        "and it must be named rather than silently absent: {:?}",
        result.entities_without_views
    );
    assert!(
        !nest.path().join("views/20-marker.sql").exists(),
        "no file either"
    );
}

/// A **nullable** field assigned only a literal may still hold nothing.
///
/// Every assignment agreeing on one literal means no row holds a different number. It does not mean every
/// row holds one: a row created by a handler that never touches the field keeps whatever the store had,
/// which for a nullable field is null. graph-node refuses to save a row whose *non-nullable* field is
/// unset, and that is the whole argument for answering every row with one literal (Jules on #1316).
#[test]
fn a_nullable_constant_field_is_not_answered_with_its_literal() {
    let schema = r#"
type Pool @entity {
  id: ID!
  plain: BigInt!
  collectedFeesUSD: BigDecimal
}
"#;
    let (_nest, result) = emitted_nest(schema, CONSTANT_MAPPING);
    let view = result.views.iter().find(|v| v.entity == "Pool").unwrap();
    let sql = select_sql(&view.sql);

    assert!(
        !sql.contains("AS \"collectedFeesUSD\""),
        "a nullable field may be null on a row no handler wrote, and null is not 0:\n{sql}"
    );
    assert!(
        !view.exact_fields.contains(&"collectedFeesUSD".to_string()),
        "nor may it be listed as answered: {:?}",
        view.exact_fields
    );
}

/// A field assigned the literal only inside a branch is not that literal on the rows that took the other
/// one.
///
/// Jules's case on #1316, and the real shape: Uniswap V4 sets `Token.totalValueLockedUSDUntracked` inside
/// `if (token0 === null)`, so this refusal costs a field whose value genuinely is always zero. Losing it
/// is a missing number; projecting the literal for a row that never got it is a wrong one.
#[test]
fn a_conditionally_assigned_constant_is_not_answered_with_its_literal() {
    // **A branch that does not construct the receiver.** The creation guard is the other case and is
    // answered, because a row that skipped it was created by an earlier execution of the same branch
    // (#1326); this one leaves an existing row exactly as it was.
    let mapping = r#"
export function handlePoolCreated(event: PoolCreated): void {
  let pool = Pool.load(event.params.pool.toHexString())
  if (event.params.tickSpacing > 0) {
    pool.collectedFeesUSD = ZERO_BD
  }
  pool.plain = event.params.fee
  pool.save()
}
"#;
    let (_nest, result) = emitted_nest(CONSTANT_SCHEMA, mapping);
    let view = result.views.iter().find(|v| v.entity == "Pool").unwrap();
    let sql = select_sql(&view.sql);

    assert!(
        !sql.contains("AS \"collectedFeesUSD\""),
        "the branch gives the literal to the rows that took it and leaves the rest as they were:\n{sql}"
    );
    assert!(
        !view.exact_fields.contains(&"collectedFeesUSD".to_string()),
        "nor may it be listed as answered: {:?}",
        view.exact_fields
    );
}

/// A braceless conditional is conditional (Jules on #1316).
///
/// `if (cond) pool.collectedFeesUSD = ZERO_BD` sits at brace depth zero, so a depth test alone reads it
/// as unconditional and projects zero for the rows that took the other branch.
#[test]
fn a_braceless_conditional_constant_is_not_answered_with_its_literal() {
    let mapping = r#"
export function handlePoolCreated(event: PoolCreated): void {
  let pool = new Pool(event.params.pool.toHexString())
  pool.plain = event.params.fee
  if (event.params.tickSpacing > 0)
    pool.collectedFeesUSD = ZERO_BD
  pool.save()
}
"#;
    let (_nest, result) = emitted_nest(CONSTANT_SCHEMA, mapping);
    let view = result.views.iter().find(|v| v.entity == "Pool").unwrap();
    let sql = select_sql(&view.sql);
    assert!(
        !sql.contains("AS \"collectedFeesUSD\""),
        "no brace does not make a conditional unconditional:\n{sql}"
    );

    // The same thing on one line, which is the shape where the header and the assignment share a line and
    // the check has to look at the assignment's own text rather than the line above it.
    let one_line = r#"
export function handlePoolCreated(event: PoolCreated): void {
  let pool = new Pool(event.params.pool.toHexString())
  pool.plain = event.params.fee
  if (event.params.tickSpacing > 0) pool.collectedFeesUSD = ZERO_BD
  pool.save()
}
"#;
    let (_nest, result) = emitted_nest(CONSTANT_SCHEMA, one_line);
    let view = result.views.iter().find(|v| v.entity == "Pool").unwrap();
    let sql = select_sql(&view.sql);
    assert!(
        !sql.contains("AS \"collectedFeesUSD\""),
        "nor on one line:\n{sql}"
    );
}

/// A row already saved before the assignment can be stored without it (Jules on #1316).
///
/// The early-`return` case, stated the way that makes it decidable: a `return` before the *save* stores
/// nothing and leaves no row missing the field, but a `save()` before the assignment persists a row that
/// the assignment had not reached yet.
#[test]
fn a_constant_assigned_after_a_save_is_not_answered_with_its_literal() {
    let mapping = r#"
export function handlePoolCreated(event: PoolCreated): void {
  let pool = new Pool(event.params.pool.toHexString())
  pool.plain = event.params.fee
  pool.save()
  if (event.params.tickSpacing > 0) {
    return
  }
  pool.collectedFeesUSD = ZERO_BD
  pool.save()
}
"#;
    let (_nest, result) = emitted_nest(CONSTANT_SCHEMA, mapping);
    let view = result.views.iter().find(|v| v.entity == "Pool").unwrap();
    let sql = select_sql(&view.sql);
    assert!(
        !sql.contains("AS \"collectedFeesUSD\""),
        "a row stored before the assignment never got it:\n{sql}"
    );
}

/// A `return` *before* the save leaves no row at all, so it does not make the assignment conditional.
///
/// Uniswap V4's `handleInitialize` returns three times - a skip list and two null-decimal bails - before
/// `pool.save()`. Refusing on any earlier `return` would cost all six of `Pool`'s initialiser constants
/// for paths that store nothing.
#[test]
fn a_return_before_the_save_does_not_make_a_constant_conditional() {
    let mapping = r#"
export function handlePoolCreated(event: PoolCreated): void {
  if (event.params.tickSpacing > 0) {
    return
  }
  let pool = new Pool(event.params.pool.toHexString())
  pool.plain = event.params.fee
  pool.collectedFeesUSD = ZERO_BD
  pool.save()
}
"#;
    let (_nest, result) = emitted_nest(CONSTANT_SCHEMA, mapping);
    let view = result.views.iter().find(|v| v.entity == "Pool").unwrap();
    let sql = select_sql(&view.sql);
    assert!(
        sql.contains("0 AS \"collectedFeesUSD\""),
        "a path that stores no row cannot leave one missing the field:\n{sql}"
    );
}

/// A function that never saves hands the row to its caller, and the value then depends on a path this
/// cannot see (Jules on #1316: `if (event.skip) return transaction; transaction.timestamp = ..`).
#[test]
fn a_constant_in_a_function_that_never_saves_is_not_answered() {
    let mapping = r#"
export function initPool(event: PoolCreated, pool: Pool): void {
  pool.collectedFeesUSD = ZERO_BD
}

export function handlePoolCreated(event: PoolCreated): void {
  let pool = new Pool(event.params.pool.toHexString())
  pool.plain = event.params.fee
  initPool(event, pool)
  pool.save()
}
"#;
    let (_nest, result) = emitted_nest(CONSTANT_SCHEMA, mapping);
    let view = result.views.iter().find(|v| v.entity == "Pool").unwrap();
    let sql = select_sql(&view.sql);
    assert!(
        !sql.contains("AS \"collectedFeesUSD\""),
        "whether the caller saved this row after the write is not visible here:\n{sql}"
    );
}

/// A **loaded** row already exists, and needs no save to keep its old value (Jules on #1316).
///
/// `let pool = Pool.load(id)!; if (event.skip) return; pool.collectedFeesUSD = ZERO_BD; pool.save()` has
/// every save after the assignment, so the persistence rule alone accepts it - and on the early-return
/// path the stored row never saw the assignment. The `new` case two tests up must stay accepted, because
/// a row that was never saved is not a row.
#[test]
fn a_loaded_receiver_with_an_early_return_is_not_answered_with_its_literal() {
    let mapping = r#"
export function handlePoolCreated(event: PoolCreated): void {
  let pool = Pool.load(event.params.pool.toHexString())
  if (event.params.tickSpacing > 0) {
    return
  }
  pool.plain = event.params.fee
  pool.collectedFeesUSD = ZERO_BD
  pool.save()
}
"#;
    let (_nest, result) = emitted_nest(CONSTANT_SCHEMA, mapping);
    let view = result.views.iter().find(|v| v.entity == "Pool").unwrap();
    let sql = select_sql(&view.sql);
    assert!(
        !sql.contains("AS \"collectedFeesUSD\""),
        "the returning path leaves a stored row that never saw the assignment:\n{sql}"
    );

    // `return` is not the only way out. AssemblyScript's `assert` throws on a false condition, and a throw
    // is a deterministic error whose block graph-node discards - so the loaded row keeps what an earlier
    // block stored, having never reached the assignment (Jules on #1316).
    for terminator in [
        "assert(event.params.tickSpacing > 0)",
        "throw new Error('no')",
    ] {
        let aborting = format!(
            r#"
export function handlePoolCreated(event: PoolCreated): void {{
  let pool = Pool.load(event.params.pool.toHexString())
  {terminator}
  pool.plain = event.params.fee
  pool.collectedFeesUSD = ZERO_BD
  pool.save()
}}
"#
        );
        let (_nest, result) = emitted_nest(CONSTANT_SCHEMA, &aborting);
        let view = result.views.iter().find(|v| v.entity == "Pool").unwrap();
        let sql = select_sql(&view.sql);
        assert!(
            !sql.contains("AS \"collectedFeesUSD\""),
            "`{terminator}` can end the handler before the assignment:\n{sql}"
        );
    }
}

/// **The creation guard is answered** (#1326).
///
/// ```ts
/// let pool = Pool.load(id)
/// if (pool === null) {
///   pool = new Pool(id)
///   pool.collectedFeesUSD = ZERO_BD
/// }
/// ```
///
/// The commonest shape in subgraph-land - load, guard on null, construct, initialise every field - and
/// the brace-depth rule refused all of it. A row that skipped the branch was created by an earlier
/// execution of that same branch, so it holds what the branch put there. Uniswap V4 writes
/// `Token.totalValueLockedUSDUntracked` this way, and refusing it cost a field whose value genuinely is
/// always zero.
#[test]
fn a_constant_set_where_the_row_is_created_is_answered() {
    let mapping = r#"
export function handlePoolCreated(event: PoolCreated): void {
  let pool = Pool.load(event.params.pool.toHexString())
  if (pool === null) {
    pool = new Pool(event.params.pool.toHexString())
    pool.collectedFeesUSD = ZERO_BD
  }
  pool.plain = event.params.fee
  pool.save()
}
"#;
    let (_nest, result) = emitted_nest(CONSTANT_SCHEMA, mapping);
    let view = result.views.iter().find(|v| v.entity == "Pool").unwrap();
    let sql = select_sql(&view.sql);
    assert!(
        sql.contains("0 AS \"collectedFeesUSD\""),
        "every row that exists came through the branch that created it:\n{sql}"
    );
}

/// A terminator between the construction and the assignment abandons a row nothing saved.
///
/// Uniswap V4's real shape: construct `token0`, fetch its decimals, `return` if they are null, then set
/// the constants. My first version of the creation-guard rule refused on that `return` and so measured
/// zero - the very field it was written for. The save-ordering rule already carries this: no
/// `<recv>.save()` precedes the assignment, so a terminator before it leaves nothing stored.
#[test]
fn a_terminator_before_any_save_abandons_the_row_rather_than_leaving_it_unassigned() {
    let mapping = r#"
export function handlePoolCreated(event: PoolCreated): void {
  let pool = Pool.load(event.params.pool.toHexString())
  if (pool === null) {
    pool = new Pool(event.params.pool.toHexString())
    if (event.params.tickSpacing > 0) {
      return
    }
    pool.collectedFeesUSD = ZERO_BD
  }
  pool.plain = event.params.fee
  pool.save()
}
"#;
    let (_nest, result) = emitted_nest(CONSTANT_SCHEMA, mapping);
    let view = result.views.iter().find(|v| v.entity == "Pool").unwrap();
    let sql = select_sql(&view.sql);
    assert!(
        sql.contains("0 AS \"collectedFeesUSD\""),
        "nothing saved the row before the handler left, so no stored row lacks the value:\n{sql}"
    );
}

/// A block that does not construct the receiver is not a creation guard, however it is spelled.
#[test]
fn a_block_that_constructs_a_different_local_is_not_a_creation_guard() {
    let mapping = r#"
export function handlePoolCreated(event: PoolCreated): void {
  let pool = Pool.load(event.params.pool.toHexString())
  if (pool === null) {
    let other = new Pool(event.block.hash.toHexString())
    other.save()
    pool.collectedFeesUSD = ZERO_BD
  }
  pool.plain = event.params.fee
  pool.save()
}
"#;
    let (_nest, result) = emitted_nest(CONSTANT_SCHEMA, mapping);
    let view = result.views.iter().find(|v| v.entity == "Pool").unwrap();
    let sql = select_sql(&view.sql);
    assert!(
        !sql.contains("AS \"collectedFeesUSD\""),
        "`other` being constructed says nothing about the row `pool` holds:\n{sql}"
    );
}

/// The **innermost** block decides, not any enclosing one.
///
/// The row is created in the outer guard, but the constant is set only inside a further branch. Taking the
/// outermost enclosing block would find the construction and answer; the innermost one does not construct,
/// and a creation that skipped the inner branch set nothing.
#[test]
fn a_branch_nested_inside_the_creation_guard_is_not_itself_one() {
    let mapping = r#"
export function handlePoolCreated(event: PoolCreated): void {
  let pool = Pool.load(event.params.pool.toHexString())
  if (pool === null) {
    pool = new Pool(event.params.pool.toHexString())
    if (event.params.tickSpacing > 0) {
      pool.collectedFeesUSD = ZERO_BD
    }
  }
  pool.plain = event.params.fee
  pool.save()
}
"#;
    let (_nest, result) = emitted_nest(CONSTANT_SCHEMA, mapping);
    let view = result.views.iter().find(|v| v.entity == "Pool").unwrap();
    let sql = select_sql(&view.sql);
    assert!(
        !sql.contains("AS \"collectedFeesUSD\""),
        "a creation that took the other branch set nothing:\n{sql}"
    );
}

/// A construction nested **inside** a further branch is not the block's creation (Jules on #1326).
///
/// ```ts
/// if (a) {
///   if (b) { pool = new Pool(id) }
///   pool.collectedFeesUSD = ZERO_BD      // runs when a holds and b does not
/// }
/// ```
///
/// Scanning the whole block for a `new` found one and answered the field. On the path where `a` holds and
/// `b` does not, an existing row is assigned - and a row created by some earlier call that also took
/// `b`-false was saved without it.
#[test]
fn a_construction_nested_in_a_further_branch_is_not_the_creation() {
    let mapping = r#"
export function handlePoolCreated(event: PoolCreated): void {
  let pool = Pool.load(event.params.pool.toHexString())
  if (event.params.tickSpacing > 0) {
    if (event.params.fee > 0) {
      pool = new Pool(event.params.pool.toHexString())
    }
    pool.collectedFeesUSD = ZERO_BD
  }
  pool.plain = event.params.fee
  pool.save()
}
"#;
    let (_nest, result) = emitted_nest(CONSTANT_SCHEMA, mapping);
    let view = result.views.iter().find(|v| v.entity == "Pool").unwrap();
    let sql = select_sql(&view.sql);
    assert!(
        !sql.contains("AS \"collectedFeesUSD\""),
        "the assignment runs on a path the construction does not:\n{sql}"
    );
}
