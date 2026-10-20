//! The files `nuthatch init` writes, as contents rather than writes (RFC-0066 S0). The node resolves
//! the chain, the ABIs and the start blocks, then writes what this returns; a browser tab resolves the
//! same inputs its own way and gets the same bytes, so the nest it builds is the nest a node indexes.

use std::collections::BTreeMap;

use alloy_json_abi::JsonAbi;
use alloy_primitives::Address;
use anyhow::{anyhow, bail, Context, Result};
use nuthatch_decode::registry::{ContractSpec, DecodeRegistry, TableSchema};
use serde::Serialize;

pub mod resolve;
pub mod semantic;

/// One contract as `init` resolved it.
#[derive(Debug, Clone)]
pub struct Contract {
    pub alias: String,
    /// Lowercase hex, `0x`-prefixed.
    pub address: String,
    pub start_block: Option<u64>,
    pub abi: serde_json::Value,
}

/// Everything `init` decides before it writes a file.
#[derive(Debug, Clone)]
pub struct Nest {
    pub name: String,
    pub chain: String,
    pub chain_id: u64,
    pub rpc_urls: Vec<String>,
    pub block_timestamps: bool,
    pub contracts: Vec<Contract>,
}

impl Contract {
    pub fn abi_path(&self) -> String {
        format!("abis/{}.json", self.alias)
    }
}

/// Every file `init` writes for `nest`, by path relative to the nest root.
pub fn files(nest: &Nest) -> Result<BTreeMap<String, Vec<u8>>> {
    let mut out = BTreeMap::new();
    for c in &nest.contracts {
        let abi = serde_json::to_string_pretty(&c.abi).context("failed to serialise ABI")?;
        out.insert(c.abi_path(), abi.into_bytes());
    }
    out.insert("nuthatch.toml".into(), config_toml(nest)?.into_bytes());
    let registry = registry(nest)?;
    let schema = registry.schema();
    out.insert(
        "schema.json".into(),
        schema_json(registry.hash(), &schema)?.into_bytes(),
    );
    let contracts: Vec<_> = nest
        .contracts
        .iter()
        .map(|c| (c.alias.as_str(), c.address.as_str()))
        .collect();
    out.insert(
        "llms.txt".into(),
        llms_txt(&nest.chain, &contracts, &schema).into_bytes(),
    );
    out.insert(
        ".claude/skills/nuthatch/SKILL.md".into(),
        skill_md(&nest.chain, &contracts, &schema).into_bytes(),
    );
    out.insert(".gitattributes".into(), GITATTRIBUTES.into());
    out.insert("views/README.md".into(), VIEWS_README.into());
    out.insert(
        "views/10-example.sql".into(),
        starter_view(&schema).into_bytes(),
    );
    let sem = semantic::generate(&schema, &nest.name, &nest.chain);
    out.insert("semantic.toml".into(), semantic::render(&sem)?.into_bytes());
    Ok(out)
}

#[derive(Serialize)]
struct Config<'a> {
    nest: NestToml<'a>,
    contracts: Vec<ContractToml<'a>>,
}

#[derive(Serialize)]
struct NestToml<'a> {
    name: &'a str,
    chain: &'a str,
    chain_id: u64,
    rpc_urls: &'a [String],
    schema_version: u32,
    // Written unconditionally, so every scaffolded nest states its schema shape in the file. Someone
    // reading `nuthatch.toml` to work out why a table has no `block_timestamp` finds the answer there.
    block_timestamps: bool,
}

#[derive(Serialize)]
struct ContractToml<'a> {
    alias: &'a str,
    address: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    start_block: Option<u64>,
    abi: String,
}

/// `nuthatch.toml` as the node's `Config` serialises the one `init` builds. The node holds the two
/// together: `project::tests` reloads this text into a `Config` and requires it to save back unchanged.
pub fn config_toml(nest: &Nest) -> Result<String> {
    let config = Config {
        nest: NestToml {
            name: &nest.name,
            chain: &nest.chain,
            chain_id: nest.chain_id,
            rpc_urls: &nest.rpc_urls,
            // A timestamped nest is a v1 file and stays readable by 0.8.x
            // (`config::required_schema_version`).
            schema_version: if nest.block_timestamps { 1 } else { 2 },
            block_timestamps: nest.block_timestamps,
        },
        contracts: nest
            .contracts
            .iter()
            .map(|c| ContractToml {
                alias: &c.alias,
                address: &c.address,
                start_block: c.start_block,
                abi: c.abi_path(),
            })
            .collect(),
    };
    toml::to_string_pretty(&config).context("failed to serialise config")
}

/// The decode registry `nest` indexes under, as the node builds it from the written files.
pub fn registry(nest: &Nest) -> Result<DecodeRegistry> {
    let mut specs = Vec::with_capacity(nest.contracts.len());
    for c in &nest.contracts {
        let abi: JsonAbi = serde_json::from_value(c.abi.clone())
            .with_context(|| format!("parsing ABI {}", c.abi_path()))?;
        let address = parse_address(&c.address)?;
        if address.is_zero() {
            bail!(
                "contract `{}` is the zero address, which is not a contract nuthatch will index",
                c.alias
            );
        }
        specs.push(ContractSpec {
            alias: c.alias.clone(),
            address,
            abi,
            events: Vec::new(),
        });
    }
    Ok(DecodeRegistry::build_with_templates(specs, Vec::new())?
        .with_timestamps(nest.block_timestamps))
}

fn parse_address(s: &str) -> Result<Address> {
    let bytes = hex::decode(s.trim_start_matches("0x")).context("bad address hex")?;
    if bytes.len() != 20 {
        return Err(anyhow!("address is not 20 bytes"));
    }
    Ok(Address::from_slice(&bytes))
}

/// `schema.json`: the decode identity and every table the nest declares.
pub fn schema_json(registry_hash: [u8; 32], schema: &[TableSchema]) -> Result<String> {
    Ok(serde_json::to_string_pretty(&serde_json::json!({
        "registry_hash": format!("0x{}", hex::encode(registry_hash)),
        "tables": schema,
    }))?)
}

/// GitHub counts SQL as data, so without this a nest repo shows no language. Hidden, so it stays
/// outside the data identity. `-text` keeps a CRLF checkout from moving a clone's NID (#2199).
pub const GITATTRIBUTES: &str = "* -text\n*.sql linguist-detectable\nabis/** linguist-generated\n";

pub const VIEWS_README: &str =
        "# views/ - this nest's authored logic (RFC-0018 §1)\n\n\
         Drop `*.sql` files here, each a `CREATE VIEW …` over your nest's tables (the live tip ∪ sealed\n\
         history, one surface - recomputed per query, never materialised). Query a view by name with\n\
         `nuthatch sql` or the MCP `sql` tool. Files load in sorted filename order (`10-…`, `20-…`), so\n\
         a later view can build on an earlier one. Describe what a view *means* in `semantic.toml` under\n\
         `[view.<name>]` so an agent sees it. A broken/drifted view fails `nuthatch check` loudly.
         \
         `views/` is the broad query-time surface. `entities/` plus `entities.toml` is separate: use it
         only for a keyed relation in the bounded incremental SQL subset, maintained at indexing time
         with explicit memory, reorg and restart semantics. `nuthatch check` refuses unsupported entity
         SQL rather than silently turning an ordinary view into stored state.\n";

/// `views/10-example.sql`: entirely comments, so it validates clean until the author uncomments it.
/// It names this nest's real first table when there is one, so it's copy-paste-true.
pub fn starter_view(schema: &[TableSchema]) -> String {
    let (table, alias) = schema
        .first()
        .map(|t| (t.table.as_str(), t.alias.as_str()))
        .unwrap_or(("your__event", "your"));
    format!(
        "-- views/10-example.sql - an authored derivation this nest computes. Uncomment to enable.\n\
         --\n\
         -- Read-only SQL over your nest's tables (tip ∪ sealed history), recomputed per query. Query it\n\
         -- by name via `nuthatch sql` or the MCP; describe it in semantic.toml `[view.<name>]`.\n\
         --\n\
         -- Footguns (see the builder skill's views.md):\n\
         --   • reserved-word columns like \"from\"/\"to\" must be double-quoted\n\
         --   • SUM(<col>_dec) is the values that fit; WHERE NOT <col>_overflow is that sum\n\
         --     ids, nonces and hashes stay raw (`_dec` is NULL for a full-width uint256)\n\
         --\n\
         -- Example over this nest's `{table}` table:\n\
         --\n\
         -- CREATE VIEW {alias}_activity AS\n\
         --   SELECT count(*) AS events,\n\
         --          min(block_number) AS first_block,\n\
         --          max(block_number) AS last_block\n\
         --   FROM \"{table}\";\n"
    )
}

/// `llms.txt`: how an AI agent queries this nest. `contracts` is (alias, address).
pub fn llms_txt(chain: &str, contracts: &[(&str, &str)], schema: &[TableSchema]) -> String {
    let list = contract_list(contracts);
    let tables: String = schema
        .iter()
        .map(|t| {
            let cols: Vec<String> = t.columns.iter().map(|c| c.name.clone()).collect();
            format!("- `{}` - {} ({})\n", t.table, t.event, cols.join(", "))
        })
        .collect();
    format!(
        "# nuthatch nest on {chain}\n\
         \n\
         A self-hosted blockchain index. Query it locally; there is no third-party API.\n\
         \n\
         ## Contracts\n{list}\n\
         ## Tables (one per contract event)\n{tables}\n\
         ## Live HTTP API (run `nuthatch dev`)\n\
         - `GET /`                    index status\n\
         - `GET /tables`              every table with its columns\n\
         - `GET /table/{{name}}?limit=N` recent rows of one table (hot + sealed)\n\
         - `GET /entity/{{id}}`         one row by id (`{{block:012}}-{{logindex:06}}`)\n\
         - `GET /sql?q=SELECT...`     read-only SQL; each table is a view named `{{alias}}__{{event}}`, and each\n\
         \x20                            authored view (`views/*.sql`) answers by its own name\n\
         - `GET /balances?limit=N`    top holder balances (when an ERC-20 Transfer table is present)\n\
         - `GET /balance/{{address}}`   one address's derived balance\n\
         - `GET /queries`             the mount's SQL mode and the queries its operator declared in\n\
         \x20                            `mounts.toml` (none on an open nest); `GET /q/{{name}}` runs one\n\
         - `GET /derived`             the nest's incremental entities (`entities.toml`); `GET /derived/{{entity}}` reads one\n\
         \n\
         ## MCP (for coding agents)\n\
         Run `nuthatch mcp` (stdio) to expose tools: status, schema, tables, table, sql, entity,\n\
         balance, top_balances. Fully offline against the local instance; nothing phones home.\n"
    )
}

/// `.claude/skills/nuthatch/SKILL.md`: the scaffolded agent skill.
pub fn skill_md(chain: &str, contracts: &[(&str, &str)], schema: &[TableSchema]) -> String {
    let list = contract_list(contracts);
    // A table this nest really has, so an agent copying the example gets an answer (#1662).
    let example = schema
        .first()
        .map(|t| t.table.as_str())
        .unwrap_or("<alias>__<event>");
    format!(
        "---\n\
         name: nuthatch\n\
         description: Query this self-hosted nuthatch nest on {chain} - decoded events, balances, \
         and read-only SQL. Use when asked about on-chain activity for these contracts.\n\
         ---\n\
         \n\
         # Querying the nuthatch nest\n\
         \n\
         Contracts indexed on {chain}:\n{list}\n\
         Data is local - never call an external API for it.\n\
         \n\
         ## Preferred: MCP\n\
         If a `nuthatch` MCP server is configured, use its tools. Call `schema` first to learn the\n\
         data model, then `sql` / `entity` / `balance` / `top_balances`.\n\
         \n\
         ## Fallback: HTTP (a `nuthatch dev` must be running)\n\
         - Recent rows:  `curl localhost:8288/entities?limit=20`\n\
         - Read-only SQL: `curl -G localhost:8288/sql --data-urlencode 'q=SELECT count(*) FROM {example}'`\n\
         \n\
         `sql` reads hot and sealed rows together, so it covers the live tip. Each answer's\n\
         `provenance.sealed_through` is where finalized data ends.\n"
    )
}

fn contract_list(contracts: &[(&str, &str)]) -> String {
    contracts
        .iter()
        .map(|(alias, address)| format!("- `{alias}` = {address}\n"))
        .collect()
}
