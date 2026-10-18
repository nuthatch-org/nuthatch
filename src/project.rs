//! `nuthatch init` - resolve each contract's ABI and scaffold a nest (RFC-0001: N contracts).
//! `nuthatch add` - resolve one more contract's ABI and grow an existing nest, no re-init.

use anyhow::{bail, Context, Result};
use std::path::{Path, PathBuf};

use crate::abi;
use crate::chains;
use crate::cli::{AddArgs, InitArgs};
use crate::config::{Config, Contract, Extract, Nest};
use crate::rpc::RpcClient;
#[cfg(test)]
use nuthatch_scaffold::resolve::slugify_alias;
use nuthatch_scaffold::resolve::{
    contract_name_from_abi, impl_from_slot, is_valid_alias, normalise_address, pick_alias,
    IMPLEMENTATION_SELECTOR, PROXY_BEACON_SLOT, PROXY_IMPL_SLOTS,
};

pub async fn init(args: InitArgs) -> Result<()> {
    let count = crate::count::Pending::of(&args); // RFC-0054 hook
    scaffold(args).await?;
    count.ask().await; // RFC-0054 hook
    Ok(())
}

async fn scaffold(args: InitArgs) -> Result<()> {
    // Three ways to start a nest: clone/copy a published one (`--from`), port a subgraph
    // (`--from-subgraph`), or resolve from addresses.
    if let Some(source) = args.from.clone() {
        return init_from(&source, &args.dir);
    }
    if let Some(source) = args.from_subgraph.clone() {
        return init_from_subgraph(&source, &args).await;
    }
    if args.addresses.is_empty() {
        bail!("provide one or more contract addresses, --from <git-url|dir>, or --from-subgraph <cid>");
    }
    // Chain identity: honour an explicit `--chain`, otherwise detect it. The first-run friction we
    // most want to delete is making the user know (and correctly spell) which chain their contract
    // is on - so when they don't say, we go and find out.
    let chain = match &args.chain {
        Some(name) => chains::resolve(name, &args.rpc).await?,
        None if args.rpc.is_empty() => detect_chain(&args.addresses).await?.into(),
        None => detect_chain_on_rpc(&args.addresses, &args.rpc).await?,
    };
    let dir = PathBuf::from(&args.dir);
    std::fs::create_dir_all(dir.join("abis"))
        .with_context(|| format!("cannot create {}", dir.display()))?;

    let addresses: Vec<String> = args
        .addresses
        .iter()
        .map(|a| normalise_address(a))
        .collect::<Result<_>>()?;
    if !args.alias.is_empty() && args.alias.len() != addresses.len() {
        bail!(
            "{} aliases for {} address(es) - provide one alias per address or none",
            args.alias.len(),
            addresses.len()
        );
    }

    // An explicit `--rpc` is the whole endpoint pool, both for first-run resolution and in the
    // persisted config. Otherwise use the chain defaults.
    let rpc_urls = crate::rpc::select_rpcs(&args.rpc, chain.rpc_urls.iter().map(|s| s.to_string()));

    // One RPC client for best-effort deployment-block detection. Verified first, which cools an
    // endpoint that does not answer before its 20 s timeout lands on the tip lookup.
    let rpc = RpcClient::new(rpc_urls.clone())?;
    rpc.verify_chain_ids(chain.chain_id).await?;
    let tip = rpc.block_number().await.ok();

    let overrides = resolve_abi_overrides(&args.abi, addresses.len())?;
    let start_blocks = resolve_start_blocks(&args.start_block, addresses.len())?;

    let mut contracts = Vec::with_capacity(addresses.len());
    let mut aliases: Vec<String> = Vec::with_capacity(addresses.len());
    let mut undetected: Vec<String> = Vec::new();
    let used: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for (i, address) in addresses.iter().enumerate() {
        let (abi_json, implementation, contract_name) = match &overrides[i] {
            Some(path) => {
                println!("→ using local ABI {path} for {address}");
                let (abi, name) = read_local_abi(path)?;
                (abi, None, name)
            }
            None => {
                println!("→ resolving ABI for {address} on {}…", chain.name);
                let resolved =
                    resolve_abi(&rpc, chain.chain_id, address, args.explorer.as_deref()).await?;
                (
                    resolved.abi,
                    resolved.implementation,
                    resolved.contract_name,
                )
            }
        };
        let provided = args.alias.get(i).map(|s| s.as_str());
        let name = contract_name
            .as_deref()
            .or_else(|| contract_name_from_abi(&abi_json));
        let alias = pick_alias(provided, name, &used, &aliases)?;
        println!("  · alias {alias}");
        aliases.push(alias);
        let alias = aliases.last().unwrap();

        let start_block =
            resolve_contract_start(&rpc, address, tip, start_blocks[i], &mut undetected).await;

        report_proxy_history_gap(&rpc, implementation.as_deref(), start_block, tip, alias).await;

        // Does the ABI we just vendored actually decode what this address emits? Best-effort and
        // never fatal - but loud when the answer is no, because the alternative is a nest that
        // indexes nothing and says nothing about it.
        report_abi_fit(
            check_abi_fits(&rpc, address, &abi_json, tip, start_block, chain.log_window).await,
            alias,
            address,
        );

        contracts.push(nuthatch_scaffold::Contract {
            alias: alias.clone(),
            address: address.clone(),
            start_block,
            abi: abi_json,
        });
    }

    let nest = nuthatch_scaffold::Nest {
        name: nest_name(&dir),
        chain: chain.name.clone(),
        chain_id: chain.chain_id,
        rpc_urls,
        block_timestamps: !args.no_timestamps,
        contracts,
    };
    // The config and ABIs come from the scaffold as a tab gets them; the rest is written by
    // `write_nest_artifacts`, which leaves an author's existing views and semantics alone.
    for (path, bytes) in nuthatch_scaffold::files(&nest)? {
        if path == crate::config::CONFIG_FILE || path.starts_with("abis/") {
            std::fs::write(dir.join(&path), bytes)
                .with_context(|| format!("failed to write {path}"))?;
        }
    }
    let config = Config::load(&dir)?;

    // Build the registry from the vendored ABIs to generate the schema artifact + AI surface (one
    // source of truth: schema.json, llms.txt, the skill, and `/tables` all come from here).
    let table_count = write_nest_artifacts(&dir, &chain.name, &config)?;
    let skipped = crate::registry::from_nest(&dir, &config)
        .map(|r| r.skipped_anonymous())
        .unwrap_or(0);

    println!(
        "{}",
        scaffold_summary(
            &config.nest.name,
            config.contracts.len(),
            table_count,
            skipped,
            &dir.display().to_string(),
        )
    );
    println!("    nuthatch.toml              config");
    println!("    abis/                      resolved ABIs");
    println!("    schema.json                decoded tables + columns");
    println!("    semantic.toml              what the data means (edit freely)");
    println!("    views/                     authored SQL derivations (a commented starter to uncomment)");
    println!("    llms.txt                   how an AI agent queries this index");
    println!("    .claude/skills/nuthatch/   Claude Code skill (offline, no phone-home)");
    println!();
    println!("next:  nuthatch dev{}", dir_hint(&args.dir));
    if let Some(hint) = recent_history_hint(
        config.contracts.iter().filter_map(|c| c.start_block).min(),
        tip,
        chain.log_window,
        &args.dir,
    ) {
        println!("{hint}");
    }
    println!("       nuthatch mcp   (expose this index to a coding agent over MCP)");
    print_undetected_start_blocks(&undetected);
    if let Some(note) = chains::keyless_caveat(
        &chain.name,
        &config.nest.rpc_urls,
        config.nest.block_timestamps,
    ) {
        println!();
        println!("note:  {note}");
    }
    Ok(())
}

/// `nuthatch add 0xAnother` - grow an existing nest with more contracts without re-`init`. This is
/// the natural "one or many contracts" flow (RFC-0001): the chain, RPC endpoints and the rest of the
/// config are already settled by `init`, so `add` only resolves each new contract's ABI, vendors it,
/// appends it to `nuthatch.toml`, and regenerates the derived artifacts (schema.json + the AI
/// surface). The next `dev` backfills the new contract from its own deployment block - the existing
/// contracts resume from their stored cursor, untouched.
pub async fn add(args: AddArgs) -> Result<()> {
    let dir = PathBuf::from(&args.dir);
    let mut config = Config::load(&dir).with_context(|| {
        format!(
            "no nest at '{}' (run `nuthatch init` first, or pass --dir)",
            dir.display()
        )
    })?;
    // The chain is the nest's, already chosen at init - never re-detected. Adding a contract that
    // lives on a different chain is a different nest (one cursor, one chain - non-negotiable).
    let chain = chains::from_config(&config.nest.chain, config.nest.chain_id);

    let new_addresses: Vec<String> = args
        .addresses
        .iter()
        .map(|a| normalise_address(a))
        .collect::<Result<_>>()?;
    // Refuse duplicates: a contract already in the nest must not be added twice (it would collide on
    // the alias/ABI and double-register decoders).
    for addr in &new_addresses {
        if config
            .contracts
            .iter()
            .any(|c| c.address.eq_ignore_ascii_case(addr))
        {
            bail!("{addr} is already in this nest");
        }
    }
    if !args.alias.is_empty() && args.alias.len() != new_addresses.len() {
        bail!(
            "{} aliases for {} address(es) - provide one alias per address or none",
            args.alias.len(),
            new_addresses.len()
        );
    }

    let rpc_urls = crate::rpc::select_rpcs(&args.rpc, config.nest.rpc_urls.iter().cloned());
    let rpc = RpcClient::new(rpc_urls)?;
    let tip = rpc.block_number().await.ok();

    std::fs::create_dir_all(dir.join("abis"))
        .with_context(|| format!("cannot create {}", dir.join("abis").display()))?;

    let overrides = resolve_abi_overrides(&args.abi, new_addresses.len())?;
    let start_blocks = resolve_start_blocks(&args.start_block, new_addresses.len())?;

    let mut new_aliases: Vec<String> = Vec::with_capacity(new_addresses.len());
    let mut undetected: Vec<String> = Vec::new();
    for (i, address) in new_addresses.iter().enumerate() {
        let (abi_json, implementation, contract_name) = match &overrides[i] {
            Some(path) => {
                println!("→ using local ABI {path} for {address}");
                let (abi, name) = read_local_abi(path)?;
                (abi, None, name)
            }
            None => {
                println!("→ resolving ABI for {address} on {}…", chain.name);
                let resolved =
                    resolve_abi(&rpc, chain.chain_id, address, args.explorer.as_deref()).await?;
                (
                    resolved.abi,
                    resolved.implementation,
                    resolved.contract_name,
                )
            }
        };
        let used: std::collections::HashSet<&str> =
            config.contracts.iter().map(|c| c.alias.as_str()).collect();
        let provided = args.alias.get(i).map(|s| s.as_str());
        let name = contract_name
            .as_deref()
            .or_else(|| contract_name_from_abi(&abi_json));
        let alias = pick_alias(provided, name, &used, &new_aliases)?;
        println!("  · alias {alias}");
        new_aliases.push(alias);
        let alias = new_aliases.last().unwrap();
        let abi_path = format!("abis/{alias}.json");
        std::fs::write(
            dir.join(&abi_path),
            serde_json::to_string_pretty(&abi_json).context("failed to serialise ABI")?,
        )
        .with_context(|| format!("failed to write {abi_path}"))?;

        let start_block =
            resolve_contract_start(&rpc, address, tip, start_blocks[i], &mut undetected).await;

        report_proxy_history_gap(&rpc, implementation.as_deref(), start_block, tip, alias).await;

        report_abi_fit(
            check_abi_fits(&rpc, address, &abi_json, tip, start_block, chain.log_window).await,
            alias,
            address,
        );

        config.contracts.push(Contract {
            alias: alias.clone(),
            address: address.clone(),
            start_block,
            abi: abi_path,
            events: Vec::new(),
        });
    }

    config.save(&dir)?;
    let table_count = write_nest_artifacts(&dir, &chain.name, &config)?;

    println!(
        "✓ added {} contract(s); nest '{}' now has {} contract(s), {} table(s)",
        new_addresses.len(),
        config.nest.name,
        config.contracts.len(),
        table_count,
    );
    println!(
        "next:  nuthatch dev{}   (backfills the new contract(s) from deployment)",
        dir_hint(&args.dir)
    );
    print_undetected_start_blocks(&undetected);
    Ok(())
}

/// `nuthatch schema` - regenerate the derived artifacts (`schema.json`, `llms.txt`, `semantic.toml`
/// footguns) from the current `nuthatch.toml`. The manual counterpart to what `init`/`add` do
/// automatically: run it after hand-editing the config - notably adding a factory `[[templates]]` /
/// `[[factories]]`, which introduces the `{template}__{event}` tables and their `*_dec` columns that
/// the auto path never saw. Idempotent: authored views and semantic descriptions are preserved.
pub fn regen(args: crate::cli::SchemaArgs) -> Result<()> {
    let dir = PathBuf::from(&args.dir);
    let config = Config::load(&dir)
        .with_context(|| format!("no nest at '{}' (need a nuthatch.toml)", dir.display()))?;
    let n = write_nest_artifacts(&dir, &config.nest.chain, &config)?;
    println!("✓ regenerated schema.json + AI surface from nuthatch.toml - {n} table(s)");
    Ok(())
}

/// `nuthatch nest rename-alias <old> <new>` (#671). Declared, not inferred: `merge` still will not
/// guess a rename, because it cannot tell one from a removal.
pub fn rename_alias(dir: &Path, old: &str, new: &str) -> Result<()> {
    if !is_valid_alias(new) {
        bail!("alias '{new}' must match [a-z][a-z0-9_]*");
    }
    if old == new {
        bail!("old and new alias are the same");
    }
    let mut config = Config::load(dir)
        .with_context(|| format!("no nest at '{}' (need a nuthatch.toml)", dir.display()))?;
    let idx = config
        .contracts
        .iter()
        .position(|c| c.alias == old)
        .ok_or_else(|| anyhow::anyhow!("no contract with alias '{old}' in this nest"))?;
    if config.contracts.iter().any(|c| c.alias == new) {
        bail!("alias '{new}' is already used in this nest");
    }
    let old_abi = dir.join(&config.contracts[idx].abi);
    let new_rel = format!("abis/{new}.json");
    let new_abi = dir.join(&new_rel);
    if old_abi.exists() && old_abi != new_abi {
        std::fs::rename(&old_abi, &new_abi)
            .with_context(|| format!("moving {} → {}", old_abi.display(), new_abi.display()))?;
    }
    config.contracts[idx].alias = new.to_string();
    config.contracts[idx].abi = new_rel;
    config.save(dir)?;
    if let Some(mut sem) = crate::semantic::load(dir)? {
        crate::semantic::rekey_alias(&mut sem, old, new);
        crate::semantic::save(dir, &sem)?;
    }
    write_nest_artifacts(dir, &config.nest.chain, &config)?;
    println!("✓ renamed alias {old} → {new}");
    Ok(())
}

/// Regenerate the derived artifacts **if they are missing or older than `nuthatch.toml`**, returning
/// what it did so the caller can say so.
///
/// A hand-written `nuthatch.toml` has no `schema.json`, and the consequence is worse than a missing
/// file: `schema.json` is what creates the derived `{col}_dec` columns, while the *advice* to use them
/// comes from the live registry. So the schema tool confidently recommended `delta → delta_dec` on 196
/// lines while **zero** `_dec` columns existed, and an agent following that advice got
/// `Binder Error: Referenced column "delta_dec" not found` (issue #241 item 2).
///
/// The AI surface being confidently wrong is the most expensive place to be wrong, so this does not
/// warn - it fixes. Regenerating is cheap, idempotent, and preserves authored views and semantic
/// descriptions; `dev` already rebuilds the child registry on startup for the same reason.
pub fn refresh_stale_artifacts(dir: &Path, config: &Config) -> Result<Option<String>> {
    let schema = dir.join("schema.json");
    let toml = dir.join(crate::config::CONFIG_FILE);
    let why = if !schema.exists() {
        "schema.json was missing"
    } else {
        // Compare mtimes. An unreadable timestamp on either side means "cannot prove it is fresh",
        // and regenerating costs milliseconds - so the doubt resolves toward correctness.
        let stale = match (
            std::fs::metadata(&schema).and_then(|m| m.modified()),
            std::fs::metadata(&toml).and_then(|m| m.modified()),
        ) {
            (Ok(s), Ok(t)) => s < t,
            _ => true,
        };
        if !stale {
            return Ok(None);
        }
        "schema.json was older than nuthatch.toml"
    };
    let n = write_nest_artifacts(dir, &config.nest.chain, config)?;
    Ok(Some(format!(
        "{why} - regenerated schema.json + AI surface ({n} table(s)). \
         The `{{col}}_dec` companions the schema tool recommends come from this file."
    )))
}

/// Build the registry from the vendored ABIs and (re)write the derived artifacts - `schema.json` and
/// the AI surface (`llms.txt` + the scaffolded skill). One source of truth: `init` and `add` both
/// call this so the artifacts never drift from `nuthatch.toml`. Returns the table count.
/// `nuthatch init --from-subgraph <cid|url>` (#241 item 5).
///
/// Ports a subgraph manifest into a nest: `dataSources` become `[[contracts]]`,
/// `templates` become `[[templates]]`, ABIs are vendored from the CIDs the
/// manifest pins, and `startBlock` carries across so the backfill starts where
/// the subgraph did rather than at genesis.
///
/// The report at the end is the point as much as the config is. A manifest
/// cannot say which template a factory creates — that lives in the mapping
/// WASM — so this states plainly what it mapped, what it skipped, and which
/// templates still need a creating event, rather than emitting a config that
/// looks complete and indexes nothing.
async fn init_from_subgraph(source: &str, args: &InitArgs) -> Result<()> {
    use crate::subgraph_import as sg;
    use std::collections::BTreeMap;

    let gateways: Vec<String> = if args.ipfs.is_empty() {
        sg::DEFAULT_IPFS_GATEWAYS
            .iter()
            .map(|s| s.to_string())
            .collect()
    } else {
        args.ipfs.clone()
    };

    // A path that exists is read from disk; anything else goes to the gateways (#1321). The home it
    // returns is threaded to every ABI read, because whether a `file:` reference means anything is a
    // property of where the manifest came from, not of the reference.
    let (raw, home) = sg::read_manifest(source, &gateways).await?;
    match &home {
        sg::ManifestHome::Local(dir) => {
            println!(
                "→ reading subgraph manifest {source} (local, ABIs resolve under {})…",
                dir.display()
            )
        }
        sg::ManifestHome::Remote => println!("→ fetching subgraph manifest {source}…"),
    }
    let manifest = sg::parse_manifest(&raw)?;

    let network = manifest.network()?;
    // `--chain` wins if given: a manifest's network name and ours can disagree
    // (and a user porting to a fork needs the override).
    let chain = match &args.chain {
        Some(name) => chains::resolve(name, &args.rpc).await?,
        None => chains::lookup(&network)
            .with_context(|| {
                format!(
                    "the manifest indexes '{network}', which nuthatch has no built-in chain for - \
                     re-run with --chain <name> --rpc <url> to point at it yourself"
                )
            })?
            .into(),
    };

    let dir = PathBuf::from(&args.dir);
    std::fs::create_dir_all(dir.join("abis"))
        .with_context(|| format!("cannot create {}", dir.display()))?;

    let mut notes: Vec<String> = Vec::new();
    let mut taken: BTreeMap<String, u32> = BTreeMap::new();
    // CID -> ABI body, so a shared ABI is downloaded once however many sources pin it.
    let mut fetched: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    let mut contracts: Vec<Contract> = Vec::new();
    let mut address_params: Vec<sg::AddressParam> = Vec::new();
    // Sources whose only handlers are block handlers: no event of theirs was ever decoded (#1941).
    let mut block_only: Vec<&sg::ManifestSource> = Vec::new();

    // ── dataSources → [[contracts]] ──────────────────────────────────────
    for ds in &manifest.data_sources {
        if !ds.is_evm() {
            notes.push(format!(
                "skipped `{}` (kind `{}`) - nuthatch indexes EVM logs, and stores a content \
                 hash as a column value rather than fetching what it points at",
                ds.name, ds.kind
            ));
            continue;
        }
        if ds.events.is_empty() && ds.has_block_handlers && !ds.has_call_handlers {
            block_only.push(ds);
            continue;
        }
        let Some(address) = ds.address.clone() else {
            notes.push(format!(
                "skipped `{}` - an EVM dataSource with no `source.address`",
                ds.name
            ));
            continue;
        };
        // A subgraph may legitimately declare a placeholder it never indexes.
        if address
            .trim_start_matches("0x")
            .trim_matches('0')
            .is_empty()
        {
            notes.push(format!("skipped `{}` - address is {address}", ds.name));
            continue;
        }
        // Normalise before anything is fetched or written. This is the one check that used to
        // abort the whole import, and it fired after the ABI was already on disk and the alias
        // already consumed - a malformed address is the manifest's mistake, and every other
        // malformed field here is a note and a `continue`.
        let address = match normalise_address(&address) {
            Ok(a) => a,
            Err(e) => {
                notes.push(format!("skipped `{}` - {e}", ds.name));
                continue;
            }
        };
        let Some(abi_ref) = ds.own_abi().cloned() else {
            notes.push(format!(
                "skipped `{}` - the manifest pins no ABI for it, so there is nothing to decode",
                ds.name
            ));
            continue;
        };
        if ds.abi_is_fallback() {
            notes.push(format!(
                "`{}` asks for ABI `{}`, which `mapping.abis` does not contain - vendored `{}` \
                 instead; check it decodes what you expect",
                ds.name,
                ds.abi_name.as_deref().unwrap_or("?"),
                abi_ref.name
            ));
        }

        let alias = sg::dedupe_alias(&sg::to_alias(&ds.name), &mut taken);
        let abi_json = fetch_and_vendor_abi(
            &dir,
            &alias,
            &abi_ref,
            &gateways,
            &home,
            &mut notes,
            &mut fetched,
        )
        .await?;
        address_params.extend(sg::address_params(&alias, &abi_json));

        // Carry the manifest's event allowlist: a subgraph that handles only
        // `Transfer` should not silently grow every other event's table.
        let events: Vec<String> = ds
            .events
            .iter()
            .map(|sig| sg::event_name(sig).to_string())
            .collect();
        // An empty allowlist means "every event in the ABI" (see `config::Contract::events`),
        // so a source whose handlers are all block/call handlers lands in exactly the state the
        // comment above says it should not - silently, unless we say so.
        if events.is_empty() && ds.has_call_handlers {
            notes.push(format!(
                "`{}` declares only block/call handlers. A block handler's nearest equivalent is \
                 `[extract] blocks = true`; a call handler is `[extract] top_level_calls = true`. \
                 This contract will index **every** event its ABI defines; narrow it with \
                 `events = [...]` in nuthatch.toml if that is not what you want",
                ds.name
            ));
        }
        if let Some(end) = ds.end_block {
            notes.push(format!(
                "`{}` stops at block {end} in the subgraph; a nest has no end block, so it will \
                 keep indexing past it",
                ds.name
            ));
        }

        println!(
            "  ✓ {alias:<28} {address}  start={}  {} event(s)",
            ds.start_block
                .map(|b| b.to_string())
                .unwrap_or_else(|| "-".into()),
            events.len()
        );
        contracts.push(Contract {
            alias,
            address,
            start_block: ds.start_block,
            abi: String::new(), // filled below, once the alias is final
            events,
        });
        // `abi` path mirrors the alias; set it now that the alias is settled.
        let last = contracts.last_mut().expect("just pushed");
        last.abi = format!("abis/{}.json", last.alias);
    }

    // A subgraph made only of block handlers gets block rows, the one thing such a handler can be
    // reading from the manifest's point of view. Beside event sources it is dropped instead: block
    // rows cost a header per block, which the rest of the nest did not ask for.
    let blocks = contracts.is_empty() && !block_only.is_empty();
    for ds in &block_only {
        let start = ds
            .start_block
            .map(|b| format!(" from block {b}"))
            .unwrap_or_default();
        notes.push(if blocks {
            format!(
                "`{}` has only block handlers, so this nest indexes block rows (`[extract] blocks \
                 = true`) rather than its ABI's events. The subgraph ran{start}; with no contract \
                 there is no start block, so give `dev --backfill <blocks>` to reach back",
                ds.name
            )
        } else {
            format!(
                "skipped `{}` - it has only block handlers, and what they read lives in the \
                 mapping. `[extract] blocks = true` adds a row per block{start} if the header \
                 fields are what it needs",
                ds.name
            )
        });
    }

    if contracts.is_empty() && !blocks {
        bail!(
            "no indexable dataSources in this manifest - {} source(s) were all skipped:\n  {}",
            manifest.data_sources.len(),
            notes.join("\n  ")
        );
    }

    // ── templates → [[templates]] ────────────────────────────────────────
    let mut templates: Vec<crate::config::Template> = Vec::new();
    // `file/ipfs` templates, reported separately because their remedy is `[[ipfs]]`, not `[[factories]]`.
    let mut ipfs_templates: Vec<String> = Vec::new();
    // Manifest name → the alias it actually settled on, so factory rules can be emitted
    // against the same name the `[[templates]]` entry carries.
    let mut template_alias: BTreeMap<String, String> = BTreeMap::new();
    for t in &manifest.templates {
        if !t.is_evm() {
            // A `file/ipfs` template is not an unportable thing any more (RFC-0037): it is a
            // resolution nuthatch can express, once somebody says which column carries the CID.
            // Which column that is lives in the mapping WASM - same reason a factory's rule does -
            // so the manifest cannot tell us, but "we cannot do this" is now the wrong answer.
            if t.kind.starts_with("file/ipfs") {
                ipfs_templates.push(t.name.clone());
            } else {
                notes.push(format!("skipped template `{}` (kind `{}`)", t.name, t.kind));
            }
            continue;
        }
        let Some(abi_ref) = t.own_abi().cloned() else {
            notes.push(format!("skipped template `{}` - no ABI pinned", t.name));
            continue;
        };
        let alias = sg::dedupe_alias(&sg::to_alias(&t.name), &mut taken);
        let abi_json = fetch_and_vendor_abi(
            &dir,
            &alias,
            &abi_ref,
            &gateways,
            &home,
            &mut notes,
            &mut fetched,
        )
        .await?;
        // Both the entry and its ABI path must use the *settled* alias. Recomputing
        // `to_alias(&t.name)` here would name the file the template was never written to:
        // a dataSource `Vault` plus a template `Vault` — the canonical factory shape — makes
        // the template point at the dataSource's ABI. Nothing errors, because that file
        // exists; every discovered child is just decoded against the wrong contract.
        template_alias.insert(t.name.clone(), alias.clone());
        // Carry the manifest's own allowlist, exactly as the `[[contracts]]` loop does. A subgraph
        // declares `eventHandlers` per template, so the manifest already says which of the ABI's
        // events the author cared about - dropping it here made an imported nest decode a superset
        // of what the subgraph decoded, which is the one thing this import must not do.
        let events: Vec<String> = t
            .events
            .iter()
            .map(|sig| sg::event_name(sig).to_string())
            .collect();
        // The gap is worth saying out loud at init: it is the difference between the workload the
        // subgraph ran and the one this nest will run, and it is invisible until a row count
        // surprises someone.
        if !events.is_empty() {
            let defined = abi_json
                .as_array()
                .map(|a| a.iter().filter(|e| e["type"] == "event").count())
                .unwrap_or(0);
            if defined > events.len() {
                notes.push(format!(
                    "template `{alias}` handles {} of the {defined} events its ABI defines - \
                     the rest are not indexed",
                    events.len()
                ));
            }
        }
        templates.push(crate::config::Template {
            name: alias.clone(),
            abi: format!("abis/{alias}.json"),
            filter: None,
            events,
        });
    }

    // ── factory inference ────────────────────────────────────────────────
    // Inference runs on the manifest's own names, so the rules it returns have to be mapped
    // back through the same settled aliases before they are written out.
    let template_names: Vec<String> = template_alias.keys().cloned().collect();
    let (inferred, unresolved) = sg::infer_factories(&template_names, &address_params);

    let factories: Vec<crate::config::Factory> = inferred
        .iter()
        .map(|f| crate::config::Factory {
            watch: f.watch.clone(),
            event: f.event.clone(),
            child_param: f.child_param.clone(),
            // The settled alias, not a fresh `to_alias` of the manifest name — see the
            // template loop above. A recomputed alias names a template that may not exist.
            template: template_alias
                .get(&f.template)
                .cloned()
                .unwrap_or_else(|| sg::to_alias(&f.template)),
            start: None,
        })
        .collect();

    let rpc_urls = crate::rpc::select_rpcs(&args.rpc, chain.rpc_urls.iter().map(|s| s.to_string()));
    let config = Config {
        state_rpc_urls: Vec::new(),
        read_only_role: false,
        freshness: Default::default(),
        ipfs_window_deadline: crate::ipfs_resolve::WINDOW_DEADLINE,
        route: None,
        ipfs_gateways: Vec::new(),
        ipfs: Vec::new(),
        nest: Nest {
            name: nest_name(&dir),
            chain: chain.name.clone(),
            chain_id: chain.chain_id,
            rpc_urls,
            schema_version: crate::config::required_schema_version(!args.no_timestamps),
            block_timestamps: !args.no_timestamps,
        },
        contracts,
        flags: crate::config::Flags::default(),
        alerts: Vec::new(),
        templates,
        factories,
        webhooks: Vec::new(),
        extract: Extract {
            blocks,
            ..Extract::default()
        },
        calls: Vec::new(),
        address_history: None,
    };
    config.save(&dir)?;
    let table_count = write_nest_artifacts(&dir, &chain.name, &config)?;

    // ── the honest report ────────────────────────────────────────────────
    println!(
        "\n✓ scaffolded nest '{}' from subgraph {source}",
        config.nest.name
    );
    println!(
        "    {} contract(s), {} template(s), {} table(s) on {}",
        config.contracts.len(),
        config.templates.len(),
        table_count,
        chain.name
    );
    for f in &inferred {
        println!(
            "  ✓ factory: {}.{} → {} via `{}` ({})",
            f.watch, f.event, f.template, f.child_param, f.because
        );
    }
    for u in &unresolved {
        if u.candidates.is_empty() {
            println!(
                "  ⚠ template `{}` has no creating event in this manifest - it will index \
                 nothing until you add a [[factories]] rule",
                u.template
            );
        } else {
            println!(
                "  ⚠ template `{}` needs a creating event; candidates:\n      {}",
                u.template,
                u.candidates.join("\n      ")
            );
        }
    }
    for n in &notes {
        println!("  ⚠ {n}");
    }
    if !unresolved.is_empty() || !notes.is_empty() {
        println!(
            "\n  The warnings above are work the manifest cannot do for us: which template a \
             factory creates lives in the mapping WASM, not in the manifest. Resolve them by \
             adding [[factories]] rules to nuthatch.toml."
        );
    }
    if !ipfs_templates.is_empty() {
        // Deliberately *not* filed with the warnings above, and deliberately not called "skipped":
        // the remedy is a different config block, and telling someone to write `[[factories]]` for a
        // file template sends them to a rule that cannot express it.
        println!(
            "\n  {} `file/ipfs` template(s) - {} - index the *content* behind a CID. nuthatch \
             resolves those (RFC-0037), but which column carries the CID lives in the mapping WASM \
             rather than the manifest, so it needs one line each:\n\n    \
             [[ipfs]]\n    name = \"token_metadata\"     # the table to put documents in\n    \
             on = \"<table>\"                # the table whose rows carry the CID\n    \
             cid_column = \"<column>\"       # which column that is\n\n  \
             Then run with --ipfs <gateway-or-your-own-node>. Every document is verified against its \
             CID before it is stored, and one that will not resolve leaves no row rather than a wrong \
             one.",
            ipfs_templates.len(),
            ipfs_templates.join(", ")
        );
    }
    println!("\n  Next: nuthatch dev{}", dir_hint(&args.dir));
    Ok(())
}

/// Fetch one ABI by CID and vendor it into `abis/<alias>.json`.
async fn fetch_and_vendor_abi(
    dir: &Path,
    alias: &str,
    abi_ref: &crate::subgraph_import::AbiRef,
    gateways: &[String],
    home: &crate::subgraph_import::ManifestHome,
    notes: &mut Vec<String>,
    fetched: &mut std::collections::BTreeMap<String, serde_json::Value>,
) -> Result<serde_json::Value> {
    use crate::subgraph_import::{AbiLink, Origin};

    // One link, one read. Sharing an ABI across dataSources is the normal shape - every proxy in a
    // beacon codebase pins the same implementation ABI - and re-fetching it per source multiplies
    // gateway load by a factor the manifest chooses. Each source still gets its own file, because
    // the alias is what the config points at. A local path caches on the same key for the same
    // reason: one read, one parse, however many sources name it.
    // The rendered string is not an identity: a repository can quite legitimately contain a
    // `file: Qm...` beside a deployed reference to `/ipfs/Qm...`. Those are different sources,
    // even though the human-facing spelling is the same. Collapsing them here vendors whichever
    // happened to be visited first for both declarations.
    let key = match &abi_ref.link {
        AbiLink::Cid(cid) => format!("cid:{cid}"),
        AbiLink::Path(path) => format!("path:{path}"),
    };
    if let Some(cached) = fetched.get(&key) {
        write_abi(dir, alias, cached)?;
        return Ok(cached.clone());
    }
    let raw = match &abi_ref.link {
        // From the manifest, so CID only - see `subgraph_import::Origin`.
        AbiLink::Cid(cid) => crate::subgraph_import::fetch_ipfs(cid, gateways, Origin::Manifest)
            .await
            .with_context(|| format!("fetching ABI `{}` ({cid})", abi_ref.name))?,
        // A repository manifest (#1321). `resolve_abi_path` refuses a remote manifest outright and
        // refuses a local one that reaches outside its own directory.
        AbiLink::Path(rel) => {
            let path = crate::subgraph_import::resolve_abi_path(home, rel)
                .with_context(|| format!("resolving ABI `{}`", abi_ref.name))?;
            std::fs::read_to_string(&path).with_context(|| {
                format!("reading ABI `{}` from {}", abi_ref.name, path.display())
            })?
        }
    };
    let parsed: serde_json::Value = serde_json::from_str(&raw)
        .with_context(|| format!("ABI `{}` ({key}) is not JSON", abi_ref.name))?;
    if !parsed.is_array() {
        notes.push(format!(
            "ABI `{}` is not a JSON array - vendored as-is, but the registry may reject it",
            abi_ref.name
        ));
    }
    write_abi(dir, alias, &parsed)?;
    fetched.insert(key, parsed.clone());
    Ok(parsed)
}

/// Vendor one ABI under its settled alias.
fn write_abi(dir: &Path, alias: &str, abi: &serde_json::Value) -> Result<()> {
    std::fs::write(
        dir.join(format!("abis/{alias}.json")),
        serde_json::to_string_pretty(abi).context("failed to serialise ABI")?,
    )
    .with_context(|| format!("failed to write abis/{alias}.json"))
}

/// The decode identity a nest's rows are produced under: the event registry folded with every other
/// declared source of rows, in a fixed order.
///
/// `schema.json`, the store's identity guard and query provenance all read this one function. The
/// guard and provenance used to hash the event registry alone, so an event-less nest (a calldata-only
/// DataEdge) claimed `e3b0c442…`, the hash of nothing, and changing an `[[ipfs]]` or `[[calls]]`
/// declaration went unnoticed by a store that had already indexed under the old one.
pub fn decode_identity(
    dir: &Path,
    config: &Config,
    registry: &crate::registry::DecodeRegistry,
) -> Result<[u8; 32]> {
    use sha2::{Digest, Sha256};
    let fold = |hash: [u8; 32], part: [u8; 32]| -> [u8; 32] {
        let mut h = Sha256::new();
        h.update(hash);
        h.update(part);
        h.finalize().into()
    };
    let mut hash = registry.hash();
    // RFC-0014: two nests differing only in what they extract must not claim one decode version.
    if config.extract.decodes_calls() {
        hash = fold(
            hash,
            crate::calldata::CallRegistry::from_nest(dir, config)?.hash(),
        );
    }
    if !config.ipfs.is_empty() {
        hash = fold(hash, crate::ipfs::decl_hash(&config.ipfs));
    }
    // RFC-0023 tier 3: a declared read is a table too, and moves the identity for the same reason.
    if !config.calls.is_empty() {
        hash = fold(hash, crate::calls::decl_hash(&config.calls));
    }
    Ok(hash)
}

fn write_nest_artifacts(dir: &Path, chain_name: &str, config: &Config) -> Result<usize> {
    let registry = crate::registry::from_nest(dir, config)?;
    let mut schema = registry.schema();
    if config.extract.decodes_calls() {
        let calls = crate::calldata::CallRegistry::from_nest(dir, config)?;
        schema.extend(calls.schema(&config.extract));
    }
    if !config.ipfs.is_empty() {
        schema.extend(crate::ipfs::schema(&config.ipfs, registry.timestamps()));
    }
    if !config.calls.is_empty() {
        schema.extend(crate::calls::schema(&config.calls, registry.timestamps()));
    }
    crate::indexer::refuse_duplicate_tables(&schema)?;
    let hash = decode_identity(dir, config, &registry)?;
    std::fs::write(
        dir.join("schema.json"),
        nuthatch_scaffold::schema_json(hash, &schema)?,
    )
    .context("failed to write schema.json")?;
    scaffold_ai_surface(dir, chain_name, &config.contracts, &schema)?;

    // The logic layer (RFC-0018 §1): scaffold `views/` with a commented, ready-to-uncomment starter so
    // the authored-derivations layer is *discoverable* the moment you `init` - the happy path is
    // unchanged (a directory of comments; the commented starter is a no-op that validates clean).
    scaffold_views(dir, &schema)?;

    // The governed semantic layer (RFC-0016): generate `semantic.toml` from the registry - ABI-seeded
    // descriptions + derived footguns. On `add`, merge onto the existing file so authored descriptions
    // survive while the footguns are refreshed (init has no existing file, so it just writes fresh).
    let generated = crate::semantic::generate(&schema, &config.nest.name, chain_name);
    let sem = match crate::semantic::load(dir)? {
        Some(existing) => crate::semantic::merge(existing, generated),
        None => generated,
    };
    crate::semantic::save(dir, &sem)?;

    Ok(schema.len())
}

/// Scaffold the `views/` logic layer (RFC-0018 §1b) with a commented, ready-to-uncomment starter view
/// derived from the nest's own first table, plus a README. Idempotent: if `views/` already exists (an
/// `add` on a nest whose author already wrote views), it's left untouched. The starter is entirely
/// comments, so it's a no-op for the query surface and validates clean until the author uncomments it.
fn scaffold_views(dir: &Path, schema: &[crate::registry::TableSchema]) -> Result<()> {
    let attrs = dir.join(".gitattributes");
    if !attrs.exists() {
        std::fs::write(&attrs, nuthatch_scaffold::GITATTRIBUTES)
            .context("failed to write .gitattributes")?;
    }

    let views = dir.join("views");
    if views.exists() {
        return Ok(()); // author already has a views/ - never clobber it
    }
    std::fs::create_dir_all(&views)
        .with_context(|| format!("cannot create {}", views.display()))?;
    std::fs::write(views.join("README.md"), nuthatch_scaffold::VIEWS_README)
        .context("failed to write views/README.md")?;
    std::fs::write(
        views.join("10-example.sql"),
        nuthatch_scaffold::starter_view(schema),
    )
    .context("failed to write views/10-example.sql")?;
    Ok(())
}

/// Initialise a nest from a published one - a git URL or a local directory - instead of resolving
/// from addresses. The nest is self-contained (ABIs vendored, `nuthatch.toml` committed), so this
/// clones/copies it and validates it: the toml parses at a supported schema version and the decode
/// registry builds from the vendored ABIs. Publishing a nest is `git push`; consuming it is this.
fn init_from(source: &str, dir_arg: &str) -> Result<()> {
    // Default the target dir to the nest's own name (repo/dir basename) unless one was given.
    let target = if dir_arg == "." {
        PathBuf::from(source_basename(source))
    } else {
        PathBuf::from(dir_arg)
    };
    if target.exists() && target.read_dir().map(|mut d| d.next().is_some())? {
        bail!(
            "target '{}' already exists and is not empty",
            target.display()
        );
    }

    if is_git_source(source) {
        println!("→ cloning nest from {source} …");
        clone_repo(source, &target)?;
        // Drop the clone's history: a consumed nest is a plain working copy, not a live checkout.
        let _ = std::fs::remove_dir_all(target.join(".git"));
    } else {
        let src = PathBuf::from(source);
        if !src.is_dir() {
            bail!("--from '{source}' is neither a git URL nor an existing local directory");
        }
        println!("→ copying nest from {} …", src.display());
        copy_dir(&src, &target)?;
    }

    // Validate: it must be a real nest - toml at a supported schema version, ABIs present + decodable.
    let config = Config::load(&target)
        .with_context(|| format!("'{}' is not a valid nuthatch nest", target.display()))?;
    let registry = crate::registry::from_nest(&target, &config)
        .context("nest ABIs failed to build a decode registry (is the nest self-contained?)")?;
    // Validate factory rules (RFC-0009): references must resolve and depth stays within the ceiling.
    let factories = crate::factory::FactorySet::build(&config)
        .context("nest declares invalid factory/template rules")?;

    println!(
        "✓ nest '{}' ready - {} on {}, {} contract(s), {} table(s), {} anonymous event(s) skipped",
        config.nest.name,
        source_basename(source),
        config.nest.chain,
        config.contracts.len(),
        registry.tables().len(),
        registry.skipped_anonymous(),
    );
    if !factories.is_empty() {
        println!(
            "  factories: {} template(s), {} rule(s) - children discovered at runtime (RFC-0009)",
            config.templates.len(),
            config.factories.len(),
        );
    }
    println!("next:  nuthatch dev --dir {}", target.display());
    Ok(())
}

/// Whether `--from` names a git remote (vs. a local directory).
/// Transports git will accept from us. Anything else is not a git source, whatever it is named.
///
/// **Audit finding 6.** The `.git` suffix used to be sufficient on its own, so `ext::sh -c … .git`
/// reached `git clone` - and git's `ext::` transport *executes the command*. It is refused today only
/// by git's own `protocol.ext.allow=never` default (git >= 2.12). An operator with
/// `protocol.ext.allow=always` in their gitconfig - not unheard of in CI images - would have turned
/// `nuthatch init --from <url>` into remote code execution.
///
/// Being safe by someone else's default is not the same as being safe. The scheme is now checked here,
/// so the guarantee is ours: a `.git` suffix qualifies a source only when its transport is one we
/// named. `ext::`, `file::`, `transport-helper::` and anything else are simply not git sources.
const GIT_SCHEMES: &[&str] = &["http://", "https://", "ssh://", "git://"];

fn is_git_source(source: &str) -> bool {
    if source.starts_with("git@") {
        return true; // scp-like syntax: user@host:path, no scheme to check
    }
    if GIT_SCHEMES.iter().any(|p| source.starts_with(p)) {
        return true;
    }
    // A `.git` suffix alone is not enough: it must still look like a plain path or scp-like target,
    // never a transport helper. `::` is the marker git uses for those (`ext::`, `transport::`).
    source.ends_with(".git") && !source.contains("::")
}

/// The nest's own name: the last path component, minus a trailing `.git` or slash.
fn source_basename(source: &str) -> String {
    source
        .trim_end_matches('/')
        .trim_end_matches(".git")
        .rsplit(['/', ':'])
        .next()
        .filter(|s| !s.is_empty())
        .unwrap_or("nest")
        .to_string()
}

/// Shallow-clone a nest repo into `target` using the system `git` (no in-process git dependency).
fn clone_repo(url: &str, target: &Path) -> Result<()> {
    let status = std::process::Command::new("git")
        .args(["clone", "--depth", "1", url])
        .arg(target)
        .status()
        .context("failed to run `git` - is it installed and on PATH?")?;
    if !status.success() {
        bail!("git clone of '{url}' failed");
    }
    Ok(())
}

/// Recursively copy a local nest directory (skipping any `.git`).
pub(crate) fn copy_dir(src: &Path, dst: &Path) -> Result<()> {
    std::fs::create_dir_all(dst).with_context(|| format!("cannot create {}", dst.display()))?;
    for entry in std::fs::read_dir(src).with_context(|| format!("cannot read {}", src.display()))? {
        let entry = entry?;
        let name = entry.file_name();
        if name == ".git" {
            continue;
        }
        let from = entry.path();
        let to = dst.join(&name);
        if entry.file_type()?.is_dir() {
            copy_dir(&from, &to)?;
        } else {
            std::fs::copy(&from, &to)
                .with_context(|| format!("failed to copy {}", from.display()))?;
        }
    }
    Ok(())
}

/// Resolve the ABI to index events with. For a proxy (e.g. USDC), events emit from the proxy address
/// but use the *implementation's* event definitions, so resolve the implementation's ABI. Falls back
/// to the address's own ABI if it isn't a proxy or the implementation can't resolve. Init-time only -
/// the resolved ABI is vendored and frozen, so the deterministic decode path never depends on a live
/// proxy read.
struct ResolvedAbi {
    abi: serde_json::Value,
    /// `Some` only when the vendored ABI came from an implementation rather than the address itself.
    implementation: Option<String>,
    /// Sourcify (or a wrapped ABI) sometimes names the contract. Used as the default alias (#774).
    contract_name: Option<String>,
}

async fn resolve_abi(
    rpc: &RpcClient,
    chain_id: u64,
    address: &str,
    explorer: Option<&str>,
) -> Result<ResolvedAbi> {
    if let Some(implementation) = resolve_implementation(rpc, address).await {
        println!("  · proxy → implementation {implementation}");
        if let Ok(resolved) = abi::resolve(chain_id, &implementation, explorer).await {
            print_abi_resolved(&resolved);
            return Ok(ResolvedAbi {
                abi: resolved.abi,
                implementation: Some(implementation),
                contract_name: resolved.contract_name,
            });
        }
        println!("  · implementation ABI unresolved; using the proxy's own ABI");
    }
    let resolved = abi::resolve(chain_id, address, explorer).await?;
    print_abi_resolved(&resolved);
    Ok(ResolvedAbi {
        abi: resolved.abi,
        implementation: None,
        contract_name: resolved.contract_name,
    })
}

/// The pretty lines for a resolved ABI, in the same `→`/`✓`/`·` two-space-indented prose the rest of
/// `init`/`add` use - a pure function (no I/O) so the exact wording is unit-tested without a live
/// network call (see `abi_resolved_lines` tests below). Previously this was a raw `tracing::info!`
/// that printed its own ISO timestamp and log level through the middle of the pretty output (#675);
/// the resolver name is real information worth keeping, so it moves here rather than disappearing.
fn abi_resolved_lines(resolved: &abi::Resolved) -> Vec<String> {
    let mut lines = Vec::new();
    if let Some(reason) = &resolved.fallback_reason {
        lines.push(format!("  · {reason}"));
    }
    lines.push(format!("  ✓ ABI resolved via {}", resolved.via));
    lines
}

fn print_abi_resolved(resolved: &abi::Resolved) {
    for line in abi_resolved_lines(resolved) {
        println!("{line}");
    }
}

/// Follow the well-known proxy patterns to an implementation address, or `None` if `address` is not a
/// recognised proxy. Direct-slot proxies (EIP-1967 / EIP-1822 / legacy zeppelinos) hold the impl
/// address in a storage slot; an EIP-897 proxy exposes `implementation()` directly; a beacon proxy
/// holds a beacon whose `implementation()` we then call.
async fn resolve_implementation(rpc: &RpcClient, address: &str) -> Option<String> {
    for slot in PROXY_IMPL_SLOTS {
        if let Ok(word) = rpc.get_storage_at(address, slot).await {
            if let Some(implementation) = impl_from_slot(&word) {
                return Some(implementation);
            }
        }
    }
    // EIP-897 / Aragon AppProxyUpgradeable: unlike a beacon, the proxy itself exposes the logic
    // address through `implementation()`. stETH is this shape. We used to call this selector only
    // on a beacon found in EIP-1967 storage, so a proxy that had the standard public method but no
    // standard storage slot was needlessly treated as a bespoke, unresolvable proxy.
    if let Ok(ret) = rpc.eth_call(address, IMPLEMENTATION_SELECTOR).await {
        if let Some(implementation) = impl_from_slot(&ret) {
            return Some(implementation);
        }
    }
    // Beacon proxy: the implementation is one hop further - the proxy points at a beacon, and the
    // beacon answers `implementation()`. Both the stored word and the call return are 32-byte,
    // left-padded addresses, so `impl_from_slot` decodes either.
    if let Ok(word) = rpc.get_storage_at(address, PROXY_BEACON_SLOT).await {
        if let Some(beacon) = impl_from_slot(&word) {
            if let Ok(ret) = rpc.eth_call(&beacon, IMPLEMENTATION_SELECTOR).await {
                if let Some(implementation) = impl_from_slot(&ret) {
                    return Some(implementation);
                }
            }
        }
    }
    None
}

/// What a sample of the contract's real logs says about the ABI we resolved for it.
///
/// The failure this exists to catch is the quietest one nuthatch has: a proxy whose implementation
/// ABI the public resolvers don't return. Sourcify/Etherscan answer with the *proxy's* ABI, which is
/// usually two or three administrative events, so `init` succeeds, the schema looks plausible, and
/// `dev` then indexes **nothing** - no error, no warning, just empty tables. It cost us most of a day
/// on the Livepeer nest, whose `ManagerProxy` matches no standard proxy slot, and the only reason we
/// worked it out was noticing the event count looked too small.
///
/// Detecting it directly is cheap: fetch a handful of the address's actual logs and see whether the
/// ABI decodes any of them.
#[derive(Debug, PartialEq)]
enum AbiFit {
    /// At least one sampled log matches an event in the ABI. Says nothing about the other events,
    /// which is fine - it rules out the total-mismatch case, and that is the one that is silent.
    Fits,
    /// Logs exist and **none** of them match. This is the proxy signature.
    Mismatch { sampled: usize },
    /// Recent tip logs match, but historical logs near deployment do not match the current ABI.
    /// This happens when a proxy was upgraded across an event rename (#773).
    HistoricalMismatch { sampled: usize },
    /// The sample was empty, so there is nothing to conclude. A dormant contract and a wrong ABI look
    /// identical from here, and claiming otherwise would train people to ignore the warning.
    NoSample,
    /// The probe could not be run (RPC down, range refused, rate limited). Never blocks `init` -
    /// resolution is best-effort by design, like deploy-block detection above it.
    Unknown,
}

/// Sample recent logs from `address` and report whether `abi` decodes any of them.
///
/// Two probes at most. The tip window catches any active contract - which is what the people who hit
/// this are indexing - and the deployment window catches a contract that was busy once and has since
/// gone quiet. A dormant contract yields [`AbiFit::NoSample`] and no claim is made.
async fn check_abi_fits(
    rpc: &RpcClient,
    address: &str,
    abi: &serde_json::Value,
    tip: Option<u64>,
    start_block: Option<u64>,
    log_window: u64,
) -> AbiFit {
    let Some(tip) = tip else {
        return AbiFit::Unknown;
    };

    let topic0s = abi_event_topic0s(abi);
    if topic0s.is_empty() {
        // An ABI with no events at all decodes nothing by construction - the strongest version of the
        // signal, and worth reporting without spending an RPC call on it.
        return AbiFit::Mismatch { sampled: 0 };
    }

    let windows = probe_windows(tip, start_block, log_window);

    let mut any_error = false;
    let mut tip_fit: Option<AbiFit> = None;
    let mut hist_fit: Option<AbiFit> = None;

    for (i, (from, to)) in windows.into_iter().enumerate() {
        match rpc.get_logs(&[address.to_string()], &[], from, to).await {
            Ok(logs) if !logs.is_empty() => {
                let sample: Vec<Option<&str>> = logs
                    .iter()
                    .map(|l| l.topics.first().map(|s| s.as_str()))
                    .collect();
                let fit = fit_from_sample(&topic0s, &sample);
                if i == 0 {
                    tip_fit = Some(fit);
                } else {
                    hist_fit = Some(fit);
                }
            }
            Ok(_) => {}
            Err(_) => any_error = true,
        }
    }

    combine_abi_fits(tip_fit, hist_fit, any_error)
}

/// Verdict from the two probe windows. Split from fetching so #773 can be tested without a chain:
/// tip matches the current ABI, history near deployment matches none of it.
fn combine_abi_fits(tip_fit: Option<AbiFit>, hist_fit: Option<AbiFit>, any_error: bool) -> AbiFit {
    match (tip_fit, hist_fit) {
        (Some(AbiFit::Mismatch { sampled }), _) => AbiFit::Mismatch { sampled },
        (Some(AbiFit::Fits), Some(AbiFit::Mismatch { sampled })) => {
            AbiFit::HistoricalMismatch { sampled }
        }
        (None, Some(AbiFit::Mismatch { sampled })) => AbiFit::Mismatch { sampled },
        (Some(AbiFit::Fits), _) | (_, Some(AbiFit::Fits)) => AbiFit::Fits,
        (None, None) if any_error => AbiFit::Unknown,
        (None, None) => AbiFit::NoSample,
        _ => AbiFit::Unknown,
    }
}

/// Block ranges to probe, tip window first. `probe` is the chain's own measured, RPC-safe
/// `eth_getLogs` span (`Chain::log_window`) - not a fixed guess. A wider guess trips a busy mainnet
/// contract straight through every default provider's result-size cap (#512: a hardcoded 1,000-block
/// probe of USDC errored on all three mainnet defaults - nodies, drpc, and onfinality), and the
/// fallback below then reports a confident verdict from an ancient, unrepresentative sample instead
/// of the `Unknown` an error is supposed to produce.
fn probe_windows(tip: u64, start_block: Option<u64>, probe: u64) -> Vec<(u64, u64)> {
    let mut windows = vec![(tip.saturating_sub(probe), tip)];
    if let Some(start) = start_block {
        if start.saturating_add(probe) < tip.saturating_sub(probe) {
            windows.push((start, start + probe));
        }
    }
    windows
}

/// The verdict itself, separated from fetching so it can be tested without a chain. `sample` is each
/// log's topic0 (`None` for an anonymous event, which has no topic0 and can never match).
fn fit_from_sample(topic0s: &[String], sample: &[Option<&str>]) -> AbiFit {
    if sample.is_empty() {
        return AbiFit::NoSample;
    }
    let hit = sample
        .iter()
        .flatten()
        // Case-insensitively: `eth_getLogs` responses are lowercase hex by convention but that is a
        // convention, not a guarantee, and a case mismatch here would fire the warning on a nest that
        // is perfectly fine.
        .any(|t| topic0s.iter().any(|k| k.eq_ignore_ascii_case(t)));
    if hit {
        AbiFit::Fits
    } else {
        AbiFit::Mismatch {
            sampled: sample.len(),
        }
    }
}

/// Print the verdict from [`check_abi_fits`].
fn report_abi_fit(fit: AbiFit, alias: &str, address: &str) {
    match fit {
        AbiFit::Mismatch { sampled } => {
            let seen = if sampled == 0 {
                "the resolved ABI declares no events at all".to_string()
            } else {
                format!("none of its last {sampled} log(s) match any event in the resolved ABI")
            };
            eprintln!();
            eprintln!("  ⚠ {alias} ({address}): {seen}.");
            eprintln!("    As configured this contract will index **zero rows**, silently.");
            eprintln!("    The usual cause is a proxy: the public ABI resolvers return the *proxy's* ABI,");
            eprintln!("    while the events are defined by the implementation behind it. nuthatch follows");
            eprintln!(
                "    the standard proxy slots automatically, so this one uses a bespoke pattern."
            );
            eprintln!("    Fix: get the implementation's ABI and re-run with");
            eprintln!("      nuthatch init {address} --abi path/to/implementation.json");
            eprintln!(
                "    or overwrite abis/{alias}.json and run `nuthatch schema` to regenerate."
            );
            eprintln!();
        }
        AbiFit::HistoricalMismatch { sampled } => {
            eprintln!();
            eprintln!(
                "  ⚠ {alias} ({address}): pre-upgrade history (sampled near deployment) emitted"
            );
            eprintln!("    {sampled} log(s) matching none of the current implementation's events.");
            eprintln!("    This contract appears to have been upgraded across an event rename or ABI change.");
            eprintln!("    Pre-upgrade history will index **zero rows** under the current ABI.");
            eprintln!("    Fix: add an older [[contracts]] entry for {alias} in nuthatch.toml with the legacy ABI,");
            eprintln!("    bounded to the era that emitted those events.");
            eprintln!();
        }
        _ => {}
    }
}

/// Per-address `--abi` overrides, positionally aligned with the addresses like `--alias`. An empty
/// entry means "resolve this one normally", so overriding the second of three contracts does not
/// force you to find local ABIs for the other two.
fn resolve_abi_overrides(provided: &[String], n: usize) -> Result<Vec<Option<String>>> {
    if provided.is_empty() {
        return Ok(vec![None; n]);
    }
    if provided.len() != n {
        bail!(
            "{} --abi path(s) for {n} address(es) - provide one per address (an empty entry resolves \
             that address normally) or none at all",
            provided.len()
        );
    }
    Ok(provided
        .iter()
        .map(|p| {
            let t = p.trim();
            if t.is_empty() {
                None
            } else {
                Some(t.to_string())
            }
        })
        .collect())
}

/// `--start-block` in address order. Empty entry means probe. Same shape as `--abi`.
fn resolve_start_blocks(provided: &[String], n: usize) -> Result<Vec<Option<u64>>> {
    if provided.is_empty() {
        return Ok(vec![None; n]);
    }
    if provided.len() != n {
        bail!(
            "{} --start-block value(s) for {n} address(es) - provide one per address (an empty \
             entry still probes) or none at all",
            provided.len()
        );
    }
    provided
        .iter()
        .map(|p| {
            let t = p.trim();
            if t.is_empty() {
                Ok(None)
            } else {
                t.parse::<u64>()
                    .map(Some)
                    .with_context(|| format!("--start-block '{p}' is not a block number"))
            }
        })
        .collect()
}

async fn resolve_contract_start(
    rpc: &RpcClient,
    address: &str,
    tip: Option<u64>,
    provided: Option<u64>,
    undetected: &mut Vec<String>,
) -> Option<u64> {
    if let Some(b) = provided {
        println!("  ✓ start block {b} (--start-block)");
        return Some(b);
    }
    let tip = tip?;
    match detect_deploy_block(rpc, address, tip).await {
        Ok(b) => {
            println!("  ✓ deployed at block {b}");
            Some(b)
        }
        Err(e) => {
            println!("  · deployment block undetected ({e:#})");
            undetected.push(address.to_string());
            None
        }
    }
}

fn print_undetected_start_blocks(undetected: &[String]) {
    if undetected.is_empty() {
        return;
    }
    println!();
    println!(
        "⚠ deployment block undetected for {} contract(s):",
        undetected.len()
    );
    for a in undetected {
        println!("    {a}");
    }
    println!("  this nest will backfill a recent window, not those contracts' history.");
    println!(
        "  re-run with --start-block <n,...> in the same order as the addresses, then `nuthatch dev`."
    );
}

/// Read and validate a local ABI file. Parsed as a `JsonAbi` before it is accepted so a wrong file
/// (a subgraph manifest, a contract artifact wrapping the ABI under `"abi"`) fails here with a clear
/// message rather than at decode time as an empty registry.
/// ABI JSON plus an optional `contractName` from a Hardhat/Foundry wrapper. The name is taken
/// from the wrapper *before* the ABI array is unwrapped - after that, it is gone (#774).
fn read_local_abi(path: &str) -> Result<(serde_json::Value, Option<String>)> {
    let raw =
        std::fs::read_to_string(path).with_context(|| format!("cannot read ABI file '{path}'"))?;
    let parsed: serde_json::Value =
        serde_json::from_str(&raw).with_context(|| format!("'{path}' is not valid JSON"))?;
    let name = parsed
        .get("contractName")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());
    // Solidity build artifacts (Hardhat/Foundry) wrap the ABI in an object. Accepting that shape is
    // two lines here and saves everyone a confusing failure later.
    let abi = match parsed.get("abi") {
        Some(inner) => inner.clone(),
        None => parsed,
    };
    serde_json::from_value::<alloy_json_abi::JsonAbi>(abi.clone())
        .with_context(|| format!("'{path}' does not parse as a contract ABI"))?;
    Ok((abi, name))
}

/// `0x`-prefixed topic0 of every event in an ABI. Uses the same `alloy_json_abi` selector the decode
/// registry keys on, so a match here means a match at decode time - not an approximation of one.
fn abi_event_topic0s(abi: &serde_json::Value) -> Vec<String> {
    let Ok(parsed) = serde_json::from_value::<alloy_json_abi::JsonAbi>(abi.clone()) else {
        return Vec::new();
    };
    parsed
        .events()
        .map(|e| format!("0x{}", hex::encode(e.selector())))
        .collect()
}

/// The line `init` prints after scaffolding. A 0-table nest whose ABI was entirely anonymous
/// must not look like USDC's success (#766).
fn scaffold_summary(
    nest: &str,
    contracts: usize,
    tables: usize,
    skipped_anonymous: usize,
    dir: &str,
) -> String {
    if tables == 0 && skipped_anonymous > 0 {
        format!(
            "WARN nest '{nest}' has {tables} table(s) from {contracts} contract(s) in {dir}. \
             {skipped_anonymous} anonymous event(s) were skipped: they have no topic0, and \
             nuthatch's decode is topic0-keyed. This nest cannot index that ABI. Not a successful \
             scaffold of an empty protocol - a limit. See docs/rfcs/0001-generalized-decode-and-nests.md."
        )
    } else {
        format!("✓ scaffolded nest '{nest}' ({contracts} contract(s), {tables} table(s)) in {dir}")
    }
}

fn nest_name(dir: &Path) -> String {
    dir.canonicalize()
        .ok()
        .and_then(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
        .filter(|n| !n.is_empty() && n != ".")
        .unwrap_or_else(|| "nest".to_string())
}

/// Binary-search the deployment block: smallest block where the contract has code.
/// ~log2(tip) ≈ 25 `eth_getCode` calls. Best-effort - the caller tolerates failure.
async fn detect_deploy_block(rpc: &RpcClient, address: &str, tip: u64) -> Result<u64> {
    if is_empty_code(&rpc.get_code(address, tip).await?) {
        bail!("no code at tip");
    }
    let (mut lo, mut hi) = (0u64, tip);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if is_empty_code(&rpc.get_code(address, mid).await?) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

/// A proxy cannot have emitted events decoded by its *current* implementation before that
/// implementation was deployed. This is not a proof that its event surface changed, but it is a
/// cheap, concrete warning that the automatically vendored ABI cannot describe all declared history.
fn proxy_history_may_be_missing(proxy_start: u64, implementation_start: u64) -> bool {
    proxy_start < implementation_start
}

/// Warn about the current-implementation trap from #773. Resolving the current ABI correctly is not
/// enough when the nest starts before that implementation existed: the old event names are absent
/// from the registry, so their logs would be skipped while the cursor appears healthy.
async fn report_proxy_history_gap(
    rpc: &RpcClient,
    implementation: Option<&str>,
    proxy_start: Option<u64>,
    tip: Option<u64>,
    alias: &str,
) {
    let (Some(implementation), Some(proxy_start), Some(tip)) = (implementation, proxy_start, tip)
    else {
        return;
    };
    let Ok(implementation_start) = detect_deploy_block(rpc, implementation, tip).await else {
        return;
    };
    if proxy_history_may_be_missing(proxy_start, implementation_start) {
        eprintln!();
        eprintln!(
            "  ⚠ {alias}: the proxy starts at block {proxy_start}, but its current implementation \
             was deployed at block {implementation_start}."
        );
        eprintln!(
            "    Events before block {implementation_start} may use earlier implementation ABIs and \
             will not be decoded by this scaffold."
        );
        eprintln!(
            "    Add the earlier ABI as a second contract entry at the same address, bounded to its \
             implementation era, before trusting historical totals."
        );
        eprintln!();
    }
}

fn is_empty_code(code: &str) -> bool {
    code.trim_start_matches("0x").is_empty()
}

/// Detect which registered chain a contract lives on by probing `eth_getCode` on each chain's
/// default endpoints in parallel. We probe the *first* address (a nest's contracts are expected to
/// share a chain - one cursor, one chain, per the non-negotiables) and pick, in registry order, the
/// first chain with bytecode there. Best-effort per chain: an unreachable endpoint reads as "not
/// here", never a hard failure, so one flaky RPC can't veto detection.
async fn detect_chain(addresses: &[String]) -> Result<&'static chains::Chain> {
    let probe = normalise_address(&addresses[0])?;
    println!("→ no --chain given; probing known chains for {probe}…");

    let candidates = chains::all()
        .iter()
        .map(|chain| {
            (
                *chain,
                chain.rpc_urls.iter().map(|s| s.to_string()).collect(),
            )
        })
        .collect();
    let probed = probe_chains(&probe, candidates, CHAIN_PROBE_TIMEOUT).await;
    let found: Vec<&'static chains::Chain> = probed
        .iter()
        .filter_map(|(c, f)| (*f == Some(true)).then_some(*c))
        .collect();
    let unreachable: Vec<&str> = probed
        .iter()
        .take_while(|(c, _)| found.first().map(|f| f.name) != Some(c.name))
        .filter(|(_, f)| f.is_none())
        .map(|(c, _)| c.name)
        .collect();
    if !unreachable.is_empty() {
        println!(
            "  ⚠ no answer from {} within {}s; pass --chain if the contract lives there",
            unreachable.join(", "),
            CHAIN_PROBE_TIMEOUT.as_secs()
        );
    }

    match found.as_slice() {
        [] => bail!(
            "couldn't find bytecode for {probe} on any known chain (mainnet, arbitrum-one, base).\n\
             Pass --chain explicitly, or --rpc <url> for a custom endpoint."
        ),
        [only] => {
            println!("  ✓ found on {}", only.name);
            Ok(only)
        }
        [first, rest @ ..] => {
            let others: Vec<&str> = rest.iter().map(|c| c.name).collect();
            println!(
                "  ✓ found on {} (also deployed on {} - pass --chain to pick another)",
                first.name,
                others.join(", ")
            );
            Ok(first)
        }
    }
}

/// Per request while probing. A public endpoint that never answers (tenderly from a Docker network,
/// 2026-10-05) otherwise holds `init` for the client's 20 s before failover reaches the next one.
const CHAIN_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// Whether each candidate has bytecode at `probe`, in candidate order: `None` when no endpoint of
/// that chain answered.
async fn probe_chains(
    probe: &str,
    candidates: Vec<(&'static chains::Chain, Vec<String>)>,
    timeout: std::time::Duration,
) -> Vec<(&'static chains::Chain, Option<bool>)> {
    let probes = candidates.into_iter().map(|(chain, urls)| async move {
        let found = async {
            let rpc = RpcClient::with_timeout(urls, timeout).ok()?;
            let tip = rpc.block_number().await.ok()?;
            let code = rpc.get_code(probe, tip).await.ok()?;
            Some(!is_empty_code(&code))
        }
        .await;
        (chain, found)
    });
    futures::future::join_all(probes).await
}

/// Detect a known chain through the endpoint(s) the operator supplied, without probing public
/// defaults. `--rpc` is commonly supplied precisely to keep a run inside a paid, audited pool.
async fn detect_chain_on_rpc(
    addresses: &[String],
    rpc_urls: &[String],
) -> Result<chains::ResolvedChain> {
    let probe = normalise_address(&addresses[0])?;
    println!("→ no --chain given; checking {probe} through the supplied RPC endpoint(s)…");
    let rpc = RpcClient::new(rpc_urls.to_vec())?;
    let chain_id = rpc
        .chain_id()
        .await
        .context("could not read chain id from --rpc")?;
    let chain = chains::all()
        .iter()
        .find(|chain| chain.chain_id == chain_id)
        .copied()
        .with_context(|| {
            format!(
                "--rpc reports unregistered chain id {chain_id}; pass --chain <name> to name it"
            )
        })?;
    let tip = rpc
        .block_number()
        .await
        .context("could not read tip from --rpc")?;
    let code = rpc
        .get_code(&probe, tip)
        .await
        .context("could not read contract bytecode from --rpc")?;
    if is_empty_code(&code) {
        bail!(
            "no bytecode for {probe} on {} via the supplied RPC endpoint(s)",
            chain.name
        );
    }
    println!("  ✓ found on {}", chain.name);
    Ok(chains::ResolvedChain {
        name: chain.name.to_string(),
        chain_id: chain.chain_id,
        rpc_urls: rpc_urls.to_vec(),
        finality: chain.finality,
        log_window: chain.log_window,
    })
}

pub(crate) fn scaffold_ai_surface(
    dir: &Path,
    chain: &str,
    contracts: &[Contract],
    schema: &[crate::registry::TableSchema],
) -> Result<()> {
    let contracts: Vec<_> = contracts
        .iter()
        .map(|c| (c.alias.as_str(), c.address.as_str()))
        .collect();
    std::fs::write(
        dir.join("llms.txt"),
        nuthatch_scaffold::llms_txt(chain, &contracts, schema),
    )
    .context("failed to write llms.txt")?;
    let skill_dir = dir.join(".claude/skills/nuthatch");
    std::fs::create_dir_all(&skill_dir).context("failed to create skill dir")?;
    std::fs::write(
        skill_dir.join("SKILL.md"),
        nuthatch_scaffold::skill_md(chain, &contracts, schema),
    )
    .context("failed to write SKILL.md")?;
    Ok(())
}

/// The line under `next:  nuthatch dev` when that command would backfill a long history (#1607): the
/// same command over the last fifteen windows, which a public endpoint serves in seconds.
fn recent_history_hint(
    start: Option<u64>,
    tip: Option<u64>,
    window: u64,
    dir: &str,
) -> Option<String> {
    let span = crate::indexer::long_backfill(start?, tip?, window)?;
    let recent = window.saturating_mul(15);
    Some(format!(
        "       nuthatch dev{} --backfill {recent}   (the last {recent} blocks only; without it, dev \
         backfills {span} blocks from deployment)",
        dir_hint(dir)
    ))
}

fn dir_hint(dir: &str) -> String {
    if dir == "." {
        String::new()
    } else {
        format!(" --dir {dir}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn files_under(root: &Path) -> std::collections::BTreeMap<String, Vec<u8>> {
        crate::blob::collect_files(root, None)
            .unwrap()
            .into_iter()
            .map(|p| {
                let rel = p.strip_prefix(root).unwrap().to_string_lossy().into_owned();
                (rel, std::fs::read(&p).unwrap())
            })
            .collect()
    }

    /// RFC-0066 S0. The goldens are what `nuthatch init` wrote on 2026-10-18 before its file contents
    /// moved to `nuthatch-scaffold`: one with block timestamps, one with two contracts (a proxy, and a
    /// local ABI) without. A tab calls `files`; the node writes the config and ABIs from it and the
    /// rest through `write_nest_artifacts`. Both must give the golden, byte for byte.
    #[test]
    fn the_scaffold_writes_what_init_wrote() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/scaffold");
        for name in ["pair", "two"] {
            let golden = root.join(name);
            let want = files_under(&golden);
            let config = Config::load(&golden).unwrap();
            let nest = nuthatch_scaffold::Nest {
                name: config.nest.name.clone(),
                chain: config.nest.chain.clone(),
                chain_id: config.nest.chain_id,
                rpc_urls: config.nest.rpc_urls.clone(),
                block_timestamps: config.nest.block_timestamps,
                contracts: config
                    .contracts
                    .iter()
                    .map(|c| nuthatch_scaffold::Contract {
                        alias: c.alias.clone(),
                        address: c.address.clone(),
                        start_block: c.start_block,
                        abi: serde_json::from_slice(&want[&c.abi]).unwrap(),
                    })
                    .collect(),
            };
            let files = nuthatch_scaffold::files(&nest).unwrap();
            assert_eq!(
                files.keys().collect::<Vec<_>>(),
                want.keys().collect::<Vec<_>>(),
                "{name}"
            );
            for (path, bytes) in &want {
                assert!(
                    files[path] == *bytes,
                    "{name}: the scaffold's {path} differs"
                );
            }

            let dir = tempfile::tempdir().unwrap();
            std::fs::create_dir_all(dir.path().join("abis")).unwrap();
            for (path, bytes) in &files {
                if path == crate::config::CONFIG_FILE || path.starts_with("abis/") {
                    std::fs::write(dir.path().join(path), bytes).unwrap();
                }
            }
            let config = Config::load(dir.path()).unwrap();
            write_nest_artifacts(dir.path(), &config.nest.chain, &config).unwrap();
            let got = files_under(dir.path());
            assert_eq!(
                got.keys().collect::<Vec<_>>(),
                want.keys().collect::<Vec<_>>()
            );
            for (path, bytes) in &want {
                assert!(got[path] == *bytes, "{name}: the node's {path} differs");
            }

            // The scaffold serialises its own config; the node's `Config` must agree with it.
            let resaved = tempfile::tempdir().unwrap();
            config.save(resaved.path()).unwrap();
            assert_eq!(
                std::fs::read(resaved.path().join(crate::config::CONFIG_FILE)).unwrap(),
                want[crate::config::CONFIG_FILE],
                "{name}: Config saves a different nuthatch.toml than the scaffold writes"
            );
        }
    }

    /// #1662: the scaffolded skill's example names a table the nest has, and it does not steer an
    /// agent away from `/sql` for live data, which reads hot and sealed rows together.
    #[test]
    fn the_scaffolded_skill_names_a_real_table_and_does_not_call_sql_finalized_only() {
        let dir = tempfile::tempdir().unwrap();
        let contracts: Vec<Contract> = vec![serde_json::from_value(serde_json::json!({
            "alias": "usdc", "address": "0xa0b8", "abi": "abis/usdc.json"
        }))
        .unwrap()];
        let schema: Vec<crate::registry::TableSchema> = vec![serde_json::from_value(
            serde_json::json!({"table": "usdc__transfer", "alias": "usdc", "event": "Transfer", "columns": []}),
        )
        .unwrap()];
        scaffold_ai_surface(dir.path(), "mainnet", &contracts, &schema).unwrap();
        let skill =
            std::fs::read_to_string(dir.path().join(".claude/skills/nuthatch/SKILL.md")).unwrap();
        assert!(skill.contains("FROM usdc__transfer"), "{skill}");
        assert!(!skill.contains("finalized data only"), "{skill}");
    }

    /// Pure mirror of the deployment binary search, for algorithm confidence without RPC.
    fn find_deploy_block(tip: u64, deployed_from: u64) -> Option<u64> {
        let is_deployed = |b: u64| b >= deployed_from;
        if !is_deployed(tip) {
            return None;
        }
        let (mut lo, mut hi) = (0u64, tip);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if is_deployed(mid) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        Some(lo)
    }

    #[test]
    fn deploy_block_binary_search() {
        assert_eq!(find_deploy_block(1000, 137), Some(137));
        assert_eq!(find_deploy_block(1000, 0), Some(0));
        assert_eq!(find_deploy_block(1000, 1000), Some(1000));
        assert_eq!(find_deploy_block(100, 500), None); // not deployed by tip
    }

    #[test]
    fn slugify_and_pick_alias_prefer_the_contract_name() {
        // Deleting slugify (always None) makes DelegationManager fall through to c0.
        assert_eq!(
            slugify_alias("DelegationManager").as_deref(),
            Some("delegation_manager")
        );
        assert_eq!(slugify_alias("Vat").as_deref(), Some("vat"));
        assert_eq!(slugify_alias("L2GNS").as_deref(), Some("l2gns"));
        assert_eq!(slugify_alias("").as_deref(), None);
        assert_eq!(slugify_alias("123").as_deref(), None); // would not pass is_valid_alias

        let empty = std::collections::HashSet::new();
        let none: &[String] = &[];
        assert_eq!(
            pick_alias(None, Some("DelegationManager"), &empty, none).unwrap(),
            "delegation_manager"
        );
        assert_eq!(pick_alias(None, Some("Vat"), &empty, none).unwrap(), "vat");
        assert_eq!(
            pick_alias(None, None, &empty, none).unwrap(),
            "c0",
            "a nameless ABI still gets cN"
        );
        assert_eq!(
            pick_alias(Some("gns"), Some("DelegationManager"), &empty, none).unwrap(),
            "gns",
            "--alias still wins"
        );
        assert!(pick_alias(Some("WETH"), None, &empty, none).is_err());
        assert!(pick_alias(Some("1bad"), None, &empty, none).is_err());

        let used: std::collections::HashSet<&str> = ["delegation_manager"].into_iter().collect();
        assert_eq!(
            pick_alias(None, Some("DelegationManager"), &used, none).unwrap(),
            "delegation_manager_2",
            "a collision is uniqued, not overwritten"
        );

        let used_c: std::collections::HashSet<&str> = ["c0", "c1"].into_iter().collect();
        assert_eq!(pick_alias(None, None, &used_c, none).unwrap(), "c2");
        let mixed: std::collections::HashSet<&str> = ["usdc", "c1"].into_iter().collect();
        assert_eq!(
            pick_alias(None, None, &mixed, none).unwrap(),
            "c0",
            "cN is the next free slot, not nest.len()"
        );
        assert!(pick_alias(Some("c0"), None, &used_c, none).is_err());
    }

    #[test]
    fn scaffold_summary_does_not_tick_an_anonymous_only_abi() {
        let warn = scaffold_summary("vat", 1, 0, 1, "/tmp/vat");
        assert!(
            warn.starts_with("WARN"),
            "0 tables + skipped anonymous must not look like a successful scaffold: {warn}"
        );
        assert!(
            warn.contains("anonymous") && warn.contains("topic0"),
            "the warning must name the limit, got: {warn}"
        );
        assert!(
            !warn.contains('✓'),
            "a checkmark on 0 tables is the bug: {warn}"
        );

        let ok = scaffold_summary("usdc", 1, 2, 0, "/tmp/usdc");
        assert!(
            ok.starts_with('✓') && ok.contains("2 table(s)"),
            "a Transfer ABI still prints the ordinary success line: {ok}"
        );
        assert!(!ok.contains("WARN"), "{ok}");
    }

    #[test]
    fn rename_alias_rekeys_authored_prose_and_moves_the_abi() {
        let dir = tempfile::tempdir().unwrap();
        let abis = dir.path().join("abis");
        std::fs::create_dir_all(&abis).unwrap();
        const TRANSFER: &str = r#"[{"type":"event","name":"Transfer","inputs":[
            {"name":"from","type":"address","indexed":true},
            {"name":"to","type":"address","indexed":true},
            {"name":"value","type":"uint256","indexed":false}],"anonymous":false}]"#;
        std::fs::write(abis.join("c0.json"), TRANSFER).unwrap();
        std::fs::write(
            dir.path().join("nuthatch.toml"),
            r#"
                [nest]
                name = "t"
                chain = "mainnet"
                chain_id = 1
                rpc_urls = ["http://127.0.0.1:1"]
                schema_version = 1
                block_timestamps = true

                [[contracts]]
                alias = "c0"
                address = "0x0000000000000000000000000000000000000001"
                abi = "abis/c0.json"
            "#,
        )
        .unwrap();
        let mut sem = crate::semantic::Semantic::default();
        let mut ts = crate::semantic::TableSemantic {
            description: "every transfer through the token".into(),
            ..Default::default()
        };
        ts.columns
            .insert("from".into(), "the sender, authored".into());
        sem.tables.insert("c0__transfer".into(), ts);
        crate::semantic::save(dir.path(), &sem).unwrap();

        rename_alias(dir.path(), "c0", "gns").unwrap();

        let cfg = Config::load(dir.path()).unwrap();
        assert_eq!(cfg.contracts[0].alias, "gns");
        assert_eq!(cfg.contracts[0].abi, "abis/gns.json");
        assert!(dir.path().join("abis/gns.json").exists());
        assert!(!dir.path().join("abis/c0.json").exists());

        let after = crate::semantic::load(dir.path()).unwrap().unwrap();
        assert!(
            after.tables.contains_key("gns__transfer"),
            "re-key must land the table under the new alias: {:?}",
            after.tables.keys().collect::<Vec<_>>()
        );
        assert_eq!(
            after.tables["gns__transfer"].description, "every transfer through the token",
            "authored prose must survive; deleting rekey_alias fails this"
        );
        assert_eq!(
            after.tables["gns__transfer"].columns["from"],
            "the sender, authored"
        );
        assert!(
            !after.tables.contains_key("c0__transfer"),
            "the old key must be gone"
        );
    }

    #[test]
    fn scaffold_views_creates_a_commented_starter_and_never_clobbers() {
        use crate::registry::{ColumnSchema, TableSchema};
        let dir = tempfile::tempdir().unwrap();
        let schema = vec![TableSchema {
            table: "usdc__transfer".into(),
            alias: "usdc".into(),
            kind: crate::registry::TableKind::Event,
            event: "Transfer".into(),
            topic0: "0xddf2".into(),
            function: String::new(),
            selector: String::new(),
            columns: vec![ColumnSchema {
                name: "value".into(),
                sol_type: "uint256".into(),
                storage: "word32".into(),
                indexed: false,
                components: Vec::new(),
            }],
        }];
        scaffold_views(dir.path(), &schema).unwrap();
        let starter = std::fs::read_to_string(dir.path().join("views/10-example.sql")).unwrap();
        assert!(dir.path().join("views/README.md").exists());
        assert_eq!(
            std::fs::read_to_string(dir.path().join(".gitattributes")).unwrap(),
            nuthatch_scaffold::GITATTRIBUTES
        );
        // References the nest's real table, and every line is a comment (a no-op that validates clean).
        assert!(starter.contains("usdc__transfer"));
        assert!(starter
            .lines()
            .all(|l| l.trim().is_empty() || l.trim_start().starts_with("--")));

        // Idempotent: a second call (e.g. `add` on a nest with authored views) never overwrites.
        std::fs::write(dir.path().join("views/10-example.sql"), "-- author's edit").unwrap();
        scaffold_views(dir.path(), &schema).unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.path().join("views/10-example.sql")).unwrap(),
            "-- author's edit",
            "existing views/ is never clobbered"
        );

        // A nest that already has views/ still gets the attributes, and an author's own are kept.
        let old = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(old.path().join("views")).unwrap();
        scaffold_views(old.path(), &schema).unwrap();
        assert_eq!(
            std::fs::read_to_string(old.path().join(".gitattributes")).unwrap(),
            nuthatch_scaffold::GITATTRIBUTES
        );
        std::fs::write(
            old.path().join(".gitattributes"),
            "*.md linguist-documentation\n",
        )
        .unwrap();
        scaffold_views(old.path(), &schema).unwrap();
        assert_eq!(
            std::fs::read_to_string(old.path().join(".gitattributes")).unwrap(),
            "*.md linguist-documentation\n"
        );
    }

    /// #1607: the README demo, USDC from deployment, is offered its last 300 blocks; a nest with a
    /// short history or no detected start is offered nothing.
    #[test]
    fn init_offers_recent_history_only_beside_a_long_backfill() {
        let hint = recent_history_hint(Some(6_082_465), Some(26_096_175), 20, "usdc").unwrap();
        assert!(
            hint.contains("nuthatch dev --dir usdc --backfill 300 "),
            "{hint}"
        );
        assert!(hint.contains("20013710 blocks from deployment"), "{hint}");
        assert_eq!(
            recent_history_hint(Some(26_090_000), Some(26_096_175), 20, "."),
            None
        );
        assert_eq!(recent_history_hint(None, Some(26_096_175), 20, "."), None);
        assert_eq!(recent_history_hint(Some(6_082_465), None, 20, "."), None);
    }

    #[test]
    fn start_blocks_are_positional_like_abi_overrides() {
        assert_eq!(resolve_start_blocks(&[], 2).unwrap(), vec![None, None]);
        assert_eq!(
            resolve_start_blocks(&["100".into(), "".into(), "200".into()], 3).unwrap(),
            vec![Some(100), None, Some(200)]
        );
        assert!(resolve_start_blocks(&["100".into()], 2).is_err());
        assert!(resolve_start_blocks(&["nope".into()], 1).is_err());
    }

    #[test]
    fn eip1967_impl_extraction() {
        assert!(impl_from_slot(
            "0x0000000000000000000000000000000000000000000000000000000000000000"
        )
        .is_none());
        assert_eq!(
            impl_from_slot("0x00000000000000000000000043506849d7c04f9138d1a2050bbf3a0c054402dd"),
            Some("0x43506849d7c04f9138d1a2050bbf3a0c054402dd".to_string())
        );
        // A beacon's `implementation()` return is the same 32-byte left-padded address as a slot word,
        // so the same decoder handles the beacon hop.
        assert_eq!(
            impl_from_slot("0x000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
            Some("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48".to_string())
        );
        // An empty `eth_call` return (non-proxy / reverted) yields no implementation, not a bad address.
        assert!(impl_from_slot("0x").is_none());

        // stETH's Aragon AppProxyUpgradeable answers `implementation()` directly with this word.
        // Keep the observed shape here: its public ABI has only ProxyDeposit, so missing this hop
        // makes an otherwise ordinary `init <address>` scaffold an event surface that cannot match.
        assert_eq!(
            impl_from_slot("0x000000000000000000000000028271e30a695c0527a0c50ca30603fed004cdb0"),
            Some("0x028271e30a695c0527a0c50ca30603fed004cdb0".to_string())
        );
    }

    #[test]
    fn current_implementation_cannot_describe_history_before_it_existed() {
        assert!(proxy_history_may_be_missing(100, 200));
        assert!(!proxy_history_may_be_missing(200, 200));
        assert!(!proxy_history_may_be_missing(300, 200));
    }

    #[test]
    fn proxy_slots_are_well_formed() {
        // The three direct-address patterns (EIP-1967, EIP-1822, legacy zeppelinos) plus the beacon
        // slot are all 32-byte (66-char) storage keys.
        assert_eq!(PROXY_IMPL_SLOTS.len(), 3);
        assert!(PROXY_IMPL_SLOTS
            .iter()
            .all(|s| s.len() == 66 && s.starts_with("0x")));
        assert_eq!(PROXY_BEACON_SLOT.len(), 66);
        assert_eq!(IMPLEMENTATION_SELECTOR, "0x5c60da1b");
    }

    #[test]
    fn address_normalisation() {
        assert_eq!(
            normalise_address("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48").unwrap(),
            "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        );
        assert!(normalise_address("0x123").is_err());
    }

    #[test]
    fn git_source_detection() {
        assert!(is_git_source("https://github.com/cargopete/horizon-nest"));
        assert!(is_git_source("git@github.com:cargopete/horizon-nest.git"));
        assert!(is_git_source("./local-bare-repo.git"));
        assert!(!is_git_source("./horizon-nest"));
        assert!(!is_git_source("/abs/path/to/nest"));
    }

    #[test]
    fn source_basename_derives_nest_dir() {
        assert_eq!(
            source_basename("https://github.com/cargopete/horizon-nest"),
            "horizon-nest"
        );
        assert_eq!(
            source_basename("https://github.com/cargopete/horizon-nest.git"),
            "horizon-nest"
        );
        assert_eq!(
            source_basename("git@github.com:cargopete/horizon-nest.git"),
            "horizon-nest"
        );
        assert_eq!(source_basename("./local/my-nest/"), "my-nest");
    }

    // ---- The silent-proxy check (RFC-0001 follow-up) --------------------------------------------

    /// The Livepeer shape: a `ManagerProxy` whose own ABI carries two administrative events, in front
    /// of an implementation that emits everything anyone actually wants. Sourcify returns the former.
    const PROXY_ABI: &str = r#"[
      {"type":"event","name":"ParameterUpdate","inputs":[{"name":"param","type":"string","indexed":false}],"anonymous":false},
      {"type":"event","name":"SetController","inputs":[{"name":"controller","type":"address","indexed":false}],"anonymous":false}
    ]"#;

    const IMPL_ABI: &str = r#"[
      {"type":"event","name":"Bond","inputs":[
        {"name":"newDelegate","type":"address","indexed":true},
        {"name":"oldDelegate","type":"address","indexed":true}],"anonymous":false}
    ]"#;

    fn topic0s_of(abi_src: &str) -> Vec<String> {
        abi_event_topic0s(&serde_json::from_str(abi_src).unwrap())
    }

    #[test]
    fn topic0s_come_from_the_same_selector_decode_uses() {
        let t = topic0s_of(IMPL_ABI);
        assert_eq!(t.len(), 1);
        // keccak("Bond(address,address)") - if this ever disagrees with the registry's topic0 the
        // check would pass on nests that cannot decode and fail on nests that can.
        let via_registry = {
            let abi: alloy_json_abi::JsonAbi = serde_json::from_str(IMPL_ABI).unwrap();
            let ev = abi.events().next().unwrap().clone();
            format!("0x{}", hex::encode(ev.selector()))
        };
        assert_eq!(t[0], via_registry);
    }

    /// The whole point: logs exist, the ABI decodes none of them.
    #[test]
    fn a_proxy_abi_against_implementation_logs_is_a_mismatch() {
        let bond = topic0s_of(IMPL_ABI)[0].clone();
        let fit = fit_from_sample(&topic0s_of(PROXY_ABI), &[Some(&bond), Some(&bond)]);
        assert_eq!(fit, AbiFit::Mismatch { sampled: 2 });
    }

    #[test]
    fn one_matching_log_is_enough_to_clear_the_check() {
        let bond = topic0s_of(IMPL_ABI)[0].clone();
        let other = "0xdeadbeef".repeat(8);
        assert_eq!(
            fit_from_sample(&topic0s_of(IMPL_ABI), &[Some(&other), Some(&bond)]),
            AbiFit::Fits,
            "a contract may emit events its ABI omits; only a total miss is the silent failure"
        );
    }

    /// A dormant contract and a wrong ABI look identical from an empty sample. Claiming a mismatch
    /// here would train people to ignore the warning.
    #[test]
    fn an_empty_sample_makes_no_claim() {
        assert_eq!(
            fit_from_sample(&topic0s_of(IMPL_ABI), &[]),
            AbiFit::NoSample
        );
    }

    #[test]
    fn topic0_matching_is_case_insensitive() {
        let upper = topic0s_of(IMPL_ABI)[0].to_uppercase().replace("0X", "0x");
        assert_eq!(
            fit_from_sample(&topic0s_of(IMPL_ABI), &[Some(&upper)]),
            AbiFit::Fits,
            "lowercase hex is a convention of eth_getLogs, not a guarantee"
        );
    }

    #[test]
    fn anonymous_events_never_match() {
        assert_eq!(
            fit_from_sample(&topic0s_of(IMPL_ABI), &[None]),
            AbiFit::Mismatch { sampled: 1 }
        );
    }

    /// #512: the tip probe must use the chain's own RPC-safe span, not a wider fixed guess. A
    /// hardcoded 1,000-block window is exactly what sent USDC's real probe through every default
    /// mainnet provider's result-size cap.
    #[test]
    fn tip_probe_window_matches_the_chains_log_window() {
        let tip = 25_745_042u64;
        let windows = probe_windows(tip, Some(6_082_465), 20);
        assert_eq!(windows[0], (tip - 20, tip));
    }

    /// Deep gap between deploy and tip: both probes stay within `probe` blocks wide, and never widen
    /// back out to some other constant regardless of how old the contract is.
    #[test]
    fn deploy_probe_window_is_also_bounded_by_log_window() {
        let tip = 25_745_042u64;
        let start = 6_082_465u64;
        let windows = probe_windows(tip, Some(start), 20);
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[1], (start, start + 20));
    }

    /// A contract deployed within `2 * probe` of tip has overlapping windows - only the tip probe
    /// runs, so a young contract isn't probed twice for the same blocks.
    #[test]
    fn recently_deployed_contract_gets_one_window() {
        let tip = 25_745_042u64;
        let windows = probe_windows(tip, Some(tip - 10), 20);
        assert_eq!(windows.len(), 1);
    }

    /// #773: Graph staking's shape. Tip logs match the Horizon ABI; logs near deployment match none
    /// of it. The height heuristic cannot see this (the current impl address predates the proxy).
    #[test]
    fn tip_fit_and_historical_mismatch_is_the_upgrade_trap() {
        assert_eq!(
            combine_abi_fits(
                Some(AbiFit::Fits),
                Some(AbiFit::Mismatch { sampled: 32 }),
                false
            ),
            AbiFit::HistoricalMismatch { sampled: 32 }
        );
        // A wrong current ABI is still a full mismatch, even if older history happens to fit.
        assert_eq!(
            combine_abi_fits(
                Some(AbiFit::Mismatch { sampled: 10 }),
                Some(AbiFit::Fits),
                false
            ),
            AbiFit::Mismatch { sampled: 10 }
        );
        assert_eq!(
            combine_abi_fits(Some(AbiFit::Fits), None, false),
            AbiFit::Fits
        );
        assert_eq!(combine_abi_fits(None, None, true), AbiFit::Unknown);
        assert_eq!(combine_abi_fits(None, None, false), AbiFit::NoSample);
    }

    // ---- --abi override -------------------------------------------------------------------------

    #[test]
    fn abi_overrides_align_positionally_and_allow_gaps() {
        assert_eq!(
            resolve_abi_overrides(&[], 3).unwrap(),
            vec![None, None, None]
        );
        assert_eq!(
            resolve_abi_overrides(&["".into(), "impl.json".into()], 2).unwrap(),
            vec![None, Some("impl.json".to_string())],
            "overriding one contract must not force you to find ABIs for the others"
        );
        assert!(
            resolve_abi_overrides(&["a.json".into()], 2).is_err(),
            "a count mismatch silently applying to the wrong address is the bug this prevents"
        );
    }

    #[test]
    fn a_local_abi_is_validated_and_artifact_wrappers_are_unwrapped() {
        let dir = tempfile::tempdir().unwrap();
        let plain = dir.path().join("plain.json");
        std::fs::write(&plain, IMPL_ABI).unwrap();
        let (got, name) = read_local_abi(plain.to_str().unwrap()).unwrap();
        assert_eq!(abi_event_topic0s(&got).len(), 1);
        assert_eq!(name, None, "a bare ABI array has no contractName");

        // A Hardhat/Foundry artifact wraps the ABI under "abi" - the file people reach for first.
        // The name lives on the wrapper and is gone once the array is unwrapped, so it has to be
        // taken here (#774).
        let wrapped = dir.path().join("artifact.json");
        std::fs::write(
            &wrapped,
            format!(r#"{{"contractName":"Vat","abi":{IMPL_ABI},"bytecode":"0x"}}"#),
        )
        .unwrap();
        let (abi, name) = read_local_abi(wrapped.to_str().unwrap()).unwrap();
        assert_eq!(abi_event_topic0s(&abi).len(), 1);
        assert_eq!(
            name.as_deref(),
            Some("Vat"),
            "unwrapping must not drop contractName"
        );

        let junk = dir.path().join("junk.json");
        std::fs::write(&junk, r#"{"schema":"this is a subgraph manifest"}"#).unwrap();
        assert!(
            read_local_abi(junk.to_str().unwrap()).is_err(),
            "a wrong file must fail here, not silently become an empty registry"
        );
        assert!(read_local_abi(dir.path().join("nope.json").to_str().unwrap()).is_err());
    }

    /// **Issue #241 item 2.** A hand-written `nuthatch.toml` has no `schema.json`, and the schema tool
    /// then recommends `{col}_dec` companions that do not exist - `Binder Error: Referenced column
    /// "delta_dec" not found` for anyone (or any agent) that follows the advice.
    ///
    /// The advice comes from the live registry; the columns come from `schema.json`. They can only
    /// agree if the file is there and current, so startup makes it so.
    #[test]
    fn a_missing_schema_is_regenerated_not_merely_warned_about() {
        let dir = tempfile::tempdir().unwrap();
        write_hand_authored_nest(dir.path());
        assert!(!dir.path().join("schema.json").exists());

        let cfg = Config::load(dir.path()).unwrap();
        let what = refresh_stale_artifacts(dir.path(), &cfg).unwrap();
        assert!(
            what.as_deref().unwrap_or("").contains("was missing"),
            "it must say what it did and why: {what:?}"
        );
        assert!(
            dir.path().join("schema.json").exists(),
            "the file the _dec columns come from must now exist"
        );

        // Idempotent: a second call finds nothing to do, so `dev` does not rewrite artifacts on every
        // restart and churn anyone's git status.
        assert_eq!(refresh_stale_artifacts(dir.path(), &cfg).unwrap(), None);
    }

    /// A schema older than the config is the subtler half: `add` a contract by hand, and the file
    /// exists but describes the previous nest.
    #[test]
    fn a_schema_older_than_the_config_is_refreshed() {
        let dir = tempfile::tempdir().unwrap();
        write_hand_authored_nest(dir.path());
        let cfg = Config::load(dir.path()).unwrap();
        refresh_stale_artifacts(dir.path(), &cfg).unwrap();

        // Touch the config forward, which is what editing it does.
        let later = std::time::SystemTime::now() + std::time::Duration::from_secs(120);
        let f = std::fs::File::options()
            .write(true)
            .open(dir.path().join(crate::config::CONFIG_FILE))
            .unwrap();
        f.set_modified(later).unwrap();

        let what = refresh_stale_artifacts(dir.path(), &cfg).unwrap();
        assert!(
            what.as_deref().unwrap_or("").contains("older than"),
            "a stale schema must be refreshed, not just a missing one: {what:?}"
        );
    }

    /// The regenerated schema must actually carry the big-int columns whose `_dec` companions the
    /// advice promises - otherwise the file exists and the advice is still wrong, which is the failure
    /// wearing a hat.
    #[test]
    fn the_regenerated_schema_carries_the_columns_the_advice_promises() {
        let dir = tempfile::tempdir().unwrap();
        write_hand_authored_nest(dir.path());
        let cfg = Config::load(dir.path()).unwrap();
        refresh_stale_artifacts(dir.path(), &cfg).unwrap();

        let raw = std::fs::read_to_string(dir.path().join("schema.json")).unwrap();
        let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let cols: Vec<String> = v["tables"][0]["columns"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["name"].as_str().unwrap().to_string())
            .collect();
        assert!(
            cols.iter().any(|c| c == "value"),
            "the uint256 column must be described: {cols:?}"
        );
    }

    /// A nest written by hand: config + ABI, no derived artifacts. Exactly the POA case from #241.
    fn write_hand_authored_nest(dir: &Path) {
        std::fs::create_dir_all(dir.join("abis")).unwrap();
        std::fs::write(
            dir.join("abis/tok.json"),
            r#"[{"type":"event","name":"Transfer","anonymous":false,"inputs":[
                {"name":"from","type":"address","indexed":true},
                {"name":"to","type":"address","indexed":true},
                {"name":"value","type":"uint256","indexed":false}]}]"#,
        )
        .unwrap();
        std::fs::write(
            dir.join(crate::config::CONFIG_FILE),
            r#"
[nest]
name = "hand"
chain = "mainnet"
chain_id = 1
rpc_urls = ["https://rpc.example"]

[[contracts]]
alias = "tok"
address = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
abi = "abis/tok.json"
"#,
        )
        .unwrap();
    }

    /// **Audit finding 6.** A `.git` suffix must not qualify a transport helper as a git source.
    ///
    /// `git clone 'ext::sh -c <cmd>'` executes the command. Before this, `ext::sh -c … .git` passed
    /// `is_git_source` and reached `git clone`, and was refused only by git's own
    /// `protocol.ext.allow=never` default. An operator who has set that to `always` - CI images do -
    /// would have had remote code execution from `init --from`.
    #[test]
    fn a_transport_helper_is_not_a_git_source() {
        for hostile in [
            "ext::sh -c touch /tmp/pwned .git",
            "ext::sh -c id",
            "transport-helper::whatever.git",
            "file::/etc/passwd.git",
        ] {
            assert!(
                !is_git_source(hostile),
                "must not be treated as a git source: {hostile}"
            );
        }
    }

    /// …and the real ones still are, or `init --from` stops working for everyone.
    #[test]
    fn ordinary_git_sources_still_qualify() {
        for ok in [
            "https://github.com/nuthatch-org/poa-nest",
            "https://github.com/nuthatch-org/poa-nest.git",
            "http://internal.example/nest.git",
            "ssh://git@github.com/x/y.git",
            "git@github.com:nuthatch-org/poa-nest.git",
            "git://legacy.example/x.git",
            "/srv/nests/mine.git",
        ] {
            assert!(is_git_source(ok), "must still be a git source: {ok}");
        }
        // A plain local directory is not a git source and must still load as a directory.
        assert!(!is_git_source("./my-nest"));
        assert!(!is_git_source("/srv/nests/mine"));
    }

    // ---- #535: init rejects chains the nest format supports ---------------------------------------
    //
    // `--from-subgraph` on a manifest for a chain outside the built-in three named a remedy
    // (`--chain <name> --rpc <url>`) that `chains::lookup` then refused. The round trip below is the
    // one the user actually took: fetch the manifest, hit the error, follow its own advice exactly as
    // printed, and expect it to work - not a test that pins whichever half of `init_from_subgraph` got
    // edited.

    /// A local HTTP server answering fixed paths with fixed bodies - stands in for the manifest URL
    /// (`--from-subgraph`, `Origin::Operator`, fetched directly) and an IPFS gateway (`--ipfs`,
    /// `Origin::Manifest`, `{gateway}{cid}`). Real HTTP, so `fetch_ipfs`'s actual request path runs.
    async fn fake_gateway(
        routes: Vec<(&'static str, &'static str)>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        use axum::{extract::State, routing::get, Router};
        use std::collections::BTreeMap;
        use std::sync::Arc;

        let routes: Arc<BTreeMap<&'static str, &'static str>> =
            Arc::new(routes.into_iter().collect());

        async fn handler(
            State(routes): State<Arc<BTreeMap<&'static str, &'static str>>>,
            uri: axum::http::Uri,
        ) -> Result<String, axum::http::StatusCode> {
            routes
                .get(uri.path())
                .map(|s| s.to_string())
                .ok_or(axum::http::StatusCode::NOT_FOUND)
        }

        let app = Router::new()
            .route("/{*rest}", get(handler))
            .with_state(routes);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{addr}"), handle)
    }

    /// A one-endpoint fake JSON-RPC server answering `eth_chainId` - the round trip's `--rpc <url>`.
    async fn fake_chain_id_rpc(chain_id: u64) -> (String, tokio::task::JoinHandle<()>) {
        use axum::{extract::State, routing::post, Json, Router};
        use serde_json::{json, Value};

        async fn handler(State(chain_id): State<u64>, Json(_req): Json<Value>) -> Json<Value> {
            Json(json!({"jsonrpc": "2.0", "id": 1, "result": format!("0x{chain_id:x}")}))
        }

        let app = Router::new().route("/", post(handler)).with_state(chain_id);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{addr}"), handle)
    }

    /// Mainnet at block 0x10, answering `eth_getCode` with `code`.
    async fn fake_code_rpc(code: &'static str) -> (String, tokio::task::JoinHandle<()>) {
        use axum::{extract::State, routing::post, Json, Router};
        use serde_json::{json, Value};

        async fn handler(State(code): State<&'static str>, Json(req): Json<Value>) -> Json<Value> {
            let result = match req["method"].as_str() {
                Some("eth_chainId") => json!("0x1"),
                Some("eth_blockNumber") => json!("0x10"),
                Some("eth_getCode") => json!(code),
                _ => Value::Null,
            };
            Json(json!({"jsonrpc": "2.0", "id": 1, "result": result}))
        }

        let app = Router::new().route("/", post(handler)).with_state(code);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{addr}"), handle)
    }

    /// Accepts connections and never answers.
    async fn silent_rpc() -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((sock, _)) = listener.accept().await {
                held.push(sock);
            }
        });
        (format!("http://{addr}"), handle)
    }

    /// #1885: a silent first endpoint held `init`'s chain probe for the client's 20 s timeout.
    #[tokio::test]
    async fn a_silent_endpoint_costs_the_chain_probe_at_most_its_short_budget() {
        let (silent, hs) = silent_rpc().await;
        let (code, hc) = fake_code_rpc("0x6080").await;
        let (empty, he) = fake_code_rpc("0x").await;
        let all = chains::all();
        let (mainnet, base, arb) = (all[0], all[1], all[2]);

        let started = std::time::Instant::now();
        let probed = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            probe_chains(
                "0x00000000000000000000000000000000000000aa",
                vec![
                    (mainnet, vec![silent.clone(), code]),
                    (base, vec![silent.clone(), empty]),
                    (arb, vec![silent]),
                ],
                CHAIN_PROBE_TIMEOUT,
            ),
        )
        .await
        .expect("the probe hung");
        let elapsed = started.elapsed();

        let probed: Vec<_> = probed.iter().map(|(c, f)| (c.name, *f)).collect();
        assert_eq!(
            probed,
            vec![
                (mainnet.name, Some(true)),
                (base.name, Some(false)),
                (arb.name, None)
            ],
            "a healthy endpoint behind a silent one still answers, and a chain that never \
             answered is unreachable rather than absent"
        );
        assert!(
            elapsed < std::time::Duration::from_secs(4),
            "a silent endpoint held the chain probe for {elapsed:?}"
        );
        for h in [hs, hc, he] {
            h.abort();
        }
    }

    /// #1885: after detection, `init` built a fresh pool and paid the silent endpoint's 20 s again.
    #[tokio::test]
    async fn a_silent_endpoint_costs_init_at_most_its_short_budget() {
        let (silent, hs) = silent_rpc().await;
        let (good, hg) = fake_code_rpc("0x6080").await;
        let dir = tempfile::tempdir().unwrap();
        let abi = dir.path().join("pool.json");
        std::fs::write(&abi, POOL_ABI).unwrap();
        let args = InitArgs {
            explorer: None,
            addresses: vec!["0x00000000000000000000000000000000000000aa".into()],
            from: None,
            from_subgraph: None,
            ipfs: vec![],
            alias: vec!["pool".into()],
            abi: vec![abi.to_string_lossy().into_owned()],
            start_block: vec!["1".into()],
            chain: Some("mainnet".into()),
            rpc: vec![silent, good],
            dir: dir.path().join("nest").to_string_lossy().into_owned(),
            no_timestamps: false,
        };

        let started = std::time::Instant::now();
        tokio::time::timeout(std::time::Duration::from_secs(90), init(args))
            .await
            .expect("init hung")
            .unwrap();
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "a silent endpoint held init for {elapsed:?}"
        );
        hs.abort();
        hg.abort();
    }

    const BSC_MANIFEST: &str = r#"
specVersion: 0.0.5
dataSources:
  - kind: ethereum
    name: Pool
    network: bsc
    source:
      abi: Pool
      address: "0x0000000000000000000000000000000000000001"
    mapping:
      abis:
        - file:
            /: /ipfs/Qmco6j6G3fpC1VVoBFFYjTY6hvJxUxUrtaqgFCftA6RW4s
          name: Pool
      eventHandlers:
        - event: Transfer(indexed address,indexed address,uint256)
          handler: handleTransfer
"#;

    const POOL_ABI: &str = r#"[{"type":"event","name":"Transfer","anonymous":false,"inputs":[
        {"name":"from","type":"address","indexed":true},
        {"name":"to","type":"address","indexed":true},
        {"name":"value","type":"uint256","indexed":false}]}]"#;

    /// **The whole of #1321, end to end**: a subgraph *repository* - `subgraph.yaml` beside
    /// `abis/*.json`, which is what a builder has before they deploy - scaffolds a working nest
    /// from a path, with no CID and no gateway anywhere in the run.
    ///
    /// It is deliberately not a unit test of the resolver: the resolver already has its own, and
    /// three fixtures in this repo's history proved a handler and never the wiring. This drives
    /// `init_from_subgraph`, which is what the operator types.
    #[tokio::test]
    async fn a_subgraph_repository_scaffolds_from_a_path_with_no_gateway() {
        let repo = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(repo.path().join("abis")).unwrap();
        std::fs::write(repo.path().join("abis/Pool.json"), POOL_ABI).unwrap();
        std::fs::write(
            repo.path().join("subgraph.yaml"),
            r#"
specVersion: 0.0.5
dataSources:
  - kind: ethereum
    name: Pool
    network: mainnet
    source:
      abi: Pool
      address: "0x0000000000000000000000000000000000000001"
      startBlock: 1234
    mapping:
      abis:
        - name: Pool
          file: ./abis/Pool.json
      eventHandlers:
        - event: Transfer(indexed address,indexed address,uint256)
          handler: handleTransfer
"#,
        )
        .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let source = repo
            .path()
            .join("subgraph.yaml")
            .to_string_lossy()
            .into_owned();
        let args = InitArgs {
            explorer: None,
            addresses: vec![],
            from: None,
            from_subgraph: Some(source.clone()),
            // Deliberately an unreachable gateway: if anything in this path touches IPFS, the test
            // fails rather than quietly succeeding through a network it should never have used.
            ipfs: vec!["http://127.0.0.1:1/never/".into()],
            alias: vec![],
            abi: vec![],
            start_block: vec![],
            chain: None,
            rpc: vec![],
            dir: dir.path().to_string_lossy().into_owned(),
            no_timestamps: false,
        };

        init_from_subgraph(&source, &args)
            .await
            .expect("a repository manifest is the whole input a nest needs");

        let config = Config::load(dir.path()).unwrap();
        // `mainnet` is the canonical name in `chains.rs`; `ethereum` is one of its aliases.
        assert_eq!(config.nest.chain, "mainnet");
        assert_eq!(config.nest.chain_id, 1);
        assert_eq!(
            config.contracts.len(),
            1,
            "the dataSource must have become a contract"
        );
        // The issue says startBlock, the event allowlist and the handler notes carry over unchanged.
        assert_eq!(config.contracts[0].start_block, Some(1234));

        // And the ABI was vendored from disk, not invented or silently skipped - which is what the
        // old `filter_map` did to a `file:` entry.
        let vendored = std::fs::read_to_string(
            dir.path()
                .join(format!("abis/{}.json", config.contracts[0].alias)),
        )
        .expect("the ABI must have been vendored");
        assert!(
            vendored.contains("Transfer"),
            "the vendored ABI is not the repository's: {vendored}"
        );
    }

    /// Write a repository manifest (and a one-event ABI per source) and scaffold from it.
    async fn scaffold_repository(manifest: &str, abis: &[&str]) -> (tempfile::TempDir, Result<()>) {
        let repo = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(repo.path().join("abis")).unwrap();
        for name in abis {
            std::fs::write(repo.path().join(format!("abis/{name}.json")), POOL_ABI).unwrap();
        }
        std::fs::write(repo.path().join("subgraph.yaml"), manifest).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let source = repo
            .path()
            .join("subgraph.yaml")
            .to_string_lossy()
            .into_owned();
        let args = InitArgs {
            explorer: None,
            addresses: vec![],
            from: None,
            from_subgraph: Some(source.clone()),
            ipfs: vec!["http://127.0.0.1:1/never/".into()],
            alias: vec![],
            abi: vec![],
            start_block: vec![],
            chain: None,
            rpc: vec![],
            dir: dir.path().to_string_lossy().into_owned(),
            no_timestamps: false,
        };
        let r = init_from_subgraph(&source, &args).await;
        (dir, r)
    }

    /// #1941: thena-blocks (`QmenTzjuovnYnptb6tMpwLPvzvyiH7zGtWUMxCJaHupXJF`) is one dataSource with
    /// one block handler. It must become a blocks nest, not a contract indexing every ABI event.
    #[tokio::test]
    async fn a_block_handler_only_subgraph_scaffolds_a_blocks_nest() {
        let (dir, r) = scaffold_repository(
            r#"
specVersion: 0.0.4
dataSources:
  - kind: ethereum/contract
    name: blocks
    network: bsc
    source:
      abi: blocks
      address: "0x0000000000000000000000000000000000001000"
      startBlock: 24468802
    mapping:
      abis:
        - name: blocks
          file: ./abis/blocks.json
      blockHandlers:
        - handler: handleBlock
"#,
            &["blocks"],
        )
        .await;
        r.expect("a blocks subgraph scaffolds");
        let config = Config::load(dir.path()).unwrap();
        assert!(
            config.contracts.is_empty(),
            "no contract may index the ABI's events: {:?}",
            config.contracts
        );
        assert!(
            config.extract.blocks,
            "the block handler becomes block rows"
        );
    }

    /// polygon-blocks (`QmdNFXbQooUNuy2UQGciY5Lzb3LgoKegfkn96Le6gio78p`): a block-handler-only source
    /// beside event sources is dropped rather than indexing its ABI's every event, and block rows
    /// are not switched on for the whole nest on its account.
    #[tokio::test]
    async fn a_block_handler_only_source_beside_event_sources_is_not_a_contract() {
        let (dir, r) = scaffold_repository(
            r#"
specVersion: 0.0.5
dataSources:
  - kind: ethereum
    name: SystemData
    network: matic
    source:
      abi: Registry
      address: "0x0000000000000000000000000000000000000001"
      startBlock: 10
    mapping:
      abis:
        - name: Registry
          file: ./abis/Registry.json
      blockHandlers:
        - handler: handleBlock
  - kind: ethereum
    name: Registry
    network: matic
    source:
      abi: Registry
      address: "0x0000000000000000000000000000000000000001"
      startBlock: 10
    mapping:
      abis:
        - name: Registry
          file: ./abis/Registry.json
      eventHandlers:
        - event: Transfer(indexed address,indexed address,uint256)
          handler: handleTransfer
"#,
            &["Registry"],
        )
        .await;
        r.expect("scaffolds");
        let config = Config::load(dir.path()).unwrap();
        let aliases: Vec<&str> = config.contracts.iter().map(|c| c.alias.as_str()).collect();
        assert_eq!(aliases, vec!["registry"]);
        assert_eq!(config.contracts[0].events, vec!["Transfer".to_string()]);
        assert!(!config.extract.blocks);
    }

    /// A local repository may have a file whose name is also a CID. The two links render to the
    /// same string, but one is disk content and the other is content addressed: sharing their
    /// cache entry silently vendors the first ABI for both (#1321).
    #[tokio::test]
    async fn a_path_and_a_cid_with_the_same_spelling_do_not_share_an_abi_cache_entry() {
        use crate::subgraph_import::{AbiLink, AbiRef, ManifestHome};

        let remote = r#"[{"type":"event","name":"Remote","inputs":[]}]"#;
        let cid = crate::cid::cid_v0_for(remote.as_bytes());
        let repo = tempfile::tempdir().unwrap();
        std::fs::write(
            repo.path().join(&cid),
            r#"[{"type":"event","name":"Local","inputs":[]}]"#,
        )
        .unwrap();
        let path: &'static str = Box::leak(format!("/{cid}").into_boxed_str());
        let (gateway, handle) =
            fake_gateway(vec![(path, Box::leak(remote.to_string().into_boxed_str()))]).await;

        let nest = tempfile::tempdir().unwrap();
        std::fs::create_dir(nest.path().join("abis")).unwrap();
        let mut notes = Vec::new();
        let mut fetched = std::collections::BTreeMap::new();
        let home = ManifestHome::Local(repo.path().to_path_buf());
        let gateways = vec![format!("{gateway}/")];

        fetch_and_vendor_abi(
            nest.path(),
            "local",
            &AbiRef {
                name: "local".into(),
                link: AbiLink::Path(cid.clone()),
            },
            &gateways,
            &home,
            &mut notes,
            &mut fetched,
        )
        .await
        .unwrap();
        fetch_and_vendor_abi(
            nest.path(),
            "remote",
            &AbiRef {
                name: "remote".into(),
                link: AbiLink::Cid(cid),
            },
            &gateways,
            &home,
            &mut notes,
            &mut fetched,
        )
        .await
        .unwrap();
        handle.abort();

        let local = std::fs::read_to_string(nest.path().join("abis/local.json")).unwrap();
        let remote = std::fs::read_to_string(nest.path().join("abis/remote.json")).unwrap();
        assert!(local.contains("Local"), "local ABI was replaced: {local}");
        assert!(
            remote.contains("Remote"),
            "CID ABI reused the path cache entry: {remote}"
        );
    }

    #[tokio::test]
    async fn from_subgraph_recommendation_is_followable_end_to_end() {
        // The CID is *derived from the content*, not invented. Before RFC-0037 slice 1 nothing
        // checked the two matched, so this fixture named a CID its body did not hash to - and the
        // test passed. It does not any more, which is the verification working.
        let pool_cid = crate::cid::cid_v0_for(POOL_ABI.as_bytes());
        // Leaked deliberately: `fake_gateway` wants `'static` paths and this is a test that ends.
        let pool_path: &'static str = Box::leak(format!("/ipfslike/{pool_cid}").into_boxed_str());
        // The manifest names the ABI by CID, so it has to name the *real* one too. It also has to
        // name a chain nuthatch genuinely does not ship: `bsc` became built-in when the top-25 list
        // added it, and this test is about the blind-then-remedy flow for an *unshipped* chain, which
        // stops being exercised the moment the chain is shipped.
        let manifest: &'static str = Box::leak(
            BSC_MANIFEST
                .replace("Qmco6j6G3fpC1VVoBFFYjTY6hvJxUxUrtaqgFCftA6RW4s", &pool_cid)
                .replace("bsc", "avalanche")
                .into_boxed_str(),
        );
        let (gateway, _gw) =
            fake_gateway(vec![("/manifest.yaml", manifest), (pool_path, POOL_ABI)]).await;
        let (rpc_url, _rpc) = fake_chain_id_rpc(43114).await;

        let dir = tempfile::tempdir().unwrap();
        let source = format!("{gateway}/manifest.yaml");
        let mut args = InitArgs {
            explorer: None,
            addresses: vec![],
            from: None,
            from_subgraph: Some(source.clone()),
            ipfs: vec![format!("{gateway}/ipfslike/")],
            alias: vec![],
            abi: vec![],
            start_block: vec![],
            chain: None,
            rpc: vec![],
            dir: dir.path().to_string_lossy().into_owned(),
            no_timestamps: false,
        };

        // Step 1: blind, this is the error the user actually hits - and the remedy it names.
        let err = init_from_subgraph(&source, &args).await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains(
                "the manifest indexes 'avalanche', which nuthatch has no built-in chain for"
            ),
            "{msg}"
        );
        assert!(
            msg.contains("re-run with --chain <name> --rpc <url> to point at it yourself"),
            "{msg}"
        );

        // Step 2: follow that remedy exactly as printed - `--chain bsc --rpc <url>` - and it must work.
        args.chain = Some("avalanche".to_string());
        args.rpc = vec![rpc_url.clone()];
        init_from_subgraph(&source, &args)
            .await
            .expect("the recommended re-run must succeed - the nest format already supports it");

        let config = Config::load(dir.path()).unwrap();
        assert_eq!(config.nest.chain, "avalanche");
        assert_eq!(config.nest.chain_id, 43114);
        assert_eq!(config.nest.rpc_urls, vec![rpc_url]);
    }

    /// #675: a Sourcify resolve used to print through a raw `tracing::info!` line (ISO timestamp, log
    /// level, ANSI codes) crashing through the `→`/`✓` pretty output. It now prints only the tick.
    /// Covers `abi_resolved_lines`'s formatted wording only - not that `resolve_abi` calls it, so a
    /// regression that drops the call site entirely would still pass this test.
    #[test]
    fn abi_resolved_lines_sourcify_is_a_single_clean_tick() {
        let resolved = abi::Resolved {
            abi: serde_json::json!([]),
            via: "Sourcify",
            fallback_reason: None,
            contract_name: None,
        };
        assert_eq!(
            abi_resolved_lines(&resolved),
            vec!["  ✓ ABI resolved via Sourcify".to_string()]
        );
    }

    #[test]
    fn abi_resolved_lines_names_the_keyless_fallback() {
        let resolved = abi::Resolved {
            abi: serde_json::json!([]),
            via: "Blockscout",
            fallback_reason: Some("Sourcify miss: no ABI".into()),
            contract_name: None,
        };
        assert_eq!(
            abi_resolved_lines(&resolved),
            vec![
                "  · Sourcify miss: no ABI".to_string(),
                "  ✓ ABI resolved via Blockscout".to_string(),
            ]
        );
    }

    /// The Etherscan-fallback path (#675's "sweep the other paths" ask): the miss reason is real,
    /// non-redundant information, so it earns its own `·` line ahead of the `✓` tick rather than being
    /// dropped along with the redundant Sourcify-success announcement.
    #[test]
    fn abi_resolved_lines_etherscan_fallback_names_the_reason_then_the_tick() {
        let resolved = abi::Resolved {
            abi: serde_json::json!([]),
            via: "Etherscan",
            fallback_reason: Some(
                "Sourcify miss: Sourcify returned HTTP 404; Blockscout miss: no ABI".to_string(),
            ),
            contract_name: None,
        };
        assert_eq!(
            abi_resolved_lines(&resolved),
            vec![
                "  · Sourcify miss: Sourcify returned HTTP 404; Blockscout miss: no ABI"
                    .to_string(),
                "  ✓ ABI resolved via Etherscan".to_string(),
            ]
        );
    }
}
