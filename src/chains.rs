//! The chain registry is `nuthatch_core::chains` (RFC-0077 S4), so an embedding follows a chain on
//! the node's policy. What stays here reaches an RPC or parses a URL.

use anyhow::Context;

pub use nuthatch_core::chains::*;

/// What an operator should know before indexing `chain` through the endpoints actually in use, when
/// it matters (#1570): only when timestamps are being fetched, and only for `arb1.arbitrum.io` itself.
///
/// `arb1` sends `blockTimestamp: 0x0` on every log, so every block's timestamp costs a header, and it
/// takes header batches of about ten at most. The header batch width is the narrowest any endpoint in
/// the pool takes, so while `arb1` is in the pool it bounds the rest: the note says so, and says to
/// replace it rather than add to it.
pub fn keyless_caveat(chain: &str, rpc_urls: &[String], timestamps: bool) -> Option<&'static str> {
    let arbitrum = lookup(chain).is_some_and(|c| c.chain_id == 42161);
    let arb1 = rpc_urls.iter().any(|u| host_is(u, ARB1));
    (timestamps && arbitrum && arb1).then_some(
        "arb1.arbitrum.io sends no timestamp on its logs, so each block's comes from a header, and it \
         takes header batches of about ten. While it is anywhere in the pool, primary or fallback, \
         every header batch goes out at ten, keyed endpoints included: a backfill here runs at a few \
         times chain speed, and a long one may be refused. For history, use a keyed Arbitrum RPC in \
         place of arb1 (in rpc_urls, or --rpc <url>).",
    )
}

/// Arbitrum's own keyless RPC, which the registry ships first for `arbitrum-one`.
const ARB1: &str = "arb1.arbitrum.io";

fn host_is(url: &str, host: &str) -> bool {
    reqwest::Url::parse(url).is_ok_and(|u| u.host_str() == Some(host))
}

/// The most block headers an endpoint in `rpc_urls` answers in one batch, where a known keyless one
/// answers fewer than nuthatch would send (#1570).
///
/// `arb1.arbitrum.io` answers ten and refuses fifty, and refuses them with a 429 that no response can
/// tell from a throttle. Inferring the limit from refusals (or probing for it) therefore mistakes a
/// throttle for a size limit and spends quota doing it, so the fact is recorded here, beside the
/// endpoint itself, as the registry records the rest of what it knows about the endpoints it ships.
pub fn header_batch_cap(rpc_urls: &[String]) -> Option<usize> {
    rpc_urls.iter().any(|u| host_is(u, ARB1)).then_some(10)
}

/// A resolved chain, ready for `init`/`add` to act on. `Chain` stays a purely `&'static` built-in
/// registry entry; this is the owned shape a custom (non-built-in) chain needs, since its name and
/// endpoints come from the command line rather than a `const`.
///
/// `Debug` is load-bearing rather than decorative: a test under the `scaled` feature unwraps a
/// `Result` carrying this, which needs it, and the default build never compiles that test - so the
/// omission was invisible until the scaled-mode CI job ran.
#[derive(Debug)]
pub struct ResolvedChain {
    pub name: String,
    pub chain_id: u64,
    pub rpc_urls: Vec<String>,
    pub finality: Finality,
    pub log_window: u64,
}

impl From<&'static Chain> for ResolvedChain {
    fn from(c: &'static Chain) -> Self {
        ResolvedChain {
            name: c.name.to_string(),
            chain_id: c.chain_id,
            rpc_urls: c.rpc_urls.iter().map(|s| s.to_string()).collect(),
            finality: c.finality,
            log_window: c.log_window,
        }
    }
}

/// Resolve `--chain` for `init`: a name in the built-in registry always wins, on its own vetted
/// endpoints and policy. A name outside it is accepted too, but only when paired with `--rpc` -
/// nothing else could tell nuthatch where to send traffic for a chain it doesn't ship. The custom
/// chain's `chain_id` comes from the RPC itself (`eth_chainId`, one call) rather than a flag, since
/// the endpoint already knows and a wrong hand-typed id would silently corrupt the nest.
///
/// This is the fix for issue #535: the nest format (`chain` + `chain_id` + `rpc_urls` in
/// `nuthatch.toml`) and the runtime (`indexer::index` falls back to [`UNREGISTERED_FINALITY`] /
/// [`UNREGISTERED_WINDOW`] for exactly this case) already support any chain - `init`'s allow-list
/// was the only thing narrower than what it scaffolds.
pub async fn resolve(name: &str, rpc_urls: &[String]) -> anyhow::Result<ResolvedChain> {
    if let Some(c) = lookup(name) {
        return Ok(c.into());
    }
    if rpc_urls.is_empty() {
        let known = all().iter().map(|c| c.name).collect::<Vec<_>>().join(", ");
        anyhow::bail!(
            "unknown chain '{name}' (try: {known}) - or pass --rpc <url> to point nuthatch at a \
             chain it doesn't ship built-in"
        );
    }
    let chain_id = crate::rpc::RpcClient::new(rpc_urls.to_vec())?
        .chain_id()
        .await
        .with_context(|| format!("could not read the chain id for '{name}' from --rpc"))?;
    Ok(ResolvedChain {
        name: name.to_string(),
        chain_id,
        rpc_urls: rpc_urls.to_vec(),
        finality: UNREGISTERED_FINALITY,
        log_window: UNREGISTERED_WINDOW,
    })
}

/// The chain of a nest already on disk: a known chain uses its own registry policy, and a nest
/// scaffolded against a custom chain (see [`resolve`]) has its `chain_id` saved in `nuthatch.toml`
/// already, so there is nothing left to ask an RPC for. Used by `add`, which grows an existing nest
/// and must never re-detect or re-resolve the chain it already committed to at `init`.
pub fn from_config(name: &str, chain_id: u64) -> ResolvedChain {
    match lookup(name) {
        Some(c) => c.into(),
        None => ResolvedChain {
            name: name.to_string(),
            chain_id,
            rpc_urls: Vec::new(),
            finality: UNREGISTERED_FINALITY,
            log_window: UNREGISTERED_WINDOW,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #1570: the caveat is for Arbitrum through `arb1`, under any name the chain goes by, and not
    /// for a nest that brought its own endpoint or another chain.
    #[test]
    fn the_keyless_arbitrum_caveat_names_only_the_case_it_is_about() {
        let arb1 = vec!["https://arb1.arbitrum.io/rpc".to_string()];
        let keyed = vec!["https://arb-mainnet.g.alchemy.com/v2/k".to_string()];
        assert!(keyless_caveat("arbitrum-one", &arb1, true).is_some());
        assert!(keyless_caveat("arbitrum", &arb1, true).is_some());
        assert!(keyless_caveat("arbitrum-one", &keyed, true).is_none());
        assert!(keyless_caveat("base", &arb1, true).is_none());
        assert_eq!(header_batch_cap(&arb1), Some(10));
        assert_eq!(header_batch_cap(&keyed), None);
        assert!(
            keyless_caveat("arbitrum-one", &arb1, false).is_none(),
            "no timestamps, no headers"
        );
        let lookalike = vec!["https://proxy.example/arb1.arbitrum.io/rpc".to_string()];
        assert!(
            keyless_caveat("arbitrum-one", &lookalike, true).is_none(),
            "the host, not a substring"
        );
    }

    #[test]
    fn arbitrum_is_registered_with_l2_finality() {
        let c = lookup("arbitrum-one").expect("arbitrum-one in registry");
        assert_eq!(c.chain_id, 42161);
        assert_eq!(
            c.finality,
            Finality::FinalizedTag {
                fallback_depth: 1800
            }
        );
        assert!(
            c.log_window >= 1000,
            "sparse L2 wants a wide getLogs window"
        );
        assert!(!c.rpc_urls.is_empty());
        // Aliases resolve to the same chain.
        assert_eq!(lookup("arb").unwrap().chain_id, 42161);
        assert_eq!(lookup("arbitrum").unwrap().chain_id, 42161);
    }

    #[test]
    fn mainnet_uses_depth_finality() {
        let c = lookup("mainnet").unwrap();
        assert_eq!(c.finality, Finality::Depth(64));
        assert_eq!(c.log_window, 20);
    }

    /// #761: onfinality's public URL was a third mainnet default that could not serve a backfill
    /// (no archive, and on 2026-08-23 the doctor probe did not complete). Failover across a corpse
    /// is not failover.
    #[test]
    fn mainnet_does_not_ship_onfinality_or_a_single_live_host() {
        let urls = lookup("mainnet").unwrap().rpc_urls;
        assert!(
            !urls.iter().any(|u| u.contains("onfinality")),
            "onfinality public is not archive and does not complete a doctor probe: {urls:?}"
        );
        assert!(
            urls.len() >= 2,
            "mainnet must keep a spare after pruning: {urls:?}"
        );
        assert!(
            lookup("mainnet").unwrap().topic0_only_getlogs,
            "mainnet factory flip is the ordinary case"
        );
    }

    /// #761: BSC's only keyless default refuses address-less getLogs, which is the factory flip.
    #[test]
    fn bsc_shipped_default_does_not_claim_topic0_only_getlogs() {
        let c = lookup("bsc").unwrap();
        assert!(
            !c.topic0_only_getlogs,
            "bsc-rpc.publicnode.com refuses an empty address list"
        );
        assert_eq!(c.rpc_urls, &["https://bsc-rpc.publicnode.com"]);
    }

    #[test]
    fn base_is_registered_as_op_stack_l2() {
        let c = lookup("base").expect("base in registry");
        assert_eq!(c.chain_id, 8453);
        // OP-stack L2 → same finalized-tag policy as Arbitrum.
        assert!(matches!(c.finality, Finality::FinalizedTag { .. }));
        assert!(!c.rpc_urls.is_empty());
        assert_eq!(lookup("base-mainnet").unwrap().chain_id, 8453);
    }

    /// RFC-0051: the first BFT single-slot-final chain in the registry. The seal boundary is a depth
    /// of exactly eight blocks - 2.4 s, more than twice Monad's three-block execution deferral - not
    /// the `finalized` tag, until a soak shows finalized and executed are coupled under load (RFC-0051
    /// addendum, item 14). The window sits at the narrowest shipped endpoint's documented cap rather
    /// than above it, because Monad's blocks are dense enough that the result cap, not the range cap,
    /// is what a busy contract hits.
    #[test]
    fn monad_is_registered_with_an_execution_margin_at_the_narrowest_cap() {
        let c = lookup("monad").expect("monad in registry");
        assert_eq!(c.chain_id, 143);
        assert_eq!(
            c.finality,
            Finality::Depth(8),
            "an execution margin over k=3 deferred execution; the tag is a soak away, not a sample"
        );
        assert_eq!(
            c.log_window, 100,
            "QuickNode's documented cap - see the entry's measurements before raising it"
        );
        assert!(
            c.topic0_only_getlogs,
            "measured 2026-09-03: an address-less topic0 getLogs is served by all three"
        );
        assert_eq!(lookup("monad-mainnet").unwrap().chain_id, 143);
    }

    /// RFC-0050: an Arbitrum Nitro L2, so it takes Arbitrum One's tag policy, with the fallback
    /// depth sized for 100 ms blocks rather than copied. The RFC's body asks for the `safe` tag;
    /// `Finality` has no such arm, and the carve-out took the tag the code has rather than a
    /// seal-loop change. The window is what `doctor` recommended on the busiest token, under a
    /// 10,000-log result cap the endpoint names in its refusal.
    #[test]
    fn robinhood_is_registered_on_the_nitro_tag_policy_with_a_cadence_sized_fallback() {
        let c = lookup("robinhood").expect("robinhood in registry");
        assert_eq!(c.chain_id, 4663);
        assert_eq!(
            c.finality,
            Finality::FinalizedTag {
                fallback_depth: 15_000
            },
            "finalized runs about 12,000 blocks behind tip at 10 blocks/s; 1,800 is Arbitrum One's \
             number and would fall short of the tag by ten minutes"
        );
        assert_eq!(
            c.log_window, 320,
            "doctor's recommendation on 2026-09-04 against USDG; the result cap is 10,000 logs"
        );
        assert!(
            c.topic0_only_getlogs,
            "measured 2026-09-04: address-less getLogs windows are served"
        );
        assert_eq!(
            c.rpc_urls.len(),
            1,
            "one keyless endpoint passed the RFC-0030 §4 bar; the two others are recorded in the \
             entry's doc comment with why not"
        );
        assert_eq!(lookup("robinhood-chain").unwrap().chain_id, 4663);
    }

    #[test]
    fn unknown_chain_is_none() {
        assert!(lookup("dogechain").is_none());
    }

    #[test]
    fn all_chains_are_probeable_and_lead_with_l1() {
        let all = all();
        // Every registered chain is reachable via `lookup` and carries endpoints to probe.
        assert!(!all.is_empty());
        for c in all {
            assert!(
                lookup(c.name).is_some(),
                "{} must resolve via lookup",
                c.name
            );
            assert!(
                !c.rpc_urls.is_empty(),
                "{} needs endpoints to probe",
                c.name
            );
        }
        // L1 first: mainnet is the most likely home and the least surprising default hit.
        assert_eq!(all[0].name, "mainnet");
    }

    /// RFC-0030 §4: a chain may ship only with **at least two** endpoints that independently clear
    /// the bar, because the round-robin failover in `rpc_urls` is the mitigation for a flaky host -
    /// and a list of one has nothing to fail over to.
    ///
    /// Enforced here rather than remembered, because the failure is silent: pruning a bad endpoint
    /// is exactly when this drops to one, and the person doing it is looking at what they removed
    /// rather than at what is left. Issue #267 pruned three lists in one pass.
    #[test]
    fn every_chain_ships_at_least_two_endpoints() {
        // BSC is the one exception, and it is recorded rather than waved through. No keyless public
        // archive endpoint for it has been found: probed 2026-08-21 with `nuthatch doctor`,
        // `bsc.drpc.org` and `binance.llamarpc.com` both refused the getLogs probe outright and could
        // not be asked about archive depth, and `bsc-dataseed.bnbchain.org` answered but is
        // tip-following only, so a from-deployment backfill cannot use it.
        //
        // This surfaced only when #696 put BSC into `all()`: the rule had been passing because the
        // chain that breaks it was not in the list the rule iterates. A short list hid a real RFC-0030
        // §4 violation.
        //
        // Robinhood Chain is the second, recorded the same way (RFC-0050 addendum, item 8). Seven
        // keyless hosts were tried on 2026-09-04 and one cleared the bar: `robinhood.drpc.org`
        // answers the chain id but has no `eth_blockNumber` (`-32601`), `robinhood-rpc.publicnode.com`
        // wants an archive token even for a 10-block getLogs, `rpc.ankr.com/robinhood` is 403,
        // `1rpc.io/robinhood` is `unknown network`, thirdweb's two hosts say `Invalid chain`,
        // Tenderly's gateway is 404, and the explorer's `eth-rpc` sits behind a Cloudflare challenge.
        // The chain is two months old; re-probe before the entry's next measurement.
        //
        // Each exception announces its own obsolescence. It asserts the chain has **exactly one**
        // endpoint, so the day somebody adds a second this test fails and whoever does it deletes
        // that name rather than leaving a stale carve-out behind.
        const SINGLE_ENDPOINT_EXCEPTIONS: &[&str] = &["bsc", "robinhood"];
        for c in all() {
            if SINGLE_ENDPOINT_EXCEPTIONS.contains(&c.name) {
                assert_eq!(
                    c.rpc_urls.len(),
                    1,
                    "{} now ships {} endpoints - delete the exception in this test, it has served \
                     its purpose",
                    c.name,
                    c.rpc_urls.len()
                );
                continue;
            }
            assert!(
                c.rpc_urls.len() >= 2,
                "{} ships {} endpoint(s); RFC-0030 §4 requires at least two so round-robin has \
                 somewhere to fail over. Measure a replacement before removing the last spare.",
                c.name,
                c.rpc_urls.len()
            );
        }
    }

    /// No endpoint appears twice in one chain's list - a duplicate looks like failover and is not.
    #[test]
    fn a_chains_endpoints_are_distinct() {
        for c in all() {
            let mut seen = std::collections::BTreeSet::new();
            for u in c.rpc_urls {
                assert!(seen.insert(*u), "{} lists {u} twice", c.name);
            }
        }
    }

    // ---- #535: `resolve`/`from_config` - a chain outside the built-in three ----------------------

    /// A one-endpoint fake JSON-RPC server answering only `eth_chainId`. Mirrors `rpc::tests::fake_rpc`
    /// (private to that module) - real HTTP, so `resolve`'s actual RPC round trip is exercised.
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

    #[tokio::test]
    async fn resolve_of_a_known_name_ignores_rpc_and_never_dials_it() {
        // A known chain never needs the network: `resolve` must not even try to dial `--rpc` for it -
        // `127.0.0.1:1` is a port nothing listens on, so a dial here would hang or error.
        let resolved = resolve("mainnet", &["http://127.0.0.1:1".into()])
            .await
            .unwrap();
        assert_eq!(resolved.chain_id, 1);
        assert_eq!(resolved.finality, Finality::Depth(64));
        assert_eq!(resolved.log_window, 20);
    }

    /// Every shipped chain resolves under each of its aliases, with the right id.
    ///
    /// Added with the four chains in the top-25 list (BNB, Polygon, Gnosis, Optimism). A typo in an
    /// alias arm is invisible until somebody types that alias, and then it reads as "nuthatch does
    /// not support this chain" rather than "we spelled it wrong".
    #[test]
    fn every_shipped_chain_resolves_under_all_of_its_aliases() {
        // Also the source of truth for `every_registered_chain_is_probed` below, so a new chain is
        // added here once and both properties are checked from it.
        for (aliases, id) in [
            (&["mainnet", "ethereum", "eth"][..], 1u64),
            (&["arbitrum-one", "arbitrum", "arb", "arb1"][..], 42161),
            (&["base", "base-mainnet", "base-one"][..], 8453),
            (
                &["bsc", "bnb", "binance", "bnb-smart-chain", "bsc-mainnet"][..],
                56,
            ),
            (
                &["polygon", "matic", "polygon-pos", "polygon-mainnet"][..],
                137,
            ),
            (&["gnosis", "xdai", "gnosis-chain"][..], 100),
            (
                &["optimism", "op", "op-mainnet", "optimism-mainnet"][..],
                10,
            ),
            (&["monad", "monad-mainnet", "mon"][..], 143),
            (
                &["robinhood", "robinhood-chain", "robinhood-mainnet", "rh"][..],
                4663,
            ),
        ] {
            for a in aliases {
                let c = lookup(a).unwrap_or_else(|| panic!("alias {a:?} resolves to nothing"));
                assert_eq!(c.chain_id, id, "alias {a:?} points at the wrong chain");
            }
        }
    }

    /// Every chain `lookup` knows must also be **probed** by `init` when `--chain` is omitted (#696).
    ///
    /// `all()` returned three while `lookup` knew seven, so a contract on Optimism, Polygon, BSC or
    /// Gnosis could be indexed by name and never found by auto-detect - and the README said in bold
    /// that omitting `--chain` probes all seven.
    ///
    /// The existing `all_chains_are_probeable_and_lead_with_l1` could not see it: it asserts every
    /// chain in `all()` resolves via `lookup`, which is trivially true of any *subset*. One direction
    /// of a two-list invariant, and the missing direction is the one that rots. Same shape as #353.
    ///
    /// The ids come from the alias table above rather than a fresh literal, so there are two lists in
    /// this file and not three: add a chain, add it there, and this fails until `all()` has it too.
    #[test]
    fn every_registered_chain_is_probed_by_auto_detect() {
        let probed: Vec<u64> = all().iter().map(|c| c.chain_id).collect();
        for id in [1u64, 42161, 8453, 56, 137, 100, 10, 143, 4663] {
            assert!(
                probed.contains(&id),
                "chain {id} resolves via `lookup` but `all()` never probes it, so `init` without \
                 `--chain` cannot find a contract that lives there: probed {probed:?}"
            );
        }
        assert_eq!(
            probed.len(),
            9,
            "a chain was added to `all()` without being added to the alias table above: {probed:?}"
        );
    }

    /// A shipped chain with no endpoint would be worse than an unshipped one: `init` would look
    /// supported and then have nowhere to ask.
    #[test]
    fn no_shipped_chain_has_an_empty_endpoint_list() {
        for name in [
            "mainnet",
            "arbitrum-one",
            "base",
            "bsc",
            "polygon",
            "gnosis",
            "optimism",
        ] {
            let c = lookup(name).unwrap();
            assert!(!c.rpc_urls.is_empty(), "{name} ships with no endpoints");
            assert!(
                c.log_window > 0,
                "{name} has a zero log window, which schedules nothing"
            );
        }
    }

    #[tokio::test]
    async fn resolve_of_an_unknown_name_without_rpc_names_the_remedy() {
        let err = resolve("avalanche", &[]).await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("unknown chain 'avalanche'"), "{msg}");
        assert!(msg.contains("--rpc"), "{msg}");
        for c in all() {
            assert!(
                msg.contains(c.name),
                "{} missing from the remedy: {msg}",
                c.name
            );
        }
    }

    /// The fix for #535: an unregistered chain name is accepted once `--rpc` names where it lives,
    /// and its `chain_id` comes from the endpoint itself rather than a hand-typed flag.
    #[tokio::test]
    async fn resolve_of_an_unknown_name_with_rpc_reads_the_chain_id_live() {
        let (url, _rpc) = fake_chain_id_rpc(43114).await;
        let resolved = resolve("avalanche", std::slice::from_ref(&url))
            .await
            .unwrap();
        assert_eq!(resolved.name, "avalanche");
        assert_eq!(resolved.chain_id, 43114);
        assert_eq!(resolved.rpc_urls, vec![url]);
        // Same fallback policy the indexer already applies to a nest on an unregistered chain
        // (`indexer::DEFAULT_FINALITY`/`DEFAULT_WINDOW`) - a custom chain must not silently get a
        // laxer or stricter default than the runtime it will actually run under.
        assert_eq!(resolved.finality, UNREGISTERED_FINALITY);
        assert_eq!(resolved.log_window, UNREGISTERED_WINDOW);
    }

    #[test]
    fn from_config_of_a_known_chain_uses_the_registry_not_the_saved_id() {
        // A hand-edited `chain_id` that disagrees with the registry is not this function's problem to
        // catch (RPC startup's `verify_chain_ids` is) - it returns the registry's own policy either way.
        let resolved = from_config("base", 999);
        assert_eq!(resolved.chain_id, 8453);
        assert!(matches!(resolved.finality, Finality::FinalizedTag { .. }));
    }

    #[test]
    fn from_config_of_a_custom_chain_carries_the_saved_id_with_no_rpc_round_trip() {
        let resolved = from_config("avalanche", 43114);
        assert_eq!(resolved.name, "avalanche");
        assert_eq!(resolved.chain_id, 43114);
        assert_eq!(resolved.finality, UNREGISTERED_FINALITY);
        assert_eq!(resolved.log_window, UNREGISTERED_WINDOW);
    }
}
