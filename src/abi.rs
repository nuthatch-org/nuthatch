//! ABI resolution: Sourcify first, then keyless Blockscout where it is available, then Etherscan v2
//! as a last-resort keyed fallback.
//! Correctness-critical decoding lives elsewhere; this is just acquisition.

use anyhow::{anyhow, bail, Context, Result};
use nuthatch_scaffold::resolve::{parse_sourcify, sourcify_contract_name};
use serde_json::Value;

/// An ABI plus which resolver produced it, so a caller with a presentation layer (`init`/`add`'s
/// pretty printer) can say which one won without a `tracing` line crashing through its output -
/// see #675. `fallback_reason` is set only when Sourcify was tried and missed before Etherscan won.
pub struct Resolved {
    pub abi: Value,
    pub via: &'static str,
    pub fallback_reason: Option<String>,
    /// Sourcify v2 `name` when present. Not a decode input; `init`/`add` use it as a default alias.
    pub contract_name: Option<String>,
}

/// Resolve a contract ABI without making an API token the normal path. Sourcify is the primary
/// source; Blockscout is the second, keyless Etherscan-compatible source on the chains where it
/// operates; Etherscan is retained for chains Blockscout does not cover and as a final fallback.
/// `explorer` is an operator-supplied Blockscout root for a chain we have not verified (`--explorer`,
/// #1322). It is tried after Sourcify and *instead of* the built-in list, which by definition does
/// not cover the chain if the operator had to name one. Used for this invocation only and never
/// written to the nest, because an ABI source is an access path and must not enter the content
/// address - the same rule `--ipfs` and `--rpc` follow.
pub async fn resolve(chain_id: u64, address: &str, explorer: Option<&str>) -> Result<Resolved> {
    let sourcify_miss = match sourcify(chain_id, address).await {
        Ok((abi, name)) => {
            return Ok(Resolved {
                abi,
                via: "Sourcify",
                fallback_reason: None,
                contract_name: name,
            })
        }
        Err(e) => format!("Sourcify miss: {e:#}"),
    };
    after_sourcify(address, explorer, sourcify_miss, |miss| {
        built_in(chain_id, address, miss)
    })
    .await
}

/// Everything after a Sourcify miss. **A named explorer's answer stands**: the built-in roots and
/// Etherscan are reached only when the operator named none. "This contract is not verified" is an
/// answer, and a far more useful one than a demand for an Etherscan key for a chain Etherscan does
/// not index (#1322). Falling through to the built-in root on a chain that also has one would be
/// worse than that: it would hand back a different source's ABI and silently ignore the explorer
/// the operator asked for (Jules on #1388). `built_in` is a parameter so that ordering is testable
/// without the network.
async fn after_sourcify<F, Fut>(
    address: &str,
    explorer: Option<&str>,
    sourcify_miss: String,
    built_in: F,
) -> Result<Resolved>
where
    F: FnOnce(String) -> Fut,
    Fut: std::future::Future<Output = Result<Resolved>>,
{
    let Some(root) = explorer else {
        return built_in(sourcify_miss).await;
    };
    match blockscout_v2(root, address).await {
        Ok((abi, name)) => Ok(Resolved {
            abi,
            via: "Blockscout (--explorer)",
            fallback_reason: Some(sourcify_miss),
            contract_name: name,
        }),
        Err(e) => bail!(
            "Sourcify had no verified ABI, and the explorer you named could not supply one: \
             {e:#}\n  Pass --abi path/to.json if you have the ABI, or check the address is \
             verified on that instance."
        ),
    }
}

/// The built-in Blockscout root for `chain_id`, then Etherscan.
async fn built_in(chain_id: u64, address: &str, sourcify_miss: String) -> Result<Resolved> {
    match blockscout(chain_id, address).await {
        Ok(abi) => Ok(Resolved {
            abi,
            via: "Blockscout",
            fallback_reason: Some(sourcify_miss),
            contract_name: None,
        }),
        Err(blockscout_err) => {
            let abi = etherscan(chain_id, address).await?;
            Ok(Resolved {
                abi,
                via: "Etherscan",
                fallback_reason: Some(format!(
                    "{sourcify_miss}; Blockscout miss: {blockscout_err:#}"
                )),
                contract_name: None,
            })
        }
    }
}

/// An operator-named Blockscout instance, over the **v2** API rather than the Etherscan-compatible
/// v1 shim (#1322).
///
/// v2 is the right surface here because it says `is_verified` out loud. The v1 shim answers an
/// unverified contract with a generic error, which is indistinguishable from the instance being
/// unreachable or the root being wrong - and on Arc Testnet the contracts *were* simply unverified,
/// which is a fact an operator can act on and a demand for an `ETHERSCAN_API_KEY` is not.
async fn blockscout_v2(root: &str, address: &str) -> Result<(Value, Option<String>)> {
    let base = root.trim_end_matches('/');
    let url = format!("{base}/api/v2/smart-contracts/{address}");
    let resp = reqwest::get(&url)
        .await
        .with_context(|| format!("could not reach the explorer at {base}"))?;
    // A 404 is an answer about the *contract*, not about the root, and conflating the two sends the
    // operator to re-check a URL that was right. Measured 2026-09-14: `testnet.arcscan.app` answers
    // 404 for an address with no verified source, and lists **zero** verified contracts chain-wide -
    // which is the finding #1322 was actually chasing.
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        bail!("{base} has no verified source for {address}");
    }
    if !resp.status().is_success() {
        bail!(
            "{base} answered {} for {address} - check the explorer root, which should be the site \
             origin such as https://testnet.arcscan.app",
            resp.status()
        );
    }
    let body: Value = resp.json().await.with_context(|| {
        format!("{base} did not answer with JSON; is it a Blockscout instance?")
    })?;
    // **Verified must be asserted, not merely not-denied** (Jules on #1388). Rejecting only an
    // explicit `false` accepts a body with the field missing, or carrying the string `"false"`, or
    // any other JSON that happens to hold an array under `abi` - which is exactly how a wrong
    // endpoint gets its ABI vendored into a nest while this function claims to be using
    // Blockscout's verification signal. Requiring `true` is the same allowlist-not-denylist move
    // the CORS validator needed three rounds to learn.
    //
    // Safe to require, measured 2026-09-14 rather than assumed: `eth.`, `base.` and
    // `gnosis.blockscout.com` all answer a verified contract with a real JSON boolean `true`, and a
    // v2 instance answers 404 for an unverified one (`testnet.arcscan.app`) rather than omitting
    // the field. Checked before `abi`, so an unverified contract reports as unverified rather than
    // as a parse problem.
    if body.get("is_verified").and_then(Value::as_bool) != Some(true) {
        bail!("{address} is not verified on {base}");
    }
    let abi = body
        .get("abi")
        .filter(|v| v.is_array())
        .cloned()
        .ok_or_else(|| anyhow!("{base} returned no ABI for {address}"))?;
    let name = body.get("name").and_then(Value::as_str).map(str::to_string);
    Ok((abi, name))
}

async fn sourcify(chain_id: u64, address: &str) -> Result<(Value, Option<String>)> {
    // Sourcify server API v2. The legacy /server/files endpoint is retired.
    //
    // `compilation`, not `name` (#1138). The contract's identifier used to be a top-level field and
    // is now `compilation.name`; asking for `name` gets HTTP 400 `Field selector name is not a valid
    // field`, on every chain, which silently demoted this whole path to "Blockscout where wired,
    // else an Etherscan key" for however long it stood.
    let url = format!(
        "https://sourcify.dev/server/v2/contract/{chain_id}/{address}?fields=abi,compilation"
    );
    let resp = reqwest::get(&url)
        .await
        .context("Sourcify request failed")?;
    if !resp.status().is_success() {
        bail!("Sourcify returned HTTP {}", resp.status());
    }
    let body: Value = resp
        .json()
        .await
        .context("Sourcify response was not JSON")?;
    Ok((parse_sourcify(&body)?, sourcify_contract_name(&body)))
}

/// Keyless Blockscout API roots we have independently verified. Do not guess a host for a chain:
/// an invented fallback is merely an outage with a more misleading error message. More instances
/// can be added once an ABI response has been measured against the actual chain.
fn blockscout_api(chain_id: u64) -> Option<&'static str> {
    match chain_id {
        1 => Some("https://eth.blockscout.com/api"),
        8453 => Some("https://base.blockscout.com/api"),
        100 => Some("https://gnosis.blockscout.com/api"),
        _ => None,
    }
}

async fn blockscout(chain_id: u64, address: &str) -> Result<Value> {
    let base = blockscout_api(chain_id).ok_or_else(|| {
        anyhow!("no keyless Blockscout ABI endpoint is configured for chain {chain_id}")
    })?;
    let url = format!("{base}?module=contract&action=getabi&address={address}");
    let body: Value = reqwest::get(&url)
        .await
        .context("Blockscout request failed")?
        .json()
        .await
        .context("Blockscout response was not JSON")?;
    parse_etherscan(&body).context("Blockscout could not return an ABI")
}

/// Why `init` cannot continue without `ETHERSCAN_API_KEY`. The key is the last resort, never the
/// only option: `--abi` is always available, and Blockscout is named only on chains where we have
/// measured an instance. Inventing one for BSC (#762) would be the same class of lie as a $93
/// formula that omits the rest of the bill.
fn missing_etherscan_key(chain_id: u64) -> String {
    match blockscout_api(chain_id) {
        Some(_) => "Sourcify and Blockscout had no verified ABI, and ETHERSCAN_API_KEY is not set. \
                    Set it, pass --abi path/to.json, or use a Sourcify-verified contract."
            .into(),
        None => format!(
            "Sourcify had no verified ABI, chain {chain_id} has no keyless Blockscout ABI endpoint, \
             and ETHERSCAN_API_KEY is not set. Set it, pass --abi path/to.json, or use a \
             Sourcify-verified contract."
        ),
    }
}

async fn etherscan(chain_id: u64, address: &str) -> Result<Value> {
    let key = std::env::var("ETHERSCAN_API_KEY")
        .map_err(|_| anyhow!("{}", missing_etherscan_key(chain_id)))?;
    let url = format!(
        "https://api.etherscan.io/v2/api?chainid={chain_id}&module=contract&action=getabi&address={address}&apikey={key}"
    );
    let body: Value = reqwest::get(&url)
        .await
        .context("Etherscan request failed")?
        .json()
        .await
        .context("Etherscan response was not JSON")?;
    parse_etherscan(&body)
}

/// The ABI out of an Etherscan v2 response body - split from the request so it is testable against
/// fixtures. Etherscan signals failure *in a 200 body* (`status: "0"`, with the reason in `result`),
/// so this check is the only thing standing between a rate-limit notice and a "parsed" ABI - and the
/// ABI itself arrives as a JSON string that must be parsed a second time.
fn parse_etherscan(body: &Value) -> Result<Value> {
    if body.get("status").and_then(Value::as_str) != Some("1") {
        let msg = body
            .get("result")
            .and_then(Value::as_str)
            .unwrap_or("unknown error");
        bail!("Etherscan could not return an ABI: {msg}");
    }
    let result = body
        .get("result")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("Etherscan result missing"))?;
    serde_json::from_str(result).context("Etherscan ABI was not valid JSON")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const ABI: &str = r#"[{"type":"event","name":"Transfer","inputs":[]}]"#;

    #[test]
    fn sourcify_name_is_the_alias_hint_and_empty_is_absent() {
        // Where v2 keeps it today (#1138): measured 2026-09-03, mainnet USDC answers
        // `{"compilation": {"name": "FiatTokenProxy", "fullyQualifiedName": ...}, "abi": [...]}`.
        assert_eq!(
            sourcify_contract_name(
                &json!({"compilation": {"name": "FiatTokenProxy", "language": "Solidity"}, "abi": []})
            )
            .as_deref(),
            Some("FiatTokenProxy")
        );
        // Where it used to be, still accepted.
        assert_eq!(
            sourcify_contract_name(&json!({"name": "DelegationManager", "abi": []})).as_deref(),
            Some("DelegationManager")
        );
        assert_eq!(
            sourcify_contract_name(&json!({"compilation": {"name": ""}, "abi": []})),
            None
        );
        // An unusable `compilation.name` - empty, or not a string - must not shadow a legacy
        // top-level name that is usable.
        assert_eq!(
            sourcify_contract_name(
                &json!({"compilation": {"name": ""}, "name": "LegacyName", "abi": []})
            )
            .as_deref(),
            Some("LegacyName")
        );
        assert_eq!(
            sourcify_contract_name(
                &json!({"compilation": {"name": 7}, "name": "LegacyName", "abi": []})
            )
            .as_deref(),
            Some("LegacyName")
        );
        assert_eq!(
            sourcify_contract_name(&json!({"name": "", "abi": []})),
            None
        );
        assert_eq!(sourcify_contract_name(&json!({"abi": []})), None);
    }

    /// The request must not ask for a selector Sourcify refuses (#1138). Pinned as a string test
    /// because the network half has no other test, and `fields=abi,name` was accepted when #774
    /// wrote it and is HTTP 400 now.
    #[test]
    fn sourcify_request_asks_for_compilation_not_name() {
        // The request line only, not the whole file: this test's own strings would otherwise match.
        let request = include_str!("abi.rs")
            .lines()
            .find(|l| l.contains("sourcify.dev/server/v2/contract/"))
            .expect("the v2 request URL line");
        assert!(
            request.contains("?fields=abi,compilation\""),
            "the Sourcify v2 request must select `compilation`, where the contract name lives: {request}"
        );
        assert!(
            !request.contains(",name"),
            "`name` is not a valid v2 field selector any more and gets HTTP 400: {request}"
        );
    }

    #[test]
    fn sourcify_success_error_and_malformed() {
        // Success: the `abi` field is an array and comes back verbatim.
        let body =
            json!({ "abi": [{"type": "event", "name": "Transfer"}], "match": "exact_match" });
        let abi = parse_sourcify(&body).unwrap();
        assert!(abi.is_array());
        assert_eq!(abi[0]["name"], "Transfer");

        // Unverified contract: v2 answers 200 with no `abi` field at all.
        let err = parse_sourcify(&json!({ "match": null })).unwrap_err();
        assert!(err.to_string().contains("no ABI"), "{err}");

        // Malformed: `abi` present but not an array. Accepting this would hand a non-ABI to the
        // decoder, so the `is_array` filter is load-bearing rather than decorative.
        for bad in [
            json!({"abi": "not-an-array"}),
            json!({"abi": {}}),
            json!({"abi": null}),
        ] {
            assert!(
                parse_sourcify(&bad).is_err(),
                "a non-array `abi` must be refused: {bad}"
            );
        }
    }

    #[test]
    fn etherscan_success_error_and_malformed() {
        // Success: `status: "1"`, ABI as a JSON *string* needing a second parse.
        let abi = parse_etherscan(&json!({"status": "1", "result": ABI})).unwrap();
        assert!(abi.is_array());
        assert_eq!(abi[0]["name"], "Transfer");

        // The failure mode that matters: Etherscan-compatible APIs report errors in a 200 body. Without the status
        // check, a rate-limit notice would be parsed as if it were an ABI - and the message must be
        // surfaced, because "rate limited" and "not verified" need different operator responses.
        let err = parse_etherscan(&json!({
            "status": "0",
            "result": "Max rate limit reached"
        }))
        .unwrap_err();
        assert!(err.to_string().contains("Max rate limit reached"), "{err}");

        let err =
            parse_etherscan(&json!({"status": "0", "result": "Contract source code not verified"}))
                .unwrap_err();
        assert!(err.to_string().contains("not verified"), "{err}");

        // A failure body with no usable reason still fails, with a placeholder rather than a panic.
        assert!(parse_etherscan(&json!({"status": "0"})).is_err());

        // Malformed successes: result missing, or not parseable as JSON.
        assert!(parse_etherscan(&json!({"status": "1"})).is_err());
        let err = parse_etherscan(&json!({"status": "1", "result": "{not json"})).unwrap_err();
        assert!(err.to_string().contains("not valid JSON"), "{err}");
    }

    #[test]
    fn keyless_blockscout_is_used_only_where_its_instance_is_verified() {
        assert_eq!(blockscout_api(1), Some("https://eth.blockscout.com/api"));
        assert_eq!(
            blockscout_api(8453),
            Some("https://base.blockscout.com/api")
        );
        assert_eq!(
            blockscout_api(100),
            Some("https://gnosis.blockscout.com/api")
        );
        assert_eq!(blockscout_api(56), None, "do not invent a BSC fallback");
    }

    // ---------------------------------------------------------------------------------------
    // `--explorer` (#1322): an operator-named Blockscout for a chain we ship no root for.
    // ---------------------------------------------------------------------------------------

    /// A Blockscout v2 instance answering `/api/v2/smart-contracts/{address}` with `answer`.
    async fn fake_explorer(answer: serde_json::Value) -> (String, tokio::task::JoinHandle<()>) {
        use axum::{extract::State, routing::get, Json, Router};
        async fn handler(State(a): State<std::sync::Arc<Value>>) -> Json<Value> {
            Json((*a).clone())
        }
        let app = Router::new()
            .route("/api/v2/smart-contracts/{addr}", get(handler))
            .with_state(std::sync::Arc::new(answer));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{addr}"), handle)
    }

    #[tokio::test]
    async fn a_named_explorer_supplies_a_verified_abi() {
        let abi: Value = serde_json::from_str(ABI).unwrap();
        let (root, h) =
            fake_explorer(json!({"is_verified": true, "abi": abi, "name": "Token"})).await;
        let got = blockscout_v2(&root, "0xabc").await.unwrap();
        h.abort();
        assert_eq!(got.0, serde_json::from_str::<Value>(ABI).unwrap());
        assert_eq!(got.1.as_deref(), Some("Token"));
    }

    /// A named explorer's refusal stands even where a built-in root would have answered (Jules on
    /// #1388). Falling through would return the built-in ABI and quietly ignore the explorer the
    /// operator asked for, so the built-in path must not even be asked.
    #[tokio::test]
    async fn a_named_explorers_refusal_is_not_overridden_by_the_built_in_root() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let abi: Value = serde_json::from_str(ABI).unwrap();
        let (root, h) = fake_explorer(json!({"is_verified": false, "abi": abi})).await;
        let asked = AtomicBool::new(false);
        let got = after_sourcify("0xabc", Some(&root), "Sourcify miss: test".into(), |miss| {
            asked.store(true, Ordering::SeqCst);
            let abi = abi.clone();
            async move {
                Ok(Resolved {
                    abi,
                    via: "Blockscout",
                    fallback_reason: Some(miss),
                    contract_name: None,
                })
            }
        })
        .await;
        h.abort();
        let err = match got {
            Ok(r) => panic!("{} answered in place of the named explorer", r.via),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("not verified"), "{err}");
        assert!(
            !asked.load(Ordering::SeqCst),
            "the built-in root was asked after the named explorer answered"
        );

        // And the seam is live: with no explorer named, the built-in path is what answers.
        let got = after_sourcify(
            "0xabc",
            None,
            "Sourcify miss: test".into(),
            |miss| async move {
                Ok(Resolved {
                    abi: Value::Null,
                    via: "Blockscout",
                    fallback_reason: Some(miss),
                    contract_name: None,
                })
            },
        )
        .await;
        assert_eq!(got.map(|r| r.via).ok(), Some("Blockscout"));
    }

    /// **The finding the issue is actually about.** On Arc Testnet the contracts turned out to be
    /// unverified, and the resolver refused before finding that out - so the operator was told to
    /// set `ETHERSCAN_API_KEY` for a chain Etherscan does not index. "Not verified" is an answer,
    /// and an actionable one; a demand for a key that would not have helped is not.
    #[tokio::test]
    async fn an_unverified_contract_is_reported_as_unverified() {
        let abi: Value = serde_json::from_str(ABI).unwrap();
        // Every shape that is not an explicit boolean `true`, each one carrying a perfectly good
        // ABI array - which is the case that matters, because a body with no ABI would be refused
        // by the next check anyway and would prove nothing about this one (Jules on #1388).
        for body in [
            json!({"is_verified": false, "abi": null}),
            json!({"is_verified": false, "abi": abi}),
            json!({"abi": abi}),
            json!({"is_verified": "true", "abi": abi}),
            json!({"is_verified": 1, "abi": abi}),
            json!({"is_verified": null, "abi": abi}),
        ] {
            let (root, h) = fake_explorer(body.clone()).await;
            let err = blockscout_v2(&root, "0xabc")
                .await
                .expect_err("a body that does not assert verification has no ABI to give")
                .to_string();
            h.abort();
            assert!(
                err.contains("not verified"),
                "{body} was reported as: {err}"
            );
            assert!(
                !err.contains("ETHERSCAN"),
                "and must not send the operator after a key that would not help: {err}"
            );
        }
    }

    /// A body that says verified but carries no usable ABI is refused rather than passed on.
    ///
    /// Two shapes reach this, and neither is exotic: an instance that omits `is_verified` entirely
    /// (so the check above says nothing) with a null `abi`, and Etherscan's habit of returning the
    /// ABI as a *string* rather than an array. Letting either through would vendor a non-ABI into
    /// the nest and fail much later, at decode, where the cause is no longer visible.
    #[tokio::test]
    async fn a_verified_answer_with_no_usable_abi_is_refused() {
        // All explicitly verified: the point is a body that *asserts* verification and still has
        // no usable ABI. Bodies that do not assert it are the other test's business now.
        for body in [
            json!({"is_verified": true, "abi": null}),
            json!({"is_verified": true, "abi": "[{\"type\":\"event\"}]"}),
            json!({"is_verified": true}),
            json!({"is_verified": true, "abi": {}}),
        ] {
            let (root, h) = fake_explorer(body.clone()).await;
            let got = blockscout_v2(&root, "0xabc").await;
            h.abort();
            let err = got.expect_err(&format!("{body} is not an ABI"));
            assert!(
                err.to_string().contains("no ABI"),
                "{body} was refused as: {err}"
            );
        }
    }

    /// A 404 is a fact about the contract, not about the root the operator typed. Telling them to
    /// check a URL that was correct is the same unhelpfulness as the `ETHERSCAN_API_KEY` demand,
    /// one level down.
    #[tokio::test]
    async fn a_missing_contract_is_not_reported_as_a_bad_root() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let h = tokio::spawn(async move {
            let app = axum::Router::new()
                .fallback(|| async { (axum::http::StatusCode::NOT_FOUND, "not found") });
            let _ = axum::serve(listener, app).await;
        });
        let err = blockscout_v2(&format!("http://{addr}"), "0xabc")
            .await
            .expect_err("a 404 has no ABI in it");
        h.abort();
        let msg = err.to_string();
        assert!(msg.contains("no verified source"), "{msg}");
        assert!(
            !msg.contains("check the explorer root"),
            "a missing contract must not be blamed on the root: {msg}"
        );
    }

    /// A trailing slash on the root is the obvious way to type it, and must not produce a double
    /// slash that 404s - which would read as "the instance is wrong" rather than "you typed a /".
    #[tokio::test]
    async fn a_trailing_slash_on_the_root_is_tolerated() {
        let abi: Value = serde_json::from_str(ABI).unwrap();
        let (root, h) = fake_explorer(json!({"is_verified": true, "abi": abi})).await;
        let got = blockscout_v2(&format!("{root}/"), "0xabc").await;
        h.abort();
        assert!(got.is_ok(), "a trailing slash broke the URL: {got:?}");
    }

    /// An instance that is reachable but is not Blockscout - a plain website, a proxy error page -
    /// must say so, because the operator's next move is to check the root they typed.
    #[tokio::test]
    async fn a_root_that_is_not_a_blockscout_says_so() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let h = tokio::spawn(async move {
            let app = axum::Router::new().fallback(|| async { "<html>hello</html>" });
            let _ = axum::serve(listener, app).await;
        });
        let err = blockscout_v2(&format!("http://{addr}"), "0xabc")
            .await
            .expect_err("HTML is not an ABI");
        h.abort();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("Blockscout instance") || msg.contains("JSON"),
            "unhelpful for a wrong root: {msg}"
        );
    }

    #[test]
    fn missing_etherscan_key_does_not_claim_a_blockscout_that_does_not_exist() {
        let bsc = missing_etherscan_key(56);
        assert!(
            bsc.contains("chain 56 has no keyless Blockscout"),
            "BSC must not be told to try a host we do not ship: {bsc}"
        );
        assert!(bsc.contains("--abi"), "{bsc}");
        let mainnet = missing_etherscan_key(1);
        assert!(
            mainnet.contains("Sourcify and Blockscout"),
            "mainnet already tried Blockscout: {mainnet}"
        );
        assert!(!mainnet.contains("has no keyless Blockscout"), "{mainnet}");
    }
}
