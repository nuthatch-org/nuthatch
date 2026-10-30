//! RFC-0077 S7: a nest narrowed to one user's logs. A host names, per declared event, the values its
//! indexed arguments must take, and the cursor asks the RPC for those logs only: one `eth_getLogs`
//! per value, since an endpoint that serves ten million blocks to a filter with one value per topic
//! serves a hundred thousand to one with a list (arb1.arbitrum.io, measured 2026-10-31).
//!
//! A narrowed nest is not the contract's history: an event no filter names is not fetched at all, and
//! its table stays empty. It is a selection the host made, so it is never hydrated from a mirror.

use crate::nest::NestSpec;
use anyhow::{anyhow, bail, ensure, Result};
use futures::{StreamExt, TryStreamExt};
use nuthatch_core::rpc::{class_of, FailureClass};
use nuthatch_core::source::{Log, LogFilter, Source};
use serde::Deserialize;
use std::collections::BTreeMap;

/// One filter as `nh_open`'s `filters` gives it.
#[derive(Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct FilterSpec {
    /// The contract's alias in `nuthatch.toml`.
    pub contract: String,
    /// The event's name, or its signature where the ABI overloads the name.
    pub event: String,
    /// The first block to ask from, where the event cannot occur before it; never before the
    /// contract's `start_block`.
    pub from: Option<u64>,
    /// Indexed argument by name, and the values it may take: addresses or 32-byte words.
    #[serde(default)]
    pub args: BTreeMap<String, Vec<String>>,
}

/// One `eth_getLogs` filter the cursor asks for every window, from `from`.
#[derive(Clone, Debug)]
pub struct Narrowed {
    pub filter: LogFilter,
    pub from: u64,
}

/// At most this many filters' requests are with the host at once.
const IN_FLIGHT: usize = 8;

/// A throttled or failed request is asked again this many times, resting longer each time.
const RETRIES: u32 = 6;

/// The filters `specs` describe over `spec`'s contracts, refused by name where one names an event
/// the nest does not declare, an argument the event does not index, or a value that is not a topic.
pub fn build(spec: &NestSpec, specs: &[FilterSpec]) -> Result<Vec<Narrowed>> {
    ensure!(
        spec.factory.is_none(),
        "`filters` narrow a nest's own contracts, and a factory's children are found, not named"
    );
    ensure!(
        !specs.is_empty(),
        "`filters` is empty, which would index nothing"
    );
    let tables = spec.registry.tables();
    let mut out = Vec::new();
    for f in specs {
        let start = *spec.starts.get(&f.contract).ok_or_else(|| {
            anyhow!(
                "filter on `{}`: no contract has that alias in nuthatch.toml",
                f.contract
            )
        })?;
        let matching: Vec<_> = tables
            .iter()
            .filter(|d| d.alias == f.contract)
            .filter(|d| {
                if f.event.contains('(') {
                    d.signature == f.event
                } else {
                    d.signature.split('(').next() == Some(f.event.as_str())
                }
            })
            .collect();
        let decoder = match matching.as_slice() {
            [d] => *d,
            [] => bail!(
                "filter on `{}.{}`: the contract declares no such event",
                f.contract,
                f.event
            ),
            _ => bail!(
                "filter on `{}.{}`: the name is overloaded; give the signature, one of {}",
                f.contract,
                f.event,
                matching
                    .iter()
                    .map(|d| d.signature.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        };
        let from = match f.from {
            Some(b) if b < start => bail!(
                "filter on `{}.{}` starts at block {b}, before the contract's start_block {start}",
                f.contract,
                f.event
            ),
            Some(b) => b,
            None => start,
        };
        let indexed: Vec<&str> = decoder
            .columns
            .iter()
            .filter(|c| c.indexed)
            .map(|c| c.name.as_str())
            .collect();
        // One value per position per request, so a list of values multiplies the filters.
        let mut combos: Vec<[Vec<String>; 3]> = vec![Default::default()];
        for (arg, values) in &f.args {
            let at = indexed.iter().position(|n| n == arg).ok_or_else(|| {
                anyhow!(
                    "filter on `{}.{}`: `{arg}` is not one of its indexed arguments ({})",
                    f.contract,
                    f.event,
                    indexed.join(", ")
                )
            })?;
            ensure!(
                at < 3,
                "filter on `{}.{}`: `{arg}` is past the third topic",
                f.contract,
                f.event
            );
            ensure!(
                !values.is_empty(),
                "filter on `{}.{}`: `{arg}` lists no values",
                f.contract,
                f.event
            );
            let words = values
                .iter()
                .map(|v| topic_word(v))
                .collect::<Result<Vec<_>>>()?;
            combos = combos
                .into_iter()
                .flat_map(|c| {
                    words.iter().map(move |w| {
                        let mut c = c.clone();
                        c[at] = vec![w.clone()];
                        c
                    })
                })
                .collect();
        }
        let address = format!("0x{}", hex::encode(decoder.contract));
        let topic0 = format!("0x{}", hex::encode(decoder.topic0));
        for topics in combos {
            let filter = LogFilter::new(
                std::slice::from_ref(&address),
                std::slice::from_ref(&topic0),
            )
            .expect("an address and a topic0")
            .with_topics(topics);
            out.push(Narrowed { filter, from });
        }
    }
    Ok(out)
}

/// An address left-padded to a topic, or a 32-byte word as given, in lower case.
fn topic_word(v: &str) -> Result<String> {
    let hex = v
        .strip_prefix("0x")
        .filter(|h| h.bytes().all(|b| b.is_ascii_hexdigit()))
        .ok_or_else(|| anyhow!("`{v}` is not a 0x-prefixed hex value"))?;
    match hex.len() {
        40 => Ok(format!("0x{:0>64}", hex.to_ascii_lowercase())),
        64 => Ok(format!("0x{}", hex.to_ascii_lowercase())),
        n => bail!(
            "`{v}` is {} hex digits; a topic is an address (40) or a word (64)",
            n
        ),
    }
}

/// The window's logs every filter names, in chain order, each once however many filters name it.
pub async fn fetch(
    filters: &[Narrowed],
    source: &dyn Source,
    from: u64,
    to: u64,
) -> Result<Vec<Log>> {
    let mut logs: Vec<Log> = futures::stream::iter(filters.iter().filter(|n| n.from <= to))
        .map(|n| async move {
            let logs = logs_of(source, &n.filter, from.max(n.from), to).await?;
            // An endpoint that ignored the topics answered more than was asked; only the asked-for
            // logs are this nest's.
            Ok::<_, anyhow::Error>(
                logs.into_iter()
                    .filter(|l| n.filter.matches(l))
                    .collect::<Vec<_>>(),
            )
        })
        .buffer_unordered(IN_FLIGHT)
        .try_concat()
        .await?;
    logs.sort_by_key(|l| (l.block_number, l.log_index));
    logs.dedup_by_key(|l| (l.block_number, l.log_index));
    Ok(logs)
}

/// One filter's logs over `from..=to`. A range the endpoint will not answer whole is halved, and a
/// throttle or a failed request is asked again after a rest, as the node's backfill does. A cursor
/// window fails whole otherwise, and a narrowed nest asks for many filters a window.
fn logs_of<'a>(
    source: &'a dyn Source,
    filter: &'a LogFilter,
    from: u64,
    to: u64,
) -> futures::future::BoxFuture<'a, Result<Vec<Log>>> {
    Box::pin(async move {
        let mut tries = 0;
        loop {
            let err = match source.logs(filter, from, to).await {
                Ok(logs) => return Ok(logs),
                Err(e) => e,
            };
            match class_of(&err) {
                Some(FailureClass::Narrowable { .. }) if from < to => {
                    let mid = from + (to - from) / 2;
                    let mut logs = logs_of(source, filter, from, mid).await?;
                    logs.extend(logs_of(source, filter, mid + 1, to).await?);
                    return Ok(logs);
                }
                Some(FailureClass::RateLimited { .. } | FailureClass::Transient)
                    if tries < RETRIES =>
                {
                    tries += 1;
                    nuthatch_core::time::sleep(std::time::Duration::from_millis(250 << tries))
                        .await;
                }
                _ => return Err(err),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::derived::NestFiles;

    const ABI: &str = r#"[
      {"type":"event","name":"Moved","anonymous":false,"inputs":[
        {"name":"from","type":"address","indexed":true},
        {"name":"to","type":"address","indexed":true},
        {"name":"amount","type":"uint256","indexed":false}]},
      {"type":"event","name":"Noted","anonymous":false,"inputs":[
        {"name":"who","type":"address","indexed":true}]},
      {"type":"event","name":"Noted","anonymous":false,"inputs":[
        {"name":"who","type":"address","indexed":true},
        {"name":"what","type":"uint256","indexed":false}]}]"#;

    fn spec() -> NestSpec {
        let mut files = NestFiles::new();
        files.insert(
            "nuthatch.toml".into(),
            r#"[nest]
name = "narrow"
chain = "arbitrum-one"
chain_id = 42161
rpc_urls = []
schema_version = 1

[[contracts]]
alias = "token"
address = "0x00000000000000000000000000000000000000aa"
start_block = 100
abi = "abis/token.json"
"#
            .into(),
        );
        files.insert("abis/token.json".into(), ABI.into());
        NestSpec::from_files(&files).unwrap()
    }

    fn one(contract: &str, event: &str, from: Option<u64>, args: &[(&str, &[&str])]) -> FilterSpec {
        FilterSpec {
            contract: contract.into(),
            event: event.into(),
            from,
            args: args
                .iter()
                .map(|(k, v)| (k.to_string(), v.iter().map(|s| s.to_string()).collect()))
                .collect(),
        }
    }

    const A: &str = "0x00000000000000000000000000000000000000a1";
    const B: &str = "0x00000000000000000000000000000000000000B2";

    #[test]
    fn each_value_is_its_own_request_and_addresses_become_topics() {
        let n = build(&spec(), &[one("token", "Moved", None, &[("to", &[A, B])])]).unwrap();
        assert_eq!(n.len(), 2);
        assert_eq!(n[0].from, 100);
        assert_eq!(
            n[1].filter.topics()[1],
            vec![format!("0x{:0>64}", "b2")],
            "`to` is the second indexed argument, so topic 2, padded and lower-cased"
        );
        assert!(n[0].filter.topics()[0].is_empty());
        let both = build(
            &spec(),
            &[one(
                "token",
                "Moved",
                Some(150),
                &[("from", &[A, B]), ("to", &[A, B])],
            )],
        )
        .unwrap();
        assert_eq!(both.len(), 4);
        assert!(both.iter().all(|n| n.from == 150));
    }

    #[test]
    fn what_a_filter_cannot_mean_is_refused_by_name() {
        let err = |f: FilterSpec| format!("{:#}", build(&spec(), &[f]).unwrap_err());
        assert!(err(one("tok", "Moved", None, &[])).contains("no contract has that alias"));
        assert!(err(one("token", "Gone", None, &[])).contains("declares no such event"));
        assert!(err(one("token", "Noted", None, &[])).contains("overloaded"));
        assert!(err(one("token", "Moved", None, &[("amount", &[A])]))
            .contains("not one of its indexed"));
        assert!(
            err(one("token", "Moved", Some(99), &[])).contains("before the contract's start_block")
        );
        assert!(err(one("token", "Moved", None, &[("to", &["0x12"])])).contains("2 hex digits"));
        assert!(err(one("token", "Moved", None, &[("to", &[])])).contains("lists no values"));
        assert_eq!(
            build(
                &spec(),
                &[one("token", "Noted(address)", None, &[("who", &[A])])]
            )
            .unwrap()
            .len(),
            1,
            "a signature picks one of an overloaded name"
        );
    }
}
