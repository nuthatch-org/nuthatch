use anyhow::Result;
use nuthatch_embed::address_discovery::Rpc;
use nuthatch_embed::address_history::AddressHistory;
use nuthatch_embed::host::{block_on, Endpoint, HostRpc, Reply, Request};
use nuthatch_embed::memstore::Bytes;
use nuthatch_embed::store::Store;
use serde_json::{json, Value};

const ALICE: &str = "0x00000000000000000000000000000000000a11ce";

fn open(bytes: &Bytes, seed: &[String]) -> AddressHistory {
    let db = redb::Builder::new()
        .create_with_backend(bytes.clone())
        .unwrap();
    AddressHistory::open(Store::from_database(db), 1, seed).unwrap()
}

#[test]
fn a_store_handed_out_and_back_keeps_what_it_held() {
    let bytes = Bytes::default();
    let h = open(&bytes, &[ALICE.into()]);
    h.finalize_at(100, [7; 32]).unwrap();
    let saved = bytes.snapshot();
    drop(h);

    let again = open(&Bytes::from_vec(saved), &[]);
    assert_eq!(again.watched().unwrap(), vec![ALICE.to_string()]);
    assert_eq!(again.finals().unwrap(), vec![(100, [7; 32])]);
}

#[test]
fn concurrent_calls_reach_the_host_as_one_round() {
    let mut rounds: Vec<usize> = Vec::new();
    let mut transport = |reqs: &[Request]| -> Result<Vec<Reply>> {
        rounds.push(reqs.len());
        Ok(reqs
            .iter()
            .map(|r| {
                let n = r.params[0].as_u64().unwrap();
                let body = json!({"jsonrpc": "2.0", "id": 1, "result": n * 2}).to_string();
                Reply::Body { body }
            })
            .collect())
    };
    let rpc = HostRpc(Endpoint::Main);
    let got = block_on(
        async {
            let calls = (0..5u64).map(|n| rpc.call("double", json!([n])));
            futures::future::try_join_all(calls).await
        },
        &mut transport,
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        got,
        (0..5u64).map(|n| Value::from(n * 2)).collect::<Vec<_>>()
    );
    assert_eq!(rounds, vec![5]);
}

#[test]
fn a_json_rpc_error_keeps_the_providers_words() {
    let mut transport = |_: &[Request]| -> Result<Vec<Reply>> {
        Ok(vec![Reply::Body {
            body: r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"Block range is too large; limited to 100 blocks"}}"#.into(),
        }])
    };
    let err = block_on(
        HostRpc(Endpoint::Trace).call("trace_filter", json!([{}])),
        &mut transport,
    )
    .unwrap()
    .unwrap_err()
    .to_string();
    assert!(err.contains("limited to 100 blocks"), "{err}");
}
