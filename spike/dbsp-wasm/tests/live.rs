//! RFC-0075 S3 in a browser: the recorded young contract followed over `fetch` from a dedicated
//! worker, held to the node's pinned segments and balances, with the module's memory and the RPC
//! bytes printed. Needs fixture-rpc serving the recording, named at build time:
//! `TAB_PARITY_RPC=http://127.0.0.1:8947/ CHROMEDRIVER=… cargo test --target wasm32-unknown-unknown --test live`.
#![cfg(target_arch = "wasm32")]

use dbsp_wasm_spike::live::LiveNest;
use wasm_bindgen::JsCast;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_dedicated_worker);

const EXPECTED: &str = include_str!("../../../tests/fixtures/tab-parity/expected.json");
const CONTRACT: &str = "0x0c26fe5e579c3b007ba16d921ba8204a47c05f1a";
const START: u64 = 26_239_807;

#[wasm_bindgen_test]
async fn the_tab_seals_and_serves_what_the_node_does() {
    let rpc = option_env!("TAB_PARITY_RPC").unwrap_or("http://127.0.0.1:8947/");
    js_sys::Function::new_no_args(
        "const f = globalThis.fetch; globalThis.__rpc = {posts: 0, sent: 0, received: 0};
         globalThis.fetch = async (u, i) => { const r = await f(u, i); const s = globalThis.__rpc;
           s.posts++; s.sent += String(i?.body ?? '').length;
           s.received += Number(r.headers.get('content-length') ?? 0); return r; };",
    )
    .call0(&wasm_bindgen::JsValue::NULL)
    .unwrap();
    let t0 = js_sys::Date::now();
    let nest = LiveNest::new(rpc.into(), CONTRACT.into(), START).unwrap();
    let got: serde_json::Value = serde_json::from_str(&nest.follow().await.unwrap()).unwrap();
    let ms = js_sys::Date::now() - t0;
    let memory = wasm_bindgen::memory()
        .unchecked_into::<js_sys::WebAssembly::Memory>()
        .buffer()
        .unchecked_into::<js_sys::ArrayBuffer>()
        .byte_length();
    let rpc_stats = js_sys::Reflect::get(&js_sys::global(), &"__rpc".into()).unwrap();
    console_log!(
        "live: {ms:.0} ms; wasm memory {:.1} MiB; rpc {}; segment bytes {}; hot rows {}",
        memory as f64 / (1 << 20) as f64,
        js_sys::JSON::stringify(&rpc_stats).unwrap(),
        got["segment_bytes"],
        got["hot_rows"],
    );
    let expected: serde_json::Value = serde_json::from_str(EXPECTED).unwrap();
    for k in expected.as_object().unwrap().keys() {
        assert_eq!(got[k], expected[k], "{k} differs from the node's");
    }
    for s in got["segments"]
        .as_object()
        .unwrap()
        .values()
        .flat_map(|v| v.as_array().unwrap())
    {
        assert_eq!(s["hash"], s["bytes_hash"]);
    }
}
