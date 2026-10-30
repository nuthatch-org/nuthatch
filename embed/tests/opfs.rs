//! RFC-0075 S2, the browser half of the gate: the hot store on OPFS, in a dedicated worker. Node has
//! no OPFS, so this runs under `wasm-bindgen-test-runner` in headless Chrome:
//! `CHROMEDRIVER=… cargo test --target wasm32-unknown-unknown --test opfs`.
#![cfg(target_arch = "wasm32")]

use nuthatch_core::opfs::{self, OpenError, OpfsBackend};
use nuthatch_core::redb_store::Store;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::*;
use web_sys::{FileSystemDirectoryHandle, FileSystemRemoveOptions};

wasm_bindgen_test_configure!(run_in_dedicated_worker);

const FILE: &str = "nuthatch.redb";

async fn fresh(nid: &str) -> FileSystemDirectoryHandle {
    let root = opfs::root().await.unwrap();
    let opts = FileSystemRemoveOptions::new();
    opts.set_recursive(true);
    let _ = JsFuture::from(root.remove_entry_with_options(nid, &opts)).await;
    opfs::directory(&root, nid).await.unwrap()
}

async fn store(dir: &FileSystemDirectoryHandle) -> Result<Store, OpenError> {
    let backend = OpfsBackend::open(dir, FILE).await?;
    let db = redb::Builder::new().create_with_backend(backend).unwrap();
    Ok(Store::from_db(db, None).unwrap())
}

#[wasm_bindgen_test]
async fn a_store_on_opfs_reopens_with_what_it_committed() {
    let dir = fresh("round-trip").await;
    {
        let s = store(&dir).await.unwrap();
        s.set_meta("last_block", "24").unwrap();
        s.commit_window(
            &[("24-0".into(), r#"{"block_hash":"0x18"}"#.into())],
            Some((24, "0x18")),
            24,
        )
        .unwrap();
    }
    let s = store(&dir).await.unwrap();
    assert_eq!(s.get_meta("last_block").unwrap().as_deref(), Some("24"));
    assert_eq!(
        s.get_entity("24-0").unwrap().as_deref(),
        Some(r#"{"block_hash":"0x18"}"#)
    );
    assert_eq!(s.get_block_hash(24).unwrap().as_deref(), Some("0x18"));
}

#[wasm_bindgen_test]
async fn a_second_open_of_a_held_store_is_refused_by_name() {
    let dir = fresh("held").await;
    let held = store(&dir).await.unwrap();
    let again = OpfsBackend::open(&dir, FILE).await.unwrap_err();
    assert_eq!(again, OpenError::Locked { file: FILE.into() });
    assert!(again.to_string().contains("open in another tab"), "{again}");
    drop(held);
    store(&dir).await.expect("the lock goes with the handle");
}

/// Another worker asks the browser for the same file while this one holds it, as a second tab
/// would: the browser refuses it, which is the error [`OpenError::Locked`] is read from.
#[wasm_bindgen_test]
async fn another_worker_cannot_take_a_held_store() {
    let dir = fresh("other-worker").await;
    let held = store(&dir).await.unwrap();
    assert_eq!(
        other_worker_opens("other-worker").await,
        "NoModificationAllowedError"
    );
    drop(held);
    assert_eq!(other_worker_opens("other-worker").await, "opened");
}

const OTHER: &str = r#"
onmessage = async (e) => {
  try {
    const root = await navigator.storage.getDirectory();
    const dir = await root.getDirectoryHandle(e.data, { create: true });
    const file = await dir.getFileHandle("nuthatch.redb", { create: true });
    (await file.createSyncAccessHandle()).close();
    postMessage("opened");
  } catch (err) {
    postMessage(err.name);
  }
};
"#;

async fn other_worker_opens(nid: &str) -> String {
    let parts = js_sys::Array::of1(&JsValue::from_str(OTHER));
    let bag = web_sys::BlobPropertyBag::new();
    bag.set_type("text/javascript");
    let blob = web_sys::Blob::new_with_str_sequence_and_options(&parts, &bag).unwrap();
    let url = web_sys::Url::create_object_url_with_blob(&blob).unwrap();
    let worker = web_sys::Worker::new(&url).unwrap();
    let answer = js_sys::Promise::new(&mut |resolve, _| {
        let on = Closure::once_into_js(move |e: web_sys::MessageEvent| {
            let _ = resolve.call1(&JsValue::NULL, &e.data());
        });
        worker.set_onmessage(Some(on.unchecked_ref()));
    });
    worker.post_message(&JsValue::from_str(nid)).unwrap();
    let got = JsFuture::from(answer).await.unwrap();
    worker.terminate();
    got.as_string().unwrap_or_default()
}
