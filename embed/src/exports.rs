//! The module's ABI. Every export taking input reads JSON (or the store's bytes) from guest memory the
//! host wrote through `nh_alloc`, and returns the length of its output, negative for an error message;
//! the host then copies the output out with `nh_output`.
//!
//! Imports, module `nuthatch`: `rpc_batch(ptr, len) -> len` takes a JSON array of
//! `{endpoint, method, params}` and returns the length of a JSON array of `{body}` or `{error}`, one
//! per request in order, which `rpc_take(ptr)` then copies into guest memory.

use anyhow::{anyhow, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use std::cell::RefCell;
use std::collections::HashMap;

use crate::address_history::AddressHistory;
use crate::driver::{self, HostDiscoverer};
use crate::host::{Reply, Request};
use crate::memstore::Bytes;
use crate::store::Store;

#[link(wasm_import_module = "nuthatch")]
extern "C" {
    fn rpc_batch(ptr: *const u8, len: usize) -> usize;
    fn rpc_take(ptr: *mut u8);
}

struct Nest {
    history: AddressHistory,
    bytes: Bytes,
    discoverer: HostDiscoverer,
    cover_windows: u64,
}

thread_local! {
    static NEST: RefCell<Option<Nest>> = const { RefCell::new(None) };
    static OUTPUT: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

fn transport(requests: &[Request]) -> Result<Vec<Reply>> {
    let body = serde_json::to_vec(requests)?;
    let len = unsafe { rpc_batch(body.as_ptr(), body.len()) };
    let mut out = vec![0u8; len];
    unsafe { rpc_take(out.as_mut_ptr()) };
    serde_json::from_slice(&out).map_err(|e| anyhow!("the host's rpc_batch answer: {e}"))
}

fn finish(r: Result<Vec<u8>>) -> i64 {
    let (bytes, sign) = match r {
        Ok(b) => (b, 1),
        Err(e) => (format!("{e:#}").into_bytes(), -1),
    };
    let n = bytes.len() as i64;
    OUTPUT.with(|o| *o.borrow_mut() = bytes);
    sign * n
}

unsafe fn input<'a>(ptr: *const u8, len: usize) -> &'a [u8] {
    if len == 0 {
        &[]
    } else {
        std::slice::from_raw_parts(ptr, len)
    }
}

fn with_nest<T>(f: impl FnOnce(&Nest) -> Result<T>) -> Result<T> {
    NEST.with(|n| match &*n.borrow() {
        Some(nest) => f(nest),
        None => Err(anyhow!("nh_open has not been called")),
    })
}

#[no_mangle]
pub extern "C" fn nh_alloc(len: usize) -> *mut u8 {
    let mut v = Vec::<u8>::with_capacity(len.max(1));
    let p = v.as_mut_ptr();
    std::mem::forget(v);
    p
}

#[no_mangle]
pub unsafe extern "C" fn nh_free(ptr: *mut u8, len: usize) {
    drop(Vec::from_raw_parts(ptr, 0, len.max(1)));
}

#[no_mangle]
pub unsafe extern "C" fn nh_output(dst: *mut u8) {
    OUTPUT.with(|o| {
        let o = o.borrow();
        std::ptr::copy_nonoverlapping(o.as_ptr(), dst, o.len());
    });
}

#[derive(Deserialize)]
struct OpenConfig {
    chain_id: u64,
    #[serde(default)]
    addresses: Vec<String>,
    /// redb's page cache, bytes. The store itself is in guest memory already, so this stays small.
    #[serde(default = "default_cache")]
    cache_bytes: usize,
    /// Windows an `nh_query` may sync before it answers incomplete; the rest is left to `nh_sync`.
    #[serde(default = "default_cover")]
    cover_windows: u64,
}

fn default_cache() -> usize {
    4 << 20
}

fn default_cover() -> u64 {
    4
}

/// Open the store: from `state` (bytes an earlier `nh_export` gave), or empty when `state_len` is 0.
#[no_mangle]
pub unsafe extern "C" fn nh_open(
    cfg: *const u8,
    cfg_len: usize,
    state: *const u8,
    state_len: usize,
) -> i64 {
    finish((|| {
        let c: OpenConfig = serde_json::from_slice(input(cfg, cfg_len))?;
        let bytes = Bytes::from_vec(input(state, state_len).to_vec());
        let mut b = redb::Builder::new();
        b.set_cache_size(c.cache_bytes);
        let db = b.create_with_backend(bytes.clone())?;
        let history = AddressHistory::open(Store::from_database(db), c.chain_id, &c.addresses)?;
        let watched = history.watched()?;
        NEST.with(|n| {
            *n.borrow_mut() = Some(Nest {
                history,
                bytes,
                discoverer: driver::discoverer(c.chain_id),
                cover_windows: c.cover_windows,
            })
        });
        Ok(serde_json::to_vec(&json!({ "watched": watched }))?)
    })())
}

#[derive(Deserialize)]
struct SyncArgs {
    addresses: Option<Vec<String>>,
    #[serde(default)]
    from: u64,
    to: Option<u64>,
    #[serde(default = "unbounded")]
    max_windows: u64,
}

fn unbounded() -> u64 {
    u64::MAX
}

/// Discover and record every watched address up to `to`, or the finalized head.
#[no_mangle]
pub unsafe extern "C" fn nh_sync(ptr: *const u8, len: usize) -> i64 {
    let args = input(ptr, len);
    finish(with_nest(|nest| {
        let a: SyncArgs = serde_json::from_slice(args)?;
        let report = crate::host::block_on(
            driver::sync(
                &nest.discoverer,
                &nest.history,
                a.addresses,
                a.from,
                a.to,
                a.max_windows,
            ),
            &mut transport,
        )??;
        Ok(serde_json::to_vec(&report)?)
    }))
}

#[derive(Deserialize)]
struct WatchArgs {
    #[serde(default)]
    add: Vec<String>,
    #[serde(default)]
    remove: Vec<String>,
}

#[no_mangle]
pub unsafe extern "C" fn nh_watch(ptr: *const u8, len: usize) -> i64 {
    let args = input(ptr, len);
    finish(with_nest(|nest| {
        let a: WatchArgs = serde_json::from_slice(args)?;
        nest.history.watch_many(&a.add)?;
        for r in &a.remove {
            nest.history.unwatch(r)?;
        }
        Ok(serde_json::to_vec(
            &json!({ "watched": nest.history.watched()? }),
        )?)
    }))
}

/// Answer one Etherscan-shaped request, `{"module": ..., "action": ..., ...}`, as the nest's `/api`
/// answers it.
#[no_mangle]
pub unsafe extern "C" fn nh_query(ptr: *const u8, len: usize) -> i64 {
    let args = input(ptr, len);
    finish(with_nest(|nest| {
        let raw: HashMap<String, Value> = serde_json::from_slice(args)?;
        let params: HashMap<String, String> = raw
            .into_iter()
            .map(|(k, v)| {
                let s = match v {
                    Value::String(s) => s,
                    other => other.to_string(),
                };
                (k, s)
            })
            .collect();
        let answer = crate::host::block_on(
            driver::answer(&nest.discoverer, &nest.history, &params, nest.cover_windows),
            &mut transport,
        )?;
        Ok(serde_json::to_vec(&answer)?)
    }))
}

/// The whole store, for the host to keep and hand back to `nh_open`.
#[no_mangle]
pub extern "C" fn nh_export() -> i64 {
    finish(with_nest(|nest| Ok(nest.bytes.snapshot())))
}
