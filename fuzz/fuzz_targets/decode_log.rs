#![no_main]

//! Fuzzes malformed *logs* against a fixed set of curated ABIs (nuthatch#290). The ABIs are
//! deliberately not fuzzed here (`abi_json`/`abi_arbitrary` own that axis) - this target's whole
//! job is truncated `data`, wrong topic counts, oversized/undersized topic words, and non-hex
//! garbage, run against event shapes chosen to stress `EventDecoder`/`decode_log_parts` and
//! `value_from_dynsol` specifically:
//!   - `fuzz__transfer`      ordinary indexed-address / non-indexed-uint256 (the common case)
//!   - `fuzz__hashed`        indexed `string` - arrives as a topic hash, exercises `StorageKind::Hash32`
//!   - `fuzz__collection`    non-indexed dynamic array + tuple - exercises `DynSeq`/`FixedSeq` detokenize
//!   - `fuzz__huge_array`    non-indexed `uint256[4000000000]` - the allocation-risk shape: decoding
//!                           against truncated `data` must error, not try to allocate 4B elements
//!   - `fuzz__deep`          a tuple nested 512 levels - decode-time recursion depth, not just the
//!                           registry-build-time signature computation `abi_arbitrary` covers
//!   - `fuzz__narrow`        non-indexed `uint64` - exercises the `bits <= 64` branch in
//!                           `value_from_dynsol` (COR-11 guard, nuthatch#290); no uint narrower than
//!                           256 appears in any other fixture so that branch was structurally
//!                           unreachable before this fixture was added (nuthatch#612)
//!   - `fuzz__narrow_int`    indexed and non-indexed `int64` - the signed `bits <= 64` branch, whose
//!                           `as_i64()` panicked on any word outside `i64` (#2148)
//!
//! `topic0` is left fixed per fixture (the registry has already resolved it; it is not the part of
//! a log an attacker or a byzantine RPC provider controls) - everything else is fuzzed.

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use nuthatch_decode::registry::{ContractSpec, DecodeRegistry};
use nuthatch_decode::rpc::Log;
use std::sync::OnceLock;

/// Builds a `tuple` parameter nested `depth` levels deep. `alloy-json-abi` only accepts the
/// `indexed` key on a top-level event parameter (`EventParam`); every `components` entry
/// deserializes as a plain `Param`, which rejects the key outright - nuthatch#290. So only the
/// outermost object (returned to become an event's top-level input) carries `indexed`.
fn nested_tuple_param(depth: u16) -> serde_json::Value {
    let mut param = serde_json::json!({"name": "leaf", "type": "uint256"});
    for i in 0..depth {
        param = serde_json::json!({
            "name": format!("t{i}"),
            "type": "tuple",
            "components": [param],
        });
    }
    param
}

fn nested_tuple_inputs(depth: u16) -> serde_json::Value {
    let mut top = nested_tuple_param(depth);
    top["indexed"] = serde_json::Value::Bool(false);
    serde_json::json!([top])
}

fn build_registry() -> DecodeRegistry {
    let events = serde_json::json!([
        {
            "type": "event",
            "name": "Transfer",
            "anonymous": false,
            "inputs": [
                {"name": "from", "type": "address", "indexed": true},
                {"name": "to", "type": "address", "indexed": true},
                {"name": "value", "type": "uint256", "indexed": false},
            ],
        },
        {
            "type": "event",
            "name": "Hashed",
            "anonymous": false,
            "inputs": [
                {"name": "label", "type": "string", "indexed": true},
                {"name": "amount", "type": "uint256", "indexed": false},
            ],
        },
        {
            "type": "event",
            "name": "Collection",
            "anonymous": false,
            "inputs": [
                {"name": "amounts", "type": "uint256[]", "indexed": false},
                {
                    "name": "pair",
                    "type": "tuple",
                    "indexed": false,
                    "components": [
                        {"name": "a", "type": "address"},
                        {"name": "b", "type": "uint256"},
                    ],
                },
            ],
        },
        {
            "type": "event",
            "name": "HugeArray",
            "anonymous": false,
            "inputs": [{"name": "data", "type": "uint256[4000000000]", "indexed": false}],
        },
        {
            "type": "event",
            "name": "Deep",
            "anonymous": false,
            "inputs": nested_tuple_inputs(512),
        },
        {
            "type": "event",
            "name": "Narrow",
            "anonymous": false,
            "inputs": [
                {"name": "id", "type": "uint64", "indexed": false},
            ],
        },
        {
            "type": "event",
            "name": "NarrowInt",
            "anonymous": false,
            "inputs": [
                {"name": "t", "type": "int64", "indexed": true},
                {"name": "v", "type": "int64", "indexed": false},
            ],
        },
    ]);
    let abi: alloy_json_abi::JsonAbi = serde_json::from_value(events).unwrap();
    let spec = ContractSpec {
        alias: "fuzz".to_string(),
        address: contract_address(),
        abi,
        events: Vec::new(),
    };
    DecodeRegistry::build(vec![spec]).expect("curated fixture ABI must register cleanly")
}

fn contract_address() -> alloy_primitives::Address {
    alloy_primitives::Address::repeat_byte(0x11)
}

fn registry() -> &'static DecodeRegistry {
    static REG: OnceLock<DecodeRegistry> = OnceLock::new();
    REG.get_or_init(build_registry)
}

#[derive(Arbitrary, Debug)]
struct FuzzInput {
    fixture: Fixture,
    /// Raw topic words, each of arbitrary length (not forced to 32 bytes) - "wrong topic count" and
    /// undersized/oversized topics both come out of this directly.
    extra_topics: Vec<Vec<u8>>,
    data: Vec<u8>,
    /// Occasionally corrupt the hex encoding itself rather than only varying the underlying bytes.
    corrupt_hex: bool,
}

#[derive(Arbitrary, Debug, Clone, Copy)]
enum Fixture {
    Transfer,
    Hashed,
    Collection,
    HugeArray,
    Deep,
    Narrow,
    NarrowInt,
}

impl Fixture {
    fn table(self) -> &'static str {
        match self {
            Self::Transfer => "fuzz__transfer",
            Self::Hashed => "fuzz__hashed",
            Self::Collection => "fuzz__collection",
            Self::HugeArray => "fuzz__huge_array",
            Self::Deep => "fuzz__deep",
            Self::Narrow => "fuzz__narrow",
            Self::NarrowInt => "fuzz__narrow_int",
        }
    }
}

fn to_hex(bytes: &[u8], corrupt: bool) -> String {
    let mut s = format!("0x{}", hex::encode(bytes));
    if corrupt && !s.is_empty() {
        // Flip the last character to a definitely-non-hex byte, forcing the malformed-hex path.
        s.push('z');
    }
    s
}

fuzz_target!(|input: FuzzInput| {
    let reg = registry();
    let dec = reg
        .tables()
        .into_iter()
        .find(|d| d.table == input.fixture.table())
        .expect("fixture table always present in the curated registry");

    let mut topics = vec![format!("0x{}", hex::encode(dec.topic0))];
    // Cap how many extra topics we add so a single corpus entry can't ask for an absurd
    // `Vec<String>` allocation before nuthatch's own decode path even runs - that would be testing
    // `arbitrary`/this harness, not the decode path nuthatch#290 is about.
    for t in input.extra_topics.iter().take(64) {
        topics.push(to_hex(t, input.corrupt_hex));
    }

    let log = Log {
        address: format!("0x{}", hex::encode(contract_address())),
        topics,
        data: to_hex(&input.data, input.corrupt_hex),
        block_number: 1,
        block_hash: "0x00".to_string(),
        tx_hash: "0x00".to_string(),
        log_index: 0,
    };

    // Hunting panics and unbounded allocation (nuthatch#290) - a malformed log producing `Err` (or
    // `Ok(None)` on a topic0/address mismatch, or `Ok(Some(..))` on a well-formed decode) is all
    // correct; only a panic or an OOM abort is a finding.
    let _ = reg.decode(&log);
});
