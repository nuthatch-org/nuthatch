#![no_main]

//! `nuthatch::subgraph_import::parse_manifest` against a hostile gateway's manifest (#2743), and the
//! names it hands on to `to_alias` and `event_name`.
//!
//! Half the inputs are raw text. The other half fill a real manifest's values, so the source,
//! mapping and handler fields are reached without a seed corpus. An `Err` is always fine; a panic
//! or a runaway allocation is a finding.

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use nuthatch::subgraph_import::{event_name, parse_manifest, to_alias};

#[derive(Arbitrary, Debug)]
enum Input {
    Raw(String),
    Shaped {
        name: String,
        network: String,
        address: String,
        start_block: String,
        abi_file: String,
        event: String,
        /// Repeat the data source, to reach the source cap.
        copies: u16,
        tail: String,
    },
}

fn scalar(s: &str) -> String {
    serde_json::to_string(s).expect("a string serialises")
}

fuzz_target!(|input: Input| {
    let text = match input {
        Input::Raw(s) => s,
        Input::Shaped {
            name,
            network,
            address,
            start_block,
            abi_file,
            event,
            copies,
            tail,
        } => {
            let source = format!(
                "  - kind: ethereum/contract\n    name: {}\n    network: {}\n    source:\n      \
                 address: {}\n      abi: {}\n      startBlock: {}\n    mapping:\n      abis:\n        \
                 - name: {}\n          file: {}\n      eventHandlers:\n        - event: {}\n          \
                 handler: h\n",
                scalar(&name),
                scalar(&network),
                scalar(&address),
                scalar(&name),
                start_block.replace('\n', " "),
                scalar(&name),
                scalar(&abi_file),
                scalar(&event),
            );
            let mut text = String::from("specVersion: 0.0.8\ndataSources:\n");
            for _ in 0..=copies % 600 {
                text.push_str(&source);
            }
            text.push_str(&tail);
            text
        }
    };
    let Ok(manifest) = parse_manifest(&text) else {
        return;
    };
    let _ = manifest.network();
    for s in manifest.data_sources.iter().chain(&manifest.templates) {
        let _ = to_alias(&s.name);
        for e in &s.events {
            let _ = event_name(e);
        }
    }
});
