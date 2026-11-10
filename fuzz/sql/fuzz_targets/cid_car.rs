#![no_main]

//! `nuthatch::cid` against a hostile IPFS gateway (#2743): the CAR body `[[ipfs]]` resolution and
//! `nuthatch port` accept, the CID text a manifest names, and the re-encoding `verify` does.
//!
//! Random bytes almost never hash to the CID that names them, so most inputs build a real CAR: each
//! block is a raw leaf or a dag-pb node whose fuzzed UnixFS fields and links (to earlier blocks, any
//! number of times) are encoded here, then named by its true sha2-256. That puts the walk, the
//! protobuf reader and the caps in reach. Raw framing is fuzzed alongside. An `Err` is always fine;
//! a panic, an overflow or an allocation past the caps is a finding.

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use nuthatch::cid::{content_from_car, verify, Cid, CAPS};
use sha2::{Digest, Sha256};

const RAW: u64 = 0x55;
const DAG_PB: u64 = 0x70;
const SHA2_256: u64 = 0x12;

#[derive(Arbitrary, Debug)]
enum Block {
    Raw(Vec<u8>),
    Node(Node),
}

#[derive(Arbitrary, Debug)]
struct Node {
    /// Earlier blocks, by index modulo how many there are.
    links: Vec<u8>,
    /// Name the children by bare multihash, as CIDv0 does.
    v0_links: bool,
    kind: Option<u64>,
    inline: Option<Vec<u8>>,
    filesize: Option<u64>,
    blocksizes: Vec<u64>,
    /// A field neither dag-pb nor UnixFS defines, or a metadata one: `(field, wire, bytes)`.
    stray: Option<(u8, u8, Vec<u8>)>,
    omit_data: bool,
}

#[derive(Arbitrary, Debug)]
struct Input {
    blocks: Vec<Block>,
    root: u8,
    root_v0: bool,
    header: Vec<u8>,
    tail: Vec<u8>,
    raw_car: Vec<u8>,
    cid_text: String,
    content: Vec<u8>,
}

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let b = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(b);
            return;
        }
        out.push(b | 0x80);
    }
}

fn put_bytes(out: &mut Vec<u8>, field: u64, b: &[u8]) {
    put_varint(out, field << 3 | 2);
    put_varint(out, b.len() as u64);
    out.extend_from_slice(b);
}

fn put_uint(out: &mut Vec<u8>, field: u64, v: u64) {
    put_varint(out, field << 3);
    put_varint(out, v);
}

fn binary_cid(v0: bool, codec: u64, digest: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    if !v0 {
        put_varint(&mut out, 1);
        put_varint(&mut out, codec);
    }
    put_varint(&mut out, SHA2_256);
    put_varint(&mut out, digest.len() as u64);
    out.extend_from_slice(digest);
    out
}

fn encode(node: &Node, built: &[(u64, Vec<u8>)]) -> Vec<u8> {
    let mut out = Vec::new();
    if !built.is_empty() {
        for &l in node.links.iter().take(256) {
            let (codec, digest) = &built[l as usize % built.len()];
            let mut link = Vec::new();
            put_bytes(&mut link, 1, &binary_cid(node.v0_links, *codec, digest));
            put_bytes(&mut out, 2, &link);
        }
    }
    if !node.omit_data {
        let mut data = Vec::new();
        if let Some(k) = node.kind {
            put_uint(&mut data, 1, k);
        }
        if let Some(b) = &node.inline {
            put_bytes(&mut data, 2, b);
        }
        if let Some(s) = node.filesize {
            put_uint(&mut data, 3, s);
        }
        for &s in node.blocksizes.iter().take(256) {
            put_uint(&mut data, 4, s);
        }
        if let Some((field, wire, b)) = &node.stray {
            put_varint(&mut data, u64::from(*field) << 3 | u64::from(*wire & 7));
            data.extend_from_slice(b);
        }
        put_bytes(&mut out, 1, &data);
    }
    out
}

fuzz_target!(|input: Input| {
    let mut built: Vec<(u64, Vec<u8>)> = Vec::new();
    let mut car = Vec::new();
    put_varint(&mut car, input.header.len() as u64);
    car.extend_from_slice(&input.header);
    for block in input.blocks.iter().take(64) {
        let (codec, body) = match block {
            Block::Raw(b) => (RAW, b.clone()),
            Block::Node(n) => (DAG_PB, encode(n, &built)),
        };
        let digest = Sha256::digest(&body).to_vec();
        let id = binary_cid(false, codec, &digest);
        put_varint(&mut car, (id.len() + body.len()) as u64);
        car.extend_from_slice(&id);
        car.extend_from_slice(&body);
        built.push((codec, digest));
    }
    car.extend_from_slice(&input.tail);

    let root = match built.get(input.root as usize % built.len().max(1)) {
        Some((codec, digest)) => Cid {
            version: if input.root_v0 && *codec == DAG_PB {
                0
            } else {
                1
            },
            codec: *codec,
            hash_code: SHA2_256,
            digest: digest.clone(),
        },
        None => Cid {
            version: 1,
            codec: DAG_PB,
            hash_code: SHA2_256,
            digest: Sha256::digest(&input.header).to_vec(),
        },
    };
    if let Ok(file) = content_from_car(&root, &car, &CAPS) {
        assert!(
            file.len() <= CAPS.max_bytes,
            "{} bytes past the cap",
            file.len()
        );
    }
    let _ = content_from_car(&root, &input.raw_car, &CAPS);

    if let Ok(cid) = Cid::parse(&input.cid_text) {
        let _ = verify(&cid, &input.content);
        let _ = content_from_car(&cid, &input.raw_car, &CAPS);
    }
    let _ = verify(&root, &input.content);
});
