#!/usr/bin/env python3
"""A deterministic EVM RPC for the dense multi-nest footprint measurement.

`footprint-rpc.py` serves one contract, one event type, and a fixed tip - the right fixture for the
single-nest backfill budget it backs. This one exists because the ≤2 GB per-cursor budget
(CLAUDE.md non-negotiable 2, RFC-0021) is about the *adversarial* case, and that case differs in
three ways:

  1. **A large ABI.** Ten event types on one contract, several of them eight inputs wide, so decode
     carries ten topic0 registrations and ten tables per nest rather than one. The ABI is the real
     Uniswap V4 `PoolManager` one (nuthatch-org/uniswap-v4-ethereum), read from a file rather than
     hardcoded here - see below.
  2. **A high event rate**, from many contracts each emitting every block.
  3. **At tip, not mid-backfill.** The tip *moves*: `eth_blockNumber` advances by `TIP_STEP` blocks
     per call up to `FINAL_TIP`, so after the backfill drains, the cursor spends the rest of the run
     on the live path - near-tip window, finality, sealing - which is the regime the budget is about.
     A fixed tip only ever measures a backfill.

**The tip advance is deterministic in its endpoint, not in its schedule.** A faster machine polls
more often and gets there sooner, but every run ends at exactly `FINAL_TIP` having served exactly
the same logs, so peak RSS stays comparable between runs. That is the property `footprint-rpc.py`
was rewritten to get (issue #260) and it is not given up here.

**topic0 is derived, never hardcoded.** The harness writes an ABI and this reads *the same file*
(`--abi`), hashing each event signature to its topic0. Hardcoding them would let the fixture and the
nest drift apart silently, and a drifted topic0 does not fail loudly - it decodes to an empty table,
which is precisely the shape of a footprint check that passes because it measured nothing.

No network, no secrets, identical on a fork.

Usage: multinest-rpc.py --port N --abi PATH [--contracts N] [--logs-per-block N]
                        [--initial-tip N] [--final-tip N] [--tip-step N]
"""

import argparse
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer

# --- keccak-256 ------------------------------------------------------------------------------
# Pure Python because CI has no `cast` and no `eth_hash`/`pycryptodome`, and the alternative -
# hardcoding ten topic0 constants - is the drift this file exists to avoid. Verified at import
# against a known vector (`Transfer(address,address,uint256)`), so a wrong implementation fails
# immediately and loudly rather than by serving logs nothing matches.

_MASK = (1 << 64) - 1
_RC = [
    0x0000000000000001, 0x0000000000008082, 0x800000000000808A, 0x8000000080008000,
    0x000000000000808B, 0x0000000080000001, 0x8000000080008081, 0x8000000000008009,
    0x000000000000008A, 0x0000000000000088, 0x0000000080008009, 0x000000008000000A,
    0x000000008000808B, 0x800000000000008B, 0x8000000000008089, 0x8000000000008003,
    0x8000000000008002, 0x8000000000000080, 0x000000000000800A, 0x800000008000000A,
    0x8000000080008081, 0x8000000000008080, 0x0000000080000001, 0x8000000080008008,
]
_ROT = [
    [0, 36, 3, 41, 18],
    [1, 44, 10, 45, 2],
    [62, 6, 43, 15, 61],
    [28, 55, 25, 21, 56],
    [27, 20, 39, 8, 14],
]


def _rotl(x, n):
    n %= 64
    return ((x << n) | (x >> (64 - n))) & _MASK if n else x


def _keccak_f(a):
    for rnd in range(24):
        c = [a[x][0] ^ a[x][1] ^ a[x][2] ^ a[x][3] ^ a[x][4] for x in range(5)]
        d = [c[(x - 1) % 5] ^ _rotl(c[(x + 1) % 5], 1) for x in range(5)]
        for x in range(5):
            for y in range(5):
                a[x][y] ^= d[x]
        b = [[0] * 5 for _ in range(5)]
        for x in range(5):
            for y in range(5):
                b[y][(2 * x + 3 * y) % 5] = _rotl(a[x][y], _ROT[x][y])
        for x in range(5):
            for y in range(5):
                a[x][y] = b[x][y] ^ ((~b[(x + 1) % 5][y] & _MASK) & b[(x + 2) % 5][y])
        a[0][0] ^= _RC[rnd]
    return a


def keccak256(data: bytes) -> bytes:
    rate = 136  # 1088 bits, the keccak-256 rate
    # Keccak's original padding (0x01 .. 0x80), NOT SHA3's 0x06 - Ethereum uses the former.
    padded = bytearray(data)
    padded.append(0x01)
    while len(padded) % rate != 0:
        padded.append(0x00)
    padded[-1] |= 0x80

    a = [[0] * 5 for _ in range(5)]
    for off in range(0, len(padded), rate):
        block = padded[off:off + rate]
        for i in range(rate // 8):
            lane = int.from_bytes(block[i * 8:(i + 1) * 8], "little")
            a[i % 5][i // 5] ^= lane
        _keccak_f(a)

    out = bytearray()
    for i in range(4):  # 32 bytes = 4 lanes, all within the first rate-worth of state
        out += a[i % 5][i // 5].to_bytes(8, "little")
    return bytes(out[:32])


_KNOWN = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
if "0x" + keccak256(b"Transfer(address,address,uint256)").hex() != _KNOWN:
    raise SystemExit("multinest-rpc: keccak256 self-test failed - refusing to serve a bogus chain")

# --- ABI -> event table ----------------------------------------------------------------------


def canonical(ev) -> str:
    """The signature topic0 hashes: `Name(type,type,...)`, with tuples flattened."""
    def ty(i):
        if i["type"].startswith("tuple"):
            inner = ",".join(ty(c) for c in i.get("components", []))
            return "(" + inner + ")" + i["type"][len("tuple"):]
        return i["type"]
    return ev["name"] + "(" + ",".join(ty(i) for i in ev["inputs"]) + ")"


def load_events(path):
    with open(path) as f:
        abi = json.load(f)
    out = []
    for e in abi:
        if e.get("type") != "event" or e.get("anonymous"):
            continue
        sig = canonical(e)
        out.append({
            "name": e["name"],
            "sig": sig,
            "topic0": "0x" + keccak256(sig.encode()).hex(),
            "indexed": [i["type"] for i in e["inputs"] if i.get("indexed")],
            "data": [i["type"] for i in e["inputs"] if not i.get("indexed")],
        })
    if not out:
        raise SystemExit(f"multinest-rpc: {path} declares no non-anonymous events")
    return out


# --- value encoding --------------------------------------------------------------------------
# Every V4 PoolManager input is a static type, so a value is exactly one 32-byte word and `data` is
# their concatenation - no dynamic-offset encoding needed. Signed types are emitted two's-complement
# and *do* go negative (a V4 `Swap` always has one negative amount), because a decoder that gets
# sign wrong should fail here rather than in production.

def word(n: int) -> str:
    return f"{n & ((1 << 256) - 1):064x}"


def encode(sol_type: str, seed: int) -> str:
    if sol_type == "address":
        return word(seed % 4096 + 1)
    if sol_type == "bool":
        return word(seed % 2)
    if sol_type.startswith("int"):
        bits = int(sol_type[3:] or 256)
        # Straddle zero so the sign bit is exercised on roughly half the rows.
        mag = seed % (1 << min(bits - 2, 48))
        return word(-mag if seed % 2 else mag)
    if sol_type.startswith("uint"):
        bits = int(sol_type[4:] or 256)
        return word((seed * 1_000_003) % (1 << min(bits, 160)))
    if sol_type.startswith("bytes") and sol_type != "bytes":
        return word(seed % (1 << 32))
    # `bytes`/`string` are dynamic; this ABI has none and encoding them properly is not worth it.
    raise SystemExit(f"multinest-rpc: unsupported input type {sol_type}")


class Chain:
    def __init__(self, args, events):
        self.events = events
        self.contracts = ["0x" + f"{i + 1:040x}" for i in range(args.contracts)]
        self.by_addr = {a: i for i, a in enumerate(self.contracts)}
        self.logs_per_block = args.logs_per_block
        self.initial_tip = args.initial_tip
        self.final_tip = args.final_tip
        self.tip_step = args.tip_step
        self.tip = args.initial_tip
        self.served_to = 0
        self.lock = threading.Lock()

    def block_number(self):
        """Hold at `initial_tip` until the backfill has drained, then advance `tip_step` per call.

        The hold is what makes the run reproducible. `dev --backfill N` indexes N blocks back from
        *whatever tip it first observes*, so a tip that moves from the first poll gives a different
        first block on every run - and then "the same logs every run", the property this fixture
        exists to provide, is not true. Holding until the cursor has actually asked for blocks up to
        `initial_tip` pins the backfill to exactly `initial_tip - N + 1 ..= initial_tip`, and cleanly
        separates the two phases: backfill first, live tip-following after.
        """
        with self.lock:
            if self.served_to >= self.initial_tip and self.tip < self.final_tip:
                self.tip = min(self.tip + self.tip_step, self.final_tip)
            return self.tip

    def note_served(self, to_block):
        """Record how far the cursor has asked for logs - the signal that the backfill has drained."""
        with self.lock:
            self.served_to = max(self.served_to, to_block)

    def logs(self, from_block, to_block, addresses):
        want = self.by_addr.keys() if not addresses else [a for a in addresses if a in self.by_addr]
        out = []
        lo, hi = max(from_block, 1), min(to_block, self.final_tip)
        for b in range(lo, hi + 1):
            for a in want:
                ci = self.by_addr[a]
                for i in range(self.logs_per_block):
                    # Cycle the event types so every one of the ten tables gets rows, and the
                    # per-table row-count floor in the harness can prove each topic0 landed.
                    ev = self.events[(b * self.logs_per_block + i) % len(self.events)]
                    seed = b * 131 + i * 17 + ci * 7
                    topics = [ev["topic0"]] + [
                        "0x" + encode(t, seed + j + 1) for j, t in enumerate(ev["indexed"])
                    ]
                    data = "".join(encode(t, seed + 100 + j) for j, t in enumerate(ev["data"]))
                    idx = (b * self.logs_per_block + i) * len(self.contracts) + ci
                    out.append({
                        "address": a,
                        "topics": topics,
                        "data": "0x" + data,
                        "blockNumber": hex(b),
                        "blockHash": "0x" + f"{b:064x}",
                        "transactionHash": "0x" + f"{idx:064x}",
                        "transactionIndex": hex(i),
                        # Per block, as a node numbers them: nuthatch refuses one wider than 20 bits (#2228).
                        "logIndex": hex(i * len(self.contracts) + ci),
                        "removed": False,
                    })
        return out


def make_handler(chain):
    def handle(req):
        m = req.get("method")
        p = req.get("params") or []
        if m == "eth_chainId":
            return hex(1)
        if m == "eth_blockNumber":
            return hex(chain.block_number())
        if m == "eth_getBlockByNumber":
            n = int(p[0], 16) if isinstance(p[0], str) and p[0].startswith("0x") else chain.tip
            return {
                "number": hex(n),
                "hash": "0x" + f"{n:064x}",
                "parentHash": "0x" + f"{max(n - 1, 0):064x}",
                "timestamp": hex(1_600_000_000 + n * 12),
            }
        if m == "eth_getLogs":
            f = p[0].get("fromBlock", "0x0")
            t = p[0].get("toBlock", "latest")
            f = chain.tip if f == "latest" else int(f, 16)
            t = chain.tip if t == "latest" else int(t, 16)
            addr = p[0].get("address")
            if isinstance(addr, str):
                addr = [addr]
            addr = [a.lower() for a in (addr or [])]
            chain.note_served(t)
            return chain.logs(f, t, addr)
        # Loudly: an unimplemented method returning null is indistinguishable from an empty chain,
        # and an empty chain is how a footprint check passes without measuring anything.
        raise KeyError(m)

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_POST(self):  # noqa: N802
            body = self.rfile.read(int(self.headers.get("content-length", 0)))
            try:
                req = json.loads(body)
            except json.JSONDecodeError:
                self.send_response(400)
                self.end_headers()
                return
            batch = isinstance(req, list)
            items = req if batch else [req]
            out = []
            for it in items:
                try:
                    out.append({"jsonrpc": "2.0", "id": it.get("id"), "result": handle(it)})
                except KeyError as e:
                    out.append({
                        "jsonrpc": "2.0", "id": it.get("id"),
                        "error": {"code": -32601, "message": f"multinest mock: unimplemented {e}"},
                    })
            payload = json.dumps(out if batch else out[0]).encode()
            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_args):
            pass  # keep CI output about the measurement, not the traffic

    return Handler


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8546)
    ap.add_argument("--abi", required=True, help="the ABI the harness wrote; topic0s come from it")
    ap.add_argument("--contracts", type=int, default=8, help="one per nest on the cursor")
    ap.add_argument("--logs-per-block", type=int, default=10, help="per contract, per block")
    ap.add_argument("--initial-tip", type=int, default=20_000)
    ap.add_argument("--final-tip", type=int, default=20_240)
    ap.add_argument("--tip-step", type=int, default=8, help="blocks released per eth_blockNumber")
    args = ap.parse_args()

    events = load_events(args.abi)
    chain = Chain(args, events)
    print(
        f"multinest-rpc: {len(events)} events, {args.contracts} contracts, "
        f"{args.logs_per_block} logs/block/contract, tip {args.initial_tip}->{args.final_tip} "
        f"step {args.tip_step}",
        file=sys.stderr,
    )
    ThreadingHTTPServer(("127.0.0.1", args.port), make_handler(chain)).serve_forever()


if __name__ == "__main__":
    main()
