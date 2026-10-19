#!/usr/bin/env python3
"""A deterministic EVM RPC, just complete enough to back a footprint measurement.

The RAM budget is a **required** check, and it used to be measured against free public endpoints.
That had two costs. Repository secrets are not exposed to fork pull requests, so an external
contribution could never satisfy it - it failed on the honest "indexed 0 transfers, refusing to
false-pass" path and a maintainer had to read the log and wave it through, which is how a gate stops
meaning anything (issue #260). And the number itself was not comparable between runs: peak RSS
depended on how many blocks a rate-limited endpoint felt like serving that morning.

Serving the chain ourselves fixes both. Every run indexes exactly the same logs, so a change in peak
RSS is a change in nuthatch. No network, no secrets, no flake, and identical behaviour on a fork.

Deliberately NOT a general-purpose mock: it answers the four methods a backfill actually calls and
rejects everything else loudly, so a future method silently returning `null` cannot look like an
empty chain.
"""

import json
import sys
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

CHAIN_ID = 1
TIP = 20_000
LOGS_PER_BLOCK = 4
# `Transfer(address,address,uint256)`
TOPIC0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
CONTRACT = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"

# `--moving` (tip-lag.sh): the tip advances one block per `fixture_advance` call, and the fixture
# records when each block appeared and when an `eth_blockNumber` first reported it. Without it the
# chain is fixed at TIP, as every other job expects.
MOVING = "--moving" in sys.argv[2:]
FINALITY = 64
APPEARED = {}
SEEN = {}


def now_ms() -> float:
    return time.time() * 1000


def h(n: int) -> str:
    return hex(n)


def addr(n: int) -> str:
    return "0x" + f"{n:040x}"


def word(n: int) -> str:
    return "0x" + f"{n:064x}"


def logs_for(from_block: int, to_block: int):
    out = []
    for b in range(max(from_block, 1), min(to_block, TIP) + 1):
        for i in range(LOGS_PER_BLOCK):
            idx = b * LOGS_PER_BLOCK + i
            out.append({
                "address": CONTRACT,
                "topics": [TOPIC0, word(idx % 512), word((idx * 7 + 3) % 512)],
                # A value large enough to exercise the u256-as-text path rather than a small int.
                "data": word(1_000_000_000_000_000_000 * (idx % 97 + 1)),
                "blockNumber": h(b),
                "blockHash": "0x" + f"{b:064x}",
                "transactionHash": "0x" + f"{idx:064x}",
                "transactionIndex": h(i),
                # Per block, as a node numbers them: nuthatch refuses one wider than 20 bits (#2228).
                "logIndex": h(i),
                "removed": False,
            })
    return out


def handle(req):
    global TIP
    m = req.get("method")
    p = req.get("params") or []
    if m == "eth_chainId":
        return h(CHAIN_ID)
    if m == "eth_blockNumber":
        if MOVING:
            SEEN.setdefault(TIP, now_ms())
        return h(TIP)
    if m == "eth_getBlockByNumber":
        n = int(p[0], 16) if isinstance(p[0], str) and p[0].startswith("0x") else TIP
        if MOVING:
            if p[0] in ("finalized", "safe"):
                n = max(TIP - FINALITY, 0)
            if n > TIP:
                return None
        return {
            "number": h(n),
            "hash": "0x" + f"{n:064x}",
            "parentHash": "0x" + f"{max(n - 1, 0):064x}",
            # Deterministic, and monotonic so finality maths behaves.
            "timestamp": h(1_600_000_000 + n * 12),
        }
    if m == "eth_getLogs":
        f = p[0].get("fromBlock", "0x0")
        t = p[0].get("toBlock", h(TIP))
        f = TIP if f == "latest" else int(f, 16)
        t = TIP if t == "latest" else int(t, 16)
        return logs_for(f, t)
    if MOVING and m == "fixture_advance":
        TIP += 1
        APPEARED[TIP] = now_ms()
        return {"tip": TIP, "at": APPEARED[TIP]}
    if MOVING and m == "fixture_seen":
        return SEEN.get(int(p[0]))
    # Loudly, so an unimplemented method can never be mistaken for an empty chain.
    raise KeyError(m)


class Handler(BaseHTTPRequestHandler):
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
                    "jsonrpc": "2.0",
                    "id": it.get("id"),
                    "error": {"code": -32601, "message": f"footprint mock: unimplemented {e}"},
                })
        payload = json.dumps(out if batch else out[0]).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *_args):
        pass  # keep CI output about the measurement, not the traffic


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8545
    HTTPServer(("127.0.0.1", port), Handler).serve_forever()
