// RFC-0077 S2: the dedicated worker that runs the plain module through `abi: 1` (`host.js`), with its
// store in OPFS and its RPC over `fetch`, and answers the node's routes for the Service Worker. The
// page posts `{nest: "open", ...}` once, then `{nest: "request", ...}` with a port for each answer, or
// `{nest: "call", ...}` for RFC-0075 §7's JS API. With `keepFollowing` it follows the tip every poll
// interval and posts `{nest: "followed", ...}` after each pass; `{nest: "follow"}` asks for one pass.
// With `stopOnceSealed` it follows only until a segment is sealed and posts `{nest: "stopped", ...}`
// with requests outstanding, for a harness to kill the browser there (RFC-0077 S3).
import { fetchRpc, load, onceSealed } from "./host.js";
import { opfsStorage } from "./opfs.js";

let engine;
const enc = new TextEncoder();

async function follow(stop) {
  const s = await engine.run(stop);
  const f = engine.report();
  return {
    last_block: f.last_block, sealed_through: f.sealed_through, hot_rows: f.hot_rows,
    holders: f.balances.holders, resumed_from: f.resumed_from, backend: f.backend,
    segments: f.segments, stopped: s.stopped ?? false, mirror: f.mirror,
  };
}

function keepFollowing(secs) {
  const pass = async () => {
    try {
      self.postMessage({ nest: "followed", ...(await follow()) });
    } catch (e) {
      self.postMessage({ nest: "followed", error: String(e && e.message || e) });
    }
    setTimeout(pass, secs * 1000);
  };
  setTimeout(pass, secs * 1000);
}

const hex = (buf) => [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, "0")).join("");

self.onmessage = async (m) => {
  const d = m.data;
  try {
    if (d.nest === "open") {
      const bytes = await (await fetch(d.module)).arrayBuffer();
      const module_sha256 = hex(await crypto.subtle.digest("SHA-256", bytes));
      engine = await load(bytes, {
        storage: await opfsStorage(d.nid),
        rpc: fetchRpc({ main: d.rpc }),
        log: (level, s) => console.log(`guest[${level}]: ${s}`),
      });
      // `config` is a whole `nh_open` config, as a page opening a mirrored nest sends one (RFC-0077 S4).
      engine.open(d.config ?? {
        nid: d.nid, contract: d.contract, start: Number(d.start), rpc: ["main"], storage: "host",
        fresh: d.fresh ?? true,
        memory_mib: d.memoryCeilingMib, poll_interval_secs: d.pollIntervalSecs, files: d.files,
      });
      if (d.stopOnceSealed) {
        self.postMessage({ nest: "stopped", module_sha256, ...(await follow(onceSealed(engine))) });
        return;
      }
      self.postMessage({ nest: "opened", module_sha256, abi: engine.abi(), ...(await follow()) });
      if (d.keepFollowing) keepFollowing(d.pollIntervalSecs || 12);
    } else if (d.nest === "follow") {
      self.postMessage({ nest: "followed", ...(await follow()) });
    } else if (d.nest === "request") {
      const a = engine.request(d.method, d.target, d.body);
      // An Arrow answer's bytes cross as base64, with its envelope as the node's header (RFC-0081 S1).
      const body = a.body_base64 === undefined
        ? enc.encode(a.body)
        : Uint8Array.from(atob(a.body_base64), (c) => c.charCodeAt(0));
      m.ports[0].postMessage({ status: a.status, contentType: a.content_type, headers: a.headers, body }, [body.buffer]);
    } else if (d.nest === "call") {
      const [a, b] = d.args;
      const e = encodeURIComponent;
      const target = d.fn === "sql" ? `/sql?q=${e(a)}`
        : d.fn === "entity" ? `/entity/${e(a)}`
        : d.fn === "derived" ? `/derived/${e(a)}${b == null ? "" : `/${e(b)}`}`
        : null;
      if (target === null) throw new Error(`no such call: ${d.fn}`);
      m.ports[0].postMessage({ text: engine.request("GET", target).body });
    }
  } catch (e) {
    const error = String(e && e.message || e);
    if (m.ports[0]) m.ports[0].postMessage({ error });
    else self.postMessage({ nest: "failed", error });
  }
};
