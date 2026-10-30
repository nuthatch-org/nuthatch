// RFC-0077 S2: the reference JavaScript host for `abi: 1` (`embed/README.md`), with no wasm-bindgen.
// It knows nothing of its surroundings: the browser worker hands it OPFS (`opfs.js`) and Node hands
// it files (`../node/fs-storage.mjs`), and both hand it `fetch` for the RPC.

const enc = new TextEncoder();
const dec = new TextDecoder();

/// An export's error, `{"fault", "message"}`, as a JavaScript error.
export class GuestFault extends Error {
  constructor(f) {
    super(`${f.fault}: ${f.message}`);
    this.fault = f.fault;
  }
}

/// `rpc` for `load`: each named endpoint POSTed with `fetch`, answered as `nh_resume` takes it.
export function fetchRpc(endpoints) {
  return async (r) => {
    const url = endpoints[r.endpoint];
    if (!url) return { error: `no endpoint named ${r.endpoint}` };
    try {
      const res = await fetch(url, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: r.body,
        signal: AbortSignal.timeout(r.deadline_ms),
      });
      return {
        status: res.status,
        status_text: res.statusText,
        retry_after: res.headers.get("retry-after"),
        body: await res.text(),
      };
    } catch (e) {
      return { error: String(e.cause ?? e.message), timed_out: e.name === "TimeoutError" };
    }
  };
}

/// A storage name the host will touch: relative, with no `..` or empty parts.
export function checkName(name) {
  if (typeof name !== "string" || name.split("/").some((p) => p === "" || p === "." || p === "..")) {
    throw new Error(`refused storage name ${JSON.stringify(name)}`);
  }
  return name;
}

/// Instantiate `module` (bytes or a `WebAssembly.Module`). `storage` is `{open, remove, list}`, whose
/// `open` resolves to a file with the sync access handle's methods; `rpc` performs an `rpc` request.
export async function load(module, { storage, rpc, log }) {
  const files = new Map();
  let nextHandle = 1;
  let mem;
  const view = (ptr, len) => new Uint8Array(mem.buffer, ptr >>> 0, len >>> 0);
  const file = (h) => {
    const f = files.get(h);
    if (!f) throw new Error(`no open file ${h}`);
    return f;
  };
  // A storage import answers a negative number for a failure, never a thrown error.
  const guarded = (name, f, failed = -1) => (...a) => {
    try {
      return f(...a);
    } catch (e) {
      log?.(1, `host storage: ${name}: ${e}`);
      return failed;
    }
  };
  const imports = {
    nuthatch: {
      now_ms: () => Date.now(),
      random_fill: (ptr, len) => {
        // getRandomValues fills at most 65,536 bytes a call.
        for (let at = 0; at < len; at += 65536) crypto.getRandomValues(view((ptr >>> 0) + at, Math.min(65536, len - at)));
      },
      log: (level, ptr, len) => log?.(level, dec.decode(view(ptr, len))),
      store_read: guarded("store_read", (h, at, ptr, len) => file(h).read(view(ptr, len), { at: Number(at) })),
      store_write: guarded("store_write", (h, at, ptr, len) => file(h).write(view(ptr, len), { at: Number(at) })),
      store_len: guarded("store_len", (h) => BigInt(file(h).getSize()), -1n),
      store_set_len: guarded("store_set_len", (h, len) => (file(h).truncate(Number(len)), 0)),
      store_sync: guarded("store_sync", (h) => (file(h).flush(), 0)),
      store_close: (h) => {
        const f = files.get(h);
        files.delete(h);
        try {
          f?.close();
        } catch (e) {
          log?.(1, `host storage: store_close: ${e}`);
        }
      },
    },
  };
  const instance = module instanceof WebAssembly.Module
    ? await WebAssembly.instantiate(module, imports)
    : (await WebAssembly.instantiate(module, imports)).instance;
  const g = instance.exports;
  mem = g.memory;

  const put = (data) => {
    const b = typeof data === "string" ? enc.encode(data) : data;
    const ptr = g.nh_alloc(b.length) >>> 0;
    view(ptr, b.length).set(b);
    return [ptr, b.length];
  };
  const take = (n) => {
    const len = Math.abs(n);
    const ptr = g.nh_alloc(len) >>> 0;
    g.nh_output(ptr);
    const bytes = view(ptr, len).slice();
    g.nh_free(ptr, len);
    if (n < 0) throw new GuestFault(JSON.parse(dec.decode(bytes)));
    return bytes;
  };
  const call = (name, ...args) => JSON.parse(dec.decode(take(g[name](...args))));

  async function perform(r) {
    try {
      switch (r.kind) {
        case "rpc":
          return await rpc(r);
        case "open": {
          const f = await storage.open(checkName(r.name));
          const h = nextHandle++;
          files.set(h, f);
          return { handle: h };
        }
        case "remove":
          await storage.remove(checkName(r.name));
          return {};
        case "list":
          return { names: await storage.list(r.prefix) };
        default:
          return { error: `unsupported request kind: ${r.kind}` };
      }
    } catch (e) {
      // OPFS refuses a second sync access handle on one file with this name (RFC-0075 S2).
      const locked = e?.name === "NoModificationAllowedError" || e?.locked === true;
      return { error: String(e?.message ?? e), ...(r.kind === "open" ? { locked } : {}) };
    }
  }

  const stats = { steps: 0, waits: 0, requests: 0, resume_bytes: 0, rpc_body_bytes: 0 };
  return {
    exports: g,
    stats,
    abi: () => call("nh_abi"),
    open: (config) => call("nh_open", ...put(JSON.stringify(config))),
    /// Step until the engine is idle at the tip, performing what it asks.
    async run() {
      for (;;) {
        stats.steps += 1;
        const s = call("nh_step");
        if (s.idle) return stats;
        if (s.requests) {
          const answers = await Promise.all(s.requests.map(perform));
          const json = JSON.stringify(answers);
          stats.requests += answers.length;
          stats.resume_bytes += json.length;
          for (const a of answers) stats.rpc_body_bytes += a.body?.length ?? 0;
          call("nh_resume", ...put(json));
        } else if (s.wait_ms !== undefined) {
          stats.waits += 1;
          await new Promise((r) => setTimeout(r, s.wait_ms));
        } else {
          throw new Error(`nh_step answered ${JSON.stringify(s)}`);
        }
      }
    },
    request: (method, target, body = "") => call("nh_request", ...put(method), ...put(target), ...put(body)),
    export: () => take(g.nh_export()),
    report: () => call("nh_report"),
    memoryBytes: () => g.nh_memory_bytes(),
  };
}
