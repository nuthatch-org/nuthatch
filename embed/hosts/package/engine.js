// RFC-0081 S2: the host package, released as `nuthatch-engine.js`. One ES module for browsers and Node,
// bundled by `build.mjs` from this file and the reference hosts it imports, which it runs unchanged.
// It owns the worker, storage, retries across a transport failure, a header-batch size per endpoint
// and the `follow_failed` carry. RPC URLs, keys and a light client are the caller's, passed in.
// `embed/README.md` ("The host package") is its interface.
import { fetchRpc, GuestFault, load } from "../browser/host.js";
import { opfsStorage } from "../browser/opfs.js";
import { fsStorage } from "../node/fs-storage.mjs";

export { GuestFault };

const ON_NODE = typeof process !== "undefined" && !!process.versions?.node;
const WORKER_NAME = "nuthatch-engine";
const ABI = 1;

/// Endpoints that refuse a long JSON-RPC batch, by host. arb1.arbitrum.io answers ten headers to a
/// batch and refuses fifty (#1570); the module leaves an endpoint's cap to its host.
const HEADER_BATCH = { "arb1.arbitrum.io": 10 };
const TRANSPORT_RETRIES = 3;
const FOLLOW_RETRIES = 5;

/// The package's own options; every other key is `nh_open`'s, passed through as given.
const OWN = ["module", "nest", "rpc", "storage", "nid", "fresh", "headerBatch", "verified", "log", "onProgress",
  "worker", "workerUrl"];

const rest = (ms) => new Promise((ok) => setTimeout(ok, ms));
const hex = (buf) => [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, "0")).join("");
const sha256 = async (bytes) => hex(await crypto.subtle.digest("SHA-256", bytes));

/// `rpc` as names the module sees and the URLs only the host holds.
function endpointsOf(rpc) {
  if (rpc === undefined) return { main: undefined };
  return typeof rpc === "string" || rpc instanceof URL ? { main: String(rpc) } : { ...rpc };
}

/// The host's `rpc`: each POST retried across a transport failure, and a batch longer than its
/// endpoint takes sent as several, one after another, each part retried on a throttle.
function rpcFor(endpoints, headerBatch) {
  const post = fetchRpc(endpoints);
  const batchOf = (name) => {
    if (typeof headerBatch === "number") return headerBatch;
    if (headerBatch?.[name] !== undefined) return headerBatch[name];
    try {
      return HEADER_BATCH[new URL(endpoints[name]).hostname];
    } catch {
      return undefined;
    }
  };
  const once = async (r) => {
    for (let k = 0; ; k++) {
      const a = await post(r);
      // No reply at all is the host's to retry; a deadline passed is the module's to judge.
      if (!a.error || a.timed_out || k >= TRANSPORT_RETRIES) return a;
      await rest(500 * 2 ** k);
    }
  };
  return async (r) => {
    const n = batchOf(r.endpoint);
    let batch;
    try {
      batch = JSON.parse(r.body);
    } catch {
      return once(r);
    }
    if (!n || !Array.isArray(batch) || batch.length <= n) return once(r);
    const items = [];
    for (let i = 0; i < batch.length; i += n) {
      let a;
      for (let k = 0; ; k++) {
        a = await once({ ...r, body: JSON.stringify(batch.slice(i, i + n)) });
        if (a.status !== 429 || k >= 6) break;
        await rest(500 * 2 ** k);
      }
      if (a.error || a.status !== 200) return a;
      const v = JSON.parse(a.body);
      if (!Array.isArray(v)) return a;
      items.push(...v);
    }
    return { status: 200, status_text: "OK", retry_after: null, body: JSON.stringify(items) };
  };
}

/// The module's bytes from a URL, a path (Node) or bytes; a `.gz` name is inflated.
async function moduleBytes(m) {
  if (m instanceof ArrayBuffer || ArrayBuffer.isView(m)) return m;
  const name = String(m);
  let body;
  if (ON_NODE && !/^(https?|data|blob):/i.test(name)) {
    const { readFile } = process.getBuiltinModule("node:fs/promises");
    const file = /^file:/i.test(name) ? new URL(name) : name;
    body = new Response(await readFile(file)).body;
  } else {
    const res = await fetch(m);
    if (!res.ok) throw new Error(`the engine's module could not be fetched from ${name}: ${res.status}`);
    body = res.body;
  }
  if (/\.gz$/i.test(new URL(name, "file:///").pathname)) body = body.pipeThrough(new DecompressionStream("gzip"));
  return new Uint8Array(await new Response(body).arrayBuffer());
}

/// Compiled once per source in this context, with the sha256 of its bytes.
const compiled = new Map();
function compile(m) {
  if (m instanceof WebAssembly.Module) return { module: m, sha256: undefined };
  const key = typeof m === "string" || m instanceof URL ? String(m) : undefined;
  if (key && compiled.has(key)) return compiled.get(key);
  const p = (async () => {
    const bytes = await moduleBytes(m);
    return { module: await WebAssembly.compile(bytes), sha256: await sha256(bytes) };
  })();
  if (key) compiled.set(key, p);
  return p;
}

/// What a node leaves out of a nest's inputs (`src/blob.rs`), by name at any depth and at the root.
const NOT_INPUT = ["nuthatch.redb", "segments", ".git", ".DS_Store"];
const NOT_INPUT_AT_ROOT = ["maintained", "quarantine"];

/// A nest's files: given by path, or in Node a directory read as a node reads one for its NID, so
/// the files name the nest a node's `/sql` names (hidden files included).
async function filesOf(nest) {
  if (nest === undefined || typeof nest === "object") return nest;
  if (!ON_NODE) throw new Error("`nest` is a nest's files by path; a directory is read in Node only");
  const { readdirSync, readFileSync } = process.getBuiltinModule("node:fs");
  const out = {};
  const walk = (rel) => {
    for (const e of readdirSync(rel ? `${nest}/${rel}` : nest, { withFileTypes: true })) {
      if (NOT_INPUT.includes(e.name) || e.name === ".admin-token" || e.name.startsWith(".admin-token.")) continue;
      const p = rel ? `${rel}/${e.name}` : e.name;
      if (!rel && NOT_INPUT_AT_ROOT.includes(e.name) && e.isDirectory()) continue;
      if (e.isSymbolicLink()) throw new Error(`${nest}/${p} is a symbolic link; a node refuses one in a nest`);
      if (e.isDirectory()) walk(p);
      else if (e.isFile()) out[p] = readFileSync(`${nest}/${p}`, "utf8");
    }
  };
  walk("");
  return out;
}

/// A storage name for a nest no caller named: one per module, files and what the store holds, so a
/// second open reuses the first and a store is never reopened under other files, filters or engine.
async function nidOf(moduleSha, files, config) {
  const { until, memory_mib, poll_interval_secs, attach, follow, ...held } = config;
  const sorted = Object.keys(files ?? {}).sort().map((p) => [p, files[p]]);
  const text = JSON.stringify([moduleSha ?? null, sorted, held]);
  return `nest-${(await sha256(new TextEncoder().encode(text))).slice(0, 24)}`;
}

/// A nest run in this context: in a worker or in Node.
class Nest {
  static async open(o) {
    if (o.module === undefined) throw new Error("Engine.open needs a `module`");
    const n = new Nest();
    n.options = o;
    n.config = Object.fromEntries(Object.entries(o).filter(([k]) => !OWN.includes(k)));
    if (o.verified) n.config.verified = true;
    const [{ module, sha256: moduleSha }, files] = await Promise.all([compile(o.module), filesOf(o.nest)]);
    if (files) n.config.files = files;
    n.module = module;
    n.module_sha256 = moduleSha;
    if (o.nid === undefined && !moduleSha) throw new Error("a compiled module has no bytes to name a store by: give a `nid`");
    n.nid = o.nid ?? (await nidOf(moduleSha, files, n.config));
    const endpoints = endpointsOf(o.rpc);
    n.endpoints = Object.keys(endpoints);
    n.requests = 0;
    const rpc = rpcFor(endpoints, o.headerBatch);
    n.rpc = (r) => {
      n.requests += 1;
      return rpc(r);
    };
    await n.start(o.fresh ?? false);
    return n;
  }

  async storageFor() {
    const s = this.options.storage ?? (ON_NODE ? "memory" : "opfs");
    if (s === "memory") return { storage: null, mode: "bytes" };
    if (typeof s === "object") return { storage: s, mode: "host" };
    if (s === "opfs") {
      try {
        return { storage: await opfsStorage(this.nid), mode: "host" };
      } catch (e) {
        // No OPFS (a private window, an older browser): the nest still runs, in memory.
        this.log(2, `no OPFS here (${e?.message ?? e}); the nest is kept in memory`);
        return { storage: null, mode: "bytes" };
      }
    }
    if (!ON_NODE) throw new Error(`storage ${JSON.stringify(s)}: in a browser it is "opfs" or "memory"`);
    return { storage: fsStorage(`${s}/${this.nid}`), mode: "host" };
  }

  log(level, line) {
    this.options.log?.(level, line);
  }

  async start(fresh) {
    const { storage, mode } = await this.storageFor();
    this.mode = mode;
    this.fresh = mode === "host" && fresh;
    this.engine = await load(this.module, {
      storage,
      rpc: this.rpc,
      log: (level, line) => this.log(level, line),
      ...(this.options.verified ? { verified: this.options.verified } : {}),
    });
    const { abi } = this.engine.abi();
    if (abi !== ABI) throw new Error(`the module speaks abi ${abi}, and this package abi ${ABI}`);
    this.engine.open({
      ...this.config,
      nid: this.nid,
      rpc: this.endpoints,
      storage: mode,
      ...(mode === "host" ? { fresh: this.fresh } : {}),
    });
  }

  ready() {
    try {
      return JSON.parse(this.engine.request("GET", "/ready").body);
    } catch {
      // Storage `host` opens its store through the first steps; until then there is no cursor.
      return {};
    }
  }

  /// Step until idle, answering every request. A failed follow carries on from the store, and a kept
  /// store the module will not reopen is opened fresh, once.
  async follow() {
    const started = performance.now();
    let last = 0;
    const progress = () => {
      const now = performance.now();
      if (this.options.onProgress && now - last > 500) {
        last = now;
        const r = this.ready();
        this.options.onProgress({ block: r.last_block ?? null, tip: r.tip ?? null, requests: this.requests });
      }
      return false;
    };
    let reopened = false;
    for (let failures = 0; ;) {
      try {
        const s = await this.engine.run(progress);
        const r = this.ready();
        let mirror = null;
        let identity = null;
        try {
          const report = this.engine.report();
          mirror = report.mirror ?? null;
          identity = report.identity ?? null;
        } catch {
          // A probe; a nest that cannot report still answers.
        }
        return {
          nid: this.nid, storage: this.mode === "host" ? "kept" : "memory", block: r.last_block ?? null,
          tip: r.tip ?? null, requests: this.requests, ms: Math.round(performance.now() - started), mirror,
          identity,
          ...(s.unverified ? { unverified: s.unverified } : {}),
        };
      } catch (e) {
        if (!reopened && this.mode === "host" && !this.fresh && ["open_failed", "follow_failed"].includes(e?.fault) && /open it fresh/.test(e.message)) {
          this.log(2, `${e.message}; opening it fresh`);
          this.engine.close();
          await this.start(true);
          reopened = true;
          continue;
        }
        if (e?.fault !== "follow_failed" || failures >= FOLLOW_RETRIES) throw e;
        this.log(2, `${e.message}; carrying on from the store`);
        await rest(2000 * 2 ** failures);
        failures += 1;
      }
    }
  }

  async request(method, target, body = "") {
    return this.engine.request(method, target, body);
  }

  close() {
    this.engine?.close();
    this.engine = undefined;
  }
}

/// What every nest answers, over its `request`.
const Answers = {
  /// `/sql`'s envelope for `q`; with `attach`, other nests' Arrow answers by name and table.
  async sql(q, { attach } = {}) {
    const a = await this.request("POST", "/sql", JSON.stringify(attach ? { q, attach } : { q }));
    if (a.status !== 200) throw Object.assign(new Error(`/sql answered ${a.status}: ${a.body}`), { status: a.status });
    return JSON.parse(a.body);
  },
  /// `/sql`'s Arrow answer for `q`, as another nest's `attach` takes it.
  async arrow(q) {
    const a = await this.request("POST", "/sql", JSON.stringify({ q, format: "arrow" }));
    if (a.status !== 200 || a.body_base64 === undefined) {
      throw Object.assign(new Error(`/sql answered ${a.status}: ${a.body}`), { status: a.status });
    }
    return { arrow: a.body_base64, answer: a.headers["x-nuthatch-answer"] };
  },
};
Object.assign(Nest.prototype, Answers);

/// The page's side of a nest that runs in the package's worker. One worker per module script serves
/// every nest the page opens, and ends when the last one closes.
const workers = new Map();
function workerAt(url) {
  let w = workers.get(url);
  if (w) return w;
  const worker = new Worker(url, { type: "module", name: WORKER_NAME });
  w = { worker, url, next: 0, pending: new Map(), nests: new Map() };
  worker.onmessage = async (m) => {
    const d = m.data;
    const nest = w.nests.get(d.nest);
    if (d.op === "log") return nest?.log?.(d.level, d.line);
    if (d.op === "progress") return nest?.onProgress?.(d.progress);
    if (d.op === "verified") {
      try {
        worker.postMessage({ op: "answer", call: d.call, ok: true, value: await nest.verified() });
      } catch (e) {
        worker.postMessage({ op: "answer", call: d.call, ok: false, error: String(e?.message ?? e) });
      }
      return;
    }
    const p = w.pending.get(d.id);
    if (!p) return;
    w.pending.delete(d.id);
    if (d.ok) p.ok(d.value);
    else p.fail(Object.assign(new Error(d.error), d.fault ? { fault: d.fault } : {}));
  };
  worker.onerror = (e) => {
    for (const p of w.pending.values()) p.fail(new Error(e.message || "the engine's worker stopped"));
    w.pending.clear();
  };
  workers.set(url, w);
  return w;
}

class WorkerNest {
  static async open(o) {
    const url = String(o.workerUrl ?? import.meta.url);
    const w = workerAt(url);
    const n = new WorkerNest();
    n.w = w;
    n.id = `n${w.next++}`;
    w.nests.set(n.id, { log: o.log, onProgress: o.onProgress, verified: o.verified });
    const { log, onProgress, verified, worker, workerUrl, ...sent } = o;
    // A relative URL is the page's; the worker would resolve it against its own script.
    if (typeof sent.module === "string" || sent.module instanceof URL) sent.module = new URL(sent.module, location.href).href;
    try {
      const v = await n.ask({ op: "open", options: sent, log: !!log, progress: !!onProgress, verified: !!verified });
      n.nid = v.nid;
      n.module_sha256 = v.module_sha256;
    } catch (e) {
      n.close();
      throw e;
    }
    return n;
  }

  ask(msg) {
    return new Promise((ok, fail) => {
      const id = this.w.next++;
      this.w.pending.set(id, { ok, fail });
      this.w.worker.postMessage({ ...msg, id, nest: this.id });
    });
  }

  follow() {
    return this.ask({ op: "follow" });
  }

  request(method, target, body = "") {
    return this.ask({ op: "request", method, target, body });
  }

  close() {
    const w = this.w;
    if (!w.nests.delete(this.id)) return;
    w.worker.postMessage({ op: "close", id: -1, nest: this.id });
    if (w.nests.size === 0) {
      // Closing the worker releases every OPFS handle it held.
      w.worker.terminate();
      for (const p of w.pending.values()) p.fail(new Error("closed"));
      workers.delete(w.url);
    }
  }
}
Object.assign(WorkerNest.prototype, Answers);

/// The package's worker: nests run here, each over OPFS, which only a worker can open synchronously.
function serve() {
  const nests = new Map();
  const calls = new Map();
  let next = 0;
  const askPage = (nest, op) =>
    new Promise((ok, fail) => {
      const call = next++;
      calls.set(call, { ok, fail });
      self.postMessage({ op, nest, call });
    });
  self.onmessage = async (m) => {
    const d = m.data;
    if (d.op === "answer") {
      const c = calls.get(d.call);
      calls.delete(d.call);
      return d.ok ? c?.ok(d.value) : c?.fail(new Error(d.error));
    }
    try {
      let value;
      if (d.op === "open") {
        const n = await Nest.open({
          ...d.options,
          ...(d.log ? { log: (level, line) => self.postMessage({ op: "log", nest: d.nest, level, line }) } : {}),
          ...(d.progress ? { onProgress: (progress) => self.postMessage({ op: "progress", nest: d.nest, progress }) } : {}),
          ...(d.verified ? { verified: () => askPage(d.nest, "verified") } : {}),
        });
        nests.set(d.nest, n);
        value = { nid: n.nid, module_sha256: n.module_sha256 };
      } else if (d.op === "close") {
        nests.get(d.nest)?.close();
        nests.delete(d.nest);
        return;
      } else {
        const n = nests.get(d.nest);
        if (!n) throw new Error(`no nest ${d.nest} is open`);
        value = d.op === "follow" ? await n.follow() : await n.request(d.method, d.target, d.body);
      }
      self.postMessage({ id: d.id, ok: true, value });
    } catch (e) {
      self.postMessage({ id: d.id, ok: false, error: String(e?.message ?? e), fault: e?.fault });
    }
  };
}

/// A value as the wire carries it, compared as text, as RFC-0077 S7's page compares a figure.
const asText = (v) => (v == null ? null : typeof v === "string" ? v : typeof v === "object" ? JSON.stringify(v) : String(v));

/// A `/sql` answer as a node or a gateway in front of one sends it: `{rows, provenance}`, or with the
/// rows under `data`. Columns are the answer's own order where it kept one.
function answerOf(a) {
  const body = typeof a === "string" ? JSON.parse(a) : a;
  const rows = body?.rows ?? body?.data?.rows;
  const p = body?.provenance ?? {};
  if (!Array.isArray(rows)) throw new Error("not a /sql answer: it has no rows");
  if (typeof p.nid !== "string" || !/^[0-9a-f]{64}$/i.test(p.nid)) {
    throw new Error("the answer names no nest (`provenance.nid`), so there is no nest to open");
  }
  if (!Number.isSafeInteger(p.as_of)) {
    throw new Error("the answer names no block (`provenance.as_of`), so there is no block to compare at");
  }
  if (body.truncated || p.truncated) throw new Error("the answer was truncated, so it cannot be compared whole");
  const columns = Array.isArray(body.columns) ? body.columns : Object.keys(rows[0] ?? {});
  return { rows, columns, nid: p.nid.toLowerCase(), as_of: p.as_of };
}

/// Rows in one order whatever order each side listed them in, so a comparison is of the rows.
const ordered = (rows, columns) => rows
  .map((r) => columns.map((c) => asText(r[c])))
  .sort((a, b) => (JSON.stringify(a) < JSON.stringify(b) ? -1 : JSON.stringify(a) > JSON.stringify(b) ? 1 : 0));

/// Column by column: equal, differs with the values on each side, or not computable here, named.
function compare(server, here) {
  const both = server.columns.filter((c) => here.columns.includes(c));
  const s = ordered(server.rows, both);
  const h = ordered(here.rows, both);
  const columns = both.map((column, k) => {
    const differs = [];
    for (let i = 0; i < Math.max(s.length, h.length); i++) {
      const a = s[i]?.[k] ?? null;
      const b = h[i]?.[k] ?? null;
      if (a !== b || !s[i] !== !h[i]) differs.push({ row: i, server: a, here: b });
    }
    return differs.length ? { column, verdict: "differs", values: differs.slice(0, 20), rows_differing: differs.length } : { column, verdict: "equal" };
  });
  for (const column of server.columns.filter((c) => !both.includes(c))) {
    columns.push({ column, verdict: "not computable", reason: "this nest's answer has no such column" });
  }
  for (const column of here.columns.filter((c) => !both.includes(c))) {
    columns.push({ column, verdict: "not computable", reason: "the server's answer has no such column" });
  }
  return { rows: { server: server.rows.length, here: here.rows.length }, columns };
}

/// What a check ran under, for the result to say every time (RFC-0081 §7.2).
const hostsOf = (rpc) => Object.values(endpointsOf(rpc)).filter(Boolean).map((u) => {
  try {
    return new URL(u).host;
  } catch {
    return "(given)";
  }
});

/// RFC-0081 S3: hosted answers checked by opening the nest that answered, by its NID, at the block it
/// answered as of, narrowed by the caller's filters. Equality says the server and this RPC agree.
class Verifier {
  constructor(options) {
    if (options.nest === undefined) throw new Error("a verifier needs the nest's files (`nest`), to open the nest an answer names");
    this.options = options;
    this.open = null;
  }

  /// The nest at `as_of` under `filters`, opened once and kept for the next check at that block.
  async nest(nid, as_of, filters) {
    const key = JSON.stringify([nid, as_of, filters]);
    if (this.open?.key === key) return this.open;
    this.close();
    const { module, nest, mirror, rpc, storage, headerBatch, verified, log, onProgress, window, memory_mib, worker, workerUrl } = this.options;
    const source = mirror && filters.every((f) => f.privacy === "public") ? "mirror+rpc" : "rpc";
    const opened = await Engine.open({
      module, nest, rpc, storage, headerBatch, verified, log, onProgress, window, memory_mib, worker, workerUrl,
      filters, until: as_of, expect_nid: nid, source, ...(source === "mirror+rpc" ? { mirror } : {}),
    });
    try {
      const followed = await opened.follow();
      if (followed.block !== as_of) {
        throw new Error(`the nest reached block ${followed.block} and the answer is as of block ${as_of}: the RPC is behind the server`);
      }
      this.open = { key, nest: opened, followed, source };
      return this.open;
    } catch (e) {
      opened.close();
      throw e;
    }
  }

  async sql(nest, q, whole) {
    const r = await nest.request("POST", "/sql", JSON.stringify(whole ? { q, whole } : { q }));
    const body = JSON.parse(r.body);
    if (r.status !== 200) return { error: body.error ?? r.body };
    return { rows: body.rows, columns: body.columns, provenance: body.provenance };
  }

  /// One statement or several, each `{answer, statement, name}`, all as of one block of one nest.
  async verify(answer, statement, filters) {
    const items = Array.isArray(answer) ? answer : [{ answer, statement }];
    if (!Array.isArray(filters) || !filters.length) {
      throw new Error("verify needs the filters the statements are about: a narrowed nest answers only what they cover");
    }
    const parsed = items.map((i) => ({ ...i, server: answerOf(i.answer) }));
    const { nid, as_of } = parsed[0].server;
    for (const p of parsed) {
      if (p.server.nid !== nid || p.server.as_of !== as_of) {
        throw new Error(`answers from nest ${p.server.nid} at block ${p.server.as_of} and ${nid} at ${as_of}: check each nest and block apart`);
      }
    }
    const { nest, followed, source } = await this.nest(nid, as_of, filters);
    const results = [];
    for (const [k, p] of parsed.entries()) {
      const name = p.name ?? `statement ${k + 1}`;
      const here = await this.sql(nest, p.statement);
      if (here.error) {
        results.push({ name, verdict: "not computable", reason: `this nest refused the statement: ${here.error}` });
        continue;
      }
      if (here.provenance?.as_of !== as_of) {
        results.push({ name, verdict: "not computable", reason: `this nest answered as of ${here.provenance?.as_of}, not ${as_of}` });
        continue;
      }
      const outside = await this.outside(nest, p.statement, here, followed);
      if (outside) {
        results.push({ name, verdict: "outside the filters", reason: outside });
        continue;
      }
      const c = compare(p.server, { rows: here.rows, columns: here.columns ?? Object.keys(here.rows[0] ?? {}) });
      const verdict = c.columns.some((x) => x.verdict === "differs") || c.rows.server !== c.rows.here ? "differs" : "equal";
      results.push({ name, verdict, read: here.provenance?.narrowed?.read ?? null, ...c });
    }
    return {
      nid, as_of, source, filters, dataset: followed.identity?.dataset ?? null,
      rpc: hostsOf(this.options.rpc),
      head: this.options.verified ? "verified" : "the RPC's, unverified",
      mirror: followed.mirror
        ? { base: this.options.mirror.base, ...followed.mirror }
        : null,
      requests: followed.requests,
      statements: results,
    };
  }

  /// Why a statement reads outside the filters, or null. Two rules: it names a table no filter
  /// covers, which this nest holds none of; or, where the tables came whole from a mirror, its answer
  /// over every hydrated row differs from its answer through the filters.
  async outside(nest, q, here, followed) {
    const n = here.provenance?.narrowed;
    if (n?.unfiltered?.length) return `it reads ${n.unfiltered.join(", ")}, which no filter covers`;
    if (!followed.mirror) return null;
    const whole = await this.sql(nest, q, true);
    if (whole.error) return `its answer over the whole hydrated tables could not be read: ${whole.error}`;
    const cols = here.columns ?? Object.keys(here.rows[0] ?? {});
    if (JSON.stringify(ordered(whole.rows, cols)) !== JSON.stringify(ordered(here.rows, cols))) {
      return `its answer changes when the rows outside the filters (the published dataset's, through block ${followed.mirror.complete_through}) are read`;
    }
    return null;
  }

  close() {
    this.open?.nest.close();
    this.open = null;
  }
}

export const Engine = {
  /// Open a nest. In a page it runs in the package's worker; in a worker or in Node, here.
  open(options) {
    const page = !ON_NODE && typeof document !== "undefined";
    const inWorker = page && options.worker !== false && typeof options.storage !== "object";
    return inWorker ? WorkerNest.open(options) : Nest.open(options);
  },
  /// A checker of hosted answers (RFC-0081 §3.4): `verifier(options).verify(answer, statement, filters)`.
  verifier(options) {
    return new Verifier(options);
  },
};

if (typeof WorkerGlobalScope !== "undefined" && self instanceof WorkerGlobalScope && self.name === WORKER_NAME) serve();
