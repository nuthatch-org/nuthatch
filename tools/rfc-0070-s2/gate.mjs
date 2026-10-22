// RFC-0070 S2's gates, in headless Chrome over the DevTools protocol.
//   node gate.mjs tab <spec-index> <out-dir>   replay a mirrored spec in the page, export, record peak memory
//   node gate.mjs load <recording-dir> <out-dir>   load a native recording through the page, export it
//   node gate.mjs offline <file.html>          open an export with networking off, play every frame, count requests
// CHROME names the browser; the default is Playwright's headless shell.
import { spawn, execFileSync } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";

const here = path.dirname(fileURLToPath(import.meta.url));
const CHROME = process.env.CHROME ?? path.join(os.homedir(), "Library/Caches/ms-playwright/chromium_headless_shell-1243/chrome-headless-shell-mac-arm64/chrome-headless-shell");
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

export async function launch(extra) {
  const profile = fs.mkdtempSync(path.join(os.tmpdir(), "s2-gate-"));
  const proc = spawn(CHROME, ["--headless=new", "--disable-gpu", "--no-first-run", "--remote-debugging-port=0", `--user-data-dir=${profile}`, ...extra, "about:blank"], { stdio: "ignore" });
  const portFile = path.join(profile, "DevToolsActivePort");
  for (let i = 0; i < 100 && !fs.existsSync(portFile); i++) await wait(100);
  const port = fs.readFileSync(portFile, "utf8").split("\n")[0];
  const pages = await (await fetch(`http://127.0.0.1:${port}/json/list`)).json();
  const ws = new WebSocket(pages.find((p) => p.type === "page").webSocketDebuggerUrl);
  await new Promise((r, j) => { ws.onopen = r; ws.onerror = j; });
  let id = 0;
  const pending = new Map();
  const listeners = [];
  ws.onmessage = ({ data }) => {
    const m = JSON.parse(data);
    if (m.id && pending.has(m.id)) { const { r, j } = pending.get(m.id); pending.delete(m.id); m.error ? j(new Error(m.error.message)) : r(m.result); }
    else if (m.method) for (const l of listeners) l(m);
  };
  const send = (method, params = {}) => new Promise((r, j) => { pending.set(++id, { r, j }); ws.send(JSON.stringify({ id, method, params })); });
  const evaluate = async (expression) => {
    const r = await send("Runtime.evaluate", { expression, awaitPromise: true, returnByValue: true });
    if (r.exceptionDetails) throw new Error(r.exceptionDetails.exception?.description ?? r.exceptionDetails.text);
    return r.result.value;
  };
  const exited = new Promise((r) => proc.once("exit", r));
  const close = async () => { ws.close(); proc.kill(); await exited; fs.rmSync(profile, { recursive: true, force: true, maxRetries: 5 }); };
  return { proc, send, evaluate, on: (f) => listeners.push(f), close };
}

// Resident memory of the browser and every process under it, by `ps`, in MiB.
function rss(root) {
  const rows = execFileSync("ps", ["-A", "-o", "pid=,ppid=,rss="], { encoding: "utf8" }).trim().split("\n").map((l) => l.trim().split(/\s+/).map(Number));
  const kids = new Map();
  for (const [pid, ppid] of rows) kids.set(ppid, [...(kids.get(ppid) ?? []), pid]);
  const mine = new Set([root]);
  for (const p of mine) for (const k of kids.get(p) ?? []) mine.add(k);
  const sizes = rows.filter(([pid]) => mine.has(pid)).map(([, , kb]) => kb / 1024);
  return { total: sizes.reduce((a, b) => a + b, 0), largest: Math.max(0, ...sizes) };
}

export function serve(root) {
  const types = { ".html": "text/html", ".js": "text/javascript", ".json": "application/json", ".wasm": "application/wasm", ".toml": "text/plain" };
  const server = http.createServer((req, res) => {
    const p = path.join(root, decodeURIComponent(new URL(req.url, "http://x").pathname));
    if (!p.startsWith(root) || !fs.existsSync(p) || fs.statSync(p).isDirectory()) { res.writeHead(404).end(); return; }
    res.writeHead(200, { "content-type": types[path.extname(p)] ?? "application/octet-stream" });
    fs.createReadStream(p).pipe(res);
  });
  return new Promise((r) => server.listen(0, "127.0.0.1", () => r(server)));
}

/// The frames an export embeds, and their SHA-256, read from the file as the player reads them.
export function embedded(html) {
  const m = /<script id="frames" type="application\/json">(.*?)<\/script>/s.exec(html);
  const jsonl = JSON.parse(m[1]);
  return { jsonl, sha256: createHash("sha256").update(jsonl).digest("hex"), frames: jsonl.split("\n").filter(Boolean).length };
}

async function tab(specIndex, out) {
  fs.mkdirSync(out, { recursive: true });
  const server = await serve(path.join(here, "web"));
  const b = await launch([]);
  try {
    await b.send("Browser.setDownloadBehavior", { behavior: "allow", downloadPath: path.resolve(out) }).catch(() =>
      b.send("Page.setDownloadBehavior", { behavior: "allow", downloadPath: path.resolve(out) }));
    await b.send("Page.enable");
    await b.send("Page.navigate", { url: `http://127.0.0.1:${server.address().port}/timelapse.html` });
    for (let i = 0; i < 100 && !(await b.evaluate(`document.getElementById("spec-pick")?.options.length > 0 && document.getElementById("spec").value.length > 0`).catch(() => false)); i++) await wait(100);
    await b.evaluate(`(async () => { const s = document.getElementById("spec-pick"); s.value = "${specIndex}"; s.onchange(); await new Promise((r) => setTimeout(r, 500)); })()`);
    const spec = await b.evaluate(`document.getElementById("spec").value`);
    const base = rss(b.proc.pid);
    let peak = { ...base };
    await b.evaluate(`document.getElementById("run").click()`);
    const t0 = Date.now();
    let state;
    for (;;) {
      await wait(250);
      const r = rss(b.proc.pid);
      peak = { total: Math.max(peak.total, r.total), largest: Math.max(peak.largest, r.largest) };
      state = await b.evaluate(`({ status: document.getElementById("run-status").textContent, err: document.getElementById("run-status").className.includes("err"), facts: document.getElementById("facts").innerText, exportReady: !document.getElementById("export").disabled })`);
      if (state.err || state.exportReady) break;
      if (Date.now() - t0 > 30 * 60_000) throw new Error("the replay ran past 30 minutes");
    }
    const result = { spec_index: Number(specIndex), wall_secs: (Date.now() - t0) / 1e3, status: state.status, facts: state.facts, rss_before_mib: base, rss_peak_mib: peak };
    if (!state.err) {
      const before = new Set(fs.readdirSync(out));
      await b.evaluate(`document.getElementById("export").click()`);
      let file;
      for (let i = 0; i < 100 && !file; i++) { await wait(100); file = fs.readdirSync(out).find((f) => !before.has(f) && f.endsWith(".html")); }
      if (!file) throw new Error("the export did not arrive");
      await wait(300);
      result.export = path.resolve(out, file);
      result.embedded = (({ sha256, frames }) => ({ sha256, frames }))(embedded(fs.readFileSync(result.export, "utf8")));
      fs.writeFileSync(path.join(out, `spec-${specIndex}.toml`), spec);
    }
    console.log(JSON.stringify(result, null, 1));
    return result;
  } finally {
    await b.close();
    server.close();
  }
}

async function load(dir, out) {
  fs.mkdirSync(out, { recursive: true });
  const server = await serve(path.join(here, "web"));
  const b = await launch([]);
  try {
    await b.send("Browser.setDownloadBehavior", { behavior: "allow", downloadPath: path.resolve(out) });
    await b.send("Page.enable");
    await b.send("Page.navigate", { url: `http://127.0.0.1:${server.address().port}/timelapse.html` });
    for (let i = 0; i < 100 && !(await b.evaluate(`document.getElementById("spec").value.length > 0`).catch(() => false)); i++) await wait(100);
    const { root } = await b.send("DOM.getDocument");
    const { nodeId } = await b.send("DOM.querySelector", { nodeId: root.nodeId, selector: "#files" });
    await b.send("DOM.setFileInputFiles", { nodeId, files: ["frames.jsonl", "manifest.json"].map((f) => path.resolve(dir, f)) });
    let s;
    for (let i = 0; i < 100; i++) {
      await wait(100);
      s = await b.evaluate(`({ status: document.getElementById("load-status").textContent, ready: !document.getElementById("export").disabled })`);
      if (s.ready || s.status) break;
    }
    if (!s.ready) throw new Error(`the page refused the recording: ${s.status}`);
    const before = new Set(fs.readdirSync(out));
    await b.evaluate(`document.getElementById("export").click()`);
    let file;
    for (let i = 0; i < 100 && !file; i++) { await wait(100); file = fs.readdirSync(out).find((f) => !before.has(f) && f.endsWith(".html")); }
    if (!file) throw new Error("the export did not arrive");
    await wait(300);
    const recorded = JSON.parse(fs.readFileSync(path.join(dir, "manifest.json"), "utf8")).frames_sha256;
    const got = embedded(fs.readFileSync(path.join(out, file), "utf8"));
    const result = { export: path.resolve(out, file), status: s.status, recorded, embedded: got.sha256, frames: got.frames, pass: got.sha256 === recorded };
    console.log(JSON.stringify(result, null, 1));
    return result;
  } finally {
    await b.close();
    server.close();
  }
}

async function offline(file) {
  const abs = path.resolve(file);
  const want = embedded(fs.readFileSync(abs, "utf8"));
  const b = await launch(["--host-resolver-rules=MAP * ~NOTFOUND"]);
  const requests = [];
  b.on((m) => { if (m.method === "Network.requestWillBeSent") requests.push(m.params.request.url); });
  try {
    await b.send("Network.enable");
    await b.send("Network.emulateNetworkConditions", { offline: true, latency: 0, downloadThroughput: -1, uploadThroughput: -1 });
    await b.send("Page.enable");
    // Every frame index the player draws, collected from data-frame as it changes.
    await b.send("Page.addScriptToEvaluateOnNewDocument", { source: `window.__seen = new Set(); new MutationObserver((rs) => { for (const r of rs) if (r.oldValue !== null && r.oldValue !== "") window.__seen.add(r.oldValue); }).observe(document, { subtree: true, attributes: true, attributeFilter: ["data-frame"], attributeOldValue: true });` });
    const url = `file://${abs}`;
    await b.send("Page.navigate", { url });
    for (let i = 0; i < 100 && (await b.evaluate(`document.readyState`).catch(() => "")) !== "complete"; i++) await wait(100);
    await b.evaluate(`(() => { const s = document.getElementById("speed"); s.value = "120"; s.dispatchEvent(new Event("change")); document.getElementById("play").click(); })()`);
    const t0 = Date.now();
    let s;
    for (;;) {
      await wait(500);
      s = await b.evaluate(`({ frame: document.getElementById("player").dataset.frame, label: document.getElementById("play").textContent, frameno: document.getElementById("frameno").textContent, sha2: document.getElementById("sha2").textContent })`);
      if (s.label === "Play" && Number(s.frame) === want.frames - 1) break;
      if (Date.now() - t0 > 10 * 60_000) throw new Error(`playback stalled at ${s.frameno}`);
    }
    const seen = await b.evaluate(`(() => { const s = new Set(window.__seen); s.add(document.getElementById("player").dataset.frame); return [...s].map(Number).sort((a, b) => a - b); })()`);
    const every = seen.length === want.frames && seen.every((v, i) => v === i);
    const others = requests.filter((u) => u !== url);
    const result = {
      file: abs, frames: want.frames, frames_drawn: seen.length, every_frame: every, play_secs: (Date.now() - t0) / 1e3,
      requests_other_than_the_document: others.length, request_urls: others, in_page_hash: s.sha2, embedded_sha256: want.sha256,
      pass: every && others.length === 0 && s.sha2.startsWith(want.sha256) && s.sha2.includes("matches"),
    };
    console.log(JSON.stringify(result, null, 1));
    return result;
  } finally {
    await b.close();
  }
}

const [mode, ...args] = import.meta.main ? process.argv.slice(2) : [];
if (mode === "tab") await tab(args[0], args[1]);
else if (mode === "load") { const r = await load(args[0], args[1]); process.exit(r.pass ? 0 : 1); }
else if (mode === "offline") { const r = await offline(args[0]); process.exit(r.pass ? 0 : 1); }
else if (mode !== undefined) throw new Error("usage: node gate.mjs tab <spec-index> <out-dir> | load <recording-dir> <out-dir> | offline <file.html>");
