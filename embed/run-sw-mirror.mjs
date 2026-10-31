// RFC-0077 S4's gate in a browser: `hosts/browser/mirror.html` opens the nest at <nest-dir> from the
// mirror at <mirror-dir>, both served on the page's origin, in OPFS through `abi: 1`, and answers
// `mirror-answers.mjs`'s requests through the Service Worker. They must equal the report
// `run-mirror-node.mjs` wrote from a node seeded from the same mirror. With `--refused`, the page must
// instead be refused before it fetches anything from the mirror.
// <mirror-dir> may instead be a published mirror's URL.
// Usage: node run-sw-mirror.mjs <chrome> <module.wasm> <nest-dir> <mirror-dir or URL>
//          (--expect <report.json> [--rpc <url>] | --refused --memory <mib> --max-rows <n>)
import { createServer } from "node:http";
import { spawn } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { differences, digest, nestFiles } from "./mirror.mjs";

const [chrome, wasm, nestDir, mirrorDir, ...flags] = process.argv.slice(2);
const flag = (name) => {
  const at = flags.indexOf(name);
  return at < 0 ? undefined : flags[at + 1];
};
const refused = flags.includes("--refused");
const here = path.dirname(new URL(import.meta.url).pathname);
const module_sha256 = createHash("sha256").update(readFileSync(wasm)).digest("hex");
const files = nestFiles(nestDir);
if (flag("--max-rows")) {
  files["entities.toml"] = files["entities.toml"].replace(/max_rows\s*=\s*\d+/g, `max_rows = ${flag("--max-rows")}`);
}
const types = { ".html": "text/html", ".js": "text/javascript", ".mjs": "text/javascript", ".wasm": "application/wasm" };
const mirrorRoot = /^https?:\/\//.test(mirrorDir) ? undefined : path.resolve(mirrorDir);
let mirrorGets = 0;

let done;
const result = new Promise((resolve) => { done = resolve; });
const server = createServer((req, res) => {
  const url = new URL(req.url, "http://x/");
  if (req.method === "POST" && url.pathname === "/result") {
    let body = "";
    req.on("data", (c) => { body += c; });
    req.on("end", () => { res.end("ok"); done(JSON.parse(body)); });
    return;
  }
  const [, root, ...rest] = url.pathname.split("/").map(decodeURIComponent);
  if (rest.includes("..")) {
    res.writeHead(400).end();
    return;
  }
  let file;
  if (root === "nestfiles.json") {
    res.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify(files));
    return;
  }
  if (root === "module") file = wasm;
  else if (root === "browser") file = path.join(here, "hosts", "browser", ...rest);
  else if (root === "embed" && rest.join("/") === "mirror-answers.mjs") file = path.join(here, "mirror-answers.mjs");
  else if (root === "mirror" && mirrorRoot) {
    mirrorGets += 1;
    file = path.join(mirrorRoot, ...rest);
  }
  try {
    if (!file || !statSync(file).isFile()) throw new Error();
    res.writeHead(200, { "content-type": types[path.extname(file)] || "application/octet-stream" });
    res.end(readFileSync(file));
  } catch {
    res.writeHead(404).end();
  }
});
await new Promise((r) => server.listen(0, "127.0.0.1", r));
const origin = `http://127.0.0.1:${server.address().port}`;
const query = new URLSearchParams({ module: "../module/plain.wasm" });
if (flag("--rpc")) query.set("rpc", flag("--rpc"));
if (flag("--memory")) query.set("memory", flag("--memory"));
// A published mirror elsewhere, which must answer a page on another origin (CORS).
if (/^https?:\/\//.test(mirrorDir)) query.set("base", mirrorDir);
const profile = mkdtempSync(path.join(tmpdir(), "nest-sw-mirror-"));
const browser = spawn(chrome, [
  "--headless=new", "--no-first-run", "--no-default-browser-check", "--disable-gpu",
  // A CI runner's AppArmor refuses Chrome's sandbox its user namespace.
  ...(process.env.CI ? ["--no-sandbox"] : []),
  `--user-data-dir=${profile}`, `${origin}/browser/mirror.html?${query}`,
], { stdio: ["ignore", "ignore", process.env.CHROME_LOG ? "inherit" : "ignore"] });
const exited = new Promise((r) => browser.once("exit", r));
const timeout = setTimeout(() => done({ ok: false, error: "no result within 300 s" }), 300_000);
const r = await result;
clearTimeout(timeout);
browser.kill("SIGTERM");
await exited;
server.close();
rmSync(profile, { recursive: true, force: true, maxRetries: 5 });

if (!r.ok) {
  console.error(`the page failed: ${r.error}`);
  process.exit(1);
}
if (refused) {
  console.log(JSON.stringify({ host: "chrome", gate: "s4-refused", refused: r.refused, mirror_gets: mirrorGets }));
  if (!r.refused || mirrorGets > 0) {
    console.error(r.refused ? `refused, but after ${mirrorGets} GET(s) to the mirror` : "the nest was not refused");
    process.exit(1);
  }
  console.log("gate: refused before anything was downloaded");
  process.exit(0);
}
if (r.refused) {
  console.error(`the nest was refused: ${r.refused}`);
  process.exit(1);
}
const want = JSON.parse(readFileSync(flag("--expect"), "utf8"));
const failures = differences(want.answers, r.answers).map((k) => `differs: ${k}`);
if (r.opened.module_sha256 !== module_sha256) failures.push(`the page ran another module than ${wasm}`);
if (r.opened.last_block !== want.head) failures.push(`the page is at ${r.opened.last_block} and the node at ${want.head}`);
console.log(JSON.stringify({
  host: "chrome", gate: "s4", module_sha256: r.opened.module_sha256, mirror: r.opened.mirror,
  last_block: r.opened.last_block, open_ms: Math.round(r.open_ms), answers: Object.keys(r.answers).length,
  answers_sha256: digest(r.answers), expected_sha256: digest(want.answers), mirror_gets: mirrorGets,
}));
if (failures.length) {
  for (const f of failures) console.error(`MISMATCH ${f}`);
  process.exit(1);
}
console.log(`gate: all ${Object.keys(r.answers).length} answers through the Service Worker equal the node's`);
