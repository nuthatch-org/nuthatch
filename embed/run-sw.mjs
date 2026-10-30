// RFC-0075 S5's gate in a browser, and RFC-0077 S2's Chrome leg: `hosts/browser/index.html` runs the
// plain module through `abi: 1` in a dedicated worker, its store in OPFS, and every request as a
// plain same-origin `fetch`, answered by the Service Worker (`hosts/browser/sw.js`). Its answers must
// equal the node's pinned bytes, the scaffolded skill's examples and llms.txt's routes are run as
// written against the page, and the module the page ran must hash to the file given here.
// Usage: node run-sw.mjs <chrome> <module.wasm> <rpc-url> <tab-parity-dir>
//   (fixture-rpc recorded --fork serving <rpc-url>)
import { createServer } from "node:http";
import { spawn } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import { tmpdir } from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { answersDigest, differences, mask, sameTarget } from "./mask.mjs";

const [chrome, wasm, rpc, dir] = process.argv.slice(2);
const module_sha256 = createHash("sha256").update(readFileSync(wasm)).digest("hex");
const here = path.dirname(new URL(import.meta.url).pathname);
const chain = JSON.parse(gunzipSync(readFileSync(path.join(dir, "chain.json.gz"))));
const expected = JSON.parse(readFileSync(path.join(dir, "s5-expected.json")));
const start = Number(BigInt(chain.headers[0][0])) + 1;
const roots = { browser: path.join(here, "hosts", "browser"), fixture: dir };
const types = { ".html": "text/html", ".js": "text/javascript", ".mjs": "text/javascript", ".wasm": "application/wasm" };

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
  const [, root, ...rest] = url.pathname.split("/");
  const file = root === "module" ? wasm : roots[root] && path.join(roots[root], ...rest);
  try {
    const bytes = readFileSync(file);
    res.writeHead(200, { "content-type": types[path.extname(file)] || "application/octet-stream" });
    res.end(bytes);
  } catch {
    res.writeHead(404);
    res.end();
  }
});
await new Promise((r) => server.listen(0, "127.0.0.1", r));
const origin = `http://127.0.0.1:${server.address().port}`;
const page = `${origin}/browser/index.html?rpc=${encodeURIComponent(rpc)}&contract=${chain.contract}&start=${start}&module=../module/plain.wasm`;

const profile = mkdtempSync(path.join(tmpdir(), "nest-sw-"));
const browser = spawn(chrome, [
  "--headless=new", "--no-first-run", "--no-default-browser-check", "--disable-gpu", "--enable-logging=stderr",
  // A CI runner's AppArmor refuses Chrome's sandbox its user namespace.
  ...(process.env.CI ? ["--no-sandbox"] : []),
  `--user-data-dir=${profile}`, page,
],{ stdio: ["ignore", "ignore", process.env.CHROME_LOG ? "inherit" : "ignore"] });
const timeout = setTimeout(() => done({ ok: false, error: "no result within 300 s" }), 300_000);
const r = await result;
clearTimeout(timeout);
const exited = new Promise((r) => browser.once("exit", r));
browser.kill();
await exited;
server.close();
rmSync(profile, { recursive: true, force: true, maxRetries: 5 });

if (!r.ok) {
  console.error(`the page failed: ${r.error}`);
  process.exit(1);
}
const failures = differences(r.answers, expected);
if (r.opened.module_sha256 !== module_sha256) {
  failures.push(`the page ran a module hashing to ${r.opened.module_sha256}, not ${wasm}'s ${module_sha256}`);
}

// The skill's examples, as written, must answer what the node answers for the same request.
const skill = r.examples.filter((e) => e.source === "SKILL.md");
if (skill.length === 0) failures.push("no `curl` example found in SKILL.md");
for (const e of skill) {
  const want = expected.find((x) => x.method === "GET" && sameTarget(x.path, e.path));
  if (!want) failures.push(`SKILL.md's \`curl ${e.curl}\` has no pinned node answer to compare with`);
  else if (e.status !== want.status || mask(e.body) !== want.body) {
    failures.push(`SKILL.md's \`curl ${e.curl}\` answered ${e.status}, not the node's bytes`);
  }
}
const llms = r.examples.filter((e) => e.source === "llms.txt");
console.log(JSON.stringify({
  host: "chrome", gate: "s5", storage: "host", module_sha256: r.opened.module_sha256,
  answers_sha256: answersDigest(r.answers), pin_sha256: answersDigest(expected, { pinned: true }),
  follow_ms: r.follow_ms,
  opened: r.opened,
  answers: r.answers.length,
  skill_examples: skill.map((e) => `${e.path} ${e.status}`),
  llms_routes_served: llms.filter((e) => e.served).map((e) => e.route),
  llms_routes_not_served: llms.filter((e) => e.runnable !== false && !e.served).map((e) => e.route),
  llms_routes_with_placeholders: llms.filter((e) => e.runnable === false).map((e) => e.route),
}));
if (failures.length) {
  for (const f of failures) console.error(`MISMATCH ${f}`);
  process.exit(1);
}
console.log(`gate: all ${r.answers.length} answers through the Service Worker byte-identical to the node's, and the skill's examples with them`);
