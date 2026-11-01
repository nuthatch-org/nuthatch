// RFC-0075 S5's gate in a browser, and RFC-0077 S2's browser legs: `hosts/browser/index.html` runs the
// plain module through `abi: 1` in a dedicated worker, its store in OPFS, and every request as a
// plain same-origin `fetch`, answered by the Service Worker (`hosts/browser/sw.js`). Its answers must
// equal the node's pinned bytes, the scaffolded skill's examples and llms.txt's routes are run as
// written against the page, and the module the page ran must hash to the file given here.
// With `--restart` (RFC-0077 S3) the browser first follows from an empty store until a segment is
// sealed, is killed there with SIGKILL, and is started again on the same profile, which reopens the
// same OPFS. With `shapes` (RFC-0081 S5), `hosts/browser/shapes.html` follows RFC-0077 S5's nest shapes
// instead, each against its own fixture-rpc, and every era's answers must equal the node's pin.
// Chrome is launched directly; with `--geckodriver`, the browser is Firefox under geckodriver.
// Usage: node run-sw.mjs <browser> <module.wasm> <rpc-url> <tab-parity-dir> [--restart] [--geckodriver <path>]
//          (fixture-rpc recorded --fork serving <rpc-url>)
//        node run-sw.mjs <browser> <module.wasm> shapes <fixture-rpc> <embed-shapes-dir> [--geckodriver <path>]
import { createServer } from "node:http";
import { createServer as createNetServer } from "node:net";
import { spawn } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import { tmpdir } from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { answersDigest, differences, mask, sameTarget, segmentsDigest } from "./mask.mjs";
import { nestFiles } from "./mirror.mjs";

const args = process.argv.slice(2);
const flagAt = args.indexOf("--geckodriver");
const geckodriver = flagAt < 0 ? undefined : args.splice(flagAt, 2)[1];
const [browserPath, wasm, rpc, dir, ...flags] = args;
const host = geckodriver ? "firefox" : "chrome";
const shapes = rpc === "shapes";
const restart = flags.includes("--restart");
const module_sha256 = createHash("sha256").update(readFileSync(wasm)).digest("hex");
const here = path.dirname(new URL(import.meta.url).pathname);
const roots = { browser: path.join(here, "hosts", "browser"), fixture: shapes ? undefined : dir };
const types = { ".html": "text/html", ".js": "text/javascript", ".mjs": "text/javascript", ".wasm": "application/wasm" };

const freePort = () => new Promise((ok) => {
  const s = createNetServer().listen(0, "127.0.0.1", () => {
    const { port } = s.address();
    s.close(() => ok(port));
  });
});

// `shapes`: one fixture-rpc a shape, each on its own recording, as `hosts/node/shapes.mjs` runs them.
const fixtures = [];
let shapeList;
if (shapes) {
  const [fixtureRpc, shapesDir] = [dir, flags[0]];
  shapeList = [];
  for (const name of JSON.parse(readFileSync(path.join(shapesDir, "shapes.json")))) {
    const d = path.join(shapesDir, name);
    const shape = JSON.parse(readFileSync(path.join(d, "shape.json")));
    const port = await freePort();
    const child = spawn(fixtureRpc, ["recorded", "--port", String(port), "--file", path.join(d, "tape.json.gz")],
      { stdio: ["ignore", "pipe", "inherit"] });
    fixtures.push(child);
    await new Promise((ok, fail) => {
      child.stdout.once("data", ok);
      child.once("exit", (c) => fail(new Error(`fixture-rpc exited ${c}`)));
    });
    shapeList.push({
      name, rpc: `http://127.0.0.1:${port}/`, files: nestFiles(path.join(d, shape.nest)),
      requests: JSON.parse(readFileSync(path.join(d, "requests.json"))),
      expected: JSON.parse(readFileSync(path.join(d, "expected.json"))),
    });
  }
}

let done, result;
const server = createServer((req, res) => {
  const url = new URL(req.url, "http://x/");
  if (req.method === "POST" && url.pathname === "/result") {
    let body = "";
    req.on("data", (c) => { body += c; });
    req.on("end", () => { res.end("ok"); done(JSON.parse(body)); });
    return;
  }
  if (shapes && url.pathname === "/shapes") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify(shapeList.map(({ expected, ...s }) => ({ ...s, eras: expected.length }))));
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
// One origin for both launches, so the second sees the first's OPFS.
const origin = `http://127.0.0.1:${server.address().port}`;
let page;
if (shapes) {
  page = `${origin}/browser/shapes.html?module=../module/plain.wasm`;
} else {
  const chain = JSON.parse(gunzipSync(readFileSync(path.join(dir, "chain.json.gz"))));
  const start = Number(BigInt(chain.headers[0][0])) + 1;
  page = `${origin}/browser/index.html?rpc=${encodeURIComponent(rpc)}&contract=${chain.contract}&start=${start}&module=../module/plain.wasm`;
}
const profile = mkdtempSync(path.join(tmpdir(), "nest-sw-"));
const log = process.env.BROWSER_LOG ?? process.env.CHROME_LOG;

function chrome(url) {
  const browser = spawn(browserPath, [
    "--headless=new", "--no-first-run", "--no-default-browser-check", "--disable-gpu", "--enable-logging=stderr",
    // A CI runner's AppArmor refuses Chrome's sandbox its user namespace.
    ...(process.env.CI ? ["--no-sandbox"] : []),
    `--user-data-dir=${profile}`, url,
  ], { stdio: ["ignore", "ignore", log ? "inherit" : "ignore"] });
  const exited = new Promise((r) => browser.once("exit", r));
  return { stop: async (signal) => { browser.kill(signal); await exited; } };
}

/// Firefox through geckodriver's WebDriver session, on `profile` itself rather than a copy, so a
/// second launch reopens the first one's OPFS. A kill is SIGKILL to the browser's own process.
async function firefox(url) {
  const port = await freePort();
  const driver = spawn(geckodriver, ["--port", String(port)], { stdio: ["ignore", log ? "inherit" : "ignore", log ? "inherit" : "ignore"] });
  const driverExited = new Promise((r) => driver.once("exit", r));
  const wd = async (method, at, body) => {
    const r = await fetch(`http://127.0.0.1:${port}${at}`, {
      method, headers: { "content-type": "application/json" }, body: body && JSON.stringify(body),
    });
    return (await r.json()).value;
  };
  for (let i = 0; ; i++) {
    try {
      if ((await wd("GET", "/status")).ready !== undefined) break;
    } catch (e) {
      if (i > 100) throw new Error(`geckodriver did not answer: ${e}`);
    }
    await new Promise((r) => setTimeout(r, 100));
  }
  const session = await wd("POST", "/session", {
    capabilities: {
      alwaysMatch: {
        browserName: "firefox",
        pageLoadStrategy: "none",
        "moz:firefoxOptions": { binary: browserPath, args: ["-headless", "-profile", profile] },
      },
    },
  });
  if (!session.sessionId) throw new Error(`geckodriver refused the session: ${JSON.stringify(session)}`);
  const pid = session.capabilities["moz:processID"];
  await wd("POST", `/session/${session.sessionId}/url`, { url });
  return {
    stop: async (signal) => {
      if (signal === "SIGKILL") process.kill(pid, "SIGKILL");
      else await wd("DELETE", `/session/${session.sessionId}`).catch(() => {});
      driver.kill();
      await driverExited;
    },
  };
}

/// The browser on `url` until the page posts its result, then stopped by `signal`.
async function launch(url, signal) {
  result = new Promise((resolve) => { done = resolve; });
  const browser = geckodriver ? await firefox(url) : chrome(url);
  const secs = shapes ? 900 : 300;
  const timeout = setTimeout(() => done({ ok: false, error: `no result within ${secs} s` }), secs * 1000);
  const r = await result;
  clearTimeout(timeout);
  await browser.stop(signal);
  return r;
}

let killed;
if (restart) {
  const d = await launch(`${page}&phase=die`, "SIGKILL");
  if (!d.ok) {
    console.error(`the follow to kill failed: ${d.error}`);
    process.exit(1);
  }
  killed = { last_block: d.stopped.last_block, sealed_through: d.stopped.sealed_through };
  // The browser's helper processes outlive it by a moment; they hold the profile's OPFS.
  await new Promise((r) => setTimeout(r, 2000));
}
const r = await launch(restart ? `${page}&fresh=0` : page, "SIGTERM");
server.close();
for (const f of fixtures) f.kill();
rmSync(profile, { recursive: true, force: true, maxRetries: 5 });

if (!r.ok) {
  console.error(`the page failed: ${r.error}`);
  process.exit(1);
}

if (shapes) {
  const results = [];
  let failed = 0;
  for (const s of shapeList) {
    const got = r.results.find((x) => x.shape === s.name);
    const failures = [];
    if (!got) failures.push("the page did not run it");
    else {
      if (got.error) failures.push(got.error);
      if (got.module_sha256 !== module_sha256) failures.push(`the page ran a module hashing to ${got.module_sha256}`);
      if (got.eras.length !== s.expected.length) failures.push(`${got.eras.length} eras answered of ${s.expected.length}`);
      for (const [era, answers] of got.eras.entries()) {
        if (answersDigest(answers) !== answersDigest(s.expected[era], { pinned: true })) {
          failures.push(...differences(answers, s.expected[era]).map((d) => `era ${era}: ${d}`));
        }
      }
    }
    if (failures.length) failed++;
    results.push({ shape: s.name, eras: s.expected.length, answers: s.requests.length, ok: !failures.length,
      ms: got?.ms, digests: got?.eras.map((a) => answersDigest(a)) });
    console.log(`${failures.length ? "FAIL" : "ok"} - ${s.name}: ${s.expected.length} eras x ${s.requests.length} answers`);
    for (const f of failures) console.error(`MISMATCH ${s.name} ${f}`);
  }
  console.log(JSON.stringify({ host, gate: "shapes", storage: "host", module_sha256, results }));
  if (failed) {
    console.error(`RESULT: FAIL (${failed} of ${shapeList.length} shapes in ${host})`);
    process.exit(1);
  }
  console.log(`RESULT: PASS (${shapeList.length} shapes equal to the node's, in ${host} on OPFS)`);
  process.exit(0);
}

const expected = JSON.parse(readFileSync(path.join(dir, "s5-expected.json")));
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
  host, gate: "s5", storage: "host", ...(restart ? { restart: true, killed_at: killed } : {}),
  module_sha256: r.opened.module_sha256, segments_sha256: segmentsDigest(r.opened.segments),
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
const after = restart ? `, after a kill at block ${killed.last_block}, sealed through ${killed.sealed_through}` : "";
console.log(`gate: all ${r.answers.length} answers through the Service Worker byte-identical to the node's, and the skill's examples with them${after}`);
