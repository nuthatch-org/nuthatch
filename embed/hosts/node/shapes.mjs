// RFC-0077 S5's parity gate, the module's half: every nest shape `tests/e2e_embed_shapes.rs` writes
// out of the binary's tests, followed era by era through the Node host over the same recording, its
// answers after each era equal to the node's pin. fixture-rpc serves each recording; `POST
// /control/fork` moves it to the next era, as the node's half does.
// Usage: node shapes.mjs <module.wasm> <fixture-rpc> <embed-shapes-dir> [shape...]
import { readFileSync } from "node:fs";
import { spawn } from "node:child_process";
import { createServer } from "node:net";
import path from "node:path";
import { fetchRpc, load } from "../browser/host.js";
import { nestFiles } from "../../mirror.mjs";
import { answersDigest, differences } from "../../mask.mjs";

const [wasm, fixtureRpc, dir, ...only] = process.argv.slice(2);
if (!dir) {
  console.error("usage: shapes.mjs <module.wasm> <fixture-rpc> <embed-shapes-dir> [shape...]");
  process.exit(2);
}
const bytes = readFileSync(wasm);
const names = only.length ? only : JSON.parse(readFileSync(path.join(dir, "shapes.json")));

const freePort = () => new Promise((ok) => {
  const s = createServer().listen(0, "127.0.0.1", () => {
    const { port } = s.address();
    s.close(() => ok(port));
  });
});

async function serve(tape) {
  const port = await freePort();
  const child = spawn(fixtureRpc, ["recorded", "--port", String(port), "--file", tape], { stdio: ["ignore", "pipe", "inherit"] });
  await new Promise((ok, fail) => {
    child.stdout.once("data", ok);
    child.once("exit", (c) => fail(new Error(`fixture-rpc exited ${c}`)));
  });
  return { url: `http://127.0.0.1:${port}/`, stop: () => child.kill() };
}

const results = [];
let failed = 0;
for (const name of names) {
  const shapeDir = path.join(dir, name);
  const shape = JSON.parse(readFileSync(path.join(shapeDir, "shape.json")));
  const requests = JSON.parse(readFileSync(path.join(shapeDir, "requests.json")));
  const expected = JSON.parse(readFileSync(path.join(shapeDir, "expected.json")));
  const files = nestFiles(path.join(shapeDir, shape.nest));
  const rpc = await serve(path.join(shapeDir, "tape.json.gz"));
  const t = performance.now();
  const failures = [];
  const digests = [];
  try {
    const engine = await load(bytes, {
      storage: null,
      rpc: fetchRpc({ main: rpc.url }),
      log: (level, s) => level < 2 && console.error(`guest[${level}]: ${s}`),
    });
    engine.open({ nid: name, rpc: ["main"], files, poll_interval_secs: 1 });
    for (const [era, want] of expected.entries()) {
      if (era > 0) await fetch(`${rpc.url}control/fork`, { method: "POST" });
      await engine.run();
      const got = requests.map((req) => {
        const a = engine.request(req.method, req.path, req.body ?? "");
        return { method: req.method, path: req.path, status: a.status, content_type: a.content_type, body: a.body };
      });
      digests.push(answersDigest(got));
      if (answersDigest(got) !== answersDigest(want, { pinned: true })) {
        failures.push(...differences(got, want).map((d) => `era ${era}: ${d}`));
      }
    }
  } catch (e) {
    failures.push(String(e?.stack ?? e));
  } finally {
    rpc.stop();
  }
  if (failures.length) failed++;
  results.push({ shape: name, eras: expected.length, answers: requests.length, ok: !failures.length,
    ms: Math.round(performance.now() - t), digests });
  console.log(`${failures.length ? "FAIL" : "ok"} - ${name}: ${expected.length} eras x ${requests.length} answers`);
  for (const f of failures) console.error(`MISMATCH ${name} ${f}`);
}
console.log(JSON.stringify({ module_bytes: bytes.length, peak_rss_mib: +(process.resourceUsage().maxRSS / 1024).toFixed(1), results }));
if (failed) {
  console.error(`RESULT: FAIL (${failed} of ${names.length} shapes)`);
  process.exit(1);
}
console.log(`RESULT: PASS (${names.length} shapes equal to the node's)`);
