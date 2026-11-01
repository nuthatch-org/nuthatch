// RFC-0081 S2's gates for the host package's Node half, run on the bundle as released:
//   s5     RFC-0075 S5's 32 answers through `Engine.open`, `follow` and `request`, equal to the node's
//          pin; then the same nest opened again from the store it kept, the same answers again.
//   reuse  RFC-0077 S4's entity nest from its local mirror, closed and opened again: the second open
//          asks the mirror for nothing and answers the same; a pack with one byte altered is refused
//          by the module and the package opens the nest fresh, which fetches the pack again.
// Prints one JSON line per open, then the verdict; exits non-zero on a mismatch.
// Usage: node run.mjs <nuthatch-engine.js> <module.wasm> s5 <rpc-url> <tab-parity-dir>
//        node run.mjs <nuthatch-engine.js> <module.wasm> reuse <s4-nest dir> <s4-mirror dir>
import { mkdtempSync, openSync, readFileSync, readdirSync, readSync, rmSync, writeSync, closeSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { answersDigest, differences } from "../../mask.mjs";
import { answersOf, digest, nestFiles, serveDirectory } from "../../mirror.mjs";

const [bundle, wasm, gate, a, b] = process.argv.slice(2);
if (!bundle || !wasm || !["s5", "reuse"].includes(gate) || !a || !b) {
  console.error("usage: run.mjs <nuthatch-engine.js> <module.wasm> <s5 <rpc> <tab-parity-dir> | reuse <nest> <mirror>>");
  process.exit(2);
}
const { Engine, VERSION } = await import(path.resolve(bundle));
const storage = mkdtempSync(path.join(tmpdir(), "nest-package-"));
const failures = [];

// Every GET, so a reopen can be shown to have fetched nothing.
const gets = [];
const realFetch = globalThis.fetch;
globalThis.fetch = (url, init) => {
  if (!init?.method || init.method === "GET") gets.push(String(url));
  return realFetch(url, init);
};

async function opened(options, label) {
  const before = gets.length;
  const nest = await Engine.open({ module: wasm, storage, log: (l, s) => l < 2 && console.error(`guest[${l}]: ${s}`), ...options });
  const followed = await nest.follow();
  console.log(JSON.stringify({ package: VERSION, open: label, ...followed, gets: gets.length - before }));
  return { nest, followed, gets: gets.length - before };
}

if (gate === "s5") {
  const dir = b;
  const nestDir = path.join(dir, "s4");
  const files = {};
  for (const sub of ["entities", "views"]) {
    for (const f of readdirSync(path.join(nestDir, sub)).sort()) files[`${sub}/${f}`] = readFileSync(path.join(nestDir, sub, f), "utf8");
  }
  files["entities.toml"] = readFileSync(path.join(nestDir, "entities.toml"), "utf8");
  const { gunzipSync } = await import("node:zlib");
  const chain = JSON.parse(gunzipSync(readFileSync(path.join(dir, "chain.json.gz"))));
  const options = { nest: files, rpc: a, contract: chain.contract, start: Number(BigInt(chain.headers[0][0])) + 1, poll_interval_secs: 1 };
  const requests = JSON.parse(readFileSync(path.join(dir, "s5", "requests.json")));
  const expected = JSON.parse(readFileSync(path.join(dir, "s5-expected.json")));
  const pin = answersDigest(expected, { pinned: true });
  for (const label of ["fresh", "kept"]) {
    const { nest, followed } = await opened(options, label);
    const answers = [];
    for (const req of requests) {
      const r = await nest.request(req.method, req.path, req.body ?? "");
      answers.push({ method: req.method, path: req.path, status: r.status, content_type: r.content_type, body: r.body });
    }
    nest.close();
    const got = answersDigest(answers);
    console.log(JSON.stringify({ open: label, nid: followed.nid, answers: answers.length, answers_sha256: got, pin_sha256: pin }));
    failures.push(...differences(answers, expected).map((d) => `${label}: ${d}`));
    if (got !== pin) failures.push(`${label}: answers ${got}, pin ${pin}`);
  }
  if (!failures.length) console.log(`gate: the package answers all ${requests.length} of S5's requests equal to the node's pin (${pin}), opened fresh and from its kept store`);
} else {
  const { server, base } = await serveDirectory(path.resolve(b));
  const files = nestFiles(a);
  const options = { nest: a, mirror: { base }, follow: false };
  const answer = async (nest) => digest(await answersOf(files, (target) => nest.request("GET", target)));
  const first = await opened(options, "first");
  const want = await answer(first.nest);
  first.nest.close();
  const second = await opened(options, "second");
  const again = await answer(second.nest);
  second.nest.close();
  if (first.followed.nid !== second.followed.nid) failures.push("the second open used another store");
  if (!first.followed.mirror || first.followed.mirror.reused) failures.push("the first open did not hydrate");
  if (!second.followed.mirror?.reused) failures.push("the second open did not reuse the pack");
  if (second.gets !== 0) failures.push(`the second open asked the mirror ${second.gets} times`);
  if (again !== want) failures.push(`the second open answered ${again}, the first ${want}`);
  // One byte of the kept pack altered: the module refuses it, and the package opens the nest fresh.
  const pack = path.join(storage, first.followed.nid, "mirror.pack");
  const fd = openSync(pack, "r+");
  const byte = Buffer.alloc(1);
  readSync(fd, byte, 0, 1, 1000);
  byte[0] ^= 0xff;
  writeSync(fd, byte, 0, 1, 1000);
  closeSync(fd);
  const third = await opened(options, "altered");
  const after = await answer(third.nest);
  third.nest.close();
  if (third.followed.mirror?.reused !== false || third.gets === 0) failures.push("an altered pack was not fetched again");
  if (after !== want) failures.push(`after an altered pack the nest answered ${after}, before ${want}`);
  server.close();
  if (!failures.length) {
    console.log(`gate: reopened from its pack with no GET and the same answers (${want}); an altered pack fetched again (${third.gets} GETs)`);
  }
}
rmSync(storage, { recursive: true, force: true });
if (failures.length) {
  for (const f of failures) console.error(`MISMATCH ${f}`);
  process.exit(1);
}
