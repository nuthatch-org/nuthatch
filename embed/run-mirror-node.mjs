// RFC-0077 S4's gate, the node's half: a copy of the nest seeded from the mirror with `nuthatch seed`,
// then served without indexing (`serve`) or, with `--rpc`, followed to the chain's tip (`dev`), and
// asked `mirror.mjs`'s requests. Writes the report `hosts/node/mirror.mjs --expect` compares with.
// Usage: node run-mirror-node.mjs <nuthatch> <nest-dir> <mirror: url or directory> --out <report.json>
//          [--rpc <url> --tip <block>]
import { cpSync, mkdtempSync, openSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { spawn } from "node:child_process";
import { tmpdir } from "node:os";
import path from "node:path";
import { answersOf, digest, nestFiles, serveDirectory } from "./mirror.mjs";

const [bin, nestDir, mirrorArg, ...flags] = process.argv.slice(2);
const flag = (name) => {
  const at = flags.indexOf(name);
  return at < 0 ? undefined : flags[at + 1];
};
if (!bin || !nestDir || !mirrorArg || !flag("--out")) {
  console.error("usage: run-mirror-node.mjs <nuthatch> <nest-dir> <mirror> --out <report.json> [--rpc <url> --tip <block>]");
  process.exit(2);
}
let base = mirrorArg;
let served;
if (!/^https?:\/\//.test(mirrorArg)) {
  served = await serveDirectory(path.resolve(mirrorArg));
  base = served.base;
}
const work = mkdtempSync(path.join(tmpdir(), "nest-mirror-node-"));
const dir = path.join(work, "nest");
cpSync(nestDir, dir, {
  recursive: true,
  filter: (p) => !["nuthatch.redb", "segments", ".git"].includes(path.basename(p)),
});

const t0 = performance.now();
// Not spawnSync: the mirror may be served from this process.
const seeded = await new Promise((resolve) => {
  const child = spawn(bin, ["seed", "--dir", dir, "--from", base]);
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (d) => (stdout += d));
  child.stderr.on("data", (d) => (stderr += d));
  child.on("exit", (status) => resolve({ status, stdout, stderr }));
});
if (seeded.status !== 0) {
  console.error(seeded.stdout, seeded.stderr);
  process.exit(1);
}
const ms_seed = performance.now() - t0;
console.error(seeded.stdout.trim());

const rpc = flag("--rpc");
// Port 0, read back from the log: a port picked here can be taken before the node binds it.
const listen = ["--listen", "127.0.0.1:0"];
const args = rpc
  ? ["dev", "--dir", dir, ...listen, "--rpc", rpc, "--poll-interval", "1s"]
  : ["serve", "--dir", dir, ...listen];
const logPath = path.join(work, "node.log");
const log = openSync(logPath, "w");
const node = spawn(bin, args, { stdio: ["ignore", log, log] });
let exited;
node.on("exit", (code, signal) => (exited = signal ?? code));
const stop = (code, why) => {
  if (why) console.error(`${why}\n--- node.log ---\n${readFileSync(logPath, "utf8")}`);
  node.kill("SIGKILL");
  served?.server.close();
  if (code === 0) rmSync(work, { recursive: true, force: true });
  else console.error(`kept ${work}`);
  process.exit(code);
};

const deadline = Date.now() + 300_000;
let api;
while (!api) {
  if (exited !== undefined) stop(1, `the node exited (${exited}) before it was listening`);
  if (Date.now() > deadline) stop(1, "the node never said where it was listening");
  api = readFileSync(logPath, "utf8").match(/API live on (http:\/\/127\.0\.0\.1:[1-9]\d*)/)?.[1];
  await new Promise((r) => setTimeout(r, 100));
}

const get = async (target) => {
  const res = await fetch(api + target, { signal: AbortSignal.timeout(30_000) });
  return { status: res.status, body: await res.text() };
};
const want = flag("--tip") ? Number(flag("--tip")) : undefined;
for (;;) {
  if (exited !== undefined) stop(1, `the node exited (${exited}) before it answered`);
  try {
    const p = JSON.parse((await get(`/sql?q=${encodeURIComponent("SELECT 1")}`)).body).provenance;
    if (want === undefined || p?.as_of === want) break;
  } catch {}
  if (Date.now() > deadline) stop(1, "the node never answered at the head it was asked for");
  await new Promise((r) => setTimeout(r, 250));
}

const files = nestFiles(dir);
const answers = await answersOf(files, get);
const head = answers[`/sql?q=${encodeURIComponent("SELECT 1")}`].body.provenance.as_of;
writeFileSync(flag("--out"), JSON.stringify({ head, answers }, null, 1));
console.log(JSON.stringify({ head, ms_seed: Math.round(ms_seed), answers: Object.keys(answers).length, answers_sha256: digest(answers) }));
stop(0);
