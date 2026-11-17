"use strict";

const assert = require("node:assert/strict");
const { execFileSync, spawn } = require("node:child_process");
const crypto = require("node:crypto");
const fs = require("node:fs");
const http = require("node:http");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");

const PKG = path.join(__dirname, "..");
const VERSION = require("../package.json").version;

// A stand-in release: a tarball holding a `nuthatch` that echoes its arguments and exits 3.
function fakeRelease() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "nuthatch-npm-release-"));
  fs.writeFileSync(path.join(dir, "nuthatch"), '#!/bin/sh\necho "fake nuthatch $*"\nexit 3\n', { mode: 0o755 });
  execFileSync("tar", ["-czf", path.join(dir, "release.tar.gz"), "-C", dir, "nuthatch"]);
  const tarball = fs.readFileSync(path.join(dir, "release.tar.gz"));
  return { tarball, digest: crypto.createHash("sha256").update(tarball).digest("hex") };
}

function serve(tarball, sidecar) {
  const hits = [];
  const server = http.createServer((req, res) => {
    hits.push(req.url);
    const m = req.url.match(/^\/v([^/]+)\/(nuthatch-[a-z0-9_-]+\.tar\.gz)(\.sha256)?$/);
    if (!m || m[1] !== VERSION) {
      res.writeHead(404).end();
      return;
    }
    res.writeHead(200).end(m[3] ? `${sidecar}  ${m[2]}\n` : tarball);
  });
  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => resolve({ server, hits, url: `http://127.0.0.1:${server.address().port}` }));
  });
}

function run(pkgDir, env, args) {
  return new Promise((resolve) => {
    const child = spawn(process.execPath, [path.join(pkgDir, "bin", "nuthatch.js"), ...args], {
      env: { ...process.env, ...env },
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (d) => (stdout += d));
    child.stderr.on("data", (d) => (stderr += d));
    child.on("exit", (code) => resolve({ code, stdout, stderr }));
  });
}

function cachedBinaries(cache) {
  if (!fs.existsSync(cache)) return [];
  return fs.readdirSync(cache, { recursive: true }).filter((p) => path.basename(p) === "nuthatch");
}

test("downloads once, verifies, runs the binary and passes its exit code through", async (t) => {
  const { tarball, digest } = fakeRelease();
  const { server, hits, url } = await serve(tarball, digest);
  t.after(() => server.close());
  const cache = fs.mkdtempSync(path.join(os.tmpdir(), "nuthatch-npm-cache-"));
  const env = { NUTHATCH_NPM_RELEASE_URL: url, NUTHATCH_NPM_CACHE: cache };

  const first = await run(PKG, env, ["mcp", "--url", "http://127.0.0.1:8288"]);
  assert.equal(first.code, 3, first.stderr);
  assert.equal(first.stdout, "fake nuthatch mcp --url http://127.0.0.1:8288\n");
  assert.match(first.stderr, new RegExp(`verified sha256 ${digest}`));
  assert.equal(hits.length, 2);

  const second = await run(PKG, env, ["--version"]);
  assert.equal(second.stdout, "fake nuthatch --version\n");
  assert.equal(second.stderr, "");
  assert.equal(hits.length, 2, "the cached binary is reused");
});

test("refuses a tarball that does not match its .sha256, and caches nothing", async (t) => {
  const { tarball } = fakeRelease();
  const { server, url } = await serve(tarball, "0".repeat(64));
  t.after(() => server.close());
  const cache = fs.mkdtempSync(path.join(os.tmpdir(), "nuthatch-npm-cache-"));

  const r = await run(PKG, { NUTHATCH_NPM_RELEASE_URL: url, NUTHATCH_NPM_CACHE: cache }, ["--version"]);
  assert.equal(r.code, 1);
  assert.equal(r.stdout, "");
  assert.match(r.stderr, /refusing to run it/);
  assert.deepEqual(cachedBinaries(cache), []);
});

test("refuses a release whose tarball and .sha256 both changed after the package was published", async (t) => {
  const { tarball, digest } = fakeRelease();
  const { server, url } = await serve(tarball, digest);
  t.after(() => server.close());
  const published = fs.mkdtempSync(path.join(os.tmpdir(), "nuthatch-npm-pkg-"));
  fs.mkdirSync(path.join(published, "bin"));
  fs.copyFileSync(path.join(PKG, "bin", "nuthatch.js"), path.join(published, "bin", "nuthatch.js"));
  fs.copyFileSync(path.join(PKG, "package.json"), path.join(published, "package.json"));
  const assets = ["aarch64-apple-darwin", "x86_64-apple-darwin", "x86_64-unknown-linux-gnu", "x86_64-unknown-linux-musl", "aarch64-unknown-linux-gnu"];
  fs.writeFileSync(
    path.join(published, "checksums.json"),
    JSON.stringify(Object.fromEntries(assets.map((t) => [`nuthatch-${t}.tar.gz`, "1".repeat(64)]))),
  );
  const cache = fs.mkdtempSync(path.join(os.tmpdir(), "nuthatch-npm-cache-"));

  const r = await run(published, { NUTHATCH_NPM_RELEASE_URL: url, NUTHATCH_NPM_CACHE: cache }, ["--version"]);
  assert.equal(r.code, 1);
  assert.match(r.stderr, /published with 1{64}/);
  assert.deepEqual(cachedBinaries(cache), []);
});

test("checksums.js pins every target the launcher maps to, and refuses a release missing one", () => {
  const sums = fs.mkdtempSync(path.join(os.tmpdir(), "nuthatch-npm-sums-"));
  const script = path.join(PKG, "scripts", "checksums.js");
  const targets = ["aarch64-apple-darwin", "x86_64-apple-darwin", "x86_64-unknown-linux-gnu", "x86_64-unknown-linux-musl"];
  for (const [i, t] of targets.entries()) {
    fs.writeFileSync(path.join(sums, `nuthatch-${t}.tar.gz.sha256`), `${String(i).repeat(64)}  nuthatch-${t}.tar.gz\n`);
  }
  assert.throws(() => execFileSync(process.execPath, [script, sums], { stdio: "pipe" }), /aarch64-unknown-linux-gnu/);

  fs.writeFileSync(path.join(sums, "nuthatch-aarch64-unknown-linux-gnu.tar.gz.sha256"), `${"f".repeat(64)}  x\n`);
  fs.writeFileSync(path.join(sums, "nuthatch-graph-aarch64-apple-darwin.tar.gz.sha256"), `${"e".repeat(64)}  x\n`);
  const out = JSON.parse(execFileSync(process.execPath, [script, sums]));
  assert.equal(Object.keys(out).length, 5);
  assert.equal(out["nuthatch-aarch64-unknown-linux-gnu.tar.gz"], "f".repeat(64));
  assert.equal(out["nuthatch-aarch64-apple-darwin.tar.gz"], "0".repeat(64));
});

test("says plainly when the release is not there", async (t) => {
  const { tarball, digest } = fakeRelease();
  const { server, url } = await serve(tarball, digest);
  t.after(() => server.close());
  const cache = fs.mkdtempSync(path.join(os.tmpdir(), "nuthatch-npm-cache-"));

  const r = await run(PKG, { NUTHATCH_NPM_RELEASE_URL: `${url}/missing`, NUTHATCH_NPM_CACHE: cache }, []);
  assert.equal(r.code, 1);
  assert.match(r.stderr, /HTTP 404/);
});
