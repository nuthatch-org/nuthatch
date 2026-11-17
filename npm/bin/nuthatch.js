#!/usr/bin/env node
"use strict";

// Runs the nuthatch release binary matching this package's version. On first run it downloads the
// release tarball from GitHub, checks it against the release's .sha256 (and against the digests
// bundled at publish time, when present), and caches the binary. Nothing runs at install time.
//
// Everything this script prints goes to stderr: under `nuthatch mcp`, stdout is the MCP channel.

const { spawn, spawnSync } = require("node:child_process");
const crypto = require("node:crypto");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const pkg = require("../package.json");
const REPO = "https://github.com/nuthatch-org/nuthatch";

function fail(message) {
  process.stderr.write(`nuthatch (npm): ${message}\n`);
  process.exit(1);
}

function isMusl() {
  if (process.platform !== "linux") return false;
  const report = process.report && process.report.getReport();
  return !(report && report.header && report.header.glibcVersionRuntime);
}

function target() {
  const key = `${process.platform}-${process.arch}`;
  switch (key) {
    case "darwin-arm64":
      return "aarch64-apple-darwin";
    case "darwin-x64":
      return "x86_64-apple-darwin";
    case "linux-x64":
      return isMusl() ? "x86_64-unknown-linux-musl" : "x86_64-unknown-linux-gnu";
    case "linux-arm64":
      if (isMusl()) fail("there is no release binary for musl on arm64; build from source: " + REPO);
      return "aarch64-unknown-linux-gnu";
    default:
      fail(`there is no release binary for ${key}; build from source: ${REPO}#install`);
  }
}

function cacheRoot() {
  if (process.env.NUTHATCH_NPM_CACHE) return process.env.NUTHATCH_NPM_CACHE;
  if (process.platform === "darwin") return path.join(os.homedir(), "Library", "Caches", "nuthatch", "npm");
  const xdg = process.env.XDG_CACHE_HOME || path.join(os.homedir(), ".cache");
  return path.join(xdg, "nuthatch", "npm");
}

async function get(url) {
  let res;
  try {
    res = await fetch(url, { redirect: "follow" });
  } catch (e) {
    fail(`could not fetch ${url}: ${e.cause ? e.cause.message : e.message}`);
  }
  if (!res.ok) fail(`could not fetch ${url}: HTTP ${res.status}`);
  return Buffer.from(await res.arrayBuffer());
}

function bundledDigest(asset) {
  try {
    return require("../checksums.json")[asset];
  } catch {
    return undefined;
  }
}

async function install(version, triple, dest) {
  const base = process.env.NUTHATCH_NPM_RELEASE_URL || `${REPO}/releases/download`;
  const asset = `nuthatch-${triple}.tar.gz`;
  const url = `${base}/v${version}/${asset}`;
  process.stderr.write(`nuthatch (npm): downloading ${url}\n`);

  const [tarball, sidecar] = await Promise.all([get(url), get(`${url}.sha256`)]);
  const published = sidecar.toString("utf8").trim().split(/\s+/)[0].toLowerCase();
  const actual = crypto.createHash("sha256").update(tarball).digest("hex");
  if (!/^[0-9a-f]{64}$/.test(published)) fail(`${url}.sha256 holds no SHA-256 digest`);
  if (actual !== published) fail(`${asset} is ${actual}, but its .sha256 says ${published}; refusing to run it`);
  const bundled = bundledDigest(asset);
  if (bundled && bundled !== actual) {
    fail(`${asset} is ${actual}, but this npm package was published with ${bundled}; refusing to run it`);
  }

  const dir = path.dirname(dest);
  fs.mkdirSync(dir, { recursive: true });
  const work = fs.mkdtempSync(path.join(dir, ".partial-"));
  try {
    const archive = path.join(work, asset);
    fs.writeFileSync(archive, tarball);
    const tar = spawnSync("tar", ["-xzf", archive, "-C", work, "nuthatch"], { stdio: ["ignore", "ignore", "pipe"] });
    if (tar.error) fail(`could not run tar to unpack ${asset}: ${tar.error.message}`);
    if (tar.status !== 0) fail(`could not unpack ${asset}: ${tar.stderr.toString().trim()}`);
    fs.chmodSync(path.join(work, "nuthatch"), 0o755);
    fs.renameSync(path.join(work, "nuthatch"), dest);
  } finally {
    fs.rmSync(work, { recursive: true, force: true });
  }
  process.stderr.write(`nuthatch (npm): verified sha256 ${actual}, cached at ${dest}\n`);
}

async function main() {
  const triple = target();
  const dest = path.join(cacheRoot(), pkg.version, triple, "nuthatch");
  if (!fs.existsSync(dest)) await install(pkg.version, triple, dest);

  const child = spawn(dest, process.argv.slice(2), { stdio: "inherit" });
  for (const sig of ["SIGINT", "SIGTERM", "SIGHUP"]) {
    process.on(sig, () => child.kill(sig));
  }
  child.on("error", (e) => fail(`could not start ${dest}: ${e.message}`));
  child.on("exit", (code, signal) => {
    if (signal) {
      process.removeAllListeners(signal);
      process.kill(process.pid, signal);
    } else {
      process.exit(code);
    }
  });
}

main();
