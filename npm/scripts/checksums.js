#!/usr/bin/env node
"use strict";

// Prints checksums.json for the package from a directory of a release's `.sha256` files, so the
// published package pins the digests the release had when it was published.
//
//   node scripts/checksums.js <dir-of-sha256-files> > checksums.json

const fs = require("node:fs");
const path = require("node:path");

const TARGETS = [
  "aarch64-apple-darwin",
  "x86_64-apple-darwin",
  "x86_64-unknown-linux-gnu",
  "x86_64-unknown-linux-musl",
  "aarch64-unknown-linux-gnu",
];

const dir = process.argv[2];
if (!dir) {
  process.stderr.write("usage: checksums.js <dir-of-sha256-files>\n");
  process.exit(2);
}

const out = {};
for (const t of TARGETS) {
  const asset = `nuthatch-${t}.tar.gz`;
  const file = path.join(dir, `${asset}.sha256`);
  if (!fs.existsSync(file)) {
    process.stderr.write(`missing ${file}: every target the launcher maps to must be in the release\n`);
    process.exit(1);
  }
  const digest = fs.readFileSync(file, "utf8").trim().split(/\s+/)[0].toLowerCase();
  if (!/^[0-9a-f]{64}$/.test(digest)) {
    process.stderr.write(`${file} holds no SHA-256 digest\n`);
    process.exit(1);
  }
  out[asset] = digest;
}
process.stdout.write(`${JSON.stringify(out, null, 2)}\n`);
