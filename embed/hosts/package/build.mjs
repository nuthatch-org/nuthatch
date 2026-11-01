// RFC-0081 S2: bundles the host package into one ES module, `nuthatch-engine.js`, from `engine.js` and
// the reference hosts it imports, their code copied as it is. Nothing is fetched and no dependency is
// installed: the files are joined in order, their relative imports dropped and their exports made
// local, and a `node:` default import becomes a lazy one so a browser never asks for it. The output
// names the version and the sha256 of every source, and is the same bytes on every machine.
// Usage: node build.mjs <out.js> [--version x.y.z]
import { readFileSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import path from "node:path";

const here = path.dirname(new URL(import.meta.url).pathname);
const args = process.argv.slice(2);
const at = args.indexOf("--version");
const version = at < 0
  ? readFileSync(path.join(here, "../../../Cargo.toml"), "utf8").match(/^version = "(.*)"$/m)[1]
  : args.splice(at, 2)[1];
const [out] = args;
if (!out) {
  console.error("usage: build.mjs <out.js> [--version x.y.z]");
  process.exit(2);
}

// Each file before anything that uses it; `engine.js` last, and the only one whose exports stay.
const SOURCES = ["../browser/host.js", "../browser/opfs.js", "../node/fs-storage.mjs", "engine.js"];

const parts = SOURCES.map((rel) => {
  const text = readFileSync(path.join(here, rel), "utf8");
  const entry = rel === "engine.js";
  const lines = text.split("\n").map((line) => {
    if (/^import .* from "\.\.?\/.*";$/.test(line)) return null;
    const node = line.match(/^import (\w+) from "(node:[\w/]+)";$/);
    if (node) {
      return `const ${node[1]} = ON_NODE_ ? (await import(/* webpackIgnore: true */ /* @vite-ignore */ "${node[2]}")).default : undefined;`;
    }
    if (/^import /.test(line)) throw new Error(`${rel}: an import the bundle cannot place: ${line}`);
    if (entry) return line;
    if (/^export \{/.test(line)) throw new Error(`${rel}: an export list the bundle cannot place: ${line}`);
    return line.replace(/^export (?=(async )?(function|class|const|let) )/, "");
  });
  const body = lines.filter((l) => l !== null).join("\n").trimEnd();
  return { rel, sha256: createHash("sha256").update(text).digest("hex"), body };
});

const bundle = [
  `// nuthatch-engine.js ${version}: the host package for nuthatch's embeddable engine (RFC-0081 §3.3),`,
  "// for nuthatch-events.wasm of the same release. MIT OR Apache-2.0. Built by",
  "// embed/hosts/package/build.mjs from:",
  ...parts.map((p) => `//   embed/hosts/${path.normalize(path.join("package", p.rel))} ${p.sha256}`),
  "",
  `export const VERSION = ${JSON.stringify(version)};`,
  'const ON_NODE_ = typeof process !== "undefined" && !!process.versions?.node;',
  ...parts.map((p) => `\n// ---- ${path.normalize(path.join("package", p.rel))} ----\n${p.body}`),
  "",
].join("\n");
writeFileSync(out, bundle);
console.log(JSON.stringify({ out, version, bytes: Buffer.byteLength(bundle), sha256: createHash("sha256").update(bundle).digest("hex") }));
