// RFC-0077 S4's gate in Node, shared by its two halves: a nest's files read as a host hands them to
// the module, a mirror served from a directory, and a postcard made from one. `hosts/node/mirror.mjs`
// asks the module; `run-mirror-node.mjs` asks a node seeded from the same mirror. What they compare
// is `mirror-answers.mjs`.
import { createReadStream, readdirSync, readFileSync, statSync } from "node:fs";
import { createHash } from "node:crypto";
import { createServer } from "node:http";
import path from "node:path";
import { canon } from "./mirror-answers.mjs";

export { answersOf, comparable, differences } from "./mirror-answers.mjs";

/// Every authored file of the nest at `dir`, by `/`-separated path, as text. Hidden files and what a
/// node writes beside them (its store and segments) are not the nest's.
export function nestFiles(dir) {
  const out = {};
  const walk = (rel) => {
    for (const e of readdirSync(path.join(dir, rel), { withFileTypes: true })) {
      if (e.name.startsWith(".")) continue;
      const p = rel ? `${rel}/${e.name}` : e.name;
      if (!rel && ["nuthatch.redb", "segments"].includes(e.name)) continue;
      if (e.isDirectory()) walk(p);
      else if (e.isFile()) out[p] = readFileSync(path.join(dir, p), "utf8");
    }
  };
  walk("");
  return out;
}

export const digest = (answers) => createHash("sha256").update(canon(answers)).digest("hex");

/// A postcard for the catalogue `base` publishes as it stands, as `/play` makes one (RFC-0068 §3,
/// `postcard.js` in burrmill-wasm): each table's highest sealed `to_block`, and the digest over
/// `schema.json` and the sorted entries of the cut.
export async function postcardOf(base, dataset) {
  const get = async (k) => {
    const r = await fetch(`${base}/${dataset}/${k}`);
    if (r.status !== 200) throw new Error(`${k}: ${r.status}`);
    return new Uint8Array(await r.arrayBuffer());
  };
  const manifest = JSON.parse(new TextDecoder().decode(await get("manifest.json")));
  const schema = await get("schema.json");
  const cuts = {};
  const lines = [];
  for (const [t, es] of Object.entries(manifest.tables)) {
    const sealed = es.filter((e) => !e.provisional);
    if (!sealed.length) continue;
    cuts[t] = Math.max(...sealed.map((e) => e.to_block));
    for (const e of sealed) lines.push(`${t} ${e.hash} ${e.from_block} ${e.to_block} ${e.rows ?? "-"}\n`);
  }
  const h = createHash("sha256").update(schema).update(lines.sort().join(""));
  return { cuts, digest: h.digest("base64url") };
}

/// A published directory served over HTTP on loopback, as a static host serves a mirror. Answers the
/// server and its base URL.
export async function serveDirectory(root) {
  const server = createServer((req, res) => {
    const rel = decodeURIComponent(new URL(req.url, "http://x").pathname).replace(/^\/+/, "");
    const p = path.join(root, rel);
    if (rel.split("/").includes("..") || !p.startsWith(root)) {
      res.writeHead(400).end();
      return;
    }
    let st;
    try {
      st = statSync(p);
    } catch {
      res.writeHead(404).end();
      return;
    }
    if (!st.isFile()) {
      res.writeHead(404).end();
      return;
    }
    res.writeHead(200, { "content-length": st.size, "access-control-allow-origin": "*" });
    createReadStream(p).pipe(res);
  });
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  return { server, base: `http://127.0.0.1:${server.address().port}` };
}
