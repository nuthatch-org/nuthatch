// A recording made natively, exported as the page exports one.
//   node export.mjs <recording-dir> <out.html> [--decimals n] [--unit s] [--noun s]
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { fill, checkRecording } from "./web/fill.js";

const [dir, out, ...rest] = process.argv.slice(2);
if (!dir || !out) throw new Error("usage: node export.mjs <recording-dir> <out.html> [--decimals n] [--unit s] [--noun s]");
const display = { decimals: 0, unit: "", noun: "keys" };
for (let i = 0; i < rest.length; i += 2) {
  const k = rest[i].replace(/^--/, "");
  if (!(k in display) || rest[i + 1] === undefined) throw new Error(`unknown flag ${rest[i]}`);
  display[k] = k === "decimals" ? Number(rest[i + 1]) : rest[i + 1];
}
const here = path.dirname(fileURLToPath(import.meta.url));
const jsonl = fs.readFileSync(path.join(dir, "frames.jsonl"), "utf8");
const manifest = await checkRecording(jsonl, fs.readFileSync(path.join(dir, "manifest.json"), "utf8"));
const html = fill(fs.readFileSync(path.join(here, "web/player.html"), "utf8"), { jsonl, manifest, display });
fs.writeFileSync(out, html);
console.log(JSON.stringify({ out, bytes: Buffer.byteLength(html), frames: manifest.frames, frames_sha256: manifest.frames_sha256 }));
