// RFC-0077 S2's gate across hosts: every run in the given logs loaded the same module file, and
// every S5 run's answers digest equals the pin's. Fails unless each host named after `--hosts` ran S5.
// Usage: node agree.mjs <module.wasm> <log>... --hosts node,wasmtime,chrome
import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";

const args = process.argv.slice(2);
const at = args.indexOf("--hosts");
const hosts = at < 0 ? [] : args[at + 1].split(",");
const [wasm, ...logs] = at < 0 ? args : args.slice(0, at);
const module_sha256 = createHash("sha256").update(readFileSync(wasm)).digest("hex");

const runs = logs.flatMap((f) => readFileSync(f, "utf8").split("\n"))
  .filter((l) => l.startsWith("{") && l.includes('"module_sha256"'))
  .map((l) => JSON.parse(l));
const failures = [];
for (const r of runs) {
  const s5 = r.gate === "s5" ? ` answers ${r.answers_sha256} pin ${r.pin_sha256}` : "";
  console.log(`${r.host} ${r.gate} ${r.storage}: module ${r.module_sha256}${s5}`);
  if (r.module_sha256 !== module_sha256) failures.push(`${r.host} ran another module`);
  if (r.gate === "s5" && (!r.answers_sha256 || r.answers_sha256 !== r.pin_sha256)) {
    failures.push(`${r.host} ${r.storage}: answers differ from the pin`);
  }
}
for (const h of hosts) {
  if (!runs.some((r) => r.host === h && r.gate === "s5")) failures.push(`no S5 run from ${h}`);
}
if (failures.length) {
  for (const f of failures) console.error(`MISMATCH ${f}`);
  process.exit(1);
}
console.log(`gate: ${runs.length} runs of module ${module_sha256}, every S5 answer set equal to the pin`);
