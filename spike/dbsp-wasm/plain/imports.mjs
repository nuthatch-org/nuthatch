// RFC-0077 S0's first gate: the module imports nothing outside `nuthatch`. Prints every import and
// exits non-zero if any other module is named.
// Usage: node imports.mjs <module.wasm>
import { readFileSync } from "node:fs";

const imports = WebAssembly.Module.imports(new WebAssembly.Module(readFileSync(process.argv[2])));
for (const i of imports) console.log(`${i.module}.${i.name} (${i.kind})`);
const foreign = imports.filter((i) => i.module !== "nuthatch");
console.log(`${imports.length} imports, ${foreign.length} outside nuthatch`);
if (foreign.length) process.exit(1);
