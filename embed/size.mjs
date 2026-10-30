// RFC-0075 S6: the module's size as a page downloads it, beside burrmill-wasm's 5.6 MB brotli.
// Usage: node size.mjs <module.wasm>
import { readFileSync } from "node:fs";
import { brotliCompressSync, gzipSync, constants } from "node:zlib";

const bytes = readFileSync(process.argv[2]);
const brotli = brotliCompressSync(bytes, {
  params: { [constants.BROTLI_PARAM_QUALITY]: 11, [constants.BROTLI_PARAM_LGWIN]: 24 },
}).length;
console.log(JSON.stringify({ raw: bytes.length, gzip: gzipSync(bytes, { level: 9 }).length, brotli }));
