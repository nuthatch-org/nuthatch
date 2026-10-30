// RFC-0075 S6: the module's size as a page downloads it, beside burrmill-wasm's 5.6 MB brotli.
// RFC-0077 S5: with `--max-brotli <bytes>`, exit non-zero past it; CI holds the event build to 5.5 MB.
// Usage: node size.mjs <module.wasm> [--max-brotli <bytes>]
import { readFileSync } from "node:fs";
import { brotliCompressSync, gzipSync, constants } from "node:zlib";

const [file, flag, max] = process.argv.slice(2);
const bytes = readFileSync(file);
const brotli = brotliCompressSync(bytes, {
  params: { [constants.BROTLI_PARAM_QUALITY]: 11, [constants.BROTLI_PARAM_LGWIN]: 24 },
}).length;
console.log(JSON.stringify({ raw: bytes.length, gzip: gzipSync(bytes, { level: 9 }).length, brotli }));
if (flag === "--max-brotli" && brotli > Number(max)) {
  console.error(`${file} is ${brotli} bytes brotli, past the ${max} budget`);
  process.exit(1);
}
