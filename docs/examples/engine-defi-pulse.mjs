// The embeddable engine through its host package (RFC-0081 S2): defi-pulse-nest opened from the
// public mirror in Node, and one question asked of it. The same calls run in a page, where the
// package keeps the nest in OPFS inside its own worker.
//
//   git clone https://github.com/nuthatch-org/defi-pulse-nest
//   node engine-defi-pulse.mjs nuthatch-engine.js nuthatch-events.wasm defi-pulse-nest
//
// Both files are assets of a nuthatch release. No RPC is given, so the nest answers from what the
// mirror sealed and follows nothing; give `rpc` and drop `follow: false` to carry on to the tip.
import path from "node:path";

const [pkg, wasm, nestDir] = process.argv.slice(2);
const { Engine } = await import(path.resolve(pkg));

const nest = await Engine.open({
  module: wasm,
  nest: nestDir,
  storage: ".nests",
  mirror: { base: "https://nestsss.nestsss-worker.workers.dev" },
  follow: false,
});
const opened = await nest.follow();
console.error(`hydrated ${opened.mirror.segments} segments through block ${opened.mirror.complete_through}`);

const answer = await nest.sql(
  "SELECT count(*) AS mints, sum(CAST(amount AS HUGEINT)) AS minted FROM usdc__mint WHERE block_number <= 24500000",
);
console.log(JSON.stringify(answer.rows));
nest.close();
