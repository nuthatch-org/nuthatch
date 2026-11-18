import { spawnSync } from "node:child_process";

const nestUrl = process.env.NUTHATCH_URL ?? "http://127.0.0.1:8288";
const result = spawnSync(
  "npx",
  ["-y", "nuthatch-indexer", "dev", "--dir", "nest", "--backfill", "300", "--detach"],
  { stdio: "inherit" },
);
if (result.error) throw result.error;
if (result.status !== 0) process.exit(result.status ?? 1);

// --wait-rows is not a flag in this nuthatch version; wait through /sql instead.
const deadline = Date.now() + 120_000;
while (Date.now() < deadline) {
  try {
    const response = await fetch(new URL("/sql", nestUrl), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ q: "SELECT count(*) AS n FROM usdc__transfer", max_rows: 1 }),
      signal: AbortSignal.timeout(5_000),
    });
    const answer = await response.json();
    if (response.ok && Number(answer.rows?.[0]?.n) > 0) {
      console.log(`Nest is serving USDC transfers at ${nestUrl}`);
      process.exit(0);
    }
  } catch {
    // The detached process may still be starting or waiting on the RPC.
  }
  await new Promise((resolve) => setTimeout(resolve, 2_000));
}
console.error("Timed out waiting for USDC Transfer rows; inspect the detached nest's log.");
process.exit(1);
