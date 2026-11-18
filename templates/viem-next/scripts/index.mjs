import { spawnSync } from "node:child_process";

const nestUrl = process.env.NUTHATCH_URL ?? "http://127.0.0.1:8288";
const result = spawnSync(
  "npx",
  ["-y", "nuthatch-indexer", "dev", "--dir", "nest", "--backfill", "300", "--detach"],
  { stdio: "inherit" },
);
if (result.error) throw result.error;
if (result.status !== 0) process.exit(result.status ?? 1);

// This nuthatch version has no --wait-rows flag. --detach waits for the first
// chain poll; query /sql until a decoded Transfer is available as well.
const deadline = Date.now() + 120_000;
let lastError = "the nest did not answer";
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
      console.log(`Nest is indexing and serving rows at ${nestUrl}`);
      process.exit(0);
    }
    lastError = response.ok ? "no USDC Transfer rows indexed yet" : JSON.stringify(answer);
  } catch (error) {
    lastError = error instanceof Error ? error.message : String(error);
  }
  await new Promise((resolve) => setTimeout(resolve, 2_000));
}

console.error(`Timed out waiting for USDC Transfer rows: ${lastError}`);
console.error("Check the detached nest's log; it is printed by dev --detach --json.");
process.exit(1);
