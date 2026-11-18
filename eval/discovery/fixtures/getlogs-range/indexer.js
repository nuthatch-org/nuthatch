// Collects every USDC Transfer on Ethereum into transfers.json.
import { writeFileSync } from "node:fs";

const RPC_URL = process.env.RPC_URL ?? "https://ethereum-rpc.publicnode.com";
const USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
const TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const START_BLOCK = 6082465;

async function rpc(method, params) {
  const res = await fetch(RPC_URL, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }),
  });
  const body = await res.json();
  if (body.error) throw new Error(`${method}: ${body.error.message}`);
  return body.result;
}

const latest = parseInt(await rpc("eth_blockNumber", []), 16);
const logs = await rpc("eth_getLogs", [
  {
    address: USDC,
    topics: [TRANSFER],
    fromBlock: "0x" + START_BLOCK.toString(16),
    toBlock: "0x" + latest.toString(16),
  },
]);

const transfers = logs.map((log) => ({
  block: parseInt(log.blockNumber, 16),
  tx: log.transactionHash,
  from: "0x" + log.topics[1].slice(26),
  to: "0x" + log.topics[2].slice(26),
  value: BigInt(log.data).toString(),
}));
writeFileSync("transfers.json", JSON.stringify(transfers, null, 2));
console.log(`wrote ${transfers.length} transfers`);
