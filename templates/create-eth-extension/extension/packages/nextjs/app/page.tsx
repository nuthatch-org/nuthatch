import { formatUnits } from "viem";

export const dynamic = "force-dynamic";

type Transfer = {
  from: string;
  to: string;
  value: string;
  block_number: number;
  tx_hash: string;
  log_index: number;
};

const nestUrl = process.env.NUTHATCH_URL ?? "http://127.0.0.1:8288";

async function latestTransfers(): Promise<Transfer[]> {
  const response = await fetch(new URL("/sql", nestUrl), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      q: 'SELECT "from", "to", value, block_number, tx_hash, log_index FROM usdc__transfer ORDER BY block_number DESC, log_index DESC LIMIT 20',
      max_rows: 20,
    }),
    cache: "no-store",
  });
  const answer = await response.json();
  if (!response.ok) throw new Error(answer.error ?? `Nest returned HTTP ${response.status}`);
  return answer.rows;
}

export default async function Home() {
  const transfers = await latestTransfers();
  return (
    <main style={{ padding: "2rem" }}>
      <h1>Latest Base USDC transfers</h1>
      <table>
        <thead><tr><th>Block</th><th>From</th><th>To</th><th>USDC</th><th>Transaction</th></tr></thead>
        <tbody>
          {transfers.map((transfer) => (
            <tr key={`${transfer.tx_hash}-${transfer.log_index}`}>
              <td>{transfer.block_number}</td>
              <td>{transfer.from}</td>
              <td>{transfer.to}</td>
              <td>{formatUnits(BigInt(transfer.value), 6)}</td>
              <td>{transfer.tx_hash}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </main>
  );
}
