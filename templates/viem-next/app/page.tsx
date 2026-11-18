import { createPublicClient, formatUnits, http } from "viem";
import { base } from "viem/chains";
import { latestTransfers, NUTHATCH_URL, type Transfer } from "../lib/nest";

export const dynamic = "force-dynamic";

const client = createPublicClient({
  chain: base,
  transport: http(process.env.NUTHATCH_RPC),
});

function short(address: string) {
  return `${address.slice(0, 6)}…${address.slice(-4)}`;
}

function TransferRows({ transfers }: { transfers: Transfer[] }) {
  return (
    <table>
      <thead>
        <tr><th>Block</th><th>From</th><th>To</th><th>USDC</th><th>Transaction</th></tr>
      </thead>
      <tbody>
        {transfers.map((transfer) => (
          <tr key={`${transfer.tx_hash}-${transfer.log_index}`}>
            <td>{transfer.block_number}</td>
            <td title={transfer.from}>{short(transfer.from)}</td>
            <td title={transfer.to}>{short(transfer.to)}</td>
            <td className="amount">{formatUnits(BigInt(transfer.value), 6)}</td>
            <td title={transfer.tx_hash}>{short(transfer.tx_hash)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

export default async function Home() {
  let transfers: Transfer[] = [];
  let error: string | undefined;
  try {
    transfers = await latestTransfers();
  } catch (cause) {
    error = cause instanceof Error ? cause.message : String(cause);
  }

  let currentBlock: bigint | undefined;
  try {
    currentBlock = await client.getBlockNumber();
  } catch {
    // Chain metadata is supplementary; event history still comes from the nest.
  }

  return (
    <main>
      <h1>Latest Base USDC transfers</h1>
      <p className="subtle">SQL from {NUTHATCH_URL}{currentBlock ? ` · RPC block ${currentBlock}` : ""}</p>
      {error ? (
        <p className="error">Could not query the nest: {error}. Run <code>npm run index</code> first.</p>
      ) : transfers.length === 0 ? (
        <p>No transfers indexed yet.</p>
      ) : (
        <TransferRows transfers={transfers} />
      )}
    </main>
  );
}
