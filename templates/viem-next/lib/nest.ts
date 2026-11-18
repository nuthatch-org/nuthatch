export const NUTHATCH_URL = process.env.NUTHATCH_URL ?? "http://127.0.0.1:8288";

export type Transfer = {
  from: string;
  to: string;
  value: string;
  block_number: number;
  tx_hash: string;
  log_index: number;
};

type SqlAnswer = {
  rows: Transfer[];
  error?: string;
};

export async function latestTransfers(): Promise<Transfer[]> {
  const response = await fetch(new URL("/sql", NUTHATCH_URL), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      q: 'SELECT "from", "to", value, block_number, tx_hash, log_index FROM usdc__transfer ORDER BY block_number DESC, log_index DESC LIMIT 20',
      max_rows: 20,
    }),
    cache: "no-store",
  });
  const answer = (await response.json()) as SqlAnswer;
  if (!response.ok) throw new Error(answer.error ?? `Nest returned HTTP ${response.status}`);
  return answer.rows;
}
