// RFC-0080 S0: N shapes over the multinest fixture, seeded, so every run asks the same.
//
//   node shapes.mjs <n> [uniform|skew|hot:<k>|all-tables] > shapes.json
//
// uniform     Over the swap table. The fixture draws `sender` from 4,096 addresses
//             (`seed % 4096 + 1`); each shape names one to four of them, and one in five also names
//             `id` values, so the follower checks a second term.
// skew        As uniform, but half the shapes include one of eight hot senders: a popular token's
//             fan-out, about 600 shapes for each of those senders' swaps.
//             The fixture draws those senders as rarely as any other, so this is a tail, not a load.
// hot:<k>     As uniform, but k of the shapes are the whole pool's swaps (`address` is the one
//             contract), so every swap row matches k shapes: a popular token's fan-out under load.
// all-tables  Spread over all ten tables, each naming one to four `tx_hash`es of its table from the
//             blocks the run follows. Every row the writer commits is then in a shaped table and is
//             read in full, the filter's worst case for parsing; each hash matches one row.
//
// A swap's `id` is (seed + 1) mod 2^32 and its `sender` (seed + 2) mod 4096 + 1, so the ids a
// sender can carry are 4096 apart. Over blocks 20,000 to 20,300 the seeds put them at multiples 636
// to 655; a shape names six of the twenty, which admits about a third of its first sender's swaps.

const [n = "10000", mode = "uniform"] = process.argv.slice(2);
const TABLES = [
  "approval",
  "donate",
  "initialize",
  "modify_liquidity",
  "operator_set",
  "ownership_transferred",
  "protocol_fee_controller_updated",
  "protocol_fee_updated",
  "swap",
  "transfer",
].map((t) => `pool_manager__${t}`);

let state = 0x9e3779b9;
const rand = (m) => {
  state ^= state << 13;
  state ^= state >>> 17;
  state ^= state << 5;
  return (state >>> 0) % m;
};
const word = (i) => "0x" + i.toString(16).padStart(64, "0");
const addr = (i) => "0x" + i.toString(16).padStart(40, "0");

const shapes = [];
for (let i = 0; i < Number(n); i++) {
  if (mode === "all-tables") {
    // A log's tx_hash is block * 200 + its index i in the block, and its event (so its table) the
    // ABI's (i mod 10)th, the TABLES order, with one contract at 200 logs a block.
    const t = rand(TABLES.length);
    const hashes = Array.from({ length: 1 + rand(4) }, () => word((20000 + rand(300)) * 200 + t + 10 * rand(20)));
    shapes.push({ table: TABLES[t], where: { tx_hash: hashes } });
    continue;
  }
  if (mode.startsWith("hot:") && i < Number(mode.slice(4))) {
    shapes.push({ table: "pool_manager__swap", where: { address: [addr(1)] } });
    continue;
  }
  const senders = new Set();
  const k = 1 + rand(4);
  if (mode === "skew" && rand(2) === 0) senders.add(addr(1 + rand(8)));
  while (senders.size < k) senders.add(addr(1 + rand(4096)));
  const where = { sender: [...senders] };
  if (rand(5) === 0) {
    const first = Number(BigInt([...senders][0]));
    const base = (((first - 2) % 4096) + 4096) % 4096;
    where.id = Array.from({ length: 6 }, () => word(base + 4096 * (636 + rand(20))));
  }
  shapes.push({ table: "pool_manager__swap", where });
}
process.stdout.write(JSON.stringify(shapes));
