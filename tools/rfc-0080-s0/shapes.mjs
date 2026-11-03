// RFC-0080 §20: the S0 shape sets, seeded, one shape a line, in `/sync`'s wire form.
//
//   node shapes.mjs <n> <uniform|hot:<k>|all-tables> > shapes.jsonl
//
// The generator is §11 and §12's (`pete/rfc-0080-s0b`, 8054ab4a, `tools/rfc-0080-s0/shapes.mjs`),
// with its seed and its derivations from the multinest fixture, so a mix asks what it asked then:
//
// uniform     Over the swap table. The fixture draws `sender` from 4,096 addresses
//             (`seed % 4096 + 1`); each shape names one to four of them, and one in five also names
//             `id` values, so the follower checks a second term.
// hot:<k>     As uniform, but k of the shapes are the whole pool's swaps (`address` is the one
//             contract), so every swap row matches k shapes.
// all-tables  Spread over all ten tables, each naming one to four `tx_hash`es of its table. Every
//             row the writer commits is in a shaped table. One change from §11: the hashes are drawn
//             from blocks 20,000 to 20,800 rather than 20,300, since this run's window starts once
//             10,000 streams have their first answers and the tip has moved on.

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

const out = [];
for (let i = 0; i < Number(n); i++) {
  if (mode === "all-tables") {
    // A log's tx_hash is block * 200 + its index i in the block, and its event (so its table) the
    // ABI's (i mod 10)th, the TABLES order, with one contract at 200 logs a block.
    const t = rand(TABLES.length);
    const hashes = Array.from({ length: 1 + rand(4) }, () => word((20000 + rand(800)) * 200 + t + 10 * rand(20)));
    out.push({ table: TABLES[t], where: { tx_hash: hashes } });
    continue;
  }
  if (mode.startsWith("hot:") && i < Number(mode.slice(4))) {
    out.push({ table: "pool_manager__swap", where: { address: [addr(1)] } });
    continue;
  }
  const senders = new Set();
  const k = 1 + rand(4);
  while (senders.size < k) senders.add(addr(1 + rand(4096)));
  const where = { sender: [...senders] };
  if (rand(5) === 0) {
    const first = Number(BigInt([...senders][0]));
    const base = (((first - 2) % 4096) + 4096) % 4096;
    where.id = Array.from({ length: 6 }, () => word(base + 4096 * (636 + rand(20))));
  }
  out.push({ table: "pool_manager__swap", where });
}
for (const shape of out) console.log(JSON.stringify(shape));
