// RFC-0077 S4's gate, the part a browser runs too: the requests both halves answer, and the answers
// made comparable. Neither engine chooses the order of a view's rows, so rows are compared as
// multisets, and the NID each was opened under is left out. No Node module is imported here.

const q = (sql) => `/sql?q=${encodeURIComponent(sql)}`;

/// Each `CREATE VIEW` in the nest's files: its name and its query.
function views(files) {
  const out = [];
  for (const [p, text] of Object.entries(files).sort()) {
    if (!p.startsWith("views/") || !p.endsWith(".sql")) continue;
    for (const stmt of text.split(";")) {
      const m = /CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+"?([A-Za-z0-9_]+)"?\s+AS\s+([\s\S]*)$/i.exec(stmt.replace(/--[^\n]*/g, ""));
      if (m) out.push({ name: m[1].toLowerCase(), body: m[2].trim() });
    }
  }
  return out;
}

/// Every answer the gate compares, asked through `ask(target)`: every authored view, each table's
/// count and range, each entity's first thousand rows, the balances and the hot rows. A view ending in
/// `ORDER BY … LIMIT n` may choose among rows tied at its last place as it likes, so it is compared by
/// its shape, and its rows are asked again with every column added to the order to break the tie.
export async function answersOf(files, ask) {
  const targets = [q("SELECT 1"), "/balances?limit=1000", "/entities?limit=100", "/derived"];
  const tables = JSON.parse(files["schema.json"] ?? '{"tables": []}').tables.map((t) => t.table).sort();
  for (const t of tables) {
    targets.push(q(`SELECT count(*) AS n, min(block_number) AS lo, max(block_number) AS hi, sum(log_index) AS li FROM ${t}`));
  }
  const shapeOnly = new Set();
  const tiebroken = [];
  for (const v of views(files)) {
    targets.push(q(`SELECT * FROM ${v.name}`), q(`SELECT count(*) AS n FROM ${v.name}`));
    const m = /^([\s\S]*\bORDER\s+BY\s+[\s\S]*?)\s+LIMIT\s+(\d+)\s*$/i.exec(v.body);
    if (m) {
      shapeOnly.add(q(`SELECT * FROM ${v.name}`));
      tiebroken.push({ name: v.name, ordered: m[1], limit: m[2] });
    }
  }
  for (const m of (files["entities.toml"] ?? "").matchAll(/^\s*name\s*=\s*"([^"]+)"/gm)) {
    targets.push(`/derived/${m[1]}?limit=1000`);
  }
  const out = {};
  for (const t of targets) {
    const a = await ask(t);
    out[t] = comparable(a.status, a.body, shapeOnly.has(t));
  }
  for (const v of tiebroken) {
    const cols = JSON.parse((await ask(q(`SELECT * FROM ${v.name} LIMIT 0`))).body).columns?.length ?? 0;
    const ordinals = Array.from({ length: cols }, (_, i) => i + 1).join(", ");
    const t = q(`${v.ordered}, ${ordinals} LIMIT ${v.limit}`);
    const a = await ask(t);
    out[t] = comparable(a.status, a.body);
  }
  return out;
}

export const canon = (v) => JSON.stringify(v, (_, x) =>
  x && typeof x === "object" && !Array.isArray(x) ? Object.fromEntries(Object.entries(x).sort()) : x);
const sortRows = (rows) => [...rows].sort((a, b) => (canon(a) < canon(b) ? -1 : canon(a) > canon(b) ? 1 : 0));

/// One answer as the gate compares it: the status and the JSON body, rows and items as multisets,
/// with the NID, the clock and whether the node answered from its cache left out. Rows past the cap
/// (`truncated`) are whichever the engine reached first, so only the shape of such an answer is
/// compared, as it is for `shape`.
export function comparable(status, body, shape = false) {
  let v;
  try {
    v = JSON.parse(body);
  } catch {
    return { status, body };
  }
  const strip = (o) => {
    if (!o || typeof o !== "object") return;
    for (const k of ["nid", "now", "cached", "age", "last_poll"]) delete o[k];
    for (const x of Object.values(o)) strip(x);
  };
  strip(v);
  if (Array.isArray(v.rows)) v.rows = shape || v.truncated ? v.rows.length : sortRows(v.rows);
  if (Array.isArray(v.items)) v.items = sortRows(v.items);
  return { status, body: v };
}

/// The paths where two reports differ, at most `max` of them.
export function differences(a, b, max = 20) {
  const out = [];
  for (const k of new Set([...Object.keys(a), ...Object.keys(b)])) {
    if (canon(a[k]) !== canon(b[k])) out.push(k);
    if (out.length >= max) break;
  }
  return out;
}
