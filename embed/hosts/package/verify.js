// RFC-0081 S3: how `verify` compares a hosted answer with this nest's and labels the head it ran under.
// Pure functions, so `verify.test.mjs` runs them in Node without a module; `build.mjs` bundles this
// file into `nuthatch-engine.js` before `engine.js`, which imports it.

/// A value as the wire carries it, compared as text, as RFC-0077 S7's page compares a figure.
export const asText =(v) => (v == null ? null : typeof v === "string" ? v : typeof v === "object" ? JSON.stringify(v) : String(v));

/// Rows in one order whatever order each side listed them in, so a comparison is of the rows.
export const ordered = (rows, columns) => rows
  .map((r) => columns.map((c) => asText(r[c])))
  .sort((a, b) => (JSON.stringify(a) < JSON.stringify(b) ? -1 : JSON.stringify(a) > JSON.stringify(b) ? 1 : 0));

/// The columns an answer named and every key its rows carry: a client reads the rows, so a column
/// left out of `columns` is still part of what it was told (#2614).
export const columnsOf = (answer) => {
  const out = [...answer.columns];
  for (const r of answer.rows) for (const k of Object.keys(r ?? {})) if (!out.includes(k)) out.push(k);
  return out;
};

/// Column by column: equal, differs with the values on each side, or not computable here, named.
export function compare(server, here) {
  server = { ...server, columns: columnsOf(server) };
  here = { ...here, columns: columnsOf(here) };
  const both = server.columns.filter((c) => here.columns.includes(c));
  const s = ordered(server.rows, both);
  const h = ordered(here.rows, both);
  const columns = both.map((column, k) => {
    const differs = [];
    for (let i = 0; i < Math.max(s.length, h.length); i++) {
      const a = s[i]?.[k] ?? null;
      const b = h[i]?.[k] ?? null;
      if (a !== b || !s[i] !== !h[i]) differs.push({ row: i, server: a, here: b });
    }
    return differs.length ? { column, verdict: "differs", values: differs.slice(0, 20), rows_differing: differs.length } : { column, verdict: "equal" };
  });
  for (const column of server.columns.filter((c) => !both.includes(c))) {
    columns.push({ column, verdict: "not computable", reason: "this nest's answer has no such column", server: true });
  }
  for (const column of here.columns.filter((c) => !both.includes(c))) {
    columns.push({ column, verdict: "not computable", reason: "the server's answer has no such column" });
  }
  return { rows: { server: server.rows.length, here: here.rows.length }, columns };
}

/// A statement's verdict from its comparison. `incomplete` when nothing differs but a column the
/// server returned could not be computed here: it was not checked, so the answer is not equal.
export function statementVerdict(c) {
  if (c.columns.some((x) => x.verdict === "differs") || c.rows.server !== c.rows.here) return "differs";
  return c.columns.some((x) => x.server && x.verdict === "not computable") ? "incomplete" : "equal";
}

/// The head a check ran under, from what the follow did rather than what was asked (RFC-0081 §7.2,
/// #2615). Only blocks at or below the verified finalized block are checked against it.
export function headOf(verified, followed, as_of) {
  if (!verified) return { head: "the RPC's, unverified" };
  if (followed.unverified) return { head: `refused: ${followed.unverified}`, unverified: followed.unverified };
  const n = followed.verified?.finalized?.number;
  if (!Number.isSafeInteger(n)) {
    const why = "the module reported no verified finalized block";
    return { head: `refused: ${why}`, unverified: why };
  }
  if (as_of <= n) return { head: "verified", verified_through: n };
  return { head: `verified through block ${n}; blocks ${n + 1} to ${as_of} are the RPC's`, verified_through: n };
}
