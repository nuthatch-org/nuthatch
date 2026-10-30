// `tests/e2e_tab_answers.rs`'s mask: the same keys, the same replacement, so the tab's bytes are
// compared with the node's exactly where the node's own pin compares them.
import { createHash } from "node:crypto";

const MASKED = ["fetch_window_blocks", "version", "last_poll_unixtime", "seconds_since_poll",
  "seconds_since_seal_progress", "seconds_since_progress", "nid"];

export function mask(body) {
  let out = body;
  for (const key of MASKED) {
    const needle = `"${key}":`;
    let from = 0;
    for (;;) {
      const at = out.indexOf(needle, from);
      if (at < 0) break;
      const s = at + needle.length;
      const rest = out.slice(s);
      let len;
      if (rest.startsWith('"')) {
        const i = rest.indexOf('"', 1);
        len = i < 0 ? rest.length : i + 1;
      } else {
        const m = rest.search(/[,}]/);
        len = m < 0 ? rest.length : m;
      }
      out = out.slice(0, s) + '"<masked>"' + out.slice(s + len);
      from = s;
    }
  }
  return out;
}

/// Each answer that differs from the pin, by field, as text for a log.
export function differences(got, expected) {
  const out = [];
  if (got.length !== expected.length) {
    out.push(`the pin holds ${expected.length} answers and ${got.length} came back`);
  }
  for (const [i, want] of expected.entries()) {
    const g = got[i];
    if (!g) break;
    for (const k of ["method", "path", "status", "content_type", "body"]) {
      const v = k === "body" ? mask(g[k]) : g[k];
      if (v !== want[k]) {
        out.push(`${want.method} ${want.path} ${k}\n got: ${String(v).slice(0, 1500)}\nwant: ${String(want[k]).slice(0, 1500)}`);
        break;
      }
    }
  }
  return out;
}

/// RFC-0077 S2: one sha256 over a list of answers, masked, so hosts compare with each other and with
/// the pin in one line. The pin is masked already; `embed/tests/wasmtime-host` computes the same.
export function answersDigest(answers, { pinned = false } = {}) {
  const h = createHash("sha256");
  for (const a of answers) {
    const body = pinned ? a.body : mask(a.body);
    h.update(`${a.method} ${a.path}\n${a.status}\n${a.content_type ?? ""}\n${body}\n`);
  }
  return h.digest("hex");
}

/// RFC-0077 S3: one sha256 over a report's segments, by table name, so a run that was killed and
/// reopened compares with one that was not. `embed/tests/wasmtime-host` computes the same.
export function segmentsDigest(segments) {
  const h = createHash("sha256");
  for (const table of Object.keys(segments).sort()) {
    for (const s of segments[table]) {
      h.update(`${table} ${s.from_block} ${s.to_block} ${s.rows} ${s.hash} ${s.bytes_hash}\n`);
    }
  }
  return h.digest("hex");
}

/// A path with its query, comparable however the query was encoded.
export function sameTarget(a, b) {
  const ua = new URL(a, "http://x/"), ub = new URL(b, "http://x/");
  const q = (u) => JSON.stringify([...u.searchParams.entries()].sort());
  return ua.pathname === ub.pathname && q(ua) === q(ub);
}
