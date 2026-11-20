// Nuthatch nest showcase. Static, no build, no dependencies. Everything it shows comes from the nest's own
// public read routes (/ready, /tables, /sql). Page and nest must share an origin (the nest sends no CORS
// headers); showcase/README.md says how. All nest data goes in through textContent, never innerHTML.
"use strict";
const $ = (s) => document.querySelector(s);
const el = (tag, props = {}, ...kids) => {
  const e = Object.assign(document.createElement(tag), props);
  kids.forEach((k) => e.append(k));
  return e;
};
const q = new URLSearchParams(location.search);
// Same-origin only: a bare name (-> /nests/NAME) or a relative path starting with a single "/". Anything else
// (absolute, protocol-relative, backslashes) falls back to the default nest.
const rawNest = (q.get("nest") || "arc-usdc").replace(/\/+$/, "");
const nestParam = /^[A-Za-z0-9._-]+$/.test(rawNest) || /^\/(?!\/)[A-Za-z0-9._\/-]*$/.test(rawNest) ? rawNest : "arc-usdc";
const base = nestParam.startsWith("/") ? nestParam : "/nests/" + nestParam;
const nestName = base.split("/").pop();
const absBase = new URL(base, location.href).href;
const qi = (id) => '"' + id.replace(/"/g, '""') + '"';

// ---- theme
const root = document.documentElement;
root.dataset.theme = localStorage.theme || (matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light");
$("#theme").onclick = () => {
  root.dataset.theme = root.dataset.theme === "dark" ? "light" : "dark";
  localStorage.theme = root.dataset.theme;
};
$("#raw-link").href = base + "/nest";

// ---- nest access
async function sql(text) {
  const t0 = performance.now();
  const r = await fetch(base + "/sql?q=" + encodeURIComponent(text));
  const body = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(body.error || (r.status === 429 ? "Rate limited. Wait a few seconds." : "HTTP " + r.status));
  body.ms = Math.round(performance.now() - t0);
  return body;
}
const getJSON = async (p) => {
  const r = await fetch(base + p);
  if (!r.ok) throw new Error(p + " HTTP " + r.status);
  return r.json();
};

// ---- formatting
const nf = new Intl.NumberFormat("en-US");
const fmtInt = (n) => nf.format(Number(n));
function fmtAmount(s, decimals, dp = 2) {
  if (s == null) return "";
  let v;
  try { v = BigInt(String(s).split(".")[0]); } catch { return String(s); }
  const neg = v < 0n; if (neg) v = -v;
  const d = BigInt(10) ** BigInt(decimals);
  const whole = v / d, frac = decimals ? String(v % d).padStart(decimals, "0").slice(0, dp) : "";
  return (neg ? "-" : "") + nf.format(whole) + (dp && decimals ? "." + frac.padEnd(dp, "0") : "");
}
const shortHex = (h) => (typeof h === "string" && h.length > 16 ? h.slice(0, 8) + "..." + h.slice(-6) : h);
function ago(ts) {
  const s = Math.max(0, Math.floor(Date.now() / 1000 - Number(ts)));
  if (s < 90) return s + "s ago";
  if (s < 5400) return Math.round(s / 60) + "m ago";
  if (s < 172800) return Math.round(s / 3600) + "h ago";
  return Math.round(s / 86400) + "d ago";
}

// ---- config: configs/<name>.json overrides what is inferred from /tables
async function loadConfig() {
  try {
    const r = await fetch("configs/" + encodeURIComponent(nestName) + ".json");
    if (r.ok) return await r.json();
  } catch {}
  return {};
}

function infer(cfg, tables) {
  const ev = Object.assign({}, cfg.events);
  const explicit = (name) => tables.find((t) => t.table === name);
  let t = explicit(ev.table) || tables.find((x) => /transfer/i.test(x.table)) || tables[0];
  const user = t.columns.filter((c) => c.sol_type !== "implicit");
  const addrs = user.filter((c) => c.sol_type === "address").map((c) => c.name);
  const amt = user.find((c) => /^u?int/.test(c.sol_type) && !c.indexed);
  ev.table = t.table;
  ev.from = ev.from || addrs[0];
  ev.to = ev.to || addrs[1];
  ev.amount = ev.amount || (amt && amt.name);
  ev.decimals = ev.decimals ?? 0;
  ev.symbol = ev.symbol || "";
  ev.userCols = user;
  return ev;
}

function defaults(cfg, ev, tables) {
  const T = qi(ev.table), amountDec = ev.amount && qi(ev.amount + "_dec");
  const out = Object.assign({}, cfg);
  out.title = cfg.title || nestName;
  out.tagline = cfg.tagline || "A live, queryable index of this nest's contracts.";
  const qs = [{ title: "Latest events", sql: `SELECT * FROM ${T} ORDER BY block_number DESC, log_index DESC LIMIT 20` }];
  if (ev.from) qs.push({ title: "Top senders, last 24h", sql: `SELECT ${qi(ev.from)}, count(*) AS events${amountDec ? `, sum(${amountDec}) AS total_base_units` : ""} FROM ${T} WHERE block_timestamp > extract(epoch from now()) - 86400 GROUP BY 1 ORDER BY ${amountDec ? "total_base_units" : "events"} DESC LIMIT 10` });
  qs.push({ title: "Events per hour, last 24h", sql: `SELECT date_trunc('hour', to_timestamp(block_timestamp)) AS hour, count(*) AS events FROM ${T} WHERE block_timestamp > extract(epoch from now()) - 86400 GROUP BY 1 ORDER BY 1` });
  out.queries = cfg.queries || qs;
  const unit = ev.symbol || "units";
  out.prompts = cfg.prompts || [
    `What tables does this nest have, and how many rows are in each?`,
    `Show me the 10 most recent ${ev.table} events in a table.`,
    ev.from ? `Who were the top 10 addresses by ${ev.from} in the last 24 hours on this nest?` : `How many events per hour in the last 24 hours?`,
    `How many ${ev.table} events happened per hour over the last 12 hours? Which hour was busiest?`,
    `What block range does this nest cover, and what is the UTC time of its newest event?`,
  ];
  return out;
}

// ---- charts (inline SVG, one colour)
const NS = "http://www.w3.org/2000/svg";
const s = (tag, attrs = {}, text) => {
  const e = document.createElementNS(NS, tag);
  for (const k in attrs) e.setAttribute(k, attrs[k]);
  if (text != null) e.textContent = text;
  return e;
};
function barChart(target, rows, label) {
  const W = 420, H = 220, L = 40, B = 24, T = 8, n = rows.length;
  const max = Math.max(1, ...rows.map((r) => r.v));
  const svg = s("svg", { viewBox: `0 0 ${W} ${H}`, role: "img", "aria-label": label });
  [0, 0.5, 1].forEach((f) => {
    const y = T + (H - B - T) * (1 - f);
    svg.append(s("line", { x1: L, x2: W, y1: y, y2: y, stroke: "var(--line)" }), s("text", { x: L - 6, y: y + 4, "text-anchor": "end" }, fmtCompact(max * f)));
  });
  const bw = (W - L) / n;
  rows.forEach((r, i) => {
    const h = ((H - B - T) * r.v) / max;
    const b = s("rect", { class: "bar", x: L + i * bw + bw * 0.12, y: H - B - h, width: bw * 0.76, height: h, rx: 2 });
    b.append(s("title", {}, `${r.t}: ${fmtInt(r.v)}`));
    svg.append(b);
    if (i % Math.ceil(n / 6) === 0) svg.append(s("text", { x: L + i * bw + bw / 2, y: H - 6, "text-anchor": "middle" }, r.x));
  });
  target.replaceChildren(svg);
}
const fmtCompact = (v) => new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 }).format(v);
function hbars(target, rows, label) {
  const rowH = 28, W = 420, L = 128, H = rows.length * rowH + 6;
  const max = Math.max(1, ...rows.map((r) => r.v));
  const svg = s("svg", { viewBox: `0 0 ${W} ${H}`, role: "img", "aria-label": label });
  rows.forEach((r, i) => {
    const y = 3 + i * rowH, w = Math.max(2, ((W - L - 118) * r.v) / max);
    svg.append(s("text", { x: L - 8, y: y + 15, "text-anchor": "end" }, r.label));
    const b = s("rect", { class: "bar", x: L, y: y + 3, width: w, height: rowH - 10, rx: 2 });
    b.append(s("title", {}, `${r.full}: ${r.text}`));
    svg.append(b, s("text", { x: L + w + 6, y: y + 15 }, r.text));
  });
  target.replaceChildren(svg);
}

// ---- tables
function renderTable(tbl, cols, rows, fmt = {}) {
  const head = el("tr");
  cols.forEach((c) => head.append(el("th", { textContent: c, className: fmt[c]?.num ? "num" : "" })));
  tbl.tHead.replaceChildren(head);
  const body = tbl.tBodies[0] || tbl.createTBody();
  body.replaceChildren(...rows.map((r) => {
    const tr = el("tr");
    cols.forEach((c) => {
      const f = fmt[c], v = r[c];
      const td = el("td", { textContent: f ? f.fn(v, r) : v == null ? "" : typeof v === "object" ? JSON.stringify(v) : String(v) });
      if (f?.num) td.className = "num";
      else if (typeof v === "string" && v.startsWith("0x")) td.className = "mono";
      if (typeof v === "string" && v.length > 20) td.title = v;
      tr.append(td);
    });
    return tr;
  }));
}

// ---- sections
function renderAgent(cfg) {
  const url = absBase + "/mcp", name = nestName;
  const cmds = {
    claude: `claude mcp add --transport http ${name} ${url}`,
    cursor: `// ~/.cursor/mcp.json\n{ "mcpServers": { "${name}": { "url": "${url}" } } }`,
    codex: `# ~/.codex/config.toml\n[mcp_servers.${name.replace(/[^A-Za-z0-9_-]/g, "_")}]\nurl = "${url}"`,
  };
  const notes = {
    claude: "Run it once in a terminal. Add --scope user to use it in every project.",
    cursor: "Merge this into your existing mcpServers, then reload Cursor.",
    codex: "Streamable HTTP MCP servers need a recent Codex CLI. Restart Codex after saving.",
  };
  const show = (k) => {
    $("#mcp-cmd").textContent = cmds[k];
    $("#mcp-note").textContent = notes[k];
    document.querySelectorAll(".tabs button").forEach((b) => b.setAttribute("aria-selected", String(b.dataset.tab === k)));
  };
  document.querySelectorAll(".tabs button").forEach((b) => (b.onclick = () => show(b.dataset.tab)));
  show("claude");
  const ol = $("#prompts");
  ol.replaceChildren(...cfg.prompts.map((p) => {
    const li = el("li", {}, el("span", { textContent: p }));
    li.append(el("button", { className: "copy", type: "button", textContent: "Copy" }));
    li.lastChild.dataset.text = p;
    return li;
  }));
}
document.addEventListener("click", async (e) => {
  const b = e.target.closest(".copy");
  if (!b) return;
  const text = b.dataset.text ?? $(b.dataset.copy).textContent;
  try { await navigator.clipboard.writeText(text); b.textContent = "Copied"; } catch { b.textContent = "Press Ctrl+C"; }
  setTimeout(() => (b.textContent = "Copy"), 1400);
});

async function renderStatus() {
  try {
    const r = await getJSON("/ready");
    const lag = r.lag_blocks, ok = r.ready && !r.stalled && !r.wedged;
    const chip = (...k) => el("span", { className: "chip" }, ...k);
    $("#status").replaceChildren(
      chip(el("span", { className: "dot " + (ok ? "ok" : "bad") }), ok ? "Live" : "Not ready"),
      chip("Tip ", el("b", { textContent: fmtInt(r.tip) })),
      chip("Indexed to ", el("b", { textContent: fmtInt(r.last_block) })),
      chip("Lag ", el("b", { textContent: fmtInt(lag) + (lag === 1 ? " block" : " blocks") })),
      chip("Sealed to ", el("b", { textContent: fmtInt(r.sealed_through) })),
      chip("nuthatch ", el("b", { textContent: r.version })),
    );
  } catch (e) {
    $("#status").replaceChildren(el("span", { className: "chip err", textContent: "Status unavailable: " + e.message }));
  }
}

async function renderTables(tables) {
  const parts = tables.map((t) => `SELECT '${t.table}' AS t, count(*) AS n, min(block_number) AS a, max(block_number) AS b FROM ${qi(t.table)}`);
  const res = await sql(parts.join(" UNION ALL "));
  const by = Object.fromEntries(res.rows.map((r) => [r.t, r]));
  const rows = tables.map((t) => Object.assign({ table: t.table, event: t.event }, by[t.table] || {}));
  renderTable($("#tables"), ["table", "event", "n", "a", "b"], rows, {
    n: { num: true, fn: (v) => (v == null ? "" : fmtInt(v)) },
    a: { num: true, fn: (v) => (v == null ? "-" : fmtInt(v)) },
    b: { num: true, fn: (v) => (v == null ? "-" : fmtInt(v)) },
  });
  document.querySelectorAll("#tables th").forEach((th, i) => (th.textContent = ["Table", "Event", "Rows", "First block", "Last block"][i]));
}

async function renderCharts(ev) {
  const T = qi(ev.table);
  const act = await sql(`SELECT date_trunc('hour', to_timestamp(block_timestamp)) AS t, count(*) AS n FROM ${T} WHERE block_timestamp > extract(epoch from now()) - 86400 GROUP BY 1 ORDER BY 1`);
  $("#activity-sub").textContent = `${ev.table} events per hour (UTC)`;
  barChart($("#chart-activity"), act.rows.map((r) => ({ t: r.t.replace("T", " ").replace(":00:00Z", ":00 UTC"), x: r.t.slice(11, 13) + "h", v: Number(r.n) })), "Events per hour");
  if (!ev.from) { $("#chart-top").textContent = "No address column in this table."; return; }
  const dec = ev.amount ? qi(ev.amount + "_dec") : null;
  const top = await sql(`SELECT ${qi(ev.from)} AS a, count(*) AS n${dec ? `, sum(${dec}) AS v` : ""} FROM ${T} WHERE block_timestamp > extract(epoch from now()) - 86400 GROUP BY 1 ORDER BY ${dec ? "v" : "n"} DESC LIMIT 8`);
  $("#top-sub").textContent = dec ? `by total ${ev.symbol || "amount"} sent` : "by number of events";
  hbars($("#chart-top"), top.rows.map((r) => {
    const zero = /^0x0+$/.test(r.a);
    const sum = BigInt(String(r.v).split(".")[0]);
    const v = dec ? (ev.decimals >= 2 ? Number(sum / 10n ** BigInt(ev.decimals - 2)) / 100 : Number(sum) / 10 ** ev.decimals) : Number(r.n);
    return { label: zero ? "mint (0x0)" : shortHex(r.a), full: r.a, v, text: dec ? fmtCompact(v) + " " + ev.symbol : fmtInt(v) };
  }), "Top senders");
}

async function renderEvents(ev) {
  const cols = ["block_number", "block_timestamp"].concat(ev.userCols.map((c) => c.name), ["tx_hash"]);
  const sel = cols.map((c) => {
    const uc = ev.userCols.find((u) => u.name === c);
    return uc && /^u?int/.test(uc.sol_type) ? qi(c + "_dec") + " AS " + qi(c) : qi(c);
  });
  const res = await sql(`SELECT ${sel.join(", ")} FROM ${qi(ev.table)} ORDER BY block_number DESC, log_index DESC LIMIT 15`);
  const fmt = { block_number: { num: true, fn: (v) => fmtInt(v) }, block_timestamp: { fn: (v) => ago(v) }, tx_hash: { fn: shortHex } };
  ev.userCols.forEach((c) => {
    if (c.sol_type === "address") fmt[c.name] = { fn: shortHex };
    else if (c.name === ev.amount) fmt[c.name] = { num: true, fn: (v) => fmtAmount(v, ev.decimals) + (ev.symbol ? " " + ev.symbol : "") };
  });
  renderTable($("#events"), cols, res.rows, fmt);
}

function renderSqlBox(cfg) {
  const sel = $("#examples");
  sel.replaceChildren(...cfg.queries.map((x, i) => el("option", { value: i, textContent: x.title })));
  const set = (i) => ($("#sql").value = cfg.queries[i].sql);
  sel.onchange = () => { set(sel.value); run(); };
  set(0);
  async function run() {
    const meta = $("#sql-meta");
    meta.className = "muted small"; meta.textContent = "Running...";
    try {
      const r = await sql($("#sql").value);
      const cols = r.columns || Object.keys(r.rows[0] || {});
      renderTable($("#sql-out"), cols, r.rows, Object.fromEntries(cols.map((c) => [c, { num: r.rows.length && typeof r.rows[0][c] === "number", fn: (v) => (v == null ? "" : typeof v === "number" ? fmtInt(v) : String(v)) }])));
      meta.textContent = `${fmtInt(r.count)} rows${r.truncated ? " (truncated)" : ""} in ${r.ms} ms, as of block ${fmtInt(r.provenance?.as_of ?? 0)}`;
    } catch (e) {
      meta.className = "err small"; meta.textContent = e.message;
    }
  }
  $("#run").onclick = run;
  $("#sql").addEventListener("keydown", (e) => { if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) { e.preventDefault(); run(); } });
  run();
}

// ---- boot
(async function main() {
  let cfg0 = await loadConfig();
  let tables;
  try { tables = (await getJSON("/tables")).tables; } catch (e) {
    $("#title").textContent = "Cannot reach this nest";
    $("#tagline").textContent = e.message + ". Check the ?nest= parameter and that this page is served from the same origin as the nest.";
    return;
  }
  const ev = infer(cfg0, tables), cfg = defaults(cfg0, ev, tables);
  document.title = cfg.title + " - Nuthatch nest";
  $("#title").textContent = cfg.title;
  $("#tagline").textContent = cfg.tagline;
  $("#chain").textContent = cfg.chain || "";
  renderAgent(cfg);
  renderSqlBox(cfg);
  const guard = (p, target) => p.catch((e) => { const n = $(target); if (n) n.replaceChildren(el("p", { className: "err", textContent: e.message })); });
  renderStatus();
  guard(renderTables(tables), "#tables tbody");
  guard(renderCharts(ev), "#chart-activity");
  guard(renderEvents(ev), "#events tbody");
  setInterval(() => {
    if (document.hidden) return;
    renderStatus();
    if ($("#live").checked) renderEvents(ev).catch(() => {});
  }, 15000);
})();
