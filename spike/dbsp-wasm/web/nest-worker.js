// RFC-0075 S5: the dedicated worker that runs the whole nest (§4) and answers its routes. The page
// posts `{nest: "open", ...}` once, then `{nest: "request", ...}` with a port for each answer, or
// `{nest: "call", ...}` for §7's JS API. With `keepFollowing` it follows the tip every poll interval
// and posts `{nest: "followed", ...}` after each pass. `pkg` is the module's URL, relative to here.

let nest;

async function follow() {
  const f = JSON.parse(await nest.follow());
  return {
    last_block: f.last_block, sealed_through: f.sealed_through, hot_rows: f.hot_rows,
    holders: f.balances.holders, resumed_from: f.resumed_from, backend: f.backend,
  };
}

function keepFollowing(secs) {
  const pass = async () => {
    try {
      self.postMessage({ nest: "followed", ...(await follow()) });
    } catch (e) {
      self.postMessage({ nest: "followed", error: String(e && e.message || e) });
    }
    setTimeout(pass, secs * 1000);
  };
  setTimeout(pass, secs * 1000);
}

self.onmessage = async (m) => {
  const d = m.data;
  try {
    if (d.nest === "open") {
      const { default: init, LiveNest } = await import(d.pkg || "../pkg-web/dbsp_wasm_spike.js");
      await init();
      nest = await LiveNest.open(d.nid, d.rpc, d.contract, BigInt(d.start));
      // Before `derive`, which admits the entities against it (§17).
      if (d.memoryCeilingMib) nest.setMemoryCeiling(d.memoryCeilingMib);
      nest.derive(JSON.stringify(d.files));
      if (d.pollIntervalSecs) nest.setPollInterval(BigInt(d.pollIntervalSecs));
      self.postMessage({ nest: "opened", ...(await follow()) });
      if (d.keepFollowing) keepFollowing(d.pollIntervalSecs || 12);
    } else if (d.nest === "request") {
      const a = await nest.request(d.method, d.target, d.body);
      const body = a.body;
      m.ports[0].postMessage({ status: a.status, contentType: a.contentType, body }, [body.buffer]);
    } else if (d.nest === "call") {
      const [a, b] = d.args;
      const text = d.fn === "sql" ? await nest.sql(a)
        : d.fn === "entity" ? await nest.entity(a)
        : d.fn === "derived" ? await nest.derived(a, b ?? undefined)
        : null;
      if (text === null) throw new Error(`no such call: ${d.fn}`);
      m.ports[0].postMessage({ text });
    }
  } catch (e) {
    const error = String(e && e.message || e);
    if (m.ports[0]) m.ports[0].postMessage({ error });
    else self.postMessage({ nest: "failed", error });
  }
};
