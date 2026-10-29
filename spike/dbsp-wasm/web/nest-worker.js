// RFC-0075 S5: the dedicated worker that runs the whole nest (§4) and answers its routes. The page
// posts `{nest: "open", ...}` once, then `{nest: "request", ...}` with a port for each answer.
import init, { LiveNest } from "../pkg-web/dbsp_wasm_spike.js";

let nest;

self.onmessage = async (m) => {
  const d = m.data;
  try {
    if (d.nest === "open") {
      await init();
      nest = await LiveNest.open(d.nid, d.rpc, d.contract, BigInt(d.start));
      nest.derive(JSON.stringify(d.files));
      if (d.pollIntervalSecs) nest.setPollInterval(BigInt(d.pollIntervalSecs));
      const followed = JSON.parse(await nest.follow());
      self.postMessage({ nest: "opened", last_block: followed.last_block, backend: followed.backend });
    } else if (d.nest === "request") {
      const a = await nest.request(d.method, d.target, d.body);
      const body = a.body;
      m.ports[0].postMessage({ status: a.status, contentType: a.contentType, body }, [body.buffer]);
    }
  } catch (e) {
    const error = String(e && e.message || e);
    if (m.ports[0]) m.ports[0].postMessage({ error });
    else self.postMessage({ nest: "failed", error });
  }
};
