import { fill, checkRecording } from "./fill.js";

const $ = (id) => document.getElementById(id);
const CHUNK_ROWS = 200_000;
const [index, template] = await Promise.all([
  fetch("./specs.json").then((r) => r.json()),
  fetch("./player.html").then((r) => r.text()),
]);

let current = null;
let worker = null;

const mib = (n) => `${(n / 2 ** 20).toFixed(1)} MiB`;
const status = (el, text, err = false) => { el.textContent = text; el.className = err ? "status err" : "status"; };
const slug = (s) => s.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "").slice(0, 60) || "time-lapse";

function display() {
  return { decimals: Math.max(0, Number($("decimals").value) || 0), unit: $("unit").value.trim(), noun: $("noun").value.trim() || "keys" };
}
function setDisplay(d) {
  $("decimals").value = d?.decimals ?? 0;
  $("unit").value = d?.unit ?? "";
  $("noun").value = d?.noun ?? "keys";
}

function render() {
  if (!current) return;
  const html = fill(template, { ...current, display: display() });
  const frame = document.createElement("iframe");
  frame.title = current.manifest.title;
  frame.onload = () => { frame.style.height = `${frame.contentDocument.documentElement.scrollHeight + 8}px`; };
  frame.srcdoc = html;
  $("stage").replaceChildren(frame);
  current.html = html;
  $("export").disabled = false;
  $("save-rec").disabled = false;
  const m = current.manifest;
  $("facts").innerHTML = "";
  const facts = [
    ["source", current.source],
    ["frames_sha256", m.frames_sha256],
    ["frames", `${m.frames} of ${m.step_blocks} blocks, ${m.from_block} to ${m.to_block}`],
    ["export", `${(new Blob([html]).size / 1e6).toFixed(2)} MB, no requests`],
    ...(current.run ? [["replay", current.run]] : []),
  ];
  for (const [k, v] of facts) {
    const line = document.createElement("div");
    const b = document.createElement("b");
    b.textContent = k;
    line.append(b, ` ${v}`);
    $("facts").append(line);
  }
}

function download(name, text, type) {
  const a = document.createElement("a");
  a.href = URL.createObjectURL(new Blob([text], { type }));
  a.download = name;
  document.body.append(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(a.href), 10_000);
}

$("export").onclick = () => current && download(`${slug(current.manifest.title)}.html`, current.html, "text/html");
$("save-rec").onclick = () => {
  if (!current) return;
  download("frames.jsonl", current.jsonl, "application/x-ndjson");
  setTimeout(() => download("manifest.json", JSON.stringify(current.manifest, null, 2) + "\n", "application/json"), 300);
};
for (const id of ["decimals", "unit", "noun"]) $(id).addEventListener("change", render);

for (const [i, s] of index.specs.entries()) {
  const o = document.createElement("option");
  o.value = i;
  o.textContent = s.spec.replace(/^specs\//, "").replace(/\.toml$/, "");
  $("spec-pick").append(o);
}
async function pick() {
  const s = index.specs[Number($("spec-pick").value)];
  $("spec").value = await fetch(`./${s.spec}`).then((r) => r.text());
}
$("spec-pick").onchange = pick;
await pick();

function finish() {
  worker?.terminate();
  worker = null;
  $("run").disabled = false;
  $("stop").disabled = true;
}
$("stop").onclick = () => { finish(); status($("run-status"), "Stopped. Nothing was kept.", true); };

$("run").onclick = () => {
  const s = index.specs[Number($("spec-pick").value)];
  const spec = $("spec").value;
  const name = (p) => /^\s*nest\s*=\s*"([^"]*)"/m.exec(p)?.[1];
  const nest = name(spec);
  // The dataset belongs to the picked spec's nest; an edited spec naming another nest has none here.
  fetch(`./${s.spec}`).then((r) => r.text()).then((orig) => {
    if (nest !== name(orig)) { status($("run-status"), `This page mirrors ${name(orig)} for that spec, not ${nest}.`, true); return; }
    $("run").disabled = true;
    $("stop").disabled = false;
    status($("run-status"), "Starting the engine…");
    const t0 = performance.now();
    let peak = 0;
    worker = new Worker(new URL("./replay-worker.js", import.meta.url), { type: "module" });
    worker.onmessage = async ({ data }) => {
      if (data.progress) {
        const p = data.progress;
        peak = Math.max(peak, p.memory);
        $("run-status").innerHTML = "";
        $("run-status").append(`chunk ${p.done} / ${p.total} · ${p.fetched} segments, ${mib(p.bytes)} fetched and checked · wasm memory ${mib(p.memory)}`);
        return;
      }
      finish();
      if (data.error) { status($("run-status"), data.error, true); return; }
      const out = data.done, st = data.stats;
      const manifest = JSON.parse(out.manifest);
      current = {
        jsonl: out.jsonl,
        manifest,
        source: `replayed in this tab from ${index.base}/${s.dataset}`,
        run: `${((performance.now() - t0) / 1e3).toFixed(1)} s, ${out.events.toLocaleString("en-GB")} events, ${out.live_keys_peak.toLocaleString("en-GB")} live keys at peak, ${st.fetched} segments (${mib(st.bytes)}), wasm memory ${mib(Math.max(peak, st.memory))}, segment bytes held at peak ${mib(st.peak_held_bytes)}`,
      };
      status($("run-status"), `Done: ${manifest.frames} frames, frames_sha256 ${manifest.frames_sha256}`);
      setDisplay(s.display);
      render();
    };
    worker.onerror = (e) => { finish(); status($("run-status"), e.message || "the worker failed", true); };
    worker.postMessage({ base: `${index.base}/${s.dataset}`, spec, chunkRows: CHUNK_ROWS });
  });
};

$("files").onchange = async () => {
  const files = [...$("files").files];
  const jsonlFile = files.find((f) => f.name.endsWith(".jsonl"));
  const manifestFile = files.find((f) => f.name.endsWith(".json"));
  if (!jsonlFile || !manifestFile) { status($("load-status"), "Choose both frames.jsonl and manifest.json.", true); return; }
  try {
    const jsonl = await jsonlFile.text();
    const manifest = await checkRecording(jsonl, await manifestFile.text());
    current = { jsonl, manifest, source: `loaded from ${jsonlFile.name} and ${manifestFile.name}` };
    // A recording of one of this page's specs reads as that spec does; any other starts plain.
    const texts = await Promise.all(index.specs.map((s) => fetch(`./${s.spec}`).then((r) => r.text())));
    const same = index.specs.find((s, i) => /^\s*title\s*=\s*"([^"]*)"/m.exec(texts[i])?.[1] === manifest.title);
    setDisplay(same?.display);
    status($("load-status"), `Loaded: ${manifest.frames} frames, and they hash to the manifest's frames_sha256.`);
    render();
  } catch (e) {
    status($("load-status"), String(e.message ?? e), true);
  }
};
