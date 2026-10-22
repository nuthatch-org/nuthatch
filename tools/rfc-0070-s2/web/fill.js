// A recording into the player template: one self-contained .html, the same bytes from the page and
// from export.mjs. The frames go in as one JSON string, so the player hashes exactly frames.jsonl.

const SLOTS = { frames: '"__FRAMES__"', manifest: '{"__MANIFEST__":1}', display: '{"__DISPLAY__":1}' };

// `<` is escaped so no value can close the script element it sits in.
const embed = (value) => JSON.stringify(value).replace(/</g, "\\u003c");

export function fill(template, { jsonl, manifest, display }) {
  let out = template;
  for (const [slot, value] of [["frames", jsonl], ["manifest", manifest], ["display", display]]) {
    const at = out.indexOf(SLOTS[slot]);
    if (at < 0 || out.indexOf(SLOTS[slot], at + 1) >= 0) throw new Error(`the template needs exactly one ${slot} slot`);
    out = out.slice(0, at) + embed(value) + out.slice(at + SLOTS[slot].length);
  }
  return out;
}

export async function sha256Hex(text) {
  const d = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return [...new Uint8Array(d)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

/// A recording as the page and export.mjs both accept it, refused unless frames.jsonl hashes to the
/// manifest's frames_sha256 and holds as many frames as it says.
export async function checkRecording(jsonl, manifestText) {
  const manifest = JSON.parse(manifestText);
  for (const k of ["title", "frames_sha256", "frames", "top"]) {
    if (manifest[k] === undefined) throw new Error(`manifest.json has no ${k}: is it a time-lapse manifest?`);
  }
  // S0's harness wrote no `fold`; it built only `sum`.
  manifest.fold ??= "sum";
  const got = await sha256Hex(jsonl);
  if (got !== manifest.frames_sha256) throw new Error(`frames.jsonl hashes to ${got}, not the manifest's ${manifest.frames_sha256}`);
  const lines = jsonl.split("\n").filter(Boolean).length;
  if (lines !== manifest.frames) throw new Error(`frames.jsonl holds ${lines} frames; the manifest says ${manifest.frames}`);
  return manifest;
}
