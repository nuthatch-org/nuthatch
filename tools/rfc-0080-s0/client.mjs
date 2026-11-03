// RFC-0080 §20: hold one live `/sync` stream per shape against a follower.
//
//   node client.mjs <follower base url> <route> <shapes.jsonl>
//
// Connects at most 64 at a time (macOS's listen backlog is 128), waits for every stream's
// `{"up_to_date":true}`, prints `attached <n>` once, then counts what each stream receives. On SIGTERM
// it prints `frames <n> ended <n> refused <n>` and exits.
import http from "node:http";
import { readFileSync } from "node:fs";

const [base, route, file] = process.argv.slice(2);
const shapes = readFileSync(file, "utf8").split("\n").filter(Boolean);
const agent = new http.Agent({ keepAlive: false, maxSockets: Infinity });
const url = new URL(base);
let attached = 0, frames = 0, ended = 0, refused = 0, busy = 0, next = 0, inflight = 0;
const RAMP = 64;

function open(shape) {
  return new Promise((done) => {
    const path = `/${route}/sync?s=${encodeURIComponent(shape)}&live=1`;
    const req = http.get({ host: url.hostname, port: url.port, path, agent }, (res) => {
      if (res.statusCode === 503 || res.statusCode === 429) {
        // The SQL gate is full: a client takes its first answer again, as it would in production.
        busy++;
        res.resume();
        setTimeout(() => open(shape).then(done), 20 + Math.random() * 180);
        return;
      }
      if (res.statusCode !== 200) {
        refused++;
        let body = "";
        res.on("data", (c) => (body += c));
        res.on("end", () => {
          if (refused <= 3) console.error(`refused ${res.statusCode}: ${body.slice(0, 200)}`);
          done();
        });
        return;
      }
      let buf = "", up = false;
      res.setEncoding("utf8");
      res.on("data", (chunk) => {
        buf += chunk;
        let i;
        while ((i = buf.indexOf("\n")) >= 0) {
          const line = buf.slice(0, i);
          buf = buf.slice(i + 1);
          if (!up) {
            if (line.includes('"up_to_date":true')) {
              up = true;
              attached++;
              done();
            }
          } else if (line.length) {
            frames++;
          }
        }
      });
      res.on("end", () => {
        ended++;
        if (!up) done();
      });
    });
    req.on("error", (e) => {
      refused++;
      if (refused <= 3) console.error(`error: ${e.message}`);
      done();
    });
  });
}

function pump() {
  while (inflight < RAMP && next < shapes.length) {
    inflight++;
    open(shapes[next++]).then(() => {
      inflight--;
      if (next < shapes.length) pump();
      else if (inflight === 0) console.log(`attached ${attached}`);
    });
  }
}

process.on("SIGTERM", () => {
  console.log(`frames ${frames} ended ${ended} refused ${refused} busy ${busy}`);
  process.exit(0);
});
if (shapes.length === 0) console.log("attached 0");
pump();
