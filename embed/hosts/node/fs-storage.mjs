// RFC-0077 S2: storage mode `host` over files, so the browser host (`../browser/host.js`) runs in
// Node. Each file answers the sync access handle's methods, which is all `host.js` calls.
import fs from "node:fs";
import path from "node:path";

export function fsStorage(root) {
  fs.mkdirSync(root, { recursive: true });
  return {
    async open(name) {
      const p = path.join(root, name);
      fs.mkdirSync(path.dirname(p), { recursive: true });
      const fd = fs.openSync(p, fs.existsSync(p) ? "r+" : "w+");
      return {
        read: (buf, { at }) => fs.readSync(fd, buf, 0, buf.length, at),
        write: (buf, { at }) => fs.writeSync(fd, buf, 0, buf.length, at),
        getSize: () => fs.fstatSync(fd).size,
        truncate: (n) => fs.ftruncateSync(fd, n),
        flush: () => fs.fdatasyncSync(fd),
        close: () => fs.closeSync(fd),
      };
    },
    async remove(name) {
      fs.rmSync(path.join(root, name), { recursive: true, force: true });
    },
    async list(prefix) {
      return fs.readdirSync(root, { recursive: true, withFileTypes: true })
        .filter((e) => e.isFile())
        .map((e) => path.relative(root, path.join(e.parentPath, e.name)).split(path.sep).join("/"))
        .filter((n) => n.startsWith(prefix))
        .sort();
    },
  };
}
