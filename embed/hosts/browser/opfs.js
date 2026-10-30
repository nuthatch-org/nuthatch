// RFC-0077 S2: storage mode `host` over OPFS, for a dedicated worker. The nest's files sit under a
// directory named for it; `open` gives a sync access handle, which the storage imports call.

export async function opfsStorage(nid) {
  const root = await (await navigator.storage.getDirectory()).getDirectoryHandle(nid, { create: true });
  const parent = async (name, create) => {
    const parts = name.split("/");
    const leaf = parts.pop();
    let dir = root;
    for (const p of parts) dir = await dir.getDirectoryHandle(p, { create });
    return [dir, leaf];
  };
  return {
    async open(name) {
      const [dir, leaf] = await parent(name, true);
      const fh = await dir.getFileHandle(leaf, { create: true });
      return fh.createSyncAccessHandle();
    },
    async remove(name) {
      const [dir, leaf] = await parent(name, false);
      await dir.removeEntry(leaf, { recursive: true });
    },
    async list(prefix) {
      const names = [];
      const walk = async (dir, at) => {
        for await (const [n, h] of dir.entries()) {
          if (h.kind === "directory") await walk(h, `${at}${n}/`);
          else if (`${at}${n}`.startsWith(prefix)) names.push(`${at}${n}`);
        }
      };
      await walk(root, "");
      return names.sort();
    },
  };
}
