// RFC-0075 S5: a page's side of the relay. `startNest` starts the nest worker, registers the Service
// Worker over `scope`, and forwards each request the Service Worker sends to the worker. Once it
// resolves, `fetch(scope + "sql?q=...")` and the rest answer from the nest on this page, and so do
// the handle's `sql`, `entity` and `derived` (§7). `onFollowed` hears each later pass of the cursor.

export async function startNest({ scope, swUrl, workerUrl, open, onFollowed }) {
  const worker = new Worker(workerUrl, { type: "module" });
  const opened = new Promise((resolve, reject) => {
    worker.onmessage = (m) => {
      if (m.data.nest === "opened") resolve(m.data);
      if (m.data.nest === "failed") reject(new Error(m.data.error));
      if (m.data.nest === "followed" && onFollowed) onFollowed(m.data);
    };
  });
  worker.postMessage({ nest: "open", ...open });

  navigator.serviceWorker.addEventListener("message", (m) => {
    if (m.data && m.data.nest === "request") {
      worker.postMessage(m.data, [m.ports[0], m.data.body.buffer]);
    }
  });
  navigator.serviceWorker.startMessages();
  await navigator.serviceWorker.register(swUrl, { scope });
  if (!navigator.serviceWorker.controller) {
    await new Promise((resolve) =>
      navigator.serviceWorker.addEventListener("controllerchange", resolve, { once: true }));
  }

  const call = (fn, ...args) => new Promise((resolve, reject) => {
    const { port1, port2 } = new MessageChannel();
    port1.onmessage = (m) => (m.data.error ? reject(new Error(m.data.error)) : resolve(m.data.text));
    worker.postMessage({ nest: "call", fn, args }, [port2]);
  });
  return {
    opened: await opened,
    sql: (q) => call("sql", q),
    entity: (id) => call("entity", id),
    derived: (name, key) => call("derived", name, key),
  };
}

export async function openNest(options) {
  return (await startNest(options)).opened;
}
