// RFC-0075 S5: a page's side of the relay. `openNest` starts the nest worker, registers the Service
// Worker over `scope`, and forwards each request the Service Worker sends to the worker. Once it
// resolves, `fetch(scope + "sql?q=...")` and the rest answer from the nest on this page.

export async function openNest({ scope, swUrl, workerUrl, open }) {
  const worker = new Worker(workerUrl, { type: "module" });
  const opened = new Promise((resolve, reject) => {
    worker.onmessage = (m) => {
      if (m.data.nest === "opened") resolve(m.data);
      if (m.data.nest === "failed") reject(new Error(m.data.error));
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
  return opened;
}
