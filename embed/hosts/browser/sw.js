// RFC-0075 S5: the nest's HTTP API on the page's own origin. A fetch for one of the node's query
// routes is relayed to the page that sent it, which hands it to its nest worker; the answer comes
// back as the node's status and bytes. Anything else goes to the network as usual. Nothing outside
// this browser can reach it: it is the HTTP API inside the page, not a server.

const ROUTES = /^\/(ready|sql|explain|entities|balances|derived)\/?$|^\/(entity|balance|derived)\/[^/]+(\/[^/]+)?\/?$/;

self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (event) => event.waitUntil(self.clients.claim()));

self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);
  const base = new URL(self.registration.scope).pathname.replace(/\/$/, "");
  if (url.origin !== self.location.origin || !url.pathname.startsWith(`${base}/`)) return;
  const route = url.pathname.slice(base.length);
  if (!ROUTES.test(route) || !event.clientId) return;
  event.respondWith(relay(event, route + url.search));
});

async function relay(event, target) {
  const client = await self.clients.get(event.clientId);
  if (!client) return new Response("no page holds this nest", { status: 503 });
  const body = new Uint8Array(await event.request.arrayBuffer());
  const { port1, port2 } = new MessageChannel();
  const answer = new Promise((resolve) => { port1.onmessage = (m) => resolve(m.data); });
  client.postMessage({ nest: "request", method: event.request.method, target, body }, [port2, body.buffer]);
  const a = await answer;
  if (a.error) return new Response(a.error, { status: 503 });
  return new Response(a.status === 204 ? null : a.body, {
    status: a.status,
    headers: a.contentType ? { "content-type": a.contentType } : {},
  });
}
