# nuthatch-count-receiver

The receiver for RFC-0054's head count. It runs behind Caddy on the Helsinki host at
`https://count.nuthatch-indexer.com`, increments some tallies and answers `204`.

What it stores, per UTC day, and nothing else:

| Key | Incremented by |
|-----|----------------|
| `counted` | every valid `counted` event |
| `init` | every valid `init` event |
| `init.version.<version>` | `init` |
| `init.os_arch.<os>/<arch>` | `init` |
| `init.chain.<id>` | `init` |
| `init.source.<source>` | `init` |

A `counted` event's `chain` and `source`, if present, are not tallied.

What it never stores: the request IP, any header, the body after the increment, or any time finer
than the UTC day. `src/main.rs` reads the request line, `Content-Length` and the body, and nothing
else; there is no code path that writes any of them. Caddy is configured with no `log` directive for
this site, so ingress keeps no access log either. Process logs are operational only.

Anyone can send a valid ping, so the totals are floors with noise on top, never counts. A day's
distinct keys are capped at 512; past that a new key is tallied under `init.<field>.other`, so a flood
of invented chain ids or versions cannot grow the store without bound.

`GET /totals` returns the whole store as JSON. It holds nothing that is not published anyway.

```
nuthatch-count-receiver --listen 127.0.0.1:8290 --store /var/lib/nuthatch-count/tallies.json
```
