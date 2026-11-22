# Hosted nests: from "I'll try it" to a link, in under an hour

A prospect says yes. We run a read-only nest for their contracts on the operator's host and send them a page.
This is the stock binary run as an operator would run it (see `CLAUDE.md`, out of scope: no accounts, no
billing, nothing in this tree built for it). The mechanics are the public-nest kit in
[public-nests.md](public-nests.md); this page is the order of work, and the two scripts that remove the typing.

What the prospect gets: `https://nuthatch-indexer.com/nests/<name>` (SQL at `/sql`, MCP at `/mcp`) and a
showcase page at `https://nuthatch-indexer.com/showcase/?nest=<name>`
([showcase/](../showcase/README.md)). No key, no account, read-only.

## Before you start (5 minutes)

Ask the prospect for: the chain, the contract addresses, and the block the contracts were deployed at (or how
far back they care). Check that the chain is one nuthatch indexes, and decide the RPC:

- A chain with a keyless public RPC: use it. Fine for a demo.
- Anything else, or a big backfill: a keyed URL (GraphOps for Base, for example) in a file you keep off the
  repo: `NUTHATCH_RPC=https://...`. The script copies it into the nest's env file and never prints it.

Pick `<name>`: lowercase, dashes, something the prospect would recognise (`acme-vault`). It becomes the URL.

## The runbook

Times are what to expect, not promises. Backfill dominates: Arc's 3.2 M events took 36 minutes at about
1,500 events a second; `--seal-direct` with a keyed RPC is faster. Start it first and do the rest while it runs.

**1. Generate (2 min, on your machine or the host).** Dry run, read the output:

```sh
deploy/hosted-nest/new-nest.sh acme-vault base 0xAddr1,0xAddr2 21000000 ~/secrets/acme.rpc.env
```

The last argument is optional. It prints the env file, the nest unit, the proxy unit, the nginx config, the
Caddy block, the two entries for nuthatch-frontend and a checklist. Nothing is written. For a nest someone has
already authored, pass its directory or git URL instead of addresses, and `-` for the block.

**2. Apply on the host (3 min).** The host needs the one-time setup in public-nests.md steps 0 to 2 (service
user, binary, nginx installed); it already has it if a nest is running. Then:

```sh
sudo deploy/hosted-nest/new-nest.sh --apply acme-vault base 0xAddr1,0xAddr2 21000000 ~/secrets/acme.rpc.env
```

It writes the files and appends the Caddy block, and starts nothing. Running it again changes nothing; a file
that differs is reported and left alone. Ports are the next free pair (nest 81xx, proxy 84xx).

**3. Create the nest (5 to 10 min).** The first checklist line: `nuthatch init` with the addresses as the
service user (it resolves ABIs from Sourcify, then Etherscan-class APIs; a proxy contract whose events do not
show may need `--abi`, see `nuthatch init --help`). Read `nuthatch.toml` and the tables it made. Remove events
the prospect does not need if the backfill would be huge. `chmod 0700` the directory.

**4. Start (then wait).**

```sh
sudo systemctl daemon-reload && sudo systemctl enable --now nuthatch-public-acme-vault
sudo journalctl -u nuthatch-public-acme-vault -f
curl -s 127.0.0.1:<port>/ready | grep -o '"lag_blocks":[0-9]*'
sudo systemctl enable --now nuthatch-public-acme-vault-proxy
```

`429`s from a public RPC during backfill are normal. While it runs, do 5 and 6.

**5. Route it (10 min).** `sudo caddy validate --config /etc/caddy/Caddyfile && sudo systemctl reload caddy`.
In nuthatch-frontend add the `vercel.json` rewrite and the `public-nests.ts` entry the script printed (fill in
the `for` and `summary` lines in plain words), and add `public/showcase/` if it is not there yet (see
`showcase/README.md`). Merging is not publishing: `vercel --prod`.

**6. Write the showcase config (10 min).** Copy `showcase/configs/_template.json` to `<name>.json` and fill in
the title, decimals and symbol, and five questions someone at their company would ask. Run
`showcase/check-examples.sh https://nuthatch-indexer.com/nests/<name>` once the nest has data: every example
query must say `ok`. Deploy the showcase again.

**7. Verify from outside (5 min), before you send anything.**

```sh
deploy/public-nests/check-proxy.sh https://nuthatch-indexer.com/nests/<name>     # routes, limits, /_admin 403
showcase/check-examples.sh https://nuthatch-indexer.com/nests/<name>
claude mcp add --transport http <name> https://nuthatch-indexer.com/nests/<name>/mcp
claude -p --model haiku "<each of the five prompts>"        # the answer must be right, not just present
```

Open the showcase on a phone as well. The lag chip should read a few blocks.

**8. Send the link.** One message: the showcase URL, the `claude mcp add` line, and "it is read-only, and tell
me what you want to ask it". Note in the tracking issue: name, port, start block, who it is for, date.

## What can go wrong

| Symptom | Cause |
|---|---|
| The nest refuses every request (a Host error) | `PUBLIC_HOST` in the env file and Caddy's `header_up Host` differ. The script writes both from one value. |
| Showcase says "Cannot reach this nest" | The page is not on the same origin as `/nests/<name>`, or the Vercel rewrite is not deployed. |
| Tables empty, no events | Wrong start block (after the first event), or the ABI resolved is a proxy's. Fix in the nest dir, `rm -r data`, restart. |
| `429` in the page | The visitor's own limit (30 queries a minute). It clears in a few seconds. |
| Backfill crawls | Public RPC limits. Give it a keyed URL in `NUTHATCH_RPC` and restart. |

## Removing a nest

```sh
deploy/hosted-nest/remove-nest.sh acme-vault                 # dry run
sudo deploy/hosted-nest/remove-nest.sh --apply acme-vault    # stops and removes units, nginx, env, Caddy block
sudo deploy/hosted-nest/remove-nest.sh --apply --purge-data acme-vault   # and deletes the data directory
```

Without `--purge-data` the data is kept, so putting the nest back is the same `new-nest.sh --apply` and a
start. It refuses the permanent demo nests. Then reload Caddy, delete the nest's rewrite and entry from
nuthatch-frontend (and its showcase config), `vercel --prod`, and tell the prospect.

## Limits worth telling a prospect

The limits in public-nests.md apply to every nest made this way: 5,000 rows and 10 s a query, 30 queries a
minute per address, a 2 GB memory ceiling per nest, read routes only, no admin. It is a showcase, not a
service with an uptime promise.

## Tested

`new-nest.sh` and `remove-nest.sh` were run against a scratch `NEST_ROOT` (dry run, apply, apply again,
remove). The generated units and nginx config come from the Base USDC files by substitution, and were not
started on a real host or run through `test-proxy.sh` in this change.

## Notes from hosting the lead nests (gooddollar-xdc, sablier-xdc, pharos-eth)

- **XDC needs a User-Agent.** `rpc.xinfin.network`, `erpc.` and `rpc1.` sit behind Cloudflare, which answers 403 to a
  request with no `User-Agent`, and nuthatch sends none (nuthatch#3068). From the host the nest never starts
  (`initial_poll_failed`). `deploy/hosted-nest/xdc-rpc-ua/` is a loopback nginx that adds one; set the nest's
  `rpc_urls = ["http://127.0.0.1:18550"]`. Not needed once #3068 is fixed.
- **A finished nest is copied without its data.** Copy `nuthatch.toml`, `abis/`, `views/` and `semantic.toml`, point
  `rpc_urls` at keyless endpoints (a measured config often points at a local counting proxy), and let the unit backfill.
- **`/ready` says `ready: true` and `lag_blocks: 0` while `--seal-direct` backfill runs.** Read `sealed_through`
  against the chain tip for progress.
- The showcase needs `block_timestamp`; a nest with `block_timestamps = false` shows its tables and SQL only.
- **The XDC shim is a separate host service and the nest units do not order after it.** After a reboot an XDC nest
  can start before `127.0.0.1:18550` listens and fail its first poll; `Restart=on-failure` brings it back. To avoid
  the failed first start, add `After=nuthatch-public-xdc-rpc-ua.service` and `Wants=` the same to that nest's unit.
- **Order for the scrape targets.** `deploy/monitoring/targets/nests.yml` scrapes the public nests on their tailnet
  address, which only answers once the unit runs with `--local-host 100.82.188.91`. Restart each public unit from the
  new unit file first, then land the targets; otherwise Prometheus raises TargetDown on a nest that is fine.

## Notes from hosting spendshield-aevum (Aevum testnet, a chain nuthatch has no registry entry for)

- **An unregistered chain gets `Depth(64)` finality and a 20-block window.** There is no key in `nuthatch.toml` for
  either (only the runtime's admin API takes `finality_depth`), so the `finalized` tag is never asked for, even when
  the RPC answers it (Aevum's does, about 960 blocks behind the tip). Pass `--window 1000` for an RPC that caps
  `eth_getLogs` at 1,000 blocks. A registry entry with `FinalizedTag` would need a release; the nest ran without one.
- **`sudo` drops `NEST_DEV_ARGS` and `NEST_LOCAL_HOST`** (`sudo -E` is ignored on the host). Run the script with
  `sudo env NEST_DEV_ARGS=... NEST_LOCAL_HOST=... deploy/hosted-nest/new-nest.sh --apply ...`, or the unit is written
  with the defaults (3 s poll, 4-way backfill) and a light-load nest hits the RPC much harder. Check `ExecStart`.
- **The generated nginx conf has no CORS block**; the live pharos-eth conf does (a 204 preflight and
  `Access-Control-Allow-Origin: *` on `/sql` and `/mcp`). Without it a browser cannot call the nest and the showcase
  page fails. Copy that conf for the new nest instead of the generated one until the template carries it.
