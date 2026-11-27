# Public read-only demo nests

How to put a nest on a public URL that anyone can query, without an account and without being able to
change anything. First user: the Arc testnet USDC nest
([#3031](https://github.com/nuthatch-org/nuthatch/issues/3031), under
[#3028](https://github.com/nuthatch-org/nuthatch/issues/3028)); second, [Base USDC](#the-second-nest-base-usdc-on-the-graphops-rpc),
the one on a keyed RPC. The kit is in [`deploy/public-nests/`](../deploy/public-nests/).

This is the stock binary run as an operator would run it. Nothing in nuthatch is built for it: no
accounts, no billing, no hosted-service code (see the out-of-scope list in `CLAUDE.md`). The credentials,
the host and the DNS name stay the operator's.

```
internet -> Caddy :443            TLS, the public name           (already on the host)
         -> nginx 127.0.0.1:8480  route allowlist, per-IP limits  deploy/public-nests/nginx.conf
         -> nuthatch 127.0.0.1:8120  --no-admin, read routes only
```

The URL people are given is a path on the main site, `https://nuthatch-indexer.com/nests/<name>`, not a name of
its own: [see below](#the-public-url-is-a-path-not-a-subdomain). A new nest needs no DNS record.

## Live

The Arc testnet USDC nest has been live since 2026-11-19 at **https://nuthatch-indexer.com/nests/arc-usdc**, and
still at https://arc.nuthatch-indexer.com (helsinki-nests,
unit `nuthatch-public-arc-testnet` behind `nuthatch-public-proxy` and Caddy, nuthatch 5.23.0, Arc's keyless
public RPCs). Cold backfill from block 72,550,000 took 35 min 52 s (3,253,137 events, about 1,500 events a
second, 221 MB on disk, 423 MB peak memory), then it follows the tip at `lag_blocks` 0. Smoke-tested from
outside: `/sql` and `/mcp` `tools/list` answer `200` with no key, `check-proxy.sh` passes including
`--burst`, `/_admin` and `/metrics` answer `403`.

```sh
curl -sG "https://nuthatch-indexer.com/nests/arc-usdc/sql" --data-urlencode \
  'q=SELECT block_number, tx_hash, "from", "to", value_dec FROM usdc__transfer ORDER BY block_number DESC, log_index DESC LIMIT 3'
```

The Base USDC nest is the second one, at **https://nuthatch-indexer.com/nests/base-usdc** (it has no name of its
own; helsinki-nests, unit `nuthatch-public-base-usdc` behind `nuthatch-public-base-usdc-proxy` and Caddy, nuthatch
5.23.0, the GraphOps RPC; see [the section below](#the-second-nest-base-usdc-on-the-graphops-rpc)).

```sh
curl -sG "https://nuthatch-indexer.com/nests/base-usdc/sql" --data-urlencode \
  'q=SELECT block_number, tx_hash, "from", "to", value_dec FROM usdc__transfer ORDER BY block_number DESC, log_index DESC LIMIT 3'
```

For an agent, either nest speaks MCP over HTTP, with no key. With Claude Code:

```sh
claude mcp add --transport http arc-usdc https://nuthatch-indexer.com/nests/arc-usdc/mcp
claude mcp add --transport http base-usdc https://nuthatch-indexer.com/nests/base-usdc/mcp
```

On the host, `nginx` was already installed and in use, so step 5's `apt-get install`, `policy-rc.d` and
`systemctl disable nginx` were skipped, as the note under step 5 says. The Caddyfile was backed up to
`/etc/caddy/Caddyfile.bak-public-nests-<timestamp>` before the site was appended.

## The public URL is a path, not a subdomain

`https://nuthatch-indexer.com/nests/<name>` is what people are given, as it is for the other hosted nests
(`bounty-nest`, `irmarket`, ...). Two pieces make it work, and neither is a DNS record:

1. **nuthatch-frontend**, `vercel.json`: `/nests/<name>/:path*` rewrites to `https://<name>.89.167.109.4.sslip.io/:path*`,
   and `src/data/public-nests.ts` has an entry, which renders the page at `/nests/<name>` (status, query it,
   tables) from the live nest at build. A name under `sslip.io` already resolves to the host.
2. **Caddy on the host**: [`Caddyfile.path`](../deploy/public-nests/Caddyfile.path) has a site per nest on that
   `sslip.io` name. It forwards to the same nginx as before, so the route allowlist, body and time limits and
   per-address limits are unchanged.

Through Vercel, a request reaches Caddy from one of Vercel's addresses, and a different one most times, so left
alone nginx would count all visitors as one caller. Caddy therefore takes the visitor's address from
`X-Vercel-Forwarded-For`, which Vercel sets and overwrites, and passes it on as `X-Forwarded-For`. It also sends
the `Host` the nest was started with (`--public-host`), since the request arrives under the `sslip.io` name.
`POST /mcp` and `POST /sql` pass through the rewrite as they do direct. The reasoning and what was tested are in
the comment at the top of `Caddyfile.path`.

Checked on 2026-11-19 against the real URLs: `check-proxy.sh https://nuthatch-indexer.com/nests/base-usdc --burst` and
the same for `/nests/arc-usdc` pass every check except where Vercel answers for the host (a trailing-slash
`/_admin/` is a 404 and `//_admin/` a 308, both refusals, which the script now accepts); `/sql` and `POST /mcp`
`tools/list` answer `200`, and the flood from one address is answered `429`.

To add one: start the nest and its nginx as above, add a site to `Caddyfile.path` on the host (a new name
`<name>.89.167.109.4.sslip.io`, the port of its nginx and the `Host` it was started with), and add the rewrite and
the `public-nests.ts` entry to nuthatch-frontend. Merging that repo is not publishing it: the site is deployed
with `vercel --prod` (see its README).

## What nuthatch's read-only surface is

Checked against the code (`src/serve.rs`, `mount_router`, and `src/cli.rs`, `DevArgs`) and against a
5.23.0 binary on 2026-11-18. **No new Rust is needed**: the binary can already run with no write and no
admin surface.

- **Nothing on a nest's router writes.** Every route is a `GET`, except `POST /sql`, `POST /explain`,
  `POST /q/{name}` and `/mcp`, which read. `PUT`, `DELETE` and `PATCH` get `405`. `/sql` accepts `SELECT`
  and `WITH` only; a `COPY` or a `DELETE` is refused with "only SELECT/WITH queries are allowed on the
  read-only SQL surface".
- **The admin surface is the only other thing, and a flag removes it.** `--no-admin` means the
  `/_admin*` routes are never registered, so they answer `404`, not a login. (`dev` on a loopback bind
  would otherwise serve them, token-gated.) This unit passes it.
- **`/mcp` offers only the tools whose routes the proxy serves.** The env file sets
  `NUTHATCH_MCP_TOOLS=status,schema,tables,table,sql,explain`, so `tools/list` returns those and a
  `tools/call` for any other (`entity`, `diff`, `at`, `balance`, `top_balances`) is refused as unknown.
  The prompts that name a withheld tool are withheld too. Unset, a nest offers every query tool; set but
  empty, none. The authoring tools (`init_nest`, `start_indexing`, ...) stay on stdio. A request carrying a non-loopback `Origin` is refused,
  so a web page cannot use it; `curl` and MCP clients send none.
- **A loopback bind checks `Host`** (#3002), so the unit names the public name with `--public-host`.
- **`/metrics` is a read route** and shows operator-side counters. It is not public here: the proxy refuses it.

The proxy blocks the same things anyway. `nuthatch dev` on a non-loopback bind, a later flag change or a
future route would otherwise decide what the internet sees; the allowlist in `nginx.conf` makes that
decision in one place. The tool calls `/mcp` makes run in process and are not
affected by it, which is why the tool list is cut at the nest (`NUTHATCH_MCP_TOOLS`) and not only here;
`test-proxy.sh` fails if the list names a tool whose route the proxy 404s.

## Arc testnet: what was verified

Read on 2026-11-18, not taken from the nest's README:

| Fact | Value | Source |
|---|---|---|
| Chain ID (testnet) | `5042002` (`0x4cef52`) | [Connect to Arc](https://docs.arc.io/arc/references/connect-to-arc) and [RPC endpoints](https://docs.arc.io/arc/references/rpc-endpoints), "Network parameters" |
| Primary public RPC | `https://rpc.testnet.arc.io`, keyless | same pages |
| Other testnet RPCs | Blockdaemon, dRPC and QuickNode (`https://rpc.quicknode.testnet.arc.io`) | [RPC endpoints](https://docs.arc.io/arc/references/rpc-endpoints), "Testnet endpoints" |
| `eth_getLogs` range | 10,000 blocks, `-32012` above it | RPC endpoints, "Supported methods" |
| Live check | both `rpc.testnet.arc.io` and `rpc.quicknode.testnet.arc.io` answered `eth_chainId` with `0x4cef52` | `curl`, 2026-11-18 |

(Arc mainnet exists now, chain ID `5042`. This kit is testnet only.)

**GraphOps RPC and Arc: not determined, so this deploy uses Arc's public RPC.** Arc's docs list no GraphOps
endpoint, and I have no GraphOps credentials or provider list to ask. #3028 says "GraphOps RPC where Arc is
available"; if it is, put its URL in `ARC_RPC_URL` and keep an Arc public endpoint as `ARC_RPC_URL_2`. The
nest was measured against the two public endpoints in its README (backfill 17 minutes, `429` throughout,
nuthatch waits each one out), and following the tip at a 2 s poll is a few requests a second, so the
public endpoints are enough for a demo. They carry no promise, which is the reason to prefer a keyed one
if the demo is going to stay up.

## The limits, and where each is set

The routes, methods, rates, body size and the proxy's timeout are exercised by
`deploy/public-nests/test-proxy.sh`. The result-row cap was exercised on a 5.23.0 nest with the env
file's values (a 106,000-row table answered 5,000 rows, `truncated: true`). The 10 s statement timeout
and the two-permit gate are nuthatch's documented guards and were not provoked.

| Limit | Value | Where |
|---|---|---|
| Routes served | `/`, `/health`, `/ready`, `/nest`, `/shape`, `/schema`, `/tables`, `/table/<name>`, `/sql`, `/explain`, `/mcp`; anything else `404` | `nginx.conf` |
| `/_admin*`, `/metrics` | `403` | `nginx.conf` (the nest has no `/_admin` either) |
| Methods | `/sql`, `/explain`: GET, POST. `/mcp`: POST; any other method `405` with `Allow: POST, OPTIONS` (no GET SSE stream is offered, and a `403` reads as blocked to a client or a directory scanner probing the URL). The rest: GET. Others `403` | `nginx.conf` |
| Query rate, per IP | 30 a minute, burst 15, then `429` (`/sql`, `/explain`, `/mcp` share it) | `nginx.conf` `per_ip_query` |
| Query rate, all callers | 5 a second, burst 40, then `429` | `nginx.conf` `all_query` |
| Read rate, per IP | 120 a minute, burst 30 | `nginx.conf` `per_ip_read` |
| Connections, per IP | 4 at once, then `429` | `nginx.conf` `per_ip_conn` |
| Request body | 32 KiB, then `413` (Caddy caps at 64 KiB first) | `nginx.conf`, `Caddyfile.arc-testnet` |
| Header and body read time | 5 s | `nginx.conf` |
| Query time | 10 s at the nest, 15 s at the proxy (`504`) | env file `NUTHATCH_SQL_TIMEOUT_MS`; `proxy_read_timeout` |
| Result size | 5,000 rows and 8 MiB, then `truncated: true` | env file `NUTHATCH_SQL_MAX_ROWS`, `..._MAX_RESULT_BYTES` |
| Concurrent queries | 2 at the nest, a 250 ms wait, then `503` | nuthatch's default ([operators.md](operators.md), "Query guards") |
| Memory | 2 GiB for the nest, 256 MiB for nginx | `MemoryMax` in the units |

The caller's address comes from `X-Forwarded-For`, which only loopback may set, and Caddy overwrites it
with the address it saw. Reaching nginx some other way gets no spoofed identity.

## Deploy: what to run on helsinki-nests

Assumes a Debian or Ubuntu host with systemd, a public IP, and Caddy already serving 80 and 443 (the
Helsinki setup in [`deploy/monitoring/README.md`](../deploy/monitoring/README.md)). Run from a checkout of
this repo as a user with `sudo`. Nothing below touches any other user's files or units. Read each
`PORT` and name before you run it.

**0. Pick the public name and make sure the ports are free.**

```sh
export PUBLIC_HOST=arc.<your-domain>          # the name you will point DNS at
ss -ltn '( sport = :8120 or sport = :8480 )'  # must print only the header; else change the port everywhere
```

If a port is taken, change it in `nuthatch-public-arc-testnet.service` (`--listen`) and `nginx.conf`
(`upstream` and `listen`) together. 8120 is free in the 81xx range the Helsinki metrics site already
scrapes.

**1. Create the service user.** No login, no shell, its own state directory.

```sh
sudo useradd --system --user-group --home-dir /var/lib/nuthatch-public --create-home \
     --shell /usr/sbin/nologin nests-public
sudo chmod 0750 /var/lib/nuthatch-public
```

**2. Install the binary.** `scripts/deploy-nest.sh install` copies it to `/usr/local/bin/nuthatch-<version>`
after checking it reports that version. The unit names 5.23.0, the release this kit was tested with.

```sh
REPO=$PWD                                      # this checkout
mkdir -p ~/nuthatch-dl && cd ~/nuthatch-dl
gh release download v5.23.0 --repo nuthatch-org/nuthatch -p 'nuthatch-x86_64-unknown-linux-gnu.tar.gz*'
sha256sum -c nuthatch-x86_64-unknown-linux-gnu.tar.gz.sha256
tar xzf nuthatch-x86_64-unknown-linux-gnu.tar.gz
cd "$REPO" && sudo scripts/deploy-nest.sh install ~/nuthatch-dl/nuthatch 5.23.0
/usr/local/bin/nuthatch-5.23.0 --version      # nuthatch 5.23.0
```

(Use the `aarch64-unknown-linux-gnu` asset on an ARM host. If `gh` is not on the box, download the same
two files from the release page on a machine that has it and `scp` them.)

**3. Fetch the nest.** `git clone` pinned to the commit the kit was built against; the nest is a directory
of config, ABIs and views.

```sh
sudo -u nests-public -H git -C /var/lib/nuthatch-public clone https://github.com/nuthatch-org/arc-usdc-nest
sudo -u nests-public -H git -C /var/lib/nuthatch-public/arc-usdc-nest checkout 2ac2ce7a2f0e72ced7236b308c264d2b6976b54d
sudo chmod 0700 /var/lib/nuthatch-public/arc-usdc-nest
```

**4. The env file.** The RPC URLs and the public name. It holds no secret as shipped, and would if you
put a keyed RPC URL in it.

```sh
sudo install -d -m 0750 -o root -g nests-public /etc/nuthatch-public
sudo install -m 0640 -o root -g nests-public deploy/public-nests/arc-testnet.env.example \
     /etc/nuthatch-public/arc-testnet.env
sudo sed -i "s/^PUBLIC_HOST=.*/PUBLIC_HOST=$PUBLIC_HOST/" /etc/nuthatch-public/arc-testnet.env
sudoedit /etc/nuthatch-public/arc-testnet.env       # RPC URLs, if you are using a keyed one
sudo grep -n '^PUBLIC_HOST\|^ARC_RPC' /etc/nuthatch-public/arc-testnet.env   # read it back; "example.org" must be gone
```

**5. Install the proxy config and both units.**

```sh
# The package would start its own nginx on port 80, which Caddy holds, and fail the install. Say no to that
# one start (Debian's policy-rc.d), install, and put the policy file back.
printf '#!/bin/sh\nexit 101\n' | sudo tee /usr/sbin/policy-rc.d >/dev/null && sudo chmod +x /usr/sbin/policy-rc.d
sudo apt-get install -y nginx
sudo rm -f /usr/sbin/policy-rc.d
sudo systemctl disable nginx                   # the packaged instance; this kit runs its own
sudo install -m 0644 deploy/public-nests/nginx.conf /etc/nuthatch-public/nginx.conf
sudo install -m 0644 deploy/public-nests/nuthatch-public-arc-testnet.service /etc/systemd/system/
sudo install -m 0644 deploy/public-nests/nuthatch-public-proxy.service /etc/systemd/system/
sudo systemctl daemon-reload
```

If this host already runs nginx for something else, skip the whole `policy-rc.d` block and the `disable`:
this kit never touches the system instance, it only uses the binary.

**6. Prove the proxy before it is public.** This runs the same nginx config against a stub on high ports;
no root, no network, about two minutes.

```sh
NGINX_BIN=/usr/sbin/nginx deploy/public-nests/test-proxy.sh
```

It must print `all checks passed` twice (the second after a flood from one address). If it says `no nginx`,
step 5 did not install it.

**7. Start the nest, then the proxy.**

```sh
sudo systemctl enable --now nuthatch-public-arc-testnet
sudo journalctl -u nuthatch-public-arc-testnet -f      # "API live on http://127.0.0.1:8120", then backfill progress
```

Backfill runs from block 72,550,000, and the public RPC will answer `429` while it does; nuthatch waits
each one out and logs it, which is not a fault. The nest's README measured 178,433 blocks in 17 minutes
under the same `429`s; the range today is about twice that, so **expect 30 to 40 minutes before it is at
the tip** (an extrapolation from that run, not a measurement). Watch it:

```sh
curl -s http://127.0.0.1:8120/ready | grep -o '"lag_blocks":[0-9]*'      # until it is a handful
sudo systemctl enable --now nuthatch-public-proxy
curl -s http://127.0.0.1:8480/health
```

Then a check of the nest through the proxy, on the box, and *before* DNS (the Host the nest has been
told is `$PUBLIC_HOST`, so send it):

```sh
curl -s -H "Host: $PUBLIC_HOST" http://127.0.0.1:8480/ready
```

**8. DNS and TLS.** Point an `A` (and `AAAA`) record for `$PUBLIC_HOST` at the host's public IP. Then add
the site to Caddy, which gets the certificate by itself on first request:

```sh
sudo cp /etc/caddy/Caddyfile /etc/caddy/Caddyfile.bak-public-nests
sed "s/arc.example.org/$PUBLIC_HOST/" deploy/public-nests/Caddyfile.arc-testnet | sudo tee -a /etc/caddy/Caddyfile >/dev/null
sudo caddy validate --config /etc/caddy/Caddyfile && sudo systemctl reload caddy
```

If `validate` fails, `sudo cp /etc/caddy/Caddyfile.bak-public-nests /etc/caddy/Caddyfile` puts it back.

**9. Smoke test, from a machine that is not the host.**

```sh
export PUBLIC_URL=https://$PUBLIC_HOST

# 1. SQL
curl -sG "$PUBLIC_URL/sql" --data-urlencode \
  'q=SELECT block_number, tx_hash, "from", "to", value_dec FROM usdc__transfer ORDER BY block_number DESC, log_index DESC LIMIT 3'

# 2. MCP tools/list
curl -s "$PUBLIC_URL/mcp" -H 'content-type: application/json' -H 'accept: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'

# 3. how far behind the Arc head it is
curl -s "$PUBLIC_URL/ready" | grep -o '"lag_blocks":[0-9]*'

# 4. the same checks the test ran, against the real URL (read-only; about 50 requests)
deploy/public-nests/check-proxy.sh "$PUBLIC_URL"
```

1 and 2 must answer `200` without a header; 3 must be a few blocks (the nest's README measured 5 or 6); 4 must
end `all checks passed`. `check-proxy.sh "$PUBLIC_URL" --burst` also floods `/sql` and expects `429`: run it
from somewhere you can afford to have limited for a minute.

**10. Close the issues.** When 1 to 4 pass from outside, comment the URL and the curl in #3031 and #3028
(`Closes`, not `Refs`), and put `deploy/public-nests/arc-usdc-nest-README-section.md` at the end of the
nest repo's README with `<PUBLIC_URL>` replaced.

## Operating it

- **Logs.** `journalctl -u nuthatch-public-arc-testnet`, and `journalctl -t nuthatch_public` for the proxy's
  access log. The access log has callers' addresses and query strings, and journald's own retention applies.
- **A new nuthatch.** `sudo scripts/deploy-nest.sh install <binary> <version>`, change the version in the
  unit's `ExecStart`, `daemon-reload`, `restart`. The nest's README measured a restart after a minute and a
  half down at the tip again in 1 s.
- **Stop it.** `sudo systemctl disable --now nuthatch-public-proxy nuthatch-public-arc-testnet`, and remove
  the Caddy site. The data is in `/var/lib/nuthatch-public/arc-usdc-nest/data`; `nuthatch prune` or `rm -r`
  reclaims it.
- **Monitoring.** Both public units are in `deploy/monitoring/targets/nests.yml`
  (`nuthatch-public-arc-testnet` on 8120, `nuthatch-public-base-usdc` on 8121); the tailnet-only Caddy site
  maps `/8120/metrics` and `/8121/metrics`. A nest on a loopback bind refuses a `Host` it was not told, and
  Prometheus reaches it by the tailnet address, so each unit also passes `--local-host 100.82.188.91`.
- **Raising a limit** is one line in `nginx.conf` or the env file, then `systemctl reload
  nuthatch-public-proxy` or `restart nuthatch-public-arc-testnet`. Change the table above with it.

## The second nest: Base USDC, on the GraphOps RPC

Circle's native USDC on Base (`0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913`), from
[`nuthatch-org/usdc-base-nest`](https://github.com/nuthatch-org/usdc-base-nest), for
[#3028](https://github.com/nuthatch-org/nuthatch/issues/3028)'s "a chain people recognise, on the GraphOps RPC".
Everything above applies; this is what differs, and the files are `*-base-usdc*` beside the Arc ones.

| | Arc testnet USDC | Base USDC |
|---|---|---|
| Public URL | `https://nuthatch-indexer.com/nests/arc-usdc` (and `arc.nuthatch-indexer.com`) | `https://nuthatch-indexer.com/nests/base-usdc` |
| Unit, port | `nuthatch-public-arc-testnet`, 8120 | `nuthatch-public-base-usdc`, 8121 |
| Proxy | `nuthatch-public-proxy`, `nginx.conf`, 8480 | `nuthatch-public-base-usdc-proxy`, `nginx-base-usdc.conf`, 8481 |
| Env file | `arc-testnet.env` | `base-usdc.env` |
| RPC | Arc's keyless public endpoints | GraphOps, a key limited to RPC, in `NUTHATCH_RPC` |
| Nest | `nuthatch-org/arc-usdc-nest` | `nuthatch-org/usdc-base-nest` |
| History | from block 72,550,000 | the last 3,000,000 blocks (`--backfill`) |

- **A second proxy, not a second route.** `nginx-base-usdc.conf` is `nginx.conf` with the two ports, the runtime
  directory and the syslog tag changed (`diff nginx.conf nginx-base-usdc.conf` is those lines), so the limits
  are the same table above, counted per nest, and a change to one front door cannot take the other down.
- **The key is in one file.** GraphOps' URL is `https://rpc.graphops.xyz/v1/evm/8453/<api key>` (chain id 8453;
  the key is the last segment). It goes in `base-usdc.env` as `NUTHATCH_RPC`, which `nuthatch dev` reads as
  `--rpc`, so it is not in the unit, not in `ps`, not in `systemctl status` and not in this repo.
  `base-usdc.env.example` has a placeholder. Checked on 2026-11-19 that the key does not come back on a public route:
  `/`, `/health`, `/ready`, `/nest`, `/shape`, `/schema`, `/tables`, `/mcp` (`status`) and `/sql` through the proxy
  contained neither the RPC host nor `/v1/evm`. Make the key with `graphops api-keys create` limited to RPC for
  Base, so it can be revoked without touching another nest.
- **Not from genesis.** The nest declares `start_block = 2797221`, the proxy's deployment. Base now makes a block
  every 200 ms, so that is 56.7 M blocks. `--backfill 3000000` overrides it on the
  first run and starts seven days back (3 M blocks at 5 a second is 6.9 days); a store that has indexed a block
  ignores the flag on restart. The 7.6 M block the implementation contract was deployed at is long behind it, so
  the vendored ABI decodes everything in range.
- **GraphOps answers at most 20,000 logs a call** (`-32012`, "query exceeds max results 20000"). USDC on Base
  is about 9 Transfers and 13 events of any kind a block (28.0 M Transfers over 3.02 M blocks is 9.3 a block). The cap counts every log the `eth_getLogs` filter matches: the USDC address and the topic0s of the events in the nest's ABI. That includes more than `Transfer`, so the ceiling lies between 20,000 / 13 (about 1,540 blocks, if all 13 events match) and 20,000 / 9 (about 2,200 blocks, Transfers only); the figures here do not say how many of the 13 the ABI's topic0 list matches. `--window 1500` is under the lower of the two, so it is safe either way, and a window over about 1,540 blocks may be refused;
  the unit sets `--window 1500` rather than finding that out by refusals. `--seal-direct --concurrency 4`
  backfills straight to sealed segments, four windows at a time.
- **The key stays out of error responses.** Checked on 2026-11-20 with nuthatch 5.23.0: `NUTHATCH_RPC` pointed at
  a closed local port, and at a local server answering every POST with HTTP 500, each with the dummy path segment
  `SECRETKEYSEG123`. `/ready`, `/health`, `/nest`, `/metrics`, `/status` and `/` and the process log were searched for it
  and it appeared in none: the endpoint is logged as scheme, host and port only (`redact_url`). That is a test of
  those two failure shapes, not of every upstream error body.
- **What it costs in credits.** GraphOps prices `eth_getLogs` and `eth_call` at 2 credits and a block header at 1
  (docs/lodestar-rpc-cost-graphops.md). The backfill is the log calls, about 9,300 of them, so about 19,000
  credits. Following the tip is a tip call, one log call and a header for each block since the last poll
  (`block_timestamps`), about 6 credits a second at `--poll-interval 6s`: roughly 0.5 M a day, 15 M a month.
  [Base USDC: measured](#base-usdc-measured) has the counts.
- **Deploy.** As steps 1 to 10, with the files above, `git clone https://github.com/nuthatch-org/usdc-base-nest`
  in step 3 and `PUBLIC_HOST=base.<your-domain>`. The service user, the binary and nginx are already there from
  the Arc deploy. Check before the DNS record exists, on the box, with `curl -s -H "Host: $PUBLIC_HOST"
  http://127.0.0.1:8481/ready`. Caddy retries the certificate with back-off until the record resolves.

### Base USDC: measured

Read from the nest's own `/metrics` and `/ready` on 2026-11-19, nuthatch 5.23.0, one RPC endpoint (GraphOps),
`--seal-direct --concurrency 4 --window 1500`:

| | |
|---|---|
| Range indexed | blocks 56,516,981 to the tip (59,537,520 at the check), 6.99 days |
| Backfill time | 65 min (13:09:32 to 14:15:31 UTC), 28.0 M `Transfer` rows, about 7,100 a second |
| Backfill RPC | 9,261 `eth_getLogs`, 0 failover retries (`nuthatch_rpc_endpoint_retries_total` counts only retries onto another endpoint, and there is one), 2 failed calls that nuthatch retried against the same endpoint; about 19,000 credits. 3.02 M blocks over 9,261 calls is about 330 blocks a call, not the 1,500 of `--window`: that is a ceiling, and nuthatch narrows the range it asks for to stay under the 20,000-log answer (`fetch_window_blocks` read 332 at the check) |
| Disk | 1.8 GB for the nest directory |
| Memory | 436 MB resident when following; the cgroup's 2 GB limit counts 1.3 GB of page cache, which the kernel gives back |
| Following the tip | `lag_blocks` 0 at a 6 s poll. A poll is one tip call, one `eth_getLogs` and one `eth_getBlockByNumber` per block since the last poll (about 37, sent as batches). Re-read on 2026-11-19 over 63 s from `/metrics`: 9 polls, 9 `eth_getLogs`, 9 tip calls and 335 headers, so 362 credits, **about 5.7 credits a second** (0.5 M a day). The earlier "36 of each in 4 minutes" counted polls, not headers |
| Through the proxy | `check-proxy.sh http://127.0.0.1:8481 --burst` passed, including the flood (7 served, 73 limited); `/sql`, `/mcp` `tools/list` answer, `/_admin` and a wrong `Host` answer `403` |

The two reads that are not measured: the credit totals are the call counts times GraphOps' published prices,
not a figure from platform.graphops.xyz (the CLI's usage report is per organisation, and other nests run on it),
and the public name was not tried because its DNS record did not exist yet. It was served under the path URL instead, below.

## What this does not prove

- The units were checked with `systemd-analyze verify` and their sandbox directives have not been run on a
  real system manager; the first `systemctl start` is their first test. If `nuthatch-public-arc-testnet` fails
  to start under `ProtectSystem=strict`, `journalctl -u` names the path it could not write.
- The 30 to 40 minute backfill is extrapolated, as above.
- Rate limits are per address. A caller with many addresses gets the all-callers cap (5 a second) and
  nothing more; this is a demo's defence, not a service's. Authentication, keys, quotas and billing are the
  gateway's job, as `operators.md` says.
- `/mcp` tool calls (`diff`, `entity`, `balance`, ...) run in the nest, past the proxy's route list. `diff`
  has its own 30 s budget and takes one of the two query permits while it runs; the proxy cuts the
  client off at 15 s, the nest finishes or times out by itself.

## Handing the deploy to a sudo user (what an unprivileged session cannot do)

An agent session without root (the 2026-11-19 attempt at #3031 had no credentials for helsinki-nests at
all) can do nothing on the host. For a sudo user, steps 0 to 9 above are the whole list. Checked against
the steps as written, the ones that need root, and have no user-level equivalent that keeps the limits, are:

- step 1 (`useradd`), step 2 (`/usr/local/bin`), step 3 (`sudo -u nests-public`, the state directory is
  `0750` and owned by that user), step 4 (`/etc/nuthatch-public`), step 5 (`apt-get install nginx`, the
  system units: `MemoryMax`, `ProtectSystem` and the sandbox are the read-only limits), step 7
  (`systemctl enable --now`, `journalctl -u`) and step 8 (DNS, and editing the Caddyfile).
- Steps 0, 6, 9 (and the `curl` lines in step 7) need no root, though step 6 needs the nginx binary that
  step 5 installs. Step 10 is a comment on the issues.

If the nest is to run as an existing low-privilege account instead of `nests-public`, change `User=`,
`Group=`, `ReadWritePaths=` and `WorkingDirectory=` in both units and the `--dir` path together, and keep the
rest.

**Verify the host key out of band before the first `ssh`; do not accept it blind.** On the host, read
the fingerprint of its own key with `ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub` (or from the
provider's console) and compare it with what `ssh` shows on first connect. A value recorded in this repo
cannot stand in for that check. For the record, grokbot-box saw the ED25519 fingerprint
`SHA256:L1nA/wyDwfKi6vOtgfNsV7xTzA6nMd71aMBA67BAyKM` for 100.82.188.91 on 2026-11-19 from an
unauthenticated first contact over the network, so it is what the network returned, not an authenticated
key. Matching it proves nothing about the host; a mismatch would show the key changed since then.
