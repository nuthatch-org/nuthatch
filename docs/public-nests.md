# Public read-only demo nests

How to put a nest on a public URL that anyone can query, without an account and without being able to
change anything. First user: the Arc testnet USDC nest
([#3031](https://github.com/nuthatch-org/nuthatch/issues/3031), under
[#3028](https://github.com/nuthatch-org/nuthatch/issues/3028)). The kit is in
[`deploy/public-nests/`](../deploy/public-nests/).

This is the stock binary run as an operator would run it. Nothing in nuthatch is built for it: no
accounts, no billing, no hosted-service code (see the out-of-scope list in `CLAUDE.md`). The credentials,
the host and the DNS name stay the operator's.

```
internet -> Caddy :443            TLS, the public name           (already on the host)
         -> nginx 127.0.0.1:8480  route allowlist, per-IP limits  deploy/public-nests/nginx.conf
         -> nuthatch 127.0.0.1:8120  --no-admin, read routes only
```

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
- **`/mcp` offers only query tools.** `tools/list` returns `status`, `schema`, `tables`, `table`, `sql`,
  `at`, `explain`, `entity`, `diff`, `balance` and `top_balances`, all read-only. The authoring tools
  (`init_nest`, `start_indexing`, ...) stay on stdio. A request carrying a non-loopback `Origin` is refused,
  so a web page cannot use it; `curl` and MCP clients send none.
- **A loopback bind checks `Host`** (#3002), so the unit names the public name with `--public-host`.
- **`/metrics` is a read route** and shows operator-side counters. It is not public here: the proxy refuses it.

The proxy blocks the same things anyway. `nuthatch dev` on a non-loopback bind, a later flag change or a
future route would otherwise decide what the internet sees; the allowlist in `nginx.conf` makes that
decision in one place. The tool calls `/mcp` makes (`entity`, `diff`, ...) run in process and are not
affected by it, so the nest's own guards are what bound them.

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
| Routes served | `/`, `/health`, `/ready`, `/nest`, `/shape`, `/schema`, `/tables`, `/table/*`, `/sql`, `/explain`, `/mcp`; anything else `404` | `nginx.conf` |
| `/_admin*`, `/metrics` | `403` | `nginx.conf` (the nest has no `/_admin` either) |
| Methods | `/sql`, `/explain`: GET, POST. `/mcp`: POST. The rest: GET. Others `403` | `nginx.conf` |
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
- **Monitoring.** To scrape it, add `{targets: ["100.82.188.91:8120"], labels: {unit: nuthatch-public-arc-testnet, box: helsinki, role: cursor}}`
  to `deploy/monitoring/targets/nests.yml` once it is live; the tailnet-only Caddy site already maps
  `/8120/metrics`. It is not added in this change, because it would page `TargetDown` until the unit exists.
- **Raising a limit** is one line in `nginx.conf` or the env file, then `systemctl reload
  nuthatch-public-proxy` or `restart nuthatch-public-arc-testnet`. Change the table above with it.

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
