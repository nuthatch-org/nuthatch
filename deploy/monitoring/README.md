# Production monitoring

Prometheus, Alertmanager, a blackbox prober and Grafana, run on the ThinkPad with Docker Compose.
They scrape the Helsinki nests and the ThinkPad's QoS nest over the tailnet and page Discord.
This directory is the whole configuration; nothing about it should live only on a box.

| Piece | Listens on | What |
|---|---|---|
| Prometheus | `127.0.0.1:9490` | scrapes every nest's `/metrics` every 15 s, evaluates `rules/` |
| Alertmanager | `100.83.44.63:9493` (tailnet only) | routes to Discord; Helsinki reads its `Watchdog` |
| blackbox | `127.0.0.1:9115` | probes each nest's `/ready` (`probe_success`) |
| Grafana | `100.83.44.63:3300` (tailnet only) | the "Nuthatch nests" dashboard |

All four use host networking, because the targets are tailnet addresses the host routes and a
Docker bridge may not.

## The nests

`targets/nests.yml`, one line per nest. Prometheus re-reads it on change, so adding a nest is one
line and no restart. `box: helsinki` targets are scraped through the Caddy site below,
`box: thinkpad` targets directly. `role: archive` marks a `serve`-only frozen nest
(`graph-staking-legacy-readonly`), which has no cursor and is left out of the ingest and readiness
rules on purpose.

## Install on the ThinkPad

```sh
cd ~/nuthatch && git pull && cd deploy/monitoring
mkdir -p secrets && cp secrets.example/* secrets/
$EDITOR secrets/discord_webhook_url secrets/grafana.env
sudo chmod 600 secrets/* && sudo chown 65534:65534 secrets/discord_webhook_url   # Alertmanager runs as nobody
# grafana.env stays yours: compose reads env_file as the user running it, before any container starts
docker compose up -d
curl -s 127.0.0.1:9490/api/v1/targets | jq -r '.data.activeTargets[] | "\(.health) \(.scrapeUrl)"'
```

`secrets/` is ignored by git. Grafana binds the tailnet address, so it starts only once Tailscale is
up; `restart: unless-stopped` retries until it is.

**The dead-man signal.** `Watchdog` always fires, and Prometheus re-sends it to Alertmanager every
evaluation; Alertmanager drops it a few minutes after the last one. `helsinki/deadman.sh`, run from
Helsinki's cron every minute, asks Alertmanager over the tailnet whether `Watchdog` is still active
and pages Discord once when it is not and once when it returns. That covers the ThinkPad going down,
Prometheus stopping or wedging its rule evaluation, and Alertmanager dying, and nothing leaves the
tailnet. On Helsinki:

```sh
install -m 755 deploy/monitoring/helsinki/deadman.sh /usr/local/bin/nuthatch-deadman
install -m 600 /dev/stdin /etc/nuthatch/deadman_discord_webhook_url <<<"https://discord.com/api/webhooks/..."
echo '* * * * * root /usr/local/bin/nuthatch-deadman' > /etc/cron.d/nuthatch-deadman
```

## On Helsinki: /metrics on the tailnet only

Every nest unit listens on `127.0.0.1`, and the public Caddy vhost proxies to it. **Recommended:
a Caddy site bound to the tailnet address**, `helsinki/Caddyfile.metrics`. It maps
`/<port>/metrics` and `/<port>/ready` to `127.0.0.1:<port>` and answers 404 to everything else,
so `/sql` and the admin routes are not reachable through it. Nothing about the units changes.

```sh
cp /etc/caddy/Caddyfile /etc/caddy/Caddyfile.bak-metrics
cat Caddyfile.metrics >> /etc/caddy/Caddyfile
caddy validate --config /etc/caddy/Caddyfile && systemctl reload caddy
# from the ThinkPad
curl -s http://100.82.188.91:9180/8107/metrics | head -3
# from anywhere off the tailnet: must fail to connect
curl -m5 http://89.167.109.4:9180/8107/metrics
```

The alternative, `--listen 100.82.188.91:<port>` on each unit, is worse on three counts: it puts the
whole API (`/sql` included) on the tailnet, every public Caddy route has to follow the address,
and a unit then cannot start before `tailscaled` has brought the address up.

The public vhost forwards any path, so `/alloc/metrics` is reachable from the internet behind basic
auth. To keep metrics off the public interface, add this to the top of the nest vhost's block and
reload:

```caddy
@metrics path /metrics /*/metrics
respond @metrics 404
```

## The rules

`rules/nuthatch.yml`. The conditions in `docs/operators.md` "What to alert on", then:

| Alert | Fires when |
|---|---|
| `SqlOutOfMemory` | any `/sql` query fails out of memory (`reason="out_of_memory"`), within a minute |
| `SqlRefusalRate` | over 10% of `/sql` refused by the node (busy, too_large, timeout, out_of_memory) for 5 minutes |
| `NestNotReady` | `/ready` not 200 for 10 minutes |
| `TargetDown` | a scrape target has not answered for 2 minutes |
| `Watchdog` | always; read by `helsinki/deadman.sh` |

Four things differ from the page they come from, deliberately:

- **A single-nest `dev`, every unit today, exports no `nuthatch_nest_health`,
  `nuthatch_cursor_live` or `nuthatch_nest_quarantine_total`.** Those are a multi-nest runtime's
  series. `NestQuarantined` and `QuarantineFlapping` therefore cannot fire here until a runtime is
  scraped, and `CursorDead` also reads `up`, because for a single nest the process is the cursor.
- **`IngestStalled` is 1800 s, not 300 s.** These units poll every 15 minutes (`zz-poll-15m.conf`), so
  the age of the last poll reaches 900 s on every healthy cycle; 1800 s is two missed polls.
- **`MemoryNearBudget` is 75% of 2 GiB**, the per-cursor budget, whatever `NUTHATCH_MAX_RSS` a unit
  was given.
- **`SealedHistoryDisagrees` and `AuditNotRunning` need a unit started with `--audit-rpc`** (#1786),
  the only kind that exports the audit series. No unit is today, so neither can fire yet.
  `AuditNotRunning` looks back six hours, which assumes the default 24 samples a day.

`reason="out_of_memory"` exists from the first release carrying it (nuthatch #1714). Until a unit
runs one, an out-of-memory failure is counted as `invalid` and `SqlOutOfMemory` cannot fire for
that unit.

## Validate

```sh
promtool check config prometheus.yml   # paths are the container's; mount as docker-compose.yml does
promtool check rules rules/nuthatch.yml
promtool test rules tests/nuthatch_test.yml
amtool check-config alertmanager/alertmanager.yml
```

## Proving it

Issue #1714 closes on a recorded page: stop one Helsinki unit for 40 minutes. `TargetDown` and
`CursorDead` fire after 2 minutes and `IngestStalled` within 35; starting the unit resolves them.
Paste the Discord messages onto the issue.
