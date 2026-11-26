# nuthatch-gateway: a reference read gateway (#3094)

nuthatch has no authentication: who may read what is the gateway's job (CLAUDE.md, out-of-scope.md).
This is a tested gateway for that job, so an operator sharing one runtime between teams or partners
does not have to write one from scratch. It is a separate binary. It is not part of nuthatch, and a
default nuthatch build is exactly what it would be had this never been written.

It does three things:

1. **Verifies a bearer token** on every request: an OIDC/JWT access token signed RS256, ES256, EdDSA
   (Ed25519) or HS256, with `exp` required, `nbf` honoured, and `iss` and `aud` checked against the
   config.
2. **Checks the token's read scopes** against the nest and table the request reaches, over the HTTP
   routes, `/sql`, `/explain`, `/q/*` and every MCP message.
3. **Forwards what passes** to nuthatch, without the token: nuthatch sees the request it would have
   seen without the gateway.

## Run it

```sh
cargo build --release --locked --manifest-path tools/gateway/Cargo.toml
tools/gateway/target/release/nuthatch-gateway gateway.toml
```

```toml
listen   = "0.0.0.0:8443"            # put TLS in front, or terminate it here with your proxy of choice
upstream = "http://127.0.0.1:8080"   # nuthatch, bound where only the gateway can reach it
issuer   = "https://idp.example.org/realms/data"
audience = "nuthatch"
leeway_secs = 60                     # clock skew allowed on exp/nbf (default 60)
identity_header = "x-forwarded-user" # optional: set to the token's `sub`; a client's own copy is dropped.
                                     # Give nuthatch the same name as --audit-log-identity-header

# Exactly one key source:
jwks_url = "https://idp.example.org/realms/data/protocol/openid-connect/certs"
# jwks_file = "/etc/nuthatch-gateway/jwks.json"
# hs256_secret_env = "NUTHATCH_GATEWAY_HS256_SECRET"   # the variable's name, never the secret

# One entry per nest the gateway exposes. `prefix` is where nuthatch serves it: `/<alias>` or
# `/<tenant>/<alias>` on a runtime, "" for a solo `nuthatch dev`.
[[nest]]
name   = "usdc"
prefix = "/usdc"

[[nest]]
name   = "acme-dai"
prefix = "/acme/dai"
```

**Bind nuthatch where only the gateway can reach it** (loopback, a private interface or a firewall
rule). A gateway that can be walked around guards nothing.

## Scopes

Scopes are read from the token's `scope` claim (space-separated) or `scp` (string or array). Nests are
named by `[[nest]] name`.

| Scope | Grants |
|---|---|
| `read:<nest>` | everything the nest serves: every typed route, `/sql`, `/explain`, `/q/*`, `/derived/*`, and every MCP method and tool |
| `read:<nest>/table/<table>` | `GET /table/<table>`, and the MCP `table` tool for that table |
| `read:<nest>/q/<query>` | `/q/<query>` (GET or POST), and the MCP `query` tool for that query |

Any scope on a nest also admits MCP's discovery messages there (`initialize`, `ping`, `tools/list`,
`prompts/list`, `resources/list`, notifications), so a client scoped to one table can still start.

**Free-form SQL needs the nest scope.** A statement can reach any relation through a CTE, a join, a
view or a table function, and only the engine that binds it knows which. A gateway that guessed from
the SQL text would be one quoting trick away from a bypass, so a token scoped to tables is refused
`/sql`, `/explain` and the MCP `sql` and `explain` tools by name. Give a team a named query
(`views/*.sql` and `read:<nest>/q/<name>`) when it needs SQL over some tables and not others.

## What it refuses, and how

Every refusal has a stable name in a JSON body, `{"error": "<name>"}`, and the same name in the log.

| Status | `error` | When |
|---|---|---|
| 401 | `token_missing` | no `Authorization` header |
| 401 | `token_malformed` | not `Bearer <JWS compact>`, or a part that does not decode |
| 401 | `token_alg_unsupported` | `alg` is not RS256/ES256/EdDSA/HS256 (`none` included), or a `crit` header |
| 401 | `token_key_unknown` | no key of the token's algorithm with its `kid` |
| 401 | `token_signature_invalid` | the signature does not verify |
| 401 | `token_without_expiry` | no `exp` |
| 401 | `token_expired` | `exp` passed (beyond the leeway) |
| 401 | `token_not_yet_valid` | `nbf` in the future (beyond the leeway) |
| 401 | `token_wrong_issuer` | `iss` is not `issuer` |
| 401 | `token_wrong_audience` | `aud` does not contain `audience` |
| 403 | `scope_missing` | the token holds none of the scopes the request needs (`needs` lists them) |
| 403 | `admin_not_proxied` | `/_admin*`: the admin UI is the operator's, behind nuthatch's own token |
| 404 | `not_routed` | a path under no `[[nest]]` prefix, or no read route of the nest |
| 400 | `path_not_canonical` | percent-encoding, a dot or empty segment, or a backslash in the path |
| 413 | `body_too_large` | a request body over 4 MiB |
| 502 | `upstream_unreachable` | nuthatch did not answer |

401s carry `WWW-Authenticate: Bearer` (with `error="invalid_token"` and the name as
`error_description`), 403s `error="insufficient_scope"` (RFC 6750 §3). A refused MCP call is a 403
whose body is a JSON-RPC error with the call's `id` and a message starting `scope_missing`.

The algorithm a token may use is fixed by the key that verifies it, so an RSA public key can never be
replayed as an HMAC secret. Signatures are checked before any claim is read.

## Network calls

Two, both named in the config: `upstream`, and `jwks_url` when it is set. The JWKS is fetched at
start (the gateway refuses to start without keys), again when a token names an unknown `kid` (at most
every 30 s, for key rotation), and when the held keys are an hour old; a failed refresh keeps the
keys held. Each JWKS fetch is cut off after `jwks_timeout_secs` (default 10) and runs outside the
key lock, so a stalled IdP never delays a token that verifies against the keys held;
`jwks_min_refetch_secs` (default 30) sets the rotation interval. Neither client follows redirects or reads `HTTP(S)_PROXY` from the environment. No
telemetry, nothing else.

## Logs

One line per refusal: `refused <name> <METHOD> <path>`. Never the query string (it can carry SQL),
never a header, never a token or key. A test runs the binary and checks its output for every token
and the secret it used.

## MCP clients

`nuthatch mcp --url https://gateway.example.org/usdc --token-env MY_TOKEN` sends `MY_TOKEN` as a bearer
token on every request (RFC-0073 FR13), so the stdio bridge works through the gateway unchanged; its
tool calls arrive as the HTTP routes they become and are judged as those. Streamable-HTTP clients
post to `/<prefix>/mcp` with the same header. The gateway judges the message it parsed and forwards
that message, re-serialised, so no difference between two JSON parsers can split what was checked
from what runs.

## Keeping it in step with nuthatch

`policy::tests::routes_are_classified` reads `src/serve.rs` and fails when nuthatch's nest router
registers a route this gateway has not decided. A new route is refused (`not_routed`) until it is
added to `policy::need`.

## Tests

```sh
yatr run --agent crate-gateway
```

Unit tests cover the RFC 7515 RS256 example, algorithm confusion, `alg: none`, path canonicalisation
and every scope rule. `tests/gateway.rs` runs the gateway between a stub nuthatch and a JWKS server,
with ES256 and EdDSA tokens made in the test: the allow path, each refusal above, MCP calls allowed and
refused, and the log check against the real binary with an HS256 secret.
