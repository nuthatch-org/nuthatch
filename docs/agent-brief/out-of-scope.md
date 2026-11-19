# Out of scope: do not build, do not suggest (full text)

Moved verbatim from `CLAUDE.md` on 2026-11-19 to keep the always-loaded standing brief short. Nothing was reworded or dropped; only relative links were re-pointed (`../../`) because the file moved two directories down. `CLAUDE.md` links here; the non-negotiables and the rules summarised there still bind.

## Out of scope - do not build, do not suggest

- In this tree: hosted service, billing, metering, **hosted-SaaS multi-tenancy** (per-tenant
  authz/quotas, isolation between mutually-untrusting paying customers - that's the
  become-a-data-service-company path, and the gateway's job regardless). Note: **multi-nest
  tenancy in the runtime** (a tenant is an opaque ownership label plus refcounting - no identity,
  no authn, no quotas, no metering) and *distributed **self-hosted** scaled mode* (one
  operator's writer pool + query-FE tier + control-plane, RFC-0022) are both **in scope** - see Architecture. **The line is what
  nuthatch does about tenants, not who they are** (amended 2026-08-04): it sees a string, refcounts
  it, and knows nothing else, so an operator's tenants may well be paying customers and nuthatch
  has no concept of it. Per-tenant billing and authz stay out and are the gateway's job. The
  earlier wording drew the line at "cooperating tenants an operator picked - not paying strangers",
  which asked a question nobody could answer from the code.
  **Amended 2026-09-29: Nuthatch does not run a hosted nest service.** The 2026-09-23 override
  that approved one is withdrawn, and the platform built under it (`nuthatch-org/nuthatch-hosted`)
  is retired. Hosted nests will be offered by GraphOps on its own platform, running the stock
  binary like any other operator, which is why the website's hosted page says "coming soon".
  Nothing in this tree is built for it: a hosted need that would require a change here is a normal
  nuthatch feature on its own merits, or it is not built.
  **Amended 2026-10-06: Chief lifted that rule for the subgraph stopgap.** Nuthatch serves stopgap
  nests for BNB and Polygon subgraphs the network does not serve, on the ThinkPad or new VPSes, until an
  indexer picks each one up ([docs/subgraph-stopgap.md](../../docs/subgraph-stopgap.md)). It is the stock
  binary run as an operator would, not a platform: no billing, no accounts, nothing in this tree built
  for it. The hosted-platform retirement above otherwise stands.
  **Amended 2026-10-09 by Chief: we may offer to host nests for people and teams**, not only stopgap
  subgraphs. The same terms bind: the stock binary run as an operator would, on the ThinkPad or our
  VPSes, no billing, no accounts, nothing in this tree built for it. It is an offer made to a named
  user, not a platform or a public service; a hosted need that requires a change here is a normal
  nuthatch feature on its own merits, or it is not built. That includes a direct offer to a named
  Graph user whose subgraph is hurt by the Studio sunset or a chain's deprecation; the 2026-10-08
  stopgap hold still rules out promotion and any approach through the Foundation.
  **Amended 2026-10-21 by Chief: the hosting offer is public.** "It is a public offer on the site."
  nuthatch-indexer.com/subgraphs says so, and people reach out on the Nuthatch Discord server. The
  other terms of the 2026-10-09 amendment still bind: the stock binary, no billing, no accounts,
  nothing in this tree built for it.
  **Amended 2026-10-27 by Chief: the shared runtime ([RFC-0073](../../docs/rfcs/0073-the-shared-runtime.md)).**
  GraphOps asked for sixteen things to host every customer's nests in one runtime, and Chief said
  "lets do ALL of it". Three of them crossed this list and are now in scope, as operator
  configuration the binary enforces without knowing who anyone is: **per-tenant-label shares and caps
  on query execution** (FR2), **query isolation between mounts hardened for mutually untrusted
  tenants** (FR8), and **a per-request cost report** (FR4). The guarantee FR8 makes holds for the
  shared-runtime configuration RFC-0073 defines, and is not claimed before an independent review has
  tried to break it. Authentication, API keys, accounts, billing and identity stay out and stay the
  gateway's job: nuthatch still sees a string. The 2026-09-29 retirement stands; we do not run the
  service.
- Token, staking, decentralized network features (a possible future Graph Horizon data
  service is explicitly deferred).
- Non-EVM chains before EVM is airtight. **Amended 2026-10-21 by Chief:** "Non evm chains could be
  supported upon user request." A request is a named user asking for a named chain; the site says
  so. None is built today, and building one is new ingestion and decode, so each request is a
  decision for Chief, not a standing authorisation.
- TEE attestation, zk proofs (verifiability = deterministic re-execution of the core
  + content-addressed segments; nothing heavier).
- Kubernetes manifests, Helm charts, or any deployment story beyond binary + compose.
