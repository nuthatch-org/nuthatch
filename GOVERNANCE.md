# Governance & scope

nuthatch is a free, `MIT OR Apache-2.0` public good with a single maintainer and no direct monetization. This
document states how it is sustained, what stays out of scope regardless of who's paying, and the
neutrality guarantees that make it safe to depend on.

## Sustainability

nuthatch is a self-funded public good, maintained by one person; everything is open source. It takes
no grants and has never had one.

## Neutrality (the guarantee you can depend on)

**No operator has exclusivity, a private fork, partner-only features in the core, or roadmap veto.**
Any operator may host nuthatch; a partner's edge is partnership, being first, and priority support of
*their own* integration - never a gate on anyone else. The permissive licence makes capture of *this*
project impossible: anyone can run, fork, host or embed the exact same software, and the maintainer
holds no rights over it that anyone else lacks.

Stated plainly, because the relicence changed this and pretending otherwise would be dishonest: a
permissive licence also allows a **closed** derivative. Someone may take nuthatch, extend it privately
and sell it without publishing anything. That is a deliberate trade - maximal adoption and zero
friction for embedders, in exchange for giving up the copyleft protection that would have forbidden it.
What remains guaranteed is that the upstream project stays open and stays free; what is not guaranteed
is that every derivative does.

### Operator-partnership disclosure

> GraphOps, an indexer and core developer on The Graph, is a design partner. The relationship is
> partnership, not ownership: no exclusivity, no relicensing, no private features, no roadmap veto.

## The dividing line: core vs operator layer

nuthatch ships the **guards and signals** that make it safe to run as a service; it does **not** grow
the operator's product into the binary. Concretely (RFC-0005 §6):

| In the core (nuthatch) | The operator's layer (a gateway in front) |
|---|---|
| `/sql` resource guards (timeout, row cap, concurrency) - bound *how much* | Authentication - decides *who* may query |
| `/metrics`, `/health`, structured logs | Metering, billing, quotas |
| Bind posture + a loud warning off-localhost | Multi-tenancy, per-tenant isolation |
| Config/data stability contract | The hosted product itself |

## What we will not do - for funding or partnership

Non-negotiable regardless of who asks or pays:

- **No token**, no decentralised-network features, no staking.
- **No telemetry by default, no phone-home**: the one exception is the opt-in head count of
  RFC-0054, off until a person answers yes and carrying no identifier. No mandatory API tokens or gated data services in the data path.
- **The core stays permissively licensed** (`MIT OR Apache-2.0`) and is never relicensed to anything
  more restrictive or proprietary; **no private forks**; **no partner-only features** in core.
- **No roadmap veto** for any funder or partner (input is welcome; veto is not).
- **No auth / metering / multi-tenancy in core** - that is the operator layer, and it is contractual.

If a funder or partner requires any item on this list, we decline that term rather than the principle.

## Release integrity & key custody

Releases are the supply chain an operator depends on. Signing/release-key custody, and a named
successor/escrow arrangement for those keys, are tracked as an open governance item (RFC-0006 Q3) to
be settled while the project is small. Until then: releases are cut from tagged commits on `main`,
published to GitHub Releases (with per-artifact SHA-256), and reproducible from the
pinned toolchain.
