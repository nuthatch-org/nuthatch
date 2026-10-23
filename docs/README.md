# Documentation guide

The documentation is split between the product's current contracts and dated records of decisions.
Start with the current material below; use historical records to understand why a decision was made,
not to infer work that remains.

## Current work

- [Backlog guide](backlog.md) - GitHub is the live queue; this explains its labels and settled
  decisions.
- [Frozen for 2027](frozen-for-2027.md) - capability work deliberately deferred during the 2026
  feature freeze. These issues are closed, with an explicit reopening rule.
- [Parked issues closed, 2026-09-28](parked-closed.md) - the open `parked` queue, closed with the
  decision kept. Reopening one is a new decision.
- [RFC index](rfcs/README.md) - design and implementation status for each RFC.

## Porting a subgraph

- [The Graph compatibility surface: what it is, and what it is not](graph-compatibility-what-it-is.md) -
  **read this first.** Whether a nest can serve your client, and why a coverage percentage overstates it.
- [The accepted query dialect](graph-query-dialect-accepted.md) - every shape that compiles, every
  refusal and its reason.
- [Schema generation rules](graph-schema-generation-rules.md) - how the Graph-shaped schema is derived.

## Operating and verifying nuthatch

- [Production readiness](prod-readiness.md)
- [Verification](verification.md)
- [Production guide](production.md)
- [Operator reference](operators.md)
- [Runtime admin API](admin-api.md): mount by NID, jobs, suspend, move, reclaim, dry run - for a platform driving a runtime
- [Reading sealed segments without nuthatch](reading-segments.md)
- [Reading a published nest](reading-published-nest.md)
- [Offchain data: file drops and scheduled price pulls](offchain.md)
- [Address history](../skills/nuthatch-builder/config-reference.md#address_history---an-accounts-history-for-rotki-rfc-0063):
  a nest that watches accounts and serves them Etherscan-shaped at `/api` (RFC-0063). Its rows live in
  the nest's store, not in SQL tables or sealed segments.
- [`nuthatch diff`](../skills/nuthatch-builder/cli-reference.md#nuthatch-diff): what happened between
  two blocks, as sentences (RFC-0071).
- [Release notes](releases/README.md)

## In a browser

- [nuthatch-indexer.com/play](https://nuthatch-indexer.com/play): query a published nest, or build and
  index a new one from a contract address, in a tab (RFC-0065, RFC-0066). No install.

## Dated records

- [2027 direction](roadmap-2027.md)
- The progress log, the sprint scopes and the July-August 2026 roadmap left the tree on 2026-10-08.
  They are kept at [`ede03ca7`](https://github.com/nuthatch-org/nuthatch/tree/ede03ca7/docs); sprint
  labels remain on GitHub issues as provenance.

When a document and a live issue disagree about outstanding work, the issue wins. If the disagreement
is substantive, correct the document as well. Otherwise the attic fills up, and before long it starts
answering questions nobody asked.
