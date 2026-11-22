---
name: nuthatch-subgraph-port
description: Port a Graph Protocol subgraph to a nuthatch nest. Use when the user has a subgraph (schema.graphql, mappings, subgraph.yaml, or a deployment CID) and wants a nest, a port report, or to know which entity fields will not match. Classify every field and emit the report BEFORE scaffolding. Then overlay [[calls]], Exact views, checks and a README onto `nuthatch init --from-subgraph`. Do not run AssemblyScript. Do not invent contract reads the mapping did not make.
---

# Porting a subgraph

The first artefact is the **port report**. Write it before anything is scaffolded. A team that already
runs a subgraph is deciding whether to keep it; an honest list of fields that will not match is the
deliverable. Then, and only then, emit onto the importer's nest.

This sits on top of `nuthatch init --from-subgraph <CID>` (RFC-0038). That importer covers the
**inputs** (manifest, ABIs, start blocks, inferred factories). This skill covers the **outputs**:
every entity field in `schema.graphql`, classified against the mappings, then `[[calls]]`, views,
checks and a README.

**Do not run AssemblyScript.** Read the mappings. `nuthatch port-report --dir <subgraph>` classifies;
`nuthatch port-emit --dir <subgraph> --out <nest>` overlays the nest. Treat the classes as the
starting point and investigate any field that disagrees with a hand port.

## The 30-second path

```sh
# 1. Report first. Nothing is written.
nuthatch port-report --dir path/to/subgraph > port-report.md

# 2. A human accepts the report. Then the importer, then the overlay:
nuthatch init --from-subgraph <CID> --dir nest/
nuthatch port-emit --dir path/to/subgraph --out nest/
```

If a field the report called *exact* later diverges, that is a defect in the report. If it named the
divergence in advance as *fixed point*, *call-derived* or *unreachable*, the port is doing its job.

Every `[[calls]]` stanza must trace to a `.try_*` / `.bind` line in the mapping. Dropping that line
drops the stanza. Do not guess a getter.

## When to read what

- **[classes.md](classes.md)** - the four RFC-0044 §5a classes, how to cite a mapping line, and how
  the classifier decides. Read this before touching a field.
- **[emit.md](emit.md)** - S2: what the overlay writes, and how `[[calls]]` is derived.
- **[traps.md](traps.md)** - the five traps the hand ports paid for. The proxy trap **does not**
  apply to `--from-subgraph`; the others do.
- **[example-uniswap-v3-pricing.md](example-uniswap-v3-pricing.md)** - the worked example. RFC-0038
  §6a on Uniswap V3 `pricing.ts`: `getEthPriceInUSD` is exact, `findEthPerToken` / `derivedETH` is
  fixed point. Horizon staking is the other worked shape (event-native exact, current balances a fold).
- **[fixtures/four-classes/](fixtures/four-classes/)** - the committed fixture whose expected report
  CI diffs. A class change is a red test.
- **[fixtures/one-call/](fixtures/one-call/)** - one `eth_call`. The emit test's signature traces to
  that line; mutating it away must drop the `[[calls]]`.

For driving nuthatch itself (flags, `nuthatch.toml`, factories), use the **nuthatch-builder** skill.
This one translates a subgraph into a report and an overlay. It does not invent flags.

## Golden rules

1. **Report before scaffold.** A nest with no account of what will not match is worse than no nest.
2. **Cite a mapping line for every field.** `src/common/pricing.ts:73`, not "the pricing helper".
3. **A report that cannot say "this field will not reproduce, and here is why" is worse than no report.**
4. **Do not run the mappings.** Do not add a GraphQL compatibility layer. Do not invent `[[calls]]`.
5. **Read `traps.md` before `--from-subgraph`.** The advice on ABIs inverts relative to `init 0xAddr`.
6. **Emit `views/*.sql` now.** `entities.toml` is S5.
