# Can the gates fail? (#913)

Six gates in two days either could not fail or enforced something that had stopped being true, and
every one was found because something *else* broke. This is the pass #913 asked for, and the tooling
so it can be repeated rather than remembered.

`scripts/gate-audit.sh` mutates the artefact each gate guards and asserts the gate goes red.
`--check` verifies every case still has a target without running anything, and
`tests/gate_audit_cases.rs` puts that on every push.

## Result

Eight cases across seven gates. **All eight caught their mutation.**

| gate | mutation | |
| --- | --- | --- |
| `ivm_claims` | CLAUDE.md's `shipped 2026-08-28` | caught |
| `ivm_claims` | CLAUDE.md's `Not incremental` | caught |
| `rfc_index_status` | an RFC's status column in the index | caught |
| `doc_command_check` | a real `nuthatch dev` becomes a fiction | caught |
| `required_checks` | a required-check name | caught |
| `skill_refs` | a flag in an authored skill page | caught |
| `skill_refs` | a flag in the generated CLI reference | caught |
| `tape_clean` | an `"outcome":"ok"` becomes `"err"` in the clean tape | caught |

So the existing gate set, on this evidence, is watching what it claims.

## The three real failures found this sprint were in gates written *this sprint*

Not in the audited set. In the new gates, by the same method, within hours of writing them.

| gate | failure | shape |
| --- | --- | --- |
| `actions_are_pinned` | its parser matched only bare `uses:`, so it saw **15 of 53** lines - it would have passed while checking 28% of the workflows | 1: the scan stopped matching reality |
| `release_provenance` | deleting `id-token: write` left the gate green, because the **comment above it** says `id-token: write` | 3: the mechanism cannot fail |
| `release_provenance` | gutting `gh attestation verify` left the gate green, for the same reason | 3 |

The second and third are the same defect: a gate that documents itself well enough will match its own
prose. `release_yml()` now strips comments before any scan, and asserts stripping removed something so
the filter cannot rot in turn. The first was caught only by a floor assertion (`len() >= 20`) written
specifically to stop a vacuous pass, which is now the pattern in all three new gates.

**A gate is at its most dangerous on the day it is written**, when everyone believes it works and
nobody has watched it fail.

## The method has a failure mode, and the first run hit it

The first audit run reported **three survivors**. All three were wrong:

- `ivm_claims` - I mutated a CLAUDE.md phrase the gate never asserts on.
- `required_contexts_script` - it builds a throwaway repo root with a synthetic file, so it cannot see
  the real one. Correctly green.
- `tape_clean` - I changed a hex digit; it guards exactly one thing, `"outcome":"err"`, and says so.

**Mutation only works if the mutation is of the thing the gate asserts.** Guessing produces confident
false findings, which is worse than no audit at all. Every case in the script now names the assertion
it provokes, with a file and line.

## Why a SKIP fails the run

If an artefact is rewritten and a case's needle stops matching, that case silently stops covering its
gate while everything else still reports success. That is shape 1 reappearing inside the tool built to
detect shape 1. So a skipped case is a failure, and the fix is to repair the case rather than delete
it.

## A case that runs no tests fails

When most test files became modules of `tests/it.rs`, every `cargo test --test <name>` stopped naming
a target and the mutating run proved nothing (#2304). Each case now runs `--test <name>` when
`Cargo.toml` declares that target and `--test it <name>::` when it is a module of `tests/it.rs`, and
`--check` fails a case whose target is neither. A filter that matches nothing still exits 0, so the
run counts the tests that ran, and a case where none did is a failure.

## Not covered

The `e2e_*` behaviour tests. They are not doc-or-config gates and cargo-mutants already mutates the
code they exercise; this audit is about gates that read an artefact and assert something about it.
