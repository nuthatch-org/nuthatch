# Security policy

nuthatch is a single-binary indexer that people run on their own machines and, increasingly,
as a hosted service in front of untrusted callers. The attack surface that matters most is the
`/sql` and MCP surfaces: a caller can run arbitrary read-only SQL. The core ships resource guards
(timeout, row cap, concurrency semaphore) and binds to `127.0.0.1` by default with a loud warning
off-localhost. Authentication is deliberately *not* in the core (see [GOVERNANCE.md](GOVERNANCE.md) -
that is the operator layer). A report that defeats the guards, reads outside the attached read-only
segments, or escalates a read into a write is in scope.

Also in scope: reorg / finality handling that could corrupt sealed segments, decode paths that
panic or mis-attribute on adversarial ABIs/logs, and any path that breaks the ≤2 GB footprint
budget into an OOM DoS.

Out of scope: third-party RPC endpoints you point nuthatch at (trust them or don't), and
anything requiring a malicious operator who already controls the host.

## Reporting

**Please do not open a public issue for a vulnerability.** Use GitHub's private advisory flow:
**Security → Report a vulnerability** on the repo, or email the maintainer at the address on
the [GitHub profile](https://github.com/cargopete). Include a description, affected version
(`nuthatch --version`), and a reproduction if you have one.

Expect an acknowledgement within a few days - this is a solo-maintained public good, so please
be patient with the "few," not the "days." We will agree a disclosure timeline with you; the
default is coordinated disclosure once a fix ships, with credit unless you'd rather stay
anonymous.

## Supported versions

The latest minor release receives security fixes, and so does the **previous minor for 30 days**
after the next one ships. When 5.13.0 is released, 5.12 keeps receiving security fixes until 30 days
after that date, then only 5.13 does. A backport is a patch release cut from `release/<major>.<minor>`
by the process in [docs/releases/README.md](docs/releases/README.md#patch-releases-on-4x); it
carries the security fix and nothing else, so an operator pinned to that minor can take it without
taking the next minor's features. Older lines are not patched: upgrading is the remedy.

**Advance notice for hosting operators.** An operator who runs nuthatch for other people may ask
for private notice of a security release before it is public. Write to the address under
Reporting, naming the deployment and a security contact. Notice is given under a disclosure
agreement, normally a few days before the release, with the affected versions and the fix; it
carries no exploit detail beyond what is needed to judge urgency. It is offered to any such
operator on the same terms, and it is not a support contract.

Releases are cut from tagged commits on `main` or a `release/<major>.<minor>` branch,
published to GitHub Releases with per-artifact SHA-256, and reproducible from the pinned toolchain (see
[GOVERNANCE.md § Release integrity](GOVERNANCE.md#release-integrity--key-custody)).
