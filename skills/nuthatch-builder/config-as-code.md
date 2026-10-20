# Config as code: `nest.star` (Starlark front-end) - REMOVED

> **Retired 2026-07-21, removed from the binary in 2.0. Do not author nests in Starlark - use
> `nuthatch.toml`.** Every first-party nest is a plain `nuthatch.toml` (that's what `init` writes).
> A directory that still holds a `nest.star` is **refused at load**, not silently ignored, because the
> file used to take precedence over `nuthatch.toml`:
>
> `<dir> has a nest.star, and the Starlark front-end was removed in 2.0 (RFC-0018 §2, retired
> 2026-07-21). Port it to nuthatch.toml - nuthatch init emits that shape, and it is what every other
> nest already uses.`
>
> This page is kept only so a legacy `.star` file can be read and ported. Everything below describes
> how the front-end behaved while it existed; none of it runs today.

Historically, RFC-0018 §2 added an **optional** second front-end: a `nest.star` file that *computes*
the config in [Starlark](https://github.com/facebook/starlark-rust) (a small, hermetic configuration language)
and evaluates to the **exact same `Config`** the TOML would - sugar, never a new capability. A `.star`
and its equivalent `.toml` produce a byte-identical config.

## When it took precedence

Before 2.0, if a nest dir contained **`nest.star`, it was used and `nuthatch.toml` was ignored.** That
is why the file is refused now rather than skipped: skipping it would change what a nest indexes
without saying so. Delete the `.star` once its TOML equivalent is in place.

## The four verbs (the whole surface)

The evaluation environment is *closed*: standard Starlark (lists, dicts, comprehensions, `%`
formatting, `enumerate`, `range`, defs) plus exactly four host builtins. There is no file, clock,
network, or randomness access - a `nest.star` is a description, not a program.

- **`nest(...)`** - call **exactly once** per file. It defines the nest and its arguments map 1:1 to
  the TOML sections:
  - `name` (str), `chain` (str), `rpc_urls` (list[str]) - required.
  - `chain_id` (int) - **optional**; for a known chain it is derived from `chain` exactly as `init`
    does. Pass it explicitly only for a custom chain nuthatch doesn't know.
  - `contracts`, `templates`, `factories`, `alerts`, `webhooks` - lists of the builtins below.
  - `flags` - a dict matching the `[flags]` TOML table.
- **`contract(alias, address, abi, start_block=None, events=[])`** - one contract to index. `events`
  is the optional per-contract event allowlist (same as `[[contracts]].events`).
- **`template(name, abi, filter=None)`** - a child-contract template (factory pattern, RFC-0009).
- **`factory(watch, event, child_param, template, start=None)`** - a factory discovery rule.

All arguments are **keyword-only** - `contract(alias="usdc", ...)`, never `contract("usdc", ...)`.

## The canonical win: a loop instead of copy-paste

```starlark
# nest.star - index a basket of ERC-20s that all share one ABI
STABLES = {
    "usdc": "0xA0b86991c6218b36c1D19D4a2e9Eb0cE3606eB48",
    "usdt": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
    "dai":  "0x6B175474E89094C44Da98b954EedeAC495271d0F",
}

nest(
    name = "stables",
    chain = "mainnet",
    rpc_urls = ["https://eth.example/rpc"],
    contracts = [
        contract(alias = name, address = addr, abi = "abis/erc20.json")
        for name, addr in STABLES.items()
    ],
)
```

That is the whole file. Adding a fourth stablecoin is one line, and every contract is guaranteed to
share the same ABI and settings - the drift a hand-written three-contract TOML invites is gone.

The leading `alias`, `address`, `start_block` of `contract(...)` may be **positional**, so a small
`def` wrapper reads cleanly: `def erc20(alias, addr, sb): return contract(alias, addr, sb,
abi="abis/erc20.json")`. `abi` and `events` are always keyword.

## Composition: one logic, many chains (`load()`)

Where `load()` earns its keep is **the same nest instantiated across several chains** - one shared
definition, one thin instance per chain, differing only by chain, address, and RPC. (Two *identical*
nests on the *same* chain aren't a composition case - that's one nest wearing two names. Don't fork or
`load()`; just keep the one.)

The contract is **library-defines, entry-instantiates**:

- A **reusable** nest is a `.star` that *defines a factory function* and **never calls `nest()`
  itself**. It is a library, not an entry.
- An **entry** `nest.star` `load()`s that function and calls it once, with its chain's parameters.

```starlark
# uniswap-v3/lib.star - the reusable unit. Defines; never self-instantiates.
def uniswap_v3(name, chain, factory, rpc):
    return nest(
        name = name,
        chain = chain,
        rpc_urls = [rpc],
        contracts = [contract("factory", factory, abi = "abis/uniswap_v3_factory.json")],
        # …templates + factory rules for the pools, shared by every chain…
    )
```

```starlark
# uniswap-v3-arbitrum/nest.star - one instance; its siblings differ only in the three arguments.
load("//uniswap-v3:lib.star", "uniswap_v3")
uniswap_v3(
    name = "uniswap-v3-arbitrum",
    chain = "arbitrum-one",
    factory = "0x1F98431c8aD98523631AE4a59f267346ea31F984",
    rpc = "https://arb.example/rpc",
)
```

Fix a bug in `uniswap-v3/lib.star` once and mainnet, Arbitrum, Optimism, and Base all inherit it -
the whole point. Reach for this only when the instances genuinely differ; identical config isn't reuse.

**`load()` paths are confined** (a `.star` can never read an arbitrary file):

- `load("lib.star", "sym")` - relative to *this nest's own directory* (a nest with its own library
  file), confined under the nest dir.
- `load("//pkg:file.star", "sym")` - a **catalogue** load: `pkg/file.star` under the catalogue root,
  which is `$NUTHATCH_CATALOGUE` if set, else the nest directory's parent (so sibling nests in a
  checkout resolve with no configuration). `..` and paths escaping the root are refused.

If you put a top-level `nest()` in a file you then `load()`, you get a clear error - that's the
library/entry split being enforced, not a bug. Keep instantiation in the entry.

## Rules that keep it honest

1. **One `nest()` call in the entry.** Zero or two is an error, caught at load with a clear message.
   A `load()`ed library must *not* call `nest()` - it defines, the entry instantiates.
2. **Only the fields listed above** - an unknown field is a load error, not a silent no-op.
3. **It resolves to a `Config`, then stops.** The interpreter runs once at `nuthatch dev` / mount time
   and is dropped; it never touches the data path, so determinism in the core is untouched.
4. **Everything downstream is identical.** `bundle`, load, semantic layer, views - all operate on the
   resolved config and neither know nor care that it came from Starlark.

If you find yourself wanting logic a `nest.star` can't express, that's the signal you're reaching past
config into derivation territory - that is `views/*.sql` or `entities.toml`, not this one.
