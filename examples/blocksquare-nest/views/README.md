# views/ - this nest's authored logic (RFC-0018 §1)

Drop `*.sql` files here, each a `CREATE VIEW …` over your nest's tables (the live tip ∪ sealed
history, one surface - recomputed per query unless the view is named in `maintained.toml`).
Query a view by name with `nuthatch sql` or the MCP `sql` tool. Files load in sorted filename
order (`10-…`, `20-…`), so a later view can build on an earlier one. Describe what a view
*means* in `semantic.toml` under `[view.<name>]` so an agent sees it. A broken/drifted view
fails `nuthatch check` loudly.

A view listed in `maintained.toml` (`[[view]]` with `name = "<view>"`) is answered from a
stored copy of its own evaluation, used only while every input it reads is unchanged, so the
answer is the same either way.

`entities/` plus `entities.toml` is separate: use it only for a keyed relation in the bounded
incremental SQL subset, maintained at indexing time with explicit memory, reorg and restart
semantics. `nuthatch check` refuses unsupported entity SQL rather than silently turning an
ordinary view into stored state.
