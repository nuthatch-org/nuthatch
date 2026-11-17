# Directory listings

Where agents and their users look for MCP servers, skills and plugins, and what each listing of
nuthatch needs. Each item below needs a maintainer's own login, token or account, so none of it is
automated. Written for #2986 on 2026-11-17; every URL was fetched that day.

## Text to paste

Most forms ask for the same few things. Use these.

- Name: `nuthatch`
- Repository: `https://github.com/nuthatch-org/nuthatch`
- Website: `https://www.nuthatch-indexer.com`
- Licence: `MIT OR Apache-2.0`
- Tagline: Self-hosted EVM indexer: a contract address to a live SQL API in under two minutes.
- Short description: Query an EVM contract's indexed history with SQL, via a local nuthatch nest and
  nuthatch dev.
- Long description:

  > nuthatch indexes a smart contract's events into a local SQL database and serves them over HTTP
  > and MCP. One Rust binary, no Postgres, no Docker, no API key, and no third-party data service.
  > `nuthatch init <address>` resolves the ABI and detects the chain, `nuthatch dev` backfills,
  > follows the tip and serves, and the MCP server gives an agent the schema, SQL and entity lookups
  > against that local index, offline.

- Install and run:

  ```sh
  curl -fsSL https://nuthatch-indexer.com/install.sh | sh
  nuthatch init 0xA0b86991c6218b36c1D19D4a2e9Eb0cE3606eB48 --alias usdc
  nuthatch dev --backfill 300
  ```

- MCP server config (stdio; `nuthatch dev` must be running):

  ```json
  { "mcpServers": { "nuthatch": { "command": "nuthatch", "args": ["mcp", "--url", "http://127.0.0.1:8288"] } } }
  ```

  Once the npm launcher is published, the same with no install step:
  `"command": "npx", "args": ["-y", "nuthatch-indexer", "mcp", "--url", "http://127.0.0.1:8288"]`.

- Tags: `evm`, `ethereum`, `blockchain`, `indexer`, `sql`, `subgraph`, `self-hosted`, `mcp`.

## To do

1. **Official MCP Registry: deprecate the stale entry.** `io.github.nightswatchhq/nuthatch` 4.0.0
   is still `active` beside `io.github.nuthatch-org/nuthatch`, and third-party sites copy its
   `ghcr.io/nightswatchhq/nuthatch:4.0.0` image. Check with
   `curl -s "https://registry.modelcontextprotocol.io/v0.1/servers?search=nuthatch"`.
   The command, run by someone holding publish rights on the `io.github.nightswatchhq` namespace:

   ```sh
   mcp-publisher login github
   mcp-publisher status --status deprecated --all-versions \
     --message "Moved to io.github.nuthatch-org/nuthatch" io.github.nightswatchhq/nuthatch
   ```

   The catch is that `nightswatchhq` no longer exists on GitHub: the organisation was renamed to
   `nuthatch-org`, and the registry grants `io.github.<name>` only to a login that is, or belongs
   to, `<name>`. Recreating an organisation called `nightswatchhq` to get the namespace back would
   end GitHub's redirects from the old URLs, which is a worse trade than the stale entry. So ask the
   maintainers instead: open an issue at
   https://github.com/modelcontextprotocol/registry/issues/new/choose with this text.

   > `io.github.nightswatchhq/nuthatch` (4.0.0) is the same server as
   > `io.github.nuthatch-org/nuthatch`. The GitHub organisation `nightswatchhq` was renamed to
   > `nuthatch-org`, so nobody can authenticate for the old namespace any more. Please mark every
   > version of `io.github.nightswatchhq/nuthatch` deprecated with the message "Moved to
   > io.github.nuthatch-org/nuthatch". I maintain both; github.com/nightswatchhq/nuthatch redirects
   > to github.com/nuthatch-org/nuthatch.

2. **npm publish rights** for the launcher in `npm/` (#2985), once that lands. See `npm/README.md`
   in that change for the first publish and the trusted-publisher setup; after it, a tag publishes
   by itself and the `mcp registry` workflow can publish the entry's npm package.

3. **mcp.so**: https://mcp.so/submit. Sign in, choose "MCP Server", paste the repository URL and the
   name. A search for "nuthatch" there finds nothing of ours.

4. **MCP Market**: https://mcpmarket.com/submit. Paste the repository URL and choose "MCP Server".
   The free queue takes four to six weeks; the paid option is not needed. Submit a second time as
   "Agent Skill" for `skills/evm-indexing` if the form allows both.

5. **cursor.directory**: https://cursor.directory/plugins/new (the address its source,
   github.com/cursor/community-plugins, gives; the site refuses scripted requests with 429, so open
   it in a browser). Sign in with GitHub and paste the repository URL. It reads
   `.cursor-plugin/plugin.json`, `rules/*.mdc`, `mcp.json` and `skills/*/SKILL.md` from the repo root,
   all of which exist.

6. **Cursor Marketplace**: https://cursor.com/marketplace/publish. A publisher application, reviewed
   by hand. Submit the repository URL; the manifest is `.cursor-plugin/plugin.json`. A logo
   (`logo` in that manifest) is the one thing the listing would want that the repo does not have.

7. **Anthropic's plugin directory**: https://claude.ai/directory/manage, from a paid claude.ai account.
   The plugin is the repository root (`.claude-plugin/plugin.json`). The validator asks for a square
   PNG icon, 512 to 2048 px, at `.claude-plugin/icon.png`; the first one submitted becomes the
   listing's icon for good, so add the real one before submitting.

8. **affpapa.org** and any other copier of the 4.0.0 command: ask them to use the install and MCP
   lines above. The page was reported in #2986; a web search on 2026-11-17 did not find its URL, so
   find it from the site.

9. **PulseMCP**: https://www.pulsemcp.com/submit. Submissions are paused and the page refuses
   scripted requests (403). It imports from the official registry when it reopens. Recheck monthly.

## Nothing to do

- **skills.sh** (https://skills.sh): no submission form. It lists a repository once people install
  from it with `npx skills add nuthatch-org/nuthatch`, which finds `evm-indexing`, `nuthatch-builder`
  and `nuthatch-subgraph-port`.
- **Claude Code marketplace**: published by `.claude-plugin/marketplace.json` being on `main`.
- **Glama**: listed (the README badge).
- **Smithery**: self-publishing closed on 2026-10-31.
- **awesome-mcp-servers**: punkpeye/awesome-mcp-servers#19822 is open, waiting on its maintainer.
