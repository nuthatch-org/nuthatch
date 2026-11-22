# Nest showcase page

A static page (HTML, CSS, one JS file, no build, no dependencies, no backend) that shows a public nest to a
prospect: what is indexed, how fresh it is, recent events, two charts, an SQL box, and a "Use it from your
agent" section with the MCP one-liners and five prompts. Everything comes from the nest's own read routes
(`/ready`, `/tables`, `/sql`), so it only ever shows what any visitor could already query.

```
showcase/?nest=arc-usdc        reads /nests/arc-usdc/{ready,tables,sql}
showcase/?nest=/nests/foo      a path on the same origin
```

**It must be served from the same origin as the nest.** The nest sends no CORS headers, on purpose. The
public nests live under `https://nuthatch-indexer.com/nests/<name>`, so the page goes on that site too.

## Per-nest config

`configs/<name>.json` is optional; without it the page infers a title, the events table (the first
`*transfer*` table), the address and amount columns, example queries and prompts from `/tables`. Write one for
a prospect so the title, the decimals and symbol, and the five prompts fit them. Start from
`configs/_template.json`. Fields: `title`, `tagline`, `chain`, `events` (`table`, `from`, `to`, `amount`,
`decimals`, `symbol`), `queries` (`title`, `sql`), `prompts`. Amounts use the `<col>_dec` columns. Check the
queries with `check-examples.sh https://nuthatch-indexer.com/nests/<name>`.

## Preview

```sh
python3 showcase/serve.py            # http://localhost:8099/?nest=arc-usdc, proxying /nests/* to the live site
python3 showcase/serve.py --upstream http://127.0.0.1:8480    # or to a nest's nginx on this machine
```

## Deploy

Copy the page into the website repo so it is served beside `/nests/*`:

```sh
rsync -a --exclude serve.py --exclude examples --exclude README.md --exclude check-examples.sh \
      showcase/ ../nuthatch-frontend/public/showcase/
# then, in nuthatch-frontend: merge, vercel --prod.  Link: https://nuthatch-indexer.com/showcase/?nest=<name>
```

The page asks for 5 or 6 SQL queries on load for a small nest, plus one more per 25 tables beyond the first 25
(a 192-table nest makes about 12), and one every 15 seconds while "live" is ticked. The proxy allows 30 a minute
per address with a burst of 15, so a wide nest can show "Rate limited" on some panels if the page is reloaded
within about 20 seconds.

`examples/` holds screenshots and the agent-prompt check for the Arc nest.
