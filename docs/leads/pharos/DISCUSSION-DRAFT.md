# Discussion draft (do not post until the lead has reviewed)

Category: Ideas

**Title:** Optional local replay for the permanent-liquidity scan (#2048)

Issue #2048 replays every Transfer of a fungible LP token from genesis through `eth_getLogs`, and the
scan was stopped because the local network could not sustain it. I wanted to see how far a local
indexer gets on that exact job, so I tried Nuthatch (a single-binary, self-hosted indexer, MIT/Apache).

What I ran: two of your LP tokens (0xed279f... and 0x3041cb...), blocks 10,000,000 to 26,450,931
(16.45M blocks), one public RPC, one laptop, 4 parallel windows of 5,000 blocks.

- Wall time: 7m26s to the tip, 109,446 Transfer events sealed to Parquet.
- Result: replayed supply matches `totalSupply()` for the first token exactly (1,272 holders). The second
  matches once the 1000 wei held by 0x0 is added (373 holders), which is the normal Uniswap V2 minimum
  liquidity lock. This check is mass conservation only: it would catch a missing mint or burn, but a wrong,
  missing or duplicated transfer between two non-zero holders leaves the sum unchanged. I did not compare
  individual holder balances with `balanceOf` at the tip, so this is not yet a verified per-holder replay.
- The run is resumable. Re-running continues where it stopped instead of starting over.
- Caveat: the public RPC rate limited us a lot (hundreds of retries). A run of 10 tokens at once against
  the same public endpoint did not finish. A paid archive key would be faster; I did not try one.

I did not measure your current Dwellir path, so I can't give a before/after number for it.

Ask: would you take a small opt-in script (`npm run scan:permanent-liquidity`) that builds this nest per
token and prints the supply check? It is read-only and does not touch the Worker, D1 or scoring. The
patch is ready (one script, one test, a short doc, the argv policy line). If the answer is no, that is
fine, and the script lives happily outside your repo.

Try it without any code from us (include this paragraph only if the hosted nest is live when posting): NEST_URL (hosted nest, https://nuthatch-indexer.com/nests/pharos-eth)
has the same two tokens and answers SQL.
