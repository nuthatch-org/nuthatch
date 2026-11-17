<!-- Written by nuthatch from nuthatch.toml; `nuthatch schema` rewrites it. -->
# AGENTS.md

This directory is a nuthatch nest: a local SQL index of these contracts on mainnet.
Query it here; never call a third-party API for this data.

- `usdc` = 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
- `pool` = 0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc

## Tables
- `pool__approval` - Approval(address,address,uint256) (block_number, block_hash, tx_hash, log_index, address, _seq, owner, spender, value)
- `pool__burn` - Burn(address,uint256,uint256,address) (block_number, block_hash, tx_hash, log_index, address, _seq, sender, amount0, amount1, to)
- `pool__mint` - Mint(address,uint256,uint256) (block_number, block_hash, tx_hash, log_index, address, _seq, sender, amount0, amount1)
- `pool__swap` - Swap(address,uint256,uint256,uint256,uint256,address) (block_number, block_hash, tx_hash, log_index, address, _seq, sender, amount0In, amount1In, amount0Out, amount1Out, to)
- `pool__sync` - Sync(uint112,uint112) (block_number, block_hash, tx_hash, log_index, address, _seq, reserve0, reserve1)
- `pool__transfer` - Transfer(address,address,uint256) (block_number, block_hash, tx_hash, log_index, address, _seq, from, to, value)
- `usdc__approval` - Approval(address,address,uint256) (block_number, block_hash, tx_hash, log_index, address, _seq, owner, spender, value)
- `usdc__authorization_canceled` - AuthorizationCanceled(address,bytes32) (block_number, block_hash, tx_hash, log_index, address, _seq, authorizer, nonce)
- `usdc__authorization_used` - AuthorizationUsed(address,bytes32) (block_number, block_hash, tx_hash, log_index, address, _seq, authorizer, nonce)
- `usdc__blacklisted` - Blacklisted(address) (block_number, block_hash, tx_hash, log_index, address, _seq, _account)
- `usdc__blacklister_changed` - BlacklisterChanged(address) (block_number, block_hash, tx_hash, log_index, address, _seq, newBlacklister)
- `usdc__burn` - Burn(address,uint256) (block_number, block_hash, tx_hash, log_index, address, _seq, burner, amount)
- `usdc__master_minter_changed` - MasterMinterChanged(address) (block_number, block_hash, tx_hash, log_index, address, _seq, newMasterMinter)
- `usdc__mint` - Mint(address,address,uint256) (block_number, block_hash, tx_hash, log_index, address, _seq, minter, to, amount)
- `usdc__minter_configured` - MinterConfigured(address,uint256) (block_number, block_hash, tx_hash, log_index, address, _seq, minter, minterAllowedAmount)
- `usdc__minter_removed` - MinterRemoved(address) (block_number, block_hash, tx_hash, log_index, address, _seq, oldMinter)
- `usdc__ownership_transferred` - OwnershipTransferred(address,address) (block_number, block_hash, tx_hash, log_index, address, _seq, previousOwner, newOwner)
- `usdc__pause` - Pause() (block_number, block_hash, tx_hash, log_index, address, _seq)
- `usdc__pauser_changed` - PauserChanged(address) (block_number, block_hash, tx_hash, log_index, address, _seq, newAddress)
- `usdc__rescuer_changed` - RescuerChanged(address) (block_number, block_hash, tx_hash, log_index, address, _seq, newRescuer)
- `usdc__transfer` - Transfer(address,address,uint256) (block_number, block_hash, tx_hash, log_index, address, _seq, from, to, value)
- `usdc__un_blacklisted` - UnBlacklisted(address) (block_number, block_hash, tx_hash, log_index, address, _seq, _account)
- `usdc__unpause` - Unpause() (block_number, block_hash, tx_hash, log_index, address, _seq)

## Run it headless

`nuthatch dev` runs until it is stopped. Start it in the background with its log outside the
nest, whose identity counts every file in it, then wait until `/ready` puts it fewer than ten
blocks behind the tip:

```sh
nuthatch dev --backfill 300 > /tmp/nuthatch-dev.log 2>&1 &
for i in $(seq 150); do curl -sf 127.0.0.1:8288/ready | grep -Eq '"lag_blocks":[0-9],' && break; sleep 2; done
```

`--backfill 300` starts 300 blocks behind the tip; without it `dev` reads from each contract's
start block. A nest that has already indexed ignores it. The log says why if it never gets
there.

## Query

```sh
nuthatch sql "SELECT count(*) FROM pool__approval" --json
```

`llms.txt` has the HTTP routes and the SQL type rules: wide integers are text with a `_dec`
companion, and `"from"` and `"to"` must be double-quoted.

## MCP

`nuthatch mcp --print-config --client <name>` prints the config file for `claude`, `cursor`,
`codex`, `vscode`, `windsurf` or `gemini`, and says on stderr where it goes. The server bridges
to the running `nuthatch dev`; call its `schema` tool first.
