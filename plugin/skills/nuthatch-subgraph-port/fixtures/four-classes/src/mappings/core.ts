import { Address, BigDecimal, BigInt, ethereum } from '@graphprotocol/graph-ts'
import { PoolCreated } from '../../generated/Factory/Factory'
import { Swap as SwapEvent } from '../../generated/Pool/Pool'
import { BlockStat, Bundle, Pool, Swap, Token } from '../../generated/schema'
import { fetchTokenDecimals, fetchTokenSymbol } from '../common/token'
import { findEthPerToken, getEthPriceInUSD, sqrtPriceX96ToTokenPrices } from '../common/pricing'

let ZERO_BD = BigDecimal.fromString('0')
let ZERO_BI = BigInt.fromI32(0)

export function handlePoolCreated(event: PoolCreated): void {
  let token0 = new Token(event.params.token0.toHex())
  token0.symbol = fetchTokenSymbol(event.params.token0)
  token0.decimals = fetchTokenDecimals(event.params.token0)
  token0.derivedETH = ZERO_BD
  token0.save()

  let token1 = new Token(event.params.token1.toHex())
  token1.symbol = fetchTokenSymbol(event.params.token1)
  token1.decimals = fetchTokenDecimals(event.params.token1)
  token1.derivedETH = ZERO_BD
  token1.save()

  let pool = new Pool(event.params.pool.toHex())
  pool.token0 = token0.id
  pool.token1 = token1.id
  pool.sqrtPrice = ZERO_BI
  pool.token0Price = ZERO_BD
  pool.token1Price = ZERO_BD
  pool.liquidity = ZERO_BI
  pool.totalValueLockedToken0 = ZERO_BD
  pool.save()
}

export function handleSwap(event: SwapEvent): void {
  let bundle = Bundle.load('1')
  if (bundle == null) {
    bundle = new Bundle('1')
  }
  bundle.ethPriceUSD = getEthPriceInUSD()
  bundle.save()

  let pool = Pool.load(event.address.toHex())!
  pool.sqrtPrice = event.params.sqrtPriceX96
  pool.liquidity = event.params.liquidity
  pool.totalValueLockedToken0 = pool.totalValueLockedToken0.plus(event.params.amount0)
  let prices = sqrtPriceX96ToTokenPrices(pool.sqrtPrice)
  pool.token0Price = prices[0]
  pool.token1Price = prices[1]
  pool.save()

  let token0 = Token.load(pool.token0)!
  token0.derivedETH = findEthPerToken(token0 as Token)
  token0.save()

  let swap = new Swap(event.transaction.hash.toHex() + '-' + event.logIndex.toString())
  swap.pool = pool.id
  swap.amount0 = event.params.amount0
  swap.timestamp = event.block.timestamp
  swap.save()
}

export function handleBlock(block: ethereum.Block): void {
  let stat = new BlockStat(block.hash.toHex())
  stat.blockNumber = block.number
  stat.save()
}
