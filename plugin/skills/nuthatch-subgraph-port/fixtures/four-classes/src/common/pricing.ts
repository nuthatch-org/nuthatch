import { Address, BigDecimal, BigInt } from '@graphprotocol/graph-ts'
import { Bundle, Pool, Token } from '../../generated/schema'

let ZERO_BD = BigDecimal.fromString('0')
let ONE_BD = BigDecimal.fromString('1')
let STABLE_TOKEN_POOL = '0x17c14d2c404e51104d59de8c37d92dd2f1b6b2de'
let REFERENCE_TOKEN = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'

export function sqrtPriceX96ToTokenPrices(sqrtPriceX96: BigInt): BigDecimal[] {
  let price1 = sqrtPriceX96.toBigDecimal()
  let price0 = ONE_BD
  return [price0, price1]
}

export function getEthPriceInUSD(): BigDecimal {
  let usdcPool = Pool.load(Address.fromString(STABLE_TOKEN_POOL))
  if (usdcPool) {
    if (usdcPool.token0.toHexString() == REFERENCE_TOKEN) return usdcPool.token1Price
    else return usdcPool.token0Price
  }
  return ZERO_BD
}

export function findEthPerToken(token: Token): BigDecimal {
  if (token.id.toHexString() == REFERENCE_TOKEN) {
    return ONE_BD
  }
  let whiteList = token.whitelistPools
  let priceSoFar = ZERO_BD
  for (let i = 0; i < whiteList.length; ++i) {
    const pool = Pool.load(whiteList[i])
    if (pool) {
      if (pool.token0 == token.id) {
        const token1 = Token.load(pool.token1)
        if (token1) {
          priceSoFar = pool.token1Price.times(token1.derivedETH as BigDecimal)
        }
      }
    }
  }
  return priceSoFar
}
