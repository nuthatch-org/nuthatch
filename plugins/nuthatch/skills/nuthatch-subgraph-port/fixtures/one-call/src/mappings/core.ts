import { Address } from '@graphprotocol/graph-ts'
import { PoolCreated } from '../../generated/Factory/Factory'
import { ERC20 } from '../../generated/Factory/ERC20'
import { Token } from '../../generated/schema'

export function fetchTokenSymbol(tokenAddress: Address): string {
  let contract = ERC20.bind(tokenAddress)
  let result = contract.try_symbol()
  if (!result.reverted) {
    return result.value
  }
  return 'unknown'
}

export function handlePoolCreated(event: PoolCreated): void {
  let token0 = new Token(event.params.token0.toHex())
  token0.symbol = fetchTokenSymbol(event.params.token0)
  token0.save()
}
