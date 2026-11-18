import { BigInt, Bytes } from "@graphprotocol/graph-ts";
import { Transfer as TransferEvent } from "../generated/UNI/ERC20";
import { Account, Transfer } from "../generated/schema";

function account(id: Bytes): Account {
  let a = Account.load(id);
  if (a == null) {
    a = new Account(id);
    a.balance = BigInt.zero();
    a.transferCount = 0;
  }
  return a;
}

export function handleTransfer(event: TransferEvent): void {
  let t = new Transfer(event.transaction.hash.concatI32(event.logIndex.toI32()));
  t.from = event.params.from;
  t.to = event.params.to;
  t.value = event.params.value;
  t.blockNumber = event.block.number;
  t.timestamp = event.block.timestamp;
  t.transactionHash = event.transaction.hash;
  t.save();

  let from = account(event.params.from);
  from.balance = from.balance.minus(event.params.value);
  from.transferCount += 1;
  from.save();

  let to = account(event.params.to);
  to.balance = to.balance.plus(event.params.value);
  to.transferCount += 1;
  to.save();
}
