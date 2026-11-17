import { ponder } from "ponder:registry";
import { approval, deposit, transfer, withdrawal } from "ponder:schema";

ponder.on("WETH:Transfer", async ({ event, context }) => {
  await context.db
    .insert(transfer)
    .values({ id: event.id, blockNumber: event.block.number, ...event.args });
});

ponder.on("WETH:Approval", async ({ event, context }) => {
  await context.db
    .insert(approval)
    .values({ id: event.id, blockNumber: event.block.number, ...event.args });
});

ponder.on("WETH:Deposit", async ({ event, context }) => {
  await context.db
    .insert(deposit)
    .values({ id: event.id, blockNumber: event.block.number, ...event.args });
});

ponder.on("WETH:Withdrawal", async ({ event, context }) => {
  await context.db
    .insert(withdrawal)
    .values({ id: event.id, blockNumber: event.block.number, ...event.args });
});
