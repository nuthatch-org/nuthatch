import { onchainTable } from "ponder";

export const transfer = onchainTable("transfer", (t) => ({
  id: t.text().primaryKey(),
  blockNumber: t.bigint().notNull(),
  src: t.hex().notNull(),
  dst: t.hex().notNull(),
  wad: t.bigint().notNull(),
}));

export const approval = onchainTable("approval", (t) => ({
  id: t.text().primaryKey(),
  blockNumber: t.bigint().notNull(),
  src: t.hex().notNull(),
  guy: t.hex().notNull(),
  wad: t.bigint().notNull(),
}));

export const deposit = onchainTable("deposit", (t) => ({
  id: t.text().primaryKey(),
  blockNumber: t.bigint().notNull(),
  dst: t.hex().notNull(),
  wad: t.bigint().notNull(),
}));

export const withdrawal = onchainTable("withdrawal", (t) => ({
  id: t.text().primaryKey(),
  blockNumber: t.bigint().notNull(),
  src: t.hex().notNull(),
  wad: t.bigint().notNull(),
}));
