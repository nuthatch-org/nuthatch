// `verify`'s comparison and head label, without a module (#2614, #2615).
// Usage: node --test embed/hosts/package/verify.test.mjs
import { test } from "node:test";
import assert from "node:assert/strict";
import { compare, headOf, statementVerdict } from "./verify.js";

const here = { rows: [{ who: "0x1", total: "100" }], columns: ["who", "total"] };
const verdict = (server, h = here) => statementVerdict(compare(server, h));

test("a column the server left out of `columns` is still compared from its rows", () => {
  const server = { rows: [{ who: "0x1", total: "999999" }], columns: ["who"] };
  assert.equal(verdict(server), "differs");
  const total = compare(server, here).columns.find((c) => c.column === "total");
  assert.equal(total.verdict, "differs");
  assert.deepEqual(total.values[0], { row: 0, server: "999999", here: "100" });
});

test("an answer with `columns: []` is compared on the keys its rows carry", () => {
  assert.equal(verdict({ rows: [{ who: "0x1", total: "999999" }], columns: [] }), "differs");
  assert.equal(verdict({ rows: [{ who: "0x1", total: "100" }], columns: [] }), "equal");
});

test("a column the server returned that this nest cannot compute keeps the verdict from equal", () => {
  const server = { rows: [{ who: "0x1", total: "100", extra: "7" }], columns: ["who", "total", "extra"] };
  assert.equal(verdict(server), "incomplete");
  const extra = compare(server, here).columns.find((c) => c.column === "extra");
  assert.equal(extra.verdict, "not computable");
});

test("a column the server left out altogether went unchecked, so the statement is not equal", () => {
  const server = { rows: [{ who: "0x1" }], columns: ["who"] };
  assert.equal(verdict(server), "incomplete");
});

test("the same columns and rows on both sides are equal", () => {
  assert.equal(verdict({ rows: [{ total: "100", who: "0x1" }], columns: ["total", "who"] }), "equal");
});

test("without a verified head the head is the RPC's", () => {
  assert.equal(headOf(undefined, {}, 150).head, "the RPC's, unverified");
});

test("a refused verification is said, with its reason, not labelled verified", () => {
  const h = headOf(true, { unverified: "block 7's parent is 0xab", verified: { finalized: { number: 100 } } }, 150);
  assert.match(h.head, /^refused/);
  assert.equal(h.unverified, "block 7's parent is 0xab");
});

test("rows above the verified block are labelled the RPC's", () => {
  const h = headOf(true, { verified: { finalized: { number: 100, hash: "0x1" } } }, 150);
  assert.equal(h.verified_through, 100);
  assert.match(h.head, /verified through block 100/);
  assert.match(h.head, /101 to 150 are the RPC's/);
});

test("an answer at or below the verified block is verified", () => {
  const h = headOf(true, { verified: { finalized: { number: 200, hash: "0x1" } } }, 150);
  assert.equal(h.head, "verified");
  assert.equal(h.verified_through, 200);
});

test("a verified source with no verified block reported is not called verified", () => {
  assert.match(headOf(true, { verified: null }, 150).head, /^refused/);
});

const hydrated = { dataset: "d6fd", indexed_from: 10, complete_through: 120, pinned: false };

test("history hydrated from a mirror is labelled the mirror's, even under a verified head (#2738)", () => {
  const h = headOf(true, { verified: { finalized: { number: 200, hash: "0x1" } }, mirror: hydrated }, 150);
  assert.notEqual(h.head, "verified");
  assert.match(h.head, /blocks 10 to 120 are the mirror's, unchecked/);
  assert.match(h.head, /verified for blocks 121 to 150/);
  assert.deepEqual(h.mirror_unchecked, { from: 10, through: 120 });
});

test("the mirror's blocks are named beside the RPC's above the verified block (#2738)", () => {
  const h = headOf(true, { verified: { finalized: { number: 130, hash: "0x1" } }, mirror: hydrated }, 150);
  assert.match(h.head, /verified for blocks 121 to 130/);
  assert.match(h.head, /131 to 150 are the RPC's/);
  assert.match(h.head, /10 to 120 are the mirror's/);
});

test("without a verified head a mirror's history is still named as the mirror's (#2738)", () => {
  const h = headOf(undefined, { mirror: hydrated }, 150);
  assert.match(h.head, /^the RPC's, unverified/);
  assert.match(h.head, /blocks 10 to 120 are the mirror's/);
  assert.deepEqual(h.mirror_unchecked, { from: 10, through: 120 });
});

test("a mirror that held no sealed block leaves the label alone (#2738)", () => {
  const h = headOf(true, { verified: { finalized: { number: 200, hash: "0x1" } }, mirror: { ...hydrated, complete_through: 0 } }, 150);
  assert.equal(h.head, "verified");
  assert.equal(h.mirror_unchecked, undefined);
});

test("a verified block below the mirror's end verifies nothing past it (#2738)", () => {
  const h = headOf(true, { verified: { finalized: { number: 100, hash: "0x1" } }, mirror: hydrated }, 150);
  assert.doesNotMatch(h.head, /verified for/);
  assert.match(h.head, /10 to 120 are the mirror's, unchecked; blocks 121 to 150 are the RPC's/);
});
