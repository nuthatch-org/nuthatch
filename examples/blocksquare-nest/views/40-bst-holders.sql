-- BST balance by address from the token's Transfer events, summed from the start block. Raw token
-- units (18 decimals). Mints and burns show as the zero address on the other side. Rows whose
-- amount does not fit `_amount_dec` are left out (none in this history).
CREATE VIEW bst_balances AS
  SELECT addr, sum(delta) AS balance_raw FROM (
    SELECT "_to" AS addr, "_amount_dec" AS delta FROM blocksquare_token__transfer WHERE NOT "_amount_overflow"
    UNION ALL
    SELECT "_from" AS addr, -"_amount_dec" AS delta FROM blocksquare_token__transfer WHERE NOT "_amount_overflow"
  ) GROUP BY addr;
