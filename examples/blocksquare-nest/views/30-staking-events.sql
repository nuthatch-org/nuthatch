-- Deposits and withdrawals across the four property staking pools, one row per event.
-- `pool` is the alias of the pool contract. Amounts are the raw event values.
CREATE VIEW staking_events AS
  SELECT 'community' AS pool, 'deposit' AS kind, block_number, block_timestamp, "owner", property, "inAmount_dec" AS amount_in, "outAmount_dec" AS amount_out FROM staking_community__deposit
  UNION ALL SELECT 'community', 'withdraw', block_number, block_timestamp, "owner", property, "inAmount_dec", "outAmount_dec" FROM staking_community__withdraw
  UNION ALL SELECT 'issuer', 'deposit', block_number, block_timestamp, "owner", property, "inAmount_dec", "outAmount_dec" FROM staking_issuer__deposit
  UNION ALL SELECT 'issuer', 'withdraw', block_number, block_timestamp, "owner", property, "inAmount_dec", "outAmount_dec" FROM staking_issuer__withdraw
  UNION ALL SELECT 'rwa', 'deposit', block_number, block_timestamp, "owner", property, "inAmount_dec", "outAmount_dec" FROM staking_rwa__deposit
  UNION ALL SELECT 'rwa', 'withdraw', block_number, block_timestamp, "owner", property, "inAmount_dec", "outAmount_dec" FROM staking_rwa__withdraw
  UNION ALL SELECT 'landhive', 'deposit', block_number, block_timestamp, "owner", property, "inAmount_dec", "outAmount_dec" FROM staking_landhive__deposit
  UNION ALL SELECT 'landhive', 'withdraw', block_number, block_timestamp, "owner", property, "inAmount_dec", "outAmount_dec" FROM staking_landhive__withdraw;
