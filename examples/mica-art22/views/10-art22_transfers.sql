-- Every EURC transfer as a transaction (MiCA Art 22(1)(c)), mints and burns included: a mint or
-- a burn is an exchange with the issuer, which art22_ded_exchange deducts from (d).
-- A plain CAST, so a value that will not parse refuses the figures rather than dropping a row.
CREATE VIEW art22_transfers AS
SELECT block_number, log_index, lower("from") AS payer, lower("to") AS payee,
       CAST(value AS HUGEINT) AS value,
       DATE '1970-01-01' + CAST(block_timestamp // 86400 AS INTEGER) AS day
FROM "fiat_token_v2_2__transfer";
