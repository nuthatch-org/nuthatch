-- Recipe: holder_count (RFC-0023 tier 1) - the number of non-zero holders, DERIVED from the
-- Transfer events already indexed. No eth_call. Query it: SELECT * FROM fiat_token_v2_2_holder_count
CREATE VIEW fiat_token_v2_2_holder_count AS
SELECT COUNT(*) AS holders FROM (SELECT addr, SUM(d) AS balance FROM (SELECT lower("to") AS addr, CASE WHEN TRY_CAST("value" AS DECIMAL(38,0)) IS NOT NULL THEN CAST("value" AS HUGEINT) END AS d FROM "fiat_token_v2_2__transfer" UNION ALL SELECT lower("from") AS addr, -CASE WHEN TRY_CAST("value" AS DECIMAL(38,0)) IS NOT NULL THEN CAST("value" AS HUGEINT) END AS d FROM "fiat_token_v2_2__transfer") GROUP BY addr) WHERE addr <> '0x0000000000000000000000000000000000000000' AND balance <> 0;
