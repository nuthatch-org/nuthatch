SELECT t."to", COUNT(*) AS n, SUM(CAST(t.value AS HUGEINT)) AS total, MIN(t.block_number) AS first_block, MAX(t.block_number) AS last_block
FROM tab__transfer t
GROUP BY t."to"
