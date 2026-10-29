-- RFC-0075 S4: views over the hot and sealed facts, and over the entities.
CREATE VIEW per_kblock AS
SELECT block_number - block_number % 1000 AS kblock, COUNT(*) AS transfers, SUM(value_dec) AS volume
FROM tab__transfer
GROUP BY 1;

CREATE VIEW busy_receivers AS
SELECT "to", n, total FROM received WHERE n >= 3;

CREATE VIEW two_way AS
SELECT r."to" AS account, r.n AS received_n, s.n AS sent_n, r.total - s.total AS net
FROM received r JOIN sent s ON r."to" = s."from";
