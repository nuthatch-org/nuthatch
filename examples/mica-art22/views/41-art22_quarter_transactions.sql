-- Art 22(1)(c): the quarterly average number and aggregate value of transactions per day, value
-- in EUR (EURC has 6 decimals, and EUR is the issuer's home currency, so no FX applies). Both are
-- truncated: the count to hundredths, the value to the micro-euro, so the figure is exact decimal.
CREATE VIEW art22_quarter_transactions AS
SELECT q.year, q.quarter, q.days, COUNT(*) AS transactions, SUM(t.value) AS value_base_units,
       CAST(CAST(COUNT(*) AS HUGEINT) * 100 // q.days AS DECIMAL(38,0)) * CAST('0.01' AS DECIMAL(3,2)) AS avg_per_day,
       CAST(SUM(t.value) // q.days AS DECIMAL(38,0)) * CAST('0.000001' AS DECIMAL(7,6)) AS avg_eur_per_day
FROM art22_transfers t
JOIN art22_quarter_days q ON q.year = CAST(date_part('year', t.day) AS INTEGER)
                         AND q.quarter = CAST(date_part('quarter', t.day) AS INTEGER)
GROUP BY q.year, q.quarter, q.days;
