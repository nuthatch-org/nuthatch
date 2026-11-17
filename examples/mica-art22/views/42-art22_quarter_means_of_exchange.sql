-- Art 22(1)(d): the same averages for what art22_means_of_exchange leaves, per single currency area.
CREATE VIEW art22_quarter_means_of_exchange AS
SELECT q.year, q.quarter, m.area, q.days, COUNT(*) AS transactions, SUM(m.value) AS value_base_units,
       CAST(CAST(COUNT(*) AS HUGEINT) * 100 // q.days AS DECIMAL(38,0)) * CAST('0.01' AS DECIMAL(3,2)) AS avg_per_day,
       CAST(SUM(m.value) // q.days AS DECIMAL(38,0)) * CAST('0.000001' AS DECIMAL(7,6)) AS avg_eur_per_day
FROM art22_means_of_exchange m
JOIN art22_quarter_days q ON q.year = CAST(date_part('year', m.day) AS INTEGER)
                         AND q.quarter = CAST(date_part('quarter', m.day) AS INTEGER)
GROUP BY q.year, q.quarter, m.area, q.days;
