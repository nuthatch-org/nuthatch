-- The calendar days of each quarter a transfer fell in: a day with no transfer still counts toward
-- the average (RTS Art 4(1)). A quarter the nest holds only part of averages over the whole of it.
CREATE VIEW art22_quarter_days AS
SELECT year, quarter,
       CASE quarter
         WHEN 1 THEN CASE WHEN year % 4 = 0 AND (year % 100 <> 0 OR year % 400 = 0) THEN 91 ELSE 90 END
         WHEN 2 THEN 91
         ELSE 92
       END AS days
FROM (SELECT DISTINCT CAST(date_part('year', day) AS INTEGER) AS year,
                      CAST(date_part('quarter', day) AS INTEGER) AS quarter
      FROM art22_transfers);
