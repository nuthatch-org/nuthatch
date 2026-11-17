-- Each deduction on its own line. A transfer can meet several, so these overlap and do not sum to
-- (c) less (d); art22_means_of_exchange removes each transfer once.
CREATE VIEW art22_quarter_deductions AS
WITH d AS (
  SELECT 'exchange' AS deduction, day, value FROM art22_ded_exchange
  UNION ALL SELECT 'collateral', day, value FROM art22_ded_collateral
  UNION ALL SELECT 'derivative', day, value FROM art22_ded_derivative
  UNION ALL SELECT 'not_payment', day, value FROM art22_ded_not_payment
  UNION ALL SELECT 'same_person', day, value FROM art22_ded_same_person
  UNION ALL SELECT 'non_custodial', day, value FROM art22_ded_non_custodial
)
SELECT CAST(date_part('year', day) AS INTEGER) AS year,
       CAST(date_part('quarter', day) AS INTEGER) AS quarter,
       deduction, COUNT(*) AS transactions, SUM(value) AS value_base_units
FROM d GROUP BY 1, 2, 3;
