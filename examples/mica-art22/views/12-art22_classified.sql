-- Each transfer once, with every RTS deduction it meets as a flag. The deduction views and (d) read
-- these flags, so a rule is written once.
CREATE VIEW art22_classified AS
SELECT t.block_number, t.log_index, t.day, t.value,
       (t.payer = '0x0000000000000000000000000000000000000000' OR t.payee = '0x0000000000000000000000000000000000000000'
        OR COALESCE(a.exchange, 0) = 1 OR COALESCE(b.exchange, 0) = 1) AS ded_exchange,
       (COALESCE(a.collateral, 0) = 1 OR COALESCE(b.collateral, 0) = 1) AS ded_collateral,
       (COALESCE(a.derivative, 0) = 1 OR COALESCE(b.derivative, 0) = 1) AS ded_derivative,
       (COALESCE(a.not_payment, 0) = 1 OR COALESCE(b.not_payment, 0) = 1) AS ded_not_payment,
       (t.payer = t.payee OR COALESCE(a.person = b.person, false)) AS ded_same_person,
       (COALESCE(a.custodial, 0) = 0 AND COALESCE(b.custodial, 0) = 0) AS ded_non_custodial,
       CASE WHEN a.area IS NOT NULL AND a.area = b.area THEN a.area ELSE 'unassigned' END AS area
FROM art22_transfers t
LEFT JOIN art22_roles a ON a.address = t.payer
LEFT JOIN art22_roles b ON b.address = t.payee;
