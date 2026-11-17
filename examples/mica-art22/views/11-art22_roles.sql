-- What the issuer's label sets say about each address, one row per address. Labels: issuer, casp,
-- custodial, collateral, derivative, not_payment, person:<id> (one person's addresses) and
-- area:<currency> (where its holder lives, from the CASP's report). An address given two persons or
-- two areas gets neither, so it is never deducted or assigned on a guess. See the README.
CREATE VIEW art22_roles AS
SELECT lower(address) AS address,
       MAX(CASE WHEN label IN ('issuer', 'casp') THEN 1 ELSE 0 END) AS exchange,
       MAX(CASE WHEN label = 'collateral' THEN 1 ELSE 0 END) AS collateral,
       MAX(CASE WHEN label = 'derivative' THEN 1 ELSE 0 END) AS derivative,
       MAX(CASE WHEN label = 'not_payment' THEN 1 ELSE 0 END) AS not_payment,
       MAX(CASE WHEN label = 'custodial' THEN 1 ELSE 0 END) AS custodial,
       CASE WHEN MIN(CASE WHEN label LIKE 'person:%' THEN label END)
               = MAX(CASE WHEN label LIKE 'person:%' THEN label END)
            THEN MIN(CASE WHEN label LIKE 'person:%' THEN label END) END AS person,
       CASE WHEN MIN(CASE WHEN label LIKE 'area:%' THEN label END)
               = MAX(CASE WHEN label LIKE 'area:%' THEN label END)
            THEN substr(MIN(CASE WHEN label LIKE 'area:%' THEN label END), 6) END AS area
FROM labels WHERE address IS NOT NULL
GROUP BY lower(address);
