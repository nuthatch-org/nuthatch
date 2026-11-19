-- One row per property token the PropertyFactory has announced, with transfer counts and the raw
-- capital-stack figures from its last CapitalStackChange. Raw event values, no valuation math.
CREATE VIEW property_tokens AS
  SELECT f.proptoken AS property_token, f."certifiedPartner" AS certified_partner,
         f.block_number AS created_block, f.block_timestamp AS created_at,
         (SELECT count(*) FROM property_token__transfer t WHERE t.address = f.proptoken) AS transfers,
         (SELECT count(*) FROM property_token__capital_stack_change c WHERE c.address = f.proptoken) AS capital_stack_changes
  FROM property_factory__new_prop_token f;
