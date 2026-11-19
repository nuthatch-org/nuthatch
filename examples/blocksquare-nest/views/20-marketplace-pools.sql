-- One row per MarketplacePool the factory created, with its deposit and withdraw event counts.
-- Amounts are raw token units (`_dec` where they fit), not valued in any currency.
CREATE VIEW marketplace_pools AS
  SELECT c."marketplacePoolAddress" AS pool, c."cpIdentifier" AS cp_identifier, c."cpUrl" AS cp_url,
         c."tokenName" AS token_name, c."tokenSymbol" AS token_symbol,
         c.block_number AS created_block, c.block_timestamp AS created_at,
         (SELECT count(*) FROM marketplace_pool__deposit d WHERE d.address = c."marketplacePoolAddress") AS deposits,
         (SELECT count(*) FROM marketplace_pool__withdraw w WHERE w.address = c."marketplacePoolAddress") AS withdrawals
  FROM marketplace_pool_factory__marketplace_pool_created c;
