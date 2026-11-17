-- RTS recital 2: no custodial wallet on either side, so outside (d): the issuer cannot know who or
-- where the parties are. Still in (c).
CREATE VIEW art22_ded_non_custodial AS
SELECT block_number, log_index, day, value FROM art22_classified WHERE ded_non_custodial;
