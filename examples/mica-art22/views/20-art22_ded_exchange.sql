-- RTS Art 3(1)(a): exchanged for funds or other crypto-assets with the issuer or a CASP. A mint or
-- a burn is one with the issuer.
CREATE VIEW art22_ded_exchange AS
SELECT block_number, log_index, day, value FROM art22_classified WHERE ded_exchange;
