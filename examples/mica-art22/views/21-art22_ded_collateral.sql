-- RTS Art 3(1)(b): used as collateral for transactions in financial instruments.
CREATE VIEW art22_ded_collateral AS
SELECT block_number, log_index, day, value FROM art22_classified WHERE ded_collateral;
