-- RTS Art 3(1)(c): used to settle a derivative contract.
CREATE VIEW art22_ded_derivative AS
SELECT block_number, log_index, day, value FROM art22_classified WHERE ded_derivative;
