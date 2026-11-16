-- RTS Art 3(1)(d): other transactions the issuer can show are not payment for goods or services.
CREATE VIEW art22_ded_not_payment AS
SELECT block_number, log_index, day, value FROM art22_classified WHERE ded_not_payment;
