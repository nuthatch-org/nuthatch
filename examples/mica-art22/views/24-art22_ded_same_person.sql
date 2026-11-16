-- RTS Art 3(4): between addresses of the same person. The chain shows only an address paying
-- itself; the rest come from person:<id> labels.
CREATE VIEW art22_ded_same_person AS
SELECT block_number, log_index, day, value FROM art22_classified WHERE ded_same_person;
