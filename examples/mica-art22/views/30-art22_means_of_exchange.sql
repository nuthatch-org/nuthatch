-- What is left after every deduction, each transfer counted once however many it meets, in a
-- single currency area only when both parties are labelled in the same one (RTS Art 3(5)). The
-- rest is 'unassigned': cross-area, or a party with no area label.
CREATE VIEW art22_means_of_exchange AS
SELECT block_number, log_index, day, value, area FROM art22_classified
WHERE NOT (ded_exchange OR ded_collateral OR ded_derivative OR ded_not_payment
           OR ded_same_person OR ded_non_custodial);
