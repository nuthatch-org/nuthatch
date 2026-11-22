# Agent prompts for the gooddollar-xdc nest (replace NEST_URL)

1. Use the Nuthatch nest at NEST_URL (GoodDollar UBIScheme on XDC). Read GET NEST_URL/tables first. Then, with
   GET NEST_URL/sql?q=..., count UBIClaimed events per UTC day for the last 14 days of data (table
   ubi_scheme__u_b_i_claimed, use block_timestamp) and give the total claimed per day using the `amount_dec`
   column. Never SUM the raw `amount` column. Show the SQL.

2. Use the nest at NEST_URL. How many distinct addresses ("claimer") have ever claimed UBI on XDC, and how many
   claimed in the most recent 30 days of data? Then list the top 10 claimers by number of claims with their
   first and last claim block_timestamp. Run read-only SQL via GET NEST_URL/sql?q=... and show the queries.

3. Use the nest at NEST_URL. Join ubi_scheme__u_b_i_calculated (day, dailyUbi_dec) with the claim counts per day
   from ubi_scheme__u_b_i_claimed. Find the protocol days where the daily UBI per claimer changed by more than
   20% from the previous day, and print day, dailyUbi_dec, claimer count. Also report the highest block the nest
   has (GET NEST_URL/ is the status page) so the reader knows how fresh the answer is.
