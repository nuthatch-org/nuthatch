# Agent prompts for the sablier-xdc nest (replace NEST_URL)

1. List the Sablier Lockup streams created on XDC. Use the nest at NEST_URL: read GET NEST_URL/tables, then run
   GET NEST_URL/sql?q=... over the tables named lockup_v2_0__create_lockup_*, lockup_v3_0__create_lockup_* and
   lockup_v4_0__create_lockup_*. Give a count per contract version and the 5 most recent streams (streamId_dec,
   block_timestamp, tx_hash). Use the `_dec` columns, never SUM the raw uint columns.

2. Using the nest at NEST_URL, how many Flow streams were created on XDC, how many were paused, voided or
   refunded, and what is the first and last block_timestamp of any Flow event? Tables are named flow_v1_1__*,
   flow_v2_0__*, flow_v3_0__*. Run the SQL through GET NEST_URL/sql?q=... and show the queries you ran.

3. Using the nest at NEST_URL, list every Sablier Airdrops campaign created on XDC (tables factory_merkle_*__create_*
   and merkle_factory_v1_3__create_*): campaign address, the block it was created in, and its factory version.
   Then say which of those campaigns have emitted any event in the matching merkle_* child tables.
