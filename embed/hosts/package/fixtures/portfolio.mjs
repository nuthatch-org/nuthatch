// RFC-0081 S3's gate fixture: the four portfolio statements of RFC-0077 S7, copied from Lodestar's
// `src/lib/verify/sql.ts` (2026-11-01) so that no Lodestar code is in the path. Its CTEs are
// graph-allocations-nest's `views/90-lodestar-indexers.sql` and kittiwake's portfolio queries. Where
// Lodestar wrote the pools and thaw requests in as literal rows, these read the nest's own tables,
// limited to the wallet and the indexers whose pools it holds, so one statement can be sent to the
// hosted nest and run in a nest narrowed by `filtersFor`.

const LEDGER = () => `
WITH horizon_start AS (SELECT 408847369 AS bn),
pool_shares_series AS (
  SELECT sp, k, SUM(sh) OVER (PARTITION BY sp ORDER BY k ROWS UNBOUNDED PRECEDING) AS cum_shares FROM (
    SELECT LOWER(indexer) AS sp, block_number * 100000 + log_index AS k,  CAST(shares AS HUGEINT) AS sh FROM staking_legacy__stake_delegated
    UNION ALL SELECT LOWER(indexer), block_number * 100000 + log_index, -CAST(shares AS HUGEINT) FROM staking_legacy__stake_delegated_locked
    UNION ALL SELECT LOWER("serviceProvider"), block_number * 100000 + log_index,  CAST(shares AS HUGEINT) FROM staking__tokens_delegated
    UNION ALL SELECT LOWER("serviceProvider"), block_number * 100000 + log_index, -CAST(shares AS HUGEINT) FROM staking__tokens_undelegated
  )
),
cuts AS (SELECT LOWER(indexer) AS sp, CAST("indexingRewardCut" AS HUGEINT) AS cut, block_number * 100000 + log_index AS k FROM staking_legacy__delegation_parameters_updated),
legacy_reward_share AS (
  SELECT r.sp, r.ts, r.bn, r.k, r.amount - r.amount * c.cut // 1000000 AS t
  FROM (
    SELECT sp, k, amount, ts, bn, ck, pk FROM (
      SELECT sp, k, amount, ts, bn, src,
             MAX(CASE WHEN src = 0 THEN k END) OVER (PARTITION BY sp ORDER BY k, src ROWS UNBOUNDED PRECEDING) AS ck,
             MAX(CASE WHEN src = 1 THEN k END) OVER (PARTITION BY sp ORDER BY k, src ROWS UNBOUNDED PRECEDING) AS pk
      FROM (
        SELECT sp, k, 0 AS src, CAST(NULL AS HUGEINT) AS amount, CAST(NULL AS BIGINT) AS ts, CAST(NULL AS UBIGINT) AS bn FROM cuts
        UNION ALL SELECT sp, k, 1, CAST(NULL AS HUGEINT), CAST(NULL AS BIGINT), CAST(NULL AS UBIGINT) FROM pool_shares_series
        UNION ALL SELECT LOWER(indexer), block_number * 100000 + log_index, 2, CAST(amount AS HUGEINT), CAST(NULL AS BIGINT), block_number FROM rewards__rewards_assigned
      )
    ) WHERE src = 2
  ) r
  JOIN cuts c ON c.sp = r.sp AND c.k = r.ck
  LEFT JOIN pool_shares_series ps ON ps.sp = r.sp AND ps.k = r.pk
  WHERE r.bn < COALESCE((SELECT bn FROM horizon_start), 9223372036854775807) AND COALESCE(ps.cum_shares, 0) > 0
),
legacy_path_share AS (
  SELECT h.sp, h.ts, h.bn, h.k, h.amount - h.amount * c.cut // 1000000 AS t
  FROM (
    SELECT sp, k, a, amount, ts, bn, ck, pk FROM (
      SELECT sp, k, a, amount, ts, bn, src,
             MAX(CASE WHEN src = 0 THEN k END) OVER (PARTITION BY sp ORDER BY k, src ROWS UNBOUNDED PRECEDING) AS ck,
             MAX(CASE WHEN src = 1 THEN k END) OVER (PARTITION BY sp ORDER BY k, src ROWS UNBOUNDED PRECEDING) AS pk
      FROM (
        SELECT sp, k, 0 AS src, CAST(NULL AS VARCHAR) AS a, CAST(NULL AS HUGEINT) AS amount, CAST(NULL AS BIGINT) AS ts, CAST(NULL AS UBIGINT) AS bn FROM cuts
        UNION ALL SELECT sp, k, 1, CAST(NULL AS VARCHAR), CAST(NULL AS HUGEINT), CAST(NULL AS BIGINT), CAST(NULL AS UBIGINT) FROM pool_shares_series
        UNION ALL SELECT LOWER(indexer), block_number * 100000 + log_index, 2, LOWER("allocationID"), CAST(amount AS HUGEINT), CAST(NULL AS BIGINT), block_number FROM rewards__horizon_rewards_assigned
      )
    ) WHERE src = 2
  ) h
  JOIN cuts c ON c.sp = h.sp AND c.k = h.ck
  LEFT JOIN pool_shares_series ps ON ps.sp = h.sp AND ps.k = h.pk
  WHERE h.a IN (SELECT LOWER("allocationID") FROM staking_legacy__allocation_created) AND COALESCE(ps.cum_shares, 0) > 0
)
SELECT LOWER(indexer) AS indexer, CAST(NULL AS BIGINT) AS ts, block_number, block_number * 100000 + log_index AS k, 'delegated' AS kind, CAST(0 AS HUGEINT) AS stake_delta, CAST(tokens AS HUGEINT) AS pool_delta, CAST(shares AS HUGEINT) AS shares_delta, CAST(0 AS HUGEINT) AS thawing_delta FROM staking_legacy__stake_delegated
UNION ALL SELECT LOWER(indexer), CAST(NULL AS BIGINT), block_number, block_number * 100000 + log_index, 'undelegated', 0, -CAST(tokens AS HUGEINT), -CAST(shares AS HUGEINT), 0 FROM staking_legacy__stake_delegated_locked
UNION ALL SELECT LOWER("serviceProvider"), CAST(NULL AS BIGINT), block_number, block_number * 100000 + log_index, 'delegated',   0,  CAST(tokens AS HUGEINT),  CAST(shares AS HUGEINT), 0 FROM staking__tokens_delegated
UNION ALL SELECT LOWER("serviceProvider"), CAST(NULL AS BIGINT), block_number, block_number * 100000 + log_index, 'undelegated', 0, -CAST(tokens AS HUGEINT), -CAST(shares AS HUGEINT), CAST(tokens AS HUGEINT) FROM staking__tokens_undelegated
UNION ALL SELECT LOWER("serviceProvider"), CAST(NULL AS BIGINT), block_number, block_number * 100000 + log_index, 'pool_added',  0,  CAST(tokens AS HUGEINT), 0, 0 FROM staking__tokens_to_delegation_pool_added
UNION ALL SELECT LOWER("serviceProvider"), CAST(NULL AS BIGINT), block_number, block_number * 100000 + log_index, 'pool_slashed', 0, -CAST(tokens AS HUGEINT), 0, 0 FROM staking__delegation_slashed
UNION ALL SELECT LOWER(indexer), CAST(NULL AS BIGINT), block_number, block_number * 100000 + log_index, 'rebate_delegation_rewards', 0, CAST("delegationRewards" AS HUGEINT), 0, 0 FROM staking_legacy__rebate_collected
UNION ALL SELECT LOWER(indexer), CAST(NULL AS BIGINT), block_number, block_number * 100000 + log_index, 'rebate_delegation_fees',    0, CAST("delegationFees" AS HUGEINT)   , 0, 0 FROM staking_legacy__rebate_claimed
UNION ALL SELECT LOWER("serviceProvider"), CAST(NULL AS BIGINT), block_number, block_number * 100000 + log_index, 'withdrawn', 0, 0, 0, -CAST(tokens AS HUGEINT) FROM staking__delegated_tokens_withdrawn
UNION ALL SELECT sp, ts, bn, k, 'legacy_reward_share', 0, t, 0, 0 FROM legacy_reward_share
UNION ALL SELECT sp, ts, bn, k, 'legacy_path_share', 0, t, 0, 0 FROM legacy_path_share`;

const INDEXERS = () => `
WITH
stake AS (
  SELECT indexer AS sp,
         SUM(pool_delta) FILTER (WHERE kind NOT IN ('delegated', 'undelegated')) AS pool_rewards_added
  FROM lodestar_indexer_ledger GROUP BY 1
),
delegation AS (
  SELECT delegator, sp, SUM(tok) AS net_tokens, SUM(sh) AS net_shares FROM (
    SELECT LOWER(delegator) AS delegator, LOWER(indexer) AS sp,            CAST(tokens AS HUGEINT) AS tok,  CAST(shares AS HUGEINT) AS sh FROM staking_legacy__stake_delegated
    UNION ALL SELECT LOWER(delegator), LOWER(indexer),                     -CAST(tokens AS HUGEINT),        -CAST(shares AS HUGEINT)      FROM staking_legacy__stake_delegated_locked
    UNION ALL SELECT LOWER(delegator), LOWER("serviceProvider"),            CAST(tokens AS HUGEINT),         CAST(shares AS HUGEINT)      FROM staking__tokens_delegated
    UNION ALL SELECT LOWER(delegator), LOWER("serviceProvider"),           -CAST(tokens AS HUGEINT),        -CAST(shares AS HUGEINT)      FROM staking__tokens_undelegated
  ) GROUP BY 1, 2
),
pool AS (
  SELECT sp,
         SUM(net_tokens)                                     AS delegated_net,
         SUM(net_shares)                                     AS delegator_shares
  FROM delegation GROUP BY 1
),
thawing AS (
  SELECT sp, GREATEST(SUM(t), 0) AS delegated_thawing_tokens FROM (
    SELECT LOWER("serviceProvider") AS sp,  CAST(tokens AS HUGEINT) AS t FROM staking__tokens_undelegated
    UNION ALL SELECT LOWER("serviceProvider"), -CAST(tokens AS HUGEINT) FROM staking__delegated_tokens_withdrawn
  ) GROUP BY 1
),
cut_events AS (
  SELECT LOWER(indexer) AS sp, 'indexing' AS kind, CAST("indexingRewardCut" AS HUGEINT) AS cut, block_number AS bn, log_index AS li, CAST(NULL AS BIGINT) AS ts, 'legacy' AS era FROM staking_legacy__delegation_parameters_updated
  UNION ALL SELECT LOWER(indexer), 'query',    CAST("queryFeeCut" AS HUGEINT),       block_number, log_index, CAST(NULL AS BIGINT), 'legacy' FROM staking_legacy__delegation_parameters_updated
  UNION ALL SELECT LOWER("serviceProvider"), CASE WHEN CAST("paymentType" AS INTEGER) = 2 THEN 'indexing' WHEN CAST("paymentType" AS INTEGER) = 0 THEN 'query' ELSE 'other' END,
                   1000000 - CAST("feeCut" AS HUGEINT), block_number, log_index, CAST(NULL AS BIGINT), 'horizon' FROM staking__delegation_fee_cut_set
),
cuts AS (
  SELECT c.sp,
         MAX(c.cut) FILTER (WHERE c.kind = 'indexing' AND c.rn = 1) AS indexing_reward_cut,
         MAX(c.cut) FILTER (WHERE c.kind = 'query'    AND c.rn = 1) AS query_fee_cut
  FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY sp, kind ORDER BY bn DESC, li DESC) AS rn FROM cut_events
        WHERE era = 'horizon' OR sp NOT IN (SELECT LOWER("serviceProvider") FROM staking__provision_created)) c
  GROUP BY 1
)
SELECT s.sp                                                     AS id,
       COALESCE(p.delegated_net, 0) + COALESCE(s.pool_rewards_added, 0) + COALESCE(th.delegated_thawing_tokens, 0) AS delegated_tokens,
       COALESCE(p.delegator_shares, 0)                          AS delegator_shares,
       COALESCE(th.delegated_thawing_tokens, 0)                 AS delegated_thawing_tokens,
       COALESCE(c.indexing_reward_cut, 1000000)                 AS indexing_reward_cut,
       COALESCE(c.query_fee_cut, 1000000)                       AS query_fee_cut
FROM stake s
LEFT JOIN pool        p  ON p.sp  = s.sp
LEFT JOIN thawing     th ON th.sp = s.sp
LEFT JOIN cuts        c  ON c.sp  = s.sp`;

const POSITIONS = (THAWS) => `
WITH events AS (
  SELECT LOWER(delegator) AS delegator, LOWER(indexer) AS indexer, 'in' AS kind, CAST(tokens AS HUGEINT) AS tok, CAST(shares AS HUGEINT) AS sh, CAST(block_timestamp AS BIGINT) AS ts, block_number * 100000 + log_index AS k FROM staking_legacy__stake_delegated
  UNION ALL SELECT LOWER(delegator), LOWER(indexer),            'out', CAST(tokens AS HUGEINT), CAST(shares AS HUGEINT), CAST(block_timestamp AS BIGINT), block_number * 100000 + log_index FROM staking_legacy__stake_delegated_locked
  UNION ALL SELECT LOWER(delegator), LOWER("serviceProvider"),  'in',  CAST(tokens AS HUGEINT), CAST(shares AS HUGEINT), CAST(block_timestamp AS BIGINT), block_number * 100000 + log_index FROM staking__tokens_delegated
  UNION ALL SELECT LOWER(delegator), LOWER("serviceProvider"),  'out', CAST(tokens AS HUGEINT), CAST(shares AS HUGEINT), CAST(block_timestamp AS BIGINT), block_number * 100000 + log_index FROM staking__tokens_undelegated
),
exact AS (
  SELECT delegator, indexer,
         SUM(CASE WHEN kind = 'in' THEN sh ELSE -sh END)      AS share_amount,
         SUM(CASE WHEN kind = 'in' THEN tok ELSE 0 END)       AS total_delegated_tokens,
         SUM(CASE WHEN kind = 'out' THEN tok ELSE 0 END)      AS total_undelegated_tokens,
         MIN(ts)                                              AS created_at,
         MAX(CASE WHEN kind = 'out' THEN ts END)              AS last_undelegated_at
  FROM events GROUP BY 1, 2
),
folded AS (
  SELECT delegator, indexer,
         list_reduce(
           list_prepend(
             {'kind': 'init', 't': 0.0::DOUBLE, 'sh': 0.0::DOUBLE, 'shares': 0.0::DOUBLE, 'rate': 0.0::DOUBLE, 'realized': 0.0::DOUBLE},
             list({'kind': kind, 't': CAST(tok AS DOUBLE), 'sh': CAST(sh AS DOUBLE), 'shares': 0.0::DOUBLE, 'rate': 0.0::DOUBLE, 'realized': 0.0::DOUBLE} ORDER BY k)
           ),
           lambda acc, e:
             CASE WHEN e.kind = 'in' THEN
               {'kind': 's', 't': 0.0::DOUBLE, 'sh': 0.0::DOUBLE,
                'shares': acc.shares + e.sh,
                'rate': CASE WHEN acc.shares + e.sh > 0 THEN (acc.rate * acc.shares + e.t) / (acc.shares + e.sh) ELSE acc.rate END,
                'realized': acc.realized}
             ELSE
               {'kind': 's', 't': 0.0::DOUBLE, 'sh': 0.0::DOUBLE,
                'shares': acc.shares - e.sh,
                'rate': acc.rate,
                'realized': acc.realized + (e.t - e.sh * acc.rate)}
             END
         ) AS st
  FROM events GROUP BY 1, 2
),
withdrawn AS (
  SELECT delegator, indexer, SUM(t) AS t FROM (
    SELECT LOWER(delegator) AS delegator, LOWER(indexer) AS indexer, CAST(tokens AS HUGEINT) AS t FROM staking__stake_delegated_withdrawn
    UNION ALL SELECT LOWER(delegator), LOWER("serviceProvider"), CAST(tokens AS HUGEINT) FROM staking__delegated_tokens_withdrawn
  ) GROUP BY 1, 2
),
thaw AS (${THAWS})
SELECT x.delegator || '-' || x.indexer                        AS id,
       x.delegator,
       x.indexer,
       x.share_amount,
       x.total_delegated_tokens,
       x.total_undelegated_tokens,
       f.st.rate                                               AS personal_exchange_rate,
       CAST(f.st.realized AS HUGEINT)                          AS realized_rewards,
       GREATEST(x.total_undelegated_tokens - COALESCE(w.t, 0), 0) AS locked_tokens,
       CASE WHEN x.total_undelegated_tokens - COALESCE(w.t, 0) > 0 THEN COALESCE(t.thawing_until, 0) ELSE 0 END AS locked_until,
       x.share_amount > 0                                      AS active,
       x.created_at,
       x.last_undelegated_at
FROM exact x
JOIN folded f ON f.delegator = x.delegator AND f.indexer = x.indexer
LEFT JOIN withdrawn w ON w.delegator = x.delegator AND w.indexer = x.indexer
LEFT JOIN thaw t ON t.delegator = x.delegator AND t.indexer = x.indexer`;

const DELEGATORS = () => `
SELECT delegator                                  AS id,
       SUM(total_delegated_tokens)                AS total_staked_tokens,
       SUM(total_undelegated_tokens)              AS total_unstaked_tokens,
       SUM(realized_rewards)                      AS total_realized_rewards,
       COUNT(*)                                   AS stakes_count,
       COUNT(*) FILTER (WHERE active)             AS active_stakes_count
FROM lodestar_delegator_positions GROUP BY 1`;

const CURATOR_SIGNALS = () => `
WITH pos AS (
  SELECT curator, dep, SUM(sig) AS signal, SUM(tin) AS signalled_tokens, SUM(tout) AS unsignalled_tokens, MAX(ts) AS last_signal_change FROM (
    SELECT LOWER(curator) AS curator, "subgraphDeploymentID" AS dep,  CAST(signal AS HUGEINT) AS sig, CAST(tokens AS HUGEINT) - CAST("curationTax" AS HUGEINT) AS tin, CAST(0 AS HUGEINT) AS tout, CAST(block_timestamp AS BIGINT) AS ts FROM curation__signalled
    UNION ALL SELECT LOWER(curator), "subgraphDeploymentID", -CAST(signal AS HUGEINT), 0, CAST(tokens AS HUGEINT), CAST(block_timestamp AS BIGINT) FROM curation__burned
  ) GROUP BY 1, 2
)
SELECT p.curator || '-' || CAST(p.dep AS VARCHAR)   AS id,
       p.curator,
       p.dep                                        AS subgraph_deployment,
       p.signalled_tokens,
       p.unsignalled_tokens,
       p.signal,
       p.last_signal_change,
       CAST(0 AS HUGEINT)                           AS realized_rewards
FROM pos p`;

const CURATORS = () => `
WITH positions AS (
  SELECT curator, position, SUM(sig) AS net_signal, SUM(tin) AS tokens_in, SUM(tout) AS tokens_out FROM (
    SELECT LOWER(curator) AS curator, 'v:' || CAST("subgraphDeploymentID" AS VARCHAR) AS position,  CAST(signal AS HUGEINT) AS sig, CAST(tokens AS HUGEINT) - CAST("curationTax" AS HUGEINT) AS tin, CAST(0 AS HUGEINT) AS tout FROM curation__signalled
    UNION ALL SELECT LOWER(curator), 'v:' || CAST("subgraphDeploymentID" AS VARCHAR), -CAST(signal AS HUGEINT), 0, CAST(tokens AS HUGEINT) FROM curation__burned
    UNION ALL SELECT LOWER(curator), 'n:' || CAST("subgraphID" AS VARCHAR),  CAST("nSignalCreated" AS HUGEINT), CAST("tokensDeposited" AS HUGEINT), 0 FROM gns__signal_minted
    UNION ALL SELECT LOWER(curator), 'n:' || CAST("subgraphID" AS VARCHAR), -CAST("nSignalBurnt" AS HUGEINT), 0, CAST("tokensReceived" AS HUGEINT) FROM gns__signal_burned
  ) GROUP BY 1, 2
)
SELECT curator                                            AS id,
       SUM(tokens_in)                                     AS total_signalled_tokens,
       SUM(tokens_out)                                    AS total_unsignalled_tokens,
       COUNT(*) FILTER (WHERE position LIKE 'v:%')                          AS signal_count,
       COUNT(*) FILTER (WHERE position LIKE 'v:%' AND net_signal > 0)       AS active_signal_count,
       CAST(0 AS HUGEINT)                                 AS realized_rewards
FROM positions GROUP BY 1`;


function address(a) {
  if (!/^0x[0-9a-fA-F]{40}$/.test(a)) throw new Error(`not an address: ${a}`);
  return a.toLowerCase();
}

/// The wallet's thaw requests, as the positions view's `thaw` CTE reads them.
const thaws = (wallet) => `SELECT LOWER(owner) AS delegator, LOWER("serviceProvider") AS indexer, MAX(CAST("thawingUntil" AS BIGINT)) AS thawing_until \
FROM staking__thaw_request_created WHERE CAST("requestType" AS INTEGER) IN (1, 2) AND LOWER(owner) = '${address(wallet)}' GROUP BY 1, 2`;

/// `lodestar_indexers`' pool and cut columns for `pools`, and no row for any other indexer.
const indexers = (pools) => {
  const ids = pools.length ? pools.map((i) => `'${address(i)}'`).join(", ") : "NULL";
  return `WITH lodestar_indexer_ledger AS (${LEDGER()}),
lodestar_indexers AS (${INDEXERS()})
SELECT id, delegated_tokens, delegator_shares, delegated_thawing_tokens, indexing_reward_cut, query_fee_cut \
FROM lodestar_indexers WHERE id IN (${ids})`;
};

/// kittiwake's `delegator_sql`.
export function delegatorSql(wallet) {
  return `WITH lodestar_delegator_positions AS (${POSITIONS(thaws(wallet))}),
lodestar_delegators AS (${DELEGATORS()})
SELECT id, CAST(total_staked_tokens AS VARCHAR) AS total_staked_tokens, \
CAST(total_unstaked_tokens AS VARCHAR) AS total_unstaked_tokens, \
CAST(total_realized_rewards AS VARCHAR) AS total_realized_rewards, stakes_count, active_stakes_count \
FROM lodestar_delegators WHERE id = '${address(wallet)}'`;
}

/// kittiwake's `delegator_stakes_sql`, with `pools` the indexers whose pool figures it computes.
export function delegatorStakesSql(wallet, pools) {
  return `WITH lodestar_delegator_positions AS (${POSITIONS(thaws(wallet))}),
lodestar_indexers AS (${indexers(pools)}),
s AS (SELECT p.id, p.indexer, p.total_delegated_tokens, p.share_amount, p.locked_tokens, p.locked_until, \
p.realized_rewards, p.total_undelegated_tokens, p.created_at, p.last_undelegated_at, p.active, \
CASE WHEN i.delegator_shares > 0 \
     THEN (p.share_amount // i.delegator_shares) * (i.delegated_tokens - i.delegated_thawing_tokens) \
          + CAST(CAST(p.share_amount % i.delegator_shares AS DOUBLE) * CAST(i.delegated_tokens - i.delegated_thawing_tokens AS DOUBLE) / CAST(i.delegator_shares AS DOUBLE) AS HUGEINT) \
     ELSE 0 END AS staked_tokens, \
CAST(CAST(p.share_amount AS DOUBLE) * p.personal_exchange_rate AS HUGEINT) AS cost_basis, \
i.delegated_tokens AS indexer_delegated_tokens, \
i.delegated_thawing_tokens AS indexer_delegated_thawing_tokens, i.delegator_shares AS indexer_delegator_shares, \
i.indexing_reward_cut, i.query_fee_cut \
FROM lodestar_delegator_positions p LEFT JOIN lodestar_indexers i ON i.id = p.indexer \
WHERE p.delegator = '${address(wallet)}') \
SELECT id, indexer, CAST(staked_tokens AS VARCHAR) AS staked_tokens, \
CAST(staked_tokens AS VARCHAR) AS current_tokens, CAST(cost_basis AS VARCHAR) AS cost_basis, \
CAST(total_delegated_tokens AS VARCHAR) AS total_delegated_tokens, CAST(share_amount AS VARCHAR) AS share_amount, \
CAST(locked_tokens AS VARCHAR) AS locked_tokens, locked_until, \
CAST(realized_rewards AS VARCHAR) AS realized_rewards, \
CAST(total_undelegated_tokens AS VARCHAR) AS unstaked_tokens, created_at, last_undelegated_at, active, \
CAST(indexer_delegated_tokens AS VARCHAR) AS indexer_delegated_tokens, \
CAST(indexer_delegated_thawing_tokens AS VARCHAR) AS indexer_delegated_thawing_tokens, \
CAST(indexer_delegator_shares AS VARCHAR) AS indexer_delegator_shares, \
indexing_reward_cut, query_fee_cut \
FROM s ORDER BY staked_tokens DESC, indexer LIMIT 200`;
}

/// kittiwake's `curator_sql`.
export function curatorSql(wallet) {
  return `WITH lodestar_curators AS (${CURATORS()})
SELECT id, CAST(total_signalled_tokens AS VARCHAR) AS total_signalled_tokens, \
CAST(total_unsignalled_tokens AS VARCHAR) AS total_unsignalled_tokens, \
CAST(realized_rewards AS VARCHAR) AS realized_rewards, signal_count, active_signal_count \
FROM lodestar_curators WHERE id = '${address(wallet)}'`;
}

/// kittiwake's `curator_signals_sql`, without the deployment-wide columns.
export function curatorSignalsSql(wallet) {
  return `WITH lodestar_curator_signals AS (${CURATOR_SIGNALS()})
SELECT id, subgraph_deployment, CAST(signalled_tokens AS VARCHAR) AS signalled_tokens, \
CAST(unsignalled_tokens AS VARCHAR) AS unsignalled_tokens, CAST(signal AS VARCHAR) AS signal, \
last_signal_change, CAST(realized_rewards AS VARCHAR) AS realized_rewards \
FROM lodestar_curator_signals s WHERE s.curator = '${address(wallet)}' \
ORDER BY s.signalled_tokens DESC, s.subgraph_deployment LIMIT 200`;
}

/// The four, by name.
export const statements = (wallet, pools) => ({
  delegator: delegatorSql(wallet),
  stakes: delegatorStakesSql(wallet, pools),
  curator: curatorSql(wallet),
  signals: curatorSignalsSql(wallet),
});

/// The indexers whose pool the stakes statement computes: a position with shares, or GRT thawing.
export function poolsOf(stakes) {
  const nonzero = (v) => v != null && !/^0*$/.test(String(v));
  const held = stakes.filter((s) => nonzero(s.share_amount) || nonzero(s.locked_tokens));
  return [...new Set(held.map((s) => String(s.indexer).toLowerCase()))].sort();
}

export const HORIZON_UPGRADE_BLOCK = 408_825_706;

/// The logs the four read for one wallet and its pools, as `nh_open`'s filters, public: Lodestar's
/// `walletFilters` and `poolFilters`.
export function filtersFor(wallet, pools) {
  const w = [address(wallet)];
  const horizon = (event, args) => ({ contract: "staking", event, from: HORIZON_UPGRADE_BLOCK, args });
  const out = [
    horizon("TokensDelegated", { delegator: w }),
    horizon("TokensUndelegated", { delegator: w }),
    horizon("DelegatedTokensWithdrawn", { delegator: w }),
    { contract: "staking", event: "StakeDelegatedWithdrawn", args: { delegator: w } },
    { contract: "staking_legacy", event: "StakeDelegated", args: { delegator: w } },
    { contract: "staking_legacy", event: "StakeDelegatedLocked", args: { delegator: w } },
    { contract: "curation", event: "Signalled", args: { curator: w } },
    { contract: "curation", event: "Burned", args: { curator: w } },
    { contract: "gns", event: "SignalMinted", args: { curator: w } },
    { contract: "gns", event: "SignalBurned", args: { curator: w } },
  ];
  // A wallet in no pool names the zero address, which selects no row: its statements still read the
  // pool tables, and a table no filter names is one this nest holds none of.
  const i = pools.length ? pools.map(address) : ["0x0000000000000000000000000000000000000000"];
  out.push(
    horizon("TokensDelegated", { serviceProvider: i }),
    horizon("TokensUndelegated", { serviceProvider: i }),
    horizon("DelegatedTokensWithdrawn", { serviceProvider: i }),
    horizon("ThawRequestCreated", { serviceProvider: i }),
    horizon("DelegationFeeCutSet", { serviceProvider: i }),
    horizon("ProvisionCreated", { serviceProvider: i }),
    horizon("TokensToDelegationPoolAdded", { serviceProvider: i }),
    horizon("DelegationSlashed", { serviceProvider: i }),
    { contract: "rewards", event: "HorizonRewardsAssigned", from: HORIZON_UPGRADE_BLOCK, args: { indexer: i } },
    { contract: "rewards", event: "RewardsAssigned", args: { indexer: i } },
    { contract: "staking_legacy", event: "StakeDelegated", args: { indexer: i } },
    { contract: "staking_legacy", event: "StakeDelegatedLocked", args: { indexer: i } },
    { contract: "staking_legacy", event: "DelegationParametersUpdated", args: { indexer: i } },
    { contract: "staking_legacy", event: "AllocationCreated", args: { indexer: i } },
    { contract: "staking_legacy", event: "RebateClaimed", args: { indexer: i } },
    { contract: "staking_legacy", event: "RebateCollected", args: { indexer: i } },
  );
  return out.map((f) => ({ ...f, privacy: "public" }));
}

/// RFC-0077 S7's seven addresses (§18).
export const WALLETS = [
  "0xcdb918200c36537196a4ef7e6ad1edf55b8cb2c3",
  "0x3a3526a7b467da9f0f4780a7691876550871bb3c",
  "0xe3af50914a7ed67cecfec8c037cc7899f4352ac2",
  "0xd7d7c0059ebfb6f5ca8641c90345dbb9263ad076",
  "0x96a30266b9f96f37677e47ef2d8a69ce0265e5b2",
  "0x6d64874a768607ed847fb7dd6f522b3dc52024bc",
  "0xacbdc195a79ea9766204ad7e082f1b36a32c0db5",
];

/// A statement over the whole network: every delegation's tokens, which no wallet's filters cover.
export const NETWORK_WIDE =
  "SELECT CAST(SUM(CAST(tokens AS HUGEINT)) AS VARCHAR) AS delegated, COUNT(*) AS delegations FROM staking__tokens_delegated";

/// A statement over a table none of `filtersFor`'s filters name.
export const UNFILTERED = "SELECT COUNT(*) AS epochs, MAX(block_number) AS last FROM epochs__epoch_run";
