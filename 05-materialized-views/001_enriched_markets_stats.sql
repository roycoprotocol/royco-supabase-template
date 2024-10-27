-- Drop materialized view if exists
DROP MATERIALIZED VIEW IF EXISTS public.enriched_markets_stats;

-- Create materialized view
CREATE MATERIALIZED VIEW public.enriched_markets_stats AS
WITH 
base_raw_recipe_offers AS (
  SELECT 
    ro.chain_id::TEXT || '_' || ro.market_type::TEXT || '_' || ro.market_id::TEXT AS id,
    ro.quantity,
    ro.token_ids,
    ro.token_amounts,
    COALESCE(rm.lockup_time, 0) AS lockup_time
  FROM 
    raw_offers ro
    LEFT JOIN
    raw_markets rm
    ON ro.chain_id::TEXT || '_' || ro.market_type::TEXT || '_' || ro.market_id::TEXT = rm.id
  WHERE
    ro.market_type = 0 -- Only Recipe Markets
    AND ro.offer_side = 1 -- IP Offer Side
    AND ro.is_cancelled = FALSE -- Not Cancelled
    AND ro.expiry < EXTRACT(EPOCH FROM NOW()) -- Not Expired
),
summed_quantity AS (
  SELECT 
    ro.id,
    ro.lockup_time,
    SUM(ro.quantity) AS quantity
  FROM 
    base_raw_recipe_offers ro
  GROUP BY
    ro.id,
    ro.lockup_time
),
unnested_offers AS (
  SELECT
    ro.id,
    ro.lockup_time,
    token_id,
    token_amount
  FROM 
    base_raw_recipe_offers ro
     -- Unnest token_ids with ordinality
    CROSS JOIN LATERAL unnest(ro.token_ids) WITH ORDINALITY AS token_ids_with_ord(token_id, ordinality)
    -- Unnest token_amounts with ordinality
    CROSS JOIN LATERAL unnest(ro.token_amounts) WITH ORDINALITY AS token_amounts_with_ord(token_amount, ordinality)
    -- Ensure ordinality matches across token_id and token_amount arrays
    WHERE 
      token_ids_with_ord.ordinality = token_amounts_with_ord.ordinality
),
summed_offers AS (
  SELECT
    uo.id,
    uo.token_id,
    uo.lockup_time,
    SUM(uo.token_amount) AS token_amount
  FROM 
    unnested_offers uo
  GROUP BY 
    uo.id, 
    uo.token_id,
    uo.lockup_time
),
merged_offers AS (
  SELECT
    so.id,
    array_agg(so.token_id ORDER BY so.token_id) AS token_ids, -- Aggregate token_ids into an array
    array_agg(so.token_amount ORDER BY so.token_id) AS token_amounts, -- Aggregate token_amounts into an array
    array_agg(
      CASE 
        WHEN so.lockup_time = 0 THEN (10 ^ 18)
        ELSE (so.token_amount / so.lockup_time) 
      END
      ORDER BY so.token_id
    ) AS token_rates -- Calculate token rates and apply ORDER BY outside CASE
  FROM 
    summed_offers so
  GROUP BY 
    so.id
),
base_raw_positions_recipe AS (
  SELECT 
    rp.chain_id::TEXT || '_' || '0' || '_' || rp.market_id::TEXT AS id,
    quantity
  FROM 
    raw_positions_recipe rp
  WHERE
    rp.offer_side = 0 -- Only AP positions
    AND is_withdrawn = false -- Not withdrawn
),
recipe_locked_quantity AS (
  SELECT 
    rp.id,
    SUM(rp.quantity) AS locked_quantity
  FROM 
    base_raw_positions_recipe rp
  GROUP BY
    rp.id
),
enriched_recipe_market_data AS (
  SELECT 
    mo.id,
    sq.quantity,
    COALESCE(lq.locked_quantity, 0) AS locked_quantity,
    mo.token_ids AS incentive_ids,
    mo.token_amounts AS incentive_amounts,
    mo.token_rates AS incentive_rates
  FROM 
    merged_offers mo
  LEFT JOIN
    summed_quantity sq
  ON mo.id = sq.id
  LEFT JOIN
    recipe_locked_quantity lq
  ON mo.id = lq.id
),
base_raw_vault_markets AS (
  SELECT 
    rm.id,
    rm.quantity_offered AS quantity, -- for vault markets, this refers to assets
    rm.incentives_offered_ids AS token_ids,
    rm.incentives_offered_amount AS token_amounts,
    rm.incentives_rates AS token_rates,
    rm.start_timestamps,
    rm.end_timestamps
  FROM
    raw_markets rm
  WHERE
    rm.market_type = 1 -- Only Vault Markets
),
unnested_markets AS (
  SELECT
    rm.id,
    token_id,
    token_amount,
    token_rate,
    start_timestamp,
    end_timestamp
  FROM 
    base_raw_vault_markets rm
    -- Unnest token_ids with ordinality
    CROSS JOIN LATERAL unnest(rm.token_ids) WITH ORDINALITY AS token_ids_with_ord(token_id, ordinality)
    -- Unnest token_amounts with ordinality
    CROSS JOIN LATERAL unnest(rm.token_amounts) WITH ORDINALITY AS token_amounts_with_ord(token_amount, ordinality)
    -- Unnest token_rates with ordinality
    CROSS JOIN LATERAL unnest(rm.token_rates) WITH ORDINALITY AS token_rates_with_ord(token_rate, ordinality)
    -- Unnest start_timestamps with ordinality
    CROSS JOIN LATERAL unnest(rm.start_timestamps) WITH ORDINALITY AS start_timestamps_with_ord(start_timestamp, ordinality)
    -- Unnest end_timestamps with ordinality
    CROSS JOIN LATERAL unnest(rm.end_timestamps) WITH ORDINALITY AS end_timestamps_with_ord(end_timestamp, ordinality)
    -- Ensure that all ordinality values match to keep the elements in sync
    WHERE 
      token_ids_with_ord.ordinality = token_amounts_with_ord.ordinality
      AND token_ids_with_ord.ordinality = token_rates_with_ord.ordinality
      AND token_ids_with_ord.ordinality = start_timestamps_with_ord.ordinality
      AND token_ids_with_ord.ordinality = end_timestamps_with_ord.ordinality
),
filtered_markets AS (
  SELECT 
    rm.*
  FROM 
    unnested_markets rm
  WHERE
    rm.end_timestamp >= EXTRACT(EPOCH FROM NOW()) -- Compare end_timestamp with current time in seconds
),
merged_markets AS (
  SELECT
    so.id,
    array_agg(so.token_id ORDER BY so.token_id) AS token_ids, -- Aggregate token_ids into an array
    array_agg(so.token_amount ORDER BY so.token_id) AS token_amounts, -- Aggregate token_amounts into an array
    array_agg(so.token_rate ORDER BY so.token_id) AS token_rates -- Aggregate token_rates into an array
  FROM 
    filtered_markets so
  GROUP BY 
    so.id
),
enriched_vault_market_data AS (
  SELECT 
    mm.id,
    bm.quantity,
    bm.quantity AS locked_quantity,
    mm.token_ids AS incentive_ids,
    mm.token_amounts AS incentive_amounts, -- for vault markets, this represents rate
    mm.token_rates AS incentive_rates
  FROM 
    merged_markets mm
  LEFT JOIN
    base_raw_vault_markets bm
  ON mm.id = bm.id
),
combined_markets_data AS (
  SELECT * FROM enriched_recipe_market_data
  UNION
  SELECT * FROM enriched_vault_market_data
),
combined_markets_with_userdata AS (
    SELECT
        cmd.*,  -- Select all columns from combined_markets_data
        mu.name,  -- Add the 'name' column from market_userdata
        mu.description  -- Add the 'description' column from market_userdata
    FROM
        combined_markets_data cmd
    LEFT JOIN
        market_userdata mu ON cmd.id = mu.id  -- Join with market_userdata on 'id'
)
SELECT * FROM combined_markets_with_userdata;

-- Refresh materialized view every minute
SELECT cron.schedule(
  'refresh_enriched_markets_stats',
  '*/1 * * * *',  -- Every 1 minute
  'REFRESH MATERIALIZED VIEW public.enriched_markets_stats'
);