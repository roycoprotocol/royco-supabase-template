-- Drop materialized view if exists
DROP MATERIALIZED VIEW IF EXISTS public.enriched_markets_stats;

-- Create materialized view
CREATE MATERIALIZED VIEW public.enriched_markets_stats AS
WITH 
-- Recipe: Get all AP offers which are not cancelled, not expired and are valid
raw_offers_recipe_ap AS (
  SELECT 
    ro.chain_id::TEXT || '_' || ro.market_type::TEXT || '_' || ro.market_id::TEXT AS id, -- id for raw_markets
    ro.quantity,
    ro.quantity_remaining,
    ro.token_ids,
    ro.token_amounts,
    rm.lockup_time AS lockup_time
  FROM 
    raw_offers ro
    LEFT JOIN
    raw_markets rm
    ON ro.chain_id::TEXT || '_' || ro.market_type::TEXT || '_' || ro.market_id::TEXT = rm.id
  WHERE
    ro.market_type = 0 -- Only Recipe Markets
    AND ro.offer_side = 0 -- AP Offer Side
    AND ro.is_cancelled = FALSE -- Not Cancelled
    AND (ro.expiry = 0 OR ro.expiry > EXTRACT(EPOCH FROM NOW())) -- Not Expired
    AND ro.is_valid = TRUE
),
-- Recipe: Get all IP offers which are not cancelled and not expired (note: IP offers are always valid, so no need to check is_valid)
raw_offers_recipe_ip AS (
  SELECT 
    ro.chain_id::TEXT || '_' || ro.market_type::TEXT || '_' || ro.market_id::TEXT AS id, -- id for raw_markets
    ro.quantity,
    ro.quantity_remaining,
    ro.token_ids,
    ro.token_amounts,
    rm.lockup_time AS lockup_time
  FROM 
    raw_offers ro
    LEFT JOIN
    raw_markets rm
    ON ro.chain_id::TEXT || '_' || ro.market_type::TEXT || '_' || ro.market_id::TEXT = rm.id
  WHERE
    ro.market_type = 0 -- Only Recipe Markets
    AND ro.offer_side = 1 -- IP Offer Side
    AND ro.is_cancelled = FALSE -- Not Cancelled
    AND (ro.expiry = 0 OR ro.expiry > EXTRACT(EPOCH FROM NOW())) -- Not Expired
),

-- Recipe: Get total available quantity for ap
recipe_market_quantity_ap AS (
  SELECT 
    ro.id,
    ro.lockup_time,
    SUM(ro.quantity_remaining) AS quantity_ap
  FROM 
    raw_offers_recipe_ap ro
  GROUP BY
    ro.id,
    ro.lockup_time
),
-- Recipe: Get total available quantity for ip
recipe_market_quantity_ip AS (
  SELECT 
    ro.id,
    ro.lockup_time,
    SUM(ro.quantity_remaining) AS quantity_ip
  FROM 
    raw_offers_recipe_ip ro
  GROUP BY
    ro.id,
    ro.lockup_time
),

-- Recipe: Unnest all IP offers for incentive token and their amounts
raw_offers_recipe_ip_unnested AS (
  SELECT
    ro.id,
    ro.lockup_time,
    token_id,
    token_amount
  FROM 
    raw_offers_recipe_ip ro
     -- Unnest token_ids with ordinality
    CROSS JOIN LATERAL unnest(ro.token_ids) WITH ORDINALITY AS token_ids_with_ord(token_id, ordinality)
    -- Unnest token_amounts with ordinality
    CROSS JOIN LATERAL unnest(ro.token_amounts) WITH ORDINALITY AS token_amounts_with_ord(token_amount, ordinality)
    -- Ensure ordinality matches across token_id and token_amount arrays
    WHERE 
      token_ids_with_ord.ordinality = token_amounts_with_ord.ordinality
),
-- Recipe: Get incentive amounts for each incentive id
raw_offers_recipe_ip_incentives AS (
  SELECT
    uo.id,
    uo.lockup_time,
    uo.token_id,
    SUM(uo.token_amount) AS token_amount
  FROM 
    raw_offers_recipe_ip_unnested uo
  GROUP BY 
    uo.id, 
    uo.token_id,
    uo.lockup_time
),
-- Recipe: Get total incentive amounts for each incentive id
raw_offers_recipe_ip_incentives_total AS (
  SELECT
    so.id,
    array_agg(so.token_id ORDER BY so.token_id) AS token_ids, -- Aggregate token_ids into an array
    array_agg(so.token_amount ORDER BY so.token_id) AS token_amounts, -- Aggregate token_amounts into an array
    array_agg(
      CASE 
        WHEN so.lockup_time = 0 THEN (10 ^ 18) -- 10^18 refers to N/D
        ELSE (so.token_amount / so.lockup_time) 
      END
      ORDER BY so.token_id
    ) AS token_rates -- Calculate token rates and apply ORDER BY outside CASE
  FROM 
    raw_offers_recipe_ip_incentives so
  GROUP BY 
    so.id
),

-- Recipe: Get positions which are not withdrawn
raw_positions_recipe AS (
  SELECT 
    rp.chain_id::TEXT || '_' || '0' || '_' || rp.market_id::TEXT AS id,
    quantity
  FROM 
    raw_positions_recipe rp
  WHERE
    rp.offer_side = 0 -- Only AP positions
    AND is_withdrawn = false -- Not withdrawn
),
-- Recipe: Get total quantity locked
locked_quantity_recipe AS (
  SELECT 
    rp.id,
    SUM(rp.quantity) AS locked_quantity
  FROM 
    raw_positions_recipe rp
  GROUP BY
    rp.id
),

-- Recipe: Combine all data
enriched_recipe_market_data AS (
  SELECT 
    rm.id,
    COALESCE(mqap.quantity_ap, 0) AS quantity_ap,
    COALESCE(mqip.quantity_ip, 0) AS quantity_ip,
    COALESCE(lq.locked_quantity, 0) AS locked_quantity,
    COALESCE(mo.token_ids, ARRAY[]::TEXT[]) AS incentive_ids,
    COALESCE(mo.token_amounts, ARRAY[]::NUMERIC[]) AS incentive_amounts,
    COALESCE(mo.token_rates, ARRAY[]::NUMERIC[]) AS incentive_rates
  FROM 
    raw_markets rm
  LEFT JOIN
    raw_offers_recipe_ip_incentives_total mo
  ON rm.id = mo.id
  LEFT JOIN
    recipe_market_quantity_ap mqap
  ON rm.id = mqap.id
  LEFT JOIN
    recipe_market_quantity_ip mqip
  ON rm.id = mqip.id
  LEFT JOIN
    locked_quantity_recipe lq
  ON rm.id = lq.id
  WHERE
    rm.market_type = 0
),

-- Vault: Get base vault markets
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

-- Vault: Unnest markets for each incentive token
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

-- Vault: Get only those incentives whose rewards haven't ended yet
filtered_markets AS (
  SELECT 
    rm.*
  FROM 
    unnested_markets rm
  -- WHERE
  --   rm.end_timestamp >= EXTRACT(EPOCH FROM NOW()) -- Compare end_timestamp with current time in seconds
),

-- Vault: Merge markets
merged_markets AS (
  SELECT
    so.id,
    array_agg(so.token_id ORDER BY so.token_id) AS token_ids, -- Aggregate token_ids into an array
    -- array_agg(so.token_amount ORDER BY so.token_id) AS token_amounts, -- Aggregate token_amounts into an array
    -- array_agg(so.token_rate ORDER BY so.token_id) AS token_rates -- Aggregate token_rates into an array
    array_agg(
      CASE 
        WHEN so.end_timestamp <= EXTRACT(EPOCH FROM NOW()) THEN 0
        ELSE so.token_amount
      END 
      ORDER BY so.token_id
    ) AS token_amounts, -- Aggregate token_amounts into an array with condition
    array_agg(
      CASE 
        WHEN so.end_timestamp <= EXTRACT(EPOCH FROM NOW()) THEN 0
        ELSE so.token_rate
      END 
      ORDER BY so.token_id
    ) AS token_rates -- Aggregate token_rates into an array with condition
  FROM 
    filtered_markets so
  GROUP BY 
    so.id
),

-- Vault: Get all AP offers which are not cancelled, not expired and are valid
raw_offers_vault_ap AS (
  SELECT 
    ro.chain_id::TEXT || '_' || ro.market_type::TEXT || '_' || ro.market_id::TEXT AS id, -- id for raw_markets
    ro.quantity,
    ro.token_ids,
    ro.token_amounts,
    rm.lockup_time AS lockup_time
  FROM 
    raw_offers ro
    LEFT JOIN
    raw_markets rm
    ON ro.chain_id::TEXT || '_' || ro.market_type::TEXT || '_' || ro.market_id::TEXT = rm.id
  WHERE
    ro.market_type = 1 -- Only Vault Markets
    AND ro.offer_side = 0 -- AP Offer Side
    AND ro.is_cancelled = FALSE -- Not Cancelled
    AND (ro.expiry = 0 OR ro.expiry > EXTRACT(EPOCH FROM NOW())) -- Not Expired
    AND ro.is_valid = TRUE
),

-- Vault: Get total available quantity for ap
vault_market_quantity_ap AS (
  SELECT 
    ro.id,
    ro.lockup_time,
    SUM(ro.quantity) AS quantity_ap
  FROM 
    raw_offers_vault_ap ro
  GROUP BY
    ro.id,
    ro.lockup_time
),

-- Vault: Create enriched data for vaults
enriched_vault_market_data AS (
  SELECT 
    rm.id,
    COALESCE(mqap.quantity_ap, 0) AS quantity_ap,
    0 AS quantity_ip,
    -- COALESCE(rm.quantity, 0) AS quantity_ip,
    COALESCE(rm.quantity, 0) AS locked_quantity,
    COALESCE(mm.token_ids, ARRAY[]::TEXT[]) AS incentive_ids,
    COALESCE(mm.token_amounts, ARRAY[]::NUMERIC[]) AS incentive_amounts, -- for vault markets, this represents rate
    COALESCE(mm.token_rates, ARRAY[]::NUMERIC[]) AS incentive_rates
  FROM 
    base_raw_vault_markets rm
  LEFT JOIN
    merged_markets mm
  ON rm.id = mm.id
  LEFT JOIN
    vault_market_quantity_ap mqap
  ON rm.id = mqap.id
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

-- Drop the existing scheduled job if it exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'enriched_markets_stats_job') THEN
        PERFORM cron.unschedule('enriched_markets_stats_job');
    END IF;
END
$$;

-- Refresh materialized view every minute
SELECT cron.schedule(
  'enriched_markets_stats_job',
  '* * * * *',  -- Every 1 min
  'REFRESH MATERIALIZED VIEW public.enriched_markets_stats'
);

-- Test manual call
-- REFRESH MATERIALIZED VIEW public.enriched_markets_stats;
