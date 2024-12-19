-- Drop Materialized View
DROP MATERIALIZED VIEW IF EXISTS public.token_quotes_latest;

-- Create Materialized View
CREATE MATERIALIZED VIEW public.token_quotes_latest AS
WITH 
t1 AS (
  SELECT 
    token_id,
    decimals,
    source || '-' || search_id AS match_key
  FROM
    token_index
  WHERE
    is_active = TRUE
),
t2 AS (
  SELECT 
    subquery.match_key,
    subquery.price,
    subquery.total_supply,
    subquery.fully_diluted_market_cap,
    subquery.last_updated
  FROM (
    SELECT 
      source || '-' || search_id AS match_key,
      price,
      total_supply,
      fully_diluted_market_cap,
      last_updated,
      ROW_NUMBER() OVER (PARTITION BY source || '-' || search_id ORDER BY last_updated DESC) as rn
    FROM 
      token_quotes_historical
  ) subquery
  WHERE subquery.rn = 1
),
base_points AS (
  SELECT
    id as token_id,
    decimals,
    total_supply
  FROM
    public.raw_points
),
unnested_points AS (
  -- Unnest the parallel arrays of token_ids and token_amounts
  -- Only consider valid, active IP offers
  SELECT 
    unnest(token_ids) as token_id,
    unnest(token_amounts) * (quantity_remaining::numeric / quantity::numeric) as token_amount
  FROM raw_offers
  WHERE 
    offer_side = 1
    AND (expiry = 0 OR expiry > EXTRACT(EPOCH FROM NOW())::numeric)
    AND is_cancelled = false
),
points_offer_supply AS (
  -- Sum up the token_amounts for each token_id
  SELECT 
    token_id,
    SUM(token_amount) as total_supply
  FROM unnested_points
  GROUP BY token_id
),
enriched_points AS (
  SELECT 
    bp.token_id,
    bp.decimals,
    0::NUMERIC AS price,
    (COALESCE(bp.total_supply, 0) + COALESCE(pos.total_supply, 0)::NUMERIC) / POWER(10, bp.decimals) AS total_supply,
    0::NUMERIC AS fdv
  FROM
    base_points bp
    LEFT JOIN
    points_offer_supply pos
    ON bp.token_id = pos.token_id
),
combined_results AS (
    SELECT  
        t1.token_id,
        t1.decimals,
        t2.price::NUMERIC,
        t2.total_supply::NUMERIC AS total_supply,
        t2.fully_diluted_market_cap::NUMERIC AS fdv,
        1 as source_priority -- Ordering priority
    FROM
        t1 
    LEFT JOIN 
        t2 
    ON 
        t1.match_key = t2.match_key
    WHERE 
        t2.match_key IS NOT NULL 
        AND t2.price IS NOT NULL
        AND t2.fully_diluted_market_cap IS NOT NULL
        AND t2.last_updated IS NOT NULL

    UNION ALL

    SELECT
        token_id,
        decimals,
        price,
        total_supply,
        fdv,
        2 as source_priority -- Ordering priority
    FROM
        enriched_points
),
final_results AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY token_id 
            ORDER BY source_priority ASC
        ) as rn
    FROM combined_results
)
SELECT 
    token_id,
    decimals,
    price,
    total_supply,
    fdv
FROM final_results
WHERE rn = 1;
  
-- Drop the existing scheduled job if it exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'token_quotes_latest_job') THEN
        PERFORM cron.unschedule('token_quotes_latest_job');
    END IF;
END
$$;

-- Refresh Materialized View every minute
SELECT cron.schedule(
    'token_quotes_latest_job', 
    '* * * * *', -- Every 1 min
    'REFRESH MATERIALIZED VIEW token_quotes_latest;'
);

-- Test manual call
-- REFRESH MATERIALIZED VIEW public.token_quotes_latest;