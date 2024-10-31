-- Drop existing view
DROP MATERIALIZED VIEW IF EXISTS public.distinct_assets;

-- Create View
CREATE MATERIALIZED VIEW public.distinct_assets AS
WITH distinct_assets AS (
    SELECT DISTINCT
        input_token_id AS id
    FROM
        raw_markets
),
assets AS (
    SELECT
        t.symbol,
        ARRAY_AGG(t.token_id) AS ids
    FROM
        distinct_assets a
    LEFT JOIN 
        token_index t ON a.id = t.token_id
    WHERE
        t.symbol IS NOT NULL
    GROUP BY
        t.symbol
)
SELECT
    assets.symbol,
    assets.ids
FROM
    assets;

-- Drop existing view
DROP MATERIALIZED VIEW IF EXISTS public.distinct_incentives;

-- Create View
CREATE MATERIALIZED VIEW public.distinct_incentives AS
WITH distinct_incentives AS (
    SELECT DISTINCT
        UNNEST(incentive_ids) AS id
    FROM
        enriched_markets_stats
),
incentives AS (
    SELECT
        t.symbol,
        ARRAY_AGG(t.token_id) AS ids
    FROM
        distinct_incentives a
    LEFT JOIN 
        token_index t ON a.id = t.token_id
    GROUP BY
        t.symbol
)
SELECT
    incentives.symbol,
    incentives.ids
FROM
    incentives;

-- Refresh materialized view every minute
SELECT cron.schedule(
  'refresh_distinct_assets',
  '*/1 * * * *',  -- Every 1 minute
  'REFRESH MATERIALIZED VIEW public.distinct_assets'
);

-- Refresh materialized view every minute
SELECT cron.schedule(
  'refresh_distinct_incentives',
  '*/1 * * * *',  -- Every 1 minute
  'REFRESH MATERIALIZED VIEW public.distinct_incentives'
);