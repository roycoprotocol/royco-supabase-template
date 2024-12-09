-- Drop Materialized View
DROP MATERIALIZED VIEW IF EXISTS public.distinct_assets;

-- Create Materialized View
CREATE MATERIALIZED VIEW public.distinct_assets AS
WITH asset_list AS (
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
        asset_list a
    LEFT JOIN 
        token_index t ON a.id = t.token_id
    WHERE
        a.id IS NOT NULL AND
        t.token_id IS NOT NULL AND
        t.symbol IS NOT NULL
    GROUP BY
        t.symbol
)
SELECT
    assets.symbol,
    assets.ids
FROM
    assets;

-- Drop Materialized View
DROP MATERIALIZED VIEW IF EXISTS public.distinct_incentives;

-- Create Materialized View
CREATE MATERIALIZED VIEW public.distinct_incentives AS
WITH incentive_list AS (
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
        incentive_list a
    LEFT JOIN 
        token_index t ON a.id = t.token_id
    WHERE
        a.id IS NOT NULL AND
        t.token_id IS NOT NULL AND
        t.symbol IS NOT NULL
    GROUP BY
        t.symbol
)
SELECT
    incentives.symbol,
    incentives.ids
FROM
    incentives;

-- Drop the existing scheduled job if it exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'distinct_assets_job') THEN
        PERFORM cron.unschedule('distinct_assets_job');
    END IF;
END
$$;

-- Refresh materialized view every minute at 30th second
SELECT cron.schedule(
  'distinct_assets_job',
  '30 * * * * *',  -- At 30th second of every minute
  'REFRESH MATERIALIZED VIEW public.distinct_assets;'
);

-- Drop the existing scheduled job if it exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'distinct_incentives_job') THEN
        PERFORM cron.unschedule('distinct_incentives_job');
    END IF;
END
$$;

-- Refresh materialized view every minute at 30th second
SELECT cron.schedule(
  'distinct_incentives_job',
  '30 * * * * *',  -- At 30th second of every minute
  'REFRESH MATERIALIZED VIEW public.distinct_incentives;'
);

-- Test manual calls
-- REFRESH MATERIALIZED VIEW public.distinct_assets;
-- REFRESH MATERIALIZED VIEW public.distinct_incentives;
