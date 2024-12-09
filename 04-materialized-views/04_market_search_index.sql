-- Drop Materialized View
DROP MATERIALIZED VIEW IF EXISTS public.market_search_index;

-- Create Materialized View
CREATE MATERIALIZED VIEW public.market_search_index AS
WITH 
t1 AS (
  SELECT 
    rm.id,
    rm.chain_id,
    rm.market_id,
    rm.input_token_id,
    rm.market_type,  
    mu.name,
    mu.description,
    rm.incentives_offered_ids
  FROM 
    raw_markets rm
    LEFT JOIN
    market_userdata mu
    ON rm.id = mu.id
),
t2 AS (
  SELECT 
    rm.id,
    CONCAT(
      LOWER(rm.market_id), ' ',
      LOWER(rm.name), ' ', 
      LOWER(rm.description), ' ',
      CASE WHEN rm.market_type = 0 THEN 'recipe ' ELSE 'vault ' END,  -- Added conditional text
      LOWER((SELECT STRING_AGG(t.name || ' ' || t.symbol || ' ' || t.contract_address, ' ') 
            FROM token_index t 
            WHERE t.token_id = ANY(rm.incentives_offered_ids))), ' ', 
      LOWER((SELECT STRING_AGG(t.name || ' ' || t.symbol || ' ' || t.contract_address, ' ') 
            FROM token_index t 
            WHERE t.token_id = rm.input_token_id)), ' ', 
      LOWER((SELECT STRING_AGG(c.name || ' ' || c.symbol, ' ') FROM chains c WHERE c.chain_id = rm.chain_id))
    ) AS search_id
  FROM 
    t1 rm
)
SELECT * FROM t2;

-- Drop the existing scheduled job if it exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'market_search_index_job') THEN
        PERFORM cron.unschedule('market_search_index_job');
    END IF;
END
$$;

-- Refresh Materialized View every minute
SELECT cron.schedule(
  'market_search_index_job',
  '* * * * *',  -- Every 1 min
  'REFRESH MATERIALIZED VIEW public.market_search_index'
);

-- Test manual call
-- REFRESH MATERIALIZED VIEW public.market_search_index;

