-- Drop Materialized View
DROP MATERIALIZED VIEW IF EXISTS public.contract_search_index;

-- Create Materialized View
CREATE MATERIALIZED VIEW public.contract_search_index AS 
SELECT 
  t.id,
  CONCAT(
    LOWER(t.address), ' ',
    LOWER(t.contract_name), ' ', 
    LOWER(t.label), ' ', 
    LOWER(t.type), ' ', 
    LOWER(t.proxy_type), ' ',
    LOWER(t.implementation_id)
  ) AS search_id
FROM 
  contracts t;

-- Drop the existing scheduled job if it exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'contract_search_index_job') THEN
        PERFORM cron.unschedule('contract_search_index_job');
    END IF;
END
$$;

-- Refresh materialized view every minute
SELECT cron.schedule(
  'contract_search_index_job',
  '* * * * *',  -- Every 1 min
  'REFRESH MATERIALIZED VIEW public.contract_search_index'
);