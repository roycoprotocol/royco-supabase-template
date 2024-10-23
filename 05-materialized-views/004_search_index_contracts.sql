-- Drop existing view
DROP MATERIALIZED VIEW IF EXISTS public.search_index_contracts;

-- Create Materialized View
CREATE MATERIALIZED VIEW public.search_index_contracts AS 
SELECT 
  t.id,
  CONCAT(
    LOWER(t.address), ' ',
    LOWER(t.contract_name), ' ', 
    LOWER(t.label), ' ', 
    LOWER(t.type), ' ', 
    LOWER(t.proxy_type),
    LOWER(t.implementation_id)
  ) AS search_id
FROM 
  contracts t;

-- Refresh materialized view every minute
SELECT cron.schedule(
  'refresh_search_index_contracts',
  '*/1 * * * *',  -- Every 1 minute
  'REFRESH MATERIALIZED VIEW public.search_index_contracts'
);