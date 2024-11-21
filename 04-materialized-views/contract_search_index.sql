-- Drop View
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

-- Refresh materialized view every minute
SELECT cron.schedule(
  'refresh_contract_search_index',
  '* * * * *',  -- Every 1 min
  'REFRESH MATERIALIZED VIEW public.contract_search_index'
);