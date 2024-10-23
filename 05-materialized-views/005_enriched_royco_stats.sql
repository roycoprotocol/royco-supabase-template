-- Drop existing view
DROP MATERIALIZED VIEW IF EXISTS public.enriched_royco_stats;

-- Create View
CREATE MATERIALIZED VIEW public.enriched_royco_stats AS
WITH
token_quotes AS (
  -- Integrate the input token data and combine it with existing token prices
  SELECT 
    token_id,
    price,
    fdv,
    total_supply,
    decimals 
  FROM 
    token_quotes_latest tql
),
raw_data AS (
  SELECT 
    em.id,
    rm.chain_id,
    rm.input_token_id,
    tq.price AS input_token_price,
    tq.decimals AS input_token_decimals,
    em.locked_quantity,
    em.incentive_ids,
    em.incentive_amounts,
    rm.volume_token_ids,
    rm.volume_amounts
  FROM 
    enriched_markets_stats em
    LEFT JOIN
    raw_markets rm
    ON em.id = rm.id
    LEFT JOIN 
    token_quotes tq
    ON rm.input_token_id = tq.token_id
),
tvl AS (
  SELECT 
    rd.chain_id,
    SUM(
      input_token_price * (locked_quantity / (10 ^ input_token_decimals))
    ) AS total_tvl
  FROM raw_data rd
  GROUP BY rd.chain_id
),
incentives AS (
  -- Unnest incentive_ids and incentive_amounts, join with token_quotes to get price and decimals, then calculate the incentive total value
  SELECT
    rd.chain_id,
    SUM(incentive_amount * (tq.price / (10 ^ tq.decimals))) AS total_incentive_value
  FROM 
    raw_data rd,
    UNNEST(rd.incentive_ids, rd.incentive_amounts) AS t(incentive_id, incentive_amount)
  LEFT JOIN 
    token_quotes tq
    ON t.incentive_id = tq.token_id
  GROUP BY rd.chain_id
),
volumes AS (
  -- Unnest volume_token_ids and volume_amounts, join with token_quotes to get price and decimals, then calculate the total volume value
  SELECT
    rd.chain_id,
    SUM(volume_amount * (tq.price / (10 ^ tq.decimals))) AS total_volume_value
  FROM 
    raw_data rd,
    UNNEST(rd.volume_token_ids, rd.volume_amounts) AS t(volume_token_id, volume_amount)
  LEFT JOIN 
    token_quotes tq
    ON t.volume_token_id = tq.token_id
  GROUP BY rd.chain_id
),
enriched_data AS (
  SELECT 
    t.chain_id,
    COALESCE(t.total_tvl, 0) AS total_tvl,
    COALESCE(i.total_incentive_value, 0) AS total_incentives,
    COALESCE(v.total_volume_value, 0) AS total_volume
  FROM 
    tvl t
    LEFT JOIN incentives i ON t.chain_id = i.chain_id
    LEFT JOIN volumes v ON t.chain_id = v.chain_id
  GROUP BY 
    t.chain_id, t.total_tvl, i.total_incentive_value, v.total_volume_value
)
SELECT * FROM enriched_data;

-- Refresh materialized view every minute
SELECT cron.schedule(
  'refresh_enriched_royco_stats',
  '*/1 * * * *',  -- Every 1 minute
  'REFRESH MATERIALIZED VIEW public.enriched_royco_stats'
);
