-- Drop all function variations
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT oid::regprocedure AS func_signature
        FROM pg_proc
        WHERE proname = 'get_enriched_royco_stats'
    LOOP
        EXECUTE 'DROP FUNCTION ' || r.func_signature || ' CASCADE';
    END LOOP;
END $$;

-- Drop existing data type
DROP TYPE IF EXISTS enriched_royco_stats_data_type;

-- Create new data type
CREATE TYPE enriched_royco_stats_data_type AS (
  chain_id NUMERIC,
  total_tvl NUMERIC,
  total_incentives NUMERIC,
  total_volume NUMERIC
);

-- Create new function
CREATE OR REPLACE FUNCTION get_enriched_royco_stats(
    custom_token_data JSONB DEFAULT '[]'::JSONB -- Input parameter for array of token data (token_id, decimals, price, fdv, total_supply) 
)
RETURNS enriched_royco_stats_data_type[] AS 
$$
DECLARE
    result_data enriched_royco_stats_data_type[];
    base_query TEXT;
    row enriched_royco_stats_data_type;
BEGIN
  base_query := '
    WITH 
    ranked_custom_data AS (
        SELECT 
            input.token_id,
            NULLIF(input.decimals, '''')::NUMERIC AS decimals,
            NULLIF(input.price, '''')::NUMERIC AS price,
            NULLIF(input.fdv, '''')::NUMERIC AS fdv,
            NULLIF(input.total_supply, '''')::NUMERIC AS total_supply,
            ROW_NUMBER() OVER (PARTITION BY input.token_id ORDER BY (SELECT NULL)) AS row_num
        FROM 
            jsonb_to_recordset($1) AS input(token_id TEXT, decimals TEXT, price TEXT, fdv TEXT, total_supply TEXT)
    ),
    aggregated_custom_data AS (
        SELECT 
            rcd.token_id,
            (SELECT r.decimals FROM ranked_custom_data r WHERE r.token_id = rcd.token_id AND r.decimals IS NOT NULL ORDER BY r.row_num DESC LIMIT 1) AS decimals,
            (SELECT r.price FROM ranked_custom_data r WHERE r.token_id = rcd.token_id AND r.price IS NOT NULL ORDER BY r.row_num DESC LIMIT 1) AS price,
            (SELECT r.fdv FROM ranked_custom_data r WHERE r.token_id = rcd.token_id AND r.fdv IS NOT NULL ORDER BY r.row_num DESC LIMIT 1) AS fdv,
            (SELECT r.total_supply FROM ranked_custom_data r WHERE r.token_id = rcd.token_id AND r.total_supply IS NOT NULL ORDER BY r.row_num DESC LIMIT 1) AS total_supply
        FROM ranked_custom_data rcd
        GROUP BY rcd.token_id
    ),
    token_quotes AS (
        -- Combine token quotes from the latest data and aggregated input data
        SELECT 
            COALESCE(acd.token_id, tql.token_id) AS token_id,
            COALESCE(acd.decimals, tql.decimals, 18) AS decimals,
            COALESCE(acd.price, tql.price, 0) AS price,
            COALESCE(acd.fdv, tql.fdv, 0) AS fdv,
            COALESCE(acd.total_supply, tql.total_supply, 0) AS total_supply
        FROM 
            token_quotes_latest tql
        FULL OUTER JOIN 
            aggregated_custom_data acd
            ON tql.token_id = acd.token_id
    ),
    raw_data AS (
      SELECT 
        em.id,
        rm.chain_id,
        rm.input_token_id,
        COALESCE(tq.price, 0) AS input_token_price,
        COALESCE(tq.decimals, 0) AS input_token_decimals,
        COALESCE(em.locked_quantity, 0) AS locked_quantity,
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
          COALESCE(rd.input_token_price, 0) * (COALESCE(rd.locked_quantity, 0) / (10 ^ COALESCE(rd.input_token_decimals, 0)))
        ) AS total_tvl
      FROM raw_data rd
      GROUP BY rd.chain_id
    ),
    incentives AS (
      -- Unnest incentive_ids and incentive_amounts, join with token_quotes to get price and decimals, then calculate the incentive total value
      SELECT
        rd.chain_id,
        SUM(incentive_amount * (COALESCE(tq.price, 0) / (10 ^ COALESCE(tq.decimals, 0)))) AS total_incentive_value
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
        SUM(volume_amount * (COALESCE(tq.price, 0) / (10 ^ COALESCE(tq.decimals, 0)))) AS total_volume_value
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
  ';

  FOR row IN EXECUTE base_query USING custom_token_data
  LOOP
    result_data := array_append(result_data, row);
  END LOOP;

  RETURN result_data;
END;
$$ LANGUAGE plpgsql;

-- Grant permission
GRANT EXECUTE ON FUNCTION get_enriched_royco_stats TO anon;

-- Sample query
SELECT *
FROM unnest((
    get_enriched_royco_stats(

    )
)) AS enriched_royco_stats;


