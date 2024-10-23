-- Drop existing function
DROP FUNCTION IF EXISTS get_enriched_markets CASCADE;

-- Drop existing return type
DROP TYPE IF EXISTS enriched_markets_return_type;

-- Drop existing return type
DROP TYPE IF EXISTS enriched_market_data_type;

-- Create new data type
CREATE TYPE enriched_market_data_type AS (
  id TEXT,
  chain_id NUMERIC,
  market_type INTEGER,
  market_id TEXT,
  creator TEXT,
  input_token_id TEXT,
  lockup_time TEXT,
  frontend_fee TEXT,
  reward_style INTEGER,

  transaction_hash TEXT,
  block_number NUMERIC,
  block_timestamp NUMERIC,
  log_index NUMERIC,
  underlying_vault_address TEXT,

  base_incentive_ids TEXT[],
  base_incentive_amounts TEXT[],

  start_timestamps TEXT[],
  end_timestamps TEXT[],
  name TEXT,
  description TEXT,

  quantity TEXT,
  locked_quantity TEXT,
  incentive_ids TEXT[],
  incentive_amounts TEXT[],
  incentive_rates TEXT[],

  input_token_price NUMERIC,
  input_token_fdv NUMERIC,
  input_token_total_supply NUMERIC,

  incentive_token_price_values NUMERIC[], -- only incentives offered
  incentive_token_total_supply_values NUMERIC[], -- only incentives offered
  incentive_token_fdv_values NUMERIC[], -- only incentives offered

  incentive_amounts_usd NUMERIC[],
  incentive_rates_usd NUMERIC[],
  quantity_usd NUMERIC,
  locked_quantity_usd NUMERIC,
  total_incentive_amounts_usd NUMERIC,
  
  annual_change_ratios NUMERIC[],
  annual_change_ratio NUMERIC
);

-- Create new return type
CREATE TYPE enriched_markets_return_type AS (
  count INT,
  data enriched_market_data_type[]
);

-- Create new function
CREATE OR REPLACE FUNCTION get_enriched_markets(
    in_chain_id NUMERIC DEFAULT NULL,
    in_market_type INTEGER DEFAULT NULL,
    in_market_id TEXT DEFAULT NULL,
    in_token_data JSONB DEFAULT '[]'::JSONB, -- Input parameter for array of token data (token_id, price, fdv, total_supply) 
    page_index INT DEFAULT 0,
    filters TEXT DEFAULT NULL,  -- New input parameter for additional filters
    sorting TEXT DEFAULT NULL,
    search_key TEXT DEFAULT NULL
)
RETURNS enriched_markets_return_type AS 
$$
DECLARE
    total_count INT;
    result_data enriched_market_data_type[];
    base_query TEXT;
BEGIN
  base_query := '
    WITH 
    token_quotes AS (
        -- Integrate the input token data and combine it with existing token prices
        SELECT 
          COALESCE(input.token_id, tql.token_id) AS token_id,
          COALESCE(input.price::NUMERIC, tql.price) AS price,
          COALESCE(input.fdv::NUMERIC, tql.fdv) AS fdv,
          COALESCE(input.total_supply::NUMERIC, tql.total_supply) AS total_supply,
          COALESCE(tql.decimals, 18) AS decimals -- Default decimals if missing
        FROM 
          token_quotes_latest tql
        LEFT JOIN 
          jsonb_to_recordset($1) AS input(token_id TEXT, price NUMERIC, fdv NUMERIC, total_supply NUMERIC)
          ON tql.token_id = input.token_id
    ),
    base_raw_markets AS (
      SELECT 
        rm.id,
        rm.chain_id,
        rm.market_type,
        rm.market_id,
        rm.creator,
        rm.input_token_id,
        rm.lockup_time,
        rm.frontend_fee,
        rm.reward_style,
        rm.transaction_hash,
        rm.block_number,
        rm.block_timestamp,
        rm.log_index,
        rm.underlying_vault_address,
        rm.incentives_offered_ids AS base_incentive_ids,
        rm.incentives_offered_amount AS base_incentive_amounts,
        rm.start_timestamps,
        rm.end_timestamps,

        COALESCE(mu.name, ''Unknown market'') AS name,
        COALESCE(mu.description, ''No description available'') AS description,

        COALESCE(ms.quantity, 0) AS quantity,
        COALESCE(ms.locked_quantity, 0) AS locked_quantity,
        COALESCE(ms.incentive_ids, ARRAY[]::TEXT[]) AS incentive_ids, -- Default empty array of type TEXT[]
        COALESCE(ms.incentive_amounts, ARRAY[]::NUMERIC[]) AS incentive_amounts, -- Default empty array of type NUMERIC[]
        COALESCE(ms.incentive_rates, ARRAY[]::NUMERIC[]) AS incentive_rates -- Default empty array of type NUMERIC[]

      FROM
        raw_markets rm
        LEFT JOIN
        market_userdata mu
        ON rm.id = mu.id
        LEFT JOIN
        enriched_markets_stats ms
        ON rm.id = ms.id
        
      
      WHERE
        rm.id IS NOT NULL
        AND ($2 IS NULL OR rm.chain_id = $2) -- Optional chain_id
        AND ($3 IS NULL OR rm.market_type = $3) -- Optional market_type
        AND ($4 IS NULL OR rm.market_id = $4) -- Optional market_id
    ),
    enriched_raw_markets AS (
      SELECT 
        rm.*,

        -- Input Token Data
        COALESCE(tq.price, 0) AS input_token_price,
        COALESCE(tq.fdv, 0) AS input_token_fdv,
        COALESCE(tq.total_supply, 0) AS input_token_total_supply,

        COALESCE(incentives_data.incentive_token_price_values, ''{}'') AS incentive_token_price_values,
        COALESCE(incentives_data.incentive_token_total_supply_values, ''{}'') AS incentive_token_total_supply_values,
        COALESCE(incentives_data.incentive_token_fdv_values, ''{}'') AS incentive_token_fdv_values,
        COALESCE(incentives_data.incentive_amounts_usd, ''{}'') AS incentive_amounts_usd,
        COALESCE(incentives_data.incentive_rates_usd, ''{}'') AS incentive_rates_usd,

        -- Calculate quantity_usd and locked_quantity_usd
        COALESCE((quantity / (10 ^ tq.decimals)) * tq.price, 0) AS quantity_usd,
        COALESCE((locked_quantity / (10 ^ tq.decimals)) * tq.price, 0) AS locked_quantity_usd
      FROM 
          base_raw_markets rm
        LEFT JOIN 
          token_quotes tq ON rm.input_token_id = tq.token_id

        -- Precompute and join incentives data
        LEFT JOIN LATERAL (
        SELECT
            array_agg(
                COALESCE(tq1.price, 0) 
                ORDER BY rm_incentives.ord  -- Ensure correct order by ordinality
            ) AS incentive_token_price_values,
            array_agg(
                COALESCE(tq1.total_supply, 0) 
                ORDER BY rm_incentives.ord  -- Ensure correct order by ordinality
            ) AS incentive_token_total_supply_values,
            array_agg(
                COALESCE(tq1.fdv, 1) 
                ORDER BY rm_incentives.ord  -- Ensure correct order by ordinality
            ) AS incentive_token_fdv_values,
            array_agg(
                COALESCE(((tq1.price * rm_incentives_amount.val) / (10 ^ tq1.decimals)), 0) 
                ORDER BY rm_incentives.ord  -- Ensure correct order by ordinality
            ) AS incentive_amounts_usd,
            array_agg(
                COALESCE(((tq1.price * rm_incentives_rate.val) / (10 ^ tq1.decimals)), 0) 
                ORDER BY rm_incentives.ord  -- Ensure correct order by ordinality
            ) AS incentive_rates_usd
        FROM 
            unnest(rm.incentive_ids) WITH ORDINALITY AS rm_incentives(id, ord)
        LEFT JOIN 
            token_quotes tq1 ON rm_incentives.id = tq1.token_id
        CROSS JOIN LATERAL
            unnest(rm.incentive_amounts) WITH ORDINALITY AS rm_incentives_amount(val, ord_amt)
        LEFT JOIN 
            token_quotes tq2 ON rm_incentives.id = tq2.token_id
        CROSS JOIN LATERAL
            unnest(rm.incentive_rates) WITH ORDINALITY AS rm_incentives_rate(val, ord_rate)
        LEFT JOIN 
            token_quotes tq3 ON rm_incentives.id = tq3.token_id
        WHERE 
            rm_incentives.ord = ord_amt  -- Ensures alignment between IDs and amounts
            AND rm_incentives.ord = ord_rate
        ) AS incentives_data ON true

    ),
    pre_enriched_data AS (
      SELECT
        rm.*,

        -- Sum the values inside the incentive_amounts_usd array
        COALESCE((
          SELECT SUM(val)
          FROM UNNEST(rm.incentive_amounts_usd) AS val
        ), 0) AS total_incentive_amounts_usd,  -- Default to 0 if array is empty or NULL

        -- Calculate annual_change_ratio for each incentive, using COALESCE to handle NULL values and empty arrays
        COALESCE((
          SELECT ARRAY_AGG(
            CASE 
              WHEN rm.lockup_time = 0 
                OR rm.quantity_usd = 0 
                OR COALESCE(unnest_incentive_rates, 0) = 10 ^ 18 THEN 10 ^ 18
              ELSE
                (COALESCE(unnest_incentive_rates_usd, 0) / COALESCE(rm.quantity_usd, 1)) * (365 * 24 * 60 * 60) -- Annualize the rate
            END
          )
          FROM UNNEST(rm.incentive_rates) AS unnest_incentive_rates,
              UNNEST(rm.incentive_rates_usd) AS unnest_incentive_rates_usd
        ), ARRAY[]::NUMERIC[]) AS annual_change_ratios -- Default to an empty NUMERIC array if result is NULL

      FROM 
        enriched_raw_markets rm
    ),
    final_enriched_data AS (
      SELECT
        rm.id,
        rm.chain_id,
        rm.market_type,
        rm.market_id,
        rm.creator,
        rm.input_token_id,
        -- rm.lockup_time,
        to_char(rm.lockup_time, ''FM9999999999999999999999999999999999999999'') AS lockup_time,
        -- rm.frontend_fee,
        to_char(rm.frontend_fee, ''FM9999999999999999999999999999999999999999'') AS lockup_time,
        rm.reward_style,
        rm.transaction_hash,
        rm.block_number,
        rm.block_timestamp,
        rm.log_index,
        rm.underlying_vault_address,
        rm.base_incentive_ids,
        rm.base_incentive_amounts,

        -- rm.start_timestamps,
        -- Convert token_amounts from NUMERIC[] to TEXT[]
        array(
            SELECT to_char(col_value, ''FM9999999999999999999999999999999999999999'')
            FROM unnest(rm.start_timestamps) AS col_value
        ) AS start_timestamps,  -- Conversion handled here

        -- rm.end_timestamps,
        -- Convert token_amounts from NUMERIC[] to TEXT[]
        array(
            SELECT to_char(col_value, ''FM9999999999999999999999999999999999999999'')
            FROM unnest(rm.end_timestamps) AS col_value
        ) AS end_timestamps,  -- Conversion handled here
        
        rm.name,
        rm.description,

        -- rm.quantity,
        to_char(rm.quantity, ''FM9999999999999999999999999999999999999999'') AS quantity,
        -- rm.locked_quantity,
        to_char(rm.locked_quantity, ''FM9999999999999999999999999999999999999999'') AS locked_quantity,

        rm.incentive_ids,

        -- rm.incentive_amounts,
        -- Convert token_amounts from NUMERIC[] to TEXT[]
        array(
            SELECT to_char(col_value, ''FM9999999999999999999999999999999999999999'')
            FROM unnest(rm.incentive_amounts) AS col_value
        ) AS incentive_amounts,  -- Conversion handled here

        -- rm.incentive_rates,
        -- Convert token_amounts from NUMERIC[] to TEXT[]
        array(
            SELECT to_char(col_value, ''FM9999999999999999999999999999999999999999'')
            FROM unnest(rm.incentive_rates) AS col_value
        ) AS incentive_rates,  -- Conversion handled here

        rm.input_token_price,
        rm.input_token_fdv,
        rm.input_token_total_supply,

        rm.incentive_token_price_values, -- only incentives offered
        rm.incentive_token_total_supply_values, -- only incentives offered
        rm.incentive_token_fdv_values, -- only incentives offered

        rm.incentive_amounts_usd,
        rm.incentive_rates_usd,
        rm.quantity_usd,
        rm.locked_quantity_usd,

        rm.total_incentive_amounts_usd,
        rm.annual_change_ratios,

        -- Calculate total_annual_change_ratio: if any value is 10^18, return 10^18, else sum the values or return 0 for empty array
        CASE 
          WHEN EXISTS (
            SELECT 1 
            FROM UNNEST(rm.annual_change_ratios) AS val
            WHERE val = 10 ^ 18
          ) THEN 10 ^ 18
          ELSE COALESCE(
            (SELECT SUM(val) FROM UNNEST(rm.annual_change_ratios) AS val), 0
          )
        END AS annual_change_ratio

      FROM 
        pre_enriched_data rm
      ),
      enriched_data AS (
        SELECT * FROM final_enriched_data 
        WHERE id IS NOT NULL
      
  ';

  -- Step 2: Add dynamic filters if provided
  IF filters IS NOT NULL AND filters <> '' THEN
      base_query := base_query || ' AND ' || filters;
  END IF;

  -- Step 3: Add search filter if search_key is provided
  IF search_key IS NOT NULL AND search_key <> '' THEN
    -- Replace spaces with '+' in search_key
    search_key := replace(search_key, ' ', '+');
  
    base_query := base_query || ' AND ' || 'id IN (SELECT id FROM market_search_index WHERE to_tsvector(search_id) @@ to_tsquery(''' || search_key || ':*''))';
  END IF;

  -- Step 4: Calculate total count after filters are applied
  EXECUTE base_query || ' ) SELECT COUNT(*) FROM enriched_data;'
  INTO total_count
  USING in_token_data, in_chain_id, in_market_type, in_market_id;

  -- Step 5: Add sorting
  IF sorting IS NOT NULL AND sorting <> '' THEN
      base_query := base_query || ' ORDER BY ' || sorting;
  ELSE
      base_query := base_query || ' ORDER BY block_timestamp DESC';
  END IF;

  -- Step 6: Execute the paginated query to fetch the result data
  EXECUTE base_query || ' OFFSET $5 LIMIT 20 ) SELECT ARRAY_AGG(result.*) FROM enriched_data AS result;'
  INTO result_data
  USING in_token_data, in_chain_id, in_market_type, in_market_id, page_index * 20;

  -- Step 7: Return both total count and data
  RETURN (total_count, result_data)::enriched_markets_return_type;

END;
$$ LANGUAGE plpgsql;

-- Grant permission 
GRANT EXECUTE ON FUNCTION get_enriched_markets TO anon;

-- Sample Query: Change parameters based on your table data
-- SELECT *
-- FROM unnest((
--     get_enriched_markets(
--       11155111, -- in_chain_id, defaulting to NULL
--       0, -- in_market_type, defaulting to NULL
--       NULL, -- in_market_id, defaulting to NULL
--       '[{
--             "token_id": "11155111-0x3c727dd5ea4c55b7b9a85ea2f287c641481400f7",
--             "price": 0.0001,
--             "fdv": 50000000,
--             "total_supply": 1000000
--           }]'::JSONB, -- in_token_data

--         0,
--         NULL,
--         'locked_quantity_usd DESC',
--         'dewscrptas'
--     )
-- ).data) AS enriched_market;



