-- Drop all function variations
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT oid::regprocedure AS func_signature
        FROM pg_proc
        WHERE proname = 'get_enriched_markets'
    LOOP
        EXECUTE 'DROP FUNCTION ' || r.func_signature || ' CASCADE';
    END LOOP;
END $$;

-- Drop existing return type
DROP TYPE IF EXISTS enriched_markets_return_type;

-- Drop existing return type
DROP TYPE IF EXISTS enriched_markets_data_type;

-- Create new data type
CREATE TYPE enriched_markets_data_type AS (
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
  base_start_timestamps TEXT[],
  base_end_timestamps TEXT[],

  name TEXT,
  description TEXT,

  quantity_ap TEXT,
  quantity_ip TEXT,
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
  quantity_ap_usd NUMERIC,
  quantity_ip_usd NUMERIC,
  locked_quantity_usd NUMERIC,
  total_incentive_amounts_usd NUMERIC,
  
  annual_change_ratios NUMERIC[],
  annual_change_ratio NUMERIC
);

-- Create new return type
CREATE TYPE enriched_markets_return_type AS (
  count INT,
  data enriched_markets_data_type[]
);

-- Create new function
CREATE OR REPLACE FUNCTION get_enriched_markets(
    in_chain_id NUMERIC DEFAULT NULL,
    in_market_type INTEGER DEFAULT NULL,
    in_market_id TEXT DEFAULT NULL,
    custom_token_data JSONB DEFAULT '[]'::JSONB, -- Input parameter for array of token data (token_id, decimals, price, fdv, total_supply) 
    -- in_token_data JSONB DEFAULT '[]'::JSONB, -- Input parameter for array of token data (token_id, price, fdv, total_supply) 
    page_index INT DEFAULT 0,
    filters TEXT DEFAULT NULL,  -- New input parameter for additional filters
    sorting TEXT DEFAULT NULL,
    search_key TEXT DEFAULT NULL
)
RETURNS enriched_markets_return_type AS 
$$
DECLARE
    total_count INT;
    result_data enriched_markets_data_type[];
    base_query TEXT;
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

        COALESCE(ms.quantity_ap, 0) AS quantity_ap,
        COALESCE(ms.quantity_ip, 0) AS quantity_ip,
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

        -- Calculate quantity_ap_usd, quantity_ip_usd and locked_quantity_usd
        COALESCE((quantity_ap/ (10 ^ tq.decimals)) * tq.price, 0) AS quantity_ap_usd,
        COALESCE((quantity_ip/ (10 ^ tq.decimals)) * tq.price, 0) AS quantity_ip_usd,
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
              WHEN rm.market_type = 1 AND rm.quantity_ip_usd != 0 -- rm.lockup_time is always 0 in vault markets
                THEN (COALESCE(unnest_incentive_rates_usd, 0) / COALESCE(rm.quantity_ip_usd, 1)) * (365 * 24 * 60 * 60) -- Annualize the rate
              WHEN rm.lockup_time = 0 
                OR rm.quantity_ip_usd = 0 
                OR COALESCE(unnest_incentive_rates, 0) = 10 ^ 18 THEN 10 ^ 18 -- 10^18 refers N/D
              ELSE
                (COALESCE(unnest_incentive_rates_usd, 0) / COALESCE(rm.quantity_ip_usd, 1)) * (365 * 24 * 60 * 60) -- Annualize the rate
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
        to_char(rm.lockup_time, ''FM9999999999999999999999999999999999999999'') AS lockup_time,
        to_char(rm.frontend_fee, ''FM9999999999999999999999999999999999999999'') AS lockup_time,
        rm.reward_style,

        rm.transaction_hash,
        rm.block_number,
        rm.block_timestamp,
        rm.log_index,
        rm.underlying_vault_address,

        rm.base_incentive_ids,
        rm.base_incentive_amounts,
        array(
            SELECT to_char(col_value, ''FM9999999999999999999999999999999999999999'')
            FROM unnest(rm.start_timestamps) AS col_value
        ) AS base_start_timestamps,  
        array(
            SELECT to_char(col_value, ''FM9999999999999999999999999999999999999999'')
            FROM unnest(rm.end_timestamps) AS col_value
        ) AS base_end_timestamps,  
        
        rm.name,
        rm.description,

        to_char(rm.quantity_ap, ''FM9999999999999999999999999999999999999999'') AS quantity_ap,
        to_char(rm.quantity_ip, ''FM9999999999999999999999999999999999999999'') AS quantity_ip,
        to_char(rm.locked_quantity, ''FM9999999999999999999999999999999999999999'') AS locked_quantity,

        rm.incentive_ids,

        -- Convert token_amounts from NUMERIC[] to TEXT[]
        array(
            SELECT to_char(col_value, ''FM9999999999999999999999999999999999999999'')
            FROM unnest(rm.incentive_amounts) AS col_value
        ) AS incentive_amounts,  -- Conversion handled here

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
        rm.quantity_ap_usd,
        rm.quantity_ip_usd,
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

  -- Add search filter if search_key is provided
  IF search_key IS NOT NULL AND search_key <> '' THEN
    -- Replace spaces with '+' in search_key
    search_key := replace(search_key, ' ', '+');
  
    base_query := base_query || ' AND ' || 'id IN (SELECT id FROM market_search_index WHERE to_tsvector(search_id) @@ to_tsquery(''' || search_key || ':*''))';
  END IF;

  -- Step 3: Calculate total count after filters are applied
  EXECUTE base_query || ' ) SELECT COUNT(*) FROM enriched_data;'
  INTO total_count
  USING custom_token_data, in_chain_id, in_market_type, in_market_id;

  -- Step 4: Add sorting
  IF sorting IS NOT NULL AND sorting <> '' THEN
      base_query := base_query || ' ORDER BY ' || sorting;
  ELSE
      base_query := base_query || ' ORDER BY block_timestamp DESC';
  END IF;

  -- Step 4: Execute the paginated query to fetch the result data
  EXECUTE base_query || ' OFFSET $5 LIMIT 20 ) SELECT ARRAY_AGG(result.*) FROM enriched_data AS result;'
  INTO result_data
  USING custom_token_data, in_chain_id, in_market_type, in_market_id, page_index * 20;

  -- Step 5: Return both total count and data
  RETURN (total_count, result_data)::enriched_markets_return_type;

END;
$$ LANGUAGE plpgsql;


-- Sample Query 1
SELECT *
FROM unnest((
    get_enriched_markets(

    )
).data) AS enriched_market;

-- Sample Query 2
-- SELECT *
-- FROM unnest((
--     get_enriched_markets(
--       -- 11155111, -- in_chain_id, defaulting to NULL
--       -- 1, -- in_market_type, defaulting to NULL
--       -- '0x5802fb13468d943be6e4dca369f651e6e6088e92' -- in_market_id, defaulting to NULL
--       -- '[{
--       --       "token_id": "11155111-0x3c727dd5ea4c55b7b9a85ea2f287c641481400f7",
--       --       "price": 0.0001,
--       --       "fdv": 50000000,
--       --       "total_supply": 1000000
--       --     }]'::JSONB, -- in_token_data

--       --   0,
--       --   NULL,
--       --   'locked_quantity_usd DESC',
--       --   'dewscrptas'
--     )
-- ).data) AS enriched_market;