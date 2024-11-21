-- Drop all function variations
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT oid::regprocedure AS func_signature
        FROM pg_proc
        WHERE proname = 'get_enriched_positions_vault'
    LOOP
        EXECUTE 'DROP FUNCTION ' || r.func_signature || ' CASCADE';
    END LOOP;
END $$;

-- Drop return type
DROP TYPE IF EXISTS enriched_positions_vault_return_type;

-- Drop data type
DROP TYPE IF EXISTS enriched_positions_vault_data_type;

-- Create data type
CREATE TYPE enriched_positions_vault_data_type AS (
  id TEXT,
  chain_id NUMERIC,
  offer_side INTEGER,
  market_id TEXT,
  annual_change_ratio NUMERIC,

  account_address TEXT,

  input_token_id TEXT,
  quantity TEXT,

  token_ids TEXT[],
  token_amounts TEXT[],

  input_token_price NUMERIC,
  input_token_fdv NUMERIC,
  input_token_total_supply NUMERIC,

  token_price_values NUMERIC[],
  token_fdv_values NUMERIC[],
  token_total_supply_values NUMERIC[],

  can_withdraw BOOLEAN,
  name TEXT,
  underlying_vault_address TEXT
);

-- Create return type
CREATE TYPE enriched_positions_vault_return_type AS (
  count INT,
  data enriched_positions_vault_data_type[]
);

-- Create function
CREATE OR REPLACE FUNCTION get_enriched_positions_vault(
    account_address TEXT,
    chain_id NUMERIC DEFAULT NULL,
    market_id TEXT DEFAULT NULL,
    custom_token_data JSONB DEFAULT '[]'::JSONB, -- Input parameter for array of token data (token_id, decimals, price, fdv, total_supply) 
    page_index INT DEFAULT 0,
    filters TEXT DEFAULT NULL, 
    sorting TEXT DEFAULT NULL
)
RETURNS enriched_positions_vault_return_type AS
$$
DECLARE
    total_count INT;
    result_data enriched_positions_vault_data_type[];
    base_query TEXT;
BEGIN
    -- Step 1: Construct the base query with the WITH clause properly enclosed
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

        refined_rab AS (
            SELECT
                rab.*,
                rm.incentives_offered_ids::TEXT[] AS incentives_received_ids,
                ARRAY(
                    SELECT ''0''
                    FROM unnest(rm.incentives_offered_ids) 
                )::NUMERIC[] AS incentives_received_amount
            FROM 
                public.raw_account_balances_vault rab
                LEFT JOIN public.raw_markets rm
                ON rab.chain_id::TEXT || ''_'' || rab.market_type || ''_'' || rab.market_id = rm.id
        ),

        raw_positions AS (
            SELECT
                id,
                chain_id,
                0 AS offer_side,
                market_id,
                account_address,
                input_token_id,
                quantity_given_amount AS quantity,
                incentives_received_ids AS token_ids,
                incentives_received_amount AS token_amounts
            FROM 
                refined_rab
            WHERE 
                account_address = $2
                AND ($3 IS NULL OR chain_id = $3)
            
            UNION ALL
            
            SELECT
                id,
                chain_id,
                1 AS offer_side,
                market_id,
                account_address,
                input_token_id,
                quantity_received_amount AS quantity,
                incentives_given_ids AS token_ids,
                incentives_given_amount AS token_amounts
            FROM 
                refined_rab
            WHERE 
                account_address = $2
                AND ($3 IS NULL OR chain_id = $3)
        ),

        enriched_positions AS (
            SELECT
              rro.id,
              rro.chain_id,
              rro.offer_side,
              rro.market_id,
            
              COALESCE(
                (
                    -- Get the enriched market
                    SELECT 
                        (unnest(data)).annual_change_ratio
                    FROM 
                        get_enriched_markets(
                            rro.chain_id, 
                            1, 
                            rro.market_id, 
                            $1
                        ) 
                    LIMIT 1
                ), 
              0) AS annual_change_ratio,

              rro.account_address,

              rro.input_token_id,
              to_char(rro.quantity, ''FM9999999999999999999999999999999999999999'') AS quantity,

              rro.token_ids,
              -- Convert token_amounts from NUMERIC[] to TEXT[]
              array(
                  SELECT to_char(col_value, ''FM9999999999999999999999999999999999999999'')
                  FROM unnest(rro.token_amounts) AS col_value
              ) AS token_amounts,  -- Conversion handled here

              rro.chain_id::TEXT  || ''_'' || ''1'' || ''_'' || rro.market_id AS market_key,

              -- Input Token Data
              tp.price AS input_token_price,
              tp.fdv AS input_token_fdv,
              tp.total_supply AS input_token_total_supply,

              -- Return token prices, fdv, and total_supply
              ARRAY(
                SELECT COALESCE(tp.price::NUMERIC, 0::NUMERIC)
                FROM UNNEST(rro.token_ids::TEXT[]) AS unnested_token_id -- Cast token_ids to TEXT[] if necessary
                LEFT JOIN token_quotes tp ON unnested_token_id = tp.token_id
              ) AS token_price_values,

              ARRAY(
                SELECT COALESCE(tp.fdv::NUMERIC, 0::NUMERIC)
                FROM UNNEST(rro.token_ids::TEXT[]) AS unnested_token_id -- Cast token_ids to TEXT[] if necessary
                LEFT JOIN token_quotes tp ON unnested_token_id = tp.token_id
              ) AS token_fdv_values,

              ARRAY(
                SELECT COALESCE(tp.total_supply::NUMERIC, 0::NUMERIC)
                FROM UNNEST(rro.token_ids::TEXT[]) AS unnested_token_id -- Cast token_ids to TEXT[] if necessary
                LEFT JOIN token_quotes tp ON unnested_token_id = tp.token_id
              ) AS token_total_supply_values,

              -- Can Withdraw Column
              CASE 
                  WHEN rro.quantity != 0 AND rro.offer_side = 0 THEN true
                  ELSE false
              END AS can_withdraw

            FROM 
              raw_positions rro
            LEFT JOIN 
              token_quotes tp ON rro.input_token_id = tp.token_id
            WHERE 
              rro.account_address = $2
              AND ($3 IS NULL OR rro.chain_id = $3) -- Optional chain_id
          ),
          enriched_data AS (
            SELECT 
                ro.id,
                ro.chain_id,
                ro.offer_side,
                ro.market_id,
                ro.annual_change_ratio,
            
                ro.account_address,

                ro.input_token_id,
                ro.quantity,

                ro.token_ids,
                ro.token_amounts,

                ro.input_token_price,
                ro.input_token_fdv,
                ro.input_token_total_supply,
                ro.token_price_values,
                ro.token_fdv_values,
                ro.token_total_supply_values,

                ro.can_withdraw,
                mu.name,
                rm.underlying_vault_address
            FROM 
              enriched_positions ro 
            LEFT JOIN 
              public.raw_markets rm
            ON
              ro.market_key = rm.id
            LEFT JOIN 
              public.market_userdata mu
            ON
              ro.market_key = mu.id
            WHERE 
              ro.id IS NOT NULL
          
    ';

    -- Step 2: Add market_id condition if it's not NULL
    IF market_id IS NOT NULL THEN
        base_query := base_query || ' AND ro.market_id = $4';
    END IF;

    -- Step 3: Add dynamic filters if provided
    IF filters IS NOT NULL AND filters <> '' THEN
        base_query := base_query || ' AND ' || filters;
    END IF;

    -- Step 4: Calculate total count after filters are applied
    EXECUTE base_query || ' ) SELECT COUNT(*) FROM enriched_data;'
    INTO total_count
    USING custom_token_data, account_address, chain_id, market_id;

    -- Step 5: Add sorting
    IF sorting IS NOT NULL AND sorting <> '' THEN
        base_query := base_query || ' ORDER BY ' || sorting;
    ELSE
        base_query := base_query || ' ORDER BY block_timestamp DESC';
    END IF;

    -- Step 6: Execute the paginated query to fetch the result data
    EXECUTE base_query || ' OFFSET $5 LIMIT 20 ) SELECT ARRAY_AGG(result.*) FROM enriched_data AS result;'
    INTO result_data
    USING custom_token_data, account_address, chain_id,  market_id, page_index * 20;

    -- Step 7: Return both total count and data
    RETURN (total_count, result_data)::enriched_positions_vault_return_type;
END;
$$ LANGUAGE plpgsql;

-- Grant permission
GRANT EXECUTE ON FUNCTION get_enriched_positions_vault TO anon;

-- Sample query
SELECT *
FROM unnest((
    get_enriched_positions_vault(
        '0x77777cc68b333a2256b436d675e8d257699aa667'
    )
).data) AS enriched_offer;
