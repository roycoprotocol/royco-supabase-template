-- Drop all function variations
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT oid::regprocedure AS func_signature
        FROM pg_proc
        WHERE proname = 'get_enriched_offers'
    LOOP
        EXECUTE 'DROP FUNCTION ' || r.func_signature || ' CASCADE';
    END LOOP;
END $$;

-- Drop return type
DROP TYPE IF EXISTS enriched_offers_return_type;

-- Drop data type
DROP TYPE IF EXISTS enriched_offer_data_type;

-- Create data type
CREATE TYPE enriched_offer_data_type AS (
  id TEXT,
  chain_id NUMERIC,
  market_type INTEGER,
  offer_side INTEGER,
  offer_id TEXT,
  market_id TEXT,

  creator TEXT,
  funding_vault TEXT,

  input_token_id TEXT,
  quantity TEXT,
  quantity_remaining TEXT,
  expiry TEXT,

  token_ids TEXT[],
  token_amounts TEXT[],
  protocol_fee_amounts TEXT[],
  frontend_fee_amounts TEXT[],
  is_cancelled BOOLEAN,

  transaction_hash TEXT,
  block_timestamp NUMERIC,
  can_be_filled BOOLEAN,

  input_token_price NUMERIC,
  input_token_fdv NUMERIC,
  input_token_total_supply NUMERIC,
  
  token_price_values NUMERIC[],
  token_fdv_values NUMERIC[],
  token_total_supply_values NUMERIC[],
  quantity_value_usd NUMERIC,
  incentive_value_usd NUMERIC,

  name TEXT,
  lockup_time TEXT,
  reward_style NUMERIC,
  change_ratio NUMERIC,
  annual_change_ratio NUMERIC
);

-- Create return type
CREATE TYPE enriched_offers_return_type AS (
  count INT,
  data enriched_offer_data_type[]
);

-- Create function
CREATE OR REPLACE FUNCTION get_enriched_offers(
    chain_id NUMERIC DEFAULT NULL,
    market_type INTEGER DEFAULT NULL,
    market_id TEXT DEFAULT NULL,
    creator TEXT DEFAULT NULL,   
    can_be_filled BOOLEAN DEFAULT NULL,
    custom_token_data JSONB DEFAULT '[]'::JSONB, -- Input parameter for array of token data (token_id, decimals, price, fdv, total_supply) 
    page_index INT DEFAULT 0,
    filters TEXT DEFAULT NULL,  
    sorting TEXT DEFAULT NULL
)
RETURNS enriched_offers_return_type AS
$$
DECLARE
    total_count INT;
    result_data enriched_offer_data_type[];
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

        raw_base_offers AS (
          SELECT 
            ro.id,
            ro.chain_id,
            ro.market_type,
            ro.offer_side,
            ro.offer_id,
            ro.market_id,
            ro.creator,
            ro.funding_vault,
            ro.input_token_id,
            ro.quantity,
            ro.quantity_remaining,
            ro.expiry,
            ro.token_ids,
            ro.token_amounts,
            ro.protocol_fee_amounts,
            ro.frontend_fee_amounts,
            ro.is_cancelled,
            ro.transaction_hash,
            ro.block_timestamp,
            CASE 
                WHEN ro.is_valid = false OR ro.is_cancelled = true OR ro.quantity_remaining = 0
                    THEN false
                WHEN ro.expiry != 0 AND ro.expiry <= EXTRACT(EPOCH FROM NOW())
                    THEN false
                ELSE 
                    true
            END AS can_be_filled
          FROM 
            raw_offers ro
          WHERE
            ro.id IS NOT NULL
            AND ($2 IS NULL OR ro.chain_id = $2) -- Optional chain_id
            AND ($3 IS NULL OR ro.market_type = $3) -- Optional market_type
            AND ($4 IS NULL OR ro.market_id = $4) -- Optional market_id
            AND ($5 IS NULL OR ro.creator = $5) -- Optional creator
          ORDER BY
            block_timestamp DESC
        ),

        enriched_offers AS (
            SELECT
              rro.id,
              rro.chain_id,
              rro.market_type,
              rro.offer_side,
              rro.offer_id,
              rro.market_id,
              rro.creator,
              rro.funding_vault,
              rro.input_token_id,
              rro.quantity,
              rro.quantity_remaining,
              rro.expiry,
              rro.token_ids,
              rro.token_amounts,
              rro.protocol_fee_amounts,
              rro.frontend_fee_amounts,
              rro.is_cancelled,
              rro.transaction_hash,
              rro.block_timestamp,
              rro.can_be_filled,

              rro.chain_id::TEXT  || ''_'' || rro.market_type::TEXT || ''_'' || rro.market_id AS market_key,

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

              -- Calculate quantity_value_usd for the input_token_id
              COALESCE(
                  (rro.quantity / (10 ^ tp.decimals)) * tp.price
                  , 0
              ) AS quantity_value_usd,

              -- Calculate incentive_value_usd by dividing each element of token_amounts with its corresponding decimals and multiplying with price
              COALESCE(
                  (SELECT SUM((rro.token_amounts[idx] / (10 ^ tp.decimals)) * tp.price)
                  FROM 
                      UNNEST(rro.token_ids, rro.token_amounts) WITH ORDINALITY AS token(token_id, amount, idx)
                  LEFT JOIN 
                      token_quotes tp ON token.token_id = tp.token_id
                  WHERE tp.token_id IS NOT NULL
                  ), 
                  0
              ) AS incentive_value_usd
            
            FROM 
              raw_base_offers rro
            LEFT JOIN 
              token_quotes tp ON rro.input_token_id = tp.token_id
            WHERE
              rro.id IS NOT NULL
              AND ($6 IS NULL OR rro.can_be_filled = $6) -- Optional can_be_filled
          ),
          enriched_raw_data AS (
            SELECT 
                ro.id,
                ro.chain_id,
                ro.market_type,
                ro.offer_side,

                ro.offer_id,

                ro.market_id,
                ro.creator,
                ro.funding_vault,
                ro.input_token_id,

                to_char(ro.quantity, ''FM9999999999999999999999999999999999999999'') AS quantity,
                to_char(ro.quantity_remaining, ''FM9999999999999999999999999999999999999999'') AS quantity_remaining,

                to_char(ro.expiry, ''FM9999999999999999999999999999999999999999'') AS expiry,

                ro.token_ids,

                -- Convert token_amounts from NUMERIC[] to TEXT[]
                array(
                    SELECT to_char(col_value, ''FM9999999999999999999999999999999999999999'')
                    FROM unnest(ro.token_amounts) AS col_value
                ) AS token_amounts,  -- Conversion handled here

                -- Convert protocol_fee_amounts from NUMERIC[] to TEXT[]
                array(
                    SELECT to_char(col_value, ''FM9999999999999999999999999999999999999999'')
                    FROM unnest(ro.protocol_fee_amounts) AS col_value
                ) AS protocol_fee_amounts,  -- Conversion handled here

                -- Convert frontend_fee_amounts from NUMERIC[] to TEXT[]
                array(
                    SELECT to_char(col_value, ''FM9999999999999999999999999999999999999999'')
                    FROM unnest(ro.frontend_fee_amounts) AS col_value
                ) AS frontend_fee_amounts,  -- Conversion handled here

                ro.is_cancelled,
                ro.transaction_hash,
                ro.block_timestamp,
                ro.can_be_filled,
                ro.input_token_price,
                ro.input_token_fdv,
                ro.input_token_total_supply,
                ro.token_price_values,
                ro.token_fdv_values,
                ro.token_total_supply_values,
                ro.quantity_value_usd,
                ro.incentive_value_usd,

                mu.name,
                to_char(rm.lockup_time, ''FM9999999999999999999999999999999999999999'') AS lockup_time,
                rm.reward_style
            FROM 
              enriched_offers ro 
            LEFT JOIN 
              public.raw_markets rm
            ON
              ro.market_key = rm.id
            LEFT JOIN
              public.market_userdata mu
            ON 
              ro.market_key = mu.id
          ),
          enriched_pre_data AS (
              SELECT
                enriched_raw_data.*,

                -- Calculate change_ratio using the calculated `quantity_value_usd`
                COALESCE(
                    CASE 
                        WHEN enriched_raw_data.quantity_value_usd <= 0
                            THEN 0
                        ELSE 
                            enriched_raw_data.incentive_value_usd / enriched_raw_data.quantity_value_usd
                    END, 0
                ) AS change_ratio,

                -- Calculate annual_change_ratio considering lockup_time and reward_style
                COALESCE(
                    CASE 
                        WHEN enriched_raw_data.quantity_value_usd <= 0
                            THEN 0
                        WHEN enriched_raw_data.market_type = 1 
                            THEN ((enriched_raw_data.incentive_value_usd / enriched_raw_data.quantity_value_usd )) * (365 * 24 * 60 * 60) -- in vaults, incentive_value_usd represents incentive_rate_usd
                        WHEN enriched_raw_data.market_type = 0 AND enriched_raw_data.lockup_time::NUMERIC = 0 
                            THEN 10^18 -- 10^18 refers to N/D
                        ELSE 
                            ((enriched_raw_data.incentive_value_usd / enriched_raw_data.quantity_value_usd ) * (365 * 24 * 60 * 60) ) / (enriched_raw_data.lockup_time::NUMERIC)
                    END, 0
                ) AS annual_change_ratio
              FROM 
                enriched_raw_data
          ),
          enriched_data AS (
            SELECT * FROM enriched_pre_data ro
            WHERE ro.id IS NOT NULL

    ';

    -- Step 2: Add dynamic filters if provided
    IF filters IS NOT NULL AND filters <> '' THEN
        base_query := base_query || ' AND ' || filters;
    END IF;

    -- Step 3: Calculate total count after filters are applied
    EXECUTE base_query || ' ) SELECT COUNT(*) FROM enriched_data;'
    INTO total_count
    USING custom_token_data, chain_id, market_type, market_id, creator, can_be_filled;

    -- Step 4: Add sorting
    IF sorting IS NOT NULL AND sorting <> '' THEN
        base_query := base_query || ' ORDER BY ' || sorting;
    ELSE
        base_query := base_query || ' ORDER BY block_timestamp DESC';
    END IF;

    -- Step 4: Execute the paginated query to fetch the result data
    EXECUTE base_query || ' OFFSET $7 LIMIT 20 ) SELECT ARRAY_AGG(result.*) FROM enriched_data AS result;'
    INTO result_data
    USING custom_token_data, chain_id, market_type, market_id, creator, can_be_filled, page_index * 20;

    -- Step 5: Return both total count and data
    RETURN (total_count, result_data)::enriched_offers_return_type;
END;
$$ LANGUAGE plpgsql;

-- Grant permission
GRANT EXECUTE ON FUNCTION get_enriched_offers TO anon;

-- SELECT *
-- FROM unnest((
--     get_enriched_offers(
--         11155111::NUMERIC,            -- chain_id
--         0::INTEGER,                   -- market_type
--         '0'::TEXT,                         -- in_market_id (set as NULL or pass valid TEXT)
--         '0x77777cc68b333a2256b436d675e8d257699aa667'::TEXT, -- creator
--          NULL,
--         '[{
--             "token_id": "11155111-0x3c727dd5ea4c55b7b9a85ea2f287c641481400f7",
--             "price": 10,
--             "fdv": 50000000,
--             "total_supply": 1000000
--           }]'::JSONB,                 -- in_token_data (JSONB)
--         0::INTEGER                 -- page_index
--         -- 'offer_side = 1'::TEXT,        -- filters
--         -- 'reward_style DESC, change_ratio DESC'
--     )
-- ).data) AS enriched_offer;


SELECT *
FROM unnest((
    get_enriched_offers(
    )
).data) AS enriched_offer;