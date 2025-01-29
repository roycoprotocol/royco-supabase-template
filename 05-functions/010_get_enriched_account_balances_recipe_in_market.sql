-- Drop all function variations
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT oid::regprocedure AS func_signature
        FROM pg_proc
        WHERE proname = 'get_enriched_account_balances_recipe_in_market'
    LOOP
        EXECUTE 'DROP FUNCTION ' || r.func_signature || ' CASCADE';
    END LOOP;
END $$;

-- Create function
CREATE OR REPLACE FUNCTION get_enriched_account_balances_recipe_in_market(
    in_chain_id NUMERIC,
    in_market_id TEXT,
    in_account_address TEXT,
    custom_token_data JSONB DEFAULT '[]'::JSONB -- Input parameter for array of token data (token_id, decimals, price, fdv, total_supply) 
)
RETURNS TABLE (
    input_token_id TEXT,
    input_token_price NUMERIC,
    input_token_fdv NUMERIC,
    input_token_total_supply NUMERIC,

    quantity_ap TEXT,
    quantity_ip TEXT,

    incentives_ids_ap TEXT[],
    incentives_price_values_ap NUMERIC[],
    incentives_fdv_values_ap NUMERIC[],
    incentives_total_supply_values_ap NUMERIC[],

    incentives_amount_ap TEXT[],

    incentives_ids_ip TEXT[],
    incentives_price_values_ip NUMERIC[],
    incentives_fdv_values_ip NUMERIC[],
    incentives_total_supply_values_ip NUMERIC[],

    incentives_amount_ip TEXT[],
    protocol_fee_amounts TEXT[],
    frontend_fee_amounts TEXT[]
) AS 
$$
BEGIN
    RETURN QUERY
    WITH 
    -- ranked_custom_data AS (
    --     SELECT 
    --         input.token_id,
    --         NULLIF(input.decimals, '''')::NUMERIC AS decimals,
    --         NULLIF(input.price, '''')::NUMERIC AS price,
    --         NULLIF(input.fdv, '''')::NUMERIC AS fdv,
    --         NULLIF(input.total_supply, '''')::NUMERIC AS total_supply,
    --         ROW_NUMBER() OVER (PARTITION BY input.token_id ORDER BY (SELECT NULL)) AS row_num
    --     FROM 
    --         jsonb_to_recordset(custom_token_data) AS input(token_id TEXT, decimals TEXT, price TEXT, fdv TEXT, total_supply TEXT)
    -- ),
    -- aggregated_custom_data AS (
    --     SELECT 
    --         rcd.token_id,
    --         (SELECT r.decimals FROM ranked_custom_data r WHERE r.token_id = rcd.token_id AND r.decimals IS NOT NULL ORDER BY r.row_num DESC LIMIT 1) AS decimals,
    --         (SELECT r.price FROM ranked_custom_data r WHERE r.token_id = rcd.token_id AND r.price IS NOT NULL ORDER BY r.row_num DESC LIMIT 1) AS price,
    --         (SELECT r.fdv FROM ranked_custom_data r WHERE r.token_id = rcd.token_id AND r.fdv IS NOT NULL ORDER BY r.row_num DESC LIMIT 1) AS fdv,
    --         (SELECT r.total_supply FROM ranked_custom_data r WHERE r.token_id = rcd.token_id AND r.total_supply IS NOT NULL ORDER BY r.row_num DESC LIMIT 1) AS total_supply
    --     FROM ranked_custom_data rcd
    --     GROUP BY rcd.token_id
    -- ),
    -- token_quotes AS (
    --     -- Combine token quotes from the latest data and aggregated input data
    --     SELECT 
    --         COALESCE(acd.token_id, tql.token_id) AS token_id,
    --         COALESCE(acd.decimals, tql.decimals, 18) AS decimals,
    --         COALESCE(acd.price, tql.price, 0) AS price,
    --         COALESCE(acd.fdv, tql.fdv, 0) AS fdv,
    --         COALESCE(acd.total_supply, tql.total_supply, 0) AS total_supply
    --     FROM 
    --         token_quotes_latest tql
    --     FULL OUTER JOIN 
    --         aggregated_custom_data acd
    --         ON tql.token_id = acd.token_id
    -- )

    token_quotes AS (
        SELECT * FROM public.token_quotes_latest
    )

    SELECT        
        rab.input_token_id,
        COALESCE(tp.price, 0) as input_token_price,
        COALESCE(tp.fdv, 0) as input_token_fdv,
        COALESCE(tp.total_supply, 0) as input_token_total_supply,

        to_char(rab.quantity_given_amount, 'FM9999999999999999999999999999999999999999') AS quantity_ap,
        to_char(rab.quantity_received_amount, 'FM9999999999999999999999999999999999999999') AS quantity_ip,

        rab.incentives_received_ids AS incentives_ids_ap,
        ARRAY(
            SELECT COALESCE(tq.price, 0)
            FROM UNNEST(rab.incentives_received_ids) WITH ORDINALITY AS ids(token_id, i)
            LEFT JOIN token_quotes tq ON ids.token_id = tq.token_id
        ) as incentives_price_values_ap,
        ARRAY(
            SELECT COALESCE(tq.fdv, 0)
            FROM UNNEST(rab.incentives_received_ids) WITH ORDINALITY AS ids(token_id, i)
            LEFT JOIN token_quotes tq ON ids.token_id = tq.token_id
        ) as incentives_fdv_values_ap,
        ARRAY(
            SELECT COALESCE(tq.total_supply, 0)
            FROM UNNEST(rab.incentives_received_ids) WITH ORDINALITY AS ids(token_id, i)
            LEFT JOIN token_quotes tq ON ids.token_id = tq.token_id
        ) as incentives_total_supply_values_ap,

        array(
            SELECT to_char(col_value, 'FM9999999999999999999999999999999999999999')
            FROM unnest(rab.incentives_received_amount) AS col_value
        ) AS incentives_amount_ap,

        rab.incentives_given_ids AS incentives_ids_ip,
        ARRAY(
            SELECT COALESCE(tq.price, 0)
            FROM UNNEST(rab.incentives_given_ids) WITH ORDINALITY AS ids(token_id, i)
            LEFT JOIN token_quotes tq ON ids.token_id = tq.token_id
        ) as incentives_price_values_ip,
        ARRAY(
            SELECT COALESCE(tq.fdv, 0)
            FROM UNNEST(rab.incentives_given_ids) WITH ORDINALITY AS ids(token_id, i)
            LEFT JOIN token_quotes tq ON ids.token_id = tq.token_id
        ) as incentives_fdv_values_ip,
        ARRAY(
            SELECT COALESCE(tq.total_supply, 0)
            FROM UNNEST(rab.incentives_given_ids) WITH ORDINALITY AS ids(token_id, i)
            LEFT JOIN token_quotes tq ON ids.token_id = tq.token_id
        ) as incentives_total_supply_values_ip,

        array(
            SELECT to_char(col_value, 'FM9999999999999999999999999999999999999999')
            FROM unnest(rab.incentives_given_amount) AS col_value
        ) AS incentives_amount_ip,

        array(
            SELECT to_char(col_value, 'FM9999999999999999999999999999999999999999')
            FROM unnest(rab.protocol_fee_amounts) AS col_value
        ) AS protocol_fee_amounts,

        array(
            SELECT to_char(col_value, 'FM9999999999999999999999999999999999999999')
            FROM unnest(rab.frontend_fee_amounts) AS col_value
        ) AS frontend_fee_amounts
    FROM 
        public.raw_account_balances_recipe rab
    LEFT JOIN 
        token_quotes tp ON rab.input_token_id = tp.token_id
    WHERE 
        rab.chain_id = in_chain_id
        AND rab.market_id = in_market_id  
        AND rab.account_address = in_account_address
    LIMIT 1;
END;
$$ LANGUAGE plpgsql PARALLEL SAFE STABLE;

-- Grant permission
GRANT EXECUTE ON FUNCTION get_enriched_account_balances_recipe_in_market TO anon;

-- Test query
SELECT * FROM get_enriched_account_balances_recipe_in_market(11155111::NUMERIC, '0x3ab5695b341fc27f80271b7425d33e658bed758cd8c3b919f0763bd23d33dfc7'::TEXT, '0x77777cc68b333a2256b436d675e8d257699aa667'::TEXT);


-- Backup for global summation
-- raw_rows AS (
--         SELECT 
--             rab.chain_id::TEXT  || ''_'' || '0' || ''_'' || rab.market_id AS market_key,
--             rab.input_token_id,
--             rab.quantity_received_amount,
--             rab.quantity_given_amount,
--             rab.incentives_received_ids,
--             rab.incentives_given_ids,
--             rab.incentives_received_amount,
--             rab.incentives_given_amount,
--             rab.protocol_fee_amounts,
--             rab.frontend_fee_amounts
--         FROM 
--             public.raw_account_balances_recipe rab
--         WHERE
--             rab.account_address = in_account_address
--             AND (in_chain_id IS NULL OR rab.chain_id = in_chain_id) -- Optional chain_id
--             AND (in_market_id IS NULL OR rab.market_id = in_market_id) -- Optional market_id
--     ),
--  enriched_quantity AS (
--         SELECT 
--             SUM(rr.quantity_received_amount) AS quantity_ap,
--             SUM(rr.quantity_given_amount) AS quantity_ip
--         FROM
--             raw_rows rr
--         GROUP BY market_key
--     ),

--     unnested_rows_ap AS (
--         SELECT 
--             rr.token_id,
--             rr.token_amount
--         FROM 
--             raw_rows rr
--             CROSS JOIN LATERAL unnest(rr.incentives_received_ids) WITH ORDINALITY AS token_ids_with_ord(token_id, ordinality)
--             CROSS JOIN LATERAL unnest(rr.incentives_received_amounts) WITH ORDINALITY AS token_amounts_with_ord(token_amount, ordinality)
--         WHERE 
--             token_ids_with_ord.ordinality = token_amounts_with_ord.ordinality
--     ),
--     unnested_rows_ip AS (
--         SELECT 
--             rr.token_id,
--             rr.token_amount,
--             rr.protocol_fee_amount,
--             rr.frontend_fee_amount
--         FROM 
--             raw_rows rr
--             CROSS JOIN LATERAL unnest(rr.incentives_given_ids) WITH ORDINALITY AS token_ids_with_ord(token_id, ordinality)
--             CROSS JOIN LATERAL unnest(rr.incentives_given_amounts) WITH ORDINALITY AS token_amounts_with_ord(token_amount, ordinality)
--             CROSS JOIN LATERAL unnest(rr.protocol_fee_amounts) WITH ORDINALITY AS protocol_fee_amounts_with_ord(protocol_fee_amount, ordinality)
--             CROSS JOIN LATERAL unnest(rr.frontend_fee_amounts) WITH ORDINALITY AS frontend_fee_amounts_with_ord(frontend_fee_amount, ordinality)
--         WHERE 
--             token_ids_with_ord.ordinality = token_amounts_with_ord.ordinality
--             AND token_ids_with_ord.ordinality = protocol_fee_amounts_with_ord.ordinality
--             AND token_ids_with_ord.ordinality = frontend_fee_amounts_with_ord.ordinality
--     ),

--     summed_rows_ap AS (
--         SELECT 
--             market_key,
--             token_id,
--             SUM(token_amount) as token_amount
--         FROM unnested_rows_ap
--         GROUP BY market_key, token_id
--     ),
--     summed_rows_ip AS (
--         SELECT 
--             market_key,
--             token_id,
--             SUM(token_amount) as token_amount,
--             SUM(protocol_fee_amount) as protocol_fee_amount,
--             SUM(frontend_fee_amount) as frontend_fee_amount
--         FROM unnested_rows_ip
--         GROUP BY market_key, token_id
--     ),

--     enriched_rows_ap AS (
--         SELECT 
--             market_key,
--             ARRAY_AGG(token_id ORDER BY token_id) as token_ids_ap,
--             ARRAY_AGG(total_amount ORDER BY token_id) as token_amounts_ap
--         FROM summed_rows_ap
--         GROUP BY market_key
--     ),
--     enriched_rows_ip AS (
--         SELECT 
--             market_key,
--             ARRAY_AGG(token_id ORDER BY token_id) as token_ids_ip,
--             ARRAY_AGG(total_amount ORDER BY token_id) as token_amounts_ip,
--             ARRAY_AGG(total_protocol_fee ORDER BY token_id) as protocol_fees_ip,
--             ARRAY_AGG(total_frontend_fee ORDER BY token_id) as frontend_fees_ip
--         FROM summed_rows_ip
--         GROUP BY market_key
--     ),

--     enriched_pre_data AS (
--         SELECT 
--             rr.market_key,
--             rr.input_token_id,

--     )