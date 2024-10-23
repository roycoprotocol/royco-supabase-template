-- Drop function
DROP FUNCTION IF EXISTS get_market_offers CASCADE;

-- Create Function
CREATE OR REPLACE FUNCTION get_market_offers(
    in_chain_id NUMERIC,
    in_market_type NUMERIC,
    in_market_id TEXT,
    in_offer_side SMALLINT,
    in_quantity TEXT,
    in_token_data JSONB DEFAULT '[]'::JSONB, -- Input parameter for array of token data (token_id, price, fdv, total_supply) 
    in_incentive_ids TEXT[] DEFAULT NULL
)
RETURNS TABLE (
    id TEXT,
    market_id TEXT,
    offer_id TEXT,
    offer_side SMALLINT,
    expiry TEXT,
    funding_vault TEXT,
    creator TEXT,
    token_amounts TEXT[],  -- Changed to TEXT[]
    token_ids TEXT[],
    quantity TEXT,
    quantity_remaining TEXT,
    protocol_fee_amounts TEXT[],
    frontend_fee_amounts TEXT[],
    is_valid BOOLEAN,
    block_timestamp BIGINT,
    incentive_value_usd FLOAT,
    quantity_value_usd FLOAT,
    change_percent FLOAT,
    rank BIGINT,
    fill_quantity TEXT
) AS
$$
DECLARE
    fill_quantity_remaining NUMERIC := COALESCE(in_quantity::NUMERIC, 0);  -- Initialize fill_quantity_remaining with in_quantity
    r RECORD;  -- Declare the loop variable as a record
BEGIN
    -- Fetch ranked offers into a cursor-like variable
    FOR r IN 
    (
        WITH 
        token_prices AS (
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
              jsonb_to_recordset(in_token_data) AS input(token_id TEXT, price NUMERIC, fdv NUMERIC, total_supply NUMERIC)
              ON tql.token_id = input.token_id
        ),
        enriched_market_data AS (
          SELECT 
            em.input_token_id
          FROM 
            public.raw_markets em
          WHERE
            em.id = LOWER(in_chain_id::TEXT || '_' || '0' || '_' || in_market_id) -- Concatenated market identifier
          LIMIT 1
        ),
        enriched_offers AS (
            SELECT
                ro.*, -- All columns from the recipe_offers table
                -- Calculate incentive_value_usd by dividing each element of token_amounts with its corresponding decimals and multiplying with price
                COALESCE(
                    (SELECT SUM((ro.token_amounts[idx] / (10 ^ tp.decimals)) * tp.price)
                     FROM 
                        UNNEST(ro.token_ids, ro.token_amounts) WITH ORDINALITY AS token(token_id, amount, idx)
                     LEFT JOIN 
                        token_prices tp ON token.token_id = tp.token_id
                     WHERE tp.token_id IS NOT NULL
                    ), 
                    0
                ) AS incentive_value_usd,
                -- Calculate quantity_value_usd for the input_token_id
                COALESCE(
                    (SELECT 
                        (ro.quantity / (10 ^ tp.decimals)) * tp.price
                     FROM 
                        enriched_market_data emd_input
                     LEFT JOIN 
                        token_prices tp ON emd_input.input_token_id = tp.token_id
                     WHERE 
                        tp.token_id IS NOT NULL
                     LIMIT 1
                    ), 0
                ) AS quantity_value_usd
            FROM
                public.raw_offers ro
            WHERE ro.is_valid = TRUE
            AND ro.chain_id = in_chain_id
            AND ro.market_id = in_market_id
            AND ro.offer_side = in_offer_side
            AND ro.is_cancelled = false
            AND ((ro.expiry = 0) OR (ro.expiry > EXTRACT(EPOCH FROM NOW())))

            -- If in_incentive_ids is not NULL, ensure all elements in ro.token_ids are present in in_incentive_ids
            AND (
                in_incentive_ids IS NULL OR 
                (SELECT COUNT(*) = array_length(ro.token_ids, 1)
                 FROM unnest(ro.token_ids) AS token_id
                 WHERE token_id = ANY(in_incentive_ids))
            )
        ),
        offers_with_change_percent AS (
            SELECT
                eo.*,
                -- Calculate change_percent using the provided formula
                CASE 
                    WHEN eo.quantity_value_usd > 0 THEN 
                        (eo.incentive_value_usd / eo.quantity_value_usd)
                    ELSE 
                        0
                END AS change_percent
            FROM
                enriched_offers eo
        ),
        ranked_offers AS (
            SELECT
                o.*,
                ROW_NUMBER() OVER (PARTITION BY o.offer_side 
                    ORDER BY 
                        CASE 
                            WHEN o.offer_side = 0 THEN o.change_percent 
                            WHEN o.offer_side = 1 THEN -o.change_percent 
                            ELSE -o.change_percent 
                        END ASC,
                        o.block_timestamp ASC
                ) AS rank
            FROM
                offers_with_change_percent o
        )
        SELECT
            o.id,
            o.market_id,
            o.offer_id,
            o.offer_side,
            o.expiry,
            o.funding_vault,
            o.creator,
            -- Convert token_amounts from NUMERIC[] to TEXT[]
            array(
                SELECT to_char(amount, 'FM9999999999999999999999999999999999999999')
                FROM unnest(o.token_amounts) AS amount
            ) AS token_amounts,  -- Conversion handled here

            -- Convert token_amounts from NUMERIC[] to TEXT[]
            array(
                SELECT to_char(amount, 'FM9999999999999999999999999999999999999999')
                FROM unnest(o.protocol_fee_amounts) AS amount
            ) AS protocol_fee_amounts,  -- Conversion handled here

            -- Convert token_amounts from NUMERIC[] to TEXT[]
            array(
                SELECT to_char(amount, 'FM9999999999999999999999999999999999999999')
                FROM unnest(o.frontend_fee_amounts) AS amount
            ) AS frontend_fee_amounts,  -- Conversion handled here

            o.token_ids,
            o.quantity,
            o.quantity_remaining,
            o.is_valid,
            o.block_timestamp,
            o.incentive_value_usd,
            o.quantity_value_usd,
            o.change_percent,
            ro.rank
        FROM ranked_offers ro
        JOIN offers_with_change_percent o ON o.id = ro.id
        ORDER BY ro.rank
    ) 
    LOOP
        -- Calculate the fill quantity
        IF r.quantity_remaining <= fill_quantity_remaining THEN
            -- fill_quantity := r.quantity_remaining;
            fill_quantity := to_char(r.quantity_remaining, 'FM9999999999999999999999999999999999999999');
            fill_quantity_remaining := fill_quantity_remaining - r.quantity_remaining;
        ELSE
            -- fill_quantity := fill_quantity_remaining;
            fill_quantity := to_char(fill_quantity_remaining, 'FM9999999999999999999999999999999999999999');
            fill_quantity_remaining := 0;
        END IF;

        -- For recipe markets, check if offer_side is 0 and if fill_quantity / r.quantity is less than 0.25
        IF in_market_type = 0 AND r.offer_side = 0 AND (fill_quantity::NUMERIC / r.quantity) <= 0.25 THEN
            fill_quantity_remaining := fill_quantity_remaining + fill_quantity::NUMERIC;
            CONTINUE;
        END IF;

        -- Assign the current row values to the OUT parameters
        id := r.id;
        market_id := r.market_id;
        offer_id := r.offer_id;
        offer_side := r.offer_side;
        expiry := to_char(r.expiry, 'FM9999999999999999999999999999999999999999');
        funding_vault := r.funding_vault;
        creator := r.creator;
        token_amounts := r.token_amounts;  -- No need to convert again here
        protocol_fee_amounts := r.protocol_fee_amounts;
        frontend_fee_amounts := r.frontend_fee_amounts;
        token_ids := r.token_ids;
        quantity := to_char(r.quantity, 'FM9999999999999999999999999999999999999999');
        quantity_remaining := to_char(r.quantity_remaining, 'FM9999999999999999999999999999999999999999');
        is_valid := r.is_valid;
        block_timestamp := r.block_timestamp;
        incentive_value_usd := r.incentive_value_usd;
        quantity_value_usd := r.quantity_value_usd;
        change_percent := r.change_percent;
        rank := r.rank;
        
        -- Return the current row
        RETURN NEXT;
        
        -- Break the loop if fill_quantity_remaining has been exhausted
        EXIT WHEN fill_quantity_remaining <= 0;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Grant permission
GRANT EXECUTE ON FUNCTION get_market_offers TO anon;

-- Sample Query: Change parameters based on your table data
-- SELECT * FROM get_market_offers(11155111::NUMERIC, 0, '0x655c42f78c176db052676ac728b585d1b9c2c50ec1ed5c93a33e359e43a7f857'::TEXT, 0::SMALLINT, '26000000000000000000');



