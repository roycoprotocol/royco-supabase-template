-- Drop all function variations
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT oid::regprocedure AS func_signature
        FROM pg_proc
        WHERE proname = 'get_validation_offers'
    LOOP
        EXECUTE 'DROP FUNCTION ' || r.func_signature || ' CASCADE';
    END LOOP;
END $$;

-- Create function
CREATE OR REPLACE FUNCTION get_validation_offers(
    offer_ids TEXT[] -- Input parameter for array of offer IDs
)
RETURNS TABLE (
    id TEXT,
    chain_id NUMERIC,
    market_type INTEGER,
    market_id TEXT,
    offer_side INTEGER,
    creator TEXT,
    funding_vault TEXT,
    input_token_id TEXT,
    quantity_remaining TEXT
) AS
$$
BEGIN
    RETURN QUERY
    WITH 
    filtered_raw_offers AS (
        SELECT 
            ro.id,
            ro.chain_id,
            ro.market_type,
            ro.market_id,
            ro.offer_side,
            ro.creator,
            ro.funding_vault,
            ro.input_token_id,
            to_char(ro.quantity_remaining, 'FM9999999999999999999999999999999999999999') AS quantity_remaining
        FROM
            raw_offers ro
        WHERE
            ro.id = ANY(offer_ids)  -- Use ANY to check for array membership
    ) 
    SELECT * FROM filtered_raw_offers;
END;
$$ LANGUAGE plpgsql;

-- Grant permission
GRANT EXECUTE ON FUNCTION get_validation_offers TO anon;

-- Sample query
SELECT * FROM get_validation_offers(
    ARRAY['11155111_0_0_0']  
);
