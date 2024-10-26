-- Drop all function variations
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT oid::regprocedure AS func_signature
        FROM pg_proc
        WHERE proname = 'get_token_quotes'
    LOOP
        EXECUTE 'DROP FUNCTION ' || r.func_signature || ' CASCADE';
    END LOOP;
END $$;

-- Create function
CREATE OR REPLACE FUNCTION get_token_quotes(
    token_ids TEXT[], -- Input parameter for array of token IDs
    custom_token_data JSONB DEFAULT '[]'::JSONB -- Input parameter for array of token data (token_id, decimals, price, fdv, total_supply) 
)
RETURNS TABLE (
    token_id TEXT,
    decimals NUMERIC,
    price NUMERIC,
    total_supply NUMERIC,
    fdv NUMERIC
) AS
$$
BEGIN
  RETURN QUERY
  WITH 
  ranked_custom_data AS (
      SELECT 
        input.token_id,
        NULLIF(input.decimals, '')::NUMERIC AS decimals,
        NULLIF(input.price, '')::NUMERIC AS price,
        NULLIF(input.fdv, '')::NUMERIC AS fdv,
        NULLIF(input.total_supply, '')::NUMERIC AS total_supply,
        ROW_NUMBER() OVER (PARTITION BY input.token_id ORDER BY (SELECT NULL)) AS row_num
      FROM 
        jsonb_to_recordset(custom_token_data) AS input(token_id TEXT, decimals TEXT, price TEXT, fdv TEXT, total_supply TEXT)
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
      SELECT 
        tql.token_id AS token_id,
        COALESCE(acd.decimals, tql.decimals, 0) AS decimals,
        COALESCE(acd.price, tql.price, 0) AS price,
        COALESCE(acd.fdv, tql.fdv, 0) AS fdv,
        COALESCE(acd.total_supply, tql.total_supply, 0) AS total_supply
      FROM 
        token_quotes_latest tql
      LEFT JOIN 
        aggregated_custom_data acd
        ON tql.token_id = acd.token_id
      WHERE tql.token_id = ANY(token_ids)
      
      UNION ALL
      
      SELECT 
        acd.token_id AS token_id,
        COALESCE(acd.decimals, 0) AS decimals,
        COALESCE(acd.price, 0) AS price,
        COALESCE(acd.fdv, 0) AS fdv,
        COALESCE(acd.total_supply, 0) AS total_supply
      FROM 
        aggregated_custom_data acd
      WHERE acd.token_id = ANY(token_ids)
        AND acd.token_id NOT IN (SELECT tql.token_id FROM token_quotes_latest tql)
  )
  SELECT DISTINCT ON (at.token_id) -- Select only distinct variations
      at.token_id,
      COALESCE(tq.decimals, 0) AS decimals,
      COALESCE(tq.price, 0) AS price,
      COALESCE(tq.total_supply, 0) AS total_supply,
      COALESCE(tq.fdv, 0) AS fdv
  FROM unnest(token_ids) AS at(token_id)
  LEFT JOIN token_quotes tq ON at.token_id = tq.token_id
  ORDER BY at.token_id;
END;
$$ LANGUAGE plpgsql;

-- Grant permission
GRANT EXECUTE ON FUNCTION get_token_quotes TO anon;

-- Sample query
SELECT * FROM get_token_quotes(
    ARRAY['tokenA', 'tokenB', 'tokenC', 'tokenD', '1-0x7c5a0ce9267ed19b22f8cae653f198e3e8daf098', '1-0x7c5a0ce9267ed19b22f8cae653f198e3e8daf098'],
    '[{"token_id": "tokenA", "price": "1.5", "fdv": "1500", "total_supply": "1000"}, 
      {"token_id": "tokenB", "price": "2.5"},
      {"token_id": "tokenB", "price": "88", "total_supply": "90"},
            {"token_id": "1-0x7c5a0ce9267ed19b22f8cae653f198e3e8daf098", "total_supply": "5"},
             {"token_id": "1-0x7c5a0ce9267ed19b22f8cae653f198e3e8daf098", "total_supply": "890"},
              {"token_id": "1-0x7c5a0ce9267ed19b22f8cae653f198e3e8daf098", "total_supply": "69"},
      {"token_id": "tokenC", "fdv": "3000"}]'::JSONB
);