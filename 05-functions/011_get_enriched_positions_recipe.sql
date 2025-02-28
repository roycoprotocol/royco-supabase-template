-- Drop all function variations
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT oid::regprocedure AS func_signature
        FROM pg_proc
        WHERE proname = 'get_enriched_positions_recipe'
    LOOP
        EXECUTE 'DROP FUNCTION ' || r.func_signature || ' CASCADE';
    END LOOP;
END $$;

-- Drop return type
DROP TYPE IF EXISTS enriched_positions_recipe_return_type;

-- Drop data type
DROP TYPE IF EXISTS enriched_positions_recipe_data_type;

-- Create data type
CREATE TYPE enriched_positions_recipe_data_type AS (
  id TEXT,
  chain_id NUMERIC,
  weiroll_wallet TEXT,
  offer_side INTEGER,
  market_id TEXT,
  reward_style INTEGER,

  raw_offer_side INTEGER,
  raw_offer_id TEXT,
  account_address TEXT,

  ap TEXT,
  ip TEXT,
  input_token_id TEXT,
  quantity TEXT,

  token_ids TEXT[],
  token_amounts TEXT[],
  protocol_fee_amounts TEXT[],
  frontend_fee_amounts TEXT[],

  is_claimed BOOLEAN[],
  is_forfeited BOOLEAN,
  is_withdrawn BOOLEAN,
  unlock_timestamp TEXT,

  block_number NUMERIC,
  transaction_hash TEXT,
  block_timestamp NUMERIC,

  input_token_price NUMERIC,
  input_token_fdv NUMERIC,
  input_token_total_supply NUMERIC,

  token_price_values NUMERIC[],
  token_fdv_values NUMERIC[],
  token_total_supply_values NUMERIC[],

  can_withdraw BOOLEAN,
  can_claim BOOLEAN,

  name TEXT,
  lockup_time NUMERIC,

  destination_chain_id NUMERIC,
  status TEXT,

  weiroll_wallet_destination TEXT,
  merkle_deposit_nonce NUMERIC,

  deposit_transaction_hash TEXT,
  forfeit_transaction_hash TEXT,
  bridge_transaction_hash TEXT,
  process_transaction_hash TEXT,
  execute_transaction_hash TEXT,
  withdraw_transaction_hash TEXT
);

-- Create return type
CREATE TYPE enriched_positions_recipe_return_type AS (
  count INT,
  data enriched_positions_recipe_data_type[]
);

-- Create function
CREATE OR REPLACE FUNCTION get_enriched_positions_recipe(
    account_address TEXT,
    chain_id NUMERIC DEFAULT NULL,
    market_id TEXT DEFAULT NULL,
    custom_token_data JSONB DEFAULT '[]'::JSONB, -- Input parameter for array of token data (token_id, decimals, price, fdv, total_supply) 
    page_index INT DEFAULT 0,
    page_size INT DEFAULT 20,
    filters TEXT DEFAULT NULL, 
    sorting TEXT DEFAULT NULL
)
RETURNS enriched_positions_recipe_return_type AS
$$
DECLARE
    total_count INT;
    result_data enriched_positions_recipe_data_type[];
    base_query TEXT;
BEGIN
    -- Step 1: Construct the base query with the WITH clause properly enclosed
    base_query := '
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
        --         jsonb_to_recordset($1) AS input(token_id TEXT, decimals TEXT, price TEXT, fdv TEXT, total_supply TEXT)
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
        -- ),

        token_quotes AS (
            SELECT * FROM public.token_quotes_latest
        ),
        ccdm_source AS (
            SELECT 
                ws.chain_id,
                wd.chain_id AS destination_chain_id,
                ws.weiroll_wallet,
                wd.weiroll_wallet AS weiroll_wallet_destination,
                ws.market_id,
                ws.ccdm_nonce,
                dm.merkle_deposit_nonce,
                ws.deposit_leaf,
                ws.amount_deposited,
                rs.total_amount_bridged,
                wd.receipt_token_amount,
                ws.deposit_transaction_hash,
                ws.withdraw_transaction_hash AS forfeit_transaction_hash,
                rs.bridge_transaction_hash,
                wd.process_transaction_hash,
                wd.execute_transaction_hash
            FROM 
                raw_weiroll_wallet_source ws
                LEFT JOIN raw_bridge_ref_source rs
                ON ws.ccdm_nonce = rs.ccdm_nonce
                AND ws.deposit_leaf = rs.deposit_leaf
                AND ws.chain_id = rs.chain_id
                LEFT JOIN raw_weiroll_wallet_destination wd
                ON ws.ccdm_nonce = wd.ccdm_nonce
                AND wd.chain_id = CASE 
                    WHEN ws.chain_id = 11155111 THEN 80000
                    WHEN ws.chain_id = 1 THEN 80094
                END
                LEFT JOIN raw_merkle_deposit_made dm
                ON ws.chain_id = dm.chain_id AND ws.weiroll_wallet = dm.weiroll_wallet
        ),
        ccdm_destination AS (
            SELECT 
                cs.*,
                rd.withdraw_transaction_hash,
                CASE
                    WHEN cs.forfeit_transaction_hash IS NOT NULL THEN ''forfeited''
                    WHEN cs.bridge_transaction_hash IS NULL THEN ''deposited''
                    WHEN rd.withdraw_transaction_hash IS NOT NULL AND cs.execute_transaction_hash IS NOT NULL THEN ''withdrawn''
                    WHEN rd.withdraw_transaction_hash IS NOT NULL AND cs.execute_transaction_hash IS NULL THEN ''overdue''
                    WHEN rd.withdraw_transaction_hash IS NULL AND cs.process_transaction_hash IS NOT NULL THEN ''processed''
                    ELSE NULL
                END AS status,
                CASE 
                    WHEN cs.forfeit_transaction_hash IS NOT NULL THEN true
                    WHEN rd.withdraw_transaction_hash IS NOT NULL THEN true
                    ELSE false
                END AS is_withdrawn,
                md.receipt_token_id AS input_token_id,
                CASE 
                    WHEN cs.amount_deposited IS NOT NULL 
                    AND cs.total_amount_bridged IS NOT NULL 
                    AND cs.receipt_token_amount IS NOT NULL 
                    AND cs.total_amount_bridged != 0
                    AND md.receipt_token_id IS NOT NULL
                    THEN (cs.amount_deposited * cs.receipt_token_amount) / cs.total_amount_bridged 
                    ELSE NULL 
                END AS quantity,
                md.unlock_timestamp
            FROM
                ccdm_source cs
                LEFT JOIN raw_bridge_ref_destination rd
                ON cs.destination_chain_id = rd.chain_id
                AND cs.ccdm_nonce = rd.ccdm_nonce
                AND cs.deposit_leaf = rd.deposit_leaf
                LEFT JOIN raw_market_destination md
                ON cs.destination_chain_id = md.chain_id
                AND cs.market_id = md.market_id
        ),

        enriched_positions AS (
            SELECT
              rro.id,
              rro.chain_id,
              rro.weiroll_wallet,
              rro.offer_side,
              rro.market_id,
              rro.reward_style,

              rro.raw_offer_side,
              rro.raw_offer_id,
              rro.account_address,

              rro.ap,
              rro.ip,
              COALESCE(cd.input_token_id, rro.input_token_id) AS input_token_id,
              to_char(COALESCE(cd.quantity, rro.quantity), ''FM9999999999999999999999999999999999999999'') AS quantity,

              rro.token_ids,
              -- Convert token_amounts from NUMERIC[] to TEXT[]
              array(
                  SELECT to_char(col_value, ''FM9999999999999999999999999999999999999999'')
                  FROM unnest(rro.token_amounts) AS col_value
              ) AS token_amounts,  -- Conversion handled here

              -- Convert token_amounts from NUMERIC[] to TEXT[]
              array(
                  SELECT to_char(col_value, ''FM9999999999999999999999999999999999999999'')
                  FROM unnest(rro.protocol_fee_amounts) AS col_value
              ) AS protocol_fee_amounts,  -- Conversion handled here

              -- Convert token_amounts from NUMERIC[] to TEXT[]
              array(
                  SELECT to_char(col_value, ''FM9999999999999999999999999999999999999999'')
                  FROM unnest(rro.frontend_fee_amounts) AS col_value
              ) AS frontend_fee_amounts,  -- Conversion handled here

              rro.is_claimed,
              rro.is_forfeited,

              COALESCE(cd.is_withdrawn, rro.is_withdrawn) AS is_withdrawn,
              to_char(COALESCE(cd.unlock_timestamp, rro.unlock_timestamp), ''FM9999999999999999999999999999999999999999'') AS unlock_timestamp,

              rro.block_number,
              rro.transaction_hash,
              rro.block_timestamp,

              rro.chain_id::TEXT  || ''_'' || ''0'' || ''_'' || rro.market_id AS market_key,

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
                  WHEN (COALESCE(cd.is_withdrawn, rro.is_withdrawn)) = true THEN false
                  WHEN COALESCE(cd.unlock_timestamp, rro.unlock_timestamp) < EXTRACT(EPOCH FROM NOW()) AND rro.offer_side = 0 THEN true
                  ELSE false
              END AS can_withdraw,

              -- Can Claim Column
              CASE
                  WHEN rro.unlock_timestamp < EXTRACT(EPOCH FROM NOW())
                      AND rro.is_forfeited = false
                      AND rro.offer_side = 0
                      AND EXISTS (
                          SELECT 1
                          FROM UNNEST(rro.is_claimed) AS claimed_value
                          WHERE claimed_value = false
                      )
                  THEN true
                  ELSE false
              END AS can_claim,

              cd.destination_chain_id,
              cd.status,

              cd.weiroll_wallet_destination,
              cd.merkle_deposit_nonce,

              cd.deposit_transaction_hash,
              cd.forfeit_transaction_hash,
              cd.bridge_transaction_hash,
              cd.process_transaction_hash,
              cd.execute_transaction_hash,
              cd.withdraw_transaction_hash

            FROM 
              raw_positions_recipe rro
            LEFT JOIN
                ccdm_destination cd ON rro.chain_id = cd.chain_id AND rro.weiroll_wallet = cd.weiroll_wallet
            LEFT JOIN 
              token_quotes tp 
              ON COALESCE(cd.input_token_id, rro.input_token_id) = tp.token_id
            WHERE 
              rro.account_address = $2
              AND ($3 IS NULL OR rro.chain_id = $3) -- Optional chain_id
            
          ),
          enriched_data AS (
            SELECT 
                ro.id,
                ro.chain_id,
                ro.weiroll_wallet,
                ro.offer_side,
                ro.market_id,
                ro.reward_style,

                ro.raw_offer_side,
                ro.raw_offer_id,
                ro.account_address,

                ro.ap,
                ro.ip,
                ro.input_token_id,
                ro.quantity,

                ro.token_ids,
                ro.token_amounts,
                ro.protocol_fee_amounts,
                ro.frontend_fee_amounts,
                
                ro.is_claimed,
                ro.is_forfeited,
                ro.is_withdrawn,
                ro.unlock_timestamp,

                ro.block_number,
                ro.transaction_hash,
                ro.block_timestamp,

                ro.input_token_price,
                ro.input_token_fdv,
                ro.input_token_total_supply,
                ro.token_price_values,
                ro.token_fdv_values,
                ro.token_total_supply_values,

                ro.can_withdraw,
                ro.can_claim,
                mu.name,
                COALESCE(bm.lockup_time, rm.lockup_time) AS lockup_time,

                ro.destination_chain_id,
                ro.status,

                ro.weiroll_wallet_destination,
                ro.merkle_deposit_nonce,

                ro.deposit_transaction_hash,
                ro.forfeit_transaction_hash,
                ro.bridge_transaction_hash,
                ro.process_transaction_hash,
                ro.execute_transaction_hash,
                ro.withdraw_transaction_hash
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
            LEFT JOIN
              public.boyco_market_data bm
            ON 
              ro.market_key = bm.id
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
    EXECUTE base_query || ' OFFSET $5 LIMIT $6 ) SELECT ARRAY_AGG(result.*) FROM enriched_data AS result;'
    INTO result_data
    USING custom_token_data, account_address, chain_id, market_id, page_index * page_size, page_size;

    -- Step 7: Return both total count and data
    RETURN (total_count, result_data)::enriched_positions_recipe_return_type;
END;
$$ LANGUAGE plpgsql PARALLEL SAFE STABLE;

-- Grant permission
GRANT EXECUTE ON FUNCTION get_enriched_positions_recipe TO anon;

-- Sample query
SELECT *
FROM unnest((
    get_enriched_positions_recipe(
        '0x77777cc68b333a2256b436d675e8d257699aa667',
        NULL,
        '0x2226f288fb81a54c5f1ebd244313374c0d49ebea0953e0e7ac23d949d64ecbc1',
        '[]'::JSONB, -- Input parameter for array of token data (token_id, decimals, price, fdv, total_supply) 
        0,
        100,
        NULL, 
        NULL
    )
).data) AS enriched_position;
