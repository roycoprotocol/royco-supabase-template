-- @note: Update the <BASE_FRONTEND_URL> with your own frontend URL before running this SQL script
-- @heads-up: This function will be updated in the future

-- Drop existing function
DROP FUNCTION IF EXISTS get_contracts CASCADE;

-- Create new function
CREATE OR REPLACE FUNCTION get_contracts(_contracts jsonb) 
RETURNS 
TABLE (
    contract_id text,
    chain_id int8,
    address text,
    source text,
    contract_name text,
    label text,
    type text,
    image text,
    updated_at timestamp with time zone,
    abi jsonb,
    implementation_id text,
    proxy_type text
)
LANGUAGE plpgsql 
AS $$
DECLARE
    contract jsonb;
    contract_id text;
    chain_id bigint;
    contract_address text;
    source text;
    label text;
    type text;
    image text;
    existing_contract record;
    api_url text := '<BASE_FRONTEND_URL>/api/evm/contract';
    api_response jsonb;
    api_contract jsonb;
    api_function jsonb;
BEGIN
    SET LOCAL pg_temp.role = 'rpc_get_contracts';

    -- Loop through each contract in the input array
    FOR contract IN SELECT * FROM jsonb_array_elements(_contracts) LOOP
        contract_id := LOWER((contract->>'chain_id') || '-' || (contract->>'contract_address'));
        chain_id := (contract->>'chain_id')::bigint;
        contract_address := LOWER(contract->>'contract_address');
        source := COALESCE(contract->>'source', 'user');
        label := contract->>'label';
        type := contract->>'type';
        image := contract->>'image';

        -- Check if the contract exists in the database
        SELECT * INTO existing_contract
        FROM public.contracts AS c
        WHERE LOWER(c.id) = LOWER(contract_id);

        -- If contract exists and abi is not null, skip to the next contract
        IF existing_contract.id IS NOT NULL AND (
          existing_contract.abi IS NOT NULL AND (
            existing_contract.proxy_type IS NULL OR (
              existing_contract.proxy_type IS NOT NULL AND existing_contract.implementation_id IS NOT NULL
            )
          )
        ) THEN
            CONTINUE;
        END IF;

        -- If contract does not exist, insert it
        IF existing_contract.id IS NULL THEN
            INSERT INTO public.contracts (id, chain_id, address, source, label, type, image)
            VALUES (LOWER(contract_id), chain_id, LOWER(contract_address), source, label, type, image);
        END IF;

        -- If contract exists but abi is null, check the updated_at timestamp
        IF existing_contract.id IS NULL OR (
          existing_contract.id IS NOT NULL AND existing_contract.abi is NULL AND (current_timestamp - existing_contract.updated_at > interval '5 minutes') 
        ) OR (
          existing_contract.id IS NOT NULL AND existing_contract.proxy_type IS NOT NULL AND (current_timestamp - existing_contract.updated_at > interval '5 minutes') AND existing_contract.implementation_id IS NULL
        )
           THEN

            -- Fetch data from the API
            SELECT content::jsonb INTO api_response
            FROM http_post(api_url, jsonb_build_object('contracts', jsonb_build_array(
                jsonb_build_object('chain_id', chain_id, 'contract_address', contract_address)
            ))::text, 'application/json');

            -- First, loop through 'data' to upsert contracts
            FOR api_contract IN SELECT * FROM jsonb_array_elements(api_response->'data') LOOP
              INSERT INTO public.contracts AS c
              (id, chain_id, address, source, contract_name, label, type, image, updated_at, abi, proxy_type, implementation_id)
              VALUES (
                  LOWER((api_contract->>'chain_id') || '-' || (api_contract->>'contract_address')), 
                  (api_contract->>'chain_id')::int8, 
                  LOWER(api_contract->>'contract_address'), 
                  source,
                  api_contract->>'contract_name', 
                  label, 
                  type, 
                  image, 
                  current_timestamp, 
                  api_contract->'abi', 
                  api_contract->>'proxy_type', 
                  api_contract->>'implementation_id'
              )
              ON CONFLICT (id) DO UPDATE
              SET 
                  contract_name = EXCLUDED.contract_name,
                  abi = EXCLUDED.abi,
                  updated_at = EXCLUDED.updated_at,
                  proxy_type = EXCLUDED.proxy_type,
                  implementation_id = EXCLUDED.implementation_id;
            END LOOP;
        END IF;
    END LOOP; 

    -- Return the contracts
    RETURN QUERY
    WITH 
    t1 AS (
      SELECT 
        c.*
      FROM 
        public.contracts AS c
      WHERE 
        c.id IN (
            SELECT 
              LOWER((c_inner->>'chain_id') || '-' || (c_inner->>'contract_address'))
            FROM 
              jsonb_array_elements(_contracts) AS c_inner
        )
    ),
    t2 AS (
      SELECT 
        c.*
      FROM 
        public.contracts AS c
      WHERE 
        c.id IN (SELECT t1.implementation_id FROM t1)
    ),
    t3 AS (
      SELECT * FROM t1
      UNION
      SELECT * FROM t2
    )
    SELECT 
      c.id,
      c.chain_id,
      c.address,
      c.source,
      c.contract_name,
      c.label,
      c.type,
      c.image,
      c.updated_at,
      c.abi,
      c.proxy_type,
      c.implementation_id
    FROM 
      t3 AS c;
END;
$$;

-- Grant permission for public execution
GRANT EXECUTE ON FUNCTION get_contracts TO anon;

-- Sample Query: Update parameters based on your table data
--SELECT * FROM get_contracts('[{"chain_id": 1, "contract_address": "0xa9dd04720a5d62ed6c0dd7acd735773652c8baab"}]'::jsonb);

