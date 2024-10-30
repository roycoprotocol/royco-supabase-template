-- @heads-up: This function will be updated in the future

-- Drop all function variations
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT oid::regprocedure AS func_signature
        FROM pg_proc
        WHERE proname = 'search_contracts'
    LOOP
        EXECUTE 'DROP FUNCTION ' || r.func_signature || ' CASCADE';
    END LOOP;
END $$;

-- Drop exisiting type
DROP TYPE IF EXISTS search_contracts_return;

-- Create return type
CREATE TYPE search_contracts_return AS (
  data JSONB,
  count INT
);

-- Drop existing function
DROP FUNCTION IF EXISTS search_contracts CASCADE;

-- Create new function
CREATE OR REPLACE FUNCTION search_contracts(
  search_key TEXT,
  sorting TEXT,
  filters TEXT,
  page_index INT,
  page_size INT
)
RETURNS search_contracts_return AS $$
DECLARE
  base_query TEXT;
  total_count INT;
  rows JSONB;
  total_pages INT;
BEGIN
  -- Base query for the data
  base_query := '
    WITH
    custom_contracts AS (
      SELECT * FROM contracts WHERE abi IS NOT NULL
    )
    SELECT 
      id,
      chain_id,
      address,
      source,
      contract_name,
      label,
      type,
      image,
      proxy_type,
      implementation_id
    FROM 
      custom_contracts
  ';

  -- Add filters
  IF filters IS NOT NULL AND filters <> '' THEN
    base_query := base_query || ' WHERE ' || filters;
  END IF;

  -- Add search filter if search_key is provided
  IF search_key IS NOT NULL AND search_key <> '' THEN
    -- Replace spaces with '+' in search_key
    search_key := replace(search_key, ' ', '+');
    
    IF filters IS NOT NULL AND filters <> '' THEN
      base_query := base_query || ' AND ';
    ELSE
      base_query := base_query || ' WHERE ';
    END IF;
    base_query := base_query || 'id IN (SELECT id FROM contract_search_index WHERE to_tsvector(search_id) @@ to_tsquery(''' || search_key || ':*''))';
  END IF;

  -- Add sorting
  IF sorting IS NOT NULL AND sorting <> '' THEN
    base_query := base_query || ' ORDER BY ' || sorting;
  END IF;
  
  -- Calculate total count
  EXECUTE 'SELECT COUNT(*) FROM (' || base_query || ') AS total' INTO total_count;

  -- Add pagination
  base_query := base_query || ' OFFSET ' || (page_index * page_size) || ' LIMIT ' || page_size;

  -- Fetch the rows
  EXECUTE 'SELECT json_agg(final_query) FROM (' || base_query || ') AS final_query' INTO rows;

  -- Return the result as a JSON object
  RETURN (rows, total_count)::search_contracts_return;
END;
$$ LANGUAGE plpgsql;

-- Grant permission 
GRANT EXECUTE ON FUNCTION search_contracts TO anon;
