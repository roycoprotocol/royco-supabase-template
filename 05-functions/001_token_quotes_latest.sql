-- @note: Update the <COINMARKETCAP_API_KEY> & <COINGECKO_API_KEY> with your own key before running this SQL script

-- Drop all function variations
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT oid::regprocedure AS func_signature
        FROM pg_proc
        WHERE proname = 'update_token_quotes_historical_coinmarketcap'
    LOOP
        EXECUTE 'DROP FUNCTION ' || r.func_signature || ' CASCADE';
    END LOOP;
END $$;

-- Coinmarketcap Update Function
CREATE OR REPLACE FUNCTION update_token_quotes_historical_coinmarketcap() 
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE 
  api_key text := '<COINMARKETCAP_API_KEY>';
  api_url text;
  search_ids text;
  json_response jsonb;
  token jsonb;
  token_id text;
  token_price double precision;
  token_total_supply double precision;
  token_volume_24h double precision;
  token_market_cap double precision;
  token_fully_diluted_market_cap double precision;
  token_last_updated timestamp with time zone;
BEGIN
  -- Fetch search_ids and their last updated timestamps
  SELECT string_agg(subquery.search_id, ',')
  INTO search_ids
  FROM (
    SELECT 
      search_id,
      last_updated
    FROM 
      public.token_index
    WHERE 
      is_active = TRUE
      AND source = 'coinmarketcap'
      AND NOW() - last_updated >= INTERVAL '1 min'
    ORDER BY last_updated ASC
    LIMIT 100
  ) AS subquery;

  -- Check if there are any search_ids to update
  IF search_ids IS NOT NULL THEN
    -- Create the API URL with the comma-separated list of search_ids
    api_url := 'https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest?id=' || search_ids || '&CMC_PRO_API_KEY=' || api_key;

    -- Make the HTTP request and parse the JSON response
    json_response := (SELECT content::jsonb FROM http_get(api_url));

    -- Loop over the returned tokens and insert into the historical table
    FOR token_id IN SELECT jsonb_object_keys(json_response->'data')
    LOOP
      token := json_response->'data'->token_id;

      -- Extract the required fields from the JSON with NULL checks
      token_price := COALESCE(NULLIF((token->'quote'->'USD'->>'price'), 'null')::double precision, 0);
      token_total_supply := COALESCE(NULLIF((token->'total_supply')::text, 'null')::double precision, 0);
      token_volume_24h := COALESCE(NULLIF((token->'quote'->'USD'->>'volume_24h'), 'null')::double precision, 0);
      token_market_cap := COALESCE(NULLIF((token->'quote'->'USD'->>'market_cap'), 'null')::double precision, 0);
      token_fully_diluted_market_cap := COALESCE(NULLIF((token->'quote'->'USD'->>'fully_diluted_market_cap'), 'null')::double precision, 0);
      token_last_updated := COALESCE(NULLIF((token->'quote'->'USD'->>'last_updated'), 'null')::timestamp with time zone, NOW());

      -- Only proceed with insertion if token_price or other key values are valid
      IF token_price IS NOT NULL THEN
        -- Insert the data into the token_quotes_historical table
        INSERT INTO public.token_quotes_historical (
          source, 
          search_id, 
          price, 
          total_supply,
          volume_24h, 
          market_cap, 
          fully_diluted_market_cap, 
          last_updated
        ) VALUES (
          'coinmarketcap', 
          token_id, 
          token_price, 
          token_total_supply,
          token_volume_24h, 
          token_market_cap, 
          token_fully_diluted_market_cap, 
          token_last_updated
        );

        -- Insert or update the data into the token_quotes_archive table
        INSERT INTO public.token_quotes_archive (
            source, 
            search_id, 
            price, 
            total_supply,
            volume_24h, 
            market_cap, 
            fully_diluted_market_cap, 
            last_updated
        ) VALUES (
            'coinmarketcap', 
            token_id, 
            token_price, 
            token_total_supply,
            token_volume_24h, 
            token_market_cap, 
            token_fully_diluted_market_cap, 
            date_trunc('day', token_last_updated) -- Round to day
        )
        ON CONFLICT (source, search_id, last_updated)
        DO UPDATE
        SET 
            price = EXCLUDED.price, 
            total_supply = EXCLUDED.total_supply,
            volume_24h = EXCLUDED.volume_24h, 
            market_cap = EXCLUDED.market_cap, 
            fully_diluted_market_cap = EXCLUDED.fully_diluted_market_cap;
      END IF;
    END LOOP;
  ELSE
    -- No search_ids to update
    RETURN;
  END IF;
END;
$$;

-- Drop all function variations
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT oid::regprocedure AS func_signature
        FROM pg_proc
        WHERE proname = 'update_token_quotes_historical_coingecko'
    LOOP
        EXECUTE 'DROP FUNCTION ' || r.func_signature || ' CASCADE';
    END LOOP;
END $$;

-- Coingecko Update Function
CREATE OR REPLACE FUNCTION update_token_quotes_historical_coingecko() 
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE 
  api_key text := '<COINGECKO_API_KEY>';
  api_url text;
  search_ids text;
  json_response jsonb;
  token jsonb;
  token_id text;
  token_price double precision;
  token_total_supply double precision;
  token_volume_24h double precision;
  token_market_cap double precision;
  token_fully_diluted_market_cap double precision;
  token_last_updated timestamp with time zone;
BEGIN
  -- Fetch search_ids and their last updated timestamps
  SELECT string_agg(subquery.search_id, ',')
  INTO search_ids
  FROM (
    SELECT 
      search_id,
      last_updated
    FROM 
      public.token_index
    WHERE 
      is_active = TRUE
      AND source = 'coingecko'
      AND NOW() - last_updated >= INTERVAL '1 min'
    ORDER BY last_updated ASC
    LIMIT 100
  ) AS subquery;

  -- Check if there are any search_ids to update
  IF search_ids IS NOT NULL THEN
    -- Create the API URL with the comma-separated list of search_ids
    api_url := 'https://pro-api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=' || search_ids || '&x_cg_pro_api_key=' || api_key;

    -- Make the HTTP request and parse the JSON response
    json_response := (SELECT content::jsonb FROM http_get(api_url));

    -- Loop over the returned tokens and insert into the historical table
    FOR token IN SELECT jsonb_array_elements(json_response)
    LOOP
      -- Extract the required fields from the JSON with NULL checks
      token_id := token->>'id';
      token_price := COALESCE(NULLIF((token->>'current_price')::text, 'null')::double precision, 0);
      token_total_supply := COALESCE(NULLIF((token->>'total_supply')::text, 'null')::double precision, 0);
      token_volume_24h := COALESCE(NULLIF((token->>'total_volume')::text, 'null')::double precision, 0);
      token_market_cap := COALESCE(NULLIF((token->>'market_cap')::text, 'null')::double precision, 0);
      token_fully_diluted_market_cap := COALESCE(NULLIF((token->>'fully_diluted_valuation')::text, 'null')::double precision, 0);
      token_last_updated := COALESCE((token->>'last_updated')::timestamp with time zone, NOW());

      -- Only proceed with insertion if token_price or other key values are valid
      IF token_price IS NOT NULL THEN
        -- Insert the data into the token_quotes_historical table
        INSERT INTO public.token_quotes_historical (
          source, 
          search_id, 
          price, 
          total_supply,
          volume_24h, 
          market_cap, 
          fully_diluted_market_cap, 
          last_updated
        ) VALUES (
          'coingecko', 
          token_id, 
          token_price, 
          token_total_supply,
          token_volume_24h, 
          token_market_cap, 
          token_fully_diluted_market_cap, 
          token_last_updated
        );

        -- Insert or update the data into the token_quotes_archive table
        INSERT INTO public.token_quotes_archive (
            source, 
            search_id, 
            price, 
            total_supply,
            volume_24h, 
            market_cap, 
            fully_diluted_market_cap, 
            last_updated
        ) VALUES (
            'coingecko', 
            token_id, 
            token_price, 
            token_total_supply,
            token_volume_24h, 
            token_market_cap, 
            token_fully_diluted_market_cap, 
            date_trunc('day', token_last_updated) -- Round to day
        )
        ON CONFLICT (source, search_id, last_updated)
        DO UPDATE
        SET 
            price = EXCLUDED.price, 
            total_supply = EXCLUDED.total_supply,
            volume_24h = EXCLUDED.volume_24h, 
            market_cap = EXCLUDED.market_cap, 
            fully_diluted_market_cap = EXCLUDED.fully_diluted_market_cap;
      END IF;
    END LOOP;
  ELSE
    -- No search_ids to update
    RETURN;
  END IF;
END;
$$;

-- Drop the existing scheduled job if it exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'token_quotes_historical_coinmarketcap_job') THEN
        PERFORM cron.unschedule('token_quotes_historical_coinmarketcap_job');
    END IF;
END
$$;

-- Cron Job for Coinmarketcap
SELECT cron.schedule(
    'token_quotes_historical_coinmarketcap_job', 
    '* * * * *', -- Every 1 min          
    'SELECT update_token_quotes_historical_coinmarketcap();'
);

-- Drop the existing scheduled job if it exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'token_quotes_historical_coingecko_job') THEN
        PERFORM cron.unschedule('token_quotes_historical_coingecko_job');
    END IF;
END
$$;

-- Cron Job for Coingecko
SELECT cron.schedule(
    'token_quotes_historical_coingecko_job',  
    '* * * * *', -- Every 1 min
    'SELECT update_token_quotes_historical_coingecko();'
);

-- Test manual calls
-- SELECT update_token_quotes_historical_coinmarketcap();
-- SELECT update_token_quotes_historical_coingecko();

-- Drop all function variations
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT oid::regprocedure AS func_signature
        FROM pg_proc
        WHERE proname = 'clean_up_token_quotes_historical'
    LOOP
        EXECUTE 'DROP FUNCTION ' || r.func_signature || ' CASCADE';
    END LOOP;
END $$;

-- Clean up token_quotes_historical
CREATE OR REPLACE FUNCTION clean_up_token_quotes_historical()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  -- Delete rows in token_quotes_historical where there are multiple entries with the same source and search_id
  DELETE FROM public.token_quotes_historical
  WHERE track_id IN (
    SELECT track_id
    FROM (
      SELECT 
        track_id,
        ROW_NUMBER() OVER (PARTITION BY source, search_id ORDER BY last_updated DESC) AS rn
      FROM 
        public.token_quotes_historical
    ) AS subquery
    WHERE rn > 1
  );
END;
$$;

-- Schedule the clean up function for token_quotes_historical
SELECT cron.schedule(
    'clean_up_token_quotes_historical',     
    '* * * * *', -- Every 1 min
    'SELECT clean_up_token_quotes_historical();' 
);

-- Test manual call
-- SELECT clean_up_token_quotes_historical();

-- Drop all function variations
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT oid::regprocedure AS func_signature
        FROM pg_proc
        WHERE proname = 'clean_up_token_quotes_archive'
    LOOP
        EXECUTE 'DROP FUNCTION ' || r.func_signature || ' CASCADE';
    END LOOP;
END $$;

-- Clean Token Quotes Archive
CREATE OR REPLACE FUNCTION clean_up_token_quotes_archive() RETURNS VOID AS $$
BEGIN
    -- Delete rows that are not the maximum `last_updated` for each combination of source, search_id, and day
    DELETE FROM public.token_quotes_archive tq
    WHERE (tq.source, tq.search_id, tq.last_updated) NOT IN (
        WITH ranked_rows AS (
            SELECT 
                source,
                search_id,
                MAX(last_updated) AS max_last_updated
            FROM 
                public.token_quotes_archive
            GROUP BY 
                source, 
                search_id, 
                DATE_TRUNC('day', last_updated)
        )
        SELECT 
            source, 
            search_id, 
            max_last_updated
        FROM 
            ranked_rows
    );
END;
$$ LANGUAGE plpgsql;

-- Test manual call
-- SELECT clean_up_token_quotes_archive();

-- Drop the existing scheduled job if it exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'clean_up_token_quotes_archive_job') THEN
        PERFORM cron.unschedule('clean_up_token_quotes_archive_job');
    END IF;
END
$$;

-- Schedule the clean up function for token_quotes_archive
SELECT cron.schedule(   
    'clean_up_token_quotes_archive_job',          
    '0 * * * *', -- Every hour
    'SELECT clean_up_token_quotes_archive();' 
);

-- Test manual call
-- SELECT clean_up_token_quotes_archive();

-- Drop all function variations
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT oid::regprocedure AS func_signature
        FROM pg_proc
        WHERE proname = 'update_token_index_last_updated'
    LOOP
        EXECUTE 'DROP FUNCTION ' || r.func_signature || ' CASCADE';
    END LOOP;
END $$;

-- Trigger Function
CREATE OR REPLACE FUNCTION update_token_index_last_updated()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  -- Update the last_updated column in token_index table
  UPDATE public.token_index
  SET last_updated = NOW()
  WHERE source = NEW.source
    AND search_id = NEW.search_id;

  -- Return the new record for further processing (if needed)
  RETURN NEW;
END;
$$;

-- Trigger
CREATE TRIGGER trigger_token_index_last_updated
AFTER INSERT ON public.token_quotes_historical
FOR EACH ROW
EXECUTE FUNCTION update_token_index_last_updated();

-- Enable row level security on token_quotes_archive table
alter table "token_quotes_archive" enable row level security;

-- Grant read access to all users
drop policy if exists "Read access to token_quotes_archive" on token_quotes_archive;
create policy "Read access to token_quotes_archive"
on token_quotes_archive for select
to authenticated, anon
using ( true );

-- Enable row level security on token_quotes_historical table
alter table "token_quotes_historical" enable row level security;

-- Grant read access to all users
drop policy if exists "Read access to token_quotes_historical" on token_quotes_historical;
create policy "Read access to token_quotes_historical"
on token_quotes_historical for select
to authenticated, anon
using ( true );