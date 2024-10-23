-- @note: Update the <COINMARKETCAP_API_KEY> with your own key before running this SQL script

-- Drop existing materialized view
DROP MATERIALIZED VIEW IF EXISTS token_quotes_latest;

-- Drop existing archive table
DROP TABLE IF EXISTS public.token_quotes_archive;

-- Create archive table
create table
  public.token_quotes_archive (
    source text not null,
    search_id text not null,
    price double precision not null,
    total_supply double precision not null,
    volume_24h double precision not null,
    market_cap double precision not null,
    fully_diluted_market_cap double precision not null,
    last_updated timestamp with time zone not null,
    constraint token_quotes_archive_pkey primary key (source, search_id, last_updated)
  ) tablespace pg_default;

-- Drop existing historical table
DROP TABLE IF EXISTS public.token_quotes_historical;

-- Create historical table
create table
  public.token_quotes_historical (
    track_id uuid not null default gen_random_uuid (),
    source text not null,
    search_id text not null,
    price double precision not null,
    total_supply double precision not null,
    volume_24h double precision not null,
    market_cap double precision not null,
    fully_diluted_market_cap double precision not null,
    last_updated timestamp with time zone not null,
    constraint token_quotes_historical_pkey primary key (track_id)
  ) tablespace pg_default;

-- Update Function
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


-- Cron Schedule
SELECT cron.schedule(
    'cron_token_quotes_historical_coinmarketcap', -- Cron job name 
    '* * * * *', -- Cron syntax for every min          
    $$SELECT update_token_quotes_historical_coinmarketcap();$$
);

SELECT update_token_quotes_historical_coinmarketcap();

-- Create Materialized View
CREATE MATERIALIZED VIEW token_quotes_latest AS
WITH 
t1 AS (
  SELECT 
    token_id,
    decimals,
    source || '-' || search_id AS match_key
  FROM
    token_index
  WHERE
    is_active = TRUE
),
t2 AS (
  SELECT 
    subquery.match_key,
    subquery.price,
    subquery.total_supply,
    subquery.volume_24h,
    subquery.market_cap,
    subquery.fully_diluted_market_cap,
    subquery.last_updated
  FROM (
    SELECT 
      source || '-' || search_id AS match_key,
      price,
      total_supply,
      volume_24h,
      market_cap,
      fully_diluted_market_cap,
      last_updated,
      ROW_NUMBER() OVER (PARTITION BY source || '-' || search_id ORDER BY last_updated DESC) as rn
    FROM 
      token_quotes_historical
  ) subquery
  WHERE subquery.rn = 1
)
SELECT  
  t1.token_id,
  t1.decimals,
  t2.price::NUMERIC,
  t2.total_supply::NUMERIC as total_supply,
  t2.fully_diluted_market_cap::NUMERIC AS fdv
FROM
  t1 
LEFT JOIN 
  t2 
ON 
  t1.match_key = t2.match_key
WHERE 
  t2.match_key IS NOT NULL 
  AND t2.price IS NOT NULL
  AND t2.volume_24h IS NOT NULL
  AND t2.market_cap IS NOT NULL
  AND t2.fully_diluted_market_cap IS NOT NULL
  AND t2.last_updated IS NOT NULL;

-- Cron for refreshing token_quotes_latest
SELECT cron.schedule(
    'refresh_token_quotes_latest', -- Cron job name      
    '* * * * *', -- Cron syntax for every minute
    $$REFRESH MATERIALIZED VIEW token_quotes_latest;$$ -- The command to refresh the view
);

-- Test manual refresh
-- REFRESH MATERIALIZED VIEW token_quotes_latest;

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
    'clean_up_token_quotes_historical', -- Cron job name     
    '* * * * *', -- Cron syntax for every minute
    $$SELECT clean_up_token_quotes_historical();$$ -- The command to call
);

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

-- Schedule the clean up function for token_quotes_archive
SELECT cron.schedule(   
    'clean_up_token_quotes_archive',  -- Cron job name         
    '0 * * * *', -- Cron syntax for every hour
    $$SELECT clean_up_token_quotes_archive();$$ -- The command to call
);

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
