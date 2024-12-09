-- @note: Update the <BASE_FRONTEND_URL> with your own frontend URL before running this SQL script

-- Drop all function variations
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT oid::regprocedure AS func_signature
        FROM pg_proc
        WHERE proname = 'sync_market_userdata'
    LOOP
        EXECUTE 'DROP FUNCTION ' || r.func_signature || ' CASCADE';
    END LOOP;
END $$;

-- Create the function to call the API
CREATE OR REPLACE FUNCTION sync_market_userdata()
RETURNS void AS $$
DECLARE
    api_url text := '<BASE_FRONTEND_URL>/api/sync/market';
    json_response jsonb;
BEGIN
    -- Make the HTTP request
    json_response := (SELECT content::jsonb FROM http_get(api_url));
    
    -- Log the response (optional)
    RAISE NOTICE 'API Response: %', json_response;
    
EXCEPTION WHEN OTHERS THEN
    -- Log any errors
    RAISE NOTICE 'Error calling API: %', SQLERRM;
END;
$$ LANGUAGE plpgsql;

-- Drop the existing scheduled job if it exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'sync_market_userdata_job') THEN
        PERFORM cron.unschedule('sync_market_userdata_job');
    END IF;
END
$$;

-- Create the scheduled job to run every minute 
SELECT cron.schedule(
    'sync_market_userdata_job',    
   '* * * * *',  -- Every 1 min          
    'SELECT sync_market_userdata();'
);

-- Sync Tokens
-- Drop existing function if it exists
DROP FUNCTION IF EXISTS sync_token_index();

-- Create the function to call the API
CREATE OR REPLACE FUNCTION sync_token_index()
RETURNS void AS $$
DECLARE
    api_url text := '<BASE_FRONTEND_URL>/api/sync/token';
    json_response jsonb;
BEGIN
    -- Make the HTTP request
    json_response := (SELECT content::jsonb FROM http_get(api_url));
    
    -- Log the response (optional)
    RAISE NOTICE 'API Response: %', json_response;
    
EXCEPTION WHEN OTHERS THEN
    -- Log any errors
    RAISE NOTICE 'Error calling API: %', SQLERRM;
END;
$$ LANGUAGE plpgsql;

-- Drop the existing scheduled job if it exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'sync_token_index_job') THEN
        PERFORM cron.unschedule('sync_token_index_job');
    END IF;
END
$$;

-- Create the scheduled job to run every minute 
SELECT cron.schedule(
    'sync_token_index_job',    
    '* * * * *',  -- Every 1 min  
    'SELECT sync_token_index();'
);