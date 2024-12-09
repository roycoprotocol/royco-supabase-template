-- @note: Update the <BASE_FRONTEND_URL> with your own frontend URL before running this SQL script

-- Drop all function variations
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT oid::regprocedure AS func_signature
        FROM pg_proc
        WHERE proname = 'calculate_native_annual_change_ratio'
    LOOP
        EXECUTE 'DROP FUNCTION ' || r.func_signature || ' CASCADE';
    END LOOP;
END $$;

-- Create function
CREATE OR REPLACE FUNCTION calculate_native_annual_change_ratio()
RETURNS TRIGGER AS $$
BEGIN
    -- Only calculate if all required fields are not null
    IF (
        NEW.prev_total_assets IS NOT NULL AND
        NEW.prev_total_supply IS NOT NULL AND
        NEW.prev_block_timestamp IS NOT NULL AND
        NEW.curr_total_assets IS NOT NULL AND
        NEW.curr_total_supply IS NOT NULL AND
        NEW.curr_block_timestamp IS NOT NULL
    ) THEN
        -- Calculate share values
        DECLARE
            prev_share_value numeric;
            curr_share_value numeric;
            growth_rate numeric;
            annual_period numeric = 365 * 24 * 60 * 60;
            time_period numeric;
        BEGIN
            -- Check for division by zero in share value calculations
            IF NEW.prev_total_supply = '0' OR NEW.curr_total_supply = '0' THEN
                NEW.native_annual_change_ratio := NULL;
                RAISE NOTICE 'Cannot calculate ratio: division by zero in total_supply';
            ELSE
                prev_share_value := NEW.prev_total_assets::NUMERIC / NEW.prev_total_supply::NUMERIC;
                curr_share_value := NEW.curr_total_assets::NUMERIC / NEW.curr_total_supply::NUMERIC;
                
                -- Calculate time period
                time_period := NEW.curr_block_timestamp::NUMERIC - NEW.prev_block_timestamp::NUMERIC;
                
                -- Check for division by zero in time period
                IF time_period = 0 THEN
                    NEW.native_annual_change_ratio := NULL;
                    RAISE NOTICE 'Cannot calculate ratio: time period is zero';
                ELSE
                    -- Check for division by zero in growth rate calculation
                    IF prev_share_value = 0 THEN
                        NEW.native_annual_change_ratio := NULL;
                        RAISE NOTICE 'Cannot calculate ratio: previous share value is zero';
                    ELSE
                        -- Calculate growth rate
                        growth_rate := (curr_share_value - prev_share_value) / prev_share_value;
                        
                        -- Calculate native annual change ratio
                        NEW.native_annual_change_ratio := POWER(1 + growth_rate, annual_period / time_period) - 1;
                    END IF;
                END IF;
            END IF;
        END;
    ELSE
        NEW.native_annual_change_ratio := NULL;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop existing trigger if it exists
DROP TRIGGER IF EXISTS calculate_native_annual_change_ratio_trigger ON public.raw_underlying_vaults;

-- Create the trigger
CREATE TRIGGER calculate_native_annual_change_ratio_trigger
    BEFORE INSERT OR UPDATE
    ON public.raw_underlying_vaults
    FOR EACH ROW
    EXECUTE FUNCTION calculate_native_annual_change_ratio();

-- Install the pg_net extension (needs superuser privileges)
CREATE EXTENSION IF NOT EXISTS pg_net;

-- Drop all function variations
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT oid::regprocedure AS func_signature
        FROM pg_proc
        WHERE proname = 'insert_underlying_vaults'
    LOOP
        EXECUTE 'DROP FUNCTION ' || r.func_signature || ' CASCADE';
    END LOOP;
END $$;

-- Create function
CREATE OR REPLACE FUNCTION insert_underlying_vaults()
RETURNS void AS $$
BEGIN
  -- First do the insert operation
  INSERT INTO raw_underlying_vaults (
    chain_id,
    underlying_vault_address,
    last_updated,
    retries
  )
  SELECT 
    rm.chain_id,
    rm.underlying_vault_address,
    CURRENT_TIMESTAMP as last_updated,
    0 as retries
  FROM raw_markets rm
  WHERE 
    rm.underlying_vault_address IS NOT NULL 
    AND rm.underlying_vault_address != '0x0000000000000000000000000000000000000000'
    AND rm.chain_id IS NOT NULL
    AND NOT EXISTS (
      SELECT 1 
      FROM raw_underlying_vaults ruv 
      WHERE ruv.underlying_vault_address = rm.underlying_vault_address
      AND ruv.chain_id = rm.chain_id
    )
  ON CONFLICT (chain_id, underlying_vault_address) DO NOTHING;

  -- Make the POST request using pg_net
  PERFORM net.http_post(
    url := '<BASE_FRONTEND_URL>/api/native',
    headers := '{"Content-Type": "application/json"}'::jsonb
  );

END;
$$ LANGUAGE plpgsql;

-- Drop the existing scheduled job if it exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'insert_underlying_vaults_job') THEN
        PERFORM cron.unschedule('insert_underlying_vaults_job');
    END IF;
END
$$;

-- Create the scheduled job to run every minute
SELECT cron.schedule(
  'insert_underlying_vaults_job',  
  '* * * * *',  -- Every 1 min
  'SELECT insert_underlying_vaults();'
);