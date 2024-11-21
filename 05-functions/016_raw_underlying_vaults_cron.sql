-- @note: Update the <BASE_FRONTEND_URL> (Eg: https://royco-testnet.vercel.app) with your own frontend URL before running this SQL script

-- First, install the pg_net extension (needs superuser privileges)
CREATE EXTENSION IF NOT EXISTS pg_net;

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

-- Create the scheduled job to run every minute
SELECT cron.schedule(
  'insert_underlying_vaults_job',  -- job name
  '* * * * *',                    -- cron expression (every minute)
  'SELECT insert_underlying_vaults();'
);

-- External trigger
SELECT insert_underlying_vaults();