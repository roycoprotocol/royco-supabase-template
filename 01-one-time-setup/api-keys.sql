-- First create the schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS private;

-- Drop the table if it already exists
-- DROP TABLE IF EXISTS private.anon_api_keys;

-- Then create the table
CREATE TABLE IF NOT EXISTS private.anon_api_keys (
    id uuid NOT NULL DEFAULT gen_random_uuid(),
    name text NOT NULL,
    description text NOT NULL DEFAULT '',
    CONSTRAINT anon_api_keys_pkey PRIMARY KEY (id)
) TABLESPACE pg_default;

-- Request checker function
CREATE OR REPLACE FUNCTION public.check_request()
  RETURNS void
  LANGUAGE plpgsql
  SECURITY DEFINER
AS $$
DECLARE
  req_app_api_key text := current_setting('request.headers', true)::json->>'x-royco-api-key';
  is_app_api_key_registered boolean;
  jwt_role text := current_setting('request.jwt.claims', true)::json->>'role';
BEGIN
  -- RETURN;

  IF jwt_role <> 'anon' THEN
    -- not `anon` role, allow the request to pass
    RETURN;
  END IF;

  SELECT
    true INTO is_app_api_key_registered
  FROM private.anon_api_keys
  WHERE
    id = req_app_api_key::uuid
  LIMIT 1;

  IF is_app_api_key_registered IS true THEN
    -- api key is registered, allow the request to pass
    RETURN;
  END IF;

  RAISE SQLSTATE 'PGRST' USING
    MESSAGE = json_build_object(
      'message', 'No registered API key found in x-royco-api-key header.')::text,
    DETAIL = json_build_object(
      'status', 403)::text;
END;
$$;

-- Check every request
ALTER ROLE authenticator
  SET pgrst.db_pre_request = 'public.check_request';

NOTIFY pgrst, 'reload config';