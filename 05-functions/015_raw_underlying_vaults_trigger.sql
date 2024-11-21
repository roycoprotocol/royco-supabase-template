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
DROP TRIGGER IF EXISTS trigger_calculate_native_annual_change_ratio ON public.raw_underlying_vaults;

-- Create the trigger
CREATE TRIGGER trigger_calculate_native_annual_change_ratio
    BEFORE INSERT OR UPDATE
    ON public.raw_underlying_vaults
    FOR EACH ROW
    EXECUTE FUNCTION calculate_native_annual_change_ratio();