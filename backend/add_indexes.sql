-- Run this against your Railway PostgreSQL database to add missing indexes
-- These indexes will significantly speed up insurance-related carrier queries

-- Ensure pg_trgm extension is available (needed for existing trigram indexes)
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Expression indexes for integer-cast range filters on new_ventures
CREATE INDEX IF NOT EXISTS idx_new_ventures_total_pwr_int ON new_ventures((NULLIF(total_pwr, '')::int)) WHERE total_pwr IS NOT NULL AND total_pwr != '';
CREATE INDEX IF NOT EXISTS idx_new_ventures_total_drivers_int ON new_ventures((NULLIF(total_drivers, '')::int)) WHERE total_drivers IS NOT NULL AND total_drivers != '';

-- Composite covering index on insurance_history for carrier join pattern
-- This is the MOST IMPORTANT index - it covers the docket+type+cancellation lookup
-- that every insurance filter uses
CREATE INDEX IF NOT EXISTS idx_ih_docket_type_cancl ON insurance_history(docket_number, ins_type_desc, cancl_effective_date);

-- Insurance history company name lookup
CREATE INDEX IF NOT EXISTS idx_ih_docket_company ON insurance_history(docket_number, name_company);

-- Insurance history effective date for date range filters
CREATE INDEX IF NOT EXISTS idx_ih_docket_effective ON insurance_history(docket_number, effective_date);

-- Verify indexes were created
SELECT indexname, tablename FROM pg_indexes
WHERE indexname IN (
    'idx_new_ventures_total_pwr_int',
    'idx_new_ventures_total_drivers_int',
    'idx_ih_docket_type_cancl',
    'idx_ih_docket_company',
    'idx_ih_docket_effective'
);
