-- ============================================================================
-- Insurance History Performance Optimization
-- Switch matching from docket_number (varchar) to dot_number (integer-friendly),
-- add parsed date columns for fast range filtering, and create supporting
-- composite indexes.
--
-- Safe to run multiple times (all IF NOT EXISTS / IF EXISTS guards).
-- ============================================================================

BEGIN;

-- 1. Parsed date columns (real DATE type) – populated from the MM/DD/YYYY
--    string columns.  We use plain columns (not GENERATED) because the
--    source strings may contain garbage values; a trigger keeps them in sync.
ALTER TABLE insurance_history
    ADD COLUMN IF NOT EXISTS effective_date_parsed DATE,
    ADD COLUMN IF NOT EXISTS cancl_date_parsed     DATE;

-- 2. One-off backfill – only touch rows where the parsed value is still NULL
--    so re-running the migration is cheap.
UPDATE insurance_history
SET effective_date_parsed = CASE
        WHEN effective_date IS NOT NULL
         AND effective_date LIKE '%/%/%'
         AND length(effective_date) BETWEEN 8 AND 10
        THEN to_date(effective_date, 'MM/DD/YYYY')
        ELSE NULL
    END
WHERE effective_date_parsed IS NULL
  AND effective_date IS NOT NULL
  AND effective_date <> '';

UPDATE insurance_history
SET cancl_date_parsed = CASE
        WHEN cancl_effective_date IS NOT NULL
         AND cancl_effective_date LIKE '%/%/%'
         AND length(cancl_effective_date) BETWEEN 8 AND 10
        THEN to_date(cancl_effective_date, 'MM/DD/YYYY')
        ELSE NULL
    END
WHERE cancl_date_parsed IS NULL
  AND cancl_effective_date IS NOT NULL
  AND cancl_effective_date <> '';

-- 3. Trigger to keep parsed columns in sync on INSERT / UPDATE
CREATE OR REPLACE FUNCTION _ih_sync_parsed_dates() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.effective_date IS NOT NULL
       AND NEW.effective_date LIKE '%/%/%'
       AND length(NEW.effective_date) BETWEEN 8 AND 10 THEN
        BEGIN
            NEW.effective_date_parsed := to_date(NEW.effective_date, 'MM/DD/YYYY');
        EXCEPTION WHEN OTHERS THEN
            NEW.effective_date_parsed := NULL;
        END;
    ELSE
        NEW.effective_date_parsed := NULL;
    END IF;

    IF NEW.cancl_effective_date IS NOT NULL
       AND NEW.cancl_effective_date LIKE '%/%/%'
       AND length(NEW.cancl_effective_date) BETWEEN 8 AND 10 THEN
        BEGIN
            NEW.cancl_date_parsed := to_date(NEW.cancl_effective_date, 'MM/DD/YYYY');
        EXCEPTION WHEN OTHERS THEN
            NEW.cancl_date_parsed := NULL;
        END;
    ELSE
        NEW.cancl_date_parsed := NULL;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_ih_sync_parsed_dates ON insurance_history;
CREATE TRIGGER trg_ih_sync_parsed_dates
    BEFORE INSERT OR UPDATE OF effective_date, cancl_effective_date
    ON insurance_history
    FOR EACH ROW
    EXECUTE FUNCTION _ih_sync_parsed_dates();

-- 4. Core index on dot_number – this is the new join key
CREATE INDEX IF NOT EXISTS idx_ih_dot_number
    ON insurance_history (dot_number);

-- 5. Composite indexes that cover the hottest filter combinations
CREATE INDEX IF NOT EXISTS idx_ih_dot_type_cancl
    ON insurance_history (dot_number, ins_type_desc, cancl_date_parsed);

CREATE INDEX IF NOT EXISTS idx_ih_dot_effective
    ON insurance_history (dot_number, effective_date_parsed);

CREATE INDEX IF NOT EXISTS idx_ih_dot_cancl
    ON insurance_history (dot_number, cancl_date_parsed);

CREATE INDEX IF NOT EXISTS idx_ih_dot_company
    ON insurance_history (dot_number, name_company);

-- 6. Plain date indexes (helpful for default / unfiltered dashboards)
CREATE INDEX IF NOT EXISTS idx_ih_effective_parsed
    ON insurance_history (effective_date_parsed)
    WHERE effective_date_parsed IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_ih_cancl_parsed
    ON insurance_history (cancl_date_parsed)
    WHERE cancl_date_parsed IS NOT NULL;

COMMIT;

-- 7. Refresh planner stats so the new indexes are used immediately
ANALYZE insurance_history;
