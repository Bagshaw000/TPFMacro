-- ============================================================================
-- Migration: add a `period` column to the LSE economic-indicator tables and
-- switch the uniqueness key from (report_date, country_code) to
-- (country_code, period).
--
-- WHY
--   A reading's real identity is (country, reference month), not (country,
--   release date). A flash estimate, the final print and any later revision
--   for the same month are three publications of the same data point. Keyed on
--   report_date they pile up as separate rows; keyed on `period` they upsert
--   onto one row. `period` is "YYYY-MM"; controller/lse_.py::process_event
--   sets it exactly from period_hint for every new row.
--
-- HOW TO RUN
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f sql/add_period_column.sql
--   ON_ERROR_STOP=1 makes psql abort + roll back on any error. The migration
--   is one BEGIN/COMMIT (Postgres does transactional DDL), so it either fully
--   applies or fully rolls back. Re-running after success is a no-op.
--   Deploy the code changes AFTER this migration succeeds, not before.
--
-- SCOPE
--   The five tables this pipeline writes: cpi, ppi, unemp, inflation, retail.
--   Add 'gdp' / 'lse' to the FOREACH array if you want them constrained too.
--
-- WHAT THE DO BLOCK DOES, per table:
--   0. Abort (RAISE EXCEPTION) if any row has NULL report_date - it couldn't
--      get a period, and step 4 would then fail half-migrated.
--   1. ADD COLUMN period text (nullable for now).
--   2. Backfill historical rows: period = report_date's month minus
--      `lag_months`. This is an APPROXIMATION - we don't have period_hint for
--      old rows. lag_months = 1 fits on-time CPI/PPI/inflation/retail prints;
--      a late release (Dec data published Feb) gets mislabelled by a month,
--      and some `unemp` series print in their own reference month (lag 0).
--      Sanity-check with pre-flight (d) and set lag_months accordingly.
--   3. Delete duplicate rows: within each (country_code, period) keep the
--      newest report_date; on a tie keep the lowest id.
--   4. ALTER COLUMN period SET NOT NULL.
--   5. Drop the old UNIQUE constraint on exactly {country_code, report_date},
--      whatever it is named. NOTE: a key created as a bare `CREATE UNIQUE
--      INDEX` (not a table constraint) is NOT handled - drop it by hand; see
--      pre-flight (c).
--   6. Add UNIQUE (country_code, period) as "<table>_country_period_unique",
--      unless it already exists (re-run safe). This constraint's own index
--      covers (country_code, period) reads - no separate CREATE INDEX needed.
--
-- PRE-FLIGHT - run each of these on its own first; they change nothing:
--
--   a) Rows that would abort the migration (fix these first):
--        SELECT 'cpi' t, count(*) FROM cpi WHERE report_date IS NULL
--        UNION ALL SELECT 'ppi',       count(*) FROM ppi       WHERE report_date IS NULL
--        UNION ALL SELECT 'unemp',     count(*) FROM unemp     WHERE report_date IS NULL
--        UNION ALL SELECT 'inflation', count(*) FROM inflation WHERE report_date IS NULL
--        UNION ALL SELECT 'retail',    count(*) FROM retail    WHERE report_date IS NULL;
--
--   b) How many rows step 3 would delete (per table, swap the name):
--        SELECT count(*) FROM (
--          SELECT row_number() OVER (
--            PARTITION BY country_code,
--              to_char(date_trunc('month', report_date) - interval '1 month', 'YYYY-MM')
--            ORDER BY report_date DESC, id ASC) rn
--          FROM cpi) x WHERE rn > 1;
--
--   c) Is the current key a table constraint or a bare index? The migration
--      drops the constraint form only:
--        SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint
--        WHERE conrelid = 'cpi'::regclass AND contype = 'u';
--        SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'cpi';
--
--   d) Sanity-check the backfill guess against real release dates:
--        SELECT country_code, report_date,
--          to_char(date_trunc('month', report_date) - interval '1 month', 'YYYY-MM') AS guessed
--        FROM unemp ORDER BY country_code, report_date DESC LIMIT 40;
--
-- VERIFY before COMMIT - each should return 0 rows:
--        SELECT 'cpi' t, country_code, period, count(*) FROM cpi       GROUP BY 1,2,3 HAVING count(*) > 1
--        UNION ALL SELECT 'ppi',       country_code, period, count(*) FROM ppi       GROUP BY 1,2,3 HAVING count(*) > 1
--        UNION ALL SELECT 'unemp',     country_code, period, count(*) FROM unemp     GROUP BY 1,2,3 HAVING count(*) > 1
--        UNION ALL SELECT 'inflation', country_code, period, count(*) FROM inflation GROUP BY 1,2,3 HAVING count(*) > 1
--        UNION ALL SELECT 'retail',    country_code, period, count(*) FROM retail    GROUP BY 1,2,3 HAVING count(*) > 1;
--   If anything looks wrong, run ROLLBACK; instead of COMMIT;.
--
-- AFTER
--   The "<table>_recent" views do NOT pick up `period` on their own - a view's
--   SELECT * is frozen at creation. The pipeline still works without it
--   (period is optional in the model; those views are only read for
--   country_code / report_date). To expose it:
--        SELECT pg_get_viewdef('cpi_recent', true);   -- inspect
--        CREATE OR REPLACE VIEW cpi_recent AS ...;     -- re-declare with period
-- ============================================================================


BEGIN;

DO $$
DECLARE
    tbl          text;
    old_name     text;
    null_count   bigint;
    lag_months   int := 1;
BEGIN
    FOREACH tbl IN ARRAY ARRAY['cpi', 'ppi', 'unemp', 'inflation', 'retail']
    LOOP
        EXECUTE format('SELECT count(*) FROM %I WHERE report_date IS NULL', tbl)
            INTO null_count;
        IF null_count > 0 THEN
            RAISE EXCEPTION
                '% has % row(s) with NULL report_date - fix those first', tbl, null_count;
        END IF;

        EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS period text', tbl);

        EXECUTE format($f$
            UPDATE %I
            SET period = to_char(
                date_trunc('month', report_date) - make_interval(months => %s),
                'YYYY-MM')
            WHERE period IS NULL
        $f$, tbl, lag_months);

        EXECUTE format($f$
            DELETE FROM %I a
            USING %I b
            WHERE a.country_code = b.country_code
              AND a.period = b.period
              AND (a.report_date < b.report_date
                   OR (a.report_date = b.report_date AND a.id > b.id))
        $f$, tbl, tbl);

        EXECUTE format('ALTER TABLE %I ALTER COLUMN period SET NOT NULL', tbl);

        FOR old_name IN
            SELECT con.conname
            FROM pg_constraint con
            WHERE con.conrelid = format('%I', tbl)::regclass
              AND con.contype = 'u'
              AND (
                  SELECT array_agg(att.attname::text ORDER BY att.attname::text)
                  FROM pg_attribute att
                  WHERE att.attrelid = con.conrelid
                    AND att.attnum = ANY(con.conkey)
              ) = ARRAY['country_code', 'report_date']
        LOOP
            EXECUTE format('ALTER TABLE %I DROP CONSTRAINT %I', tbl, old_name);
        END LOOP;

        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conrelid = format('%I', tbl)::regclass
              AND conname = tbl || '_country_period_unique'
        ) THEN
            EXECUTE format(
                'ALTER TABLE %I ADD CONSTRAINT %I UNIQUE (country_code, period)',
                tbl, tbl || '_country_period_unique');
        END IF;

        RAISE NOTICE 'migrated %', tbl;
    END LOOP;
END $$;

COMMIT;
