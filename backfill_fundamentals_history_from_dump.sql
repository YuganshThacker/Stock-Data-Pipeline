-- Backfill missing values in `public.fundamentals_history` using the scraped dataset from `dump.sql`.
--
-- Assumptions
-- 1) `dump.sql` has already been loaded into THIS same Postgres DB and the scraped tables exist,
--    especially `public.company_data`.
-- 2) `public.fundamentals_history` exists and has (at minimum) these columns:
--    - symbol (text)
--    - fiscal_year (int)
--    - revenue (numeric)
--    - net_profit (numeric)
--    - eps (numeric)
--    - pe_ratio (numeric)
--    - roe (numeric)
--    - roce (numeric)
-- 3) `fiscal_year = 1900` is used as a stub row for `TTM` in your system, and should be skipped.
--
-- JSON keys used from `public.company_data.profit_loss` (confirmed via parsing dump.sql):
--  - revenue: profit_loss->'sales' (fallback: 'revenue', 'revenue_from_operations')
--  - net profit: profit_loss->'net_profit' (fallback: 'profit_after_tax')
--  - eps: profit_loss->'eps_in_rs'
--  - ebitda: computed as operating_profit + depreciation (both are year-keyed in profit_loss)
--  - fiscal-year keys format: 'Mar YYYY'
--
-- Safety: only fills NULL cells; it does not overwrite existing non-NULL values.

BEGIN;

-- 1) Fill per-year revenue/net_profit/eps from `profit_loss`
WITH
src AS (
  SELECT
    symbol,
    pe,
    roe,
    roce,
    profit_loss
  FROM public.company_data
  WHERE symbol IS NOT NULL
),
rev AS (
  -- Priority: sales -> revenue -> revenue_from_operations
  SELECT
    s.symbol,
    (regexp_replace(e.k, '^Mar ([0-9]{4})$', '\1'))::int AS fiscal_year,
    NULLIF(e.v::text, 'null')::numeric AS revenue
  FROM src s
  CROSS JOIN LATERAL jsonb_each(s.profit_loss->'sales') AS e(k, v)
  WHERE
    (s.profit_loss ? 'sales')
    AND e.k ~ '^Mar [0-9]{4}$'

  UNION ALL

  SELECT
    s.symbol,
    (regexp_replace(e.k, '^Mar ([0-9]{4})$', '\1'))::int AS fiscal_year,
    NULLIF(e.v::text, 'null')::numeric AS revenue
  FROM src s
  CROSS JOIN LATERAL jsonb_each(s.profit_loss->'revenue') AS e(k, v)
  WHERE
    (NOT (s.profit_loss ? 'sales'))
    AND (s.profit_loss ? 'revenue')
    AND e.k ~ '^Mar [0-9]{4}$'

  UNION ALL

  SELECT
    s.symbol,
    (regexp_replace(e.k, '^Mar ([0-9]{4})$', '\1'))::int AS fiscal_year,
    NULLIF(e.v::text, 'null')::numeric AS revenue
  FROM src s
  CROSS JOIN LATERAL jsonb_each(s.profit_loss->'revenue_from_operations') AS e(k, v)
  WHERE
    (NOT (s.profit_loss ? 'sales'))
    AND (NOT (s.profit_loss ? 'revenue'))
    AND (s.profit_loss ? 'revenue_from_operations')
    AND e.k ~ '^Mar [0-9]{4}$'
),
np AS (
  -- Priority: net_profit -> profit_after_tax
  SELECT
    s.symbol,
    (regexp_replace(e.k, '^Mar ([0-9]{4})$', '\1'))::int AS fiscal_year,
    NULLIF(e.v::text, 'null')::numeric AS net_profit
  FROM src s
  CROSS JOIN LATERAL jsonb_each(s.profit_loss->'net_profit') AS e(k, v)
  WHERE
    (s.profit_loss ? 'net_profit')
    AND e.k ~ '^Mar [0-9]{4}$'

  UNION ALL

  SELECT
    s.symbol,
    (regexp_replace(e.k, '^Mar ([0-9]{4})$', '\1'))::int AS fiscal_year,
    NULLIF(e.v::text, 'null')::numeric AS net_profit
  FROM src s
  CROSS JOIN LATERAL jsonb_each(s.profit_loss->'profit_after_tax') AS e(k, v)
  WHERE
    (NOT (s.profit_loss ? 'net_profit'))
    AND (s.profit_loss ? 'profit_after_tax')
    AND e.k ~ '^Mar [0-9]{4}$'
),
eps_year AS (
  SELECT
    s.symbol,
    (regexp_replace(e.k, '^Mar ([0-9]{4})$', '\1'))::int AS fiscal_year,
    NULLIF(e.v::text, 'null')::numeric AS eps
  FROM src s
  CROSS JOIN LATERAL jsonb_each(s.profit_loss->'eps_in_rs') AS e(k, v)
  WHERE
    (s.profit_loss ? 'eps_in_rs')
    AND e.k ~ '^Mar [0-9]{4}$'
),
ex AS (
  SELECT
    t.symbol,
    t.fiscal_year,
    MAX(t.revenue) AS revenue,
    MAX(t.net_profit) AS net_profit,
    MAX(t.eps) AS eps
  FROM (
    SELECT symbol, fiscal_year, revenue, NULL::numeric AS net_profit, NULL::numeric AS eps FROM rev
    UNION ALL
    SELECT symbol, fiscal_year, NULL::numeric AS revenue, net_profit, NULL::numeric AS eps FROM np
    UNION ALL
    SELECT symbol, fiscal_year, NULL::numeric AS revenue, NULL::numeric AS net_profit, eps FROM eps_year
  ) t
  GROUP BY t.symbol, t.fiscal_year
)
UPDATE public.fundamentals_history fh
SET
  revenue = COALESCE(fh.revenue, ex.revenue),
  net_profit = COALESCE(fh.net_profit, ex.net_profit),
  eps = COALESCE(fh.eps, ex.eps)
FROM ex
WHERE
  fh.symbol = ex.symbol
  AND fh.fiscal_year = ex.fiscal_year
  AND fh.fiscal_year <> 1900
  AND (fh.revenue IS NULL OR fh.net_profit IS NULL OR fh.eps IS NULL);

-- 2) Fill base ratios that are not year-specific in dump.sql
UPDATE public.fundamentals_history fh
SET
  pe_ratio = COALESCE(fh.pe_ratio, cd.pe),
  roe = COALESCE(fh.roe, cd.roe),
  roce = COALESCE(fh.roce, cd.roce),
  debt_to_equity = COALESCE(fh.debt_to_equity, cd.debt_to_equity)
FROM public.company_data cd
WHERE
  fh.symbol = cd.symbol
  AND fh.fiscal_year <> 1900
  AND (fh.pe_ratio IS NULL OR fh.roe IS NULL OR fh.roce IS NULL OR fh.debt_to_equity IS NULL);

-- 3) Fill ebitda from `profit_loss`:
--    EBITDA (approx) = operating_profit + depreciation, for matching `Mar YYYY` fiscal keys.
WITH
op_year AS (
  SELECT
    s.symbol,
    (regexp_replace(e.k, '^Mar ([0-9]{4})$', '\1'))::int AS fiscal_year,
    NULLIF(e.v::text, 'null')::numeric AS operating_profit
  FROM public.company_data s
  CROSS JOIN LATERAL jsonb_each(s.profit_loss->'operating_profit') AS e(k, v)
  WHERE
    s.symbol IS NOT NULL
    AND (s.profit_loss ? 'operating_profit')
    AND e.k ~ '^Mar [0-9]{4}$'
),
dep_year AS (
  SELECT
    s.symbol,
    (regexp_replace(e.k, '^Mar ([0-9]{4})$', '\1'))::int AS fiscal_year,
    NULLIF(e.v::text, 'null')::numeric AS depreciation
  FROM public.company_data s
  CROSS JOIN LATERAL jsonb_each(s.profit_loss->'depreciation') AS e(k, v)
  WHERE
    s.symbol IS NOT NULL
    AND (s.profit_loss ? 'depreciation')
    AND e.k ~ '^Mar [0-9]{4}$'
),
ex AS (
  SELECT
    op.symbol,
    op.fiscal_year,
    CASE
      WHEN op.operating_profit IS NULL AND d.depreciation IS NULL THEN NULL
      ELSE COALESCE(op.operating_profit, 0) + COALESCE(d.depreciation, 0)
    END AS ebitda
  FROM op_year op
  LEFT JOIN dep_year d
    ON d.symbol = op.symbol
    AND d.fiscal_year = op.fiscal_year
)
UPDATE public.fundamentals_history fh
SET
  ebitda = COALESCE(fh.ebitda, ex.ebitda)
FROM ex
WHERE
  fh.symbol = ex.symbol
  AND fh.fiscal_year = ex.fiscal_year
  AND fh.fiscal_year <> 1900
  AND fh.ebitda IS NULL
  AND ex.ebitda IS NOT NULL;

COMMIT;

