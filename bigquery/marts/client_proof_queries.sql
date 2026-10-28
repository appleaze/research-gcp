-- =============================================================================
-- GOLD LAYER PROOF & CLIENT DEMO QUERIES
-- Dataset: idx_analytics (Jakarta asia-southeast2)
-- Saved in: bigquery/marts/client_proof_queries.sql
-- =============================================================================

-- -----------------------------------------------------------------------------
-- QUERY 1A: Per-Stock Month-over-Month (MoM) Trend by Category & Holder
-- Source: idx_analytics.mart_complete_shareholder_breakdown
-- Purpose:
--   Shows the exact MoM share and % change for every breakdown line of a stock
--   (both Named >=1%/5% players and Unnamed <1%/<5% category remainders).
-- -----------------------------------------------------------------------------
WITH monthly_breakdown AS (
    SELECT
        report_date,
        symbol,
        category,
        investor_name,
        holder_type,
        total_shares,
        ownership_percentage
    FROM `idx_analytics.mart_complete_shareholder_breakdown`
    WHERE symbol = 'BBCA' -- <-- Replace with any ticker (e.g. 'BREN', 'DCII', 'BMRI', 'BBRI')
),
mom_calc AS (
    SELECT
        report_date,
        symbol,
        category,
        investor_name,
        holder_type,
        total_shares AS curr_shares,
        LAG(total_shares) OVER (
            PARTITION BY symbol, category, investor_name 
            ORDER BY report_date
        ) AS prev_shares,
        ownership_percentage AS curr_pct,
        LAG(ownership_percentage) OVER (
            PARTITION BY symbol, category, investor_name 
            ORDER BY report_date
        ) AS prev_pct
    FROM monthly_breakdown
)
SELECT
    report_date,
    symbol,
    category,
    holder_type,
    investor_name,
    prev_shares,
    curr_shares,
    (curr_shares - COALESCE(prev_shares, curr_shares)) AS mom_shares_delta,
    prev_pct,
    curr_pct,
    ROUND(curr_pct - COALESCE(prev_pct, curr_pct), 4) AS mom_pct_delta,
    CASE
        WHEN prev_shares IS NULL THEN 'BASE_MONTH_OR_NEW_ENTRY'
        WHEN curr_shares > prev_shares THEN 'ACCUMULATION (+)'
        WHEN curr_shares < prev_shares THEN 'DISTRIBUTION (-)'
        ELSE 'UNCHANGED'
    END AS mom_signal
FROM mom_calc
WHERE curr_shares > 0 OR prev_shares > 0
ORDER BY report_date DESC, curr_pct DESC;


-- -----------------------------------------------------------------------------
-- QUERY 1B: Per-Stock MoM Trend Aggregated by the 18 KSEI Categories
-- Purpose:
--   Normalized MoM view across all 18 KSEI investor buckets (e.g., LOCAL_ID,
--   FOREIGN_IB, LOCAL_MF) + True Float & True Retail summary per stock.
-- -----------------------------------------------------------------------------
WITH category_monthly AS (
    SELECT
        report_date,
        symbol,
        category,
        SUM(CASE WHEN holder_type NOT LIKE '<%' THEN total_shares ELSE 0 END) AS named_insider_shares,
        SUM(CASE WHEN holder_type LIKE '<%' THEN total_shares ELSE 0 END) AS unnamed_float_shares,
        SUM(total_shares) AS total_category_shares,
        ROUND(SUM(ownership_percentage), 4) AS total_category_pct
    FROM `idx_analytics.mart_complete_shareholder_breakdown`
    WHERE symbol = 'BBCA' -- <-- Replace with any ticker
    GROUP BY report_date, symbol, category
)
SELECT
    report_date,
    symbol,
    category,
    LAG(total_category_shares) OVER (PARTITION BY symbol, category ORDER BY report_date) AS prev_month_shares,
    total_category_shares AS curr_month_shares,
    total_category_shares - LAG(total_category_shares) OVER (PARTITION BY symbol, category ORDER BY report_date) AS mom_shares_change,
    LAG(total_category_pct) OVER (PARTITION BY symbol, category ORDER BY report_date) AS prev_month_pct,
    total_category_pct AS curr_month_pct,
    ROUND(total_category_pct - LAG(total_category_pct) OVER (PARTITION BY symbol, category ORDER BY report_date), 4) AS mom_pct_change,
    named_insider_shares,
    unnamed_float_shares
FROM category_monthly
WHERE total_category_shares > 0
ORDER BY report_date DESC, curr_month_pct DESC;


-- -----------------------------------------------------------------------------
-- QUERY 2: Highly Concentrated Stock Screener (1-5% "Friendly Partner" Analysis)
-- Sources:
--   - idx_analytics.mart_true_float (Joined KSEI + 1-5% Ownership)
--   - idx_analytics.mart_true_retail
--   - idx_analytics.ksei_shareholder_ownership
-- Logic:
--   1. Splits named holders into:
--      - Major Controllers (>= 5%)
--      - "Friendly / Related Partners" (1% <= ownership < 5%)
--   2. Calculates "Apparent Float" (if 1-5% holders were mistakenly counted as float)
--      vs. "True Free Float" (once 1-5% friendly partners & locked scrip are removed).
--   3. Evaluates against both the 15% threshold and the 7.5% Free Float threshold.
-- -----------------------------------------------------------------------------
WITH latest_1pct_snapshot AS (
    SELECT MAX(report_date) AS max_date
    FROM `idx_analytics.ksei_shareholder_ownership`
    WHERE ownership_percentage < 5.0
),
holder_buckets AS (
    SELECT
        o.report_date,
        o.symbol,
        -- Controllers (>= 5%)
        COUNT(DISTINCT CASE WHEN o.ownership_percentage >= 5.0 THEN o.investor_name END) AS controllers_ge_5pct_count,
        SUM(CASE WHEN o.ownership_percentage >= 5.0 THEN COALESCE(o.scripless_shares, o.total_shares) ELSE 0 END) AS controllers_ge_5pct_shares,
        ROUND(SUM(CASE WHEN o.ownership_percentage >= 5.0 THEN o.ownership_percentage ELSE 0 END), 2) AS controllers_ge_5pct_pct,

        -- Friendly / Related Partners (1% to < 5%)
        COUNT(DISTINCT CASE WHEN o.ownership_percentage >= 1.0 AND o.ownership_percentage < 5.0 THEN o.investor_name END) AS friendly_1to5pct_count,
        SUM(CASE WHEN o.ownership_percentage >= 1.0 AND o.ownership_percentage < 5.0 THEN COALESCE(o.scripless_shares, o.total_shares) ELSE 0 END) AS friendly_1to5pct_shares,
        ROUND(SUM(CASE WHEN o.ownership_percentage >= 1.0 AND o.ownership_percentage < 5.0 THEN o.ownership_percentage ELSE 0 END), 2) AS friendly_1to5pct_pct,

        -- Audit list of the 1-5% friendly partner names
        STRING_AGG(
            CASE WHEN o.ownership_percentage >= 1.0 AND o.ownership_percentage < 5.0 
                 THEN CONCAT(o.investor_name, ' (', CAST(ROUND(o.ownership_percentage, 2) AS STRING), '%)') 
            END, 
            ', ' ORDER BY o.ownership_percentage DESC
        ) AS friendly_partner_list
    FROM `idx_analytics.ksei_shareholder_ownership` o
    JOIN latest_1pct_snapshot s ON o.report_date = s.max_date
    GROUP BY o.report_date, o.symbol
),
joined_gold AS (
    SELECT
        f.report_date,
        f.symbol,
        f.total_listed_shares,
        ROUND(SAFE_DIVIDE(f.non_scripless_locked_shares, NULLIF(f.total_listed_shares, 0)) * 100, 2) AS locked_non_scripless_pct,

        -- >= 5% Controllers
        COALESCE(b.controllers_ge_5pct_count, 0) AS controllers_5pct_count,
        COALESCE(b.controllers_ge_5pct_pct, 0) AS controllers_5pct_pct,

        -- 1% - 5% Friendly Partners
        COALESCE(b.friendly_1to5pct_count, 0) AS friendly_1to5pct_count,
        COALESCE(b.friendly_1to5pct_pct, 0) AS friendly_1to5pct_pct,

        -- Apparent Float under Old 5% Rule (if 1-5% partners Pretend to be Public Float)
        ROUND(SAFE_DIVIDE(
            GREATEST(0, f.ksei_scripless_total - COALESCE(b.controllers_ge_5pct_shares, 0)),
            NULLIF(f.total_listed_shares, 0)
        ) * 100, 2) AS apparent_float_5pct_rule,

        -- True Free Float (after removing Locked Scrip + >=5% Controllers + 1-5% Friendly Partners)
        f.true_float_pct AS true_free_float_pct,

        -- True Retail % of Listed Shares
        r.true_retail_pct,

        b.friendly_partner_list
    FROM `idx_analytics.mart_true_float` f
    JOIN latest_1pct_snapshot s ON f.report_date = s.max_date
    LEFT JOIN `idx_analytics.mart_true_retail` r
      ON f.report_date = r.report_date AND f.symbol = r.symbol
    LEFT JOIN holder_buckets b
      ON f.report_date = b.report_date AND f.symbol = b.symbol
    WHERE f.total_listed_shares > 0
)
SELECT
    report_date,
    symbol,
    locked_non_scripless_pct,
    controllers_5pct_count,
    controllers_5pct_pct,
    friendly_1to5pct_count,
    friendly_1to5pct_pct,
    apparent_float_5pct_rule,
    true_free_float_pct,
    true_retail_pct,
    CASE
        WHEN true_free_float_pct < 7.5 AND friendly_1to5pct_pct >= 15.0
            THEN '1. EXTREME: Friendly Partners >=15% & True Float <7.5%'
        WHEN true_free_float_pct < 7.5
            THEN '2. CRITICAL FLOAT (<7.5% True Float - Below IDX Min)'
        WHEN true_free_float_pct < 15.0 AND friendly_1to5pct_pct >= 15.0
            THEN '3. SHADOW PARKED: Friendly Partners >=15% & True Float <15%'
        WHEN true_free_float_pct < 15.0
            THEN '4. TIGHT FLOAT (7.5% - 15% True Float)'
        WHEN friendly_1to5pct_pct >= 15.0
            THEN '5. HIGH FRIENDLY BLOC (Friendly Partners >=15%)'
        ELSE 'NORMAL'
    END AS concentration_tier,
    friendly_partner_list
FROM joined_gold
WHERE
    -- Matches stocks that breach the 7.5% or 15% free float threshold
    -- OR have >= 15% parked in 1-5% "friendly partner" wallets:
    (true_free_float_pct < 15.0 OR friendly_1to5pct_pct >= 15.0)
    AND friendly_1to5pct_count > 0
ORDER BY
    -- Prioritize stocks where 1-5% friendly partners (>=15%) cause True Float to drop below 7.5%
    (CASE WHEN friendly_1to5pct_pct >= 15.0 AND true_free_float_pct < 7.5 THEN 0 ELSE 1 END),
    true_free_float_pct ASC,
    friendly_1to5pct_pct DESC;
