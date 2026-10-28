-- =============================================================================
-- GOLD LAYER VIEW: Complete Shareholder Breakdown (Dynamic Threshold Aware)
-- Location: idx_analytics.mart_complete_shareholder_breakdown
-- Purpose: 
--   Replaces legacy `*_complete_breakdown.csv`.
--   Automatically detects whether a monthly snapshot was disclosed at the 
--   5% threshold (e.g. Jan 2026) or the expanded 1% threshold (March 2026 onwards).
--   Combines individual named holders with calculated remainder (<1% or <5%)
--   shares for each of the 18 KSEI investor categories.
-- =============================================================================

CREATE OR REPLACE VIEW `idx_analytics.mart_complete_shareholder_breakdown` AS
WITH named_holders AS (
    SELECT
        report_date,
        symbol,
        category,
        investor_name,
        holder_type,
        COALESCE(scripless_shares, total_shares) AS scripless_shares,
        total_shares,
        ownership_percentage
    FROM `idx_analytics.ksei_shareholder_ownership`
),
-- Detect disclosure threshold per date: if any holder < 5.0%, threshold is 1%; else 5%.
date_threshold AS (
    SELECT
        report_date,
        CASE 
            WHEN MIN(ownership_percentage) < 5.0 THEN 1 
            ELSE 5 
        END AS threshold_int
    FROM named_holders
    GROUP BY report_date
),
category_named_totals AS (
    SELECT
        report_date,
        symbol,
        category,
        SUM(scripless_shares) AS named_category_shares
    FROM named_holders
    GROUP BY report_date, symbol, category
),
unpivoted_ksei AS (
    -- Unpivot all 18 KSEI categories from Silver ksei_shareholder_composition
    SELECT report_date, symbol, sec_num, 'LOCAL_IS' AS category, local_is AS total_shares FROM `idx_analytics.ksei_shareholder_composition`
    UNION ALL
    SELECT report_date, symbol, sec_num, 'LOCAL_CP', local_cp FROM `idx_analytics.ksei_shareholder_composition`
    UNION ALL
    SELECT report_date, symbol, sec_num, 'LOCAL_PF', local_pf FROM `idx_analytics.ksei_shareholder_composition`
    UNION ALL
    SELECT report_date, symbol, sec_num, 'LOCAL_IB', local_ib FROM `idx_analytics.ksei_shareholder_composition`
    UNION ALL
    SELECT report_date, symbol, sec_num, 'LOCAL_ID', local_id FROM `idx_analytics.ksei_shareholder_composition`
    UNION ALL
    SELECT report_date, symbol, sec_num, 'LOCAL_MF', local_mf FROM `idx_analytics.ksei_shareholder_composition`
    UNION ALL
    SELECT report_date, symbol, sec_num, 'LOCAL_SC', local_sc FROM `idx_analytics.ksei_shareholder_composition`
    UNION ALL
    SELECT report_date, symbol, sec_num, 'LOCAL_FD', local_fd FROM `idx_analytics.ksei_shareholder_composition`
    UNION ALL
    SELECT report_date, symbol, sec_num, 'LOCAL_OT', local_ot FROM `idx_analytics.ksei_shareholder_composition`
    UNION ALL
    SELECT report_date, symbol, sec_num, 'FOREIGN_IS', foreign_is FROM `idx_analytics.ksei_shareholder_composition`
    UNION ALL
    SELECT report_date, symbol, sec_num, 'FOREIGN_CP', foreign_cp FROM `idx_analytics.ksei_shareholder_composition`
    UNION ALL
    SELECT report_date, symbol, sec_num, 'FOREIGN_PF', foreign_pf FROM `idx_analytics.ksei_shareholder_composition`
    UNION ALL
    SELECT report_date, symbol, sec_num, 'FOREIGN_IB', foreign_ib FROM `idx_analytics.ksei_shareholder_composition`
    UNION ALL
    SELECT report_date, symbol, sec_num, 'FOREIGN_ID', foreign_id FROM `idx_analytics.ksei_shareholder_composition`
    UNION ALL
    SELECT report_date, symbol, sec_num, 'FOREIGN_MF', foreign_mf FROM `idx_analytics.ksei_shareholder_composition`
    UNION ALL
    SELECT report_date, symbol, sec_num, 'FOREIGN_SC', foreign_sc FROM `idx_analytics.ksei_shareholder_composition`
    UNION ALL
    SELECT report_date, symbol, sec_num, 'FOREIGN_FD', foreign_fd FROM `idx_analytics.ksei_shareholder_composition`
    UNION ALL
    SELECT report_date, symbol, sec_num, 'FOREIGN_OT', foreign_ot FROM `idx_analytics.ksei_shareholder_composition`
),
remainder_holders AS (
    SELECT
        k.report_date,
        k.symbol,
        k.category,
        CONCAT('<', CAST(COALESCE(t.threshold_int, 5) AS STRING), '% ', REPLACE(k.category, '_', ' ')) AS investor_name,
        CONCAT('<', CAST(COALESCE(t.threshold_int, 5) AS STRING), '%') AS holder_type,
        GREATEST(0, k.total_shares - COALESCE(c.named_category_shares, 0)) AS total_shares,
        ROUND(SAFE_DIVIDE(GREATEST(0, k.total_shares - COALESCE(c.named_category_shares, 0)), NULLIF(k.sec_num, 0)) * 100, 4) AS ownership_percentage
    FROM unpivoted_ksei k
    LEFT JOIN date_threshold t
      ON k.report_date = t.report_date
    LEFT JOIN category_named_totals c
      ON k.report_date = c.report_date AND k.symbol = c.symbol AND k.category = c.category
)
SELECT 
    report_date,
    symbol,
    category,
    investor_name,
    holder_type,
    total_shares,
    ownership_percentage
FROM named_holders
UNION ALL
SELECT 
    report_date,
    symbol,
    category,
    investor_name,
    holder_type,
    total_shares,
    ownership_percentage
FROM remainder_holders;
