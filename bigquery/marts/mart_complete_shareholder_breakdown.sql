-- =============================================================================
-- GOLD LAYER VIEW: Complete Shareholder Breakdown (Dynamic Threshold Aware)
-- Location: idx_analytics.mart_complete_shareholder_breakdown
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
date_threshold AS (
    SELECT
        report_date,
        CASE WHEN MIN(ownership_percentage) < 5.0 THEN 1 ELSE 5 END AS threshold_int
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
    SELECT report_date, symbol, sec_num, UPPER(category) AS category, total_shares
    FROM `idx_analytics.ksei_shareholder_composition`
    UNPIVOT (
        total_shares FOR category IN (
            local_is, local_cp, local_pf, local_ib, local_id, local_mf, local_sc, local_fd, local_ot,
            foreign_is, foreign_cp, foreign_pf, foreign_ib, foreign_id, foreign_mf, foreign_sc, foreign_fd, foreign_ot
        )
    )
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
    LEFT JOIN date_threshold t ON k.report_date = t.report_date
    LEFT JOIN category_named_totals c ON k.report_date = c.report_date AND k.symbol = c.symbol AND k.category = c.category
)
SELECT report_date, symbol, category, investor_name, holder_type, total_shares, ownership_percentage FROM named_holders
UNION ALL
SELECT report_date, symbol, category, investor_name, holder_type, total_shares, ownership_percentage FROM remainder_holders;
