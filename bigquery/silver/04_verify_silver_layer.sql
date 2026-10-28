-- =============================================================================
-- SILVER LAYER VERIFICATION & AUDIT
-- Purpose: 
--   Quick health-check query to verify row counts, active tickers, and 
--   distribution across both Silver tables.
-- =============================================================================

-- 1. Check Row Counts & Date Distribution
SELECT 
    'ksei_shareholder_composition' AS table_name,
    report_date,
    COUNT(*) AS total_records,
    COUNT(DISTINCT symbol) AS active_tickers,
    NULL AS block_holders_5pct,
    NULL AS substantial_holders_1pct
FROM `idx_analytics.ksei_shareholder_composition`
GROUP BY report_date

UNION ALL

SELECT 
    'ksei_shareholder_ownership' AS table_name,
    report_date,
    COUNT(*) AS total_records,
    COUNT(DISTINCT symbol) AS active_tickers,
    COUNTIF(holder_type = '>5%') AS block_holders_5pct,
    COUNTIF(holder_type = '>1%') AS substantial_holders_1pct
FROM `idx_analytics.ksei_shareholder_ownership`
GROUP BY report_date
ORDER BY table_name, report_date DESC;
