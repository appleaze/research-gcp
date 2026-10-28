-- =============================================================================
-- GOLD LAYER VERIFICATION QUERY
-- =============================================================================

-- 1. Math Invariant Audit: Must return 0 error rows
SELECT 
    f.report_date,
    COUNT(*) AS total_stocks,
    COUNTIF(f.true_float_pct < 0 OR f.true_float_pct > 100) AS float_pct_errors,
    COUNTIF(r.true_retail_pct < 0 OR r.true_retail_pct > 100) AS retail_pct_errors,
    COUNTIF(r.true_retail_shares > f.true_float_shares) AS impossible_retail_errors,
    ROUND(AVG(f.true_float_pct), 2) AS avg_true_float_pct,
    ROUND(AVG(r.true_retail_pct), 2) AS avg_true_retail_pct
FROM `idx_analytics.mart_true_float` f
JOIN `idx_analytics.mart_true_retail` r
  ON f.report_date = r.report_date AND f.symbol = r.symbol
GROUP BY f.report_date;

-- 2. Spot Check Key Stocks
SELECT 
    f.symbol,
    f.report_date,
    f.total_listed_shares,
    f.ksei_scripless_total,
    f.non_scripless_locked_shares,
    f.named_ksei_shares,
    f.true_float_shares,
    f.true_float_pct,
    r.true_retail_shares,
    r.true_retail_pct
FROM `idx_analytics.mart_true_float` f
JOIN `idx_analytics.mart_true_retail` r
  ON f.report_date = r.report_date AND f.symbol = r.symbol
WHERE f.symbol IN ('AADI', 'BBRI', 'BBCA', 'BREN')
ORDER BY f.symbol;
