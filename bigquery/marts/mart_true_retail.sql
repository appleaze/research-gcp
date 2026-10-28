-- =============================================================================
-- GOLD LAYER VIEW: True Retail
-- Location: idx_analytics.mart_true_retail
-- Logic: Total KSEI Local ID MINUS named individual owners (LOCAL_ID >=1%)
-- Sources (Silver):
--   - idx_analytics.ksei_shareholder_composition
--   - idx_analytics.ksei_shareholder_ownership
-- =============================================================================

CREATE OR REPLACE VIEW `idx_analytics.mart_true_retail` AS
WITH named_individuals AS (
    SELECT 
        report_date,
        symbol,
        COUNT(DISTINCT investor_name) AS named_individual_count,
        SUM(total_shares) AS named_individual_shares,
        ROUND(SUM(ownership_percentage), 2) AS named_individual_pct
    FROM `idx_analytics.ksei_shareholder_ownership`
    WHERE category = 'LOCAL_ID'
    GROUP BY report_date, symbol
)
SELECT
    k.report_date,
    k.symbol,
    k.sec_num AS total_listed_shares,
    k.local_id AS ksei_aggregate_local_id_shares,
    ROUND(SAFE_DIVIDE(k.local_id, NULLIF(k.sec_num, 0)) * 100, 2) AS ksei_aggregate_local_id_pct,
    
    -- Named Individual Whales (>= 1%)
    COALESCE(w.named_individual_count, 0) AS named_individual_count,
    COALESCE(w.named_individual_shares, 0) AS named_individual_shares,
    COALESCE(w.named_individual_pct, 0) AS named_individual_pct,
    
    -- True Retail (pure unnamed retail crowd)
    GREATEST(0, k.local_id - COALESCE(w.named_individual_shares, 0)) AS true_retail_shares,
    ROUND(SAFE_DIVIDE(GREATEST(0, k.local_id - COALESCE(w.named_individual_shares, 0)), NULLIF(k.sec_num, 0)) * 100, 2) AS true_retail_pct
FROM `idx_analytics.ksei_shareholder_composition` k
LEFT JOIN named_individuals w
  ON k.report_date = w.report_date AND k.symbol = w.symbol;
