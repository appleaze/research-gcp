-- =============================================================================
-- GOLD LAYER VIEW: True Float & True Retail
-- Location: idx_analytics.mart_true_float_and_retail
-- Logic:
--   1. True Float  = sec_num MINUS every named player (all >=1% holders)
--   2. True Retail = local_id MINUS named individual owners (LOCAL_ID >=1%)
-- Sources (Silver):
--   - idx_analytics.ksei_shareholder_composition
--   - idx_analytics.ksei_shareholder_ownership
-- =============================================================================

CREATE OR REPLACE VIEW `idx_analytics.mart_true_float_and_retail` AS
WITH named_aggregates AS (
    SELECT 
        report_date,
        symbol,
        -- All named players combined (Whales, Controllers, Institutions >= 1%)
        SUM(total_shares) AS total_named_player_shares,
        ROUND(SUM(ownership_percentage), 2) AS total_named_player_pct,
        COUNT(DISTINCT investor_name) AS named_player_count,
        
        -- Named Local Individuals only (Whale retail >= 1%)
        SUM(CASE WHEN category = 'LOCAL_ID' THEN total_shares ELSE 0 END) AS named_local_id_shares,
        ROUND(SUM(CASE WHEN category = 'LOCAL_ID' THEN ownership_percentage ELSE 0 END), 2) AS named_local_id_pct,
        COUNT(DISTINCT CASE WHEN category = 'LOCAL_ID' THEN investor_name END) AS named_local_id_count
    FROM `idx_analytics.ksei_shareholder_ownership`
    GROUP BY report_date, symbol
)
SELECT
    k.report_date,
    k.symbol,
    k.sec_num AS total_listed_shares,
    k.price,
    
    -- 1. True Float (Listed shares minus EVERY known named holder)
    COALESCE(n.total_named_player_shares, 0) AS named_player_shares,
    COALESCE(n.total_named_player_pct, 0) AS named_player_pct,
    GREATEST(0, k.sec_num - COALESCE(n.total_named_player_shares, 0)) AS true_float_shares,
    ROUND(SAFE_DIVIDE(GREATEST(0, k.sec_num - COALESCE(n.total_named_player_shares, 0)), NULLIF(k.sec_num, 0)) * 100, 2) AS true_float_pct,
    
    -- 2. True Retail (KSEI local_id minus named local individuals)
    k.local_id AS ksei_aggregate_local_id_shares,
    COALESCE(n.named_local_id_shares, 0) AS named_local_id_shares,
    GREATEST(0, k.local_id - COALESCE(n.named_local_id_shares, 0)) AS true_retail_shares,
    ROUND(SAFE_DIVIDE(GREATEST(0, k.local_id - COALESCE(n.named_local_id_shares, 0)), NULLIF(k.sec_num, 0)) * 100, 2) AS true_retail_pct,
    
    -- Invariant validation: true_retail must never exceed true_float
    CASE 
        WHEN GREATEST(0, k.local_id - COALESCE(n.named_local_id_shares, 0)) <= GREATEST(0, k.sec_num - COALESCE(n.total_named_player_shares, 0)) 
        THEN 'VALID' 
        ELSE 'CHECK_ANOMALY' 
    END AS math_status
FROM `idx_analytics.ksei_shareholder_composition` k
LEFT JOIN named_aggregates n
  ON k.report_date = n.report_date AND k.symbol = n.symbol;
