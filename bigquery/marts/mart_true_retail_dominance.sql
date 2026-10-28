-- =============================================================================
-- GOLD LAYER VIEW: True Retail Dominance vs Whale Individuals
-- Location: idx_analytics.mart_true_retail_dominance
-- Purpose:
--   Calculates pure granular retail float by subtracting high-net-worth 
--   whales (named individuals holding >=1%) from aggregate KSEI LOCAL_ID.
--   Direct replacement for Streamlit get_true_retail() logic.
-- =============================================================================

CREATE OR REPLACE VIEW `{project_id}.idx_analytics.mart_true_retail_dominance` AS
WITH whale_individuals AS (
    SELECT 
        report_date,
        symbol,
        COUNT(DISTINCT investor_name) AS whale_individual_count,
        SUM(total_shares) AS whale_individual_shares,
        SUM(ownership_percentage) AS whale_individual_pct
    FROM `{project_id}.idx_analytics.ksei_shareholder_ownership`
    WHERE category = 'LOCAL_ID'
    GROUP BY report_date, symbol
)
SELECT
    k.report_date,
    k.symbol,
    k.sec_num AS total_listed_shares,
    
    -- KSEI Aggregate Local Individual
    k.local_id AS ksei_aggregate_local_id_shares,
    ROUND(SAFE_DIVIDE(k.local_id, NULLIF(k.sec_num, 0)) * 100, 2) AS ksei_aggregate_local_id_pct,
    
    -- Identified Whale Individuals (>=1%)
    COALESCE(w.whale_individual_count, 0) AS whale_individual_count,
    COALESCE(w.whale_individual_shares, 0) AS whale_individual_shares,
    ROUND(COALESCE(w.whale_individual_pct, 0), 2) AS whale_individual_pct,
    
    -- True Retail Floating Supply (<1% ID)
    GREATEST(0, k.local_id - COALESCE(w.whale_individual_shares, 0)) AS true_retail_shares,
    ROUND(SAFE_DIVIDE(GREATEST(0, k.local_id - COALESCE(w.whale_individual_shares, 0)), NULLIF(k.sec_num, 0)) * 100, 2) AS true_retail_pct,
    
    -- Retail Dominance Signal
    CASE 
        WHEN SAFE_DIVIDE(GREATEST(0, k.local_id - COALESCE(w.whale_individual_shares, 0)), NULLIF(k.sec_num, 0)) >= 0.25 THEN 'HIGH_RETAIL_DOMINANCE'
        WHEN SAFE_DIVIDE(GREATEST(0, k.local_id - COALESCE(w.whale_individual_shares, 0)), NULLIF(k.sec_num, 0)) <= 0.05 THEN 'TIGHT_INSTITUTIONAL_CONTROL'
        ELSE 'BALANCED'
    END AS float_structure_regime
FROM `{project_id}.idx_analytics.ksei_shareholder_composition` k
LEFT JOIN whale_individuals w
  ON k.report_date = w.report_date AND k.symbol = w.symbol;
