-- =============================================================================
-- GOLD LAYER VIEW: True Float (KSEI Scripless Unnamed Circulating Float)
-- Location: idx_analytics.mart_true_float
-- Logic:
--   Total KSEI Scripless (local_total + foreign_total)
--   MINUS Named Players Scripless in KSEI
--   = Unnamed Floating Supply in KSEI
-- =============================================================================

CREATE OR REPLACE VIEW `idx_analytics.mart_true_float` AS
WITH named_ksei AS (
    SELECT 
        report_date,
        symbol,
        COUNT(DISTINCT investor_name) AS named_player_count,
        -- Scripless shares held by named players in KSEI C-BEST
        SUM(COALESCE(scripless_shares, total_shares)) AS named_ksei_shares,
        ROUND(SUM(ownership_percentage), 2) AS named_ksei_pct
    FROM `idx_analytics.ksei_shareholder_ownership`
    GROUP BY report_date, symbol
)
SELECT
    k.report_date,
    k.symbol,
    k.sec_num AS total_listed_shares,
    (COALESCE(k.local_total, 0) + COALESCE(k.foreign_total, 0)) AS ksei_scripless_total,
    GREATEST(0, COALESCE(k.sec_num, 0) - (COALESCE(k.local_total, 0) + COALESCE(k.foreign_total, 0))) AS non_scripless_locked_shares,
    
    -- Named blocks inside KSEI
    COALESCE(n.named_player_count, 0) AS named_player_count,
    COALESCE(n.named_ksei_shares, 0) AS named_ksei_shares,
    COALESCE(n.named_ksei_pct, 0) AS named_ksei_pct,
    
    -- True Float: Unnamed circulating shares in KSEI
    GREATEST(0, (COALESCE(k.local_total, 0) + COALESCE(k.foreign_total, 0)) - COALESCE(n.named_ksei_shares, 0)) AS true_float_shares,
    
    -- True Float % relative to Total Listed Shares (sec_num)
    ROUND(SAFE_DIVIDE(
        GREATEST(0, (COALESCE(k.local_total, 0) + COALESCE(k.foreign_total, 0)) - COALESCE(n.named_ksei_shares, 0)),
        NULLIF(k.sec_num, 0)
    ) * 100, 2) AS true_float_pct,
    
    -- True Float % relative to KSEI Scripless Pool
    ROUND(SAFE_DIVIDE(
        GREATEST(0, (COALESCE(k.local_total, 0) + COALESCE(k.foreign_total, 0)) - COALESCE(n.named_ksei_shares, 0)),
        NULLIF((COALESCE(k.local_total, 0) + COALESCE(k.foreign_total, 0)), 0)
    ) * 100, 2) AS true_float_of_ksei_pct
FROM `idx_analytics.ksei_shareholder_composition` k
LEFT JOIN named_ksei n
  ON k.report_date = n.report_date AND k.symbol = n.symbol;
