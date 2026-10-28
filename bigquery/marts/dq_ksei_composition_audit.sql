-- =============================================================================
-- DATA QUALITY AUDIT DDL: KSEI Shareholder Composition
-- Location: idx_analytics.dq_ksei_composition_audit
-- Mathematical reconciliation checks:
--   1. local_is + ... + local_ot == local_total
--   2. foreign_is + ... + foreign_ot == foreign_total
--   3. local_total + foreign_total == sec_num
-- =============================================================================

CREATE OR REPLACE VIEW `{project_id}.idx_analytics.dq_ksei_composition_audit` AS
WITH metrics AS (
    SELECT
        report_date,
        symbol,
        equity_type,
        sec_num,
        
        -- Calculated Sums
        (COALESCE(local_is, 0) + COALESCE(local_cp, 0) + COALESCE(local_pf, 0) + 
         COALESCE(local_ib, 0) + COALESCE(local_id, 0) + COALESCE(local_mf, 0) + 
         COALESCE(local_sc, 0) + COALESCE(local_fd, 0) + COALESCE(local_ot, 0)) AS calc_local_total,
        local_total AS reported_local_total,
        
        (COALESCE(foreign_is, 0) + COALESCE(foreign_cp, 0) + COALESCE(foreign_pf, 0) + 
         COALESCE(foreign_ib, 0) + COALESCE(foreign_id, 0) + COALESCE(foreign_mf, 0) + 
         COALESCE(foreign_sc, 0) + COALESCE(foreign_fd, 0) + COALESCE(foreign_ot, 0)) AS calc_foreign_total,
        foreign_total AS reported_foreign_total,
        
        (COALESCE(local_total, 0) + COALESCE(foreign_total, 0)) AS calc_grand_total
    FROM `{project_id}.idx_analytics.ksei_shareholder_composition`
)
SELECT
    report_date,
    symbol,
    equity_type,
    sec_num,
    calc_grand_total AS scriptless_shares,
    GREATEST(0, COALESCE(sec_num, 0) - calc_grand_total) AS non_scriptless_shares,
    ROUND(SAFE_DIVIDE(calc_grand_total, NULLIF(sec_num, 0)) * 100, 4) AS scriptless_pct,
    ROUND(SAFE_DIVIDE(GREATEST(0, COALESCE(sec_num, 0) - calc_grand_total), NULLIF(sec_num, 0)) * 100, 4) AS non_scriptless_pct,
    
    -- Mathematical Reconciliation (Checks if source breakdown sums equal reported totals)
    reported_local_total - calc_local_total AS local_discrepancy,
    reported_foreign_total - calc_foreign_total AS foreign_discrepancy,
    
    -- Overall Data Quality Status (PASSED if math is correct and KSEI shares do not exceed listed shares)
    CASE 
        WHEN (reported_local_total = calc_local_total)
         AND (reported_foreign_total = calc_foreign_total)
         AND (calc_grand_total <= COALESCE(sec_num, 0))
        THEN 'PASSED'
        ELSE 'FAILED'
    END AS dq_status
FROM metrics;
