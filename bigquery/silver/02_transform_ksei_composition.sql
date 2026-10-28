-- =============================================================================
-- SILVER TRANSFORMATION: KSEI Scripless Balance Position
-- Source: idx_raw.ksei_balancepos_raw (Bronze)
-- Target: idx_analytics.ksei_shareholder_composition (Silver)
-- Rules:
--   1. Filters exclusively for 'EQUITY' (filters out bonds, rights, mutual funds).
--   2. Safe casts string columns into INT64 and FLOAT64.
--   3. Idempotently deduplicates via QUALIFY ROW_NUMBER() by latest _ingested_at.
-- =============================================================================

MERGE INTO `idx_analytics.ksei_shareholder_composition` T
USING (
    SELECT
        PARSE_DATE('%d-%b-%Y', INITCAP(date_raw)) AS report_date,
        UPPER(TRIM(symbol_raw)) AS symbol,
        UPPER(TRIM(type_raw)) AS equity_type,
        SAFE_CAST(sec_num_raw AS INT64) AS sec_num,
        SAFE_CAST(price_raw AS FLOAT64) AS price,
        
        -- Local Categories
        SAFE_CAST(local_is_raw AS INT64) AS local_is,
        SAFE_CAST(local_cp_raw AS INT64) AS local_cp,
        SAFE_CAST(local_pf_raw AS INT64) AS local_pf,
        SAFE_CAST(local_ib_raw AS INT64) AS local_ib,
        SAFE_CAST(local_id_raw AS INT64) AS local_id,
        SAFE_CAST(local_mf_raw AS INT64) AS local_mf,
        SAFE_CAST(local_sc_raw AS INT64) AS local_sc,
        SAFE_CAST(local_fd_raw AS INT64) AS local_fd,
        SAFE_CAST(local_ot_raw AS INT64) AS local_ot,
        SAFE_CAST(local_total_raw AS INT64) AS local_total,
        
        -- Foreign Categories
        SAFE_CAST(foreign_is_raw AS INT64) AS foreign_is,
        SAFE_CAST(foreign_cp_raw AS INT64) AS foreign_cp,
        SAFE_CAST(foreign_pf_raw AS INT64) AS foreign_pf,
        SAFE_CAST(foreign_ib_raw AS INT64) AS foreign_ib,
        SAFE_CAST(foreign_id_raw AS INT64) AS foreign_id,
        SAFE_CAST(foreign_mf_raw AS INT64) AS foreign_mf,
        SAFE_CAST(foreign_sc_raw AS INT64) AS foreign_sc,
        SAFE_CAST(foreign_fd_raw AS INT64) AS foreign_fd,
        SAFE_CAST(foreign_ot_raw AS INT64) AS foreign_ot,
        SAFE_CAST(foreign_total_raw AS INT64) AS foreign_total,
        
        CURRENT_TIMESTAMP() AS processed_at
    FROM `idx_raw.ksei_balancepos_raw`
    WHERE UPPER(TRIM(type_raw)) = 'EQUITY'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY PARSE_DATE('%d-%b-%Y', INITCAP(date_raw)), UPPER(TRIM(symbol_raw))
        ORDER BY _ingested_at DESC
    ) = 1
) S
ON T.report_date = S.report_date AND T.symbol = S.symbol
WHEN MATCHED THEN
    UPDATE SET 
        equity_type = S.equity_type,
        sec_num = S.sec_num,
        price = S.price,
        local_is = S.local_is,
        local_cp = S.local_cp,
        local_pf = S.local_pf,
        local_ib = S.local_ib,
        local_id = S.local_id,
        local_mf = S.local_mf,
        local_sc = S.local_sc,
        local_fd = S.local_fd,
        local_ot = S.local_ot,
        local_total = S.local_total,
        foreign_is = S.foreign_is,
        foreign_cp = S.foreign_cp,
        foreign_pf = S.foreign_pf,
        foreign_ib = S.foreign_ib,
        foreign_id = S.foreign_id,
        foreign_mf = S.foreign_mf,
        foreign_sc = S.foreign_sc,
        foreign_fd = S.foreign_fd,
        foreign_ot = S.foreign_ot,
        foreign_total = S.foreign_total,
        processed_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT ROW;
