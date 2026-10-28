-- =============================================================================
-- SILVER TRANSFORMATION: Monthly Shareholder Ownership Disclosures (1% & 5%)
-- Source: idx_raw.ksei_shareholder_disclosures_raw (Bronze)
-- Target: idx_analytics.ksei_shareholder_ownership (Silver)
-- Rules:
--   1. Converts Indonesian number format (thousands '.' to '', decimal ',' to '.').
--   2. Maps KSEI investor category (e.g. LOCAL_CP, LOCAL_ID, FOREIGN_IS).
--   3. Tags threshold bracket ('>5%' vs '>1%').
--   4. Deduplicates via QUALIFY ROW_NUMBER() on (report_date, symbol, investor_name).
-- =============================================================================

MERGE INTO `idx_analytics.ksei_shareholder_ownership` T
USING (
    WITH raw_parsed AS (
        SELECT
            PARSE_DATE('%d-%b-%Y', INITCAP(date_raw)) AS report_date,
            UPPER(TRIM(symbol_raw)) AS symbol,
            TRIM(issuer_and_investor_raw) AS investor_name,
            UPPER(TRIM(COALESCE(investor_type_raw, 'OT'))) AS investor_type,
            CONCAT(
                CASE WHEN UPPER(TRIM(local_foreign_raw)) = 'L' THEN 'LOCAL_' ELSE 'FOREIGN_' END,
                UPPER(TRIM(COALESCE(investor_type_raw, 'OT')))
            ) AS category,
            UPPER(TRIM(local_foreign_raw)) = 'A' AS is_foreign,
            CASE 
                WHEN SAFE_CAST(REPLACE(percentage_raw, ',', '.') AS FLOAT64) >= 5.0 THEN '>5%'
                ELSE '>1%'
            END AS holder_type,
            SAFE_CAST(REPLACE(holdings_scripless_raw, '.', '') AS INT64) AS scripless_shares,
            SAFE_CAST(REPLACE(holdings_scrip_raw, '.', '') AS INT64) AS scrip_shares,
            SAFE_CAST(REPLACE(total_shares_raw, '.', '') AS INT64) AS total_shares,
            SAFE_CAST(REPLACE(percentage_raw, ',', '.') AS FLOAT64) AS ownership_percentage,
            _source_file,
            _ingested_at
        FROM `idx_raw.ksei_shareholder_disclosures_raw`
        WHERE symbol_raw IS NOT NULL AND total_shares_raw IS NOT NULL
    )
    SELECT * FROM raw_parsed
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY report_date, symbol, investor_name
        ORDER BY _ingested_at DESC
    ) = 1
) S
ON T.report_date = S.report_date AND T.symbol = S.symbol AND T.investor_name = S.investor_name
WHEN MATCHED THEN
    UPDATE SET
        investor_type = S.investor_type,
        category = S.category,
        is_foreign = S.is_foreign,
        holder_type = S.holder_type,
        scripless_shares = S.scripless_shares,
        scrip_shares = S.scrip_shares,
        total_shares = S.total_shares,
        ownership_percentage = S.ownership_percentage,
        _source_file = S._source_file,
        processed_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        report_date,
        symbol,
        investor_name,
        investor_type,
        category,
        is_foreign,
        holder_type,
        scripless_shares,
        scrip_shares,
        total_shares,
        ownership_percentage,
        _source_file,
        processed_at
    )
    VALUES (
        S.report_date,
        S.symbol,
        S.investor_name,
        S.investor_type,
        S.category,
        S.is_foreign,
        S.holder_type,
        S.scripless_shares,
        S.scrip_shares,
        S.total_shares,
        S.ownership_percentage,
        S._source_file,
        CURRENT_TIMESTAMP()
    );
