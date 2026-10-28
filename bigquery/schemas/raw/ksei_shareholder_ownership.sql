-- =============================================================================
-- SILVER LAYER DDL: Normalized & Conformed Monthly Shareholder Holdings
-- Location: idx_analytics.ksei_shareholder_ownership
-- Partition: report_date
-- Cluster: symbol, investor_name
-- =============================================================================

CREATE TABLE IF NOT EXISTS `{project_id}.idx_analytics.ksei_shareholder_ownership` (
    report_date DATE OPTIONS(description="Position / snapshot date from disclosure"),
    symbol STRING OPTIONS(description="Stock ticker symbol (e.g. AADI)"),
    investor_name STRING OPTIONS(description="Cleaned, standardized investor name"),
    investor_type STRING OPTIONS(description="Investor type (CP, ID, IS, PF, MF, IB, SC, FD, OT)"),
    category STRING OPTIONS(description="Standardized KSEI category (e.g. LOCAL_CP, LOCAL_ID, FOREIGN_CP)"),
    is_foreign BOOLEAN OPTIONS(description="True if foreign investor, False if domestic"),
    holder_type STRING OPTIONS(description="Tier classification: '>5%' or '>1%'"),
    scripless_shares INT64 OPTIONS(description="Total scripless shares held in C-BEST SRE"),
    scrip_shares INT64 OPTIONS(description="Total scrip shares held in eBAE"),
    total_shares INT64 OPTIONS(description="Grand total shares held"),
    ownership_percentage FLOAT64 OPTIONS(description="Ownership percentage of listed shares"),
    
    -- Ingestion audit
    _source_file STRING,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY report_date
CLUSTER BY symbol, investor_name;
