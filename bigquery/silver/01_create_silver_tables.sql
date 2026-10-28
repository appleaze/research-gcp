-- =============================================================================
-- SILVER LAYER DDLs: Provision Core Analytics Tables
-- Location: idx_analytics.ksei_shareholder_composition
--           idx_analytics.ksei_shareholder_ownership
-- Partitioning: report_date
-- Clustering: symbol, investor_name
-- =============================================================================

-- Table 1: KSEI Scripless Shareholder Composition (1,002 Equities, All 18 Categories)
CREATE TABLE IF NOT EXISTS `idx_analytics.ksei_shareholder_composition` (
    report_date DATE OPTIONS(description="Position date from KSEI"),
    symbol STRING OPTIONS(description="Stock ticker e.g. AADI, BBCA"),
    equity_type STRING,
    sec_num INT64,
    price FLOAT64,
    
    -- Local Breakdown
    local_is INT64, local_cp INT64, local_pf INT64, local_ib INT64, local_id INT64,
    local_mf INT64, local_sc INT64, local_fd INT64, local_ot INT64, local_total INT64,
    
    -- Foreign Breakdown
    foreign_is INT64, foreign_cp INT64, foreign_pf INT64, foreign_ib INT64, foreign_id INT64,
    foreign_mf INT64, foreign_sc INT64, foreign_fd INT64, foreign_ot INT64, foreign_total INT64,
    
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY report_date
CLUSTER BY symbol;


-- Table 2: Monthly Shareholder Disclosures (Individual named >=1% and >=5% holders)
CREATE TABLE IF NOT EXISTS `idx_analytics.ksei_shareholder_ownership` (
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
    _source_file STRING,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY report_date
CLUSTER BY symbol, investor_name;
