-- =============================================================================
-- RAW DDL: KSEI Monthly Balance Position (Raw Pipe-Delimited Ingestion)
-- Location: idx_raw.ksei_balancepos_raw
-- Note: All fields stored as STRING to ensure 100% ingestion success without type errors.
-- =============================================================================

CREATE TABLE IF NOT EXISTS `{project_id}.idx_raw.ksei_balancepos_raw` (
    date_raw STRING OPTIONS(description="Date string e.g. 30-JAN-2026"),
    symbol_raw STRING OPTIONS(description="Stock or instrument code e.g. AADI"),
    type_raw STRING OPTIONS(description="EQUITY, BOND, WARRANT, etc."),
    sec_num_raw STRING OPTIONS(description="Total listed shares"),
    price_raw STRING OPTIONS(description="Price per share"),
    
    -- Local Shareholder Breakdown
    local_is_raw STRING OPTIONS(description="Local Insurance"),
    local_cp_raw STRING OPTIONS(description="Local Corporate"),
    local_pf_raw STRING OPTIONS(description="Local Pension Fund"),
    local_ib_raw STRING OPTIONS(description="Local Bank"),
    local_id_raw STRING OPTIONS(description="Local Individual"),
    local_mf_raw STRING OPTIONS(description="Local Mutual Fund"),
    local_sc_raw STRING OPTIONS(description="Local Securities Company"),
    local_fd_raw STRING OPTIONS(description="Local Foundation"),
    local_ot_raw STRING OPTIONS(description="Local Others"),
    local_total_raw STRING OPTIONS(description="Local Total"),
    
    -- Foreign Shareholder Breakdown
    foreign_is_raw STRING OPTIONS(description="Foreign Insurance"),
    foreign_cp_raw STRING OPTIONS(description="Foreign Corporate"),
    foreign_pf_raw STRING OPTIONS(description="Foreign Pension Fund"),
    foreign_ib_raw STRING OPTIONS(description="Foreign Bank"),
    foreign_id_raw STRING OPTIONS(description="Foreign Individual"),
    foreign_mf_raw STRING OPTIONS(description="Foreign Mutual Fund"),
    foreign_sc_raw STRING OPTIONS(description="Foreign Securities Company"),
    foreign_fd_raw STRING OPTIONS(description="Foreign Foundation"),
    foreign_ot_raw STRING OPTIONS(description="Foreign Others"),
    foreign_total_raw STRING OPTIONS(description="Foreign Total"),
    
    -- Ingestion Metadata
    _source_file STRING OPTIONS(description="Source filename (e.g. Balancepos20260130.txt)"),
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Timestamp when loaded into BigQuery")
)
OPTIONS (
    description = "Bronze / Raw landing table for KSEI monthly balance position text file"
);
