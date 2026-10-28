-- =============================================================================
-- BRONZE LAYER DDL: IDX / KSEI Monthly Shareholder Disclosures (>=1% and >=5%)
-- Location: idx_raw.ksei_shareholder_disclosures_raw
-- Mode: Append-Only, Permissive STRING schema, Auditable Lineage
-- =============================================================================

CREATE TABLE IF NOT EXISTS `{project_id}.idx_raw.ksei_shareholder_disclosures_raw` (
    date_raw STRING OPTIONS(description="Raw date from PDF (e.g. 31-Mar-2026)"),
    symbol_raw STRING OPTIONS(description="Stock ticker symbol (e.g. AADI, BBCA)"),
    issuer_and_investor_raw STRING OPTIONS(description="Raw combined text of Issuer Name and Investor Name"),
    investor_type_raw STRING OPTIONS(description="KSEI Investor Type code (CP, ID, IS, PF, MF, etc.)"),
    local_foreign_raw STRING OPTIONS(description="L (Local) or A (Asing/Foreign)"),
    domicile_raw STRING OPTIONS(description="Domicile or nationality string"),
    holdings_scripless_raw STRING OPTIONS(description="Raw string of scripless shares"),
    holdings_scrip_raw STRING OPTIONS(description="Raw string of scrip (warkat) shares"),
    total_shares_raw STRING OPTIONS(description="Raw string of total shares held"),
    percentage_raw STRING OPTIONS(description="Raw string of percentage with comma decimal (e.g. 41,10)"),
    
    -- Ingestion Metadata
    _source_file STRING OPTIONS(description="Originating PDF filename"),
    _page_number INT64 OPTIONS(description="Page number in the PDF where the record appeared"),
    _ingested_at TIMESTAMP OPTIONS(description="UTC timestamp when loaded into Bronze")
);
