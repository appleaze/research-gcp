CREATE OR REPLACE TABLE `idx_raw.ksei_balancepos_raw` (
    date_raw STRING,
    symbol_raw STRING,
    type_raw STRING,
    sec_num_raw STRING,
    price_raw STRING,
    
    -- Local breakdown
    local_is_raw STRING,
    local_cp_raw STRING,
    local_pf_raw STRING,
    local_ib_raw STRING,
    local_id_raw STRING,
    local_mf_raw STRING,
    local_sc_raw STRING,
    local_fd_raw STRING,
    local_ot_raw STRING,
    local_total_raw STRING,
    
    -- Foreign breakdown
    foreign_is_raw STRING,
    foreign_cp_raw STRING,
    foreign_pf_raw STRING,
    foreign_ib_raw STRING,
    foreign_id_raw STRING,
    foreign_mf_raw STRING,
    foreign_sc_raw STRING,
    foreign_fd_raw STRING,
    foreign_ot_raw STRING,
    foreign_total_raw STRING,
    
    -- Metadata
    _source_file STRING,
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);