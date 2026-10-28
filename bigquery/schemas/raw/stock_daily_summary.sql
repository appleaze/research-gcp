-- BigQuery DDL for Stock Daily Summary
CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.stock_daily_summary` (
    trade_date DATE OPTIONS(description="Date of the transaction"),
    symbol STRING OPTIONS(description="IDX stock ticker symbol"),
    board STRING OPTIONS(description="Board: RG, NG, TN"),
    total_value INT64 OPTIONS(description="Total transaction value in IDR"),
    foreign_net_value INT64 OPTIONS(description="Foreign net transaction value in IDR"),
    total_lot INT64 OPTIONS(description="Total lots traded"),
    average_price FLOAT64,
    created_at TIMESTAMP
)
PARTITION BY trade_date
CLUSTER BY symbol
OPTIONS (
    description = "Daily stock transaction header summary from Indopremier"
);
