-- BigQuery DDL for Broker Summary
-- Partitioned by trade_date, Clustered by symbol, broker
CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_id}.broker_summary` (
    trade_date DATE OPTIONS(description="Date of the transaction"),
    symbol STRING OPTIONS(description="IDX stock ticker symbol (e.g. BBRI, BREN)"),
    broker STRING OPTIONS(description="Broker 2-letter code (e.g. CC, YP, AK)"),
    buy_lot INT64,
    buy_value INT64,
    buy_avg_price FLOAT64,
    sell_lot INT64,
    sell_value INT64,
    sell_avg_price FLOAT64,
    net_lot INT64,
    net_value INT64,
    created_at TIMESTAMP
)
PARTITION BY trade_date
CLUSTER BY symbol, broker
OPTIONS (
    description = "Normalized broker summary data scraped from Indopremier"
);
