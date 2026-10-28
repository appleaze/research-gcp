-- BigQuery Mart: Broker Daily Aggregation and Net Metrics
-- Pre-aggregates Top 5 Net Buyers and Sellers per Ticker
CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.mart_broker_flow_summary` AS
WITH ranked_buyers AS (
    SELECT
        trade_date,
        symbol,
        broker,
        net_value,
        ROW_NUMBER() OVER (PARTITION BY trade_date, symbol ORDER BY net_value DESC) as buy_rank
    FROM `{project_id}.{dataset_id}.broker_summary`
    WHERE net_value > 0
),
ranked_sellers AS (
    SELECT
        trade_date,
        symbol,
        broker,
        net_value,
        ROW_NUMBER() OVER (PARTITION BY trade_date, symbol ORDER BY net_value ASC) as sell_rank
    FROM `{project_id}.{dataset_id}.broker_summary`
    WHERE net_value < 0
),
top_aggregates AS (
    SELECT
        trade_date,
        symbol,
        SUM(CASE WHEN buy_rank <= 3 THEN net_value ELSE 0 END) AS top3_buy_val,
        SUM(CASE WHEN buy_rank <= 5 THEN net_value ELSE 0 END) AS top5_buy_val
    FROM ranked_buyers
    GROUP BY trade_date, symbol
),
bottom_aggregates AS (
    SELECT
        trade_date,
        symbol,
        SUM(CASE WHEN sell_rank <= 3 THEN net_value ELSE 0 END) AS top3_sell_val,
        SUM(CASE WHEN sell_rank <= 5 THEN net_value ELSE 0 END) AS top5_sell_val
    FROM ranked_sellers
    GROUP BY trade_date, symbol
)
SELECT
    COALESCE(t.trade_date, b.trade_date) AS trade_date,
    COALESCE(t.symbol, b.symbol) AS symbol,
    COALESCE(t.top3_buy_val, 0) AS top3_buy_val,
    COALESCE(t.top5_buy_val, 0) AS top5_buy_val,
    COALESCE(b.top3_sell_val, 0) AS top3_sell_val,
    COALESCE(b.top5_sell_val, 0) AS top5_sell_val,
    (COALESCE(t.top5_buy_val, 0) + COALESCE(b.top5_sell_val, 0)) AS top5_accumulation_val
FROM top_aggregates t
FULL OUTER JOIN bottom_aggregates b
  ON t.trade_date = b.trade_date AND t.symbol = b.symbol;
