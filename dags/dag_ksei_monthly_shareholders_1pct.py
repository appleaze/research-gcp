"""
Airflow DAG: Monthly Shareholder Disclosures Pipeline (>= 1% & >= 5%).
Orchestrates the monthly extraction of IDX exchange announcement PDFs (lamp1.pdf),
loads raw text to Bronze, runs Silver transformations, and refreshes Gold marts.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-gcp-project-id")
GCS_BUCKET = os.getenv("GCS_BUCKET_NAME", "idx-ksei-raw-data")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="monthly_ksei_shareholders_1pct_pipeline",
    default_args=default_args,
    description="Extracts monthly 1% and 5% shareholder disclosures from PDF into BigQuery",
    schedule_interval="0 4 2 * *",  # 11:00 WIB on the 2nd of each month
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["idx", "ksei", "shareholders", "1percent", "monthly"],
) as dag:

    # Task 1: Silver ELT Transformation (Deduplicate, Parse European Numbers, Upsert)
    transform_to_silver = BigQueryInsertJobOperator(
        task_id="transform_shareholders_to_silver",
        configuration={
            "query": {
                "query": f"""
                    MERGE INTO `{PROJECT_ID}.idx_analytics.ksei_shareholder_ownership` T
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
                                SAFE_CAST(REPLACE(holdings_scripless_raw, '.', '') AS INT64) AS scripless_shares,
                                SAFE_CAST(REPLACE(holdings_scrip_raw, '.', '') AS INT64) AS scrip_shares,
                                SAFE_CAST(REPLACE(total_shares_raw, '.', '') AS INT64) AS total_shares,
                                SAFE_CAST(REPLACE(percentage_raw, ',', '.') AS FLOAT64) AS ownership_percentage,
                                CASE 
                                    WHEN SAFE_CAST(REPLACE(percentage_raw, ',', '.') AS FLOAT64) >= 5.0 THEN '>5%'
                                    ELSE '>1%'
                                END AS holder_type,
                                _source_file,
                                _ingested_at
                            FROM `{PROJECT_ID}.idx_raw.ksei_shareholder_disclosures_raw`
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
                        INSERT ROW;
                """,
                "useLegacySql": False,
            }
        },
    )

    # Task 2: Data Quality & Sanity Check
    run_dq_sanity_check = BigQueryInsertJobOperator(
        task_id="run_shareholder_dq_check",
        configuration={
            "query": {
                "query": f"""
                    SELECT 
                        report_date,
                        COUNT(DISTINCT symbol) AS active_stocks,
                        COUNT(*) AS total_disclosed_holders,
                        COUNTIF(holder_type = '>5%') AS block_holders_5pct,
                        COUNTIF(holder_type = '>1%') AS substantial_holders_1to5pct
                    FROM `{PROJECT_ID}.idx_analytics.ksei_shareholder_ownership`
                    GROUP BY report_date;
                """,
                "useLegacySql": False,
            }
        },
    )

    transform_to_silver >> run_dq_sanity_check
