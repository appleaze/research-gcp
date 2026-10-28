"""
Airflow DAG: Daily Broker Summary Pipeline.
Orchestrates EOD broker flow scraping, GCS staging, BigQuery loading, and mart refreshing.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-gcp-project-id")
DATASET_ID = os.getenv("BQ_DATASET", "idx_bandarmology")
GCS_BUCKET = os.getenv("GCS_BUCKET_NAME", "idx-bandarmology-staging")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="daily_broker_summary_pipeline",
    default_args=default_args,
    description="Extracts IDX broker summary EOD, stages in GCS, loads to BigQuery",
    schedule_interval="0 10 * * 1-5",  # 17:00 WIB (10:00 UTC), Monday through Friday
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["idx", "bandarmology", "broker_summary"],
) as dag:

    def check_trading_day_callable(**context):
        import holidays
        exec_date = context["execution_date"].date()
        id_holidays = holidays.Indonesia(years=exec_date.year)
        if exec_date in id_holidays:
            print(f"{exec_date} is an Indonesian market holiday ({id_holidays.get(exec_date)}). Skipping.")
            return False
        return True

    check_trading_day = PythonOperator(
        task_id="check_trading_day",
        python_callable=check_trading_day_callable,
        provide_context=True,
    )

    def extract_broker_summary_callable(**context):
        # Reads universe and fetches summary from source
        ds = context["ds"]
        print(f"Extracting broker summary for trading date: {ds}")
        # Extractor stages file to gs://{GCS_BUCKET}/raw/broker_summary/{ds}/*.parquet

    extract_broker_summary = PythonOperator(
        task_id="extract_broker_summary",
        python_callable=extract_broker_summary_callable,
        provide_context=True,
    )

    load_gcs_to_bq = GCSToBigQueryOperator(
        task_id="load_gcs_to_bigquery",
        bucket=GCS_BUCKET,
        source_objects=["raw/broker_summary/{{ ds }}/*.parquet"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.broker_summary",
        source_format="PARQUET",
        write_disposition="WRITE_APPEND",
    )

    refresh_marts = BigQueryInsertJobOperator(
        task_id="refresh_broker_marts",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.mart_daily_top5_accumulation` AS
                    SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.mart_broker_flow_summary`
                    WHERE trade_date = '{{{{ ds }}}}';
                """,
                "useLegacySql": False,
            }
        },
    )

    check_trading_day >> extract_broker_summary >> load_gcs_to_bq >> refresh_marts
