"""
Airflow DAG: Chunked Supabase to BigQuery Migration for broker_summary.
Extracts rows in memory-safe chunks from Supabase PostgreSQL,
stages single Parquet in GCS, loads into BigQuery partitioned table.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "project-99693d37-acaf-4ebd-86c")
DATASET_ID = os.getenv("BQ_DATASET", "idx_raw")
GCS_BUCKET = os.getenv("GCS_BUCKET_NAME", "idx-ksei-raw-data")

POSTGRES_CONN_ID = "supabase_postgres"
GCP_CONN_ID = "google_cloud_default"

CHUNK_SIZE = 25000
GCS_OBJECT_PATH = "raw/supabase/broker_summary/broker_summary.parquet"
LOCAL_TMP_PARQUET = "/tmp/broker_summary_chunked.parquet"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def test_supabase_callable():
    """Smoke test: verify Supabase Postgres connection and schema."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    df = hook.get_pandas_df("SELECT * FROM broker_summary LIMIT 5;")
    print(f"Supabase connection successful. Sample 5 rows:\n{df}")
    return len(df)

def extract_chunked_to_gcs_callable():
    """Stream table in chunks via server-side cursor to avoid memory blowup."""
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()

    sql = "SELECT * FROM broker_summary ORDER BY trade_date;"
    
    if os.path.exists(LOCAL_TMP_PARQUET):
        os.remove(LOCAL_TMP_PARQUET)

    writer = None
    total_rows = 0

    print(f"Starting chunked extract with chunk size {CHUNK_SIZE}...")
    
    # Server-side stream cursor
    with engine.connect().execution_options(stream_results=True) as conn:
        for chunk_idx, chunk_df in enumerate(pd.read_sql(sql, conn, chunksize=CHUNK_SIZE)):
            row_count = len(chunk_df)
            total_rows += row_count
            print(f"Processing chunk {chunk_idx + 1}: {row_count} rows (Cumulative: {total_rows})")

            # Standardize datatypes for BigQuery
            chunk_df["trade_date"] = pd.to_datetime(chunk_df["trade_date"]).dt.date
            if "created_at" in chunk_df.columns:
                chunk_df["created_at"] = pd.to_datetime(chunk_df["created_at"])

            table = pa.Table.from_pandas(chunk_df, preserve_index=False)
            
            if writer is None:
                writer = pq.ParquetWriter(LOCAL_TMP_PARQUET, table.schema, compression="SNAPPY")
            
            writer.write_table(table)

    if writer:
        writer.close()

    print(f"Extraction complete. Total rows written: {total_rows}")

    print(f"Uploading {LOCAL_TMP_PARQUET} to gs://{GCS_BUCKET}/{GCS_OBJECT_PATH}...")
    gcs_hook.upload(
        bucket_name=GCS_BUCKET,
        object_name=GCS_OBJECT_PATH,
        filename=LOCAL_TMP_PARQUET,
    )
    
    if os.path.exists(LOCAL_TMP_PARQUET):
        os.remove(LOCAL_TMP_PARQUET)

with DAG(
    dag_id="dag_supabase_broker_summary_migration",
    default_args=default_args,
    description="Chunked backfill of broker_summary from Supabase to BigQuery",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,  # Trigger manually for migration
    catchup=False,
    tags=["supabase", "migration", "broker_summary"],
) as dag:

    test_conn = PythonOperator(
        task_id="test_supabase_connection",
        python_callable=test_supabase_callable,
    )

    extract_chunked = PythonOperator(
        task_id="extract_chunked_to_gcs",
        python_callable=extract_chunked_to_gcs_callable,
    )

    load_gcs_to_bq = GCSToBigQueryOperator(
        task_id="load_gcs_to_bigquery",
        gcp_conn_id=GCP_CONN_ID,
        bucket=GCS_BUCKET,
        source_objects=[GCS_OBJECT_PATH],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.broker_summary",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",  
    )

    verify_load = BigQueryInsertJobOperator(
        task_id="verify_bigquery_counts",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "query": {
                "query": f"""
                    SELECT 
                        COUNT(*) AS total_rows,
                        MIN(trade_date) AS min_date,
                        MAX(trade_date) AS max_date,
                        COUNT(DISTINCT symbol) AS unique_symbols
                    FROM `{PROJECT_ID}.{DATASET_ID}.broker_summary`;
                """,
                "useLegacySql": False,
            }
        },
    )

    test_conn >> extract_chunked >> load_gcs_to_bq >> verify_load
