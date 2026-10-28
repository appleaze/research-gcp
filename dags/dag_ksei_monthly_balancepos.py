"""
Airflow DAG: Monthly KSEI Balance Position Ingestion & DQ Audit.
Self-contained: includes scraping, unzipping, GCS staging, BigQuery ELT & audit.
Zero dependency on external local packages.
"""

from __future__ import annotations

import io
import os
import zipfile
from datetime import datetime, timedelta
from typing import Tuple

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import storage, bigquery

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "project-99693d37-acaf-4ebd-86c")
GCS_BUCKET = os.getenv("GCS_BUCKET_NAME", "idx-ksei-raw-data")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

BASE_URLS = [
    "https://web.ksei.co.id/Download/BalanceposEfek{date_str}.zip",
    "https://www.ksei.co.id/Download/BalanceposEfek{date_str}.zip",
]

RAW_COLUMNS = [
    "date_raw", "symbol_raw", "type_raw", "sec_num_raw", "price_raw",
    "local_is_raw", "local_cp_raw", "local_pf_raw", "local_ib_raw", "local_id_raw",
    "local_mf_raw", "local_sc_raw", "local_fd_raw", "local_ot_raw", "local_total_raw",
    "foreign_is_raw", "foreign_cp_raw", "foreign_pf_raw", "foreign_ib_raw", "foreign_id_raw",
    "foreign_mf_raw", "foreign_sc_raw", "foreign_fd_raw", "foreign_ot_raw", "foreign_total_raw",
]

RAW_SCHEMA_FIELDS = [{"name": col, "type": "STRING", "mode": "NULLABLE"} for col in RAW_COLUMNS]


# =============================================================================
# Helper Functions (Self-Contained Extractor)
# =============================================================================

def _fetch_ksei_zip(target_date: datetime, max_lookback_days: int = 7) -> Tuple[bytes, str]:
    session = requests.Session()
    session.headers.update(HEADERS)
    curr_date = target_date

    for _ in range(max_lookback_days):
        date_str = curr_date.strftime("%Y%m%d")
        for base_tmpl in BASE_URLS:
            url = base_tmpl.format(date_str=date_str)
            try:
                resp = session.get(url, timeout=30)
                if resp.status_code == 200 and len(resp.content) > 1024:
                    print(f"Found KSEI data for date: {date_str} ({len(resp.content):,} bytes)")
                    return resp.content, date_str
            except requests.RequestException:
                pass
        curr_date -= timedelta(days=1)

    raise FileNotFoundError(f"No KSEI Balancepos found near {target_date.strftime('%Y-%m-%d')} (checked back {max_lookback_days} days)")


def _resolve_target_date(exec_date: datetime, conf: dict | None = None) -> datetime:
    if conf and "target_date" in conf:
        raw_val = conf["target_date"]
        if isinstance(raw_val, str):
            return datetime.strptime(raw_val.strip(), "%Y-%m-%d")
        elif isinstance(raw_val, datetime):
            return raw_val

    # If scheduled on the 1st of the month: previous month's last day
    if exec_date.day == 1:
        return exec_date - timedelta(days=1)

    # If triggered mid-month manually, find the last completed month-end
    first_of_curr_month = exec_date.replace(day=1)
    return first_of_curr_month - timedelta(days=1)


def _extract_and_stage(zip_bytes: bytes, found_date_str: str, bucket_name: str) -> str:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        for fname in z.namelist():
            if "balancepos" in fname.lower() and fname.endswith(".txt"):
                txt_bytes = z.read(fname)
                client = storage.Client(project=PROJECT_ID)
                bucket = client.bucket(bucket_name)
                gcs_path = f"ksei/balancepos/{found_date_str}/{fname}"
                blob = bucket.blob(gcs_path)
                blob.upload_from_file(io.BytesIO(txt_bytes), content_type="text/plain")
                uri = f"gs://{bucket_name}/{gcs_path}"
                print(f"Staged to {uri}")
                return gcs_path

    raise ValueError("ZIP contains no Balancepos*.txt")


def fetch_and_stage_ksei_balancepos(target_date: datetime, gcs_bucket_name: str) -> str:
    zip_bytes, found_date = _fetch_ksei_zip(target_date)
    return _extract_and_stage(zip_bytes, found_date, gcs_bucket_name)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="monthly_ksei_balancepos_pipeline",
    default_args=default_args,
    description="Loads monthly KSEI balance position, transforms equities, and runs DQ audit",
    schedule_interval="0 2 1 * *",  # 09:00 WIB on 1st of month
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["idx", "ksei", "balancepos", "monthly"],
) as dag:

    def extract_ksei_callable(**context):
        exec_date = context["execution_date"]
        dag_run = context.get("dag_run")
        conf = dag_run.conf if dag_run else {}
        target_date = _resolve_target_date(exec_date=exec_date, conf=conf)
        print(f"Ingesting KSEI balancepos for month-end: {target_date.strftime('%Y-%m-%d')} (exec_date={exec_date})")
        return fetch_and_stage_ksei_balancepos(target_date=target_date, gcs_bucket_name=GCS_BUCKET)

    extract_ksei_raw = PythonOperator(
        task_id="extract_ksei_balancepos",
        python_callable=extract_ksei_callable,
        provide_context=True,
    )

    load_raw_to_bq = GCSToBigQueryOperator(
        task_id="load_raw_ksei_to_bigquery",
        bucket=GCS_BUCKET,
        source_objects=["{{ ti.xcom_pull(task_ids='extract_ksei_balancepos') or ('ksei/balancepos/' ~ ds_nodash ~ '/*.txt') }}"],
        destination_project_dataset_table=f"{PROJECT_ID}.idx_raw.ksei_balancepos_raw",
        source_format="CSV",
        field_delimiter="|",
        skip_leading_rows=1,
        schema_fields=RAW_SCHEMA_FIELDS,
        allow_jagged_rows=True,
        ignore_unknown_values=True,
        write_disposition="WRITE_APPEND",
    )

    transform_equities = BigQueryInsertJobOperator(
        task_id="transform_ksei_equities",
        configuration={
            "query": {
                "query": f"""
                    MERGE INTO `{PROJECT_ID}.idx_analytics.ksei_shareholder_composition` T
                    USING (
                        SELECT
                            PARSE_DATE('%d-%b-%Y', INITCAP(date_raw)) AS report_date,
                            UPPER(TRIM(symbol_raw)) AS symbol,
                            UPPER(TRIM(type_raw)) AS equity_type,
                            SAFE_CAST(sec_num_raw AS INT64) AS sec_num,
                            SAFE_CAST(price_raw AS FLOAT64) AS price,
                            
                            SAFE_CAST(local_is_raw AS INT64) AS local_is,
                            SAFE_CAST(local_cp_raw AS INT64) AS local_cp,
                            SAFE_CAST(local_pf_raw AS INT64) AS local_pf,
                            SAFE_CAST(local_ib_raw AS INT64) AS local_ib,
                            SAFE_CAST(local_id_raw AS INT64) AS local_id,
                            SAFE_CAST(local_mf_raw AS INT64) AS local_mf,
                            SAFE_CAST(local_sc_raw AS INT64) AS local_sc,
                            SAFE_CAST(local_fd_raw AS INT64) AS local_fd,
                            SAFE_CAST(local_ot_raw AS INT64) AS local_ot,
                            SAFE_CAST(local_total_raw AS INT64) AS local_total,
                            
                            SAFE_CAST(foreign_is_raw AS INT64) AS foreign_is,
                            SAFE_CAST(foreign_cp_raw AS INT64) AS foreign_cp,
                            SAFE_CAST(foreign_pf_raw AS INT64) AS foreign_pf,
                            SAFE_CAST(foreign_ib_raw AS INT64) AS foreign_ib,
                            SAFE_CAST(foreign_id_raw AS INT64) AS foreign_id,
                            SAFE_CAST(foreign_mf_raw AS INT64) AS foreign_mf,
                            SAFE_CAST(foreign_sc_raw AS INT64) AS foreign_sc,
                            SAFE_CAST(foreign_fd_raw AS INT64) AS foreign_fd,
                            SAFE_CAST(foreign_ot_raw AS INT64) AS foreign_ot,
                            SAFE_CAST(foreign_total_raw AS INT64) AS foreign_total,
                            
                            CURRENT_TIMESTAMP() AS processed_at
                        FROM `{PROJECT_ID}.idx_raw.ksei_balancepos_raw`
                        WHERE UPPER(TRIM(type_raw)) = 'EQUITY'
                        QUALIFY ROW_NUMBER() OVER (
                            PARTITION BY PARSE_DATE('%d-%b-%Y', INITCAP(date_raw)), UPPER(TRIM(symbol_raw))
                            ORDER BY _ingested_at DESC
                        ) = 1
                    ) S
                    ON T.report_date = S.report_date AND T.symbol = S.symbol
                    WHEN MATCHED THEN
                        UPDATE SET 
                            equity_type = S.equity_type,
                            sec_num = S.sec_num,
                            price = S.price,
                            local_is = S.local_is,
                            local_cp = S.local_cp,
                            local_pf = S.local_pf,
                            local_ib = S.local_ib,
                            local_id = S.local_id,
                            local_mf = S.local_mf,
                            local_sc = S.local_sc,
                            local_fd = S.local_fd,
                            local_ot = S.local_ot,
                            local_total = S.local_total,
                            foreign_is = S.foreign_is,
                            foreign_cp = S.foreign_cp,
                            foreign_pf = S.foreign_pf,
                            foreign_ib = S.foreign_ib,
                            foreign_id = S.foreign_id,
                            foreign_mf = S.foreign_mf,
                            foreign_sc = S.foreign_sc,
                            foreign_fd = S.foreign_fd,
                            foreign_ot = S.foreign_ot,
                            foreign_total = S.foreign_total,
                            processed_at = CURRENT_TIMESTAMP()
                    WHEN NOT MATCHED THEN
                        INSERT ROW;
                """,
                "useLegacySql": False,
            }
        },
    )

    run_dq_audit = BigQueryInsertJobOperator(
        task_id="run_data_quality_audit",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.idx_analytics.ksei_composition_audit_log` AS
                    SELECT 
                        report_date,
                        COUNT(*) as total_audited,
                        COUNTIF(dq_status = 'PASSED') as total_passed,
                        COUNTIF(dq_status = 'FAILED') as total_failed,
                        CURRENT_TIMESTAMP() as audit_executed_at
                    FROM `{PROJECT_ID}.idx_analytics.dq_ksei_composition_audit`
                    GROUP BY report_date;
                """,
                "useLegacySql": False,
            }
        },
    )

    # Task 4: Missing Snapshots Audit
    audit_missing_snapshots = BigQueryInsertJobOperator(
        task_id="audit_missing_snapshots",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.idx_analytics.ksei_missing_snapshots` AS
                    WITH expected_months AS (
                        SELECT 
                            DATE_SUB(DATE_TRUNC(DATE_ADD(month_start, INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY) AS month_end_date
                        FROM UNNEST(GENERATE_DATE_ARRAY('2026-01-01', CURRENT_DATE(), INTERVAL 1 MONTH)) AS month_start
                        WHERE month_start < DATE_TRUNC(CURRENT_DATE(), MONTH)
                    ),
                    actual_dates AS (
                        SELECT DISTINCT report_date
                        FROM `{PROJECT_ID}.idx_analytics.ksei_shareholder_composition`
                    )
                    SELECT 
                        e.month_end_date AS expected_month_end,
                        CURRENT_TIMESTAMP() AS checked_at
                    FROM expected_months e
                    LEFT JOIN actual_dates a 
                        ON a.report_date BETWEEN DATE_SUB(e.month_end_date, INTERVAL 4 DAY) AND e.month_end_date
                    WHERE a.report_date IS NULL
                    ORDER BY expected_month_end;
                """,
                "useLegacySql": False,
            }
        },
    )

    # Task 5: Auto-backfill missing snapshots
    def backfill_missing_callable(**context):
        bclient = bigquery.Client(project=PROJECT_ID)
        query = f"SELECT expected_month_end FROM `{PROJECT_ID}.idx_analytics.ksei_missing_snapshots`"
        rows = list(bclient.query(query).result())
        for row in rows:
            target_dt = datetime.combine(row.expected_month_end, datetime.min.time())
            try:
                fetch_and_stage_ksei_balancepos(target_dt, GCS_BUCKET)
            except Exception as e:
                print(f"Failed {target_dt}: {e}")

    backfill_missing_task = PythonOperator(
        task_id="backfill_missing_snapshots",
        python_callable=backfill_missing_callable,
        provide_context=True,
    )

    extract_ksei_raw >> load_raw_to_bq >> transform_equities >> run_dq_audit >> audit_missing_snapshots >> backfill_missing_task
