from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import bigquery
import pandas as pd
# pyrefly: ignore[missing-import]
from google.api_core.exceptions import NotFound

# Load environment variables
load_dotenv()


def get_bigquery_client() -> bigquery.Client:
    """Initializes BigQuery client using ADC or SERVICE_ACCOUNT_KEY."""
    project_id = os.getenv("GCP_PROJECT_ID")
    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    
    if cred_path:
        if not os.path.exists(cred_path):
            raise FileNotFoundError(
                f"GOOGLE_APPLICATION_CREDENTIALS is set to '{cred_path}', but that file does not exist.\n"
                "Please check the path in your .env file."
            )
        client = bigquery.Client.from_service_account_json(cred_path, project=project_id)
    else:
        try:
            client = bigquery.Client(project=project_id)
        except Exception as e:
            raise RuntimeError(
                "\n[ERROR] No Google Cloud credentials found!\n"
                "Please do ONE of the following:\n"
                "  Option A: Provide a Service Account Key in your .env file:\n"
                "            GOOGLE_APPLICATION_CREDENTIALS=\"C:/path/to/your-service-account-key.json\"\n"
                "  Option B: Install Google Cloud CLI and authenticate locally:\n"
                "            gcloud auth application-default login\n"
            ) from e
        
    print(f"[OK] Connected to Google Cloud Project: {client.project}")
    return client


def ensure_dataset(client: bigquery.Client, dataset_name: str, location: str = "asia-southeast2") -> None:
    """Ensures a BigQuery dataset exists, creates it if not."""
    dataset_id = f"{client.project}.{dataset_name}"
    try:
        client.get_dataset(dataset_id)
        print(f"[OK] Dataset {dataset_name} exists.")
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = location
        client.create_dataset(dataset, timeout=30)
        print(f"[CREATED] Dataset {dataset_name} in {location}.")


def step1_apply_ddls(client: bigquery.Client, repo_root: Path) -> None:
    """Executes DDL scripts for raw and analytics tables."""
    print("\n--- Step 1: Applying Table DDLs ---")
    ensure_dataset(client, "idx_raw")
    ensure_dataset(client, "idx_analytics")

    raw_ddl_path = repo_root / "bigquery" / "schemas" / "raw" / "ksei_balancepos_raw.sql"
    with open(raw_ddl_path, "r", encoding="utf-8") as f:
        ddl_raw = f.read().replace("{project_id}", client.project)
    client.query(ddl_raw).result()
    print("[OK] Raw table provisioned: idx_raw.ksei_balancepos_raw")

    clean_ddl = f"""
    CREATE TABLE IF NOT EXISTS `{client.project}.idx_analytics.ksei_shareholder_composition` (
        report_date DATE OPTIONS(description="Position date from KSEI"),
        symbol STRING OPTIONS(description="Stock ticker e.g. AADI, BBCA"),
        equity_type STRING,
        sec_num INT64,
        price FLOAT64,
        
        -- Local Breakdown
        local_is INT64, local_cp INT64, local_pf INT64, local_ib INT64, local_id INT64,
        local_mf INT64, local_sc INT64, local_fd INT64, local_ot INT64, local_total INT64,
        
        -- Foreign Breakdown
        foreign_is INT64, foreign_cp INT64, foreign_pf INT64, foreign_ib INT64, foreign_id INT64,
        foreign_mf INT64, foreign_sc INT64, foreign_fd INT64, foreign_ot INT64, foreign_total INT64,
        
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    PARTITION BY report_date
    CLUSTER BY symbol;
    """
    client.query(clean_ddl).result()
    print("[OK] Clean table provisioned: idx_analytics.ksei_shareholder_composition")


RAW_COLUMNS = [
    "date_raw", "symbol_raw", "type_raw", "sec_num_raw", "price_raw",
    "local_is_raw", "local_cp_raw", "local_pf_raw", "local_ib_raw", "local_id_raw",
    "local_mf_raw", "local_sc_raw", "local_fd_raw", "local_ot_raw", "local_total_raw",
    "foreign_is_raw", "foreign_cp_raw", "foreign_pf_raw", "foreign_ib_raw", "foreign_id_raw",
    "foreign_mf_raw", "foreign_sc_raw", "foreign_fd_raw", "foreign_ot_raw", "foreign_total_raw",
]


def step2_ingest_raw_file(client: bigquery.Client, file_path: Path) -> int:
    """Loads raw pipe-delimited Balancepos text file into idx_raw.ksei_balancepos_raw."""
    print(f"\n--- Step 2: Loading Raw File: {file_path.name} ---")
    table_id = f"{client.project}.idx_raw.ksei_balancepos_raw"

    # Read 25 raw pipe-delimited columns as string and enrich with metadata
    df = pd.read_csv(file_path, sep="|", dtype=str, names=RAW_COLUMNS, header=0)
    df["_source_file"] = file_path.name
    df["_ingested_at"] = pd.Timestamp.now(tz="UTC")

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print(f"[SUCCESS] Ingested {len(df):,} raw records into {table_id}")
    return len(df)


def step3_run_transformation(client: bigquery.Client) -> None:
    """Transforms raw string data into typed, partitioned analytics table."""
    print("\n--- Step 3: Running ELT SQL Transformation ---")
    
    transform_sql = f"""
    MERGE INTO `{client.project}.idx_analytics.ksei_shareholder_composition` T
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
        FROM `{client.project}.idx_raw.ksei_balancepos_raw`
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
    """
    query_job = client.query(transform_sql)
    query_job.result()
    print("[SUCCESS] Transformation complete (Deduplicated & Merged into idx_analytics.ksei_shareholder_composition).")


def step4_run_data_quality_audit(client: bigquery.Client, repo_root: Path) -> None:
    """Deploys DQ audit view and runs validation queries."""
    print("\n--- Step 4: Executing Data Quality (DQ) Audit ---")
    
    dq_view_path = repo_root / "bigquery" / "marts" / "dq_ksei_composition_audit.sql"
    with open(dq_view_path, "r", encoding="utf-8") as f:
        dq_sql = f.read().replace("{project_id}", client.project)
    client.query(dq_sql).result()
    print("[OK] Data Quality view ready: idx_analytics.dq_ksei_composition_audit")

    # Run DQ Summary Check
    check_sql = f"""
    SELECT 
        report_date,
        COUNT(*) as total_stocks_audited,
        COUNTIF(dq_status = 'PASSED') as math_validation_passed,
        COUNTIF(dq_status = 'FAILED') as math_validation_failed,
        COUNTIF(local_discrepancy != 0) as local_math_errors,
        COUNTIF(foreign_discrepancy != 0) as foreign_math_errors,
        COUNTIF(non_scriptless_shares > 0) as stocks_with_non_scriptless,
        ROUND(AVG(non_scriptless_pct), 2) as avg_non_scriptless_pct,
        ROUND(AVG(scriptless_pct), 2) as avg_scriptless_pct
    FROM `{client.project}.idx_analytics.dq_ksei_composition_audit`
    GROUP BY report_date
    ORDER BY report_date DESC;
    """
    df_audit = client.query(check_sql).to_dataframe()
    print("\n================ DATA QUALITY AUDIT RESULTS ================")
    print(df_audit.to_string(index=False))
    print("============================================================")


def main():
    parser = argparse.ArgumentParser(description="KSEI Balance Position BigQuery Ingestion & DQ Pipeline")
    parser.add_argument("--file", required=True, help="Path to raw Balancepos text file")
    args = parser.parse_args()

    file_path = Path(args.file)
    if not file_path.exists():
        print(f"[ERROR] File does not exist: {file_path}")
        sys.exit(1)

    repo_root = Path(__file__).resolve().parent.parent
    client = get_bigquery_client()

    step1_apply_ddls(client, repo_root)
    step2_ingest_raw_file(client, file_path)
    step3_run_transformation(client)
    step4_run_data_quality_audit(client, repo_root)


if __name__ == "__main__":
    main()
