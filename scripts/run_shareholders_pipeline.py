from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import pandas as pd

# Add repo root to sys.path
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.bandarmology.extractors.pdf_shareholders import parse_pdf_to_dataframe

load_dotenv()


def get_bigquery_client() -> bigquery.Client:
    """Initializes BigQuery client using ADC or SERVICE_ACCOUNT_KEY."""
    project_id = os.getenv("GCP_PROJECT_ID")
    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    if cred_path and os.path.exists(cred_path):
        client = bigquery.Client.from_service_account_json(cred_path, project=project_id)
    else:
        client = bigquery.Client(project=project_id)

    print(f"[OK] Connected to BigQuery: {client.project}")
    return client


def ensure_dataset(client: bigquery.Client, dataset_name: str, location: str = "asia-southeast2") -> None:
    """Ensures a BigQuery dataset exists."""
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
    """Provisions all required DDLs across Bronze, Silver, and Gold."""
    print("\n--- Step 1: Applying Table & View DDLs ---")
    ensure_dataset(client, "idx_raw")
    ensure_dataset(client, "idx_analytics")

    # 1. Bronze Table DDL
    bronze_ddl_path = repo_root / "bigquery" / "schemas" / "raw" / "ksei_shareholder_disclosures_raw.sql"
    with open(bronze_ddl_path, "r", encoding="utf-8") as f:
        ddl_bronze = f.read().replace("{project_id}", client.project)
    client.query(ddl_bronze).result()
    print("[OK] Bronze table ready: idx_raw.ksei_shareholder_disclosures_raw")

    # 2. Silver Table DDL
    silver_ddl_path = repo_root / "bigquery" / "schemas" / "raw" / "ksei_shareholder_ownership.sql"
    with open(silver_ddl_path, "r", encoding="utf-8") as f:
        ddl_silver = f.read().replace("{project_id}", client.project)
    client.query(ddl_silver).result()
    print("[OK] Silver table ready: idx_analytics.ksei_shareholder_ownership")

    # 3. Gold Marts
    mart_breakdown_path = repo_root / "bigquery" / "marts" / "mart_complete_shareholder_breakdown.sql"
    with open(mart_breakdown_path, "r", encoding="utf-8") as f:
        ddl_mart_b = f.read().replace("{project_id}", client.project)
    client.query(ddl_mart_b).result()
    print("[OK] Gold view ready: idx_analytics.mart_complete_shareholder_breakdown")

    mart_retail_path = repo_root / "bigquery" / "marts" / "mart_true_retail_dominance.sql"
    with open(mart_retail_path, "r", encoding="utf-8") as f:
        ddl_mart_r = f.read().replace("{project_id}", client.project)
    client.query(ddl_mart_r).result()
    print("[OK] Gold view ready: idx_analytics.mart_true_retail_dominance")


def step2_extract_and_ingest_bronze(client: bigquery.Client, pdf_path: Path) -> int:
    """Extracts tables from PDF and appends records into Bronze landing table."""
    print(f"\n--- Step 2: Extracting PDF & Ingesting to Bronze: {pdf_path.name} ---")
    df = parse_pdf_to_dataframe(pdf_path)

    if df.empty:
        print("[WARN] No shareholder records found in PDF!")
        return 0

    print(f"[EXTRACTED] Found {len(df):,} records across pages.")
    table_id = f"{client.project}.idx_raw.ksei_shareholder_disclosures_raw"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print(f"[SUCCESS] Ingested {len(df):,} records into {table_id}")
    return len(df)


def step3_run_silver_transformation(client: bigquery.Client) -> None:
    """Transforms raw PDF string records into typed, deduplicated Silver ownership table."""
    print("\n--- Step 3: Running Silver ELT SQL Transformation ---")

    transform_sql = f"""
    MERGE INTO `{client.project}.idx_analytics.ksei_shareholder_ownership` T
    USING (
        WITH raw_parsed AS (
            SELECT
                PARSE_DATE('%d-%b-%Y', INITCAP(date_raw)) AS report_date,
                UPPER(TRIM(symbol_raw)) AS symbol,
                -- In lamp1.pdf issuer_and_investor contains Issuer Name followed by Investor Name.
                -- We extract clean investor name by removing the symbol or prefix if necessary.
                TRIM(issuer_and_investor_raw) AS investor_name,
                UPPER(TRIM(COALESCE(investor_type_raw, 'OT'))) AS investor_type,
                CONCAT(
                    CASE WHEN UPPER(TRIM(local_foreign_raw)) = 'L' THEN 'LOCAL_' ELSE 'FOREIGN_' END,
                    UPPER(TRIM(COALESCE(investor_type_raw, 'OT')))
                ) AS category,
                UPPER(TRIM(local_foreign_raw)) = 'A' AS is_foreign,
                CASE 
                    WHEN SAFE_CAST(REPLACE(percentage_raw, ',', '.') AS FLOAT64) >= 5.0 THEN '>5%'
                    ELSE '>1%'
                END AS holder_type,
                SAFE_CAST(REPLACE(holdings_scripless_raw, '.', '') AS INT64) AS scripless_shares,
                SAFE_CAST(REPLACE(holdings_scrip_raw, '.', '') AS INT64) AS scrip_shares,
                SAFE_CAST(REPLACE(total_shares_raw, '.', '') AS INT64) AS total_shares,
                SAFE_CAST(REPLACE(percentage_raw, ',', '.') AS FLOAT64) AS ownership_percentage,
                _source_file,
                _ingested_at
            FROM `{client.project}.idx_raw.ksei_shareholder_disclosures_raw`
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
        INSERT (
            report_date,
            symbol,
            investor_name,
            investor_type,
            category,
            is_foreign,
            holder_type,
            scripless_shares,
            scrip_shares,
            total_shares,
            ownership_percentage,
            _source_file,
            processed_at
        )
        VALUES (
            S.report_date,
            S.symbol,
            S.investor_name,
            S.investor_type,
            S.category,
            S.is_foreign,
            S.holder_type,
            S.scripless_shares,
            S.scrip_shares,
            S.total_shares,
            S.ownership_percentage,
            S._source_file,
            CURRENT_TIMESTAMP()
        );
    """
    job = client.query(transform_sql)
    job.result()
    print("[SUCCESS] Silver Transformation complete: idx_analytics.ksei_shareholder_ownership")


def step4_run_summary_audit(client: bigquery.Client) -> None:
    """Executes a summary audit verifying the ingested holdings and retail dominance metrics."""
    print("\n--- Step 4: Executing Shareholder Audit & Retail Dominance Summary ---")

    audit_sql = f"""
    SELECT 
        report_date,
        COUNT(DISTINCT symbol) AS active_stocks,
        COUNT(*) AS total_disclosed_holders,
        COUNTIF(holder_type = '>5%') AS block_holders_5pct,
        COUNTIF(holder_type = '>1%') AS substantial_holders_1to5pct,
        ROUND(AVG(ownership_percentage), 2) AS avg_holding_pct
    FROM `{client.project}.idx_analytics.ksei_shareholder_ownership`
    GROUP BY report_date
    ORDER BY report_date DESC;
    """
    df_audit = client.query(audit_sql).to_dataframe()
    print("\n================ MONTHLY SHAREHOLDER SUMMARY ================")
    print(df_audit.to_string(index=False))
    print("=============================================================")


def main():
    parser = argparse.ArgumentParser(description="IDX Monthly Shareholder Disclosures Pipeline (1% & 5%)")
    parser.add_argument("--pdf", required=True, help="Path to monthly lamp1.pdf file")
    args = parser.parse_args()

    pdf_path = Path(args.pdf)
    if not pdf_path.exists():
        print(f"[ERROR] File does not exist: {pdf_path}")
        sys.exit(1)

    client = get_bigquery_client()
    step1_apply_ddls(client, REPO_ROOT)
    step2_extract_and_ingest_bronze(client, pdf_path)
    step3_run_silver_transformation(client)
    step4_run_summary_audit(client)


if __name__ == "__main__":
    main()
