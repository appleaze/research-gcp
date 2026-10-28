# IDX Bandarmology Research (GCP & Cloud Composer Edition)

Production ELT data pipelines, market microstructure analysis, and Wyckoff regime models for the Indonesia Stock Exchange (IDX), orchestrated by **Google Cloud Composer (Managed Apache Airflow)** and stored in **Google BigQuery**.

## Quick Start
```bash
# 1. Install local package
pip install -e .

# 2. Configure .env with your GCP project ID
cp .env.example .env

# 3. Run the KSEI Pipeline (Ingest, Transform & DQ check)
python scripts/run_ksei_pipeline.py --file "G:/Dean/Saham IDX/Research-Bandarmology/Data/BalanceposEfek20260130/Balancepos20260130.txt"
```

## Project Structure
```text
research-gcp/
├── .env.example                                   # Template for environment variables
├── pyproject.toml                                 # Installable package spec (pip install -e .)
├── requirements.txt                               # Local Python dependencies
├── requirements-composer.txt                      # Extra PyPI packages for Cloud Composer
├── PROJECT_CONTEXT.md                             # Project background, architecture & handoff notes
│
├── .github/workflows/
│   ├── deploy_dags.yml                            # CI/CD: Syncs dags/ to Composer GCS bucket
│   └── test_dags.yml                              # CI/CD: Airflow DAG syntax & integrity tests
│
├── dags/                                          # Cloud Composer Airflow DAGs
│   ├── config/
│   │   └── universe.txt                           # Monitored stock tickers
│   ├── dag_daily_broker_summary.py                # EOD broker summary ingestion & mart refresh
│   ├── dag_ksei_monthly_balancepos.py             # Monthly KSEI balance position ELT & DQ audit
│   ├── dag_ksei_monthly_shareholders_1pct.py      # Monthly 1% & 5% shareholder disclosures ELT
│   └── dag_supabase_broker_summary_migration.py   # Supabase to BigQuery broker summary backfill
│
├── bigquery/                                      # BigQuery SQL Schemas & Transformations
│   ├── schemas/raw/                               # Raw Layer (Bronze) DDLs
│   │   ├── broker_summary.sql
│   │   ├── ksei_balancepos_raw.sql
│   │   ├── ksei_shareholder_disclosures_raw.sql
│   │   └── stock_daily_summary.sql
│   ├── silver/                                    # Silver Layer DDLs & MERGE Transformations
│   │   ├── 01_create_silver_tables.sql
│   │   ├── 02_transform_ksei_composition.sql
│   │   ├── 03_transform_shareholder_ownership.sql
│   │   ├── 04_verify_silver_layer.sql
│   │   └── README.md
│   └── marts/                                     # Gold Layer Analytical Views & Verification
│       ├── client_proof_queries.sql
│       ├── dq_ksei_composition_audit.sql
│       ├── mart_broker_event_study.sql
│       ├── mart_complete_shareholder_breakdown.sql
│       ├── mart_true_float.sql
│       ├── mart_true_retail.sql
│       └── verify_gold_layer.sql
│
├── src/bandarmology/                              # Core Python Package
│   ├── __init__.py
│   ├── pipeline.py                                # Unified market data fetcher (BigQuery + yfinance)
│   ├── analysis/
│   │   ├── __init__.py
│   │   ├── backtest_broker_strategy.py            # Strategy backtester
│   │   └── broker_event_study.py                  # Win-rate & t-test event study engine
│   ├── clients/
│   │   ├── __init__.py
│   │   ├── bigquery.py                            # BigQueryClient wrapper
│   │   └── gcs.py                                 # GCSClient wrapper
│   └── extractors/
│       ├── __init__.py
│       ├── pdf_shareholders.py                    # Parser for 1% & 5% shareholder disclosure PDFs
│       └── scrape_ksei.py                         # KSEI download & ZIP extractor
│
├── scripts/                                       # Local CLI Runner Scripts
│   ├── run_ksei_pipeline.py                       # Local runner for KSEI Balancepos ELT & DQ check
│   └── run_shareholders_pipeline.py               # Local runner for 1% & 5% PDF disclosures ELT
│
└── docs/                                          # Documentation directory
```
