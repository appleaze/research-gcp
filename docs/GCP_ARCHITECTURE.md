# IDX Bandarmology — GCP Architecture & Cloud Composer Guide

This repository contains the production data pipelines, analytical models, and orchestration for the **IDX Bandarmology & Microstructure Research Platform**, built on **Google BigQuery** and **Google Cloud Composer** (Managed Apache Airflow).

---

## 1. Architecture Overview

```
[ External Data Sources ]
  - IndoPremier Scrapers (Broker Summary)
  - yfinance (OHLCV prices)
  - KSEI Shareholder Composition (Balancepos TXT & 1%/5% PDFs)
           │
           ▼
[ Google Cloud Storage (GCS) ]
  gs://<landing-bucket>/raw/<pipeline>/<YYYY-MM-DD>/...
           │
           ▼  (GCSToBigQueryOperator)
[ Google BigQuery: Raw Dataset ]
  - `broker_summary` (Partitioned by trade_date, Clustered by symbol, broker)
  - `stock_daily_summary` (Partitioned by trade_date, Clustered by symbol)
  - `ksei_balancepos_raw` (Partitioned by report_date, Clustered by symbol)
  - `ksei_shareholder_disclosures_raw`
           │
           ▼  (BigQueryInsertJobOperator / SQL Transformations)
[ Google BigQuery: Silver & Marts Dataset ]
  - `ksei_shareholder_composition` (Silver layer, typed)
  - `ksei_shareholder_ownership` (Silver layer, typed)
  - `mart_complete_shareholder_breakdown` (Gold)
  - `mart_true_float` & `mart_true_retail` (Gold)
  - `mart_broker_event_study` (Gold)
           │
           ▼
[ Research & Consuming Applications ]
  - Jupyter Research Notebooks (`notebooks/`)
  - Backtesting & Alpha Evaluation Engine (`src/bandarmology/analysis/`)
  - Standalone Streamlit Dashboards (via BigQuery ADC client)
```

---

## 2. Repository Layout

```
research-gcp/
├── dags/                                 # Airflow DAGs (synced to Composer GCS bucket)
│   ├── config/
│   │   └── universe.txt                  # Monitored stock universe
│   ├── dag_ksei_monthly_balancepos.py    # Monthly KSEI balance position ELT & DQ audit
│   ├── dag_ksei_monthly_shareholders_1pct.py # Monthly 1% & 5% shareholder disclosures DAG
│   └── dag_daily_broker_summary.py       # End-of-day broker summary ETL
│
├── bigquery/                             # BigQuery SQL definitions
│   ├── schemas/raw/                      # DDL for partitioned & clustered tables
│   │   ├── broker_summary.sql
│   │   ├── stock_daily_summary.sql
│   │   ├── ksei_balancepos_raw.sql
│   │   └── ksei_shareholder_disclosures_raw.sql
│   ├── silver/                           # Silver Layer transformations & DDLs
│   │   ├── 01_create_silver_tables.sql
│   │   ├── 02_transform_ksei_composition.sql
│   │   ├── 03_transform_shareholder_ownership.sql
│   │   └── 04_verify_silver_layer.sql
│   └── marts/                            # SQL Views and analytical tables
│       ├── dq_ksei_composition_audit.sql
│       ├── mart_complete_shareholder_breakdown.sql
│       ├── mart_true_float.sql
│       ├── mart_true_retail.sql
│       └── mart_broker_event_study.sql
│
├── src/                                  # Installable Python package (`bandarmology`)
│   └── bandarmology/
│       ├── clients/                      # GCP BigQuery & GCS wrapper clients
│       │   ├── bigquery.py
│       │   └── gcs.py
│       ├── extractors/                   # Scrapers and ingestion routines
│       │   ├── scrape_ksei.py
│       │   └── pdf_shareholders.py
│       ├── analysis/                     # Quantitative alpha & backtest engines
│       │   ├── broker_event_study.py
│       │   └── backtest_broker_strategy.py
│       └── pipeline.py                   # Unified market data fetcher (BigQuery + yfinance)
│
├── notebooks/                            # Exploratory Jupyter notebooks
├── docs/                                 # Replication papers and documentation
├── .github/workflows/                    # CI/CD: Automated DAG testing & GCS sync
├── requirements.txt                      # Local development dependencies
├── requirements-composer.txt             # PyPI packages for Cloud Composer environment
└── pyproject.toml                        # Package installer config
```

---

## 3. Initial Setup

### A. Local Setup
1. Clone the repository and install in editable mode:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Or on Windows: .venv\Scripts\activate
   pip install -e .
   ```
2. Configure credentials:
   ```bash
   cp .env.example .env
   # Set GCP_PROJECT_ID, GCS_BUCKET_NAME, and GOOGLE_APPLICATION_CREDENTIALS in .env
   ```
3. Authenticate with GCP:
   ```bash
   gcloud auth application-default login
   ```

### B. BigQuery Tables Provisioning
Execute the DDL scripts in `bigquery/schemas/raw/` in the Google Cloud Console or using `bq query`:
```bash
bq query --use_legacy_sql=false < bigquery/schemas/raw/broker_summary.sql
bq query --use_legacy_sql=false < bigquery/schemas/raw/stock_daily_summary.sql
bq query --use_legacy_sql=false < bigquery/schemas/raw/done_detail.sql
bq query --use_legacy_sql=false < bigquery/marts/mart_broker_event_study.sql
```

### C. Cloud Composer Deployment
1. Install extra PyPI packages listed in `requirements-composer.txt` into your Cloud Composer environment via Google Cloud Console or gcloud:
   ```bash
   gcloud composer environments update <ENVIRONMENT_NAME> \
       --location <LOCATION> \
       --update-pypi-packages-from-file requirements-composer.txt
   ```
2. Build and upload the `bandarmology` package wheel to the Composer `plugins/` or install it as a package dependency:
   ```bash
   pip install build
   python -m build --wheel
   # Upload dist/*.whl to Composer environment plugins/ or PyPI dependencies
   ```
3. Sync DAGs to Composer's DAGs bucket:
   ```bash
   gsutil -m rsync -r -d dags/ gs://<COMPOSER_DAGS_BUCKET>/dags/
   ```
   *(This is automated on git push via `.github/workflows/deploy_dags.yml`)*
