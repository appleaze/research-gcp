# Project Context & Handoff: IDX Bandarmology (GCP & Cloud Composer)

> **File Location**: `G:\Dean\Saham IDX\research-gcp\PROJECT_CONTEXT.md`  
> **Last Updated**: 2026-10-27  
> **Status**: Core Architecture, BigQuery Schemas, Airflow DAGs, and KSEI ELT Pipeline Built

---

## 1. Project Background & Objective

This repository (`research-gcp`) is the production cloud migration of the **IDX Bandarmology & Microstructure Research Platform** (originally in `Research-Bandarmology`).

### Core Objectives:
1. **Data Warehouse**: Migrate storage from PostgreSQL / Supabase into **Google BigQuery** using partitioned and clustered tables for low-cost, sub-second analytical queries.
2. **Orchestration**: Transition from ad-hoc local scripts into **Google Cloud Composer (Managed Apache Airflow)**.
3. **Data Quality (DQ)**: Implement automated mathematical reconciliation for Indonesian financial datasets (specifically KSEI shareholder composition and IndoPremier broker flows).
4. **App Isolation**: The Streamlit user interface is deliberately decoupled and not hosted in this repo (focused strictly on ELT pipelines, quant models, and data warehousing).

---

## 2. Source Data Assets & Key Discoveries

### A. KSEI Shareholder Composition (`BalanceposYYYYMMDD.txt`)
* **Raw Characteristics**: Pipe-delimited (`|`), header contains duplicate column names (`Total` appears twice — once for Local and once for Foreign). Dates formatted as `30-JAN-2026`.
* **Instrument Filtering**: The raw file has **3,634 total instruments** (Government Bonds, Corporate Bonds, Warrants, Rights, Mutual Funds, Equities). Filtering on `Type = 'EQUITY'` yields **exactly 1,002 listed equities** on the Indonesia Stock Exchange.
* **Mathematical Invariants (DQ Rules)**:
  1. `SUM(local_is ... local_ot) == local_total`
  2. `SUM(foreign_is ... foreign_ot) == foreign_total`
  3. `local_total + foreign_total == sec_num` (Total Listed Shares)

### B. Existing Supabase Database (`ResearchBDM` / `ljrltypgbhpjmporuhsq`)
The legacy database remains live and queryable via Supabase MCP:
* `broker_summary`: **186,558** rows (Historical broker net buy/sell values)
* `stock_daily_summary`: **15,480** rows (Header traded values, foreign net values)
* `ksei_shareholder_composition`: **3,006** rows (3 historical monthly snapshots: Jan 2026, Feb 2026, Mar 2026 at 1,002 rows each)
* `tracked_tickers`: **21** watchlisted tickers

---

## 3. Repository Architecture (`research-gcp`)

```text
research-gcp/
├── .env                                # Local credentials (GCP project, service account, Supabase)
├── .env.example                        # Template for environment variables
├── pyproject.toml                      # Installable package spec (pip install -e .)
├── requirements.txt                    # Local dependencies (BigQuery, GCS, yfinance, scipy)
├── requirements-composer.txt           # Extra PyPI packages for Cloud Composer environment
│
├── dags/                               # Cloud Composer Airflow DAGs (gs://<bucket>/dags)
│   ├── config/
│   │   └── universe.txt                # Monitored stock tickers (AADI, BBRI, BREN, etc.)
│   ├── dag_ksei_monthly_balancepos.py  # Monthly KSEI balance position ELT & DQ audit
│   ├── dag_ksei_monthly_shareholders_1pct.py # Monthly 1% & 5% shareholder disclosures DAG
│   └── dag_daily_broker_summary.py     # EOD broker summary ingestion & mart refresh
│
├── bigquery/                           # BigQuery SQL Schemas & Transformations
│   ├── schemas/raw/
│   │   ├── ksei_balancepos_raw.sql     # Raw pipe-delimited landing table (All STRING)
│   │   ├── ksei_shareholder_disclosures_raw.sql # Raw 1% & 5% PDF shareholder landing table
│   │   ├── broker_summary.sql          # Partition: trade_date | Cluster: symbol, broker
│   │   └── stock_daily_summary.sql     # Partition: trade_date | Cluster: symbol
│   ├── silver/                         # Silver Layer DDLs & ELT Transformations
│   │   ├── 01_create_silver_tables.sql # DDLs: ksei_shareholder_composition & ownership
│   │   ├── 02_transform_ksei_composition.sql # MERGE: Raw Balancepos -> Silver composition
│   │   ├── 03_transform_shareholder_ownership.sql # MERGE: Raw Disclosures -> Silver ownership
│   │   └── 04_verify_silver_layer.sql  # Audit & verification query
│   └── marts/
│       ├── dq_ksei_composition_audit.sql # SQL View with mathematical reconciliation checks
│       ├── mart_complete_shareholder_breakdown.sql # View replacing complete_breakdown.csv
│       ├── mart_true_float.sql           # View: True Float (sec_num minus all named holders)
│       ├── mart_true_retail.sql          # View: True Retail (local_id minus named individuals)
│       └── mart_broker_event_study.sql # Top 5 Accumulation/Distribution view
│
├── src/bandarmology/                   # Core Python Library
│   ├── clients/
│   │   ├── bigquery.py                 # BigQueryClient wrapper (queries, dataframe loader)
│   │   └── gcs.py                      # GCSClient wrapper (raw file staging)
│   ├── extractors/
│   │   ├── scrape_ksei.py              # KSEI download & ZIP extractor
│   │   └── pdf_shareholders.py         # Fast parser for monthly 1% & 5% disclosure PDFs
│   ├── analysis/
│   │   ├── broker_event_study.py       # Win-rate & t-test event study engine
│   │   └── backtest_broker_strategy.py # Strategy backtester
│   └── pipeline.py                     # Unified market data fetcher (BigQuery + yfinance)
│
├── scripts/
│   ├── run_ksei_pipeline.py            # Local runner for KSEI Balancepos ingestion & DQ check
│   └── run_shareholders_pipeline.py    # Local runner for 1% & 5% PDF disclosures & breakdown marts
│
├── notebooks/                          # Jupyter research & EDA
│   ├── eda.ipynb
│   └── eda_broker_summary.ipynb
│
├── docs/                               # Methodologies & Architecture
│   ├── GCP_ARCHITECTURE.md             # Full deployment & Airflow setup guide
│   └── broker_win_rate_replication.md  # Win-rate event study methodology
│
└── .github/workflows/
    ├── deploy_dags.yml                 # CI/CD: Syncs dags/ to Composer GCS bucket on push
    └── test_dags.yml                   # CI/CD: Tests Airflow DAG syntax & integrity
```

---

## 4. How to Run & Verify

### Local Pipeline Execution
```bash
cd "G:\Dean\Saham IDX\research-gcp"

# 1. Install dependencies
pip install -e .

# 2. Run the KSEI Pipeline (Provisions DDL -> Ingests Raw -> Transforms -> Runs DQ Check)
python scripts/run_ksei_pipeline.py --file "G:/Dean/Saham IDX/Research-Bandarmology/Data/BalanceposEfek20260130/Balancepos20260130.txt"

# 3. Run the 1% & 5% Monthly Shareholder Disclosures Pipeline
python scripts/run_shareholders_pipeline.py --pdf "G:/Dean/Saham IDX/Research-Bandarmology/Data/20260402_Semua Emiten Saham_Pengumuman Bursa_32057297_lamp1.pdf"
```

### Cloud Composer Deployment
1. Set the bucket secret in GitHub repository: `COMPOSER_DAGS_BUCKET` and `GCP_SA_KEY`.
2. Push commits to `main` — `.github/workflows/deploy_dags.yml` automatically syncs `dags/` to `gs://<composer-bucket>/dags/`.
3. In Airflow UI, trigger `monthly_ksei_balancepos_pipeline` or `daily_broker_summary_pipeline`.

---

## 5. Next Planned Roadmap Steps

1. **Migrate Supabase Data for Traded Tickers**: Backfill historical broker summary and daily summary data for key tracked tickers from Supabase into BigQuery.
2. **Create Airflow DAGs for BigQuery Mapping**: Automate and schedule the continuous mapping and transformation pipelines directly in BigQuery.
