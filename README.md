# IDX Bandarmology Research (GCP & Cloud Composer Edition)

Production ELT data pipelines, market microstructure analysis, and Wyckoff regime models for the Indonesia Stock Exchange (IDX), orchestrated by **Google Cloud Composer (Managed Apache Airflow)** and stored in **Google BigQuery**.

## Quick Links
- **Full Project Context & Handoff**: [PROJECT_CONTEXT.md](PROJECT_CONTEXT.md)
- **Architecture & Setup Guide**: [docs/GCP_ARCHITECTURE.md](docs/GCP_ARCHITECTURE.md)
- **Replication Guide for Broker Win Rates**: [docs/broker_win_rate_replication.md](docs/broker_win_rate_replication.md)
- **BigQuery Schemas & DDLs**: [bigquery/schemas/raw/](bigquery/schemas/raw/)
- **Airflow DAGs**: [dags/](dags/)
- **Local Runner Scripts**: [scripts/run_ksei_pipeline.py](scripts/run_ksei_pipeline.py)

## Quick Start
```bash
# 1. Install local package
pip install -e .

# 2. Configure .env with your GCP project ID
cp .env.example .env

# 3. Run the KSEI Pipeline (Ingest, Transform & DQ check)
python scripts/run_ksei_pipeline.py --file "G:/Dean/Saham IDX/Research-Bandarmology/Data/BalanceposEfek20260130/Balancepos20260130.txt"
```
