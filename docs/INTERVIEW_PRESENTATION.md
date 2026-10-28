# Interview Presentation Deck & Spoken Script: IDX Financial Data Platform (GCP)

**Target Presentation Length:** 10–12 minutes (+ Q&A)  
**Role Context:** Data Engineer / Analytics Engineer / Cloud Data Architect  
**Repository:** [research-gcp](file:///g:/Dean/Saham%20IDX/research-gcp)

---

## Slide 1: Title & Executive Summary

### Slide Visual
- **Title:** Scaling Indonesian Equities Ownership Data with Google BigQuery & Cloud Composer
- **Subtitle:** Automated Medallion ELT, Mathematical DQ Invariants, and Float Disclosures
- **Presenter:** [Your Name]
- **Core Stack:** Google Cloud Storage | Google BigQuery | Google Cloud Composer (Apache Airflow) | Python

### Spoken Script (Say this)
> "Good morning/afternoon everyone. Today, I am presenting my project: a cloud-native financial data platform engineered to ingest, audit, and model equity ownership and market flow on the Indonesia Stock Exchange (IDX).
>
> In Indonesian capital markets, understanding institutional versus retail participation—and calculating real market liquidity—requires processing complex regulatory disclosures from KSEI (the Central Securities Depository) and the exchange.
>
> In this project, I built an automated Medallion data warehouse in Google BigQuery, orchestrated by Cloud Composer, with automated mathematical data quality invariants that guarantee zero corrupt data reaches downstream analytics."

---

## Slide 2: Problem Statement & Upstream Data Challenges

### Slide Visual
- **The Core Problem:**
  - Previous local setup was ad-hoc and unscalable.
  - Depository data feeds (KSEI) are notoriously dirty, unstandardized, and lack relational integrity.
- **The KSEI Feed Reality:**
  - Monthly pipe-delimited file (`BalanceposYYYYMMDD.txt`): Contains **duplicate column headers** (`Total` appears twice—once for Local and once for Foreign).
  - Mixed instrument types: **3,634 total instruments** (government bonds, corporate bonds, mutual funds, warrants).
  - Target: Filter and isolate **exactly 1,002 listed equities**.
  - Non-standard dates: formatted as strings (e.g. `30-JAN-2026`).

### Spoken Script (Say this)
> "Financial data in emerging markets like Indonesia is notoriously difficult to parse cleanly.
>
> Our primary data source is the monthly KSEI balance position file. When you open this raw pipe-delimited file, it presents several engineering headaches:
> First, it has duplicate column headers—the word 'Total' appears twice, once for local investors and once for foreign. Standard CSV loaders immediately crash on duplicate headers.
> Second, it contains 3,634 mixed financial instruments, of which only 1,002 are actually listed equities.
> Third, dates and numbers are unformatted strings.
>
> My objective was to build an enterprise-grade ELT pipeline that ingests this raw mess reliably, normalizes it, and guarantees mathematical accuracy."

---

## Slide 3: End-to-End System Architecture

### Slide Visual
```
[ Upstream Data Sources ]
  - KSEI Balancepos (Monthly pipe-delimited TXT)
  - IDX Disclosures (Monthly 1% & 5% shareholder PDFs)
  - EOD Broker Summary
         │
         ▼
[ Google Cloud Storage (GCS Landing) ]
  gs://<bucket>/raw/ksei/<YYYY-MM-DD>/...
         │
         ▼  (GCSToBigQueryOperator)
[ BigQuery Medallion Architecture ]
  ┌────────────────────────────────────────────────────────┐
  │ 1. Bronze (Raw): Untyped STRING landing tables         │
  │    (ksei_balancepos_raw, ksei_disclosures_raw)         │
  ├────────────────────────────────────────────────────────┤
  │ 2. Silver: Typed, normalized, deduplicated tables      │
  │    (ksei_shareholder_composition, shareholder_ownership)│
  │    ──▶ Automated Mathematical Invariant Audit Gate     │
  ├────────────────────────────────────────────────────────┤
  │ 3. Gold (Marts): Sub-second analytical views           │
  │    (mart_complete_shareholder_breakdown,               │
  │     mart_true_float, mart_true_retail)                 │
  └────────────────────────────────────────────────────────┘
         │
         ▲
[ Orchestration: Cloud Composer (Apache Airflow) ]
  - dag_ksei_monthly_balancepos.py
  - dag_ksei_monthly_shareholders_1pct.py
```

### Spoken Script (Say this)
> "Here is our architecture, following a strict Medallion architecture:
>
> 1. **Ingestion & Bronze Layer:** Python extractors fetch depository files and PDF disclosures, staging them in date-partitioned Google Cloud Storage buckets. Airflow loads these into BigQuery raw tables where every column is initially typed as `STRING`. This guarantees that unexpected formatting anomalies never fail the landing step.
>
> 2. **Silver Layer:** Using BigQuery SQL MERGE statements, we cast strings into strict types, parse dates into proper ISO DATE formats, and filter for active equities.
>
> 3. **Gold Layer:** We generate high-performance analytical views that calculate True Float and investor dominance for our downstream consumers."

---

## Slide 4: Data Quality (DQ) Invariants & Mathematical Validation

### Slide Visual
- **Automated DQ View (`dq_ksei_composition_audit.sql`):**
  Every batch must satisfy three financial invariants:
  1. $$\sum \text{local\_investors (IS + CP + PF + IB + ID + MF + SC + FD + OT)} = \text{local\_total}$$
  2. $$\sum \text{foreign\_investors (IS + CP + PF + IB + ID + MF + SC + FD + OT)} = \text{foreign\_total}$$
  3. $$\text{local\_total} + \text{foreign\_total} = \text{sec\_num} \text{ (Total Listed Shares)}$$
- **The Audit Gate:**
  - Airflow runs the DQ query immediately after the Silver layer MERGE.
  - If any row produces `is_valid = FALSE`, the task fails and halts downstream mart updates.

### Spoken Script (Say this)
> "A core highlight of this system is our mathematical Data Quality audit.
>
> In accounting and securities data, numbers must reconcile down to the single share. In our Silver layer, we implemented an automated mathematical reconciliation view:
> First: Does the sum of all local investor breakdown categories—Insurance, Corporate, Pension Fund, Financial Institutions, Individuals, Mutual Funds, Securities, Foundations, and Others—exactly match the local total?
> Second: Does the sum of foreign investor categories match the foreign total?
> Third: Does Local Total plus Foreign Total equal the exact total listed shares (`sec_num`)?
>
> If any company's filing fails these equations by even 1 share, the Airflow audit task raises an exception and halts the pipeline. Zero corrupted data is ever allowed to propagate."

---

## Slide 5: The Gold Layer Innovation: True Float & True Retail

### Slide Visual
- **The Market Problem:**
  - Standard market data classifies large founder shares registered under individual local accounts (`local_id`) as 'retail'.
  - Reported 'Free Float' often wildly overstates the liquid tradable shares.
- **The Solution:**
  - Extracted 1% and 5% monthly shareholder disclosures from exchange PDFs (`pdf_shareholders.py`).
  - Merged depository composition with named disclosures in BigQuery Gold marts:
    $$\text{True Float} = \text{sec\_num} - \sum \text{Named Strategic/Insider Holdings}$$
    $$\text{True Retail} = \text{local\_individual} - \text{Named Retail Insiders}$$
- **Result:** Provides true institutional vs. liquid retail ownership dominance across all 1,002 stocks.

### Spoken Script (Say this)
> "In the Gold layer, we solved a major problem in Indonesian equity analysis: the calculation of 'True Float'.
>
> In traditional market metrics, major company founders and board members who hold stock in personal accounts are lumped together under 'local individual' accounts. This leads people to think retail investors control a stock, when in reality, the shares are locked up by insiders.
>
> We parsed monthly 1% and 5% PDF shareholder announcements and merged them with our KSEI Silver tables in BigQuery (`mart_true_float` and `mart_true_retail`).
> By subtracting named strategic holders from the individual aggregate, we expose the *actual* liquid retail float. This is a massive competitive advantage for anyone analyzing supply and demand on IDX."

---

## Slide 6: BigQuery Optimization & Cloud Composer Orchestration

### Slide Visual
- **Storage & Compute Optimization:**
  - Partitioning by `report_date` (MONTH) and Clustering by `symbol`.
  - Analytical queries scan only the specific partitions needed, reducing query scan costs by ~90% and returning results in under 400ms.
- **Production Airflow DAGs:**
  - `dag_ksei_monthly_balancepos.py`: Manages the monthly Balancepos GCS -> Bronze -> Silver -> DQ Audit pipeline.
  - `dag_ksei_monthly_shareholders_1pct.py`: Extracts PDF shareholder disclosures and updates the True Float mart.
- **CI/CD Automation:**
  - GitHub Actions automatically validates DAG syntax and deploys code to Composer GCS buckets on push.

### Spoken Script (Say this)
> "For performance and reliability, all BigQuery tables are partitioned by `report_date` and clustered by `symbol`. This eliminates full-table scans. Analysts querying ownership history over a specific quarter hit only the targeted cluster blocks, keeping query costs minimal and execution times under 400 milliseconds.
>
> Orchestration is fully handled by Cloud Composer using dedicated monthly DAGs. Our deployment workflow is backed by GitHub Actions, which tests DAG integrity on pull requests and continuously syncs code to Composer's storage bucket on push."

---

## Slide 7: What Has Been Done vs. Next Roadmap

### Slide Visual
- **What Has Been Built & Delivered (Done):**
  - ✅ End-to-end Medallion pipeline (Bronze → Silver → Gold) for KSEI investor composition.
  - ✅ Automated 1% and 5% PDF shareholder disclosure extraction.
  - ✅ BigQuery Gold marts for True Float and True Retail.
  - ✅ Automated mathematical DQ reconciliation gates.
  - ✅ Cloud Composer Airflow DAGs and GitHub Actions CI/CD.
- **Next Roadmap (What Will Be Done):**
  - 🔄 **Supabase Migration:** Backfill historical data from legacy Supabase into BigQuery for key tracked tickers.
  - 🔄 **BigQuery Mapping DAGs:** Create scheduled Airflow DAGs to continuously map and update broker summary datasets in BigQuery.

### Spoken Script (Say this)
> "To summarize our current status:
>
> **What has been accomplished:**
> We have successfully mapped the complete Bronze to Gold pipeline for KSEI investor types and 1% / 5% disclosures, enforced mathematical data quality invariants, and automated the monthly workflows in Cloud Composer.
>
> **What is next on the immediate roadmap:**
> First, migrating and backfilling our historical broker summary dataset from our legacy Supabase database into BigQuery for our core universe of tracked tickers.
> Second, deploying the remaining Airflow DAGs to orchestrate the continuous mapping and transformation of this broker flow data in BigQuery.
>
> Thank you, and I look forward to your questions."

---

# Anticipated Technical Q&A Defense

### Q1: "Why use an ELT approach in BigQuery instead of transforming data in Python inside Airflow?"
> **Answer:** "Airflow is designed to be an orchestrator, not a heavy distributed compute engine. Processing and transforming millions of financial records inside Airflow worker containers consumes excessive memory and risks out-of-memory crashes. By adopting ELT, Airflow simply coordinates SQL `MERGE` and transformation queries inside BigQuery. BigQuery's massively parallel compute engine executes these operations in seconds at very low cost."

### Q2: "How do you handle duplicate headers in the raw KSEI pipe-delimited file?"
> **Answer:** "In standard CSV parsers, duplicate column names (like 'Total' appearing for both local and foreign) throw errors. In our ingestion stage, our extractor intercepts the header line during staging, disambiguates them into `local_total` and `foreign_total`, and loads them cleanly into the BigQuery Bronze layer."

### Q3: "What happens if a monthly KSEI file fails the mathematical invariant check?"
> **Answer:** "Our Airflow DAG executes the `dq_ksei_composition_audit` task immediately after loading the Silver layer. If any equity fails the invariant `(local_total + foreign_total != sec_num)`, the task raises an Airflow exception. This halts the DAG before the Gold analytical marts are generated, preventing bad numbers from reaching analytical consumers."
