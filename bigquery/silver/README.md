# BigQuery Silver Layer Transformations

This directory contains the DDLs and idempotent SQL transformation scripts for building the **Silver Layer (`idx_analytics`)** from raw Bronze tables.

---

## Files & Execution Order

| Step | File | Description |
| :--- | :--- | :--- |
| **01** | [`01_create_silver_tables.sql`](./01_create_silver_tables.sql) | DDLs for `ksei_shareholder_composition` and `ksei_shareholder_ownership` (partitioned & clustered). |
| **02** | [`02_transform_ksei_composition.sql`](./02_transform_ksei_composition.sql) | `MERGE` query transforming raw KSEI balance into typed, deduplicated 1,002 equities. |
| **03** | [`03_transform_shareholder_ownership.sql`](./03_transform_shareholder_ownership.sql) | `MERGE` query parsing European numbers, categorizing investors, and tagging `>5%` vs `>1%`. |
| **04** | [`04_verify_silver_layer.sql`](./04_verify_silver_layer.sql) | Audit query checking record counts, active tickers, and threshold distribution. |

---

## How to Run

You can run each query in sequence directly in the **Google BigQuery Console** under project dataset `idx_analytics`.
