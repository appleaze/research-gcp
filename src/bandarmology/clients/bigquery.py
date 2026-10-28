"""
BigQuery Client wrapper for Bandarmology pipelines.
"""

from __future__ import annotations

import os
from typing import Optional, List, Dict, Any
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account


class BigQueryClient:
    """Wrapper around google-cloud-bigquery client."""

    def __init__(
        self,
        project_id: Optional[str] = None,
        dataset_id: Optional[str] = None,
        credentials_path: Optional[str] = None,
    ):
        self.project_id = project_id or os.getenv("GCP_PROJECT_ID")
        self.dataset_id = dataset_id or os.getenv("BQ_DATASET", "idx_bandarmology")
        
        credentials = None
        cred_path = credentials_path or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if cred_path and os.path.exists(cred_path):
            credentials = service_account.Credentials.from_service_account_file(cred_path)

        self.client = bigquery.Client(
            project=self.project_id,
            credentials=credentials
        )

    def query_to_dataframe(self, query: str, params: Optional[List[bigquery.ScalarQueryParameter]] = None) -> pd.DataFrame:
        """Executes a SQL query and returns results as a pandas DataFrame."""
        job_config = bigquery.QueryJobConfig(query_parameters=params) if params else None
        query_job = self.client.query(query, job_config=job_config)
        return query_job.to_dataframe()

    def load_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        dataset_id: Optional[str] = None,
        write_disposition: str = "WRITE_APPEND",
    ) -> bigquery.LoadJob:
        """Loads a pandas DataFrame into a BigQuery table.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame to upload.
        table_name : str
            Target table name.
        dataset_id : Optional[str]
            Dataset name (defaults to self.dataset_id).
        write_disposition : str
            "WRITE_APPEND", "WRITE_TRUNCATE", or "WRITE_EMPTY".
        """
        ds = dataset_id or self.dataset_id
        table_ref = f"{self.project_id}.{ds}.{table_name}"
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
        )
        job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Wait for the job to complete
        return job

    def execute_sql(self, query: str) -> None:
        """Executes a DDL or DML SQL statement."""
        query_job = self.client.query(query)
        query_job.result()
