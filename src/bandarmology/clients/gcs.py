"""
Google Cloud Storage (GCS) Client wrapper for raw data staging.
"""

from __future__ import annotations

import os
from typing import Optional
from google.cloud import storage
from google.oauth2 import service_account


class GCSClient:
    """Wrapper around google-cloud-storage client."""

    def __init__(
        self,
        bucket_name: Optional[str] = None,
        project_id: Optional[str] = None,
        credentials_path: Optional[str] = None,
    ):
        self.project_id = project_id or os.getenv("GCP_PROJECT_ID")
        self.bucket_name = bucket_name or os.getenv("GCS_BUCKET_NAME")

        credentials = None
        cred_path = credentials_path or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if cred_path and os.path.exists(cred_path):
            credentials = service_account.Credentials.from_service_account_file(cred_path)

        self.client = storage.Client(
            project=self.project_id,
            credentials=credentials
        )
        self.bucket = self.client.bucket(self.bucket_name) if self.bucket_name else None

    def upload_file(self, local_path: str, destination_blob_name: str, bucket_name: Optional[str] = None) -> str:
        """Uploads a local file to GCS and returns gs:// URI."""
        bucket = self.client.bucket(bucket_name) if bucket_name else self.bucket
        if not bucket:
            raise ValueError("Bucket name must be provided.")
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(local_path)
        return f"gs://{bucket.name}/{destination_blob_name}"

    def upload_string(self, content: str, destination_blob_name: str, content_type: str = "text/plain", bucket_name: Optional[str] = None) -> str:
        """Uploads string content directly to GCS."""
        bucket = self.client.bucket(bucket_name) if bucket_name else self.bucket
        if not bucket:
            raise ValueError("Bucket name must be provided.")
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(content, content_type=content_type)
        return f"gs://{bucket.name}/{destination_blob_name}"
