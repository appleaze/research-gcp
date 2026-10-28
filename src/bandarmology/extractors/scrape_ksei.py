"""
KSEI Balancepos Extractor & GCS Stager.
Downloads monthly/daily BalanceposEfek ZIP from KSEI, extracts pipe-delimited
text file in-memory, and uploads to GCS landing zone for BigQuery ingestion.
"""

from __future__ import annotations

import argparse
import io
import os
import zipfile
from datetime import datetime, timedelta
from typing import Optional, Tuple

import requests
from google.cloud import storage

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

BASE_URLS = [
    "https://web.ksei.co.id/Download/BalanceposEfek{date_str}.zip",
    "https://www.ksei.co.id/Download/BalanceposEfek{date_str}.zip",
]


def fetch_ksei_zip(target_date: datetime, max_lookback_days: int = 5) -> Tuple[bytes, str]:
    """
    Tries fetching BalanceposEfek ZIP from KSEI, stepping back on weekends/holidays.
    Returns (zip_bytes, found_date_str).
    """
    session = requests.Session()
    session.headers.update(HEADERS)

    curr_date = target_date
    for _ in range(max_lookback_days):
        date_str = curr_date.strftime("%Y%m%d")
        for base_tmpl in BASE_URLS:
            url = base_tmpl.format(date_str=date_str)
            print(f"Checking KSEI download URL: {url}")
            try:
                resp = session.get(url, timeout=30)
                if resp.status_code == 200 and len(resp.content) > 1024:
                    print(f"Found KSEI data for date: {date_str} ({len(resp.content):,} bytes)")
                    return resp.content, date_str
            except requests.RequestException as e:
                print(f"Request error for {url}: {e}")

        # Step back 1 day (e.g. if today is Sunday, Saturday, or holiday)
        curr_date -= timedelta(days=1)

    raise FileNotFoundError(
        f"Could not find KSEI BalanceposEfek ZIP within {max_lookback_days} days of {target_date.strftime('%Y-%m-%d')}"
    )


def extract_balancepos_txt(zip_bytes: bytes) -> Tuple[str, bytes]:
    """
    Extracts the first matching Balancepos*.txt file from in-memory ZIP bytes.
    Returns (filename, file_bytes).
    """
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        for fname in z.namelist():
            if "balancepos" in fname.lower() and fname.endswith(".txt"):
                txt_bytes = z.read(fname)
                line_count = txt_bytes.count(b"\n")
                print(f"Extracted {fname} from ZIP ({len(txt_bytes):,} bytes, ~{line_count:,} lines)")
                return fname, txt_bytes

    raise ValueError("ZIP archive contains no Balancepos*.txt file.")


def fetch_and_stage_ksei_balancepos(
    target_date: datetime,
    gcs_bucket_name: str,
    max_lookback_days: int = 5,
    local_output_dir: Optional[str] = None,
) -> str:
    """
    End-to-end ingestion:
    1. Downloads latest BalanceposEfek ZIP from KSEI.
    2. Unzips in-memory to get pipe-delimited Balancepos text file.
    3. Uploads raw text file to GCS: gs://{gcs_bucket_name}/ksei/balancepos/{date_str}/{filename}
    4. Optionally saves a copy locally for debugging.
    """
    zip_bytes, found_date_str = fetch_ksei_zip(target_date, max_lookback_days=max_lookback_days)
    filename, txt_bytes = extract_balancepos_txt(zip_bytes)

    # Optional local save
    if local_output_dir:
        os.makedirs(local_output_dir, exist_ok=True)
        local_path = os.path.join(local_output_dir, filename)
        with open(local_path, "wb") as f:
            f.write(txt_bytes)
        print(f"Saved local copy to {local_path}")

    # Upload to GCS
    client = storage.Client()
    bucket = client.bucket(gcs_bucket_name)
    gcs_path = f"ksei/balancepos/{found_date_str}/{filename}"
    blob = bucket.blob(gcs_path)
    blob.upload_from_file(io.BytesIO(txt_bytes), content_type="text/plain")

    full_uri = f"gs://{gcs_bucket_name}/{gcs_path}"
    print(f"Successfully staged KSEI data to {full_uri}")
    return full_uri


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape and stage KSEI Balancepos data.")
    parser.add_argument(
        "--date",
        type=str,
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Target date (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default=os.getenv("GCS_BUCKET_NAME", "idx-ksei-raw-data"),
        help="Target GCS bucket name.",
    )
    parser.add_argument(
        "--local-dir",
        type=str,
        default=None,
        help="Optional local directory to save extracted .txt file.",
    )
    args = parser.parse_args()

    parsed_date = datetime.strptime(args.date, "%Y-%m-%d")
    fetch_and_stage_ksei_balancepos(
        target_date=parsed_date,
        gcs_bucket_name=args.bucket,
        local_output_dir=args.local_dir,
    )
