from __future__ import annotations

import re
from pathlib import Path
from typing import List, Dict, Any, Tuple
import pypdf
import pdfplumber
import pandas as pd


# Regex pattern to match shareholder rows from the expanded 1% report:
# Example: 31-Mar-2026 AADI ADARO ANDALAN INDONESIA Tbk ADARO STRATEGIC INVESTMENTS CP L INDONESIA 3.200.142.830 0 3.200.142.830 41,10
EXPANDED_1PCT_PATTERN = re.compile(
    r"^\s*(\d{1,2}-[A-Za-z]{3}-\d{4})\s+"  # 1: Date
    r"([A-Z0-9]{4})\s+"                     # 2: Symbol (4 chars)
    r"(.+?)\s+"                             # 3: Issuer + Investor name
    r"([A-Z]{2})\s+"                        # 4: Investor Type (CP, ID, IS, PF, etc.)
    r"([LA])\s+"                            # 5: Local / Foreign (L or A)
    r"(.+?)\s+"                             # 6: Domicile / Nationality
    r"([\d\.]+)\s+"                         # 7: Holdings Scripless
    r"([\d\.]+)\s+"                         # 8: Holdings Scrip
    r"([\d\.]+)\s+"                         # 9: Total Holdings Shares
    r"([\d,]+)\s*$"                         # 10: Percentage
)


def detect_pdf_report_format(pdf_path: str | Path) -> str:
    """Detects whether the PDF is an expanded 1% report or a 5% SID custody report."""
    reader = pypdf.PdfReader(str(pdf_path))
    combined_header = ""
    for page in reader.pages[:3]:
        text = page.extract_text() or ""
        combined_header += text + "\n"

    if "SHARE_CODE" in combined_header or "HOLDINGS_SCRIPLESS" in combined_header:
        return "1PCT_EXPANDED"
    elif "BERDASARKAN SID" in combined_header:
        return "5PCT_SID"
    return "UNKNOWN"


def extract_expanded_1pct_records(pdf_path: Path) -> List[Dict[str, Any]]:
    """Fast stream extraction for the 1% & 5% expanded disclosure format."""
    reader = pypdf.PdfReader(str(pdf_path))
    records: List[Dict[str, Any]] = []

    for page_idx, page in enumerate(reader.pages, start=1):
        text = page.extract_text()
        if not text:
            continue

        for line in text.split("\n"):
            line = line.strip()
            match = EXPANDED_1PCT_PATTERN.match(line)
            if not match:
                continue

            records.append({
                "date_raw": match.group(1),
                "symbol_raw": match.group(2),
                "issuer_and_investor_raw": match.group(3).strip(),
                "investor_type_raw": match.group(4),
                "local_foreign_raw": match.group(5),
                "domicile_raw": match.group(6).strip(),
                "holdings_scripless_raw": match.group(7),
                "holdings_scrip_raw": match.group(8),
                "total_shares_raw": match.group(9),
                "percentage_raw": match.group(10),
                "_source_file": pdf_path.name,
                "_page_number": page_idx,
            })

    return records


def extract_5pct_sid_records(pdf_path: Path) -> List[Dict[str, Any]]:
    """Table extraction for the baseline 5% SID custody report format (e.g. Jan 2026)."""
    records: List[Dict[str, Any]] = []

    # Extract date from filename or first page
    # Example: 20260130_... -> 30-Jan-2026
    date_match = re.search(r"(\d{4})(\d{2})(\d{2})", pdf_path.name)
    if date_match:
        dt = pd.to_datetime(f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}")
        fallback_date = dt.strftime("%d-%b-%Y")
    else:
        fallback_date = "30-Jan-2026"

    with pdfplumber.open(str(pdf_path)) as pdf:
        for page_idx, page in enumerate(pdf.pages[1:], start=2):
            tables = page.extract_tables()
            for t in tables:
                for row in t:
                    # Column layout:
                    # [No, Kode Efek, Nama Emiten, Custodian, Investor Name, Sub Account, Address, Address2, Nat, Domicile, L/A, SharesBefore, CombBefore, PctBefore, SharesCurrent, CombCurrent, PctCurrent, Change]
                    if len(row) >= 17 and row[1] and row[1] != "Kode Efek" and row[4] and row[15]:
                        symbol = str(row[1]).strip()
                        investor = str(row[4]).strip()
                        local_foreign = str(row[10]).strip() if row[10] else "L"
                        total_shares = str(row[15]).strip()
                        pct = str(row[16]).strip()

                        records.append({
                            "date_raw": fallback_date,
                            "symbol_raw": symbol,
                            "issuer_and_investor_raw": investor,
                            "investor_type_raw": "CP" if any(k in investor.upper() for k in ["PT", "TBK", "LTD", "CORP"]) else "ID",
                            "local_foreign_raw": local_foreign,
                            "domicile_raw": "INDONESIA",
                            "holdings_scripless_raw": total_shares,
                            "holdings_scrip_raw": "0",
                            "total_shares_raw": total_shares,
                            "percentage_raw": pct.replace(".", ","),
                            "_source_file": pdf_path.name,
                            "_page_number": page_idx,
                        })

    return records


def parse_pdf_to_dataframe(pdf_path: str | Path) -> pd.DataFrame:
    """
    Auto-detects PDF report format and returns a clean DataFrame ready for Bronze ingestion.
    """
    path = Path(pdf_path)
    if not path.exists():
        raise FileNotFoundError(f"PDF file not found: {path}")

    fmt = detect_pdf_report_format(path)
    print(f"[{path.name}] Detected Report Format: {fmt}")

    if fmt == "1PCT_EXPANDED":
        records = extract_expanded_1pct_records(path)
    elif fmt == "5PCT_SID":
        records = extract_5pct_sid_records(path)
    else:
        # Fallback to expanded regex
        records = extract_expanded_1pct_records(path)
        if not records:
            records = extract_5pct_sid_records(path)

    df = pd.DataFrame(records)
    if not df.empty:
        df["_ingested_at"] = pd.Timestamp.now(tz="UTC")
    return df
