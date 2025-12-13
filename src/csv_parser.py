"""CSV parser for AIAAIC Google Sheets data."""

import csv
import re
import subprocess
from io import StringIO
from typing import Iterator

import httpx

from .models import AIAAICIncident, ExternalHarms, InternalImpacts

CSV_URL = "https://docs.google.com/spreadsheets/d/1Bn55B4xz21-_Rgdr8BBb2lt0n_4rzLGxFADMlVW0PYI/export?format=csv&gid=888071280"

# Column indices (0-based) based on the CSV structure
COL_AIAAIC_ID = 0
COL_HEADLINE = 1
COL_OCCURRED = 2
COL_COUNTRIES = 3
COL_SECTORS = 4
COL_DEPLOYERS = 5
COL_DEVELOPERS = 6
COL_SYSTEM_NAMES = 7
COL_TECHNOLOGIES = 8
COL_PURPOSES = 9
COL_NEWS_TRIGGERS = 10
COL_ISSUES = 11
# External harms (sub-columns)
COL_HARMS_INDIVIDUAL = 12
COL_HARMS_SOCIETAL = 13
COL_HARMS_ENVIRONMENTAL = 14
# Internal impacts (sub-columns)
COL_IMPACTS_STRATEGIC = 15
COL_IMPACTS_OPERATIONAL = 16
COL_IMPACTS_FINANCIAL = 17
COL_IMPACTS_LEGAL = 18
# Summary/links
COL_DETAIL_URL = 19


def split_field(value: str) -> list[str]:
    """Split a semicolon-separated field into a list of values."""
    if not value or not value.strip():
        return []
    # Split on "; " or ";" and strip whitespace
    parts = re.split(r";\s*", value.strip())
    return [p.strip() for p in parts if p.strip()]


def download_csv(url: str = CSV_URL) -> str:
    """Download CSV content from Google Sheets.

    Uses curl subprocess as httpx has issues with Google's cross-origin redirects.
    """
    result = subprocess.run(
        [
            "curl", "-sL",
            "-H", "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
            url
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    if result.returncode != 0:
        raise RuntimeError(f"curl failed with code {result.returncode}: {result.stderr}")
    return result.stdout


def parse_csv_row(row: list[str]) -> AIAAICIncident | None:
    """Parse a single CSV row into an AIAAICIncident object.

    Returns None if the row should be skipped (empty AIAAIC ID).
    """
    # Skip rows with empty AIAAIC ID
    aiaaic_id = row[COL_AIAAIC_ID].strip() if len(row) > COL_AIAAIC_ID else ""
    if not aiaaic_id:
        return None

    # Extract detail page URL
    detail_url = row[COL_DETAIL_URL].strip() if len(row) > COL_DETAIL_URL else ""
    if detail_url and "aiaaic.org" not in detail_url:
        detail_url = None  # Invalid URL

    return AIAAICIncident(
        aiaaic_id=aiaaic_id,
        headline=row[COL_HEADLINE].strip() if len(row) > COL_HEADLINE else "",
        occurred=row[COL_OCCURRED].strip() if len(row) > COL_OCCURRED else "",
        countries=split_field(row[COL_COUNTRIES]) if len(row) > COL_COUNTRIES else [],
        sectors=split_field(row[COL_SECTORS]) if len(row) > COL_SECTORS else [],
        deployers=split_field(row[COL_DEPLOYERS]) if len(row) > COL_DEPLOYERS else [],
        developers=split_field(row[COL_DEVELOPERS]) if len(row) > COL_DEVELOPERS else [],
        system_names=split_field(row[COL_SYSTEM_NAMES]) if len(row) > COL_SYSTEM_NAMES else [],
        technologies=split_field(row[COL_TECHNOLOGIES]) if len(row) > COL_TECHNOLOGIES else [],
        purposes=split_field(row[COL_PURPOSES]) if len(row) > COL_PURPOSES else [],
        news_triggers=split_field(row[COL_NEWS_TRIGGERS]) if len(row) > COL_NEWS_TRIGGERS else [],
        issues=split_field(row[COL_ISSUES]) if len(row) > COL_ISSUES else [],
        external_harms=ExternalHarms(
            individual=split_field(row[COL_HARMS_INDIVIDUAL]) if len(row) > COL_HARMS_INDIVIDUAL else [],
            societal=split_field(row[COL_HARMS_SOCIETAL]) if len(row) > COL_HARMS_SOCIETAL else [],
            environmental=split_field(row[COL_HARMS_ENVIRONMENTAL]) if len(row) > COL_HARMS_ENVIRONMENTAL else [],
        ),
        internal_impacts=InternalImpacts(
            strategic_reputational=split_field(row[COL_IMPACTS_STRATEGIC]) if len(row) > COL_IMPACTS_STRATEGIC else [],
            operational=split_field(row[COL_IMPACTS_OPERATIONAL]) if len(row) > COL_IMPACTS_OPERATIONAL else [],
            financial=split_field(row[COL_IMPACTS_FINANCIAL]) if len(row) > COL_IMPACTS_FINANCIAL else [],
            legal_regulatory=split_field(row[COL_IMPACTS_LEGAL]) if len(row) > COL_IMPACTS_LEGAL else [],
        ),
        detail_page_url=detail_url if detail_url else None,
        page_scraped=False,
    )


def parse_csv(csv_content: str) -> Iterator[AIAAICIncident]:
    """Parse CSV content and yield AIAAICIncident objects.

    Skips:
    - Row 1: Title row ("Incidents [ REPORT INCIDENT ]")
    - Row 2: Main header
    - Row 3: Sub-header for External harms/Internal impacts
    - Rows with empty AIAAIC ID
    """
    reader = csv.reader(StringIO(csv_content))
    rows = list(reader)

    # Skip first 3 rows (title, header, sub-header)
    # Data starts at row 4 (index 3)
    for row in rows[3:]:
        incident = parse_csv_row(row)
        if incident is not None:
            yield incident


def fetch_incidents(url: str = CSV_URL) -> list[AIAAICIncident]:
    """Download CSV and parse all incidents."""
    csv_content = download_csv(url)
    return list(parse_csv(csv_content))
