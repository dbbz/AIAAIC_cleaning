# CLAUDE.md - Project Instructions for Claude

## Project Overview

This is the **AIAAIC Database Scraper** - a Python tool to extract and structure data from the AIAAIC (AI, Algorithmic, and Automation Incidents and Controversies) database.

## Tech Stack

- **Python 3.14+** with type hints
- **uv** for dependency management (not pip/requirements.txt)
- **httpx** for async HTTP requests (detail page scraping)
- **curl** (subprocess) for CSV download (httpx has issues with Google's cross-origin redirects)
- **BeautifulSoup + lxml** for HTML parsing
- **Pydantic** for data validation
- **Rich** for terminal output
- **Streamlit** for data quality inspection dashboard
- **rapidfuzz** for fuzzy string matching (value consistency detection)

## Key Commands

```bash
# Run the scraper
uv run scrape.py

# Run with options
uv run scrape.py --sample 10 --verbose  # Test mode
uv run scrape.py --force                 # Re-scrape all
uv run scrape.py --retry-errors          # Retry failures
uv run scrape.py --export json           # Export to JSON
uv run scrape.py --no-url                # List incidents without URLs
uv run scrape.py --single AIAAIC123      # View single incident
uv run scrape.py --errors                # List failed scrapes
uv run scrape.py --incomplete            # Find missing page data
uv run scrape.py --rescrape-incomplete   # Rescrape incomplete
uv run scrape.py --rescrape-incomplete --min-desc-length 500  # Also rescrape short descriptions

# Run the data quality inspector (Streamlit dashboard)
uv run streamlit run app.py
```

## Project Structure

```
├── app.py           # Streamlit data quality inspector (imports from src/)
├── scrape.py        # CLI entry point for scraper
├── src/
│   ├── models.py        # Pydantic schemas (AIAAICIncident, etc.)
│   ├── csv_parser.py    # Downloads and parses CSV from Google Sheets
│   ├── page_scraper.py  # Scrapes detail pages (text-pattern based)
│   ├── scraper.py       # Main orchestration (async)
│   ├── console.py       # Rich terminal output helpers
│   └── utils.py         # File I/O (JSONL read/write) - SHARED WITH app.py
└── data/
    ├── aiaaic_incidents.jsonl  # Scraped incidents
    └── errors.jsonl            # Scraping errors
```

## Data Flow

1. CSV downloaded from Google Sheets (via curl) → parsed into partial `AIAAICIncident` objects
2. For each incident with a detail URL → scrape the page (via httpx)
3. Merge CSV data + page data → validate with Pydantic
4. Append to `data/aiaaic_incidents.jsonl`

## Important Design Decisions

### CSV Download (Google Sheets)

The CSV is downloaded using **curl subprocess** instead of httpx. This is because Google Sheets uses cross-origin redirects (docs.google.com → googleusercontent.com) that strip headers in httpx, causing 400 errors. curl handles this correctly.

See `src/csv_parser.py` `download_csv()` function.

### Page Scraping (CRITICAL)

The AIAAIC pages use **Google Sites** with auto-generated CSS class names that can change. **Do NOT rely on CSS classes.**

Instead, use:
- **Text patterns** via regex: `Occurred:`, `Page published:`, `AIAAIC Repository ID:`
- **URL patterns** for links: external URLs = source links, `/aiaaic-repository/ai-algorithmic-and-automation-incidents/` = related
- **Section-based extraction**: Content is in `<section>` elements, not `role="main"`
- **Plain text URLs**: Many pages list source URLs as underlined `<span>` elements, NOT as `<a>` tags. The scraper extracts both hyperlinks AND plain text URLs.

See `src/page_scraper.py` for the implementation.

### Output Format

**JSONL** (JSON Lines) - one record per line. This allows:
- Resumable scraping (check existing IDs)
- Incremental updates (append new lines)
- No file corruption if interrupted mid-write

### Concurrency

Uses `asyncio` + `httpx.AsyncClient` with semaphore-based concurrency limiting.
Default: 10 concurrent requests.

### Shared Utilities (IMPORTANT)

The `src/utils.py` module provides shared functions used by both the scraper and the Streamlit app:

- `load_incidents(path)` - Load incidents with Pydantic validation
- `load_errors(path)` - Load scraping errors with Pydantic validation
- `append_incident(path, incident)` - Append a validated incident
- `append_error(path, error)` - Append a validated error

**Do NOT reimplement data loading/saving logic.** Always import from `src.utils` to ensure consistency between the CLI scraper and the Streamlit dashboard.

The `src/models.py` module defines all Pydantic schemas:
- `AIAAICIncident` - Main incident record
- `ExternalHarms`, `InternalImpacts` - Nested impact structures
- `SourceLink`, `RelatedIncident` - Reference types
- `ScrapingError` - Error tracking

Field lists in `app.py` are derived from these models (e.g., `ExternalHarms.model_fields.keys()`) to stay in sync.

### Data Quality Inspector (Streamlit)

The `app.py` dashboard provides:

1. **Quality Overview** - Completeness metrics, field coverage charts
2. **Scraping Issues** - Failed scrapes, missing descriptions/dates
3. **Missing Data** - Field-by-field gap analysis
4. **Value Consistency** - Fuzzy matching to detect typos, case variations, similar values
5. **Impact Assessment** - External harms / internal impacts coverage
6. **Record Inspector** - Individual record view with missing fields highlighted

Run with: `uv run streamlit run app.py`

## Common Tasks

### Adding a new field to extract

1. Add field to `AIAAICIncident` in `src/models.py`
2. If from CSV: update column mapping in `src/csv_parser.py`
3. If from page: update extraction logic in `src/page_scraper.py`
4. Update `PageData` dataclass if needed
5. If it should appear in quality checks: update `CORE_FIELDS` or `LIST_FIELDS` in `app.py`

### Debugging page scraping

```python
# Quick debug script
import httpx
from bs4 import BeautifulSoup

url = "https://www.aiaaic.org/aiaaic-repository/ai-algorithmic-and-automation-incidents/INCIDENT-SLUG"
response = httpx.get(url, follow_redirects=True)
soup = BeautifulSoup(response.text, "lxml")

sections = soup.find_all("section")
for i, s in enumerate(sections):
    print(f"Section {i}: {s.get_text()[:100]}")
```

### Updating skip patterns

Edit `skip_patterns` list in `src/page_scraper.py` to filter unwanted URLs from source links.

## Files to Update on Changes

When making changes, remember to update:
- `CHANGELOG.md` - Document what changed
- `README.md` - If usage or features changed
- This file - If architecture or conventions changed
- `app.py` - If data model fields changed (CORE_FIELDS, LIST_FIELDS may need updating)
