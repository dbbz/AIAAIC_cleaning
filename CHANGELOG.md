# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `--min-desc-length N` option for `--rescrape-incomplete` to also rescrape records with descriptions shorter than N characters (e.g., `--min-desc-length 500`)
- `--no-url` flag to list incidents without detail page URLs for manual investigation
- `--single <ID>` flag to scrape and display a single incident with formatted output
- `--errors` flag to list all failed scrapes from errors.jsonl
- `--incomplete` flag to find scraped incidents missing page data (description, sources)
- `--rescrape-incomplete` flag to find and rescrape incomplete incidents in one step
- `--check` flag for data consistency validation (detects duplicates, malformed records)
- `--deduplicate` flag to automatically remove duplicates, keeping the best version
- **Data Consistency page** in Streamlit app for visual duplicate detection and one-click deduplication

### Improved

- **Full multi-paragraph description extraction**: Now extracts complete descriptions with all narrative sections ("What happened", "Why it happened", "What it means") instead of just the first paragraph. Descriptions went from ~150 chars to ~800+ chars on average.
- **Duplicate prevention**: Added `remove_ids_from_jsonl()` utility to prevent duplicates when using `--retry-errors` or `--rescrape-incomplete`
- **Faster scraping**: Increased default concurrency from 10 to 20 parallel requests. Added HTTP/2 support and optimized connection pooling.

### Fixed

- **Fixed short description extraction**: Three bugs were causing incomplete descriptions:
  1. `_has_narrative_content()` required 2+ substantial paragraphs - now accepts 1+ or >200 chars total
  2. Narrative headings like "What happened" (13 chars) were filtered by length check before recognition - now handled first
  3. `^Developer` pattern in metadata detection matched narrative text - now requires colon (e.g., `^Developer\s*:`)
- **Fixed Google Sheets 400 error**: Switched to curl subprocess for CSV download to handle Google's cross-origin redirects that strip headers in httpx
- **Fixed missing source links**: Now extracts URLs from plain text (not just `<a>` tags) - many AIAAIC pages list source URLs as underlined `<span>` elements
- **Fixed --single mode**: Added sync wrapper for page scraping to fix TypeError

## [0.1.0] - 2024-12-11

### Added

- Initial release of the AIAAIC Database Scraper
- CSV parsing from Google Sheets export
  - Handles multi-row headers (rows 2-3)
  - Parses all 20 columns including nested harm/impact categories
  - Splits semicolon-separated fields into lists
- Detail page scraping with robust text-pattern extraction
  - Description extraction with multiple fallback strategies
  - Source link extraction (external news/analysis URLs)
  - Related incident linking
  - Metadata extraction (occurred date, page published)
- JSONL output format for resumability
- CLI interface with options:
  - `--sample N` for testing
  - `--force` for full re-scrape
  - `--retry-errors` for retrying failures
  - `--export json|csv` for format conversion
  - `--concurrency N` for parallel requests
  - `--verbose` for detailed logging
- Rich terminal UI:
  - Progress bars with ETA
  - Configuration panel
  - Summary tables with extraction rates
  - Colored status output
- Error handling:
  - Automatic retry with backoff for rate limiting
  - Error logging to `errors.jsonl`
  - Graceful handling of 404s and timeouts

### Technical Notes

- Uses section-based parsing instead of CSS selectors (Google Sites compatibility)
- URL pattern filtering excludes social share links, Wikipedia, Google Forms
- Async HTTP with httpx and semaphore-based concurrency
- Pydantic v2 for data validation
