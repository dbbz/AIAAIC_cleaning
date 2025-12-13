# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `--no-url` flag to list incidents without detail page URLs for manual investigation
- `--single <ID>` flag to scrape and display a single incident with formatted output
- `--errors` flag to list all failed scrapes from errors.jsonl
- `--incomplete` flag to find scraped incidents missing page data (description, sources)
- `--rescrape-incomplete` flag to find and rescrape incomplete incidents in one step

### Fixed

- Added User-Agent header to CSV download to prevent 400 Bad Request errors from Google Sheets
- **Fixed missing source links**: Now extracts URLs from plain text (not just `<a>` tags) - many AIAAIC pages list source URLs as underlined `<span>` elements

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
