# AIAAIC Database Scraper

A Python tool to extract and structure data from the [AIAAIC](https://www.aiaaic.org/) (AI, Algorithmic, and Automation Incidents and Controversies) database.

## Features

- **Full data extraction**: Combines CSV metadata with detail page content
- **Resumable**: Automatically skips already-processed incidents
- **Concurrent**: Async HTTP requests with configurable concurrency
- **Robust parsing**: Uses text patterns instead of fragile CSS selectors
- **Rich terminal UI**: Progress bars, colored output, and summary tables
- **Export options**: JSONL (default), JSON, or CSV formats

## Data Source

The scraper extracts data from two sources:

1. **CSV**: The AIAAIC public database exported from Google Sheets (~2,100+ incidents)
2. **Detail pages**: Individual incident pages on aiaaic.org with descriptions, source links, and related incidents

## Installation

Requires Python 3.14+ and [uv](https://docs.astral.sh/uv/).

```bash
# Clone the repository
git clone <repository-url>
cd AIAAIC_scrap

# Install dependencies (uv handles this automatically on first run)
uv sync
```

## Usage

### Basic Usage

```bash
# Full scrape (resumes from previous progress)
uv run scrape.py

# Test with a small sample
uv run scrape.py --sample 10 --verbose
```

### Options

```bash
uv run scrape.py [OPTIONS]

Options:
  --sample N        Scrape only first N incidents (for testing)
  --force           Re-scrape all entries, ignoring previous progress
  --retry-errors    Retry only entries that previously failed
  --update          Re-scrape existing entries (update mode)
  --export FORMAT   Export JSONL to another format (json or csv)
  --no-url          List incidents without detail page URLs
  --single ID       Scrape and display a single incident by AIAAIC ID
  --errors          List incidents that failed scraping
  --incomplete      List incidents with missing page data
  --rescrape-incomplete  Find and rescrape incomplete incidents
  --concurrency N   Number of concurrent requests (default: 20)
  --verbose, -v     Show detailed status for each incident
  --output, -o      Output file path (default: data/aiaaic_incidents.jsonl)
```

### Examples

```bash
# Full scrape with default settings
uv run scrape.py

# Quick test run with verbose output
uv run scrape.py --sample 10 --verbose

# Re-scrape everything from scratch
uv run scrape.py --force

# Retry only failed entries
uv run scrape.py --retry-errors

# Export to JSON array
uv run scrape.py --export json

# Export to CSV (flattened)
uv run scrape.py --export csv

# Even higher concurrency for faster scraping
uv run scrape.py --concurrency 50

# List incidents without detail URLs (for manual investigation)
uv run scrape.py --no-url

# Scrape and display a single incident
uv run scrape.py --single AIAAIC2155

# List failed scrapes
uv run scrape.py --errors

# Find incidents with missing page data
uv run scrape.py --incomplete

# Rescrape all incomplete incidents
uv run scrape.py --rescrape-incomplete
```

## Output

### Default: JSONL

The scraper outputs to `data/aiaaic_incidents.jsonl` (JSON Lines format - one JSON object per line).

This format is chosen for:
- **Resumability**: Can check existing IDs and skip processed records
- **Incremental updates**: Append new records without rewriting the file
- **Robustness**: No file corruption if interrupted mid-write

### Data Schema

Each incident record contains:

```json
{
  "aiaaic_id": "AIAAIC2155",
  "headline": "Incident headline from CSV",
  "occurred": "2024",
  "countries": ["USA", "UK"],
  "sectors": ["Technology", "Finance"],
  "deployers": ["Company A"],
  "developers": ["Company B"],
  "system_names": ["System X"],
  "technologies": ["Machine learning"],
  "purposes": ["Classification"],
  "news_triggers": ["News report"],
  "issues": ["Bias/discrimination"],
  "external_harms": {
    "individual": ["Privacy violation"],
    "societal": ["Trust erosion"],
    "environmental": []
  },
  "internal_impacts": {
    "strategic_reputational": ["Brand damage"],
    "operational": [],
    "financial": ["Legal costs"],
    "legal_regulatory": ["Fine"]
  },
  "detail_page_url": "https://www.aiaaic.org/aiaaic-repository/...",
  "description": "Full description from detail page",
  "source_links": [
    {"url": "https://news.example.com/article", "title": "Article Title"}
  ],
  "related_incidents": [
    {"title": "Related Incident", "url": "https://www.aiaaic.org/..."}
  ],
  "page_published": "January 2024",
  "page_scraped": true,
  "scraped_at": "2024-01-15T10:30:00"
}
```

### Export Formats

```bash
# Export to single JSON array
uv run scrape.py --export json
# Output: data/aiaaic_incidents.json

# Export to flattened CSV
uv run scrape.py --export csv
# Output: data/aiaaic_incidents.csv
```

## Project Structure

```
AIAAIC_scrap/
├── src/
│   ├── __init__.py
│   ├── models.py          # Pydantic schemas
│   ├── csv_parser.py      # CSV download and parsing
│   ├── page_scraper.py    # Detail page scraping
│   ├── scraper.py         # Main orchestration
│   ├── console.py         # Rich terminal output
│   └── utils.py           # File I/O helpers
├── data/
│   ├── aiaaic_incidents.jsonl  # Main output
│   └── errors.jsonl            # Failed scrapes
├── scrape.py              # CLI entry point
├── pyproject.toml         # Dependencies (uv)
├── CLAUDE.md              # AI assistant instructions
├── README.md              # This file
└── CHANGELOG.md           # Version history
```

## Technical Notes

### CSV Download

The CSV is downloaded from Google Sheets using `curl` subprocess. This is because httpx has issues with Google's cross-origin redirects that strip headers. The scraper requires `curl` to be available on the system (standard on macOS/Linux).

### Robust Page Parsing

The AIAAIC website uses Google Sites, which generates random CSS class names that can change between builds. This scraper uses text patterns and URL filtering instead of CSS selectors:

- **Metadata**: Extracted via regex patterns (`Occurred:`, `Page published:`)
- **Source links**: External URLs filtered by domain (also extracts plain text URLs)
- **Related incidents**: Internal AIAAIC URLs in the "Related" section
- **Description**: Multi-strategy extraction with fallbacks

### Error Handling

- Failed requests are logged to `data/errors.jsonl`
- Use `--retry-errors` to retry failed entries
- HTTP 404s are recorded but not treated as errors
- Rate limiting (429) triggers automatic retry with backoff

## License

MIT License - see LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Update CHANGELOG.md
5. Submit a pull request

## Acknowledgments

Data sourced from the [AIAAIC Repository](https://www.aiaaic.org/aiaaic-repository), an open database of AI incidents maintained by the AI Incident Database community.
