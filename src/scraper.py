"""Main scraper orchestration for AIAAIC database."""

import asyncio
from datetime import datetime
from pathlib import Path
from typing import Callable

import httpx

from . import console as con
from .console import ScrapeStats
from .csv_parser import CSV_URL, fetch_incidents
from .models import AIAAICIncident, ScrapingError
from .page_scraper import scrape_page
from .utils import (
    append_error,
    append_incident,
    clear_errors,
    load_error_ids,
    load_processed_ids,
    remove_ids_from_jsonl,
)

# Default paths
DEFAULT_OUTPUT_DIR = Path("data")
DEFAULT_OUTPUT_FILE = DEFAULT_OUTPUT_DIR / "aiaaic_incidents.jsonl"
DEFAULT_ERRORS_FILE = DEFAULT_OUTPUT_DIR / "errors.jsonl"

# Timeouts and limits
REQUEST_TIMEOUT = 30.0
MAX_RETRIES = 3
RETRY_BACKOFF = [1, 2, 5]  # seconds
DEFAULT_CONCURRENCY = 20  # Parallel requests (was 10)


async def scrape_single_incident(
    client: httpx.AsyncClient,
    incident: AIAAICIncident,
    stats: ScrapeStats,
    output_path: Path,
    errors_path: Path,
    verbose: bool = False,
) -> bool:
    """Scrape a single incident's detail page and update the record.

    Returns True if successful, False otherwise.
    """
    if not incident.detail_page_url:
        # No URL to scrape, just save the CSV data
        stats.no_url += 1
        incident.page_scraped = False
        append_incident(output_path, incident)
        if verbose:
            con.print_incident_status(
                incident.aiaaic_id, incident.headline, "skip", "no detail URL"
            )
        return True

    # Try to scrape the page
    last_error = None
    for attempt, backoff in enumerate(RETRY_BACKOFF):
        try:
            page_data = await scrape_page(client, incident.detail_page_url)

            # Update incident with scraped data
            incident.description = page_data.description
            incident.source_links = page_data.source_links or []
            incident.related_incidents = page_data.related_incidents or []
            incident.page_published = page_data.page_published
            incident.page_scraped = True
            incident.scraped_at = datetime.now()

            # Update stats
            if page_data.description:
                stats.descriptions_found += 1
            if page_data.source_links:
                stats.source_links_found += 1
            if page_data.related_incidents:
                stats.related_found += 1

            # Save to file
            append_incident(output_path, incident)

            if verbose:
                details = []
                if page_data.description:
                    details.append("desc")
                if page_data.source_links:
                    details.append(f"{len(page_data.source_links)} links")
                con.print_incident_status(
                    incident.aiaaic_id,
                    incident.headline,
                    "success",
                    ", ".join(details) if details else None,
                )

            return True

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                # Page doesn't exist, skip it
                incident.page_scraped = False
                append_incident(output_path, incident)
                if verbose:
                    con.print_incident_status(
                        incident.aiaaic_id, incident.headline, "warning", "404 not found"
                    )
                return True
            elif e.response.status_code in (429, 503):
                # Rate limited or server overloaded, retry
                last_error = e
                if attempt < len(RETRY_BACKOFF) - 1:
                    await asyncio.sleep(backoff)
                    continue
            else:
                last_error = e
                break

        except httpx.TimeoutException as e:
            last_error = e
            if attempt < len(RETRY_BACKOFF) - 1:
                await asyncio.sleep(backoff)
                continue

        except Exception as e:
            last_error = e
            break

    # Failed after all retries
    error = ScrapingError(
        aiaaic_id=incident.aiaaic_id,
        url=incident.detail_page_url,
        error_type=type(last_error).__name__ if last_error else "Unknown",
        error_message=str(last_error) if last_error else "Unknown error",
    )
    append_error(errors_path, error)

    if verbose:
        con.print_incident_status(
            incident.aiaaic_id,
            incident.headline,
            "error",
            str(last_error)[:50] if last_error else None,
        )

    return False


async def scrape_batch(
    incidents: list[AIAAICIncident],
    stats: ScrapeStats,
    output_path: Path,
    errors_path: Path,
    concurrency: int = DEFAULT_CONCURRENCY,
    verbose: bool = False,
    on_progress: Callable[[int], None] | None = None,
) -> None:
    """Scrape a batch of incidents concurrently."""
    semaphore = asyncio.Semaphore(concurrency)

    # Configure client for high-throughput scraping
    limits = httpx.Limits(
        max_connections=concurrency + 10,  # Allow extra connections
        max_keepalive_connections=concurrency,
    )
    async with httpx.AsyncClient(
        timeout=REQUEST_TIMEOUT,
        limits=limits,
        http2=True,  # Enable HTTP/2 multiplexing if server supports it
    ) as client:

        async def scrape_with_semaphore(incident: AIAAICIncident) -> bool:
            async with semaphore:
                success = await scrape_single_incident(
                    client, incident, stats, output_path, errors_path, verbose
                )
                stats.processed += 1
                if success:
                    stats.successful += 1
                else:
                    stats.failed += 1
                if on_progress:
                    on_progress(1)
                return success

        # Run all scrapes concurrently (limited by semaphore)
        await asyncio.gather(*[scrape_with_semaphore(i) for i in incidents])


async def run_scraper(
    output_path: Path = DEFAULT_OUTPUT_FILE,
    errors_path: Path = DEFAULT_ERRORS_FILE,
    force: bool = False,
    retry_errors: bool = False,
    update: bool = False,
    sample: int | None = None,
    concurrency: int = DEFAULT_CONCURRENCY,
    verbose: bool = False,
    target_ids: set[str] | None = None,
) -> ScrapeStats:
    """Run the main scraping process.

    Args:
        output_path: Path to output JSONL file
        errors_path: Path to errors JSONL file
        force: If True, re-scrape all incidents
        retry_errors: If True, only retry incidents that had errors
        update: If True, re-scrape existing incidents
        sample: If set, only scrape this many incidents
        concurrency: Number of concurrent requests
        verbose: If True, print detailed status for each incident
        target_ids: If set, only scrape these specific incident IDs (implies force for those IDs)

    Returns:
        ScrapeStats with final statistics
    """
    stats = ScrapeStats()

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    con.print_header()

    # Load already-processed IDs (unless force mode)
    processed_ids: set[str] = set()
    error_ids: set[str] = set()

    if not force:
        processed_ids = load_processed_ids(output_path)
        error_ids = load_error_ids(errors_path)

    # Fetch incidents from CSV
    con.print_info("Fetching incident list from CSV...")
    all_incidents = fetch_incidents(CSV_URL)
    stats.total = len(all_incidents)

    # Determine which incidents to process
    if target_ids:
        # Only process specific IDs (for rescrape-incomplete, etc.)
        incidents_to_process = [i for i in all_incidents if i.aiaaic_id in target_ids]
        # Remove existing entries to prevent duplicates
        removed = remove_ids_from_jsonl(output_path, target_ids)
        if removed:
            con.print_info(f"Removed {removed} existing records (will be re-scraped)")
        con.print_info(f"Targeting {len(incidents_to_process)} specific incidents")
    elif retry_errors:
        # Only process incidents that had errors
        incidents_to_process = [i for i in all_incidents if i.aiaaic_id in error_ids]
        # Remove existing entries to prevent duplicates (in case partial data was saved)
        removed = remove_ids_from_jsonl(output_path, error_ids)
        if removed:
            con.print_info(f"Removed {removed} existing records (will be re-scraped)")
        clear_errors(errors_path)
        con.print_info(f"Retrying {len(incidents_to_process)} failed incidents")
    elif update:
        # Re-process all incidents (but don't clear the file - will create duplicates)
        # For update mode, we'd need more complex logic to replace entries
        con.print_warning("Update mode not fully implemented - use --force instead")
        incidents_to_process = all_incidents
    else:
        # Skip already-processed incidents
        incidents_to_process = [i for i in all_incidents if i.aiaaic_id not in processed_ids]
        stats.skipped = len(processed_ids)

    # Apply sample limit
    if sample and len(incidents_to_process) > sample:
        incidents_to_process = incidents_to_process[:sample]

    # Print configuration
    con.print_config(
        csv_url=CSV_URL,
        output_path=str(output_path),
        total_incidents=stats.total,
        already_processed=stats.skipped,
        concurrency=concurrency,
        sample=sample,
    )

    if not incidents_to_process:
        con.print_success("All incidents already processed!")
        return stats

    # Create progress bar
    progress = con.create_progress()

    with progress:
        task = progress.add_task(
            "[cyan]Scraping incidents...", total=len(incidents_to_process)
        )

        def update_progress(n: int) -> None:
            progress.update(task, advance=n)

        # Run the scraper
        await scrape_batch(
            incidents_to_process,
            stats,
            output_path,
            errors_path,
            concurrency=concurrency,
            verbose=verbose,
            on_progress=update_progress,
        )

    # Print summary
    con.print_summary(stats)

    # Show sample output if in sample mode
    if sample and stats.successful > 0:
        # Load and show a sample
        from .utils import load_incidents

        for incident in load_incidents(output_path):
            if incident.page_scraped:
                con.print_sample_output(incident)
                break

    return stats
