#!/usr/bin/env python3
"""AIAAIC Database Scraper CLI.

Usage:
    uv run scrape.py                    # Full scrape (resume if interrupted)
    uv run scrape.py --sample 10        # Scrape only 10 pages (for testing)
    uv run scrape.py --force            # Re-scrape all entries
    uv run scrape.py --retry-errors     # Retry only failed entries
    uv run scrape.py --export json      # Export JSONL to single JSON file
    uv run scrape.py --export csv       # Export to flattened CSV
    uv run scrape.py --no-url           # List incidents without detail URLs
    uv run scrape.py --single AIAAIC123 # Scrape and display a single incident
    uv run scrape.py --errors           # List failed scrapes
    uv run scrape.py --incomplete       # List incidents with missing page data
    uv run scrape.py --rescrape-incomplete  # Find and rescrape incomplete
    uv run scrape.py --concurrency 20   # Set concurrent requests (default: 20)
    uv run scrape.py --verbose          # Show detailed extraction info
"""

import argparse
import asyncio
import sys
from pathlib import Path

from src import console as con
from src.scraper import DEFAULT_ERRORS_FILE, DEFAULT_OUTPUT_FILE, run_scraper
from src.utils import export_to_csv, export_to_json


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Scrape the AIAAIC AI Incidents database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Mode arguments (mutually exclusive)
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--force",
        action="store_true",
        help="Re-scrape all entries, ignoring previous progress",
    )
    mode_group.add_argument(
        "--retry-errors",
        action="store_true",
        help="Only retry entries that previously failed",
    )
    mode_group.add_argument(
        "--update",
        action="store_true",
        help="Re-scrape entries that already exist (update mode)",
    )
    mode_group.add_argument(
        "--export",
        choices=["json", "csv"],
        help="Export JSONL to another format (json or csv)",
    )
    mode_group.add_argument(
        "--no-url",
        action="store_true",
        help="List incidents without detail page URLs (for manual investigation)",
    )
    mode_group.add_argument(
        "--single",
        metavar="ID",
        help="Scrape a single incident by AIAAIC ID and display it",
    )
    mode_group.add_argument(
        "--errors",
        action="store_true",
        help="List incidents that failed scraping",
    )
    mode_group.add_argument(
        "--incomplete",
        action="store_true",
        help="List scraped incidents with missing page data (description, sources)",
    )
    mode_group.add_argument(
        "--rescrape-incomplete",
        action="store_true",
        help="Find and rescrape incidents with missing page data",
    )

    # Options
    parser.add_argument(
        "--sample",
        type=int,
        metavar="N",
        help="Only scrape first N incidents (for testing)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=20,
        metavar="N",
        help="Number of concurrent requests (default: 20)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show detailed status for each incident",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=DEFAULT_OUTPUT_FILE,
        help=f"Output file path (default: {DEFAULT_OUTPUT_FILE})",
    )

    args = parser.parse_args()

    # Handle --no-url mode: list incidents without detail page URLs
    if args.no_url:
        from src.csv_parser import fetch_incidents

        con.console.print("[bold]Fetching CSV from Google Sheets...[/bold]")
        incidents = fetch_incidents()
        no_url_incidents = [i for i in incidents if not i.detail_page_url]

        from rich.table import Table

        table = Table(title=f"Incidents without detail page URL ({len(no_url_incidents)} total)")
        table.add_column("AIAAIC ID", style="cyan")
        table.add_column("Occurred", style="yellow")
        table.add_column("Headline", style="white", max_width=80)

        for incident in no_url_incidents:
            table.add_row(
                incident.aiaaic_id,
                incident.occurred or "-",
                incident.headline[:80] + "..." if len(incident.headline) > 80 else incident.headline,
            )

        con.console.print(table)
        con.console.print(f"\n[dim]Total incidents: {len(incidents)} | Without URL: {len(no_url_incidents)}[/dim]")
        return 0

    # Handle --single mode: scrape and display a single incident
    if args.single:
        from src.csv_parser import fetch_incidents
        from src.page_scraper import scrape_page_sync
        from rich.panel import Panel
        from rich.text import Text
        import httpx

        aiaaic_id = args.single.upper()
        if not aiaaic_id.startswith("AIAAIC"):
            aiaaic_id = f"AIAAIC{aiaaic_id}"

        con.console.print(f"[bold]Fetching CSV to find {aiaaic_id}...[/bold]")
        incidents = fetch_incidents()
        incident = next((i for i in incidents if i.aiaaic_id == aiaaic_id), None)

        if not incident:
            con.print_error(f"Incident {aiaaic_id} not found in CSV")
            return 1

        # Scrape the detail page if URL exists
        if incident.detail_page_url:
            con.console.print(f"[bold]Scraping detail page...[/bold]")
            try:
                page_data = scrape_page_sync(incident.detail_page_url)
                incident.description = page_data.description
                incident.source_links = page_data.source_links
                incident.related_incidents = page_data.related_incidents
                incident.page_published = page_data.page_published
                incident.page_scraped = True
            except httpx.HTTPError as e:
                con.print_warning(f"Failed to scrape page: {e}")

        # Display the incident nicely
        con.console.print()
        con.console.print(Panel(
            f"[bold cyan]{incident.aiaaic_id}[/bold cyan]",
            title="AIAAIC Incident",
            subtitle=incident.occurred or "Unknown date",
        ))
        con.console.print(f"\n[bold]{incident.headline}[/bold]\n")

        if incident.description:
            con.console.print(Panel(incident.description, title="Description", border_style="dim"))

        # Metadata table
        from rich.table import Table
        meta = Table(show_header=False, box=None, padding=(0, 2))
        meta.add_column("Field", style="dim")
        meta.add_column("Value")

        if incident.countries:
            meta.add_row("Countries", ", ".join(incident.countries))
        if incident.sectors:
            meta.add_row("Sectors", ", ".join(incident.sectors))
        if incident.deployers:
            meta.add_row("Deployers", ", ".join(incident.deployers))
        if incident.developers:
            meta.add_row("Developers", ", ".join(incident.developers))
        if incident.system_names:
            meta.add_row("Systems", ", ".join(incident.system_names))
        if incident.technologies:
            meta.add_row("Technologies", ", ".join(incident.technologies))
        if incident.issues:
            meta.add_row("Issues", ", ".join(incident.issues))

        con.console.print(meta)

        # Source links
        if incident.source_links:
            con.console.print(f"\n[bold]Source Links ({len(incident.source_links)}):[/bold]")
            for link in incident.source_links:
                title = link.title or link.url
                con.console.print(f"  [link={link.url}]{title}[/link]")

        # Related incidents
        if incident.related_incidents:
            con.console.print(f"\n[bold]Related Incidents ({len(incident.related_incidents)}):[/bold]")
            for rel in incident.related_incidents:
                con.console.print(f"  [link={rel.url}]{rel.title}[/link]")

        if incident.detail_page_url:
            con.console.print(f"\n[dim]Detail page: {incident.detail_page_url}[/dim]")

        return 0

    # Handle --errors mode: list failed scrapes
    if args.errors:
        from src.utils import load_errors

        errors = list(load_errors(DEFAULT_ERRORS_FILE))
        if not errors:
            con.console.print("[green]No errors found![/green]")
            return 0

        from rich.table import Table

        table = Table(title=f"Scraping Errors ({len(errors)} total)")
        table.add_column("AIAAIC ID", style="cyan")
        table.add_column("Error Type", style="red")
        table.add_column("Message", style="white", max_width=60)
        table.add_column("When", style="dim")

        for error in errors:
            table.add_row(
                error.aiaaic_id,
                error.error_type,
                error.error_message[:60] + "..." if len(error.error_message) > 60 else error.error_message,
                error.timestamp.strftime("%Y-%m-%d %H:%M"),
            )

        con.console.print(table)
        con.console.print(f"\n[dim]Use --retry-errors to retry these incidents[/dim]")
        return 0

    # Handle --incomplete mode: find incidents with missing page data
    if args.incomplete:
        from src.utils import load_incidents

        output_path = args.output
        if not output_path.exists():
            con.print_error(f"Output file not found: {output_path}")
            con.console.print("[dim]Run the scraper first to generate data[/dim]")
            return 1

        incidents = list(load_incidents(output_path))
        incomplete = []

        for inc in incidents:
            if not inc.page_scraped:
                continue  # Skip unscraped incidents
            missing = []
            if not inc.description:
                missing.append("description")
            if not inc.source_links:
                missing.append("sources")
            # Note: related_incidents can legitimately be empty, so not checked
            if missing:
                incomplete.append((inc, missing))

        if not incomplete:
            con.console.print("[green]All scraped incidents have complete page data![/green]")
            return 0

        from rich.table import Table

        table = Table(title=f"Incomplete Incidents ({len(incomplete)} total)")
        table.add_column("AIAAIC ID", style="cyan")
        table.add_column("Headline", style="white", max_width=50)
        table.add_column("Missing", style="yellow")

        for inc, missing in incomplete:
            table.add_row(
                inc.aiaaic_id,
                inc.headline[:50] + "..." if len(inc.headline) > 50 else inc.headline,
                ", ".join(missing),
            )

        con.console.print(table)
        scraped_count = sum(1 for i in incidents if i.page_scraped)
        con.console.print(f"\n[dim]Scraped: {scraped_count} | Incomplete: {len(incomplete)} | Complete: {scraped_count - len(incomplete)}[/dim]")
        return 0

    # Handle --rescrape-incomplete mode: find and rescrape incomplete incidents
    if args.rescrape_incomplete:
        from src.utils import load_incidents

        output_path = args.output
        if not output_path.exists():
            con.print_error(f"Output file not found: {output_path}")
            con.console.print("[dim]Run the scraper first to generate data[/dim]")
            return 1

        # Find incomplete incident IDs
        incidents = list(load_incidents(output_path))
        incomplete_ids: set[str] = set()

        for inc in incidents:
            if not inc.page_scraped:
                continue
            if not inc.description or not inc.source_links:
                incomplete_ids.add(inc.aiaaic_id)

        if not incomplete_ids:
            con.console.print("[green]All scraped incidents have complete page data![/green]")
            return 0

        con.console.print(f"[bold]Found {len(incomplete_ids)} incomplete incidents to rescrape[/bold]\n")

        # Run the scraper targeting only incomplete IDs
        try:
            stats = asyncio.run(
                run_scraper(
                    output_path=output_path,
                    errors_path=DEFAULT_ERRORS_FILE,
                    target_ids=incomplete_ids,
                    concurrency=args.concurrency,
                    verbose=args.verbose,
                )
            )
            return 1 if stats.failed > 0 else 0
        except KeyboardInterrupt:
            con.print_warning("\nInterrupted by user. Progress has been saved.")
            return 130

    # Handle export mode
    if args.export:
        output_path = args.output
        if not output_path.exists():
            con.print_error(f"Output file not found: {output_path}")
            return 1

        if args.export == "json":
            export_path = output_path.with_suffix(".json")
            count = export_to_json(output_path, export_path)
            con.print_success(f"Exported {count} incidents to {export_path}")
        elif args.export == "csv":
            export_path = output_path.with_suffix(".csv")
            count = export_to_csv(output_path, export_path)
            con.print_success(f"Exported {count} incidents to {export_path}")

        return 0

    # Run the scraper
    try:
        stats = asyncio.run(
            run_scraper(
                output_path=args.output,
                errors_path=DEFAULT_ERRORS_FILE,
                force=args.force,
                retry_errors=args.retry_errors,
                update=args.update,
                sample=args.sample,
                concurrency=args.concurrency,
                verbose=args.verbose,
            )
        )

        # Return error code if there were failures
        if stats.failed > 0:
            return 1
        return 0

    except KeyboardInterrupt:
        con.print_warning("\nInterrupted by user. Progress has been saved.")
        return 130
    except Exception as e:
        con.print_error(f"Unexpected error: {e}", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
