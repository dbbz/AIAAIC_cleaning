"""Rich console output helpers for the AIAAIC scraper."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table
from rich.text import Text

console = Console()


@dataclass
class ScrapeStats:
    """Statistics for the scraping session."""

    total: int = 0
    processed: int = 0
    successful: int = 0
    failed: int = 0
    skipped: int = 0
    no_url: int = 0
    start_time: datetime = field(default_factory=datetime.now)

    # Field extraction stats
    descriptions_found: int = 0
    source_links_found: int = 0
    related_found: int = 0

    @property
    def elapsed_seconds(self) -> float:
        return (datetime.now() - self.start_time).total_seconds()

    @property
    def rate(self) -> float:
        if self.elapsed_seconds > 0:
            return self.processed / self.elapsed_seconds
        return 0.0


def create_progress() -> Progress:
    """Create a Rich progress bar for scraping."""
    return Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
        expand=True,
    )


def print_header() -> None:
    """Print the scraper header."""
    console.print()
    console.print(
        Panel.fit(
            "[bold cyan]AIAAIC Database Scraper[/bold cyan]\n"
            "[dim]Extracting AI incident data from aiaaic.org[/dim]",
            border_style="cyan",
        )
    )
    console.print()


def print_config(
    csv_url: str,
    output_path: str,
    total_incidents: int,
    already_processed: int,
    concurrency: int,
    sample: int | None = None,
) -> None:
    """Print the configuration panel."""
    table = Table(show_header=False, box=None, padding=(0, 2))
    table.add_column("Key", style="dim")
    table.add_column("Value")

    table.add_row("CSV Source", f"[link={csv_url}]{csv_url[:60]}...[/link]")
    table.add_row("Output", output_path)
    table.add_row("Total Incidents", str(total_incidents))
    table.add_row("Already Processed", str(already_processed))
    table.add_row("To Process", str(total_incidents - already_processed))
    table.add_row("Concurrency", str(concurrency))
    if sample:
        table.add_row("Sample Mode", f"[yellow]First {sample} incidents only[/yellow]")

    console.print(Panel(table, title="[bold]Configuration[/bold]", border_style="blue"))
    console.print()


def print_incident_status(
    aiaaic_id: str,
    headline: str,
    status: str,
    details: str | None = None,
) -> None:
    """Print status for a single incident (verbose mode)."""
    status_style = {
        "success": "green",
        "warning": "yellow",
        "error": "red",
        "skip": "dim",
    }.get(status, "white")

    headline_short = headline[:50] + "..." if len(headline) > 50 else headline
    msg = f"[{status_style}]{aiaaic_id}[/{status_style}] {headline_short}"
    if details:
        msg += f" [dim]({details})[/dim]"
    console.print(msg)


def print_summary(stats: ScrapeStats) -> None:
    """Print the final summary table."""
    console.print()

    # Main stats table
    table = Table(title="[bold]Scraping Summary[/bold]", show_header=True)
    table.add_column("Metric", style="cyan")
    table.add_column("Count", justify="right")
    table.add_column("", justify="left")

    table.add_row("Total Processed", str(stats.processed), "")
    table.add_row(
        "Successful",
        str(stats.successful),
        f"[green]({stats.successful/max(stats.processed,1)*100:.1f}%)[/green]",
    )
    table.add_row(
        "Failed",
        str(stats.failed),
        f"[red]({stats.failed/max(stats.processed,1)*100:.1f}%)[/red]" if stats.failed else "",
    )
    table.add_row("Skipped (already done)", str(stats.skipped), "")
    table.add_row("No detail page URL", str(stats.no_url), "")

    console.print(table)

    # Field extraction stats
    if stats.successful > 0:
        console.print()
        field_table = Table(title="[bold]Field Extraction Rates[/bold]", show_header=True)
        field_table.add_column("Field", style="cyan")
        field_table.add_column("Found", justify="right")
        field_table.add_column("Rate", justify="right")

        field_table.add_row(
            "Descriptions",
            str(stats.descriptions_found),
            f"{stats.descriptions_found/stats.successful*100:.1f}%",
        )
        field_table.add_row(
            "Source Links",
            str(stats.source_links_found),
            f"{stats.source_links_found/stats.successful*100:.1f}%",
        )
        field_table.add_row(
            "Related Incidents",
            str(stats.related_found),
            f"{stats.related_found/stats.successful*100:.1f}%",
        )

        console.print(field_table)

    # Timing
    elapsed = stats.elapsed_seconds
    console.print()
    console.print(f"[dim]Time elapsed: {elapsed:.1f}s | Rate: {stats.rate:.2f} incidents/sec[/dim]")
    console.print()


def print_error(message: str, exception: Exception | None = None) -> None:
    """Print an error message."""
    console.print(f"[red]Error:[/red] {message}")
    if exception:
        console.print(f"[dim]{type(exception).__name__}: {exception}[/dim]")


def print_warning(message: str) -> None:
    """Print a warning message."""
    console.print(f"[yellow]Warning:[/yellow] {message}")


def print_success(message: str) -> None:
    """Print a success message."""
    console.print(f"[green]Success:[/green] {message}")


def print_info(message: str) -> None:
    """Print an info message."""
    console.print(f"[blue]Info:[/blue] {message}")


def print_sample_output(incident: Any) -> None:
    """Print a sample incident for validation."""
    console.print()
    console.print(Panel(f"[bold]Sample: {incident.aiaaic_id}[/bold]", border_style="green"))

    table = Table(show_header=False, box=None)
    table.add_column("Field", style="cyan", width=20)
    table.add_column("Value", overflow="fold")

    table.add_row("Headline", incident.headline[:100] + "..." if len(incident.headline) > 100 else incident.headline)
    table.add_row("Occurred", incident.occurred)
    table.add_row("Countries", ", ".join(incident.countries) if incident.countries else "[dim]None[/dim]")
    table.add_row("Sectors", ", ".join(incident.sectors[:3]) + ("..." if len(incident.sectors) > 3 else "") if incident.sectors else "[dim]None[/dim]")
    table.add_row("Description", (incident.description[:150] + "...") if incident.description and len(incident.description) > 150 else (incident.description or "[dim]None[/dim]"))
    table.add_row("Source Links", str(len(incident.source_links)) if incident.source_links else "[dim]0[/dim]")
    table.add_row("Related", str(len(incident.related_incidents)) if incident.related_incidents else "[dim]0[/dim]")
    table.add_row("Page Scraped", "[green]Yes[/green]" if incident.page_scraped else "[yellow]No[/yellow]")

    console.print(table)
    console.print()
