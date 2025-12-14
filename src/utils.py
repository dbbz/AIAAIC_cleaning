"""File I/O utilities for the AIAAIC scraper."""

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterator

from .models import AIAAICIncident, ScrapingError


def load_processed_ids(jsonl_path: Path) -> set[str]:
    """Load the set of already-processed AIAAIC IDs from the JSONL file."""
    ids: set[str] = set()
    if not jsonl_path.exists():
        return ids

    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                if "aiaaic_id" in data:
                    ids.add(data["aiaaic_id"])
            except json.JSONDecodeError:
                continue

    return ids


def load_error_ids(errors_path: Path) -> set[str]:
    """Load the set of AIAAIC IDs that had errors."""
    ids: set[str] = set()
    if not errors_path.exists():
        return ids

    with open(errors_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                if "aiaaic_id" in data:
                    ids.add(data["aiaaic_id"])
            except json.JSONDecodeError:
                continue

    return ids


def load_errors(errors_path: Path) -> Iterator[ScrapingError]:
    """Load all scraping errors from the errors JSONL file."""
    if not errors_path.exists():
        return

    with open(errors_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                yield ScrapingError.model_validate(data)
            except (json.JSONDecodeError, ValueError):
                continue


def append_incident(jsonl_path: Path, incident: AIAAICIncident) -> None:
    """Append a single incident to the JSONL file."""
    with open(jsonl_path, "a", encoding="utf-8") as f:
        f.write(incident.model_dump_json() + "\n")
        f.flush()


def append_error(errors_path: Path, error: ScrapingError) -> None:
    """Append a scraping error to the errors JSONL file."""
    with open(errors_path, "a", encoding="utf-8") as f:
        f.write(error.model_dump_json() + "\n")
        f.flush()


def load_incidents(jsonl_path: Path) -> Iterator[AIAAICIncident]:
    """Load all incidents from the JSONL file."""
    if not jsonl_path.exists():
        return

    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                yield AIAAICIncident.model_validate(data)
            except (json.JSONDecodeError, ValueError):
                continue


def export_to_json(jsonl_path: Path, output_path: Path) -> int:
    """Export JSONL to a single JSON array file.

    Returns the number of incidents exported.
    """
    incidents = list(load_incidents(jsonl_path))
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(
            [incident.model_dump(mode="json") for incident in incidents],
            f,
            indent=2,
            ensure_ascii=False,
        )
    return len(incidents)


def export_to_csv(jsonl_path: Path, output_path: Path) -> int:
    """Export JSONL to a flattened CSV file.

    Returns the number of incidents exported.
    """
    import csv

    incidents = list(load_incidents(jsonl_path))
    if not incidents:
        return 0

    # Define CSV columns (flattening nested structures)
    fieldnames = [
        "aiaaic_id",
        "headline",
        "occurred",
        "countries",
        "sectors",
        "deployers",
        "developers",
        "system_names",
        "technologies",
        "purposes",
        "news_triggers",
        "issues",
        "external_harms_individual",
        "external_harms_societal",
        "external_harms_environmental",
        "internal_impacts_strategic_reputational",
        "internal_impacts_operational",
        "internal_impacts_financial",
        "internal_impacts_legal_regulatory",
        "detail_page_url",
        "description",
        "source_links_count",
        "related_incidents_count",
        "page_published",
        "page_scraped",
        "scraped_at",
    ]

    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for incident in incidents:
            row = {
                "aiaaic_id": incident.aiaaic_id,
                "headline": incident.headline,
                "occurred": incident.occurred,
                "countries": "; ".join(incident.countries),
                "sectors": "; ".join(incident.sectors),
                "deployers": "; ".join(incident.deployers),
                "developers": "; ".join(incident.developers),
                "system_names": "; ".join(incident.system_names),
                "technologies": "; ".join(incident.technologies),
                "purposes": "; ".join(incident.purposes),
                "news_triggers": "; ".join(incident.news_triggers),
                "issues": "; ".join(incident.issues),
                "external_harms_individual": "; ".join(incident.external_harms.individual),
                "external_harms_societal": "; ".join(incident.external_harms.societal),
                "external_harms_environmental": "; ".join(incident.external_harms.environmental),
                "internal_impacts_strategic_reputational": "; ".join(
                    incident.internal_impacts.strategic_reputational
                ),
                "internal_impacts_operational": "; ".join(incident.internal_impacts.operational),
                "internal_impacts_financial": "; ".join(incident.internal_impacts.financial),
                "internal_impacts_legal_regulatory": "; ".join(
                    incident.internal_impacts.legal_regulatory
                ),
                "detail_page_url": incident.detail_page_url or "",
                "description": incident.description or "",
                "source_links_count": len(incident.source_links),
                "related_incidents_count": len(incident.related_incidents),
                "page_published": incident.page_published or "",
                "page_scraped": str(incident.page_scraped),
                "scraped_at": incident.scraped_at.isoformat(),
            }
            writer.writerow(row)

    return len(incidents)


def clear_errors(errors_path: Path) -> None:
    """Clear the errors file."""
    if errors_path.exists():
        errors_path.unlink()


def remove_ids_from_jsonl(jsonl_path: Path, ids_to_remove: set[str]) -> int:
    """Remove records with specific IDs from the JSONL file.

    This prevents duplicates when re-scraping specific incidents.

    Returns the number of records removed.
    """
    if not jsonl_path.exists() or not ids_to_remove:
        return 0

    # Read all records, filtering out the ones to remove
    records_to_keep = []
    removed_count = 0

    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                if data.get("aiaaic_id") in ids_to_remove:
                    removed_count += 1
                else:
                    records_to_keep.append(line)
            except json.JSONDecodeError:
                records_to_keep.append(line)  # Keep malformed lines

    # Write back
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for line in records_to_keep:
            f.write(line + "\n")

    return removed_count


# === DATA CONSISTENCY CHECKING ===


@dataclass
class DuplicateGroup:
    """A group of duplicate records for the same AIAAIC ID."""

    aiaaic_id: str
    records: list[AIAAICIncident]
    best_record: AIAAICIncident
    removed_count: int

    @property
    def count(self) -> int:
        return len(self.records)


@dataclass
class ConsistencyReport:
    """Report from data consistency check."""

    total_records: int
    unique_ids: int
    duplicate_groups: list[DuplicateGroup]
    malformed_lines: int
    records_without_id: int

    @property
    def total_duplicates(self) -> int:
        return sum(g.count - 1 for g in self.duplicate_groups)

    @property
    def has_issues(self) -> bool:
        return bool(self.duplicate_groups) or self.malformed_lines > 0 or self.records_without_id > 0


def check_consistency(jsonl_path: Path) -> ConsistencyReport:
    """Check JSONL file for data consistency issues.

    Checks for:
    - Duplicate AIAAIC IDs
    - Malformed JSON lines
    - Records without IDs

    Returns a ConsistencyReport with detailed findings.
    """
    if not jsonl_path.exists():
        return ConsistencyReport(
            total_records=0,
            unique_ids=0,
            duplicate_groups=[],
            malformed_lines=0,
            records_without_id=0,
        )

    # Group records by ID
    records_by_id: dict[str, list[AIAAICIncident]] = {}
    malformed = 0
    no_id = 0
    total = 0

    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            total += 1
            try:
                data = json.loads(line)
                aiaaic_id = data.get("aiaaic_id")
                if not aiaaic_id:
                    no_id += 1
                    continue
                incident = AIAAICIncident.model_validate(data)
                records_by_id.setdefault(aiaaic_id, []).append(incident)
            except json.JSONDecodeError:
                malformed += 1
            except ValueError:
                malformed += 1

    # Find duplicates and determine best record for each
    duplicate_groups = []
    for aiaaic_id, records in records_by_id.items():
        if len(records) > 1:
            # Sort by scraped_at (newest first), then by data quality
            def score(r: AIAAICIncident) -> tuple:
                """Score record by quality (higher = better)."""
                quality = 0
                if r.description:
                    quality += len(r.description)
                if r.source_links:
                    quality += len(r.source_links) * 100
                if r.page_scraped:
                    quality += 1000
                return (r.scraped_at or datetime.min, quality)

            sorted_records = sorted(records, key=score, reverse=True)
            best = sorted_records[0]

            duplicate_groups.append(DuplicateGroup(
                aiaaic_id=aiaaic_id,
                records=records,
                best_record=best,
                removed_count=len(records) - 1,
            ))

    return ConsistencyReport(
        total_records=total,
        unique_ids=len(records_by_id),
        duplicate_groups=duplicate_groups,
        malformed_lines=malformed,
        records_without_id=no_id,
    )


def deduplicate_jsonl(jsonl_path: Path, dry_run: bool = False) -> tuple[int, int]:
    """Remove duplicate records from JSONL file, keeping the best version.

    For each duplicate ID, keeps the record with:
    1. Most recent scraped_at timestamp
    2. Highest data quality (longer description, more sources)

    Args:
        jsonl_path: Path to the JSONL file
        dry_run: If True, report what would be removed without modifying file

    Returns:
        Tuple of (records_kept, records_removed)
    """
    if not jsonl_path.exists():
        return 0, 0

    # Load all records grouped by ID
    records_by_id: dict[str, list[tuple[str, AIAAICIncident]]] = {}  # id -> [(line, incident)]
    malformed_lines: list[str] = []

    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                aiaaic_id = data.get("aiaaic_id")
                if not aiaaic_id:
                    malformed_lines.append(line)
                    continue
                incident = AIAAICIncident.model_validate(data)
                records_by_id.setdefault(aiaaic_id, []).append((line, incident))
            except (json.JSONDecodeError, ValueError):
                malformed_lines.append(line)

    # Select best record for each ID
    lines_to_keep: list[str] = []
    removed_count = 0

    for aiaaic_id, records in records_by_id.items():
        if len(records) == 1:
            lines_to_keep.append(records[0][0])
        else:
            # Score and sort
            def score(r: tuple[str, AIAAICIncident]) -> tuple:
                inc = r[1]
                quality = 0
                if inc.description:
                    quality += len(inc.description)
                if inc.source_links:
                    quality += len(inc.source_links) * 100
                if inc.page_scraped:
                    quality += 1000
                return (inc.scraped_at or datetime.min, quality)

            sorted_records = sorted(records, key=score, reverse=True)
            lines_to_keep.append(sorted_records[0][0])
            removed_count += len(records) - 1

    # Keep malformed lines (don't lose data)
    lines_to_keep.extend(malformed_lines)

    if not dry_run:
        with open(jsonl_path, "w", encoding="utf-8") as f:
            for line in lines_to_keep:
                f.write(line + "\n")

    return len(lines_to_keep), removed_count


