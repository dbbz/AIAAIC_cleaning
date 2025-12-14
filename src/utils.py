"""File I/O utilities for the AIAAIC scraper."""

import json
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
