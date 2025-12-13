"""Pydantic models for AIAAIC incident data."""

from datetime import datetime
from pydantic import BaseModel, Field


class ExternalHarms(BaseModel):
    """External harms caused by the incident."""
    individual: list[str] = Field(default_factory=list)
    societal: list[str] = Field(default_factory=list)
    environmental: list[str] = Field(default_factory=list)


class InternalImpacts(BaseModel):
    """Internal impacts on the organization."""
    strategic_reputational: list[str] = Field(default_factory=list)
    operational: list[str] = Field(default_factory=list)
    financial: list[str] = Field(default_factory=list)
    legal_regulatory: list[str] = Field(default_factory=list)


class SourceLink(BaseModel):
    """A source/reference link for the incident."""
    url: str
    title: str | None = None


class RelatedIncident(BaseModel):
    """A related incident reference."""
    title: str
    url: str


class AIAAICIncident(BaseModel):
    """Complete AIAAIC incident record."""

    # Identifiers
    aiaaic_id: str = Field(..., description="e.g., AIAAIC2155")

    # From CSV
    headline: str
    occurred: str = Field(..., description="Year or date string")
    countries: list[str] = Field(default_factory=list)
    sectors: list[str] = Field(default_factory=list)
    deployers: list[str] = Field(default_factory=list)
    developers: list[str] = Field(default_factory=list)
    system_names: list[str] = Field(default_factory=list)
    technologies: list[str] = Field(default_factory=list)
    purposes: list[str] = Field(default_factory=list)
    news_triggers: list[str] = Field(default_factory=list)
    issues: list[str] = Field(default_factory=list)
    external_harms: ExternalHarms = Field(default_factory=ExternalHarms)
    internal_impacts: InternalImpacts = Field(default_factory=InternalImpacts)
    detail_page_url: str | None = None

    # From detail page scraping
    description: str | None = None
    source_links: list[SourceLink] = Field(default_factory=list)
    related_incidents: list[RelatedIncident] = Field(default_factory=list)
    page_published: str | None = None
    page_scraped: bool = False

    # Metadata
    scraped_at: datetime = Field(default_factory=datetime.now)


class ScrapingError(BaseModel):
    """Record of a scraping error for retry purposes."""
    aiaaic_id: str
    url: str | None
    error_type: str
    error_message: str
    timestamp: datetime = Field(default_factory=datetime.now)
