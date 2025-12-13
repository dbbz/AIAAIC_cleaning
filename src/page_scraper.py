"""Page scraper for AIAAIC incident detail pages.

Uses robust text-pattern extraction instead of CSS class selectors,
since Google Sites uses auto-generated class names that can change.
"""

import re
from dataclasses import dataclass
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from .models import RelatedIncident, SourceLink

# Boilerplate text patterns to filter out
BOILERPLATE_PATTERNS = [
    "Report incident",
    "Improve page",
    "Access database",
    "Page updated",
    "Google Sites",
    "Skip to main",
    "Skip to navigation",
    "Copy heading link",
    "Back to site",
    "Search this site",
]

# Base URL for resolving relative links
BASE_URL = "https://www.aiaaic.org"


@dataclass
class PageData:
    """Data extracted from an AIAAIC incident page."""

    description: str | None = None
    source_links: list[SourceLink] | None = None
    related_incidents: list[RelatedIncident] | None = None
    page_published: str | None = None
    occurred_from_page: str | None = None


def is_boilerplate(text: str) -> bool:
    """Check if text is boilerplate/navigation content."""
    if len(text) < 30:
        return True
    text_lower = text.lower()
    return any(bp.lower() in text_lower for bp in BOILERPLATE_PATTERNS)


def extract_metadata_from_text(text: str) -> tuple[str | None, str | None]:
    """Extract occurred date and page published from page text using regex.

    Returns:
        Tuple of (occurred, page_published)
    """
    occurred = None
    page_published = None

    # Try to find "Occurred: <date>"
    occurred_match = re.search(
        r"Occurred:\s*([A-Za-z0-9\s,]+?)(?:\s*Page published|\s*$|\n)",
        text,
        re.IGNORECASE,
    )
    if occurred_match:
        occurred = occurred_match.group(1).strip()

    # Try to find "Page published: <date>"
    published_match = re.search(
        r"Page published:\s*([A-Za-z0-9\s,]+?)(?:\s*$|\n|Report)",
        text,
        re.IGNORECASE,
    )
    if published_match:
        page_published = published_match.group(1).strip()

    return occurred, page_published


def extract_text_urls(soup: BeautifulSoup) -> list[str]:
    """Extract URLs that appear as plain text (not in <a> tags).

    Many AIAAIC pages list source URLs as underlined text in <span> elements
    rather than as proper hyperlinks.
    """
    urls: list[str] = []

    # URL regex pattern
    url_pattern = re.compile(r'https?://[^\s<>"\'\\]+[^\s<>"\'\\.,;:!?\)\]\}]')

    # Find spans with underline styling (common pattern for text URLs)
    for span in soup.find_all("span", style=True):
        style = span.get("style", "")
        if "underline" in style:
            text = span.get_text(strip=True)
            if text.startswith("http"):
                urls.append(text)

    # Also look for URLs in list items that aren't wrapped in <a> tags
    for li in soup.find_all("li"):
        # Check if this li has no anchor children but contains a URL
        if not li.find("a"):
            text = li.get_text(strip=True)
            matches = url_pattern.findall(text)
            urls.extend(matches)

    # Look for URLs in paragraphs without links
    for p in soup.find_all("p"):
        if not p.find("a"):
            text = p.get_text(strip=True)
            matches = url_pattern.findall(text)
            urls.extend(matches)

    return urls


def extract_links(
    soup: BeautifulSoup, current_url: str
) -> tuple[list[SourceLink], list[RelatedIncident]]:
    """Extract source links and related incidents from page.

    The page structure is:
    - Section 0: Title
    - Section 1: Metadata
    - Section 2+: Content sections with description, news links, etc.
    - Section with "Related": Related incidents
    - Last section: Footer (skip)

    Uses URL patterns instead of CSS classes.
    Also extracts plain text URLs that aren't wrapped in <a> tags.
    """
    source_links: list[SourceLink] = []
    related_incidents: list[RelatedIncident] = []
    seen_source_urls: set[str] = set()
    seen_related_urls: set[str] = set()

    # Normalize current URL for comparison
    current_path = current_url.replace(BASE_URL, "").rstrip("/")

    # Get all sections
    sections = soup.find_all("section")

    # URLs to skip (not actual news sources)
    skip_patterns = [
        "facebook.com/sharer", "twitter.com/intent", "linkedin.com/share",
        "linktree", "gstatic.com", "google.com/url", "docs.google.com/forms",
        "docs.google.com/spreadsheets", "wikipedia.org", "doubao.com",
    ]

    # Process each section (except last which is footer)
    for section in sections[:-1] if len(sections) > 1 else sections:
        section_text = section.get_text(strip=True)

        # Check if this is the "Related" section
        is_related_section = section_text.lower().startswith("related")

        for link in section.find_all("a", href=True):
            href = link.get("href", "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue

            title = link.get_text(strip=True)

            # Handle external links (source links)
            if href.startswith("http") and "aiaaic.org" not in href:
                # Skip non-news links
                if any(pattern in href.lower() for pattern in skip_patterns):
                    continue

                if href in seen_source_urls:
                    continue
                seen_source_urls.add(href)

                # Clean up title
                if not title or is_boilerplate(title) or title.startswith("http"):
                    title = None

                source_links.append(SourceLink(url=href, title=title))

            # Handle related incidents (only in the Related section)
            elif is_related_section and "/aiaaic-repository/ai-algorithmic-and-automation-incidents/" in href:
                link_path = href.replace(BASE_URL, "").rstrip("/")
                if link_path == current_path:
                    continue  # Skip self-reference

                full_url = urljoin(BASE_URL, href) if href.startswith("/") else href
                if full_url in seen_related_urls:
                    continue
                seen_related_urls.add(full_url)

                if title and not is_boilerplate(title):
                    related_incidents.append(
                        RelatedIncident(title=title, url=full_url)
                    )

    # Also extract plain text URLs (not in <a> tags)
    text_urls = extract_text_urls(soup)
    for url in text_urls:
        # Skip if already seen or matches skip patterns
        if url in seen_source_urls:
            continue
        if any(pattern in url.lower() for pattern in skip_patterns):
            continue
        if "aiaaic.org" in url:
            continue

        seen_source_urls.add(url)
        source_links.append(SourceLink(url=url, title=None))

    return source_links, related_incidents


def extract_description(soup: BeautifulSoup) -> str | None:
    """Extract the main description from the page using multiple fallback strategies.

    Strategy order:
    1. Look for role="main" and extract substantial paragraphs
    2. Look for bold text that appears to be a summary
    3. Use og:description meta tag
    """
    # Strategy 1: Find main content area
    main = soup.find(attrs={"role": "main"})
    if main:
        paragraphs = main.find_all("p")
        for p in paragraphs:
            text = p.get_text(strip=True)
            # Look for substantial text that's not boilerplate
            if len(text) > 80 and not is_boilerplate(text):
                # Skip if it looks like a link list or metadata
                if not text.startswith("http") and "Occurred:" not in text:
                    return text

    # Strategy 2: Look for bold/strong text that might be a summary
    for bold in soup.find_all(["b", "strong"]):
        text = bold.get_text(strip=True)
        if len(text) > 50 and not is_boilerplate(text):
            # Check parent for more context
            parent = bold.parent
            if parent:
                parent_text = parent.get_text(strip=True)
                if len(parent_text) > len(text) and len(parent_text) < 1000:
                    return parent_text
            return text

    # Strategy 3: Look in section elements
    for section in soup.find_all("section"):
        paragraphs = section.find_all("p")
        for p in paragraphs:
            text = p.get_text(strip=True)
            if len(text) > 100 and not is_boilerplate(text):
                if not text.startswith("http") and "Occurred:" not in text:
                    return text

    # Strategy 4: Fallback to og:description meta tag
    og_desc = soup.find("meta", property="og:description")
    if og_desc:
        content = og_desc.get("content", "").strip()
        if content and len(content) > 20:
            return content

    # Strategy 5: Try itemprop description
    itemprop_desc = soup.find("meta", attrs={"itemprop": "description"})
    if itemprop_desc:
        content = itemprop_desc.get("content", "").strip()
        if content and len(content) > 20:
            return content

    return None


async def fetch_page(client: httpx.AsyncClient, url: str) -> str:
    """Fetch page HTML content."""
    response = await client.get(url, follow_redirects=True)
    response.raise_for_status()
    return response.text


def parse_page(html: str, url: str) -> PageData:
    """Parse HTML content and extract structured data."""
    soup = BeautifulSoup(html, "lxml")

    # Get full page text for regex extraction
    page_text = soup.get_text(separator="\n", strip=True)

    # Extract metadata using text patterns
    occurred, page_published = extract_metadata_from_text(page_text)

    # Extract links using URL patterns
    source_links, related_incidents = extract_links(soup, url)

    # Extract description using structural heuristics
    description = extract_description(soup)

    return PageData(
        description=description,
        source_links=source_links,
        related_incidents=related_incidents,
        page_published=page_published,
        occurred_from_page=occurred,
    )


async def scrape_page(client: httpx.AsyncClient, url: str) -> PageData:
    """Fetch and parse an AIAAIC incident page."""
    html = await fetch_page(client, url)
    return parse_page(html, url)
