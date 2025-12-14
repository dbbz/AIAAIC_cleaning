"""AIAAIC Data Quality Inspector v2 - Actionable data quality insights."""

from collections import Counter
from pathlib import Path

import httpx
import pandas as pd
import streamlit as st
from bs4 import BeautifulSoup
from markdownify import markdownify as md
from rapidfuzz import fuzz

from src.utils import load_errors, load_incidents, check_consistency, deduplicate_jsonl

# === PAGE CONFIG (must be first Streamlit command) ===
st.set_page_config(page_title="AIAAIC Inspector", page_icon="🔍", layout="wide")

# === CONSTANTS ===

DATA_PATH = Path("data/aiaaic_incidents.jsonl")
ERRORS_PATH = Path("data/errors.jsonl")
CACHE_TTL = 300  # 5 minutes
DEFAULT_PAGE_SIZE = 50
DEFAULT_SIMILARITY_THRESHOLD = 85

LIST_FIELDS = ["countries", "sectors", "deployers", "developers", "system_names",
               "technologies", "purposes", "news_triggers", "issues"]

KNOWN_TYPOS = {
    "Accuracy/reliabiity": "Accuracy/reliability",
    "Accuracy/reliablity": "Accuracy/reliability",
    "Accountabiity": "Accountability",
    "Compeititon/monopolisation": "Competition/monopolisation",
    "Surveillanc": "Surveillance",
    "Privacy/surveillance/surveillance": "Privacy/surveillance",
}


# === DATA LOADING ===

@st.cache_data(ttl=CACHE_TTL)
def load_data():
    """Load incidents with cache TTL."""
    if not DATA_PATH.exists():
        return pd.DataFrame()
    incidents = list(load_incidents(DATA_PATH))
    return pd.DataFrame([i.model_dump(mode="json") for i in incidents])


@st.cache_data(ttl=CACHE_TTL)
def load_errs():
    """Load scraping errors."""
    if not ERRORS_PATH.exists():
        return []
    return [e.model_dump(mode="json") for e in load_errors(ERRORS_PATH)]


# === HELPERS ===

def empty(v):
    """Check if a value is empty."""
    return v is None or (isinstance(v, (list, str)) and len(v) == 0)


def has_description(df):
    """Return boolean mask for rows with non-empty description."""
    return ~df["description"].apply(empty)


def has_sources(df):
    """Return boolean mask for rows with non-empty source_links."""
    return df["source_links"].apply(lambda x: not empty(x) and len(x) > 0)


def is_complete(df):
    """Return boolean mask for rows that are 'complete' (have desc and sources)."""
    return has_description(df) & has_sources(df)


def is_incomplete(df):
    """Return boolean mask for rows that are 'incomplete' (missing desc or sources)."""
    return ~has_description(df) | ~has_sources(df)


def has_typos(df):
    """Return boolean mask for rows with known typos in issues field."""
    typo_set = set(KNOWN_TYPOS.keys())
    return df["issues"].apply(
        lambda x: bool(set(x) & typo_set) if isinstance(x, list) else False
    )


def join_list(v, sep=", "):
    """Join list values into a string."""
    return sep.join(v) if isinstance(v, list) and v else ""


@st.cache_data(ttl=CACHE_TTL)
def compute_metrics(_df):
    """Pre-compute all quality metrics.

    Note: _df prefix tells Streamlit to skip hashing this parameter.
    DataFrames with list columns (countries, sectors, etc.) are unhashable.
    """
    df = _df
    total = len(df)
    if total == 0:
        return {}

    scraped = df["page_scraped"].sum()
    with_desc = has_description(df).sum()
    with_sources = has_sources(df).sum()

    id_counts = df["aiaaic_id"].value_counts()
    duplicates = id_counts[id_counts > 1].index.tolist()

    # Vectorized typo detection
    typo_set = set(KNOWN_TYPOS.keys())
    has_typo = df["issues"].apply(
        lambda x: bool(set(x) & typo_set) if isinstance(x, list) else False
    )
    typo_records = df.loc[has_typo, "aiaaic_id"].tolist()

    field_completeness = {}
    for field in ["description", "source_links", "developers", "deployers",
                  "system_names", "occurred", "countries", "technologies"]:
        if field == "source_links":
            filled = has_sources(df).sum()
        elif field == "description":
            filled = has_description(df).sum()
        else:
            filled = (~df[field].apply(empty)).sum()
        field_completeness[field] = round(filled / total * 100, 1)

    return {
        "total": total,
        "scraped": scraped,
        "scraped_pct": round(scraped / total * 100, 1),
        "with_desc": with_desc,
        "with_desc_pct": round(with_desc / total * 100, 1),
        "with_sources": with_sources,
        "with_sources_pct": round(with_sources / total * 100, 1),
        "duplicates": duplicates,
        "duplicate_count": len(duplicates),
        "typo_ids": typo_records,
        "typo_count": len(typo_records),
        "field_completeness": field_completeness,
        "no_date": (df["occurred"].apply(empty)).sum(),
    }


def get_records_with_value(df, field, value):
    """Get all records that have a specific value in a list field."""
    mask = df[field].apply(lambda x: value in x if isinstance(x, list) else False)
    return df[mask]


def paginate_df(df, key_prefix, page_size=DEFAULT_PAGE_SIZE):
    """Add pagination controls and return current page of dataframe."""
    total = len(df)
    if total == 0:
        return df, 0, 0

    total_pages = (total - 1) // page_size + 1
    page_key = f"page_{key_prefix}"

    if page_key not in st.session_state:
        st.session_state[page_key] = 0

    col1, col2, col3 = st.columns([1, 3, 1])
    with col1:
        if st.button("← Prev", disabled=st.session_state[page_key] == 0, key=f"prev_{key_prefix}"):
            st.session_state[page_key] -= 1
            st.rerun()
    with col2:
        st.markdown(f"**Page {st.session_state[page_key] + 1} of {total_pages}** ({total} total)")
    with col3:
        if st.button("Next →", disabled=st.session_state[page_key] >= total_pages - 1, key=f"next_{key_prefix}"):
            st.session_state[page_key] += 1
            st.rerun()

    start = st.session_state[page_key] * page_size
    end = min(start + page_size, total)
    return df.iloc[start:end], start, end


@st.cache_data(ttl=CACHE_TTL)
def fetch_page_content(url: str) -> str | None:
    """Fetch and convert page content to markdown."""
    try:
        resp = httpx.get(url, follow_redirects=True, timeout=10)
        soup = BeautifulSoup(resp.text, "lxml")
        sections = soup.find_all("section")
        if sections:
            html = "\n".join(str(s) for s in sections)
        else:
            # Fallback: get body
            body = soup.find("body")
            html = str(body) if body else None
        if not html:
            return None
        return md(html, heading_style="ATX", strip=["script", "style"])
    except Exception:
        return None


# === PAGE FUNCTIONS ===

def page_dashboard():
    """Dashboard - quality overview with clickable issues."""
    df = get_data()
    metrics = get_metrics()
    errors = get_errors()

    st.header("Data Quality Dashboard")

    # Top metrics
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Incidents", metrics["total"])
    c2.metric("Scraped", f"{metrics['scraped_pct']}%", f"{metrics['scraped']}/{metrics['total']}")
    c3.metric("With Description", f"{metrics['with_desc_pct']}%", f"{metrics['with_desc']}/{metrics['total']}")
    c4.metric("With Sources", f"{metrics['with_sources_pct']}%", f"{metrics['with_sources']}/{metrics['total']}")

    st.divider()

    # Critical issues
    st.subheader("Critical Issues")
    issue_cols = st.columns(4)

    with issue_cols[0]:
        st.metric("Typos in issues", metrics["typo_count"])
        if metrics["typo_count"] > 0:
            st.caption("See Values page for details")

    with issue_cols[1]:
        st.metric("Duplicate IDs", metrics["duplicate_count"])
        if metrics["duplicate_count"] > 0:
            st.caption(f"IDs: {', '.join(metrics['duplicates'][:3])}")

    with issue_cols[2]:
        st.metric("Missing date", metrics["no_date"])

    with issue_cols[3]:
        st.metric("Scraping errors", len(errors))

    st.divider()

    # Field completeness
    st.subheader("Field Completeness")
    completeness = metrics["field_completeness"]
    sorted_fields = sorted(completeness.items(), key=lambda x: x[1], reverse=True)

    for field, pct in sorted_fields:
        col1, col2, col3 = st.columns([2, 6, 1])
        with col1:
            st.markdown(f"**{field}**")
        with col2:
            st.progress(pct / 100)
        with col3:
            st.markdown(f"{pct}%")


def page_browse():
    """Browse - searchable table with pagination."""
    df = get_data()
    metrics = get_metrics()

    st.header("Browse Incidents")

    # Search and filters
    col1, col2 = st.columns([3, 1])
    with col1:
        search = st.text_input("Search", placeholder="Search headline, ID, country, developer...")
    with col2:
        status = st.selectbox("Filter", ["all", "complete", "incomplete", "duplicates"])

    # Apply search
    view = df.copy()
    if search:
        search_lower = search.lower()
        mask = (
            view["headline"].str.lower().str.contains(search_lower, na=False) |
            view["aiaaic_id"].str.lower().str.contains(search_lower, na=False) |
            view["countries"].apply(lambda x: any(search_lower in c.lower() for c in x) if isinstance(x, list) else False) |
            view["developers"].apply(lambda x: any(search_lower in d.lower() for d in x) if isinstance(x, list) else False)
        )
        view = view[mask]

    # Apply filter
    if status == "complete":
        view = view[is_complete(view)]
    elif status == "incomplete":
        view = view[is_incomplete(view)]
    elif status == "duplicates":
        view = view[view["aiaaic_id"].isin(metrics["duplicates"])]

    view = view.sort_values("occurred", ascending=False).reset_index(drop=True)

    # Display table
    display_df = view[["aiaaic_id", "headline", "occurred"]].copy()
    display_df["desc"] = has_description(view)
    display_df["sources"] = view["source_links"].apply(lambda x: len(x) if isinstance(x, list) else 0)
    display_df.columns = ["ID", "Headline", "Date", "Has Desc", "Sources"]

    paged_df, start, _ = paginate_df(display_df, "browse", page_size=DEFAULT_PAGE_SIZE)

    event = st.dataframe(
        paged_df,
        hide_index=True,
        width="stretch",
        height=400,
        on_select="rerun",
        selection_mode="single-row",
    )

    # Detail panel
    if event and event.selection and event.selection.rows:
        selected_idx = start + event.selection.rows[0]
        row = view.iloc[selected_idx]

        st.divider()
        st.subheader(row["headline"])

        c1, c2, c3, c4 = st.columns(4)
        c1.markdown(f"**ID:** {row['aiaaic_id']}")
        c2.markdown(f"**Date:** {row['occurred'] or 'Unknown'}")
        c3.markdown(f"**Scraped:** {'Yes' if row['page_scraped'] else 'No'}")
        if row.get("detail_page_url"):
            c4.link_button("Open page", row["detail_page_url"])

        left, right = st.columns(2)
        with left:
            st.markdown("**Description:**")
            if empty(row["description"]):
                st.warning("No description")
            else:
                st.markdown(row["description"])

        with right:
            st.markdown("**Metadata:**")
            for field, icon in [("countries", "🌍"), ("technologies", "🔧"),
                                ("developers", "👨‍💻"), ("deployers", "🏢"),
                                ("sectors", "📊"), ("issues", "⚠️")]:
                if row[field]:
                    st.markdown(f"{icon} {join_list(row[field])}")

            st.markdown("**Source Links:**")
            sources = row.get("source_links", [])
            if sources:
                for s in sources:
                    if isinstance(s, dict):
                        url = s.get("url", "")
                        title = s.get("title") or url[:60]
                        st.markdown(f"- [{title}]({url})")
            else:
                st.warning("No source links")


def page_values():
    """Values - field consistency with actionable findings."""
    df = get_data()

    st.header("Value Consistency")

    # Controls
    col1, col2 = st.columns([1, 3])
    with col1:
        field = st.selectbox("Field", LIST_FIELDS, index=LIST_FIELDS.index("issues"))
        thresh = st.slider("Similarity", 70, 99, DEFAULT_SIMILARITY_THRESHOLD)

    # Gather values
    values = []
    for v in df[field].dropna():
        if isinstance(v, list):
            values.extend(v)

    if not values:
        st.warning("No values found")
        return

    unique = list(set(values))
    counts = Counter(values)
    singletons = [v for v, c in counts.items() if c == 1]

    # Metrics
    with col2:
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total uses", len(values))
        m2.metric("Unique values", len(unique))
        m3.metric("Avg per incident", f"{len(values)/len(df):.1f}")
        m4.metric("Singletons", len(singletons))

    # Show known typos if in issues field
    if field == "issues" and any(t in unique for t in KNOWN_TYPOS):
        st.error("**Known Typos Detected:**")
        typo_cols = st.columns(3)
        col_idx = 0
        for typo, correction in KNOWN_TYPOS.items():
            if typo in unique:
                with typo_cols[col_idx % 3]:
                    st.code(f"{typo} → {correction}")
                    affected = get_records_with_value(df, field, typo)
                    st.caption(f"{len(affected)} records affected")
                col_idx += 1

    # Two columns: frequency and issues
    left, right = st.columns(2)

    with left:
        st.subheader("Value Frequency")
        freq = pd.DataFrame(counts.most_common(), columns=["Value", "Count"])
        csv = freq.to_csv(index=False)
        st.download_button("Export CSV", csv, f"{field}_frequency.csv", "text/csv")
        st.dataframe(freq, hide_index=True, height=350, width="stretch")

    with right:
        st.subheader("Potential Issues")

        # Similar values
        similar = []
        for i, v1 in enumerate(unique):
            for v2 in unique[i+1:]:
                score = fuzz.ratio(v1.lower(), v2.lower())
                if thresh <= score < 100:
                    similar.append((v1, v2, score, counts[v1], counts[v2]))
        similar.sort(key=lambda x: -x[2])

        if similar:
            st.markdown(f"**Similar values ({len(similar)})**")
            sim_df = pd.DataFrame(similar[:20], columns=["Value A", "Value B", "%", "Count A", "Count B"])
            st.dataframe(sim_df, hide_index=True, height=200)
        else:
            st.success("No similar values found")

        # Case variations
        lower_map = {}
        for v in unique:
            lower_map.setdefault(v.lower(), []).append(v)
        case_vars = [(k, vs) for k, vs in lower_map.items() if len(vs) > 1]

        if case_vars:
            st.markdown(f"**Case variations ({len(case_vars)})**")
            for _, variants in case_vars[:10]:
                st.markdown(f"- {' / '.join(variants)}")

    # Singletons expander
    if singletons:
        with st.expander(f"View {len(singletons)} singleton values"):
            st.dataframe(pd.DataFrame({"Value": singletons}), hide_index=True, height=300)


def show_record_detail(row):
    """Show detail panel for a selected record."""
    st.divider()
    st.subheader(row["headline"])

    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(f"**ID:** {row['aiaaic_id']}")
    c2.markdown(f"**Date:** {row['occurred'] or 'Unknown'}")
    c3.markdown(f"**Scraped:** {'✅' if row['page_scraped'] else '⏳'}")
    if row.get("detail_page_url"):
        c4.link_button("Open page", row["detail_page_url"])

    left, right = st.columns(2)
    with left:
        st.markdown("**Description:**")
        if empty(row["description"]):
            st.warning("No description")
        else:
            st.markdown(row["description"])

    with right:
        st.markdown("**Metadata:**")
        for field, icon in [("countries", "🌍"), ("technologies", "🔧"),
                            ("developers", "👨‍💻"), ("deployers", "🏢"),
                            ("sectors", "📊")]:
            val = row.get(field, [])
            if val:
                st.markdown(f"{icon} {join_list(val)}")

        # Highlight typos in issues
        issues = row.get("issues", [])
        if issues:
            display_issues = []
            for issue in issues:
                if issue in KNOWN_TYPOS:
                    display_issues.append(f"~~{issue}~~ → {KNOWN_TYPOS[issue]}")
                else:
                    display_issues.append(issue)
            st.markdown(f"⚠️ {', '.join(display_issues)}")

        st.markdown("**Source Links:**")
        sources = row.get("source_links", [])
        if sources:
            for s in sources:
                if isinstance(s, dict):
                    url = s.get("url", "")
                    title = s.get("title") or url[:60]
                    st.markdown(f"- [{title}]({url})")
        else:
            st.warning("No source links")


def show_gap_table(data_df, key_prefix):
    """Show a gap table with clickable rows and detail panel."""
    display = data_df[["aiaaic_id", "headline", "occurred"]].reset_index(drop=True)
    display.columns = ["ID", "Headline", "Date"]

    event = st.dataframe(
        display,
        hide_index=True,
        width="stretch",
        height=300,
        on_select="rerun",
        selection_mode="single-row",
        key=f"gaps_{key_prefix}_table",
    )

    if event and event.selection and event.selection.rows:
        selected_idx = event.selection.rows[0]
        row = data_df.iloc[selected_idx]
        show_record_detail(row)


def page_gaps():
    """Gaps - missing data with drill-down."""
    df = get_data()
    errors = get_errors()

    st.header("Data Gaps")

    # Compute gap categories
    no_url = df[df["detail_page_url"].apply(empty)].reset_index(drop=True)
    no_desc = df[(df["page_scraped"]) & (~has_description(df))].reset_index(drop=True)
    no_src = df[~has_sources(df)].reset_index(drop=True)
    no_date = df[df["occurred"].apply(empty)].reset_index(drop=True)
    not_scraped = df[~df["page_scraped"] & ~df["detail_page_url"].apply(empty)].reset_index(drop=True)
    with_typos = df[has_typos(df)].reset_index(drop=True)

    # Summary metrics
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("No URL", len(no_url))
    c2.metric("No description", len(no_desc))
    c3.metric("No sources", len(no_src))
    c4.metric("No date", len(no_date))
    c5.metric("Has typos", len(with_typos))
    c6.metric("Errors", len(errors))

    st.divider()

    # Tabs for each category
    tabs = st.tabs(["No description", "No sources", "No date", "Has typos", "Errors", "No URL", "Not scraped"])

    with tabs[0]:
        if no_desc.empty:
            st.success("All scraped incidents have descriptions")
        else:
            st.caption(f"{len(no_desc)} records - click to view details")
            show_gap_table(no_desc, "desc")

    with tabs[1]:
        if no_src.empty:
            st.success("All incidents have source links")
        else:
            st.caption(f"{len(no_src)} records - click to view details")
            show_gap_table(no_src, "src")

    with tabs[2]:
        if no_date.empty:
            st.success("All incidents have dates")
        else:
            st.caption(f"{len(no_date)} records - click to view details")
            show_gap_table(no_date, "date")

    with tabs[3]:
        if with_typos.empty:
            st.success("No known typos detected")
        else:
            st.caption(f"{len(with_typos)} records - click to view details")
            show_gap_table(with_typos, "typos")

    with tabs[4]:
        if not errors:
            st.success("No scraping errors")
        else:
            error_types = {}
            for e in errors:
                etype = e.get("error_type", "Unknown")
                error_types.setdefault(etype, []).append(e)

            for etype, errs in sorted(error_types.items(), key=lambda x: -len(x[1])):
                with st.expander(f"{etype} ({len(errs)} errors)"):
                    for e in errs[:20]:
                        st.markdown(f"**{e.get('aiaaic_id', 'Unknown')}**")
                        st.code(e.get("error_message", "No message")[:200])

    with tabs[5]:
        if no_url.empty:
            st.success("All incidents have URLs")
        else:
            st.caption(f"{len(no_url)} records - click to view details")
            show_gap_table(no_url, "url")

    with tabs[6]:
        if not_scraped.empty:
            st.success("All incidents with URLs have been scraped")
        else:
            st.caption(f"{len(not_scraped)} records - click to view details")
            show_gap_table(not_scraped, "not_scraped")


def page_inspect():
    """Inspect - side-by-side comparison of scraped data vs original page."""
    df = get_data()

    st.header("Scrape Inspector")

    # Filter options with descriptions
    filter_options = {
        "all": "All records",
        "missing description": "Missing description",
        "missing sources": "Missing sources",
        "missing date": "Missing date",
        "no URL": "No detail page URL",
        "not scraped": "Not yet scraped",
        "has typos": "Has known typos",
        "complete": "Complete records",
    }

    # Filter controls
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        filter_opt = st.selectbox(
            "Filter by issue",
            options=list(filter_options.keys()),
            format_func=lambda x: filter_options[x],
        )
    with col2:
        id_input = st.text_input("Jump to ID", placeholder="e.g. AIAAIC0001", label_visibility="visible")

    # Apply filter
    if filter_opt == "missing description":
        filtered = df[~has_description(df)]
    elif filter_opt == "missing sources":
        filtered = df[~has_sources(df)]
    elif filter_opt == "missing date":
        filtered = df[df["occurred"].apply(empty)]
    elif filter_opt == "no URL":
        filtered = df[df["detail_page_url"].apply(empty)]
    elif filter_opt == "not scraped":
        filtered = df[~df["page_scraped"] & ~df["detail_page_url"].apply(empty)]
    elif filter_opt == "has typos":
        filtered = df[has_typos(df)]
    elif filter_opt == "complete":
        filtered = df[is_complete(df)]
    else:  # all
        filtered = df

    filtered = filtered.reset_index(drop=True)

    with col3:
        st.metric("Matching", len(filtered))

    # Handle ID lookup
    if id_input:
        id_input = id_input.strip().upper()
        # Check if ID exists in filtered data
        matches = filtered[filtered["aiaaic_id"].str.upper() == id_input]
        if not matches.empty:
            st.session_state.inspect_idx = matches.index[0]
        else:
            # Check if ID exists in full data
            all_matches = df[df["aiaaic_id"].str.upper() == id_input]
            if not all_matches.empty:
                st.warning(f"ID '{id_input}' exists but doesn't match current filter. Switch to 'All records' to view it.")
            else:
                st.error(f"ID '{id_input}' not found")

    if filtered.empty:
        st.success("No records match this filter")
        return

    st.divider()

    # Navigation
    if "inspect_idx" not in st.session_state:
        st.session_state.inspect_idx = 0
    if st.session_state.inspect_idx >= len(filtered):
        st.session_state.inspect_idx = 0

    nav1, nav2, nav3, nav4, nav5 = st.columns([1, 1, 2, 2, 1])
    with nav1:
        if st.button("⏮ First", use_container_width=True):
            st.session_state.inspect_idx = 0
            st.rerun()
    with nav2:
        if st.button("◀ Prev", use_container_width=True, disabled=st.session_state.inspect_idx == 0):
            st.session_state.inspect_idx -= 1
            st.rerun()
    with nav3:
        st.markdown(f"### {st.session_state.inspect_idx + 1} / {len(filtered)}")
    with nav4:
        jump = st.number_input("Go to", 1, len(filtered), st.session_state.inspect_idx + 1, label_visibility="collapsed")
        if jump - 1 != st.session_state.inspect_idx:
            st.session_state.inspect_idx = jump - 1
            st.rerun()
    with nav5:
        if st.button("Next ▶", use_container_width=True, disabled=st.session_state.inspect_idx >= len(filtered) - 1):
            st.session_state.inspect_idx += 1
            st.rerun()

    # Get current record
    row = filtered.iloc[st.session_state.inspect_idx]

    # Header
    st.divider()
    st.subheader(row["headline"])
    st.caption(f"**{row['aiaaic_id']}** | {row['occurred'] or 'Unknown date'}")

    # Quality indicators
    row_has_desc = not empty(row["description"])
    sources = row.get("source_links", [])
    row_has_sources = bool(sources) and len(sources) > 0
    row_has_url = not empty(row.get("detail_page_url"))

    ind1, ind2, ind3, ind4 = st.columns(4)
    ind1.markdown(f"**Description:** {'✅' if row_has_desc else '❌'}")
    ind2.markdown(f"**Sources:** {'✅ ' + str(len(sources)) if row_has_sources else '❌'}")
    ind3.markdown(f"**Scraped:** {'✅' if row['page_scraped'] else '⏳'}")
    ind4.markdown(f"**URL:** {'✅' if row_has_url else '❌'}")

    st.divider()

    # Two columns: scraped data | original page
    left, right = st.columns(2)

    with left:
        st.markdown("### Scraped Data")

        st.markdown("**Description:**")
        if row_has_desc:
            st.markdown(row["description"])
        else:
            st.warning("No description extracted")

        st.markdown("---")

        st.markdown("**Source Links:**")
        if row_has_sources:
            for i, s in enumerate(sources, 1):
                if isinstance(s, dict):
                    url = s.get("url", "")
                    title = s.get("title") or f"Source {i}"
                    st.markdown(f"{i}. [{title}]({url})")
        else:
            st.warning("No source links found")

        st.markdown("---")

        st.markdown("**Metadata:**")
        for field, icon in [("countries", "🌍"), ("technologies", "🔧"),
                            ("developers", "👨‍💻"), ("deployers", "🏢"),
                            ("sectors", "📊")]:
            val = row.get(field, [])
            if val:
                st.markdown(f"{icon} **{field}:** {join_list(val)}")

        # Special handling for issues field - highlight typos
        issues = row.get("issues", [])
        if issues:
            display_issues = []
            for issue in issues:
                if issue in KNOWN_TYPOS:
                    display_issues.append(f"~~{issue}~~ → {KNOWN_TYPOS[issue]}")
                else:
                    display_issues.append(issue)
            st.markdown(f"⚠️ **issues:** {', '.join(display_issues)}")

    with right:
        st.markdown("### Original Page")
        url = row.get("detail_page_url")
        if url:
            st.link_button("Open in new tab", url, use_container_width=True)

            with st.spinner("Loading page content..."):
                content = fetch_page_content(url)

            if content:
                with st.container(height=500):
                    st.markdown(content)
            else:
                st.error("Could not fetch page content")
        else:
            st.info("No detail page URL for this record")


def page_consistency():
    """Data Consistency - check and fix duplicate records."""
    st.header("Data Consistency")

    st.markdown("""
    This page helps you identify and fix data consistency issues:
    - **Duplicate records** - Same AIAAIC ID appearing multiple times
    - **Malformed records** - Invalid JSON or missing required fields
    - **Version conflicts** - Multiple versions of the same incident
    """)

    st.divider()

    # Run consistency check
    report = check_consistency(DATA_PATH)

    # Summary metrics
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Records", report.total_records)
    c2.metric("Unique IDs", report.unique_ids)
    c3.metric("Duplicate Groups", len(report.duplicate_groups),
              delta=f"-{report.total_duplicates}" if report.total_duplicates else None,
              delta_color="inverse")
    c4.metric("Issues", report.malformed_lines + report.records_without_id,
              delta="needs attention" if report.malformed_lines + report.records_without_id > 0 else None,
              delta_color="inverse")

    st.divider()

    if not report.has_issues:
        st.success("No consistency issues found! Your data is clean.")
        return

    # Duplicates section
    if report.duplicate_groups:
        st.subheader(f"Duplicate Records ({len(report.duplicate_groups)} groups)")

        st.markdown("""
        These AIAAIC IDs appear multiple times in the data. This can happen during re-scraping operations.
        The **best version** is determined by: most recent scrape date + highest data quality (longer description, more sources).
        """)

        # Build comparison table
        dup_data = []
        for group in report.duplicate_groups:
            best = group.best_record
            for i, record in enumerate(group.records):
                is_best = record == best
                dup_data.append({
                    "ID": group.aiaaic_id,
                    "Version": i + 1,
                    "Best": "✅" if is_best else "",
                    "Scraped": record.scraped_at.strftime("%Y-%m-%d %H:%M") if record.scraped_at else "?",
                    "Has Desc": "✅" if record.description else "❌",
                    "Desc Length": len(record.description) if record.description else 0,
                    "Sources": len(record.source_links) if record.source_links else 0,
                    "Page Scraped": "✅" if record.page_scraped else "❌",
                })

        dup_df = pd.DataFrame(dup_data)
        st.dataframe(dup_df, hide_index=True, height=400, use_container_width=True)

        # Deduplication action
        st.divider()
        st.subheader("Fix Duplicates")

        col1, col2 = st.columns([3, 1])
        with col1:
            st.markdown(f"""
            **Deduplication will:**
            - Remove **{report.total_duplicates}** duplicate records
            - Keep the **best version** of each incident (newest + most complete)
            - Preserve all unique incidents ({report.unique_ids} total)
            """)

        with col2:
            if st.button("Deduplicate Now", type="primary", use_container_width=True):
                with st.spinner("Removing duplicates..."):
                    kept, removed = deduplicate_jsonl(DATA_PATH)
                    # Clear cache to reload data
                    load_data.clear()
                    load_errs.clear()
                st.success(f"Done! Kept {kept} records, removed {removed} duplicates")
                st.rerun()

    # Other issues
    if report.malformed_lines > 0 or report.records_without_id > 0:
        st.divider()
        st.subheader("Other Issues")

        if report.malformed_lines > 0:
            st.error(f"**{report.malformed_lines}** malformed JSON lines found")
            st.caption("These lines could not be parsed and may indicate data corruption.")

        if report.records_without_id > 0:
            st.error(f"**{report.records_without_id}** records without AIAAIC ID")
            st.caption("These records are missing the required identifier field.")


# === DATA ACCESS HELPERS ===
# These wrap cached functions to provide a clean interface for page functions


def get_data():
    """Get incident data (cached)."""
    return load_data()


def get_errors():
    """Get scraping errors (cached)."""
    return load_errs()


def get_metrics():
    """Get computed metrics (cached)."""
    df = get_data()
    return compute_metrics(df) if not df.empty else {}


# === MAIN APP ===

# Check for data availability
df = get_data()
if df.empty:
    st.error("No data. Run: `uv run scrape.py`")
    st.stop()

metrics = get_metrics()

# Define pages
dashboard = st.Page(page_dashboard, title="Dashboard", icon="📊", default=True)
browse = st.Page(page_browse, title="Browse", icon="📋")
values = st.Page(page_values, title="Values", icon="🔤")
gaps = st.Page(page_gaps, title="Gaps", icon="⚠️")
inspect = st.Page(page_inspect, title="Inspect", icon="🔍")
consistency = st.Page(page_consistency, title="Consistency", icon="🔧")

# Navigation
pg = st.navigation({
    "Overview": [dashboard],
    "Data": [browse, values],
    "Quality": [gaps, inspect, consistency],
})

# Sidebar info
with st.sidebar:
    st.divider()
    st.caption("Quality Score")
    avg_completeness = sum(metrics["field_completeness"].values()) / len(metrics["field_completeness"])
    st.progress(avg_completeness / 100)
    st.markdown(f"**{avg_completeness:.0f}%** complete")
    st.caption(f"{metrics['total']} incidents")

# Run the selected page
pg.run()
