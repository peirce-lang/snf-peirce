"""
guided_api.py — Fetch data from an API and query it

Connects to a supported data source, fetches records,
and drops you into a Peirce query shell. No coding required.

Requirements:
    pip install snf-peirce

Usage:
    python guided_api.py
    python guided_api.py scryfall
    python guided_api.py loc
    python guided_api.py discogs

Supported sources:
    scryfall    Magic: The Gathering cards
    loc         Library of Congress catalog
    discogs     Record collections (requires free API token)
"""

from __future__ import annotations

import sys
from guided_base import ask, confirm, banner, section, info, success, warn, error, query_loop


# ─────────────────────────────────────────────────────────────────────────────
# Supported APIs
# ─────────────────────────────────────────────────────────────────────────────

SOURCES = {
    "scryfall": {
        "label":       "Magic: The Gathering cards (Scryfall)",
        "description": "Search for cards by set, name, or type.",
        "needs_key":   False,
        "example":     'WHAT.card_type = "Creature" AND HOW.cmc = "3"',
    },
    "loc": {
        "label":       "Library of Congress catalog",
        "description": "Search books, manuscripts, and archival records.",
        "needs_key":   False,
        "example":     'WHO.author = "Twain" AND WHEN.year BETWEEN "1870" AND "1900"',
    },
    "discogs": {
        "label":       "Record collections (Discogs)",
        "description": "Search releases, artists, and labels.",
        "needs_key":   True,
        "key_name":    "Discogs personal access token",
        "key_help":    "Get one free at discogs.com → Settings → Developers",
        "example":     'WHO.artist = "Miles Davis" AND WHEN.year = "1959"',
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# Fetchers
# ─────────────────────────────────────────────────────────────────────────────

def fetch_scryfall(search_term: str):
    """Fetch Magic cards from Scryfall."""
    from snf_peirce.fetch_scryfall import ScryfallFetcher
    import pandas as pd

    info(f"Fetching from Scryfall: '{search_term}'...")

    # ScryfallFetcher uses set codes — if the term looks like a set code use it
    set_code = search_term.lower().strip()
    fetcher  = ScryfallFetcher(set_code)

    try:
        cards = fetcher.fetch()
        rows  = [fetcher.translate(card) for card in cards]

        # Flatten facts into a DataFrame
        flat = []
        for entity_facts in rows:
            for f in entity_facts:
                flat.append({
                    "entity_id":   f[0],
                    "dimension":   f[1],
                    "field":       f[2],
                    "value":       f[3],
                })
        df = pd.DataFrame(flat)
        success(f"Fetched {len(cards):,} cards.")
        return df, cards, fetcher

    except Exception as e:
        error(f"Scryfall fetch failed: {e}")
        info("Try a set code like: grn, war, eld, neo, mom")
        sys.exit(1)


def fetch_loc(search_term: str):
    """Fetch records from Library of Congress."""
    from snf_peirce.fetch_loc import LOCFetcher
    import pandas as pd

    info(f"Fetching from Library of Congress: '{search_term}'...")

    try:
        fetcher = LOCFetcher(search_term)
        records = fetcher.fetch()
        rows    = [fetcher.translate(r) for r in records]

        flat = []
        for entity_facts in rows:
            for f in entity_facts:
                flat.append({
                    "entity_id": f[0],
                    "dimension": f[1],
                    "field":     f[2],
                    "value":     f[3],
                })
        df = pd.DataFrame(flat)
        success(f"Fetched {len(records):,} records.")
        return df, records, fetcher

    except Exception as e:
        error(f"Library of Congress fetch failed: {e}")
        sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    banner("SNF / Peirce  —  Query an API")

    print("  Fetch data from an online source, map it to meaning,")
    print("  and query it in plain language. No SQL. No physical schema knowledge.")
    print("")

    # ── Choose a source ───────────────────────────────────────────────────────

    if len(sys.argv) > 1 and sys.argv[1] in SOURCES:
        source_key = sys.argv[1]
        info(f"Using: {SOURCES[source_key]['label']}")
    else:
        print("  Available data sources:\n")
        for key, meta in SOURCES.items():
            needs = "  (free API token required)" if meta["needs_key"] else ""
            print(f"    {key:<12}  {meta['label']}{needs}")
        print("")

        source_key = ask("Which source would you like to use").strip().lower()

        if source_key not in SOURCES:
            error(f"'{source_key}' is not supported.")
            info(f"Choose from: {', '.join(SOURCES.keys())}")
            sys.exit(1)

    source = SOURCES[source_key]
    print("")
    info(source["description"])
    print("")

    # ── API key if needed ─────────────────────────────────────────────────────

    api_key = None
    if source.get("needs_key"):
        section("API access")
        info(source["key_help"])
        print("")
        api_key = ask(source["key_name"])
        if not api_key:
            error("No token provided. Exiting.")
            sys.exit(1)

    # ── What to search for ────────────────────────────────────────────────────

    section("What are you looking for?")

    if source_key == "scryfall":
        print("  Enter a Magic set code to fetch all cards from that set.")
        print("  Popular sets: grn, war, eld, neo, mom, lci, mkm, otj, dsk")
        print("")
        search = ask("Set code", default="grn")

    elif source_key == "loc":
        print("  Enter a search term — author name, subject, title keyword.")
        print("")
        search = ask("Search term", default="jazz")

    elif source_key == "discogs":
        print("  Enter an artist name or release title to search for.")
        print("")
        search = ask("Search term", default="Miles Davis")

    else:
        search = ask("Search term")

    # ── Fetch ─────────────────────────────────────────────────────────────────

    section("Fetching data")

    df = None

    if source_key == "scryfall":
        df, raw, fetcher = fetch_scryfall(search)

    elif source_key == "loc":
        df, raw, fetcher = fetch_loc(search)

    elif source_key == "discogs":
        try:
            # Discogs fetcher — uses base_fetcher pattern
            info(f"Fetching from Discogs: '{search}'...")
            import requests
            headers = {
                "Authorization": f"Discogs token={api_key}",
                "User-Agent":    "snf-peirce/0.1.1",
            }
            resp = requests.get(
                "https://api.discogs.com/database/search",
                params={"q": search, "per_page": 50},
                headers=headers,
                timeout=15,
            )
            resp.raise_for_status()
            results = resp.json().get("results", [])
            if not results:
                error("No results found. Try a different search term.")
                sys.exit(1)

            import pandas as pd
            rows = []
            for r in results:
                rows.append({
                    "title":       r.get("title", ""),
                    "year":        str(r.get("year", "")),
                    "genre":       ", ".join(r.get("genre", [])),
                    "style":       ", ".join(r.get("style", [])),
                    "label":       ", ".join(r.get("label", [])),
                    "country":     r.get("country", ""),
                    "format":      ", ".join(r.get("format", [])),
                    "resource_url": r.get("resource_url", ""),
                    "id":          str(r.get("id", "")),
                })
            df = pd.DataFrame(rows)
            success(f"Fetched {len(df):,} releases.")

        except Exception as e:
            error(f"Discogs fetch failed: {e}")
            sys.exit(1)

    if df is None or len(df) == 0:
        error("No data returned. Try a different search term.")
        sys.exit(1)

    # ── Suggest lens ──────────────────────────────────────────────────────────

    section("Mapping your data")

    print("  Analysing the data...")
    print("")

    try:
        from snf_peirce import suggest, compile_data
        draft = suggest(df)
        print(draft)
        print("")
    except Exception as e:
        error(f"Could not analyse data: {e}")
        sys.exit(1)

    if not confirm("Does this mapping look reasonable"):
        print("")
        while True:
            field = ask("Field to remap (or press Enter to continue)")
            if not field:
                break
            dim = ask(f"  Dimension for '{field}'? (WHO/WHAT/WHEN/WHERE/WHY/HOW)").upper()
            key = ask(f"  Key for '{field}'", default=field.lower().replace(" ", "_"))
            try:
                draft.map(field, dim.lower(), key)
                success(f"Mapped {field} → {dim}.{key}")
            except Exception as e:
                error(f"Could not remap: {e}")

    # ── Nucleus ───────────────────────────────────────────────────────────────

    # Try to find an ID field automatically
    id_candidates = [c for c in df.columns if c.lower() in ("id", "resource_url", "entity_id")]
    if id_candidates:
        nucleus_field = id_candidates[0]
        info(f"Using '{nucleus_field}' as the unique identifier.")
    else:
        nucleus_field = ask(
            "Which field uniquely identifies each record",
            default=df.columns[0]
        )

    prefix = ask("Short prefix for entity IDs", default=f"{source_key}:record")

    try:
        draft.nucleus(nucleus_field, prefix=prefix)
    except Exception as e:
        warn(f"Nucleus issue: {e}")
        info("Continuing with auto-generated IDs.")

    lens_id = ask("Name for this dataset", default=f"{source_key}_{search[:10].replace(' ','_').lower()}")

    # ── Compile ───────────────────────────────────────────────────────────────

    section("Compiling")

    print("  Building your queryable substrate...")

    try:
        lens     = draft.to_lens(lens_id=lens_id, authority="me")
        compiled = compile_data(df, lens)
        success("Done. Your data is ready to query.")
    except Exception as e:
        error(f"Compilation failed: {e}")
        sys.exit(1)

    # ── Example query hint ────────────────────────────────────────────────────

    print("")
    print(f"  Example query for this data:")
    print(f'    {source["example"]}')
    print("")

    # ── Query loop ────────────────────────────────────────────────────────────

    query_loop(compiled, welcome="Your data is ready.")


if __name__ == "__main__":
    main()
