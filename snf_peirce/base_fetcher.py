"""
base_fetcher.py — Generic API → SNF Fetcher Base Class

Provides a base class for fetching data from any API and compiling
it into a queryable SNF substrate.

To add a new data source, subclass SNFFetcher and implement three methods:
    fetch()       → returns list of raw objects from the API
    translate()   → turns one object into a list of SNF fact dicts
    entity_id()   → returns the unique ID string for one object

Everything else — compilation, spoke file writing, shell launch — is
handled automatically by the base class.

Example
-------
See fetch_scryfall.py for a complete working example.

Quick template:

    from base_fetcher import SNFFetcher, fact

    class MyApiFetcher(SNFFetcher):
        lens_id   = "myapi_v1"
        set_name  = "My Dataset"
        spoke_dir = "myapi_spoke"

        def fetch(self):
            import requests
            response = requests.get("https://api.example.com/data")
            return response.json()["items"]

        def entity_id(self, item):
            return f"myapi:{item['id']}"

        def translate(self, item):
            eid = self.entity_id(item)
            return [
                fact(eid, "what", "title",    item.get("title")),
                fact(eid, "who",  "author",   item.get("author")),
                fact(eid, "when", "year",     item.get("year")),
                fact(eid, "where","location", item.get("location")),
            ]

    if __name__ == "__main__":
        MyApiFetcher().run()

Fact helpers
------------
fact(entity_id, dimension, semantic_key, value)
    → single fact dict, skips None/empty values automatically

facts_from_list(entity_id, dimension, semantic_key, values)
    → one fact per item in a list (for multi-valued fields like colors,
      keywords, genres, tags)

facts_from_dict(entity_id, dimension, key_prefix, d)
    → one fact per key/value pair in a dict

SNF dimensions
--------------
    WHO   — people, organisations, roles, creators
    WHAT  — things, topics, identifiers, categories, content
    WHEN  — dates, years, time periods
    WHERE — places, locations, regions, collections
    WHY   — reasons, types, purposes, formats, genres
    HOW   — methods, formats, media, measurements, stats
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Any


# ─────────────────────────────────────────────────────────────────────────────
# Fact helpers — the building blocks of translation
# ─────────────────────────────────────────────────────────────────────────────

def fact(entity_id, dimension, semantic_key, value):
    """
    Build a single SNF fact dict.

    Returns None if value is empty — callers should filter these out,
    or use the facts() helper which does this automatically.

    Args:
        entity_id:    unique entity identifier string
        dimension:    one of: who, what, when, where, why, how
        semantic_key: the semantic key (e.g. "author", "title", "year")
        value:        the value — strings, numbers, booleans all accepted

    Returns:
        dict or None
    """
    if value is None:
        return None
    str_val = str(value).strip()
    if str_val in ("", "None", "nan", "undefined", "null"):
        return None
    return {
        "entity_id":    entity_id,
        "dimension":    dimension.lower(),
        "semantic_key": semantic_key.lower(),
        "value":        str_val,
    }


def facts(*fact_tuples):
    """
    Build multiple facts from (entity_id, dimension, key, value) tuples.
    Automatically filters out None/empty values.

    Usage:
        facts(
            (eid, "what", "title",  item["title"]),
            (eid, "who",  "author", item["author"]),
            (eid, "when", "year",   item["year"]),
        )
    """
    result = []
    for t in fact_tuples:
        f = fact(*t)
        if f:
            result.append(f)
    return result


def facts_from_list(entity_id, dimension, semantic_key, values):
    """
    Build one fact per item in a list — for multi-valued fields.

    Usage:
        facts_from_list(eid, "what", "color",   ["Blue", "Black"])
        facts_from_list(eid, "what", "keyword", ["Flying", "Deathtouch"])
        facts_from_list(eid, "why",  "genre",   ["Drama", "Thriller"])
    """
    result = []
    for v in (values or []):
        f = fact(entity_id, dimension, semantic_key, v)
        if f:
            result.append(f)
    return result


def facts_from_dict(entity_id, dimension, key_prefix, d):
    """
    Build one fact per key/value pair in a dict.

    Usage:
        facts_from_dict(eid, "why", "legality", {"standard": "legal", "modern": "legal"})
        → fact(eid, "why", "legality_standard", "legal")
        → fact(eid, "why", "legality_modern",   "legal")
    """
    result = []
    for k, v in (d or {}).items():
        f = fact(entity_id, dimension, f"{key_prefix}_{k}", v)
        if f:
            result.append(f)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# SNFFetcher base class
# ─────────────────────────────────────────────────────────────────────────────

class SNFFetcher:
    """
    Base class for fetching data from any API and compiling to SNF.

    Subclass and implement:
        fetch()       → list of raw API objects
        translate()   → list of fact dicts for one object
        entity_id()   → unique ID string for one object

    Class attributes to set:
        lens_id   (str) — identifier for this dataset, e.g. "scryfall_grn_v1"
        set_name  (str) — human-readable name, e.g. "Guilds of Ravnica"
        spoke_dir (str) — output directory for spoke CSV files

    Optional class attributes:
        rate_limit_seconds (float) — pause between paginated API calls (default 0.1)
        skip_errors        (bool)  — skip individual item errors vs raise (default True)
    """

    lens_id            = "dataset_v1"
    set_name           = "Dataset"
    spoke_dir          = "dataset_spoke"
    rate_limit_seconds = 0.1
    skip_errors        = True

    # ── Methods to override ──────────────────────────────────────────────────

    def fetch(self):
        """
        Fetch raw data from the source.

        Returns:
            list of raw objects (dicts, etc.) — one per entity

        This is where your API calls go. For paginated APIs, handle
        all pages here and return the full list.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement fetch()"
        )

    def translate(self, item):
        """
        Translate one raw API object into a list of SNF fact dicts.

        Args:
            item: one raw object from fetch()

        Returns:
            list of fact dicts — use the fact() and facts_from_list()
            helpers to build them cleanly

        Example:
            def translate(self, card):
                eid = self.entity_id(card)
                return [
                    *facts(
                        (eid, "what", "name",   card["name"]),
                        (eid, "who",  "artist", card["artist"]),
                        (eid, "when", "year",   card["year"]),
                    ),
                    *facts_from_list(eid, "what", "color", card["colors"]),
                ]
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement translate()"
        )

    def entity_id(self, item):
        """
        Return the unique entity ID string for one raw object.

        Convention: "source_prefix:raw_id"
        Examples:   "scryfall:abc123", "imdb:tt0068646", "cl:1234567"
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement entity_id()"
        )

    # ── Optional hooks ───────────────────────────────────────────────────────

    def on_fetch_complete(self, items):
        """Called after fetch() completes. Override for custom post-processing."""
        pass

    def on_compile_complete(self, substrate):
        """Called after compilation. Override to add custom output."""
        pass

    def example_queries(self):
        """
        Return a list of example Peirce query strings to show after compilation.
        Override to provide dataset-specific examples.
        """
        return [
            'WHAT.title = "example"',
            'WHO.author PREFIX "Smith"',
        ]

    # ── Core run loop ────────────────────────────────────────────────────────

    def run(self, open_shell=None):
        """
        Run the full pipeline: fetch → translate → compile → (shell).

        Args:
            open_shell: True/False/None (None = ask the user)

        Returns:
            Substrate
        """
        self._print_header()

        # Step 1 — fetch
        print(f"  Fetching data from {self.set_name}...")
        try:
            items = self.fetch()
        except ImportError as e:
            print(f"\n  Missing dependency: {e}")
            sys.exit(1)
        except Exception as e:
            print(f"\n  Fetch failed: {e}")
            sys.exit(1)

        if not items:
            print(f"  No data returned. Check your fetch() implementation.")
            sys.exit(1)

        print(f"  {len(items):,} items fetched.")
        self.on_fetch_complete(items)

        # Step 2 — translate
        print(f"  Translating to SNF facts...")
        all_facts = []
        errors    = 0
        for item in items:
            try:
                translated = self.translate(item)
                all_facts.extend([f for f in translated if f is not None])
            except Exception as e:
                errors += 1
                if not self.skip_errors:
                    raise
        if errors:
            print(f"  Warning: {errors} items skipped due to translation errors.")

        print(f"  {len(all_facts):,} facts from {len(items):,} entities.")
        print()

        # Step 3 — compile
        print(f"  Compiling to SNF substrate...")
        substrate = self._compile_facts(all_facts)

        # Step 4 — write to CSV spoke files
        from compile import _write_csv
        _write_csv(substrate._conn, substrate.lens_id, self.spoke_dir)

        # Step 5 — save lens JSON
        self._save_lens()

        # Step 6 — summary
        self._print_summary(substrate)
        self.on_compile_complete(substrate)

        # Step 7 — open shell
        if open_shell is None:
            answer = input(f"  Open query shell? [Y/n]: ").strip().lower()
            open_shell = answer != "n"

        if open_shell:
            from shell import run_shell
            run_shell(f"csv://{self.spoke_dir}")
        else:
            print(f"\n  To query later:")
            print(f"    python shell.py csv://{self.spoke_dir}")
            print()

        return substrate

    # ── Internal helpers ─────────────────────────────────────────────────────

    def _compile_facts(self, all_facts):
        """Compile a flat list of fact dicts directly into a Substrate."""
        try:
            import duckdb
        except ImportError:
            raise ImportError("pip install duckdb")

        from compile import Substrate, _SPOKE_DDL, _SPOKE_INDEX_DDL

        conn = duckdb.connect(":memory:")
        conn.execute(_SPOKE_DDL)

        rows = []
        for f in all_facts:
            dim   = f["dimension"]
            key   = f["semantic_key"]
            val   = f["value"]
            eid   = f["entity_id"]
            coord = f"{dim.upper()}|{key}|{val}"
            rows.append((eid, dim, key, val, coord, self.lens_id))

        if rows:
            conn.executemany(
                "INSERT INTO snf_spoke "
                "(entity_id, dimension, semantic_key, value, coordinate, lens_id) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                rows
            )

        for idx_ddl in _SPOKE_INDEX_DDL:
            conn.execute(idx_ddl)

        return Substrate(conn, self.lens_id)

    def _save_lens(self):
        """Save a minimal lens JSON file for this dataset."""
        import datetime
        lens = {
            "lens_id":      self.lens_id,
            "lens_version": "1.0",
            "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
            "declaration": {
                "why|intent":        self.lens_id,
                "why|authority":     self.__class__.__name__,
                "why|scope":         self.set_name,
                "why|permitted_ops": "field_mapping canonical_tagging",
                "source_format":     "api_json",
                "domain":            self.set_name,
                "created":           datetime.date.today().isoformat(),
                "created_by":        self.__class__.__name__,
            },
            "coordinate_map": {},
            "stats": {
                "total_fields": 0,
                "by_dimension": {
                    "who": 0, "what": 0, "when": 0,
                    "where": 0, "how": 0
                },
            },
            "nucleus": {
                "type":   "single",
                "field":  "entity_id",
                "prefix": "",
            },
        }
        lens_path = Path(f"{self.spoke_dir}_lens.json")
        with open(lens_path, "w", encoding="utf-8") as f:
            json.dump(lens, f, indent=2)

    def _print_header(self):
        print()
        print(f"  {'─' * 50}")
        print(f"  {self.set_name}")
        print(f"  {'─' * 50}")
        print()

    def _print_summary(self, substrate):
        d = substrate.describe()
        print(f"  ✓ Compiled successfully")
        print()
        print(f"  Entities:  {d['entity_count']:,}")
        print(f"  Facts:     {d['fact_count']:,}")
        print()
        print(f"  By dimension:")
        for dim, count in sorted(d["facts_by_dim"].items()):
            if count > 0:
                print(f"    {dim.upper():<8}  {count:,}")
        print()
        print(f"  Output: {self.spoke_dir}/")
        print()

        queries = self.example_queries()
        if queries:
            print("  Example queries:")
            for q in queries:
                print(f'    {q}')
            print()


# ─────────────────────────────────────────────────────────────────────────────
# Pagination helper — useful for paginated REST APIs
# ─────────────────────────────────────────────────────────────────────────────

def paginate(url, headers=None, data_key="data", next_key="next_page",
             has_more_key="has_more", rate_limit=0.1, params=None):
    """
    Generic paginator for REST APIs that return paginated JSON.

    Works with APIs that use:
        { "data": [...], "has_more": true, "next_page": "https://..." }

    Args:
        url:          starting URL
        headers:      request headers dict
        data_key:     key containing the list of items (default "data")
        next_key:     key containing the next page URL (default "next_page")
        has_more_key: key indicating more pages exist (default "has_more")
        rate_limit:   seconds to wait between pages (default 0.1)
        params:       query parameters dict for the first request

    Yields:
        individual items from each page

    Usage:
        for card in paginate("https://api.scryfall.com/cards/search?q=set:grn",
                             headers={"User-Agent": "myapp/1.0"}):
            process(card)
    """
    try:
        import requests
    except ImportError:
        raise ImportError("pip install requests")

    page = 1
    while url:
        print(f"  Fetching page {page}...", end="\r")
        response = requests.get(url, headers=headers or {}, params=params, timeout=30)
        params = None  # only use params on first request

        if not response.ok:
            try:
                error = response.json().get("details", response.reason)
            except Exception:
                error = response.reason
            raise RuntimeError(f"API error {response.status_code}: {error}")

        data = response.json()
        items = data.get(data_key, [])
        yield from items

        if data.get(has_more_key):
            url = data.get(next_key)
            page += 1
            if rate_limit:
                time.sleep(rate_limit)
        else:
            url = None

    print(f"  Fetched {page} page{'s' if page != 1 else ''}.          ")


# ─────────────────────────────────────────────────────────────────────────────
# CLI helper — makes any fetcher runnable as a script
# ─────────────────────────────────────────────────────────────────────────────

def run_fetcher(fetcher_class, *args, **kwargs):
    """
    Run a fetcher class as a CLI script.

    Usage at the bottom of any fetcher file:
        if __name__ == "__main__":
            run_fetcher(MyFetcher)

    Handles --help and Ctrl+C gracefully.
    """
    import argparse

    parser = argparse.ArgumentParser(
        description=f"Fetch {fetcher_class.set_name} into SNF substrate"
    )
    parser.add_argument(
        "--no-shell",
        action="store_true",
        help="Compile only, do not open query shell"
    )
    parsed = parser.parse_args()

    try:
        fetcher = fetcher_class(*args, **kwargs)
        fetcher.run(open_shell=not parsed.no_shell)
    except KeyboardInterrupt:
        print("\n\n  Exited.\n")
        sys.exit(0)
