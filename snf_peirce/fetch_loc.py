"""
fetch_loc.py — Library of Congress Catalog → SNF

Fetches bibliographic records from the Library of Congress public API
and compiles them into a queryable SNF substrate using the MARC
Bibliographic Lens v1.0.

This is where snf-peirce started — the LOC catalog was the first
real dataset used to prove the SNF architecture.

Usage:
    python fetch_loc.py                          # default: jazz music
    python fetch_loc.py "toni morrison"          # search by keyword
    python fetch_loc.py "civil war" --limit 200  # more results
    python fetch_loc.py --subject "cooking"      # subject search
    python fetch_loc.py --author "morrison"      # author search

Requirements:
    pip install requests

The LOC API is free and requires no authentication.

Try these queries once compiled:
    WHO.author = "Morrison, Toni"
    WHAT.subject_topic = "Jazz"
    WHEN.publication_date BETWEEN "1950" AND "1970"
    WHERE.publication_place = "New York"
    WHAT.subject_topic CONTAINS "African American"
    WHO.subject_person CONTAINS "Lincoln"
    WHAT.genre = "Fiction"
    WHO.publisher CONTAINS "University"
"""

from __future__ import annotations

import sys
import time
from snf_peirce.base_fetcher import SNFFetcher, fact, facts, facts_from_list
from snf_peirce.marc_translator import MARCTranslator


# ─────────────────────────────────────────────────────────────────────────────
# LOC JSON → normalized MARC record
#
# The LOC API returns MARC data in a JSON structure that needs to be
# normalized to the standard shape before passing to MARCTranslator.
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_loc_record(item):
    """
    Normalize a LOC API result item into the standard MARC record shape.

    LOC returns MARC data in its own JSON structure. This converts it
    to the same normalized shape that MARCTranslator expects.
    """
    marc_record = {
        "leader":        "",
        "controlFields": [],
        "dataFields":    [],
    }

    # The LOC API returns MARC fields as a list under item["marc_items"]
    # or directly in the item dict depending on the endpoint
    marc_data = item.get("marc_items") or item.get("item", {})

    # Try to extract from the formatted MARC structure
    # LOC /search/?fo=json returns records with various field structures
    # We normalize the most common patterns

    # Control number from id or number_lccn
    control_num = (
        item.get("number_lccn", [None])[0] or
        item.get("id", "").split("/")[-1].strip() or
        ""
    )
    if control_num:
        marc_record["controlFields"].append({"tag": "001", "data": control_num})

    # Title — from title field
    title = item.get("title", "")
    if title:
        # Strip trailing slash and normalize
        title = title.rstrip("/ ").strip()
        marc_record["dataFields"].append({
            "tag": "245", "indicator1": "1", "indicator2": "0",
            "subfields": [{"code": "a", "data": title}]
        })

    # Authors — from contributor or creator
    for author in (item.get("contributor") or item.get("creator") or []):
        if author:
            marc_record["dataFields"].append({
                "tag": "100", "indicator1": "1", "indicator2": " ",
                "subfields": [{"code": "a", "data": author}]
            })

    # Publisher / date / place — from publisher field
    for pub in (item.get("publisher") or []):
        if pub:
            marc_record["dataFields"].append({
                "tag": "264", "indicator1": " ", "indicator2": "1",
                "subfields": [{"code": "b", "data": pub}]
            })

    # Publication date — from date field
    date = item.get("date", "")
    if date:
        marc_record["dataFields"].append({
            "tag": "264", "indicator1": " ", "indicator2": "1",
            "subfields": [{"code": "c", "data": str(date)}]
        })

    # Subjects — from subject field
    for subject in (item.get("subject") or []):
        if subject:
            marc_record["dataFields"].append({
                "tag": "650", "indicator1": " ", "indicator2": "0",
                "subfields": [{"code": "a", "data": subject}]
            })

    # Language
    for lang in (item.get("language") or []):
        if lang:
            marc_record["dataFields"].append({
                "tag": "337", "indicator1": " ", "indicator2": " ",
                "subfields": [{"code": "a", "data": lang}]
            })

    # Format / type
    for fmt in (item.get("format") or []):
        if fmt:
            marc_record["dataFields"].append({
                "tag": "338", "indicator1": " ", "indicator2": " ",
                "subfields": [{"code": "a", "data": fmt}]
            })

    # Location / place
    for place in (item.get("location") or []):
        if place:
            marc_record["dataFields"].append({
                "tag": "264", "indicator1": " ", "indicator2": "1",
                "subfields": [{"code": "a", "data": place}]
            })

    # Notes / description
    for note in (item.get("description") or []):
        if note:
            marc_record["dataFields"].append({
                "tag": "500", "indicator1": " ", "indicator2": " ",
                "subfields": [{"code": "a", "data": note}]
            })

    # Type / genre
    item_type = item.get("type", [])
    if isinstance(item_type, list):
        for t in item_type:
            if t:
                marc_record["dataFields"].append({
                    "tag": "655", "indicator1": " ", "indicator2": "7",
                    "subfields": [{"code": "a", "data": t}]
                })
    elif item_type:
        marc_record["dataFields"].append({
            "tag": "655", "indicator1": " ", "indicator2": "7",
            "subfields": [{"code": "a", "data": str(item_type)}]
        })

    return marc_record


# ─────────────────────────────────────────────────────────────────────────────
# LOC Fetcher
# ─────────────────────────────────────────────────────────────────────────────

class LOCFetcher(SNFFetcher):
    """
    Fetches Library of Congress catalog records and compiles to SNF.

    Uses the free LOC JSON API — no authentication required.
    Applies the MARC Bibliographic Lens v1.0 for field mapping.
    """

    def __init__(self, query="jazz music", limit=100,
                 subject=None, author=None):
        self.query    = query
        self.limit    = limit
        self.subject  = subject
        self.author   = author

        # Build a slug for file naming
        slug = (subject or author or query).lower()
        slug = "".join(c if c.isalnum() else "_" for c in slug)[:30].strip("_")

        self.set_name  = f"Library of Congress: {subject or author or query}"
        self.lens_id   = f"loc_marc_{slug}_v1"
        self.spoke_dir = f"loc_{slug}_spoke"

        self._translator = MARCTranslator(source_id="loc")

    def fetch(self):
        """Fetch records from the LOC search API."""
        try:
            import requests
        except ImportError:
            raise ImportError("pip install requests")

        results = []
        per_page = min(self.limit, 100)
        fetched  = 0
        page     = 1

        # Build query params
        params = {
            "fo":      "json",
            "c":       per_page,
            "at":      "results",
        }

        # Subject search vs keyword search
        if self.subject:
            params["q"] = f'subject:"{self.subject}"'
        elif self.author:
            params["q"] = f'contributor:"{self.author}"'
        else:
            params["q"] = self.query

        # Filter to books and resources with MARC data
        params["fa"] = "online-format:ebook|online-format:printable"

        base_url = "https://www.loc.gov/search/"
        headers  = {
            "User-Agent": "snf-peirce/0.1.0 fetch_loc.py (educational use)",
            "Accept":     "application/json",
        }

        while fetched < self.limit:
            params["sp"] = page
            print(f"  Fetching page {page}...", end="\r")

            try:
                response = requests.get(
                    base_url, params=params, headers=headers, timeout=30
                )
                if not response.ok:
                    print(f"\n  Warning: LOC API returned {response.status_code} on page {page}")
                    break

                data = response.json()
            except Exception as e:
                print(f"\n  Warning: fetch error on page {page}: {e}")
                break

            items = data.get("results", [])
            if not items:
                break

            results.extend(items)
            fetched += len(items)

            # Check if more pages exist
            pagination = data.get("pagination", {})
            if not pagination.get("next") or fetched >= self.limit:
                break

            page += 1
            time.sleep(0.5)   # be polite to LOC

        print(f"  Fetched {len(results)} records.          ")
        return results[:self.limit]

    def entity_id(self, item):
        control_num = (
            item.get("number_lccn", [None])[0] or
            item.get("id", "").split("/")[-1].strip() or
            "unknown"
        )
        return f"marc:loc:{control_num}"

    def translate(self, item):
        """Translate a LOC result item into SNF facts via MARCTranslator."""
        # Normalize to standard MARC shape
        marc_record = _normalize_loc_record(item)

        # Translate using the MARC lens
        marc_facts = self._translator.translate_record(marc_record)

        # marc_facts already have entity_id set correctly by the translator
        # Just return them — they're already in the right format
        return marc_facts

    def example_queries(self):
        base = []
        if self.subject:
            base = [
                f'WHAT.subject_topic CONTAINS "{self.subject}"',
                f'WHEN.publication_date BETWEEN "1950" AND "2000"',
                f'WHERE.publication_place = "New York"',
                f'WHO.publisher CONTAINS "University"',
            ]
        elif self.author:
            base = [
                f'WHO.author CONTAINS "{self.author}"',
                f'WHAT.genre = "Fiction"',
                f'WHEN.publication_date BETWEEN "1970" AND "2024"',
            ]
        else:
            base = [
                f'WHAT.subject_topic CONTAINS "{self.query.split()[0]}"',
                f'WHO.author PREFIX "Morrison"',
                f'WHEN.publication_date BETWEEN "1950" AND "1980"',
                f'WHERE.publication_place = "New York"',
                f'WHAT.genre = "Fiction"',
            ]
        base += [
            "WHAT|subject_topic|*",
            "WHO|author|*",
            "WHERE|publication_place|*",
        ]
        return base


# ─────────────────────────────────────────────────────────────────────────────
# MARCFileFetcher — full-fidelity path via pymarc
#
# Reads binary .mrc / MARCXML files directly.
# This is the high-fidelity path — full subfield access, all qualifiers,
# identical output to MARCTranslator_v3.js reading the same files.
#
# Requires: pip install pymarc
#
# LOC bulk MARC downloads: https://www.loc.gov/cds/products/marcDist.php
# ─────────────────────────────────────────────────────────────────────────────

def _pymarc_record_to_normalized(record):
    """
    Convert a pymarc Record object to the normalized MARC dict shape
    that MARCTranslator expects.

    This is the same shape as MARCTranslator_v3.js _normalizeMARC4JSRecord().
    """
    control_fields = []
    data_fields    = []

    for field in record.fields:
        if field.is_control_field():
            control_fields.append({
                "tag":  field.tag,
                "data": field.data or "",
            })
        else:
            subfields = []
            # pymarc stores subfields as alternating [code, value, code, value, ...]
            sf_list = field.subfields
            for i in range(0, len(sf_list) - 1, 2):
                subfields.append({
                    "code": sf_list[i],
                    "data": sf_list[i + 1],
                })
            data_fields.append({
                "tag":        field.tag,
                "indicator1": field.indicator1 or " ",
                "indicator2": field.indicator2 or " ",
                "subfields":  subfields,
            })

    return {
        "leader":        str(record.leader),
        "controlFields": control_fields,
        "dataFields":    data_fields,
    }


class MARCFileFetcher(SNFFetcher):
    """
    Load MARC records from a .mrc binary file or MARCXML file.

    Full-fidelity path — uses pymarc to read raw MARC with all subfields.
    Produces richer facts than the JSON API path (qualifiers, subfield
    subdivisions, etc.) because it has access to the raw MARC structure.

    Usage:
        python fetch_loc.py --marc-file catalog.mrc
        python fetch_loc.py --marc-file export.xml --marc-format xml

    Requirements:
        pip install pymarc
    """

    def __init__(self, marc_file, marc_format="marc", source_id="loc"):
        self.marc_file   = marc_file
        self.marc_format = marc_format  # "marc" for binary, "xml" for MARCXML

        from pathlib import Path
        stem = Path(marc_file).stem[:30]

        self.set_name  = f"MARC File: {Path(marc_file).name}"
        self.lens_id   = f"marc_{stem}_v1"
        self.spoke_dir = f"marc_{stem}_spoke"
        self.source_id = source_id

        self._translator = MARCTranslator(source_id=source_id)

    def fetch(self):
        """Read MARC records from file using parse_marc (no extra dependencies)."""
        from parse_marc import parse_marc_file
        records = parse_marc_file(self.marc_file)
        print(f"  Read {len(records):,} records from {self.marc_file}")
        return records

    def entity_id(self, record):
        for cf in record.get("controlFields", []):
            if cf["tag"] == "001" and cf.get("data"):
                return 'marc:' + self.source_id + ':' + cf['data'].strip()
        return f"marc:{self.source_id}:unknown"

    def translate(self, record):
        """Translate a normalized MARC record into SNF facts via MARCTranslator."""
        return self._translator.translate_record(record)

    def example_queries(self):
        return [
            'WHO.author PREFIX "Morrison"',
            'WHAT.subject_topic CONTAINS "history"',
            'WHEN.publication_date BETWEEN "1950" AND "2000"',
            'WHERE.publication_place = "New York"',
            'WHAT.genre = "Fiction"',
            'WHO.publisher CONTAINS "University"',
            "WHAT|subject_topic|*",
            "WHO|author|*",
        ]


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Fetch Library of Congress catalog records into SNF",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  python fetch_loc.py                           # jazz music (default)
  python fetch_loc.py "toni morrison"           # keyword search
  python fetch_loc.py --subject "cooking"       # subject search
  python fetch_loc.py --author "hemingway"      # author search
  python fetch_loc.py "civil war" --limit 200   # more results

  # Full-fidelity path from a .mrc file (requires: pip install pymarc)
  python fetch_loc.py --marc-file catalog.mrc
  python fetch_loc.py --marc-file export.xml --marc-format xml
        """
    )
    parser.add_argument(
        "query",
        nargs="?",
        default="jazz music",
        help="Keyword search query (default: 'jazz music')"
    )
    parser.add_argument(
        "--subject",
        default=None,
        help="Search by subject heading"
    )
    parser.add_argument(
        "--author",
        default=None,
        help="Search by author/contributor"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of records to fetch (default: 100)"
    )
    parser.add_argument(
        "--marc-file",
        default=None,
        help="Path to a .mrc or MARCXML file (full-fidelity path, requires pymarc)"
    )
    parser.add_argument(
        "--marc-format",
        default="marc",
        choices=["marc", "xml"],
        help="Format of --marc-file: 'marc' for binary .mrc, 'xml' for MARCXML"
    )
    parser.add_argument(
        "--no-shell",
        action="store_true",
        help="Compile only, do not open query shell"
    )

    args = parser.parse_args()

    try:
        if args.marc_file:
            # Full-fidelity path — read from .mrc or MARCXML file
            fetcher = MARCFileFetcher(
                marc_file   = args.marc_file,
                marc_format = args.marc_format,
            )
        else:
            # JSON API path — fetch from LOC search API
            fetcher = LOCFetcher(
                query   = args.query,
                limit   = args.limit,
                subject = args.subject,
                author  = args.author,
            )
        fetcher.run(open_shell=not args.no_shell)
    except KeyboardInterrupt:
        print("\n\n  Exited.\n")


if __name__ == "__main__":
    main()
