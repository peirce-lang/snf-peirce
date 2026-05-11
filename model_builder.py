"""
model_builder.py — SNF Model Builder  (Layer A + DuckDB emitter)

Translates structured source data into an SNF substrate.
First vertical slice: DuckDB output only.

Usage:
    python model_builder.py \\
        --input  my_collection.csv \\
        --lens   discogs_v1.lens.json \\
        --into   duckdb://my_collection.duckdb \\
        --translator discogs

    python model_builder.py --info my_collection.duckdb

Supported translators:
    discogs              — Discogs collection CSV export
    disney_characters    — disney-characters.csv
    disney_director      — disney-director.csv
    disney_voice_actors  — disney-voice-actors.csv
    disney_total_gross   — disney_movies_total_gross.csv
    shibuya              — shibuya_staging Postgres export

Multi-table ingest (I17):
    Run model_builder.py once per table against the same --into duckdb://path.
    The DuckDB emitter appends — entities from different tables merge automatically
    when they share the same entity_id (same normalized movie title nucleus).

Output targets:
    --into duckdb://path            Ready-to-query DuckDB file
    --into csv://dir                Spoke CSVs + lens.json (snf-peirce compatible)
    --into postgres-import://dir    Full Postgres import package (C4)
                                    DDL + COPY CSVs + load.sh + verification report

Architecture:
    Layer A — canonical row compilation (this file, _compile_rows)
    Layer B — artifact emitters (_emit_duckdb, _emit_csv — others TBD)
    Layer C — compile_job(spec) → BuildResult (service boundary, called by JS shell)

BuildSpec fields:
    source              path to input file
    translator          translator name (discogs | generic | ...)
    lens_id             lens identity string
    translator_version  semver string stamped into every spoke row
    into                output target: duckdb://path | csv://dir
    verbose             bool

BuildResult fields:
    success             bool
    output_path         str
    entity_count        int
    fact_count          int
    facts_by_dim        dict  {dimension: count}
    errors              list[str]
    warnings            list[str]
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Optional

try:
    import pandas as pd
except ImportError:
    print("pandas required: pip install pandas")
    sys.exit(1)

try:
    import duckdb
except ImportError:
    print("duckdb required: pip install duckdb")
    sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
# Spoke row schema
# ─────────────────────────────────────────────────────────────────────────────

DIMENSIONS = ["who", "what", "when", "where", "why", "how"]

SPOKE_DDL = """
CREATE TABLE IF NOT EXISTS snf_spoke (
    entity_id          TEXT NOT NULL,
    dimension          TEXT NOT NULL,
    semantic_key       TEXT NOT NULL,
    value              TEXT NOT NULL,
    coordinate         TEXT NOT NULL,
    lens_id            TEXT NOT NULL,
    translator_version TEXT NOT NULL
)
"""

SPOKE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_spoke_coordinate   ON snf_spoke (coordinate)",
    "CREATE INDEX IF NOT EXISTS idx_spoke_entity_id    ON snf_spoke (entity_id)",
    "CREATE INDEX IF NOT EXISTS idx_spoke_dim_key      ON snf_spoke (dimension, semantic_key)",
    "CREATE INDEX IF NOT EXISTS idx_spoke_dim_key_val  ON snf_spoke (dimension, semantic_key, value)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_spoke_entity_coord ON snf_spoke (entity_id, coordinate)",
]

META_DDL = """
CREATE TABLE IF NOT EXISTS snf_meta (
    entity_id TEXT PRIMARY KEY,
    nucleus   TEXT,
    label     TEXT,
    sublabel  TEXT
)
"""


# ─────────────────────────────────────────────────────────────────────────────
# Discogs translator
# ─────────────────────────────────────────────────────────────────────────────

# Primary format types — these describe WHAT the object is
FORMAT_TYPES = {
    "LP", "2xLP", "3xLP", "4xLP", "5xLP", "6xLP",
    "7\"", "10\"", "12\"",
    "CD", "2xCD", "3xCD",
    "Cass", "8-Track",
    "Box Set", "Flexi", "Lathe Cut",
}

# Pressing/release types — also WHAT (type of release)
PRESSING_TYPES = {
    "Album", "Single", "EP", "Maxi", "Comp",
    "RE", "RM", "Club", "Ltd", "Promo",
    "RSD", "Unofficial", "Bootleg",
}

# Mono/Stereo — WHAT (a different mix is a different artifact)
MIX_TYPES = {"Mono", "Stereo", "Quad"}

# Vinyl color codes — WHAT (physical attribute of the object)
COLOR_CODES = {
    "Whi": "White Vinyl",
    "Blu": "Blue Vinyl",
    "Red": "Red Vinyl",
    "Gre": "Green Vinyl",
    "Yel": "Yellow Vinyl",
    "Ora": "Orange Vinyl",
    "Pur": "Purple Vinyl",
    "Pin": "Pink Vinyl",
    "Cle": "Clear Vinyl",
    "Gol": "Gold Vinyl",
    "Sil": "Silver Vinyl",
    "Bla": "Black Vinyl",
    "Fla": "Flame Vinyl",
    "Mar": "Marbled Vinyl",
    "Spl": "Splatter Vinyl",
    "Pic": "Picture Disc",
    "Mon": "Mono",    # Discogs sometimes uses Mon for Mono
}

# Weight — borderline WHAT/HOW, keeping in WHAT as physical attribute
WEIGHT_CODES = {"180", "200", "140", "150", "160", "220"}

# Misc codes to silently skip (Discogs regional/distribution codes)
SKIP_CODES = {"San", "Ter", "Ind", "Pit", "Ind"}


def _parse_format(format_str: str) -> dict:
    """
    Parse a Discogs format string into semantic categories.
    e.g. "LP, Album, Mono, 180" → {formats, pressing_types, mixes, colors, weights}
    """
    if not format_str:
        return {}

    parts = [p.strip() for p in format_str.split(",") if p.strip()]
    result = {
        "formats":        [],
        "pressing_types": [],
        "mixes":          [],
        "colors":         [],
        "weights":        [],
    }

    for part in parts:
        if part in FORMAT_TYPES:
            result["formats"].append(part)
        elif part in PRESSING_TYPES:
            result["pressing_types"].append(part)
        elif part in MIX_TYPES:
            result["mixes"].append(part)
        elif part in COLOR_CODES:
            result["colors"].append(COLOR_CODES[part])
        elif part in WEIGHT_CODES:
            result["weights"].append(f"{part}g")
        elif part in SKIP_CODES:
            pass  # regional/distribution codes — not semantically useful
        # else: silently skip unrecognised components

    return result


def _parse_artist(artist_str: str) -> Optional[str]:
    """Strip Discogs disambiguation numbers like (4) from artist names."""
    if not artist_str:
        return None
    return re.sub(r"\s*\(\d+\)\s*$", "", str(artist_str)).strip()


def _parse_labels(label_str: str) -> list[str]:
    """Deduplicate comma-separated label values."""
    if not label_str:
        return []
    parts = [p.strip() for p in str(label_str).split(",") if p.strip()]
    return list(dict.fromkeys(parts))  # deduplicate preserving order


def _parse_catalog(catalog_str: str) -> Optional[str]:
    """Use first catalog number if comma-separated."""
    if not catalog_str:
        return None
    return str(catalog_str).split(",")[0].strip() or None


def _parse_year(value: str) -> Optional[str]:
    """Extract 4-digit year from a date string."""
    if not value or str(value).strip() in ("", "0", "nan"):
        return None
    s = str(value).strip()
    m = re.search(r"\b(\d{4})\b", s)
    return m.group(1) if m else None


def _translate_discogs_row(row: dict) -> dict:
    """
    Translate a single Discogs CSV row into a canonical entity dict.

    Returns:
        {
            entity_id: str,
            facts: [{dimension, semantic_key, value}],
            _label: str,
            _sublabel: str,
            _nucleus: str,
        }
    """
    release_id = str(row.get("release_id", "")).strip()
    if not release_id or release_id == "nan":
        return None

    artist    = _parse_artist(str(row.get("Artist", "")))
    title     = str(row.get("Title", "")).strip() or None
    labels    = _parse_labels(str(row.get("Label", "")))
    catalog   = _parse_catalog(str(row.get("Catalog#", "")))
    released  = _parse_year(str(row.get("Released", "")))
    added     = _parse_year(str(row.get("Date Added", "")))
    media_cond  = str(row.get("Collection Media Condition",  "")).strip() or None
    sleeve_cond = str(row.get("Collection Sleeve Condition", "")).strip() or None
    fmt       = _parse_format(str(row.get("Format", "")))

    facts = []

    # WHO — agents
    if artist:
        facts.append({"dimension": "who", "semantic_key": "artist", "value": artist})
    for label in labels:
        facts.append({"dimension": "who", "semantic_key": "label", "value": label})

    # WHAT — what the object is
    if title:
        facts.append({"dimension": "what", "semantic_key": "title", "value": title})
    if catalog:
        facts.append({"dimension": "what", "semantic_key": "catalog_no", "value": catalog})
    for f in fmt.get("formats", []):
        facts.append({"dimension": "what", "semantic_key": "format", "value": f})
    for p in fmt.get("pressing_types", []):
        facts.append({"dimension": "what", "semantic_key": "pressing_type", "value": p})
    for m in fmt.get("mixes", []):
        facts.append({"dimension": "what", "semantic_key": "mix", "value": m})
    for c in fmt.get("colors", []):
        facts.append({"dimension": "what", "semantic_key": "color", "value": c})
    for w in fmt.get("weights", []):
        facts.append({"dimension": "what", "semantic_key": "weight", "value": w})

    # WHEN — temporal
    if released:
        facts.append({"dimension": "when", "semantic_key": "released", "value": released})
    if added:
        facts.append({"dimension": "when", "semantic_key": "year_added", "value": added})

    # HOW — condition of this specific copy
    if media_cond:
        facts.append({"dimension": "how", "semantic_key": "media_condition",  "value": media_cond})
    if sleeve_cond:
        facts.append({"dimension": "how", "semantic_key": "sleeve_condition", "value": sleeve_cond})

    # Filter empty/null values
    facts = [f for f in facts if f["value"] and f["value"] != "nan"]

    return {
        "entity_id": f"discogs:{release_id}",
        "facts":     facts,
        "_nucleus":  release_id,
        "_label":    title or release_id,
        "_sublabel": " · ".join(filter(None, [artist, released])),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Layer A — canonical row compilation
# ─────────────────────────────────────────────────────────────────────────────

TRANSLATORS = {
    "discogs": _translate_discogs_row,
}


# ─────────────────────────────────────────────────────────────────────────────
# Disney translators
#
# Five tables, one entity type: Disney movies.
# Nucleus: movie title, normalized to a slug.
# Each table contributes different facts to the same entity_id.
#
# Title normalization handles known variants:
#   "One Hundred and One Dalmatians" → same entity as "101 Dalmatians"
#   Leading/trailing whitespace and newlines stripped.
# ─────────────────────────────────────────────────────────────────────────────

# Known title aliases — map variant → canonical
_DISNEY_TITLE_ALIASES = {
    "one hundred and one dalmatians": "101 dalmatians",
}


def _disney_entity_id(title: str) -> Optional[str]:
    """
    Normalize a Disney movie title to a stable entity_id.
    Strips whitespace, lowercases, applies known aliases, then slugifies.
    """
    if not title or str(title).strip() in ("", "nan"):
        return None
    clean = str(title).strip().lower()
    clean = _DISNEY_TITLE_ALIASES.get(clean, clean)
    slug  = re.sub(r"[^a-z0-9]+", "_", clean).strip("_")
    return f"disney:{slug}"


def _disney_title_display(title: str) -> Optional[str]:
    """Return cleaned display title (no leading newlines, stripped)."""
    if not title or str(title).strip() in ("", "nan"):
        return None
    return str(title).strip()


def _parse_gross(value: str) -> Optional[str]:
    """Strip $ and commas from gross figures. Return None if empty."""
    if not value or str(value).strip() in ("", "nan"):
        return None
    return re.sub(r"[$,]", "", str(value).strip()) or None


def _translate_disney_characters_row(row: dict) -> Optional[dict]:
    """
    disney-characters.csv
    Columns: index, movie_title, release_date, hero, villian, song
    """
    title = _disney_title_display(str(row.get("movie_title", "")))
    eid   = _disney_entity_id(str(row.get("movie_title", "")))
    if not eid:
        return None

    hero    = str(row.get("hero",    "")).strip() or None
    villain = str(row.get("villian", "")).strip() or None  # note: typo in source
    song    = str(row.get("song",    "")).strip() or None
    year    = _parse_year(str(row.get("release_date", "")))

    facts = []
    if hero    and hero    != "nan": facts.append({"dimension": "who",  "semantic_key": "hero",    "value": hero})
    if villain and villain != "nan": facts.append({"dimension": "who",  "semantic_key": "villain", "value": villain})
    if song    and song    != "nan": facts.append({"dimension": "what", "semantic_key": "song",    "value": song})
    if title:                        facts.append({"dimension": "what", "semantic_key": "title",   "value": title})
    if year:                         facts.append({"dimension": "when", "semantic_key": "released","value": year})

    return {
        "entity_id": eid,
        "facts":     facts,
        "_nucleus":  title,
        "_label":    title or eid,
        "_sublabel": year or "",
    }


def _translate_disney_director_row(row: dict) -> Optional[dict]:
    """
    disney-director.csv
    Columns: index, name, director
    """
    eid      = _disney_entity_id(str(row.get("name", "")))
    title    = _disney_title_display(str(row.get("name", "")))
    director = str(row.get("director", "")).strip() or None

    if not eid:
        return None

    # Filter out non-name values from source data quality issues
    _DIRECTOR_SKIP = {"full credits", "nan", "", "n/a", "unknown"}
    if director and director.lower() in _DIRECTOR_SKIP:
        director = None

    facts = []
    if director:
        facts.append({"dimension": "who", "semantic_key": "director", "value": director})
    if title:
        facts.append({"dimension": "what", "semantic_key": "title", "value": title})

    return {
        "entity_id": eid,
        "facts":     facts,
        "_nucleus":  title,
        "_label":    title or eid,
        "_sublabel": director or "",
    }


def _translate_disney_voice_actors_row(row: dict) -> Optional[dict]:
    """
    disney-voice-actors.csv
    Columns: index, character, voice-actor, movie
    """
    eid         = _disney_entity_id(str(row.get("movie", "")))
    title       = _disney_title_display(str(row.get("movie", "")))
    character   = str(row.get("character",   "")).strip() or None
    voice_actor = str(row.get("voice-actor", "")).strip() or None

    if not eid:
        return None

    facts = []
    if voice_actor and voice_actor != "nan":
        facts.append({"dimension": "who", "semantic_key": "voice_actor", "value": voice_actor})
    if character and character != "nan":
        facts.append({"dimension": "who", "semantic_key": "character", "value": character})
    if title:
        facts.append({"dimension": "what", "semantic_key": "title", "value": title})

    return {
        "entity_id": eid,
        "facts":     facts,
        "_nucleus":  title,
        "_label":    title or eid,
        "_sublabel": "",
    }


def _translate_disney_total_gross_row(row: dict) -> Optional[dict]:
    """
    disney_movies_total_gross.csv
    Columns: index, movie_title, release_date, genre, MPAA_rating,
             total_gross, inflation_adjusted_gross
    """
    title = _disney_title_display(str(row.get("movie_title", "")))
    eid   = _disney_entity_id(str(row.get("movie_title", "")))
    if not eid:
        return None

    genre    = str(row.get("genre",       "")).strip() or None
    mpaa     = str(row.get("MPAA_rating", "")).strip() or None
    gross    = _parse_gross(str(row.get("total_gross",             "")))
    adj      = _parse_gross(str(row.get("inflation_adjusted_gross","")))
    year     = _parse_year(str(row.get("release_date", "")))

    facts = []
    if title:                        facts.append({"dimension": "what", "semantic_key": "title",                    "value": title})
    if genre and genre != "nan":     facts.append({"dimension": "what", "semantic_key": "genre",                    "value": genre})
    if mpaa  and mpaa  != "nan":     facts.append({"dimension": "what", "semantic_key": "mpaa_rating",              "value": mpaa})
    if gross and gross != "nan":     facts.append({"dimension": "what", "semantic_key": "total_gross",              "value": gross})
    if adj   and adj   != "nan":     facts.append({"dimension": "what", "semantic_key": "inflation_adjusted_gross", "value": adj})
    if year:                         facts.append({"dimension": "when", "semantic_key": "released",                 "value": year})

    return {
        "entity_id": eid,
        "facts":     facts,
        "_nucleus":  title,
        "_label":    title or eid,
        "_sublabel": year or "",
    }


TRANSLATORS.update({
    "disney_characters":   _translate_disney_characters_row,
    "disney_director":     _translate_disney_director_row,
    "disney_voice_actors": _translate_disney_voice_actors_row,
    "disney_total_gross":  _translate_disney_total_gross_row,
})


# ─────────────────────────────────────────────────────────────────────────────
# Shibuya translator
#
# Source: shibuya_staging table exported to CSV
# Nucleus: eventid (UUID)
# Columns used: eventid, artist, title, host, release_year, date, day_of_week,
#               self-titled (exported as "self-titled" header)
# ─────────────────────────────────────────────────────────────────────────────

def _parse_session_year(date_str: str) -> Optional[str]:
    """Extract year from MM/DD/YYYY date string."""
    if not date_str or str(date_str).strip() in ("", "nan"):
        return None
    parts = str(date_str).strip().split("/")
    if len(parts) == 3:
        return parts[2].strip()
    return None


def _translate_shibuya_row(row: dict) -> Optional[dict]:
    """
    shibuya_staging CSV export
    Columns: eventid, artist, title, host, release_year, date, day_of_week, self-titled
    """
    eventid = str(row.get("eventid", "")).strip()
    if not eventid or eventid in ("nan", "eventid"):
        return None

    artist       = str(row.get("artist",       "")).strip() or None
    title        = str(row.get("title",        "")).strip() or None
    host         = str(row.get("host",         "")).strip() or None
    release_year = str(row.get("release_year", "")).strip() or None
    date_str     = str(row.get("date",         "")).strip() or None
    day_of_week  = str(row.get("day_of_week",  "")).strip() or None
    self_titled  = str(row.get("self-titled",  "0")).strip()

    session_year = _parse_session_year(date_str)

    facts = []

    # WHO — agents
    if artist    and artist    != "nan": facts.append({"dimension": "who",  "semantic_key": "artist",       "value": artist})
    if host      and host      != "nan": facts.append({"dimension": "who",  "semantic_key": "host",         "value": host})

    # WHAT — what was played
    if title     and title     != "nan": facts.append({"dimension": "what", "semantic_key": "title",        "value": title})
    if self_titled == "1":               facts.append({"dimension": "what", "semantic_key": "self_titled",  "value": "true"})

    # WHEN — temporal
    if release_year and release_year != "nan": facts.append({"dimension": "when", "semantic_key": "release_year",  "value": release_year})
    if session_year:                           facts.append({"dimension": "when", "semantic_key": "session_year",  "value": session_year})
    if day_of_week  and day_of_week  != "nan": facts.append({"dimension": "when", "semantic_key": "day_of_week",   "value": day_of_week})

    return {
        "entity_id": f"shibuya:{eventid}",
        "facts":     facts,
        "_nucleus":  eventid,
        "_label":    title or eventid,
        "_sublabel": " · ".join(filter(None, [artist, release_year])),
    }


TRANSLATORS["shibuya"] = _translate_shibuya_row


def _compile_rows(
    df: pd.DataFrame,
    translator_fn,
    lens_id: str,
    translator_version: str,
) -> tuple[list[tuple], list[tuple], list[str]]:
    """
    Compile a DataFrame into canonical spoke rows and meta rows.

    Returns:
        spoke_rows   — list of (entity_id, dimension, semantic_key, value, coordinate, lens_id, translator_version)
        meta_rows    — list of (entity_id, nucleus, label, sublabel)
        warnings     — list of warning strings
    """
    spoke_rows = []
    meta_rows  = []
    warnings   = []
    seen_coords: set = set()  # deduplicate (entity_id, coordinate) across tables

    for i, row in enumerate(df.to_dict(orient="records")):
        entity = translator_fn(row)
        if not entity:
            warnings.append(f"Row {i}: skipped — no entity_id")
            continue

        eid = entity["entity_id"]

        for fact in entity["facts"]:
            dim = fact["dimension"].lower()
            key = fact["semantic_key"].lower()
            val = str(fact["value"]).strip()
            if not val or val == "nan":
                continue
            coord    = f"{dim.upper()}|{key}|{val}"
            dedup_key = (eid, coord)
            if dedup_key in seen_coords:
                continue
            seen_coords.add(dedup_key)
            spoke_rows.append((eid, dim, key, val, coord, lens_id, translator_version))

        meta_rows.append((
            eid,
            entity.get("_nucleus"),
            entity.get("_label"),
            entity.get("_sublabel"),
        ))

    return spoke_rows, meta_rows, warnings


# ─────────────────────────────────────────────────────────────────────────────
# Layer B — DuckDB emitter
# ─────────────────────────────────────────────────────────────────────────────

def _emit_duckdb(
    spoke_rows: list[tuple],
    meta_rows:  list[tuple],
    output_path: str,
    overwrite:  bool = False,
) -> dict:
    """
    Write spoke rows and meta rows to a DuckDB file.
    Returns stats dict.

    overwrite=True: drop and recreate snf_spoke and snf_meta before inserting.
                    Use for single-table rebuilds.
    overwrite=False (default): append. Use for multi-table ingest (I17).
    """
    conn = duckdb.connect(output_path)

    if overwrite:
        conn.execute("DROP TABLE IF EXISTS snf_spoke")
        conn.execute("DROP TABLE IF EXISTS snf_meta")

    conn.execute(SPOKE_DDL)
    conn.execute(META_DDL)
    for idx in SPOKE_INDEXES:
        conn.execute(idx)

    # Insert spoke rows in chunks — OR IGNORE deduplicates cross-table builds
    CHUNK = 500
    for i in range(0, len(spoke_rows), CHUNK):
        chunk = spoke_rows[i:i + CHUNK]
        conn.executemany(
            "INSERT OR IGNORE INTO snf_spoke "
            "(entity_id, dimension, semantic_key, value, coordinate, lens_id, translator_version) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            chunk
        )

    # Insert meta rows
    for i in range(0, len(meta_rows), CHUNK):
        chunk = meta_rows[i:i + CHUNK]
        conn.executemany(
            "INSERT OR REPLACE INTO snf_meta (entity_id, nucleus, label, sublabel) "
            "VALUES (?, ?, ?, ?)",
            chunk
        )

    # Stats
    facts_by_dim = {}
    for dim in DIMENSIONS:
        row = conn.execute(
            "SELECT COUNT(*) FROM snf_spoke WHERE dimension = ?", [dim]
        ).fetchone()
        facts_by_dim[dim] = row[0] if row else 0

    entity_count = conn.execute(
        "SELECT COUNT(DISTINCT entity_id) FROM snf_spoke"
    ).fetchone()[0]

    conn.close()

    return {
        "entity_count": entity_count,
        "fact_count":   len(spoke_rows),
        "facts_by_dim": facts_by_dim,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Layer B — CSV emitter
# ─────────────────────────────────────────────────────────────────────────────

def _emit_csv(
    spoke_rows: list[tuple],
    meta_rows:  list[tuple],
    output_dir: str,
    lens_id: str,
) -> dict:
    """
    Write spoke rows to per-dimension CSV files in snf-peirce spoke format.
    Compatible with substrate_from_spoke_dir() in reckoner_api.py.
    """
    import csv
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    by_dim = {dim: [] for dim in DIMENSIONS}
    for row in spoke_rows:
        eid, dim, key, val, coord, lid, tver = row
        by_dim[dim].append(row)

    facts_by_dim = {}
    for dim, rows in by_dim.items():
        if not rows:
            facts_by_dim[dim] = 0
            continue
        csv_path = out / f"snf_{dim}.csv"
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["entity_id", "dimension", "semantic_key", "value", "coordinate", "lens_id", "translator_version"])
            writer.writerows(rows)
        facts_by_dim[dim] = len(rows)

    # Write lens.json so substrate_from_spoke_dir can read lens_id
    lens_meta = {"lens_id": lens_id}
    with open(out / "lens.json", "w") as f:
        json.dump(lens_meta, f, indent=2)

    entity_count = len({r[0] for r in spoke_rows})

    return {
        "entity_count": entity_count,
        "fact_count":   len(spoke_rows),
        "facts_by_dim": facts_by_dim,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Layer B — Postgres import emitter (C4)
# ─────────────────────────────────────────────────────────────────────────────

def _emit_postgres_import(
    spoke_rows:         list[tuple],
    meta_rows:          list[tuple],
    output_dir:         str,
    lens_id:            str,
    translator_version: str,
    schema:             str = "public",
) -> dict:
    """
    Write a complete Postgres import package to output_dir.

    Package contents (per Model Builder brief section 5.4):
        00_ddl.sql          — CREATE TABLE, indexes, snf_hub, snf_affordances view
        snf_who.csv         — COPY-ready spoke rows (7 columns)
        snf_what.csv
        snf_when.csv
        snf_where.csv
        snf_why.csv
        snf_how.csv
        snf_hub.csv         — display/provenance data
        load.sh             — one-command load script (DDL + COPY)
        verification_report.json

    The person runs:
        psql -d mydb -f 00_ddl.sql
        psql -d mydb -f load.sh   (or run COPY commands manually)
    """
    import csv as csv_mod
    import json as json_mod

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    s = schema  # schema qualifier shorthand

    # ── Split spoke rows by dimension ────────────────────────────────────────
    by_dim = {dim: [] for dim in DIMENSIONS}
    for row in spoke_rows:
        eid, dim, key, val, coord, lid, tver = row
        by_dim[dim].append(row)

    entity_ids = sorted({r[0] for r in spoke_rows})

    # ── Write spoke CSVs ─────────────────────────────────────────────────────
    facts_by_dim = {}
    for dim, rows in by_dim.items():
        csv_path = out / f"snf_{dim}.csv"
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv_mod.writer(f)
            writer.writerow(["entity_id", "dimension", "semantic_key",
                             "value", "coordinate", "lens_id", "translator_version"])
            writer.writerows(rows)
        facts_by_dim[dim] = len(rows)

    # ── Write snf_hub.csv ────────────────────────────────────────────────────
    hub_path = out / "snf_hub.csv"
    with open(hub_path, "w", newline="", encoding="utf-8") as f:
        writer = csv_mod.writer(f)
        writer.writerow(["entity_id", "nucleus", "label", "sublabel",
                        "lens_id", "translator_version"])
        for eid, nucleus, label, sublabel in meta_rows:
            writer.writerow([eid, nucleus or "", label or eid,
                            sublabel or "", lens_id, translator_version])

    # ── Write 00_ddl.sql ─────────────────────────────────────────────────────
    ddl_path = out / "00_ddl.sql"
    ddl = f"""-- =============================================================================
-- SNF Substrate DDL — generated by SNF Model Builder
-- lens_id:            {lens_id}
-- translator_version: {translator_version}
-- schema:             {s}
--
-- Run this first, then run load.sh (or the COPY commands manually).
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS "{s}";

-- Spoke tables (one per dimension)
"""
    for dim in DIMENSIONS:
        ddl += f"""
CREATE TABLE IF NOT EXISTS "{s}"."snf_{dim}" (
    entity_id          TEXT        NOT NULL,
    dimension          TEXT        NOT NULL,
    semantic_key       TEXT        NOT NULL,
    value              TEXT        NOT NULL,
    coordinate         TEXT        NOT NULL,
    lens_id            TEXT        NOT NULL,
    translator_version TEXT        NOT NULL
);

CREATE INDEX IF NOT EXISTS "idx_{dim}_coordinate"
    ON "{s}"."snf_{dim}" (coordinate);
CREATE INDEX IF NOT EXISTS "idx_{dim}_entity_id"
    ON "{s}"."snf_{dim}" (entity_id);
CREATE INDEX IF NOT EXISTS "idx_{dim}_dim_key_val"
    ON "{s}"."snf_{dim}" (dimension, semantic_key, value);
"""

    ddl += f"""
-- Hub table (display + provenance)
CREATE TABLE IF NOT EXISTS "{s}"."snf_hub" (
    entity_id          TEXT        PRIMARY KEY,
    nucleus            TEXT,
    label              TEXT,
    sublabel           TEXT,
    lens_id            TEXT        NOT NULL,
    translator_version TEXT        NOT NULL
);

-- Affordances materialized view
-- Required for interactive speed at scale (50K+ entities).
-- Refresh with: REFRESH MATERIALIZED VIEW "{s}"."snf_affordances";
CREATE MATERIALIZED VIEW IF NOT EXISTS "{s}"."snf_affordances" AS
SELECT
    dimension,
    semantic_key,
    value,
    coordinate,
    COUNT(DISTINCT entity_id) AS entity_count,
    COUNT(*)                  AS fact_count
FROM (
"""
    union_parts = []
    for dim in DIMENSIONS:
        union_parts.append(
            f'    SELECT entity_id, dimension, semantic_key, value, coordinate '
            f'FROM "{s}"."snf_{dim}"'
        )
    ddl += "\n    UNION ALL\n".join(union_parts)
    ddl += f"""
) all_spokes
GROUP BY dimension, semantic_key, value, coordinate
WITH DATA;

CREATE INDEX IF NOT EXISTS "idx_affordances_dim_key"
    ON "{s}"."snf_affordances" (dimension, semantic_key);
CREATE INDEX IF NOT EXISTS "idx_affordances_coordinate"
    ON "{s}"."snf_affordances" (coordinate);
"""

    with open(ddl_path, "w", encoding="utf-8") as f:
        f.write(ddl)

    # ── Write load.sh ─────────────────────────────────────────────────────────
    # Uses \COPY (client-side) so it works without superuser privileges.
    # The user substitutes their connection string.
    load_path = out / "load.sh"
    copy_cmds = ""
    for dim in DIMENSIONS:
        if facts_by_dim.get(dim, 0) > 0:
            copy_cmds += (
                f'psql "$DATABASE_URL" -c '
                f'"\\COPY \\"{s}\\".\\\"snf_{dim}\\\" '
                f'(entity_id, dimension, semantic_key, value, coordinate, lens_id, translator_version) '
                f'FROM \'$(pwd)/snf_{dim}.csv\' CSV HEADER ENCODING \'UTF8\'"\n'
            )
    copy_cmds += (
        f'psql "$DATABASE_URL" -c '
        f'"\\COPY \\"{s}\\".\\\"snf_hub\\\" '
        f'(entity_id, nucleus, label, sublabel, lens_id, translator_version) '
        f'FROM \'$(pwd)/snf_hub.csv\' CSV HEADER ENCODING \'UTF8\'"\n'
    )

    load_script = f"""#!/bin/bash
# SNF Model Builder — Postgres load script
# lens_id:            {lens_id}
# translator_version: {translator_version}
#
# Usage:
#   export DATABASE_URL=postgresql://user:pass@localhost:5432/mydb
#   bash load.sh
#
# Requires psql on PATH. Runs DDL then COPY for all spoke tables.

set -e
SCRIPT_DIR="$(cd "$(dirname "${{BASH_SOURCE[0]}}")" && pwd)"
cd "$SCRIPT_DIR"

if [ -z "$DATABASE_URL" ]; then
  echo "ERROR: DATABASE_URL not set."
  echo "  export DATABASE_URL=postgresql://user:pass@localhost:5432/mydb"
  exit 1
fi

echo "Running DDL..."
psql "$DATABASE_URL" -f 00_ddl.sql

echo "Loading spoke tables..."
{copy_cmds}
echo "Done. Substrate loaded."
echo "Verify: psql \\"$DATABASE_URL\\" -c \\"SELECT COUNT(*) FROM \\"{s}\\".snf_hub;\\""
"""
    with open(load_path, "w", encoding="utf-8") as f:
        f.write(load_script)

    # ── Write verification_report.json ────────────────────────────────────────
    sample_coords = {}
    for dim, rows in by_dim.items():
        if rows:
            sample_coords[dim] = [r[4] for r in rows[:3]]  # first 3 coordinates

    report = {
        "lens_id":            lens_id,
        "translator_version": translator_version,
        "schema":             schema,
        "entity_count":       len(entity_ids),
        "fact_count":         len(spoke_rows),
        "facts_by_dim":       facts_by_dim,
        "hub_rows":           len(meta_rows),
        "sample_coordinates": sample_coords,
        "provenance_check": {
            "lens_id_present":            True,
            "translator_version_present": True,
            "entity_ids_sample":          entity_ids[:5],
        },
        "files": [
            "00_ddl.sql",
            *[f"snf_{dim}.csv" for dim in DIMENSIONS if facts_by_dim.get(dim, 0) > 0],
            "snf_hub.csv",
            "load.sh",
            "verification_report.json",
        ]
    }
    report_path = out / "verification_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json_mod.dump(report, f, indent=2)

    return {
        "entity_count": len(entity_ids),
        "fact_count":   len(spoke_rows),
        "facts_by_dim": facts_by_dim,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Layer C — compile_job (service boundary)
# ─────────────────────────────────────────────────────────────────────────────

def compile_job(spec: dict) -> dict:
    """
    Main entry point. Accepts a BuildSpec dict, returns a BuildResult dict.

    BuildSpec:
        source              str  — path to input CSV
        translator          str  — translator name (discogs | ...)
        lens_id             str  — lens identity
        translator_version  str  — semver string
        into                str  — duckdb://path | csv://dir
        overwrite           bool — drop and recreate tables before inserting (single-table rebuild)
        verbose             bool

    BuildResult:
        success       bool
        output_path   str
        entity_count  int
        fact_count    int
        facts_by_dim  dict
        errors        list[str]
        warnings      list[str]
    """
    errors   = []
    warnings = []

    # Validate spec
    source             = spec.get("source")
    translator_name    = spec.get("translator", "discogs")
    lens_id            = spec.get("lens_id", "unknown")
    translator_version = spec.get("translator_version", "0.0.0")
    into               = spec.get("into", "")
    overwrite          = spec.get("overwrite", False)
    verbose            = spec.get("verbose", False)

    if not source:
        return {"success": False, "errors": ["source is required"], "warnings": []}
    if not into:
        return {"success": False, "errors": ["into is required"], "warnings": []}

    translator_fn = TRANSLATORS.get(translator_name)
    if not translator_fn:
        return {
            "success": False,
            "errors":  [f"Unknown translator: '{translator_name}'. Available: {list(TRANSLATORS)}"],
            "warnings": [],
        }

    # Load source
    if verbose:
        print(f"Loading {source}...")
    try:
        df = pd.read_csv(source)
    except Exception as e:
        return {"success": False, "errors": [f"Could not read source: {e}"], "warnings": []}

    if verbose:
        print(f"  {len(df):,} rows loaded")

    # Layer A — compile
    t0 = time.perf_counter()
    spoke_rows, meta_rows, row_warnings = _compile_rows(
        df, translator_fn, lens_id, translator_version
    )
    warnings.extend(row_warnings)
    compile_ms = (time.perf_counter() - t0) * 1000

    if verbose:
        print(f"  {len(spoke_rows):,} facts compiled  ({compile_ms:.0f}ms)")

    if not spoke_rows:
        return {"success": False, "errors": ["No facts produced — check translator and input"], "warnings": warnings}

    # Layer B — emit
    t1 = time.perf_counter()

    if into.startswith("duckdb://"):
        output_path = into[len("duckdb://"):]
        # Warn if file exists and overwrite not set
        if Path(output_path).exists() and not overwrite:
            warnings.append(
                f"Output file '{output_path}' already exists — appending (multi-table mode). "
                f"Use --overwrite to replace instead."
            )
        stats = _emit_duckdb(spoke_rows, meta_rows, output_path, overwrite=overwrite)
        emit_ms = (time.perf_counter() - t1) * 1000
        if verbose:
            print(f"  DuckDB written: {output_path}  ({emit_ms:.0f}ms)")

    elif into.startswith("csv://"):
        output_path = into[len("csv://"):]
        stats = _emit_csv(spoke_rows, meta_rows, output_path, lens_id)
        emit_ms = (time.perf_counter() - t1) * 1000
        if verbose:
            print(f"  CSV spoke files written: {output_path}  ({emit_ms:.0f}ms)")

    elif into.startswith("postgres-import://"):
        output_path = into[len("postgres-import://"):]
        pg_schema   = spec.get("pg_schema", "public")
        stats = _emit_postgres_import(
            spoke_rows, meta_rows, output_path,
            lens_id, translator_version, schema=pg_schema
        )
        emit_ms = (time.perf_counter() - t1) * 1000
        if verbose:
            print(f"  Postgres import package written: {output_path}  ({emit_ms:.0f}ms)")

    else:
        return {
            "success": False,
            "errors":  [f"Unsupported into= target: '{into}'. Supported: duckdb://, csv://, postgres-import://"],
            "warnings": warnings,
        }

    return {
        "success":            True,
        "output_path":        output_path,
        "entity_count":       stats["entity_count"],
        "fact_count":         stats["fact_count"],
        "facts_by_dim":       stats["facts_by_dim"],
        "errors":             errors,
        "warnings":           warnings,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Info command — inspect an existing DuckDB substrate
# ─────────────────────────────────────────────────────────────────────────────

def cmd_info(db_path: str):
    """Print a summary of an existing DuckDB substrate."""
    conn = duckdb.connect(db_path, read_only=True)

    try:
        entity_count = conn.execute(
            "SELECT COUNT(DISTINCT entity_id) FROM snf_spoke"
        ).fetchone()[0]
        fact_count = conn.execute("SELECT COUNT(*) FROM snf_spoke").fetchone()[0]

        # Check provenance
        sample = conn.execute(
            "SELECT lens_id, translator_version FROM snf_spoke LIMIT 1"
        ).fetchone()
        lens_id            = sample[0] if sample else "?"
        translator_version = sample[1] if sample else "?"

        print(f"\n{db_path}")
        print(f"  lens_id:            {lens_id}")
        print(f"  translator_version: {translator_version}")
        print(f"  entities:           {entity_count:,}")
        print(f"  facts:              {fact_count:,}")
        print()
        print("  Dimension       Facts")
        print("  ─────────────  ──────────")
        for dim in DIMENSIONS:
            n = conn.execute(
                "SELECT COUNT(*) FROM snf_spoke WHERE dimension = ?", [dim]
            ).fetchone()[0]
            if n > 0:
                print(f"  {dim.upper():<13}  {n:>10,}")
        print()

    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="SNF Model Builder — translate structured data into an SNF substrate",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python model_builder.py \\
      --input  my_collection.csv \\
      --lens   discogs_v1.lens.json \\
      --into   duckdb://my_collection.duckdb \\
      --translator discogs

  python model_builder.py \\
      --input  my_collection.csv \\
      --lens   discogs_v1.lens.json \\
      --into   csv://my_spoke_dir \\
      --translator discogs

  python model_builder.py --info my_collection.duckdb
        """
    )

    parser.add_argument("--input",       "-i", help="Path to input CSV")
    parser.add_argument("--lens",        "-l", help="Path to lens JSON file")
    parser.add_argument("--into",             help="Output target: duckdb://path or csv://dir")
    parser.add_argument("--translator",  "-t", default="discogs",
                        help=f"Translator name. Available: {list(TRANSLATORS)} (default: discogs)")
    parser.add_argument("--lens-id",          default=None,
                        help="Override lens_id (otherwise read from lens JSON)")
    parser.add_argument("--translator-version", default="1.0.0",
                        help="Translator version string (default: 1.0.0)")
    parser.add_argument("--pg-schema",          default="public",
                        help="Postgres schema name for postgres-import:// output (default: public)")
    parser.add_argument("--verbose",     "-v", action="store_true")
    parser.add_argument("--overwrite",         action="store_true",
                        help="Drop and recreate tables before inserting. Use for single-table rebuilds. Default is append (for multi-table ingest).")
    parser.add_argument("--info",             metavar="DB_PATH",
                        help="Inspect an existing DuckDB substrate and exit")

    args = parser.parse_args()

    # Info mode
    if args.info:
        cmd_info(args.info)
        return

    # Build mode
    if not args.input:
        parser.error("--input is required")
    if not args.into:
        parser.error("--into is required")

    # Resolve lens_id
    lens_id = args.lens_id
    if not lens_id and args.lens:
        try:
            with open(args.lens) as f:
                lens_data = json.load(f)
            lens_id = lens_data.get("lens_id")
        except Exception as e:
            print(f"Warning: could not read lens file: {e}")
    if not lens_id:
        print("Error: lens_id required. Pass --lens-id or provide a lens JSON with lens_id field.")
        sys.exit(1)

    spec = {
        "source":             args.input,
        "translator":         args.translator,
        "lens_id":            lens_id,
        "translator_version": args.translator_version,
        "into":               args.into,
        "overwrite":          args.overwrite,
        "pg_schema":          args.pg_schema,
        "verbose":            args.verbose,
    }

    if args.verbose:
        print(f"\nSNF Model Builder")
        print(f"  input:              {args.input}")
        print(f"  translator:         {args.translator}")
        print(f"  lens_id:            {lens_id}")
        print(f"  translator_version: {args.translator_version}")
        print(f"  into:               {args.into}")
        print(f"  overwrite:          {args.overwrite}")
        print()

    result = compile_job(spec)

    if not result["success"]:
        print("Build failed:")
        for e in result.get("errors", []):
            print(f"  ERROR: {e}")
        sys.exit(1)

    if result.get("warnings"):
        for w in result["warnings"]:
            print(f"  WARNING: {w}")

    print(f"\nBuild complete.")
    print(f"  output:    {result['output_path']}")
    print(f"  entities:  {result['entity_count']:,}")
    print(f"  facts:     {result['fact_count']:,}")
    print()
    print("  Dimension       Facts")
    print("  ─────────────  ──────────")
    for dim, count in result["facts_by_dim"].items():
        if count > 0:
            print(f"  {dim.upper():<13}  {count:>10,}")
    print()
    print(f"Try it:")
    print(f"  python model_builder.py --info {result['output_path']}")


if __name__ == "__main__":
    main()
