"""
lens.py — LensDraft, suggest(), load(), save(), validate()

Week 2 of the Python SNF package.

The lens is the declaration of what a dataset means.
This module is the Python-native authoring surface for that declaration.

Public API
----------
suggest(source, sample=100) -> LensDraft
    Infer field mappings from a DataFrame or CSV path using pandas
    dtype and cardinality inference. Returns an editable LensDraft.

load(path) -> dict
    Load a lens JSON file. Returns the raw lens dict.
    Raises LensValidationError if the file is not a valid lens.

save(lens, path)
    Write a lens dict to a JSON file.

validate(lens) -> dict
    Validate a lens dict.
    Returns {"valid": True} or {"valid": False, "errors": [...]}

LensDraft
---------
Editable lens authoring object. Renders as a table in Jupyter.

    draft = suggest(df)
    draft.map("Artist", "who", "artist")          # chainable
    draft.map("Released", "when", "released")
    draft.nucleus("release_id", prefix="discogs:release")
    draft.nucleus_composite(["client_id", "matter_id"],
                            separator="-",
                            prefix="legal:matter")
    lens = draft.to_lens(lens_id="discogs_v1", authority="abk")

Conformance
-----------
to_lens() produces JSON identical in schema to discogs_community_v1.json.
load() round-trips a JS-created lens without loss.
The coordinate_map, nucleus, stats, and declaration shapes are fixed
by the JS reference — do not change them without updating the JS tool.
"""

from __future__ import annotations

import json
import copy
from datetime import datetime, timezone
from pathlib import Path
from typing import Union

# pandas is required
try:
    import pandas as pd
except ImportError:
    raise ImportError("lens.py requires pandas. Install with: pip install pandas")


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

VALID_DIMENSIONS = {"who", "what", "when", "where", "why", "how"}

# Cardinality thresholds for suggest() inference
# Low cardinality = likely categorical/enum = good semantic key candidate
_HIGH_CARD_RATIO   = 0.8   # > 80% unique → probably a free-text or ID field
_LOW_CARD_RATIO    = 0.05  # < 5% unique  → likely enum/categorical
_NUCLEUS_UNIQUE    = 0.98  # > 98% unique → candidate nucleus


# ─────────────────────────────────────────────────────────────────────────────
# Errors
# ─────────────────────────────────────────────────────────────────────────────

class LensValidationError(ValueError):
    """Raised when a lens dict fails validation."""
    def __init__(self, errors):
        self.errors = errors if isinstance(errors, list) else [errors]
        super().__init__("; ".join(self.errors))


# ─────────────────────────────────────────────────────────────────────────────
# Suggest — pandas-powered field inference
# ─────────────────────────────────────────────────────────────────────────────

def _infer_mapping(col, series, n_rows):
    """
    Infer dimension and semantic_key for a single column.

    Returns a dict:
        {
            "column":        str,
            "dimension":     str | None,
            "semantic_key":  str | None,
            "confidence":    "high" | "medium" | "low",
            "reason":        str,
            "nucleus_candidate": bool,
        }
    """
    col_lower  = col.lower().replace(" ", "_")
    dtype      = series.dtype
    n_non_null = series.count()
    n_unique   = series.nunique()

    if n_rows == 0 or n_non_null == 0:
        return {"column": col, "dimension": None, "semantic_key": None,
                "confidence": "low", "reason": "empty column", "nucleus_candidate": False}

    card_ratio = n_unique / n_non_null

    # ── Nucleus candidate detection ──────────────────────────────────────────
    # High uniqueness + integer-like or name suggests ID field
    is_nucleus_candidate = (
        card_ratio >= _NUCLEUS_UNIQUE
        and n_non_null > 0
        and ("id" in col_lower or "key" in col_lower or "number" in col_lower
             or "num" in col_lower or "code" in col_lower
             or pd.api.types.is_integer_dtype(dtype))
    )

    # ── Date / time detection ────────────────────────────────────────────────
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return {"column": col, "dimension": "when", "semantic_key": _key_from_col(col_lower, "when"),
                "confidence": "high", "reason": "datetime dtype", "nucleus_candidate": False}

    # Numeric columns named year/date/released/added
    if pd.api.types.is_numeric_dtype(dtype):
        if any(t in col_lower for t in ("year", "date", "released", "added", "modified", "created")):
            return {"column": col, "dimension": "when", "semantic_key": _key_from_col(col_lower, "when"),
                    "confidence": "high", "reason": "numeric date/year column name", "nucleus_candidate": False}

    # String columns that look like dates
    # Note: newer pandas may use StringDtype not object for string columns
    is_string_col = dtype == object or pd.api.types.is_string_dtype(dtype)
    if is_string_col:
        date_words = ("year", "date", "released", "added", "modified", "created", "month", "period")
        if any(t in col_lower for t in date_words):
            return {"column": col, "dimension": "when", "semantic_key": _key_from_col(col_lower, "when"),
                    "confidence": "high", "reason": "date-like column name", "nucleus_candidate": False}

    # ── WHO detection ────────────────────────────────────────────────────────
    who_words = ("artist", "author", "creator", "person", "name", "label",
                 "publisher", "editor", "contributor", "photographer",
                 "director", "writer", "performer", "composer", "band",
                 "artist_name", "attorney", "client", "party")
    if any(t in col_lower for t in who_words):
        key = _key_from_col(col_lower, "who")
        conf = "high" if card_ratio < _HIGH_CARD_RATIO else "medium"
        reason = "WHO-like column name"
        if "label" in col_lower or "publisher" in col_lower:
            key = "publisher"
        elif "author" in col_lower or "artist" in col_lower:
            key = col_lower.replace("_name", "").replace("name", "artist").strip("_") or "artist"
        return {"column": col, "dimension": "who", "semantic_key": key,
                "confidence": conf, "reason": reason, "nucleus_candidate": False}

    # ── WHERE detection ──────────────────────────────────────────────────────
    where_words = ("city", "country", "region", "location", "office",
                   "address", "territory", "place", "venue", "room",
                   "state", "province", "district", "site")
    if any(t in col_lower for t in where_words):
        return {"column": col, "dimension": "where", "semantic_key": _key_from_col(col_lower, "where"),
                "confidence": "high", "reason": "WHERE-like column name", "nucleus_candidate": False}

    # ── WHY detection ────────────────────────────────────────────────────────
    why_words = ("reason", "intent", "purpose", "cause", "motive",
                 "category", "type", "matter_type", "case_type")
    if any(t in col_lower for t in why_words):
        return {"column": col, "dimension": "why", "semantic_key": _key_from_col(col_lower, "why"),
                "confidence": "medium", "reason": "WHY-like column name", "nucleus_candidate": False}

    # ── HOW detection ────────────────────────────────────────────────────────
    how_words = ("format", "medium", "method", "protocol", "carrier",
                 "transmission", "language", "encoding", "mode")
    if any(t in col_lower for t in how_words):
        return {"column": col, "dimension": "how", "semantic_key": _key_from_col(col_lower, "how"),
                "confidence": "high", "reason": "HOW-like column name", "nucleus_candidate": False}

    # ── WHAT — fallback for most columns ────────────────────────────────────
    what_words = ("title", "subject", "topic", "description", "content",
                  "genre", "tag", "keyword", "note", "rating", "score",
                  "condition", "status", "identifier", "catalog", "folder",
                  "series", "edition")
    if any(t in col_lower for t in what_words):
        return {"column": col, "dimension": "what", "semantic_key": _key_from_col(col_lower, "what"),
                "confidence": "high", "reason": "WHAT-like column name", "nucleus_candidate": False}

    # Numeric low-cardinality → probably a WHAT categorical
    if pd.api.types.is_numeric_dtype(dtype) and card_ratio < _LOW_CARD_RATIO:
        return {"column": col, "dimension": "what", "semantic_key": col_lower,
                "confidence": "low", "reason": "low-cardinality numeric", "nucleus_candidate": False}

    # High-cardinality string — could be a nucleus or free-text WHAT
    if is_string_col and card_ratio >= _NUCLEUS_UNIQUE and is_nucleus_candidate:
        return {"column": col, "dimension": "what", "semantic_key": col_lower,
                "confidence": "medium", "reason": "high-cardinality ID-like field — nucleus candidate",
                "nucleus_candidate": True}

    # Low-cardinality string — enum-like, probably WHAT
    if is_string_col and card_ratio <= _LOW_CARD_RATIO:
        return {"column": col, "dimension": "what", "semantic_key": col_lower,
                "confidence": "medium", "reason": "low-cardinality string — likely categorical",
                "nucleus_candidate": False}

    # Default — WHAT with low confidence
    return {"column": col, "dimension": "what", "semantic_key": col_lower,
            "confidence": "low", "reason": "no strong signal — defaulted to WHAT",
            "nucleus_candidate": False}


def _key_from_col(col_lower, dimension):
    """
    Derive a clean semantic_key from a lowercased column name.
    Strips common noise words, normalises spaces to underscores.
    """
    noise = {
        "when": ("date", "year", "time", "period", "at", "on"),
        "who":  ("name", "full"),
        "where": ("location", "place"),
        "why":  ("reason", "type"),
        "how":  ("method", "mode"),
        "what": ("field", "value", "data"),
    }
    key = col_lower.strip()
    # Normalise spaces/hyphens to underscores
    for ch in (" ", "-"):
        key = key.replace(ch, "_")
    return key


# ─────────────────────────────────────────────────────────────────────────────
# LensDraft
# ─────────────────────────────────────────────────────────────────────────────

class LensDraft:
    """
    Editable lens authoring object.

    Created by suggest() or manually. Renders as a table in Jupyter.
    Call to_lens() to produce the final conformant lens dict.

    All map() calls are chainable:
        draft.map("Artist", "who", "artist").map("Released", "when", "released")
    """

    def __init__(self, rows):
        """
        rows: list of dicts with keys:
            column, dimension, semantic_key, confidence, reason, nucleus_candidate
        """
        # Internal state — keyed by column name for O(1) updates
        self._rows = {r["column"]: dict(r) for r in rows}
        self._order = [r["column"] for r in rows]  # preserve insertion order
        self._nucleus = None  # set by nucleus() or nucleus_composite()

    # ── Authoring methods ────────────────────────────────────────────────────

    def map(self, column, dimension, semantic_key):
        """
        Assign or override the dimension and semantic_key for a column.
        Chainable. Raises KeyError if column is not in the draft.

        Args:
            column:       source column name (exact match)
            dimension:    one of who/what/when/where/why/how
            semantic_key: the semantic key string

        Returns self for chaining.
        """
        if column not in self._rows:
            raise KeyError(
                f"Column '{column}' not found in draft. "
                f"Available columns: {list(self._rows.keys())}"
            )
        dim = dimension.lower()
        if dim not in VALID_DIMENSIONS:
            raise ValueError(f"Invalid dimension '{dimension}'. Must be one of: {sorted(VALID_DIMENSIONS)}")

        self._rows[column]["dimension"]    = dim
        self._rows[column]["semantic_key"] = semantic_key.lower()
        self._rows[column]["confidence"]   = "manual"
        self._rows[column]["reason"]       = "manually mapped"
        return self

    def nucleus(self, column, prefix=None):
        """
        Declare a single-field nucleus.

        Args:
            column: source column name — must be in the draft
            prefix: optional URI prefix for entity IDs (e.g. "discogs:release")

        Returns self for chaining.
        """
        if column not in self._rows:
            raise KeyError(
                f"Column '{column}' not found in draft. "
                f"Available columns: {list(self._rows.keys())}"
            )
        self._nucleus = {
            "type":   "single",
            "field":  column,
            "prefix": prefix or "",
        }
        return self

    def nucleus_composite(self, columns, separator="-", prefix=None):
        """
        Declare a composite nucleus from two or more columns.

        The entity_id for each row will be:
            prefix + ":" + col1_value + separator + col2_value + ...

        Args:
            columns:   list of column names — all must be in the draft
            separator: string to join the column values (default "-")
            prefix:    optional URI prefix for entity IDs

        Returns self for chaining.
        """
        for col in columns:
            if col not in self._rows:
                raise KeyError(
                    f"Column '{col}' not found in draft. "
                    f"Available columns: {list(self._rows.keys())}"
                )
        if len(columns) < 2:
            raise ValueError("nucleus_composite requires at least 2 columns")

        self._nucleus = {
            "type":      "composite",
            "fields":    list(columns),
            "separator": separator,
            "prefix":    prefix or "",
        }
        return self

    # ── Output ───────────────────────────────────────────────────────────────

    def to_lens(self, lens_id, authority, **kwargs):
        """
        Produce a conformant lens dict from the current draft state.

        Args:
            lens_id:   lens identifier string (e.g. "discogs_v1")
            authority: authority string (e.g. "abk")
            **kwargs:  optional declaration fields:
                       intent, scope, permitted_ops, source_format,
                       domain, created_by, lens_version

        Returns:
            dict — same schema as discogs_community_v1.json

        Raises:
            ValueError if no nucleus has been declared.
        """
        if self._nucleus is None:
            raise ValueError(
                "No nucleus declared. Call draft.nucleus() or "
                "draft.nucleus_composite() before to_lens()."
            )

        # Build coordinate_map — only rows that have a dimension assigned
        coordinate_map = {}
        for col in self._order:
            row = self._rows[col]
            if row.get("dimension") and row.get("semantic_key"):
                coordinate_map[col] = {
                    "dimension":    row["dimension"],
                    "semantic_key": row["semantic_key"],
                }

        # Compute stats
        dim_counts = {d: 0 for d in ("who", "what", "when", "where", "how")}
        for entry in coordinate_map.values():
            d = entry["dimension"]
            if d in dim_counts:
                dim_counts[d] += 1

        stats = {
            "total_fields": len(coordinate_map),
            "by_dimension": dim_counts,
        }

        # Build declaration
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        declaration = {
            "why|intent":        kwargs.get("intent", f"field_mapping_{lens_id}"),
            "why|authority":     authority,
            "why|scope":         kwargs.get("scope", ""),
            "why|permitted_ops": kwargs.get("permitted_ops", "field_mapping canonical_tagging"),
            "source_format":     kwargs.get("source_format", ""),
            "domain":            kwargs.get("domain", ""),
            "created":           kwargs.get("created", now),
            "created_by":        kwargs.get("created_by", ""),
        }

        # Build nucleus — copy to avoid mutating internal state
        nucleus = dict(self._nucleus)

        return {
            "lens_id":      lens_id,
            "lens_version": kwargs.get("lens_version", "1.0"),
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "declaration":  declaration,
            "coordinate_map": coordinate_map,
            "stats":        stats,
            "nucleus":      nucleus,
        }

    # ── Inspection ───────────────────────────────────────────────────────────

    def columns(self):
        """Return list of column names in insertion order."""
        return list(self._order)

    def get(self, column):
        """Return the current mapping dict for a column."""
        return dict(self._rows[column])

    def unmapped(self):
        """Return list of column names with no dimension assigned."""
        return [c for c in self._order if not self._rows[c].get("dimension")]

    # ── Jupyter rendering ────────────────────────────────────────────────────

    def _repr_html_(self):
        nucleus_col = None
        if self._nucleus:
            if self._nucleus["type"] == "single":
                nucleus_col = {self._nucleus["field"]}
            else:
                nucleus_col = set(self._nucleus.get("fields", []))

        rows_html = []
        for col in self._order:
            r    = self._rows[col]
            dim  = r.get("dimension") or ""
            key  = r.get("semantic_key") or ""
            conf = r.get("confidence") or ""
            reason = r.get("reason") or ""

            is_nucleus = nucleus_col and col in nucleus_col
            nuc_badge  = " <span style='color:#888;font-size:0.8em'>⬤ nucleus</span>" if is_nucleus else ""

            # Confidence colour
            conf_colour = {"high": "#2a9d2a", "manual": "#1a6bb5",
                           "medium": "#e07a00", "low": "#999"}.get(conf, "#999")

            rows_html.append(
                f"<tr>"
                f"<td style='padding:4px 8px'>{col}{nuc_badge}</td>"
                f"<td style='padding:4px 8px;font-weight:bold'>{dim.upper() if dim else '—'}</td>"
                f"<td style='padding:4px 8px;font-family:monospace'>{key or '—'}</td>"
                f"<td style='padding:4px 8px;color:{conf_colour}'>{conf}</td>"
                f"<td style='padding:4px 8px;color:#888;font-size:0.85em'>{reason}</td>"
                f"</tr>"
            )

        nucleus_note = ""
        if self._nucleus:
            if self._nucleus["type"] == "single":
                nucleus_note = (
                    f"<p style='margin:4px 0;color:#555;font-size:0.9em'>"
                    f"Nucleus: <code>{self._nucleus['field']}</code>"
                    f"{' — prefix: ' + self._nucleus['prefix'] if self._nucleus.get('prefix') else ''}"
                    f"</p>"
                )
            else:
                fields = " + ".join(self._nucleus.get("fields", []))
                nucleus_note = (
                    f"<p style='margin:4px 0;color:#555;font-size:0.9em'>"
                    f"Composite nucleus: <code>{fields}</code>"
                    f" (separator: '{self._nucleus.get('separator', '-')}')"
                    f"{' — prefix: ' + self._nucleus['prefix'] if self._nucleus.get('prefix') else ''}"
                    f"</p>"
                )
        else:
            nucleus_note = "<p style='margin:4px 0;color:#c00;font-size:0.9em'>⚠ No nucleus declared</p>"

        return (
            f"<div style='font-family:sans-serif'>"
            f"{nucleus_note}"
            f"<table style='border-collapse:collapse;width:100%'>"
            f"<thead><tr style='border-bottom:2px solid #ddd'>"
            f"<th style='padding:4px 8px;text-align:left'>Column</th>"
            f"<th style='padding:4px 8px;text-align:left'>Dimension</th>"
            f"<th style='padding:4px 8px;text-align:left'>Semantic Key</th>"
            f"<th style='padding:4px 8px;text-align:left'>Confidence</th>"
            f"<th style='padding:4px 8px;text-align:left'>Reason</th>"
            f"</tr></thead>"
            f"<tbody>{''.join(rows_html)}</tbody>"
            f"</table>"
            f"</div>"
        )

    def __repr__(self):
        lines = ["LensDraft:"]
        for col in self._order:
            r = self._rows[col]
            dim = r.get("dimension") or "—"
            key = r.get("semantic_key") or "—"
            lines.append(f"  {col:30s}  {dim.upper():8s}  {key}")
        if self._nucleus:
            if self._nucleus["type"] == "single":
                lines.append(f"\n  nucleus: {self._nucleus['field']}")
            else:
                lines.append(f"\n  nucleus (composite): {' + '.join(self._nucleus['fields'])}")
        else:
            lines.append("\n  nucleus: not declared")
        return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# suggest()
# ─────────────────────────────────────────────────────────────────────────────

def suggest(source, sample=100):
    """
    Infer field mappings from a DataFrame or CSV path.

    Args:
        source: pd.DataFrame, str path, or pathlib.Path to a CSV
        sample: number of rows to sample for inference (default 100)

    Returns:
        LensDraft — editable, Jupyter-renderable

    The inference uses pandas dtype and cardinality, not just column
    name patterns. This gives smarter suggestions than name-only matching.
    """
    if isinstance(source, (str, Path)):
        df = pd.read_csv(source, nrows=max(sample, 500))
    elif isinstance(source, pd.DataFrame):
        df = source
    else:
        raise TypeError(f"source must be a DataFrame, str path, or Path. Got {type(source)}")

    # Sample for inference but use full column list
    sample_df = df.head(sample) if len(df) > sample else df
    n_rows    = len(sample_df)

    rows = []
    for col in df.columns:
        series = sample_df[col]
        row    = _infer_mapping(col, series, n_rows)
        rows.append(row)

    return LensDraft(rows)


# ─────────────────────────────────────────────────────────────────────────────
# load() / save() / validate()
# ─────────────────────────────────────────────────────────────────────────────

def validate(lens):
    """
    Validate a lens dict against the expected schema.

    Returns:
        {"valid": True}
        {"valid": False, "errors": [str, ...]}
    """
    errors = []

    if not isinstance(lens, dict):
        return {"valid": False, "errors": ["lens must be a dict"]}

    # Required top-level keys
    for key in ("lens_id", "coordinate_map", "nucleus"):
        if key not in lens:
            errors.append(f"Missing required field: '{key}'")

    # coordinate_map entries
    if "coordinate_map" in lens:
        cm = lens["coordinate_map"]
        if not isinstance(cm, dict):
            errors.append("coordinate_map must be a dict")
        else:
            for col, entry in cm.items():
                if not isinstance(entry, dict):
                    errors.append(f"coordinate_map['{col}'] must be a dict")
                    continue
                if "dimension" not in entry:
                    errors.append(f"coordinate_map['{col}'] missing 'dimension'")
                elif entry["dimension"].lower() not in VALID_DIMENSIONS:
                    errors.append(
                        f"coordinate_map['{col}'] invalid dimension '{entry['dimension']}'. "
                        f"Must be one of: {sorted(VALID_DIMENSIONS)}"
                    )
                if "semantic_key" not in entry:
                    errors.append(f"coordinate_map['{col}'] missing 'semantic_key'")

    # nucleus
    if "nucleus" in lens:
        nuc = lens["nucleus"]
        if not isinstance(nuc, dict):
            errors.append("nucleus must be a dict")
        else:
            if "type" not in nuc:
                errors.append("nucleus missing 'type'")
            elif nuc["type"] == "single":
                if "field" not in nuc:
                    errors.append("nucleus type 'single' requires 'field'")
            elif nuc["type"] == "composite":
                if "fields" not in nuc:
                    errors.append("nucleus type 'composite' requires 'fields'")
                elif len(nuc["fields"]) < 2:
                    errors.append("nucleus type 'composite' requires at least 2 fields")
            else:
                errors.append(f"nucleus type must be 'single' or 'composite', got '{nuc['type']}'")

    if errors:
        return {"valid": False, "errors": errors}
    return {"valid": True}


def load(path):
    """
    Load a lens JSON file.

    Args:
        path: str or pathlib.Path

    Returns:
        dict — the lens

    Raises:
        FileNotFoundError if the file does not exist
        LensValidationError if the file is not a valid lens
        json.JSONDecodeError if the file is not valid JSON
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Lens file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        lens = json.load(f)

    result = validate(lens)
    if not result["valid"]:
        raise LensValidationError(result["errors"])

    return lens


def save(lens, path):
    """
    Write a lens dict to a JSON file.

    Args:
        lens: dict — a valid lens
        path: str or pathlib.Path

    Raises:
        LensValidationError if the lens is not valid
    """
    result = validate(lens)
    if not result["valid"]:
        raise LensValidationError(result["errors"])

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(lens, f, indent=2, ensure_ascii=False)
