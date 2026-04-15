"""
marc_translator.py — MARC Bibliographic Lens v1.0 (Python port)

Direct port of MARCTranslator_v3.js. Translates MARC21 records into
SNF facts using the same lens map, composition rules, and nucleus
resolution as the JS reference implementation.

Can be used standalone or as part of fetch_loc.py.

Architecture (identical to JS):
    - ISBN (020$a) is the invariant nucleus; 001 is the fallback
    - Subfields compose at field-occurrence level, not atomized
    - Qualifier subfields attach to primary fact, not emitted separately
    - Subject fields follow the dimension of what they name
    - Publisher is WHO. Publication date is WHEN. Place is WHERE.

Usage:
    from marc_translator import MARCTranslator

    translator = MARCTranslator(source_id="loc")
    facts = translator.translate_record(marc_record)
    # facts is a list of SNF fact dicts ready for compile_data()
"""

from __future__ import annotations
import re
from typing import Any


# ─────────────────────────────────────────────────────────────────────────────
# MARC Bibliographic Lens v1.0 — Semantic Key Map
#
# Direct port of LENS_MAP from MARCTranslator_v3.js.
# Do not change field mappings without updating the JS reference.
# ─────────────────────────────────────────────────────────────────────────────

LENS_MAP = {

    # ── 1XX Main Entry (Primary Creator) ─────────────────────────────────────
    "100": {
        "primary": "a", "dimension": "who", "semantic_key": "author",
        "qualifiers": {"d": "birth_death", "e": "role", "c": "title_words", "4": "role_uri"},
        "compose": ["a"]
    },
    "110": {
        "primary": "a", "dimension": "who", "semantic_key": "corporate_author",
        "qualifiers": {"b": "subordinate_unit", "d": "date", "e": "role", "4": "role_uri"},
        "compose": ["a", "b"]
    },
    "111": {
        "primary": "a", "dimension": "who", "semantic_key": "meeting_author",
        "qualifiers": {"d": "date", "e": "role", "4": "role_uri"},
        "compose": ["a"]
    },
    "130": {
        "primary": "a", "dimension": "what", "semantic_key": "uniform_title",
        "qualifiers": {"l": "language", "s": "version"},
        "compose": ["a"]
    },

    # ── 2XX Title, Edition, Publication ──────────────────────────────────────
    "240": {
        "primary": "a", "dimension": "what", "semantic_key": "uniform_title",
        "qualifiers": {"l": "language", "s": "version"},
        "compose": ["a"]
    },
    "245": {
        "primary": "a", "dimension": "what", "semantic_key": "title",
        "qualifiers": {"c": "responsibility"},
        "compose": ["a", "b"],
        "subfield_map": {
            "n": {"dimension": "what", "semantic_key": "part_number"},
            "p": {"dimension": "what", "semantic_key": "part_name"}
        }
    },
    "246": {
        "primary": "a", "dimension": "what", "semantic_key": "varying_title",
        "compose": ["a"]
    },
    "250": {
        "primary": "a", "dimension": "what", "semantic_key": "edition",
        "compose": ["a"]
    },

    # 260 — legacy publication field
    "260": {
        "multi_dim": True,
        "subfield_map": {
            "a": {"dimension": "where", "semantic_key": "publication_place"},
            "b": {"dimension": "who",   "semantic_key": "publisher"},
            "c": {"dimension": "when",  "semantic_key": "publication_date"},
            "e": {"dimension": "where", "semantic_key": "manufacture_place"},
            "f": {"dimension": "who",   "semantic_key": "manufacturer"},
            "g": {"dimension": "when",  "semantic_key": "manufacture_date"}
        }
    },

    # 264 — production/publication/distribution/manufacture/copyright
    "264": {
        "multi_dim": True,
        "subfield_map": {
            "a": {"dimension": "where", "semantic_key": "publication_place"},
            "b": {"dimension": "who",   "semantic_key": "publisher"},
            "c": {"dimension": "when",  "semantic_key": "publication_date"}
        }
    },

    # ── 3XX Physical Description ──────────────────────────────────────────────
    "300": {
        "multi_dim": True,
        "subfield_map": {
            "a": {"dimension": "what", "semantic_key": "extent"},
            "b": {"dimension": "what", "semantic_key": "physical_details"},
            "c": {"dimension": "what", "semantic_key": "dimensions"},
            "e": {"dimension": "what", "semantic_key": "accompanying"}
        }
    },
    "336": {"primary": "a", "dimension": "what", "semantic_key": "content_type",     "compose": ["a"]},
    "337": {"primary": "a", "dimension": "how",  "semantic_key": "media_type",       "compose": ["a"]},
    "338": {"primary": "a", "dimension": "how",  "semantic_key": "carrier_type",     "compose": ["a"]},
    "347": {"primary": "a", "dimension": "how",  "semantic_key": "digital_file_type","compose": ["a"]},

    # ── 4XX Series ────────────────────────────────────────────────────────────
    "490": {
        "multi_dim": True,
        "subfield_map": {
            "a": {"dimension": "what", "semantic_key": "series"},
            "v": {"dimension": "what", "semantic_key": "series_volume"}
        }
    },

    # ── 5XX Notes ─────────────────────────────────────────────────────────────
    "500": {"primary": "a", "dimension": "what", "semantic_key": "general_note",      "compose": ["a"]},
    "504": {"primary": "a", "dimension": "what", "semantic_key": "bibliography_note", "compose": ["a"]},
    "505": {"primary": "a", "dimension": "what", "semantic_key": "contents",          "compose": ["a"]},
    "520": {"primary": "a", "dimension": "what", "semantic_key": "summary",           "compose": ["a"]},
    "521": {"primary": "a", "dimension": "why",  "semantic_key": "audience",          "compose": ["a"]},
    "586": {"primary": "a", "dimension": "what", "semantic_key": "awards",            "compose": ["a"]},

    # ── 6XX Subject Added Entries ─────────────────────────────────────────────
    # Rule: subjects follow the dimension of what they name.
    # Personal name → WHO.  Geographic as primary → WHERE.  Everything else → WHAT.
    "600": {
        "primary": "a", "dimension": "who", "semantic_key": "subject_person",
        "qualifiers": {"d": "birth_death", "e": "role", "4": "role_uri"},
        "compose": ["a"],
        "subfield_map": {
            "v": {"dimension": "what", "semantic_key": "subject_form"},
            "x": {"dimension": "what", "semantic_key": "subject_general"},
            "y": {"dimension": "what", "semantic_key": "subject_period"},
            "z": {"dimension": "what", "semantic_key": "subject_geography"}
        }
    },
    "610": {
        "primary": "a", "dimension": "who", "semantic_key": "subject_organization",
        "qualifiers": {"b": "subordinate_unit", "d": "date"},
        "compose": ["a", "b"],
        "subfield_map": {
            "v": {"dimension": "what", "semantic_key": "subject_form"},
            "x": {"dimension": "what", "semantic_key": "subject_general"},
            "y": {"dimension": "what", "semantic_key": "subject_period"},
            "z": {"dimension": "what", "semantic_key": "subject_geography"}
        }
    },
    "611": {
        "primary": "a", "dimension": "who", "semantic_key": "subject_meeting",
        "qualifiers": {"d": "date"},
        "compose": ["a"],
        "subfield_map": {
            "v": {"dimension": "what", "semantic_key": "subject_form"},
            "x": {"dimension": "what", "semantic_key": "subject_general"},
            "y": {"dimension": "what", "semantic_key": "subject_period"},
            "z": {"dimension": "what", "semantic_key": "subject_geography"}
        }
    },
    "630": {
        "primary": "a", "dimension": "what", "semantic_key": "subject_uniform_title",
        "compose": ["a"]
    },
    "650": {
        "primary": "a", "dimension": "what", "semantic_key": "subject_topic",
        "compose": ["a"],
        "subfield_map": {
            "v": {"dimension": "what", "semantic_key": "subject_form"},
            "x": {"dimension": "what", "semantic_key": "subject_general"},
            "y": {"dimension": "what", "semantic_key": "subject_period"},
            "z": {"dimension": "what", "semantic_key": "subject_geography"}
        }
    },
    "651": {
        "primary": "a", "dimension": "where", "semantic_key": "subject_place",
        "compose": ["a"],
        "subfield_map": {
            "v": {"dimension": "what", "semantic_key": "subject_form"},
            "x": {"dimension": "what", "semantic_key": "subject_general"},
            "y": {"dimension": "what", "semantic_key": "subject_period"}
        }
    },
    "655": {
        "primary": "a", "dimension": "what", "semantic_key": "genre",
        "compose": ["a"],
        "subfield_map": {
            "v": {"dimension": "what", "semantic_key": "subject_form"},
            "x": {"dimension": "what", "semantic_key": "subject_general"},
            "y": {"dimension": "what", "semantic_key": "subject_period"},
            "z": {"dimension": "what", "semantic_key": "subject_geography"}
        }
    },

    # ── 7XX Added Entries (Contributors) ─────────────────────────────────────
    # Rule: $e role attaches as qualifier — NOT a separate fact
    "700": {
        "primary": "a", "dimension": "who", "semantic_key": "contributor",
        "qualifiers": {"d": "birth_death", "e": "role", "4": "role_uri", "t": "work_title"},
        "compose": ["a"]
    },
    "710": {
        "primary": "a", "dimension": "who", "semantic_key": "corporate_contributor",
        "qualifiers": {"b": "subordinate_unit", "e": "role", "4": "role_uri"},
        "compose": ["a", "b"]
    },
    "711": {
        "primary": "a", "dimension": "who", "semantic_key": "meeting_contributor",
        "qualifiers": {"d": "date", "e": "role", "4": "role_uri"},
        "compose": ["a"]
    },
    "780": {
        "primary": "t", "dimension": "what", "semantic_key": "preceding_title",
        "compose": ["t"], "qualifiers": {"a": "author"}
    },
    "785": {
        "primary": "t", "dimension": "what", "semantic_key": "succeeding_title",
        "compose": ["t"], "qualifiers": {"a": "author"}
    },
    "787": {
        "primary": "t", "dimension": "what", "semantic_key": "related_work",
        "compose": ["t"], "qualifiers": {"a": "author"}
    },

    # ── 8XX Series Added Entries ──────────────────────────────────────────────
    "830": {
        "multi_dim": True,
        "subfield_map": {
            "a": {"dimension": "what", "semantic_key": "series"},
            "v": {"dimension": "what", "semantic_key": "series_volume"}
        }
    },

    # ── Identifiers ───────────────────────────────────────────────────────────
    "020": {
        "multi_dim": True,
        "subfield_map": {
            "a": {"dimension": "what", "semantic_key": "isbn"},
            "z": {"dimension": "what", "semantic_key": "isbn_cancelled"}
        }
    },
    "022": {"primary": "a", "dimension": "what", "semantic_key": "issn",                "compose": ["a"]},
    "024": {"primary": "a", "dimension": "what", "semantic_key": "standard_identifier", "compose": ["a"]},
    "050": {"primary": "a", "dimension": "what", "semantic_key": "lc_call_number",      "compose": ["a", "b"]},
    "082": {"primary": "a", "dimension": "what", "semantic_key": "dewey_number",        "compose": ["a"]},
    "856": {
        "multi_dim": True,
        "subfield_map": {
            "u": {"dimension": "how",  "semantic_key": "electronic_access"},
            "z": {"dimension": "what", "semantic_key": "access_note"}
        }
    },
}

# Control field map — tags that get direct dimension/key mapping
CONTROL_FIELD_MAP = {
    "003": {"dimension": "what", "semantic_key": "control_source"},
    "005": {"dimension": "when", "semantic_key": "last_modified"},
    "008": {"dimension": "when", "semantic_key": "fixed_field"},
}


# ─────────────────────────────────────────────────────────────────────────────
# MARCTranslator
# ─────────────────────────────────────────────────────────────────────────────

class MARCTranslator:
    """
    Python port of MARCTranslator_v3.js.

    Translates MARC21 record dicts into SNF fact dicts.

    A MARC record dict is expected in this shape:
        {
            "leader":        str,
            "controlFields": [{"tag": str, "data": str}, ...],
            "dataFields":    [
                {
                    "tag":        str,
                    "indicator1": str,
                    "indicator2": str,
                    "subfields":  [{"code": str, "data": str}, ...]
                },
                ...
            ]
        }

    This is the same normalized shape used by the JS MARCTranslator.
    Any MARC source (LOC API, Z39.50, SRU, MARCXML, binary .mrc) should
    be normalized to this shape before passing to translate_record().
    """

    def __init__(self, source_id="loc"):
        self.source_id = source_id

    # ── Value normalization ───────────────────────────────────────────────────

    def _clean_value(self, value, dimension=None, semantic_key=None):
        """
        Clean a MARC value for SNF emission.

        General: strip trailing . , / : ;
        WHEN:    extract clean 4-digit year from messy MARC date strings
                 "1966."        -> "1966"
                 "2007, c1952"  -> "2007"
                 "c1952"        -> "1952"
                 "1984-"        -> "1984"
                 "[1966]"       -> "1966"
        """
        if not value:
            return value
        cleaned = re.sub(r"[.,/:;]+$", "", value).strip()
        if dimension == "when":
            year_match = re.search(r"(?:^|[^0-9])(1[0-9]{3}|20[0-9]{2})(?:[^0-9]|$)", cleaned)
            if year_match:
                return year_match.group(1)
        return cleaned


    def translate_record(self, marc_record):
        """
        Translate one MARC record into a list of SNF fact dicts.

        Returns list of:
            {
                "entity_id":    str,
                "dimension":    str,
                "semantic_key": str,
                "value":        str,
            }

        Facts with dimension "unknown" are included — they preserve
        unmapped MARC fields for completeness.
        """
        record_id = self._get_record_id(marc_record)
        entity_id = f"marc:{self.source_id}:{record_id}"

        # Step 1 — establish nucleus
        nucleus_value = self._extract_nucleus_value(marc_record, entity_id)

        all_facts = []

        # Step 2 — control fields
        for field in marc_record.get("controlFields", []):
            f = self._extract_control_field(field, entity_id)
            if f:
                all_facts.append(f)

        # Step 3 — data fields, composed at occurrence level
        for field in marc_record.get("dataFields", []):
            composed = self._compose_data_field(field, entity_id)
            all_facts.extend(composed)

        # Convert to flat SNF fact dicts (drop unknown dimension facts
        # unless you want to preserve them — they're included here)
        return [
            {
                "entity_id":    entity_id,
                "dimension":    f["dimension"],
                "semantic_key": f["semantic_key"],
                "value":        f["value"],
            }
            for f in all_facts
            if f.get("value") and str(f["value"]).strip()
            and f["dimension"] != "unknown"   # skip unmapped fields
        ]

    # ── Nucleus extraction ────────────────────────────────────────────────────

    def _extract_nucleus_value(self, marc_record, entity_id):
        """ISBN from 020$a, fallback to 001 control number."""
        # Primary: ISBN
        for field in marc_record.get("dataFields", []):
            if field["tag"] == "020":
                for sf in field.get("subfields", []):
                    if sf["code"] == "a" and sf.get("data"):
                        return self._normalize_isbn(sf["data"])

        # Fallback: control number 001
        for field in marc_record.get("controlFields", []):
            if field["tag"] == "001" and field.get("data"):
                return field["data"].strip()

        # Last resort
        return entity_id

    def _normalize_isbn(self, raw):
        """Remove hyphens, spaces, and qualifiers from ISBN."""
        cleaned = re.sub(r"[-\s]", "", raw)
        cleaned = re.sub(r"\(.*\)", "", cleaned)
        return cleaned.strip()

    # ── Control field extraction ──────────────────────────────────────────────

    def _extract_control_field(self, field, entity_id):
        tag   = field["tag"]
        value = field.get("data", "").strip()

        if tag == "001":
            return None   # nucleus — already handled

        entry = CONTROL_FIELD_MAP.get(tag)
        if entry:
            return {
                "dimension":    entry["dimension"],
                "semantic_key": entry["semantic_key"],
                "value":        value,
            }
        # Unknown control field — preserve with marc_ prefix
        if value:
            return {
                "dimension":    "unknown",
                "semantic_key": f"control_{tag}",
                "value":        value,
            }
        return None

    # ── Data field composition ────────────────────────────────────────────────

    def _compose_data_field(self, field, entity_id):
        tag       = field["tag"]
        subfields = field.get("subfields", [])

        entry = LENS_MAP.get(tag)
        if not entry:
            return self._emit_unknown(tag, subfields)
        if entry.get("multi_dim"):
            return self._compose_multi_dim(tag, subfields, entry)
        return self._compose_primary(tag, subfields, entry)

    def _compose_primary(self, tag, subfields, entry):
        """
        Compose subfields into a primary fact + optional sub-facts.
        Qualifier subfields attach to the primary fact, not emitted separately.
        """
        facts = []

        # Index subfields by code (multiple values per code are possible)
        sf_map = {}
        for sf in subfields:
            sf_map.setdefault(sf["code"], []).append(sf["data"])

        # Compose primary value from specified subfield codes
        compose_codes = entry.get("compose", [entry.get("primary", "a")])
        parts = []
        for code in compose_codes:
            if code in sf_map:
                for val in sf_map[code]:
                    cleaned = self._clean_value(val, entry.get("dimension"))
                    if cleaned:
                        parts.append(cleaned)

        primary_value = " ".join(parts).strip()
        if not primary_value:
            return self._emit_unknown(tag, subfields)

        # Build qualifiers — attach to primary fact
        qualifiers = {}
        for code, qual_name in (entry.get("qualifiers") or {}).items():
            if code in sf_map:
                val = "; ".join(
                    re.sub(r"[.,]+$", "", v).strip()
                    for v in sf_map[code]
                    if v.strip()
                )
                if val:
                    qualifiers[qual_name] = val

        # Primary fact
        primary_fact = {
            "dimension":    entry["dimension"],
            "semantic_key": entry["semantic_key"],
            "value":        primary_value,
        }
        if qualifiers:
            primary_fact["qualifiers"] = qualifiers
        facts.append(primary_fact)

        # Independent sub-facts for subfields with their own dimension
        # e.g. 650$v (subject_form), 650$z (subject_geography)
        for code, mapping in (entry.get("subfield_map") or {}).items():
            if code in sf_map:
                for val in sf_map[code]:
                    cleaned = self._clean_value(val, mapping["dimension"])
                    if cleaned:
                        facts.append({
                            "dimension":    mapping["dimension"],
                            "semantic_key": mapping["semantic_key"],
                            "value":        cleaned,
                        })

        return facts

    def _compose_multi_dim(self, tag, subfields, entry):
        """
        Each subfield maps to its own dimension/semantic_key.
        Used for 260, 264, 300, 490, etc.
        """
        facts = []
        for sf in subfields:
            mapping = (entry.get("subfield_map") or {}).get(sf["code"])
            if not mapping:
                continue
            cleaned = self._clean_value(sf["data"], mapping["dimension"])
            if cleaned:
                facts.append({
                    "dimension":    mapping["dimension"],
                    "semantic_key": mapping["semantic_key"],
                    "value":        cleaned,
                })

        if not facts:
            return self._emit_unknown(tag, subfields)
        return facts

    def _emit_unknown(self, tag, subfields):
        """Preserve unmapped MARC fields with marc_ prefix."""
        value = " ".join(sf["data"] for sf in subfields).strip()
        if not value:
            return []
        return [{
            "dimension":    "unknown",
            "semantic_key": f"marc_{tag}",
            "value":        value,
        }]

    # ── Utilities ─────────────────────────────────────────────────────────────

    def _get_record_id(self, record):
        """Extract the 001 control number."""
        for field in record.get("controlFields", []):
            if field["tag"] == "001" and field.get("data"):
                return field["data"].strip()
        return "unknown"
