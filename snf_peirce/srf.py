"""
srf.py — Semantic Record Format (SRF) parser and substrate emitter.

Part of the snf-peirce package.
Spec: SRF Wire Format Specification v1.0

Three public methods on SRFRecord:
    SRFRecord.from_dict(d)   — parse + validate a dict, return SRFRecord or raise SRFValidationError
    .to_dict()               — serialize back to a JSON-compatible dict (lossless round-trip)
    .to_snf_rows()           — explode facts into substrate-ready spoke rows + a meta row

Spoke row shape (matches snf_spoke DDL in reckoner_api.py / model_builder_api.py):
    entity_id | dimension | semantic_key | value | coordinate | lens_id

Meta row shape (matches snf_meta DDL in model_builder_api.py):
    entity_id | nucleus | label | sublabel | lens_id | translator_version

UNKNOWN dimension facts are stored in to_dict() but NOT emitted as spoke rows.
Routing must not index them; see spec §4.3.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SRF_VERSION = "1.0"

VALID_DIMENSIONS = {"WHO", "WHAT", "WHEN", "WHERE", "WHY", "HOW"}
# UNKNOWN is permitted for storage but not routable — handled separately
UNKNOWN_DIMENSION = "UNKNOWN"

_ISO8601_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$"
)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class SRFValidationError(ValueError):
    """
    Raised when an SRF dict fails validation.

    Attributes:
        field   — dotted path to the offending field, e.g. "facts[0].dimension"
        reason  — human-readable explanation
    """
    def __init__(self, field: str, reason: str) -> None:
        self.field  = field
        self.reason = reason
        super().__init__(f"SRF validation error at '{field}': {reason}")


# ---------------------------------------------------------------------------
# Internal dataclasses (plain dicts internally; typed wrappers for callers)
# ---------------------------------------------------------------------------

class _Nucleus:
    __slots__ = ("type", "value", "authority")

    def __init__(self, type: str, value: str, authority: Optional[str] = None) -> None:
        self.type      = type
        self.value     = value
        self.authority = authority

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {"type": self.type, "value": self.value}
        if self.authority is not None:
            d["authority"] = self.authority
        return d


class _Fact:
    __slots__ = ("dimension", "semantic_key", "value", "qualifiers")

    def __init__(
        self,
        dimension: str,
        semantic_key: str,
        value: str,
        qualifiers: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.dimension   = dimension      # canonical uppercase
        self.semantic_key = semantic_key
        self.value       = value
        self.qualifiers  = qualifiers

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "dimension":    self.dimension,
            "semantic_key": self.semantic_key,
            "value":        self.value,
        }
        if self.qualifiers:
            d["qualifiers"] = self.qualifiers
        return d


class _Provenance:
    __slots__ = (
        "source", "translated_by", "translator_version",
        "lens", "translated_at",
        "source_record_id", "source_url",
    )

    def __init__(
        self,
        source: str,
        translated_by: str,
        translator_version: str,
        lens: str,
        translated_at: str,
        source_record_id: Optional[str] = None,
        source_url: Optional[str] = None,
    ) -> None:
        self.source             = source
        self.translated_by      = translated_by
        self.translator_version = translator_version
        self.lens               = lens
        self.translated_at      = translated_at
        self.source_record_id   = source_record_id
        self.source_url         = source_url

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "source":             self.source,
            "translated_by":      self.translated_by,
            "translator_version": self.translator_version,
            "lens":               self.lens,
            "translated_at":      self.translated_at,
        }
        if self.source_record_id is not None:
            d["source_record_id"] = self.source_record_id
        if self.source_url is not None:
            d["source_url"] = self.source_url
        return d


class _AlternateView:
    __slots__ = ("lens", "srf_uri", "label")

    def __init__(self, lens: str, srf_uri: str, label: Optional[str] = None) -> None:
        self.lens    = lens
        self.srf_uri = srf_uri
        self.label   = label

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {"lens": self.lens, "srf_uri": self.srf_uri}
        if self.label is not None:
            d["label"] = self.label
        return d


# ---------------------------------------------------------------------------
# SRFRecord
# ---------------------------------------------------------------------------

class SRFRecord:
    """
    A parsed, validated SRF record.

    Do not construct directly — use SRFRecord.from_dict().
    """

    def __init__(
        self,
        srf_version: str,
        srf_uri: str,
        entity_id: str,
        nucleus: _Nucleus,
        facts: List[_Fact],
        provenance: _Provenance,
        alternate_views: Optional[List[_AlternateView]] = None,
    ) -> None:
        self._srf_version     = srf_version
        self._srf_uri         = srf_uri
        self._entity_id       = entity_id
        self._nucleus         = nucleus
        self._facts           = facts
        self._provenance      = provenance
        self._alternate_views = alternate_views or []

    # -----------------------------------------------------------------------
    # Primary API
    # -----------------------------------------------------------------------

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "SRFRecord":
        """
        Parse and validate a dict.  Returns a SRFRecord or raises SRFValidationError.

        Validation rules follow SRF Wire Format Specification v1.0:
        - All REQUIRED fields must be present and non-empty strings.
        - facts must be a non-empty array; each fact must have dimension,
          semantic_key, value.
        - dimension is case-insensitive on receipt; canonical form is uppercase.
          Must be one of WHO/WHAT/WHEN/WHERE/WHY/HOW/UNKNOWN.
        - provenance.translated_at must parse as ISO 8601.
        - srf_version must be "1.0".
        """
        if not isinstance(d, dict):
            raise SRFValidationError("(root)", "record must be a JSON object")

        # --- Envelope fields ------------------------------------------------
        srf_version = _require_str(d, "srf_version")
        if srf_version != SRF_VERSION:
            raise SRFValidationError(
                "srf_version",
                f"expected '1.0', got '{srf_version}'"
            )

        srf_uri   = _require_str(d, "srf_uri")
        entity_id = _require_str(d, "entity_id")

        # --- Nucleus --------------------------------------------------------
        nucleus_raw = d.get("nucleus")
        if nucleus_raw is None:
            raise SRFValidationError("nucleus", "required field is missing")
        if not isinstance(nucleus_raw, dict):
            raise SRFValidationError("nucleus", "must be an object")
        nucleus = _Nucleus(
            type      = _require_str(nucleus_raw, "nucleus.type"),
            value     = _require_str(nucleus_raw, "nucleus.value"),
            authority = _opt_str(nucleus_raw, "nucleus.authority"),
        )

        # --- Facts ----------------------------------------------------------
        facts_raw = d.get("facts")
        if facts_raw is None:
            raise SRFValidationError("facts", "required field is missing")
        if not isinstance(facts_raw, list):
            raise SRFValidationError("facts", "must be an array")
        if len(facts_raw) == 0:
            raise SRFValidationError("facts", "must contain at least one fact")

        facts: List[_Fact] = []
        for i, f in enumerate(facts_raw):
            if not isinstance(f, dict):
                raise SRFValidationError(f"facts[{i}]", "each fact must be an object")

            dim_raw = f.get("dimension")
            if dim_raw is None or not isinstance(dim_raw, str) or not dim_raw.strip():
                raise SRFValidationError(f"facts[{i}].dimension", "required, must be a non-empty string")

            dim = dim_raw.strip().upper()
            if dim not in VALID_DIMENSIONS and dim != UNKNOWN_DIMENSION:
                raise SRFValidationError(
                    f"facts[{i}].dimension",
                    f"'{dim_raw}' is not a valid dimension; "
                    f"must be one of {sorted(VALID_DIMENSIONS)} or UNKNOWN"
                )

            semantic_key = _require_str(f, f"facts[{i}].semantic_key")
            value        = _require_str(f, f"facts[{i}].value")

            qualifiers = f.get("qualifiers")
            if qualifiers is not None and not isinstance(qualifiers, dict):
                raise SRFValidationError(f"facts[{i}].qualifiers", "must be an object if present")

            facts.append(_Fact(
                dimension    = dim,
                semantic_key = semantic_key,
                value        = value,
                qualifiers   = qualifiers,
            ))

        # --- Provenance -----------------------------------------------------
        prov_raw = d.get("provenance")
        if prov_raw is None:
            raise SRFValidationError("provenance", "required field is missing")
        if not isinstance(prov_raw, dict):
            raise SRFValidationError("provenance", "must be an object")

        translated_at = _require_str(prov_raw, "provenance.translated_at")
        if not _ISO8601_RE.match(translated_at):
            raise SRFValidationError(
                "provenance.translated_at",
                f"'{translated_at}' is not a valid ISO 8601 timestamp "
                f"(expected format: 2026-05-03T00:00:00Z)"
            )

        provenance = _Provenance(
            source             = _require_str(prov_raw, "provenance.source"),
            translated_by      = _require_str(prov_raw, "provenance.translated_by"),
            translator_version = _require_str(prov_raw, "provenance.translator_version"),
            lens               = _require_str(prov_raw, "provenance.lens"),
            translated_at      = translated_at,
            source_record_id   = _opt_str(prov_raw, "provenance.source_record_id"),
            source_url         = _opt_str(prov_raw, "provenance.source_url"),
        )

        # --- Alternate views (optional) -------------------------------------
        alternate_views: List[_AlternateView] = []
        av_raw = d.get("alternate_views")
        if av_raw is not None:
            if not isinstance(av_raw, list):
                raise SRFValidationError("alternate_views", "must be an array if present")
            for i, av in enumerate(av_raw):
                if not isinstance(av, dict):
                    raise SRFValidationError(f"alternate_views[{i}]", "each entry must be an object")
                alternate_views.append(_AlternateView(
                    lens    = _require_str(av, f"alternate_views[{i}].lens"),
                    srf_uri = _require_str(av, f"alternate_views[{i}].srf_uri"),
                    label   = _opt_str(av, f"alternate_views[{i}].label"),
                ))

        return cls(
            srf_version     = srf_version,
            srf_uri         = srf_uri,
            entity_id       = entity_id,
            nucleus         = nucleus,
            facts           = facts,
            provenance      = provenance,
            alternate_views = alternate_views,
        )

    def to_dict(self) -> Dict[str, Any]:
        """
        Serialize back to a JSON-compatible dict.  Lossless round-trip with from_dict().
        All fields present in the original (including UNKNOWN facts) are preserved.
        """
        d: Dict[str, Any] = {
            "srf_version": self._srf_version,
            "srf_uri":     self._srf_uri,
            "entity_id":   self._entity_id,
            "nucleus":     self._nucleus.to_dict(),
            "facts":       [f.to_dict() for f in self._facts],
            "provenance":  self._provenance.to_dict(),
        }
        if self._alternate_views:
            d["alternate_views"] = [av.to_dict() for av in self._alternate_views]
        return d

    def to_snf_rows(self) -> Dict[str, Any]:
        """
        Explode the record into rows ready for substrate ingestion.

        Returns a dict with two keys:

            "spoke_rows" — list of dicts, one per routable fact.
                Each dict matches the snf_spoke DDL:
                    entity_id | dimension | semantic_key | value | coordinate | lens_id

                coordinate format: "DIMENSION|semantic_key|value"
                UNKNOWN dimension facts are NOT included.

            "meta_row" — a single dict matching the snf_meta DDL:
                    entity_id | nucleus | label | sublabel | lens_id | translator_version

                nucleus is serialized as "type:value" for compact storage.
                label is set to the first WHAT.title value found, or entity_id if absent.
                sublabel is set to the first WHO fact value found, or None.

        The import endpoint writes spoke_rows to snf_spoke and meta_row to snf_meta.
        It does not need to touch provenance fields — translator_version is in meta_row.
        """
        lens_id            = self._provenance.lens
        translator_version = self._provenance.translator_version
        entity_id          = self._entity_id

        spoke_rows = []
        for fact in self._facts:
            if fact.dimension == UNKNOWN_DIMENSION:
                # Spec §4.3: UNKNOWN facts must not be indexed in posting lists.
                continue
            coordinate = f"{fact.dimension}|{fact.semantic_key}|{fact.value}"
            spoke_rows.append({
                "entity_id":    entity_id,
                "dimension":    fact.dimension,
                "semantic_key": fact.semantic_key,
                "value":        fact.value,
                "coordinate":   coordinate,
                "lens_id":      lens_id,
            })

        # Derive display label from facts (best-effort; importer may override)
        label    = self._first_fact_value("WHAT", "title") or entity_id
        sublabel = self._first_fact_value("WHO",  None)    or None

        nucleus_str = f"{self._nucleus.type}:{self._nucleus.value}"

        meta_row = {
            "entity_id":          entity_id,
            "nucleus":            nucleus_str,
            "label":              label,
            "sublabel":           sublabel,
            "lens_id":            lens_id,
            "translator_version": translator_version,
        }

        return {
            "spoke_rows": spoke_rows,
            "meta_row":   meta_row,
        }

    # -----------------------------------------------------------------------
    # Properties (read-only access for callers who need them)
    # -----------------------------------------------------------------------

    @property
    def entity_id(self) -> str:
        return self._entity_id

    @property
    def lens_id(self) -> str:
        return self._provenance.lens

    @property
    def translator_version(self) -> str:
        return self._provenance.translator_version

    @property
    def nucleus_type(self) -> str:
        return self._nucleus.type

    @property
    def nucleus_value(self) -> str:
        return self._nucleus.value

    @property
    def fact_count(self) -> int:
        return len(self._facts)

    @property
    def routable_fact_count(self) -> int:
        """Number of facts that will become spoke rows (excludes UNKNOWN)."""
        return sum(1 for f in self._facts if f.dimension != UNKNOWN_DIMENSION)

    def __repr__(self) -> str:
        return (
            f"SRFRecord(entity_id={self._entity_id!r}, "
            f"lens={self._provenance.lens!r}, "
            f"facts={self.fact_count}, "
            f"routable={self.routable_fact_count})"
        )

    # -----------------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------------

    def _first_fact_value(
        self, dimension: str, semantic_key: Optional[str]
    ) -> Optional[str]:
        """Return the value of the first matching fact, or None."""
        for fact in self._facts:
            if fact.dimension != dimension:
                continue
            if semantic_key is None or fact.semantic_key == semantic_key:
                return fact.value
        return None


# ---------------------------------------------------------------------------
# Validation helpers (module-private)
# ---------------------------------------------------------------------------

def _require_str(d: Dict[str, Any], field: str) -> str:
    """
    Extract a required non-empty string field.
    field may be a dotted path like "provenance.source"; only the last
    segment is used as the dict key, but the full path appears in errors.
    """
    key = field.rsplit(".", 1)[-1]
    val = d.get(key)
    if val is None:
        raise SRFValidationError(field, "required field is missing")
    if not isinstance(val, str):
        raise SRFValidationError(field, f"must be a string, got {type(val).__name__}")
    if not val.strip():
        raise SRFValidationError(field, "must not be empty")
    return val


def _opt_str(d: Dict[str, Any], field: str) -> Optional[str]:
    """Extract an optional string field. Returns None if absent or null."""
    key = field.rsplit(".", 1)[-1]
    val = d.get(key)
    if val is None:
        return None
    if not isinstance(val, str):
        raise SRFValidationError(field, f"must be a string if present, got {type(val).__name__}")
    return val or None   # treat empty string as absent


# ---------------------------------------------------------------------------
# Convenience: parse from JSON string or file path
# ---------------------------------------------------------------------------

def load_srf_json(json_str: str) -> SRFRecord:
    """Parse an SRF record from a JSON string."""
    import json
    try:
        d = json.loads(json_str)
    except json.JSONDecodeError as e:
        raise SRFValidationError("(root)", f"invalid JSON: {e}") from e
    return SRFRecord.from_dict(d)


def load_srf_file(path: str) -> SRFRecord:
    """Parse an SRF record from a .srf (JSON) file path."""
    import json
    from pathlib import Path
    try:
        d = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        raise SRFValidationError("(root)", f"could not read file '{path}': {e}") from e
    return SRFRecord.from_dict(d)
