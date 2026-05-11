"""
rset.py — Fieldguild+ Expedition Bundle parser and serializer.

Part of the snf-peirce package.
Spec: Fieldguild+ Platform Specification v0.4

File extension: .rset (JSON)

Three public methods on RsetBundle:
    RsetBundle.from_dict(d)   — parse + validate a dict, return RsetBundle or raise RsetValidationError
    .to_dict()                — serialize back to a JSON-compatible dict (lossless round-trip)
    .to_index_record()        — emit the minimal record the central index needs for discovery

Bundle structure:
    - Envelope: fg_version, expedition_id, curator_id, title, tags, entries, published_at
    - Optional: abstract, lens_id, fork_of, feed_url
    - Entries: srf_uri + nucleus required per entry; entry_tags and lens_id optional

Design invariants from spec v0.4:
    - Entries are UNORDERED. No position field. No sequencing.
    - No per-entry curatorial notes. The title is the argument.
    - abstract is ONE PARAGRAPH MAX. Optional.
    - tags REQUIRED and non-empty — at least one discovery handle.
    - nucleus type determines entity-type grouping (films/recordings/books) at render time.
    - WHY is curatorial intent — lives in the bundle, not routed.
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

FG_VERSION = "1.0"

_ISO8601_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$"
)

# Nucleus types the system knows how to group for rendering
# Keys are nucleus type strings; values are human-readable group labels
NUCLEUS_TYPE_GROUPS = {
    "tmdb_id":         "Films",
    "musicbrainz_id":  "Recordings",
    "isbn":            "Books",
    "imdb_id":         "Films",
    "discogs_id":      "Recordings",
    "loc_id":          "Books",
    "wikidata_id":     "Other",
}


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class RsetValidationError(ValueError):
    """
    Raised when an .rset bundle fails validation.

    Attributes:
        field   — dotted path to the offending field, e.g. "entries[0].nucleus.type"
        reason  — human-readable explanation
    """
    def __init__(self, field: str, reason: str) -> None:
        self.field  = field
        self.reason = reason
        super().__init__(f"Rset validation error at '{field}': {reason}")


# ---------------------------------------------------------------------------
# Internal data classes
# ---------------------------------------------------------------------------

class _Nucleus:
    __slots__ = ("type", "value", "authority")

    def __init__(self, type: str, value: str, authority: Optional[str] = None) -> None:
        self.type      = type
        self.value     = value
        self.authority = authority

    @property
    def group(self) -> str:
        """Human-readable entity-type group for client rendering."""
        return NUCLEUS_TYPE_GROUPS.get(self.type, "Other")

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {"type": self.type, "value": self.value}
        if self.authority is not None:
            d["authority"] = self.authority
        return d


class _Entry:
    __slots__ = ("srf_uri", "nucleus", "entry_tags", "lens_id")

    def __init__(
        self,
        srf_uri:    str,
        nucleus:    _Nucleus,
        entry_tags: Optional[List[str]] = None,
        lens_id:    Optional[str] = None,
    ) -> None:
        self.srf_uri    = srf_uri
        self.nucleus    = nucleus
        self.entry_tags = entry_tags or []
        self.lens_id    = lens_id

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "srf_uri": self.srf_uri,
            "nucleus": self.nucleus.to_dict(),
        }
        if self.entry_tags:
            d["entry_tags"] = self.entry_tags
        if self.lens_id is not None:
            d["lens_id"] = self.lens_id
        return d


# ---------------------------------------------------------------------------
# RsetBundle
# ---------------------------------------------------------------------------

class RsetBundle:
    """
    A parsed, validated Fieldguild+ expedition bundle.

    Do not construct directly — use RsetBundle.from_dict() or RsetBundle.create().
    """

    def __init__(
        self,
        fg_version:    str,
        expedition_id: str,
        curator_id:    str,
        title:         str,
        tags:          List[str],
        entries:       List[_Entry],
        published_at:  str,
        abstract:      Optional[str] = None,
        lens_id:       Optional[str] = None,
        fork_of:       Optional[str] = None,
        feed_url:      Optional[str] = None,
    ) -> None:
        self._fg_version    = fg_version
        self._expedition_id = expedition_id
        self._curator_id    = curator_id
        self._title         = title
        self._tags          = tags
        self._entries       = entries
        self._published_at  = published_at
        self._abstract      = abstract
        self._lens_id       = lens_id
        self._fork_of       = fork_of
        self._feed_url      = feed_url

    # -----------------------------------------------------------------------
    # Primary API
    # -----------------------------------------------------------------------

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "RsetBundle":
        """
        Parse and validate an expedition bundle dict.
        Returns an RsetBundle or raises RsetValidationError.

        Validation rules follow Fieldguild+ Platform Specification v0.4:
        - All REQUIRED fields must be present and non-empty.
        - tags must be a non-empty list of non-empty strings.
        - entries must be a non-empty list; each entry needs srf_uri and nucleus.
        - published_at must be ISO 8601.
        - fg_version must be "1.0".
        - abstract, if present, is stored as-is (length is a social norm, not enforced here).
        """
        if not isinstance(d, dict):
            raise RsetValidationError("(root)", "bundle must be a JSON object")

        fg_version = _require_str(d, "fg_version")
        if fg_version != FG_VERSION:
            raise RsetValidationError(
                "fg_version",
                f"expected '1.0', got '{fg_version}'"
            )

        expedition_id = _require_str(d, "expedition_id")
        curator_id    = _require_str(d, "curator_id")
        title         = _require_str(d, "title")

        # --- Tags -----------------------------------------------------------
        tags_raw = d.get("tags")
        if tags_raw is None:
            raise RsetValidationError("tags", "required field is missing")
        if not isinstance(tags_raw, list):
            raise RsetValidationError("tags", "must be an array")
        if len(tags_raw) == 0:
            raise RsetValidationError("tags", "must contain at least one tag")
        tags = []
        for i, t in enumerate(tags_raw):
            if not isinstance(t, str) or not t.strip():
                raise RsetValidationError(f"tags[{i}]", "each tag must be a non-empty string")
            tags.append(t.strip())

        # --- Entries --------------------------------------------------------
        entries_raw = d.get("entries")
        if entries_raw is None:
            raise RsetValidationError("entries", "required field is missing")
        if not isinstance(entries_raw, list):
            raise RsetValidationError("entries", "must be an array")
        if len(entries_raw) == 0:
            raise RsetValidationError("entries", "must contain at least one entry")

        entries: List[_Entry] = []
        for i, e in enumerate(entries_raw):
            if not isinstance(e, dict):
                raise RsetValidationError(f"entries[{i}]", "each entry must be an object")

            srf_uri = _require_str(e, f"entries[{i}].srf_uri")

            # nucleus
            nuc_raw = e.get("nucleus")
            if nuc_raw is None:
                raise RsetValidationError(f"entries[{i}].nucleus", "required field is missing")
            if not isinstance(nuc_raw, dict):
                raise RsetValidationError(f"entries[{i}].nucleus", "must be an object")
            nucleus = _Nucleus(
                type      = _require_str(nuc_raw, f"entries[{i}].nucleus.type"),
                value     = _require_str(nuc_raw, f"entries[{i}].nucleus.value"),
                authority = _opt_str(nuc_raw, f"entries[{i}].nucleus.authority"),
            )

            # entry_tags (optional)
            et_raw = e.get("entry_tags")
            entry_tags: List[str] = []
            if et_raw is not None:
                if not isinstance(et_raw, list):
                    raise RsetValidationError(f"entries[{i}].entry_tags", "must be an array if present")
                for j, t in enumerate(et_raw):
                    if not isinstance(t, str) or not t.strip():
                        raise RsetValidationError(
                            f"entries[{i}].entry_tags[{j}]",
                            "each tag must be a non-empty string"
                        )
                    entry_tags.append(t.strip())

            entry_lens_id = _opt_str(e, f"entries[{i}].lens_id")

            entries.append(_Entry(
                srf_uri    = srf_uri,
                nucleus    = nucleus,
                entry_tags = entry_tags,
                lens_id    = entry_lens_id,
            ))

        # --- published_at ---------------------------------------------------
        published_at = _require_str(d, "published_at")
        if not _ISO8601_RE.match(published_at):
            raise RsetValidationError(
                "published_at",
                f"'{published_at}' is not a valid ISO 8601 timestamp"
            )

        # --- Optional fields ------------------------------------------------
        abstract = _opt_str(d, "abstract")
        lens_id  = _opt_str(d, "lens_id")
        fork_of  = _opt_str(d, "fork_of")
        feed_url = _opt_str(d, "feed_url")

        return cls(
            fg_version    = fg_version,
            expedition_id = expedition_id,
            curator_id    = curator_id,
            title         = title,
            tags          = tags,
            entries       = entries,
            published_at  = published_at,
            abstract      = abstract,
            lens_id       = lens_id,
            fork_of       = fork_of,
            feed_url      = feed_url,
        )

    @classmethod
    def create(
        cls,
        title:      str,
        tags:       List[str],
        entries:    List[Dict[str, Any]],
        curator_id: str = "0000",
        abstract:   Optional[str] = None,
        lens_id:    Optional[str] = None,
        fork_of:    Optional[str] = None,
    ) -> "RsetBundle":
        """
        Convenience constructor for building new bundles programmatically.

        entries should be a list of dicts with at minimum:
            {"srf_uri": "...", "nucleus": {"type": "...", "value": "..."}}

        expedition_id and published_at are generated automatically.
        """
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        d = {
            "fg_version":    FG_VERSION,
            "expedition_id": str(uuid.uuid4()),
            "curator_id":    curator_id,
            "title":         title,
            "tags":          tags,
            "entries":       entries,
            "published_at":  now,
        }
        if abstract:
            d["abstract"] = abstract
        if lens_id:
            d["lens_id"] = lens_id
        if fork_of:
            d["fork_of"] = fork_of
        return cls.from_dict(d)

    def to_dict(self) -> Dict[str, Any]:
        """
        Serialize back to a JSON-compatible dict. Lossless round-trip with from_dict().
        """
        d: Dict[str, Any] = {
            "fg_version":    self._fg_version,
            "expedition_id": self._expedition_id,
            "curator_id":    self._curator_id,
            "title":         self._title,
            "tags":          self._tags,
            "entries":       [e.to_dict() for e in self._entries],
            "published_at":  self._published_at,
        }
        if self._abstract is not None:
            d["abstract"] = self._abstract
        if self._lens_id is not None:
            d["lens_id"] = self._lens_id
        if self._fork_of is not None:
            d["fork_of"] = self._fork_of
        if self._feed_url is not None:
            d["feed_url"] = self._feed_url
        return d

    def to_index_record(self) -> Dict[str, Any]:
        """
        Emit the minimal record the central index needs for discovery.

        Contains:
            expedition_id, title, tags, lens_id, entity_nuclei,
            fork_of, published_at, feed_url

        entity_nuclei is the list of all nuclei across all entries —
        this is what enables "find expeditions containing this entity" lookup.

        Does NOT contain: curator_id, abstract, entry_tags.
        Those stay in the bundle.
        """
        return {
            "expedition_id":  self._expedition_id,
            "title":          self._title,
            "tags":           self._tags,
            "lens_id":        self._lens_id,
            "entity_nuclei":  [e.nucleus.to_dict() for e in self._entries],
            "fork_of":        self._fork_of,
            "published_at":   self._published_at,
            "feed_url":       self._feed_url,
        }

    def entries_by_group(self) -> Dict[str, List[_Entry]]:
        """
        Group entries by entity type for client rendering.

        Returns a dict keyed by group label ("Films", "Recordings", "Books", "Other"),
        each value being a list of entries in that group.
        Preserves insertion order within each group.
        """
        groups: Dict[str, List[_Entry]] = {}
        for entry in self._entries:
            group = entry.nucleus.group
            if group not in groups:
                groups[group] = []
            groups[group].append(entry)
        return groups

    def all_entry_tags(self) -> List[Dict[str, Any]]:
        """
        Return all entry_tags across all entries, each annotated with its nucleus.
        Useful for contributing tags to the central index against each nucleus.

        Returns list of:
            {"nucleus": {...}, "tag": "...", "lens_id": "..."}
        """
        contributions = []
        for entry in self._entries:
            lens = entry.lens_id or self._lens_id
            for tag in entry.entry_tags:
                contributions.append({
                    "nucleus": entry.nucleus.to_dict(),
                    "tag":     tag,
                    "lens_id": lens,
                })
        return contributions

    # -----------------------------------------------------------------------
    # Properties
    # -----------------------------------------------------------------------

    @property
    def expedition_id(self) -> str:
        return self._expedition_id

    @property
    def title(self) -> str:
        return self._title

    @property
    def tags(self) -> List[str]:
        return list(self._tags)

    @property
    def entry_count(self) -> int:
        return len(self._entries)

    @property
    def lens_id(self) -> Optional[str]:
        return self._lens_id

    @property
    def fork_of(self) -> Optional[str]:
        return self._fork_of

    @property
    def is_fork(self) -> bool:
        return self._fork_of is not None

    def __repr__(self) -> str:
        return (
            f"RsetBundle(expedition_id={self._expedition_id!r}, "
            f"title={self._title!r}, "
            f"entries={self.entry_count}, "
            f"tags={self._tags})"
        )


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def _require_str(d: Dict[str, Any], field: str) -> str:
    key = field.rsplit(".", 1)[-1]
    val = d.get(key)
    if val is None:
        raise RsetValidationError(field, "required field is missing")
    if not isinstance(val, str):
        raise RsetValidationError(field, f"must be a string, got {type(val).__name__}")
    if not val.strip():
        raise RsetValidationError(field, "must not be empty")
    return val


def _opt_str(d: Dict[str, Any], field: str) -> Optional[str]:
    key = field.rsplit(".", 1)[-1]
    val = d.get(key)
    if val is None:
        return None
    if not isinstance(val, str):
        raise RsetValidationError(field, f"must be a string if present, got {type(val).__name__}")
    return val or None


# ---------------------------------------------------------------------------
# Convenience: load from JSON string or file
# ---------------------------------------------------------------------------

def load_rset_json(json_str: str) -> RsetBundle:
    """Parse an expedition bundle from a JSON string."""
    try:
        d = json.loads(json_str)
    except json.JSONDecodeError as e:
        raise RsetValidationError("(root)", f"invalid JSON: {e}") from e
    return RsetBundle.from_dict(d)


def load_rset_file(path: str) -> RsetBundle:
    """Parse an expedition bundle from a .rset file path."""
    try:
        d = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        raise RsetValidationError("(root)", f"could not read file '{path}': {e}") from e
    return RsetBundle.from_dict(d)
