"""
roaring_substrate.py — Roaring Bitmap SNF Substrate

An alternative SNF substrate implementation using bitmap posting lists
instead of DuckDB SQL. This is the data structure the SNF Boolean routing
algebra was describing in its formal specification.

Architecture
------------
The SNF routing algebra is:
    AND across dimensions = set intersection
    OR within dimension   = set union
    NOT                   = set complement

Roaring bitmaps implement exactly these operations, compressed and
SIMD-accelerated. Each coordinate maps to a bitmap of entity positions.
A query is pure bitwise operations — no SQL, no query planner, no disk I/O
after the index is loaded.

    coordinate "WHO|author|Morrison, Toni" → BitMap({0, 4, 17, 23, ...})
    coordinate "WHEN|year|1987"            → BitMap({4, 12, 23, 45, ...})
    AND                                    → intersection → BitMap({4, 23, ...})
    result                                 → ["marc:loc:100004", "marc:loc:100023"]

Substrate options
-----------------
    compile_data(df, lens)
        → DuckDB substrate (default, general purpose)

    compile_to_roaring(df, lens)
        → RoaringSubstrate (faster Boolean routing, lower memory)

    compile_to_roaring(df, lens, into="roaring://path/to/index")
        → persist index to disk

The two substrates have the same public interface so the Peirce query
layer works against both without modification.

Dependencies
------------
    pip install pyroaring      # production — SIMD-accelerated C library
    
    Falls back to Python sets if pyroaring is not installed.
    Python sets demonstrate the same logic but without compression
    or SIMD acceleration. Fine for development and small corpora.
    For production use at scale, install pyroaring.

When to use each substrate
--------------------------
    DuckDB substrate:
        - General purpose queries
        - Rich result sets (you need the full spoke table back)
        - Mixed analytical + Boolean workloads
        - Default for most use cases

    Roaring substrate:
        - Pure Boolean routing at high speed
        - Large corpora (1M+ entities) where memory matters
        - When you need entity_ids and will fetch details separately
        - Closest to the formal SNF routing algebra
        - What Lucene does internally
"""

from __future__ import annotations

import json
import pickle
from pathlib import Path
from typing import Any, Optional

# Try pyroaring first, fall back to Python sets
try:
    from pyroaring import BitMap as _BitMap
    _ROARING_AVAILABLE = True
except ImportError:
    _ROARING_AVAILABLE = False
    # Fallback — Python set with the same interface
    class _BitMap(set):
        """
        Fallback bitmap using Python sets.
        Same interface as pyroaring.BitMap for basic operations.
        Install pyroaring for production use: pip install pyroaring
        """
        def __and__(self, other): return _BitMap(super().__and__(other))
        def __or__(self, other):  return _BitMap(super().__or__(other))
        def __sub__(self, other): return _BitMap(super().__sub__(other))
        def serialize(self):      return pickle.dumps(set(self))

        @staticmethod
        def deserialize(data):    return _BitMap(pickle.loads(data))


# ─────────────────────────────────────────────────────────────────────────────
# RoaringSubstrate
# ─────────────────────────────────────────────────────────────────────────────

class RoaringSubstrate:
    """
    SNF substrate backed by bitmap posting lists.

    Same public interface as compile.Substrate — works transparently
    with the Peirce query layer.

    Internal structure:
        _entity_ids:    list[str]       — position → entity_id
        _entity_pos:    dict[str, int]  — entity_id → position
        _index:         dict[str, BitMap] — coordinate → bitmap
        _facts:         dict[str, list]   — entity_id → list of fact dicts
        _lens_id:       str
    """

    def __init__(self, entity_ids, index, facts, lens_id, source_path=None):
        self._entity_ids  = entity_ids           # list — position is the key
        self._entity_pos  = {e: i for i, e in enumerate(entity_ids)}
        self._index       = index                # coordinate → BitMap
        self._facts       = facts                # entity_id → [fact dicts]
        self._lens_id     = lens_id
        self._source      = str(source_path) if source_path else None

        # Pre-compute dimension sets for schema queries
        self._dimensions  = sorted(set(
            coord.split("|")[0].lower()
            for coord in index
        ))

    @property
    def lens_id(self):
        return self._lens_id

    # ── Query execution ───────────────────────────────────────────────────────

    def query(self, constraints):
        """
        Execute constraints using bitmap set operations.

        This is the pure Boolean routing algebra:
            AND across dimensions = bitmap intersection
            OR within dimension   = bitmap union (same key, multiple values)
            NOT                   = full set minus posting list

        Returns list of entity_id strings.
        """
        if not constraints:
            return []

        from collections import defaultdict
        by_dim = defaultdict(list)
        for c in constraints:
            dim = (c.get("category") or c.get("dimension") or "").upper()
            by_dim[dim].append(c)

        # Per dimension: union of matching bitmaps (OR within dim)
        # Then intersect across dimensions (AND across dims)
        result_bitmap = None

        for dim, dim_constraints in by_dim.items():
            dim_bitmap = _BitMap()

            for c in dim_constraints:
                c_bitmap = self._eval_constraint(dim, c)
                dim_bitmap = dim_bitmap | c_bitmap   # OR within dimension

            if result_bitmap is None:
                result_bitmap = dim_bitmap
            else:
                result_bitmap = result_bitmap & dim_bitmap  # AND across dimensions

        if result_bitmap is None:
            return []

        # Convert positions back to entity_ids
        return sorted(self._entity_ids[pos] for pos in result_bitmap)

    def _eval_constraint(self, dimension, c):
        """Evaluate a single constraint to a bitmap."""
        op    = c.get("op", "eq")
        key   = (c.get("field") or "").lower()
        value = c.get("value")
        dim   = dimension.upper()

        # Handle negation by flipping operator
        negated = c.get("negated", False)
        if negated:
            op = {
                "eq": "not_eq", "not_eq": "eq",
                "gt": "lte",    "lt":     "gte",
                "gte": "lt",    "lte":    "gt",
            }.get(op, op)

        if op == "eq":
            coord = f"{dim}|{key}|{value}"
            return _BitMap(self._index.get(coord, _BitMap()))

        elif op == "not_eq":
            coord   = f"{dim}|{key}|{value}"
            exclude = self._index.get(coord, _BitMap())
            # All entities in this dimension/key minus the excluded ones
            all_in_key = self._all_for_key(dim, key)
            return all_in_key - exclude

        elif op == "between":
            value2 = c.get("value2", value)
            return self._range_bitmap(dim, key, str(value), str(value2))

        elif op in ("gt", "gte", "lt", "lte"):
            return self._comparison_bitmap(dim, key, op, str(value))

        elif op == "contains":
            result = _BitMap()
            for coord, bitmap in self._index.items():
                parts = coord.split("|")
                if (len(parts) == 3 and parts[0] == dim
                        and parts[1] == key
                        and str(value).lower() in parts[2].lower()):
                    result = result | bitmap
            return result

        elif op == "prefix":
            result = _BitMap()
            prefix = str(value).lower()
            for coord, bitmap in self._index.items():
                parts = coord.split("|")
                if (len(parts) == 3 and parts[0] == dim
                        and parts[1] == key
                        and parts[2].lower().startswith(prefix)):
                    result = result | bitmap
            return result

        return _BitMap()

    def _range_bitmap(self, dim, key, lo, hi):
        """Union of all bitmaps where lo <= value <= hi."""
        result = _BitMap()
        for coord, bitmap in self._index.items():
            parts = coord.split("|")
            if len(parts) == 3 and parts[0] == dim and parts[1] == key:
                val = parts[2]
                if lo <= val <= hi:
                    result = result | bitmap
        return result

    def _comparison_bitmap(self, dim, key, op, value):
        """Bitmap for gt/gte/lt/lte comparisons."""
        result = _BitMap()
        for coord, bitmap in self._index.items():
            parts = coord.split("|")
            if len(parts) == 3 and parts[0] == dim and parts[1] == key:
                val = parts[2]
                match = (
                    (op == "gt"  and val >  value) or
                    (op == "gte" and val >= value) or
                    (op == "lt"  and val <  value) or
                    (op == "lte" and val <= value)
                )
                if match:
                    result = result | bitmap
        return result

    def _all_for_key(self, dim, key):
        """All entity positions that have any value for dim/key."""
        result = _BitMap()
        for coord, bitmap in self._index.items():
            parts = coord.split("|")
            if len(parts) == 3 and parts[0] == dim and parts[1] == key:
                result = result | bitmap
        return result

    # ── Schema / introspection ────────────────────────────────────────────────

    def dimensions(self):
        return self._dimensions

    def count(self):
        """Total number of facts (coordinate occurrences)."""
        return sum(len(b) for b in self._index.values())

    def entity_count(self):
        return len(self._entity_ids)

    def coordinates(self):
        """All coordinates in the index."""
        return list(self._index.keys())

    def values_for(self, dimension, semantic_key):
        """All distinct values for a dimension/key pair."""
        dim    = dimension.upper()
        prefix = f"{dim}|{semantic_key}|"
        return [
            coord[len(prefix):]
            for coord in self._index
            if coord.startswith(prefix)
        ]

    def describe(self):
        dims = self.dimensions()
        facts_by_dim = {}
        for dim in dims:
            dim_upper = dim.upper()
            count = sum(
                len(b) for coord, b in self._index.items()
                if coord.startswith(dim_upper + "|")
            )
            facts_by_dim[dim] = count

        return {
            "lens_id":        self._lens_id,
            "entity_count":   self.entity_count(),
            "fact_count":     self.count(),
            "dimensions":     dims,
            "facts_by_dim":   facts_by_dim,
            "source":         self._source,
            "backend":        "roaring_bitmap" if _ROARING_AVAILABLE else "python_set",
            "roaring":        _ROARING_AVAILABLE,
        }

    def get_facts(self, entity_id):
        """Return all facts for an entity_id."""
        return self._facts.get(entity_id, [])

    def to_dataframe(self):
        """Return all facts as a pandas DataFrame."""
        import pandas as pd
        rows = []
        for eid, facts in self._facts.items():
            for f in facts:
                rows.append({
                    "entity_id":    eid,
                    "dimension":    f["dimension"],
                    "semantic_key": f["semantic_key"],
                    "value":        f["value"],
                    "coordinate":   f"{f['dimension'].upper()}|{f['semantic_key']}|{f['value']}",
                    "lens_id":      self._lens_id,
                })
        return pd.DataFrame(rows)

    def __repr__(self):
        d = self.describe()
        backend = "pyroaring" if _ROARING_AVAILABLE else "python sets"
        return (
            f"RoaringSubstrate(lens_id='{d['lens_id']}', "
            f"entities={d['entity_count']:,}, "
            f"facts={d['fact_count']:,}, "
            f"backend={backend})"
        )

    def _repr_html_(self):
        d = self.describe()
        backend = "pyroaring" if _ROARING_AVAILABLE else "python sets (install pyroaring for production)"
        dim_rows = "".join(
            f"<tr><td style='padding:3px 8px'>{dim}</td>"
            f"<td style='padding:3px 8px;text-align:right'>{count:,}</td></tr>"
            for dim, count in sorted(d["facts_by_dim"].items())
        )
        return (
            f"<div style='font-family:sans-serif'>"
            f"<p style='margin:4px 0'><strong>RoaringSubstrate</strong> — "
            f"lens: <code>{d['lens_id']}</code></p>"
            f"<p style='margin:4px 0;color:#555'>"
            f"{d['entity_count']:,} entities · {d['fact_count']:,} facts · "
            f"backend: {backend}</p>"
            f"<table style='border-collapse:collapse;margin-top:6px'>"
            f"<thead><tr style='border-bottom:1px solid #ddd'>"
            f"<th style='padding:3px 8px;text-align:left'>Dimension</th>"
            f"<th style='padding:3px 8px;text-align:right'>Facts</th>"
            f"</tr></thead>"
            f"<tbody>{dim_rows}</tbody>"
            f"</table></div>"
        )

    # ── Persistence ───────────────────────────────────────────────────────────

    def save(self, path):
        """
        Save the index to disk.

        Format: a directory containing:
            index.json    — entity_ids, lens_id, metadata
            <coord>.bin   — one bitmap file per coordinate
            facts.pkl     — entity facts (for result display)
        """
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)

        # Save metadata
        meta = {
            "lens_id":    self._lens_id,
            "entity_ids": self._entity_ids,
            "coordinates": list(self._index.keys()),
            "source":     self._source,
            "roaring":    _ROARING_AVAILABLE,
        }
        with open(path / "index.json", "w") as f:
            json.dump(meta, f)

        # Save each bitmap
        bitmaps_dir = path / "bitmaps"
        bitmaps_dir.mkdir(exist_ok=True)
        coord_map = {}
        for i, (coord, bitmap) in enumerate(self._index.items()):
            fname = f"b{i:06d}.bin"
            with open(bitmaps_dir / fname, "wb") as f:
                if _ROARING_AVAILABLE:
                    f.write(bitmap.serialize())
                else:
                    f.write(pickle.dumps(set(bitmap)))
            coord_map[coord] = fname

        with open(path / "coord_map.json", "w") as f:
            json.dump(coord_map, f)

        # Save facts
        with open(path / "facts.pkl", "wb") as f:
            pickle.dump(self._facts, f)

        print(f"  Saved roaring index to {path}/")
        print(f"  {len(self._entity_ids):,} entities, {len(self._index):,} coordinates")

    @classmethod
    def load(cls, path):
        """Load a RoaringSubstrate from disk."""
        path = Path(path)

        with open(path / "index.json") as f:
            meta = json.load(f)

        with open(path / "coord_map.json") as f:
            coord_map = json.load(f)

        bitmaps_dir = path / "bitmaps"
        index = {}
        for coord, fname in coord_map.items():
            with open(bitmaps_dir / fname, "rb") as f:
                data = f.read()
            if _ROARING_AVAILABLE:
                index[coord] = _BitMap.deserialize(data)
            else:
                index[coord] = _BitMap(pickle.loads(data))

        with open(path / "facts.pkl", "rb") as f:
            facts = pickle.load(f)

        return cls(
            entity_ids  = meta["entity_ids"],
            index       = index,
            facts       = facts,
            lens_id     = meta["lens_id"],
            source_path = meta.get("source"),
        )


# ─────────────────────────────────────────────────────────────────────────────
# compile_to_roaring() — build a RoaringSubstrate from source data
# ─────────────────────────────────────────────────────────────────────────────

def compile_to_roaring(source, lens, into=None):
    """
    Compile source data into a RoaringSubstrate.

    Same interface as compile_data() but produces a bitmap index
    instead of a DuckDB table.

    Args:
        source: pd.DataFrame, str path, or Path to CSV
        lens:   lens dict or path to lens JSON
        into:   None → ephemeral in-memory substrate
                "roaring://path" → persist to disk

    Returns:
        RoaringSubstrate
    """
    import json as _json
    import pandas as pd
    from pathlib import Path as _Path
    from compile import _compile_row, CompileError, NucleusError

    # Load lens
    if isinstance(lens, (str, _Path)):
        with open(lens) as f:
            lens = _json.load(f)

    lens_id        = lens.get("lens_id")
    coordinate_map = lens.get("coordinate_map", {})
    nucleus        = lens.get("nucleus")

    if not lens_id:      raise CompileError("lens missing 'lens_id'")
    if not coordinate_map: raise CompileError("lens missing 'coordinate_map'")
    if not nucleus:      raise CompileError("lens missing 'nucleus'")

    # Load source
    if isinstance(source, (str, _Path)):
        df = pd.read_csv(source)
    elif isinstance(source, pd.DataFrame):
        df = source
    else:
        raise TypeError(f"source must be DataFrame or path, got {type(source)}")

    # Compile rows
    rows_dict = df.to_dict(orient="records")
    all_facts = []
    for i, row in enumerate(rows_dict):
        facts = _compile_row(row, i, coordinate_map, nucleus, lens_id)
        all_facts.extend(facts)

    # Build entity registry
    entity_ids = list(dict.fromkeys(f[0] for f in all_facts))  # ordered unique
    entity_pos = {e: i for i, e in enumerate(entity_ids)}

    # Build posting lists (one bitmap per coordinate)
    index = {}
    facts_by_entity = {eid: [] for eid in entity_ids}

    for eid, dim, key, val, coord, lid in all_facts:
        # Build index
        if coord not in index:
            index[coord] = _BitMap()
        index[coord].add(entity_pos[eid])

        # Store facts for result display
        facts_by_entity[eid].append({
            "dimension":    dim,
            "semantic_key": key,
            "value":        val,
        })

    substrate = RoaringSubstrate(
        entity_ids = entity_ids,
        index      = index,
        facts      = facts_by_entity,
        lens_id    = lens_id,
        source_path = source if isinstance(source, (str, _Path)) else None,
    )

    if into:
        if into.startswith("roaring://"):
            substrate.save(into[10:])
        else:
            raise CompileError(f"into= must be 'roaring://path', got '{into}'")

    return substrate


# ─────────────────────────────────────────────────────────────────────────────
# Peirce integration — make RoaringSubstrate work with peirce.execute()
# ─────────────────────────────────────────────────────────────────────────────

def roaring_query(substrate, query_string, limit=20):
    """
    Run a Peirce query against a RoaringSubstrate.

    Same interface as peirce.query() — drop-in replacement.
    """
    from parser import parse_to_constraints
    from peirce import ResultSet, PeirceParseError

    parsed = parse_to_constraints(query_string)
    if not parsed["success"]:
        raise PeirceParseError(parsed["error"])

    if parsed["type"] == "discovery":
        return _handle_discovery(substrate, parsed)

    # DNF — union of conjuncts
    all_entity_ids = set()
    for conjunct in parsed.get("conjuncts", []):
        constraints = conjunct if isinstance(conjunct, list) else conjunct.get("constraints", [])
        ids = substrate.query(constraints)
        all_entity_ids.update(ids)

    entity_ids = sorted(all_entity_ids)
    if limit:
        entity_ids = entity_ids[:limit]

    return RoaringResultSet(entity_ids, substrate, query_string, limit)


def _handle_discovery(substrate, parsed):
    """Handle discovery expressions against a RoaringSubstrate."""
    scope = parsed.get("scope")
    if scope == "all":
        print(f"\n  Dimensions: {', '.join(d.upper() for d in substrate.dimensions())}\n")
    elif scope == "dimension":
        dim    = parsed.get("dimension", "").upper()
        keys   = set()
        for coord in substrate._index:
            parts = coord.split("|")
            if parts[0] == dim:
                keys.add(parts[1])
        print(f"\n  {dim} fields: {', '.join(sorted(keys))}\n")
    elif scope == "field":
        dim  = parsed.get("dimension", "").upper()
        key  = parsed.get("field", "")
        vals = substrate.values_for(dim, key)
        print(f"\n  {dim}.{key} — {len(vals)} values")
        for v in sorted(vals)[:20]:
            count = len(substrate._index.get(f"{dim}|{key}|{v}", set()))
            print(f"    {v:<40}  {count} entities")
        print()
    return None


class RoaringResultSet:
    """Result set from a RoaringSubstrate query — same interface as peirce.ResultSet."""

    def __init__(self, entity_ids, substrate, query_string=None, limit=None):
        self.entity_ids   = entity_ids
        self.count        = len(entity_ids)
        self._substrate   = substrate
        self._query_string = query_string
        self._limit       = limit

    def to_dataframe(self):
        import pandas as pd
        rows = []
        for eid in self.entity_ids:
            for f in self._substrate.get_facts(eid):
                rows.append({
                    "entity_id":    eid,
                    "dimension":    f["dimension"],
                    "semantic_key": f["semantic_key"],
                    "value":        f["value"],
                })
        return pd.DataFrame(rows)

    def pivot(self):
        df = self.to_dataframe()
        if df.empty:
            return df
        return df.pivot_table(
            index="entity_id",
            columns="semantic_key",
            values="value",
            aggfunc=lambda x: " | ".join(str(v) for v in x)
        ).reset_index()

    def _repr_html_(self):
        df = self.to_dataframe()
        if df.empty:
            return f"<p><em>{self.count} results</em></p>"
        return (
            f"<p><strong>{self.count}</strong> result{'s' if self.count != 1 else ''}</p>"
            + df.to_html(index=False, max_rows=50)
        )

    def __repr__(self):
        return f"RoaringResultSet(count={self.count})"
