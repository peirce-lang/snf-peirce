"""
compile.py — compile_data() and Substrate

Week 3 of the Python SNF package.

Takes a source DataFrame and a lens dict, applies the coordinate map
row by row, resolves the nucleus, and produces a Substrate — a compiled
SNF-queryable object backed by an in-memory DuckDB instance.

Public API
----------
compile_data(source, lens, into=None) -> Substrate
    Compile a source DataFrame or CSV path using a lens dict.

    source: pd.DataFrame, str path, or pathlib.Path
    lens:   dict (from lens.load() or lens.to_lens()) or str/Path to JSON
    into:   None           → ephemeral in-memory DuckDB (default)
            "duckdb://path" → persist to DuckDB file
            "csv://dir"     → write spoke CSVs to directory
            "sql://path"    → write SQL INSERT statements to file

    Returns: Substrate

Substrate
---------
    substrate.query(constraints) -> list[str]   # entity_ids
    substrate.dimensions()       -> list[str]   # dimensions present
    substrate.count()            -> int         # total fact rows
    substrate.lens_id            -> str
    substrate.describe()         -> dict        # summary stats

Conformance
-----------
Spoke table schema (matches JS lens-tool output and all other SNF substrates):
    entity_id    TEXT    — resolved nucleus value (with prefix if declared)
    dimension    TEXT    — lowercase dimension name
    semantic_key TEXT    — semantic key from coordinate_map
    value        TEXT    — stringified source value
    coordinate   TEXT    — DIMENSION|semantic_key|value  (triadic string)
    lens_id      TEXT    — lens_id from the lens

Errors
------
CompileError        — raised for structural problems (missing nucleus, etc.)
NucleusError        — raised when a row has no resolvable nucleus value
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Union

try:
    import pandas as pd
except ImportError:
    raise ImportError("compile.py requires pandas. Install with: pip install pandas")

try:
    import duckdb
except ImportError:
    raise ImportError("compile.py requires duckdb. Install with: pip install duckdb")


# ─────────────────────────────────────────────────────────────────────────────
# Errors
# ─────────────────────────────────────────────────────────────────────────────

class CompileError(ValueError):
    """Raised for structural problems during compilation."""
    pass


class NucleusError(CompileError):
    """Raised when a row has no resolvable nucleus value."""
    def __init__(self, row_index, fields):
        self.row_index = row_index
        self.fields    = fields
        super().__init__(
            f"Row {row_index}: nucleus field(s) {fields} are null or empty. "
            f"Every row must have a non-null nucleus value."
        )


# ─────────────────────────────────────────────────────────────────────────────
# Spoke table DDL
# ─────────────────────────────────────────────────────────────────────────────

_SPOKE_DDL = """
CREATE TABLE IF NOT EXISTS snf_spoke (
    entity_id    TEXT NOT NULL,
    dimension    TEXT NOT NULL,
    semantic_key TEXT NOT NULL,
    value        TEXT NOT NULL,
    coordinate   TEXT NOT NULL,
    lens_id      TEXT NOT NULL
)
"""

_SPOKE_INDEX_DDL = [
    "CREATE INDEX IF NOT EXISTS idx_dimension    ON snf_spoke (dimension)",
    "CREATE INDEX IF NOT EXISTS idx_coordinate   ON snf_spoke (coordinate)",
    "CREATE INDEX IF NOT EXISTS idx_entity_id    ON snf_spoke (entity_id)",
    "CREATE INDEX IF NOT EXISTS idx_dim_key_val  ON snf_spoke (dimension, semantic_key, value)",
]


# ─────────────────────────────────────────────────────────────────────────────
# Nucleus resolution
# ─────────────────────────────────────────────────────────────────────────────

def _resolve_nucleus(row, nucleus, row_index):
    """
    Resolve the entity_id for a single row from the nucleus declaration.

    Single:    prefix + ":" + str(row[field])
               or just str(row[field]) if no prefix
    Composite: prefix + ":" + val1 + separator + val2 + ...
               or just val1 + separator + val2 if no prefix

    Raises NucleusError if any nucleus field is null/empty.
    """
    nuc_type = nucleus["type"]
    prefix   = nucleus.get("prefix", "").strip()

    if nuc_type == "single":
        field = nucleus["field"]
        raw   = row.get(field)
        if raw is None or str(raw).strip() == "" or str(raw).lower() == "nan":
            raise NucleusError(row_index, [field])
        val = str(raw).strip()
        return f"{prefix}:{val}" if prefix else val

    elif nuc_type == "composite":
        fields    = nucleus["fields"]
        separator = nucleus.get("separator", "-")
        parts     = []
        missing   = []
        for f in fields:
            raw = row.get(f)
            if raw is None or str(raw).strip() == "" or str(raw).lower() == "nan":
                missing.append(f)
            else:
                parts.append(str(raw).strip())
        if missing:
            raise NucleusError(row_index, missing)
        val = separator.join(parts)
        return f"{prefix}:{val}" if prefix else val

    else:
        raise CompileError(f"Unknown nucleus type: '{nuc_type}'")


# ─────────────────────────────────────────────────────────────────────────────
# Row compilation
# ─────────────────────────────────────────────────────────────────────────────

def _compile_row(row, row_index, coordinate_map, nucleus, lens_id):
    """
    Compile a single source row into a list of spoke fact tuples.

    Returns list of (entity_id, dimension, semantic_key, value, coordinate, lens_id)
    Skips columns not in coordinate_map.
    Skips null/empty values silently — missing data is not an error.
    """
    entity_id = _resolve_nucleus(row, nucleus, row_index)
    facts     = []

    for col, mapping in coordinate_map.items():
        raw = row.get(col)

        # Skip nulls and empty strings — missing data is not emitted
        if raw is None or str(raw).strip() == "" or str(raw).lower() == "nan":
            continue

        dim = mapping["dimension"].lower()
        key = mapping["semantic_key"].lower()
        val = str(raw).strip()

        # Coordinate is the triadic string: DIMENSION|semantic_key|value
        coord = f"{dim.upper()}|{key}|{val}"

        facts.append((entity_id, dim, key, val, coord, lens_id))

    return facts


# ─────────────────────────────────────────────────────────────────────────────
# Substrate
# ─────────────────────────────────────────────────────────────────────────────

class Substrate:
    """
    Compiled SNF substrate backed by DuckDB.

    Not constructed directly — use compile_data().

    The substrate holds a DuckDB connection with an snf_spoke table.
    Query execution is via SQL against this table.
    The DuckDB connection is reused across queries — compile once, query many times.
    This is the engine property: Reckoner can load a Substrate once
    and call query() repeatedly without recompiling.
    """

    def __init__(self, conn, lens_id, source_path=None):
        self._conn       = conn
        self._lens_id    = lens_id
        self._source     = str(source_path) if source_path else None

    @property
    def lens_id(self):
        return self._lens_id

    def query(self, constraints):
        """
        Execute a list of Portolan-format constraint dicts against the substrate.

        This is the low-level engine query. For Peirce string queries use
        peirce.query(substrate, query_string) which calls this internally.

        constraints: list of dicts from parse_to_constraints()
            [{"category": "WHO", "field": "artist", "op": "eq", "value": "Miles Davis"}, ...]

        Returns list of entity_id strings satisfying all constraints.

        Boolean semantics:
            AND across dimensions = intersection
            OR within dimension   = union (same dimension, multiple constraints)
            between               = range scan on value (lexicographic for strings)
        """
        if not constraints:
            return []

        # Group constraints by dimension for union/intersection logic
        from collections import defaultdict
        by_dim = defaultdict(list)
        for c in constraints:
            dim = (c.get("category") or c.get("dimension") or "").upper()
            by_dim[dim].append(c)

        # For each dimension, build the set of entity_ids (union within dim)
        # Then intersect across dimensions
        dim_sets = []
        for dim, dim_constraints in by_dim.items():
            ids = self._query_dimension(dim, dim_constraints)
            dim_sets.append(set(ids))

        if not dim_sets:
            return []

        result = dim_sets[0]
        for s in dim_sets[1:]:
            result = result & s

        return sorted(result)

    def _query_dimension(self, dimension, constraints):
        """
        Return entity_ids matching ANY of the constraints in this dimension (union).
        """
        entity_ids = set()
        for c in constraints:
            ids = self._query_single_constraint(dimension, c)
            entity_ids.update(ids)
        return entity_ids

    def _query_single_constraint(self, dimension, c):
        """
        Execute a single constraint against the spoke table.
        Returns list of matching entity_ids.
        """
        op    = c.get("op", "eq")
        key   = (c.get("field") or "").lower()
        value = c.get("value")
        dim   = dimension.lower()

        # NOT negation: flip the operator before execution
        if c.get("negated"):
            op = {
                "eq":     "not_eq",
                "not_eq": "eq",
                "gt":     "lte",
                "lt":     "gte",
                "gte":    "lt",
                "lte":    "gt",
            }.get(op, op)

        

        if op == "eq":
            rows = self._conn.execute(
                "SELECT DISTINCT entity_id FROM snf_spoke "
                "WHERE dimension = ? AND semantic_key = ? AND value = ? AND lens_id = ?",
                [dim, key, str(value), self._lens_id]
            ).fetchall()

        elif op == "not_eq":
            rows = self._conn.execute(
                "SELECT DISTINCT entity_id FROM snf_spoke "
                "WHERE dimension = ? AND semantic_key = ? AND value != ? AND lens_id = ?",
                [dim, key, str(value), self._lens_id]
            ).fetchall()

        elif op == "gt":
            rows = self._conn.execute(
                "SELECT DISTINCT entity_id FROM snf_spoke "
                "WHERE dimension = ? AND semantic_key = ? AND value > ? AND lens_id = ?",
                [dim, key, str(value), self._lens_id]
            ).fetchall()

        elif op == "lt":
            rows = self._conn.execute(
                "SELECT DISTINCT entity_id FROM snf_spoke "
                "WHERE dimension = ? AND semantic_key = ? AND value < ? AND lens_id = ?",
                [dim, key, str(value), self._lens_id]
            ).fetchall()

        elif op == "gte":
            rows = self._conn.execute(
                "SELECT DISTINCT entity_id FROM snf_spoke "
                "WHERE dimension = ? AND semantic_key = ? AND value >= ? AND lens_id = ?",
                [dim, key, str(value), self._lens_id]
            ).fetchall()

        elif op == "lte":
            rows = self._conn.execute(
                "SELECT DISTINCT entity_id FROM snf_spoke "
                "WHERE dimension = ? AND semantic_key = ? AND value <= ? AND lens_id = ?",
                [dim, key, str(value), self._lens_id]
            ).fetchall()

        elif op == "contains":
            rows = self._conn.execute(
                "SELECT DISTINCT entity_id FROM snf_spoke "
                "WHERE dimension = ? AND semantic_key = ? AND value LIKE ? AND lens_id = ?",
                [dim, key, f"%{value}%", self._lens_id]
            ).fetchall()

        elif op == "prefix":
            rows = self._conn.execute(
                "SELECT DISTINCT entity_id FROM snf_spoke "
                "WHERE dimension = ? AND semantic_key = ? AND value LIKE ? AND lens_id = ?",
                [dim, key, f"{value}%", self._lens_id]
            ).fetchall()

        elif op == "between":
            value2 = c.get("value2", value)
            rows = self._conn.execute(
                "SELECT DISTINCT entity_id FROM snf_spoke "
                "WHERE dimension = ? AND semantic_key = ? "
                "AND value >= ? AND value <= ? AND lens_id = ?",
                [dim, key, str(value), str(value2), self._lens_id]
            ).fetchall()

        else:
            raise CompileError(f"Unknown operator: '{op}'")

        return [r[0] for r in rows]

    def dimensions(self):
        """Return list of dimensions present in this substrate."""
        rows = self._conn.execute(
            "SELECT DISTINCT dimension FROM snf_spoke WHERE lens_id = ? ORDER BY dimension",
            [self._lens_id]
        ).fetchall()
        return [r[0] for r in rows]

    def count(self):
        """Return total number of fact rows in the substrate."""
        row = self._conn.execute(
            "SELECT COUNT(*) FROM snf_spoke WHERE lens_id = ?",
            [self._lens_id]
        ).fetchone()
        return row[0]

    def entity_count(self):
        """Return number of distinct entity_ids."""
        row = self._conn.execute(
            "SELECT COUNT(DISTINCT entity_id) FROM snf_spoke WHERE lens_id = ?",
            [self._lens_id]
        ).fetchone()
        return row[0]

    def describe(self):
        """Return a summary dict — useful for diagnostics and Reckoner engine calls."""
        dims = self.dimensions()
        dim_counts = {}
        for d in dims:
            row = self._conn.execute(
                "SELECT COUNT(*) FROM snf_spoke WHERE dimension = ? AND lens_id = ?",
                [d, self._lens_id]
            ).fetchone()
            dim_counts[d] = row[0]

        return {
            "lens_id":      self._lens_id,
            "entity_count": self.entity_count(),
            "fact_count":   self.count(),
            "dimensions":   dims,
            "facts_by_dim": dim_counts,
            "source":       self._source,
        }

    def to_dataframe(self):
        """Return the full spoke table as a pandas DataFrame."""
        return self._conn.execute(
            "SELECT entity_id, dimension, semantic_key, value, coordinate, lens_id "
            "FROM snf_spoke WHERE lens_id = ? ORDER BY entity_id, dimension",
            [self._lens_id]
        ).df()

    def __repr__(self):
        try:
            d = self.describe()
            return (
                f"Substrate(lens_id='{d['lens_id']}', "
                f"entities={d['entity_count']}, "
                f"facts={d['fact_count']}, "
                f"dimensions={d['dimensions']})"
            )
        except Exception:
            return f"Substrate(lens_id='{self._lens_id}')"

    def _repr_html_(self):
        try:
            d = self.describe()
            dim_rows = "".join(
                f"<tr><td style='padding:3px 8px'>{dim}</td>"
                f"<td style='padding:3px 8px;text-align:right'>{count:,}</td></tr>"
                for dim, count in sorted(d["facts_by_dim"].items())
            )
            return (
                f"<div style='font-family:sans-serif'>"
                f"<p style='margin:4px 0'><strong>Substrate</strong> — "
                f"lens: <code>{d['lens_id']}</code></p>"
                f"<p style='margin:4px 0;color:#555'>"
                f"{d['entity_count']:,} entities · {d['fact_count']:,} facts</p>"
                f"<table style='border-collapse:collapse;margin-top:6px'>"
                f"<thead><tr style='border-bottom:1px solid #ddd'>"
                f"<th style='padding:3px 8px;text-align:left'>Dimension</th>"
                f"<th style='padding:3px 8px;text-align:right'>Facts</th>"
                f"</tr></thead>"
                f"<tbody>{dim_rows}</tbody>"
                f"</table></div>"
            )
        except Exception:
            return f"<code>Substrate(lens_id='{self._lens_id}')</code>"


# ─────────────────────────────────────────────────────────────────────────────
# Output writers
# ─────────────────────────────────────────────────────────────────────────────

def _write_csv(conn, lens_id, out_dir):
    """Write spoke facts to CSV files in out_dir, one per dimension."""
    import csv
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    dims = conn.execute(
        "SELECT DISTINCT dimension FROM snf_spoke WHERE lens_id = ?", [lens_id]
    ).fetchall()

    for (dim,) in dims:
        rows = conn.execute(
            "SELECT entity_id, dimension, semantic_key, value, coordinate, lens_id "
            "FROM snf_spoke WHERE dimension = ? AND lens_id = ? ORDER BY entity_id",
            [dim, lens_id]
        ).fetchall()
        csv_path = out_path / f"snf_{dim}.csv"
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["entity_id", "dimension", "semantic_key", "value", "coordinate", "lens_id"])
            writer.writerows(rows)


def _write_sql(conn, lens_id, sql_path):
    """Write spoke facts as SQL INSERT statements."""
    rows = conn.execute(
        "SELECT entity_id, dimension, semantic_key, value, coordinate, lens_id "
        "FROM snf_spoke WHERE lens_id = ? ORDER BY entity_id, dimension",
        [lens_id]
    ).fetchall()

    def esc(s):
        return s.replace("'", "''")

    path = Path(sql_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write("-- SNF spoke table export\n")
        f.write("-- Generated by compile.py\n\n")
        for row in rows:
            vals = ", ".join(f"'{esc(str(v))}'" for v in row)
            f.write(
                f"INSERT INTO snf_spoke "
                f"(entity_id, dimension, semantic_key, value, coordinate, lens_id) "
                f"VALUES ({vals});\n"
            )


def _write_duckdb(conn, lens_id, db_path):
    """Persist to a DuckDB file at db_path."""
    persist_conn = duckdb.connect(str(db_path))
    persist_conn.execute(_SPOKE_DDL)
    rows = conn.execute(
        "SELECT entity_id, dimension, semantic_key, value, coordinate, lens_id "
        "FROM snf_spoke WHERE lens_id = ?",
        [lens_id]
    ).fetchall()
    persist_conn.executemany(
        "INSERT INTO snf_spoke VALUES (?, ?, ?, ?, ?, ?)", rows
    )
    for idx_ddl in _SPOKE_INDEX_DDL:
        persist_conn.execute(idx_ddl)
    persist_conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# compile_data()
# ─────────────────────────────────────────────────────────────────────────────

def compile_data(source, lens, into=None):
    """
    Compile source data using a lens into a queryable Substrate.

    Args:
        source: pd.DataFrame, str path, or pathlib.Path to a CSV
        lens:   dict (from lens.load() or draft.to_lens())
                or str/Path to a lens JSON file
        into:   None           → ephemeral in-memory DuckDB (default)
                "duckdb://path" → persist to DuckDB file
                "csv://dir"     → write spoke CSVs to directory
                "sql://path"    → write SQL INSERT statements to file

    Returns:
        Substrate — compiled, queryable, reusable

    Raises:
        CompileError    — structural problems (bad lens, bad into path)
        NucleusError    — a row has no resolvable nucleus value
        TypeError       — wrong source type
    """
    # ── Load lens if path given ──────────────────────────────────────────────
    if isinstance(lens, (str, Path)):
        with open(lens, "r", encoding="utf-8") as f:
            lens = json.load(f)

    if not isinstance(lens, dict):
        raise CompileError(f"lens must be a dict or path to a JSON file. Got {type(lens)}")

    lens_id        = lens.get("lens_id")
    coordinate_map = lens.get("coordinate_map", {})
    nucleus        = lens.get("nucleus")

    if not lens_id:
        raise CompileError("lens missing 'lens_id'")
    if not coordinate_map:
        raise CompileError("lens missing 'coordinate_map' or it is empty")
    if not nucleus:
        raise CompileError("lens missing 'nucleus'")

    # ── Load source data ─────────────────────────────────────────────────────
    if isinstance(source, (str, Path)):
        df = pd.read_csv(source)
    elif isinstance(source, pd.DataFrame):
        df = source
    else:
        raise TypeError(f"source must be a DataFrame, str path, or Path. Got {type(source)}")

    # ── Validate nucleus fields exist in dataframe ───────────────────────────
    nuc_type = nucleus.get("type")
    if nuc_type == "single":
        nuc_fields = [nucleus["field"]]
    elif nuc_type == "composite":
        nuc_fields = nucleus["fields"]
    else:
        raise CompileError(f"nucleus type must be 'single' or 'composite', got '{nuc_type}'")

    missing_nuc_cols = [f for f in nuc_fields if f not in df.columns]
    if missing_nuc_cols:
        raise CompileError(
            f"Nucleus field(s) {missing_nuc_cols} not found in source data. "
            f"Available columns: {list(df.columns)}"
        )

    # ── Warn about coordinate_map columns not in dataframe ──────────────────
    missing_map_cols = [c for c in coordinate_map if c not in df.columns]
    if missing_map_cols:
        # Not fatal — just skip those columns during compilation
        coordinate_map = {k: v for k, v in coordinate_map.items() if k in df.columns}

    # ── Build in-memory DuckDB ───────────────────────────────────────────────
    conn = duckdb.connect(":memory:")
    conn.execute(_SPOKE_DDL)

    # ── Compile rows ─────────────────────────────────────────────────────────
    all_facts = []
    rows_dict = df.to_dict(orient="records")

    for i, row in enumerate(rows_dict):
        facts = _compile_row(row, i, coordinate_map, nucleus, lens_id)
        all_facts.extend(facts)

    if all_facts:
        conn.executemany(
            "INSERT INTO snf_spoke (entity_id, dimension, semantic_key, value, coordinate, lens_id) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            all_facts
        )

    # Build indexes for query performance
    for idx_ddl in _SPOKE_INDEX_DDL:
        conn.execute(idx_ddl)

    # ── Handle into= output paths ────────────────────────────────────────────
    if into is not None:
        if not isinstance(into, str):
            raise CompileError(f"into must be a string like 'csv://dir', got {type(into)}")

        if into.startswith("csv://"):
            out_dir = into[6:]
            _write_csv(conn, lens_id, out_dir)

        elif into.startswith("duckdb://"):
            db_path = into[9:]
            _write_duckdb(conn, lens_id, db_path)

        elif into.startswith("sql://"):
            sql_path = into[6:]
            _write_sql(conn, lens_id, sql_path)

        else:
            raise CompileError(
                f"Unrecognised into= format: '{into}'. "
                f"Use 'csv://dir', 'duckdb://path', or 'sql://path'."
            )

    source_path = source if isinstance(source, (str, Path)) else None
    return Substrate(conn, lens_id, source_path=source_path)
