"""
peirce.py — peirce.query(), peirce.execute(), ResultSet

Week 4 of the Python SNF package.

Connects the parser (parser.py) to the substrate (compile.py) and
returns results as a ResultSet — the primary user-facing query object.

Public API
----------
query(substrate, query_string, limit=20) -> ResultSet
    Parse a Peirce query string and execute it against a Substrate.
    Raises PeirceParseError on invalid syntax.
    Raises PeirceDiscoveryError if given a discovery expression —
    discovery expressions do not execute against Portolan.

execute(constraints, substrate) -> ResultSet
    Execute pre-parsed constraints (from parse_to_constraints) against
    a Substrate. For callers that already have parsed constraints —
    e.g. Reckoner building constraints from chips without a Peirce string.

ResultSet
---------
    result.entity_ids         -> list[str]
    result.count              -> int
    result.to_dataframe()     -> pd.DataFrame   (full spoke rows for these entities)
    result.__iter__()         -> iterates entity_ids
    result._repr_html_()      -> Jupyter inline table
    result.__repr__()         -> plain text summary

Errors
------
PeirceParseError      — invalid Peirce syntax. Carries .error, .position, .token
                        matching the JS error envelope exactly.
PeirceDiscoveryError  — discovery expression given to query() — wrong entry point.

DNF semantics
-------------
A query with top-level OR produces multiple conjuncts in parse_to_constraints().
Each conjunct is executed independently against the substrate.
Results are unioned across conjuncts.
This matches JS behaviour and the formal SNF Boolean semantics.

    (WHO.artist = "Miles Davis" AND WHEN.released = "1959")
    OR
    (WHO.artist = "John Coltrane" AND WHEN.released = "1964")

    → execute conjunct 1 → {entity_A}
    → execute conjunct 2 → {entity_B}
    → union             → {entity_A, entity_B}
"""

from __future__ import annotations

from typing import Union

try:
    import pandas as pd
except ImportError:
    raise ImportError("peirce.py requires pandas. Install with: pip install pandas")


# ─────────────────────────────────────────────────────────────────────────────
# Errors
# ─────────────────────────────────────────────────────────────────────────────

class PeirceParseError(ValueError):
    """
    Raised when a Peirce query string fails to parse.

    Attributes match the JS error envelope exactly:
        error    — human-readable error message
        position — character position in the input string
        token    — the token that caused the failure (may be None)
    """
    def __init__(self, error, position=0, token=None):
        self.error    = error
        self.position = position
        self.token    = token
        super().__init__(error)

    def __repr__(self):
        return (
            f"PeirceParseError(error={self.error!r}, "
            f"position={self.position}, token={self.token!r})"
        )


class PeirceDiscoveryError(ValueError):
    """
    Raised when a discovery expression is passed to query().

    Discovery expressions (*,  WHO|*, WHO|role|*) do not execute
    against Portolan. They route to the affordances/discovery layer.
    Use peirce.discover() for those.
    """
    def __init__(self, scope, dimension=None, field=None):
        self.scope     = scope
        self.dimension = dimension
        self.field     = field
        msg = {
            "all":       "Discovery expression '*' lists dimensions — use peirce.discover().",
            "dimension": f"Discovery expression '{dimension}|*' lists semantic keys — use peirce.discover().",
            "field":     f"Discovery expression '{dimension}|{field}|*' lists values — use peirce.discover().",
        }.get(scope, "Discovery expression given to query() — use peirce.discover().")
        super().__init__(msg)


# ─────────────────────────────────────────────────────────────────────────────
# ResultSet
# ─────────────────────────────────────────────────────────────────────────────

class ResultSet:
    """
    The result of a Peirce query execution.

    Holds entity_ids and provides access to the full spoke rows
    for those entities via to_dataframe().

    Jupyter-renderable via _repr_html_().
    Plain-text via __repr__().
    Iterable — yields entity_ids.
    """

    def __init__(self, entity_ids, substrate, query_string=None, limit=None):
        """
        Not constructed directly — returned by peirce.query() and peirce.execute().

        entity_ids:   list of matching entity_id strings (already limit-applied)
        substrate:    the Substrate this result came from
        query_string: the original Peirce string (for display)
        limit:        the limit that was applied (for display)
        """
        self._entity_ids   = list(entity_ids)
        self._substrate    = substrate
        self._query_string = query_string
        self._limit        = limit

    # ── Primary interface ────────────────────────────────────────────────────

    @property
    def entity_ids(self):
        """List of matching entity_id strings."""
        return list(self._entity_ids)

    @property
    def count(self):
        """Number of matching entities."""
        return len(self._entity_ids)

    def __iter__(self):
        return iter(self._entity_ids)

    def __len__(self):
        return len(self._entity_ids)

    # ── DataFrame projection ─────────────────────────────────────────────────

    def to_dataframe(self):
        """
        Return a pandas DataFrame of the full spoke rows for matching entities.

        Columns: entity_id, dimension, semantic_key, value, coordinate, lens_id

        If there are no matching entities, returns an empty DataFrame
        with the correct columns.
        """
        if not self._entity_ids:
            return pd.DataFrame(
                columns=["entity_id", "dimension", "semantic_key", "value", "coordinate", "lens_id"]
            )

        # Pull the spoke rows for these entity_ids from the substrate
        # Use parameterised IN clause — safe, no string interpolation
        placeholders = ", ".join("?" * len(self._entity_ids))
        rows = self._substrate._conn.execute(
            f"SELECT entity_id, dimension, semantic_key, value, coordinate, lens_id "
            f"FROM snf_spoke "
            f"WHERE entity_id IN ({placeholders}) "
            f"AND lens_id = ? "
            f"ORDER BY entity_id, dimension, semantic_key",
            self._entity_ids + [self._substrate.lens_id]
        ).fetchall()

        return pd.DataFrame(
            rows,
            columns=["entity_id", "dimension", "semantic_key", "value", "coordinate", "lens_id"]
        )

    def pivot(self):
        """
        Return a wide-format DataFrame: one row per entity, one column per semantic_key.

        Useful for inspection and export. Multi-valued fields are joined with " | ".
        """
        df = self.to_dataframe()
        if df.empty:
            return df

        # Pivot: entity_id as index, semantic_key as columns, value as cells
        # Multi-valued: join with " | "
        pivot = (
            df.groupby(["entity_id", "semantic_key"])["value"]
            .apply(lambda x: " | ".join(sorted(set(x))))
            .unstack(fill_value="")
            .reset_index()
        )
        return pivot

    # ── Jupyter rendering ────────────────────────────────────────────────────

    def _repr_html_(self):
        if not self._entity_ids:
            return (
                f"<div style='font-family:sans-serif;color:#888'>"
                f"<em>No results</em>"
                f"{self._query_badge()}"
                f"</div>"
            )

        try:
            df = self.pivot()
            if df.empty:
                return self._simple_html()

            # Build table header
            cols = list(df.columns)
            headers = "".join(
                f"<th style='padding:4px 8px;text-align:left;"
                f"border-bottom:2px solid #ddd'>{c}</th>"
                for c in cols
            )

            # Build table rows (up to 50 for display)
            display_df = df.head(50)
            body_rows  = []
            for _, row in display_df.iterrows():
                cells = "".join(
                    f"<td style='padding:3px 8px;border-bottom:1px solid #f0f0f0'>{row[c]}</td>"
                    for c in cols
                )
                body_rows.append(f"<tr>{cells}</tr>")

            truncated = ""
            if len(df) > 50:
                truncated = (
                    f"<p style='color:#888;font-size:0.85em'>"
                    f"Showing 50 of {self.count} entities.</p>"
                )

            return (
                f"<div style='font-family:sans-serif'>"
                f"{self._query_badge()}"
                f"<p style='margin:4px 0;color:#555'>{self.count} entit{'y' if self.count == 1 else 'ies'}</p>"
                f"<div style='overflow-x:auto'>"
                f"<table style='border-collapse:collapse;width:100%'>"
                f"<thead><tr>{headers}</tr></thead>"
                f"<tbody>{''.join(body_rows)}</tbody>"
                f"</table>"
                f"</div>"
                f"{truncated}"
                f"</div>"
            )
        except Exception:
            return self._simple_html()

    def _simple_html(self):
        """Fallback: just list entity_ids."""
        items = "".join(
            f"<li style='font-family:monospace'>{eid}</li>"
            for eid in self._entity_ids[:50]
        )
        return (
            f"<div style='font-family:sans-serif'>"
            f"{self._query_badge()}"
            f"<p style='margin:4px 0;color:#555'>{self.count} entit{'y' if self.count == 1 else 'ies'}</p>"
            f"<ul style='margin:4px 0'>{items}</ul>"
            f"</div>"
        )

    def _query_badge(self):
        if self._query_string:
            qs = self._query_string.replace("<", "&lt;").replace(">", "&gt;")
            return (
                f"<p style='margin:4px 0;font-family:monospace;font-size:0.85em;"
                f"color:#555;background:#f5f5f5;padding:3px 6px;border-radius:3px'>"
                f"{qs}</p>"
            )
        return ""

    # ── Plain text ───────────────────────────────────────────────────────────

    def __repr__(self):
        if not self._entity_ids:
            qs = f" for '{self._query_string}'" if self._query_string else ""
            return f"ResultSet(0 results{qs})"

        limit_note = f", limit={self._limit}" if self._limit else ""
        qs = f"\n  query: {self._query_string}" if self._query_string else ""
        ids_preview = self._entity_ids[:5]
        more = f" ... +{self.count - 5} more" if self.count > 5 else ""
        return (
            f"ResultSet({self.count} result{'s' if self.count != 1 else ''}"
            f"{limit_note}){qs}\n"
            f"  entities: {ids_preview}{more}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Internal execution
# ─────────────────────────────────────────────────────────────────────────────

def _expand_only(constraint, substrate):
    """
    Expand an ONLY constraint into a set of entity_ids via set difference.

    ONLY semantics: entities where field = value AND field has no other values.

    Example: WHAT.color ONLY 'Red'
        → entities with color = Red
        MINUS entities with color = anything other than Red

    Strategy:
        1. Fetch entities matching eq(value)  — the candidates
        2. Fetch all values for this field via _run_discovery (field scope)
        3. For each other value, fetch entities matching eq(other_value)
        4. Return candidates minus the union of all other-value sets

    This is two discovery/query passes plus one set difference per other value.
    It does not generate a NOT IN chain — the substrate never sees ONLY.

    Raises ValueError if op is not 'only'. Caller is responsible for routing.
    """
    op = constraint.get("op")
    if op != "only":
        raise ValueError(f"_expand_only called with op={op!r}, expected 'only'")

    dim   = (constraint.get("category") or constraint.get("dimension") or "").upper()
    field = (constraint.get("field") or "").lower()
    value = str(constraint.get("value", ""))

    # Step 1: entities that have this value
    eq_constraint = {"category": dim, "field": field, "op": "eq", "value": value}
    candidates = set(substrate.query([eq_constraint]))

    if not candidates:
        return set()

    # Step 2: all values for this field
    discovery = _run_discovery(substrate, "field", dim, field, limit=None)
    all_values = [row["value"] for row in discovery.rows]

    # Step 3: for each other value, find entities that have it and subtract
    contaminated = set()
    for other_value in all_values:
        if str(other_value) == value:
            continue
        other_constraint = {"category": dim, "field": field, "op": "eq", "value": other_value}
        contaminated |= set(substrate.query([other_constraint]))

    # Step 4: candidates that have no other values for this field
    return candidates - contaminated


def _execute_conjunct(conjunct, substrate):
    """
    Execute a single conjunct (flat list of Portolan constraints)
    against the substrate. Returns a set of entity_ids.

    ONLY constraints are expanded before the conjunct reaches substrate.query().
    The substrate never sees op='only'.
    """
    if not conjunct:
        return set()

    # Partition: separate ONLY constraints from everything else
    only_constraints  = [c for c in conjunct if c.get("op") == "only"]
    plain_constraints = [c for c in conjunct if c.get("op") != "only"]

    # Execute plain constraints normally
    result = set(substrate.query(plain_constraints)) if plain_constraints else None

    # Expand each ONLY constraint and intersect into result
    for c in only_constraints:
        only_ids = _expand_only(c, substrate)
        if result is None:
            result = only_ids
        else:
            result = result & only_ids

    return result if result is not None else set()


def _execute_dnf(conjuncts, substrate):
    """
    Execute a DNF query (list of conjuncts) against the substrate.

    Each conjunct is executed independently.
    Results are unioned across conjuncts.

    ONLY constraints are treated as global filters — they apply across
    all conjuncts, not just the one they appear in. This is correct
    semantics: ONLY "RCA" means the entity has only RCA as a label,
    regardless of which title OR branch matched it.

    Returns a sorted list of entity_ids.
    """
    # Collect ONLY constraints from all conjuncts — they are global filters.
    # Also strip them from conjuncts so _execute_conjunct doesn't double-apply.
    global_only = []
    stripped_conjuncts = []
    for conjunct in conjuncts:
        only = [c for c in conjunct if c.get("op") == "only"]
        rest = [c for c in conjunct if c.get("op") != "only"]
        global_only.extend(only)
        stripped_conjuncts.append(rest)

    # Deduplicate ONLY constraints (same dim+field+value from multiple conjuncts)
    seen_only = set()
    deduped_only = []
    for c in global_only:
        key = (c.get("category") or c.get("dimension"), c.get("field"), c.get("value"))
        if key not in seen_only:
            seen_only.add(key)
            deduped_only.append(c)

    # Union across conjuncts (ONLY already stripped)
    result = set()
    for conjunct in stripped_conjuncts:
        if conjunct:  # skip empty conjuncts
            result |= set(substrate.query(conjunct))

    # Apply global ONLY filters as intersection
    for c in deduped_only:
        only_ids = _expand_only(c, substrate)
        result = result & only_ids

    return sorted(result)


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def query(substrate, query_string, limit=20):
    """
    Parse a Peirce query string and execute it against a Substrate.

    Args:
        substrate:    a compiled Substrate (from compile_data())
        query_string: a Peirce query string
        limit:        maximum number of entity_ids to return (default 20)
                      pass None or 0 for no limit

    Returns:
        ResultSet

    Raises:
        PeirceParseError      — invalid Peirce syntax
        PeirceDiscoveryError  — discovery expression given (wrong entry point)
        TypeError             — substrate is not a Substrate instance
    """
    # Import here to avoid circular import if ever reorganised into a package
    from snf_peirce.parser import parse_to_constraints
    from snf_peirce.compile import Substrate as _Substrate

    if not hasattr(substrate, 'query') and not hasattr(substrate, 'execute'):
        raise TypeError(
            f"substrate must have a .query() or .execute() method. "
            f"Got: {type(substrate)}"
        )

    # Parse
    parsed = parse_to_constraints(query_string)

    if not parsed["success"]:
        raise PeirceParseError(
            error    = parsed["error"],
            position = parsed.get("position", 0),
            token    = parsed.get("token"),
        )

    # Discovery expressions do not execute against Portolan
    if parsed["type"] == "discovery":
        raise PeirceDiscoveryError(
            scope     = parsed["scope"],
            dimension = parsed.get("dimension"),
            field     = parsed.get("field"),
        )

    # Execute DNF
    conjuncts   = parsed["conjuncts"]
    entity_ids  = _execute_dnf(conjuncts, substrate)

    # Apply limit
    if limit:
        entity_ids = entity_ids[:limit]

    return ResultSet(
        entity_ids   = entity_ids,
        substrate    = substrate,
        query_string = query_string,
        limit        = limit,
    )


def execute(constraints, substrate):
    """
    Execute pre-parsed constraints against a Substrate.

    For callers that already have parsed constraints — e.g. Reckoner
    building constraints from chips without going through a Peirce string.

    Args:
        constraints: list of Portolan-format constraint dicts, or
                     a parse_to_constraints() result dict with "conjuncts"
        substrate:   a compiled Substrate

    Returns:
        ResultSet (no limit applied — caller controls)

    Raises:
        TypeError      — substrate is not a Substrate instance
        ValueError     — constraints shape is invalid
    """
    from snf_peirce.compile import Substrate as _Substrate

    if not hasattr(substrate, 'query') and not hasattr(substrate, 'execute'):
        raise TypeError(
            f"substrate must be a Substrate instance. Got {type(substrate)}"
        )

    # Accept either a flat list of constraints (single conjunct)
    # or a parse_to_constraints() result dict
    if isinstance(constraints, dict):
        if not constraints.get("success"):
            raise ValueError(
                f"constraints dict has success=False: {constraints.get('error')}"
            )
        if constraints.get("type") == "discovery":
            raise PeirceDiscoveryError(
                scope     = constraints["scope"],
                dimension = constraints.get("dimension"),
                field     = constraints.get("field"),
            )
        conjuncts = constraints.get("conjuncts", [])
    elif isinstance(constraints, list):
        # Flat list — treat as single conjunct
        conjuncts = [constraints]
    else:
        raise ValueError(
            f"constraints must be a list of constraint dicts or a "
            f"parse_to_constraints() result. Got {type(constraints)}"
        )

    entity_ids = _execute_dnf(conjuncts, substrate)

    return ResultSet(
        entity_ids   = entity_ids,
        substrate    = substrate,
        query_string = None,
        limit        = None,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Discovery API
# ─────────────────────────────────────────────────────────────────────────────

def discover(substrate, expression, limit=None):
    """
    Execute a Peirce discovery expression against a substrate.

    Discovery expressions explore what's in a substrate — they don't
    return entity_ids, they return schema information.

    Supported expressions:
        *               — list all dimensions with fact counts
        WHO|*           — list all fields in WHO with fact counts
        WHO|artist|*    — list all values for WHO.artist with fact counts

    Args:
        substrate:   a compiled Substrate (from compile_data())
        expression:  a Peirce discovery expression string
        limit:       maximum number of rows to return (default None = all)

    Returns:
        DiscoveryResult

    Raises:
        PeirceParseError  — if the expression is not a valid discovery expression
    """
    from snf_peirce.parser import parse_to_constraints

    parsed = parse_to_constraints(expression)

    if not parsed["success"]:
        raise PeirceParseError(
            error    = parsed["error"],
            position = parsed.get("position", 0),
            token    = parsed.get("token"),
        )

    if parsed["type"] != "discovery":
        raise PeirceParseError(
            error    = f"Not a discovery expression: {expression!r}",
            position = 0,
            token    = None,
        )

    scope     = parsed["scope"]
    dimension = parsed.get("dimension")
    field     = parsed.get("field")

    return _run_discovery(substrate, scope, dimension, field, limit=limit)


def _run_discovery(substrate, scope, dimension, field, limit=None):
    """
    Run a discovery query against the substrate.
    Returns a DiscoveryResult.

    limit=None means no limit — return everything.
    """
    rows     = []
    limit_sql = f"LIMIT {limit}" if limit else ""

    try:
        conn = substrate._conn

        if scope == "all":
            # No limit on dimensions — there are only 6
            result = conn.execute(
                "SELECT dimension, COUNT(DISTINCT entity_id) as entities, "
                "COUNT(*) as facts "
                "FROM snf_spoke "
                "WHERE lens_id = ? "
                "GROUP BY dimension "
                "ORDER BY facts DESC",
                [substrate.lens_id]
            ).fetchall()
            rows = [
                {"dimension": r[0], "entities": r[1], "facts": r[2]}
                for r in result
            ]

        elif scope == "dimension":
            result = conn.execute(
                "SELECT semantic_key, COUNT(DISTINCT entity_id) as entities, "
                "COUNT(*) as facts "
                "FROM snf_spoke "
                "WHERE dimension = ? AND lens_id = ? "
                "GROUP BY semantic_key "
                f"ORDER BY facts DESC {limit_sql}",
                [dimension.lower(), substrate.lens_id]
            ).fetchall()
            rows = [
                {"semantic_key": r[0], "entities": r[1], "facts": r[2]}
                for r in result
            ]

        elif scope == "field":
            result = conn.execute(
                "SELECT value, COUNT(DISTINCT entity_id) as entities "
                "FROM snf_spoke "
                "WHERE dimension = ? AND semantic_key = ? AND lens_id = ? "
                "GROUP BY value "
                f"ORDER BY entities DESC {limit_sql}",
                [dimension.lower(), field, substrate.lens_id]
            ).fetchall()
            rows = [
                {"value": r[0], "entities": r[1]}
                for r in result
            ]

    except Exception:
        # Non-DuckDB substrate — return empty result gracefully
        rows = []

    return DiscoveryResult(
        scope     = scope,
        dimension = dimension,
        field     = field,
        rows      = rows,
    )


class DiscoveryResult:
    """
    The result of a Peirce discovery expression.

    Attributes:
        scope:      "all" | "dimension" | "field"
        dimension:  dimension name (if scope is "dimension" or "field")
        field:      field name (if scope is "field")
        rows:       list of dicts with discovery data
    """

    def __init__(self, scope, dimension, field, rows):
        self.scope     = scope
        self.dimension = dimension
        self.field     = field
        self.rows      = rows

    def __repr__(self):
        if self.scope == "all":
            lines = [f"  {'Dimension':<10}  {'Entities':>10}  {'Facts':>10}"]
            lines.append("  " + "─" * 34)
            for r in self.rows:
                lines.append(
                    f"  {r['dimension']:<10}  {r['entities']:>10,}  {r['facts']:>10,}"
                )
            return "\n".join(lines)

        elif self.scope == "dimension":
            lines = [f"  {self.dimension} fields\n"]
            lines.append(f"  {'Field':<40}  {'Entities':>10}")
            lines.append("  " + "─" * 53)
            for r in self.rows:
                lines.append(
                    f"  {r['semantic_key']:<40}  {r['entities']:>10,}"
                )
            return "\n".join(lines)

        elif self.scope == "field":
            lines = [f"  {self.dimension}.{self.field} values\n"]
            lines.append(f"  {'Value':<40}  {'Entities':>10}")
            lines.append("  " + "─" * 53)
            for r in self.rows:
                lines.append(
                    f"  {str(r['value']):<40}  {r['entities']:>10,}"
                )
            return "\n".join(lines)

        return f"DiscoveryResult(scope={self.scope}, rows={len(self.rows)})"

    def _repr_html_(self):
        """Jupyter inline display."""
        if not self.rows:
            return "<p style='color:#999'>No results</p>"

        if self.scope == "all":
            header = "<tr><th>Dimension</th><th>Entities</th><th>Facts</th></tr>"
            body   = "".join(
                f"<tr><td>{r['dimension']}</td>"
                f"<td style='text-align:right'>{r['entities']:,}</td>"
                f"<td style='text-align:right'>{r['facts']:,}</td></tr>"
                for r in self.rows
            )
        elif self.scope == "dimension":
            header = f"<tr><th>{self.dimension} fields</th><th>Entities</th></tr>"
            body   = "".join(
                f"<tr><td style='font-family:monospace'>{r['semantic_key']}</td>"
                f"<td style='text-align:right'>{r['entities']:,}</td></tr>"
                for r in self.rows
            )
        else:
            header = f"<tr><th>{self.dimension}.{self.field}</th><th>Entities</th></tr>"
            body   = "".join(
                f"<tr><td>{r['value']}</td>"
                f"<td style='text-align:right'>{r['entities']:,}</td></tr>"
                for r in self.rows
            )

        return (
            f"<table style='border-collapse:collapse;font-family:sans-serif;"
            f"font-size:0.9em'>"
            f"<thead style='background:#f5f5f5'>{header}</thead>"
            f"<tbody>{body}</tbody></table>"
        )
