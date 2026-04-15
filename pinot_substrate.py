"""
pinot_substrate.py — SNF PinotSubstrate

Read-only connection substrate for Apache Pinot.
Targets the standard SNF spoke schema:

    Per-dimension tables:  snf_who, snf_what, snf_when, snf_where, snf_why, snf_how
    Per-table columns:     entity_id     VARCHAR
                           semantic_key  VARCHAR   (e.g. "WHO.attorney=Smith")
                           coordinate    VARCHAR   (same as semantic_key in v1)
                           lens_id       VARCHAR

Execution strategy: sequential IN-clause chaining with cardinality-based
ordering. Dimensions with fewer matching entities are queried first,
keeping intermediate result sets small.

Interface contract (matches DuckDB Substrate):
    sub = PinotSubstrate(broker_url="http://localhost:8099")
    peirce.query(sub, 'WHO.attorney = "Smith" AND WHEN.year = "2024"')

The substrate is transparent to peirce.query() — it calls sub.execute(plan)
and receives a list of entity_id strings. No Pinot-specific code above
this layer.

Dependencies:
    requests  (stdlib-adjacent, always available)

No other dependencies. Pinot REST API only.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set

import requests

from plan import Constraint, DimensionGroup, SNFPlan


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DIMENSION_TABLE_SUFFIXES = {
    "WHO":   "who",
    "WHAT":  "what",
    "WHEN":  "when",
    "WHERE": "where",
    "WHY":   "why",
    "HOW":   "how",
}

# Maximum entity_ids to pass in a single IN clause.
MAX_IN_CLAUSE = 50_000

# Pinot SQL broker endpoint
_BROKER_SQL_PATH = "/query/sql"


# ---------------------------------------------------------------------------
# Internal result type
# ---------------------------------------------------------------------------

@dataclass
class _ExecutionResult:
    entity_ids: List[str]
    execution_time_ms: float
    probe_time_ms: float
    dimension_order: List[str]
    strategy: str
    pinot_time_ms: Optional[float] = None


# ---------------------------------------------------------------------------
# PinotSubstrate
# ---------------------------------------------------------------------------

class PinotSubstrate:
    """
    Read-only SNF substrate backed by Apache Pinot.

    Parameters
    ----------
    broker_url : str
        Pinot broker URL, e.g. "http://localhost:8099"
    table_prefix : str
        Table name prefix. Default "snf" → tables snf_who, snf_what, etc.
        Override if your cluster uses a different prefix.
    timeout : float
        HTTP request timeout in seconds. Default 30.
    skip_probe : bool
        If True, skip cardinality probing and execute in plan order.
        Useful for testing SQL generation without a live cluster.
    debug : bool
        Log SQL and intermediate result counts to stdout.
    """

    def __init__(
        self,
        broker_url: str,
        table_prefix: str = "snf",
        timeout: float = 30.0,
        skip_probe: bool = False,
        debug: bool = False,
    ):
        self.broker_url   = broker_url.rstrip("/")
        self.table_prefix = table_prefix
        self.timeout      = timeout
        self.skip_probe   = skip_probe
        self.debug        = debug

        self._tables: Dict[str, str] = {
            dim: f"{table_prefix}_{suffix}"
            for dim, suffix in _DIMENSION_TABLE_SUFFIXES.items()
        }

    # ------------------------------------------------------------------
    # Public interface — called by peirce.query()
    # ------------------------------------------------------------------

    def execute(self, plan: SNFPlan) -> List[str]:
        """
        Execute an SNFPlan against Pinot and return entity_id list.

        Parameters
        ----------
        plan : SNFPlan
            Compiled query plan from the Peirce parser.

        Returns
        -------
        list[str]
            Matched entity_ids. Empty list if no matches or UNSAT.
        """
        return self._run(plan).entity_ids

    def ping(self) -> bool:
        """
        Check connectivity to Pinot broker.
        Returns True if broker responds to a trivial query.
        """
        try:
            self._sql(f"SELECT 1 FROM {self._tables['WHO']} LIMIT 1")
            return True
        except Exception:
            return False

    def schema(self) -> Dict[str, List[str]]:
        """
        Return available semantic_key values per dimension.
        Useful for shell TAB completion and discovery expressions.
        """
        result = {}
        for dim, table in self._tables.items():
            sql = (
                f"SELECT DISTINCT semantic_key, COUNT(*) AS fact_count "
                f"FROM {table} "
                f"GROUP BY semantic_key "
                f"ORDER BY fact_count DESC "
                f"LIMIT 500"
            )
            try:
                rows = self._sql(sql)
                result[dim] = [r["semantic_key"] for r in rows]
            except Exception:
                result[dim] = []
        return result

    def explain(self, plan: SNFPlan) -> str:
        """
        Return the SQL that would be executed for this plan without
        hitting Pinot. Useful for \\explain in the shell.
        """
        if plan.unsatisfiable:
            return "UNSATISFIABLE — no SQL generated"

        groups = (
            list(plan.dimension_groups) if self.skip_probe
            else self._sorted_by_count(plan.dimension_groups)
        )
        lines = ["-- PinotSubstrate execution plan"]

        for i, group in enumerate(groups):
            table    = self._tables.get(group.dimension, "??")
            coords   = self._coordinates(group)
            card     = group.estimated_cardinality
            card_str = f"{card:,}" if card is not None else "unknown"

            lines.append(
                f"-- Step {i + 1}: {group.dimension} "
                f"(est. {card_str} candidates) → {table}"
            )
            if i == 0:
                sql, _ = self._anchor_sql(table, coords)
            else:
                sql, _ = self._filter_sql(table, coords, ["<prev_entity_ids>"])
            lines.append(sql)
            lines.append("")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Internal execution
    # ------------------------------------------------------------------

    def _run(self, plan: SNFPlan) -> _ExecutionResult:
        start = time.perf_counter()

        if plan.unsatisfiable:
            return _ExecutionResult(
                entity_ids=[], execution_time_ms=0.0, probe_time_ms=0.0,
                dimension_order=[], strategy="unsatisfiable",
            )

        if not plan.dimension_groups:
            return _ExecutionResult(
                entity_ids=[], execution_time_ms=0.0, probe_time_ms=0.0,
                dimension_order=[], strategy="empty_plan",
            )

        # Order dimensions by candidate count
        probe_start = time.perf_counter()
        groups = (
            list(plan.dimension_groups) if self.skip_probe
            else self._sorted_by_count(plan.dimension_groups)
        )
        probe_ms = (time.perf_counter() - probe_start) * 1000.0

        # Early exit if any dimension has zero candidates
        for group in groups:
            if getattr(group, "estimated_cardinality", None) == 0:
                return _ExecutionResult(
                    entity_ids=[],
                    execution_time_ms=(time.perf_counter() - start) * 1000.0,
                    probe_time_ms=probe_ms,
                    dimension_order=[g.dimension for g in groups],
                    strategy="short_circuit",
                )

        # Sequential IN-clause chaining
        current_ids: Optional[Set[str]] = None
        pinot_ms_total = 0.0

        for group in groups:
            table  = self._tables[group.dimension]
            coords = self._coordinates(group)

            if not coords:
                continue

            if current_ids is None:
                sql, _ = self._anchor_sql(table, coords)
            else:
                if not current_ids:
                    break
                sql, _ = self._filter_sql(table, coords, list(current_ids))

            if self.debug:
                print(f"[pinot] {group.dimension}: {sql[:120]}...")

            rows = self._sql(sql)
            pinot_ms_total += self._last_pinot_ms

            current_ids = {str(r["entity_id"]) for r in rows}

            if self.debug:
                print(f"[pinot] {group.dimension}: {len(current_ids)} candidates")

            if not current_ids:
                break

        entity_ids = sorted(current_ids) if current_ids else []

        return _ExecutionResult(
            entity_ids=entity_ids,
            execution_time_ms=(time.perf_counter() - start) * 1000.0,
            probe_time_ms=probe_ms,
            dimension_order=[g.dimension for g in groups],
            strategy="chained_in",
            pinot_time_ms=pinot_ms_total,
        )

    def _sorted_by_count(
        self, groups: List[DimensionGroup]
    ) -> List[DimensionGroup]:
        """
        Return groups ordered by ascending candidate count.
        Groups with a pre-set estimated_cardinality are not re-probed.
        """
        enriched = []
        for group in groups:
            card = (
                group.estimated_cardinality
                if group.estimated_cardinality is not None
                else self._probe_count(group)
            )
            enriched.append(
                DimensionGroup(
                    dimension=group.dimension,
                    constraints=group.constraints,
                    estimated_cardinality=card,
                    execution_step=group.execution_step,
                )
            )

        enriched.sort(
            key=lambda g: g.estimated_cardinality
            if g.estimated_cardinality is not None
            else float("inf")
        )
        for i, g in enumerate(enriched, start=1):
            g.execution_step = i

        return enriched

    def _probe_count(self, group: DimensionGroup) -> int:
        """COUNT distinct entity_ids matching this group's constraints."""
        table  = self._tables.get(group.dimension)
        coords = self._coordinates(group)

        if not table or not coords:
            return 0

        placeholders = ", ".join(f"'{_esc(c)}'" for c in coords)
        sql = (
            f"SELECT COUNT(DISTINCT entity_id) AS n "
            f"FROM {table} "
            f"WHERE semantic_key IN ({placeholders})"
        )
        try:
            rows = self._sql(sql)
            return int(rows[0]["n"]) if rows else 0
        except Exception as exc:
            if self.debug:
                print(f"[pinot] probe failed for {group.dimension}: {exc}")
            return 0

    def _coordinates(self, group: DimensionGroup) -> List[str]:
        """
        Convert a group's eq constraints to coordinate strings.
        Format: "DIMENSION.field=value"
        NOT_EQ constraints are excluded — handled at result-set level.
        """
        return [
            f"{c.dimension}.{c.key}={c.value}"
            for c in group.constraints
            if c.operator == "eq"
        ]

    def _anchor_sql(self, table: str, coords: List[str]):
        placeholders = ", ".join(f"'{_esc(c)}'" for c in coords)
        sql = (
            f"SELECT DISTINCT entity_id "
            f"FROM {table} "
            f"WHERE semantic_key IN ({placeholders})"
        )
        return sql, []

    def _filter_sql(
        self, table: str, coords: List[str], entity_ids: List[str]
    ):
        coord_ph = ", ".join(f"'{_esc(c)}'" for c in coords)
        id_ph    = ", ".join(
            f"'{_esc(str(i))}'" for i in entity_ids[:MAX_IN_CLAUSE]
        )
        sql = (
            f"SELECT DISTINCT entity_id "
            f"FROM {table} "
            f"WHERE semantic_key IN ({coord_ph}) "
            f"AND entity_id IN ({id_ph})"
        )
        return sql, []

    # ------------------------------------------------------------------
    # Pinot REST transport
    # ------------------------------------------------------------------

    _last_pinot_ms: float = 0.0

    def _sql(self, sql: str) -> List[Dict[str, Any]]:
        """
        POST SQL to Pinot broker. Returns list of row dicts.
        Raises on HTTP error or Pinot exception in response body.
        """
        url  = f"{self.broker_url}{_BROKER_SQL_PATH}"
        resp = requests.post(url, json={"sql": sql}, timeout=self.timeout)
        resp.raise_for_status()

        body = resp.json()
        self._last_pinot_ms = float(body.get("timeUsedMs", 0))

        exceptions = body.get("exceptions", [])
        if exceptions:
            raise PinotQueryError(
                f"Pinot error: {exceptions[0].get('message', str(exceptions))}"
            )

        schema    = body.get("resultTable", {}).get("dataSchema", {})
        col_names = schema.get("columnNames", [])
        rows_data = body.get("resultTable", {}).get("rows", [])

        return [dict(zip(col_names, row)) for row in rows_data]


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class PinotQueryError(Exception):
    """Raised when Pinot returns an exception in the response body."""
    pass


class PinotConnectionError(Exception):
    """Raised when the broker is unreachable."""
    pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _esc(s: str) -> str:
    """Minimal SQL string escaping — single-quote doubling."""
    return str(s).replace("'", "''")
