"""
test_substrate_neutrality.py — SNF P8 Substrate Neutrality Proof

THEOREM (from Peirce Query Language Spec v1.0, Section 10):
    For any two SNF-compliant substrates S₁ and S₂, and any valid Peirce query Q:
        execute(Q, S₁) = execute(Q, S₂)

This test proves it in practice, not just in spec.

What it does
------------
Hands the same unordered SNFPlan to three substrates:

    1. RoaringSubstrate  — in-process bitmap posting lists
    2. DuckDB Substrate  — in-process SQL
    3. PinotSubstrate    — external Apache Pinot cluster

Asserts that all three return identical entity_id sets.

The test constructs plans directly and hands them to substrates.
Each substrate handles execution internally. The test knows nothing
about how any substrate works — only that they must agree.

Running
-------
Without Pinot (DuckDB + Roaring only — always runnable):
    pytest test_substrate_neutrality.py -v

With Pinot:
    PINOT_BROKER_URL=http://localhost:8099 pytest test_substrate_neutrality.py -v

Pinot requires data pre-loaded via load_into_pinot.py.
If PINOT_BROKER_URL is not set, Pinot tests skip cleanly.

Dataset
-------
Uses the controlled synthetic store from store.py:
    WHO.id = 3512155           →  52 entities
    WHAT.matter_type=litigation → ~76,028 entities  (7 overlap with WHO)
    WHERE.region_id=12          → ~100,001 entities  (1 overlap with all)
    WHY.reason_code=billing     → ~168,940 entities  (1 overlap with all)
    WHEN.year=2024              → ~250,001 entities  (1 overlap with all)

    5-dim AND → 1 result  (entity_id 100000)
"""

from __future__ import annotations

import os
import pytest
from typing import Optional, Set, List

from plan import Constraint, DimensionGroup, SNFPlan
from store import build_controlled_store
from executor import execute_plan, materialize_conjunct_result


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PINOT_BROKER_URL = os.environ.get("PINOT_BROKER_URL", "")
PINOT_AVAILABLE  = bool(PINOT_BROKER_URL)

EXPECTED_ENTITY_ID = 100_000  # first entity in WHO posting list


# ---------------------------------------------------------------------------
# Query cases — unordered plans handed directly to substrates
# ---------------------------------------------------------------------------

def _make_plans():
    """
    Build test plans. Called fresh each time so estimated_cardinality
    fields are unset — the substrate receives a plain unordered plan.
    """
    return [
        {
            "label": "5-dim AND (benchmark query)",
            "plan": SNFPlan(dimension_groups=[
                DimensionGroup("WHO",   [Constraint("WHO",   "id",          3512155)]),
                DimensionGroup("WHAT",  [Constraint("WHAT",  "matter_type", "litigation")]),
                DimensionGroup("WHERE", [Constraint("WHERE", "region_id",   12)]),
                DimensionGroup("WHY",   [Constraint("WHY",   "reason_code", "billing")]),
                DimensionGroup("WHEN",  [Constraint("WHEN",  "year",        2024)]),
            ]),
            "expected_count": 1,
            "expected_ids":   {EXPECTED_ENTITY_ID},
        },
        {
            "label": "2-dim AND (WHO ∩ WHEN)",
            "plan": SNFPlan(dimension_groups=[
                DimensionGroup("WHO",  [Constraint("WHO",  "id",   3512155)]),
                DimensionGroup("WHEN", [Constraint("WHEN", "year", 2024)]),
            ]),
            "expected_count": 1,
            "expected_ids":   {EXPECTED_ENTITY_ID},
        },
        {
            "label": "OR within dimension",
            "plan": SNFPlan(dimension_groups=[
                DimensionGroup("WHO", [
                    Constraint("WHO", "id", 3512155),
                    Constraint("WHO", "id", 8888888),
                ]),
                DimensionGroup("WHEN", [Constraint("WHEN", "year", 2024)]),
            ]),
            "expected_count": 5,   # 1 from first WHO + 4 from second WHO ∩ WHEN
            "expected_ids":   None,
        },
        {
            "label": "1-dim (WHO only)",
            "plan": SNFPlan(dimension_groups=[
                DimensionGroup("WHO", [Constraint("WHO", "id", 3512155)]),
            ]),
            "expected_count": 52,
            "expected_ids":   None,
        },
    ]


# ---------------------------------------------------------------------------
# Substrate factories
# ---------------------------------------------------------------------------

def make_roaring_substrate():
    store, U_L = build_controlled_store()
    return _RoaringSubstrate(store, U_L)


def make_duckdb_substrate():
    try:
        import duckdb
    except ImportError:
        pytest.skip("duckdb not installed")
    store, U_L = build_controlled_store()
    return _DuckDBSubstrate(store, U_L)


def make_pinot_substrate():
    if not PINOT_AVAILABLE:
        pytest.skip("PINOT_BROKER_URL not set")
    from pinot_substrate import PinotSubstrate
    sub = PinotSubstrate(broker_url=PINOT_BROKER_URL)
    if not sub.ping():
        pytest.skip(f"Pinot broker not reachable at {PINOT_BROKER_URL}")
    return sub


# ---------------------------------------------------------------------------
# Substrate implementations
# ---------------------------------------------------------------------------

class _RoaringSubstrate:
    """
    Wraps the (store, U_L) roaring bitmap execution model.
    execute(plan) → list[str] — same contract as every other substrate.
    """

    def __init__(self, store, U_L):
        self._store = store
        self._U_L   = U_L

    def execute(self, plan: SNFPlan) -> List[str]:
        if plan.unsatisfiable or not plan.dimension_groups:
            return []
        result = materialize_conjunct_result(plan, self._store, self._U_L)
        return [str(eid) for eid in sorted(result)]


class _DuckDBSubstrate:
    """
    In-memory DuckDB spoke tables populated from the controlled store.
    execute(plan) → list[str] via SQL set intersection.
    """

    def __init__(self, store, U_L):
        import duckdb

        self._conn = duckdb.connect()
        self._conn.execute("PRAGMA threads=4")

        dims = set(k[0] for k in store.keys())
        for dim in dims:
            table = f"snf_{dim.lower()}"
            self._conn.execute(f"""
                CREATE TABLE {table} (
                    entity_id    INTEGER NOT NULL,
                    semantic_key VARCHAR NOT NULL,
                    coordinate   VARCHAR NOT NULL,
                    lens_id      VARCHAR
                )
            """)
            self._conn.execute(
                f"CREATE INDEX idx_{table}_sk  ON {table}(semantic_key)"
            )
            self._conn.execute(
                f"CREATE INDEX idx_{table}_eid ON {table}(entity_id)"
            )

        for (dim, key, value), bitmap in store.items():
            table = f"snf_{dim.lower()}"
            coord = f"{dim}.{key}={value}"
            rows  = [(eid, coord, coord, "test") for eid in sorted(bitmap)]
            self._conn.executemany(
                f"INSERT INTO {table} VALUES (?, ?, ?, ?)", rows
            )

    def execute(self, plan: SNFPlan) -> List[str]:
        if plan.unsatisfiable or not plan.dimension_groups:
            return []

        current_ids: Optional[Set[str]] = None

        for group in plan.dimension_groups:
            table  = f"snf_{group.dimension.lower()}"
            coords = [
                f"{c.dimension}.{c.key}={c.value}"
                for c in group.constraints
                if c.operator == "eq"
            ]
            if not coords:
                continue

            ph = ", ".join(f"'{c}'" for c in coords)

            if current_ids is None:
                sql  = (
                    f"SELECT DISTINCT CAST(entity_id AS VARCHAR) "
                    f"FROM {table} WHERE semantic_key IN ({ph})"
                )
                rows = self._conn.execute(sql).fetchall()
            else:
                if not current_ids:
                    return []
                id_ph = ", ".join(f"'{i}'" for i in current_ids)
                sql   = (
                    f"SELECT DISTINCT CAST(entity_id AS VARCHAR) "
                    f"FROM {table} "
                    f"WHERE semantic_key IN ({ph}) "
                    f"AND CAST(entity_id AS VARCHAR) IN ({id_ph})"
                )
                rows = self._conn.execute(sql).fetchall()

            current_ids = {row[0] for row in rows}
            if not current_ids:
                return []

        return sorted(current_ids) if current_ids else []


# ---------------------------------------------------------------------------
# Core assertion
# ---------------------------------------------------------------------------

def assert_agree(
    substrates: dict,
    plan: SNFPlan,
    expected_count: int,
    expected_ids: Optional[Set[int]],
    label: str,
):
    """
    Execute plan against all substrates. Assert identical results.
    """
    results = {}
    for name, sub in substrates.items():
        ids = sub.execute(plan)
        results[name] = {str(i) for i in ids}

    names = list(results.keys())
    for i in range(1, len(names)):
        a, b = names[0], names[i]
        assert results[a] == results[b], (
            f"\n[SUBSTRATE NEUTRALITY VIOLATION]\n"
            f"  Query: {label}\n"
            f"  {a}: {len(results[a])} results — {sorted(results[a])[:5]}\n"
            f"  {b}: {len(results[b])} results — {sorted(results[b])[:5]}\n"
            f"  Only in {a}: {sorted(results[a] - results[b])[:5]}\n"
            f"  Only in {b}: {sorted(results[b] - results[a])[:5]}"
        )

    first = results[names[0]]
    assert len(first) == expected_count, (
        f"\n[COUNT MISMATCH]\n"
        f"  Query:    {label}\n"
        f"  Expected: {expected_count}\n"
        f"  Got:      {len(first)}"
    )

    if expected_ids is not None:
        expected_str = {str(i) for i in expected_ids}
        assert expected_str.issubset(first), (
            f"\n[EXPECTED IDs NOT FOUND]\n"
            f"  Query:    {label}\n"
            f"  Expected: {sorted(expected_str)}\n"
            f"  Got:      {sorted(first)[:10]}"
        )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def roaring():
    return make_roaring_substrate()


@pytest.fixture(scope="module")
def duckdb_sub():
    return make_duckdb_substrate()


@pytest.fixture(scope="module")
def pinot():
    return make_pinot_substrate()


# ---------------------------------------------------------------------------
# P8a: Roaring vs DuckDB — always runs, no external dependencies
# ---------------------------------------------------------------------------

class TestRoaringVsDuckDB:
    """
    Substrate neutrality: bitmap posting lists vs SQL.
    Two different execution models, identical data, must return identical results.
    Runs without any external services.
    """

    @pytest.mark.parametrize("case", _make_plans(), ids=[c["label"] for c in _make_plans()])
    def test_neutrality(self, roaring, duckdb_sub, case):
        assert_agree(
            substrates={"roaring": roaring, "duckdb": duckdb_sub},
            plan=case["plan"],
            expected_count=case["expected_count"],
            expected_ids=case["expected_ids"],
            label=case["label"],
        )


# ---------------------------------------------------------------------------
# P8b: Roaring vs Pinot — requires PINOT_BROKER_URL
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not PINOT_AVAILABLE, reason="PINOT_BROKER_URL not set")
class TestRoaringVsPinot:
    """
    Substrate neutrality: bitmap posting lists vs Apache Pinot.
    Requires Pinot running with spoke tables populated via load_into_pinot.py.
    """

    @pytest.mark.parametrize("case", _make_plans(), ids=[c["label"] for c in _make_plans()])
    def test_neutrality(self, roaring, pinot, case):
        assert_agree(
            substrates={"roaring": roaring, "pinot": pinot},
            plan=case["plan"],
            expected_count=case["expected_count"],
            expected_ids=case["expected_ids"],
            label=case["label"],
        )


# ---------------------------------------------------------------------------
# P8c: DuckDB vs Pinot — requires PINOT_BROKER_URL
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not PINOT_AVAILABLE, reason="PINOT_BROKER_URL not set")
class TestDuckDBVsPinot:
    """
    Substrate neutrality: DuckDB SQL vs Apache Pinot SQL.
    Different engines, same schema, same data, same query.
    """

    @pytest.mark.parametrize("case", _make_plans(), ids=[c["label"] for c in _make_plans()])
    def test_neutrality(self, duckdb_sub, pinot, case):
        assert_agree(
            substrates={"duckdb": duckdb_sub, "pinot": pinot},
            plan=case["plan"],
            expected_count=case["expected_count"],
            expected_ids=case["expected_ids"],
            label=case["label"],
        )


# ---------------------------------------------------------------------------
# P8d: All three simultaneously — requires PINOT_BROKER_URL
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not PINOT_AVAILABLE, reason="PINOT_BROKER_URL not set")
class TestAllThree:
    """
    P8 proof: all three substrates simultaneously.
    Same plan. Three substrates. Identical results.
    """

    def test_benchmark_query(self, roaring, duckdb_sub, pinot):
        plan = SNFPlan(dimension_groups=[
            DimensionGroup("WHO",   [Constraint("WHO",   "id",          3512155)]),
            DimensionGroup("WHAT",  [Constraint("WHAT",  "matter_type", "litigation")]),
            DimensionGroup("WHERE", [Constraint("WHERE", "region_id",   12)]),
            DimensionGroup("WHY",   [Constraint("WHY",   "reason_code", "billing")]),
            DimensionGroup("WHEN",  [Constraint("WHEN",  "year",        2024)]),
        ])
        assert_agree(
            substrates={"roaring": roaring, "duckdb": duckdb_sub, "pinot": pinot},
            plan=plan,
            expected_count=1,
            expected_ids={EXPECTED_ENTITY_ID},
            label="5-dim AND benchmark (P8)",
        )

    @pytest.mark.parametrize("case", _make_plans(), ids=[c["label"] for c in _make_plans()])
    def test_all_cases(self, roaring, duckdb_sub, pinot, case):
        assert_agree(
            substrates={"roaring": roaring, "duckdb": duckdb_sub, "pinot": pinot},
            plan=case["plan"],
            expected_count=case["expected_count"],
            expected_ids=case["expected_ids"],
            label=case["label"],
        )


# ---------------------------------------------------------------------------
# Standalone runner
# ---------------------------------------------------------------------------

def main():
    import traceback

    print("=" * 70)
    print("SNF Substrate Neutrality Proof — P8")
    print("=" * 70)
    print(f"\n  Roaring  : in-process bitmap posting lists")
    print(f"  DuckDB   : in-process SQL")
    if PINOT_AVAILABLE:
        print(f"  Pinot    : {PINOT_BROKER_URL}")
    else:
        print(f"  Pinot    : SKIPPED (set PINOT_BROKER_URL to enable)")

    substrates = {
        "roaring": make_roaring_substrate(),
        "duckdb":  make_duckdb_substrate(),
    }
    if PINOT_AVAILABLE:
        try:
            substrates["pinot"] = make_pinot_substrate()
        except Exception as e:
            print(f"\n  WARNING: Pinot unavailable — {e}")

    all_passed = True
    for case in _make_plans():
        print(f"\n{'─' * 70}")
        print(f"  {case['label']}")

        results = {}
        for name, sub in substrates.items():
            try:
                ids = sub.execute(case["plan"])
                results[name] = sorted(str(i) for i in ids)
                sample = results[name][:3]
                suffix = "..." if len(ids) > 3 else ""
                print(f"  {name:10s}: {len(ids):>6,} results  {sample}{suffix}")
            except Exception as e:
                print(f"  {name:10s}: ERROR — {e}")
                traceback.print_exc()
                all_passed = False

        names = list(results.keys())
        agreed = all(
            set(results[names[0]]) == set(results[n])
            for n in names[1:]
        )
        if agreed:
            print(f"  ✓ All substrates agree")
        else:
            print(f"  ✗ NEUTRALITY VIOLATION")
            all_passed = False

    print(f"\n{'=' * 70}")
    if all_passed:
        print("✓ SUBSTRATE NEUTRALITY PROVEN")
        print("  Same plan. Multiple substrates. Identical results.")
    else:
        print("✗ VIOLATION DETECTED — see above")
    print("=" * 70)


if __name__ == "__main__":
    main()
