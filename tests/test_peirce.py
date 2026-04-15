"""
test_peirce.py — Tests for peirce.py

Run with: python test_peirce.py
"""

import sys
import os
import types

sys.path.insert(0, os.path.dirname(__file__))

# ── Stub duckdb with sqlite3 for sandbox ────────────────────────────────────
import sqlite3

class _FakeConn:
    def __init__(self):
        self._conn = sqlite3.connect(':memory:')
    def execute(self, sql, params=None):
        cur = self._conn.cursor()
        cur.execute(sql, params or [])
        self._conn.commit()
        return _FakeCursor(cur)
    def executemany(self, sql, data):
        cur = self._conn.cursor()
        cur.executemany(sql, data)
        self._conn.commit()
    def close(self):
        self._conn.close()

class _FakeCursor:
    def __init__(self, cur):
        self._cur = cur
    def fetchall(self):
        return self._cur.fetchall()
    def fetchone(self):
        return self._cur.fetchone()
    def df(self):
        import pandas as pd
        cols = [d[0] for d in self._cur.description] if self._cur.description else []
        rows = self._cur.fetchall()
        return pd.DataFrame(rows, columns=cols)

duckdb_mod = types.ModuleType('duckdb')
duckdb_mod.connect = lambda path=':memory:': _FakeConn()
sys.modules['duckdb'] = duckdb_mod
# ─────────────────────────────────────────────────────────────────────────────

import pandas as pd
from compile import compile_data
from peirce import query, execute, ResultSet, PeirceParseError, PeirceDiscoveryError

passed = 0
failed = 0

def check(name, condition, detail=''):
    global passed, failed
    if condition:
        print(f"  PASS  {name}")
        passed += 1
    else:
        print(f"  FAIL  {name}" + (f": {detail}" if detail else ""))
        failed += 1

def check_raises(name, fn, exc_type):
    global passed, failed
    try:
        fn()
        print(f"  FAIL  {name}: expected {exc_type.__name__} but no exception raised")
        failed += 1
    except exc_type:
        print(f"  PASS  {name}")
        passed += 1
    except Exception as e:
        print(f"  FAIL  {name}: expected {exc_type.__name__} but got {type(e).__name__}: {e}")
        failed += 1


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

DISCOGS_DF = pd.DataFrame({
    "Catalog#":   ["CAT001", "CAT002", "CAT003"],
    "Artist":     ["Miles Davis", "John Coltrane", "Bill Evans"],
    "Title":      ["Kind of Blue", "A Love Supreme", "Waltz for Debby"],
    "Label":      ["Columbia", "Impulse!", "Riverside"],
    "Format":     ["LP", "LP", "LP"],
    "Released":   ["1959", "1964", "1961"],
    "release_id": [100001, 100002, 100003],
})

DISCOGS_LENS = {
    "lens_id": "discogs_v1",
    "lens_version": "1.0",
    "generated_at": "2026-04-01T00:00:00Z",
    "declaration": {},
    "coordinate_map": {
        "Artist":     {"dimension": "who",  "semantic_key": "artist"},
        "Title":      {"dimension": "what", "semantic_key": "title"},
        "Label":      {"dimension": "who",  "semantic_key": "publisher"},
        "Format":     {"dimension": "how",  "semantic_key": "format"},
        "Released":   {"dimension": "when", "semantic_key": "released"},
        "release_id": {"dimension": "what", "semantic_key": "release_id"},
    },
    "stats": {"total_fields": 6, "by_dimension": {"who": 2, "what": 2, "when": 1, "where": 0, "how": 1}},
    "nucleus": {"type": "single", "field": "release_id", "prefix": "discogs:release"},
}

substrate = compile_data(DISCOGS_DF, DISCOGS_LENS)


# ─────────────────────────────────────────────────────────────────────────────
# 1. query() — basic
# ─────────────────────────────────────────────────────────────────────────────

print("--- query() basic ---")

r = query(substrate, 'WHO.artist = "Miles Davis"')
check("returns ResultSet", isinstance(r, ResultSet))
check("correct entity", r.entity_ids == ["discogs:release:100001"])
check("count property", r.count == 1)

r_none = query(substrate, 'WHO.artist = "Nobody"')
check("no match returns empty ResultSet", r_none.count == 0)
check("empty entity_ids", r_none.entity_ids == [])


# ─────────────────────────────────────────────────────────────────────────────
# 2. query() — all operators via Peirce strings
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- query() operators ---")

r_ne = query(substrate, 'WHO.artist != "Miles Davis"', limit=None)
check("not_eq operator", "discogs:release:100001" not in r_ne.entity_ids)
check("not_eq returns others", r_ne.count == 2)

r_gt = query(substrate, 'WHEN.released > "1960"', limit=None)
check("gt operator", "discogs:release:100001" not in r_gt.entity_ids)

r_between = query(substrate, 'WHEN.released BETWEEN "1959" AND "1962"', limit=None)
check("between operator",
      set(r_between.entity_ids) == {"discogs:release:100001", "discogs:release:100003"})

r_contains = query(substrate, 'WHO.artist CONTAINS "Davis"', limit=None)
check("contains operator", "discogs:release:100001" in r_contains.entity_ids)

r_prefix = query(substrate, 'WHO.artist PREFIX "Miles"', limit=None)
check("prefix operator", "discogs:release:100001" in r_prefix.entity_ids)
check("prefix excludes non-match", "discogs:release:100002" not in r_prefix.entity_ids)

r_not = query(substrate, 'NOT WHO.artist = "Miles Davis"', limit=None)
check("NOT operator", "discogs:release:100001" not in r_not.entity_ids)
check("NOT returns others", r_not.count == 2)


# ─────────────────────────────────────────────────────────────────────────────
# 3. query() — AND across dimensions
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- query() AND ---")

r_and = query(substrate, 'WHO.artist = "Miles Davis" AND WHEN.released = "1959"', limit=None)
check("AND narrows result", r_and.entity_ids == ["discogs:release:100001"])

r_and_miss = query(substrate,
    'WHO.artist = "Miles Davis" AND WHEN.released = "1964"', limit=None)
check("AND with non-matching dims returns empty", r_and_miss.count == 0)


# ─────────────────────────────────────────────────────────────────────────────
# 4. query() — OR (DNF)
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- query() OR / DNF ---")

r_or = query(substrate,
    'WHO.artist = "Miles Davis" OR WHO.artist = "John Coltrane"', limit=None)
check("OR widens result",
      set(r_or.entity_ids) == {"discogs:release:100001", "discogs:release:100002"})

# Full DNF
r_dnf = query(substrate,
    '(WHO.artist = "Miles Davis" AND WHEN.released = "1959") '
    'OR (WHO.artist = "John Coltrane" AND WHEN.released = "1964")',
    limit=None)
check("DNF both conjuncts",
      set(r_dnf.entity_ids) == {"discogs:release:100001", "discogs:release:100002"})

# DNF where one conjunct matches nothing
r_dnf_partial = query(substrate,
    'WHO.artist = "Miles Davis" OR WHO.artist = "Nobody"', limit=None)
check("DNF partial match", r_dnf_partial.entity_ids == ["discogs:release:100001"])


# ─────────────────────────────────────────────────────────────────────────────
# 5. limit
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- limit ---")

r_limited = query(substrate, 'HOW.format = "LP"', limit=2)
check("limit applied", r_limited.count <= 2)

r_no_limit = query(substrate, 'HOW.format = "LP"', limit=None)
check("limit=None returns all", r_no_limit.count == 3)

r_limit_0 = query(substrate, 'HOW.format = "LP"', limit=0)
check("limit=0 returns all", r_limit_0.count == 3)


# ─────────────────────────────────────────────────────────────────────────────
# 6. PeirceParseError
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- PeirceParseError ---")

def bad_query():
    query(substrate, 'WHO.artist "missing operator"')

check_raises("bad syntax raises PeirceParseError", bad_query, PeirceParseError)

try:
    query(substrate, 'WHO.artist "missing operator"')
except PeirceParseError as e:
    check("error has .error", isinstance(e.error, str) and len(e.error) > 0)
    check("error has .position", isinstance(e.position, int))
    check("error has .token", hasattr(e, "token"))

check_raises("empty string raises PeirceParseError",
             lambda: query(substrate, ""), PeirceParseError)


# ─────────────────────────────────────────────────────────────────────────────
# 7. PeirceDiscoveryError
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- PeirceDiscoveryError ---")

check_raises("* raises PeirceDiscoveryError",
             lambda: query(substrate, "*"), PeirceDiscoveryError)
check_raises("WHO|* raises PeirceDiscoveryError",
             lambda: query(substrate, "WHO|*"), PeirceDiscoveryError)
check_raises("WHO|role|* raises PeirceDiscoveryError",
             lambda: query(substrate, "WHO|role|*"), PeirceDiscoveryError)

try:
    query(substrate, "WHO|*")
except PeirceDiscoveryError as e:
    check("discovery error has scope", e.scope == "dimension")
    check("discovery error has dimension", e.dimension == "WHO")


# ─────────────────────────────────────────────────────────────────────────────
# 8. TypeError for wrong substrate
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- TypeError ---")

check_raises("non-Substrate raises TypeError",
             lambda: query("not a substrate", 'WHO.artist = "x"'), TypeError)


# ─────────────────────────────────────────────────────────────────────────────
# 9. execute() — flat constraint list
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- execute() flat list ---")

constraints = [{"category": "WHO", "field": "artist", "op": "eq", "value": "Miles Davis"}]
r_exec = execute(constraints, substrate)
check("execute flat list", r_exec.entity_ids == ["discogs:release:100001"])
check("execute returns ResultSet", isinstance(r_exec, ResultSet))
check("execute has no query_string", r_exec._query_string is None)


# ─────────────────────────────────────────────────────────────────────────────
# 10. execute() — parse_to_constraints result dict
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- execute() parsed dict ---")

from parser import parse_to_constraints
parsed = parse_to_constraints('WHO.artist = "Miles Davis" AND WHEN.released = "1959"')
r_parsed = execute(parsed, substrate)
check("execute parsed dict", r_parsed.entity_ids == ["discogs:release:100001"])

# DNF via execute
parsed_dnf = parse_to_constraints(
    'WHO.artist = "Miles Davis" OR WHO.artist = "John Coltrane"'
)
r_dnf_exec = execute(parsed_dnf, substrate)
check("execute DNF dict",
      set(r_dnf_exec.entity_ids) == {"discogs:release:100001", "discogs:release:100002"})

# Discovery expression passed to execute raises
from parser import parse
discovery_parsed = parse("WHO|*")
check_raises("execute discovery dict raises PeirceDiscoveryError",
             lambda: execute(discovery_parsed, substrate), PeirceDiscoveryError)


# ─────────────────────────────────────────────────────────────────────────────
# 11. ResultSet — to_dataframe()
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- ResultSet.to_dataframe() ---")

r_df = query(substrate, 'WHO.artist = "Miles Davis"', limit=None)
df = r_df.to_dataframe()
check("to_dataframe returns DataFrame", isinstance(df, pd.DataFrame))
check("to_dataframe has correct columns",
      set(df.columns) == {"entity_id", "dimension", "semantic_key", "value", "coordinate", "lens_id"})
check("to_dataframe has rows", len(df) > 0)
check("to_dataframe entity_id correct",
      (df["entity_id"] == "discogs:release:100001").all())

# Empty result
r_empty = query(substrate, 'WHO.artist = "Nobody"', limit=None)
df_empty = r_empty.to_dataframe()
check("empty to_dataframe has correct columns",
      set(df_empty.columns) == {"entity_id", "dimension", "semantic_key", "value", "coordinate", "lens_id"})
check("empty to_dataframe has no rows", len(df_empty) == 0)


# ─────────────────────────────────────────────────────────────────────────────
# 12. ResultSet — pivot()
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- ResultSet.pivot() ---")

r_pivot = query(substrate, 'WHO.artist = "Miles Davis"', limit=None)
pv = r_pivot.pivot()
check("pivot returns DataFrame", isinstance(pv, pd.DataFrame))
check("pivot has entity_id column", "entity_id" in pv.columns)
check("pivot has semantic_key columns", "artist" in pv.columns or "title" in pv.columns)
check("pivot one row per entity", len(pv) == 1)


# ─────────────────────────────────────────────────────────────────────────────
# 13. ResultSet — iter and len
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- ResultSet iteration ---")

r_iter = query(substrate, 'HOW.format = "LP"', limit=None)
check("len() works", len(r_iter) == 3)
check("iter yields entity_ids",
      list(r_iter) == r_iter.entity_ids)


# ─────────────────────────────────────────────────────────────────────────────
# 14. The Jupyter demo — end-to-end
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- Jupyter demo (end-to-end) ---")

# This is the target state from the implementation brief
from lens import suggest, LensDraft

df_demo = pd.read_csv if False else DISCOGS_DF.copy()

draft = suggest(df_demo)
draft.map("Artist", "who", "artist")
draft.map("Released", "when", "released")
draft.nucleus("release_id", prefix="discogs:release")
lens_demo = draft.to_lens(lens_id="discogs_v1", authority="abk")
compiled_demo = compile_data(df_demo, lens_demo)
result_demo = query(compiled_demo, 'WHO.artist = "Miles Davis"')

check("demo: query returns ResultSet", isinstance(result_demo, ResultSet))
check("demo: correct entity found", "discogs:release:100001" in result_demo.entity_ids)
check("demo: to_dataframe works", isinstance(result_demo.to_dataframe(), pd.DataFrame))
check("demo: _repr_html_ works", isinstance(result_demo._repr_html_(), str))
check("demo: __repr__ works", isinstance(repr(result_demo), str))

# AND query
result_and = query(compiled_demo, 'WHO.artist = "Miles Davis" AND WHEN.released = "1959"')
check("demo: AND query", result_and.count == 1)


# ─────────────────────────────────────────────────────────────────────────────
# Results
# ─────────────────────────────────────────────────────────────────────────────

print()
print(f"Results: {passed} passed, {failed} failed")
