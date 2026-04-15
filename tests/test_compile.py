"""
test_compile.py — Tests for compile.py

Run with: python test_compile.py
"""

import sys
import os
import json
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd
from compile import compile_data, Substrate, CompileError, NucleusError

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

LEGAL_DF = pd.DataFrame({
    "client_id":   ["CLI001", "CLI001", "CLI002"],
    "matter_id":   ["MAT001", "MAT002", "MAT001"],
    "attorney":    ["Smith",  "Jones",  "Smith"],
    "matter_type": ["litigation", "ip", "litigation"],
    "year":        ["2023", "2024", "2024"],
    "office":      ["Seattle", "New York", "Seattle"],
})

LEGAL_LENS = {
    "lens_id": "legal_v1",
    "lens_version": "1.0",
    "generated_at": "2026-04-01T00:00:00Z",
    "declaration": {},
    "coordinate_map": {
        "attorney":    {"dimension": "who",   "semantic_key": "attorney"},
        "matter_type": {"dimension": "why",   "semantic_key": "matter_type"},
        "year":        {"dimension": "when",  "semantic_key": "year"},
        "office":      {"dimension": "where", "semantic_key": "office"},
    },
    "stats": {"total_fields": 4, "by_dimension": {"who": 1, "what": 0, "when": 1, "where": 1, "how": 0}},
    "nucleus": {
        "type": "composite",
        "fields": ["client_id", "matter_id"],
        "separator": "-",
        "prefix": "legal:matter",
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# 1. Basic compilation
# ─────────────────────────────────────────────────────────────────────────────

print("--- Basic compilation ---")

s = compile_data(DISCOGS_DF, DISCOGS_LENS)
check("returns Substrate", isinstance(s, Substrate))
check("lens_id correct", s.lens_id == "discogs_v1")
check("entity_count correct", s.entity_count() == 3)
check("dimensions present", set(s.dimensions()) == {"who", "what", "how", "when"})

# fact count: 3 rows × (artist + title + publisher + format + released + release_id) = 18
check("fact_count correct", s.count() == 18)


# ─────────────────────────────────────────────────────────────────────────────
# 2. Nucleus resolution — single
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- Nucleus resolution (single) ---")

df_spoke = s.to_dataframe()
entity_ids = df_spoke["entity_id"].unique().tolist()
check("entity_ids have prefix", all(eid.startswith("discogs:release:") for eid in entity_ids))
check("entity_id for row 0", "discogs:release:100001" in entity_ids)
check("entity_id for row 1", "discogs:release:100002" in entity_ids)
check("entity_id for row 2", "discogs:release:100003" in entity_ids)

# No prefix
lens_no_prefix = dict(DISCOGS_LENS)
lens_no_prefix = {**DISCOGS_LENS, "nucleus": {"type": "single", "field": "release_id", "prefix": ""}}
s2 = compile_data(DISCOGS_DF, lens_no_prefix)
df2 = s2.to_dataframe()
check("no prefix — entity_id is raw value", "100001" in df2["entity_id"].values)


# ─────────────────────────────────────────────────────────────────────────────
# 3. Nucleus resolution — composite
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- Nucleus resolution (composite) ---")

legal_s = compile_data(LEGAL_DF, LEGAL_LENS)
legal_df = legal_s.to_dataframe()
eids = legal_df["entity_id"].unique().tolist()
check("composite entity_ids have prefix", all(e.startswith("legal:matter:") for e in eids))
check("composite entity_id CLI001-MAT001", "legal:matter:CLI001-MAT001" in eids)
check("composite entity_id CLI001-MAT002", "legal:matter:CLI001-MAT002" in eids)
check("composite entity_id CLI002-MAT001", "legal:matter:CLI002-MAT001" in eids)
check("composite entity count", legal_s.entity_count() == 3)


# ─────────────────────────────────────────────────────────────────────────────
# 4. Spoke table schema
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- Spoke table schema ---")

df_spoke = s.to_dataframe()
check("columns present", set(df_spoke.columns) == {"entity_id","dimension","semantic_key","value","coordinate","lens_id"})
check("coordinate format", all("|" in c for c in df_spoke["coordinate"]))

# Check a specific coordinate
coord_sample = df_spoke[df_spoke["value"] == "Miles Davis"]["coordinate"].values
check("coordinate is DIMENSION|key|value", len(coord_sample) > 0 and coord_sample[0] == "WHO|artist|Miles Davis")

check("lens_id in all rows", (df_spoke["lens_id"] == "discogs_v1").all())


# ─────────────────────────────────────────────────────────────────────────────
# 5. Query — equality
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- Query — equality ---")

result = s.query([{"category": "WHO", "field": "artist", "op": "eq", "value": "Miles Davis"}])
check("eq query returns entity_ids", isinstance(result, list))
check("eq query finds Miles Davis", "discogs:release:100001" in result)
check("eq query exact match only", len(result) == 1)

# Multi-dimension AND
result2 = s.query([
    {"category": "WHO",  "field": "artist",   "op": "eq", "value": "Miles Davis"},
    {"category": "WHEN", "field": "released",  "op": "eq", "value": "1959"},
])
check("AND two dims", result2 == ["discogs:release:100001"])

# No match
result3 = s.query([{"category": "WHO", "field": "artist", "op": "eq", "value": "Nonexistent"}])
check("no match returns empty list", result3 == [])

# Empty constraints
check("empty constraints returns empty", s.query([]) == [])


# ─────────────────────────────────────────────────────────────────────────────
# 6. Query — OR within dimension
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- Query — OR within dimension ---")

result_or = s.query([
    {"category": "WHO", "field": "artist", "op": "eq", "value": "Miles Davis"},
    {"category": "WHO", "field": "artist", "op": "eq", "value": "John Coltrane"},
])
check("OR within dimension returns both", set(result_or) == {
    "discogs:release:100001", "discogs:release:100002"
})


# ─────────────────────────────────────────────────────────────────────────────
# 7. Query — other operators
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- Query — other operators ---")

result_ne = s.query([{"category": "WHO", "field": "artist", "op": "not_eq", "value": "Miles Davis"}])
check("not_eq excludes Miles Davis", "discogs:release:100001" not in result_ne)
check("not_eq includes others", len(result_ne) == 2)

result_gt = s.query([{"category": "WHEN", "field": "released", "op": "gt", "value": "1960"}])
check("gt filters correctly", "discogs:release:100001" not in result_gt)  # 1959 excluded
check("gt includes 1961 and 1964", len(result_gt) == 2)

result_contains = s.query([{"category": "WHO", "field": "artist", "op": "contains", "value": "Davis"}])
check("contains matches substring", "discogs:release:100001" in result_contains)

result_prefix = s.query([{"category": "WHO", "field": "artist", "op": "prefix", "value": "Miles"}])
check("prefix matches prefix", "discogs:release:100001" in result_prefix)
check("prefix excludes non-match", "discogs:release:100002" not in result_prefix)

result_between = s.query([
    {"category": "WHEN", "field": "released", "op": "between", "value": "1959", "value2": "1962"}
])
check("between inclusive", set(result_between) == {"discogs:release:100001", "discogs:release:100003"})


# ─────────────────────────────────────────────────────────────────────────────
# 8. describe()
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- describe() ---")

d = s.describe()
check("describe has lens_id", d["lens_id"] == "discogs_v1")
check("describe has entity_count", d["entity_count"] == 3)
check("describe has fact_count", d["fact_count"] == 18)
check("describe has dimensions", isinstance(d["dimensions"], list))
check("describe has facts_by_dim", isinstance(d["facts_by_dim"], dict))


# ─────────────────────────────────────────────────────────────────────────────
# 9. Null/empty value handling
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- Null handling ---")

df_nulls = pd.DataFrame({
    "Artist":     ["Miles Davis", None, "Bill Evans"],
    "Title":      ["Kind of Blue", "A Love Supreme", None],
    "release_id": [100001, 100002, 100003],
})
s_nulls = compile_data(df_nulls, {
    **DISCOGS_LENS,
    "coordinate_map": {
        "Artist":     {"dimension": "who",  "semantic_key": "artist"},
        "Title":      {"dimension": "what", "semantic_key": "title"},
        "release_id": {"dimension": "what", "semantic_key": "release_id"},
    }
})
check("null values skipped silently", s_nulls.entity_count() == 3)
# Row 1 has no artist, row 2 has no title — entities still created via nucleus
check("entity with null field still exists", len(s_nulls.query(
    [{"category": "WHAT", "field": "release_id", "op": "eq", "value": "100002"}]
)) == 1)


# ─────────────────────────────────────────────────────────────────────────────
# 10. NucleusError
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- NucleusError ---")

df_bad_nucleus = pd.DataFrame({
    "Artist":     ["Miles Davis", "John Coltrane"],
    "release_id": [100001, None],   # row 1 has null nucleus
})
check_raises("null nucleus raises NucleusError",
    lambda: compile_data(df_bad_nucleus, DISCOGS_LENS), NucleusError)


# ─────────────────────────────────────────────────────────────────────────────
# 11. CompileError cases
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- CompileError ---")

check_raises("bad lens type raises CompileError",
    lambda: compile_data(DISCOGS_DF, 42), CompileError)

check_raises("missing lens_id raises CompileError",
    lambda: compile_data(DISCOGS_DF, {"coordinate_map": {}, "nucleus": {"type":"single","field":"x"}}),
    CompileError)

check_raises("nucleus field not in df raises CompileError",
    lambda: compile_data(DISCOGS_DF, {**DISCOGS_LENS,
        "nucleus": {"type": "single", "field": "nonexistent_field", "prefix": ""}}),
    CompileError)

check_raises("bad source type raises TypeError",
    lambda: compile_data(42, DISCOGS_LENS), TypeError)

check_raises("bad into= format raises CompileError",
    lambda: compile_data(DISCOGS_DF, DISCOGS_LENS, into="badformat://x"), CompileError)


# ─────────────────────────────────────────────────────────────────────────────
# 12. into= output paths
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- into= output ---")

with tempfile.TemporaryDirectory() as tmpdir:
    tmpdir = Path(tmpdir)

    # CSV output
    s_csv = compile_data(DISCOGS_DF, DISCOGS_LENS, into=f"csv://{tmpdir / 'csv_out'}")
    csv_files = list((tmpdir / "csv_out").glob("snf_*.csv"))
    check("csv output creates files", len(csv_files) > 0)
    check("csv has one file per dimension", len(csv_files) == len(s_csv.dimensions()))

    # SQL output
    sql_path = tmpdir / "spoke.sql"
    compile_data(DISCOGS_DF, DISCOGS_LENS, into=f"sql://{sql_path}")
    check("sql output creates file", sql_path.exists())
    sql_content = sql_path.read_text()
    check("sql contains INSERT statements", "INSERT INTO snf_spoke" in sql_content)

    # DuckDB output
    db_path = tmpdir / "spoke.duckdb"
    compile_data(DISCOGS_DF, DISCOGS_LENS, into=f"duckdb://{db_path}")
    check("duckdb output creates file", db_path.exists())

    # Lens from JSON file
    lens_path = tmpdir / "lens.json"
    lens_path.write_text(json.dumps(DISCOGS_LENS))
    s_from_path = compile_data(DISCOGS_DF, str(lens_path))
    check("lens from JSON path works", s_from_path.entity_count() == 3)

    # Source from CSV path
    csv_source = tmpdir / "source.csv"
    DISCOGS_DF.to_csv(csv_source, index=False)
    s_from_csv = compile_data(str(csv_source), DISCOGS_LENS)
    check("source from CSV path works", s_from_csv.entity_count() == 3)


# ─────────────────────────────────────────────────────────────────────────────
# 13. Substrate is reusable — engine property
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- Substrate reuse (engine property) ---")

# Simulate Reckoner calling query() multiple times against one compiled substrate
s_reuse = compile_data(DISCOGS_DF, DISCOGS_LENS)
r1 = s_reuse.query([{"category": "WHO", "field": "artist", "op": "eq", "value": "Miles Davis"}])
r2 = s_reuse.query([{"category": "WHO", "field": "artist", "op": "eq", "value": "John Coltrane"}])
r3 = s_reuse.query([{"category": "WHEN", "field": "released", "op": "between", "value": "1960", "value2": "1965"}])
check("repeated queries work", len(r1) == 1 and len(r2) == 1)
check("third query correct", set(r3) == {"discogs:release:100002", "discogs:release:100003"})
check("substrate unchanged after queries", s_reuse.entity_count() == 3)


# ─────────────────────────────────────────────────────────────────────────────
# Results
# ─────────────────────────────────────────────────────────────────────────────

print()
print(f"Results: {passed} passed, {failed} failed")
