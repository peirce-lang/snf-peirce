"""
test_conformance.py — Cross-language conformance test

Week 5 of the Python SNF package.

This is the conformance proof: a JS-created lens loaded by Python,
the same data compiled, the same Peirce queries executed, entity_ids
must match JS output exactly.

JS reference output captured from peirce> REPL session:

    WHO.author = "Miles Davis"
        → discogs:release:1234567

    WHO.author = "John Coltrane" AND WHEN.publication_date = "1964"
        → discogs:release:2345678

    WHEN.publication_date BETWEEN "1955" AND "1965"
        → discogs:release:1234567, discogs:release:2345678, discogs:release:3456789

    WHO.publisher = "Columbia"
        → discogs:release:1234567

If all four pass — the Python implementation is conformant with the
JS reference implementation on this dataset.

Run with: python test_conformance.py
"""

import sys
import os
import types
import json
import tempfile
from pathlib import Path

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
from peirce import query, ResultSet

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


# ─────────────────────────────────────────────────────────────────────────────
# Source data — reconstructed exactly from JS REPL output
#
# Every field value here is taken directly from the JS fact rows.
# These are the three entities the JS substrate contains.
# ─────────────────────────────────────────────────────────────────────────────

SOURCE_DF = pd.DataFrame([
    {
        "Catalog#":                    "CL 1355",
        "Artist":                      "Miles Davis",
        "Title":                       "Kind of Blue",
        "Label":                       "Columbia",
        "Format":                      "Vinyl LP Album",
        "Rating":                      "5",
        "Released":                    "1959",
        "release_id":                  "1234567",
        "CollectionFolder":            "All",
        "Date Added":                  "2023-01-15",
        "Collection Media Condition":  "Very Good Plus (VG+)",
        "Collection Sleeve Condition": "Very Good (VG)",
        "Collection Notes":            "First pressing",
    },
    {
        "Catalog#":                    "PRST 7829",
        "Artist":                      "John Coltrane",
        "Title":                       "A Love Supreme",
        "Label":                       "Impulse!",
        "Format":                      "Vinyl LP Album",
        "Rating":                      "5",
        "Released":                    "1964",
        "release_id":                  "2345678",
        "CollectionFolder":            "Jazz",
        "Date Added":                  "2023-02-20",
        "Collection Media Condition":  "Near Mint (NM or M-)",
        "Collection Sleeve Condition": "Very Good Plus (VG+)",
        "Collection Notes":            None,
    },
    {
        "Catalog#":                    "LSP-1707",
        "Artist":                      "Elvis Presley",
        "Title":                       "Elvis' Golden Records",
        "Label":                       "RCA Victor",
        "Format":                      "Vinyl LP Album",
        "Rating":                      "4",
        "Released":                    "1958",
        "release_id":                  "3456789",
        "CollectionFolder":            "Rock",
        "Date Added":                  "2022-11-10",
        "Collection Media Condition":  "Good Plus (G+)",
        "Collection Sleeve Condition": "Good (G)",
        "Collection Notes":            "Some wear",
    },
])

# ─────────────────────────────────────────────────────────────────────────────
# JS lens — discogs_community_v1.json (exact file from project)
# ─────────────────────────────────────────────────────────────────────────────

JS_LENS = {
    "lens_id": "discogs_community_v1",
    "lens_version": "1.0",
    "generated_at": "2026-04-11T23:13:05.933Z",
    "declaration": {
        "why|intent":        "community_field_mapping_discogs",
        "why|authority":     "discogs_collectors_working_group",
        "why|scope":         "music_collection_bibliographic",
        "why|permitted_ops": "field_mapping canonical_tagging",
        "source_format":     "discogs_csv_export",
        "domain":            "music_collection",
        "created":           "2026-04-11",
        "created_by":        "working_group_chair",
    },
    "coordinate_map": {
        "Catalog#":                    {"dimension": "what", "semantic_key": "identifier"},
        "Artist":                      {"dimension": "who",  "semantic_key": "author"},
        "Title":                       {"dimension": "what", "semantic_key": "title"},
        "Label":                       {"dimension": "who",  "semantic_key": "publisher"},
        "Format":                      {"dimension": "what", "semantic_key": "carrier_type"},
        "Rating":                      {"dimension": "what", "semantic_key": "user_rating"},
        "Released":                    {"dimension": "when", "semantic_key": "publication_date"},
        "release_id":                  {"dimension": "what", "semantic_key": "release_id"},
        "CollectionFolder":            {"dimension": "what", "semantic_key": "collection_folder"},
        "Date Added":                  {"dimension": "when", "semantic_key": "date_added"},
        "Collection Media Condition":  {"dimension": "what", "semantic_key": "media_condition"},
        "Collection Sleeve Condition": {"dimension": "what", "semantic_key": "sleeve_condition"},
        "Collection Notes":            {"dimension": "what", "semantic_key": "general_note"},
    },
    "stats": {
        "total_fields": 13,
        "by_dimension": {"who": 2, "what": 9, "when": 2, "where": 0, "how": 0},
    },
    "nucleus": {
        "type":   "single",
        "field":  "release_id",
        "prefix": "discogs:release",
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# JS reference results — taken verbatim from REPL output
# ─────────────────────────────────────────────────────────────────────────────

JS_RESULTS = {
    'WHO.author = "Miles Davis"':
        {"discogs:release:1234567"},

    'WHO.author = "John Coltrane" AND WHEN.publication_date = "1964"':
        {"discogs:release:2345678"},

    'WHEN.publication_date BETWEEN "1955" AND "1965"':
        {"discogs:release:1234567", "discogs:release:2345678", "discogs:release:3456789"},

    'WHO.publisher = "Columbia"':
        {"discogs:release:1234567"},
}


# ─────────────────────────────────────────────────────────────────────────────
# Setup — compile once, query many times (engine property)
# ─────────────────────────────────────────────────────────────────────────────

print("--- Setup ---")
substrate = compile_data(SOURCE_DF, JS_LENS)
check("compiled successfully", substrate is not None)
check("correct entity count", substrate.entity_count() == 3)
check("lens_id matches JS", substrate.lens_id == "discogs_community_v1")

expected_dims = {"who", "what", "when"}
check("dimensions present", set(substrate.dimensions()) == expected_dims)

# Verify entity IDs match JS exactly
df_all = substrate.to_dataframe()
python_entity_ids = set(df_all["entity_id"].unique())
js_all_entity_ids = {
    "discogs:release:1234567",
    "discogs:release:2345678",
    "discogs:release:3456789",
}
check("entity_ids match JS exactly", python_entity_ids == js_all_entity_ids,
      f"got {python_entity_ids}")


# ─────────────────────────────────────────────────────────────────────────────
# Conformance queries — each must match JS output exactly
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- Conformance queries ---")

for peirce_string, expected_ids in JS_RESULTS.items():
    result = query(substrate, peirce_string, limit=None)
    python_ids = set(result.entity_ids)
    check(
        f'"{peirce_string}"',
        python_ids == expected_ids,
        f"got {python_ids}, expected {expected_ids}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Fact-level conformance — spot-check specific facts from JS output
#
# These are taken directly from the JS REPL display rows.
# If these pass, the coordinate map is being applied identically.
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- Fact-level conformance ---")

r_miles = query(substrate, 'WHO.author = "Miles Davis"', limit=None)
df_miles = r_miles.to_dataframe()

def has_fact(df, dimension, semantic_key, value):
    return ((df["dimension"] == dimension) &
            (df["semantic_key"] == semantic_key) &
            (df["value"] == value)).any()

# From JS output for discogs:release:1234567
check("WHO author Miles Davis",     has_fact(df_miles, "who",  "author",           "Miles Davis"))
check("WHO publisher Columbia",     has_fact(df_miles, "who",  "publisher",         "Columbia"))
check("WHAT title Kind of Blue",    has_fact(df_miles, "what", "title",             "Kind of Blue"))
check("WHAT identifier CL 1355",    has_fact(df_miles, "what", "identifier",        "CL 1355"))
check("WHAT carrier_type",          has_fact(df_miles, "what", "carrier_type",      "Vinyl LP Album"))
check("WHAT user_rating 5",         has_fact(df_miles, "what", "user_rating",       "5"))
check("WHAT release_id 1234567",    has_fact(df_miles, "what", "release_id",        "1234567"))
check("WHEN publication_date 1959", has_fact(df_miles, "when", "publication_date",  "1959"))
check("WHEN date_added",            has_fact(df_miles, "when", "date_added",        "2023-01-15"))
check("WHAT media_condition",       has_fact(df_miles, "what", "media_condition",   "Very Good Plus (VG+)"))
check("WHAT general_note",          has_fact(df_miles, "what", "general_note",      "First pressing"))

# Coordinate string format
coord_sample = df_miles[
    (df_miles["dimension"] == "who") &
    (df_miles["semantic_key"] == "author")
]["coordinate"].values
check("coordinate triadic format",
      len(coord_sample) > 0 and coord_sample[0] == "WHO|author|Miles Davis")

# Null note field for Coltrane — should not appear as a fact
r_coltrane = query(substrate, 'WHO.author = "John Coltrane"', limit=None)
df_coltrane = r_coltrane.to_dataframe()
check("null field not emitted",
      not has_fact(df_coltrane, "what", "general_note", ""))
check("null field truly absent",
      not (df_coltrane["semantic_key"] == "general_note").any())


# ─────────────────────────────────────────────────────────────────────────────
# Lens round-trip — load JS lens JSON from file, compile, query
# Proves load() → compile_data() → query() chain works end-to-end
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- Lens round-trip from JSON file ---")

from lens import load, save

with tempfile.TemporaryDirectory() as tmpdir:
    lens_path = Path(tmpdir) / "discogs_community_v1.json"

    # Save the JS lens to a file (simulates downloading from lens-tool UI)
    save(JS_LENS, lens_path)

    # Load it back (simulates lens.load() in the real workflow)
    loaded_lens = load(lens_path)

    # Compile against the loaded lens
    substrate_rt = compile_data(SOURCE_DF, loaded_lens)

    check("round-trip lens_id", substrate_rt.lens_id == "discogs_community_v1")
    check("round-trip entity_count", substrate_rt.entity_count() == 3)

    # Run the conformance queries against the round-tripped substrate
    for peirce_string, expected_ids in JS_RESULTS.items():
        result = query(substrate_rt, peirce_string, limit=None)
        check(
            f"round-trip: {peirce_string[:40]}...",
            set(result.entity_ids) == expected_ids,
            f"got {set(result.entity_ids)}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# ResultSet conformance — verify result object shape is correct
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- ResultSet shape ---")

r = query(substrate, 'WHEN.publication_date BETWEEN "1955" AND "1965"', limit=None)
check("entity_ids is list", isinstance(r.entity_ids, list))
check("count matches", r.count == 3)
check("len() matches", len(r) == 3)
check("iter yields ids", sorted(r) == sorted(r.entity_ids))
check("to_dataframe works", isinstance(r.to_dataframe(), pd.DataFrame))
check("pivot works", isinstance(r.pivot(), pd.DataFrame))
check("_repr_html_ works", "<table" in r._repr_html_())
check("__repr__ works", "3 results" in repr(r))


# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────

print()
print("=" * 60)
print(f"  Conformance result: {passed} passed, {failed} failed")
if failed == 0:
    print("  ✓ Python implementation is conformant with JS reference")
else:
    print("  ✗ Conformance failures — check output above")
print("=" * 60)
