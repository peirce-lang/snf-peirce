"""
test_lens.py — Tests for lens.py

Run with: pytest test_lens.py -v
Or directly: python test_lens.py
"""

import json
import sys
import os
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd
from lens import (
    LensDraft, suggest, load, save, validate, LensValidationError
)

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

def make_discogs_df():
    return pd.DataFrame({
        "Catalog#":    ["CAT001", "CAT002", "CAT003"],
        "Artist":      ["Miles Davis", "John Coltrane", "Bill Evans"],
        "Title":       ["Kind of Blue", "A Love Supreme", "Waltz for Debby"],
        "Label":       ["Columbia", "Impulse!", "Riverside"],
        "Format":      ["LP", "LP", "LP"],
        "Rating":      [5, 4, 5],
        "Released":    ["1959", "1964", "1961"],
        "release_id":  [100001, 100002, 100003],
        "Date Added":  ["2024-01-01", "2024-01-02", "2024-01-03"],
    })

def make_legal_df():
    return pd.DataFrame({
        "client_id":   ["CLI001", "CLI001", "CLI002"],
        "matter_id":   ["MAT001", "MAT002", "MAT001"],
        "attorney":    ["Smith", "Jones", "Smith"],
        "matter_type": ["litigation", "ip", "litigation"],
        "year":        [2023, 2024, 2024],
        "office":      ["Seattle", "New York", "Seattle"],
    })

DISCOGS_LENS = {
    "lens_id": "discogs_community_v1",
    "lens_version": "1.0",
    "generated_at": "2026-04-11T23:13:05.933Z",
    "declaration": {
        "why|intent": "community_field_mapping_discogs",
        "why|authority": "discogs_collectors_working_group",
        "why|scope": "music_collection_bibliographic",
        "why|permitted_ops": "field_mapping canonical_tagging",
        "source_format": "discogs_csv_export",
        "domain": "music_collection",
        "created": "2026-04-11",
        "created_by": "working_group_chair"
    },
    "coordinate_map": {
        "Artist":   {"dimension": "who",  "semantic_key": "author"},
        "Title":    {"dimension": "what", "semantic_key": "title"},
        "Released": {"dimension": "when", "semantic_key": "publication_date"},
        "release_id": {"dimension": "what", "semantic_key": "release_id"},
    },
    "stats": {
        "total_fields": 4,
        "by_dimension": {"who": 1, "what": 2, "when": 1, "where": 0, "how": 0}
    },
    "nucleus": {
        "type": "single",
        "field": "release_id",
        "prefix": "discogs:release"
    }
}


# ─────────────────────────────────────────────────────────────────────────────
# 1. LensDraft direct construction
# ─────────────────────────────────────────────────────────────────────────────

print("--- LensDraft construction ---")

rows = [
    {"column": "Artist", "dimension": "who", "semantic_key": "artist",
     "confidence": "high", "reason": "test", "nucleus_candidate": False},
    {"column": "Released", "dimension": "when", "semantic_key": "released",
     "confidence": "high", "reason": "test", "nucleus_candidate": False},
    {"column": "release_id", "dimension": "what", "semantic_key": "release_id",
     "confidence": "medium", "reason": "test", "nucleus_candidate": True},
]
draft = LensDraft(rows)
check("columns in order", draft.columns() == ["Artist", "Released", "release_id"])
check("get returns copy", draft.get("Artist")["dimension"] == "who")


# ─────────────────────────────────────────────────────────────────────────────
# 2. map() — overriding mappings
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- map() ---")

draft2 = LensDraft(rows)
result = draft2.map("Artist", "who", "artist_name")
check("map returns self for chaining", result is draft2)
check("map updates dimension", draft2.get("Artist")["dimension"] == "who")
check("map updates semantic_key", draft2.get("Artist")["semantic_key"] == "artist_name")
check("map sets confidence to manual", draft2.get("Artist")["confidence"] == "manual")

# map chaining
draft3 = LensDraft(rows)
draft3.map("Artist", "who", "artist").map("Released", "when", "released")
check("chained map works", draft3.get("Released")["semantic_key"] == "released")

# map invalid column
check_raises("map unknown column raises KeyError",
             lambda: draft3.map("NonExistent", "who", "x"), KeyError)

# map invalid dimension
check_raises("map invalid dimension raises ValueError",
             lambda: draft3.map("Artist", "foo", "x"), ValueError)


# ─────────────────────────────────────────────────────────────────────────────
# 3. nucleus()
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- nucleus() ---")

draft4 = LensDraft(rows)
result = draft4.nucleus("release_id", prefix="discogs:release")
check("nucleus returns self for chaining", result is draft4)

check_raises("nucleus unknown column raises KeyError",
             lambda: LensDraft(rows).nucleus("nonexistent"), KeyError)


# ─────────────────────────────────────────────────────────────────────────────
# 4. nucleus_composite()
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- nucleus_composite() ---")

legal_rows = [
    {"column": "client_id", "dimension": "who", "semantic_key": "client",
     "confidence": "medium", "reason": "test", "nucleus_candidate": True},
    {"column": "matter_id", "dimension": "what", "semantic_key": "matter",
     "confidence": "medium", "reason": "test", "nucleus_candidate": True},
    {"column": "attorney", "dimension": "who", "semantic_key": "attorney",
     "confidence": "high", "reason": "test", "nucleus_candidate": False},
]
legal_draft = LensDraft(legal_rows)
result = legal_draft.nucleus_composite(["client_id", "matter_id"],
                                        separator="-", prefix="legal:matter")
check("nucleus_composite returns self", result is legal_draft)

check_raises("nucleus_composite < 2 fields raises ValueError",
             lambda: LensDraft(legal_rows).nucleus_composite(["client_id"]), ValueError)

check_raises("nucleus_composite unknown column raises KeyError",
             lambda: LensDraft(legal_rows).nucleus_composite(["client_id", "nonexistent"]), KeyError)


# ─────────────────────────────────────────────────────────────────────────────
# 5. to_lens()
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- to_lens() ---")

draft5 = LensDraft(rows)
draft5.nucleus("release_id", prefix="discogs:release")
lens = draft5.to_lens(lens_id="discogs_v1", authority="abk")

check("to_lens has lens_id", lens["lens_id"] == "discogs_v1")
check("to_lens has lens_version", "lens_version" in lens)
check("to_lens has generated_at", "generated_at" in lens)
check("to_lens has declaration", "declaration" in lens)
check("to_lens declaration has authority", lens["declaration"]["why|authority"] == "abk")
check("to_lens has coordinate_map", "coordinate_map" in lens)
check("to_lens has stats", "stats" in lens)
check("to_lens has nucleus", lens["nucleus"]["type"] == "single")
check("to_lens nucleus field", lens["nucleus"]["field"] == "release_id")
check("to_lens nucleus prefix", lens["nucleus"]["prefix"] == "discogs:release")

# coordinate_map shape
cm = lens["coordinate_map"]
check("coordinate_map Artist dimension", cm["Artist"]["dimension"] == "who")
check("coordinate_map Artist semantic_key", cm["Artist"]["semantic_key"] == "artist")
check("coordinate_map entry has only dimension and semantic_key",
      set(cm["Artist"].keys()) == {"dimension", "semantic_key"})

# stats computed correctly
check("stats total_fields", lens["stats"]["total_fields"] == 3)
check("stats by_dimension who", lens["stats"]["by_dimension"]["who"] == 1)
check("stats by_dimension when", lens["stats"]["by_dimension"]["when"] == 1)

# to_lens without nucleus raises
check_raises("to_lens without nucleus raises ValueError",
             lambda: LensDraft(rows).to_lens(lens_id="x", authority="y"), ValueError)

# to_lens with composite nucleus
legal_draft2 = LensDraft(legal_rows)
legal_draft2.nucleus_composite(["client_id", "matter_id"], separator="-", prefix="legal:matter")
legal_lens = legal_draft2.to_lens(lens_id="legal_v1", authority="firm")
check("composite nucleus type", legal_lens["nucleus"]["type"] == "composite")
check("composite nucleus fields", legal_lens["nucleus"]["fields"] == ["client_id", "matter_id"])
check("composite nucleus separator", legal_lens["nucleus"]["separator"] == "-")
check("composite nucleus prefix", legal_lens["nucleus"]["prefix"] == "legal:matter")

# optional kwargs in to_lens
lens2 = draft5.to_lens(
    lens_id="discogs_v1", authority="abk",
    domain="music_collection", source_format="discogs_csv_export",
    created_by="test_user"
)
check("to_lens kwargs domain", lens2["declaration"]["domain"] == "music_collection")
check("to_lens kwargs source_format", lens2["declaration"]["source_format"] == "discogs_csv_export")
check("to_lens kwargs created_by", lens2["declaration"]["created_by"] == "test_user")


# ─────────────────────────────────────────────────────────────────────────────
# 6. suggest()
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- suggest() ---")

df = make_discogs_df()
draft6 = suggest(df)
check("suggest returns LensDraft", isinstance(draft6, LensDraft))
check("suggest has all columns", set(draft6.columns()) == set(df.columns))

# Check specific inferences
artist_row = draft6.get("Artist")
check("suggest Artist → who", artist_row["dimension"] == "who")

# Released inferred as when when in full dataframe context
# (string column "Released" has a date-like name — confirmed with full df)
released_row = suggest(make_discogs_df()).get("Released")
check("suggest Released → when", released_row["dimension"] == "when")

# From CSV path
with tempfile.NamedTemporaryFile(suffix=".csv", mode="w", delete=False) as f:
    df.to_csv(f, index=False)
    csv_path = f.name
try:
    draft_from_csv = suggest(csv_path)
    check("suggest from CSV path works", isinstance(draft_from_csv, LensDraft))
    check("suggest from Path works", isinstance(suggest(Path(csv_path)), LensDraft))
finally:
    os.unlink(csv_path)

# suggest from invalid type
check_raises("suggest invalid type raises TypeError",
             lambda: suggest(42), TypeError)

# Legal dataset
legal_df = make_legal_df()
legal_draft3 = suggest(legal_df)
check("suggest attorney → who", legal_draft3.get("attorney")["dimension"] == "who")
check("suggest year → when",    legal_draft3.get("year")["dimension"] == "when")
check("suggest office → where", legal_draft3.get("office")["dimension"] == "where")


# ─────────────────────────────────────────────────────────────────────────────
# 7. validate()
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- validate() ---")

check("validate valid lens", validate(DISCOGS_LENS)["valid"] is True)
check("validate non-dict fails", validate("string")["valid"] is False)
check("validate missing lens_id", validate({"coordinate_map": {}, "nucleus": {"type": "single", "field": "x"}})["valid"] is False)
check("validate missing coordinate_map", validate({"lens_id": "x", "nucleus": {"type": "single", "field": "x"}})["valid"] is False)
check("validate missing nucleus", validate({"lens_id": "x", "coordinate_map": {}})["valid"] is False)
check("validate invalid dimension", validate({
    "lens_id": "x",
    "coordinate_map": {"col": {"dimension": "INVALID", "semantic_key": "k"}},
    "nucleus": {"type": "single", "field": "col"}
})["valid"] is False)
check("validate composite nucleus needs 2 fields", validate({
    "lens_id": "x",
    "coordinate_map": {},
    "nucleus": {"type": "composite", "fields": ["only_one"]}
})["valid"] is False)
check("validate errors is list", isinstance(validate({"x": 1})["errors"], list))


# ─────────────────────────────────────────────────────────────────────────────
# 8. load() / save()
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- load() / save() ---")

with tempfile.TemporaryDirectory() as tmpdir:
    path = Path(tmpdir) / "test_lens.json"

    # save then load round-trip
    save(DISCOGS_LENS, path)
    check("save creates file", path.exists())
    loaded = load(path)
    check("load returns dict", isinstance(loaded, dict))
    check("round-trip lens_id", loaded["lens_id"] == DISCOGS_LENS["lens_id"])
    check("round-trip nucleus type", loaded["nucleus"]["type"] == "single")
    check("round-trip coordinate_map", loaded["coordinate_map"] == DISCOGS_LENS["coordinate_map"])

    # save invalid lens raises
    check_raises("save invalid lens raises LensValidationError",
                 lambda: save({"bad": "lens"}, path), LensValidationError)

# load nonexistent file
check_raises("load nonexistent raises FileNotFoundError",
             lambda: load("/nonexistent/path/lens.json"), FileNotFoundError)

# load invalid JSON shape
with tempfile.NamedTemporaryFile(suffix=".json", mode="w", delete=False) as f:
    json.dump({"not": "a valid lens"}, f)
    bad_path = f.name
try:
    check_raises("load invalid lens raises LensValidationError",
                 lambda: load(bad_path), LensValidationError)
finally:
    os.unlink(bad_path)


# ─────────────────────────────────────────────────────────────────────────────
# 9. Cross-language conformance shape
# ─────────────────────────────────────────────────────────────────────────────

print()
print("--- Cross-language conformance ---")

# Build a lens that should match discogs_community_v1.json schema exactly
df_full = pd.DataFrame({
    "Artist":    ["Miles Davis"],
    "Title":     ["Kind of Blue"],
    "Released":  ["1959"],
    "release_id": [100001],
})
draft_full = suggest(df_full)
draft_full.map("Artist", "who", "author")
draft_full.map("Title", "what", "title")
draft_full.map("Released", "when", "publication_date")
draft_full.map("release_id", "what", "release_id")
draft_full.nucleus("release_id", prefix="discogs:release")
lens_full = draft_full.to_lens(
    lens_id="discogs_community_v1",
    authority="discogs_collectors_working_group",
    intent="community_field_mapping_discogs",
    scope="music_collection_bibliographic",
    source_format="discogs_csv_export",
    domain="music_collection",
)

check("schema has all required top-level keys",
      {"lens_id","lens_version","generated_at","declaration","coordinate_map","stats","nucleus"}
      .issubset(set(lens_full.keys())))
check("declaration has all required keys",
      {"why|intent","why|authority","why|scope","why|permitted_ops",
       "source_format","domain","created","created_by"}
      .issubset(set(lens_full["declaration"].keys())))
check("coordinate_map entries have only dimension+semantic_key",
      all(set(v.keys()) == {"dimension","semantic_key"}
          for v in lens_full["coordinate_map"].values()))
check("stats has total_fields and by_dimension",
      "total_fields" in lens_full["stats"] and "by_dimension" in lens_full["stats"])
check("by_dimension has all six dims minus why",
      set(lens_full["stats"]["by_dimension"].keys()) == {"who","what","when","where","how"})


# ─────────────────────────────────────────────────────────────────────────────
# Results
# ─────────────────────────────────────────────────────────────────────────────

print()
print(f"Results: {passed} passed, {failed} failed")
