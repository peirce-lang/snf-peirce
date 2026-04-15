# snf-peirce — Setup & Usage Cheat Sheet
# For Python novices. Everything you need to get running.

# ─────────────────────────────────────────────────────────────────────────────
# 1. WHAT SHOULD BE IN YOUR FOLDER
# ─────────────────────────────────────────────────────────────────────────────

# Your folder (call it snf-peirce/ or whatever you like) should contain:
#
#   parser.py           ← Peirce query parser
#   lens.py             ← LensDraft, suggest(), load(), save()
#   compile.py          ← compile_data(), Substrate
#   peirce.py           ← query(), execute(), ResultSet
#   shell.py            ← interactive REPL shell
#   __init__.py         ← makes it a package (create this — see below)
#
# Test files (optional, keep these too):
#   test_parser.py
#   test_lens.py
#   test_compile.py
#   test_peirce.py
#   test_conformance.py


# ─────────────────────────────────────────────────────────────────────────────
# 2. CREATE __init__.py  (one-time setup)
# ─────────────────────────────────────────────────────────────────────────────

# In your folder, create a file called __init__.py with this content:

"""
snf-peirce — Python SNF package
"""
from .parser  import parse, parse_to_constraints, parseToConstraints
from .lens    import suggest, load, save, validate, LensDraft, LensValidationError
from .compile import compile_data, Substrate, CompileError, NucleusError
from .peirce  import query, execute, ResultSet, PeirceParseError, PeirceDiscoveryError

# That's it. One file, these lines exactly.


# ─────────────────────────────────────────────────────────────────────────────
# 3. INSTALL DEPENDENCIES  (one-time, run in terminal)
# ─────────────────────────────────────────────────────────────────────────────

#   pip install pandas duckdb

# That's all you need. No other dependencies.


# ─────────────────────────────────────────────────────────────────────────────
# 4. RUNNING THE TESTS  (from inside your folder)
# ─────────────────────────────────────────────────────────────────────────────

#   python test_parser.py
#   python test_lens.py
#   python test_compile.py
#   python test_peirce.py
#   python test_conformance.py

# You should see "Results: N passed, 0 failed" for each one.
# If any fail, something is wrong with the installation — check step 3.


# ─────────────────────────────────────────────────────────────────────────────
# 5. THE INTERACTIVE SHELL  (from inside your folder)
# ─────────────────────────────────────────────────────────────────────────────

# First you need a compiled substrate. Two ways to get one:

# WAY A — you already have a csv:// substrate from lens-tool ingest:
#   python shell.py csv://path/to/your/spoke_dir

# WAY B — compile fresh from a CSV + lens JSON:
#   python -c "
#   import pandas as pd
#   from lens import load
#   from compile import compile_data
#   df = pd.read_csv('mydata.csv')
#   lens = load('mylens.json')
#   compile_data(df, lens, into='csv://my_spoke_dir')
#   print('done')
#   "
#   Then: python shell.py csv://my_spoke_dir

# Shell commands once you're in:
#   WHO.artist = "Miles Davis"              run a query
#   WHO.artist = "Miles Davis" AND WHEN.released = "1959"
#   WHEN.released BETWEEN "1955" AND "1965"
#   WHO.<TAB>                               TAB-complete field names
#   \schema                                 show all dimensions and fields
#   \schema WHO                             show just WHO fields
#   \explain                                explain last query's execution plan
#   \pivot                                  toggle wide table view
#   \limit 50                               show up to 50 results
#   \history                                show previous queries
#   exit                                    quit


# ─────────────────────────────────────────────────────────────────────────────
# 6. USING FROM A PYTHON SCRIPT OR JUPYTER NOTEBOOK
# ─────────────────────────────────────────────────────────────────────────────

# If your script/notebook is INSIDE the snf-peirce folder:
import sys
sys.path.insert(0, '.')          # tells Python to look in current folder

from lens    import suggest, load
from compile import compile_data
from peirce  import query

# If your script/notebook is OUTSIDE the folder (e.g. one level up):
import sys
sys.path.insert(0, './snf-peirce')   # adjust path to wherever your folder is


# ─────────────────────────────────────────────────────────────────────────────
# 7. THE FULL WORKFLOW — from CSV to query result
# ─────────────────────────────────────────────────────────────────────────────

import pandas as pd
from lens    import suggest, load
from compile import compile_data
from peirce  import query

# Step 1 — load your data
df = pd.read_csv("mydata.csv")
df.head()                        # check it looks right

# Step 2 — author a lens
draft = suggest(df)              # get smart suggestions
draft                            # in Jupyter: renders as a table
                                 # in a script: print(draft)

# Step 3 — adjust mappings you disagree with (all chainable)
draft.map("Artist", "who", "artist")
draft.map("Released", "when", "released")
draft.map("Title", "what", "title")
draft.nucleus("release_id", prefix="mydata:release")

# Step 4 — compile
lens     = draft.to_lens(lens_id="mydata_v1", authority="me")
compiled = compile_data(df, lens)
compiled                         # shows entity count, fact count, dimensions

# Step 5 — query
result = query(compiled, 'WHO.artist = "Miles Davis"')
result                           # in Jupyter: renders as a table

# Step 6 — use the results
result.entity_ids                # list of matching entity IDs
result.count                     # number of results
result.to_dataframe()            # full spoke rows as pandas DataFrame
result.pivot()                   # wide format: one row per entity


# ─────────────────────────────────────────────────────────────────────────────
# 8. LOADING AN EXISTING LENS-TOOL LENS
# ─────────────────────────────────────────────────────────────────────────────

# If you already have a lens JSON from the JS lens-tool UI:
from lens import load
from compile import compile_data
import pandas as pd

lens     = load("discogs_community_v1.json")   # load the JS-created lens
df       = pd.read_csv("mydata.csv")
compiled = compile_data(df, lens)

# The lens and the Python package speak the same format.
# A JS-created lens loads without modification.


# ─────────────────────────────────────────────────────────────────────────────
# 9. SAVING A COMPILED SUBSTRATE  (so you don't recompile every time)
# ─────────────────────────────────────────────────────────────────────────────

# Compile once and save:
compiled = compile_data(df, lens, into="csv://my_spoke_dir")

# Next time, load it directly in the shell:
#   python shell.py csv://my_spoke_dir

# Or load it in a script:
from compile import compile_data
import pandas as pd
from lens import load

# Re-compile from source (simplest approach while iterating):
lens     = load("my_lens.json")
df       = pd.read_csv("mydata.csv")
compiled = compile_data(df, lens)          # ephemeral, in-memory
result   = query(compiled, 'WHO.artist = "Miles Davis"')


# ─────────────────────────────────────────────────────────────────────────────
# 10. COMPOSITE NUCLEUS EXAMPLE  (client + matter)
# ─────────────────────────────────────────────────────────────────────────────

import pandas as pd
from lens    import suggest
from compile import compile_data
from peirce  import query

df = pd.DataFrame({
    "client_id":   ["CLI001", "CLI001", "CLI002"],
    "matter_id":   ["MAT001", "MAT002", "MAT001"],
    "attorney":    ["Smith",  "Jones",  "Smith"],
    "matter_type": ["litigation", "ip", "litigation"],
    "year":        ["2023", "2024", "2024"],
    "office":      ["Seattle", "New York", "Seattle"],
})

draft = suggest(df)
draft.map("attorney",    "who",   "attorney")
draft.map("matter_type", "why",   "matter_type")
draft.map("year",        "when",  "year")
draft.map("office",      "where", "office")
draft.nucleus_composite(["client_id", "matter_id"],
                        separator="-",
                        prefix="legal:matter")

lens     = draft.to_lens(lens_id="legal_v1", authority="firm")
compiled = compile_data(df, lens)

# Entity IDs look like: legal:matter:CLI001-MAT001
result = query(compiled, 'WHO.attorney = "Smith"')
print(result.entity_ids)
# → ['legal:matter:CLI001-MAT001', 'legal:matter:CLI002-MAT001']


# ─────────────────────────────────────────────────────────────────────────────
# 11. COMMON ERRORS AND WHAT THEY MEAN
# ─────────────────────────────────────────────────────────────────────────────

# NucleusError: "Row 3: nucleus field(s) ['release_id'] are null or empty"
#   → One of your rows has a blank value in the nucleus column.
#   → Every row must have a non-null nucleus value.
#   → Fix: clean your data before compiling, or choose a different nucleus field.

# CompileError: "Nucleus field(s) ['release_id'] not found in source data"
#   → The field you declared as nucleus doesn't exist as a column.
#   → Check spelling — column names are case-sensitive.

# PeirceParseError: "Expected operator after 'WHO.artist' but got 'Miles'"
#   → Missing operator in your query. Did you forget = ?
#   → Correct: WHO.artist = "Miles Davis"
#   → Wrong:   WHO.artist "Miles Davis"

# KeyError in draft.map("ColumnName", ...)
#   → The column name doesn't match exactly.
#   → Column names are case-sensitive. Check df.columns to see exact names.

# LensValidationError when loading a lens
#   → The JSON file is missing required fields (lens_id, coordinate_map, nucleus).
#   → Usually means you're loading the wrong file.


# ─────────────────────────────────────────────────────────────────────────────
# 12. QUICK REFERENCE — PEIRCE QUERY SYNTAX
# ─────────────────────────────────────────────────────────────────────────────

# Equality
'WHO.artist = "Miles Davis"'
'WHEN.released = "1959"'

# Inequality
'WHO.artist != "Miles Davis"'

# Comparison (works on string values lexicographically)
'WHEN.released > "1960"'
'WHEN.released >= "1959"'

# Range (inclusive)
'WHEN.released BETWEEN "1955" AND "1965"'

# Text matching
'WHAT.title CONTAINS "Blue"'       # substring match
'WHO.artist PREFIX "Miles"'        # starts with

# Negation
'NOT WHERE.office = "Seattle"'

# AND — intersection across dimensions (narrows results)
'WHO.artist = "Miles Davis" AND WHEN.released = "1959"'

# OR — union, top-level only (widens results)
'WHO.artist = "Miles Davis" OR WHO.artist = "John Coltrane"'

# Full DNF — OR of AND groups
'(WHO.artist = "Miles Davis" AND WHEN.released = "1959") OR (WHO.artist = "John Coltrane" AND WHEN.released = "1964")'

# Discovery (in the shell — shows schema, not query results)
'*'             # all dimensions
'WHO|*'         # all fields in WHO
'WHO|artist|*'  # all values for WHO.artist
