# snf-peirce

Python implementation of the SNF (Semantic Normalized Form) stack.

Map your data to meaning once. Query it in plain language forever. No SQL. No physical schema knowledge. No joins.

The lens is the semantic schema — `suggest()` builds it from your data, you confirm it, and from that point on your data answers questions.

```python
from snf_peirce import suggest, compile_data, query
import pandas as pd

df       = pd.read_csv("my_collection.csv")
draft    = suggest(df)
draft.map("Artist", "who", "artist").nucleus("release_id", prefix="discogs:release")
lens     = draft.to_lens(lens_id="discogs_v1", authority="me")
compiled = compile_data(df, lens)

query(compiled, 'WHO.artist = "Miles Davis" AND WHEN.released = "1959"')
```

---

## What is SNF?

SNF (Semantic Normalized Form) is a data model and query protocol built around six universal dimensions:

| Dimension | Meaning | Examples |
|---|---|---|
| **WHO** | People, organisations, roles | author, publisher, attorney, artist |
| **WHAT** | Things, topics, identifiers | title, subject, ISBN, genre |
| **WHEN** | Dates, years, time periods | publication_date, year, date_added |
| **WHERE** | Places, locations, regions | publication_place, office, territory |
| **WHY** | Reasons, types, purposes | matter_type, audience, format_legal |
| **HOW** | Methods, formats, measurements | carrier_type, cmc, media_type |

Every fact in your data maps to one of these dimensions. Once mapped, you query by meaning — not by column name, table structure, or JOIN logic.

**Peirce** is the query language for SNF. Named after Charles Sanders Peirce, whose triadic sign relation maps directly onto the SNF record structure: Dimension → SemanticKey → Value.

---

## Install

```bash
pip install snf-peirce
```

Or clone and use directly:

```bash
git clone https://github.com/peirce-lang/snf-peirce
cd snf-peirce
pip install -e .
```

---

## Quick start

### From a CSV

```python
import pandas as pd
from snf_peirce import suggest, compile_data, query

df    = pd.read_csv("matters.csv")
draft = suggest(df)
print(draft)   # renders as a table in Jupyter

draft.map("attorney_name", "who",   "attorney")
draft.map("matter_type",   "why",   "matter_type")
draft.map("fiscal_year",   "when",  "year")
draft.map("office",        "where", "office")
draft.nucleus_composite(["client_id", "matter_id"],
                         separator="-", prefix="legal:matter")

lens     = draft.to_lens(lens_id="legal_v1", authority="firm")
compiled = compile_data(df, lens)

query(compiled, 'WHO.attorney = "Smith" AND WHERE.office = "Seattle"')
query(compiled, 'WHEN.year BETWEEN "2022" AND "2024"')
query(compiled, 'WHO.attorney = "Smith" OR WHO.attorney = "Jones"')
```

### From an existing lens-tool lens

Lenses created by the JavaScript lens-tool are directly compatible:

```python
from snf_peirce import load, compile_data, query
import pandas as pd

lens     = load("discogs_community_v1.json")
df       = pd.read_csv("discogs_sample.csv")
compiled = compile_data(df, lens)

query(compiled, 'WHO.author = "Miles Davis"')
query(compiled, 'WHEN.publication_date BETWEEN "1955" AND "1965"')
```

---

## Interactive shell

```bash
python shell.py csv://my_spoke_dir
```

```
peirce> WHO.author = "Miles Davis"
peirce> WHEN.publication_date BETWEEN "1955" AND "1965"
peirce> WHO.author = "Miles Davis" AND WHEN.publication_date = "1959"
peirce> \schema             — show all dimensions and fields
peirce> \schema WHO         — show fields in WHO with counts
peirce> \explain            — show execution plan for last query
peirce> WHO.<TAB>           — TAB-completes field names from substrate
peirce> \pivot              — toggle wide table view
peirce> exit
```

Shell features beyond the JS version:
- **TAB completion** — field names from the actual substrate
- **`\explain`** — execution plan with cardinality bars
- **`\schema`** — dimensions, fields, entity and value counts
- **Discovery expressions** — `WHO|*`, `WHAT|genre|*` work inline

---

## Guided setup (no coding required)

```bash
python guided_ingest.py
python guided_ingest.py mydata.csv
```

Walks through CSV → lens authoring → compilation → shell with prompts. No code required.

---

## Fetch from public APIs

### Scryfall (Magic: The Gathering)

```bash
pip install requests
python fetch_scryfall.py              # Guilds of Ravnica (default)
python fetch_scryfall.py war          # War of the Spark
python fetch_scryfall.py --list       # see available sets
```

```
peirce> WHAT.guild = "Dimir"
peirce> WHAT.color = "Blue" AND WHAT.color = "Black"
peirce> WHAT.card_type = "Creature" AND HOW.cmc BETWEEN "1" AND "3"
peirce> WHAT.keyword = "Surveil"
peirce> WHO.artist = "Seb McKinnon"
peirce> WHAT|guild|*
```

### Library of Congress catalog

```bash
python fetch_loc.py                          # default: jazz music
python fetch_loc.py "toni morrison"          # keyword search
python fetch_loc.py --subject "cooking"      # subject search
python fetch_loc.py --author "hemingway"     # author search
python fetch_loc.py --marc-file catalog.mrc  # from a .mrc file
```

```
peirce> WHO.author CONTAINS "Morrison"
peirce> WHAT.subject_topic CONTAINS "Jazz"
peirce> WHEN.publication_date BETWEEN "1950" AND "1970"
peirce> WHERE.publication_place = "New York"
peirce> WHAT|subject_topic|*
```

### Build your own fetcher

```python
from base_fetcher import SNFFetcher, fact, facts, facts_from_list

class MyAPIFetcher(SNFFetcher):
    lens_id   = "myapi_v1"
    set_name  = "My Dataset"
    spoke_dir = "myapi_spoke"

    def fetch(self):
        import requests
        return requests.get("https://api.example.com/data").json()["items"]

    def entity_id(self, item):
        return f"myapi:{item['id']}"

    def translate(self, item):
        eid = self.entity_id(item)
        return [
            *facts(
                (eid, "what", "title",  item.get("title")),
                (eid, "who",  "author", item.get("author")),
                (eid, "when", "year",   item.get("year")),
            ),
            *facts_from_list(eid, "what", "genre", item.get("genres", [])),
        ]

if __name__ == "__main__":
    MyAPIFetcher().run()
```

---

## Peirce query syntax

```
WHO.artist = "Miles Davis"                         equality
WHO.artist != "Miles Davis"                        not equal
WHEN.released > "1960"                             comparison
WHEN.released BETWEEN "1955" AND "1965"            range (inclusive)
WHAT.title CONTAINS "Blue"                         substring
WHO.artist PREFIX "Miles"                          starts with
NOT WHERE.office = "Seattle"                       negation
WHO.artist = "Miles Davis" AND WHEN.released = "1959"    AND (intersection)
WHO.artist = "Miles Davis" OR WHO.artist = "Coltrane"    OR (union)
(WHO.artist = "Miles Davis" AND WHEN.released = "1959")
OR
(WHO.artist = "John Coltrane" AND WHEN.released = "1964")  DNF

# Discovery (shell only — shows schema)
*                 all dimensions
WHO|*             all fields in WHO
WHO|artist|*      all values for WHO.artist
```

---

## MARC support

snf-peirce ships with the MARC Bibliographic Lens v1.0 — a complete
field mapping from MARC21 tags to SNF dimensions. Python port of
`MARCTranslator_v3.js`. No extra dependencies required.

```python
from parse_marc import parse_mrc
from marc_translator import MARCTranslator

records    = parse_mrc("catalog.mrc")
translator = MARCTranslator(source_id="loc")

for record in records:
    facts = translator.translate_record(record)
```

Key field mappings:

| MARC tag | → | Dimension | Semantic key |
|---|---|---|---|
| 100$a | → | WHO | author |
| 245$a+$b | → | WHAT | title |
| 260$b / 264$b | → | WHO | publisher |
| 260$a / 264$a | → | WHERE | publication_place |
| 260$c / 264$c | → | WHEN | publication_date |
| 650$a | → | WHAT | subject_topic |
| 651$a | → | WHERE | subject_place |
| 600$a | → | WHO | subject_person |
| 655$a | → | WHAT | genre |
| 020$a | → | WHAT | isbn (nucleus) |

---

## Jupyter workflow

Results render as tables inline. Output is pandas.

```python
import pandas as pd
from snf_peirce import suggest, compile_data, query

df       = pd.read_csv("my_collection.csv")
draft    = suggest(df)             # renders as mapping table
compiled = compile_data(df, lens)  # renders substrate summary

query(compiled, 'WHO.artist = "Miles Davis"')  # renders result table inline

result   = query(compiled, 'WHEN.released BETWEEN "1955" AND "1965"', limit=None)
df_result = result.to_dataframe()   # pandas DataFrame — use anything
df_result.groupby("semantic_key")["value"].value_counts()
```

---

## File inventory

| File | Purpose |
|---|---|
| `parser.py` | Peirce query parser — conformant with JS reference |
| `lens.py` | Lens authoring: `suggest()`, `LensDraft`, `load()`, `save()` |
| `compile.py` | Data compilation: `compile_data()`, `Substrate` |
| `peirce.py` | Query execution: `query()`, `execute()`, `ResultSet` |
| `shell.py` | Interactive query shell |
| `guided_ingest.py` | Guided setup script — no coding required |
| `base_fetcher.py` | Base class for API fetchers |
| `fetch_scryfall.py` | Scryfall / Magic: The Gathering fetcher |
| `fetch_loc.py` | Library of Congress catalog fetcher |
| `marc_translator.py` | MARC Bibliographic Lens v1.0 |
| `parse_marc.py` | Pure Python binary MARC / MARCXML parser |

---

## Running the tests

```bash
python -m pytest test_parser.py -v    # 59 tests — parser conformance
python test_lens.py                    # 69 tests — lens authoring
python test_compile.py                 # 57 tests — compilation and queries
python test_peirce.py                  # 57 tests — end-to-end query
python test_conformance.py             # 37 tests — cross-language proof
```

---

## Architecture

```
SNF / Peirce specification   open protocol (MIT)
           ↓
     snf-peirce              Python runtime / engine  ← this package
           ↓
       Reckoner               visual application for non-technical users
```

snf-peirce is Reckoner's data engine. It is also usable standalone
for data practitioners who want SNF in Python and Jupyter workflows.

---

## Note on Portolan

The `\explain` shell command displays a simplified execution plan
showing constraints ordered by estimated cardinality. This implements
the same ordering heuristic as Portolan's I1 algorithm for display
purposes.

Full Portolan — schema validation, type checking, query rejection,
composite constraint reasoning — is a separate licensed component
not included in this package.

---

## License

MIT. See LICENSE file.

SNF specification, Peirce query language, and MARC Bibliographic
Lens v1.0 are original works. Attribution required in source code
and documentation. See project licensing documentation for details
on Portolan and Reckoner licensing.
