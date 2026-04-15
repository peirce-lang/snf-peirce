# SNF / Peirce — Guided Query Scripts

Map your data to meaning once. Query it in plain language forever.
No SQL. No physical schema knowledge. No joins.

The lens is the semantic schema. These scripts build it with you.

## Install

```bash
pip install snf-peirce
```

## Scripts

### guided_csv.py — Query a CSV file

The universal entry point. Give it any CSV and it walks you through
mapping your fields and drops you into a query shell.

```bash
python guided_csv.py
python guided_csv.py mydata.csv
```

Works for: record collections, legal matter lists, museum catalogs,
book collections, research datasets, spreadsheet exports — anything
that lives in a CSV.

---

### guided_api.py — Fetch from an API and query

Fetches data from a supported online source and lets you query it immediately.

```bash
python guided_api.py
python guided_api.py scryfall      # Magic: The Gathering cards
python guided_api.py loc           # Library of Congress catalog
python guided_api.py discogs       # Record collections
```

---

### guided_explore.py — Query data you've already compiled

For when your data is already compiled and you just want to query it.
Shortest path from "I have a substrate" to "I'm asking questions."

```bash
python guided_explore.py
python guided_explore.py csv://my_spoke_dir
python guided_explore.py duckdb://my_data.duckdb
```

---

### guided_pinot.py — Query data in Apache Pinot

Connects to a running Pinot cluster. Data must be pre-loaded
via load_into_pinot.py.

```bash
python guided_pinot.py
python guided_pinot.py http://localhost:8099
```

Requires: `pip install requests` and `pinot_substrate.py` in the same folder.

---

### guided_sql.py — Query data in Postgres or SQL Server

Connects to an existing database with SNF spoke tables already loaded.
The lens-tool `--into sql://` output creates those tables.

```bash
python guided_sql.py
```

Requires:
- Postgres: `pip install psycopg2-binary`
- SQL Server: `pip install pyodbc`

---

## Query syntax (quick reference)

```
WHO.artist = "Miles Davis"                    equality
WHEN.year != "1960"                           not equal
WHEN.year BETWEEN "1955" AND "1965"           range
WHAT.title CONTAINS "Blue"                    text match
WHO.artist PREFIX "Miles"                     starts with
NOT WHERE.office = "Seattle"                  negation
WHO.x = "a" AND WHEN.y = "b"                 AND — narrows
WHO.x = "a" OR WHO.x = "b"                   OR — widens
```

## Shell commands

```
\schema              show all dimensions and fields
\schema WHO          show fields in WHO
\explain             how last query was executed
\limit 50            show up to 50 results
\help                all commands
exit                 quit
```

## Who these are for

- **Record collectors** — your Discogs collection, queryable in plain language
- **Magic players** — every card in a set, queryable by type, color, cost
- **Librarians** — catalog records, queryable without SQL
- **Researchers** — any dataset from any API, queryable immediately
- **Legal professionals** — matter lists, queryable without writing JOINs
- **Museum enthusiasts** — collection data from museum APIs
- **Anyone** with data they understand but can't query

The code is the barrier SNF removes. These scripts remove the remaining friction.
