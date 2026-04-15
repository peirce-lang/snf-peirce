"""
load_into_pinot.py — SNF Pinot Loader

Takes a compiled SNF substrate (DuckDB or CSV spoke directory) and loads
the spoke rows into Apache Pinot via its batch ingest REST API.

This is a one-time setup utility, not part of the query path.
PinotSubstrate is read-only — data must be loaded before querying.

Usage
-----
From Python:

    from load_into_pinot import load_into_pinot
    from compile import compile_data
    from lens import load
    import pandas as pd

    lens     = load("my_lens.json")
    df       = pd.read_csv("my_data.csv")
    compiled = compile_data(df, lens)           # DuckDB substrate

    load_into_pinot(compiled, broker_url="http://localhost:8099")

From CLI (CSV spoke directory):

    python load_into_pinot.py \
        --from csv://my_spoke_dir \
        --broker http://localhost:8099

Or from a lens + CSV directly:

    python load_into_pinot.py \
        --input my_data.csv \
        --lens my_lens.json \
        --broker http://localhost:8099

What this does
--------------
1. Reads spoke rows from DuckDB substrate or CSV spoke directory
2. Creates Pinot tables (snf_who, snf_what, etc.) if they don't exist
3. Pushes rows via Pinot's /ingestFromFile or inline batch ingest endpoint

Pinot table schema (per dimension table)
-----------------------------------------
    entity_id     STRING
    semantic_key  STRING    (e.g. "WHO.attorney=Smith")
    coordinate    STRING    (same as semantic_key in v1)
    lens_id       STRING

Indexes applied automatically:
    - Inverted index on semantic_key  ← the critical one for SNF routing
    - Inverted index on entity_id     ← needed for IN clause filtering

Dependencies
------------
    requests
    pandas
    duckdb     (if loading from a compiled DuckDB substrate)

No Pinot SDK required — REST API only.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

import requests

# ---------------------------------------------------------------------------
# Pinot table schema template
# ---------------------------------------------------------------------------

def _table_config(table_name: str) -> dict:
    """
    Pinot table configuration for an SNF spoke table.

    OFFLINE table type — batch ingest, not streaming.
    Inverted indexes on semantic_key and entity_id.
    """
    return {
        "tableName": table_name,
        "tableType": "OFFLINE",
        "segmentsConfig": {
            "replication": "1",
            "schemaName": table_name,
        },
        "tableIndexConfig": {
            "invertedIndexColumns": ["semantic_key", "entity_id"],
            "noDictionaryColumns": [],
            "sortedColumn": ["semantic_key"],
        },
        "tenants": {
            "broker": "DefaultTenant",
            "server": "DefaultTenant",
        },
        "metadata": {
            "customConfigs": {
                "snf.schema": "v1",
                "snf.dimension": table_name.split("_")[-1].upper(),
            }
        },
    }


def _schema_config(table_name: str) -> dict:
    """
    Pinot schema definition for an SNF spoke table.
    """
    return {
        "schemaName": table_name,
        "dimensionFieldSpecs": [
            {"name": "entity_id",    "dataType": "STRING"},
            {"name": "semantic_key", "dataType": "STRING"},
            {"name": "coordinate",   "dataType": "STRING"},
            {"name": "lens_id",      "dataType": "STRING"},
        ],
        "metricFieldSpecs": [],
        "dateTimeFieldSpecs": [],
    }


# ---------------------------------------------------------------------------
# Dimension → table name
# ---------------------------------------------------------------------------

DIMENSIONS = ["WHO", "WHAT", "WHEN", "WHERE", "WHY", "HOW"]

def _table_for(dim: str, prefix: str = "snf") -> str:
    return f"{prefix}_{dim.lower()}"


# ---------------------------------------------------------------------------
# Pinot REST helpers
# ---------------------------------------------------------------------------

class PinotAdmin:
    """Thin wrapper around Pinot controller REST API."""

    def __init__(self, controller_url: str, timeout: float = 30.0):
        self.controller_url = controller_url.rstrip("/")
        self.timeout = timeout

    def table_exists(self, table_name: str) -> bool:
        url = f"{self.controller_url}/tables/{table_name}"
        resp = requests.get(url, timeout=self.timeout)
        return resp.status_code == 200

    def create_schema(self, schema: dict) -> None:
        url = f"{self.controller_url}/schemas"
        resp = requests.post(url, json=schema, timeout=self.timeout)
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"Failed to create schema {schema['schemaName']}: "
                f"{resp.status_code} {resp.text}"
            )

    def create_table(self, table_config: dict) -> None:
        url = f"{self.controller_url}/tables"
        resp = requests.post(url, json=table_config, timeout=self.timeout)
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"Failed to create table {table_config['tableName']}: "
                f"{resp.status_code} {resp.text}"
            )

    def ingest_json_records(self, table_name: str, records: List[dict]) -> dict:
        """
        Inline batch ingest via /ingestFromFile endpoint (Pinot 0.12+).
        For smaller datasets (< ~500K rows). For larger, use segment files.

        Pinot does not have a true inline JSON array ingest on the broker.
        We use the controller's segment generation from inline data if available,
        otherwise fall back to writing a temp JSON file and using the file ingest API.
        """
        # Write to a temp JSONL file, then use the batch ingest endpoint
        import tempfile
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".jsonl", delete=False
        ) as f:
            for record in records:
                f.write(json.dumps(record) + "\n")
            tmp_path = f.name

        try:
            result = self._ingest_from_jsonl(table_name, tmp_path)
        finally:
            os.unlink(tmp_path)

        return result

    def _ingest_from_jsonl(self, table_name: str, jsonl_path: str) -> dict:
        """
        Use Pinot's /ingestFromFile endpoint.
        Requires Pinot 0.10+ with the ingestion job API enabled.
        """
        url = (
            f"{self.controller_url}/ingestFromFile"
            f"?tableNameWithType={table_name}_OFFLINE"
            f"&batchConfigMapStr="
            + requests.utils.quote(
                json.dumps({
                    "inputFormat": "json",
                    "recordReader.prop.delimiter": "",
                })
            )
        )
        with open(jsonl_path, "rb") as f:
            resp = requests.post(
                url,
                files={"file": (os.path.basename(jsonl_path), f, "application/json")},
                timeout=300,  # ingestion can be slow
            )
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"Ingest failed for {table_name}: {resp.status_code} {resp.text}"
            )
        return resp.json()


# ---------------------------------------------------------------------------
# Spoke row readers
# ---------------------------------------------------------------------------

def _read_from_duckdb_substrate(substrate) -> Dict[str, List[dict]]:
    """
    Read spoke rows from a compiled DuckDB Substrate object.

    Returns dict: dimension → list of row dicts
    Each row: {entity_id, semantic_key, coordinate, lens_id}
    """
    rows_by_dim: Dict[str, List[dict]] = {dim: [] for dim in DIMENSIONS}

    conn = substrate._conn  # DuckDB connection inside the Substrate object

    for dim in DIMENSIONS:
        table = f"snf_{dim.lower()}"
        try:
            result = conn.execute(
                f"SELECT entity_id, semantic_key, coordinate, lens_id "
                f"FROM {table}"
            ).fetchall()
            for row in result:
                rows_by_dim[dim].append({
                    "entity_id":    str(row[0]),
                    "semantic_key": str(row[1]),
                    "coordinate":   str(row[2]),
                    "lens_id":      str(row[3]) if row[3] else "",
                })
        except Exception:
            pass  # dimension table may not exist if no data for that dim

    return rows_by_dim


def _read_from_csv_dir(csv_dir: str) -> Dict[str, List[dict]]:
    """
    Read spoke rows from a CSV spoke directory (csv:// output from compile_data).

    Expected file layout:
        my_spoke_dir/snf_who.csv
        my_spoke_dir/snf_what.csv
        ... etc.

    Column names must include: entity_id, semantic_key, coordinate, lens_id
    """
    import csv

    path = csv_dir.replace("csv://", "")
    rows_by_dim: Dict[str, List[dict]] = {dim: [] for dim in DIMENSIONS}

    for dim in DIMENSIONS:
        csv_path = Path(path) / f"snf_{dim.lower()}.csv"
        if not csv_path.exists():
            continue
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows_by_dim[dim].append({
                    "entity_id":    row.get("entity_id", ""),
                    "semantic_key": row.get("semantic_key", ""),
                    "coordinate":   row.get("coordinate", row.get("semantic_key", "")),
                    "lens_id":      row.get("lens_id", ""),
                })

    return rows_by_dim


# ---------------------------------------------------------------------------
# Main loader
# ---------------------------------------------------------------------------

def load_into_pinot(
    source,
    broker_url: str,
    controller_url: Optional[str] = None,
    table_prefix: str = "snf",
    create_tables: bool = True,
    batch_size: int = 10_000,
    verbose: bool = True,
) -> Dict[str, int]:
    """
    Load SNF spoke rows into Pinot.

    Parameters
    ----------
    source : Substrate | str
        Either a compiled DuckDB Substrate object, or a CSV directory
        path string like "csv://my_spoke_dir".
    broker_url : str
        Pinot broker URL, e.g. "http://localhost:8099"
        Used for verification queries after load.
    controller_url : str, optional
        Pinot controller URL, e.g. "http://localhost:9000"
        Defaults to broker_url with port 9000.
    table_prefix : str
        Table name prefix. Default "snf".
    create_tables : bool
        If True, create tables and schemas if they don't exist.
    batch_size : int
        Rows per ingest batch. Default 10,000.
    verbose : bool
        Print progress.

    Returns
    -------
    dict
        {dimension: row_count} for each dimension loaded.
    """
    if controller_url is None:
        # Derive controller URL from broker URL: replace port 8099 → 9000
        controller_url = broker_url.replace(":8099", ":9000")
        if controller_url == broker_url:
            # Port wasn't 8099 — append :9000 as best guess
            from urllib.parse import urlparse, urlunparse
            parsed = urlparse(broker_url)
            controller_url = urlunparse(parsed._replace(netloc=f"{parsed.hostname}:9000"))

    admin = PinotAdmin(controller_url)

    # Read source rows
    if verbose:
        print(f"[loader] Reading source data...")

    if isinstance(source, str) and source.startswith("csv://"):
        rows_by_dim = _read_from_csv_dir(source)
        source_label = source
    else:
        # Assume DuckDB Substrate object
        rows_by_dim = _read_from_duckdb_substrate(source)
        source_label = "DuckDB substrate"

    total = sum(len(v) for v in rows_by_dim.values())
    if verbose:
        print(f"[loader] Source: {source_label}")
        print(f"[loader] Total spoke rows: {total:,}")
        for dim in DIMENSIONS:
            count = len(rows_by_dim[dim])
            if count:
                print(f"[loader]   {dim}: {count:,} rows")

    if total == 0:
        print("[loader] No rows found. Nothing to load.")
        return {}

    # Create tables if needed
    counts: Dict[str, int] = {}

    for dim in DIMENSIONS:
        rows = rows_by_dim[dim]
        if not rows:
            continue

        table_name = _table_for(dim, table_prefix)

        if create_tables and not admin.table_exists(table_name):
            if verbose:
                print(f"[loader] Creating schema + table: {table_name}")
            admin.create_schema(_schema_config(table_name))
            admin.create_table(_table_config(table_name))
            time.sleep(0.5)  # brief pause for Pinot to register the table

        # Ingest in batches
        if verbose:
            print(f"[loader] Ingesting {len(rows):,} rows → {table_name}...", end="", flush=True)

        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            admin.ingest_json_records(table_name, batch)
            if verbose:
                print(".", end="", flush=True)

        if verbose:
            print(f" done ({len(rows):,})")

        counts[dim] = len(rows)

    if verbose:
        print(f"\n[loader] Load complete.")
        print(f"[loader] Verify with: PinotSubstrate('{broker_url}').ping()")

    return counts


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _cli():
    parser = argparse.ArgumentParser(
        description="Load SNF spoke rows into Apache Pinot"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--from",
        dest="source",
        help="CSV spoke directory, e.g. csv://my_spoke_dir",
    )
    group.add_argument(
        "--input",
        dest="input_csv",
        help="Source CSV file (requires --lens)",
    )
    parser.add_argument(
        "--lens",
        dest="lens_path",
        help="Lens JSON file (required with --input)",
    )
    parser.add_argument(
        "--broker",
        dest="broker_url",
        default="http://localhost:8099",
        help="Pinot broker URL (default: http://localhost:8099)",
    )
    parser.add_argument(
        "--controller",
        dest="controller_url",
        default=None,
        help="Pinot controller URL (default: broker host + port 9000)",
    )
    parser.add_argument(
        "--prefix",
        dest="table_prefix",
        default="snf",
        help="Table name prefix (default: snf)",
    )
    parser.add_argument(
        "--batch-size",
        dest="batch_size",
        type=int,
        default=10_000,
        help="Rows per ingest batch (default: 10000)",
    )

    args = parser.parse_args()

    if args.input_csv:
        if not args.lens_path:
            print("ERROR: --lens is required with --input", file=sys.stderr)
            sys.exit(1)

        # Import compile pipeline
        try:
            import pandas as pd
            from compile import compile_data
            from lens import load as load_lens
        except ImportError as exc:
            print(f"ERROR: {exc}. Run from the snf-peirce package directory.", file=sys.stderr)
            sys.exit(1)

        print(f"[loader] Compiling {args.input_csv} with lens {args.lens_path}...")
        lens     = load_lens(args.lens_path)
        df       = pd.read_csv(args.input_csv)
        compiled = compile_data(df, lens)
        source   = compiled

    else:
        source = args.source

    load_into_pinot(
        source=source,
        broker_url=args.broker_url,
        controller_url=args.controller_url,
        table_prefix=args.table_prefix,
        batch_size=args.batch_size,
        verbose=True,
    )


if __name__ == "__main__":
    _cli()
