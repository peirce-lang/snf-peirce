"""
guided_explore.py — Query a substrate you've already compiled

For when you've already run guided_csv.py or guided_api.py
and saved your data. Just point at it and start querying.

Requirements:
    pip install snf-peirce

Usage:
    python guided_explore.py
    python guided_explore.py csv://my_spoke_dir
    python guided_explore.py duckdb://my_data.duckdb
"""

from __future__ import annotations

import sys
from guided_base import ask, confirm, banner, section, info, success, warn, error, query_loop


def load_substrate(path: str):
    """
    Load a compiled substrate from a path string.
    Supports csv:// and duckdb:// paths.
    """
    if path.startswith("csv://"):
        from snf_peirce.compile import Substrate
        return Substrate.from_csv(path.replace("csv://", ""))

    elif path.startswith("duckdb://"):
        from snf_peirce.compile import Substrate
        return Substrate.from_duckdb(path.replace("duckdb://", ""))

    else:
        # Try as a raw directory path
        import os
        if os.path.isdir(path):
            from snf_peirce.compile import Substrate
            return Substrate.from_csv(path)
        elif path.endswith(".duckdb"):
            from snf_peirce.compile import Substrate
            return Substrate.from_duckdb(path)
        else:
            raise ValueError(
                f"Don't know how to load: {path}\n"
                f"  Try: csv://my_spoke_dir  or  duckdb://my_data.duckdb"
            )


def main():
    banner("SNF / Peirce  —  Explore Your Data")

    print("  Query a dataset you've already compiled.")
    print("  Point at a spoke directory or DuckDB file and start asking questions.")
    print("")

    # ── Get the substrate path ────────────────────────────────────────────────

    if len(sys.argv) > 1:
        path = sys.argv[1]
        info(f"Loading: {path}")
    else:
        print("  Where is your compiled data?")
        print("  Examples:")
        print("    csv://my_spoke_dir        (from lens-tool or compile_data)")
        print("    duckdb://my_data.duckdb   (DuckDB substrate)")
        print("    my_spoke_dir              (directory path also works)")
        print("")
        path = ask("Path to your data")

    if not path:
        error("No path provided. Exiting.")
        sys.exit(1)

    # ── Load ──────────────────────────────────────────────────────────────────

    section("Loading")

    try:
        substrate = load_substrate(path)
        success("Loaded successfully.")
    except Exception as e:
        error(f"Could not load: {e}")
        info("Make sure the path is correct and the data has been compiled.")
        info("Run guided_csv.py first if you haven't compiled your data yet.")
        sys.exit(1)

    # ── Show what's there ─────────────────────────────────────────────────────

    print("")
    print("  What's in this dataset:")
    print("")

    try:
        schema = substrate.schema() if hasattr(substrate, "schema") else {}
        if schema:
            for dim, fields in schema.items():
                if fields:
                    sample = ", ".join(str(f) for f in fields[:3])
                    more   = f"  (+{len(fields)-3} more)" if len(fields) > 3 else ""
                    print(f"    {dim:<8}  {sample}{more}")
        else:
            info("(Schema not available — you can still query)")
    except Exception:
        info("(Could not retrieve schema — you can still query)")

    print("")

    # ── Query loop ────────────────────────────────────────────────────────────

    query_loop(substrate, welcome="Ready to query.")


if __name__ == "__main__":
    main()
