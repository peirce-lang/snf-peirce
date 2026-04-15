"""
guided_csv.py — Query a CSV file using plain language

No coding required. Just answer the prompts.

Requirements:
    pip install snf-peirce

Usage:
    python guided_csv.py
    python guided_csv.py mydata.csv
"""

from __future__ import annotations

import sys
from guided_base import ask, confirm, banner, section, info, success, warn, error, query_loop


def main():
    banner("SNF / Peirce  —  Query Your CSV")

    print("  This will help you map your data to meaning and query it")
    print("  in plain language. No SQL. No physical schema knowledge.")
    print("")
    print("  You map your fields to meaning once — then query forever.")
    print("")
    print("  You will need:")
    print("    • A CSV file with your data")
    print("    • About 2 minutes to map your fields")
    print("")

    # ── Get the file ──────────────────────────────────────────────────────────

    if len(sys.argv) > 1:
        path = sys.argv[1]
        info(f"Using: {path}")
    else:
        path = ask("Where is your CSV file")

    if not path:
        error("No file specified. Exiting.")
        sys.exit(1)

    # ── Load it ───────────────────────────────────────────────────────────────

    try:
        import pandas as pd
        df = pd.read_csv(path)
    except FileNotFoundError:
        error(f"Could not find: {path}")
        error("Check the path and try again.")
        sys.exit(1)
    except Exception as e:
        error(f"Could not read that file: {e}")
        sys.exit(1)

    success(f"Loaded {len(df):,} rows, {len(df.columns)} columns.")
    print("")

    # ── Show columns ──────────────────────────────────────────────────────────

    info("Your columns:")
    for col in df.columns:
        sample = df[col].dropna().iloc[0] if len(df[col].dropna()) > 0 else "(empty)"
        print(f"    {col:<30}  e.g. {sample}")
    print("")

    # ── Suggest a lens ────────────────────────────────────────────────────────

    section("Mapping your fields")

    print("  SNF organises data into six dimensions:")
    print("    WHO   — people, organisations, parties")
    print("    WHAT  — things, types, categories, topics")
    print("    WHEN  — dates, years, time periods")
    print("    WHERE — places, locations, regions")
    print("    WHY   — reasons, purposes, causes")
    print("    HOW   — methods, formats, quantities")
    print("")
    print("  Analysing your data...")
    print("")

    try:
        from snf_peirce import suggest
        draft = suggest(df)
    except Exception as e:
        error(f"Could not analyse data: {e}")
        sys.exit(1)

    print(draft)
    print("")
    print("  Each row shows a field and the dimension SNF thinks it belongs to.")
    print("  This is a suggestion — you can accept it or adjust it.")
    print("")

    if not confirm("Does this mapping look reasonable"):
        print("")
        print("  You can adjust individual mappings.")
        print("  For each field you want to change, enter the field name")
        print("  and which dimension it should go in.")
        print("")

        while True:
            field = ask("Field to remap (or press Enter to continue)")
            if not field:
                break
            dim = ask(f"  Which dimension for '{field}'? (WHO/WHAT/WHEN/WHERE/WHY/HOW)").upper()
            key = ask(f"  Semantic key for '{field}'", default=field.lower().replace(" ", "_"))
            try:
                draft.map(field, dim.lower(), key)
                success(f"Mapped {field} → {dim}.{key}")
                print(draft)
            except Exception as e:
                error(f"Could not remap: {e}")

    # ── Nucleus ───────────────────────────────────────────────────────────────

    section("Entity identity")

    print("  Every record needs a unique identifier — called the nucleus.")
    print("  This is the field that uniquely identifies each item.")
    print("  Examples: record_id, isbn, case_number, release_id, card_id")
    print("")

    nucleus_field = ask("Which field uniquely identifies each row")

    if not nucleus_field or nucleus_field not in df.columns:
        # Try to find one automatically
        candidates = [c for c in df.columns if "id" in c.lower()]
        if candidates:
            nucleus_field = candidates[0]
            info(f"Using '{nucleus_field}' as the identifier.")
        else:
            warn("Could not find a unique identifier field.")
            warn("Using row number as fallback.")
            df["_row_id"] = [f"row_{i}" for i in range(len(df))]
            nucleus_field = "_row_id"

    prefix = ask("Short prefix for entity IDs", default="data:record")

    try:
        draft.nucleus(nucleus_field, prefix=prefix)
        success(f"Nucleus set to '{nucleus_field}'")
    except Exception as e:
        error(f"Could not set nucleus: {e}")
        sys.exit(1)

    # ── Name it ───────────────────────────────────────────────────────────────

    import os
    default_name = os.path.splitext(os.path.basename(path))[0].lower().replace(" ", "_")
    lens_id = ask("Give this dataset a short name", default=default_name)

    # ── Compile ───────────────────────────────────────────────────────────────

    section("Compiling")

    print("  Building your queryable substrate...")

    try:
        from snf_peirce import compile_data
        lens     = draft.to_lens(lens_id=lens_id, authority="me")
        compiled = compile_data(df, lens)
        success(f"Done. Your data is ready to query.")
    except Exception as e:
        error(f"Compilation failed: {e}")
        info("This usually means a nucleus field has blank values.")
        info("Try a different identifier field.")
        sys.exit(1)

    # ── Show example queries ──────────────────────────────────────────────────

    print("")
    print("  Here are some queries to try:")
    print("")

    try:
        schema = compiled.schema() if hasattr(compiled, "schema") else {}
        shown  = 0
        for dim, fields in schema.items():
            if shown >= 4:
                break
            if fields:
                f = fields[0]
                key = f.split(".")[-1] if "." in str(f) else str(f)
                print(f'    {dim}.{key} = "something"')
                shown += 1
    except Exception:
        print('    WHO.name = "someone"')
        print('    WHEN.year = "2024"')

    print("")

    # ── Query loop ────────────────────────────────────────────────────────────

    query_loop(compiled, welcome="Your data is ready.")


if __name__ == "__main__":
    main()
