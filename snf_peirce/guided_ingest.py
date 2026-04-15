"""
guided_ingest.py — Guided SNF Setup

Walks a user through the full workflow from CSV to query shell:
  1. Load a CSV file
  2. Review and adjust field mappings
  3. Declare a nucleus
  4. Compile to SNF substrate
  5. Open the query shell

No coding required. Just run:
    python guided_ingest.py

Or with a CSV path already known:
    python guided_ingest.py mydata.csv
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


# ─────────────────────────────────────────────────────────────────────────────
# Terminal helpers — plain, no colour dependencies
# ─────────────────────────────────────────────────────────────────────────────

def _print_header(text):
    print()
    print("  " + "─" * 50)
    print(f"  {text}")
    print("  " + "─" * 50)
    print()

def _print_step(n, text):
    print()
    print(f"  [{n}] {text}")
    print()

def _ask(prompt, default=None):
    """Prompt for input with an optional default."""
    if default:
        full_prompt = f"  {prompt} [{default}]: "
    else:
        full_prompt = f"  {prompt}: "
    answer = input(full_prompt).strip()
    if not answer and default:
        return default
    return answer

def _ask_yes_no(prompt, default="y"):
    answer = input(f"  {prompt} [{'Y/n' if default == 'y' else 'y/N'}]: ").strip().lower()
    if not answer:
        return default == "y"
    return answer.startswith("y")

def _clear():
    os.system("cls" if os.name == "nt" else "clear")


# ─────────────────────────────────────────────────────────────────────────────
# Display helpers
# ─────────────────────────────────────────────────────────────────────────────

VALID_DIMS = ("WHO", "WHAT", "WHEN", "WHERE", "WHY", "HOW")

def _print_draft_table(draft):
    """Print the current lens draft as a plain-text table."""
    nucleus = draft._nucleus

    # Work out nucleus columns for marking
    nuc_cols = set()
    if nucleus:
        if nucleus["type"] == "single":
            nuc_cols = {nucleus["field"]}
        else:
            nuc_cols = set(nucleus.get("fields", []))

    # Header
    print(f"  {'Column':<30}  {'Dimension':<10}  {'Semantic Key':<25}  {'Confidence'}")
    print(f"  {'─' * 30}  {'─' * 10}  {'─' * 25}  {'─' * 10}")

    for col in draft.columns():
        row  = draft.get(col)
        dim  = row.get("dimension") or "—"
        key  = row.get("semantic_key") or "—"
        conf = row.get("confidence") or "—"
        nuc_marker = "  ← nucleus" if col in nuc_cols else ""
        print(f"  {col:<30}  {dim.upper():<10}  {key:<25}  {conf}{nuc_marker}")

    print()

    if nucleus:
        if nucleus["type"] == "single":
            prefix = f" (prefix: {nucleus['prefix']})" if nucleus.get("prefix") else ""
            print(f"  Nucleus: {nucleus['field']}{prefix}")
        else:
            fields = " + ".join(nucleus["fields"])
            sep    = nucleus.get("separator", "-")
            prefix = f" (prefix: {nucleus['prefix']})" if nucleus.get("prefix") else ""
            print(f"  Nucleus (composite): {fields}  separator: '{sep}'{prefix}")
    else:
        print("  Nucleus: not declared yet")
    print()


def _print_dim_guide():
    print("  Dimensions:")
    print("    WHO   — people, organisations, roles")
    print("    WHAT  — things, topics, identifiers, categories")
    print("    WHEN  — dates, years, time periods")
    print("    WHERE — places, locations, offices, regions")
    print("    WHY   — reasons, types, purposes, categories")
    print("    HOW   — formats, methods, media, protocols")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# Step handlers
# ─────────────────────────────────────────────────────────────────────────────

def step_load_csv(csv_arg=None):
    """Step 1 — load a CSV file."""
    _print_step(1, "Load your CSV file")

    while True:
        if csv_arg:
            path_str = csv_arg
            csv_arg  = None  # only use arg on first pass
        else:
            path_str = _ask("Path to your CSV file")

        if not path_str:
            print("  Please enter a file path.")
            continue

        path = Path(path_str.strip('"').strip("'"))
        if not path.exists():
            print(f"  File not found: {path}")
            print("  Please check the path and try again.")
            continue

        try:
            import pandas as pd
            df = pd.read_csv(path)
            print(f"  Loaded {len(df):,} rows and {len(df.columns)} columns.")
            print()
            print("  Columns found:")
            for col in df.columns:
                sample = df[col].dropna().head(3).tolist()
                sample_str = ", ".join(str(s) for s in sample)
                print(f"    {col:<30}  e.g. {sample_str}")
            print()
            return df, path
        except Exception as e:
            print(f"  Could not read file: {e}")
            continue


def step_suggest(df):
    """Step 2 — run suggest() and show the draft."""
    _print_step(2, "Analyse your data")
    print("  Analysing column names and data types...")
    print()

    from snf_peirce.lens import suggest
    draft = suggest(df)

    print("  Here is what was inferred. Review each column below.")
    print()
    _print_dim_guide()
    _print_draft_table(draft)

    return draft


def step_review_mappings(draft, df):
    """Step 3 — let the user review and correct mappings."""
    _print_step(3, "Review and adjust mappings")

    print("  For each column you can:")
    print("    Press Enter     — keep the suggestion as-is")
    print("    Type a dim      — change dimension (WHO / WHAT / WHEN / WHERE / WHY / HOW)")
    print("    Type SKIP       — exclude this column from the lens")
    print("    Type RENAME     — change the semantic key")
    print()

    skipped = set()

    for col in draft.columns():
        row  = draft.get(col)
        dim  = (row.get("dimension") or "?").upper()
        key  = row.get("semantic_key") or "?"
        conf = row.get("confidence") or "?"

        print(f"  {col}")
        print(f"    Current: {dim} / {key}  (confidence: {conf})")

        answer = input("    Action [Enter/WHO/WHAT/WHEN/WHERE/WHY/HOW/SKIP/RENAME]: ").strip().upper()

        if not answer:
            # Keep suggestion
            pass

        elif answer == "SKIP":
            skipped.add(col)
            print(f"    Skipped.")

        elif answer == "RENAME":
            new_key = input(f"    New semantic key for {col}: ").strip().lower()
            if new_key:
                draft.map(col, row.get("dimension") or "what", new_key)
                print(f"    Renamed to: {dim} / {new_key}")

        elif answer in VALID_DIMS:
            new_dim = answer.lower()
            # Ask for semantic key — default to current
            new_key = input(f"    Semantic key [{key}]: ").strip().lower()
            if not new_key:
                new_key = key
            draft.map(col, new_dim, new_key)
            print(f"    Updated to: {answer} / {new_key}")

        else:
            print(f"    Unrecognised input — keeping current mapping.")

        print()

    # Remove skipped columns from the coordinate map by setting dimension to None
    # We do this by rebuilding with only non-skipped columns
    if skipped:
        print(f"  Skipping {len(skipped)} column(s): {', '.join(skipped)}")
        for col in skipped:
            # Map skipped columns to a placeholder that compile_data will ignore
            # by removing them from the lens coordinate_map at to_lens() time
            # Simplest approach: just don't include them — we handle this at lens build time
            pass

    print()
    print("  Current mappings:")
    _print_draft_table(draft)

    if _ask_yes_no("Make any more changes?", default="n"):
        return step_review_mappings(draft, df)

    return draft, skipped


def step_nucleus(draft, df):
    """Step 4 — declare the nucleus."""
    _print_step(4, "Declare the nucleus")

    print("  The nucleus is the unique identifier for each entity.")
    print("  It is the column (or combination of columns) that makes each row unique.")
    print("  Every other fact in the row is attached to this identity.")
    print()
    print("  Examples:")
    print("    Single:    release_id, isbn, employee_id, case_number")
    print("    Composite: client_id + matter_id (neither alone is unique)")
    print()

    # Show columns with uniqueness stats
    print("  Column uniqueness (higher = better nucleus candidate):")
    import pandas as pd
    for col in df.columns:
        n_unique = df[col].nunique()
        n_total  = df[col].count()
        pct      = f"{100 * n_unique / n_total:.0f}%" if n_total > 0 else "?"
        marker   = "  ← good candidate" if n_unique / max(n_total, 1) > 0.95 else ""
        print(f"    {col:<30}  {n_unique:>6} unique / {n_total} rows  ({pct}){marker}")
    print()

    # Single or composite?
    nuc_type = _ask("Single column or composite? [single/composite]", default="single").lower()

    if nuc_type.startswith("c"):
        # Composite
        print()
        print("  Enter the column names that together form the unique ID.")
        fields = []
        while True:
            col = _ask(f"  Column {len(fields) + 1} (Enter when done)").strip()
            if not col:
                if len(fields) >= 2:
                    break
                print("  Need at least 2 columns for a composite nucleus.")
                continue
            if col not in df.columns:
                print(f"  Column '{col}' not found. Available: {list(df.columns)}")
                continue
            fields.append(col)

        separator = _ask("Separator between values", default="-")
        prefix    = _ask("Entity ID prefix (e.g. 'legal:matter', or Enter to skip)", default="")
        draft.nucleus_composite(fields, separator=separator, prefix=prefix or None)
        print(f"  Composite nucleus: {' + '.join(fields)}  →  {prefix + ':' if prefix else ''}{fields[0]}{separator}{fields[1]}...")

    else:
        # Single
        while True:
            col = _ask("Which column is the unique ID?").strip()
            if col in df.columns:
                break
            print(f"  Column '{col}' not found. Available: {list(df.columns)}")

        prefix = _ask("Entity ID prefix (e.g. 'discogs:release', or Enter to skip)", default="")
        draft.nucleus(col, prefix=prefix or None)
        print(f"  Nucleus: {col}  →  {prefix + ':' if prefix else ''}<value>")

    return draft


def step_name_lens(draft):
    """Step 5 — name the lens."""
    _print_step(5, "Name your lens")

    print("  A lens ID is a short identifier for this dataset's interpretation.")
    print("  Examples: discogs_v1, legal_matters_2024, library_catalog_v2")
    print()

    lens_id   = _ask("Lens ID").strip().lower().replace(" ", "_")
    authority = _ask("Your name or organisation (for attribution)").strip()

    # Optional metadata
    print()
    print("  Optional — press Enter to skip any of these:")
    domain        = _ask("Domain (e.g. music_collection, legal_billing)", default="").strip()
    source_format = _ask("Source format (e.g. csv_export, database_dump)", default="").strip()

    return lens_id, authority, domain, source_format


def step_compile(draft, df, lens_id, authority, domain, source_format, skipped):
    """Step 6 — build the lens and compile."""
    _print_step(6, "Compile your data")

    # Build the lens — exclude skipped columns
    from snf_peirce.lens import save as save_lens

    # Remove skipped columns from coordinate_map before building lens
    if skipped:
        for col in skipped:
            if col in draft._rows:
                draft._rows[col]["dimension"]    = None
                draft._rows[col]["semantic_key"] = None

    lens = draft.to_lens(
        lens_id       = lens_id,
        authority     = authority,
        domain        = domain        or lens_id,
        source_format = source_format or "csv",
    )

    # Save the lens JSON
    lens_path = Path(f"{lens_id}.json")
    save_lens(lens, lens_path)
    print(f"  Lens saved to: {lens_path}")

    # Compile
    spoke_dir = f"{lens_id}_spoke"
    print(f"  Compiling data...")

    from snf_peirce.compile import compile_data
    compiled = compile_data(df, lens, into=f"csv://{spoke_dir}")

    print(f"  Done.")
    print()
    print(f"  Entities compiled:  {compiled.entity_count():,}")
    print(f"  Facts compiled:     {compiled.count():,}")
    print(f"  Dimensions:         {', '.join(compiled.dimensions())}")
    print(f"  Spoke directory:    {spoke_dir}/")
    print(f"  Lens file:          {lens_id}.json")
    print()

    return compiled, spoke_dir


def step_open_shell(compiled, spoke_dir):
    """Step 7 — optionally open the query shell."""
    _print_step(7, "Query your data")

    print("  Your data is ready to query.")
    print()
    print("  Example queries:")
    dims = compiled.dimensions()
    if "who" in dims:
        print('    WHO.field = "value"')
    if "when" in dims:
        print('    WHEN.field BETWEEN "2020" AND "2024"')
    if "what" in dims:
        print('    WHAT.field CONTAINS "text"')
    print()
    print("  Type \\help in the shell for all commands.")
    print("  Type exit to quit the shell.")
    print()

    if _ask_yes_no("Open query shell now?", default="y"):
        from shell import run_shell
        run_shell(f"csv://{spoke_dir}")
    else:
        print()
        print("  To open the shell later, run:")
        print(f"    python shell.py csv://{spoke_dir}")
        print()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    # Get CSV path from command line arg if provided
    csv_arg = sys.argv[1] if len(sys.argv) > 1 else None

    _print_header("SNF Guided Setup  —  CSV → Query Shell")

    print("  This tool will walk you through:")
    print("    1. Loading your CSV")
    print("    2. Mapping fields to semantic dimensions")
    print("    3. Declaring your entity identifier (nucleus)")
    print("    4. Compiling your data into a queryable substrate")
    print("    5. Opening the query shell")
    print()
    print("  No coding required. Press Enter to accept suggestions.")
    print("  Press Ctrl+C at any time to exit.")
    print()

    try:
        # Step 1 — load CSV
        df, csv_path = step_load_csv(csv_arg)

        # Step 2 — suggest mappings
        draft = step_suggest(df)

        # Step 3 — review mappings
        draft, skipped = step_review_mappings(draft, df)

        # Step 4 — nucleus
        draft = step_nucleus(draft, df)

        # Step 5 — name the lens
        lens_id, authority, domain, source_format = step_name_lens(draft)

        # Step 6 — compile
        compiled, spoke_dir = step_compile(
            draft, df, lens_id, authority, domain, source_format, skipped
        )

        # Step 7 — open shell
        step_open_shell(compiled, spoke_dir)

    except KeyboardInterrupt:
        print()
        print()
        print("  Exited.")
        print()
        sys.exit(0)


if __name__ == "__main__":
    main()
