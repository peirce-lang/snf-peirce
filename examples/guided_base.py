"""
guided_base.py — Shared utilities for SNF guided scripts

All guided_*.py scripts import from here.
Not meant to be run directly.

pip install snf-peirce
"""

from __future__ import annotations


# ─────────────────────────────────────────────────────────────────────────────
# Simple prompt helpers
# ─────────────────────────────────────────────────────────────────────────────

def ask(prompt: str, default: str = None) -> str:
    """Ask a question. Return stripped input. Use default if blank."""
    suffix = f" [{default}]" if default else ""
    try:
        response = input(f"{prompt}{suffix}: ").strip()
    except (EOFError, KeyboardInterrupt):
        print("\nGoodbye.")
        raise SystemExit(0)
    return response if response else default


def confirm(prompt: str) -> bool:
    """Yes/no question. Returns True for yes/y."""
    try:
        response = input(f"{prompt} (yes/no): ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print("\nGoodbye.")
        raise SystemExit(0)
    return response in ("yes", "y")


def banner(title: str) -> None:
    width = max(len(title) + 6, 52)
    print("")
    print("─" * width)
    print(f"  {title}")
    print("─" * width)
    print("")


def section(title: str) -> None:
    print(f"\n  ── {title} ──\n")


def info(msg: str) -> None:
    print(f"  {msg}")


def success(msg: str) -> None:
    print(f"  ✓  {msg}")


def warn(msg: str) -> None:
    print(f"  ⚠  {msg}")


def error(msg: str) -> None:
    print(f"  ✗  {msg}")


# ─────────────────────────────────────────────────────────────────────────────
# Result display
# ─────────────────────────────────────────────────────────────────────────────

def show_result(result, wide_mode: bool = False) -> None:
    """
    Display a ResultSet in human-readable form.

    Default (wide_mode=False): record card format — one block per entity
    Wide mode (wide_mode=True): table format — one row per entity

    Falls back to entity ID list if hydration fails.
    """
    count = result.count
    if count == 0:
        print("\n  No matches found.\n")
        return

    print(f"\n  {count:,} match{'es' if count != 1 else ''} found.\n")

    try:
        df = result.to_dataframe()

        if df is None or len(df) == 0:
            raise ValueError("empty dataframe")

        # Shorten semantic_key for display — "WHO.artist" → "artist"
        df = df.copy()
        df["field"] = df["semantic_key"].apply(
            lambda k: k.split(".")[-1] if "." in str(k) else str(k)
        )

        if wide_mode:
            # ── Wide table format ─────────────────────────────────────────
            wide = df.pivot_table(
                index="entity_id",
                columns="field",
                values="value",
                aggfunc="first"
            ).reset_index()
            wide.columns.name = None
            wide = wide.rename(columns={"entity_id": "id"})

            display = wide.head(20)

            col_widths = {}
            for col in display.columns:
                max_val = display[col].astype(str).str.len().max()
                col_widths[col] = max(len(str(col)), max_val if max_val else 0)

            header  = "  " + "  ".join(str(c).ljust(col_widths[c]) for c in display.columns)
            divider = "  " + "  ".join("─" * col_widths[c] for c in display.columns)
            print(header)
            print(divider)

            for _, row in display.iterrows():
                line = "  " + "  ".join(
                    str(row[c] if row[c] is not None else "").ljust(col_widths[c])
                    for c in display.columns
                )
                print(line)

            if count > 20:
                print(f"\n  ... and {count - 20:,} more")

        else:
            # ── Record card format ────────────────────────────────────────
            entities = result.entity_ids[:20]
            for eid in entities:
                print(f"  {eid}")
                rows = df[df["entity_id"] == eid][["field", "value"]].drop_duplicates()
                for _, row in rows.iterrows():
                    print(f"    {row['field']:<20}  {row['value']}")
                print("")

            if count > 20:
                print(f"  ... and {count - 20:,} more\n")

    except Exception:
        # Fallback — entity IDs only
        for eid in result.entity_ids[:20]:
            print(f"    {eid}")
        if count > 20:
            print(f"    ... and {count - 20:,} more")
        print("")


# ─────────────────────────────────────────────────────────────────────────────
# Query loop — works with any substrate
# ─────────────────────────────────────────────────────────────────────────────

def query_loop(substrate, welcome: str = None) -> None:
    """
    Interactive Peirce query loop.
    Works with any substrate — DuckDB, Pinot, CSV, Postgres.
    The loop has no idea what's underneath.

    Commands:
        WHO.field = "value"     run a query
        \\schema                 show dimensions and fields
        \\schema WHO             show fields in a dimension
        \\explain               explain last query
        \\limit N               change result limit
        \\help                  show this help
        exit                    quit
    """
    from snf_peirce import query as peirce_query

    if welcome:
        print(f"\n  {welcome}")

    print("")
    print("  Type a query to get started. Examples:")
    print('    WHO.artist = "Miles Davis"')
    print('    WHEN.year BETWEEN "1955" AND "1965"')
    print('    WHO.artist = "Miles Davis" AND WHEN.year = "1959"')
    print("")
    print("  Explore what's available:")
    print("    *              — show all dimensions")
    print("    WHO|*          — show fields in WHO")
    print("    WHO|artist|*   — show values for WHO.artist")
    print("")
    print(r"  Type \help for all commands. Type exit to quit.")
    print("")

    limit      = 20
    last_query = None
    wide_mode  = False  # default: record card format

    while True:
        try:
            raw = input("peirce> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n\n  Goodbye.\n")
            break

        if not raw:
            continue

        q = raw

        # ── exit ──
        if q.lower() in ("exit", "quit", "q"):
            print("\n  Goodbye.\n")
            break

        # ── \help ──
        if q.lower() in (r"\help", "help", "?"):
            _show_help()
            continue

        # ── \schema ──
        if q.lower() == r"\schema":
            _show_schema(substrate)
            continue

        if q.lower().startswith(r"\schema "):
            dim = q.split(None, 1)[1].upper()
            _show_schema(substrate, dim)
            continue

        # ── \explain ──
        if q.lower() == r"\explain":
            if last_query is None:
                info("No query to explain yet.")
            else:
                try:
                    print(substrate.explain(last_query))
                except AttributeError:
                    info("This substrate does not support \\explain.")
            continue

        # ── \limit ──
        if q.lower().startswith(r"\limit"):
            parts = q.split()
            if len(parts) == 2 and parts[1].isdigit():
                limit = int(parts[1])
                info(f"Result limit set to {limit}.")
            else:
                info(r"Usage: \limit 50")
            continue

        # ── \wide ──
        if q.lower() == r"\wide":
            wide_mode = not wide_mode
            mode_name = "wide table" if wide_mode else "record card"
            info(f"Display mode: {mode_name}")
            continue

        # ── run query or discovery ──
        try:
            result     = peirce_query(substrate, q, limit=limit)
            last_query = q
            show_result(result, wide_mode=wide_mode)

        except Exception as e:
            # Discovery expression — WHO|*, *, WHO|artist|*
            if "PeirceDiscoveryError" in type(e).__name__ or type(e).__name__ == "PeirceDiscoveryError":
                _run_discovery_display(substrate, e, q)

            # Parse error — bad syntax
            elif "PeirceParseError" in type(e).__name__:
                print(f"\n  Couldn't understand that query.")
                print(f"  {e}")
                print(f'  Try: WHO.fieldname = "value"')
                print(f'  Or:  WHO|*  to see available fields')
                print("")

            # Anything else
            else:
                print(f"\n  Error: {e}")
                print("")


def _run_discovery_display(substrate, error, expression: str) -> None:
    """
    Handle a discovery expression typed at the prompt.
    Routes WHO|*, *, WHO|artist|* to appropriate display.
    """
    try:
        from snf_peirce.peirce import discover
        result = discover(substrate, expression, limit=None)
        print("")
        print(repr(result))
        print("")
        return
    except Exception:
        pass

    # Fallback — use schema display if discover() not available
    scope     = getattr(error, "scope",     None)
    dimension = getattr(error, "dimension", None)
    field     = getattr(error, "field",     None)

    print("")
    if scope == "all":
        _show_schema(substrate)
    elif scope == "dimension" and dimension:
        _show_schema(substrate, dimension)
    elif scope == "field" and dimension and field:
        # Best effort — show field in schema
        _show_schema(substrate, dimension)
    else:
        _show_schema(substrate)


def _show_help() -> None:
    print("")
    print("  ── Query syntax ──────────────────────────────────────────")
    print('    WHO.artist = "Miles Davis"          equality')
    print('    WHEN.year != "1960"                 not equal')
    print('    WHEN.year BETWEEN "1955" AND "1965" range')
    print('    WHAT.title CONTAINS "Blue"          text match')
    print('    WHO.artist PREFIX "Miles"           starts with')
    print('    NOT WHERE.office = "Seattle"        negation')
    print('    WHO.x = "a" AND WHEN.y = "b"        AND (narrows)')
    print('    WHO.x = "a" OR WHO.x = "b"          OR (widens)')
    print("")
    print("  ── Discovery ─────────────────────────────────────────────")
    print("    *                        all dimensions with counts")
    print("    WHO|*                    all fields in WHO")
    print("    WHO|artist|*             all values for WHO.artist")
    print(r"    \schema                  same as *")
    print(r"    \schema WHO              same as WHO|*")
    print("")
    print("  ── Commands ──────────────────────────────────────────────")
    print(r"    \explain                 how last query was executed")
    print(r"    \limit 50                show up to 50 results")
    print(r"    \wide                    toggle wide table / record card view")
    print(r"    \help                    this screen")
    print("    exit                     quit")
    print("")


def _show_schema(substrate, dimension: str = None) -> None:
    try:
        schema = substrate.schema() if hasattr(substrate, "schema") else {}
    except Exception:
        info("Schema not available for this substrate.")
        return

    if not schema:
        info("No schema information available.")
        return

    print("")
    if dimension:
        fields = schema.get(dimension, [])
        if not fields:
            info(f"No fields found for {dimension}.")
        else:
            print(f"  {dimension} fields:")
            for f in fields:
                print(f"    {f}")
    else:
        print("  Available dimensions:")
        for dim, fields in schema.items():
            if fields:
                sample = ", ".join(str(f) for f in fields[:4])
                more   = f"  (+{len(fields)-4} more)" if len(fields) > 4 else ""
                print(f"    {dim:<8}  {sample}{more}")
    print("")
