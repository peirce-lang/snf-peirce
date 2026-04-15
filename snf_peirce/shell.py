"""
shell.py — Peirce Interactive Shell

An interactive query shell for SNF substrates.
Thin wrapper over everything already built — parser, compile, peirce.

Usage
-----
From CLI (after packaging):
    peirce shell csv://my_spoke_dir
    peirce shell csv://my_spoke_dir --lens-id discogs_community_v1
    peirce shell csv://my_spoke_dir --limit 50

As a module (development):
    python -m shell csv://my_spoke_dir

Programmatically:
    from shell import PeirceShell
    sh = PeirceShell(substrate)
    sh.run()

Shell commands
--------------
    WHO.artist = "Miles Davis"        → run a Peirce query
    \\schema                           → show dimensions and fields
    \\schema WHO                       → show fields in WHO dimension
    \explain                          → explain the last query's execution plan
    \pivot                            → toggle pivot/vertical display mode
    \limit N                          → set result limit (0 = unlimited)
    \history                          → show query history
    \clear                            → clear the screen
    \help                             → show help
    exit / quit / \q                  → exit

TAB completion
--------------
    WHO.<TAB>               → completes field names from substrate
    WHAT.<TAB>              → completes field names from substrate
    \<TAB>                  → completes shell commands
"""

from __future__ import annotations

import os
import sys
import time
import textwrap
from pathlib import Path
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# Terminal helpers
# ─────────────────────────────────────────────────────────────────────────────

# Detect colour support
_USE_COLOUR = sys.stdout.isatty() and os.environ.get("NO_COLOR") is None

def _c(text, code):
    if not _USE_COLOUR:
        return text
    return f"\033[{code}m{text}\033[0m"

def _bold(t):    return _c(t, "1")
def _dim(t):     return _c(t, "2")
def _green(t):   return _c(t, "32")
def _yellow(t):  return _c(t, "33")
def _cyan(t):    return _c(t, "36")
def _red(t):     return _c(t, "31")

_RULE = _dim("  " + "─" * 52)


# ─────────────────────────────────────────────────────────────────────────────
# Result display
# ─────────────────────────────────────────────────────────────────────────────

def _display_results(result, substrate, pivot_mode=False):
    """
    Render a ResultSet to the terminal.

    vertical mode (default) — matches JS peirce> output format
    pivot mode              — wide table, one row per entity
    """
    count = result.count
    count_str = f"  {_bold(str(count))} result{'s' if count != 1 else ''}"
    print(count_str)
    print()

    if count == 0:
        return

    if pivot_mode:
        _display_pivot(result)
    else:
        _display_vertical(result, substrate)


def _display_vertical(result, substrate):
    """JS-style vertical fact display."""
    df = result.to_dataframe()
    if df.empty:
        return

    for entity_id in result.entity_ids:
        print(_RULE)
        print(f"  {_cyan(entity_id)}")
        print()

        entity_rows = df[df["entity_id"] == entity_id].sort_values(
            ["dimension", "semantic_key"]
        )

        for _, row in entity_rows.iterrows():
            dim = row["dimension"].upper()
            key = row["semantic_key"]
            val = row["value"]
            print(f"    {_yellow(f'{dim:<6}')}  {key:<28}  {val}")

        print()

    print(_RULE)
    print()


def _display_pivot(result):
    """Wide pivot table — one row per entity."""
    pv = result.pivot()
    if pv.empty:
        return

    # Simple fixed-width table
    cols = list(pv.columns)
    col_widths = {c: max(len(c), pv[c].astype(str).str.len().max()) for c in cols}
    col_widths = {c: min(w, 40) for c, w in col_widths.items()}  # cap at 40

    header = "  " + "  ".join(f"{c:<{col_widths[c]}}" for c in cols)
    rule   = "  " + "  ".join("-" * col_widths[c] for c in cols)
    print(_bold(header))
    print(_dim(rule))

    for _, row in pv.iterrows():
        line = "  " + "  ".join(
            f"{str(row[c])[:col_widths[c]]:<{col_widths[c]}}" for c in cols
        )
        print(line)
    print()


# ─────────────────────────────────────────────────────────────────────────────
# Explain — execution plan display
# ─────────────────────────────────────────────────────────────────────────────

def _display_explain(parsed, substrate):
    """
    Display a simplified execution plan for the last query.

    Shows constraints ordered by estimated cardinality (ascending) —
    the same ordering heuristic used by Portolan's I1 algorithm.

    NOTE: This is a display-only cardinality estimation, not full
    Portolan planning. Full Portolan includes schema validation, type
    checking, query rejection, and composite constraint reasoning.
    Those capabilities are a separate licensed component.
    See: https://github.com/peirce-lang for Portolan licensing details.
    """
    if not parsed or parsed.get("type") != "query":
        print("  No query to explain.\n")
        return

    conjuncts = parsed.get("conjuncts", [])

    for ci, conjunct in enumerate(conjuncts):
        if len(conjuncts) > 1:
            print(f"  {_bold(f'Conjunct {ci + 1}')}")

        # Estimate cardinality for each constraint
        estimates = []
        for c in conjunct:
            dim   = (c.get("category") or c.get("dimension") or "").lower()
            key   = (c.get("field") or "").lower()
            op    = c.get("op", "eq")
            value = c.get("value")

            try:
                if op == "eq":
                    row = substrate._conn.execute(
                        "SELECT COUNT(DISTINCT entity_id) FROM snf_spoke "
                        "WHERE dimension=? AND semantic_key=? AND value=? AND lens_id=?",
                        [dim, key, str(value), substrate.lens_id]
                    ).fetchone()
                    card = row[0] if row else 0
                elif op == "between":
                    v2 = c.get("value2", value)
                    row = substrate._conn.execute(
                        "SELECT COUNT(DISTINCT entity_id) FROM snf_spoke "
                        "WHERE dimension=? AND semantic_key=? "
                        "AND value>=? AND value<=? AND lens_id=?",
                        [dim, key, str(value), str(v2), substrate.lens_id]
                    ).fetchone()
                    card = row[0] if row else 0
                else:
                    # For other ops estimate using total for that key
                    row = substrate._conn.execute(
                        "SELECT COUNT(DISTINCT entity_id) FROM snf_spoke "
                        "WHERE dimension=? AND semantic_key=? AND lens_id=?",
                        [dim, key, substrate.lens_id]
                    ).fetchone()
                    card = row[0] if row else 0
            except Exception:
                card = -1

            estimates.append((card, c))

        # Sort by cardinality ascending — Portolan ordering
        estimates.sort(key=lambda x: x[0] if x[0] >= 0 else float("inf"))

        total = substrate.entity_count()
        for step, (card, c) in enumerate(estimates, 1):
            dim   = (c.get("category") or c.get("dimension") or "").upper()
            key   = (c.get("field") or "")
            op    = c.get("op", "eq")
            value = c.get("value")

            op_display = {
                "eq": "=", "not_eq": "!=", "gt": ">", "lt": "<",
                "gte": ">=", "lte": "<=", "contains": "CONTAINS",
                "prefix": "PREFIX", "between": "BETWEEN",
            }.get(op, op)

            if op == "between":
                expr = f'{dim}.{key} BETWEEN "{value}" AND "{c.get("value2")}"'
            else:
                expr = f'{dim}.{key} {op_display} "{value}"'

            pct = f"{100 * card / total:.0f}%" if total > 0 else "?"
            bar_width = int(20 * card / total) if total > 0 else 0
            bar = "█" * bar_width + "░" * (20 - bar_width)

            print(
                f"  Step {step}  {expr:<40}  "
                f"{_yellow(str(card))} entities  {_dim(bar)}  {_dim(pct)}"
            )

        print()


# ─────────────────────────────────────────────────────────────────────────────
# Schema display
# ─────────────────────────────────────────────────────────────────────────────

def _display_schema(substrate, dimension=None):
    """Show dimensions and fields available in the substrate."""
    if dimension:
        dim = dimension.upper()
        rows = substrate._conn.execute(
            "SELECT semantic_key, COUNT(DISTINCT entity_id) as cnt, "
            "COUNT(DISTINCT value) as vals "
            "FROM snf_spoke WHERE dimension=? AND lens_id=? "
            "GROUP BY semantic_key ORDER BY cnt DESC",
            [dim.lower(), substrate.lens_id]
        ).fetchall()

        if not rows:
            print(f"  {_red(f'No fields found for dimension {dim}')}\n")
            return

        print(f"  {_bold(dim)} — {len(rows)} field{'s' if len(rows) != 1 else ''}\n")
        for key, cnt, vals in rows:
            print(f"    {_cyan(f'{dim}.{key}'):<40}  "
                  f"{_dim(f'{cnt} entities · {vals} distinct values')}")
        print()

    else:
        dims = substrate.dimensions()
        total_entities = substrate.entity_count()
        total_facts    = substrate.count()

        print(f"\n  {_bold('Substrate')}  —  lens: {_cyan(substrate.lens_id)}")
        print(f"  {total_entities} entities · {total_facts} facts\n")

        for dim in sorted(dims):
            rows = substrate._conn.execute(
                "SELECT semantic_key, COUNT(DISTINCT entity_id) as cnt "
                "FROM snf_spoke WHERE dimension=? AND lens_id=? "
                "GROUP BY semantic_key ORDER BY cnt DESC",
                [dim, substrate.lens_id]
            ).fetchall()

            fields = [r[0] for r in rows]
            print(f"  {_bold(_yellow(dim.upper()))}")
            for key in fields:
                print(f"    {dim.upper()}.{key}")
            print()


# ─────────────────────────────────────────────────────────────────────────────
# TAB completer
# ─────────────────────────────────────────────────────────────────────────────

class _PeirceCompleter:
    """
    Schema-aware TAB completer for the Peirce shell.

    Completions:
        WHO.<TAB>        → WHO.field1  WHO.field2  ...
        \<TAB>           → \schema  \explain  \pivot  ...
        WHO.art<TAB>     → WHO.artist  (if artist is a field in WHO)
    """

    DIMENSIONS   = ["WHO", "WHAT", "WHEN", "WHERE", "WHY", "HOW"]
    SHELL_CMDS   = [
        r"\schema", r"\explain", r"\pivot", r"\limit",
        r"\history", r"\clear", r"\help", r"\q",
    ]
    OPERATORS    = ["=", "!=", ">", "<", ">=", "<=", "CONTAINS", "PREFIX", "BETWEEN"]
    KEYWORDS     = ["AND", "OR", "NOT", "BETWEEN", "AND", "true", "false"]

    def __init__(self, substrate):
        self._substrate = substrate
        self._field_cache = {}   # dim → [field, ...]
        self._matches    = []
        self._build_cache()

    def _build_cache(self):
        """Pre-load field names per dimension from the substrate."""
        for dim in self.DIMENSIONS:
            rows = self._substrate._conn.execute(
                "SELECT DISTINCT semantic_key FROM snf_spoke "
                "WHERE dimension=? AND lens_id=?",
                [dim.lower(), self._substrate.lens_id]
            ).fetchall()
            self._field_cache[dim] = [r[0] for r in rows]

    def complete(self, text, state):
        if state == 0:
            self._matches = self._get_matches(text)
        try:
            return self._matches[state]
        except IndexError:
            return None

    def _get_matches(self, text):
        # Shell commands
        if text.startswith("\\"):
            return [c for c in self.SHELL_CMDS if c.startswith(text)]

        # DIM.field completion — e.g. "WHO." or "WHO.art"
        for dim in self.DIMENSIONS:
            prefix = dim + "."
            if text.upper().startswith(prefix):
                typed_field = text[len(prefix):]
                fields = self._field_cache.get(dim, [])
                return [
                    f"{dim}.{f}" for f in fields
                    if f.startswith(typed_field.lower())
                ]

        # Dimension name completion
        upper = text.upper()
        dim_matches = [d for d in self.DIMENSIONS if d.startswith(upper)]
        if dim_matches:
            return dim_matches

        # Keyword completion
        kw_matches = [k for k in self.KEYWORDS if k.upper().startswith(text.upper())]
        if kw_matches:
            return kw_matches

        return []


# ─────────────────────────────────────────────────────────────────────────────
# PeirceShell
# ─────────────────────────────────────────────────────────────────────────────

class PeirceShell:
    """
    Interactive Peirce query shell.

    Not constructed directly in normal use — use run_shell() or the CLI.
    Can be constructed programmatically for embedding or testing.
    """

    PROMPT = _green("peirce") + _dim(">") + " "

    def __init__(self, substrate, limit=20):
        self._substrate    = substrate
        self._limit        = limit
        self._pivot_mode   = False
        self._history      = []
        self._last_parsed  = None
        self._last_result  = None
        self._completer    = None

    def _setup_readline(self):
        """Wire up readline history and TAB completion."""
        try:
            import readline
            self._completer = _PeirceCompleter(self._substrate)
            readline.set_completer(self._completer.complete)
            readline.parse_and_bind(
                "tab: complete" if sys.platform != "darwin"
                else "bind ^I rl_complete"
            )
            readline.set_completer_delims(" \t\n")
        except ImportError:
            pass  # readline not available — degrade gracefully

    def _print_banner(self):
        d = self._substrate.describe()
        print()
        print(f"  {_bold('Peirce')} — SNF query shell")
        print(f"  lens:     {_cyan(d['lens_id'])}")
        print(f"  entities: {d['entity_count']:,}")
        print(f"  facts:    {d['fact_count']:,}")
        print(f"  dims:     {', '.join(d['dimensions'])}")
        print()
        print(f"  Type a Peirce query or {_dim('\\help')} for commands.")
        print(f"  TAB completes field names.  {_dim('exit')} to quit.")
        print()

    def _handle_command(self, line):
        """Handle backslash commands. Returns True if handled."""
        parts = line.strip().split()
        cmd   = parts[0].lower() if parts else ""

        if cmd in (r"\q", "exit", "quit"):
            print(_dim("  bye"))
            return "exit"

        if cmd == r"\help":
            self._print_help()
            return True

        if cmd == r"\schema":
            dim = parts[1].upper() if len(parts) > 1 else None
            _display_schema(self._substrate, dim)
            return True

        if cmd == r"\explain":
            if self._last_parsed is None:
                print("  No previous query to explain.\n")
            else:
                _display_explain(self._last_parsed, self._substrate)
            return True

        if cmd == r"\pivot":
            self._pivot_mode = not self._pivot_mode
            state = "on" if self._pivot_mode else "off"
            print(f"  Pivot mode {_bold(state)}\n")
            return True

        if cmd == r"\limit":
            if len(parts) < 2:
                print(f"  Current limit: {self._limit or 'none'}\n")
            else:
                try:
                    n = int(parts[1])
                    self._limit = n if n > 0 else None
                    print(f"  Limit set to {self._limit or 'none'}\n")
                except ValueError:
                    print(f"  {_red('Invalid limit — must be a number')}\n")
            return True

        if cmd == r"\history":
            if not self._history:
                print("  No history yet.\n")
            else:
                for i, q in enumerate(self._history, 1):
                    print(f"  {_dim(str(i).rjust(3))}  {q}")
                print()
            return True

        if cmd == r"\clear":
            os.system("clear" if os.name != "nt" else "cls")
            return True

        return False

    def _run_query(self, line):
        """Parse and execute a Peirce query string."""
        from snf_peirce.parser import parse_to_constraints
        from snf_peirce.peirce import (
            query as peirce_query,
            PeirceParseError,
            PeirceDiscoveryError,
        )

        # Parse first so we can store for \explain
        parsed = parse_to_constraints(line)

        if not parsed["success"]:
            pos = parsed.get("position", 0)
            tok = parsed.get("token")
            print(f"  {_red('Parse error:')} {parsed['error']}")
            print(f"  {_dim('position ' + str(pos) + ', near: ' + repr(tok))}")
            print()
            return

        if parsed["type"] == "discovery":
            # Route discovery expressions to \schema display
            scope = parsed["scope"]
            if scope == "all":
                _display_schema(self._substrate)
            elif scope == "dimension":
                _display_schema(self._substrate, parsed["dimension"])
            elif scope == "field":
                dim   = parsed["dimension"]
                field = parsed["field"]
                rows  = self._substrate._conn.execute(
                    "SELECT DISTINCT value, COUNT(DISTINCT entity_id) as cnt "
                    "FROM snf_spoke WHERE dimension=? AND semantic_key=? AND lens_id=? "
                    "GROUP BY value ORDER BY cnt DESC LIMIT 50",
                    [dim.lower(), field, self._substrate.lens_id]
                ).fetchall()
                print(f"  {_bold(f'{dim.upper()}.{field}')} — {len(rows)} values\n")
                for val, cnt in rows:
                    print(f"    {val:<40}  {_dim(f'{cnt} entities')}")
                print()
            return

        # Store parsed result for \explain
        self._last_parsed = parsed

        # Execute
        t0 = time.perf_counter()
        try:
            from snf_peirce.compile import Substrate
            from snf_peirce.peirce import execute
            result = execute(parsed, self._substrate)
        except Exception as e:
            print(f"  {_red('Execution error:')} {e}\n")
            return

        elapsed_ms = (time.perf_counter() - t0) * 1000
        self._last_result = result

        # Apply limit for display
        if self._limit:
            from snf_peirce.peirce import ResultSet
            display_result = ResultSet(
                result.entity_ids[:self._limit],
                self._substrate,
                query_string=line,
                limit=self._limit,
            )
        else:
            display_result = result

        # Time display
        if elapsed_ms < 1:
            time_str = _dim(f"({elapsed_ms:.2f}ms)")
        else:
            time_str = _dim(f"({elapsed_ms:.0f}ms)")

        print(f"  {time_str}")
        print()

        _display_results(display_result, self._substrate, self._pivot_mode)

        if self._limit and result.count > self._limit:
            print(
                f"  {_dim(f'Showing {self._limit} of {result.count}. '
                          f'Use \\limit N to see more.')}\n"
            )

    def _print_help(self):
        print(textwrap.dedent(f"""
  {_bold('Peirce shell commands')}

  {_cyan('Queries')}
    WHO.field = "value"              equality
    WHO.field != "value"             not equal
    WHEN.field BETWEEN "a" AND "b"   range
    WHAT.field CONTAINS "text"       substring
    WHO.field PREFIX "text"          prefix match
    NOT WHO.field = "value"          negation
    expr AND expr                    intersection (across dims)
    expr OR expr                     union (top-level DNF)

  {_cyan('Discovery')}
    *                                list all dimensions
    WHO|*                            list fields in WHO
    WHO|field|*                      list values for WHO.field

  {_cyan('Shell commands')}
    \\schema                          show all dimensions and fields
    \\schema WHO                      show fields in WHO
    \\explain                         explain last query plan
    \\pivot                           toggle pivot/vertical display
    \\limit N                         set result limit (0 = unlimited)
    \\history                         show query history
    \\clear                           clear screen
    \\help                            this message
    exit / quit / \\q                 exit

  {_cyan('Tips')}
    TAB completes dimension fields:  WHO.<TAB>
    Arrow keys navigate history.
        """))

    def run(self):
        """Start the interactive shell loop."""
        self._setup_readline()
        self._print_banner()

        while True:
            try:
                line = input(self.PROMPT).strip()
            except EOFError:
                print()
                break
            except KeyboardInterrupt:
                print()
                continue

            if not line:
                continue

            # Store non-command lines in history
            if not line.startswith("\\") and line not in ("exit", "quit"):
                self._history.append(line)

            # Handle commands
            result = self._handle_command(line)
            if result == "exit":
                break
            if result:
                continue

            # Run as a query
            self._run_query(line)


# ─────────────────────────────────────────────────────────────────────────────
# Substrate loader
# ─────────────────────────────────────────────────────────────────────────────

def _load_substrate(path_str, lens_id=None):
    """
    Load a substrate from a path string.

    Supports:
        csv://path/to/spoke_dir    — load from CSV spoke files
        duckdb://path/to/file.db   — load from DuckDB file

    Returns a Substrate instance.
    """
    import types as _types

    # Stub duckdb if not available — handled by compile.py
    try:
        import duckdb
    except ImportError:
        raise ImportError(
            "duckdb is required for the Peirce shell. "
            "Install with: pip install duckdb"
        )

    from snf_peirce.compile import Substrate, _SPOKE_DDL, _SPOKE_INDEX_DDL

    if path_str.startswith("csv://"):
        dir_path = Path(path_str[6:])
        if not dir_path.exists():
            raise FileNotFoundError(f"Substrate directory not found: {dir_path}")

        import csv
        conn = duckdb.connect(":memory:")
        conn.execute(_SPOKE_DDL)

        csv_files = list(dir_path.glob("snf_*.csv"))
        if not csv_files:
            raise FileNotFoundError(
                f"No snf_*.csv files found in {dir_path}. "
                f"Run compile_data(..., into='csv://{dir_path}') to create them."
            )

        rows = []
        detected_lens_id = lens_id
        for csv_file in csv_files:
            with open(csv_file, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if not detected_lens_id:
                        detected_lens_id = row.get("lens_id", "unknown")
                    rows.append((
                        row["entity_id"],
                        row["dimension"],
                        row["semantic_key"],
                        row["value"],
                        row["coordinate"],
                        row["lens_id"],
                    ))

        if rows:
            conn.executemany(
                "INSERT INTO snf_spoke VALUES (?, ?, ?, ?, ?, ?)", rows
            )
        for idx_ddl in _SPOKE_INDEX_DDL:
            conn.execute(idx_ddl)

        return Substrate(conn, detected_lens_id or "unknown", source_path=dir_path)

    elif path_str.startswith("duckdb://"):
        db_path = Path(path_str[9:])
        if not db_path.exists():
            raise FileNotFoundError(f"DuckDB file not found: {db_path}")

        conn = duckdb.connect(str(db_path))

        # Detect lens_id
        if not lens_id:
            row = conn.execute(
                "SELECT DISTINCT lens_id FROM snf_spoke LIMIT 1"
            ).fetchone()
            lens_id = row[0] if row else "unknown"

        return Substrate(conn, lens_id, source_path=db_path)

    else:
        raise ValueError(
            f"Unrecognised substrate path: '{path_str}'. "
            f"Use 'csv://path' or 'duckdb://path'."
        )


# ─────────────────────────────────────────────────────────────────────────────
# run_shell() — main entry point
# ─────────────────────────────────────────────────────────────────────────────

def run_shell(substrate_path, lens_id=None, limit=20):
    """
    Load a substrate and start the interactive shell.

    Args:
        substrate_path: "csv://path" or "duckdb://path"
        lens_id:        optional — override lens_id detection
        limit:          default result limit (default 20)
    """
    try:
        substrate = _load_substrate(substrate_path, lens_id=lens_id)
    except (FileNotFoundError, ValueError) as e:
        print(f"\n  {_red('Error:')} {e}\n")
        sys.exit(1)

    shell = PeirceShell(substrate, limit=limit)
    shell.run()


# ─────────────────────────────────────────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────────────────────────────────────────

def main():
    """
    CLI entry point.

    Usage:
        peirce shell csv://my_spoke_dir
        peirce shell csv://my_spoke_dir --limit 50
        peirce shell csv://my_spoke_dir --lens-id my_lens

    Also works as:
        python -m shell csv://my_spoke_dir
    """
    import argparse

    parser = argparse.ArgumentParser(
        prog="peirce shell",
        description="Interactive Peirce query shell for SNF substrates.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""
examples:
  peirce shell csv://discogs_full
  peirce shell csv://discogs_full --limit 50
  peirce shell duckdb://my_substrate.db
        """),
    )
    parser.add_argument(
        "substrate",
        help="Substrate path — csv://dir or duckdb://file.db",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Default result limit per query (default 20, 0 = unlimited)",
    )
    parser.add_argument(
        "--lens-id",
        default=None,
        help="Override lens_id detection (usually not needed)",
    )

    args = parser.parse_args()
    run_shell(args.substrate, lens_id=args.lens_id, limit=args.limit)


if __name__ == "__main__":
    main()
