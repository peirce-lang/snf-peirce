"""
guided_sql.py — Query data in Postgres or SQL Server

Connects to an existing database that already has SNF spoke
tables loaded, and lets you query it using plain language.

Spoke tables are created by running the lens-tool sql:// output
against your database, or by using the SNF emitter pipeline.

Requirements:
    pip install snf-peirce

    For Postgres:    pip install psycopg2-binary
    For SQL Server:  pip install pyodbc

Usage:
    python guided_sql.py
"""

from __future__ import annotations

import sys
import getpass
from guided_base import ask, confirm, banner, section, info, success, warn, error, query_loop


DB_TYPES = {
    "postgres":  "PostgreSQL",
    "sqlserver": "Microsoft SQL Server",
}


# ─────────────────────────────────────────────────────────────────────────────
# Minimal inline substrate classes
# (full versions would live in postgres_substrate.py / mssql_substrate.py)
# ─────────────────────────────────────────────────────────────────────────────

class _PostgresSubstrate:
    """
    Minimal Postgres substrate for guided querying.
    Executes Peirce queries via anchor + IN-clause chaining.
    """

    DIMENSIONS = ["WHO", "WHAT", "WHEN", "WHERE", "WHY", "HOW"]

    def __init__(self, host, port, database, user, password, schema="public", table_prefix="snf"):
        import psycopg2
        self._conn   = psycopg2.connect(
            host=host, port=port, dbname=database,
            user=user, password=password
        )
        self._schema = schema
        self._prefix = table_prefix
        self._tables = {
            dim: f"{table_prefix}_{dim.lower()}"
            for dim in self.DIMENSIONS
        }

    def execute(self, plan):
        if plan.unsatisfiable or not plan.dimension_groups:
            return []

        current_ids = None
        cur = self._conn.cursor()

        for group in plan.dimension_groups:
            table  = f'"{self._schema}"."{self._tables[group.dimension]}"'
            coords = [
                f"{c.dimension}.{c.key}={c.value}"
                for c in group.constraints
                if c.operator == "eq"
            ]
            if not coords:
                continue

            placeholders = ", ".join(["%s"] * len(coords))

            if current_ids is None:
                cur.execute(
                    f"SELECT DISTINCT entity_id FROM {table} "
                    f"WHERE coordinate IN ({placeholders})",
                    coords
                )
            else:
                if not current_ids:
                    return []
                id_placeholders = ", ".join(["%s"] * len(current_ids))
                cur.execute(
                    f"SELECT DISTINCT entity_id FROM {table} "
                    f"WHERE coordinate IN ({placeholders}) "
                    f"AND entity_id IN ({id_placeholders})",
                    coords + list(current_ids)
                )

            current_ids = {row[0] for row in cur.fetchall()}
            if not current_ids:
                return []

        return sorted(str(i) for i in current_ids) if current_ids else []

    def schema(self):
        result = {}
        cur    = self._conn.cursor()
        for dim, table in self._tables.items():
            try:
                cur.execute(
                    f'SELECT DISTINCT coordinate FROM "{self._schema}"."{table}" '
                    f'LIMIT 100'
                )
                coords = [row[0] for row in cur.fetchall() if row[0]]
                fields = list({c.split("=")[0].split(".")[-1] for c in coords if "=" in c})
                result[dim] = fields
            except Exception:
                result[dim] = []
        return result

    def ping(self):
        try:
            cur = self._conn.cursor()
            cur.execute("SELECT 1")
            return True
        except Exception:
            return False


class _MSSQLSubstrate:
    """
    Minimal SQL Server substrate for guided querying.
    """

    DIMENSIONS = ["WHO", "WHAT", "WHEN", "WHERE", "WHY", "HOW"]

    def __init__(self, host, database, user, password, schema="dbo", table_prefix="snf"):
        import pyodbc
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={host};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password}"
        )
        self._conn   = pyodbc.connect(conn_str)
        self._schema = schema
        self._prefix = table_prefix
        self._tables = {
            dim: f"{table_prefix}_{dim.lower()}"
            for dim in self.DIMENSIONS
        }

    def execute(self, plan):
        if plan.unsatisfiable or not plan.dimension_groups:
            return []

        current_ids = None
        cur = self._conn.cursor()

        for group in plan.dimension_groups:
            table  = f"[{self._schema}].[{self._tables[group.dimension]}]"
            coords = [
                f"{c.dimension}.{c.key}={c.value}"
                for c in group.constraints
                if c.operator == "eq"
            ]
            if not coords:
                continue

            placeholders = ", ".join(["?"] * len(coords))

            if current_ids is None:
                cur.execute(
                    f"SELECT DISTINCT entity_id FROM {table} "
                    f"WHERE coordinate IN ({placeholders})",
                    coords
                )
            else:
                if not current_ids:
                    return []
                id_placeholders = ", ".join(["?"] * len(current_ids))
                cur.execute(
                    f"SELECT DISTINCT entity_id FROM {table} "
                    f"WHERE coordinate IN ({placeholders}) "
                    f"AND entity_id IN ({id_placeholders})",
                    coords + list(current_ids)
                )

            current_ids = {row[0] for row in cur.fetchall()}
            if not current_ids:
                return []

        return sorted(str(i) for i in current_ids) if current_ids else []

    def schema(self):
        result = {}
        cur    = self._conn.cursor()
        for dim, table in self._tables.items():
            try:
                cur.execute(
                    f"SELECT DISTINCT coordinate FROM [{self._schema}].[{table}]"
                )
                coords = [row[0] for row in cur.fetchall() if row[0]]
                fields = list({c.split("=")[0].split(".")[-1] for c in coords if "=" in c})
                result[dim] = fields
            except Exception:
                result[dim] = []
        return result

    def ping(self):
        try:
            cur = self._conn.cursor()
            cur.execute("SELECT 1")
            return True
        except Exception:
            return False


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    banner("SNF / Peirce  —  Query Your Database")

    print("  Connect to an existing database and query your data")
    print("  using plain language.")
    print("")
    print("  Your database must already have SNF spoke tables loaded.")
    print("  If not, run the lens-tool with --into sql:// first.")
    print("")

    # ── Database type ─────────────────────────────────────────────────────────

    print("  Database type:\n")
    for key, label in DB_TYPES.items():
        print(f"    {key:<12}  {label}")
    print("")

    db_type = ask("Which database").strip().lower()

    if db_type not in DB_TYPES:
        error(f"'{db_type}' is not supported.")
        info(f"Choose from: {', '.join(DB_TYPES.keys())}")
        sys.exit(1)

    # ── Connection details ────────────────────────────────────────────────────

    section("Connection details")

    substrate = None

    if db_type == "postgres":
        # Check psycopg2 is available
        try:
            import psycopg2
        except ImportError:
            error("psycopg2 is required for Postgres.")
            info("Install it with: pip install psycopg2-binary")
            sys.exit(1)

        host     = ask("Host",          default="localhost")
        port     = ask("Port",          default="5432")
        database = ask("Database name")
        schema   = ask("Schema",        default="public")
        user     = ask("Username",      default="postgres")
        password = getpass.getpass("  Password: ")

        info(f"Connecting to {host}:{port}/{database}...")

        try:
            substrate = _PostgresSubstrate(
                host=host, port=int(port), database=database,
                user=user, password=password, schema=schema
            )
        except Exception as e:
            error(f"Connection failed: {e}")
            info("Check your connection details and try again.")
            sys.exit(1)

    elif db_type == "sqlserver":
        # Check pyodbc is available
        try:
            import pyodbc
        except ImportError:
            error("pyodbc is required for SQL Server.")
            info("Install it with: pip install pyodbc")
            info("You also need the ODBC Driver 17 for SQL Server.")
            sys.exit(1)

        host     = ask("Host",          default="localhost")
        database = ask("Database name")
        schema   = ask("Schema",        default="dbo")
        user     = ask("Username")
        password = getpass.getpass("  Password: ")

        info(f"Connecting to {host}/{database}...")

        try:
            substrate = _MSSQLSubstrate(
                host=host, database=database,
                user=user, password=password, schema=schema
            )
        except Exception as e:
            error(f"Connection failed: {e}")
            info("Check your connection details and try again.")
            sys.exit(1)

    if not substrate.ping():
        error("Connected but database did not respond.")
        sys.exit(1)

    success("Connected.")

    # ── Show what's there ─────────────────────────────────────────────────────

    print("")
    print("  Available data:")
    print("")

    try:
        schema_data = substrate.schema()
        has_data    = False
        for dim, fields in schema_data.items():
            if fields:
                has_data = True
                sample = ", ".join(str(f) for f in fields[:3])
                more   = f"  (+{len(fields)-3} more)" if len(fields) > 3 else ""
                print(f"    {dim:<8}  {sample}{more}")
        if not has_data:
            warn("No SNF spoke tables found in this database.")
            warn("Have you run the lens-tool with --into sql:// ?")
    except Exception:
        info("(Could not retrieve schema — you can still try querying)")

    print("")

    # ── Wrap in a plan-aware adapter ──────────────────────────────────────────
    # The query_loop calls peirce.query(substrate, string)
    # peirce.query expects a Substrate with a DuckDB connection
    # For SQL substrates we wrap with a thin adapter

    class _SQLAdapter:
        """Wraps our SQL substrate to work with peirce.query()."""

        def __init__(self, inner):
            self._inner = inner

        def execute(self, plan):
            return self._inner.execute(plan)

        def schema(self):
            return self._inner.schema()

        def ping(self):
            return self._inner.ping()

    # ── Query loop ────────────────────────────────────────────────────────────

    # Use a direct query loop since SQL substrates don't wrap DuckDB
    _direct_query_loop(substrate)


def _direct_query_loop(substrate):
    """
    Query loop for SQL substrates — parses Peirce and calls substrate.execute()
    directly rather than going through peirce.query() which expects DuckDB.
    """
    from snf_peirce.parser import parse_to_constraints
    from guided_base import show_result, _show_help, _show_schema

    print("")
    print("  Your database is ready.")
    print("")
    print("  Type a query to get started. Examples:")
    print('    WHO.attorney = "Smith"')
    print('    WHEN.year BETWEEN "2020" AND "2024"')
    print('    WHO.attorney = "Smith" AND WHERE.office = "Seattle"')
    print("")
    print(r"  Type \help for all commands. Type exit to quit.")
    print("")

    while True:
        try:
            raw = input("peirce> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n\n  Goodbye.\n")
            break

        if not raw:
            continue

        q = raw

        if q.lower() in ("exit", "quit", "q"):
            print("\n  Goodbye.\n")
            break

        if q.lower() in (r"\help", "help", "?"):
            _show_help()
            continue

        if q.lower() == r"\schema":
            _show_schema(substrate)
            continue

        if q.lower().startswith(r"\schema "):
            dim = q.split(None, 1)[1].upper()
            _show_schema(substrate, dim)
            continue

        # Parse and execute
        try:
            from snf_peirce.parser import parse_to_constraints
            from snf_peirce.peirce import PeirceParseError
            from plan import SNFPlan, DimensionGroup, Constraint

            parsed = parse_to_constraints(q)

            if not parsed.get("success"):
                print(f"\n  Could not parse that query: {parsed.get('error')}\n")
                continue

            # Build SNFPlan from parsed constraints
            conjuncts  = parsed.get("conjuncts", [])
            all_ids    = set()

            for conjunct in conjuncts:
                groups = []
                by_dim = {}
                for c in conjunct:
                    dim = c.get("dimension") or c.get("category", "").upper()
                    if dim not in by_dim:
                        by_dim[dim] = []
                    by_dim[dim].append(Constraint(
                        dimension = dim,
                        key       = c.get("field", ""),
                        value     = c.get("value", ""),
                        operator  = c.get("operator", "eq"),
                    ))
                for dim, constraints in by_dim.items():
                    groups.append(DimensionGroup(dim, constraints))

                plan    = SNFPlan(dimension_groups=groups)
                ids     = substrate.execute(plan)
                all_ids |= set(ids)

            count = len(all_ids)
            if count == 0:
                print("\n  No matches found.\n")
            elif count == 1:
                print(f"\n  1 match found.\n")
                for eid in sorted(all_ids):
                    print(f"    {eid}")
                print("")
            else:
                print(f"\n  {count:,} matches found.\n")
                for eid in sorted(all_ids)[:20]:
                    print(f"    {eid}")
                if count > 20:
                    print(f"    ... and {count - 20:,} more")
                print("")

        except Exception as e:
            print(f"\n  Error: {e}\n")


if __name__ == "__main__":
    main()
