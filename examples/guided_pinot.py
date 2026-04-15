"""
guided_pinot.py — Query data in Apache Pinot

Connects to a running Pinot cluster and lets you query
your data using plain language. No coding required.

Requirements:
    pip install snf-peirce requests

Pinot must be running and spoke tables must be loaded first.
Run load_into_pinot.py if you haven't loaded your data yet.

Usage:
    python guided_pinot.py
    python guided_pinot.py http://localhost:8099
"""

from __future__ import annotations

import sys
from guided_base import ask, confirm, banner, section, info, success, warn, error, query_loop


def main():
    banner("SNF / Peirce  —  Query Pinot")

    print("  Connect to Apache Pinot and query your data")
    print("  using plain language.")
    print("")

    # ── Check requests is available ───────────────────────────────────────────

    try:
        import requests
    except ImportError:
        error("The 'requests' library is required.")
        info("Install it with: pip install requests")
        sys.exit(1)

    # ── Broker URL ────────────────────────────────────────────────────────────

    if len(sys.argv) > 1:
        broker_url = sys.argv[1]
    else:
        print("  You need the address of your Pinot broker.")
        print("  If you're running Pinot locally the default is:")
        print("    http://localhost:8099")
        print("")
        broker_url = ask("Pinot broker URL", default="http://localhost:8099")

    # ── Connect ───────────────────────────────────────────────────────────────

    section("Connecting")

    info(f"Connecting to {broker_url}...")

    try:
        from pinot_substrate import PinotSubstrate
        substrate = PinotSubstrate(broker_url=broker_url)
    except ImportError:
        error("pinot_substrate.py not found.")
        info("Make sure pinot_substrate.py is in the same folder as this script.")
        sys.exit(1)
    except Exception as e:
        error(f"Could not create substrate: {e}")
        sys.exit(1)

    if not substrate.ping():
        print("")
        error(f"Could not reach Pinot at {broker_url}")
        print("")
        print("  Things to check:")
        print("    • Is Pinot running?  (run start-pinot.sh)")
        print("    • Is the broker URL correct?")
        print("    • Is the port 8099 accessible?")
        print("")
        sys.exit(1)

    success("Connected.")

    # ── Show what's available ─────────────────────────────────────────────────

    print("")
    print("  Available data:")
    print("")

    try:
        schema = substrate.schema()
        if schema:
            for dim, fields in schema.items():
                if fields:
                    sample = ", ".join(str(f) for f in fields[:3])
                    more   = f"  (+{len(fields)-3} more)" if len(fields) > 3 else ""
                    print(f"    {dim:<8}  {sample}{more}")
        else:
            info("(No data found — have you loaded your data into Pinot?)")
            info("Run: python load_into_pinot.py --from csv://my_spoke_dir --broker " + broker_url)
    except Exception:
        info("(Could not retrieve schema — you can still try querying)")

    print("")

    # ── Query loop ────────────────────────────────────────────────────────────

    query_loop(substrate, welcome="Pinot is ready.")


if __name__ == "__main__":
    main()
