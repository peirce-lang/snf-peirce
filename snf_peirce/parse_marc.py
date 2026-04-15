"""
parse_marc.py — Pure Python MARC21 binary file parser

Parses binary MARC (.mrc) and MARCXML files into the normalized
record shape that MARCTranslator expects.

No dependencies beyond the Python standard library.
This replaces the need for pymarc for basic MARC ingestion.

Usage:
    from parse_marc import parse_mrc, parse_marcxml

    # Binary MARC (.mrc)
    records = parse_mrc("catalog.mrc")

    # MARCXML
    records = parse_marcxml("catalog.xml")

    # Then translate
    from marc_translator import MARCTranslator
    translator = MARCTranslator(source_id="loc")
    for record in records:
        facts = translator.translate_record(record)

Output shape (same as MARCTranslator_v3.js normalizeMARC4JSRecord):
    {
        "leader":        str,
        "controlFields": [{"tag": str, "data": str}, ...],
        "dataFields": [
            {
                "tag":        str,
                "indicator1": str,
                "indicator2": str,
                "subfields":  [{"code": str, "data": str}, ...]
            },
            ...
        ]
    }
"""

from __future__ import annotations
from pathlib import Path


# ─────────────────────────────────────────────────────────────────────────────
# Binary MARC (.mrc) parser
#
# MARC21 binary format (ISO 2709):
#   Leader:     bytes 0-23   — fixed-length record metadata
#   Directory:  bytes 24-n   — index of fields (12 bytes per entry)
#   Fields:     bytes n+1-end — field data
#
# Each directory entry: tag(3) + length(4) + offset(5) = 12 bytes
# Fields end with field terminator \x1e
# Records end with record terminator \x1d
# Subfields start with subfield delimiter \x1f followed by 1-char code
# ─────────────────────────────────────────────────────────────────────────────

_FIELD_TERM     = b"\x1e"
_RECORD_TERM    = b"\x1d"
_SUBFIELD_DELIM = b"\x1f"


def parse_mrc(filepath, encoding="utf-8"):
    """
    Parse a binary MARC (.mrc) file.

    Args:
        filepath: str or Path to a .mrc file
        encoding: character encoding (default utf-8, try marc-8 for older files)

    Returns:
        list of normalized MARC record dicts
    """
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"MARC file not found: {filepath}")

    with open(path, "rb") as f:
        data = f.read()

    records = []
    pos     = 0

    while pos < len(data):
        # Need at least 24 bytes for a leader
        if pos + 24 > len(data):
            break

        # Record length is first 5 bytes of leader
        try:
            rec_len = int(data[pos:pos + 5])
        except (ValueError, TypeError):
            break

        if rec_len < 24 or pos + rec_len > len(data):
            break

        record_bytes = data[pos:pos + rec_len]
        record = _parse_record(record_bytes, encoding)
        if record:
            records.append(record)

        pos += rec_len

    return records


def _parse_record(record_bytes, encoding):
    """Parse a single MARC record from bytes."""
    try:
        leader = record_bytes[:24].decode(encoding, errors="replace")

        # Base address of data (bytes 12-16 of leader)
        base_addr = int(leader[12:17])

        # Directory ends at first field terminator
        dir_end = record_bytes.index(_FIELD_TERM[0])
        directory_bytes = record_bytes[24:dir_end]
        directory = directory_bytes.decode(encoding, errors="replace")

        control_fields = []
        data_fields    = []

        # Each directory entry is 12 characters: tag(3) length(4) offset(5)
        for i in range(0, len(directory) - 11, 12):
            tag    = directory[i:i + 3]
            try:
                length = int(directory[i + 3:i + 7])
                offset = int(directory[i + 7:i + 12])
            except ValueError:
                continue

            start = base_addr + offset
            end   = start + length
            if end > len(record_bytes):
                continue

            field_bytes = record_bytes[start:end]
            field_str   = field_bytes.decode(encoding, errors="replace")
            field_str   = field_str.rstrip("\x1e\x1d")

            if tag < "010":
                # Control field — no indicators or subfields
                control_fields.append({
                    "tag":  tag,
                    "data": field_str,
                })
            else:
                # Data field — first 2 chars are indicators
                ind1 = field_str[0] if len(field_str) > 0 else " "
                ind2 = field_str[1] if len(field_str) > 1 else " "

                # Subfields are delimited by \x1f followed by a 1-char code
                subfields = []
                raw_subfields = field_str[2:].split("\x1f")
                for sf in raw_subfields:
                    if sf and len(sf) >= 2:
                        subfields.append({
                            "code": sf[0],
                            "data": sf[1:],
                        })

                data_fields.append({
                    "tag":        tag,
                    "indicator1": ind1,
                    "indicator2": ind2,
                    "subfields":  subfields,
                })

        return {
            "leader":        leader,
            "controlFields": control_fields,
            "dataFields":    data_fields,
        }

    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# MARCXML parser
# ─────────────────────────────────────────────────────────────────────────────

def parse_marcxml(filepath, encoding="utf-8"):
    """
    Parse a MARCXML file.

    MARCXML is the XML representation of MARC21.
    Namespace: http://www.loc.gov/MARC21/slim

    Args:
        filepath: str or Path to a .xml MARCXML file

    Returns:
        list of normalized MARC record dicts
    """
    import xml.etree.ElementTree as ET

    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"MARCXML file not found: {filepath}")

    tree = ET.parse(path)
    root = tree.getroot()

    # Handle namespace
    ns = ""
    if root.tag.startswith("{"):
        ns = root.tag.split("}")[0] + "}"

    records = []

    # Find all record elements
    record_elements = root.findall(f".//{ns}record")
    if not record_elements:
        record_elements = [root] if root.tag.endswith("record") else []

    for rec_el in record_elements:
        record = _parse_marcxml_record(rec_el, ns)
        if record:
            records.append(record)

    return records


def _parse_marcxml_record(rec_el, ns):
    """Parse a single MARCXML record element."""
    try:
        control_fields = []
        data_fields    = []

        # Leader
        leader_el = rec_el.find(f"{ns}leader")
        leader    = leader_el.text or "" if leader_el is not None else ""

        # Control fields (001-009)
        for cf in rec_el.findall(f"{ns}controlfield"):
            tag  = cf.get("tag", "")
            data = cf.text or ""
            control_fields.append({"tag": tag, "data": data})

        # Data fields (010-999)
        for df in rec_el.findall(f"{ns}datafield"):
            tag  = df.get("tag", "")
            ind1 = df.get("ind1", " ")
            ind2 = df.get("ind2", " ")

            subfields = []
            for sf in df.findall(f"{ns}subfield"):
                code = sf.get("code", "")
                data = sf.text or ""
                subfields.append({"code": code, "data": data})

            data_fields.append({
                "tag":        tag,
                "indicator1": ind1,
                "indicator2": ind2,
                "subfields":  subfields,
            })

        return {
            "leader":        leader,
            "controlFields": control_fields,
            "dataFields":    data_fields,
        }

    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Auto-detect format
# ─────────────────────────────────────────────────────────────────────────────

def parse_marc_file(filepath, encoding="utf-8"):
    """
    Parse a MARC file — auto-detects binary MARC vs MARCXML by extension.

    Args:
        filepath: str or Path — .mrc, .marc, .dat for binary; .xml for MARCXML
        encoding: character encoding (default utf-8)

    Returns:
        list of normalized MARC record dicts
    """
    path = Path(filepath)
    ext  = path.suffix.lower()

    if ext in (".xml", ".marcxml"):
        return parse_marcxml(filepath, encoding)
    else:
        # .mrc, .marc, .dat, or anything else — try binary MARC
        return parse_mrc(filepath, encoding)


# ─────────────────────────────────────────────────────────────────────────────
# CLI — quick inspection tool
# ─────────────────────────────────────────────────────────────────────────────

def main():
    """
    Quick inspection of a MARC file from the command line.

    Usage:
        python parse_marc.py catalog.mrc
        python parse_marc.py catalog.xml
        python parse_marc.py catalog.mrc --count   # just count records
        python parse_marc.py catalog.mrc --n 5     # show first N records
    """
    import sys
    import argparse

    parser = argparse.ArgumentParser(
        description="Inspect a MARC file",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("filepath", help="Path to .mrc or .xml file")
    parser.add_argument("--count", action="store_true",
                        help="Just show record count")
    parser.add_argument("--n", type=int, default=3,
                        help="Number of records to display (default 3)")
    parser.add_argument("--encoding", default="utf-8",
                        help="Character encoding (default utf-8, try marc-8 for older files)")
    args = parser.parse_args()

    records = parse_marc_file(args.filepath, encoding=args.encoding)
    print(f"\n  {args.filepath}")
    print(f"  {len(records)} records\n")

    if args.count:
        return

    for i, rec in enumerate(records[:args.n]):
        print(f"  {'─' * 50}")
        print(f"  Record {i + 1}")
        print(f"  {'─' * 50}")

        for cf in rec["controlFields"]:
            print(f"    {cf['tag']}:  {cf['data']}")

        for df in rec["dataFields"]:
            sf_str = "  ".join(
                f"${sf['code']}={sf['data']}" for sf in df["subfields"]
            )
            print(f"    {df['tag']} [{df['indicator1']}{df['indicator2']}]:  {sf_str}")
        print()


if __name__ == "__main__":
    main()
