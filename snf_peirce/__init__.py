"""
snf-peirce — Python SNF package

A Python implementation of the SNF stack:
  - Peirce query parser (conformant with JS reference implementation)
  - Lens authoring (suggest, map, nucleus, LensDraft)
  - Substrate compilation (compile_data → DuckDB-backed Substrate)
  - Query execution (query, execute → ResultSet)
  - Interactive shell (peirce shell csv://...)
  - SRF record parsing and substrate ingestion (SRFRecord)
  - Expedition bundle parsing and serialization (RsetBundle)

MIT License. See LICENSE file.
SNF specification and Peirce Query Language are original works.
Attribution required in source code and documentation.
"""

try:
    # Installed package — relative imports
    from .parser  import parse, parse_to_constraints, parseToConstraints
    from .lens    import suggest, load, save, validate, LensDraft, LensValidationError
    from .compile import compile_data, Substrate, CompileError, NucleusError
    from .peirce  import query, execute, discover, ResultSet, DiscoveryResult, PeirceParseError, PeirceDiscoveryError
    from .srf     import SRFRecord, SRFValidationError, load_srf_json, load_srf_file
    from .rset    import RsetBundle, RsetValidationError, load_rset_json, load_rset_file
except ImportError:
    # Standalone folder — direct imports
    from parser  import parse, parse_to_constraints, parseToConstraints
    from lens    import suggest, load, save, validate, LensDraft, LensValidationError
    from compile import compile_data, Substrate, CompileError, NucleusError
    from peirce  import query, execute, discover, ResultSet, DiscoveryResult, PeirceParseError, PeirceDiscoveryError
    from srf     import SRFRecord, SRFValidationError, load_srf_json, load_srf_file
    from rset    import RsetBundle, RsetValidationError, load_rset_json, load_rset_file

__version__ = "0.1.10"

__all__ = [
    "parse", "parse_to_constraints", "parseToConstraints",
    "suggest", "load", "save", "validate", "LensDraft", "LensValidationError",
    "compile_data", "Substrate", "CompileError", "NucleusError",
    "query", "execute", "discover", "ResultSet", "DiscoveryResult", "PeirceParseError", "PeirceDiscoveryError",
    "SRFRecord", "SRFValidationError", "load_srf_json", "load_srf_file",
    "RsetBundle", "RsetValidationError", "load_rset_json", "load_rset_file",
]
