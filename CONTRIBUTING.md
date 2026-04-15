# Contributing to snf-peirce

Thank you for your interest in contributing. This document explains what we welcome, what we don't, and how to contribute effectively.

---

## What this project is

`snf-peirce` is the Python reference implementation of the SNF (Semantic Normalized Form) stack and the Peirce query language. It is not a general-purpose data tool — it is a specific implementation of a specific protocol.

The governing documents are:

- **SNF specification** — defines the coordinate model, hub-and-spoke constraint, and Boolean routing algebra
- **Peirce query language specification** — defines the grammar, Boolean semantics, and substrate neutrality theorem
- **MARC Bibliographic Lens v1.0** — defines the MARC translation layer

These specifications are the authority. If something conflicts with the spec, the spec wins.

---

## What we welcome

**Bug reports** — if something doesn't work as documented, please file an issue. Include a minimal reproducible example.

**Conformance issues** — if the Python implementation produces different results from the JS reference implementation on the same input, that is a bug. Please include the input, the Python output, and the JS output.

**New substrate adapters** — implementations of the substrate contract for additional backends (Postgres, SQL Server, additional Pinot configurations). Must conform to the substrate neutrality contract — same Peirce query, same results as DuckDB and Roaring substrates.

**New translators** — CADP translators for additional source formats (MARC variants, domain-specific formats). Must conform to the Minimum Viable Ingestor specification — emit evidence, not interpretation.

**New fetchers** — additional `base_fetcher.py` implementations for public APIs. Should follow the pattern established by `fetch_scryfall.py` and `fetch_loc.py`.

**Documentation improvements** — corrections, clarifications, additional examples. The `CHEATSHEET.py` and `examples/` directory are especially welcome targets.

**Test additions** — additional conformance tests, edge cases, substrate neutrality assertions.

---

## What we do not accept

**Changes to the Peirce grammar** — the grammar is defined in the specification and frozen at v1.0. Parser changes require a specification update first.

**Changes to the SNF coordinate model** — the six dimensions, the hub-and-spoke constraint, and the Boolean routing semantics are architectural invariants.

**Changes to the substrate contract** — the five requirements of a conformant substrate are fixed. Implementations may vary; the contract may not.

**Portolan-related contributions** — Portolan is a separate licensed component. The `snf-peirce` package intentionally does not include Portolan. Please do not submit planning, ordering, or admissibility logic for inclusion in this package.

**General data tool features** — aggregation, projection, sorting, joins, visualisation. These are out of scope for the routing layer. See the Peirce spec section 9 for the full exclusion list.

**Breaking changes to the public API** — `query()`, `discover()`, `compile_data()`, `suggest()`, `LensDraft`, `ResultSet`, and `DiscoveryResult` are the locked public surface. Changes that break existing code will not be accepted without a major version discussion.

---

## How to contribute

**For bug reports and questions** — open an issue. Describe what you expected, what you got, and include a minimal example.

**For code contributions:**

1. Fork the repo
2. Create a branch: `git checkout -b your-feature-name`
3. Make your changes
4. Run the tests: `python -m pytest tests/`
5. Confirm all 245 tests pass — and add tests for any new behaviour
6. For substrate additions: run `test_substrate_neutrality.py` and confirm your substrate returns identical results to DuckDB on the same plan
7. Open a pull request with a clear description of what you changed and why

**For new translators or fetchers** — open an issue first describing the source format and proposed coordinate mapping before writing code. This avoids wasted effort if the mapping approach needs discussion.

---

## Conformance requirement

A conformant Peirce parser must produce identical output to the JS reference implementation (`peirce_parser.cjs`) for all valid input. The cross-language conformance tests in `tests/test_conformance.py` are the enforcement mechanism.

If you implement Peirce in another language, we would love to know about it. Please open an issue — cross-language implementations that pass the conformance test suite are a significant contribution to the protocol.

---

## Licensing

`snf-peirce` is MIT licensed. Contributions are accepted under the same license.

The SNF specification, Peirce query language specification, and MARC Bibliographic Lens v1.0 are original works. Attribution is required in source code and documentation.

Portolan is separately licensed (AGPL v3 for community use, commercial license available). Do not submit Portolan implementation code to this repository.

---

## Questions

Open an issue. We read them all.
