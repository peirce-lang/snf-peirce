"""
test_parser.py — Peirce Parser Conformance Tests

Tests are organized to match JS test cases exactly. Every test that
passes here must produce the same output as the JS peirce_parser.cjs
on the same input. That is the conformance guarantee.

Run with: pytest test_parser.py -v
"""


from parser import parse, parse_to_constraints


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def ok(result):
    assert result["success"] is True, f"Expected success but got error: {result.get('error')}"
    return result

def fail(result):
    assert result["success"] is False, f"Expected failure but got: {result}"
    return result

def constraint(category, field, op, value, value2=None, negated=None):
    c = {"category": category, "field": field, "op": op, "value": value}
    if value2 is not None:
        c["value2"] = value2
    if negated is not None:
        c["negated"] = negated
    return c


# ─────────────────────────────────────────────────────────────────────────────
# 1. Discovery expressions
# ─────────────────────────────────────────────────────────────────────────────

class TestDiscovery:

    def test_all_dimensions(self):
        r = ok(parse("*"))
        assert r["type"]  == "discovery"
        assert r["scope"] == "all"

    def test_dimension_scope(self):
        r = ok(parse("WHO|*"))
        assert r["type"]      == "discovery"
        assert r["scope"]     == "dimension"
        assert r["dimension"] == "WHO"

    def test_dimension_scope_lowercase(self):
        # dimension tokens are uppercased by the lexer
        r = ok(parse("who|*"))
        assert r["scope"]     == "dimension"
        assert r["dimension"] == "WHO"

    def test_field_scope(self):
        r = ok(parse("WHO|role|*"))
        assert r["type"]      == "discovery"
        assert r["scope"]     == "field"
        assert r["dimension"] == "WHO"
        assert r["field"]     == "role"

    def test_all_six_dimensions_scope(self):
        for dim in ("WHO", "WHAT", "WHEN", "WHERE", "WHY", "HOW"):
            r = ok(parse(f"{dim}|*"))
            assert r["dimension"] == dim

    def test_discovery_passthrough_in_parse_to_constraints(self):
        r = ok(parse_to_constraints("WHO|role|*"))
        assert r["type"]  == "discovery"
        assert r["scope"] == "field"


# ─────────────────────────────────────────────────────────────────────────────
# 2. Simple equality constraints
# ─────────────────────────────────────────────────────────────────────────────

class TestSimpleEquality:

    def test_string_value(self):
        r = ok(parse_to_constraints('WHO.role = "attorney"'))
        assert r["conjuncts"] == [[constraint("WHO", "role", "eq", "attorney")]]

    def test_single_quote_string(self):
        r = ok(parse_to_constraints("WHO.role = 'attorney'"))
        assert r["conjuncts"] == [[constraint("WHO", "role", "eq", "attorney")]]

    def test_number_value(self):
        r = ok(parse_to_constraints("WHEN.year = 2024"))
        assert r["conjuncts"] == [[constraint("WHEN", "year", "eq", 2024.0)]]

    def test_boolean_true(self):
        r = ok(parse_to_constraints("WHAT.active = true"))
        assert r["conjuncts"] == [[constraint("WHAT", "active", "eq", True)]]

    def test_boolean_false(self):
        r = ok(parse_to_constraints("WHAT.active = false"))
        assert r["conjuncts"] == [[constraint("WHAT", "active", "eq", False)]]

    def test_dimension_case_insensitive(self):
        r = ok(parse_to_constraints('who.role = "attorney"'))
        assert r["conjuncts"][0][0]["category"] == "WHO"

    def test_field_lowercased(self):
        r = ok(parse_to_constraints('WHO.Role = "attorney"'))
        assert r["conjuncts"][0][0]["field"] == "role"

    def test_negative_number(self):
        r = ok(parse_to_constraints("WHAT.balance = -42"))
        assert r["conjuncts"][0][0]["value"] == -42.0

    def test_float_value(self):
        r = ok(parse_to_constraints("WHAT.score = 3.14"))
        assert r["conjuncts"][0][0]["value"] == 3.14


# ─────────────────────────────────────────────────────────────────────────────
# 3. All operators
# ─────────────────────────────────────────────────────────────────────────────

class TestOperators:

    def test_not_eq(self):
        r = ok(parse_to_constraints('WHO.role != "partner"'))
        assert r["conjuncts"][0][0]["op"] == "not_eq"

    def test_gt(self):
        r = ok(parse_to_constraints("WHEN.year > 2020"))
        assert r["conjuncts"][0][0]["op"] == "gt"

    def test_lt(self):
        r = ok(parse_to_constraints("WHEN.year < 2024"))
        assert r["conjuncts"][0][0]["op"] == "lt"

    def test_gte(self):
        r = ok(parse_to_constraints("WHEN.year >= 2020"))
        assert r["conjuncts"][0][0]["op"] == "gte"

    def test_lte(self):
        r = ok(parse_to_constraints("WHEN.year <= 2024"))
        assert r["conjuncts"][0][0]["op"] == "lte"

    def test_contains(self):
        r = ok(parse_to_constraints('WHAT.title CONTAINS "merger"'))
        assert r["conjuncts"][0][0]["op"] == "contains"

    def test_prefix(self):
        r = ok(parse_to_constraints('WHO.name PREFIX "Smith"'))
        assert r["conjuncts"][0][0]["op"] == "prefix"

    def test_contains_case_insensitive(self):
        r = ok(parse_to_constraints('WHAT.title contains "merger"'))
        assert r["conjuncts"][0][0]["op"] == "contains"


# ─────────────────────────────────────────────────────────────────────────────
# 4. AND (multi-dimension conjunction)
# ─────────────────────────────────────────────────────────────────────────────

class TestAnd:

    def test_two_dimensions(self):
        r = ok(parse_to_constraints('WHO.role = "attorney" AND WHERE.office = "Seattle"'))
        assert len(r["conjuncts"])    == 1
        assert len(r["conjuncts"][0]) == 2
        assert r["conjuncts"][0][0]["category"] == "WHO"
        assert r["conjuncts"][0][1]["category"] == "WHERE"

    def test_three_dimensions(self):
        r = ok(parse_to_constraints('WHO.role = "attorney" AND WHERE.office = "Seattle" AND WHEN.year = 2024'))
        assert len(r["conjuncts"][0]) == 3

    def test_single_conjunct(self):
        r = ok(parse_to_constraints('WHO.role = "attorney"'))
        assert len(r["conjuncts"]) == 1


# ─────────────────────────────────────────────────────────────────────────────
# 5. OR (top-level DNF)
# ─────────────────────────────────────────────────────────────────────────────

class TestOr:

    def test_simple_or(self):
        r = ok(parse_to_constraints('WHO.role = "attorney" OR WHO.role = "paralegal"'))
        assert len(r["conjuncts"]) == 2
        assert r["conjuncts"][0][0]["value"] == "attorney"
        assert r["conjuncts"][1][0]["value"] == "paralegal"

    def test_dnf_two_conjuncts(self):
        q = '(WHO.role = "attorney" AND WHERE.office = "Seattle") OR (WHO.role = "partner" AND WHERE.office = "New York")'
        r = ok(parse_to_constraints(q))
        assert len(r["conjuncts"])    == 2
        assert len(r["conjuncts"][0]) == 2
        assert len(r["conjuncts"][1]) == 2

    def test_three_or_conjuncts(self):
        q = 'WHO.role = "a" OR WHO.role = "b" OR WHO.role = "c"'
        r = ok(parse_to_constraints(q))
        assert len(r["conjuncts"]) == 3


# ─────────────────────────────────────────────────────────────────────────────
# 6. NOT
# ─────────────────────────────────────────────────────────────────────────────

class TestNot:

    def test_simple_not(self):
        r = ok(parse_to_constraints('NOT WHERE.office = "Seattle"'))
        c = r["conjuncts"][0][0]
        assert c["category"] == "WHERE"
        assert c["negated"]  is True

    def test_not_does_not_appear_when_false(self):
        r = ok(parse_to_constraints('WHERE.office = "Seattle"'))
        c = r["conjuncts"][0][0]
        assert "negated" not in c

    def test_not_in_conjunction(self):
        r = ok(parse_to_constraints('WHO.role = "attorney" AND NOT WHERE.office = "Seattle"'))
        assert r["conjuncts"][0][0].get("negated") is None or r["conjuncts"][0][0].get("negated") is False
        assert r["conjuncts"][0][1]["negated"] is True


# ─────────────────────────────────────────────────────────────────────────────
# 7. BETWEEN
# ─────────────────────────────────────────────────────────────────────────────

class TestBetween:

    def test_number_range(self):
        r = ok(parse_to_constraints("WHEN.year BETWEEN 2020 AND 2024"))
        c = r["conjuncts"][0][0]
        assert c["op"]     == "between"
        assert c["value"]  == 2020.0
        assert c["value2"] == 2024.0

    def test_string_range(self):
        r = ok(parse_to_constraints('WHEN.date BETWEEN "2020-01-01" AND "2024-12-31"'))
        c = r["conjuncts"][0][0]
        assert c["op"]     == "between"
        assert c["value"]  == "2020-01-01"
        assert c["value2"] == "2024-12-31"

    def test_between_in_conjunction(self):
        r = ok(parse_to_constraints('WHO.role = "attorney" AND WHEN.year BETWEEN 2020 AND 2024'))
        assert len(r["conjuncts"][0]) == 2
        assert r["conjuncts"][0][1]["op"] == "between"


# ─────────────────────────────────────────────────────────────────────────────
# 8. Parenthesized grouping
# ─────────────────────────────────────────────────────────────────────────────

class TestParens:

    def test_grouped_constraint_flattened(self):
        r = ok(parse_to_constraints('(WHO.role = "attorney") AND WHERE.office = "Seattle"'))
        assert len(r["conjuncts"][0]) == 2
        assert r["conjuncts"][0][0]["category"] == "WHO"

    def test_nested_grouping_flattened(self):
        r = ok(parse_to_constraints('(WHO.role = "attorney" AND WHERE.office = "Seattle")'))
        assert len(r["conjuncts"][0]) == 2


# ─────────────────────────────────────────────────────────────────────────────
# 9. String escaping
# ─────────────────────────────────────────────────────────────────────────────

class TestStringEscaping:

    def test_escaped_double_quote(self):
        r = ok(parse_to_constraints('WHO.name = "O\\"Brien"'))
        assert r["conjuncts"][0][0]["value"] == 'O"Brien'

    def test_escaped_backslash(self):
        r = ok(parse_to_constraints(r'WHO.name = "back\\slash"'))
        assert r["conjuncts"][0][0]["value"] == "back\\slash"


# ─────────────────────────────────────────────────────────────────────────────
# 10. Structured errors
# ─────────────────────────────────────────────────────────────────────────────

class TestErrors:

    def test_empty_string(self):
        r = fail(parse(""))
        assert "error" in r
        assert "position" in r
        assert "token" in r

    def test_whitespace_only(self):
        r = fail(parse("   "))
        assert "error" in r

    def test_missing_operator(self):
        r = fail(parse('WHO.role "attorney"'))
        assert r["success"] is False
        assert "position" in r

    def test_missing_value(self):
        r = fail(parse("WHO.role ="))
        assert r["success"] is False

    def test_unknown_dimension(self):
        r = fail(parse('WHAT_NOT.role = "x"'))
        assert r["success"] is False

    def test_unterminated_string(self):
        r = fail(parse('WHO.role = "unterminated'))
        assert r["success"] is False
        assert "Unterminated" in r["error"]

    def test_or_inside_parens_gives_helpful_error(self):
        r = fail(parse('(WHO.role = "attorney" OR WHO.role = "paralegal")'))
        assert r["success"] is False
        assert "OR inside parentheses" in r["error"]
        assert "DNF" in r["error"]

    def test_unexpected_character(self):
        r = fail(parse("WHO.role @ attorney"))
        assert r["success"] is False

    def test_missing_between_and(self):
        r = fail(parse("WHEN.year BETWEEN 2020 OR 2024"))
        assert r["success"] is False
        assert "AND" in r["error"]

    def test_error_has_position(self):
        r = fail(parse('WHO.role = "attorney" BADTOKEN'))
        assert isinstance(r["position"], int)
        assert r["position"] >= 0

    def test_error_has_token(self):
        r = fail(parse('WHO.role = "attorney" BADTOKEN'))
        assert "token" in r


# ─────────────────────────────────────────────────────────────────────────────
# 11. Output shape invariants
# ─────────────────────────────────────────────────────────────────────────────

class TestOutputShape:

    def test_success_key_present(self):
        r = parse('WHO.role = "attorney"')
        assert "success" in r

    def test_conjuncts_is_list_of_lists(self):
        r = ok(parse_to_constraints('WHO.role = "attorney"'))
        assert isinstance(r["conjuncts"], list)
        assert isinstance(r["conjuncts"][0], list)

    def test_constraint_has_required_keys(self):
        r = ok(parse_to_constraints('WHO.role = "attorney"'))
        c = r["conjuncts"][0][0]
        assert "category" in c
        assert "field"    in c
        assert "op"       in c
        assert "value"    in c

    def test_between_has_value2(self):
        r = ok(parse_to_constraints("WHEN.year BETWEEN 2020 AND 2024"))
        assert "value2" in r["conjuncts"][0][0]

    def test_no_negated_key_when_not_negated(self):
        r = ok(parse_to_constraints('WHO.role = "attorney"'))
        assert "negated" not in r["conjuncts"][0][0]

    def test_discovery_has_type_and_scope(self):
        r = ok(parse("WHO|*"))
        assert "type"  in r
        assert "scope" in r

    def test_deterministic(self):
        q = 'WHO.role = "attorney" AND WHERE.office = "Seattle"'
        assert parse_to_constraints(q) == parse_to_constraints(q)


# ─────────────────────────────────────────────────────────────────────────────
# 12. camelCase alias
# ─────────────────────────────────────────────────────────────────────────────

class TestAlias:

    def test_camel_case_alias_exists(self):
        from parser import parseToConstraints
        r = ok(parseToConstraints('WHO.role = "attorney"'))
        assert r["conjuncts"][0][0]["category"] == "WHO"

    def test_alias_identical_to_snake_case(self):
        from parser import parseToConstraints
        q = 'WHO.role = "attorney" AND WHEN.year = 2024'
        assert parseToConstraints(q) == parse_to_constraints(q)


# ─────────────────────────────────────────────────────────────────────────────
# 13. ONLY operator — happy path
# ─────────────────────────────────────────────────────────────────────────────

class TestOnly:

    def test_basic_only(self):
        r = ok(parse_to_constraints('WHAT.color ONLY "Red"'))
        assert r["conjuncts"] == [[constraint("WHAT", "color", "only", "Red")]]

    def test_only_string_value(self):
        r = ok(parse_to_constraints('WHO.partner_id ONLY "KPM"'))
        c = r["conjuncts"][0][0]
        assert c["op"]    == "only"
        assert c["value"] == "KPM"

    def test_only_numeric_value(self):
        r = ok(parse_to_constraints("WHAT.cmc ONLY 3"))
        c = r["conjuncts"][0][0]
        assert c["op"]    == "only"
        assert c["value"] == 3.0

    def test_only_boolean_value(self):
        r = ok(parse_to_constraints("WHAT.is_legal ONLY true"))
        c = r["conjuncts"][0][0]
        assert c["op"]    == "only"
        assert c["value"] is True

    def test_only_in_conjunction_first(self):
        r = ok(parse_to_constraints('WHAT.color ONLY "Red" AND WHEN.year = "2023"'))
        assert len(r["conjuncts"][0]) == 2
        assert r["conjuncts"][0][0]["op"] == "only"
        assert r["conjuncts"][0][1]["op"] == "eq"

    def test_only_in_conjunction_second(self):
        # ONLY appearing after another constraint — ordering must be preserved
        r = ok(parse_to_constraints('WHEN.year = "2023" AND WHAT.color ONLY "Red"'))
        assert r["conjuncts"][0][0]["op"] == "eq"
        assert r["conjuncts"][0][1]["op"] == "only"

    def test_only_in_or_both_conjuncts(self):
        r = ok(parse_to_constraints('WHAT.color ONLY "Red" OR WHAT.color ONLY "White"'))
        assert len(r["conjuncts"])         == 2
        assert r["conjuncts"][0][0]["op"]  == "only"
        assert r["conjuncts"][0][0]["value"] == "Red"
        assert r["conjuncts"][1][0]["op"]  == "only"
        assert r["conjuncts"][1][0]["value"] == "White"

    def test_only_not_negated_by_default(self):
        r = ok(parse_to_constraints('WHAT.color ONLY "Red"'))
        assert "negated" not in r["conjuncts"][0][0]

    def test_only_case_insensitive(self):
        # Lexer uppercases keywords — 'only' must tokenize identically to 'ONLY'
        r = ok(parse_to_constraints('WHAT.color only "Red"'))
        assert r["conjuncts"][0][0]["op"] == "only"

    def test_only_output_shape(self):
        r = ok(parse_to_constraints('WHAT.color ONLY "Red"'))
        c = r["conjuncts"][0][0]
        assert "category" in c
        assert "field"    in c
        assert "op"       in c
        assert "value"    in c
        assert c["category"] == "WHAT"
        assert c["field"]    == "color"

    def test_only_all_six_dimensions(self):
        for dim in ("WHO", "WHAT", "WHEN", "WHERE", "WHY", "HOW"):
            r = ok(parse_to_constraints(f'{dim}.field ONLY "val"'))
            assert r["conjuncts"][0][0]["category"] == dim
            assert r["conjuncts"][0][0]["op"]       == "only"

    def test_only_single_quote_value(self):
        r = ok(parse_to_constraints("WHAT.color ONLY 'Red'"))
        assert r["conjuncts"][0][0]["value"] == "Red"


# ─────────────────────────────────────────────────────────────────────────────
# 14. NOT ONLY — passthrough behavior
#
# NOT ONLY is semantically unusual (complement of an exclusivity assertion)
# and Portolan does not currently expand it. The parser's job is to represent
# the intent faithfully — op: "only", negated: True — and pass it through.
# Portolan raises "NOT ONLY is not yet supported" at plan time.
# ─────────────────────────────────────────────────────────────────────────────

class TestNotOnly:

    def test_not_only_parses_without_error(self):
        r = ok(parse_to_constraints('NOT WHAT.color ONLY "Red"'))
        c = r["conjuncts"][0][0]
        assert c["op"]      == "only"
        assert c["negated"] is True

    def test_not_only_output_shape(self):
        r = ok(parse_to_constraints('NOT WHAT.color ONLY "Red"'))
        c = r["conjuncts"][0][0]
        assert c["category"] == "WHAT"
        assert c["field"]    == "color"
        assert c["value"]    == "Red"
        assert c["op"]       == "only"
        assert c["negated"]  is True

    def test_not_only_missing_value_is_error(self):
        r = fail(parse("WHAT.color ONLY"))
        assert r["success"] is False

    def test_only_without_dimension_field_is_error(self):
        r = fail(parse('ONLY "Red"'))
        assert r["success"] is False

    def test_only_with_operator_after_is_error(self):
        # ONLY = "Red" is not valid — ONLY takes a value directly, not an operator
        r = fail(parse('WHAT.color ONLY = "Red"'))
        assert r["success"] is False
