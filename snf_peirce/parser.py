"""
parser.py — Peirce Parser Reference Implementation (Python port)

Port of peirce_parser.cjs v1.1. Conformance surface matches JS exactly:
  - Same output shapes (plain dicts/lists, not dataclasses)
  - Same error envelope: {"success": False, "error": str, "position": int, "token": any}
  - Same discovery expression detection
  - Same constraint field names at the public boundary (category, field, op, value)

Public API
----------
parse(input: str) -> dict
    Returns raw parse result. Mirrors JS parse().
    On success (query):
        {"success": True, "type": "query", "conjuncts": [conjunct, ...]}
        where conjunct = {"constraints": [constraint_node, ...]}
    On success (discovery):
        {"success": True, "type": "discovery", "scope": "all"}
        {"success": True, "type": "discovery", "scope": "dimension", "dimension": "WHO"}
        {"success": True, "type": "discovery", "scope": "field", "dimension": "WHO", "field": "role"}
    On failure:
        {"success": False, "error": str, "position": int, "token": any}

parse_to_constraints(input: str) -> dict
    Returns engine-facing flattened form. Mirrors JS parseToConstraints().
    On success (query):
        {"success": True, "type": "query", "conjuncts": [[constraint, ...], ...]}
        where constraint = {"category": str, "field": str, "op": str, "value": any}
        and optionally: "value2", "negated"
    On success (discovery):
        passthrough of parse() discovery result
    On failure:
        same error envelope as parse()

Naming
------
Python-native snake_case is primary. JS-style camelCase aliases provided:
    parseToConstraints = parse_to_constraints

Cross-language conformance
--------------------------
The output of parse_to_constraints() must be identical in structure to
JS parseToConstraints() for all valid inputs. This is the conformance
boundary Reckoner depends on.
"""

# ─────────────────────────────────────────────────────────────────────────────
# Token type constants
# ─────────────────────────────────────────────────────────────────────────────

class T:
    DIMENSION  = "DIMENSION"
    DOT        = "DOT"
    PIPE       = "PIPE"
    STAR       = "STAR"
    IDENTIFIER = "IDENTIFIER"
    OPERATOR   = "OPERATOR"
    KEYWORD    = "KEYWORD"
    STRING     = "STRING"
    NUMBER     = "NUMBER"
    BOOLEAN    = "BOOLEAN"
    LPAREN     = "LPAREN"
    RPAREN     = "RPAREN"
    EOF        = "EOF"


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

DIMENSIONS = {"WHO", "WHAT", "WHEN", "WHERE", "WHY", "HOW"}
KEYWORDS   = {"AND", "OR", "NOT", "BETWEEN", "CONTAINS", "PREFIX", "TRUE", "FALSE", "ONLY"}

# Two-char operators must come before single-char to match correctly
SYMBOLIC_OPERATORS = [">=", "<=", "!=", "=", ">", "<"]

OPERATOR_MAP = {
    "=":        "eq",
    "!=":       "not_eq",
    ">":        "gt",
    "<":        "lt",
    ">=":       "gte",
    "<=":       "lte",
    "CONTAINS": "contains",
    "PREFIX":   "prefix",
}


# ─────────────────────────────────────────────────────────────────────────────
# Lexer
# ─────────────────────────────────────────────────────────────────────────────

def tokenize(input_str):
    """
    Tokenize a Peirce query string.

    Returns:
        {"success": True,  "tokens": [...]}
        {"success": False, "error": str, "position": int, "token": any}
    """
    tokens = []
    i = 0
    n = len(input_str)

    while i < n:
        # Skip whitespace
        if input_str[i].isspace():
            i += 1
            continue

        start = i

        # Quoted strings — single or double quote
        if input_str[i] in ('"', "'"):
            quote = input_str[i]
            i += 1
            value = []
            while i < n and input_str[i] != quote:
                if input_str[i] == '\\' and i + 1 < n:
                    i += 1
                    value.append(input_str[i])
                else:
                    value.append(input_str[i])
                i += 1
            if i >= n:
                return {"success": False, "error": "Unterminated string literal", "position": start, "token": quote}
            i += 1  # consume closing quote
            tokens.append({"type": T.STRING, "value": "".join(value), "position": start})
            continue

        # Star — wildcard / discovery
        if input_str[i] == '*':
            tokens.append({"type": T.STAR, "value": "*", "position": start})
            i += 1
            continue

        # Pipe — discovery separator
        if input_str[i] == '|':
            tokens.append({"type": T.PIPE, "value": "|", "position": start})
            i += 1
            continue

        # Symbolic operators — check two-char before one-char
        matched_op = None
        for op in SYMBOLIC_OPERATORS:
            if input_str[i:i + len(op)] == op:
                matched_op = op
                break
        if matched_op:
            tokens.append({"type": T.OPERATOR, "value": matched_op, "position": start})
            i += len(matched_op)
            continue

        # Dot
        if input_str[i] == '.':
            tokens.append({"type": T.DOT, "value": ".", "position": start})
            i += 1
            continue

        # Parens
        if input_str[i] == '(':
            tokens.append({"type": T.LPAREN, "value": "(", "position": start})
            i += 1
            continue
        if input_str[i] == ')':
            tokens.append({"type": T.RPAREN, "value": ")", "position": start})
            i += 1
            continue

        # Numbers — including negative numbers
        if input_str[i].isdigit() or (input_str[i] == '-' and i + 1 < n and input_str[i + 1].isdigit()):
            num_chars = []
            if input_str[i] == '-':
                num_chars.append('-')
                i += 1
            while i < n and input_str[i].isdigit():
                num_chars.append(input_str[i])
                i += 1
            if i < n and input_str[i] == '.':
                num_chars.append('.')
                i += 1
                while i < n and input_str[i].isdigit():
                    num_chars.append(input_str[i])
                    i += 1
            tokens.append({"type": T.NUMBER, "value": float("".join(num_chars)), "position": start})
            continue

        # Identifiers, keywords, dimensions, booleans
        if input_str[i].isalpha() or input_str[i] == '_':
            word_chars = []
            while i < n and (input_str[i].isalnum() or input_str[i] == '_'):
                word_chars.append(input_str[i])
                i += 1
            word  = "".join(word_chars)
            upper = word.upper()
            if upper in DIMENSIONS:
                tokens.append({"type": T.DIMENSION, "value": upper, "position": start})
            elif upper in ("TRUE", "FALSE"):
                tokens.append({"type": T.BOOLEAN, "value": upper == "TRUE", "position": start})
            elif upper in ("CONTAINS", "PREFIX"):
                tokens.append({"type": T.OPERATOR, "value": upper, "position": start})
            elif upper in KEYWORDS:
                tokens.append({"type": T.KEYWORD, "value": upper, "position": start})
            else:
                tokens.append({"type": T.IDENTIFIER, "value": word.lower(), "position": start})
            continue

        return {"success": False, "error": f"Unexpected character '{input_str[i]}'", "position": i, "token": input_str[i]}

    tokens.append({"type": T.EOF, "value": None, "position": n})
    return {"success": True, "tokens": tokens}


# ─────────────────────────────────────────────────────────────────────────────
# Discovery expression detection
#
# Checked before full parse. Three patterns (token count includes EOF):
#   *                  →  [STAR, EOF]                      scope: all
#   DIM|*              →  [DIM, PIPE, STAR, EOF]           scope: dimension
#   DIM|field|*        →  [DIM, PIPE, ID, PIPE, STAR, EOF] scope: field
# ─────────────────────────────────────────────────────────────────────────────

def _try_parse_discovery(tokens):
    """
    Attempt to match a discovery expression from the token list.
    Returns a result dict if matched, None if not a discovery expression.
    """
    # * EOF
    if (len(tokens) == 2
            and tokens[0]["type"] == T.STAR
            and tokens[1]["type"] == T.EOF):
        return {"success": True, "type": "discovery", "scope": "all"}

    # DIMENSION | * EOF
    if (len(tokens) == 4
            and tokens[0]["type"] == T.DIMENSION
            and tokens[1]["type"] == T.PIPE
            and tokens[2]["type"] == T.STAR
            and tokens[3]["type"] == T.EOF):
        return {"success": True, "type": "discovery", "scope": "dimension", "dimension": tokens[0]["value"]}

    # DIMENSION | IDENTIFIER | * EOF
    if (len(tokens) == 6
            and tokens[0]["type"] == T.DIMENSION
            and tokens[1]["type"] == T.PIPE
            and tokens[2]["type"] == T.IDENTIFIER
            and tokens[3]["type"] == T.PIPE
            and tokens[4]["type"] == T.STAR
            and tokens[5]["type"] == T.EOF):
        return {
            "success":   True,
            "type":      "discovery",
            "scope":     "field",
            "dimension": tokens[0]["value"],
            "field":     tokens[2]["value"],
        }

    return None  # not a discovery expression


# ─────────────────────────────────────────────────────────────────────────────
# Recursive descent parser
# ─────────────────────────────────────────────────────────────────────────────

class _Parser:
    """
    Recursive descent parser for Peirce query expressions.
    Not intended for direct use — call parse() or parse_to_constraints().
    """

    def __init__(self, tokens):
        self._tokens = tokens
        self._pos    = 0

    def _peek(self):
        return self._tokens[self._pos]

    def _consume(self):
        tok = self._tokens[self._pos]
        self._pos += 1
        return tok

    def _expect(self, type_, value=None):
        tok = self._peek()
        if tok["type"] != type_:
            label = f" '{value}'" if value is not None else ""
            got   = tok["value"] if tok["value"] is not None else "EOF"
            return {"success": False, "error": f"Expected {type_}{label} but got '{got}'", "position": tok["position"], "token": tok["value"]}
        if value is not None and tok["value"] != value:
            return {"success": False, "error": f"Expected '{value}' but got '{tok['value']}'", "position": tok["position"], "token": tok["value"]}
        return {"success": True, "token": self._consume()}

    # query := conjunct (OR conjunct)*
    def parse_query(self):
        conjuncts = []
        first = self._parse_conjunct()
        if not first["success"]:
            return first
        conjuncts.append(first["conjunct"])

        while self._peek()["type"] == T.KEYWORD and self._peek()["value"] == "OR":
            self._consume()
            nxt = self._parse_conjunct()
            if not nxt["success"]:
                return nxt
            conjuncts.append(nxt["conjunct"])

        if self._peek()["type"] != T.EOF:
            tok = self._peek()
            got = tok["value"] if tok["value"] is not None else "EOF"
            return {"success": False, "error": f"Unexpected token '{got}' — expected AND, OR, or end of query", "position": tok["position"], "token": tok["value"]}

        return {"success": True, "type": "query", "conjuncts": conjuncts}

    # conjunct := constraint (AND constraint)*
    def _parse_conjunct(self):
        constraints = []
        first = self._parse_constraint()
        if not first["success"]:
            return first
        constraints.append(first["constraint"])

        while self._peek()["type"] == T.KEYWORD and self._peek()["value"] == "AND":
            self._consume()
            nxt = self._parse_constraint()
            if not nxt["success"]:
                return nxt
            constraints.append(nxt["constraint"])

        return {"success": True, "conjunct": {"constraints": constraints}}

    # constraint := NOT constraint
    #             | ( conjunct )
    #             | dimension . field operator value
    #             | dimension . field BETWEEN value AND value
    def _parse_constraint(self):
        tok = self._peek()

        # NOT constraint
        if tok["type"] == T.KEYWORD and tok["value"] == "NOT":
            self._consume()
            inner = self._parse_constraint()
            if not inner["success"]:
                return inner
            negated = dict(inner["constraint"])
            negated["negated"] = True
            return {"success": True, "constraint": negated}

        # ( conjunct )
        if tok["type"] == T.LPAREN:
            self._consume()
            inner = self._parse_conjunct()
            if not inner["success"]:
                return inner
            close = self._expect(T.RPAREN)
            if not close["success"]:
                # Helpful error: OR inside parens
                if self._peek()["type"] == T.KEYWORD and self._peek()["value"] == "OR":
                    msg = "\n".join([
                        "OR inside parentheses is not valid in Peirce.",
                        "To express multiple values for the same field, use repetition:",
                        "  WHAT.matter_type = 'litigation' AND WHAT.matter_type = 'ip'",
                        "To express OR across full constraint sets, use top-level DNF:",
                        "  WHAT.matter_type = 'litigation' AND WHEN.year = '2023'",
                        "  OR",
                        "  WHAT.matter_type = 'ip' AND WHEN.year = '2023'",
                    ])
                    p = self._peek()
                    return {"success": False, "error": msg, "position": p["position"], "token": p["value"]}
                return close
            return {"success": True, "constraint": {"grouped": True, "conjunct": inner["conjunct"]}}

        # Must start with a dimension
        if tok["type"] != T.DIMENSION:
            got = tok["value"] if tok["value"] is not None else "EOF"
            return {"success": False, "error": f"Expected dimension (WHO, WHAT, WHEN, WHERE, WHY, HOW) but got '{got}'", "position": tok["position"], "token": tok["value"]}

        dimension = self._consume()["value"]

        dot = self._expect(T.DOT)
        if not dot["success"]:
            return dot

        field_tok = self._peek()
        if field_tok["type"] != T.IDENTIFIER:
            got = field_tok["value"] if field_tok["value"] is not None else "EOF"
            return {"success": False, "error": f"Expected field name after '{dimension}.' but got '{got}'", "position": field_tok["position"], "token": field_tok["value"]}
        field = self._consume()["value"]

        # ONLY
        if self._peek()["type"] == T.KEYWORD and self._peek()["value"] == "ONLY":
            self._consume()
            val = self._parse_value()
            if not val["success"]:
                return val
            return {"success": True, "constraint": {"dimension": dimension, "field": field, "operator": "only", "value": val["value"], "negated": False}}

        # BETWEEN
        if self._peek()["type"] == T.KEYWORD and self._peek()["value"] == "BETWEEN":
            return self._parse_between(dimension, field)

        # operator
        op_tok = self._peek()
        if op_tok["type"] != T.OPERATOR:
            got = op_tok["value"] if op_tok["value"] is not None else "EOF"
            return {"success": False, "error": f"Expected operator after '{dimension}.{field}' but got '{got}'", "position": op_tok["position"], "token": op_tok["value"]}
        self._consume()
        operator = OPERATOR_MAP[op_tok["value"]]

        val = self._parse_value()
        if not val["success"]:
            return val

        return {"success": True, "constraint": {"dimension": dimension, "field": field, "operator": operator, "value": val["value"], "negated": False}}

    def _parse_between(self, dimension, field):
        self._consume()  # consume BETWEEN
        v1 = self._parse_value()
        if not v1["success"]:
            return v1
        and_tok = self._peek()
        if and_tok["type"] != T.KEYWORD or and_tok["value"] != "AND":
            got = and_tok["value"] if and_tok["value"] is not None else "EOF"
            return {"success": False, "error": f"Expected AND after first BETWEEN value but got '{got}'", "position": and_tok["position"], "token": and_tok["value"]}
        self._consume()
        v2 = self._parse_value()
        if not v2["success"]:
            return v2
        return {"success": True, "constraint": {"dimension": dimension, "field": field, "operator": "between", "value": v1["value"], "value2": v2["value"], "negated": False}}

    def _parse_value(self):
        tok = self._peek()
        if tok["type"] in (T.STRING, T.NUMBER, T.BOOLEAN):
            self._consume()
            return {"success": True, "value": tok["value"]}
        got = tok["value"] if tok["value"] is not None else "EOF"
        return {"success": False, "error": f"Expected a value (string, number, or boolean) but got '{got}'", "position": tok["position"], "token": tok["value"]}


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def parse(input_str):
    """
    Parse a Peirce query string. Returns the raw parse result.

    Mirrors JS parse(). Output is plain dicts — no dataclasses.

    Returns:
        On success (query):
            {"success": True, "type": "query", "conjuncts": [...]}
        On success (discovery):
            {"success": True, "type": "discovery", "scope": ..., ...}
        On failure:
            {"success": False, "error": str, "position": int, "token": any}
    """
    if not isinstance(input_str, str) or not input_str.strip():
        return {"success": False, "error": "Input must be a non-empty string", "position": 0, "token": None}

    lex = tokenize(input_str.strip())
    if not lex["success"]:
        return lex

    # Discovery expressions are detected before the full parse
    discovery = _try_parse_discovery(lex["tokens"])
    if discovery is not None:
        return discovery

    return _Parser(lex["tokens"]).parse_query()


def parse_to_constraints(input_str):
    """
    Parse a Peirce query string and return engine-facing flattened constraints.

    Mirrors JS parseToConstraints(). This is the function Reckoner calls.

    Constraint shape (Portolan/Reckoner format):
        {
            "category": str,         # dimension name — matches JS "category"
            "field":    str,
            "op":       str,         # "eq", "not_eq", "gt", "lt", "gte", "lte",
                                     # "contains", "prefix", "between"
            "value":    str|int|float|bool,
            "value2":   str|int|float,   # only for "between"
            "negated":  bool,            # only when True
        }

    Returns:
        On success (query):
            {"success": True, "type": "query", "conjuncts": [[constraint, ...], ...]}
        On success (discovery):
            passthrough of parse() result
        On failure:
            {"success": False, "error": str, "position": int, "token": any}
    """
    parsed = parse(input_str)
    if not parsed["success"]:
        return parsed

    # Discovery expressions pass through — caller handles routing
    if parsed["type"] == "discovery":
        return parsed

    def to_portolan_constraint(c):
        result = {
            "category": c["dimension"],
            "field":    c["field"],
            "op":       c["operator"],
            "value":    c["value"],
        }
        if "value2" in c:
            result["value2"] = c["value2"]
        if c.get("negated"):
            result["negated"] = c["negated"]
        return result

    def flatten_conjunct(conjunct):
        result = []
        for c in conjunct["constraints"]:
            if c.get("grouped"):
                result.extend(flatten_conjunct(c["conjunct"]))
            else:
                result.append(to_portolan_constraint(c))
        return result

    return {
        "success":   True,
        "type":      "query",
        "conjuncts": [flatten_conjunct(cj) for cj in parsed["conjuncts"]],
    }


# JS-style camelCase alias — for cross-language comparisons and documentation
parseToConstraints = parse_to_constraints
