"""A closed expression language for schema-declared derived fields, e.g. `surprise: actual - forecast`.

A dataset schema is config a strategy trusts implicitly, so the text that computes a derived
field must never become a code-execution surface. This module is therefore a real tokenizer
plus a recursive-descent parser -- never `eval`, `exec`, `ast.literal_eval`, or `compile()` --
and every name or function it does not recognize is rejected at `compile_expr` time, before the
expression ever touches a record, rather than failing (or worse, silently returning null)
partway through a production run.

The language is small on purpose: `+ - * /`, parentheses, unary minus, decimal literals, bare
field names, and a fixed set of history-aware functions (`zscore`, `lag`, `diff`, `pct_rank`,
`since`, `until`). It has no variables, no branching, and no way to call anything but those six
functions, so a compiled expression's set of possible behaviors can be reasoned about by reading
the schema alone.

Two invariants hold everywhere: a `None` (missing) input anywhere makes the whole expression
`None` -- missing data must never quietly become zero -- and any arithmetic failure (division by
zero, overflow, underflow, or any other `decimal` signal the fixed context traps) yields `None`
rather than raising, because a schema author should not have to prove a denominator is nonzero,
or a product bounded, for every record that will ever exist.

Contract for `since`/`until`: there is no external clock available to a pure expression, so both
functions read `known_at` from the PAYLOAD MAPPING passed into the compiled `Expr` at evaluation
time -- not from any `Record` envelope object. The caller (the pipeline that evaluates derived
fields against a dataset's records) is responsible for merging the record's envelope `known_at`
into the payload dict it hands to a compiled expression; without that, `since`/`until` return
`None` rather than guessing a reference time.
"""
from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Context, Decimal, DivisionByZero, InvalidOperation, Overflow, Underflow

from hubread.errors import SchemaError
from hubread.record import decimal_str

#: The arithmetic context every derived-field evaluation shares. A fixed precision means the
#: same expression produces the same digits regardless of which host or Python build runs it.
CTX = Context(prec=28)

#: Every `decimal` signal that a fixed context can raise out of ordinary arithmetic (division by
#: zero, an invalid combination such as 0/0, overflow past `Emax`, or underflow past `Emin`).
#: Caught wherever this module performs arithmetic, so a compiled `Expr` never raises on bad
#: numbers -- it returns `None`, the same way it does for missing data.
_ARITH_ERRORS = (DivisionByZero, InvalidOperation, Overflow, Underflow)

#: What the caller passes an evaluated expression: the current payload, and prior payloads for
#: the same dataset/key in ascending `known_at` order (oldest first).
Payload = Mapping[str, object]
History = Sequence[Payload]

#: The compiled form of an expression: total, exception-free (bar programmer error), and closed
#: over nothing but the parsed expression tree -- no reference back to the source text.
Expr = Callable[[Payload, History], "str | None"]

#: The function names this language reserves. A schema loader should check a dataset's declared
#: field names against this set at load time and reject a collision there -- with a clear error
#: naming the field -- rather than let a schema author discover the collision only when an
#: expression referencing that field name is rejected as "unknown" the first time it is used.
RESERVED_EXPRESSION_NAMES = frozenset({"zscore", "lag", "diff", "pct_rank", "since", "until"})

_TOKEN_RE = re.compile(
    r"""
    (?P<NUMBER>\d+(\.\d+)?)
  | (?P<NAME>[A-Za-z_][A-Za-z0-9_]*)
  | (?P<OP>[+\-*/(),=])
  | (?P<WS>\s+)
    """,
    re.VERBOSE,
)


@dataclass(frozen=True, slots=True)
class _Token:
    """One lexeme plus the position it came from, so a parse error can point at it."""

    kind: str
    text: str
    pos: int


def _tokenize(expr: str) -> list[_Token]:
    """Splits expression text into tokens, raising `SchemaError` on the first byte it cannot read.

    Anything the regex does not match is rejected here rather than being handed further down
    the pipeline as an opaque string -- a schema author gets a position, not a stack trace.
    """
    tokens: list[_Token] = []
    pos = 0
    while pos < len(expr):
        m = _TOKEN_RE.match(expr, pos)
        if m is None:
            raise SchemaError(f"cannot tokenize expression at position {pos}: {expr[pos:pos + 10]!r}")
        kind = m.lastgroup
        assert kind is not None
        text = m.group()
        if kind != "WS":
            tokens.append(_Token(kind, text, pos))
        pos = m.end()
    return tokens


# -- AST ----------------------------------------------------------------------------------


class _Node:
    """Base of the parsed expression tree. Every node knows how to evaluate itself."""

    def names(self) -> frozenset[str]:
        """Field names this node (and its children) read from a payload."""
        raise NotImplementedError

    def eval(self, payload: Payload, history: History) -> Decimal | None:
        """Evaluate against one payload and its history, `None` on any missing input."""
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class _Literal(_Node):
    """A decimal literal parsed straight from the expression text; it reads no field."""

    value: Decimal

    def names(self) -> frozenset[str]:
        return frozenset()

    def eval(self, payload: Payload, history: History) -> Decimal | None:
        return self.value


@dataclass(frozen=True, slots=True)
class _FieldRef(_Node):
    """A bare field name; evaluates to that field's value in the current payload, or `None`."""

    name: str

    def names(self) -> frozenset[str]:
        return frozenset({self.name})

    def eval(self, payload: Payload, history: History) -> Decimal | None:
        return _to_decimal(payload.get(self.name))


@dataclass(frozen=True, slots=True)
class _Unary(_Node):
    """Unary minus: negates its operand, or propagates `None`/an arithmetic failure as `None`."""

    operand: _Node

    def names(self) -> frozenset[str]:
        return self.operand.names()

    def eval(self, payload: Payload, history: History) -> Decimal | None:
        value = self.operand.eval(payload, history)
        if value is None:
            return None
        try:
            return CTX.minus(value)
        except _ARITH_ERRORS:
            return None


@dataclass(frozen=True, slots=True)
class _BinOp(_Node):
    """One of `+ - * /` applied to two subexpressions, total over missing input and bad
    arithmetic alike: either operand being `None`, or the operation itself failing (division by
    zero, overflow, underflow), yields `None` rather than raising."""

    op: str
    left: _Node
    right: _Node

    def names(self) -> frozenset[str]:
        return self.left.names() | self.right.names()

    def eval(self, payload: Payload, history: History) -> Decimal | None:
        left = self.left.eval(payload, history)
        right = self.right.eval(payload, history)
        if left is None or right is None:
            return None
        try:
            if self.op == "+":
                return CTX.add(left, right)
            if self.op == "-":
                return CTX.subtract(left, right)
            if self.op == "*":
                return CTX.multiply(left, right)
            if self.op == "/":
                return CTX.divide(left, right)
        except _ARITH_ERRORS:
            return None
        raise AssertionError(f"unreachable operator {self.op!r}")


@dataclass(frozen=True, slots=True)
class _Call(_Node):
    """A history-aware function call. `field` is the payload field the function reads;
    `int_args` are its integer arguments in a function-specific, fixed order."""

    func: str
    field: str
    int_args: tuple[int, ...]

    def names(self) -> frozenset[str]:
        return frozenset({self.field})

    def eval(self, payload: Payload, history: History) -> Decimal | None:
        if self.func == "lag":
            (n,) = self.int_args
            return _lag(self.field, n, history)
        if self.func == "diff":
            (n,) = self.int_args
            current = _to_decimal(payload.get(self.field))
            prior = _lag(self.field, n, history)
            if current is None or prior is None:
                return None
            try:
                return CTX.subtract(current, prior)
            except _ARITH_ERRORS:
                return None
        if self.func == "zscore":
            window, min_obs = self.int_args
            return _zscore(self.field, window, min_obs, payload, history)
        if self.func == "pct_rank":
            (window,) = self.int_args
            return _pct_rank(self.field, window, payload, history)
        if self.func in ("since", "until"):
            sign = 1 if self.func == "since" else -1
            return _since_until(self.field, sign, payload)
        raise AssertionError(f"unreachable function {self.func!r}")


# -- function semantics ---------------------------------------------------------------------


def _to_decimal(value: object) -> Decimal | None:
    """Reads one payload value as a `Decimal`, or `None` if it is missing or not numeric.

    A bool is deliberately excluded even though Python lets `int(True)` work -- a schema field
    is typed as numeric or it is not, and `True` sliding through as `1` would be a silent
    misread of the schema rather than a real value.
    """
    if value is None or isinstance(value, bool):
        return None
    try:
        return CTX.create_decimal(str(value))
    except _ARITH_ERRORS:
        return None


def _recent_window(field: str, window: int, history: History) -> list[Decimal] | None:
    """The most recent `window` historical values for `field`, oldest first.

    `None` (rather than a short list) marks that some value inside the window was itself
    missing -- a statistic built on top of a hole in the data is not a number, it is a guess.
    """
    tail = list(history[-window:]) if window > 0 else []
    values: list[Decimal] = []
    for payload in tail:
        value = _to_decimal(payload.get(field))
        if value is None:
            return None
        values.append(value)
    return values


def _lag(field: str, n: int, history: History) -> Decimal | None:
    """`lag(field, n)`: the value n payloads back; `lag(x, 1)` is the immediately prior payload."""
    if n <= 0 or n > len(history):
        return None
    return _to_decimal(history[-n].get(field))


def _zscore(field: str, window: int, min_obs: int, payload: Payload, history: History) -> Decimal | None:
    """`zscore(field, window=N, min_obs=M)`: how many sample standard deviations the CURRENT
    value sits from the mean of the preceding N historical values. The current value is what is
    being scored; it is never itself a member of the window.

    Sample standard deviation (n-1 denominator) is used rather than population (n), because the
    window is treated as a sample drawn from the field's ongoing behavior rather than the whole
    of it -- the conventional choice for a rolling statistic. Returns `None` when fewer than
    `min_obs` usable historical observations exist, or when the window is a constant series
    (stdev of zero): a z-score against no spread is not meaningful, not infinite.
    """
    current = _to_decimal(payload.get(field))
    values = _recent_window(field, window, history)
    if current is None or values is None or len(values) < min_obs or len(values) < 2:
        return None
    try:
        n = len(values)
        total = Decimal(0)
        for v in values:
            total = CTX.add(total, v)
        mean = CTX.divide(total, Decimal(n))
        variance_num = Decimal(0)
        for v in values:
            delta = CTX.subtract(v, mean)
            variance_num = CTX.add(variance_num, CTX.multiply(delta, delta))
        variance = CTX.divide(variance_num, Decimal(n - 1))
        stdev = variance.sqrt(CTX)
        if stdev == 0:
            return None
        return CTX.divide(CTX.subtract(current, mean), stdev)
    except _ARITH_ERRORS:
        return None


def _pct_rank(field: str, window: int, payload: Payload, history: History) -> Decimal | None:
    """`pct_rank(field, window=N)`: fraction, in `[0, 1]`, of the window strictly below the
    current value. An empty comparison (window of zero usable observations) is undefined, not
    zero, so it returns `None` rather than asserting the current value ranks at the top.
    """
    values = _recent_window(field, window, history)
    current = _to_decimal(payload.get(field))
    if values is None or current is None or len(values) == 0:
        return None
    below = sum(1 for v in values if v < current)
    try:
        return CTX.divide(Decimal(below), Decimal(len(values)))
    except _ARITH_ERRORS:
        return None


def _since_until(field: str, sign: int, payload: Payload) -> Decimal | None:
    """`since(ts_field)` / `until(ts_field)`: milliseconds between the payload's `known_at` and
    its named timestamp field, in the direction the function name implies.

    There is no external clock available to a pure expression, so `known_at` -- the payload's
    own point-in-time marker -- is the only reference this function can use without breaking
    determinism. When the payload carries no `known_at` (it evaluated outside the envelope,
    e.g. in a test payload), the result is `None`: a reference-free "since" is not zero elapsed
    time, it is unknown elapsed time.
    """
    known_at = _to_decimal(payload.get("known_at"))
    ts = _to_decimal(payload.get(field))
    if known_at is None or ts is None:
        return None
    try:
        if sign > 0:
            return CTX.subtract(known_at, ts)
        return CTX.subtract(ts, known_at)
    except _ARITH_ERRORS:
        return None


# -- parser ---------------------------------------------------------------------------------


class _Parser:
    """Recursive-descent parser over a token list -- the untrusted-text boundary this module
    exists to police. Grammar, precedence low to high:

        expr   := term (('+' | '-') term)*
        term   := factor (('*' | '/') factor)*
        factor := '-' factor | atom
        atom   := NUMBER | call | NAME | '(' expr ')'
        call   := NAME '(' arg (',' arg)* ')'
    """

    def __init__(self, tokens: list[_Token], source: str):
        self._tokens = tokens
        self._source = source
        self._pos = 0

    def parse(self) -> _Node:
        node = self._expr()
        if self._pos != len(self._tokens):
            tok = self._tokens[self._pos]
            raise SchemaError(f"unexpected token {tok.text!r} in expression {self._source!r}")
        return node

    def _peek(self) -> _Token | None:
        return self._tokens[self._pos] if self._pos < len(self._tokens) else None

    def _advance(self) -> _Token:
        tok = self._peek()
        if tok is None:
            raise SchemaError(f"unexpected end of expression {self._source!r}")
        self._pos += 1
        return tok

    def _expect_op(self, text: str) -> _Token:
        tok = self._peek()
        if tok is None or tok.kind != "OP" or tok.text != text:
            got = tok.text if tok is not None else "end of expression"
            raise SchemaError(f"expected {text!r} but found {got!r} in expression {self._source!r}")
        return self._advance()

    def _expr(self) -> _Node:
        node = self._term()
        while (tok := self._peek()) is not None and tok.kind == "OP" and tok.text in ("+", "-"):
            self._advance()
            node = _BinOp(tok.text, node, self._term())
        return node

    def _term(self) -> _Node:
        node = self._factor()
        while (tok := self._peek()) is not None and tok.kind == "OP" and tok.text in ("*", "/"):
            self._advance()
            node = _BinOp(tok.text, node, self._factor())
        return node

    def _factor(self) -> _Node:
        tok = self._peek()
        if tok is not None and tok.kind == "OP" and tok.text == "-":
            self._advance()
            return _Unary(self._factor())
        return self._atom()

    def _atom(self) -> _Node:
        tok = self._peek()
        if tok is None:
            raise SchemaError(f"unexpected end of expression {self._source!r}")
        if tok.kind == "NUMBER":
            self._advance()
            try:
                return _Literal(CTX.create_decimal(tok.text))
            except InvalidOperation as e:
                raise SchemaError(f"not a decimal literal: {tok.text!r}") from e
        if tok.kind == "OP" and tok.text == "(":
            self._advance()
            node = self._expr()
            self._expect_op(")")
            return node
        if tok.kind == "NAME":
            self._advance()
            nxt = self._peek()
            if nxt is not None and nxt.kind == "OP" and nxt.text == "(":
                return self._call(tok)
            if tok.text in RESERVED_EXPRESSION_NAMES:
                raise SchemaError(
                    f"{tok.text!r} is a reserved function name and cannot be used as a field "
                    f"reference, in expression {self._source!r}"
                )
            return _FieldRef(tok.text)
        raise SchemaError(f"unexpected token {tok.text!r} in expression {self._source!r}")

    def _call(self, name_tok: _Token) -> _Node:
        func = name_tok.text
        if func not in RESERVED_EXPRESSION_NAMES:
            raise SchemaError(f"unknown function {func!r} in expression {self._source!r}")
        self._expect_op("(")
        field_tok = self._advance()
        if field_tok.kind != "NAME":
            raise SchemaError(f"{func}() expects a field name as its first argument, in {self._source!r}")
        field = field_tok.text
        int_args = self._call_args_for(func)
        self._expect_op(")")
        return _Call(func, field, int_args)

    def _call_args_for(self, func: str) -> tuple[int, ...]:
        if func == "lag" or func == "diff":
            self._expect_op(",")
            return (self._positional_int(),)
        if func == "zscore":
            self._expect_op(",")
            kwargs = self._named_int_kwargs(frozenset({"window", "min_obs"}))
            return (kwargs["window"], kwargs["min_obs"])
        if func == "pct_rank":
            self._expect_op(",")
            kwargs = self._named_int_kwargs(frozenset({"window"}))
            return (kwargs["window"],)
        if func in ("since", "until"):
            return ()
        raise AssertionError(f"unreachable function {func!r}")

    def _positional_int(self) -> int:
        tok = self._advance()
        if tok.kind != "NUMBER" or "." in tok.text:
            raise SchemaError(f"expected an integer argument, found {tok.text!r} in {self._source!r}")
        return int(tok.text)

    def _named_int_kwargs(self, names: frozenset[str]) -> dict[str, int]:
        """Parses `name=INT[, name=INT ...]` for exactly `names`, accepted in any order.

        `zscore(x, min_obs=4, window=5)` and `zscore(x, window=5, min_obs=4)` must mean the same
        thing -- a schema author should not have to memorize an internal argument order for a
        keyword-only call. Raises on an unrecognized, duplicate, or missing keyword.
        """
        found: dict[str, int] = {}
        while True:
            name_tok = self._advance()
            if name_tok.kind != "NAME" or name_tok.text not in names:
                raise SchemaError(
                    f"expected one of keyword arguments {sorted(names)}, found {name_tok.text!r} "
                    f"in {self._source!r}"
                )
            if name_tok.text in found:
                raise SchemaError(f"duplicate keyword argument {name_tok.text!r} in {self._source!r}")
            self._expect_op("=")
            found[name_tok.text] = self._positional_int()
            nxt = self._peek()
            if nxt is not None and nxt.kind == "OP" and nxt.text == ",":
                self._advance()
                continue
            break
        missing = names - found.keys()
        if missing:
            raise SchemaError(f"missing keyword argument(s) {sorted(missing)} in {self._source!r}")
        return found


# -- public API -------------------------------------------------------------------------------


def referenced_names(expr: str) -> frozenset[str]:
    """The set of payload field names an expression reads, for a schema to validate dependencies
    against a dataset's declared fields before anything ever tries to evaluate it.
    """
    tokens = _tokenize(expr)
    node = _Parser(tokens, expr).parse()
    return node.names()


def compile_expr(expr: str) -> Expr:
    """Parses and validates `expr` once, returning a closure that evaluates it against a payload
    and its history.

    Compiling ahead of evaluation is what lets an unknown name or function raise `SchemaError`
    at schema-load time -- when a human is looking at the config -- instead of at evaluation
    time, deep inside a pipeline run where it would surface as a silently missing field.
    """
    tokens = _tokenize(expr)
    node = _Parser(tokens, expr).parse()

    def run(payload: Payload, history: History) -> str | None:
        result = node.eval(payload, history)
        if result is None:
            return None
        return decimal_str(result)

    return run
