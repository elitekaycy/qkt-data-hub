"""A restricted YAML-subset parser, stdlib only.

The hub ships with zero runtime dependencies for the same reason its sibling qkt-guardrails
does: every dependency is a way for a system that guards real money to fail, and a dataset
schema is small enough not to need a general parser. This one is deliberately larger than the
guardian's -- schemas nest and carry lists -- but it is still a subset, and anything outside it
raises with a line number rather than guessing.

Supported:
  - block mappings, nested to any depth by consistent indentation
  - block sequences (`- item`), including sequences of mappings (`- key: value`)
  - flow sequences and mappings on one line: `[a, b]`, `{a: 1, b: 2}`, nested
  - scalars: single/double-quoted strings, ints, floats, booleans, `null`/`~`, bare strings
  - `#` comments outside quotes; blank lines

Not supported, by design: anchors and aliases, multi-line scalars (`|`, `>`), multiple
documents, tags, complex keys. Each raises `SimpleYamlError`.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from hubread.errors import ConfigError


class SimpleYamlError(ConfigError):
    """A YAML document used a construct this parser deliberately does not support."""


_TRUE = {"true", "yes", "on"}
_FALSE = {"false", "no", "off"}
_NULL = {"null", "~", ""}


@dataclass(frozen=True)
class _Line:
    number: int
    indent: int
    text: str


def _strip_comment(line: str) -> str:
    quote: str | None = None
    for i, ch in enumerate(line):
        if quote is not None:
            if ch == quote:
                quote = None
        elif ch in ("'", '"'):
            quote = ch
        elif ch == "#" and (i == 0 or line[i - 1] in " \t"):
            return line[:i]
    return line


def _scan(text: str) -> list[_Line]:
    out: list[_Line] = []
    for number, raw in enumerate(text.splitlines(), start=1):
        if "\t" in raw[: len(raw) - len(raw.lstrip())]:
            raise SimpleYamlError(f"line {number}: tab in indentation; use spaces")
        stripped = _strip_comment(raw).rstrip()
        if not stripped.strip():
            continue
        if stripped.lstrip().startswith(("---", "...")):
            raise SimpleYamlError(f"line {number}: multiple documents are not supported")
        indent = len(stripped) - len(stripped.lstrip(" "))
        out.append(_Line(number=number, indent=indent, text=stripped.strip()))
    return out


def _split_key(text: str, number: int) -> tuple[str, str]:
    """Split `key: value` at the first colon that is outside quotes and brackets."""
    quote: str | None = None
    depth = 0
    for i, ch in enumerate(text):
        if quote is not None:
            if ch == quote:
                quote = None
            continue
        if ch in ("'", '"'):
            quote = ch
        elif ch in "[{":
            depth += 1
        elif ch in "]}":
            depth -= 1
        elif ch == ":" and depth == 0:
            if i + 1 < len(text) and text[i + 1] not in " \t":
                continue
            return text[:i].strip(), text[i + 1 :].strip()
    raise SimpleYamlError(f"line {number}: expected 'key: value', got {text!r}")


def _unquote(token: str) -> str | None:
    if len(token) >= 2 and token[0] == token[-1] and token[0] in ("'", '"'):
        body = token[1:-1]
        return body.replace('\\"', '"') if token[0] == '"' else body
    return None


def _scalar(token: str, number: int) -> Any:
    token = token.strip()
    quoted = _unquote(token)
    if quoted is not None:
        return quoted
    low = token.lower()
    if low in _NULL:
        return None
    if low in _TRUE:
        return True
    if low in _FALSE:
        return False
    try:
        return int(token)
    except ValueError:
        pass
    try:
        return float(token)
    except ValueError:
        pass
    if token.startswith(("&", "*", "!")):
        raise SimpleYamlError(f"line {number}: anchors, aliases and tags are not supported")
    if token in ("|", ">"):
        raise SimpleYamlError(f"line {number}: multi-line scalars are not supported")
    return token


def _split_flow(body: str, number: int) -> list[str]:
    """Split a flow body on top-level commas."""
    parts: list[str] = []
    depth = 0
    quote: str | None = None
    current: list[str] = []
    for ch in body:
        if quote is not None:
            current.append(ch)
            if ch == quote:
                quote = None
            continue
        if ch in ("'", '"'):
            quote = ch
            current.append(ch)
        elif ch in "[{":
            depth += 1
            current.append(ch)
        elif ch in "]}":
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
    if quote is not None:
        raise SimpleYamlError(f"line {number}: unterminated quote")
    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts


def _flow(token: str, number: int) -> Any:
    token = token.strip()
    if token.startswith("["):
        if not token.endswith("]"):
            raise SimpleYamlError(f"line {number}: unterminated flow sequence")
        return [_flow(p, number) for p in _split_flow(token[1:-1], number)]
    if token.startswith("{"):
        if not token.endswith("}"):
            raise SimpleYamlError(f"line {number}: unterminated flow mapping")
        out: dict[str, Any] = {}
        for part in _split_flow(token[1:-1], number):
            key, value = _split_key(part, number)
            name = _unquote(key) or key
            if name in out:
                raise SimpleYamlError(f"line {number}: duplicate key {name!r}")
            out[name] = _flow(value, number)
        return out
    return _scalar(token, number)


class _Parser:
    def __init__(self, lines: list[_Line]) -> None:
        self._lines = lines
        self._i = 0

    def _peek(self) -> _Line | None:
        return self._lines[self._i] if self._i < len(self._lines) else None

    def parse_block(self, indent: int) -> Any:
        line = self._peek()
        if line is None:
            return None
        return self._sequence(indent) if line.text.startswith("- ") or line.text == "-" else self._mapping(indent)

    def _mapping(self, indent: int) -> dict[str, Any]:
        out: dict[str, Any] = {}
        while (line := self._peek()) is not None and line.indent >= indent:
            if line.indent > indent:
                raise SimpleYamlError(f"line {line.number}: unexpected indentation")
            if line.text.startswith("- "):
                raise SimpleYamlError(f"line {line.number}: sequence item where a mapping key was expected")
            key, value = _split_key(line.text, line.number)
            name = _unquote(key) or key
            if name in out:
                raise SimpleYamlError(f"line {line.number}: duplicate key {name!r}")
            self._i += 1
            out[name] = self._value(value, indent, line.number)
        return out

    def _sequence(self, indent: int) -> list[Any]:
        out: list[Any] = []
        while (line := self._peek()) is not None and line.indent == indent and (
            line.text.startswith("- ") or line.text == "-"
        ):
            body = line.text[1:].strip()
            self._i += 1
            if not body:
                out.append(self.parse_block(self._child_indent(indent)))
                continue
            if body.startswith(("[", "{")):
                out.append(_flow(body, line.number))
                continue
            try:
                key, value = _split_key(body, line.number)
            except SimpleYamlError:
                out.append(_scalar(body, line.number))
                continue
            item: dict[str, Any] = {_unquote(key) or key: self._value(value, indent + 2, line.number)}
            while (nxt := self._peek()) is not None and nxt.indent > indent and not nxt.text.startswith("- "):
                nkey, nvalue = _split_key(nxt.text, nxt.number)
                nname = _unquote(nkey) or nkey
                if nname in item:
                    raise SimpleYamlError(f"line {nxt.number}: duplicate key {nname!r}")
                self._i += 1
                item[nname] = self._value(nvalue, nxt.indent, nxt.number)
            out.append(item)
        return out

    def _child_indent(self, indent: int) -> int:
        line = self._peek()
        if line is None or line.indent <= indent:
            raise SimpleYamlError(f"line {line.number if line else '?'}: expected an indented block")
        return line.indent

    def _value(self, token: str, indent: int, number: int) -> Any:
        if token:
            return _flow(token, number) if token.startswith(("[", "{")) else _scalar(token, number)
        nxt = self._peek()
        if nxt is None or nxt.indent <= indent:
            return None
        if nxt.text.startswith("- ") or nxt.text == "-":
            # A sequence may sit at the parent's indentation or deeper; both are valid YAML.
            return self._sequence(nxt.indent)
        return self._mapping(nxt.indent)


def loads(text: str) -> Any:
    """Parse a YAML document. Returns a dict, a list, or None for an empty document."""
    lines = _scan(text)
    if not lines:
        return None
    parser = _Parser(lines)
    value = parser.parse_block(lines[0].indent)
    if (leftover := parser._peek()) is not None:
        raise SimpleYamlError(f"line {leftover.number}: unexpected content after the document body")
    return value


def load_path(path: Any) -> Any:
    """Parse the YAML document at `path`, naming the file in any error."""
    from pathlib import Path

    p = Path(path)
    try:
        text = p.read_text(encoding="utf-8")
    except OSError as e:
        raise ConfigError(f"cannot read {p}: {e}") from e
    try:
        return loads(text)
    except SimpleYamlError as e:
        raise SimpleYamlError(f"{p}: {e}") from e
