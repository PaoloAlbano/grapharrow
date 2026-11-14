"""Scalar Cypher conversion and numeric string formatting."""

from __future__ import annotations

import math
import re
from collections.abc import Callable
from decimal import Decimal, InvalidOperation

from grapharrow.core.errors import QueryError

_INTEGER_MIN = -(2**63)
_INTEGER_MAX = 2**63 - 1
_DECIMAL = re.compile(
    r"[+-]?(?:\d(?:_?\d)*(?:\.(?:\d(?:_?\d)*)?)?|\.\d(?:_?\d)*)(?:[eE][+-]?\d(?:_?\d)*)?[fFdD]?"
)
_HEX_INTEGER = re.compile(r"-?0x[0-9a-fA-F](?:_?[0-9a-fA-F])*")
_OCTAL_INTEGER = re.compile(r"-?0o[0-7](?:_?[0-7])*")
_HEX_FLOAT = re.compile(
    r"[+-]?0[xX](?:[0-9a-fA-F]+(?:\.[0-9a-fA-F]*)?|\.[0-9a-fA-F]+)[pP][+-]?[0-9]+[fFdD]?"
)
_SPECIAL_FLOAT = re.compile(r"[+-]?(?:Infinity|NaN)")


def to_integer(value: object) -> int | None:
    if isinstance(value, int):
        return _checked_integer(int(value))
    if isinstance(value, float):
        if math.isnan(value):
            return 0
        return math.trunc(min(max(value, _INTEGER_MIN), _INTEGER_MAX))
    if isinstance(value, str):
        number = _parse_integer_string(value)
        if number is not None:
            return _checked_integer(number)
        return None
    raise _invalid_operand("toInteger")


def _parse_integer_string(value: str) -> int | Decimal | None:
    text = value.strip()
    if text.startswith("+") and not value.startswith("+"):
        return None
    if _HEX_INTEGER.fullmatch(text) or _OCTAL_INTEGER.fullmatch(text):
        return int(text, 0)
    if not _DECIMAL.fullmatch(text):
        return None
    try:
        return Decimal(text.rstrip("fFdD").replace("_", ""))
    except InvalidOperation:
        return None


def _checked_integer(value: int | Decimal) -> int:
    if not _INTEGER_MIN <= value <= _INTEGER_MAX:
        raise QueryError(
            "toInteger() result is outside the signed 64-bit range.",
            category="TypeError",
            detail="NumberOutOfRange",
            phase="runtime",
        )
    return int(value)


def to_float(value: object) -> float | None:
    if type(value) in {int, float}:
        assert isinstance(value, int | float)
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if _HEX_INTEGER.fullmatch(text) or _OCTAL_INTEGER.fullmatch(text):
            number = int(text, 0)
            try:
                return float(number)
            except OverflowError:
                return -math.inf if number < 0 else math.inf
        if _HEX_FLOAT.fullmatch(text):
            try:
                return float.fromhex(text.rstrip("fFdD"))
            except OverflowError:
                return -math.inf if text.startswith("-") else math.inf
        if (_DECIMAL.fullmatch(text) and text.isascii()) or _SPECIAL_FLOAT.fullmatch(text):
            return float(text.rstrip("fFdD").replace("_", ""))
        return None
    raise _invalid_operand("toFloat")


def to_boolean(value: object) -> bool | None:
    if isinstance(value, int):
        return bool(value)
    if isinstance(value, str):
        return {"true": True, "false": False}.get(value.strip(" \t\n\r\v\f").lower())
    raise _invalid_operand("toBoolean")


def to_string(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str | int):
        return str(value)
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "-Infinity" if value < 0 else "Infinity"
        if value and (abs(value) < 0.001 or abs(value) >= 10_000_000):
            # Java's floating-point notation uses an uppercase exponent and
            # always includes a digit after the decimal point.
            mantissa, exponent = format(Decimal(repr(value)), "E").split("E")
            mantissa = mantissa.rstrip("0").rstrip(".") if "." in mantissa else mantissa
            if "." not in mantissa:
                mantissa += ".0"
            return f"{mantissa}E{int(exponent)}"
        return str(value)
    raise _invalid_operand("toString")


def sign(value: object) -> int:
    if isinstance(value, int | float) and not isinstance(value, bool):
        return int(value > 0) - int(value < 0)
    raise _invalid_operand("sign")


def _invalid_operand(function: str) -> QueryError:
    return QueryError(
        f"Invalid argument for {function}().",
        category="TypeError",
        detail="InvalidArgumentValue",
        phase="runtime",
    )


SCALAR_CONVERSIONS: dict[str, Callable[[object], object]] = {
    "tointeger": to_integer,
    "tofloat": to_float,
    "toboolean": to_boolean,
    "tostring": to_string,
    "sign": sign,
}
