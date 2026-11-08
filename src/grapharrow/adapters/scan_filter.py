"""Conservatively translate storage predicates into Arrow dataset expressions."""

from __future__ import annotations

import math
from collections.abc import Sequence
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.dataset as ds  # type: ignore[import-untyped]

from grapharrow.domain.scan import ScanPredicate


def dataset_filter(schema: pa.Schema, predicates: Sequence[ScanPredicate]) -> Any:
    """Conjoin only predicates whose scalar semantics match the residual Arrow filter."""
    combined = None
    for predicate in predicates:
        column = predicate.column
        if column not in schema.names:
            column = f"`{column}`"
        if schema.names.count(column) != 1:
            continue
        field = schema.field(column)
        expression = ds.field(column)
        operator, value = predicate.operator, predicate.value
        if operator == "!=" and pa.types.is_floating(field.type):
            # Parquet min=max statistics can omit NaNs that the residual != must retain.
            continue
        if operator == "is_null":
            part = expression.is_null(nan_is_null=False)
        elif operator == "is_not_null":
            part = expression.is_valid()
        elif operator == "in":
            if not isinstance(value, tuple):
                continue
            if not all(item is None or _compatible_scalar(field.type, item) for item in value):
                continue
            part = expression.isin(pa.array(value, type=field.type)) if value else ds.scalar(False)
        elif value is not None and _compatible_scalar(field.type, value):
            scalar = ds.scalar(pa.scalar(value))
            if operator == "=":
                part = expression == scalar
            elif operator == "!=":
                part = expression != scalar
            elif operator == "<":
                part = expression < scalar
            elif operator == "<=":
                part = expression <= scalar
            elif operator == ">":
                part = expression > scalar
            elif operator == ">=":
                part = expression >= scalar
            else:
                continue
        else:
            continue
        combined = part if combined is None else combined & part
    return combined


def _compatible_scalar(dtype: pa.DataType, value: object) -> bool:
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return type(value) is str
    if pa.types.is_boolean(dtype):
        return type(value) is bool
    if pa.types.is_integer(dtype) and type(value) is int:
        # Avoid narrowing, overflow, and integer-to-float coercion in the scanner binder.
        lower = 0 if pa.types.is_unsigned_integer(dtype) else -(1 << (dtype.bit_width - 1))
        upper = (1 << (dtype.bit_width - (0 if pa.types.is_unsigned_integer(dtype) else 1))) - 1
        return bool(lower <= value <= min(upper, (1 << 63) - 1))
    return bool(pa.types.is_float64(dtype) and type(value) is float and math.isfinite(value))
