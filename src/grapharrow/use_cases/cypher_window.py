"""Conservative projection deferral and runtime bounds for Cypher windows."""

from __future__ import annotations

import os

from grapharrow.core.errors import QueryError
from grapharrow.use_cases.cypher_expression import Expression, ExpressionEvaluator


def limit_optimizations_enabled() -> bool:
    return os.environ.get("GRAPHARROW_DISABLE_CYPHER_LIMIT_OPTIMIZATIONS", "").lower() not in {
        "1",
        "true",
    }


def window_value(
    expression: Expression | None, default: int, evaluator: ExpressionEvaluator
) -> int:
    if expression is None:
        return default
    value = evaluator.evaluate(expression, {})
    if type(value) is not int or value < 0:
        raise QueryError("SKIP and LIMIT require nonnegative integers")
    return value


def safe_to_defer(expression: Expression) -> bool:
    """Total operations after columnar schema/type validation; no fallible kernels.

    Arithmetic and conversions remain eager so LIMIT cannot hide overflow or
    invalid operands in discarded rows. This predicate alone is not sufficient
    to enable early reading on untyped row bindings.
    """
    if expression.kind in {"literal", "variable"}:
        return True
    if expression.kind == "property":
        return expression.arguments[0].kind == "variable"
    if expression.kind == "function" and expression.value != "coalesce":
        return False
    return expression.kind in {
        "function",
        "case",
        "is_null",
        "compare",
        "not",
        "and",
        "or",
        "xor",
        "string",
    } and all(safe_to_defer(child) for child in expression.arguments)


def variables(expression: Expression) -> set[str]:
    result = {expression.value} if expression.kind == "variable" else set()
    for child in expression.arguments:
        result.update(variables(child))
    return result
