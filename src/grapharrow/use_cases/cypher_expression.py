"""Typed expression trees for composable Cypher read pipelines.

Expressions are compiled from ANTLR contexts, never evaluated as Python source.
CASE and coalesce evaluate only the selected branch. Boolean and equality
operations retain Cypher's three-valued null semantics.
"""

from __future__ import annotations

import math
import operator as operations
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from grapharrow.core.errors import QueryError
from grapharrow.core.temporal import Duration, Instant, TemporalResult
from grapharrow.repositories import EdgeRecord, NodeRecord
from grapharrow.use_cases.cypher_conversion import SCALAR_CONVERSIONS
from grapharrow.use_cases.cypher_path import PathResult, PathValue
from grapharrow.use_cases.cypher_temporal import (
    TEMPORAL_ARITIES,
    evaluate_temporal,
    temporal_arithmetic,
)

Row = dict[str, Any]
Scope = dict[str, str]
LIST_ACCESSORS = frozenset({"head", "last", "tail"})
TYPED_SCALAR_FUNCTIONS = frozenset(
    {"range", "properties", *LIST_ACCESSORS, *SCALAR_CONVERSIONS, *TEMPORAL_ARITIES}
)
_MAX_RANGE_ITEMS = 1 << 20


@dataclass(frozen=True, slots=True)
class Expression:
    kind: str
    value: Any = None
    arguments: tuple[Expression, ...] = ()


def rule(node: Any) -> str:
    return str(node.parser.ruleNames[node.getRuleIndex()])


def children(node: Any) -> list[Any]:
    return [child for child in node.children or [] if hasattr(child, "getRuleIndex")]


def identifier(text: str) -> str:
    return text[1:-1].replace("``", "`") if text.startswith("`") else text


class ExpressionCompiler:
    def __init__(
        self,
        parameters: Mapping[str, object],
        exists: Callable[[Any, Scope], Any],
    ) -> None:
        self.parameters = parameters
        self.exists = exists

    def compile(self, node: Any, scope: Scope) -> Expression:
        name = rule(node)
        parts = children(node)

        def build(child: Any) -> Expression:
            return self.compile(child, scope)

        if name == "oC_Variable":
            variable = identifier(node.getText())
            if variable not in scope:
                raise QueryError.syntax(f"Unknown variable: {variable}", "UndefinedVariable")
            return Expression("variable", variable)
        if name == "oC_Parameter":
            parameter = identifier(node.getText()[1:])
            if parameter not in self.parameters:
                raise QueryError.missing_parameter(parameter)
            return Expression("literal", self.parameters[parameter])
        if name == "oC_ListLiteral":
            return Expression("list", arguments=tuple(build(p) for p in node.oC_Expression()))
        if name == "oC_MapLiteral":
            return Expression(
                "map",
                tuple(identifier(p.getText()) for p in node.oC_PropertyKeyName()),
                tuple(build(p) for p in node.oC_Expression()),
            )
        if name in {"oC_ExistentialSubquery", "oC_PatternPredicate"}:
            if name == "oC_PatternPredicate":
                parent = node.parentCtx
                while parent is not None and rule(parent) not in {"oC_Where", "oC_ProjectionItem"}:
                    parent = parent.parentCtx
                if parent is None or rule(parent) != "oC_Where":
                    raise QueryError("Pattern predicates are only supported in predicate contexts")
            return Expression("exists", self.exists(node, scope))
        if name == "oC_FunctionInvocation":
            function = node.oC_FunctionName().getText().lower()
            arguments = tuple(build(p) for p in node.oC_Expression())
            if function in TYPED_SCALAR_FUNCTIONS:
                allowed_counts = TEMPORAL_ARITIES.get(
                    function, (2, 3) if function == "range" else (1,)
                )
                if len(arguments) not in allowed_counts:
                    raise QueryError.syntax(
                        f"{function}() accepts argument counts {allowed_counts}.",
                        "InvalidNumberOfArguments",
                    )
                if node.DISTINCT() is not None:
                    raise QueryError.syntax(
                        "DISTINCT requires an aggregation function.", "InvalidArgumentPassingMode"
                    )
                if function in LIST_ACCESSORS or function == "properties":
                    operand = arguments[0]
                    invalid_kinds = (
                        {"map"} if function in LIST_ACCESSORS else {"list", "comprehension"}
                    )
                    invalid_entities = (
                        {"node", "relationship", "path"}
                        if function in (LIST_ACCESSORS)
                        else {"path"}
                    )
                    if (
                        operand.kind in invalid_kinds
                        or (operand.kind == "variable" and scope[operand.value] in invalid_entities)
                        or (
                            operand.kind == "literal"
                            and operand.value is not None
                            and not node.oC_Expression()[0].getText().lstrip("(").startswith("$")
                        )
                    ):
                        raise QueryError.invalid_type(
                            f"Invalid operand for {function}", phase="compile"
                        )
                return Expression("function", function, arguments)
            if function in {"count", "sum", "avg", "min", "max", "collect"}:
                if len(arguments) != 1 or any(has_aggregate(a) for a in arguments):
                    raise QueryError("Aggregates require one non-aggregate operand")
                return Expression("aggregate", (function, node.DISTINCT() is not None), arguments)
            arities = {
                "abs": 1,
                "size": 1,
                "tolower": 1,
                "toupper": 1,
                "id": 1,
                "labels": 1,
                "type": 1,
                "keys": 1,
                "length": 1,
                "nodes": 1,
                "relationships": 1,
            }
            if function == "coalesce":
                if not arguments:
                    raise QueryError("coalesce requires at least one argument")
            elif function not in arities:
                raise QueryError.unsupported(f"Unsupported scalar function: {function}")
            elif len(arguments) != arities[function]:
                raise QueryError(f"{function} requires {arities[function]} argument")
            entity_types = {
                "id": {"node", "relationship"},
                "labels": {"node"},
                "type": {"relationship"},
                "length": {"path"},
                "nodes": {"path"},
                "relationships": {"path"},
            }
            if function in entity_types and arguments[0].kind == "variable":
                kind = scope[arguments[0].value]
                if kind not in entity_types[function] | {"any", "null"}:
                    raise QueryError.invalid_type(
                        f"Invalid operand for {function}: {kind}", phase="compile"
                    )
            return Expression("function", function, arguments)
        if name == "oC_Atom" and node.COUNT() is not None:
            return Expression("aggregate", ("count", False), ())
        if name == "oC_CaseExpression":
            base = None
            otherwise = Expression("literal", None)
            alternatives: list[Expression] = []
            seen_alternative = False
            for part in parts:
                if rule(part) == "oC_CaseAlternative":
                    seen_alternative = True
                    condition, result = (build(p) for p in part.oC_Expression())
                    if base is None:
                        validate_predicate(condition)
                    alternatives.extend((condition, result))
                elif rule(part) == "oC_Expression":
                    if seen_alternative:
                        otherwise = build(part)
                    else:
                        base = build(part)
            return Expression(
                "case",
                base is not None,
                (*((base,) if base is not None else ()), *alternatives, otherwise),
            )
        if name in {"oC_ListComprehension", "oC_Quantifier"}:
            filtering = node.oC_FilterExpression()
            binding = filtering.oC_IdInColl()
            variable = identifier(binding.oC_Variable().getText())
            collection = build(binding.oC_Expression())
            local = scope | {variable: "any"}
            where = filtering.oC_Where()
            predicate = (
                self.compile(where.oC_Expression(), local)
                if where is not None
                else Expression("literal", True)
            )
            validate_predicate(predicate)
            if name == "oC_ListComprehension":
                projection = node.oC_Expression()
                value = (
                    self.compile(projection, local)
                    if projection is not None
                    else Expression("variable", variable)
                )
                if has_aggregate(predicate) or has_aggregate(value):
                    raise QueryError("List expressions cannot contain aggregates")
                return Expression("comprehension", variable, (collection, predicate, value))
            operation = next(c.getText().lower() for c in node.children if c.getText().strip())
            return Expression("quantifier", (operation, variable), (collection, predicate))
        if name == "oC_NonArithmeticOperatorExpression":
            expression = build(parts[0])
            for part in parts[1:]:
                part_name = rule(part)
                if part_name == "oC_PropertyLookup":
                    expression = Expression(
                        "property", identifier(part.oC_PropertyKeyName().getText()), (expression,)
                    )
                elif part_name == "oC_NodeLabels":
                    labels = tuple(
                        identifier(p.oC_LabelName().getText()) for p in part.oC_NodeLabel()
                    )
                    expression = Expression("labels", labels, (expression,))
                elif part_name == "oC_ListOperatorExpression":
                    positions = part.oC_Expression()
                    if ".." in [c.getText() for c in part.children]:
                        lower, upper = Expression("literal", None), Expression("literal", None)
                        explicit = [False, False]
                        separator = next(
                            i for i, c in enumerate(part.children) if c.getText() == ".."
                        )
                        for i, c in enumerate(part.children):
                            if hasattr(c, "getRuleIndex"):
                                if i < separator:
                                    lower = build(c)
                                    explicit[0] = True
                                else:
                                    upper = build(c)
                                    explicit[1] = True
                        expression = Expression(
                            "slice", tuple(explicit), (expression, lower, upper)
                        )
                    else:
                        expression = Expression(
                            "index", arguments=(expression, build(positions[0]))
                        )
                else:
                    raise QueryError(f"Unsupported expression component: {part.getText()}")
            return expression
        if name in {"oC_OrExpression", "oC_XorExpression", "oC_AndExpression"}:
            operation = {
                "oC_OrExpression": "or",
                "oC_XorExpression": "xor",
                "oC_AndExpression": "and",
            }[name]
            values = tuple(build(p) for p in parts)
            if len(values) > 1:
                for operand in values:
                    validate_predicate(operand)
            return values[0] if len(values) == 1 else Expression(operation, arguments=values)
        if name == "oC_NotExpression":
            value = build(parts[0])
            if node.NOT():
                validate_predicate(value)
            return Expression("not", arguments=(value,)) if len(node.NOT()) % 2 else value
        if name == "oC_ComparisonExpression":
            left = build(parts[0])
            comparisons = parts[1:]
            if not comparisons:
                return left
            operations = tuple(
                next(c.getText() for c in p.children if c.getText().strip()) for p in comparisons
            )
            return Expression(
                "compare", operations, (left, *(build(children(p)[0]) for p in comparisons))
            )
        if name == "oC_StringListNullPredicateExpression":
            result = build(parts[0])
            for part in parts[1:]:
                part_name = rule(part)
                if part_name == "oC_NullPredicateExpression":
                    result = Expression("is_null", part.NOT() is None, (result,))
                elif part_name == "oC_ListPredicateExpression":
                    collection = build(children(part)[0])
                    if collection.kind == "map" or (
                        collection.kind == "literal"
                        and collection.value is not None
                        and not isinstance(collection.value, list | tuple)
                    ):
                        raise QueryError.invalid_type("IN requires a list", phase="compile")
                    result = Expression("in", arguments=(result, collection))
                else:
                    op = (
                        "starts"
                        if part.STARTS() is not None
                        else "ends"
                        if part.ENDS() is not None
                        else "contains"
                    )
                    result = Expression("string", op, (result, build(children(part)[0])))
            return result
        if name in {
            "oC_AddOrSubtractExpression",
            "oC_MultiplyDivideModuloExpression",
            "oC_PowerOfExpression",
        }:
            result = build(parts[0])
            operators = [
                c.getText()
                for c in node.children
                if not hasattr(c, "getRuleIndex") and c.getText().strip()
            ]
            for operator, part in zip(operators, parts[1:], strict=True):
                result = Expression("binary", operator, (result, build(part)))
            return result
        if name == "oC_UnaryAddOrSubtractExpression":
            if re.fullmatch(r"[+-]\d+", node.getText()):
                from grapharrow.use_cases.cypher import _parse_value

                return Expression(
                    "literal", _parse_value(node.getText().lstrip("+"), dict(self.parameters))
                )
            result = build(parts[0])
            prefix = node.children[0].getText()
            return Expression("unary", prefix, (result,)) if prefix in {"+", "-"} else result
        if len(parts) == 1:
            return build(parts[0])
        if not parts:
            from grapharrow.use_cases.cypher import _parse_value

            return Expression("literal", _parse_value(node.getText(), dict(self.parameters)))
        raise QueryError(f"Unsupported expression: {node.getText()}")


def has_aggregate(expression: Expression) -> bool:
    return expression.kind == "aggregate" or any(has_aggregate(p) for p in expression.arguments)


def validate_predicate(expression: Expression) -> None:
    """Reject known non-boolean predicates even when the input has no rows."""
    if expression.kind in {"list", "map"} or (
        expression.kind == "literal"
        and expression.value is not None
        and type(expression.value) is not bool
    ):
        raise QueryError.invalid_type(
            "A predicate must evaluate to a boolean or null", phase="compile"
        )


def truth(value: Any) -> bool | None:
    if value is None or type(value) is bool:
        return value
    raise QueryError.invalid_type("A predicate must evaluate to a boolean or null", phase="runtime")


def freeze(value: Any) -> Any:
    if isinstance(value, PathValue):
        return (
            "path",
            tuple(node.node_id for node in value.nodes),
            tuple(edge.edge_id for edge in value.relationships),
        )
    if isinstance(value, NodeRecord):
        return ("node", value.node_id)
    if isinstance(value, EdgeRecord):
        return ("relationship", value.edge_id)
    if value is None:
        return ("null",)
    if type(value) is bool:
        return ("boolean", value)
    if isinstance(value, int | float):
        return (
            ("number", "nan")
            if isinstance(value, float) and math.isnan(value)
            else ("number", value)
        )
    if isinstance(value, list | tuple):
        return ("list", tuple(freeze(p) for p in value))
    if isinstance(value, dict):
        return ("map", tuple((k, freeze(v)) for k, v in sorted(value.items())))
    return (type(value).__name__, value)


def equal(left: Any, right: Any) -> bool | None:
    if left is None or right is None:
        return None
    if any(isinstance(value, float) and math.isnan(value) for value in (left, right)):
        return False
    if isinstance(left, list | tuple) and isinstance(right, list | tuple):
        if len(left) != len(right):
            return False
        return conjunction([equal(a, b) for a, b in zip(left, right, strict=True)])
    if isinstance(left, dict) and isinstance(right, dict):
        if left.keys() != right.keys():
            return False
        return conjunction([equal(left[k], right[k]) for k in left])
    return bool(freeze(left) == freeze(right))


def conjunction(values: list[bool | None]) -> bool | None:
    return False if False in values else None if None in values else True


def property_value(value: Any, key: str) -> Any:
    if value is None:
        return None
    if isinstance(value, NodeRecord | EdgeRecord):
        return value.properties.get(key)
    if isinstance(value, Mapping):
        return value.get(key)
    if isinstance(value, Instant | Duration):
        try:
            return value.component(key)
        except KeyError as error:
            raise QueryError(
                f"Unknown temporal component: {key}",
                category="ArgumentError",
                phase="runtime",
                detail="InvalidArgumentValue",
            ) from error
    raise QueryError.invalid_type(
        "Property access requires a node, relationship or map", phase="runtime"
    )


def _range_values(arguments: list[Any]) -> list[int]:
    step = arguments[2] if len(arguments) == 3 else 1
    if type(step) is int and step == 0:
        raise QueryError(
            "range() requires a nonzero step.",
            category="ArgumentError",
            detail="NumberOutOfRange",
            phase="runtime",
        )
    if any(argument is None for argument in arguments):
        raise QueryError.invalid_type("range() requires non-null integers.", phase="runtime")
    if any(type(argument) is not int for argument in arguments):
        raise QueryError(
            "range() requires integer arguments.",
            category="ArgumentError",
            detail="InvalidArgumentType",
            phase="runtime",
        )
    start, end = arguments[:2]
    if any(not -(2**63) <= argument < 2**63 for argument in arguments):
        raise QueryError(
            "range() requires signed 64-bit integers.",
            category="ArgumentError",
            detail="NumberOutOfRange",
            phase="runtime",
        )
    # Compute cardinality without overflowing a machine-sized length or
    # allocating the list. Python also permits the exclusive bound just
    # beyond either signed 64-bit endpoint.
    count = (end - start) // step + 1
    if count > _MAX_RANGE_ITEMS:
        raise QueryError(
            f"range() produces more than {_MAX_RANGE_ITEMS} items.",
            category="UnsupportedFeature",
            detail="RangeTooLarge",
            phase="runtime",
        )
    return list(range(start, end + (1 if step > 0 else -1), step))


def public_value(value: Any) -> Any:
    if isinstance(value, Instant | Duration):
        return TemporalResult(value)
    if isinstance(value, PathValue):
        return PathResult(
            nodes=public_value(value.nodes), relationships=public_value(value.relationships)
        )
    if isinstance(value, NodeRecord):
        return {
            "id": value.node_id,
            "labels": list(value.labels),
            "properties": public_value(value.properties),
        }
    if isinstance(value, EdgeRecord):
        return {
            "id": value.edge_id,
            "type": value.rel_type,
            "source": value.src_id,
            "target": value.dst_id,
            "properties": public_value(value.properties),
        }
    if isinstance(value, list | tuple):
        return [public_value(p) for p in value]
    if isinstance(value, dict):
        return {k: public_value(v) for k, v in value.items()}
    return value


def sort_key(value: Any) -> Any:
    if value is None:
        return (9,)
    if isinstance(value, dict):
        return (0, tuple((k, sort_key(v)) for k, v in sorted(value.items())))
    if isinstance(value, NodeRecord):
        return (1, value.node_id)
    if isinstance(value, EdgeRecord):
        return (2, value.edge_id)
    if isinstance(value, list | tuple):
        return (3, tuple(sort_key(p) for p in value))
    if isinstance(value, PathValue):
        parts = [
            (node.node_id, edge.edge_id)
            for node, edge in zip(value.nodes[:-1], value.relationships, strict=True)
        ]
        return (4, (*parts, (value.nodes[-1].node_id,)))
    if isinstance(value, Instant):
        rank = ("datetime", "localdatetime", "date", "time", "localtime").index(value.kind)
        return (5, rank, value.order_key())
    if isinstance(value, Duration):
        return (5, 5, value.order_key())
    if isinstance(value, str):
        return (5, 6, value)
    if type(value) is bool:
        return (6, value)
    if isinstance(value, int | float):
        return (7, 1, 0) if isinstance(value, float) and math.isnan(value) else (7, 0, value)
    raise QueryError(f"Unsupported ordered value: {type(value).__name__}")


class ExpressionEvaluator:
    def __init__(
        self,
        exists: Callable[[Any, Row], bool],
        *,
        resolve_entity: Callable[[Any], Any] | None = None,
        transaction_time: int | None = None,
    ) -> None:
        self.exists = exists
        self.resolve_entity = resolve_entity
        self._statement_time: int | None = None
        self._transaction_time = transaction_time

    def evaluate(self, expression: Expression, row: Row, group: list[Row] | None = None) -> Any:
        kind, value, args = expression.kind, expression.value, expression.arguments

        def evaluate(child: Expression) -> Any:
            return self.evaluate(child, row, group)

        if kind == "literal":
            return value
        if kind == "variable":
            return row[value]
        if kind == "property":
            operand = evaluate(args[0])
            if self.resolve_entity is not None:
                operand = self.resolve_entity(operand)
            return property_value(operand, value)
        if kind == "list":
            return [evaluate(p) for p in args]
        if kind == "map":
            return {k: evaluate(p) for k, p in zip(value, args, strict=True)}
        if kind == "exists":
            return self.exists(value, row)
        if kind == "case":
            subject = evaluate(args[0]) if value else None
            choices = args[1:-1] if value else args[:-1]
            for when, then in zip(choices[::2], choices[1::2], strict=True):
                condition = equal(subject, evaluate(when)) if value else truth(evaluate(when))
                if condition is True:
                    return evaluate(then)
            return evaluate(args[-1])
        if kind == "function":
            if value in TEMPORAL_ARITIES:
                if self._statement_time is None:
                    self._statement_time = time.time_ns()
                now = (
                    time.time_ns()
                    if value.endswith(".realtime")
                    else self._transaction_time or self._statement_time
                    if value.endswith(".transaction")
                    else self._statement_time
                )
                return evaluate_temporal(value, [evaluate(arg) for arg in args], now)
            if value == "range":
                return _range_values([evaluate(arg) for arg in args])
            if value == "coalesce":
                for child in args:
                    result = evaluate(child)
                    if result is not None:
                        return result
                return None
            operand = evaluate(args[0])
            if operand is None:
                return None
            if value in SCALAR_CONVERSIONS:
                if value == "tostring" and isinstance(operand, Instant | Duration):
                    return str(operand)
                return SCALAR_CONVERSIONS[value](operand)
            if self.resolve_entity is not None and value in {"labels", "keys", "properties"}:
                operand = self.resolve_entity(operand)
            if value in LIST_ACCESSORS and isinstance(operand, list | tuple):
                if value == "tail":
                    return list(operand[1:])
                return operand[0 if value == "head" else -1] if operand else None
            if value == "properties":
                if isinstance(operand, NodeRecord | EdgeRecord):
                    return dict(operand.properties)
                if isinstance(operand, Mapping):
                    return dict(operand)
            if value == "abs" and type(operand) in {int, float}:
                return abs(operand)
            if value == "size" and isinstance(operand, str | list | tuple):
                return len(operand)
            if value in {"tolower", "toupper"} and isinstance(operand, str):
                return operand.lower() if value == "tolower" else operand.upper()
            if value == "id" and isinstance(operand, NodeRecord | EdgeRecord):
                return operand.node_id if isinstance(operand, NodeRecord) else operand.edge_id
            if value == "labels" and isinstance(operand, NodeRecord):
                return list(operand.labels)
            if value == "type" and isinstance(operand, EdgeRecord):
                return operand.rel_type
            if value == "keys" and isinstance(operand, NodeRecord | EdgeRecord | dict):
                return list(operand if isinstance(operand, dict) else operand.properties)
            if isinstance(operand, PathValue):
                if value == "length":
                    return len(operand.relationships)
                if value == "nodes":
                    return list(operand.nodes)
                if value == "relationships":
                    return list(operand.relationships)
            raise QueryError.invalid_type(f"Invalid operand for {value}", phase="runtime")
        if kind == "aggregate":
            if group is None:
                raise QueryError("Aggregate is not allowed in this expression context")
            function, distinct = value
            if not args:
                return len(group)
            values = [self.evaluate(args[0], r) for r in group]
            values = [v for v in values if v is not None]
            if distinct:
                unique: dict[Any, Any] = {}
                for entry in values:
                    unique.setdefault(freeze(entry), entry)
                values = list(unique.values())
            if function == "count":
                return len(values)
            if function == "collect":
                return values
            if function in {"sum", "avg"}:
                if any(type(v) not in {int, float} for v in values):
                    raise QueryError(f"{function} requires numeric values")
                total = sum(values)
                return total if function == "sum" else total / len(values) if values else None
            return (
                (min(values, key=sort_key) if function == "min" else max(values, key=sort_key))
                if values
                else None
            )
        if kind in {"and", "or", "xor"}:
            values = []
            for child in args:
                result = truth(evaluate(child))
                values.append(result)
                if kind == "and" and result is False:
                    return False
                if kind == "or" and result is True:
                    return True
            if kind == "and":
                return conjunction(values)
            if kind == "or":
                return None if None in values else False
            return None if None in values else sum(bool(p) for p in values) % 2 == 1
        if kind == "not":
            operand = truth(evaluate(args[0]))
            return None if operand is None else not operand
        if kind == "is_null":
            return (evaluate(args[0]) is None) == value
        if kind == "labels":
            operand = evaluate(args[0])
            if self.resolve_entity is not None:
                operand = self.resolve_entity(operand)
            if operand is None:
                return None
            if not isinstance(operand, NodeRecord):
                raise QueryError("Label predicates require a node")
            return all(label in operand.labels for label in value)
        if kind == "compare":
            values = [evaluate(a) for a in args]
            results = []
            for operator, left, right in zip(value, values[:-1], values[1:], strict=True):
                if operator in {"=", "<>"}:
                    result = equal(left, right)
                    results.append(
                        None if result is None else result if operator == "=" else not result
                    )
                elif left is None or right is None:
                    results.append(None)
                else:
                    if isinstance(left, Instant) and isinstance(right, Instant):
                        if left.kind != right.kind:
                            results.append(None)
                            continue
                        left, right = left.order_key(), right.order_key()
                    if isinstance(left, Duration) or isinstance(right, Duration):
                        results.append(None)
                        continue
                    if type(left) is bool or type(right) is bool:
                        if type(left) is not type(right):
                            results.append(None)
                            continue
                    elif not (
                        type(left) is type(right)
                        or type(left) in {int, float}
                        and type(right) in {int, float}
                    ):
                        results.append(None)
                        continue
                    try:
                        results.append(
                            {
                                "<": operations.lt,
                                "<=": operations.le,
                                ">": operations.gt,
                                ">=": operations.ge,
                            }[operator](left, right)
                        )
                    except TypeError as error:
                        raise QueryError("Values cannot be compared") from error
            return conjunction(results)
        if kind == "in":
            needle, collection = evaluate(args[0]), evaluate(args[1])
            if collection is None:
                return None
            if not isinstance(collection, list | tuple):
                raise QueryError.invalid_type("IN requires a list", phase="runtime")
            results = [equal(needle, p) for p in collection]
            return True if True in results else None if None in results else False
        if kind == "string":
            left, right = evaluate(args[0]), evaluate(args[1])
            if left is None or right is None:
                return None
            if not isinstance(left, str) or not isinstance(right, str):
                raise QueryError.invalid_type("String predicates require strings", phase="runtime")
            return (
                left.startswith(right)
                if value == "starts"
                else left.endswith(right)
                if value == "ends"
                else right in left
            )
        if kind in {"unary", "binary"}:
            values = [evaluate(p) for p in args]
            if any(p is None for p in values):
                return None
            if kind == "binary" and value == "+":
                left, right = values
                if isinstance(left, list):
                    return left + (right if isinstance(right, list) else [right])
                if isinstance(right, list):
                    return [left] + right
                if isinstance(left, str) and isinstance(right, str):
                    return left + right
                if isinstance(left, str) and type(right) in {int, float}:
                    return left + str(right)
                if isinstance(right, str) and type(left) in {int, float}:
                    return str(left) + right
            if kind == "binary" and any(isinstance(p, Instant | Duration) for p in values):
                return temporal_arithmetic(value, *values)
            if any(type(p) not in {int, float} for p in values):
                raise QueryError.invalid_type(
                    "Arithmetic requires numeric operands", phase="runtime"
                )
            if kind == "unary":
                return values[0] if value == "+" else -values[0]
            left, right = values
            try:
                if value == "/" and type(left) is int and type(right) is int:
                    return (abs(left) // abs(right)) * (-1 if (left < 0) != (right < 0) else 1)
                if value == "%":
                    if type(left) is int and type(right) is int:
                        quotient = (abs(left) // abs(right)) * (
                            -1 if (left < 0) != (right < 0) else 1
                        )
                        return left - quotient * right
                    return math.fmod(left, right)
                if value == "^":
                    return math.pow(left, right)
                return {
                    "+": lambda: left + right,
                    "-": lambda: left - right,
                    "*": lambda: left * right,
                    "/": lambda: left / right,
                    "%": lambda: left % right,
                }[value]()
            except ZeroDivisionError as error:
                raise QueryError(
                    "Division by zero", category="ArithmeticError", detail="DivisionByZero"
                ) from error
            except (ArithmeticError, ValueError) as error:
                raise QueryError(
                    f"Invalid arithmetic operation: {value}",
                    category="ArithmeticError",
                    detail="InvalidArithmeticOperation",
                ) from error
        if kind in {"index", "slice"}:
            collection = evaluate(args[0])
            if collection is None:
                return None
            positions = [evaluate(p) for p in args[1:]]
            if kind == "index" and isinstance(collection, dict):
                if positions[0] is not None and not isinstance(positions[0], str):
                    raise QueryError("Map indexes must be strings")
                return collection.get(positions[0])
            if not isinstance(collection, list | tuple):
                raise QueryError("Indexing requires a list or map")
            if any(p is not None and type(p) is not int for p in positions):
                raise QueryError("List indexes must be integers")
            if kind == "slice":
                if any(
                    explicit and position is None
                    for explicit, position in zip(value, positions, strict=True)
                ):
                    return None
                return collection[slice(*positions)]
            index = positions[0]
            return (
                collection[index]
                if index is not None and -len(collection) <= index < len(collection)
                else None
            )
        if kind in {"comprehension", "quantifier"}:
            collection = evaluate(args[0])
            if collection is None:
                return None
            if not isinstance(collection, list | tuple):
                raise QueryError("List expressions require a list")
            variable = value if kind == "comprehension" else value[1]
            results = []
            for item in collection:
                local = row | {variable: item}
                match = truth(self.evaluate(args[1], local, group))
                if kind == "comprehension":
                    if match is True:
                        results.append(self.evaluate(args[2], local, group))
                else:
                    results.append(match)
            if kind == "comprehension":
                return results
            operation = value[0]
            if operation == "all":
                return conjunction(results)
            if operation in {"any", "none"}:
                any_result = True if True in results else None if None in results else False
                return any_result if operation == "any" or any_result is None else not any_result
            true_count = sum(r is True for r in results)
            return False if true_count > 1 else None if None in results else true_count == 1
        raise QueryError(f"Unsupported expression operation: {kind}")
