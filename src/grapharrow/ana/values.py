"""Value operations used by GraphArrow's ANA bindings."""

from __future__ import annotations

import re
from collections.abc import Iterable
from itertools import product
from operator import ge, gt, le, lt
from typing import Any

from grapharrow.ana.handles import EntityPayload, entity_identity
from grapharrow.ana.handles import _columns as _columns

_ORDER = {"lt": lt, "lte": le, "gt": gt, "gte": ge}


def _int_arg(
    args: dict[str, Any],
    name: str,
    default: int,
    *,
    min_value: int = 0,
    max_value: int | None = None,
) -> int:
    value = args.get(name)
    if value is None:
        return default
    if type(value) is not int or value < min_value:
        raise ValueError(f"{name} must be an integer >= {min_value}")
    if max_value is not None and value > max_value:
        raise ValueError(f"{name} must be <= {max_value}")
    return value


def _bool_arg(value: Any) -> bool:
    return bool(value)


def _primary_label(labels: list[str]) -> str:
    return next(iter(labels), "")


def _row_property(row: dict[str, Any], var: str, prop: str) -> Any:
    bound = row.get(var)
    if isinstance(bound, dict):
        return bound.get("properties", bound).get(prop)
    return row.get(prop) if var not in row else None


def freeze(value: Any, *, entities: bool = True) -> Any:
    if entities and isinstance(value, EntityPayload) and value.reference is not None:
        return value.reference
    if isinstance(value, dict):
        return tuple((key, freeze(item, entities=entities)) for key, item in sorted(value.items()))
    if isinstance(value, (list, tuple)):
        return tuple(freeze(item, entities=entities) for item in value)
    return (type(value), value)


def _distinct_rows(
    rows: list[dict[str, Any]],
    *,
    use_references: bool = True,
) -> list[dict[str, Any]]:
    unique: dict[Any, dict[str, Any]] = {}
    for row in rows:
        visible = {key: value for key, value in row.items() if not key.startswith("__")}
        unique.setdefault(freeze(visible, entities=use_references), row)
    return list(unique.values())


def equal(left: Any, right: Any) -> bool:
    if isinstance(left, bool) != isinstance(right, bool):
        return False
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(
            equal(a, b) for a, b in zip(left, right, strict=True)
        )
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(equal(left[k], right[k]) for k in left)
    return bool(left == right)


def _validate_membership_operand(op: str, value: Any) -> None:
    if op in {"in", "not_in"} and isinstance(value, (list, dict, tuple, set)):
        raise ValueError(f"{op} requires a scalar membership value")


def _predicate(actual: Any, op: str, expected: Any) -> bool:
    _validate_membership_operand(op, expected)
    if op in {"is_null", "is_not_null"}:
        return (actual is None) == (op == "is_null")
    if op.endswith("_or_null"):
        return actual is None or _predicate(actual, op.removesuffix("_or_null"), expected)
    if actual is None or expected is None:
        return False
    if op in {"eq", "neq"}:
        return equal(actual, expected) == (op == "eq")
    if op == "eq_ci":
        return str(actual).casefold() == str(expected).casefold()
    if op in _ORDER:
        try:
            return bool(_ORDER[op](actual, expected))
        except TypeError:
            return False
    if op in {"contains", "not_contains", "contains_ci", "starts_with", "ends_with"}:
        lhs, rhs = str(actual), str(expected)
        if op == "starts_with":
            return lhs.startswith(rhs)
        if op == "ends_with":
            return lhs.endswith(rhs)
        if op == "not_contains":
            return rhs not in lhs
        return rhs.casefold() in lhs.casefold() if op == "contains_ci" else rhs in lhs
    if op in {"in", "not_in"}:
        included = isinstance(actual, list) and any(equal(item, expected) for item in actual)
        return included if op == "in" else not included
    if op in {"contains_any", "contains_all"}:
        values = expected if isinstance(expected, list) else [expected]
        checks = (_predicate(actual, "in", value) for value in values)
        return any(checks) if op == "contains_any" else all(checks)
    raise ValueError(f"Unsupported filter op: {op}")


def _metric_identity(row: dict[str, Any], var: str) -> Any:
    value = row.get(var)
    return entity_identity(value) or value


def _metric_scalar_value(row: dict[str, Any], var: str, prop: str | None) -> Any:
    return _row_property(row, var, prop) if prop else row.get(var)


def _distinct_projection_vars(args: dict[str, Any]) -> list[str]:
    return list(
        dict.fromkeys(str(item["var"]) for item in args.get("select") or [] if "var" in item)
    )


def _normalize_aggregate_select_to_group_by(args: dict[str, Any]) -> dict[str, Any]:
    if args.get("metrics") and args.get("select") and not args.get("group_by"):
        return {**args, "group_by": args["select"], "select": []}
    return args


def _pattern_var_contexts(
    label: str,
    alias: str | None,
    hops: list[dict[str, Any]],
) -> dict[str, dict[str, str]]:
    contexts = {alias or label.lower(): {"kind": "node", "label": label}}
    for index, hop in enumerate(hops):
        target = str(hop["target_label"])
        contexts[str(hop.get("as") or target.lower())] = {"kind": "node", "label": target}
        contexts[str(hop.get("rel_as") or f"r{index}")] = {
            "kind": "relationship",
            "relationship_type": str(hop["relationship_type"]),
        }
    return contexts


def _normalize_pattern_var_references(
    args: dict[str, Any],
    contexts: dict[str, dict[str, str]],
) -> dict[str, Any]:
    normalized = dict(args)
    for key in ("filters", "select", "group_by", "metrics", "order_by"):
        if key not in args:
            continue
        items = []
        for entry in args[key] or []:
            if not isinstance(entry, dict):
                raise ValueError(f"{key} entries must be objects")
            variable = entry.get("var")
            candidates = [
                name
                for name, context in contexts.items()
                if variable is not None
                and str(variable).casefold()
                in {
                    name.casefold(),
                    context.get("label", "").casefold(),
                }
            ]
            items.append(
                {**entry, "var": candidates[0]}
                if variable not in contexts and len(candidates) == 1
                else entry
            )
        normalized[key] = items
    return normalized


def _optional_count_source_from_group_by(
    grouping: list[dict[str, Any]],
    contexts: dict[str, dict[str, str]],
) -> str | None:
    sources = {item.get("var") for item in grouping if item.get("var") in contexts}
    return next(iter(sources)) if len(sources) == 1 else None


def _unique_query_var(name: str, used: set[str]) -> str:
    variable, index = name, 1
    while variable in used:
        variable, index = f"{name}_{index}", index + 1
    used.add(variable)
    return variable


def _ambiguous_relationship_only_constraints(
    constraints: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for item in constraints:
        if not item.get("property") and item.get("value") is None:
            key = (str(item.get("target_label")), str(item.get("direction", "out")))
            groups.setdefault(key, []).append(item)
    return next((group for group in groups.values() if len(group) > 1), [])


def _constraint_return_properties(args: dict[str, Any]) -> list[dict[str, str]]:
    return [
        item if isinstance(item, dict) else {"property": str(item), "alias": str(item)}
        for item in args.get("return_properties") or []
    ]


def _expand_projected_rows(
    row: dict[str, Any],
    fields: list[dict[str, Any]],
) -> Iterable[dict[str, Any]]:
    columns = [str(item.get("alias") or item["property"]) for item in fields if item.get("explode")]
    values = [row.get(column) for column in columns]
    choices = [
        value if isinstance(value, list) else [] if value is None else [value] for value in values
    ]
    return (row | dict(zip(columns, combination, strict=True)) for combination in product(*choices))


def _distinctive_entity_search_tokens(text: str) -> set[str]:
    return {word for word in re.findall(r"\w+", text.casefold()) if len(word) > 2}


def _token_variants(words: set[str]) -> set[str]:
    return words | {word[:-1] for word in words if len(word) > 3 and word.endswith("s")}
