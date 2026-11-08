"""Select pattern seed IDs before constructing workflow-owned entity payloads."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.compute as pc  # type: ignore[import-untyped]

from grapharrow.use_cases.ana_search import _search_rows


def _same_type(field: Any, expected: Any) -> bool:
    return bool(
        pa.types.is_string(field.type)
        and type(expected) is str
        or pa.types.is_boolean(field.type)
        and type(expected) is bool
        or pa.types.is_integer(field.type)
        and type(expected) is int
        or pa.types.is_float64(field.type)
        and type(expected) is float
    )


def _mask(table: pa.Table, flt: dict[str, Any]) -> pa.Array | pa.ChunkedArray | None:
    """Translate an exact scalar predicate; unsupported conversions stay residual."""
    prop, op = str(flt["property"]), str(flt.get("op", "eq"))
    if prop == "node_id" or prop not in table.column_names:
        return None
    field = table.schema.field(prop)
    if (field.metadata or {}).get(b"grapharrow.encoding") != b"native":
        return None
    values = table[prop]
    if op in {"is_null", "is_not_null"}:
        return pc.is_null(values) if op == "is_null" else pc.is_valid(values)
    expected = flt.get("value")
    nullable = op in {"gte_or_null", "lte_or_null"}
    if expected is None:
        return (
            pc.is_null(values) if nullable else pa.array([False] * table.num_rows, type=pa.bool_())
        )
    if flt.get("value_type") == "integer":
        try:
            expected = int(expected)
        except (ValueError, TypeError, OverflowError):
            return None
    if not _same_type(field, expected):
        return None
    comparisons = {
        "eq": pc.equal,
        "neq": pc.not_equal,
        "gt": pc.greater,
        "gte": pc.greater_equal,
        "lt": pc.less,
        "lte": pc.less_equal,
        "gte_or_null": pc.greater_equal,
        "lte_or_null": pc.less_equal,
    }
    try:
        if op in comparisons:
            result = comparisons[op](values, expected)
        elif op in {"contains", "not_contains"} and type(expected) is str:
            result = pc.match_substring(values, expected)
            if op == "not_contains":
                result = pc.invert(result)
        elif op == "eq_ci" and type(expected) is str and expected.isascii():
            if pc.any(pc.match_substring_regex(values, pattern=r"[^\x00-\x7f]")).as_py():
                return None
            result = pc.equal(pc.utf8_lower(values), expected.lower())
        else:
            return None
        return pc.or_kleene(pc.is_null(values), result) if nullable else result
    except (
        pa.ArrowInvalid,
        pa.ArrowNotImplementedError,
        pa.ArrowTypeError,
        OverflowError,
        TypeError,
        UnicodeError,
    ):
        return None


def select_node_ids(
    backend: Any,
    label: str,
    nodes: list[int],
    filters: list[dict[str, Any]],
    match: Callable[[dict[str, Any], dict[str, Any]], bool],
) -> list[int]:
    """Apply a safe Arrow prefix, then exact predicates over narrow property rows.

    A prefix cannot bypass an earlier residual conversion/error. Node order and
    the input-handle scope remain authoritative; all predicates remain in the
    pattern executor as residual checks after selection.
    """
    if not nodes or not filters:
        return nodes
    table = backend._property_tables.get(label)
    remaining = filters
    keys = pa.array(nodes, type=pa.int64())
    if (
        table is not None
        and "node_id" in table.column_names
        and pa.types.is_int64(table["node_id"].type)
        and not table.schema.field("node_id").metadata
        and pc.index_in(keys, value_set=table["node_id"]).null_count
    ):
        table = None
    if (
        table is not None
        and "node_id" in table.column_names
        and pa.types.is_int64(table["node_id"].type)
        and not table.schema.field("node_id").metadata
    ):
        for index, flt in enumerate(filters):
            indexed = None
            prop = str(flt["property"])
            # Eligible equality declarations can avoid a complete property scan.
            if (
                str(flt.get("op", "eq")) == "eq"
                and flt.get("value_type") != "integer"
                and prop != "node_id"
                and prop in table.column_names
                and (table.schema.field(prop).metadata or {}).get(b"grapharrow.encoding")
                == b"native"
                and _same_type(table.schema.field(prop), flt.get("value"))
            ):
                indexed = backend._application.use_cases.graph.lookup_node_property_table(
                    snapshot=backend._state.snapshot,
                    catalog=backend._state.catalog,
                    label=label,
                    equalities={str(flt["property"]): flt.get("value")},
                    table=table,
                )
            if indexed is not None:
                matched = indexed["node_id"]
            else:
                mask = _mask(table, flt)
                if mask is None:
                    remaining = filters[index:]
                    break
                matched = pc.filter(table["node_id"], mask)
            keys = pc.filter(keys, pc.is_in(keys, value_set=matched))
            nodes = keys.to_pylist()
            if not nodes:
                return []
        else:
            remaining = []
    if not remaining:
        return nodes
    properties = tuple(dict.fromkeys(str(flt["property"]) for flt in remaining))
    return [
        node
        for node, props in _search_rows(
            backend,
            label,
            properties,
            node_ids=nodes,
            require_all_properties=True,
        )
        if all(match(props, flt) for flt in remaining)
    ]
