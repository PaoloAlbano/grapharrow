"""Native binding-table execution for ANA pattern and aggregate operations."""

from __future__ import annotations

import json
from typing import Any
from uuid import uuid4

import pyarrow.compute as pc  # type: ignore[import-untyped]

from grapharrow.ana.handles import EntityPayload, EntityReference, _preview, entity_identity
from grapharrow.ana.values import (
    _bool_arg,
    _distinct_projection_vars,
    _int_arg,
    _metric_identity,
    _metric_scalar_value,
    _normalize_aggregate_select_to_group_by,
    _normalize_pattern_var_references,
    _optional_count_source_from_group_by,
    _pattern_var_contexts,
    _predicate,
    _row_property,
    _validate_membership_operand,
    equal,
)


def validate_filter(flt: dict[str, Any]) -> None:
    """Reject invalid predicates even when an earlier filter empties the rows."""
    op = str(flt.get("op", "eq"))
    if op not in {
        "eq",
        "eq_ci",
        "neq",
        "gt",
        "gte",
        "lt",
        "lte",
        "gte_or_null",
        "lte_or_null",
        "contains",
        "not_contains",
        "in",
        "not_in",
        "is_null",
        "is_not_null",
    }:
        raise ValueError(f"Unsupported filter op: {op}")
    _validate_membership_operand(op, flt.get("value"))


def initial_rows(backend: Any, args: dict[str, Any]) -> tuple[list[dict[str, Any]], str]:
    label = str(args["start_label"])
    variable = str(args.get("start_as") or label.lower())
    nodes = backend._nodes(label)["node_id"].to_pylist()
    if args.get("from"):
        handle = backend._handle(str(args["from"]))
        backend._guard_large_handle_input(
            handle,
            tool="pattern_query",
            var=variable,
            args=args,
            alternatives=["start from a more selective search handle"],
        )
        allowed = set()
        for row in handle.rows:
            if variable not in row:
                continue
            ref = entity_identity(row[variable])
            if not isinstance(ref, EntityReference) or ref.owner != backend._owner:
                raise ValueError("Source entity does not belong to this workflow")
            allowed.add(int(ref.key))
        nodes = [node for node in nodes if node in allowed]
    return [{variable: backend._node(node), "__rel_ids": []} for node in nodes], variable


def extend_rows(
    backend: Any,
    rows: list[dict[str, Any]],
    source: str,
    hop: dict[str, Any],
    index: int,
    *,
    exclude_traversed: bool = True,
) -> tuple[list[dict[str, Any]], str]:
    rel_type = str(hop["relationship_type"])
    target_label = str(hop["target_label"])
    target = str(hop.get("as") or target_label.lower())
    rel_var = str(hop.get("rel_as") or f"r{index}")
    direction = str(hop.get("direction", "out"))
    if direction not in {"in", "out"}:
        raise ValueError("Pattern direction must be 'out' or 'in'")
    if rel_type not in backend._schema["relationship_types"]:
        raise ValueError(f"Unknown relationship_type {rel_type!r}")
    if rel_var in {source, target}:
        raise ValueError("Relationship and node variables must be distinct")
    edges = backend._state.edge_table
    edges = edges.filter(pc.equal(edges["rel_type"], rel_type))
    source_column, target_column = (
        ("src_id", "dst_id") if direction == "out" else ("dst_id", "src_id")
    )
    target_ids = set(backend._label_ids.get(target_label, []))
    output = []
    for row in rows:
        source_ref = entity_identity(row[source])
        if not isinstance(source_ref, EntityReference) or source_ref.owner != backend._owner:
            raise ValueError("Source entity does not belong to this workflow")
        selected = edges.filter(pc.equal(edges[source_column], int(source_ref.key)))
        for edge in selected.to_pylist():
            edge_id = str(edge["edge_id"])
            target_id = edge[target_column]
            if target_id not in target_ids:
                continue
            if exclude_traversed and edge_id in row.get("__rel_ids", []):
                continue
            node = backend._node(target_id)
            if target in row and entity_identity(row[target]) != entity_identity(node):
                continue
            if rel_var in row and str(row[rel_var]["id"]) != edge_id:
                continue
            output.append(
                {
                    **row,
                    target: node,
                    rel_var: {
                        "id": edge_id,
                        "type": rel_type,
                        "start": str(edge["src_id"]),
                        "end": str(edge["dst_id"]),
                        "properties": json.loads(edge["properties_json"]),
                    },
                    "__rel_ids": [*row.get("__rel_ids", []), edge_id],
                }
            )
    return output, target


def matches(row: dict[str, Any], flt: dict[str, Any]) -> bool:
    actual = _row_property(row, str(flt["var"]), str(flt["property"]))
    expected = flt.get("value")
    op = str(flt.get("op", "eq"))
    _validate_membership_operand(op, expected)
    if op in {"is_null", "is_not_null"}:
        return bool(_predicate(actual, op, expected))
    if op in {"gte_or_null", "lte_or_null"}:
        return actual is None or matches(row, {**flt, "op": op.split("_", 1)[0]})
    if actual is None or expected is None:
        return False
    if flt.get("value_type") == "integer":
        expected = int(expected)
    if op == "eq_ci":
        return str(actual).lower() == str(expected).lower()
    if op in {"eq", "neq"}:
        return equal(actual, expected) == (op == "eq")
    return bool(_predicate(actual, op, expected))


def prepare_pattern(
    backend: Any,
    args: dict[str, Any],
    *,
    require_hops: bool = False,
) -> tuple[list[dict[str, Any]], str, dict[str, Any]]:
    hops = list(args.get("hops") or [])
    if require_hops and not hops:
        raise ValueError("multi_hop_query requires at least one hop")
    if any(not isinstance(hop, dict) for hop in hops):
        raise ValueError("Each hop must be an object")
    contexts = _pattern_var_contexts(str(args["start_label"]), args.get("start_as"), hops)
    normalized = _normalize_pattern_var_references(
        _normalize_aggregate_select_to_group_by(dict(args)),
        contexts,
    )
    filters = list(normalized.get("filters") or [])
    for index, hop in enumerate(hops):
        for item in hop.get("relationship_filters") or []:
            filters.append({"var": str(hop.get("rel_as") or f"r{index}"), **item})
    normalized_filters = []
    for flt in filters:
        if not isinstance(flt, dict) or str(flt.get("var")) not in contexts:
            raise ValueError("Filter references an unknown pattern variable")
        normalized_filters.append(
            backend._normalize_filter_for_context(
                flt,
                contexts=contexts,
                context="pattern_query.filters",
            )
        )
        validate_filter(normalized_filters[-1])
    backend._validate_projection_context_properties(normalized, contexts, context="pattern_query")
    for metric in normalized.get("metrics") or []:
        if metric.get("var") not in contexts:
            raise ValueError("Metric references an unknown pattern variable")
        if metric.get("op") not in {"count", "count_distinct", "sum", "avg", "min", "max"}:
            raise ValueError(f"Unsupported metric op: {metric.get('op')}")
    rows, current = initial_rows(backend, normalized)
    pending = list(normalized_filters)

    def filter_bound(bound: set[str]) -> None:
        nonlocal rows, pending
        ready = [flt for flt in pending if str(flt["var"]) in bound]
        rows = [row for row in rows if all(matches(row, flt) for flt in ready)]
        pending = [flt for flt in pending if str(flt["var"]) not in bound]

    bound = {current}
    filter_bound(bound)
    for index, hop in enumerate(hops):
        rows, current = extend_rows(backend, rows, current, hop, index)
        bound.update({current, str(hop.get("rel_as") or f"r{index}")})
        filter_bound(bound)
    return rows, current, normalized


def freeze(value: Any) -> Any:
    if isinstance(value, EntityPayload):
        return value.reference
    if isinstance(value, dict):
        return tuple(sorted((key, freeze(item)) for key, item in value.items()))
    if isinstance(value, list):
        return tuple(freeze(item) for item in value)
    return (type(value).__name__, value)


def aggregate_rows(
    rows: list[dict[str, Any]], args: dict[str, Any]
) -> tuple[list[dict[str, Any]], list[str]]:
    grouping = list(args.get("group_by") or [])
    metrics = list(args.get("metrics") or [])
    columns = [str(item.get("alias") or item["property"]) for item in grouping]
    columns += [str(metric["alias"]) for metric in metrics]
    groups: dict[Any, tuple[list[Any], list[dict[str, Any]]]] = {}
    if not grouping:
        groups[()] = ([], [])
    for row in rows:
        values = [_row_property(row, str(item["var"]), str(item["property"])) for item in grouping]
        key = freeze(values)
        groups.setdefault(key, (values, []))[1].append(row)
    result = []
    for values, members in groups.values():
        out = dict(zip(columns[: len(grouping)], values, strict=True))
        for metric in metrics:
            op, var, alias = str(metric["op"]), str(metric["var"]), str(metric["alias"])
            value: Any
            if op == "count":
                value = sum(row.get(var) is not None for row in members)
            elif op == "count_distinct":
                value = len(
                    {
                        freeze(_metric_identity(row, var))
                        for row in members
                        if row.get(var) is not None
                    }
                )
            else:
                scalars = [
                    _metric_scalar_value(row, var, metric.get("property")) for row in members
                ]
                scalars = [item for item in scalars if item is not None]
                if op == "sum":
                    value = sum(scalars)
                elif op == "avg":
                    value = sum(scalars) / len(scalars) if scalars else None
                elif op == "min":
                    value = min(scalars) if scalars else None
                elif op == "max":
                    value = max(scalars) if scalars else None
                else:
                    raise ValueError(f"Unsupported metric op: {op}")
            out[alias] = value
        result.append(out)
    return result, columns


def finish_pattern(
    backend: Any,
    rows: list[dict[str, Any]],
    focus: str,
    args: dict[str, Any],
    *,
    relationship_variables: tuple[str, str] | None = None,
) -> dict[str, Any]:
    selected = list(args.get("select") or [])
    grouped = bool(args.get("metrics") or args.get("group_by"))
    distinct = _bool_arg(args.get("distinct"))
    if grouped:
        output, columns = aggregate_rows(rows, args)
        pairs = [(row, row) for row in output]
    else:
        if selected:
            columns = [str(item.get("alias") or item["property"]) for item in selected]
            pairs = [
                (
                    row,
                    {
                        alias: _row_property(row, str(item["var"]), str(item["property"]))
                        for alias, item in zip(columns, selected, strict=True)
                    },
                )
                for row in rows
            ]
        elif relationship_variables:
            columns = ["source", "target"]
            source_var, target_var = relationship_variables
            pairs = [(row, {"source": row[source_var], "target": row[target_var]}) for row in rows]
        else:
            columns = []
            pairs = [(row, {focus: row[focus], "__rel_ids": []}) for row in rows]
        if distinct:
            variables = _distinct_projection_vars(args)
            seen = set()
            kept = []
            for original, projected in pairs:
                key = (
                    freeze({var: original[var] for var in variables})
                    if variables
                    else freeze(projected)
                )
                if key not in seen:
                    seen.add(key)
                    kept.append((original, projected))
            pairs = kept
    for order in reversed(args.get("order_by") or []):
        direction = str(order.get("direction", "asc")).lower()
        if direction not in {"asc", "desc"}:
            raise ValueError("Order direction must be asc or desc")
        field = order.get("field")
        if not field and grouped:
            field = next(
                (
                    str(item.get("alias") or item["property"])
                    for item in args.get("group_by") or []
                    if item["var"] == order.get("var") and item["property"] == order.get("property")
                ),
                None,
            )
        if field and field not in columns:
            raise ValueError(f"Unknown order field: {field}")

        def key(pair: Any, field: Any = field, order: Any = order) -> Any:
            value = (
                pair[1].get(field)
                if field
                else _row_property(pair[0], str(order.get("var")), str(order.get("property")))
            )
            return value is None, value

        pairs.sort(key=key, reverse=direction == "desc")
    if args.get("limit") is not None:
        pairs = pairs[: _int_arg(args, "limit", 1, min_value=1)]
    output = [pair[1] for pair in pairs]
    table = grouped or bool(selected) or relationship_variables is not None
    metadata = None
    if not table:
        node_ids = {
            int(ref.key) for row in rows if (ref := entity_identity(row[focus])) is not None
        }
        # Prefer the declared focus label, including for empty branches.
        hops = list(args.get("hops") or [])
        target_label = str(hops[-1]["target_label"] if hops else args["start_label"])
        metadata = backend._entity_query_metadata(node_ids, focus, target_label)
    return dict(
        backend._store(
            output,
            focus=None if table else focus,
            kind="table" if table else "rows",
            columns=columns if output else [],
            metadata=metadata,
        )
    )


def pattern_query(backend: Any, args: dict[str, Any], *, multi_hop: bool = False) -> dict[str, Any]:
    normalized = dict(args)
    if not multi_hop:
        normalized.setdefault("distinct", True)
        if not normalized.get("hops") and not any(
            normalized.get(k) for k in ("select", "group_by", "metrics")
        ):
            var = str(normalized.get("start_as") or str(normalized["start_label"]).lower())
            normalized["select"] = [{"var": var, "property": "name", "alias": "name"}]
    rows, focus, normalized = prepare_pattern(backend, normalized, require_hops=multi_hop)
    return finish_pattern(backend, rows, focus, normalized)


def relationship_query(backend: Any, args: dict[str, Any]) -> dict[str, Any]:
    source = str(args.get("source_as") or str(args["source_label"]).lower())
    target = str(args.get("target_as") or str(args["target_label"]).lower())
    normalized = {
        **args,
        "start_label": args["source_label"],
        "start_as": source,
        "hops": [
            {
                "relationship_type": args["relationship_type"],
                "target_label": args["target_label"],
                "direction": args.get("direction", "out"),
                "as": target,
                "rel_as": args.get("rel_as") or "rel",
            }
        ],
    }
    rows, focus, normalized = prepare_pattern(backend, normalized)
    return finish_pattern(backend, rows, focus, normalized, relationship_variables=(source, target))


def top_entities_by_property(backend: Any, args: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(args)
    hops = list(args.get("hops") or [])
    target = str(
        args.get("target")
        or (
            (hops[-1].get("as") or str(hops[-1]["target_label"]).lower())
            if hops
            else args.get("start_as") or str(args["start_label"]).lower()
        )
    )
    prop = str(args["property"])
    direction = str(args.get("order") or "desc").lower()
    if direction not in {"asc", "desc"}:
        raise ValueError("top_entities_by_property order must be asc or desc")
    contexts = _pattern_var_contexts(str(args["start_label"]), args.get("start_as"), hops)
    if target not in contexts or contexts[target]["kind"] != "node":
        raise ValueError("Ranking target must be a node variable produced by the pattern")
    selected = [dict(item) for item in args.get("select") or []]
    if not selected:
        selected = [{"var": target, "property": "name", "alias": args.get("name_alias") or "name"}]
    existing = next(
        (item for item in selected if item["var"] == target and item["property"] == prop), None
    )
    hidden = None
    if existing:
        rank_alias = str(existing.get("alias") or existing["property"])
    else:
        rank_alias = (
            str(args.get("property_alias") or prop)
            if _bool_arg(args.get("include_property"))
            else f"__rank_{uuid4().hex}"
        )
        if not _bool_arg(args.get("include_property")):
            hidden = rank_alias
        selected.append({"var": target, "property": prop, "alias": rank_alias})
    normalized.update(
        select=selected,
        order_by=[{"field": rank_alias, "direction": direction}],
        limit=_int_arg(args, "limit", 1, min_value=1),
        distinct=args.get("distinct", True),
    )
    result = pattern_query(backend, normalized)
    handle = backend._handle(str(result["handle"]))
    if hidden:
        for row in handle.rows:
            row.pop(hidden, None)
        handle.columns = [column for column in handle.columns if column != hidden]
        result["columns"] = handle.columns
        result["preview"] = _preview(handle.rows, focus=handle.focus, columns=handle.columns)
    result["ranking"] = {
        "target": target,
        "property": prop,
        "order": direction,
        "limit": normalized["limit"],
    }
    result["finalization_hint"] = (
        "This handle is already ranked. Fetch it if its columns match the question."
    )
    return result


def optional_expand_count(backend: Any, args: dict[str, Any]) -> dict[str, Any]:
    handle = backend._handle(str(args["from"]))
    source = str(args["source"])
    grouping = args.get("group_by") or [{"var": source, "property": "name", "alias": "name"}]
    alias = str(args.get("alias") or "count")
    distinct = args.get("distinct") is not False
    seen = set()
    sources = []
    for row in handle.rows:
        if source not in row:
            continue
        identity = entity_identity(row[source])
        if identity not in seen:
            sources.append(row)
            seen.add(identity)
    if args.get("limit") is not None:
        sources = sources[: _int_arg(args, "limit", 1, min_value=1)]
    out = []
    target = str(args.get("as") or str(args["target_label"]).lower())
    if target == source:
        target = "__optional_target"
    hop = {**args, "as": target, "rel_as": "__optional_relationship"}
    extend_rows(backend, [], source, hop, 0, exclude_traversed=False)
    for row in sources:
        expanded, _ = extend_rows(
            backend,
            [{source: row[source], "__rel_ids": []}],
            source,
            hop,
            0,
            exclude_traversed=False,
        )
        count = (
            len({entity_identity(item[target]) for item in expanded}) if distinct else len(expanded)
        )
        out.append(
            {
                **{
                    str(item.get("alias") or item["property"]): _row_property(
                        row, str(item["var"]), str(item["property"])
                    )
                    for item in grouping
                },
                alias: count,
            }
        )
    columns = [str(item.get("alias") or item["property"]) for item in grouping] + [alias]
    result = dict(backend._store(out, kind="table", columns=columns))
    result["finalization_hint"] = (
        "This is a left-preserving aggregate table. Fetch this handle next."
    )
    return result


def optional_count_by_pattern(backend: Any, args: dict[str, Any]) -> dict[str, Any]:
    optional = args.get("optional_relationship")
    if not isinstance(optional, dict):
        raise ValueError("optional_relationship must describe a relationship hop")
    source_args = {**args, "metrics": [], "select": []}
    rows, last, normalized = prepare_pattern(backend, source_args, require_hops=True)
    contexts = _pattern_var_contexts(str(args["start_label"]), args.get("start_as"), args["hops"])
    grouping = list(args.get("group_by") or [])
    source = str(
        args.get("source") or _optional_count_source_from_group_by(grouping, contexts) or last
    )
    if source not in contexts or contexts[source]["kind"] != "node":
        raise ValueError("optional_count_by_pattern source must be a bound node variable")
    if not grouping:
        grouping = [{"var": source, "property": "name", "alias": "name"}]
    kept_vars = {source}
    for item in grouping:
        variable = str(item["var"])
        if variable not in contexts or contexts[variable]["kind"] != "node":
            raise ValueError("Optional grouping must reference a bound node variable")
        kept_vars.add(variable)
    target = str(optional.get("as") or str(optional["target_label"]).lower())
    rel_var = str(optional.get("rel_as") or "optional_rel")
    optional_contexts = {
        **contexts,
        target: {"kind": "node", "label": str(optional["target_label"])},
        rel_var: {"kind": "relationship", "relationship_type": str(optional["relationship_type"])},
    }
    filters = [
        backend._normalize_filter_for_context(
            {"var": rel_var, **flt},
            contexts=optional_contexts,
            context="optional_count_by_pattern.relationship_filters",
        )
        for flt in optional.get("relationship_filters") or []
    ]
    for flt in filters:
        validate_filter(flt)
    hop = {**optional, "as": target, "rel_as": rel_var}
    extend_rows(backend, [], source, hop, 0, exclude_traversed=False)
    seen = set()
    optional_rows = []
    for row in rows:
        bindings = {var: row[var] for var in kept_vars}
        key = freeze(bindings)
        if key in seen:
            continue
        seen.add(key)
        expanded, _ = extend_rows(
            backend,
            [{**bindings, "__rel_ids": []}],
            source,
            hop,
            0,
            exclude_traversed=False,
        )
        expanded = [item for item in expanded if all(matches(item, flt) for flt in filters)]
        optional_rows.extend(expanded or [{**bindings, target: bindings.get(target)}])
    metric_alias = next((m.get("alias") for m in args.get("metrics") or [] if m.get("alias")), None)
    alias = str(args.get("alias") or metric_alias or "count")
    result = finish_pattern(
        backend,
        optional_rows,
        source,
        {
            **normalized,
            "select": [],
            "group_by": grouping,
            "metrics": [
                {
                    "var": target,
                    "op": "count_distinct" if args.get("distinct") is not False else "count",
                    "alias": alias,
                }
            ],
        },
    )
    result["finalization_hint"] = (
        "This is a left-preserving optional aggregate table. Fetch it next."
    )
    columns = [str(item.get("alias") or item["property"]) for item in grouping] + [alias]
    backend._handle(str(result["handle"])).columns = columns
    result["columns"] = columns
    return result
