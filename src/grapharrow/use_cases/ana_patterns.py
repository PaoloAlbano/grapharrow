"""Native binding-table execution for ANA pattern and aggregate operations."""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Iterable, Iterator
from itertools import islice
from typing import Any
from uuid import uuid4

import pyarrow as pa  # type: ignore[import-untyped]
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

_BATCH_SOURCE_THRESHOLD = 8
_EDGE_ROW_BATCH_SIZE = 256


def source_node_id(value: Any, owner: str) -> int:
    reference = entity_identity(value)
    if not isinstance(reference, EntityReference) or reference.owner != owner:
        raise ValueError("Source entity does not belong to this workflow")
    return int(reference.key)


def edges_for_sources(
    edges: pa.Table,
    node_ids: list[int],
    source_columns: tuple[str, ...],
    *,
    batch: bool = True,
    max_batch_rows: int | None = None,
) -> Iterator[Iterable[dict[str, Any]]]:
    """Resolve candidates for each source without changing edge or input order."""
    if not batch or len(node_ids) < _BATCH_SOURCE_THRESHOLD:
        for node_id in node_ids:
            mask = pc.equal(edges[source_columns[0]], node_id)
            if len(source_columns) == 2:
                mask = pc.or_(mask, pc.equal(edges[source_columns[1]], node_id))
            yield _edge_rows(edges.filter(mask))
        return

    source_ids = set(node_ids)
    keys = pa.array(list(source_ids), type=pa.int64())
    mask = pc.is_in(edges[source_columns[0]], keys)
    if len(source_columns) == 2:
        mask = pc.or_(mask, pc.is_in(edges[source_columns[1]], keys))
    candidates = edges.filter(mask)
    if max_batch_rows is not None and candidates.num_rows > max_batch_rows:
        yield from edges_for_sources(candidates, node_ids, source_columns, batch=False)
        return
    buckets: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for edge in candidates.to_pylist():
        primary = edge[source_columns[0]]
        if primary in source_ids:
            buckets[primary].append(edge)
        if len(source_columns) == 2:
            secondary = edge[source_columns[1]]
            if secondary != primary and secondary in source_ids:
                buckets[secondary].append(edge)
    for node_id in node_ids:
        yield buckets.get(node_id, [])


def _edge_rows(edges: pa.Table) -> Iterator[dict[str, Any]]:
    """Decode a bounded batch at a time so limited traversals can stop early."""
    for batch in edges.to_batches(max_chunksize=_EDGE_ROW_BATCH_SIZE):
        yield from batch.to_pylist()


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
    nodes = backend._label_ids.get(label, [])
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
            allowed.add(source_node_id(row[variable], backend._owner))
        nodes = [node for node in nodes if node in allowed]
    return [
        {variable: entity, "__rel_ids": []} for entity in backend._materialize_nodes(nodes)
    ], variable


def extend_rows(
    backend: Any,
    rows: list[dict[str, Any]],
    source: str,
    hop: dict[str, Any],
    index: int,
    *,
    exclude_traversed: bool = True,
) -> tuple[list[dict[str, Any]], str]:
    expanded, target = iter_extend_rows(
        backend, rows, source, hop, index, exclude_traversed=exclude_traversed
    )
    return list(expanded), target


def iter_extend_rows(
    backend: Any,
    rows: Iterable[dict[str, Any]],
    source: str,
    hop: dict[str, Any],
    index: int,
    *,
    exclude_traversed: bool = True,
    max_rows: int | None = None,
    distinct: bool = False,
    matched_node_ids: set[int] | None = None,
) -> tuple[Iterator[dict[str, Any]], str]:
    """Validate a hop eagerly and expand bounded input batches in binding order."""
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

    def expand() -> Iterator[dict[str, Any]]:
        inputs = iter(rows)
        emitted = 0
        while batch := list(islice(inputs, _EDGE_ROW_BATCH_SIZE)):
            node_ids = [source_node_id(row[source], backend._owner) for row in batch]
            candidates = edges_for_sources(edges, node_ids, (source_column,), max_batch_rows=4096)
            for row, selected in zip(batch, candidates, strict=True):
                for edge in selected:
                    edge_id = str(edge["edge_id"])
                    target_id = edge[target_column]
                    if target_id not in target_ids:
                        continue
                    if exclude_traversed and edge_id in row.get("__rel_ids", []):
                        continue
                    if target in row and entity_identity(row[target]) != EntityReference(
                        backend._owner, str(target_id)
                    ):
                        continue
                    if rel_var in row and str(row[rel_var]["id"]) != edge_id:
                        continue
                    if matched_node_ids is not None:
                        duplicate = target_id in matched_node_ids
                        matched_node_ids.add(target_id)
                        if (max_rows is not None and emitted >= max_rows) or distinct and duplicate:
                            continue
                    emitted += 1
                    yield {
                        **row,
                        target: backend._node(target_id),
                        rel_var: {
                            "id": edge_id,
                            "type": rel_type,
                            "start": str(edge["src_id"]),
                            "end": str(edge["dst_id"]),
                            "properties": json.loads(edge["properties_json"]),
                        },
                        "__rel_ids": [*row.get("__rel_ids", []), edge_id],
                    }

    return expand(), target


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
    limit_final_rows: bool = False,
) -> tuple[Iterable[dict[str, Any]], str, dict[str, Any], set[int] | None]:
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
    initial, current = initial_rows(backend, normalized)
    rows: Iterable[dict[str, Any]] = initial
    matched_node_ids: set[int] | None = None
    pending = list(normalized_filters)

    def filter_bound(bound: set[str]) -> None:
        nonlocal rows, pending
        ready = [flt for flt in pending if str(flt["var"]) in bound]
        rows = (row for row in rows if all(matches(row, flt) for flt in ready))
        pending = [flt for flt in pending if str(flt["var"]) not in bound]

    bound = {current}
    filter_bound(bound)
    for index, hop in enumerate(hops):
        max_rows = None
        if (
            limit_final_rows
            and index == len(hops) - 1
            and normalized.get("limit") is not None
            and not pending
            and not any(
                normalized.get(key) for key in ("select", "group_by", "metrics", "order_by")
            )
        ):
            max_rows = _int_arg(normalized, "limit", 1, min_value=1)
            matched_node_ids = set()
        rows, current = iter_extend_rows(
            backend,
            rows,
            current,
            hop,
            index,
            max_rows=max_rows,
            distinct=_bool_arg(normalized.get("distinct")),
            matched_node_ids=matched_node_ids,
        )
        bound.update({current, str(hop.get("rel_as") or f"r{index}")})
        filter_bound(bound)
    return rows, current, normalized, matched_node_ids


def freeze(value: Any) -> Any:
    if isinstance(value, EntityPayload):
        return value.reference
    if isinstance(value, dict):
        return tuple(sorted((key, freeze(item)) for key, item in value.items()))
    if isinstance(value, list):
        return tuple(freeze(item) for item in value)
    return (type(value).__name__, value)


def aggregate_rows(
    rows: Iterable[dict[str, Any]], args: dict[str, Any]
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
    rows: Iterable[dict[str, Any]],
    focus: str,
    args: dict[str, Any],
    *,
    relationship_variables: tuple[str, str] | None = None,
    node_ids: set[int] | None = None,
) -> dict[str, Any]:
    selected = list(args.get("select") or [])
    grouped = bool(args.get("metrics") or args.get("group_by"))
    distinct = _bool_arg(args.get("distinct"))
    table = grouped or bool(selected) or relationship_variables is not None
    limit = _int_arg(args, "limit", 1, min_value=1) if args.get("limit") is not None else None
    ordered = bool(args.get("order_by"))
    complete_node_ids = node_ids if node_ids is not None else set()
    pairs: list[tuple[dict[str, Any], dict[str, Any]]] = []
    if grouped:
        output, columns = aggregate_rows(rows, args)
        pairs = [(row, row) for row in output]
    else:
        columns = [str(item.get("alias") or item["property"]) for item in selected]
        if relationship_variables and not selected:
            columns = ["source", "target"]
        variables = _distinct_projection_vars(args)
        seen = set()
        for row in rows:
            if not table and node_ids is None:
                reference = entity_identity(row[focus])
                if reference is not None:
                    complete_node_ids.add(int(reference.key))
            if selected:
                projected = {
                    alias: _row_property(row, str(item["var"]), str(item["property"]))
                    for alias, item in zip(columns, selected, strict=True)
                }
            elif relationship_variables:
                source_var, target_var = relationship_variables
                projected = {"source": row[source_var], "target": row[target_var]}
            else:
                projected = {focus: row[focus], "__rel_ids": []}
            if distinct:
                key = (
                    freeze({var: row[var] for var in variables}) if variables else freeze(projected)
                )
                if key in seen:
                    continue
                seen.add(key)
            if ordered or limit is None or len(pairs) < limit:
                # Ordering may reference original bindings outside the projection.
                pairs.append((row if ordered else projected, projected))
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
    metadata = None
    if not table:
        # Prefer the declared focus label, including for empty branches.
        hops = list(args.get("hops") or [])
        target_label = str(hops[-1]["target_label"] if hops else args["start_label"])
        metadata = backend._entity_query_metadata(complete_node_ids, focus, target_label)
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
    rows, focus, normalized, node_ids = prepare_pattern(
        backend, normalized, require_hops=multi_hop, limit_final_rows=True
    )
    return finish_pattern(backend, rows, focus, normalized, node_ids=node_ids)


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
    rows, focus, normalized, _ = prepare_pattern(backend, normalized)
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
    rows, last, normalized, _ = prepare_pattern(backend, source_args, require_hops=True)
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
