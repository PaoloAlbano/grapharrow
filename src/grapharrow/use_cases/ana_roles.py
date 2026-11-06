"""Native ANA one-hop constraints and shared-entity role operations."""

from __future__ import annotations

from typing import Any

from grapharrow.ana.handles import entity_identity
from grapharrow.ana.values import (
    _ambiguous_relationship_only_constraints,
    _bool_arg,
    _columns,
    _constraint_return_properties,
    _distinct_rows,
    _expand_projected_rows,
    _int_arg,
    _normalize_aggregate_select_to_group_by,
    _row_property,
    _unique_query_var,
)
from grapharrow.use_cases.ana_patterns import (
    extend_rows,
    finish_pattern,
    freeze,
    initial_rows,
    matches,
    validate_filter,
)


def normalize_filters(
    backend: Any, filters: list[dict[str, Any]], contexts: dict[str, Any], tool: str
) -> list[dict[str, Any]]:
    normalized = []
    for flt in filters:
        if str(flt.get("var")) not in contexts:
            raise ValueError(f"{tool}: filter references an unknown variable")
        item = backend._normalize_filter_for_context(flt, contexts=contexts, context=tool)
        validate_filter(item)
        normalized.append(item)
    return normalized


def sort_pairs(pairs: list[Any], orders: list[dict[str, Any]]) -> None:
    for order in reversed(orders):
        direction = str(order.get("direction", "asc")).lower()
        if direction not in {"asc", "desc"}:
            raise ValueError("Order direction must be asc or desc")

        def key(pair: Any, order: Any = order) -> Any:
            value = (
                pair[1].get(str(order["field"]))
                if order.get("field")
                else _row_property(pair[0], str(order["var"]), str(order["property"]))
            )
            return value is None, value

        pairs.sort(key=key, reverse=direction == "desc")


def constraint_query(backend: Any, args: dict[str, Any]) -> dict[str, Any]:
    find = dict(args.get("find") or {})
    if not find.get("label"):
        raise ValueError("constraint_query requires find.label")
    label = str(find["label"])
    variable = str(find.get("as") or label.lower())
    if label not in backend._schema["labels"]:
        raise ValueError(f"Unknown find.label {label!r}")
    constraints = [item for item in args.get("where") or [] if isinstance(item, dict)]
    if not constraints:
        raise ValueError("constraint_query requires at least one where constraint")
    ambiguous = _ambiguous_relationship_only_constraints(constraints)
    if ambiguous:
        raise ValueError(
            "constraint_query cannot express whether repeated relationship-only constraints "
            "must share the same target entity. Use same_target_role_intersection instead."
        )
    rows, _ = initial_rows(backend, {"start_label": label, "start_as": variable})
    used = {variable}
    for index, constraint in enumerate(constraints):
        target_label = str(constraint["target_label"])
        if target_label not in backend._schema["labels"]:
            raise ValueError(f"Unknown target_label {target_label!r}")
        backend._validate_relationship_pattern(
            source_label=label,
            relationship_type=str(constraint["relationship_type"]),
            direction=str(constraint.get("direction", "out")),
            target_label=target_label,
        )
        target = _unique_query_var(str(constraint.get("target_as") or target_label.lower()), used)
        used.add(target)
        filters = []
        if constraint.get("property"):
            filters = normalize_filters(
                backend,
                [{**constraint, "var": target}],
                {target: {"kind": "node", "label": target_label}},
                "constraint_query.where",
            )
        rows, _ = extend_rows(
            backend,
            rows,
            variable,
            {**constraint, "as": target, "rel_as": f"cq_r{index}"},
            index,
            exclude_traversed=False,
        )
        rows = [row for row in rows if all(matches(row, flt) for flt in filters)]
    mode = str(args.get("return_mode") or "names")
    distinct = _bool_arg(args.get("distinct", True))
    offset = _int_arg(args, "offset", 0, min_value=0)
    limit = _int_arg(args, "limit", 1, min_value=1) if args.get("limit") is not None else None
    prop = str(args.get("return_property") or "name")
    alias = str(args.get("alias") or (f"{variable}_count" if mode == "count" else prop))
    if mode == "count":
        count = len({entity_identity(row[variable]) for row in rows}) if distinct else len(rows)
        return dict(backend._store([{alias: count}], kind="table", columns=[alias]))
    if mode == "entities":
        pairs = [(row, {variable: row[variable], "__rel_ids": []}) for row in rows]
        columns = None
    elif mode == "names":
        selected = [{"property": prop, "alias": alias}, *_constraint_return_properties(args)]
        columns = [item["alias"] for item in selected]
        pairs = [
            (
                row,
                {
                    item["alias"]: _row_property(row, variable, item["property"])
                    for item in selected
                },
            )
            for row in rows
        ]
    else:
        raise ValueError("constraint_query return_mode must be one of: entities, names, count")
    if distinct:
        seen = set()
        kept = []
        for pair in pairs:
            identity = freeze(pair[1])
            if identity not in seen:
                seen.add(identity)
                kept.append(pair)
        pairs = kept
    orders = [
        item
        for item in args.get("order_by") or []
        if item.get("field") == alias or (item.get("var") == variable and item.get("property"))
    ]
    if not args.get("order_by") and mode == "names":
        orders = [{"field": alias}]
    sort_pairs(pairs, orders)
    output = [pair[1] for pair in pairs[offset : None if limit is None else offset + limit]]
    metadata = (
        backend._entity_query_metadata(
            {int(ref.key) for row in rows if (ref := entity_identity(row[variable])) is not None},
            variable,
            label,
        )
        if mode == "entities"
        else None
    )
    return dict(
        backend._store(
            output,
            focus=variable if mode == "entities" else None,
            kind="rows" if mode == "entities" else "table",
            columns=columns,
            metadata=metadata,
        )
    )


def same_target_role_intersection(backend: Any, args: dict[str, Any]) -> dict[str, Any]:
    source_label, target_label = str(args["source_label"]), str(args["target_label"])
    source = str(args.get("source_as") or source_label.lower())
    target = str(args.get("target_as") or target_label.lower())
    relationships = list(args.get("relationships") or [])
    if len(relationships) < 2:
        raise ValueError("same_target_role_intersection requires at least two relationships")
    contexts = {
        source: {"kind": "node", "label": source_label},
        target: {"kind": "node", "label": target_label},
    }
    raw_filters = list(args.get("filters") or [])
    types = [item["relationship_type"] for item in relationships]
    if len(set(types)) < len(types) and any(
        flt.get("var") == target
        and flt.get("op") in {"in", "contains_any"}
        and isinstance(flt.get("value"), list)
        and len(flt["value"]) >= 2
        for flt in raw_filters
    ):
        raise ValueError(
            "Repeated roles must share the same target entity. Resolve each target "
            "separately and use entity_set_operation op='intersect' instead."
        )
    filters = normalize_filters(backend, raw_filters, contexts, "same_target_role_intersection")
    backend._validate_projection_context_properties(
        args, contexts, context="same_target_role_intersection"
    )
    rows, _ = initial_rows(backend, {"start_label": source_label, "start_as": source})
    for index, rel in enumerate(relationships):
        rows, _ = extend_rows(
            backend,
            rows,
            source,
            {
                **rel,
                "direction": rel.get("direction", "in"),
                "target_label": target_label,
                "as": target,
                "rel_as": f"r{index}",
            },
            index,
            exclude_traversed=False,
        )
    rows = [row for row in rows if all(matches(row, flt) for flt in filters)]
    select = list(args.get("select") or [])
    keep = _bool_arg(args.get("keep_entities"))
    distinct = _bool_arg(args.get("distinct", True))
    limit = _int_arg(args, "limit", 1000, min_value=1)
    pairs = []
    for row in rows:
        projected = {source: row[source], target: row[target]} if keep or not select else {}
        for item in select:
            projected[str(item.get("alias") or item["property"])] = _row_property(
                row, str(item["var"]), str(item["property"])
            )
        pairs.append((row, projected))
    # The reference caps query records before list explosion and local ordering.
    if distinct:
        seen = set()
        kept = []
        for pair in pairs:
            hidden_order = [
                _row_property(pair[0], str(item["var"]), str(item["property"]))
                for item in args.get("order_by") or []
                if not item.get("field")
            ]
            identity = freeze([pair[1], hidden_order])
            if identity not in seen:
                kept.append(pair)
                seen.add(identity)
        pairs = kept
    expanded: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for original, projected in pairs[:limit]:
        expanded.extend((original, item) for item in _expand_projected_rows(projected, select))
    if select:
        sort_pairs(expanded, list(args.get("order_by") or []))
    output = [pair[1] for pair in expanded]
    if distinct:
        output = _distinct_rows(output, use_references=True)
    if select and not keep:
        result = dict(backend._store(output, kind="table", columns=_columns(output)))
        result["finalization_hint"] = True
        return result
    for row in output:
        row["__rel_ids"] = []
    selected_vars = sorted({item["var"] for item in select if item["var"] in {source, target}})
    focus = selected_vars[0] if select and keep and len(selected_vars) == 1 else source
    metadata = (
        {
            "entity_projection_distinct_default": True,
            "preferred_fetch_columns": [
                str(item.get("alias") or item["property"]) for item in select
            ],
        }
        if select and keep
        else None
    )
    result = dict(backend._store(output, focus=focus, metadata=metadata))
    if select and keep:
        result["continuation_hint"] = (
            "Selected scalar columns were kept together with source/target entities. "
            "Use filter, entity_set_operation, expand, or project on this handle before "
            "fetching if more graph work is needed."
        )
    return result


def shared_role_aggregate(backend: Any, args: dict[str, Any]) -> dict[str, Any]:
    args = dict(_normalize_aggregate_select_to_group_by(args))
    args["distinct"] = _bool_arg(args.get("distinct", True))
    seed, shared, peer = (str(args.get(key + "_as") or key) for key in ("seed", "shared", "peer"))
    source = {"start_label": args["seed_label"], "start_as": seed}
    if args.get("seed_from"):
        handle = backend._handle(str(args["seed_from"]))
        seed = backend._resolve_seed_var_from_handle(handle, seed)
        if not any(seed in row and entity_identity(row[seed]) is not None for row in handle.rows):
            raise ValueError(
                "shared_role_aggregate seed_from does not contain the seed entity variable"
            )
        source.update(start_as=seed, **{"from": args["seed_from"]})
    seed_rel, peer_rel = dict(args["seed_relationship"]), dict(args["peer_relationship"])
    contexts = {
        seed: {"kind": "node", "label": str(args["seed_label"])},
        shared: {"kind": "node", "label": str(args["shared_label"])},
        peer: {"kind": "node", "label": str(args["peer_label"])},
    }
    for name, rel in (("seed_rel", seed_rel), ("peer_rel", peer_rel)):
        contexts[name] = {
            "kind": "relationship",
            "relationship_type": str(rel["relationship_type"]),
        }
    filters = normalize_filters(
        backend,
        [
            flt
            for section in ("seed_filters", "shared_filters", "peer_filters")
            for flt in args.get(section) or []
        ],
        contexts,
        "shared_role_aggregate",
    )
    backend._validate_projection_context_properties(args, contexts, context="shared_role_aggregate")
    for metric in args.get("metrics") or []:
        if metric.get("var") not in contexts or metric.get("op") not in {
            "count",
            "count_distinct",
            "sum",
            "avg",
            "min",
            "max",
        }:
            raise ValueError("Invalid shared_role_aggregate metric")
    rows, _ = initial_rows(backend, source)
    rows, _ = extend_rows(
        backend,
        rows,
        seed,
        {**seed_rel, "target_label": args["shared_label"], "as": shared, "rel_as": "seed_rel"},
        0,
        exclude_traversed=False,
    )
    direction = str(peer_rel.get("direction", "out"))
    if direction not in {"in", "out"}:
        raise ValueError("Shared-role direction must be in or out")
    rows, _ = extend_rows(
        backend,
        rows,
        shared,
        {
            **peer_rel,
            "direction": "in" if direction == "out" else "out",
            "target_label": args["peer_label"],
            "as": peer,
            "rel_as": "peer_rel",
        },
        1,
        exclude_traversed=False,
    )
    if _bool_arg(args.get("exclude_seed", True)) and args["seed_label"] == args["peer_label"]:
        rows = [row for row in rows if entity_identity(row[seed]) != entity_identity(row[peer])]
    rows = [row for row in rows if all(matches(row, flt) for flt in filters)]
    args.setdefault("limit", 1000)
    if args.get("select") or args.get("group_by") or args.get("metrics"):
        return finish_pattern(backend, rows, peer, args)
    pairs = [(row, {peer: row[peer], shared: row[shared], "__rel_ids": []}) for row in rows]
    sort_pairs(pairs, list(args.get("order_by") or []))
    output = [pair[1] for pair in pairs]
    if args["distinct"]:
        output = _distinct_rows(output, use_references=True)
    return dict(backend._store(output[: _int_arg(args, "limit", 1000, min_value=1)], focus=peer))
