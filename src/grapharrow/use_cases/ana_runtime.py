"""GraphArrow row composition for ANA workflow handles."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, ClassVar

from grapharrow.ana.handles import Handle, HandleStore, display, entity_identity
from grapharrow.ana.values import (
    _expand_projected_rows,
    _int_arg,
    _row_property,
    freeze,
)
from grapharrow.use_cases import ana_patterns, ana_roles
from grapharrow.use_cases.ana_schema import SchemaTools


def _unique(rows: Iterable[dict[str, Any]], keys: Iterable[Any]) -> list[dict[str, Any]]:
    distinct: dict[Any, dict[str, Any]] = {}
    for row, key in zip(rows, keys, strict=True):
        distinct.setdefault(freeze(key), row)
    return list(distinct.values())


class AnaRuntime(SchemaTools):
    """Operations that consume materialized results rather than reading graph tables."""

    _handle_store: HandleStore
    last_fetch: list[list[Any]] | None
    _DEFAULT_MAX_INPUT_ROWS: ClassVar[int] = 5000

    def _store(
        self,
        rows: list[dict[str, Any]],
        *,
        focus: str | None = None,
        kind: str = "rows",
        columns: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        raise NotImplementedError

    def _combined_server_side_entity_metadata(
        self,
        *,
        op: str,
        left: Handle,
        right: Handle,
        left_var: str,
        right_var: str,
        out_var: str,
    ) -> dict[str, Any] | None:
        return None

    def _server_side_entity_set_count(
        self, handle: Handle, *, var: Any, distinct: bool
    ) -> int | None:
        return None

    def _handle(self, name: str) -> Handle:
        return self._handle_store.get(name)

    def _guard_large_handle_input(
        self,
        handle: Handle,
        *,
        tool: str,
        var: str,
        args: dict[str, Any],
        alternatives: list[str],
    ) -> None:
        cap = _int_arg(args, "max_input_rows", self._DEFAULT_MAX_INPUT_ROWS, min_value=1)
        if not args.get("allow_large") and len(handle.rows) > cap:
            raise ValueError(f"{tool} input {var!r} exceeds {cap} rows; {'; '.join(alternatives)}")

    def _resolve_seed_var_from_handle(self, handle: Handle, preferred: str | None) -> str:
        if preferred in handle.variables:
            return str(preferred)
        if handle.focus is not None:
            return handle.focus
        if len(handle.variables) == 1:
            return handle.variables[0]
        if not handle.rows and preferred:
            return preferred
        raise ValueError(f"Select an entity variable from handle {handle.id!r}")

    def _fetch(self, args: dict[str, Any]) -> dict[str, Any]:
        handle = self._handle(args["from"])
        offset = _int_arg(args, "offset", 0)
        limit = _int_arg(args, "limit", 1000, min_value=1)
        columns = handle.metadata.get("preferred_fetch_columns") or handle.columns
        materialized = handle.rows
        if handle.metadata.get("entity_projection_distinct_default") and handle.variables:
            variables = handle.variables
            materialized = _unique(
                handle.rows,
                ([entity_identity(row.get(var)) for var in variables] for row in handle.rows),
            )
        page = materialized[offset : offset + limit]
        if (
            handle.kind == "rows"
            and handle.focus
            and not handle.metadata.get("preferred_fetch_columns")
        ):
            columns = [f"{handle.focus}.name"]
            rows = [[display(row.get(handle.focus))] for row in page]
        else:
            rows = [[display(row.get(key)) for key in columns] for row in page]
        next_offset = offset + len(rows)
        more = next_offset < len(materialized)
        self.last_fetch = rows
        return {
            "contract_version": "ana-fetch-v1",
            "columns": columns,
            "rows": rows,
            "offset": offset,
            "limit": limit,
            "returned_count": len(rows),
            "total_count": len(materialized),
            "next_offset": next_offset if more else None,
            "truncated": more,
            "preview": rows[:20],
            "preview_truncated": len(rows) > 20,
        }

    def _summarize_handle(self, args: dict[str, Any]) -> dict[str, Any]:
        handle = self._handle(args["from"])
        count = _int_arg(args, "sample_limit", 5, min_value=1)
        summary = self._handle_store.summary(handle)
        samples = [
            {key: display(row.get(key)) for key in handle.columns} for row in handle.rows[:count]
        ]
        variables = []
        traversals: list[dict[str, Any]] = []
        for key in handle.columns:
            values = [row[key] for row in handle.rows if row.get(key) is not None]
            entities = [value for value in values if entity_identity(value)]
            labels = sorted({label for value in entities for label in value["labels"]})
            variables.append(
                {
                    "name": key,
                    "shape": "entity" if entities else "scalar",
                    "labels": labels,
                    "properties": sorted(
                        {prop for value in entities for prop in value["properties"]}
                    ),
                    "non_null_count": len(values),
                    "sample_values": [display(value) for value in values[:count]],
                }
            )
            for label in labels:
                traversals.extend(
                    {"var": key, "label": label, **navigation}
                    for navigation in self._traversals(label)
                )
        return {
            key: value
            for key, value in summary.items()
            if key not in {"preview", "entity_variables"}
        } | {
            "variables": variables,
            "samples": samples,
            "next_traversals": traversals,
            "planning_hints": ["Use entity variables for traversal or set operations."],
        }

    def _handle_recap(self, args: dict[str, Any]) -> dict[str, Any]:
        handle = self._handle(args["from"])
        limit = _int_arg(args, "limit", 10, min_value=1)
        return self._handle_store.summary(handle) | {
            "lineage": self._handle_store.lineage.get(handle.id, [])[-limit:],
            "available_handles": [
                self._handle_store.summary(item) for item in self._handle_store.handles.values()
            ],
            "planning_hints": ["Use only handle names returned in this workflow."],
        }

    def _count_handle(self, args: dict[str, Any]) -> dict[str, Any]:
        handle = self._handle(args["from"])
        variable = args.get("var") or handle.focus
        distinct = bool(args.get("distinct"))
        count = self._server_side_entity_set_count(handle, var=args.get("var"), distinct=distinct)
        if count is None:
            if distinct:
                values = (
                    (row.get(variable) for row in handle.rows) if variable else iter(handle.rows)
                )
                count = len({freeze(value) for value in values if value is not None})
            else:
                count = (
                    len(handle.rows)
                    if variable is None
                    else sum(row.get(variable) is not None for row in handle.rows)
                )
        alias = str(args.get("alias") or "count")
        return self._store([{alias: count}], kind="table", columns=[alias])

    def _filter(self, args: dict[str, Any]) -> dict[str, Any]:
        handle = self._handle(args["from"])
        var = args["var"]
        label = next(
            (
                row[var]["labels"][0]
                for row in handle.rows
                if entity_identity(row.get(var)) and row[var].get("labels")
            ),
            None,
        )
        predicate = self._normalize_filter_for_context(
            args,
            contexts={var: {"kind": "node", "label": label}},
            context="filter",
        )
        ana_patterns.validate_filter(predicate)
        rows = [row for row in handle.rows if ana_patterns.matches(row, predicate)]
        return self._store(rows, focus=handle.focus, kind=handle.kind, columns=handle.columns)

    def _filter_same_node(self, args: dict[str, Any]) -> dict[str, Any]:
        handle = self._handle(args["from"])
        left, right = args["left_var"], args["right_var"]
        rows = [
            row
            for row in handle.rows
            if entity_identity(row.get(left)) is not None
            and entity_identity(row.get(left)) == entity_identity(row.get(right))
        ]
        return self._store(rows, focus=handle.focus, kind=handle.kind, columns=handle.columns)

    def _project(self, args: dict[str, Any]) -> dict[str, Any]:
        handle = self._handle(args["from"])
        fields = args["select"]
        columns = list(dict.fromkeys(item.get("alias") or item["property"] for item in fields))
        keep = bool(args.get("keep_entities"))
        pairs: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for row in handle.rows:
            selected = {
                item.get("alias") or item["property"]: _row_property(
                    row,
                    item["var"],
                    item["property"],
                )
                for item in fields
            }
            pairs.extend((row, projected) for projected in _expand_projected_rows(selected, fields))
        if args.get("distinct"):
            entity_scope = args.get("distinct_scope") == "entity" or (
                args.get("distinct_scope") is None
                and ("native_entity_set" in handle.metadata or keep)
            )
            seen = set()
            unique = []
            for row, projected in pairs:
                identities = [entity_identity(row.get(item["var"])) for item in fields]
                key = freeze([projected, identities] if entity_scope else projected)
                if key not in seen:
                    unique.append((row, projected))
                    seen.add(key)
            pairs = unique
        orders = args.get("order_by") or []
        for order in orders:
            field = order.get("field")
            if field is not None and field not in columns:
                raise ValueError(f"Unknown order field: {field}")
        ana_roles.sort_pairs(pairs, orders)
        limit = _int_arg(args, "limit", len(pairs), min_value=1)
        output = [row | projected if keep else projected for row, projected in pairs[:limit]]
        return self._store(output, kind="table", columns=columns)

    def _group_handle(self, args: dict[str, Any]) -> dict[str, Any]:
        handle = self._handle(args["from"])
        grouping = args.get("group_by") or []
        metrics = args.get("metrics") or [
            {"var": handle.focus or "*", "op": "count", "alias": "count"}
        ]
        rows = handle.rows
        if any(metric.get("var") == "*" for metric in metrics):
            rows = [{**row, "*": 1} for row in rows]
        result, columns = ana_patterns.aggregate_rows(
            rows, {"group_by": grouping, "metrics": metrics}
        )
        return self._store(result, kind="table", columns=columns)

    def _compare(self, args: dict[str, Any]) -> dict[str, Any]:
        left, right = (self._handle(args[key]).rows for key in ("left", "right"))
        if len(left) != 1 or len(right) != 1:
            raise ValueError("compare requires exactly one row from each handle")
        lv, rv, prop = args["left_var"], args["right_var"], args["property"]
        a, b = _row_property(left[0], lv, prop), _row_property(right[0], rv, prop)
        op = args["op"]
        if op == "difference":
            result = a - b
        elif op in {"argmax", "argmin"}:
            select_left = a >= b if op == "argmax" else a <= b
            result = display(left[0].get(lv) if select_left else right[0].get(rv))
        else:
            result = ana_patterns.matches(
                {"value": a}, {"var": "_", "property": "value", "op": op, "value": b}
            )
        alias = args.get("alias") or "answer"
        return self._store([{alias: result}], kind="table", columns=[alias])

    def _scalar_compute(self, args: dict[str, Any]) -> dict[str, Any]:
        values = []
        names = []
        for item in args["inputs"]:
            handle = self._handle(item["handle"])
            index = _int_arg(item, "row_index", 0)
            column = item["column"]
            if index >= len(handle.rows) or column not in handle.rows[index]:
                raise ValueError(f"No cell {column!r} at row {index}")
            values.append(handle.rows[index][column])
            names.append(item.get("alias") or column)
        if not values:
            raise ValueError("scalar_compute requires at least one input")
        op = args["op"]
        if op in {"difference", "absolute_difference"}:
            if len(values) != 2:
                raise ValueError(f"{op} requires two inputs")
            answer = values[0] - values[1]
            if op == "absolute_difference":
                answer = abs(answer)
        elif op == "sum":
            answer = sum(values)
        elif op == "avg":
            answer = sum(values) / len(values)
        elif op in {"min", "max"}:
            answer = min(values) if op == "min" else max(values)
        else:
            index = (min if op == "argmin" else max)(range(len(values)), key=values.__getitem__)
            answer = names[index]
        alias = args.get("alias") or "answer"
        return self._store([{alias: answer}], kind="table", columns=[alias])

    def _join_handles(self, args: dict[str, Any]) -> dict[str, Any]:
        left, right = (self._handle(args[key]) for key in ("left", "right"))
        left_var = self._resolve_seed_var_from_handle(left, args.get("left_var"))
        right_var = self._resolve_seed_var_from_handle(right, args.get("right_var"))
        index: dict[Any, list[dict[str, Any]]] = {}
        for row in right.rows:
            key = entity_identity(row.get(right_var))
            if key is not None:
                index.setdefault(key, []).append(row)
        op = args.get("op") or "inner"
        alias = args.get("as") or left_var
        output: list[dict[str, Any]] = []
        for row in left.rows:
            matches = index.get(entity_identity(row.get(left_var)), [])
            if op == "inner":
                output.extend(other | row | {alias: row.get(left_var)} for other in matches)
            elif bool(matches) == (op == "left_semi"):
                output.append(row | {alias: row.get(left_var)})
        return self._store(output, focus=alias)

    def _combine(self, args: dict[str, Any]) -> dict[str, Any]:
        left, right = (self._handle(args[key]) for key in ("left", "right"))
        left_var = self._resolve_seed_var_from_handle(left, args.get("left_var"))
        right_var = self._resolve_seed_var_from_handle(right, args.get("right_var"))
        alias = args.get("as") or left_var
        op = args["op"]
        a = [row[left_var] for row in left.rows if entity_identity(row.get(left_var))]
        b = [row[right_var] for row in right.rows if entity_identity(row.get(right_var))]
        keys = {entity_identity(item) for item in b}
        if op == "union":
            candidates = [*a, *b]
        elif op in {"intersect", "difference"}:
            candidates = [
                item for item in a if (entity_identity(item) in keys) == (op == "intersect")
            ]
        else:
            raise ValueError(f"Unsupported set operation: {op}")
        result = _unique(({alias: item} for item in candidates), candidates)
        metadata = self._combined_server_side_entity_metadata(
            op=op,
            left=left,
            right=right,
            left_var=left_var,
            right_var=right_var,
            out_var=alias,
        )
        return self._store(result, focus=alias, metadata=metadata)

    def _entity_set_operation(self, args: dict[str, Any]) -> dict[str, Any]:
        operands = args["operands"]
        if len(operands) < 2:
            raise ValueError("entity_set_operation requires at least two operands")
        cap = _int_arg(args, "max_operand_rows", 10000, min_value=1)
        for operand in operands:
            handle = self._handle(operand["handle"])
            if len(handle.rows) > cap and not args.get("allow_large"):
                raise ValueError(f"Set operand {handle.id!r} exceeds {cap} rows")
            if handle.rows and operand["var"] not in handle.variables:
                raise ValueError(f"Unknown entity variable {operand['var']!r} in {handle.id!r}")
        current = operands[0]
        alias = args.get("as") or current["var"]
        for right in operands[1:]:
            result = self._combine(
                {
                    "left": current["handle"],
                    "left_var": current["var"],
                    "right": right["handle"],
                    "right_var": right["var"],
                    "op": args["op"],
                    "as": alias,
                }
            )
            current = {"handle": result["handle"], "var": alias}
        return result
