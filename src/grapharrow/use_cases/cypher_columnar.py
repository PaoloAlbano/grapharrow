"""Narrow Parquet scans and Arrow projections for composable node reads.

Eligibility is conservative: one labeled node scan followed by projections.
Physical encoding metadata is checked before offering any reader predicates.
Unsupported types or kernels fall back on the same captured snapshot.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.compute as pc  # type: ignore[import-untyped]

from grapharrow.adapters.tabular import sort_window_indices
from grapharrow.domain.scan import ScanPredicate
from grapharrow.use_cases import cypher as core
from grapharrow.use_cases.cypher_expression import Expression, freeze, has_aggregate
from grapharrow.use_cases.cypher_pipeline import (
    MatchStage,
    ProjectionStage,
    ReadBranch,
    ReadQuery,
    ReadRuntime,
)
from grapharrow.use_cases.cypher_window import (
    limit_optimizations_enabled,
    safe_to_defer,
    variables,
    window_value,
)
from grapharrow.use_cases.graph import GraphStateUseCase, QueryResult


class UnsupportedColumnar(Exception):
    """An internal eligibility signal, never a user-visible query error."""


@dataclass(frozen=True)
class EntityColumns:
    properties: dict[str, str]


@dataclass
class Batch:
    table: pa.Table
    scope: dict[str, str | EntityColumns]


@dataclass(frozen=True)
class ScanPlan:
    match: MatchStage
    pattern: core.NodePattern
    fields: tuple[str, ...]
    projections: tuple[ProjectionStage, ...]
    residual: tuple[ProjectionStage, ...]


def dependencies(expression: Expression, entities: set[str]) -> set[str]:
    kind, value, args = expression.kind, expression.value, expression.arguments
    if kind == "property" and args[0].kind == "variable" and args[0].value in entities:
        return {value}
    if kind == "literal":
        if value is not None and type(value) not in {bool, int, float, str}:
            raise UnsupportedColumnar
        return set()
    if kind == "variable":
        if value in entities:
            raise UnsupportedColumnar
        return set()
    if kind == "binary" and value not in {"+", "-", "*"}:
        raise UnsupportedColumnar
    if kind == "function" and value not in {"coalesce", "abs", "size", "tolower", "toupper"}:
        raise UnsupportedColumnar
    if kind not in {
        "binary",
        "unary",
        "function",
        "case",
        "compare",
        "is_null",
        "not",
        "and",
        "or",
        "xor",
        "string",
    }:
        raise UnsupportedColumnar
    fields: set[str] = set()
    for child in args:
        fields.update(dependencies(child, entities))
    return fields


def plan_branch(branch: ReadBranch) -> ScanPlan:
    if branch.initial_scope or not branch.stages or not isinstance(branch.stages[0], MatchStage):
        raise UnsupportedColumnar
    match = branch.stages[0]
    if match.optional or len(match.patterns) != 1:
        raise UnsupportedColumnar
    pattern = match.patterns[0]
    if not isinstance(pattern, core.NodePattern) or len(pattern.labels) != 1:
        raise UnsupportedColumnar
    fields = set(pattern.properties)
    for value in pattern.properties.values():
        dependencies(Expression("literal", value), set())
    entities = {pattern.variable}
    if match.predicate is not None:
        fields.update(dependencies(match.predicate, entities))
    projections = []
    residual: list[ProjectionStage] = []
    for stage in branch.stages[1:]:
        if not isinstance(stage, ProjectionStage) or stage.globals:
            raise UnsupportedColumnar
        if residual:
            residual.append(stage)
            continue
        try:
            if stage.distinct or any(has_aggregate(e) for _, e in stage.items):
                raise UnsupportedColumnar
            stage_fields: set[str] = set()
            projected_entities = set()
            for name, expression in stage.items:
                if expression.kind == "variable" and expression.value in entities:
                    projected_entities.add(name)
                else:
                    stage_fields.update(dependencies(expression, entities))
            for expression, _ in stage.ordering:
                stage_fields.update(dependencies(expression, entities | projected_entities))
            if stage.predicate is not None:
                stage_fields.update(dependencies(stage.predicate, entities | projected_entities))
        except UnsupportedColumnar:
            if entities:
                raise
            residual.append(stage)
            continue
        fields.update(stage_fields)
        entities = projected_entities
        projections.append(stage)
    if entities or not projections:
        raise UnsupportedColumnar
    return ScanPlan(match, pattern, tuple(sorted(fields)), tuple(projections), tuple(residual))


def _category(dtype: pa.DataType) -> str:
    if pa.types.is_null(dtype):
        return "null"
    if pa.types.is_boolean(dtype):
        return "bool"
    if pa.types.is_integer(dtype):
        return "int"
    if pa.types.is_floating(dtype):
        return "float"
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return "str"
    raise UnsupportedColumnar


def _compatible(values: list[Any]) -> str:
    kinds = {_category(value.type) for value in values} - {"null"}
    if len(kinds) > 1:
        raise UnsupportedColumnar
    return next(iter(kinds), "null")


def evaluate(expression: Expression, batch: Batch) -> Any:
    kind, value, args = expression.kind, expression.value, expression.arguments

    def ev(child: Expression) -> Any:
        return evaluate(child, batch)

    if kind == "literal":
        return pa.scalar(value)
    if kind == "variable":
        binding = batch.scope[value]
        return binding if isinstance(binding, EntityColumns) else batch.table[binding]
    if kind == "property":
        entity = ev(args[0])
        if not isinstance(entity, EntityColumns):
            raise UnsupportedColumnar
        return batch.table[entity.properties[value]]
    if kind == "is_null":
        result = pc.is_null(ev(args[0]), nan_is_null=False)
        return result if value else pc.invert(result)
    if kind in {"and", "or", "xor", "not"}:
        operands = [ev(child) for child in args]
        if _compatible(operands) not in {"bool", "null"}:
            raise UnsupportedColumnar
        if kind == "not":
            return pc.invert(operands[0])
        result = operands[0]
        for right in operands[1:]:
            result = {"and": pc.and_kleene, "or": pc.or_kleene, "xor": pc.xor}[kind](result, right)
        return result
    if kind == "compare":
        operands = [ev(child) for child in args]
        result = pa.scalar(True)
        for operator, left, right in zip(value, operands[:-1], operands[1:], strict=True):
            _compatible([left, right])
            mask = {
                "=": pc.equal,
                "<>": pc.not_equal,
                "<": pc.less,
                "<=": pc.less_equal,
                ">": pc.greater,
                ">=": pc.greater_equal,
            }[operator](left, right)
            result = pc.and_kleene(result, mask)
        return result
    if kind in {"binary", "unary"}:
        operands = [ev(child) for child in args]
        category = _compatible(operands)
        if category not in {"null", "int", "float"}:
            raise UnsupportedColumnar
        if kind == "unary":
            return operands[0] if value == "+" else pc.negate_checked(operands[0])
        return {"+": pc.add_checked, "-": pc.subtract_checked, "*": pc.multiply_checked}[value](
            *operands
        )
    if kind == "function":
        operands = [ev(child) for child in args]
        category = _compatible(operands)
        if value == "coalesce":
            return pc.coalesce(*operands)
        operand = operands[0]
        if value == "abs" and category in {"null", "int", "float"}:
            return pc.abs_checked(operand)
        if value == "size" and category in {"null", "str"}:
            return pc.utf8_length(operand)
        if value in {"tolower", "toupper"} and category in {"null", "str"}:
            if pc.any(pc.match_substring_regex(operand, pattern=r"[^\x00-\x7f]")).as_py():
                raise UnsupportedColumnar
            return pc.utf8_lower(operand) if value == "tolower" else pc.utf8_upper(operand)
        raise UnsupportedColumnar
    if kind == "case":
        subject = ev(args[0]) if value else None
        choices = args[1:-1] if value else args[:-1]
        conditions, results = [], []
        for when, then in zip(choices[::2], choices[1::2], strict=True):
            condition = ev(when)
            if value:
                _compatible([subject, condition])
                condition = pc.equal(subject, condition)
            elif _category(condition.type) not in {"bool", "null"}:
                raise UnsupportedColumnar
            conditions.append(pc.fill_null(condition, False))
            results.append(ev(then))
        otherwise = ev(args[-1])
        _compatible([*results, otherwise])
        for condition, result in reversed(list(zip(conditions, results, strict=True))):
            otherwise = pc.if_else(condition, result, otherwise)
        return otherwise
    if kind == "string":
        operand, pattern = ev(args[0]), ev(args[1])
        if _compatible([operand, pattern]) not in {"str", "null"} or not isinstance(
            pattern, pa.Scalar
        ):
            raise UnsupportedColumnar
        if not pattern.is_valid:
            return pa.scalar(None, type=pa.bool_())
        return {"starts": pc.starts_with, "ends": pc.ends_with, "contains": pc.match_substring}[
            value
        ](operand, pattern=pattern.as_py())
    raise UnsupportedColumnar


def scan_predicates(expression: Expression | None, variable: str) -> tuple[ScanPredicate, ...]:
    """Offer exact conjuncts only; the full Arrow predicate remains authoritative."""
    if expression is None:
        return ()
    if expression.kind == "and":
        return tuple(p for child in expression.arguments for p in scan_predicates(child, variable))
    args = expression.arguments
    if expression.kind == "is_null":
        prop = args[0]
        if prop.kind == "property" and prop.arguments[0] == Expression("variable", variable):
            return (ScanPredicate(prop.value, "is_null" if expression.value else "is_not_null"),)
    if expression.kind == "compare" and len(expression.value) == 1:
        prop, literal = args
        operator = expression.value[0]
        if prop.kind == "literal":
            prop, literal = literal, prop
            operator = {"<": ">", "<=": ">=", ">": "<", ">=": "<=", "=": "=", "<>": "<>"}[operator]
        if (
            prop.kind == "property"
            and prop.arguments[0] == Expression("variable", variable)
            and literal.kind == "literal"
        ):
            return (ScanPredicate(prop.value, {"<>": "!="}.get(operator, operator), literal.value),)
    return ()


class ColumnarReadRuntime:
    def __init__(self, runtime: ReadRuntime) -> None:
        self.runtime = runtime
        self.counter = 0

    def execute(self, query: ReadQuery) -> QueryResult | None:
        graph = self.runtime.executor._graph_service
        if not isinstance(graph, GraphStateUseCase):
            return None
        try:
            plans = tuple(plan_branch(branch) for branch in query.branches)
            self.runtime._snapshot = graph.capture_snapshot()
            rows: list[dict[str, Any]] = []
            for plan in plans:
                batch, windowed, late_fields = self.scan(graph, plan)
                for index, stage in enumerate(plan.projections):
                    if index == 0 and windowed:
                        stage = replace(stage, skip=None, limit=None)
                    batch = self.project(
                        stage,
                        batch,
                        late_fields=late_fields if index == 0 else (),
                        label=plan.pattern.labels[0],
                    )
                if any(isinstance(binding, EntityColumns) for binding in batch.scope.values()):
                    raise UnsupportedColumnar
                names = tuple(batch.scope)
                result = (
                    batch.table.select([batch.scope[name] for name in names])
                    .rename_columns(names)
                    .to_pylist()
                )
                for stage in plan.residual:
                    result = self.runtime.project(stage, result, {})
                rows.extend({name: row[name] for name in query.columns or ()} for row in result)
            if len(plans) > 1 and not query.union_all:
                unique: dict[Any, dict[str, Any]] = {}
                for row in rows:
                    unique.setdefault(freeze(row), row)
                rows = list(unique.values())
            return QueryResult(query.columns or (), tuple(rows))
        except (UnsupportedColumnar, pa.ArrowException, OverflowError, TypeError, ValueError):
            return None

    def scan(self, graph: GraphStateUseCase, plan: ScanPlan) -> tuple[Batch, bool, tuple[str, ...]]:
        assert self.runtime._snapshot is not None
        snapshot = self.runtime._snapshot
        label, variable = plan.pattern.labels[0], plan.pattern.variable
        schema = graph.load_node_property_schema(snapshot=snapshot, label=label)
        if "node_id" not in schema.names or schema.field("node_id").metadata:
            raise UnsupportedColumnar
        for field in plan.fields:
            if field == "node_id":
                raise UnsupportedColumnar
            if field in schema.names:
                physical = schema.field(field)
                if (physical.metadata or {}).get(b"grapharrow.encoding") != b"native":
                    raise UnsupportedColumnar
                _category(physical.type)
        columns = ["node_id", *(name for name in plan.fields if name in schema.names)]
        predicate = plan.match.predicate
        for field, value in plan.pattern.properties.items():
            inline = Expression(
                "compare",
                ("=",),
                (
                    Expression("property", field, (Expression("variable", variable),)),
                    Expression("literal", value),
                ),
            )
            predicate = (
                inline if predicate is None else Expression("and", arguments=(inline, predicate))
            )

        def binding(table: pa.Table) -> Batch:
            for name in plan.fields:
                if name not in table.column_names:
                    table = table.append_column(name, pa.nulls(table.num_rows))
            return Batch(table, {variable: EntityColumns({name: name for name in plan.fields})})

        # Validate complete expressions/types before pushing any part of WHERE.
        empty = binding(
            pa.Table.from_arrays(
                [pa.array([], type=schema.field(c).type) for c in columns], names=columns
            )
        )
        empty_scan = empty
        if predicate is not None:
            evaluate(predicate, empty)
        for stage in plan.projections:
            empty = self.project(stage, empty)
        first = plan.projections[0]
        if (
            limit_optimizations_enabled()
            and first.limit is not None
            and not first.ordering
            and all(safe_to_defer(expression) for _, expression in first.items)
            and (predicate is None or safe_to_defer(predicate))
        ):
            skip = window_value(first.skip, 0, self.runtime.evaluator)
            limit = window_value(first.limit, 0, self.runtime.evaluator)
            if limit == 0:
                return empty_scan, True, ()
            selected = []
            consumed = 0
            # Apply the complete MATCH residual before counting rows. WITH's
            # predicate remains after this window, including across batches.
            batches = graph.iter_node_property_batches(
                snapshot=snapshot,
                label=label,
                columns=columns,
                predicates=scan_predicates(predicate, variable),
                batch_size=max(1024, min(skip + limit, 65536)),
            )
            try:
                for chunk in batches:
                    batch = binding(pa.Table.from_batches([chunk]))
                    if predicate is not None:
                        batch = self.filter(batch, evaluate(predicate, batch))
                    start = max(0, skip - consumed)
                    count = min(batch.table.num_rows - start, skip + limit - max(skip, consumed))
                    if count > 0:
                        selected.append(batch.table.slice(start, count))
                    consumed += batch.table.num_rows
                    if consumed >= skip + limit:
                        break
            finally:
                batches.close()
            table = pa.concat_tables(selected) if selected else empty_scan.table
            return Batch(table, empty_scan.scope), True, ()
        late_fields: tuple[str, ...] = ()
        if limit_optimizations_enabled() and first.ordering and first.limit is not None:
            bound = window_value(first.skip, 0, self.runtime.evaluator) + window_value(
                first.limit, 0, self.runtime.evaluator
            )
            if 0 < bound <= 1024:
                entities = {variable}
                needed = set().union(*(variables(expression) for expression, _ in first.ordering))
                early = set(plan.pattern.properties)
                if predicate is not None:
                    early.update(dependencies(predicate, entities))
                aliases = set()
                for name, expression in first.items:
                    if expression.kind == "variable" and expression.value in entities:
                        aliases.add(name)
                    elif name in needed or not safe_to_defer(expression):
                        early.update(dependencies(expression, entities))
                for expression, _ in first.ordering:
                    early.update(dependencies(expression, entities | aliases))
                late_fields = tuple(
                    field for field in plan.fields if field not in early and field in schema.names
                )
        table = graph.load_node_property_table(
            snapshot=snapshot,
            label=label,
            columns=[column for column in columns if column not in late_fields],
            predicates=scan_predicates(predicate, variable),
        )
        batch = binding(table)
        for name in plan.fields:
            if (
                pa.types.is_floating(batch.table[name].type)
                and pc.any(pc.is_nan(batch.table[name])).as_py()
            ):
                raise UnsupportedColumnar
        return (
            (self.filter(batch, evaluate(predicate, batch)) if predicate is not None else batch),
            False,
            late_fields,
        )

    def filter(self, batch: Batch, mask: Any) -> Batch:
        if _category(mask.type) not in {"bool", "null"}:
            raise UnsupportedColumnar
        if isinstance(mask, pa.Scalar):
            return batch if mask.as_py() is True else Batch(batch.table.slice(0, 0), batch.scope)
        return Batch(batch.table.filter(mask), batch.scope)

    def project(
        self,
        stage: ProjectionStage,
        batch: Batch,
        *,
        late_fields: tuple[str, ...] = (),
        label: str | None = None,
    ) -> Batch:
        table = batch.table
        output: dict[str, str | EntityColumns] = {}
        deferred = []
        enabled = limit_optimizations_enabled()
        needed = set().union(*(variables(expression) for expression, _ in stage.ordering))

        def append(name: str, result: Any) -> None:
            nonlocal table
            if isinstance(result, EntityColumns):
                output[name] = result
            else:
                self.counter += 1
                column = f"__projection_{self.counter}"
                table = table.append_column(
                    column,
                    pa.repeat(result, table.num_rows) if isinstance(result, pa.Scalar) else result,
                )
                output[name] = column

        for name, expression in stage.items:
            if (
                enabled
                and stage.limit is not None
                and name not in needed
                and safe_to_defer(expression)
            ):
                deferred.append((name, expression))
            else:
                append(name, evaluate(expression, batch))

        skip = window_value(stage.skip, 0, self.runtime.evaluator)
        limit = window_value(stage.limit, table.num_rows, self.runtime.evaluator)
        working = Batch(table, batch.scope | output)
        key_columns = {}
        keys = []
        for index, (expression, descending) in enumerate(stage.ordering):
            values = evaluate(expression, working)
            if isinstance(values, pa.Scalar):
                continue
            _category(values.type)
            column = f"order_{index}"
            key_columns[column] = values
            keys.append(
                (
                    column,
                    "descending" if descending else "ascending",
                    "at_start" if descending else "at_end",
                )
            )
        if keys:
            indices = sort_window_indices(
                pa.table(key_columns),
                keys,
                skip=skip,
                limit=limit if stage.limit is not None else None,
                top_k=enabled,
            )
            table = table.take(indices)
        else:
            table = table.slice(skip, limit)
        if late_fields and table.num_rows:
            graph = self.runtime.executor._graph_service
            assert isinstance(graph, GraphStateUseCase) and self.runtime._snapshot is not None
            assert label is not None
            batches = graph.iter_node_property_batches(
                snapshot=self.runtime._snapshot,
                label=label,
                columns=["node_id", *late_fields],
                predicates=(ScanPredicate("node_id", "in", tuple(table["node_id"].to_pylist())),),
                batch_size=1024,
            )
            parts = []
            count = 0
            try:
                for chunk in batches:
                    part = pa.Table.from_batches([chunk])
                    part = part.filter(pc.is_in(part["node_id"], value_set=table["node_id"]))
                    if part.num_rows:
                        parts.append(part)
                        count += part.num_rows
                    if count == table.num_rows:
                        break
            finally:
                batches.close()
            if count != table.num_rows:
                raise UnsupportedColumnar
            fetched = pa.concat_tables(parts)
            positions = pc.index_in(table["node_id"], value_set=fetched["node_id"])
            if positions.null_count:
                raise UnsupportedColumnar
            for field in late_fields:
                table = table.set_column(
                    table.schema.get_field_index(field), field, pc.take(fetched[field], positions)
                )
        # Projection expressions always bind against the incoming scope, not
        # aliases declared by another item in the same projection.
        incoming = Batch(table, batch.scope)
        for name, expression in deferred:
            append(name, evaluate(expression, incoming))
        output = {name: output[name] for name, _ in stage.items}
        working = Batch(table, batch.scope | output)
        if stage.predicate is not None:
            working = self.filter(working, evaluate(stage.predicate, working))
        keep: set[str] = set()
        for binding in output.values():
            keep.update(
                binding.properties.values() if isinstance(binding, EntityColumns) else [binding]
            )
        return Batch(working.table.select(sorted(keep)), output)
