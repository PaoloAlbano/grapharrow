# mypy: disable-error-code=no-untyped-call
"""Composable read stages, UNION and correlated read subqueries.

Established query shapes retain the existing Arrow executor. This path reuses
its pattern matcher and one captured graph state, with explicit row scopes at
projection and subquery boundaries. Compilation completes before execution.
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, replace
from functools import cmp_to_key
from heapq import nsmallest
from typing import Any

from grapharrow.core.errors import QueryError
from grapharrow.domain import SnapshotManifest
from grapharrow.repositories import EdgeRecord, NodeRecord
from grapharrow.use_cases import cypher as core
from grapharrow.use_cases.cypher_expression import (
    Expression,
    ExpressionCompiler,
    ExpressionEvaluator,
    Row,
    Scope,
    children,
    equal,
    freeze,
    has_aggregate,
    identifier,
    property_value,
    public_value,
    rule,
    sort_key,
    truth,
    validate_predicate,
)
from grapharrow.use_cases.cypher_window import limit_optimizations_enabled, window_value
from grapharrow.use_cases.graph import GraphState, GraphStateUseCase, QueryResult


@dataclass(frozen=True)
class MatchStage:
    patterns: tuple[core.CompiledPattern, ...]
    bindings: Scope
    predicate: Expression | None
    optional: bool


@dataclass(frozen=True)
class UnwindStage:
    expression: Expression
    variable: str


@dataclass(frozen=True)
class ProjectionStage:
    items: tuple[tuple[str, Expression], ...]
    globals: Scope
    distinct: bool
    predicate: Expression | None
    ordering: tuple[tuple[Expression, bool], ...]
    skip: Expression | None
    limit: Expression | None


@dataclass(frozen=True)
class CallStage:
    query: ReadQuery
    optional: bool


Stage = MatchStage | UnwindStage | ProjectionStage | CallStage


@dataclass(frozen=True)
class ReadBranch:
    initial_scope: Scope
    stages: tuple[Stage, ...]
    output_scope: Scope


@dataclass(frozen=True)
class ReadQuery:
    branches: tuple[ReadBranch, ...]
    union_all: bool
    columns: tuple[str, ...] | None


def walk(node: Any) -> Any:
    yield node
    for child in children(node):
        yield from walk(child)


def needs_pipeline(tree: Any, source: str) -> bool:
    if not any(
        word in source.lower() for word in ("case", "coalesce", "with", "union", "call", "exists")
    ):
        return False
    for node in walk(tree):
        name = rule(node)
        if name in {"oC_CaseExpression", "oC_SubqueryCall", "oC_Union"}:
            return True
        if name == "oC_ExistentialSubquery" and (
            node.oC_RegularQuery() is not None or node.oC_ExistentialBody() is not None
        ):
            return True
        if (
            name == "oC_FunctionInvocation"
            and node.oC_FunctionName().getText().lower() == "coalesce"
        ):
            return True
        if name == "oC_MultiPartQuery" and len(node.oC_With()) > 1:
            return True
        if name == "oC_With" and node.oC_ProjectionBody().oC_ProjectionItems().getText().startswith(
            "*"
        ):
            return True
    return False


def contains_updates(tree: Any) -> bool:
    return any(rule(node) == "oC_UpdatingClause" for node in walk(tree))


def expression_kind(expression: Expression, scope: Scope) -> str:
    if expression.kind == "variable":
        return scope[expression.value]
    if expression.kind == "property":
        base = expression.arguments[0]
        if expression_kind(base, scope) in {"node", "relationship"}:
            return "scalar"
        if base.kind == "map" and expression.value in base.value:
            return expression_kind(base.arguments[base.value.index(expression.value)], scope)
        return "any"
    if expression.kind == "function" and expression.value == "coalesce":
        kinds = {
            expression_kind(a, scope)
            for a in expression.arguments
            if not (a.kind == "literal" and a.value is None)
        }
        return next(iter(kinds)) if len(kinds) == 1 else "any"
    if expression.kind == "case":
        choices = expression.arguments[1:] if expression.value else expression.arguments
        values = (*choices[1:-1:2], choices[-1])
        kinds = {
            expression_kind(a, scope)
            for a in values
            if not (a.kind == "literal" and a.value is None)
        }
        return next(iter(kinds)) if len(kinds) == 1 else "any"
    return "scalar"


def pattern_bindings(patterns: tuple[core.CompiledPattern, ...]) -> Scope:
    scope = {}
    for pattern in patterns:
        scope[
            pattern.variable if isinstance(pattern, core.NodePattern) else pattern.start.variable
        ] = "node"
        if isinstance(pattern, core.PathPattern):
            for step in pattern.steps:
                scope[step.node.variable] = "node"
                if step.variable is not None:
                    scope[step.variable] = "relationship"
    return scope


def _grouping_valid(
    expression: Expression, grouping: tuple[Expression, ...], aliases: set[str]
) -> bool:
    if expression in grouping or expression.kind in {"aggregate", "literal", "exists"}:
        return True
    if expression.kind == "variable":
        return expression.value in aliases
    if expression.kind in {"comprehension", "quantifier"}:
        variable = expression.value if expression.kind == "comprehension" else expression.value[1]
        return _grouping_valid(expression.arguments[0], grouping, aliases) and all(
            _grouping_valid(child, grouping, aliases | {variable})
            for child in expression.arguments[1:]
        )
    return all(_grouping_valid(child, grouping, aliases) for child in expression.arguments)


class ReadCompiler:
    def __init__(self, parameters: Mapping[str, object]) -> None:
        self.parameters = dict(parameters)
        self.state = core._CompilerState()
        self.expressions = ExpressionCompiler(parameters, self._exists)

    def compile(self, tree: Any) -> ReadQuery:
        if contains_updates(tree):
            raise QueryError(
                "Composable pipelines and subqueries currently support read queries only"
            )
        regular = tree.oC_Statement().oC_Query().oC_RegularQuery()
        if regular is None:
            raise QueryError("Expected a read query")
        return self.regular(regular, {})

    def regular(
        self,
        node: Any,
        scope: Scope,
        *,
        globals: Scope | None = None,
        legacy_outer: Scope | None = None,
    ) -> ReadQuery:
        unions = node.oC_Union()
        modes = {u.ALL() is not None for u in unions}
        if len(modes) > 1:
            raise QueryError("Cannot mix UNION and UNION ALL in one query")
        singles = [node.oC_SingleQuery(), *(u.oC_SingleQuery() for u in unions)]
        branches = []
        columns = None
        for single in singles:
            multi = single.oC_MultiPartQuery()
            parts = children(multi) if multi is not None else [single.oC_SinglePartQuery()]
            ordered = []
            for part in parts:
                if rule(part) == "oC_SinglePartQuery":
                    ordered.extend(children(part))
                else:
                    ordered.append(part)
            visible = dict(scope)
            initial = dict(scope)
            stages: list[Stage] = []
            returned = None
            for part in ordered:
                kind = rule(part)
                if kind == "oC_With" and not stages and legacy_outer is not None:
                    imported = self._legacy_import(part, legacy_outer)
                    if imported is not None:
                        visible.update(imported)
                        initial.update(imported)
                if kind == "oC_ReadingClause":
                    stage, visible = self.reading(part, visible)
                elif kind in {"oC_With", "oC_Return"}:
                    stage, visible = self.projection(
                        part, visible, globals or {}, terminal=kind == "oC_Return"
                    )
                    if kind == "oC_Return":
                        returned = tuple(name for name, _ in stage.items)
                else:
                    raise QueryError("Updating clauses are not supported in read pipelines")
                stages.append(stage)
            if returned is None:
                raise QueryError("Read queries require a RETURN clause")
            if columns is not None and set(returned) != set(columns):
                raise QueryError("UNION branches must return the same column names")
            if columns is None:
                columns = returned
            branches.append(ReadBranch(initial, tuple(stages), visible))
        return ReadQuery(tuple(branches), bool(modes and True in modes), columns)

    def _legacy_import(self, node: Any, outer: Scope) -> Scope | None:
        body = node.oC_ProjectionBody()
        items = body.oC_ProjectionItems()
        if items.getText().startswith("*"):
            imported = dict(outer)
        else:
            texts = [p.oC_Expression().getText() for p in items.oC_ProjectionItem()]
            if not any(identifier(text) in outer for text in texts):
                return None
            if any(
                p.oC_Variable() is not None or identifier(p.oC_Expression().getText()) not in outer
                for p in items.oC_ProjectionItem()
            ):
                raise QueryError("An importing WITH accepts only unaliased outer variables")
            imported = {identifier(text): outer[identifier(text)] for text in texts}
        if (
            node.oC_Where() is not None
            or body.DISTINCT() is not None
            or body.oC_Order() is not None
            or body.oC_Skip() is not None
            or body.oC_Limit() is not None
        ):
            raise QueryError("An importing WITH cannot filter, aggregate, sort or limit")
        return imported

    def reading(self, node: Any, scope: Scope) -> tuple[Stage, Scope]:
        match = node.oC_Match()
        if match is not None:
            return self.match(
                match.oC_Pattern(), match.oC_Where(), match.OPTIONAL() is not None, scope
            )
        unwind = node.oC_Unwind()
        if unwind is not None:
            variable = identifier(unwind.oC_Variable().getText())
            if variable in scope:
                raise QueryError(f"Variable already declared: {variable}")
            expression = self.expressions.compile(unwind.oC_Expression(), scope)
            if has_aggregate(expression):
                raise QueryError("UNWIND cannot contain an aggregate")
            return UnwindStage(expression, variable), scope | {variable: "any"}
        call = node.oC_SubqueryCall()
        if call is None:
            raise QueryError(
                "Only read CALL subqueries are supported here, not arbitrary procedures"
            )
        imports = call.oC_SubqueryScope()
        if imports is None:
            query = self.regular(call.oC_RegularQuery(), {}, legacy_outer=scope)
        else:
            names = (
                list(scope)
                if imports.getText().replace(" ", "") == "(*)"
                else [identifier(p.getText()) for p in imports.oC_Variable()]
            )
            if len(set(names)) != len(names) or any(n not in scope for n in names):
                raise QueryError("CALL imports must be distinct variables from the outer scope")
            imported = {name: scope[name] for name in names}
            query = self.regular(call.oC_RegularQuery(), imported, globals=imported)
        exported = {}
        for column in query.columns or ():
            if column in scope:
                raise QueryError(
                    f"Subquery return variable already declared in outer scope: {column}"
                )
            kinds = {branch.output_scope[column] for branch in query.branches}
            exported[column] = next(iter(kinds)) if len(kinds) == 1 else "any"
        return CallStage(query, call.OPTIONAL() is not None), scope | exported

    def match(
        self, pattern: Any, where: Any, optional: bool, scope: Scope
    ) -> tuple[MatchStage, Scope]:
        patterns = core._compile_match_patterns(pattern, self.parameters, self.state)
        # Give property-bearing anonymous relationships an internal identity so
        # deferred property expressions can be checked after matching as well.
        patterns = tuple(
            replace(
                p,
                steps=tuple(
                    replace(s, variable=self.state.next_variable())
                    if s.variable is None and s.properties
                    else s
                    for s in p.steps
                ),
            )
            if isinstance(p, core.PathPattern)
            else p
            for p in patterns
        )
        bindings = pattern_bindings(patterns)
        for name, kind in bindings.items():
            if name in scope and scope[name] not in {kind, "any"}:
                raise QueryError(f"MATCH variable {name} must be a {kind}")
        visible = scope | {
            name: kind for name, kind in bindings.items() if not name.startswith("_anon_")
        }

        def check(value: Any) -> None:
            if isinstance(value, core.PropertyReferenceValue) and value.variable not in visible:
                raise QueryError(f"Unknown variable: {value.variable}")
            if isinstance(value, dict):
                for child in value.values():
                    check(child)
            if isinstance(value, list | tuple):
                for child in value:
                    check(child)

        for p in patterns:
            nodes = (
                [p] if isinstance(p, core.NodePattern) else [p.start, *(s.node for s in p.steps)]
            )
            for n in nodes:
                check(n.properties)
            if isinstance(p, core.PathPattern):
                for s in p.steps:
                    check(s.properties)
        predicate = (
            None if where is None else self.expressions.compile(where.oC_Expression(), visible)
        )
        if predicate is not None and has_aggregate(predicate):
            raise QueryError("MATCH WHERE cannot contain an aggregate")
        if predicate is not None:
            validate_predicate(predicate)
        return MatchStage(patterns, bindings, predicate, optional), visible

    def projection(
        self, node: Any, scope: Scope, globals: Scope, *, terminal: bool
    ) -> tuple[ProjectionStage, Scope]:
        body = node.oC_ProjectionBody()
        projection = body.oC_ProjectionItems()
        items: list[tuple[str, Expression]] = []
        if projection.getText().startswith("*"):
            if not scope:
                raise QueryError("A star projection requires visible bindings")
            items.extend((name, Expression("variable", name)) for name in sorted(scope))
        for item in projection.oC_ProjectionItem():
            expression = self.expressions.compile(item.oC_Expression(), scope)
            alias = item.oC_Variable()
            if alias is not None:
                name = identifier(alias.getText())
            elif expression.kind == "variable":
                name = expression.value
            elif terminal:
                name = item.oC_Expression().getText()
            else:
                raise QueryError("WITH expressions require an explicit alias")
            if name in globals and expression != Expression("variable", name):
                raise QueryError(f"Cannot shadow imported variable: {name}")
            items.append((name, expression))
        names = [name for name, _ in items]
        if len(names) != len(set(names)):
            raise QueryError("Projection defines the same output name more than once")
        grouping = tuple(expression for _, expression in items if not has_aggregate(expression))
        aggregated = any(has_aggregate(expression) for _, expression in items)
        if aggregated:
            for _, expression in items:
                if not _grouping_valid(expression, grouping, set()):
                    raise QueryError(
                        "Aggregation references an expression outside the grouping keys"
                    )
        projected = {name: expression_kind(expression, scope) for name, expression in items}
        output = projected if terminal else globals | projected
        predicate = None
        where = node.oC_Where() if not terminal else None
        if where is not None:
            predicate_scope = (
                output if aggregated or body.DISTINCT() is not None else scope | output
            )
            predicate = self.expressions.compile(where.oC_Expression(), predicate_scope)
            if has_aggregate(predicate):
                raise QueryError("WITH WHERE must reference projected aggregates by alias")
            validate_predicate(predicate)
        ordering = []
        order = body.oC_Order()
        if order is not None:
            for part in order.oC_SortItem():
                expression = self.expressions.compile(part.oC_Expression(), scope | output)
                if (aggregated or body.DISTINCT() is not None) and not _grouping_valid(
                    expression, tuple(e for _, e in items), set(projected)
                ):
                    raise QueryError(
                        "ORDER BY references a non-projected value after aggregation or DISTINCT"
                    )
                ordering.append(
                    (expression, part.DESC() is not None or part.DESCENDING() is not None)
                )

        def window(part: Any) -> Expression | None:
            if part is None:
                return None
            expression = self.expressions.compile(part.oC_Expression(), {})
            if has_aggregate(expression):
                raise QueryError("SKIP and LIMIT cannot contain aggregates")
            return expression

        return ProjectionStage(
            tuple(items),
            {} if terminal else globals,
            body.DISTINCT() is not None,
            predicate,
            tuple(ordering),
            window(body.oC_Skip()),
            window(body.oC_Limit()),
        ), output

    def _exists(self, node: Any, scope: Scope) -> ReadQuery:
        regular = node.oC_RegularQuery() if rule(node) == "oC_ExistentialSubquery" else None
        if regular is not None:
            if contains_updates(regular):
                raise QueryError("EXISTS subqueries cannot write")
            return self.regular(regular, scope, globals=scope)
        body = node.oC_ExistentialBody() if rule(node) == "oC_ExistentialSubquery" else None
        stages: list[Stage] = []
        visible = dict(scope)
        if body is not None:
            for part in children(body):
                if rule(part) == "oC_ReadingClause":
                    stage, visible = self.reading(part, visible)
                else:
                    stage, visible = self.projection(part, visible, scope, terminal=False)
                stages.append(stage)
        elif rule(node) == "oC_ExistentialSubquery":
            stage, visible = self.match(node.oC_Pattern(), node.oC_Where(), False, scope)
            stages.append(stage)
        else:
            from grapharrow.adapters.opencypher import parse_cypher

            fragment = parse_cypher("MATCH " + node.getText() + " RETURN 1")
            match = (
                fragment.oC_Statement()
                .oC_Query()
                .oC_RegularQuery()
                .oC_SingleQuery()
                .oC_SinglePartQuery()
                .oC_ReadingClause(0)
                .oC_Match()
            )
            stage, visible = self.match(match.oC_Pattern(), None, False, scope)
            stages.append(stage)
        return ReadQuery((ReadBranch(dict(scope), tuple(stages), visible),), False, None)


def _deferred(value: Any) -> bool:
    return (
        isinstance(value, core.PropertyReferenceValue)
        or isinstance(value, dict)
        and any(_deferred(v) for v in value.values())
        or isinstance(value, list | tuple)
        and any(_deferred(v) for v in value)
    )


def _resolve(value: Any, row: Row) -> Any:
    if isinstance(value, core.PropertyReferenceValue):
        return property_value(row[value.variable], value.field)
    if isinstance(value, list | tuple):
        return [_resolve(v, row) for v in value]
    if isinstance(value, dict):
        return {k: _resolve(v, row) for k, v in value.items()}
    return value


def _static_pattern(pattern: core.CompiledPattern) -> core.CompiledPattern:
    def node(p: core.NodePattern) -> core.NodePattern:
        return replace(p, properties={k: v for k, v in p.properties.items() if not _deferred(v)})

    if isinstance(pattern, core.NodePattern):
        return node(pattern)
    return replace(
        pattern,
        start=node(pattern.start),
        steps=tuple(
            replace(
                s,
                node=node(s.node),
                properties={k: v for k, v in s.properties.items() if not _deferred(v)},
            )
            for s in pattern.steps
        ),
    )


class ReadRuntime:
    def __init__(self, executor: core.CypherQueryUseCase) -> None:
        self.executor = executor
        self._state: GraphState | None = None
        self._snapshot: SnapshotManifest | None = None
        self._nodes: dict[int, NodeRecord] = {}
        self._edges: dict[int, EdgeRecord] = {}
        self._match_indexes: dict[tuple[int, tuple[str, ...]], dict[Any, list[Row]]] = {}
        self.evaluator = ExpressionEvaluator(lambda query, row: bool(self.rows(query, [row])))

    @property
    def state(self) -> GraphState:
        if self._state is None:
            graph = self.executor._graph_service
            if self._snapshot is not None and isinstance(graph, GraphStateUseCase):
                self._state = graph.load_state(snapshot=self._snapshot)
            else:
                self._state = graph.load_state()
            self._nodes = {node.node_id: node for node in self._state.nodes}
            self._edges = {edge.edge_id: edge for edge in self._state.edges}
        return self._state

    def execute(self, query: ReadQuery) -> QueryResult:
        if os.environ.get("GRAPHARROW_DISABLE_CYPHER_COLUMNAR", "").lower() not in {"1", "true"}:
            from grapharrow.use_cases.cypher_columnar import ColumnarReadRuntime

            result = ColumnarReadRuntime(self).execute(query)
            if result is not None:
                return result
        rows = self.rows(query, [{}])
        columns = query.columns or ()
        return QueryResult(
            columns, tuple({name: public_value(row[name]) for name in columns} for row in rows)
        )

    def rows(self, query: ReadQuery, input_rows: list[Row]) -> list[Row]:
        result = []
        for branch in query.branches:
            rows = [{name: row[name] for name in branch.initial_scope} for row in input_rows]
            for stage in branch.stages:
                if isinstance(stage, MatchStage):
                    rows = self.match(stage, rows)
                elif isinstance(stage, UnwindStage):
                    unwound: list[Row] = []
                    for row in rows:
                        value = self.evaluator.evaluate(stage.expression, row)
                        if value is None:
                            continue
                        values = value if isinstance(value, list | tuple) else [value]
                        unwound.extend(row | {stage.variable: p} for p in values)
                    rows = unwound
                elif isinstance(stage, ProjectionStage):
                    rows = self.project(stage, rows, input_rows[0] if input_rows else {})
                else:
                    called: list[Row] = []
                    for row in rows:
                        nested = self.rows(stage.query, [row])
                        called.extend(
                            row | {name: entry[name] for name in stage.query.columns or ()}
                            for entry in nested
                        )
                        if not nested and stage.optional:
                            called.append(row | dict.fromkeys(stage.query.columns or ()))
                    rows = called
            if query.columns is not None:
                rows = [{name: row[name] for name in query.columns} for row in rows]
            result.extend(rows)
        if len(query.branches) > 1 and not query.union_all:
            unique: dict[Any, Row] = {}
            for row in result:
                unique.setdefault(freeze(row), row)
            result = list(unique.values())
        return result

    def match(self, stage: MatchStage, rows: list[Row]) -> list[Row]:
        if not rows:
            return []
        for row in rows:
            for variable, kind in stage.bindings.items():
                if variable not in row or row[variable] is None:
                    continue
                expected_type = NodeRecord if kind == "node" else EdgeRecord
                if not isinstance(row[variable], expected_type):
                    raise QueryError(f"MATCH variable {variable} must be a {kind}")
        shared = tuple(name for name in stage.bindings if name in rows[0])
        cache_key = (id(stage), shared)
        index = self._match_indexes.get(cache_key)
        if index is None:
            # Static candidates depend only on this stage and captured state.
            # Reuse their join index across correlated invocations; predicates
            # and deferred properties must still be evaluated for each row.
            state = self.state
            patterns = tuple(_static_pattern(p) for p in stage.patterns)
            required_fields: dict[str, set[str]] = {name: set() for name in stage.bindings}
            for pattern in patterns:
                nodes = (
                    [pattern]
                    if isinstance(pattern, core.NodePattern)
                    else [pattern.start, *(step.node for step in pattern.steps)]
                )
                for node in nodes:
                    required_fields[node.variable].update(node.properties)
            table = core._match_patterns_table(
                state.node_table,
                state.edge_table,
                patterns,
                self.executor._tabular,
                required_fields=required_fields,
                adjacency_by_source_loader=self.executor._adjacency_loader_for_state(state),
            )
            index = {}
            for entry in table.to_pylist():
                binding = {}
                for variable, kind in stage.bindings.items():
                    identity = entry.get(
                        f"{variable}.__id" if kind == "node" else f"{variable}.__entity_id"
                    )
                    if identity is None and kind == "relationship":
                        identity = entry.get(f"{variable}.id")
                    binding[variable] = (self._nodes if kind == "node" else self._edges).get(
                        identity
                    )
                key = tuple(freeze(binding[name]) for name in shared)
                index.setdefault(key, []).append(binding)
            self._match_indexes[cache_key] = index
        output = []
        for row in rows:
            found = False
            for binding in index.get(tuple(freeze(row[name]) for name in shared), []):
                joined = row | binding
                if any(row[name] is None for name in shared):
                    continue
                if not self._properties(stage.patterns, joined):
                    continue
                if (
                    stage.predicate is not None
                    and truth(self.evaluator.evaluate(stage.predicate, joined)) is not True
                ):
                    continue
                output.append(joined)
                found = True
            if stage.optional and not found:
                output.append(dict.fromkeys(stage.bindings) | row)
        return output

    def _properties(self, patterns: tuple[core.CompiledPattern, ...], row: Row) -> bool:
        checks: list[tuple[str, dict[str, object]]] = []
        for pattern in patterns:
            nodes = (
                [pattern]
                if isinstance(pattern, core.NodePattern)
                else [pattern.start, *(s.node for s in pattern.steps)]
            )
            checks.extend((n.variable, n.properties) for n in nodes)
            if isinstance(pattern, core.PathPattern):
                checks.extend(
                    (s.variable, s.properties) for s in pattern.steps if s.variable is not None
                )
        return all(
            equal(property_value(row[name], key), _resolve(value, row)) is True
            for name, props in checks
            for key, value in props.items()
        )

    def project(self, stage: ProjectionStage, rows: list[Row], global_seed: Row) -> list[Row]:
        grouping = tuple(
            expression for _, expression in stage.items if not has_aggregate(expression)
        )
        aggregated = any(has_aggregate(expression) for _, expression in stage.items)
        batches: Iterable[tuple[Row, list[Row] | None]]
        upper_bound = len(rows)
        if aggregated:
            groups: dict[Any, list[Row]] = {}
            for row in rows:
                key = tuple(freeze(self.evaluator.evaluate(e, row)) for e in grouping)
                groups.setdefault(key, []).append(row)
            if not groups and not grouping:
                groups[()] = []
            batches = ((group[0] if group else global_seed, group) for group in groups.values())
            upper_bound = len(groups)
        else:
            batches = ((row, None) for row in rows)

        def candidates() -> Iterator[tuple[Row, Row, list[Row] | None]]:
            seen = set()
            for original, group in batches:
                row = {
                    name: self.evaluator.evaluate(expression, original, group)
                    for name, expression in stage.items
                }
                row = {name: original[name] for name in stage.globals} | row
                if stage.distinct:
                    identity = freeze(row)
                    if identity in seen:
                        continue
                    seen.add(identity)
                yield row, original | row, group

        skip = window_value(stage.skip, 0, self.evaluator)
        limit = window_value(stage.limit, upper_bound, self.evaluator)
        bound = skip + limit
        enabled = limit_optimizations_enabled()
        if enabled and not stage.ordering and stage.limit is not None:
            # Consume every candidate to preserve expression errors, but retain
            # only the requested window instead of a full projected row list.
            projected = [item for index, item in enumerate(candidates()) if skip <= index < bound]
        elif enabled and stage.limit is not None and (limit == 0 or skip >= upper_bound):
            for item in candidates():
                for expression, _ in stage.ordering:
                    sort_key(self.evaluator.evaluate(expression, item[1], item[2]))
            projected = []
        elif enabled and stage.limit is not None and 0 < bound <= upper_bound // 4:

            def ranked() -> Iterator[Any]:
                for index, item in enumerate(candidates()):
                    keys = tuple(
                        sort_key(self.evaluator.evaluate(expression, item[1], item[2]))
                        for expression, _ in stage.ordering
                    )
                    yield keys, index, item

            def compare(left: Any, right: Any) -> int:
                for index, (_, descending) in enumerate(stage.ordering):
                    if left[0][index] != right[0][index]:
                        order = (left[0][index] > right[0][index]) - (
                            left[0][index] < right[0][index]
                        )
                        return int(-order if descending else order)
                return int(left[1] - right[1])

            projected = [item[2] for item in nsmallest(bound, ranked(), key=cmp_to_key(compare))][
                skip:
            ]
        else:
            projected = list(candidates())
            for expression, descending in reversed(stage.ordering):
                projected.sort(
                    key=lambda item: sort_key(
                        self.evaluator.evaluate(expression, item[1], item[2])
                    ),
                    reverse=descending,
                )
            projected = projected[skip:bound]
        return [
            row
            for row, environment, _ in projected
            if stage.predicate is None
            or truth(self.evaluator.evaluate(stage.predicate, environment)) is True
        ]
