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

from grapharrow.core import property_codec
from grapharrow.core.errors import QueryError
from grapharrow.domain import SnapshotManifest
from grapharrow.repositories import EdgeRecord, NodeRecord
from grapharrow.use_cases import cypher as core
from grapharrow.use_cases.cypher_expression import (
    TYPED_SCALAR_FUNCTIONS,
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
from grapharrow.use_cases.cypher_path import PathValue
from grapharrow.use_cases.cypher_window import limit_optimizations_enabled, window_value
from grapharrow.use_cases.graph import (
    GraphState,
    GraphStateUseCase,
    GraphTransactionUseCase,
    QueryResult,
)


@dataclass(frozen=True)
class PathBinding:
    name: str
    nodes: tuple[str, ...]
    relationships: tuple[str, ...]

    def value(self, row: Row, nodes_by_id: Mapping[int, NodeRecord] | None = None) -> PathValue:
        if nodes_by_id is None:
            return PathValue(
                tuple(row[name] for name in self.nodes),
                tuple(row[name] for name in self.relationships),
            )
        # Edge records retain their canonical orientation. The start binding
        # and ordered edges determine the traversal, including zero-hop segments.
        nodes = [row[self.nodes[0]]]
        relationships = []
        for name in self.relationships:
            value = row[name]
            for edge in value if isinstance(value, list) else (value,):
                current = nodes[-1].node_id
                other = edge.dst_id if edge.src_id == current else edge.src_id
                nodes.append(nodes_by_id[other])
                relationships.append(edge)
        return PathValue(tuple(nodes), tuple(relationships))


@dataclass(frozen=True)
class MatchStage:
    patterns: tuple[core.CompiledPattern, ...]
    bindings: Scope
    predicate: Expression | None
    optional: bool
    paths: tuple[PathBinding, ...] = ()


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


def needs_pipeline(tree: Any, source: str, parameters: Mapping[str, object] | None = None) -> bool:
    if parameters and property_codec.contains_temporal(parameters):
        return True
    if not any(
        word in source.lower()
        for word in (
            "case",
            "coalesce",
            *TYPED_SCALAR_FUNCTIONS,
            "with",
            "union",
            "call",
            "exists",
            "^",
            "unwind",
            "*",
            "distinct",
            "{",
            "=",
        )
    ):
        return False
    nodes = tuple(walk(tree))
    updating = any(rule(node) == "oC_UpdatingClause" for node in nodes)
    for node in nodes:
        name = rule(node)
        if name == "oC_PatternPart" and node.oC_Variable() is not None:
            return True
        if name == "oC_ProjectionItem" and any(
            rule(part) == "oC_MapLiteral"
            and any(rule(value) == "oC_Variable" for value in walk(part))
            for part in walk(node)
        ):
            return True
        if not updating:
            if (
                name == "oC_Return"
                and node.oC_ProjectionBody().DISTINCT() is not None
                and node.oC_ProjectionBody().oC_Order() is not None
            ):
                return True
            if name == "oC_With":
                return True
            if name == "oC_PowerOfExpression" and len(children(node)) > 1:
                return True
            if (
                name == "oC_RelationshipDetail"
                and node.oC_Variable() is not None
                and node.oC_RangeLiteral() is not None
            ):
                return True
            if name == "oC_Unwind":
                try:
                    value = core._compile_value_expression(
                        node.oC_Expression(), dict(parameters or {})
                    )
                except QueryError:
                    continue
                if isinstance(value, list | tuple):
                    kinds = {type(v) for v in value if v is not None}
                    if len(kinds) > 1 or any(isinstance(v, list | tuple | dict) for v in value):
                        return True
        if name in {"oC_CaseExpression", "oC_SubqueryCall", "oC_Union"}:
            return True
        if name == "oC_ExistentialSubquery" and (
            node.oC_RegularQuery() is not None or node.oC_ExistentialBody() is not None
        ):
            return True
        if (
            name == "oC_FunctionInvocation"
            and node.oC_FunctionName().getText().lower() in TYPED_SCALAR_FUNCTIONS | {"coalesce"}
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


def ordered_clauses(single: Any) -> list[Any]:
    multi = single.oC_MultiPartQuery()
    parts = children(multi) if multi is not None else [single.oC_SinglePartQuery()]
    return [
        clause
        for part in parts
        for clause in (children(part) if rule(part) == "oC_SinglePartQuery" else [part])
    ]


def expression_kind(expression: Expression, scope: Scope) -> str:
    if expression.kind == "variable":
        return scope[expression.value]
    if expression.kind == "literal" and expression.value is None:
        return "null"
    if expression.kind == "property":
        base = expression.arguments[0]
        if expression_kind(base, scope) in {"node", "relationship"}:
            return "scalar"
        if base.kind == "map" and expression.value in base.value:
            return expression_kind(base.arguments[base.value.index(expression.value)], scope)
        return "any"
    if expression.kind == "function" and expression.value in {"head", "last"}:
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
    scope: Scope = {}

    def bind(variable: str, kind: str) -> None:
        if variable in scope and scope[variable] != kind:
            raise QueryError.syntax(
                f"Variable {variable} cannot be both {scope[variable]} and {kind}",
                "VariableTypeConflict",
            )
        scope[variable] = kind

    for pattern in patterns:
        bind(
            pattern.variable if isinstance(pattern, core.NodePattern) else pattern.start.variable,
            "node",
        )
        if isinstance(pattern, core.PathPattern):
            for step in pattern.steps:
                if step.variable is not None:
                    if step.variable in scope and scope[step.variable] in {
                        "relationship",
                        "relationship_list",
                    }:
                        raise QueryError.syntax(
                            "A relationship variable cannot be repeated in one MATCH pattern.",
                            "RelationshipUniquenessViolation",
                        )
                    bind(
                        step.variable,
                        "relationship_list" if step.variable_length else "relationship",
                    )
                bind(step.node.variable, "node")
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


def _expression_nodes(expression: Expression) -> Iterator[Expression]:
    yield expression
    for child in expression.arguments:
        yield from _expression_nodes(child)


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
        self.reserve_variables(regular)
        return self.regular(regular, {})

    def reserve_variables(self, node: Any) -> None:
        self.state.reserved_variables = frozenset(
            identifier(variable.getText())
            for variable in walk(node)
            if rule(variable) == "oC_Variable"
        )

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
            raise QueryError.syntax(
                "Cannot mix UNION and UNION ALL in one query", "InvalidClauseComposition"
            )
        singles = [node.oC_SingleQuery(), *(u.oC_SingleQuery() for u in unions)]
        branches = []
        columns = None
        for single in singles:
            visible = dict(scope)
            initial = dict(scope)
            stages: list[Stage] = []
            returned = None
            for part in ordered_clauses(single):
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
                    raise QueryError.unsupported(
                        "Updating clauses are not supported in read subqueries"
                    )
                stages.append(stage)
            if returned is None:
                raise QueryError("Read queries require a RETURN clause")
            if columns is not None and set(returned) != set(columns):
                raise QueryError.syntax(
                    "UNION branches must return the same column names", "DifferentColumnsInUnion"
                )
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
                raise QueryError.syntax(
                    f"Variable already declared: {variable}", "VariableAlreadyBound"
                )
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
        def compile_properties(node: Any) -> dict[str, object]:
            if node is None or node.oC_MapLiteral() is None:
                return core._compile_properties(node, self.parameters)
            literal = node.oC_MapLiteral()
            return {
                identifier(key.getText()): (
                    _PatternExpression(expression)
                    if any(
                        rule(part) == "oC_FunctionInvocation"
                        and identifier(part.oC_FunctionName().getText()).lower()
                        in TYPED_SCALAR_FUNCTIONS
                        for part in walk(expression)
                    )
                    else core._compile_value_expression(expression, self.parameters)
                )
                for key, expression in zip(
                    literal.oC_PropertyKeyName(), literal.oC_Expression(), strict=True
                )
            }

        patterns_list: list[core.CompiledPattern] = []
        paths = []
        for part in pattern.oC_PatternPart():
            alias = part.oC_Variable()
            compiled = core._compile_pattern_element(
                part.oC_AnonymousPatternPart().oC_PatternElement(),
                self.parameters,
                self.state,
                allow_relationship_variables=True,
                allow_ranged_bindings=True,
                compile_properties=compile_properties,
            )

            if isinstance(compiled, core.NodePattern):
                compiled = replace(compiled, variable=identifier(compiled.variable))
            else:
                capture = any(
                    step.variable_length
                    and ((step.min_hops, step.max_hops) != (1, 1))
                    and (
                        alias is not None
                        or step.variable is not None
                        or step.properties
                        or step.direction != "outgoing"
                        or step.min_hops == 0
                    )
                    for step in compiled.steps
                )
                compiled = replace(
                    compiled,
                    start=replace(compiled.start, variable=identifier(compiled.start.variable)),
                    capture_relationships=capture,
                    steps=tuple(
                        replace(
                            step,
                            node=replace(step.node, variable=identifier(step.node.variable)),
                            variable=(
                                identifier(step.variable)
                                if step.variable is not None
                                else self.state.next_variable()
                                if alias is not None or step.properties or capture
                                else None
                            ),
                        )
                        for step in compiled.steps
                    ),
                )
            if alias is not None:
                name = identifier(alias.getText())
                if isinstance(compiled, core.NodePattern):
                    paths.append(PathBinding(name, (compiled.variable,), ()))
                else:
                    paths.append(
                        PathBinding(
                            name,
                            (
                                compiled.start.variable,
                                *(step.node.variable for step in compiled.steps),
                            ),
                            tuple(
                                step.variable
                                for step in compiled.steps
                                if step.variable is not None
                            ),
                        )
                    )
            patterns_list.append(compiled)
        patterns = tuple(patterns_list)
        bindings = pattern_bindings(patterns)
        for name, kind in bindings.items():
            if name in scope and scope[name] not in {kind, "any", "null"}:
                if scope[name] == "scalar":
                    raise QueryError(
                        f"MATCH after WITH cannot reuse scalar binding '{name}' as a graph entity."
                    )
                raise QueryError(f"MATCH variable {name} must be a {kind}")
        visible = scope | {
            name: kind
            for name, kind in bindings.items()
            if not name.startswith("_anon_") or name in self.state.reserved_variables
        }

        for path in paths:
            if path.name in visible:
                detail = (
                    "VariableTypeConflict"
                    if path.name not in scope
                    and path.name in bindings
                    and path.name not in (*path.nodes, *path.relationships)
                    else "VariableAlreadyBound"
                )
                raise QueryError.syntax(f"Cannot bind path variable {path.name}.", detail)
            visible[path.name] = "path"

        def check(value: Any) -> None:
            if (
                isinstance(value, core.PropertyReferenceValue)
                and identifier(value.variable) not in visible
            ):
                raise QueryError.syntax(f"Unknown variable: {value.variable}", "UndefinedVariable")
            if isinstance(value, dict):
                for child in value.values():
                    check(child)
            if isinstance(value, list | tuple):
                for child in value:
                    check(child)

        def prepare(properties: dict[str, object]) -> None:
            for key, value in properties.items():
                if isinstance(value, _PatternExpression):
                    expression = self.expressions.compile(value.node, visible)
                    if has_aggregate(expression):
                        raise QueryError.syntax(
                            "Pattern properties cannot contain an aggregate.", "InvalidAggregation"
                        )
                    properties[key] = expression
                else:
                    check(value)

        for p in patterns:
            nodes = (
                [p] if isinstance(p, core.NodePattern) else [p.start, *(s.node for s in p.steps)]
            )
            for n in nodes:
                prepare(n.properties)
            if isinstance(p, core.PathPattern):
                for s in p.steps:
                    prepare(s.properties)
        predicate = (
            None if where is None else self.expressions.compile(where.oC_Expression(), visible)
        )
        if predicate is not None and has_aggregate(predicate):
            raise QueryError("MATCH WHERE cannot contain an aggregate")
        if predicate is not None:
            validate_predicate(predicate)
        return MatchStage(patterns, bindings, predicate, optional, tuple(paths)), visible

    def projection(
        self, node: Any, scope: Scope, globals: Scope, *, terminal: bool
    ) -> tuple[ProjectionStage, Scope]:
        body = node.oC_ProjectionBody()
        projection = body.oC_ProjectionItems()
        items: list[tuple[str, Expression]] = []
        if projection.getText().startswith("*"):
            if not scope:
                raise QueryError.syntax(
                    "A star projection requires visible bindings", "NoVariablesInScope"
                )
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
                raise QueryError.syntax(
                    "WITH expressions require an explicit alias", "NoExpressionAlias"
                )
            if name in globals and expression != Expression("variable", name):
                raise QueryError(f"Cannot shadow imported variable: {name}")
            items.append((name, expression))
        names = [name for name, _ in items]
        if len(names) != len(set(names)):
            raise QueryError.syntax(
                "Projection defines the same output name more than once", "ColumnNameConflict"
            )
        grouping = tuple(expression for _, expression in items if not has_aggregate(expression))
        aggregated = any(has_aggregate(expression) for _, expression in items)
        if aggregated:
            for _, expression in items:
                if has_aggregate(expression) and not _grouping_valid(
                    expression,
                    tuple(g for g in grouping if g.kind in {"variable", "property", "literal"}),
                    set(),
                ):
                    raise QueryError(
                        "Aggregation references an expression outside the grouping keys"
                    )
        projected = {name: expression_kind(expression, scope) for name, expression in items}
        output = projected if terminal else globals | projected
        predicate = None
        where = node.oC_Where() if not terminal else None
        if where is not None:
            predicate = self.expressions.compile(where.oC_Expression(), scope | output)
            if (aggregated or body.DISTINCT() is not None) and not _grouping_valid(
                predicate, tuple(e for _, e in items), set(output)
            ):
                raise QueryError(
                    "WITH WHERE references a non-projected value after aggregation or DISTINCT"
                )
            if has_aggregate(predicate):
                raise QueryError("WITH WHERE must reference projected aggregates by alias")
            validate_predicate(predicate)
        ordering = []
        order = body.oC_Order()
        if order is not None:
            for part in order.oC_SortItem():
                expression = self.expressions.compile(part.oC_Expression(), scope | output)
                projected_aggregates = [
                    e for _, item in items for e in _expression_nodes(item) if e.kind == "aggregate"
                ]
                if any(
                    e.kind == "aggregate" and e not in projected_aggregates
                    for e in _expression_nodes(expression)
                ):
                    raise QueryError("ORDER BY cannot introduce a non-projected aggregate")
                if (aggregated or body.DISTINCT() is not None) and not _grouping_valid(
                    expression,
                    tuple(
                        e
                        for _, e in items
                        if e.kind in {"variable", "property", "literal", "aggregate"}
                    ),
                    set(projected),
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
                raise QueryError.unsupported("EXISTS subqueries cannot write")
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
            if any(name not in scope and not name.startswith("_anon_") for name in stage.bindings):
                raise QueryError("Pattern predicates cannot introduce named variables")
            stages.append(stage)
        return ReadQuery((ReadBranch(dict(scope), tuple(stages), visible),), False, None)


@dataclass(frozen=True, slots=True)
class _PatternExpression:
    node: Any


def _deferred(value: Any) -> bool:
    return (
        isinstance(value, core.PropertyReferenceValue | Expression)
        or isinstance(value, dict)
        and any(_deferred(v) for v in value.values())
        or isinstance(value, list | tuple)
        and any(_deferred(v) for v in value)
    )


def _resolve(value: Any, row: Row, evaluator: ExpressionEvaluator) -> Any:
    if isinstance(value, Expression):
        return evaluator.evaluate(value, row)
    if isinstance(value, core.PropertyReferenceValue):
        return property_value(row[identifier(value.variable)], value.field)
    if isinstance(value, list | tuple):
        return [_resolve(v, row, evaluator) for v in value]
    if isinstance(value, dict):
        return {k: _resolve(v, row, evaluator) for k, v in value.items()}
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
        graph = executor._graph_service
        self.evaluator = ExpressionEvaluator(
            lambda query, row: bool(self.rows(query, [row])),
            transaction_time=graph.transaction_time_ns
            if isinstance(graph, GraphTransactionUseCase)
            else None,
        )

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
                rows = self.apply_stage(stage, rows, input_rows[0] if input_rows else {})
            if query.columns is not None:
                rows = [{name: row[name] for name in query.columns} for row in rows]
            result.extend(rows)
        if len(query.branches) > 1 and not query.union_all:
            unique: dict[Any, Row] = {}
            for row in result:
                unique.setdefault(freeze(row), row)
            result = list(unique.values())
        return result

    def apply_stage(self, stage: Stage, rows: list[Row], seed: Row) -> list[Row]:
        if isinstance(stage, MatchStage):
            return self.match(stage, rows)
        if isinstance(stage, ProjectionStage):
            return self.project(stage, rows, seed)
        if isinstance(stage, UnwindStage):
            unwound: list[Row] = []
            for row in rows:
                value = self.evaluator.evaluate(stage.expression, row)
                if value is None:
                    continue
                values = value if isinstance(value, list | tuple) else [value]
                unwound.extend(row | {stage.variable: p} for p in values)
            return unwound
        called: list[Row] = []
        for row in rows:
            nested = self.rows(stage.query, [row])
            called.extend(
                row | {name: entry[name] for name in stage.query.columns or ()} for entry in nested
            )
            if not nested and stage.optional:
                called.append(row | dict.fromkeys(stage.query.columns or ()))
        return called

    def match(self, stage: MatchStage, rows: list[Row]) -> list[Row]:
        if not rows:
            return []
        for row in rows:
            for variable, kind in stage.bindings.items():
                if variable not in row or row[variable] is None:
                    continue
                expected_type = (
                    NodeRecord
                    if kind == "node"
                    else list
                    if kind == "relationship_list"
                    else EdgeRecord
                )
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
            recorded_steps = {
                step.variable: core._path_edge_id_columns(step)
                for pattern in patterns
                if isinstance(pattern, core.PathPattern) and pattern.capture_relationships
                for step in pattern.steps
            }
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
                binding: Row = {}
                for variable, kind in stage.bindings.items():
                    if variable in recorded_steps:
                        edges = [
                            self._edges[entry[column]]
                            for column in recorded_steps[variable]
                            if entry[column] is not None
                        ]
                        binding[variable] = edges if kind == "relationship_list" else edges[0]
                        continue
                    identity = entry.get(
                        f"{variable}.__id" if kind == "node" else f"{variable}.__entity_id"
                    )
                    if identity is None and kind in {"relationship", "relationship_list"}:
                        identity = entry.get(f"{variable}.id")
                    binding[variable] = (self._nodes if kind == "node" else self._edges).get(
                        identity
                    )
                    if kind == "relationship_list" and binding[variable] is not None:
                        binding[variable] = [binding[variable]]
                for path in stage.paths:
                    binding[path.name] = path.value(binding, self._nodes)
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
                output.append(
                    dict.fromkeys((*stage.bindings, *(path.name for path in stage.paths))) | row
                )
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
            equal(property_value(entity, key), _resolve(value, row, self.evaluator)) is True
            for name, props in checks
            for key, value in props.items()
            for entity in (row[name] if isinstance(row[name], list) else [row[name]])
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
