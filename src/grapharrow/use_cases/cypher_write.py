# mypy: disable-error-code=no-untyped-call
"""Compile updating clauses into stages over a transaction-local graph."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, replace
from typing import Any, Literal

from grapharrow.core.errors import QueryError
from grapharrow.repositories import EdgeRecord, NodeRecord
from grapharrow.use_cases import cypher as core
from grapharrow.use_cases.cypher_expression import (
    Expression,
    ExpressionEvaluator,
    Row,
    Scope,
    children,
    equal,
    has_aggregate,
    identifier,
    public_value,
    rule,
)
from grapharrow.use_cases.cypher_path import PathResult, PathValue
from grapharrow.use_cases.cypher_pipeline import (
    PathBinding,
    ReadCompiler,
    ReadRuntime,
    Stage,
    expression_kind,
    ordered_clauses,
)
from grapharrow.use_cases.graph import GraphTransactionUseCase, QueryResult


@dataclass(frozen=True)
class WriteNode:
    name: str
    bound: bool
    labels: tuple[str, ...]
    properties: Expression | None


@dataclass(frozen=True)
class WriteRelationship:
    name: str
    left: str
    right: str
    rel_type: str
    direction: Literal["incoming", "outgoing", "undirected"]
    properties: Expression | None


@dataclass(frozen=True)
class PropertyUpdate:
    target: str
    mode: Literal["property", "merge", "replace", "add_labels", "remove_labels"]
    value: Expression | None = None
    key: str = ""
    labels: tuple[str, ...] = ()


@dataclass(frozen=True)
class CreateStage:
    nodes: tuple[WriteNode, ...]
    relationships: tuple[WriteRelationship, ...]
    paths: tuple[PathBinding, ...]


@dataclass(frozen=True)
class MergeStage:
    pattern: CreateStage
    on_create: tuple[PropertyUpdate, ...]
    on_match: tuple[PropertyUpdate, ...]


@dataclass(frozen=True)
class SetStage:
    items: tuple[PropertyUpdate, ...]


@dataclass(frozen=True)
class DeleteStage:
    targets: tuple[Expression, ...]
    detach: bool


UpdateStage = CreateStage | MergeStage | SetStage | DeleteStage


@dataclass(frozen=True)
class WriteQuery:
    stages: tuple[Stage | UpdateStage, ...]
    columns: tuple[str, ...]


def _labels(node: Any) -> tuple[str, ...]:
    return (
        ()
        if node is None
        else tuple(identifier(item.oC_LabelName().getText()) for item in node.oC_NodeLabel())
    )


class WriteCompiler(ReadCompiler):
    def compile_write(self, tree: Any) -> WriteQuery:
        regular = tree.oC_Statement().oC_Query().oC_RegularQuery()
        if regular is None or regular.oC_Union():
            raise QueryError.unsupported("Updating UNION branches are not supported.")
        self.reserve_variables(regular)
        stages: list[Stage | UpdateStage] = []
        columns: tuple[str, ...] = ()
        scope: Scope = {}
        for clause in ordered_clauses(regular.oC_SingleQuery()):
            kind = rule(clause)
            stage: Stage | UpdateStage
            if kind == "oC_ReadingClause":
                stage, scope = self.reading(clause, scope)
            elif kind in {"oC_With", "oC_Return"}:
                stage, scope = self.projection(clause, scope, {}, terminal=kind == "oC_Return")
                if kind == "oC_Return":
                    columns = tuple(name for name, _ in stage.items)
            else:
                stage, scope = self.update(clause, scope)
            stages.append(stage)
        if not any(
            isinstance(stage, CreateStage | MergeStage | SetStage | DeleteStage) for stage in stages
        ):
            raise QueryError.unsupported("Updating subqueries are not supported.")
        return WriteQuery(tuple(stages), columns)

    def update(self, clause: Any, scope: Scope) -> tuple[UpdateStage, Scope]:
        node = children(clause)[0]
        kind = rule(node)
        if kind == "oC_Create":
            return self.pattern(node.oC_Pattern().oC_PatternPart(), scope, merge=False)
        if kind == "oC_Merge":
            pattern, visible = self.pattern([node.oC_PatternPart()], scope, merge=True)
            on_create: list[PropertyUpdate] = []
            on_match: list[PropertyUpdate] = []
            for action in node.oC_MergeAction():
                items = on_create if action.CREATE() is not None else on_match
                items.extend(self.set_item(item, visible) for item in action.oC_Set().oC_SetItem())
            return MergeStage(pattern, tuple(on_create), tuple(on_match)), visible
        if kind == "oC_Set":
            return SetStage(tuple(self.set_item(p, scope) for p in node.oC_SetItem())), scope
        if kind == "oC_Remove":
            return SetStage(
                tuple(self.set_item(p, scope, remove=True) for p in node.oC_RemoveItem())
            ), scope

        targets = tuple(self.value(p, scope) for p in node.oC_Expression())
        for target in targets:
            if expression_kind(target, scope) not in {
                "node",
                "relationship",
                "path",
                "any",
                "null",
            }:
                raise QueryError.syntax(
                    "DELETE requires a node, relationship or path.", "InvalidDelete"
                )
        return DeleteStage(targets, node.DETACH() is not None), scope

    def value(self, node: Any, scope: Scope) -> Expression:
        expression = self.expressions.compile(node, scope)
        if has_aggregate(expression):
            raise QueryError.syntax(
                "Updating expressions cannot contain aggregates.", "InvalidAggregation"
            )
        return expression

    def properties(self, node: Any, scope: Scope) -> Expression | None:
        properties = node.oC_Properties()
        return None if properties is None else self.value(properties, scope)

    def pattern(self, parts: list[Any], scope: Scope, *, merge: bool) -> tuple[CreateStage, Scope]:
        nodes: dict[str, WriteNode] = {}
        edges: list[WriteRelationship] = []
        paths: list[PathBinding] = []
        visible = dict(scope)

        def node_spec(node: Any, *, standalone: bool) -> str:
            variable = node.oC_Variable()
            name = (
                identifier(variable.getText())
                if variable is not None
                else self.state.next_variable()
            )
            labels = _labels(node.oC_NodeLabels())
            bound = name in visible
            if bound and visible[name] not in {"node", "any", "null"}:
                raise QueryError.syntax(f"Variable {name} must be a node.", "VariableTypeConflict")
            if bound and (labels or node.oC_Properties() is not None or standalone):
                raise QueryError.syntax(
                    f"Node variable already bound: {name}", "VariableAlreadyBound"
                )
            if variable is not None:
                visible[name] = "node"
            if name not in nodes:
                nodes[name] = WriteNode(name, bound, labels, self.properties(node, scope))
            return name

        for part in parts:
            alias = part.oC_Variable()
            element = part.oC_AnonymousPatternPart().oC_PatternElement()
            while element.oC_NodePattern() is None:
                element = element.oC_PatternElement()
            chain = element.oC_PatternElementChain()
            left = node_spec(element.oC_NodePattern(), standalone=not chain)
            path_nodes = [left]
            path_edges = []
            for step in chain:
                right = node_spec(step.oC_NodePattern(), standalone=False)
                relationship = step.oC_RelationshipPattern()
                detail = relationship.oC_RelationshipDetail()
                if detail is not None and detail.oC_RangeLiteral() is not None:
                    raise QueryError.syntax(
                        "Variable-length relationships are not allowed in CREATE or MERGE.",
                        "CreatingVarLength",
                    )
                types = None if detail is None else detail.oC_RelationshipTypes()
                if types is None or len(types.oC_RelTypeName()) != 1:
                    raise QueryError.syntax(
                        "Exactly one relationship type is required.", "NoSingleRelationshipType"
                    )
                incoming = relationship.oC_LeftArrowHead() is not None
                outgoing = relationship.oC_RightArrowHead() is not None
                if incoming and outgoing or (not merge and not (incoming or outgoing)):
                    raise QueryError.syntax(
                        "CREATE requires a directed relationship with a single arrow.",
                        "RequiresDirectedRelationship",
                    )
                variable = detail.oC_Variable()
                name = (
                    identifier(variable.getText())
                    if variable is not None
                    else self.state.next_variable()
                )
                if name in visible:
                    raise QueryError.syntax(
                        f"Relationship variable already bound: {name}", "VariableAlreadyBound"
                    )
                if variable is not None:
                    visible[name] = "relationship"
                edges.append(
                    WriteRelationship(
                        name,
                        left,
                        right,
                        identifier(types.oC_RelTypeName(0).getText()),
                        "incoming" if incoming else "outgoing" if outgoing else "undirected",
                        self.properties(detail, scope),
                    )
                )
                left = right
                path_nodes.append(right)
                path_edges.append(name)
            if alias is not None:
                name = identifier(alias.getText())
                if name in visible:
                    detail = (
                        "VariableTypeConflict"
                        if visible[name] != "path"
                        else "VariableAlreadyBound"
                    )
                    raise QueryError.syntax(f"Cannot bind path variable {name}.", detail)
                visible[name] = "path"
                paths.append(PathBinding(name, tuple(path_nodes), tuple(path_edges)))
        return CreateStage(tuple(nodes.values()), tuple(edges), tuple(paths)), visible

    def set_item(self, node: Any, scope: Scope, *, remove: bool = False) -> PropertyUpdate:
        prop = node.oC_PropertyExpression()
        labels = _labels(node.oC_NodeLabels())
        if prop is not None:
            variable = prop.oC_Atom().oC_Variable()
            if variable is None or len(prop.oC_PropertyLookup()) != 1:
                raise QueryError.unsupported(
                    "Updating a property requires a single entity binding."
                )
        else:
            variable = node.oC_Variable()
        name = identifier(variable.getText())
        if name not in scope:
            raise QueryError.syntax(f"Unknown variable: {name}", "UndefinedVariable")
        allowed = {"node", "any", "null"} if labels else {"node", "relationship", "any", "null"}
        if scope[name] not in allowed:
            raise QueryError.invalid_type(f"Cannot update variable {name}.", phase="compile")
        if labels:
            return PropertyUpdate(name, "remove_labels" if remove else "add_labels", labels=labels)
        value = None if remove else self.value(node.oC_Expression(), scope)
        if prop is not None:
            key = identifier(prop.oC_PropertyLookup(0).oC_PropertyKeyName().getText())
            return PropertyUpdate(name, "property", value, key)
        merge = any(child.getText() == "+=" for child in node.children)
        return PropertyUpdate(name, "merge" if merge else "replace", value)


class WriteRuntime(ReadRuntime):
    def __init__(self, executor: core.CypherQueryUseCase) -> None:
        super().__init__(executor)
        graph = executor._graph_service
        assert isinstance(graph, GraphTransactionUseCase)
        self.graph = graph
        state = graph.load_state()
        self.nodes = {node.node_id: node for node in state.nodes}
        self.edges = {edge.edge_id: edge for edge in state.edges}
        self.next_node = max(self.nodes, default=0) + 1
        self.next_edge = max(self.edges, default=0) + 1
        self.evaluator = ExpressionEvaluator(
            self.evaluator.exists,
            resolve_entity=self.live,
            transaction_time=self.graph.transaction_time_ns,
        )

    def live(self, value: Any) -> Any:
        if isinstance(value, NodeRecord):
            record: NodeRecord | EdgeRecord | None = self.nodes.get(value.node_id)
        elif isinstance(value, EdgeRecord):
            record = self.edges.get(value.edge_id)
        else:
            return value
        if record is None:
            raise QueryError(
                "Cannot access properties or labels of a deleted entity.",
                category="EntityNotFound",
                detail="DeletedEntityAccess",
            )
        return record

    def output(self, value: Any) -> Any:
        if isinstance(value, PathValue):
            return PathResult(
                nodes=self.output(value.nodes), relationships=self.output(value.relationships)
            )
        if isinstance(value, NodeRecord):
            return public_value(self.nodes.get(value.node_id, value))
        if isinstance(value, EdgeRecord):
            return public_value(self.edges.get(value.edge_id, value))
        if isinstance(value, list | tuple):
            return [self.output(item) for item in value]
        if isinstance(value, dict):
            return {key: self.output(item) for key, item in value.items()}
        return public_value(value)

    def execute_write(self, query: WriteQuery) -> QueryResult:
        rows: list[Row] = [{}]
        for stage in query.stages:
            if isinstance(stage, CreateStage):
                rows = [self.create(stage, row) for row in rows]
            elif isinstance(stage, MergeStage):
                rows = self.merge(stage, rows)
            elif isinstance(stage, SetStage):
                for row in rows:
                    self.set_items(stage.items, row)
            elif isinstance(stage, DeleteStage):
                self.delete(stage, rows)
            else:
                rows = self.apply_stage(stage, rows, {})
                continue
            previous = self.graph.load_state()
            state = self.graph.replace_records(
                tuple(self.nodes.values()), tuple(self.edges.values())
            )
            if state is not previous:
                self._state = None
                self._match_indexes.clear()
        return QueryResult(
            query.columns,
            tuple({name: self.output(row[name]) for name in query.columns} for row in rows)
            if query.columns
            else (),
        )

    def properties(self, expression: Expression | None, row: Row) -> dict[str, object]:
        if expression is None:
            return {}
        value = self.evaluator.evaluate(expression, row)
        if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
            raise QueryError.invalid_type("Pattern properties require a map.", phase="runtime")
        return dict(value)

    def create(
        self, pattern: CreateStage, row: Row, properties: dict[str, dict[str, object]] | None = None
    ) -> Row:
        result = dict(row)
        for node in pattern.nodes:
            if node.bound:
                result[node.name] = self.node_endpoint(row[node.name])
            else:
                values = (
                    self.properties(node.properties, row)
                    if properties is None
                    else properties[node.name]
                )
                record = NodeRecord(self.next_node, node.labels, core._cypher_properties(values))
                self.nodes[record.node_id] = record
                self.next_node += 1
                result[node.name] = record
        for relationship in pattern.relationships:
            left, right = result[relationship.left], result[relationship.right]
            if relationship.direction == "incoming":
                left, right = right, left
            values = (
                self.properties(relationship.properties, row)
                if properties is None
                else properties[relationship.name]
            )
            edge = EdgeRecord(
                self.next_edge,
                relationship.rel_type,
                left.node_id,
                right.node_id,
                core._cypher_properties(values),
            )
            self.edges[edge.edge_id] = edge
            self.next_edge += 1
            result[relationship.name] = edge
        return self.bind_paths(pattern, result)

    @staticmethod
    def bind_paths(pattern: CreateStage, result: Row) -> Row:
        for path in pattern.paths:
            result[path.name] = path.value(result)
        return result

    def node_endpoint(self, entity: Any) -> NodeRecord:
        if not isinstance(entity, NodeRecord):
            raise QueryError.invalid_type(
                "A relationship endpoint must be a bound node.", phase="runtime"
            )
        record = self.live(entity)
        assert isinstance(record, NodeRecord)
        return record

    def merge(self, stage: MergeStage, rows: list[Row]) -> list[Row]:
        output = []
        pattern = stage.pattern
        adjacency = MergeAdjacency(self.edges.values()) if rows and pattern.relationships else None
        for row in rows:
            properties = {
                node.name: self.properties(node.properties, row) for node in pattern.nodes
            }
            for relationship in pattern.relationships:
                properties[relationship.name] = self.properties(relationship.properties, row)
            for values in properties.values():
                core._validate_merge_properties(values)
            matches = self.merge_matches(pattern, row, properties, adjacency)
            if matches:
                for match in matches:
                    self.set_items(stage.on_match, match)
                output.extend(matches)
            else:
                created = self.create(pattern, row, properties)
                if adjacency is not None:
                    for spec in pattern.relationships:
                        adjacency.add(created[spec.name])
                self.set_items(stage.on_create, created)
                output.append(created)
        return output

    def merge_matches(
        self,
        pattern: CreateStage,
        row: Row,
        properties: dict[str, dict[str, object]],
        adjacency: MergeAdjacency | None,
    ) -> list[Row]:
        bound = {
            node.name: self.node_endpoint(row[node.name]) for node in pattern.nodes if node.bound
        }
        by_name = {node.name: node for node in pattern.nodes}

        def matches(node: NodeRecord, name: str) -> bool:
            return all(
                label in node.labels for label in by_name[name].labels
            ) and _matches_properties(node.properties, properties[name])

        start = pattern.nodes[0].name
        starts = (bound[start],) if start in bound else self.nodes.values()
        candidates: list[tuple[Row, frozenset[int]]] = [
            (bound | {start: node}, frozenset()) for node in starts if matches(node, start)
        ]
        if adjacency is not None:
            for spec in pattern.relationships:
                extended = []
                for bindings, used_edges in candidates:
                    left = bindings[spec.left]
                    right = bindings.get(spec.right)
                    for edge_id, other_id in adjacency.follow(spec, left.node_id):
                        if edge_id in used_edges or (
                            right is not None and right.node_id != other_id
                        ):
                            continue
                        edge = self.edges[edge_id]
                        other = self.nodes[other_id]
                        if matches(other, spec.right) and _matches_properties(
                            edge.properties, properties[spec.name]
                        ):
                            extended.append(
                                (
                                    bindings | {spec.right: other, spec.name: edge},
                                    used_edges | {edge_id},
                                )
                            )
                candidates = extended
                if not candidates:
                    break
        return [self.bind_paths(pattern, row | bindings) for bindings, _ in candidates]

    def set_items(self, items: tuple[PropertyUpdate, ...], row: Row) -> None:
        for item in items:
            entity = self.live(row[item.target])
            if entity is None:
                continue
            if not isinstance(entity, NodeRecord | EdgeRecord):
                raise QueryError.invalid_type(
                    "SET and REMOVE require a graph entity.", phase="runtime"
                )
            if item.mode in {"add_labels", "remove_labels"}:
                if not isinstance(entity, NodeRecord):
                    raise QueryError.invalid_type("Only nodes have labels.", phase="runtime")
                labels = (
                    tuple(dict.fromkeys((*entity.labels, *item.labels)))
                    if item.mode == "add_labels"
                    else tuple(label for label in entity.labels if label not in item.labels)
                )
                self.nodes[entity.node_id] = replace(entity, labels=labels)
                continue
            value = None if item.value is None else self.evaluator.evaluate(item.value, row)
            if item.mode == "property":
                changes = {item.key: value}
            else:
                value = self.live(value)
                if isinstance(value, NodeRecord | EdgeRecord):
                    value = value.properties
                if value is None:
                    value = {}
                if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
                    keyword = "+=" if item.mode == "merge" else "="
                    raise QueryError.invalid_type(
                        f"SET {keyword} requires a map value or an entity.", phase="runtime"
                    )
                changes = value
            properties = {} if item.mode == "replace" else dict(entity.properties)
            for key, entry in changes.items():
                if entry is None:
                    properties.pop(key, None)
                else:
                    properties[key] = core._validate_cypher_property(entry)
            if isinstance(entity, NodeRecord):
                self.nodes[entity.node_id] = replace(entity, properties=properties)
            else:
                self.edges[entity.edge_id] = replace(entity, properties=properties)

    def delete(self, stage: DeleteStage, rows: list[Row]) -> None:
        nodes: set[int] = set()
        edges: set[int] = set()
        for row in rows:
            for expression in stage.targets:
                entity = self.evaluator.evaluate(expression, row)
                if isinstance(entity, NodeRecord):
                    nodes.add(entity.node_id)
                elif isinstance(entity, EdgeRecord):
                    edges.add(entity.edge_id)
                elif isinstance(entity, PathValue):
                    nodes.update(node.node_id for node in entity.nodes)
                    edges.update(edge.edge_id for edge in entity.relationships)
                elif entity is not None:
                    raise QueryError.invalid_type(
                        "DELETE requires a node, relationship or path.", phase="runtime"
                    )
        for edge in self.edges.values():
            if edge.src_id in nodes or edge.dst_id in nodes:
                if not stage.detach and edge.edge_id not in edges:
                    raise QueryError(
                        "DELETE cannot remove a node with existing relationships.",
                        category="ConstraintVerificationFailed",
                        detail="DeleteConnectedNode",
                    )
                edges.add(edge.edge_id)
        for edge_id in edges:
            self.edges.pop(edge_id, None)
        for node_id in nodes:
            self.nodes.pop(node_id, None)


class MergeAdjacency:
    """Index live relationship identities for one MERGE clause."""

    def __init__(self, edges: Iterable[EdgeRecord]) -> None:
        self.outgoing: dict[tuple[str, int], list[tuple[int, int]]] = {}
        self.incoming: dict[tuple[str, int], list[tuple[int, int]]] = {}
        for edge in edges:
            self.add(edge)

    def add(self, edge: EdgeRecord) -> None:
        self.outgoing.setdefault((edge.rel_type, edge.src_id), []).append(
            (edge.edge_id, edge.dst_id)
        )
        self.incoming.setdefault((edge.rel_type, edge.dst_id), []).append(
            (edge.edge_id, edge.src_id)
        )

    def follow(self, spec: WriteRelationship, node_id: int) -> Iterator[tuple[int, int]]:
        key = (spec.rel_type, node_id)
        if spec.direction != "incoming":
            yield from self.outgoing.get(key, ())
        if spec.direction != "outgoing":
            for edge_id, other_id in self.incoming.get(key, ()):
                if spec.direction == "incoming" or other_id != node_id:
                    yield edge_id, other_id


def _matches_properties(actual: dict[str, object], expected: dict[str, object]) -> bool:
    return all(equal(actual.get(key), value) is True for key, value in expected.items())
