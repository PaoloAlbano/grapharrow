# mypy: disable-error-code=no-untyped-call
"""Use case for executing the supported Cypher subset."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, replace
from typing import Any, Literal, TypeAlias, cast

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.compute as pc  # type: ignore[import-untyped]

from grapharrow.adapters import PyArrowTabularAdapter
from grapharrow.adapters.query_cache import CypherParseCache
from grapharrow.core.errors import CatalogError, QueryError
from grapharrow.domain import IndexKind, PropertyOwnerKind
from grapharrow.domain.scan import ScanOperator, ScanPredicate
from grapharrow.interfaces.opencypher.generated.vendor.opencypher.CypherParser import CypherParser
from grapharrow.repositories import EdgeRecord, NodeRecord
from grapharrow.use_cases.cypher_plan import (
    AggregateCountPlan,
    DistinctPlan,
    ExpandPlan,
    FilterPlan,
    LogicalPlan,
    MatchLogicalPlan,
    MatchLogicalPlanResult,
    NodeScanPlan,
    OptionalMatchPlan,
    ProjectPlan,
    ReturnProjectPlan,
    SlicePlan,
    SortPlan,
    WithAggregatePlan,
    WithProjectPlan,
)
from grapharrow.use_cases.graph import (
    GraphState,
    GraphStateUseCase,
    GraphTabularState,
    GraphTransactionUseCase,
    QueryResult,
)
from grapharrow.use_cases.indexes import IndexCatalogUseCase

_IDENTIFIER_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_INTERNAL_VARIABLE_PREFIX = "__varhop_"
_FUNCTION_FIELD_PREFIX = "__fn__:"
_ENTITY_ID_FIELD = "__entity_id"
_NODE_ID_FIELD = "__id"
_JOIN_JSON_PREFIX = "__grapharrow_join_json__:"
_FULL_ENTITY_FIELDS = "*"
_DISABLE_PLANNER_ENV = "GRAPHARROW_DISABLE_CYPHER_PLANNER"
_UNRESOLVED_REFERENCE = object()
_INCREMENTAL_MATCH_MAX_INPUT_ROWS = 256
NodeBindingSource: TypeAlias = tuple[NodeRecord, ...] | pa.Table
EdgeBindingSource: TypeAlias = tuple[EdgeRecord, ...] | pa.Table
PropertyTableLoader: TypeAlias = Callable[[str, set[str] | None], pa.Table]


@dataclass(frozen=True, slots=True)
class _PropertyProjectionLoader:
    load: PropertyTableLoader
    lookup: Callable[[str, dict[str, object]], pa.Table | None] | None = None
    scan: Callable[[str, set[str] | None, tuple[ScanPredicate, ...]], pa.Table] | None = None

    def __call__(self, label: str, fields: set[str] | None = None) -> pa.Table:
        return self.load(label, fields)


@dataclass(frozen=True, slots=True)
class NodePattern:
    variable: str
    labels: tuple[str, ...]
    properties: dict[str, object]


@dataclass(frozen=True, slots=True)
class RelationshipStep:
    variable: str | None
    rel_type: str | None
    properties: dict[str, object]
    node: NodePattern
    direction: str = "outgoing"
    min_hops: int = 1
    max_hops: int = 1
    rel_type_alternatives: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class PathPattern:
    start: NodePattern
    steps: tuple[RelationshipStep, ...]


CompiledPattern = NodePattern | PathPattern
ScopeBindingKind = Literal["entity", "scalar"]


@dataclass(frozen=True, slots=True)
class CreateRelationshipPattern:
    left_variable: str
    left_bound: bool
    left_labels: tuple[str, ...]
    left_properties: dict[str, object]
    right_variable: str
    right_bound: bool
    right_labels: tuple[str, ...]
    right_properties: dict[str, object]
    rel_type: str
    properties: dict[str, object]


@dataclass(frozen=True, slots=True)
class MergeNodePattern:
    variable: str
    labels: tuple[str, ...]
    properties: dict[str, object]


@dataclass(frozen=True, slots=True)
class MergeRelationshipPattern:
    variable: str | None
    left_bound: bool
    left_labels: tuple[str, ...]
    left_properties: dict[str, object]
    right_bound: bool
    right_labels: tuple[str, ...]
    right_properties: dict[str, object]
    left_variable: str
    right_variable: str
    rel_type: str
    properties: dict[str, object]


@dataclass(frozen=True, slots=True)
class PropertyReferenceValue:
    variable: str
    field: str


@dataclass(frozen=True, slots=True)
class SetPropertyItem:
    variable: str
    field: str
    value: object


@dataclass(frozen=True, slots=True)
class SetMapMergeItem:
    variable: str
    properties: dict[str, object]


@dataclass(frozen=True, slots=True)
class SetMapReferenceMergeItem:
    variable: str
    source: PropertyReferenceValue


SetItem: TypeAlias = SetPropertyItem | SetMapMergeItem | SetMapReferenceMergeItem


@dataclass(frozen=True, slots=True)
class RemovePropertyItem:
    variable: str
    field: str


@dataclass(frozen=True, slots=True)
class RemoveLabelItem:
    variable: str
    label: str


RemoveItem: TypeAlias = RemovePropertyItem | RemoveLabelItem


@dataclass(frozen=True, slots=True)
class FieldExpression:
    variable: str
    field: str


@dataclass(frozen=True, slots=True)
class LiteralExpression:
    value: object


@dataclass(frozen=True, slots=True)
class BinaryExpression:
    operator: str
    left: ScalarExpression
    right: ScalarExpression


@dataclass(frozen=True, slots=True)
class FunctionExpression:
    name: str
    argument: ScalarExpression


ScalarExpression = FieldExpression | LiteralExpression | BinaryExpression | FunctionExpression


@dataclass(frozen=True, slots=True)
class Comparison:
    variable: str
    field: str
    operator: str
    value: object
    left_expression: ScalarExpression | None = None


@dataclass(frozen=True, slots=True)
class StringPredicate:
    variable: str
    field: str
    operator: str
    value: object


@dataclass(frozen=True, slots=True)
class ExistsPredicate:
    variable: str
    field: str


@dataclass(frozen=True, slots=True)
class ExistsPatternPredicate:
    patterns: tuple[CompiledPattern, ...]
    where_predicate: WherePredicate | None = None


@dataclass(frozen=True, slots=True)
class InPredicate:
    variable: str
    field: str
    values: tuple[object, ...]


@dataclass(frozen=True, slots=True)
class NullPredicate:
    variable: str
    field: str
    is_null: bool


@dataclass(frozen=True, slots=True)
class LabelPredicate:
    variable: str
    label: str


@dataclass(frozen=True, slots=True)
class NotPredicate:
    predicate: WherePredicate


@dataclass(frozen=True, slots=True)
class AndPredicate:
    predicates: tuple[WherePredicate, ...]


@dataclass(frozen=True, slots=True)
class OrPredicate:
    predicates: tuple[WherePredicate, ...]


WherePredicate = (
    Comparison
    | StringPredicate
    | ExistsPredicate
    | ExistsPatternPredicate
    | InPredicate
    | NullPredicate
    | LabelPredicate
    | NotPredicate
    | AndPredicate
    | OrPredicate
)
DistinctEndpointRangeShape: TypeAlias = tuple["PathPattern", "ReturnItem"]


@dataclass(frozen=True, slots=True)
class ReturnItem:
    expression: str
    variable: str
    field: str | None
    scalar_expression: ScalarExpression | None = None
    aggregate: str | None = None
    count_all: bool = False
    aggregate_distinct: bool = False
    aggregate_transform: str | None = None


@dataclass(frozen=True, slots=True)
class WithItem:
    output_name: str
    source_variable: str
    source_field: str | None
    source_kind: ScopeBindingKind


@dataclass(frozen=True, slots=True)
class OrderByItem:
    variable: str
    field: str
    descending: bool


@dataclass(frozen=True, slots=True)
class CompiledUnwind:
    output_name: str
    literal_values: tuple[object, ...] | None
    source_variable: str | None
    source_field: str | None


@dataclass(frozen=True, slots=True)
class CompiledCreateQuery:
    patterns: tuple[CompiledPattern, ...]
    return_items: tuple[ReturnItem, ...]
    distinct: bool


@dataclass(frozen=True, slots=True)
class CompiledMergeQuery:
    pattern: MergeNodePattern
    on_create_set_items: tuple[SetItem, ...]
    on_match_set_items: tuple[SetItem, ...]
    return_items: tuple[ReturnItem, ...]
    distinct: bool


@dataclass(frozen=True, slots=True)
class CompiledMatchQuery:
    patterns: tuple[CompiledPattern, ...]
    where_predicate: WherePredicate | None
    return_items: tuple[ReturnItem, ...]
    distinct: bool
    order_by: tuple[OrderByItem, ...] | None
    skip: int | None
    limit: int | None


@dataclass(frozen=True, slots=True)
class CompiledOptionalMatchQuery:
    patterns: tuple[CompiledPattern, ...]
    where_predicate: WherePredicate | None
    return_items: tuple[ReturnItem, ...]
    distinct: bool
    order_by: tuple[OrderByItem, ...] | None
    skip: int | None
    limit: int | None


@dataclass(frozen=True, slots=True)
class CompiledUnwindQuery:
    unwind: CompiledUnwind
    return_items: tuple[ReturnItem, ...]
    distinct: bool
    order_by: tuple[OrderByItem, ...] | None
    skip: int | None
    limit: int | None


@dataclass(frozen=True, slots=True)
class CompiledMatchOptionalQuery:
    match_patterns: tuple[CompiledPattern, ...]
    match_where_predicate: WherePredicate | None
    optional_patterns: tuple[CompiledPattern, ...]
    optional_where_predicate: WherePredicate | None
    return_items: tuple[ReturnItem, ...]
    distinct: bool
    order_by: tuple[OrderByItem, ...] | None
    skip: int | None
    limit: int | None


@dataclass(frozen=True, slots=True)
class CompiledMatchCreateQuery:
    match_patterns: tuple[CompiledPattern, ...]
    where_predicate: WherePredicate | None
    create_patterns: tuple[CreateRelationshipPattern, ...]
    return_items: tuple[ReturnItem, ...]
    distinct: bool
    order_by: tuple[OrderByItem, ...] | None
    skip: int | None
    limit: int | None


@dataclass(frozen=True, slots=True)
class CompiledMatchMergeQuery:
    match_patterns: tuple[CompiledPattern, ...]
    where_predicate: WherePredicate | None
    merge_pattern: MergeRelationshipPattern
    on_create_set_items: tuple[SetItem, ...]
    on_match_set_items: tuple[SetItem, ...]
    return_items: tuple[ReturnItem, ...]
    distinct: bool
    order_by: tuple[OrderByItem, ...] | None
    skip: int | None
    limit: int | None


@dataclass(frozen=True, slots=True)
class CompiledMatchSetQuery:
    match_patterns: tuple[CompiledPattern, ...]
    where_predicate: WherePredicate | None
    set_items: tuple[SetItem, ...]
    return_items: tuple[ReturnItem, ...]
    distinct: bool
    order_by: tuple[OrderByItem, ...] | None
    skip: int | None
    limit: int | None


@dataclass(frozen=True, slots=True)
class DeleteItem:
    variable: str


@dataclass(frozen=True, slots=True)
class CompiledMatchDeleteQuery:
    match_patterns: tuple[CompiledPattern, ...]
    where_predicate: WherePredicate | None
    detach: bool
    delete_items: tuple[DeleteItem, ...]
    return_items: tuple[ReturnItem, ...]
    distinct: bool
    order_by: tuple[OrderByItem, ...] | None
    skip: int | None
    limit: int | None


@dataclass(frozen=True, slots=True)
class CompiledMatchRemoveQuery:
    match_patterns: tuple[CompiledPattern, ...]
    where_predicate: WherePredicate | None
    remove_items: tuple[RemoveItem, ...]
    return_items: tuple[ReturnItem, ...]
    distinct: bool
    order_by: tuple[OrderByItem, ...] | None
    skip: int | None
    limit: int | None


@dataclass(frozen=True, slots=True)
class CompiledMatchWithQuery:
    match_patterns: tuple[CompiledPattern, ...]
    match_where_predicate: WherePredicate | None
    match_optional: bool
    pre_with_unwind: CompiledUnwind | None
    with_items: tuple[WithItem, ...]
    with_aggregate_items: tuple[ReturnItem, ...]
    with_where_predicate: WherePredicate | None
    with_distinct: bool
    with_order_by: tuple[OrderByItem, ...] | None
    with_skip: int | None
    with_limit: int | None
    post_with_match_patterns: tuple[CompiledPattern, ...]
    post_with_match_where_predicate: WherePredicate | None
    post_with_match_optional: bool
    post_with_unwind: CompiledUnwind | None
    post_with_create_patterns: tuple[CompiledPattern, ...]
    post_with_create_relationship_patterns: tuple[CreateRelationshipPattern, ...]
    post_with_merge_node_pattern: MergeNodePattern | None
    post_with_merge_relationship_pattern: MergeRelationshipPattern | None
    post_with_merge_on_create_set_items: tuple[SetItem, ...]
    post_with_merge_on_match_set_items: tuple[SetItem, ...]
    post_with_set_items: tuple[SetItem, ...]
    post_with_delete_detach: bool
    post_with_delete_items: tuple[DeleteItem, ...]
    return_items: tuple[ReturnItem, ...]
    return_distinct: bool
    return_order_by: tuple[OrderByItem, ...] | None
    return_skip: int | None
    return_limit: int | None


CompiledQuery: TypeAlias = (
    CompiledCreateQuery
    | CompiledMergeQuery
    | CompiledMatchQuery
    | CompiledOptionalMatchQuery
    | CompiledUnwindQuery
    | CompiledMatchOptionalQuery
    | CompiledMatchCreateQuery
    | CompiledMatchMergeQuery
    | CompiledMatchSetQuery
    | CompiledMatchDeleteQuery
    | CompiledMatchRemoveQuery
    | CompiledMatchWithQuery
)


@dataclass(slots=True)
class _CompilerState:
    anonymous_index: int = 0

    def next_variable(self) -> str:
        """Return a fresh anonymous variable name for compiler-generated bindings."""
        self.anonymous_index += 1
        return f"_anon_{self.anonymous_index}"


@dataclass(frozen=True, slots=True)
class _ParsedIndexDeclaration:
    """Index declaration extracted from client DDL syntax before Cypher parsing."""

    name: str
    kind: IndexKind
    owner_kind: PropertyOwnerKind
    owner_name: str
    property_names: tuple[str, ...]
    if_not_exists: bool = False


@dataclass(frozen=True, slots=True)
class _ParsedDropIndexStatement:
    """DROP INDEX statement extracted before Cypher parsing."""

    name: str
    if_exists: bool = False


class CypherQueryUseCase:
    """Execute a narrow but useful subset of Cypher statements."""

    def __init__(
        self,
        graph_service: GraphStateUseCase,
        tabular_adapter: PyArrowTabularAdapter | None = None,
        index_service: IndexCatalogUseCase | None = None,
        parse_cache: CypherParseCache | None = None,
    ) -> None:
        """Store the services that the Cypher compiler and executor use at runtime."""
        self._graph_service = graph_service
        self._tabular = tabular_adapter or PyArrowTabularAdapter()
        self._index_service = index_service
        self._parse_cache = parse_cache if parse_cache is not None else CypherParseCache()

    def execute(self, query: str, params: dict[str, object] | None = None) -> QueryResult:
        """Execute ``query`` against the current graph state."""

        normalized = query.strip()
        if not normalized:
            raise QueryError("Query string cannot be empty.")

        parameters = params or {}
        ddl_result = self._execute_ddl_statement(normalized)
        if ddl_result is not None:
            return ddl_result

        compatibility_result = _execute_neo4j_compatibility_procedure(normalized)
        if compatibility_result is not None:
            return compatibility_result

        standalone_return = _execute_standalone_return(normalized, parameters)
        if standalone_return is not None:
            return standalone_return

        compatibility_lower = _normalize_compatibility_statement(normalized).lower()
        if compatibility_lower.startswith("show "):
            show_result = _execute_show_compatibility_statement(
                normalized,
                self._graph_service.load_state(),
            )
            if show_result is not None:
                return show_result

        if _is_schema_compatibility_procedure(compatibility_lower):
            schema_result = _execute_schema_compatibility_procedure(
                normalized,
                self._graph_service.load_state(),
            )
            if schema_result is not None:
                return schema_result

        parse_tree = self._parse_cache.parse(query)
        compiled = _compile_query(parse_tree, parameters)
        writes = (
            isinstance(
                compiled,
                (
                    CompiledCreateQuery,
                    CompiledMergeQuery,
                    CompiledMatchCreateQuery,
                    CompiledMatchMergeQuery,
                    CompiledMatchSetQuery,
                    CompiledMatchDeleteQuery,
                    CompiledMatchRemoveQuery,
                ),
            )
            or isinstance(compiled, CompiledMatchWithQuery)
            and bool(
                compiled.post_with_create_patterns
                or compiled.post_with_create_relationship_patterns
                or compiled.post_with_merge_node_pattern
                or compiled.post_with_merge_relationship_pattern
                or compiled.post_with_set_items
                or compiled.post_with_delete_items
            )
        )
        if writes and not isinstance(self._graph_service, GraphTransactionUseCase):
            with self._graph_service.writer_lock():
                return self._execute_compiled(compiled)
        return self._execute_compiled(compiled)

    def _execute_compiled(self, compiled: CompiledQuery) -> QueryResult:
        """Dispatch a compiled statement under its caller's mutation boundary."""
        if isinstance(compiled, CompiledCreateQuery):
            return self._execute_create(compiled)
        if isinstance(compiled, CompiledMergeQuery):
            return self._execute_merge(compiled)
        if isinstance(compiled, CompiledOptionalMatchQuery):
            return self._execute_optional_match(compiled)
        if isinstance(compiled, CompiledUnwindQuery):
            return self._execute_unwind(compiled)
        if isinstance(compiled, CompiledMatchOptionalQuery):
            return self._execute_match_optional(compiled)
        if isinstance(compiled, CompiledMatchCreateQuery):
            return self._execute_match_create(compiled)
        if isinstance(compiled, CompiledMatchMergeQuery):
            return self._execute_match_merge(compiled)
        if isinstance(compiled, CompiledMatchSetQuery):
            return self._execute_match_set(compiled)
        if isinstance(compiled, CompiledMatchDeleteQuery):
            return self._execute_match_delete(compiled)
        if isinstance(compiled, CompiledMatchRemoveQuery):
            return self._execute_match_remove(compiled)
        if isinstance(compiled, CompiledMatchWithQuery):
            return self._execute_match_with(compiled)
        return self._execute_match(compiled)

    def _execute_ddl_statement(self, query: str) -> QueryResult | None:
        """Handle index DDL before the ANTLR parser sees client-specific syntax."""

        normalized = _normalize_compatibility_statement(query)
        drop_statement = _parse_drop_index_statement(normalized)
        if drop_statement is not None:
            if self._index_service is None:
                return QueryResult(columns=(), rows=())
            try:
                self._index_service.drop_index(
                    name=drop_statement.name,
                    if_exists=drop_statement.if_exists,
                )
            except CatalogError as exc:
                raise QueryError(str(exc)) from exc
            return QueryResult(columns=(), rows=())

        declaration = _parse_create_index_statement(normalized)
        if declaration is None:
            return None
        if self._index_service is None:
            return QueryResult(columns=(), rows=())

        try:
            self._index_service.create_index(
                name=declaration.name,
                kind=declaration.kind,
                owner_kind=declaration.owner_kind,
                owner_name=declaration.owner_name,
                property_names=declaration.property_names,
                if_not_exists=declaration.if_not_exists,
            )
        except CatalogError as exc:
            raise QueryError(str(exc)) from exc
        return QueryResult(columns=(), rows=())

    def _adjacency_loader_for_state(
        self,
        state: GraphState | GraphTabularState,
    ) -> Callable[[str], pa.Table] | None:
        """Support the current Cypher compilation or execution step."""
        source_cache: dict[str, pa.Table] = {}
        dest_cache: dict[str, pa.Table] = {}

        def load_transaction(rel_type: str) -> pa.Table:
            table = source_cache.get(rel_type)
            if table is None:
                types = pa.array(
                    [_cypher_name(value) for value in state.edge_table["rel_type"].to_pylist()],
                    type=pa.string(),
                )
                table = state.edge_table.filter(pc.equal(types, rel_type)).select(
                    ["edge_id", "src_id", "dst_id"]
                )
                source_cache[rel_type] = table
            return table

        def load_source(rel_type: str) -> pa.Table:
            """Load and cache the outgoing adjacency table for the current execution step."""
            if isinstance(self._graph_service, GraphTransactionUseCase):
                return load_transaction(rel_type)
            table = source_cache.get(rel_type)
            if table is None:
                table = self._graph_service.load_adjacency_by_source_table(
                    snapshot=state.snapshot,
                    rel_type=rel_type,
                )
                if table.num_rows == 0:
                    table = self._graph_service.load_adjacency_by_source_table(
                        snapshot=state.snapshot,
                        rel_type=f"`{rel_type}`",
                    )
                source_cache[rel_type] = table
            return table

        def load_dest(rel_type: str) -> pa.Table:
            """Load and cache the incoming adjacency table for the current execution step."""
            table = dest_cache.get(rel_type)
            if table is None:
                if isinstance(self._graph_service, GraphTransactionUseCase):
                    return load_transaction(rel_type)
                table = self._graph_service.load_adjacency_by_dest_table(
                    snapshot=state.snapshot,
                    rel_type=rel_type,
                )
                if table.num_rows == 0:
                    table = self._graph_service.load_adjacency_by_dest_table(
                        snapshot=state.snapshot,
                        rel_type=f"`{rel_type}`",
                    )
                dest_cache[rel_type] = table
            return table

        cast(Any, load_source).by_dest = load_dest

        def select_edges(
            step: RelationshipStep, source_ids: pa.Array | pa.ChunkedArray
        ) -> pa.Table | None:
            if len(source_ids) > max(256, state.node_table.num_rows // 4):
                return None
            unique_ids = pc.drop_null(pc.unique(source_ids))
            if len(unique_ids) == 0:
                return state.edge_table.slice(0, 0)
            # Broad frontiers are better served by the existing vectorized join.
            if len(unique_ids) > max(8, state.node_table.num_rows // 4):
                return None
            index = self._graph_service.adjacency_for_state(state)
            if index is None:
                return None
            names = index.relationship_types(state.edge_table)
            requested = (
                step.rel_type_alternatives
                if len(step.rel_type_alternatives) > 1
                else (step.rel_type,)
                if step.rel_type is not None
                else None
            )
            names = tuple(
                name for name in names if requested is None or _cypher_name(name) in requested
            )
            if requested is not None and len({_cypher_name(name) for name in names}) != len(names):
                # Preserve the established projection/derived-table fallback
                # when imported raw names have ambiguous escaped aliases.
                return None
            return index.select(
                state.edge_table,
                unique_ids.to_pylist(),
                names,
                ("src_id",) if step.direction == "outgoing" else ("dst_id",),
            )

        cast(Any, load_source).select_edges = select_edges
        return load_source

    def _node_property_loader_for_state(
        self,
        state: GraphState | GraphTabularState,
    ) -> PropertyTableLoader | None:
        """Handle node-specific matching, projection, or lookup logic for Cypher."""
        if isinstance(self._graph_service, GraphTransactionUseCase):
            return None
        cache: dict[tuple[str, tuple[str, ...] | None, str], pa.Table] = {}

        def load(
            label: str,
            property_fields: set[str] | None = None,
            predicates: tuple[ScanPredicate, ...] = (),
        ) -> pa.Table:
            """Load and cache the derived table needed by the current execution step."""
            columns = _node_property_projection_columns(property_fields)
            escaped_columns = _escaped_property_projection_columns(columns)
            cache_key = (label, tuple(columns) if columns is not None else None, repr(predicates))
            table = cache.get(cache_key)
            if table is None:
                table = _load_property_table_with_column_fallback(
                    lambda selected_columns: self._graph_service.load_node_property_table(
                        snapshot=state.snapshot,
                        label=label,
                        columns=selected_columns,
                        predicates=predicates,
                    ),
                    columns=columns,
                    escaped_columns=escaped_columns,
                )
                if table.num_columns == 0:
                    table = _load_property_table_with_column_fallback(
                        lambda selected_columns: self._graph_service.load_node_property_table(
                            snapshot=state.snapshot,
                            label=f"`{label}`",
                            columns=selected_columns,
                            predicates=predicates,
                        ),
                        columns=columns,
                        escaped_columns=escaped_columns,
                    )
                table = _normalize_property_table_schema(table)
                cache[cache_key] = table
            return table

        def lookup(label: str, equalities: dict[str, object]) -> pa.Table | None:
            table = self._graph_service.lookup_node_property_table(
                snapshot=state.snapshot,
                catalog=state.catalog,
                label=_cypher_name(label),
                equalities=equalities,
            )
            return _normalize_property_table_schema(table) if table is not None else None

        return _PropertyProjectionLoader(load, lookup, load)

    def _edge_property_loader_for_state(
        self,
        state: GraphState | GraphTabularState,
    ) -> PropertyTableLoader | None:
        """Handle edge-specific matching, projection, or lookup logic for Cypher."""
        if isinstance(self._graph_service, GraphTransactionUseCase):
            return None
        cache: dict[tuple[str, tuple[str, ...] | None, str], pa.Table] = {}

        def load(
            rel_type: str,
            property_fields: set[str] | None = None,
            predicates: tuple[ScanPredicate, ...] = (),
        ) -> pa.Table:
            """Load and cache the derived table needed by the current execution step."""
            columns = _edge_property_projection_columns(property_fields)
            escaped_columns = _escaped_property_projection_columns(columns)
            cache_key = (
                rel_type,
                tuple(columns) if columns is not None else None,
                repr(predicates),
            )
            table = cache.get(cache_key)
            if table is None:
                table = _load_property_table_with_column_fallback(
                    lambda selected_columns: self._graph_service.load_edge_property_table(
                        snapshot=state.snapshot,
                        rel_type=rel_type,
                        columns=selected_columns,
                        predicates=predicates,
                    ),
                    columns=columns,
                    escaped_columns=escaped_columns,
                )
                if table.num_columns == 0:
                    table = _load_property_table_with_column_fallback(
                        lambda selected_columns: self._graph_service.load_edge_property_table(
                            snapshot=state.snapshot,
                            rel_type=f"`{rel_type}`",
                            columns=selected_columns,
                            predicates=predicates,
                        ),
                        columns=columns,
                        escaped_columns=escaped_columns,
                    )
                table = _normalize_property_table_schema(table)
                cache[cache_key] = table
            return table

        return _PropertyProjectionLoader(load, scan=load)

    def _execute_create(self, query: CompiledCreateQuery) -> QueryResult:
        """Execute this query shape and return the result needed by the caller."""
        previous_state = self._graph_service.load_state()
        new_nodes, new_edges, node_bindings = _materialize_create_patterns(
            previous_state,
            query.patterns,
        )
        self._graph_service.publish_records(
            previous_state=previous_state,
            new_nodes=new_nodes,
            new_edges=new_edges,
        )
        if query.return_items:
            result = _build_query_result(query.return_items, [node_bindings])
            return _apply_distinct_to_query_result(result) if query.distinct else result
        return QueryResult(columns=(), rows=())

    def _execute_merge(self, query: CompiledMergeQuery) -> QueryResult:
        """Execute this query shape and return the result needed by the caller."""
        state = self._graph_service.load_state()
        existing_matches = tuple(
            node for node in state.nodes if _node_matches_merge_pattern(node, query.pattern)
        )
        required_fields: dict[str, set[str]] = {}
        _collect_required_fields_from_return_items(query.return_items, required_fields)
        _merge_required_fields(
            required_fields,
            _required_fields_for_entity_variables(
                item.variable for item in (*query.on_create_set_items, *query.on_match_set_items)
            ),
        )
        if existing_matches:
            matched_rows = _node_binding_table(
                existing_matches,
                query.pattern.variable,
                requested_fields=_requested_fields_for_variable(
                    required_fields,
                    query.pattern.variable,
                ),
            )
            if query.on_match_set_items:
                node_updates, edge_updates = _materialize_set_updates_from_matches(
                    matched_rows,
                    query.on_match_set_items,
                )
                self._graph_service.update_properties_batch(
                    node_updates=node_updates,
                    edge_updates=edge_updates,
                )
                matched_rows = _apply_set_items_to_table(matched_rows, query.on_match_set_items)
            return self._finalize_table_query_result(
                query.return_items,
                matched_rows,
                distinct=query.distinct,
                order_by=None,
                skip=None,
                limit=None,
            )

        node_properties = _apply_merge_set_items_to_properties(
            query.pattern.properties,
            variable=query.pattern.variable,
            set_items=query.on_create_set_items,
        )
        node = NodeRecord(
            node_id=max((item.node_id for item in state.nodes), default=0) + 1,
            labels=query.pattern.labels,
            properties=node_properties,
        )
        self._graph_service.publish_records(
            previous_state=state,
            new_nodes=(node,),
            new_edges=(),
        )
        result_rows = _node_binding_table(
            (node,),
            query.pattern.variable,
            requested_fields=_requested_fields_for_variable(
                required_fields,
                query.pattern.variable,
            ),
        )
        return self._finalize_table_query_result(
            query.return_items,
            result_rows,
            distinct=query.distinct,
            order_by=None,
            skip=None,
            limit=None,
        )

    def _execute_match(self, query: CompiledMatchQuery) -> QueryResult:
        """Execute this query shape and return the result needed by the caller."""
        endpoint_range_result = self._try_execute_distinct_endpoint_range_match(query)
        if endpoint_range_result is not None:
            return endpoint_range_result

        logical_plan = (
            _build_match_logical_plan(query)
            if _cypher_planner_enabled()
            and not _predicate_has_exists_pattern(query.where_predicate)
            else None
        )
        if logical_plan is not None:
            planned = self._execute_match_logical_plan(
                logical_plan,
                include_edges=_match_requires_edges(query.patterns, query.where_predicate),
            )
            if planned.query_result is not None:
                return cast(QueryResult, planned.query_result)
            assert planned.table is not None
            return self._finalize_table_query_result(
                query.return_items,
                planned.table,
                distinct=False if planned.planned_distinct else query.distinct,
                order_by=None if planned.planned_order_window else query.order_by,
                skip=None if planned.planned_order_window else query.skip,
                limit=None if planned.planned_order_window else query.limit,
            )
        matched_rows = self._match_rows(
            query.patterns,
            query.where_predicate,
            return_items=query.return_items,
            order_by=None if _contains_aggregates(query.return_items) else query.order_by,
        )
        return self._finalize_table_query_result(
            query.return_items,
            matched_rows,
            distinct=query.distinct,
            order_by=query.order_by,
            skip=query.skip,
            limit=query.limit,
        )

    def _try_execute_distinct_endpoint_range_match(
        self,
        query: CompiledMatchQuery,
    ) -> QueryResult | None:
        """Fast path for bounded variable-length queries returning only distinct endpoints."""
        pushed_patterns, residual_predicate = _push_down_match_where_predicate(
            query.patterns,
            query.where_predicate,
        )
        if residual_predicate is not None:
            return None
        shape = _distinct_endpoint_range_shape(
            replace(query, patterns=pushed_patterns, where_predicate=None)
        )
        if shape is None:
            return None

        pattern, return_item = shape
        state = self._graph_service.load_tabular_state(include_edges=False)
        adjacency_loader = self._adjacency_loader_for_state(state)
        node_property_loader = self._node_property_loader_for_state(state)
        result_rows = _match_distinct_endpoint_range_table(
            state.node_table,
            pattern,
            return_item,
            self._tabular,
            adjacency_by_source_loader=adjacency_loader,
            node_property_loader=node_property_loader,
        )
        result_rows = self._apply_result_window(
            result_rows,
            order_by=query.order_by,
            skip=query.skip,
            limit=query.limit,
        )
        return _build_query_result_from_table(query.return_items, result_rows)

    def _execute_match_logical_plan(
        self, plan: MatchLogicalPlan, *, include_edges: bool = True
    ) -> MatchLogicalPlanResult:
        """Execute this query shape and return the result needed by the caller."""
        state = self._graph_service.load_tabular_state(include_edges=include_edges)
        adjacency_loader = self._adjacency_loader_for_state(state)
        edge_property_loader = self._edge_property_loader_for_state(state)
        node_property_loader = self._node_property_loader_for_state(state)
        if isinstance(plan.root, AggregateCountPlan):
            table = self._execute_logical_plan_node(
                plan.root.input,
                state=state,
                adjacency_loader=adjacency_loader,
                edge_property_loader=edge_property_loader,
                node_property_loader=node_property_loader,
            )
            return MatchLogicalPlanResult(
                table=None,
                query_result=_build_aggregate_query_result_from_table(
                    cast(tuple[ReturnItem, ...], plan.root.return_items),
                    table,
                ),
                planned_order_window=False,
                planned_distinct=False,
            )
        table = self._execute_logical_plan_node(
            plan.root,
            state=state,
            adjacency_loader=adjacency_loader,
            edge_property_loader=edge_property_loader,
            node_property_loader=node_property_loader,
        )
        return MatchLogicalPlanResult(
            table=table,
            query_result=None,
            planned_order_window=plan.planned_order_window,
            planned_distinct=plan.planned_distinct,
        )

    def _execute_logical_plan_node(
        self,
        plan: LogicalPlan,
        *,
        state: GraphTabularState,
        adjacency_loader: Callable[[str], pa.Table] | None,
        edge_property_loader: PropertyTableLoader | None,
        node_property_loader: PropertyTableLoader | None,
    ) -> pa.Table:
        """Execute this query shape and return the result needed by the caller."""
        if isinstance(plan, NodeScanPlan):
            pattern = NodePattern(
                variable=plan.variable,
                labels=plan.labels,
                properties=plan.properties,
            )
            scanned = _apply_node_pattern_filter(
                _node_binding_table(
                    state.node_table,
                    plan.variable,
                    requested_fields=plan.requested_fields,
                    pattern=pattern,
                    node_property_loader=node_property_loader,
                    pushdown_predicate=cast(WherePredicate | None, plan.pushdown_predicate),
                ),
                pattern,
            )
            return _apply_where_predicate_table(
                scanned,
                cast(WherePredicate | None, plan.pushdown_predicate),
                nodes=state.node_table,
                edges=state.edge_table,
                tabular=self._tabular,
                adjacency_by_source_loader=adjacency_loader,
                node_property_loader=node_property_loader,
                edge_property_loader=edge_property_loader,
            )
        if isinstance(plan, ExpandPlan):
            input_table = self._execute_logical_plan_node(
                plan.input,
                state=state,
                adjacency_loader=adjacency_loader,
                edge_property_loader=edge_property_loader,
                node_property_loader=node_property_loader,
            )
            step = RelationshipStep(
                variable=plan.edge_variable,
                rel_type=plan.rel_type,
                properties=plan.edge_properties,
                node=NodePattern(
                    variable=plan.end_variable,
                    labels=plan.end_labels,
                    properties=plan.end_properties,
                ),
                direction=plan.direction,
                rel_type_alternatives=plan.rel_type_alternatives,
            )
            edge_table = _edge_match_table(
                state.edge_table,
                plan.edge_variable,
                step,
                requested_fields=plan.edge_requested_fields,
                adjacency_by_source_loader=adjacency_loader,
                edge_property_loader=edge_property_loader,
                source_ids=input_table[_binding_column_name(plan.start_variable, _NODE_ID_FIELD)],
            )
            if not _uses_typed_adjacency_path(
                step, plan.edge_requested_fields
            ) or _has_relationship_type_alternatives(step):
                edge_table = _apply_edge_pattern_filter(edge_table, plan.edge_variable, step)
            edge_table = _apply_where_predicate_table(
                edge_table,
                cast(WherePredicate | None, plan.edge_pushdown_predicate),
                nodes=state.node_table,
                edges=state.edge_table,
                tabular=self._tabular,
                adjacency_by_source_loader=adjacency_loader,
                node_property_loader=node_property_loader,
                edge_property_loader=edge_property_loader,
            )
            current_edge_key = (
                f"{plan.edge_variable}.source"
                if plan.direction == "outgoing"
                else f"{plan.edge_variable}.target"
            )
            next_edge_key = (
                f"{plan.edge_variable}.target"
                if plan.direction == "outgoing"
                else f"{plan.edge_variable}.source"
            )
            matched = self._tabular.inner_join(
                input_table,
                edge_table,
                left_keys=_binding_column_name(plan.start_variable, _NODE_ID_FIELD),
                right_keys=current_edge_key,
            )
            matched = _ensure_edge_source_column(
                matched,
                edge_prefix=plan.edge_variable,
                source_variable=plan.start_variable,
                endpoint="source" if plan.direction == "outgoing" else "target",
            )
            matched = _filter_reused_relationships_in_fixed_path(
                matched,
                current_edge_prefix=plan.edge_variable,
                previous_edge_prefixes=plan.previous_edge_variables,
            )
            next_nodes = _node_binding_table(
                state.node_table,
                plan.end_variable,
                requested_fields=plan.end_requested_fields,
                pattern=step.node,
                node_property_loader=node_property_loader,
            )
            matched = self._tabular.inner_join(
                matched,
                next_nodes,
                left_keys=next_edge_key,
                right_keys=_binding_column_name(plan.end_variable, "__join_id"),
            )
            matched = _apply_node_pattern_filter(matched, step.node)
            return _apply_where_predicate_table(
                matched,
                cast(WherePredicate | None, plan.end_pushdown_predicate),
                nodes=state.node_table,
                edges=state.edge_table,
                tabular=self._tabular,
                adjacency_by_source_loader=adjacency_loader,
                node_property_loader=node_property_loader,
                edge_property_loader=edge_property_loader,
            )
        if isinstance(plan, FilterPlan):
            return _apply_where_predicate_table(
                self._execute_logical_plan_node(
                    plan.input,
                    state=state,
                    adjacency_loader=adjacency_loader,
                    edge_property_loader=edge_property_loader,
                    node_property_loader=node_property_loader,
                ),
                cast(WherePredicate, plan.predicate),
                nodes=state.node_table,
                edges=state.edge_table,
                tabular=self._tabular,
                adjacency_by_source_loader=adjacency_loader,
                node_property_loader=node_property_loader,
                edge_property_loader=edge_property_loader,
            )
        if isinstance(plan, OptionalMatchPlan):
            if plan.input is None:
                matched = _match_patterns_table(
                    state.node_table,
                    state.edge_table,
                    cast(tuple[CompiledPattern, ...], plan.patterns),
                    self._tabular,
                    required_fields=plan.required_fields,
                    adjacency_by_source_loader=adjacency_loader,
                    node_property_loader=node_property_loader,
                    edge_property_loader=edge_property_loader,
                )
                matched = _apply_where_predicate_table(
                    matched,
                    cast(WherePredicate | None, plan.where_predicate),
                    nodes=state.node_table,
                    edges=state.edge_table,
                    tabular=self._tabular,
                    adjacency_by_source_loader=adjacency_loader,
                    node_property_loader=node_property_loader,
                    edge_property_loader=edge_property_loader,
                )
                if matched.num_rows == 0 and plan.null_columns is not None:
                    matched = _build_optional_null_table(list(plan.null_columns))
                return matched
            input_table = self._execute_logical_plan_node(
                plan.input,
                state=state,
                adjacency_loader=adjacency_loader,
                edge_property_loader=edge_property_loader,
                node_property_loader=node_property_loader,
            )
            return _optional_match_patterns_from_input_table(
                input_table,
                state.node_table,
                state.edge_table,
                cast(tuple[CompiledPattern, ...], plan.patterns),
                cast(WherePredicate | None, plan.where_predicate),
                self._tabular,
                required_fields=plan.required_fields,
                adjacency_by_source_loader=adjacency_loader,
                node_property_loader=node_property_loader,
                edge_property_loader=edge_property_loader,
            )
        if isinstance(plan, WithAggregatePlan):
            input_table = self._execute_logical_plan_node(
                plan.input,
                state=state,
                adjacency_loader=adjacency_loader,
                edge_property_loader=edge_property_loader,
                node_property_loader=node_property_loader,
            )
            return _build_aggregate_table_from_table(
                cast(tuple[WithItem, ...], plan.grouping_items),
                cast(tuple[ReturnItem, ...], plan.aggregate_items),
                input_table,
            )
        if isinstance(plan, WithProjectPlan):
            input_table = self._execute_logical_plan_node(
                plan.input,
                state=state,
                adjacency_loader=adjacency_loader,
                edge_property_loader=edge_property_loader,
                node_property_loader=node_property_loader,
            )
            return _project_with_items_table(
                input_table,
                cast(tuple[WithItem, ...], plan.with_items),
            )
        if isinstance(plan, DistinctPlan):
            input_table = self._execute_logical_plan_node(
                plan.input,
                state=state,
                adjacency_loader=adjacency_loader,
                edge_property_loader=edge_property_loader,
                node_property_loader=node_property_loader,
            )
            return _apply_distinct_to_table(input_table)
        if isinstance(plan, ReturnProjectPlan):
            input_table = self._execute_logical_plan_node(
                plan.input,
                state=state,
                adjacency_loader=adjacency_loader,
                edge_property_loader=edge_property_loader,
                node_property_loader=node_property_loader,
            )
            return _project_return_items_table(
                input_table,
                cast(tuple[ReturnItem, ...], plan.return_items),
            )
        if isinstance(plan, ProjectPlan):
            input_table = self._execute_logical_plan_node(
                plan.input,
                state=state,
                adjacency_loader=adjacency_loader,
                edge_property_loader=edge_property_loader,
                node_property_loader=node_property_loader,
            )
            projected_columns = [
                column_name
                for column_name in plan.columns
                if column_name in input_table.column_names
            ]
            if not projected_columns:
                return input_table
            return input_table.select(projected_columns)
        if isinstance(plan, SortPlan):
            input_table = self._execute_logical_plan_node(
                plan.input,
                state=state,
                adjacency_loader=adjacency_loader,
                edge_property_loader=edge_property_loader,
                node_property_loader=node_property_loader,
            )
            return self._apply_order_by(input_table, cast(tuple[OrderByItem, ...], plan.order_by))
        if isinstance(plan, SlicePlan):
            input_table = self._execute_logical_plan_node(
                plan.input,
                state=state,
                adjacency_loader=adjacency_loader,
                edge_property_loader=edge_property_loader,
                node_property_loader=node_property_loader,
            )
            if plan.skip is not None:
                input_table = input_table.slice(plan.skip)
            if plan.limit is not None:
                input_table = input_table.slice(0, plan.limit)
            return input_table
        raise QueryError(f"Unsupported logical plan node: {type(plan)!r}")

    def _execute_optional_match(self, query: CompiledOptionalMatchQuery) -> QueryResult:
        """Execute this query shape and return the result needed by the caller."""
        logical_plan = (
            _build_optional_match_logical_plan(query) if _cypher_planner_enabled() else None
        )
        if logical_plan is not None:
            planned = self._execute_match_logical_plan(logical_plan)
            if planned.query_result is not None:
                return cast(QueryResult, planned.query_result)
            assert planned.table is not None
            return self._finalize_table_query_result(
                query.return_items,
                planned.table,
                distinct=False if planned.planned_distinct else query.distinct,
                order_by=None if planned.planned_order_window else query.order_by,
                skip=None if planned.planned_order_window else query.skip,
                limit=None if planned.planned_order_window else query.limit,
            )
        state = self._graph_service.load_tabular_state()
        adjacency_loader = self._adjacency_loader_for_state(state)
        node_property_loader = self._node_property_loader_for_state(state)
        edge_property_loader = self._edge_property_loader_for_state(state)
        required_fields = _required_fields_for_match(
            query.patterns,
            query.where_predicate,
            return_items=query.return_items,
            order_by=query.order_by,
        )
        matched_rows = _match_patterns_table(
            state.node_table,
            state.edge_table,
            query.patterns,
            self._tabular,
            required_fields=required_fields,
            adjacency_by_source_loader=adjacency_loader,
            node_property_loader=node_property_loader,
            edge_property_loader=edge_property_loader,
            where_pushdown_predicate=query.where_predicate,
        )
        matched_rows = _apply_where_predicate_table(
            matched_rows,
            query.where_predicate,
            nodes=state.node_table,
            edges=state.edge_table,
            tabular=self._tabular,
            adjacency_by_source_loader=adjacency_loader,
            node_property_loader=node_property_loader,
            edge_property_loader=edge_property_loader,
        )
        if matched_rows.num_rows == 0:
            matched_rows = _build_optional_null_table(matched_rows.column_names)
        return self._finalize_table_query_result(
            query.return_items,
            matched_rows,
            distinct=query.distinct,
            order_by=query.order_by,
            skip=query.skip,
            limit=query.limit,
        )

    def _execute_unwind(self, query: CompiledUnwindQuery) -> QueryResult:
        """Execute this query shape and return the result needed by the caller."""
        rows = _apply_unwind_to_input_table(
            pa.table({}),
            query.unwind,
        )
        return self._finalize_table_query_result(
            query.return_items,
            rows,
            distinct=query.distinct,
            order_by=query.order_by,
            skip=query.skip,
            limit=query.limit,
        )

    def _execute_match_optional(self, query: CompiledMatchOptionalQuery) -> QueryResult:
        """Execute this query shape and return the result needed by the caller."""
        logical_plan = (
            _build_match_optional_logical_plan(query) if _cypher_planner_enabled() else None
        )
        if logical_plan is not None:
            planned = self._execute_match_logical_plan(logical_plan)
            if planned.query_result is not None:
                return cast(QueryResult, planned.query_result)
            assert planned.table is not None
            return self._finalize_table_query_result(
                query.return_items,
                planned.table,
                distinct=False if planned.planned_distinct else query.distinct,
                order_by=None if planned.planned_order_window else query.order_by,
                skip=None if planned.planned_order_window else query.skip,
                limit=None if planned.planned_order_window else query.limit,
            )
        state = self._graph_service.load_tabular_state()
        adjacency_loader = self._adjacency_loader_for_state(state)
        node_property_loader = self._node_property_loader_for_state(state)
        edge_property_loader = self._edge_property_loader_for_state(state)
        match_required_fields = _required_fields_for_optional_match(
            query.match_patterns,
            query.match_where_predicate,
            query.optional_patterns,
            query.optional_where_predicate,
            return_items=query.return_items,
            order_by=query.order_by,
        )
        optional_required_fields = _required_fields_for_match(
            query.optional_patterns,
            query.optional_where_predicate,
            return_items=query.return_items,
            order_by=query.order_by,
        )
        matched_rows = _apply_where_predicate_table(
            _match_patterns_table(
                state.node_table,
                state.edge_table,
                query.match_patterns,
                self._tabular,
                required_fields=match_required_fields,
                adjacency_by_source_loader=adjacency_loader,
                node_property_loader=node_property_loader,
                edge_property_loader=edge_property_loader,
                where_pushdown_predicate=query.match_where_predicate,
            ),
            query.match_where_predicate,
            nodes=state.node_table,
            edges=state.edge_table,
            tabular=self._tabular,
            adjacency_by_source_loader=adjacency_loader,
            node_property_loader=node_property_loader,
            edge_property_loader=edge_property_loader,
        )
        result_rows = _optional_match_patterns_from_input_table(
            matched_rows,
            state.node_table,
            state.edge_table,
            query.optional_patterns,
            query.optional_where_predicate,
            self._tabular,
            required_fields=optional_required_fields,
            adjacency_by_source_loader=adjacency_loader,
            node_property_loader=node_property_loader,
            edge_property_loader=edge_property_loader,
        )
        return self._finalize_table_query_result(
            query.return_items,
            result_rows,
            distinct=query.distinct,
            order_by=query.order_by,
            skip=query.skip,
            limit=query.limit,
        )

    def _execute_match_create(self, query: CompiledMatchCreateQuery) -> QueryResult:
        """Execute this query shape and return the result needed by the caller."""
        state = self._graph_service.load_state()
        matched_rows = self._match_rows(
            query.match_patterns,
            query.where_predicate,
            return_items=query.return_items,
            order_by=query.order_by,
            extra_required_fields=_required_fields_for_create_bound_variables(
                query.create_patterns
            ),
        )

        new_nodes, new_edges, node_bindings = _materialize_match_create_records_from_matches(
            state,
            matched_rows,
            query.create_patterns,
        )
        self._graph_service.publish_records(
            previous_state=state,
            new_nodes=new_nodes,
            new_edges=new_edges,
        )
        if not query.return_items:
            return QueryResult(columns=(), rows=())
        required_fields: dict[str, set[str]] = {}
        _collect_required_fields_from_return_items(query.return_items, required_fields)
        _collect_required_fields_from_order_by(query.order_by, required_fields)
        result_rows = matched_rows
        for variable, records in node_bindings.items():
            result_rows = _append_node_binding_columns(
                result_rows,
                variable=variable,
                node_records=records,
                requested_fields=_requested_fields_for_variable(required_fields, variable),
                context="CREATE node",
            )
        return self._finalize_table_query_result(
            query.return_items,
            result_rows,
            distinct=query.distinct,
            order_by=query.order_by,
            skip=query.skip,
            limit=query.limit,
        )

    def _execute_match_merge(self, query: CompiledMatchMergeQuery) -> QueryResult:
        """Execute this query shape and return the result needed by the caller."""
        extra_required_fields = _required_fields_for_entity_variables(
            item.variable
            for item in (*query.on_create_set_items, *query.on_match_set_items)
            if item.variable != query.merge_pattern.variable
        )
        _merge_required_fields(
            extra_required_fields,
            _required_fields_for_merge_bound_nodes(query.merge_pattern),
        )
        matched_rows = self._match_rows(
            query.match_patterns,
            query.where_predicate,
            return_items=query.return_items,
            order_by=query.order_by,
            extra_required_fields=extra_required_fields,
        )
        matched_rows = _apply_bound_merge_node_filters(matched_rows, query.merge_pattern)
        state = self._graph_service.load_state()
        if not query.merge_pattern.left_bound or not query.merge_pattern.right_bound:
            return self._execute_match_merge_with_inline_node(query, state, matched_rows)
        created_row_mask, merge_edge_records, edges_to_create = _classify_merge_edges_from_matches(
            matched_rows,
            state.edges,
            query.merge_pattern,
        )
        new_edges: tuple[EdgeRecord, ...] = ()
        if edges_to_create:
            _, new_edges = self._graph_service.create_edges_batch(edges_to_create=edges_to_create)
        resolved_merge_edge_records = _resolve_created_merge_edge_records(
            merge_edge_records,
            new_edges,
        )
        result_rows = matched_rows
        if query.merge_pattern.variable is not None:
            merge_required_fields = _required_fields_for_match_merge_variable(query)
            result_rows = _append_merge_edge_binding_columns(
                matched_rows,
                variable=query.merge_pattern.variable,
                edge_records=resolved_merge_edge_records,
                requested_fields=_requested_fields_for_variable(
                    merge_required_fields,
                    query.merge_pattern.variable,
                ),
            )
        if query.on_create_set_items or query.on_match_set_items:
            indexed_rows = _append_row_index_column(matched_rows, "__merge_row_index")
            if query.merge_pattern.variable is not None:
                indexed_rows = _append_merge_edge_binding_columns(
                    indexed_rows,
                    variable=query.merge_pattern.variable,
                    edge_records=resolved_merge_edge_records,
                    requested_fields=_requested_fields_for_variable(
                        _required_fields_for_match_merge_variable(query),
                        query.merge_pattern.variable,
                    ),
                )
            created_rows = _filter_table_with_mask(indexed_rows, created_row_mask)
            matched_existing_rows = _filter_table_with_mask(
                indexed_rows,
                [not value for value in created_row_mask],
            )

            node_updates: list[tuple[int, str, object]] = []
            edge_updates: list[tuple[int, str, object]] = []
            updated_tables: list[pa.Table] = []

            if query.on_create_set_items and created_rows.num_rows > 0:
                created_node_updates, created_edge_updates = _materialize_set_updates_from_matches(
                    created_rows,
                    query.on_create_set_items,
                )
                node_updates.extend(created_node_updates)
                edge_updates.extend(created_edge_updates)
                updated_tables.append(
                    _apply_set_items_to_table(created_rows, query.on_create_set_items)
                )
            elif created_rows.num_rows > 0:
                updated_tables.append(created_rows)

            if query.on_match_set_items and matched_existing_rows.num_rows > 0:
                matched_node_updates, matched_edge_updates = _materialize_set_updates_from_matches(
                    matched_existing_rows,
                    query.on_match_set_items,
                )
                node_updates.extend(matched_node_updates)
                edge_updates.extend(matched_edge_updates)
                updated_tables.append(
                    _apply_set_items_to_table(matched_existing_rows, query.on_match_set_items)
                )
            elif matched_existing_rows.num_rows > 0:
                updated_tables.append(matched_existing_rows)

            if node_updates or edge_updates:
                self._graph_service.update_properties_batch(
                    node_updates=tuple(node_updates),
                    edge_updates=tuple(edge_updates),
                )
            if updated_tables:
                result_rows = self._tabular.sort_table(
                    self._tabular.concatenate_tables(updated_tables, schema=indexed_rows.schema),
                    [("__merge_row_index", "ascending")],
                )
            else:
                result_rows = indexed_rows
        if not query.return_items:
            return QueryResult(columns=(), rows=())
        return self._finalize_table_query_result(
            query.return_items,
            result_rows,
            distinct=query.distinct,
            order_by=query.order_by,
            skip=query.skip,
            limit=query.limit,
        )

    def _execute_match_merge_with_inline_node(
        self,
        query: CompiledMatchMergeQuery,
        state: GraphState,
        matched_rows: pa.Table,
    ) -> QueryResult:
        """Execute this query shape and return the result needed by the caller."""
        (
            created_row_mask,
            merge_node_records,
            merge_edge_records,
            new_nodes,
            new_edges,
        ) = _classify_match_merge_with_inline_node(
            matched_rows,
            state,
            query.merge_pattern,
        )
        if new_nodes or new_edges:
            self._graph_service.publish_records(
                previous_state=state,
                new_nodes=new_nodes,
                new_edges=new_edges,
            )

        result_rows = matched_rows
        merge_node_variable = _merge_inline_node_variable(query.merge_pattern)
        if merge_node_variable is not None:
            result_rows = _append_merge_node_binding_columns(
                result_rows,
                variable=merge_node_variable,
                node_records=merge_node_records,
                requested_fields=_requested_fields_for_variable(
                    _required_fields_for_match_merge_inline_node(query),
                    merge_node_variable,
                ),
            )
        if query.merge_pattern.variable is not None:
            result_rows = _append_merge_edge_binding_columns(
                result_rows,
                variable=query.merge_pattern.variable,
                edge_records=merge_edge_records,
                requested_fields=_requested_fields_for_variable(
                    _required_fields_for_match_merge_variable(query),
                    query.merge_pattern.variable,
                ),
            )

        if query.on_create_set_items or query.on_match_set_items:
            indexed_rows = _append_row_index_column(result_rows, "__merge_row_index")
            created_rows = _filter_table_with_mask(indexed_rows, created_row_mask)
            matched_existing_rows = _filter_table_with_mask(
                indexed_rows,
                [not value for value in created_row_mask],
            )

            node_updates: list[tuple[int, str, object]] = []
            edge_updates: list[tuple[int, str, object]] = []
            updated_tables: list[pa.Table] = []

            if query.on_create_set_items and created_rows.num_rows > 0:
                created_node_updates, created_edge_updates = _materialize_set_updates_from_matches(
                    created_rows,
                    query.on_create_set_items,
                )
                node_updates.extend(created_node_updates)
                edge_updates.extend(created_edge_updates)
                updated_tables.append(
                    _apply_set_items_to_table(created_rows, query.on_create_set_items)
                )
            elif created_rows.num_rows > 0:
                updated_tables.append(created_rows)

            if query.on_match_set_items and matched_existing_rows.num_rows > 0:
                matched_node_updates, matched_edge_updates = _materialize_set_updates_from_matches(
                    matched_existing_rows,
                    query.on_match_set_items,
                )
                node_updates.extend(matched_node_updates)
                edge_updates.extend(matched_edge_updates)
                updated_tables.append(
                    _apply_set_items_to_table(matched_existing_rows, query.on_match_set_items)
                )
            elif matched_existing_rows.num_rows > 0:
                updated_tables.append(matched_existing_rows)

            if node_updates or edge_updates:
                self._graph_service.update_properties_batch(
                    node_updates=tuple(node_updates),
                    edge_updates=tuple(edge_updates),
                )
            if updated_tables:
                result_rows = self._tabular.sort_table(
                    self._tabular.concatenate_tables(updated_tables, schema=indexed_rows.schema),
                    [("__merge_row_index", "ascending")],
                )
            else:
                result_rows = indexed_rows

        if not query.return_items:
            return QueryResult(columns=(), rows=())
        return self._finalize_table_query_result(
            query.return_items,
            result_rows,
            distinct=query.distinct,
            order_by=query.order_by,
            skip=query.skip,
            limit=query.limit,
        )

    def _execute_match_set(self, query: CompiledMatchSetQuery) -> QueryResult:
        """Execute this query shape and return the result needed by the caller."""
        matched_rows = self._match_rows(
            query.match_patterns,
            query.where_predicate,
            return_items=query.return_items,
            order_by=query.order_by,
            extra_required_fields=_required_fields_for_entity_variables(
                item.variable for item in query.set_items
            ),
        )

        node_updates, edge_updates = _materialize_set_updates_from_matches(
            matched_rows,
            query.set_items,
        )
        self._graph_service.update_properties_batch(
            node_updates=node_updates,
            edge_updates=edge_updates,
        )
        if not query.return_items:
            return QueryResult(columns=(), rows=())
        result_rows = _apply_set_items_to_table(matched_rows, query.set_items)
        return self._finalize_table_query_result(
            query.return_items,
            result_rows,
            distinct=query.distinct,
            order_by=query.order_by,
            skip=query.skip,
            limit=query.limit,
        )

    def _execute_match_delete(self, query: CompiledMatchDeleteQuery) -> QueryResult:
        """Execute this query shape and return the result needed by the caller."""
        matched_rows = self._match_rows(
            query.match_patterns,
            query.where_predicate,
            return_items=query.return_items,
            order_by=query.order_by,
            extra_required_fields=_required_fields_for_entity_variables(
                item.variable for item in query.delete_items
            ),
        )

        node_ids, edge_ids = _materialize_delete_targets_from_matches(
            matched_rows,
            query.delete_items,
        )
        if query.detach:
            edge_ids = _with_incident_edge_ids(
                node_ids=node_ids,
                edge_ids=edge_ids,
                edges=self._graph_service.load_state().edges,
            )
        self._graph_service.delete_entities_batch(node_ids=node_ids, edge_ids=edge_ids)
        if not query.return_items:
            return QueryResult(columns=(), rows=())
        return self._finalize_table_query_result(
            query.return_items,
            matched_rows,
            distinct=query.distinct,
            order_by=query.order_by,
            skip=query.skip,
            limit=query.limit,
        )

    def _execute_match_remove(self, query: CompiledMatchRemoveQuery) -> QueryResult:
        """Execute MATCH ... REMOVE by deleting matched properties from graph entities."""
        matched_rows = self._match_rows(
            query.match_patterns,
            query.where_predicate,
            return_items=query.return_items,
            order_by=query.order_by,
            extra_required_fields=_required_fields_for_entity_variables(
                item.variable for item in query.remove_items
            ),
        )

        (
            node_removals,
            edge_removals,
            node_label_removals,
        ) = _materialize_remove_targets_from_matches(matched_rows, query.remove_items)
        self._graph_service.remove_properties_batch(
            node_removals=node_removals,
            edge_removals=edge_removals,
        )
        self._graph_service.remove_labels_batch(node_label_removals=node_label_removals)
        if not query.return_items:
            return QueryResult(columns=(), rows=())
        result_rows = _apply_remove_items_to_table(matched_rows, query.remove_items)
        return self._finalize_table_query_result(
            query.return_items,
            result_rows,
            distinct=query.distinct,
            order_by=query.order_by,
            skip=query.skip,
            limit=query.limit,
        )

    def _execute_match_with(self, query: CompiledMatchWithQuery) -> QueryResult:
        """Execute this query shape and return the result needed by the caller."""
        logical_plan = (
            _build_match_with_logical_plan(query)
            if _cypher_planner_enabled() and not _match_with_query_has_exists_pattern(query)
            else None
        )
        if logical_plan is not None:
            planned = self._execute_match_logical_plan(logical_plan)
            if planned.query_result is not None:
                return cast(QueryResult, planned.query_result)
            assert planned.table is not None
            return self._finalize_table_query_result(
                query.return_items,
                planned.table,
                distinct=False if planned.planned_distinct else query.return_distinct,
                order_by=None if planned.planned_order_window else query.return_order_by,
                skip=None if planned.planned_order_window else query.return_skip,
                limit=None if planned.planned_order_window else query.return_limit,
            )
        state = self._graph_service.load_tabular_state()
        adjacency_loader = self._adjacency_loader_for_state(state)
        node_property_loader = self._node_property_loader_for_state(state)
        edge_property_loader = self._edge_property_loader_for_state(state)
        if query.pre_with_unwind is not None:
            matched_rows = _apply_unwind_to_input_table(
                _single_row_table(),
                query.pre_with_unwind,
            )
        elif query.match_optional:
            matched_rows = _match_patterns_table(
                state.node_table,
                state.edge_table,
                query.match_patterns,
                self._tabular,
                adjacency_by_source_loader=adjacency_loader,
                node_property_loader=node_property_loader,
                edge_property_loader=edge_property_loader,
            )
            matched_rows = _apply_where_predicate_table(
                matched_rows,
                query.match_where_predicate,
                nodes=state.node_table,
                edges=state.edge_table,
                tabular=self._tabular,
                adjacency_by_source_loader=adjacency_loader,
                node_property_loader=node_property_loader,
                edge_property_loader=edge_property_loader,
            )
            if matched_rows.num_rows == 0:
                matched_rows = _build_optional_null_table(
                    _pattern_null_columns(query.match_patterns)
                )
        else:
            matched_rows = _match_patterns_table(
                state.node_table,
                state.edge_table,
                query.match_patterns,
                self._tabular,
                adjacency_by_source_loader=adjacency_loader,
                node_property_loader=node_property_loader,
                edge_property_loader=edge_property_loader,
            )
            matched_rows = _apply_where_predicate_table(
                matched_rows,
                query.match_where_predicate,
                nodes=state.node_table,
                edges=state.edge_table,
                tabular=self._tabular,
                adjacency_by_source_loader=adjacency_loader,
                node_property_loader=node_property_loader,
                edge_property_loader=edge_property_loader,
            )

        if query.with_aggregate_items:
            with_rows = _build_aggregate_table_from_table(
                query.with_items,
                query.with_aggregate_items,
                matched_rows,
            )
        else:
            with_rows = _project_with_items_table(matched_rows, query.with_items)
        if query.with_distinct:
            with_rows = _apply_distinct_to_table(with_rows)
        with_rows = _apply_where_predicate_table(
            with_rows,
            query.with_where_predicate,
            nodes=state.node_table,
            edges=state.edge_table,
            tabular=self._tabular,
            adjacency_by_source_loader=adjacency_loader,
            node_property_loader=node_property_loader,
            edge_property_loader=edge_property_loader,
        )
        if query.with_order_by is not None:
            with_rows = self._apply_order_by(with_rows, query.with_order_by)
        if query.with_skip is not None:
            with_rows = with_rows.slice(query.with_skip)
        if query.with_limit is not None:
            with_rows = with_rows.slice(0, query.with_limit)

        result_input_rows = with_rows
        if query.post_with_create_patterns:
            result_input_rows = _materialize_create_patterns_from_input_rows(
                self._graph_service,
                with_rows,
                query.post_with_create_patterns,
                return_items=query.return_items,
                order_by=query.return_order_by,
            )
            if not query.return_items and not query.post_with_set_items:
                return QueryResult(columns=(), rows=())
        elif query.post_with_unwind is not None:
            result_input_rows = _apply_unwind_to_input_table(with_rows, query.post_with_unwind)
        elif query.post_with_match_patterns:
            if query.post_with_match_optional:
                result_input_rows = _optional_match_patterns_from_input_table(
                    with_rows,
                    state.node_table,
                    state.edge_table,
                    query.post_with_match_patterns,
                    query.post_with_match_where_predicate,
                    self._tabular,
                    required_fields=_required_fields_for_match(
                        query.post_with_match_patterns,
                        query.post_with_match_where_predicate,
                        return_items=query.return_items,
                        order_by=query.return_order_by,
                    ),
                    adjacency_by_source_loader=adjacency_loader,
                    node_property_loader=node_property_loader,
                    edge_property_loader=edge_property_loader,
                )
            else:
                required_fields = _required_fields_for_match(
                    query.post_with_match_patterns,
                    query.post_with_match_where_predicate,
                    return_items=query.return_items,
                    order_by=query.return_order_by,
                )
                if _can_use_input_row_node_match_fast_path(query.post_with_match_patterns):
                    result_input_rows = _match_node_patterns_from_input_rows(
                        with_rows,
                        self._graph_service.load_state().nodes,
                        query.post_with_match_patterns,
                        required_fields=required_fields,
                    )
                else:
                    result_input_rows = _match_patterns_from_input_table(
                        with_rows,
                        state.node_table,
                        state.edge_table,
                        query.post_with_match_patterns,
                        self._tabular,
                        required_fields=required_fields,
                        adjacency_by_source_loader=adjacency_loader,
                        node_property_loader=node_property_loader,
                        edge_property_loader=edge_property_loader,
                        where_pushdown_predicate=query.post_with_match_where_predicate,
                    )
                result_input_rows = _apply_where_predicate_table(
                    result_input_rows,
                    query.post_with_match_where_predicate,
                    nodes=state.node_table,
                    edges=state.edge_table,
                    tabular=self._tabular,
                    adjacency_by_source_loader=adjacency_loader,
                    node_property_loader=node_property_loader,
                    edge_property_loader=edge_property_loader,
                )

        if query.post_with_create_relationship_patterns:
            create_state = self._graph_service.load_state()
            new_nodes, new_edges, node_bindings = _materialize_match_create_records_from_matches(
                create_state,
                result_input_rows,
                query.post_with_create_relationship_patterns,
            )
            self._graph_service.publish_records(
                previous_state=create_state,
                new_nodes=new_nodes,
                new_edges=new_edges,
            )
            if not query.return_items and not query.post_with_set_items:
                return QueryResult(columns=(), rows=())
            create_required_fields: dict[str, set[str]] = {}
            _collect_required_fields_from_return_items(query.return_items, create_required_fields)
            _collect_required_fields_from_order_by(query.return_order_by, create_required_fields)
            for variable, records in node_bindings.items():
                result_input_rows = _append_node_binding_columns(
                    result_input_rows,
                    variable=variable,
                    node_records=records,
                    requested_fields=_requested_fields_for_variable(
                        create_required_fields,
                        variable,
                    ),
                    context="CREATE node",
                )

        if query.post_with_merge_node_pattern is not None:
            result_input_rows = _materialize_merge_node_from_input_rows(
                self._graph_service,
                result_input_rows,
                query.post_with_merge_node_pattern,
                on_create_set_items=query.post_with_merge_on_create_set_items,
                on_match_set_items=query.post_with_merge_on_match_set_items,
                return_items=query.return_items,
                order_by=query.return_order_by,
            )
            if not query.return_items and not query.post_with_set_items:
                return QueryResult(columns=(), rows=())

        if query.post_with_merge_relationship_pattern is not None:
            result_input_rows = _materialize_merge_relationship_from_input_rows(
                self._graph_service,
                result_input_rows,
                query.post_with_merge_relationship_pattern,
                on_create_set_items=query.post_with_merge_on_create_set_items,
                on_match_set_items=query.post_with_merge_on_match_set_items,
                return_items=query.return_items,
                order_by=query.return_order_by,
            )
            if not query.return_items and not query.post_with_set_items:
                return QueryResult(columns=(), rows=())

        if query.post_with_set_items:
            result_input_rows = _materialize_set_from_input_rows(
                self._graph_service,
                result_input_rows,
                query.post_with_set_items,
            )
            if not query.return_items:
                return QueryResult(columns=(), rows=())

        if query.post_with_delete_items:
            node_ids, edge_ids = _materialize_delete_targets_from_matches(
                result_input_rows,
                query.post_with_delete_items,
            )
            if query.post_with_delete_detach:
                edge_ids = _with_incident_edge_ids(
                    node_ids=node_ids,
                    edge_ids=edge_ids,
                    edges=self._graph_service.load_state().edges,
                )
            self._graph_service.delete_entities_batch(node_ids=node_ids, edge_ids=edge_ids)
            if not query.return_items:
                return QueryResult(columns=(), rows=())

        return self._finalize_table_query_result(
            query.return_items,
            result_input_rows,
            distinct=query.return_distinct,
            order_by=query.return_order_by,
            skip=query.return_skip,
            limit=query.return_limit,
        )

    def _match_rows(
        self,
        patterns: tuple[CompiledPattern, ...],
        where_predicate: WherePredicate | None,
        *,
        return_items: tuple[ReturnItem, ...] = (),
        order_by: tuple[OrderByItem, ...] | None = None,
        extra_required_fields: Mapping[str, set[str]] | None = None,
    ) -> pa.Table:
        """Produce binding rows for this pattern against the current graph state."""
        state = self._graph_service.load_tabular_state(
            include_edges=_match_requires_edges(patterns, where_predicate)
        )
        adjacency_loader = self._adjacency_loader_for_state(state)
        node_property_loader = self._node_property_loader_for_state(state)
        edge_property_loader = self._edge_property_loader_for_state(state)
        pushed_patterns, residual_predicate = _push_down_match_where_predicate(
            patterns,
            where_predicate,
        )
        required_fields = _required_fields_for_match(
            pushed_patterns,
            residual_predicate,
            return_items=return_items,
            order_by=order_by,
        )
        if extra_required_fields is not None:
            _merge_required_fields(required_fields, extra_required_fields)
        return _apply_where_predicate_table(
            _match_patterns_table(
                state.node_table,
                state.edge_table,
                pushed_patterns,
                self._tabular,
                required_fields=required_fields,
                adjacency_by_source_loader=adjacency_loader,
                node_property_loader=node_property_loader,
                edge_property_loader=edge_property_loader,
                where_pushdown_predicate=residual_predicate,
            ),
            residual_predicate,
            nodes=state.node_table,
            edges=state.edge_table,
            tabular=self._tabular,
            adjacency_by_source_loader=adjacency_loader,
            node_property_loader=node_property_loader,
            edge_property_loader=edge_property_loader,
        )

    def _finalize_table_query_result(
        self,
        return_items: tuple[ReturnItem, ...],
        rows: pa.Table,
        *,
        distinct: bool,
        order_by: tuple[OrderByItem, ...] | None,
        skip: int | None,
        limit: int | None,
    ) -> QueryResult:
        # Aggregate projections first collapse the matched rows into an Arrow table, then apply
        # ordering/windowing to that aggregate result instead of to the pre-aggregate matches.
        """Support the current Cypher compilation or execution step."""
        if _contains_aggregates(return_items):
            aggregate_items = _aggregate_return_items(return_items)
            grouping_items = _grouping_items_for_aggregate_return(return_items)
            if grouping_items or order_by is not None or skip is not None or limit is not None:
                aggregate_table = _build_aggregate_table_from_table(
                    grouping_items,
                    aggregate_items,
                    rows,
                )
                if distinct:
                    aggregate_table = _apply_distinct_to_table(aggregate_table)
                result_rows = self._apply_result_window(
                    aggregate_table,
                    order_by=order_by,
                    skip=skip,
                    limit=limit,
                )
                return _build_grouped_aggregate_query_result(return_items, result_rows)
            result = _build_aggregate_query_result_from_table(aggregate_items, rows)
            return _apply_distinct_to_query_result(result) if distinct else result

        if distinct and _can_apply_projected_distinct_window(return_items, order_by):
            projected_rows = _project_return_items_table(rows, return_items)
            projected_rows = _apply_distinct_to_table(projected_rows)
            projected_rows = self._apply_result_window(
                projected_rows,
                order_by=order_by,
                skip=skip,
                limit=limit,
            )
            return _build_query_result_from_table(return_items, projected_rows)

        result_rows = self._apply_result_window(rows, order_by=order_by, skip=skip, limit=limit)
        result = _build_query_result_from_table(return_items, result_rows)
        return _apply_distinct_to_query_result(result) if distinct else result

    def _apply_result_window(
        self,
        table: pa.Table,
        *,
        order_by: tuple[OrderByItem, ...] | None,
        skip: int | None,
        limit: int | None,
    ) -> pa.Table:
        """Apply this transformation or predicate inside the Cypher pipeline."""
        if order_by is not None:
            table = self._apply_order_by(table, order_by)
        if skip is not None:
            table = table.slice(skip)
        if limit is not None:
            table = table.slice(0, limit)
        return table

    def _apply_order_by(
        self,
        table: pa.Table,
        order_by_items: tuple[OrderByItem, ...],
    ) -> pa.Table:
        """Apply this transformation or predicate inside the Cypher pipeline."""
        sort_keys: list[tuple[str, str]] = []
        sorted_table = table
        for index, item in enumerate(order_by_items):
            sorted_table, order_column = _ensure_order_column(
                sorted_table,
                variable=item.variable,
                field=item.field,
                index=index,
            )
            sort_keys.append(
                (
                    order_column,
                    "descending" if item.descending else "ascending",
                )
            )
        return self._tabular.sort_table(sorted_table, sort_keys)


def _parse_drop_index_statement(query: str) -> _ParsedDropIndexStatement | None:
    """Parse common Neo4j/Memgraph DROP INDEX forms outside the Cypher grammar."""

    legacy = re.fullmatch(
        r"drop\s+index\s+on\s+:(?P<label>[A-Za-z_][A-Za-z0-9_]*)"
        r"\s*\(\s*(?P<property>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
        query,
        flags=re.IGNORECASE,
    )
    if legacy is not None:
        label = legacy.group("label")
        property_name = legacy.group("property")
        return _ParsedDropIndexStatement(
            name=f"{label}_{property_name}_idx",
            if_exists=True,
        )

    modern = re.fullmatch(
        r"drop\s+index\s+(?P<name>[A-Za-z_][A-Za-z0-9_]*)"
        r"(?:\s+(?P<if_exists>if\s+exists))?",
        query,
        flags=re.IGNORECASE,
    )
    if modern is None:
        return None
    return _ParsedDropIndexStatement(
        name=modern.group("name"),
        if_exists=modern.group("if_exists") is not None,
    )


def _parse_create_index_statement(query: str) -> _ParsedIndexDeclaration | None:
    """Parse common Neo4j/Memgraph CREATE INDEX forms outside the Cypher grammar."""

    legacy = re.fullmatch(
        r"create\s+index\s+on\s+:(?P<label>[A-Za-z_][A-Za-z0-9_]*)"
        r"\s*\(\s*(?P<property>[A-Za-z_][A-Za-z0-9_]*)\s*\)",
        query,
        flags=re.IGNORECASE,
    )
    if legacy is not None:
        label = legacy.group("label")
        property_name = legacy.group("property")
        return _ParsedIndexDeclaration(
            name=f"{label}_{property_name}_idx",
            kind="range",
            owner_kind="label",
            owner_name=label,
            property_names=(property_name,),
            if_not_exists=True,
        )

    modern = re.fullmatch(
        r"create\s+(?P<kind>text\s+)?index\s+(?P<name>[A-Za-z_][A-Za-z0-9_]*)?"
        r"\s*(?P<if_not_exists>if\s+not\s+exists\s+)?"
        r"for\s*\(\s*(?P<variable>[A-Za-z_][A-Za-z0-9_]*)"
        r"\s*:\s*(?P<label>[A-Za-z_][A-Za-z0-9_]*)\s*\)"
        r"\s*on\s*\(\s*(?P<properties>.+)\s*\)",
        query,
        flags=re.IGNORECASE,
    )
    if modern is None:
        return None

    variable = modern.group("variable")
    owner_name = modern.group("label")
    property_names = _parse_index_property_list(
        modern.group("properties"),
        variable=variable,
    )
    kind: IndexKind = "text" if modern.group("kind") else "range"
    name = modern.group("name") or f"{owner_name}_{'_'.join(property_names)}_{kind}_idx"
    return _ParsedIndexDeclaration(
        name=name,
        kind=kind,
        owner_kind="label",
        owner_name=owner_name,
        property_names=property_names,
        if_not_exists=modern.group("if_not_exists") is not None,
    )


def _parse_index_property_list(properties_text: str, *, variable: str) -> tuple[str, ...]:
    """Extract ``n.property`` items from a CREATE INDEX ON clause."""

    properties: list[str] = []
    for raw_item in _split_top_level_items(properties_text):
        item = raw_item.strip()
        match = re.fullmatch(
            rf"{re.escape(variable)}\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)",
            item,
            flags=re.IGNORECASE,
        )
        if match is None:
            raise QueryError(f"Unsupported CREATE INDEX property expression: {item}")
        properties.append(match.group(1))
    if not properties:
        raise QueryError("CREATE INDEX requires at least one property.")
    return tuple(properties)


def _normalize_compatibility_statement(query: str) -> str:
    """Normalize client metadata statements that live outside the openCypher parser."""
    return " ".join(query.strip().rstrip(";").split())


def _execute_neo4j_compatibility_procedure(query: str) -> QueryResult | None:
    """Return static metadata for Neo4j client discovery procedures."""
    normalized = _normalize_compatibility_statement(query)
    lower = normalized.lower()
    if lower.startswith("call db.ping"):
        return QueryResult(columns=(), rows=())

    if not lower.startswith("call dbms."):
        return None

    if lower.startswith("call dbms.components"):
        row: dict[str, Any] = {
            "name": "Neo4j Kernel",
            "versions": ["5.26.0"],
            "version": "5.26.0",
            "edition": "community",
        }
        return _compatibility_query_result(
            query,
            row,
            default_columns=("name", "versions", "edition"),
        )

    if lower.startswith("call dbms.protocols"):
        row = {
            "name": "bolt",
            "address": "0.0.0.0:7687",
            "versions": ["5.8"],
        }
        return _compatibility_query_result(
            query,
            row,
            default_columns=("name", "address", "versions"),
        )

    if lower.startswith("call dbms.showcurrentuser"):
        row = {
            "username": "neo4j",
            "roles": ["PUBLIC"],
            "flags": [],
        }
        return _compatibility_query_result(
            query,
            row,
            default_columns=("username", "roles", "flags"),
        )

    if lower.startswith("call dbms.procedures"):
        return QueryResult(
            columns=("name", "signature", "description", "mode"),
            rows=(
                {
                    "name": "db.labels",
                    "signature": "db.labels() :: (label :: STRING)",
                    "description": "Lists node labels.",
                    "mode": "READ",
                },
                {
                    "name": "db.relationshipTypes",
                    "signature": "db.relationshipTypes() :: (relationshipType :: STRING)",
                    "description": "Lists relationship types.",
                    "mode": "READ",
                },
                {
                    "name": "db.propertyKeys",
                    "signature": "db.propertyKeys() :: (propertyKey :: STRING)",
                    "description": "Lists property keys.",
                    "mode": "READ",
                },
                {
                    "name": "db.schema.nodeTypeProperties",
                    "signature": (
                        "db.schema.nodeTypeProperties() :: "
                        "(nodeType :: STRING, nodeLabels :: LIST, "
                        "propertyName :: STRING, propertyTypes :: LIST, mandatory :: BOOLEAN)"
                    ),
                    "description": "Lists node label property metadata.",
                    "mode": "READ",
                },
                {
                    "name": "db.schema.relTypeProperties",
                    "signature": (
                        "db.schema.relTypeProperties() :: "
                        "(relType :: STRING, propertyName :: STRING, "
                        "propertyTypes :: LIST, mandatory :: BOOLEAN)"
                    ),
                    "description": "Lists relationship type property metadata.",
                    "mode": "READ",
                },
                {
                    "name": "db.schema.visualization",
                    "signature": (
                        "db.schema.visualization() :: (nodes :: LIST, relationships :: LIST)"
                    ),
                    "description": "Returns a lightweight graph schema visualization.",
                    "mode": "READ",
                },
            ),
        )

    return None


def _execute_show_compatibility_statement(
    query: str,
    state: GraphState,
) -> QueryResult | None:
    """Return lightweight Memgraph SHOW metadata used by Memgraph Lab."""
    normalized = _normalize_compatibility_statement(query)
    lower = normalized.lower()
    if not lower.startswith("show "):
        return None

    if lower in {"show storage info", "show storage info on current database"}:
        return QueryResult(
            columns=("name", "value"),
            rows=(
                {"name": "vertex_count", "value": len(state.nodes)},
                {"name": "edge_count", "value": len(state.edges)},
                {"name": "database_name", "value": "grapharrow"},
            ),
        )

    if lower == "show replication role":
        return QueryResult(columns=("role",), rows=({"role": "main"},))

    if lower == "show license info":
        return QueryResult(
            columns=("license_type", "organization_name", "valid_until"),
            rows=(
                {
                    "license_type": "community",
                    "organization_name": "GraphArrow",
                    "valid_until": None,
                },
            ),
        )

    if lower == "show index info":
        return QueryResult(
            columns=("index_type", "label", "property", "count", "identifier"),
            rows=tuple(_show_index_info_rows(state)),
        )

    if lower == "show constraint info":
        return QueryResult(
            columns=("constraint_type", "label", "properties"),
            rows=(),
        )

    if lower == "show triggers":
        return QueryResult(
            columns=("name", "event_type", "phase", "statement", "owner"),
            rows=(),
        )

    if lower == "show schema info":
        return QueryResult(
            columns=("nodes", "relationships"),
            rows=(),
        )

    return QueryResult(columns=("name", "value"), rows=())


def _execute_schema_compatibility_procedure(
    query: str,
    state: GraphState,
) -> QueryResult | None:
    """Return schema metadata used by graph exploration clients."""
    normalized = _normalize_compatibility_statement(query)
    lower = normalized.lower()
    if lower.startswith("call db.labels"):
        labels = sorted({label for node in state.nodes for label in node.labels})
        return _compatibility_rows_query_result(
            query,
            ({"label": label} for label in labels),
            default_columns=("label",),
        )

    if lower.startswith("call db.relationshiptypes"):
        relationship_types = sorted({edge.rel_type for edge in state.edges})
        return _compatibility_rows_query_result(
            query,
            ({"relationshipType": rel_type} for rel_type in relationship_types),
            default_columns=("relationshipType",),
        )

    if lower.startswith("call db.propertykeys"):
        property_keys: set[str] = set()
        for node in state.nodes:
            property_keys.update(key for key, value in node.properties.items() if value is not None)
        for edge in state.edges:
            property_keys.update(key for key, value in edge.properties.items() if value is not None)
        return _compatibility_rows_query_result(
            query,
            ({"propertyKey": key} for key in sorted(property_keys)),
            default_columns=("propertyKey",),
        )

    if lower.startswith("call db.schema.nodetypeproperties"):
        return _compatibility_rows_query_result(
            query,
            _schema_node_type_property_rows(state),
            default_columns=(
                "nodeType",
                "nodeLabels",
                "propertyName",
                "propertyTypes",
                "mandatory",
            ),
        )

    if lower.startswith("call db.schema.reltypeproperties") or lower.startswith(
        "call db.schema.relationshiptypeproperties"
    ):
        return _compatibility_rows_query_result(
            query,
            _schema_relationship_type_property_rows(state),
            default_columns=("relType", "propertyName", "propertyTypes", "mandatory"),
        )

    if not lower.startswith("call db.schema.visualization"):
        return None

    labels = sorted({label for node in state.nodes for label in node.labels})
    label_node_ids = {label: index for index, label in enumerate(labels)}
    schema_nodes = [
        {
            "id": node_id,
            "labels": [label],
            "properties": {"name": label},
        }
        for label, node_id in label_node_ids.items()
    ]

    nodes_by_id = {node.node_id: node for node in state.nodes}
    schema_relationships: list[dict[str, Any]] = []
    seen_relationships: set[tuple[str, str, str]] = set()
    for edge in state.edges:
        source = nodes_by_id.get(edge.src_id)
        target = nodes_by_id.get(edge.dst_id)
        if source is None or target is None:
            continue
        for source_label in source.labels:
            for target_label in target.labels:
                key = (source_label, edge.rel_type, target_label)
                if key in seen_relationships:
                    continue
                seen_relationships.add(key)
                schema_relationships.append(
                    {
                        "id": len(schema_relationships),
                        "start": label_node_ids[source_label],
                        "end": label_node_ids[target_label],
                        "type": edge.rel_type,
                        "properties": {},
                    }
                )

    row = {"nodes": schema_nodes, "relationships": schema_relationships}
    return _compatibility_query_result(
        query,
        row,
        default_columns=("nodes", "relationships"),
    )


def _is_schema_compatibility_procedure(normalized_lower_query: str) -> bool:
    """Return true when a compatibility procedure needs graph schema state."""
    return normalized_lower_query.startswith(
        (
            "call db.labels",
            "call db.relationshiptypes",
            "call db.propertykeys",
            "call db.schema.nodetypeproperties",
            "call db.schema.reltypeproperties",
            "call db.schema.relationshiptypeproperties",
            "call db.schema.visualization",
        )
    )


def _show_index_info_rows(state: GraphState) -> tuple[dict[str, Any], ...]:
    """Build Memgraph-compatible rows from logical catalog index declarations."""

    rows: list[dict[str, Any]] = []
    for index in state.catalog.indexes:
        if index.owner_kind == "label":
            count = sum(1 for node in state.nodes if index.owner_name in node.labels)
            label = index.owner_name
        else:
            count = sum(1 for edge in state.edges if edge.rel_type == index.owner_name)
            label = index.owner_name
        rows.append(
            {
                "index_type": index.kind,
                "label": label,
                "property": ",".join(index.property_names),
                "count": count,
                "identifier": index.name,
            }
        )
    return tuple(rows)


def _schema_node_type_property_rows(state: GraphState) -> tuple[dict[str, Any], ...]:
    """Build Neo4j-compatible node property metadata rows."""
    labels = sorted({label for node in state.nodes for label in node.labels})
    rows: list[dict[str, Any]] = []
    for label in labels:
        label_nodes = [node for node in state.nodes if label in node.labels]
        property_names = sorted({key for node in label_nodes for key in node.properties})
        for property_name in property_names:
            values = [node.properties.get(property_name) for node in label_nodes]
            rows.append(
                {
                    "nodeType": f":`{label}`",
                    "nodeLabels": [label],
                    "propertyName": property_name,
                    "propertyTypes": _schema_property_types(values),
                    "mandatory": all(value is not None for value in values),
                }
            )
    return tuple(rows)


def _schema_relationship_type_property_rows(state: GraphState) -> tuple[dict[str, Any], ...]:
    """Build Neo4j-compatible relationship property metadata rows."""
    rel_types = sorted({edge.rel_type for edge in state.edges})
    rows: list[dict[str, Any]] = []
    for rel_type in rel_types:
        rel_edges = [edge for edge in state.edges if edge.rel_type == rel_type]
        property_names = sorted({key for edge in rel_edges for key in edge.properties})
        for property_name in property_names:
            values = [edge.properties.get(property_name) for edge in rel_edges]
            rows.append(
                {
                    "relType": f":`{rel_type}`",
                    "propertyName": property_name,
                    "propertyTypes": _schema_property_types(values),
                    "mandatory": all(value is not None for value in values),
                }
            )
    return tuple(rows)


def _schema_property_types(values: Iterable[object]) -> list[str]:
    """Return coarse Neo4j-style property type names for schema metadata."""
    type_names = {_schema_property_type_name(value) for value in values if value is not None}
    return sorted(type_names) or ["NULL"]


def _schema_property_type_name(value: object) -> str:
    """Map Python property values to coarse Neo4j-style schema type names."""
    if isinstance(value, bool):
        return "Boolean"
    if isinstance(value, int):
        return "Integer"
    if isinstance(value, float):
        return "Float"
    if isinstance(value, str):
        return "String"
    if isinstance(value, list):
        return "List"
    if isinstance(value, Mapping):
        return "Map"
    return type(value).__name__


def _compatibility_query_result(
    query: str,
    row: Mapping[str, Any],
    *,
    default_columns: tuple[str, ...],
) -> QueryResult:
    """Project a static compatibility row through a simple RETURN list when present."""
    columns = _compatibility_return_columns(query) or default_columns
    return QueryResult(
        columns=columns,
        rows=(dict(zip(columns, (row.get(column) for column in columns), strict=True)),),
    )


def _compatibility_rows_query_result(
    query: str,
    rows: Iterable[Mapping[str, Any]],
    *,
    default_columns: tuple[str, ...],
) -> QueryResult:
    """Project multiple compatibility metadata rows through a simple RETURN list."""
    columns = _compatibility_return_columns(query) or default_columns
    return QueryResult(
        columns=columns,
        rows=tuple({column: row.get(column) for column in columns} for row in rows),
    )


def _compatibility_return_columns(query: str) -> tuple[str, ...]:
    """Extract simple identifiers from a RETURN list used by client metadata probes."""
    match = re.search(
        r"(?is)\bRETURN\b\s+(.+?)(?:\s+\bORDER\s+BY\b|\s+\bSKIP\b|\s+\bLIMIT\b|$)",
        query,
    )
    if match is None:
        return ()

    columns: list[str] = []
    for raw_item in match.group(1).split(","):
        item = raw_item.strip()
        alias_match = re.search(r"(?is)\s+AS\s+([A-Za-z_][A-Za-z0-9_]*)$", item)
        if alias_match is not None:
            columns.append(alias_match.group(1))
            continue
        item = re.sub(r"(?is)^.*\s+AS\s+", "", item).strip()
        if _IDENTIFIER_RE.fullmatch(item):
            columns.append(item)
    return tuple(columns)


def _execute_standalone_return(
    query: str,
    params: dict[str, object],
) -> QueryResult | None:
    """Evaluate simple literal RETURN clauses used by clients as connectivity probes."""
    match = re.fullmatch(r"(?is)RETURN\s+(.+)", query)
    if match is None:
        return None

    columns: list[str] = []
    row: dict[str, Any] = {}
    for raw_item in match.group(1).split(","):
        item = raw_item.strip()
        alias_match = re.fullmatch(
            r"(?is)(.+?)\s+AS\s+([A-Za-z_][A-Za-z0-9_]*)",
            item,
        )
        expression = alias_match.group(1).strip() if alias_match is not None else item
        column = alias_match.group(2) if alias_match is not None else expression
        value = _parse_value(expression, params)
        columns.append(column)
        row[column] = value
    return QueryResult(columns=tuple(columns), rows=(row,))


def _compile_query(
    tree: CypherParser.OC_CypherContext,
    params: dict[str, object],
) -> CompiledQuery:
    """Compile this Cypher fragment into the internal structures used later."""
    statement = tree.oC_Statement()
    if statement is None:
        raise QueryError("Expected a Cypher statement.")

    query_ctx = statement.oC_Query()
    if query_ctx is None:
        raise QueryError("Expected a Cypher query.")

    regular_query = query_ctx.oC_RegularQuery()
    if regular_query is None:
        raise QueryError("Standalone procedure calls are not supported yet.")
    if regular_query.oC_Union():
        raise QueryError("UNION queries are not supported yet.")

    single_query = regular_query.oC_SingleQuery()
    if single_query is None:
        raise QueryError("Expected a single query.")
    multi_part = single_query.oC_MultiPartQuery()
    if multi_part is not None:
        return _compile_multi_part_query(multi_part, params)

    single_part = single_query.oC_SinglePartQuery()
    if single_part is None:
        raise QueryError("Expected a single-part query.")

    reading_clauses = cast(
        list[CypherParser.OC_ReadingClauseContext],
        single_part.oC_ReadingClause(),
    )
    updating_clauses = cast(
        list[CypherParser.OC_UpdatingClauseContext],
        single_part.oC_UpdatingClause(),
    )

    if len(updating_clauses) > 1:
        raise QueryError("Multiple updating clauses are not supported yet.")

    # The compiler keeps a shared anonymous-variable counter so synthetic bindings
    # created while walking the official openCypher parse tree stay stable across
    # MATCH and CREATE parts of the same query.
    compiler_state = _CompilerState()
    return_ctx = single_part.oC_Return()

    if updating_clauses:
        if len(reading_clauses) > 1:
            raise QueryError("Only a single MATCH clause is supported before updating clauses.")
        updating_clause = updating_clauses[0]
        create_ctx = updating_clause.oC_Create()
        merge_ctx = updating_clause.oC_Merge()
        set_ctx = updating_clause.oC_Set()
        delete_ctx = updating_clause.oC_Delete()
        remove_ctx = updating_clause.oC_Remove()
        if (
            create_ctx is None
            and merge_ctx is None
            and set_ctx is None
            and delete_ctx is None
            and remove_ctx is None
        ):
            raise QueryError(
                "Only CREATE, MERGE, SET, DELETE, REMOVE, and MATCH are supported by the "
                "bootstrap engine."
            )
        if reading_clauses:
            match_ctx = reading_clauses[0].oC_Match()
            if match_ctx is None:
                raise QueryError(
                    "Only MATCH with CREATE, MERGE, SET, DELETE, or REMOVE is supported in "
                    "mixed queries."
                )
            if create_ctx is not None:
                return _compile_match_create_query(
                    match_ctx,
                    create_ctx,
                    return_ctx,
                    params,
                    compiler_state,
                )
            if merge_ctx is not None:
                return _compile_match_merge_query(
                    match_ctx,
                    merge_ctx,
                    return_ctx,
                    params,
                    compiler_state,
                )
            if set_ctx is not None:
                return _compile_match_set_query(
                    match_ctx,
                    set_ctx,
                    return_ctx,
                    params,
                    compiler_state,
                )
            if delete_ctx is not None:
                return _compile_match_delete_query(
                    match_ctx,
                    delete_ctx,
                    return_ctx,
                    params,
                    compiler_state,
                )
            if remove_ctx is not None:
                return _compile_match_remove_query(
                    match_ctx,
                    remove_ctx,
                    return_ctx,
                    params,
                    compiler_state,
                )
            raise QueryError("Unsupported updating clause.")
        if set_ctx is not None or delete_ctx is not None or remove_ctx is not None:
            raise QueryError("SET, DELETE, and REMOVE currently require a MATCH clause.")
        if merge_ctx is not None:
            return _compile_merge_query(merge_ctx, return_ctx, params, compiler_state)
        return _compile_create_query(create_ctx, return_ctx, params, compiler_state)

    if reading_clauses:
        if return_ctx is None:
            raise QueryError("Reading queries require a RETURN clause.")
        if len(reading_clauses) == 1:
            match_ctx = reading_clauses[0].oC_Match()
            if match_ctx is not None:
                if match_ctx.OPTIONAL() is not None:
                    return _compile_optional_match_query(
                        match_ctx,
                        return_ctx,
                        params,
                        compiler_state,
                    )
                return _compile_match_query(match_ctx, return_ctx, params, compiler_state)
            unwind_ctx = reading_clauses[0].oC_Unwind()
            if unwind_ctx is not None:
                return _compile_unwind_query(unwind_ctx, return_ctx, params)
            raise QueryError(
                "Only CREATE, MERGE, SET, DELETE, REMOVE, MATCH, and UNWIND are supported by the "
                "bootstrap engine."
            )
        if len(reading_clauses) == 2:
            first_match_ctx = reading_clauses[0].oC_Match()
            second_match_ctx = reading_clauses[1].oC_Match()
            if first_match_ctx is None or second_match_ctx is None:
                raise QueryError("Only MATCH and OPTIONAL MATCH are supported in reading clauses.")
            if first_match_ctx.OPTIONAL() is not None:
                raise QueryError("OPTIONAL MATCH cannot be the first clause in a two-MATCH query.")
            if second_match_ctx.OPTIONAL() is None:
                return _compile_multi_match_query(
                    reading_clauses,
                    return_ctx,
                    params,
                    compiler_state,
                )
            return _compile_match_optional_query(
                first_match_ctx,
                second_match_ctx,
                return_ctx,
                params,
                compiler_state,
            )
        return _compile_multi_match_query(
            reading_clauses,
            return_ctx,
            params,
            compiler_state,
        )

    raise QueryError(
        "Only CREATE, MERGE, SET, DELETE, REMOVE, and MATCH are supported by the bootstrap engine."
    )


def _compile_create_query(
    create_ctx: CypherParser.OC_CreateContext,
    return_ctx: CypherParser.OC_ReturnContext | None,
    params: dict[str, object],
    state: _CompilerState,
) -> CompiledCreateQuery:
    """Compile this Cypher fragment into the internal structures used later."""
    patterns = _compile_create_patterns(create_ctx.oC_Pattern(), params, state)
    if return_ctx is not None:
        return_items, distinct = _compile_return_items(
            return_ctx.oC_ProjectionBody(),
            scope=_scope_from_patterns(patterns),
        )
    else:
        return_items, distinct = (), False
    return CompiledCreateQuery(
        patterns=patterns,
        return_items=return_items,
        distinct=distinct,
    )


def _compile_merge_query(
    merge_ctx: CypherParser.OC_MergeContext,
    return_ctx: CypherParser.OC_ReturnContext | None,
    params: dict[str, object],
    state: _CompilerState,
) -> CompiledMergeQuery:
    """Compile this Cypher fragment into the internal structures used later."""
    pattern = _compile_merge_node_pattern(merge_ctx, params, state)
    on_create_set_items, on_match_set_items = _compile_merge_actions(
        merge_ctx,
        params,
        bound_variables={pattern.variable},
    )
    if return_ctx is not None:
        scope: dict[str, ScopeBindingKind] = {pattern.variable: "entity"}
        return_items, distinct = _compile_return_items(return_ctx.oC_ProjectionBody(), scope=scope)
    else:
        return_items, distinct = (), False
    return CompiledMergeQuery(
        pattern=pattern,
        on_create_set_items=on_create_set_items,
        on_match_set_items=on_match_set_items,
        return_items=return_items,
        distinct=distinct,
    )


def _compile_multi_part_query(
    multi_part_ctx: CypherParser.OC_MultiPartQueryContext,
    params: dict[str, object],
) -> CompiledMatchWithQuery:
    """Compile this Cypher fragment into the internal structures used later."""
    with_clauses = cast(list[CypherParser.OC_WithContext], multi_part_ctx.oC_With())
    if len(with_clauses) != 1:
        raise QueryError("Only a single WITH clause is supported per multi-part query.")

    updating_clauses = cast(
        list[CypherParser.OC_UpdatingClauseContext],
        multi_part_ctx.oC_UpdatingClause(),
    )
    if updating_clauses:
        raise QueryError("Updating clauses inside multi-part queries are not supported yet.")

    reading_clauses = cast(
        list[CypherParser.OC_ReadingClauseContext],
        multi_part_ctx.oC_ReadingClause(),
    )
    if len(reading_clauses) != 1:
        raise QueryError("A single MATCH or UNWIND clause is required before WITH for now.")
    match_ctx = reading_clauses[0].oC_Match()
    pre_with_unwind_ctx = reading_clauses[0].oC_Unwind()
    if match_ctx is None and pre_with_unwind_ctx is None:
        raise QueryError("Only MATCH/UNWIND ... WITH ... RETURN is supported for now.")

    single_part = multi_part_ctx.oC_SinglePartQuery()
    if single_part is None:
        raise QueryError("A final RETURN clause is required after WITH.")
    final_updating_clauses = cast(
        list[CypherParser.OC_UpdatingClauseContext],
        single_part.oC_UpdatingClause(),
    )
    if len(final_updating_clauses) > 2:
        raise QueryError("Only up to two updating clauses are supported after WITH for now.")
    final_reading_clauses = cast(
        list[CypherParser.OC_ReadingClauseContext],
        single_part.oC_ReadingClause(),
    )
    if len(final_reading_clauses) > 1:
        raise QueryError("Only a single MATCH clause is supported after WITH for now.")
    post_with_match_patterns: tuple[CompiledPattern, ...] = ()
    post_with_match_where_predicate: WherePredicate | None = None
    post_with_match_optional = False
    post_with_unwind: CompiledUnwind | None = None
    post_with_create_patterns: tuple[CompiledPattern, ...] = ()
    post_with_create_relationship_patterns: tuple[CreateRelationshipPattern, ...] = ()
    post_with_merge_node_pattern: MergeNodePattern | None = None
    post_with_merge_relationship_pattern: MergeRelationshipPattern | None = None
    post_with_merge_on_create_set_items: tuple[SetItem, ...] = ()
    post_with_merge_on_match_set_items: tuple[SetItem, ...] = ()
    post_with_set_items: tuple[SetItem, ...] = ()
    post_with_delete_detach = False
    post_with_delete_items: tuple[DeleteItem, ...] = ()
    return_ctx = single_part.oC_Return()
    if return_ctx is None and not final_updating_clauses:
        raise QueryError("A final RETURN clause is required after WITH.")

    compiler_state = _CompilerState()
    match_patterns: tuple[CompiledPattern, ...] = ()
    match_where_predicate: WherePredicate | None = None
    match_optional = False
    pre_with_unwind: CompiledUnwind | None = None
    if match_ctx is not None:
        match_optional = match_ctx.OPTIONAL() is not None
        match_patterns = _compile_match_patterns(match_ctx.oC_Pattern(), params, compiler_state)
        input_scope = _scope_from_patterns(match_patterns)
        match_where_predicate = (
            _compile_where(match_ctx.oC_Where(), params, input_scope)
            if match_ctx.oC_Where() is not None
            else None
        )
    else:
        assert pre_with_unwind_ctx is not None
        pre_with_unwind = _compile_unwind(pre_with_unwind_ctx, params, scope=None)
        input_scope = {pre_with_unwind.output_name: "scalar"}

    with_ctx = with_clauses[0]
    with_projection_body = with_ctx.oC_ProjectionBody()
    with_items, with_aggregate_items, with_distinct, with_scope = _compile_with_items(
        with_projection_body,
        input_scope,
    )
    with_where_predicate = (
        _compile_where(with_ctx.oC_Where(), params, with_scope)
        if with_ctx.oC_Where() is not None
        else None
    )
    final_scope = dict(with_scope)
    if final_reading_clauses:
        final_reading_clause = final_reading_clauses[0]
        final_match_ctx = final_reading_clause.oC_Match()
        if final_match_ctx is not None:
            post_with_match_optional = final_match_ctx.OPTIONAL() is not None
            post_with_match_patterns = _compile_match_patterns(
                final_match_ctx.oC_Pattern(),
                params,
                compiler_state,
            )
            final_scope = _extend_scope_with_patterns(
                with_scope,
                post_with_match_patterns,
            )
            post_with_match_where_predicate = (
                _compile_where(final_match_ctx.oC_Where(), params, final_scope)
                if final_match_ctx.oC_Where() is not None
                else None
            )
        else:
            final_unwind_ctx = final_reading_clause.oC_Unwind()
            if final_unwind_ctx is None:
                raise QueryError("Only MATCH and UNWIND are supported after WITH for now.")
            post_with_unwind = _compile_unwind(final_unwind_ctx, params, scope=with_scope)
            final_scope = dict(with_scope)
            final_scope[post_with_unwind.output_name] = "scalar"
    if final_updating_clauses:
        create_ctx = final_updating_clauses[0].oC_Create()
        merge_ctx = final_updating_clauses[0].oC_Merge()
        set_ctx = final_updating_clauses[0].oC_Set()
        delete_ctx = final_updating_clauses[0].oC_Delete()
        if create_ctx is not None:
            if final_reading_clauses:
                post_with_create_relationship_patterns = _compile_create_relationship_patterns(
                    create_ctx.oC_Pattern(),
                    params,
                    compiler_state,
                    bound_variables=set(final_scope),
                )
                final_scope = _extend_scope_with_create_patterns(
                    final_scope,
                    post_with_create_relationship_patterns,
                )
            else:
                post_with_create_patterns = _compile_create_patterns(
                    create_ctx.oC_Pattern(),
                    params,
                    compiler_state,
                )
                final_scope = _extend_scope_with_patterns(final_scope, post_with_create_patterns)
        elif merge_ctx is not None:
            if final_reading_clauses:
                post_with_merge_relationship_pattern = _compile_merge_relationship_pattern(
                    merge_ctx,
                    params,
                    compiler_state,
                    bound_variables=set(final_scope),
                )
                merge_action_bound_variables = set(final_scope)
                if post_with_merge_relationship_pattern.variable is not None:
                    merge_action_bound_variables.add(post_with_merge_relationship_pattern.variable)
                    final_scope[post_with_merge_relationship_pattern.variable] = "entity"
                post_with_merge_on_create_set_items, post_with_merge_on_match_set_items = (
                    _compile_merge_actions(
                        merge_ctx,
                        params,
                        bound_variables=merge_action_bound_variables,
                    )
                )
            else:
                post_with_merge_node_pattern = _compile_merge_node_pattern(
                    merge_ctx,
                    params,
                    compiler_state,
                )
                final_scope[post_with_merge_node_pattern.variable] = "entity"
                post_with_merge_on_create_set_items, post_with_merge_on_match_set_items = (
                    _compile_merge_actions(
                        merge_ctx,
                        params,
                        bound_variables={post_with_merge_node_pattern.variable},
                    )
                )
        elif set_ctx is not None:
            post_with_set_items = _compile_set_items(
                set_ctx,
                params,
                bound_variables={
                    variable for variable, kind in final_scope.items() if kind == "entity"
                },
            )
        elif delete_ctx is not None:
            post_with_delete_detach, post_with_delete_items = _compile_delete_items(
                delete_ctx,
                bound_variables={
                    variable for variable, kind in final_scope.items() if kind == "entity"
                },
            )
        else:
            clause_name = _updating_clause_name(final_updating_clauses[0])
            raise QueryError(
                "Only CREATE, MERGE, SET, and DELETE are supported after WITH for now. "
                f"Got {clause_name}."
            )
        if len(final_updating_clauses) == 2:
            trailing_set_ctx = final_updating_clauses[1].oC_Set()
            if trailing_set_ctx is None:
                raise QueryError("Only trailing SET is supported after CREATE/MERGE for now.")
            post_with_set_items = _compile_set_items(
                trailing_set_ctx,
                params,
                bound_variables={
                    variable for variable, kind in final_scope.items() if kind == "entity"
                },
            )
    if return_ctx is not None:
        return_projection_body = return_ctx.oC_ProjectionBody()
        return_items, return_distinct = _compile_return_items(
            return_projection_body,
            scope=final_scope,
        )
        _validate_return_items(return_items)
        return_order_by = _compile_order_by(return_projection_body.oC_Order(), scope=final_scope)
        return_skip = _compile_optional_int(return_projection_body.oC_Skip(), params)
        return_limit = _compile_optional_int(return_projection_body.oC_Limit(), params)
    else:
        return_items, return_distinct = (), False
        return_order_by, return_skip, return_limit = None, None, None
    return CompiledMatchWithQuery(
        match_patterns=match_patterns,
        match_where_predicate=match_where_predicate,
        match_optional=match_optional,
        pre_with_unwind=pre_with_unwind,
        with_items=with_items,
        with_aggregate_items=with_aggregate_items,
        with_where_predicate=with_where_predicate,
        with_distinct=with_distinct,
        with_order_by=_compile_order_by(with_projection_body.oC_Order(), scope=with_scope),
        with_skip=_compile_optional_int(with_projection_body.oC_Skip(), params),
        with_limit=_compile_optional_int(with_projection_body.oC_Limit(), params),
        post_with_match_patterns=post_with_match_patterns,
        post_with_match_where_predicate=post_with_match_where_predicate,
        post_with_match_optional=post_with_match_optional,
        post_with_unwind=post_with_unwind,
        post_with_create_patterns=post_with_create_patterns,
        post_with_create_relationship_patterns=post_with_create_relationship_patterns,
        post_with_merge_node_pattern=post_with_merge_node_pattern,
        post_with_merge_relationship_pattern=post_with_merge_relationship_pattern,
        post_with_merge_on_create_set_items=post_with_merge_on_create_set_items,
        post_with_merge_on_match_set_items=post_with_merge_on_match_set_items,
        post_with_set_items=post_with_set_items,
        post_with_delete_detach=post_with_delete_detach,
        post_with_delete_items=post_with_delete_items,
        return_items=return_items,
        return_distinct=return_distinct,
        return_order_by=return_order_by,
        return_skip=return_skip,
        return_limit=return_limit,
    )


def _compile_match_query(
    match_ctx: CypherParser.OC_MatchContext,
    return_ctx: CypherParser.OC_ReturnContext,
    params: dict[str, object],
    state: _CompilerState,
) -> CompiledMatchQuery:
    """Compile this Cypher fragment into the internal structures used later."""
    if match_ctx.OPTIONAL() is not None:
        raise QueryError("OPTIONAL MATCH is not supported yet.")

    patterns = _compile_match_patterns(match_ctx.oC_Pattern(), params, state)
    scope = _scope_from_patterns(patterns)
    where_predicate = (
        _compile_where(match_ctx.oC_Where(), params, scope)
        if match_ctx.oC_Where() is not None
        else None
    )
    projection_body = return_ctx.oC_ProjectionBody()
    return_items, distinct = _compile_return_items(projection_body, scope=scope)
    _validate_return_items(return_items)
    return CompiledMatchQuery(
        patterns=patterns,
        where_predicate=where_predicate,
        return_items=return_items,
        distinct=distinct,
        order_by=_compile_order_by(
            projection_body.oC_Order(),
            scope=scope,
            return_items=return_items,
        ),
        skip=_compile_optional_int(projection_body.oC_Skip(), params),
        limit=_compile_optional_int(projection_body.oC_Limit(), params),
    )


def _compile_multi_match_query(
    reading_clauses: list[CypherParser.OC_ReadingClauseContext],
    return_ctx: CypherParser.OC_ReturnContext,
    params: dict[str, object],
    state: _CompilerState,
) -> CompiledMatchQuery:
    """Compile multiple connected MATCH clauses into one joined match pipeline."""
    patterns: list[CompiledPattern] = []
    where_predicates: list[WherePredicate] = []
    for reading_clause in reading_clauses:
        match_ctx = reading_clause.oC_Match()
        if match_ctx is None:
            raise QueryError("Only MATCH clauses are supported in multi-MATCH queries.")
        if match_ctx.OPTIONAL() is not None:
            raise QueryError("OPTIONAL MATCH is supported only as MATCH ... OPTIONAL MATCH.")
        patterns.extend(_compile_match_patterns(match_ctx.oC_Pattern(), params, state))
        if match_ctx.oC_Where() is not None:
            where_predicates.append(
                _compile_where(match_ctx.oC_Where(), params, _scope_from_patterns(tuple(patterns)))
            )

    compiled_patterns = _fuse_linear_match_patterns(tuple(patterns))
    scope = _scope_from_patterns(compiled_patterns)
    where_predicate = _combine_and_predicates(tuple(where_predicates))
    projection_body = return_ctx.oC_ProjectionBody()
    return_items, distinct = _compile_return_items(projection_body, scope=scope)
    _validate_return_items(return_items)
    return CompiledMatchQuery(
        patterns=compiled_patterns,
        where_predicate=where_predicate,
        return_items=return_items,
        distinct=distinct,
        order_by=_compile_order_by(
            projection_body.oC_Order(),
            scope=scope,
            return_items=return_items,
        ),
        skip=_compile_optional_int(projection_body.oC_Skip(), params),
        limit=_compile_optional_int(projection_body.oC_Limit(), params),
    )


def _fuse_linear_match_patterns(
    patterns: tuple[CompiledPattern, ...],
) -> tuple[CompiledPattern, ...]:
    """Fuse connected multi-MATCH path fragments into one path when semantics are unchanged."""
    if len(patterns) <= 1:
        return patterns

    node_constraints = {
        pattern.variable: pattern for pattern in patterns if isinstance(pattern, NodePattern)
    }
    constrained_patterns: list[CompiledPattern] = []
    consumed_node_constraints: set[str] = set()
    for pattern in patterns:
        if isinstance(pattern, NodePattern):
            continue
        constrained, consumed = _path_with_node_constraints(pattern, node_constraints)
        consumed_node_constraints.update(consumed)
        constrained_patterns.append(constrained)

    for pattern in patterns:
        if isinstance(pattern, NodePattern) and pattern.variable not in consumed_node_constraints:
            constrained_patterns.append(pattern)

    if not constrained_patterns:
        return patterns

    fused: list[CompiledPattern] = []
    for pattern in constrained_patterns:
        if not fused:
            fused.append(pattern)
            continue
        previous = fused[-1]
        merged = _try_append_path_pattern(previous, pattern)
        if merged is None:
            fused.append(pattern)
            continue
        fused[-1] = merged
    return tuple(fused)


def _path_with_node_constraints(
    pattern: PathPattern,
    node_constraints: Mapping[str, NodePattern],
) -> tuple[PathPattern, set[str]]:
    """Apply standalone MATCH node constraints to a connected path fragment."""
    consumed: set[str] = set()
    start, start_consumed = _merge_optional_node_constraint(
        pattern.start,
        node_constraints.get(pattern.start.variable),
    )
    if start_consumed:
        consumed.add(pattern.start.variable)
    steps: list[RelationshipStep] = []
    for step in pattern.steps:
        node, node_consumed = _merge_optional_node_constraint(
            step.node,
            node_constraints.get(step.node.variable),
        )
        if node_consumed:
            consumed.add(step.node.variable)
        steps.append(replace(step, node=node))
    return replace(pattern, start=start, steps=tuple(steps)), consumed


def _merge_optional_node_constraint(
    node: NodePattern,
    constraint: NodePattern | None,
) -> tuple[NodePattern, bool]:
    """Merge compatible labels and inline properties for the same node variable."""
    if constraint is None:
        return node, False
    merged = _merge_node_patterns(node, constraint)
    return (node, False) if merged is None else (merged, True)


def _try_append_path_pattern(
    left: CompiledPattern,
    right: CompiledPattern,
) -> PathPattern | None:
    """Append ``right`` to ``left`` when they form one linear path."""
    if not isinstance(left, PathPattern) or not isinstance(right, PathPattern):
        return None
    if not left.steps:
        return None
    left_tail = left.steps[-1].node
    if left_tail.variable != right.start.variable:
        return None
    merged_tail = _merge_node_patterns(left_tail, right.start)
    if merged_tail is None:
        return None
    left_steps = [*left.steps[:-1], replace(left.steps[-1], node=merged_tail)]
    return PathPattern(start=left.start, steps=tuple([*left_steps, *right.steps]))


def _merge_node_patterns(left: NodePattern, right: NodePattern) -> NodePattern | None:
    """Merge constraints for the same node variable, or return None on conflicts."""
    if left.variable != right.variable:
        return None
    labels = tuple(dict.fromkeys((*left.labels, *right.labels)))
    properties = dict(left.properties)
    for key, value in right.properties.items():
        if key in properties and properties[key] != value:
            return None
        properties[key] = value
    return replace(left, labels=labels, properties=properties)


def _compile_optional_match_query(
    match_ctx: CypherParser.OC_MatchContext,
    return_ctx: CypherParser.OC_ReturnContext,
    params: dict[str, object],
    state: _CompilerState,
) -> CompiledOptionalMatchQuery:
    """Compile this Cypher fragment into the internal structures used later."""
    patterns = _compile_match_patterns(match_ctx.oC_Pattern(), params, state)
    scope = _scope_from_patterns(patterns)
    where_predicate = (
        _compile_where(match_ctx.oC_Where(), params, scope)
        if match_ctx.oC_Where() is not None
        else None
    )
    projection_body = return_ctx.oC_ProjectionBody()
    return_items, distinct = _compile_return_items(projection_body, scope=scope)
    _validate_return_items(return_items)
    return CompiledOptionalMatchQuery(
        patterns=patterns,
        where_predicate=where_predicate,
        return_items=return_items,
        distinct=distinct,
        order_by=_compile_order_by(
            projection_body.oC_Order(), scope=scope, return_items=return_items
        ),
        skip=_compile_optional_int(projection_body.oC_Skip(), params),
        limit=_compile_optional_int(projection_body.oC_Limit(), params),
    )


def _compile_unwind_query(
    unwind_ctx: CypherParser.OC_UnwindContext,
    return_ctx: CypherParser.OC_ReturnContext,
    params: dict[str, object],
) -> CompiledUnwindQuery:
    """Compile this Cypher fragment into the internal structures used later."""
    unwind = _compile_unwind(unwind_ctx, params, scope=None)
    projection_body = return_ctx.oC_ProjectionBody()
    return_scope: dict[str, ScopeBindingKind] = {unwind.output_name: "scalar"}
    return_items, distinct = _compile_return_items(projection_body, scope=return_scope)
    _validate_return_items(return_items)
    return CompiledUnwindQuery(
        unwind=unwind,
        return_items=return_items,
        distinct=distinct,
        order_by=_compile_order_by(projection_body.oC_Order(), scope=return_scope),
        skip=_compile_optional_int(projection_body.oC_Skip(), params),
        limit=_compile_optional_int(projection_body.oC_Limit(), params),
    )


def _compile_match_optional_query(
    match_ctx: CypherParser.OC_MatchContext,
    optional_match_ctx: CypherParser.OC_MatchContext,
    return_ctx: CypherParser.OC_ReturnContext,
    params: dict[str, object],
    state: _CompilerState,
) -> CompiledMatchOptionalQuery:
    """Compile this Cypher fragment into the internal structures used later."""
    match_patterns = _compile_match_patterns(match_ctx.oC_Pattern(), params, state)
    match_scope = _scope_from_patterns(match_patterns)
    match_where_predicate = (
        _compile_where(match_ctx.oC_Where(), params, match_scope)
        if match_ctx.oC_Where() is not None
        else None
    )

    optional_patterns = _compile_match_patterns(optional_match_ctx.oC_Pattern(), params, state)
    final_scope = _extend_scope_with_patterns(match_scope, optional_patterns)
    optional_where_predicate = (
        _compile_where(optional_match_ctx.oC_Where(), params, final_scope)
        if optional_match_ctx.oC_Where() is not None
        else None
    )

    projection_body = return_ctx.oC_ProjectionBody()
    return_items, distinct = _compile_return_items(projection_body, scope=final_scope)
    _validate_return_items(return_items)
    return CompiledMatchOptionalQuery(
        match_patterns=match_patterns,
        match_where_predicate=match_where_predicate,
        optional_patterns=optional_patterns,
        optional_where_predicate=optional_where_predicate,
        return_items=return_items,
        distinct=distinct,
        order_by=_compile_order_by(
            projection_body.oC_Order(), scope=final_scope, return_items=return_items
        ),
        skip=_compile_optional_int(projection_body.oC_Skip(), params),
        limit=_compile_optional_int(projection_body.oC_Limit(), params),
    )


def _compile_match_create_query(
    match_ctx: CypherParser.OC_MatchContext,
    create_ctx: CypherParser.OC_CreateContext,
    return_ctx: CypherParser.OC_ReturnContext | None,
    params: dict[str, object],
    state: _CompilerState,
) -> CompiledMatchCreateQuery:
    """Compile this Cypher fragment into the internal structures used later."""
    if match_ctx.OPTIONAL() is not None:
        raise QueryError("OPTIONAL MATCH is not supported yet.")

    match_patterns = _compile_match_patterns(match_ctx.oC_Pattern(), params, state)
    bound_variables = {
        variable for pattern in match_patterns for variable in _pattern_variables(pattern)
    }
    scope = _scope_from_patterns(match_patterns)
    create_patterns = _compile_create_relationship_patterns(
        create_ctx.oC_Pattern(),
        params,
        state,
        bound_variables=bound_variables,
    )
    where_predicate = (
        _compile_where(match_ctx.oC_Where(), params, scope)
        if match_ctx.oC_Where() is not None
        else None
    )
    projection_scope = _extend_scope_with_create_patterns(scope, create_patterns)
    return_items, distinct, order_by, skip, limit = _compile_optional_return_projection(
        return_ctx,
        params,
        scope=projection_scope,
    )
    _validate_return_items(return_items)
    return CompiledMatchCreateQuery(
        match_patterns=match_patterns,
        where_predicate=where_predicate,
        create_patterns=create_patterns,
        return_items=return_items,
        distinct=distinct,
        order_by=order_by,
        skip=skip,
        limit=limit,
    )


def _compile_match_merge_query(
    match_ctx: CypherParser.OC_MatchContext,
    merge_ctx: CypherParser.OC_MergeContext,
    return_ctx: CypherParser.OC_ReturnContext | None,
    params: dict[str, object],
    state: _CompilerState,
) -> CompiledMatchMergeQuery:
    """Compile this Cypher fragment into the internal structures used later."""
    if match_ctx.OPTIONAL() is not None:
        raise QueryError("OPTIONAL MATCH is not supported yet.")

    match_patterns = _compile_match_patterns(match_ctx.oC_Pattern(), params, state)
    bound_variables = {
        variable for pattern in match_patterns for variable in _pattern_variables(pattern)
    }
    scope = _scope_from_patterns(match_patterns)
    merge_pattern = _compile_merge_relationship_pattern(
        merge_ctx,
        params,
        state,
        bound_variables=bound_variables,
    )
    merge_scope = dict(scope)
    merge_action_bound_variables = set(bound_variables)
    if merge_pattern.variable is not None:
        merge_scope[merge_pattern.variable] = "entity"
        merge_action_bound_variables.add(merge_pattern.variable)
    if not merge_pattern.left_bound and not _is_internal_variable(merge_pattern.left_variable):
        merge_scope[merge_pattern.left_variable] = "entity"
        merge_action_bound_variables.add(merge_pattern.left_variable)
    if not merge_pattern.right_bound and not _is_internal_variable(merge_pattern.right_variable):
        merge_scope[merge_pattern.right_variable] = "entity"
        merge_action_bound_variables.add(merge_pattern.right_variable)
    on_create_set_items, on_match_set_items = _compile_merge_actions(
        merge_ctx,
        params,
        bound_variables=merge_action_bound_variables,
    )
    where_predicate = (
        _compile_where(match_ctx.oC_Where(), params, scope)
        if match_ctx.oC_Where() is not None
        else None
    )
    return_items, distinct, order_by, skip, limit = _compile_optional_return_projection(
        return_ctx,
        params,
        scope=merge_scope,
    )
    _validate_return_items(return_items)
    return CompiledMatchMergeQuery(
        match_patterns=match_patterns,
        where_predicate=where_predicate,
        merge_pattern=merge_pattern,
        on_create_set_items=on_create_set_items,
        on_match_set_items=on_match_set_items,
        return_items=return_items,
        distinct=distinct,
        order_by=order_by,
        skip=skip,
        limit=limit,
    )


def _compile_match_set_query(
    match_ctx: CypherParser.OC_MatchContext,
    set_ctx: CypherParser.OC_SetContext,
    return_ctx: CypherParser.OC_ReturnContext | None,
    params: dict[str, object],
    state: _CompilerState,
) -> CompiledMatchSetQuery:
    """Compile this Cypher fragment into the internal structures used later."""
    if match_ctx.OPTIONAL() is not None:
        raise QueryError("OPTIONAL MATCH is not supported yet.")

    match_patterns = _compile_match_patterns(match_ctx.oC_Pattern(), params, state)
    bound_variables = {
        variable for pattern in match_patterns for variable in _pattern_public_variables(pattern)
    }
    scope = _scope_from_patterns(match_patterns)
    set_items = _compile_set_items(set_ctx, params, bound_variables=bound_variables)
    where_predicate = (
        _compile_where(match_ctx.oC_Where(), params, scope)
        if match_ctx.oC_Where() is not None
        else None
    )
    return_items, distinct, order_by, skip, limit = _compile_optional_return_projection(
        return_ctx,
        params,
        scope=scope,
    )
    _validate_return_items(return_items)
    return CompiledMatchSetQuery(
        match_patterns=match_patterns,
        where_predicate=where_predicate,
        set_items=set_items,
        return_items=return_items,
        distinct=distinct,
        order_by=order_by,
        skip=skip,
        limit=limit,
    )


def _compile_match_delete_query(
    match_ctx: CypherParser.OC_MatchContext,
    delete_ctx: CypherParser.OC_DeleteContext,
    return_ctx: CypherParser.OC_ReturnContext | None,
    params: dict[str, object],
    state: _CompilerState,
) -> CompiledMatchDeleteQuery:
    """Compile this Cypher fragment into the internal structures used later."""
    if match_ctx.OPTIONAL() is not None:
        raise QueryError("OPTIONAL MATCH is not supported yet.")

    match_patterns = _compile_match_patterns(match_ctx.oC_Pattern(), params, state)
    bound_variables = {
        variable for pattern in match_patterns for variable in _pattern_public_variables(pattern)
    }
    scope = _scope_from_patterns(match_patterns)
    detach, delete_items = _compile_delete_items(delete_ctx, bound_variables=bound_variables)
    where_predicate = (
        _compile_where(match_ctx.oC_Where(), params, scope)
        if match_ctx.oC_Where() is not None
        else None
    )
    return_items, distinct, order_by, skip, limit = _compile_optional_return_projection(
        return_ctx,
        params,
        scope=scope,
    )
    _validate_return_items(return_items)
    return CompiledMatchDeleteQuery(
        match_patterns=match_patterns,
        where_predicate=where_predicate,
        detach=detach,
        delete_items=delete_items,
        return_items=return_items,
        distinct=distinct,
        order_by=order_by,
        skip=skip,
        limit=limit,
    )


def _compile_match_remove_query(
    match_ctx: CypherParser.OC_MatchContext,
    remove_ctx: CypherParser.OC_RemoveContext,
    return_ctx: CypherParser.OC_ReturnContext | None,
    params: dict[str, object],
    state: _CompilerState,
) -> CompiledMatchRemoveQuery:
    """Compile MATCH ... REMOVE for dataset loaders and simple write workflows."""
    if match_ctx.OPTIONAL() is not None:
        raise QueryError("OPTIONAL MATCH is not supported yet.")

    match_patterns = _compile_match_patterns(match_ctx.oC_Pattern(), params, state)
    bound_variables = {
        variable for pattern in match_patterns for variable in _pattern_public_variables(pattern)
    }
    scope = _scope_from_patterns(match_patterns)
    remove_items = _compile_remove_items(remove_ctx, bound_variables=bound_variables)
    where_predicate = (
        _compile_where(match_ctx.oC_Where(), params, scope)
        if match_ctx.oC_Where() is not None
        else None
    )
    return_items, distinct, order_by, skip, limit = _compile_optional_return_projection(
        return_ctx,
        params,
        scope=scope,
    )
    _validate_return_items(return_items)
    return CompiledMatchRemoveQuery(
        match_patterns=match_patterns,
        where_predicate=where_predicate,
        remove_items=remove_items,
        return_items=return_items,
        distinct=distinct,
        order_by=order_by,
        skip=skip,
        limit=limit,
    )


def _compile_optional_return_projection(
    return_ctx: CypherParser.OC_ReturnContext | None,
    params: dict[str, object],
    *,
    scope: Mapping[str, ScopeBindingKind],
) -> tuple[
    tuple[ReturnItem, ...],
    bool,
    tuple[OrderByItem, ...] | None,
    int | None,
    int | None,
]:
    """Compile RETURN metadata, or an empty projection for write-only queries."""
    if return_ctx is None:
        return (), False, None, None, None
    projection_body = return_ctx.oC_ProjectionBody()
    return_items, distinct = _compile_return_items(projection_body, scope=scope)
    return (
        return_items,
        distinct,
        _compile_order_by(projection_body.oC_Order(), scope=scope),
        _compile_optional_int(projection_body.oC_Skip(), params),
        _compile_optional_int(projection_body.oC_Limit(), params),
    )


def _compile_pattern(
    pattern_ctx: CypherParser.OC_PatternContext | None,
    params: dict[str, object],
    state: _CompilerState,
) -> NodePattern | PathPattern:
    """Compile this Cypher fragment into the internal structures used later."""
    if pattern_ctx is None:
        raise QueryError("Pattern clause is required.")

    pattern_parts = cast(list[CypherParser.OC_PatternPartContext], pattern_ctx.oC_PatternPart())
    if len(pattern_parts) != 1:
        raise QueryError("Multiple pattern parts are not supported yet.")

    part_ctx = pattern_parts[0]
    if part_ctx.oC_Variable() is not None:
        raise QueryError("Named pattern parts are not supported yet.")

    anonymous = part_ctx.oC_AnonymousPatternPart()
    if anonymous is None:
        raise QueryError("Expected an anonymous pattern part.")

    return _compile_pattern_element(
        anonymous.oC_PatternElement(),
        params,
        state,
        allow_relationship_variables=True,
    )


def _compile_match_patterns(
    pattern_ctx: CypherParser.OC_PatternContext | None,
    params: dict[str, object],
    state: _CompilerState,
) -> tuple[CompiledPattern, ...]:
    """Compile this Cypher fragment into the internal structures used later."""
    if pattern_ctx is None:
        raise QueryError("Pattern clause is required.")

    pattern_parts = cast(list[CypherParser.OC_PatternPartContext], pattern_ctx.oC_PatternPart())
    if not pattern_parts:
        raise QueryError("Pattern clause is required.")

    compiled_parts: list[CompiledPattern] = []
    for part_ctx in pattern_parts:
        if part_ctx.oC_Variable() is not None:
            raise QueryError("Named pattern parts are not supported yet.")

        anonymous = part_ctx.oC_AnonymousPatternPart()
        if anonymous is None:
            raise QueryError("Expected an anonymous pattern part.")
        compiled_parts.append(
            _compile_pattern_element(
                anonymous.oC_PatternElement(),
                params,
                state,
                allow_relationship_variables=True,
            )
        )
    return tuple(compiled_parts)


def _compile_create_patterns(
    pattern_ctx: CypherParser.OC_PatternContext | None,
    params: dict[str, object],
    state: _CompilerState,
) -> tuple[CompiledPattern, ...]:
    """Compile this Cypher fragment into the internal structures used later."""
    if pattern_ctx is None:
        raise QueryError("CREATE pattern clause is required.")

    pattern_parts = cast(list[CypherParser.OC_PatternPartContext], pattern_ctx.oC_PatternPart())
    if not pattern_parts:
        raise QueryError("CREATE pattern clause is required.")

    compiled_parts: list[CompiledPattern] = []
    for part_ctx in pattern_parts:
        if part_ctx.oC_Variable() is not None:
            raise QueryError("Named pattern parts are not supported yet.")

        anonymous = part_ctx.oC_AnonymousPatternPart()
        if anonymous is None:
            raise QueryError("Expected an anonymous pattern part.")
        compiled_parts.append(
            _compile_pattern_element(
                anonymous.oC_PatternElement(),
                params,
                state,
                allow_relationship_variables=False,
            )
        )
    return tuple(compiled_parts)


def _compile_create_relationship_patterns(
    pattern_ctx: CypherParser.OC_PatternContext | None,
    params: dict[str, object],
    state: _CompilerState,
    *,
    bound_variables: set[str],
) -> tuple[CreateRelationshipPattern, ...]:
    """Compile this Cypher fragment into the internal structures used later."""
    if pattern_ctx is None:
        raise QueryError("CREATE pattern clause is required.")

    pattern_parts = cast(list[CypherParser.OC_PatternPartContext], pattern_ctx.oC_PatternPart())
    if not pattern_parts:
        raise QueryError("CREATE pattern clause is required.")

    create_patterns: list[CreateRelationshipPattern] = []
    for part_ctx in pattern_parts:
        if part_ctx.oC_Variable() is not None:
            raise QueryError("Named pattern parts are not supported yet.")
        anonymous = part_ctx.oC_AnonymousPatternPart()
        if anonymous is None:
            raise QueryError("Expected an anonymous pattern part.")
        compiled = _compile_pattern_element(
            anonymous.oC_PatternElement(),
            params,
            state,
            allow_relationship_variables=False,
        )
        if not isinstance(compiled, PathPattern) or len(compiled.steps) != 1:
            raise QueryError(
                "MATCH ... CREATE currently supports only single-hop relationship patterns."
            )

        step = compiled.steps[0]
        if step.min_hops != 1 or step.max_hops != 1:
            raise QueryError("Variable-length relationships are not supported yet in CREATE.")
        if step.rel_type is None:
            raise QueryError("MATCH ... CREATE requires an explicit relationship type.")
        left_bound = _classify_create_node_binding(compiled.start, bound_variables)
        right_bound = _classify_create_node_binding(step.node, bound_variables)
        if not left_bound and not right_bound:
            raise QueryError("MATCH ... CREATE currently supports at least one node already bound.")
        create_patterns.append(
            CreateRelationshipPattern(
                left_variable=compiled.start.variable,
                left_bound=left_bound,
                left_labels=compiled.start.labels,
                left_properties=compiled.start.properties,
                right_variable=step.node.variable,
                right_bound=right_bound,
                right_labels=step.node.labels,
                right_properties=step.node.properties,
                rel_type=step.rel_type,
                properties=step.properties,
            )
        )
    return tuple(create_patterns)


def _compile_merge_node_pattern(
    merge_ctx: CypherParser.OC_MergeContext | None,
    params: dict[str, object],
    state: _CompilerState,
) -> MergeNodePattern:
    """Compile this Cypher fragment into the internal structures used later."""
    if merge_ctx is None:
        raise QueryError("MERGE clause is required.")

    pattern_part = merge_ctx.oC_PatternPart()
    if pattern_part is None or pattern_part.oC_Variable() is not None:
        raise QueryError("Named MERGE pattern parts are not supported yet.")
    anonymous = pattern_part.oC_AnonymousPatternPart()
    if anonymous is None:
        raise QueryError("Expected an anonymous MERGE pattern part.")
    compiled = _compile_pattern_element(
        anonymous.oC_PatternElement(),
        params,
        state,
        allow_relationship_variables=True,
    )
    if not isinstance(compiled, NodePattern):
        raise QueryError("Standalone MERGE currently supports only a single node pattern.")
    if not compiled.labels and not compiled.properties:
        raise QueryError("MERGE on nodes requires at least one label or property.")
    return MergeNodePattern(
        variable=compiled.variable,
        labels=compiled.labels,
        properties=compiled.properties,
    )


def _compile_merge_relationship_pattern(
    merge_ctx: CypherParser.OC_MergeContext | None,
    params: dict[str, object],
    state: _CompilerState,
    *,
    bound_variables: set[str],
) -> MergeRelationshipPattern:
    """Compile this Cypher fragment into the internal structures used later."""
    if merge_ctx is None:
        raise QueryError("MERGE clause is required.")

    pattern_part = merge_ctx.oC_PatternPart()
    if pattern_part is None or pattern_part.oC_Variable() is not None:
        raise QueryError("Named MERGE pattern parts are not supported yet.")
    anonymous = pattern_part.oC_AnonymousPatternPart()
    if anonymous is None:
        raise QueryError("Expected an anonymous MERGE pattern part.")
    compiled = _compile_pattern_element(
        anonymous.oC_PatternElement(),
        params,
        state,
        allow_relationship_variables=True,
    )
    if not isinstance(compiled, PathPattern) or len(compiled.steps) != 1:
        raise QueryError(
            "MATCH ... MERGE currently supports only single-hop relationship patterns."
        )

    step = compiled.steps[0]
    if step.min_hops != 1 or step.max_hops != 1:
        raise QueryError("Variable-length relationships are not supported yet in MERGE.")
    if step.rel_type is None:
        raise QueryError("MATCH ... MERGE requires an explicit relationship type.")
    left_bound = _classify_merge_node_binding(compiled.start, bound_variables)
    right_bound = _classify_merge_node_binding(step.node, bound_variables)
    if not left_bound and not right_bound:
        raise QueryError("MATCH ... MERGE currently supports at least one node already bound.")
    return MergeRelationshipPattern(
        variable=step.variable,
        left_bound=left_bound,
        left_labels=compiled.start.labels,
        left_properties=compiled.start.properties,
        right_bound=right_bound,
        right_labels=step.node.labels,
        right_properties=step.node.properties,
        left_variable=compiled.start.variable,
        right_variable=step.node.variable,
        rel_type=step.rel_type,
        properties=step.properties,
    )


def _compile_merge_actions(
    merge_ctx: CypherParser.OC_MergeContext | None,
    params: dict[str, object],
    *,
    bound_variables: set[str],
) -> tuple[tuple[SetItem, ...], tuple[SetItem, ...]]:
    """Compile this Cypher fragment into the internal structures used later."""
    if merge_ctx is None:
        raise QueryError("MERGE clause is required.")

    on_create_items: list[SetItem] = []
    on_match_items: list[SetItem] = []
    for action_ctx in cast(list[CypherParser.OC_MergeActionContext], merge_ctx.oC_MergeAction()):
        set_ctx = action_ctx.oC_Set()
        compiled_items = _compile_set_items(set_ctx, params, bound_variables=bound_variables)
        if action_ctx.CREATE() is not None:
            on_create_items.extend(compiled_items)
            continue
        if action_ctx.MATCH() is not None:
            on_match_items.extend(compiled_items)
            continue
        raise QueryError("Unsupported MERGE action.")
    return tuple(on_create_items), tuple(on_match_items)


def _compile_pattern_element(
    element_ctx: CypherParser.OC_PatternElementContext | None,
    params: dict[str, object],
    state: _CompilerState,
    *,
    allow_relationship_variables: bool,
) -> NodePattern | PathPattern:
    """Compile this Cypher fragment into the internal structures used later."""
    if element_ctx is None:
        raise QueryError("Pattern element is required.")
    if element_ctx.oC_PatternElement() is not None:
        raise QueryError("Parenthesized nested pattern elements are not supported yet.")

    start_node = _compile_node_pattern(element_ctx.oC_NodePattern(), params, state)
    chains = cast(
        list[CypherParser.OC_PatternElementChainContext],
        element_ctx.oC_PatternElementChain(),
    )
    if not chains:
        return start_node

    steps: list[RelationshipStep] = []
    for chain_ctx in chains:
        relationship_pattern = chain_ctx.oC_RelationshipPattern()
        next_node = _compile_node_pattern(chain_ctx.oC_NodePattern(), params, state)
        variable, rel_type, rel_type_alternatives, properties, direction, min_hops, max_hops = (
            _compile_relationship_pattern(
                relationship_pattern,
                params,
                allow_variable=allow_relationship_variables,
            )
        )
        steps.append(
            RelationshipStep(
                variable=variable,
                rel_type=rel_type,
                properties=properties,
                node=next_node,
                direction=direction,
                min_hops=min_hops,
                max_hops=max_hops,
                rel_type_alternatives=rel_type_alternatives,
            )
        )

    return PathPattern(start=start_node, steps=tuple(steps))


def _compile_node_pattern(
    node_ctx: CypherParser.OC_NodePatternContext | None,
    params: dict[str, object],
    state: _CompilerState,
) -> NodePattern:
    """Compile this Cypher fragment into the internal structures used later."""
    if node_ctx is None:
        raise QueryError("Node pattern is required.")

    variable_ctx = node_ctx.oC_Variable()
    variable = variable_ctx.getText() if variable_ctx is not None else state.next_variable()
    labels = _compile_node_labels(node_ctx.oC_NodeLabels())
    properties = _compile_properties(node_ctx.oC_Properties(), params)
    return NodePattern(variable=variable, labels=labels, properties=properties)


def _classify_create_node_binding(pattern: NodePattern, bound_variables: set[str]) -> bool:
    """Support the current Cypher compilation or execution step."""
    if pattern.variable not in bound_variables:
        if pattern.variable.startswith("_anon_"):
            raise QueryError(
                "MATCH ... CREATE requires unbound inline nodes to use an explicit variable."
            )
        if not pattern.labels and not pattern.properties:
            raise QueryError(
                "MATCH ... CREATE requires unbound nodes to specify at least one label or property."
            )
        return False
    if pattern.labels or pattern.properties:
        raise QueryError(
            "MATCH ... CREATE does not support inline labels or properties on bound nodes yet."
        )
    return True


def _classify_merge_node_binding(pattern: NodePattern, bound_variables: set[str]) -> bool:
    """Support the current Cypher compilation or execution step."""
    if pattern.variable in bound_variables:
        return True
    if not pattern.labels and not pattern.properties:
        raise QueryError(
            "MATCH ... MERGE requires unbound nodes to specify at least one label or property."
        )
    return False


def _compile_node_labels(
    labels_ctx: CypherParser.OC_NodeLabelsContext | None,
) -> tuple[str, ...]:
    """Compile this Cypher fragment into the internal structures used later."""
    if labels_ctx is None:
        return ()
    return tuple(
        _cypher_name(label_ctx.oC_LabelName().getText()) for label_ctx in labels_ctx.oC_NodeLabel()
    )


def _compile_relationship_pattern(
    relationship_ctx: CypherParser.OC_RelationshipPatternContext | None,
    params: dict[str, object],
    *,
    allow_variable: bool,
) -> tuple[str | None, str | None, tuple[str, ...], dict[str, object], str, int, int]:
    """Compile this Cypher fragment into the internal structures used later."""
    if relationship_ctx is None:
        raise QueryError("Relationship pattern is required.")
    has_left_arrow = relationship_ctx.oC_LeftArrowHead() is not None
    has_right_arrow = relationship_ctx.oC_RightArrowHead() is not None
    if has_left_arrow and has_right_arrow:
        raise QueryError("Bidirectional relationship arrows are not supported.")
    direction = "incoming" if has_left_arrow else "outgoing" if has_right_arrow else "undirected"

    detail = relationship_ctx.oC_RelationshipDetail()
    if detail is None:
        return None, None, (), {}, direction, 1, 1
    variable_ctx = detail.oC_Variable()
    variable = variable_ctx.getText() if variable_ctx is not None else None
    if variable is not None and not allow_variable:
        raise QueryError("Relationship variables are not supported yet.")

    rel_type: str | None = None
    rel_type_alternatives: tuple[str, ...] = ()
    relationship_types = detail.oC_RelationshipTypes()
    if relationship_types is not None:
        rel_type_names = cast(
            list[CypherParser.OC_RelTypeNameContext],
            relationship_types.oC_RelTypeName(),
        )
        rel_type_alternatives = tuple(
            _cypher_name(rel_type_name.getText()) for rel_type_name in rel_type_names
        )
        rel_type = rel_type_alternatives[0] if len(rel_type_alternatives) == 1 else None

    properties = _compile_properties(detail.oC_Properties(), params)
    min_hops, max_hops = _compile_range_literal(detail.oC_RangeLiteral())
    if direction == "undirected" and (min_hops != 1 or max_hops != 1):
        raise QueryError("Variable-length undirected relationships are not supported yet.")
    if variable is not None and (min_hops != 1 or max_hops != 1):
        raise QueryError("Relationship variables are not supported on variable-length hops yet.")
    return variable, rel_type, rel_type_alternatives, properties, direction, min_hops, max_hops


def _compile_range_literal(
    range_ctx: CypherParser.OC_RangeLiteralContext | None,
) -> tuple[int, int]:
    """Compile this Cypher fragment into the internal structures used later."""
    if range_ctx is None:
        return 1, 1

    text = range_ctx.getText()
    exact_match = re.fullmatch(r"\*(\d+)", text)
    if exact_match is not None:
        exact = int(exact_match.group(1))
        if exact < 1:
            raise QueryError("Variable-length relationships require at least one hop.")
        if exact > 5:
            raise QueryError("Variable-length relationships are limited to at most 5 hops.")
        return exact, exact

    bounded_match = re.fullmatch(r"\*(\d+)\.\.(\d+)", text)
    if bounded_match is not None:
        min_hops = int(bounded_match.group(1))
        max_hops = int(bounded_match.group(2))
        if min_hops < 1:
            raise QueryError("Variable-length relationships require at least one hop.")
        if max_hops < min_hops:
            raise QueryError("Variable-length relationship upper bound must be >= lower bound.")
        if max_hops > 5:
            raise QueryError("Variable-length relationships are limited to at most 5 hops.")
        return min_hops, max_hops

    raise QueryError(
        "Only exact or fully bounded variable-length relationships are supported for now."
    )


def _compile_properties(
    properties_ctx: CypherParser.OC_PropertiesContext | None,
    params: dict[str, object],
) -> dict[str, object]:
    """Compile this Cypher fragment into the internal structures used later."""
    if properties_ctx is None:
        return {}

    parameter_ctx = properties_ctx.oC_Parameter()
    if parameter_ctx is not None:
        parameter_value = _resolve_parameter(parameter_ctx.getText(), params)
        if not isinstance(parameter_value, dict):
            raise QueryError("Property parameters must resolve to a map/dict value.")
        return dict(parameter_value)

    map_ctx = properties_ctx.oC_MapLiteral()
    if map_ctx is None:
        raise QueryError("Unsupported properties expression.")

    keys = cast(list[CypherParser.OC_PropertyKeyNameContext], map_ctx.oC_PropertyKeyName())
    expressions = cast(list[CypherParser.OC_ExpressionContext], map_ctx.oC_Expression())
    return {
        _cypher_name(key_ctx.getText()): _compile_value_expression(expression_ctx, params)
        for key_ctx, expression_ctx in zip(keys, expressions, strict=True)
    }


def _compile_set_items(
    set_ctx: CypherParser.OC_SetContext | None,
    params: dict[str, object],
    *,
    bound_variables: set[str],
) -> tuple[SetItem, ...]:
    """Compile this Cypher fragment into the internal structures used later."""
    if set_ctx is None:
        raise QueryError("SET clause is required.")

    compiled_items: list[SetItem] = []
    for item_ctx in cast(list[CypherParser.OC_SetItemContext], set_ctx.oC_SetItem()):
        property_expression = item_ctx.oC_PropertyExpression()
        if property_expression is not None:
            variable, field = _parse_property_reference(property_expression.getText())
            if variable not in bound_variables:
                raise QueryError(f"SET references an unbound variable: {variable}")
            if field in {"id", "labels", "type", "source", "target", "__join_id"}:
                raise QueryError(f"SET cannot update reserved field '{field}'.")
            compiled_items.append(
                SetPropertyItem(
                    variable=variable,
                    field=field,
                    value=_compile_value_expression(item_ctx.oC_Expression(), params),
                )
            )
            continue

        variable_ctx = item_ctx.oC_Variable()
        is_map_merge = (
            variable_ctx is not None
            and item_ctx.getChildCount() >= 3
            and item_ctx.getChild(2).getText() == "+="
        )
        if is_map_merge:
            variable = variable_ctx.getText()
            if variable not in bound_variables:
                raise QueryError(f"SET references an unbound variable: {variable}")
            compiled_value = _compile_value_expression(item_ctx.oC_Expression(), params)
            if isinstance(compiled_value, PropertyReferenceValue):
                compiled_items.append(
                    SetMapReferenceMergeItem(variable=variable, source=compiled_value)
                )
                continue
            if not isinstance(compiled_value, dict):
                raise QueryError("SET += requires a map value.")
            if not all(isinstance(key, str) for key in compiled_value):
                raise QueryError("SET += requires string property keys.")
            reserved_fields = {"id", "labels", "type", "source", "target", "__join_id"}
            invalid_fields = sorted(key for key in compiled_value if key in reserved_fields)
            if invalid_fields:
                raise QueryError(f"SET cannot update reserved field '{invalid_fields[0]}'.")
            compiled_items.append(
                SetMapMergeItem(variable=variable, properties=dict(compiled_value))
            )
            continue

        raise QueryError("Only property assignments and += map merges are supported in SET.")
    if not compiled_items:
        raise QueryError("SET clause cannot be empty.")
    return tuple(compiled_items)


def _compile_delete_items(
    delete_ctx: CypherParser.OC_DeleteContext | None,
    *,
    bound_variables: set[str],
) -> tuple[bool, tuple[DeleteItem, ...]]:
    """Compile this Cypher fragment into the internal structures used later."""
    if delete_ctx is None:
        raise QueryError("DELETE clause is required.")
    detach = delete_ctx.DETACH() is not None

    compiled_items: list[DeleteItem] = []
    for expression_ctx in cast(list[CypherParser.OC_ExpressionContext], delete_ctx.oC_Expression()):
        expression_text = expression_ctx.getText()
        if not _IDENTIFIER_RE.fullmatch(expression_text):
            raise QueryError("DELETE currently supports only bound variables.")
        if expression_text not in bound_variables:
            raise QueryError(f"DELETE references an unbound variable: {expression_text}")
        compiled_items.append(DeleteItem(variable=expression_text))
    if not compiled_items:
        raise QueryError("DELETE clause cannot be empty.")
    return detach, tuple(compiled_items)


def _compile_remove_items(
    remove_ctx: CypherParser.OC_RemoveContext,
    *,
    bound_variables: set[str],
) -> tuple[RemoveItem, ...]:
    """Compile REMOVE targets, accepting property deletion and label cleanup shims."""
    compiled_items: list[RemoveItem] = []
    for item_ctx in cast(list[CypherParser.OC_RemoveItemContext], remove_ctx.oC_RemoveItem()):
        property_expression = item_ctx.oC_PropertyExpression()
        if property_expression is not None:
            variable, field = _parse_property_reference(property_expression.getText())
            if variable not in bound_variables:
                raise QueryError(f"REMOVE references an unbound variable: {variable}")
            if field in {"id", "labels", "type", "source", "target", "__join_id"}:
                raise QueryError(f"REMOVE cannot update reserved field '{field}'.")
            compiled_items.append(RemovePropertyItem(variable=variable, field=field))
            continue

        variable_ctx = item_ctx.oC_Variable()
        labels_ctx = item_ctx.oC_NodeLabels()
        if variable_ctx is None or labels_ctx is None:
            raise QueryError("Unsupported REMOVE item.")
        variable = variable_ctx.getText()
        if variable not in bound_variables:
            raise QueryError(f"REMOVE references an unbound variable: {variable}")
        for label in _compile_node_labels(labels_ctx):
            compiled_items.append(RemoveLabelItem(variable=variable, label=label))

    if not compiled_items:
        raise QueryError("REMOVE clause cannot be empty.")
    return tuple(compiled_items)


def _updating_clause_name(clause: CypherParser.OC_UpdatingClauseContext) -> str:
    """Return a readable name for unsupported updating clauses."""
    if clause.oC_Create() is not None:
        return "CREATE"
    if clause.oC_Merge() is not None:
        return "MERGE"
    if clause.oC_Set() is not None:
        return "SET"
    if clause.oC_Delete() is not None:
        return "DELETE"
    if clause.oC_Remove() is not None:
        return "REMOVE"
    return "UNKNOWN"


def _compile_with_items(
    projection_body: CypherParser.OC_ProjectionBodyContext | None,
    input_scope: Mapping[str, ScopeBindingKind],
) -> tuple[
    tuple[WithItem, ...],
    tuple[ReturnItem, ...],
    bool,
    dict[str, ScopeBindingKind],
]:
    """Compile this Cypher fragment into the internal structures used later."""
    if projection_body is None:
        raise QueryError("WITH clause is required.")
    distinct = projection_body.DISTINCT() is not None

    projection_items = projection_body.oC_ProjectionItems()
    if projection_items is None:
        raise QueryError("WITH clause cannot be empty.")
    if projection_items.getText() == "*":
        raise QueryError("WITH * is not supported yet.")

    items = cast(list[CypherParser.OC_ProjectionItemContext], projection_items.oC_ProjectionItem())
    if not items:
        raise QueryError("WITH clause cannot be empty.")

    aggregate_items = tuple(
        aggregate_item
        for item_ctx in items
        if (
            aggregate_item := _compile_aggregate_return_item(
                item_ctx.oC_Expression().getText(),
                item_ctx.oC_Variable().getText()
                if item_ctx.oC_Variable() is not None
                else item_ctx.oC_Expression().getText(),
                scope=input_scope,
            )
        )
        is not None
    )
    if aggregate_items:
        if distinct:
            raise QueryError("DISTINCT with aggregate WITH items is not supported yet.")
        _validate_return_items(aggregate_items)
        compiled_items: list[WithItem] = []
        grouping_scope: dict[str, ScopeBindingKind] = {}
        for item_ctx in items:
            expression_text = item_ctx.oC_Expression().getText()
            aggregate_item = _compile_aggregate_return_item(
                expression_text,
                item_ctx.oC_Variable().getText()
                if item_ctx.oC_Variable() is not None
                else expression_text,
                scope=input_scope,
            )
            if aggregate_item is not None:
                continue

            source_variable, source_field, source_kind = _compile_with_expression(
                expression_text,
                input_scope,
            )
            alias_ctx = item_ctx.oC_Variable()
            if (
                source_kind == "scalar"
                and alias_ctx is None
                and not _IDENTIFIER_RE.fullmatch(expression_text)
            ):
                raise QueryError("WITH scalar expressions currently require an explicit alias.")

            output_name = alias_ctx.getText() if alias_ctx is not None else source_variable
            if output_name in grouping_scope:
                raise QueryError(
                    f"WITH defines the same output variable more than once: {output_name}"
                )

            compiled_items.append(
                WithItem(
                    output_name=output_name,
                    source_variable=source_variable,
                    source_field=source_field,
                    source_kind=source_kind,
                )
            )
            grouping_scope[output_name] = source_kind

        aggregate_scope: dict[str, ScopeBindingKind] = dict(grouping_scope)
        aggregate_scope.update({item.expression: "scalar" for item in aggregate_items})
        return tuple(compiled_items), aggregate_items, distinct, aggregate_scope
    else:
        compiled_items = []
        output_scope: dict[str, ScopeBindingKind] = {}
        for item_ctx in items:
            expression_text = item_ctx.oC_Expression().getText()
            source_variable, source_field, source_kind = _compile_with_expression(
                expression_text,
                input_scope,
            )
            alias_ctx = item_ctx.oC_Variable()
            if (
                source_kind == "scalar"
                and alias_ctx is None
                and not _IDENTIFIER_RE.fullmatch(expression_text)
            ):
                raise QueryError("WITH scalar expressions currently require an explicit alias.")

            output_name = alias_ctx.getText() if alias_ctx is not None else source_variable
            if output_name in output_scope:
                raise QueryError(
                    f"WITH defines the same output variable more than once: {output_name}"
                )

            compiled_items.append(
                WithItem(
                    output_name=output_name,
                    source_variable=source_variable,
                    source_field=source_field,
                    source_kind=source_kind,
                )
            )
            output_scope[output_name] = source_kind

        return tuple(compiled_items), tuple(), distinct, output_scope


def _compile_unwind(
    unwind_ctx: CypherParser.OC_UnwindContext,
    params: dict[str, object],
    *,
    scope: Mapping[str, ScopeBindingKind] | None,
) -> CompiledUnwind:
    """Compile this Cypher fragment into the internal structures used later."""
    expression_ctx = unwind_ctx.oC_Expression()
    expression_text = expression_ctx.getText()
    output_name = unwind_ctx.oC_Variable().getText()

    if expression_text.startswith("$"):
        parameter_value = _resolve_parameter(expression_text, params)
        if not isinstance(parameter_value, list | tuple):
            raise QueryError("UNWIND requires a list-like literal, parameter, or bound value.")
        return CompiledUnwind(
            output_name=output_name,
            literal_values=tuple(parameter_value),
            source_variable=None,
            source_field=None,
        )

    list_literal = _extract_list_literal_from_expression(expression_ctx)
    if list_literal is not None:
        return CompiledUnwind(
            output_name=output_name,
            literal_values=tuple(
                _compile_value_expression(item_ctx, params)
                for item_ctx in list_literal.oC_Expression()
            ),
            source_variable=None,
            source_field=None,
        )

    source_variable, source_field = _compile_unwind_source(
        expression_text,
        scope=scope,
    )
    return CompiledUnwind(
        output_name=output_name,
        literal_values=None,
        source_variable=source_variable,
        source_field=source_field,
    )


def _compile_return_items(
    projection_body: CypherParser.OC_ProjectionBodyContext | None,
    *,
    scope: Mapping[str, ScopeBindingKind] | None = None,
) -> tuple[tuple[ReturnItem, ...], bool]:
    """Compile this Cypher fragment into the internal structures used later."""
    if projection_body is None:
        raise QueryError("RETURN clause is required.")
    distinct = projection_body.DISTINCT() is not None

    projection_items = projection_body.oC_ProjectionItems()
    if projection_items is None:
        raise QueryError("RETURN clause cannot be empty.")
    if projection_items.getText() == "*":
        return _compile_return_star_items(scope), distinct

    items = cast(list[CypherParser.OC_ProjectionItemContext], projection_items.oC_ProjectionItem())
    if not items:
        raise QueryError("RETURN clause cannot be empty.")

    compiled_items: list[ReturnItem] = []
    for item_ctx in items:
        expression_text = item_ctx.oC_Expression().getText()
        alias_ctx = item_ctx.oC_Variable()
        column_name = alias_ctx.getText() if alias_ctx is not None else expression_text
        aggregate_item = _compile_aggregate_return_item(
            expression_text,
            column_name,
            scope=scope,
        )
        if aggregate_item is not None:
            compiled_items.append(aggregate_item)
            continue

        scalar_expression = _compile_scalar_expression(expression_text, scope=scope)
        if scalar_expression is not None and not isinstance(scalar_expression, FieldExpression):
            compiled_items.append(
                ReturnItem(
                    expression=column_name,
                    variable=column_name,
                    field="value",
                    scalar_expression=scalar_expression,
                )
            )
            continue

        variable, field = _compile_projection_expression(expression_text, scope=scope)
        compiled_items.append(
            ReturnItem(
                expression=column_name,
                variable=variable,
                field=field,
            )
        )

    return tuple(compiled_items), distinct


def _compile_return_star_items(
    scope: Mapping[str, ScopeBindingKind] | None,
) -> tuple[ReturnItem, ...]:
    """Expand RETURN * into the currently visible public bindings."""

    if scope is None:
        raise QueryError("RETURN * requires a known query scope.")
    items: list[ReturnItem] = []
    for variable, binding_kind in scope.items():
        if _is_internal_variable(variable):
            continue
        items.append(
            ReturnItem(
                expression=variable,
                variable=variable,
                field=None if binding_kind == "entity" else "value",
            )
        )
    if not items:
        raise QueryError("RETURN * has no visible bindings.")
    return tuple(items)


def _compile_unwind_source(
    expression_text: str,
    *,
    scope: Mapping[str, ScopeBindingKind] | None,
) -> tuple[str, str]:
    """Compile this Cypher fragment into the internal structures used later."""
    if "." in expression_text:
        variable, field = _parse_property_reference(expression_text)
        if scope is not None and scope.get(variable) == "scalar":
            raise QueryError(f"Cannot access properties on scalar binding '{variable}'.")
        return variable, field

    function_binding = _compile_function_binding(expression_text, scope=scope)
    if function_binding is not None:
        return function_binding

    if scope is not None and _IDENTIFIER_RE.fullmatch(expression_text):
        binding_kind = scope.get(expression_text)
        if binding_kind == "scalar":
            return expression_text, "value"
        if binding_kind == "entity":
            raise QueryError(f"UNWIND requires a list-like value, not entity '{expression_text}'.")

    raise QueryError("UNWIND requires a list-like literal, parameter, or bound value.")


def _compile_aggregate_return_item(
    expression_text: str,
    column_name: str,
    *,
    scope: Mapping[str, ScopeBindingKind] | None = None,
) -> ReturnItem | None:
    """Compile this Cypher fragment into the internal structures used later."""
    normalized_expression = expression_text.replace(" ", "")
    rounded_avg_match = re.fullmatch(
        r"(?i)round\(100\*avg\((.+)\)\)/100",
        normalized_expression,
    )
    if rounded_avg_match is not None:
        return _compile_scalar_aggregate_item(
            "avg",
            rounded_avg_match.group(1).strip(),
            column_name,
            scope=scope,
            aggregate_transform="round_2",
        )

    match = re.fullmatch(r"(?i)(count|sum|avg|min|max|collect)\((.+)\)", expression_text)
    if match is None:
        return None

    aggregate_name = match.group(1).lower()
    operand = match.group(2).strip()
    aggregate_distinct = False
    if operand.upper().startswith("DISTINCT "):
        aggregate_distinct = True
        operand = operand[len("DISTINCT ") :].strip()
    if operand == "*":
        if aggregate_name != "count":
            raise QueryError(f"{aggregate_name.upper()}(*) is not supported.")
        if aggregate_distinct:
            raise QueryError("COUNT(DISTINCT *) is not supported.")
        return ReturnItem(
            expression=column_name,
            variable="*",
            field=None,
            aggregate="count",
            count_all=True,
        )
    if aggregate_name not in {"count", "collect"} and aggregate_distinct:
        raise QueryError(f"{aggregate_name.upper()}(DISTINCT ...) is not supported yet.")
    return _compile_scalar_aggregate_item(
        aggregate_name,
        operand,
        column_name,
        scope=scope,
        aggregate_distinct=aggregate_distinct,
    )


def _compile_scalar_aggregate_item(
    aggregate_name: str,
    operand: str,
    column_name: str,
    *,
    scope: Mapping[str, ScopeBindingKind] | None,
    aggregate_distinct: bool = False,
    aggregate_transform: str | None = None,
) -> ReturnItem:
    """Compile an aggregate over a property, scalar binding, or entity binding."""

    if "." in operand:
        variable, field = _parse_property_reference(operand)
        return ReturnItem(
            expression=column_name,
            variable=variable,
            field=field,
            aggregate=aggregate_name,
            aggregate_distinct=aggregate_distinct,
            aggregate_transform=aggregate_transform,
        )
    if _IDENTIFIER_RE.fullmatch(operand):
        operand_field: str | None = None
        if scope is not None:
            binding_kind = scope.get(operand)
            if binding_kind == "scalar":
                operand_field = "value"
        if aggregate_name not in {"count", "collect"} and operand_field is None:
            raise QueryError(f"{aggregate_name.upper()} requires a scalar or property operand.")
        return ReturnItem(
            expression=column_name,
            variable=operand,
            field=operand_field,
            aggregate=aggregate_name,
            aggregate_distinct=aggregate_distinct,
            aggregate_transform=aggregate_transform,
        )
    raise QueryError(f"Unsupported aggregate RETURN expression: {aggregate_name}({operand})")


def _validate_return_items(return_items: tuple[ReturnItem, ...]) -> None:
    """Support the current Cypher compilation or execution step."""
    aggregate_items = [item for item in return_items if item.aggregate is not None]
    if not aggregate_items:
        return
    unsupported = [
        item
        for item in aggregate_items
        if item.aggregate not in {"count", "sum", "avg", "min", "max", "collect"}
    ]
    if unsupported:
        raise QueryError(
            "Only COUNT, SUM, AVG, MIN, MAX, and COLLECT aggregate RETURN items are supported."
        )


def _compile_order_by(
    order_ctx: CypherParser.OC_OrderContext | None,
    *,
    scope: Mapping[str, ScopeBindingKind] | None = None,
    return_items: tuple[ReturnItem, ...] = (),
) -> tuple[OrderByItem, ...] | None:
    """Compile this Cypher fragment into the internal structures used later."""
    if order_ctx is None:
        return None

    sort_items = cast(list[CypherParser.OC_SortItemContext], order_ctx.oC_SortItem())
    compiled_items: list[OrderByItem] = []
    for sort_item in sort_items:
        expression_text = sort_item.oC_Expression().getText()
        return_order = _compile_return_item_order_expression(expression_text, return_items)
        if return_order is None:
            variable, field = _compile_order_expression(expression_text, scope=scope)
        else:
            variable, field = return_order
        descending = sort_item.DESC() is not None or sort_item.DESCENDING() is not None
        compiled_items.append(OrderByItem(variable=variable, field=field, descending=descending))
    return tuple(compiled_items)


def _compile_return_item_order_expression(
    expression_text: str,
    return_items: tuple[ReturnItem, ...],
) -> tuple[str, str] | None:
    """Resolve ORDER BY references to aggregate RETURN output columns."""
    has_aggregate = _contains_aggregates(return_items)
    for item in return_items:
        if item.expression == expression_text:
            if has_aggregate:
                return item.expression, "value"
            if item.field is None:
                return item.variable, _ENTITY_ID_FIELD
            return item.variable, item.field
    return None


def _compile_projection_expression(
    expression_text: str,
    *,
    scope: Mapping[str, ScopeBindingKind] | None = None,
) -> tuple[str, str | None]:
    """Compile this Cypher fragment into the internal structures used later."""
    function_binding = _compile_function_binding(expression_text, scope=scope)
    if function_binding is not None:
        return function_binding

    if "." in expression_text:
        variable, field = _parse_property_reference(expression_text)
        if scope is not None and scope.get(variable) == "scalar":
            raise QueryError(f"Cannot access properties on scalar binding '{variable}'.")
        return variable, field

    if not _IDENTIFIER_RE.fullmatch(expression_text):
        raise QueryError(f"Unsupported RETURN expression: {expression_text}")
    if scope is not None:
        binding_kind = scope.get(expression_text)
        if binding_kind is None:
            raise QueryError(f"RETURN references an unknown variable: {expression_text}")
        if binding_kind == "scalar":
            return expression_text, "value"
    return expression_text, None


def _compile_scalar_expression(
    expression_text: str,
    *,
    scope: Mapping[str, ScopeBindingKind] | None = None,
) -> ScalarExpression | None:
    """Compile a small numeric/list scalar expression used by projections and WHERE."""

    expression_text = expression_text.strip()
    wrapped = _strip_wrapping_parentheses(expression_text)
    if wrapped != expression_text:
        return _compile_scalar_expression(wrapped, scope=scope)

    function_match = re.fullmatch(r"(?i)(abs|size|tolower|toupper)\((.+)\)", expression_text)
    if function_match is not None:
        argument = _compile_scalar_expression(function_match.group(2), scope=scope)
        if argument is None:
            return None
        return FunctionExpression(name=function_match.group(1).lower(), argument=argument)

    for operators in (("+", "-"), ("*", "/")):
        split = _split_top_level_operator(expression_text, operators)
        if split is None:
            continue
        left_text, operator, right_text = split
        left = _compile_scalar_expression(left_text, scope=scope)
        right = _compile_scalar_expression(right_text, scope=scope)
        if left is None or right is None:
            return None
        return BinaryExpression(operator=operator, left=left, right=right)

    try:
        parsed_value = _parse_value(expression_text, {})
        if not isinstance(parsed_value, PropertyReferenceValue):
            return LiteralExpression(value=parsed_value)
    except QueryError:
        pass

    if "." in expression_text:
        variable, field = _parse_property_reference(expression_text)
        if scope is not None and scope.get(variable) == "scalar":
            raise QueryError(f"Cannot access properties on scalar binding '{variable}'.")
        return FieldExpression(variable=variable, field=field)

    if scope is not None and _IDENTIFIER_RE.fullmatch(expression_text):
        binding_kind = scope.get(expression_text)
        if binding_kind == "scalar":
            return FieldExpression(variable=expression_text, field="value")
        return None
    if _IDENTIFIER_RE.fullmatch(expression_text):
        return None
    return None


def _strip_wrapping_parentheses(expression_text: str) -> str:
    """Remove one pair of parentheses that wraps the whole expression."""

    if not expression_text.startswith("(") or not expression_text.endswith(")"):
        return expression_text
    depth = 0
    quote: str | None = None
    for index, character in enumerate(expression_text):
        if quote is not None:
            if character == quote:
                quote = None
            continue
        if character in {"'", '"'}:
            quote = character
            continue
        if character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if depth == 0 and index != len(expression_text) - 1:
                return expression_text
    return expression_text[1:-1].strip() if depth == 0 else expression_text


def _split_top_level_operator(
    expression_text: str,
    operators: tuple[str, ...],
) -> tuple[str, str, str] | None:
    """Split an expression on the right-most top-level arithmetic operator."""

    depth = 0
    quote: str | None = None
    for index in range(len(expression_text) - 1, -1, -1):
        character = expression_text[index]
        if quote is not None:
            if character == quote:
                quote = None
            continue
        if character in {"'", '"'}:
            quote = character
            continue
        if character in ")]}":
            depth += 1
            continue
        if character in "([{":
            depth -= 1
            continue
        if depth != 0 or character not in operators:
            continue
        if character in {"+", "-"} and (index == 0 or expression_text[index - 1] in "+-*/("):
            continue
        left = expression_text[:index].strip()
        right = expression_text[index + 1 :].strip()
        if left and right:
            return left, character, right
    return None


def _compile_order_expression(
    expression_text: str,
    *,
    scope: Mapping[str, ScopeBindingKind] | None = None,
) -> tuple[str, str]:
    """Compile this Cypher fragment into the internal structures used later."""
    function_binding = _compile_function_binding(expression_text, scope=scope)
    if function_binding is not None:
        return function_binding

    if "." in expression_text:
        variable, field = _parse_property_reference(expression_text)
        if scope is not None and scope.get(variable) == "scalar":
            raise QueryError(f"Cannot order by a property on scalar binding '{variable}'.")
        return variable, field

    if scope is not None and _IDENTIFIER_RE.fullmatch(expression_text):
        binding_kind = scope.get(expression_text)
        if binding_kind is None:
            raise QueryError(f"ORDER BY references an unknown variable: {expression_text}")
        if binding_kind == "scalar":
            return expression_text, "value"

    raise QueryError(f"Unsupported ORDER BY expression: {expression_text}")


def _compile_with_expression(
    expression_text: str,
    scope: Mapping[str, ScopeBindingKind],
) -> tuple[str, str | None, ScopeBindingKind]:
    """Compile this Cypher fragment into the internal structures used later."""
    function_binding = _compile_function_binding(expression_text, scope=scope)
    if function_binding is not None:
        variable, field = function_binding
        if field == "labels":
            raise QueryError("WITH labels(...) is not supported yet.")
        return variable, field, "scalar"

    if "." in expression_text:
        variable, field = _parse_property_reference(expression_text)
        if scope.get(variable) != "entity":
            raise QueryError(f"WITH references an unknown entity variable: {variable}")
        return variable, field, "scalar"

    if not _IDENTIFIER_RE.fullmatch(expression_text):
        raise QueryError(f"Unsupported WITH expression: {expression_text}")
    binding_kind = scope.get(expression_text)
    if binding_kind is None:
        raise QueryError(f"WITH references an unknown variable: {expression_text}")
    if binding_kind == "scalar":
        return expression_text, "value", "scalar"
    return expression_text, None, "entity"


def _compile_function_binding(
    expression_text: str,
    *,
    scope: Mapping[str, ScopeBindingKind] | None = None,
) -> tuple[str, str] | None:
    """Compile this Cypher fragment into the internal structures used later."""
    match = re.fullmatch(
        r"(?i)(id|keys|labels|type|tolower|toupper|size)\(([^)]+)\)",
        expression_text,
    )
    if match is None:
        return None

    function_name = match.group(1).lower()
    operand = match.group(2).strip()
    if function_name == "id":
        if not _IDENTIFIER_RE.fullmatch(operand):
            raise QueryError(f"Unsupported function operand: {expression_text}")
        return operand, _ENTITY_ID_FIELD
    if function_name == "labels":
        if not _IDENTIFIER_RE.fullmatch(operand):
            raise QueryError(f"Unsupported function operand: {expression_text}")
        return operand, "labels"
    if function_name == "keys":
        if not _IDENTIFIER_RE.fullmatch(operand):
            raise QueryError(f"Unsupported function operand: {expression_text}")
        return operand, "keys"
    if function_name == "type":
        if not _IDENTIFIER_RE.fullmatch(operand):
            raise QueryError(f"Unsupported function operand: {expression_text}")
        return operand, "type"
    if function_name in {"tolower", "toupper", "size"}:
        if "." in operand:
            variable, source_field = _parse_property_reference(operand)
            if scope is not None and scope.get(variable) == "scalar":
                raise QueryError(f"Cannot access properties on scalar binding '{variable}'.")
            return variable, _encode_function_field(function_name, source_field)
        if not _IDENTIFIER_RE.fullmatch(operand):
            raise QueryError(f"Unsupported function operand: {expression_text}")
        if scope is not None:
            binding_kind = scope.get(operand)
            if binding_kind == "scalar":
                return operand, _encode_function_field(function_name, "value")
            if binding_kind == "entity":
                raise QueryError(f"{match.group(1)}(...) requires a scalar or property operand.")
        raise QueryError(f"{match.group(1)}(...) requires a scalar or property operand.")
    raise QueryError(f"Unsupported function expression: {expression_text}")


def _compile_optional_int(
    clause_ctx: CypherParser.OC_SkipContext | CypherParser.OC_LimitContext | None,
    params: dict[str, object],
) -> int | None:
    """Compile this Cypher fragment into the internal structures used later."""
    if clause_ctx is None:
        return None

    value = _compile_value_expression(clause_ctx.oC_Expression(), params)
    if not isinstance(value, int):
        keyword = "SKIP" if isinstance(clause_ctx, CypherParser.OC_SkipContext) else "LIMIT"
        raise QueryError(f"{keyword} requires an integer value.")
    return value


def _compile_where(
    where_ctx: CypherParser.OC_WhereContext,
    params: dict[str, object],
    scope: Mapping[str, ScopeBindingKind] | None = None,
) -> WherePredicate:
    """Compile this Cypher fragment into the internal structures used later."""
    return _compile_or_expression(where_ctx.oC_Expression().oC_OrExpression(), params, scope)


def _compile_or_expression(
    or_ctx: Any,
    params: dict[str, object],
    scope: Mapping[str, ScopeBindingKind] | None = None,
) -> WherePredicate:
    """Compile this Cypher fragment into the internal structures used later."""
    xor_expressions = cast(list[Any], or_ctx.oC_XorExpression())
    compiled = tuple(_compile_xor_expression(xor_ctx, params, scope) for xor_ctx in xor_expressions)
    if len(compiled) == 1:
        return compiled[0]
    return OrPredicate(predicates=compiled)


def _compile_xor_expression(
    xor_ctx: Any,
    params: dict[str, object],
    scope: Mapping[str, ScopeBindingKind] | None = None,
) -> WherePredicate:
    """Compile this Cypher fragment into the internal structures used later."""
    and_expressions = cast(list[Any], xor_ctx.oC_AndExpression())
    if len(and_expressions) > 1:
        raise QueryError("XOR conditions are not supported yet.")
    return _compile_and_expression(and_expressions[0], params, scope)


def _compile_and_expression(
    and_ctx: Any,
    params: dict[str, object],
    scope: Mapping[str, ScopeBindingKind] | None = None,
) -> WherePredicate:
    """Compile this Cypher fragment into the internal structures used later."""
    not_expressions = cast(list[Any], and_ctx.oC_NotExpression())
    compiled = tuple(_compile_not_expression(context, params, scope) for context in not_expressions)
    if len(compiled) == 1:
        return compiled[0]
    return AndPredicate(predicates=compiled)


def _compile_not_expression(
    not_ctx: Any,
    params: dict[str, object],
    scope: Mapping[str, ScopeBindingKind] | None = None,
) -> WherePredicate:
    """Compile this Cypher fragment into the internal structures used later."""
    predicate = _compile_comparison_expression(not_ctx.oC_ComparisonExpression(), params, scope)
    if len(not_ctx.NOT()) % 2 == 1:
        return NotPredicate(predicate=predicate)
    return predicate


def _compile_comparison_expression(
    comparison_ctx: CypherParser.OC_ComparisonExpressionContext,
    params: dict[str, object],
    scope: Mapping[str, ScopeBindingKind] | None = None,
) -> WherePredicate:
    """Compile this Cypher fragment into the internal structures used later."""
    partials = cast(
        list[CypherParser.OC_PartialComparisonExpressionContext],
        comparison_ctx.oC_PartialComparisonExpression(),
    )
    left_operand = comparison_ctx.oC_StringListNullPredicateExpression()
    if len(partials) > 1:
        raise QueryError("WHERE clauses must contain a single comparison.")

    exists_pattern_predicate = _compile_exists_pattern_predicate(
        left_operand,
        params,
        scope=scope,
    )
    if exists_pattern_predicate is not None:
        if partials:
            raise QueryError("EXISTS { ... } cannot be used with a comparison operator.")
        return exists_pattern_predicate

    if partials:
        left_expression_text = left_operand.oC_AddOrSubtractExpression().getText()
        left_expression = _compile_scalar_expression(left_expression_text, scope=scope)
        left_variable: str
        left_field: str
        if left_expression is not None and not isinstance(left_expression, FieldExpression):
            left_variable, left_field = "__expr__", "value"
        else:
            left_variable, left_field = _compile_property_operand(left_operand, scope=scope)
        partial = partials[0]
        operator = partial.getChild(0).getText()
        if operator not in {"=", "!=", "<>", ">", ">=", "<", "<="}:
            raise QueryError(f"Unsupported comparison operator: {operator}")
        if operator == "<>":
            operator = "!="

        right_operand = partial.oC_StringListNullPredicateExpression()
        right_expression_text = right_operand.oC_AddOrSubtractExpression().getText()
        right_expression = _compile_scalar_expression(right_expression_text, scope=scope)
        right_value = (
            right_expression
            if right_expression is not None and not isinstance(right_expression, LiteralExpression)
            else _compile_scalar_operand(right_operand, params)
        )
        return Comparison(
            variable=left_variable,
            field=left_field,
            operator=operator,
            value=right_value,
            left_expression=left_expression
            if left_expression is not None and not isinstance(left_expression, FieldExpression)
            else None,
        )

    exists_predicate = _compile_exists_predicate(left_operand, scope=scope)
    if exists_predicate is not None:
        return exists_predicate

    parenthesized_predicate = _compile_parenthesized_predicate(
        left_operand,
        params,
        scope=scope,
    )
    if parenthesized_predicate is not None:
        return parenthesized_predicate

    label_predicate = _compile_label_predicate(left_operand, scope=scope)
    if label_predicate is not None:
        return label_predicate

    string_predicates = cast(
        list[CypherParser.OC_StringPredicateExpressionContext],
        left_operand.oC_StringPredicateExpression(),
    )
    if string_predicates:
        left_variable, left_field = _compile_property_operand(left_operand, scope=scope)
        if len(string_predicates) != 1:
            raise QueryError("WHERE clauses support only a single string predicate per operand.")
        return _compile_string_predicate(
            left_variable,
            left_field,
            string_predicates[0],
            params,
        )

    null_predicates = cast(list[Any], left_operand.oC_NullPredicateExpression())
    if null_predicates:
        left_variable, left_field = _compile_property_operand(left_operand, scope=scope)
        if len(null_predicates) != 1:
            raise QueryError("WHERE clauses support only a single NULL predicate per operand.")
        return NullPredicate(
            variable=left_variable,
            field=left_field,
            is_null=null_predicates[0].NOT() is None,
        )

    list_predicates = cast(list[Any], left_operand.oC_ListPredicateExpression())
    if list_predicates:
        left_variable, left_field = _compile_property_operand(left_operand, scope=scope)
        if len(list_predicates) != 1:
            raise QueryError("WHERE clauses support only a single list predicate per operand.")
        values = _compile_list_predicate_values(list_predicates[0], params)
        return InPredicate(
            variable=left_variable,
            field=left_field,
            values=values,
        )

    raise QueryError("Unsupported WHERE predicate.")


def _compile_parenthesized_predicate(
    predicate_ctx: Any,
    params: dict[str, object],
    *,
    scope: Mapping[str, ScopeBindingKind] | None = None,
) -> WherePredicate | None:
    """Compile a parenthesized boolean expression inside WHERE."""
    atom = _single_predicate_atom(predicate_ctx.oC_AddOrSubtractExpression())
    if atom is None:
        return None
    parenthesized = atom.oC_ParenthesizedExpression()
    if parenthesized is None:
        return None
    return _compile_or_expression(parenthesized.oC_Expression().oC_OrExpression(), params, scope)


def _compile_label_predicate(
    predicate_ctx: Any,
    *,
    scope: Mapping[str, ScopeBindingKind] | None = None,
) -> LabelPredicate | None:
    """Compile ``WHERE n:Label`` into a label-membership predicate."""
    expression_ctx = predicate_ctx.oC_AddOrSubtractExpression()
    multiply_contexts = expression_ctx.oC_MultiplyDivideModuloExpression()
    if len(multiply_contexts) != 1:
        return None
    power_contexts = multiply_contexts[0].oC_PowerOfExpression()
    if len(power_contexts) != 1:
        return None
    unary_contexts = power_contexts[0].oC_UnaryAddOrSubtractExpression()
    if len(unary_contexts) != 1:
        return None
    non_arithmetic = unary_contexts[0].oC_NonArithmeticOperatorExpression()
    labels_ctx = non_arithmetic.oC_NodeLabels()
    if labels_ctx is None:
        return None
    atom = non_arithmetic.oC_Atom()
    if atom is None or atom.oC_Variable() is None:
        return None
    variable = atom.oC_Variable().getText()
    if scope is not None and scope.get(variable) == "scalar":
        raise QueryError(f"Cannot use label predicate on scalar binding '{variable}'.")
    labels = _compile_node_labels(labels_ctx)
    if len(labels) != 1:
        raise QueryError("WHERE label predicates support exactly one label.")
    return LabelPredicate(variable=variable, label=labels[0])


def _compile_exists_pattern_predicate(
    predicate_ctx: Any,
    params: dict[str, object],
    *,
    scope: Mapping[str, ScopeBindingKind] | None = None,
) -> ExistsPatternPredicate | None:
    """Compile EXISTS { pattern } into a correlated pattern predicate."""

    atom = _single_predicate_atom(predicate_ctx.oC_AddOrSubtractExpression())
    if atom is None:
        return None
    exists_ctx = atom.oC_ExistentialSubquery()
    if exists_ctx is None:
        return None
    if exists_ctx.oC_RegularQuery() is not None:
        raise QueryError("EXISTS subqueries with full queries are not supported yet.")
    pattern_ctx = exists_ctx.oC_Pattern()
    if pattern_ctx is None:
        raise QueryError("EXISTS { ... } requires a pattern.")
    patterns = _compile_match_patterns(pattern_ctx, params, _CompilerState())
    pattern_scope = _scope_from_patterns(patterns)
    inner_scope = (
        _extend_scope_with_patterns(scope, patterns) if scope is not None else pattern_scope
    )
    where_predicate = (
        _compile_where(exists_ctx.oC_Where(), params, inner_scope)
        if exists_ctx.oC_Where() is not None
        else None
    )
    return ExistsPatternPredicate(patterns=patterns, where_predicate=where_predicate)


def _single_predicate_atom(expression_ctx: Any) -> Any | None:
    """Return the single atom for non-arithmetic predicate expressions."""

    multiply_contexts = expression_ctx.oC_MultiplyDivideModuloExpression()
    if len(multiply_contexts) != 1:
        return None
    power_contexts = multiply_contexts[0].oC_PowerOfExpression()
    if len(power_contexts) != 1:
        return None
    unary_contexts = power_contexts[0].oC_UnaryAddOrSubtractExpression()
    if len(unary_contexts) != 1:
        return None
    non_arithmetic = unary_contexts[0].oC_NonArithmeticOperatorExpression()
    if len(non_arithmetic.oC_PropertyLookup()) > 0 or non_arithmetic.oC_NodeLabels() is not None:
        return None
    return non_arithmetic.oC_Atom()


def _compile_property_operand(
    predicate_ctx: Any,
    *,
    scope: Mapping[str, ScopeBindingKind] | None = None,
) -> tuple[str, str]:
    """Compile this Cypher fragment into the internal structures used later."""
    expression_text = predicate_ctx.oC_AddOrSubtractExpression().getText()
    function_binding = _compile_function_binding(expression_text, scope=scope)
    if function_binding is not None:
        return function_binding

    if "." in expression_text:
        variable, field = _parse_property_reference(expression_text)
        if scope is not None and scope.get(variable) == "scalar":
            raise QueryError(f"Cannot access properties on scalar binding '{variable}'.")
        return variable, field

    if scope is not None and _IDENTIFIER_RE.fullmatch(expression_text):
        binding_kind = scope.get(expression_text)
        if binding_kind == "scalar":
            return expression_text, "value"

    raise QueryError(f"Expected property or function reference, got: {expression_text}")


def _compile_string_predicate(
    variable: str,
    field: str,
    predicate_ctx: CypherParser.OC_StringPredicateExpressionContext,
    params: dict[str, object],
) -> StringPredicate:
    """Compile this Cypher fragment into the internal structures used later."""
    operator: str
    if predicate_ctx.CONTAINS() is not None:
        operator = "contains"
    elif predicate_ctx.STARTS() is not None:
        operator = "starts_with"
    elif predicate_ctx.ENDS() is not None:
        operator = "ends_with"
    else:
        raise QueryError("Unsupported string predicate.")

    expression = predicate_ctx.oC_AddOrSubtractExpression()
    if expression is None:
        raise QueryError("String predicates require a right-hand operand.")
    value = _parse_value(expression.getText(), params)
    if not isinstance(value, str):
        raise QueryError("String predicates require a string literal or parameter value.")
    return StringPredicate(variable=variable, field=field, operator=operator, value=value)


def _compile_exists_predicate(
    predicate_ctx: Any,
    *,
    scope: Mapping[str, ScopeBindingKind] | None = None,
) -> ExistsPredicate | None:
    """Compile this Cypher fragment into the internal structures used later."""
    expression_text = predicate_ctx.oC_AddOrSubtractExpression().getText()
    match = re.fullmatch(r"(?i)exists\((.+)\)", expression_text)
    if match is None:
        return None

    operand = match.group(1).strip()
    if "." not in operand:
        raise QueryError("EXISTS currently supports only property references.")
    variable, field = _parse_property_reference(operand)
    if scope is not None and scope.get(variable) == "scalar":
        raise QueryError(f"Cannot use EXISTS on scalar binding '{variable}'.")
    return ExistsPredicate(variable=variable, field=field)


def _compile_scalar_operand(predicate_ctx: Any, params: dict[str, object]) -> object:
    """Compile this Cypher fragment into the internal structures used later."""
    _ensure_plain_predicate_operand(predicate_ctx)
    expression_text = predicate_ctx.oC_AddOrSubtractExpression().getText()
    return _parse_value(expression_text, params)


def _ensure_plain_predicate_operand(predicate_ctx: Any) -> None:
    """Support the current Cypher compilation or execution step."""
    if predicate_ctx.oC_StringPredicateExpression():
        raise QueryError("String predicate expressions are not supported yet.")
    if predicate_ctx.oC_ListPredicateExpression():
        raise QueryError("List predicate expressions are not supported yet.")
    if predicate_ctx.oC_NullPredicateExpression():
        raise QueryError("NULL predicate expressions are not supported yet.")


def _compile_list_predicate_values(
    list_predicate_ctx: Any,
    params: dict[str, object],
) -> tuple[object, ...]:
    """Compile this Cypher fragment into the internal structures used later."""
    expression = list_predicate_ctx.oC_AddOrSubtractExpression()
    expression_text = expression.getText()
    if expression_text.startswith("$"):
        parameter_value = _resolve_parameter(expression_text, params)
        if not isinstance(parameter_value, list | tuple):
            raise QueryError("IN predicates require a list-like parameter value.")
        return tuple(parameter_value)

    list_literal = _extract_list_literal(expression)
    if list_literal is None:
        raise QueryError("IN predicates currently require a literal list or parameter.")
    return tuple(
        _compile_value_expression(item_ctx, params) for item_ctx in list_literal.oC_Expression()
    )


def _compile_value_expression(
    expression_ctx: CypherParser.OC_ExpressionContext,
    params: dict[str, object],
) -> object:
    """Compile this Cypher fragment into the internal structures used later."""
    list_literal = _extract_list_literal_from_expression(expression_ctx)
    if list_literal is not None:
        item_contexts = cast(
            list[CypherParser.OC_ExpressionContext],
            list_literal.oC_Expression(),
        )
        return [_compile_value_expression(item_ctx, params) for item_ctx in item_contexts]
    expression_text = expression_ctx.getText()
    return _parse_value(expression_text, params)


def _resolve_parameter(text: str, params: Mapping[str, object]) -> object:
    """Resolve this reference into the concrete value or array used downstream."""
    if not text.startswith("$"):
        raise QueryError(f"Expected a parameter reference, got: {text}")
    name = text[1:]
    if name not in params:
        raise QueryError(f"Missing parameter: ${name}")
    return params[name]


def _match_nodes_table(
    nodes: NodeBindingSource,
    pattern: NodePattern,
    *,
    requested_fields: set[str] | None = None,
    node_property_loader: PropertyTableLoader | None = None,
    pushdown_predicate: WherePredicate | None = None,
) -> pa.Table:
    """Produce binding rows for this pattern against the current graph state."""
    table = _node_binding_table(
        nodes,
        pattern.variable,
        requested_fields=requested_fields,
        pattern=pattern,
        node_property_loader=node_property_loader,
        pushdown_predicate=pushdown_predicate,
    )
    return _apply_node_pattern_filter(table, pattern)


def _match_patterns_table(
    nodes: NodeBindingSource,
    edges: EdgeBindingSource,
    patterns: tuple[CompiledPattern, ...],
    tabular: PyArrowTabularAdapter,
    *,
    required_fields: Mapping[str, set[str]] | None = None,
    adjacency_by_source_loader: Callable[[str], pa.Table] | None = None,
    node_property_loader: PropertyTableLoader | None = None,
    edge_property_loader: PropertyTableLoader | None = None,
    where_pushdown_predicate: WherePredicate | None = None,
) -> pa.Table:
    """Produce binding rows for this pattern against the current graph state."""
    if not patterns:
        raise QueryError("At least one pattern is required.")

    matched = _match_single_pattern_table(
        nodes,
        edges,
        patterns[0],
        tabular,
        required_fields=required_fields,
        adjacency_by_source_loader=adjacency_by_source_loader,
        node_property_loader=node_property_loader,
        edge_property_loader=edge_property_loader,
        where_pushdown_predicate=where_pushdown_predicate,
    )
    bound_variables = set(_pattern_variables(patterns[0]))
    allow_disconnected_cross_join = _is_bounded_node_lookup_pattern(patterns[0])

    for pattern in patterns[1:]:
        pattern_variables = set(_pattern_variables(pattern))
        if _can_match_path_pattern_from_input_table(matched, pattern, bound_variables):
            matched = _match_fixed_path_from_input_table(
                matched,
                nodes,
                edges,
                cast(PathPattern, pattern),
                tabular,
                required_fields=required_fields,
                adjacency_by_source_loader=adjacency_by_source_loader,
                node_property_loader=node_property_loader,
                edge_property_loader=edge_property_loader,
                where_pushdown_predicate=where_pushdown_predicate,
            )
            bound_variables.update(pattern_variables)
            continue

        next_table = _match_single_pattern_table(
            nodes,
            edges,
            pattern,
            tabular,
            required_fields=required_fields,
            adjacency_by_source_loader=adjacency_by_source_loader,
            node_property_loader=node_property_loader,
            edge_property_loader=edge_property_loader,
            where_pushdown_predicate=where_pushdown_predicate,
        )
        shared_variables = tuple(sorted(bound_variables & pattern_variables))
        if not shared_variables:
            if not allow_disconnected_cross_join or not _is_bounded_node_lookup_pattern(pattern):
                raise QueryError("Disconnected pattern parts are not supported yet.")
            matched = _cross_join_tables(matched, next_table, tabular)
            bound_variables.update(pattern_variables)
            allow_disconnected_cross_join = (
                allow_disconnected_cross_join and _is_bounded_node_lookup_pattern(pattern)
            )
            continue

        # The executor only supports connected pattern parts for now, so every
        # additional pattern is joined back on the already-bound node ids.
        next_table = _project_joinable_pattern_table(
            next_table,
            pattern_variables=pattern_variables,
            shared_variables=set(shared_variables),
        )
        join_keys = [_binding_column_name(variable, "__join_id") for variable in shared_variables]
        matched = tabular.inner_join(
            matched,
            next_table,
            left_keys=join_keys,
            right_keys=join_keys,
        )
        bound_variables.update(pattern_variables)

    return matched


def _is_bounded_node_lookup_pattern(pattern: CompiledPattern) -> bool:
    """Return true for disconnected node lookups that are safe to cross join."""
    return isinstance(pattern, NodePattern) and bool(pattern.properties)


def _can_match_path_pattern_from_input_table(
    input_table: pa.Table,
    pattern: CompiledPattern,
    bound_variables: set[str],
) -> bool:
    """Return true when a path can extend already-bound rows directly."""

    if input_table.num_rows > _INCREMENTAL_MATCH_MAX_INPUT_ROWS:
        return False
    if not isinstance(pattern, PathPattern):
        return False
    if pattern.start.variable not in bound_variables:
        return False
    if pattern.start.labels or pattern.start.properties:
        return False
    return all(step.min_hops == 1 and step.max_hops == 1 for step in pattern.steps)


def _match_fixed_path_from_input_table(
    input_table: pa.Table,
    nodes: NodeBindingSource,
    edges: EdgeBindingSource,
    pattern: PathPattern,
    tabular: PyArrowTabularAdapter,
    *,
    required_fields: Mapping[str, set[str]] | None = None,
    adjacency_by_source_loader: Callable[[str], pa.Table] | None = None,
    node_property_loader: PropertyTableLoader | None = None,
    edge_property_loader: PropertyTableLoader | None = None,
    where_pushdown_predicate: WherePredicate | None = None,
) -> pa.Table:
    """Extend existing bindings with one fixed-hop path pattern."""

    matched = input_table
    for index, step in enumerate(pattern.steps):
        edge_prefix = step.variable or f"_edge_{index}"
        current_variable = (
            pattern.start.variable if index == 0 else pattern.steps[index - 1].node.variable
        )
        current_edge_key = (
            f"{edge_prefix}.source" if step.direction == "outgoing" else f"{edge_prefix}.target"
        )
        next_edge_key = (
            f"{edge_prefix}.target" if step.direction == "outgoing" else f"{edge_prefix}.source"
        )
        edge_requested_fields = _requested_fields_for_edge_step(required_fields, edge_prefix, step)
        edge_table = _edge_match_table(
            edges,
            edge_prefix,
            step,
            requested_fields=edge_requested_fields,
            adjacency_by_source_loader=adjacency_by_source_loader,
            edge_property_loader=edge_property_loader,
            source_ids=matched[_binding_column_name(current_variable, _NODE_ID_FIELD)],
        )
        matched = tabular.inner_join(
            matched,
            edge_table,
            left_keys=_binding_column_name(current_variable, _NODE_ID_FIELD),
            right_keys=current_edge_key,
        )
        matched = _ensure_edge_source_column(
            matched,
            edge_prefix=edge_prefix,
            source_variable=current_variable,
            endpoint="source" if step.direction == "outgoing" else "target",
        )
        if not _uses_typed_adjacency_path(
            step, edge_requested_fields
        ) or _has_relationship_type_alternatives(step):
            matched = _apply_edge_pattern_filter(matched, edge_prefix, step)
        if matched.num_rows == 0:
            return matched

        target_id_column = _binding_column_name(step.node.variable, _NODE_ID_FIELD)
        if target_id_column in matched.column_names:
            matched = _apply_existing_binding_filter(
                matched,
                edge_target_column=next_edge_key,
                pattern=step.node,
            )
            if matched.num_rows == 0:
                return matched
            continue

        next_nodes = _node_binding_table(
            nodes,
            step.node.variable,
            requested_fields=_requested_fields_for_variable(required_fields, step.node.variable),
            pattern=step.node,
            node_property_loader=node_property_loader,
        )
        matched = tabular.inner_join(
            matched,
            next_nodes,
            left_keys=next_edge_key,
            right_keys=_binding_column_name(step.node.variable, "__join_id"),
        )
        matched = _apply_node_pattern_filter(matched, step.node)
        matched = _apply_where_predicate_table(
            matched,
            _pushdown_predicate_for_bound_variable(
                where_pushdown_predicate,
                step.node.variable,
            ),
        )
        matched = _ensure_join_id_column(matched, step.node.variable)
        if matched.num_rows == 0:
            return matched

    return matched


def _match_patterns_from_input_table(
    input_table: pa.Table,
    nodes: NodeBindingSource,
    edges: EdgeBindingSource,
    patterns: tuple[CompiledPattern, ...],
    tabular: PyArrowTabularAdapter,
    *,
    required_fields: Mapping[str, set[str]] | None = None,
    adjacency_by_source_loader: Callable[[str], pa.Table] | None = None,
    node_property_loader: PropertyTableLoader | None = None,
    edge_property_loader: PropertyTableLoader | None = None,
    where_pushdown_predicate: WherePredicate | None = None,
) -> pa.Table:
    """Produce binding rows for this pattern against the current graph state."""
    if not patterns:
        return input_table

    matched = input_table
    bound_variables = _entity_variables_from_table(matched)
    for pattern in patterns:
        next_table = _match_single_pattern_table(
            nodes,
            edges,
            pattern,
            tabular,
            required_fields=required_fields,
            adjacency_by_source_loader=adjacency_by_source_loader,
            node_property_loader=node_property_loader,
            edge_property_loader=edge_property_loader,
            where_pushdown_predicate=where_pushdown_predicate,
        )
        pattern_variables = set(_pattern_variables(pattern))
        shared_variables = tuple(sorted(bound_variables & pattern_variables))
        if not shared_variables:
            if not _pattern_has_property_reference_values(pattern):
                raise QueryError("MATCH after WITH requires a pattern connected to the WITH scope.")
            matched = _cross_join_tables(matched, next_table, tabular)
            matched = _apply_deferred_pattern_reference_filters(matched, pattern)
        else:
            next_table = _project_joinable_pattern_table(
                next_table,
                pattern_variables=pattern_variables,
                shared_variables=set(shared_variables),
            )
            join_keys = [
                _binding_column_name(variable, "__join_id") for variable in shared_variables
            ]
            matched = tabular.inner_join(
                matched,
                next_table,
                left_keys=join_keys,
                right_keys=join_keys,
            )
        bound_variables.update(pattern_variables)

    return matched


def _optional_match_patterns_from_input_table(
    input_table: pa.Table,
    nodes: NodeBindingSource,
    edges: EdgeBindingSource,
    patterns: tuple[CompiledPattern, ...],
    where_predicate: WherePredicate | None,
    tabular: PyArrowTabularAdapter,
    *,
    required_fields: Mapping[str, set[str]] | None = None,
    adjacency_by_source_loader: Callable[[str], pa.Table] | None = None,
    node_property_loader: PropertyTableLoader | None = None,
    edge_property_loader: PropertyTableLoader | None = None,
) -> pa.Table:
    """Produce optional-match rows while preserving left-side bindings when needed."""
    if input_table.num_rows == 0:
        return input_table

    if _can_use_optional_direct_path(patterns, input_table, where_predicate):
        return _optional_match_single_path_from_input_table(
            input_table,
            nodes,
            edges,
            cast(PathPattern, patterns[0]),
            tabular,
            required_fields=required_fields,
            adjacency_by_source_loader=adjacency_by_source_loader,
            node_property_loader=node_property_loader,
            edge_property_loader=edge_property_loader,
        )

    matched = _match_patterns_from_input_table(
        input_table,
        nodes,
        edges,
        patterns,
        tabular,
        required_fields=required_fields,
        adjacency_by_source_loader=adjacency_by_source_loader,
        node_property_loader=node_property_loader,
        edge_property_loader=edge_property_loader,
    )
    matched = _apply_where_predicate_table(
        matched,
        where_predicate,
        nodes=nodes,
        edges=edges,
        tabular=tabular,
        adjacency_by_source_loader=adjacency_by_source_loader,
        node_property_loader=node_property_loader,
        edge_property_loader=edge_property_loader,
    )

    input_variables = _entity_variables_from_table(input_table)
    optional_variables = {
        variable for pattern in patterns for variable in _pattern_variables(pattern)
    }
    shared_variables = input_variables & optional_variables
    projected_optional = _project_joinable_pattern_table(
        matched,
        pattern_variables=optional_variables,
        shared_variables=shared_variables,
    )
    projected_optional = _with_missing_optional_columns(
        projected_optional,
        [
            column_name
            for column_name in _pattern_null_columns(patterns)
            if not any(column_name.startswith(f"{variable}.") for variable in shared_variables)
        ],
    )
    join_keys = [
        _binding_column_name(variable, "__join_id") for variable in sorted(shared_variables)
    ]
    return tabular.left_join(
        input_table,
        projected_optional,
        left_keys=join_keys,
        right_keys=join_keys,
    )


def _can_use_optional_direct_path(
    patterns: tuple[CompiledPattern, ...],
    input_table: pa.Table,
    where_predicate: WherePredicate | None,
) -> bool:
    """Check whether the optimized or semantically valid path applies here."""
    if where_predicate is not None or len(patterns) != 1:
        return False
    pattern = patterns[0]
    if not isinstance(pattern, PathPattern):
        return False
    if pattern.start.variable not in _entity_variables_from_table(input_table):
        return False
    for step in pattern.steps:
        if step.min_hops != 1 or step.max_hops != 1:
            return False
        if step.node.variable in _entity_variables_from_table(input_table):
            return False
    return True


def _optional_match_single_path_from_input_table(
    input_table: pa.Table,
    nodes: NodeBindingSource,
    edges: EdgeBindingSource,
    pattern: PathPattern,
    tabular: PyArrowTabularAdapter,
    *,
    required_fields: Mapping[str, set[str]] | None = None,
    adjacency_by_source_loader: Callable[[str], pa.Table] | None = None,
    node_property_loader: PropertyTableLoader | None = None,
    edge_property_loader: PropertyTableLoader | None = None,
) -> pa.Table:
    """Produce optional-match rows while preserving left-side bindings when needed."""
    shared_variables = {pattern.start.variable}
    optional_variables = set(_pattern_variables(pattern))
    working_input = _project_optional_direct_input_table(input_table)
    matched = working_input
    current_variable = pattern.start.variable
    for index, step in enumerate(pattern.steps):
        edge_prefix = step.variable or f"_edge_{index}"
        current_edge_key = (
            f"{edge_prefix}.source" if step.direction == "outgoing" else f"{edge_prefix}.target"
        )
        next_edge_key = (
            f"{edge_prefix}.target" if step.direction == "outgoing" else f"{edge_prefix}.source"
        )
        edge_requested_fields = _requested_fields_for_edge_step(required_fields, edge_prefix, step)
        edge_table = _edge_match_table(
            edges,
            edge_prefix,
            step,
            requested_fields=edge_requested_fields,
            adjacency_by_source_loader=adjacency_by_source_loader,
            edge_property_loader=edge_property_loader,
            source_ids=matched[_binding_column_name(current_variable, _NODE_ID_FIELD)],
        )
        if not _uses_typed_adjacency_path(
            step, edge_requested_fields
        ) or _has_relationship_type_alternatives(step):
            edge_table = _apply_edge_pattern_filter(edge_table, edge_prefix, step)
        matched = tabular.left_join(
            matched,
            edge_table,
            left_keys=_binding_column_name(current_variable, _NODE_ID_FIELD),
            right_keys=current_edge_key,
        )
        matched = _ensure_edge_source_column(
            matched,
            edge_prefix=edge_prefix,
            source_variable=current_variable,
            endpoint="source" if step.direction == "outgoing" else "target",
        )

        next_nodes = _match_nodes_table(
            nodes,
            step.node,
            requested_fields=_requested_fields_for_variable(required_fields, step.node.variable),
            node_property_loader=node_property_loader,
        )
        matched = tabular.left_join(
            matched,
            next_nodes,
            left_keys=next_edge_key,
            right_keys=_binding_column_name(step.node.variable, "__join_id"),
        )
        current_variable = step.node.variable

    projected_optional = _project_optional_direct_result_table(
        matched,
        input_columns=input_table.column_names,
        optional_variables=optional_variables - shared_variables,
    )
    projected_optional = _with_missing_optional_columns(
        projected_optional,
        [
            column_name
            for column_name in _pattern_null_columns((pattern,))
            if not any(column_name.startswith(f"{variable}.") for variable in shared_variables)
        ],
    )
    return projected_optional


def _project_optional_direct_input_table(
    input_table: pa.Table,
) -> pa.Table:
    """Project bindings into the columns or values needed by the next step."""
    return input_table


def _project_optional_direct_result_table(
    table: pa.Table,
    *,
    input_columns: list[str],
    optional_variables: set[str],
) -> pa.Table:
    """Project bindings into the columns or values needed by the next step."""
    columns = [column_name for column_name in input_columns if column_name in table.column_names]
    for variable in sorted(optional_variables):
        prefix = f"{variable}."
        columns.extend(column for column in table.column_names if column.startswith(prefix))
    return table.select(columns)


def _apply_unwind_to_input_table(
    input_table: pa.Table,
    unwind: CompiledUnwind,
) -> pa.Table:
    """Apply this transformation or predicate inside the Cypher pipeline."""
    output_column = _binding_column_name(unwind.output_name, "value")
    if unwind.literal_values is not None:
        return _expand_unwind_values(
            input_table,
            output_column=output_column,
            values=unwind.literal_values,
        )

    if unwind.source_variable is None or unwind.source_field is None:
        raise QueryError("UNWIND source is not defined.")

    if input_table.num_rows == 0:
        return _empty_unwind_result(input_table, output_column)
    source_array = _resolve_table_field_array(
        input_table,
        variable=unwind.source_variable,
        field=unwind.source_field,
    )
    if source_array is None:
        source_column = _binding_column_name(unwind.source_variable, unwind.source_field)
        raise QueryError(f"UNWIND references an unknown binding: {source_column}")
    if not pa.types.is_list(source_array.type):
        raise QueryError("UNWIND requires a list-like literal, parameter, or bound value.")
    parent_indices = pc.list_parent_indices(source_array)
    if len(parent_indices) == 0:
        return _empty_unwind_result(input_table, output_column)
    output_columns: dict[str, pa.Array | pa.ChunkedArray] = {
        column_name: pc.take(input_table[column_name], parent_indices)
        for column_name in input_table.column_names
    }
    output_columns[output_column] = pc.list_flatten(source_array)
    return pa.table(output_columns)


def _expand_unwind_values(
    input_table: pa.Table,
    *,
    output_column: str,
    values: tuple[object, ...],
) -> pa.Table:
    """Expand this pattern or value into the variants required by execution."""
    if input_table.num_rows == 0:
        if input_table.column_names:
            return _empty_unwind_result(input_table, output_column)
        if not values:
            return pa.table({output_column: pa.array([], type=pa.null())})
        return pa.table({output_column: pa.array(values)})

    if not values:
        return _empty_unwind_result(input_table, output_column)
    literal_array = pa.array([list(values)] * input_table.num_rows)
    parent_indices = pc.list_parent_indices(literal_array)
    output_columns: dict[str, pa.Array | pa.ChunkedArray] = {
        column_name: pc.take(input_table[column_name], parent_indices)
        for column_name in input_table.column_names
    }
    output_columns[output_column] = pc.list_flatten(literal_array)
    return pa.table(output_columns)


def _empty_unwind_result(input_table: pa.Table, output_column: str) -> pa.Table:
    """Build an empty typed result used by edge cases and fallback paths."""
    columns: dict[str, pa.ChunkedArray | pa.Array] = {
        column_name: input_table[column_name].slice(0, 0)
        for column_name in input_table.column_names
    }
    columns[output_column] = pa.array([], type=pa.null_())
    return pa.table(columns)


def _single_row_table() -> pa.Table:
    """Build the one-row seed table used by standalone UNWIND pipelines."""
    return pa.Table.from_pylist([{}])


def _match_single_pattern_table(
    nodes: NodeBindingSource,
    edges: EdgeBindingSource,
    pattern: CompiledPattern,
    tabular: PyArrowTabularAdapter,
    *,
    required_fields: Mapping[str, set[str]] | None = None,
    adjacency_by_source_loader: Callable[[str], pa.Table] | None = None,
    node_property_loader: PropertyTableLoader | None = None,
    edge_property_loader: PropertyTableLoader | None = None,
    where_pushdown_predicate: WherePredicate | None = None,
) -> pa.Table:
    """Produce binding rows for this pattern against the current graph state."""
    if _can_use_iterative_variable_length_match(pattern):
        return _match_single_variable_length_path_table(
            nodes,
            edges,
            cast(PathPattern, pattern),
            tabular,
            required_fields=required_fields,
            adjacency_by_source_loader=adjacency_by_source_loader,
            node_property_loader=node_property_loader,
            edge_property_loader=edge_property_loader,
        )

    variants = _expand_pattern_variants(pattern)
    tables: list[pa.Table] = []
    for variant in variants:
        if isinstance(variant, PathPattern):
            tables.append(
                _match_fixed_path_table(
                    nodes,
                    edges,
                    variant,
                    tabular,
                    required_fields=required_fields,
                    adjacency_by_source_loader=adjacency_by_source_loader,
                    edge_property_loader=edge_property_loader,
                    where_pushdown_predicate=where_pushdown_predicate,
                )
            )
        else:
            tables.append(
                _match_nodes_table(
                    nodes,
                    variant,
                    requested_fields=_requested_fields_for_variable(
                        required_fields,
                        variant.variable,
                    ),
                    node_property_loader=node_property_loader,
                    pushdown_predicate=where_pushdown_predicate,
                )
            )
    return tabular.concatenate_tables(tables, schema=tables[0].schema)


def _match_fixed_path_table(
    nodes: NodeBindingSource,
    edges: EdgeBindingSource,
    pattern: PathPattern,
    tabular: PyArrowTabularAdapter,
    *,
    required_fields: Mapping[str, set[str]] | None = None,
    adjacency_by_source_loader: Callable[[str], pa.Table] | None = None,
    node_property_loader: PropertyTableLoader | None = None,
    edge_property_loader: PropertyTableLoader | None = None,
    where_pushdown_predicate: WherePredicate | None = None,
) -> pa.Table:
    """Produce binding rows for this pattern against the current graph state."""
    matched = _apply_node_pattern_filter(
        _node_binding_table(
            nodes,
            pattern.start.variable,
            requested_fields=_requested_fields_for_variable(
                required_fields,
                pattern.start.variable,
            ),
            pattern=pattern.start,
            node_property_loader=node_property_loader,
        ),
        pattern.start,
    )

    for index, step in enumerate(pattern.steps):
        edge_prefix = step.variable or f"_edge_{index}"
        current_variable = (
            pattern.start.variable if index == 0 else pattern.steps[index - 1].node.variable
        )
        current_edge_key = (
            f"{edge_prefix}.source" if step.direction == "outgoing" else f"{edge_prefix}.target"
        )
        next_edge_key = (
            f"{edge_prefix}.target" if step.direction == "outgoing" else f"{edge_prefix}.source"
        )
        edge_requested_fields = _requested_fields_for_edge_step(required_fields, edge_prefix, step)
        edge_table = _edge_match_table(
            edges,
            edge_prefix,
            step,
            requested_fields=edge_requested_fields,
            adjacency_by_source_loader=adjacency_by_source_loader,
            edge_property_loader=edge_property_loader,
            source_ids=matched[_binding_column_name(current_variable, _NODE_ID_FIELD)],
        )
        matched = tabular.inner_join(
            matched,
            edge_table,
            left_keys=_binding_column_name(current_variable, _NODE_ID_FIELD),
            right_keys=current_edge_key,
        )
        matched = _ensure_edge_source_column(
            matched,
            edge_prefix=edge_prefix,
            source_variable=current_variable,
            endpoint="source" if step.direction == "outgoing" else "target",
        )
        matched = _filter_reused_relationships_in_fixed_path(
            matched,
            current_edge_prefix=edge_prefix,
            previous_edge_prefixes=tuple(
                pattern.steps[previous_index].variable or f"_edge_{previous_index}"
                for previous_index in range(index)
            ),
        )
        if not _uses_typed_adjacency_path(
            step, edge_requested_fields
        ) or _has_relationship_type_alternatives(step):
            matched = _apply_edge_pattern_filter(matched, edge_prefix, step)
        if matched.num_rows == 0:
            return _project_public_pattern_columns(matched, pattern, required_fields)

        target_id_column = _binding_column_name(step.node.variable, _NODE_ID_FIELD)
        if target_id_column in matched.column_names:
            matched = _apply_existing_binding_filter(
                matched,
                edge_target_column=next_edge_key,
                pattern=step.node,
            )
            continue

        next_nodes = _node_binding_table(
            nodes,
            step.node.variable,
            requested_fields=_requested_fields_for_variable(required_fields, step.node.variable),
            pattern=step.node,
            node_property_loader=node_property_loader,
        )
        matched = tabular.inner_join(
            matched,
            next_nodes,
            left_keys=next_edge_key,
            right_keys=_binding_column_name(step.node.variable, "__join_id"),
        )
        matched = _apply_node_pattern_filter(matched, step.node)
        matched = _apply_where_predicate_table(
            matched,
            _pushdown_predicate_for_bound_variable(
                where_pushdown_predicate,
                step.node.variable,
            ),
        )
        matched = _ensure_join_id_column(matched, step.node.variable)
        if matched.num_rows == 0:
            return _project_public_pattern_columns(matched, pattern, required_fields)

    return _project_public_pattern_columns(matched, pattern, required_fields)


def _filter_reused_relationships_in_fixed_path(
    table: pa.Table,
    *,
    current_edge_prefix: str,
    previous_edge_prefixes: tuple[str, ...],
) -> pa.Table:
    """Apply Cypher relationship uniqueness for fixed-hop path patterns."""
    current_column = f"{current_edge_prefix}.id"
    if current_column not in table.column_names:
        return table
    masks: list[pa.Array] = []
    for previous_edge_prefix in previous_edge_prefixes:
        previous_column = f"{previous_edge_prefix}.id"
        if previous_column not in table.column_names:
            continue
        masks.append(pc.not_equal(table[current_column], table[previous_column]))
    return _filter_table_with_masks(table, masks)


def _can_use_iterative_variable_length_match(pattern: CompiledPattern) -> bool:
    """Check whether the optimized or semantically valid path applies here."""
    if not isinstance(pattern, PathPattern):
        return False
    if len(pattern.steps) != 1:
        return False
    step = pattern.steps[0]
    if step.min_hops == 1 and step.max_hops == 1:
        return False
    if step.variable is not None:
        return False
    if pattern.start.variable == step.node.variable:
        return False
    return True


def _match_single_variable_length_path_table(
    nodes: NodeBindingSource,
    edges: EdgeBindingSource,
    pattern: PathPattern,
    tabular: PyArrowTabularAdapter,
    *,
    required_fields: Mapping[str, set[str]] | None = None,
    adjacency_by_source_loader: Callable[[str], pa.Table] | None = None,
    node_property_loader: PropertyTableLoader | None = None,
    edge_property_loader: PropertyTableLoader | None = None,
) -> pa.Table:
    """Produce binding rows for this pattern against the current graph state."""
    step = pattern.steps[0]
    start_table = _apply_node_pattern_filter(
        _node_binding_table(
            nodes,
            pattern.start.variable,
            requested_fields=_requested_fields_for_variable(
                required_fields,
                pattern.start.variable,
            ),
            pattern=pattern.start,
            node_property_loader=node_property_loader,
        ),
        pattern.start,
    )
    if start_table.num_rows == 0:
        return _empty_public_path_table(
            start_table=start_table,
            nodes=nodes,
            end_variable=step.node.variable,
            required_fields=required_fields,
            end_pattern=step.node,
            node_property_loader=node_property_loader,
        )

    current_id_column = "__varlen_current.id"
    start_id_column = "__varlen_start.id"
    start_prefix = f"{pattern.start.variable}."
    start_columns = [
        column_name
        for column_name in start_table.column_names
        if column_name.startswith(start_prefix)
    ]
    frontier = pa.table(
        {
            start_id_column: start_table[
                _binding_column_name(pattern.start.variable, _NODE_ID_FIELD)
            ],
            current_id_column: start_table[
                _binding_column_name(pattern.start.variable, _NODE_ID_FIELD)
            ],
        }
    )

    edge_prefix = "__varlen_edge"
    edge_requested_fields = _requested_fields_for_edge_step(required_fields, edge_prefix, step)
    filtered_edges = _edge_match_table(
        edges,
        edge_prefix,
        step,
        requested_fields=edge_requested_fields,
        adjacency_by_source_loader=adjacency_by_source_loader,
        edge_property_loader=edge_property_loader,
    )
    if not _uses_typed_adjacency_path(
        step, edge_requested_fields
    ) or _has_relationship_type_alternatives(step):
        filtered_edges = _apply_edge_pattern_filter(filtered_edges, edge_prefix, step)
    if filtered_edges.num_rows == 0:
        return _empty_public_path_table(
            start_table=start_table,
            nodes=nodes,
            end_variable=step.node.variable,
            required_fields=required_fields,
            end_pattern=step.node,
            node_property_loader=node_property_loader,
        )
    filtered_edges = filtered_edges.select(
        [f"{edge_prefix}.id", f"{edge_prefix}.source", f"{edge_prefix}.target"]
    )

    end_nodes = _node_binding_table(
        nodes,
        step.node.variable,
        requested_fields=_requested_fields_for_variable(required_fields, step.node.variable),
        pattern=step.node,
        node_property_loader=node_property_loader,
    )
    result_tables: list[pa.Table] = []
    for hop_count in range(1, step.max_hops + 1):
        current_edge_id_column = f"__varlen_edge_{hop_count - 1}.id"
        joined_edges = tabular.inner_join(
            frontier,
            filtered_edges,
            left_keys=current_id_column,
            right_keys=f"{edge_prefix}.source",
        )
        if joined_edges.num_rows == 0:
            break
        joined_edges = joined_edges.rename_columns(
            [
                current_edge_id_column if column_name == f"{edge_prefix}.id" else column_name
                for column_name in joined_edges.column_names
            ]
        )
        joined_edges = _filter_reused_relationships_in_variable_length_path(
            joined_edges,
            current_edge_id_column=current_edge_id_column,
            previous_edge_id_columns=tuple(
                f"__varlen_edge_{previous_hop}.id" for previous_hop in range(hop_count - 1)
            ),
        )
        if joined_edges.num_rows == 0:
            break

        joined_nodes = tabular.inner_join(
            joined_edges,
            end_nodes,
            left_keys=f"{edge_prefix}.target",
            right_keys=_binding_column_name(step.node.variable, "__join_id"),
        )
        if joined_nodes.num_rows == 0:
            break

        if hop_count >= step.min_hops:
            candidate_rows = _apply_node_pattern_filter(joined_nodes, step.node)
            if candidate_rows.num_rows > 0:
                result_tables.append(
                    _project_public_pattern_columns(
                        _with_variable_length_start_fields(
                            candidate_rows,
                            start_table.select(start_columns),
                            start_variable=pattern.start.variable,
                            start_id_column=start_id_column,
                            tabular=tabular,
                        ),
                        pattern,
                    )
                )

        frontier = pa.table(
            {
                start_id_column: joined_nodes[start_id_column],
                current_id_column: joined_nodes[
                    _binding_column_name(step.node.variable, _NODE_ID_FIELD)
                ],
                **{
                    edge_id_column: joined_nodes[edge_id_column]
                    for edge_id_column in (
                        f"__varlen_edge_{visited_hop}.id" for visited_hop in range(hop_count)
                    )
                    if edge_id_column in joined_nodes.column_names
                },
            }
        )

    if result_tables:
        return tabular.concatenate_tables(result_tables, schema=result_tables[0].schema)
    return _empty_public_path_table(
        start_table=start_table,
        nodes=nodes,
        end_variable=step.node.variable,
        required_fields=required_fields,
        end_pattern=step.node,
        node_property_loader=node_property_loader,
    )


def _empty_public_path_table(
    *,
    start_table: pa.Table,
    nodes: NodeBindingSource,
    end_variable: str,
    required_fields: Mapping[str, set[str]] | None = None,
    end_pattern: NodePattern | None = None,
    node_property_loader: PropertyTableLoader | None = None,
) -> pa.Table:
    """Build an empty typed result used by edge cases and fallback paths."""
    start_columns = {
        column_name: start_table[column_name].slice(0, 0)
        for column_name in start_table.column_names
    }
    end_table = _node_binding_table(
        nodes,
        end_variable,
        requested_fields=_requested_fields_for_variable(required_fields, end_variable),
        pattern=end_pattern,
        node_property_loader=node_property_loader,
    ).slice(0, 0)
    end_columns = {
        column_name: end_table[column_name]
        for column_name in end_table.column_names
        if column_name.startswith(f"{end_variable}.")
    }
    return pa.table({**start_columns, **end_columns})


def _with_variable_length_start_fields(
    table: pa.Table,
    start_table: pa.Table,
    *,
    start_variable: str,
    start_id_column: str,
    tabular: PyArrowTabularAdapter,
) -> pa.Table:
    """Attach start-node fields after variable-length expansion produces output rows."""

    if all(column_name in table.column_names for column_name in start_table.column_names):
        return table
    return tabular.inner_join(
        table,
        start_table,
        left_keys=start_id_column,
        right_keys=_binding_column_name(start_variable, _NODE_ID_FIELD),
    )


def _filter_reused_relationships_in_variable_length_path(
    table: pa.Table,
    *,
    current_edge_id_column: str,
    previous_edge_id_columns: tuple[str, ...],
) -> pa.Table:
    """Apply Cypher relationship uniqueness while expanding one variable-length path."""
    if current_edge_id_column not in table.column_names:
        return table
    masks: list[pa.Array] = []
    for previous_edge_id_column in previous_edge_id_columns:
        if previous_edge_id_column not in table.column_names:
            continue
        masks.append(pc.not_equal(table[current_edge_id_column], table[previous_edge_id_column]))
    return _filter_table_with_masks(table, masks)


def _match_distinct_endpoint_range_table(
    nodes: pa.Table,
    pattern: PathPattern,
    return_item: ReturnItem,
    tabular: PyArrowTabularAdapter,
    *,
    adjacency_by_source_loader: Callable[[str], pa.Table] | None,
    node_property_loader: PropertyTableLoader | None = None,
) -> pa.Table:
    """Return distinct endpoint projections for bounded variable-length traversals."""
    assert return_item.field is not None
    step = pattern.steps[0]
    start_table = _apply_node_pattern_filter(
        _node_binding_table(
            nodes,
            pattern.start.variable,
            requested_fields={
                _NODE_ID_FIELD,
                *pattern.start.properties.keys(),
            },
            pattern=pattern.start,
            node_property_loader=node_property_loader,
        ),
        pattern.start,
    )
    if start_table.num_rows == 0:
        return pa.table({_binding_column_name(return_item.variable, return_item.field): []})

    end_table = _apply_node_pattern_filter(
        _node_binding_table(
            nodes,
            step.node.variable,
            requested_fields={
                _NODE_ID_FIELD,
                return_item.field,
                *step.node.properties.keys(),
            },
            pattern=step.node,
            node_property_loader=node_property_loader,
        ),
        step.node,
    )
    if end_table.num_rows == 0:
        return _project_return_items_table(end_table, (return_item,))

    edge_prefix = "__endpoint_range_edge"
    edge_table = _edge_match_table(
        pa.table({}),
        edge_prefix,
        step,
        requested_fields=set(),
        adjacency_by_source_loader=adjacency_by_source_loader,
    )
    if edge_table.num_rows == 0:
        return _project_return_items_table(end_table.slice(0, 0), (return_item,))

    end_value_by_id = _endpoint_value_map_from_table(end_table, return_item=return_item)
    allowed_end_ids = set(end_value_by_id)
    start_ids = [
        int(value)
        for value in start_table[_binding_column_name(pattern.start.variable, _NODE_ID_FIELD)]
        .combine_chunks()
        .to_pylist()
    ]

    if step.min_hops == 1:
        matched_endpoint_ids = _distinct_reachable_endpoint_ids(
            start_ids=start_ids,
            adjacency=_adjacency_targets_from_edge_table(edge_table, edge_prefix=edge_prefix),
            allowed_end_ids=allowed_end_ids,
            max_hops=step.max_hops,
        )
    else:
        matched_endpoint_ids = _distinct_endpoint_ids_with_relationship_uniqueness(
            start_ids=start_ids,
            adjacency=_adjacency_map_from_edge_table(edge_table, edge_prefix=edge_prefix),
            allowed_end_ids=allowed_end_ids,
            min_hops=step.min_hops,
            max_hops=step.max_hops,
        )

    if not matched_endpoint_ids:
        return _project_return_items_table(end_table.slice(0, 0), (return_item,))

    values = [end_value_by_id[node_id] for node_id in sorted(matched_endpoint_ids)]
    projected = pa.table({_binding_column_name(return_item.variable, return_item.field): values})
    return _apply_distinct_to_table(projected)


def _distinct_reachable_endpoint_ids(
    *,
    start_ids: list[int],
    adjacency: dict[int, tuple[int, ...]],
    allowed_end_ids: set[int],
    max_hops: int,
) -> set[int]:
    """Find distinct reachable endpoints for min-hop-one endpoint-only traversals."""
    matched_endpoint_ids: set[int] = set()
    frontier = set(start_ids)
    seen_nodes = set(start_ids)
    for _ in range(1, max_hops + 1):
        next_frontier: set[int] = set()
        for current_node_id in frontier:
            for target_id in adjacency.get(current_node_id, ()):
                if target_id in allowed_end_ids:
                    matched_endpoint_ids.add(target_id)
                next_frontier.add(target_id)
        next_frontier.difference_update(seen_nodes)
        if not next_frontier:
            break
        seen_nodes.update(next_frontier)
        frontier = next_frontier
    return matched_endpoint_ids


def _distinct_endpoint_ids_with_relationship_uniqueness(
    *,
    start_ids: list[int],
    adjacency: dict[int, tuple[tuple[int, int], ...]],
    allowed_end_ids: set[int],
    min_hops: int,
    max_hops: int,
) -> set[int]:
    """Find endpoints while preserving relationship uniqueness for min-hop greater than one."""
    matched_endpoint_ids: set[int] = set()
    frontier: set[tuple[int, tuple[int, ...]]] = {(node_id, ()) for node_id in start_ids}
    for hop_count in range(1, max_hops + 1):
        next_frontier: set[tuple[int, tuple[int, ...]]] = set()
        for current_node_id, visited_edge_ids in frontier:
            for edge_id, target_id in adjacency.get(current_node_id, ()):
                if edge_id in visited_edge_ids:
                    continue
                next_visited_edge_ids = (*visited_edge_ids, edge_id)
                if hop_count >= min_hops and target_id in allowed_end_ids:
                    matched_endpoint_ids.add(target_id)
                if hop_count < max_hops:
                    next_frontier.add((target_id, next_visited_edge_ids))
        if not next_frontier:
            break
        frontier = next_frontier
    return matched_endpoint_ids


def _adjacency_map_from_edge_table(
    edge_table: pa.Table,
    *,
    edge_prefix: str,
) -> dict[int, tuple[tuple[int, int], ...]]:
    """Build a compact source-id adjacency map for endpoint-only traversal."""
    edge_ids = edge_table[f"{edge_prefix}.id"].combine_chunks().to_pylist()
    source_ids = edge_table[f"{edge_prefix}.source"].combine_chunks().to_pylist()
    target_ids = edge_table[f"{edge_prefix}.target"].combine_chunks().to_pylist()
    adjacency_lists: dict[int, list[tuple[int, int]]] = {}
    for edge_id, source_id, target_id in zip(edge_ids, source_ids, target_ids, strict=True):
        adjacency_lists.setdefault(int(source_id), []).append((int(edge_id), int(target_id)))
    return {source_id: tuple(targets) for source_id, targets in adjacency_lists.items()}


def _adjacency_targets_from_edge_table(
    edge_table: pa.Table,
    *,
    edge_prefix: str,
) -> dict[int, tuple[int, ...]]:
    """Build a compact source-id to target-ids map for reachability-only traversal."""
    source_ids = edge_table[f"{edge_prefix}.source"].combine_chunks().to_pylist()
    target_ids = edge_table[f"{edge_prefix}.target"].combine_chunks().to_pylist()
    adjacency_lists: dict[int, list[int]] = {}
    for source_id, target_id in zip(source_ids, target_ids, strict=True):
        adjacency_lists.setdefault(int(source_id), []).append(int(target_id))
    return {source_id: tuple(targets) for source_id, targets in adjacency_lists.items()}


def _endpoint_value_map_from_table(
    table: pa.Table,
    *,
    return_item: ReturnItem,
) -> dict[int, Any]:
    """Map endpoint node ids to the scalar value returned by the optimized endpoint path."""
    assert return_item.field is not None
    node_ids = table[_binding_column_name(return_item.variable, _NODE_ID_FIELD)].combine_chunks()
    values = table[_binding_column_name(return_item.variable, return_item.field)].combine_chunks()
    return {
        int(node_id): value
        for node_id, value in zip(node_ids.to_pylist(), values.to_pylist(), strict=True)
    }


def _empty_endpoint_range_projection_table(
    start_table: pa.Table,
    end_table: pa.Table,
    *,
    return_items: tuple[ReturnItem, ...],
) -> pa.Table:
    """Build an empty projected result for the endpoint-range executor."""

    columns: dict[str, pa.ChunkedArray | pa.Array] = {}
    for column_name in start_table.column_names:
        columns[column_name] = start_table[column_name].slice(0, 0)
    for column_name in end_table.column_names:
        if column_name not in columns:
            columns[column_name] = end_table[column_name].slice(0, 0)
    return _project_return_items_table(pa.table(columns), return_items)


def _expand_pattern_variants(pattern: CompiledPattern) -> tuple[CompiledPattern, ...]:
    """Expand this pattern or value into the variants required by execution."""
    if isinstance(pattern, NodePattern):
        return (pattern,)
    if not any(
        step.min_hops != 1
        or step.max_hops != 1
        or step.direction == "undirected"
        or len(step.rel_type_alternatives) > 1
        for step in pattern.steps
    ):
        return (pattern,)

    variants = _expand_path_steps(pattern.start, pattern.steps, 0)
    return tuple(variants)


def _expand_path_steps(
    start: NodePattern,
    steps: tuple[RelationshipStep, ...],
    step_index: int,
) -> list[PathPattern]:
    """Expand this pattern or value into the variants required by execution."""
    if not steps:
        return [PathPattern(start=start, steps=())]

    current = steps[0]
    tail = steps[1:]
    expanded_current_steps = _expand_relationship_step_variants(current, step_index)
    variants: list[PathPattern] = []
    for current_steps in expanded_current_steps:
        head_pattern = PathPattern(start=start, steps=current_steps)
        current_end = current_steps[-1].node
        for tail_pattern in _expand_path_steps(current_end, tail, step_index + 1):
            variants.append(
                PathPattern(
                    start=head_pattern.start,
                    steps=(*head_pattern.steps, *tail_pattern.steps),
                )
            )
    return variants


def _expand_relationship_step_variants(
    step: RelationshipStep,
    step_index: int,
) -> tuple[tuple[RelationshipStep, ...], ...]:
    """Expand this pattern or value into the variants required by execution."""
    if len(step.rel_type_alternatives) > 1:
        alternative_variants: list[tuple[RelationshipStep, ...]] = []
        for rel_type in step.rel_type_alternatives:
            alternative_variants.extend(
                _expand_relationship_step_variants(
                    _with_step_rel_type(step, rel_type),
                    step_index,
                )
            )
        return tuple(alternative_variants)
    if step.direction == "undirected":
        return (
            _with_step_direction(step, "outgoing"),
            _with_step_direction(step, "incoming"),
        )
    if step.min_hops == 1 and step.max_hops == 1:
        return ((step,),)

    variants: list[tuple[RelationshipStep, ...]] = []
    for hop_count in range(step.min_hops, step.max_hops + 1):
        expanded_steps: list[RelationshipStep] = []
        for hop_index in range(1, hop_count + 1):
            is_last_hop = hop_index == hop_count
            node = (
                step.node
                if is_last_hop
                else NodePattern(
                    variable=f"{_INTERNAL_VARIABLE_PREFIX}{step_index}_{hop_count}_{hop_index}",
                    labels=(),
                    properties={},
                )
            )
            expanded_steps.append(
                RelationshipStep(
                    variable=step.variable if is_last_hop else None,
                    rel_type=step.rel_type,
                    properties=step.properties,
                    node=node,
                    direction=step.direction,
                    min_hops=1,
                    max_hops=1,
                    rel_type_alternatives=(),
                )
            )
        variants.append(tuple(expanded_steps))
    return tuple(variants)


def _with_step_direction(
    step: RelationshipStep,
    direction: str,
) -> tuple[RelationshipStep, ...]:
    """Return a fixed-hop step variant with a concrete traversal direction."""

    return (
        RelationshipStep(
            variable=step.variable,
            rel_type=step.rel_type,
            properties=step.properties,
            node=step.node,
            direction=direction,
            min_hops=step.min_hops,
            max_hops=step.max_hops,
            rel_type_alternatives=step.rel_type_alternatives,
        ),
    )


def _with_step_rel_type(
    step: RelationshipStep,
    rel_type: str,
) -> RelationshipStep:
    """Return a step variant constrained to one relationship type."""

    return RelationshipStep(
        variable=step.variable,
        rel_type=rel_type,
        properties=step.properties,
        node=step.node,
        direction=step.direction,
        min_hops=step.min_hops,
        max_hops=step.max_hops,
        rel_type_alternatives=(),
    )


def _build_query_result(
    return_items: tuple[ReturnItem, ...],
    bindings: list[dict[str, Any]],
) -> QueryResult:
    """Build the intermediate plan, table, or result used by later stages."""
    rows: list[dict[str, Any]] = []
    for binding in bindings:
        row: dict[str, Any] = {}
        for item in return_items:
            row[item.expression] = _project_value(binding[item.variable], item.field)
        rows.append(row)
    return QueryResult(
        columns=tuple(item.expression for item in return_items),
        rows=tuple(rows),
    )


def _build_query_result_from_table(
    return_items: tuple[ReturnItem, ...],
    bindings: pa.Table,
) -> QueryResult:
    # Result shaping happens after Arrow evaluation: the executor keeps internal
    # binding columns columnar for joins/filters, then projects public node maps
    # or scalar properties only at the boundary returned to REST/Bolt/Python.
    """Build the intermediate plan, table, or result used by later stages."""
    projected_values: list[tuple[str, list[Any]]] = []
    for item in return_items:
        if item.scalar_expression is not None:
            values = _evaluate_scalar_expression_array(bindings, item.scalar_expression).to_pylist()
        elif item.field is None:
            values = _project_bound_entity_values_from_table(bindings, item.variable)
        else:
            values = _project_bound_field_values_from_table(bindings, item.variable, item.field)
        projected_values.append((item.expression, values))

    rows: list[dict[str, Any]] = []
    for index in range(bindings.num_rows):
        row = {expression: values[index] for expression, values in projected_values}
        rows.append(row)
    return QueryResult(
        columns=tuple(item.expression for item in return_items),
        rows=tuple(rows),
    )


def _project_return_items_table(
    table: pa.Table,
    return_items: tuple[ReturnItem, ...],
) -> pa.Table:
    """Project bindings into the columns or values needed by the next step."""
    columns: dict[str, pa.ChunkedArray | pa.Array] = {}
    for item in return_items:
        if item.scalar_expression is not None:
            columns[_binding_column_name(item.expression, "value")] = (
                _evaluate_scalar_expression_array(table, item.scalar_expression)
            )
            continue
        if item.field is None:
            prefix = f"{item.variable}."
            entity_columns = [
                column_name for column_name in table.column_names if column_name.startswith(prefix)
            ]
            if not entity_columns:
                raise QueryError(f"RETURN references an unknown binding: {item.variable}")
            for column_name in entity_columns:
                columns[column_name] = table[column_name]
            continue
        resolved = _resolve_table_field_array(
            table,
            variable=item.variable,
            field=item.field,
        )
        if resolved is None:
            column_name = _binding_column_name(item.variable, item.field)
            raise QueryError(f"RETURN references an unknown binding: {column_name}")
        columns[_binding_column_name(item.variable, item.field)] = resolved
    return pa.table(columns)


def _build_aggregate_query_result_from_table(
    return_items: tuple[ReturnItem, ...],
    bindings: pa.Table,
) -> QueryResult:
    """Build the intermediate plan, table, or result used by later stages."""
    row: dict[str, Any] = {}
    for item in return_items:
        if item.aggregate not in {"count", "sum", "avg", "min", "max", "collect"}:
            raise QueryError(f"Unsupported aggregate: {item.aggregate}")
        if item.count_all:
            row[item.expression] = bindings.num_rows
            continue

        field = _ENTITY_ID_FIELD if item.field is None else item.field
        column = _resolve_table_field_array(bindings, variable=item.variable, field=field)
        if column is None:
            column_name = _binding_column_name(item.variable, field)
            raise QueryError(f"COUNT references an unknown binding: {column_name}")
        if item.aggregate_distinct:
            row[item.expression] = (
                _collect_column_values(column, distinct=True)
                if item.aggregate == "collect"
                else _count_distinct_non_null(column)
            )
            continue
        row[item.expression] = _apply_aggregate_transform(
            _aggregate_column_value(column, item.aggregate),
            item.aggregate_transform,
        )
    return QueryResult(
        columns=tuple(item.expression for item in return_items),
        rows=(row,),
    )


def _build_grouped_aggregate_query_result(
    return_items: tuple[ReturnItem, ...],
    bindings: pa.Table,
) -> QueryResult:
    """Project grouped aggregate tables back into public Cypher result rows."""
    source_rows = bindings.to_pylist()
    rows: list[dict[str, Any]] = []
    for record in source_rows:
        row: dict[str, Any] = {}
        for item in return_items:
            if item.aggregate is not None:
                value = _resolve_record_field_value(
                    record,
                    variable=item.expression,
                    field="value",
                )
                row[item.expression] = (
                    _deduplicate_list_values(value)
                    if item.aggregate == "collect" and item.aggregate_distinct
                    else value
                )
                continue
            if item.field is None:
                row[item.expression] = _project_bound_entity_from_row(record, item.expression)
                continue
            value = _resolve_record_field_value(
                record,
                variable=item.expression,
                field="value",
            )
            row[item.expression] = (
                None
                if value is None
                else _decode_labels(value)
                if item.field == "labels"
                else _decode_join_json_value(value)
            )
        rows.append(row)
    return QueryResult(
        columns=tuple(item.expression for item in return_items),
        rows=tuple(rows),
    )


def _aggregate_return_items(return_items: tuple[ReturnItem, ...]) -> tuple[ReturnItem, ...]:
    """Return only aggregate projections from a RETURN item list."""
    return tuple(item for item in return_items if item.aggregate is not None)


def _grouping_items_for_aggregate_return(
    return_items: tuple[ReturnItem, ...],
) -> tuple[WithItem, ...]:
    """Convert non-aggregate RETURN items into implicit grouping keys."""
    grouping_items: list[WithItem] = []
    for item in return_items:
        if item.aggregate is not None:
            continue
        grouping_items.append(
            WithItem(
                output_name=item.expression,
                source_variable=item.variable,
                source_field=item.field,
                source_kind="entity" if item.field is None else "scalar",
            )
        )
    return tuple(grouping_items)


def _build_aggregate_table_from_table(
    grouping_items: tuple[WithItem, ...],
    return_items: tuple[ReturnItem, ...],
    bindings: pa.Table,
) -> pa.Table:
    """Build the intermediate plan, table, or result used by later stages."""
    if not grouping_items:
        payload = _build_aggregate_query_result_from_table(return_items, bindings)
        row = payload.rows[0] if payload.rows else {}
        return pa.table(
            {
                _binding_column_name(item.expression, "value"): pa.array([row[item.expression]])
                for item in return_items
            }
        )

    if bindings.num_rows == 0:
        grouping_projection = _empty_grouping_projection_table(grouping_items)
        return _empty_grouped_aggregate_table(grouping_projection, return_items)

    grouping_projection = _encode_nested_grouping_columns(
        _project_with_items_table(bindings, grouping_items)
    )
    if grouping_projection.num_rows == 0:
        return _empty_grouped_aggregate_table(grouping_projection, return_items)
    aggregate_input_columns: dict[str, pa.ChunkedArray | pa.Array] = {
        column_name: grouping_projection[column_name]
        for column_name in grouping_projection.column_names
    }
    aggregate_specs: list[tuple[str, str]] = []
    aggregate_output_columns: list[tuple[str, str]] = []
    resolved_arrays: dict[str, pa.ChunkedArray | pa.Array] = {}
    count_all_array: pa.Array | None = None
    for index, item in enumerate(return_items):
        if item.aggregate not in {"count", "sum", "avg", "min", "max", "collect"}:
            raise QueryError(f"Unsupported aggregate: {item.aggregate}")
        if item.count_all:
            temp_name = f"__agg__.{index}"
            if count_all_array is None:
                count_all_array = pa.array([1] * bindings.num_rows, type=pa.int8())
            aggregate_input_columns[temp_name] = count_all_array
            aggregate_specs.append((temp_name, "sum"))
            aggregate_output_columns.append((f"{temp_name}_sum", item.expression))
            continue

        source_column = _aggregate_source_column_name(item)
        cache_key = _binding_column_name(item.variable, source_column)
        resolved = resolved_arrays.get(cache_key)
        if resolved is None:
            resolved = _resolve_table_field_array(
                bindings,
                variable=item.variable,
                field=source_column,
            )
        if resolved is None:
            resolved = _null_array_for_missing_bound_field(bindings, item.variable)
            if resolved is None:
                column_name = _binding_column_name(item.variable, source_column)
                raise QueryError(f"COUNT references an unknown binding: {column_name}")
        resolved_arrays[cache_key] = resolved
        temp_name = f"__agg__.{index}"
        aggregate_input_columns[temp_name] = resolved
        function_name = _arrow_group_by_aggregate_name(item)
        aggregate_specs.append((temp_name, function_name))
        aggregate_output_columns.append((f"{temp_name}_{function_name}", item.expression))

    aggregate_input = pa.table(aggregate_input_columns)
    aggregated = aggregate_input.group_by(grouping_projection.column_names).aggregate(
        aggregate_specs
    )
    output_columns: dict[str, pa.ChunkedArray | pa.Array] = {
        column_name: aggregated[column_name] for column_name in grouping_projection.column_names
    }
    transform_by_expression = {
        item.expression: item.aggregate_transform
        for item in return_items
        if item.aggregate_transform is not None
    }
    for source_name, expression in aggregate_output_columns:
        values = aggregated[source_name]
        transform = transform_by_expression.get(expression)
        if transform is not None:
            values = _apply_aggregate_transform_array(values, transform)
        output_columns[_binding_column_name(expression, "value")] = values
    return pa.table(output_columns)


def _aggregate_column_value(
    column: pa.ChunkedArray | pa.Array,
    aggregate: str | None,
) -> object:
    """Compute a scalar aggregate value over an Arrow column."""

    if aggregate == "count":
        return int(pc.sum(pc.is_valid(column)).as_py() or 0)
    if aggregate == "sum":
        return pc.sum(column).as_py()
    if aggregate == "avg":
        return pc.mean(column).as_py()
    if aggregate == "min":
        return pc.min(column).as_py()
    if aggregate == "max":
        return pc.max(column).as_py()
    if aggregate == "collect":
        return _collect_column_values(column, distinct=False)
    raise QueryError(f"Unsupported aggregate: {aggregate}")


def _collect_column_values(
    column: pa.ChunkedArray | pa.Array,
    *,
    distinct: bool,
) -> list[object]:
    """Collect non-null Arrow values into a Cypher list value."""

    values = [_decode_join_json_value(value) for value in column.to_pylist() if value is not None]
    return _deduplicate_list_values(values) if distinct else values


def _deduplicate_list_values(value: object) -> list[object]:
    """Deduplicate collected list values while preserving their first-seen order."""

    if value is None:
        return []
    if not isinstance(value, list):
        return [value]
    seen: set[str] = set()
    deduplicated: list[object] = []
    for item in value:
        key = json.dumps(item, sort_keys=True)
        if key in seen:
            continue
        seen.add(key)
        deduplicated.append(item)
    return deduplicated


def _arrow_group_by_aggregate_name(item: ReturnItem) -> str:
    """Map the internal aggregate name to the Arrow group-by aggregate name."""

    if item.aggregate == "count":
        return "count_distinct" if item.aggregate_distinct else "count"
    if item.aggregate == "avg":
        return "mean"
    if item.aggregate in {"sum", "min", "max"}:
        return item.aggregate
    if item.aggregate == "collect":
        return "list"
    raise QueryError(f"Unsupported aggregate: {item.aggregate}")


def _apply_aggregate_transform(value: object, transform: str | None) -> object:
    """Apply a supported post-aggregate scalar transform."""

    if transform is None:
        return value
    if transform == "round_2":
        if value is None:
            return None
        if not isinstance(value, int | float):
            raise QueryError("round(...) requires a numeric aggregate value.")
        return round(float(value), 2)
    raise QueryError(f"Unsupported aggregate transform: {transform}")


def _apply_aggregate_transform_array(
    values: pa.ChunkedArray | pa.Array,
    transform: str,
) -> pa.ChunkedArray | pa.Array:
    """Apply a supported post-aggregate transform to an Arrow column."""

    if transform == "round_2":
        return pc.round(values, ndigits=2)
    raise QueryError(f"Unsupported aggregate transform: {transform}")


def _encode_nested_grouping_columns(table: pa.Table) -> pa.Table:
    """Encode nested grouping keys into stable strings before Arrow group_by."""
    updated = table
    for index, field in enumerate(table.schema):
        if not (
            pa.types.is_list(field.type)
            or pa.types.is_large_list(field.type)
            or pa.types.is_struct(field.type)
            or pa.types.is_map(field.type)
        ):
            continue
        encoded = pa.array(
            [
                None if value is None else f"{_JOIN_JSON_PREFIX}{json.dumps(value)}"
                for value in table[field.name].to_pylist()
            ],
            type=pa.string(),
        )
        updated = updated.set_column(index, field.name, encoded)
    return updated


def _empty_grouped_aggregate_table(
    grouping_projection: pa.Table,
    aggregate_items: tuple[ReturnItem, ...],
) -> pa.Table:
    """Build an empty typed result used by edge cases and fallback paths."""
    columns: dict[str, pa.Array] = {}
    for column_name in grouping_projection.column_names:
        columns[column_name] = pa.array([], type=grouping_projection[column_name].type)
    for item in aggregate_items:
        value_type = pa.list_(pa.null()) if item.aggregate == "collect" else pa.int64()
        columns[_binding_column_name(item.expression, "value")] = pa.array([], type=value_type)
    return pa.table(columns)


def _empty_grouping_projection_table(grouping_items: tuple[WithItem, ...]) -> pa.Table:
    """Build the grouping schema for grouped aggregates over an empty match result."""
    columns: dict[str, pa.Array] = {}
    for item in grouping_items:
        if item.source_kind == "entity":
            continue
        columns[_binding_column_name(item.output_name, "value")] = pa.array([], type=pa.null())
    return pa.table(columns)


def _contains_aggregates(return_items: tuple[ReturnItem, ...]) -> bool:
    """Support the current Cypher compilation or execution step."""
    return any(item.aggregate is not None for item in return_items)


def _can_apply_projected_distinct_window(
    return_items: tuple[ReturnItem, ...],
    order_by: tuple[OrderByItem, ...] | None,
) -> bool:
    """Return true when DISTINCT/ORDER/LIMIT can run on projected scalar columns."""

    if not return_items:
        return False
    projected_fields: set[tuple[str, str]] = set()
    for item in return_items:
        if item.scalar_expression is not None or item.aggregate is not None or item.field is None:
            return False
        projected_fields.add((item.variable, item.field))
    if order_by is None:
        return True
    return all((item.variable, item.field) in projected_fields for item in order_by)


def _distinct_endpoint_range_shape(
    query: CompiledMatchQuery,
) -> DistinctEndpointRangeShape | None:
    """Detect bounded variable-length endpoint queries that can avoid full path materialization."""
    if not query.distinct or len(query.patterns) != 1 or len(query.return_items) != 1:
        return None
    if query.skip is not None:
        return None
    return_item = query.return_items[0]
    if (
        return_item.field is None
        or return_item.scalar_expression is not None
        or return_item.aggregate is not None
    ):
        return None
    if query.order_by is not None and not all(
        item.variable == return_item.variable
        and item.field == return_item.field
        and not item.descending
        for item in query.order_by
    ):
        return None
    pattern = query.patterns[0]
    if not isinstance(pattern, PathPattern) or len(pattern.steps) != 1:
        return None
    step = pattern.steps[0]
    if (
        step.min_hops == 1
        and step.max_hops == 1
        or step.variable is not None
        or step.direction != "outgoing"
        or step.rel_type is None
        or len(step.rel_type_alternatives) > 1
        or step.properties
        or pattern.start.variable == step.node.variable
        or return_item.variable != step.node.variable
    ):
        return None
    if step.max_hops > 5:
        return None
    return (pattern, return_item)


def _match_with_query_has_exists_pattern(query: CompiledMatchWithQuery) -> bool:
    """Return true when a MATCH/WITH query uses EXISTS { pattern } predicates."""

    return any(
        _predicate_has_exists_pattern(predicate)
        for predicate in (
            query.match_where_predicate,
            query.with_where_predicate,
            query.post_with_match_where_predicate,
        )
    )


def _match_with_query_has_relationship_type_alternatives(query: CompiledMatchWithQuery) -> bool:
    """Return true when a MATCH/WITH query contains relationship type alternatives."""

    return any(
        _patterns_have_relationship_type_alternatives(patterns)
        for patterns in (
            query.match_patterns,
            query.post_with_match_patterns,
        )
    )


def _patterns_have_relationship_type_alternatives(
    patterns: tuple[CompiledPattern, ...],
) -> bool:
    """Check whether any path step still carries relationship type alternatives."""

    for pattern in patterns:
        if isinstance(pattern, NodePattern):
            continue
        if any(len(step.rel_type_alternatives) > 1 for step in pattern.steps):
            return True
    return False


def _match_requires_edges(
    patterns: tuple[CompiledPattern, ...], predicate: WherePredicate | None
) -> bool:
    """Check for relationships in the match patterns or in a WHERE subquery."""
    return any(isinstance(pattern, PathPattern) for pattern in patterns) or (
        _predicate_has_exists_pattern(predicate)
    )


def _predicate_has_exists_pattern(predicate: WherePredicate | None) -> bool:
    """Check whether a predicate tree contains an existential pattern subquery."""

    if predicate is None:
        return False
    if isinstance(predicate, ExistsPatternPredicate):
        return True
    if isinstance(predicate, NotPredicate):
        return _predicate_has_exists_pattern(predicate.predicate)
    if isinstance(predicate, AndPredicate | OrPredicate):
        return any(_predicate_has_exists_pattern(child) for child in predicate.predicates)
    return False


def _build_match_logical_plan(query: CompiledMatchQuery) -> MatchLogicalPlan | None:
    """Build the intermediate plan, table, or result used by later stages."""
    if len(query.patterns) != 1:
        return None
    if query.distinct and not _can_plan_return_distinct(query.return_items, query.order_by):
        return None

    pushed_patterns, residual_predicate = _push_down_match_where_predicate(
        query.patterns,
        query.where_predicate,
    )
    pushed_pattern = pushed_patterns[0]

    required_fields = _required_fields_for_match(
        pushed_patterns,
        residual_predicate,
        return_items=query.return_items,
        order_by=query.order_by,
    )
    root = _build_match_scan_plan(
        pushed_pattern,
        required_fields,
        where_predicate=residual_predicate,
    )
    if root is None:
        return None
    residual_predicate = _residual_predicate_after_bound_pushdowns(
        pushed_pattern,
        residual_predicate,
    )
    if residual_predicate is not None:
        root = FilterPlan(input=root, predicate=residual_predicate)

    if _contains_aggregates(query.return_items):
        if _grouping_items_for_aggregate_return(query.return_items):
            return None
        if (
            query.distinct
            or query.order_by is not None
            or query.skip is not None
            or query.limit is not None
        ):
            return None
        root = AggregateCountPlan(
            input=root,
            return_items=cast(tuple[object, ...], query.return_items),
        )
        return MatchLogicalPlan(
            root=root,
            planned_order_window=False,
            debug_name="match-aggregate-count",
        )

    if query.distinct:
        root = ReturnProjectPlan(
            input=root,
            return_items=cast(tuple[object, ...], query.return_items),
        )
        root = DistinctPlan(input=root)
    else:
        project_columns = _project_columns_for_pattern(pushed_pattern, required_fields)
        if project_columns is not None:
            root = ProjectPlan(input=root, columns=project_columns)

    planned_order_window = (
        query.order_by is not None or query.skip is not None or query.limit is not None
    )
    if query.order_by is not None:
        root = SortPlan(input=root, order_by=cast(tuple[object, ...], query.order_by))
    if query.skip is not None or query.limit is not None:
        root = SlicePlan(input=root, skip=query.skip, limit=query.limit)

    return MatchLogicalPlan(
        root=root,
        planned_order_window=planned_order_window,
        planned_distinct=query.distinct,
    )


def _build_match_with_logical_plan(query: CompiledMatchWithQuery) -> MatchLogicalPlan | None:
    """Build the intermediate plan, table, or result used by later stages."""
    if (
        query.match_optional
        or query.pre_with_unwind is not None
        or query.post_with_match_patterns
        or query.post_with_unwind is not None
        or query.post_with_create_patterns
        or query.post_with_create_relationship_patterns
        or query.post_with_merge_node_pattern is not None
        or query.post_with_merge_relationship_pattern is not None
        or query.post_with_set_items
        or query.post_with_delete_items
        or len(query.match_patterns) != 1
    ):
        return None

    required_fields = _required_fields_for_match(
        query.match_patterns,
        query.match_where_predicate,
        return_items=query.with_aggregate_items,
        order_by=None,
    )
    _collect_required_fields_from_with_items(query.with_items, required_fields)
    root = _build_scan_filter_plan(
        query.match_patterns,
        query.match_where_predicate,
        required_fields=required_fields,
    )
    if root is None:
        return None
    with_input_columns = _project_columns_for_with_stage_input(
        query.with_items,
        query.with_aggregate_items,
    )
    if with_input_columns is not None:
        root = ProjectPlan(input=root, columns=with_input_columns)

    if query.with_aggregate_items:
        root = WithAggregatePlan(
            input=root,
            grouping_items=cast(tuple[object, ...], query.with_items),
            aggregate_items=cast(tuple[object, ...], query.with_aggregate_items),
        )
    else:
        root = WithProjectPlan(
            input=root,
            with_items=cast(tuple[object, ...], query.with_items),
        )
    if query.with_distinct:
        root = DistinctPlan(input=root)
    if query.with_where_predicate is not None:
        root = FilterPlan(input=root, predicate=query.with_where_predicate)
    if query.with_order_by is not None:
        root = SortPlan(input=root, order_by=cast(tuple[object, ...], query.with_order_by))
    if query.with_skip is not None or query.with_limit is not None:
        root = SlicePlan(input=root, skip=query.with_skip, limit=query.with_limit)

    planned_order_window = (
        query.return_order_by is not None
        or query.return_skip is not None
        or query.return_limit is not None
    )
    if query.return_order_by is not None:
        root = SortPlan(input=root, order_by=cast(tuple[object, ...], query.return_order_by))
    if query.return_skip is not None or query.return_limit is not None:
        root = SlicePlan(input=root, skip=query.return_skip, limit=query.return_limit)

    return MatchLogicalPlan(
        root=root,
        planned_order_window=planned_order_window,
        debug_name=("match-with-aggregate" if query.with_aggregate_items else "match-with-project"),
    )


def _build_optional_match_logical_plan(
    query: CompiledOptionalMatchQuery,
) -> MatchLogicalPlan | None:
    """Build the intermediate plan, table, or result used by later stages."""
    if _contains_aggregates(query.return_items):
        return None
    if query.distinct and any(item.field is not None for item in query.return_items):
        return None
    if query.distinct and not _can_plan_return_distinct(query.return_items, query.order_by):
        return None
    required_fields = _required_fields_for_match(
        query.patterns,
        query.where_predicate,
        return_items=query.return_items,
        order_by=query.order_by,
    )
    root = _build_optional_root_plan(
        patterns=query.patterns,
        where_predicate=query.where_predicate,
        required_fields=required_fields,
    )
    if root is None:
        return None
    root = _wrap_match_output_plan(
        root,
        patterns=query.patterns,
        required_fields=required_fields,
        return_items=query.return_items,
        distinct=query.distinct,
        order_by=query.order_by,
        skip=query.skip,
        limit=query.limit,
    )
    return MatchLogicalPlan(
        root=root,
        planned_order_window=(
            query.order_by is not None or query.skip is not None or query.limit is not None
        ),
        planned_distinct=query.distinct,
        debug_name="optional-match",
    )


def _build_match_optional_logical_plan(
    query: CompiledMatchOptionalQuery,
) -> MatchLogicalPlan | None:
    """Build the intermediate plan, table, or result used by later stages."""
    if _contains_aggregates(query.return_items):
        return None
    if query.distinct and any(item.field is not None for item in query.return_items):
        return None
    if query.distinct and not _can_plan_return_distinct(query.return_items, query.order_by):
        return None
    match_required_fields = _required_fields_for_optional_match(
        query.match_patterns,
        query.match_where_predicate,
        query.optional_patterns,
        query.optional_where_predicate,
        return_items=query.return_items,
        order_by=query.order_by,
    )
    optional_required_fields = _required_fields_for_match(
        query.optional_patterns,
        query.optional_where_predicate,
        return_items=query.return_items,
        order_by=query.order_by,
    )
    match_root = _build_scan_filter_plan(
        query.match_patterns,
        query.match_where_predicate,
        required_fields=match_required_fields,
    )
    if match_root is None or len(query.optional_patterns) != 1:
        return None
    match_root = _push_window_before_optional_match(
        match_root,
        match_patterns=query.match_patterns,
        order_by=query.order_by,
        skip=query.skip,
        limit=query.limit,
        distinct=query.distinct,
    )
    root: LogicalPlan = OptionalMatchPlan(
        input=match_root,
        patterns=cast(tuple[object, ...], query.optional_patterns),
        where_predicate=query.optional_where_predicate,
        required_fields=dict(optional_required_fields),
        null_columns=None,
    )
    root = _wrap_match_output_plan(
        root,
        patterns=query.match_patterns + query.optional_patterns,
        required_fields=match_required_fields,
        return_items=query.return_items,
        distinct=query.distinct,
        order_by=query.order_by,
        skip=query.skip,
        limit=query.limit,
    )
    return MatchLogicalPlan(
        root=root,
        planned_order_window=(
            query.order_by is not None or query.skip is not None or query.limit is not None
        ),
        planned_distinct=query.distinct,
        debug_name="match-optional",
    )


def _cypher_planner_enabled() -> bool:
    """Support the current Cypher compilation or execution step."""
    value = os.getenv(_DISABLE_PLANNER_ENV, "").strip().lower()
    return value not in {"1", "true", "yes", "on"}


def _build_optional_root_plan(
    *,
    patterns: tuple[CompiledPattern, ...],
    where_predicate: WherePredicate | None,
    required_fields: Mapping[str, set[str]],
) -> LogicalPlan | None:
    """Build the intermediate plan, table, or result used by later stages."""
    if len(patterns) != 1:
        return None
    return OptionalMatchPlan(
        input=None,
        patterns=cast(tuple[object, ...], patterns),
        where_predicate=where_predicate,
        required_fields=dict(required_fields),
        null_columns=tuple(_pattern_null_columns(patterns)),
    )


def _build_scan_filter_plan(
    patterns: tuple[CompiledPattern, ...],
    where_predicate: WherePredicate | None,
    *,
    required_fields: Mapping[str, set[str]],
) -> LogicalPlan | None:
    """Build the intermediate plan, table, or result used by later stages."""
    if len(patterns) != 1:
        return None
    pushed_patterns, residual_predicate = _push_down_match_where_predicate(
        patterns,
        where_predicate,
    )
    root = _build_match_scan_plan(
        pushed_patterns[0],
        required_fields,
        where_predicate=residual_predicate,
    )
    if root is None:
        return None
    residual_predicate = _residual_predicate_after_bound_pushdowns(
        pushed_patterns[0],
        residual_predicate,
    )
    if residual_predicate is not None:
        root = FilterPlan(input=root, predicate=residual_predicate)
    return root


def _wrap_match_output_plan(
    root: LogicalPlan,
    *,
    patterns: tuple[CompiledPattern, ...],
    required_fields: Mapping[str, set[str]],
    return_items: tuple[ReturnItem, ...],
    distinct: bool,
    order_by: tuple[OrderByItem, ...] | None,
    skip: int | None,
    limit: int | None,
) -> LogicalPlan:
    """Support the current Cypher compilation or execution step."""
    if distinct:
        root = ReturnProjectPlan(
            input=root,
            return_items=cast(tuple[object, ...], return_items),
        )
        root = DistinctPlan(input=root)
    else:
        project_columns = _project_columns_for_patterns(patterns, required_fields)
        if project_columns is not None:
            root = ProjectPlan(input=root, columns=project_columns)
    if order_by is not None:
        root = SortPlan(input=root, order_by=cast(tuple[object, ...], order_by))
    if skip is not None or limit is not None:
        root = SlicePlan(input=root, skip=skip, limit=limit)
    return root


def _push_window_before_optional_match(
    root: LogicalPlan,
    *,
    match_patterns: tuple[CompiledPattern, ...],
    order_by: tuple[OrderByItem, ...] | None,
    skip: int | None,
    limit: int | None,
    distinct: bool,
) -> LogicalPlan:
    """Support the current Cypher compilation or execution step."""
    if distinct or order_by is None or limit is None:
        return root
    allowed_variables = {
        variable for pattern in match_patterns for variable in _pattern_variables(pattern)
    }
    if any(item.variable not in allowed_variables for item in order_by):
        return root
    root = SortPlan(input=root, order_by=cast(tuple[object, ...], order_by))
    return SlicePlan(input=root, skip=None, limit=(skip or 0) + limit)


def _build_match_scan_plan(
    pattern: CompiledPattern,
    required_fields: Mapping[str, set[str]],
    *,
    where_predicate: WherePredicate | None = None,
) -> LogicalPlan | None:
    """Build the intermediate plan, table, or result used by later stages."""
    if isinstance(pattern, NodePattern):
        return NodeScanPlan(
            variable=pattern.variable,
            labels=pattern.labels,
            properties=pattern.properties,
            requested_fields=_requested_fields_for_variable(required_fields, pattern.variable),
            pushdown_predicate=_pushdown_predicate_for_bound_variable(
                where_predicate,
                pattern.variable,
            ),
        )
    root: LogicalPlan = NodeScanPlan(
        variable=pattern.start.variable,
        labels=pattern.start.labels,
        properties=pattern.start.properties,
        requested_fields=_requested_fields_for_variable(
            required_fields,
            pattern.start.variable,
        ),
        pushdown_predicate=_pushdown_predicate_for_bound_variable(
            where_predicate,
            pattern.start.variable,
        ),
    )
    seen_variables = {pattern.start.variable}
    seen_edge_variables: tuple[str, ...] = ()
    current_variable = pattern.start.variable
    for index, step in enumerate(pattern.steps):
        if step.min_hops != 1 or step.max_hops != 1:
            return None
        if step.direction not in {"outgoing", "incoming"}:
            return None
        edge_variable = step.variable or f"_edge_{index}"
        if edge_variable in seen_variables or step.node.variable in seen_variables:
            return None
        seen_variables.add(edge_variable)
        seen_variables.add(step.node.variable)
        root = ExpandPlan(
            input=root,
            start_variable=current_variable,
            edge_variable=edge_variable,
            previous_edge_variables=seen_edge_variables,
            direction=step.direction,
            rel_type=step.rel_type,
            rel_type_alternatives=step.rel_type_alternatives,
            edge_properties=step.properties,
            edge_requested_fields=_requested_fields_for_edge_step(
                required_fields,
                edge_variable,
                step,
            ),
            end_variable=step.node.variable,
            end_labels=step.node.labels,
            end_properties=step.node.properties,
            end_requested_fields=_requested_fields_for_variable(
                required_fields,
                step.node.variable,
            ),
            edge_pushdown_predicate=_pushdown_predicate_for_bound_variable(
                where_predicate,
                edge_variable,
            ),
            end_pushdown_predicate=_pushdown_predicate_for_bound_variable(
                where_predicate,
                step.node.variable,
            ),
        )
        seen_edge_variables = (*seen_edge_variables, edge_variable)
        current_variable = step.node.variable
    return root


def _project_columns_for_pattern(
    pattern: CompiledPattern,
    required_fields: Mapping[str, set[str]],
) -> tuple[str, ...] | None:
    """Project bindings into the columns or values needed by the next step."""
    return _project_columns_for_patterns((pattern,), required_fields)


def _project_columns_for_patterns(
    patterns: tuple[CompiledPattern, ...],
    required_fields: Mapping[str, set[str]],
) -> tuple[str, ...] | None:
    """Project bindings into the columns or values needed by the next step."""
    roles: list[tuple[str, bool]] = []
    for pattern in patterns:
        if isinstance(pattern, NodePattern):
            roles.append((pattern.variable, True))
        else:
            roles.append((pattern.start.variable, True))
            for index, step in enumerate(pattern.steps):
                roles.append((step.variable or f"_edge_{index}", False))
                roles.append((step.node.variable, True))

    columns: list[str] = []
    projected_any = False
    for variable, is_node in roles:
        requested_fields = _requested_fields_for_variable(required_fields, variable)
        if requested_fields is None:
            return None
        columns.extend(
            _project_columns_for_required_fields(
                variable,
                requested_fields,
                is_node=is_node,
            )
        )
        projected_any = True
    if not projected_any:
        return None
    return tuple(dict.fromkeys(columns))


def _project_columns_for_required_fields(
    variable: str,
    requested_fields: set[str] | None,
    *,
    is_node: bool,
) -> tuple[str, ...]:
    """Project bindings into the columns or values needed by the next step."""
    if requested_fields is None:
        return ()
    reserved = (
        {_NODE_ID_FIELD, _ENTITY_ID_FIELD, "__join_id", "labels"}
        if is_node
        else {"id", _ENTITY_ID_FIELD, _NODE_ID_FIELD, "type", "source", "target"}
    )
    id_field = _NODE_ID_FIELD if is_node else "id"
    columns = [_binding_column_name(variable, id_field)]
    if _ENTITY_ID_FIELD in requested_fields and _ENTITY_ID_FIELD != id_field:
        columns.append(_binding_column_name(variable, _ENTITY_ID_FIELD))
    if is_node:
        columns.append(_binding_column_name(variable, "labels"))
    else:
        columns.extend(
            [
                _binding_column_name(variable, "type"),
                _binding_column_name(variable, "source"),
                _binding_column_name(variable, "target"),
            ]
        )
    projected_fields = sorted(field for field in requested_fields if field not in reserved)
    for field in projected_fields:
        columns.append(_binding_column_name(variable, field))
    return tuple(columns)


def _project_columns_for_with_stage_input(
    with_items: tuple[WithItem, ...],
    aggregate_items: tuple[ReturnItem, ...],
) -> tuple[str, ...] | None:
    """Project bindings into the columns or values needed by the next step."""
    columns: list[str] = []
    for with_item in with_items:
        if with_item.source_kind == "entity":
            return None
        if with_item.source_field is None:
            continue
        columns.append(_binding_column_name(with_item.source_variable, with_item.source_field))
    for aggregate_item in aggregate_items:
        if aggregate_item.count_all:
            continue
        source_column = _aggregate_source_column_name(aggregate_item)
        if source_column == _ENTITY_ID_FIELD:
            columns.append(_binding_column_name(aggregate_item.variable, _NODE_ID_FIELD))
            columns.append(_binding_column_name(aggregate_item.variable, "id"))
            continue
        columns.append(
            _binding_column_name(
                aggregate_item.variable,
                source_column,
            )
        )
    if not columns:
        return None
    return tuple(dict.fromkeys(columns))


def _push_down_match_where_predicate(
    patterns: tuple[CompiledPattern, ...],
    predicate: WherePredicate | None,
) -> tuple[tuple[CompiledPattern, ...], WherePredicate | None]:
    """Support the current Cypher compilation or execution step."""
    if predicate is None:
        return patterns, None

    node_variables = _node_variables_in_patterns(patterns)
    if not node_variables:
        return patterns, predicate

    pushable_predicates: list[Comparison] = []
    residual_predicates: list[WherePredicate] = []
    for conjunct in _flatten_and_predicates(predicate):
        if _is_pushdown_node_equality(conjunct, node_variables):
            pushable_predicates.append(cast(Comparison, conjunct))
            continue
        residual_predicates.append(conjunct)

    if not pushable_predicates:
        return patterns, predicate

    properties_by_variable: dict[str, dict[str, object]] = {}
    for comparison in pushable_predicates:
        variable_properties = properties_by_variable.setdefault(comparison.variable, {})
        if comparison.field in variable_properties:
            # Preserve conflicting or duplicated semantics in the residual path.
            residual_predicates.append(comparison)
            continue
        variable_properties[comparison.field] = comparison.value

    pushed_patterns = tuple(
        _pattern_with_pushed_properties(pattern, properties_by_variable) for pattern in patterns
    )
    return pushed_patterns, _combine_and_predicates(tuple(residual_predicates))


def _pushdown_predicate_for_bound_variable(
    predicate: WherePredicate | None,
    variable: str,
) -> WherePredicate | None:
    """Extract safe single-variable filters once a traversal variable is bound."""

    if predicate is None:
        return None
    pushable = tuple(
        conjunct
        for conjunct in _flatten_and_predicates(predicate)
        if _is_single_variable_pushdown_predicate(conjunct, variable)
    )
    return _combine_and_predicates(pushable)


def _residual_predicate_after_bound_pushdowns(
    pattern: CompiledPattern,
    predicate: WherePredicate | None,
) -> WherePredicate | None:
    """Remove single-variable predicates already applied by scan/expand operators."""
    if predicate is None:
        return None
    pattern_variables = set(_pattern_variables(pattern))
    residual = tuple(
        conjunct
        for conjunct in _flatten_and_predicates(predicate)
        if not any(
            _is_single_variable_pushdown_predicate(conjunct, variable)
            for variable in pattern_variables
        )
    )
    return _combine_and_predicates(residual)


def _is_single_variable_pushdown_predicate(
    predicate: WherePredicate,
    variable: str,
) -> bool:
    """Return true for predicates that can be applied immediately after binding."""

    if isinstance(predicate, Comparison):
        if predicate.left_expression is not None:
            return False
        if predicate.variable != variable:
            return False
        if isinstance(predicate.value, PropertyReferenceValue | ScalarExpression):
            return False
        return True
    if isinstance(predicate, StringPredicate | ExistsPredicate | InPredicate | NullPredicate):
        return predicate.variable == variable
    return False


def _flatten_and_predicates(predicate: WherePredicate) -> tuple[WherePredicate, ...]:
    """Support the current Cypher compilation or execution step."""
    if isinstance(predicate, AndPredicate):
        flattened: list[WherePredicate] = []
        for child in predicate.predicates:
            flattened.extend(_flatten_and_predicates(child))
        return tuple(flattened)
    return (predicate,)


def _combine_and_predicates(predicates: tuple[WherePredicate, ...]) -> WherePredicate | None:
    """Support the current Cypher compilation or execution step."""
    if not predicates:
        return None
    if len(predicates) == 1:
        return predicates[0]
    return AndPredicate(predicates=predicates)


def _node_variables_in_patterns(patterns: tuple[CompiledPattern, ...]) -> set[str]:
    """Handle node-specific matching, projection, or lookup logic for Cypher."""
    variables: set[str] = set()
    for pattern in patterns:
        if isinstance(pattern, NodePattern):
            variables.add(pattern.variable)
            continue
        variables.add(pattern.start.variable)
        for step in pattern.steps:
            variables.add(step.node.variable)
    return variables


def _is_pushdown_node_equality(
    predicate: WherePredicate,
    node_variables: set[str],
) -> bool:
    """Check whether the optimized or semantically valid path applies here."""
    if not isinstance(predicate, Comparison):
        return False
    if predicate.operator != "=":
        return False
    if predicate.variable not in node_variables:
        return False
    if predicate.field in {"id", "labels", "__join_id"}:
        return False
    if _decode_function_field(predicate.field) is not None:
        return False
    return True


def _pattern_with_pushed_properties(
    pattern: CompiledPattern,
    properties_by_variable: Mapping[str, Mapping[str, object]],
) -> CompiledPattern:
    """Support the current Cypher compilation or execution step."""
    if isinstance(pattern, NodePattern):
        return _node_pattern_with_pushed_properties(pattern, properties_by_variable)

    start = _node_pattern_with_pushed_properties(pattern.start, properties_by_variable)
    steps = tuple(
        replace(
            step,
            node=_node_pattern_with_pushed_properties(step.node, properties_by_variable),
        )
        for step in pattern.steps
    )
    return replace(pattern, start=start, steps=steps)


def _node_pattern_with_pushed_properties(
    pattern: NodePattern,
    properties_by_variable: Mapping[str, Mapping[str, object]],
) -> NodePattern:
    """Handle node-specific matching, projection, or lookup logic for Cypher."""
    pushed_properties = properties_by_variable.get(pattern.variable)
    if not pushed_properties:
        return pattern
    merged_properties = dict(pattern.properties)
    for key, value in pushed_properties.items():
        if key in merged_properties:
            continue
        merged_properties[key] = value
    return replace(pattern, properties=merged_properties)


def _required_fields_for_match(
    patterns: tuple[CompiledPattern, ...],
    where_predicate: WherePredicate | None,
    *,
    return_items: tuple[ReturnItem, ...],
    order_by: tuple[OrderByItem, ...] | None,
) -> dict[str, set[str]]:
    """Compute the fields or identifiers needed by downstream execution."""
    required: dict[str, set[str]] = {}
    for pattern in patterns:
        _collect_required_fields_from_pattern(pattern, required)
    _collect_required_fields_from_predicate(where_predicate, required)
    _collect_required_fields_from_return_items(return_items, required)
    if _contains_aggregates(return_items):
        _collect_required_fields_from_with_items(
            _grouping_items_for_aggregate_return(return_items),
            required,
        )
    _collect_required_fields_from_order_by(order_by, required)
    return required


def _required_fields_for_optional_match(
    match_patterns: tuple[CompiledPattern, ...],
    match_where_predicate: WherePredicate | None,
    optional_patterns: tuple[CompiledPattern, ...],
    optional_where_predicate: WherePredicate | None,
    *,
    return_items: tuple[ReturnItem, ...],
    order_by: tuple[OrderByItem, ...] | None,
) -> dict[str, set[str]]:
    """Compute the fields or identifiers needed by downstream execution."""
    required = _required_fields_for_match(
        match_patterns,
        match_where_predicate,
        return_items=return_items,
        order_by=order_by,
    )
    _merge_required_fields(
        required,
        _required_fields_for_match(
            optional_patterns,
            optional_where_predicate,
            return_items=return_items,
            order_by=order_by,
        ),
    )
    return required


def _required_fields_for_entity_variables(
    variables: Iterable[str],
) -> dict[str, set[str]]:
    """Compute the fields or identifiers needed by downstream execution."""
    required: dict[str, set[str]] = {}
    for variable in variables:
        _add_required_field(required, variable, _NODE_ID_FIELD)
        _add_required_field(required, variable, "id")
    return required


def _required_fields_for_create_bound_variables(
    create_patterns: tuple[CreateRelationshipPattern, ...],
) -> dict[str, set[str]]:
    """Require ids for the already-bound endpoints used by MATCH ... CREATE."""
    variables: list[str] = []
    for pattern in create_patterns:
        if pattern.left_bound:
            variables.append(pattern.left_variable)
        if pattern.right_bound:
            variables.append(pattern.right_variable)
    return _required_fields_for_entity_variables(variables)


def _merge_required_fields(
    target: dict[str, set[str]],
    source: Mapping[str, set[str]],
) -> None:
    """Merge related planner or write-path state into one structure."""
    for variable, fields in source.items():
        existing = target.get(variable)
        if existing is None:
            target[variable] = set(fields)
            continue
        if _FULL_ENTITY_FIELDS in existing or _FULL_ENTITY_FIELDS in fields:
            target[variable] = {_FULL_ENTITY_FIELDS}
            continue
        existing.update(fields)


def _collect_required_fields_from_pattern(
    pattern: CompiledPattern,
    required: dict[str, set[str]],
) -> None:
    """Collect metadata used by compilation, planning, or required-field tracking."""
    if isinstance(pattern, NodePattern):
        _add_required_node_fields(required, pattern.variable, pattern.properties)
        return
    _add_required_node_fields(required, pattern.start.variable, pattern.start.properties)
    for step in pattern.steps:
        if step.variable is not None:
            _add_required_edge_fields(required, step.variable, step.properties)
        _add_required_node_fields(required, step.node.variable, step.node.properties)


def _collect_required_fields_from_predicate(
    predicate: WherePredicate | None,
    required: dict[str, set[str]],
) -> None:
    """Collect metadata used by compilation, planning, or required-field tracking."""
    if predicate is None:
        return
    simple_predicates = (
        StringPredicate,
        ExistsPredicate,
        InPredicate,
        NullPredicate,
    )
    if isinstance(predicate, Comparison):
        if predicate.left_expression is not None:
            _collect_required_fields_from_scalar_expression(predicate.left_expression, required)
        else:
            _add_required_field(required, predicate.variable, predicate.field)
        if isinstance(predicate.value, ScalarExpression):
            _collect_required_fields_from_scalar_expression(predicate.value, required)
        elif isinstance(predicate.value, PropertyReferenceValue):
            _add_required_field(required, predicate.value.variable, predicate.value.field)
        return
    if isinstance(predicate, simple_predicates):
        _add_required_field(required, predicate.variable, predicate.field)
        return
    if isinstance(predicate, ExistsPatternPredicate):
        for pattern in predicate.patterns:
            _collect_required_fields_from_pattern(pattern, required)
        _collect_required_fields_from_predicate(predicate.where_predicate, required)
        return
    if isinstance(predicate, NotPredicate):
        _collect_required_fields_from_predicate(predicate.predicate, required)
        return
    if isinstance(predicate, AndPredicate | OrPredicate):
        for child in predicate.predicates:
            _collect_required_fields_from_predicate(child, required)


def _collect_required_fields_from_return_items(
    return_items: tuple[ReturnItem, ...],
    required: dict[str, set[str]],
) -> None:
    """Collect metadata used by compilation, planning, or required-field tracking."""
    for item in return_items:
        if item.scalar_expression is not None:
            _collect_required_fields_from_scalar_expression(item.scalar_expression, required)
            continue
        if item.aggregate is not None:
            if item.count_all:
                continue
            _add_required_field(required, item.variable, _aggregate_source_column_name(item))
            continue
        if item.field is None:
            _mark_requires_full_entity(required, item.variable)
            continue
        if item.field == "keys":
            _mark_requires_full_entity(required, item.variable)
            continue
        _add_required_field(required, item.variable, item.field)


def _collect_required_fields_from_scalar_expression(
    expression: ScalarExpression,
    required: dict[str, set[str]],
) -> None:
    """Collect fields read by a compiled scalar expression."""

    if isinstance(expression, FieldExpression):
        _add_required_field(required, expression.variable, expression.field)
        return
    if isinstance(expression, LiteralExpression):
        return
    if isinstance(expression, FunctionExpression):
        _collect_required_fields_from_scalar_expression(expression.argument, required)
        return
    if isinstance(expression, BinaryExpression):
        _collect_required_fields_from_scalar_expression(expression.left, required)
        _collect_required_fields_from_scalar_expression(expression.right, required)


def _collect_required_fields_from_order_by(
    order_by: tuple[OrderByItem, ...] | None,
    required: dict[str, set[str]],
) -> None:
    """Collect metadata used by compilation, planning, or required-field tracking."""
    if order_by is None:
        return
    for item in order_by:
        _add_required_field(required, item.variable, item.field)


def _can_plan_return_distinct(
    return_items: tuple[ReturnItem, ...],
    order_by: tuple[OrderByItem, ...] | None,
) -> bool:
    """Check whether the optimized or semantically valid path applies here."""
    if not return_items or any(item.aggregate is not None for item in return_items):
        return False
    if order_by is None:
        return True
    return all(
        any(
            return_item.variable == order_item.variable
            and (return_item.field is None or return_item.field == order_item.field)
            for return_item in return_items
        )
        for order_item in order_by
    )


def _collect_required_fields_from_with_items(
    with_items: tuple[WithItem, ...],
    required: dict[str, set[str]],
) -> None:
    """Collect metadata used by compilation, planning, or required-field tracking."""
    for item in with_items:
        if item.source_kind == "entity":
            _mark_requires_full_entity(required, item.source_variable)
            continue
        if item.source_field is None:
            continue
        _add_required_field(required, item.source_variable, _NODE_ID_FIELD)
        _add_required_field(required, item.source_variable, "id")
        _add_required_field(required, item.source_variable, item.source_field)


def _mark_requires_full_entity(required: dict[str, set[str]], variable: str) -> None:
    """Support the current Cypher compilation or execution step."""
    required[variable] = {_FULL_ENTITY_FIELDS}


def _add_required_node_fields(
    required: dict[str, set[str]],
    variable: str,
    properties: Mapping[str, object],
) -> None:
    """Support the current Cypher compilation or execution step."""
    for field_name in properties:
        _add_required_field(required, variable, field_name)


def _add_required_edge_fields(
    required: dict[str, set[str]],
    variable: str,
    properties: Mapping[str, object],
) -> None:
    """Support the current Cypher compilation or execution step."""
    for field_name in properties:
        _add_required_field(required, variable, field_name)


def _add_required_field(
    required: dict[str, set[str]],
    variable: str,
    field: str,
) -> None:
    """Support the current Cypher compilation or execution step."""
    if required.get(variable) == {_FULL_ENTITY_FIELDS}:
        return

    decoded = _decode_function_field(field)
    if decoded is not None:
        function_name, source_field = decoded
        if function_name in {"tolower", "toupper"}:
            _add_required_field(required, variable, source_field)
            return
        if function_name == "size":
            required.setdefault(variable, set()).add(field)
            _add_required_field(required, variable, source_field)
            return

    required.setdefault(variable, set()).add(field)


def _aggregate_source_column_name(item: ReturnItem) -> str:
    """Support the current Cypher compilation or execution step."""
    if item.field is None:
        return _ENTITY_ID_FIELD
    return item.field


def _count_distinct_non_null(column: pa.ChunkedArray) -> int:
    """Count values in the form expected by aggregate execution."""
    non_null = pc.drop_null(column)
    if len(non_null) == 0:
        return 0
    return len(pc.unique(non_null))


def _apply_distinct_to_query_result(result: QueryResult) -> QueryResult:
    """Apply this transformation or predicate inside the Cypher pipeline."""
    seen: set[str] = set()
    distinct_rows: list[dict[str, Any]] = []
    for row in result.rows:
        key = json.dumps(row, sort_keys=True)
        if key in seen:
            continue
        seen.add(key)
        distinct_rows.append(dict(row))
    return QueryResult(columns=result.columns, rows=tuple(distinct_rows))


def _apply_distinct_to_table(table: pa.Table) -> pa.Table:
    """Apply this transformation or predicate inside the Cypher pipeline."""
    if table.num_rows == 0:
        return table
    if not table.column_names:
        return table.slice(0, 1)
    try:
        return table.group_by(table.column_names).aggregate([])
    except pa.ArrowException:
        seen: set[str] = set()
        keep_mask: list[bool] = []
        for row in table.to_pylist():
            key = json.dumps(row, sort_keys=True)
            if key in seen:
                keep_mask.append(False)
                continue
            seen.add(key)
            keep_mask.append(True)
        return table.filter(pa.array(keep_mask, type=pa.bool_()))


def _materialize_create_edges_from_matches(
    matched_rows: pa.Table,
    create_patterns: tuple[CreateRelationshipPattern, ...],
) -> tuple[tuple[int, int, str, dict[str, object]], ...]:
    """Materialize write-side operations from matched rows for the graph service."""
    edge_specs: list[tuple[int, int, str, dict[str, object]]] = []
    for pattern in create_patterns:
        source_ids = _required_node_id_values_from_table(matched_rows, pattern.left_variable)
        target_ids = _required_node_id_values_from_table(matched_rows, pattern.right_variable)
        for source_id, target_id in zip(source_ids, target_ids, strict=True):
            edge_specs.append((source_id, target_id, pattern.rel_type, dict(pattern.properties)))
    return tuple(edge_specs)


def _materialize_match_create_records_from_matches(
    state: GraphState,
    matched_rows: pa.Table,
    create_patterns: tuple[CreateRelationshipPattern, ...],
) -> tuple[
    tuple[NodeRecord, ...],
    tuple[EdgeRecord, ...],
    dict[str, tuple[NodeRecord, ...]],
]:
    """Materialize nodes and edges produced by MATCH ... CREATE for one snapshot publish."""
    new_nodes: list[NodeRecord] = []
    new_edges: list[EdgeRecord] = []
    created_by_row: dict[tuple[str, int], NodeRecord] = {}
    bindings: dict[str, list[NodeRecord | None]] = {}
    source_rows = matched_rows.to_pylist()
    next_node_id = max((node.node_id for node in state.nodes), default=0) + 1
    next_edge_id = max((edge.edge_id for edge in state.edges), default=0) + 1

    def allocate_node(
        pattern: CreateRelationshipPattern,
        *,
        side: Literal["left", "right"],
        row: int,
    ) -> NodeRecord:
        """Create or reuse the inline node bound to one MATCH row."""
        nonlocal next_node_id
        variable = pattern.left_variable if side == "left" else pattern.right_variable
        labels = pattern.left_labels if side == "left" else pattern.right_labels
        properties = pattern.left_properties if side == "left" else pattern.right_properties
        key = (variable, row)
        existing = created_by_row.get(key)
        if existing is not None:
            if existing.labels != labels or existing.properties != properties:
                raise QueryError(
                    f"CREATE cannot redefine variable '{variable}' with different inline data."
                )
            return existing

        node = NodeRecord(
            node_id=next_node_id,
            labels=labels,
            properties=dict(properties),
        )
        next_node_id += 1
        created_by_row[key] = node
        new_nodes.append(node)
        bindings.setdefault(variable, [None] * matched_rows.num_rows)[row] = node
        return node

    for pattern in create_patterns:
        left_ids = (
            _required_node_id_values_from_table(matched_rows, pattern.left_variable)
            if pattern.left_bound
            else ()
        )
        right_ids = (
            _required_node_id_values_from_table(matched_rows, pattern.right_variable)
            if pattern.right_bound
            else ()
        )
        for row_index in range(matched_rows.num_rows):
            source_id = (
                left_ids[row_index]
                if pattern.left_bound
                else allocate_node(pattern, side="left", row=row_index).node_id
            )
            target_id = (
                right_ids[row_index]
                if pattern.right_bound
                else allocate_node(pattern, side="right", row=row_index).node_id
            )
            new_edges.append(
                EdgeRecord(
                    edge_id=next_edge_id,
                    rel_type=pattern.rel_type,
                    src_id=source_id,
                    dst_id=target_id,
                    properties=_resolve_input_create_properties(
                        pattern.properties,
                        source_row=source_rows[row_index],
                    ),
                )
            )
            next_edge_id += 1

    aligned_bindings: dict[str, tuple[NodeRecord, ...]] = {}
    for variable, records in bindings.items():
        if any(record is None for record in records):
            raise QueryError(f"CREATE node bindings for '{variable}' could not be aligned.")
        aligned_bindings[variable] = tuple(cast(NodeRecord, record) for record in records)
    return tuple(new_nodes), tuple(new_edges), aligned_bindings


def _materialize_merge_edges_from_matches(
    matched_rows: pa.Table,
    existing_edges: tuple[EdgeRecord, ...],
    merge_pattern: MergeRelationshipPattern,
) -> tuple[tuple[int, int, str, dict[str, object]], ...]:
    # MERGE on relationships must be idempotent across both the persisted graph state and
    # duplicate match rows produced by the current query execution.
    """Materialize write-side operations from matched rows for the graph service."""
    existing_specs = {
        (
            edge.src_id,
            edge.dst_id,
            edge.rel_type,
            json.dumps(edge.properties, sort_keys=True),
        )
        for edge in existing_edges
    }
    edge_specs: list[tuple[int, int, str, dict[str, object]]] = []
    planned_specs: set[tuple[int, int, str, str]] = set()
    source_ids = _required_node_id_values_from_table(matched_rows, merge_pattern.left_variable)
    target_ids = _required_node_id_values_from_table(matched_rows, merge_pattern.right_variable)
    for source_id, target_id in zip(source_ids, target_ids, strict=True):
        spec_key = (
            source_id,
            target_id,
            merge_pattern.rel_type,
            json.dumps(merge_pattern.properties, sort_keys=True),
        )
        if spec_key in existing_specs or spec_key in planned_specs:
            continue
        planned_specs.add(spec_key)
        edge_specs.append(
            (
                source_id,
                target_id,
                merge_pattern.rel_type,
                dict(merge_pattern.properties),
            )
        )
    return tuple(edge_specs)


def _classify_merge_edges_from_matches(
    matched_rows: pa.Table,
    existing_edges: tuple[EdgeRecord, ...],
    merge_pattern: MergeRelationshipPattern,
) -> tuple[
    tuple[bool, ...],
    tuple[EdgeRecord | int, ...],
    tuple[tuple[int, int, str, dict[str, object]], ...],
]:
    """Support the current Cypher compilation or execution step."""
    planned_specs: dict[tuple[int, int, str, str], int] = {}
    created_row_mask: list[bool] = []
    merge_edge_records: list[EdgeRecord | int] = []
    edge_specs: list[tuple[int, int, str, dict[str, object]]] = []
    source_ids = _required_node_id_values_from_table(matched_rows, merge_pattern.left_variable)
    target_ids = _required_node_id_values_from_table(matched_rows, merge_pattern.right_variable)
    property_key = json.dumps(merge_pattern.properties, sort_keys=True)
    for source_id, target_id in zip(source_ids, target_ids, strict=True):
        spec_key = (
            source_id,
            target_id,
            merge_pattern.rel_type,
            property_key,
        )
        existing_edge = _find_existing_merge_edge(
            existing_edges,
            source_id=source_id,
            target_id=target_id,
            rel_type=merge_pattern.rel_type,
            properties=merge_pattern.properties,
        )
        if existing_edge is not None:
            created_row_mask.append(False)
            merge_edge_records.append(existing_edge)
            continue
        planned_index = planned_specs.get(spec_key)
        if planned_index is not None:
            created_row_mask.append(False)
            merge_edge_records.append(planned_index)
            continue
        planned_index = len(edge_specs)
        planned_specs[spec_key] = planned_index
        created_row_mask.append(True)
        merge_edge_records.append(planned_index)
        edge_specs.append(
            (
                source_id,
                target_id,
                merge_pattern.rel_type,
                dict(merge_pattern.properties),
            )
        )
    return tuple(created_row_mask), tuple(merge_edge_records), tuple(edge_specs)


def _find_existing_merge_edge(
    edges: tuple[EdgeRecord, ...],
    *,
    source_id: int,
    target_id: int,
    rel_type: str,
    properties: Mapping[str, object],
) -> EdgeRecord | None:
    """Find the existing item or separator needed by this helper."""
    for edge in edges:
        if edge.src_id != source_id or edge.dst_id != target_id or edge.rel_type != rel_type:
            continue
        if _properties_match(edge.properties, dict(properties)):
            return edge
    return None


def _classify_match_merge_with_inline_node(
    matched_rows: pa.Table,
    state: GraphState,
    merge_pattern: MergeRelationshipPattern,
) -> tuple[
    tuple[bool, ...],
    tuple[NodeRecord, ...],
    tuple[EdgeRecord, ...],
    tuple[NodeRecord, ...],
    tuple[EdgeRecord, ...],
]:
    """Support the current Cypher compilation or execution step."""
    if merge_pattern.left_bound == merge_pattern.right_bound:
        raise QueryError("Inline-node MERGE classification requires exactly one unbound endpoint.")

    nodes_by_id = _nodes_by_id_tuple(state.nodes)
    existing_edges_sorted = tuple(sorted(state.edges, key=lambda edge: edge.edge_id))
    next_node_id = max((node.node_id for node in state.nodes), default=0) + 1
    next_edge_id = max((edge.edge_id for edge in state.edges), default=0) + 1

    created_row_mask: list[bool] = []
    merge_node_records: list[NodeRecord] = []
    merge_edge_records: list[EdgeRecord] = []
    new_nodes: list[NodeRecord] = []
    new_edges: list[EdgeRecord] = []
    planned_specs: dict[tuple[int, str, str, str, str], tuple[NodeRecord, EdgeRecord]] = {}

    if merge_pattern.left_bound:
        bound_ids = _required_node_id_values_from_table(matched_rows, merge_pattern.left_variable)
        unbound_labels = merge_pattern.right_labels
        unbound_properties = merge_pattern.right_properties
        edge_side: Literal["left", "right"] = "right"
    else:
        bound_ids = _required_node_id_values_from_table(matched_rows, merge_pattern.right_variable)
        unbound_labels = merge_pattern.left_labels
        unbound_properties = merge_pattern.left_properties
        edge_side = "left"

    unbound_pattern = NodePattern(
        variable=_merge_inline_node_variable(merge_pattern) or "_inline_merge_node",
        labels=unbound_labels,
        properties=unbound_properties,
    )
    rel_properties_key = json.dumps(merge_pattern.properties, sort_keys=True)
    node_properties_key = json.dumps(unbound_properties, sort_keys=True)
    labels_key = json.dumps(unbound_labels)

    for bound_id in bound_ids:
        existing_match = _find_existing_inline_merge_pattern(
            edges=existing_edges_sorted,
            nodes_by_id=nodes_by_id,
            bound_id=bound_id,
            edge_side=edge_side,
            rel_type=merge_pattern.rel_type,
            rel_properties=merge_pattern.properties,
            unbound_pattern=unbound_pattern,
        )
        if existing_match is not None:
            existing_node, existing_edge = existing_match
            created_row_mask.append(False)
            merge_node_records.append(existing_node)
            merge_edge_records.append(existing_edge)
            continue

        planned_key = (
            bound_id,
            edge_side,
            labels_key,
            node_properties_key,
            rel_properties_key,
        )
        planned_match = planned_specs.get(planned_key)
        if planned_match is not None:
            planned_node, planned_edge = planned_match
            created_row_mask.append(False)
            merge_node_records.append(planned_node)
            merge_edge_records.append(planned_edge)
            continue

        planned_node = NodeRecord(
            node_id=next_node_id,
            labels=unbound_labels,
            properties=dict(unbound_properties),
        )
        next_node_id += 1
        if edge_side == "right":
            planned_edge = EdgeRecord(
                edge_id=next_edge_id,
                rel_type=merge_pattern.rel_type,
                src_id=bound_id,
                dst_id=planned_node.node_id,
                properties=dict(merge_pattern.properties),
            )
        else:
            planned_edge = EdgeRecord(
                edge_id=next_edge_id,
                rel_type=merge_pattern.rel_type,
                src_id=planned_node.node_id,
                dst_id=bound_id,
                properties=dict(merge_pattern.properties),
            )
        next_edge_id += 1
        planned_specs[planned_key] = (planned_node, planned_edge)
        new_nodes.append(planned_node)
        new_edges.append(planned_edge)
        created_row_mask.append(True)
        merge_node_records.append(planned_node)
        merge_edge_records.append(planned_edge)

    return (
        tuple(created_row_mask),
        tuple(merge_node_records),
        tuple(merge_edge_records),
        tuple(new_nodes),
        tuple(new_edges),
    )


def _find_existing_inline_merge_pattern(
    *,
    edges: tuple[EdgeRecord, ...],
    nodes_by_id: Mapping[int, NodeRecord],
    bound_id: int,
    edge_side: Literal["left", "right"],
    rel_type: str,
    rel_properties: Mapping[str, object],
    unbound_pattern: NodePattern,
) -> tuple[NodeRecord, EdgeRecord] | None:
    """Find the existing item or separator needed by this helper."""
    for edge in edges:
        if edge.rel_type != rel_type or not _properties_match(
            edge.properties,
            dict(rel_properties),
        ):
            continue
        if edge_side == "right":
            if edge.src_id != bound_id:
                continue
            unbound_node = nodes_by_id.get(edge.dst_id)
        else:
            if edge.dst_id != bound_id:
                continue
            unbound_node = nodes_by_id.get(edge.src_id)
        if unbound_node is None:
            continue
        if _node_matches_node_pattern(unbound_node, unbound_pattern):
            return unbound_node, edge
    return None


def _nodes_by_id_tuple(nodes: tuple[NodeRecord, ...]) -> dict[int, NodeRecord]:
    """Handle node-specific matching, projection, or lookup logic for Cypher."""
    return {node.node_id: node for node in nodes}


def _resolve_created_merge_edge_records(
    edge_records: tuple[EdgeRecord | int, ...],
    new_edges: tuple[EdgeRecord, ...],
) -> tuple[EdgeRecord, ...]:
    """Resolve this reference into the concrete value or array used downstream."""
    resolved: list[EdgeRecord] = []
    for record in edge_records:
        if isinstance(record, EdgeRecord):
            resolved.append(record)
            continue
        resolved.append(new_edges[record])
    return tuple(resolved)


def _apply_merge_set_items_to_properties(
    base_properties: Mapping[str, object],
    *,
    variable: str,
    set_items: tuple[SetItem, ...],
) -> dict[str, object]:
    """Apply this transformation or predicate inside the Cypher pipeline."""
    properties = dict(base_properties)
    for item in set_items:
        if item.variable != variable:
            raise QueryError(f"MERGE action references an unexpected variable: {item.variable}")
        if isinstance(item, SetPropertyItem):
            properties[item.field] = item.value
            continue
        if isinstance(item, SetMapMergeItem):
            properties.update(item.properties)
            continue
        raise QueryError("MERGE SET += row references are supported only after WITH.")
    return properties


def _append_row_index_column(table: pa.Table, column_name: str) -> pa.Table:
    """Append aligned data needed by the current write or plan helper."""
    if column_name in table.column_names:
        return table
    return table.append_column(column_name, pa.array(range(table.num_rows), type=pa.int64()))


def _cross_join_tables(
    left: pa.Table,
    right: pa.Table,
    tabular: PyArrowTabularAdapter,
) -> pa.Table:
    """Cross-join two binding tables through a temporary constant join key."""
    join_key = "__grapharrow_cross_join"
    left_joinable = left.append_column(join_key, pa.array([0] * left.num_rows, type=pa.int8()))
    right_joinable = right.append_column(join_key, pa.array([0] * right.num_rows, type=pa.int8()))
    joined = tabular.inner_join(
        left_joinable,
        right_joinable,
        left_keys=join_key,
        right_keys=join_key,
    )
    return joined.drop([join_key])


def _pattern_has_property_reference_values(pattern: CompiledPattern) -> bool:
    """Return true when a pattern property reads from an existing row binding."""
    if isinstance(pattern, NodePattern):
        return any(
            isinstance(value, PropertyReferenceValue) for value in pattern.properties.values()
        )
    if any(
        isinstance(value, PropertyReferenceValue) for value in pattern.start.properties.values()
    ):
        return True
    return any(
        any(isinstance(value, PropertyReferenceValue) for value in step.properties.values())
        or any(isinstance(value, PropertyReferenceValue) for value in step.node.properties.values())
        for step in pattern.steps
    )


def _apply_deferred_pattern_reference_filters(
    table: pa.Table,
    pattern: CompiledPattern,
) -> pa.Table:
    """Apply dynamic row-reference filters after input rows have been joined in."""
    if isinstance(pattern, NodePattern):
        return _apply_node_pattern_filter(table, pattern)
    filtered = _apply_node_pattern_filter(table, pattern.start)
    for index, step in enumerate(pattern.steps):
        prefix = f"{step.variable or f'_rel_{index}'}"
        filtered = _apply_edge_pattern_filter(filtered, prefix, step)
        filtered = _apply_node_pattern_filter(filtered, step.node)
    return filtered


def _filter_table_with_mask(table: pa.Table, keep_mask: Iterable[bool]) -> pa.Table:
    """Support the current Cypher compilation or execution step."""
    return table.filter(pa.array(tuple(keep_mask), type=pa.bool_()))


def _required_fields_for_match_merge_variable(
    query: CompiledMatchMergeQuery,
) -> dict[str, set[str]]:
    """Compute the fields or identifiers needed by downstream execution."""
    if query.merge_pattern.variable is None:
        return {}
    required: dict[str, set[str]] = {}
    _collect_required_fields_from_return_items(query.return_items, required)
    _collect_required_fields_from_order_by(query.order_by, required)
    _merge_required_fields(
        required,
        _required_fields_for_entity_variables(
            item.variable
            for item in (*query.on_create_set_items, *query.on_match_set_items)
            if item.variable == query.merge_pattern.variable
        ),
    )
    return required


def _required_fields_for_merge_bound_nodes(
    merge_pattern: MergeRelationshipPattern,
) -> dict[str, set[str]]:
    """Compute the fields or identifiers needed by downstream execution."""
    required: dict[str, set[str]] = {}
    if merge_pattern.left_bound:
        _add_required_field(required, merge_pattern.left_variable, "labels")
        _add_required_node_fields(
            required,
            merge_pattern.left_variable,
            merge_pattern.left_properties,
        )
    if merge_pattern.right_bound:
        _add_required_field(required, merge_pattern.right_variable, "labels")
        _add_required_node_fields(
            required,
            merge_pattern.right_variable,
            merge_pattern.right_properties,
        )
    return required


def _required_fields_for_match_merge_inline_node(
    query: CompiledMatchMergeQuery,
) -> dict[str, set[str]]:
    """Compute the fields or identifiers needed by downstream execution."""
    variable = _merge_inline_node_variable(query.merge_pattern)
    if variable is None:
        return {}
    required: dict[str, set[str]] = {}
    _collect_required_fields_from_return_items(query.return_items, required)
    _collect_required_fields_from_order_by(query.order_by, required)
    _merge_required_fields(
        required,
        _required_fields_for_entity_variables(
            item.variable
            for item in (*query.on_create_set_items, *query.on_match_set_items)
            if item.variable == variable
        ),
    )
    return required


def _merge_inline_node_variable(merge_pattern: MergeRelationshipPattern) -> str | None:
    """Merge related planner or write-path state into one structure."""
    if not merge_pattern.left_bound:
        return merge_pattern.left_variable
    if not merge_pattern.right_bound:
        return merge_pattern.right_variable
    return None


def _append_merge_edge_binding_columns(
    table: pa.Table,
    *,
    variable: str,
    edge_records: tuple[EdgeRecord, ...],
    requested_fields: set[str] | None,
) -> pa.Table:
    """Append aligned data needed by the current write or plan helper."""
    edge_table = _edge_binding_table(
        edge_records,
        variable,
        requested_fields=requested_fields,
    )
    if edge_table.num_rows != table.num_rows:
        raise QueryError("MERGE relationship bindings could not be aligned with match rows.")
    updated = table
    for column_name in edge_table.column_names:
        if column_name in updated.column_names:
            column_index = updated.column_names.index(column_name)
            updated = updated.set_column(column_index, column_name, edge_table[column_name])
        else:
            updated = updated.append_column(column_name, edge_table[column_name])
    return updated


def _append_merge_node_binding_columns(
    table: pa.Table,
    *,
    variable: str,
    node_records: tuple[NodeRecord, ...],
    requested_fields: set[str] | None,
) -> pa.Table:
    """Append aligned data needed by the current write or plan helper."""
    return _append_node_binding_columns(
        table,
        variable=variable,
        node_records=node_records,
        requested_fields=requested_fields,
        context="MERGE node",
    )


def _append_node_binding_columns(
    table: pa.Table,
    *,
    variable: str,
    node_records: tuple[NodeRecord, ...],
    requested_fields: set[str] | None,
    context: str,
) -> pa.Table:
    """Append node binding columns aligned one-to-one with the input rows."""
    node_table = _node_binding_table(
        node_records,
        variable,
        requested_fields=requested_fields,
    )
    if node_table.num_rows != table.num_rows:
        raise QueryError(f"{context} bindings could not be aligned with match rows.")
    updated = table
    for column_name in node_table.column_names:
        if column_name in updated.column_names:
            column_index = updated.column_names.index(column_name)
            updated = updated.set_column(column_index, column_name, node_table[column_name])
        else:
            updated = updated.append_column(column_name, node_table[column_name])
    return updated


def _apply_bound_merge_node_filters(
    table: pa.Table,
    merge_pattern: MergeRelationshipPattern,
) -> pa.Table:
    """Apply this transformation or predicate inside the Cypher pipeline."""
    filtered = table
    if merge_pattern.left_bound:
        filtered = _apply_node_pattern_filter(
            filtered,
            NodePattern(
                variable=merge_pattern.left_variable,
                labels=merge_pattern.left_labels,
                properties=merge_pattern.left_properties,
            ),
        )
    if merge_pattern.right_bound:
        filtered = _apply_node_pattern_filter(
            filtered,
            NodePattern(
                variable=merge_pattern.right_variable,
                labels=merge_pattern.right_labels,
                properties=merge_pattern.right_properties,
            ),
        )
    return filtered


def _materialize_set_updates_from_matches(
    matched_rows: pa.Table,
    set_items: tuple[SetItem, ...],
) -> tuple[tuple[tuple[int, str, object], ...], tuple[tuple[int, str, object], ...]]:
    """Materialize write-side operations from matched rows for the graph service."""
    node_updates: list[tuple[int, str, object]] = []
    edge_updates: list[tuple[int, str, object]] = []
    seen_node_updates: set[tuple[int, str, str]] = set()
    seen_edge_updates: set[tuple[int, str, str]] = set()

    for item in set_items:
        entity_ids = _required_entity_id_values_from_table(
            matched_rows,
            item.variable,
            context="SET",
        )
        property_updates = _static_set_item_property_updates(item)

        if _is_node_binding_table(matched_rows, item.variable):
            for entity_id in entity_ids:
                for key, value in property_updates:
                    update_key = (entity_id, key, json.dumps(value, sort_keys=True))
                    if update_key in seen_node_updates:
                        continue
                    seen_node_updates.add(update_key)
                    node_updates.append((entity_id, key, value))
            continue
        if _is_edge_binding_table(matched_rows, item.variable):
            for entity_id in entity_ids:
                for key, value in property_updates:
                    update_key = (entity_id, key, json.dumps(value, sort_keys=True))
                    if update_key in seen_edge_updates:
                        continue
                    seen_edge_updates.add(update_key)
                    edge_updates.append((entity_id, key, value))
            continue
        raise QueryError(f"SET variable '{item.variable}' is not a supported node or relationship.")

    return tuple(node_updates), tuple(edge_updates)


def _static_set_item_property_updates(item: SetItem) -> tuple[tuple[str, object], ...]:
    """Return compile-time SET updates or reject row-dependent map updates."""
    if isinstance(item, SetPropertyItem):
        return ((item.field, item.value),)
    if isinstance(item, SetMapMergeItem):
        return tuple(item.properties.items())
    raise QueryError("SET += row references are supported only after WITH.")


def _materialize_set_from_input_rows(
    graph_service: GraphStateUseCase,
    input_rows: pa.Table,
    set_items: tuple[SetItem, ...],
) -> pa.Table:
    """Materialize row-driven SET clauses after WITH."""
    source_rows = input_rows.to_pylist()
    node_updates: list[tuple[int, str, object]] = []
    edge_updates: list[tuple[int, str, object]] = []
    output_rows: list[dict[str, Any]] = []

    for source_row in source_rows:
        output_row = dict(source_row)
        for item in set_items:
            entity_id = _required_entity_id_value_from_row(source_row, item.variable)
            property_updates = _resolve_input_set_property_updates(item, source_row=source_row)
            if _is_node_binding_row(source_row, item.variable):
                for key, value in property_updates:
                    node_updates.append((entity_id, key, value))
                    output_row[_binding_column_name(item.variable, key)] = value
                continue
            if _is_edge_binding_row(source_row, item.variable):
                for key, value in property_updates:
                    edge_updates.append((entity_id, key, value))
                    output_row[_binding_column_name(item.variable, key)] = value
                continue
            raise QueryError(
                f"SET variable '{item.variable}' is not a supported node or relationship."
            )
        output_rows.append(output_row)

    if node_updates or edge_updates:
        graph_service.update_properties_batch(
            node_updates=tuple(node_updates),
            edge_updates=tuple(edge_updates),
        )
    if not output_rows:
        return input_rows
    return pa.Table.from_pylist(output_rows)


def _resolve_input_set_property_updates(
    item: SetItem,
    *,
    source_row: Mapping[str, Any],
) -> tuple[tuple[str, object], ...]:
    """Resolve one SET item against the current WITH row."""
    if isinstance(item, SetPropertyItem):
        return (
            (
                item.field,
                _resolve_input_set_value(item.value, source_row=source_row),
            ),
        )
    if isinstance(item, SetMapMergeItem):
        return tuple(
            (key, _resolve_input_set_value(value, source_row=source_row))
            for key, value in item.properties.items()
        )
    resolved = _resolve_input_set_value(item.source, source_row=source_row)
    if not isinstance(resolved, Mapping):
        raise QueryError("SET += row requires the row reference to resolve to a map.")
    reserved_fields = {"id", "labels", "type", "source", "target", "__join_id"}
    return tuple(
        (str(key), value)
        for key, value in resolved.items()
        if isinstance(key, str) and key not in reserved_fields
    )


def _resolve_input_set_value(
    value: object,
    *,
    source_row: Mapping[str, Any],
) -> object:
    """Resolve row references used by SET after WITH."""
    if isinstance(value, PropertyReferenceValue):
        return _resolve_input_create_value(value, source_row=source_row)
    return value


def _required_entity_id_value_from_row(row: Mapping[str, Any], variable: str) -> int:
    """Read one bound entity id from a row."""
    value = row.get(_binding_column_name(variable, _ENTITY_ID_FIELD))
    if isinstance(value, int):
        return value
    raise QueryError(f"SET variable '{variable}' does not resolve to an entity id.")


def _is_node_binding_row(row: Mapping[str, Any], variable: str) -> bool:
    """Return whether one row contains a node binding for this variable."""
    return (
        _binding_column_name(variable, _NODE_ID_FIELD) in row
        or _binding_column_name(variable, "labels") in row
    ) and _binding_column_name(variable, "type") not in row


def _is_edge_binding_row(row: Mapping[str, Any], variable: str) -> bool:
    """Return whether one row contains an edge binding for this variable."""
    return _binding_column_name(variable, "type") in row


def _materialize_delete_targets_from_matches(
    matched_rows: pa.Table,
    delete_items: tuple[DeleteItem, ...],
) -> tuple[tuple[int, ...], tuple[int, ...]]:
    """Materialize write-side operations from matched rows for the graph service."""
    node_ids: list[int] = []
    edge_ids: list[int] = []
    seen_node_ids: set[int] = set()
    seen_edge_ids: set[int] = set()

    for item in delete_items:
        entity_ids = _required_entity_id_values_from_table(
            matched_rows,
            item.variable,
            context="DELETE",
        )
        if _is_node_binding_table(matched_rows, item.variable):
            for entity_id in entity_ids:
                if entity_id not in seen_node_ids:
                    seen_node_ids.add(entity_id)
                    node_ids.append(entity_id)
            continue
        if _is_edge_binding_table(matched_rows, item.variable):
            for entity_id in entity_ids:
                if entity_id not in seen_edge_ids:
                    seen_edge_ids.add(entity_id)
                    edge_ids.append(entity_id)
            continue
        raise QueryError(
            f"DELETE variable '{item.variable}' is not a supported node or relationship."
        )

    return tuple(node_ids), tuple(edge_ids)


def _materialize_remove_targets_from_matches(
    matched_rows: pa.Table,
    remove_items: tuple[RemoveItem, ...],
) -> tuple[
    tuple[tuple[int, str], ...],
    tuple[tuple[int, str], ...],
    tuple[tuple[int, str], ...],
]:
    """Materialize property removals from matched rows for the graph service."""
    node_removals: list[tuple[int, str]] = []
    edge_removals: list[tuple[int, str]] = []
    node_label_removals: list[tuple[int, str]] = []
    seen_node_removals: set[tuple[int, str]] = set()
    seen_edge_removals: set[tuple[int, str]] = set()
    seen_node_label_removals: set[tuple[int, str]] = set()

    for item in remove_items:
        if isinstance(item, RemoveLabelItem):
            entity_ids = _required_entity_id_values_from_table(
                matched_rows,
                item.variable,
                context="REMOVE",
            )
            if not _is_node_binding_table(matched_rows, item.variable):
                raise QueryError(f"REMOVE label target '{item.variable}' is not a node.")
            for entity_id in entity_ids:
                removal_key = (entity_id, item.label)
                if removal_key in seen_node_label_removals:
                    continue
                seen_node_label_removals.add(removal_key)
                node_label_removals.append(removal_key)
            continue
        entity_ids = _required_entity_id_values_from_table(
            matched_rows,
            item.variable,
            context="REMOVE",
        )
        if _is_node_binding_table(matched_rows, item.variable):
            for entity_id in entity_ids:
                removal_key = (entity_id, item.field)
                if removal_key in seen_node_removals:
                    continue
                seen_node_removals.add(removal_key)
                node_removals.append(removal_key)
            continue
        if _is_edge_binding_table(matched_rows, item.variable):
            for entity_id in entity_ids:
                removal_key = (entity_id, item.field)
                if removal_key in seen_edge_removals:
                    continue
                seen_edge_removals.add(removal_key)
                edge_removals.append(removal_key)
            continue
        raise QueryError(
            f"REMOVE variable '{item.variable}' is not a supported node or relationship."
        )

    return tuple(node_removals), tuple(edge_removals), tuple(node_label_removals)


def _with_incident_edge_ids(
    *,
    node_ids: tuple[int, ...],
    edge_ids: tuple[int, ...],
    edges: tuple[EdgeRecord, ...],
) -> tuple[int, ...]:
    """Support the current Cypher compilation or execution step."""
    if not node_ids:
        return edge_ids

    collected_edge_ids = list(edge_ids)
    seen_edge_ids = set(edge_ids)
    node_id_set = set(node_ids)
    for edge in edges:
        if edge.edge_id in seen_edge_ids:
            continue
        if edge.src_id in node_id_set or edge.dst_id in node_id_set:
            seen_edge_ids.add(edge.edge_id)
            collected_edge_ids.append(edge.edge_id)
    return tuple(collected_edge_ids)


def _required_node_id_values_from_table(table: pa.Table, variable: str) -> tuple[int, ...]:
    """Compute the fields or identifiers needed by downstream execution."""
    if (
        not _is_node_binding_table(table, variable)
        and _resolve_table_field_array(table, variable=variable, field=_ENTITY_ID_FIELD) is None
    ):
        raise QueryError(f"Bound variable '{variable}' does not resolve to a node id.")
    return _required_entity_id_values_from_table(table, variable, context="Bound")


def _required_entity_id_values_from_table(
    table: pa.Table,
    variable: str,
    *,
    context: str,
) -> tuple[int, ...]:
    """Compute the fields or identifiers needed by downstream execution."""
    column = _resolve_table_field_array(table, variable=variable, field=_ENTITY_ID_FIELD)
    if column is None:
        raise QueryError(f"{context} variable '{variable}' does not resolve to an entity id.")
    values = column.to_pylist()
    entity_ids: list[int] = []
    for value in values:
        if not isinstance(value, int):
            raise QueryError(f"{context} variable '{variable}' does not resolve to an entity id.")
        entity_ids.append(value)
    return tuple(entity_ids)


def _is_node_binding_table(table: pa.Table, variable: str) -> bool:
    """Check whether the optimized or semantically valid path applies here."""
    return _binding_column_name(variable, "labels") in table.column_names


def _is_edge_binding_table(table: pa.Table, variable: str) -> bool:
    """Check whether the optimized or semantically valid path applies here."""
    return _binding_column_name(variable, "type") in table.column_names


def _apply_set_items_to_table(
    table: pa.Table,
    set_items: tuple[SetItem, ...],
) -> pa.Table:
    """Apply this transformation or predicate inside the Cypher pipeline."""
    updated = table
    for item in set_items:
        if (
            _resolve_table_field_array(updated, variable=item.variable, field=_ENTITY_ID_FIELD)
            is None
        ):
            raise QueryError(f"SET references an unknown binding: {item.variable}")
        property_updates = _static_set_item_property_updates(item)
        for field, value in property_updates:
            value_column = pa.array([value] * updated.num_rows)
            target_column = _binding_column_name(item.variable, field)
            if target_column in updated.column_names:
                column_index = updated.column_names.index(target_column)
                updated = updated.set_column(column_index, target_column, value_column)
            else:
                updated = updated.append_column(target_column, value_column)
    return updated


def _apply_remove_items_to_table(
    table: pa.Table,
    remove_items: tuple[RemoveItem, ...],
) -> pa.Table:
    """Reflect property removals in rows returned by MATCH ... REMOVE ... RETURN."""
    updated = table
    for item in remove_items:
        if isinstance(item, RemoveLabelItem):
            labels_array = _resolve_table_field_array(
                updated,
                variable=item.variable,
                field="labels",
            )
            if labels_array is None:
                raise QueryError(f"REMOVE label target '{item.variable}' is not a node.")
            target_column = _binding_column_name(item.variable, "labels")
            updated_labels = [
                [label for label in _decode_labels(labels) if label != item.label]
                for labels in labels_array.to_pylist()
            ]
            column_index = updated.column_names.index(target_column)
            updated = updated.set_column(
                column_index,
                target_column,
                pa.array(updated_labels),
            )
            continue
        if (
            _resolve_table_field_array(updated, variable=item.variable, field=_ENTITY_ID_FIELD)
            is None
        ):
            raise QueryError(f"REMOVE references an unknown binding: {item.variable}")
        target_column = _binding_column_name(item.variable, item.field)
        if target_column not in updated.column_names:
            continue
        column_index = updated.column_names.index(target_column)
        updated = updated.set_column(
            column_index,
            target_column,
            pa.nulls(updated.num_rows),
        )
    return updated


def _materialize_create_patterns(
    state: GraphState,
    patterns: tuple[CompiledPattern, ...],
) -> tuple[tuple[NodeRecord, ...], tuple[EdgeRecord, ...], dict[str, NodeRecord]]:
    """Materialize write-side operations from matched rows for the graph service."""
    bindings: dict[str, NodeRecord] = {}
    new_nodes: list[NodeRecord] = []
    new_edges: list[EdgeRecord] = []
    next_node_id = max((node.node_id for node in state.nodes), default=0) + 1
    next_edge_id = max((edge.edge_id for edge in state.edges), default=0) + 1

    def allocate_node_id() -> int:
        """Allocate the next synthetic identifier while materializing CREATE patterns."""
        nonlocal next_node_id
        current = next_node_id
        next_node_id += 1
        return current

    def allocate_edge_id() -> int:
        """Allocate the next synthetic identifier while materializing CREATE patterns."""
        nonlocal next_edge_id
        current = next_edge_id
        next_edge_id += 1
        return current

    # CREATE is materialized against an immutable snapshot view first, then the
    # write path publishes the whole batch as one new snapshot.
    for pattern in patterns:
        if isinstance(pattern, NodePattern):
            _resolve_or_create_create_node(
                pattern,
                bindings=bindings,
                next_node_id=allocate_node_id,
                new_nodes=new_nodes,
            )
            continue

        if len(pattern.steps) != 1:
            raise QueryError("CREATE currently supports only a single relationship hop.")

        step = pattern.steps[0]
        if step.min_hops != 1 or step.max_hops != 1:
            raise QueryError("Variable-length relationships are not supported yet in CREATE.")
        if step.rel_type is None:
            raise QueryError("CREATE requires an explicit relationship type.")

        left_node = _resolve_or_create_create_node(
            pattern.start,
            bindings=bindings,
            next_node_id=allocate_node_id,
            new_nodes=new_nodes,
        )
        right_node = _resolve_or_create_create_node(
            step.node,
            bindings=bindings,
            next_node_id=allocate_node_id,
            new_nodes=new_nodes,
        )

        new_edges.append(
            EdgeRecord(
                edge_id=allocate_edge_id(),
                rel_type=step.rel_type,
                src_id=left_node.node_id,
                dst_id=right_node.node_id,
                properties=dict(step.properties),
            )
        )

    return tuple(new_nodes), tuple(new_edges), bindings


def _materialize_create_patterns_from_input_rows(
    graph_service: GraphStateUseCase,
    input_rows: pa.Table,
    patterns: tuple[CompiledPattern, ...],
    *,
    return_items: tuple[ReturnItem, ...],
    order_by: tuple[OrderByItem, ...] | None,
) -> pa.Table:
    """Materialize CREATE after WITH while preserving row-aligned bindings."""
    state = graph_service.load_state()
    source_rows = input_rows.to_pylist()
    new_nodes: list[NodeRecord] = []
    new_edges: list[EdgeRecord] = []
    binding_rows: list[dict[str, Any]] = []
    next_node_id = max((node.node_id for node in state.nodes), default=0) + 1
    next_edge_id = max((edge.edge_id for edge in state.edges), default=0) + 1

    def allocate_node_id() -> int:
        """Allocate the next node identifier for row-driven CREATE."""
        nonlocal next_node_id
        current = next_node_id
        next_node_id += 1
        return current

    def allocate_edge_id() -> int:
        """Allocate the next edge identifier for row-driven CREATE."""
        nonlocal next_edge_id
        current = next_edge_id
        next_edge_id += 1
        return current

    requested_fields: dict[str, set[str]] = {}
    _collect_required_fields_from_return_items(return_items, requested_fields)
    _collect_required_fields_from_order_by(order_by, requested_fields)

    for source_row in source_rows:
        row_bindings: dict[str, NodeRecord] = {}
        for pattern in patterns:
            if isinstance(pattern, NodePattern):
                _resolve_or_create_input_create_node(
                    pattern,
                    source_row=source_row,
                    bindings=row_bindings,
                    next_node_id=allocate_node_id,
                    new_nodes=new_nodes,
                )
                continue

            if len(pattern.steps) != 1:
                raise QueryError("CREATE after WITH currently supports only one relationship hop.")
            step = pattern.steps[0]
            if step.min_hops != 1 or step.max_hops != 1:
                raise QueryError("Variable-length relationships are not supported yet in CREATE.")
            if step.rel_type is None:
                raise QueryError("CREATE requires an explicit relationship type.")

            left_node = _resolve_or_create_input_create_node(
                pattern.start,
                source_row=source_row,
                bindings=row_bindings,
                next_node_id=allocate_node_id,
                new_nodes=new_nodes,
            )
            right_node = _resolve_or_create_input_create_node(
                step.node,
                source_row=source_row,
                bindings=row_bindings,
                next_node_id=allocate_node_id,
                new_nodes=new_nodes,
            )
            new_edges.append(
                EdgeRecord(
                    edge_id=allocate_edge_id(),
                    rel_type=step.rel_type,
                    src_id=left_node.node_id,
                    dst_id=right_node.node_id,
                    properties=_resolve_input_create_properties(
                        step.properties,
                        source_row=source_row,
                    ),
                )
            )
        binding_rows.append(dict(source_row) | _node_binding_values(row_bindings, requested_fields))

    graph_service.publish_records(
        previous_state=state,
        new_nodes=tuple(new_nodes),
        new_edges=tuple(new_edges),
    )
    if not binding_rows:
        return input_rows
    return pa.Table.from_pylist(binding_rows)


def _materialize_merge_node_from_input_rows(
    graph_service: GraphStateUseCase,
    input_rows: pa.Table,
    pattern: MergeNodePattern,
    *,
    on_create_set_items: tuple[SetItem, ...],
    on_match_set_items: tuple[SetItem, ...],
    return_items: tuple[ReturnItem, ...],
    order_by: tuple[OrderByItem, ...] | None,
) -> pa.Table:
    """Materialize row-driven MERGE node imports after WITH."""
    state = graph_service.load_state()
    source_rows = input_rows.to_pylist()
    new_nodes: list[NodeRecord] = []
    node_updates: list[tuple[int, str, object]] = []
    binding_rows: list[dict[str, Any]] = []
    next_node_id = max((node.node_id for node in state.nodes), default=0) + 1

    requested_fields: dict[str, set[str]] = {}
    _collect_required_fields_from_return_items(return_items, requested_fields)
    _collect_required_fields_from_order_by(order_by, requested_fields)
    _merge_required_fields(
        requested_fields,
        _required_fields_for_entity_variables(
            item.variable for item in (*on_create_set_items, *on_match_set_items)
        ),
    )

    for source_row in source_rows:
        resolved_pattern = MergeNodePattern(
            variable=pattern.variable,
            labels=pattern.labels,
            properties=_resolve_input_create_properties(
                pattern.properties,
                source_row=source_row,
            ),
        )
        node = _find_input_merge_node(state.nodes, tuple(new_nodes), resolved_pattern)
        created = node is None
        if node is None:
            node = NodeRecord(
                node_id=next_node_id,
                labels=resolved_pattern.labels,
                properties=_resolve_input_merge_set_properties(
                    resolved_pattern.properties,
                    variable=pattern.variable,
                    set_items=on_create_set_items,
                    source_row=source_row,
                ),
            )
            next_node_id += 1
            new_nodes.append(node)
        elif on_match_set_items:
            updated_properties = _resolve_input_merge_set_properties(
                node.properties,
                variable=pattern.variable,
                set_items=on_match_set_items,
                source_row=source_row,
            )
            for key, value in updated_properties.items():
                if node.properties.get(key) != value:
                    node_updates.append((node.node_id, key, value))
            node = NodeRecord(
                node_id=node.node_id,
                labels=node.labels,
                properties=updated_properties,
            )
        row_binding = _node_binding_values(
            {pattern.variable: node},
            requested_fields,
        )
        binding_rows.append(dict(source_row) | row_binding)
        if created and on_match_set_items:
            continue

    if new_nodes:
        graph_service.publish_records(
            previous_state=state,
            new_nodes=tuple(new_nodes),
            new_edges=(),
        )
    if node_updates:
        graph_service.update_properties_batch(
            node_updates=tuple(node_updates),
            edge_updates=(),
        )
    if not binding_rows:
        return input_rows
    return pa.Table.from_pylist(binding_rows)


def _find_input_merge_node(
    existing_nodes: tuple[NodeRecord, ...],
    planned_nodes: tuple[NodeRecord, ...],
    pattern: MergeNodePattern,
) -> NodeRecord | None:
    """Find an existing or same-batch node for row-driven MERGE."""
    for node in (*existing_nodes, *planned_nodes):
        if _node_matches_merge_pattern(node, pattern):
            return node
    return None


def _materialize_merge_relationship_from_input_rows(
    graph_service: GraphStateUseCase,
    input_rows: pa.Table,
    pattern: MergeRelationshipPattern,
    *,
    on_create_set_items: tuple[SetItem, ...],
    on_match_set_items: tuple[SetItem, ...],
    return_items: tuple[ReturnItem, ...],
    order_by: tuple[OrderByItem, ...] | None,
) -> pa.Table:
    """Materialize row-driven MERGE relationship imports after WITH."""
    if not pattern.left_bound or not pattern.right_bound:
        raise QueryError("MERGE after WITH currently requires both relationship endpoints bound.")

    state = graph_service.load_state()
    source_rows = input_rows.to_pylist()
    new_edges: list[EdgeRecord] = []
    edge_updates: list[tuple[int, str, object]] = []
    binding_rows: list[dict[str, Any]] = []
    next_edge_id = max((edge.edge_id for edge in state.edges), default=0) + 1

    requested_fields: dict[str, set[str]] = {}
    _collect_required_fields_from_return_items(return_items, requested_fields)
    _collect_required_fields_from_order_by(order_by, requested_fields)
    _merge_required_fields(
        requested_fields,
        _required_fields_for_entity_variables(
            item.variable for item in (*on_create_set_items, *on_match_set_items)
        ),
    )

    for source_row in source_rows:
        source_id = _required_node_id_value_from_row(source_row, pattern.left_variable)
        target_id = _required_node_id_value_from_row(source_row, pattern.right_variable)
        rel_properties = _resolve_input_create_properties(
            pattern.properties,
            source_row=source_row,
        )
        edge = _find_input_merge_edge(
            existing_edges=state.edges,
            planned_edges=tuple(new_edges),
            source_id=source_id,
            target_id=target_id,
            rel_type=pattern.rel_type,
            properties=rel_properties,
        )
        if edge is None:
            edge = EdgeRecord(
                edge_id=next_edge_id,
                rel_type=pattern.rel_type,
                src_id=source_id,
                dst_id=target_id,
                properties=_resolve_input_merge_set_properties(
                    rel_properties,
                    variable=pattern.variable or "",
                    set_items=on_create_set_items,
                    source_row=source_row,
                )
                if pattern.variable is not None
                else rel_properties,
            )
            next_edge_id += 1
            new_edges.append(edge)
        elif pattern.variable is not None and on_match_set_items:
            updated_properties = _resolve_input_merge_set_properties(
                edge.properties,
                variable=pattern.variable,
                set_items=on_match_set_items,
                source_row=source_row,
            )
            for key, value in updated_properties.items():
                if edge.properties.get(key) != value:
                    edge_updates.append((edge.edge_id, key, value))
            edge = EdgeRecord(
                edge_id=edge.edge_id,
                rel_type=edge.rel_type,
                src_id=edge.src_id,
                dst_id=edge.dst_id,
                properties=updated_properties,
            )
        row = dict(source_row)
        if pattern.variable is not None:
            row |= _edge_binding_values(
                pattern.variable,
                edge,
                requested_fields,
            )
        binding_rows.append(row)

    if new_edges:
        graph_service.publish_records(
            previous_state=state,
            new_nodes=(),
            new_edges=tuple(new_edges),
        )
    if edge_updates:
        graph_service.update_properties_batch(
            node_updates=(),
            edge_updates=tuple(edge_updates),
        )
    if not binding_rows:
        return input_rows
    return pa.Table.from_pylist(binding_rows)


def _find_input_merge_edge(
    *,
    existing_edges: tuple[EdgeRecord, ...],
    planned_edges: tuple[EdgeRecord, ...],
    source_id: int,
    target_id: int,
    rel_type: str,
    properties: Mapping[str, object],
) -> EdgeRecord | None:
    """Find an existing or same-batch relationship for row-driven MERGE."""
    for edge in (*existing_edges, *planned_edges):
        if edge.src_id != source_id or edge.dst_id != target_id or edge.rel_type != rel_type:
            continue
        if _properties_match(edge.properties, dict(properties)):
            return edge
    return None


def _required_node_id_value_from_row(row: Mapping[str, Any], variable: str) -> int:
    """Read one node id binding from a row produced by MATCH after WITH."""
    for field in (_ENTITY_ID_FIELD, _NODE_ID_FIELD):
        value = row.get(_binding_column_name(variable, field))
        if isinstance(value, int):
            return value
    raise QueryError(f"Bound variable '{variable}' does not resolve to a node id.")


def _edge_binding_values(
    variable: str,
    edge: EdgeRecord,
    requested_fields: Mapping[str, set[str]],
) -> dict[str, Any]:
    """Convert a merged relationship into row columns consumed by RETURN/ORDER BY."""
    fields = requested_fields.get(variable)
    row: dict[str, Any] = {
        _binding_column_name(variable, _ENTITY_ID_FIELD): edge.edge_id,
        _binding_column_name(variable, "id"): edge.edge_id,
    }
    if fields is None or "type" in fields or _FULL_ENTITY_FIELDS in fields:
        row[_binding_column_name(variable, "type")] = edge.rel_type
    if fields is None or "source" in fields or _FULL_ENTITY_FIELDS in fields:
        row[_binding_column_name(variable, "source")] = edge.src_id
    if fields is None or "target" in fields or _FULL_ENTITY_FIELDS in fields:
        row[_binding_column_name(variable, "target")] = edge.dst_id
    property_fields = edge.properties.keys() if fields is None else fields
    for field in property_fields:
        if field in {_ENTITY_ID_FIELD, "id", "type", "source", "target", _FULL_ENTITY_FIELDS}:
            continue
        row[_binding_column_name(variable, field)] = edge.properties.get(field)
    return row


def _resolve_input_merge_set_properties(
    base_properties: Mapping[str, object],
    *,
    variable: str,
    set_items: tuple[SetItem, ...],
    source_row: Mapping[str, Any],
) -> dict[str, object]:
    """Apply MERGE action properties while resolving row references."""
    properties = dict(base_properties)
    for item in set_items:
        if item.variable != variable:
            raise QueryError(f"MERGE action references an unexpected variable: {item.variable}")
        if isinstance(item, SetPropertyItem):
            properties[item.field] = _resolve_input_merge_set_value(
                item.value,
                source_row=source_row,
            )
            continue
        if isinstance(item, SetMapMergeItem):
            properties.update(
                {
                    key: _resolve_input_merge_set_value(value, source_row=source_row)
                    for key, value in item.properties.items()
                }
            )
            continue
        resolved = _resolve_input_merge_set_value(item.source, source_row=source_row)
        if not isinstance(resolved, Mapping):
            raise QueryError("MERGE SET += row requires the row reference to resolve to a map.")
        properties.update(
            {
                key: value
                for key, value in resolved.items()
                if isinstance(key, str)
                and key not in {"id", "labels", "type", "source", "target", "__join_id"}
            }
        )
    return properties


def _resolve_input_merge_set_value(
    value: object,
    *,
    source_row: Mapping[str, Any],
) -> object:
    """Resolve one MERGE action value against the current WITH row."""
    if isinstance(value, PropertyReferenceValue):
        return _resolve_input_create_value(value, source_row=source_row)
    return value


def _can_use_input_row_node_match_fast_path(patterns: tuple[CompiledPattern, ...]) -> bool:
    """Use a simple row-wise matcher for CSV-style WITH row MATCH node lookups."""
    return all(isinstance(pattern, NodePattern) for pattern in patterns) and any(
        _pattern_has_property_reference_values(pattern) for pattern in patterns
    )


def _match_node_patterns_from_input_rows(
    input_rows: pa.Table,
    nodes: tuple[NodeRecord, ...],
    patterns: tuple[CompiledPattern, ...],
    *,
    required_fields: Mapping[str, set[str]],
) -> pa.Table:
    """Match node patterns whose inline properties reference WITH-row values."""
    rows: list[dict[str, Any]] = [dict(row) for row in input_rows.to_pylist()]
    for pattern in patterns:
        assert isinstance(pattern, NodePattern)
        expanded_rows: list[dict[str, Any]] = []
        for row in rows:
            for node in nodes:
                if _node_matches_input_row_pattern(node, pattern, row):
                    expanded_rows.append(
                        row
                        | _node_binding_values(
                            {pattern.variable: node},
                            required_fields,
                        )
                    )
        rows = expanded_rows
        if not rows:
            break
    if not rows:
        return input_rows.slice(0, 0)
    return pa.Table.from_pylist(rows)


def _node_matches_input_row_pattern(
    node: NodeRecord,
    pattern: NodePattern,
    row: Mapping[str, Any],
) -> bool:
    """Match one node against a pattern that may reference row values."""
    if pattern.labels and not all(label in node.labels for label in pattern.labels):
        return False
    for key, expected in pattern.properties.items():
        expected_value = (
            _resolve_input_create_value(expected, source_row=row)
            if isinstance(expected, PropertyReferenceValue)
            else expected
        )
        if node.properties.get(key) != expected_value:
            return False
    return True


def _resolve_or_create_input_create_node(
    pattern: NodePattern,
    *,
    source_row: Mapping[str, Any],
    bindings: dict[str, NodeRecord],
    next_node_id: Callable[[], int],
    new_nodes: list[NodeRecord],
) -> NodeRecord:
    """Resolve a CREATE node for one row produced by WITH."""
    existing = bindings.get(pattern.variable)
    if existing is not None:
        if pattern.labels or pattern.properties:
            raise QueryError(
                "CREATE cannot redefine variable "
                f"'{pattern.variable}' with inline labels or properties."
            )
        return existing

    node = NodeRecord(
        node_id=next_node_id(),
        labels=pattern.labels,
        properties=_resolve_input_create_properties(pattern.properties, source_row=source_row),
    )
    bindings[pattern.variable] = node
    new_nodes.append(node)
    return node


def _resolve_input_create_properties(
    properties: Mapping[str, object],
    *,
    source_row: Mapping[str, Any],
) -> dict[str, object]:
    """Resolve inline CREATE properties against one WITH row."""
    return {
        key: _resolve_input_create_value(value, source_row=source_row)
        for key, value in properties.items()
    }


def _resolve_input_create_value(
    value: object,
    *,
    source_row: Mapping[str, Any],
) -> object:
    """Resolve a literal or row property reference for CREATE after WITH."""
    if not isinstance(value, PropertyReferenceValue):
        return value
    column = _binding_column_name(value.variable, value.field)
    if column in source_row:
        return source_row[column]
    if value.field == "value" and value.variable in source_row:
        return source_row[value.variable]
    scalar_column = _binding_column_name(value.variable, "value")
    scalar_value = _decode_join_json_value(source_row.get(scalar_column))
    if isinstance(scalar_value, Mapping):
        return scalar_value.get(value.field)
    raise QueryError(f"CREATE references an unknown binding: {value.variable}.{value.field}")


def _try_resolve_input_reference_value(
    value: PropertyReferenceValue,
    *,
    source_row: Mapping[str, Any],
) -> object:
    """Resolve a row reference, or mark it unresolved until input rows are joined."""
    try:
        return _resolve_input_create_value(value, source_row=source_row)
    except QueryError:
        return _UNRESOLVED_REFERENCE


def _node_binding_values(
    bindings: Mapping[str, NodeRecord],
    requested_fields: Mapping[str, set[str]],
) -> dict[str, Any]:
    """Convert created nodes into row columns consumed by RETURN/ORDER BY."""
    row: dict[str, Any] = {}
    for variable, node in bindings.items():
        fields = requested_fields.get(variable)
        row[_binding_column_name(variable, _ENTITY_ID_FIELD)] = node.node_id
        row[_binding_column_name(variable, _NODE_ID_FIELD)] = node.node_id
        if fields is None or "labels" in fields or _FULL_ENTITY_FIELDS in fields:
            row[_binding_column_name(variable, "labels")] = list(node.labels)
        property_fields = node.properties.keys() if fields is None else fields
        for field in property_fields:
            if field in {_ENTITY_ID_FIELD, _NODE_ID_FIELD, "labels", _FULL_ENTITY_FIELDS}:
                continue
            row[_binding_column_name(variable, field)] = node.properties.get(field)
    return row


def _resolve_or_create_create_node(
    pattern: NodePattern,
    *,
    bindings: dict[str, NodeRecord],
    next_node_id: Callable[[], int],
    new_nodes: list[NodeRecord],
) -> NodeRecord:
    """Resolve this reference into the concrete value or array used downstream."""
    existing = bindings.get(pattern.variable)
    if existing is not None:
        if pattern.labels or pattern.properties:
            raise QueryError(
                "CREATE cannot redefine variable "
                f"'{pattern.variable}' with inline labels or properties."
            )
        return existing

    node = NodeRecord(
        node_id=next_node_id(),
        labels=pattern.labels,
        properties=dict(pattern.properties),
    )
    bindings[pattern.variable] = node
    new_nodes.append(node)
    return node


def _node_matches_merge_pattern(node: NodeRecord, pattern: MergeNodePattern) -> bool:
    """Handle node-specific matching, projection, or lookup logic for Cypher."""
    if pattern.labels and not all(label in node.labels for label in pattern.labels):
        return False
    return _properties_match(node.properties, pattern.properties)


def _node_matches_node_pattern(node: NodeRecord, pattern: NodePattern) -> bool:
    """Handle node-specific matching, projection, or lookup logic for Cypher."""
    if pattern.labels and not all(label in node.labels for label in pattern.labels):
        return False
    return _properties_match(node.properties, pattern.properties)


def _project_value(value: object, field: str | None) -> Any:
    """Project bindings into the columns or values needed by the next step."""
    if isinstance(value, NodeRecord):
        if field is None:
            return {
                "id": value.node_id,
                "labels": list(value.labels),
                "properties": dict(value.properties),
            }
        if field in {_ENTITY_ID_FIELD, _NODE_ID_FIELD}:
            return value.node_id
        return value.properties.get(field)

    if isinstance(value, EdgeRecord):
        if field is None:
            return {
                "id": value.edge_id,
                "type": value.rel_type,
                "source": value.src_id,
                "target": value.dst_id,
                "properties": dict(value.properties),
            }
        if field in {"id", _ENTITY_ID_FIELD}:
            return value.edge_id
        if field == "type":
            return value.rel_type
        if field == "source":
            return value.src_id
        if field == "target":
            return value.dst_id
        return value.properties.get(field)

    raise QueryError(f"Unsupported bound value type: {type(value)!r}")


def _project_node_from_row(record: Mapping[str, Any], variable: str) -> dict[str, Any] | None:
    """Project bindings into the columns or values needed by the next step."""
    prefix = f"{variable}."
    node_id = record.get(_binding_column_name(variable, _NODE_ID_FIELD))
    if node_id is None:
        return None
    labels_value = record.get(f"{prefix}labels")
    labels = _decode_labels(labels_value)
    properties = {
        column.removeprefix(prefix): _decode_join_json_value(value)
        for column, value in record.items()
        if column.startswith(prefix)
        and column
        not in {
            _binding_column_name(variable, _ENTITY_ID_FIELD),
            _binding_column_name(variable, _NODE_ID_FIELD),
            f"{prefix}labels",
            f"{prefix}__join_id",
        }
        and value is not None
    }
    return {
        "id": node_id,
        "labels": list(labels),
        "properties": properties,
    }


def _project_edge_from_row(record: Mapping[str, Any], variable: str) -> dict[str, Any] | None:
    """Project bindings into the columns or values needed by the next step."""
    prefix = f"{variable}."
    edge_id = record.get(f"{prefix}id")
    if edge_id is None:
        return None
    properties = {
        column.removeprefix(prefix): _decode_join_json_value(value)
        for column, value in record.items()
        if column.startswith(prefix)
        and column
        not in {
            f"{prefix}id",
            f"{prefix}type",
            f"{prefix}source",
            f"{prefix}target",
        }
        and value is not None
    }
    return {
        "id": edge_id,
        "type": record.get(f"{prefix}type"),
        "source": record.get(f"{prefix}source"),
        "target": record.get(f"{prefix}target"),
        "properties": properties,
    }


def _project_bound_entity_from_row(
    record: Mapping[str, Any],
    variable: str,
) -> dict[str, Any] | None:
    """Project bindings into the columns or values needed by the next step."""
    if _binding_column_name(variable, "labels") in record:
        return _project_node_from_row(record, variable)
    if _binding_column_name(variable, "type") in record:
        return _project_edge_from_row(record, variable)
    raise QueryError(f"RETURN references an unknown bound entity: {variable}")


def _project_bound_entity_values_from_table(table: pa.Table, variable: str) -> list[Any]:
    """Project bindings into the columns or values needed by the next step."""
    if _is_node_binding_table(table, variable):
        return _project_node_values_from_table(table, variable)
    if _is_edge_binding_table(table, variable):
        return _project_edge_values_from_table(table, variable)
    raise QueryError(f"RETURN references an unknown bound entity: {variable}")


def _project_node_values_from_table(table: pa.Table, variable: str) -> list[dict[str, Any] | None]:
    """Project bindings into the columns or values needed by the next step."""
    prefix = f"{variable}."
    source_columns = [name for name in table.column_names if name.startswith(prefix)]
    if not source_columns:
        raise QueryError(f"RETURN references an unknown bound entity: {variable}")
    projected = table.select(source_columns)
    return [_project_node_from_row(record, variable) for record in projected.to_pylist()]


def _project_edge_values_from_table(table: pa.Table, variable: str) -> list[dict[str, Any] | None]:
    """Project bindings into the columns or values needed by the next step."""
    prefix = f"{variable}."
    source_columns = [name for name in table.column_names if name.startswith(prefix)]
    if not source_columns:
        raise QueryError(f"RETURN references an unknown bound entity: {variable}")
    projected = table.select(source_columns)
    return [_project_edge_from_row(record, variable) for record in projected.to_pylist()]


def _project_bound_field_from_row(record: Mapping[str, Any], variable: str, field: str) -> Any:
    """Project bindings into the columns or values needed by the next step."""
    value = _resolve_record_field_value(record, variable=variable, field=field)
    if field == "labels":
        if value is None:
            return None
        return _decode_labels(value)
    return _decode_join_json_value(value)


def _project_bound_field_values_from_table(
    table: pa.Table,
    variable: str,
    field: str,
) -> list[Any]:
    """Project bindings into the columns or values needed by the next step."""
    column = _resolve_table_field_array(table, variable=variable, field=field)
    if column is None:
        if _is_node_binding_table(table, variable) or _is_edge_binding_table(table, variable):
            return [None] * table.num_rows
        source_column = _binding_column_name(variable, field)
        raise QueryError(f"RETURN references an unknown binding: {source_column}")
    values = cast(list[Any], column.to_pylist())
    if field == "labels":
        return [None if value is None else _decode_labels(value) for value in values]
    return [_decode_join_json_value(value) for value in values]


def _project_with_items_table(table: pa.Table, with_items: tuple[WithItem, ...]) -> pa.Table:
    """Project bindings into the columns or values needed by the next step."""
    columns: dict[str, pa.ChunkedArray | pa.Array] = {}
    for item in with_items:
        if item.source_kind == "entity":
            prefix = f"{item.source_variable}."
            source_columns = [name for name in table.column_names if name.startswith(prefix)]
            if not source_columns:
                raise QueryError(
                    f"WITH references an unknown entity binding: {item.source_variable}"
                )
            for source_column in source_columns:
                field_name = source_column.removeprefix(prefix)
                columns[_binding_column_name(item.output_name, field_name)] = table[source_column]
            continue

        if item.source_field is None:
            raise QueryError("Scalar WITH projections require a source field.")
        source_array = _resolve_table_field_array(
            table,
            variable=item.source_variable,
            field=item.source_field,
        )
        if source_array is None:
            if _has_bound_variable_columns(table, item.source_variable):
                source_array = pa.nulls(table.num_rows)
            else:
                source_column = _binding_column_name(item.source_variable, item.source_field)
                raise QueryError(f"WITH references an unknown scalar binding: {source_column}")
        columns[_binding_column_name(item.output_name, "value")] = source_array

    if columns:
        return pa.table(columns)
    return table.slice(0, 0)


def _has_bound_variable_columns(table: pa.Table, variable: str) -> bool:
    """Return true when the table still carries any binding for this variable."""
    prefix = f"{variable}."
    return any(column_name.startswith(prefix) for column_name in table.column_names)


def _null_array_for_missing_bound_field(table: pa.Table, variable: str) -> pa.Array | None:
    """Return nulls for a missing property on an existing binding, else none."""
    if not _has_bound_variable_columns(table, variable):
        return None
    return pa.nulls(table.num_rows)


def _build_optional_null_table(column_names: list[str]) -> pa.Table:
    """Build the intermediate plan, table, or result used by later stages."""
    return pa.Table.from_pylist([_build_optional_null_row({}, column_names)])


def _build_optional_null_row(
    base_row: Mapping[str, Any],
    column_names: list[str],
) -> dict[str, Any]:
    """Build the intermediate plan, table, or result used by later stages."""
    row = dict(base_row)
    for column_name in column_names:
        row.setdefault(column_name, None)
    return row


def _with_missing_optional_columns(table: pa.Table, column_names: list[str]) -> pa.Table:
    """Support the current Cypher compilation or execution step."""
    completed = table
    for column_name in column_names:
        if column_name in completed.column_names:
            continue
        completed = completed.append_column(
            column_name,
            pa.array([None] * completed.num_rows, type=_optional_column_type(column_name)),
        )
    return completed


def _optional_column_type(column_name: str) -> pa.DataType:
    """Support the current Cypher compilation or execution step."""
    if column_name.endswith((".id", ".__join_id", ".source", ".target")):
        return pa.int64()
    if column_name.endswith((".labels", ".type")):
        return pa.string()
    return pa.null()


def _pattern_null_columns(patterns: tuple[CompiledPattern, ...]) -> list[str]:
    """Support the current Cypher compilation or execution step."""
    columns: list[str] = []
    for pattern in patterns:
        if isinstance(pattern, NodePattern):
            columns.extend(_node_null_columns(pattern.variable))
            continue

        columns.extend(_node_null_columns(pattern.start.variable))
        for step in pattern.steps:
            if step.variable is not None:
                columns.extend(_edge_null_columns(step.variable))
            columns.extend(_node_null_columns(step.node.variable))
    return list(dict.fromkeys(columns))


def _node_null_columns(variable: str) -> list[str]:
    """Handle node-specific matching, projection, or lookup logic for Cypher."""
    return [
        _binding_column_name(variable, _NODE_ID_FIELD),
        _binding_column_name(variable, "__join_id"),
        _binding_column_name(variable, "labels"),
    ]


def _edge_null_columns(variable: str) -> list[str]:
    """Handle edge-specific matching, projection, or lookup logic for Cypher."""
    return [
        _binding_column_name(variable, "id"),
        _binding_column_name(variable, "type"),
        _binding_column_name(variable, "source"),
        _binding_column_name(variable, "target"),
    ]


def _decode_labels(value: object) -> list[str]:
    """Decode this serialized internal value back into the execution form."""
    if value is None:
        return []
    if isinstance(value, str):
        labels = cast(list[str], json.loads(value))
    else:
        labels = list(cast(list[str], value))
    return [_cypher_name(label) for label in labels]


def _decode_join_json_value(value: object) -> object:
    """Decode nested values temporarily stringified before Arrow joins."""
    if isinstance(value, str) and value.startswith(_JOIN_JSON_PREFIX):
        return json.loads(value.removeprefix(_JOIN_JSON_PREFIX))
    return value


def _normalize_property_map(properties: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize escaped Cypher property keys loaded from existing graph files."""
    return {_cypher_name(str(key)): value for key, value in properties.items()}


def _normalize_property_table_schema(table: pa.Table) -> pa.Table:
    """Normalize escaped property-column names from derived Parquet datasets."""

    normalized_names = [_cypher_name(column_name) for column_name in table.column_names]
    if normalized_names == table.column_names:
        return table
    return table.rename_columns(normalized_names)


def _node_property_projection_columns(property_fields: set[str] | None) -> list[str] | None:
    """Return the minimal node property-table columns needed by one scan."""

    if property_fields is None:
        return None
    return ["node_id", *sorted(property_fields)]


def _edge_property_projection_columns(property_fields: set[str] | None) -> list[str] | None:
    """Return the minimal relationship property-table columns needed by one scan."""

    if property_fields is None:
        return None
    return ["edge_id", "src_id", "dst_id", *sorted(property_fields)]


def _escaped_property_projection_columns(columns: list[str] | None) -> list[str] | None:
    """Map logical property columns to their escaped Parquet names for imported datasets."""

    if columns is None:
        return None
    structural_columns = {"node_id", "edge_id", "src_id", "dst_id"}
    return [
        column if column in structural_columns or column.startswith("`") else f"`{column}`"
        for column in columns
    ]


def _load_property_table_with_column_fallback(
    load: Callable[[list[str] | None], pa.Table],
    *,
    columns: list[str] | None,
    escaped_columns: list[str] | None,
) -> pa.Table:
    """Read a projected property table, retrying escaped property names when needed."""

    try:
        return load(columns)
    except (pa.ArrowInvalid, KeyError):
        if escaped_columns is None or escaped_columns == columns:
            return load(None)
        try:
            return load(escaped_columns)
        except (pa.ArrowInvalid, KeyError):
            return load(None)


def _predicate_property_fields_for_variable(
    predicate: WherePredicate | None,
    variable: str,
) -> set[str]:
    """Return property fields referenced by a predicate for one variable."""

    if predicate is None:
        return set()
    if isinstance(predicate, Comparison):
        return {predicate.field} if predicate.variable == variable else set()
    if isinstance(predicate, (StringPredicate, ExistsPredicate, InPredicate, NullPredicate)):
        return {predicate.field} if predicate.variable == variable else set()
    if isinstance(predicate, LabelPredicate):
        return set()
    if isinstance(predicate, NotPredicate):
        return _predicate_property_fields_for_variable(predicate.predicate, variable)
    if isinstance(predicate, (AndPredicate, OrPredicate)):
        fields: set[str] = set()
        for child in predicate.predicates:
            fields.update(_predicate_property_fields_for_variable(child, variable))
        return fields
    return set()


def _property_keys_from_record(record: Mapping[str, Any], variable: str) -> list[str]:
    """Return non-null property names for a bound node or relationship row."""
    prefix = f"{variable}."
    reserved = _reserved_property_fields_for_record(record, variable)
    return [
        column.removeprefix(prefix)
        for column, value in record.items()
        if column.startswith(prefix)
        and column.removeprefix(prefix) not in reserved
        and value is not None
    ]


def _property_keys_array_from_table(table: pa.Table, variable: str) -> pa.Array:
    """Build a list<string> array containing property names present on each row."""
    property_columns = [
        column_name
        for column_name in table.column_names
        if column_name.startswith(f"{variable}.")
        and column_name.removeprefix(f"{variable}.")
        not in _reserved_property_fields_for_table(table, variable)
    ]
    if not property_columns:
        return pa.array([[] for _ in range(table.num_rows)], type=pa.list_(pa.string()))

    source_rows = table.select(property_columns).to_pylist()
    return pa.array(
        [
            [
                column.removeprefix(f"{variable}.")
                for column, value in row.items()
                if value is not None
            ]
            for row in source_rows
        ],
        type=pa.list_(pa.string()),
    )


def _reserved_property_fields_for_record(
    record: Mapping[str, Any],
    variable: str,
) -> set[str]:
    """Return technical fields that are not user properties for this binding."""
    if _binding_column_name(variable, "labels") in record:
        return {_ENTITY_ID_FIELD, _NODE_ID_FIELD, "__join_id", "labels"}
    if _binding_column_name(variable, "type") in record:
        return {"id", "type", "source", "target"}
    return set()


def _reserved_property_fields_for_table(table: pa.Table, variable: str) -> set[str]:
    """Return technical fields that are not user properties for this binding table."""
    if _is_node_binding_table(table, variable):
        return {_ENTITY_ID_FIELD, _NODE_ID_FIELD, "__join_id", "labels"}
    if _is_edge_binding_table(table, variable):
        return {"id", "type", "source", "target"}
    return set()


def _encode_function_field(function_name: str, source_field: str) -> str:
    """Encode this internal field name into the normalized planner form."""
    return f"{_FUNCTION_FIELD_PREFIX}{function_name}:{source_field}"


def _decode_function_field(field: str) -> tuple[str, str] | None:
    """Decode this serialized internal value back into the execution form."""
    if not field.startswith(_FUNCTION_FIELD_PREFIX):
        return None
    payload = field.removeprefix(_FUNCTION_FIELD_PREFIX)
    function_name, separator, source_field = payload.partition(":")
    if not separator:
        raise QueryError(f"Invalid function field encoding: {field}")
    return function_name, source_field


def _resolve_record_field_value(
    record: Mapping[str, Any],
    *,
    variable: str,
    field: str,
) -> Any:
    """Resolve this reference into the concrete value or array used downstream."""
    if field == "keys":
        return _property_keys_from_record(record, variable)

    if field == _ENTITY_ID_FIELD:
        node_id = record.get(_binding_column_name(variable, _NODE_ID_FIELD))
        if node_id is not None:
            return node_id
        return record.get(_binding_column_name(variable, "id"))

    function_field = _decode_function_field(field)
    if function_field is None:
        return record.get(_binding_column_name(variable, field))

    function_name, source_field = function_field
    source_value = record.get(_binding_column_name(variable, source_field))
    return _apply_scalar_function(function_name, source_value)


def _resolve_table_field_array(
    table: pa.Table,
    *,
    variable: str,
    field: str,
) -> pa.ChunkedArray | pa.Array | None:
    """Resolve this reference into the concrete value or array used downstream."""
    if field == "keys":
        if not _is_node_binding_table(table, variable) and not _is_edge_binding_table(
            table, variable
        ):
            return None
        return _property_keys_array_from_table(table, variable)

    if field == _ENTITY_ID_FIELD:
        node_column = _binding_column_name(variable, _NODE_ID_FIELD)
        if node_column in table.column_names:
            return table[node_column]
        edge_column = _binding_column_name(variable, "id")
        if edge_column in table.column_names:
            return table[edge_column]
        return None

    function_field = _decode_function_field(field)
    if function_field is None:
        column_name = _binding_column_name(variable, field)
        if column_name not in table.column_names:
            return None
        return table[column_name]

    function_name, source_field = function_field
    source_column = _binding_column_name(variable, source_field)
    if source_column not in table.column_names:
        return None
    return _apply_scalar_function_array(function_name, table[source_column])


def _ensure_order_column(
    table: pa.Table,
    *,
    variable: str,
    field: str,
    index: int,
) -> tuple[pa.Table, str]:
    """Support the current Cypher compilation or execution step."""
    function_field = _decode_function_field(field)
    if function_field is None:
        column_name = _binding_column_name(variable, field)
        if field == _ENTITY_ID_FIELD and column_name not in table.column_names:
            node_id_column = _binding_column_name(variable, _NODE_ID_FIELD)
            if node_id_column in table.column_names:
                return table, node_id_column
        if column_name not in table.column_names:
            raise QueryError(f"ORDER BY references an unknown binding: {column_name}")
        return table, column_name

    resolved = _resolve_table_field_array(table, variable=variable, field=field)
    if resolved is None:
        source_column = _binding_column_name(variable, function_field[1])
        raise QueryError(f"ORDER BY references an unknown binding: {source_column}")
    order_column = f"__order__.{index}"
    return table.append_column(order_column, resolved), order_column


def _apply_scalar_function(function_name: str, value: object) -> Any:
    """Apply this transformation or predicate inside the Cypher pipeline."""
    if value is None:
        return None
    if function_name == "tolower":
        if not isinstance(value, str):
            raise QueryError(f"{function_name}(...) requires a string-compatible operand.")
        return value.lower()
    if function_name == "toupper":
        if not isinstance(value, str):
            raise QueryError(f"{function_name}(...) requires a string-compatible operand.")
        return value.upper()
    if function_name == "size":
        if isinstance(value, str | list | tuple):
            return len(value)
        raise QueryError("size(...) requires a string- or list-compatible operand.")
    raise QueryError(f"Unsupported scalar function: {function_name}")


def _apply_scalar_function_array(
    function_name: str,
    column: pa.ChunkedArray,
) -> pa.Array | pa.ChunkedArray:
    """Apply this transformation or predicate inside the Cypher pipeline."""
    if function_name == "tolower":
        return pc.utf8_lower(column)
    if function_name == "toupper":
        return pc.utf8_upper(column)
    if function_name == "size":
        if pa.types.is_string(column.type) or pa.types.is_large_string(column.type):
            return pc.utf8_length(column)
        if pa.types.is_list(column.type) or pa.types.is_large_list(column.type):
            return pc.list_value_length(column)
        raise QueryError("size(...) requires a string- or list-compatible operand.")
    raise QueryError(f"Unsupported scalar function: {function_name}")


def _evaluate_scalar_expression_array(
    table: pa.Table,
    expression: ScalarExpression,
) -> pa.ChunkedArray | pa.Array:
    """Evaluate a small scalar expression against a binding table."""

    if isinstance(expression, FieldExpression):
        column = _resolve_table_field_array(
            table,
            variable=expression.variable,
            field=expression.field,
        )
        if column is None:
            column_name = _binding_column_name(expression.variable, expression.field)
            raise QueryError(f"Expression references an unknown binding: {column_name}")
        return column
    if isinstance(expression, LiteralExpression):
        return pa.array([expression.value] * table.num_rows)
    if isinstance(expression, FunctionExpression):
        argument = _evaluate_scalar_expression_array(table, expression.argument)
        if expression.name == "abs":
            return pc.abs(argument)
        return _apply_scalar_function_array(expression.name, argument)
    if isinstance(expression, BinaryExpression):
        left = _evaluate_scalar_expression_array(table, expression.left)
        right = _evaluate_scalar_expression_array(table, expression.right)
        if expression.operator == "+":
            return pc.add(left, right)
        if expression.operator == "-":
            return pc.subtract(left, right)
        if expression.operator == "*":
            return pc.multiply(left, right)
        if expression.operator == "/":
            return pc.divide(pc.cast(left, pa.float64()), pc.cast(right, pa.float64()))
    raise QueryError(f"Unsupported scalar expression: {expression!r}")


def _normalize_unwind_values(value: object) -> tuple[object, ...]:
    """Normalize this value into the shape expected by later execution steps."""
    if isinstance(value, list | tuple):
        return tuple(value)
    raise QueryError("UNWIND requires a list-like literal, parameter, or bound value.")


def _sort_key(binding: Mapping[str, Any], variable: str, field: str) -> Any:
    """Build the sort representation used by the current ordering path."""
    return _project_value(binding[variable], field)


def _node_matches_pattern(node: NodeRecord, pattern: NodePattern) -> bool:
    """Handle node-specific matching, projection, or lookup logic for Cypher."""
    if pattern.labels and not all(label in node.labels for label in pattern.labels):
        return False
    return _properties_match(node.properties, pattern.properties)


def _properties_match(actual: dict[str, object], expected: dict[str, object]) -> bool:
    """Support the current Cypher compilation or execution step."""
    return all(actual.get(key) == value for key, value in expected.items())


def _can_bind_node(binding: Mapping[str, Any], variable: str, node: NodeRecord) -> bool:
    """Check whether the optimized or semantically valid path applies here."""
    existing = binding.get(variable)
    if existing is None:
        return True
    if not isinstance(existing, NodeRecord):
        return False
    return existing.node_id == node.node_id


def _parse_value(text: str, params: dict[str, object]) -> object:
    """Parse this low-level fragment into the normalized internal form."""
    if text.startswith("$"):
        return _resolve_parameter(text, params)

    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*", text):
        variable, field = _parse_property_reference(text)
        return PropertyReferenceValue(variable=variable, field=field)

    if text.startswith("{") and text.endswith("}"):
        return _parse_map_literal(text, params)

    if text.startswith("'") and text.endswith("'"):
        return text[1:-1]
    if text.startswith('"') and text.endswith('"'):
        return text[1:-1]

    if text.upper() == "TRUE":
        return True
    if text.upper() == "FALSE":
        return False
    if text.upper() == "NULL":
        return None

    if _IDENTIFIER_RE.fullmatch(text):
        return PropertyReferenceValue(variable=text, field="value")

    if re.fullmatch(r"-?\d+", text):
        return int(text)
    if re.fullmatch(r"-?\d+\.\d+", text):
        return float(text)

    raise QueryError(f"Unsupported literal value: {text}")


def _parse_map_literal(text: str, params: dict[str, object]) -> dict[str, object]:
    """Parse this low-level fragment into the normalized internal form."""
    body = text[1:-1].strip()
    if not body:
        return {}

    result: dict[str, object] = {}
    # The bootstrap engine only needs a shallow Cypher map literal here, but we still split
    # at top level so nested literals and quoted strings remain intact.
    for item in _split_top_level_items(body):
        separator_index = _find_top_level_separator(item, ":")
        if separator_index < 0:
            raise QueryError(f"Invalid map literal entry: {item}")
        key = item[:separator_index].strip()
        value_text = item[separator_index + 1 :].strip()
        if not _IDENTIFIER_RE.fullmatch(key):
            raise QueryError(f"Invalid map literal key: {key}")
        result[key] = _parse_value(value_text, params)
    return result


def _split_top_level_items(text: str) -> list[str]:
    """Split this text while preserving the grouping rules expected here."""
    items: list[str] = []
    current: list[str] = []
    depth = 0
    quote: str | None = None

    for character in text:
        if quote is not None:
            current.append(character)
            if character == quote:
                quote = None
            continue

        if character in {"'", '"'}:
            quote = character
            current.append(character)
            continue
        if character in "{[(":
            depth += 1
            current.append(character)
            continue
        if character in "}])":
            depth -= 1
            current.append(character)
            continue
        if character == "," and depth == 0:
            item = "".join(current).strip()
            if item:
                items.append(item)
            current = []
            continue
        current.append(character)

    trailing = "".join(current).strip()
    if trailing:
        items.append(trailing)
    return items


def _find_top_level_separator(text: str, separator: str) -> int:
    """Find the existing item or separator needed by this helper."""
    depth = 0
    quote: str | None = None

    for index, character in enumerate(text):
        if quote is not None:
            if character == quote:
                quote = None
            continue
        if character in {"'", '"'}:
            quote = character
            continue
        if character in "{[(":
            depth += 1
            continue
        if character in "}])":
            depth -= 1
            continue
        if character == separator and depth == 0:
            return index
    return -1


def _extract_list_literal(
    expression_ctx: CypherParser.OC_AddOrSubtractExpressionContext,
) -> CypherParser.OC_ListLiteralContext | None:
    """Extract the requested fragment for reuse by later compiler helpers."""
    multiply_expressions = cast(
        list[CypherParser.OC_MultiplyDivideModuloExpressionContext],
        expression_ctx.oC_MultiplyDivideModuloExpression(),
    )
    if len(multiply_expressions) != 1:
        return None

    power_expressions = cast(
        list[CypherParser.OC_PowerOfExpressionContext],
        multiply_expressions[0].oC_PowerOfExpression(),
    )
    if len(power_expressions) != 1:
        return None

    unary_expressions = cast(
        list[CypherParser.OC_UnaryAddOrSubtractExpressionContext],
        power_expressions[0].oC_UnaryAddOrSubtractExpression(),
    )
    if len(unary_expressions) != 1:
        return None

    non_arithmetic = unary_expressions[0].oC_NonArithmeticOperatorExpression()
    if non_arithmetic is None:
        return None
    if non_arithmetic.oC_ListOperatorExpression():
        return None
    if non_arithmetic.oC_NodeLabels() is not None:
        return None
    if non_arithmetic.oC_PropertyLookup():
        return None

    atom = non_arithmetic.oC_Atom()
    if atom is None:
        return None
    literal = atom.oC_Literal()
    if literal is None:
        return None
    return cast(CypherParser.OC_ListLiteralContext | None, literal.oC_ListLiteral())


def _extract_list_literal_from_expression(
    expression_ctx: CypherParser.OC_ExpressionContext,
) -> CypherParser.OC_ListLiteralContext | None:
    """Extract the requested fragment for reuse by later compiler helpers."""
    or_expressions = cast(list[Any], expression_ctx.oC_OrExpression().oC_XorExpression())
    if len(or_expressions) != 1:
        return None
    and_expressions = cast(list[Any], or_expressions[0].oC_AndExpression())
    if len(and_expressions) != 1:
        return None
    not_expressions = cast(list[Any], and_expressions[0].oC_NotExpression())
    if len(not_expressions) != 1:
        return None
    comparison = not_expressions[0].oC_ComparisonExpression()
    if comparison is None:
        return None
    partials = cast(list[Any], comparison.oC_PartialComparisonExpression())
    if partials:
        return None
    predicate = comparison.oC_StringListNullPredicateExpression()
    if predicate is None:
        return None
    return _extract_list_literal(predicate.oC_AddOrSubtractExpression())


def _parse_property_reference(text: str) -> tuple[str, str]:
    """Parse this low-level fragment into the normalized internal form."""
    if "." not in text:
        raise QueryError(f"Expected property reference, got: {text}")
    variable, field = text.split(".", 1)
    return variable.strip(), _cypher_name(field.strip())


def _cypher_name(text: str) -> str:
    """Normalize escaped Cypher schema/property names to their semantic value."""
    if len(text) >= 2 and text.startswith("`") and text.endswith("`"):
        return text[1:-1].replace("``", "`")
    return text


def _binding_column_name(variable: str, field: str) -> str:
    """Construct the binding column names used by the tabular execution model."""
    return f"{variable}.{field}"


def _entity_variables_from_table(table: pa.Table) -> set[str]:
    """Support the current Cypher compilation or execution step."""
    variables: set[str] = set()
    for column_name in table.column_names:
        if not column_name.endswith(".__join_id"):
            continue
        variables.add(column_name.removesuffix(".__join_id"))
    return variables


def _scope_from_patterns(patterns: tuple[CompiledPattern, ...]) -> dict[str, ScopeBindingKind]:
    """Build or extend the scope information used during compilation."""
    scope: dict[str, ScopeBindingKind] = {}
    for pattern in patterns:
        for variable in _pattern_public_variables(pattern):
            scope[variable] = "entity"
    return scope


def _extend_scope_with_patterns(
    base_scope: Mapping[str, ScopeBindingKind],
    patterns: tuple[CompiledPattern, ...],
) -> dict[str, ScopeBindingKind]:
    """Build or extend the scope information used during compilation."""
    scope = dict(base_scope)
    for pattern in patterns:
        for variable in _pattern_public_variables(pattern):
            existing = scope.get(variable)
            if existing == "scalar":
                raise QueryError(
                    f"MATCH after WITH cannot reuse scalar binding '{variable}' as a graph entity."
                )
            scope[variable] = "entity"
    return scope


def _extend_scope_with_create_patterns(
    base_scope: Mapping[str, ScopeBindingKind],
    create_patterns: tuple[CreateRelationshipPattern, ...],
) -> dict[str, ScopeBindingKind]:
    """Add variables introduced by MATCH ... CREATE to the RETURN/ORDER BY scope."""
    scope = dict(base_scope)
    for pattern in create_patterns:
        scope[pattern.left_variable] = "entity"
        scope[pattern.right_variable] = "entity"
    return scope


def _pattern_variables(pattern: CompiledPattern) -> tuple[str, ...]:
    """Support the current Cypher compilation or execution step."""
    if isinstance(pattern, NodePattern):
        return (pattern.variable,)
    variables: list[str] = [pattern.start.variable]
    for step in pattern.steps:
        if step.variable is not None:
            variables.append(step.variable)
        variables.append(step.node.variable)
    return tuple(variables)


def _pattern_public_variables(pattern: CompiledPattern) -> tuple[str, ...]:
    """Support the current Cypher compilation or execution step."""
    if isinstance(pattern, NodePattern):
        return (pattern.variable,)

    variables: list[str] = [pattern.start.variable]
    for step in pattern.steps:
        if step.variable is not None:
            variables.append(step.variable)
        variables.append(step.node.variable)
    return tuple(variables)


def _public_pattern_variables(pattern: CompiledPattern) -> tuple[str, ...]:
    """Support the current Cypher compilation or execution step."""
    variables: list[str] = []
    for variable in _pattern_public_variables(pattern):
        if _is_internal_variable(variable):
            continue
        if variable in variables:
            continue
        variables.append(variable)
    return tuple(variables)


def _is_internal_variable(variable: str) -> bool:
    """Check whether the optimized or semantically valid path applies here."""
    return variable.startswith(_INTERNAL_VARIABLE_PREFIX)


def _requested_fields_for_variable(
    required_fields: Mapping[str, set[str]] | None,
    variable: str,
) -> set[str] | None:
    """Support the current Cypher compilation or execution step."""
    if required_fields is None:
        return None
    fields = required_fields.get(variable)
    if fields is None:
        return set()
    if _FULL_ENTITY_FIELDS in fields:
        return None
    return set(fields)


def _requested_fields_for_edge_step(
    required_fields: Mapping[str, set[str]] | None,
    edge_prefix: str,
    step: RelationshipStep,
) -> set[str] | None:
    """Support the current Cypher compilation or execution step."""
    base_fields = _requested_fields_for_variable(required_fields, edge_prefix)
    if not step.properties:
        return base_fields
    merged = set(base_fields or set())
    merged.update(step.properties)
    return merged


def _project_public_pattern_columns(
    table: pa.Table,
    pattern: CompiledPattern,
    required_fields: Mapping[str, set[str]] | None = None,
) -> pa.Table:
    """Project bindings into the columns or values needed by the next step."""
    if table.num_rows == 0 and required_fields is not None:
        # An empty expansion can finish before its target-node join. Preserve
        # requested field names so projecting that empty result remains valid.
        for variable, fields in required_fields.items():
            for field in sorted(fields):
                column = _binding_column_name(variable, field)
                if column not in table.column_names:
                    table = table.append_column(column, pa.nulls(0))
    columns: list[str] = []
    for variable in _public_pattern_variables(pattern):
        prefix = f"{variable}."
        columns.extend(column for column in table.column_names if column.startswith(prefix))
    if not columns:
        return table
    return table.select(columns)


def _project_joinable_pattern_table(
    table: pa.Table,
    *,
    pattern_variables: set[str],
    shared_variables: set[str],
) -> pa.Table:
    """Project bindings into the columns or values needed by the next step."""
    columns: list[str] = []
    for variable in sorted(pattern_variables):
        if variable in shared_variables:
            columns.append(_binding_column_name(variable, "__join_id"))
            continue
        prefix = f"{variable}."
        columns.extend(column for column in table.column_names if column.startswith(prefix))
    return table.select(columns)


def _ensure_join_id_column(table: pa.Table, variable: str) -> pa.Table:
    """Support the current Cypher compilation or execution step."""
    join_column = _binding_column_name(variable, "__join_id")
    if join_column in table.column_names:
        return table
    id_column = _binding_column_name(variable, _NODE_ID_FIELD)
    if id_column not in table.column_names:
        return table
    return table.append_column(join_column, table[id_column])


def _uses_typed_adjacency_path(
    step: RelationshipStep,
    requested_fields: set[str] | None,
) -> bool:
    """Support the current Cypher compilation or execution step."""
    if requested_fields is None:
        return False
    has_typed_relationship = step.rel_type is not None or len(step.rel_type_alternatives) > 1
    return has_typed_relationship and not step.properties and requested_fields.issubset({"type"})


def _has_relationship_type_alternatives(step: RelationshipStep) -> bool:
    """Return true when a relationship step can match more than one concrete type."""

    return len(step.rel_type_alternatives) > 1


def _edge_match_table(
    edges: EdgeBindingSource,
    edge_prefix: str,
    step: RelationshipStep,
    *,
    requested_fields: set[str] | None,
    adjacency_by_source_loader: Callable[[str], pa.Table] | None = None,
    edge_property_loader: PropertyTableLoader | None = None,
    source_ids: pa.Array | pa.ChunkedArray | None = None,
) -> pa.Table:
    """Handle edge-specific matching, projection, or lookup logic for Cypher."""
    select_edges = getattr(adjacency_by_source_loader, "select_edges", None)
    if source_ids is not None and select_edges is not None and requested_fields is not None:
        selected = select_edges(step, source_ids)
        if selected is not None:
            if _uses_typed_adjacency_path(step, requested_fields):
                # Keep the established per-type derived ordering for Cypher's
                # table joins; ANA independently retains canonical edge order.
                tables = []
                names = (
                    step.rel_type_alternatives
                    if len(step.rel_type_alternatives) > 1
                    else (cast(str, step.rel_type),)
                )
                types = pa.array(
                    [_cypher_name(value) for value in selected["rel_type"].to_pylist()],
                    type=pa.string(),
                )
                for name in names:
                    table = selected.filter(pc.equal(types, name))
                    sort = (
                        ("src_id", "dst_id")
                        if step.direction == "outgoing"
                        else ("dst_id", "src_id")
                    )
                    table = table.sort_by([(key, "ascending") for key in (*sort, "edge_id")])
                    tables.append(
                        _typed_adjacency_binding_table(
                            table,
                            edge_prefix,
                            rel_type=name,
                            include_type="type" in requested_fields or len(names) > 1,
                        )
                    )
                return pa.concat_tables(tables)
            narrowed = _narrow_edge_property_loader(edge_property_loader, selected["edge_id"])
            return _edge_binding_table(
                selected,
                edge_prefix,
                requested_fields=requested_fields,
                step=step,
                edge_property_loader=narrowed,
            )
    if (
        _uses_typed_adjacency_path(step, requested_fields)
        and adjacency_by_source_loader is not None
    ):
        load_adjacency = adjacency_by_source_loader
        if step.direction == "incoming":
            load_adjacency = cast(
                Callable[[str], pa.Table],
                getattr(adjacency_by_source_loader, "by_dest", adjacency_by_source_loader),
            )
        rel_types = (
            step.rel_type_alternatives
            if len(step.rel_type_alternatives) > 1
            else (cast(str, step.rel_type),)
        )
        adjacency_tables = tuple(load_adjacency(rel_type) for rel_type in rel_types)
        if step.direction == "incoming" and any(table.num_rows == 0 for table in adjacency_tables):
            fallback = _edge_binding_table(
                edges,
                edge_prefix,
                requested_fields=requested_fields,
                step=step,
                edge_property_loader=edge_property_loader,
            )
            return _apply_edge_pattern_filter(fallback, edge_prefix, step)
        return _typed_adjacency_binding_table_for_loaded_types(
            adjacency_tables,
            edge_prefix=edge_prefix,
            rel_types=rel_types,
            include_type=(
                requested_fields is not None
                and ("type" in requested_fields or len(step.rel_type_alternatives) > 1)
            ),
        )
    return _edge_binding_table(
        edges,
        edge_prefix,
        requested_fields=requested_fields,
        step=step,
        edge_property_loader=edge_property_loader,
    )


def _narrow_edge_property_loader(
    loader: PropertyTableLoader | None, edge_ids: pa.ChunkedArray
) -> PropertyTableLoader | None:
    """Preserve physical projection types and residual filters after topology selection."""
    if loader is None:
        return None

    def load(
        rel_type: str, fields: set[str] | None = None, predicates: tuple[ScanPredicate, ...] = ()
    ) -> pa.Table:
        if predicates and isinstance(loader, _PropertyProjectionLoader) and loader.scan is not None:
            table = loader.scan(rel_type, fields, predicates)
        else:
            table = loader(rel_type, fields)
        if "edge_id" not in table.column_names:
            return table
        return table.filter(pc.is_in(table["edge_id"], value_set=edge_ids))

    return _PropertyProjectionLoader(load, None, load)


def _typed_adjacency_binding_table_for_loaded_types(
    adjacency_tables: tuple[pa.Table, ...],
    *,
    edge_prefix: str,
    rel_types: tuple[str, ...],
    include_type: bool,
) -> pa.Table:
    """Build an edge binding table from already loaded typed adjacency tables."""

    tables = [
        _typed_adjacency_binding_table(
            adjacency_table,
            edge_prefix,
            rel_type=rel_type,
            include_type=include_type,
        )
        for adjacency_table, rel_type in zip(adjacency_tables, rel_types, strict=True)
    ]
    if not tables:
        return _typed_adjacency_binding_table(
            pa.table(
                {
                    "edge_id": pa.array([], type=pa.int64()),
                    "src_id": pa.array([], type=pa.int64()),
                    "dst_id": pa.array([], type=pa.int64()),
                }
            ),
            edge_prefix,
            rel_type="",
            include_type=include_type,
        )
    return pa.concat_tables(tables)


def _typed_adjacency_binding_table_for_types(
    load_adjacency: Callable[[str], pa.Table],
    *,
    edge_prefix: str,
    rel_types: tuple[str, ...],
    include_type: bool,
) -> pa.Table:
    """Build an edge binding table by reading one typed adjacency table per relationship type."""

    return _typed_adjacency_binding_table_for_loaded_types(
        tuple(load_adjacency(rel_type) for rel_type in rel_types),
        edge_prefix=edge_prefix,
        rel_types=rel_types,
        include_type=include_type,
    )


def _typed_adjacency_binding_table(
    adjacency_table: pa.Table,
    edge_prefix: str,
    *,
    rel_type: str,
    include_type: bool,
) -> pa.Table:
    """Support the current Cypher compilation or execution step."""
    if adjacency_table.num_rows == 0:
        empty_columns: dict[str, pa.Array] = {
            f"{edge_prefix}.id": pa.array([], type=pa.int64()),
            f"{edge_prefix}.source": pa.array([], type=pa.int64()),
            f"{edge_prefix}.target": pa.array([], type=pa.int64()),
        }
        if include_type:
            empty_columns[f"{edge_prefix}.type"] = pa.array([], type=pa.string())
        return pa.table(empty_columns)
    columns: dict[str, pa.Array | pa.ChunkedArray] = {
        f"{edge_prefix}.id": adjacency_table["edge_id"],
        f"{edge_prefix}.source": adjacency_table["src_id"],
        f"{edge_prefix}.target": adjacency_table["dst_id"],
    }
    if include_type:
        columns[f"{edge_prefix}.type"] = pa.array(
            [rel_type] * adjacency_table.num_rows,
            type=pa.string(),
        )
    return pa.table(columns)


def _property_scan_predicates(
    variable: str, properties: dict[str, object], predicate: WherePredicate | None
) -> tuple[ScanPredicate, ...]:
    """Offer scalar conjuncts only; the existing binding-table filters remain authoritative."""

    def scalar(value: object) -> bool:
        return value is None or type(value) in {str, bool, int, float}

    predicates = [
        ScanPredicate(field, "=", value) for field, value in properties.items() if scalar(value)
    ]
    for conjunct in _flatten_and_predicates(predicate) if predicate is not None else ():
        if getattr(conjunct, "variable", None) != variable:
            continue
        if (
            isinstance(conjunct, Comparison)
            and conjunct.left_expression is None
            and conjunct.operator in {"=", "!=", "<", "<=", ">", ">="}
            and scalar(conjunct.value)
        ):
            predicates.append(
                ScanPredicate(conjunct.field, cast(ScanOperator, conjunct.operator), conjunct.value)
            )
        elif isinstance(conjunct, InPredicate) and all(scalar(value) for value in conjunct.values):
            predicates.append(ScanPredicate(conjunct.field, "in", conjunct.values))
        elif isinstance(conjunct, NullPredicate):
            predicates.append(
                ScanPredicate(conjunct.field, "is_null" if conjunct.is_null else "is_not_null")
            )
        elif isinstance(conjunct, ExistsPredicate):
            predicates.append(ScanPredicate(conjunct.field, "is_not_null"))
    return tuple(predicates)


def _node_binding_table(
    nodes: NodeBindingSource,
    variable: str,
    *,
    requested_fields: set[str] | None = None,
    pattern: NodePattern | None = None,
    node_property_loader: PropertyTableLoader | None = None,
    pushdown_predicate: WherePredicate | None = None,
) -> pa.Table:
    """Handle node-specific matching, projection, or lookup logic for Cypher."""
    if isinstance(nodes, pa.Table):
        if (
            pattern is not None
            and node_property_loader is not None
            and _can_use_node_property_projection(pattern, requested_fields)
        ):
            assert requested_fields is not None
            property_fields = _requested_node_property_fields(
                pattern,
                requested_fields,
                pushdown_predicate,
            )
            property_table = None
            if (
                isinstance(node_property_loader, _PropertyProjectionLoader)
                and node_property_loader.lookup is not None
            ):
                equalities = dict(pattern.properties)
                for conjunct in (
                    _flatten_and_predicates(pushdown_predicate)
                    if pushdown_predicate is not None
                    else ()
                ):
                    if (
                        isinstance(conjunct, Comparison)
                        and conjunct.variable == variable
                        and conjunct.left_expression is None
                        and conjunct.operator == "="
                        and not isinstance(
                            conjunct.value, PropertyReferenceValue | ScalarExpression
                        )
                    ):
                        equalities.setdefault(conjunct.field, conjunct.value)
                property_table = node_property_loader.lookup(pattern.labels[0], equalities)
            if property_table is None:
                predicates = _property_scan_predicates(
                    variable, pattern.properties, pushdown_predicate
                )
                if (
                    predicates
                    and isinstance(node_property_loader, _PropertyProjectionLoader)
                    and node_property_loader.scan is not None
                ):
                    property_table = node_property_loader.scan(
                        pattern.labels[0], property_fields, predicates
                    )
                else:
                    property_table = node_property_loader(pattern.labels[0], property_fields)
            if _can_project_requested_node_fields(
                pattern=pattern,
                requested_fields=requested_fields,
                property_table=property_table,
            ):
                return _node_binding_table_from_property_projection(
                    nodes,
                    variable,
                    pattern=pattern,
                    requested_fields=requested_fields,
                    property_table=property_table,
                    pushdown_predicate=pushdown_predicate,
                )
        return _node_binding_table_from_canonical_table(
            nodes,
            variable,
            requested_fields=requested_fields,
        )
    node_records = cast(tuple[NodeRecord, ...], nodes)
    reserved_fields = {_NODE_ID_FIELD, _ENTITY_ID_FIELD, "__join_id", "labels"}
    if requested_fields is None:
        property_names = sorted({key for node in node_records for key in node.properties})
    else:
        available_properties = {key for node in node_records for key in node.properties}
        property_names = sorted(
            field
            for field in requested_fields
            if field not in reserved_fields and field in available_properties
        )
    rows = [
        {
            _binding_column_name(variable, _NODE_ID_FIELD): node.node_id,
            _binding_column_name(variable, "__join_id"): node.node_id,
            _binding_column_name(variable, "labels"): json.dumps(list(node.labels)),
            **{
                _binding_column_name(variable, property_name): node.properties.get(property_name)
                for property_name in property_names
            },
        }
        for node in node_records
    ]
    if rows:
        return pa.Table.from_pylist(rows)
    return pa.table(
        {
            _binding_column_name(variable, _NODE_ID_FIELD): pa.array([], type=pa.int64()),
            _binding_column_name(variable, "__join_id"): pa.array([], type=pa.int64()),
            _binding_column_name(variable, "labels"): pa.array([], type=pa.string()),
        }
    )


def _edge_binding_table(
    edges: EdgeBindingSource,
    prefix: str,
    *,
    requested_fields: set[str] | None = None,
    step: RelationshipStep | None = None,
    edge_property_loader: PropertyTableLoader | None = None,
) -> pa.Table:
    """Handle edge-specific matching, projection, or lookup logic for Cypher."""
    if isinstance(edges, pa.Table):
        if (
            step is not None
            and edge_property_loader is not None
            and _can_use_edge_property_projection(step, requested_fields)
        ):
            assert step.rel_type is not None
            assert requested_fields is not None
            property_fields = _requested_edge_property_fields(step, requested_fields)
            predicates = _property_scan_predicates(step.variable or "", step.properties, None)
            if (
                predicates
                and isinstance(edge_property_loader, _PropertyProjectionLoader)
                and edge_property_loader.scan is not None
            ):
                property_table = edge_property_loader.scan(
                    step.rel_type, property_fields, predicates
                )
            else:
                property_table = edge_property_loader(step.rel_type, property_fields)
            if _can_project_requested_edge_fields(
                step=step,
                requested_fields=requested_fields,
                property_table=property_table,
            ):
                return _edge_binding_table_from_property_projection(
                    property_table,
                    prefix,
                    rel_type=step.rel_type,
                    requested_fields=requested_fields,
                )
        return _edge_binding_table_from_canonical_table(
            edges,
            prefix,
            requested_fields=requested_fields,
        )
    edge_records = cast(tuple[EdgeRecord, ...], edges)
    reserved_fields = {"id", "type", "source", "target"}
    if requested_fields is None:
        property_names = sorted({key for edge in edge_records for key in edge.properties})
    else:
        available_properties = {key for edge in edge_records for key in edge.properties}
        property_names = sorted(
            field
            for field in requested_fields
            if field not in reserved_fields and field in available_properties
        )
    rows = [
        {
            f"{prefix}.id": edge.edge_id,
            f"{prefix}.type": edge.rel_type,
            f"{prefix}.source": edge.src_id,
            f"{prefix}.target": edge.dst_id,
            **{
                f"{prefix}.{property_name}": edge.properties.get(property_name)
                for property_name in property_names
            },
        }
        for edge in edge_records
    ]
    if rows:
        return pa.Table.from_pylist(rows)
    return pa.table(
        {
            f"{prefix}.id": pa.array([], type=pa.int64()),
            f"{prefix}.type": pa.array([], type=pa.string()),
            f"{prefix}.source": pa.array([], type=pa.int64()),
            f"{prefix}.target": pa.array([], type=pa.int64()),
        }
    )


def _node_binding_table_from_canonical_table(
    table: pa.Table,
    variable: str,
    *,
    requested_fields: set[str] | None,
) -> pa.Table:
    """Handle node-specific matching, projection, or lookup logic for Cypher."""
    reserved_fields = {_NODE_ID_FIELD, _ENTITY_ID_FIELD, "__join_id", "labels"}
    if requested_fields is None:
        properties = [
            _normalize_property_map(json.loads(value))
            for value in table["properties_json"].to_pylist()
        ]
        property_names = sorted({key for item in properties for key in item})
    else:
        requested_property_fields = sorted(
            field for field in requested_fields if field not in reserved_fields
        )
        if requested_property_fields:
            properties = [
                _normalize_property_map(json.loads(value))
                for value in table["properties_json"].to_pylist()
            ]
        else:
            properties = []
        available_properties = {key for item in properties for key in item}
        property_names = sorted(
            field for field in requested_property_fields if field in available_properties
        )

    columns: dict[str, pa.Array | pa.ChunkedArray] = {
        _binding_column_name(variable, _ENTITY_ID_FIELD): table["node_id"],
        _binding_column_name(variable, _NODE_ID_FIELD): table["node_id"],
        _binding_column_name(variable, "__join_id"): table["node_id"],
        _binding_column_name(variable, "labels"): _encode_binding_labels(
            table["labels"], normalize=True
        ),
    }
    for property_name in property_names:
        columns[_binding_column_name(variable, property_name)] = pa.array(
            [item.get(property_name) for item in properties]
        )
    return pa.table(columns)


def _can_use_node_property_projection(
    pattern: NodePattern,
    requested_fields: set[str] | None,
) -> bool:
    """Check whether the optimized or semantically valid path applies here."""
    if requested_fields is None or len(pattern.labels) != 1:
        return False
    if any(isinstance(value, PropertyReferenceValue) for value in pattern.properties.values()):
        return False
    property_fields = {
        field
        for field in requested_fields
        if field not in {_NODE_ID_FIELD, _ENTITY_ID_FIELD, "__join_id", "labels"}
    }
    if pattern.properties:
        property_fields.update(pattern.properties)
    return bool(property_fields)


def _requested_node_property_fields(
    pattern: NodePattern,
    requested_fields: set[str],
    pushdown_predicate: WherePredicate | None,
) -> set[str]:
    """Return the logical node property columns needed by a property projection."""

    reserved_fields = {_NODE_ID_FIELD, _ENTITY_ID_FIELD, "__join_id", "labels"}
    property_fields = {field for field in requested_fields if field not in reserved_fields}
    property_fields.update(pattern.properties)
    property_fields.update(
        _predicate_property_fields_for_variable(pushdown_predicate, pattern.variable)
    )
    return property_fields


def _node_binding_table_from_property_projection(
    canonical_table: pa.Table,
    variable: str,
    *,
    pattern: NodePattern,
    requested_fields: set[str],
    property_table: pa.Table,
    pushdown_predicate: WherePredicate | None = None,
) -> pa.Table:
    """Handle node-specific matching, projection, or lookup logic for Cypher."""
    restrict_candidates = _node_property_projection_restricts_candidates(
        property_table,
        variable=variable,
        pattern=pattern,
        predicate=pushdown_predicate,
    )
    property_table = _apply_node_property_table_pattern_filter(property_table, pattern)
    property_table = _apply_node_property_table_pushdown(
        property_table,
        variable=variable,
        predicate=pushdown_predicate,
    )
    candidates = canonical_table.select(["node_id", "labels"])
    if restrict_candidates:
        candidates = candidates.filter(
            pc.is_in(candidates["node_id"], value_set=property_table["node_id"])
        )
    filtered = candidates.filter(
        _canonical_labels_contains_all_mask(candidates["labels"], pattern.labels)
    )
    if filtered.num_rows == 0:
        empty = pa.table(
            {
                _binding_column_name(variable, _ENTITY_ID_FIELD): pa.array([], type=pa.int64()),
                _binding_column_name(variable, _NODE_ID_FIELD): pa.array([], type=pa.int64()),
                _binding_column_name(variable, "__join_id"): pa.array([], type=pa.int64()),
                _binding_column_name(variable, "labels"): pa.array([], type=pa.string()),
            }
        )

        for field in sorted(requested_fields):
            name = _binding_column_name(variable, field)
            if field in property_table.column_names and name not in empty.column_names:
                empty = empty.append_column(name, property_table[field].slice(0, 0))
        return empty

    base = pa.table(
        {
            _binding_column_name(variable, _ENTITY_ID_FIELD): filtered["node_id"],
            _binding_column_name(variable, _NODE_ID_FIELD): filtered["node_id"],
            _binding_column_name(variable, "__join_id"): filtered["node_id"],
            _binding_column_name(variable, "labels"): _encode_binding_labels(filtered["labels"]),
        }
    )
    if restrict_candidates:
        base = base.join(
            property_table.select(["node_id"]),
            keys=_binding_column_name(variable, _NODE_ID_FIELD),
            right_keys="node_id",
            join_type="inner",
        )
        base = base.select(
            [column_name for column_name in base.column_names if column_name != "node_id"]
        )
    if property_table.num_rows == 0:
        projected = base
    else:
        projection_columns = {
            "node_id": property_table["node_id"],
        }
        for field in sorted(
            field
            for field in requested_fields
            if field not in {_NODE_ID_FIELD, _ENTITY_ID_FIELD, "__join_id", "labels"}
        ):
            if field in property_table.column_names:
                projection_columns[field] = property_table[field]
        projected = base.join(
            pa.table(projection_columns),
            keys=_binding_column_name(variable, _NODE_ID_FIELD),
            right_keys="node_id",
            join_type="left outer",
        )
    for field in sorted(
        field
        for field in requested_fields
        if field not in {_NODE_ID_FIELD, _ENTITY_ID_FIELD, "__join_id", "labels"}
    ):
        column_name = _binding_column_name(variable, field)
        if field in projected.column_names and column_name not in projected.column_names:
            projected = projected.append_column(column_name, projected[field])
        if column_name not in projected.column_names:
            projected = projected.append_column(
                column_name,
                pa.array([None] * projected.num_rows),
            )

    drop_columns = [column for column in requested_fields if column in projected.column_names]
    if "node_id" in projected.column_names:
        drop_columns.append("node_id")
    kept_columns = [
        column_name for column_name in projected.column_names if column_name not in drop_columns
    ]
    return projected.select(kept_columns)


def _encode_binding_labels(column: pa.ChunkedArray, *, normalize: bool = False) -> pa.Array:
    """Reuse the JSON encoding for nodes with the same ordered label list."""
    encoded: dict[tuple[str, ...], str] = {}
    rows = []
    for labels in column.to_pylist():
        key = tuple(labels or ())
        if key not in encoded:
            values = [_cypher_name(label) for label in key] if normalize else list(key)
            encoded[key] = json.dumps(values)
        rows.append(encoded[key])
    return pa.array(rows, type=pa.string())


def _node_property_projection_restricts_candidates(
    property_table: pa.Table,
    *,
    variable: str,
    pattern: NodePattern,
    predicate: WherePredicate | None,
) -> bool:
    """Return true when property-side filters can reduce candidate node ids early."""

    if pattern.properties:
        return True
    if predicate is None:
        return False
    return any(
        _node_property_table_predicate_mask(
            property_table,
            variable=variable,
            predicate=conjunct,
        )
        is not None
        for conjunct in _flatten_and_predicates(predicate)
    )


def _apply_node_property_table_pattern_filter(
    property_table: pa.Table,
    pattern: NodePattern,
) -> pa.Table:
    """Filter per-label property projections before joining them back to node bindings."""

    masks: list[pa.Array] = []
    for key, value in pattern.properties.items():
        if key not in property_table.column_names:
            return property_table.slice(0, 0)
        masks.append(_comparison_mask(property_table[key], "=", value))
    return _filter_table_with_masks(property_table, masks)


def _apply_node_property_table_pushdown(
    property_table: pa.Table,
    *,
    variable: str,
    predicate: WherePredicate | None,
) -> pa.Table:
    """Apply single-node predicates directly on property projection tables."""

    if predicate is None:
        return property_table
    masks: list[pa.Array] = []
    for conjunct in _flatten_and_predicates(predicate):
        mask = _node_property_table_predicate_mask(
            property_table,
            variable=variable,
            predicate=conjunct,
        )
        if mask is None:
            continue
        masks.append(mask)
    return _filter_table_with_masks(property_table, masks)


def _node_property_table_predicate_mask(
    property_table: pa.Table,
    *,
    variable: str,
    predicate: WherePredicate,
) -> pa.Array | None:
    """Return a property-table mask for predicates that do not need full node bindings."""

    if isinstance(predicate, Comparison):
        if predicate.variable != variable or predicate.left_expression is not None:
            return None
        if isinstance(predicate.value, PropertyReferenceValue | ScalarExpression):
            return None
        if predicate.field not in property_table.column_names:
            return None
        return _comparison_mask(
            property_table[predicate.field],
            predicate.operator,
            predicate.value,
        )
    if isinstance(predicate, StringPredicate):
        if predicate.variable != variable:
            return None
        if predicate.field not in property_table.column_names:
            return None
        return _string_predicate_mask(
            property_table[predicate.field],
            predicate.operator,
            predicate.value,
        )
    if isinstance(predicate, ExistsPredicate):
        if predicate.variable != variable:
            return None
        if predicate.field not in property_table.column_names:
            return None
        return _exists_mask(property_table[predicate.field])
    if isinstance(predicate, InPredicate):
        if predicate.variable != variable:
            return None
        if predicate.field not in property_table.column_names:
            return None
        return _in_mask(property_table[predicate.field], predicate.values)
    if isinstance(predicate, NullPredicate):
        if predicate.variable != variable:
            return None
        if predicate.field not in property_table.column_names:
            return None
        return _null_mask(property_table[predicate.field], predicate.is_null)
    return None


def _can_project_requested_node_fields(
    *,
    pattern: NodePattern,
    requested_fields: set[str] | None,
    property_table: pa.Table,
) -> bool:
    """Check whether the optimized or semantically valid path applies here."""
    if "node_id" not in property_table.column_names:
        return False
    if not pa.types.is_int64(property_table["node_id"].type):
        return False

    required_fields = set(pattern.properties)
    if requested_fields is not None:
        required_fields.update(
            field
            for field in requested_fields
            if field not in {_NODE_ID_FIELD, _ENTITY_ID_FIELD, "__join_id", "labels"}
        )
    return required_fields.issubset(set(property_table.column_names))


def _can_use_edge_property_projection(
    step: RelationshipStep,
    requested_fields: set[str] | None,
) -> bool:
    """Check whether the optimized or semantically valid path applies here."""
    if requested_fields is None or step.rel_type is None:
        return False
    property_fields = {
        field for field in requested_fields if field not in {"id", "type", "source", "target"}
    }
    if step.properties:
        property_fields.update(step.properties)
    return bool(property_fields or requested_fields.intersection({"id", "source", "target"}))


def _requested_edge_property_fields(
    step: RelationshipStep,
    requested_fields: set[str],
) -> set[str]:
    """Return the logical relationship property columns needed by a property projection."""

    reserved_fields = {"id", "type", "source", "target"}
    property_fields = {field for field in requested_fields if field not in reserved_fields}
    property_fields.update(step.properties)
    return property_fields


def _can_project_requested_edge_fields(
    *,
    step: RelationshipStep,
    requested_fields: set[str] | None,
    property_table: pa.Table,
) -> bool:
    """Check whether the optimized or semantically valid path applies here."""
    required_columns = {"edge_id", "src_id", "dst_id"}
    if not required_columns.issubset(set(property_table.column_names)):
        return False

    required_fields = set(step.properties)
    if requested_fields is not None:
        required_fields.update(
            field for field in requested_fields if field not in {"id", "type", "source", "target"}
        )
    return required_fields.issubset(set(property_table.column_names))


def _edge_binding_table_from_property_projection(
    property_table: pa.Table,
    prefix: str,
    *,
    rel_type: str,
    requested_fields: set[str] | None,
) -> pa.Table:
    """Handle edge-specific matching, projection, or lookup logic for Cypher."""
    columns: dict[str, pa.Array | pa.ChunkedArray] = {
        f"{prefix}.id": property_table["edge_id"],
        f"{prefix}.type": pa.array([rel_type] * property_table.num_rows, type=pa.string()),
        f"{prefix}.source": property_table["src_id"],
        f"{prefix}.target": property_table["dst_id"],
    }

    if requested_fields is None:
        property_names = sorted(
            column_name
            for column_name in property_table.column_names
            if column_name not in {"edge_id", "src_id", "dst_id"}
        )
    else:
        property_names = sorted(
            field
            for field in requested_fields
            if field not in {"id", "type", "source", "target"}
            and field in property_table.column_names
        )
    for property_name in property_names:
        columns[f"{prefix}.{property_name}"] = property_table[property_name]
    return pa.table(columns)


def _canonical_labels_contains_all_mask(
    column: pa.ChunkedArray,
    expected_labels: tuple[str, ...],
) -> pa.Array:
    """Check whether the optimized or semantically valid path applies here."""
    if not expected_labels:
        return pa.array([True] * len(column), type=pa.bool_())

    canonical = column.combine_chunks()
    if canonical.null_count:
        # Flattening skips null lists, while parent indices can include their children.
        canonical = pc.fill_null(canonical, pa.scalar([], type=canonical.type))
    flat_labels = pc.list_flatten(canonical)
    parent_indices = pc.list_parent_indices(canonical)
    raw_labels = pc.unique(flat_labels).to_pylist()
    row_indices = pa.array(range(len(canonical)), type=pa.int64())
    masks = []
    for label in expected_labels:
        matching_labels = pa.array(
            [raw for raw in raw_labels if raw is not None and _cypher_name(raw) == label],
            type=flat_labels.type,
        )
        matched_parents = pc.filter(
            parent_indices, pc.is_in(flat_labels, value_set=matching_labels)
        )
        masks.append(pc.is_in(row_indices, value_set=matched_parents))
    return _combine_masks(masks, operator="and")


def _edge_binding_table_from_canonical_table(
    table: pa.Table,
    prefix: str,
    *,
    requested_fields: set[str] | None,
) -> pa.Table:
    """Handle edge-specific matching, projection, or lookup logic for Cypher."""
    reserved_fields = {"id", "type", "source", "target"}
    if requested_fields is None:
        properties = [
            _normalize_property_map(json.loads(value))
            for value in table["properties_json"].to_pylist()
        ]
        property_names = sorted({key for item in properties for key in item})
    else:
        requested_property_fields = sorted(
            field for field in requested_fields if field not in reserved_fields
        )
        if requested_property_fields:
            properties = [
                _normalize_property_map(json.loads(value))
                for value in table["properties_json"].to_pylist()
            ]
        else:
            properties = []
        available_properties = {key for item in properties for key in item}
        property_names = sorted(
            field for field in requested_property_fields if field in available_properties
        )

    columns: dict[str, pa.Array | pa.ChunkedArray] = {
        f"{prefix}.id": table["edge_id"],
        f"{prefix}.type": pa.array(
            [_cypher_name(value) for value in table["rel_type"].to_pylist()],
            type=pa.string(),
        ),
        f"{prefix}.source": table["src_id"],
        f"{prefix}.target": table["dst_id"],
    }
    for property_name in property_names:
        columns[f"{prefix}.{property_name}"] = pa.array(
            [item.get(property_name) for item in properties]
        )
    return pa.table(columns)


def _apply_node_pattern_filter(table: pa.Table, pattern: NodePattern) -> pa.Table:
    """Apply this transformation or predicate inside the Cypher pipeline."""
    if any(isinstance(value, PropertyReferenceValue) for value in pattern.properties.values()):
        return _apply_node_pattern_reference_filter(table, pattern)

    masks: list[pa.Array] = []
    if pattern.labels:
        label_column = _binding_column_name(pattern.variable, "labels")
        if label_column not in table.column_names:
            return table.slice(0, 0)
        masks.append(_labels_contains_all_mask(table[label_column], pattern.labels))

    for key, value in pattern.properties.items():
        column_name = _binding_column_name(pattern.variable, key)
        if column_name not in table.column_names:
            return table.slice(0, 0)
        masks.append(_comparison_mask(table[column_name], "=", value))
    return _filter_table_with_masks(table, masks)


def _apply_node_pattern_reference_filter(table: pa.Table, pattern: NodePattern) -> pa.Table:
    """Apply a node pattern filter whose inline properties reference row bindings."""
    if table.num_rows == 0:
        return table
    keep: list[bool] = []
    for row in table.to_pylist():
        if pattern.labels:
            labels = row.get(_binding_column_name(pattern.variable, "labels"))
            if not isinstance(labels, list) or not all(label in labels for label in pattern.labels):
                keep.append(False)
                continue
        matched = True
        for key, expected in pattern.properties.items():
            actual = row.get(_binding_column_name(pattern.variable, key))
            if isinstance(expected, PropertyReferenceValue):
                expected_value = _try_resolve_input_reference_value(expected, source_row=row)
                if expected_value is _UNRESOLVED_REFERENCE:
                    continue
            else:
                expected_value = expected
            if actual != expected_value:
                matched = False
                break
        keep.append(matched)
    return table.filter(pa.array(keep, type=pa.bool_()))


def _apply_edge_pattern_filter(table: pa.Table, prefix: str, step: RelationshipStep) -> pa.Table:
    """Apply this transformation or predicate inside the Cypher pipeline."""
    masks: list[pa.Array] = []
    if len(step.rel_type_alternatives) > 1:
        type_column = f"{prefix}.type"
        if type_column not in table.column_names:
            raise QueryError("Relationship type alternatives require relationship type bindings.")
        masks.append(pc.is_in(table[type_column], value_set=pa.array(step.rel_type_alternatives)))
    if step.rel_type is not None:
        masks.append(_comparison_mask(table[f"{prefix}.type"], "=", step.rel_type))

    for key, value in step.properties.items():
        column_name = f"{prefix}.{key}"
        if column_name not in table.column_names:
            return table.slice(0, 0)
        masks.append(_comparison_mask(table[column_name], "=", value))
    return _filter_table_with_masks(table, masks)


def _apply_existing_binding_filter(
    table: pa.Table,
    *,
    edge_target_column: str,
    pattern: NodePattern,
) -> pa.Table:
    """Apply this transformation or predicate inside the Cypher pipeline."""
    node_id_column = _binding_column_name(pattern.variable, _NODE_ID_FIELD)
    masks = [_comparison_mask(table[node_id_column], "=", table[edge_target_column])]

    label_column = _binding_column_name(pattern.variable, "labels")
    if pattern.labels:
        masks.append(_labels_contains_all_mask(table[label_column], pattern.labels))

    for key, value in pattern.properties.items():
        column_name = _binding_column_name(pattern.variable, key)
        if column_name not in table.column_names:
            return table.slice(0, 0)
        masks.append(_comparison_mask(table[column_name], "=", value))
    return _filter_table_with_masks(table, masks)


def _ensure_edge_source_column(
    table: pa.Table,
    *,
    edge_prefix: str,
    source_variable: str,
    endpoint: str = "source",
) -> pa.Table:
    """Restore relationship endpoint ids when Arrow drops the right join key."""
    endpoint_column = f"{edge_prefix}.{endpoint}"
    if endpoint_column in table.column_names:
        return table
    node_id_column = _binding_column_name(source_variable, _NODE_ID_FIELD)
    if node_id_column not in table.column_names:
        return table
    return table.append_column(endpoint_column, table[node_id_column])


def _apply_where_predicate_table(
    table: pa.Table,
    predicate: WherePredicate | None,
    *,
    nodes: NodeBindingSource | None = None,
    edges: EdgeBindingSource | None = None,
    tabular: PyArrowTabularAdapter | None = None,
    adjacency_by_source_loader: Callable[[str], pa.Table] | None = None,
    node_property_loader: PropertyTableLoader | None = None,
    edge_property_loader: PropertyTableLoader | None = None,
) -> pa.Table:
    """Apply this transformation or predicate inside the Cypher pipeline."""
    if predicate is None:
        return table
    # WHERE predicates compile into Arrow boolean masks, which keeps the read
    # path columnar even though the public result is projected back to Python maps.
    return table.filter(
        pc.fill_null(
            _predicate_mask(
                table,
                predicate,
                nodes=nodes,
                edges=edges,
                tabular=tabular,
                adjacency_by_source_loader=adjacency_by_source_loader,
                node_property_loader=node_property_loader,
                edge_property_loader=edge_property_loader,
            ),
            False,
        )
    )


def _predicate_mask(
    table: pa.Table,
    predicate: WherePredicate,
    *,
    nodes: NodeBindingSource | None = None,
    edges: EdgeBindingSource | None = None,
    tabular: PyArrowTabularAdapter | None = None,
    adjacency_by_source_loader: Callable[[str], pa.Table] | None = None,
    node_property_loader: PropertyTableLoader | None = None,
    edge_property_loader: PropertyTableLoader | None = None,
) -> pa.Array:
    """Support the current Cypher compilation or execution step."""
    if isinstance(predicate, Comparison):
        column = (
            _evaluate_scalar_expression_array(table, predicate.left_expression)
            if predicate.left_expression is not None
            else _resolve_table_field_array(
                table,
                variable=predicate.variable,
                field=predicate.field,
            )
        )
        if column is None:
            return pa.array([False] * table.num_rows, type=pa.bool_())
        if predicate.field == "labels":
            return _labels_comparison_mask(column, predicate.operator, predicate.value)
        right_value = _comparison_right_value(table, predicate.value)
        return _comparison_mask(column, predicate.operator, right_value)

    if isinstance(predicate, StringPredicate):
        column = _resolve_table_field_array(
            table,
            variable=predicate.variable,
            field=predicate.field,
        )
        if column is None:
            return pa.array([False] * table.num_rows, type=pa.bool_())
        if predicate.field == "labels":
            raise QueryError("String predicates are not supported for labels(...).")
        return _string_predicate_mask(column, predicate.operator, predicate.value)

    if isinstance(predicate, ExistsPredicate):
        column = _resolve_table_field_array(
            table,
            variable=predicate.variable,
            field=predicate.field,
        )
        if column is None:
            return pa.array([False] * table.num_rows, type=pa.bool_())
        return _exists_mask(column)

    if isinstance(predicate, ExistsPatternPredicate):
        return _exists_pattern_mask(
            table,
            predicate,
            nodes=nodes,
            edges=edges,
            tabular=tabular,
            adjacency_by_source_loader=adjacency_by_source_loader,
            node_property_loader=node_property_loader,
            edge_property_loader=edge_property_loader,
        )

    if isinstance(predicate, InPredicate):
        column = _resolve_table_field_array(
            table,
            variable=predicate.variable,
            field=predicate.field,
        )
        if column is None:
            return pa.array([False] * table.num_rows, type=pa.bool_())
        return _in_mask(column, predicate.values)

    if isinstance(predicate, NullPredicate):
        column = _resolve_table_field_array(
            table,
            variable=predicate.variable,
            field=predicate.field,
        )
        if column is None:
            return (
                pa.array([True] * table.num_rows, type=pa.bool_())
                if predicate.is_null
                else pa.array([False] * table.num_rows, type=pa.bool_())
            )
        return _null_mask(column, predicate.is_null)

    if isinstance(predicate, LabelPredicate):
        column = _resolve_table_field_array(
            table,
            variable=predicate.variable,
            field="labels",
        )
        if column is None:
            return pa.array([False] * table.num_rows, type=pa.bool_())
        return _label_membership_mask(column, predicate.label)

    if isinstance(predicate, NotPredicate):
        return pc.fill_null(
            pc.invert(
                _predicate_mask(
                    table,
                    predicate.predicate,
                    nodes=nodes,
                    edges=edges,
                    tabular=tabular,
                    adjacency_by_source_loader=adjacency_by_source_loader,
                    node_property_loader=node_property_loader,
                    edge_property_loader=edge_property_loader,
                )
            ),
            False,
        )

    if isinstance(predicate, AndPredicate):
        child_masks = [
            _predicate_mask(
                table,
                child,
                nodes=nodes,
                edges=edges,
                tabular=tabular,
                adjacency_by_source_loader=adjacency_by_source_loader,
                node_property_loader=node_property_loader,
                edge_property_loader=edge_property_loader,
            )
            for child in predicate.predicates
        ]
        return _combine_masks(child_masks, operator="and")

    if isinstance(predicate, OrPredicate):
        child_masks = [
            _predicate_mask(
                table,
                child,
                nodes=nodes,
                edges=edges,
                tabular=tabular,
                adjacency_by_source_loader=adjacency_by_source_loader,
                node_property_loader=node_property_loader,
                edge_property_loader=edge_property_loader,
            )
            for child in predicate.predicates
        ]
        return _combine_masks(child_masks, operator="or")

    raise QueryError(f"Unsupported WHERE predicate type: {type(predicate)!r}")


def _exists_pattern_mask(
    table: pa.Table,
    predicate: ExistsPatternPredicate,
    *,
    nodes: NodeBindingSource | None,
    edges: EdgeBindingSource | None,
    tabular: PyArrowTabularAdapter | None,
    adjacency_by_source_loader: Callable[[str], pa.Table] | None,
    node_property_loader: PropertyTableLoader | None,
    edge_property_loader: PropertyTableLoader | None,
) -> pa.Array:
    """Evaluate EXISTS { pattern } as a correlated semi-join mask."""

    if nodes is None or edges is None or tabular is None:
        raise QueryError("EXISTS { ... } requires graph state during WHERE evaluation.")
    if table.num_rows == 0:
        return pa.array([], type=pa.bool_())

    required_fields = _required_fields_for_match(
        predicate.patterns,
        predicate.where_predicate,
        return_items=(),
        order_by=None,
    )
    matched = _match_patterns_table(
        nodes,
        edges,
        predicate.patterns,
        tabular,
        required_fields=required_fields,
        adjacency_by_source_loader=adjacency_by_source_loader,
        node_property_loader=node_property_loader,
        edge_property_loader=edge_property_loader,
    )
    matched = _apply_where_predicate_table(
        matched,
        predicate.where_predicate,
        nodes=nodes,
        edges=edges,
        tabular=tabular,
        adjacency_by_source_loader=adjacency_by_source_loader,
        node_property_loader=node_property_loader,
        edge_property_loader=edge_property_loader,
    )

    outer_variables = _entity_variables_from_table(table)
    pattern_variables = {
        variable for pattern in predicate.patterns for variable in _pattern_variables(pattern)
    }
    shared_variables = tuple(sorted(outer_variables & pattern_variables))
    if not shared_variables:
        return pa.array([matched.num_rows > 0] * table.num_rows, type=pa.bool_())
    if matched.num_rows == 0:
        return pa.array([False] * table.num_rows, type=pa.bool_())

    left_key = _binding_membership_key(table, shared_variables)
    right_key = _binding_membership_key(matched, shared_variables)
    return pc.fill_null(pc.is_in(left_key, value_set=right_key), False)


def _binding_membership_key(
    table: pa.Table,
    variables: tuple[str, ...],
) -> pa.ChunkedArray | pa.Array:
    """Build the Arrow key used to correlate EXISTS subquery bindings."""

    if len(variables) == 1:
        return table[_binding_column_name(variables[0], "__join_id")]
    columns = [
        pc.cast(table[_binding_column_name(variable, "__join_id")], pa.string())
        for variable in variables
    ]
    return pc.binary_join_element_wise(*columns, "|")


def _combine_masks(masks: list[pa.Array], *, operator: str) -> pa.Array:
    """Support the current Cypher compilation or execution step."""
    if not masks:
        return pa.array([], type=pa.bool_())

    combined = masks[0]
    for mask in masks[1:]:
        if operator == "and":
            combined = pc.and_(combined, mask)
        elif operator == "or":
            combined = pc.or_(combined, mask)
        else:
            raise QueryError(f"Unsupported mask operator: {operator}")
    return pc.fill_null(combined, False)


def _filter_table_with_masks(table: pa.Table, masks: list[pa.Array]) -> pa.Table:
    """Support the current Cypher compilation or execution step."""
    if not masks:
        return table

    combined = masks[0]
    for mask in masks[1:]:
        combined = pc.and_(combined, mask)
    return table.filter(pc.fill_null(combined, False))


def _comparison_right_value(
    table: pa.Table,
    value: object,
) -> object:
    """Resolve property/scalar references used on the right side of comparisons."""

    if isinstance(value, LiteralExpression):
        return value.value
    if isinstance(value, FieldExpression | BinaryExpression | FunctionExpression):
        return _evaluate_scalar_expression_array(table, value)
    if not isinstance(value, PropertyReferenceValue):
        return value
    resolved = _resolve_table_field_array(
        table,
        variable=value.variable,
        field=value.field,
    )
    if resolved is None:
        column_name = _binding_column_name(value.variable, value.field)
        raise QueryError(f"WHERE references an unknown binding: {column_name}")
    return resolved


def _comparison_mask(column: pa.ChunkedArray, operator: str, value: object) -> pa.Array:
    """Support the current Cypher compilation or execution step."""
    right = value if isinstance(value, pa.ChunkedArray) else pa.scalar(value)
    if operator == "=":
        result = pc.equal(column, right)
    elif operator == "!=":
        result = pc.not_equal(column, right)
    elif operator == ">":
        result = pc.greater(column, right)
    elif operator == ">=":
        result = pc.greater_equal(column, right)
    elif operator == "<":
        result = pc.less(column, right)
    elif operator == "<=":
        result = pc.less_equal(column, right)
    else:
        raise QueryError(f"Unsupported comparison operator: {operator}")
    return pc.fill_null(result, False)


def _string_predicate_mask(column: pa.ChunkedArray, operator: str, value: object) -> pa.Array:
    """Support the current Cypher compilation or execution step."""
    if not isinstance(value, str):
        raise QueryError("String predicate values must be strings.")
    if operator == "contains":
        result = pc.match_substring(column, value)
    elif operator == "starts_with":
        result = pc.starts_with(column, value)
    elif operator == "ends_with":
        result = pc.ends_with(column, value)
    else:
        raise QueryError(f"Unsupported string predicate operator: {operator}")
    return pc.fill_null(result, False)


def _labels_comparison_mask(column: pa.ChunkedArray, operator: str, value: object) -> pa.Array:
    """Support the current Cypher compilation or execution step."""
    if not isinstance(value, list | tuple):
        raise QueryError("labels(...) comparisons require a list-like value.")

    expected = json.dumps(list(value))
    if operator == "=":
        return _comparison_mask(column, "=", expected)
    if operator == "!=":
        return _comparison_mask(column, "!=", expected)
    raise QueryError(f"Unsupported comparison operator for labels(...): {operator}")


def _labels_contains_all_mask(
    column: pa.ChunkedArray,
    expected_labels: tuple[str, ...],
) -> pa.Array:
    """Support the current Cypher compilation or execution step."""
    if not expected_labels:
        return pa.array([True] * len(column), type=pa.bool_())
    masks = [_label_membership_mask(column, label) for label in expected_labels]
    return _combine_masks(masks, operator="and")


def _label_membership_mask(
    column: pa.ChunkedArray,
    value: str,
) -> pa.Array:
    """Support the current Cypher compilation or execution step."""
    normalized_value = _cypher_name(value)
    encoded_candidates = {
        json.dumps(normalized_value),
        json.dumps(f"`{normalized_value}`"),
        json.dumps(value),
    }
    masks = [
        pc.fill_null(pc.match_substring(column, encoded_candidate), False)
        for encoded_candidate in encoded_candidates
    ]
    return _combine_masks(masks, operator="or")


def _exists_mask(column: pa.ChunkedArray) -> pa.Array:
    """Support the current Cypher compilation or execution step."""
    return pc.fill_null(pc.is_valid(column), False)


def _in_mask(column: pa.ChunkedArray, values: tuple[object, ...]) -> pa.Array:
    """Support the current Cypher compilation or execution step."""
    if not values:
        return pa.array([False] * len(column), type=pa.bool_())
    result = pc.is_in(column, value_set=pa.array(values))
    return pc.fill_null(result, False)


def _null_mask(column: pa.ChunkedArray, is_null: bool) -> pa.Array:
    """Support the current Cypher compilation or execution step."""
    result = pc.is_null(column)
    if not is_null:
        result = pc.invert(result)
    return pc.fill_null(result, False)


__all__ = ["CypherQueryUseCase"]
