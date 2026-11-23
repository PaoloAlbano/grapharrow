"""Selective execution for constant property SET by entity ID."""

from __future__ import annotations

from contextlib import AbstractContextManager
from typing import TYPE_CHECKING

import pyarrow as pa  # type: ignore[import-untyped]

from grapharrow.domain.records import EdgeRecord, NodeRecord
from grapharrow.domain.snapshot import IdempotencyReceipt
from grapharrow.storage.models import PointWriteReceipt
from grapharrow.use_cases import cypher as core
from grapharrow.use_cases.graph import GraphStateUseCase, QueryResult

if TYPE_CHECKING:
    from grapharrow.use_cases.cypher import (
        CompiledMatchSetQuery,
        CypherQueryUseCase,
        _IdempotentWrite,
    )


def try_point_set(
    executor: CypherQueryUseCase,
    query: CompiledMatchSetQuery,
    *,
    idempotency: _IdempotentWrite | None = None,
) -> QueryResult | None:
    """Decline shapes whose read dependencies extend beyond a single entity."""
    graph = executor._graph_service
    if not isinstance(graph, GraphStateUseCase) or len(query.match_patterns) != 1 or query.order_by:
        return None
    pattern = query.match_patterns[0]
    node = pattern if isinstance(pattern, core.NodePattern) and not pattern.properties else None
    edge = _single_edge(pattern)
    if node is None and edge is None:
        return None
    variable = node.variable if node is not None else edge.variable if edge is not None else None
    assert variable is not None
    predicate = query.where_predicate
    if not (
        isinstance(predicate, core.Comparison)
        and predicate.variable == variable
        and predicate.field == core._ENTITY_ID_FIELD
        and predicate.operator == "="
        and predicate.left_expression is None
        and type(predicate.value) is int
        and 0 < predicate.value < 2**63
    ):
        return None
    properties: dict[str, object] = {}
    for item in query.set_items:
        if not isinstance(item, core.SetPropertyItem) or item.variable != variable:
            return None
        if type(item.value) not in (str, bool, int, float):
            return None
        properties[item.field] = item.value
    if not properties:
        return None
    for projection in query.return_items:
        if (
            projection.variable != variable
            or projection.field is None
            or projection.field in {"type", "source", "target"}
            or (node is not None and projection.field == "labels")
            or projection.field.startswith("__")
            or projection.scalar_expression is not None
            or projection.aggregate is not None
            or projection.metadata_only
        ):
            return None
    result: QueryResult
    receipt = None
    if idempotency is not None:

        def prepare_receipt() -> IdempotencyReceipt:
            return idempotency.prepare_receipt(result)

        receipt = PointWriteReceipt(idempotency.base, prepare_receipt)
    edit: AbstractContextManager[tuple[EdgeRecord | NodeRecord | None] | None]
    if node is not None:
        edit = graph.edit_node_properties_by_id(
            node_id=predicate.value, labels=node.labels, properties=properties, receipt=receipt
        )
    else:
        assert edge is not None
        edit = graph.edit_edge_properties_by_id(
            edge_id=predicate.value, rel_type=edge.rel_type, properties=properties, receipt=receipt
        )
    with edit as candidate:
        if candidate is None:
            return None
        entity = candidate[0]
        if not query.return_items:
            result = QueryResult((), ())
        else:
            columns = {
                f"{variable}.{item.field}": core._property_binding_array(
                    [entity.properties.get(item.field or "")] if entity is not None else []
                )
                for item in query.return_items
            }
            result = executor._finalize_table_query_result(
                query.return_items,
                pa.table(columns),
                distinct=query.distinct,
                order_by=query.order_by,
                skip=query.skip,
                limit=query.limit,
            )
    return result


def _single_edge(pattern: core.CompiledPattern) -> core.RelationshipStep | None:
    if not isinstance(pattern, core.PathPattern) or len(pattern.steps) != 1:
        return None
    step = pattern.steps[0]
    if (
        step.variable is None
        or pattern.capture_relationships
        or pattern.start.labels
        or pattern.start.properties
        or step.node.labels
        or step.node.properties
        or pattern.start.variable == step.node.variable
        or step.direction not in {"outgoing", "incoming"}
        or step.variable_length
        or step.min_hops != 1
        or step.max_hops != 1
        or step.properties
        or len(step.rel_type_alternatives) > 1
    ):
        return None
    return step
