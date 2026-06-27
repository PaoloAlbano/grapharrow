"""Use cases for working with the graph state."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]

from grapharrow.core.errors import NotFoundError, QueryError, SchemaError
from grapharrow.domain import (
    CatalogVersion,
    DatasetFile,
    LabelDefinition,
    LabelId,
    RelationshipTypeDefinition,
    RelationshipTypeId,
    SnapshotId,
    SnapshotManifest,
    VersionedCatalog,
)
from grapharrow.repositories import (
    CatalogRepository,
    EdgeRecord,
    GraphRepository,
    NodeRecord,
    SnapshotRepository,
)
from grapharrow.use_cases.admin import DatabaseAdminUseCase


@dataclass(frozen=True, slots=True)
class GraphState:
    """Fully materialized graph state for the current snapshot."""

    catalog: VersionedCatalog
    snapshot: SnapshotManifest
    node_table: pa.Table
    edge_table: pa.Table
    nodes: tuple[NodeRecord, ...]
    edges: tuple[EdgeRecord, ...]


@dataclass(frozen=True, slots=True)
class GraphTabularState:
    """Arrow-first graph state for read/query execution paths."""

    catalog: VersionedCatalog
    snapshot: SnapshotManifest
    node_table: pa.Table
    edge_table: pa.Table


@dataclass(frozen=True, slots=True)
class QueryResult:
    """Tabular result returned by Cypher execution."""

    columns: tuple[str, ...]
    rows: tuple[dict[str, Any], ...]


class GraphStateUseCase:
    """High-level read and write operations on the graph state."""

    def __init__(
        self,
        *,
        admin_service: DatabaseAdminUseCase,
        catalog_store: CatalogRepository,
        snapshot_store: SnapshotRepository,
        graph_store: GraphRepository,
    ) -> None:
        """Wire the repositories and admin service used by the graph state workflows."""
        self._admin_service = admin_service
        self._catalog_store = catalog_store
        self._snapshot_store = snapshot_store
        self._graph_store = graph_store

    def load_state(self) -> GraphState:
        """Load the current catalog, snapshot, and graph records."""

        self._admin_service.initialize()
        catalog = self._catalog_store.load()
        snapshot = self._snapshot_store.load_manifest()
        node_table = self._graph_store.load_node_table(snapshot.nodes)
        edge_table = self._graph_store.load_edge_table(snapshot.edges)
        nodes = tuple(self._graph_store.node_records_from_table(node_table))
        edges = tuple(self._graph_store.edge_records_from_table(edge_table))
        return GraphState(
            catalog=catalog,
            snapshot=snapshot,
            node_table=node_table,
            edge_table=edge_table,
            nodes=nodes,
            edges=edges,
        )

    def load_tabular_state(self) -> GraphTabularState:
        """Load the current catalog, snapshot, and canonical Arrow tables."""

        self._admin_service.initialize()
        catalog = self._catalog_store.load()
        snapshot = self._snapshot_store.load_manifest()
        node_table = self._graph_store.load_node_table(snapshot.nodes)
        edge_table = self._graph_store.load_edge_table(snapshot.edges)
        return GraphTabularState(
            catalog=catalog,
            snapshot=snapshot,
            node_table=node_table,
            edge_table=edge_table,
        )

    def load_node_property_table(
        self,
        *,
        snapshot: SnapshotManifest,
        label: str,
    ) -> pa.Table:
        """Load the property-columnar dataset for ``label``."""

        return self._graph_store.load_node_property_table(snapshot.node_properties, label=label)

    def load_edge_property_table(
        self,
        *,
        snapshot: SnapshotManifest,
        rel_type: str,
    ) -> pa.Table:
        """Load the property-columnar dataset for ``rel_type``."""

        return self._graph_store.load_edge_property_table(
            snapshot.edge_properties,
            rel_type=rel_type,
        )

    def load_adjacency_by_source_table(
        self,
        *,
        snapshot: SnapshotManifest,
        rel_type: str,
    ) -> pa.Table:
        """Load the adjacency-by-source table for a relationship type."""

        files = tuple(
            dataset_file
            for dataset_file in snapshot.adjacency_by_source
            if _adjacency_rel_type_from_file(dataset_file) == rel_type
        )
        return self._graph_store.load_adjacency_by_source_table(files)

    def create_node(
        self,
        *,
        labels: tuple[str, ...],
        properties: dict[str, object],
    ) -> tuple[GraphState, NodeRecord]:
        """Append a node and publish a new snapshot."""

        state = self.load_state()
        next_node_id = _next_node_id(state.nodes)
        node = NodeRecord(node_id=next_node_id, labels=labels, properties=properties)
        self._validate_new_nodes(
            catalog=state.catalog,
            existing_nodes=state.nodes,
            new_nodes=(node,),
        )
        next_catalog = self._ensure_catalog(state.catalog, labels=labels, rel_types=())
        next_state = self._publish_graph(
            previous_state=state,
            catalog=next_catalog,
            nodes=[*state.nodes, node],
            edges=list(state.edges),
        )
        return next_state, node

    def create_nodes_bulk(
        self,
        *,
        table: pa.Table,
        default_labels: tuple[str, ...] = (),
        labels_column: str = "labels",
        properties_column: str = "properties",
    ) -> tuple[GraphState, tuple[NodeRecord, ...]]:
        """Append multiple nodes and publish a single snapshot."""

        state = self.load_state()
        if table.num_rows == 0:
            return state, ()

        next_node_id = _next_node_id(state.nodes)
        new_nodes: list[NodeRecord] = []
        for row in table.to_pylist():
            labels = _bulk_labels_for_row(
                row,
                labels_column=labels_column,
                default_labels=default_labels,
            )
            properties = _bulk_properties_for_row(
                row,
                properties_column=properties_column,
                reserved_columns={labels_column},
            )
            new_nodes.append(
                NodeRecord(
                    node_id=next_node_id,
                    labels=labels,
                    properties=properties,
                )
            )
            next_node_id += 1

        new_nodes_tuple = tuple(new_nodes)
        self._validate_new_nodes(
            catalog=state.catalog,
            existing_nodes=state.nodes,
            new_nodes=new_nodes_tuple,
        )
        next_catalog = self._ensure_catalog(
            state.catalog,
            labels=tuple(label for node in new_nodes for label in node.labels),
            rel_types=(),
        )
        next_state = self._publish_graph(
            previous_state=state,
            catalog=next_catalog,
            nodes=[*state.nodes, *new_nodes],
            edges=list(state.edges),
        )
        return next_state, new_nodes_tuple

    def create_relationship_with_inline_nodes(
        self,
        *,
        left_labels: tuple[str, ...],
        left_properties: dict[str, object],
        rel_type: str,
        rel_properties: dict[str, object],
        right_labels: tuple[str, ...],
        right_properties: dict[str, object],
    ) -> tuple[GraphState, NodeRecord, EdgeRecord, NodeRecord]:
        """Append two inline nodes plus a relationship and publish a new snapshot."""

        state = self.load_state()
        next_left_node_id = _next_node_id(state.nodes)
        next_right_node_id = next_left_node_id + 1
        next_edge_id = _next_edge_id(state.edges)

        left_node = NodeRecord(
            node_id=next_left_node_id,
            labels=left_labels,
            properties=left_properties,
        )
        right_node = NodeRecord(
            node_id=next_right_node_id,
            labels=right_labels,
            properties=right_properties,
        )
        edge = EdgeRecord(
            edge_id=next_edge_id,
            rel_type=rel_type,
            src_id=left_node.node_id,
            dst_id=right_node.node_id,
            properties=rel_properties,
        )
        self._validate_new_nodes(
            catalog=state.catalog,
            existing_nodes=state.nodes,
            new_nodes=(left_node, right_node),
        )
        self._validate_edge(
            catalog=state.catalog,
            nodes=(*state.nodes, left_node, right_node),
            nodes_by_id=_nodes_by_id((*state.nodes, left_node, right_node)),
            edge=edge,
        )

        next_catalog = self._ensure_catalog(
            state.catalog,
            labels=(*left_labels, *right_labels),
            rel_types=(rel_type,),
        )
        next_state = self._publish_graph(
            previous_state=state,
            catalog=next_catalog,
            nodes=[*state.nodes, left_node, right_node],
            edges=[*state.edges, edge],
        )
        return next_state, left_node, edge, right_node

    def create_edge(
        self,
        *,
        src_id: int,
        dst_id: int,
        rel_type: str,
        properties: dict[str, object],
    ) -> tuple[GraphState, EdgeRecord]:
        """Append an edge between existing nodes and publish a new snapshot."""

        state = self.load_state()
        nodes_by_id = {node.node_id: node for node in state.nodes}
        if src_id not in nodes_by_id:
            raise NotFoundError(f"Source node does not exist: {src_id}")
        if dst_id not in nodes_by_id:
            raise NotFoundError(f"Target node does not exist: {dst_id}")

        edge = EdgeRecord(
            edge_id=_next_edge_id(state.edges),
            rel_type=rel_type,
            src_id=src_id,
            dst_id=dst_id,
            properties=properties,
        )
        self._validate_edge(
            catalog=state.catalog,
            nodes=state.nodes,
            nodes_by_id=nodes_by_id,
            edge=edge,
        )
        next_catalog = self._ensure_catalog(state.catalog, labels=(), rel_types=(rel_type,))
        next_state = self._publish_graph(
            previous_state=state,
            catalog=next_catalog,
            nodes=list(state.nodes),
            edges=[*state.edges, edge],
        )
        return next_state, edge

    def create_edge_by_reference(
        self,
        *,
        source_label: str,
        source_key: str,
        source_value: object,
        target_label: str,
        target_key: str,
        target_value: object,
        rel_type: str,
        properties: dict[str, object],
    ) -> tuple[GraphState, EdgeRecord]:
        """Append an edge between nodes resolved from explicit business-key references."""

        state = self.load_state()
        src_id = self._resolve_node_id(
            state.nodes,
            label=source_label,
            key=source_key,
            value=source_value,
            role="source",
        )
        dst_id = self._resolve_node_id(
            state.nodes,
            label=target_label,
            key=target_key,
            value=target_value,
            role="target",
        )

        edge = EdgeRecord(
            edge_id=_next_edge_id(state.edges),
            rel_type=rel_type,
            src_id=src_id,
            dst_id=dst_id,
            properties=properties,
        )
        self._validate_edge(
            catalog=state.catalog,
            nodes=state.nodes,
            nodes_by_id=_nodes_by_id(state.nodes),
            edge=edge,
        )
        next_catalog = self._ensure_catalog(state.catalog, labels=(), rel_types=(rel_type,))
        next_state = self._publish_graph(
            previous_state=state,
            catalog=next_catalog,
            nodes=list(state.nodes),
            edges=[*state.edges, edge],
        )
        return next_state, edge

    def create_edges_batch(
        self,
        *,
        edges_to_create: tuple[tuple[int, int, str, dict[str, object]], ...],
    ) -> tuple[GraphState, tuple[EdgeRecord, ...]]:
        """Append multiple edges in one published snapshot."""

        state = self.load_state()
        if not edges_to_create:
            return state, ()

        new_edges = self._materialize_new_edges(
            state=state,
            edges_to_create=edges_to_create,
        )

        next_catalog = self._ensure_catalog(
            state.catalog,
            labels=(),
            rel_types=tuple(edge.rel_type for edge in new_edges),
        )
        next_state = self._publish_graph(
            previous_state=state,
            catalog=next_catalog,
            nodes=list(state.nodes),
            edges=[*state.edges, *new_edges],
        )
        return next_state, tuple(new_edges)

    def create_edges_bulk(
        self,
        *,
        table: pa.Table,
        rel_type_column: str = "rel_type",
        properties_column: str = "properties",
        src_id_column: str = "src_id",
        dst_id_column: str = "dst_id",
        source_key: tuple[str, str] | None = None,
        target_key: tuple[str, str] | None = None,
        source_value_column: str = "src_key",
        target_value_column: str = "dst_key",
    ) -> tuple[GraphState, tuple[EdgeRecord, ...]]:
        """Append multiple edges and publish a single snapshot."""

        state = self.load_state()
        if table.num_rows == 0:
            return state, ()

        if (source_key is None) != (target_key is None):
            raise QueryError("source_key and target_key must be provided together.")

        edges_to_create: list[tuple[int, int, str, dict[str, object]]] = []
        for row in table.to_pylist():
            rel_type = _required_string_value(row, rel_type_column)
            if source_key is None or target_key is None:
                src_id = _required_int_value(row, src_id_column)
                dst_id = _required_int_value(row, dst_id_column)
                reserved_columns = {src_id_column, dst_id_column, rel_type_column}
            else:
                src_id = self._resolve_node_id(
                    state.nodes,
                    label=source_key[0],
                    key=source_key[1],
                    value=row.get(source_value_column),
                    role="source",
                )
                dst_id = self._resolve_node_id(
                    state.nodes,
                    label=target_key[0],
                    key=target_key[1],
                    value=row.get(target_value_column),
                    role="target",
                )
                reserved_columns = {
                    source_value_column,
                    target_value_column,
                    rel_type_column,
                }

            properties = _bulk_properties_for_row(
                row,
                properties_column=properties_column,
                reserved_columns=reserved_columns,
            )
            edges_to_create.append((src_id, dst_id, rel_type, properties))

        new_edges = self._materialize_new_edges(
            state=state,
            edges_to_create=tuple(edges_to_create),
        )
        next_catalog = self._ensure_catalog(
            state.catalog,
            labels=(),
            rel_types=tuple(edge.rel_type for edge in new_edges),
        )
        next_state = self._publish_graph(
            previous_state=state,
            catalog=next_catalog,
            nodes=list(state.nodes),
            edges=[*state.edges, *new_edges],
        )
        return next_state, tuple(new_edges)

    def update_properties_batch(
        self,
        *,
        node_updates: tuple[tuple[int, str, object], ...],
        edge_updates: tuple[tuple[int, str, object], ...],
    ) -> GraphState:
        """Apply node and edge property updates and publish a new snapshot."""

        state = self.load_state()
        if not node_updates and not edge_updates:
            return state

        nodes = list(state.nodes)
        edges = list(state.edges)
        node_indexes = {node.node_id: index for index, node in enumerate(nodes)}
        edge_indexes = {edge.edge_id: index for index, edge in enumerate(edges)}

        for node_id, property_name, value in node_updates:
            index = node_indexes.get(node_id)
            if index is None:
                raise NotFoundError(f"Node does not exist: {node_id}")
            properties = dict(nodes[index].properties)
            properties[property_name] = value
            nodes[index] = NodeRecord(
                node_id=nodes[index].node_id,
                labels=nodes[index].labels,
                properties=properties,
            )

        for edge_id, property_name, value in edge_updates:
            index = edge_indexes.get(edge_id)
            if index is None:
                raise NotFoundError(f"Relationship does not exist: {edge_id}")
            properties = dict(edges[index].properties)
            properties[property_name] = value
            edges[index] = EdgeRecord(
                edge_id=edges[index].edge_id,
                rel_type=edges[index].rel_type,
                src_id=edges[index].src_id,
                dst_id=edges[index].dst_id,
                properties=properties,
            )

        self._validate_full_graph(
            catalog=state.catalog,
            nodes=tuple(nodes),
            edges=tuple(edges),
        )
        return self._publish_graph(
            previous_state=state,
            catalog=state.catalog,
            nodes=nodes,
            edges=edges,
        )

    def remove_properties_batch(
        self,
        *,
        node_removals: tuple[tuple[int, str], ...],
        edge_removals: tuple[tuple[int, str], ...],
    ) -> GraphState:
        """Remove node and edge properties and publish a new snapshot."""

        state = self.load_state()
        if not node_removals and not edge_removals:
            return state

        nodes = list(state.nodes)
        edges = list(state.edges)
        node_indexes = {node.node_id: index for index, node in enumerate(nodes)}
        edge_indexes = {edge.edge_id: index for index, edge in enumerate(edges)}

        for node_id, property_name in node_removals:
            index = node_indexes.get(node_id)
            if index is None:
                raise NotFoundError(f"Node does not exist: {node_id}")
            properties = dict(nodes[index].properties)
            properties.pop(property_name, None)
            nodes[index] = NodeRecord(
                node_id=nodes[index].node_id,
                labels=nodes[index].labels,
                properties=properties,
            )

        for edge_id, property_name in edge_removals:
            index = edge_indexes.get(edge_id)
            if index is None:
                raise NotFoundError(f"Relationship does not exist: {edge_id}")
            properties = dict(edges[index].properties)
            properties.pop(property_name, None)
            edges[index] = EdgeRecord(
                edge_id=edges[index].edge_id,
                rel_type=edges[index].rel_type,
                src_id=edges[index].src_id,
                dst_id=edges[index].dst_id,
                properties=properties,
            )

        self._validate_full_graph(
            catalog=state.catalog,
            nodes=tuple(nodes),
            edges=tuple(edges),
        )
        return self._publish_graph(
            previous_state=state,
            catalog=state.catalog,
            nodes=nodes,
            edges=edges,
        )

    def remove_labels_batch(
        self,
        *,
        node_label_removals: tuple[tuple[int, str], ...],
    ) -> GraphState:
        """Remove labels from nodes and publish a new snapshot."""

        state = self.load_state()
        if not node_label_removals:
            return state

        nodes = list(state.nodes)
        node_indexes = {node.node_id: index for index, node in enumerate(nodes)}

        for node_id, label in node_label_removals:
            index = node_indexes.get(node_id)
            if index is None:
                raise NotFoundError(f"Node does not exist: {node_id}")
            labels = tuple(existing for existing in nodes[index].labels if existing != label)
            nodes[index] = NodeRecord(
                node_id=nodes[index].node_id,
                labels=labels,
                properties=nodes[index].properties,
            )

        self._validate_full_graph(
            catalog=state.catalog,
            nodes=tuple(nodes),
            edges=state.edges,
        )
        return self._publish_graph(
            previous_state=state,
            catalog=state.catalog,
            nodes=nodes,
            edges=list(state.edges),
        )

    def delete_entities_batch(
        self,
        *,
        node_ids: tuple[int, ...],
        edge_ids: tuple[int, ...],
    ) -> GraphState:
        """Delete bound nodes and relationships and publish a new snapshot."""

        state = self.load_state()
        if not node_ids and not edge_ids:
            return state

        node_id_set = set(node_ids)
        edge_id_set = set(edge_ids)

        existing_node_ids = {node.node_id for node in state.nodes}
        existing_edge_ids = {edge.edge_id for edge in state.edges}
        missing_nodes = sorted(
            node_id for node_id in node_id_set if node_id not in existing_node_ids
        )
        missing_edges = sorted(
            edge_id for edge_id in edge_id_set if edge_id not in existing_edge_ids
        )
        if missing_nodes:
            raise NotFoundError(f"Node does not exist: {missing_nodes[0]}")
        if missing_edges:
            raise NotFoundError(f"Relationship does not exist: {missing_edges[0]}")

        remaining_edges = [edge for edge in state.edges if edge.edge_id not in edge_id_set]
        for node_id in node_id_set:
            if any(edge.src_id == node_id or edge.dst_id == node_id for edge in remaining_edges):
                raise QueryError(
                    "DELETE on nodes with existing relationships is not supported yet. "
                    "Delete relationships first."
                )

        remaining_nodes = [node for node in state.nodes if node.node_id not in node_id_set]
        self._validate_full_graph(
            catalog=state.catalog,
            nodes=tuple(remaining_nodes),
            edges=tuple(remaining_edges),
        )
        return self._publish_graph(
            previous_state=state,
            catalog=state.catalog,
            nodes=remaining_nodes,
            edges=remaining_edges,
        )

    def publish_records(
        self,
        *,
        previous_state: GraphState,
        new_nodes: tuple[NodeRecord, ...],
        new_edges: tuple[EdgeRecord, ...],
    ) -> GraphState:
        """Publish a batch of already-materialized records in a single snapshot."""

        if not new_nodes and not new_edges:
            return previous_state

        self._validate_new_nodes(
            catalog=previous_state.catalog,
            existing_nodes=previous_state.nodes,
            new_nodes=new_nodes,
        )

        candidate_nodes = (*previous_state.nodes, *new_nodes)
        candidate_nodes_by_id = _nodes_by_id(candidate_nodes)
        for edge in new_edges:
            self._validate_edge(
                catalog=previous_state.catalog,
                nodes=candidate_nodes,
                nodes_by_id=candidate_nodes_by_id,
                edge=edge,
            )

        next_catalog = self._ensure_catalog(
            previous_state.catalog,
            labels=tuple(label for node in new_nodes for label in node.labels),
            rel_types=tuple(edge.rel_type for edge in new_edges),
        )
        return self._publish_graph(
            previous_state=previous_state,
            catalog=next_catalog,
            nodes=[*previous_state.nodes, *new_nodes],
            edges=[*previous_state.edges, *new_edges],
        )

    def _publish_graph(
        self,
        *,
        previous_state: GraphState,
        catalog: VersionedCatalog,
        nodes: list[NodeRecord],
        edges: list[EdgeRecord],
    ) -> GraphState:
        """Persist a staged snapshot, publish it atomically, and return the new state."""
        if catalog.version != previous_state.catalog.version:
            self._catalog_store.write(catalog)

        next_snapshot_id = SnapshotId(previous_state.snapshot.snapshot_id.value + 1)
        persisted = self._graph_store.persist_snapshot(
            snapshot_id=next_snapshot_id,
            nodes=nodes,
            edges=edges,
        )
        manifest = SnapshotManifest(
            snapshot_id=next_snapshot_id,
            parent_snapshot_id=previous_state.snapshot.snapshot_id,
            commit_txid=next_snapshot_id.value,
            created_at=_utc_now(),
            catalog_version=catalog.version,
            nodes=persisted.nodes,
            edges=persisted.edges,
            node_labels=persisted.node_labels,
            node_properties=persisted.node_properties,
            adjacency_by_source=persisted.adjacency_by_source,
            adjacency_by_dest=(),
            edge_properties=persisted.edge_properties,
        )
        try:
            self._snapshot_store.publish(manifest, staged_relative_path=persisted.staged_dir)
            node_table = self._graph_store.load_node_table(persisted.nodes)
            edge_table = self._graph_store.load_edge_table(persisted.edges)
            return GraphState(
                catalog=catalog,
                snapshot=manifest,
                node_table=node_table,
                edge_table=edge_table,
                nodes=tuple(nodes),
                edges=tuple(edges),
            )
        finally:
            self._graph_store.cleanup_staged(persisted.staged_dir)

    def _ensure_catalog(
        self,
        catalog: VersionedCatalog,
        *,
        labels: tuple[str, ...],
        rel_types: tuple[str, ...],
    ) -> VersionedCatalog:
        """Extend the catalog only when new labels or relationship types appear in a write."""
        existing_labels = {definition.name for definition in catalog.labels}
        existing_rel_types = {definition.name for definition in catalog.relationship_types}

        new_labels = tuple(label for label in dict.fromkeys(labels) if label not in existing_labels)
        new_rel_types = tuple(
            rel_type for rel_type in dict.fromkeys(rel_types) if rel_type not in existing_rel_types
        )

        if not new_labels and not new_rel_types:
            return catalog

        next_version = CatalogVersion(catalog.version.value + 1)
        next_label_id = max((definition.id.value for definition in catalog.labels), default=0) + 1
        next_rel_type_id = (
            max((definition.id.value for definition in catalog.relationship_types), default=0) + 1
        )

        label_definitions = list(catalog.labels)
        for label in new_labels:
            label_definitions.append(LabelDefinition(id=LabelId(next_label_id), name=label))
            next_label_id += 1

        relationship_type_definitions = list(catalog.relationship_types)
        for rel_type in new_rel_types:
            relationship_type_definitions.append(
                RelationshipTypeDefinition(
                    id=RelationshipTypeId(next_rel_type_id),
                    name=rel_type,
                )
            )
            next_rel_type_id += 1

        return VersionedCatalog(
            version=next_version,
            labels=tuple(label_definitions),
            relationship_types=tuple(relationship_type_definitions),
            properties=catalog.properties,
            constraints=catalog.constraints,
        )

    def _validate_new_nodes(
        self,
        *,
        catalog: VersionedCatalog,
        existing_nodes: tuple[NodeRecord, ...],
        new_nodes: tuple[NodeRecord, ...],
    ) -> None:
        """Validate incoming nodes incrementally so uniqueness checks see earlier inserts too."""
        candidate_nodes = list(existing_nodes)
        for node in new_nodes:
            self._validate_node(catalog=catalog, nodes=tuple(candidate_nodes), node=node)
            candidate_nodes.append(node)

    def _validate_node(
        self,
        *,
        catalog: VersionedCatalog,
        nodes: tuple[NodeRecord, ...],
        node: NodeRecord,
    ) -> None:
        """Enforce label constraints and property definitions for a single candidate node."""
        for label in node.labels:
            constraints = catalog.constraints_for_owner(owner_kind="label", owner_name=label)
            constrained_properties = {
                constraint.property_name
                for constraint in constraints
                if constraint.property_name is not None
            }

            for constraint in constraints:
                property_name = constraint.property_name
                if constraint.kind == "required":
                    if property_name is None or node.properties.get(property_name) is None:
                        raise SchemaError(
                            f"Property '{property_name}' is required for label '{label}'."
                        )
                elif constraint.kind == "nullability":
                    if property_name is None or node.properties.get(property_name) is None:
                        raise SchemaError(
                            f"Property '{property_name}' must be non-null for label '{label}'."
                        )
                elif constraint.kind in {"unique", "primary_key"}:
                    if property_name is None:
                        raise SchemaError(
                            f"Constraint '{constraint.kind}' on label '{label}' "
                            "requires a property."
                        )
                    value = node.properties.get(property_name)
                    if value is None:
                        raise SchemaError(
                            f"Property '{property_name}' must be set for label '{label}'."
                        )
                    for existing in nodes:
                        if label not in existing.labels:
                            continue
                        if existing.properties.get(property_name) == value:
                            raise SchemaError(
                                f"Duplicate value for '{label}.{property_name}': {value!r}."
                            )

            for definition in catalog.properties_for_owner(owner_kind="label", owner_name=label):
                if definition.name in constrained_properties:
                    continue
                if not definition.nullable and node.properties.get(definition.name) is None:
                    raise SchemaError(
                        f"Property '{definition.name}' must be non-null for label '{label}'."
                    )

    def _validate_edge(
        self,
        *,
        catalog: VersionedCatalog,
        nodes: tuple[NodeRecord, ...],
        nodes_by_id: Mapping[int, NodeRecord] | None = None,
        edge: EdgeRecord,
    ) -> None:
        """Validate endpoints, endpoint-label rules, and relationship property constraints."""
        if nodes_by_id is None:
            nodes_by_id = _nodes_by_id(nodes)
        source = nodes_by_id.get(edge.src_id)
        target = nodes_by_id.get(edge.dst_id)
        if source is None:
            raise NotFoundError(f"Source node does not exist: {edge.src_id}")
        if target is None:
            raise NotFoundError(f"Target node does not exist: {edge.dst_id}")

        constraints = catalog.constraints_for_owner(
            owner_kind="relationship_type",
            owner_name=edge.rel_type,
        )
        constrained_properties = {
            constraint.property_name
            for constraint in constraints
            if constraint.property_name is not None
        }

        for constraint in constraints:
            property_name = constraint.property_name
            if constraint.kind == "required":
                if property_name is None or edge.properties.get(property_name) is None:
                    raise SchemaError(
                        f"Property '{property_name}' is required for "
                        f"relationship '{edge.rel_type}'."
                    )
            elif constraint.kind == "nullability":
                if property_name is None or edge.properties.get(property_name) is None:
                    raise SchemaError(
                        f"Property '{property_name}' must be non-null for "
                        f"relationship '{edge.rel_type}'."
                    )
            elif constraint.kind == "endpoint":
                if constraint.source_labels and not any(
                    label in source.labels for label in constraint.source_labels
                ):
                    raise SchemaError(
                        f"Relationship '{edge.rel_type}' requires source labels "
                        f"{list(constraint.source_labels)}."
                    )
                if constraint.target_labels and not any(
                    label in target.labels for label in constraint.target_labels
                ):
                    raise SchemaError(
                        f"Relationship '{edge.rel_type}' requires target labels "
                        f"{list(constraint.target_labels)}."
                    )

        for definition in catalog.properties_for_owner(
            owner_kind="relationship_type",
            owner_name=edge.rel_type,
        ):
            if definition.name in constrained_properties:
                continue
            if not definition.nullable and edge.properties.get(definition.name) is None:
                raise SchemaError(
                    f"Property '{definition.name}' must be non-null for "
                    f"relationship '{edge.rel_type}'."
                )

    def _resolve_node_id(
        self,
        nodes: tuple[NodeRecord, ...],
        *,
        label: str,
        key: str,
        value: object,
        role: str,
    ) -> int:
        """Resolve this reference into the concrete value used later in the flow."""
        matches = [
            node.node_id
            for node in nodes
            if label in node.labels and node.properties.get(key) == value
        ]
        if not matches:
            raise NotFoundError(f"No {role} node matches reference {label}.{key}={value!r}.")
        if len(matches) > 1:
            raise QueryError(f"Reference {label}.{key}={value!r} matches multiple {role} nodes.")
        return matches[0]

    def _validate_full_graph(
        self,
        *,
        catalog: VersionedCatalog,
        nodes: tuple[NodeRecord, ...],
        edges: tuple[EdgeRecord, ...],
    ) -> None:
        """Re-run graph-wide validation after updates or deletes that may affect many records."""
        for node in nodes:
            other_nodes = tuple(
                candidate for candidate in nodes if candidate.node_id != node.node_id
            )
            self._validate_node(catalog=catalog, nodes=other_nodes, node=node)
        for edge in edges:
            self._validate_edge(catalog=catalog, nodes=nodes, edge=edge)

    def _materialize_new_edges(
        self,
        *,
        state: GraphState,
        edges_to_create: tuple[tuple[int, int, str, dict[str, object]], ...],
    ) -> list[EdgeRecord]:
        """Allocate edge ids and validate a batch before it is appended to the snapshot."""
        new_edges: list[EdgeRecord] = []
        next_edge_id = _next_edge_id(state.edges)
        state_nodes_by_id = _nodes_by_id(state.nodes)
        for src_id, dst_id, rel_type, properties in edges_to_create:
            edge = EdgeRecord(
                edge_id=next_edge_id,
                rel_type=rel_type,
                src_id=src_id,
                dst_id=dst_id,
                properties=dict(properties),
            )
            self._validate_edge(
                catalog=state.catalog,
                nodes=state.nodes,
                nodes_by_id=state_nodes_by_id,
                edge=edge,
            )
            new_edges.append(edge)
            next_edge_id += 1
        return new_edges


class GraphTransactionUseCase:
    """In-memory transaction wrapper that publishes only on commit."""

    def __init__(self, graph_service: GraphStateUseCase) -> None:
        """Start an isolated in-memory transaction from the current published snapshot."""
        self._graph_service = graph_service
        self._base_state = graph_service.load_state()
        self._state = self._base_state
        self._closed = False

    def load_state(self) -> GraphState:
        """Return the current transaction-local graph state."""

        return self._state

    def load_tabular_state(self) -> GraphTabularState:
        """Return the current transaction-local state in Arrow-first form."""

        return GraphTabularState(
            catalog=self._state.catalog,
            snapshot=self._state.snapshot,
            node_table=self._state.node_table,
            edge_table=self._state.edge_table,
        )

    def publish_records(
        self,
        *,
        previous_state: GraphState,
        new_nodes: tuple[NodeRecord, ...],
        new_edges: tuple[EdgeRecord, ...],
    ) -> GraphState:
        """Apply a materialized write to the transaction-local state."""

        self._ensure_open()
        if previous_state.snapshot.snapshot_id != self._state.snapshot.snapshot_id:
            raise QueryError("Transaction state is out of date for publish_records.")
        if not new_nodes and not new_edges:
            return self._state

        self._graph_service._validate_new_nodes(
            catalog=self._state.catalog,
            existing_nodes=self._state.nodes,
            new_nodes=new_nodes,
        )

        candidate_nodes = (*self._state.nodes, *new_nodes)
        candidate_nodes_by_id = _nodes_by_id(candidate_nodes)
        for edge in new_edges:
            self._graph_service._validate_edge(
                catalog=self._state.catalog,
                nodes=candidate_nodes,
                nodes_by_id=candidate_nodes_by_id,
                edge=edge,
            )

        next_catalog = self._graph_service._ensure_catalog(
            self._state.catalog,
            labels=tuple(label for node in new_nodes for label in node.labels),
            rel_types=tuple(edge.rel_type for edge in new_edges),
        )
        return self._replace_state(
            catalog=next_catalog,
            nodes=[*self._state.nodes, *new_nodes],
            edges=[*self._state.edges, *new_edges],
        )

    def create_edges_batch(
        self,
        *,
        edges_to_create: tuple[tuple[int, int, str, dict[str, object]], ...],
    ) -> tuple[GraphState, tuple[EdgeRecord, ...]]:
        """Create multiple relationships inside the transaction-local state."""

        self._ensure_open()
        if not edges_to_create:
            return self._state, ()

        new_edges: list[EdgeRecord] = []
        next_edge_id = _next_edge_id(self._state.edges)
        state_nodes_by_id = _nodes_by_id(self._state.nodes)
        for src_id, dst_id, rel_type, properties in edges_to_create:
            edge = EdgeRecord(
                edge_id=next_edge_id,
                rel_type=rel_type,
                src_id=src_id,
                dst_id=dst_id,
                properties=dict(properties),
            )
            self._graph_service._validate_edge(
                catalog=self._state.catalog,
                nodes=self._state.nodes,
                nodes_by_id=state_nodes_by_id,
                edge=edge,
            )
            new_edges.append(edge)
            next_edge_id += 1

        next_catalog = self._graph_service._ensure_catalog(
            self._state.catalog,
            labels=(),
            rel_types=tuple(edge.rel_type for edge in new_edges),
        )
        next_state = self._replace_state(
            catalog=next_catalog,
            nodes=list(self._state.nodes),
            edges=[*self._state.edges, *new_edges],
        )
        return next_state, tuple(new_edges)

    def update_properties_batch(
        self,
        *,
        node_updates: tuple[tuple[int, str, object], ...],
        edge_updates: tuple[tuple[int, str, object], ...],
    ) -> GraphState:
        """Apply property updates inside the transaction-local state."""

        self._ensure_open()
        if not node_updates and not edge_updates:
            return self._state

        nodes = list(self._state.nodes)
        edges = list(self._state.edges)
        node_indexes = {node.node_id: index for index, node in enumerate(nodes)}
        edge_indexes = {edge.edge_id: index for index, edge in enumerate(edges)}

        for node_id, property_name, value in node_updates:
            index = node_indexes.get(node_id)
            if index is None:
                raise NotFoundError(f"Node does not exist: {node_id}")
            properties = dict(nodes[index].properties)
            properties[property_name] = value
            nodes[index] = NodeRecord(
                node_id=nodes[index].node_id,
                labels=nodes[index].labels,
                properties=properties,
            )

        for edge_id, property_name, value in edge_updates:
            index = edge_indexes.get(edge_id)
            if index is None:
                raise NotFoundError(f"Relationship does not exist: {edge_id}")
            properties = dict(edges[index].properties)
            properties[property_name] = value
            edges[index] = EdgeRecord(
                edge_id=edges[index].edge_id,
                rel_type=edges[index].rel_type,
                src_id=edges[index].src_id,
                dst_id=edges[index].dst_id,
                properties=properties,
            )

        self._graph_service._validate_full_graph(
            catalog=self._state.catalog,
            nodes=tuple(nodes),
            edges=tuple(edges),
        )
        return self._replace_state(
            catalog=self._state.catalog,
            nodes=nodes,
            edges=edges,
        )

    def remove_properties_batch(
        self,
        *,
        node_removals: tuple[tuple[int, str], ...],
        edge_removals: tuple[tuple[int, str], ...],
    ) -> GraphState:
        """Remove properties inside the transaction-local state."""

        self._ensure_open()
        if not node_removals and not edge_removals:
            return self._state

        nodes = list(self._state.nodes)
        edges = list(self._state.edges)
        node_indexes = {node.node_id: index for index, node in enumerate(nodes)}
        edge_indexes = {edge.edge_id: index for index, edge in enumerate(edges)}

        for node_id, property_name in node_removals:
            index = node_indexes.get(node_id)
            if index is None:
                raise NotFoundError(f"Node does not exist: {node_id}")
            properties = dict(nodes[index].properties)
            properties.pop(property_name, None)
            nodes[index] = NodeRecord(
                node_id=nodes[index].node_id,
                labels=nodes[index].labels,
                properties=properties,
            )

        for edge_id, property_name in edge_removals:
            index = edge_indexes.get(edge_id)
            if index is None:
                raise NotFoundError(f"Relationship does not exist: {edge_id}")
            properties = dict(edges[index].properties)
            properties.pop(property_name, None)
            edges[index] = EdgeRecord(
                edge_id=edges[index].edge_id,
                rel_type=edges[index].rel_type,
                src_id=edges[index].src_id,
                dst_id=edges[index].dst_id,
                properties=properties,
            )

        self._graph_service._validate_full_graph(
            catalog=self._state.catalog,
            nodes=tuple(nodes),
            edges=tuple(edges),
        )
        return self._replace_state(
            catalog=self._state.catalog,
            nodes=nodes,
            edges=edges,
        )

    def remove_labels_batch(
        self,
        *,
        node_label_removals: tuple[tuple[int, str], ...],
    ) -> GraphState:
        """Remove labels from nodes inside the transaction-local state."""

        self._ensure_open()
        if not node_label_removals:
            return self._state

        nodes = list(self._state.nodes)
        node_indexes = {node.node_id: index for index, node in enumerate(nodes)}

        for node_id, label in node_label_removals:
            index = node_indexes.get(node_id)
            if index is None:
                raise NotFoundError(f"Node does not exist: {node_id}")
            labels = tuple(existing for existing in nodes[index].labels if existing != label)
            nodes[index] = NodeRecord(
                node_id=nodes[index].node_id,
                labels=labels,
                properties=nodes[index].properties,
            )

        self._graph_service._validate_full_graph(
            catalog=self._state.catalog,
            nodes=tuple(nodes),
            edges=self._state.edges,
        )
        return self._replace_state(
            catalog=self._state.catalog,
            nodes=nodes,
            edges=list(self._state.edges),
        )

    def delete_entities_batch(
        self,
        *,
        node_ids: tuple[int, ...],
        edge_ids: tuple[int, ...],
    ) -> GraphState:
        """Delete nodes and relationships inside the transaction-local state."""

        self._ensure_open()
        if not node_ids and not edge_ids:
            return self._state

        node_id_set = set(node_ids)
        edge_id_set = set(edge_ids)

        existing_node_ids = {node.node_id for node in self._state.nodes}
        existing_edge_ids = {edge.edge_id for edge in self._state.edges}
        missing_nodes = sorted(
            node_id for node_id in node_id_set if node_id not in existing_node_ids
        )
        missing_edges = sorted(
            edge_id for edge_id in edge_id_set if edge_id not in existing_edge_ids
        )
        if missing_nodes:
            raise NotFoundError(f"Node does not exist: {missing_nodes[0]}")
        if missing_edges:
            raise NotFoundError(f"Relationship does not exist: {missing_edges[0]}")

        remaining_edges = [edge for edge in self._state.edges if edge.edge_id not in edge_id_set]
        for node_id in node_id_set:
            if any(edge.src_id == node_id or edge.dst_id == node_id for edge in remaining_edges):
                raise QueryError(
                    "DELETE on nodes with existing relationships is not supported yet. "
                    "Delete relationships first."
                )

        remaining_nodes = [node for node in self._state.nodes if node.node_id not in node_id_set]
        self._graph_service._validate_full_graph(
            catalog=self._state.catalog,
            nodes=tuple(remaining_nodes),
            edges=tuple(remaining_edges),
        )
        return self._replace_state(
            catalog=self._state.catalog,
            nodes=remaining_nodes,
            edges=remaining_edges,
        )

    def commit(self) -> GraphState:
        """Persist the transaction-local state as a new published snapshot."""

        self._ensure_open()
        self._closed = True
        if self._state == self._base_state:
            return self._base_state
        return self._graph_service._publish_graph(
            previous_state=self._base_state,
            catalog=self._state.catalog,
            nodes=list(self._state.nodes),
            edges=list(self._state.edges),
        )

    def rollback(self) -> GraphState:
        """Discard all local changes and close the transaction."""

        self._ensure_open()
        self._closed = True
        self._state = self._base_state
        return self._base_state

    def _replace_state(
        self,
        *,
        catalog: VersionedCatalog,
        nodes: list[NodeRecord],
        edges: list[EdgeRecord],
    ) -> GraphState:
        """Rebuild the transaction-local Arrow tables after an in-memory mutation."""
        node_table = pa.table(
            {
                "node_id": pa.array([node.node_id for node in nodes], type=pa.int64()),
                "labels": pa.array(
                    [list(node.labels) for node in nodes],
                    type=pa.list_(pa.string()),
                ),
                "properties_json": pa.array(
                    [json.dumps(node.properties, sort_keys=True) for node in nodes],
                    type=pa.string(),
                ),
            },
            schema=self._base_state.node_table.schema,
        )
        edge_table = pa.table(
            {
                "edge_id": pa.array([edge.edge_id for edge in edges], type=pa.int64()),
                "rel_type": pa.array([edge.rel_type for edge in edges], type=pa.string()),
                "src_id": pa.array([edge.src_id for edge in edges], type=pa.int64()),
                "dst_id": pa.array([edge.dst_id for edge in edges], type=pa.int64()),
                "properties_json": pa.array(
                    [json.dumps(edge.properties, sort_keys=True) for edge in edges],
                    type=pa.string(),
                ),
            },
            schema=self._base_state.edge_table.schema,
        )
        self._state = GraphState(
            catalog=catalog,
            snapshot=self._base_state.snapshot,
            node_table=node_table,
            edge_table=edge_table,
            nodes=tuple(nodes),
            edges=tuple(edges),
        )
        return self._state

    def _ensure_open(self) -> None:
        """Reject operations after commit or rollback has already closed the transaction."""
        if self._closed:
            raise QueryError("Transaction is already closed.")


def _next_node_id(nodes: tuple[NodeRecord, ...]) -> int:
    """Return the next monotonic node id for a newly created record."""
    return max((node.node_id for node in nodes), default=0) + 1


def _next_edge_id(edges: tuple[EdgeRecord, ...]) -> int:
    """Return the next monotonic relationship id for a newly created edge."""
    return max((edge.edge_id for edge in edges), default=0) + 1


def _nodes_by_id(
    nodes: tuple[NodeRecord, ...] | list[NodeRecord],
) -> dict[int, NodeRecord]:
    """Build the node-id index reused by edge validation and reference resolution."""
    return {node.node_id: node for node in nodes}


def _adjacency_rel_type_from_file(dataset_file: DatasetFile) -> str | None:
    """Extract the relationship type encoded in an adjacency derived-dataset path."""
    parts = PurePosixPath(dataset_file.path).parts
    if "adjacency" not in parts or "by_source" not in parts or len(parts) < 2:
        return None
    return parts[-2]


def _bulk_labels_for_row(
    row: dict[str, Any],
    *,
    labels_column: str,
    default_labels: tuple[str, ...],
) -> tuple[str, ...]:
    """Normalize bulk-ingest labels from a row into the tuple form used by NodeRecord."""
    raw_labels = row.get(labels_column)
    if raw_labels is None:
        labels = default_labels
    elif isinstance(raw_labels, str):
        labels = tuple(label.strip() for label in raw_labels.split(";") if label.strip())
    elif isinstance(raw_labels, list):
        labels = tuple(str(label) for label in raw_labels)
    else:
        raise SchemaError(f"Bulk node column '{labels_column}' must contain a string or list[str].")

    if not labels:
        raise SchemaError("Bulk node rows must provide labels or default_labels.")
    return labels


def _bulk_properties_for_row(
    row: dict[str, Any],
    *,
    properties_column: str,
    reserved_columns: set[str],
) -> dict[str, object]:
    """Merge explicit and tabular bulk-ingest properties while skipping reserved columns."""
    properties: dict[str, object] = {}
    raw_properties = row.get(properties_column)
    if raw_properties is not None:
        if not isinstance(raw_properties, dict):
            raise SchemaError(
                f"Bulk ingest column '{properties_column}' must contain dictionaries."
            )
        properties.update(raw_properties)

    reserved = set(reserved_columns)
    reserved.add(properties_column)
    for key, value in row.items():
        if key in reserved:
            continue
        properties[key] = value
    return properties


def _required_string_value(row: dict[str, Any], column: str) -> str:
    """Read a required non-empty string column from a bulk-ingest row."""
    value = row.get(column)
    if not isinstance(value, str) or not value:
        raise SchemaError(f"Bulk ingest column '{column}' must contain non-empty strings.")
    return value


def _required_int_value(row: dict[str, Any], column: str) -> int:
    """Read a required integer column from a bulk-ingest row without accepting bools."""
    value = row.get(column)
    if isinstance(value, bool) or not isinstance(value, int):
        raise SchemaError(f"Bulk ingest column '{column}' must contain integers.")
    return value


def _utc_now() -> str:
    """Return the timestamp format used in snapshot manifests."""
    return datetime.now(tz=UTC).isoformat(timespec="seconds")


__all__ = ["GraphState", "GraphStateUseCase", "GraphTransactionUseCase", "QueryResult"]
