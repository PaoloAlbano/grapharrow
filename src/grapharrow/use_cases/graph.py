"""Use cases for working with the graph state."""

from __future__ import annotations

import time
from collections.abc import Generator, Mapping
from contextlib import AbstractContextManager, ExitStack, contextmanager, nullcontext
from dataclasses import dataclass, replace
from functools import partial
from threading import BoundedSemaphore
from typing import Any, cast

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.compute as pc  # type: ignore[import-untyped]

from grapharrow.adapters.adjacency import AdjacencyIndexStore, GraphAdjacency
from grapharrow.adapters.property_index import PropertyIndexStore
from grapharrow.adapters.snapshot_tables import SnapshotTableCache
from grapharrow.adapters.writer import writer_locked
from grapharrow.core import property_codec
from grapharrow.core.errors import NotFoundError, QueryError, SchemaError
from grapharrow.domain import (
    CatalogVersion,
    LabelDefinition,
    LabelId,
    RelationshipTypeDefinition,
    RelationshipTypeId,
    SnapshotManifest,
    VersionedCatalog,
)
from grapharrow.domain.scan import ScanPredicate
from grapharrow.domain.snapshot import IdempotencyReceipt
from grapharrow.repositories import (
    CatalogRepository,
    EdgeRecord,
    GraphRepository,
    NodeRecord,
    SnapshotRepository,
)
from grapharrow.repositories.graph import EDGE_SCHEMA, _edges_table, _nodes_table
from grapharrow.storage.contracts import CatalogStore, GraphStore, SnapshotStore, StorageBackend


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
        catalog_store: CatalogStore,
        snapshot_store: SnapshotStore,
        graph_store: GraphStore,
        storage_backend: StorageBackend | None = None,
        cypher_max_concurrent_queries: int = 0,
        snapshot_table_cache_max_bytes: int = 64 * 1024 * 1024,
        snapshot_tables: SnapshotTableCache | None = None,
        property_index_max_bytes: int = 32 * 1024 * 1024,
        adjacency_index_max_bytes: int = 32 * 1024 * 1024,
    ) -> None:
        """Wire the repositories and admin service used by the graph state workflows."""
        self._catalog_store = catalog_store
        self._snapshot_store = snapshot_store
        self._graph_store = graph_store
        if storage_backend is None:
            from grapharrow.storage.native import NativeStorageBackend

            storage_backend = NativeStorageBackend(
                catalog=cast(CatalogRepository, catalog_store),
                snapshot=cast(SnapshotRepository, snapshot_store),
                graph=cast(GraphRepository, graph_store),
            )
        elif not (
            storage_backend.catalog is catalog_store
            and storage_backend.snapshot is snapshot_store
            and storage_backend.graph is graph_store
        ):
            raise ValueError(
                "The Storage Backend must use the same catalog, snapshot and graph stores."
            )
        self._storage_backend = storage_backend
        self._writer = storage_backend.writer
        self._cypher_slots = (
            BoundedSemaphore(cypher_max_concurrent_queries)
            if cypher_max_concurrent_queries
            else None
        )
        self.snapshot_tables = (
            snapshot_tables
            if snapshot_tables is not None
            else SnapshotTableCache(snapshot_table_cache_max_bytes)
        )
        self.property_indexes = PropertyIndexStore(property_index_max_bytes)
        self.adjacency_indexes = AdjacencyIndexStore(adjacency_index_max_bytes)

    def adjacency_for_state(self, state: GraphState | GraphTabularState) -> GraphAdjacency | None:
        """Reuse immutable topology positions across frontends on this application."""
        return self.adjacency_indexes.get_or_create(
            state.snapshot.edges, state.edge_table, node_count=state.node_table.num_rows
        )

    def writer_lock(self) -> AbstractContextManager[None]:
        """Serialize a complete database mutation with other coordinated writers."""
        return self._writer.hold()

    @contextmanager
    def cypher_query_slot(self) -> Generator[None, None, None]:
        """Admit a Cypher statement before it starts allocating query state."""
        slots = self._cypher_slots
        if slots is None:
            yield
            return
        if not slots.acquire(blocking=False):
            raise QueryError(
                "Too many Cypher queries are running. Retry after a query completes.",
                category="ResourceExhausted",
                phase="runtime",
                detail="ConcurrentQueryLimit",
            )
        try:
            yield
        finally:
            slots.release()

    def capture_snapshot(self) -> SnapshotManifest:
        """Capture a published manifest without loading canonical graph tables."""
        return self._storage_backend.capture_snapshot()

    def pin_snapshot(self, snapshot: SnapshotManifest) -> AbstractContextManager[None]:
        return self._storage_backend.pin_snapshot(snapshot.snapshot_id)

    def idempotency_base(self, key_hash: str) -> tuple[SnapshotManifest, IdempotencyReceipt | None]:
        """Find a receipt and the write base in the same published history."""
        pointer = self._snapshot_store.try_load_current_pointer()
        if pointer is None:
            self.capture_snapshot()
            pointer = self._snapshot_store.load_current_pointer()
        return self._snapshot_store.idempotency_base(pointer, key_hash)

    def edit_edge_properties_by_id(
        self, *, edge_id: int, rel_type: str | None, properties: dict[str, object]
    ) -> AbstractContextManager[tuple[EdgeRecord | None] | None]:
        """Let the selected backend prepare and atomically publish a selective edit."""
        return self._storage_backend.edit_edge_properties_by_id(
            edge_id=edge_id, rel_type=rel_type, properties=properties
        )

    def edit_node_properties_by_id(
        self, *, node_id: int, labels: tuple[str, ...], properties: dict[str, object]
    ) -> AbstractContextManager[tuple[NodeRecord | None] | None]:
        return self._storage_backend.edit_node_properties_by_id(
            node_id=node_id, labels=labels, properties=properties
        )

    def load_state(self, *, snapshot: SnapshotManifest | None = None) -> GraphState:
        """Load the current catalog, snapshot, and graph records."""

        if snapshot is None:
            snapshot = self.capture_snapshot()
        with self.pin_snapshot(snapshot):
            catalog = self._catalog_store.load_for_snapshot(snapshot)
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

    def load_tabular_state(
        self, *, include_edges: bool = True, snapshot: SnapshotManifest | None = None
    ) -> GraphTabularState:
        """Load canonical tables and their catalog from a pinned or current snapshot."""

        if snapshot is None:
            snapshot = self.capture_snapshot()
        with self.pin_snapshot(snapshot):
            catalog = self._catalog_store.load_for_snapshot(snapshot)
            node_table = self._graph_store.load_node_table(snapshot.nodes)
            edge_table = (
                self._graph_store.load_edge_table(snapshot.edges)
                if include_edges
                else EDGE_SCHEMA.empty_table()
            )
            return GraphTabularState(
                catalog=catalog,
                snapshot=snapshot,
                node_table=node_table,
                edge_table=edge_table,
            )

    @contextmanager
    def borrow_tabular_state(
        self, *, include_edges: bool = True, snapshot: SnapshotManifest | None = None
    ) -> Generator[GraphTabularState, None, None]:
        """Share canonical buffers until the snapshot reader releases them."""
        with ExitStack() as pins:
            if snapshot is None:
                snapshot = pins.enter_context(self._storage_backend.borrow_current_snapshot())
            else:
                pins.enter_context(self.pin_snapshot(snapshot))
            catalog = self._catalog_store.load_for_snapshot(snapshot)
            nodes, edges = snapshot.nodes, snapshot.edges
            node_key = (
                self._graph_store.node_cache_files(nodes)
                if self.snapshot_tables.max_bytes
                else nodes
            )
            edge_table = (
                self.snapshot_tables.borrow(
                    ("edges", edges), lambda: self._graph_store.load_edge_table(edges)
                )
                if include_edges
                else nullcontext(EDGE_SCHEMA.empty_table())
            )
            with (
                self.snapshot_tables.borrow(
                    ("nodes", node_key), lambda: self._graph_store.load_node_table(nodes)
                ) as node_table,
                edge_table as borrowed_edges,
            ):
                yield GraphTabularState(catalog, snapshot, node_table, borrowed_edges)

    def load_node_property_table(
        self,
        *,
        snapshot: SnapshotManifest,
        label: str,
        columns: list[str] | None = None,
        predicates: tuple[ScanPredicate, ...] = (),
    ) -> pa.Table:
        """Load the property-columnar dataset for ``label``."""

        return self._graph_store.load_node_property_table(
            snapshot.node_properties,
            label=label,
            columns=columns,
            predicates=predicates,
        )

    @contextmanager
    def borrow_node_property_tables(
        self, *, snapshot: SnapshotManifest
    ) -> Generator[dict[str, pa.Table], None, None]:
        """Share full label projections directly from one published manifest."""
        grouped = self._graph_store.group_node_property_files(snapshot.node_properties)
        with ExitStack() as leases:
            yield {
                label: leases.enter_context(
                    self.snapshot_tables.borrow(
                        (
                            "node_properties",
                            self._graph_store.node_cache_files(files)
                            if self.snapshot_tables.max_bytes
                            else files,
                        ),
                        partial(
                            self._graph_store.load_node_property_projection, files, label=label
                        ),
                    )
                )
                for label, files in grouped.items()
            }

    def lookup_node_property_table(
        self,
        *,
        snapshot: SnapshotManifest,
        catalog: VersionedCatalog,
        label: str,
        equalities: dict[str, object],
        table: pa.Table | None = None,
    ) -> pa.Table | None:
        """Use a declared single-property equality index, or let the caller scan."""
        for index in catalog.indexes:
            if (
                index.owner_kind != "label"
                or index.owner_name != label
                or index.kind not in {"range", "lookup"}
                or not index.case_sensitive
                or len(index.property_names) != 1
            ):
                continue
            prop = index.property_names[0]
            if prop not in equalities:
                continue
            files = self._graph_store.node_property_files(snapshot.node_properties, label=label)
            selected = self.property_indexes.lookup(
                files,
                prop,
                equalities[prop],
                lambda: (
                    table
                    if table is not None
                    else self.load_node_property_table(snapshot=snapshot, label=label)
                ),
            )
            if selected is not None:
                return selected
        return None

    def iter_node_property_batches(
        self,
        *,
        snapshot: SnapshotManifest,
        label: str,
        columns: list[str],
        predicates: tuple[ScanPredicate, ...] = (),
        batch_size: int = 65536,
    ) -> Generator[pa.RecordBatch, None, None]:
        """Stream narrow property batches from a pinned snapshot."""
        return self._graph_store.iter_node_property_batches(
            snapshot.node_properties,
            label=label,
            columns=columns,
            predicates=predicates,
            batch_size=batch_size,
        )

    def load_node_property_schema(self, *, snapshot: SnapshotManifest, label: str) -> pa.Schema:
        """Inspect physical property encodings before enabling a columnar read."""
        return self._graph_store.load_node_property_schema(snapshot.node_properties, label=label)

    def load_edge_property_table(
        self,
        *,
        snapshot: SnapshotManifest,
        rel_type: str,
        columns: list[str] | None = None,
        predicates: tuple[ScanPredicate, ...] = (),
    ) -> pa.Table:
        """Load the property-columnar dataset for ``rel_type``."""

        return self._graph_store.load_edge_property_table(
            snapshot.edge_properties,
            rel_type=rel_type,
            columns=columns,
            predicates=predicates,
        )

    def load_adjacency_by_source_table(
        self,
        *,
        snapshot: SnapshotManifest,
        rel_type: str,
        predicates: tuple[ScanPredicate, ...] = (),
    ) -> pa.Table:
        """Load the adjacency-by-source table for a relationship type."""

        files = self._graph_store.adjacency_files(snapshot.adjacency_by_source, rel_type=rel_type)
        return self._graph_store.load_adjacency_by_source_table(files, predicates=predicates)

    def load_adjacency_by_dest_table(
        self,
        *,
        snapshot: SnapshotManifest,
        rel_type: str,
        predicates: tuple[ScanPredicate, ...] = (),
    ) -> pa.Table:
        """Load the adjacency-by-destination table for a relationship type."""

        files = self._graph_store.adjacency_files(snapshot.adjacency_by_dest, rel_type=rel_type)
        return self._graph_store.load_adjacency_by_dest_table(files, predicates=predicates)

    def has_complete_adjacency(
        self, snapshot: SnapshotManifest, rel_type: str, *, incoming: bool
    ) -> bool:
        """Use a derived read only when the manifest accounts for every edge."""
        return self._graph_store.has_complete_adjacency(snapshot, rel_type, incoming=incoming)

    @writer_locked
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

    @writer_locked
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

    @writer_locked
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

    @writer_locked
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

        return self._append_edge(
            state,
            nodes_by_id=nodes_by_id,
            src_id=src_id,
            dst_id=dst_id,
            rel_type=rel_type,
            properties=properties,
        )

    @writer_locked
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

        return self._append_edge(
            state,
            nodes_by_id=_nodes_by_id(state.nodes),
            src_id=src_id,
            dst_id=dst_id,
            rel_type=rel_type,
            properties=properties,
        )

    def _append_edge(
        self,
        state: GraphState,
        *,
        nodes_by_id: Mapping[int, NodeRecord],
        src_id: int,
        dst_id: int,
        rel_type: str,
        properties: dict[str, object],
    ) -> tuple[GraphState, EdgeRecord]:
        """Append one row to the canonical buffers held by the writer."""
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
        edge_table = pa.concat_tables([state.edge_table, _edges_table([edge])])
        next_catalog = self._ensure_catalog(state.catalog, labels=(), rel_types=(rel_type,))
        next_state = self._publish_graph(
            previous_state=state,
            catalog=next_catalog,
            nodes=list(state.nodes),
            edges=[*state.edges, edge],
            edge_table=edge_table,
        )
        return next_state, edge

    @writer_locked
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

    @writer_locked
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

    @writer_locked
    def update_properties_batch(
        self,
        *,
        node_updates: tuple[tuple[int, str, object], ...],
        edge_updates: tuple[tuple[int, str, object], ...],
        remove_nulls: bool = False,
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
            if remove_nulls and value is None:
                properties.pop(property_name, None)
            else:
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
            if remove_nulls and value is None:
                properties.pop(property_name, None)
            else:
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

    @writer_locked
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

    @writer_locked
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

    @writer_locked
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

    @writer_locked
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

    @writer_locked
    def _publish_graph(
        self,
        *,
        previous_state: GraphState,
        catalog: VersionedCatalog,
        nodes: list[NodeRecord],
        edges: list[EdgeRecord],
        edge_table: pa.Table | None = None,
        idempotency_receipt: IdempotencyReceipt | None = None,
    ) -> GraphState:
        """Publish through the selected backend's atomic commit implementation."""
        return self._storage_backend.publish_graph(
            previous_state=previous_state,
            catalog=catalog,
            nodes=nodes,
            edges=edges,
            edge_table=edge_table,
            idempotency_receipt=idempotency_receipt,
        )

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
            indexes=catalog.indexes,
        )

    def _validate_new_nodes(
        self,
        *,
        catalog: VersionedCatalog,
        existing_nodes: tuple[NodeRecord, ...],
        new_nodes: tuple[NodeRecord, ...],
    ) -> None:
        """Validate incoming nodes incrementally so uniqueness checks see earlier inserts too."""
        if not catalog.constraints and all(prop.nullable for prop in catalog.properties):
            return
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
        if catalog.constraints or any(not prop.nullable for prop in catalog.properties):
            for node in nodes:
                other_nodes = tuple(
                    candidate for candidate in nodes if candidate.node_id != node.node_id
                )
                self._validate_node(catalog=catalog, nodes=other_nodes, node=node)
        nodes_by_id = _nodes_by_id(nodes)
        for edge in edges:
            self._validate_edge(catalog=catalog, nodes=nodes, nodes_by_id=nodes_by_id, edge=edge)

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

    def __init__(
        self, graph_service: GraphStateUseCase, *, base_snapshot: SnapshotManifest | None = None
    ) -> None:
        """Start an isolated in-memory transaction from the current published snapshot."""
        self._graph_service = graph_service
        with graph_service.writer_lock(), ExitStack() as leases:
            snapshot = base_snapshot or graph_service.capture_snapshot()
            leases.enter_context(graph_service.pin_snapshot(snapshot))
            self._base_state = graph_service.load_state(snapshot=snapshot)
            self._snapshot_lease = leases.pop_all()
        self.transaction_time_ns = time.time_ns()
        self._state = self._base_state
        self._reuse_edge_table_on_commit = False
        self._closed = False
        self._adjacency_table: pa.Table | None = None
        self._adjacency_index: GraphAdjacency | None = None

    def __enter__(self) -> GraphTransactionUseCase:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        if not self._closed:
            self.rollback()

    @contextmanager
    def statement(self) -> Generator[None, None, None]:
        """Keep earlier transaction changes if an updating statement fails."""
        self._ensure_open()
        before = self._state
        reuse_edge_table = self._reuse_edge_table_on_commit
        try:
            yield
        except BaseException:
            self._state = before
            self._reuse_edge_table_on_commit = reuse_edge_table
            self._adjacency_table = None
            self._adjacency_index = None
            raise

    def adjacency_for_state(self, state: GraphState | GraphTabularState) -> GraphAdjacency | None:
        """Share published positions only for the exact base table; isolate overlays."""
        if self._closed:
            raise RuntimeError("Transaction is closed")
        if state.edge_table is self._base_state.edge_table:
            return self._graph_service.adjacency_for_state(state)
        if state.edge_table is not self._adjacency_table:
            self._adjacency_table = state.edge_table
            size = GraphAdjacency.reservation(
                state.edge_table.num_rows,
                (item.name for item in state.catalog.relationship_types),
                self._graph_service.adjacency_indexes.max_bytes,
                node_count=state.node_table.num_rows,
            )
            self._adjacency_index = GraphAdjacency(reserved_bytes=size) if size else None
        return self._adjacency_index

    def load_state(self) -> GraphState:
        """Return the current transaction-local graph state."""

        return self._state

    def load_tabular_state(self, *, include_edges: bool = True) -> GraphTabularState:
        """Return the current transaction-local state in Arrow-first form."""

        return GraphTabularState(
            catalog=self._state.catalog,
            snapshot=self._state.snapshot,
            node_table=self._state.node_table,
            edge_table=self._state.edge_table if include_edges else EDGE_SCHEMA.empty_table(),
        )

    def cypher_query_slot(self) -> AbstractContextManager[None]:
        """Share the application's statement limit with explicit transactions."""
        return self._graph_service.cypher_query_slot()

    def borrow_tabular_state(
        self, *, include_edges: bool = True
    ) -> AbstractContextManager[GraphTabularState]:
        """Keep transaction-local overlays on their existing read path."""
        return nullcontext(self.load_tabular_state(include_edges=include_edges))

    def replace_records(
        self,
        nodes: tuple[NodeRecord, ...],
        edges: tuple[EdgeRecord, ...],
        *,
        edge_property_update: bool = False,
        deleted_edge_ids: set[int] | None = None,
    ) -> GraphState:
        """Validate and stage the records produced by one updating clause."""
        self._ensure_open()
        nodes_unchanged = len(nodes) == len(self._state.nodes) and all(
            new is old for new, old in zip(nodes, self._state.nodes, strict=True)
        )
        if (
            nodes_unchanged
            and len(edges) == len(self._state.edges)
            and all(new is old for new, old in zip(edges, self._state.edges, strict=True))
        ):
            return self._state
        catalog = self._graph_service._ensure_catalog(
            self._state.catalog,
            labels=tuple(label for node in nodes for label in node.labels),
            rel_types=tuple(edge.rel_type for edge in edges),
        )
        self._graph_service._validate_full_graph(catalog=catalog, nodes=nodes, edges=edges)
        if deleted_edge_ids and nodes_unchanged:
            return self._stage_edge_table(
                catalog,
                self._state.nodes,
                edges,
                _without_edge_ids(self._state.edge_table, deleted_edge_ids),
            )
        if edge_property_update:
            positions = [
                index
                for index, (new, old) in enumerate(zip(edges, self._state.edges, strict=True))
                if new is not old
            ]
            if positions:
                return self._stage_edge_properties(
                    catalog, self._state.nodes if nodes_unchanged else nodes, edges, positions
                )
        return self._replace_state(catalog=catalog, nodes=list(nodes), edges=list(edges))

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
        remove_nulls: bool = False,
    ) -> GraphState:
        """Apply property updates inside the transaction-local state."""

        self._ensure_open()
        if not node_updates and not edge_updates:
            return self._state

        nodes = list(self._state.nodes)
        edges = list(self._state.edges)
        node_indexes = (
            {node.node_id: index for index, node in enumerate(nodes)} if node_updates else {}
        )
        edge_indexes = (
            {edge.edge_id: index for index, edge in enumerate(edges)} if edge_updates else {}
        )
        changed_edge_positions: set[int] = set()

        for node_id, property_name, value in node_updates:
            index = node_indexes.get(node_id)
            if index is None:
                raise NotFoundError(f"Node does not exist: {node_id}")
            properties = dict(nodes[index].properties)
            if remove_nulls and value is None:
                properties.pop(property_name, None)
            else:
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
            changed_edge_positions.add(index)
            properties = dict(edges[index].properties)
            if remove_nulls and value is None:
                properties.pop(property_name, None)
            else:
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
        if changed_edge_positions:
            return self._stage_edge_properties(
                self._state.catalog,
                tuple(nodes) if node_updates else self._state.nodes,
                tuple(edges),
                sorted(changed_edge_positions),
            )
        return self._replace_state(catalog=self._state.catalog, nodes=nodes, edges=edges)

    def _stage_edge_properties(
        self,
        catalog: VersionedCatalog,
        nodes: tuple[NodeRecord, ...],
        edges: tuple[EdgeRecord, ...],
        positions: list[int],
    ) -> GraphState:
        edge_table = _replace_edge_property_rows(self._state.edge_table, edges, positions)
        return self._stage_edge_table(catalog, nodes, edges, edge_table)

    def _stage_edge_table(
        self,
        catalog: VersionedCatalog,
        nodes: tuple[NodeRecord, ...],
        edges: tuple[EdgeRecord, ...],
        edge_table: pa.Table,
    ) -> GraphState:
        node_table = (
            self._state.node_table if nodes is self._state.nodes else _nodes_table(list(nodes))
        )
        self._state = replace(
            self._state,
            catalog=catalog,
            node_table=node_table,
            edge_table=edge_table,
            nodes=nodes,
            edges=edges,
        )
        self._adjacency_table = None
        self._adjacency_index = None
        self._reuse_edge_table_on_commit = True
        return self._state

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

        edge_id_set = set(edge_ids)
        if not node_ids:
            remaining = tuple(edge for edge in self._state.edges if edge.edge_id not in edge_id_set)
            if len(self._state.edges) - len(remaining) != len(edge_id_set):
                missing = edge_id_set.difference(edge.edge_id for edge in self._state.edges)
                raise NotFoundError(f"Relationship does not exist: {min(missing)}")
            self._graph_service._validate_full_graph(
                catalog=self._state.catalog,
                nodes=self._state.nodes,
                edges=remaining,
            )
            return self._stage_edge_table(
                self._state.catalog,
                self._state.nodes,
                remaining,
                _without_edge_ids(self._state.edge_table, edge_id_set),
            )

        node_id_set = set(node_ids)
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
                    "DELETE cannot remove a node with existing relationships.",
                    category="ConstraintVerificationFailed",
                    phase="runtime",
                    detail="DeleteConnectedNode",
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

    def commit(self, *, idempotency_receipt: IdempotencyReceipt | None = None) -> GraphState:
        """Persist the transaction-local state as a new published snapshot."""

        self._ensure_open()
        self._closed = True
        self._adjacency_table = None
        self._adjacency_index = None
        with self._snapshot_lease:
            if idempotency_receipt is None and self._state == self._base_state:
                return self._base_state
            return self._graph_service._publish_graph(
                previous_state=self._base_state,
                catalog=self._state.catalog,
                nodes=list(self._state.nodes),
                edges=list(self._state.edges),
                edge_table=self._state.edge_table if self._reuse_edge_table_on_commit else None,
                idempotency_receipt=idempotency_receipt,
            )

    def rollback(self) -> GraphState:
        """Discard all local changes and close the transaction."""

        self._ensure_open()
        self._closed = True
        self._adjacency_table = None
        self._adjacency_index = None
        self._state = self._base_state
        self._snapshot_lease.close()
        return self._base_state

    def _replace_state(
        self,
        *,
        catalog: VersionedCatalog,
        nodes: list[NodeRecord],
        edges: list[EdgeRecord],
    ) -> GraphState:
        """Rebuild the transaction-local Arrow tables after an in-memory mutation."""
        self._adjacency_table = None
        self._adjacency_index = None
        node_table = pa.table(
            {
                "node_id": pa.array([node.node_id for node in nodes], type=pa.int64()),
                "labels": pa.array(
                    [list(node.labels) for node in nodes],
                    type=pa.list_(pa.string()),
                ),
                "properties_json": pa.array(
                    [property_codec.dumps(node.properties) for node in nodes],
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
                    [property_codec.dumps(edge.properties) for edge in edges],
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


def _without_edge_ids(table: pa.Table, edge_ids: set[int]) -> pa.Table:
    """Remove edges in row order and reuse Arrow buffers for sparse deletions."""
    mask = pc.is_in(table["edge_id"], value_set=pa.array(sorted(edge_ids), type=pa.int64()))
    if max(column.num_chunks for column in table.columns) + len(edge_ids) > 256:
        return table.filter(pc.invert(mask))

    spans = []
    start = 0
    for position in pc.indices_nonzero(mask).to_pylist():
        if position > start:
            spans.append(table.slice(start, position - start))
        start = position + 1
    if start < table.num_rows:
        spans.append(table.slice(start))
    return pa.concat_tables(spans) if spans else table.slice(0, 0)


def _replace_edge_property_rows(
    table: pa.Table, edges: tuple[EdgeRecord, ...], positions: list[int]
) -> pa.Table:
    """Replace encoded properties while keeping the endpoint and ID columns."""
    field = table.schema.field("properties_json")
    column = table[field.name]
    replacements = pa.array(
        [property_codec.dumps(edges[position].properties) for position in positions],
        type=field.type,
    )
    if column.num_chunks + 2 * len(positions) <= 256:
        chunks = []
        start = 0
        for offset, position in enumerate(positions):
            if position > start:
                chunks.extend(column.slice(start, position - start).chunks)
            chunks.append(replacements.slice(offset, 1))
            start = position + 1
        if start < len(column):
            chunks.extend(column.slice(start).chunks)
        updated = pa.chunked_array(chunks, type=field.type)
    else:
        # Bound fragmentation after large batches or repeated sparse updates.
        identifiers = table["edge_id"]
        changed_ids = pc.take(identifiers, pa.array(positions, type=pa.int64()))
        mask = pc.is_in(identifiers, value_set=changed_ids).combine_chunks()
        updated = pc.replace_with_mask(column.combine_chunks(), mask, replacements)
    return table.set_column(table.schema.get_field_index(field.name), field, updated)


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


__all__ = ["GraphState", "GraphStateUseCase", "GraphTransactionUseCase", "QueryResult"]
