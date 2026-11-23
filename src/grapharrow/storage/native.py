"""Native Parquet stores, atomic publication and selective graph updates."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import AbstractContextManager, contextmanager, nullcontext
from dataclasses import replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pyarrow as pa  # type: ignore[import-untyped]

from grapharrow.adapters import FileSystemAdapter
from grapharrow.adapters.writer import WriterLock, writer_locked
from grapharrow.core.errors import WriteConflictError
from grapharrow.domain import SnapshotId, SnapshotManifest, VersionedCatalog
from grapharrow.domain.records import EdgeRecord, NodeRecord
from grapharrow.domain.snapshot import IdempotencyReceipt
from grapharrow.repositories import CatalogRepository, GraphRepository, SnapshotRepository
from grapharrow.storage.contracts import CatalogStore, GraphStore, SnapshotStore, StorageMaintenance
from grapharrow.storage.native_maintenance import NativeStorageMaintenance

if TYPE_CHECKING:
    from grapharrow.use_cases.graph import GraphState


class NativeStorageBackend:
    """Compose existing native stores without changing their layout or I/O behavior."""

    def __init__(
        self,
        *,
        catalog: CatalogRepository,
        snapshot: SnapshotRepository,
        graph: GraphRepository,
        filesystem: FileSystemAdapter | None = None,
    ) -> None:
        self._catalog = catalog
        self._snapshot = snapshot
        self._graph = graph
        self._writer = snapshot.writer
        self._maintenance = NativeStorageMaintenance(
            layout=snapshot.layout,
            catalog_store=catalog,
            snapshot_store=snapshot,
            graph_store=graph,
            filesystem=filesystem,
        )

    @property
    def name(self) -> str:
        return "native"

    @property
    def catalog(self) -> CatalogStore:
        return self._catalog

    @property
    def snapshot(self) -> SnapshotStore:
        return self._snapshot

    @property
    def graph(self) -> GraphStore:
        return self._graph

    @property
    def writer(self) -> WriterLock:
        return self._writer

    @property
    def maintenance(self) -> StorageMaintenance:
        return self._maintenance

    def capture_snapshot(self) -> SnapshotManifest:
        return self._catalog.bootstrap_snapshot()

    def borrow_current_snapshot(self) -> AbstractContextManager[SnapshotManifest]:
        return nullcontext(self.capture_snapshot())

    def snapshot_open_guard(self) -> AbstractContextManager[None]:
        """Prevent vacuum from removing files while a snapshot is materialized."""
        if self._writer.read_only:
            # Native vacuum keeps every published snapshot reachable from the head.
            return nullcontext()
        return self._writer.hold()

    def pin_snapshot(self, snapshot_id: SnapshotId) -> AbstractContextManager[None]:
        return nullcontext()

    @contextmanager
    def edit_edge_properties_by_id(
        self, *, edge_id: int, rel_type: str | None, properties: dict[str, object]
    ) -> Generator[tuple[EdgeRecord | None] | None, None, None]:
        """Publish a selective edit only after its caller successfully builds the result.

        None selects the full write path. A one-element tuple containing None
        represents a successful lookup with no match. Explicit transactions keep
        their existing staging and rollback path.
        """
        with self.writer.hold():
            snapshot = self.capture_snapshot()
            catalog = self._catalog.load_for_snapshot(snapshot)
            if catalog.constraints:
                yield None
                return
            eligible, patch = self._graph.prepare_edge_property_patch(
                snapshot, edge_id=edge_id, rel_type=rel_type, properties=properties
            )
            if not eligible:
                yield None
                return
            yield (patch.after if patch is not None else None,)
            if patch is None or patch.before == patch.after:
                return
            if self._snapshot.load_current_pointer().snapshot_id != snapshot.snapshot_id:
                raise WriteConflictError(
                    "Database changed since this write began. Retry in a new transaction."
                )
            next_id = SnapshotId(snapshot.snapshot_id.value + 1)
            canonical, projected = self._graph.persist_edge_property_patch(patch, next_id)
            manifest = replace(
                snapshot,
                snapshot_id=next_id,
                parent_snapshot_id=snapshot.snapshot_id,
                commit_txid=next_id.value,
                created_at=_utc_now(),
                idempotency_receipt=None,
                edges=tuple(
                    canonical if file == patch.canonical_file else file for file in snapshot.edges
                ),
                edge_properties=tuple(
                    projected if file == patch.property_file else file
                    for file in snapshot.edge_properties
                ),
            )
            self._snapshot.publish(manifest, base=snapshot)

    @contextmanager
    def edit_node_properties_by_id(
        self, *, node_id: int, labels: tuple[str, ...], properties: dict[str, object]
    ) -> Generator[tuple[NodeRecord | None] | None, None, None]:
        """Publish the touched node range and projections once the result is ready."""
        with self.writer.hold():
            snapshot = self.capture_snapshot()
            if self._catalog.load_for_snapshot(snapshot).constraints:
                yield None
                return
            eligible, patch = self._graph.prepare_node_property_patch(
                snapshot, node_id=node_id, labels=labels, properties=properties
            )
            if not eligible:
                yield None
                return
            yield (patch.after if patch is not None else None,)
            if patch is None or patch.before == patch.after:
                return
            if self._snapshot.load_current_pointer().snapshot_id != snapshot.snapshot_id:
                raise WriteConflictError(
                    "Database changed since this write began. Retry in a new transaction."
                )
            next_id = SnapshotId(snapshot.snapshot_id.value + 1)
            replacements = self._graph.persist_node_property_patch(patch, next_id)
            manifest = replace(
                snapshot,
                snapshot_id=next_id,
                parent_snapshot_id=snapshot.snapshot_id,
                commit_txid=next_id.value,
                created_at=_utc_now(),
                idempotency_receipt=None,
                nodes=tuple(replacements.get(file, file) for file in snapshot.nodes),
                node_properties=tuple(
                    replacements.get(file, file) for file in snapshot.node_properties
                ),
            )
            self._snapshot.publish(manifest, base=snapshot)

    @writer_locked
    def publish_graph(
        self,
        *,
        previous_state: GraphState,
        catalog: VersionedCatalog,
        nodes: list[NodeRecord],
        edges: list[EdgeRecord],
        edge_table: pa.Table | None = None,
        idempotency_receipt: IdempotencyReceipt | None = None,
    ) -> GraphState:
        """Prepare immutable objects and publish the complete graph state."""
        from grapharrow.use_cases.graph import GraphState

        if self._snapshot.load_current_pointer().snapshot_id != (
            previous_state.snapshot.snapshot_id
        ):
            raise WriteConflictError(
                "Database changed since this write began. Retry in a new transaction."
            )
        catalog_path = previous_state.snapshot.catalog_path
        if catalog.version != previous_state.catalog.version:
            catalog_path = self._catalog.stage(catalog)

        next_snapshot_id = SnapshotId(previous_state.snapshot.snapshot_id.value + 1)
        persisted = self._graph.persist_snapshot(
            snapshot_id=next_snapshot_id,
            nodes=nodes,
            edges=edges,
            previous_snapshot=previous_state.snapshot,
            previous_nodes=previous_state.nodes,
            previous_edges=previous_state.edges,
            previous_edge_table=previous_state.edge_table,
            edge_table=edge_table,
        )
        manifest = SnapshotManifest(
            snapshot_id=next_snapshot_id,
            parent_snapshot_id=previous_state.snapshot.snapshot_id,
            commit_txid=next_snapshot_id.value,
            created_at=_utc_now(),
            catalog_version=catalog.version,
            catalog_path=catalog_path,
            nodes=persisted.nodes,
            edges=persisted.edges,
            node_labels=persisted.node_labels,
            node_properties=persisted.node_properties,
            adjacency_by_source=persisted.adjacency_by_source,
            adjacency_by_dest=persisted.adjacency_by_dest,
            edge_properties=persisted.edge_properties,
            idempotency_receipt=idempotency_receipt,
        )
        node_table = (
            previous_state.node_table
            if persisted.nodes == previous_state.snapshot.nodes
            else self._graph.load_node_table(persisted.nodes)
        )
        if persisted.edges == previous_state.snapshot.edges:
            edge_table = previous_state.edge_table
        elif edge_table is None:
            edge_table = self._graph.load_edge_table(persisted.edges)
        state = GraphState(
            catalog=catalog,
            snapshot=manifest,
            node_table=node_table,
            edge_table=edge_table,
            nodes=tuple(nodes),
            edges=tuple(edges),
        )
        self._snapshot.publish(manifest, base=previous_state.snapshot)
        return state


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")
