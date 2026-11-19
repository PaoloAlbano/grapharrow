"""Typed persistence contracts consumed by the graph engine and interfaces.

Dataset references identify immutable data selected by a snapshot. Implementations
own their physical layout; callers consume Arrow tables and graph records.
"""

from __future__ import annotations

from collections.abc import Generator, Iterator
from contextlib import AbstractContextManager
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, runtime_checkable

import pyarrow as pa  # type: ignore[import-untyped]

from grapharrow.adapters.writer import WriterLock
from grapharrow.core import DatabaseLayout
from grapharrow.domain import (
    CatalogVersion,
    CurrentSnapshotPointer,
    DatasetFile,
    SnapshotId,
    SnapshotManifest,
    VersionedCatalog,
)
from grapharrow.domain.records import EdgeRecord, NodeRecord
from grapharrow.domain.scan import ScanPredicate
from grapharrow.domain.snapshot import IdempotencyReceipt
from grapharrow.storage.models import (
    InitializationResult,
    RecoveryReport,
    RetentionPlan,
    VerificationReport,
)

if TYPE_CHECKING:
    from grapharrow.use_cases.graph import GraphState


class CatalogStore(Protocol):
    """CatalogStore operations used by storage clients."""

    @property
    def writer(self) -> WriterLock: ...

    @property
    def layout(self) -> DatabaseLayout: ...

    def bootstrap_snapshot(self) -> SnapshotManifest: ...

    def write(
        self, catalog: VersionedCatalog, *, base: SnapshotManifest | None = None
    ) -> str | Path: ...

    def load_for_snapshot(self, manifest: SnapshotManifest) -> VersionedCatalog: ...

    def load(self, version: CatalogVersion | None = None) -> VersionedCatalog: ...


class SnapshotStore(Protocol):
    """SnapshotStore operations used by storage clients."""

    @property
    def writer(self) -> WriterLock: ...

    @property
    def layout(self) -> DatabaseLayout: ...

    def current_exists(self) -> bool: ...

    def publish(
        self, manifest: SnapshotManifest, *, base: SnapshotManifest | None = None
    ) -> CurrentSnapshotPointer: ...

    def load_manifest(self, snapshot_id: SnapshotId | None = None) -> SnapshotManifest: ...

    def history(
        self, pointer: CurrentSnapshotPointer | None = None
    ) -> Iterator[tuple[str, SnapshotManifest]]: ...

    def idempotency_base(
        self, pointer: CurrentSnapshotPointer, key_hash: str
    ) -> tuple[SnapshotManifest, IdempotencyReceipt | None]: ...

    def load_at(self, pointer: CurrentSnapshotPointer) -> SnapshotManifest: ...

    def load_current_pointer(self) -> CurrentSnapshotPointer: ...

    def try_load_current_pointer(self) -> CurrentSnapshotPointer | None: ...


class GraphStore(Protocol):
    """GraphStore operations used by storage clients."""

    def load_node_table(self, files: tuple[DatasetFile, ...]) -> pa.Table: ...

    def load_edge_table(self, files: tuple[DatasetFile, ...]) -> pa.Table: ...

    def load_node_label_table(self, files: tuple[DatasetFile, ...]) -> pa.Table: ...

    @staticmethod
    def adjacency_files(
        files: tuple[DatasetFile, ...], *, rel_type: str
    ) -> tuple[DatasetFile, ...]: ...

    @staticmethod
    def has_complete_adjacency(
        snapshot: SnapshotManifest, rel_type: str, *, incoming: bool
    ) -> bool: ...

    def load_adjacency_by_source_table(
        self, files: tuple[DatasetFile, ...], *, predicates: tuple[ScanPredicate, ...] = ()
    ) -> pa.Table: ...

    def load_adjacency_by_dest_table(
        self, files: tuple[DatasetFile, ...], *, predicates: tuple[ScanPredicate, ...] = ()
    ) -> pa.Table: ...

    @staticmethod
    def node_property_files(
        files: tuple[DatasetFile, ...], *, label: str
    ) -> tuple[DatasetFile, ...]: ...

    @staticmethod
    def group_node_property_files(
        files: tuple[DatasetFile, ...],
    ) -> dict[str, tuple[DatasetFile, ...]]: ...

    def load_node_property_projection(
        self, files: tuple[DatasetFile, ...], *, label: str
    ) -> pa.Table: ...

    def load_node_property_table(
        self,
        files: tuple[DatasetFile, ...],
        *,
        label: str,
        columns: list[str] | None = None,
        predicates: tuple[ScanPredicate, ...] = (),
    ) -> pa.Table: ...

    def iter_node_property_batches(
        self,
        files: tuple[DatasetFile, ...],
        *,
        label: str,
        columns: list[str],
        predicates: tuple[ScanPredicate, ...] = (),
        batch_size: int = 65536,
    ) -> Generator[pa.RecordBatch, None, None]: ...

    def load_node_property_schema(
        self, files: tuple[DatasetFile, ...], *, label: str
    ) -> pa.Schema: ...

    def load_edge_property_table(
        self,
        files: tuple[DatasetFile, ...],
        *,
        rel_type: str,
        columns: list[str] | None = None,
        predicates: tuple[ScanPredicate, ...] = (),
    ) -> pa.Table: ...

    def load_nodes(self, files: tuple[DatasetFile, ...]) -> list[NodeRecord]: ...

    def load_edges(self, files: tuple[DatasetFile, ...]) -> list[EdgeRecord]: ...

    def node_records_from_table(self, table: pa.Table) -> list[NodeRecord]: ...

    def edge_records_from_table(self, table: pa.Table) -> list[EdgeRecord]: ...


class StorageMaintenance(Protocol):
    """Backend-owned integrity, recovery and snapshot maintenance."""

    def initialize(self) -> InitializationResult: ...
    def verify(self, *, verify_checksums: bool = False) -> VerificationReport: ...
    def recover(
        self,
        *,
        dry_run: bool = True,
        cleanup_orphan_staged: bool = True,
        verify_checksums: bool = False,
    ) -> RecoveryReport: ...
    def compact(self) -> SnapshotManifest: ...
    def vacuum(self) -> tuple[str, ...]: ...
    def plan_retention(self, *, keep_last: int, min_age_hours: int = 0) -> RetentionPlan: ...


@runtime_checkable
class StorageBackend(Protocol):
    """One coherent set of stores and publication rules for a database."""

    @property
    def name(self) -> str: ...
    @property
    def catalog(self) -> CatalogStore: ...
    @property
    def snapshot(self) -> SnapshotStore: ...
    @property
    def graph(self) -> GraphStore: ...
    @property
    def writer(self) -> WriterLock: ...
    @property
    def maintenance(self) -> StorageMaintenance: ...

    def capture_snapshot(self) -> SnapshotManifest: ...

    def publish_graph(
        self,
        *,
        previous_state: GraphState,
        catalog: VersionedCatalog,
        nodes: list[NodeRecord],
        edges: list[EdgeRecord],
        edge_table: pa.Table | None = None,
        idempotency_receipt: IdempotencyReceipt | None = None,
    ) -> GraphState: ...
    def edit_edge_properties_by_id(
        self, *, edge_id: int, rel_type: str | None, properties: dict[str, object]
    ) -> AbstractContextManager[tuple[EdgeRecord | None] | None]: ...
