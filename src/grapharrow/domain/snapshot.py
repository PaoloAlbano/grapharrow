"""Snapshot domain models used by the storage layer."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Literal

from grapharrow.domain.ids import CatalogVersion, SnapshotId

DatasetKind = Literal[
    "nodes",
    "edges",
    "node_labels",
    "node_properties",
    "adjacency_by_source",
    "adjacency_by_dest",
    "edge_properties",
]
ChecksumAlgorithm = Literal["sha256"]


@dataclass(frozen=True, slots=True)
class DatasetFile:
    """Metadata for a single file referenced by a snapshot manifest."""

    path: str
    row_count: int
    kind: DatasetKind
    checksum: str | None = None
    checksum_algorithm: ChecksumAlgorithm | None = None
    min_key: int | None = None
    max_key: int | None = None


@dataclass(frozen=True, slots=True)
class SnapshotManifest:
    """Immutable description of a published snapshot."""

    snapshot_id: SnapshotId
    parent_snapshot_id: SnapshotId | None
    commit_txid: int
    created_at: str
    catalog_version: CatalogVersion
    nodes: tuple[DatasetFile, ...] = field(default_factory=tuple)
    edges: tuple[DatasetFile, ...] = field(default_factory=tuple)
    node_labels: tuple[DatasetFile, ...] = field(default_factory=tuple)
    node_properties: tuple[DatasetFile, ...] = field(default_factory=tuple)
    adjacency_by_source: tuple[DatasetFile, ...] = field(default_factory=tuple)
    adjacency_by_dest: tuple[DatasetFile, ...] = field(default_factory=tuple)
    edge_properties: tuple[DatasetFile, ...] = field(default_factory=tuple)

    @classmethod
    def empty(
        cls,
        *,
        snapshot_id: SnapshotId,
        catalog_version: CatalogVersion,
        commit_txid: int = 0,
        created_at: str | None = None,
    ) -> SnapshotManifest:
        """Build an empty manifest suitable for database bootstrap."""

        return cls(
            snapshot_id=snapshot_id,
            parent_snapshot_id=None,
            commit_txid=commit_txid,
            created_at=created_at or _utc_now(),
            catalog_version=catalog_version,
        )


@dataclass(frozen=True, slots=True)
class CurrentSnapshotPointer:
    """Pointer to the snapshot currently visible to readers."""

    snapshot_id: SnapshotId
    manifest_path: str
    commit_txid: int
    created_at: str


def _utc_now() -> str:
    return datetime.now(tz=UTC).isoformat(timespec="seconds")
