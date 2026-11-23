"""Storage publication and maintenance results."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Literal

import pyarrow as pa  # type: ignore[import-untyped]

from grapharrow.domain import (
    CatalogVersion,
    CurrentSnapshotPointer,
    DatasetFile,
    SnapshotManifest,
    VersionedCatalog,
)
from grapharrow.domain.records import EdgeRecord, NodeRecord
from grapharrow.domain.snapshot import IdempotencyReceipt


@dataclass(frozen=True, slots=True)
class PointWriteReceipt:
    """Publish a receipt prepared after building the result, against the lookup snapshot."""

    base: SnapshotManifest
    prepare: Callable[[], IdempotencyReceipt]


@dataclass(frozen=True, slots=True)
class PersistedGraph:
    """Files generated for a published graph snapshot."""

    nodes: tuple[DatasetFile, ...]
    edges: tuple[DatasetFile, ...]
    node_labels: tuple[DatasetFile, ...]
    node_properties: tuple[DatasetFile, ...]
    adjacency_by_source: tuple[DatasetFile, ...]
    adjacency_by_dest: tuple[DatasetFile, ...]
    edge_properties: tuple[DatasetFile, ...]


@dataclass(frozen=True, slots=True)
class EdgePropertyPatch:
    """A schema-preserving edit of one canonical range and its property projection."""

    before: EdgeRecord
    after: EdgeRecord
    canonical_file: DatasetFile
    canonical_table: pa.Table
    property_file: DatasetFile
    property_table: pa.Table


@dataclass(frozen=True, slots=True)
class NodePropertyPatch:
    """A schema-preserving edit of one node range and its label projections."""

    before: NodeRecord
    after: NodeRecord
    canonical_file: DatasetFile
    canonical_table: pa.Table
    projections: tuple[tuple[DatasetFile, pa.Table], ...]


@dataclass(frozen=True, slots=True)
class InitializationResult:
    """Result of bootstrapping a database layout."""

    catalog: VersionedCatalog
    snapshot: SnapshotManifest


@dataclass(frozen=True, slots=True)
class VerificationReport:
    """Structured result of a database verification pass."""

    ok: bool
    catalog_version: CatalogVersion | None
    snapshot_pointer: CurrentSnapshotPointer | None
    missing_paths: tuple[str, ...] = field(default_factory=tuple)
    checksum_failures: tuple[str, ...] = field(default_factory=tuple)
    warnings: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class RecoveryReport:
    """Structured result of a recovery pass."""

    verification: VerificationReport
    orphan_staged_paths: tuple[str, ...] = field(default_factory=tuple)
    removed_paths: tuple[str, ...] = field(default_factory=tuple)
    warnings: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class StorageFileSummary:
    """Observed object count, logical size and modification-time range."""

    file_count: int
    size_bytes: int
    oldest_modified_at: str | None
    newest_modified_at: str | None


@dataclass(frozen=True, slots=True)
class DeltaRetentionPin:
    """Snapshot protection held by one replica incarnation."""

    replica_id: str
    incarnation_id: str
    snapshot_id: int
    acquired_at: str
    age_seconds: float


@dataclass(frozen=True, slots=True)
class DeltaRetentionStatus:
    """Current replica coordination state for managed retention."""

    minimum_snapshot_id: int
    active_pins: int
    oldest_pinned_snapshot_id: int | None
    cleanup_mode: Literal["maintenance", "online"] | None
    pins: tuple[DeltaRetentionPin, ...] = ()


@dataclass(frozen=True, slots=True)
class DeltaStorageInventory:
    """Classify observed Delta files against one published graph snapshot."""

    head_snapshot_id: int
    delta_version: int
    latest_checkpoint_version: int | None
    commits_since_checkpoint: int
    active_parquet: StorageFileSummary
    historical_parquet: StorageFileSummary
    untracked_parquet: StorageFileSummary
    commit_logs: StorageFileSummary
    checkpoints: StorageFileSummary
    missing_active_parquet_count: int
    retention: DeltaRetentionStatus | None = None


@dataclass(frozen=True, slots=True)
class DeltaRetentionResult:
    """Completed cleanup and the durable floor shared by all replicas."""

    minimum_snapshot_id: int
    removed_parquet_paths: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class DeltaPinRecoveryResult:
    """Pins reclaimed after fencing one replica incarnation."""

    incarnation_id: str
    released_pins: int


@dataclass(frozen=True, slots=True)
class RetentionFile:
    """One existing Parquet file outside the proposed retention window."""

    path: str
    size_bytes: int


@dataclass(frozen=True, slots=True)
class RetentionPlan:
    """Estimated data retention against one published head."""

    head_snapshot_id: int
    evaluated_at: str
    keep_last: int
    min_age_hours: int
    retained_snapshot_ids: tuple[int, ...]
    older_snapshot_ids: tuple[int, ...]
    shared_parquet_paths: tuple[str, ...]
    candidate_files: tuple[RetentionFile, ...]
    missing_candidate_paths: tuple[str, ...]
    estimated_reclaimable_bytes: int
