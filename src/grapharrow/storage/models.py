"""Storage publication and maintenance results."""

from __future__ import annotations

from dataclasses import dataclass, field

import pyarrow as pa  # type: ignore[import-untyped]

from grapharrow.domain import (
    CatalogVersion,
    CurrentSnapshotPointer,
    DatasetFile,
    SnapshotManifest,
    VersionedCatalog,
)
from grapharrow.domain.records import EdgeRecord


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
