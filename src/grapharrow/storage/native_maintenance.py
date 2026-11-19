"""Use cases for bootstrap, verification, and recovery."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from grapharrow.adapters import FileSystemAdapter
from grapharrow.adapters.checksum import sha256_file as _compute_sha256
from grapharrow.adapters.writer import writer_locked
from grapharrow.core import DatabaseLayout
from grapharrow.core.errors import StorageError
from grapharrow.domain import (
    CurrentSnapshotPointer,
    DatasetFile,
    SnapshotId,
    SnapshotManifest,
    VersionedCatalog,
)
from grapharrow.storage.contracts import CatalogStore, GraphStore, SnapshotStore
from grapharrow.storage.models import (
    InitializationResult,
    RecoveryReport,
    RetentionFile,
    RetentionPlan,
    VerificationReport,
)


class NativeStorageMaintenance:
    """High-level operations built on top of the storage primitives."""

    def __init__(
        self,
        *,
        layout: DatabaseLayout,
        catalog_store: CatalogStore,
        snapshot_store: SnapshotStore,
        graph_store: GraphStore,
        filesystem: FileSystemAdapter | None = None,
    ) -> None:
        """Wire the storage collaborators used by bootstrap, verification, and maintenance flows."""
        self._layout = layout
        self._catalog_store = catalog_store
        self._snapshot_store = snapshot_store
        self._graph_store = graph_store
        self._filesystem = filesystem or FileSystemAdapter()
        self._writer = snapshot_store.writer

    def initialize(self) -> InitializationResult:
        """Create the base layout, catalog, and first empty snapshot."""

        snapshot = self._catalog_store.bootstrap_snapshot()
        return InitializationResult(
            catalog=self._catalog_store.load_for_snapshot(snapshot), snapshot=snapshot
        )

    def verify(self, *, verify_checksums: bool = False) -> VerificationReport:
        """Verify that the current catalog and snapshot are readable and consistent."""

        missing_paths: list[str] = []
        checksum_failures: list[str] = []
        warnings: list[str] = []

        catalog: VersionedCatalog | None = None
        pointer: CurrentSnapshotPointer | None = None
        manifest: SnapshotManifest | None = None

        try:
            pointer = self._snapshot_store.load_current_pointer()
            manifest = self._snapshot_store.load_at(pointer)
        except Exception as exc:  # pragma: no cover - thin error aggregation
            warnings.append(f"Failed to load current snapshot: {exc}")

        if manifest is not None:
            try:
                catalog = self._catalog_store.load_for_snapshot(manifest)
            except Exception as exc:  # pragma: no cover - thin error aggregation
                warnings.append(f"Failed to load current catalog: {exc}")

        if manifest is not None:
            try:
                self._snapshot_store.validate_manifest(manifest)
            except Exception as exc:  # pragma: no cover - thin error aggregation
                warnings.append(f"Current snapshot manifest is invalid: {exc}")
            for dataset_file in _iter_manifest_files(manifest):
                absolute_path = self._absolute_location(dataset_file.path)
                if not self._filesystem.exists(absolute_path):
                    missing_paths.append(dataset_file.path)
                    continue

                if verify_checksums and dataset_file.checksum is not None:
                    if dataset_file.checksum_algorithm != "sha256":
                        warnings.append(
                            f"Unsupported checksum algorithm for {dataset_file.path}: "
                            f"{dataset_file.checksum_algorithm}"
                        )
                        continue

                    digest = _compute_sha256(absolute_path, filesystem=self._filesystem)
                    if digest != dataset_file.checksum:
                        checksum_failures.append(dataset_file.path)

        ok = not missing_paths and not checksum_failures and not warnings

        return VerificationReport(
            ok=ok,
            catalog_version=catalog.version if catalog is not None else None,
            snapshot_pointer=pointer,
            missing_paths=tuple(missing_paths),
            checksum_failures=tuple(checksum_failures),
            warnings=tuple(warnings),
        )

    @writer_locked
    def recover(
        self,
        *,
        dry_run: bool = True,
        cleanup_orphan_staged: bool = True,
        verify_checksums: bool = False,
    ) -> RecoveryReport:
        """Run a conservative recovery pass on the database layout."""

        verification = self.verify(verify_checksums=verify_checksums)
        orphan_staged_paths = tuple(self._list_orphan_staged_paths())
        removed_paths: list[str] = []
        warnings: list[str] = []

        if cleanup_orphan_staged and not dry_run and self._writer.local_root is not None:
            for relative_path in orphan_staged_paths:
                absolute_path = self._absolute_location(relative_path)
                if self._filesystem.exists(absolute_path):
                    try:
                        self._filesystem.delete_dir(absolute_path)
                    except Exception:
                        self._filesystem.delete_file(absolute_path)
                removed_paths.append(relative_path)
        elif orphan_staged_paths and cleanup_orphan_staged and not dry_run:
            warnings.append("Remote staged paths were retained for offline inspection.")
        elif orphan_staged_paths and not cleanup_orphan_staged:
            warnings.append("Orphan staged paths were detected but cleanup is disabled.")

        return RecoveryReport(
            verification=verification,
            orphan_staged_paths=orphan_staged_paths,
            removed_paths=tuple(removed_paths),
            warnings=tuple(warnings),
        )

    @writer_locked
    def compact(self) -> SnapshotManifest:
        """Rewrite the current graph state into a fresh snapshot and rebuild derived files."""

        current_manifest = self._catalog_store.bootstrap_snapshot()
        nodes = self._graph_store.load_nodes(current_manifest.nodes)
        edges = self._graph_store.load_edges(current_manifest.edges)

        next_snapshot_id = SnapshotId(current_manifest.snapshot_id.value + 1)
        persisted = self._graph_store.persist_snapshot(
            snapshot_id=next_snapshot_id,
            nodes=nodes,
            edges=edges,
            previous_snapshot=current_manifest,
        )
        manifest = SnapshotManifest(
            snapshot_id=next_snapshot_id,
            parent_snapshot_id=current_manifest.snapshot_id,
            commit_txid=next_snapshot_id.value,
            created_at=datetime.now(UTC).isoformat(timespec="seconds"),
            catalog_version=current_manifest.catalog_version,
            catalog_path=current_manifest.catalog_path,
            nodes=persisted.nodes,
            edges=persisted.edges,
            node_labels=persisted.node_labels,
            node_properties=persisted.node_properties,
            adjacency_by_source=persisted.adjacency_by_source,
            adjacency_by_dest=persisted.adjacency_by_dest,
            edge_properties=persisted.edge_properties,
        )
        self._snapshot_store.publish(manifest, base=current_manifest)
        return manifest

    def plan_retention(self, *, keep_last: int, min_age_hours: int = 0) -> RetentionPlan:
        """Estimate reclaimable Parquet bytes without changing the database."""
        if keep_last < 1:
            raise ValueError("keep_last must be at least 1.")
        if min_age_hours < 0:
            raise ValueError("min_age_hours must be nonnegative.")

        head = self._snapshot_store.load_current_pointer()
        now = datetime.now(UTC)
        retained_ids: list[int] = []
        older_ids: list[int] = []
        retained_paths: set[str] = set()
        older_paths: set[str] = set()
        for position, (_, snapshot) in enumerate(self._snapshot_store.history(head)):
            snapshot_id = snapshot.snapshot_id.value
            paths = (
                entry.path
                for entry in _iter_manifest_files(snapshot)
                if entry.path.endswith(".parquet")
            )
            if position < keep_last or (
                min_age_hours and _age_in_hours(snapshot, now) <= min_age_hours
            ):
                # Keep a contiguous range even if an ancestor has a newer timestamp.
                retained_ids.extend(older_ids)
                retained_ids.append(snapshot_id)
                retained_paths.update(older_paths)
                retained_paths.update(paths)
                older_ids.clear()
                older_paths.clear()
            else:
                older_ids.append(snapshot_id)
                older_paths.update(paths)

        candidates: list[RetentionFile] = []
        missing: list[str] = []
        for path in sorted(older_paths - retained_paths):
            size = self._filesystem.file_size(self._absolute_location(path))
            if size is None:
                missing.append(path)
            else:
                candidates.append(RetentionFile(path, size))

        return RetentionPlan(
            head_snapshot_id=head.snapshot_id.value,
            evaluated_at=now.isoformat(),
            keep_last=keep_last,
            min_age_hours=min_age_hours,
            retained_snapshot_ids=tuple(retained_ids),
            older_snapshot_ids=tuple(older_ids),
            shared_parquet_paths=tuple(sorted(retained_paths & older_paths)),
            candidate_files=tuple(candidates),
            missing_candidate_paths=tuple(missing),
            estimated_reclaimable_bytes=sum(file.size_bytes for file in candidates),
        )

    @writer_locked
    def vacuum(self) -> tuple[str, ...]:
        """Remove abandoned files while preserving every published snapshot."""

        self._catalog_store.bootstrap_snapshot()
        head = self._snapshot_store.load_current_pointer()
        keep_paths = set(self._snapshot_store.retained_paths(head))

        local = self._writer.local_root is not None
        removed: list[str] = []
        for directory, suffix in (
            (self._layout.manifests_dir, ".json"),
            (self._layout.idempotency_dir, ".json"),
            (self._layout.nodes_dir, ".parquet"),
            (self._layout.edges_dir, ".parquet"),
            (self._layout.node_labels_dir, ".parquet"),
            (self._layout.node_properties_dir, ".parquet"),
            (self._layout.adjacency_by_source_dir, ".parquet"),
            (self._layout.adjacency_by_dest_dir, ".parquet"),
            (self._layout.edge_properties_dir, ".parquet"),
        ):
            if not self._filesystem.exists(directory):
                continue
            for path in self._filesystem.list_paths(directory, recursive=True):
                if not str(path).endswith(suffix):
                    continue
                relative = self._layout.relative_path(path)
                if relative in keep_paths:
                    continue
                if not local and not _past_attempt(relative, head.snapshot_id):
                    continue
                if self._filesystem.exists(path):
                    try:
                        self._filesystem.delete_file(path)
                    except FileNotFoundError:
                        continue
                    removed.append(relative)

        if local:
            for staged in self._list_orphan_staged_paths():
                absolute = self._absolute_location(staged)
                if self._filesystem.exists(absolute):
                    self._filesystem.delete_dir(absolute)
                    removed.append(staged)

        return tuple(sorted(removed))

    def _list_orphan_staged_paths(self) -> list[str]:
        """List staged transaction directories that are not referenced by the current snapshot."""
        if not self._filesystem.exists(self._layout.staged_dir):
            return []

        return sorted(
            self._layout.relative_path(path)
            for path in self._filesystem.list_paths(self._layout.staged_dir, recursive=False)
        )

    def _absolute_location(self, relative_path: str) -> str | Path:
        """Resolve a database-relative path against the configured root location."""
        if isinstance(self._layout.root, Path):
            return self._layout.root / relative_path
        return f"{str(self._layout.root).rstrip('/')}/{relative_path}"


def _age_in_hours(snapshot: SnapshotManifest, now: datetime) -> float:
    try:
        created_at = datetime.fromisoformat(snapshot.created_at)
        if created_at.tzinfo is None:
            raise ValueError("Expected a timezone")
    except ValueError as exc:
        raise StorageError(
            f"Snapshot {snapshot.snapshot_id.value} has an invalid timestamp."
        ) from exc
    return (now - created_at).total_seconds() / 3600


def _past_attempt(path: str, head: SnapshotId) -> bool:
    # Across pods only delete objects whose generation can no longer win a head update.
    prefix, _, filename = Path(path).stem.partition("-")
    generation, _, attempt = filename.partition("-")
    return (
        prefix == "snapshot"
        and bool(attempt)
        and generation.isascii()
        and generation.isdecimal()
        and int(generation) <= head.value
    )


def _iter_manifest_files(manifest: SnapshotManifest) -> tuple[DatasetFile, ...]:
    """Flatten every dataset reference carried by a snapshot manifest into one iterable tuple."""
    return (
        *manifest.nodes,
        *manifest.edges,
        *manifest.node_labels,
        *manifest.node_properties,
        *manifest.adjacency_by_source,
        *manifest.adjacency_by_dest,
        *manifest.edge_properties,
    )


__all__ = [
    "NativeStorageMaintenance",
    "InitializationResult",
    "RecoveryReport",
    "VerificationReport",
]
