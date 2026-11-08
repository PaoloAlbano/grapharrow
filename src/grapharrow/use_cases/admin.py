"""Use cases for bootstrap, verification, and recovery."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from grapharrow.adapters import FileSystemAdapter
from grapharrow.adapters.writer import writer_locked
from grapharrow.core import DatabaseLayout
from grapharrow.domain import (
    CatalogVersion,
    CurrentSnapshotPointer,
    DatasetFile,
    SnapshotId,
    SnapshotManifest,
    VersionedCatalog,
)
from grapharrow.repositories import (
    CatalogRepository,
    GraphRepository,
    SnapshotRepository,
)


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


class DatabaseAdminUseCase:
    """High-level operations built on top of the storage primitives."""

    def __init__(
        self,
        *,
        layout: DatabaseLayout,
        catalog_store: CatalogRepository,
        snapshot_store: SnapshotRepository,
        graph_store: GraphRepository,
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

        if self._catalog_store.exists() and self._snapshot_store.current_exists():
            return InitializationResult(
                catalog=self._catalog_store.load(), snapshot=self._snapshot_store.load_manifest()
            )
        with self._writer.hold():
            self._layout.ensure_base_directories()
            catalog = self._catalog_store.bootstrap()
            snapshot = self._snapshot_store.bootstrap(catalog_version=catalog.version)
            return InitializationResult(catalog=catalog, snapshot=snapshot)

    def verify(self, *, verify_checksums: bool = False) -> VerificationReport:
        """Verify that the current catalog and snapshot are readable and consistent."""

        missing_paths: list[str] = []
        checksum_failures: list[str] = []
        warnings: list[str] = []

        catalog: VersionedCatalog | None = None
        pointer: CurrentSnapshotPointer | None = None
        manifest: SnapshotManifest | None = None

        try:
            catalog = self._catalog_store.load()
        except Exception as exc:  # pragma: no cover - thin error aggregation
            warnings.append(f"Failed to load current catalog: {exc}")

        try:
            pointer = self._snapshot_store.load_current_pointer()
            manifest = self._snapshot_store.load_manifest()
        except Exception as exc:  # pragma: no cover - thin error aggregation
            warnings.append(f"Failed to load current snapshot: {exc}")

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

        if cleanup_orphan_staged and not dry_run:
            for relative_path in orphan_staged_paths:
                absolute_path = self._absolute_location(relative_path)
                if self._filesystem.exists(absolute_path):
                    try:
                        self._filesystem.delete_dir(absolute_path)
                    except Exception:
                        self._filesystem.delete_file(absolute_path)
                removed_paths.append(relative_path)
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

        self.initialize()
        catalog = self._catalog_store.load()
        current_manifest = self._snapshot_store.load_manifest()
        nodes = self._graph_store.load_nodes(current_manifest.nodes)
        edges = self._graph_store.load_edges(current_manifest.edges)

        next_snapshot_id = SnapshotId(current_manifest.snapshot_id.value + 1)
        persisted = self._graph_store.persist_snapshot(
            snapshot_id=next_snapshot_id,
            nodes=nodes,
            edges=edges,
        )
        manifest = SnapshotManifest(
            snapshot_id=next_snapshot_id,
            parent_snapshot_id=current_manifest.snapshot_id,
            commit_txid=next_snapshot_id.value,
            created_at=current_manifest.created_at,
            catalog_version=catalog.version,
            nodes=persisted.nodes,
            edges=persisted.edges,
            node_labels=persisted.node_labels,
            node_properties=persisted.node_properties,
            adjacency_by_source=persisted.adjacency_by_source,
            adjacency_by_dest=persisted.adjacency_by_dest,
            edge_properties=persisted.edge_properties,
        )
        try:
            self._snapshot_store.publish(manifest, staged_relative_path=persisted.staged_dir)
        finally:
            self._graph_store.cleanup_staged(persisted.staged_dir)
        return manifest

    @writer_locked
    def vacuum(self) -> tuple[str, ...]:
        """Remove manifests and data files that are not referenced by the current snapshot."""

        self.initialize()
        current_manifest = self._snapshot_store.load_manifest()
        current_pointer = self._snapshot_store.load_current_pointer()
        keep_paths = {current_pointer.manifest_path, "current_snapshot.json"}
        keep_paths.update(dataset.path for dataset in _iter_manifest_files(current_manifest))

        removed: list[str] = []
        for directory in (
            self._layout.manifests_dir,
            self._layout.nodes_dir,
            self._layout.edges_dir,
            self._layout.node_labels_dir,
            self._layout.node_properties_dir,
            self._layout.adjacency_by_source_dir,
            self._layout.adjacency_by_dest_dir,
            self._layout.edge_properties_dir,
        ):
            if not self._filesystem.exists(directory):
                continue
            suffix = ".json" if directory == self._layout.manifests_dir else ".parquet"
            for path in self._filesystem.list_paths(directory, recursive=True):
                if not str(path).endswith(suffix):
                    continue
                relative = self._layout.relative_path(path)
                if relative in keep_paths:
                    continue
                if self._filesystem.exists(path):
                    self._filesystem.delete_file(path)
                    removed.append(relative)

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


def _compute_sha256(path: str | Path, *, filesystem: FileSystemAdapter) -> str:
    """Compute the SHA-256 checksum used by verification to validate persisted files."""
    import hashlib

    hasher = hashlib.sha256()
    resolved_filesystem, normalized_path = filesystem.resolve(path)
    with resolved_filesystem.open_input_file(normalized_path) as handle:
        for chunk in iter(lambda: handle.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


__all__ = [
    "DatabaseAdminUseCase",
    "InitializationResult",
    "RecoveryReport",
    "VerificationReport",
]
