"""JSON-backed repository for snapshot manifests and the current pointer."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

from grapharrow.adapters import FileSystemAdapter
from grapharrow.core import DatabaseLayout
from grapharrow.core.errors import StorageError
from grapharrow.domain import CatalogVersion, CurrentSnapshotPointer, DatasetFile, SnapshotId
from grapharrow.domain.snapshot import ChecksumAlgorithm, DatasetKind, SnapshotManifest


class SnapshotRepository:
    """Read and write immutable snapshot manifests on disk."""

    def __init__(
        self,
        layout: DatabaseLayout,
        filesystem: FileSystemAdapter | None = None,
    ) -> None:
        """Bind the layout and filesystem used to manage manifests and the current pointer."""
        self._layout = layout
        self._filesystem = filesystem or FileSystemAdapter()

    @property
    def layout(self) -> DatabaseLayout:
        """Return the underlying database layout."""

        return self._layout

    def current_exists(self) -> bool:
        """Report whether the current snapshot pointer exists."""

        return self._filesystem.exists(self._layout.current_snapshot_path)

    def write_manifest(self, manifest: SnapshotManifest) -> str | Path:
        """Persist a snapshot manifest without publishing it."""

        self._layout.ensure_base_directories()
        self.validate_manifest(manifest)

        manifest_path = self._manifest_path(manifest.snapshot_id)
        payload = self._serialize_manifest(manifest)
        self._filesystem.write_json(manifest_path, payload)
        return manifest_path

    def stage_manifest(
        self,
        manifest: SnapshotManifest,
        *,
        staged_relative_path: str,
    ) -> str | Path:
        """Persist a candidate manifest inside a staged transaction directory."""

        self._layout.ensure_base_directories()
        self.validate_manifest(manifest)
        staged_dir = (
            self._layout.root / staged_relative_path
            if isinstance(self._layout.root, Path)
            else f"{str(self._layout.root).rstrip('/')}/{staged_relative_path}"
        )
        self._filesystem.create_dir(staged_dir)
        manifest_path = (
            staged_dir / "manifest.json"
            if isinstance(staged_dir, Path)
            else f"{staged_dir.rstrip('/')}/manifest.json"
        )
        payload = self._serialize_manifest(manifest)
        self._filesystem.write_json(manifest_path, payload)
        return manifest_path

    def publish(
        self,
        manifest: SnapshotManifest,
        *,
        staged_relative_path: str | None = None,
    ) -> CurrentSnapshotPointer:
        """Persist and publish a snapshot as the current reader-visible state."""

        if staged_relative_path is not None:
            staged_manifest_path = self.stage_manifest(
                manifest,
                staged_relative_path=staged_relative_path,
            )
            manifest_path = self._manifest_path(manifest.snapshot_id)
            self._filesystem.copy_file(staged_manifest_path, manifest_path)
        else:
            manifest_path = self.write_manifest(manifest)
        pointer = CurrentSnapshotPointer(
            snapshot_id=manifest.snapshot_id,
            manifest_path=self._layout.relative_path(manifest_path),
            commit_txid=manifest.commit_txid,
            created_at=manifest.created_at,
        )
        self._filesystem.replace_json(
            self._layout.current_snapshot_path,
            self._serialize_pointer(pointer),
        )
        return pointer

    def load_manifest(self, snapshot_id: SnapshotId | None = None) -> SnapshotManifest:
        """Load a specific snapshot manifest or the current one."""

        manifest_path = self._resolve_manifest_path(snapshot_id)
        if not self._filesystem.exists(manifest_path):
            raise StorageError(f"Snapshot manifest does not exist: {manifest_path}")

        payload = cast(dict[str, Any], self._filesystem.read_json(manifest_path))
        return self._deserialize_manifest(payload)

    def load_current_pointer(self) -> CurrentSnapshotPointer:
        """Load the current snapshot pointer."""

        if not self._filesystem.exists(self._layout.current_snapshot_path):
            raise StorageError("Current snapshot pointer does not exist.")

        payload = cast(
            dict[str, Any],
            self._filesystem.read_json(self._layout.current_snapshot_path),
        )
        return self._deserialize_pointer(payload)

    def bootstrap(self, *, catalog_version: CatalogVersion) -> SnapshotManifest:
        """Ensure an initial empty snapshot exists and return it."""

        if self.current_exists():
            return self.load_manifest()

        manifest = SnapshotManifest.empty(
            snapshot_id=SnapshotId(1),
            catalog_version=catalog_version,
        )
        self.publish(manifest)
        return manifest

    def validate_manifest(self, manifest: SnapshotManifest) -> None:
        """Validate that a manifest is structurally coherent before publication."""

        dataset_kinds = {
            "nodes": manifest.nodes,
            "edges": manifest.edges,
            "node_labels": manifest.node_labels,
            "node_properties": manifest.node_properties,
            "adjacency_by_source": manifest.adjacency_by_source,
            "adjacency_by_dest": manifest.adjacency_by_dest,
            "edge_properties": manifest.edge_properties,
        }
        seen_paths: set[str] = set()
        for expected_kind, files in dataset_kinds.items():
            for dataset_file in files:
                if dataset_file.kind != expected_kind:
                    raise StorageError(
                        f"Manifest dataset kind mismatch for {dataset_file.path}: "
                        f"expected {expected_kind}, got {dataset_file.kind}"
                    )
                if dataset_file.path in seen_paths:
                    raise StorageError(
                        f"Manifest contains duplicate dataset path: {dataset_file.path}"
                    )
                seen_paths.add(dataset_file.path)
                if dataset_file.row_count < 0:
                    raise StorageError(
                        f"Manifest contains a negative row count for {dataset_file.path}."
                    )
                if dataset_file.checksum is not None and dataset_file.checksum_algorithm is None:
                    raise StorageError(
                        f"Manifest contains a checksum without algorithm for {dataset_file.path}."
                    )
                if dataset_file.checksum is None and dataset_file.checksum_algorithm is not None:
                    raise StorageError(
                        "Manifest contains a checksum algorithm without checksum for "
                        f"{dataset_file.path}."
                    )

    def _manifest_path(self, snapshot_id: SnapshotId) -> str | Path:
        """Return the stable manifest filename for a given snapshot id."""
        if isinstance(self._layout.root, Path):
            manifests_dir = cast(Path, self._layout.manifests_dir)
            return manifests_dir / f"snapshot-{snapshot_id.value:06d}.json"
        return f"{self._layout.manifests_dir}/snapshot-{snapshot_id.value:06d}.json"

    def _resolve_manifest_path(self, snapshot_id: SnapshotId | None) -> str | Path:
        """Resolve either an explicit snapshot id or the current pointer to a manifest path."""
        if snapshot_id is not None:
            return self._manifest_path(snapshot_id)

        pointer = self.load_current_pointer()
        if isinstance(self._layout.root, Path):
            return self._layout.root / pointer.manifest_path
        return f"{str(self._layout.root).rstrip('/')}/{pointer.manifest_path}"

    def _serialize_pointer(self, pointer: CurrentSnapshotPointer) -> dict[str, Any]:
        """Serialize the reader-visible current snapshot pointer to JSON."""
        return {
            "snapshot_id": pointer.snapshot_id.value,
            "manifest_path": pointer.manifest_path,
            "commit_txid": pointer.commit_txid,
            "created_at": pointer.created_at,
        }

    def _deserialize_pointer(self, payload: dict[str, Any]) -> CurrentSnapshotPointer:
        """Parse the current snapshot pointer JSON into the typed domain object."""
        try:
            snapshot_id = SnapshotId(int(payload["snapshot_id"]))
            manifest_path = str(payload["manifest_path"])
            commit_txid = int(payload["commit_txid"])
            created_at = str(payload["created_at"])
        except (KeyError, TypeError, ValueError) as exc:
            raise StorageError("Current snapshot pointer is invalid.") from exc

        return CurrentSnapshotPointer(
            snapshot_id=snapshot_id,
            manifest_path=manifest_path,
            commit_txid=commit_txid,
            created_at=created_at,
        )

    def _serialize_manifest(self, manifest: SnapshotManifest) -> dict[str, Any]:
        """Serialize one snapshot manifest into the JSON shape stored on disk."""
        return {
            "snapshot_id": manifest.snapshot_id.value,
            "parent_snapshot_id": (
                manifest.parent_snapshot_id.value
                if manifest.parent_snapshot_id is not None
                else None
            ),
            "commit_txid": manifest.commit_txid,
            "created_at": manifest.created_at,
            "catalog_version": manifest.catalog_version.value,
            "files": {
                "nodes": [self._serialize_dataset_file(item) for item in manifest.nodes],
                "edges": [self._serialize_dataset_file(item) for item in manifest.edges],
                "node_labels": [
                    self._serialize_dataset_file(item) for item in manifest.node_labels
                ],
                "node_properties": [
                    self._serialize_dataset_file(item) for item in manifest.node_properties
                ],
                "adjacency_by_source": [
                    self._serialize_dataset_file(item) for item in manifest.adjacency_by_source
                ],
                "adjacency_by_dest": [
                    self._serialize_dataset_file(item) for item in manifest.adjacency_by_dest
                ],
                "edge_properties": [
                    self._serialize_dataset_file(item) for item in manifest.edge_properties
                ],
            },
        }

    def _serialize_dataset_file(self, dataset_file: DatasetFile) -> dict[str, Any]:
        """Serialize one dataset-file entry embedded inside a snapshot manifest."""
        return {
            "path": dataset_file.path,
            "row_count": dataset_file.row_count,
            "kind": dataset_file.kind,
            "checksum": dataset_file.checksum,
            "checksum_algorithm": dataset_file.checksum_algorithm,
            "min_key": dataset_file.min_key,
            "max_key": dataset_file.max_key,
        }

    def _deserialize_manifest(self, payload: dict[str, Any]) -> SnapshotManifest:
        """Parse manifest JSON into the typed snapshot object used by the rest of the system."""
        try:
            snapshot_id = SnapshotId(int(payload["snapshot_id"]))
            parent_snapshot_id_raw = payload.get("parent_snapshot_id")
            parent_snapshot_id = (
                SnapshotId(int(parent_snapshot_id_raw))
                if parent_snapshot_id_raw is not None
                else None
            )
            commit_txid = int(payload["commit_txid"])
            created_at = str(payload["created_at"])
            catalog_version = CatalogVersion(int(payload["catalog_version"]))
            files = payload.get("files", {})
        except (KeyError, TypeError, ValueError) as exc:
            raise StorageError("Snapshot manifest is invalid.") from exc

        return SnapshotManifest(
            snapshot_id=snapshot_id,
            parent_snapshot_id=parent_snapshot_id,
            commit_txid=commit_txid,
            created_at=created_at,
            catalog_version=catalog_version,
            nodes=self._deserialize_dataset_files(files.get("nodes", [])),
            edges=self._deserialize_dataset_files(files.get("edges", [])),
            node_labels=self._deserialize_dataset_files(files.get("node_labels", [])),
            node_properties=self._deserialize_dataset_files(files.get("node_properties", [])),
            adjacency_by_source=self._deserialize_dataset_files(
                files.get("adjacency_by_source", [])
            ),
            adjacency_by_dest=self._deserialize_dataset_files(files.get("adjacency_by_dest", [])),
            edge_properties=self._deserialize_dataset_files(files.get("edge_properties", [])),
        )

    def _deserialize_dataset_files(self, payload: list[dict[str, Any]]) -> tuple[DatasetFile, ...]:
        """Parse a manifest dataset-file list into typed dataset metadata entries."""
        return tuple(
            DatasetFile(
                path=str(item["path"]),
                row_count=int(item["row_count"]),
                kind=cast(DatasetKind, item["kind"]),
                checksum=(str(item["checksum"]) if item.get("checksum") is not None else None),
                checksum_algorithm=(
                    cast(ChecksumAlgorithm, item["checksum_algorithm"])
                    if item.get("checksum_algorithm") is not None
                    else None
                ),
                min_key=int(item["min_key"]) if item.get("min_key") is not None else None,
                max_key=int(item["max_key"]) if item.get("max_key") is not None else None,
            )
            for item in payload
        )


__all__ = ["SnapshotRepository"]
