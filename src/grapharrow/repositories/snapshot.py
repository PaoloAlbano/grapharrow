"""JSON-backed repository for snapshot manifests and the current pointer."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

from grapharrow.adapters import FileSystemAdapter
from grapharrow.adapters.writer import WriterCoordinator, writer_locked
from grapharrow.core import DatabaseLayout
from grapharrow.core.errors import CommitOutcomeUnknownError, StorageError, WriteConflictError
from grapharrow.domain import CatalogVersion, CurrentSnapshotPointer, DatasetFile, SnapshotId
from grapharrow.domain.snapshot import (
    ChecksumAlgorithm,
    DatasetKind,
    IdempotencyReceipt,
    SnapshotManifest,
)
from grapharrow.repositories.idempotency import IdempotencyIndex


@dataclass(frozen=True, slots=True)
class _SnapshotEntry:
    manifest: SnapshotManifest
    parent_path: str | None
    idempotency_pages: tuple[str, ...]


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
        self._writer = WriterCoordinator(layout.root, self._filesystem)
        self._idempotency = IdempotencyIndex(layout.root, self._filesystem)

    @property
    def writer(self) -> WriterCoordinator:
        """Return the coordinator shared by this database's mutation paths."""
        return self._writer

    @property
    def layout(self) -> DatabaseLayout:
        """Return the underlying database layout."""

        return self._layout

    def current_exists(self) -> bool:
        """Report whether the current snapshot pointer exists."""

        return self._filesystem.exists(self._layout.current_snapshot_path)

    @writer_locked
    def publish(
        self,
        manifest: SnapshotManifest,
        *,
        base: SnapshotManifest | None = None,
    ) -> CurrentSnapshotPointer:
        """Publish a complete immutable snapshot with a conditional head update."""
        self._layout.ensure_base_directories()
        self.validate_manifest(manifest)
        head = self._filesystem.read_pointer(self._layout.current_snapshot_path)
        revision = None
        parent_path = None
        current = None
        if head is not None:
            payload, revision = head
            current = self._deserialize_pointer(cast(dict[str, Any], payload))
            parent_path = current.manifest_path
            if base is None or base.snapshot_id != current.snapshot_id:
                current_manifest = self.load_at(current)
                if manifest == current_manifest:
                    return current
                if base is not None:
                    raise WriteConflictError("Database changed. Retry from the current snapshot.")
                base = current_manifest
            if (
                manifest.snapshot_id.value != current.snapshot_id.value + 1
                or manifest.parent_snapshot_id not in (None, current.snapshot_id)
            ):
                raise WriteConflictError("Database changed. Retry from the current snapshot.")
            manifest = replace(manifest, parent_snapshot_id=current.snapshot_id)
            if manifest.catalog_path is None and manifest.catalog_version == base.catalog_version:
                manifest = replace(manifest, catalog_path=base.catalog_path)
        elif base is not None:
            raise WriteConflictError("The base snapshot is no longer available.")

        relative_manifest = (
            f"manifests/snapshot-{manifest.snapshot_id.value:06d}-{uuid4().hex}.json"
        )
        manifest_path = self._absolute_path(relative_manifest)
        index_root = current.idempotency_index_path if current is not None else None
        index_pages: tuple[str, ...] = ()
        if (receipt := manifest.idempotency_receipt) is not None:
            index_root, index_pages = self._idempotency.stage(
                previous_root=index_root,
                snapshot_id=manifest.snapshot_id,
                key_hash=receipt.key_hash,
                manifest_path=relative_manifest,
                legacy_entries=self._legacy_receipt_entries(current) if current is not None else (),
            )
        payload = self._serialize_manifest(manifest)
        payload["parent_manifest_path"] = parent_path
        if index_pages:
            payload["idempotency_pages"] = list(index_pages)
        self._filesystem.write_json(manifest_path, payload)
        prior_files = set(_dataset_paths(base)) if base is not None else set()
        new_files = [
            self._absolute_path(path)
            for path in _dataset_paths(manifest)
            if path not in prior_files
        ]
        if manifest.catalog_path is not None and (
            base is None or manifest.catalog_path != base.catalog_path
        ):
            new_files.append(self._absolute_path(manifest.catalog_path))
        new_files.extend(self._absolute_path(path) for path in index_pages)
        self._filesystem.sync_commit_files(self._layout.root, [*new_files, manifest_path])
        pointer = CurrentSnapshotPointer(
            snapshot_id=manifest.snapshot_id,
            manifest_path=relative_manifest,
            commit_txid=manifest.commit_txid,
            created_at=manifest.created_at,
            idempotency_index_path=index_root,
        )
        try:
            published = self._filesystem.replace_pointer(
                self._layout.current_snapshot_path, self._serialize_pointer(pointer), revision
            )
        except Exception as exc:
            if self._writer.local_root is not None:
                raise
            # A network error can arrive after the server commits the PUT.
            self._check_publication(pointer, cause=exc)
        else:
            if not published:
                # SDKs may retry a PUT whose response was lost and receive a failed condition.
                self._check_publication(pointer)
        return pointer

    def _check_publication(
        self, candidate: CurrentSnapshotPointer, *, cause: Exception | None = None
    ) -> None:
        try:
            for path, snapshot in self.history():
                if snapshot.snapshot_id == candidate.snapshot_id:
                    if path == candidate.manifest_path:
                        return
                    raise WriteConflictError("Database changed. Retry from the current snapshot.")
                if snapshot.snapshot_id.value < candidate.snapshot_id.value:
                    break
        except WriteConflictError:
            raise
        except Exception as exc:
            if cause is None:
                cause = exc
        message = f"Commit outcome could not be confirmed: {candidate.manifest_path}."
        raise CommitOutcomeUnknownError(message) from cause

    def load_manifest(self, snapshot_id: SnapshotId | None = None) -> SnapshotManifest:
        """Load the head or a published snapshot from its commit ancestry."""
        if snapshot_id is None:
            return self.load_at(self.load_current_pointer())
        for _, manifest in self.history():
            if manifest.snapshot_id == snapshot_id:
                return manifest
            if manifest.snapshot_id.value < snapshot_id.value:
                break
        raise StorageError(f"Snapshot manifest does not exist: {snapshot_id.value}")

    def history(
        self, pointer: CurrentSnapshotPointer | None = None
    ) -> Iterator[tuple[str, SnapshotManifest]]:
        """Follow the published chain, excluding all failed commit attempts."""
        pointer = pointer or self.load_current_pointer()
        for path, entry in self._history_entries(pointer):
            yield path, entry.manifest

    def retained_paths(self, pointer: CurrentSnapshotPointer) -> Iterator[str]:
        """Return all immutable objects retained by the published snapshot chain."""
        for path, entry in self._history_entries(pointer):
            yield path
            yield from entry.idempotency_pages
            yield from _dataset_paths(entry.manifest)

    def idempotency_base(
        self, pointer: CurrentSnapshotPointer, key_hash: str
    ) -> tuple[SnapshotManifest, IdempotencyReceipt | None]:
        """Read a receipt against the exact publication used as the write base."""
        history = self.history(pointer)
        _, base = next(history)
        if (receipt := base.idempotency_receipt) is not None and receipt.key_hash == key_hash:
            return base, receipt
        if pointer.idempotency_index_path is None:
            # Older heads store receipts only in manifests until the first indexed write.
            for _, manifest in history:
                if (receipt := manifest.idempotency_receipt) is not None and (
                    receipt.key_hash == key_hash
                ):
                    return base, receipt
            return base, None

        receipt_path = self._idempotency.lookup(pointer.idempotency_index_path, key_hash)
        if receipt_path is None:
            return base, None
        receipt = self._load_entry(receipt_path).manifest.idempotency_receipt
        if receipt is None or receipt.key_hash != key_hash:
            raise StorageError("The idempotency index does not match the receipt manifest.")
        return base, receipt

    def _legacy_receipt_entries(self, pointer: CurrentSnapshotPointer) -> Iterator[tuple[str, str]]:
        for path, manifest in self.history(pointer):
            if (receipt := manifest.idempotency_receipt) is not None:
                yield receipt.key_hash, path

    def _history_entries(
        self, pointer: CurrentSnapshotPointer
    ) -> Iterator[tuple[str, _SnapshotEntry]]:
        path: str | None = pointer.manifest_path
        expected_id: SnapshotId | None = pointer.snapshot_id
        while path is not None:
            entry = self._load_entry(path)
            manifest, parent = entry.manifest, entry.parent_path
            if manifest.snapshot_id != expected_id:
                raise StorageError("Snapshot ancestry does not match the published head.")
            yield path, entry
            if parent is not None and _is_legacy_manifest_path(parent):
                if not self._filesystem.exists(self._absolute_path(parent)):
                    # Legacy vacuum could prune published ancestors before this format existed.
                    return
            path = parent
            expected_id = manifest.parent_snapshot_id

    def load_at(self, pointer: CurrentSnapshotPointer) -> SnapshotManifest:
        """Load the exact manifest referenced by an already captured pointer."""
        manifest = self._load_entry(pointer.manifest_path).manifest
        if manifest.snapshot_id != pointer.snapshot_id:
            raise StorageError("Snapshot manifest does not match the published head.")
        return manifest

    def _load_entry(self, path: str) -> _SnapshotEntry:
        try:
            payload = cast(dict[str, Any], self._filesystem.read_json(self._absolute_path(path)))
        except FileNotFoundError as exc:
            raise StorageError(f"Snapshot manifest does not exist: {path}") from exc
        manifest = self._deserialize_manifest(payload)
        parent = payload.get("parent_manifest_path")
        if "parent_manifest_path" not in payload and manifest.parent_snapshot_id is not None:
            parent = f"manifests/snapshot-{manifest.parent_snapshot_id.value:06d}.json"
        pages = payload.get("idempotency_pages", [])
        if not isinstance(pages, list) or any(
            not isinstance(page, str) or not page.startswith("idempotency/") for page in pages
        ):
            raise StorageError("Snapshot idempotency pages are invalid.")
        return _SnapshotEntry(manifest, parent, tuple(pages))

    def load_current_pointer(self) -> CurrentSnapshotPointer:
        """Load the current snapshot pointer with a single read."""
        pointer = self.try_load_current_pointer()
        if pointer is None:
            raise StorageError("Current snapshot pointer does not exist.")
        return pointer

    def try_load_current_pointer(self) -> CurrentSnapshotPointer | None:
        """Return the current pointer, or None if the database has no head yet."""
        head = self._filesystem.read_pointer(self._layout.current_snapshot_path)
        if head is None:
            return None
        payload, _ = head
        return self._deserialize_pointer(cast(dict[str, Any], payload))

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

    def _absolute_path(self, relative: str) -> str | Path:
        root = self._layout.root
        return root / relative if isinstance(root, Path) else f"{root.rstrip('/')}/{relative}"

    def _serialize_pointer(self, pointer: CurrentSnapshotPointer) -> dict[str, Any]:
        """Serialize the reader-visible current snapshot pointer to JSON."""
        payload: dict[str, Any] = {
            "snapshot_id": pointer.snapshot_id.value,
            "manifest_path": pointer.manifest_path,
            "commit_txid": pointer.commit_txid,
            "created_at": pointer.created_at,
        }
        if pointer.idempotency_index_path is not None:
            payload["idempotency_index_path"] = pointer.idempotency_index_path
        return payload

    def _deserialize_pointer(self, payload: dict[str, Any]) -> CurrentSnapshotPointer:
        """Parse the current snapshot pointer JSON into the typed domain object."""
        try:
            snapshot_id = SnapshotId(int(payload["snapshot_id"]))
            manifest_path = str(payload["manifest_path"])
            commit_txid = int(payload["commit_txid"])
            created_at = str(payload["created_at"])
            index_path = payload.get("idempotency_index_path")
            if index_path is not None and not isinstance(index_path, str):
                raise TypeError("Expected a path for the idempotency index.")
        except (KeyError, TypeError, ValueError) as exc:
            raise StorageError("Current snapshot pointer is invalid.") from exc

        return CurrentSnapshotPointer(
            snapshot_id=snapshot_id,
            manifest_path=manifest_path,
            commit_txid=commit_txid,
            created_at=created_at,
            idempotency_index_path=index_path,
        )

    def _serialize_manifest(self, manifest: SnapshotManifest) -> dict[str, Any]:
        """Serialize one snapshot manifest into the JSON shape stored on disk."""
        payload: dict[str, Any] = {
            "snapshot_id": manifest.snapshot_id.value,
            "parent_snapshot_id": (
                manifest.parent_snapshot_id.value
                if manifest.parent_snapshot_id is not None
                else None
            ),
            "commit_txid": manifest.commit_txid,
            "created_at": manifest.created_at,
            "catalog_version": manifest.catalog_version.value,
            "catalog_path": manifest.catalog_path,
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
        if (receipt := manifest.idempotency_receipt) is not None:
            payload["idempotency_receipt"] = {
                "key_hash": receipt.key_hash,
                "request_hash": receipt.request_hash,
                "response_json": receipt.response_json,
            }
        return payload

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
            receipt = _deserialize_idempotency_receipt(payload.get("idempotency_receipt"))
        except (KeyError, TypeError, ValueError) as exc:
            raise StorageError("Snapshot manifest is invalid.") from exc

        return SnapshotManifest(
            snapshot_id=snapshot_id,
            parent_snapshot_id=parent_snapshot_id,
            commit_txid=commit_txid,
            created_at=created_at,
            catalog_version=catalog_version,
            catalog_path=payload.get("catalog_path"),
            idempotency_receipt=receipt,
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


def _deserialize_idempotency_receipt(payload: object) -> IdempotencyReceipt | None:
    if payload is None:
        return None
    if not isinstance(payload, dict):
        raise TypeError("Expected an idempotency receipt object.")
    key_hash = payload["key_hash"]
    request_hash = payload["request_hash"]
    response_json = payload["response_json"]
    if not all(isinstance(value, str) for value in (key_hash, request_hash, response_json)):
        raise TypeError("Expected string fields in the idempotency receipt.")
    return IdempotencyReceipt(key_hash, request_hash, response_json)


def _dataset_paths(manifest: SnapshotManifest) -> Iterator[str]:
    for files in (
        manifest.nodes,
        manifest.edges,
        manifest.node_labels,
        manifest.node_properties,
        manifest.adjacency_by_source,
        manifest.adjacency_by_dest,
        manifest.edge_properties,
    ):
        yield from (file.path for file in files)


def _is_legacy_manifest_path(relative: str) -> bool:
    return Path(relative).stem.count("-") == 1


__all__ = ["SnapshotRepository"]
