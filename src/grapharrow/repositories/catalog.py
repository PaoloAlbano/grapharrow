"""JSON-backed repository for immutable catalog versions."""

from __future__ import annotations

from dataclasses import asdict, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

from grapharrow.adapters import FileSystemAdapter
from grapharrow.adapters.writer import WriterCoordinator, writer_locked
from grapharrow.core import DatabaseLayout
from grapharrow.core.errors import CatalogError, StorageError, WriteConflictError
from grapharrow.domain import (
    CatalogVersion,
    LabelDefinition,
    LabelId,
    RelationshipTypeDefinition,
    RelationshipTypeId,
    SnapshotId,
    SnapshotManifest,
    VersionedCatalog,
)
from grapharrow.domain.catalog import (
    ConstraintDefinition,
    ConstraintKind,
    IndexDefinition,
    IndexKind,
    IndexStatus,
    PropertyDefinition,
    PropertyOwnerKind,
)
from grapharrow.repositories.snapshot import SnapshotRepository


class CatalogRepository:
    """Read and write immutable catalog versions on disk."""

    def __init__(
        self,
        layout: DatabaseLayout,
        filesystem: FileSystemAdapter | None = None,
        snapshot_store: SnapshotRepository | None = None,
    ) -> None:
        """Describe special object lifecycle or protocol behavior for this type."""
        self._layout = layout
        self._filesystem = filesystem or FileSystemAdapter()
        self._snapshots = snapshot_store or SnapshotRepository(layout, self._filesystem)
        self._writer = self._snapshots.writer

    @property
    def writer(self) -> WriterCoordinator:
        """Return the coordinator shared by this database's mutation paths."""
        return self._writer

    @property
    def layout(self) -> DatabaseLayout:
        """Return the underlying database layout."""

        return self._layout

    def stage(self, catalog: VersionedCatalog) -> str:
        """Write an immutable candidate and return its database-relative path."""
        self._writer.ensure_writable()
        path = f"catalog/catalog-{catalog.version.value:06d}-{uuid4().hex}.json"
        self._filesystem.write_json(self._absolute_path(path), serialize_catalog(catalog))
        return path

    def bootstrap_snapshot(self) -> SnapshotManifest:
        """Return a captured snapshot, creating the database atomically on first use."""
        pointer = self._snapshots.try_load_current_pointer()
        if pointer is not None:
            return self._snapshots.load_at(pointer)
        if self._writer.read_only:
            raise StorageError(
                "Initialize the Native database from its writer before opening a reader."
            )
        with self._writer.hold():
            if self._snapshots.try_load_current_pointer() is None:
                if self._filesystem.exists(self._layout.delta_initial_commit_path):
                    raise StorageError(
                        "This database uses the Delta backend. "
                        "Set GRAPHARROW_STORAGE_BACKEND=delta."
                    )
                catalog = VersionedCatalog.empty(version=CatalogVersion(1))
                if self._filesystem.exists(self._layout.current_catalog_path):
                    catalog = self.load()
                initial = SnapshotManifest.empty(
                    snapshot_id=SnapshotId(1), catalog_version=catalog.version
                )
                try:
                    self._snapshots.publish(replace(initial, catalog_path=self.stage(catalog)))
                except WriteConflictError:
                    # A different process installed the initial snapshot.
                    pass
            return self._snapshots.load_manifest()

    @writer_locked
    def write(
        self, catalog: VersionedCatalog, *, base: SnapshotManifest | None = None
    ) -> str | Path:
        """Publish a catalog-only commit using the same head as graph mutations."""
        if base is None and (pointer := self._snapshots.try_load_current_pointer()) is not None:
            base = self._snapshots.load_at(pointer)
        if base is None:
            manifest = SnapshotManifest.empty(
                snapshot_id=SnapshotId(1), catalog_version=catalog.version
            )
        else:
            if catalog.version.value <= base.catalog_version.value:
                if catalog == self.load_for_snapshot(base):
                    return self._absolute_path(self._snapshot_catalog_path(base))
                raise WriteConflictError("Catalog changed. Reload it before writing a new version.")
            next_id = SnapshotId(base.snapshot_id.value + 1)
            manifest = replace(
                base,
                snapshot_id=next_id,
                parent_snapshot_id=base.snapshot_id,
                commit_txid=next_id.value,
                created_at=datetime.now(UTC).isoformat(timespec="seconds"),
                catalog_version=catalog.version,
                idempotency_receipt=None,
            )
        path = self.stage(catalog)
        self._snapshots.publish(replace(manifest, catalog_path=path), base=base)
        return self._absolute_path(path)

    def load_for_snapshot(self, manifest: SnapshotManifest) -> VersionedCatalog:
        """Read the exact catalog selected by a captured graph manifest."""
        catalog = self._read(self._snapshot_catalog_path(manifest))
        if catalog.version != manifest.catalog_version:
            raise CatalogError("Catalog version does not match the snapshot.")
        return catalog

    def load(self, version: CatalogVersion | None = None) -> VersionedCatalog:
        """Load the head catalog or a catalog from published snapshot history."""
        pointer = self._snapshots.try_load_current_pointer()
        if pointer is not None:
            if version is None:
                return self.load_for_snapshot(self._snapshots.load_at(pointer))
            for _, manifest in self._snapshots.history():
                if manifest.catalog_version == version:
                    return self.load_for_snapshot(manifest)
                if manifest.catalog_version.value < version.value:
                    break
            raise CatalogError(f"Catalog version does not exist: {version.value}")
        # A standalone catalog from the legacy layout has no snapshot head.
        if version is not None:
            return self._read(f"catalog/catalog-{version.value:06d}.json")
        try:
            legacy_pointer = cast(
                dict[str, Any], self._filesystem.read_json(self._layout.current_catalog_path)
            )
        except FileNotFoundError as exc:
            raise CatalogError("Current catalog does not exist.") from exc
        relative_path = legacy_pointer.get("path")
        if not isinstance(relative_path, str):
            raise CatalogError("Current catalog pointer is invalid.")
        return self._read(relative_path)

    def _read(self, path: str) -> VersionedCatalog:
        try:
            payload = cast(dict[str, Any], self._filesystem.read_json(self._absolute_path(path)))
        except FileNotFoundError as exc:
            raise CatalogError(f"Catalog file does not exist: {path}") from exc
        return deserialize_catalog(payload)

    def _snapshot_catalog_path(self, manifest: SnapshotManifest) -> str:
        return manifest.catalog_path or f"catalog/catalog-{manifest.catalog_version.value:06d}.json"

    def _absolute_path(self, relative: str) -> str | Path:
        root = self._layout.root
        return root / relative if isinstance(root, Path) else f"{root.rstrip('/')}/{relative}"


def serialize_catalog(catalog: VersionedCatalog) -> dict[str, Any]:
    """Encode a catalog for either storage layout."""
    return {
        "version": catalog.version.value,
        "labels": [_serialize_label(item) for item in catalog.labels],
        "relationship_types": [
            _serialize_relationship_type(item) for item in catalog.relationship_types
        ],
        "properties": [asdict(item) for item in catalog.properties],
        "constraints": [asdict(item) for item in catalog.constraints],
        "indexes": [asdict(item) for item in catalog.indexes],
    }


def _serialize_label(definition: LabelDefinition) -> dict[str, Any]:
    """Handle this internal helper step for the surrounding public workflow."""
    return {
        "id": definition.id.value,
        "name": definition.name,
    }


def _serialize_relationship_type(
    definition: RelationshipTypeDefinition,
) -> dict[str, Any]:
    """Handle this internal helper step for the surrounding public workflow."""
    return {
        "id": definition.id.value,
        "name": definition.name,
    }


def deserialize_catalog(payload: dict[str, Any]) -> VersionedCatalog:
    """Read a versioned catalog from its persisted representation."""
    try:
        version = CatalogVersion(int(payload["version"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise CatalogError("Catalog payload is missing a valid version.") from exc

    return VersionedCatalog(
        version=version,
        labels=tuple(
            LabelDefinition(
                id=LabelId(int(item["id"])),
                name=str(item["name"]),
            )
            for item in payload.get("labels", [])
        ),
        relationship_types=tuple(
            RelationshipTypeDefinition(
                id=RelationshipTypeId(int(item["id"])),
                name=str(item["name"]),
            )
            for item in payload.get("relationship_types", [])
        ),
        properties=tuple(
            PropertyDefinition(
                name=str(item["name"]),
                owner_kind=cast(PropertyOwnerKind, item["owner_kind"]),
                owner_name=str(item["owner_name"]),
                data_type=str(item["data_type"]),
                nullable=bool(item.get("nullable", True)),
                deprecated=bool(item.get("deprecated", False)),
            )
            for item in payload.get("properties", [])
        ),
        constraints=tuple(
            ConstraintDefinition(
                kind=cast(ConstraintKind, item["kind"]),
                owner_kind=cast(PropertyOwnerKind, item["owner_kind"]),
                owner_name=str(item["owner_name"]),
                property_name=item.get("property_name"),
                source_labels=tuple(str(label) for label in item.get("source_labels", [])),
                target_labels=tuple(str(label) for label in item.get("target_labels", [])),
            )
            for item in payload.get("constraints", [])
        ),
        indexes=tuple(
            IndexDefinition(
                name=str(item["name"]),
                kind=cast(IndexKind, item["kind"]),
                owner_kind=cast(PropertyOwnerKind, item["owner_kind"]),
                owner_name=str(item["owner_name"]),
                property_names=tuple(
                    str(property_name) for property_name in item.get("property_names", [])
                ),
                status=cast(IndexStatus, item.get("status", "declared")),
                backend=str(item.get("backend", "metadata")),
                case_sensitive=bool(item.get("case_sensitive", True)),
                options=dict(cast(dict[str, object], item.get("options", {}))),
            )
            for item in payload.get("indexes", [])
        ),
    )


__all__ = ["CatalogRepository"]
