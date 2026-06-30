"""JSON-backed repository for immutable catalog versions."""

from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any, cast

from grapharrow.adapters import FileSystemAdapter
from grapharrow.core import DatabaseLayout
from grapharrow.core.errors import CatalogError
from grapharrow.domain import (
    CatalogVersion,
    LabelDefinition,
    LabelId,
    RelationshipTypeDefinition,
    RelationshipTypeId,
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


class CatalogRepository:
    """Read and write immutable catalog versions on disk."""

    def __init__(
        self,
        layout: DatabaseLayout,
        filesystem: FileSystemAdapter | None = None,
    ) -> None:
        """Describe special object lifecycle or protocol behavior for this type."""
        self._layout = layout
        self._filesystem = filesystem or FileSystemAdapter()

    @property
    def layout(self) -> DatabaseLayout:
        """Return the underlying database layout."""

        return self._layout

    def exists(self) -> bool:
        """Report whether a current catalog pointer exists."""

        return self._filesystem.exists(self._layout.current_catalog_path)

    def write(self, catalog: VersionedCatalog) -> str | Path:
        """Persist ``catalog`` and update the current catalog pointer."""

        self._layout.ensure_base_directories()

        catalog_path = self._catalog_version_path(catalog.version)
        payload = self._serialize_catalog(catalog)
        self._filesystem.write_json(catalog_path, payload)

        pointer = {
            "version": catalog.version.value,
            "path": self._layout.relative_path(catalog_path),
        }
        self._filesystem.write_json(self._layout.current_catalog_path, pointer)
        return catalog_path

    def load(self, version: CatalogVersion | None = None) -> VersionedCatalog:
        """Load the current catalog or a specific catalog version."""

        catalog_path = self._resolve_catalog_path(version)
        if not self._filesystem.exists(catalog_path):
            raise CatalogError(f"Catalog file does not exist: {catalog_path}")

        payload = cast(dict[str, Any], self._filesystem.read_json(catalog_path))
        return self._deserialize_catalog(payload)

    def bootstrap(self) -> VersionedCatalog:
        """Ensure an empty initial catalog exists and return it."""

        if self.exists():
            return self.load()

        catalog = VersionedCatalog.empty(version=CatalogVersion(1))
        self.write(catalog)
        return catalog

    def _catalog_version_path(self, version: CatalogVersion) -> str | Path:
        """Handle this internal helper step for the surrounding public workflow."""
        if isinstance(self._layout.root, Path):
            catalog_dir = cast(Path, self._layout.catalog_dir)
            return catalog_dir / f"catalog-{version.value:06d}.json"
        return f"{self._layout.catalog_dir}/catalog-{version.value:06d}.json"

    def _resolve_catalog_path(self, version: CatalogVersion | None) -> str | Path:
        """Resolve this reference into the concrete value used later in the flow."""
        if version is not None:
            return self._catalog_version_path(version)

        if not self._filesystem.exists(self._layout.current_catalog_path):
            raise CatalogError("Current catalog pointer does not exist.")

        pointer = cast(
            dict[str, Any],
            self._filesystem.read_json(self._layout.current_catalog_path),
        )
        relative_path = pointer.get("path")
        if not isinstance(relative_path, str):
            raise CatalogError("Current catalog pointer is invalid.")

        if isinstance(self._layout.root, Path):
            return self._layout.root / relative_path
        return f"{str(self._layout.root).rstrip('/')}/{relative_path}"

    def _serialize_catalog(self, catalog: VersionedCatalog) -> dict[str, Any]:
        """Handle this internal helper step for the surrounding public workflow."""
        return {
            "version": catalog.version.value,
            "labels": [self._serialize_label(item) for item in catalog.labels],
            "relationship_types": [
                self._serialize_relationship_type(item) for item in catalog.relationship_types
            ],
            "properties": [asdict(item) for item in catalog.properties],
            "constraints": [asdict(item) for item in catalog.constraints],
            "indexes": [asdict(item) for item in catalog.indexes],
        }

    def _serialize_label(self, definition: LabelDefinition) -> dict[str, Any]:
        """Handle this internal helper step for the surrounding public workflow."""
        return {
            "id": definition.id.value,
            "name": definition.name,
        }

    def _serialize_relationship_type(
        self,
        definition: RelationshipTypeDefinition,
    ) -> dict[str, Any]:
        """Handle this internal helper step for the surrounding public workflow."""
        return {
            "id": definition.id.value,
            "name": definition.name,
        }

    def _deserialize_catalog(self, payload: dict[str, Any]) -> VersionedCatalog:
        """Handle this internal helper step for the surrounding public workflow."""
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
