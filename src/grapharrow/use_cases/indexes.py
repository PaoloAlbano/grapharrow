"""Use cases for catalog-backed index declarations."""

from __future__ import annotations

from dataclasses import replace

from grapharrow.adapters.writer import writer_locked
from grapharrow.core.errors import CatalogError
from grapharrow.domain import CatalogVersion, IndexDefinition, IndexKind, PropertyOwnerKind
from grapharrow.repositories import CatalogRepository


class IndexCatalogUseCase:
    """Manage logical index declarations independently from any serving interface."""

    def __init__(self, catalog_store: CatalogRepository) -> None:
        """Bind index operations to the catalog repository used by the database."""

        self._catalog_store = catalog_store
        self._writer = catalog_store.writer

    def list_indexes(self) -> tuple[IndexDefinition, ...]:
        """Return the index declarations stored in the current catalog."""

        return self._catalog_store.bootstrap().indexes

    @writer_locked
    def create_index(
        self,
        *,
        name: str,
        kind: IndexKind,
        owner_kind: PropertyOwnerKind,
        owner_name: str,
        property_names: tuple[str, ...],
        if_not_exists: bool = False,
        case_sensitive: bool = True,
        options: dict[str, object] | None = None,
    ) -> IndexDefinition:
        """Declare an index and persist it by creating the next catalog version."""

        normalized_name = name.strip()
        normalized_owner_name = owner_name.strip()
        normalized_properties = tuple(property_name.strip() for property_name in property_names)
        self._validate_index_request(
            name=normalized_name,
            kind=kind,
            owner_kind=owner_kind,
            owner_name=normalized_owner_name,
            property_names=normalized_properties,
        )

        catalog = self._catalog_store.bootstrap()
        existing = self._find_existing_index(
            catalog.indexes,
            name=normalized_name,
            kind=kind,
            owner_kind=owner_kind,
            owner_name=normalized_owner_name,
            property_names=normalized_properties,
        )
        if existing is not None:
            if if_not_exists:
                return existing
            raise CatalogError(f"Index already exists: {normalized_name}")

        index = IndexDefinition(
            name=normalized_name,
            kind=kind,
            owner_kind=owner_kind,
            owner_name=normalized_owner_name,
            property_names=normalized_properties,
            case_sensitive=case_sensitive,
            options=dict(options or {}),
        )
        next_catalog = replace(
            catalog,
            version=CatalogVersion(catalog.version.value + 1),
            indexes=(*catalog.indexes, index),
        )
        self._catalog_store.write(next_catalog)
        return index

    @writer_locked
    def drop_index(self, *, name: str, if_exists: bool = False) -> IndexDefinition | None:
        """Remove one logical index declaration by name and persist the next catalog version."""

        normalized_name = name.strip()
        if not normalized_name:
            raise CatalogError("Index name cannot be empty.")

        catalog = self._catalog_store.bootstrap()
        removed: IndexDefinition | None = None
        remaining: list[IndexDefinition] = []
        for index in catalog.indexes:
            if index.name == normalized_name:
                removed = index
            else:
                remaining.append(index)

        if removed is None:
            if if_exists:
                return None
            raise CatalogError(f"Unknown index: {normalized_name}")

        next_catalog = replace(
            catalog,
            version=CatalogVersion(catalog.version.value + 1),
            indexes=tuple(remaining),
        )
        self._catalog_store.write(next_catalog)
        return removed

    def _validate_index_request(
        self,
        *,
        name: str,
        kind: IndexKind,
        owner_kind: PropertyOwnerKind,
        owner_name: str,
        property_names: tuple[str, ...],
    ) -> None:
        """Reject index declarations that would make later physical builds ambiguous."""

        if not name:
            raise CatalogError("Index name cannot be empty.")
        if kind not in ("range", "text", "fulltext", "vector", "lookup"):
            raise CatalogError(f"Unsupported index kind: {kind}")
        if owner_kind not in ("label", "relationship_type"):
            raise CatalogError(f"Unsupported index owner kind: {owner_kind}")
        if not owner_name:
            raise CatalogError("Index owner name cannot be empty.")
        if kind != "lookup" and not property_names:
            raise CatalogError("Property indexes require at least one property.")
        if any(not property_name for property_name in property_names):
            raise CatalogError("Index property names cannot be empty.")

    def _find_existing_index(
        self,
        indexes: tuple[IndexDefinition, ...],
        *,
        name: str,
        kind: IndexKind,
        owner_kind: PropertyOwnerKind,
        owner_name: str,
        property_names: tuple[str, ...],
    ) -> IndexDefinition | None:
        """Find conflicting declarations by name or by equivalent index schema."""

        for index in indexes:
            if index.name == name:
                return index
            if (
                index.kind == kind
                and index.owner_kind == owner_kind
                and index.owner_name == owner_name
                and index.property_names == property_names
            ):
                return index
        return None
