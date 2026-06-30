"""Minimal catalog domain model."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from grapharrow.core.errors import CatalogError
from grapharrow.domain.ids import CatalogVersion, LabelId, RelationshipTypeId

PropertyOwnerKind = Literal["label", "relationship_type"]
ConstraintKind = Literal["primary_key", "unique", "required", "endpoint", "nullability"]
IndexKind = Literal["range", "text", "fulltext", "vector", "lookup"]
IndexStatus = Literal["declared", "building", "ready", "stale"]


@dataclass(frozen=True, slots=True)
class LabelDefinition:
    """Catalog entry describing a node label."""

    id: LabelId
    name: str


@dataclass(frozen=True, slots=True)
class RelationshipTypeDefinition:
    """Catalog entry describing a relationship type."""

    id: RelationshipTypeId
    name: str


@dataclass(frozen=True, slots=True)
class PropertyDefinition:
    """Catalog entry describing a logical property."""

    name: str
    owner_kind: PropertyOwnerKind
    owner_name: str
    data_type: str
    nullable: bool = True
    deprecated: bool = False


@dataclass(frozen=True, slots=True)
class ConstraintDefinition:
    """Catalog entry describing a logical constraint."""

    kind: ConstraintKind
    owner_kind: PropertyOwnerKind
    owner_name: str
    property_name: str | None = None
    source_labels: tuple[str, ...] = ()
    target_labels: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class IndexDefinition:
    """Catalog entry describing a logical index declaration."""

    name: str
    kind: IndexKind
    owner_kind: PropertyOwnerKind
    owner_name: str
    property_names: tuple[str, ...] = ()
    status: IndexStatus = "declared"
    backend: str = "metadata"
    case_sensitive: bool = True
    options: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class VersionedCatalog:
    """Immutable, versioned in-memory representation of the catalog."""

    version: CatalogVersion
    labels: tuple[LabelDefinition, ...] = field(default_factory=tuple)
    relationship_types: tuple[RelationshipTypeDefinition, ...] = field(default_factory=tuple)
    properties: tuple[PropertyDefinition, ...] = field(default_factory=tuple)
    constraints: tuple[ConstraintDefinition, ...] = field(default_factory=tuple)
    indexes: tuple[IndexDefinition, ...] = field(default_factory=tuple)

    @classmethod
    def empty(cls, *, version: CatalogVersion) -> VersionedCatalog:
        """Build an empty catalog at ``version``."""

        return cls(version=version)

    def get_label(self, name: str) -> LabelDefinition:
        """Return the label definition matching ``name``."""

        for label in self.labels:
            if label.name == name:
                return label

        raise CatalogError(f"Unknown label: {name}")

    def get_relationship_type(self, name: str) -> RelationshipTypeDefinition:
        """Return the relationship type definition matching ``name``."""

        for relationship_type in self.relationship_types:
            if relationship_type.name == name:
                return relationship_type

        raise CatalogError(f"Unknown relationship type: {name}")

    def properties_for_owner(
        self,
        *,
        owner_kind: PropertyOwnerKind,
        owner_name: str,
    ) -> tuple[PropertyDefinition, ...]:
        """Return logical properties registered for a label or relationship type."""

        return tuple(
            definition
            for definition in self.properties
            if definition.owner_kind == owner_kind and definition.owner_name == owner_name
        )

    def constraints_for_owner(
        self,
        *,
        owner_kind: PropertyOwnerKind,
        owner_name: str,
    ) -> tuple[ConstraintDefinition, ...]:
        """Return logical constraints registered for a label or relationship type."""

        return tuple(
            definition
            for definition in self.constraints
            if definition.owner_kind == owner_kind and definition.owner_name == owner_name
        )

    def indexes_for_owner(
        self,
        *,
        owner_kind: PropertyOwnerKind,
        owner_name: str,
    ) -> tuple[IndexDefinition, ...]:
        """Return logical indexes registered for a label or relationship type."""

        return tuple(
            definition
            for definition in self.indexes
            if definition.owner_kind == owner_kind and definition.owner_name == owner_name
        )

    def get_index(self, name: str) -> IndexDefinition:
        """Return the index definition matching ``name``."""

        for index in self.indexes:
            if index.name == name:
                return index

        raise CatalogError(f"Unknown index: {name}")
