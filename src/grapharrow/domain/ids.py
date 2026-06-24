"""Strongly typed identifiers used by the domain model."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class IntIdentifier:
    """Base class for integer-backed identifiers."""

    value: int

    def __post_init__(self) -> None:
        if self.value < 0:
            raise ValueError("Identifiers must be non-negative integers.")


@dataclass(frozen=True, slots=True)
class NodeId(IntIdentifier):
    """Internal identifier for a node."""


@dataclass(frozen=True, slots=True)
class EdgeId(IntIdentifier):
    """Internal identifier for an edge."""


@dataclass(frozen=True, slots=True)
class LabelId(IntIdentifier):
    """Identifier assigned to a label definition."""


@dataclass(frozen=True, slots=True)
class RelationshipTypeId(IntIdentifier):
    """Identifier assigned to a relationship type definition."""


@dataclass(frozen=True, slots=True)
class CatalogVersion(IntIdentifier):
    """Identifier for a published catalog version."""


@dataclass(frozen=True, slots=True)
class SnapshotId(IntIdentifier):
    """Identifier for a published snapshot."""
