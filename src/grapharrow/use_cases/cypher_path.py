"""Ordered path values and their public result representation."""

from dataclasses import dataclass
from typing import Any

from grapharrow.repositories import EdgeRecord, NodeRecord


@dataclass(frozen=True, slots=True)
class PathValue:
    """Entities in traversal order, with each relationship's stored direction."""

    nodes: tuple[NodeRecord, ...]
    relationships: tuple[EdgeRecord, ...]


class PathResult(dict[str, Any]):
    """A mapping for JSON clients that Bolt can encode as a native Path."""
