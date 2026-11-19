"""Logical node and relationship records shared by storage implementations."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class NodeRecord:
    """Minimal canonical node representation used by the bootstrap engine."""

    node_id: int
    labels: tuple[str, ...]
    properties: dict[str, object]


@dataclass(frozen=True, slots=True)
class EdgeRecord:
    """Minimal canonical edge representation used by the bootstrap engine."""

    edge_id: int
    rel_type: str
    src_id: int
    dst_id: int
    properties: dict[str, object]
