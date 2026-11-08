"""Reuse discovered schemas while workflows keep the same snapshot alive."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from threading import Lock
from typing import Any
from weakref import WeakValueDictionary

from grapharrow.domain import SnapshotManifest


@dataclass(frozen=True, slots=True, weakref_slot=True)
class SnapshotSchema:
    """Schema data pinned and conservatively charged by each participating workflow."""

    snapshot: SnapshotManifest
    schema: dict[str, Any]


class SnapshotSchemaCache:
    """Keep weak references so the last closing workflow releases its cached entry."""

    def __init__(self) -> None:
        self._entries: WeakValueDictionary[SnapshotManifest, SnapshotSchema] = WeakValueDictionary()
        self._lock = Lock()

    def get_or_create(
        self, snapshot: SnapshotManifest, discover: Callable[[], dict[str, Any]]
    ) -> SnapshotSchema:
        with self._lock:
            entry = self._entries.get(snapshot)
            if entry is None:
                entry = SnapshotSchema(snapshot=snapshot, schema=discover())
                self._entries[snapshot] = entry
            return entry
