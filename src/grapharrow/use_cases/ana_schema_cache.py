"""Reuse observed schemas for workflows pinned to immutable snapshot data."""

from __future__ import annotations

import json
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from threading import Lock
from typing import Any
from weakref import WeakValueDictionary

from grapharrow.adapters.filesystem import FileSystemAdapter
from grapharrow.domain import SnapshotManifest

# Bump this when the format or meaning of the observed schema changes.
_FORMAT_VERSION = 1
_SCHEMA_FIELDS = {
    "labels",
    "relationship_types",
    "node_properties",
    "relationship_properties",
    "label_relationships",
}


def _digest(value: object) -> str:
    return sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _compact_schema(schema: dict[str, Any]) -> dict[str, Any]:
    """Share repeated strings and use the same layout for fresh and cached schemas."""
    strings: dict[str, str] = {}

    def compact(value: Any) -> Any:
        if isinstance(value, str):
            return strings.setdefault(value, value)
        if isinstance(value, list):
            return [compact(item) for item in value]
        if isinstance(value, dict):
            return {compact(key): compact(item) for key, item in value.items()}
        return value

    return {compact(key): compact(value) for key, value in schema.items()}


@dataclass(frozen=True, slots=True, weakref_slot=True)
class SnapshotSchema:
    """Schema data pinned and conservatively charged by each participating workflow."""

    snapshot: SnapshotManifest
    schema: dict[str, Any]


class SnapshotSchemaCache:
    """Keep weak entries in memory and optionally persist one entry per local database."""

    def __init__(self, disk: tuple[Path, FileSystemAdapter] | None = None) -> None:
        self._disk = disk
        self._entries: WeakValueDictionary[SnapshotManifest, SnapshotSchema] = WeakValueDictionary()
        self._lock = Lock()

    def get_or_create(
        self, snapshot: SnapshotManifest, discover: Callable[[], dict[str, Any]]
    ) -> SnapshotSchema:
        with self._lock:
            entry = self._entries.get(snapshot)
            if entry is None:
                schema = self._load(snapshot, discover)
                entry = SnapshotSchema(snapshot=snapshot, schema=_compact_schema(schema))
                self._entries[snapshot] = entry
            return entry

    def _load(
        self, snapshot: SnapshotManifest, discover: Callable[[], dict[str, Any]]
    ) -> dict[str, Any]:
        if self._disk is None:
            return discover()
        files = (*snapshot.nodes, *snapshot.edges)
        if any(not file.checksum or not file.checksum_algorithm for file in files):
            return discover()
        key = _digest(
            [
                (file.kind, file.path, file.row_count, file.checksum_algorithm, file.checksum)
                for file in files
            ]
        )
        path, filesystem = self._disk
        with suppress(OSError, ValueError, RecursionError):
            cached = filesystem.read_json(path)
            if (
                isinstance(cached, dict)
                and cached.get("version") == _FORMAT_VERSION
                and cached.get("key") == key
            ):
                schema = cached.get("schema")
                if (
                    isinstance(schema, dict)
                    and schema.keys() == _SCHEMA_FIELDS
                    and all(isinstance(value, list) for value in schema.values())
                    and cached.get("checksum") == _digest(schema)
                ):
                    return schema

        schema = discover()
        # Independent readers can replace this file concurrently. Every cache hit
        # verifies its key, even when an older workflow was the last to write.
        with suppress(OSError, ValueError):
            filesystem.replace_json(
                path,
                {
                    "version": _FORMAT_VERSION,
                    "key": key,
                    "schema": schema,
                    "checksum": _digest(schema),
                },
            )
        return schema
