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
from grapharrow.domain import DatasetFile, SnapshotManifest
from grapharrow.use_cases.ana_edge_statistics import EdgePropertyStatisticsCache
from grapharrow.use_cases.ana_resources import retained_size

# Bump this when the format or meaning of the observed schema changes.
_FORMAT_VERSION = 2
_SCHEMA_FIELDS = {
    "labels",
    "relationship_types",
    "node_properties",
    "relationship_properties",
    "label_relationships",
}


def _digest(value: object) -> str:
    return sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _files_key(files: tuple[DatasetFile, ...]) -> str:
    return _digest(
        [
            (file.kind, file.path, file.row_count, file.checksum_algorithm, file.checksum)
            for file in files
        ]
    )


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
    """Share active schemas and optionally retain one bounded recent schema."""

    def __init__(
        self,
        disk: tuple[Path, FileSystemAdapter] | None = None,
        *,
        edge_stats_max_bytes: int = 0,
        memory_max_bytes: int = 0,
    ) -> None:
        self._disk = disk
        edge_disk = (disk[0].with_name("ana-edge-schema.json"), disk[1]) if disk else None
        schema_bytes = memory_max_bytes // 2
        self.edge_statistics = EdgePropertyStatisticsCache(
            edge_disk,
            edge_stats_max_bytes if edge_disk is not None else memory_max_bytes - schema_bytes,
            retain_in_memory=memory_max_bytes > 0,
        )
        self._entries: WeakValueDictionary[SnapshotManifest, SnapshotSchema] = WeakValueDictionary()
        self._recent_max_bytes = schema_bytes
        self._recent: SnapshotSchema | None = None
        self._lock = Lock()

    def get_or_create(
        self,
        snapshot: SnapshotManifest,
        discover: Callable[[list[dict[str, Any]] | None], dict[str, Any]],
    ) -> SnapshotSchema:
        with self._lock:
            entry = self._entries.get(snapshot)
            if entry is None:
                schema = self._load(snapshot, discover)
                entry = SnapshotSchema(snapshot=snapshot, schema=_compact_schema(schema))
                self._entries[snapshot] = entry
                if self._recent_max_bytes and retained_size(entry) <= self._recent_max_bytes:
                    self._recent = entry
            return entry

    def _load(
        self,
        snapshot: SnapshotManifest,
        discover: Callable[[list[dict[str, Any]] | None], dict[str, Any]],
    ) -> dict[str, Any]:
        if self._disk is None:
            return discover(None)
        if any(
            not file.checksum or not file.checksum_algorithm
            for file in (*snapshot.nodes, *snapshot.edges)
        ):
            return discover(None)
        nodes_key = _files_key(snapshot.nodes)
        edges_key = _files_key(snapshot.edges)
        path, filesystem = self._disk
        node_properties = None
        with suppress(OSError, ValueError, RecursionError):
            cached = filesystem.read_json(path)
            if (
                isinstance(cached, dict)
                and cached.get("version") == _FORMAT_VERSION
                and cached.get("nodes_key") == nodes_key
            ):
                schema = cached.get("schema")
                cached_edges_key = cached.get("edges_key")
                if (
                    isinstance(schema, dict)
                    and schema.keys() == _SCHEMA_FIELDS
                    and all(isinstance(value, list) for value in schema.values())
                    and isinstance(cached_edges_key, str)
                    and cached.get("checksum") == _digest((nodes_key, cached_edges_key, schema))
                ):
                    if cached_edges_key == edges_key:
                        return schema
                    node_properties = schema["node_properties"]

        schema = discover(node_properties)
        # Independent readers can replace this file concurrently. Every cache hit
        # verifies its key, even when an older workflow was the last to write.
        with suppress(OSError, ValueError):
            filesystem.replace_json(
                path,
                {
                    "version": _FORMAT_VERSION,
                    "nodes_key": nodes_key,
                    "edges_key": edges_key,
                    "schema": schema,
                    "checksum": _digest((nodes_key, edges_key, schema)),
                },
            )
        return schema
