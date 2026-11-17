"""Reuse ANA relationship property statistics across local snapshots."""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Iterable, Iterator
from contextlib import suppress
from hashlib import sha256
from pathlib import Path
from typing import Any, TypedDict, cast

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.compute as pc  # type: ignore[import-untyped]

from grapharrow.adapters.filesystem import FileSystemAdapter
from grapharrow.core import property_codec
from grapharrow.core.temporal import Duration, Instant

# Bump when the persisted statistics or their meaning changes.
_FORMAT_VERSION = 1
_EDGE_ID_RANGE_SIZE = 4096
_EDGE_COLUMNS = ["edge_id", "rel_type", "properties_json"]


class _PropertyStats(TypedDict):
    relationship: str
    property: str
    types: list[str]
    non_null: int


class _RangeStats(TypedDict):
    counts: dict[str, int]
    properties: list[_PropertyStats]


class _RangeEntry(TypedDict):
    key: str
    stats: _RangeStats


class EdgePropertyStatisticsCache:
    """Persist a bounded set of verified, stable edge ID ranges."""

    def __init__(self, disk: tuple[Path, FileSystemAdapter] | None, max_bytes: int) -> None:
        self._disk = disk
        self._max_bytes = max_bytes

    def discover(self, edges: pa.Table) -> tuple[list[str], list[dict[str, Any]]]:
        if not edges.num_rows:
            return [], []
        if self._disk is None or not self._max_bytes:
            return _summarize((_collect_properties(edges),))
        path, filesystem = self._disk
        return _summarize(self._scan_ranges(edges, path, filesystem))

    def _load(self, path: Path) -> dict[str, _RangeEntry]:
        with suppress(OSError, ValueError, RecursionError):
            with path.open("rb") as handle:
                raw = handle.read(self._max_bytes + 1)
            if len(raw) > self._max_bytes:
                return {}
            payload = json.loads(raw)
            if (
                isinstance(payload, dict)
                and payload.get("version") == _FORMAT_VERSION
                and payload.get("width") == _EDGE_ID_RANGE_SIZE
                and isinstance(ranges := payload.get("ranges"), dict)
                and payload.get("checksum") == _checksum(ranges)
            ):
                return cast(dict[str, _RangeEntry], ranges)
        return {}

    def _scan_ranges(
        self, edges: pa.Table, path: Path, filesystem: FileSystemAdapter
    ) -> Iterator[_RangeStats]:
        previous = self._load(path)
        updated: dict[str, _RangeEntry] = {}
        compact_bytes = 0
        fits = True
        for group, rows in _ranges(edges):
            if not fits and group not in previous:
                yield _collect_properties(rows)
                continue
            key = _canonical_key(rows)
            entry = previous.get(group)
            if entry is None or entry["key"] != key:
                entry = {"key": key, "stats": _collect_properties(rows)}
            if fits:
                # The compact form bounds the retained entries before final serialization.
                compact_bytes += len(json.dumps((group, entry), separators=(",", ":")))
                if compact_bytes <= self._max_bytes:
                    updated[group] = entry
                else:
                    updated.clear()
                    fits = False
            yield entry["stats"]

        if fits and updated != previous:
            payload = {
                "version": _FORMAT_VERSION,
                "width": _EDGE_ID_RANGE_SIZE,
                "ranges": updated,
                "checksum": _checksum(updated),
            }
            # Match replace_json's ASCII serialization when enforcing the byte limit.
            if len(json.dumps(payload, indent=2, sort_keys=True)) <= self._max_bytes:
                with suppress(OSError, ValueError):
                    filesystem.replace_json(path, payload)


def _checksum(ranges: dict[str, _RangeEntry]) -> str:
    raw = json.dumps((_EDGE_ID_RANGE_SIZE, ranges), sort_keys=True, separators=(",", ":"))
    return sha256(raw.encode()).hexdigest()


def _ranges(edges: pa.Table) -> Iterator[tuple[str, pa.Table]]:
    rows = edges.select(_EDGE_COLUMNS)
    identifiers = rows["edge_id"]
    if pc.any(pc.less(identifiers.slice(1), identifiers.slice(0, len(identifiers) - 1))).as_py():
        rows = rows.sort_by([("edge_id", "ascending")])
        identifiers = rows["edge_id"]
    group_ids = pc.divide(pc.subtract(identifiers, 1), _EDGE_ID_RANGE_SIZE)
    boundaries = pc.indices_nonzero(
        pc.not_equal(group_ids.slice(0, len(group_ids) - 1), group_ids.slice(1))
    ).to_pylist()
    start = 0
    for stop in [*(index + 1 for index in boundaries), rows.num_rows]:
        yield str(group_ids[start].as_py()), rows.slice(start, stop - start)
        start = stop


def _canonical_key(rows: pa.Table) -> str:
    # Normalize slices so IPC padding and chunk boundaries cannot incorporate
    # neighboring rows or cause a miss after Parquet data is rewritten.
    table = pa.Table.from_arrays(
        [pa.concat_arrays(column.chunks) for column in rows.columns], schema=rows.schema
    )
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as stream:
        stream.write_table(table)
    return sha256(sink.getvalue()).hexdigest()


def _collect_properties(rows: pa.Table) -> _RangeStats:
    counts: dict[str, int] = defaultdict(int)
    types: dict[tuple[str, str], set[str]] = defaultdict(set)
    non_null: dict[tuple[str, str], int] = defaultdict(int)
    for relationship, raw in zip(
        rows["rel_type"].to_pylist(), rows["properties_json"].to_pylist(), strict=True
    ):
        counts[relationship] += 1
        if raw == "{}":
            continue
        for prop, value in property_codec.loads_for_schema(raw).items():
            types[relationship, prop].add(schema_property_type(value))
            if value is not None:
                non_null[relationship, prop] += 1
    return {
        "counts": dict(counts),
        "properties": [
            {
                "relationship": relationship,
                "property": prop,
                "types": sorted(observed),
                "non_null": non_null[relationship, prop],
            }
            for (relationship, prop), observed in types.items()
        ],
    }


def _summarize(groups: Iterable[_RangeStats]) -> tuple[list[str], list[dict[str, Any]]]:
    counts: dict[str, int] = defaultdict(int)
    types: dict[tuple[str, str], set[str]] = defaultdict(set)
    non_null: dict[tuple[str, str], int] = defaultdict(int)
    for group in groups:
        for relationship, count in group["counts"].items():
            counts[relationship] += count
        for prop in group["properties"]:
            key = prop["relationship"], prop["property"]
            types[key].update(prop["types"])
            non_null[key] += prop["non_null"]
    return sorted(counts), [
        {
            "relType": relationship,
            "propertyName": prop,
            "propertyTypes": sorted(observed),
            "mandatory": non_null[relationship, prop] == counts[relationship],
        }
        for (relationship, prop), observed in sorted(types.items())
    ]


def schema_property_type(value: Any) -> str:
    if isinstance(value, Instant):
        return value.type_name
    if isinstance(value, Duration):
        return "Duration"
    if isinstance(value, list):
        element = schema_property_type(next((item for item in value if item is not None), None))
        return f"List<{element}>"
    return {bool: "Boolean", int: "Integer", float: "Float", str: "String"}.get(type(value), "Any")
