"""Bounded physical equality indexes over immutable property projections."""

from __future__ import annotations

import sys
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from threading import RLock
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]

from grapharrow.domain import DatasetFile


@dataclass(frozen=True, slots=True)
class _Index:
    table: Any
    positions: dict[Any, tuple[int, ...]]
    size: int


class PropertyIndexStore:
    """Build declared scalar indexes lazily and share them within one application."""

    def __init__(self, max_bytes: int = 32 * 1024 * 1024) -> None:
        self.max_bytes = max_bytes
        self.used_bytes = 0
        self.builds = 0
        self.hits = 0
        self._entries: OrderedDict[tuple[tuple[DatasetFile, ...], str], _Index] = OrderedDict()
        self._lock = RLock()

    def _retain(self, key: tuple[tuple[DatasetFile, ...], str], index: _Index) -> None:
        if index.size > self.max_bytes:
            return
        while self._entries and self.used_bytes + index.size > self.max_bytes:
            _, evicted = self._entries.popitem(last=False)
            self.used_bytes -= evicted.size
        self._entries[key] = index
        self.used_bytes += index.size

    def _skip_oversized(self, key: tuple[tuple[DatasetFile, ...], str]) -> None:
        # Remember a failed build without retaining the oversized Arrow projection.
        size = (
            1024
            + sum(sys.getsizeof(item) + len(item.path.encode("utf-8")) + 256 for item in key[0])
            + sys.getsizeof(key[1])
        )
        self._retain(key, _Index(None, {}, size))

    def lookup(
        self,
        files: tuple[DatasetFile, ...],
        prop: str,
        value: Any,
        load: Callable[[], pa.Table],
    ) -> pa.Table | None:
        if self.max_bytes == 0 or not files or type(value) not in {str, bool, int}:
            return None
        key = (files, prop)
        with self._lock:
            index = self._entries.get(key)
            if index is not None:
                self._entries.move_to_end(key)
                if index.table is None:
                    return None
                self.hits += 1
            else:
                table = load()
                if (
                    prop == "node_id"
                    or prop not in table.column_names
                    or "node_id" not in table.column_names
                    or not pa.types.is_int64(table["node_id"].type)
                    or table.schema.field("node_id").metadata
                ):
                    return None
                field = table.schema.field(prop)
                if (field.metadata or {}).get(b"grapharrow.encoding") != b"native":
                    return None
                valid = (
                    pa.types.is_string(field.type)
                    and type(value) is str
                    or pa.types.is_integer(field.type)
                    and type(value) is int
                    or pa.types.is_boolean(field.type)
                    and type(value) is bool
                )
                if not valid:
                    return None
                # Include Arrow buffer capacity, Python keys/positions, and manifest keys.
                size = table.get_total_buffer_size() + sys.getsizeof(table) + 1024
                size += sum(
                    sys.getsizeof(item) + len(item.path.encode("utf-8")) + 256 for item in files
                )
                if size > self.max_bytes:
                    self._skip_oversized(key)
                    return None
                positions: dict[Any, list[int]] = {}
                for position, scalar in enumerate(table[prop]):
                    item = scalar.as_py()
                    if item is None:
                        continue
                    if item not in positions:
                        positions[item] = []
                        size += sys.getsizeof(item) + 128
                    positions[item].append(position)
                    size += 40
                    if size + sys.getsizeof(positions) > self.max_bytes:
                        self._skip_oversized(key)
                        return None
                size += sys.getsizeof(positions)
                index = _Index(table, {item: tuple(rows) for item, rows in positions.items()}, size)
                if size > self.max_bytes:
                    self._skip_oversized(key)
                    return None
                self._retain(key, index)
                self.builds += 1
            # Do not introduce Python bool/int equality into an Arrow string/integer index.
            dtype = index.table[prop].type
            if not (
                pa.types.is_string(dtype)
                and type(value) is str
                or pa.types.is_integer(dtype)
                and type(value) is int
                or pa.types.is_boolean(dtype)
                and type(value) is bool
            ):
                return None
            rows = index.positions.get(value, ())
            return index.table.take(pa.array(rows, type=pa.int64()))
