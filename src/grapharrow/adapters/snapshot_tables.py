"""Bounded sharing of immutable Arrow tables across active readers."""

from __future__ import annotations

import sys
from collections import OrderedDict
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from threading import RLock
from typing import Literal, TypeAlias

import pyarrow as pa  # type: ignore[import-untyped]

from grapharrow.domain import DatasetFile

_TableKey: TypeAlias = tuple[
    Literal[
        "nodes",
        "edges",
        "node_properties",
        "edge_properties",
        "edge_endpoints",
        "edge_range",
        "edge_range_properties",
    ],
    tuple[DatasetFile, ...],
]
_Buffer: TypeAlias = tuple[int, int]


@dataclass(slots=True)
class _Entry:
    table: pa.Table
    overhead: int
    buffers: set[_Buffer]
    readers: int


class SnapshotTableCache:
    """Share published table buffers and protect them from eviction while borrowed."""

    def __init__(self, max_bytes: int) -> None:
        self.max_bytes = max_bytes
        self.used_bytes = 0
        self._entries: OrderedDict[_TableKey, _Entry] = OrderedDict()
        self._buffer_refs: dict[_Buffer, int] = {}
        # A logical snapshot table can be assembled from separately cached immutable ranges.
        self._lock = RLock()

    @contextmanager
    def borrow(self, key: _TableKey, load: Callable[[], pa.Table]) -> Iterator[pa.Table]:
        """Read a table once when its buffers can be retained within the budget."""
        if not self.max_bytes or not key[1]:
            yield load()
            return

        with self._lock:
            entry = self._entries.get(key)
            if entry is not None:
                self._entries.move_to_end(key)
                entry.readers += 1
                table = entry.table
            else:
                table = load()
                buffers = {
                    (buffer.address, buffer.size)
                    for column in table.columns
                    for chunk in column.chunks
                    for buffer in chunk.buffers()
                    if buffer is not None and buffer.size
                }
                overhead = 1024 + sum(
                    sys.getsizeof(item) + sys.getsizeof(item.path) + 128 for item in key[1]
                )
                # Array.buffers() can omit data such as dictionary values. Charge
                # such tables in full when Arrow reports a different buffer total.
                total = table.get_total_buffer_size()
                if sum(size for _, size in buffers) != total:
                    buffers.clear()
                    overhead += total
                overhead += 160 * len(buffers)
                if self._reserve(overhead, buffers):
                    entry = _Entry(table, overhead, buffers, readers=1)
                    self._entries[key] = entry
                    self.used_bytes += overhead
                    for buffer in buffers:
                        references = self._buffer_refs.get(buffer, 0)
                        if not references:
                            self.used_bytes += buffer[1]
                        self._buffer_refs[buffer] = references + 1
        try:
            yield table
        finally:
            if entry is not None:
                with self._lock:
                    entry.readers -= 1

    def _reserve(self, overhead: int, buffers: set[_Buffer]) -> bool:
        new_bytes = sum(
            size for address, size in buffers if (address, size) not in self._buffer_refs
        )
        required = self.used_bytes + overhead + new_bytes - self.max_bytes
        if required <= 0:
            return True

        # Plan evictions first so a table above the available budget leaves the cache intact.
        evictions: list[_TableKey] = []
        released: dict[_Buffer, int] = {}
        for key, entry in self._entries.items():
            if not entry.readers:
                evictions.append(key)
                required -= entry.overhead
                for buffer in entry.buffers:
                    if buffer not in buffers:
                        released[buffer] = released.get(buffer, 0) + 1
                        if released[buffer] == self._buffer_refs[buffer]:
                            required -= buffer[1]
                if required <= 0:
                    for evict in evictions:
                        self._evict(evict)
                    return True
        return False

    def _evict(self, key: _TableKey) -> None:
        entry = self._entries.pop(key)
        self.used_bytes -= entry.overhead
        for buffer in entry.buffers:
            if self._buffer_refs[buffer] == 1:
                del self._buffer_refs[buffer]
                self.used_bytes -= buffer[1]
            else:
                self._buffer_refs[buffer] -= 1
