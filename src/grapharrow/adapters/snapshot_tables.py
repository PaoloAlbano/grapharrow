"""Bounded sharing of immutable canonical Arrow tables across active queries."""

from __future__ import annotations

import sys
from collections import OrderedDict
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from threading import Lock
from typing import Literal, TypeAlias

import pyarrow as pa  # type: ignore[import-untyped]

from grapharrow.domain import DatasetFile

_TableKey: TypeAlias = tuple[Literal["nodes", "edges"], tuple[DatasetFile, ...]]


@dataclass(slots=True)
class _Entry:
    table: pa.Table
    size: int
    readers: int


class SnapshotTableCache:
    """Share published table buffers and protect them from eviction while borrowed."""

    def __init__(self, max_bytes: int) -> None:
        self.max_bytes = max_bytes
        self.used_bytes = 0
        self._entries: OrderedDict[_TableKey, _Entry] = OrderedDict()
        self._lock = Lock()

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
                size = (
                    table.get_total_buffer_size()
                    + 1024
                    + sum(sys.getsizeof(item) + sys.getsizeof(item.path) + 128 for item in key[1])
                )
                if self._reserve(size):
                    entry = _Entry(table, size, readers=1)
                    self._entries[key] = entry
                    self.used_bytes += size
        try:
            yield table
        finally:
            if entry is not None:
                with self._lock:
                    entry.readers -= 1

    def _reserve(self, size: int) -> bool:
        required = self.used_bytes + size - self.max_bytes
        if required <= 0:
            return True

        # Plan evictions first so a table above the available budget leaves the cache intact.
        evictions: list[_TableKey] = []
        for key, entry in self._entries.items():
            if not entry.readers:
                evictions.append(key)
                required -= entry.size
                if required <= 0:
                    for evict in evictions:
                        self.used_bytes -= self._entries.pop(evict).size
                    return True
        return False
