"""Bounded sharing of immutable Arrow tables across active readers."""

from __future__ import annotations

import sys
from collections import OrderedDict
from collections.abc import Callable, Iterator
from concurrent.futures import Future
from contextlib import contextmanager
from dataclasses import dataclass
from threading import Condition, local
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
        "edge_range_group",
        "edge_range_properties",
    ],
    tuple[DatasetFile, ...],
]
_Buffer: TypeAlias = tuple[int, int]
_MAX_CONCURRENT_LOADS = 2


@dataclass(slots=True)
class _Entry:
    table: pa.Table
    overhead: int
    buffers: set[_Buffer]
    readers: int


@dataclass(slots=True)
class _Load:
    result: Future[_Entry]
    readers: int = 1


class SnapshotTableCache:
    """Share published table buffers and protect them from eviction while borrowed."""

    def __init__(self, max_bytes: int) -> None:
        self.max_bytes = max_bytes
        self.used_bytes = 0
        self._entries: OrderedDict[_TableKey, _Entry] = OrderedDict()
        self._buffer_refs: dict[_Buffer, int] = {}
        self._ready = Condition()
        self._loading: dict[_TableKey, _Load] = {}
        self._active_loads = 0
        self._loader = local()

    @contextmanager
    def borrow(self, key: _TableKey, load: Callable[[], pa.Table]) -> Iterator[pa.Table]:
        """Read a table once when its buffers can be retained within the budget."""
        if not self.max_bytes or not key[1]:
            yield load()
            return

        depth: int = getattr(self._loader, "depth", 0)
        with self._ready:
            self._ready.wait_for(
                lambda: (
                    depth > 0
                    or key in self._entries
                    or key in self._loading
                    or self._active_loads < _MAX_CONCURRENT_LOADS
                )
            )
            entry = self._entries.get(key)
            loading = self._loading.get(key)
            owner = False
            if entry is not None:
                self._entries.move_to_end(key)
                entry.readers += 1
            elif loading is not None:
                loading.readers += 1
            else:
                loading = self._loading[key] = _Load(Future())
                owner = True
                if depth == 0:
                    self._active_loads += 1
                self._ready.notify_all()
        if entry is None:
            assert loading is not None
            if owner:
                entry = self._load_entry(key, load, loading, depth)
            else:
                try:
                    entry = loading.result.result()
                except BaseException:
                    with self._ready:
                        if not loading.result.done():
                            loading.readers -= 1
                        elif loading.result.exception() is None:
                            loading.result.result().readers -= 1
                    raise
        try:
            yield entry.table
        finally:
            with self._ready:
                entry.readers -= 1

    def _load_entry(
        self, key: _TableKey, load: Callable[[], pa.Table], loading: _Load, depth: int
    ) -> _Entry:
        # Assembled tables can borrow immutable ranges. Their nested loads
        # share the outer slot, so each assembly can finish under contention.
        self._loader.depth = depth + 1
        try:
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
            # Array.buffers() can omit data such as dictionary values.
            total = table.get_total_buffer_size()
            if sum(size for _, size in buffers) != total:
                buffers.clear()
                overhead += total
            overhead += 160 * len(buffers)
            with self._ready:
                entry = _Entry(table, overhead, buffers, readers=loading.readers)
                if self._reserve(overhead, buffers):
                    self._entries[key] = entry
                    self.used_bytes += overhead
                    for buffer in buffers:
                        references = self._buffer_refs.get(buffer, 0)
                        if not references:
                            self.used_bytes += buffer[1]
                        self._buffer_refs[buffer] = references + 1
                del self._loading[key]
                loading.result.set_result(entry)
                return entry
        except BaseException as exc:
            with self._ready:
                del self._loading[key]
                loading.result.set_exception(exc)
            raise
        finally:
            self._loader.depth = depth
            with self._ready:
                if depth == 0:
                    self._active_loads -= 1
                self._ready.notify_all()

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
