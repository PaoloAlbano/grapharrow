"""Bound workflow-local neutral read results with an explicitly reserved budget."""

from __future__ import annotations

import sys
from collections import OrderedDict
from collections.abc import Callable
from typing import Any

from grapharrow.use_cases.ana_resources import retained_size


class AnaReadCache:
    """Cache immutable IDs and scores under the owning workflow's call lock."""

    __slots__ = ("max_bytes", "_entries", "_value_bytes", "hits", "misses")
    _BASE_BYTES = 512

    def __init__(self, max_bytes: int) -> None:
        if max_bytes < 1024:
            raise ValueError("ANA read-cache capacity must be at least 1024 bytes")
        self.max_bytes = max_bytes
        self._entries: OrderedDict[tuple[Any, ...], tuple[Any, int]] = OrderedDict()
        self._value_bytes = 0
        self.hits = 0
        self.misses = 0

    @property
    def used_bytes(self) -> int:
        """Conservatively include values, keys, entry records, and container capacity."""
        return self._BASE_BYTES + sys.getsizeof(self._entries) + self._value_bytes

    def get_or_create(self, key: tuple[Any, ...], read: Callable[[], Any]) -> Any:
        entry = self._entries.get(key)
        if entry is not None:
            self.hits += 1
            self._entries.move_to_end(key)
            return entry[0]
        self.misses += 1
        value = read()
        charge = retained_size((key, value)) + 128
        replacement = OrderedDict([(key, (value, charge))])
        if self._BASE_BYTES + sys.getsizeof(replacement) + charge > self.max_bytes:
            return value
        self._entries[key] = (value, charge)
        self._value_bytes += charge
        while self.used_bytes > self.max_bytes:
            _, (_, evicted_bytes) = self._entries.popitem(last=False)
            self._value_bytes -= evicted_bytes
            if not self._entries:
                # Discard hash-table capacity retained from an earlier, larger working set.
                self._entries = replacement
                self._value_bytes = charge
                break
        return value

    def clear(self) -> None:
        self._entries.clear()
        self._value_bytes = 0
