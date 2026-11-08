"""Bounded, application-local reuse of read-only Cypher syntax trees."""

from __future__ import annotations

from collections import OrderedDict
from threading import RLock

from grapharrow.adapters.opencypher import parse_cypher
from grapharrow.interfaces.opencypher.generated.vendor.opencypher.CypherParser import CypherParser


class CypherParseCache:
    """Cache syntax only; parameter binding and planning run afresh for each request."""

    def __init__(self, max_entries: int = 128, max_source_bytes: int = 262144) -> None:
        self.max_entries = max_entries
        self.max_source_bytes = max_source_bytes
        self._entries: OrderedDict[str, tuple[CypherParser.OC_CypherContext, int]] = OrderedDict()
        self._source_bytes = 0
        self._lock = RLock()

    def parse(self, query: str) -> CypherParser.OC_CypherContext:
        size = len(query.encode("utf-8"))
        if self.max_entries <= 0 or size > min(16384, self.max_source_bytes):
            return parse_cypher(query)
        with self._lock:
            entry = self._entries.get(query)
            if entry is not None:
                self._entries.move_to_end(query)
                return entry[0]
            tree = parse_cypher(query)
            self._entries[query] = (tree, size)
            self._source_bytes += size
            while (
                len(self._entries) > self.max_entries or self._source_bytes > self.max_source_bytes
            ):
                _, (_, removed_size) = self._entries.popitem(last=False)
                self._source_bytes -= removed_size
            return tree
