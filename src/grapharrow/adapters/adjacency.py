"""Compact snapshot adjacency shared by graph query frontends."""

from __future__ import annotations

import sys
from bisect import bisect_left
from collections import OrderedDict
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from threading import RLock
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.compute as pc  # type: ignore[import-untyped]

from grapharrow.domain import DatasetFile

_DECODE_ROWS = 256


@dataclass(frozen=True, slots=True)
class _Direction:
    table: pa.Table
    ranges: dict[str, tuple[int, int]]
    nodes: memoryview
    offsets: memoryview

    def positions(self, rel_type: str, node_id: int) -> pa.Array:
        start, end = self.ranges.get(rel_type, (0, 0))
        left = bisect_left(self.nodes, node_id, start, end)
        values = self.table["positions"].chunk(0).values
        if left == end or self.nodes[left] != node_id:
            return values.slice(0, 0)
        return values.slice(self.offsets[left], self.offsets[left + 1] - self.offsets[left])

    def groups(self, rel_type: str, node_ids: list[int]) -> pa.Array:
        """Resolve a frontier in Arrow rather than dispatching one lookup per node."""
        start, end = self.ranges.get(rel_type, (0, 0))
        typed = self.table.slice(start, end - start)
        locations = pc.index_in(pa.array(node_ids, type=pa.int64()), value_set=typed["node_id"])
        result = pc.take(typed["positions"], locations)
        return result.chunk(0) if result.num_chunks == 1 else result.combine_chunks()


@dataclass(slots=True)
class GraphAdjacency:
    """Lazily build immutable position lists for a canonical snapshot edge table.

    Only endpoint IDs and canonical row positions are retained. Entity payloads,
    handle ownership and relationship properties remain with the caller.
    A build lock makes shared construction single-flight and retryable.
    """

    outgoing: _Direction | None = None
    incoming: _Direction | None = None
    reserved_bytes: int = 0
    build_count: int = 0
    _lock: Any = field(default_factory=RLock, repr=False, compare=False)

    @staticmethod
    def reservation(
        rows: int, rel_types: Iterable[str], max_bytes: int, *, node_count: int | None = None
    ) -> int:
        # Each direction stores one int64 position per edge, plus an int64 key
        # and list offset per distinct (type, endpoint), with bitmap/wrapper slack.
        types = tuple(rel_types)
        groups = rows if node_count is None else min(rows, node_count * len(types))
        size = rows * 17 + groups * 33 + 8192 + sum(2 * sys.getsizeof(name) + 512 for name in types)
        return size if rows and size <= max_bytes else 0

    def _prepare(self, edges: pa.Table) -> tuple[_Direction, _Direction]:
        with self._lock:
            return self._prepare_locked(edges)

    def _prepare_locked(self, edges: pa.Table) -> tuple[_Direction, _Direction]:
        if self.outgoing is None or self.incoming is None:
            positions = pa.array(range(edges.num_rows), type=pa.int64())

            def direction(column: str) -> _Direction:
                keys = pa.table(
                    {"type": edges["rel_type"], "node_id": edges[column], "position": positions}
                )
                order = pc.sort_indices(
                    keys,
                    sort_keys=[
                        ("type", "ascending"),
                        ("node_id", "ascending"),
                        ("position", "ascending"),
                    ],
                )
                ordered = keys.take(order).combine_chunks()
                nodes, types = ordered["node_id"].chunk(0), ordered["type"].chunk(0)
                different = pc.or_(
                    pc.not_equal(nodes.slice(0, len(nodes) - 1), nodes.slice(1)),
                    pc.not_equal(types.slice(0, len(types) - 1), types.slice(1)),
                )
                starts = pa.concat_arrays(
                    [
                        pa.array([0], type=pa.int64()),
                        pc.add(pc.indices_nonzero(different).cast(pa.int64()), 1),
                    ]
                )
                grouped_types = pc.run_end_encode(types.take(starts))
                ranges = {}
                start = 0
                for rel_type, end in zip(
                    grouped_types.values.to_pylist(),
                    grouped_types.run_ends.to_pylist(),
                    strict=True,
                ):
                    ranges[rel_type] = (start, end)
                    start = end
                offsets = pa.concat_arrays([starts, pa.array([edges.num_rows], type=pa.int64())])
                lists = pa.LargeListArray.from_arrays(offsets, ordered["position"].chunk(0))
                table = pa.table({"node_id": nodes.take(starts), "positions": lists})
                node_view = memoryview(table["node_id"].chunk(0).buffers()[1]).cast("q")
                offset_view = memoryview(lists.offsets.buffers()[1]).cast("q")
                return _Direction(table, ranges, node_view, offset_view)

            outgoing, incoming = direction("src_id"), direction("dst_id")
            # Publish only complete structures; an interrupted build is retryable.
            self.outgoing, self.incoming = outgoing, incoming
            self.build_count += 1
        return self.outgoing, self.incoming

    def relationship_types(self, edges: pa.Table) -> tuple[str, ...]:
        outgoing, _ = self._prepare(edges)
        return tuple(outgoing.ranges)

    def select(
        self,
        edges: pa.Table,
        node_ids: list[int],
        rel_types: Iterable[str],
        columns: tuple[str, ...],
    ) -> pa.Table:
        """Gather each candidate edge once; the caller's join restores binding multiplicity."""
        if not node_ids:
            return edges.slice(0, 0)
        outgoing, incoming = self._prepare(edges)
        unique = list(dict.fromkeys(node_ids))
        groups = []
        for rel_type in rel_types:
            for column in columns:
                index = outgoing if column == "src_id" else incoming
                if len(unique) >= 8:
                    groups.append(pc.list_flatten(index.groups(rel_type, unique)))
                else:
                    groups.extend(index.positions(rel_type, node) for node in unique)
        if not groups:
            return edges.slice(0, 0)
        positions = pc.unique(pa.concat_arrays(groups))
        return edges.take(positions.take(pc.sort_indices(positions)))

    def _positions(
        self, edges: pa.Table, rel_type: str, node_id: int, columns: tuple[str, ...]
    ) -> pa.Array:
        outgoing, incoming = self._prepare(edges)
        groups = [
            (outgoing if column == "src_id" else incoming).positions(rel_type, node_id)
            for column in columns
        ]
        if len(groups) == 1:
            return groups[0]
        # A self-loop belongs to both directions, but is emitted once. Sorting
        # canonical positions also restores interleaved incoming/outgoing order.
        positions = pc.unique(pa.concat_arrays(groups))
        return positions.take(pc.sort_indices(positions))

    def rows_for_sources(
        self,
        edges: pa.Table,
        rel_type: str,
        node_ids: list[int],
        columns: tuple[str, ...],
        *,
        properties: bool = False,
    ) -> Iterator[Iterator[dict[str, Any]]]:
        selected = edges if properties else edges.select(["edge_id", "src_id", "dst_id"])

        def rows(positions: pa.Array) -> Iterator[dict[str, Any]]:
            # Gather Arrow buffers once, then bound Python payload decoding. This
            # also avoids a separate Arrow take call for every small row batch.
            for batch in selected.take(positions).to_batches(max_chunksize=_DECODE_ROWS):
                yield from batch.to_pylist()

        for start in range(0, len(node_ids), _DECODE_ROWS):
            sources = node_ids[start : start + _DECODE_ROWS]
            unique = list(dict.fromkeys(sources))
            groups = None
            if len(unique) >= 8 and len(columns) == 1:
                outgoing, incoming = self._prepare(edges)
                index = outgoing if columns[0] == "src_id" else incoming
                groups = index.groups(rel_type, unique)
                lengths = [length or 0 for length in pc.list_value_length(groups).to_pylist()]
                indices = pc.list_flatten(groups)
            else:
                positions = {
                    node: self._positions(edges, rel_type, node, columns) for node in unique
                }
                lengths = [len(group) for group in positions.values()]
                indices = None
            if sum(lengths) <= 4096:
                if indices is None:
                    indices = pa.concat_arrays(list(positions.values()))
                decoded = selected.take(indices).to_pylist()
                buckets = {}
                offset = 0
                for node, length in zip(unique, lengths, strict=True):
                    buckets[node] = decoded[offset : offset + length]
                    offset += length
                for node in sources:
                    yield iter(buckets[node])
            else:
                if groups is not None:
                    offsets = groups.offsets.to_pylist()[:-1]
                    values = groups.values
                    positions = {
                        node: values.slice(offset, length)
                        for node, offset, length in zip(unique, offsets, lengths, strict=True)
                    }
                for node in sources:
                    yield rows(positions[node])

    def targets(
        self,
        edges: pa.Table,
        rel_type: str,
        node_ids: list[int],
        columns: tuple[str, ...],
        allowed: set[int],
    ) -> set[int]:
        """Retain complete branch membership even when visible rows are limited."""
        outgoing, incoming = self._prepare(edges)
        targets: set[int] = set()
        unique_ids = set(node_ids)
        for column in columns:
            index = outgoing if column == "src_id" else incoming
            if len(unique_ids) >= 8:
                indices = pc.list_flatten(index.groups(rel_type, list(unique_ids)))
            else:
                positions = [index.positions(rel_type, node_id) for node_id in unique_ids]
                indices = (
                    pa.concat_arrays(positions) if positions else pa.array([], type=pa.int64())
                )
            if len(indices):
                target = "dst_id" if column == "src_id" else "src_id"
                values = pc.take(edges[target], indices)
                targets.update(set(pc.unique(values).to_pylist()) & allowed)
        return targets


class AdjacencyIndexStore:
    """Bound application-local index retention, keyed by ordered immutable edge files.

    The cache retains positions only, never canonical payload tables. Live ANA
    owners may pin an evicted entry under their own conservative byte quotas.
    """

    def __init__(self, max_bytes: int = 32 * 1024 * 1024) -> None:
        self.max_bytes = max_bytes
        self.used_bytes = 0
        self.hits = 0
        self._entries: OrderedDict[tuple[DatasetFile, ...], tuple[GraphAdjacency | None, int]] = (
            OrderedDict()
        )
        self._lock = RLock()

    def get_or_create(
        self,
        files: tuple[DatasetFile, ...],
        edges: pa.Table,
        *,
        node_count: int,
    ) -> GraphAdjacency | None:
        expected_rows = sum(item.row_count for item in files)
        if not self.max_bytes or not files or not expected_rows or edges.num_rows != expected_rows:
            return None
        with self._lock:
            entry = self._entries.get(files)
            if entry is not None:
                self._entries.move_to_end(files)
                self.hits += 1
                return entry[0]
            key_bytes = 2048 + sum(
                sys.getsizeof(item) + sys.getsizeof(item.path) + sys.getsizeof(item.checksum) + 256
                for item in files
            )
            if expected_rows * 17 + 8192 + key_bytes > self.max_bytes:
                self._retain(files, None, key_bytes)
                return None
            names = pc.unique(edges["rel_type"]).to_pylist()
            reservation = GraphAdjacency.reservation(
                edges.num_rows, names, self.max_bytes, node_count=node_count
            )
            charge = reservation + key_bytes
            if not reservation or charge > self.max_bytes:
                self._retain(files, None, key_bytes)
                return None
            index = GraphAdjacency(reserved_bytes=reservation)
            self._retain(files, index, charge)
            return index

    def _retain(
        self, files: tuple[DatasetFile, ...], index: GraphAdjacency | None, charge: int
    ) -> None:
        if charge > self.max_bytes:
            return
        while self._entries and self.used_bytes + charge > self.max_bytes:
            _, (_, evicted) = self._entries.popitem(last=False)
            self.used_bytes -= evicted
        self._entries[files] = (index, charge)
        self.used_bytes += charge

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()
            self.used_bytes = 0
