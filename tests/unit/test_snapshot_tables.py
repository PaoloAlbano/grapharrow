"""Canonical table sharing must preserve snapshot and memory ownership."""

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from typing import Any

import pyarrow as pa
import pytest

from grapharrow.adapters.snapshot_tables import SnapshotTableCache
from grapharrow.app import build_application
from grapharrow.domain import DatasetFile


def test_cypher_shares_snapshot_tables_across_concurrent_matches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = build_application(tmp_path / "graph")
    cypher = app.use_cases.cypher
    cypher.execute("CREATE (:Person {name:'Alice'})-[:KNOWS]->(:Person {name:'Bob'})")
    store = app.repositories.graph
    calls: Counter[str] = Counter()
    node_loader, edge_loader = store.load_node_table, store.load_edge_table

    def load_nodes(files: tuple[DatasetFile, ...]) -> pa.Table:
        calls["nodes"] += 1
        return node_loader(files)

    def load_edges(files: tuple[DatasetFile, ...]) -> pa.Table:
        calls["edges"] += 1
        return edge_loader(files)

    monkeypatch.setattr(store, "load_node_table", load_nodes)
    monkeypatch.setattr(store, "load_edge_table", load_edges)
    barrier = Barrier(8)
    match = "MATCH (a:Person {name:$name})"
    queries = (
        f"{match}-[:KNOWS]->(b:Person) RETURN b.name AS name",
        f"{match} WITH a AS seed MATCH (seed)-[:KNOWS]->(b:Person) RETURN b.name AS name",
    )

    def select(index: int) -> Any:
        barrier.wait(timeout=5)
        return cypher.execute(queries[index % len(queries)], {"name": "Alice"}).rows

    with ThreadPoolExecutor(max_workers=8) as workers:
        assert list(workers.map(select, range(8))) == [({"name": "Bob"},)] * 8
    assert calls == {"nodes": 1, "edges": 1}


def test_borrowed_snapshot_tables_keep_the_published_version(tmp_path: Path) -> None:
    app = build_application(tmp_path / "graph")
    cypher, graph = app.use_cases.cypher, app.use_cases.graph
    cypher.execute("CREATE (:Person {name:'Alice'})")
    snapshot = graph.capture_snapshot()
    with graph.borrow_tabular_state(snapshot=snapshot) as old:
        cypher.execute("CREATE (:Person {name:'Bob'})")
        with graph.borrow_tabular_state() as current:
            assert current.snapshot != old.snapshot
            assert current.node_table.num_rows == 2
        with graph.borrow_tabular_state(snapshot=snapshot) as pinned:
            assert pinned.node_table is old.node_table
            assert pinned.node_table.num_rows == 1


def test_snapshot_table_cache_charges_borrowed_buffers_until_released() -> None:
    original = DatasetFile("original.parquet", 100, "nodes")
    replacement = DatasetFile("replacement.parquet", 100, "nodes")
    first = pa.table({"value": range(100)})
    second = pa.table({"value": range(100, 200)})
    cache = SnapshotTableCache(3000)

    def fail() -> pa.Table:
        raise AssertionError("Expected a retained table")

    with cache.borrow(("nodes", (original,)), lambda: first):
        used = cache.used_bytes
        assert used > first.get_total_buffer_size()
        with cache.borrow(("nodes", (replacement,)), lambda: second) as uncached:
            assert uncached is second
            assert cache.used_bytes == used
        with cache.borrow(("nodes", (original,)), fail) as reused:
            assert reused is first

    with cache.borrow(("nodes", (replacement,)), lambda: second) as cached:
        assert cached is second
    with cache.borrow(("nodes", (replacement,)), fail) as reused:
        assert reused is second


def test_snapshot_table_loader_can_retry_and_disable_caching() -> None:
    files = (DatasetFile("original.parquet", 1, "nodes"),)
    table = pa.table({"value": [1]})
    cache = SnapshotTableCache(3000)

    def fail() -> pa.Table:
        raise OSError("Read failed")

    with pytest.raises(OSError, match="Read failed"), cache.borrow(("nodes", files), fail):
        pytest.fail("The table loader should raise before borrowing")
    assert cache.used_bytes == 0
    with cache.borrow(("nodes", files), lambda: table) as borrowed:
        assert borrowed is table

    disabled = SnapshotTableCache(0)
    with disabled.borrow(("nodes", files), lambda: table):
        pass
    with pytest.raises(OSError, match="Read failed"), disabled.borrow(("nodes", files), fail):
        pytest.fail("The disabled cache should always call the table loader")


def test_snapshot_table_cache_counts_shared_range_buffers_once() -> None:
    left = pa.table({"value": range(1024)})
    right = pa.table({"value": range(1024, 1536)})
    assembled = pa.concat_tables([left, right])
    left_key = ("edge_range", (DatasetFile("left.parquet", 1024, "edges"),))
    right_key = ("edge_range", (DatasetFile("right.parquet", 512, "edges"),))
    assembled_key = ("edges", (DatasetFile("snapshot.parquet", 1536, "edges"),))
    cache = SnapshotTableCache(18_100)

    def fail() -> pa.Table:
        pytest.fail("Expected a retained table")

    with (
        cache.borrow(left_key, lambda: left),
        cache.borrow(right_key, lambda: right),
        cache.borrow(assembled_key, lambda: assembled),
        cache.borrow(assembled_key, fail) as reused,
    ):
        assert reused is assembled
        assert assembled.get_total_buffer_size() < cache.used_bytes <= cache.max_bytes


def test_snapshot_table_cache_only_reclaims_unreferenced_buffers() -> None:
    table = pa.table({"value": range(1024)})
    alias = table.slice(20)
    replacement = pa.table({"value": range(1024, 2048)})
    first_key = ("edges", (DatasetFile("first.parquet", 1024, "edges"),))
    alias_key = ("edges", (DatasetFile("alias.parquet", 1004, "edges"),))
    next_key = ("edges", (DatasetFile("next.parquet", 1024, "edges"),))
    cache = SnapshotTableCache(15_500)

    def fail() -> pa.Table:
        pytest.fail("Expected a retained table")

    with cache.borrow(first_key, lambda: table), cache.borrow(alias_key, lambda: alias):
        pass
    with cache.borrow(alias_key, fail):
        before = cache.used_bytes
        with cache.borrow(next_key, lambda: replacement):
            assert cache.used_bytes == before
        with cache.borrow(first_key, fail):
            pass
    with cache.borrow(next_key, lambda: replacement), cache.borrow(next_key, fail) as reused:
        assert reused is replacement
        assert replacement.get_total_buffer_size() < cache.used_bytes < before


def test_snapshot_table_cache_includes_dictionary_values_in_the_budget() -> None:
    column = pa.DictionaryArray.from_arrays(pa.array([0]), pa.array(["a" * 4096]))
    table = pa.table({"value": column})
    files = (DatasetFile("dictionary.parquet", 1, "nodes"),)
    cache = SnapshotTableCache(3_000)

    with cache.borrow(("nodes", files), lambda: table):
        assert cache.used_bytes == 0
