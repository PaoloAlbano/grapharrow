"""Guard optimized reads, bounded execution, and immutable file reuse."""

import gc
import random
import weakref
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pyarrow as pa
import pytest

from grapharrow.adapters.property_index import PropertyIndexStore
from grapharrow.adapters.query_cache import CypherParseCache
from grapharrow.app import build_application
from grapharrow.core.errors import QueryError
from grapharrow.repositories.graph import NodeRecord, _node_property_table
from grapharrow.use_cases.ana import GraphArrowAgentBackend
from grapharrow.use_cases.ana_patterns import aggregate_rows, finish_pattern
from tests.ana_support import load_grapharrow


@pytest.mark.parametrize("mode", ["auto", "exact", "iexact", "contains", "prefix", "suffix"])
def test_discovery_projection_matches_canonical_values(tmp_path: Path, mode: str) -> None:
    app = build_application(tmp_path / mode)
    nodes = [
        {"name": "Alpha", "aliases": ["A"], "description": "north"},
        {"name": "alpha", "aliases": ["a"], "description": None},
        {"name": "İstanbul", "description": "south"},
        {"name": "Alpine"},
        {"name": None, "description": "None"},
    ]
    for props in nodes:
        app.use_cases.graph.create_node(labels=("Place",), properties=props)
    with GraphArrowAgentBackend(app) as backend:
        for tool, args in [
            ("entity_resolve", {"label": "Place", "text": "Al", "context_terms": ["north"]}),
            (
                "value_search",
                {"label": "Place", "property": "name", "text": "al", "match_mode": mode},
            ),
            (
                "value_search",
                {"label": "Place", "property": "name", "text": "i̇", "match_mode": mode},
            ),
        ]:
            first = backend.call(tool, args)
            page = backend.call("fetch", {"from": first["handle"]})
            tables, backend._property_tables = backend._property_tables, {}
            try:
                second = backend.call(tool, args)
                expected = backend.call("fetch", {"from": second["handle"]})
            finally:
                backend._property_tables = tables
            assert page == expected
            assert {key: value for key, value in first.items() if key != "handle"} == {
                key: value for key, value in second.items() if key != "handle"
            }


def test_search_retains_mixed_numeric_and_invalid_scalar_semantics(tmp_path: Path) -> None:
    app = build_application(tmp_path / "mixed")
    app.use_cases.graph.publish_records(
        previous_state=app.use_cases.graph.load_state(),
        new_nodes=(
            NodeRecord(0, ("Item",), {"value": 1}),
            NodeRecord(1, ("Item",), {"value": 1.5}),
        ),
        new_edges=(),
    )
    with GraphArrowAgentBackend(app) as backend:
        result = backend.call(
            "value_search",
            {
                "label": "Item",
                "property": "value",
                "text": "1",
                "match_mode": "exact",
                "keep_entities": False,
            },
        )
        assert backend.call("fetch", {"from": result["handle"]})["rows"] == [[1, 0]]
    app.use_cases.graph.create_node(labels=("Item",), properties={"value": [1]})
    with GraphArrowAgentBackend(app) as backend, pytest.raises(ValueError, match="scalar"):
        backend.call("value_search", {"label": "Item", "property": "value", "text": "1"})


def test_aggregates_stream_rows_and_preserve_empty_null_and_distinct_semantics() -> None:
    class Row(dict):
        pass

    live = weakref.WeakValueDictionary()

    def rows():
        for i in range(1000):
            assert len(live) <= 2
            row = Row(x={"group": i % 2, "value": i if i % 5 else None})
            live[i] = row
            yield row

    metrics = [
        {"op": "count", "var": "x", "alias": "count"},
        {"op": "count_distinct", "var": "x", "alias": "distinct"},
        *[
            {"op": op, "var": "x", "property": "value", "alias": op}
            for op in ["sum", "avg", "min", "max"]
        ],
    ]
    result, _ = aggregate_rows(
        rows(), {"group_by": [{"var": "x", "property": "group"}], "metrics": metrics}
    )
    for group in result:
        values = [i for i in range(1000) if i % 2 == group["group"] and i % 5]
        assert group["count"] == 500
        assert group["distinct"] == 401
        assert group["sum"] == sum(values)
        assert group["avg"] == sum(values) / len(values)
        assert (group["min"], group["max"]) == (min(values), max(values))
    empty, _ = aggregate_rows([], {"metrics": metrics})
    assert empty == [{"count": 0, "distinct": 0, "sum": 0, "avg": None, "min": None, "max": None}]


class _Capture:
    def _store(self, rows: Any, **kwargs: Any) -> dict[str, Any]:
        return {"rows": rows}


@pytest.mark.parametrize("directions", [("asc", "desc"), ("desc", "asc")])
def test_top_k_matches_stable_full_sort_with_nulls_and_unprojected_keys(
    directions: tuple[str, str],
) -> None:
    rng = random.Random(42)
    rows = [
        {"x": {"a": rng.choice([None, 0, 1, 2]), "b": rng.randrange(8), "id": i}}
        for i in range(400)
    ]
    args = {
        "select": [{"var": "x", "property": "id", "alias": "id"}],
        "order_by": [
            {"var": "x", "property": "a", "direction": directions[0]},
            {"var": "x", "property": "b", "direction": directions[1]},
        ],
    }
    expected = list(rows)
    for field, direction in reversed(list(zip(["a", "b"], directions, strict=True))):
        expected.sort(
            key=lambda row: (row["x"][field] is None, row["x"][field]), reverse=direction == "desc"
        )
    result = finish_pattern(_Capture(), iter(rows), "x", args | {"limit": 7})
    assert result["rows"] == [{"id": row["x"]["id"]} for row in expected[:7]]


def test_parse_cache_bounds_errors_and_request_parameter_isolation(tmp_path: Path) -> None:
    cache = CypherParseCache(max_entries=2, max_source_bytes=80)
    first = cache.parse("MATCH (n) RETURN n")
    assert cache.parse("MATCH (n) RETURN n") is first
    cache.parse("MATCH (m) RETURN m")
    cache.parse("MATCH (p) RETURN p")
    assert len(cache._entries) <= 2 and cache._source_bytes <= 80
    with pytest.raises(QueryError):
        cache.parse("MATCH (")
    assert "MATCH (" not in cache._entries
    app = load_grapharrow(tmp_path / "params")
    query = "MATCH (p:Person {name: $name}) RETURN p.age AS age"
    with ThreadPoolExecutor(max_workers=4) as pool:
        values = list(
            pool.map(
                lambda name: app.use_cases.cypher.execute(query, {"name": name}).rows[0]["age"],
                ["Alice", "Bob"] * 8,
            )
        )
    assert values == [50, 40] * 8
    disabled = CypherParseCache(0)
    disabled.parse(query)
    assert not disabled._entries


def test_declared_index_shared_by_ana_cypher_and_new_snapshots(tmp_path: Path) -> None:
    app = load_grapharrow(tmp_path / "indexed")
    app.use_cases.indexes.create_index(
        name="person_name",
        kind="range",
        owner_kind="label",
        owner_name="Person",
        property_names=("name",),
    )
    indexes = app.use_cases.graph.property_indexes
    query = "MATCH (p:Person {name: $name}) RETURN p.age AS age ORDER BY age"
    assert app.use_cases.cypher.execute(query, {"name": "Alice"}).rows == ({"age": 50},)
    assert indexes.builds == 1
    with GraphArrowAgentBackend(app) as old:
        result = old.call("node_search", {"label": "Person", "property": "name", "value": "Alice"})
        assert result["matched_count"] == 1 and indexes.hits >= 1
        old.call(
            "value_search",
            {"label": "Person", "property": "name", "text": "Alice", "match_mode": "exact"},
        )
        app.use_cases.graph.create_node(labels=("Person",), properties={"name": "Alice", "age": 99})
        assert (
            old.call("node_search", {"label": "Person", "property": "name", "value": "Alice"})[
                "matched_count"
            ]
            == 1
        )
        assert app.use_cases.cypher.execute(query, {"name": "Alice"}).rows == (
            {"age": 50},
            {"age": 99},
        )
        assert indexes.builds == 2
    app.use_cases.indexes.drop_index(name="person_name")
    builds, hits = indexes.builds, indexes.hits
    assert app.use_cases.cypher.execute(query, {"name": "Nobody"}).rows == ()
    assert (indexes.builds, indexes.hits) == (builds, hits)
    assert indexes.used_bytes <= indexes.max_bytes


def test_index_bounds_eviction_types_and_oversized_fallback() -> None:
    from grapharrow.domain import DatasetFile

    files = (
        DatasetFile(
            path="derived/node_properties/Item/one.parquet", kind="node_properties", row_count=30
        ),
    )
    table = _node_property_table([NodeRecord(i, ("Item",), {"name": str(i)}) for i in range(30)])
    indexes = PropertyIndexStore(20000)
    assert indexes.lookup(files, "name", "4", lambda: table)["node_id"].to_pylist() == [4]
    assert indexes.lookup(files, "name", 4, lambda: table) is None
    for i in range(20):
        new_files = (replace(files[0], path=f"derived/node_properties/Item/{i}.parquet"),)
        indexes.lookup(new_files, "name", "4", lambda: table)
        assert indexes.used_bytes <= indexes.max_bytes
    assert len(indexes._entries) < 20
    assert PropertyIndexStore(100).lookup(files, "name", "4", lambda: table) is None
    disabled = PropertyIndexStore(0)
    assert (
        disabled.lookup(files, "name", "4", lambda: pytest.fail("disabled index loaded data"))
        is None
    )
    gc.collect()


def test_publication_reuses_unaffected_files_and_vacuum_keeps_them(tmp_path: Path) -> None:
    app = load_grapharrow(tmp_path / "reuse")
    before = app.repositories.snapshot.load_manifest()
    writes = []
    original = app.adapters.parquet.write_table

    def record(table: Any, path: Any, **kwargs: Any) -> None:
        writes.append(str(path))
        original(table, path, **kwargs)

    with patch.object(app.adapters.parquet, "write_table", record):
        app.use_cases.graph.create_node(labels=("Other",), properties={"name": "new"})
    after = app.repositories.snapshot.load_manifest()
    assert after.edges == before.edges
    assert after.adjacency_by_source == before.adjacency_by_source
    assert after.adjacency_by_dest == before.adjacency_by_dest
    assert after.edge_properties == before.edge_properties
    assert set(before.node_labels) <= set(after.node_labels)
    assert set(before.node_properties) <= set(after.node_properties)
    assert len(writes) == 3  # Canonical nodes, new label, and its property projection.
    app.use_cases.admin.vacuum()
    assert app.use_cases.admin.verify(verify_checksums=True).ok
    reopened = build_application(tmp_path / "reuse")
    assert reopened.use_cases.cypher.execute("MATCH (p:Person) RETURN count(p) AS n").rows == (
        {"n": 6},
    )
    compacted = reopened.use_cases.admin.compact()
    assert compacted.edges != after.edges
    assert reopened.use_cases.admin.verify(verify_checksums=True).ok


@pytest.mark.parametrize(
    "values",
    [
        [1e16, 1.0, -1e16],
        [1e16, 1, -1e16],
        [0.1] * 1000,
        [2**63, 0.5, 1.5, -1e16],
        [1.0, 2**80, 0.1, -(2**80)],
        [float("inf"), 1.0],
    ],
)
def test_streaming_float_metrics_match_builtin_sum(values: list[Any]) -> None:
    result, _ = aggregate_rows(
        ({"x": {"value": value}} for value in values),
        {
            "metrics": [
                {"var": "x", "property": "value", "op": op, "alias": op} for op in ["sum", "avg"]
            ]
        },
    )
    assert result == [{"sum": sum(values), "avg": sum(values) / len(values)}]


def test_nan_order_preserves_full_stable_sort(tmp_path: Path) -> None:
    app = build_application(tmp_path / "nan")
    app.use_cases.graph.create_nodes_bulk(
        table=pa.table({"name": ["a", "b", "c"], "value": [2.0, float("nan"), 1.0]}),
        default_labels=("Item",),
    )
    args = {
        "select": [{"var": "x", "property": "name"}],
        "order_by": [{"var": "x", "property": "value"}],
        "limit": 1,
    }
    with GraphArrowAgentBackend(app) as backend:
        rows = [{"x": backend._node(node_id)} for node_id in backend._label_ids["Item"]]
        expected = sorted(rows, key=lambda row: row["x"]["properties"]["value"])[0]
        result = finish_pattern(backend, iter(rows), "x", args)
        page = backend.call("fetch", {"from": result["handle"]})
        assert page["rows"] == [[expected["x"]["properties"]["name"]]]


def test_canonical_search_handles_missing_projections_and_reserved_properties(
    tmp_path: Path,
) -> None:
    app = build_application(tmp_path / "reserved")
    app.use_cases.graph.create_node(
        labels=("Item",), properties={"name": "alpha", "node_id": "external"}
    )
    with GraphArrowAgentBackend(app) as backend:
        for table in [backend._property_tables["Item"], pa.table({})]:
            backend._property_tables["Item"] = table
            assert (
                backend.call("entity_resolve", {"label": "Item", "text": "alpha"})["matched_count"]
                == 1
            )
            assert (
                backend.call(
                    "value_search",
                    {"label": "Item", "property": "name", "text": "alpha", "match_mode": "exact"},
                )["matched_count"]
                == 1
            )
            assert (
                backend.call(
                    "node_search", {"label": "Item", "property": "name", "value": "alpha"}
                )["matched_count"]
                == 1
            )


def test_property_updates_keep_topology_and_label_files(tmp_path: Path) -> None:
    app = load_grapharrow(tmp_path / "topology")
    before = app.repositories.snapshot.load_manifest()
    app.use_cases.cypher.execute("MATCH (p:Person {name: 'Alice'}) SET p.age = 51")
    nodes_changed = app.repositories.snapshot.load_manifest()
    assert nodes_changed.node_labels == before.node_labels
    assert nodes_changed.edges == before.edges
    app.use_cases.cypher.execute("MATCH (:Company)-[r:CEO]->(:Person) SET r.since = 2020")
    after = app.repositories.snapshot.load_manifest()
    assert after.adjacency_by_source == nodes_changed.adjacency_by_source
    assert after.adjacency_by_dest == nodes_changed.adjacency_by_dest
    assert after.nodes == nodes_changed.nodes
    app.use_cases.admin.vacuum()
    assert app.use_cases.admin.verify(verify_checksums=True).ok


def test_oversized_index_is_not_rebuilt_on_every_query() -> None:
    from grapharrow.domain import DatasetFile

    files = (
        DatasetFile(
            path="derived/node_properties/Item/one.parquet", kind="node_properties", row_count=100
        ),
    )
    table = _node_property_table(
        [NodeRecord(i, ("Item",), {"name": "x" * 500 + str(i)}) for i in range(100)]
    )
    store = PropertyIndexStore(4096)
    assert store.lookup(files, "name", "x", lambda: table) is None
    assert (
        store.lookup(files, "name", "x", lambda: pytest.fail("oversized index was rebuilt")) is None
    )
    assert store.used_bytes <= store.max_bytes
    assert all(index.table is None for index in store._entries.values())
