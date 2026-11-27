"""Guard optimized reads, bounded execution, and immutable file reuse."""

import gc
import json
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
from grapharrow.core.errors import NotFoundError, QueryError
from grapharrow.core.temporal import Duration
from grapharrow.domain import DatasetFile
from grapharrow.repositories.graph import NodeRecord, _edges_table, _node_property_table
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


def test_persisted_adjacency_order_and_reuse(tmp_path: Path) -> None:
    root = tmp_path / "typed-adjacency"
    app = build_application(root)
    graph = app.use_cases.graph
    _, nodes = graph.create_nodes_bulk(
        table=pa.table({"name": ["A", "B", "C"]}), default_labels=("Person",)
    )
    a, b, c = (node.node_id for node in nodes)
    _, edges = graph.create_edges_bulk(
        table=pa.table(
            {
                "src_id": [c, b, b, b, b, a],
                "dst_id": [b, a, c, b, c, c],
                "rel_type": ["R", "S", "R", "R", "R", "R"],
            }
        )
    )
    initial = app.repositories.snapshot.load_manifest()

    def by_type(files: tuple[DatasetFile, ...]) -> dict[str, DatasetFile]:
        return {Path(file.path).parent.name: file for file in files}

    checks = (
        (initial.adjacency_by_source, [5, 3, 2, 4, 0], a, c),
        (initial.adjacency_by_dest, [3, 0, 5, 2, 4], b, c),
    )
    for files, order, low, high in checks:
        assert [Path(file.path).parent.name for file in files] == ["R", "S"]
        file = by_type(files)["R"]
        table = app.adapters.parquet.read_table(root / file.path)
        assert table.column_names == ["src_id", "dst_id", "edge_id"]
        assert table.to_pylist() == [
            {"src_id": edges[i].src_id, "dst_id": edges[i].dst_id, "edge_id": edges[i].edge_id}
            for i in order
        ]
        assert (file.min_key, file.max_key, file.row_count) == (low, high, len(order))

    graph.create_edge(src_id=a, dst_id=a, rel_type="R", properties={})
    appended = app.repositories.snapshot.load_manifest()
    assert appended.edge_properties[-1] == initial.edge_properties[-1]
    for old_files, new_files in (
        (initial.adjacency_by_source, appended.adjacency_by_source),
        (initial.adjacency_by_dest, appended.adjacency_by_dest),
    ):
        assert by_type(new_files)["S"] == by_type(old_files)["S"]
        new_r = by_type(new_files)["R"]
        table = app.adapters.parquet.read_table(root / new_r.path)
        assert table["edge_id"][0].as_py() > edges[-1].edge_id
        assert new_r.row_count == 6

    graph.delete_entities_batch(node_ids=(), edge_ids=(edges[1].edge_id,))
    deleted = app.repositories.snapshot.load_manifest()
    assert deleted.adjacency_by_source == (by_type(appended.adjacency_by_source)["R"],)
    assert deleted.adjacency_by_dest == (by_type(appended.adjacency_by_dest)["R"],)
    app.use_cases.admin.vacuum()
    assert app.use_cases.admin.verify(verify_checksums=True).ok


def test_persisted_adjacency_backfills_legacy_snapshot_with_unchanged_edges(tmp_path: Path) -> None:
    root = tmp_path / "legacy-adjacency"
    app = load_grapharrow(root)
    initial = app.repositories.snapshot.load_manifest()
    path = root / app.repositories.snapshot.load_current_pointer().manifest_path
    payload = json.loads(path.read_bytes())
    payload["files"].pop("adjacency_by_dest")
    path.write_text(json.dumps(payload), encoding="utf-8")

    app.use_cases.graph.create_node(labels=("Other",), properties={})
    rebuilt = app.repositories.snapshot.load_manifest()
    assert rebuilt.edges == initial.edges
    assert rebuilt.adjacency_by_source == initial.adjacency_by_source
    for old, new in zip(initial.adjacency_by_dest, rebuilt.adjacency_by_dest, strict=True):
        assert old.path != new.path
        assert (old.row_count, old.min_key, old.max_key) == (
            new.row_count,
            new.min_key,
            new.max_key,
        )
        assert app.adapters.parquet.read_table(root / new.path).equals(
            app.adapters.parquet.read_table(root / old.path), check_metadata=True
        )
    app.use_cases.admin.vacuum()
    assert app.use_cases.admin.verify(verify_checksums=True).ok


@pytest.mark.parametrize("by_reference", [False, True])
def test_single_edge_append_reuses_canonical_buffers(tmp_path: Path, by_reference: bool) -> None:
    app = build_application(tmp_path / "edge-append")
    graph = app.use_cases.graph
    _, nodes = graph.create_nodes_bulk(
        table=pa.table({"name": ["Alice", "Bob"]}), default_labels=("Person",)
    )
    src, dst = (node.node_id for node in nodes)
    graph.create_edge(
        src_id=src,
        dst_id=dst,
        rel_type="KNOWS",
        properties={"elapsed": Duration(1, 2, 3), "weight": 1},
    )
    previous = graph.load_state()
    properties = {"elapsed": Duration(0, 0, 5), "weight": 1.0}

    with (
        patch.object(graph, "load_state", return_value=previous),
        patch.object(app.repositories.graph, "load_edge_table") as reload_edge_table,
        patch("grapharrow.use_cases.graph._edges_table", wraps=_edges_table) as encode,
    ):
        if by_reference:
            published, edge = graph.create_edge_by_reference(
                source_label="Person",
                source_key="name",
                source_value="Alice",
                target_label="Person",
                target_key="name",
                target_value="Bob",
                rel_type="KNOWS",
                properties=properties,
            )
        else:
            published, edge = graph.create_edge(
                src_id=src, dst_id=dst, rel_type="KNOWS", properties=properties
            )

    encode.assert_called_once_with([edge])
    reload_edge_table.assert_not_called()
    before_values = previous.edge_table["properties_json"].chunk(0).buffers()[2]
    after_values = published.edge_table["properties_json"].chunk(0).buffers()[2]
    assert before_values.address == after_values.address
    expected = _edges_table([*previous.edges, edge])
    assert published.edge_table.equals(expected, check_metadata=True)
    assert app.repositories.graph.load_edge_table(published.snapshot.edges).equals(
        expected, check_metadata=True
    )
    assert app.use_cases.admin.verify(verify_checksums=True).ok


def test_set_many_edge_properties_keeps_order_without_fragmenting_columns(tmp_path: Path) -> None:
    from grapharrow.use_cases import GraphTransactionUseCase

    app = build_application(tmp_path / "edge-set")
    graph = app.use_cases.graph
    _, node = graph.create_node(labels=(), properties={})
    _, edges = graph.create_edges_bulk(
        table=pa.table(
            {
                "src_id": [node.node_id] * 300,
                "dst_id": [node.node_id] * 300,
                "rel_type": ["R"] * 300,
                "value": list(range(300)),
            }
        )
    )
    transaction = GraphTransactionUseCase(graph)
    updated = transaction.update_properties_batch(
        node_updates=(),
        edge_updates=tuple(
            (edge.edge_id, "value", -position)
            for position, edge in enumerate(edges)
            if position % 2
        ),
    )
    assert updated.edge_table["properties_json"].num_chunks == 1
    assert updated.edge_table.equals(_edges_table(list(updated.edges)))
    assert [edge.properties["value"] for edge in updated.edges] == [
        -position if position % 2 else position for position in range(300)
    ]
    assert transaction.commit().edge_table is updated.edge_table


def test_delete_many_edges_keeps_order_and_empty_schema(tmp_path: Path) -> None:
    from grapharrow.use_cases import GraphTransactionUseCase

    app = build_application(tmp_path / "bulk-delete")
    graph = app.use_cases.graph
    _, node = graph.create_node(labels=(), properties={})
    _, edges = graph.create_edges_bulk(
        table=pa.table(
            {
                "src_id": [node.node_id] * 600,
                "dst_id": [node.node_id] * 600,
                "rel_type": ["R"] * 600,
                "value": list(range(600)),
            }
        )
    )
    transaction = GraphTransactionUseCase(graph)
    before = transaction.load_state()
    with pytest.raises(NotFoundError, match="Relationship does not exist: 601"):
        transaction.delete_entities_batch(node_ids=(), edge_ids=(edges[0].edge_id, 601))
    assert transaction.load_state() is before
    updated = transaction.delete_entities_batch(
        node_ids=(), edge_ids=tuple(edge.edge_id for edge in edges[::2])
    )
    assert updated.node_table is before.node_table
    assert updated.edge_table.equals(_edges_table(list(edges[1::2])))
    empty = transaction.delete_entities_batch(
        node_ids=(), edge_ids=tuple(edge.edge_id for edge in edges[1::2])
    )
    assert empty.edge_table.num_rows == 0
    assert empty.edge_table.schema.equals(before.edge_table.schema, check_metadata=True)
    assert transaction.commit().edge_table is empty.edge_table
    assert app.use_cases.admin.verify(verify_checksums=True).ok


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
        current = backend._property_table("Item")
        assert current is not None
        for table in [current, pa.table({})]:
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
