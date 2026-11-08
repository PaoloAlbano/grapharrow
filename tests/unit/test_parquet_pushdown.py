"""Reader predicates must reduce candidate reads without changing query semantics."""

from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest

from grapharrow.adapters import FileSystemAdapter, ParquetAdapter
from grapharrow.adapters.scan_filter import dataset_filter
from grapharrow.app import build_application
from grapharrow.config import GraphArrowSettings
from grapharrow.domain.scan import ScanPredicate


@pytest.mark.parametrize(
    "predicates,expected",
    [
        ((ScanPredicate("number", "=", 3),), [3]),
        ((ScanPredicate("number", ">=", 2), ScanPredicate("number", "<", 4)), [2, 3]),
        ((ScanPredicate("number", "!=", 2),), [1, 3, 4]),
        ((ScanPredicate("number", "in", (1, 4)),), [1, 4]),
        ((ScanPredicate("number", "in", (None, 4)),), [0, 4]),
        ((ScanPredicate("number", "in", ()),), []),
        ((ScanPredicate("number", "is_null"),), [0]),
        ((ScanPredicate("number", "is_not_null"),), [1, 2, 3, 4]),
        ((ScanPredicate("text", "=", "é"),), [3]),
        ((ScanPredicate("flag", "=", True),), [1, 3]),
        ((ScanPredicate("real", ">", 2.5),), [3, 4]),
        ((ScanPredicate("not_present", "=", "x"),), [0, 1, 2, 3, 4]),
        ((ScanPredicate("number", "=", "3"),), [0, 1, 2, 3, 4]),
        ((ScanPredicate("number", "=", True),), [0, 1, 2, 3, 4]),
        ((ScanPredicate("number", "=", 3.0),), [0, 1, 2, 3, 4]),
        ((ScanPredicate("number", "=", 2**80),), [0, 1, 2, 3, 4]),
        ((ScanPredicate("real", "=", float("nan")),), [0, 1, 2, 3, 4]),
        ((ScanPredicate("number", "=", None),), [0, 1, 2, 3, 4]),
    ],
)
def test_reader_scalar_filters_preserve_types_nulls_and_order(
    tmp_path: Path, predicates: tuple[ScanPredicate, ...], expected: list[int]
) -> None:
    table = pa.table(
        {
            "id": list(range(5)),
            "number": [None, 1, 2, 3, 4],
            "text": [None, "a", "b", "é", "e"],
            "flag": [None, True, False, True, False],
            "real": [None, 1.0, 2.0, 3.0, 4.0],
        }
    )
    paths = []
    for i, batch in enumerate(table.to_batches(max_chunksize=2)):
        path = tmp_path / f"part-{i}.parquet"
        pq.write_table(pa.Table.from_batches([batch]), path, row_group_size=1)
        paths.append(path)
    reader = ParquetAdapter()
    result = reader.read_dataset_table(paths, columns=["id"], predicates=predicates)
    assert result["id"].to_pylist() == expected
    reader.filter_pushdown_enabled = False
    assert reader.read_dataset_table(paths, columns=["id"], predicates=predicates)[
        "id"
    ].to_pylist() == list(range(5))


def test_reader_handles_escaped_names_and_skips_row_groups(tmp_path: Path) -> None:
    path = tmp_path / "sorted.parquet"
    table = pa.table(
        {"id": list(range(100)), "`display.name`": [f"name-{i:03d}" for i in range(100)]}
    )
    pq.write_table(table, path, row_group_size=10)
    predicates = (ScanPredicate("display.name", "=", "name-042"),)
    reader = ParquetAdapter()
    result = reader.read_dataset_table([path], columns=["id"], predicates=predicates)
    assert result.to_pylist() == [{"id": 42}]
    dataset = ds.dataset(path, format="parquet")
    expression = dataset_filter(dataset.schema, predicates)
    fragment = next(dataset.get_fragments())
    assert len(fragment.split_by_row_group(filter=expression)) == 1
    assert len(fragment.split_by_row_group()) == 10


def test_scanner_filter_failure_falls_back_to_unfiltered_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "fallback.parquet"
    pq.write_table(pa.table({"id": [1, 2]}), path)
    original = ds.dataset
    calls = []

    class Dataset:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.dataset = original(*args, **kwargs)
            self.schema = self.dataset.schema

        def scanner(self, **kwargs: Any) -> Any:
            calls.append(kwargs.get("filter"))
            if kwargs.get("filter") is not None:
                raise pa.ArrowNotImplementedError("Unsupported scanner filter")
            return self.dataset.scanner(**kwargs)

    monkeypatch.setattr(ds, "dataset", Dataset)
    table = ParquetAdapter().read_dataset_table([path], predicates=(ScanPredicate("id", "=", 1),))
    assert table["id"].to_pylist() == [1, 2]
    assert len(calls) == 2 and calls[0] is not None and calls[1] is None


def test_reader_pushdown_works_through_mounted_object_store_uri(tmp_path: Path) -> None:
    local = pafs.LocalFileSystem()
    filesystem = FileSystemAdapter(
        mounts={"s3://test-bucket": (pafs.SubTreeFileSystem(str(tmp_path), local), "")}
    )
    path = tmp_path / "projection.parquet"
    pq.write_table(pa.table({"id": [1, 2], "value": [10, 20]}), path)
    result = ParquetAdapter(filesystem).read_dataset_table(
        ["s3://test-bucket/projection.parquet"],
        columns=["id"],
        predicates=(ScanPredicate("value", ">=", 20),),
    )
    assert result.to_pylist() == [{"id": 2}]


@pytest.fixture
def graph(tmp_path: Path):
    app = build_application(
        tmp_path / "db", settings=GraphArrowSettings(property_index_max_bytes=0)
    )
    _, nodes = app.use_cases.graph.create_nodes_bulk(
        table=pa.table(
            {
                "name": [f"person-{i:02d}" for i in range(30)],
                "age": [i if i % 5 else None for i in range(30)],
                "country": ["IT" if i % 2 else "FR" for i in range(30)],
                "active": [bool(i % 2) for i in range(30)],
            }
        ),
        default_labels=("Person",),
    )
    app.use_cases.graph.create_edges_bulk(
        table=pa.table(
            {
                "src_id": [node.node_id for node in nodes[:-1]],
                "dst_id": [node.node_id for node in nodes[1:]],
                "rel_type": ["KNOWS"] * 29,
                "weight": list(range(29)),
            }
        )
    )
    return app


@pytest.mark.parametrize("planner", [True, False])
@pytest.mark.parametrize(
    "query,params",
    [
        ("MATCH (p:Person {name: $name}) RETURN p.name AS name", {"name": "person-07"}),
        (
            "MATCH (p:Person) WHERE p.age >= $low AND p.age < $high "
            "RETURN p.name AS name ORDER BY name",
            {"low": 6, "high": 9},
        ),
        ("MATCH (p:Person) WHERE p.age > 999 RETURN p.name AS name ORDER BY name", {}),
        ("MATCH (p:Person) WHERE p.age IN [NULL, 1, 4] RETURN p.name AS name ORDER BY name", {}),
        ("MATCH (p:Person) WHERE p.age IS NULL RETURN p.name AS name ORDER BY name", {}),
        ("MATCH (p:Person) WHERE p.age IS NOT NULL RETURN p.name AS name ORDER BY name", {}),
        ("MATCH (p:Person) WHERE p.active = true RETURN p.name AS name ORDER BY name", {}),
        ("MATCH (p:Person) WHERE p.age < 2 OR p.age > 27 RETURN p.name AS name ORDER BY name", {}),
        ("MATCH (p:Person) WHERE NOT p.age = 1 RETURN p.name AS name ORDER BY name", {}),
        (
            "MATCH (p:Person) WHERE p.age > 6 AND toLower(p.name) CONTAINS '2' "
            "RETURN p.name AS name ORDER BY name",
            {},
        ),
        (
            "MATCH (a:Person {name: 'person-07'}), (b:Person {name: 'person-08'}) "
            "RETURN a.name AS a, b.name AS b",
            {},
        ),
        (
            "MATCH (a:Person {name: 'person-29'}) OPTIONAL MATCH (a)-[r:KNOWS]->(b:Person) "
            "RETURN a.name AS a, b.name AS b",
            {},
        ),
        (
            "MATCH (a:Person)-[r:KNOWS {weight: 7}]->(b:Person) "
            "RETURN a.name AS a, b.name AS b, r.weight AS weight",
            {},
        ),
    ],
)
def test_cypher_pushdown_matches_disabled_reader(
    graph: Any, planner: bool, query: str, params: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    if planner:
        monkeypatch.delenv("GRAPHARROW_DISABLE_CYPHER_PLANNER", raising=False)
    else:
        monkeypatch.setenv("GRAPHARROW_DISABLE_CYPHER_PLANNER", "1")
    graph.adapters.parquet.filter_pushdown_enabled = False
    expected = graph.use_cases.cypher.execute(query, params)
    graph.adapters.parquet.filter_pushdown_enabled = True
    assert graph.use_cases.cypher.execute(query, params) == expected


@pytest.mark.parametrize("planner", [True, False])
def test_cypher_reader_returns_only_candidates_before_residual_filter(
    graph: Any, monkeypatch: pytest.MonkeyPatch, planner: bool
) -> None:
    monkeypatch.setenv("GRAPHARROW_DISABLE_CYPHER_PLANNER", "0" if planner else "1")
    reader = graph.adapters.parquet
    original = reader.read_dataset_table
    reads = []

    def read(paths: Any, **kwargs: Any) -> Any:
        result = original(paths, **kwargs)
        if kwargs.get("predicates"):
            reads.append((kwargs["predicates"], result.num_rows))
        return result

    monkeypatch.setattr(reader, "read_dataset_table", read)
    result = graph.use_cases.cypher.execute(
        "MATCH (p:Person) WHERE p.age >= 6 AND p.age < 9 RETURN p.name AS name ORDER BY name"
    )
    assert result.rows == ({"name": "person-06"}, {"name": "person-07"}, {"name": "person-08"})
    assert reads and all(rows == 3 for _, rows in reads)
    reads.clear()
    assert (
        graph.use_cases.cypher.execute(
            "MATCH (p:Person) WHERE p.age > 999 RETURN p.name AS name ORDER BY name"
        ).rows
        == ()
    )
    assert reads and all(rows == 0 for _, rows in reads)


def test_escaped_import_projections_support_predicates(tmp_path: Path) -> None:
    app = build_application(tmp_path / "escaped")
    app.use_cases.graph.create_node(
        labels=("`Person`",), properties={"`name`": "Alice", "`age`": 30}
    )
    query = "MATCH (p:Person) WHERE p.age >= 30 RETURN p.name AS name"
    app.adapters.parquet.filter_pushdown_enabled = False
    expected = app.use_cases.cypher.execute(query)
    assert expected.rows == ({"name": "Alice"},)
    app.adapters.parquet.filter_pushdown_enabled = True
    assert app.use_cases.cypher.execute(query) == expected


def test_inline_relationship_predicate_reaches_reader(
    graph: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = graph.adapters.parquet.read_dataset_table
    edge_reads = []

    def read(paths: Any, **kwargs: Any) -> Any:
        table = original(paths, **kwargs)
        if paths and "edge_properties" in str(paths[0]) and kwargs.get("predicates"):
            edge_reads.append(table.num_rows)
        return table

    monkeypatch.setattr(graph.adapters.parquet, "read_dataset_table", read)
    result = graph.use_cases.cypher.execute(
        "MATCH (a:Person)-[r:KNOWS {weight: 7}]->(b:Person) RETURN r.weight AS weight"
    )
    assert result.rows == ({"weight": 7},)
    assert edge_reads == [1]


def test_float_inequality_keeps_nan_matches(tmp_path: Path) -> None:
    app = build_application(tmp_path / "nan-inequality")
    app.use_cases.graph.create_nodes_bulk(
        table=pa.table({"name": ["finite", "nan"], "value": [1.0, float("nan")]}),
        default_labels=("Measurement",),
    )
    query = "MATCH (m:Measurement) WHERE m.value <> 1.0 RETURN m.name AS name"
    app.adapters.parquet.filter_pushdown_enabled = False
    expected = app.use_cases.cypher.execute(query)
    assert expected.rows == ({"name": "nan"},)
    app.adapters.parquet.filter_pushdown_enabled = True
    assert app.use_cases.cypher.execute(query) == expected
