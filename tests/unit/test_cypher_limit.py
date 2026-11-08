"""LIMIT optimizations retain ordering, errors and clause boundaries."""

from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest

from grapharrow.adapters import FileSystemAdapter, ParquetAdapter
from grapharrow.adapters.tabular import sort_window_indices
from grapharrow.app import build_application
from grapharrow.core.errors import QueryError


@pytest.fixture
def app(tmp_path: Path) -> Any:
    app = build_application(tmp_path / "limits")
    app.use_cases.graph.create_nodes_bulk(
        table=pa.table(
            {
                "name": [f"person-{i:04d}" for i in range(512)],
                "age": list(range(512)),
                "bucket": [None if i % 7 == 0 else i % 5 for i in range(512)],
                "payload": [str(i) + ":" + "x" * 1024 for i in range(512)],
            }
        ),
        default_labels=("Person",),
    )
    return app


@pytest.mark.parametrize("columnar", [True, False])
@pytest.mark.parametrize(
    "tail",
    [
        "RETURN coalesce(p.name,'?') AS name LIMIT 3",
        "RETURN coalesce(p.name,'?') AS name SKIP 300 LIMIT 3",
        "RETURN coalesce(p.name,'?') AS name SKIP 600 LIMIT 3",
        "RETURN coalesce(p.name,'?') AS name SKIP 9223372036854775808 LIMIT 3",
        "RETURN coalesce(p.name,'?') AS name LIMIT 0",
        "RETURN coalesce(p.name,'?') AS name ORDER BY p.age DESC SKIP 3 LIMIT 5",
        "RETURN coalesce(p.name,'?') AS name ORDER BY p.bucket DESC,p.age ASC SKIP 3 LIMIT 5",
        "RETURN coalesce(p.name,'?') AS name ORDER BY p.bucket ASC,name DESC LIMIT 5",
        "WITH coalesce(p.name,'?') AS name ORDER BY p.age DESC SKIP 3 LIMIT 5 "
        "WHERE p.bucket=1 RETURN name",
        "WITH coalesce(p.bucket,-1) AS bucket WITH bucket,count(*) AS n RETURN bucket,n ORDER "
        "BY n DESC,bucket LIMIT 2",
        "WITH coalesce(p.bucket,-1) AS bucket RETURN DISTINCT bucket ORDER BY bucket DESC LIMIT 2",
        "WITH p.name AS name,p.age AS age WITH name,age ORDER BY age DESC LIMIT 4 WITH name "
        "RETURN name",
        "RETURN coalesce(p.name,'?') AS name ORDER BY p.bucket DESC LIMIT 500",
    ],
)
def test_window_matches_full_execution(
    app: Any,
    monkeypatch: pytest.MonkeyPatch,
    columnar: bool,
    tail: str,
) -> None:
    monkeypatch.setenv("GRAPHARROW_DISABLE_CYPHER_COLUMNAR", str(int(not columnar)))
    monkeypatch.setenv("GRAPHARROW_DISABLE_CYPHER_LIMIT_OPTIMIZATIONS", "1")
    query = "MATCH (p:Person) " + tail
    expected = app.use_cases.cypher.execute(query)
    monkeypatch.delenv("GRAPHARROW_DISABLE_CYPHER_LIMIT_OPTIMIZATIONS")
    assert app.use_cases.cypher.execute(query) == expected


@pytest.mark.parametrize("direction", ["ascending", "descending"])
@pytest.mark.parametrize("nulls", ["at_start", "at_end"])
@pytest.mark.parametrize("skip,limit", [(0, 0), (0, 3), (7, 5), (90, 100), (999, 3)])
def test_arrow_selection_retains_ties_nulls_and_nans(
    direction: str,
    nulls: str,
    skip: int,
    limit: int,
) -> None:
    table = pa.table(
        {
            "value": [None, 1.0, float("nan"), -0.0, 0.0, float("inf"), float("-inf"), 1.0] * 16,
            "second": ["b", "a", None, "a"] * 32,
            "ordinal": list(range(128)),
        }
    )
    by = [
        ("value", direction, nulls),
        ("second", "descending", "at_start"),
        ("ordinal", "ascending", "at_end"),
    ]
    expected = pc.sort_indices(table, sort_keys=by).slice(skip, limit)
    actual = sort_window_indices(table, by[:-1], skip=skip, limit=limit)
    assert actual.to_pylist() == expected.to_pylist()


@pytest.mark.parametrize("filter_enabled", [True, False])
def test_top_k_sorts_only_selected_candidates_and_reads_payload_late(
    app: Any,
    monkeypatch: pytest.MonkeyPatch,
    filter_enabled: bool,
) -> None:
    graph = app.use_cases.graph
    app.repositories.graph._parquet.filter_pushdown_enabled = filter_enabled
    read = graph.load_node_property_table
    stream = graph.iter_node_property_batches
    scans = []
    sorts = []
    sort = pc.sort_indices

    def scan(**kwargs: Any) -> Any:
        result = read(**kwargs)
        scans.append((kwargs, result.num_rows))
        return result

    def ordered(table: Any, *args: Any, **kwargs: Any) -> Any:
        sorts.append(len(table))
        return sort(table, *args, **kwargs)

    def batches(**kwargs: Any) -> Any:
        count = 0
        iterator = stream(**kwargs)
        try:
            for batch in iterator:
                count += batch.num_rows
                yield batch
        finally:
            iterator.close()
            scans.append((kwargs, count))

    monkeypatch.setattr(graph, "load_node_property_table", scan)
    monkeypatch.setattr(graph, "iter_node_property_batches", batches)
    monkeypatch.setattr(pc, "sort_indices", ordered)
    result = app.use_cases.cypher.execute(
        "MATCH (p:Person) RETURN coalesce(p.payload,'?') AS payload "
        "ORDER BY p.age DESC SKIP 2 LIMIT 3"
    )
    assert [row["payload"].split(":")[0] for row in result.rows] == ["509", "508", "507"]
    assert set(scans[0][0]["columns"]) == {"node_id", "age"}
    assert set(scans[1][0]["columns"]) == {"node_id", "payload"}
    assert 3 <= scans[1][1] <= 512
    assert sorts and max(sorts) <= 5
    assert scans[0][0]["snapshot"] == scans[1][0]["snapshot"]


@pytest.mark.parametrize("filter_enabled", [True, False])
def test_streamed_head_counts_residual_matches_before_skip(
    app: Any,
    monkeypatch: pytest.MonkeyPatch,
    filter_enabled: bool,
) -> None:
    # A deliberately small batch exposes windows crossing several boundaries.
    graph = app.use_cases.graph
    app.repositories.graph._parquet.filter_pushdown_enabled = filter_enabled
    original = graph.iter_node_property_batches
    consumed = []
    closed = []

    def batches(**kwargs: Any) -> Any:
        kwargs["batch_size"] = 16
        try:
            for chunk in original(**kwargs):
                consumed.append(chunk.num_rows)
                yield chunk
        finally:
            closed.append(True)

    def forbidden(**kwargs: Any) -> Any:
        raise AssertionError("Unordered bounded read materialized the entire projection")

    monkeypatch.setattr(graph, "iter_node_property_batches", batches)
    monkeypatch.setattr(graph, "load_node_property_table", forbidden)
    result = app.use_cases.cypher.execute(
        "MATCH (p:Person) WHERE p.age>=10 AND p.name CONTAINS '2' "
        "WITH coalesce(p.name,'?') AS name,p.age AS age SKIP 3 LIMIT 4 "
        "WHERE age<100 RETURN name,age"
    )
    expected = [i for i in range(10, 512) if "2" in f"person-{i:04d}"][3:7]
    assert [row["age"] for row in result.rows] == expected
    assert sum(consumed) < 512
    assert closed == [True]


def test_late_payload_read_keeps_snapshot_after_publish(
    app: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    graph = app.use_cases.graph
    original = graph.load_node_property_table
    first = True

    def read(**kwargs: Any) -> Any:
        nonlocal first
        result = original(**kwargs)
        if first:
            first = False
            graph.create_node(labels=("Person",), properties={"age": 999, "payload": "new"})
        return result

    monkeypatch.setattr(graph, "load_node_property_table", read)
    query = "MATCH (p:Person) RETURN coalesce(p.payload,'?') AS payload ORDER BY p.age DESC LIMIT 1"
    assert app.use_cases.cypher.execute(query).rows[0]["payload"].startswith("511:")
    assert app.use_cases.cypher.execute(query).rows == ({"payload": "new"},)


@pytest.mark.parametrize("limit", [0, 1])
def test_limit_does_not_hide_projection_errors(app: Any, limit: int) -> None:
    with pytest.raises(QueryError):
        app.use_cases.cypher.execute(
            f"MATCH (p:Person) RETURN coalesce(null,1/(p.age-400)) AS value LIMIT {limit}"
        )


def test_partial_stream_failure_discards_rows_and_pins_fallback(
    app: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    graph = app.use_cases.graph
    original = graph.iter_node_property_batches

    def failed(**kwargs: Any) -> Any:
        kwargs["batch_size"] = 4
        iterator = original(**kwargs)
        try:
            yield next(iterator)
            graph.create_node(labels=("Person",), properties={"name": "added", "age": 999})
            raise pa.ArrowInvalid("Simulated failure after the first batch")
        finally:
            iterator.close()

    monkeypatch.setattr(graph, "iter_node_property_batches", failed)
    query = "MATCH (p:Person) RETURN coalesce(p.name,'?') AS name LIMIT 513"
    assert len(app.use_cases.cypher.execute(query).rows) == 512
    monkeypatch.setattr(graph, "iter_node_property_batches", original)
    assert len(app.use_cases.cypher.execute(query).rows) == 513


def test_bounded_parquet_reader_prunes_groups_through_mounted_uri(tmp_path: Path) -> None:
    filesystem = FileSystemAdapter(
        mounts={
            "s3://test-bucket": (pafs.SubTreeFileSystem(str(tmp_path), pafs.LocalFileSystem()), ""),
        }
    )
    pq.write_table(
        pa.table({"id": list(range(100)), "value": list(range(100))}),
        tmp_path / "data.parquet",
        row_group_size=10,
    )
    from grapharrow.domain.scan import ScanPredicate

    chunks = list(
        ParquetAdapter(filesystem).iter_dataset_batches(
            ["s3://test-bucket/data.parquet"],
            columns=["id", "value"],
            predicates=(ScanPredicate("value", "=", 42),),
            batch_size=4,
        )
    )
    assert sum(chunk.num_rows for chunk in chunks) == 10
    assert max(chunk.num_rows for chunk in chunks) <= 4
    assert pa.Table.from_batches(chunks)["id"].to_pylist() == list(range(40, 50))
