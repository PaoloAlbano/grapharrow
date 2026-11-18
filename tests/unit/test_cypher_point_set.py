"""Selective SET preserves graph snapshots and falls back when broader state is needed."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from typing import Any

import pyarrow as pa
import pytest

from grapharrow.app import GraphArrowApplication, build_application
from grapharrow.config import GraphArrowSettings
from grapharrow.core.errors import QueryError, WriteConflictError
from grapharrow.use_cases import CypherQueryUseCase, GraphTransactionUseCase
from grapharrow.use_cases.ana import GraphArrowAgentBackend

QUERY = (
    "MATCH ()-[r:LINK]->() WHERE id(r)=$id "
    "SET r.value=$value, r.witness=$value RETURN r.value AS value"
)


def database(path: Path) -> GraphArrowApplication:
    app = build_application(path, settings=GraphArrowSettings(edge_parquet_range_size=4))
    app.use_cases.cypher.execute(
        "CREATE (a:Person), (b:Person) WITH a,b UNWIND range(1,12) AS i "
        "CREATE (a)-[:LINK {value:'seed', witness:'seed'}]->(b)"
    )
    return app


def test_point_set_reads_only_one_range_and_reuses_other_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = database(tmp_path)
    graph, repository = app.use_cases.graph, app.repositories.graph
    original = graph.capture_snapshot()
    loaded = []
    load = repository.load_edge_table

    def load_range(files: Any) -> pa.Table:
        loaded.append(files)
        assert len(files) == 1
        return load(files)

    def full_state(*args: Any, **kwargs: Any) -> Any:
        pytest.fail("A point SET must not load the complete graph or its nodes")

    with GraphArrowAgentBackend(app) as reader:
        with monkeypatch.context() as patch:
            patch.setattr(graph, "load_state", full_state)
            patch.setattr(repository, "load_node_table", full_state)
            patch.setattr(repository, "load_edge_table", load_range)
            result = app.use_cases.cypher.execute(QUERY, {"id": 6, "value": "next"})
        assert result.rows == ({"value": "next"},)
        current = graph.capture_snapshot()
        assert loaded == [(original.edges[1],)]
        assert set(original.edges) & set(current.edges) == {original.edges[0], original.edges[2]}
        assert len(set(original.edge_properties) - set(current.edge_properties)) == 1
        for name in (
            "nodes",
            "node_labels",
            "node_properties",
            "adjacency_by_source",
            "adjacency_by_dest",
        ):
            assert getattr(original, name) == getattr(current, name)
        assert current.catalog_path == original.catalog_path
        assert graph.load_state().edges[5].properties["value"] == "next"
        assert graph.load_state(snapshot=original).edges[5].properties["value"] == "seed"
        app.use_cases.admin.vacuum()
        assert reader._state.snapshot == original
        assert app.use_cases.admin.verify(verify_checksums=True).ok


@pytest.mark.parametrize(
    "suffix", ["", " RETURN r.value AS value LIMIT 0", " RETURN r.value AS value SKIP 1"]
)
def test_point_set_applies_writes_before_result_window(tmp_path: Path, suffix: str) -> None:
    app = database(tmp_path)
    result = app.use_cases.cypher.execute(
        "MATCH ()-[r:LINK]->() WHERE id(r)=1 SET r.value='changed'" + suffix
    )
    assert result.rows == ()
    assert app.use_cases.graph.load_state().edges[0].properties["value"] == "changed"


@pytest.mark.parametrize("edge_id,rel_type", [(99, "LINK"), (1, "MISSING")])
def test_missing_point_match_does_not_publish(tmp_path: Path, edge_id: int, rel_type: str) -> None:
    app = database(tmp_path)
    initial = app.use_cases.graph.capture_snapshot()
    result = app.use_cases.cypher.execute(
        QUERY.replace(":LINK", ":" + rel_type), {"id": edge_id, "value": "next"}
    )
    assert result.columns == ("value",)
    assert result.rows == ()
    assert app.use_cases.graph.capture_snapshot() == initial


def test_point_set_result_failure_rolls_back_before_writing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = database(tmp_path)
    initial = app.use_cases.graph.capture_snapshot()
    files = {file.relative_to(tmp_path) for file in tmp_path.rglob("*") if file.is_file()}

    def fail(*args: Any, **kwargs: Any) -> Any:
        raise QueryError("Result failed")

    monkeypatch.setattr(app.use_cases.cypher, "_finalize_table_query_result", fail)
    with pytest.raises(QueryError, match="Result failed"):
        app.use_cases.cypher.execute(QUERY, {"id": 1, "value": "discarded"})
    assert app.use_cases.graph.capture_snapshot() == initial
    assert files == {file.relative_to(tmp_path) for file in tmp_path.rglob("*") if file.is_file()}


@pytest.mark.parametrize("phase", ["persist", "publish"])
def test_failed_point_commit_does_not_replace_the_visible_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    app = database(tmp_path)
    initial = app.use_cases.graph.capture_snapshot()

    def fail(*args: Any, **kwargs: Any) -> Any:
        raise OSError("Interrupted commit")

    target = app.repositories.graph if phase == "persist" else app.repositories.snapshot
    method = "_write_edge_property_table" if phase == "persist" else "publish"
    with monkeypatch.context() as patch:
        patch.setattr(target, method, fail)
        with pytest.raises(OSError, match="Interrupted"):
            app.use_cases.cypher.execute(QUERY, {"id": 1, "value": "discarded"})
    assert app.use_cases.graph.capture_snapshot() == initial
    assert app.use_cases.graph.load_state().edges[0].properties["value"] == "seed"
    assert app.use_cases.admin.verify(verify_checksums=True).ok


def test_point_commit_detects_a_competing_commit_after_preparation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = database(tmp_path)
    other = build_application(tmp_path)
    publish = app.repositories.snapshot.publish

    def concurrent(manifest: Any, *, base: Any) -> Any:
        other.use_cases.cypher.execute(QUERY, {"id": 9, "value": "winner"})
        return publish(manifest, base=base)

    with monkeypatch.context() as patch:
        patch.setattr(app.repositories.snapshot, "publish", concurrent)
        with pytest.raises(WriteConflictError):
            app.use_cases.cypher.execute(QUERY, {"id": 1, "value": "discarded"})
    state = app.use_cases.graph.load_state()
    assert state.edges[0].properties["value"] == "seed"
    assert state.edges[8].properties["value"] == "winner"
    assert app.use_cases.admin.verify(verify_checksums=True).ok


@pytest.mark.parametrize("shared_edge", [False, True])
def test_ten_point_writers_preserve_coherent_properties(tmp_path: Path, shared_edge: bool) -> None:
    app = database(tmp_path)
    initial = app.use_cases.graph.capture_snapshot()
    barrier = Barrier(10)

    def write(index: int) -> None:
        barrier.wait(timeout=10)
        for wave in range(2):
            value = f"writer-{index}-{wave}"
            result = app.use_cases.cypher.execute(
                QUERY, {"id": 1 if shared_edge else index + 1, "value": value}
            )
            assert result.rows == ({"value": value},)

    with ThreadPoolExecutor(max_workers=10) as pool:
        list(pool.map(write, range(10)))
    state = app.use_cases.graph.load_state()
    assert state.snapshot.snapshot_id.value == initial.snapshot_id.value + 20
    assert all(edge.properties["value"] == edge.properties["witness"] for edge in state.edges)
    if not shared_edge:
        assert [edge.properties["value"] for edge in state.edges[:10]] == [
            f"writer-{i}-1" for i in range(10)
        ]


def test_point_set_in_explicit_transaction_keeps_rollback(tmp_path: Path) -> None:
    app = database(tmp_path)
    initial = app.use_cases.graph.capture_snapshot()
    transaction = GraphTransactionUseCase(app.use_cases.graph)
    query = CypherQueryUseCase(transaction)  # type: ignore[arg-type]
    assert query.execute(QUERY, {"id": 1, "value": "uncommitted"}).rows == (
        {"value": "uncommitted"},
    )
    assert app.use_cases.graph.capture_snapshot() == initial
    transaction.rollback()
    assert app.use_cases.graph.load_state().edges[0].properties["value"] == "seed"


@pytest.mark.parametrize(
    "query,params",
    [
        (QUERY, {"id": 1, "value": 42}),
        (QUERY, {"id": 1, "value": None}),
        ("MATCH ()-[r:LINK]->() WHERE id(r)=1 SET r.extra='new' RETURN r.extra", {}),
        ("MATCH (a:Person)-[r:LINK]->() WHERE id(r)=1 SET r.value='next' RETURN r.value", {}),
        ("MATCH ()-[r:LINK]-() WHERE id(r)=1 SET r.value='next' RETURN r.value", {}),
        (
            (
                "MATCH ()-[r:LINK]->() WHERE id(r)=1 AND r.value='seed' "
                "SET r.value='next' RETURN r.value"
            ),
            {},
        ),
        ("MATCH ()-[r:LINK]->() WHERE id(r)=1 SET r.value=r.witness RETURN r.value", {}),
        ("MATCH ()-[r:LINK]->() WHERE id(r)=1 SET r.value='next' RETURN r", {}),
    ],
)
def test_point_set_falls_back_for_broader_reads_schema_changes_and_entities(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, query: str, params: dict[str, object]
) -> None:
    app = database(tmp_path)
    load = app.use_cases.graph.load_state
    loads = []

    def full_state(**kwargs: Any) -> Any:
        loads.append(True)
        return load(**kwargs)

    monkeypatch.setattr(app.use_cases.graph, "load_state", full_state)
    app.use_cases.cypher.execute(query, params)
    assert loads
    assert app.use_cases.admin.verify(verify_checksums=True).ok


def test_point_set_preserves_mixed_types_and_other_relationship_projections(tmp_path: Path) -> None:
    app = build_application(tmp_path, settings=GraphArrowSettings(edge_parquet_range_size=4))
    _, node = app.use_cases.graph.create_node(labels=("Person",), properties={})
    app.use_cases.graph.create_edges_bulk(
        table=pa.table(
            {
                "src_id": [node.node_id] * 7,
                "dst_id": [node.node_id] * 7,
                "rel_type": ["LINK", "OTHER", "LINK", "LINK", "LINK", "OTHER", "LINK"],
                # Sparse types force JSON encoding shared across all LINK partitions.
                "payload": ['{"value":1}', '"other"', '"text"', "null", "true", "2", "[1,2]"],
            }
        )
    )
    # Use the canonical property API to construct a genuinely heterogeneous field.
    app.use_cases.cypher.execute("MATCH ()-[r:LINK]->() WHERE id(r)=1 SET r.payload=1")
    initial = app.use_cases.graph.capture_snapshot()
    result = app.use_cases.cypher.execute(
        "MATCH ()-[r:LINK]->() WHERE id(r)=1 SET r.payload=2 RETURN r.payload AS payload"
    )
    assert result.rows == ({"payload": 2},)
    state = app.use_cases.graph.load_state()
    assert state.edges[0].properties["payload"] == 2
    assert state.edges[2].properties["payload"] == '"text"'
    for file in initial.edge_properties:
        if "/OTHER/" in file.path:
            assert file in state.snapshot.edge_properties
    projection = app.repositories.graph.load_edge_property_table(
        state.snapshot.edge_properties, rel_type="LINK"
    )
    assert projection.schema.field("payload").metadata == {b"grapharrow.encoding": b"json"}
    assert projection["payload"].to_pylist()[0] == "2"
    assert app.use_cases.admin.verify(verify_checksums=True).ok


def test_point_set_with_a_constraint_uses_full_validation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dataclasses import replace

    from grapharrow import Database
    from grapharrow.core.errors import SchemaError
    from grapharrow.domain import CatalogVersion, ConstraintDefinition

    app = database(tmp_path)
    catalog = app.repositories.catalog.load_for_snapshot(app.use_cases.graph.capture_snapshot())
    Database(tmp_path).write_catalog(
        replace(
            catalog,
            version=CatalogVersion(catalog.version.value + 1),
            constraints=(
                ConstraintDefinition(
                    kind="required",
                    owner_kind="relationship_type",
                    owner_name="LINK",
                    property_name="value",
                ),
            ),
        )
    )
    initial = app.use_cases.graph.capture_snapshot()

    def no_patch(*args: Any, **kwargs: Any) -> Any:
        pytest.fail("Constrained graphs must use the validated transaction path")

    monkeypatch.setattr(app.repositories.graph, "prepare_edge_property_patch", no_patch)
    with pytest.raises((QueryError, SchemaError)):
        app.use_cases.cypher.execute(QUERY, {"id": 1, "value": None})
    assert app.use_cases.graph.capture_snapshot() == initial


def test_row_dependent_set_copies_the_property_value(tmp_path: Path) -> None:
    app = database(tmp_path)
    app.use_cases.cypher.execute("MATCH ()-[r:LINK]->() WHERE id(r)=1 SET r.witness='source'")
    result = app.use_cases.cypher.execute(
        "MATCH ()-[r:LINK]->() WHERE id(r)=1 SET r.value=r.witness RETURN r.value AS value"
    )
    assert result.rows == ({"value": "source"},)
    assert app.use_cases.graph.load_state().edges[0].properties["value"] == "source"
    assert app.use_cases.admin.verify(verify_checksums=True).ok
