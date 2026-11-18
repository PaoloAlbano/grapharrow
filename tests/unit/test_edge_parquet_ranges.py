"""Exercise immutable canonical edge files through publication, recovery, and reads."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from typing import Any

import pyarrow as pa
import pytest

from grapharrow.app import GraphArrowApplication, build_application
from grapharrow.config import GraphArrowSettings
from grapharrow.core.errors import WriteConflictError
from grapharrow.core.temporal import Duration
from grapharrow.use_cases import CypherQueryUseCase, GraphTransactionUseCase
from grapharrow.use_cases.ana import GraphArrowAgentBackend


def _database(path: Path, *, width: int | None, edge_count: int = 7) -> GraphArrowApplication:
    application = build_application(
        path, settings=GraphArrowSettings(edge_parquet_range_size=width)
    )
    graph = application.use_cases.graph
    _, node = graph.create_node(labels=("Item",), properties={"name": "anchor"})
    graph.create_edges_bulk(
        table=pa.table(
            {
                "src_id": [node.node_id] * edge_count,
                "dst_id": [node.node_id] * edge_count,
                "rel_type": ["R"] * edge_count,
                "value": list(range(1, edge_count + 1)),
            }
        )
    )
    return application


def test_new_database_partitions_canonical_edges_by_default(tmp_path: Path) -> None:
    application = _database(tmp_path / "default", width=None, edge_count=32_769)
    state = application.use_cases.graph.load_state()
    assert [(file.min_key, file.max_key) for file in state.snapshot.edges] == [
        (1, 32_768),
        (32_769, 32_769),
    ]
    assert state.edge_table.num_rows == 32_769
    assert application.use_cases.admin.verify(verify_checksums=True).ok


def test_compact_uses_default_ranges_for_a_single_file_database(tmp_path: Path) -> None:
    root = tmp_path / "compact-default"
    application = _database(root, width=0)
    assert len(Path(application.repositories.snapshot.load_manifest().edges[0].path).parts) == 3

    compacted = build_application(root).use_cases.admin.compact()
    assert Path(compacted.edges[0].path).parts[:3] == ("data", "edges", "range-32768")
    assert compacted.edges[0].row_count == 7


def test_compact_converts_legacy_edges_and_set_reuses_untouched_ranges(tmp_path: Path) -> None:
    root = tmp_path / "convert"
    legacy = _database(root, width=0)
    writer = build_application(root, settings=GraphArrowSettings(edge_parquet_range_size=3))
    graph = writer.use_cases.graph
    before = graph.load_state()
    assert len(before.snapshot.edges) == 1

    graph.create_node(labels=("Item",), properties={})
    assert graph.load_state().snapshot.edges == before.snapshot.edges
    with GraphArrowAgentBackend(legacy) as pinned:
        compacted = writer.use_cases.admin.compact()
        assert [(file.min_key, file.max_key) for file in compacted.edges] == [
            (1, 3),
            (4, 6),
            (7, 7),
        ]
        assert graph.load_state().edge_table.equals(before.edge_table, check_metadata=True)
        reopened = build_application(root)
        reopened.use_cases.cypher.execute("MATCH ()-[r:R]->() WHERE id(r)=4 SET r.value=4.0")
        middle = reopened.repositories.snapshot.load_manifest()
        assert middle.edges[0] == compacted.edges[0]
        assert middle.edges[1] != compacted.edges[1]
        assert middle.edges[2] == compacted.edges[2]
        assert middle.adjacency_by_source == compacted.adjacency_by_source
        assert middle.adjacency_by_dest == compacted.adjacency_by_dest
        assert type(graph.load_state().edges[3].properties["value"]) is float

        reopened.use_cases.cypher.execute(
            "MATCH ()-[r:R]->() WHERE id(r) IN [1,7] SET r.value=-r.value"
        )
        after = reopened.repositories.snapshot.load_manifest()
        assert after.edges[0] != middle.edges[0]
        assert after.edges[1] == middle.edges[1]
        assert after.edges[2] != middle.edges[2]
        removed = reopened.use_cases.admin.vacuum()
        assert before.snapshot.edges[0].path not in removed
        assert compacted.edges[1].path not in removed
        assert all((root / file.path).is_file() for file in after.edges)
        assert pinned._state.edge_table.equals(before.edge_table)
        assert reopened.use_cases.admin.verify(verify_checksums=True).ok
        assert [edge.properties["value"] for edge in graph.load_state().edges] == [
            -1,
            2,
            3,
            4.0,
            5,
            6,
            -7,
        ]

    table = graph.load_state().edge_table
    resized = build_application(root, settings=GraphArrowSettings(edge_parquet_range_size=2))
    assert [file.row_count for file in resized.use_cases.admin.compact().edges] == [2, 2, 2, 1]
    compacting = build_application(root, settings=GraphArrowSettings(edge_parquet_range_size=0))
    restored = compacting.use_cases.admin.compact()
    assert len(restored.edges) == 1
    assert len(Path(restored.edges[0].path).parts) == 3
    assert graph.load_state().edge_table.equals(table, check_metadata=True)
    assert compacting.use_cases.admin.verify(verify_checksums=True).ok


def test_edge_ranges_survive_tail_growth_and_removal_of_all_edges(tmp_path: Path) -> None:
    root = tmp_path / "growth"
    application = _database(root, width=2, edge_count=3)
    initial = application.repositories.snapshot.load_manifest()
    writer = build_application(root)
    graph = writer.use_cases.graph
    node_id = graph.load_state().nodes[0].node_id
    filled, _ = graph.create_edge(src_id=node_id, dst_id=node_id, rel_type="R", properties={})
    grown, _ = graph.create_edge(src_id=node_id, dst_id=node_id, rel_type="R", properties={})
    assert filled.snapshot.edges[0] == initial.edges[0]
    assert filled.snapshot.edges[1] != initial.edges[1]
    assert grown.snapshot.edges[:2] == filled.snapshot.edges
    assert [(file.min_key, file.max_key) for file in grown.snapshot.edges] == [
        (1, 2),
        (3, 4),
        (5, 5),
    ]
    fewer = graph.delete_entities_batch(node_ids=(), edge_ids=(3, 4))
    assert fewer.snapshot.edges == (grown.snapshot.edges[0], grown.snapshot.edges[2])
    empty = graph.delete_entities_batch(node_ids=(), edge_ids=(1, 2, 5))
    assert len(empty.snapshot.edges) == 1
    assert empty.snapshot.edges[0].row_count == 0
    assert empty.snapshot.edges[0].min_key is None
    assert writer.use_cases.admin.compact().edges[0].row_count == 0

    default_writer = build_application(root)
    again, _ = default_writer.use_cases.graph.create_edge(
        src_id=node_id, dst_id=node_id, rel_type="R", properties={}
    )
    assert Path(again.snapshot.edges[0].path).parts[:3] == ("data", "edges", "range-2")
    assert default_writer.use_cases.admin.verify(verify_checksums=True).ok


def test_edge_property_ranges_reuse_files_and_share_one_schema(tmp_path: Path) -> None:
    root = tmp_path / "projected"
    application = _database(root, width=3)
    repository = application.repositories.snapshot
    query = application.use_cases.cypher.execute
    before = repository.load_manifest()
    assert [(file.min_key, file.max_key) for file in before.edge_properties] == [
        (1, 3),
        (4, 6),
        (7, 7),
    ]

    query("MATCH ()-[r:R]->() WHERE id(r)=4 SET r.value=40")
    updated = repository.load_manifest()
    assert updated.edge_properties[0] == before.edge_properties[0]
    assert updated.edge_properties[1] != before.edge_properties[1]
    assert updated.edge_properties[2] == before.edge_properties[2]

    query("MATCH ()-[r:R]->() WHERE id(r)=4 SET r.value=40.0, r.extra='only here'")
    mixed = repository.load_manifest()
    assert set(mixed.edge_properties).isdisjoint(updated.edge_properties)
    tables = [
        application.adapters.parquet.read_table(root / file.path) for file in mixed.edge_properties
    ]
    schema = tables[0].schema
    assert all(schema.equals(table.schema, check_metadata=True) for table in tables)
    assert schema.field("value").metadata == {b"grapharrow.encoding": b"json"}
    assert tables[0]["extra"].null_count == 3

    result = query("MATCH ()-[r:R]->() RETURN id(r) AS id, r.value AS value")
    ordered = sorted(result.rows, key=lambda row: row["id"])
    assert [type(row["value"]) for row in ordered] == [int, int, int, float, int, int, int]
    assert query("MATCH ()-[r:R {value: 40.0}]->() RETURN id(r) AS id").rows == ({"id": 4},)
    assert query("MATCH ()-[r:R]->() WHERE r.extra IS NULL RETURN count(r) AS n").rows == (
        {"n": 6},
    )

    query("MATCH ()-[r:R]->() WHERE id(r)=7 SET r.value='seven'")
    final = repository.load_manifest()
    assert final.edge_properties[:2] == mixed.edge_properties[:2]
    assert final.edge_properties[2] != mixed.edge_properties[2]
    assert query("MATCH ()-[r:R {value: 'seven'}]->() RETURN id(r) AS id").rows == ({"id": 7},)
    native = application.use_cases.graph.update_properties_batch(
        node_updates=(), edge_updates=((1, "value", 1.0),)
    )
    assert native.snapshot.edge_properties[0] != final.edge_properties[0]
    assert native.snapshot.edge_properties[1:] == final.edge_properties[1:]
    assert type(native.edges[0].properties["value"]) is float
    application.use_cases.admin.vacuum()
    assert application.use_cases.admin.verify(verify_checksums=True).ok


def test_edge_projection_preserves_structural_ids_and_typed_properties(tmp_path: Path) -> None:
    application = _database(tmp_path / "reserved", width=2, edge_count=2)
    graph = application.use_cases.graph
    node_id = graph.load_state().nodes[0].node_id
    graph.create_edge(
        src_id=node_id,
        dst_id=node_id,
        rel_type="R",
        properties={"edge_id": 99, "src_id": 77, "elapsed": Duration(0, 1, 0)},
    )
    query = application.use_cases.cypher.execute
    identities = query("MATCH ()-[r:R]->() RETURN id(r) AS id")
    assert sorted(row["id"] for row in identities.rows) == [1, 2, 3]
    assert query(
        "MATCH ()-[r:R {edge_id: 99}]->() RETURN r.src_id AS source, r.elapsed AS elapsed"
    ).rows == ({"source": 77, "elapsed": "P1D"},)
    assert query("MATCH ()-[r:R]->() WHERE id(r)=3 RETURN r.elapsed.days AS elapsed").rows == (
        {"elapsed": 1},
    )


def test_delete_publishes_changed_range_and_preserves_pinned_ana(tmp_path: Path) -> None:
    application = _database(tmp_path / "delete", width=3)
    graph = application.use_cases.graph
    old = graph.load_state()
    with GraphArrowAgentBackend(application) as reader:
        application.use_cases.cypher.execute("MATCH ()-[r:R]->() WHERE id(r)=4 DELETE r")
        changed = graph.load_state()
        assert changed.snapshot.edges[0] == old.snapshot.edges[0]
        assert changed.snapshot.edges[1] != old.snapshot.edges[1]
        assert changed.snapshot.edges[2] == old.snapshot.edges[2]
        assert changed.snapshot.edge_properties[0] == old.snapshot.edge_properties[0]
        assert changed.snapshot.edge_properties[2] == old.snapshot.edge_properties[2]
        application.use_cases.admin.vacuum()
        assert reader._state.edge_table["edge_id"].to_pylist() == list(range(1, 8))
        with GraphArrowAgentBackend(application) as current:
            assert current._state.edge_table["edge_id"].to_pylist() == [1, 2, 3, 5, 6, 7]
    assert application.use_cases.admin.verify(verify_checksums=True).ok


@pytest.mark.parametrize("other_id", [2, 6])
def test_ranged_writers_retry_after_publication_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, other_id: int
) -> None:
    root = tmp_path / "failure"
    application = _database(root, width=3)
    saved = application.repositories.snapshot.load_manifest()
    statement = "MATCH ()-[r:R]->() WHERE id(r)=$id SET r.value=99"

    def fail(*args: Any, **kwargs: Any) -> None:
        raise OSError("Simulated pointer failure")

    with monkeypatch.context() as patch:
        patch.setattr(application.adapters.filesystem, "replace_json", fail)
        with pytest.raises(OSError, match="pointer failure"):
            application.use_cases.cypher.execute(statement, {"id": 1})
    assert application.repositories.snapshot.load_manifest() == saved
    assert application.use_cases.admin.verify(verify_checksums=True).ok

    stale = GraphTransactionUseCase(application.use_cases.graph)
    CypherQueryUseCase(stale).execute(statement, {"id": other_id})
    barrier = Barrier(2)

    def update(edge_id: int) -> None:
        writer = build_application(root)
        barrier.wait(timeout=10)
        writer.use_cases.cypher.execute(statement, {"id": edge_id})

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(update, (1, other_id)))
    with pytest.raises(WriteConflictError, match="Database changed"):
        stale.commit()
    final = application.use_cases.graph.load_state()
    assert {edge.edge_id for edge in final.edges if edge.properties["value"] == 99} == {1, other_id}
    application.use_cases.admin.vacuum()
    assert application.use_cases.admin.verify(verify_checksums=True).ok
