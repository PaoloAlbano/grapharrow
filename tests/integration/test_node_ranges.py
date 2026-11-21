"""Node property writes retain unrelated Parquet ranges on both backends."""

from dataclasses import replace
from pathlib import Path
from typing import Literal
from uuid import uuid4

import pytest

from grapharrow.app import build_application
from grapharrow.config import GraphArrowSettings
from grapharrow.domain.records import NodeRecord
from grapharrow.repositories.graph import NODE_RANGE_SIZE

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("backend", ["native", "delta"])
def test_node_update_preserves_unmodified_ranges_and_historical_reads(
    backend: Literal["native", "delta"], tmp_path: Path
) -> None:
    if backend == "delta":
        pytest.importorskip("deltalake")
    app = build_application(
        tmp_path / "graph", settings=GraphArrowSettings(storage_backend=backend)
    )
    graph = app.use_cases.graph
    cypher = app.use_cases.cypher
    high_id = NODE_RANGE_SIZE + 1
    graph.publish_records(
        previous_state=graph.load_state(),
        new_nodes=(
            NodeRecord(1, ("Person",), {"rank": 10}),
            NodeRecord(high_id, ("Person",), {"rank": 20}),
        ),
        new_edges=(),
    )
    before = graph.capture_snapshot()
    files_before = app.repositories.graph.node_cache_files(before.nodes)
    assert len(files_before) == 2
    assert cypher.execute(
        "MATCH (n:Person) WHERE id(n)=$id SET n.rank=30 RETURN n.rank AS rank", {"id": high_id}
    ).rows == ({"rank": 30},)
    after = graph.capture_snapshot()
    files_after = app.repositories.graph.node_cache_files(after.nodes)

    assert after.snapshot_id.value == before.snapshot_id.value + 1
    assert len(files_after) == 2
    assert len(set(files_before) & set(files_after)) == 1
    assert cypher.execute("MATCH (n:Person) RETURN n.rank AS rank ORDER BY id(n)").rows == (
        {"rank": 10},
        {"rank": 30},
    )
    old_rows = graph.load_node_property_table(snapshot=before, label="Person")
    assert old_rows.sort_by("node_id")["rank"].to_pylist() == [10, 20]
    new_rows = graph.load_node_property_table(snapshot=after, label="Person")
    assert new_rows.sort_by("node_id")["rank"].to_pylist() == [10, 30]
    if backend == "native":
        assert len(set(before.node_properties) & set(after.node_properties)) == 1
        assert before.node_labels == after.node_labels

    # A new property must reach the schema of every range for the label.
    cypher.execute("MATCH (n:Person) WHERE id(n)=$id SET n.extra='new'", {"id": high_id})
    assert cypher.execute("MATCH (n:Person) RETURN n.extra AS extra ORDER BY id(n)").rows == (
        {"extra": None},
        {"extra": "new"},
    )
    cypher.execute("MATCH (n:Person) WHERE id(n)=$id DELETE n", {"id": high_id})
    final = graph.capture_snapshot()
    assert len(app.repositories.graph.node_cache_files(final.nodes)) == 1
    assert cypher.execute("MATCH (n:Person) RETURN n.rank AS rank").rows == ({"rank": 10},)
    assert app.use_cases.admin.verify().ok


def test_delta_converts_an_unsplit_node_partition_without_changing_history(tmp_path: Path) -> None:
    pytest.importorskip("deltalake")
    from grapharrow.repositories.graph import _nodes_table
    from grapharrow.storage.delta import DeltaStorageBackend, _metadata

    app = build_application(
        tmp_path / "legacy", settings=GraphArrowSettings(storage_backend="delta")
    )
    graph = app.use_cases.graph
    backend = app.storage_backend
    assert isinstance(backend, DeltaStorageBackend)
    graph.publish_records(
        previous_state=graph.load_state(),
        new_nodes=(
            NodeRecord(1, ("Person",), {"rank": 7}),
            NodeRecord(NODE_RANGE_SIZE + 1, ("Person",), {"rank": 8}),
        ),
        new_edges=(),
    )
    current = graph.capture_snapshot()
    base = backend._require_table(current.snapshot_id)
    head, catalog = _metadata(base)
    legacy = backend._commit(
        base,
        replace(
            head, commit_id=uuid4().hex, node_ranges=False, metadata_version=base.version() + 1
        ),
        catalog,
        {"nodes": _nodes_table(list(graph.load_state().nodes))},
        replace_all_nodes=True,
    )
    assert len(backend.graph.node_cache_files(legacy.nodes)) == 1
    app.use_cases.cypher.execute("MATCH (n:Person) WHERE id(n)=1 SET n.rank=9")
    after = graph.capture_snapshot()
    assert len(backend.graph.node_cache_files(after.nodes)) == 2
    assert [node.properties["rank"] for node in graph.load_state(snapshot=legacy).nodes] == [7, 8]
    assert [node.properties["rank"] for node in graph.load_state(snapshot=after).nodes] == [9, 8]
