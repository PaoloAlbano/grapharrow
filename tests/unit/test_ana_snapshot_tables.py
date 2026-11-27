"""ANA borrows immutable tables for the lifetime of a workflow."""

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import replace
from pathlib import Path
from threading import Barrier, Event

import pyarrow as pa
import pytest

from grapharrow.domain import DatasetFile
from grapharrow.use_cases import ana_node_lookup
from grapharrow.use_cases.ana import GraphArrowAgentBackend
from grapharrow.use_cases.ana_resources import AnaQuotaExceeded, AnaResourceBudget
from tests.ana_support import fetch, load_grapharrow


def test_concurrent_ana_workflows_share_canonical_and_label_tables(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = load_grapharrow(tmp_path / "shared-tables")
    store = app.repositories.graph
    nodes, edges, properties = (
        store.load_node_table,
        store.load_edge_table,
        store.load_node_property_table,
    )
    calls: Counter[str] = Counter()
    start = Barrier(4)
    build = ana_node_lookup._build_lookup

    def build_lookup(table: pa.Table) -> ana_node_lookup.AnaNodeLookup:
        calls["lookup"] += 1
        return build(table)

    def load_nodes(files: tuple[DatasetFile, ...]) -> pa.Table:
        calls["nodes"] += 1
        return nodes(files)

    def load_edges(files: tuple[DatasetFile, ...]) -> pa.Table:
        calls["edges"] += 1
        return edges(files)

    def load_properties(files: tuple[DatasetFile, ...], *, label: str) -> pa.Table:
        calls[label] += 1
        return properties(files, label=label)

    monkeypatch.setattr(store, "load_node_table", load_nodes)
    monkeypatch.setattr(store, "load_edge_table", load_edges)
    monkeypatch.setattr(store, "load_node_property_table", load_properties)
    monkeypatch.setattr(ana_node_lookup, "_build_lookup", build_lookup)

    def open_workflow(_: int) -> GraphArrowAgentBackend:
        start.wait(timeout=10)
        return GraphArrowAgentBackend(app)

    graph = app.use_cases.graph
    with ExitStack() as stack, ThreadPoolExecutor(max_workers=4) as pool:
        workflows = [stack.enter_context(backend) for backend in pool.map(open_workflow, range(4))]
        first = workflows[0]
        assert calls == dict.fromkeys(["nodes", "edges", "lookup"], 1)
        assert all(not backend._property_tables for backend in workflows)
        for backend in workflows:
            assert backend._property_table("Person") is not None
        assert calls["Person"] == 1
        assert calls["Company"] == 0
        assert first._property_tables["Person"].num_rows == 6
        assert all(backend._state.node_table is first._state.node_table for backend in workflows)
        assert all(backend._state.edge_table is first._state.edge_table for backend in workflows)
        assert all(backend._node_lookup is first._node_lookup for backend in workflows)
        assert all(
            backend.resource_usage["snapshot_bytes"] == first.resource_usage["snapshot_bytes"]
            for backend in workflows
        )
        assert all(
            backend._property_tables[label] is table
            for backend in workflows
            for label, table in first._property_tables.items()
        )
        with graph.borrow_tabular_state() as cypher:
            assert cypher.node_table is first._state.node_table
            assert cypher.edge_table is first._state.edge_table
        assert all(entry.readers == 4 for entry in graph.snapshot_tables._entries.values())
        first.close()
        assert all(entry.readers == 3 for entry in graph.snapshot_tables._entries.values())
        assert fetch(workflows[1], "count_nodes", {"label": "Person"})["rows"] == [[6]]
    assert all(entry.readers == 0 for entry in graph.snapshot_tables._entries.values())
    assert not app._ana_node_lookups._entries


def test_ana_projection_reuse_tracks_immutable_files_across_publication(tmp_path: Path) -> None:
    app = load_grapharrow(tmp_path / "published-tables")
    with GraphArrowAgentBackend(app) as old:
        app.use_cases.graph.create_node(labels=("Project",), properties={"name": "new"})
        with GraphArrowAgentBackend(app) as current:
            assert old._state.node_table is not current._state.node_table
            assert old._node_lookup is not current._node_lookup
            assert old._state.edge_table is current._state.edge_table
            assert old._property_table("Person") is current._property_table("Person")
            assert old._property_table("Project") is None
            project = current._property_table("Project")
            assert project is not None and project.num_rows == 1
        app.use_cases.admin.compact()
        app.use_cases.admin.vacuum()
        assert fetch(old, "count_nodes", {"label": "Person"})["rows"] == [[6]]
        with GraphArrowAgentBackend(app) as current:
            assert fetch(current, "node_search", {"label": "Project", "value": "new"})["rows"]
            assert fetch(old, "node_search", {"label": "Project", "value": "new"})["rows"] == []


def test_ana_lookup_reuses_node_files_after_an_edge_only_commit(tmp_path: Path) -> None:
    app = load_grapharrow(tmp_path / "unchanged-nodes")
    with GraphArrowAgentBackend(app) as old:
        alice, bob, *_ = old._label_ids["Person"]
        app.use_cases.graph.create_edge(src_id=alice, dst_id=bob, rel_type="KNOWS", properties={})
        with GraphArrowAgentBackend(app) as current:
            assert current._state.snapshot != old._state.snapshot
            assert current._state.edge_table.num_rows == old._state.edge_table.num_rows + 1
            assert current._node_lookup is old._node_lookup


def test_ana_can_read_a_snapshot_without_property_projections(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = load_grapharrow(tmp_path / "legacy-projections")
    manifest = app.repositories.snapshot.load_manifest()
    legacy = replace(manifest, node_properties=())
    monkeypatch.setattr(app.repositories.snapshot, "load_at", lambda pointer: legacy)
    with GraphArrowAgentBackend(app) as backend:
        assert not backend._property_tables
        assert fetch(backend, "node_search", {"label": "Person", "value": "Alice"})["rows"] == [
            ["Alice"]
        ]


def test_slow_ana_node_lookup_allows_commit_and_vacuum(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = load_grapharrow(tmp_path / "lookup-during-write")
    indexing = Event()
    resume = Event()
    build = ana_node_lookup._build_lookup

    def pause_lookup(table: pa.Table) -> ana_node_lookup.AnaNodeLookup:
        indexing.set()
        assert resume.wait(timeout=20)
        return build(table)

    monkeypatch.setattr(ana_node_lookup, "_build_lookup", pause_lookup)

    def publish() -> None:
        app.use_cases.graph.create_node(labels=("Person",), properties={"name": "new"})
        app.use_cases.admin.vacuum()

    with ThreadPoolExecutor(max_workers=2) as pool:
        opening = pool.submit(GraphArrowAgentBackend, app)
        try:
            assert indexing.wait(timeout=10)
            pool.submit(publish).result(timeout=10)
        finally:
            resume.set()
        with opening.result(timeout=10) as old, GraphArrowAgentBackend(app) as current:
            assert fetch(old, "count_nodes", {"label": "Person"})["rows"] == [[6]]
            assert fetch(current, "count_nodes", {"label": "Person"})["rows"] == [[7]]
            assert old._node_lookup is not current._node_lookup


@pytest.mark.parametrize("failure", ["read", "admission"])
def test_failed_ana_open_releases_table_borrows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    app = load_grapharrow(tmp_path / "failed-open")
    budget = AnaResourceBudget(1)

    def fail(*args: object, **kwargs: object) -> pa.Table:
        raise OSError("Projection read failed")

    with monkeypatch.context() as patch:
        if failure == "read":
            patch.setattr(app.repositories.graph, "load_node_property_table", fail)
            with GraphArrowAgentBackend(app) as backend:
                with pytest.raises(OSError, match="Projection read failed"):
                    backend.call("node_search", {"label": "Person", "value": "Alice"})
                assert not backend._property_tables
        else:
            with pytest.raises(AnaQuotaExceeded, match="server byte capacity"):
                GraphArrowAgentBackend(app, resource_budget=budget)
    cache = app.use_cases.graph.snapshot_tables
    assert cache._entries
    assert all(entry.readers == 0 for entry in cache._entries.values())
    assert budget.used_bytes == 0
    with GraphArrowAgentBackend(app) as backend:
        assert fetch(backend, "count_nodes", {"label": "Person"})["rows"] == [[6]]


def test_ana_falls_back_when_tables_exceed_cache_budget(tmp_path: Path) -> None:
    app = load_grapharrow(tmp_path / "uncached-tables")
    cache = app.use_cases.graph.snapshot_tables
    cache.max_bytes = 1
    with GraphArrowAgentBackend(app) as first, GraphArrowAgentBackend(app) as second:
        assert first._state.node_table is not second._state.node_table
        assert first._property_table("Person") is not second._property_table("Person")
        assert fetch(first, "count_nodes", {"label": "Person"}) == fetch(
            second, "count_nodes", {"label": "Person"}
        )
    assert cache.used_bytes == 0


def test_failed_ana_tool_releases_its_new_lazy_projection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = load_grapharrow(tmp_path / "failed-tool")
    budget = AnaResourceBudget()
    cache = app.use_cases.graph.snapshot_tables
    with GraphArrowAgentBackend(app, resource_budget=budget) as backend:
        baseline = budget.used_bytes

        def fail(*args: object, **kwargs: object) -> None:
            assert "Person" in backend._property_tables
            raise AnaQuotaExceeded("response quota")

        args = {"label": "Person", "value": "Alice"}
        with monkeypatch.context() as patch:
            patch.setattr(backend._resources, "finalize", fail)
            with pytest.raises(AnaQuotaExceeded, match="response quota"):
                backend.call("node_search", args)
        assert not backend._property_tables and budget.used_bytes == baseline
        assert all(
            entry.readers == 0
            for key, entry in cache._entries.items()
            if key[0] == "node_properties"
        )
        assert fetch(backend, "node_search", args)["rows"] == [["Alice"]]
    assert budget.used_bytes == 0
