"""Schema reuse must preserve per-workflow isolation, admission, and cleanup."""

import gc
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from typing import Any

import pytest

from grapharrow.use_cases.ana import GraphArrowAgentBackend
from grapharrow.use_cases.ana_resources import AnaResourceBudget
from tests.ana_support import load_grapharrow


def test_concurrent_workflows_discover_once_and_release_the_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = load_grapharrow(tmp_path / "shared-schema")
    original = GraphArrowAgentBackend._discover_schema
    calls = []

    def discover(backend: GraphArrowAgentBackend) -> dict[str, Any]:
        calls.append(backend._state.snapshot.snapshot_id)
        return original(backend)

    monkeypatch.setattr(GraphArrowAgentBackend, "_discover_schema", discover)
    budget = AnaResourceBudget()
    barrier = Barrier(4)

    def open_workflow(_: int) -> GraphArrowAgentBackend:
        barrier.wait(timeout=10)
        return GraphArrowAgentBackend(app, resource_budget=budget)

    with ThreadPoolExecutor(max_workers=4) as pool:
        workflows = list(pool.map(open_workflow, range(4)))
    try:
        assert len(calls) == 1
        assert len(app._ana_schema_cache._entries) == 1
        assert all(
            backend._schema_snapshot is workflows[0]._schema_snapshot for backend in workflows
        )
        assert len({id(backend._schema) for backend in workflows}) == 4
        assert budget.used_bytes == sum(backend.resource_usage["bytes"] for backend in workflows)
    finally:
        for backend in workflows:
            backend.close()
    gc.collect()
    assert budget.used_bytes == 0
    assert not app._ana_schema_cache._entries
    with GraphArrowAgentBackend(app):
        assert len(calls) == 2


def test_native_schema_response_mutation_does_not_cross_workflows(tmp_path: Path) -> None:
    app = load_grapharrow(tmp_path / "isolated-schema")
    with GraphArrowAgentBackend(app) as first, GraphArrowAgentBackend(app) as second:
        response = first.call("schema_describe_label", {"label": "Person"})
        response["properties"][0]["types"].append("ChangedByCaller")
        fresh = second.call("schema_describe_label", {"label": "Person"})
        assert all("ChangedByCaller" not in prop["types"] for prop in fresh["properties"])
        with GraphArrowAgentBackend(app) as third:
            fresh = third.call("schema_describe_label", {"label": "Person"})
            assert all("ChangedByCaller" not in prop["types"] for prop in fresh["properties"])
        first.close()
        assert second.call("schema_overview", {})["labels"]


def test_new_snapshot_has_a_new_schema_while_the_old_workflow_stays_pinned(tmp_path: Path) -> None:
    app = load_grapharrow(tmp_path / "changed-schema")
    with GraphArrowAgentBackend(app) as old:
        app.use_cases.graph.create_node(labels=("Project",), properties={"code": "new"})
        with GraphArrowAgentBackend(app) as current:
            assert old._schema_snapshot is not current._schema_snapshot
            assert "Project" not in old._schema["labels"]
            assert "Project" in current._schema["labels"]
            assert len(app._ana_schema_cache._entries) == 2
            app.use_cases.admin.vacuum()
            assert old.call("schema_describe_label", {"label": "Person"})["count"] == 6
            old.close()
            assert len(app._ana_schema_cache._entries) == 1
            assert current.call("schema_describe_label", {"label": "Project"})["count"] == 1
    assert not app._ana_schema_cache._entries


def test_failed_discovery_does_not_poison_or_charge_the_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = load_grapharrow(tmp_path / "failed-schema")
    original = GraphArrowAgentBackend._discover_schema
    budget = AnaResourceBudget()
    calls = 0

    def discover(backend: GraphArrowAgentBackend) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("Simulated discovery failure")
        return original(backend)

    monkeypatch.setattr(GraphArrowAgentBackend, "_discover_schema", discover)
    with pytest.raises(RuntimeError, match="discovery failure"):
        GraphArrowAgentBackend(app, resource_budget=budget)
    assert budget.used_bytes == 0
    assert not app._ana_schema_cache._entries
    with GraphArrowAgentBackend(app, resource_budget=budget) as backend:
        assert backend.call("schema_overview", {})["labels"]
    assert calls == 2
    assert budget.used_bytes == 0
