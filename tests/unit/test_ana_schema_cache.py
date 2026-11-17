"""Schema reuse must preserve per-workflow isolation, admission, and cleanup."""

import gc
import json
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from pathlib import Path
from threading import Barrier, Event
from typing import Any

import pyarrow as pa
import pytest

from grapharrow.app import build_application
from grapharrow.config import GraphArrowSettings
from grapharrow.core import property_codec
from grapharrow.core.temporal import Duration
from grapharrow.use_cases import ana_edge_statistics
from grapharrow.use_cases.ana import GraphArrowAgentBackend, _arrow_label_relationships
from grapharrow.use_cases.ana_resources import AnaResourceBudget
from tests.ana_support import load_grapharrow


@pytest.mark.parametrize(
    "value",
    [
        1,
        1.0,
        2**64,
        -(2**63) - 1,
        {"nested": [2**64, None]},
        [None, [2**64]],
        1.0e24,
        float("nan"),
        float("inf"),
        chr(0xD800),
        Duration(1, 2, 3),
    ],
)
def test_schema_decoder_preserves_canonical_property_types(value: Any) -> None:
    payload = property_codec.dumps({"property": value})
    assert property_codec.dumps(property_codec.loads_for_schema(payload)) == payload


@pytest.mark.parametrize("multi_label", [False, True])
def test_schema_preserves_directed_label_relationships(tmp_path: Path, multi_label: bool) -> None:
    app = build_application(tmp_path / "label-relationships")
    graph = app.use_cases.graph
    source_labels = ("Source", "Audit") if multi_label else ("Source",)
    _, source = graph.create_node(labels=source_labels, properties={})
    _, target = graph.create_node(labels=("Target",), properties={})
    _, unlabeled = graph.create_node(labels=(), properties={})
    graph.create_edges_bulk(
        table=pa.table(
            {
                "src_id": [
                    source.node_id,
                    source.node_id,
                    target.node_id,
                    target.node_id,
                    source.node_id,
                    unlabeled.node_id,
                ],
                "dst_id": [
                    target.node_id,
                    target.node_id,
                    source.node_id,
                    target.node_id,
                    unlabeled.node_id,
                    target.node_id,
                ],
                "rel_type": ["OUT", "OUT", "BACK", "SELF", "HIDDEN", "HIDDEN"],
            }
        )
    )
    expected = [
        ("Source", "OUT", "Target"),
        ("Target", "BACK", "Source"),
        ("Target", "SELF", "Target"),
    ]
    if multi_label:
        expected.extend([("Audit", "OUT", "Target"), ("Target", "BACK", "Audit")])
    expected.sort()
    with GraphArrowAgentBackend(app) as reader:
        arrow = _arrow_label_relationships(reader._state.node_table, reader._state.edge_table)
        assert arrow == (None if multi_label else expected)
        overview = reader.call("schema_overview", {})
        assert overview["relationship_types"] == ["BACK", "HIDDEN", "OUT", "SELF"]
        assert [
            (row["source_label"], row["relationship_type"], row["target_label"])
            for row in overview["relationship_patterns"]
        ] == expected


def test_arrow_schema_ignores_missing_and_unlabeled_endpoints() -> None:
    nodes = pa.table({"node_id": [1, 2, 3], "labels": [["Person"], ["Person"], []]})
    edges = pa.table(
        {
            "src_id": [1, 2, 3, 9, 1],
            "dst_id": [2, 2, 1, 1, 9],
            "rel_type": ["LINK", "SELF", "UNLABELED", "MISSING", "MISSING"],
        }
    )
    assert _arrow_label_relationships(nodes, edges) == [
        ("Person", "LINK", "Person"),
        ("Person", "SELF", "Person"),
    ]


def test_concurrent_workflows_discover_once_and_release_the_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("GRAPHARROW_ANA_PERSISTENT_SCHEMA_CACHE_ENABLED", "false")
    root = tmp_path / "shared-schema"
    app = load_grapharrow(root)
    original = GraphArrowAgentBackend._discover_schema
    calls = []

    def discover(
        backend: GraphArrowAgentBackend, node_properties: list[dict[str, Any]] | None
    ) -> dict[str, Any]:
        calls.append(backend._state.snapshot.snapshot_id)
        return original(backend, node_properties)

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
    assert not (root / ".grapharrow-cache").exists()


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
            project = current.call("schema_describe_label", {"label": "Project"})
            assert project["count"] == 1
            assert project["properties"][0]["name"] == "code"
    assert not app._ana_schema_cache._entries


def test_failed_discovery_does_not_poison_or_charge_the_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = load_grapharrow(tmp_path / "failed-schema")
    original = GraphArrowAgentBackend._discover_schema
    budget = AnaResourceBudget()
    calls = 0

    def discover(
        backend: GraphArrowAgentBackend, node_properties: list[dict[str, Any]] | None
    ) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("Simulated discovery failure")
        return original(backend, node_properties)

    monkeypatch.setattr(GraphArrowAgentBackend, "_discover_schema", discover)
    with pytest.raises(RuntimeError, match="discovery failure"):
        GraphArrowAgentBackend(app, resource_budget=budget)
    assert budget.used_bytes == 0
    assert not app._ana_schema_cache._entries
    with GraphArrowAgentBackend(app, resource_budget=budget) as backend:
        assert backend.call("schema_overview", {})["labels"]
    assert calls == 2
    assert budget.used_bytes == 0


def test_local_schema_cache_reuses_exact_tool_results_after_reopening(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "reopened-schema"
    app = load_grapharrow(root)
    calls = [
        ("schema_overview", {}),
        ("schema_describe_label", {"label": "Person"}),
        ("schema_describe_relationship", {"relationship_type": "WORKS"}),
    ]
    with GraphArrowAgentBackend(app) as first:
        expected = [first.call(tool, args) for tool, args in calls]
        snapshot_bytes = first.resource_usage["snapshot_bytes"]
    assert (root / ".grapharrow-cache" / "ana-schema.json").is_file()
    assert not app._ana_schema_cache._entries

    def unexpected_discovery(
        backend: GraphArrowAgentBackend, node_properties: list[dict[str, Any]] | None
    ) -> dict[str, Any]:
        pytest.fail("Reopened database should reuse its persisted schema")

    monkeypatch.setattr(GraphArrowAgentBackend, "_discover_schema", unexpected_discovery)
    with GraphArrowAgentBackend(build_application(root)) as reopened:
        assert [reopened.call(tool, args) for tool, args in calls] == expected
        assert reopened.resource_usage["snapshot_bytes"] == snapshot_bytes


@pytest.mark.parametrize(
    "damage", ["malformed", "checksum", "legacy-version", "unsupported-version", "unwritable"]
)
def test_local_schema_cache_falls_back_when_unusable(tmp_path: Path, damage: str) -> None:
    root = tmp_path / "unusable-schema"
    app = load_grapharrow(root)
    with GraphArrowAgentBackend(app) as first:
        expected = deepcopy(first._schema)

    cache = root / ".grapharrow-cache" / "ana-schema.json"
    original = cache.read_bytes()
    if damage == "malformed":
        cache.write_text("{", encoding="utf-8")
    elif damage == "unwritable":
        cache.unlink()
        cache.mkdir()
    else:
        payload = json.loads(original)
        if damage == "checksum":
            payload["schema"]["labels"] = ["Wrong"]
        elif damage == "legacy-version":
            payload["version"] = 1
        else:
            payload["version"] += 1
        cache.write_text(json.dumps(payload), encoding="utf-8")

    with GraphArrowAgentBackend(build_application(root)) as reopened:
        assert reopened._schema == expected
    if damage != "unwritable":
        assert cache.read_bytes() == original


@pytest.mark.parametrize("corrupt_cache", [False, True])
def test_reopened_schema_reuses_only_verified_node_properties_after_edge_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, corrupt_cache: bool
) -> None:
    root = tmp_path / "edge-only"
    app = load_grapharrow(root)
    with GraphArrowAgentBackend(app) as first:
        alice = first._label_ids["Person"][0]
        acme = first._label_ids["Company"][0]
        old = first._state.snapshot
    app.use_cases.graph.create_edge(
        src_id=alice, dst_id=acme, rel_type="VISITS", properties={"times": 2}
    )
    current = app.repositories.snapshot.load_manifest()
    assert current.nodes == old.nodes
    assert current.edges != old.edges
    uncached_app = build_application(
        root, settings=GraphArrowSettings(ana_persistent_schema_cache_enabled=False)
    )
    with GraphArrowAgentBackend(uncached_app) as uncached:
        expected_schema = deepcopy(uncached._schema)
        expected_overview = uncached.call("schema_overview", {})
        relationship_args = {"relationship_type": "VISITS"}
        expected_relationship = uncached.call("schema_describe_relationship", relationship_args)
    if corrupt_cache:
        path = root / ".grapharrow-cache" / "ana-schema.json"
        payload = json.loads(path.read_bytes())
        payload["schema"]["node_properties"] = []
        path.write_text(json.dumps(payload), encoding="utf-8")

    original = GraphArrowAgentBackend._discover_node_properties
    rebuilt = []

    def discover(backend: GraphArrowAgentBackend, labels: list[list[str]]) -> list[dict[str, Any]]:
        rebuilt.append(backend._state.snapshot.snapshot_id)
        return original(backend, labels)

    monkeypatch.setattr(GraphArrowAgentBackend, "_discover_node_properties", discover)
    with GraphArrowAgentBackend(build_application(root)) as reopened:
        assert reopened._schema == expected_schema
        assert reopened.call("schema_overview", {}) == expected_overview
        assert (
            reopened.call("schema_describe_relationship", relationship_args)
            == expected_relationship
        )
    assert rebuilt == ([current.snapshot_id] if corrupt_cache else [])


@pytest.mark.parametrize("cache_file", ["ana-schema.json", "ana-edge-schema.json"])
def test_slow_schema_cache_write_allows_commit_and_vacuum(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cache_file: str
) -> None:
    root = tmp_path / "concurrent-schema"
    reader_app = load_grapharrow(root)
    writer_app = build_application(root)
    cache = root / ".grapharrow-cache" / cache_file
    writing = Event()
    resume = Event()
    replace = reader_app.adapters.filesystem.replace_json

    def pause_replace(path: str | Path, payload: object) -> None:
        if path == cache:
            writing.set()
            assert resume.wait(timeout=20)
        replace(path, payload)

    monkeypatch.setattr(reader_app.adapters.filesystem, "replace_json", pause_replace)

    def commit_and_vacuum() -> None:
        graph = writer_app.use_cases.graph
        _, node = graph.create_node(labels=("Project",), properties={"code": "new"})
        graph.create_edge(src_id=node.node_id, dst_id=node.node_id, rel_type="LINK", properties={})
        writer_app.use_cases.admin.vacuum()
        with GraphArrowAgentBackend(writer_app) as current:
            assert current.call("schema_describe_label", {"label": "Project"})["count"] == 1

    with ThreadPoolExecutor(max_workers=2) as pool:
        old_workflow = pool.submit(GraphArrowAgentBackend, reader_app)
        try:
            assert writing.wait(timeout=10)
            pool.submit(commit_and_vacuum).result(timeout=10)
            current_cache = cache.read_bytes()
        finally:
            resume.set()
        with old_workflow.result(timeout=10) as old:
            assert "Project" not in old._schema["labels"]
            assert "LINK" not in old._schema["relationship_types"]
            assert old.call("schema_describe_label", {"label": "Person"})["count"] == 6

    assert cache.read_bytes() != current_cache
    with GraphArrowAgentBackend(build_application(root)) as reopened:
        assert reopened.call("schema_describe_label", {"label": "Project"})["count"] == 1
        assert "LINK" in reopened._schema["relationship_types"]
    assert cache.read_bytes() == current_cache


def test_ana_schema_reuses_unchanged_edge_ranges_after_reopen(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "edge-ranges"
    app = build_application(root)
    graph = app.use_cases.graph
    monkeypatch.setattr(ana_edge_statistics, "_EDGE_ID_RANGE_SIZE", 2)
    _, source = graph.create_node(labels=("Source",), properties={})
    _, target = graph.create_node(labels=("Target",), properties={})
    for value in (1, 2):
        graph.create_edge(
            src_id=source.node_id, dst_id=target.node_id, rel_type="LINK", properties={"p": value}
        )
    with GraphArrowAgentBackend(app) as original:
        assert original._schema["relationship_properties"][0]["mandatory"] is True

    graph.create_edge(src_id=target.node_id, dst_id=source.node_id, rel_type="LINK", properties={})
    discovered: list[int] = []
    collect = ana_edge_statistics._collect_properties

    def record(rows: pa.Table) -> ana_edge_statistics._RangeStats:
        discovered.extend(rows["edge_id"].to_pylist())
        return collect(rows)

    monkeypatch.setattr(ana_edge_statistics, "_collect_properties", record)
    with GraphArrowAgentBackend(build_application(root)) as reopened:
        assert reopened._schema["relationship_properties"][0]["mandatory"] is False
        assert reopened._schema["label_relationships"] == [
            {"source_label": "Source", "relationship_type": "LINK", "target_label": "Target"},
            {"source_label": "Target", "relationship_type": "LINK", "target_label": "Source"},
        ]
    assert discovered == [3]
