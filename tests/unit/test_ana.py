"""Contract and native execution tests; no model, Neo4j service or network."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from fastapi.testclient import TestClient
from neo4j import GraphDatabase  # noqa: E402

from grapharrow.ana.sessions import SessionNotFound
from grapharrow.app import GraphArrowApplication, build_application  # noqa: E402
from grapharrow.config import GraphArrowSettings  # noqa: E402
from grapharrow.interfaces.rest import create_rest_app  # noqa: E402
from grapharrow.use_cases.ana import (  # noqa: E402
    GraphArrowAgentBackend,
)


@pytest.fixture
def application(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> GraphArrowApplication:
    def forbidden_driver(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("The GraphArrow ANA path must not create a Neo4j driver")

    monkeypatch.setattr(GraphDatabase, "driver", forbidden_driver)
    app = build_application(tmp_path / "db")
    _, nodes = app.use_cases.graph.create_nodes_bulk(
        table=pa.Table.from_pylist(
            [{"name": f"sensor-{n:02d}", "id": "duplicate", "reading": n} for n in range(25)]
        ),
        default_labels=("Device", "Asset"),
    )
    _, zone = app.use_cases.graph.create_node(labels=("Zone",), properties={"name": "North"})
    app.use_cases.graph.create_edges_bulk(
        table=pa.Table.from_pylist(
            [{"src_id": n.node_id, "dst_id": zone.node_id, "rel_type": "IN_ZONE"} for n in nodes]
        )
    )
    return app


@pytest.fixture
def backend(application: GraphArrowApplication) -> Any:
    with GraphArrowAgentBackend(application) as graph:
        yield graph


def _fetch(graph: GraphArrowAgentBackend, handle: dict[str, Any], **args: Any) -> dict[str, Any]:
    return graph.call("fetch", {"from": handle["handle"], **args})


def test_native_schema_search_expand_project_and_fetch(backend: Any) -> None:
    overview = backend.call("schema_overview", {})
    assert {row["label"]: row["count"] for row in overview["labels"]} == {
        "Asset": 25,
        "Device": 25,
        "Zone": 1,
    }
    schema = backend.call("schema_describe_relationship", {"relationship_type": "IN_ZONE"})
    assert schema["relationship_type"] == "IN_ZONE"
    zone = backend.call("node_search", {"label": "Zone", "value": "North", "as": "zone"})
    devices = backend.call(
        "expand",
        {
            "from": zone["handle"],
            "source": "zone",
            "relationship_type": "IN_ZONE",
            "direction": "in",
            "target_label": "Device",
            "as": "device",
            "page_size": 1,
        },
    )
    assert devices["matched_count"] == 25
    assert devices["input_count"] == 1
    projected = backend.call(
        "project",
        {
            "from": devices["handle"],
            "select": [{"var": "device", "property": "name", "alias": "name"}],
            "order_by": [{"field": "name", "direction": "asc"}],
        },
    )
    page = _fetch(backend, projected, limit=25)
    assert page["contract_version"] == "ana-fetch-v1"
    assert page["rows"] == [[f"sensor-{n:02d}"] for n in range(25)]
    assert page["returned_count"] == 25
    assert len(page["preview"]) == 20 and page["preview_truncated"] is True
    assert page["truncated"] is False and page["next_offset"] is None
    recap = backend.call("handle_recap", {"from": projected["handle"]})
    assert [step["tool"] for step in recap["lineage"]] == ["node_search", "expand", "project"]


@pytest.mark.parametrize(
    "mode",
    [
        "overview",
        "overview_light",
        "targeted_first",
        "overview_expand_first",
        "overview_light_expand_first",
    ],
)
def test_catalog_preserves_default_and_opt_in_tools(
    application: GraphArrowApplication, mode: str
) -> None:
    with GraphArrowAgentBackend(application, schema_entry=mode) as graph:
        default = {spec["function"]["name"] for spec in graph.tool_specs()}
        planning = {
            spec["function"]["name"] for spec in graph.tool_specs(enable_planning_tools=True)
        }
        assert len(default) == 35
        assert planning - default == {"draft_tool_plan", "validate_tool_plan"}
        assert {"schema_search", "expand", "pattern_query", "fetch"} <= default
        with pytest.raises(ValueError, match="not implemented"):
            graph.call("not_a_tool", {})


def test_native_identity_separates_equal_business_ids_and_survives_projection(backend: Any) -> None:
    nodes = backend.call("node_scan", {"label": "Device", "limit": 25, "as": "device"})
    for args in [{"var": "device"}, {}]:
        counted = backend.call("count_handle", {"from": nodes["handle"], "distinct": True, **args})
        assert _fetch(backend, counted)["rows"] == [[25]]
    projected = backend.call(
        "project",
        {
            "from": nodes["handle"],
            "select": [{"var": "device", "property": "id", "alias": "id"}],
            "keep_entities": True,
            "distinct": True,
        },
    )
    counted = backend.call(
        "count_handle", {"from": projected["handle"], "var": "device", "distinct": True}
    )
    assert _fetch(backend, counted)["rows"] == [[25]]
    assert _fetch(backend, projected)["rows"] == [["duplicate"]] * 25


def test_set_operations_and_filter_preserve_entity_identity(backend: Any) -> None:
    nodes = backend.call("node_scan", {"label": "Device", "limit": 25, "as": "device"})
    left = backend.call(
        "filter",
        {"from": nodes["handle"], "var": "device", "property": "reading", "op": "lt", "value": 10},
    )
    right = backend.call(
        "filter",
        {"from": nodes["handle"], "var": "device", "property": "reading", "op": "gte", "value": 5},
    )
    for op, expected in [("union", 25), ("intersect", 5), ("difference", 5)]:
        combined = backend.call(
            "entity_set_operation",
            {
                "op": op,
                "operands": [
                    {"handle": left["handle"], "var": "device"},
                    {"handle": right["handle"], "var": "device"},
                ],
                "as": "device",
            },
        )
        count = backend.call(
            "count_handle", {"from": combined["handle"], "var": "device", "distinct": True}
        )
        assert _fetch(backend, count)["rows"] == [[expected]]


def test_group_join_compare_and_scalar_operations(backend: Any) -> None:
    first = backend.call("node_search", {"label": "Device", "value": "sensor-00", "as": "a"})
    second = backend.call("node_search", {"label": "Device", "value": "sensor-01", "as": "b"})
    compared = backend.call(
        "compare",
        {
            "left": first["handle"],
            "left_var": "a",
            "right": second["handle"],
            "right_var": "b",
            "property": "reading",
            "op": "lt",
        },
    )
    assert _fetch(backend, compared)["rows"] == [[True]]
    nodes = backend.call("node_scan", {"label": "Device", "limit": 25, "as": "device"})
    grouped = backend.call(
        "group_handle",
        {
            "from": nodes["handle"],
            "group_by": [{"var": "device", "property": "id", "alias": "id"}],
            "metrics": [{"op": "count_distinct", "var": "device", "alias": "total"}],
        },
    )
    assert _fetch(backend, grouped)["rows"] == [["duplicate", 25]]
    joined = backend.call("join_handles", {"left": nodes["handle"], "right": nodes["handle"]})
    assert joined["matched_count"] == 25
    count = backend.call("count_nodes", {"label": "Device"})
    scalar = backend.call(
        "scalar_compute",
        {"inputs": [{"handle": count["handle"], "column": "device_count"}] * 2, "op": "sum"},
    )
    assert _fetch(backend, scalar)["rows"] == [[50]]


def test_expansion_does_not_reuse_relationships(backend: Any) -> None:
    zone = backend.call("node_search", {"label": "Zone", "value": "North", "as": "zone"})
    devices = backend.call(
        "expand",
        {
            "from": zone["handle"],
            "source": "zone",
            "relationship_type": "IN_ZONE",
            "direction": "in",
            "target_label": "Device",
            "as": "device",
        },
    )
    back = backend.call(
        "expand",
        {
            "from": devices["handle"],
            "source": "device",
            "relationship_type": "IN_ZONE",
            "direction": "out",
            "target_label": "Zone",
            "as": "back",
        },
    )
    assert back["matched_count"] == 0
    both = backend.call(
        "expand",
        {
            "from": zone["handle"],
            "source": "zone",
            "relationship_type": "IN_ZONE",
            "direction": "both",
            "target_label": "Device",
            "limit": 3,
        },
    )
    assert both["matched_count"] == 3 and both["limited"] is True


@pytest.mark.parametrize("value", [True, False, None, "1"])
def test_search_does_not_coerce_property_types(backend: Any, value: Any) -> None:
    result = backend.call("node_search", {"label": "Device", "property": "reading", "value": value})
    assert result["matched_count"] == 0


def test_projected_search_uses_canonical_labels_and_order(backend: Any) -> None:
    first, second = backend._label_ids["Device"][:2]
    [zone] = backend._label_ids["Zone"]
    backend._property_tables["Device"] = pa.table(
        {"node_id": [second, zone, first, zone], "name": ["match", "match", "match", "stale"]}
    )

    result = backend.call("node_search", {"label": "Device", "value": "match", "limit": 2})
    assert result["matched_count"] == 2
    assert _fetch(backend, result)["preview"] == [["sensor-00"], ["sensor-01"]]
    assert backend.call("node_search", {"label": "Device", "value": "stale"})["matched_count"] == 0


@pytest.mark.parametrize("direction", ["out", "in", "both"])
def test_limited_expand_of_hubs_preserves_rows_and_full_union_count(
    tmp_path: Path, direction: str
) -> None:
    app = build_application(tmp_path / "hub-expand")
    _, hubs = app.use_cases.graph.create_nodes_bulk(
        table=pa.table({"name": [f"hub-{index}" for index in range(8)]}),
        default_labels=("Hub",),
    )
    _, targets = app.use_cases.graph.create_nodes_bulk(
        table=pa.table({"name": [f"target-{index}" for index in range(80)]}),
        default_labels=("Target",),
    )
    edges = []
    for hub in hubs:
        # The first candidate has the wrong target label.
        edges.append({"src_id": hub.node_id, "dst_id": hub.node_id, "rel_type": "LINK"})
        for index in range(160):
            source, target = hub.node_id, targets[index % len(targets)].node_id
            if direction == "in" or direction == "both" and index % 2:
                source, target = target, source
            edges.append({"src_id": source, "dst_id": target, "rel_type": "LINK"})
    app.use_cases.graph.create_edges_bulk(table=pa.Table.from_pylist(edges))
    with GraphArrowAgentBackend(app) as backend:
        seed = backend.call("node_scan", {"label": "Hub", "limit": 8, "as": "hub"})
        result = backend.call(
            "expand",
            {
                "from": seed["handle"],
                "source": "hub",
                "relationship_type": "LINK",
                "target_label": "Target",
                "direction": direction,
                "as": "target",
                "limit": 10,
            },
        )
        assert _fetch(backend, result)["rows"] == [[f"target-{index}"] for index in range(10)]
        assert result["limited"] is True and result["input_count"] == 8
        combined = backend.call(
            "entity_set_operation",
            {
                "op": "union",
                "operands": [{"handle": result["handle"], "var": "target"}] * 2,
                "as": "target",
            },
        )
        count = backend.call(
            "count_handle", {"from": combined["handle"], "var": "target", "distinct": True}
        )
        assert _fetch(backend, count)["rows"] == [[80]]


def test_snapshot_stays_stable_across_commit_compaction_and_vacuum(
    application: GraphArrowApplication, backend: Any
) -> None:
    application.use_cases.graph.create_node(labels=("Device",), properties={"name": "new"})
    application.use_cases.admin.compact()
    application.use_cases.admin.vacuum()
    count = backend.call("count_nodes", {"label": "Device"})
    assert _fetch(backend, count)["rows"] == [[25]]
    assert backend.call("node_search", {"label": "Device", "value": "new"})["matched_count"] == 0
    with GraphArrowAgentBackend(application) as fresh:
        assert _fetch(fresh, fresh.call("count_nodes", {"label": "Device"}))["rows"] == [[26]]


@pytest.mark.parametrize(
    "arguments", [{"limit": True}, {"offset": -1}, {"limit": 0}, {"limit": "2"}]
)
def test_fetch_rejects_invalid_pages_without_losing_the_workflow(
    backend: Any, arguments: dict
) -> None:
    count = backend.call("count_nodes", {"label": "Device"})
    with pytest.raises(ValueError):
        _fetch(backend, count, **arguments)
    assert _fetch(backend, count)["rows"] == [[25]]


def test_validation_close_and_workflow_isolation(application: GraphArrowApplication) -> None:
    one = GraphArrowAgentBackend(application)
    two = GraphArrowAgentBackend(application)
    handle = one.call("count_nodes", {"label": "Device"})
    assert handle["handle"] == "h1"
    with pytest.raises(ValueError, match="Unknown handle"):
        _fetch(two, handle)
    second = two.call("count_nodes", {"label": "Zone"})
    assert second["handle"] == "h1"
    assert _fetch(one, handle)["rows"] == [[25]]
    assert _fetch(two, second)["rows"] == [[1]]
    assert one._owner != two._owner
    with pytest.raises(ValueError, match="unsupported parameter"):
        one.call("count_nodes", {"label": "Device", "invented": "not printed"})
    with pytest.raises(ValueError, match="conflicting"):
        one.call("fetch", {"from": handle["handle"], "from_": handle["handle"]})
    one.close()
    one.close()
    assert one._handle_store.handles == {} and one._snapshot_state is None
    with pytest.raises(RuntimeError, match="closed"):
        _fetch(one, handle)
    two.close()


@pytest.fixture
def http_app(
    application: GraphArrowApplication, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Any:
    auth_file = tmp_path / "clients.json"
    auth_file.write_text(
        json.dumps(
            {
                "clients": [
                    {"id": "one", "api_key": "a" * 32, "profile": "readonly"},
                    {"id": "two", "api_key": "b" * 32, "profile": "readwrite"},
                ]
            }
        )
    )
    monkeypatch.setenv("ANA_AUTH_FILE", str(auth_file))
    return create_rest_app(
        application.layout.root,
        settings=GraphArrowSettings(ana_enabled=True, ana_max_sessions=2),
    )


def _session(client: TestClient, key: str = "a") -> dict[str, str]:
    headers = {"Authorization": f"Bearer {key * 32}"}
    response = client.post("/sessions", headers=headers)
    assert response.status_code == 201, response.text
    assert response.headers["Cache-Control"] == "no-store"
    assert response.headers["X-ANA-Session"] == response.json()["session_id"]
    return {**headers, "X-ANA-Session": response.json()["session_id"]}


def test_http_session_header_round_trip_and_error_isolation(http_app: Any) -> None:
    with TestClient(http_app) as client:
        headers = _session(client)
        body = {"tool": "count_nodes", "args": {"label": "Device"}}
        response = client.post("/tools/call", headers=headers, json=body)
        assert response.status_code == 200
        assert response.headers["X-ANA-Session"] == headers["X-ANA-Session"]
        assert response.headers["Cache-Control"] == "no-store"
        invalid = {"tool": "count_nodes", "args": {"label": "Device", "invented": True}}
        response = client.post("/tools/call", headers=headers, json=invalid)
        assert response.status_code == 400
        assert response.headers["X-ANA-Session"] == headers["X-ANA-Session"]
        foreign = {**headers, "Authorization": "Bearer " + "b" * 32}
        response = client.post("/tools/call", headers=foreign, json=body)
        assert response.status_code == 404
        assert "X-ANA-Session" not in response.headers
        response = client.delete("/sessions/current", headers=headers)
        assert response.status_code == 204
        assert response.headers["X-ANA-Session"] == headers["X-ANA-Session"]
        response = client.post("/tools/call", headers=headers, json=body)
        assert response.status_code == 404
        assert "X-ANA-Session" not in response.headers


def test_short_handles_resolve_only_with_their_session_context(http_app: Any) -> None:
    with TestClient(http_app) as client:
        first, second = _session(client), _session(client)
        assert first["X-ANA-Session"] != second["X-ANA-Session"]
        for headers, label in ((first, "Device"), (second, "Zone")):
            response = client.post(
                "/tools/call",
                headers=headers,
                json={"tool": "count_nodes", "args": {"label": label}},
            )
            assert response.status_code == 200
            assert response.json()["result"]["handle"] == "h1"
        for headers, expected in ((first, 25), (second, 1)):
            response = client.post(
                "/tools/call", headers=headers, json={"tool": "fetch", "args": {"from": "h1"}}
            )
            assert response.status_code == 200
            assert response.json()["result"]["rows"] == [[expected]]
        alien = {**first, "Authorization": "Bearer " + "b" * 32}
        response = client.post(
            "/tools/call", headers=alien, json={"tool": "fetch", "args": {"from": "h1"}}
        )
        assert response.status_code == 404
        assert client.delete("/sessions/current", headers=first).status_code == 204
        assert (
            client.post(
                "/tools/call", headers=first, json={"tool": "fetch", "args": {"from": "h1"}}
            ).status_code
            == 404
        )
        replacement = _session(client)
        assert replacement["X-ANA-Session"] != first["X-ANA-Session"]
        assert (
            client.post(
                "/tools/call", headers=replacement, json={"tool": "fetch", "args": {"from": "h1"}}
            ).status_code
            == 400
        )


def test_http_paginated_workflow_and_rest_coexist(http_app: Any) -> None:
    with TestClient(http_app) as client:
        headers = _session(client)
        assert client.get("/health").json() == {"status": "ok"}
        assert client.get("/v1/health").json()["ok"] is True
        specs = client.get("/tools/specs", headers=headers).json()["tools"]
        names = {spec["function"]["name"] for spec in specs}
        assert len(names) == 35 and {"fetch", "node_scan", "count_nodes"} <= names
        sampled = client.post(
            "/tools/call",
            headers=headers,
            json={"tool": "node_scan", "args": {"label": "Device", "limit": 25}},
        ).json()["result"]
        rows = []
        offset = 0
        while True:
            response = client.post(
                "/tools/call",
                headers=headers,
                json={
                    "tool": "fetch",
                    "args": {"from": sampled["handle"], "limit": 10, "offset": offset},
                },
            )
            assert response.status_code == 200, response.text
            page = response.json()["result"]
            rows.extend(page["rows"])
            if page["next_offset"] is None:
                break
            offset = page["next_offset"]
        assert len(rows) == 25
        assert client.delete("/sessions/current", headers=headers).status_code == 204
        assert (
            client.post(
                "/tools/call",
                headers=headers,
                json={"tool": "fetch", "args": {"from": sampled["handle"]}},
            ).status_code
            == 404
        )
    assert http_app.state.ana_sessions._closed is True


def test_http_auth_capacity_ownership_and_repair(http_app: Any) -> None:
    with TestClient(http_app) as client:
        assert client.get("/tools").status_code == 401
        one = _session(client)
        two = _session(client, "b")
        assert client.post("/sessions", headers=one).status_code == 429
        foreign = {**one, "Authorization": two["Authorization"]}
        body = {"tool": "count_nodes", "args": {"label": "Device"}}
        assert client.post("/tools/call", headers=foreign, json=body).status_code == 404
        assert client.delete("/sessions/current", headers=foreign).status_code == 404
        assert (
            client.post(
                "/tools/call", headers={"Authorization": one["Authorization"]}, json=body
            ).status_code
            == 401
        )
        assert (
            client.post(
                "/tools/call", headers=one, json={"tool": "not_a_tool", "args": {}}
            ).status_code
            == 403
        )
        assert (
            client.post(
                "/tools/call",
                headers=one,
                json={"tool": "count_nodes", "args": {"label": "Device", "unexpected": True}},
            ).status_code
            == 400
        )
        assert client.post("/tools/call", headers=one, json=body).status_code == 200
        assert client.post("/tools/call", headers=one, json={"args": {}}).status_code == 422
        assert client.get("/tools?status=invalid", headers=one).status_code == 400
        assert client.delete("/sessions/current", headers=one).status_code == 204
        _session(client)


def test_ana_is_enabled_by_default_and_unconfigured_auth_is_closed(
    application: GraphArrowApplication, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("ANA_AUTH_FILE", raising=False)
    with TestClient(
        create_rest_app(application.layout.root, settings=GraphArrowSettings(ana_enabled=False))
    ) as client:
        assert client.get("/tools").status_code == 404
    with TestClient(create_rest_app(application.layout.root)) as client:
        assert client.get("/health").status_code == 200
        assert client.get("/tools").status_code == 503
        assert client.post("/sessions").status_code == 503


def test_shared_session_expiry_releases_backend(http_app: Any) -> None:
    manager = http_app.state.ana_sessions
    now = [0.0]
    manager._clock = lambda: now[0]
    first = manager.create(owner_id="one")
    owned = manager._sessions[first].backend
    now[0] = 1801.0
    second = manager.create(owner_id="one")
    assert owned._closed
    with pytest.raises(SessionNotFound):
        manager.call(first, "count_nodes", {"label": "Device"}, owner_id="one")
    manager.delete(second, owner_id="one")
    manager.close()


def test_conflicting_root_prefix_is_rejected(application: GraphArrowApplication) -> None:
    with pytest.raises(ValueError, match="non-root REST prefix"):
        create_rest_app(
            application.layout.root, settings=GraphArrowSettings(ana_enabled=True, rest_prefix="")
        )


@pytest.mark.parametrize(
    "flag,environment,expected",
    [("--ana", "false", True), ("--no-ana", "true", False), (None, "true", True)],
)
def test_server_cli_ana_selection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    flag: str | None,
    environment: str,
    expected: bool,
) -> None:
    import sys

    from grapharrow import server

    monkeypatch.setenv("GRAPHARROW_ANA_ENABLED", environment)
    monkeypatch.delenv("ANA_AUTH_FILE", raising=False)
    captured = []
    monkeypatch.setattr(server.uvicorn, "run", lambda app, **kwargs: captured.append(app))
    args = ["grapharrow-server", "--path", str(tmp_path / "server-db")]
    if flag:
        args.append(flag)
    monkeypatch.setattr(sys, "argv", args)
    assert server.main() == 0
    app = captured[0]
    assert app.state.grapharrow.settings.ana_enabled is expected
    assert hasattr(app.state, "ana_sessions") is expected
    if expected:
        app.state.ana_sessions.close()
