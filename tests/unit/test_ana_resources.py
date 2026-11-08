"""Retained ANA quotas must be atomic, recoverable and shared across workflows."""

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from fastapi.testclient import TestClient

from grapharrow.ana.handles import EntityPayload, EntityReference
from grapharrow.ana.sessions import BackendSessions, SessionConfig
from grapharrow.config import GraphArrowSettings
from grapharrow.interfaces.rest import create_rest_app
from grapharrow.use_cases.ana import GraphArrowAgentBackend
from grapharrow.use_cases.ana_resources import AnaQuotaExceeded, AnaResourceBudget, retained_size
from tests.ana_support import load_grapharrow


@pytest.fixture
def application(tmp_path: Path) -> Any:
    return load_grapharrow(tmp_path / "quota")


def page(backend: Any, handle: str) -> Any:
    return backend.call("fetch", {"from": handle})["rows"]


def test_accounting_includes_arrow_buffers_references_and_cycles() -> None:
    table = pa.table({"value": ["x" * 1000]})
    node = EntityPayload(labels=["Person"], properties={"name": "Alice"})
    node.reference = EntityReference("workflow", "node")
    payload = [node, table]
    size = retained_size(payload)
    payload.append(payload)
    assert retained_size(payload) >= size
    assert size > table.get_total_buffer_size() + retained_size(node.reference)


def test_handle_quota_keeps_existing_handles_and_schema_usable(application: Any) -> None:
    application.settings.ana_max_handles_per_session = 1
    with GraphArrowAgentBackend(application) as backend:
        first = backend.call("node_search", {"label": "Person", "value": "Alice"})["handle"]
        before = backend.resource_usage
        with pytest.raises(AnaQuotaExceeded, match="handle quota"):
            backend.call("count_nodes", {"label": "Person"})
        assert backend.resource_usage == before
        assert page(backend, first) == [["Alice"]]
        assert backend.call("schema_overview", {})["labels"]


@pytest.mark.parametrize("limit", [None, 0, "0"])
def test_handle_count_limit_is_optional_and_zero_disables_it(application: Any, limit: Any) -> None:
    settings = GraphArrowSettings(ana_max_handles_per_session=limit)
    assert settings.ana_max_handles_per_session is None
    application.settings.ana_max_handles_per_session = settings.ana_max_handles_per_session
    with GraphArrowAgentBackend(application) as backend:
        for index in range(130):
            result = backend.call("count_nodes", {"label": "Person"})
            assert result["handle"] == f"h{index + 1}"
        assert backend.resource_usage["handles"] == 130
        assert backend.resource_usage["bytes"] < application.settings.ana_max_bytes_per_session


def test_row_quota_cannot_be_bypassed_with_allow_large(application: Any) -> None:
    application.settings.ana_max_rows_per_session = 4
    with GraphArrowAgentBackend(application) as backend:
        before = backend.resource_usage
        with pytest.raises(AnaQuotaExceeded, match="row quota"):
            backend.call("node_scan", {"label": "Person", "limit": 6, "allow_large": True})
        assert backend.resource_usage == before
        assert not backend._handle_store.handles
        result = backend.call("node_scan", {"label": "Person", "limit": 4})
        with pytest.raises(AnaQuotaExceeded, match="row quota"):
            backend.call(
                "project",
                {"from": result["handle"], "select": [{"var": "person", "property": "name"}]},
            )
        assert len(page(backend, result["handle"])) == 4


def test_byte_quota_rejects_state_and_rolls_back_lineage(application: Any) -> None:
    with GraphArrowAgentBackend(application) as backend:
        before = backend.resource_usage
        backend._resources.limits = replace(
            backend._resources.limits, max_bytes=before["bytes"] + 2048
        )
        with pytest.raises(AnaQuotaExceeded, match="byte quota"):
            backend.call("node_scan", {"label": "Person", "limit": 6})
        assert backend.resource_usage == before
        assert not backend._handle_store.handles
        assert not backend._handle_store.lineage


def test_lineage_is_included_after_store_and_failure_is_atomic(application: Any) -> None:
    with GraphArrowAgentBackend(application) as backend:
        before = backend.resource_usage
        estimate = retained_size(([{"person_count": 6}], None, "table", ["person_count"], None))
        backend._resources.limits = replace(
            backend._resources.limits, max_bytes=before["bytes"] + estimate + 64
        )
        with pytest.raises(AnaQuotaExceeded, match="byte quota"):
            backend.call("count_nodes", {"label": "Person"})
        assert backend.resource_usage == before
        assert not backend._handle_store.handles
        assert not backend._handle_store.lineage


def test_multistep_set_failure_removes_intermediate_handles(application: Any) -> None:
    application.settings.ana_max_handles_per_session = 4
    with GraphArrowAgentBackend(application) as backend:
        handles = [
            backend.call("node_search", {"label": "Person", "value": name})["handle"]
            for name in ("Alice", "Bob", "Carol")
        ]
        before, lineage = backend.resource_usage, dict(backend._handle_store.lineage)
        with pytest.raises(AnaQuotaExceeded, match="handle quota"):
            backend.call(
                "entity_set_operation",
                {
                    "op": "union",
                    "operands": [{"handle": handle, "var": "person"} for handle in handles],
                },
            )
        assert set(backend._handle_store.handles) == set(handles)
        assert backend._handle_store.lineage == lineage
        assert backend.resource_usage == before
        assert page(backend, handles[0]) == [["Alice"]]


def test_snapshot_admission_and_close_release_shared_budget(application: Any) -> None:
    application.settings.ana_adjacency_index_max_bytes_per_session = 0
    with GraphArrowAgentBackend(application) as probe:
        size = probe.resource_usage["bytes"]
    budget = AnaResourceBudget(size * 2 - 1)
    with GraphArrowAgentBackend(application, resource_budget=budget) as backend:
        with pytest.raises(AnaQuotaExceeded, match="server byte capacity"):
            GraphArrowAgentBackend(application, resource_budget=budget)
        assert budget.used_bytes == backend.resource_usage["bytes"]
    assert budget.used_bytes == 0
    assert backend.resource_usage == {"handles": 0, "rows": 0, "bytes": 0, "snapshot_bytes": 0}
    backend.close()
    assert budget.used_bytes == 0
    with GraphArrowAgentBackend(application, resource_budget=budget):
        assert budget.used_bytes > 0
    assert budget.used_bytes == 0


def test_concurrent_workflows_cannot_overbook_shared_capacity(application: Any) -> None:
    application.settings.ana_adjacency_index_max_bytes_per_session = 0
    with GraphArrowAgentBackend(application) as probe:
        size = probe.resource_usage["bytes"]
    budget = AnaResourceBudget(size * 2 + 1024)

    def create(_: int) -> Any:
        try:
            return GraphArrowAgentBackend(application, resource_budget=budget)
        except AnaQuotaExceeded:
            return None

    with ThreadPoolExecutor(max_workers=4) as executor:
        backends = [backend for backend in executor.map(create, range(8)) if backend is not None]
    try:
        assert len(backends) == 2
        assert budget.used_bytes <= budget.max_bytes
    finally:
        for backend in backends:
            backend.close()
    assert budget.used_bytes == 0


def test_snapshot_larger_than_session_quota_is_rejected_without_charge(application: Any) -> None:
    application.settings.ana_max_bytes_per_session = 1
    budget = AnaResourceBudget()
    with pytest.raises(AnaQuotaExceeded, match="byte quota"):
        GraphArrowAgentBackend(application, resource_budget=budget)
    assert budget.used_bytes == 0


def test_idle_expiry_releases_bytes_before_admitting_replacement(application: Any) -> None:
    now = [0.0]
    budget = AnaResourceBudget()
    sessions = BackendSessions(
        lambda store: GraphArrowAgentBackend(
            application, handle_store=store, resource_budget=budget
        ),
        SessionConfig(max_sessions=1, idle_timeout_s=1),
        clock=lambda: now[0],
    )
    try:
        sessions.create(owner_id="reader")
        occupied = budget.used_bytes
        budget.max_bytes = occupied + 1024
        now[0] = 2
        sessions.create(owner_id="reader")
        assert 0 < budget.used_bytes <= occupied + 1024
    finally:
        sessions.close()
    assert budget.used_bytes == 0


def test_rejected_fetch_restores_previous_page_and_usage(application: Any) -> None:
    with GraphArrowAgentBackend(application) as backend:
        result = backend.call("node_scan", {"label": "Person", "limit": 6})
        backend.call("fetch", {"from": result["handle"], "limit": 1})
        before, previous_page = backend.resource_usage, backend.last_fetch
        backend._resources.limits = replace(backend._resources.limits, max_bytes=before["bytes"])
        with pytest.raises(AnaQuotaExceeded, match="byte quota"):
            backend.call("fetch", {"from": result["handle"], "limit": 6})
        assert backend.resource_usage == before
        assert backend.last_fetch is previous_page


def test_http_quota_errors_are_429_and_cleanup_restores_capacity(
    application: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    auth = tmp_path / "clients.json"
    auth.write_text(
        json.dumps({"clients": [{"id": "reader", "api_key": "a" * 32, "profile": "readonly"}]})
    )
    monkeypatch.setenv("ANA_AUTH_FILE", str(auth))
    app = create_rest_app(
        application.layout.root, settings=GraphArrowSettings(ana_max_handles_per_session=1)
    )
    headers = {"Authorization": "Bearer " + "a" * 32}
    with TestClient(app) as client:
        response = client.post("/sessions", headers=headers)
        assert response.status_code == 201
        headers["X-ANA-Session"] = response.json()["session_id"]
        body = {"tool": "count_nodes", "args": {"label": "Person"}}
        assert client.post("/tools/call", headers=headers, json=body).status_code == 200
        assert client.post("/tools/call", headers=headers, json=body).status_code == 429
        budget = app.state.ana_resource_budget
        budget.max_bytes = budget.used_bytes + 1
        assert client.post("/sessions", headers=headers).status_code == 429
        assert client.delete("/sessions/current", headers=headers).status_code == 204
        assert budget.used_bytes == 0
        assert client.post("/sessions", headers=headers).status_code == 201
    assert app.state.ana_resource_budget.used_bytes == 0
