import asyncio
import json
from threading import Event

from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient

from grapharrow.config import GraphArrowSettings
from grapharrow.core.errors import WriteConflictError
from grapharrow.interfaces.rest import create_rest_app

METRIC = "grapharrow_http_operations_in_flight"


def test_request_metrics_require_opt_in_and_rest_auth(tmp_path):
    with TestClient(create_rest_app(tmp_path)) as client:
        assert client.get("/v1/metrics").status_code == 404

    app = create_rest_app(
        tmp_path,
        settings=GraphArrowSettings(
            server_auth_token="scrape-token", rest_prefix="/graph", request_metrics_enabled=True
        ),
    )
    with TestClient(app) as client:
        assert client.get("/graph/metrics").status_code == 401
        response = client.get("/graph/metrics", headers={"Authorization": "Bearer scrape-token"})
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/plain; version=0.0.4")
        assert f'{METRIC}{{interface="rest_cypher"}} 0\n' in response.text
        assert f'{METRIC}{{interface="ana"}} 0\n' in response.text


def test_dedicated_metrics_token_only_grants_scrape_access(tmp_path):
    app = create_rest_app(
        tmp_path,
        settings=GraphArrowSettings(
            request_metrics_enabled=True,
            request_metrics_token="metrics-token",
            server_auth_token="rest-token",
        ),
    )
    rest = {"Authorization": "Bearer rest-token"}
    metrics = {"Authorization": "Bearer metrics-token"}
    with TestClient(app) as client:
        assert client.get("/v1/metrics", headers=rest).status_code == 401
        assert client.get("/v1/metrics", headers=metrics).status_code == 200
        assert (
            client.post("/v1/query", headers=metrics, json={"query": "RETURN 1"}).status_code == 401
        )


def test_in_flight_request_is_visible_and_cleared_on_failure(tmp_path, monkeypatch):
    app = create_rest_app(tmp_path, settings=GraphArrowSettings(request_metrics_enabled=True))
    cypher = app.state.grapharrow.use_cases.cypher
    entered, release = Event(), Event()

    def fail(*args, **kwargs):
        entered.set()
        assert release.wait(5)
        raise RuntimeError("request failed")

    monkeypatch.setattr(type(cypher), "execute", fail)

    async def check():
        transport = ASGITransport(app=app, raise_app_exceptions=False)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            query = asyncio.create_task(client.post("/v1/query", json={"query": "RETURN 1"}))
            try:
                assert await asyncio.to_thread(entered.wait, 5)
                metrics = await client.get("/v1/metrics")
                assert f'{METRIC}{{interface="rest_cypher"}} 1\n' in metrics.text
                assert f'{METRIC}{{interface="ana"}} 0\n' in metrics.text
                assert 'grapharrow_http_operations_executing{interface="rest_cypher"} 1\n' in (
                    metrics.text
                )
            finally:
                release.set()
                response = await query
            assert response.status_code == 500
            metrics = await client.get("/v1/metrics")
            assert f'{METRIC}{{interface="rest_cypher"}} 0\n' in metrics.text
            assert 'grapharrow_http_operations_executing{interface="rest_cypher"} 0\n' in (
                metrics.text
            )
            assert (
                'grapharrow_http_operations_total{interface="rest_cypher",status="500"} 1\n'
                in metrics.text
            )

    asyncio.run(check())


def test_request_and_execution_histograms_measure_different_boundaries(tmp_path):
    app = create_rest_app(
        tmp_path,
        settings=GraphArrowSettings(
            request_metrics_enabled=True, ana_enabled=False, server_auth_token="scrape-token"
        ),
    )
    headers = {"Authorization": "Bearer scrape-token"}
    payload = {"query": "RETURN 1 AS value"}
    with TestClient(app) as client:
        assert client.post("/v1/query", json=payload).status_code == 401
        assert client.post("/v1/query", json=payload, headers=headers).status_code == 200
        samples = client.get("/v1/metrics", headers=headers).text
    assert 'grapharrow_http_operations_total{interface="rest_cypher",status="401"} 1\n' in samples
    assert 'grapharrow_http_operations_total{interface="rest_cypher",status="200"} 1\n' in samples
    assert (
        'grapharrow_http_operation_duration_seconds_count{interface="rest_cypher"} 2\n' in samples
    )
    assert (
        'grapharrow_http_operation_execution_seconds_count{interface="rest_cypher"} 1\n' in samples
    )
    assert "grapharrow_ana_sessions_open" not in samples
    for name, expected in (
        ("grapharrow_http_operation_duration_seconds", 2),
        ("grapharrow_http_operation_execution_seconds", 1),
    ):
        bucket_counts = [
            int(sample.rsplit(" ", 1)[1])
            for sample in samples.splitlines()
            if sample.startswith(f"{name}_bucket")
        ]
        assert bucket_counts == sorted(bucket_counts)
        assert bucket_counts[-1] == expected
        assert f'{name}_bucket{{interface="rest_cypher",le="+Inf"}} {expected}\n' in samples


def test_rest_write_conflicts_distinguish_idempotency_conflicts(tmp_path, monkeypatch):
    app = create_rest_app(tmp_path, settings=GraphArrowSettings(request_metrics_enabled=True))
    cypher = app.state.grapharrow.use_cases.cypher

    def reject(_use_case, _query, _params):
        raise WriteConflictError("snapshot changed")

    monkeypatch.setattr(type(cypher), "execute", reject)
    with TestClient(app) as client:
        headers = {"Idempotency-Key": "device-ingest"}
        payload = {"query": "CREATE (:Device)"}
        assert client.post("/v1/query", json=payload, headers=headers).status_code == 200
        changed = {"query": "CREATE (:OtherDevice)"}
        assert client.post("/v1/query", json=changed, headers=headers).status_code == 409
        assert client.post("/v1/query", json=payload).status_code == 409
        metrics = client.get("/v1/metrics").text
        assert (
            'grapharrow_http_operations_total{interface="rest_cypher",status="409"} 2\n' in metrics
        )
        assert "grapharrow_rest_write_conflicts_total 1\n" in metrics
        assert (
            'grapharrow_http_operation_execution_seconds_count{interface="rest_cypher"} 3\n'
            in metrics
        )


def test_ana_metrics_follow_session_lifecycle(tmp_path, monkeypatch):
    key = "a" * 32
    auth_file = tmp_path / "clients.json"
    auth_file.write_text(
        json.dumps({"clients": [{"id": "test", "api_key": key, "profile": "readonly"}]})
    )
    monkeypatch.setenv("ANA_AUTH_FILE", str(auth_file))
    app = create_rest_app(
        tmp_path / "db",
        settings=GraphArrowSettings(request_metrics_enabled=True, ana_max_total_bytes=1234567),
    )
    with TestClient(app) as client:
        assert client.post("/v1/query", json={"query": "CREATE (:Device)"}).status_code == 200
        headers = {"Authorization": f"Bearer {key}"}
        response = client.post("/sessions", headers=headers)
        assert response.status_code == 201
        headers["X-ANA-Session"] = response.headers["X-ANA-Session"]
        tool = {"tool": "count_nodes", "args": {"label": "Device"}}
        assert client.post("/tools/call", headers=headers, json=tool).status_code == 200
        samples = client.get("/v1/metrics").text
        assert 'grapharrow_http_operations_total{interface="ana",status="200"} 1\n' in samples
        assert 'grapharrow_http_operation_execution_seconds_count{interface="ana"} 1\n' in samples
        assert "grapharrow_ana_sessions_open 1\n" in samples
        assert "grapharrow_ana_session_limit_bytes 1234567\n" in samples
        bytes_sample = next(
            value
            for value in samples.splitlines()
            if value.startswith("grapharrow_ana_session_bytes ")
        )
        assert int(bytes_sample.split()[1]) > 0

        assert client.delete("/sessions/current", headers=headers).status_code == 204
        samples = client.get("/v1/metrics").text
        assert "grapharrow_ana_sessions_open 0\n" in samples
        assert "grapharrow_ana_session_bytes 0\n" in samples
