import json

import pytest
from fastapi.testclient import TestClient

from grapharrow.config import GraphArrowSettings
from grapharrow.core.errors import StorageError
from grapharrow.interfaces.rest import create_rest_app


def test_native_reader_waits_for_writer_initialization(tmp_path):
    database = tmp_path / "database"
    with pytest.raises(StorageError, match="Initialize the Native database from its writer"):
        create_rest_app(database, settings=GraphArrowSettings(storage_read_only=True))
    assert not database.exists()


def test_native_reader_rejects_rest_and_cypher_writes(tmp_path):
    with TestClient(create_rest_app(tmp_path)) as writer:
        assert writer.post("/v1/query", json={"query": "CREATE (:Device {name:'a'})"}).is_success
    head = tmp_path / "current_snapshot.json"
    published = head.read_bytes()
    endpoints = (
        ("/v1/query", {"query": "MATCH (n:Device) SET n.name = 'b'"}),
        ("/v1/query", {"query": "CREATE INDEX device_name FOR (d:Device) ON (d.name)"}),
        ("/v1/nodes", {"labels": ["Device"]}),
        ("/v1/nodes/bulk", {"default_labels": ["Device"], "rows": [{"name": "b"}]}),
        ("/v1/admin/recover", {"dry_run": False}),
        ("/v1/admin/compact", None),
        ("/v1/admin/vacuum", None),
    )
    with TestClient(
        create_rest_app(tmp_path, settings=GraphArrowSettings(storage_read_only=True))
    ) as reader:
        for path, payload in endpoints:
            response = reader.post(path, json=payload)
            assert response.status_code == 403, (path, response.text)
            assert response.json()["error"]["code"] == "READ_ONLY"
        response = reader.post(
            "/v1/query", json={"query": "CREATE (:Device)"}, headers={"Idempotency-Key": "retry"}
        )
        assert response.status_code == 403
        result = reader.post("/v1/query", json={"query": "MATCH (d:Device) RETURN d.name AS name"})
        assert result.json()["data"]["rows"] == [{"name": "a"}]
    assert head.read_bytes() == published


def test_native_reader_keeps_ana_snapshot_without_creating_a_local_lock(tmp_path, monkeypatch):
    key = "a" * 32
    config = tmp_path / "clients.json"
    config.write_text(
        json.dumps({"clients": [{"id": "test", "api_key": key, "profile": "readonly"}]})
    )
    monkeypatch.setenv("ANA_AUTH_FILE", str(config))
    database = tmp_path / "graph"

    with TestClient(create_rest_app(database)) as writer:
        assert writer.post("/v1/query", json={"query": "CREATE (:Device)"}).is_success
        lock = database / ".grapharrow-writer.lock"
        lock.unlink()
        with TestClient(
            create_rest_app(database, settings=GraphArrowSettings(storage_read_only=True))
        ) as reader:
            auth = {"Authorization": f"Bearer {key}"}

            def open_session():
                response = reader.post("/sessions", headers=auth)
                response.raise_for_status()
                return {**auth, "X-ANA-Session": response.headers["X-ANA-Session"]}

            def count(headers):
                result = reader.post(
                    "/tools/call",
                    headers=headers,
                    json={"tool": "count_nodes", "args": {"label": "Device"}},
                )
                handle = result.json()["result"]["handle"]
                page = reader.post(
                    "/tools/call", headers=headers, json={"tool": "fetch", "args": {"from": handle}}
                )
                return page.json()["result"]["rows"]

            previous = open_session()
            assert count(previous) == [[1]]
            assert not lock.exists()
            assert not (database / ".grapharrow-cache").exists()
            assert writer.post("/v1/query", json={"query": "CREATE (:Device)"}).is_success
            assert count(previous) == [[1]]
            assert count(open_session()) == [[2]]
