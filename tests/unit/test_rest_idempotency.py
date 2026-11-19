"""Invalid and no-op keyed writes obey the same atomic publication contract."""

import json
from hashlib import sha256
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from grapharrow.interfaces.rest import create_rest_app

HEADERS = {"Idempotency-Key": "same-request"}


@pytest.mark.parametrize(
    "statement",
    [
        "CREATE (:Attempt) RETURN 1/0 AS invalid",
        "CREATE (:Attempt) RETURN $value AS oversized",
        "CREATE (:Attempt) RETURN 0.0 / 0.0 AS nan",
        "RETURN 1 AS read",
        "CREATE INDEX ON :Attempt(name)",
    ],
)
def test_rejected_keyed_request_does_not_publish_or_consume_the_key(
    tmp_path: Path, statement: str
) -> None:
    app = create_rest_app(tmp_path)
    client = TestClient(app)
    snapshot = app.state.grapharrow.repositories.snapshot
    before = snapshot.load_current_pointer()

    response = client.post(
        "/v1/query",
        json={"query": statement, "params": {"value": "x" * (64 * 1024)}},
        headers=HEADERS,
    )
    assert response.status_code == 400, response.text
    assert snapshot.load_current_pointer() == before
    assert (
        client.post(
            "/v1/query", json={"query": "CREATE (:Attempt) RETURN 1 AS value"}, headers=HEADERS
        ).status_code
        == 200
    )
    result = client.post("/v1/query", json={"query": "MATCH (n:Attempt) RETURN count(n) AS n"})
    assert result.json()["data"]["rows"] == [{"n": 1}]


def test_successful_no_match_is_replayed_after_the_graph_changes(tmp_path: Path) -> None:
    app = create_rest_app(tmp_path)
    client = TestClient(app)
    payload = {"query": "MATCH (n:Later) SET n.marked=true RETURN n.marked AS matched"}
    first = client.post("/v1/query", json=payload, headers=HEADERS)
    assert first.status_code == 200
    assert first.json()["data"]["rows"] == []
    assert client.post("/v1/query", json={"query": "CREATE (:Later)"}).status_code == 200
    restarted = TestClient(create_rest_app(tmp_path))
    assert restarted.post("/v1/query", json=payload, headers=HEADERS).json() == first.json()
    result = restarted.post(
        "/v1/query", json={"query": "MATCH (n:Later) RETURN n.marked AS marked"}
    )
    assert result.json()["data"]["rows"] == [{"marked": None}]


def test_keyed_retry_reads_index_without_walking_published_ancestry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = create_rest_app(tmp_path)
    client = TestClient(app)
    payload = {"query": "CREATE (n:Indexed) RETURN id(n) AS id"}
    first = client.post("/v1/query", json=payload, headers=HEADERS)
    assert first.status_code == 200
    # A second receipt in the same shard must preserve the first entry.
    prefix = sha256(HEADERS["Idempotency-Key"].encode()).hexdigest()[:2]
    other = next(f"key-{n}" for n in range(10_000) if _hash(f"key-{n}").startswith(prefix))
    assert (
        client.post("/v1/query", json=payload, headers={"Idempotency-Key": other}).status_code
        == 200
    )
    snapshot = app.state.grapharrow.repositories.snapshot
    root_before_compaction = snapshot.load_current_pointer().idempotency_index_path
    assert root_before_compaction is not None
    app.state.grapharrow.use_cases.admin.compact()
    assert snapshot.load_current_pointer().idempotency_index_path == root_before_compaction
    assert app.state.grapharrow.use_cases.admin.vacuum() == ()

    replica = create_rest_app(tmp_path)
    filesystem = replica.state.grapharrow.adapters.filesystem
    read_json = filesystem.read_json
    reads: list[Path] = []

    def count_reads(location: str | Path) -> object:
        reads.append(Path(location))
        return read_json(location)

    monkeypatch.setattr(filesystem, "read_json", count_reads)
    replay = TestClient(replica).post("/v1/query", json=payload, headers=HEADERS)
    assert replay.content == first.content
    assert sum(path.parent.name == "manifests" for path in reads) == 2
    assert sum(path.parent.name == "idempotency" for path in reads) == 2


def test_first_indexed_commit_migrates_legacy_receipts(tmp_path: Path) -> None:
    app = create_rest_app(tmp_path)
    client = TestClient(app)
    snapshots = app.state.grapharrow.repositories.snapshot
    path = tmp_path / "current_snapshot.json"
    old_payload = {"query": "CREATE (n:Legacy) RETURN id(n) AS id", "params": {}}
    expected = {
        "ok": True,
        "data": {"columns": ["id"], "rows": [{"id": 1}]},
        "meta": {"row_count": 1},
    }
    assert client.post("/v1/query", json=old_payload).status_code == 200
    old_head = snapshots.load_current_pointer()
    old_manifest_path = tmp_path / old_head.manifest_path
    old_manifest = json.loads(old_manifest_path.read_text())
    old_manifest["idempotency_receipt"] = {
        "key_hash": _hash("legacy"),
        "request_hash": _hash(json.dumps(old_payload, sort_keys=True, separators=(",", ":"))),
        "response_json": json.dumps(expected),
    }
    old_manifest_path.write_text(json.dumps(old_manifest))
    assert "idempotency_index_path" not in json.loads(path.read_text())
    assert (
        client.post("/v1/query", json=old_payload, headers={"Idempotency-Key": "legacy"}).json()
        == expected
    )
    assert (
        client.post("/v1/query", json={"query": "CREATE (:Indexed)"}, headers=HEADERS).status_code
        == 200
    )
    assert snapshots.load_current_pointer().idempotency_index_path is not None
    assert (
        client.post("/v1/query", json=old_payload, headers={"Idempotency-Key": "legacy"}).json()
        == expected
    )


def _hash(value: str) -> str:
    return sha256(value.encode()).hexdigest()
