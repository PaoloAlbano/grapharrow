"""REST clients can retry the outcome of a published Cypher write."""

import multiprocessing
from hashlib import sha256
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from starlette.types import Message, Receive, Scope, Send

from grapharrow.adapters.object_pointer import Revision
from grapharrow.config import GraphArrowSettings
from grapharrow.interfaces.rest import create_rest_app
from tests.integration.support import MotoS3Context

pytestmark = pytest.mark.integration

QUERY = "CREATE (n:Retried {name: $name}) RETURN id(n) AS id,n.name AS name"
HEADERS = {"Idempotency-Key": "create-once"}


@pytest.mark.parametrize("backend", ["local", "s3"])
def test_retry_after_lost_response_replays_the_committed_write(
    backend: str, tmp_path: Path, request: pytest.FixtureRequest
) -> None:
    path: str | Path = tmp_path / "lost-response"
    options: dict[str, Any] = {}
    if backend == "s3":
        storage: MotoS3Context = request.getfixturevalue("moto_s3_filesystem")
        path = f"{storage.bucket_uri}/rest-lost-response"
        options["filesystem"] = storage.filesystem
    writer = create_rest_app(path, **options)
    lost_body = bytearray()

    async def disconnect(scope: Scope, receive: Receive, send: Send) -> None:
        async def drop_response(message: Message) -> None:
            if message["type"] == "http.response.body":
                lost_body.extend(message.get("body", b""))
                if not message.get("more_body", False):
                    raise ConnectionError("Client disconnected after publication")

        await writer(scope, receive, drop_response)

    initial = writer.state.grapharrow.repositories.snapshot.load_current_pointer()
    payload = {"query": QUERY, "params": {"name": "città"}}
    with pytest.raises(ConnectionError, match="after publication"):
        TestClient(disconnect).post("/v1/query", json=payload, headers=HEADERS)

    restarted = create_rest_app(path, **options)
    client = TestClient(restarted)
    committed = restarted.state.grapharrow.repositories.snapshot.load_current_pointer()
    assert committed.snapshot_id.value == initial.snapshot_id.value + 1
    assert client.post("/v1/query", json={"query": "CREATE (:Other)"}).status_code == 200
    head = restarted.state.grapharrow.repositories.snapshot.load_current_pointer()

    replay = client.post("/v1/query", json=payload, headers=HEADERS)
    assert replay.status_code == 200
    assert replay.content == bytes(lost_body)
    assert replay.json()["data"]["rows"] == [{"id": 1, "name": "città"}]
    assert restarted.state.grapharrow.repositories.snapshot.load_current_pointer() == head

    different_payload = {"query": QUERY, "params": {"name": "different"}}
    conflict = client.post("/v1/query", json=different_payload, headers=HEADERS)
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "IDEMPOTENCY_KEY_CONFLICT"
    assert restarted.state.grapharrow.repositories.snapshot.load_current_pointer() == head
    result = client.post("/v1/query", json={"query": "MATCH (n:Retried) RETURN count(n) AS n"})
    assert result.json()["data"]["rows"] == [{"n": 1}]
    assert restarted.state.grapharrow.use_cases.admin.verify(verify_checksums=True).ok


def _pause_keyed_request(uri: str, s3_endpoint: str, channel: Connection) -> None:
    app = create_rest_app(uri, settings=_s3_settings(s3_endpoint))
    filesystem = app.state.grapharrow.adapters.filesystem
    replace_pointer = filesystem.replace_pointer

    def pause(location: str | Path, payload: object, expected: Revision | None) -> bool:
        channel.send("prepared")
        assert channel.poll(25), "Parent did not release the publisher"
        assert channel.recv() == "publish"
        return replace_pointer(location, payload, expected)

    filesystem.replace_pointer = pause
    try:
        response = TestClient(app).post(
            "/v1/query", json={"query": QUERY, "params": {"name": "winner"}}, headers=HEADERS
        )
        channel.send((response.status_code, response.json()))
    finally:
        channel.close()


def _s3_settings(endpoint: str) -> GraphArrowSettings:
    return GraphArrowSettings(
        s3_access_key="test",
        s3_secret_key="test",
        s3_region="us-east-1",
        s3_endpoint=endpoint,
        s3_scheme="http",
    )


@pytest.mark.parametrize("same_payload", [True, False], ids=["replay", "key-conflict"])
def test_concurrent_keyed_requests_in_separate_s3_processes(
    moto_s3_filesystem: MotoS3Context, same_payload: bool
) -> None:
    storage = moto_s3_filesystem
    uri = f"{storage.bucket_uri}/rest-racing"
    endpoint = storage.endpoint_url.removeprefix("http://")
    app = create_rest_app(uri, settings=_s3_settings(endpoint))
    client = TestClient(app)
    seed = {"query": "CREATE (n:Seed) RETURN id(n) AS id"}
    seed_headers = {"Idempotency-Key": "seed"}
    seeded = client.post("/v1/query", json=seed, headers=seed_headers)
    assert seeded.status_code == 200
    assert client.post("/v1/query", json={"query": "CREATE (:Between)"}).status_code == 200
    snapshots = app.state.grapharrow.repositories.snapshot
    initial = snapshots.load_current_pointer()
    assert initial.idempotency_index_path is not None
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    writer = context.Process(target=_pause_keyed_request, args=(uri, endpoint, child))
    try:
        writer.start()
        child.close()
        assert parent.poll(25), "Child did not reach the conditional PUT"
        assert parent.recv() == "prepared"
        assert app.state.grapharrow.use_cases.admin.vacuum() == ()

        name = "winner" if same_payload else "different"
        winner = client.post(
            "/v1/query", json={"query": QUERY, "params": {"name": name}}, headers=HEADERS
        )
        assert winner.status_code == 200
        parent.send("publish")
        assert parent.poll(25), "Child did not report the outcome of the request"
        status, body = parent.recv()
        if same_payload:
            assert status == 200
            assert body == winner.json()
        else:
            assert status == 409
            assert body["error"]["code"] == "IDEMPOTENCY_KEY_CONFLICT"
        writer.join(timeout=15)
        assert writer.exitcode == 0
        head = snapshots.load_current_pointer()
        assert head.snapshot_id.value == initial.snapshot_id.value + 1
        removed = app.state.grapharrow.use_cases.admin.vacuum()
        assert len([path for path in removed if path.startswith("idempotency/")]) == 2
        # A reader that captured the earlier head can still use that exact index root.
        _, pinned_receipt = snapshots.idempotency_base(initial, sha256(b"seed").hexdigest())
        assert pinned_receipt is not None
        assert pinned_receipt.response_json.encode() == seeded.content
        assert client.post("/v1/query", json=seed, headers=seed_headers).content == seeded.content
        assert (
            client.post(
                "/v1/query", json={"query": QUERY, "params": {"name": name}}, headers=HEADERS
            ).content
            == winner.content
        )
        read = client.post("/v1/query", json={"query": "MATCH (n:Retried) RETURN n.name AS name"})
        assert read.json()["data"]["rows"] == [{"name": name}]
    finally:
        parent.close()
        child.close()
        if writer.pid is not None and writer.is_alive():
            writer.kill()
            writer.join(timeout=10)
