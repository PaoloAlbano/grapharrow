"""Exercise Delta graph commits, pinned snapshots and REST replay through replicas."""

from __future__ import annotations

import json
import multiprocessing
from hashlib import sha256
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from starlette.types import Message, Receive, Scope, Send

from grapharrow.app import build_application
from grapharrow.config import GraphArrowSettings
from grapharrow.core.errors import StorageError
from grapharrow.interfaces.rest import create_rest_app
from tests.integration.support import MotoS3Context

pytestmark = pytest.mark.integration

CREATE = (
    "CREATE (a:Author {name: $name})-[:WROTE {since: 2020}]->(:Book {name: $name}) "
    "RETURN id(a) AS id, a.name AS name"
)
READ = (
    "MATCH (a:Author)-[r:WROTE]->(b:Book) "
    "RETURN id(a) AS author, id(r) AS edge, b.name AS name ORDER BY name"
)


@pytest.fixture
def delta_database(
    request: pytest.FixtureRequest, tmp_path: Path
) -> tuple[str | Path, GraphArrowSettings]:
    pytest.importorskip("deltalake")
    if request.param == "local":
        return tmp_path / "delta-db", GraphArrowSettings(storage_backend="delta")
    moto: MotoS3Context = request.getfixturevalue("moto_s3_filesystem")
    return f"{moto.bucket_uri}/delta-db", GraphArrowSettings(
        storage_backend="delta",
        s3_access_key="test",
        s3_secret_key="test",
        s3_region="us-east-1",
        s3_endpoint=moto.endpoint_url.removeprefix("http://"),
        s3_scheme="http",
    )


def _post(client: TestClient, name: str, key: str) -> Any:
    return client.post(
        "/v1/query",
        json={"query": CREATE, "params": {"name": name}},
        headers={"Idempotency-Key": key},
    )


@pytest.mark.parametrize("delta_database", ["local", "s3"], indirect=True)
def test_lost_response_replays_one_complete_delta_commit(
    delta_database: tuple[str | Path, GraphArrowSettings],
) -> None:
    path, settings = delta_database
    writer = create_rest_app(path, settings=settings)
    reader = build_application(path, settings=settings)
    graph = reader.use_cases.graph
    snapshots = reader.repositories.snapshot
    pinned = snapshots.load_current_pointer()
    lost_body = bytearray()

    async def disconnect(scope: Scope, receive: Receive, send: Send) -> None:
        async def drop_response(message: Message) -> None:
            if message["type"] == "http.response.body":
                lost_body.extend(message.get("body", b""))
                raise ConnectionError("Response lost after commit")

        await writer(scope, receive, drop_response)

    with pytest.raises(ConnectionError, match="Response lost"):
        _post(TestClient(disconnect), "Graph", "once")

    head = snapshots.load_current_pointer()
    assert head.snapshot_id.value == pinned.snapshot_id.value + 1
    key_hash = sha256(b"once").hexdigest()
    old_manifest, old_receipt = snapshots.idempotency_base(pinned, key_hash)
    old = graph.load_state(snapshot=old_manifest)
    assert old.nodes == old.edges == ()
    assert old.catalog.labels == old.catalog.relationship_types == ()
    assert old_receipt is None
    manifest, receipt = snapshots.idempotency_base(head, key_hash)
    state = graph.load_state(snapshot=manifest)
    assert len(state.nodes) == 2 and len(state.edges) == 1
    assert {label.name for label in state.catalog.labels} == {"Author", "Book"}
    assert {kind.name for kind in state.catalog.relationship_types} == {"WROTE"}
    assert receipt is not None and receipt.response_json.encode() == lost_body

    restarted = TestClient(create_rest_app(path, settings=settings))
    assert _post(restarted, "Other", "later").status_code == 200
    later = snapshots.load_current_pointer()
    replay = _post(restarted, "Graph", "once")
    assert replay.status_code == 200 and replay.content == lost_body
    mismatch = _post(restarted, "Different", "once")
    assert mismatch.status_code == 409
    assert mismatch.json()["error"]["code"] == "IDEMPOTENCY_KEY_CONFLICT"
    assert snapshots.load_current_pointer() == later
    assert reader.use_cases.cypher.execute(READ).rows == (
        {"author": 1, "edge": 1, "name": "Graph"},
        {"author": 3, "edge": 2, "name": "Other"},
    )


def _paused_writer(
    path: str | Path, settings: GraphArrowSettings, channel: Connection, name: str, key: str
) -> None:
    from grapharrow.storage import delta

    app = create_rest_app(path, settings=settings)
    original = delta.write_deltalake

    def publish(*args: Any, **kwargs: Any) -> None:
        channel.send(args[0].version())
        assert channel.poll(30) and channel.recv() == "publish"
        original(*args, **kwargs)

    delta.write_deltalake = publish
    try:
        result = _post(TestClient(app), name, key)
        channel.send((result.status_code, result.content))
    finally:
        channel.close()


@pytest.mark.parametrize("delta_database", ["local", "s3"], indirect=True)
@pytest.mark.parametrize("same_key", [False, True], ids=["independent", "replay"])
def test_concurrent_delta_writers_arbitrate_in_the_transaction_log(
    delta_database: tuple[str | Path, GraphArrowSettings], same_key: bool
) -> None:
    path, settings = delta_database
    app = create_rest_app(path, settings=settings)
    reader = app.state.grapharrow
    snapshots = reader.repositories.snapshot
    initial = snapshots.load_current_pointer()
    context = multiprocessing.get_context("spawn")
    requests = [("First", "first"), ("First", "first") if same_key else ("Second", "second")]
    pipes = [context.Pipe(), context.Pipe()]
    processes = [
        context.Process(target=_paused_writer, args=(path, settings, child, name, key))
        for (_, child), (name, key) in zip(pipes, requests, strict=True)
    ]
    try:
        for process, (_, child) in zip(processes, pipes, strict=True):
            process.start()
            child.close()
        for parent, _ in pipes:
            assert parent.poll(30), "Writer did not reach the competing commit"
            assert parent.recv() == initial.snapshot_id.value - 1
        assert reader.use_cases.cypher.execute(READ).rows == ()
        for parent, _ in pipes:
            parent.send("publish")
        responses = []
        for parent, _ in pipes:
            assert parent.poll(30), "Writer did not report the result"
            responses.append(parent.recv())
        for process in processes:
            process.join(timeout=10)
            assert process.exitcode == 0

        assert snapshots.load_current_pointer().snapshot_id.value == initial.snapshot_id.value + 1
        if same_key:
            assert responses[0] == responses[1]
            assert responses[0][0] == 200
            assert len(reader.use_cases.cypher.execute(READ).rows) == 1
        else:
            assert sorted(status for status, _ in responses) == [200, 409]
            rejected = next(index for index, response in enumerate(responses) if response[0] == 409)
            assert json.loads(responses[rejected][1])["error"]["code"] == "WRITE_CONFLICT"
            assert _post(TestClient(app), *requests[rejected]).status_code == 200
            rows = reader.use_cases.cypher.execute(READ).rows
            assert {row["name"] for row in rows} == {"First", "Second"}
            assert {row["author"] for row in rows} == {1, 3}
            assert {row["edge"] for row in rows} == {1, 2}
    finally:
        for parent, child in pipes:
            parent.close()
            child.close()
        for process in processes:
            if process.pid is not None and process.is_alive():
                process.kill()
                process.join(timeout=10)


@pytest.mark.parametrize("delta_database", ["local"], indirect=True)
def test_commit_acknowledgement_loss_is_resolved_from_the_exact_version(
    delta_database: tuple[str | Path, GraphArrowSettings], monkeypatch: pytest.MonkeyPatch
) -> None:
    from deltalake.exceptions import DeltaError

    from grapharrow.storage import delta

    path, settings = delta_database
    client = TestClient(create_rest_app(path, settings=settings))
    original = delta.write_deltalake

    def publish_and_lose_ack(*args: Any, **kwargs: Any) -> None:
        original(*args, **kwargs)
        raise DeltaError("The object store response was lost")

    with monkeypatch.context() as patch:
        patch.setattr(delta, "write_deltalake", publish_and_lose_ack)
        result = _post(client, "Graph", "once")
        assert result.status_code == 200, result.text
    assert _post(client, "Graph", "once").content == result.content
    assert len(build_application(path, settings=settings).use_cases.cypher.execute(READ).rows) == 1


@pytest.mark.parametrize("delta_database", ["local"], indirect=True)
def test_catalog_only_commits_and_compaction_keep_previous_snapshots_readable(
    delta_database: tuple[str | Path, GraphArrowSettings],
) -> None:
    from deltalake import DeltaTable

    path, settings = delta_database
    writer = build_application(path, settings=settings)
    reader = build_application(path, settings=settings)
    writer.use_cases.cypher.execute(CREATE, {"name": "Graph"})
    pinned = reader.storage_backend.capture_snapshot()
    writer.use_cases.indexes.create_index(
        name="authors",
        kind="range",
        owner_kind="label",
        owner_name="Author",
        property_names=("name",),
    )
    head = writer.storage_backend.capture_snapshot()
    assert head.catalog_version.value == pinned.catalog_version.value + 1
    uri = str(writer.layout.delta_dir)
    before = DeltaTable(uri, version=pinned.snapshot_id.value - 1)
    after = DeltaTable(uri, version=head.snapshot_id.value - 1)
    canonical = "kind IN ('nodes', 'edges')"
    assert sorted(before.file_uris(file_pruning_predicate=canonical)) == sorted(
        after.file_uris(file_pruning_predicate=canonical)
    )
    assert not reader.repositories.catalog.load_for_snapshot(pinned).indexes
    assert reader.repositories.catalog.load().indexes[0].name == "authors"
    writer.use_cases.admin.compact()
    assert writer.use_cases.admin.verify().ok
    writer.use_cases.cypher.execute("MATCH (n) DETACH DELETE n")
    assert reader.use_cases.cypher.execute(READ).rows == ()
    state = reader.use_cases.graph.load_state(snapshot=pinned)
    assert len(state.nodes) == 2 and len(state.edges) == 1
    assert state.edges[0].properties == {"since": 2020}


@pytest.mark.parametrize("first", ["native", "delta"])
def test_wrong_storage_backend_refuses_existing_database(first: Any, tmp_path: Path) -> None:
    pytest.importorskip("deltalake")
    build_application(
        tmp_path, settings=GraphArrowSettings(storage_backend=first)
    ).use_cases.admin.initialize()
    second = "delta" if first == "native" else "native"
    with pytest.raises(StorageError, match="This database uses the"):
        build_application(
            tmp_path, settings=GraphArrowSettings(storage_backend=second)
        ).use_cases.admin.initialize()
