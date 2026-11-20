"""Exercise Delta graph commits, pinned snapshots and REST replay through replicas."""

from __future__ import annotations

import json
import multiprocessing
from hashlib import sha256
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any

import orjson
import pytest
from fastapi.testclient import TestClient
from starlette.types import Message, Receive, Scope, Send

from grapharrow.app import build_application
from grapharrow.config import GraphArrowSettings
from grapharrow.core.errors import StorageError, WriteConflictError
from grapharrow.domain.snapshot import IdempotencyReceipt
from grapharrow.interfaces.rest import create_rest_app
from grapharrow.use_cases.ana import GraphArrowAgentBackend
from tests.integration.delta_log_gate import pause_delta_log_put
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
READ_PROPERTIES = "MATCH (a:Author)-[r:WROTE]->(b:Book) RETURN a.name AS author, r.since AS since"
CREATE_EDGE_RANGES = (
    "CREATE (a:Author)-[:WROTE {since:2020}]->(b:Book) "
    "CREATE (a)-[:WROTE {since:2020}]->(b), (a)-[:WROTE {since:2020}]->(b), "
    "(a)-[:WROTE {since:2020}]->(b)"
)
SET_EDGE = "MATCH ()-[r:WROTE]->() WHERE id(r)=$id SET r.since=$since RETURN r.since AS since"


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


def _write_until_killed(path: str, settings: GraphArrowSettings, key: str) -> None:
    with TestClient(create_rest_app(path, settings=settings)) as client:
        response = _post(client, "Graph", key)
        raise AssertionError(f"The writer returned before it was killed: {response.status_code}")


@pytest.mark.parametrize("delta_database", ["s3"], indirect=True)
@pytest.mark.parametrize("after_publish", [False, True], ids=["before-log-put", "after-log-put"])
def test_killed_delta_writer_leaves_one_atomic_rest_effect(
    delta_database: tuple[str | Path, GraphArrowSettings], after_publish: bool
) -> None:
    path, settings = delta_database
    assert isinstance(path, str)
    key = "crashed-writer"
    key_hash = sha256(key.encode()).hexdigest()
    reader = build_application(path, settings=settings)
    reader.use_cases.admin.initialize()
    snapshots = reader.repositories.snapshot
    initial = snapshots.load_current_pointer()
    graph = reader.use_cases.graph

    def assert_visible_state(published: bool) -> IdempotencyReceipt | None:
        head = snapshots.load_current_pointer()
        assert head.snapshot_id.value == initial.snapshot_id.value + int(published)
        manifest, receipt = snapshots.idempotency_base(head, key_hash)
        visible = graph.load_state(snapshot=manifest)
        if published:
            assert len(visible.nodes) == 2 and len(visible.edges) == 1
            assert {label.name for label in visible.catalog.labels} == {"Author", "Book"}
            assert {kind.name for kind in visible.catalog.relationship_types} == {"WROTE"}
            assert receipt is not None
            assert reader.use_cases.cypher.execute(READ).rows == (
                {"author": 1, "edge": 1, "name": "Graph"},
            )
        else:
            assert head == initial and receipt is None
            assert visible.nodes == visible.edges == ()
            assert visible.catalog.labels == visible.catalog.relationship_types == ()
        with GraphArrowAgentBackend(reader) as current:
            assert current._state.snapshot.snapshot_id == head.snapshot_id
            overview = current.call("schema_overview", {})
            assert {entry["label"]: entry["count"] for entry in overview["labels"]} == (
                {"Author": 1, "Book": 1} if published else {}
            )
        return receipt

    endpoint_option = "s3_endpoint" if path.startswith("s3://") else "azure_blob_storage_authority"
    upstream = getattr(settings, endpoint_option)
    assert upstream is not None
    authority, slash, account = upstream.partition("/")

    with (
        GraphArrowAgentBackend(reader) as pinned,
        pause_delta_log_put(
            authority, version=initial.snapshot_id.value, after_publish=after_publish
        ) as gate,
    ):
        assert pinned._state.snapshot.snapshot_id == initial.snapshot_id
        writer_settings = settings.model_copy(
            update={endpoint_option: gate.authority + slash + account}
        )
        writer = multiprocessing.get_context("spawn").Process(
            target=_write_until_killed, args=(path, writer_settings, key)
        )
        writer.start()
        try:
            assert gate.reached.wait(30), "The writer did not reach the Delta log PUT"
            assert writer.is_alive()
            assert {
                "nodes",
                "edges-00000000000000000000",
                "metadata",
                f"receipt-{key_hash[:2]}",
            } <= gate.staged_partitions
            assert_visible_state(after_publish)
            assert writer.is_alive()
            writer.kill()
            writer.join(timeout=10)
            assert writer.exitcode not in (None, 0)
        finally:
            if writer.is_alive():
                writer.kill()
                writer.join(timeout=10)

        persisted = assert_visible_state(after_publish)
        with TestClient(create_rest_app(path, settings=settings)) as restarted:
            replay = _post(restarted, "Graph", key)
            assert replay.status_code == 200
            receipt = assert_visible_state(True)
            assert receipt is not None
            assert replay.content == receipt.response_json.encode()
            if persisted is not None:
                assert receipt == persisted

            mismatch = _post(restarted, "Different", key)
            assert mismatch.status_code == 409
            assert mismatch.json()["error"]["code"] == "IDEMPOTENCY_KEY_CONFLICT"
            assert _post(restarted, "Other", "after-restart").status_code == 200
            later = snapshots.load_current_pointer()
            assert later.snapshot_id.value == initial.snapshot_id.value + 2
            assert _post(restarted, "Graph", key).content == replay.content
            assert snapshots.load_current_pointer() == later
        assert pinned.call("schema_overview", {})["labels"] == []
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
    assert _active_edge_files(before) == _active_edge_files(after)
    assert before.file_uris(file_pruning_predicate="kind = 'nodes'") == after.file_uris(
        file_pruning_predicate="kind = 'nodes'"
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


@pytest.mark.parametrize("delta_database", ["local"], indirect=True)
@pytest.mark.parametrize("cache_enabled", [True, False])
def test_delta_readers_reuse_projections_and_pin_ana_across_commits(
    delta_database: tuple[str | Path, GraphArrowSettings],
    monkeypatch: pytest.MonkeyPatch,
    cache_enabled: bool,
) -> None:
    path, settings = delta_database
    if not cache_enabled:
        settings = settings.model_copy(
            update={"ana_edge_schema_cache_max_bytes": 0, "snapshot_table_cache_max_bytes": 0}
        )
    writer = build_application(path, settings=settings)
    reader = build_application(path, settings=settings)
    writer.use_cases.cypher.execute(CREATE, {"name": "Original"})

    discover = GraphArrowAgentBackend._discover_schema
    build_edge_projection = reader.repositories.graph._load_edge_property_projection
    discoveries = projections = 0

    def observe_discovery(self: GraphArrowAgentBackend, node_properties: Any = None) -> Any:
        nonlocal discoveries
        discoveries += 1
        return discover(self, node_properties)

    def observe_edge_projection(file: Any) -> Any:
        nonlocal projections
        projections += 1
        return build_edge_projection(file)

    monkeypatch.setattr(GraphArrowAgentBackend, "_discover_schema", observe_discovery)
    monkeypatch.setattr(
        reader.repositories.graph, "_load_edge_property_projection", observe_edge_projection
    )

    def ana_properties(workflow: GraphArrowAgentBackend) -> list[list[object]]:
        result = workflow.call(
            "relationship_query",
            {
                "source_label": "Author",
                "source_as": "a",
                "target_label": "Book",
                "target_as": "b",
                "relationship_type": "WROTE",
                "rel_as": "r",
                "select": [
                    {"var": "a", "property": "name", "alias": "author"},
                    {"var": "r", "property": "since", "alias": "since"},
                ],
            },
        )
        return workflow.call("fetch", {"from": result["handle"]})["rows"]

    first = {"author": "Original", "since": 2020}
    assert reader.use_cases.cypher.execute(READ_PROPERTIES).rows == (first,)
    assert reader.use_cases.cypher.execute(READ_PROPERTIES).rows == (first,)
    assert projections == (1 if cache_enabled else 2)
    with GraphArrowAgentBackend(reader) as initial:
        assert ana_properties(initial) == [["Original", 2020]]

    with GraphArrowAgentBackend(reader) as pinned:
        assert discoveries == (1 if cache_enabled else 2)
        writer.use_cases.cypher.execute(
            "MATCH (a:Author)-[r:WROTE]->() SET a.name = 'Changed', r.since = 2025"
        )
        assert reader.use_cases.cypher.execute(READ_PROPERTIES).rows == (
            {"author": "Changed", "since": 2025},
        )
        with GraphArrowAgentBackend(reader) as fresh:
            assert ana_properties(fresh) == [["Changed", 2025]]
            assert ana_properties(pinned) == [["Original", 2020]]
    cache = reader.use_cases.graph.snapshot_tables
    assert cache.used_bytes <= cache.max_bytes


@pytest.mark.parametrize("delta_database", ["local"], indirect=True)
def test_delta_reads_edge_endpoints_without_decoding_properties(
    delta_database: tuple[str | Path, GraphArrowSettings], monkeypatch: pytest.MonkeyPatch
) -> None:
    path, settings = delta_database
    app = build_application(path, settings=settings)
    app.use_cases.cypher.execute(CREATE, {"name": "Original"})
    snapshot = app.storage_backend.capture_snapshot()

    def unexpected_decode(*_: object) -> None:
        pytest.fail("An endpoint-only scan must not decode relationship properties.")

    monkeypatch.setattr(app.repositories.graph, "_load_edge_property_projection", unexpected_decode)
    assert app.repositories.graph.load_edge_property_table(
        snapshot.edge_properties, rel_type="WROTE", columns=["dst_id", "edge_id"]
    ).to_pylist() == [{"dst_id": 2, "edge_id": 1}]


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


def _active_edge_files(table: Any) -> set[str]:
    return {uri for uri in table.file_uris() if Path(uri).parent.name.startswith("kind=edges")}


@pytest.mark.parametrize("delta_database", ["local", "s3"], indirect=True)
def test_delta_point_set_reads_and_rewrites_only_its_range(
    delta_database: tuple[str | Path, GraphArrowSettings], monkeypatch: pytest.MonkeyPatch
) -> None:
    from deltalake import DeltaTable

    from grapharrow.storage import delta

    path, settings = delta_database
    settings = settings.model_copy(update={"edge_parquet_range_size": 2})
    app = build_application(path, settings=settings)
    app.use_cases.cypher.execute(CREATE_EDGE_RANGES)
    pinned = app.storage_backend.capture_snapshot()
    uri = str(app.layout.delta_dir)
    options = delta._storage_options(path, settings)
    before = _active_edge_files(DeltaTable(uri, storage_options=options))
    partition = delta._partition
    edge_reads = []

    def observe(table: Any, kind: str, columns: list[str], expression: object = None) -> Any:
        if kind.startswith("edges"):
            edge_reads.append(kind)
        return partition(table, kind, columns, expression)

    def unexpected_load(*_: object, **__: object) -> Any:
        pytest.fail("A point SET should not load or decode all canonical records.")

    with monkeypatch.context() as patch:
        patch.setattr(delta, "_partition", observe)
        patch.setattr(app.use_cases.graph, "load_state", unexpected_load)
        assert app.use_cases.cypher.execute(SET_EDGE, {"id": 3, "since": 2025}).rows == (
            {"since": 2025},
        )
    current = app.storage_backend.capture_snapshot()
    after = _active_edge_files(DeltaTable(uri, storage_options=options))
    assert current.snapshot_id.value == pinned.snapshot_id.value + 1
    assert edge_reads == ["edges-00000000000000000001"]
    assert len(before) == len(after) == 2
    assert len(before & after) == 1
    assert all(Path(file).parent.name.endswith(edge_reads[0]) for file in before ^ after)
    assert app.use_cases.graph.load_state(snapshot=pinned).edges[2].properties == {"since": 2020}
    assert app.use_cases.graph.load_state(snapshot=current).edges[2].properties == {"since": 2025}

    assert app.use_cases.cypher.execute(SET_EDGE, {"id": 3, "since": 2025}).rows == (
        {"since": 2025},
    )
    assert app.use_cases.cypher.execute(SET_EDGE, {"id": 99, "since": 2025}).rows == ()
    assert app.storage_backend.capture_snapshot() == current

    app.use_cases.cypher.execute("MATCH ()-[r:WROTE]->() WHERE id(r)=3 SET r.new = 'property'")
    changed = _active_edge_files(DeltaTable(uri, storage_options=options))
    assert len(changed & after) == 1
    assert app.use_cases.graph.load_state().edges[2].properties == {
        "since": 2025,
        "new": "property",
    }
    app.use_cases.cypher.execute("MATCH ()-[r:WROTE]->() WHERE id(r)>=3 DELETE r")
    assert _active_edge_files(DeltaTable(uri, storage_options=options)) == after & changed
    assert app.use_cases.admin.verify().ok


@pytest.mark.parametrize("delta_database", ["local"], indirect=True)
def test_delta_compact_changes_edge_range_size_and_keeps_pinned_reads(
    delta_database: tuple[str | Path, GraphArrowSettings],
) -> None:
    from deltalake import DeltaTable

    path, settings = delta_database
    app = build_application(
        path, settings=settings.model_copy(update={"edge_parquet_range_size": 0})
    )
    uri = str(app.layout.delta_dir)
    app.use_cases.cypher.execute(CREATE_EDGE_RANGES)
    pinned = app.storage_backend.capture_snapshot()
    old = _active_edge_files(DeltaTable(uri))
    assert {Path(file).parent.name for file in old} == {"kind=edges"}

    compactor = build_application(
        path, settings=settings.model_copy(update={"edge_parquet_range_size": 2})
    )
    current = compactor.use_cases.admin.compact()
    new = _active_edge_files(DeltaTable(uri))
    assert len(new) == 2
    assert not old.intersection(new)
    assert compactor.use_cases.graph.load_state(snapshot=current).edges == (
        compactor.use_cases.graph.load_state(snapshot=pinned).edges
    )
    assert compactor.use_cases.admin.verify().ok


@pytest.mark.parametrize("delta_database", ["local"], indirect=True)
@pytest.mark.parametrize("incompatible_field", ["format", "metadata_version"])
def test_delta_rejects_incompatible_metadata_without_writing(
    delta_database: tuple[str | Path, GraphArrowSettings],
    incompatible_field: str,
) -> None:
    import pyarrow as pa
    from deltalake import DeltaTable, write_deltalake

    from grapharrow.storage.delta import _partition, _rows

    path, settings = delta_database
    app = build_application(path, settings=settings)
    app.use_cases.admin.initialize()
    uri = str(app.layout.delta_dir)
    table = DeltaTable(uri)
    metadata = _partition(table, "metadata", ["properties_json", "key_hash"])
    row = metadata.to_pylist()[0]
    payload = orjson.loads(row["properties_json"])
    assert payload["format"] == "grapharrow-delta-v1"
    assert payload["graph"]["metadata_version"] == table.version() == 0
    assert row["key_hash"] == payload["graph"]["commit_id"]
    if incompatible_field == "format":
        payload["format"] = "unknown-format"
        payload["graph"]["metadata_version"] = table.version() + 1
    else:
        del payload["graph"]["metadata_version"]
    row["properties_json"] = orjson.dumps(payload).decode()
    write_deltalake(
        table,
        _rows(pa.Table.from_pylist([row], schema=metadata.schema), "metadata"),
        mode="overwrite",
        predicate="kind = 'metadata'",
    )
    with pytest.raises(StorageError, match="Unsupported GraphArrow Delta layout"):
        build_application(path, settings=settings).use_cases.admin.initialize()
    assert DeltaTable(uri).version() == 1


@pytest.mark.parametrize("delta_database", ["local", "s3"], indirect=True)
def test_delta_point_sets_rebase_disjoint_ranges_without_rewriting_metadata(
    delta_database: tuple[str | Path, GraphArrowSettings],
) -> None:
    from deltalake import DeltaTable

    from grapharrow.storage.delta import _storage_options

    path, settings = delta_database
    settings = settings.model_copy(update={"edge_parquet_range_size": 2})
    first = build_application(path, settings=settings)
    second = build_application(path, settings=settings)
    first.use_cases.cypher.execute(CREATE_EDGE_RANGES)
    base = first.storage_backend.capture_snapshot()
    uri = str(first.layout.delta_dir)
    options = _storage_options(path, settings)
    before = DeltaTable(uri, storage_options=options).file_uris()

    with (
        pytest.raises(RuntimeError, match="Result creation failed"),
        first.storage_backend.edit_edge_properties_by_id(
            edge_id=1, rel_type="WROTE", properties={"since": 2025}
        ) as pending,
    ):
        assert pending is not None and pending[0] is not None
        raise RuntimeError("Result creation failed")
    assert first.storage_backend.capture_snapshot() == base

    with first.storage_backend.edit_edge_properties_by_id(
        edge_id=1, rel_type="WROTE", properties={"since": 2025}
    ) as pending:
        assert pending is not None and pending[0] is not None
        second.use_cases.cypher.execute(SET_EDGE, {"id": 3, "since": 2026})
        intermediate = second.storage_backend.capture_snapshot()
    current = first.use_cases.graph.load_state()
    assert current.snapshot.snapshot_id.value == base.snapshot_id.value + 2
    assert current.edges[0].properties == {"since": 2025}
    assert current.edges[2].properties == {"since": 2026}
    assert first.use_cases.graph.load_state(snapshot=intermediate).edges[0].properties == {
        "since": 2020,
    }
    after = DeltaTable(uri, storage_options=options).file_uris()
    assert sorted(file for file in after if "kind=metadata/" in file) == sorted(
        file for file in before if "kind=metadata/" in file
    )


@pytest.mark.parametrize("delta_database", ["local"], indirect=True)
@pytest.mark.parametrize("concurrent_change", ["same_range", "catalog", "compact"])
def test_delta_point_set_rejects_conflicting_changes(
    delta_database: tuple[str | Path, GraphArrowSettings], concurrent_change: str
) -> None:
    path, settings = delta_database
    settings = settings.model_copy(update={"edge_parquet_range_size": 2})
    first = build_application(path, settings=settings)
    second = build_application(path, settings=settings)
    first.use_cases.cypher.execute(CREATE_EDGE_RANGES)

    with (
        pytest.raises(WriteConflictError),
        first.storage_backend.edit_edge_properties_by_id(
            edge_id=1, rel_type="WROTE", properties={"since": 2025}
        ) as pending,
    ):
        assert pending is not None and pending[0] is not None
        if concurrent_change == "same_range":
            second.use_cases.cypher.execute(SET_EDGE, {"id": 2, "since": 2026})
        elif concurrent_change == "compact":
            second.use_cases.admin.compact()
        else:
            second.use_cases.indexes.create_index(
                name="author_name",
                kind="range",
                owner_kind="label",
                owner_name="Author",
                property_names=("name",),
            )
        latest = second.storage_backend.capture_snapshot()
    current = first.use_cases.graph.load_state()
    assert current.snapshot == latest
    assert current.edges[0].properties == {"since": 2020}
    if concurrent_change == "catalog":
        assert [index.name for index in current.catalog.indexes] == ["author_name"]


@pytest.mark.parametrize("delta_database", ["local"], indirect=True)
@pytest.mark.parametrize("reported_conflict", [False, True])
def test_delta_finds_rebased_commit_after_lost_acknowledgment(
    delta_database: tuple[str | Path, GraphArrowSettings],
    monkeypatch: pytest.MonkeyPatch,
    reported_conflict: bool,
) -> None:
    from deltalake import DeltaTable
    from deltalake.exceptions import CommitFailedError

    path, settings = delta_database
    settings = settings.model_copy(update={"edge_parquet_range_size": 2})
    first = build_application(path, settings=settings)
    second = build_application(path, settings=settings)
    first.use_cases.cypher.execute(CREATE_EDGE_RANGES)
    assert (
        _post(TestClient(create_rest_app(path, settings=settings)), "Replay", "receipt").status_code
        == 200
    )
    base = first.storage_backend.capture_snapshot()
    assert base.idempotency_receipt is not None
    original = DeltaTable.update

    def lose_acknowledgment(table: Any, **kwargs: Any) -> Any:
        metrics = original(table, **kwargs)
        if "edge_id = 1)" in kwargs["predicate"]:
            second.use_cases.cypher.execute(SET_EDGE, {"id": 3, "since": 2027})
            if reported_conflict:
                raise CommitFailedError("A retry encountered the already published commit.")
            raise OSError("The write succeeded but the connection was lost.")
        return metrics

    monkeypatch.setattr(DeltaTable, "update", lose_acknowledgment)
    with first.storage_backend.edit_edge_properties_by_id(
        edge_id=1, rel_type="WROTE", properties={"since": 2025}
    ) as pending:
        assert pending is not None and pending[0] is not None
        second.use_cases.cypher.execute(SET_EDGE, {"id": 3, "since": 2026})
    state = first.use_cases.graph.load_state()
    assert state.snapshot.snapshot_id.value == base.snapshot_id.value + 3
    assert state.snapshot.idempotency_receipt is None
    assert state.edges[0].properties == {"since": 2025}
    assert state.edges[2].properties == {"since": 2027}
    snapshots = first.repositories.snapshot
    pointer = snapshots.load_current_pointer()
    _, receipt = snapshots.idempotency_base(pointer, sha256(b"receipt").hexdigest())
    assert receipt == base.idempotency_receipt


@pytest.mark.parametrize("delta_database", ["local"], indirect=True)
def test_delta_reuses_unchanged_ranges_in_cypher_projections_and_ana_tables(
    delta_database: tuple[str | Path, GraphArrowSettings], monkeypatch: pytest.MonkeyPatch
) -> None:
    from grapharrow.repositories.graph import _edge_property_schema, _edge_property_table
    from grapharrow.storage import delta_graph

    path, settings = delta_database
    settings = settings.model_copy(update={"edge_parquet_range_size": 2})
    writer = build_application(path, settings=settings)
    reader = build_application(path, settings=settings)
    writer.use_cases.cypher.execute(
        "CREATE (a:Author)-[:WROTE {since:2020, mixed:42, local:true}]->(b:Book) "
        "CREATE (a)-[:WROTE {since:2020, mixed:'text'}]->(b), "
        "(a)-[:WROTE {since:2020, mixed:[1,2]}]->(b), (a)-[:WROTE {since:2020}]->(b) "
        "CREATE (a)-[:WROTE {since:2020, mixed:3.5}]->(b)"
    )
    graph = reader.repositories.graph
    load_partition = graph._read_partition
    project_edges = delta_graph._edge_projection
    reads: list[str] = []
    projections: list[list[int]] = []

    def observe_partition(
        snapshot_id: Any, kind: str, columns: list[str], expression: object
    ) -> Any:
        if kind.startswith("edges-"):
            reads.append(kind)
        return load_partition(snapshot_id, kind, columns, expression)

    def observe_projection(table: Any) -> Any:
        projections.append(table["edge_id"].to_pylist())
        return project_edges(table)

    def project(snapshot: Any) -> Any:
        return graph.load_edge_property_table(snapshot.edge_properties, rel_type="WROTE")

    def ana_since(workflow: GraphArrowAgentBackend) -> list[list[object]]:
        result = workflow.call(
            "relationship_query",
            {
                "source_label": "Author",
                "target_label": "Book",
                "relationship_type": "WROTE",
                "rel_as": "r",
                "select": [{"var": "r", "property": "since", "alias": "since"}],
            },
        )
        return workflow.call("fetch", {"from": result["handle"]})["rows"]

    monkeypatch.setattr(graph, "_read_partition", observe_partition)
    monkeypatch.setattr(delta_graph, "_edge_projection", observe_projection)
    base = reader.storage_backend.capture_snapshot()
    before = project(base)
    assert projections == [[1, 2], [3, 4], [5]]
    assert len(reads) == 3
    with GraphArrowAgentBackend(reader) as pinned:
        assert ana_since(pinned) == [[2020]] * 5
        reads.clear()
        projections.clear()
        writer.use_cases.cypher.execute(SET_EDGE, {"id": 3, "since": 2025})
        current = reader.storage_backend.capture_snapshot()
        with GraphArrowAgentBackend(reader) as fresh:
            assert sorted(ana_since(fresh)) == [[2020]] * 4 + [[2025]]
        assert reads == ["edges-00000000000000000001"]
        after = project(current)
        assert projections == [[3, 4]]
        assert reads == ["edges-00000000000000000001"]
        assert project(base).equals(before, check_metadata=True)
        assert ana_since(pinned) == [[2020]] * 5
    records = list(reader.use_cases.graph.load_state(snapshot=current).edges)
    assert after.equals(
        _edge_property_table(records, _edge_property_schema(records)), check_metadata=True
    )


@pytest.mark.parametrize("delta_database", ["local"], indirect=True)
def test_delta_ana_reuses_node_data_until_node_files_change(
    delta_database: tuple[str | Path, GraphArrowSettings], monkeypatch: pytest.MonkeyPatch
) -> None:
    from collections import Counter
    from functools import wraps

    from grapharrow.use_cases import ana_node_lookup

    path, settings = delta_database
    writer = build_application(path, settings=settings)
    reader = build_application(path, settings=settings)
    writer.use_cases.cypher.execute(CREATE, {"name": "Original"})
    calls: Counter[str] = Counter()

    def count(owner: object, name: str) -> None:
        original = getattr(owner, name)

        @wraps(original)
        def observed(*args: Any, **kwargs: Any) -> Any:
            calls[name] += 1
            return original(*args, **kwargs)

        monkeypatch.setattr(owner, name, observed)

    count(ana_node_lookup, "_build_lookup")
    count(reader.repositories.graph, "load_node_table")
    count(reader.repositories.graph, "load_node_property_projection")
    count(GraphArrowAgentBackend, "_discover_node_properties")

    def author_properties(workflow: GraphArrowAgentBackend) -> set[str]:
        response = workflow.call("schema_describe_label", {"label": "Author"})
        return {prop["name"] for prop in response["properties"]}

    with GraphArrowAgentBackend(reader) as initial:
        assert author_properties(initial) == {"name"}
    baseline = calls.copy()
    assert baseline == {
        "_build_lookup": 1,
        "load_node_table": 1,
        "load_node_property_projection": 2,
        "_discover_node_properties": 1,
    }
    with GraphArrowAgentBackend(reader) as pinned:
        assert calls == baseline
        writer.use_cases.cypher.execute(SET_EDGE, {"id": 1, "since": 2025})
        with GraphArrowAgentBackend(reader) as edge_update:
            assert edge_update._state.snapshot != pinned._state.snapshot
            assert edge_update._state.node_table is pinned._state.node_table
            assert edge_update._property_tables["Author"] is pinned._property_tables["Author"]
            assert author_properties(edge_update) == {"name"}
            assert calls == baseline

        writer.use_cases.cypher.execute("MATCH (a:Author) SET a.rank = 7")
        with GraphArrowAgentBackend(reader) as node_update:
            assert author_properties(node_update) == {"name", "rank"}
            assert node_update._state.node_table is not pinned._state.node_table
            assert node_update._property_tables["Author"]["rank"].to_pylist() == [7]
            assert calls == {name: times * 2 for name, times in baseline.items()}
        assert author_properties(pinned) == {"name"}
