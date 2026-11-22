"""Exercise Delta graph commits, pinned snapshots and REST replay through replicas."""

from __future__ import annotations

import json
import multiprocessing
import os
import signal
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from multiprocessing.connection import Connection
from pathlib import Path
from threading import Barrier, Event
from typing import Any

import orjson
import pytest
from fastapi.testclient import TestClient
from starlette.types import Message, Receive, Scope, Send

from grapharrow.adapters import FileSystemAdapter
from grapharrow.app import build_application
from grapharrow.config import GraphArrowSettings
from grapharrow.core import DatabaseLayout
from grapharrow.core.errors import StorageError, WriteConflictError
from grapharrow.domain import SnapshotId
from grapharrow.domain.snapshot import IdempotencyReceipt
from grapharrow.interfaces.cli import main
from grapharrow.interfaces.embedded import Database
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


def test_delta_queues_late_readers_for_a_fresh_shared_head_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("deltalake")
    from grapharrow.storage import delta

    backend = delta.DeltaStorageBackend(
        layout=DatabaseLayout("s3://grapharrow-test/db"),
        settings=GraphArrowSettings(),
        filesystem=FileSystemAdapter(),
    )
    snapshot = SnapshotId(2)
    first_entered, release_first, second_queued, shared = (Event() for _ in range(4))
    calls: list[SnapshotId] = []
    futures: list[Future[bool]] = []

    class ProbeFuture(Future[bool]):
        def __init__(self) -> None:
            super().__init__()
            futures.append(self)
            if len(futures) == 2:
                second_queued.set()

        def result(self, timeout: float | None = None) -> bool:
            if not self.done():
                shared.set()
            return super().result(timeout)

    def probe(snapshot_id: SnapshotId) -> bool:
        calls.append(snapshot_id)
        if len(calls) == 1:
            first_entered.set()
            assert release_first.wait(5)
            return True
        return False

    def collect(seconds: float) -> None:
        if second_queued.is_set():
            assert shared.wait(5)

    monkeypatch.setattr(delta, "Future", ProbeFuture)
    monkeypatch.setattr(delta, "sleep", collect)
    monkeypatch.setattr(backend, "_probe_next_commit", probe)
    with ThreadPoolExecutor(3) as workers:
        first = workers.submit(backend._cached_snapshot_is_current, snapshot)
        try:
            assert first_entered.wait(5)
            second = workers.submit(backend._cached_snapshot_is_current, snapshot)
            assert second_queued.wait(5)
            third = workers.submit(backend._cached_snapshot_is_current, snapshot)
            assert shared.wait(5)
            assert second.result(5) is False and third.result(5) is False
        finally:
            release_first.set()
        assert first.result(5) is True
    assert calls == [snapshot, snapshot]


def test_managed_delta_ana_opens_while_the_local_writer_lock_is_held(tmp_path: Path) -> None:
    pytest.importorskip("deltalake")
    app = build_application(
        tmp_path / "graph",
        settings=GraphArrowSettings(storage_backend="delta", delta_managed_retention=True),
    )
    app.use_cases.cypher.execute("CREATE (:Person)")

    def open_session() -> int:
        with GraphArrowAgentBackend(app) as session:
            result = session.call("count_nodes", {"label": "Person"})
            assert session.call("fetch", {"from": result["handle"]})["rows"] == [[1]]
            inventory = app.storage_backend.maintenance.storage_inventory()
            assert inventory.retention is not None
            return inventory.retention.active_pins

    with ThreadPoolExecutor(1) as workers:
        with app.storage_backend.writer.hold():
            assert workers.submit(open_session).result(timeout=5) == 1
    inventory = app.storage_backend.maintenance.storage_inventory()
    assert inventory.retention is not None and inventory.retention.active_pins == 0


@pytest.mark.parametrize("delta_database", ["local"], indirect=True)
def test_delta_checkpoint_interval_is_persisted_for_all_replicas(
    delta_database: tuple[str | Path, GraphArrowSettings], monkeypatch: pytest.MonkeyPatch
) -> None:
    from deltalake import DeltaTable

    path, settings = delta_database
    monkeypatch.setenv("GRAPHARROW_STORAGE_BACKEND", "delta")
    monkeypatch.setenv("GRAPHARROW_DELTA_CHECKPOINT_INTERVAL", "3")
    writer = build_application(path)
    writer.use_cases.cypher.execute(CREATE, {"name": "Graph"})
    replica = build_application(
        path, settings=settings.model_copy(update={"delta_checkpoint_interval": 5})
    )
    replica.use_cases.cypher.execute(SET_EDGE, {"id": 1, "since": 2025})
    table = DeltaTable(str(replica.layout.delta_dir))
    assert table.metadata().configuration["delta.checkpointInterval"] == "3"
    inventory = replica.use_cases.admin.storage_inventory()
    assert inventory.delta_version == inventory.latest_checkpoint_version == 2
    assert inventory.commits_since_checkpoint == 0


@pytest.mark.parametrize("delta_database", ["local", "s3"], indirect=True)
def test_delta_snapshot_probe_reuses_the_head_and_detects_remote_commits(
    delta_database: tuple[str | Path, GraphArrowSettings],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from grapharrow.storage.delta import DeltaStorageBackend

    path, settings = delta_database
    writer = build_application(path, settings=settings)
    reader = build_application(path, settings=settings)
    backend = reader.storage_backend
    assert isinstance(backend, DeltaStorageBackend)
    writer.use_cases.cypher.execute(CREATE, {"name": "Graph"})
    pinned = backend.capture_snapshot()
    open_table = backend._try_open
    opens = 0

    def count_opens():
        nonlocal opens
        opens += 1
        return open_table()

    monkeypatch.setattr(backend, "_try_open", count_opens)
    assert backend.capture_snapshot() is pinned
    assert opens == 0

    writer.use_cases.cypher.execute(SET_EDGE, {"id": 1, "since": 2025})
    writer.use_cases.cypher.execute(SET_EDGE, {"id": 1, "since": 2026})
    current = backend.capture_snapshot()
    assert current.snapshot_id.value == pinned.snapshot_id.value + 2
    assert opens == 1
    assert backend.capture_snapshot() is current
    assert opens == 1

    graph = reader.use_cases.graph
    assert graph.load_state(snapshot=pinned).edges[0].properties == {"since": 2020}
    assert graph.load_state(snapshot=current).edges[0].properties == {"since": 2026}


@pytest.mark.parametrize("commit_during_refresh", [False, True])
def test_delta_shares_head_refresh_and_rechecks_after_waiting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, commit_during_refresh: bool
) -> None:
    pytest.importorskip("deltalake")
    from grapharrow.storage.delta import DeltaStorageBackend

    settings = GraphArrowSettings(storage_backend="delta", delta_managed_retention=True)
    writer = build_application(tmp_path / "graph", settings=settings)
    writer.use_cases.cypher.execute(CREATE, {"name": "Graph"})
    reader = build_application(tmp_path / "graph", settings=settings)
    backend = reader.storage_backend
    assert isinstance(backend, DeltaStorageBackend)
    stale = backend.capture_snapshot().snapshot_id
    writer.use_cases.cypher.execute(SET_EDGE, {"id": 1, "since": 2025})

    opened, resume_refresh, waiter_probed, borrowing, release_pin = (Event() for _ in range(5))
    open_table = backend._try_open
    probe = backend._probe_next_commit
    opened_versions: list[int] = []

    def gated_open():
        table = open_table()
        assert table is not None
        opened_versions.append(table.version() + 1)
        if len(opened_versions) == 1:
            opened.set()
            assert resume_refresh.wait(5)
        return table

    def record_stale_probe(snapshot_id: SnapshotId) -> bool:
        is_current = probe(snapshot_id)
        if snapshot_id == stale and opened.is_set():
            waiter_probed.set()
        return is_current

    def hold_snapshot() -> SnapshotId:
        with backend.borrow_current_snapshot() as snapshot:
            borrowing.set()
            assert release_pin.wait(5)
            return snapshot.snapshot_id

    monkeypatch.setattr(backend, "_try_open", gated_open)
    monkeypatch.setattr(backend, "_probe_next_commit", record_stale_probe)
    with ThreadPoolExecutor(2) as workers:
        leader = workers.submit(hold_snapshot)
        try:
            assert opened.wait(5)
            if commit_during_refresh:
                writer.use_cases.cypher.execute(SET_EDGE, {"id": 1, "since": 2026})
            waiter = workers.submit(backend.capture_snapshot)
            assert waiter_probed.wait(5)
            resume_refresh.set()
            latest = waiter.result(5).snapshot_id
            assert borrowing.wait(5)
            assert latest.value == stale.value + 1 + int(commit_during_refresh)
            assert opened_versions == list(range(stale.value + 1, latest.value + 1))
            retention = writer.storage_backend.maintenance.storage_inventory().retention
            assert retention is not None and retention.active_pins == 1
            assert retention.oldest_pinned_snapshot_id == stale.value + 1
        finally:
            resume_refresh.set()
            release_pin.set()
        assert leader.result(5).value == stale.value + 1
    retention = writer.storage_backend.maintenance.storage_inventory().retention
    assert retention is not None and retention.active_pins == 0


def test_delta_waiters_share_a_refresh_started_after_they_arrive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("deltalake")
    from grapharrow.storage.delta import DeltaStorageBackend

    settings = GraphArrowSettings(storage_backend="delta", delta_managed_retention=True)
    writer = build_application(tmp_path / "graph", settings=settings)
    writer.use_cases.cypher.execute(CREATE, {"name": "Graph"})
    reader = build_application(tmp_path / "graph", settings=settings).storage_backend
    assert isinstance(reader, DeltaStorageBackend)
    stale = reader.capture_snapshot().snapshot_id
    writer.use_cases.cypher.execute(SET_EDGE, {"id": 1, "since": 2025})
    probe = reader._probe_next_commit
    entered = Barrier(2)
    checked: list[SnapshotId] = []

    def synchronize_probe(snapshot_id: SnapshotId) -> bool:
        checked.append(snapshot_id)
        if snapshot_id == stale:
            entered.wait(5)
        return probe(snapshot_id)

    monkeypatch.setattr(reader, "_probe_next_commit", synchronize_probe)
    with ThreadPoolExecutor(2) as workers:
        first = workers.submit(reader.capture_snapshot)
        second = workers.submit(reader.capture_snapshot)
        snapshot = first.result(5)
        assert snapshot == second.result(5)
        assert snapshot.snapshot_id.value == stale.value + 1
    assert checked == [stale, stale]


@pytest.mark.parametrize("delta_database", ["local", "s3"], indirect=True)
def test_delta_refresh_reuses_only_unchanged_graph_metadata(
    delta_database: tuple[str | Path, GraphArrowSettings], monkeypatch: pytest.MonkeyPatch
) -> None:
    from grapharrow.storage import delta

    path, settings = delta_database
    writer = build_application(path, settings=settings)
    writer.use_cases.cypher.execute(CREATE, {"name": "Graph"})
    reader = build_application(path, settings=settings).storage_backend
    before = reader.capture_snapshot()
    writer.use_cases.cypher.execute(SET_EDGE, {"id": 1, "since": 2025})
    load_metadata = delta._metadata
    opened_versions: list[int] = []

    def count_reads(table):
        opened_versions.append(table.version() + 1)
        return load_metadata(table)

    with monkeypatch.context() as patch:
        patch.setattr(delta, "_metadata", count_reads)
        point_update = reader.capture_snapshot()
        assert point_update.snapshot_id.value == before.snapshot_id.value + 1
        assert opened_versions == []

    writer.use_cases.cypher.execute("CREATE (:NewLabel)")
    monkeypatch.setattr(delta, "_metadata", count_reads)
    after = reader.capture_snapshot()
    assert after.snapshot_id.value == point_update.snapshot_id.value + 1
    assert [file.path.rsplit("/", 1)[-1] for file in after.node_labels] == [
        "Author",
        "Book",
        "NewLabel",
    ]
    assert opened_versions == [after.snapshot_id.value]


@pytest.mark.parametrize("delta_database", ["local", "s3"], indirect=True)
def test_delta_storage_inventory_reports_physical_files_and_log_tail(
    delta_database: tuple[str | Path, GraphArrowSettings],
) -> None:
    from grapharrow.storage.delta import DeltaStorageBackend

    path, settings = delta_database
    writer = build_application(path, settings=settings)
    reader = Database(path, settings=settings)
    backend = writer.storage_backend
    assert isinstance(backend, DeltaStorageBackend)
    writer.use_cases.cypher.execute(CREATE, {"name": "Graph"})
    before = reader.storage_inventory()
    assert before.head_snapshot_id == 2
    assert before.delta_version == 1
    assert before.active_parquet.file_count == 3
    assert before.checkpoints.file_count == 0
    assert before.latest_checkpoint_version is None
    assert before.commit_logs.file_count == before.commits_since_checkpoint == 2

    table = backend._require_table(None)
    table.create_checkpoint()
    writer.use_cases.cypher.execute(SET_EDGE, {"id": 1, "since": 2025})
    staged = b"unpublished staged data"
    location = f"{writer.layout.delta_dir}/kind=nodes/staged.parquet"
    filesystem, staged_path = writer.adapters.filesystem.resolve(location)
    with filesystem.open_output_stream(staged_path) as stream:
        stream.write(staged)
    inventory = reader.storage_inventory()
    assert inventory.head_snapshot_id == 3
    assert inventory.delta_version == 2
    assert inventory.latest_checkpoint_version == 1
    assert inventory.commits_since_checkpoint == 1
    assert inventory.commit_logs.file_count == 3
    assert inventory.commit_logs.size_bytes > before.commit_logs.size_bytes
    assert inventory.checkpoints.file_count == 1
    assert inventory.checkpoints.size_bytes > 0
    assert inventory.active_parquet.file_count == before.active_parquet.file_count
    assert inventory.missing_active_parquet_count == 0
    assert inventory.historical_parquet.file_count == before.historical_parquet.file_count + 1
    assert inventory.historical_parquet.size_bytes > before.historical_parquet.size_bytes
    assert inventory.untracked_parquet.file_count == 1
    assert inventory.untracked_parquet.size_bytes == len(staged)
    oldest = inventory.historical_parquet.oldest_modified_at
    newest = inventory.historical_parquet.newest_modified_at
    assert oldest and newest and datetime.fromisoformat(oldest) <= datetime.fromisoformat(newest)
    assert reader.storage_inventory() == inventory
    with filesystem.open_input_file(staged_path) as stream:
        assert stream.read() == staged
    assert backend.capture_snapshot().snapshot_id.value == 3


@pytest.mark.parametrize("delta_database", ["local"], indirect=True)
def test_delta_inventory_uses_one_snapshot_when_a_replica_publishes_during_listing(
    delta_database: tuple[str | Path, GraphArrowSettings], monkeypatch: pytest.MonkeyPatch
) -> None:
    from grapharrow.storage import delta

    path, settings = delta_database
    writer = build_application(path, settings=settings)
    reader = build_application(path, settings=settings)
    writer.use_cases.cypher.execute(CREATE, {"name": "Graph"})
    before = reader.use_cases.admin.storage_inventory()
    inspect_storage = delta.inspect_delta_storage

    def publish_then_inspect(*args: Any) -> Any:
        writer.use_cases.cypher.execute(SET_EDGE, {"id": 1, "since": 2025})
        writer.storage_backend.maintenance.compact()
        return inspect_storage(*args)

    with monkeypatch.context() as patch:
        patch.setattr(delta, "inspect_delta_storage", publish_then_inspect)
        during = reader.use_cases.admin.storage_inventory()
    assert during.head_snapshot_id == before.head_snapshot_id
    assert during.active_parquet == before.active_parquet
    assert during.commit_logs == before.commit_logs
    assert during.historical_parquet == before.historical_parquet
    assert during.untracked_parquet.file_count > 0
    after = reader.use_cases.admin.storage_inventory()
    assert after.head_snapshot_id == before.head_snapshot_id + 2
    assert after.untracked_parquet.file_count == 0


@pytest.mark.parametrize("delta_database", ["local"], indirect=True)
def test_delta_storage_inventory_cli_and_authenticated_rest(
    delta_database: tuple[str | Path, GraphArrowSettings],
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path, settings = delta_database
    database = Database(path, settings=settings)
    database.initialize()
    expected = asdict(database.storage_inventory())
    monkeypatch.setenv("GRAPHARROW_STORAGE_BACKEND", "delta")
    assert main(["storage-inventory", str(path)]) == 0
    assert json.loads(capsys.readouterr().out) == expected
    with TestClient(
        create_rest_app(path, settings=settings.model_copy(update={"server_auth_token": "secret"}))
    ) as client:
        route = "/v1/admin/storage/inventory"
        assert client.get(route).status_code == 401
        response = client.get(route, headers={"Authorization": "Bearer secret"})
    assert response.status_code == 200
    assert response.json()["data"] == expected


@pytest.mark.parametrize("delta_database", ["local"], indirect=True)
def test_delta_storage_inventory_does_not_initialize_a_missing_database(
    delta_database: tuple[str | Path, GraphArrowSettings],
) -> None:
    path, settings = delta_database
    assert isinstance(path, Path)
    with pytest.raises(StorageError, match="Delta graph does not exist"):
        Database(path, settings=settings).storage_inventory()
    assert not path.exists()


@pytest.mark.parametrize("delta_database", ["local"], indirect=True)
def test_delta_storage_inventory_rejects_missing_commit_json_even_after_a_checkpoint(
    delta_database: tuple[str | Path, GraphArrowSettings],
) -> None:
    from grapharrow.storage.delta import DeltaStorageBackend

    path, settings = delta_database
    app = build_application(path, settings=settings)
    backend = app.storage_backend
    assert isinstance(backend, DeltaStorageBackend)
    app.use_cases.cypher.execute(CREATE, {"name": "Graph"})
    backend._require_table(None).create_checkpoint()
    backend._log_storage.delete_file("_delta_log/00000000000000000000.json")
    with pytest.raises(StorageError, match="requires the complete commit log"):
        app.use_cases.admin.storage_inventory()


@pytest.mark.parametrize("delta_database", ["local", "s3"], indirect=True)
def test_delta_retention_preview_respects_a_pinned_reader_and_rest_replay(
    delta_database: tuple[str | Path, GraphArrowSettings],
) -> None:
    import pyarrow as pa

    from grapharrow.domain import SnapshotId
    from grapharrow.storage.delta import DeltaStorageBackend

    path, settings = delta_database
    writer = build_application(path, settings=settings)
    reader = build_application(path, settings=settings)
    backend = writer.storage_backend
    assert isinstance(backend, DeltaStorageBackend)
    database = Database(path, settings=settings)
    with TestClient(create_rest_app(path, settings=settings)) as client:
        original = _post(client, "Graph", "once")
        assert original.status_code == 200
        with GraphArrowAgentBackend(reader) as workflow:
            pinned = workflow._state.snapshot
            for year in (2025, 2026, 2027):
                writer.use_cases.cypher.execute(SET_EDGE, {"id": 1, "since": year})
            head = backend.capture_snapshot()
            assert head.snapshot_id.value == 5
            backend._require_table(None).create_checkpoint()

            filesystem, staged = writer.adapters.filesystem.resolve(
                f"{writer.layout.delta_dir}/kind=nodes/staged.parquet"
            )
            with filesystem.open_output_stream(staged) as stream:
                stream.write(b"unpublished")
            before = database.storage_inventory()
            assert before.untracked_parquet.file_count == 1

            def files_at(snapshot_id: int) -> set[str]:
                table = backend._require_table(SnapshotId(snapshot_id))
                return {
                    f"delta/{file}"
                    for file in pa.table(table.get_add_actions()).column("path").to_pylist()
                }

            plan = database.plan_retention(keep_last=2)
            assert plan.retained_snapshot_ids == (5, 4)
            assert plan.older_snapshot_ids == (3, 2, 1)
            retained = files_at(5) | files_at(4)
            older = files_at(3) | files_at(2) | files_at(1)
            assert {file.path for file in plan.candidate_files} == older - retained
            assert set(plan.shared_parquet_paths) == older & retained
            assert not plan.missing_candidate_paths
            for candidate in plan.candidate_files:
                assert (
                    writer.adapters.filesystem.file_size(f"{writer.layout.root}/{candidate.path}")
                    == candidate.size_bytes
                )
            assert plan.estimated_reclaimable_bytes == sum(
                file.size_bytes for file in plan.candidate_files
            )

            protected = database.plan_retention(
                keep_last=2, protect_from_snapshot_id=pinned.snapshot_id.value
            )
            assert protected.retained_snapshot_ids == (5, 4, 3, 2)
            assert {file.path for file in protected.candidate_files} == files_at(1) - (
                retained | files_at(3) | files_at(2)
            )
            assert database.storage_inventory() == before
            assert reader.use_cases.graph.load_state(snapshot=pinned).edges[0].properties == {
                "since": 2020
            }
            assert _post(client, "Graph", "once").content == original.content
            assert _post(client, "Other", "once").status_code == 409
            assert backend.capture_snapshot() == head


@pytest.mark.parametrize("delta_database", ["local"], indirect=True)
def test_delta_retention_uses_commit_age_and_reports_a_missing_older_file(
    delta_database: tuple[str | Path, GraphArrowSettings],
) -> None:
    path, settings = delta_database
    assert isinstance(path, Path)
    app = build_application(path, settings=settings)
    app.use_cases.cypher.execute(CREATE, {"name": "Graph"})
    app.use_cases.cypher.execute(SET_EDGE, {"id": 1, "since": 2025})
    app.use_cases.cypher.execute(SET_EDGE, {"id": 1, "since": 2026})
    now = datetime.now(UTC)
    for version, age in enumerate((48, 2, 24, 1)):
        log = path / "delta" / "_delta_log" / f"{version:020d}.json"
        actions = [orjson.loads(line) for line in log.read_bytes().splitlines()]
        commit = next(action["commitInfo"] for action in actions if "commitInfo" in action)
        commit["timestamp"] = int((now - timedelta(hours=age)).timestamp() * 1000)
        log.write_bytes(b"\n".join(orjson.dumps(action) for action in actions) + b"\n")

    database = Database(path, settings=settings)
    plan = database.plan_retention(keep_last=1, min_age_hours=4)
    assert plan.retained_snapshot_ids == (4, 3, 2)
    assert plan.older_snapshot_ids == (1,)
    (candidate,) = plan.candidate_files
    (path / candidate.path).unlink()
    updated = database.plan_retention(keep_last=1, min_age_hours=4)
    assert updated.missing_candidate_paths == (candidate.path,)
    assert updated.estimated_reclaimable_bytes == 0


@pytest.mark.parametrize("delta_database", ["local"], indirect=True)
def test_delta_retention_plan_cli_and_authenticated_rest(
    delta_database: tuple[str | Path, GraphArrowSettings],
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path, settings = delta_database
    app = build_application(path, settings=settings)
    app.use_cases.cypher.execute(CREATE, {"name": "Graph"})
    monkeypatch.setenv("GRAPHARROW_STORAGE_BACKEND", "delta")
    assert (
        main(["retention-plan", str(path), "--keep-last", "1", "--protect-from-snapshot-id", "1"])
        == 0
    )
    cli_plan = json.loads(capsys.readouterr().out)
    with TestClient(
        create_rest_app(path, settings=settings.model_copy(update={"server_auth_token": "secret"}))
    ) as client:
        route = "/v1/admin/retention/plan"
        params = {"keep_last": 1, "protect_from_snapshot_id": 1}
        assert client.get(route, params=params).status_code == 401
        auth = {"Authorization": "Bearer secret"}
        response = client.get(route, params=params, headers=auth)
        assert response.status_code == 200
        assert (
            client.get(
                route, params={**params, "protect_from_snapshot_id": 3}, headers=auth
            ).status_code
            == 400
        )
    rest_plan = response.json()["data"]
    assert rest_plan.pop("evaluated_at") and cli_plan.pop("evaluated_at")
    assert rest_plan == cli_plan
    assert rest_plan["retained_snapshot_ids"] == [2, 1]
    assert rest_plan["candidate_files"] == []


@pytest.mark.parametrize("delta_database", ["local"], indirect=True)
@pytest.mark.parametrize("warm", [False, True], ids=["fresh-head", "cached-head"])
@pytest.mark.parametrize("operation", ["ana", "point-set"])
def test_managed_delta_holds_one_admission_from_snapshot_selection_to_completion(
    delta_database: tuple[str | Path, GraphArrowSettings],
    monkeypatch: pytest.MonkeyPatch,
    warm: bool,
    operation: str,
) -> None:
    from grapharrow.storage.delta import DeltaStorageBackend

    path, settings = delta_database
    writer = build_application(
        path, settings=settings.model_copy(update={"delta_managed_retention": True})
    )
    writer.use_cases.cypher.execute(CREATE, {"name": "Graph"})
    app = build_application(path, settings=settings)
    backend = app.storage_backend
    assert isinstance(backend, DeltaStorageBackend)
    admission = backend._retention
    if warm:
        assert backend.capture_snapshot().snapshot_id.value == 2

    admit = admission._admit
    admissions: list[int] = []

    def record_admission(snapshot_id: int, token: str) -> None:
        admit(snapshot_id, token)
        admissions.append(snapshot_id)

    def assert_active() -> None:
        assert admissions == [2]
        assert admission.status().oldest_pinned_snapshot_id == 2
        assert admission.status().active_pins == 1

    monkeypatch.setattr(admission, "_admit", record_admission)
    if operation == "ana":
        with GraphArrowAgentBackend(app) as workflow:
            assert_active()
            assert workflow.call("schema_overview", {})["labels"]
            assert_active()
    else:
        publish = backend._commit_point

        def observe_publication(*args: Any) -> None:
            assert_active()
            publish(*args)

        monkeypatch.setattr(backend, "_commit_point", observe_publication)
        assert app.use_cases.cypher.execute(SET_EDGE, {"id": 1, "since": 2025}).rows == (
            {"since": 2025},
        )
    assert admissions == [2]
    assert admission.status().active_pins == 0


@pytest.mark.parametrize("delta_database", ["local", "s3"], indirect=True)
def test_managed_delta_retention_coordinates_live_replicas(
    delta_database: tuple[str | Path, GraphArrowSettings],
) -> None:
    from grapharrow.use_cases.graph import GraphTransactionUseCase

    path, settings = delta_database
    writer_settings = settings.model_copy(update={"delta_managed_retention": True})
    writer = build_application(path, settings=writer_settings)
    reader = build_application(path, settings=settings)
    database = Database(path, settings=settings)
    with TestClient(create_rest_app(path, settings=writer_settings)) as client:
        original = _post(client, "Graph", "once")
        assert original.status_code == 200
        with GraphTransactionUseCase(reader.use_cases.graph) as transaction:
            with GraphArrowAgentBackend(reader) as old:
                old_id = old._state.snapshot.snapshot_id
                status = database.storage_inventory().retention
                assert status is not None and status.active_pins == 1
                writer.use_cases.cypher.execute(SET_EDGE, {"id": 1, "since": 2025})
                head = writer.storage_backend.capture_snapshot().snapshot_id
                with pytest.raises(StorageError, match="Active Delta snapshot pins"):
                    database.apply_retention(minimum_snapshot_id=head.value, online=True)
                assert old.call("schema_overview", {})["labels"]
            assert transaction.load_state().edges[0].properties == {"since": 2020}
            with pytest.raises(StorageError, match="Active Delta snapshot pins"):
                database.apply_retention(minimum_snapshot_id=head.value, online=True)
        with GraphArrowAgentBackend(reader) as current:
            with pytest.raises(StorageError, match="Active Delta snapshot pins"):
                database.apply_retention(minimum_snapshot_id=head.value)
            plan = database.plan_retention(keep_last=1)
            assert plan.candidate_files
            filesystem = database._app.adapters.filesystem
            delete = filesystem.delete_file
            observed_online = False

            def delete_with_concurrent_work(location: str | Path) -> None:
                nonlocal observed_online
                if not observed_online:
                    observed_online = True
                    status = database.storage_inventory().retention
                    assert status is not None and status.cleanup_mode == "online"
                    assert current.call("schema_overview", {})["labels"]
                    assert writer.use_cases.cypher.execute(
                        SET_EDGE, {"id": 1, "since": 2026}
                    ).rows == ({"since": 2026},)
                delete(location)

            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(filesystem, "delete_file", delete_with_concurrent_work)
                result = database.apply_retention(minimum_snapshot_id=head.value, online=True)
            assert observed_online
            assert result.minimum_snapshot_id == head.value
            assert set(result.removed_parquet_paths) == {item.path for item in plan.candidate_files}
        with pytest.raises(StorageError, match=f"snapshot {old_id.value} has expired"):
            database.load_snapshot(old_id)
        with pytest.raises(StorageError, match="already expired"):
            database.plan_retention(keep_last=1, protect_from_snapshot_id=old_id.value)
        assert database.storage_inventory().historical_parquet.file_count == 1
        assert _post(client, "Graph", "once").content == original.content
        assert _post(client, "Different", "once").status_code == 409
        assert database.verify().ok
        assert len(list(reader.repositories.snapshot.history())) == 2


def _retention_process_stops_after_one_delete(path: Path, minimum_snapshot_id: int) -> None:
    app = build_application(path, settings=GraphArrowSettings(storage_backend="delta"))
    delete = app.adapters.filesystem.delete_file

    def delete_and_stop(location: str | Path) -> None:
        delete(location)
        os._exit(77)

    app.adapters.filesystem.delete_file = delete_and_stop  # type: ignore[method-assign]
    app.use_cases.admin.apply_retention(minimum_snapshot_id=minimum_snapshot_id)


@pytest.mark.parametrize("delta_database", ["local"], indirect=True)
def test_managed_delta_retention_recovers_after_the_cleaner_exits(
    delta_database: tuple[str | Path, GraphArrowSettings],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path, settings = delta_database
    assert isinstance(path, Path)
    writer_settings = settings.model_copy(update={"delta_managed_retention": True})
    writer = build_application(path, settings=writer_settings)
    with TestClient(create_rest_app(path, settings=writer_settings)) as client:
        original = _post(client, "Graph", "once")
        assert original.status_code == 200
        old = writer.storage_backend.capture_snapshot().snapshot_id
        for year in (2025, 2026):
            writer.use_cases.cypher.execute(SET_EDGE, {"id": 1, "since": year})
        head = writer.storage_backend.capture_snapshot().snapshot_id
        database = Database(path, settings=settings)
        plan = database.plan_retention(keep_last=1)
        assert len(plan.candidate_files) >= 2
        child = multiprocessing.get_context("spawn").Process(
            target=_retention_process_stops_after_one_delete, args=(path, head.value)
        )
        child.start()
        try:
            child.join(timeout=30)
            assert child.exitcode == 77
        finally:
            if child.is_alive():
                child.kill()
                child.join(timeout=5)

        assert sum(not (path / item.path).exists() for item in plan.candidate_files) == 1
        status = database.storage_inventory().retention
        assert status is not None and status.cleanup_mode == "maintenance"
        assert status.minimum_snapshot_id == head.value
        with pytest.raises(StorageError, match="paused for retention maintenance"):
            database.load_snapshot()
        with pytest.raises(StorageError, match=f"snapshot {old.value} has expired"):
            database.load_snapshot(old)
        with pytest.raises(StorageError, match="same cutoff and mode"):
            database.apply_retention(minimum_snapshot_id=head.value, online=True)

        monkeypatch.setenv("GRAPHARROW_STORAGE_BACKEND", "delta")
        assert main(["retention-apply", str(path), "--minimum-snapshot-id", str(head.value)]) == 0
        assert all(not (path / item.path).exists() for item in plan.candidate_files)
        assert database.load_snapshot().snapshot_id == head
        assert database.storage_inventory().commit_logs.file_count == head.value
        assert database.apply_retention(minimum_snapshot_id=head.value).removed_parquet_paths == ()
        assert _post(client, "Graph", "once").content == original.content
        assert database.verify().ok


def _retention_reader_waits_for_cutover(
    path: str | Path, channel: Connection, settings: GraphArrowSettings | None = None
) -> None:
    app = build_application(path, settings=settings or GraphArrowSettings(storage_backend="delta"))
    with GraphArrowAgentBackend(app) as workflow:
        snapshot = workflow._state.snapshot.snapshot_id
        channel.send(snapshot.value)
        assert channel.poll(30) and channel.recv() == "finish"
        assert workflow.call("schema_overview", {})["labels"]
    channel.send("closed")
    assert channel.poll(30) and channel.recv() == "after-cutover"
    try:
        app.repositories.snapshot.load_manifest(snapshot)
    except StorageError as error:
        channel.send(str(error))
    else:
        raise AssertionError("The suspended replica was able to reopen an expired snapshot")
    channel.close()


@pytest.mark.parametrize("delta_database", ["local"], indirect=True)
def test_managed_delta_retention_keeps_the_pin_of_a_waiting_process(
    delta_database: tuple[str | Path, GraphArrowSettings],
) -> None:
    path, settings = delta_database
    assert isinstance(path, Path)
    writer = build_application(
        path, settings=settings.model_copy(update={"delta_managed_retention": True})
    )
    writer.use_cases.cypher.execute(CREATE, {"name": "Graph"})
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    replica = context.Process(target=_retention_reader_waits_for_cutover, args=(path, child))
    replica.start()
    child.close()
    try:
        assert parent.poll(30)
        old = parent.recv()
        writer.use_cases.cypher.execute(SET_EDGE, {"id": 1, "since": 2025})
        cutoff = writer.storage_backend.capture_snapshot().snapshot_id.value
        maintainer = writer.use_cases.admin
        status = maintainer.storage_inventory().retention
        assert status is not None and status.oldest_pinned_snapshot_id == old
        with pytest.raises(StorageError, match="Active Delta snapshot pins"):
            maintainer.apply_retention(minimum_snapshot_id=cutoff, online=True)
        if os.name == "posix":
            assert replica.pid is not None
            os.kill(replica.pid, signal.SIGSTOP)
            try:
                with pytest.raises(StorageError, match="still holds its process lock"):
                    maintainer.recover_retention_pins(status.pins[0].incarnation_id)
            finally:
                os.kill(replica.pid, signal.SIGCONT)
        parent.send("finish")
        assert parent.poll(30) and parent.recv() == "closed"
        maintainer.apply_retention(minimum_snapshot_id=cutoff, online=True)
        parent.send("after-cutover")
        assert parent.poll(30) and f"snapshot {old} has expired" in parent.recv()
        replica.join(timeout=5)
        assert replica.exitcode == 0
    finally:
        parent.close()
        if replica.is_alive():
            replica.kill()
            replica.join(timeout=5)


@pytest.mark.skipif(os.name != "posix", reason="Requires a POSIX process and verifier executable.")
@pytest.mark.parametrize("delta_database", ["local", "s3"], indirect=True)
def test_managed_delta_recovers_only_a_fenced_incarnation(
    delta_database: tuple[str | Path, GraphArrowSettings],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from grapharrow.domain import SnapshotId
    from grapharrow.storage.delta import DeltaStorageBackend

    path, settings = delta_database
    writer = build_application(
        path, settings=settings.model_copy(update={"delta_managed_retention": True})
    )
    writer.use_cases.cypher.execute(CREATE, {"name": "Graph"})
    database = Database(path, settings=settings)
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    replica = context.Process(
        target=_retention_reader_waits_for_cutover,
        args=(path, child, settings.model_copy(update={"delta_replica_id": "spot-worker"})),
    )
    replica.start()
    child.close()
    try:
        assert parent.poll(30)
        old = parent.recv()
        status = database.storage_inventory().retention
        assert status is not None
        (pin,) = status.pins
        assert pin.replica_id == "spot-worker"
        assert pin.snapshot_id == old and pin.age_seconds >= 0
        assert datetime.fromisoformat(pin.acquired_at).tzinfo is not None
        incarnation = pin.incarnation_id
        verifier = None
        if isinstance(path, Path):
            with pytest.raises(StorageError, match="still holds its process lock"):
                database.recover_retention_pins(incarnation)
        else:
            with pytest.raises(StorageError, match="requires an external fence verifier"):
                database.recover_retention_pins(incarnation)
            backend = writer.storage_backend
            assert isinstance(backend, DeltaStorageBackend)
            table = backend._require_table(None)
            assert replica.pid is not None
            verifier = tmp_path / "local-supervisor"
            verifier.write_text(
                "#!/usr/bin/env python3\n"
                "import json, os, sys\n"
                "request = json.load(sys.stdin)\n"
                f"assert request['database_id'] == {str(table.metadata().id)!r}\n"
                f"assert request['database_location'] == {str(path)!r}\n"
                f"assert request['incarnation_id'] == {incarnation!r}\n"
                "assert request['replica_id'] == 'spot-worker'\n"
                "try:\n"
                f"    os.kill({replica.pid}, 0)\n"
                "except ProcessLookupError:\n"
                "    print(json.dumps({**request, 'fenced': True, 'drained': True}))\n"
                "else:\n"
                "    sys.exit(1)\n"
            )
            verifier.chmod(0o700)
            with pytest.raises(StorageError, match="did not confirm"):
                database.recover_retention_pins(incarnation, verifier=verifier)

        writer.use_cases.cypher.execute(SET_EDGE, {"id": 1, "since": 2025})
        head = writer.storage_backend.capture_snapshot().snapshot_id
        with pytest.raises(StorageError, match="Active Delta snapshot pins"):
            database.apply_retention(minimum_snapshot_id=head.value, online=True)
        replica.kill()
        replica.join(timeout=5)
        assert replica.exitcode == -signal.SIGKILL
        if verifier is not None:
            script = verifier.read_text()
            verifier.write_text(
                script.replace("'fenced': True", "'challenge': 'old-challenge', 'fenced': True")
            )
            with pytest.raises(StorageError, match="did not confirm"):
                database.recover_retention_pins(incarnation, verifier=verifier)
            verifier.write_text(script.replace("'drained': True", "'drained': False"))
            with pytest.raises(StorageError, match="did not confirm"):
                database.recover_retention_pins(incarnation, verifier=verifier)
            verifier.write_text(script)

        new_replica = build_application(path, settings=settings)
        with GraphArrowAgentBackend(new_replica) as current:
            result = database.recover_retention_pins(incarnation, verifier=verifier)
            assert result.incarnation_id == incarnation and result.released_pins == 1
            assert (
                database.recover_retention_pins(incarnation, verifier=verifier).released_pins == 0
            )
            status = database.storage_inventory().retention
            assert status is not None and len(status.pins) == 1
            assert status.pins[0].incarnation_id != incarnation
            database.apply_retention(minimum_snapshot_id=head.value, online=True)
            assert current.call("schema_overview", {})["labels"]

        if isinstance(path, Path):
            monkeypatch.setenv("GRAPHARROW_STORAGE_BACKEND", "delta")
            assert main(["retention-recover-pins", str(path), "--incarnation-id", incarnation]) == 0

        stale_backend = build_application(path, settings=settings).storage_backend
        assert isinstance(stale_backend, DeltaStorageBackend)
        monkeypatch.setattr(stale_backend._retention, "_incarnation_id", incarnation)
        with pytest.raises(StorageError, match="incarnation has been fenced"):
            stale_backend.load_manifest(head)
        with pytest.raises(StorageError, match="has expired"):
            database.load_snapshot(SnapshotId(old))
    finally:
        parent.close()
        if replica.is_alive():
            replica.kill()
            replica.join(timeout=5)


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
            maintainer = build_application(path, settings=settings).use_cases.admin
            inventory = maintainer.storage_inventory()
            if after_publish:
                assert inventory.untracked_parquet.file_count == 0
            else:
                assert inventory.untracked_parquet.file_count >= 4
            preview = maintainer.plan_retention(
                keep_last=1, protect_from_snapshot_id=initial.snapshot_id.value
            )
            assert preview.head_snapshot_id == inventory.head_snapshot_id
            assert preview.retained_snapshot_ids[-1] == initial.snapshot_id.value
            assert preview.candidate_files == ()
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
