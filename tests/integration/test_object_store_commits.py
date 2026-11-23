"""Independent object-storage writers publish an entire graph or lose the commit."""

import multiprocessing
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pytest

from grapharrow.adapters.object_pointer import Revision
from grapharrow.app import build_application
from grapharrow.config import GraphArrowSettings
from grapharrow.core.errors import ReadOnlyError, WriteConflictError
from grapharrow.domain import SnapshotId
from tests.integration.support import MotoS3Context, list_object_keys


def _s3_settings(uri: str, endpoint: str) -> GraphArrowSettings:
    return GraphArrowSettings(
        database_path=uri,
        s3_access_key="test",
        s3_secret_key="test",
        s3_region="us-east-1",
        s3_endpoint=urlparse(endpoint).netloc,
        s3_scheme="http",
    )


def _write_on_pod(
    uri: str, endpoint: str, label: str, channel: Connection, *, bootstrap: bool
) -> None:
    app = build_application(uri, settings=_s3_settings(uri, endpoint))
    fs = app.adapters.filesystem
    replace_pointer = fs.replace_pointer

    def pause(location, payload: object, expected: Revision | None) -> bool:
        channel.send("prepared")
        assert channel.poll(25), "Parent did not release the publisher"
        assert channel.recv() == "publish"
        return replace_pointer(location, payload, expected)

    fs.replace_pointer = pause  # type: ignore[method-assign]
    try:
        if bootstrap:
            result = app.use_cases.admin.initialize()
            channel.send(("opened", result.snapshot.catalog_path))
        else:
            app.use_cases.cypher.execute(
                f"CREATE (a:{label} {{name: 'a'}})-[:LINK {{weight: 1}}]->(b:{label} {{name: 'b'}})"
            )
            channel.send("committed")
    except WriteConflictError:
        channel.send("conflict")
    finally:
        channel.close()


@pytest.mark.integration
@pytest.mark.parametrize("bootstrap", [True, False])
def test_s3_commit_arbitrates_two_independent_writer_processes(
    moto_s3_filesystem: MotoS3Context, bootstrap: bool
) -> None:
    endpoint = moto_s3_filesystem.endpoint_url
    uri = f"{moto_s3_filesystem.bucket_uri}/concurrent"
    reader = build_application(uri, settings=_s3_settings(uri, endpoint))
    prior = None if bootstrap else reader.use_cases.graph.capture_snapshot()

    context = multiprocessing.get_context("spawn")
    channels = [context.Pipe() for _ in range(2)]
    processes = [
        context.Process(
            target=_write_on_pod,
            args=(uri, endpoint, label, child),
            kwargs={"bootstrap": bootstrap},
        )
        for label, (_, child) in zip(("Winner", "Loser"), channels, strict=True)
    ]
    try:
        for process, (_, child) in zip(processes, channels, strict=True):
            process.start()
            child.close()

        winner, loser = (parent for parent, _ in channels)
        for parent in (winner, loser):
            assert parent.poll(25), "Pod did not reach the conditional PUT"
            assert parent.recv() == "prepared"
        if not bootstrap:
            assert reader.use_cases.graph.load_state().snapshot == prior
            assert reader.repositories.catalog.load().labels == ()
        else:
            assert reader.repositories.snapshot.try_load_current_pointer() is None

        winner.send("publish")
        assert winner.poll(25), "Winner did not report its commit"
        result = winner.recv()
        published = reader.repositories.snapshot.load_current_pointer()
        if not bootstrap:
            assert result == "committed"
            assert published.snapshot_id == SnapshotId(2)
            # Another publish makes the racing writer's base two generations old.
            reader.use_cases.admin.compact()
        else:
            assert published.snapshot_id == SnapshotId(1)
            assert result == (
                "opened",
                reader.repositories.snapshot.load_at(published).catalog_path,
            )

        loser.send("publish")
        assert loser.poll(25), "Loser did not report its commit"
        assert loser.recv() == (result if bootstrap else "conflict")

        for process in processes:
            process.join(timeout=15)
            assert process.exitcode == 0

        assert reader.use_cases.admin.verify(verify_checksums=True).ok
        if not bootstrap:
            current = reader.use_cases.graph.load_state()
            assert {label.name for label in current.catalog.labels} == {"Winner"}
            assert len(current.nodes) == 2
            assert len(current.edges) == 1
            assert len(current.snapshot.adjacency_by_source) == 1
            assert len(current.snapshot.adjacency_by_dest) == 1
            assert [
                (path, manifest.snapshot_id.value)
                for path, manifest in reader.repositories.snapshot.history()
            ][1] == (published.manifest_path, 2)
            assert prior is not None
            assert reader.use_cases.graph.load_state(snapshot=prior).nodes == ()
            assert reader.use_cases.admin.vacuum()
            assert reader.use_cases.admin.verify(verify_checksums=True).ok
    finally:
        for parent, child in channels:
            parent.close()
            child.close()
        for process in processes:
            if process.pid is not None and process.is_alive():
                process.kill()
                process.join(timeout=10)


@pytest.mark.integration
def test_s3_lost_put_response_is_resolved_after_the_next_commit(
    moto_s3_filesystem: MotoS3Context, monkeypatch: pytest.MonkeyPatch
) -> None:
    endpoint = moto_s3_filesystem.endpoint_url
    uri = f"{moto_s3_filesystem.bucket_uri}/lost-response"
    settings = _s3_settings(uri, endpoint)
    writer, other = (build_application(uri, settings=settings) for _ in range(2))
    writer.use_cases.admin.initialize()

    fs = writer.adapters.filesystem
    replace_pointer = fs.replace_pointer

    def lose_response(location, payload: object, expected: Revision | None) -> bool:
        assert replace_pointer(location, payload, expected)
        other.use_cases.cypher.execute("CREATE (:Next)")
        raise TimeoutError("The provider accepted the commit before the response was lost")

    monkeypatch.setattr(fs, "replace_pointer", lose_response)
    writer.use_cases.cypher.execute("CREATE (:First)")
    assert writer.use_cases.cypher.execute(
        "MATCH (n) RETURN labels(n) AS labels ORDER BY id(n)"
    ).rows == ({"labels": ["First"]}, {"labels": ["Next"]})
    assert writer.use_cases.admin.verify(verify_checksums=True).ok


@pytest.mark.integration
def test_s3_interrupted_pod_cannot_publish_partially_prepared_state(
    moto_s3_filesystem: MotoS3Context,
) -> None:
    endpoint = moto_s3_filesystem.endpoint_url
    uri = f"{moto_s3_filesystem.bucket_uri}/interrupted"
    remaining = build_application(uri, settings=_s3_settings(uri, endpoint))
    original = remaining.use_cases.graph.capture_snapshot()
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    writer = context.Process(
        target=_write_on_pod,
        args=(uri, endpoint, "Interrupted", child),
        kwargs={"bootstrap": False},
    )
    try:
        writer.start()
        child.close()
        assert parent.poll(25)
        assert parent.recv() == "prepared"
        # The in-flight attempt belongs to the next generation, so vacuum must leave it alone.
        assert remaining.use_cases.admin.vacuum() == ()
        writer.kill()
        writer.join(timeout=15)
        assert writer.exitcode != 0
        assert remaining.use_cases.graph.load_state().snapshot == original
        assert remaining.repositories.catalog.load().labels == ()

        remaining.use_cases.cypher.execute("CREATE (:Survived)")
        assert remaining.use_cases.admin.vacuum()
        state = remaining.use_cases.graph.load_state()
        assert [label.name for label in state.catalog.labels] == ["Survived"]
        assert len(state.nodes) == 1
        assert state.edges == ()
        assert remaining.use_cases.admin.verify(verify_checksums=True).ok
    finally:
        parent.close()
        child.close()
        if writer.pid is not None and writer.is_alive():
            writer.kill()
            writer.join(timeout=10)


@pytest.mark.integration
def test_s3_vacuum_preserves_an_in_flight_commit_and_its_base_snapshot(
    moto_s3_filesystem: MotoS3Context,
) -> None:
    storage = moto_s3_filesystem
    prefix = "vacuum-during-commit/"
    uri = f"{storage.bucket_uri}/{prefix.rstrip('/')}"
    settings = _s3_settings(uri, storage.endpoint_url)
    reader = build_application(uri, settings=settings)
    reader.use_cases.cypher.execute(
        "CREATE (:Original {name: 'origin'})-[:LINK {weight: 5}]->(:Original {name: 'destination'})"
    )
    graph = reader.use_cases.graph
    original = graph.load_state()
    base_head = reader.repositories.snapshot.load_current_pointer()

    # An obsolete candidate shows that vacuum performed a deletion while the writer was paused.
    orphan = f"data/nodes/snapshot-{base_head.snapshot_id.value:06d}-abandoned.parquet"
    orphan_key = f"{prefix}{orphan}"
    storage.client.put_object(Bucket=storage.bucket_name, Key=orphan_key, Body=b"")
    original_keys = set(list_object_keys(storage, prefix=prefix))

    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    writer = context.Process(
        target=_write_on_pod,
        args=(uri, storage.endpoint_url, "Committed", child),
        kwargs={"bootstrap": False},
    )
    try:
        writer.start()
        child.close()
        assert parent.poll(25), "Writer did not reach the conditional PUT"
        assert parent.recv() == "prepared"
        prepared_keys = set(list_object_keys(storage, prefix=prefix))
        candidate_keys = prepared_keys - original_keys
        assert any(key.startswith(f"{prefix}manifests/") for key in candidate_keys)
        assert any(key.endswith(".parquet") for key in candidate_keys)

        maintainer = build_application(uri, settings=settings)
        assert maintainer.use_cases.admin.vacuum() == (orphan,)
        assert set(list_object_keys(storage, prefix=prefix)) == prepared_keys - {orphan_key}
        assert reader.repositories.snapshot.load_current_pointer() == base_head

        parent.send("publish")
        assert parent.poll(25), "Writer did not report the outcome of its commit"
        assert parent.recv() == "committed"
        writer.join(timeout=15)
        assert writer.exitcode == 0

        published = reader.repositories.snapshot.load_current_pointer()
        assert published.snapshot_id.value == base_head.snapshot_id.value + 1
        assert f"{prefix}{published.manifest_path}" in candidate_keys
        assert maintainer.use_cases.admin.vacuum() == ()
        pinned = graph.load_state(snapshot=reader.repositories.snapshot.load_at(base_head))
        assert pinned.catalog == original.catalog
        assert pinned.nodes == original.nodes
        assert pinned.edges == original.edges
        assert reader.use_cases.cypher.execute(
            "MATCH (a)-[r:LINK]->(b) RETURN a.name AS src,b.name AS dst,r.weight AS weight "
            "ORDER BY r.weight"
        ).rows == (
            {"src": "a", "dst": "b", "weight": 1},
            {"src": "origin", "dst": "destination", "weight": 5},
        )
        assert reader.use_cases.admin.verify(verify_checksums=True).ok
    finally:
        parent.close()
        child.close()
        if writer.pid is not None and writer.is_alive():
            writer.kill()
            writer.join(timeout=10)


@pytest.mark.integration
def test_s3_retention_preview_uses_one_head_during_concurrent_publication(
    moto_s3_filesystem: MotoS3Context, monkeypatch: pytest.MonkeyPatch
) -> None:
    storage = moto_s3_filesystem
    prefix = "retention-preview/"
    uri = f"{storage.bucket_uri}/retention-preview"
    settings = _s3_settings(uri, storage.endpoint_url)
    writer, maintainer = (build_application(uri, settings=settings) for _ in range(2))
    query = writer.use_cases.cypher.execute
    query("CREATE ()-[:LINK {value:'old'}]->()")
    old = writer.use_cases.graph.capture_snapshot()
    update = "MATCH ()-[r:LINK]->() WHERE id(r)=1 SET r.value=$value RETURN r.value AS value"
    query(update, {"value": "kept"})
    initial = writer.use_cases.graph.capture_snapshot()
    filesystem = maintainer.adapters.filesystem
    file_size = filesystem.file_size
    keys_after_commit: list[str] | None = None

    def publish_during_plan(location: str | Path) -> int | None:
        nonlocal keys_after_commit
        if keys_after_commit is None:
            query(update, {"value": "new-head"})
            keys_after_commit = list_object_keys(storage, prefix=prefix)
        return file_size(location)

    monkeypatch.setattr(filesystem, "file_size", publish_during_plan)

    plan = maintainer.use_cases.admin.plan_retention(keep_last=1)

    assert plan.head_snapshot_id == initial.snapshot_id.value
    assert plan.retained_snapshot_ids == (initial.snapshot_id.value,)
    assert {file.path for file in plan.candidate_files} == {
        old.edges[0].path,
        old.edge_properties[0].path,
    }
    assert plan.estimated_reclaimable_bytes > 0
    assert list_object_keys(storage, prefix=prefix) == keys_after_commit
    current = writer.use_cases.graph.load_state()
    assert current.snapshot.snapshot_id.value == initial.snapshot_id.value + 1
    assert current.edges[0].properties == {"value": "new-head"}
    pinned = maintainer.use_cases.graph.load_state(snapshot=initial)
    assert pinned.edges[0].properties == {"value": "kept"}


def _pause_before_parquet_read(uri: str, endpoint: str, channel: Connection) -> None:
    settings = _s3_settings(uri, endpoint).model_copy(update={"storage_read_only": True})
    reader = build_application(uri, settings=settings)
    reader.use_cases.admin.initialize()
    with pytest.raises(ReadOnlyError):
        reader.use_cases.cypher.execute("CREATE (:Forbidden)")
    graph = reader.use_cases.graph
    read = graph.load_node_property_table
    paused = False

    def pause(**kwargs: Any) -> Any:
        nonlocal paused
        if not paused:
            paused = True
            channel.send(("pinned", kwargs["snapshot"].snapshot_id.value))
            assert channel.poll(25), "Parent did not release the reader"
            assert channel.recv() == "read"
        return read(**kwargs)

    graph.load_node_property_table = pause  # type: ignore[method-assign]
    try:
        result = reader.use_cases.cypher.execute(
            "MATCH (p:Person) WHERE p.age>=20 AND p.age<40 "
            "WITH coalesce(p.name,'?') AS name,p.age AS age "
            "WITH name,age+1 AS next RETURN name,next ORDER BY name"
        )
        channel.send(result.rows)
    finally:
        channel.close()


@pytest.mark.integration
def test_s3_reader_opens_its_published_parquet_after_commit_and_vacuum(
    moto_s3_filesystem: MotoS3Context,
) -> None:
    storage = moto_s3_filesystem
    endpoint = storage.endpoint_url
    uri = f"{storage.bucket_uri}/reader-during-vacuum"
    writer = build_application(uri, settings=_s3_settings(uri, endpoint))
    query = writer.use_cases.cypher.execute
    query("CREATE (:Person {name:'original',age:30})")
    original = writer.use_cases.graph.capture_snapshot()

    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    reader = context.Process(target=_pause_before_parquet_read, args=(uri, endpoint, child))
    try:
        reader.start()
        child.close()
        assert parent.poll(25), "Reader did not reach the Parquet projection"
        assert parent.recv() == ("pinned", original.snapshot_id.value)

        query("MATCH (p:Person) SET p.name='new'")
        admin = writer.use_cases.admin
        estimate = admin.plan_retention(keep_last=1)
        assert original.node_properties[0].path in {item.path for item in estimate.candidate_files}
        assert admin.vacuum() == ()
        assert query("MATCH (p:Person) RETURN p.name AS name").rows == ({"name": "new"},)

        parent.send("read")
        assert parent.poll(25), "Reader did not finish"
        assert parent.recv() == ({"name": "original", "next": 31},)
        reader.join(timeout=10)
        assert reader.exitcode == 0
    finally:
        parent.close()
        child.close()
        if reader.pid is not None and reader.is_alive():
            reader.kill()
            reader.join(timeout=10)
