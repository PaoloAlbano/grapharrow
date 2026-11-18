"""Independent object-storage writers publish an entire graph or lose the commit."""

import multiprocessing
from multiprocessing.connection import Connection
from urllib.parse import urlparse

import pytest

from grapharrow.adapters.object_pointer import Revision
from grapharrow.app import build_application
from grapharrow.config import GraphArrowSettings
from grapharrow.core.errors import WriteConflictError
from grapharrow.domain import SnapshotId
from tests.integration.support import MotoS3Context


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
