"""Writers must preserve committed snapshots across processes and stale transactions."""

import hashlib
import multiprocessing
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from typing import Any

import pytest

from grapharrow import Database
from grapharrow.app import GraphArrowApplication, build_application
from grapharrow.core.errors import WriteConflictError
from grapharrow.use_cases import CypherQueryUseCase, GraphTransactionUseCase
from grapharrow.use_cases.ana import GraphArrowAgentBackend


def _write_process(path: str, start: Any, index: int) -> None:
    database = Database(path)
    start.wait(timeout=30)
    for item in range(2):
        with database.write_transaction() as session:
            name = f"writer-{index}-{item}"
            if index % 2:
                session.query("CREATE (:Person {name: $name})", {"name": name})
            else:
                session.add_node(labels=["Person"], properties={"name": name})


def _fingerprint(path: Path) -> dict[str, str]:
    return {
        str(file.relative_to(path)): hashlib.sha256(file.read_bytes()).hexdigest()
        for file in path.rglob("*")
        if file.is_file()
    }


def test_independent_processes_preserve_all_writes_during_bootstrap(tmp_path: Path) -> None:
    path = tmp_path / "process-writers"
    context = multiprocessing.get_context("spawn")
    start = context.Barrier(4)
    processes = [
        context.Process(target=_write_process, args=(str(path), start, i)) for i in range(4)
    ]
    try:
        for process in processes:
            process.start()
        for process in processes:
            process.join(timeout=45)
            assert process.exitcode == 0
    finally:
        for process in processes:
            if process.is_alive():
                process.kill()
                process.join(timeout=10)
    database = Database(path)
    with database.read_transaction() as session:
        result = session.query("MATCH (p:Person) RETURN p.name AS name ORDER BY name")
    assert result.rows == tuple({"name": f"writer-{i}-{j}"} for i in range(4) for j in range(2))
    assert database.verify(verify_checksums=True).ok


def test_concurrent_merge_uses_one_current_state_across_database_instances(tmp_path: Path) -> None:
    path = tmp_path / "merge-writers"
    databases = [Database(path), Database(path.as_uri()), Database(path), Database(path.as_uri())]
    start = Barrier(len(databases))

    def merge(database: Database) -> None:
        start.wait(timeout=10)
        with database.write_transaction() as session:
            session.query("MERGE (p:Person {name: 'same'}) RETURN p.name")

    with ThreadPoolExecutor(max_workers=len(databases)) as pool:
        list(pool.map(merge, databases))
    with databases[0].read_transaction() as session:
        assert session.query("MATCH (p:Person) RETURN count(p) AS count").rows == ({"count": 1},)
    assert databases[0].verify(verify_checksums=True).ok


@pytest.mark.parametrize("shared_edge", [False, True])
def test_one_server_serializes_ten_simultaneous_property_updates(
    tmp_path: Path, shared_edge: bool
) -> None:
    app = build_application(tmp_path / "one-server")
    query = app.use_cases.cypher.execute
    query(
        "CREATE (a:Person), (b:Person) "
        "WITH a,b UNWIND range(1,10) AS i CREATE (a)-[:LINK {value:0}]->(b)"
    )
    initial = app.use_cases.graph.capture_snapshot()
    barrier = Barrier(10)

    def increment(index: int) -> None:
        barrier.wait(timeout=10)
        for _ in range(2):
            result = query(
                "MATCH ()-[r:LINK]->() WHERE id(r)=$id "
                "SET r.value=r.value+1 RETURN r.value AS value",
                {"id": 1 if shared_edge else index + 1},
            )
            assert len(result.rows) == 1

    with GraphArrowAgentBackend(app) as reader:
        with ThreadPoolExecutor(max_workers=10) as pool:
            list(pool.map(increment, range(10)))
        state = app.use_cases.graph.load_state()
        assert state.snapshot.snapshot_id.value == initial.snapshot_id.value + 20
        assert [edge.properties["value"] for edge in state.edges] == (
            [20, *([0] * 9)] if shared_edge else [2] * 10
        )
        assert reader._state.snapshot == initial
        assert all(
            edge.properties["value"] == 0
            for edge in app.use_cases.graph.load_state(snapshot=initial).edges
        )
        app.use_cases.admin.vacuum()
        assert app.use_cases.admin.verify(verify_checksums=True).ok


def test_edge_appends_retry_and_preserve_a_pinned_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "concurrent-edge-appends"
    app = build_application(root)
    graph = app.use_cases.graph
    _, source = graph.create_node(labels=("Person",), properties={"name": "Alice"})
    _, target = graph.create_node(labels=("Person",), properties={"name": "Bob"})

    def create_edge(application: GraphArrowApplication, value: int) -> int:
        _, edge = application.use_cases.graph.create_edge(
            src_id=source.node_id, dst_id=target.node_id, rel_type="KNOWS", properties={"v": value}
        )
        return edge.edge_id

    first = create_edge(app, 0)
    start = Barrier(2)

    def append(value: int) -> int:
        writer = build_application(root)
        start.wait(timeout=10)
        return create_edge(writer, value)

    def fail(*args: Any, **kwargs: Any) -> None:
        raise OSError("Simulated publish failure")

    with GraphArrowAgentBackend(app) as reader:
        with monkeypatch.context() as patch:
            patch.setattr(app.repositories.snapshot, "publish", fail)
            with pytest.raises(OSError, match="publish failure"):
                create_edge(app, 1)
        assert app.repositories.snapshot.load_manifest() == reader._state.snapshot
        with ThreadPoolExecutor(max_workers=2) as pool:
            assert sorted(pool.map(append, (1, 2))) == [first + 1, first + 2]

        app.use_cases.admin.vacuum()
        assert reader._state.edge_table["edge_id"].to_pylist() == [first]
        state = graph.load_state()
        assert [edge.edge_id for edge in state.edges] == [first, first + 1, first + 2]
        assert sorted(edge.properties["v"] for edge in state.edges) == [0, 1, 2]
    assert app.use_cases.admin.verify(verify_checksums=True).ok


@pytest.mark.parametrize("change", ["graph", "catalog", "compact"])
def test_stale_explicit_transaction_fails_before_touching_committed_files(
    tmp_path: Path, change: str
) -> None:
    path = tmp_path / "stale-transaction"
    database = Database(path)
    transaction = GraphTransactionUseCase(database._app.use_cases.graph)
    executor = CypherQueryUseCase(transaction)  # type: ignore[arg-type]
    executor.execute("CREATE (:Stale {name: 'discarded'})")
    other = Database(path)
    if change == "graph":
        with other.write_transaction() as session:
            session.add_node(labels=["Person"], properties={"name": "committed"})
    elif change == "catalog":
        other.create_index(
            name="person_name",
            kind="range",
            owner_kind="label",
            owner_name="Person",
            property_names=("name",),
        )
    else:
        other.compact()
    committed = _fingerprint(path)
    with pytest.raises(WriteConflictError, match="Database changed"):
        transaction.commit()
    assert _fingerprint(path) == committed
    assert database.verify(verify_checksums=True).ok


def test_failed_publication_releases_writer_for_the_next_operation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = Database(tmp_path / "failed-write")
    graph = database._app.repositories.graph
    original = graph.persist_snapshot

    def fail(**kwargs: Any) -> Any:
        raise OSError("Simulated persistence failure")

    with monkeypatch.context() as patch:
        patch.setattr(graph, "persist_snapshot", fail)
        with database.write_transaction() as session, pytest.raises(OSError, match="persistence"):
            session.add_node(labels=["Person"], properties={"name": "failed"})
    assert graph.persist_snapshot == original
    with database.write_transaction() as session:
        session.add_node(labels=["Person"], properties={"name": "success"})
    with database.read_transaction() as session:
        assert session.query("MATCH (p:Person) RETURN p.name AS name").rows == (
            {"name": "success"},
        )
    assert database.verify(verify_checksums=True).ok


def _hold_writer(path: str, ready: Any) -> None:
    with Database(path)._app.use_cases.graph.writer_lock():
        ready.set()
        ready.wait(timeout=30)
        # The parent terminates this process while its OS lock is held.
        import time

        time.sleep(30)


def test_process_exit_releases_the_os_lock_without_removing_the_lock_file(tmp_path: Path) -> None:
    path = tmp_path / "terminated-writer"
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    process = context.Process(target=_hold_writer, args=(str(path), ready))
    process.start()
    try:
        assert ready.wait(timeout=15)
    finally:
        process.kill()
        process.join(timeout=10)
    assert (path / ".grapharrow-writer.lock").exists()
    with Database(path).write_transaction() as session:
        session.add_node(labels=["Person"], properties={"name": "after-exit"})
    assert Database(path).verify(verify_checksums=True).ok


def _take_inherited_lock(coordinator: Any, ready: Any, acquired: Any) -> None:
    ready.set()
    with coordinator.hold():
        acquired.set()


@pytest.mark.skipif("fork" not in multiprocessing.get_all_start_methods(), reason="Requires fork")
@pytest.mark.filterwarnings("ignore:This process.*multi-threaded.*:DeprecationWarning")
def test_forked_coordinator_keeps_the_parent_lock_held(tmp_path: Path) -> None:
    # The child exercises only the coordinator and does not call Arrow after fork.
    database = Database(tmp_path / "fork-writer")
    coordinator = database._app.repositories.snapshot.writer
    context = multiprocessing.get_context("fork")
    ready, acquired = context.Event(), context.Event()
    process = context.Process(target=_take_inherited_lock, args=(coordinator, ready, acquired))
    try:
        with coordinator.hold():
            process.start()
            assert ready.wait(timeout=10)
            assert not acquired.wait(timeout=0.1)
        assert acquired.wait(timeout=10)
        process.join(timeout=10)
        assert process.exitcode == 0
    finally:
        if process.is_alive():
            process.kill()
            process.join(timeout=10)


def test_failed_commit_leaves_schema_and_graph_unchanged(tmp_path: Path, monkeypatch) -> None:
    app = build_application(tmp_path / "failed-schema-and-data")
    reader = build_application(tmp_path / "failed-schema-and-data")
    before = reader.use_cases.graph.load_state()

    def fail(*args, **kwargs):
        # All candidate data and schema files have been written at this point.
        assert reader.repositories.catalog.load() == before.catalog
        assert reader.use_cases.graph.load_state().snapshot == before.snapshot
        raise OSError("Interrupted before replacing the head")

    with monkeypatch.context() as patch:
        patch.setattr(app.adapters.filesystem, "replace_pointer", fail)
        with pytest.raises(OSError, match="Interrupted before replacing"):
            app.use_cases.cypher.execute("CREATE (:Unpublished)-[:UnpublishedLink]->(:Unpublished)")

    assert reader.repositories.catalog.load() == before.catalog
    assert reader.use_cases.graph.load_state().snapshot == before.snapshot
    app.use_cases.cypher.execute("CREATE (:Committed)-[:CommittedLink]->(:Committed)")
    committed = reader.use_cases.graph.load_state()
    assert committed.snapshot.snapshot_id.value == before.snapshot.snapshot_id.value + 1
    assert [label.name for label in committed.catalog.labels] == ["Committed"]
    assert [edge.rel_type for edge in committed.edges] == ["CommittedLink"]
    assert reader.use_cases.admin.verify(verify_checksums=True).ok
