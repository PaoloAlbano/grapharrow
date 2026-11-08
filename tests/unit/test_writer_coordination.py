"""Writers must preserve committed snapshots across processes and stale transactions."""

import hashlib
import multiprocessing
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from typing import Any

import pytest

from grapharrow import Database
from grapharrow.core.errors import WriteConflictError
from grapharrow.use_cases import CypherQueryUseCase, GraphTransactionUseCase


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
