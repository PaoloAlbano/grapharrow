"""The Native retention preview estimates space without altering published data."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from grapharrow import Database
from grapharrow.config import GraphArrowSettings
from grapharrow.core.errors import StorageError
from grapharrow.interfaces.cli import main
from grapharrow.interfaces.rest import create_rest_app


def test_retention_preview_excludes_shared_files_and_metadata(tmp_path: Path) -> None:
    app = create_rest_app(tmp_path)
    client = TestClient(app)
    payload = {
        "query": (
            "CREATE (a:Person {name:'a'})-[r:LINK {value:'old'}]->(b:Person {name:'b'}) "
            "RETURN id(r) AS id"
        )
    }
    headers = {"Idempotency-Key": "create-relationship"}
    first = client.post("/v1/query", json=payload, headers=headers)
    assert first.status_code == 200
    database = Database(tmp_path)
    previous = database.load_snapshot()
    with database.write_transaction() as session:
        session.query(
            "MATCH ()-[r:LINK]->() WHERE id(r)=1 SET r.value='next' RETURN r.value AS value"
        )
    head = database.load_current_snapshot_pointer()
    stored = {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}

    plan = database.plan_retention(keep_last=1)

    assert plan.head_snapshot_id == head.snapshot_id.value
    assert plan.retained_snapshot_ids == (head.snapshot_id.value,)
    assert plan.older_snapshot_ids == (previous.snapshot_id.value, 1)
    old_edge, old_properties = previous.edges[0].path, previous.edge_properties[0].path
    assert {file.path: file.size_bytes for file in plan.candidate_files} == {
        old_edge: len(stored[tmp_path / old_edge]),
        old_properties: len(stored[tmp_path / old_properties]),
    }
    assert plan.estimated_reclaimable_bytes == sum(
        len(stored[tmp_path / path]) for path in (old_edge, old_properties)
    )
    assert plan.missing_candidate_paths == ()
    assert {previous.nodes[0].path, previous.adjacency_by_source[0].path}.issubset(
        plan.shared_parquet_paths
    )
    assert {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()} == stored
    assert database.load_snapshot(previous.snapshot_id) == previous
    assert database.load_current_snapshot_pointer() == head
    assert client.post("/v1/query", json=payload, headers=headers).content == first.content
    assert database.vacuum() == ()


def test_age_window_preserves_contiguous_history_with_unordered_timestamps(tmp_path: Path) -> None:
    database = Database(tmp_path)
    database.initialize()
    now = datetime.now(UTC)
    for index, age in enumerate((18, 18, 1, 18, 18)):
        if index:
            with database.write_transaction() as session:
                session.query("CREATE (:Example {index:$index})", {"index": index})
        pointer = database.load_current_snapshot_pointer()
        path = tmp_path / pointer.manifest_path
        manifest = json.loads(path.read_text())
        manifest["created_at"] = (now - timedelta(hours=age)).isoformat()
        path.write_text(json.dumps(manifest))

    count_only = database.plan_retention(keep_last=1)
    assert count_only.retained_snapshot_ids == (5,)
    age_and_count = database.plan_retention(keep_last=1, min_age_hours=4)
    assert age_and_count.retained_snapshot_ids == (5, 4, 3)
    assert age_and_count.older_snapshot_ids == (2, 1)
    all_recent = database.plan_retention(keep_last=1, min_age_hours=24)
    assert all_recent.retained_snapshot_ids == (5, 4, 3, 2, 1)
    assert all_recent.candidate_files == ()
    assert all_recent.estimated_reclaimable_bytes == 0
    assert database.plan_retention(keep_last=4, min_age_hours=4).retained_snapshot_ids == (
        5,
        4,
        3,
        2,
    )

    compacted = database.compact()
    assert datetime.fromisoformat(compacted.created_at) >= now.replace(microsecond=0)


def test_preview_reports_a_missing_file_without_counting_it(tmp_path: Path) -> None:
    database = Database(tmp_path)
    with database.write_transaction() as session:
        session.query("CREATE (:Example {value:'old'})")
    previous = database.load_snapshot()
    with database.write_transaction() as session:
        session.query("MATCH (n:Example) SET n.value = 'new'")
    old_file = previous.nodes[0].path
    (tmp_path / old_file).unlink()

    plan = database.plan_retention(keep_last=1)

    assert plan.missing_candidate_paths == (old_file,)
    assert all(file.path != old_file for file in plan.candidate_files)
    assert database.verify(verify_checksums=True).ok


def test_retention_preview_does_not_initialize_a_missing_database(tmp_path: Path) -> None:
    path = tmp_path / "absent"
    with pytest.raises(StorageError, match="Current snapshot pointer does not exist"):
        Database(path).plan_retention(keep_last=1)
    assert not path.exists()


def test_cli_and_rest_report_the_same_retention_plan(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    database = Database(tmp_path)
    database.initialize()
    with database.write_transaction() as session:
        session.query("CREATE (:Example)")

    assert main(["retention-plan", str(tmp_path), "--keep-last", "1"]) == 0
    cli_plan = json.loads(capsys.readouterr().out)
    client = TestClient(
        create_rest_app(tmp_path, settings=GraphArrowSettings(server_auth_token="secret"))
    )
    path = "/v1/admin/retention/plan"
    auth = {"Authorization": "Bearer secret"}
    response = client.get(path, params={"keep_last": 1}, headers=auth)
    assert response.status_code == 200
    rest_plan = response.json()["data"]
    assert datetime.fromisoformat(cli_plan.pop("evaluated_at")).tzinfo is not None
    assert datetime.fromisoformat(rest_plan.pop("evaluated_at")).tzinfo is not None
    assert rest_plan == cli_plan
    assert rest_plan["retained_snapshot_ids"] == [2]
    assert rest_plan["older_snapshot_ids"] == [1]
    assert client.get(path, params={"keep_last": 1}).status_code == 401
    assert client.get(path, params={"keep_last": 0}, headers=auth).status_code == 422
    assert (
        client.get(path, params={"keep_last": 1, "min_age_hours": -1}, headers=auth).status_code
        == 422
    )
