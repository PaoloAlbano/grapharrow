import json
from pathlib import Path

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]
import pytest

from grapharrow.interfaces.cli import main


def test_cli_init_bootstraps_a_database(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_path = tmp_path / "example-db"

    exit_code = main(["init", str(database_path)])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Initialized GraphArrow database" in captured.out
    assert "snapshot 1" in captured.out
    assert (database_path / "catalog" / "current_catalog.json").exists()


def test_cli_show_catalog_prints_current_catalog(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_path = tmp_path / "example-db"
    main(["init", str(database_path)])
    capsys.readouterr()

    exit_code = main(["show-catalog", str(database_path)])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert exit_code == 0
    assert payload["version"] == 1
    assert payload["labels"] == []
    assert payload["indexes"] == []


def test_cli_create_and_list_indexes(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_path = tmp_path / "example-db"
    main(["init", str(database_path)])
    capsys.readouterr()

    exit_code = main(
        [
            "create-index",
            str(database_path),
            "person_name_idx",
            "--kind",
            "text",
            "--label",
            "Person",
            "--property",
            "name",
            "--case-insensitive",
        ]
    )
    created = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert created["catalog_version"] == 2
    assert created["index"]["name"] == "person_name_idx"
    assert created["index"]["kind"] == "text"
    assert created["index"]["owner_kind"] == "label"
    assert created["index"]["owner_name"] == "Person"
    assert created["index"]["property_names"] == ["name"]
    assert created["index"]["case_sensitive"] is False

    exit_code = main(["list-indexes", str(database_path)])
    listed = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert listed["version"] == 2
    assert listed["indexes"] == [created["index"]]

    exit_code = main(["show-catalog", str(database_path)])
    catalog = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert catalog["indexes"] == [created["index"]]


def test_cli_create_index_if_not_exists_is_idempotent(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_path = tmp_path / "example-db"
    main(["init", str(database_path)])
    capsys.readouterr()

    first_exit_code = main(
        [
            "create-index",
            str(database_path),
            "knows_since_idx",
            "--relationship-type",
            "KNOWS",
            "--property",
            "since",
        ]
    )
    first = json.loads(capsys.readouterr().out)
    second_exit_code = main(
        [
            "create-index",
            str(database_path),
            "knows_since_idx",
            "--relationship-type",
            "KNOWS",
            "--property",
            "since",
            "--if-not-exists",
        ]
    )
    second = json.loads(capsys.readouterr().out)

    assert first_exit_code == 0
    assert second_exit_code == 0
    assert first["catalog_version"] == 2
    assert second["catalog_version"] == 2
    assert second["index"] == first["index"]


def test_cli_drop_index_removes_catalog_declaration(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_path = tmp_path / "example-db"
    main(["init", str(database_path)])
    capsys.readouterr()
    main(
        [
            "create-index",
            str(database_path),
            "person_name_idx",
            "--label",
            "Person",
            "--property",
            "name",
        ]
    )
    created = json.loads(capsys.readouterr().out)

    exit_code = main(["drop-index", str(database_path), "person_name_idx"])
    dropped = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert dropped["catalog_version"] == 3
    assert dropped["dropped_index"] == created["index"]

    exit_code = main(["list-indexes", str(database_path)])
    listed = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert listed["indexes"] == []

    exit_code = main(["drop-index", str(database_path), "person_name_idx", "--if-exists"])
    idempotent = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert idempotent["catalog_version"] == 3
    assert idempotent["dropped_index"] is None


def test_cli_show_snapshot_prints_the_current_snapshot(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_path = tmp_path / "example-db"
    main(["init", str(database_path)])
    capsys.readouterr()

    exit_code = main(["show-snapshot", str(database_path)])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert exit_code == 0
    assert payload["current_pointer"]["snapshot_id"] == 1
    assert payload["manifest"]["catalog_version"] == 1


def test_cli_verify_reports_a_healthy_bootstrapped_database(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_path = tmp_path / "example-db"
    main(["init", str(database_path)])
    capsys.readouterr()

    exit_code = main(["verify", str(database_path)])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert exit_code == 0
    assert payload["ok"] is True
    assert payload["missing_paths"] == []
    assert payload["warnings"] == []


def test_cli_recover_lists_orphan_staged_paths_in_dry_run_mode(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_path = tmp_path / "example-db"
    main(["init", str(database_path)])
    capsys.readouterr()
    orphan_path = database_path / "staged" / "tx-000001"
    orphan_path.mkdir(parents=True)

    exit_code = main(["recover", str(database_path)])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert exit_code == 0
    assert payload["orphan_staged_paths"] == ["staged/tx-000001"]
    assert payload["removed_paths"] == []
    assert orphan_path.exists() is True


def test_cli_bulk_add_nodes_imports_json_rows(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from grapharrow import Database

    database_path = tmp_path / "example-db"
    input_path = tmp_path / "nodes.json"
    input_path.write_text(
        json.dumps(
            [
                {"external_id": "alice", "name": "Alice"},
                {"external_id": "bob", "name": "Bob"},
            ]
        ),
        encoding="utf-8",
    )

    main(["init", str(database_path)])
    capsys.readouterr()

    exit_code = main(
        [
            "bulk-add-nodes",
            str(database_path),
            str(input_path),
            "--default-label",
            "Person",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert exit_code == 0
    assert payload["row_count"] == 2

    database = Database(database_path)
    with database.read_transaction() as session:
        result = session.query("MATCH (n:Person) RETURN n.external_id ORDER BY n.external_id ASC")
    assert result.rows == ({"n.external_id": "alice"}, {"n.external_id": "bob"})


def test_cli_bulk_add_edges_imports_jsonl_rows_with_business_keys(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from grapharrow import Database

    database_path = tmp_path / "example-db"
    nodes_path = tmp_path / "nodes.json"
    edges_path = tmp_path / "edges.jsonl"
    nodes_path.write_text(
        json.dumps(
            [
                {"external_id": "alice", "name": "Alice"},
                {"external_id": "bob", "name": "Bob"},
            ]
        ),
        encoding="utf-8",
    )
    edges_path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "src_key": "alice",
                        "dst_key": "bob",
                        "rel_type": "KNOWS",
                        "since": 2024,
                    }
                )
            ]
        ),
        encoding="utf-8",
    )

    main(["init", str(database_path)])
    capsys.readouterr()
    main(
        [
            "bulk-add-nodes",
            str(database_path),
            str(nodes_path),
            "--default-label",
            "Person",
        ]
    )
    capsys.readouterr()

    exit_code = main(
        [
            "bulk-add-edges",
            str(database_path),
            str(edges_path),
            "--source-label",
            "Person",
            "--source-key",
            "external_id",
            "--target-label",
            "Person",
            "--target-key",
            "external_id",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert exit_code == 0
    assert payload["row_count"] == 1

    database = Database(database_path)
    with database.read_transaction() as session:
        result = session.query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.external_id, b.external_id, r.since"
        )
    assert result.rows == ({"a.external_id": "alice", "b.external_id": "bob", "r.since": 2024},)


def test_cli_import_nodes_supports_csv_with_multi_labels(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from grapharrow import Database

    database_path = tmp_path / "example-db"
    input_path = tmp_path / "nodes.csv"
    input_path.write_text(
        'labels,external_id,name\n"Person;Employee",alice,Alice\nPerson,bob,Bob\n',
        encoding="utf-8",
    )

    main(["init", str(database_path)])
    capsys.readouterr()

    exit_code = main(["import-nodes", str(database_path), str(input_path)])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert exit_code == 0
    assert payload["row_count"] == 2

    database = Database(database_path)
    with database.read_transaction() as session:
        result = session.query("MATCH (n:Employee) RETURN n.external_id ORDER BY n.external_id ASC")
    assert result.rows == ({"n.external_id": "alice"},)


def test_cli_import_edges_supports_parquet_with_internal_ids(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from grapharrow import Database

    database_path = tmp_path / "example-db"
    nodes_path = tmp_path / "nodes.csv"
    edges_path = tmp_path / "edges.parquet"
    nodes_path.write_text(
        "labels,name\nPerson,Alice\nPerson,Bob\n",
        encoding="utf-8",
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {"src_id": 1, "dst_id": 2, "rel_type": "KNOWS", "since": 2024},
            ]
        ),
        edges_path,
    )

    main(["init", str(database_path)])
    capsys.readouterr()
    main(["import-nodes", str(database_path), str(nodes_path)])
    capsys.readouterr()

    exit_code = main(["import-edges", str(database_path), str(edges_path)])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert exit_code == 0
    assert payload["row_count"] == 1

    database = Database(database_path)
    with database.read_transaction() as session:
        result = session.query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, b.name, r.since"
        )
    assert result.rows == ({"a.name": "Alice", "b.name": "Bob", "r.since": 2024},)


def test_cli_bulk_add_nodes_supports_parquet_input(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from grapharrow import Database

    database_path = tmp_path / "example-db"
    input_path = tmp_path / "nodes.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [
                {"external_id": "alice", "name": "Alice"},
                {"external_id": "bob", "name": "Bob"},
            ]
        ),
        input_path,
    )

    main(["init", str(database_path)])
    capsys.readouterr()

    exit_code = main(
        [
            "bulk-add-nodes",
            str(database_path),
            str(input_path),
            "--default-label",
            "Person",
        ]
    )

    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["row_count"] == 2

    database = Database(database_path)
    with database.read_transaction() as session:
        result = session.query("MATCH (n:Person) RETURN n.external_id ORDER BY n.external_id ASC")
    assert result.rows == ({"n.external_id": "alice"}, {"n.external_id": "bob"})


def test_cli_bulk_add_edges_supports_csv_internal_ids(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from grapharrow import Database

    database_path = tmp_path / "example-db"
    nodes_path = tmp_path / "nodes.json"
    edges_path = tmp_path / "edges.csv"
    nodes_path.write_text(
        json.dumps(
            [
                {"labels": ["Person"], "name": "Alice"},
                {"labels": ["Person"], "name": "Bob"},
            ]
        ),
        encoding="utf-8",
    )
    edges_path.write_text(
        "src_id,dst_id,rel_type,since\n1,2,KNOWS,2024\n",
        encoding="utf-8",
    )

    main(["init", str(database_path)])
    capsys.readouterr()
    main(["bulk-add-nodes", str(database_path), str(nodes_path)])
    capsys.readouterr()

    exit_code = main(["bulk-add-edges", str(database_path), str(edges_path)])

    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["row_count"] == 1

    database = Database(database_path)
    with database.read_transaction() as session:
        result = session.query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, b.name, r.since"
        )
    assert result.rows == ({"a.name": "Alice", "b.name": "Bob", "r.since": 2024},)


def test_cli_import_edges_supports_csv_with_business_keys(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from grapharrow import Database

    database_path = tmp_path / "example-db"
    nodes_path = tmp_path / "nodes.csv"
    edges_path = tmp_path / "edges.csv"
    nodes_path.write_text(
        "labels,external_id,name\nPerson,alice,Alice\nPerson,bob,Bob\n",
        encoding="utf-8",
    )
    edges_path.write_text(
        "src_key,dst_key,rel_type,since\nalice,bob,KNOWS,2024\n",
        encoding="utf-8",
    )

    main(["init", str(database_path)])
    capsys.readouterr()
    main(["import-nodes", str(database_path), str(nodes_path)])
    capsys.readouterr()

    exit_code = main(
        [
            "import-edges",
            str(database_path),
            str(edges_path),
            "--source-label",
            "Person",
            "--source-key",
            "external_id",
            "--target-label",
            "Person",
            "--target-key",
            "external_id",
        ]
    )

    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["row_count"] == 1

    database = Database(database_path)
    with database.read_transaction() as session:
        result = session.query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.external_id, b.external_id, r.since"
        )
    assert result.rows == ({"a.external_id": "alice", "b.external_id": "bob", "r.since": 2024},)


def test_cli_import_commands_reject_unsupported_extensions(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_path = tmp_path / "example-db"
    input_path = tmp_path / "nodes.txt"
    input_path.write_text("not supported", encoding="utf-8")

    main(["init", str(database_path)])
    capsys.readouterr()

    with pytest.raises(ValueError, match="Supported bulk input formats"):
        main(["import-nodes", str(database_path), str(input_path)])


def test_cli_import_edges_requires_complete_reference_flags(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_path = tmp_path / "example-db"
    input_path = tmp_path / "edges.csv"
    input_path.write_text(
        "src_key,dst_key,rel_type\nalice,bob,KNOWS\n",
        encoding="utf-8",
    )

    main(["init", str(database_path)])
    capsys.readouterr()

    with pytest.raises(
        ValueError,
        match="Both --source-label and --source-key are required",
    ):
        main(
            [
                "import-edges",
                str(database_path),
                str(input_path),
                "--source-label",
                "Person",
            ]
        )
