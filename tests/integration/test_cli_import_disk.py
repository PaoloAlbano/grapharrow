import json

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]
import pytest

from grapharrow.interfaces.cli import main
from tests.integration.support import (
    LocalDiskContext,
    assert_expected_local_storage_layout,
    has_local_parquet_magic,
    load_local_json_object,
    read_local_parquet_object,
)


@pytest.mark.integration
def test_cli_import_commands_support_csv_and_parquet_on_local_disk(
    local_disk_database: LocalDiskContext,
    capsys: pytest.CaptureFixture[str],
) -> None:
    nodes_path = local_disk_database.root_dir / "nodes.csv"
    edges_path = local_disk_database.root_dir / "edges.parquet"
    nodes_path.write_text(
        'labels,external_id,name\n"Person;Employee",alice,Alice\nPerson,bob,Bob\n',
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

    init_exit_code = main(["init", str(local_disk_database.database_dir)])
    capsys.readouterr()
    import_nodes_exit_code = main(
        ["import-nodes", str(local_disk_database.database_dir), str(nodes_path)]
    )
    nodes_payload = json.loads(capsys.readouterr().out)
    import_edges_exit_code = main(
        ["import-edges", str(local_disk_database.database_dir), str(edges_path)]
    )
    edges_payload = json.loads(capsys.readouterr().out)

    current_snapshot = load_local_json_object(
        local_disk_database.database_dir,
        "current_snapshot.json",
    )
    current_manifest = load_local_json_object(
        local_disk_database.database_dir,
        str(current_snapshot["manifest_path"]),
    )
    files_payload = current_manifest["files"]
    assert isinstance(files_payload, dict)
    nodes_manifest_payload = files_payload["nodes"]
    edges_manifest_payload = files_payload["edges"]
    node_labels_manifest_payload = files_payload["node_labels"]
    adjacency_manifest_payload = files_payload["adjacency_by_source"]
    assert isinstance(nodes_manifest_payload, list)
    assert isinstance(edges_manifest_payload, list)
    assert isinstance(node_labels_manifest_payload, list)
    assert isinstance(adjacency_manifest_payload, list)

    node_file = str(nodes_manifest_payload[0]["path"])
    edge_file = str(edges_manifest_payload[0]["path"])
    node_label_files = [str(item["path"]) for item in node_labels_manifest_payload]
    person_node_label_file = next(path for path in node_label_files if "/Person/" in path)
    employee_node_label_file = next(path for path in node_label_files if "/Employee/" in path)
    adjacency_file = str(adjacency_manifest_payload[0]["path"])
    node_table = read_local_parquet_object(local_disk_database.database_dir, node_file)
    edge_table = read_local_parquet_object(local_disk_database.database_dir, edge_file)
    person_node_label_table = read_local_parquet_object(
        local_disk_database.database_dir,
        person_node_label_file,
    )
    employee_node_label_table = read_local_parquet_object(
        local_disk_database.database_dir,
        employee_node_label_file,
    )
    adjacency_table = read_local_parquet_object(local_disk_database.database_dir, adjacency_file)

    assert init_exit_code == 0
    assert import_nodes_exit_code == 0
    assert import_edges_exit_code == 0
    assert nodes_payload["row_count"] == 2
    assert edges_payload["row_count"] == 1
    assert current_snapshot["snapshot_id"] == 3
    assert current_manifest["snapshot_id"] == 3
    assert_expected_local_storage_layout(
        root_dir=local_disk_database.root_dir,
        database_dir=local_disk_database.database_dir,
        node_file=node_file,
        edge_file=edge_file,
        node_label_file=person_node_label_file,
        adjacency_file=adjacency_file,
    )
    assert has_local_parquet_magic(local_disk_database.database_dir, node_file)
    assert has_local_parquet_magic(local_disk_database.database_dir, edge_file)
    assert has_local_parquet_magic(local_disk_database.database_dir, person_node_label_file)
    assert has_local_parquet_magic(local_disk_database.database_dir, employee_node_label_file)
    assert has_local_parquet_magic(local_disk_database.database_dir, adjacency_file)
    assert node_table.to_pylist() == [
        {
            "node_id": 1,
            "labels": ["Person", "Employee"],
            "properties_json": '{"external_id": "alice", "name": "Alice"}',
        },
        {
            "node_id": 2,
            "labels": ["Person"],
            "properties_json": '{"external_id": "bob", "name": "Bob"}',
        },
    ]
    assert edge_table.to_pylist() == [
        {
            "edge_id": 1,
            "rel_type": "KNOWS",
            "src_id": 1,
            "dst_id": 2,
            "properties_json": '{"since": 2024}',
        }
    ]
    assert person_node_label_table.to_pylist() == [{"node_id": 1}, {"node_id": 2}]
    assert employee_node_label_table.to_pylist() == [{"node_id": 1}]
    assert adjacency_table.to_pylist() == [{"src_id": 1, "dst_id": 2, "edge_id": 1}]


@pytest.mark.integration
def test_cli_import_edges_supports_csv_business_keys_on_local_disk(
    local_disk_database: LocalDiskContext,
    capsys: pytest.CaptureFixture[str],
) -> None:
    nodes_path = local_disk_database.root_dir / "nodes.csv"
    edges_path = local_disk_database.root_dir / "edges.csv"
    nodes_path.write_text(
        "labels,external_id,name\nPerson,alice,Alice\nPerson,bob,Bob\n",
        encoding="utf-8",
    )
    edges_path.write_text(
        "src_key,dst_key,rel_type,since\nalice,bob,KNOWS,2024\n",
        encoding="utf-8",
    )

    init_exit_code = main(["init", str(local_disk_database.database_dir)])
    capsys.readouterr()
    import_nodes_exit_code = main(
        ["import-nodes", str(local_disk_database.database_dir), str(nodes_path)]
    )
    capsys.readouterr()
    import_edges_exit_code = main(
        [
            "import-edges",
            str(local_disk_database.database_dir),
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
    edges_payload = json.loads(capsys.readouterr().out)

    current_snapshot = load_local_json_object(
        local_disk_database.database_dir,
        "current_snapshot.json",
    )
    current_manifest = load_local_json_object(
        local_disk_database.database_dir,
        str(current_snapshot["manifest_path"]),
    )
    files_payload = current_manifest["files"]
    assert isinstance(files_payload, dict)
    nodes_manifest_payload = files_payload["nodes"]
    edges_manifest_payload = files_payload["edges"]
    node_labels_manifest_payload = files_payload["node_labels"]
    adjacency_manifest_payload = files_payload["adjacency_by_source"]
    assert isinstance(nodes_manifest_payload, list)
    assert isinstance(edges_manifest_payload, list)
    assert isinstance(node_labels_manifest_payload, list)
    assert isinstance(adjacency_manifest_payload, list)

    node_file = str(nodes_manifest_payload[0]["path"])
    edge_file = str(edges_manifest_payload[0]["path"])
    node_label_file = str(node_labels_manifest_payload[0]["path"])
    adjacency_file = str(adjacency_manifest_payload[0]["path"])
    node_table = read_local_parquet_object(local_disk_database.database_dir, node_file)
    edge_table = read_local_parquet_object(local_disk_database.database_dir, edge_file)
    node_label_table = read_local_parquet_object(local_disk_database.database_dir, node_label_file)
    adjacency_table = read_local_parquet_object(local_disk_database.database_dir, adjacency_file)

    assert init_exit_code == 0
    assert import_nodes_exit_code == 0
    assert import_edges_exit_code == 0
    assert edges_payload["row_count"] == 1
    assert current_snapshot["snapshot_id"] == 3
    assert current_manifest["snapshot_id"] == 3
    assert_expected_local_storage_layout(
        root_dir=local_disk_database.root_dir,
        database_dir=local_disk_database.database_dir,
        node_file=node_file,
        edge_file=edge_file,
        node_label_file=node_label_file,
        adjacency_file=adjacency_file,
    )
    assert has_local_parquet_magic(local_disk_database.database_dir, node_file)
    assert has_local_parquet_magic(local_disk_database.database_dir, edge_file)
    assert has_local_parquet_magic(local_disk_database.database_dir, node_label_file)
    assert has_local_parquet_magic(local_disk_database.database_dir, adjacency_file)
    assert node_table.to_pylist() == [
        {
            "node_id": 1,
            "labels": ["Person"],
            "properties_json": '{"external_id": "alice", "name": "Alice"}',
        },
        {
            "node_id": 2,
            "labels": ["Person"],
            "properties_json": '{"external_id": "bob", "name": "Bob"}',
        },
    ]
    assert edge_table.to_pylist() == [
        {
            "edge_id": 1,
            "rel_type": "KNOWS",
            "src_id": 1,
            "dst_id": 2,
            "properties_json": '{"since": 2024}',
        }
    ]
    assert node_label_table.to_pylist() == [{"node_id": 1}, {"node_id": 2}]
    assert adjacency_table.to_pylist() == [{"src_id": 1, "dst_id": 2, "edge_id": 1}]
