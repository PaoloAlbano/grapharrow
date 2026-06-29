import pytest
from neo4j import GraphDatabase, basic_auth

from grapharrow.config import GraphArrowSettings
from grapharrow.interfaces.bolt import run_bolt_server
from tests.integration.support import (
    LocalDiskContext,
    assert_expected_local_storage_layout,
    has_local_parquet_magic,
    load_local_json_object,
    read_local_parquet_object,
)


@pytest.mark.integration
def test_bolt_supports_local_disk_storage_end_to_end(
    local_disk_database: LocalDiskContext,
) -> None:
    settings = GraphArrowSettings(
        database_path=local_disk_database.database_dir,
        bolt_host="127.0.0.1",
        bolt_port=0,
        bolt_auth_token="secret",
    )

    with run_bolt_server(local_disk_database.database_dir, settings=settings) as handle:
        driver = GraphDatabase.driver(
            f"bolt://127.0.0.1:{handle.port}",
            auth=basic_auth("neo4j", "secret"),
        )
        try:
            with driver.session() as session:
                create_record = session.run(
                    "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) "
                    "RETURN a, b"
                ).single()
                assert create_record is not None
                assert create_record["a"]["name"] == "Alice"
                assert create_record["b"]["name"] == "Bob"

                result = session.run(
                    "MATCH (a:Person)-[:KNOWS]->(b:Person) "
                    "RETURN a.name, b.name ORDER BY a.name ASC"
                ).data()
                functions_result = session.run(
                    "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
                    "RETURN id(a) as source_id, labels(a) as source_labels, type(r) as rel_type"
                ).data()
                exists_result = session.run(
                    "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
                    "WHERE EXISTS(a.name) AND NOT EXISTS(r.since) "
                    "RETURN count(r) as count"
                ).single()
                relationship_count = session.run(
                    "MATCH ()-[r]->() RETURN count(r) as count"
                ).single()
        finally:
            driver.close()

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
    nodes_payload = files_payload["nodes"]
    edges_payload = files_payload["edges"]
    node_labels_payload = files_payload["node_labels"]
    adjacency_payload = files_payload["adjacency_by_source"]
    adjacency_by_dest_payload = files_payload["adjacency_by_dest"]
    assert isinstance(nodes_payload, list)
    assert isinstance(edges_payload, list)
    assert isinstance(node_labels_payload, list)
    assert isinstance(adjacency_payload, list)
    assert isinstance(adjacency_by_dest_payload, list)

    node_file = str(nodes_payload[0]["path"])
    edge_file = str(edges_payload[0]["path"])
    node_label_file = str(node_labels_payload[0]["path"])
    adjacency_file = str(adjacency_payload[0]["path"])
    adjacency_by_dest_file = str(adjacency_by_dest_payload[0]["path"])
    node_table = read_local_parquet_object(local_disk_database.database_dir, node_file)
    edge_table = read_local_parquet_object(local_disk_database.database_dir, edge_file)
    node_labels_table = read_local_parquet_object(
        local_disk_database.database_dir,
        node_label_file,
    )
    adjacency_table = read_local_parquet_object(
        local_disk_database.database_dir,
        adjacency_file,
    )
    adjacency_by_dest_table = read_local_parquet_object(
        local_disk_database.database_dir,
        adjacency_by_dest_file,
    )

    assert result == [{"a.name": "Alice", "b.name": "Bob"}]
    assert functions_result == [
        {
            "source_id": 1,
            "source_labels": ["Person"],
            "rel_type": "KNOWS",
        }
    ]
    assert exists_result is not None
    assert exists_result["count"] == 1
    assert relationship_count is not None
    assert relationship_count["count"] == 1
    assert current_snapshot["snapshot_id"] == 2
    assert current_manifest["snapshot_id"] == 2
    assert_expected_local_storage_layout(
        root_dir=local_disk_database.root_dir,
        database_dir=local_disk_database.database_dir,
        node_file=node_file,
        edge_file=edge_file,
        node_label_file=node_label_file,
        adjacency_file=adjacency_file,
        adjacency_by_dest_file=adjacency_by_dest_file,
    )
    assert has_local_parquet_magic(local_disk_database.database_dir, node_file) is True
    assert has_local_parquet_magic(local_disk_database.database_dir, edge_file) is True
    assert has_local_parquet_magic(local_disk_database.database_dir, node_label_file) is True
    assert has_local_parquet_magic(local_disk_database.database_dir, adjacency_file) is True
    assert has_local_parquet_magic(local_disk_database.database_dir, adjacency_by_dest_file) is True
    assert node_table.to_pylist() == [
        {
            "node_id": 1,
            "labels": ["Person"],
            "properties_json": '{"name": "Alice"}',
        },
        {
            "node_id": 2,
            "labels": ["Person"],
            "properties_json": '{"name": "Bob"}',
        },
    ]
    assert edge_table.to_pylist() == [
        {
            "edge_id": 1,
            "rel_type": "KNOWS",
            "src_id": 1,
            "dst_id": 2,
            "properties_json": "{}",
        }
    ]
    assert node_labels_table.to_pylist() == [{"node_id": 1}, {"node_id": 2}]
    assert adjacency_table.to_pylist() == [{"src_id": 1, "dst_id": 2, "edge_id": 1}]
    assert adjacency_by_dest_table.to_pylist() == [{"src_id": 1, "dst_id": 2, "edge_id": 1}]
