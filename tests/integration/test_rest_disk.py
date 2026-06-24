import pytest
from fastapi.testclient import TestClient

from grapharrow.interfaces.rest import create_rest_app
from tests.integration.support import (
    LocalDiskContext,
    assert_expected_local_storage_layout,
    has_local_parquet_magic,
    load_local_json_object,
    read_local_parquet_object,
)


@pytest.mark.integration
def test_rest_supports_local_disk_storage_end_to_end(
    local_disk_database: LocalDiskContext,
) -> None:
    client = TestClient(create_rest_app(local_disk_database.database_dir))

    health = client.get("/v1/health")
    create = client.post(
        "/v1/query",
        json={
            "query": (
                "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a, b"
            ),
            "params": {},
        },
    )
    match = client.post(
        "/v1/query",
        json={
            "query": (
                "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name ORDER BY a.name ASC"
            ),
            "params": {},
        },
    )
    functions_query = client.post(
        "/v1/query",
        json={
            "query": (
                "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
                "RETURN id(a) as source_id, labels(a) as source_labels, type(r) as rel_type"
            ),
            "params": {},
        },
    )
    exists_query = client.post(
        "/v1/query",
        json={
            "query": (
                "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
                "WHERE EXISTS(a.name) AND NOT EXISTS(r.since) "
                "RETURN count(r) as count"
            ),
            "params": {},
        },
    )
    verify = client.get("/v1/admin/verify")

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
    assert isinstance(nodes_payload, list)
    assert isinstance(edges_payload, list)
    assert isinstance(node_labels_payload, list)
    assert isinstance(adjacency_payload, list)

    node_file = str(nodes_payload[0]["path"])
    edge_file = str(edges_payload[0]["path"])
    node_label_file = str(node_labels_payload[0]["path"])
    adjacency_file = str(adjacency_payload[0]["path"])
    node_table = read_local_parquet_object(local_disk_database.database_dir, node_file)
    edge_table = read_local_parquet_object(local_disk_database.database_dir, edge_file)

    assert health.status_code == 200
    assert health.json()["data"]["status"] == "ok"
    assert create.status_code == 200
    assert match.status_code == 200
    assert match.json()["data"]["rows"] == [{"a.name": "Alice", "b.name": "Bob"}]
    assert functions_query.status_code == 200
    assert functions_query.json()["data"]["rows"] == [
        {
            "source_id": 1,
            "source_labels": ["Person"],
            "rel_type": "KNOWS",
        }
    ]
    assert exists_query.status_code == 200
    assert exists_query.json()["data"]["rows"] == [{"count": 1}]
    assert verify.status_code == 200
    assert verify.json()["data"]["ok"] is True
    assert current_snapshot["snapshot_id"] == 2
    assert current_manifest["snapshot_id"] == 2
    assert_expected_local_storage_layout(
        root_dir=local_disk_database.root_dir,
        database_dir=local_disk_database.database_dir,
        node_file=node_file,
        edge_file=edge_file,
        node_label_file=node_label_file,
        adjacency_file=adjacency_file,
    )
    assert has_local_parquet_magic(local_disk_database.database_dir, node_file) is True
    assert has_local_parquet_magic(local_disk_database.database_dir, edge_file) is True
    assert has_local_parquet_magic(local_disk_database.database_dir, node_label_file) is True
    assert has_local_parquet_magic(local_disk_database.database_dir, adjacency_file) is True
    assert {row["properties_json"] for row in node_table.to_pylist()} == {
        '{"name": "Alice"}',
        '{"name": "Bob"}',
    }
    assert edge_table.to_pylist() == [
        {
            "edge_id": 1,
            "rel_type": "KNOWS",
            "src_id": 1,
            "dst_id": 2,
            "properties_json": "{}",
        }
    ]


@pytest.mark.integration
def test_rest_bulk_endpoints_support_local_disk_storage(
    local_disk_database: LocalDiskContext,
) -> None:
    client = TestClient(create_rest_app(local_disk_database.database_dir))

    bulk_nodes = client.post(
        "/v1/nodes/bulk",
        json={
            "rows": [
                {"external_id": "alice", "name": "Alice"},
                {"external_id": "bob", "name": "Bob"},
            ],
            "default_labels": ["Person"],
        },
    )
    bulk_edges = client.post(
        "/v1/edges/bulk",
        json={
            "rows": [
                {
                    "src_key": "alice",
                    "dst_key": "bob",
                    "rel_type": "KNOWS",
                    "since": 2024,
                }
            ],
            "source_key": {"label": "Person", "key": "external_id"},
            "target_key": {"label": "Person", "key": "external_id"},
        },
    )
    match = client.post(
        "/v1/query",
        json={
            "query": (
                "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
                "RETURN a.external_id, b.external_id, r.since"
            ),
            "params": {},
        },
    )

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
    assert isinstance(nodes_payload, list)
    assert isinstance(edges_payload, list)
    assert isinstance(node_labels_payload, list)
    assert isinstance(adjacency_payload, list)

    node_file = str(nodes_payload[0]["path"])
    edge_file = str(edges_payload[0]["path"])
    node_label_file = str(node_labels_payload[0]["path"])
    adjacency_file = str(adjacency_payload[0]["path"])
    node_table = read_local_parquet_object(local_disk_database.database_dir, node_file)
    edge_table = read_local_parquet_object(local_disk_database.database_dir, edge_file)
    node_label_table = read_local_parquet_object(local_disk_database.database_dir, node_label_file)
    adjacency_table = read_local_parquet_object(local_disk_database.database_dir, adjacency_file)

    assert bulk_nodes.status_code == 201
    assert bulk_nodes.json()["meta"]["row_count"] == 2
    assert bulk_edges.status_code == 201
    assert bulk_edges.json()["meta"]["row_count"] == 1
    assert match.status_code == 200
    assert match.json()["data"]["rows"] == [
        {"a.external_id": "alice", "b.external_id": "bob", "r.since": 2024}
    ]
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
