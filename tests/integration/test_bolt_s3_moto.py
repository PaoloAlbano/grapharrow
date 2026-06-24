import pytest
from neo4j import GraphDatabase, basic_auth

from grapharrow.config import GraphArrowSettings
from grapharrow.interfaces.bolt import run_bolt_server
from tests.integration.support import (
    MotoS3Context,
    assert_expected_storage_layout,
    has_parquet_magic,
    list_object_keys,
    load_json_object,
    read_parquet_object,
)


@pytest.mark.integration
def test_bolt_supports_s3_storage_via_moto_end_to_end(
    moto_s3_filesystem: MotoS3Context,
) -> None:
    bucket_uri = moto_s3_filesystem.bucket_uri
    settings = GraphArrowSettings(
        database_path=f"{bucket_uri}/example-db",
        bolt_host="127.0.0.1",
        bolt_port=0,
        bolt_auth_token="secret",
    )

    with run_bolt_server(
        f"{bucket_uri}/example-db",
        settings=settings,
        filesystem=moto_s3_filesystem.filesystem,
    ) as handle:
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
                assert create_record["a"]["properties"]["name"] == "Alice"
                assert create_record["b"]["properties"]["name"] == "Bob"

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

    current_snapshot = load_json_object(moto_s3_filesystem, "current_snapshot.json")
    current_manifest = load_json_object(
        moto_s3_filesystem,
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
    keys = list_object_keys(moto_s3_filesystem, prefix="example-db/")
    node_table = read_parquet_object(moto_s3_filesystem, node_file)
    edge_table = read_parquet_object(moto_s3_filesystem, edge_file)
    node_labels_table = read_parquet_object(moto_s3_filesystem, node_label_file)
    adjacency_table = read_parquet_object(moto_s3_filesystem, adjacency_file)

    assert current_snapshot["snapshot_id"] == 2
    assert current_manifest["snapshot_id"] == 2
    assert_expected_storage_layout(
        keys=keys,
        node_file=node_file,
        edge_file=edge_file,
        node_label_file=node_label_file,
        adjacency_file=adjacency_file,
    )
    assert has_parquet_magic(moto_s3_filesystem, node_file) is True
    assert has_parquet_magic(moto_s3_filesystem, edge_file) is True
    assert has_parquet_magic(moto_s3_filesystem, node_label_file) is True
    assert has_parquet_magic(moto_s3_filesystem, adjacency_file) is True
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


@pytest.mark.integration
def test_bolt_explicit_transaction_commit_publishes_single_snapshot_on_s3(
    moto_s3_filesystem: MotoS3Context,
) -> None:
    bucket_uri = moto_s3_filesystem.bucket_uri
    settings = GraphArrowSettings(
        database_path=f"{bucket_uri}/example-db",
        bolt_host="127.0.0.1",
        bolt_port=0,
        bolt_auth_token="secret",
    )

    with run_bolt_server(
        f"{bucket_uri}/example-db",
        settings=settings,
        filesystem=moto_s3_filesystem.filesystem,
    ) as handle:
        driver = GraphDatabase.driver(
            f"bolt://127.0.0.1:{handle.port}",
            auth=basic_auth("neo4j", "secret"),
        )
        try:
            with driver.session() as session:
                transaction = session.begin_transaction()
                try:
                    first = transaction.run("CREATE (n:Person {name: 'Alice'}) RETURN n").single()
                    second = transaction.run("CREATE (n:Person {name: 'Bob'}) RETURN n").single()
                    assert first is not None
                    assert second is not None
                    transaction.commit()
                finally:
                    transaction.close()

                persisted = session.run("MATCH (n:Person) RETURN n.name ORDER BY n.name ASC").data()
        finally:
            driver.close()

    current_snapshot = load_json_object(moto_s3_filesystem, "current_snapshot.json")
    current_manifest = load_json_object(
        moto_s3_filesystem,
        str(current_snapshot["manifest_path"]),
    )
    files_payload = current_manifest["files"]
    assert isinstance(files_payload, dict)
    nodes_payload = files_payload["nodes"]
    assert isinstance(nodes_payload, list)
    node_file = str(nodes_payload[0]["path"])
    node_table = read_parquet_object(moto_s3_filesystem, node_file)

    assert persisted == [{"n.name": "Alice"}, {"n.name": "Bob"}]
    assert current_snapshot["snapshot_id"] == 2
    assert current_manifest["snapshot_id"] == 2
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
