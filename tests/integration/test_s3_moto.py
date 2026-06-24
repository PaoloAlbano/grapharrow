import pytest

from grapharrow import Database
from tests.integration.support import (
    MotoS3Context,
    assert_expected_storage_layout,
    has_parquet_magic,
    list_object_keys,
    load_json_object,
    read_parquet_object,
)


@pytest.mark.integration
def test_database_supports_s3_storage_via_moto_end_to_end(
    moto_s3_filesystem: MotoS3Context,
) -> None:
    filesystem = moto_s3_filesystem.filesystem
    bucket_uri = moto_s3_filesystem.bucket_uri
    database = Database(f"{bucket_uri}/example-db", filesystem=filesystem)

    database.initialize()

    with database.write_transaction() as session:
        session.query(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a, b"
        )

    with database.read_transaction() as session:
        result = session.query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")
        relationship_count = session.query("MATCH ()-[r]->() RETURN count(r) as count")

    manifest = database.load_snapshot()
    verification = database.verify(verify_checksums=True)
    keys = list_object_keys(moto_s3_filesystem, prefix="example-db/")
    manifest_payload = load_json_object(
        moto_s3_filesystem,
        database.load_current_snapshot_pointer().manifest_path,
    )
    nodes_table = read_parquet_object(moto_s3_filesystem, manifest.nodes[0].path)
    edges_table = read_parquet_object(moto_s3_filesystem, manifest.edges[0].path)
    node_labels_table = read_parquet_object(
        moto_s3_filesystem,
        manifest.node_labels[0].path,
    )
    node_properties_table = read_parquet_object(
        moto_s3_filesystem,
        manifest.node_properties[0].path,
    )
    adjacency_table = read_parquet_object(
        moto_s3_filesystem,
        manifest.adjacency_by_source[0].path,
    )
    edge_properties_table = read_parquet_object(
        moto_s3_filesystem,
        manifest.edge_properties[0].path,
    )

    pairs = {(row["a.name"], row["b.name"]) for row in result.rows}
    assert pairs == {("Alice", "Bob")}
    assert relationship_count.columns == ("count",)
    assert relationship_count.rows == ({"count": 1},)
    assert verification.ok is True
    assert database.load_current_snapshot_pointer().manifest_path.startswith("manifests/")
    assert len(manifest.node_labels) == 1
    assert len(manifest.node_properties) == 1
    assert len(manifest.adjacency_by_source) == 1
    assert len(manifest.edge_properties) == 1
    assert_expected_storage_layout(
        keys=keys,
        node_file=manifest.nodes[0].path,
        edge_file=manifest.edges[0].path,
        node_label_file=manifest.node_labels[0].path,
        adjacency_file=manifest.adjacency_by_source[0].path,
        node_property_file=manifest.node_properties[0].path,
        edge_property_file=manifest.edge_properties[0].path,
    )
    assert manifest_payload["snapshot_id"] == 2
    files_payload = manifest_payload["files"]
    assert isinstance(files_payload, dict)
    nodes_payload = files_payload["nodes"]
    edges_payload = files_payload["edges"]
    assert isinstance(nodes_payload, list)
    assert isinstance(edges_payload, list)
    assert nodes_payload[0]["path"] == manifest.nodes[0].path
    assert edges_payload[0]["path"] == manifest.edges[0].path
    assert has_parquet_magic(moto_s3_filesystem, manifest.nodes[0].path) is True
    assert has_parquet_magic(moto_s3_filesystem, manifest.edges[0].path) is True
    assert has_parquet_magic(moto_s3_filesystem, manifest.node_labels[0].path) is True
    assert has_parquet_magic(moto_s3_filesystem, manifest.node_properties[0].path) is True
    assert has_parquet_magic(moto_s3_filesystem, manifest.adjacency_by_source[0].path) is True
    assert has_parquet_magic(moto_s3_filesystem, manifest.edge_properties[0].path) is True
    assert nodes_table.to_pylist() == [
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
    assert edges_table.to_pylist() == [
        {
            "edge_id": 1,
            "rel_type": "KNOWS",
            "src_id": 1,
            "dst_id": 2,
            "properties_json": "{}",
        }
    ]
    assert node_labels_table.to_pylist() == [{"node_id": 1}, {"node_id": 2}]
    assert node_properties_table.to_pylist() == [
        {"node_id": 1, "name": "Alice"},
        {"node_id": 2, "name": "Bob"},
    ]
    assert adjacency_table.to_pylist() == [{"src_id": 1, "dst_id": 2, "edge_id": 1}]
    assert edge_properties_table.to_pylist() == [{"edge_id": 1, "src_id": 1, "dst_id": 2}]
