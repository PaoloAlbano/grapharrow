import pytest

from grapharrow import Database
from tests.integration.support import (
    LocalDiskContext,
    assert_expected_local_storage_layout,
    has_local_parquet_magic,
    load_local_json_object,
    read_local_parquet_object,
)


@pytest.mark.integration
def test_database_supports_local_disk_storage_end_to_end(
    local_disk_database: LocalDiskContext,
) -> None:
    database = Database(local_disk_database.database_dir)

    database.initialize()

    with database.write_transaction() as session:
        session.query(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a, b"
        )

    with database.read_transaction() as session:
        result = session.query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")
        relationship_count = session.query("MATCH ()-[r]->() RETURN count(r) as count")

    manifest = database.load_snapshot()
    manifest_payload = load_local_json_object(
        local_disk_database.database_dir,
        database.load_current_snapshot_pointer().manifest_path,
    )
    nodes_table = read_local_parquet_object(
        local_disk_database.database_dir,
        manifest.nodes[0].path,
    )
    edges_table = read_local_parquet_object(
        local_disk_database.database_dir,
        manifest.edges[0].path,
    )
    node_labels_table = read_local_parquet_object(
        local_disk_database.database_dir,
        manifest.node_labels[0].path,
    )
    node_properties_table = read_local_parquet_object(
        local_disk_database.database_dir,
        manifest.node_properties[0].path,
    )
    adjacency_table = read_local_parquet_object(
        local_disk_database.database_dir,
        manifest.adjacency_by_source[0].path,
    )
    edge_properties_table = read_local_parquet_object(
        local_disk_database.database_dir,
        manifest.edge_properties[0].path,
    )

    assert result.rows == ({"a.name": "Alice", "b.name": "Bob"},)
    assert relationship_count.columns == ("count",)
    assert relationship_count.rows == ({"count": 1},)
    assert manifest_payload["snapshot_id"] == 2
    assert_expected_local_storage_layout(
        root_dir=local_disk_database.root_dir,
        database_dir=local_disk_database.database_dir,
        node_file=manifest.nodes[0].path,
        edge_file=manifest.edges[0].path,
        node_label_file=manifest.node_labels[0].path,
        adjacency_file=manifest.adjacency_by_source[0].path,
        node_property_file=manifest.node_properties[0].path,
        edge_property_file=manifest.edge_properties[0].path,
    )
    assert has_local_parquet_magic(local_disk_database.database_dir, manifest.nodes[0].path)
    assert has_local_parquet_magic(local_disk_database.database_dir, manifest.edges[0].path)
    assert has_local_parquet_magic(
        local_disk_database.database_dir,
        manifest.node_labels[0].path,
    )
    assert has_local_parquet_magic(
        local_disk_database.database_dir,
        manifest.node_properties[0].path,
    )
    assert has_local_parquet_magic(
        local_disk_database.database_dir,
        manifest.adjacency_by_source[0].path,
    )
    assert has_local_parquet_magic(
        local_disk_database.database_dir,
        manifest.edge_properties[0].path,
    )
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
