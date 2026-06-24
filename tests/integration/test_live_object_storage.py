from __future__ import annotations

from typing import Any, cast

import pytest

from grapharrow import Database
from tests.integration.support import (
    RemoteObjectStorageContext,
    assert_expected_remote_storage_layout,
    has_remote_parquet_magic,
    list_remote_paths,
    load_remote_json_object,
    read_remote_parquet_object,
)


def _assert_live_remote_storage_roundtrip(context: RemoteObjectStorageContext) -> None:
    database = Database(context.database_uri, filesystem=context.filesystem)

    with database.write_transaction() as session:
        create_result = session.query(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2025}]->(b:Person {name: 'Bob'}) "
            "RETURN a, b"
        )
        count_result = session.query("MATCH ()-[r]->() RETURN count(r) as count")

    assert create_result.rows == (
        {
            "a": {"id": 1, "labels": ["Person"], "properties": {"name": "Alice"}},
            "b": {"id": 2, "labels": ["Person"], "properties": {"name": "Bob"}},
        },
    )
    assert count_result.rows == ({"count": 1},)

    verify_result = database.verify(verify_checksums=True)
    assert verify_result.ok is True

    current_snapshot = load_remote_json_object(context, "current_snapshot.json")
    manifest_path = str(current_snapshot["path"])
    manifest = cast(
        dict[str, Any],
        load_remote_json_object(
            context,
            manifest_path.removeprefix(f"{context.database_uri}/"),
        ),
    )

    node_entries = cast(list[dict[str, Any]], manifest["nodes"])
    edge_entries = cast(list[dict[str, Any]], manifest["edges"])
    node_label_entries = cast(list[dict[str, Any]], manifest["node_labels"])
    adjacency_entries = cast(list[dict[str, Any]], manifest["adjacency_by_source"])

    node_file = str(node_entries[0]["path"]).removeprefix(f"{context.database_uri}/")
    edge_file = str(edge_entries[0]["path"]).removeprefix(f"{context.database_uri}/")
    node_label_file = str(node_label_entries[0]["path"]).removeprefix(f"{context.database_uri}/")
    adjacency_file = str(adjacency_entries[0]["path"]).removeprefix(f"{context.database_uri}/")

    paths = list_remote_paths(context)
    assert_expected_remote_storage_layout(
        paths=paths,
        database_uri=context.database_uri,
        node_file=node_file,
        edge_file=edge_file,
        node_label_file=node_label_file,
        adjacency_file=adjacency_file,
    )

    node_table = read_remote_parquet_object(context, node_file)
    edge_table = read_remote_parquet_object(context, edge_file)
    node_labels_table = read_remote_parquet_object(context, node_label_file)
    adjacency_table = read_remote_parquet_object(context, adjacency_file)

    assert node_table.num_rows == 2
    assert edge_table.num_rows == 1
    assert node_labels_table.num_rows == 2
    assert adjacency_table.num_rows == 1
    assert edge_table.column("rel_type").to_pylist() == ["KNOWS"]
    assert edge_table.column("src_id").to_pylist() == [1]
    assert edge_table.column("dst_id").to_pylist() == [2]
    assert has_remote_parquet_magic(context, node_file) is True
    assert has_remote_parquet_magic(context, edge_file) is True
    assert has_remote_parquet_magic(context, node_label_file) is True
    assert has_remote_parquet_magic(context, adjacency_file) is True


@pytest.mark.integration
@pytest.mark.live_storage
def test_database_supports_live_s3_storage_end_to_end(
    live_s3_database: RemoteObjectStorageContext,
) -> None:
    _assert_live_remote_storage_roundtrip(live_s3_database)


@pytest.mark.integration
@pytest.mark.live_storage
def test_database_supports_live_gcs_storage_end_to_end(
    live_gcs_database: RemoteObjectStorageContext,
) -> None:
    _assert_live_remote_storage_roundtrip(live_gcs_database)


@pytest.mark.integration
@pytest.mark.live_storage
def test_database_supports_live_azure_storage_end_to_end(
    live_azure_database: RemoteObjectStorageContext,
) -> None:
    _assert_live_remote_storage_roundtrip(live_azure_database)
