import pandas as pd
import pyarrow as pa  # type: ignore[import-untyped]
import pytest

from grapharrow import Database
from tests.integration.support import (
    LocalDiskContext,
    has_local_parquet_magic,
    read_local_parquet_object,
)


@pytest.mark.integration
def test_database_supports_bulk_ingest_on_local_disk(
    local_disk_database: LocalDiskContext,
) -> None:
    database = Database(local_disk_database.database_dir)
    database.initialize()

    with database.write_transaction() as session:
        session.add_nodes_bulk(
            pa.Table.from_pylist(
                [
                    {"external_id": "alice", "name": "Alice"},
                    {"external_id": "bob", "name": "Bob"},
                ]
            ),
            default_labels=["Person"],
        )
        session.add_edges_bulk(
            pd.DataFrame(
                [
                    {
                        "src_key": "alice",
                        "dst_key": "bob",
                        "rel_type": "KNOWS",
                        "since": 2024,
                    }
                ]
            ),
            source_key=("Person", "external_id"),
            target_key=("Person", "external_id"),
        )

    with database.read_transaction() as session:
        result = session.query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.external_id, b.external_id, r.since"
        )

    manifest = database.load_snapshot()
    node_table = read_local_parquet_object(
        local_disk_database.database_dir,
        manifest.nodes[0].path,
    )
    edge_table = read_local_parquet_object(
        local_disk_database.database_dir,
        manifest.edges[0].path,
    )

    assert result.rows == ({"a.external_id": "alice", "b.external_id": "bob", "r.since": 2024},)
    assert has_local_parquet_magic(local_disk_database.database_dir, manifest.nodes[0].path)
    assert has_local_parquet_magic(local_disk_database.database_dir, manifest.edges[0].path)
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


@pytest.mark.integration
def test_database_supports_large_edge_bulk_ingest_on_local_disk(
    local_disk_database: LocalDiskContext,
) -> None:
    database = Database(local_disk_database.database_dir)
    database.initialize()

    node_count = 1_000
    edge_count = 10_000

    with database.write_transaction() as session:
        session.add_nodes_bulk(
            pa.table(
                {
                    "labels": pa.array([["Person"]] * node_count, type=pa.list_(pa.string())),
                    "external_id": pa.array(
                        [f"person-{index}" for index in range(node_count)],
                        type=pa.string(),
                    ),
                    "rank": pa.array(list(range(node_count)), type=pa.int64()),
                }
            )
        )
        session.add_edges_bulk(
            pa.table(
                {
                    "src_id": pa.array(
                        [(index % node_count) + 1 for index in range(edge_count)],
                        type=pa.int64(),
                    ),
                    "dst_id": pa.array(
                        [((index + 1) % node_count) + 1 for index in range(edge_count)],
                        type=pa.int64(),
                    ),
                    "rel_type": pa.array(["KNOWS"] * edge_count, type=pa.string()),
                    "weight": pa.array(
                        [index % 7 for index in range(edge_count)],
                        type=pa.int64(),
                    ),
                }
            )
        )

    with database.read_transaction() as session:
        result = session.query("MATCH ()-[r:KNOWS]->() RETURN count(r) as count")

    manifest = database.load_snapshot()

    assert result.rows == ({"count": edge_count},)
    assert manifest.nodes[0].row_count == node_count
    assert manifest.edges[0].row_count == edge_count
