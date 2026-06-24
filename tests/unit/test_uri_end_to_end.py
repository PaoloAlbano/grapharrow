from pathlib import Path

import pyarrow.fs as pafs  # type: ignore[import-untyped]
import pytest

from grapharrow import Database
from grapharrow.adapters import FileSystemAdapter


def test_database_supports_file_uri_end_to_end(tmp_path: Path) -> None:
    database_root = (tmp_path / "example-db").resolve()
    database = Database(database_root.as_uri())

    database.initialize()

    with database.write_transaction() as session:
        session.query(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a, b"
        )

    with database.read_transaction() as session:
        result = session.query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")

    manifest = database.load_snapshot()
    filesystem = FileSystemAdapter()

    assert result.rows == ({"a.name": "Alice", "b.name": "Bob"},)
    assert filesystem.exists(f"{database_root.as_uri()}/current_snapshot.json") is True
    assert filesystem.exists(f"{database_root.as_uri()}/{manifest.nodes[0].path}") is True
    assert filesystem.exists(f"{database_root.as_uri()}/{manifest.edges[0].path}") is True
    assert filesystem.exists(f"{database_root.as_uri()}/{manifest.node_labels[0].path}") is True
    assert filesystem.exists(f"{database_root.as_uri()}/{manifest.adjacency_by_source[0].path}")


@pytest.mark.parametrize(
    ("uri_prefix", "mount_root"),
    (
        ("s3://grapharrow-tests", "grapharrow-tests"),
        ("gs://grapharrow-tests", "grapharrow-tests"),
        ("az://grapharrow-tests", "grapharrow-tests"),
    ),
)
def test_database_supports_mounted_cloud_uris_end_to_end(
    uri_prefix: str,
    mount_root: str,
) -> None:
    filesystem = FileSystemAdapter(mounts={uri_prefix: (pafs._MockFileSystem(), mount_root)})
    database = Database(f"{uri_prefix}/example-db", filesystem=filesystem)

    database.initialize()

    with database.write_transaction() as session:
        session.query(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a, b"
        )

    with database.read_transaction() as session:
        result = session.query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")

    manifest = database.load_snapshot()
    verification = database.verify(verify_checksums=True)

    assert result.rows == ({"a.name": "Alice", "b.name": "Bob"},)
    assert verification.ok is True
    assert filesystem.exists(f"{uri_prefix}/example-db/current_snapshot.json") is True
    assert filesystem.exists(f"{uri_prefix}/example-db/{manifest.nodes[0].path}") is True
    assert filesystem.exists(f"{uri_prefix}/example-db/{manifest.edges[0].path}") is True
    assert filesystem.exists(f"{uri_prefix}/example-db/{manifest.node_labels[0].path}") is True
    assert filesystem.exists(f"{uri_prefix}/example-db/{manifest.adjacency_by_source[0].path}")
