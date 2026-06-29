from pathlib import Path

import pyarrow.parquet as pq  # type: ignore[import-untyped]

from grapharrow import Database
from grapharrow.domain import CatalogVersion, SnapshotId


def test_database_resolves_path(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    assert database.path == (tmp_path / "example-db").resolve()


def test_database_creates_read_and_write_sessions(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    read_session = database.read_transaction()
    write_session = database.write_transaction()

    assert read_session.mode == "read"
    assert write_session.mode == "write"


def test_session_context_manager_closes_session(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.session() as session:
        assert session.closed is False

    assert session.closed is True


def test_database_initialize_bootstraps_layout_and_catalog(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    result = database.initialize()
    current_snapshot = database.load_current_snapshot_pointer()
    manifest = database.load_snapshot()

    assert result.catalog.version == CatalogVersion(1)
    assert result.snapshot.snapshot_id == SnapshotId(1)
    assert isinstance(database.layout.catalog_dir, Path)
    assert isinstance(database.layout.manifests_dir, Path)
    assert isinstance(database.layout.staged_dir, Path)
    assert isinstance(database.layout.data_dir, Path)
    assert isinstance(database.layout.derived_dir, Path)
    assert isinstance(database.layout.current_catalog_path, Path)
    assert isinstance(database.layout.current_snapshot_path, Path)
    assert database.layout.catalog_dir.exists()
    assert database.layout.manifests_dir.exists()
    assert database.layout.staged_dir.exists()
    assert database.layout.data_dir.exists()
    assert database.layout.derived_dir.exists()
    assert database.layout.current_catalog_path.exists()
    assert database.layout.current_snapshot_path.exists()
    assert current_snapshot.snapshot_id == SnapshotId(1)
    assert manifest.snapshot_id == SnapshotId(1)
    assert manifest.catalog_version == CatalogVersion(1)


def test_published_snapshot_contains_derived_files_for_labels_and_adjacency(tmp_path: Path) -> None:
    database = Database(tmp_path / "example-db")

    with database.write_transaction() as session:
        session.query(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a, b"
        )
        session.query(
            "CREATE (a:Person {name: 'Carol'})-[:KNOWS]->(b:Person {name: 'Alice'}) RETURN a, b"
        )
        session.query(
            "CREATE (a:Person {name: 'Dave'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN a, b"
        )

    manifest = database.load_snapshot()

    assert len(manifest.nodes) == 1
    assert len(manifest.edges) == 1
    assert len(manifest.node_labels) == 1
    assert len(manifest.node_properties) == 1
    assert len(manifest.adjacency_by_source) == 1
    assert len(manifest.adjacency_by_dest) == 1
    assert len(manifest.edge_properties) == 1
    assert isinstance(database.path, Path)
    assert (database.path / manifest.node_labels[0].path).exists() is True
    assert (database.path / manifest.node_properties[0].path).exists() is True
    assert (database.path / manifest.adjacency_by_source[0].path).exists() is True
    assert (database.path / manifest.adjacency_by_dest[0].path).exists() is True
    assert (database.path / manifest.edge_properties[0].path).exists() is True

    adjacency_by_dest = pq.read_table(database.path / manifest.adjacency_by_dest[0].path)
    assert adjacency_by_dest.column_names == ["src_id", "dst_id", "edge_id"]
    assert adjacency_by_dest["dst_id"].to_pylist() == sorted(
        adjacency_by_dest["dst_id"].to_pylist()
    )
