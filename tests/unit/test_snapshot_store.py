from pathlib import Path

from grapharrow.core import DatabaseLayout
from grapharrow.domain import CatalogVersion, DatasetFile, SnapshotId, SnapshotManifest
from grapharrow.repositories import SnapshotRepository


def test_snapshot_store_can_publish_and_load_a_manifest(tmp_path: Path) -> None:
    layout = DatabaseLayout(root=tmp_path / "example-db")
    store = SnapshotRepository(layout=layout)
    manifest = SnapshotManifest(
        snapshot_id=SnapshotId(2),
        parent_snapshot_id=SnapshotId(1),
        commit_txid=7,
        created_at="2026-03-31T12:00:00+00:00",
        catalog_version=CatalogVersion(3),
        nodes=(
            DatasetFile(
                path="data/nodes/chunk-000001.parquet",
                row_count=10,
                kind="nodes",
                checksum="abc123",
                checksum_algorithm="sha256",
                min_key=1,
                max_key=10,
            ),
        ),
    )

    pointer = store.publish(manifest)
    loaded_manifest = store.load_manifest()
    loaded_pointer = store.load_current_pointer()

    assert pointer.snapshot_id == SnapshotId(2)
    assert loaded_pointer == pointer
    assert loaded_manifest == manifest


def test_snapshot_store_bootstrap_creates_the_initial_snapshot(tmp_path: Path) -> None:
    layout = DatabaseLayout(root=tmp_path / "example-db")
    store = SnapshotRepository(layout=layout)

    manifest = store.bootstrap(catalog_version=CatalogVersion(1))

    assert manifest.snapshot_id == SnapshotId(1)
    assert manifest.catalog_version == CatalogVersion(1)
    assert store.current_exists() is True
    assert isinstance(layout.current_snapshot_path, Path)
    assert layout.current_snapshot_path.exists()


def test_snapshot_store_can_stage_a_candidate_manifest(tmp_path: Path) -> None:
    layout = DatabaseLayout(root=tmp_path / "example-db")
    store = SnapshotRepository(layout=layout)
    manifest = SnapshotManifest.empty(
        snapshot_id=SnapshotId(2),
        catalog_version=CatalogVersion(1),
    )

    staged_manifest_path = store.stage_manifest(manifest, staged_relative_path="staged/tx-000002")

    assert isinstance(staged_manifest_path, Path)
    assert staged_manifest_path.exists()
    assert staged_manifest_path.name == "manifest.json"
