from pathlib import Path

from grapharrow.core import DatabaseLayout
from grapharrow.domain import DatasetFile, SnapshotId, SnapshotManifest
from grapharrow.repositories import CatalogRepository, GraphRepository, SnapshotRepository
from grapharrow.use_cases import DatabaseAdminUseCase


def test_admin_service_initialize_bootstraps_catalog_and_snapshot(tmp_path: Path) -> None:
    layout = DatabaseLayout(root=tmp_path / "example-db")
    service = DatabaseAdminUseCase(
        layout=layout,
        catalog_store=CatalogRepository(layout=layout),
        snapshot_store=SnapshotRepository(layout=layout),
        graph_store=GraphRepository(layout=layout),
    )

    result = service.initialize()

    assert result.catalog.version.value == 1
    assert result.snapshot.snapshot_id.value == 1
    assert isinstance(layout.current_catalog_path, Path)
    assert isinstance(layout.current_snapshot_path, Path)
    assert layout.current_catalog_path.exists()
    assert layout.current_snapshot_path.exists()


def test_admin_service_verify_flags_missing_manifest_files(tmp_path: Path) -> None:
    layout = DatabaseLayout(root=tmp_path / "example-db")
    service = DatabaseAdminUseCase(
        layout=layout,
        catalog_store=CatalogRepository(layout=layout),
        snapshot_store=SnapshotRepository(layout=layout),
        graph_store=GraphRepository(layout=layout),
    )
    service.initialize()
    snapshot_store = SnapshotRepository(layout=layout)
    manifest = snapshot_store.load_manifest()
    broken_manifest = SnapshotManifest(
        snapshot_id=SnapshotId(manifest.snapshot_id.value + 1),
        parent_snapshot_id=manifest.snapshot_id,
        commit_txid=manifest.commit_txid + 1,
        created_at=manifest.created_at,
        catalog_version=manifest.catalog_version,
        nodes=(
            manifest.nodes
            + (
                DatasetFile(
                    path="data/nodes/missing.parquet",
                    row_count=1,
                    kind="nodes",
                ),
            )
        ),
        edges=manifest.edges,
        node_labels=manifest.node_labels,
        adjacency_by_source=manifest.adjacency_by_source,
        adjacency_by_dest=manifest.adjacency_by_dest,
    )
    snapshot_store.publish(broken_manifest)

    report = service.verify()

    assert report.ok is False
    assert report.missing_paths == ("data/nodes/missing.parquet",)


def test_admin_service_recover_can_remove_orphan_staged_paths(tmp_path: Path) -> None:
    layout = DatabaseLayout(root=tmp_path / "example-db")
    service = DatabaseAdminUseCase(
        layout=layout,
        catalog_store=CatalogRepository(layout=layout),
        snapshot_store=SnapshotRepository(layout=layout),
        graph_store=GraphRepository(layout=layout),
    )
    service.initialize()
    assert isinstance(layout.staged_dir, Path)
    orphan_path = layout.staged_dir / "tx-000123"
    orphan_path.mkdir(parents=True)

    report = service.recover(dry_run=False)

    assert report.orphan_staged_paths == ("staged/tx-000123",)
    assert report.removed_paths == ("staged/tx-000123",)
    assert orphan_path.exists() is False


def test_admin_service_compact_rebuilds_a_fresh_snapshot(tmp_path: Path) -> None:
    layout = DatabaseLayout(root=tmp_path / "example-db")
    service = DatabaseAdminUseCase(
        layout=layout,
        catalog_store=CatalogRepository(layout=layout),
        snapshot_store=SnapshotRepository(layout=layout),
        graph_store=GraphRepository(layout=layout),
    )
    service.initialize()
    graph_store = GraphRepository(layout=layout)

    from grapharrow.use_cases.graph import GraphStateUseCase

    graph_service = GraphStateUseCase(
        admin_service=service,
        catalog_store=CatalogRepository(layout=layout),
        snapshot_store=SnapshotRepository(layout=layout),
        graph_store=graph_store,
    )
    graph_service.create_relationship_with_inline_nodes(
        left_labels=("Person",),
        left_properties={"name": "Alice"},
        rel_type="KNOWS",
        rel_properties={},
        right_labels=("Person",),
        right_properties={"name": "Bob"},
    )

    manifest = service.compact()

    assert manifest.snapshot_id.value == 3
    assert len(manifest.node_labels) == 1
    assert len(manifest.adjacency_by_source) == 1


def test_admin_service_vacuum_removes_obsolete_snapshot_files(tmp_path: Path) -> None:
    layout = DatabaseLayout(root=tmp_path / "example-db")
    service = DatabaseAdminUseCase(
        layout=layout,
        catalog_store=CatalogRepository(layout=layout),
        snapshot_store=SnapshotRepository(layout=layout),
        graph_store=GraphRepository(layout=layout),
    )
    service.initialize()

    from grapharrow.use_cases.graph import GraphStateUseCase

    graph_service = GraphStateUseCase(
        admin_service=service,
        catalog_store=CatalogRepository(layout=layout),
        snapshot_store=SnapshotRepository(layout=layout),
        graph_store=GraphRepository(layout=layout),
    )
    graph_service.create_node(labels=("Person",), properties={"name": "Alice"})
    graph_service.create_node(labels=("Person",), properties={"name": "Bob"})
    current_manifest = SnapshotRepository(layout=layout).load_manifest()

    removed = service.vacuum()

    assert any(path.startswith("manifests/snapshot-000001") for path in removed)
    assert any(path.startswith("data/nodes/snapshot-000002") for path in removed)
    assert isinstance(layout.root, Path)
    assert (layout.root / current_manifest.nodes[0].path).exists() is True
