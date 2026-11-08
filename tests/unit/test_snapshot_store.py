import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from threading import Event

import pytest

from grapharrow.core import DatabaseLayout, WriteConflictError
from grapharrow.domain import CatalogVersion, DatasetFile, SnapshotId, SnapshotManifest
from grapharrow.repositories import SnapshotRepository


def test_published_manifest_retry_does_not_rewrite_reader_visible_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = SnapshotRepository(DatabaseLayout(root=tmp_path / "immutable-manifest"))
    manifest = store.bootstrap(catalog_version=CatalogVersion(1))
    pointer = store.load_current_pointer()

    def fail(*args: object, **kwargs: object) -> None:
        raise AssertionError("Published metadata was rewritten")

    monkeypatch.setattr(store._filesystem, "write_json", fail)
    monkeypatch.setattr(store._filesystem, "replace_json", fail)
    assert store.publish(manifest) == pointer
    assert store.write_manifest(manifest).exists()
    with pytest.raises(WriteConflictError):
        store.publish(replace(manifest, commit_txid=manifest.commit_txid + 1))
    with pytest.raises(WriteConflictError):
        store.write_manifest(replace(manifest, commit_txid=manifest.commit_txid + 1))


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


@pytest.mark.parametrize("operation", ["fsync", "replace"])
def test_failed_local_pointer_publication_preserves_the_current_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    layout = DatabaseLayout(root=tmp_path / "example-db")
    store = SnapshotRepository(layout)
    original = store.bootstrap(catalog_version=CatalogVersion(1))
    original_pointer = store.load_current_pointer()
    candidate = SnapshotManifest.empty(snapshot_id=SnapshotId(2), catalog_version=CatalogVersion(1))

    def fail(*args: object) -> None:
        raise OSError(f"Simulated {operation} failure")

    with monkeypatch.context() as patch:
        patch.setattr(os, operation, fail)
        with pytest.raises(OSError, match=f"Simulated {operation} failure"):
            store.publish(candidate, staged_relative_path="staged/tx-000002")

    assert store.load_current_pointer() == original_pointer
    assert store.load_manifest() == original
    assert not list((tmp_path / "example-db").glob(".current_snapshot.json.*.tmp"))

    published = store.publish(candidate)
    assert store.load_current_pointer() == published
    assert store.load_manifest() == candidate


@pytest.mark.parametrize("use_file_uri", [False, True])
def test_reader_sees_complete_pointer_before_and_after_local_replacement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, use_file_uri: bool
) -> None:
    root = tmp_path / "example-db"
    layout = DatabaseLayout(root=root.as_uri() if use_file_uri else root)
    writer, reader = SnapshotRepository(layout), SnapshotRepository(layout)
    original = writer.bootstrap(catalog_version=CatalogVersion(1))
    candidate = SnapshotManifest.empty(snapshot_id=SnapshotId(2), catalog_version=CatalogVersion(1))
    ready = Event()
    allow_replace = Event()
    replace = os.replace

    def pause_before_replace(source: Path, destination: Path) -> None:
        ready.set()
        if not allow_replace.wait(timeout=10):
            raise TimeoutError("Test reader did not release snapshot replacement")
        replace(source, destination)

    monkeypatch.setattr(os, "replace", pause_before_replace)
    with ThreadPoolExecutor(max_workers=1) as executor:
        pending = executor.submit(writer.publish, candidate)
        try:
            assert ready.wait(timeout=10), "Writer did not reach pointer replacement"
            assert reader.load_current_pointer().snapshot_id == original.snapshot_id
            assert reader.load_manifest() == original
        finally:
            allow_replace.set()
        published = pending.result(timeout=10)

    assert reader.load_current_pointer() == published
    assert reader.load_manifest() == candidate
