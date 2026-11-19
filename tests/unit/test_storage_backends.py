"""The native Storage Backend preserves the existing engine and persistence contract."""

import json
import sys
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from grapharrow.adapters import FileSystemAdapter, ParquetAdapter, PyArrowTabularAdapter
from grapharrow.app import build_application
from grapharrow.config import GraphArrowSettings
from grapharrow.core import DatabaseLayout
from grapharrow.core.errors import StorageError
from grapharrow.domain.records import EdgeRecord, NodeRecord
from grapharrow.repositories import EdgeRecord as LegacyEdgeRecord
from grapharrow.repositories import NodeRecord as LegacyNodeRecord
from grapharrow.storage import StorageBackend, create_storage_backend
from grapharrow.use_cases import DatabaseAdminUseCase, GraphStateUseCase


def test_default_backend_shares_the_existing_stores_and_writer(tmp_path: Path) -> None:
    app = build_application(tmp_path)
    backend = app.storage_backend
    assert isinstance(backend, StorageBackend)
    assert backend.name == app.settings.storage_backend == "native"
    assert backend.catalog is app.repositories.catalog
    assert backend.snapshot is app.repositories.snapshot
    assert backend.graph is app.repositories.graph
    assert backend.writer is backend.snapshot.writer is backend.catalog.writer
    assert backend.writer.local_root == tmp_path.resolve()
    assert LegacyNodeRecord is NodeRecord
    assert LegacyEdgeRecord is EdgeRecord


def test_factory_preserves_supplied_filesystem_and_arrow_adapters(tmp_path: Path) -> None:
    filesystem = FileSystemAdapter()
    parquet = ParquetAdapter(filesystem=filesystem, filter_pushdown_enabled=False)
    tabular = PyArrowTabularAdapter()
    backend = create_storage_backend(
        settings=GraphArrowSettings(edge_parquet_range_size=4),
        layout=DatabaseLayout(tmp_path),
        filesystem=filesystem,
        parquet_adapter=parquet,
        tabular_adapter=tabular,
    )
    assert backend.graph._filesystem is filesystem
    assert backend.graph._parquet is parquet
    assert backend.graph._tabular is tabular
    assert backend.graph._edge_parquet_range_size == 4
    assert list(tmp_path.iterdir()) == []


def test_settings_select_native_from_the_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("GRAPHARROW_STORAGE_BACKEND", "native")
    assert build_application(tmp_path).storage_backend.name == "native"


def test_delta_dependency_is_only_required_for_delta(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setitem(sys.modules, "deltalake", None)
    monkeypatch.delitem(sys.modules, "grapharrow.storage.delta", raising=False)
    app = build_application(tmp_path, settings=GraphArrowSettings(storage_backend="native"))
    assert app.storage_backend.name == "native"
    with pytest.raises(StorageError, match=r"Install grapharrow\[delta\]"):
        build_application(tmp_path, settings=GraphArrowSettings(storage_backend="delta"))
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("name", ["iceberg", "unknown"])
def test_unavailable_backends_are_rejected_without_initializing_a_database(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str
) -> None:
    monkeypatch.setenv("GRAPHARROW_STORAGE_BACKEND", name)
    with pytest.raises(ValidationError, match="storage_backend"):
        build_application(tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_factory_rejects_unknown_backend_even_if_settings_validation_is_bypassed(
    tmp_path: Path,
) -> None:
    filesystem = FileSystemAdapter()
    with pytest.raises(StorageError, match="Unsupported Storage Backend"):
        create_storage_backend(
            settings=GraphArrowSettings.model_construct(storage_backend="unknown"),
            layout=DatabaseLayout(tmp_path),
            filesystem=filesystem,
            parquet_adapter=ParquetAdapter(filesystem=filesystem),
            tabular_adapter=PyArrowTabularAdapter(),
        )
    assert list(tmp_path.iterdir()) == []


def test_graph_engine_delegates_full_and_selective_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = build_application(tmp_path)
    backend = app.storage_backend
    calls = []
    publish = backend.publish_graph
    edit = backend.edit_edge_properties_by_id

    def publication(**kwargs: Any) -> Any:
        calls.append("publish")
        return publish(**kwargs)

    def point_edit(**kwargs: Any) -> Any:
        calls.append("point")
        return edit(**kwargs)

    monkeypatch.setattr(backend, "publish_graph", publication)
    monkeypatch.setattr(backend, "edit_edge_properties_by_id", point_edit)
    app.use_cases.cypher.execute("CREATE ()-[:LINK {value:'old'}]->()")
    before = backend.capture_snapshot()
    result = app.use_cases.cypher.execute(
        "MATCH ()-[r:LINK]->() WHERE id(r)=1 SET r.value='new' RETURN r.value AS value"
    )
    assert result.rows == ({"value": "new"},)
    assert calls == ["publish", "point"]
    after = backend.capture_snapshot()
    assert after.snapshot_id.value == before.snapshot_id.value + 1
    assert app.use_cases.graph.load_state(snapshot=before).edges[0].properties["value"] == "old"
    assert app.use_cases.admin.verify(verify_checksums=True).ok


def test_backend_selection_does_not_change_native_manifest_format(tmp_path: Path) -> None:
    app = build_application(tmp_path)
    app.use_cases.cypher.execute("CREATE (:Person {name:'same-layout'})")
    head = json.loads((tmp_path / "current_snapshot.json").read_text())
    manifest = json.loads((tmp_path / head["manifest_path"]).read_text())
    assert set(head) == {"snapshot_id", "manifest_path", "commit_txid", "created_at"}
    assert set(manifest["files"]) == {
        "nodes",
        "edges",
        "node_labels",
        "node_properties",
        "adjacency_by_source",
        "adjacency_by_dest",
        "edge_properties",
    }
    assert "storage_backend" not in manifest
    assert not (tmp_path / "database.json").exists()
    reopened = build_application(tmp_path)
    assert reopened.use_cases.cypher.execute("MATCH (p:Person) RETURN p.name AS name").rows == (
        {"name": "same-layout"},
    )


def test_engine_rejects_a_backend_with_different_stores(tmp_path: Path) -> None:
    first, second = build_application(tmp_path / "first"), build_application(tmp_path / "second")
    with pytest.raises(ValueError, match="same catalog, snapshot and graph stores"):
        GraphStateUseCase(
            catalog_store=first.repositories.catalog,
            snapshot_store=first.repositories.snapshot,
            graph_store=first.repositories.graph,
            storage_backend=second.storage_backend,
        )


def test_admin_delegates_to_the_selected_maintenance_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = build_application(tmp_path)
    backend = app.storage_backend
    calls = []
    original = backend.maintenance.compact

    def compact() -> Any:
        calls.append("compact")
        return original()

    monkeypatch.setattr(backend.maintenance, "compact", compact)
    app.use_cases.cypher.execute("CREATE (:Person)")
    result = DatabaseAdminUseCase(storage_backend=backend).compact()
    assert calls == ["compact"]
    assert result == backend.capture_snapshot()
    assert app.use_cases.admin.verify(verify_checksums=True).ok
