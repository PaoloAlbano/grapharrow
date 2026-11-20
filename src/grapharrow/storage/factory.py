"""Select a Storage Backend once at application construction."""

from __future__ import annotations

from grapharrow.adapters import FileSystemAdapter, ParquetAdapter, PyArrowTabularAdapter
from grapharrow.adapters.snapshot_tables import SnapshotTableCache
from grapharrow.config import GraphArrowSettings
from grapharrow.core import DatabaseLayout
from grapharrow.core.errors import StorageError
from grapharrow.storage.contracts import StorageBackend


def create_storage_backend(
    *,
    settings: GraphArrowSettings,
    layout: DatabaseLayout,
    filesystem: FileSystemAdapter,
    parquet_adapter: ParquetAdapter,
    tabular_adapter: PyArrowTabularAdapter,
    snapshot_tables: SnapshotTableCache | None = None,
) -> StorageBackend:
    """Build only the requested implementation and retain the supplied I/O adapters."""
    if settings.storage_backend == "delta":
        try:
            from grapharrow.storage.delta import DeltaStorageBackend
        except ModuleNotFoundError as exc:
            if exc.name != "deltalake":
                raise
            raise StorageError("Install grapharrow[delta] to use the Delta backend.") from exc
        return DeltaStorageBackend(
            layout=layout, settings=settings, filesystem=filesystem, snapshot_tables=snapshot_tables
        )

    if settings.storage_backend != "native":
        raise StorageError(f"Unsupported Storage Backend: {settings.storage_backend}")

    from grapharrow.repositories import CatalogRepository, GraphRepository, SnapshotRepository
    from grapharrow.storage.native import NativeStorageBackend

    snapshots = SnapshotRepository(layout=layout, filesystem=filesystem)
    return NativeStorageBackend(
        catalog=CatalogRepository(layout=layout, filesystem=filesystem, snapshot_store=snapshots),
        snapshot=snapshots,
        graph=GraphRepository(
            layout=layout,
            filesystem=filesystem,
            parquet_adapter=parquet_adapter,
            tabular_adapter=tabular_adapter,
            edge_parquet_range_size=settings.edge_parquet_range_size,
        ),
        filesystem=filesystem,
    )
