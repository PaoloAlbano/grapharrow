"""Application wiring for GraphArrow components."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from grapharrow.adapters import (
    BulkInputAdapter,
    FileSystemAdapter,
    OpenCypherAdapter,
    ParquetAdapter,
    PyArrowTabularAdapter,
)
from grapharrow.adapters.query_cache import CypherParseCache
from grapharrow.adapters.snapshot_tables import SnapshotTableCache
from grapharrow.config import GraphArrowSettings
from grapharrow.core import DatabaseLayout
from grapharrow.storage import StorageBackend, create_storage_backend
from grapharrow.storage.contracts import CatalogStore, GraphStore, SnapshotStore
from grapharrow.use_cases import (
    CypherQueryUseCase,
    DatabaseAdminUseCase,
    GraphStateUseCase,
    IndexCatalogUseCase,
)
from grapharrow.use_cases.ana_node_lookup import AnaNodeLookupCache
from grapharrow.use_cases.ana_schema_cache import SnapshotSchemaCache


@dataclass(frozen=True, slots=True)
class GraphArrowAdapters:
    """Technical adapters used by repositories and interfaces."""

    bulk_input: BulkInputAdapter
    filesystem: FileSystemAdapter
    parquet: ParquetAdapter
    opencypher: OpenCypherAdapter
    tabular: PyArrowTabularAdapter


@dataclass(frozen=True, slots=True)
class GraphArrowRepositories:
    """Persistence-facing components."""

    catalog: CatalogStore
    snapshot: SnapshotStore
    graph: GraphStore


@dataclass(frozen=True, slots=True)
class GraphArrowUseCases:
    """Business use cases exposed to interfaces."""

    admin: DatabaseAdminUseCase
    graph: GraphStateUseCase
    cypher: CypherQueryUseCase
    indexes: IndexCatalogUseCase


@dataclass(frozen=True, slots=True)
class GraphArrowApplication:
    """Fully wired GraphArrow application container."""

    settings: GraphArrowSettings
    layout: DatabaseLayout
    adapters: GraphArrowAdapters
    repositories: GraphArrowRepositories
    storage_backend: StorageBackend
    use_cases: GraphArrowUseCases
    _ana_schema_cache: SnapshotSchemaCache = field(default_factory=SnapshotSchemaCache, repr=False)
    _ana_node_lookups: AnaNodeLookupCache = field(default_factory=AnaNodeLookupCache, repr=False)


def build_application(
    path: str | Path,
    *,
    settings: GraphArrowSettings | None = None,
    filesystem: FileSystemAdapter | None = None,
) -> GraphArrowApplication:
    """Build all GraphArrow components for a single database path."""

    database_path = _normalize_database_location(path)
    resolved_settings = settings or GraphArrowSettings(database_path=str(database_path))

    layout = DatabaseLayout(root=database_path)
    resolved_filesystem = filesystem or FileSystemAdapter.from_settings(resolved_settings)
    adapters = GraphArrowAdapters(
        bulk_input=BulkInputAdapter(),
        filesystem=resolved_filesystem,
        parquet=ParquetAdapter(
            filesystem=resolved_filesystem,
            filter_pushdown_enabled=resolved_settings.parquet_filter_pushdown_enabled,
        ),
        opencypher=OpenCypherAdapter(),
        tabular=PyArrowTabularAdapter(),
    )
    snapshot_tables = SnapshotTableCache(resolved_settings.snapshot_table_cache_max_bytes)
    storage_backend = create_storage_backend(
        settings=resolved_settings,
        layout=layout,
        filesystem=adapters.filesystem,
        parquet_adapter=adapters.parquet,
        tabular_adapter=adapters.tabular,
        snapshot_tables=snapshot_tables,
    )
    repositories = GraphArrowRepositories(
        catalog=storage_backend.catalog,
        snapshot=storage_backend.snapshot,
        graph=storage_backend.graph,
    )
    admin_use_case = DatabaseAdminUseCase(storage_backend=storage_backend)
    graph_use_case = GraphStateUseCase(
        storage_backend=storage_backend,
        catalog_store=repositories.catalog,
        snapshot_store=repositories.snapshot,
        graph_store=repositories.graph,
        snapshot_tables=snapshot_tables,
        cypher_max_concurrent_queries=resolved_settings.cypher_max_concurrent_queries,
        property_index_max_bytes=resolved_settings.property_index_max_bytes,
        adjacency_index_max_bytes=resolved_settings.adjacency_index_max_bytes,
    )
    index_use_case = IndexCatalogUseCase(catalog_store=repositories.catalog)
    cypher_use_case = CypherQueryUseCase(
        graph_service=graph_use_case,
        tabular_adapter=adapters.tabular,
        index_service=index_use_case,
        parse_cache=CypherParseCache(
            resolved_settings.cypher_parse_cache_max_entries,
            resolved_settings.cypher_parse_cache_max_source_bytes,
        ),
    )
    use_cases = GraphArrowUseCases(
        admin=admin_use_case,
        graph=graph_use_case,
        cypher=cypher_use_case,
        indexes=index_use_case,
    )
    local_root = storage_backend.writer.local_root
    schema_cache_disk = None
    if local_root is not None and resolved_settings.ana_persistent_schema_cache_enabled:
        schema_cache_disk = (
            local_root / ".grapharrow-cache" / "ana-schema.json",
            resolved_filesystem,
        )
    return GraphArrowApplication(
        settings=resolved_settings,
        layout=layout,
        adapters=adapters,
        repositories=repositories,
        storage_backend=storage_backend,
        use_cases=use_cases,
        _ana_schema_cache=SnapshotSchemaCache(
            disk=schema_cache_disk,
            edge_stats_max_bytes=resolved_settings.ana_edge_schema_cache_max_bytes,
            memory_max_bytes=(
                resolved_settings.ana_edge_schema_cache_max_bytes
                if resolved_settings.storage_backend == "delta"
                else 0
            ),
        ),
        _ana_node_lookups=AnaNodeLookupCache(
            resolved_settings.ana_node_lookup_cache_max_bytes
            if resolved_settings.storage_backend == "delta"
            else 0
        ),
    )


def _normalize_database_location(path: str | Path) -> str | Path:
    if isinstance(path, Path):
        return path.expanduser().resolve()

    text = str(path)
    if "://" in text:
        return text.rstrip("/")
    return Path(text).expanduser().resolve()
