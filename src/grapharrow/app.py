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
from grapharrow.config import GraphArrowSettings
from grapharrow.core import DatabaseLayout
from grapharrow.repositories import CatalogRepository, GraphRepository, SnapshotRepository
from grapharrow.use_cases import (
    CypherQueryUseCase,
    DatabaseAdminUseCase,
    GraphStateUseCase,
    IndexCatalogUseCase,
)
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

    catalog: CatalogRepository
    snapshot: SnapshotRepository
    graph: GraphRepository


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
    use_cases: GraphArrowUseCases
    _ana_schema_cache: SnapshotSchemaCache = field(default_factory=SnapshotSchemaCache, repr=False)


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
    repositories = GraphArrowRepositories(
        catalog=CatalogRepository(layout=layout, filesystem=adapters.filesystem),
        snapshot=SnapshotRepository(layout=layout, filesystem=adapters.filesystem),
        graph=GraphRepository(
            layout=layout,
            parquet_adapter=adapters.parquet,
            tabular_adapter=adapters.tabular,
            filesystem=adapters.filesystem,
        ),
    )
    admin_use_case = DatabaseAdminUseCase(
        layout=layout,
        catalog_store=repositories.catalog,
        snapshot_store=repositories.snapshot,
        graph_store=repositories.graph,
        filesystem=adapters.filesystem,
    )
    graph_use_case = GraphStateUseCase(
        admin_service=admin_use_case,
        catalog_store=repositories.catalog,
        snapshot_store=repositories.snapshot,
        graph_store=repositories.graph,
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
    return GraphArrowApplication(
        settings=resolved_settings,
        layout=layout,
        adapters=adapters,
        repositories=repositories,
        use_cases=use_cases,
    )


def _normalize_database_location(path: str | Path) -> str | Path:
    if isinstance(path, Path):
        return path.expanduser().resolve()

    text = str(path)
    if "://" in text:
        return text.rstrip("/")
    return Path(text).expanduser().resolve()
