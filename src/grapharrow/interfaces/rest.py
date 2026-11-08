"""REST interface for the bootstrap GraphArrow server."""

from __future__ import annotations

from importlib.metadata import version
from pathlib import Path
from typing import Any, cast

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from grapharrow.adapters import FileSystemAdapter
from grapharrow.app import build_application
from grapharrow.config import GraphArrowSettings
from grapharrow.core.errors import (
    CatalogError,
    GraphArrowError,
    NotFoundError,
    QueryError,
    StorageError,
    TransactionError,
    WriteConflictError,
)


class QueryRequest(BaseModel):
    """Request payload for Cypher-over-HTTP execution."""

    query: str
    params: dict[str, Any] = Field(default_factory=dict)


class NodeCreateRequest(BaseModel):
    """Request payload for imperative node creation."""

    labels: list[str]
    properties: dict[str, Any] = Field(default_factory=dict)


class BulkNodeCreateRequest(BaseModel):
    """Request payload for bulk imperative node creation."""

    rows: list[dict[str, Any]] = Field(default_factory=list)
    default_labels: list[str] = Field(default_factory=list)


class NodeReferenceRequest(BaseModel):
    """Explicit reference to a node through a label-qualified business key."""

    label: str
    key: str
    value: Any


class EdgeCreateRequest(BaseModel):
    """Request payload for imperative edge creation."""

    src_id: int | None = None
    dst_id: int | None = None
    source_ref: NodeReferenceRequest | None = None
    target_ref: NodeReferenceRequest | None = None
    rel_type: str
    properties: dict[str, Any] = Field(default_factory=dict)


class EdgeReferenceConfigRequest(BaseModel):
    """Relationship bulk-ingest key configuration for one endpoint side."""

    label: str
    key: str


class BulkEdgeCreateRequest(BaseModel):
    """Request payload for bulk imperative edge creation."""

    rows: list[dict[str, Any]] = Field(default_factory=list)
    source_key: EdgeReferenceConfigRequest | None = None
    target_key: EdgeReferenceConfigRequest | None = None


class TripleCreateRequest(BaseModel):
    """Request payload for imperative triple creation."""

    subject_id: int | None = None
    subject_ref: NodeReferenceRequest | None = None
    predicate: str
    object_id: int | None = None
    object_ref: NodeReferenceRequest | None = None
    properties: dict[str, Any] = Field(default_factory=dict)


class RecoveryRequest(BaseModel):
    """Request payload for recovery execution."""

    dry_run: bool = True
    cleanup_orphan_staged: bool = True
    verify_checksums: bool = False


class VacuumResponse(BaseModel):
    """Response payload for vacuum execution."""

    removed_paths: list[str]


def create_rest_app(
    path: str | Path,
    *,
    settings: GraphArrowSettings | None = None,
    filesystem: FileSystemAdapter | None = None,
) -> FastAPI:
    """Build the FastAPI application and register the public and protected GraphArrow routes."""

    container = build_application(path, settings=settings, filesystem=filesystem)
    container.use_cases.admin.initialize()

    app = FastAPI(title="GraphArrow", version=version("grapharrow"))
    app.state.grapharrow = container
    public_router = APIRouter(prefix=container.settings.rest_prefix)
    protected_router = APIRouter(
        prefix=container.settings.rest_prefix,
        dependencies=[Depends(_require_server_auth)],
    )

    @public_router.get("/health")
    def health() -> dict[str, object]:
        """Expose a cheap readiness view plus the latest verification metadata for the database."""
        verification = container.use_cases.admin.verify(
            verify_checksums=container.settings.verify_checksums_on_verify
        )
        return _ok(
            data={
                "status": "ok" if verification.ok else "degraded",
                "database_path": str(container.layout.root),
                "catalog_version": (
                    verification.catalog_version.value
                    if verification.catalog_version is not None
                    else None
                ),
                "snapshot_id": (
                    verification.snapshot_pointer.snapshot_id.value
                    if verification.snapshot_pointer is not None
                    else None
                ),
                "warnings": list(verification.warnings),
            }
        )

    @protected_router.post("/query")
    def query(payload: QueryRequest) -> dict[str, object]:
        """Execute a Cypher statement over HTTP and wrap the tabular result in the REST envelope."""
        result = container.use_cases.cypher.execute(payload.query, dict(payload.params))
        return _ok(
            data={
                "columns": list(result.columns),
                "rows": list(result.rows),
            },
            meta={"row_count": len(result.rows)},
        )

    @protected_router.post("/nodes", status_code=status.HTTP_201_CREATED)
    def create_node(payload: NodeCreateRequest) -> dict[str, object]:
        """Create one node through the imperative REST API and return its public payload."""
        _, node = container.use_cases.graph.create_node(
            labels=tuple(payload.labels),
            properties=dict(payload.properties),
        )
        return _ok(data=_node_payload(node))

    @protected_router.post("/nodes/bulk", status_code=status.HTTP_201_CREATED)
    def create_nodes_bulk(payload: BulkNodeCreateRequest) -> dict[str, object]:
        """Create many nodes in one request through the bulk-ingest adapter and graph use case."""
        table = container.adapters.bulk_input.to_table(payload.rows)
        _, nodes = container.use_cases.graph.create_nodes_bulk(
            table=table,
            default_labels=tuple(payload.default_labels),
        )
        rows = [_node_payload(node) for node in nodes]
        return _ok(data={"nodes": rows}, meta={"row_count": len(rows)})

    @protected_router.post("/edges", status_code=status.HTTP_201_CREATED)
    def create_edge(payload: EdgeCreateRequest) -> dict[str, object]:
        """Create one relationship from ids or business-key references."""
        edge = _create_edge_from_request(container, payload)
        return _ok(data=_edge_payload(edge))

    @protected_router.post("/edges/bulk", status_code=status.HTTP_201_CREATED)
    def create_edges_bulk(payload: BulkEdgeCreateRequest) -> dict[str, object]:
        """Create many relationships in one request through the bulk edge-ingest path."""
        table = container.adapters.bulk_input.to_table(payload.rows)
        source_key = (
            (payload.source_key.label, payload.source_key.key)
            if payload.source_key is not None
            else None
        )
        target_key = (
            (payload.target_key.label, payload.target_key.key)
            if payload.target_key is not None
            else None
        )
        _, edges = container.use_cases.graph.create_edges_bulk(
            table=table,
            source_key=source_key,
            target_key=target_key,
        )
        rows = [_edge_payload(edge) for edge in edges]
        return _ok(data={"edges": rows}, meta={"row_count": len(rows)})

    @protected_router.post("/triples", status_code=status.HTTP_201_CREATED)
    def create_triple(payload: TripleCreateRequest) -> dict[str, object]:
        """Expose the edge-creation flow under a subject/predicate/object triple-shaped request."""
        edge = _create_edge_from_triple_request(container, payload)
        return _ok(data=_edge_payload(edge))

    @protected_router.get("/admin/verify")
    def verify() -> dict[str, object]:
        """Return the current integrity report for manifests, files, and checksums."""
        verification = container.use_cases.admin.verify(
            verify_checksums=container.settings.verify_checksums_on_verify
        )
        return _ok(
            data={
                "ok": verification.ok,
                "catalog_version": (
                    verification.catalog_version.value
                    if verification.catalog_version is not None
                    else None
                ),
                "snapshot_pointer": (
                    {
                        "snapshot_id": verification.snapshot_pointer.snapshot_id.value,
                        "manifest_path": verification.snapshot_pointer.manifest_path,
                        "commit_txid": verification.snapshot_pointer.commit_txid,
                        "created_at": verification.snapshot_pointer.created_at,
                    }
                    if verification.snapshot_pointer is not None
                    else None
                ),
                "missing_paths": list(verification.missing_paths),
                "checksum_failures": list(verification.checksum_failures),
                "warnings": list(verification.warnings),
            }
        )

    @protected_router.post("/admin/recover")
    def recover(payload: RecoveryRequest) -> dict[str, object]:
        """Run the recovery workflow and serialize both verification and cleanup results."""
        report = container.use_cases.admin.recover(
            dry_run=payload.dry_run,
            cleanup_orphan_staged=payload.cleanup_orphan_staged,
            verify_checksums=payload.verify_checksums,
        )
        return _ok(
            data={
                "verification": {
                    "ok": report.verification.ok,
                    "catalog_version": (
                        report.verification.catalog_version.value
                        if report.verification.catalog_version is not None
                        else None
                    ),
                    "snapshot_pointer": (
                        {
                            "snapshot_id": report.verification.snapshot_pointer.snapshot_id.value,
                            "manifest_path": report.verification.snapshot_pointer.manifest_path,
                            "commit_txid": report.verification.snapshot_pointer.commit_txid,
                            "created_at": report.verification.snapshot_pointer.created_at,
                        }
                        if report.verification.snapshot_pointer is not None
                        else None
                    ),
                    "missing_paths": list(report.verification.missing_paths),
                    "checksum_failures": list(report.verification.checksum_failures),
                    "warnings": list(report.verification.warnings),
                },
                "orphan_staged_paths": list(report.orphan_staged_paths),
                "removed_paths": list(report.removed_paths),
                "warnings": list(report.warnings),
            }
        )

    @protected_router.post("/admin/compact")
    def compact() -> dict[str, object]:
        """Publish a compacted snapshot and return the new snapshot identifiers."""
        manifest = container.use_cases.admin.compact()
        return _ok(
            data={
                "snapshot_id": manifest.snapshot_id.value,
                "parent_snapshot_id": (
                    manifest.parent_snapshot_id.value
                    if manifest.parent_snapshot_id is not None
                    else None
                ),
                "catalog_version": manifest.catalog_version.value,
            }
        )

    @protected_router.post("/admin/vacuum")
    def vacuum() -> dict[str, object]:
        """Remove unreachable snapshot files and return the deleted paths."""
        removed_paths = container.use_cases.admin.vacuum()
        payload = VacuumResponse(removed_paths=list(removed_paths))
        return _ok(data=payload.model_dump())

    app.include_router(public_router)
    app.include_router(protected_router)
    _register_error_handlers(app)
    if container.settings.ana_enabled:
        from grapharrow.interfaces.ana import install_ana_routes

        install_ana_routes(app, container)
    return app


def _require_server_auth(
    request: Request,
    authorization: str | None = Header(default=None),
) -> None:
    """Enforce the optional bearer token configured for the REST server."""
    container = request.app.state.grapharrow
    expected_token = container.settings.server_auth_token
    if expected_token is None:
        return

    expected_header = f"Bearer {expected_token}"
    if authorization != expected_header:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing authorization token.",
        )


def _register_error_handlers(app: FastAPI) -> None:
    """Register the API exception handlers used by this REST application."""

    @app.exception_handler(WriteConflictError)
    async def write_conflict_handler(_: Request, exc: WriteConflictError) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content=_error("WRITE_CONFLICT", str(exc)),
        )

    @app.exception_handler(NotFoundError)
    async def not_found_handler(_: Request, exc: NotFoundError) -> JSONResponse:
        """Translate missing-entity failures into the stable REST error envelope."""
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content=_error("NOT_FOUND", str(exc)),
        )

    async def query_error_handler(
        _: Request,
        exc: Exception,
    ) -> JSONResponse:
        """Translate query and transaction failures into client-visible 400 responses."""
        typed_exc = cast(QueryError | TransactionError, exc)
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content=_error(type(typed_exc).__name__.upper(), str(typed_exc)),
        )

    app.add_exception_handler(QueryError, query_error_handler)
    app.add_exception_handler(TransactionError, query_error_handler)

    async def storage_error_handler(
        _: Request,
        exc: Exception,
    ) -> JSONResponse:
        """Translate storage and catalog conflicts into the REST error envelope."""
        typed_exc = cast(CatalogError | StorageError, exc)
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content=_error(type(typed_exc).__name__.upper(), str(typed_exc)),
        )

    app.add_exception_handler(CatalogError, storage_error_handler)
    app.add_exception_handler(StorageError, storage_error_handler)

    @app.exception_handler(GraphArrowError)
    async def grapharrow_error_handler(_: Request, exc: GraphArrowError) -> JSONResponse:
        """Catch any remaining GraphArrow domain error and expose it as a 500 response."""
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=_error(type(exc).__name__.upper(), str(exc)),
        )


def _ok(*, data: dict[str, Any], meta: dict[str, Any] | None = None) -> dict[str, object]:
    """Wrap successful REST responses in the common GraphArrow API envelope."""
    return {
        "ok": True,
        "data": data,
        "meta": meta or {},
    }


def _error(code: str, message: str) -> dict[str, object]:
    """Wrap REST failures in the common GraphArrow API error envelope."""
    return {
        "ok": False,
        "error": {
            "code": code,
            "message": message,
        },
    }


def _create_edge_from_request(
    container: Any,
    payload: EdgeCreateRequest,
) -> Any:
    """Normalize the REST edge-create payload into the id-based or reference-based graph call."""
    using_ids = payload.src_id is not None or payload.dst_id is not None
    using_refs = payload.source_ref is not None or payload.target_ref is not None

    if using_ids and using_refs:
        raise QueryError("Use either node ids or explicit node references, not both.")
    if using_ids:
        if payload.src_id is None or payload.dst_id is None:
            raise QueryError("Both src_id and dst_id are required when using node ids.")
        _, edge = container.use_cases.graph.create_edge(
            src_id=payload.src_id,
            dst_id=payload.dst_id,
            rel_type=payload.rel_type,
            properties=dict(payload.properties),
        )
        return edge
    if using_refs:
        if payload.source_ref is None or payload.target_ref is None:
            raise QueryError(
                "Both source_ref and target_ref are required when using business-key references."
            )
        _, edge = container.use_cases.graph.create_edge_by_reference(
            source_label=payload.source_ref.label,
            source_key=payload.source_ref.key,
            source_value=payload.source_ref.value,
            target_label=payload.target_ref.label,
            target_key=payload.target_ref.key,
            target_value=payload.target_ref.value,
            rel_type=payload.rel_type,
            properties=dict(payload.properties),
        )
        return edge

    raise QueryError("Either node ids or explicit node references are required.")


def _create_edge_from_triple_request(
    container: Any,
    payload: TripleCreateRequest,
) -> Any:
    """Reuse the edge creation path for triple-shaped REST payloads."""
    edge_request = EdgeCreateRequest(
        src_id=payload.subject_id,
        dst_id=payload.object_id,
        source_ref=payload.subject_ref,
        target_ref=payload.object_ref,
        rel_type=payload.predicate,
        properties=payload.properties,
    )
    return _create_edge_from_request(container, edge_request)


def _node_payload(node: Any) -> dict[str, object]:
    """Serialize an internal node record into the public REST node representation."""
    return {
        "id": node.node_id,
        "labels": list(node.labels),
        "properties": dict(node.properties),
    }


def _edge_payload(edge: Any) -> dict[str, object]:
    """Serialize an internal edge record into the public REST relationship representation."""
    return {
        "id": edge.edge_id,
        "type": edge.rel_type,
        "source": edge.src_id,
        "target": edge.dst_id,
        "properties": dict(edge.properties),
    }


__all__ = ["create_rest_app"]
