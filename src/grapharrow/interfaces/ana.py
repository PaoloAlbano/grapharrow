"""ANA-compatible HTTP adapter using GraphArrow auth, catalog and sessions."""

from contextlib import asynccontextmanager
from typing import Annotated, Any

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Response
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from grapharrow.ana.auth import (
    ApiKeyAuth,
    AuthenticationError,
    AuthNotConfigured,
    Principal,
)
from grapharrow.ana.catalog import get_tool_contract, get_tool_specs
from grapharrow.ana.sessions import (
    BackendSessions,
    SessionCapacityError,
    SessionConfig,
    SessionNotFound,
)
from grapharrow.app import GraphArrowApplication
from grapharrow.use_cases.ana import SUPPORTED_ANA_TOOLS, GraphArrowAgentBackend
from grapharrow.use_cases.ana_resources import AnaQuotaExceeded, AnaResourceBudget


class ToolCallRequest(BaseModel):
    """The ANA v0 tool-dispatch request envelope."""

    tool: str
    args: dict[str, Any] = Field(default_factory=dict)


def install_ana_routes(app: FastAPI, application: GraphArrowApplication) -> None:
    """Add compatible root routes and share the application's shutdown lifecycle."""
    if not application.settings.rest_prefix.strip("/"):
        raise ValueError("ANA requires a non-root REST prefix, such as /v1")
    mode = application.settings.ana_schema_entry
    get_tool_specs(schema_entry=mode)
    config = SessionConfig(
        max_sessions=application.settings.ana_max_sessions,
        idle_timeout_s=application.settings.ana_session_idle_timeout_s,
    )
    budget = AnaResourceBudget(application.settings.ana_max_total_bytes)
    sessions = BackendSessions(
        lambda store: GraphArrowAgentBackend(
            application, handle_store=store, schema_entry=mode, resource_budget=budget
        ),
        config,
    )
    access = ApiKeyAuth.from_env()
    contracts = {item["name"]: item for item in get_tool_contract()}

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> Any:
        try:
            yield
        finally:
            await run_in_threadpool(sessions.close)

    router = APIRouter(lifespan=lifespan)
    app.state.ana_sessions = sessions
    app.state.ana_resource_budget = budget

    def authenticate(
        authorization: str | None = Header(default=None, alias="Authorization"),
    ) -> Principal:
        try:
            return access.authenticate(authorization)
        except AuthNotConfigured as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except AuthenticationError as exc:
            raise HTTPException(
                status_code=401, detail=str(exc), headers={"WWW-Authenticate": "Bearer"}
            ) from exc

    def permitted(principal: Principal, name: str) -> bool:
        contract = contracts.get(name)
        return bool(
            name in SUPPORTED_ANA_TOOLS and contract and principal.permits(contract["profile"])
        )

    def specs(enable_planning_tools: bool = False) -> list[dict[str, Any]]:
        return list(get_tool_specs(enable_planning_tools=enable_planning_tools, schema_entry=mode))

    callable_names = {spec["function"]["name"] for spec in specs(True)}

    @router.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @router.get("/tools")
    def tools(
        principal: Annotated[Principal, Depends(authenticate)], status: str | None = None
    ) -> dict[str, Any]:
        if status not in {None, "core", "experimental"}:
            raise HTTPException(status_code=400, detail="status must be core or experimental")
        return {
            "version": "ana-v0",
            "tools": [
                item for item in get_tool_contract(status) if permitted(principal, item["name"])
            ],
        }

    @router.get("/tools/specs")
    def tool_specs(
        principal: Annotated[Principal, Depends(authenticate)],
        enable_planning_tools: bool = False,
    ) -> dict[str, Any]:
        return {
            "version": "ana-v0",
            "tools": [
                spec
                for spec in specs(enable_planning_tools)
                if permitted(principal, spec["function"]["name"])
            ],
        }

    @router.post("/sessions", status_code=201)
    def create_session(
        response: Response, principal: Annotated[Principal, Depends(authenticate)]
    ) -> dict[str, Any]:
        try:
            token = sessions.create(owner_id=principal.subject)
        except SessionCapacityError as exc:
            raise HTTPException(status_code=429, detail=str(exc)) from exc
        except AnaQuotaExceeded as exc:
            raise HTTPException(status_code=429, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=503, detail="Could not open workflow session") from exc
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-ANA-Session"] = token
        return {"session_id": token, "idle_timeout_s": config.idle_timeout_s}

    @router.post("/tools/call")
    def call_tool(
        request: ToolCallRequest,
        response: Response,
        principal: Annotated[Principal, Depends(authenticate)],
        session_id: str | None = Header(default=None, alias="X-ANA-Session"),
    ) -> dict[str, Any]:
        if not session_id:
            raise HTTPException(status_code=401, detail="X-ANA-Session is required")
        if request.tool not in callable_names or not permitted(principal, request.tool):
            raise HTTPException(status_code=403, detail="Tool is not permitted")
        try:
            result = sessions.call(
                session_id, request.tool, request.args, owner_id=principal.subject
            )
        except SessionNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except AnaQuotaExceeded as exc:
            raise HTTPException(
                status_code=429,
                detail=str(exc),
                headers={"X-ANA-Session": session_id, "Cache-Control": "no-store"},
            ) from exc
        except Exception as exc:
            raise HTTPException(
                status_code=400,
                detail=str(exc),
                headers={"X-ANA-Session": session_id, "Cache-Control": "no-store"},
            ) from exc
        response.headers["X-ANA-Session"] = session_id
        response.headers["Cache-Control"] = "no-store"
        return {"tool": request.tool, "result": result}

    @router.delete("/sessions/current", status_code=204)
    def delete_session(
        principal: Annotated[Principal, Depends(authenticate)],
        session_id: str | None = Header(default=None, alias="X-ANA-Session"),
    ) -> Response:
        if not session_id:
            raise HTTPException(status_code=401, detail="X-ANA-Session is required")
        try:
            sessions.delete(session_id, owner_id=principal.subject)
        except SessionNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=503, detail="Session cleanup failed") from exc
        return Response(
            status_code=204, headers={"X-ANA-Session": session_id, "Cache-Control": "no-store"}
        )

    app.include_router(router)
