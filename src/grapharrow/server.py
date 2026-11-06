"""Server entrypoints for REST and future Bolt interfaces."""

from __future__ import annotations

import argparse

import uvicorn
from fastapi import FastAPI

from grapharrow.config import GraphArrowSettings
from grapharrow.interfaces.rest import create_rest_app


def build_server_application(
    path: str,
    *,
    settings: GraphArrowSettings | None = None,
) -> FastAPI:
    """Build the REST server application for a single database path."""

    return create_rest_app(path, settings=settings)


def main() -> int:
    """Run the REST server entrypoint."""

    parser = argparse.ArgumentParser(prog="grapharrow-server")
    parser.add_argument(
        "--path",
        default=None,
        help="Database directory served by the GraphArrow server.",
    )
    parser.add_argument("--host", default=None, help="Server host override.")
    parser.add_argument("--port", type=int, default=None, help="Server port override.")
    parser.add_argument(
        "--ana",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable or disable the ANA HTTP interface (enabled by default).",
    )
    args = parser.parse_args()

    settings = GraphArrowSettings()
    database_path = args.path if args.path is not None else settings.database_path
    if database_path is None:
        parser.error("A database path is required via --path or GRAPHARROW_DATABASE_PATH.")

    resolved_settings = settings.model_copy(
        update={
            "database_path": database_path,
            "ana_enabled": settings.ana_enabled if args.ana is None else args.ana,
        }
    )
    app = create_rest_app(database_path, settings=resolved_settings)
    uvicorn.run(
        app,
        host=args.host or resolved_settings.server_host,
        port=args.port or resolved_settings.server_port,
    )
    return 0


__all__ = ["build_server_application", "main"]
