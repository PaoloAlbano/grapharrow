"""Compare cold and repeated ANA discovery reads with the optional workflow cache."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import secrets
import socket
import statistics
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from typing import Any
from unittest.mock import patch

import httpx
import pyarrow as pa
import uvicorn

from grapharrow.app import build_application
from grapharrow.config import GraphArrowSettings
from grapharrow.interfaces.rest import create_rest_app
from grapharrow.use_cases.ana import GraphArrowAgentBackend

QUERIES = {
    "entity_resolve": ("entity_resolve", {"label": "Device", "text": "device-000042"}),
    "entity_resolve_context": (
        "entity_resolve",
        {"label": "Device", "text": "device-000042", "context_terms": ["x"]},
    ),
    "value_search": (
        "value_search",
        {"label": "Device", "property": "name", "text": "device-000042", "match_mode": "exact"},
    ),
    "value_search_prefix": (
        "value_search",
        {
            "label": "Device",
            "property": "name",
            "text": "device-000",
            "match_mode": "prefix",
            "limit": 100,
        },
    ),
}


def build_fixture(root: Path, size: int) -> None:
    app = build_application(root)
    _, nodes = app.use_cases.graph.create_nodes_bulk(
        table=pa.table(
            {
                "name": [f"device-{i:06d}" for i in range(size)],
                "value": list(range(size)),
                "description": ["x" * 160] * size,
            }
        ),
        default_labels=("Device",),
    )
    app.use_cases.graph.create_edges_bulk(
        table=pa.table(
            {
                "src_id": [node.node_id for node in nodes for _ in range(3)],
                "dst_id": [
                    nodes[(i + delta) % size].node_id for i in range(size) for delta in (1, 2, 3)
                ],
                "rel_type": ["LINK"] * (size * 3),
            }
        )
    )


def measure(
    call: Callable[[str, dict[str, Any]], dict[str, Any]],
    tool: str,
    arguments: dict[str, Any],
    repeats: int,
) -> dict[str, Any]:
    samples = []
    signatures = set()
    handles = set()
    for _ in range(repeats + 2):
        started = time.perf_counter_ns()
        result = call(tool, arguments)
        samples.append((time.perf_counter_ns() - started) / 1e6)
        handles.add(result["handle"])
        page = call("fetch", {"from": result["handle"]})
        visible = {key: value for key, value in result.items() if key != "handle"}
        signatures.add(
            hashlib.sha256(json.dumps([visible, page], sort_keys=True).encode()).hexdigest()
        )
    assert len(handles) == repeats + 2
    assert len(signatures) == 1
    return {
        "first_call_ms": samples[0],
        "warm_ms": samples[2:],
        "warm_median_ms": statistics.median(samples[2:]),
        "result_sha256": signatures.pop(),
    }


@contextmanager
def http_server(root: Path, settings: GraphArrowSettings, auth: Path) -> Iterator[httpx.Client]:
    key = secrets.token_urlsafe(32)
    auth.write_text(json.dumps({"clients": [{"id": "benchmark", "api_key": key}]}))
    auth.chmod(0o600)
    with patch.dict(os.environ, {"ANA_AUTH_FILE": str(auth)}):
        app = create_rest_app(root, settings=settings)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        port = listener.getsockname()[1]
        server = uvicorn.Server(uvicorn.Config(app, log_level="error", access_log=False))
        thread = Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)
        thread.start()
        try:
            deadline = time.monotonic() + 10
            while not server.started and thread.is_alive() and time.monotonic() < deadline:
                time.sleep(0.01)
            assert server.started
            with httpx.Client(
                base_url=f"http://127.0.0.1:{port}",
                trust_env=False,
                headers={"Authorization": "Bearer " + key},
                timeout=60,
            ) as client:
                yield client
        finally:
            server.should_exit = True
            thread.join(timeout=10)
            assert not thread.is_alive()


def http_case(
    client: httpx.Client, tool: str, arguments: dict[str, Any], repeats: int
) -> dict[str, Any]:
    session = client.post("/sessions")
    session.raise_for_status()
    token = session.json()["session_id"]
    assert token == session.headers["X-ANA-Session"]
    headers = {"X-ANA-Session": token}

    def call(name: str, args: dict[str, Any]) -> dict[str, Any]:
        response = client.post("/tools/call", headers=headers, json={"tool": name, "args": args})
        response.raise_for_status()
        assert response.headers["X-ANA-Session"] == token
        return dict(response.json()["result"])

    try:
        return measure(call, tool, arguments, repeats)
    finally:
        assert client.delete("/sessions/current", headers=headers).status_code == 204


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nodes", type=int, default=25000)
    parser.add_argument("--repeats", type=int, default=15)
    parser.add_argument(
        "--http", action="store_true", help="Also measure the HTTP service on loopback"
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.nodes < 1000 or args.repeats < 1:
        parser.error("Use at least 1000 nodes and positive repeats")
    report: dict[str, Any] = {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "pyarrow": pa.__version__,
        "nodes": args.nodes,
        "edges": args.nodes * 3,
        "warmups": 2,
        "repeats": args.repeats,
        "runs": {},
    }
    with TemporaryDirectory(prefix="grapharrow-read-cache-") as temporary:
        directory = Path(temporary)
        root = directory / "database"
        build_fixture(root, args.nodes)
        for enabled in (False, True):
            settings = GraphArrowSettings(
                _env_file=None, database_path=root, ana_read_cache_enabled=enabled
            )
            app = build_application(root, settings=settings)
            cases = report["runs"]["enabled" if enabled else "disabled"] = {}
            for name, (tool, query) in QUERIES.items():
                with GraphArrowAgentBackend(app) as backend:
                    case = cases[name] = {
                        "native": measure(backend.call, tool, query, args.repeats)
                    }
                    case["usage"] = backend.resource_usage
            if args.http:
                with http_server(root, settings, directory / "clients.json") as client:
                    for name, (tool, query) in QUERIES.items():
                        cases[name]["http"] = http_case(client, tool, query, args.repeats)
        for name in QUERIES:
            for interface in ("native", "http") if args.http else ("native",):
                before, after = [
                    report["runs"][mode][name][interface] for mode in ("disabled", "enabled")
                ]
                assert before["result_sha256"] == after["result_sha256"]
                print(
                    f"{name} {interface}: {before['warm_median_ms']:.3f} "
                    f"-> {after['warm_median_ms']:.3f} ms",
                    flush=True,
                )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
