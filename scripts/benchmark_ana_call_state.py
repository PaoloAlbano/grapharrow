"""Measure ANA per-call overhead as retained workflow handle count grows."""

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
import tracemalloc
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import replace
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
from grapharrow.use_cases.ana_resources import AnaQuotaExceeded, AnaResourceBudget, retained_size

Call = Callable[[str, dict[str, Any]], dict[str, Any]]


def measure(
    call: Call, tool: str, arguments: dict[str, Any], repeats: int, *, memory: bool = False
) -> dict[str, Any]:
    samples, signatures, handles = [], set(), set()
    for _ in range(repeats + 5):
        started = time.perf_counter_ns()
        result = call(tool, arguments)
        samples.append((time.perf_counter_ns() - started) / 1e6)
        visible = {key: value for key, value in result.items() if key != "handle"}
        signatures.add(hashlib.sha256(json.dumps(visible, sort_keys=True).encode()).hexdigest())
        if "handle" in result:
            handles.add(result["handle"])
    assert len(signatures) == 1
    record = {
        "warm_ms": samples[5:],
        "median_ms": statistics.median(samples[5:]),
        "result_sha256": signatures.pop(),
        "new_handles": len(handles),
    }
    if memory:
        tracemalloc.start()
        call(tool, arguments)
        _, record["python_peak_bytes"] = tracemalloc.get_traced_memory()
        tracemalloc.stop()
    return record


def exercise(call: Call, handle_count: int, repeats: int, *, backend: Any = None) -> dict[str, Any]:
    started = time.perf_counter_ns()
    first = None
    for _ in range(handle_count):
        handle = call("count_nodes", {"label": "Device"})["handle"]
        first = first or handle
    setup_ms = (time.perf_counter_ns() - started) / 1e6
    assert first == "h1"
    before = backend.resource_usage if backend is not None else None
    result = {
        "initial_handles": handle_count,
        "setup_ms": setup_ms,
        "schema_overview": measure(
            call, "schema_overview", {}, repeats, memory=backend is not None
        ),
        "fetch": measure(
            call, "fetch", {"from": first, "limit": 1}, repeats, memory=backend is not None
        ),
    }
    assert call("fetch", {"from": first, "limit": 1})["rows"] == [[5]]
    if backend is not None:
        original = backend._resources.limits
        prior = backend.resource_usage
        provisional = retained_size([{"device_count": 5}]) + retained_size(
            (None, "table", ["device_count"], None)
        )
        backend._resources.limits = replace(original, max_bytes=prior["bytes"] + provisional + 64)

        def reject(tool: str, args: dict[str, Any]) -> dict[str, Any]:
            try:
                call(tool, args)
            except AnaQuotaExceeded as error:
                return {"error": str(error)}
            raise AssertionError("Expected final-admission failure")

        try:
            result["quota_rollback"] = measure(
                reject, "count_nodes", {"label": "Device"}, repeats, memory=True
            )
            assert backend.resource_usage == prior
        finally:
            backend._resources.limits = original
    result["count_nodes"] = measure(
        call, "count_nodes", {"label": "Device"}, repeats, memory=backend is not None
    )
    if backend is not None:
        assert before["handles"] == handle_count
        assert backend.resource_usage["handles"] == handle_count + repeats + 6
        result["final_usage"] = backend.resource_usage
    return result


@contextmanager
def http_server(
    root: Path, settings: GraphArrowSettings, auth: Path
) -> Iterator[tuple[httpx.Client, Any]]:
    key = secrets.token_urlsafe(32)
    auth.write_text(json.dumps({"clients": [{"id": "benchmark", "api_key": key}]}))
    auth.chmod(0o600)
    with patch.dict(os.environ, {"ANA_AUTH_FILE": str(auth)}):
        application = create_rest_app(root, settings=settings)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        port = listener.getsockname()[1]
        server = uvicorn.Server(uvicorn.Config(application, log_level="error", access_log=False))
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
                timeout=120,
            ) as client:
                yield client, application
        finally:
            server.should_exit = True
            thread.join(timeout=10)
            assert not thread.is_alive()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--handles", type=int, nargs="+", default=[10, 1000, 10000])
    parser.add_argument("--repeats", type=int, default=101)
    parser.add_argument("--http", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert args.repeats > 0 and all(0 < count < 100000 for count in args.handles)
    report: dict[str, Any] = {
        "python": platform.python_version(),
        "pyarrow": pa.__version__,
        "platform": platform.platform(),
        "repeats": args.repeats,
        "warmups": 5,
        "native": {},
        "http": {},
    }
    settings = GraphArrowSettings(
        ana_read_cache_enabled=False,
        ana_max_rows_per_session=200000,
        ana_max_bytes_per_session=512 * 1024 * 1024,
        ana_max_total_bytes=2 * 1024 * 1024 * 1024,
    )
    with TemporaryDirectory(prefix="grapharrow-call-state-") as temporary:
        root = Path(temporary)
        app = build_application(root / "db", settings=settings)
        app.use_cases.graph.create_nodes_bulk(
            table=pa.table({"name": [f"device-{i}" for i in range(5)]}), default_labels=("Device",)
        )
        for count in args.handles:
            budget = AnaResourceBudget(settings.ana_max_total_bytes)
            with GraphArrowAgentBackend(app, resource_budget=budget) as backend:
                report["native"][str(count)] = exercise(
                    backend.call, count, args.repeats, backend=backend
                )
            assert budget.used_bytes == 0
            print(f"Native: {count} retained handles completed", flush=True)
        if args.http:
            with http_server(root / "db", settings, root / "auth.json") as (client, application):
                for count in args.handles:
                    session = client.post("/sessions")
                    session.raise_for_status()
                    token = session.json()["session_id"]
                    assert token == session.headers["X-ANA-Session"]
                    headers = {"X-ANA-Session": token}

                    def call(
                        tool: str,
                        arguments: dict[str, Any],
                        headers: dict[str, str] = headers,
                        token: str = token,
                    ) -> dict[str, Any]:
                        response = client.post(
                            "/tools/call", headers=headers, json={"tool": tool, "args": arguments}
                        )
                        response.raise_for_status()
                        assert response.headers["X-ANA-Session"] == token
                        return response.json()["result"]

                    try:
                        report["http"][str(count)] = exercise(call, count, args.repeats)
                    finally:
                        assert (
                            client.delete("/sessions/current", headers=headers).status_code == 204
                        )
                    assert application.state.ana_resource_budget.used_bytes == 0
                    print(f"HTTP: {count} retained handles completed", flush=True)
    for count in report["http"]:
        for operation in ["schema_overview", "fetch", "count_nodes"]:
            assert (
                report["http"][count][operation]["result_sha256"]
                == report["native"][count][operation]["result_sha256"]
            )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(f"Call-state benchmark saved to {args.output}", flush=True)


if __name__ == "__main__":
    main()
