"""Measure ANA seed filtering and entity ranking through native and real HTTP calls."""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import os
import platform
import secrets
import socket
import statistics
import sys
import time
import tracemalloc
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
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
from grapharrow.use_cases import ana_search
from grapharrow.use_cases.ana import GraphArrowAgentBackend


def cases(size: int) -> dict[str, tuple[str, dict[str, Any]]]:
    pattern = {
        "start_label": "Device",
        "start_as": "d",
        "hops": [],
        "limit": 10,
        "select": [{"var": "d", "property": "name", "alias": "name"}],
    }
    return {
        "seed_equal_indexed": (
            "pattern_query",
            pattern | {"filters": [{"var": "d", "property": "name", "value": "device-000042"}]},
        ),
        "seed_range": (
            "pattern_query",
            pattern
            | {"filters": [{"var": "d", "property": "value", "op": "gte", "value": size - 5}]},
        ),
        "seed_list": (
            "pattern_query",
            pattern | {"filters": [{"var": "d", "property": "tags", "value": "group-42"}]},
        ),
        "seed_unfiltered_control": ("pattern_query", pattern),
        "seed_two_hop": (
            "multi_hop_query",
            {
                "start_label": "Device",
                "start_as": "d",
                "limit": 1,
                "filters": [{"var": "d", "property": "value", "value": 42}],
                "hops": [
                    {"relationship_type": "LINK", "target_label": "Device", "as": "middle"},
                    {"relationship_type": "LINK", "target_label": "Device", "as": "target"},
                ],
            },
        ),
        "entity_name": ("entity_resolve", {"label": "Device", "text": "device-000042"}),
        "entity_context": (
            "entity_resolve",
            {"label": "Device", "text": "device-000042", "context_terms": ["x"]},
        ),
        "entity_alias": ("entity_resolve", {"label": "Device", "text": "alias-000042"}),
        "entity_unicode": ("entity_resolve", {"label": "Device", "text": "i̇stanbul"}),
        "entity_miss": ("entity_resolve", {"label": "Device", "text": "not-a-company"}),
    }


def fixture(path: Path, size: int) -> None:
    app = build_application(path)
    if app.layout.current_snapshot_path.exists():
        raise ValueError("Use a new fixture path")
    _, nodes = app.use_cases.graph.create_nodes_bulk(
        table=pa.table(
            {
                "name": [
                    f"İstanbul-{i:06d}" if i % 1000 == 0 else f"device-{i:06d}" for i in range(size)
                ],
                "value": list(range(size)),
                "description": ["x" * 160] * size,
                "aliases": [
                    [f"İstanbul-{i:06d}" if i % 1000 == 0 else f"alias-{i:06d}"]
                    for i in range(size)
                ],
                "tags": [[f"group-{i % 1000}"] for i in range(size)],
            }
        ),
        default_labels=("Device",),
    )
    app.use_cases.graph.create_edges_bulk(
        table=pa.table(
            {
                "src_id": [node.node_id for node in nodes for _ in range(2)],
                "dst_id": [
                    nodes[(i + step) % size].node_id for i in range(size) for step in (1, 2)
                ],
                "rel_type": ["LINK"] * (size * 2),
            }
        )
    )
    app.use_cases.indexes.create_index(
        name="name", kind="range", owner_kind="label", owner_name="Device", property_names=("name",)
    )


def measure(call: Any, tool: str, args: dict[str, Any], repeats: int, backend: Any = None) -> Any:
    times = []
    digests = set()
    for _ in range(repeats + 2):
        start = time.perf_counter_ns()
        result = call(tool, args)
        elapsed = (time.perf_counter_ns() - start) / 1e6
        times.append(elapsed)
        page = call("fetch", {"from": result["handle"], "limit": 1000})
        payload = [{key: value for key, value in result.items() if key != "handle"}, page]
        digests.add(hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest())
    assert len(digests) == 1
    report = {
        "first_ms": times[0],
        "samples_ms": times[2:],
        "median_ms": statistics.median(times[2:]),
        "result_sha256": digests.pop(),
    }
    if backend is not None:
        counters = {"seed_entities_materialized": 0, "narrow_property_rows": 0}
        materialize = backend._materialize_nodes
        search_rows = ana_search._search_rows

        def entities(ids: list[int]) -> Any:
            counters["seed_entities_materialized"] += len(ids)
            return materialize(ids)

        def rows(*args: Any, **kwargs: Any) -> Any:
            for row in search_rows(*args, **kwargs):
                counters["narrow_property_rows"] += 1
                yield row

        with ExitStack() as stack:
            stack.enter_context(patch.object(backend, "_materialize_nodes", entities))
            stack.enter_context(patch.object(ana_search, "_search_rows", rows))
            seed = sys.modules.get("grapharrow.use_cases.ana_seed")
            if seed is not None:
                stack.enter_context(patch.object(seed, "_search_rows", rows))
            gc.collect()
            tracemalloc.start()
            try:
                call(tool, args)
                _, peak = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()
        report.update(python_peak_bytes=peak, **counters)
        report["usage"] = backend.resource_usage
    return report


@contextmanager
def service(path: Path, settings: GraphArrowSettings, auth: Path) -> Iterator[tuple[Any, Any]]:
    key = secrets.token_urlsafe(32)
    auth.write_text(json.dumps({"clients": [{"id": "benchmark", "api_key": key}]}))
    auth.chmod(0o600)
    with patch.dict(os.environ, {"ANA_AUTH_FILE": str(auth)}):
        app = create_rest_app(path, settings=settings)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        server = uvicorn.Server(uvicorn.Config(app, log_level="error", access_log=False))
        thread = Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)
        thread.start()
        try:
            deadline = time.monotonic() + 10
            while not server.started and thread.is_alive() and time.monotonic() < deadline:
                time.sleep(0.01)
            assert server.started
            with httpx.Client(
                base_url=f"http://127.0.0.1:{listener.getsockname()[1]}",
                headers={"Authorization": "Bearer " + key},
                trust_env=False,
                timeout=120,
            ) as client:
                yield client, app
        finally:
            server.should_exit = True
            thread.join(timeout=10)
            assert not thread.is_alive()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--nodes", type=int, default=25000)
    parser.add_argument("--build-fixture", action="store_true")
    parser.add_argument("--repeats", type=int, default=21)
    parser.add_argument("--http", action="store_true")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.nodes < 1000 or args.repeats < 1:
        parser.error("Use at least 1000 nodes and positive repeats")
    if args.build_fixture:
        fixture(args.database, args.nodes)
    settings = GraphArrowSettings(_env_file=None, ana_read_cache_enabled=False)
    report = {
        "python": platform.python_version(),
        "pyarrow": pa.__version__,
        "platform": platform.platform(),
        "nodes": args.nodes,
        "edges": args.nodes * 2,
        "repeats": args.repeats,
        "warmups": 2,
        "native": {},
        "http": {},
    }
    app = build_application(args.database, settings=settings)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    for case, (tool, parameters) in cases(args.nodes).items():
        with GraphArrowAgentBackend(app) as backend:
            result = measure(backend.call, tool, parameters, args.repeats, backend)
            report["native"][case] = result
        assert backend._resources.budget.used_bytes == 0
        print(f"Native {case}: {result['median_ms']:.3f} ms", flush=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    if args.http:
        with TemporaryDirectory(prefix="grapharrow-seed-auth-") as temporary:
            with service(args.database, settings, Path(temporary) / "clients.json") as (
                client,
                http,
            ):
                for case, (tool, parameters) in cases(args.nodes).items():
                    opened = client.post("/sessions")
                    opened.raise_for_status()
                    token = opened.json()["session_id"]
                    assert token == opened.headers["X-ANA-Session"]
                    headers = {"X-ANA-Session": token}

                    def call(
                        name: str,
                        params: dict[str, Any],
                        headers: dict[str, str] = headers,
                        token: str = token,
                    ) -> Any:
                        response = client.post(
                            "/tools/call", headers=headers, json={"tool": name, "args": params}
                        )
                        response.raise_for_status()
                        assert response.headers["X-ANA-Session"] == token
                        return response.json()["result"]

                    try:
                        result = measure(call, tool, parameters, args.repeats)
                        report["http"][case] = result
                    finally:
                        assert (
                            client.delete("/sessions/current", headers=headers).status_code == 204
                        )
                    assert http.state.ana_resource_budget.used_bytes == 0
                    print(f"HTTP {case}: {result['median_ms']:.3f} ms", flush=True)
                    args.output.write_text(json.dumps(report, indent=2) + "\n")
    import grapharrow

    source = sorted(Path(grapharrow.__file__).parent.rglob("*.py"))
    report["source_sha256"] = hashlib.sha256(
        b"".join(path.read_bytes() for path in source)
    ).hexdigest()
    report["budget_after_close_bytes"] = 0
    args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
